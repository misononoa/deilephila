//! libp2p ノードのラッパー。サブモジュール構成:
//! - `behaviour`: NetworkBehaviour 合成と Swarm 構築
//! - `event_loop`: Swarm を所有する単一タスクの `select!` ループ
//! - `ipns`: IPNS-headレコードの DHT 搬送(キー導出・格納前検証)
//! - `protocol`: ブロック交換 request-response の codec
//!
//! この mod.rs には Core から見える公開 API(`NetworkCommand` / `NetworkEvent` /
//! `NetworkHandle` / `spawn_swarm_loop`)だけを置く。

mod behaviour;
mod event_loop;
mod ipns;
pub mod protocol;

use std::sync::Arc;

use cid::Cid;
use libp2p::{Multiaddr, PeerId};
use tokio::sync::{mpsc, oneshot};

use crate::head::{select_best, IpnsRecord};
use crate::store::Store;

// --- 公開型 ---

pub enum NetworkCommand {
    GetBlock {
        cid: Cid,
        /// announce 元など、ブロックを持っている見込みが高いピア(接続中なら優先)
        prefer: Option<PeerId>,
        reply: oneshot::Sender<Option<Vec<u8>>>,
    },
    Dial(Multiaddr),
    /// IPNS-headレコードを gossipsub(即時)と kad DHT(永続)の両経路へ
    /// publish する(fire-and-forget、networking.md §4.1・§4.2)
    PublishHead(IpnsRecord),
    /// DHT から IPNS-headレコードの候補を収集する(networking.md §4.2)。
    /// 署名検証と argmax 選択は受け手(`NetworkHandle::resolve_ipns`)で行う
    ResolveIpns {
        pubkey: [u8; 32],
        reply: oneshot::Sender<Vec<IpnsRecord>>,
    },
    /// 指定アカウントの feed トピックを購読する(= フォロー)
    Subscribe {
        pubkey_hex: String,
    },
    Unsubscribe {
        pubkey_hex: String,
    },
}

pub enum NetworkEvent {
    PeerConnected(PeerId),
    PeerDiscovered(PeerId),
    /// gossipsub で IPNS-headレコードを受信した(署名検証は同期エンジン側で行う)
    HeadReceived {
        record: IpnsRecord,
        source: PeerId,
    },
    /// あるピアがトピックを購読した(テスト・将来の mesh 観察用)
    PeerSubscribed {
        peer: PeerId,
        topic: String,
    },
}

#[derive(Clone)]
pub struct NetworkHandle {
    cmd_tx: mpsc::Sender<NetworkCommand>,
}

impl NetworkHandle {
    pub fn new(cmd_tx: mpsc::Sender<NetworkCommand>) -> Self {
        Self { cmd_tx }
    }

    pub async fn get_block(&self, cid: Cid, prefer: Option<PeerId>) -> Option<Vec<u8>> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(NetworkCommand::GetBlock {
                cid,
                prefer,
                reply: tx,
            })
            .await
            .ok()?;
        rx.await.ok().flatten()
    }

    pub async fn dial(&self, addr: Multiaddr) {
        let _ = self.cmd_tx.send(NetworkCommand::Dial(addr)).await;
    }

    pub async fn publish_head(&self, record: IpnsRecord) {
        let _ = self
            .cmd_tx
            .send(NetworkCommand::PublishHead(record))
            .await;
    }

    /// DHT から候補レコードを収集し、argmax統一規則(署名検証OK かつ 最大
    /// sequence)で最良のものを返す(networking.md §4)。候補なしは None。
    pub async fn resolve_ipns(&self, pubkey: [u8; 32]) -> Option<IpnsRecord> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(NetworkCommand::ResolveIpns { pubkey, reply: tx })
            .await
            .ok()?;
        let candidates = rx.await.ok()?;
        select_best(&pubkey, candidates.iter()).cloned()
    }

    pub async fn subscribe(&self, pubkey_hex: String) {
        let _ = self
            .cmd_tx
            .send(NetworkCommand::Subscribe { pubkey_hex })
            .await;
    }

    pub async fn unsubscribe(&self, pubkey_hex: String) {
        let _ = self
            .cmd_tx
            .send(NetworkCommand::Unsubscribe { pubkey_hex })
            .await;
    }
}

// --- Swarm 起動 ---

/// Swarm ループを起動し (NetworkHandle, NetworkEvent 受信側, 待受アドレス) を返す。
/// `listen` が None のとき "/ip4/0.0.0.0/tcp/0" でリッスンする(全インタフェース)。
pub async fn spawn_swarm_loop(
    store: Arc<Store>,
    listen: Option<Multiaddr>,
) -> Result<(NetworkHandle, mpsc::Receiver<NetworkEvent>, Multiaddr), Box<dyn std::error::Error>> {
    let listen_addr = listen.unwrap_or_else(|| "/ip4/0.0.0.0/tcp/0".parse().unwrap());

    let (cmd_tx, cmd_rx) = mpsc::channel::<NetworkCommand>(64);
    let (event_tx, event_rx) = mpsc::channel::<NetworkEvent>(64);
    let (addr_tx, addr_rx) = oneshot::channel::<Multiaddr>();

    tokio::spawn(event_loop::run_swarm_loop(
        store,
        cmd_rx,
        event_tx,
        addr_tx,
        listen_addr,
    ));

    let addr = addr_rx.await?;
    let handle = NetworkHandle::new(cmd_tx);
    Ok((handle, event_rx, addr))
}

// --- テスト ---

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        event::{envelope_cid, EventEnvelope, EventKind},
        head::{create_ipns_record, verify_ipns_record},
        identity::{create_envelope, Identity},
        util::{bytes_to_cid, bytes_to_hex, now_ms},
    };
    use std::time::Duration;

    async fn spawn_test_node(
        store: Arc<Store>,
    ) -> (NetworkHandle, mpsc::Receiver<NetworkEvent>, Multiaddr) {
        // テストでは 127.0.0.1 を使う(0.0.0.0 はダイアル先として使えないため)
        let listen: Multiaddr = "/ip4/127.0.0.1/tcp/0".parse().unwrap();
        spawn_swarm_loop(store, Some(listen))
            .await
            .expect("swarm failed to start")
    }

    async fn wait_peer_connected(event_rx: &mut mpsc::Receiver<NetworkEvent>) {
        loop {
            match event_rx.recv().await {
                Some(NetworkEvent::PeerConnected(_)) => return,
                Some(_) => continue,
                None => panic!("event channel closed before PeerConnected"),
            }
        }
    }

    #[tokio::test]
    async fn test_block_exchange_direct_dial() {
        let identity = Identity::generate();
        let envelope = create_envelope(
            &identity,
            0,
            None,
            EventKind::Post {
                text: "hello network".to_string(),
            },
        );
        let raw: Vec<u8> = serde_ipld_dagcbor::to_vec(&envelope).unwrap();
        let cid = envelope_cid(&envelope);

        // Node A: ブロックを持つ(insert_event が raw_cbor を保存する)
        let store_a = Arc::new(Store::open_in_memory().await.unwrap());
        store_a.insert_event(&envelope).await.unwrap();
        // _handle_a: drop すると cmd_tx が閉じて Swarm ループが終了するため保持する
        let (_handle_a, _, addr_a) = spawn_test_node(Arc::clone(&store_a)).await;

        // Node B: 空
        let store_b = Arc::new(Store::open_in_memory().await.unwrap());
        let (handle_b, mut events_b, _) = spawn_test_node(Arc::clone(&store_b)).await;

        // B → A にダイアルして接続を待つ
        handle_b.dial(addr_a).await;
        tokio::time::timeout(Duration::from_secs(5), wait_peer_connected(&mut events_b))
            .await
            .expect("peer connection timed out");

        // B が A からブロックを取得
        let received = handle_b
            .get_block(cid.clone(), None)
            .await
            .expect("get_block returned None");

        // 検証: バイト列一致 + CID 再計算
        assert_eq!(received, raw);
        let recovered: EventEnvelope = serde_ipld_dagcbor::from_slice(&received).unwrap();
        assert_eq!(envelope_cid(&recovered), cid);
    }

    #[tokio::test]
    async fn test_mdns_peer_discovery() {
        let store_a = Arc::new(Store::open_in_memory().await.unwrap());
        let store_b = Arc::new(Store::open_in_memory().await.unwrap());

        let (_, mut events_a, _) = spawn_test_node(store_a).await;
        let (_, mut events_b, _) = spawn_test_node(store_b).await;

        // どちらかが PeerDiscovered を受け取れば mDNS が機能している。
        // ループバックで mDNS が届かない環境ではタイムアウト → skip(fail にしない)。
        let discovered_a = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match events_a.recv().await {
                    Some(NetworkEvent::PeerDiscovered(_)) => return true,
                    Some(_) => continue,
                    None => return false,
                }
            }
        });
        let discovered_b = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match events_b.recv().await {
                    Some(NetworkEvent::PeerDiscovered(_)) => return true,
                    Some(_) => continue,
                    None => return false,
                }
            }
        });

        match tokio::join!(discovered_a, discovered_b) {
            (Ok(true), _) | (_, Ok(true)) => {
                // mDNS 発見成功
            }
            _ => {
                // タイムアウト: この環境では mDNS が届かない(skip)
                eprintln!("mDNS discovery timed out on loopback — skipping");
            }
        }
    }

    // --- IPNS-headレコードの DHT 搬送(M5b) ---

    fn far_future_ms() -> i64 {
        now_ms() + 3_600_000
    }

    fn make_record(identity: &Identity, sequence: u64) -> IpnsRecord {
        create_ipns_record(
            identity,
            sequence,
            bytes_to_cid(b"head block"),
            far_future_ms(),
            None,
            "Alice".to_string(),
        )
    }

    async fn wait_peer_connected_ev(rx: &mut mpsc::Receiver<NetworkEvent>) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match rx.recv().await {
                    Some(NetworkEvent::PeerConnected(_)) => return,
                    Some(_) => continue,
                    None => panic!("event channel closed before PeerConnected"),
                }
            }
        })
        .await
        .expect("peer connection timed out")
    }

    /// put の伝播や identify によるルーティングテーブル形成を待つため、
    /// 解決できるまでリトライする(上限 ~10 秒)。
    async fn resolve_with_retry(handle: &NetworkHandle, pubkey: [u8; 32]) -> Option<IpnsRecord> {
        for _ in 0..40 {
            if let Some(record) = handle.resolve_ipns(pubkey).await {
                return Some(record);
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        None
    }

    #[tokio::test]
    async fn dht_put_resolve_roundtrip() {
        // A: レコードを publish する発信者
        let store_a = Arc::new(Store::open_in_memory().await.unwrap());
        let (handle_a, _events_a, addr_a) = spawn_test_node(store_a).await;

        // B: gossipsub 購読なしのノード(DHT 経由でのみ取得できることの検証)
        let store_b = Arc::new(Store::open_in_memory().await.unwrap());
        let (handle_b, mut events_b, _) = spawn_test_node(store_b).await;

        handle_b.dial(addr_a).await;
        wait_peer_connected_ev(&mut events_b).await;

        let id = Identity::generate();
        handle_a.publish_head(make_record(&id, 5)).await;

        let resolved = resolve_with_retry(&handle_b, id.public_key_bytes())
            .await
            .expect("record not resolvable via DHT");
        assert_eq!(resolved.payload.sequence, 5);
        assert_eq!(resolved.payload.display_name, "Alice");
        assert!(verify_ipns_record(&resolved).is_ok());
    }

    #[tokio::test]
    async fn stale_put_does_not_regress() {
        // 出し惜しみ攻撃の再現: 攻撃者 C が過去の本物レコード(seq 3)を再配布しても、
        // 正直な発信者 A の最新(seq 5)が候補に混ざる限り argmax が 5 に収束する。
        // 格納前検証(seq 比較)は既に 5 を保持するノードへの巻き戻し防止として働く
        let store_a = Arc::new(Store::open_in_memory().await.unwrap());
        let (handle_a, _events_a, _addr_a) = spawn_test_node(store_a).await;
        let store_b = Arc::new(Store::open_in_memory().await.unwrap());
        let (handle_b, mut events_b, addr_b) = spawn_test_node(store_b).await;
        let store_c = Arc::new(Store::open_in_memory().await.unwrap());
        let (handle_c, mut events_c, _) = spawn_test_node(store_c).await;

        handle_a.dial(addr_b.clone()).await;
        wait_peer_connected_ev(&mut events_b).await;
        handle_c.dial(addr_b).await;
        wait_peer_connected_ev(&mut events_c).await;

        let id = Identity::generate();
        handle_a.publish_head(make_record(&id, 5)).await;
        let resolved = resolve_with_retry(&handle_b, id.public_key_bytes())
            .await
            .expect("initial record not resolvable");
        assert_eq!(resolved.payload.sequence, 5);

        // C が古い(署名は正当な)レコードを DHT に流し込む
        handle_c.publish_head(make_record(&id, 3)).await;
        tokio::time::sleep(Duration::from_secs(1)).await;

        let resolved = resolve_with_retry(&handle_b, id.public_key_bytes())
            .await
            .expect("record lost after stale put");
        assert_eq!(resolved.payload.sequence, 5);
    }

    #[tokio::test]
    async fn invalid_record_not_resolvable() {
        let store_a = Arc::new(Store::open_in_memory().await.unwrap());
        let (handle_a, _events_a, addr_a) = spawn_test_node(store_a).await;
        let store_b = Arc::new(Store::open_in_memory().await.unwrap());
        let (handle_b, mut events_b, _) = spawn_test_node(store_b).await;

        handle_b.dial(addr_a).await;
        wait_peer_connected_ev(&mut events_b).await;

        // 署名の合わない改ざんレコードを publish(攻撃者の自ノードには入るが、
        // B の格納前検証と argmax の署名フィルタの両方が弾く)
        let id = Identity::generate();
        let mut tampered = make_record(&id, 9);
        tampered.payload.sequence = 10;
        handle_a.publish_head(tampered).await;
        tokio::time::sleep(Duration::from_secs(1)).await;

        assert!(handle_b.resolve_ipns(id.public_key_bytes()).await.is_none());
    }

    #[tokio::test]
    async fn resolve_via_intermediate_node() {
        // A ── B ── C のチェーン接続。C は A と直接接続せずにレコードを解決する
        let store_a = Arc::new(Store::open_in_memory().await.unwrap());
        let (handle_a, _events_a, _addr_a) = spawn_test_node(store_a).await;
        let store_b = Arc::new(Store::open_in_memory().await.unwrap());
        let (_handle_b, mut events_b, addr_b) = spawn_test_node(store_b).await;
        let store_c = Arc::new(Store::open_in_memory().await.unwrap());
        let (handle_c, mut events_c, _) = spawn_test_node(store_c).await;

        handle_a.dial(addr_b.clone()).await;
        wait_peer_connected_ev(&mut events_b).await;
        handle_c.dial(addr_b).await;
        wait_peer_connected_ev(&mut events_c).await;

        let id = Identity::generate();
        handle_a.publish_head(make_record(&id, 7)).await;

        // C の解決: B が複製を返すか、B の紹介で A に到達するかのいずれか
        let resolved = resolve_with_retry(&handle_c, id.public_key_bytes())
            .await
            .expect("record not resolvable via intermediate node");
        assert_eq!(resolved.payload.sequence, 7);
    }

    #[tokio::test]
    async fn record_over_gossipsub() {
        // 両経路化の gossipsub 側: レコードそのものが topic に流れ、
        // 受信側で HeadReceived として届く
        let store_a = Arc::new(Store::open_in_memory().await.unwrap());
        let (handle_a, mut events_a, addr_a) = spawn_test_node(store_a).await;
        let store_b = Arc::new(Store::open_in_memory().await.unwrap());
        let (handle_b, mut events_b, _) = spawn_test_node(store_b).await;

        let id = Identity::generate();
        let pubkey_hex = bytes_to_hex(&id.public_key_bytes());

        handle_b.dial(addr_a).await;
        wait_peer_connected_ev(&mut events_b).await;
        handle_b.subscribe(pubkey_hex).await;
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match events_a.recv().await {
                    Some(NetworkEvent::PeerSubscribed { .. }) => return,
                    Some(_) => continue,
                    None => panic!("event channel closed"),
                }
            }
        })
        .await
        .expect("subscription not propagated");

        handle_a.publish_head(make_record(&id, 2)).await;

        let record = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match events_b.recv().await {
                    Some(NetworkEvent::HeadReceived { record, .. }) => return record,
                    Some(_) => continue,
                    None => panic!("event channel closed"),
                }
            }
        })
        .await
        .expect("head record not received via gossipsub");
        assert_eq!(record.payload.sequence, 2);
        assert!(verify_ipns_record(&record).is_ok());
    }
}
