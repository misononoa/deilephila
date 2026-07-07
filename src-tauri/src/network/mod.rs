pub mod protocol;

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use cid::Cid;
use futures::StreamExt;
use libp2p::{
    gossipsub, identify, kad, mdns,
    request_response::{self, OutboundRequestId, ProtocolSupport},
    swarm::SwarmEvent,
    Multiaddr, PeerId, StreamProtocol,
};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, warn};

use crate::{
    head::{
        feed_topic_str, record_from_bytes, record_to_bytes, select_best, verify_ipns_record,
        IpnsRecord,
    },
    network::protocol::{BlockExchangeCodec, BlockExchangeProtocol, BlockResponse, WantBlock},
    store::Store,
    util::{bytes_to_hex, now_ms},
};

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

// --- NetworkBehaviour ---

#[derive(libp2p::swarm::NetworkBehaviour)]
struct DeilephilaBehaviour {
    identify: identify::Behaviour,
    mdns: mdns::tokio::Behaviour,
    exchange: request_response::Behaviour<BlockExchangeCodec>,
    gossipsub: gossipsub::Behaviour,
    kademlia: kad::Behaviour<kad::store::MemoryStore>,
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

    tokio::spawn(run_swarm_loop(
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

async fn run_swarm_loop(
    store: Arc<Store>,
    mut cmd_rx: mpsc::Receiver<NetworkCommand>,
    event_tx: mpsc::Sender<NetworkEvent>,
    addr_tx: oneshot::Sender<Multiaddr>,
    listen_addr: Multiaddr,
) {
    let mut swarm = match build_swarm() {
        Ok(s) => s,
        Err(e) => {
            warn!("swarm build failed: {e}");
            return;
        }
    };

    // NAT越え(M7)導入までは常にレコードを保持・応答するサーバーモードで動かす
    // (既定の自動判定は確認済み外部アドレスを要求し、LAN/ループバックでは
    // クライアントモードに留まって put/get に応答しないため)
    swarm
        .behaviour_mut()
        .kademlia
        .set_mode(Some(kad::Mode::Server));

    if let Err(e) = swarm.listen_on(listen_addr) {
        warn!("listen failed: {e}");
        return;
    }

    let mut addr_tx_opt = Some(addr_tx);
    // request_id → (expected_cid, reply_channel)
    let mut pending: HashMap<OutboundRequestId, (Cid, oneshot::Sender<Option<Vec<u8>>>)> =
        HashMap::new();
    // DHT get_record クエリ → (reply_channel, 収集済み候補)
    let mut pending_resolves: HashMap<
        kad::QueryId,
        (oneshot::Sender<Vec<IpnsRecord>>, Vec<IpnsRecord>),
    > = HashMap::new();

    loop {
        tokio::select! {
            cmd = cmd_rx.recv() => match cmd {
                None => break,
                Some(NetworkCommand::Dial(addr)) => {
                    let _ = swarm.dial(addr);
                }
                Some(NetworkCommand::GetBlock { cid, prefer, reply }) => {
                    let peers: Vec<PeerId> = swarm.connected_peers().cloned().collect();
                    // prefer が接続中ならそれを、なければ任意の接続先を選ぶ
                    let target = prefer
                        .filter(|p| peers.contains(p))
                        .or_else(|| peers.first().cloned());
                    let Some(peer) = target else {
                        let _ = reply.send(None);
                        continue;
                    };
                    let req = WantBlock { cid_bytes: cid.to_bytes() };
                    let req_id = swarm.behaviour_mut().exchange.send_request(&peer, req);
                    pending.insert(req_id, (cid, reply));
                }
                Some(NetworkCommand::PublishHead(record)) => {
                    publish_head_record(&mut swarm, &record);
                }
                Some(NetworkCommand::ResolveIpns { pubkey, reply }) => {
                    let query_id = swarm
                        .behaviour_mut()
                        .kademlia
                        .get_record(head_record_key(&pubkey));
                    pending_resolves.insert(query_id, (reply, Vec::new()));
                }
                Some(NetworkCommand::Subscribe { pubkey_hex }) => {
                    let topic = gossipsub::IdentTopic::new(feed_topic_str(&pubkey_hex));
                    if let Err(e) = swarm.behaviour_mut().gossipsub.subscribe(&topic) {
                        warn!("subscribe failed: {e}");
                    }
                }
                Some(NetworkCommand::Unsubscribe { pubkey_hex }) => {
                    let topic = gossipsub::IdentTopic::new(feed_topic_str(&pubkey_hex));
                    let _ = swarm.behaviour_mut().gossipsub.unsubscribe(&topic);
                }
            },

            event = swarm.next() => match event {
                None => break,
                Some(SwarmEvent::NewListenAddr { address, .. }) => {
                    info!("libp2p listening on {address}");
                    if let Some(tx) = addr_tx_opt.take() {
                        let _ = tx.send(address);
                    }
                }
                Some(SwarmEvent::ConnectionEstablished { peer_id, .. }) => {
                    info!("connected to peer {peer_id}");
                    let _ = event_tx.try_send(NetworkEvent::PeerConnected(peer_id));
                }
                Some(SwarmEvent::Behaviour(DeilephilaBehaviourEvent::Mdns(
                    mdns::Event::Discovered(peers),
                ))) => {
                    for (peer_id, addr) in peers {
                        info!("mDNS discovered peer {peer_id} at {addr}");
                        swarm.add_peer_address(peer_id, addr.clone());
                        // DHT のルーティングテーブルにも載せる(kad は自前のテーブルを持つ)
                        swarm.behaviour_mut().kademlia.add_address(&peer_id, addr);
                        // 発見しただけでは接続されないため、明示的に dial する。
                        // PeerId 指定なら登録済みアドレスが使われ、接続済み/dial 中なら no-op
                        if let Err(e) = swarm.dial(peer_id) {
                            warn!("dial to discovered peer {peer_id} failed: {e}");
                        }
                        let _ = event_tx.try_send(NetworkEvent::PeerDiscovered(peer_id));
                    }
                }
                Some(SwarmEvent::Behaviour(DeilephilaBehaviourEvent::Exchange(
                    request_response::Event::Message { message, .. },
                ))) => match message {
                    request_response::Message::Request {
                        request, channel, ..
                    } => {
                        let resp = match Cid::try_from(request.cid_bytes.as_slice()) {
                            Ok(cid) => match store.get_raw_block(&cid.to_string()).await {
                                Ok(Some(data)) => BlockResponse::Found { data },
                                _ => BlockResponse::NotFound,
                            },
                            Err(_) => BlockResponse::NotFound,
                        };
                        let _ = swarm.behaviour_mut().exchange.send_response(channel, resp);
                    }
                    request_response::Message::Response {
                        request_id,
                        response,
                    } => {
                        if let Some((expected_cid, reply)) = pending.remove(&request_id) {
                            handle_block_response(response, expected_cid, reply);
                        }
                    }
                },
                Some(SwarmEvent::Behaviour(DeilephilaBehaviourEvent::Identify(
                    identify::Event::Received { peer_id, info, .. },
                ))) => {
                    // 直接 dial で接続したピア(mDNS 経由でない)も DHT に参加させる。
                    // identify が伝えるリッスンアドレスをルーティングテーブルへ登録する
                    for addr in info.listen_addrs {
                        swarm.behaviour_mut().kademlia.add_address(&peer_id, addr);
                    }
                }
                Some(SwarmEvent::Behaviour(DeilephilaBehaviourEvent::Kademlia(ev))) => {
                    handle_kad_event(&mut swarm, ev, &mut pending_resolves);
                }
                Some(SwarmEvent::Behaviour(DeilephilaBehaviourEvent::Gossipsub(ev))) => match ev {
                    gossipsub::Event::Message {
                        propagation_source,
                        message,
                        ..
                    } => match record_from_bytes(&message.data) {
                        Ok(record) => {
                            // source は転送元(直接接続中のピア)。レコードの真正性は
                            // payload 内の署名で別途検証されるため、ここでは検証しない
                            let _ = event_tx.try_send(NetworkEvent::HeadReceived {
                                record,
                                source: propagation_source,
                            });
                        }
                        Err(e) => {
                            warn!("undecodable gossipsub message from {propagation_source}: {e}");
                        }
                    },
                    gossipsub::Event::Subscribed { peer_id, topic } => {
                        let _ = event_tx.try_send(NetworkEvent::PeerSubscribed {
                            peer: peer_id,
                            topic: topic.into_string(),
                        });
                    }
                    _ => {}
                },
                Some(SwarmEvent::Behaviour(DeilephilaBehaviourEvent::Exchange(
                    request_response::Event::OutboundFailure {
                        request_id, error, ..
                    },
                ))) => {
                    warn!("outbound request failed: {error}");
                    if let Some((_, reply)) = pending.remove(&request_id) {
                        let _ = reply.send(None);
                    }
                }
                _ => {}
            }
        }
    }
}

/// 受信ブロックの CID 一致を検証して返す。
/// 永続化は行わない: チェーン同期では Edit/Delete が対象 Post より先に届きうるため、
/// 挿入順の制御(seq 昇順)は同期エンジン(sync.rs)が担う。
fn handle_block_response(
    response: BlockResponse,
    expected_cid: Cid,
    reply: oneshot::Sender<Option<Vec<u8>>>,
) {
    match response {
        BlockResponse::NotFound => {
            let _ = reply.send(None);
        }
        BlockResponse::Found { data } => {
            // CID 検証: 受信データから再計算したCIDが期待値と一致するか
            let computed = crate::util::bytes_to_cid(&data);
            if computed != expected_cid {
                warn!("CID mismatch: expected {expected_cid}, got {computed}");
                let _ = reply.send(None);
                return;
            }
            let _ = reply.send(Some(data));
        }
    }
}

// --- IPNS-headレコードの DHT 搬送(networking.md §4.2) ---

/// アカウント公開鍵から DHT レコードキーを導出する。
/// IPNS名 = アカウント公開鍵(data-model.md §2.4)の kad 上の表現
fn head_record_key(pubkey: &[u8; 32]) -> kad::RecordKey {
    kad::RecordKey::new(&format!("/deilephila/head/{}", bytes_to_hex(pubkey)))
}

/// レコードの validity(EOL、Unix epoch ミリ秒)を kad ローカル store の
/// 失効時刻へ換算する。EOL を過ぎたレコードは DHT から自然に消え、
/// 生存させるには発信者の定期 republish が要る(networking.md §4.2)
fn expires_from_validity(validity_ms: i64) -> Option<Instant> {
    let ttl_ms = validity_ms.saturating_sub(now_ms()).max(0) as u64;
    Some(Instant::now() + Duration::from_millis(ttl_ms))
}

/// IPNS-headレコードを gossipsub(即時)と DHT(永続)の両経路へ流す。
fn publish_head_record(swarm: &mut libp2p::Swarm<DeilephilaBehaviour>, record: &IpnsRecord) {
    let data = record_to_bytes(record);

    let topic_str = feed_topic_str(&bytes_to_hex(record.payload.name.as_ref()));
    let topic = gossipsub::IdentTopic::new(topic_str);
    if let Err(e) = swarm.behaviour_mut().gossipsub.publish(topic, data.clone()) {
        // 購読者がいない場合の InsufficientPeers 等。fire-and-forget なので警告のみ
        warn!("head record gossip publish failed: {e}");
    }

    let kad_record = kad::Record {
        key: head_record_key(record.payload.name.as_ref()),
        value: data,
        publisher: None,
        expires: expires_from_validity(record.payload.validity),
    };
    // put_record はまずローカル store に入れてから複製クエリを開始する。
    // ピア不在などのクエリ失敗は OutboundQueryProgressed 側で警告する
    if let Err(e) = swarm
        .behaviour_mut()
        .kademlia
        .put_record(kad_record, kad::Quorum::One)
    {
        warn!("head record dht put failed: {e:?}");
    }
}

/// 他ノードから put されたレコードを store へ格納する前に検証する
/// (`StoreInserts::FilterBoth` の受理判定)。受理条件:
/// - `IpnsRecord` としてデコードでき、署名検証に成功する(自己完結検証)
/// - キーが payload の `name` から導出したものと一致する(他人のキーの汚染を拒否)
/// - 既に保持しているレコードの sequence を上回る(stale put による巻き戻しを拒否)
/// 受理時は validity から失効時刻を再計算したレコードを返す
/// (送信者申告の expires を信用しない)。
fn validate_inbound_head_record(
    record: &kad::Record,
    existing_seq: Option<u64>,
) -> Result<kad::Record, String> {
    let decoded = record_from_bytes(&record.value).map_err(|e| format!("undecodable: {e}"))?;
    verify_ipns_record(&decoded).map_err(|_| "invalid signature".to_string())?;
    if record.key != head_record_key(decoded.payload.name.as_ref()) {
        return Err("key does not match record name".to_string());
    }
    if let Some(known) = existing_seq {
        if decoded.payload.sequence <= known {
            return Err(format!(
                "stale sequence {} (known {known})",
                decoded.payload.sequence
            ));
        }
    }
    let mut accepted = record.clone();
    accepted.expires = expires_from_validity(decoded.payload.validity);
    Ok(accepted)
}

fn handle_kad_event(
    swarm: &mut libp2p::Swarm<DeilephilaBehaviour>,
    ev: kad::Event,
    pending_resolves: &mut HashMap<
        kad::QueryId,
        (oneshot::Sender<Vec<IpnsRecord>>, Vec<IpnsRecord>),
    >,
) {
    use libp2p::kad::store::RecordStore;

    match ev {
        // FilterBoth 設定により、他ノードからの put はここで検証してから手動格納する
        kad::Event::InboundRequest {
            request:
                kad::InboundRequest::PutRecord {
                    source,
                    record: Some(record),
                    ..
                },
        } => {
            let existing_seq = swarm
                .behaviour_mut()
                .kademlia
                .store_mut()
                .get(&record.key)
                .and_then(|existing| record_from_bytes(&existing.value).ok())
                .map(|r| r.payload.sequence);
            match validate_inbound_head_record(&record, existing_seq) {
                Ok(accepted) => {
                    if let Err(e) = swarm.behaviour_mut().kademlia.store_mut().put(accepted) {
                        warn!("head record store failed: {e:?}");
                    }
                }
                Err(reason) => {
                    debug!("rejected head record from {source}: {reason}");
                }
            }
        }
        kad::Event::OutboundQueryProgressed {
            id, result, step, ..
        } => {
            match result {
                kad::QueryResult::GetRecord(res) => {
                    if let Some((_, candidates)) = pending_resolves.get_mut(&id) {
                        match res {
                            Ok(kad::GetRecordOk::FoundRecord(peer_record)) => {
                                // デコード不能な候補は落とす。署名検証と argmax 選択は
                                // NetworkHandle::resolve_ipns 側で行う
                                if let Ok(decoded) = record_from_bytes(&peer_record.record.value)
                                {
                                    candidates.push(decoded);
                                }
                            }
                            Ok(kad::GetRecordOk::FinishedWithNoAdditionalRecord { .. }) => {}
                            Err(e) => debug!("dht get_record: {e}"),
                        }
                    }
                    // クエリ終端で収集済み候補を返す(候補ゼロ = 空 Vec)
                    if step.last {
                        if let Some((reply, candidates)) = pending_resolves.remove(&id) {
                            let _ = reply.send(candidates);
                        }
                    }
                }
                kad::QueryResult::PutRecord(Err(e)) => {
                    // ローカル store には格納済み。複製失敗のみの警告
                    warn!("dht put replication failed: {e}");
                }
                _ => {}
            }
        }
        _ => {}
    }
}

fn build_swarm(
) -> Result<libp2p::Swarm<DeilephilaBehaviour>, Box<dyn std::error::Error + Send + Sync>> {
    let swarm = libp2p::SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            libp2p::tcp::Config::default(),
            libp2p::noise::Config::new,
            libp2p::yamux::Config::default,
        )?
        .with_behaviour(|key| {
            let identify = identify::Behaviour::new(identify::Config::new(
                "/deilephila/1.0.0".to_string(),
                key.public(),
            ));
            let mdns =
                mdns::tokio::Behaviour::new(mdns::Config::default(), key.public().to_peer_id())?;
            let exchange = request_response::Behaviour::new(
                [(BlockExchangeProtocol, ProtocolSupport::Full)],
                request_response::Config::default(),
            );
            // メッセージはピア鍵で署名(transport 層)。head 通知自体の真正性は
            // アカウント鍵の署名(アプリ層)で別途保証される
            let gossipsub = gossipsub::Behaviour::new(
                gossipsub::MessageAuthenticity::Signed(key.clone()),
                gossipsub::Config::default(),
            )?;
            // FilterBoth: 他ノードからの put をそのまま store に入れず、
            // InboundRequest::PutRecord で検証してから手動で格納する(§4.2)
            let mut kad_config = kad::Config::new(StreamProtocol::new("/deilephila/kad/1.0.0"));
            kad_config.set_record_filtering(kad::StoreInserts::FilterBoth);
            let peer_id = key.public().to_peer_id();
            let kademlia = kad::Behaviour::with_config(
                peer_id,
                kad::store::MemoryStore::new(peer_id),
                kad_config,
            );
            Ok(DeilephilaBehaviour {
                identify,
                mdns,
                exchange,
                gossipsub,
                kademlia,
            })
        })?
        .build();
    Ok(swarm)
}

// --- テスト ---

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        event::{envelope_cid, EventEnvelope, EventKind},
        identity::{create_envelope, Identity},
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

    use crate::util::bytes_to_cid;
    use crate::head::create_ipns_record;

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

    #[test]
    fn inbound_record_validation_rules() {
        let id = Identity::generate();
        let pubkey = id.public_key_bytes();
        let record = make_record(&id, 5);
        let kad_record = kad::Record {
            key: head_record_key(&pubkey),
            value: record_to_bytes(&record),
            publisher: None,
            expires: None,
        };

        // 新規(既知レコードなし)は受理され、validity 由来の失効時刻が付く
        let accepted = validate_inbound_head_record(&kad_record, None).unwrap();
        assert!(accepted.expires.is_some());

        // 既知 seq を上回れば受理、同じ・下回るは stale として拒否
        assert!(validate_inbound_head_record(&kad_record, Some(4)).is_ok());
        assert!(validate_inbound_head_record(&kad_record, Some(5)).is_err());
        assert!(validate_inbound_head_record(&kad_record, Some(6)).is_err());

        // 改ざんレコード(署名不一致)は拒否
        let mut tampered = record.clone();
        tampered.payload.sequence = 9;
        let bad = kad::Record {
            key: head_record_key(&pubkey),
            value: record_to_bytes(&tampered),
            publisher: None,
            expires: None,
        };
        assert!(validate_inbound_head_record(&bad, None).is_err());

        // 別アカウントのキーへの格納(キー汚染)は拒否
        let other = Identity::generate();
        let wrong_key = kad::Record {
            key: head_record_key(&other.public_key_bytes()),
            value: record_to_bytes(&record),
            publisher: None,
            expires: None,
        };
        assert!(validate_inbound_head_record(&wrong_key, None).is_err());

        // デコード不能なゴミは拒否
        let garbage = kad::Record {
            key: head_record_key(&pubkey),
            value: b"not cbor \xff".to_vec(),
            publisher: None,
            expires: None,
        };
        assert!(validate_inbound_head_record(&garbage, None).is_err());
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
