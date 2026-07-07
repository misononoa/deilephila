use cid::Cid;
use libp2p::PeerId;
use tracing::debug;

use crate::event::{verify_chain_link, verify_envelope, ChainError, EventEnvelope};
use crate::head::{record_to_bytes, verify_ipns_record, IpnsRecord};
use crate::network::NetworkHandle;
use crate::store::{Store, StoreError};
use crate::util::{bytes_to_hex, now_ms};

/// 1回の同期で辿るチェーン長の上限(不正な announce による暴走防止)。
const MAX_SYNC_DEPTH: usize = 10_000;

#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    #[error("invalid signature")]
    InvalidSignature,
    #[error("decode error: {0}")]
    Decode(String),
    /// チェーン内のイベントの author が announce の pubkey と一致しない
    #[error("author mismatch at {cid}")]
    AuthorMismatch { cid: Cid },
    /// prev を辿った先の seq が期待値(1ずつ減少)と一致しない
    #[error("seq mismatch at {cid}: expected {expected}, got {got}")]
    SeqMismatch { cid: Cid, expected: u64, got: u64 },
    /// genesis(seq=0) に prev がある / 非 genesis に prev がない
    #[error("broken chain at {cid}")]
    BrokenChain { cid: Cid },
    /// ネットワークからブロックを取得できなかった
    #[error("block unavailable: {0}")]
    BlockUnavailable(Cid),
    #[error("chain exceeds max sync depth")]
    TooDeep,
    #[error("store error: {0}")]
    Store(#[from] StoreError),
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct SyncOutcome {
    /// 新たに取り込んだイベント数(0 = stale announce だった)
    pub new_events: usize,
}

/// IPNS-headレコードを起点にチェーンを同期する(タイムライン構築の中核)。
/// gossipsub 受信・DHT resolve・(M6 の)フォローグラフ探索のどのソース由来の
/// レコードもこの1関数に合流する。
///
/// Swarm ループの**外**(core 側タスク)から呼ぶこと。`get_block` は mpsc で
/// Swarm ループへ往復するため、ループ内から呼ぶとデッドロックする。
///
/// 1. レコードの署名を検証(payload 内の `name` による自己完結検証)
/// 2. レコード自体を保存 + 表示名スナップショットを反映。チェーン取得の成否に
///    依存させない: ブロックが取れなくてもポインタと表示名は生かす
///    (可用性の床、networking.md §3.2)
/// 3. stale 判定: 既知 max seq 以下かつ head ブロック取得済みなら no-op
/// 4. 後方走査: head から `prev` を辿り、未知ブロックを収集・検証
///    (署名・author 一致・seq 連続性。CID 一致は network 層で検証済み)
/// 5. 前方挿入: Edit/Delete の projection 空振りを防ぐため **seq 昇順**で insert
/// 6. accounts.latest_head_cid を更新
pub async fn handle_head_record(
    store: &Store,
    network: &NetworkHandle,
    record: &IpnsRecord,
    source: Option<PeerId>,
) -> Result<SyncOutcome, SyncError> {
    verify_ipns_record(record).map_err(|_| SyncError::InvalidSignature)?;

    let author_hex = bytes_to_hex(record.payload.name.as_ref());
    let head_cid_str = record.payload.value.to_string();

    store
        .upsert_head_record(
            &author_hex,
            record.payload.sequence,
            &record_to_bytes(record),
            now_ms(),
        )
        .await?;
    if !record.payload.display_name.is_empty() {
        store
            .fill_display_name_snapshot(&author_hex, &record.payload.display_name)
            .await?;
    }

    if let Some((known_seq, _)) = store.get_head(&author_hex).await? {
        if record.payload.sequence <= known_seq
            && store.get_raw_block(&head_cid_str).await?.is_some()
        {
            return Ok(SyncOutcome { new_events: 0 });
        }
    }

    // 後方走査(新しい順に収集)
    let mut fetched: Vec<EventEnvelope> = Vec::new();
    let mut cursor = Some(record.payload.value.clone());
    let mut expected_seq = record.payload.sequence;

    while let Some(cid) = cursor {
        if store.get_raw_block(&cid.to_string()).await?.is_some() {
            break; // 既知ブロックに到達 = それ以前は取り込み済み
        }
        if fetched.len() >= MAX_SYNC_DEPTH {
            return Err(SyncError::TooDeep);
        }

        let raw = network
            .get_block(cid.clone(), source)
            .await
            .ok_or_else(|| SyncError::BlockUnavailable(cid.clone()))?;
        let envelope: EventEnvelope =
            serde_ipld_dagcbor::from_slice(&raw).map_err(|e| SyncError::Decode(e.to_string()))?;

        verify_envelope(&envelope).map_err(|_| SyncError::InvalidSignature)?;
        verify_chain_link(&envelope, record.payload.name.as_ref(), expected_seq).map_err(
            |e| match e {
                ChainError::WrongAuthor => SyncError::AuthorMismatch { cid: cid.clone() },
                ChainError::WrongSeq { expected, got } => SyncError::SeqMismatch {
                    cid: cid.clone(),
                    expected,
                    got,
                },
                ChainError::BrokenPrev => SyncError::BrokenChain { cid: cid.clone() },
            },
        )?;

        cursor = envelope.payload.prev.clone();
        expected_seq = expected_seq.saturating_sub(1);
        fetched.push(envelope);
    }

    // 前方挿入(seq 昇順 = 収集の逆順)
    for envelope in fetched.iter().rev() {
        store.insert_event(envelope).await?;
    }

    store.update_head_cid(&author_hex, &head_cid_str).await?;

    debug!(author = %author_hex, new_events = fetched.len(), "chain synced");
    Ok(SyncOutcome {
        new_events: fetched.len(),
    })
}

// --- テスト ---

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{envelope_cid, EventKind};
    use crate::util::bytes_to_cid;
    use crate::head::{create_ipns_record, RECORD_LIFETIME_MS};
    use crate::identity::{create_envelope, Identity};
    use crate::network::{spawn_swarm_loop, NetworkEvent};
    use libp2p::Multiaddr;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::mpsc;

    /// テスト用の最小レコード(プロフィールスナップショットなし)。
    fn make_record(identity: &Identity, sequence: u64, head_cid: Cid) -> IpnsRecord {
        create_ipns_record(
            identity,
            sequence,
            head_cid,
            now_ms() + RECORD_LIFETIME_MS,
            None,
            String::new(),
        )
    }

    /// Post×2 + Edit の3イベントチェーンを store に入れる。
    /// Edit を含むのは「seq 昇順挿入で projection が正しく更新されること」を
    /// 受信側で検証するため。
    async fn seed_chain(store: &Store) -> (Identity, Cid, u64) {
        let id = Identity::generate();
        let e0 = create_envelope(
            &id,
            0,
            None,
            EventKind::Post {
                text: "first".to_string(),
            },
        );
        let c0 = envelope_cid(&e0);
        let e1 = create_envelope(
            &id,
            1,
            Some(c0.clone()),
            EventKind::Post {
                text: "second".to_string(),
            },
        );
        let c1 = envelope_cid(&e1);
        let e2 = create_envelope(
            &id,
            2,
            Some(c1),
            EventKind::Edit {
                target: c0,
                text: "first (edited)".to_string(),
            },
        );
        let c2 = envelope_cid(&e2);
        for e in [&e0, &e1, &e2] {
            store.insert_event(e).await.unwrap();
        }
        (id, c2, 2)
    }

    /// コマンド受信側を持たない NetworkHandle(ネットワーク不要のテスト用)。
    fn dummy_network() -> NetworkHandle {
        let (tx, _rx) = mpsc::channel(1);
        NetworkHandle::new(tx)
    }

    async fn spawn_test_node(
        store: Arc<Store>,
    ) -> (NetworkHandle, mpsc::Receiver<NetworkEvent>, Multiaddr) {
        let listen: Multiaddr = "/ip4/127.0.0.1/tcp/0".parse().unwrap();
        spawn_swarm_loop(store, Some(listen))
            .await
            .expect("swarm failed to start")
    }

    async fn wait_for(
        rx: &mut mpsc::Receiver<NetworkEvent>,
        pred: impl Fn(&NetworkEvent) -> bool,
    ) -> NetworkEvent {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let ev = rx.recv().await.expect("event channel closed");
                if pred(&ev) {
                    return ev;
                }
            }
        })
        .await
        .expect("timed out waiting for network event")
    }

    #[tokio::test]
    async fn full_chain_sync_via_gossipsub() {
        // A: 3イベントのチェーンを持つ発信者
        let store_a = Arc::new(Store::open_in_memory().await.unwrap());
        let (identity_a, head_cid, head_seq) = seed_chain(&store_a).await;
        let pubkey_hex = bytes_to_hex(&identity_a.public_key_bytes());
        let (handle_a, mut events_a, addr_a) = spawn_test_node(Arc::clone(&store_a)).await;

        // B: 空のフォロワー
        let store_b = Arc::new(Store::open_in_memory().await.unwrap());
        let (handle_b, mut events_b, _) = spawn_test_node(Arc::clone(&store_b)).await;

        // B → A 接続後、A の feed トピックを購読
        handle_b.dial(addr_a).await;
        wait_for(&mut events_b, |e| {
            matches!(e, NetworkEvent::PeerConnected(_))
        })
        .await;
        handle_b.subscribe(pubkey_hex.clone()).await;

        // A: B の購読が伝わるのを待ってから publish(gossipsub の購読情報は接続上で交換される)
        wait_for(&mut events_a, |e| {
            matches!(e, NetworkEvent::PeerSubscribed { .. })
        })
        .await;
        let record = create_ipns_record(
            &identity_a,
            head_seq,
            head_cid.clone(),
            now_ms() + RECORD_LIFETIME_MS,
            None,
            "Alice".to_string(),
        );
        handle_a.publish_head(record).await;

        // B: HeadReceived を受けて同期実行
        let ev = wait_for(&mut events_b, |e| {
            matches!(e, NetworkEvent::HeadReceived { .. })
        })
        .await;
        let NetworkEvent::HeadReceived { record, source } = ev else {
            unreachable!()
        };
        let outcome = handle_head_record(&store_b, &handle_b, &record, Some(source))
            .await
            .unwrap();
        assert_eq!(outcome.new_events, 3);

        // 過去分も含めチェーン全体が取り込まれ、Edit の projection が正しく適用されている
        let posts = store_b.get_posts_by_author(&pubkey_hex).await.unwrap();
        assert_eq!(posts.len(), 2);
        let edited = posts
            .iter()
            .find(|p| p.edited)
            .expect("edited post missing");
        assert_eq!(edited.text, "first (edited)");

        // head が accounts に記録されている
        let account = store_b.get_account(&pubkey_hex).await.unwrap().unwrap();
        assert_eq!(account.latest_head_cid, Some(head_cid.to_string()));

        // レコード自体も常時保持され(R2)、表示名スナップショットが反映されている
        // (チェーンに Profile イベントがないため Tier 0 が埋める)
        let (stored_seq, _) = store_b.get_head_record(&pubkey_hex).await.unwrap().unwrap();
        assert_eq!(stored_seq, head_seq);
        assert_eq!(account.display_name, "Alice");
    }

    #[tokio::test]
    async fn tampered_record_rejected() {
        let store = Store::open_in_memory().await.unwrap();
        let id = Identity::generate();
        let mut record = make_record(&id, 5, bytes_to_cid(b"head"));
        record.payload.sequence = 6; // 改ざん

        let err = handle_head_record(&store, &dummy_network(), &record, None)
            .await
            .unwrap_err();
        assert!(matches!(err, SyncError::InvalidSignature));

        // store は無変更(レコードも保存されない)
        let author_hex = bytes_to_hex(&id.public_key_bytes());
        assert!(store.get_head(&author_hex).await.unwrap().is_none());
        assert!(store.get_head_record(&author_hex).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn stale_record_is_noop() {
        let store = Store::open_in_memory().await.unwrap();
        let (id, head_cid, head_seq) = seed_chain(&store).await;

        // 既知 seq 以下のレコード → ネットワークに触れず no-op
        // (dummy_network で成功すること自体が「取得を試みていない」ことの証明)
        let record = make_record(&id, head_seq, head_cid);
        let outcome = handle_head_record(&store, &dummy_network(), &record, None)
            .await
            .unwrap();
        assert_eq!(outcome.new_events, 0);
    }

    #[tokio::test]
    async fn unavailable_block_keeps_record() {
        let store = Store::open_in_memory().await.unwrap();
        let id = Identity::generate();
        // 署名は正しいが、指す先のブロックがどこにもないレコード
        let record = make_record(&id, 0, bytes_to_cid(b"missing"));

        let err = handle_head_record(&store, &dummy_network(), &record, None)
            .await
            .unwrap_err();
        assert!(matches!(err, SyncError::BlockUnavailable(_)));

        // チェーン取得に失敗してもポインタは保持される(可用性の床)
        let author_hex = bytes_to_hex(&id.public_key_bytes());
        assert!(store.get_head_record(&author_hex).await.unwrap().is_some());
    }

    /// M5c の中核シナリオ: gossipsub を購読していない後発フォロワーが、
    /// DHT resolve → handle_head_record でチェーン全体と表示名を取得する
    /// (発信者の publish を受信時に B はオフライン相当 = 未接続・未購読)。
    #[tokio::test]
    async fn late_follower_syncs_via_dht() {
        // A: チェーンを持つ発信者。record を DHT へ publish する
        let store_a = Arc::new(Store::open_in_memory().await.unwrap());
        let (identity_a, head_cid, head_seq) = seed_chain(&store_a).await;
        let pubkey_hex = bytes_to_hex(&identity_a.public_key_bytes());
        let (handle_a, _events_a, addr_a) = spawn_test_node(Arc::clone(&store_a)).await;
        let record = create_ipns_record(
            &identity_a,
            head_seq,
            head_cid.clone(),
            now_ms() + RECORD_LIFETIME_MS,
            None,
            "Alice".to_string(),
        );
        handle_a.publish_head(record).await;

        // B: 後から接続する空のフォロワー(gossipsub 購読なし)
        let store_b = Arc::new(Store::open_in_memory().await.unwrap());
        let (handle_b, mut events_b, _) = spawn_test_node(Arc::clone(&store_b)).await;
        handle_b.dial(addr_a).await;
        wait_for(&mut events_b, |e| {
            matches!(e, NetworkEvent::PeerConnected(_))
        })
        .await;

        // DHT からレコードを解決(ルーティングテーブル形成を待ってリトライ)
        let mut resolved = None;
        for _ in 0..40 {
            if let Some(r) = handle_b.resolve_ipns(identity_a.public_key_bytes()).await {
                resolved = Some(r);
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        let resolved = resolved.expect("record not resolvable via DHT");

        // 解決したレコードからチェーン全体を同期
        let outcome = handle_head_record(&store_b, &handle_b, &resolved, None)
            .await
            .unwrap();
        assert_eq!(outcome.new_events, 3);

        let posts = store_b.get_posts_by_author(&pubkey_hex).await.unwrap();
        assert_eq!(posts.len(), 2);
        let account = store_b.get_account(&pubkey_hex).await.unwrap().unwrap();
        assert_eq!(account.latest_head_cid, Some(head_cid.to_string()));
        assert_eq!(account.display_name, "Alice");
    }
}
