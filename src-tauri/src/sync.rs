use cid::Cid;
use libp2p::PeerId;
use tracing::debug;

use crate::event::{verify_chain_link, verify_envelope, ChainError, EventEnvelope};
use crate::head::{verify_ipns_record, IpnsRecord};
use crate::network::NetworkHandle;
use crate::store::{Store, StoreError, SyncStateRow};
use crate::util::{bytes_to_hex, now_ms};

/// 初回フォロー時に遡行取得する最新イベント件数の上限(同期窓)。
/// ユーザー設定化は M8 の保持量設定に統合する([mvp.md] §3)。
const SYNC_WINDOW: u64 = 500;
/// 遡行収集→挿入の単位。チャンク内は反転して seq 昇順で挿入する。
const SYNC_CHUNK: usize = 100;
/// 1回の同期呼び出しで取得するイベント数の上限。不正な announce による暴走防止に
/// 加え、直列な消費ループを1つの author が長時間占有しないための応答性の担保。
/// 超過はエラーではなく中断で、次回の announce/resolve が続きを再開する。
const SYNC_BUDGET: usize = 1_000;

/// 窓・チャンク・予算の束。テストが縮小値を注入するための分離で、
/// 本番経路は `Default`(上記定数)を使う。
#[derive(Debug, Clone, Copy)]
pub struct SyncLimits {
    pub window: u64,
    pub chunk: usize,
    pub budget: usize,
}

impl Default for SyncLimits {
    fn default() -> Self {
        SyncLimits {
            window: SYNC_WINDOW,
            chunk: SYNC_CHUNK,
            budget: SYNC_BUDGET,
        }
    }
}

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
    #[error("store error: {0}")]
    Store(#[from] StoreError),
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct SyncOutcome {
    /// 新たに取り込んだイベント数
    pub new_events: usize,
    /// 遡行が genesis または窓下限まで到達済みか。
    /// false = 未取得区間が残っており、次回の announce/resolve で再開される
    pub completed: bool,
    /// この同期で新たに記録された fork の件数(head 層 + イベント層)。
    /// 呼び出し側(app.rs)が UI への警告通知に使う
    pub forks_detected: usize,
}

/// 遡行の停止理由。ブロック取得失敗と予算切れはエラーではなく停止として扱う
/// (可用性は無保証のため。[networking.md] §4.4)。
enum StopKind {
    /// genesis(prev なし)に到達
    Genesis,
    /// 取得済みブロックに到達(既存の取得済み区間に合流)
    KnownBlock,
    /// 窓下限に到達
    Floor,
    /// 予算切れ
    Budget,
    /// ブロックを取得できなかった
    Unavailable,
    /// 検証失敗(呼び出し元で Err に変換。そのチェーンの続きは信用しない)
    Invalid(SyncError),
}

impl StopKind {
    /// この停止の後、同じ呼び出し内でさらに遡行を試みてよいか
    fn can_continue(&self) -> bool {
        matches!(
            self,
            StopKind::Genesis | StopKind::KnownBlock | StopKind::Floor
        )
    }
}

/// IPNS-headレコードを起点にチェーンを同期する(タイムライン構築の中核)。
/// gossipsub 受信・DHT resolve・(M6 の)フォローグラフ探索のどのソース由来の
/// レコードもこの1関数に合流する。
///
/// Swarm ループの**外**(core 側タスク)から呼ぶこと。`get_block` は mpsc で
/// Swarm ループへ往復するため、ループ内から呼ぶとデッドロックする。
pub async fn handle_head_record(
    store: &Store,
    network: &NetworkHandle,
    record: &IpnsRecord,
    source: Option<PeerId>,
) -> Result<SyncOutcome, SyncError> {
    handle_head_record_with_limits(store, network, record, source, SyncLimits::default()).await
}

/// `handle_head_record` の実体(テストは縮小した `SyncLimits` を注入する)。
///
/// 1. レコードの署名を検証(payload 内の `name` による自己完結検証)
/// 2. レコード自体を保存 + 表示名スナップショットを反映。チェーン取得の成否に
///    依存させない: ブロックが取れなくてもポインタと表示名は生かす
///    (可用性の床、networking.md §3.2)
/// 3. 窓下限(floor)の決定: 1件でも取得済みなら凍結、未取得なら最新 head 基準
/// 4. stale 判定: 既知 max seq 以下 かつ head ブロック取得済み かつ 遡行完了済み
///    なら no-op(未完了なら 6 の再開へ進む)
/// 5. Phase A(前方追いつき): head から `prev` を遡行し、チャンクごとに検証して
///    反転挿入(チャンク内 seq 昇順)。取得失敗・予算切れは挿入済み分を残して中断
/// 6. Phase B(後方再開): 取得済み区間の最下端(events から再帰的に導出)の prev
///    から遡行を再開し、窓下限または genesis を目指す
/// 7. 終了処理: 再開カーソルを events から導出し直して sync_state に保存し、
///    head ブロックが取得できていれば accounts.latest_head_cid を更新
pub(crate) async fn handle_head_record_with_limits(
    store: &Store,
    network: &NetworkHandle,
    record: &IpnsRecord,
    source: Option<PeerId>,
    limits: SyncLimits,
) -> Result<SyncOutcome, SyncError> {
    verify_ipns_record(record).map_err(|_| SyncError::InvalidSignature)?;

    let author_hex = bytes_to_hex(record.payload.name.as_ref());
    let head_cid_str = record.payload.value.to_string();

    let mut forks_detected = store
        .upsert_head_record(record, now_ms())
        .await?
        .forks_recorded;
    if !record.payload.display_name.is_empty() {
        store
            .fill_display_name_snapshot(&author_hex, &record.payload.display_name)
            .await?;
    }

    // 窓下限の決定。1件でも取得済みなら凍結(窓が後から狭まらない)、
    // 未取得なら最新 head を基準に「最新 window 件」へ引き上げる
    let state = store.get_sync_state(&author_hex).await?;
    let known = store.get_head(&author_hex).await?;
    let default_floor = (record.payload.sequence + 1).saturating_sub(limits.window);
    let floor = match (&state, &known) {
        (Some(s), Some(_)) => s.window_floor_seq,
        (Some(s), None) => s.window_floor_seq.max(default_floor),
        (None, _) => default_floor,
    };

    // stale 判定。遡行が未完了(部分同期の中断後)なら stale レコードでも再開する
    if let Some((known_seq, _)) = known {
        if record.payload.sequence <= known_seq
            && state.as_ref().is_some_and(|s| s.completed)
            && store.get_raw_block(&head_cid_str).await?.is_some()
        {
            return Ok(SyncOutcome {
                new_events: 0,
                completed: true,
                forks_detected,
            });
        }
    }

    let mut budget = limits.budget;
    let mut new_events = 0usize;
    // Unavailable / Budget / Invalid で止まったら、この呼び出しでの遡行は打ち切る
    let mut halted: Option<StopKind> = None;

    // Phase A: 前方追いつき(head ブロック未取得なら head から遡行)
    if store.get_raw_block(&head_cid_str).await?.is_none() {
        let (n, stop) = traverse_and_ingest(
            store,
            network,
            source,
            record.payload.name.as_ref(),
            record.payload.value.clone(),
            record.payload.sequence,
            floor,
            &mut budget,
            limits.chunk,
            &mut forks_detected,
        )
        .await?;
        new_events += n;
        if !stop.can_continue() {
            halted = Some(stop);
        }
    }

    // Phase B: 後方再開。取得済み区間の最下端が窓下限より上なら、その prev から
    // 遡行を続ける(前回の中断からの再開、または既知ブロック合流後の続き)
    if halted.is_none() {
        if let Some((_, bottom_prev, bottom_seq)) = store.get_chain_run_bottom(&author_hex).await?
        {
            if bottom_seq > floor {
                // 検証済みイベントで seq > 0 なら prev は必ずある(BrokenPrev で弾かれる)
                if let Some(prev_str) = bottom_prev {
                    let prev_cid = Cid::try_from(prev_str.as_str())
                        .map_err(|e| SyncError::Decode(e.to_string()))?;
                    let (n, stop) = traverse_and_ingest(
                        store,
                        network,
                        source,
                        record.payload.name.as_ref(),
                        prev_cid,
                        bottom_seq - 1,
                        floor,
                        &mut budget,
                        limits.chunk,
                        &mut forks_detected,
                    )
                    .await?;
                    new_events += n;
                    if !stop.can_continue() {
                        halted = Some(stop);
                    }
                }
            }
        }
    }

    // 終了処理: 再開カーソルは書き込んだ値を信頼せず events から導出し直す。
    // 検証失敗(Invalid)時もここを通り、挿入済み分と進捗は残す
    let bottom = store.get_chain_run_bottom(&author_hex).await?;
    let completed = bottom.as_ref().is_some_and(|(_, _, seq)| *seq <= floor);
    store
        .upsert_sync_state(
            &SyncStateRow {
                pubkey: author_hex.clone(),
                window_floor_seq: floor,
                cursor_cid: bottom.as_ref().map(|(cid, _, _)| cid.clone()),
                cursor_seq: bottom.as_ref().map(|(_, _, seq)| *seq),
                completed,
            },
            now_ms(),
        )
        .await?;

    // head ブロックが実際に取り込めた場合のみ head を確定する
    // (全損時や stale レコードで latest_head_cid を動かさない)
    if store.get_raw_block(&head_cid_str).await?.is_some() {
        let is_latest = store
            .get_head(&author_hex)
            .await?
            .is_none_or(|(max_seq, _)| record.payload.sequence >= max_seq);
        if is_latest {
            store.update_head_cid(&author_hex, &head_cid_str).await?;
        }
    }

    if let Some(StopKind::Invalid(e)) = halted {
        return Err(e);
    }

    debug!(author = %author_hex, new_events, completed, "chain sync progressed");
    Ok(SyncOutcome {
        new_events,
        completed,
        forks_detected,
    })
}

/// head 側(新)から prev を遡行し、検証済みイベントをチャンク単位で挿入する。
/// どの停止理由でもバッファの検証済み分は挿入してから戻る(部分保持)。
/// 戻り値は (挿入件数, 停止理由)。Err は store 障害のみ。
#[allow(clippy::too_many_arguments)]
async fn traverse_and_ingest(
    store: &Store,
    network: &NetworkHandle,
    source: Option<PeerId>,
    author: &[u8; 32],
    start_cid: Cid,
    start_seq: u64,
    floor: u64,
    budget: &mut usize,
    chunk: usize,
    forks_detected: &mut usize,
) -> Result<(usize, StopKind), StoreError> {
    let mut inserted = 0usize;
    let mut buffer: Vec<EventEnvelope> = Vec::new();
    let mut cursor = Some(start_cid);
    let mut expected_seq = start_seq;

    let stop = loop {
        let Some(cid) = cursor.clone() else {
            break StopKind::Genesis;
        };
        if store.get_raw_block(&cid.to_string()).await?.is_some() {
            break StopKind::KnownBlock;
        }
        if expected_seq < floor {
            break StopKind::Floor;
        }
        if *budget == 0 {
            break StopKind::Budget;
        }

        let Some(raw) = network.get_block(cid.clone(), source).await else {
            break StopKind::Unavailable;
        };
        *budget -= 1;

        let envelope: EventEnvelope = match serde_ipld_dagcbor::from_slice(&raw) {
            Ok(e) => e,
            Err(e) => break StopKind::Invalid(SyncError::Decode(e.to_string())),
        };
        if verify_envelope(&envelope).is_err() {
            break StopKind::Invalid(SyncError::InvalidSignature);
        }
        if let Err(e) = verify_chain_link(&envelope, author, expected_seq) {
            break StopKind::Invalid(match e {
                ChainError::WrongAuthor => SyncError::AuthorMismatch { cid: cid.clone() },
                ChainError::WrongSeq { expected, got } => SyncError::SeqMismatch {
                    cid: cid.clone(),
                    expected,
                    got,
                },
                ChainError::BrokenPrev => SyncError::BrokenChain { cid: cid.clone() },
            });
        }

        cursor = envelope.payload.prev.clone();
        expected_seq = expected_seq.saturating_sub(1);
        buffer.push(envelope);
        if buffer.len() >= chunk {
            flush_chunk(store, &mut buffer, &mut inserted, forks_detected).await?;
        }
    };

    flush_chunk(store, &mut buffer, &mut inserted, forks_detected).await?;
    Ok((inserted, stop))
}

/// チャンク内を反転(seq 昇順)して挿入する。チャンク間は新しい順に挿入される
/// ため、順序逆転した Edit/Delete は store の遅延適用が収束させる
/// ([data-model.md] §6)。
async fn flush_chunk(
    store: &Store,
    buffer: &mut Vec<EventEnvelope>,
    inserted: &mut usize,
    forks_detected: &mut usize,
) -> Result<(), StoreError> {
    for envelope in buffer.iter().rev() {
        *forks_detected += store.insert_event(envelope).await?.forks_recorded;
    }
    *inserted += buffer.len();
    buffer.clear();
    Ok(())
}

// --- テスト ---

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{envelope_cid, EventKind};
    use crate::head::{create_ipns_record, feed_topic_str, RECORD_LIFETIME_MS};
    use crate::identity::{create_envelope, Identity};
    use crate::network::NetworkEvent;
    use crate::testutil::{make_record_pointing, spawn_test_node, wait_for, wait_subscribed};
    use crate::util::bytes_to_cid;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::mpsc;

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
        wait_subscribed(&mut events_a, &feed_topic_str(&pubkey_hex)).await;
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
        assert!(outcome.completed);

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
        let mut record = make_record_pointing(&id, 5, bytes_to_cid(b"head"));
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
        let record = make_record_pointing(&id, head_seq, head_cid);
        let outcome = handle_head_record(&store, &dummy_network(), &record, None)
            .await
            .unwrap();
        assert_eq!(outcome.new_events, 0);
        assert!(outcome.completed);

        // 旧DB(sync_state 行なし)でも遡行完了として lazy 生成される
        let author_hex = bytes_to_hex(&id.public_key_bytes());
        let state = store.get_sync_state(&author_hex).await.unwrap().unwrap();
        assert!(state.completed);
    }

    #[tokio::test]
    async fn unavailable_block_keeps_record() {
        let store = Store::open_in_memory().await.unwrap();
        let id = Identity::generate();
        // 署名は正しいが、指す先のブロックがどこにもないレコード
        let record = make_record_pointing(&id, 0, bytes_to_cid(b"missing"));

        // 取得失敗はエラーではなく「未完了の部分同期」(次回 announce/resolve で再開)
        let outcome = handle_head_record(&store, &dummy_network(), &record, None)
            .await
            .unwrap();
        assert_eq!(outcome.new_events, 0);
        assert!(!outcome.completed);

        // チェーン取得に失敗してもポインタは保持される(可用性の床)
        let author_hex = bytes_to_hex(&id.public_key_bytes());
        assert!(store.get_head_record(&author_hex).await.unwrap().is_some());

        // 進捗も未完了として記録される
        let state = store.get_sync_state(&author_hex).await.unwrap().unwrap();
        assert!(!state.completed);

        // 全損時は head を確定しない
        let account = store.get_account(&author_hex).await.unwrap();
        assert!(account.is_none_or(|a| a.latest_head_cid.is_none()));
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
        assert!(outcome.completed);

        let posts = store_b.get_posts_by_author(&pubkey_hex).await.unwrap();
        assert_eq!(posts.len(), 2);
        let account = store_b.get_account(&pubkey_hex).await.unwrap().unwrap();
        assert_eq!(account.latest_head_cid, Some(head_cid.to_string()));
        assert_eq!(account.display_name, "Alice");
    }

    // --- 部分同期(窓・チャンク・再開) ---

    /// seq 0..n の Post チェーンを作る(store には入れない)。
    fn make_post_chain(id: &Identity, n: u64) -> Vec<EventEnvelope> {
        let mut prev: Option<Cid> = None;
        let mut chain = Vec::new();
        for seq in 0..n {
            let e = create_envelope(
                id,
                seq,
                prev.clone(),
                EventKind::Post {
                    text: format!("post {seq}"),
                },
            );
            prev = Some(envelope_cid(&e));
            chain.push(e);
        }
        chain
    }

    /// ブロック提供元 A(store_a を持つノード)と空のフォロワー B を接続して返す。
    /// NetworkHandle と受信チャネルはテスト終了まで保持する必要があるため返す。
    #[allow(clippy::type_complexity)]
    async fn connect_provider_and_follower(
        store_a: Arc<Store>,
    ) -> (
        Arc<Store>,
        (NetworkHandle, mpsc::Receiver<NetworkEvent>),
        (NetworkHandle, mpsc::Receiver<NetworkEvent>),
    ) {
        let (handle_a, events_a, addr_a) = spawn_test_node(Arc::clone(&store_a)).await;
        let store_b = Arc::new(Store::open_in_memory().await.unwrap());
        let (handle_b, mut events_b, _) = spawn_test_node(Arc::clone(&store_b)).await;
        handle_b.dial(addr_a).await;
        wait_for(&mut events_b, |e| {
            matches!(e, NetworkEvent::PeerConnected(_))
        })
        .await;
        (store_b, (handle_a, events_a), (handle_b, events_b))
    }

    #[tokio::test]
    async fn window_limits_backfill_depth() {
        let store_a = Arc::new(Store::open_in_memory().await.unwrap());
        let id = Identity::generate();
        let pubkey_hex = bytes_to_hex(&id.public_key_bytes());
        let chain = make_post_chain(&id, 8);
        for e in &chain {
            store_a.insert_event(e).await.unwrap();
        }
        let head_cid = envelope_cid(chain.last().unwrap());
        let (store_b, _node_a, (handle_b, _events_b)) =
            connect_provider_and_follower(store_a).await;

        let record = make_record_pointing(&id, 7, head_cid);
        let limits = SyncLimits {
            window: 5,
            ..SyncLimits::default()
        };
        let outcome = handle_head_record_with_limits(&store_b, &handle_b, &record, None, limits)
            .await
            .unwrap();

        // 最新5件(seq 3..=7)のみ取り込み、それより古い側は gap として受容
        assert_eq!(outcome.new_events, 5);
        assert!(outcome.completed);
        let posts = store_b.get_posts_by_author(&pubkey_hex).await.unwrap();
        assert_eq!(posts.len(), 5);
        assert!(posts.iter().all(|p| p.text != "post 0"));

        let state = store_b.get_sync_state(&pubkey_hex).await.unwrap().unwrap();
        assert_eq!(state.window_floor_seq, 3);
        assert_eq!(state.cursor_seq, Some(3));
        assert!(state.completed);
    }

    #[tokio::test]
    async fn chunked_insert_applies_edit_across_chunks() {
        let store_a = Arc::new(Store::open_in_memory().await.unwrap());
        let id = Identity::generate();
        let pubkey_hex = bytes_to_hex(&id.public_key_bytes());

        // Post0, Post1, Edit(→Post0), Post3..6 の7イベントチェーン
        let e0 = create_envelope(
            &id,
            0,
            None,
            EventKind::Post {
                text: "original".to_string(),
            },
        );
        let c0 = envelope_cid(&e0);
        let e1 = create_envelope(
            &id,
            1,
            Some(c0.clone()),
            EventKind::Post {
                text: "post 1".to_string(),
            },
        );
        let mut chain = vec![e0, e1];
        let e2 = create_envelope(
            &id,
            2,
            Some(envelope_cid(&chain[1])),
            EventKind::Edit {
                target: c0.clone(),
                text: "edited".to_string(),
            },
        );
        chain.push(e2);
        for seq in 3..7u64 {
            let e = create_envelope(
                &id,
                seq,
                Some(envelope_cid(chain.last().unwrap())),
                EventKind::Post {
                    text: format!("post {seq}"),
                },
            );
            chain.push(e);
        }
        for e in &chain {
            store_a.insert_event(e).await.unwrap();
        }
        let head_cid = envelope_cid(chain.last().unwrap());
        let (store_b, _node_a, (handle_b, _events_b)) =
            connect_provider_and_follower(store_a).await;

        // chunk=2: [6,5][4,3][2,1][0] の順に挿入され、Edit(seq2) が
        // 対象 Post0 より先に入る(チャンク跨ぎの順序逆転)
        let record = make_record_pointing(&id, 6, head_cid);
        let limits = SyncLimits {
            chunk: 2,
            ..SyncLimits::default()
        };
        let outcome = handle_head_record_with_limits(&store_b, &handle_b, &record, None, limits)
            .await
            .unwrap();
        assert_eq!(outcome.new_events, 7);
        assert!(outcome.completed);

        // 遅延適用により Post0 に Edit が反映されている
        let posts = store_b.get_posts_by_author(&pubkey_hex).await.unwrap();
        let edited = posts
            .iter()
            .find(|p| p.cid == c0.to_string())
            .expect("post 0 missing");
        assert_eq!(edited.text, "edited");
        assert!(edited.edited);
    }

    /// 中断 → 補充 → 同一レコードで再開(issue #6 の中核シナリオ)。
    /// 2回目のレコードは seq が既知 max 以下(stale)だが、遡行が未完了なら
    /// 再開されることも同時に検証する。
    #[tokio::test]
    async fn partial_sync_resumes_after_gap_filled() {
        let store_a = Arc::new(Store::open_in_memory().await.unwrap());
        let id = Identity::generate();
        let pubkey_hex = bytes_to_hex(&id.public_key_bytes());
        let chain = make_post_chain(&id, 8);
        // A は上位ブロック(seq 3..=7)しか持っていない(部分保持ピア)
        for e in &chain[3..] {
            store_a.insert_event(e).await.unwrap();
        }
        let head_cid = envelope_cid(&chain[7]);
        let (store_b, _node_a, (handle_b, _events_b)) =
            connect_provider_and_follower(Arc::clone(&store_a)).await;

        let record = make_record_pointing(&id, 7, head_cid);
        let outcome = handle_head_record(&store_b, &handle_b, &record, None)
            .await
            .unwrap();

        // 取れた分(5件)は挿入され、全損しない
        assert_eq!(outcome.new_events, 5);
        assert!(!outcome.completed);
        assert_eq!(
            store_b.get_posts_by_author(&pubkey_hex).await.unwrap().len(),
            5
        );

        // 欠けていたブロックが A に補充された
        for e in &chain[..3] {
            store_a.insert_event(e).await.unwrap();
        }

        // 同一レコード(stale)でも未完了なら残りを再開して取り込む
        let outcome = handle_head_record(&store_b, &handle_b, &record, None)
            .await
            .unwrap();
        assert_eq!(outcome.new_events, 3);
        assert!(outcome.completed);
        assert_eq!(
            store_b.get_posts_by_author(&pubkey_hex).await.unwrap().len(),
            8
        );
    }

    /// 未完了の gap を残したまま新 head が届いた場合、前方(新イベント)と
    /// 後方(gap)の両方に追いつく。
    #[tokio::test]
    async fn new_head_during_incomplete_sync_catches_up_both_ends() {
        let store_a = Arc::new(Store::open_in_memory().await.unwrap());
        let id = Identity::generate();
        let pubkey_hex = bytes_to_hex(&id.public_key_bytes());
        let chain = make_post_chain(&id, 10);
        // A は seq 3..=7 のみ保持
        for e in &chain[3..8] {
            store_a.insert_event(e).await.unwrap();
        }
        let (store_b, _node_a, (handle_b, _events_b)) =
            connect_provider_and_follower(Arc::clone(&store_a)).await;

        let record7 = make_record_pointing(&id, 7, envelope_cid(&chain[7]));
        let o1 = handle_head_record(&store_b, &handle_b, &record7, None)
            .await
            .unwrap();
        assert_eq!(o1.new_events, 5);
        assert!(!o1.completed);

        // A がチェーンを延長(seq 8,9)し、欠損分(seq 0..=2)も補充された
        for e in chain[..3].iter().chain(&chain[8..]) {
            store_a.insert_event(e).await.unwrap();
        }

        // Phase A: 9,8 → 既知ブロック(seq7)で合流。Phase B: 2,1,0 → genesis
        let record9 = make_record_pointing(&id, 9, envelope_cid(&chain[9]));
        let o2 = handle_head_record(&store_b, &handle_b, &record9, None)
            .await
            .unwrap();
        assert_eq!(o2.new_events, 5);
        assert!(o2.completed);
        assert_eq!(
            store_b.get_posts_by_author(&pubkey_hex).await.unwrap().len(),
            10
        );
    }

    /// fork(equivocation)したチェーン: 同一 author の seq=1 に2ブランチが並存する。
    /// head 層(レコードの同 sequence 異 CID)とイベント層(同 seq 異イベント)の
    /// 両方が forks に記録され、同期自体は拒否されず継続することを検証する。
    #[tokio::test]
    async fn forked_chain_is_recorded_and_sync_continues() {
        let store_a = Arc::new(Store::open_in_memory().await.unwrap());
        let id = Identity::generate();
        let author_hex = bytes_to_hex(&id.public_key_bytes());
        let e0 = create_envelope(
            &id,
            0,
            None,
            EventKind::Post {
                text: "genesis".to_string(),
            },
        );
        let c0 = envelope_cid(&e0);
        let branch_a = create_envelope(
            &id,
            1,
            Some(c0.clone()),
            EventKind::Post {
                text: "branch a".to_string(),
            },
        );
        let branch_b = create_envelope(
            &id,
            1,
            Some(c0),
            EventKind::Post {
                text: "branch b".to_string(),
            },
        );
        for e in [&e0, &branch_a, &branch_b] {
            store_a.insert_event(e).await.unwrap();
        }
        let (store_b, _node_a, (handle_b, _events_b)) =
            connect_provider_and_follower(store_a).await;

        // 1本目のブランチ: 通常の同期で fork はまだ観測されない
        let record_a = make_record_pointing(&id, 1, envelope_cid(&branch_a));
        let o1 = handle_head_record(&store_b, &handle_b, &record_a, None)
            .await
            .unwrap();
        assert_eq!(o1.new_events, 2);
        assert!(o1.completed);
        assert_eq!(o1.forks_detected, 0);
        assert!(store_b.list_forks(None).await.unwrap().is_empty());

        // 2本目のブランチ: 拒否されず取り込まれ、両層の fork が記録される
        let record_b = make_record_pointing(&id, 1, envelope_cid(&branch_b));
        let o2 = handle_head_record(&store_b, &handle_b, &record_b, None)
            .await
            .unwrap();
        assert_eq!(o2.new_events, 1);
        assert!(o2.completed);
        assert_eq!(o2.forks_detected, 2); // head 層 1 + イベント層 1

        let forks = store_b.list_forks(Some(&author_hex)).await.unwrap();
        assert!(forks.iter().any(|f| f.layer == "head" && f.seq == 1));
        assert!(forks.iter().any(|f| f.layer == "event" && f.seq == 1));
        assert_eq!(forks.len(), 2);

        // 両ブランチのイベントとも保持されている
        let posts = store_b.get_posts_by_author(&author_hex).await.unwrap();
        assert_eq!(posts.len(), 3);
    }

    /// 予算切れは中断+再開になる(旧 TooDeep のような恒久的な同期不能を残さない)。
    #[tokio::test]
    async fn budget_halts_and_resumes() {
        let store_a = Arc::new(Store::open_in_memory().await.unwrap());
        let id = Identity::generate();
        let pubkey_hex = bytes_to_hex(&id.public_key_bytes());
        let chain = make_post_chain(&id, 8);
        for e in &chain {
            store_a.insert_event(e).await.unwrap();
        }
        let head_cid = envelope_cid(&chain[7]);
        let (store_b, _node_a, (handle_b, _events_b)) =
            connect_provider_and_follower(store_a).await;

        let record = make_record_pointing(&id, 7, head_cid);
        let limits = SyncLimits {
            budget: 3,
            ..SyncLimits::default()
        };

        let o1 = handle_head_record_with_limits(&store_b, &handle_b, &record, None, limits)
            .await
            .unwrap();
        assert_eq!(o1.new_events, 3);
        assert!(!o1.completed);

        let o2 = handle_head_record_with_limits(&store_b, &handle_b, &record, None, limits)
            .await
            .unwrap();
        assert_eq!(o2.new_events, 3);
        assert!(!o2.completed);

        let o3 = handle_head_record_with_limits(&store_b, &handle_b, &record, None, limits)
            .await
            .unwrap();
        assert_eq!(o3.new_events, 2);
        assert!(o3.completed);
        assert_eq!(
            store_b.get_posts_by_author(&pubkey_hex).await.unwrap().len(),
            8
        );
    }
}
