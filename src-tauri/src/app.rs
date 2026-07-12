//! Application Core: アカウント・投稿・フォロー・同期のアプリケーションロジック。
//!
//! この層は Tauri(IPC・ウィンドウ)に依存しない。UI への通知は `Notifier`
//! (`UiEvent` チャネル)への送信で表現し、本番では lib.rs がフロントへの
//! emit にブリッジし、統合テスト(tests/)では受信端で直接観測する。
//! commands.rs は各関数を `#[tauri::command]` に包むだけの IPC グルー。

use std::path::PathBuf;
use std::sync::Arc;

use cid::Cid;
use libp2p::PeerId;
use serde::Serialize;
use tokio::sync::{mpsc, Mutex};

use crate::event::{envelope_cid, EventKind};
use crate::head::{create_ipns_record, record_to_bytes, IpnsRecord, RECORD_LIFETIME_MS};
use crate::identity::{create_envelope, Identity};
use crate::keystore::{Keystore, KeystoreError};
use crate::network::{NetworkEvent, NetworkHandle};
use crate::store::{Store, StoreError, TimelineRow};
use crate::sync::{SyncError, SyncOutcome};
use crate::util::{bytes_to_hex, hex_to_pubkey, now_ms};

// --- エラー ---

/// Application Core のエラー。各層の型付きエラー(`KeystoreError` / `StoreError` /
/// `SyncError`)を `#[from]` で集約し、呼び出し側(commands.rs・テスト)が失敗の
/// 種別を variant で判別できるようにする。文字列化は IPC 境界(commands.rs)での
/// み行い、下位エラーは `transparent` で元の Display をそのまま透過する。
#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("account not unlocked")]
    NotUnlocked,
    #[error("account already exists; use unlock_account")]
    AlreadyExists,
    /// 入力不正(公開鍵 hex の形式不正・自己フォロー等)
    #[error("{0}")]
    InvalidInput(String),
    #[error(transparent)]
    Keystore(#[from] KeystoreError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Sync(#[from] SyncError),
}

/// IPC 境界(commands.rs)用の文字列化。フロントが種別分岐を必要とするように
/// なったら serde による構造化エラー(kind + message)へ拡張する。
impl From<AppError> for String {
    fn from(e: AppError) -> String {
        e.to_string()
    }
}

// --- ヘルパー ---

/// 公開鍵 hex の正規化と検証(64桁の16進、小文字化)。
fn normalize_pubkey_hex(input: &str) -> Result<String, AppError> {
    let s = input.trim().to_ascii_lowercase();
    if s.len() != 64 || !s.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(AppError::InvalidInput(
            "public key must be a 64-char hex string".to_string(),
        ));
    }
    Ok(s)
}

// --- UI への通知 ---

/// Application Core から UI 層への通知イベント。
/// 本番では lib.rs のブリッジタスクが `TimelineUpdated` をフロントの
/// `timeline-updated` イベントに変換する。接続・購読イベントは現状 UI では
/// 未使用だが、統合テストの順序制御(相手の購読を待ってから publish する等)と
/// 将来の接続状態表示のために転送する。
#[derive(Debug, Clone)]
pub enum UiEvent {
    TimelineUpdated,
    PeerConnected(PeerId),
    PeerSubscribed { peer: PeerId, topic: String },
}

/// `UiEvent` の送信端。通知は fire-and-forget で、受信側が終了していても
/// 送信側の処理は失敗にしない。
#[derive(Clone)]
pub struct Notifier(mpsc::UnboundedSender<UiEvent>);

impl Notifier {
    pub fn channel() -> (Notifier, mpsc::UnboundedReceiver<UiEvent>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (Notifier(tx), rx)
    }

    pub fn notify(&self, event: UiEvent) {
        let _ = self.0.send(event);
    }
}

// --- IPNS-headレコードの発行 ---

/// 定期 republish の周期。EOL(`RECORD_LIFETIME_MS` = 48時間)の 1/4 で、
/// オンライン中にレコードが失効しない余裕を持たせる(networking.md §4.2)。
pub const REPUBLISH_INTERVAL: std::time::Duration =
    std::time::Duration::from_secs(12 * 60 * 60);

/// 自アカウントの IPNS-headレコードを組み立てる。表示名と最新 Profile イベント
/// CID のスナップショットを projection から取得して同梱する(data-model.md §3 Tier 0)。
async fn build_head_record(
    store: &Store,
    identity: &Identity,
    sequence: u64,
    head_cid: Cid,
) -> Result<IpnsRecord, AppError> {
    let pubkey_hex = bytes_to_hex(&identity.public_key_bytes());
    let display_name = store
        .get_account(&pubkey_hex)
        .await?
        .map(|a| a.display_name)
        .unwrap_or_default();
    let profile_cid = store
        .get_latest_profile_cid(&pubkey_hex)
        .await?
        .and_then(|s| s.parse::<Cid>().ok());
    Ok(create_ipns_record(
        identity,
        sequence,
        head_cid,
        now_ms() + RECORD_LIFETIME_MS,
        profile_cid,
        display_name,
    ))
}

/// 自レコードを head_records に保存し(再提供と M6 `GetLatestHead` 応答の源泉)、
/// gossipsub+DHT の両経路へ publish する。
async fn store_and_publish_head_record(
    store: &Store,
    network: &NetworkHandle,
    record: IpnsRecord,
) -> Result<(), AppError> {
    let pubkey_hex = bytes_to_hex(record.payload.name.as_ref());
    store
        .upsert_head_record(
            &pubkey_hex,
            record.payload.sequence,
            record.payload.validity,
            &record_to_bytes(&record),
            now_ms(),
        )
        .await?;
    network.publish_head(record).await;
    Ok(())
}

/// 自分の最新 head のレコードを validity を更新して再発行する。
/// unlock 時と定期 republish タスク(`REPUBLISH_INTERVAL` 周期、lib.rs)から呼ばれる。
/// 未アンロック・head 未作成なら何もしない。戻り値は publish したかどうか。
pub async fn republish_head(state: &AppState) -> Result<bool, AppError> {
    let record = {
        let guard = state.account.lock().await;
        let Some(account) = guard.as_ref() else {
            return Ok(false);
        };
        let Some(head_cid) = account.head_cid.clone() else {
            return Ok(false);
        };
        // head_cid が Some なら next_seq >= 1 なので -1 は安全
        build_head_record(
            &state.store,
            &account.identity,
            account.next_seq - 1,
            head_cid,
        )
        .await?
    };
    store_and_publish_head_record(&state.store, &state.network, record).await?;
    Ok(true)
}

// --- 公開型 ---

#[derive(Debug, Serialize)]
pub struct AppStatus {
    pub setup: bool,
    pub unlocked: bool,
}

#[derive(Debug, Serialize)]
pub struct PostView {
    pub cid: String,
    pub author: String,
    pub text: String,
    pub timestamp: i64,
    pub edited: bool,
    pub deleted: bool,
    /// author の表示名(accounts に Profile が fold 済みなら Some)
    pub author_display_name: Option<String>,
}

impl From<TimelineRow> for PostView {
    fn from(r: TimelineRow) -> Self {
        PostView {
            cid: r.cid,
            author: r.author,
            text: r.text,
            timestamp: r.timestamp,
            edited: r.edited,
            deleted: r.deleted,
            author_display_name: r.display_name,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct FollowView {
    pub pubkey: String,
    pub since: i64,
    pub display_name: Option<String>,
}

// --- AppState ---

/// アカウントがアンロックされているときのインメモリ状態。
/// next_seq: 次に作成するイベントに割り当てる seq (genesis では 0)
/// head_cid: 最後に作成したイベントの CID (イベントがない場合は None)
struct ActiveAccount {
    identity: Identity,
    next_seq: u64,
    head_cid: Option<Cid>,
}

pub struct AppState {
    // tokio::sync::Mutex を使う理由: create_post でロックを保持したまま async ストア呼び出しをまたぐため
    account: Mutex<Option<ActiveAccount>>,
    store: Arc<Store>,
    app_dir: PathBuf,
    network: NetworkHandle,
    notify: Notifier,
}

impl AppState {
    pub fn new(
        store: Arc<Store>,
        app_dir: PathBuf,
        network: NetworkHandle,
        notify: Notifier,
    ) -> Self {
        AppState {
            account: Mutex::new(None),
            store,
            app_dir,
            network,
            notify,
        }
    }
}

// --- NetworkEvent 消費ループ ---

/// gossipsub 由来の IPNS-headレコード1件を処理する。フォロー相手のものだけ
/// チェーン同期に流し(networking.md §6: フォローしていないユーザーのデータは
/// 原則受け取らない)、新規イベントを取り込んだら `TimelineUpdated` を通知する。
/// 自分のレコードがこの経路に来ることはない(自分のトピックは購読しない)。
pub async fn handle_head_received(
    store: &Store,
    network: &NetworkHandle,
    record: &IpnsRecord,
    source: PeerId,
    notify: &Notifier,
) {
    let author_hex = bytes_to_hex(record.payload.name.as_ref());
    match store.is_followed(&author_hex).await {
        Ok(true) => {}
        Ok(false) => {
            tracing::debug!("ignoring head record for unfollowed account {author_hex}");
            return;
        }
        Err(e) => {
            tracing::warn!("follow lookup failed for {author_hex}: {e}");
            return;
        }
    }
    match crate::sync::handle_head_record(store, network, record, Some(source)).await {
        Ok(outcome) => {
            // 部分的な進捗でも取れた分は即表示に反映する
            if outcome.new_events > 0 {
                notify.notify(UiEvent::TimelineUpdated);
            }
            if !outcome.completed {
                tracing::info!("chain sync incomplete; will resume on next announce/resolve");
            }
        }
        Err(e) => tracing::warn!("chain sync failed: {e}"),
    }
}

/// NetworkEvent を消費する core タスクの本体。IPNS-headレコードが届いたら
/// チェーン同期を実行し、新規イベントを取り込んだら `TimelineUpdated` を通知する。
/// 呼び出し側(lib.rs / テストハーネス)が spawn する。
pub async fn network_consumer_loop(
    store: Arc<Store>,
    network: NetworkHandle,
    mut event_rx: mpsc::Receiver<NetworkEvent>,
    notify: Notifier,
) {
    while let Some(event) = event_rx.recv().await {
        match event {
            NetworkEvent::HeadReceived { record, source } => {
                handle_head_received(&store, &network, &record, source, &notify).await;
            }
            NetworkEvent::PeerConnected(peer) => notify.notify(UiEvent::PeerConnected(peer)),
            NetworkEvent::PeerSubscribed { peer, topic } => {
                notify.notify(UiEvent::PeerSubscribed { peer, topic });
            }
            NetworkEvent::PeerDiscovered(_) => {}
        }
    }
}

// --- フォロー対象の同期 ---

/// フォロー対象の最新チェーンを DHT から取り込む(networking.md §4.2)。
/// `spawn_sync_follow_target` と統合テストの共通入口で、M6 の
/// フォローグラフ探索(`GetLatestHead`)もここに合流する予定。
/// Ok(None) = DHT にレコードが見つからない(以後 gossipsub / republish で追いつく)。
pub async fn sync_follow_target(
    store: &Store,
    network: &NetworkHandle,
    pubkey: [u8; 32],
) -> Result<Option<SyncOutcome>, AppError> {
    let Some(record) = network.resolve_ipns(pubkey).await else {
        return Ok(None);
    };
    let outcome = crate::sync::handle_head_record(store, network, &record, None).await?;
    Ok(Some(outcome))
}

/// フォロー相手の最新レコードを DHT から解決し、チェーンを取り込むバックグラウンド
/// タスクを起動する。新規フォロー時と unlock 時(オフライン中の取りこぼし回収。
/// gossipsub は過去分を再送しない)の両方から呼ばれる。
/// 起動直後はピア接続とルーティングテーブルが形成途中で解決に失敗しうるため、
/// 3秒間隔で最大10回リトライする。新規イベントを取り込んだら `TimelineUpdated` を通知する。
fn spawn_sync_follow_target(
    store: Arc<Store>,
    network: NetworkHandle,
    notify: Notifier,
    pubkey_hex: String,
) {
    let Some(pubkey_bytes) = hex_to_pubkey(&pubkey_hex) else {
        return;
    };
    tokio::spawn(async move {
        for attempt in 0..10 {
            if attempt > 0 {
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
            }
            match sync_follow_target(&store, &network, pubkey_bytes).await {
                Ok(Some(outcome)) => {
                    if outcome.new_events > 0 {
                        notify.notify(UiEvent::TimelineUpdated);
                    }
                    return;
                }
                Ok(None) => {} // レコード未発見: リトライ
                Err(e) => {
                    tracing::warn!("dht sync failed for {pubkey_hex}: {e}");
                    return;
                }
            }
        }
        tracing::debug!("no head record in DHT for {pubkey_hex}");
    });
}

// --- コマンド本体 ---

/// アプリの状態を返す。
/// setup: Keystore ファイルが存在するか
/// unlocked: アカウントがメモリ上にロードされているか
pub async fn get_app_status(state: &AppState) -> Result<AppStatus, AppError> {
    let setup = Keystore::exists(&state.app_dir);
    let unlocked = state.account.lock().await.is_some();
    Ok(AppStatus { setup, unlocked })
}

/// 初回セットアップ: 新しいアカウントを生成し Keystore を作成する。成功時は pubkey hex を返す。
pub async fn setup_account(state: &AppState, passphrase: String) -> Result<String, AppError> {
    if Keystore::exists(&state.app_dir) {
        return Err(AppError::AlreadyExists);
    }
    let (ks, pubkey) = Keystore::create(&passphrase, &state.app_dir)?;
    let pubkey_hex = bytes_to_hex(&pubkey);
    let identity = ks.into_identity();
    *state.account.lock().await = Some(ActiveAccount {
        identity,
        next_seq: 0,
        head_cid: None,
    });
    Ok(pubkey_hex)
}

/// 既存アカウントを passphrase で復号してアンロックする。成功時は pubkey hex を返す。
pub async fn unlock_account(state: &AppState, passphrase: String) -> Result<String, AppError> {
    let ks = Keystore::load(&passphrase, &state.app_dir)?;
    let pubkey_hex = bytes_to_hex(&ks.identity().public_key_bytes());

    // SQLite の events テーブルから最大 seq を取得して head を復元する
    let (next_seq, head_cid) = state
        .store
        .get_head(&pubkey_hex)
        .await?
        .map(|(max_seq, cid_str)| {
            let cid = cid_str.and_then(|s| s.parse::<Cid>().ok());
            (max_seq + 1, cid)
        })
        .unwrap_or((0, None));

    let identity = ks.into_identity();
    *state.account.lock().await = Some(ActiveAccount {
        identity,
        next_seq,
        head_cid,
    });

    // フォロー全件の feed トピック購読を復元し、オフライン中の取りこぼしを
    // DHT から回収する(gossipsub は過去分を再送しないため resolve が唯一の回収経路)
    for follow in state.store.get_follows().await? {
        state.network.subscribe(follow.pubkey.clone()).await;
        spawn_sync_follow_target(
            Arc::clone(&state.store),
            state.network.clone(),
            state.notify.clone(),
            follow.pubkey,
        );
    }
    // 再起動時の republish: validity を今から48時間に更新したレコードを再発行する
    // (head 未作成なら no-op)
    republish_head(state).await?;

    Ok(pubkey_hex)
}

/// テキスト投稿を作成・署名し SQLite に保存する。成功時はイベント CID を返す。
pub async fn create_post(state: &AppState, text: String) -> Result<String, AppError> {
    let mut guard = state.account.lock().await;
    let account = guard.as_mut().ok_or(AppError::NotUnlocked)?;

    let seq = account.next_seq;
    let prev = account.head_cid.clone();
    let envelope = create_envelope(&account.identity, seq, prev, EventKind::Post { text });
    let cid = envelope_cid(&envelope);
    let cid_str = cid.to_string();

    state.store.insert_event(&envelope).await?;

    state
        .store
        .update_head_cid(
            &bytes_to_hex(&account.identity.public_key_bytes()),
            &cid_str,
        )
        .await?;

    account.next_seq = seq + 1;
    account.head_cid = Some(cid.clone());

    // 新しい head の IPNS-headレコードを保存し、gossipsub+DHT の両経路へ publish する
    // (publish 自体は fire-and-forget。購読者ゼロでも失敗扱いにしない)
    let record = build_head_record(&state.store, &account.identity, seq, cid).await?;
    store_and_publish_head_record(&state.store, &state.network, record).await?;

    Ok(cid_str)
}

/// 公開鍵(hex)でフォローする。follows へ追加し、feed トピックを購読し、
/// 相手の最新 IPNS-headレコードを DHT から解決してチェーンを取り込む。
pub async fn follow_user(state: &AppState, pubkey: String) -> Result<(), AppError> {
    let pubkey_hex = normalize_pubkey_hex(&pubkey)?;

    let self_hex = {
        let guard = state.account.lock().await;
        let account = guard.as_ref().ok_or(AppError::NotUnlocked)?;
        bytes_to_hex(&account.identity.public_key_bytes())
    };
    if pubkey_hex == self_hex {
        return Err(AppError::InvalidInput("cannot follow yourself".to_string()));
    }

    state.store.add_follow(&pubkey_hex, now_ms()).await?;
    state.network.subscribe(pubkey_hex.clone()).await;

    // 後発フォロワーの初回同期(networking.md §4.2): gossipsub の次の publish を
    // 待たず、DHT の永続レコードから過去分を取り込む。DHT クエリは数秒かかり
    // うるため、コマンドは即座に返しバックグラウンドで実行する
    spawn_sync_follow_target(
        Arc::clone(&state.store),
        state.network.clone(),
        state.notify.clone(),
        pubkey_hex,
    );
    Ok(())
}

/// フォローを解除する。follows から削除し、feed トピックの購読を止める。
pub async fn unfollow_user(state: &AppState, pubkey: String) -> Result<(), AppError> {
    let pubkey_hex = normalize_pubkey_hex(&pubkey)?;
    state.store.remove_follow(&pubkey_hex).await?;
    state.network.unsubscribe(pubkey_hex).await;
    Ok(())
}

/// フォロー一覧を返す(表示名は accounts に fold 済みなら同梱)。
pub async fn get_follows(state: &AppState) -> Result<Vec<FollowView>, AppError> {
    let follows = state.store.get_follows().await?;
    Ok(follows
        .into_iter()
        .map(|f| FollowView {
            pubkey: f.pubkey,
            since: f.since,
            display_name: f.display_name,
        })
        .collect())
}

/// タイムライン(自分 + フォロー相手の投稿、時系列降順)を返す。
/// 削除済みを含む(UI 側でフィルタ)。
pub async fn get_timeline(state: &AppState) -> Result<Vec<PostView>, AppError> {
    let pubkey_hex = {
        let guard = state.account.lock().await;
        let account = guard.as_ref().ok_or(AppError::NotUnlocked)?;
        bytes_to_hex(&account.identity.public_key_bytes())
    };

    let rows = state.store.get_timeline(&pubkey_hex).await?;
    Ok(rows.into_iter().map(PostView::from).collect())
}

// --- テスト ---

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{far_future_ms, make_record};

    /// コマンド受信側を持たない NetworkHandle(ネットワーク不要のテスト用)。
    fn dummy_network() -> NetworkHandle {
        let (tx, _rx) = mpsc::channel(1);
        NetworkHandle::new(tx)
    }

    fn dummy_source() -> PeerId {
        PeerId::random()
    }

    #[tokio::test]
    async fn record_from_unfollowed_author_ignored() {
        let store = Store::open_in_memory().await.unwrap();
        let (notify, _ui) = Notifier::channel();
        let stranger = Identity::generate();
        let record = make_record(&stranger, 1, far_future_ms());

        handle_head_received(&store, &dummy_network(), &record, dummy_source(), &notify).await;

        // フォロー外のレコードは同期に流れず、何も保存されない
        let author_hex = bytes_to_hex(&stranger.public_key_bytes());
        assert!(store.get_head_record(&author_hex).await.unwrap().is_none());
        assert!(store.get_account(&author_hex).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn record_from_followed_author_processed() {
        let store = Store::open_in_memory().await.unwrap();
        let (notify, _ui) = Notifier::channel();
        let followee = Identity::generate();
        let author_hex = bytes_to_hex(&followee.public_key_bytes());
        let record = make_record(&followee, 1, far_future_ms());

        store.add_follow(&author_hex, now_ms()).await.unwrap();
        handle_head_received(&store, &dummy_network(), &record, dummy_source(), &notify).await;

        // 同期に流れてレコードが保持される(ブロック取得は dummy_network で
        // 失敗するが、ポインタと表示名スナップショットは残る = 可用性の床)
        let (seq, _) = store.get_head_record(&author_hex).await.unwrap().unwrap();
        assert_eq!(seq, 1);
        let account = store.get_account(&author_hex).await.unwrap().unwrap();
        assert_eq!(account.display_name, "Alice");
    }
}
