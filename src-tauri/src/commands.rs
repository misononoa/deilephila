use std::path::PathBuf;
use std::sync::Arc;

use cid::Cid;
use serde::Serialize;
use tauri::Emitter;
use tokio::sync::Mutex;

use crate::event::{envelope_cid, EventKind};
use crate::head::{create_ipns_record, record_to_bytes, IpnsRecord, RECORD_LIFETIME_MS};
use crate::identity::{create_envelope, Identity};
use crate::keystore::Keystore;
use crate::network::NetworkHandle;
use crate::store::{bytes_to_hex, hex_to_pubkey, PostRow, Store, TimelineRow};

// --- ヘルパー ---

trait ToCommandResult<T> {
    fn cmd(self) -> Result<T, String>;
}

impl<T, E: ToString> ToCommandResult<T> for Result<T, E> {
    fn cmd(self) -> Result<T, String> {
        self.map_err(|e| e.to_string())
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_millis() as i64
}

/// 公開鍵 hex の正規化と検証(64桁の16進、小文字化)。
fn normalize_pubkey_hex(input: &str) -> Result<String, String> {
    let s = input.trim().to_ascii_lowercase();
    if s.len() != 64 || !s.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err("public key must be a 64-char hex string".to_string());
    }
    Ok(s)
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
) -> Result<IpnsRecord, String> {
    let pubkey_hex = bytes_to_hex(&identity.public_key_bytes());
    let display_name = store
        .get_account(&pubkey_hex)
        .await
        .cmd()?
        .map(|a| a.display_name)
        .unwrap_or_default();
    let profile_cid = store
        .get_latest_profile_cid(&pubkey_hex)
        .await
        .cmd()?
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
) -> Result<(), String> {
    let pubkey_hex = bytes_to_hex(record.payload.name.as_ref());
    store
        .upsert_head_record(
            &pubkey_hex,
            record.payload.sequence,
            &record_to_bytes(&record),
            now_ms(),
        )
        .await
        .cmd()?;
    network.publish_head(record).await;
    Ok(())
}

/// フォロー相手の最新レコードを DHT から解決し、チェーンを取り込むバックグラウンド
/// タスクを起動する(networking.md §4.2)。新規フォロー時と unlock 時(オフライン中の
/// 取りこぼし回収。gossipsub は過去分を再送しない)の両方から呼ばれる。
/// 起動直後はピア接続とルーティングテーブルが形成途中で解決に失敗しうるため、
/// しばらくリトライする。新規イベントを取り込んだら timeline-updated を emit する。
fn spawn_resolve_and_sync(
    app: tauri::AppHandle,
    store: Arc<Store>,
    network: NetworkHandle,
    pubkey_hex: String,
) {
    let Some(pubkey_bytes) = hex_to_pubkey(&pubkey_hex) else {
        return;
    };
    tauri::async_runtime::spawn(async move {
        let mut resolved = None;
        for _ in 0..10 {
            if let Some(record) = network.resolve_ipns(pubkey_bytes).await {
                resolved = Some(record);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        }
        let Some(record) = resolved else {
            tracing::debug!("no head record in DHT for {pubkey_hex}");
            return;
        };
        match crate::sync::handle_head_record(&store, &network, &record, None).await {
            Ok(outcome) if outcome.new_events > 0 => {
                if let Err(e) = app.emit("timeline-updated", ()) {
                    tracing::warn!("emit timeline-updated failed: {e}");
                }
            }
            Ok(_) => {}
            Err(e) => tracing::warn!("dht sync failed for {pubkey_hex}: {e}"),
        }
    });
}

/// 自分の最新 head のレコードを validity を更新して再発行する。
/// unlock 時と定期 republish タスク(`REPUBLISH_INTERVAL` 周期、lib.rs)から呼ばれる。
/// 未アンロック・head 未作成なら何もしない。戻り値は publish したかどうか。
pub async fn republish_head(state: &AppState) -> Result<bool, String> {
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

impl From<PostRow> for PostView {
    fn from(r: PostRow) -> Self {
        PostView {
            cid: r.cid,
            author: r.author,
            text: r.text,
            timestamp: r.timestamp,
            edited: r.edited,
            deleted: r.deleted,
            author_display_name: None,
        }
    }
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
}

impl AppState {
    pub fn new(store: Arc<Store>, app_dir: PathBuf, network: NetworkHandle) -> Self {
        AppState {
            account: Mutex::new(None),
            store,
            app_dir,
            network,
        }
    }
}

// --- コマンド ---

/// アプリの状態を返す。
/// setup: Keystore ファイルが存在するか
/// unlocked: アカウントがメモリ上にロードされているか
#[tauri::command]
pub async fn get_app_status(state: tauri::State<'_, AppState>) -> Result<AppStatus, String> {
    let setup = Keystore::exists(&state.app_dir);
    let unlocked = state.account.lock().await.is_some();
    Ok(AppStatus { setup, unlocked })
}

/// 初回セットアップ: 新しいアカウントを生成し Keystore を作成する。成功時は pubkey hex を返す。
#[tauri::command]
pub async fn setup_account(
    passphrase: String,
    state: tauri::State<'_, AppState>,
) -> Result<String, String> {
    if Keystore::exists(&state.app_dir) {
        return Err("account already exists; use unlock_account".to_string());
    }
    let (ks, pubkey) = Keystore::create(&passphrase, &state.app_dir).cmd()?;
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
#[tauri::command]
pub async fn unlock_account(
    passphrase: String,
    app: tauri::AppHandle,
    state: tauri::State<'_, AppState>,
) -> Result<String, String> {
    let ks = Keystore::load(&passphrase, &state.app_dir).cmd()?;
    let pubkey_hex = bytes_to_hex(&ks.identity().public_key_bytes());

    // SQLite の events テーブルから最大 seq を取得して head を復元する
    let (next_seq, head_cid) = state
        .store
        .get_head(&pubkey_hex)
        .await
        .cmd()?
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
    for follow in state.store.get_follows().await.cmd()? {
        state.network.subscribe(follow.pubkey.clone()).await;
        spawn_resolve_and_sync(
            app.clone(),
            Arc::clone(&state.store),
            state.network.clone(),
            follow.pubkey,
        );
    }
    // 再起動時の republish: validity を今から48時間に更新したレコードを再発行する
    // (head 未作成なら no-op)
    republish_head(state.inner()).await?;

    Ok(pubkey_hex)
}

/// テキスト投稿を作成・署名し SQLite に保存する。成功時はイベント CID を返す。
#[tauri::command]
pub async fn create_post(
    text: String,
    state: tauri::State<'_, AppState>,
) -> Result<String, String> {
    let mut guard = state.account.lock().await;
    let account = guard.as_mut().ok_or("account not unlocked")?;

    let seq = account.next_seq;
    let prev = account.head_cid.clone();
    let envelope = create_envelope(&account.identity, seq, prev, EventKind::Post { text });
    let cid = envelope_cid(&envelope);
    let cid_str = cid.to_string();

    state.store.insert_event(&envelope).await.cmd()?;

    state
        .store
        .update_head_cid(
            &bytes_to_hex(&account.identity.public_key_bytes()),
            &cid_str,
        )
        .await
        .cmd()?;

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
#[tauri::command]
pub async fn follow_user(
    pubkey: String,
    app: tauri::AppHandle,
    state: tauri::State<'_, AppState>,
) -> Result<(), String> {
    let pubkey_hex = normalize_pubkey_hex(&pubkey)?;

    let self_hex = {
        let guard = state.account.lock().await;
        let account = guard.as_ref().ok_or("account not unlocked")?;
        bytes_to_hex(&account.identity.public_key_bytes())
    };
    if pubkey_hex == self_hex {
        return Err("cannot follow yourself".to_string());
    }

    state.store.add_follow(&pubkey_hex, now_ms()).await.cmd()?;
    state.network.subscribe(pubkey_hex.clone()).await;

    // 後発フォロワーの初回同期(networking.md §4.2): gossipsub の次の publish を
    // 待たず、DHT の永続レコードから過去分を取り込む。DHT クエリは数秒かかり
    // うるため、コマンドは即座に返しバックグラウンドで実行する
    spawn_resolve_and_sync(
        app,
        Arc::clone(&state.store),
        state.network.clone(),
        pubkey_hex,
    );
    Ok(())
}

/// フォローを解除する。follows から削除し、feed トピックの購読を止める。
#[tauri::command]
pub async fn unfollow_user(
    pubkey: String,
    state: tauri::State<'_, AppState>,
) -> Result<(), String> {
    let pubkey_hex = normalize_pubkey_hex(&pubkey)?;
    state.store.remove_follow(&pubkey_hex).await.cmd()?;
    state.network.unsubscribe(pubkey_hex).await;
    Ok(())
}

/// フォロー一覧を返す(表示名は accounts に fold 済みなら同梱)。
#[tauri::command]
pub async fn get_follows(state: tauri::State<'_, AppState>) -> Result<Vec<FollowView>, String> {
    let follows = state.store.get_follows().await.cmd()?;
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
#[tauri::command]
pub async fn get_timeline(state: tauri::State<'_, AppState>) -> Result<Vec<PostView>, String> {
    let pubkey_hex = {
        let guard = state.account.lock().await;
        let account = guard.as_ref().ok_or("account not unlocked")?;
        bytes_to_hex(&account.identity.public_key_bytes())
    };

    let rows = state.store.get_timeline(&pubkey_hex).await.cmd()?;
    Ok(rows.into_iter().map(PostView::from).collect())
}

/// CID 文字列でブロックを取得する。ローカルになければネットワークから取得を試みる。
/// M3 デバッグ用。M4 以降で本格利用。
#[tauri::command]
pub async fn get_block(cid: String, state: tauri::State<'_, AppState>) -> Result<Vec<u8>, String> {
    let parsed: Cid = cid.parse().map_err(|e| format!("invalid CID: {e}"))?;

    // まずローカルを確認
    if let Some(data) = state.store.get_raw_block(&cid).await.cmd()? {
        return Ok(data);
    }

    // ローカルになければネットワークから取得
    state
        .network
        .get_block(parsed, None)
        .await
        .ok_or_else(|| "block not found".to_string())
}

/// 自分の投稿一覧を返す(削除済みを含む。UI 側でフィルタ)。
#[tauri::command]
pub async fn get_my_posts(state: tauri::State<'_, AppState>) -> Result<Vec<PostView>, String> {
    let pubkey_hex = {
        let guard = state.account.lock().await;
        let account = guard.as_ref().ok_or("account not unlocked")?;
        bytes_to_hex(&account.identity.public_key_bytes())
    };

    let posts = state.store.get_posts_by_author(&pubkey_hex).await.cmd()?;

    Ok(posts.into_iter().map(PostView::from).collect())
}
