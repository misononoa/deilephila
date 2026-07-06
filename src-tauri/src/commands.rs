use std::path::PathBuf;
use std::sync::Arc;

use cid::Cid;
use serde::Serialize;
use tokio::sync::Mutex;

use crate::event::{envelope_cid, EventKind};
use crate::head::create_head_announce;
use crate::identity::{create_envelope, Identity};
use crate::keystore::Keystore;
use crate::network::NetworkHandle;
use crate::store::{bytes_to_hex, PostRow, Store, TimelineRow};

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
    // head があれば announce を作っておく(再起動時の republish — M5 の定期 republish の先取り。
    // head_cid が Some なら next_seq >= 1 なので -1 は安全)
    let announce = head_cid
        .clone()
        .map(|cid| create_head_announce(&identity, next_seq - 1, cid));

    *state.account.lock().await = Some(ActiveAccount {
        identity,
        next_seq,
        head_cid,
    });

    // フォロー全件の feed トピック購読を復元する
    for follow in state.store.get_follows().await.cmd()? {
        state.network.subscribe(follow.pubkey).await;
    }
    if let Some(announce) = announce {
        state.network.publish_head(announce).await;
    }

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

    // 新しい head をフォロワーへ通知(fire-and-forget。購読者ゼロでも失敗扱いにしない)
    let announce = create_head_announce(&account.identity, seq, cid);
    state.network.publish_head(announce).await;

    Ok(cid_str)
}

/// 公開鍵(hex)でフォローする。follows へ追加し、feed トピックを購読する。
#[tauri::command]
pub async fn follow_user(pubkey: String, state: tauri::State<'_, AppState>) -> Result<(), String> {
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
    state.network.subscribe(pubkey_hex).await;
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
