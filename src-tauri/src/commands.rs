//! IPC グルー: フロントエンドの `invoke` を Application Core([`crate::app`])へ
//! 委譲する `#[tauri::command]` ラッパのみを置く。関数名 = invoke 名。
//! ロジックはすべて app.rs 側にあり、この層は Tauri の型(`State`)を剥がすだけ。

use crate::app::{self, AppState, AppStatus, FollowView, PostView};

#[tauri::command]
pub async fn get_app_status(state: tauri::State<'_, AppState>) -> Result<AppStatus, String> {
    app::get_app_status(state.inner()).await
}

#[tauri::command]
pub async fn setup_account(
    passphrase: String,
    state: tauri::State<'_, AppState>,
) -> Result<String, String> {
    app::setup_account(state.inner(), passphrase).await
}

#[tauri::command]
pub async fn unlock_account(
    passphrase: String,
    state: tauri::State<'_, AppState>,
) -> Result<String, String> {
    app::unlock_account(state.inner(), passphrase).await
}

#[tauri::command]
pub async fn create_post(
    text: String,
    state: tauri::State<'_, AppState>,
) -> Result<String, String> {
    app::create_post(state.inner(), text).await
}

#[tauri::command]
pub async fn follow_user(
    pubkey: String,
    state: tauri::State<'_, AppState>,
) -> Result<(), String> {
    app::follow_user(state.inner(), pubkey).await
}

#[tauri::command]
pub async fn unfollow_user(
    pubkey: String,
    state: tauri::State<'_, AppState>,
) -> Result<(), String> {
    app::unfollow_user(state.inner(), pubkey).await
}

#[tauri::command]
pub async fn get_follows(state: tauri::State<'_, AppState>) -> Result<Vec<FollowView>, String> {
    app::get_follows(state.inner()).await
}

#[tauri::command]
pub async fn get_timeline(state: tauri::State<'_, AppState>) -> Result<Vec<PostView>, String> {
    app::get_timeline(state.inner()).await
}

#[tauri::command]
pub async fn get_block(cid: String, state: tauri::State<'_, AppState>) -> Result<Vec<u8>, String> {
    app::get_block(state.inner(), cid).await
}

#[tauri::command]
pub async fn get_my_posts(state: tauri::State<'_, AppState>) -> Result<Vec<PostView>, String> {
    app::get_my_posts(state.inner()).await
}
