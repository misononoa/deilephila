//! IPC グルー: フロントエンドの `invoke` を Application Core([`crate::app`])へ
//! 委譲する `#[tauri::command]` ラッパのみを置く。関数名 = invoke 名。
//! ロジックはすべて app.rs 側にあり、この層は Tauri の型(`State`)を剥がし、
//! `AppError` を文字列へ変換する(エラーの文字列化はこの境界に集約する)。

use crate::app::{self, AppState, AppStatus, FollowView, ForkView, PostView};

#[tauri::command]
pub async fn get_app_status(state: tauri::State<'_, AppState>) -> Result<AppStatus, String> {
    app::get_app_status(state.inner())
        .await
        .map_err(String::from)
}

#[tauri::command]
pub async fn setup_account(
    passphrase: String,
    state: tauri::State<'_, AppState>,
) -> Result<String, String> {
    app::setup_account(state.inner(), passphrase)
        .await
        .map_err(String::from)
}

#[tauri::command]
pub async fn unlock_account(
    passphrase: String,
    state: tauri::State<'_, AppState>,
) -> Result<String, String> {
    app::unlock_account(state.inner(), passphrase)
        .await
        .map_err(String::from)
}

#[tauri::command]
pub async fn create_post(
    text: String,
    state: tauri::State<'_, AppState>,
) -> Result<String, String> {
    app::create_post(state.inner(), text)
        .await
        .map_err(String::from)
}

#[tauri::command]
pub async fn follow_user(pubkey: String, state: tauri::State<'_, AppState>) -> Result<(), String> {
    app::follow_user(state.inner(), pubkey)
        .await
        .map_err(String::from)
}

#[tauri::command]
pub async fn unfollow_user(
    pubkey: String,
    state: tauri::State<'_, AppState>,
) -> Result<(), String> {
    app::unfollow_user(state.inner(), pubkey)
        .await
        .map_err(String::from)
}

#[tauri::command]
pub async fn get_follows(state: tauri::State<'_, AppState>) -> Result<Vec<FollowView>, String> {
    app::get_follows(state.inner()).await.map_err(String::from)
}

#[tauri::command]
pub async fn get_timeline(state: tauri::State<'_, AppState>) -> Result<Vec<PostView>, String> {
    app::get_timeline(state.inner()).await.map_err(String::from)
}

#[tauri::command]
pub async fn get_forks(state: tauri::State<'_, AppState>) -> Result<Vec<ForkView>, String> {
    app::get_forks(state.inner()).await.map_err(String::from)
}
