pub mod chain;
pub mod commands;
pub mod event;
pub mod head;
pub mod identity;
pub mod keystore;
pub mod network;
pub mod store;
pub mod sync;

use std::path::PathBuf;
use std::sync::Arc;

use commands::AppState;
use network::{spawn_swarm_loop, NetworkEvent, NetworkHandle};
use store::Store;
use tauri::{Emitter, Manager};

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .setup(|app| {
            // DEILEPHILA_DATA_DIR でデータディレクトリを上書きできる
            // (同一マシンで複数インスタンスを動かす検証用)
            let app_dir = match std::env::var("DEILEPHILA_DATA_DIR") {
                Ok(dir) if !dir.is_empty() => PathBuf::from(dir),
                _ => app
                    .path()
                    .app_data_dir()
                    .expect("failed to resolve app data dir"),
            };
            std::fs::create_dir_all(&app_dir).expect("failed to create app data dir");

            let db_path = app_dir.join("deilephila.db");
            let store = Arc::new(
                tauri::async_runtime::block_on(Store::open(&db_path))
                    .expect("failed to open SQLite store"),
            );

            let (network_handle, event_rx) = {
                let store_clone = Arc::clone(&store);
                tauri::async_runtime::block_on(async move {
                    match spawn_swarm_loop(store_clone, None).await {
                        Ok((handle, event_rx, addr)) => {
                            tracing::info!("libp2p listening on {addr}");
                            (handle, Some(event_rx))
                        }
                        Err(e) => {
                            tracing::error!("failed to start network: {e}");
                            // ネット起動失敗でもアプリは起動する(ローカル機能は動く)
                            let (cmd_tx, _) = tokio::sync::mpsc::channel(1);
                            (NetworkHandle::new(cmd_tx), None)
                        }
                    }
                })
            };

            // DEILEPHILA_DIAL: mDNS が使えない環境向けの明示接続(カンマ区切り multiaddr)。
            // 例: DEILEPHILA_DIAL=/ip4/127.0.0.1/tcp/44615
            if let Ok(dials) = std::env::var("DEILEPHILA_DIAL") {
                for addr_str in dials.split(',').filter(|s| !s.trim().is_empty()) {
                    match addr_str.trim().parse::<libp2p::Multiaddr>() {
                        Ok(addr) => {
                            let network = network_handle.clone();
                            tauri::async_runtime::block_on(network.dial(addr));
                        }
                        Err(e) => tracing::warn!("invalid DEILEPHILA_DIAL addr {addr_str}: {e}"),
                    }
                }
            }

            // core タスク: NetworkEvent を消費し、head 通知が来たらチェーン同期を実行。
            // 新規イベントを取り込んだらフロントへ timeline-updated を通知する。
            if let Some(mut event_rx) = event_rx {
                let store = Arc::clone(&store);
                let network = network_handle.clone();
                let app_handle = app.handle().clone();
                tauri::async_runtime::spawn(async move {
                    while let Some(event) = event_rx.recv().await {
                        let NetworkEvent::HeadReceived { announce, source } = event else {
                            continue;
                        };
                        match sync::handle_head_announce(&store, &network, &announce, Some(source))
                            .await
                        {
                            Ok(outcome) if outcome.new_events > 0 => {
                                if let Err(e) = app_handle.emit("timeline-updated", ()) {
                                    tracing::warn!("emit timeline-updated failed: {e}");
                                }
                            }
                            Ok(_) => {}
                            Err(e) => tracing::warn!("chain sync failed: {e}"),
                        }
                    }
                });
            }

            app.manage(AppState::new(store, app_dir, network_handle));
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            commands::get_app_status,
            commands::setup_account,
            commands::unlock_account,
            commands::create_post,
            commands::get_my_posts,
            commands::get_block,
            commands::follow_user,
            commands::unfollow_user,
            commands::get_follows,
            commands::get_timeline,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
