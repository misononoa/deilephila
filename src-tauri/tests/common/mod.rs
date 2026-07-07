//! シナリオ統合テスト用ハーネス。
//!
//! `TestApp` 1つがアプリ1インスタンス相当: tempdir 内のファイルベース SQLite +
//! keystore、実 libp2p Swarm(127.0.0.1 の実 TCP)、`network_consumer_loop`。
//! 配線は lib.rs の本番セットアップと同一で、違いは UiEvent をフロントへ emit
//! する代わりにテストが受信端(`ui`)で直接観測することだけ。

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use deilephila_lib::app::{self, AppState, Notifier, UiEvent};
use deilephila_lib::network::{spawn_swarm_loop, NetworkHandle};
use deilephila_lib::store::Store;
use libp2p::Multiaddr;
use tempfile::TempDir;
use tokio::sync::mpsc;

/// イベント待機・ポーリングの上限。CI 等の遅い環境でも耐える値。
pub const WAIT_TIMEOUT: Duration = Duration::from_secs(15);

pub struct TestApp {
    pub state: Arc<AppState>,
    pub network: NetworkHandle,
    pub store: Arc<Store>,
    /// Application Core からの通知(本番ではフロントへの emit に変換される)
    pub ui: mpsc::UnboundedReceiver<UiEvent>,
    /// この Swarm の実リッスンアドレス(他インスタンスの dial 先)
    pub addr: Multiaddr,
    consumer: tokio::task::JoinHandle<()>,
    dir: TempDir,
}

impl TestApp {
    pub async fn spawn() -> TestApp {
        let dir = tempfile::tempdir().expect("failed to create temp dir");
        Self::spawn_in(dir).await
    }

    async fn spawn_in(dir: TempDir) -> TestApp {
        let app_dir: PathBuf = dir.path().to_path_buf();
        let store = Arc::new(
            Store::open(&app_dir.join("deilephila.db"))
                .await
                .expect("failed to open store"),
        );
        // 0.0.0.0 はダイアル先として使えないため 127.0.0.1 でリッスンする
        let listen: Multiaddr = "/ip4/127.0.0.1/tcp/0".parse().unwrap();
        let (network, event_rx, addr) = spawn_swarm_loop(Arc::clone(&store), Some(listen))
            .await
            .expect("swarm failed to start");

        let (notifier, ui) = Notifier::channel();
        let consumer = tokio::spawn(app::network_consumer_loop(
            Arc::clone(&store),
            network.clone(),
            event_rx,
            notifier.clone(),
        ));
        let state = Arc::new(AppState::new(
            Arc::clone(&store),
            app_dir,
            network.clone(),
            notifier,
        ));
        TestApp {
            state,
            network,
            store,
            ui,
            addr,
            consumer,
            dir,
        }
    }

    /// アプリの再起動を再現する: 全ハンドルを破棄して Swarm ループを終了させ、
    /// 同じデータディレクトリ(keystore + SQLite)から新インスタンスを立てる。
    /// consumer は NetworkHandle を保持し Swarm と相互に生存し合うため明示 abort する。
    pub async fn restart(self) -> TestApp {
        let TestApp {
            state,
            network,
            store,
            ui,
            consumer,
            dir,
            ..
        } = self;
        consumer.abort();
        drop((state, network, store, ui));
        Self::spawn_in(dir).await
    }

    /// 相手インスタンスへ dial し、接続確立(PeerConnected)まで待つ。
    pub async fn connect(&mut self, other: &TestApp) {
        self.network.dial(other.addr.clone()).await;
        self.wait_ui(|e| matches!(e, UiEvent::PeerConnected(_)))
            .await;
    }

    /// 条件を満たす UiEvent が届くまで待つ(タイムアウト付き)。
    pub async fn wait_ui(&mut self, pred: impl Fn(&UiEvent) -> bool) -> UiEvent {
        tokio::time::timeout(WAIT_TIMEOUT, async {
            loop {
                let ev = self.ui.recv().await.expect("UiEvent channel closed");
                if pred(&ev) {
                    return ev;
                }
            }
        })
        .await
        .expect("timed out waiting for UiEvent")
    }

    /// タイムラインが条件を満たすまでポーリングして返す。
    /// UiEvent の TimelineUpdated は follow 時同期と gossipsub 受信の両経路から
    /// 届き、待ち始める前に発火済みのこともあるため、イベント購読ではなく
    /// projection(ローカル SQLite)の観測で収束を判定する。
    pub async fn wait_timeline(
        &self,
        pred: impl Fn(&[app::PostView]) -> bool,
    ) -> Vec<app::PostView> {
        tokio::time::timeout(WAIT_TIMEOUT, async {
            loop {
                let timeline = app::get_timeline(&self.state)
                    .await
                    .expect("get_timeline failed");
                if pred(&timeline) {
                    return timeline;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("timed out waiting for timeline condition")
    }
}
