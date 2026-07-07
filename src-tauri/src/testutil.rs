//! 単体テスト共通ヘルパー(`#[cfg(test)]` 専用)。tests/ の統合テストからは
//! 参照できないため、統合テスト側の共通処理は tests/common に置く。

use std::sync::Arc;
use std::time::Duration;

use cid::Cid;
use libp2p::Multiaddr;
use tokio::sync::mpsc;

use crate::head::{create_ipns_record, IpnsRecord};
use crate::identity::Identity;
use crate::network::{spawn_swarm_loop, NetworkEvent, NetworkHandle};
use crate::store::Store;
use crate::util::{bytes_to_cid, now_ms};

pub fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
}

/// 失効の絡まないテストで使う、十分先の validity(現在 + 1時間)。
pub fn far_future_ms() -> i64 {
    now_ms() + 3_600_000
}

/// display_name とプロフィール CID 入りの代表的な IPNS-headレコードを作る。
/// head/profile CID は固定ダミーなので、チェーン取得を伴わないテスト用。
pub fn make_record(identity: &Identity, sequence: u64, validity: i64) -> IpnsRecord {
    create_ipns_record(
        identity,
        sequence,
        bytes_to_cid(b"head block"),
        validity,
        Some(bytes_to_cid(b"profile block")),
        "Alice".to_string(),
    )
}

/// 実在の head CID を指す、プロフィールスナップショットなしの最小レコード。
/// チェーン同期(prev 遡行)を実際に走らせるテスト用。
pub fn make_record_pointing(identity: &Identity, sequence: u64, head_cid: Cid) -> IpnsRecord {
    create_ipns_record(
        identity,
        sequence,
        head_cid,
        far_future_ms(),
        None,
        String::new(),
    )
}

/// テスト用の libp2p ノードを起動する。127.0.0.1 を使う
/// (0.0.0.0 はダイアル先として使えないため)。
pub async fn spawn_test_node(
    store: Arc<Store>,
) -> (NetworkHandle, mpsc::Receiver<NetworkEvent>, Multiaddr) {
    let listen: Multiaddr = "/ip4/127.0.0.1/tcp/0".parse().unwrap();
    spawn_swarm_loop(store, Some(listen))
        .await
        .expect("swarm failed to start")
}

/// 条件に合う NetworkEvent が届くまで待つ(上限 10 秒)。
pub async fn wait_for(
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

/// put の伝播や identify によるルーティングテーブル形成を待つため、
/// 解決できるまでリトライする(上限 ~10 秒)。
pub async fn resolve_with_retry(handle: &NetworkHandle, pubkey: [u8; 32]) -> Option<IpnsRecord> {
    for _ in 0..40 {
        if let Some(record) = handle.resolve_ipns(pubkey).await {
            return Some(record);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    None
}
