//! コマンド層のシナリオ統合テスト。
//!
//! Application Core(app.rs)の公開関数を、実 libp2p(127.0.0.1)で会話する
//! 複数の `TestApp` インスタンス越しに叩き、setup → post → follow → 同期 →
//! timeline の全経路を検証する。mvp.md §3 の M4c/M5c 検証列の自動化に相当する
//! (Tauri IPC の配線と真の別プロセス・別マシン確認のみ手動に残る)。

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::TestApp;
use deilephila_lib::app::{self, AppState, Notifier, UiEvent};
use deilephila_lib::head::feed_topic_str;
use deilephila_lib::network::NetworkHandle;
use deilephila_lib::store::{hex_to_pubkey, Store};
use tokio::sync::mpsc;

/// 投稿間で timestamp(ミリ秒)を確実に単調増加させる。
/// タイムライン順序のアサーションが同時刻タイで不定にならないようにするための
/// 入力整形であり、イベント待ちの sleep ではない。
async fn tick() {
    tokio::time::sleep(Duration::from_millis(10)).await;
}

/// DHT の応答はルーティングテーブル形成のタイミングに依存するため、
/// 成功するまでリトライする(上限 15 秒)。
async fn retry_dht<T, F, Fut>(mut op: F) -> Option<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    for _ in 0..60 {
        if let Some(v) = op().await {
            return Some(v);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    None
}

/// M4c 検証列: フォロー相手の新規投稿がリアルタイムに同期される。
/// 2件目の head から prev 遡行でフォロー前の1件目も取り込まれること
/// (チェーンの追いつき)も同時に検証する。
#[tokio::test]
async fn realtime_sync_after_follow() {
    let mut a = TestApp::spawn().await;
    let pk_a = app::setup_account(&a.state, "pass-a".into()).await.unwrap();
    app::create_post(&a.state, "first".into()).await.unwrap();
    tick().await;

    let mut b = TestApp::spawn().await;
    app::setup_account(&b.state, "pass-b".into()).await.unwrap();
    b.connect(&a).await;
    app::follow_user(&b.state, pk_a.clone()).await.unwrap();

    // gossipsub の購読情報は接続上で交換される。A が B の購読を観測してから
    // 2件目を publish することで、リアルタイム経路の受信を決定的にする
    let topic_a = feed_topic_str(&pk_a);
    a.wait_ui(|e| matches!(e, UiEvent::PeerSubscribed { topic, .. } if *topic == topic_a))
        .await;
    app::create_post(&a.state, "second".into()).await.unwrap();

    let timeline = b.wait_timeline(|t| t.len() == 2).await;
    assert_eq!(timeline[0].text, "second");
    assert_eq!(timeline[1].text, "first");
    assert!(timeline.iter().all(|p| p.author == pk_a));

    // フォロー一覧にも反映されている
    let follows = app::get_follows(&b.state).await.unwrap();
    assert_eq!(follows.len(), 1);
    assert_eq!(follows[0].pubkey, pk_a);
}

/// M5c 検証列: 発信者の publish 時にオフラインだった後発フォロワーが、
/// DHT の永続レコード経由で過去の投稿全件に追いつく。
#[tokio::test]
async fn late_follower_catches_up_via_dht() {
    let a = TestApp::spawn().await;
    let pk_a = app::setup_account(&a.state, "pass-a".into()).await.unwrap();
    for text in ["one", "two", "three"] {
        app::create_post(&a.state, text.into()).await.unwrap();
        tick().await;
    }

    // B は A の publish 後に起動する(gossipsub は受信していない)
    let mut b = TestApp::spawn().await;
    app::setup_account(&b.state, "pass-b".into()).await.unwrap();
    b.connect(&a).await;

    // 接続確立後の定期 republish を再現し、レコードを B からも到達可能にする
    assert!(app::republish_head(&a.state).await.unwrap());

    // follow_user は購読 + バックグラウンド同期を開始する。単発の DHT resolve は
    // ルーティングテーブル形成に間に合わないことがあるため、同じ同期入口
    // (sync_follow_target)をリトライで直接叩いて収束を待つ
    app::follow_user(&b.state, pk_a.clone()).await.unwrap();
    let pk_a_bytes = hex_to_pubkey(&pk_a).unwrap();
    let outcome = retry_dht(|| async {
        app::sync_follow_target(&b.store, &b.network, pk_a_bytes)
            .await
            .unwrap()
    })
    .await
    .expect("head record not resolvable via DHT");
    assert!(outcome.new_events <= 3);

    let timeline = b.wait_timeline(|t| t.len() == 3).await;
    let texts: Vec<&str> = timeline.iter().map(|p| p.text.as_str()).collect();
    assert_eq!(texts, ["three", "two", "one"]);
}

/// M4b/M5c 検証列: 再起動 → unlock で next_seq / head / フォロー購読が復元され、
/// unlock 時 republish で DHT 上のポインタが生き返る。
#[tokio::test]
async fn restart_unlock_restores_chain_and_subscriptions() {
    let a = TestApp::spawn().await;
    let pk_a = app::setup_account(&a.state, "correct horse".into())
        .await
        .unwrap();
    app::create_post(&a.state, "one".into()).await.unwrap();
    tick().await;
    app::create_post(&a.state, "two".into()).await.unwrap();
    tick().await;

    let mut b = TestApp::spawn().await;
    let pk_b = app::setup_account(&b.state, "pass-b".into()).await.unwrap();
    // 再起動後の購読復元を検証するため、A は B をフォローしておく
    app::follow_user(&a.state, pk_b.clone()).await.unwrap();

    // アプリ再起動を再現
    let mut a = a.restart().await;
    let status = app::get_app_status(&a.state).await.unwrap();
    assert!(status.setup && !status.unlocked);

    // 誤パスフレーズは拒否され、状態は変わらない
    assert!(app::unlock_account(&a.state, "wrong".into()).await.is_err());
    assert!(!app::get_app_status(&a.state).await.unwrap().unlocked);

    // 正しい unlock: 同一アカウントに復元される
    let unlocked_pk = app::unlock_account(&a.state, "correct horse".into())
        .await
        .unwrap();
    assert_eq!(unlocked_pk, pk_a);

    b.connect(&a).await;
    app::follow_user(&b.state, pk_a.clone()).await.unwrap();
    let topic_a = feed_topic_str(&pk_a);
    a.wait_ui(|e| matches!(e, UiEvent::PeerSubscribed { topic, .. } if *topic == topic_a))
        .await;

    // unlock 時 republish の検証: 再起動で A の DHT ストア(インメモリ)は消えて
    // いるため、B が解決できるのは unlock が再発行したレコードだけ
    let pk_a_bytes = hex_to_pubkey(&pk_a).unwrap();
    let record = retry_dht(|| b.network.resolve_ipns(pk_a_bytes))
        .await
        .expect("record republished at unlock not resolvable via DHT");
    assert_eq!(record.payload.sequence, 1); // 投稿2件 = seq 0,1 の head

    // next_seq / head_cid の復元検証: 復元が誤っていれば3件目の seq / prev が
    // ずれ、B 側同期の seq 連続性検証(SeqMismatch)で弾かれて 3 件に達しない
    app::create_post(&a.state, "three".into()).await.unwrap();
    let timeline = b.wait_timeline(|t| t.len() == 3).await;
    assert_eq!(timeline[0].text, "three");

    // フォロー購読の復元検証: unlock が follows 全件を再購読しているので、
    // B の新規投稿が A のタイムラインに届く
    let topic_b = feed_topic_str(&pk_b);
    b.wait_ui(|e| matches!(e, UiEvent::PeerSubscribed { topic, .. } if *topic == topic_b))
        .await;
    app::create_post(&b.state, "from b".into()).await.unwrap();
    a.wait_timeline(|t| t.iter().any(|p| p.text == "from b" && p.author == pk_b))
        .await;
}

/// unlock 時の DHT 回収: A のオフライン中にフォロー相手 B が投稿し、B の
/// 再 publish なしに A の再起動 + unlock だけで取りこぼしがタイムラインに
/// 現れる。gossipsub は過去分を再送しないため、unlock 時に全フォロー相手へ
/// 起動する resolve(spawn_sync_follow_target)が唯一の回収経路。
#[tokio::test]
async fn unlock_recovers_missed_posts_via_dht() {
    let mut a = TestApp::spawn().await;
    app::setup_account(&a.state, "pass-a".into()).await.unwrap();
    let b = TestApp::spawn().await;
    let pk_b = app::setup_account(&b.state, "pass-b".into()).await.unwrap();

    a.connect(&b).await;
    app::follow_user(&a.state, pk_b.clone()).await.unwrap();

    // A 停止(旧インスタンスの Swarm ごと破棄)中に B が投稿する。
    // この時点で B は誰とも接続しておらず、レコードは B のローカル DHT ストアにだけある
    let mut a = a.restart().await;
    app::create_post(&b.state, "missed while offline".into())
        .await
        .unwrap();

    // A 再接続 + unlock。フォロー購読の復元と同時に DHT 回収が走る
    a.connect(&b).await;
    app::unlock_account(&a.state, "pass-a".into()).await.unwrap();

    let timeline = a
        .wait_timeline(|t| {
            t.iter()
                .any(|p| p.text == "missed while offline" && p.author == pk_b)
        })
        .await;
    assert_eq!(timeline.len(), 1);
}

/// 複数フォロー相手の投稿がタイムラインに timestamp 降順でマージされる。
#[tokio::test]
async fn timeline_merges_multiple_authors() {
    let mut a = TestApp::spawn().await;
    let pk_a = app::setup_account(&a.state, "pass-a".into()).await.unwrap();
    let mut c = TestApp::spawn().await;
    let pk_c = app::setup_account(&c.state, "pass-c".into()).await.unwrap();
    let mut b = TestApp::spawn().await;
    app::setup_account(&b.state, "pass-b".into()).await.unwrap();

    // A と C は互いを知らず、どちらも B とだけ接続する
    b.connect(&a).await;
    b.connect(&c).await;
    app::follow_user(&b.state, pk_a.clone()).await.unwrap();
    app::follow_user(&b.state, pk_c.clone()).await.unwrap();

    let topic_a = feed_topic_str(&pk_a);
    a.wait_ui(|e| matches!(e, UiEvent::PeerSubscribed { topic, .. } if *topic == topic_a))
        .await;
    let topic_c = feed_topic_str(&pk_c);
    c.wait_ui(|e| matches!(e, UiEvent::PeerSubscribed { topic, .. } if *topic == topic_c))
        .await;

    app::create_post(&a.state, "a1".into()).await.unwrap();
    tick().await;
    app::create_post(&c.state, "c1".into()).await.unwrap();
    tick().await;
    app::create_post(&a.state, "a2".into()).await.unwrap();

    let timeline = b.wait_timeline(|t| t.len() == 3).await;
    let texts: Vec<&str> = timeline.iter().map(|p| p.text.as_str()).collect();
    assert_eq!(texts, ["a2", "c1", "a1"]);
    assert_eq!(timeline[0].author, pk_a);
    assert_eq!(timeline[1].author, pk_c);
}

/// コマンドのエラー系と状態遷移(Swarm 不要。コマンド受信側を持たない
/// NetworkHandle で、ネットワーク送信が fire-and-forget であることも兼ねて検証)。
#[tokio::test]
async fn command_error_paths() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open_in_memory().await.unwrap());
    let (notifier, _ui) = Notifier::channel();
    let (cmd_tx, _) = mpsc::channel(1);
    let state = AppState::new(
        store,
        dir.path().to_path_buf(),
        NetworkHandle::new(cmd_tx),
        notifier,
    );

    // 未セットアップ・未アンロック
    let status = app::get_app_status(&state).await.unwrap();
    assert!(!status.setup && !status.unlocked);
    assert!(app::create_post(&state, "x".into()).await.is_err());
    assert!(app::get_timeline(&state).await.is_err());
    assert!(app::get_my_posts(&state).await.is_err());
    assert!(app::follow_user(&state, "ab".repeat(32)).await.is_err());
    assert!(app::unlock_account(&state, "pass".into()).await.is_err());

    // セットアップで unlocked になる
    let pk = app::setup_account(&state, "pass".into()).await.unwrap();
    let status = app::get_app_status(&state).await.unwrap();
    assert!(status.setup && status.unlocked);

    // 二重セットアップは拒否
    assert!(app::setup_account(&state, "other".into()).await.is_err());

    // 自己フォロー・不正な公開鍵は拒否
    assert!(app::follow_user(&state, pk.clone()).await.is_err());
    assert!(app::follow_user(&state, "not-hex".into()).await.is_err());
    assert!(app::follow_user(&state, "abcd".into()).await.is_err());

    // 誤パスフレーズの unlock は失敗する
    assert!(app::unlock_account(&state, "wrong".into()).await.is_err());
}
