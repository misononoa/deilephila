use std::path::Path;

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::SqlitePool;

use crate::event::{envelope_cid, EventEnvelope, EventKind};
use crate::util::{bytes_to_hex, to_dag_cbor};

pub struct Store {
    pool: SqlitePool,
}

/// 単一 author の投稿行。テストが projection の検証に使う。
#[derive(Debug, Clone)]
pub struct PostRow {
    pub cid: String,
    pub author: String,
    pub text: String,
    pub timestamp: i64,
    pub edited: bool,
    pub deleted: bool,
}

#[derive(Debug, Clone)]
pub struct FollowRow {
    pub pubkey: String,
    pub since: i64,
    /// accounts に display_name があればそれ(空文字列は未設定扱いで None)
    pub display_name: Option<String>,
}

/// タイムライン表示用の行。posts の行 + author の display_name。
#[derive(Debug, Clone)]
pub struct TimelineRow {
    pub cid: String,
    pub author: String,
    pub text: String,
    pub timestamp: i64,
    pub edited: bool,
    pub deleted: bool,
    pub display_name: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AccountRow {
    pub pubkey: String,
    pub display_name: String,
    pub bio: String,
    pub latest_head_cid: Option<String>,
    pub last_seen: i64,
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("SQLite error: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("migration error: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),
    #[error("serialization error: {0}")]
    Serialization(String),
}

impl Store {
    pub async fn open(path: &Path) -> Result<Self, StoreError> {
        let opts = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(std::time::Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(opts)
            .await?;
        sqlx::migrate!("./migrations").run(&pool).await?;
        Ok(Store { pool })
    }

    pub async fn open_in_memory() -> Result<Self, StoreError> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await?;
        sqlx::migrate!("./migrations").run(&pool).await?;
        Ok(Store { pool })
    }

    /// EventEnvelope を events テーブルに挿入し、projection を更新する。
    /// 同じ CID が既存の場合は無視する(冪等)。events と projection はトランザクションで原子的に更新。
    pub async fn insert_event(&self, envelope: &EventEnvelope) -> Result<(), StoreError> {
        let cid = envelope_cid(envelope);
        let cid_str = cid.to_string();
        let author = bytes_to_hex(envelope.payload.author.as_ref());
        let seq = envelope.payload.seq as i64;
        let prev_str = envelope.payload.prev.as_ref().map(|c| c.to_string());
        let timestamp = envelope.payload.timestamp;
        let kind_tag = kind_tag_str(&envelope.payload.kind);
        let kind_json = serde_json::to_string(&envelope.payload.kind)
            .map_err(|e| StoreError::Serialization(e.to_string()))?;
        let raw_cbor: Vec<u8> = to_dag_cbor(envelope);

        let mut tx = self.pool.begin().await?;

        sqlx::query!(
            "INSERT OR IGNORE INTO events
             (cid, author, seq, prev_cid, timestamp, kind_tag, kind_json, raw_cbor)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            cid_str,
            author,
            seq,
            prev_str,
            timestamp,
            kind_tag,
            kind_json,
            raw_cbor
        )
        .execute(&mut *tx)
        .await?;

        match &envelope.payload.kind {
            EventKind::Post { text } => {
                sqlx::query!(
                    "INSERT OR IGNORE INTO posts
                     (cid, author, seq, text, timestamp, edited, deleted, latest_edit_seq)
                     VALUES (?, ?, ?, ?, ?, 0, 0, ?)",
                    cid_str,
                    author,
                    seq,
                    text,
                    timestamp,
                    seq
                )
                .execute(&mut *tx)
                .await?;
            }
            // Edit/Delete は target が同一 author の Post を指す場合のみ有効
            // (data-model.md §4)。author 条件がないと、他人の投稿 CID を target に
            // 指す不正イベントで projection が書き換えられてしまう
            EventKind::Edit { target, text } => {
                let target_str = target.to_string();
                sqlx::query!(
                    "UPDATE posts SET text = ?, edited = 1, latest_edit_seq = ?
                     WHERE cid = ? AND author = ? AND latest_edit_seq < ?",
                    text,
                    seq,
                    target_str,
                    author,
                    seq
                )
                .execute(&mut *tx)
                .await?;
            }
            EventKind::Delete { target } => {
                let target_str = target.to_string();
                sqlx::query!(
                    "UPDATE posts SET deleted = 1 WHERE cid = ? AND author = ?",
                    target_str,
                    author
                )
                .execute(&mut *tx)
                .await?;
            }
            EventKind::Profile {
                display_name, bio, ..
            } => {
                sqlx::query!(
                    "INSERT INTO accounts (pubkey, display_name, bio, latest_head_cid, last_seen)
                     VALUES (?, ?, ?, NULL, ?)
                     ON CONFLICT(pubkey) DO UPDATE SET
                         display_name = excluded.display_name,
                         bio          = excluded.bio,
                         last_seen    = excluded.last_seen",
                    author,
                    display_name,
                    bio,
                    timestamp
                )
                .execute(&mut *tx)
                .await?;
            }
            _ => {}
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn get_posts_by_author(&self, author_hex: &str) -> Result<Vec<PostRow>, StoreError> {
        let rows = sqlx::query!(
            r#"SELECT cid, author, text, timestamp,
               edited as "edited: bool",
               deleted as "deleted: bool"
               FROM posts WHERE author = ? ORDER BY seq DESC"#,
            author_hex
        )
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|r| PostRow {
            cid: r.cid,
            author: r.author,
            text: r.text,
            timestamp: r.timestamp,
            edited: r.edited,
            deleted: r.deleted,
        })
        .collect();
        Ok(rows)
    }

    pub async fn get_account(&self, pubkey_hex: &str) -> Result<Option<AccountRow>, StoreError> {
        let row = sqlx::query!(
            "SELECT pubkey, display_name, bio, latest_head_cid, last_seen
             FROM accounts WHERE pubkey = ?",
            pubkey_hex
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| AccountRow {
            pubkey: r.pubkey,
            display_name: r.display_name,
            bio: r.bio,
            latest_head_cid: r.latest_head_cid,
            last_seen: r.last_seen,
        }))
    }

    /// アカウントの latest_head_cid を更新する。
    pub async fn update_head_cid(
        &self,
        pubkey_hex: &str,
        head_cid: &str,
    ) -> Result<(), StoreError> {
        sqlx::query!(
            "INSERT INTO accounts (pubkey, display_name, bio, latest_head_cid, last_seen)
             VALUES (?, '', '', ?, 0)
             ON CONFLICT(pubkey) DO UPDATE SET latest_head_cid = excluded.latest_head_cid",
            pubkey_hex,
            head_cid
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// CID 文字列でイベントの生 DAG-CBOR bytes を取得する(ブロック提供用)。
    pub async fn get_raw_block(&self, cid_str: &str) -> Result<Option<Vec<u8>>, StoreError> {
        let row = sqlx::query!("SELECT raw_cbor FROM events WHERE cid = ?", cid_str)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| r.raw_cbor))
    }

    /// フォローを追加する。既存なら何もしない(冪等。元の since を保持)。
    pub async fn add_follow(&self, pubkey_hex: &str, since: i64) -> Result<(), StoreError> {
        sqlx::query!(
            "INSERT INTO follows (pubkey, since) VALUES (?, ?)
             ON CONFLICT(pubkey) DO NOTHING",
            pubkey_hex,
            since
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn remove_follow(&self, pubkey_hex: &str) -> Result<(), StoreError> {
        sqlx::query!("DELETE FROM follows WHERE pubkey = ?", pubkey_hex)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// 指定した公開鍵をフォローしているかを返す(受信レコードの受理判定用)。
    pub async fn is_followed(&self, pubkey_hex: &str) -> Result<bool, StoreError> {
        let row = sqlx::query!("SELECT pubkey FROM follows WHERE pubkey = ?", pubkey_hex)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.is_some())
    }

    /// フォロー一覧を返す(新しい順)。display_name は accounts にあれば同梱。
    pub async fn get_follows(&self) -> Result<Vec<FollowRow>, StoreError> {
        let rows = sqlx::query!(
            r#"SELECT f.pubkey, f.since,
               NULLIF(a.display_name, '') AS "display_name?: String"
               FROM follows f
               LEFT JOIN accounts a ON a.pubkey = f.pubkey
               ORDER BY f.since DESC"#
        )
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|r| FollowRow {
            pubkey: r.pubkey,
            since: r.since,
            display_name: r.display_name,
        })
        .collect();
        Ok(rows)
    }

    /// タイムライン: 自分 + フォロー相手の投稿を timestamp 降順でマージして返す。
    /// 削除済みも含む(UI 側でフィルタ。get_posts_by_author と同方針)。
    pub async fn get_timeline(
        &self,
        self_pubkey_hex: &str,
    ) -> Result<Vec<TimelineRow>, StoreError> {
        let rows = sqlx::query!(
            r#"SELECT p.cid, p.author, p.text, p.timestamp,
               p.edited AS "edited: bool",
               p.deleted AS "deleted: bool",
               NULLIF(a.display_name, '') AS "display_name?: String"
               FROM posts p
               LEFT JOIN accounts a ON a.pubkey = p.author
               WHERE p.author = ? OR p.author IN (SELECT pubkey FROM follows)
               ORDER BY p.timestamp DESC"#,
            self_pubkey_hex
        )
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|r| TimelineRow {
            cid: r.cid,
            author: r.author,
            text: r.text,
            timestamp: r.timestamp,
            edited: r.edited,
            deleted: r.deleted,
            display_name: r.display_name,
        })
        .collect();
        Ok(rows)
    }

    /// アカウントの最新 head を events テーブルから取得する(unlock 時の復元用)。
    /// 戻り値: (max_seq, event_cid) のペア、またはイベントがなければ None。
    pub async fn get_head(
        &self,
        pubkey_hex: &str,
    ) -> Result<Option<(u64, Option<String>)>, StoreError> {
        let row = sqlx::query!(
            "SELECT seq, cid FROM events WHERE author = ? ORDER BY seq DESC LIMIT 1",
            pubkey_hex
        )
        .fetch_optional(&self.pool)
        .await?;

        // seq は不変条件上 ≥0 なので i64 → u64 キャストは安全
        Ok(row.map(|r| (r.seq as u64, Some(r.cid))))
    }

    /// IPNS-headレコードを保存する。既知 sequence 以下のレコードは無視する(冪等)。
    /// フォロー相手 + 自分の最新レコードの常時保持([networking.md] §3.2)の実体で、
    /// M6 の GetLatestHead 応答の源泉にもなる。
    pub async fn upsert_head_record(
        &self,
        pubkey_hex: &str,
        sequence: u64,
        record_bytes: &[u8],
        now_ms: i64,
    ) -> Result<(), StoreError> {
        let seq = sequence as i64;
        sqlx::query!(
            "INSERT INTO head_records (pubkey, sequence, record_bytes, updated_at)
             VALUES (?, ?, ?, ?)
             ON CONFLICT(pubkey) DO UPDATE SET
                 sequence = excluded.sequence,
                 record_bytes = excluded.record_bytes,
                 updated_at = excluded.updated_at
             WHERE excluded.sequence > head_records.sequence",
            pubkey_hex,
            seq,
            record_bytes,
            now_ms
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// 保持中の最新 IPNS-headレコードを (sequence, レコードのバイト列) で返す。
    pub async fn get_head_record(
        &self,
        pubkey_hex: &str,
    ) -> Result<Option<(u64, Vec<u8>)>, StoreError> {
        let row = sqlx::query!(
            "SELECT sequence, record_bytes FROM head_records WHERE pubkey = ?",
            pubkey_hex
        )
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| (r.sequence as u64, r.record_bytes)))
    }

    /// IPNS-headレコードの display_name スナップショット(Tier 0)を accounts に
    /// 反映する。チェーンの Profile fold が正典なので、未設定(空)のときだけ埋める
    /// ([data-model.md] §3: 不一致時はチェーンが勝つ)。
    pub async fn fill_display_name_snapshot(
        &self,
        pubkey_hex: &str,
        display_name: &str,
    ) -> Result<(), StoreError> {
        sqlx::query!(
            "INSERT INTO accounts (pubkey, display_name) VALUES (?, ?)
             ON CONFLICT(pubkey) DO UPDATE SET display_name = excluded.display_name
             WHERE accounts.display_name = ''",
            pubkey_hex,
            display_name
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// チェーン上最新の Profile イベントの CID(レコードの profile_cid スナップショット用)。
    pub async fn get_latest_profile_cid(
        &self,
        pubkey_hex: &str,
    ) -> Result<Option<String>, StoreError> {
        let row = sqlx::query!(
            "SELECT cid FROM events
             WHERE author = ? AND kind_tag = 'Profile'
             ORDER BY seq DESC LIMIT 1",
            pubkey_hex
        )
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| r.cid))
    }
}

fn kind_tag_str(kind: &EventKind) -> &'static str {
    match kind {
        EventKind::Post { .. } => "Post",
        EventKind::Edit { .. } => "Edit",
        EventKind::Delete { .. } => "Delete",
        EventKind::Profile { .. } => "Profile",
        EventKind::Follow { .. } => "Follow",
        EventKind::Reply { .. } => "Reply",
    }
}

// --- テスト ---

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{envelope_cid, EventKind};
    use crate::identity::{create_envelope, Identity};

    async fn make_store() -> Store {
        Store::open_in_memory().await.unwrap()
    }

    #[tokio::test]
    async fn insert_and_get_post() {
        let store = make_store().await;
        let identity = Identity::generate();
        let pubkey_hex = bytes_to_hex(&identity.public_key_bytes());

        let envelope = create_envelope(
            &identity,
            0,
            None,
            EventKind::Post {
                text: "hello store".to_string(),
            },
        );
        store.insert_event(&envelope).await.unwrap();

        let posts = store.get_posts_by_author(&pubkey_hex).await.unwrap();
        assert_eq!(posts.len(), 1);
        assert_eq!(posts[0].text, "hello store");
        assert!(!posts[0].edited);
        assert!(!posts[0].deleted);
    }

    #[tokio::test]
    async fn insert_is_idempotent() {
        let store = make_store().await;
        let identity = Identity::generate();
        let pubkey_hex = bytes_to_hex(&identity.public_key_bytes());

        let envelope = create_envelope(
            &identity,
            0,
            None,
            EventKind::Post {
                text: "once".to_string(),
            },
        );
        store.insert_event(&envelope).await.unwrap();
        store.insert_event(&envelope).await.unwrap();

        let posts = store.get_posts_by_author(&pubkey_hex).await.unwrap();
        assert_eq!(posts.len(), 1);
    }

    #[tokio::test]
    async fn edit_projection_updates() {
        let store = make_store().await;
        let identity = Identity::generate();
        let pubkey_hex = bytes_to_hex(&identity.public_key_bytes());

        let post = create_envelope(
            &identity,
            0,
            None,
            EventKind::Post {
                text: "original".to_string(),
            },
        );
        let post_cid = envelope_cid(&post);
        store.insert_event(&post).await.unwrap();

        let edit = create_envelope(
            &identity,
            1,
            Some(post_cid.clone()),
            EventKind::Edit {
                text: "edited".to_string(),
                target: post_cid,
            },
        );
        store.insert_event(&edit).await.unwrap();

        let posts = store.get_posts_by_author(&pubkey_hex).await.unwrap();
        assert_eq!(posts.len(), 1);
        assert_eq!(posts[0].text, "edited");
        assert!(posts[0].edited);
    }

    #[tokio::test]
    async fn delete_projection_updates() {
        let store = make_store().await;
        let identity = Identity::generate();
        let pubkey_hex = bytes_to_hex(&identity.public_key_bytes());

        let post = create_envelope(
            &identity,
            0,
            None,
            EventKind::Post {
                text: "bye".to_string(),
            },
        );
        let post_cid = envelope_cid(&post);
        store.insert_event(&post).await.unwrap();

        let del = create_envelope(
            &identity,
            1,
            Some(post_cid.clone()),
            EventKind::Delete { target: post_cid },
        );
        store.insert_event(&del).await.unwrap();

        let posts = store.get_posts_by_author(&pubkey_hex).await.unwrap();
        assert!(posts[0].deleted);
    }

    #[tokio::test]
    async fn cross_author_edit_is_ignored() {
        let store = make_store().await;
        let victim = Identity::generate();
        let attacker = Identity::generate();
        let victim_hex = bytes_to_hex(&victim.public_key_bytes());

        let post = create_envelope(
            &victim,
            0,
            None,
            EventKind::Post {
                text: "original".to_string(),
            },
        );
        let post_cid = envelope_cid(&post);
        store.insert_event(&post).await.unwrap();

        // 攻撃者が自分のチェーンに、被害者の投稿を target に指す Edit を載せる。
        // イベント自体は正当(署名・チェーン構造OK)なので保存はされるが、
        // fold では無視される(data-model.md §4)
        let forged_edit = create_envelope(
            &attacker,
            0,
            None,
            EventKind::Edit {
                text: "hacked".to_string(),
                target: post_cid,
            },
        );
        store.insert_event(&forged_edit).await.unwrap();

        let posts = store.get_posts_by_author(&victim_hex).await.unwrap();
        assert_eq!(posts.len(), 1);
        assert_eq!(posts[0].text, "original");
        assert!(!posts[0].edited);
    }

    #[tokio::test]
    async fn cross_author_delete_is_ignored() {
        let store = make_store().await;
        let victim = Identity::generate();
        let attacker = Identity::generate();
        let victim_hex = bytes_to_hex(&victim.public_key_bytes());

        let post = create_envelope(
            &victim,
            0,
            None,
            EventKind::Post {
                text: "keep me".to_string(),
            },
        );
        let post_cid = envelope_cid(&post);
        store.insert_event(&post).await.unwrap();

        let forged_delete = create_envelope(
            &attacker,
            0,
            None,
            EventKind::Delete { target: post_cid },
        );
        store.insert_event(&forged_delete).await.unwrap();

        let posts = store.get_posts_by_author(&victim_hex).await.unwrap();
        assert_eq!(posts.len(), 1);
        assert!(!posts[0].deleted);
    }

    #[tokio::test]
    async fn profile_upsert() {
        let store = make_store().await;
        let identity = Identity::generate();
        let pubkey_hex = bytes_to_hex(&identity.public_key_bytes());

        let profile = create_envelope(
            &identity,
            0,
            None,
            EventKind::Profile {
                display_name: "Alice".to_string(),
                bio: "hello".to_string(),
                avatar_cid: None,
            },
        );
        store.insert_event(&profile).await.unwrap();

        let account = store.get_account(&pubkey_hex).await.unwrap().unwrap();
        assert_eq!(account.display_name, "Alice");
    }

    #[tokio::test]
    async fn follow_crud_is_idempotent() {
        let store = make_store().await;

        store.add_follow("aa11", 100).await.unwrap();
        store.add_follow("bb22", 200).await.unwrap();
        // 重複追加は冪等(元の since を保持)
        store.add_follow("aa11", 999).await.unwrap();

        let follows = store.get_follows().await.unwrap();
        assert_eq!(follows.len(), 2);
        // since 降順
        assert_eq!(follows[0].pubkey, "bb22");
        assert_eq!(follows[1].pubkey, "aa11");
        assert_eq!(follows[1].since, 100);
        assert!(follows[0].display_name.is_none());

        store.remove_follow("aa11").await.unwrap();
        let follows = store.get_follows().await.unwrap();
        assert_eq!(follows.len(), 1);
        assert_eq!(follows[0].pubkey, "bb22");
    }

    #[tokio::test]
    async fn follow_includes_display_name_when_known() {
        let store = make_store().await;
        let followee = Identity::generate();
        let followee_hex = bytes_to_hex(&followee.public_key_bytes());

        store.add_follow(&followee_hex, 100).await.unwrap();
        // Profile イベント取り込みで accounts に display_name が入る
        let profile = create_envelope(
            &followee,
            0,
            None,
            EventKind::Profile {
                display_name: "Bob".to_string(),
                bio: String::new(),
                avatar_cid: None,
            },
        );
        store.insert_event(&profile).await.unwrap();

        let follows = store.get_follows().await.unwrap();
        assert_eq!(follows[0].display_name.as_deref(), Some("Bob"));
    }

    #[tokio::test]
    async fn timeline_merges_self_and_follows_only() {
        let store = make_store().await;
        let me = Identity::generate();
        let followee = Identity::generate();
        let stranger = Identity::generate();
        let me_hex = bytes_to_hex(&me.public_key_bytes());
        let followee_hex = bytes_to_hex(&followee.public_key_bytes());

        store.add_follow(&followee_hex, 1).await.unwrap();

        // 各 author が1投稿ずつ。timestamp は create_envelope の現在時刻
        // (ミリ秒精度で同値になりうるため順序は author 集合のみ検証する)
        for who in [&me, &followee, &stranger] {
            let env = create_envelope(
                who,
                0,
                None,
                EventKind::Post {
                    text: "hi".to_string(),
                },
            );
            store.insert_event(&env).await.unwrap();
        }

        let timeline = store.get_timeline(&me_hex).await.unwrap();
        let authors: Vec<&str> = timeline.iter().map(|r| r.author.as_str()).collect();
        assert_eq!(timeline.len(), 2);
        assert!(authors.contains(&me_hex.as_str()));
        assert!(authors.contains(&followee_hex.as_str()));
    }

    #[tokio::test]
    async fn timeline_is_timestamp_desc_with_display_name() {
        let store = make_store().await;
        let me = Identity::generate();
        let followee = Identity::generate();
        let me_hex = bytes_to_hex(&me.public_key_bytes());
        let followee_hex = bytes_to_hex(&followee.public_key_bytes());

        store.add_follow(&followee_hex, 1).await.unwrap();

        // timestamp を制御するため payload を直接組み立てる
        use crate::event::{payload_to_dag_cbor, EventEnvelope, EventPayload};
        fn make_post(identity: &Identity, seq: u64, ts: i64, text: &str) -> EventEnvelope {
            let payload = EventPayload {
                seq,
                kind: EventKind::Post {
                    text: text.to_string(),
                },
                prev: None,
                author: serde_bytes::ByteArray::new(identity.public_key_bytes()),
                timestamp: ts,
            };
            let sig = identity.sign_bytes(&payload_to_dag_cbor(&payload));
            EventEnvelope {
                payload,
                signature: serde_bytes::ByteArray::new(sig),
            }
        }

        let old = make_post(&followee, 0, 1000, "old");
        let new = make_post(&me, 0, 2000, "new");
        store.insert_event(&old).await.unwrap();
        store.insert_event(&new).await.unwrap();

        let profile = create_envelope(
            &followee,
            1,
            Some(envelope_cid(&old)),
            EventKind::Profile {
                display_name: "Carol".to_string(),
                bio: String::new(),
                avatar_cid: None,
            },
        );
        store.insert_event(&profile).await.unwrap();

        let timeline = store.get_timeline(&me_hex).await.unwrap();
        assert_eq!(timeline.len(), 2);
        assert_eq!(timeline[0].text, "new"); // timestamp 降順
        assert_eq!(timeline[1].text, "old");
        assert_eq!(timeline[1].display_name.as_deref(), Some("Carol"));
        assert!(timeline[0].display_name.is_none()); // 自分は Profile 未設定
    }

    #[tokio::test]
    async fn multiple_posts_returned_in_desc_order() {
        let store = make_store().await;
        let identity = Identity::generate();
        let pubkey_hex = bytes_to_hex(&identity.public_key_bytes());

        let env0 = create_envelope(
            &identity,
            0,
            None,
            EventKind::Post {
                text: "first".to_string(),
            },
        );
        let cid0 = envelope_cid(&env0);
        store.insert_event(&env0).await.unwrap();

        let env1 = create_envelope(
            &identity,
            1,
            Some(cid0),
            EventKind::Post {
                text: "second".to_string(),
            },
        );
        store.insert_event(&env1).await.unwrap();

        let posts = store.get_posts_by_author(&pubkey_hex).await.unwrap();
        assert_eq!(posts.len(), 2);
        assert_eq!(posts[0].text, "second");
    }
}
