use std::path::Path;

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::SqlitePool;

use crate::event::{envelope_cid, EventEnvelope, EventKind};
use crate::util::{bytes_to_hex, from_dag_cbor, to_dag_cbor};

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

/// author 別の遡行同期の進捗(sync_state 行)。cursor_cid/cursor_seq/completed は
/// events から再導出できるキャッシュで、正典値は window_floor_seq のみ
/// ([data-model.md] §6)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncStateRow {
    pub pubkey: String,
    /// 遡行下限 seq(これ未満のイベントは取得しない)
    pub window_floor_seq: u64,
    /// 最大 seq から prev で連続到達できる区間(run)の最下端イベントの cid
    pub cursor_cid: Option<String>,
    pub cursor_seq: Option<u64>,
    /// 遡行終了(genesis 到達 or 窓下限到達)
    pub completed: bool,
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
        let store = Store { pool };
        store.backfill_target_cid().await?;
        Ok(store)
    }

    pub async fn open_in_memory() -> Result<Self, StoreError> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await?;
        sqlx::migrate!("./migrations").run(&pool).await?;
        let store = Store { pool };
        store.backfill_target_cid().await?;
        Ok(store)
    }

    /// migration 0005 以前に挿入され `target_cid` が未設定の Edit/Delete/Reply 行を
    /// `raw_cbor` から修復し、空振りしていた projection を再適用する。
    /// 冪等で、新規 DB や修復済み DB では0行。
    async fn backfill_target_cid(&self) -> Result<(), StoreError> {
        let rows = sqlx::query!(
            "SELECT raw_cbor FROM events
             WHERE kind_tag IN ('Edit', 'Delete', 'Reply') AND target_cid IS NULL"
        )
        .fetch_all(&self.pool)
        .await?;
        for row in rows {
            let envelope: EventEnvelope =
                from_dag_cbor(&row.raw_cbor).map_err(StoreError::Serialization)?;
            let cid_str = envelope_cid(&envelope).to_string();
            let author = bytes_to_hex(envelope.payload.author.as_ref());
            let seq = envelope.payload.seq as i64;
            let mut tx = self.pool.begin().await?;
            match &envelope.payload.kind {
                EventKind::Edit { target, text } => {
                    let target_str = target.to_string();
                    sqlx::query!(
                        "UPDATE events SET target_cid = ? WHERE cid = ?",
                        target_str,
                        cid_str
                    )
                    .execute(&mut *tx)
                    .await?;
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
                        "UPDATE events SET target_cid = ? WHERE cid = ?",
                        target_str,
                        cid_str
                    )
                    .execute(&mut *tx)
                    .await?;
                    sqlx::query!(
                        "UPDATE posts SET deleted = 1 WHERE cid = ? AND author = ?",
                        target_str,
                        author
                    )
                    .execute(&mut *tx)
                    .await?;
                }
                EventKind::Reply { target, .. } => {
                    let target_str = target.to_string();
                    sqlx::query!(
                        "UPDATE events SET target_cid = ? WHERE cid = ?",
                        target_str,
                        cid_str
                    )
                    .execute(&mut *tx)
                    .await?;
                }
                _ => {}
            }
            tx.commit().await?;
        }
        Ok(())
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
        let target_str = match &envelope.payload.kind {
            EventKind::Edit { target, .. }
            | EventKind::Delete { target }
            | EventKind::Reply { target, .. } => Some(target.to_string()),
            _ => None,
        };

        let mut tx = self.pool.begin().await?;

        sqlx::query!(
            "INSERT OR IGNORE INTO events
             (cid, author, seq, prev_cid, timestamp, kind_tag, kind_json, raw_cbor, target_cid)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            cid_str,
            author,
            seq,
            prev_str,
            timestamp,
            kind_tag,
            kind_json,
            raw_cbor,
            target_str
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
                // 遅延適用: この Post より先に到着していた Edit/Delete を反映する。
                // 部分同期ではチャンク跨ぎで Edit/Delete が対象 Post より先に挿入
                // されうるため、挿入順序に依存せず projection を収束させる
                // (docs/data-model.md §6)
                apply_pending_ops(&mut tx, &cid_str, &author).await?;
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

    /// アカウントを取得する。`display_name` は表示解決済みの値(チェーンの
    /// Profile fold = Tier 1 があればそれ、なければ IPNS-headレコードの
    /// スナップショット = Tier 0、どちらも無ければ空文字列。[data-model.md] §3)。
    pub async fn get_account(&self, pubkey_hex: &str) -> Result<Option<AccountRow>, StoreError> {
        let row = sqlx::query!(
            r#"SELECT pubkey,
               COALESCE(NULLIF(display_name, ''), NULLIF(snapshot_display_name, ''), '')
                   AS "display_name!: String",
               bio, latest_head_cid, last_seen
             FROM accounts WHERE pubkey = ?"#,
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

    /// フォロー一覧を返す(新しい順)。display_name は accounts にあれば同梱
    /// (Tier 1 のチェーン fold を優先し、無ければ Tier 0 のスナップショット。
    /// [data-model.md] §3)。
    pub async fn get_follows(&self) -> Result<Vec<FollowRow>, StoreError> {
        let rows = sqlx::query!(
            r#"SELECT f.pubkey, f.since,
               COALESCE(NULLIF(a.display_name, ''), NULLIF(a.snapshot_display_name, ''))
                   AS "display_name?: String"
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
               COALESCE(NULLIF(a.display_name, ''), NULLIF(a.snapshot_display_name, ''))
                   AS "display_name?: String"
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

    /// IPNS-headレコードを保存する。既知レコードを (sequence, validity) の辞書式で
    /// 上回るものだけ反映する(stale の巻き戻しを拒否しつつ、sequence を変えず
    /// validity のみ更新する republish は受理する。[networking.md] §4.2)。
    /// フォロー相手 + 自分の最新レコードの常時保持([networking.md] §3.2)の実体で、
    /// M6 の GetLatestHead 応答の源泉にもなる。
    pub async fn upsert_head_record(
        &self,
        pubkey_hex: &str,
        sequence: u64,
        validity: i64,
        record_bytes: &[u8],
        now_ms: i64,
    ) -> Result<(), StoreError> {
        let seq = sequence as i64;
        sqlx::query!(
            "INSERT INTO head_records (pubkey, sequence, validity, record_bytes, updated_at)
             VALUES (?, ?, ?, ?, ?)
             ON CONFLICT(pubkey) DO UPDATE SET
                 sequence = excluded.sequence,
                 validity = excluded.validity,
                 record_bytes = excluded.record_bytes,
                 updated_at = excluded.updated_at
             WHERE excluded.sequence > head_records.sequence
                OR (excluded.sequence = head_records.sequence
                    AND excluded.validity > head_records.validity)",
            pubkey_hex,
            seq,
            validity,
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

    /// IPNS-headレコードの display_name スナップショット(Tier 0)を
    /// `accounts.snapshot_display_name` に反映する。チェーンの Profile fold
    /// (`accounts.display_name`, Tier 1)とは別列で、出所を混同しない
    /// ([data-model.md] §3)。同梱元レコードの `sequence` がこれまでの
    /// スナップショットより新しいときだけ上書きする(古いレコードでの
    /// 巻き戻しを防ぐ)。表示解決(fold 優先、なければスナップショット)は
    /// 読み出し側([get_account]・[Store::get_follows]・[Store::get_timeline])の責務。
    pub async fn fill_display_name_snapshot(
        &self,
        pubkey_hex: &str,
        sequence: u64,
        display_name: &str,
    ) -> Result<(), StoreError> {
        let seq = sequence as i64;
        sqlx::query!(
            "INSERT INTO accounts (pubkey, snapshot_display_name, snapshot_seq)
             VALUES (?, ?, ?)
             ON CONFLICT(pubkey) DO UPDATE SET
                 snapshot_display_name = excluded.snapshot_display_name,
                 snapshot_seq = excluded.snapshot_seq
             WHERE excluded.snapshot_seq > accounts.snapshot_seq",
            pubkey_hex,
            display_name,
            seq
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// author 別の遡行同期の進捗を取得する。行が無い = 一度も同期を試みていない。
    pub async fn get_sync_state(
        &self,
        pubkey_hex: &str,
    ) -> Result<Option<SyncStateRow>, StoreError> {
        let row = sqlx::query!(
            "SELECT pubkey, window_floor_seq, cursor_cid, cursor_seq,
             completed AS \"completed: bool\"
             FROM sync_state WHERE pubkey = ?",
            pubkey_hex
        )
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| SyncStateRow {
            pubkey: r.pubkey,
            window_floor_seq: r.window_floor_seq as u64,
            cursor_cid: r.cursor_cid,
            cursor_seq: r.cursor_seq.map(|s| s as u64),
            completed: r.completed,
        }))
    }

    /// 遡行同期の進捗を保存する(全フィールド上書き)。
    pub async fn upsert_sync_state(
        &self,
        row: &SyncStateRow,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        let floor = row.window_floor_seq as i64;
        let cursor_seq = row.cursor_seq.map(|s| s as i64);
        sqlx::query!(
            "INSERT INTO sync_state
             (pubkey, window_floor_seq, cursor_cid, cursor_seq, completed, updated_at)
             VALUES (?, ?, ?, ?, ?, ?)
             ON CONFLICT(pubkey) DO UPDATE SET
                 window_floor_seq = excluded.window_floor_seq,
                 cursor_cid       = excluded.cursor_cid,
                 cursor_seq       = excluded.cursor_seq,
                 completed        = excluded.completed,
                 updated_at       = excluded.updated_at",
            row.pubkey,
            floor,
            row.cursor_cid,
            cursor_seq,
            row.completed,
            now_ms
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// author のチェーンで「最大 seq のイベントから prev を辿って連続到達できる
    /// 区間(run)」の最下端を (cid, prev_cid, seq) で返す。イベントが無ければ None。
    /// 部分同期の再開カーソルはこの導出値を正とする(書き込んだカーソルを
    /// 信頼すると、既知ブロック到達による区間の合流や中断による分断と不整合になる)。
    pub async fn get_chain_run_bottom(
        &self,
        pubkey_hex: &str,
    ) -> Result<Option<(String, Option<String>, u64)>, StoreError> {
        let row = sqlx::query!(
            "WITH RECURSIVE run(cid, prev_cid, seq) AS (
                 SELECT cid, prev_cid, seq FROM (
                     SELECT cid, prev_cid, seq FROM events
                     WHERE author = ? ORDER BY seq DESC LIMIT 1
                 )
                 UNION ALL
                 SELECT e.cid, e.prev_cid, e.seq
                 FROM events e JOIN run r ON e.cid = r.prev_cid
             )
             SELECT cid AS \"cid!: String\", prev_cid, seq AS \"seq!: i64\"
             FROM run ORDER BY seq ASC LIMIT 1",
            pubkey_hex
        )
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| (r.cid, r.prev_cid, r.seq as u64)))
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

/// 対象 Post の挿入時に、先に events へ到着していた同一 author の Edit/Delete を
/// projection へ適用する(遅延適用)。Edit は seq 最大の1件(last-write-wins)。
/// text は kind_json ではなく raw_cbor(正典)から取り出す。冪等。
async fn apply_pending_ops(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    post_cid: &str,
    author: &str,
) -> Result<(), StoreError> {
    let edit = sqlx::query!(
        "SELECT seq, raw_cbor FROM events
         WHERE target_cid = ? AND author = ? AND kind_tag = 'Edit'
         ORDER BY seq DESC LIMIT 1",
        post_cid,
        author
    )
    .fetch_optional(&mut **tx)
    .await?;
    if let Some(row) = edit {
        let envelope: EventEnvelope =
            from_dag_cbor(&row.raw_cbor).map_err(StoreError::Serialization)?;
        if let EventKind::Edit { text, .. } = &envelope.payload.kind {
            sqlx::query!(
                "UPDATE posts SET text = ?, edited = 1, latest_edit_seq = ?
                 WHERE cid = ? AND latest_edit_seq < ?",
                text,
                row.seq,
                post_cid,
                row.seq
            )
            .execute(&mut **tx)
            .await?;
        }
    }

    sqlx::query!(
        "UPDATE posts SET deleted = 1
         WHERE cid = ?
           AND EXISTS (SELECT 1 FROM events
                       WHERE target_cid = ? AND author = ? AND kind_tag = 'Delete')",
        post_cid,
        post_cid,
        author
    )
    .execute(&mut **tx)
    .await?;
    Ok(())
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
    async fn display_name_snapshot_updates_on_newer_sequence() {
        // issue #8: 一度書かれたスナップショットが改名後の新しいレコードで
        // 更新されることを検証する(旧実装は WHERE display_name = '' ガードで
        // 二度と更新されなかった)
        let store = make_store().await;
        let pubkey_hex = "aa11";

        store
            .fill_display_name_snapshot(pubkey_hex, 3, "Alice")
            .await
            .unwrap();
        let account = store.get_account(pubkey_hex).await.unwrap().unwrap();
        assert_eq!(account.display_name, "Alice");

        // より新しい sequence の改名レコード → 上書きされる
        store
            .fill_display_name_snapshot(pubkey_hex, 4, "Alice2")
            .await
            .unwrap();
        let account = store.get_account(pubkey_hex).await.unwrap().unwrap();
        assert_eq!(account.display_name, "Alice2");

        // 既知より古い/同じ sequence のレコード → 巻き戻されない
        store
            .fill_display_name_snapshot(pubkey_hex, 4, "Mallory")
            .await
            .unwrap();
        store
            .fill_display_name_snapshot(pubkey_hex, 2, "Mallory")
            .await
            .unwrap();
        let account = store.get_account(pubkey_hex).await.unwrap().unwrap();
        assert_eq!(account.display_name, "Alice2");
    }

    #[tokio::test]
    async fn display_name_snapshot_never_overrides_chain_fold() {
        // 不一致時はチェーンが勝つ(data-model.md §3): Tier 1(Profile fold)が
        // あれば、より新しい sequence の Tier 0 スナップショットが来ても
        // 表示上はチェーン側の値を優先する
        let store = make_store().await;
        let identity = Identity::generate();
        let pubkey_hex = bytes_to_hex(&identity.public_key_bytes());

        let profile = create_envelope(
            &identity,
            0,
            None,
            EventKind::Profile {
                display_name: "ChainName".to_string(),
                bio: String::new(),
                avatar_cid: None,
            },
        );
        store.insert_event(&profile).await.unwrap();

        store
            .fill_display_name_snapshot(&pubkey_hex, 99, "SnapshotName")
            .await
            .unwrap();

        let account = store.get_account(&pubkey_hex).await.unwrap().unwrap();
        assert_eq!(account.display_name, "ChainName");
    }

    #[tokio::test]
    async fn head_record_upsert_follows_sequence_validity_order() {
        let store = make_store().await;
        store
            .upsert_head_record("aa", 5, 100, b"v1", 1)
            .await
            .unwrap();

        // republish(同一 sequence で validity のみ新しい)は反映される
        store
            .upsert_head_record("aa", 5, 200, b"v2", 2)
            .await
            .unwrap();
        let (seq, bytes) = store.get_head_record("aa").await.unwrap().unwrap();
        assert_eq!((seq, bytes.as_slice()), (5, b"v2".as_slice()));

        // stale は無視: 同 sequence の古い/同じ validity、低い sequence
        store
            .upsert_head_record("aa", 5, 150, b"v3", 3)
            .await
            .unwrap();
        store
            .upsert_head_record("aa", 5, 200, b"v4", 4)
            .await
            .unwrap();
        store
            .upsert_head_record("aa", 4, 999, b"v5", 5)
            .await
            .unwrap();
        let (seq, bytes) = store.get_head_record("aa").await.unwrap().unwrap();
        assert_eq!((seq, bytes.as_slice()), (5, b"v2".as_slice()));

        // sequence が上がれば validity に依らず反映(辞書式の主キーは sequence)
        store
            .upsert_head_record("aa", 6, 50, b"v6", 6)
            .await
            .unwrap();
        let (seq, bytes) = store.get_head_record("aa").await.unwrap().unwrap();
        assert_eq!((seq, bytes.as_slice()), (6, b"v6".as_slice()));
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

    // --- Edit/Delete の遅延適用(部分同期でのチャンク跨ぎ順序逆転への耐性) ---

    #[tokio::test]
    async fn edit_before_post_is_applied_on_post_insert() {
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

        let edit = create_envelope(
            &identity,
            1,
            Some(post_cid.clone()),
            EventKind::Edit {
                text: "edited".to_string(),
                target: post_cid,
            },
        );

        // Edit が対象 Post より先に到着(チャンク跨ぎの順序逆転)
        store.insert_event(&edit).await.unwrap();
        store.insert_event(&post).await.unwrap();

        let posts = store.get_posts_by_author(&pubkey_hex).await.unwrap();
        assert_eq!(posts.len(), 1);
        assert_eq!(posts[0].text, "edited");
        assert!(posts[0].edited);
    }

    #[tokio::test]
    async fn delete_before_post_is_applied_on_post_insert() {
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

        let del = create_envelope(
            &identity,
            1,
            Some(post_cid.clone()),
            EventKind::Delete { target: post_cid },
        );

        store.insert_event(&del).await.unwrap();
        store.insert_event(&post).await.unwrap();

        let posts = store.get_posts_by_author(&pubkey_hex).await.unwrap();
        assert!(posts[0].deleted);
    }

    #[tokio::test]
    async fn edit_lww_is_insertion_order_independent() {
        let store = make_store().await;
        let identity = Identity::generate();
        let pubkey_hex = bytes_to_hex(&identity.public_key_bytes());

        let post = create_envelope(
            &identity,
            0,
            None,
            EventKind::Post {
                text: "v0".to_string(),
            },
        );
        let post_cid = envelope_cid(&post);
        let make_edit = |seq: u64, text: &str| {
            create_envelope(
                &identity,
                seq,
                Some(post_cid.clone()),
                EventKind::Edit {
                    text: text.to_string(),
                    target: post_cid.clone(),
                },
            )
        };

        // seq5 → seq3 → Post の順で挿入しても LWW(seq 最大)が勝つ
        store.insert_event(&make_edit(5, "v5")).await.unwrap();
        store.insert_event(&make_edit(3, "v3")).await.unwrap();
        store.insert_event(&post).await.unwrap();
        let posts = store.get_posts_by_author(&pubkey_hex).await.unwrap();
        assert_eq!(posts[0].text, "v5");

        // 後から届いた古い Edit(seq4)は無効、新しい Edit(seq6)は有効
        store.insert_event(&make_edit(4, "v4")).await.unwrap();
        let posts = store.get_posts_by_author(&pubkey_hex).await.unwrap();
        assert_eq!(posts[0].text, "v5");

        store.insert_event(&make_edit(6, "v6")).await.unwrap();
        let posts = store.get_posts_by_author(&pubkey_hex).await.unwrap();
        assert_eq!(posts[0].text, "v6");
    }

    /// 直接適用経路の author 強制は cross_author_edit_is_ignored /
    /// cross_author_delete_is_ignored が検証する。ここは遅延適用経路
    /// (対象 Post より先に到着した場合)でも同じ強制が働くことの検証。
    #[tokio::test]
    async fn cross_author_pending_ops_are_ignored() {
        let store = make_store().await;
        let alice = Identity::generate();
        let alice_hex = bytes_to_hex(&alice.public_key_bytes());
        let mallory = Identity::generate();

        let post = create_envelope(
            &alice,
            0,
            None,
            EventKind::Post {
                text: "alice's post".to_string(),
            },
        );
        let post_cid = envelope_cid(&post);

        // 他 author の Edit/Delete が対象 Post より先に届いている状態
        let evil_edit = create_envelope(
            &mallory,
            0,
            None,
            EventKind::Edit {
                text: "hacked".to_string(),
                target: post_cid.clone(),
            },
        );
        let evil_delete = create_envelope(
            &mallory,
            1,
            None,
            EventKind::Delete { target: post_cid },
        );
        store.insert_event(&evil_edit).await.unwrap();
        store.insert_event(&evil_delete).await.unwrap();

        // Post 挿入時の遅延適用でも他 author の Edit/Delete は無視される
        store.insert_event(&post).await.unwrap();

        let posts = store.get_posts_by_author(&alice_hex).await.unwrap();
        assert_eq!(posts.len(), 1);
        assert_eq!(posts[0].text, "alice's post");
        assert!(!posts[0].edited);
        assert!(!posts[0].deleted);
    }

    #[tokio::test]
    async fn pending_ops_are_idempotent_on_reinsert() {
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
        store.insert_event(&post).await.unwrap();
        // 同じイベントの再挿入(部分同期の再開で起こりうる)で結果が変わらない
        store.insert_event(&post).await.unwrap();
        store.insert_event(&edit).await.unwrap();

        let posts = store.get_posts_by_author(&pubkey_hex).await.unwrap();
        assert_eq!(posts.len(), 1);
        assert_eq!(posts[0].text, "edited");
    }

    #[tokio::test]
    async fn backfill_repairs_target_cid_and_projection() {
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
        let post_cid_str = post_cid.to_string();
        let edit = create_envelope(
            &identity,
            1,
            Some(post_cid.clone()),
            EventKind::Edit {
                text: "edited".to_string(),
                target: post_cid,
            },
        );
        store.insert_event(&post).await.unwrap();
        store.insert_event(&edit).await.unwrap();

        // migration 0005 以前の状態を再現: target_cid なし + projection 空振り
        sqlx::query!("UPDATE events SET target_cid = NULL WHERE kind_tag = 'Edit'")
            .execute(&store.pool)
            .await
            .unwrap();
        sqlx::query!(
            "UPDATE posts SET text = 'original', edited = 0, latest_edit_seq = 0
             WHERE cid = ?",
            post_cid_str
        )
        .execute(&store.pool)
        .await
        .unwrap();

        store.backfill_target_cid().await.unwrap();

        let posts = store.get_posts_by_author(&pubkey_hex).await.unwrap();
        assert_eq!(posts[0].text, "edited");
        assert!(posts[0].edited);
        let remaining = sqlx::query!(
            "SELECT COUNT(*) AS n FROM events WHERE kind_tag = 'Edit' AND target_cid IS NULL"
        )
        .fetch_one(&store.pool)
        .await
        .unwrap();
        assert_eq!(remaining.n, 0);
    }
}
