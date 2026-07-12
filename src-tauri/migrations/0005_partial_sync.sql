-- 部分同期(窓・チャンク・再開)のための拡張(docs/networking.md §4.4)。

-- Edit/Delete/Reply が参照する対象イベントの CID。Post 挿入時に先着していた
-- Edit/Delete を引いて projection へ再適用するための索引(docs/data-model.md §6)。
ALTER TABLE events ADD COLUMN target_cid TEXT;
CREATE INDEX IF NOT EXISTS idx_events_target_cid
    ON events(target_cid) WHERE target_cid IS NOT NULL;

-- author 別の遡行同期の進捗。行が無い = 一度も同期を試みていない。
-- cursor_cid/cursor_seq/completed は events から再導出できるキャッシュで、
-- 正典値は window_floor_seq のみ(docs/data-model.md §6)。
CREATE TABLE IF NOT EXISTS sync_state (
    pubkey           TEXT    PRIMARY KEY NOT NULL,
    window_floor_seq INTEGER NOT NULL,
    cursor_cid       TEXT,
    cursor_seq       INTEGER,
    completed        INTEGER NOT NULL DEFAULT 0,
    updated_at       INTEGER NOT NULL
);
