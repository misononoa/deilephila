-- republish は sequence を変えず validity のみ更新するため、レコード受理判定を
-- (sequence, validity) の辞書式比較で行う(docs/networking.md §4.2)。
-- 既存行の DEFAULT 0 は「validity 不明の旧レコード」で、同 sequence の
-- 再受信で必ず上書きされる側に倒れる。
ALTER TABLE head_records ADD COLUMN validity INTEGER NOT NULL DEFAULT 0;
