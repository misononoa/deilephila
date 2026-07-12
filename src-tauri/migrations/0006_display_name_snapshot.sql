-- accounts.display_name(チェーン Profile fold 由来 = Tier 1)と IPNS-headレコードの
-- display_name スナップショット(Tier 0)を別列に分離する(data-model.md §3, issue #8)。
-- snapshot_seq は同梱元レコードの sequence。値なしは -1 (実際の sequence は 0 以上)。
ALTER TABLE accounts ADD COLUMN snapshot_display_name TEXT NOT NULL DEFAULT '';
ALTER TABLE accounts ADD COLUMN snapshot_seq INTEGER NOT NULL DEFAULT -1;
