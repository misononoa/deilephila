-- フォロー相手 + 自分の最新 IPNS-headレコードの常時保持(R2 デフォルトポリシー)。
-- ブロック本体と分離して保持し、発見可能性の床を守る(docs/networking.md §3.2)。
-- M6 の GetLatestHead 応答の源泉にもなる。
CREATE TABLE IF NOT EXISTS head_records (
    pubkey       TEXT    PRIMARY KEY NOT NULL,
    sequence     INTEGER NOT NULL,
    record_bytes BLOB    NOT NULL,
    updated_at   INTEGER NOT NULL
);
