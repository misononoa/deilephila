-- fork(equivocation)の記録(docs/data-model.md §2.1)。
-- 同一 author の同一 seq に対して異なる CID が観測された事実を、矛盾する
-- 2つの署名付き bytes(証拠)ごと保持する。events は保持ポリシーで追い出され、
-- head_records は最良1件しか残らないため、ここが自己完結の証拠保管庫になる。
-- cid_a < cid_b(辞書順)に正規化し、同一ペアの再観測は主キーで排する。
CREATE TABLE IF NOT EXISTS forks (
    author      TEXT    NOT NULL,
    layer       TEXT    NOT NULL,  -- 'event'(チェーン) | 'head'(IPNS-headレコード)
    seq         INTEGER NOT NULL,
    cid_a       TEXT    NOT NULL,
    cid_b       TEXT    NOT NULL,
    evidence_a  BLOB    NOT NULL,
    evidence_b  BLOB    NOT NULL,
    observed_at INTEGER NOT NULL,
    PRIMARY KEY (author, layer, seq, cid_a, cid_b)
);

-- fork 照合(同 author+seq の既存イベント検索)と、get_head /
-- get_chain_run_bottom の author 別 seq 走査のための索引
CREATE INDEX IF NOT EXISTS idx_events_author_seq ON events(author, seq);
