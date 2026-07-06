CREATE TABLE IF NOT EXISTS events (
    cid       TEXT    PRIMARY KEY NOT NULL,
    author    TEXT    NOT NULL,
    seq       INTEGER NOT NULL,
    prev_cid  TEXT,
    timestamp INTEGER NOT NULL,
    kind_tag  TEXT    NOT NULL,
    kind_json TEXT    NOT NULL,
    raw_cbor  BLOB    NOT NULL
);

CREATE TABLE IF NOT EXISTS posts (
    cid             TEXT    PRIMARY KEY NOT NULL,
    author          TEXT    NOT NULL,
    seq             INTEGER NOT NULL,
    text            TEXT    NOT NULL,
    timestamp       INTEGER NOT NULL,
    edited          INTEGER NOT NULL DEFAULT 0,
    deleted         INTEGER NOT NULL DEFAULT 0,
    latest_edit_seq INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS accounts (
    pubkey          TEXT    PRIMARY KEY NOT NULL,
    display_name    TEXT    NOT NULL DEFAULT '',
    bio             TEXT    NOT NULL DEFAULT '',
    latest_head_cid TEXT,
    last_seen       INTEGER NOT NULL DEFAULT 0
);
