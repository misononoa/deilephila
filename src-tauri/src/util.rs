//! 横断ヘルパー: 時刻・canonical DAG-CBOR コーデック・hex 変換。
//!
//! DAG-CBOR と CID の扱いはドメイン型ごとに散らばりやすいので、
//! 「シリアライズ → ハッシュ → CID」の正準経路をここに一本化する
//! (docs/data-model.md §2.1)。

use cid::Cid;
use multihash_codetable::{Code, MultihashDigest};
use serde::de::DeserializeOwned;
use serde::Serialize;

const DAG_CBOR_CODEC: u64 = 0x71;

/// 現在時刻(Unix epoch ミリ秒)。
/// エポック以前を指す時計は環境異常なので、0 等で握りつぶさず早期に失敗させる
/// (署名タイムスタンプや EOL 計算に黙って不正値が混入するのを防ぐ)。
pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_millis() as i64
}

/// canonical DAG-CBOR へシリアライズする。
/// 対象は自前定義のドメイン型のみで、失敗は型定義のバグなので panic とする。
pub fn to_dag_cbor<T: Serialize>(value: &T) -> Vec<u8> {
    serde_ipld_dagcbor::to_vec(value).expect("DAG-CBOR serialization failed")
}

/// DAG-CBOR バイト列からデシリアライズする(ネットワーク受信データ用)。
pub fn from_dag_cbor<T: DeserializeOwned>(data: &[u8]) -> Result<T, String> {
    serde_ipld_dagcbor::from_slice(data).map_err(|e| e.to_string())
}

/// バイト列の SHA2-256 から DAG-CBOR コーデックの CIDv1 を作る。
pub fn bytes_to_cid(bytes: &[u8]) -> Cid {
    let mh = Code::Sha2_256.digest(bytes);
    Cid::new_v1(DAG_CBOR_CODEC, mh)
}

/// バイト列を小文字 hex 文字列へ変換する(公開鍵の SQLite キー/トピック名表現)。
pub fn bytes_to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// 64桁 hex を公開鍵バイト列へ変換する(不正な形式は None)。
pub fn hex_to_pubkey(hex: &str) -> Option<[u8; 32]> {
    if hex.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(hex.get(2 * i..2 * i + 2)?, 16).ok()?;
    }
    Some(out)
}
