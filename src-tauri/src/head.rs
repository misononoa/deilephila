use cid::Cid;
use serde::{Deserialize, Serialize};

use crate::event::VerifyError;
use crate::identity::{verify_signature, Identity};

/// 最小署名付き head 通知。IPNS-headレコードの前方互換サブセットで、M5 で
/// EOL/protobuf を備えた正式レコードへ格上げされる(docs/networking.md §4.1)。
/// gossipsub topic `deilephila/feed/<pubkey_hex>` に DAG-CBOR で流す。
///
/// `seq` は head イベントの `seq` を流用する(単調増加なので argmax統一規則が
/// そのまま機能し、M5 の IPNS `sequence` へ引き継げる)。

// フィールド宣言順は DAG-CBOR canonical 順(キー長昇順)に合わせてある
// seq(3) < pubkey(6) < head_cid(8)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeadPayload {
    pub seq: u64,
    pub pubkey: serde_bytes::ByteArray<32>,
    pub head_cid: Cid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeadAnnounce {
    pub payload: HeadPayload,
    pub signature: serde_bytes::ByteArray<64>, // payload の canonical DAG-CBOR への Ed25519 署名
}

pub fn payload_to_dag_cbor(payload: &HeadPayload) -> Vec<u8> {
    serde_ipld_dagcbor::to_vec(payload).expect("HeadPayload DAG-CBOR serialization failed")
}

/// HeadAnnounce を署名付きで生成する。
pub fn create_head_announce(identity: &Identity, seq: u64, head_cid: Cid) -> HeadAnnounce {
    let payload = HeadPayload {
        seq,
        pubkey: serde_bytes::ByteArray::new(identity.public_key_bytes()),
        head_cid,
    };
    let cbor = payload_to_dag_cbor(&payload);
    let sig = identity.sign_bytes(&cbor);
    HeadAnnounce {
        payload,
        signature: serde_bytes::ByteArray::new(sig),
    }
}

/// 署名を検証する。pubkey は payload 内のものを使う(自己完結検証)。
pub fn verify_head_announce(announce: &HeadAnnounce) -> Result<(), VerifyError> {
    let cbor = payload_to_dag_cbor(&announce.payload);
    verify_signature(
        announce.payload.pubkey.as_ref(),
        &cbor,
        announce.signature.as_ref(),
    )
    .map_err(|_| VerifyError::InvalidSignature)
}

// --- gossipsub メッセージ本体との相互変換 ---

pub fn announce_to_bytes(announce: &HeadAnnounce) -> Vec<u8> {
    serde_ipld_dagcbor::to_vec(announce).expect("HeadAnnounce DAG-CBOR serialization failed")
}

pub fn announce_from_bytes(data: &[u8]) -> Result<HeadAnnounce, String> {
    serde_ipld_dagcbor::from_slice(data).map_err(|e| e.to_string())
}

/// アカウント公開鍵(hex)から gossipsub トピック名を導出する。
pub fn feed_topic_str(pubkey_hex: &str) -> String {
    format!("deilephila/feed/{pubkey_hex}")
}

// --- テスト ---

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::bytes_to_cid;

    fn make_announce() -> (Identity, HeadAnnounce) {
        let identity = Identity::generate();
        let head_cid = bytes_to_cid(b"some block");
        let announce = create_head_announce(&identity, 3, head_cid);
        (identity, announce)
    }

    #[test]
    fn sign_and_verify_ok() {
        let (identity, announce) = make_announce();
        assert!(verify_head_announce(&announce).is_ok());
        assert_eq!(announce.payload.seq, 3);
        assert_eq!(
            announce.payload.pubkey.as_ref(),
            &identity.public_key_bytes()
        );
    }

    #[test]
    fn tampered_seq_fails() {
        let (_, mut announce) = make_announce();
        announce.payload.seq += 1;
        assert!(verify_head_announce(&announce).is_err());
    }

    #[test]
    fn tampered_head_cid_fails() {
        let (_, mut announce) = make_announce();
        announce.payload.head_cid = bytes_to_cid(b"another block");
        assert!(verify_head_announce(&announce).is_err());
    }

    #[test]
    fn wrong_key_signature_fails() {
        let (_, announce) = make_announce();
        let other = Identity::generate();
        let mut forged = announce.clone();
        forged.payload.pubkey = serde_bytes::ByteArray::new(other.public_key_bytes());
        assert!(verify_head_announce(&forged).is_err());
    }

    #[test]
    fn roundtrip_is_deterministic() {
        let (_, announce) = make_announce();
        let bytes = announce_to_bytes(&announce);
        let recovered = announce_from_bytes(&bytes).unwrap();
        // 再シリアライズで同一バイト列(canonical DAG-CBOR の決定性)
        assert_eq!(announce_to_bytes(&recovered), bytes);
        assert!(verify_head_announce(&recovered).is_ok());
    }

    #[test]
    fn garbage_bytes_rejected() {
        assert!(announce_from_bytes(b"not cbor at all \xff\xff").is_err());
    }

    #[test]
    fn topic_name_format() {
        assert_eq!(feed_topic_str("ab01"), "deilephila/feed/ab01");
    }
}
