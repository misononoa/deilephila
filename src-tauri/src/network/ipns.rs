//! IPNS-headレコードの DHT 搬送まわり(networking.md §4.2): レコードキーの導出、
//! EOL の kad 失効時刻への換算、他ノードから put されたレコードの格納前検証。

use std::time::{Duration, Instant};

use libp2p::kad;

use crate::head::{record_from_bytes, verify_ipns_record};
use crate::util::{bytes_to_hex, now_ms};

/// アカウント公開鍵から DHT レコードキーを導出する。
/// IPNS名 = アカウント公開鍵(data-model.md §2.4)の kad 上の表現
pub(crate) fn head_record_key(pubkey: &[u8; 32]) -> kad::RecordKey {
    kad::RecordKey::new(&format!("/deilephila/head/{}", bytes_to_hex(pubkey)))
}

/// レコードの validity(EOL、Unix epoch ミリ秒)を kad ローカル store の
/// 失効時刻へ換算する。EOL を過ぎたレコードは DHT から自然に消え、
/// 生存させるには発信者の定期 republish が要る(networking.md §4.2)
pub(crate) fn expires_from_validity(validity_ms: i64) -> Option<Instant> {
    let ttl_ms = validity_ms.saturating_sub(now_ms()).max(0) as u64;
    Some(Instant::now() + Duration::from_millis(ttl_ms))
}

/// 他ノードから put されたレコードを store へ格納する前に検証する
/// (`StoreInserts::FilterBoth` の受理判定)。受理条件:
/// - `IpnsRecord` としてデコードでき、署名検証に成功する(自己完結検証)
/// - キーが payload の `name` から導出したものと一致する(他人のキーの汚染を拒否)
/// - 既に保持しているレコードの sequence を上回る(stale put による巻き戻しを拒否)
/// 受理時は validity から失効時刻を再計算したレコードを返す
/// (送信者申告の expires を信用しない)。
pub(crate) fn validate_inbound_head_record(
    record: &kad::Record,
    existing_seq: Option<u64>,
) -> Result<kad::Record, String> {
    let decoded = record_from_bytes(&record.value).map_err(|e| format!("undecodable: {e}"))?;
    verify_ipns_record(&decoded).map_err(|_| "invalid signature".to_string())?;
    if record.key != head_record_key(decoded.payload.name.as_ref()) {
        return Err("key does not match record name".to_string());
    }
    if let Some(known) = existing_seq {
        if decoded.payload.sequence <= known {
            return Err(format!(
                "stale sequence {} (known {known})",
                decoded.payload.sequence
            ));
        }
    }
    let mut accepted = record.clone();
    accepted.expires = expires_from_validity(decoded.payload.validity);
    Ok(accepted)
}

// --- テスト ---

#[cfg(test)]
mod tests {
    use super::*;
    use crate::head::{create_ipns_record, record_to_bytes, IpnsRecord};
    use crate::identity::Identity;
    use crate::util::bytes_to_cid;

    fn make_record(identity: &Identity, sequence: u64) -> IpnsRecord {
        create_ipns_record(
            identity,
            sequence,
            bytes_to_cid(b"head block"),
            now_ms() + 3_600_000,
            None,
            "Alice".to_string(),
        )
    }

    #[test]
    fn inbound_record_validation_rules() {
        let id = Identity::generate();
        let pubkey = id.public_key_bytes();
        let record = make_record(&id, 5);
        let kad_record = kad::Record {
            key: head_record_key(&pubkey),
            value: record_to_bytes(&record),
            publisher: None,
            expires: None,
        };

        // 新規(既知レコードなし)は受理され、validity 由来の失効時刻が付く
        let accepted = validate_inbound_head_record(&kad_record, None).unwrap();
        assert!(accepted.expires.is_some());

        // 既知 seq を上回れば受理、同じ・下回るは stale として拒否
        assert!(validate_inbound_head_record(&kad_record, Some(4)).is_ok());
        assert!(validate_inbound_head_record(&kad_record, Some(5)).is_err());
        assert!(validate_inbound_head_record(&kad_record, Some(6)).is_err());

        // 改ざんレコード(署名不一致)は拒否
        let mut tampered = record.clone();
        tampered.payload.sequence = 9;
        let bad = kad::Record {
            key: head_record_key(&pubkey),
            value: record_to_bytes(&tampered),
            publisher: None,
            expires: None,
        };
        assert!(validate_inbound_head_record(&bad, None).is_err());

        // 別アカウントのキーへの格納(キー汚染)は拒否
        let other = Identity::generate();
        let wrong_key = kad::Record {
            key: head_record_key(&other.public_key_bytes()),
            value: record_to_bytes(&record),
            publisher: None,
            expires: None,
        };
        assert!(validate_inbound_head_record(&wrong_key, None).is_err());

        // デコード不能なゴミは拒否
        let garbage = kad::Record {
            key: head_record_key(&pubkey),
            value: b"not cbor \xff".to_vec(),
            publisher: None,
            expires: None,
        };
        assert!(validate_inbound_head_record(&garbage, None).is_err());
    }
}
