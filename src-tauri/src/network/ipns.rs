//! IPNS-headレコードの DHT 搬送まわり(networking.md §4.2): レコードキーの導出、
//! EOL の kad 失効時刻への換算、他ノードから put されたレコードの格納前検証。

use std::time::{Duration, Instant};

use libp2p::{gossipsub, kad};

use crate::head::{feed_topic_str, record_from_bytes, verify_ipns_record, IpnsRecord};
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

/// gossipsub で届いたレコードが、届いたトピックの持ち主のものかを判定する
/// (networking.md §4.1: topic `deilephila/feed/<pubkey>` にはその pubkey の
/// レコードだけが流れる)。不一致は、フォロー相手のトピックに別アカウントの
/// レコードを流し込む攻撃(フォロー外チェーンの取り込み誘導)なので破棄する。
pub(crate) fn record_matches_topic(record: &IpnsRecord, topic: &gossipsub::TopicHash) -> bool {
    let expected = feed_topic_str(&bytes_to_hex(record.payload.name.as_ref()));
    gossipsub::IdentTopic::new(expected).hash() == *topic
}

/// 他ノードから put されたレコードを store へ格納する前に検証する
/// (`StoreInserts::FilterBoth` の受理判定)。受理条件:
/// - `IpnsRecord` としてデコードでき、署名検証に成功する(自己完結検証)
/// - キーが payload の `name` から導出したものと一致する(他人のキーの汚染を拒否)
/// - 既に保持しているレコードを (sequence, validity) の辞書式で上回る
///   (stale put による巻き戻しを拒否しつつ、sequence を変えず validity のみ
///   更新する republish は受理する。networking.md §4.2)
/// 受理時は validity から失効時刻を再計算したレコードを返す
/// (送信者申告の expires を信用しない)。
pub(crate) fn validate_inbound_head_record(
    record: &kad::Record,
    existing: Option<(u64, i64)>,
) -> Result<kad::Record, String> {
    let decoded = record_from_bytes(&record.value).map_err(|e| format!("undecodable: {e}"))?;
    verify_ipns_record(&decoded).map_err(|_| "invalid signature".to_string())?;
    if record.key != head_record_key(decoded.payload.name.as_ref()) {
        return Err("key does not match record name".to_string());
    }
    if let Some((known_seq, known_validity)) = existing {
        if (decoded.payload.sequence, decoded.payload.validity) <= (known_seq, known_validity) {
            return Err(format!(
                "stale record (sequence {}, validity {}) (known: sequence {known_seq}, validity {known_validity})",
                decoded.payload.sequence, decoded.payload.validity
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
    use crate::head::record_to_bytes;
    use crate::identity::Identity;
    use crate::testutil::{far_future_ms, make_record};

    #[test]
    fn record_topic_match_rules() {
        let id = Identity::generate();
        let other = Identity::generate();
        let record = make_record(&id, 1, far_future_ms());

        let own_topic = gossipsub::IdentTopic::new(crate::head::feed_topic_str(&bytes_to_hex(
            &id.public_key_bytes(),
        )))
        .hash();
        let other_topic = gossipsub::IdentTopic::new(crate::head::feed_topic_str(&bytes_to_hex(
            &other.public_key_bytes(),
        )))
        .hash();
        let unrelated_topic = gossipsub::IdentTopic::new("deilephila/unrelated").hash();

        assert!(record_matches_topic(&record, &own_topic));
        // 別アカウントのトピックへの流し込みは不一致
        assert!(!record_matches_topic(&record, &other_topic));
        assert!(!record_matches_topic(&record, &unrelated_topic));
    }

    #[test]
    fn inbound_record_validation_rules() {
        let id = Identity::generate();
        let pubkey = id.public_key_bytes();
        let validity = far_future_ms();
        let record = make_record(&id, 5, validity);
        let kad_record = kad::Record {
            key: head_record_key(&pubkey),
            value: record_to_bytes(&record),
            publisher: None,
            expires: None,
        };

        // 新規(既知レコードなし)は受理され、validity 由来の失効時刻が付く
        let accepted = validate_inbound_head_record(&kad_record, None).unwrap();
        assert!(accepted.expires.is_some());

        // (sequence, validity) の辞書式で既知を上回れば受理、同じ・下回るは stale として拒否
        assert!(validate_inbound_head_record(&kad_record, Some((4, validity))).is_ok());
        assert!(validate_inbound_head_record(&kad_record, Some((5, validity))).is_err());
        assert!(validate_inbound_head_record(&kad_record, Some((6, validity))).is_err());

        // republish(同一 sequence で validity のみ新しい)は受理される。
        // validity が既知と同じ・古いものは拒否
        assert!(validate_inbound_head_record(&kad_record, Some((5, validity - 1))).is_ok());
        assert!(validate_inbound_head_record(&kad_record, Some((5, validity + 1))).is_err());

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
