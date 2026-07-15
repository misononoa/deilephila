use cid::Cid;
use serde::{Deserialize, Serialize};

use crate::event::VerifyError;
use crate::identity::{verify_signature, Identity};
use crate::util::{from_dag_cbor, to_dag_cbor};

/// 署名付き IPNS-headレコード(docs/data-model.md §2.4)。
/// head CID を指す可変ポインタで、gossipsub(即時)と kad DHT(永続)の
/// 両経路で搬送される(docs/networking.md §4)。イベントと同じ canonical
/// DAG-CBOR でシリアライズし、IPNS 仕様の protobuf 形式は使わない。

/// レコードの既定生存期間(validity = 発行時刻 + この値)。失効前にオンライン中の
/// 定期 republish で更新する(docs/networking.md §4.2)。
pub const RECORD_LIFETIME_MS: i64 = 48 * 60 * 60 * 1000;

// フィールド宣言順は DAG-CBOR canonical 順(キー長昇順→辞書順)に合わせてある
// name(4) < value(5) < sequence(8) < validity(8) < profile_cid(11) < display_name(12)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpnsRecordPayload {
    /// アカウント公開鍵(= IPNS名。署名検証鍵を兼ねる)
    pub name: serde_bytes::ByteArray<32>,
    /// head CID(チェーン最新イベント)
    pub value: Cid,
    /// head イベントの seq(argmax統一規則の比較キー)
    pub sequence: u64,
    /// EOL: レコードが失効する絶対時刻(Unix epoch ミリ秒)
    pub validity: i64,
    /// 最新 Profile イベントの CID(未発行なら None)
    pub profile_cid: Option<Cid>,
    /// 表示名スナップショット(未設定は空文字列)
    pub display_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpnsRecord {
    pub payload: IpnsRecordPayload,
    pub signature: serde_bytes::ByteArray<64>, // payload の canonical DAG-CBOR への Ed25519 署名
}

pub fn record_payload_to_dag_cbor(payload: &IpnsRecordPayload) -> Vec<u8> {
    to_dag_cbor(payload)
}

/// IPNS-headレコードを署名付きで生成する。
/// `validity` の既定値の算出(現在時刻 + 生存期間)は呼び出し側の責務
/// (定期 republish と併せて M5c で配線する)。
pub fn create_ipns_record(
    identity: &Identity,
    sequence: u64,
    head_cid: Cid,
    validity: i64,
    profile_cid: Option<Cid>,
    display_name: String,
) -> IpnsRecord {
    let payload = IpnsRecordPayload {
        name: serde_bytes::ByteArray::new(identity.public_key_bytes()),
        value: head_cid,
        sequence,
        validity,
        profile_cid,
        display_name,
    };
    let cbor = record_payload_to_dag_cbor(&payload);
    let sig = identity.sign_bytes(&cbor);
    IpnsRecord {
        payload,
        signature: serde_bytes::ByteArray::new(sig),
    }
}

/// 署名を検証する。鍵は payload 内の `name` を使う(自己完結検証)。
/// EOL は検査しない: 失効済みレコードも候補として有効(docs/networking.md §4.3)。
pub fn verify_ipns_record(record: &IpnsRecord) -> Result<(), VerifyError> {
    let cbor = record_payload_to_dag_cbor(&record.payload);
    verify_signature(
        record.payload.name.as_ref(),
        &cbor,
        record.signature.as_ref(),
    )
    .map_err(|_| VerifyError::InvalidSignature)
}

/// EOL 失効判定。現在時刻が `validity` 以上なら失効(docs/data-model.md §2.4)。
pub fn is_expired(record: &IpnsRecord, now_ms: i64) -> bool {
    now_ms >= record.payload.validity
}

/// argmax統一規則(docs/networking.md §4): `expected_name` のアカウントに対する
/// 候補群から「署名検証OK かつ (sequence, validity) が辞書式で最大」のレコードを選ぶ。
/// gossipsub / DHT / フォローグラフ探索のどの経路で得た候補も等しくここへ合流させる。
/// EOL 失効済みも候補として有効。name 不一致(別アカウント宛)の候補は除外する。
/// validity を副キーにするのは、republish が sequence を変えず validity のみ
/// 更新するため(validity は署名対象なので第三者は延命を偽装できない)。
/// 同一 (sequence, validity) の複数候補(fork の疑い)は先に現れたものを保持する。
pub fn select_best<'a>(
    expected_name: &[u8; 32],
    candidates: impl IntoIterator<Item = &'a IpnsRecord>,
) -> Option<&'a IpnsRecord> {
    candidates
        .into_iter()
        .filter(|r| r.payload.name.as_ref() == expected_name)
        .filter(|r| verify_ipns_record(r).is_ok())
        .fold(None::<&IpnsRecord>, |best, r| match best {
            Some(b)
                if (b.payload.sequence, b.payload.validity)
                    >= (r.payload.sequence, r.payload.validity) =>
            {
                Some(b)
            }
            _ => Some(r),
        })
}

/// 候補集合から equivocation(同一 sequence で異なる head CID を指す、署名検証OKな
/// レコードのペア)を列挙する(docs/data-model.md §2.1)。`select_best` が argmax で
/// 1件を選ぶのと独立に、候補集合そのものに含まれる矛盾を観測するための関数で、
/// DHT resolve と(M6 の)フォローグラフ探索の応答検証が共用する。
/// republish(同一 sequence・同一 head CID で validity のみ異なる)は fork ではない。
/// name 不一致・署名検証に失敗する候補は第三者が偽造できるため除外する。
pub fn find_record_forks<'a>(
    expected_name: &[u8; 32],
    candidates: &'a [IpnsRecord],
) -> Vec<(&'a IpnsRecord, &'a IpnsRecord)> {
    let valid: Vec<&IpnsRecord> = candidates
        .iter()
        .filter(|r| r.payload.name.as_ref() == expected_name)
        .filter(|r| verify_ipns_record(r).is_ok())
        .collect();
    let mut pairs = Vec::new();
    for (i, a) in valid.iter().enumerate() {
        for b in &valid[i + 1..] {
            if a.payload.sequence == b.payload.sequence && a.payload.value != b.payload.value {
                pairs.push((*a, *b));
            }
        }
    }
    pairs
}

// --- IPNS-headレコードのバイト列相互変換(kad::Record / gossipsub 搬送用) ---

pub fn record_to_bytes(record: &IpnsRecord) -> Vec<u8> {
    to_dag_cbor(record)
}

#[derive(Debug, thiserror::Error)]
pub enum HeadError {
    #[error("failed to decode IPNS head record: {0}")]
    Decode(String),
}

pub fn record_from_bytes(data: &[u8]) -> Result<IpnsRecord, HeadError> {
    from_dag_cbor(data).map_err(HeadError::Decode)
}

/// アカウント公開鍵(hex)から gossipsub トピック名を導出する。
pub fn feed_topic_str(pubkey_hex: &str) -> String {
    format!("deilephila/feed/{pubkey_hex}")
}

// --- テスト ---

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::make_record;
    use crate::util::bytes_to_cid;

    #[test]
    fn topic_name_format() {
        assert_eq!(feed_topic_str("ab01"), "deilephila/feed/ab01");
    }

    #[test]
    fn record_sign_and_verify_ok() {
        let id = Identity::generate();
        let record = make_record(&id, 7, 1_000);
        assert!(verify_ipns_record(&record).is_ok());
        assert_eq!(record.payload.name.as_ref(), &id.public_key_bytes());
        assert_eq!(record.payload.sequence, 7);
        assert_eq!(record.payload.validity, 1_000);
        assert_eq!(record.payload.display_name, "Alice");
        assert_eq!(
            record.payload.profile_cid,
            Some(bytes_to_cid(b"profile block"))
        );
    }

    #[test]
    fn record_without_profile_verifies() {
        let id = Identity::generate();
        let record =
            create_ipns_record(&id, 0, bytes_to_cid(b"head"), 1_000, None, String::new());
        assert!(verify_ipns_record(&record).is_ok());
    }

    #[test]
    fn record_tampered_fields_fail() {
        let id = Identity::generate();
        let base = make_record(&id, 7, 1_000);

        let mut r = base.clone();
        r.payload.sequence += 1;
        assert!(verify_ipns_record(&r).is_err());

        let mut r = base.clone();
        r.payload.value = bytes_to_cid(b"another head");
        assert!(verify_ipns_record(&r).is_err());

        let mut r = base.clone();
        r.payload.validity += 1; // EOL 延命の偽装も署名で弾かれる
        assert!(verify_ipns_record(&r).is_err());

        let mut r = base.clone();
        r.payload.display_name = "Mallory".to_string();
        assert!(verify_ipns_record(&r).is_err());

        let mut r = base;
        r.payload.profile_cid = None;
        assert!(verify_ipns_record(&r).is_err());
    }

    #[test]
    fn record_wrong_key_fails() {
        let id = Identity::generate();
        let other = Identity::generate();
        let mut forged = make_record(&id, 7, 1_000);
        forged.payload.name = serde_bytes::ByteArray::new(other.public_key_bytes());
        assert!(verify_ipns_record(&forged).is_err());
    }

    #[test]
    fn record_roundtrip_is_deterministic() {
        let id = Identity::generate();
        let record = make_record(&id, 7, 1_000);
        let bytes = record_to_bytes(&record);
        let recovered = record_from_bytes(&bytes).unwrap();
        assert_eq!(record_to_bytes(&recovered), bytes);
        assert!(verify_ipns_record(&recovered).is_ok());
    }

    #[test]
    fn record_garbage_bytes_rejected() {
        assert!(record_from_bytes(b"not cbor at all \xff\xff").is_err());
    }

    #[test]
    fn expiry_boundary() {
        let id = Identity::generate();
        let record = make_record(&id, 0, 1_000);
        assert!(!is_expired(&record, 999)); // EOL 直前は生存
        assert!(is_expired(&record, 1_000)); // EOL ちょうどで失効
        assert!(is_expired(&record, 1_001));
    }

    #[test]
    fn select_best_picks_max_valid_sequence() {
        let id = Identity::generate();
        let pubkey = id.public_key_bytes();

        let low = make_record(&id, 3, 1_000);
        let high = make_record(&id, 5, 1_000);
        // 署名が壊れた seq=9(改ざん): 最大 seq でも選ばれてはならない
        let mut broken = make_record(&id, 8, 1_000);
        broken.payload.sequence = 9;

        let best = select_best(&pubkey, [&low, &broken, &high]).unwrap();
        assert_eq!(best.payload.sequence, 5);
    }

    #[test]
    fn select_best_accepts_expired_record() {
        // EOL 失効済みでも署名と sequence は有効な候補(networking.md §4.3)
        let id = Identity::generate();
        let pubkey = id.public_key_bytes();
        let expired_but_newer = make_record(&id, 10, 0); // validity=0 = 失効済み
        let fresh_but_older = make_record(&id, 4, i64::MAX);

        let best = select_best(&pubkey, [&fresh_but_older, &expired_but_newer]).unwrap();
        assert_eq!(best.payload.sequence, 10);
    }

    #[test]
    fn select_best_filters_other_accounts() {
        let id = Identity::generate();
        let other = Identity::generate();
        let mine = make_record(&id, 1, 1_000);
        let theirs = make_record(&other, 99, 1_000); // 正当な署名だが別アカウント宛

        let best = select_best(&id.public_key_bytes(), [&theirs, &mine]).unwrap();
        assert_eq!(best.payload.sequence, 1);
        assert_eq!(best.payload.name.as_ref(), &id.public_key_bytes());
    }

    #[test]
    fn select_best_empty_is_none() {
        let id = Identity::generate();
        assert!(select_best(&id.public_key_bytes(), []).is_none());
    }

    #[test]
    fn select_best_same_sequence_prefers_newer_validity() {
        // republish = 同一 sequence で validity のみ更新。順序に依らず新しい方を選ぶ
        let id = Identity::generate();
        let original = make_record(&id, 5, 1_000);
        let republished = make_record(&id, 5, 2_000);

        let best = select_best(&id.public_key_bytes(), [&original, &republished]).unwrap();
        assert_eq!(best.payload.validity, 2_000);
        let best = select_best(&id.public_key_bytes(), [&republished, &original]).unwrap();
        assert_eq!(best.payload.validity, 2_000);

        // sequence は validity より優先される(辞書式)
        let newer_seq = make_record(&id, 6, 500);
        let best = select_best(&id.public_key_bytes(), [&republished, &newer_seq]).unwrap();
        assert_eq!(best.payload.sequence, 6);
    }

    #[test]
    fn find_record_forks_detects_equivocation() {
        let id = Identity::generate();
        let a = make_record(&id, 5, 1_000);
        let b = create_ipns_record(
            &id,
            5,
            bytes_to_cid(b"other head"),
            2_000, // validity が違っても、同一 sequence で head CID が違えば fork
            None,
            String::new(),
        );
        let unrelated = make_record(&id, 6, 1_000);

        let candidates = [a.clone(), b.clone(), unrelated];
        let forks = find_record_forks(&id.public_key_bytes(), &candidates);
        assert_eq!(forks.len(), 1);
        let (x, y) = forks[0];
        assert_eq!(x.payload.value, a.payload.value);
        assert_eq!(y.payload.value, b.payload.value);
    }

    #[test]
    fn find_record_forks_ignores_republish_and_invalid() {
        let id = Identity::generate();
        let original = make_record(&id, 5, 1_000);
        let republished = make_record(&id, 5, 2_000); // 同一 head CID、validity のみ更新

        // 署名の壊れた矛盾候補: 第三者が偽造できるため fork の証拠にならない
        let mut forged = make_record(&id, 5, 1_000);
        forged.payload.value = bytes_to_cid(b"forged head");

        // 別アカウントの正当なレコード(name 不一致)も対象外
        let other = Identity::generate();
        let theirs = make_record(&other, 5, 1_000);

        let candidates = [original, republished, forged, theirs];
        assert!(find_record_forks(&id.public_key_bytes(), &candidates).is_empty());
    }

    #[test]
    fn select_best_tie_keeps_first() {
        // 同一 (sequence, validity) で異なる内容 = fork の疑い。規則としては先着を保持する
        let id = Identity::generate();
        let a = make_record(&id, 5, 1_000);
        let b = create_ipns_record(
            &id,
            5,
            bytes_to_cid(b"other head"),
            1_000,
            None,
            String::new(),
        );
        let best = select_best(&id.public_key_bytes(), [&a, &b]).unwrap();
        assert_eq!(best.payload.value, a.payload.value);
    }
}
