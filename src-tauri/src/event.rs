use cid::Cid;
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};

use crate::util::{bytes_to_cid, to_dag_cbor};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub payload: EventPayload,
    pub signature: serde_bytes::ByteArray<64>,
}

// フィールド宣言順は DAG-CBOR canonical 順(キー長昇順→辞書順)に合わせてある
// seq(3) < kind(4) < prev(4) < author(6) < timestamp(9)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventPayload {
    pub seq: u64,
    pub kind: EventKind,
    pub prev: Option<Cid>,
    pub author: serde_bytes::ByteArray<32>,
    pub timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EventKind {
    Post {
        text: String,
    },
    Edit {
        text: String,
        target: Cid,
    },
    Delete {
        target: Cid,
    },
    Profile {
        bio: String,
        avatar_cid: Option<Cid>,
        display_name: String,
    },
    Follow {
        added: Vec<serde_bytes::ByteArray<32>>,
        removed: Vec<serde_bytes::ByteArray<32>>,
    },
    Reply {
        text: String,
        target: Cid,
    },
}

// --- CID 生成 ---

pub fn payload_to_dag_cbor(payload: &EventPayload) -> Vec<u8> {
    to_dag_cbor(payload)
}

pub fn envelope_cid(envelope: &EventEnvelope) -> Cid {
    bytes_to_cid(&to_dag_cbor(envelope))
}

// --- 署名検証 ---

#[derive(Debug)]
pub enum VerifyError {
    InvalidAuthorKey,
    InvalidSignature,
}

pub fn verify_envelope(envelope: &EventEnvelope) -> Result<(), VerifyError> {
    let pubkey = VerifyingKey::from_bytes(envelope.payload.author.as_ref())
        .map_err(|_| VerifyError::InvalidAuthorKey)?;
    let sig = Signature::from_bytes(envelope.signature.as_ref());
    let cbor = payload_to_dag_cbor(&envelope.payload);
    pubkey
        .verify(&cbor, &sig)
        .map_err(|_| VerifyError::InvalidSignature)
}

// --- チェーン連結検証 ---

#[derive(Debug)]
pub enum ChainError {
    WrongSeq { expected: u64, got: u64 },
    WrongPrev,
    GenesisMustHaveNoPrev,
}

/// seq 順に並んだ (CID, EventEnvelope) スライスのチェーン連結を検証する。
/// CID は呼び出し元が envelope_cid() で計算済みのものを渡す。
pub fn verify_chain(pairs: &[(Cid, EventEnvelope)]) -> Result<(), ChainError> {
    for (i, (cid, envelope)) in pairs.iter().enumerate() {
        let expected_seq = i as u64;
        if envelope.payload.seq != expected_seq {
            return Err(ChainError::WrongSeq {
                expected: expected_seq,
                got: envelope.payload.seq,
            });
        }
        if i == 0 {
            if envelope.payload.prev.is_some() {
                return Err(ChainError::GenesisMustHaveNoPrev);
            }
        } else {
            let prev_cid = &pairs[i - 1].0;
            match &envelope.payload.prev {
                Some(p) if p == prev_cid => {}
                _ => return Err(ChainError::WrongPrev),
            }
        }
        let _ = cid; // 将来: CID 自体の一致検証も追加可
    }
    Ok(())
}

// --- Display ---

impl std::fmt::Display for EventKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventKind::Post { .. } => write!(f, "Post"),
            EventKind::Edit { .. } => write!(f, "Edit"),
            EventKind::Delete { .. } => write!(f, "Delete"),
            EventKind::Profile { .. } => write!(f, "Profile"),
            EventKind::Follow { .. } => write!(f, "Follow"),
            EventKind::Reply { .. } => write!(f, "Reply"),
        }
    }
}

// --- テスト ---

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::Identity;

    fn init_tracing() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_test_writer()
            .try_init();
    }

    fn make_post_payload(seq: u64, prev: Option<Cid>, author: &Identity) -> EventPayload {
        EventPayload {
            seq,
            kind: EventKind::Post {
                text: "hello".to_string(),
            },
            prev,
            author: serde_bytes::ByteArray::new(author.public_key_bytes()),
            timestamp: 1_000_000,
        }
    }

    #[test]
    fn dag_cbor_deterministic() {
        let identity = Identity::generate();
        let payload = make_post_payload(0, None, &identity);
        let a = payload_to_dag_cbor(&payload);
        let b = payload_to_dag_cbor(&payload);
        assert_eq!(a, b);
    }

    #[test]
    fn cid_deterministic() {
        let identity = Identity::generate();
        let payload = make_post_payload(0, None, &identity);
        let a = bytes_to_cid(&payload_to_dag_cbor(&payload));
        let b = bytes_to_cid(&payload_to_dag_cbor(&payload));
        assert_eq!(a, b);
    }

    #[test]
    fn cid_changes_on_mutation() {
        let identity = Identity::generate();
        let p1 = make_post_payload(0, None, &identity);
        let mut p2 = p1.clone();
        p2.kind = EventKind::Post {
            text: "different".to_string(),
        };
        let cid1 = bytes_to_cid(&payload_to_dag_cbor(&p1));
        let cid2 = bytes_to_cid(&payload_to_dag_cbor(&p2));
        assert_ne!(cid1, cid2);
    }

    #[test]
    fn sign_and_verify_envelope() {
        init_tracing();
        let identity = Identity::generate();
        let envelope = crate::identity::create_envelope(
            &identity,
            0,
            None,
            EventKind::Post {
                text: "hi".to_string(),
            },
        );
        assert!(verify_envelope(&envelope).is_ok());
    }

    #[test]
    fn tampered_payload_rejected() {
        init_tracing();
        let identity = Identity::generate();
        let mut envelope = crate::identity::create_envelope(
            &identity,
            0,
            None,
            EventKind::Post {
                text: "hi".to_string(),
            },
        );
        envelope.payload.kind = EventKind::Post {
            text: "tampered".to_string(),
        };
        assert!(verify_envelope(&envelope).is_err());
    }

    #[test]
    fn tampered_signature_rejected() {
        init_tracing();
        let identity = Identity::generate();
        let mut envelope = crate::identity::create_envelope(
            &identity,
            0,
            None,
            EventKind::Post {
                text: "hi".to_string(),
            },
        );
        envelope.signature = serde_bytes::ByteArray::new([0u8; 64]);
        assert!(verify_envelope(&envelope).is_err());
    }

    fn build_chain(identity: &Identity, n: usize) -> Vec<(Cid, EventEnvelope)> {
        let mut pairs: Vec<(Cid, EventEnvelope)> = Vec::new();
        for i in 0..n {
            let prev = if i == 0 {
                None
            } else {
                Some(pairs[i - 1].0.clone())
            };
            let envelope = crate::identity::create_envelope(
                identity,
                i as u64,
                prev,
                EventKind::Post {
                    text: format!("event {}", i),
                },
            );
            let cid = envelope_cid(&envelope);
            pairs.push((cid, envelope));
        }
        pairs
    }

    #[test]
    fn verify_chain_valid() {
        init_tracing();
        let identity = Identity::generate();
        let chain = build_chain(&identity, 3);
        assert!(verify_chain(&chain).is_ok());
    }

    #[test]
    fn verify_chain_wrong_seq() {
        init_tracing();
        let identity = Identity::generate();
        let mut chain = build_chain(&identity, 3);
        chain[1].1.payload.seq = 99;
        assert!(matches!(
            verify_chain(&chain),
            Err(ChainError::WrongSeq { .. })
        ));
    }

    #[test]
    fn verify_chain_wrong_prev() {
        init_tracing();
        let identity = Identity::generate();
        let mut chain = build_chain(&identity, 3);
        chain[1].1.payload.prev = Some(chain[0].0.clone()); // まず正しいCIDで作る
                                                            // 別の CID で上書き
        let dummy_cid = bytes_to_cid(b"dummy");
        chain[1].1.payload.prev = Some(dummy_cid);
        assert!(matches!(verify_chain(&chain), Err(ChainError::WrongPrev)));
    }

    #[test]
    fn verify_chain_genesis_with_prev() {
        init_tracing();
        let identity = Identity::generate();
        let mut chain = build_chain(&identity, 2);
        let dummy_cid = bytes_to_cid(b"dummy");
        chain[0].1.payload.prev = Some(dummy_cid);
        assert!(matches!(
            verify_chain(&chain),
            Err(ChainError::GenesisMustHaveNoPrev)
        ));
    }
}
