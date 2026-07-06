use std::time::{SystemTime, UNIX_EPOCH};

use cid::Cid;
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};

use crate::event::{payload_to_dag_cbor, EventEnvelope, EventKind, EventPayload};

pub struct Identity {
    signing_key: SigningKey,
}

impl Identity {
    /// OS RNG からシードを取得して Ed25519 鍵ペアを生成する。
    pub fn generate() -> Self {
        let mut seed = [0u8; 32];
        getrandom::fill(&mut seed).expect("OS RNG unavailable");
        Identity {
            signing_key: SigningKey::from_bytes(&seed),
        }
    }

    /// 32 バイトのシードから Identity を復元する(Keystore 復号後に使用)。
    pub fn from_seed(seed: &[u8; 32]) -> Self {
        Identity {
            signing_key: SigningKey::from_bytes(seed),
        }
    }

    /// 秘密鍵のシードバイト列を返す(Keystore での保存用)。
    pub fn seed_bytes(&self) -> [u8; 32] {
        self.signing_key.to_bytes()
    }

    pub fn public_key_bytes(&self) -> [u8; 32] {
        self.signing_key.verifying_key().to_bytes()
    }

    /// 任意のバイト列に署名して 64 バイトの署名を返す。
    pub fn sign_bytes(&self, msg: &[u8]) -> [u8; 64] {
        self.signing_key.sign(msg).to_bytes()
    }
}

/// Ed25519 署名を検証する。pubkey/msg/sig はすべて生バイト列で受け取る。
pub fn verify_signature(
    pubkey_bytes: &[u8; 32],
    msg: &[u8],
    sig_bytes: &[u8; 64],
) -> Result<(), ed25519_dalek::SignatureError> {
    let pubkey = VerifyingKey::from_bytes(pubkey_bytes)?;
    let sig = Signature::from_bytes(sig_bytes);
    pubkey.verify(msg, &sig)
}

/// EventEnvelope を署名付きで生成するヘルパー。
/// テストや IPC ハンドラから利用する。
pub fn create_envelope(
    identity: &Identity,
    seq: u64,
    prev: Option<Cid>,
    kind: EventKind,
) -> EventEnvelope {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_millis() as i64;

    let payload = EventPayload {
        seq,
        kind,
        prev,
        author: serde_bytes::ByteArray::new(identity.public_key_bytes()),
        timestamp: now_ms,
    };
    let cbor = payload_to_dag_cbor(&payload);
    let sig = identity.sign_bytes(&cbor);
    let envelope = EventEnvelope {
        payload,
        signature: serde_bytes::ByteArray::new(sig),
    };

    let author_bytes = identity.public_key_bytes();
    let author_prefix: String = author_bytes[..4]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    tracing::debug!(
        seq,
        kind = %envelope.payload.kind,
        author = %author_prefix,
        "envelope signed"
    );

    envelope
}

// --- テスト ---

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keypair_generation() {
        let identity = Identity::generate();
        let bytes = identity.public_key_bytes();
        assert_eq!(bytes.len(), 32);
        // 全ゼロにならないことを確認(getrandom が機能している)
        assert_ne!(bytes, [0u8; 32]);
    }

    #[test]
    fn sign_and_verify() {
        let identity = Identity::generate();
        let msg = b"test message";
        let sig = identity.sign_bytes(msg);
        let pubkey = identity.public_key_bytes();
        assert!(verify_signature(&pubkey, msg, &sig).is_ok());
    }

    #[test]
    fn verify_wrong_message() {
        let identity = Identity::generate();
        let sig = identity.sign_bytes(b"original");
        let pubkey = identity.public_key_bytes();
        assert!(verify_signature(&pubkey, b"tampered", &sig).is_err());
    }

    #[test]
    fn verify_wrong_key() {
        let identity1 = Identity::generate();
        let identity2 = Identity::generate();
        let sig = identity1.sign_bytes(b"hello");
        let wrong_pubkey = identity2.public_key_bytes();
        assert!(verify_signature(&wrong_pubkey, b"hello", &sig).is_err());
    }
}
