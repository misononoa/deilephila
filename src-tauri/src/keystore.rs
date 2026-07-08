use std::path::Path;

use aes_gcm::aead::Aead;
use aes_gcm::{Aes256Gcm, Key, KeyInit, Nonce};
use argon2::Argon2;

use crate::identity::Identity;

const KEYSTORE_FILE: &str = "keystore.bin";
const SALT_LEN: usize = 16;
const NONCE_LEN: usize = 12;

/// 最小ファイルサイズ: salt + nonce + (32バイトシード + 16バイトGCMタグ)
const MIN_BLOB_LEN: usize = SALT_LEN + NONCE_LEN + 48;

#[derive(Debug, thiserror::Error)]
pub enum KeystoreError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("key derivation failed")]
    KeyDerivation,
    #[error("wrong passphrase")]
    WrongPassphrase,
    #[error("invalid keystore format")]
    InvalidFormat,
}

pub struct Keystore {
    identity: Identity,
}

impl Keystore {
    /// 新しいアカウントを生成し、passphrase で暗号化して dir に保存する。
    /// 成功時は (Keystore, pubkey_bytes) を返す。
    pub fn create(passphrase: &str, dir: &Path) -> Result<(Self, [u8; 32]), KeystoreError> {
        let seed = random_bytes::<32>();
        let salt = random_bytes::<SALT_LEN>();
        let nonce_bytes = random_bytes::<NONCE_LEN>();

        let enc_key = derive_key(passphrase, &salt)?;
        let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&enc_key));
        let nonce = Nonce::from_slice(&nonce_bytes);
        let ciphertext = cipher
            .encrypt(nonce, seed.as_ref())
            .map_err(|_| KeystoreError::KeyDerivation)?;

        let mut blob = Vec::with_capacity(SALT_LEN + NONCE_LEN + ciphertext.len());
        blob.extend_from_slice(&salt);
        blob.extend_from_slice(&nonce_bytes);
        blob.extend_from_slice(&ciphertext);

        std::fs::create_dir_all(dir)?;
        std::fs::write(dir.join(KEYSTORE_FILE), &blob)?;

        let identity = Identity::from_seed(&seed);
        let pubkey = identity.public_key_bytes();
        Ok((Keystore { identity }, pubkey))
    }

    /// 既存のキーストアを passphrase で復号してロードする。
    pub fn load(passphrase: &str, dir: &Path) -> Result<Self, KeystoreError> {
        let blob = std::fs::read(dir.join(KEYSTORE_FILE))?;

        if blob.len() < MIN_BLOB_LEN {
            return Err(KeystoreError::InvalidFormat);
        }

        let salt = &blob[..SALT_LEN];
        let nonce_bytes = &blob[SALT_LEN..SALT_LEN + NONCE_LEN];
        let ciphertext = &blob[SALT_LEN + NONCE_LEN..];

        let enc_key = derive_key(passphrase, salt)?;
        let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&enc_key));
        let nonce = Nonce::from_slice(nonce_bytes);
        let seed_vec = cipher
            .decrypt(nonce, ciphertext)
            .map_err(|_| KeystoreError::WrongPassphrase)?;

        if seed_vec.len() != 32 {
            return Err(KeystoreError::InvalidFormat);
        }
        let mut seed = [0u8; 32];
        seed.copy_from_slice(&seed_vec);

        Ok(Keystore {
            identity: Identity::from_seed(&seed),
        })
    }

    /// キーストアファイルが存在するか確認する(初回判定用)。
    pub fn exists(dir: &Path) -> bool {
        dir.join(KEYSTORE_FILE).exists()
    }

    pub fn identity(&self) -> &Identity {
        &self.identity
    }

    pub fn into_identity(self) -> Identity {
        self.identity
    }
}

fn random_bytes<const N: usize>() -> [u8; N] {
    let mut buf = [0u8; N];
    getrandom::fill(&mut buf).expect("OS RNG unavailable");
    buf
}

fn derive_key(passphrase: &str, salt: &[u8]) -> Result<[u8; 32], KeystoreError> {
    let mut key = [0u8; 32];
    Argon2::default()
        .hash_password_into(passphrase.as_bytes(), salt, &mut key)
        .map_err(|_| KeystoreError::KeyDerivation)?;
    Ok(key)
}

// --- テスト ---

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("deilephila_keystore_test_{name}"));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn create_and_load_roundtrip() {
        let dir = tmp_dir("roundtrip");
        let (ks, pubkey) = Keystore::create("correct_passphrase", &dir).unwrap();
        let original_pubkey = ks.identity().public_key_bytes();
        assert_eq!(pubkey, original_pubkey);

        let loaded = Keystore::load("correct_passphrase", &dir).unwrap();
        assert_eq!(loaded.identity().public_key_bytes(), original_pubkey);
    }

    #[test]
    fn wrong_passphrase_returns_error() {
        let dir = tmp_dir("wrong_pass");
        Keystore::create("correct", &dir).unwrap();
        let result = Keystore::load("wrong", &dir);
        assert!(matches!(result, Err(KeystoreError::WrongPassphrase)));
    }

    #[test]
    fn exists_returns_false_before_create() {
        let dir = tmp_dir("exists_check");
        assert!(!Keystore::exists(&dir));
        Keystore::create("pass", &dir).unwrap();
        assert!(Keystore::exists(&dir));
    }

    #[test]
    fn identity_sign_verify_after_load() {
        let dir = tmp_dir("sign_verify");
        let (ks, _) = Keystore::create("mypassphrase", &dir).unwrap();
        let msg = b"hello deilephila";
        let sig = ks.identity().sign_bytes(msg);

        let loaded = Keystore::load("mypassphrase", &dir).unwrap();
        let pubkey = loaded.identity().public_key_bytes();
        assert!(crate::identity::verify_signature(&pubkey, msg, &sig).is_ok());
    }
}
