use std::collections::{HashMap, HashSet};

use cid::Cid;

use crate::event::{EventEnvelope, EventKind};

#[derive(Debug, Clone)]
pub struct PostEntry {
    pub cid: Cid,
    pub author: [u8; 32],
    pub text: String,
    pub timestamp: i64,
    pub deleted: bool,
    /// このエントリに最後に適用した Edit の seq(Post 自身の seq で初期化)
    pub latest_edit_seq: u64,
}

#[derive(Debug, Clone)]
pub struct ProfileState {
    pub display_name: String,
    pub bio: String,
    pub avatar_cid: Option<Cid>,
}

#[derive(Debug, Clone, Default)]
pub struct ChainState {
    pub posts: HashMap<Cid, PostEntry>,
    /// (適用した seq, ProfileState)。複数の Profile があれば最大 seq を採用。
    pub profile: Option<(u64, ProfileState)>,
    pub following: HashSet<[u8; 32]>,
}

/// seq 順に並んだ `(Cid, EventEnvelope)` スライスを fold して ChainState を返す。
/// CID は envelope_cid() で事前計算済みのものを受け取る。
pub fn fold(events: &[(Cid, EventEnvelope)]) -> ChainState {
    let mut state = ChainState::default();

    tracing::debug!(count = events.len(), "fold start");

    for (cid, envelope) in events {
        let seq = envelope.payload.seq;
        let author: [u8; 32] = *envelope.payload.author.as_ref();
        let timestamp = envelope.payload.timestamp;

        let cid_str = cid.to_string();
        tracing::trace!(
            seq,
            kind = %envelope.payload.kind,
            cid = &cid_str[..cid_str.len().min(8)],
            "fold apply"
        );

        match &envelope.payload.kind {
            EventKind::Post { text } => {
                state.posts.insert(
                    cid.clone(),
                    PostEntry {
                        cid: cid.clone(),
                        author,
                        text: text.clone(),
                        timestamp,
                        deleted: false,
                        latest_edit_seq: seq,
                    },
                );
            }
            EventKind::Edit { target, text } => {
                if let Some(entry) = state.posts.get_mut(target) {
                    if seq > entry.latest_edit_seq {
                        entry.text = text.clone();
                        entry.latest_edit_seq = seq;
                    }
                }
            }
            EventKind::Delete { target } => {
                if let Some(entry) = state.posts.get_mut(target) {
                    entry.deleted = true;
                }
            }
            EventKind::Profile {
                display_name,
                bio,
                avatar_cid,
            } => {
                let should_update = match &state.profile {
                    None => true,
                    Some((existing_seq, _)) => seq > *existing_seq,
                };
                if should_update {
                    state.profile = Some((
                        seq,
                        ProfileState {
                            display_name: display_name.clone(),
                            bio: bio.clone(),
                            avatar_cid: avatar_cid.clone(),
                        },
                    ));
                }
            }
            EventKind::Follow { added, removed } => {
                for key in added {
                    state.following.insert(*key.as_ref());
                }
                for key in removed {
                    state.following.remove(key.as_ref());
                }
            }
            EventKind::Reply { .. } => {
                // MVP スコープ外。状態への反映なし。
            }
        }
    }

    tracing::debug!(
        posts = state.posts.len(),
        following = state.following.len(),
        has_profile = state.profile.is_some(),
        "fold done"
    );

    state
}

// --- テスト ---

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::envelope_cid;
    use crate::identity::{create_envelope, Identity};

    fn init_tracing() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_test_writer()
            .try_init();
    }

    fn make_chain(identity: &Identity, kinds: Vec<EventKind>) -> Vec<(Cid, EventEnvelope)> {
        let mut pairs: Vec<(Cid, EventEnvelope)> = Vec::new();
        for (i, kind) in kinds.into_iter().enumerate() {
            let prev = if i == 0 {
                None
            } else {
                Some(pairs[i - 1].0.clone())
            };
            let envelope = create_envelope(identity, i as u64, prev, kind);
            let cid = envelope_cid(&envelope);
            pairs.push((cid, envelope));
        }
        pairs
    }

    #[test]
    fn fold_empty() {
        init_tracing();
        let state = fold(&[]);
        assert!(state.posts.is_empty());
        assert!(state.profile.is_none());
        assert!(state.following.is_empty());
    }

    #[test]
    fn fold_post() {
        init_tracing();
        let identity = Identity::generate();
        let chain = make_chain(
            &identity,
            vec![EventKind::Post {
                text: "hi".to_string(),
            }],
        );
        let state = fold(&chain);
        assert_eq!(state.posts.len(), 1);
        let entry = state.posts.values().next().unwrap();
        assert_eq!(entry.text, "hi");
        assert!(!entry.deleted);
    }

    #[test]
    fn fold_edit_last_write_wins() {
        init_tracing();
        let identity = Identity::generate();
        // genesis: Post
        // seq 1: Edit(新しいテキスト)
        // seq 2: Edit(さらに新しい)
        let mut pairs: Vec<(Cid, EventEnvelope)> = Vec::new();

        let post_env = create_envelope(
            &identity,
            0,
            None,
            EventKind::Post {
                text: "original".to_string(),
            },
        );
        let post_cid = envelope_cid(&post_env);
        pairs.push((post_cid.clone(), post_env));

        let edit1 = create_envelope(
            &identity,
            1,
            Some(post_cid.clone()),
            EventKind::Edit {
                text: "edit1".to_string(),
                target: post_cid.clone(),
            },
        );
        let edit1_cid = envelope_cid(&edit1);
        pairs.push((edit1_cid.clone(), edit1));

        let edit2 = create_envelope(
            &identity,
            2,
            Some(edit1_cid.clone()),
            EventKind::Edit {
                text: "edit2".to_string(),
                target: post_cid.clone(),
            },
        );
        let edit2_cid = envelope_cid(&edit2);
        pairs.push((edit2_cid, edit2));

        let state = fold(&pairs);
        let entry = state.posts.get(&post_cid).unwrap();
        assert_eq!(entry.text, "edit2");
        assert_eq!(entry.latest_edit_seq, 2);
    }

    #[test]
    fn fold_delete() {
        init_tracing();
        let identity = Identity::generate();
        let mut pairs: Vec<(Cid, EventEnvelope)> = Vec::new();

        let post_env = create_envelope(
            &identity,
            0,
            None,
            EventKind::Post {
                text: "bye".to_string(),
            },
        );
        let post_cid = envelope_cid(&post_env);
        pairs.push((post_cid.clone(), post_env));

        let delete_env = create_envelope(
            &identity,
            1,
            Some(post_cid.clone()),
            EventKind::Delete {
                target: post_cid.clone(),
            },
        );
        let delete_cid = envelope_cid(&delete_env);
        pairs.push((delete_cid, delete_env));

        let state = fold(&pairs);
        let entry = state.posts.get(&post_cid).unwrap();
        assert!(entry.deleted);
    }

    #[test]
    fn fold_profile_latest_wins() {
        init_tracing();
        let identity = Identity::generate();
        let chain = make_chain(
            &identity,
            vec![
                EventKind::Profile {
                    display_name: "old".to_string(),
                    bio: "bio1".to_string(),
                    avatar_cid: None,
                },
                EventKind::Profile {
                    display_name: "new".to_string(),
                    bio: "bio2".to_string(),
                    avatar_cid: None,
                },
            ],
        );
        let state = fold(&chain);
        let (seq, profile) = state.profile.unwrap();
        assert_eq!(seq, 1);
        assert_eq!(profile.display_name, "new");
    }

    #[test]
    fn fold_follow() {
        init_tracing();
        let identity = Identity::generate();
        let other = Identity::generate();
        let other_key = other.public_key_bytes();

        let chain = make_chain(
            &identity,
            vec![
                EventKind::Follow {
                    added: vec![serde_bytes::ByteArray::new(other_key)],
                    removed: vec![],
                },
                EventKind::Follow {
                    added: vec![],
                    removed: vec![serde_bytes::ByteArray::new(other_key)],
                },
            ],
        );
        let state = fold(&chain);
        assert!(!state.following.contains(&other_key));
    }
}
