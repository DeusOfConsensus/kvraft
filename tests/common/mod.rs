//! Helpers shared by the integration tests.
//! Each test file compiles this separately and uses a different subset.
#![allow(dead_code)]

use kvraft::raft::{Entry, Message, Node, NodeId};
use kvraft::storage::Storage;
use std::path::PathBuf;

/// How many fuzz seeds to run. `KVRAFT_FUZZ_SEEDS=2000 cargo test --release`
/// for a much heavier sweep.
pub fn seeds(default: u64) -> u64 {
    std::env::var("KVRAFT_FUZZ_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

pub fn e(term: u64, data: &[u8]) -> Entry {
    Entry {
        term,
        data: data.to_vec(),
    }
}

pub fn peers_of(id: NodeId, total: u64) -> Vec<NodeId> {
    (1..=total).filter(|&p| p != id).collect()
}

/// Everything the node wants to send, as bare messages, clearing its outbox.
pub fn drained<S: Storage>(n: &mut Node<S>) -> Vec<Message> {
    n.drain_messages().into_iter().map(|env| env.msg).collect()
}

/// The single message the node produced, asserting there was exactly one.
pub fn only_reply<S: Storage>(n: &mut Node<S>) -> Message {
    let mut msgs = drained(n);
    assert_eq!(msgs.len(), 1, "expected exactly one reply, got {msgs:?}");
    msgs.remove(0)
}

/// Tiny deterministic RNG (xorshift) so tests need no dependencies.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }
    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    pub fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }
}

/// Unique scratch file per test, removed on drop. No external crates needed.
pub struct TempWal(pub PathBuf);

impl TempWal {
    pub fn new(name: &str) -> Self {
        let p = std::env::temp_dir().join(format!("kvraft-{}-{}.wal", std::process::id(), name));
        let _ = std::fs::remove_file(&p);
        TempWal(p)
    }
}

impl Drop for TempWal {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
