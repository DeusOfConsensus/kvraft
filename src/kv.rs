//! The replicated key-value state machine.
//!
//! Every replica applies the same committed commands in the same order, so
//! `Kv::apply` must be **deterministic**: no clock, no randomness, no iteration
//! over a hash map. Two replicas that applied the same prefix of the log hold
//! byte-identical state (the simulator checks this with `digest`).
//!
//! # Exactly-once writes: client sessions
//!
//! Networks lose replies. A client whose reply was lost times out and retries,
//! possibly against a different leader, and the original write may already be
//! committed. Without protection an `Append` would be applied twice.
//!
//! So every command carries `(client, seq)`, and the state machine remembers,
//! per client, the last `seq` it executed and the response it gave:
//!
//!   * `seq == last`  -> a retry of the last command: return the saved
//!     response, change nothing.
//!   * `seq <  last`  -> a stale duplicate (the client has moved on): ignore.
//!   * `seq >  last`  -> a new command: execute it and remember the result.
//!
//! This lives INSIDE the replicated state machine, not in the leader, because
//! the retry may reach a different leader than the original. Only replicated
//! state is guaranteed to be there after a leader change.
//!
//! Rule for clients: **one outstanding request at a time**, with `seq`
//! increasing. If a client pipelined seq 6 and 7 and 7 committed first, 6 would
//! later look stale and be silently dropped.
//!
//! Known limit: the session table grows with the number of clients and is never
//! expired. Real systems expire idle sessions.
//!
//! # Reads
//!
//! `Get` is never written to the log by the server: it is served by the
//! ReadIndex protocol (see `raft.rs`). `Kv::read` is the pure lookup behind it.
//! `apply` still handles a `Get` command so that it is a total function.

use std::collections::BTreeMap;

pub type ClientId = u64;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Op {
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Get {
        key: Vec<u8>,
    },
    Delete {
        key: Vec<u8>,
    },
    /// Appends to the value (creating it if absent) and returns the NEW value.
    /// Returning the whole value makes every append's position in the order
    /// visible to the linearizability checker.
    Append {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    /// Compare-and-set. `expected: None` means "the key must be absent".
    Cas {
        key: Vec<u8>,
        expected: Option<Vec<u8>>,
        new: Vec<u8>,
    },
}

impl Op {
    pub fn key(&self) -> &[u8] {
        match self {
            Op::Put { key, .. }
            | Op::Get { key }
            | Op::Delete { key }
            | Op::Append { key, .. }
            | Op::Cas { key, .. } => key,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Resp {
    Ok,
    Value(Option<Vec<u8>>),
    /// Whether the key existed.
    Deleted(bool),
    /// Whether the swap happened.
    Cas(bool),
    /// The command was a stale duplicate: the client already moved past it.
    /// Never shown to a client that follows the one-outstanding-request rule.
    Stale,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Command {
    pub client: ClientId,
    pub seq: u64,
    pub op: Op,
}

// ---------------------------------------------------------------------------
// Binary codec. Log entries are opaque bytes to Raft, so commands need one.
//
//   [tag u8][client u64 LE][seq u64 LE][fields...]
//   bytes field = [len u32 LE][data]
//
//   tag 1 Put:    key, value
//   tag 2 Get:    key
//   tag 3 Delete: key
//   tag 4 Append: key, value
//   tag 5 Cas:    key, has_expected u8, expected (only if has_expected = 1), new
//
// An encoded command is never empty, so it can never be confused with the
// empty no-op entry that a new leader appends.
// ---------------------------------------------------------------------------

pub(crate) fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    out.extend_from_slice(&(b.len() as u32).to_le_bytes());
    out.extend_from_slice(b);
}

pub(crate) struct Reader<'a> {
    pub(crate) buf: &'a [u8],
    pub(crate) pos: usize,
}

impl<'a> Reader<'a> {
    pub(crate) fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(n)?;
        let s = self.buf.get(self.pos..end)?;
        self.pos = end;
        Some(s)
    }
    pub(crate) fn u8(&mut self) -> Option<u8> {
        Some(self.take(1)?[0])
    }
    pub(crate) fn u32(&mut self) -> Option<u32> {
        Some(u32::from_le_bytes(self.take(4)?.try_into().ok()?))
    }
    pub(crate) fn u64(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes(self.take(8)?.try_into().ok()?))
    }
    pub(crate) fn bytes(&mut self) -> Option<Vec<u8>> {
        let n = self.u32()? as usize;
        Some(self.take(n)?.to_vec())
    }
    pub(crate) fn finished(&self) -> bool {
        self.pos == self.buf.len()
    }
}

impl Command {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        let tag = match &self.op {
            Op::Put { .. } => 1u8,
            Op::Get { .. } => 2,
            Op::Delete { .. } => 3,
            Op::Append { .. } => 4,
            Op::Cas { .. } => 5,
        };
        out.push(tag);
        out.extend_from_slice(&self.client.to_le_bytes());
        out.extend_from_slice(&self.seq.to_le_bytes());
        match &self.op {
            Op::Put { key, value } | Op::Append { key, value } => {
                put_bytes(&mut out, key);
                put_bytes(&mut out, value);
            }
            Op::Get { key } | Op::Delete { key } => put_bytes(&mut out, key),
            Op::Cas { key, expected, new } => {
                put_bytes(&mut out, key);
                match expected {
                    Some(e) => {
                        out.push(1);
                        put_bytes(&mut out, e);
                    }
                    None => out.push(0),
                }
                put_bytes(&mut out, new);
            }
        }
        out
    }

    /// Returns `None` for anything that is not exactly one well-formed command.
    /// Every replica decodes the same bytes the same way, so a malformed entry
    /// is skipped identically everywhere.
    pub fn decode(buf: &[u8]) -> Option<Command> {
        let mut r = Reader { buf, pos: 0 };
        let tag = r.u8()?;
        let client = r.u64()?;
        let seq = r.u64()?;
        let op = match tag {
            1 => Op::Put {
                key: r.bytes()?,
                value: r.bytes()?,
            },
            2 => Op::Get { key: r.bytes()? },
            3 => Op::Delete { key: r.bytes()? },
            4 => Op::Append {
                key: r.bytes()?,
                value: r.bytes()?,
            },
            5 => {
                let key = r.bytes()?;
                let expected = match r.u8()? {
                    0 => None,
                    1 => Some(r.bytes()?),
                    _ => return None,
                };
                Op::Cas {
                    key,
                    expected,
                    new: r.bytes()?,
                }
            }
            _ => return None,
        };
        if !r.finished() {
            return None;
        }
        Some(Command { client, seq, op })
    }
}

// ---------------------------------------------------------------------------
// The state machine
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Default)]
pub struct Kv {
    data: BTreeMap<Vec<u8>, Vec<u8>>,
    /// client -> (last seq executed, the response it produced)
    sessions: BTreeMap<ClientId, (u64, Resp)>,
}

impl Kv {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, key: &[u8]) -> Option<&Vec<u8>> {
        self.data.get(key)
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// The pure lookup behind a linearizable read.
    pub fn read(&self, key: &[u8]) -> Resp {
        Resp::Value(self.data.get(key).cloned())
    }

    /// What the session table says about `(client, seq)`:
    /// `Some(saved response)` for a retry of the last command,
    /// `Some(Resp::Stale)` for an older one, `None` if it is new.
    pub fn session_lookup(&self, client: ClientId, seq: u64) -> Option<Resp> {
        match self.sessions.get(&client) {
            Some((last, resp)) if seq == *last => Some(resp.clone()),
            Some((last, _)) if seq < *last => Some(Resp::Stale),
            _ => None,
        }
    }

    /// Apply one committed command. Deterministic.
    pub fn apply(&mut self, cmd: &Command) -> Resp {
        if let Some(resp) = self.session_lookup(cmd.client, cmd.seq) {
            return resp;
        }
        let resp = self.execute(&cmd.op);
        self.sessions.insert(cmd.client, (cmd.seq, resp.clone()));
        resp
    }

    fn execute(&mut self, op: &Op) -> Resp {
        match op {
            Op::Put { key, value } => {
                self.data.insert(key.clone(), value.clone());
                Resp::Ok
            }
            Op::Get { key } => self.read(key),
            Op::Delete { key } => Resp::Deleted(self.data.remove(key).is_some()),
            Op::Append { key, value } => {
                let v = self.data.entry(key.clone()).or_default();
                v.extend_from_slice(value);
                Resp::Value(Some(v.clone()))
            }
            Op::Cas { key, expected, new } => {
                if self.data.get(key) == expected.as_ref() {
                    self.data.insert(key.clone(), new.clone());
                    Resp::Cas(true)
                } else {
                    Resp::Cas(false)
                }
            }
        }
    }

    /// A 64-bit fingerprint of the whole replicated state (data and sessions).
    /// Replicas that applied the same log prefix must agree on it.
    pub fn digest(&self) -> u64 {
        fn mix(h: &mut u64, bytes: &[u8]) {
            for b in bytes {
                *h ^= u64::from(*b);
                *h = h.wrapping_mul(0x0100_0000_01b3); // FNV-1a
            }
        }
        fn mix_bytes(h: &mut u64, b: &[u8]) {
            mix(h, &(b.len() as u64).to_le_bytes());
            mix(h, b);
        }
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        mix(&mut h, &(self.data.len() as u64).to_le_bytes());
        for (k, v) in &self.data {
            mix_bytes(&mut h, k);
            mix_bytes(&mut h, v);
        }
        mix(&mut h, &(self.sessions.len() as u64).to_le_bytes());
        for (client, (seq, resp)) in &self.sessions {
            mix(&mut h, &client.to_le_bytes());
            mix(&mut h, &seq.to_le_bytes());
            match resp {
                Resp::Ok => mix(&mut h, &[0]),
                Resp::Value(None) => mix(&mut h, &[1, 0]),
                Resp::Value(Some(v)) => {
                    mix(&mut h, &[1, 1]);
                    mix_bytes(&mut h, v);
                }
                Resp::Deleted(b) => mix(&mut h, &[2, u8::from(*b)]),
                Resp::Cas(b) => mix(&mut h, &[3, u8::from(*b)]),
                Resp::Stale => mix(&mut h, &[4]),
            }
        }
        h
    }
}
