//! The bytes that cross the network. No I/O here: just codecs, so everything in
//! this file can be tested (and fuzzed) without a socket.
//!
//! # Framing
//!
//! Every message on a TCP stream is `[len u32 LE][payload]`. A reader must
//! reject `len > MAX_FRAME` BEFORE allocating, or one hostile (or corrupted)
//! length prefix makes the server allocate gigabytes.
//!
//! # Peer connections (server <-> server, one direction each)
//!
//! The first frame is a hello, `[b'P'][node id u64]`, saying who is calling.
//! Every later frame is one Raft [`Message`]:
//!
//! ```text
//! tag 1 RequestVote       term, last_log_index, last_log_term
//! tag 2 RequestVoteReply  term, granted u8
//! tag 3 PreVote           term, last_log_index, last_log_term
//! tag 4 PreVoteReply      term, granted u8
//! tag 5 AppendEntries     term, prev_log_index, prev_log_term, leader_commit,
//!                         round, count u32, count * (term, len u32, data)
//! tag 6 AppendEntriesReply term, success u8, round, match_index, hint
//! ```
//!
//! All integers little-endian u64 unless stated. The sender's identity comes
//! from the hello, not from each message, so one connection cannot claim to be
//! several nodes.
//!
//! # Client connections
//!
//! Request frame: exactly a [`Command`] encoding (tag, client, seq, op).
//! Response frame: `[client u64][seq u64][outcome]`, where
//!
//! ```text
//! outcome 0 Done(resp)   resp: 0 Ok | 1 Value(flag u8 [, bytes]) | 2 Deleted(u8)
//!                              | 3 Cas(u8) | 4 Stale
//! outcome 1 NotLeader    hint flag u8 [, node id u64]
//! outcome 2 Retry
//! ```
//!
//! Status request: the single byte `b'S'` (no command is that short). Reply:
//! `[b'S'][id][role u8][term][leader flag u8 [, id]][commit_index][last_applied]`.
//! It lets operators, the CLI and tests ask "who is leading?".
//!
//! Every decoder returns `None` for anything that is not exactly one
//! well-formed value: truncated, trailing bytes, unknown tag, flag byte other
//! than 0/1. None of them can panic or over-allocate on hostile input.

use crate::kv::{Command, Reader, Resp, put_bytes};
use crate::raft::{Entry, Index, Message, NodeId, Role, Term};
use crate::server::{Outcome, Reply};

/// Largest frame either side will accept: 16 MiB.
pub const MAX_FRAME: usize = 16 * 1024 * 1024;

/// Prefix a payload with its length.
pub fn frame(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + payload.len());
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(payload);
    out
}

fn put_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn flag(r: &mut Reader) -> Option<bool> {
    match r.u8()? {
        0 => Some(false),
        1 => Some(true),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Hello
// ---------------------------------------------------------------------------

pub fn encode_hello(id: NodeId) -> Vec<u8> {
    let mut out = vec![b'P'];
    put_u64(&mut out, id);
    out
}

pub fn decode_hello(buf: &[u8]) -> Option<NodeId> {
    let mut r = Reader { buf, pos: 0 };
    if r.u8()? != b'P' {
        return None;
    }
    let id = r.u64()?;
    r.finished().then_some(id)
}

// ---------------------------------------------------------------------------
// Raft messages
// ---------------------------------------------------------------------------

pub fn encode_message(m: &Message) -> Vec<u8> {
    let mut o = Vec::new();
    match m {
        Message::RequestVote {
            term,
            last_log_index,
            last_log_term,
        } => {
            o.push(1);
            put_u64(&mut o, *term);
            put_u64(&mut o, *last_log_index);
            put_u64(&mut o, *last_log_term);
        }
        Message::RequestVoteReply { term, granted } => {
            o.push(2);
            put_u64(&mut o, *term);
            o.push(u8::from(*granted));
        }
        Message::PreVote {
            term,
            last_log_index,
            last_log_term,
        } => {
            o.push(3);
            put_u64(&mut o, *term);
            put_u64(&mut o, *last_log_index);
            put_u64(&mut o, *last_log_term);
        }
        Message::PreVoteReply { term, granted } => {
            o.push(4);
            put_u64(&mut o, *term);
            o.push(u8::from(*granted));
        }
        Message::AppendEntries {
            term,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
            round,
        } => {
            o.push(5);
            put_u64(&mut o, *term);
            put_u64(&mut o, *prev_log_index);
            put_u64(&mut o, *prev_log_term);
            put_u64(&mut o, *leader_commit);
            put_u64(&mut o, *round);
            o.extend_from_slice(&(entries.len() as u32).to_le_bytes());
            for e in entries {
                put_u64(&mut o, e.term);
                put_bytes(&mut o, &e.data);
            }
        }
        Message::AppendEntriesReply {
            term,
            success,
            round,
            match_index,
            hint,
        } => {
            o.push(6);
            put_u64(&mut o, *term);
            o.push(u8::from(*success));
            put_u64(&mut o, *round);
            put_u64(&mut o, *match_index);
            put_u64(&mut o, *hint);
        }
    }
    o
}

pub fn decode_message(buf: &[u8]) -> Option<Message> {
    let mut r = Reader { buf, pos: 0 };
    let m = match r.u8()? {
        1 => Message::RequestVote {
            term: r.u64()?,
            last_log_index: r.u64()?,
            last_log_term: r.u64()?,
        },
        2 => Message::RequestVoteReply {
            term: r.u64()?,
            granted: flag(&mut r)?,
        },
        3 => Message::PreVote {
            term: r.u64()?,
            last_log_index: r.u64()?,
            last_log_term: r.u64()?,
        },
        4 => Message::PreVoteReply {
            term: r.u64()?,
            granted: flag(&mut r)?,
        },
        5 => {
            let term = r.u64()?;
            let prev_log_index = r.u64()?;
            let prev_log_term = r.u64()?;
            let leader_commit = r.u64()?;
            let round = r.u64()?;
            let count = r.u32()?;
            // Never pre-allocate from an attacker-controlled count: each entry
            // needs at least 12 bytes, so a short buffer fails fast in the loop.
            let mut entries = Vec::new();
            for _ in 0..count {
                entries.push(Entry {
                    term: r.u64()?,
                    data: r.bytes()?,
                });
            }
            Message::AppendEntries {
                term,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                round,
            }
        }
        6 => Message::AppendEntriesReply {
            term: r.u64()?,
            success: flag(&mut r)?,
            round: r.u64()?,
            match_index: r.u64()?,
            hint: r.u64()?,
        },
        _ => return None,
    };
    r.finished().then_some(m)
}

// ---------------------------------------------------------------------------
// Client replies (requests reuse `Command::encode` / `Command::decode`)
// ---------------------------------------------------------------------------

pub fn encode_request(c: &Command) -> Vec<u8> {
    c.encode()
}

pub fn decode_request(buf: &[u8]) -> Option<Command> {
    Command::decode(buf)
}

fn put_resp(o: &mut Vec<u8>, r: &Resp) {
    match r {
        Resp::Ok => o.push(0),
        Resp::Value(None) => o.extend_from_slice(&[1, 0]),
        Resp::Value(Some(v)) => {
            o.extend_from_slice(&[1, 1]);
            put_bytes(o, v);
        }
        Resp::Deleted(b) => o.extend_from_slice(&[2, u8::from(*b)]),
        Resp::Cas(b) => o.extend_from_slice(&[3, u8::from(*b)]),
        Resp::Stale => o.push(4),
    }
}

fn get_resp(r: &mut Reader) -> Option<Resp> {
    Some(match r.u8()? {
        0 => Resp::Ok,
        1 => Resp::Value(if flag(r)? { Some(r.bytes()?) } else { None }),
        2 => Resp::Deleted(flag(r)?),
        3 => Resp::Cas(flag(r)?),
        4 => Resp::Stale,
        _ => return None,
    })
}

pub fn encode_reply(rep: &Reply) -> Vec<u8> {
    let mut o = Vec::new();
    put_u64(&mut o, rep.client);
    put_u64(&mut o, rep.seq);
    match &rep.outcome {
        Outcome::Done(resp) => {
            o.push(0);
            put_resp(&mut o, resp);
        }
        Outcome::NotLeader(hint) => {
            o.push(1);
            match hint {
                Some(h) => {
                    o.push(1);
                    put_u64(&mut o, *h);
                }
                None => o.push(0),
            }
        }
        Outcome::Retry => o.push(2),
    }
    o
}

pub fn decode_reply(buf: &[u8]) -> Option<Reply> {
    let mut r = Reader { buf, pos: 0 };
    let client = r.u64()?;
    let seq = r.u64()?;
    let outcome = match r.u8()? {
        0 => Outcome::Done(get_resp(&mut r)?),
        1 => Outcome::NotLeader(if flag(&mut r)? { Some(r.u64()?) } else { None }),
        2 => Outcome::Retry,
        _ => return None,
    };
    r.finished().then_some(Reply {
        client,
        seq,
        outcome,
    })
}

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------

/// A server's view of itself, for operators and tests.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Status {
    pub id: NodeId,
    pub role: Role,
    pub term: Term,
    pub leader: Option<NodeId>,
    pub commit_index: Index,
    pub last_applied: Index,
}

pub fn encode_status_request() -> Vec<u8> {
    vec![b'S']
}

pub fn is_status_request(buf: &[u8]) -> bool {
    buf == b"S"
}

pub fn encode_status(s: &Status) -> Vec<u8> {
    let mut o = vec![b'S'];
    put_u64(&mut o, s.id);
    o.push(match s.role {
        Role::Follower => 0,
        Role::PreCandidate => 1,
        Role::Candidate => 2,
        Role::Leader => 3,
    });
    put_u64(&mut o, s.term);
    match s.leader {
        Some(l) => {
            o.push(1);
            put_u64(&mut o, l);
        }
        None => o.push(0),
    }
    put_u64(&mut o, s.commit_index);
    put_u64(&mut o, s.last_applied);
    o
}

pub fn decode_status(buf: &[u8]) -> Option<Status> {
    let mut r = Reader { buf, pos: 0 };
    if r.u8()? != b'S' {
        return None;
    }
    let id = r.u64()?;
    let role = match r.u8()? {
        0 => Role::Follower,
        1 => Role::PreCandidate,
        2 => Role::Candidate,
        3 => Role::Leader,
        _ => return None,
    };
    let term = r.u64()?;
    let leader = if flag(&mut r)? { Some(r.u64()?) } else { None };
    let commit_index = r.u64()?;
    let last_applied = r.u64()?;
    r.finished().then_some(Status {
        id,
        role,
        term,
        leader,
        commit_index,
        last_applied,
    })
}
