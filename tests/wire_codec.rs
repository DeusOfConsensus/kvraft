mod common;

use common::Rng;
use kvraft::kv::{Command, Op, Resp};
use kvraft::raft::{Entry, Message};
use kvraft::server::{Outcome, Reply};
use kvraft::wire::*;

fn b(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

fn all_messages() -> Vec<Message> {
    let entries = vec![
        Entry {
            term: 3,
            data: vec![],
        },
        Entry {
            term: 4,
            data: b("payload"),
        },
        Entry {
            term: u64::MAX,
            data: vec![0, 255, 0],
        },
    ];
    vec![
        Message::RequestVote {
            term: 9,
            last_log_index: 100,
            last_log_term: 8,
        },
        Message::RequestVoteReply {
            term: 9,
            granted: true,
        },
        Message::RequestVoteReply {
            term: u64::MAX,
            granted: false,
        },
        Message::PreVote {
            term: 1,
            last_log_index: 0,
            last_log_term: 0,
        },
        Message::PreVoteReply {
            term: 2,
            granted: true,
        },
        Message::AppendEntries {
            term: 5,
            prev_log_index: 7,
            prev_log_term: 4,
            entries: vec![],
            leader_commit: 6,
            round: 0,
        },
        Message::AppendEntries {
            term: 5,
            prev_log_index: 7,
            prev_log_term: 4,
            entries,
            leader_commit: u64::MAX,
            round: 77,
        },
        Message::AppendEntriesReply {
            term: 5,
            success: true,
            round: 77,
            match_index: 10,
            hint: 0,
        },
        Message::AppendEntriesReply {
            term: 5,
            success: false,
            round: 1,
            match_index: 0,
            hint: 3,
        },
    ]
}

fn all_replies() -> Vec<Reply> {
    let outcomes = vec![
        Outcome::Done(Resp::Ok),
        Outcome::Done(Resp::Value(None)),
        Outcome::Done(Resp::Value(Some(vec![]))),
        Outcome::Done(Resp::Value(Some(b("v")))),
        Outcome::Done(Resp::Deleted(true)),
        Outcome::Done(Resp::Deleted(false)),
        Outcome::Done(Resp::Cas(true)),
        Outcome::Done(Resp::Cas(false)),
        Outcome::Done(Resp::Stale),
        Outcome::NotLeader(None),
        Outcome::NotLeader(Some(3)),
        Outcome::Retry,
    ];
    outcomes
        .into_iter()
        .enumerate()
        .map(|(i, outcome)| Reply {
            client: u64::MAX - i as u64,
            seq: i as u64 * 31,
            outcome,
        })
        .collect()
}

#[test]
fn every_message_round_trips() {
    for m in all_messages() {
        assert_eq!(
            decode_message(&encode_message(&m)),
            Some(m.clone()),
            "{m:?}"
        );
    }
}

#[test]
fn every_truncation_and_extension_of_a_message_is_rejected() {
    for m in all_messages() {
        let bytes = encode_message(&m);
        for cut in 0..bytes.len() {
            assert_eq!(decode_message(&bytes[..cut]), None, "{m:?} cut at {cut}");
        }
        let mut longer = bytes.clone();
        longer.push(0);
        assert_eq!(decode_message(&longer), None, "{m:?} with trailing byte");
    }
}

#[test]
fn unknown_tags_and_bad_flags_are_rejected() {
    assert_eq!(decode_message(&[]), None);
    assert_eq!(decode_message(&[0]), None);
    assert_eq!(decode_message(&[7]), None);
    let mut v = encode_message(&Message::RequestVoteReply {
        term: 1,
        granted: true,
    });
    assert!(decode_message(&v).is_some());
    *v.last_mut().unwrap() = 2; // granted must be 0 or 1
    assert_eq!(decode_message(&v), None);
}

#[test]
fn a_huge_entry_count_with_no_data_fails_without_allocating() {
    let mut v = encode_message(&Message::AppendEntries {
        term: 1,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![],
        leader_commit: 0,
        round: 0,
    });
    let n = v.len();
    v[n - 4..].copy_from_slice(&u32::MAX.to_le_bytes());
    assert_eq!(decode_message(&v), None);

    // And an entry whose length field claims more than the buffer holds.
    let mut v = encode_message(&Message::AppendEntries {
        term: 1,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![Entry {
            term: 1,
            data: b("x"),
        }],
        leader_commit: 0,
        round: 0,
    });
    let n = v.len();
    v[n - 5..n - 1].copy_from_slice(&u32::MAX.to_le_bytes());
    assert_eq!(decode_message(&v), None);
}

#[test]
fn every_reply_round_trips_and_rejects_truncation() {
    for r in all_replies() {
        let bytes = encode_reply(&r);
        assert_eq!(decode_reply(&bytes), Some(r.clone()), "{r:?}");
        for cut in 0..bytes.len() {
            assert_eq!(decode_reply(&bytes[..cut]), None, "{r:?} cut at {cut}");
        }
        let mut longer = bytes.clone();
        longer.push(9);
        assert_eq!(decode_reply(&longer), None);
    }
}

#[test]
fn requests_reuse_the_command_codec() {
    let c = Command {
        client: 5,
        seq: 6,
        op: Op::Cas {
            key: b("k"),
            expected: None,
            new: b("n"),
        },
    };
    assert_eq!(decode_request(&encode_request(&c)), Some(c));
}

#[test]
fn hello_round_trips_and_is_strict() {
    assert_eq!(decode_hello(&encode_hello(42)), Some(42));
    assert_eq!(decode_hello(&[]), None);
    assert_eq!(decode_hello(&[b'X', 1, 0, 0, 0, 0, 0, 0, 0]), None);
    let mut h = encode_hello(1);
    h.push(0);
    assert_eq!(decode_hello(&h), None);
    h.truncate(5);
    assert_eq!(decode_hello(&h), None);
}

#[test]
fn framing_prefixes_the_length() {
    let f = frame(b"abc");
    assert_eq!(f, vec![3, 0, 0, 0, b'a', b'b', b'c']);
    assert_eq!(frame(b""), vec![0, 0, 0, 0]);
}

/// Random bytes, and valid encodings with a few bytes flipped: a decoder may
/// return anything, but it must never panic, and anything it accepts must
/// re-encode to the same bytes (so there is exactly one encoding of each value).
#[test]
fn decoders_survive_garbage_and_accept_only_canonical_encodings() {
    let mut rng = Rng::new(7);
    let valid: Vec<Vec<u8>> = all_messages().iter().map(encode_message).collect();
    let valid_replies: Vec<Vec<u8>> = all_replies().iter().map(encode_reply).collect();
    for _ in 0..common::seeds(1) * 40_000 {
        let mut buf: Vec<u8> = if rng.below(3) == 0 {
            (0..rng.below(64)).map(|_| rng.below(256) as u8).collect()
        } else if rng.below(2) == 0 {
            valid[rng.below(valid.len() as u64) as usize].clone()
        } else {
            valid_replies[rng.below(valid_replies.len() as u64) as usize].clone()
        };
        for _ in 0..rng.below(4) {
            if !buf.is_empty() {
                let i = rng.below(buf.len() as u64) as usize;
                buf[i] = rng.below(256) as u8;
            }
        }
        if let Some(m) = decode_message(&buf) {
            assert_eq!(encode_message(&m), buf, "non-canonical message accepted");
        }
        if let Some(r) = decode_reply(&buf) {
            assert_eq!(encode_reply(&r), buf, "non-canonical reply accepted");
        }
        if let Some(c) = decode_request(&buf) {
            assert_eq!(encode_request(&c), buf, "non-canonical request accepted");
        }
        let _ = decode_hello(&buf);
    }
}
