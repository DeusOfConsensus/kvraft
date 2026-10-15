use kvraft::kv::{Command, Kv, Op, Resp};

fn b(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

fn cmd(client: u64, seq: u64, op: Op) -> Command {
    Command { client, seq, op }
}

fn put(k: &str, v: &str) -> Op {
    Op::Put {
        key: b(k),
        value: b(v),
    }
}

fn append(k: &str, v: &str) -> Op {
    Op::Append {
        key: b(k),
        value: b(v),
    }
}

fn all_ops() -> Vec<Op> {
    vec![
        put("k", "v"),
        put("", ""), // empty key and value are legal
        Op::Get { key: b("k") },
        Op::Delete { key: b("k") },
        append("k", "tail"),
        Op::Cas {
            key: b("k"),
            expected: None,
            new: b("n"),
        },
        Op::Cas {
            key: b("k"),
            expected: Some(b("old")),
            new: b("n"),
        },
        Op::Cas {
            key: b("k"),
            expected: Some(Vec::new()), // "must equal the empty value" is not "must be absent"
            new: Vec::new(),
        },
        put("binary", "\u{0}\u{1}\u{ff}"),
    ]
}

// ---------------------------------------------------------------------------
// Codec
// ---------------------------------------------------------------------------

#[test]
fn every_command_round_trips() {
    for (i, op) in all_ops().into_iter().enumerate() {
        let c = cmd(u64::MAX - i as u64, i as u64 * 1_000_003, op);
        let bytes = c.encode();
        assert!(!bytes.is_empty(), "an encoded command must never be empty");
        assert_eq!(Command::decode(&bytes), Some(c.clone()), "{c:?}");
    }
}

#[test]
fn every_truncation_is_rejected() {
    for op in all_ops() {
        let bytes = cmd(7, 9, op).encode();
        for cut in 0..bytes.len() {
            assert_eq!(
                Command::decode(&bytes[..cut]),
                None,
                "a {cut}-byte prefix of a {}-byte command must not decode",
                bytes.len()
            );
        }
    }
}

#[test]
fn trailing_bytes_and_bad_tags_are_rejected() {
    let mut bytes = cmd(1, 1, put("a", "b")).encode();
    bytes.push(0);
    assert_eq!(Command::decode(&bytes), None);

    let mut bytes = cmd(1, 1, put("a", "b")).encode();
    bytes[0] = 0;
    assert_eq!(Command::decode(&bytes), None);
    bytes[0] = 6;
    assert_eq!(Command::decode(&bytes), None);

    // A Cas whose `has_expected` flag is neither 0 nor 1.
    let good = cmd(
        1,
        1,
        Op::Cas {
            key: b("k"),
            expected: None,
            new: b("n"),
        },
    )
    .encode();
    let flag_at = 1 + 8 + 8 + 4 + 1; // tag, client, seq, key length, key "k"
    let mut bad = good.clone();
    bad[flag_at] = 2;
    assert_eq!(Command::decode(&bad), None);
    assert!(Command::decode(&good).is_some());
}

#[test]
fn a_length_field_larger_than_the_buffer_is_rejected_without_panicking() {
    let mut bytes = cmd(1, 1, put("a", "b")).encode();
    // Overwrite the key length with u32::MAX.
    bytes[17..21].copy_from_slice(&u32::MAX.to_le_bytes());
    assert_eq!(Command::decode(&bytes), None);
}

// ---------------------------------------------------------------------------
// Semantics
// ---------------------------------------------------------------------------

#[test]
fn put_get_delete() {
    let mut kv = Kv::new();
    assert_eq!(kv.apply(&cmd(1, 1, put("a", "1"))), Resp::Ok);
    assert_eq!(kv.read(b"a"), Resp::Value(Some(b("1"))));
    assert_eq!(kv.read(b"missing"), Resp::Value(None));
    assert_eq!(kv.apply(&cmd(1, 2, put("a", "2"))), Resp::Ok);
    assert_eq!(kv.read(b"a"), Resp::Value(Some(b("2"))));
    assert_eq!(
        kv.apply(&cmd(1, 3, Op::Delete { key: b("a") })),
        Resp::Deleted(true)
    );
    assert_eq!(
        kv.apply(&cmd(1, 4, Op::Delete { key: b("a") })),
        Resp::Deleted(false)
    );
    assert_eq!(kv.read(b"a"), Resp::Value(None));
}

#[test]
fn append_returns_the_new_whole_value() {
    let mut kv = Kv::new();
    assert_eq!(
        kv.apply(&cmd(1, 1, append("k", "a"))),
        Resp::Value(Some(b("a")))
    );
    assert_eq!(
        kv.apply(&cmd(1, 2, append("k", "b"))),
        Resp::Value(Some(b("ab")))
    );
}

#[test]
fn cas_distinguishes_absent_from_empty() {
    let mut kv = Kv::new();
    let absent = |new: &str| Op::Cas {
        key: b("k"),
        expected: None,
        new: b(new),
    };
    let equals = |exp: &str, new: &str| Op::Cas {
        key: b("k"),
        expected: Some(b(exp)),
        new: b(new),
    };

    assert_eq!(kv.apply(&cmd(1, 1, equals("", "x"))), Resp::Cas(false));
    assert_eq!(kv.apply(&cmd(1, 2, absent("empty-now"))), Resp::Cas(true));
    assert_eq!(kv.apply(&cmd(1, 3, absent("again"))), Resp::Cas(false));
    assert_eq!(
        kv.apply(&cmd(1, 4, equals("empty-now", ""))),
        Resp::Cas(true)
    );
    // The value is now the empty string: present, but not absent.
    assert_eq!(kv.read(b"k"), Resp::Value(Some(Vec::new())));
    assert_eq!(kv.apply(&cmd(1, 5, absent("no"))), Resp::Cas(false));
    assert_eq!(kv.apply(&cmd(1, 6, equals("", "yes"))), Resp::Cas(true));
}

// ---------------------------------------------------------------------------
// Sessions: exactly-once
// ---------------------------------------------------------------------------

#[test]
fn a_retried_append_is_applied_once_and_gets_the_same_answer() {
    let mut kv = Kv::new();
    let c = cmd(5, 1, append("log", "x"));
    let first = kv.apply(&c);
    assert_eq!(first, Resp::Value(Some(b("x"))));

    // The reply was lost, the client retries: same (client, seq).
    let digest = kv.digest();
    assert_eq!(kv.apply(&c), first, "the retry must get the saved response");
    assert_eq!(kv.apply(&c), first);
    assert_eq!(kv.read(b"log"), Resp::Value(Some(b("x"))), "applied once");
    assert_eq!(kv.digest(), digest, "a duplicate must not change any state");
}

#[test]
fn an_old_duplicate_after_the_client_moved_on_is_ignored() {
    let mut kv = Kv::new();
    kv.apply(&cmd(5, 1, append("k", "a")));
    kv.apply(&cmd(5, 2, append("k", "b")));
    // A delayed copy of seq 1 finally shows up.
    assert_eq!(kv.apply(&cmd(5, 1, append("k", "a"))), Resp::Stale);
    assert_eq!(kv.read(b"k"), Resp::Value(Some(b("ab"))));
}

#[test]
fn sessions_are_per_client() {
    let mut kv = Kv::new();
    kv.apply(&cmd(1, 1, append("k", "a")));
    // Another client using the same seq number is a different command.
    assert_eq!(
        kv.apply(&cmd(2, 1, append("k", "b"))),
        Resp::Value(Some(b("ab")))
    );
    assert_eq!(
        kv.apply(&cmd(1, 2, append("k", "c"))),
        Resp::Value(Some(b("abc")))
    );
}

#[test]
fn session_lookup_reports_without_executing() {
    let mut kv = Kv::new();
    assert_eq!(kv.session_lookup(1, 1), None);
    kv.apply(&cmd(1, 3, put("k", "v")));
    assert_eq!(kv.session_lookup(1, 3), Some(Resp::Ok));
    assert_eq!(kv.session_lookup(1, 2), Some(Resp::Stale));
    assert_eq!(kv.session_lookup(1, 4), None);
    assert_eq!(kv.session_lookup(2, 3), None);
}

// ---------------------------------------------------------------------------
// Determinism
// ---------------------------------------------------------------------------

#[test]
fn replicas_that_apply_the_same_commands_have_the_same_digest() {
    let script: Vec<Command> = vec![
        cmd(1, 1, put("a", "1")),
        cmd(2, 1, append("a", "2")),
        cmd(1, 2, Op::Delete { key: b("a") }),
        cmd(3, 1, put("b", "x")),
        cmd(2, 2, append("b", "y")),
    ];
    let mut r1 = Kv::new();
    let mut r2 = Kv::new();
    for c in &script {
        r1.apply(c);
        r2.apply(c);
        assert_eq!(r1.digest(), r2.digest());
    }
}

#[test]
fn the_digest_notices_data_and_session_differences() {
    let mut a = Kv::new();
    let mut b2 = Kv::new();
    a.apply(&cmd(1, 1, put("k", "v")));
    b2.apply(&cmd(1, 1, put("k", "w")));
    assert_ne!(a.digest(), b2.digest(), "different data");

    // Same data, different session history.
    let mut x = Kv::new();
    let mut y = Kv::new();
    x.apply(&cmd(1, 1, put("k", "v")));
    y.apply(&cmd(2, 1, put("k", "v")));
    assert_eq!(x.read(b"k"), y.read(b"k"));
    assert_ne!(x.digest(), y.digest(), "different sessions");
    assert_ne!(Kv::new().digest(), x.digest());
}
