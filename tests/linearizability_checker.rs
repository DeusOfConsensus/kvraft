//! The checker is the oracle for every later KV test, so it is tested on its
//! own first: histories that must pass, histories that must be rejected, and a
//! cross-check against the real state machine.

mod common;

use common::Rng;
use kvraft::kv::{Command, Kv, Op, Resp};
use kvraft::linearizability::{HistOp, Verdict, check, check_with_limit};

fn b(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}
fn put(k: &str, v: &str) -> Op {
    Op::Put {
        key: b(k),
        value: b(v),
    }
}
fn get(k: &str) -> Op {
    Op::Get { key: b(k) }
}
fn del(k: &str) -> Op {
    Op::Delete { key: b(k) }
}
fn append(k: &str, v: &str) -> Op {
    Op::Append {
        key: b(k),
        value: b(v),
    }
}
fn cas(k: &str, expected: Option<&str>, new: &str) -> Op {
    Op::Cas {
        key: b(k),
        expected: expected.map(b),
        new: b(new),
    }
}
fn val(v: Option<&str>) -> Resp {
    Resp::Value(v.map(b))
}

/// A finished operation.
fn done(client: u64, op: Op, invoke: u64, ret: u64, resp: Resp) -> HistOp {
    HistOp {
        client,
        op,
        invoke,
        ret: Some(ret),
        resp: Some(resp),
    }
}

/// An operation whose client never got an answer.
fn pending(client: u64, op: Op, invoke: u64) -> HistOp {
    HistOp {
        client,
        op,
        invoke,
        ret: None,
        resp: None,
    }
}

fn ok(h: &[HistOp]) -> bool {
    check(h) == Verdict::Linearizable
}

fn violation(h: &[HistOp]) -> bool {
    matches!(check(h), Verdict::Violation { .. })
}

#[test]
fn an_empty_history_is_linearizable() {
    assert!(ok(&[]));
}

#[test]
fn a_plain_sequential_history_passes() {
    let h = [
        done(1, put("a", "1"), 0, 1, Resp::Ok),
        done(1, get("a"), 2, 3, val(Some("1"))),
        done(1, del("a"), 4, 5, Resp::Deleted(true)),
        done(1, get("a"), 6, 7, val(None)),
        done(1, del("a"), 8, 9, Resp::Deleted(false)),
    ];
    assert!(ok(&h));
}

#[test]
fn a_stale_read_is_rejected() {
    let h = [
        done(1, put("a", "1"), 0, 1, Resp::Ok),
        done(1, put("a", "2"), 2, 3, Resp::Ok),
        done(2, get("a"), 4, 5, val(Some("1"))), // started after "2" was acknowledged
    ];
    assert!(violation(&h));
}

#[test]
fn a_read_of_a_never_written_value_is_rejected() {
    let h = [
        done(1, put("a", "1"), 0, 1, Resp::Ok),
        done(2, get("a"), 2, 3, val(Some("nonsense"))),
    ];
    assert!(violation(&h));
}

#[test]
fn a_read_concurrent_with_a_write_may_see_either_value() {
    for seen in [None, Some("1")] {
        let h = [
            done(1, put("a", "1"), 0, 10, Resp::Ok),
            done(2, get("a"), 2, 3, val(seen)),
        ];
        assert!(ok(&h), "seeing {seen:?} must be allowed");
    }
}

#[test]
fn a_value_cannot_be_unseen() {
    // The write is still in flight, one reader saw it, and a LATER reader did not.
    let h = [
        done(1, put("a", "1"), 0, 10, Resp::Ok),
        done(2, get("a"), 1, 2, val(Some("1"))),
        done(3, get("a"), 3, 4, val(None)),
    ];
    assert!(violation(&h));
}

#[test]
fn an_unanswered_write_may_or_may_not_have_happened() {
    for seen in [None, Some("1")] {
        let h = [
            pending(1, put("a", "1"), 0),
            done(2, get("a"), 5, 6, val(seen)),
        ];
        assert!(ok(&h), "seeing {seen:?} must be allowed");
    }
}

#[test]
fn an_unanswered_write_cannot_explain_a_different_value() {
    let h = [
        pending(1, put("a", "1"), 0),
        done(2, get("a"), 5, 6, val(Some("2"))),
    ];
    assert!(violation(&h));
}

#[test]
fn an_unanswered_write_cannot_take_effect_before_it_was_sent() {
    let h = [
        done(2, get("a"), 0, 1, val(Some("1"))),
        pending(1, put("a", "1"), 5), // sent after the read finished
    ];
    assert!(violation(&h));
}

#[test]
fn an_append_applied_twice_is_caught() {
    let wrong = [
        done(1, append("k", "a"), 0, 1, val(Some("a"))),
        done(2, get("k"), 2, 3, val(Some("aa"))), // the duplicate
    ];
    assert!(violation(&wrong));
    let right = [
        done(1, append("k", "a"), 0, 1, val(Some("a"))),
        done(2, get("k"), 2, 3, val(Some("a"))),
    ];
    assert!(ok(&right));
}

#[test]
fn concurrent_appends_must_agree_on_one_order() {
    let consistent = [
        done(1, append("k", "a"), 0, 5, val(Some("a"))),
        done(2, append("k", "b"), 1, 6, val(Some("ab"))),
    ];
    assert!(ok(&consistent));
    // Both claim to have gone first.
    let impossible = [
        done(1, append("k", "a"), 0, 5, val(Some("a"))),
        done(2, append("k", "b"), 1, 6, val(Some("b"))),
    ];
    assert!(violation(&impossible));
}

#[test]
fn only_one_of_two_cas_on_an_absent_key_can_win() {
    let one_wins = [
        done(1, cas("k", None, "x"), 0, 5, Resp::Cas(true)),
        done(2, cas("k", None, "y"), 1, 6, Resp::Cas(false)),
        done(3, get("k"), 7, 8, val(Some("x"))),
    ];
    assert!(ok(&one_wins));
    let both_win = [
        done(1, cas("k", None, "x"), 0, 5, Resp::Cas(true)),
        done(2, cas("k", None, "y"), 1, 6, Resp::Cas(true)),
    ];
    assert!(violation(&both_win));
}

#[test]
fn a_response_of_the_wrong_kind_is_rejected() {
    assert!(violation(&[done(1, put("a", "1"), 0, 1, val(Some("1")))]));
    assert!(violation(&[done(1, get("a"), 0, 1, Resp::Ok)]));
    assert!(violation(&[done(1, put("a", "1"), 0, 1, Resp::Stale)]));
}

#[test]
fn keys_are_checked_independently_and_the_bad_one_is_named() {
    let h = [
        done(1, put("good", "1"), 0, 1, Resp::Ok),
        done(1, get("good"), 2, 3, val(Some("1"))),
        done(1, put("bad", "1"), 4, 5, Resp::Ok),
        done(2, get("bad"), 6, 7, val(None)),
    ];
    match check(&h) {
        Verdict::Violation { key, detail } => {
            assert_eq!(key, b("bad"));
            assert!(detail.contains("\"bad\""), "{detail}");
        }
        other => panic!("expected a violation, got {other:?}"),
    }
}

#[test]
fn an_exhausted_budget_is_reported_as_inconclusive_not_as_a_verdict() {
    let h = [
        done(1, put("a", "1"), 0, 10, Resp::Ok),
        done(2, put("a", "2"), 0, 10, Resp::Ok),
        done(3, get("a"), 0, 10, val(Some("2"))),
        done(4, get("a"), 0, 10, val(Some("1"))),
    ];
    assert!(matches!(
        check_with_limit(&h, 1),
        Verdict::Inconclusive { .. }
    ));
    assert!(ok(&h), "with the default budget it is decidable");
}

// ---------------------------------------------------------------------------
// Cross-check against the real state machine
// ---------------------------------------------------------------------------

/// Run random operations one at a time on the real `Kv`, then smear each
/// operation's timestamps so neighbours overlap. The result is linearizable by
/// construction (the true order is a valid linearization). The checker's model
/// and `Kv` are separate implementations, so agreement here is evidence both
/// are right.
fn smeared_history(seed: u64, ops: usize) -> Vec<HistOp> {
    let mut rng = Rng::new(seed);
    let mut kv = Kv::new();
    let mut seq = [0u64; 4];
    let mut hist = Vec::new();
    for i in 0..ops {
        let client = 1 + rng.below(4);
        let key = format!("k{}", rng.below(3));
        let uniq = format!("v{i}");
        let op = match rng.below(5) {
            0 => put(&key, &uniq),
            1 => get(&key),
            2 => del(&key),
            3 => append(&key, &uniq),
            _ => {
                let expected = match rng.below(3) {
                    0 => None,
                    1 => kv
                        .get(key.as_bytes())
                        .map(|v| String::from_utf8_lossy(v).into_owned()),
                    _ => Some("never".to_string()),
                };
                cas(&key, expected.as_deref(), &uniq)
            }
        };
        let s = &mut seq[(client - 1) as usize];
        *s += 1;
        let resp = match &op {
            Op::Get { key } => kv.read(key),
            _ => kv.apply(&Command {
                client,
                seq: *s,
                op: op.clone(),
            }),
        };
        let at = 10 * i as u64 + 100;
        hist.push(done(
            client,
            op,
            at - rng.below(25),
            at + rng.below(25),
            resp,
        ));
    }
    hist
}

#[test]
fn histories_generated_from_the_real_state_machine_are_linearizable() {
    for seed in 1..=common::seeds(100) {
        let h = smeared_history(seed, 60);
        assert_eq!(check(&h), Verdict::Linearizable, "seed {seed}");
    }
}

#[test]
fn corrupting_one_read_in_such_a_history_is_always_detected() {
    let mut detected = 0;
    for seed in 1..=common::seeds(100) {
        let mut h = smeared_history(seed, 60);
        let Some(idx) = h.iter().position(|o| matches!(o.op, Op::Get { .. })) else {
            continue;
        };
        h[idx].resp = Some(val(Some("a-value-nobody-ever-wrote")));
        assert!(violation(&h), "seed {seed}: corrupted read not detected");
        detected += 1;
    }
    assert!(
        detected > 50,
        "the test must actually exercise the corruption"
    );
}

#[test]
fn dropping_a_completed_write_from_the_history_is_noticed_by_later_reads() {
    // Pretend the system lost a write: reads that saw it are now unexplained.
    let mut caught = 0;
    for seed in 1..=common::seeds(100) {
        let mut h = smeared_history(seed, 60);
        let Some(w) = h.iter().position(|o| matches!(o.op, Op::Put { .. })) else {
            continue;
        };
        let key = h[w].op.key().to_vec();
        let value = match &h[w].op {
            Op::Put { value, .. } => value.clone(),
            _ => unreachable!(),
        };
        let seen_later = h.iter().any(|o| {
            matches!(&o.resp, Some(Resp::Value(Some(v))) if *v == value) && o.op.key() == key
        });
        h.remove(w);
        if seen_later {
            assert!(violation(&h), "seed {seed}: the lost write went unnoticed");
            caught += 1;
        }
    }
    assert!(caught > 10, "the test must actually exercise lost writes");
}
