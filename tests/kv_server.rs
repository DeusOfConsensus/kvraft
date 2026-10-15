//! The key-value server and cluster, one behaviour at a time. The randomized
//! sweep lives in `kv_linearizable.rs`.

mod common;

use kvraft::kv::{Op, Resp};
use kvraft::kvsim::KvCluster;
use kvraft::linearizability::Verdict;
use kvraft::raft::Config;
use kvraft::server::{KvServer, Outcome, Reply};
use kvraft::storage::MemStorage;

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
fn append(k: &str, v: &str) -> Op {
    Op::Append {
        key: b(k),
        value: b(v),
    }
}

// ---------------------------------------------------------------------------
// One server on its own (a cluster of one is its own quorum)
// ---------------------------------------------------------------------------

fn single() -> KvServer<MemStorage> {
    let mut s = KvServer::new(1, vec![], Config::default(), MemStorage::new());
    for _ in 0..100 {
        s.tick();
    }
    s
}

/// Submit and tick until exactly one reply for `(client, seq)` arrives.
fn ask(s: &mut KvServer<MemStorage>, client: u64, seq: u64, op: Op) -> Outcome {
    s.submit(client, seq, op);
    let mut got: Vec<Reply> = s.drain_replies();
    for _ in 0..50 {
        if !got.is_empty() {
            break;
        }
        s.tick();
        got = s.drain_replies();
    }
    assert_eq!(got.len(), 1, "expected one reply, got {got:?}");
    assert_eq!((got[0].client, got[0].seq), (client, seq));
    got.remove(0).outcome
}

#[test]
fn a_single_server_stores_and_reads_back() {
    let mut s = single();
    assert_eq!(ask(&mut s, 1, 1, put("a", "1")), Outcome::Done(Resp::Ok));
    assert_eq!(
        ask(&mut s, 1, 2, get("a")),
        Outcome::Done(Resp::Value(Some(b("1"))))
    );
    assert_eq!(
        ask(&mut s, 1, 3, get("zzz")),
        Outcome::Done(Resp::Value(None))
    );
}

#[test]
fn a_read_is_not_written_to_the_log() {
    let mut s = single();
    ask(&mut s, 1, 1, put("a", "1"));
    let before = s.node().last_index();
    for seq in 2..20 {
        ask(&mut s, 1, seq, get("a"));
    }
    assert_eq!(s.node().last_index(), before, "reads must not grow the log");
}

#[test]
fn a_retried_write_gets_the_saved_answer_and_is_applied_once() {
    let mut s = single();
    let first = ask(&mut s, 7, 1, append("log", "x"));
    assert_eq!(first, Outcome::Done(Resp::Value(Some(b("x")))));
    let log_len = s.node().last_index();
    // The reply was lost; the client asks again, several times.
    for _ in 0..3 {
        assert_eq!(ask(&mut s, 7, 1, append("log", "x")), first);
    }
    assert_eq!(
        s.node().last_index(),
        log_len,
        "a retry of an executed command must not add log entries"
    );
    assert_eq!(s.kv().read(b"log"), Resp::Value(Some(b("x"))));
}

#[test]
fn sessions_survive_a_restart() {
    let mut s = single();
    let first = ask(&mut s, 7, 1, append("log", "x"));
    // Process dies; only the disk survives. The state machine is rebuilt by
    // replaying the log, session table included.
    let storage = s.into_storage();
    let mut s = KvServer::new(1, vec![], Config::default(), storage);
    for _ in 0..100 {
        s.tick();
    }
    assert_eq!(ask(&mut s, 7, 1, append("log", "x")), first);
    assert_eq!(
        s.kv().read(b"log"),
        Resp::Value(Some(b("x"))),
        "applied once"
    );
}

// ---------------------------------------------------------------------------
// A cluster
// ---------------------------------------------------------------------------

#[test]
fn a_follower_points_the_client_at_the_leader() {
    let mut c = KvCluster::new(3, 5);
    assert!(c.run_until(300, |c| c.leader().is_some()));
    c.run(30); // let followers learn who leads
    let leader = c.leader().unwrap();
    let follower = *c.ids().iter().find(|&&i| i != leader).unwrap();
    let s = c.servers.get_mut(&follower).unwrap();
    s.submit(9, 1, put("a", "1"));
    s.submit(9, 2, get("a"));
    let replies = s.drain_replies();
    assert_eq!(replies.len(), 2);
    for r in replies {
        assert_eq!(r.outcome, Outcome::NotLeader(Some(leader)), "{r:?}");
    }
}

#[test]
fn clients_through_a_three_node_cluster_see_a_linearizable_store() {
    for seed in 1..=common::seeds(10) {
        let mut c = KvCluster::new(3, seed);
        for _ in 0..4 {
            let ops = c.random_ops(25, 3);
            c.add_client(ops, None);
        }
        assert!(c.run_until(5_000, KvCluster::clients_done), "seed {seed}");
        assert_eq!(c.verdict(), Verdict::Linearizable, "seed {seed}");
        assert!(c.run_until(500, KvCluster::converged), "seed {seed}");
    }
}

#[test]
fn a_lossy_network_never_applies_an_append_twice() {
    for seed in 1..=common::seeds(10) {
        let mut c = KvCluster::new(3, seed);
        c.drop_pct = 25;
        let mut expected: Vec<String> = Vec::new();
        for client in 0..3 {
            let mut ops = Vec::new();
            for i in 0..12 {
                let piece = format!("c{client}-{i};");
                ops.push(append("log", &piece));
                expected.push(piece);
            }
            c.add_client(ops, None);
        }
        assert!(c.run_until(20_000, KvCluster::clients_done), "seed {seed}");
        c.drop_pct = 0;
        let reader = c.add_client(vec![get("log")], None);
        assert!(c.run_until(2_000, KvCluster::clients_done), "seed {seed}");
        let _ = reader;
        let last = c.history.last().unwrap();
        let Some(Resp::Value(Some(v))) = &last.resp else {
            panic!("seed {seed}: final read returned {:?}", last.resp);
        };
        let text = String::from_utf8(v.clone()).unwrap();
        for piece in &expected {
            assert_eq!(
                text.matches(piece.as_str()).count(),
                1,
                "seed {seed}: {piece} must appear exactly once in {text}"
            );
        }
        assert_eq!(c.verdict(), Verdict::Linearizable, "seed {seed}");
    }
}

/// The classic ReadIndex scenario. The old leader is cut off from the other
/// servers but still reachable by a client. It still believes it leads, and its
/// local copy of `x` is out of date. A read served from that copy would return
/// a value that was overwritten before the read began.
#[test]
fn a_deposed_leader_that_clients_can_still_reach_never_answers_a_read() {
    for seed in 1..=common::seeds(10) {
        // CheckQuorum would make the old leader step down by itself; turn it
        // off so the old leader stays convinced for as long as we like.
        let mut c = KvCluster::with_config(
            3,
            Config {
                seed,
                check_quorum: false,
                ..Config::default()
            },
        );
        let w1 = c.add_client(vec![put("x", "1")], None);
        assert!(c.run_until(3_000, KvCluster::clients_done), "seed {seed}");
        assert_eq!(c.clients[w1].completed, 1);
        assert!(c.run_until(500, KvCluster::converged), "seed {seed}");

        let old = c.leader().unwrap();
        c.isolate(old);
        let reader = c.add_client(vec![get("x")], Some(old));
        assert!(
            c.run_until(500, |c| c.leader().is_some_and(|l| l != old)),
            "seed {seed}: the others must elect a new leader"
        );
        let w2 = c.add_client(vec![put("x", "2")], None);
        assert!(
            c.run_until(3_000, |c| c.clients[w2].is_done()),
            "seed {seed}: the write through the new leader must succeed"
        );
        c.run(100);
        assert_eq!(
            c.clients[reader].completed,
            0,
            "seed {seed}: the deposed leader answered a read ({:?})",
            c.history.last()
        );

        // Reconnect it. The reader may now be redirected, and must see 2.
        c.heal_all();
        c.clients[reader].pinned = None;
        assert!(c.run_until(3_000, KvCluster::clients_done), "seed {seed}");
        let read = c
            .history
            .iter()
            .find(|h| h.client == c.clients[reader].id)
            .unwrap();
        assert_eq!(read.resp, Some(Resp::Value(Some(b("2")))), "seed {seed}");
        assert_eq!(c.verdict(), Verdict::Linearizable, "seed {seed}");
    }
}

#[test]
fn the_whole_cluster_can_crash_and_come_back_with_its_data() {
    for seed in 1..=common::seeds(10) {
        let mut c = KvCluster::new(3, seed);
        for _ in 0..3 {
            let ops = c.random_ops(20, 4);
            c.add_client(ops, None);
        }
        assert!(c.run_until(5_000, KvCluster::clients_done), "seed {seed}");
        assert!(c.run_until(500, KvCluster::converged), "seed {seed}");
        let digest = c.servers[&1].kv().digest();
        let committed = c.servers[&1].node().commit_index();

        for id in c.ids().to_vec() {
            c.crash(id);
        }
        assert!(c.servers.is_empty());
        c.restart_all();
        assert!(
            c.run_until(2_000, |c| c.converged()
                && c.servers
                    .values()
                    .all(|s| s.node().commit_index() >= committed)),
            "seed {seed}: cluster did not recover"
        );
        assert_eq!(c.servers[&1].kv().digest(), digest, "seed {seed}");

        // And it still serves.
        c.add_client(vec![put("after", "crash"), get("after")], None);
        assert!(c.run_until(2_000, KvCluster::clients_done), "seed {seed}");
        assert_eq!(c.verdict(), Verdict::Linearizable, "seed {seed}");
    }
}

#[test]
fn a_leader_that_steps_down_answers_everything_it_was_holding() {
    // With CheckQuorum on, a leader cut off from its peers steps down after an
    // election timeout. Requests parked on it must be released (with "retry"),
    // not held forever.
    for seed in 1..=common::seeds(10) {
        let mut c = KvCluster::new(3, seed);
        assert!(c.run_until(500, |c| c.leader().is_some()), "seed {seed}");
        c.run(30);
        let old = c.leader().unwrap();
        c.isolate(old);
        // One write and one read, delivered straight to the cut-off leader.
        let s = c.servers.get_mut(&old).unwrap();
        s.submit(50, 1, put("x", "lost"));
        s.submit(51, 1, get("x"));
        assert_eq!(s.waiting(), 2, "seed {seed}: both must be parked");
        c.run(60);
        assert_ne!(
            c.servers[&old].node().role(),
            kvraft::raft::Role::Leader,
            "seed {seed}: CheckQuorum should have deposed it"
        );
        assert_eq!(
            c.servers[&old].waiting(),
            0,
            "seed {seed}: requests still parked on a node that is no longer leader"
        );
    }
}
