//! Crash/restart behaviour: what a node must remember, and a fuzz that crashes
//! nodes at random while partitions and message loss are happening.

mod common;

use common::{TempWal, e, only_reply, peers_of, seeds};
use kvraft::raft::{Config, Entry, Message, Node, Role};
use kvraft::sim::Sim;
use kvraft::storage::{FileStorage, MemStorage, Storage};

fn vote_request(term: u64) -> Message {
    Message::RequestVote {
        term,
        last_log_index: 0,
        last_log_term: 0,
    }
}

// ---------------------------------------------------------------------------
// Targeted tests on a single node
// ---------------------------------------------------------------------------

/// If a node forgets its vote when it reboots, it can vote for two different
/// candidates in the same term, which can elect two leaders. This is the
/// reason `voted_for` is persistent state.
#[test]
fn restarted_node_cannot_vote_twice_in_one_term() {
    let cfg = Config::default();
    let mut n = Node::new(1, peers_of(1, 3), cfg.clone(), MemStorage::new());

    n.step(2, vote_request(5));
    assert_eq!(
        only_reply(&mut n),
        Message::RequestVoteReply {
            term: 5,
            granted: true
        }
    );
    assert_eq!(n.voted_for(), Some(2));

    // Crash: the process is gone, the disk survives.
    let disk = n.into_storage();
    let mut n = Node::new(1, peers_of(1, 3), cfg, disk);
    assert_eq!(n.term(), 5);
    assert_eq!(n.voted_for(), Some(2));

    // A different candidate asks for the same term: must be refused.
    n.step(3, vote_request(5));
    assert_eq!(
        only_reply(&mut n),
        Message::RequestVoteReply {
            term: 5,
            granted: false
        }
    );
    // The original candidate asking again is fine (its first reply may have been lost).
    n.step(2, vote_request(5));
    assert_eq!(
        only_reply(&mut n),
        Message::RequestVoteReply {
            term: 5,
            granted: true
        }
    );
}

/// Same story against a real WAL file, plus: an entry the follower acknowledged
/// must still be there after the restart, so the node still refuses to vote for
/// a candidate whose log is missing it.
#[test]
fn restart_from_real_wal_keeps_vote_term_and_acknowledged_entries() {
    let wal = TempWal::new("node-restart");
    let cfg = Config::default();

    let mut n = Node::new(
        1,
        peers_of(1, 3),
        cfg.clone(),
        FileStorage::open(&wal.0).unwrap(),
    );
    n.step(
        2,
        Message::AppendEntries {
            term: 3,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![e(3, b"a"), e(3, b"b")],
            leader_commit: 0,
            round: 0,
        },
    );
    assert_eq!(
        only_reply(&mut n),
        Message::AppendEntriesReply {
            term: 3,
            success: true,
            round: 0,
            match_index: 2,
            hint: 0
        }
    );
    drop(n.into_storage()); // process dies, file handle closed

    let mut n = Node::new(
        1,
        peers_of(1, 3),
        cfg.clone(),
        FileStorage::open(&wal.0).unwrap(),
    );
    assert_eq!(n.term(), 3);
    assert_eq!(n.log(), &[e(3, b"a"), e(3, b"b")]);

    // A candidate with an empty log is behind what we acknowledged: refuse.
    n.step(3, vote_request(4));
    assert_eq!(
        only_reply(&mut n),
        Message::RequestVoteReply {
            term: 4,
            granted: false
        }
    );
    assert_eq!(n.term(), 4);
    drop(n.into_storage());

    // And the new term (4) must itself have been saved.
    let n = Node::new(1, peers_of(1, 3), cfg, FileStorage::open(&wal.0).unwrap());
    assert_eq!(n.term(), 4);
    assert_eq!(n.log().len(), 2);
}

/// The strongest check on the "persist at the end of every entry point" design:
/// after ANY operation, crashing and restarting must give back exactly the
/// persistent state the node had a moment earlier. A handler that changes
/// `term`, `voted_for` or the log without it being saved makes this fail.
fn crash_recovery_equivalence<S: Storage>(
    seed: u64,
    ops: usize,
    make: impl FnOnce() -> S,
    mut reopen: impl FnMut(S) -> S,
) {
    let cfg = Config {
        seed,
        ..Config::default()
    };
    let mut rng = common::Rng::new(seed);
    let mut n = Node::new(1, peers_of(1, 3), cfg.clone(), make());

    for i in 0..ops {
        let peer = 2 + rng.below(2);
        match rng.below(10) {
            0..=1 => n.tick(),
            2..=3 => {
                let term = 1 + rng.below(8);
                n.step(
                    peer,
                    Message::RequestVote {
                        term,
                        last_log_index: rng.below(n.last_index() + 2),
                        last_log_term: rng.below(term + 1),
                    },
                );
            }
            4..=5 => {
                let last = n.last_index();
                let prev = rng.below(last + 1);
                let prev_term = if prev == 0 || rng.below(10) == 0 {
                    rng.below(4)
                } else {
                    n.log()[(prev - 1) as usize].term
                };
                let msg_term = 1 + rng.below(8);
                let entries: Vec<Entry> = (0..rng.below(4))
                    .map(|_| Entry {
                        term: 1 + rng.below(msg_term),
                        data: vec![1 + rng.below(250) as u8],
                    })
                    .collect();
                n.step(
                    peer,
                    Message::AppendEntries {
                        term: msg_term,
                        prev_log_index: prev,
                        prev_log_term: prev_term,
                        entries,
                        // Kept at 0: garbage senders must not be able to "commit"
                        // entries that a later garbage sender then truncates.
                        leader_commit: 0,
                        round: 0,
                    },
                );
            }
            6 => {
                let term = 1 + rng.below(9);
                n.step(
                    peer,
                    Message::PreVote {
                        term,
                        last_log_index: rng.below(n.last_index() + 2),
                        last_log_term: rng.below(term + 1),
                    },
                );
            }
            7 => n.step(
                peer,
                Message::RequestVoteReply {
                    term: rng.below(9),
                    granted: rng.below(2) == 0,
                },
            ),
            8 => n.step(
                peer,
                Message::PreVoteReply {
                    term: rng.below(9),
                    granted: rng.below(2) == 0,
                },
            ),
            _ => {
                let _ = n.propose(vec![1 + rng.below(250) as u8]);
            }
        }
        n.drain_messages();

        let before = (n.term(), n.voted_for(), n.log().to_vec());
        n = Node::new(1, peers_of(1, 3), cfg.clone(), reopen(n.into_storage()));
        let after = (n.term(), n.voted_for(), n.log().to_vec());
        assert_eq!(
            after, before,
            "persistent state lost across a restart after op #{i} (seed {seed})"
        );
    }
}

#[test]
fn state_survives_restart_after_every_operation_mem() {
    for seed in 1..=seeds(200) {
        crash_recovery_equivalence(seed, 300, MemStorage::new, |s| s);
    }
}

#[test]
fn state_survives_restart_after_every_operation_file() {
    for seed in 1..=seeds(15).min(40) {
        let wal = TempWal::new(&format!("equiv-{seed}"));
        let path = wal.0.clone();
        crash_recovery_equivalence(
            seed,
            150,
            || FileStorage::open(&path).unwrap(),
            |s| {
                drop(s);
                FileStorage::open(&path).unwrap()
            },
        );
    }
}

// ---------------------------------------------------------------------------
// Whole-cluster scenarios
// ---------------------------------------------------------------------------

#[test]
fn restarted_follower_comes_back_with_its_term_and_log() {
    let mut sim = Sim::new(3, 11);
    assert!(sim.run_until(300, |s| s.leader().is_some()));
    let leader = sim.leader().unwrap();
    let mut last = 0;
    for i in 1..=5u8 {
        last = sim.propose(vec![i]).unwrap().2;
        sim.run(3);
    }
    assert!(sim.run_until(300, |s| s.nodes.values().all(|n| n.commit_index() >= last)));

    let follower = (1..=3).find(|&id| id != leader).unwrap();
    let term = sim.nodes[&follower].term();
    let log: Vec<Entry> = sim.nodes[&follower].log().to_vec();

    sim.crash(follower);
    assert!(sim.is_down(follower));
    sim.run(40); // the cluster carries on without it
    sim.restart(follower);

    // Right after boot, before any tick: exactly what was on disk.
    assert_eq!(sim.nodes[&follower].term(), term);
    assert_eq!(sim.nodes[&follower].log(), &log[..]);
    assert_eq!(
        sim.nodes[&follower].role(),
        Role::Follower,
        "a restarted node always starts as a follower"
    );
    assert_eq!(
        sim.nodes[&follower].commit_index(),
        0,
        "commit index is volatile"
    );

    // It re-learns the commit point from the leader.
    assert!(sim.run_until(300, |s| s.nodes[&follower].commit_index() >= last));
}

#[test]
fn whole_cluster_crashes_and_recovers_without_losing_committed_entries() {
    let mut sim = Sim::new(5, 77);
    assert!(sim.run_until(300, |s| s.leader().is_some()));
    let mut last = 0;
    for i in 1..=10u8 {
        last = sim.propose(vec![i]).unwrap().2;
        sim.run(3);
    }
    assert!(sim.run_until(300, |s| s.nodes.values().all(|n| n.commit_index() >= last)));
    let committed: Vec<Entry> = sim.nodes[&1].log()[..last as usize].to_vec();

    for id in 1..=5 {
        sim.crash(id);
    }
    assert_eq!(sim.down_count(), 5);
    sim.run(50); // total outage
    sim.restart_all();

    assert!(sim.run_until(500, |s| s.leader().is_some()));
    let leader = sim.leader().unwrap();
    assert_eq!(
        &sim.nodes[&leader].log()[..last as usize],
        &committed[..],
        "new leader must hold every entry committed before the outage"
    );

    let after = sim.propose(b"after".to_vec()).unwrap().2;
    assert!(sim.run_until(500, |s| s.nodes.values().all(|n| n.commit_index() >= after)));
}

/// Random crashes and restarts on top of random partitions and message loss.
/// Invariants (election safety, state machine safety, leader completeness)
/// are asserted inside `Sim::tick`.
#[test]
fn fuzz_crash_restart_partitions_and_loss() {
    for seed in 1..=seeds(60) {
        let size = if seed % 2 == 0 { 5 } else { 3 };
        let mut sim = Sim::new(size, seed);
        sim.drop_pct = (seed % 4) * 5; // 0, 5, 10, 15 percent loss
        sim.crash_per_mille = 3; // plus crashes right after replying to a message
        let mut counter = 0u8;

        for _ in 0..3000 {
            let id = 1 + sim.rand_below(size);
            match sim.rand_below(100) {
                0 => sim.isolate(id),
                1..=2 => sim.heal(id),
                3..=4 => sim.crash(id),
                5..=9 => sim.restart(id),
                10..=30 => {
                    counter = counter.wrapping_add(1).max(1);
                    let _ = sim.propose(vec![counter]);
                }
                _ => {}
            }
            sim.tick();
        }

        // Liveness once the world is healthy again: that includes no more crashes.
        sim.drop_pct = 0;
        sim.crash_per_mille = 0;
        sim.heal_all();
        sim.restart_all();
        assert!(
            sim.run_until(1000, |s| s.leader().is_some()),
            "seed {seed}: no leader after recovery"
        );
        // The recovered cluster must be able to commit a NEW write. "The leader
        // accepted it" is not "it committed": if leadership changes before the
        // entry replicates, the new leader legitimately discards it (seed 648 of
        // a 1000-seed sweep did exactly this), so on a miss we simply try again.
        let mut converged = None;
        for _attempt in 0..10 {
            let mut proposed = None;
            for _ in 0..500 {
                if let Some((_, _, idx)) = sim.propose(vec![0xFF]) {
                    proposed = Some(idx);
                    break;
                }
                sim.tick();
            }
            let idx =
                proposed.unwrap_or_else(|| panic!("seed {seed}: leader never accepted a write"));
            if sim.run_until(300, |s| s.nodes.values().all(|n| n.commit_index() >= idx)) {
                converged = Some(idx);
                break;
            }
        }
        let idx = converged
            .unwrap_or_else(|| panic!("seed {seed}: recovered cluster could not commit a write"));
        let reference = sim.nodes[&1].log()[..idx as usize].to_vec();
        for n in sim.nodes.values() {
            assert_eq!(
                &n.log()[..idx as usize],
                &reference[..],
                "seed {seed}: node {} diverged",
                n.id()
            );
        }
    }
}

/// Aimed at lost votes. Three nodes, a healthy network, and a high chance that a
/// node dies the instant after it replies to a message. If a vote or an
/// acknowledged entry is not durable before the reply leaves, a restarted node
/// can vote twice in one term or forget an entry the leader counted toward a
/// majority.
#[test]
fn fuzz_crash_right_after_replying() {
    for seed in 1..=seeds(100) {
        let mut sim = Sim::new(3, seed);
        sim.crash_per_mille = 25;
        let mut counter = 0u8;

        for _ in 0..4000 {
            let id = 1 + sim.rand_below(3);
            match sim.rand_below(100) {
                0..=14 => sim.restart(id),
                15..=35 => {
                    counter = counter.wrapping_add(1).max(1);
                    let _ = sim.propose(vec![counter]);
                }
                _ => {}
            }
            sim.tick();
        }

        sim.crash_per_mille = 0;
        sim.restart_all();
        assert!(
            sim.run_until(1000, |s| s.leader().is_some()),
            "seed {seed}: no leader after recovery"
        );
        sim.run(200);
        assert!(
            sim.committed_len() > 0,
            "seed {seed}: nothing ever committed"
        );
    }
}
