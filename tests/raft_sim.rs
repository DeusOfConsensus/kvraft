use kvraft::raft::Role;
use kvraft::sim::Sim;

fn leaders(sim: &Sim) -> Vec<u64> {
    sim.nodes
        .values()
        .filter(|n| n.role() == Role::Leader)
        .map(|n| n.id())
        .collect()
}

#[test]
fn elects_exactly_one_leader() {
    for seed in 1..=20 {
        let mut sim = Sim::new(3, seed);
        assert!(
            sim.run_until(200, |s| s.leader().is_some()),
            "seed {seed}: no leader elected"
        );
        sim.run(50);
        assert_eq!(
            leaders(&sim).len(),
            1,
            "seed {seed}: expected one stable leader"
        );
    }
}

#[test]
fn single_node_cluster_self_elects_and_commits() {
    let mut sim = Sim::new(1, 7);
    assert!(sim.run_until(50, |s| s.leader().is_some()));
    let (_, _, idx) = sim.propose(b"x".to_vec()).unwrap();
    sim.run(3);
    assert!(sim.nodes[&1].commit_index() >= idx);
}

#[test]
fn replicates_and_commits_on_all_nodes() {
    let mut sim = Sim::new(5, 42);
    assert!(sim.run_until(200, |s| s.leader().is_some()));
    let mut last = 0;
    for i in 0..20u8 {
        let (_, _, idx) = sim.propose(vec![i + 1]).expect("leader should accept");
        last = idx;
        sim.run(2);
    }
    assert!(
        sim.run_until(200, |s| s.nodes.values().all(|n| n.commit_index() >= last)),
        "all nodes should commit all 20 entries"
    );
    let reference: Vec<_> = sim.nodes[&1].log()[..last as usize].to_vec();
    for n in sim.nodes.values() {
        assert_eq!(
            &n.log()[..last as usize],
            &reference[..],
            "node {} diverged",
            n.id()
        );
    }
}

#[test]
fn survives_leader_failure_and_old_leader_catches_up() {
    let mut sim = Sim::new(5, 99);
    assert!(sim.run_until(200, |s| s.leader().is_some()));
    let old = sim.leader().unwrap();

    sim.propose(b"before".to_vec()).unwrap();
    sim.run(20);

    // Cut the leader off. It keeps believing it leads and accepts a write that can never commit.
    sim.isolate(old);
    sim.nodes
        .get_mut(&old)
        .unwrap()
        .propose(b"doomed".to_vec())
        .unwrap();

    // The majority side elects a new leader and makes progress.
    assert!(sim.run_until(300, |s| s.leader().is_some_and(|l| l != old)));
    let new = sim.leader().unwrap();
    let (_, _, idx) = sim.propose(b"after".to_vec()).unwrap();
    assert!(sim.run_until(100, |s| s.nodes[&new].commit_index() >= idx));

    // Heal: the old leader must step down, drop "doomed", and converge.
    sim.heal(old);
    assert!(sim.run_until(300, |s| s.nodes.values().all(|n| n.commit_index() >= idx)));
    let datas: Vec<_> = sim.nodes[&old]
        .log()
        .iter()
        .map(|e| e.data.clone())
        .collect();
    assert!(
        !datas.contains(&b"doomed".to_vec()),
        "uncommitted entry must be overwritten"
    );
    assert!(datas.contains(&b"after".to_vec()));
    assert_ne!(sim.nodes[&old].role(), Role::Leader);
}

#[test]
fn minority_partition_cannot_commit() {
    let mut sim = Sim::new(5, 5);
    assert!(sim.run_until(200, |s| s.leader().is_some()));
    let l = sim.leader().unwrap();
    // Isolate the leader plus nobody else: it is a minority of one.
    sim.isolate(l);
    let before = sim.nodes[&l].commit_index();
    sim.nodes
        .get_mut(&l)
        .unwrap()
        .propose(b"never".to_vec())
        .unwrap();
    sim.run(100);
    assert_eq!(
        sim.nodes[&l].commit_index(),
        before,
        "isolated leader must not commit"
    );
}

/// Randomized partitions, message loss and writes across many seeds.
/// Invariants are asserted inside `Sim::tick`; a failing seed replays exactly.
#[test]
fn fuzz_safety_under_partitions_and_loss() {
    // `KVRAFT_FUZZ_SEEDS=2000 cargo test --release` for a much heavier sweep.
    let seeds: u64 = std::env::var("KVRAFT_FUZZ_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60);
    for seed in 1..=seeds {
        let size = if seed % 2 == 0 { 5 } else { 3 };
        let mut sim = Sim::new(size, seed);
        sim.drop_pct = (seed % 4) * 5; // 0, 5, 10, 15 percent loss
        let mut counter = 0u8;
        for _ in 0..3000 {
            match sim.rand_below(100) {
                0 => {
                    let id = 1 + sim.rand_below(size);
                    sim.isolate(id);
                }
                1..=2 => {
                    let id = 1 + sim.rand_below(size);
                    sim.heal(id);
                }
                3..=20 => {
                    counter = counter.wrapping_add(1).max(1);
                    let _ = sim.propose(vec![counter]);
                }
                _ => {}
            }
            sim.tick();
        }
        // Liveness after the network recovers.
        sim.drop_pct = 0;
        sim.heal_all();
        assert!(
            sim.run_until(500, |s| s.leader().is_some()),
            "seed {seed}: no leader after healing"
        );
        sim.run(200);
        let leader = sim.leader().unwrap();
        let lc = sim.nodes[&leader].commit_index();
        for n in sim.nodes.values() {
            assert_eq!(
                n.commit_index(),
                lc,
                "seed {seed}: node {} did not converge",
                n.id()
            );
        }
    }
}
