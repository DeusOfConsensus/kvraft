//! Leader-churn fuzz: repeatedly cut off the *current leader* shortly after it
//! accepts writes, so entries end up partially replicated, then bring nodes
//! back in varying orders. It is a stress test for election and replication
//! safety under heavy leader turnover.
//!
//! KNOWN LIMITATION (verified by mutation testing): this does NOT detect the
//! Figure 8 bug from the Raft paper, i.e. a leader committing an old-term entry
//! just because a majority holds it. With that check deleted (and even with the
//! leader no-op also deleted), 1500+ seeds still passed. Random fuzzing cannot
//! steer the exact election sequence that bug needs. Covering it requires a
//! scripted scenario with controlled elections

use kvraft::sim::Sim;

#[test]
fn fuzz_leader_churn_never_loses_committed_entries() {
    // `KVRAFT_FUZZ_SEEDS=2000 cargo test --release` for a much heavier sweep.
    let seeds: u64 = std::env::var("KVRAFT_FUZZ_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);
    let size = 5u64;

    for seed in 1..=seeds {
        let mut sim = Sim::new(size, seed);
        let mut counter = 0u8;
        for _ in 0..4000 {
            let isolated = (1..=size).filter(|&i| sim.is_isolated(i)).count();
            match sim.rand_below(100) {
                // Cut off whoever currently leads (keep a majority reachable).
                0..=3 if isolated < 2 => {
                    if let Some(l) = sim.leader() {
                        sim.isolate(l);
                    }
                }
                // Bring a random node back.
                4..=8 => {
                    let id = 1 + sim.rand_below(size);
                    sim.heal(id);
                }
                // Write to the current leader.
                9..=40 => {
                    counter = counter.wrapping_add(1).max(1);
                    let _ = sim.propose(vec![counter]);
                }
                _ => {}
            }
            sim.tick(); // invariants are asserted inside tick
        }

        sim.heal_all();
        assert!(
            sim.run_until(500, |s| s.leader().is_some()),
            "seed {seed}: no leader after healing"
        );
    }
}
