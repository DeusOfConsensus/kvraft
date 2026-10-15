//! The headline test: clients hammer a cluster that is being partitioned,
//! crashed and starved of packets, and the recorded history must still be
//! linearizable, with every replica ending in the same state.

mod common;

use kvraft::kvsim::KvCluster;
use kvraft::linearizability::Verdict;
use kvraft::raft::Config;

fn chaos(nodes: u64, seed: u64, cfg_variant: u64) {
    let cfg = Config {
        seed,
        // Without CheckQuorum a cut-off leader lingers for a very long time,
        // which is the hardest case for reads.
        check_quorum: cfg_variant != 1,
        pre_vote: cfg_variant != 2,
        ..Config::default()
    };
    let mut c = KvCluster::with_config(nodes, cfg);
    c.drop_pct = 8;
    c.crash_per_mille = 3;
    for _ in 0..5 {
        let ops = c.random_ops(30, 3);
        c.add_client(ops, None);
    }
    let minority = (nodes as usize - 1) / 2;

    for _ in 0..1_500 {
        match c.rand_below(60) {
            0 => {
                let id = 1 + c.rand_below(nodes);
                c.isolate(id);
            }
            1 | 2 => {
                let id = 1 + c.rand_below(nodes);
                c.heal(id);
            }
            3 if c.down_count() < minority => {
                let id = 1 + c.rand_below(nodes);
                c.crash(id);
            }
            4 | 5 => {
                let id = 1 + c.rand_below(nodes);
                c.restart(id);
            }
            _ => {}
        }
        // Random crashes must not take a majority down for good.
        if c.down_count() > minority {
            let id = 1 + c.rand_below(nodes);
            c.restart(id);
        }
        c.tick();
    }

    // Stop the chaos; every client must now be able to finish.
    c.crash_per_mille = 0;
    c.drop_pct = 0;
    c.heal_all();
    c.restart_all();
    assert!(
        c.run_until(20_000, KvCluster::clients_done),
        "seed {seed} nodes {nodes} variant {cfg_variant}: clients stuck after healing: {:?}",
        c.clients
            .iter()
            .map(|cl| (cl.id, cl.completed))
            .collect::<Vec<_>>()
    );
    assert!(
        c.run_until(2_000, KvCluster::converged),
        "seed {seed}: replicas never converged"
    );
    match c.verdict() {
        Verdict::Linearizable => {}
        Verdict::Violation { key, detail } => panic!(
            "seed {seed} nodes {nodes} variant {cfg_variant}: NOT LINEARIZABLE on key {:?}\n{detail}",
            String::from_utf8_lossy(&key)
        ),
        Verdict::Inconclusive { key } => panic!(
            "seed {seed}: checker gave up on key {:?}",
            String::from_utf8_lossy(&key)
        ),
    }
}

#[test]
fn three_nodes_under_chaos_stay_linearizable() {
    for seed in 1..=common::seeds(60) {
        chaos(3, seed, seed % 3);
    }
}

#[test]
fn five_nodes_under_chaos_stay_linearizable() {
    for seed in 1..=common::seeds(40) {
        chaos(5, 1000 + seed, seed % 3);
    }
}
