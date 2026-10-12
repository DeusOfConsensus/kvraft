//! Deterministic cluster simulator.
//!
//! Same seed => same message delays, drops and elections => same run, so any
//! invariant violation found by fuzzing can be replayed exactly.
//!
//! Safety invariants are checked after every tick:
//!   * Election safety: at most one leader per term.
//!   * State machine safety: once an entry is committed anywhere, every node
//!     that ever commits that index holds the identical entry.

use crate::raft::{Config, Entry, Envelope, Node, NodeId, Role, Term};
use std::collections::{BTreeMap, BTreeSet};

pub struct Sim {
    pub nodes: BTreeMap<NodeId, Node>,
    pub now: u64,
    /// Percent (0-100) of messages silently dropped.
    pub drop_pct: u64,
    inflight: Vec<(u64, Envelope)>,
    isolated: BTreeSet<NodeId>,
    rng: u64,
    leaders_by_term: BTreeMap<Term, NodeId>,
    committed_history: Vec<Entry>,
}

impl Sim {
    pub fn new(n: u64, seed: u64) -> Self {
        let ids: Vec<NodeId> = (1..=n).collect();
        let nodes = ids
            .iter()
            .map(|&id| {
                let peers = ids.iter().copied().filter(|&p| p != id).collect();
                let cfg = Config {
                    seed,
                    ..Config::default()
                };
                (id, Node::new(id, peers, cfg))
            })
            .collect();
        Sim {
            nodes,
            now: 0,
            drop_pct: 0,
            inflight: Vec::new(),
            isolated: BTreeSet::new(),
            rng: seed.wrapping_mul(0x2545_F491_4F6C_DD1D) | 1,
            leaders_by_term: BTreeMap::new(),
            committed_history: Vec::new(),
        }
    }

    fn rand(&mut self) -> u64 {
        let mut x = self.rng;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng = x;
        x
    }

    pub fn rand_below(&mut self, n: u64) -> u64 {
        self.rand() % n
    }

    // ---------- fault injection ----------

    pub fn isolate(&mut self, id: NodeId) {
        self.isolated.insert(id);
    }
    pub fn heal(&mut self, id: NodeId) {
        self.isolated.remove(&id);
    }
    pub fn heal_all(&mut self) {
        self.isolated.clear();
    }
    pub fn is_isolated(&self, id: NodeId) -> bool {
        self.isolated.contains(&id)
    }

    fn blocked(&self, a: NodeId, b: NodeId) -> bool {
        self.isolated.contains(&a) || self.isolated.contains(&b)
    }

    // ---------- driving time ----------

    pub fn tick(&mut self) {
        self.now += 1;

        // Deliver everything that is due, in send order.
        let now = self.now;
        let (due, later): (Vec<_>, Vec<_>) =
            self.inflight.drain(..).partition(|(at, _)| *at <= now);
        self.inflight = later;
        for (_, env) in due {
            if self.blocked(env.from, env.to) {
                continue; // partition started while the message was in flight
            }
            if let Some(node) = self.nodes.get_mut(&env.to) {
                node.step(env.from, env.msg);
            }
        }

        for node in self.nodes.values_mut() {
            node.tick();
        }

        // Collect outgoing traffic; apply loss and a random 1-3 tick latency.
        let ids: Vec<NodeId> = self.nodes.keys().copied().collect();
        for id in ids {
            let out = self.nodes.get_mut(&id).unwrap().drain_messages();
            for env in out {
                if self.blocked(env.from, env.to) || self.rand_below(100) < self.drop_pct {
                    continue;
                }
                let delay = 1 + self.rand_below(3);
                self.inflight.push((self.now + delay, env));
            }
        }

        self.check_invariants();
    }

    pub fn run(&mut self, ticks: u64) {
        for _ in 0..ticks {
            self.tick();
        }
    }

    /// Run until `cond` holds or `max_ticks` elapse. Returns whether it held.
    pub fn run_until(&mut self, max_ticks: u64, mut cond: impl FnMut(&Sim) -> bool) -> bool {
        for _ in 0..max_ticks {
            if cond(self) {
                return true;
            }
            self.tick();
        }
        cond(self)
    }

    // ---------- queries / client ops ----------

    /// The reachable leader with the highest term, if any.
    pub fn leader(&self) -> Option<NodeId> {
        self.nodes
            .values()
            .filter(|n| n.role() == Role::Leader && !self.isolated.contains(&n.id()))
            .max_by_key(|n| n.term())
            .map(|n| n.id())
    }

    /// Propose via whichever reachable node currently believes it is leader.
    pub fn propose(&mut self, data: Vec<u8>) -> Option<(NodeId, Term, u64)> {
        let leader = self.leader()?;
        let (term, index) = self.nodes.get_mut(&leader).unwrap().propose(data).ok()?;
        Some((leader, term, index))
    }

    // ---------- invariants ----------

    fn check_invariants(&mut self) {
        // Election safety.
        for n in self.nodes.values() {
            if n.role() == Role::Leader {
                let prev = self.leaders_by_term.entry(n.term()).or_insert(n.id());
                assert_eq!(
                    *prev,
                    n.id(),
                    "ELECTION SAFETY VIOLATED at t={}: nodes {} and {} both lead term {}",
                    self.now,
                    prev,
                    n.id(),
                    n.term()
                );
            }
        }
        // State machine safety / committed-entry stability.
        for n in self.nodes.values() {
            let commit = n.commit_index() as usize;
            for i in 0..commit {
                let e = &n.log()[i];
                if i < self.committed_history.len() {
                    assert_eq!(
                        &self.committed_history[i],
                        e,
                        "STATE MACHINE SAFETY VIOLATED at t={}: node {} disagrees at committed index {}",
                        self.now,
                        n.id(),
                        i + 1
                    );
                } else {
                    self.committed_history.push(e.clone());
                }
            }
        }
    }

    pub fn committed_len(&self) -> usize {
        self.committed_history.len()
    }
}
