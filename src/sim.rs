//! Deterministic cluster simulator.
//!
//! Same seed => same message delays, drops, crashes and elections => same run,
//! so any invariant violation found by fuzzing can be replayed exactly.
//!
//! Faults it can inject: message delay and loss, network isolation of a node,
//! and crash/restart (the `Node` is dropped, its `MemStorage` survives, and a
//! fresh `Node` is rebuilt from it).
//!
//! Safety invariants are checked after every tick:
//!   * Election safety: at most one leader per term.
//!   * State machine safety: once an entry is committed anywhere, every node
//!     that ever commits that index holds the identical entry.
//!   * Leader completeness: a leader of a later term holds every entry that was
//!     committed in an earlier term. This is the invariant a lost vote or a
//!     lost log entry after a crash breaks first, at election time, long before
//!     the damage would show up as divergent state machines.

use crate::raft::{Config, Entry, Envelope, Node, NodeId, Role, Term};
use crate::storage::MemStorage;
use std::collections::{BTreeMap, BTreeSet};

pub struct Sim {
    pub nodes: BTreeMap<NodeId, Node<MemStorage>>,
    pub now: u64,
    /// Percent (0-100) of messages silently dropped.
    pub drop_pct: u64,
    /// Chance, in 1/1000, that a node crashes immediately after handling a
    /// message, once its replies have been sent. 0 disables these crashes.
    pub crash_per_mille: u64,
    cfg: Config,
    ids: Vec<NodeId>,
    /// Crashed nodes: their disks, waiting for a restart.
    down: BTreeMap<NodeId, MemStorage>,
    restarts: u64,
    inflight: Vec<(u64, Envelope)>,
    isolated: BTreeSet<NodeId>,
    rng: u64,
    leaders_by_term: BTreeMap<Term, NodeId>,
    committed_history: Vec<Entry>,
    /// For each committed index, an upper bound on the term it was committed in
    /// (the highest term any live node had when we first saw it committed).
    commit_term_bound: Vec<Term>,
}

impl Sim {
    /// A cluster of `n` nodes with the default config (PreVote and CheckQuorum on).
    pub fn new(n: u64, seed: u64) -> Self {
        Self::with_config(
            n,
            Config {
                seed,
                ..Config::default()
            },
        )
    }

    pub fn with_config(n: u64, cfg: Config) -> Self {
        let ids: Vec<NodeId> = (1..=n).collect();
        let nodes = ids
            .iter()
            .map(|&id| {
                let peers = ids.iter().copied().filter(|&p| p != id).collect();
                (id, Node::new(id, peers, cfg.clone(), MemStorage::new()))
            })
            .collect();
        Sim {
            nodes,
            now: 0,
            drop_pct: 0,
            crash_per_mille: 0,
            rng: cfg.seed.wrapping_mul(0x2545_F491_4F6C_DD1D) | 1,
            cfg,
            ids,
            down: BTreeMap::new(),
            restarts: 0,
            inflight: Vec::new(),
            isolated: BTreeSet::new(),
            leaders_by_term: BTreeMap::new(),
            committed_history: Vec::new(),
            commit_term_bound: Vec::new(),
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

    /// Kill the process: the `Node` (all volatile state) is dropped, the disk is kept.
    pub fn crash(&mut self, id: NodeId) {
        if let Some(node) = self.nodes.remove(&id) {
            self.down.insert(id, node.into_storage());
        }
    }

    /// Boot a crashed node from whatever its disk holds.
    pub fn restart(&mut self, id: NodeId) {
        if let Some(storage) = self.down.remove(&id) {
            self.restarts += 1;
            let peers = self.ids.iter().copied().filter(|&p| p != id).collect();
            // A different timer seed per boot, as a real process would have.
            let cfg = Config {
                seed: self.cfg.seed.wrapping_add(self.restarts.wrapping_mul(7919)),
                ..self.cfg.clone()
            };
            self.nodes.insert(id, Node::new(id, peers, cfg, storage));
        }
    }

    pub fn restart_all(&mut self) {
        let ids: Vec<NodeId> = self.down.keys().copied().collect();
        for id in ids {
            self.restart(id);
        }
    }

    pub fn is_down(&self, id: NodeId) -> bool {
        self.down.contains_key(&id)
    }

    pub fn down_count(&self) -> usize {
        self.down.len()
    }

    fn blocked(&self, a: NodeId, b: NodeId) -> bool {
        self.isolated.contains(&a) || self.isolated.contains(&b)
    }

    // ---------- driving time ----------

    pub fn tick(&mut self) {
        self.now += 1;

        // Deliver everything that is due, in send order. Messages for a crashed
        // node vanish, like packets sent to a dead machine.
        let now = self.now;
        let (due, later): (Vec<_>, Vec<_>) =
            self.inflight.drain(..).partition(|(at, _)| *at <= now);
        self.inflight = later;
        for (_, env) in due {
            if self.blocked(env.from, env.to) {
                continue; // partition started while the message was in flight
            }
            let to = env.to;
            if let Some(node) = self.nodes.get_mut(&to) {
                node.step(env.from, env.msg);
                // A real driver sends the replies as soon as `step` returns, so
                // do the same. This is also the most dangerous moment to crash:
                // the replies are already on the wire.
                self.flush_outbox(to);
                if self.crash_per_mille > 0 && self.rand_below(1000) < self.crash_per_mille {
                    self.crash(to);
                }
            }
        }

        for node in self.nodes.values_mut() {
            node.tick();
        }
        let ids: Vec<NodeId> = self.nodes.keys().copied().collect();
        for id in ids {
            self.flush_outbox(id);
        }

        self.check_invariants();
    }

    /// Put everything `id` wants to send onto the network, applying loss and a
    /// random 1-3 tick latency.
    fn flush_outbox(&mut self, id: NodeId) {
        let out = match self.nodes.get_mut(&id) {
            Some(node) => node.drain_messages(),
            None => return,
        };
        for env in out {
            if self.blocked(env.from, env.to) || self.rand_below(100) < self.drop_pct {
                continue;
            }
            let delay = 1 + self.rand_below(3);
            self.inflight.push((self.now + delay, env));
        }
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
        let max_term = self.nodes.values().map(|n| n.term()).max().unwrap_or(0);
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
                    self.commit_term_bound.push(max_term);
                }
            }
        }

        // Leader completeness. A leader at term T must hold every entry that
        // was committed in a term below T. We only know an upper bound on each
        // entry's commit term, so we only check where bound < T. That can miss
        // a violation but can never raise a false alarm: a stale isolated
        // leader of an old term is correctly exempt.
        for n in self.nodes.values() {
            if n.role() != Role::Leader {
                continue;
            }
            for (i, want) in self.committed_history.iter().enumerate() {
                if self.commit_term_bound[i] < n.term() {
                    assert_eq!(
                        n.log().get(i),
                        Some(want),
                        "LEADER COMPLETENESS VIOLATED at t={}: node {} became leader of term {} \
                         without committed entry at index {}",
                        self.now,
                        n.id(),
                        n.term(),
                        i + 1
                    );
                }
            }
        }
    }

    pub fn committed_len(&self) -> usize {
        self.committed_history.len()
    }
}
