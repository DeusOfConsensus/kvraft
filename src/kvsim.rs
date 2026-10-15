//! Deterministic simulation of a whole key-value service: replicas, a lossy
//! network, crashes, partitions, and clients that retry, recording a history
//! the linearizability checker can judge.
//!
//! Same seed => same run. Faults:
//!   * server<->server and client<->server packets are delayed (1-3 ticks) and
//!     dropped (`drop_pct`);
//!   * `isolate(id)` cuts a server off from the OTHER SERVERS only. Clients can
//!     still reach it, which is exactly the "stale leader" situation;
//!   * `cut_clients(id)` additionally hides a server from clients;
//!   * crash/restart keeps the disk, drops everything else.
//!
//! Clients follow the rules the state machine depends on: one outstanding
//! request, increasing `seq`, the same `(client, seq)` on every retry.

use crate::kv::{Op, Resp};
use crate::linearizability::{HistOp, Verdict, check};
use crate::raft::{Config, Envelope, NodeId, Role, Term};
use crate::server::{KvServer, Outcome, Reply};
use crate::storage::MemStorage;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

const CLIENT_TIMEOUT: u64 = 20;

enum Packet {
    Raft(Envelope),
    Request {
        to: NodeId,
        client: u64,
        seq: u64,
        op: Op,
    },
    Reply(Reply),
}

struct Current {
    seq: u64,
    op: Op,
    hist: usize,
    next_send: u64,
    /// True after a send that is waiting on a timeout: the next send moves on
    /// to another server. False after an explicit redirect.
    rotate: bool,
}

pub struct Client {
    pub id: u64,
    seq: u64,
    todo: VecDeque<Op>,
    cur: Option<Current>,
    /// Always talk to this server, whatever it says.
    pub pinned: Option<NodeId>,
    target: NodeId,
    pub completed: usize,
}

impl Client {
    pub fn is_done(&self) -> bool {
        self.cur.is_none() && self.todo.is_empty()
    }
}

pub struct KvCluster {
    pub now: u64,
    pub drop_pct: u64,
    /// Chance in 1/1000 that a server crashes right after handling a packet.
    pub crash_per_mille: u64,
    pub clients: Vec<Client>,
    pub history: Vec<HistOp>,
    pub servers: BTreeMap<NodeId, KvServer<MemStorage>>,
    cfg: Config,
    ids: Vec<NodeId>,
    down: BTreeMap<NodeId, MemStorage>,
    restarts: u64,
    net: Vec<(u64, Packet)>,
    isolated: BTreeSet<NodeId>,
    hidden_from_clients: BTreeSet<NodeId>,
    rng: u64,
    uniq: u64,
    recent: BTreeMap<Vec<u8>, Vec<u8>>,
    leaders_by_term: BTreeMap<Term, NodeId>,
}

impl KvCluster {
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
        let servers = ids
            .iter()
            .map(|&id| {
                let peers = ids.iter().copied().filter(|&p| p != id).collect();
                (id, KvServer::new(id, peers, cfg.clone(), MemStorage::new()))
            })
            .collect();
        KvCluster {
            now: 0,
            drop_pct: 0,
            crash_per_mille: 0,
            clients: Vec::new(),
            history: Vec::new(),
            servers,
            rng: cfg.seed.wrapping_mul(0x2545_F491_4F6C_DD1D) | 1,
            cfg,
            ids,
            down: BTreeMap::new(),
            restarts: 0,
            net: Vec::new(),
            isolated: BTreeSet::new(),
            hidden_from_clients: BTreeSet::new(),
            uniq: 0,
            recent: BTreeMap::new(),
            leaders_by_term: BTreeMap::new(),
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
        self.hidden_from_clients.clear();
    }
    pub fn cut_clients(&mut self, id: NodeId) {
        self.hidden_from_clients.insert(id);
    }

    pub fn crash(&mut self, id: NodeId) {
        if let Some(s) = self.servers.remove(&id) {
            self.down.insert(id, s.into_storage());
        }
    }

    pub fn restart(&mut self, id: NodeId) {
        if let Some(storage) = self.down.remove(&id) {
            self.restarts += 1;
            let peers = self.ids.iter().copied().filter(|&p| p != id).collect();
            let cfg = Config {
                seed: self.cfg.seed.wrapping_add(self.restarts.wrapping_mul(7919)),
                ..self.cfg.clone()
            };
            self.servers
                .insert(id, KvServer::new(id, peers, cfg, storage));
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
    pub fn ids(&self) -> &[NodeId] {
        &self.ids
    }

    /// The server-reachable leader with the highest term, if any.
    pub fn leader(&self) -> Option<NodeId> {
        self.servers
            .values()
            .filter(|s| s.node().role() == Role::Leader && !self.isolated.contains(&s.id()))
            .max_by_key(|s| s.node().term())
            .map(|s| s.id())
    }

    // ---------- clients ----------

    /// Add a client that will run `ops` one at a time. Returns its index.
    pub fn add_client(&mut self, ops: Vec<Op>, pinned: Option<NodeId>) -> usize {
        let id = self.clients.len() as u64 + 1;
        let target = pinned.unwrap_or(self.ids[(id as usize) % self.ids.len()]);
        self.clients.push(Client {
            id,
            seq: 0,
            todo: ops.into(),
            cur: None,
            pinned,
            target,
            completed: 0,
        });
        self.clients.len() - 1
    }

    /// `n` random operations over keys `k0..k{keys}`. Written values are unique,
    /// so every anomaly is attributable.
    pub fn random_ops(&mut self, n: usize, keys: u64) -> Vec<Op> {
        let mut ops = Vec::new();
        for _ in 0..n {
            let key = format!("k{}", self.rand_below(keys)).into_bytes();
            self.uniq += 1;
            let val = format!("<{}>", self.uniq).into_bytes();
            let op = match self.rand_below(10) {
                0..=2 => Op::Get { key: key.clone() },
                3..=4 => Op::Put {
                    key: key.clone(),
                    value: val.clone(),
                },
                5 => Op::Delete { key: key.clone() },
                6..=7 => Op::Append {
                    key: key.clone(),
                    value: val.clone(),
                },
                _ => {
                    let expected = match self.rand_below(3) {
                        0 => None,
                        _ => self.recent.get(&key).cloned(),
                    };
                    Op::Cas {
                        key: key.clone(),
                        expected,
                        new: val.clone(),
                    }
                }
            };
            if let Op::Put { value, .. } = &op {
                self.recent.insert(key, value.clone());
            }
            ops.push(op);
        }
        ops
    }

    pub fn clients_done(&self) -> bool {
        self.clients.iter().all(Client::is_done)
    }

    pub fn verdict(&self) -> Verdict {
        check(&self.history)
    }

    // ---------- driving time ----------

    fn blocked(&self, a: NodeId, b: NodeId) -> bool {
        self.isolated.contains(&a) || self.isolated.contains(&b)
    }

    fn push(&mut self, p: Packet) {
        let delay = 1 + self.rand_below(3);
        self.net.push((self.now + delay, p));
    }

    /// Put everything server `id` wants to say onto the network.
    fn flush(&mut self, id: NodeId) {
        let Some(s) = self.servers.get_mut(&id) else {
            return;
        };
        let msgs = s.drain_messages();
        let replies = s.drain_replies();
        for env in msgs {
            if self.blocked(env.from, env.to) || self.rand_below(100) < self.drop_pct {
                continue;
            }
            self.push(Packet::Raft(env));
        }
        for r in replies {
            if self.hidden_from_clients.contains(&id) || self.rand_below(100) < self.drop_pct {
                continue;
            }
            self.push(Packet::Reply(r));
        }
    }

    fn maybe_crash(&mut self, id: NodeId) {
        if self.crash_per_mille > 0 && self.rand_below(1000) < self.crash_per_mille {
            self.crash(id);
        }
    }

    pub fn tick(&mut self) {
        self.now += 1;
        let now = self.now;
        let (due, later): (Vec<_>, Vec<_>) = self.net.drain(..).partition(|(at, _)| *at <= now);
        self.net = later;
        for (_, p) in due {
            match p {
                Packet::Raft(env) => {
                    if self.blocked(env.from, env.to) {
                        continue;
                    }
                    let to = env.to;
                    if let Some(s) = self.servers.get_mut(&to) {
                        s.step(env.from, env.msg);
                        self.flush(to);
                        self.maybe_crash(to);
                    }
                }
                Packet::Request {
                    to,
                    client,
                    seq,
                    op,
                } => {
                    if self.hidden_from_clients.contains(&to) {
                        continue;
                    }
                    if let Some(s) = self.servers.get_mut(&to) {
                        s.submit(client, seq, op);
                        self.flush(to);
                        self.maybe_crash(to);
                    }
                }
                Packet::Reply(r) => self.client_reply(r),
            }
        }

        for s in self.servers.values_mut() {
            s.tick();
        }
        let ids: Vec<NodeId> = self.servers.keys().copied().collect();
        for id in ids {
            self.flush(id);
        }

        for ci in 0..self.clients.len() {
            self.client_tick(ci);
        }

        self.check_invariants();
    }

    pub fn run(&mut self, ticks: u64) {
        for _ in 0..ticks {
            self.tick();
        }
    }

    pub fn run_until(&mut self, max_ticks: u64, mut cond: impl FnMut(&KvCluster) -> bool) -> bool {
        for _ in 0..max_ticks {
            if cond(self) {
                return true;
            }
            self.tick();
        }
        cond(self)
    }

    // ---------- client behaviour ----------

    fn client_tick(&mut self, ci: usize) {
        if self.clients[ci].cur.is_none() && !self.clients[ci].todo.is_empty() {
            // Think time of a few ticks between operations.
            if self.rand_below(4) == 0 {
                let c = &mut self.clients[ci];
                let op = c.todo.pop_front().unwrap();
                c.seq += 1;
                let seq = c.seq;
                let client = c.id;
                c.cur = Some(Current {
                    seq,
                    op: op.clone(),
                    hist: self.history.len(),
                    next_send: self.now,
                    rotate: false,
                });
                self.history.push(HistOp {
                    client,
                    op,
                    invoke: self.now,
                    ret: None,
                    resp: None,
                });
            }
        }

        let ids_len = self.ids.len();
        let now = self.now;
        let c = &mut self.clients[ci];
        let Some(cur) = c.cur.as_mut() else { return };
        if now < cur.next_send {
            return;
        }
        if cur.rotate && c.pinned.is_none() {
            let pos = self.ids.iter().position(|&i| i == c.target).unwrap_or(0);
            c.target = self.ids[(pos + 1) % ids_len];
        }
        cur.rotate = true;
        cur.next_send = now + CLIENT_TIMEOUT;
        let (to, client, seq, op) = (c.target, c.id, cur.seq, cur.op.clone());

        if self.hidden_from_clients.contains(&to) || self.rand_below(100) < self.drop_pct {
            return;
        }
        self.push(Packet::Request {
            to,
            client,
            seq,
            op,
        });
    }

    fn client_reply(&mut self, r: Reply) {
        let Some(c) = self.clients.iter_mut().find(|c| c.id == r.client) else {
            return;
        };
        let Some(cur) = c.cur.as_mut() else { return };
        if cur.seq != r.seq {
            return; // an answer to an earlier attempt of an earlier request
        }
        match r.outcome {
            Outcome::Done(resp) => {
                assert_ne!(
                    resp,
                    Resp::Stale,
                    "client {} was told its current request is stale",
                    c.id
                );
                let h = &mut self.history[cur.hist];
                h.ret = Some(self.now);
                h.resp = Some(resp);
                c.cur = None;
                c.completed += 1;
            }
            Outcome::NotLeader(hint) => {
                if c.pinned.is_none()
                    && let Some(h) = hint.filter(|h| self.ids.contains(h))
                {
                    c.target = h;
                    cur.rotate = false;
                }
                cur.next_send = self.now + 1;
            }
            Outcome::Retry => cur.next_send = self.now + 3,
        }
    }

    // ---------- invariants ----------

    fn check_invariants(&mut self) {
        for s in self.servers.values() {
            let n = s.node();
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
        // Replicas that applied the same prefix of the log must hold identical state.
        if self.now.is_multiple_of(5) {
            self.check_replicas_agree();
        }
    }

    /// Servers at the same `last_applied` must have the same digest.
    pub fn check_replicas_agree(&self) {
        let mut seen: BTreeMap<u64, (NodeId, u64)> = BTreeMap::new();
        for s in self.servers.values() {
            let applied = s.node().last_applied();
            let d = s.kv().digest();
            let (other, od) = *seen.entry(applied).or_insert((s.id(), d));
            assert_eq!(
                od,
                d,
                "REPLICA DIVERGENCE at t={}: nodes {} and {} applied {} entries but hold different state",
                self.now,
                other,
                s.id(),
                applied
            );
        }
    }

    /// True when every live server has applied everything committed and all
    /// agree on the state.
    pub fn converged(&self) -> bool {
        let mut digests = self.servers.values().map(|s| {
            (
                s.node().last_applied() == s.node().commit_index(),
                s.node().commit_index(),
                s.kv().digest(),
            )
        });
        let Some(first) = digests.next() else {
            return true;
        };
        first.0 && digests.all(|d| d == first)
    }
}
