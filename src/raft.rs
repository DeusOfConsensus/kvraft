//! Pure Raft consensus core.
//!
//! No sockets, no threads, no wall clock. The caller drives the node with:
//!   * `tick()`            - one unit of logical time
//!   * `step(from, msg)`   - deliver an incoming message
//!   * `propose(data)`     - ask the leader to replicate a command
//!
//! and collects results with `drain_messages()` / `drain_committed()`.
//!
//! Because every input is explicit, a whole cluster can be simulated
//! deterministically from a single seed (see `sim.rs`).

use std::collections::{BTreeMap, BTreeSet};

pub type NodeId = u64;
pub type Term = u64;
/// Log indices are 1-based. Index 0 means "before the first entry".
pub type Index = u64;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub term: Term,
    /// Empty data marks the no-op a new leader appends to commit prior-term entries.
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message {
    RequestVote {
        term: Term,
        last_log_index: Index,
        last_log_term: Term,
    },
    RequestVoteReply {
        term: Term,
        granted: bool,
    },
    AppendEntries {
        term: Term,
        prev_log_index: Index,
        prev_log_term: Term,
        entries: Vec<Entry>,
        leader_commit: Index,
    },
    AppendEntriesReply {
        term: Term,
        success: bool,
        /// On success: highest index known to match the leader.
        match_index: Index,
        /// On failure: follower's suggestion for where the leader should retry from (hint + 1).
        hint: Index,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Envelope {
    pub from: NodeId,
    pub to: NodeId,
    pub msg: Message,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Clone, Debug)]
pub struct Config {
    /// Base election timeout in ticks. Actual timeout is randomized in [base, 2*base).
    pub election_ticks: u64,
    pub heartbeat_ticks: u64,
    pub max_entries_per_append: usize,
    pub seed: u64,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            election_ticks: 10,
            heartbeat_ticks: 2,
            max_entries_per_append: 64,
            seed: 1,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ProposeError {
    NotLeader { leader_hint: Option<NodeId> },
}

/// A committed, non-no-op command ready to be applied to the state machine.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Committed {
    pub index: Index,
    pub data: Vec<u8>,
}

pub struct Node {
    id: NodeId,
    peers: Vec<NodeId>,
    cfg: Config,

    role: Role,
    term: Term,
    voted_for: Option<NodeId>,
    leader_id: Option<NodeId>,

    log: Vec<Entry>, // log[i-1] holds index i
    commit_index: Index,
    last_applied: Index,

    // leader-only
    next_index: BTreeMap<NodeId, Index>,
    match_index: BTreeMap<NodeId, Index>,

    // candidate-only
    votes: BTreeSet<NodeId>,

    election_elapsed: u64,
    election_timeout: u64,
    heartbeat_elapsed: u64,

    rng: u64,
    outbox: Vec<Envelope>,
}

impl Node {
    pub fn new(id: NodeId, peers: Vec<NodeId>, cfg: Config) -> Self {
        let rng = (cfg.seed.wrapping_mul(0x9E37_79B9_7F4A_7C15)
            ^ id.wrapping_mul(0xBF58_476D_1CE4_E5B9))
            | 1;
        let mut n = Node {
            id,
            peers,
            cfg,
            role: Role::Follower,
            term: 0,
            voted_for: None,
            leader_id: None,
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            next_index: BTreeMap::new(),
            match_index: BTreeMap::new(),
            votes: BTreeSet::new(),
            election_elapsed: 0,
            election_timeout: 0,
            heartbeat_elapsed: 0,
            rng,
            outbox: Vec::new(),
        };
        n.reset_election_timer();
        n
    }

    // ---------- accessors ----------

    pub fn id(&self) -> NodeId {
        self.id
    }
    pub fn role(&self) -> Role {
        self.role
    }
    pub fn term(&self) -> Term {
        self.term
    }
    pub fn leader_id(&self) -> Option<NodeId> {
        self.leader_id
    }
    pub fn commit_index(&self) -> Index {
        self.commit_index
    }
    pub fn last_index(&self) -> Index {
        self.log.len() as Index
    }
    pub fn log(&self) -> &[Entry] {
        &self.log
    }

    // ---------- driving the node ----------

    pub fn tick(&mut self) {
        if self.role == Role::Leader {
            self.heartbeat_elapsed += 1;
            if self.heartbeat_elapsed >= self.cfg.heartbeat_ticks {
                self.heartbeat_elapsed = 0;
                self.broadcast_append();
            }
        } else {
            self.election_elapsed += 1;
            if self.election_elapsed >= self.election_timeout {
                self.start_election();
            }
        }
    }

    pub fn propose(&mut self, data: Vec<u8>) -> Result<(Term, Index), ProposeError> {
        assert!(!data.is_empty(), "empty data is reserved for leader no-ops");
        if self.role != Role::Leader {
            return Err(ProposeError::NotLeader {
                leader_hint: self.leader_id,
            });
        }
        self.log.push(Entry {
            term: self.term,
            data,
        });
        let index = self.last_index();
        self.advance_commit(); // single-node cluster commits immediately
        self.broadcast_append();
        Ok((self.term, index))
    }

    pub fn step(&mut self, from: NodeId, msg: Message) {
        match msg {
            Message::RequestVote {
                term,
                last_log_index,
                last_log_term,
            } => self.handle_request_vote(from, term, last_log_index, last_log_term),
            Message::RequestVoteReply { term, granted } => {
                self.handle_vote_reply(from, term, granted)
            }
            Message::AppendEntries {
                term,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => self.handle_append_entries(
                from,
                term,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            ),
            Message::AppendEntriesReply {
                term,
                success,
                match_index,
                hint,
            } => self.handle_append_reply(from, term, success, match_index, hint),
        }
    }

    pub fn drain_messages(&mut self) -> Vec<Envelope> {
        std::mem::take(&mut self.outbox)
    }

    /// Newly committed commands since the last call (no-ops are skipped).
    pub fn drain_committed(&mut self) -> Vec<Committed> {
        let mut out = Vec::new();
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            let e = &self.log[(self.last_applied - 1) as usize];
            if !e.data.is_empty() {
                out.push(Committed {
                    index: self.last_applied,
                    data: e.data.clone(),
                });
            }
        }
        out
    }

    // ---------- helpers ----------

    fn quorum(&self) -> usize {
        let cluster_size = self.peers.len() + 1;
        cluster_size / 2 + 1
    }

    fn next_rand(&mut self) -> u64 {
        let mut x = self.rng;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng = x;
        x
    }

    fn reset_election_timer(&mut self) {
        self.election_elapsed = 0;
        let jitter = self.next_rand() % self.cfg.election_ticks;
        self.election_timeout = self.cfg.election_ticks + jitter;
    }

    fn term_at(&self, index: Index) -> Option<Term> {
        if index == 0 {
            Some(0)
        } else {
            self.log.get((index - 1) as usize).map(|e| e.term)
        }
    }

    fn last_term(&self) -> Term {
        self.log.last().map(|e| e.term).unwrap_or(0)
    }

    fn send(&mut self, to: NodeId, msg: Message) {
        self.outbox.push(Envelope {
            from: self.id,
            to,
            msg,
        });
    }

    // ---------- role transitions ----------

    fn become_follower(&mut self, term: Term, leader: Option<NodeId>) {
        if term > self.term {
            self.term = term;
            self.voted_for = None;
        }
        self.role = Role::Follower;
        self.leader_id = leader;
        self.reset_election_timer();
    }

    fn start_election(&mut self) {
        self.term += 1;
        self.role = Role::Candidate;
        self.voted_for = Some(self.id);
        self.leader_id = None;
        self.votes.clear();
        self.votes.insert(self.id);
        self.reset_election_timer();

        if self.votes.len() >= self.quorum() {
            self.become_leader();
            return;
        }
        let (lli, llt) = (self.last_index(), self.last_term());
        for p in self.peers.clone() {
            self.send(
                p,
                Message::RequestVote {
                    term: self.term,
                    last_log_index: lli,
                    last_log_term: llt,
                },
            );
        }
    }

    fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.leader_id = Some(self.id);
        self.heartbeat_elapsed = 0;
        let next = self.last_index() + 1;
        for p in self.peers.clone() {
            self.next_index.insert(p, next);
            self.match_index.insert(p, 0);
        }
        // A leader may only commit entries from its own term by counting replicas
        // (Raft paper, section 5.4.2). The no-op gives it one to commit, which
        // transitively commits everything before it.
        self.log.push(Entry {
            term: self.term,
            data: Vec::new(),
        });
        self.advance_commit();
        self.broadcast_append();
    }

    // ---------- elections ----------

    fn handle_request_vote(&mut self, from: NodeId, term: Term, lli: Index, llt: Term) {
        if term < self.term {
            self.send(
                from,
                Message::RequestVoteReply {
                    term: self.term,
                    granted: false,
                },
            );
            return;
        }
        if term > self.term {
            self.become_follower(term, None);
        }
        // Candidate's log must be at least as up-to-date as ours (section 5.4.1).
        let up_to_date = (llt, lli) >= (self.last_term(), self.last_index());
        let can_vote = self.voted_for.is_none() || self.voted_for == Some(from);
        let grant = can_vote && up_to_date;
        if grant {
            self.voted_for = Some(from);
            self.reset_election_timer();
        }
        self.send(
            from,
            Message::RequestVoteReply {
                term: self.term,
                granted: grant,
            },
        );
    }

    fn handle_vote_reply(&mut self, from: NodeId, term: Term, granted: bool) {
        if term > self.term {
            self.become_follower(term, None);
            return;
        }
        if self.role != Role::Candidate || term < self.term {
            return;
        }
        if granted {
            self.votes.insert(from);
            if self.votes.len() >= self.quorum() {
                self.become_leader();
            }
        }
    }

    // ---------- replication ----------

    fn broadcast_append(&mut self) {
        for p in self.peers.clone() {
            self.send_append(p);
        }
    }

    fn send_append(&mut self, peer: NodeId) {
        let next = self.next_index[&peer];
        let prev = next - 1;
        let prev_term = self.term_at(prev).expect("leader has its own prev entry");
        let start = prev as usize;
        let end = self.log.len().min(start + self.cfg.max_entries_per_append);
        let entries = self.log[start..end].to_vec();
        self.send(
            peer,
            Message::AppendEntries {
                term: self.term,
                prev_log_index: prev,
                prev_log_term: prev_term,
                entries,
                leader_commit: self.commit_index,
            },
        );
    }

    fn handle_append_entries(
        &mut self,
        from: NodeId,
        term: Term,
        prev_idx: Index,
        prev_term: Term,
        entries: Vec<Entry>,
        leader_commit: Index,
    ) {
        if term < self.term {
            self.send(
                from,
                Message::AppendEntriesReply {
                    term: self.term,
                    success: false,
                    match_index: 0,
                    hint: 0,
                },
            );
            return;
        }
        // Same or higher term: `from` is the legitimate leader for this term.
        self.become_follower(term, Some(from));

        if prev_idx != 0 && self.term_at(prev_idx) != Some(prev_term) {
            let hint = self.last_index().min(prev_idx - 1);
            self.send(
                from,
                Message::AppendEntriesReply {
                    term: self.term,
                    success: false,
                    match_index: 0,
                    hint,
                },
            );
            return;
        }

        let mut idx = prev_idx;
        for e in entries {
            idx += 1;
            match self.term_at(idx) {
                Some(t) if t == e.term => {} // already have it
                Some(_) => {
                    debug_assert!(
                        idx > self.commit_index,
                        "must never truncate committed entries"
                    );
                    self.log.truncate((idx - 1) as usize);
                    self.log.push(e);
                }
                None => self.log.push(e),
            }
        }

        if leader_commit > self.commit_index {
            self.commit_index = leader_commit.min(idx);
        }
        self.send(
            from,
            Message::AppendEntriesReply {
                term: self.term,
                success: true,
                match_index: idx,
                hint: 0,
            },
        );
    }

    fn handle_append_reply(
        &mut self,
        from: NodeId,
        term: Term,
        success: bool,
        match_index: Index,
        hint: Index,
    ) {
        if term > self.term {
            self.become_follower(term, None);
            return;
        }
        if self.role != Role::Leader || term < self.term {
            return;
        }
        if success {
            let m = self.match_index.entry(from).or_insert(0);
            *m = (*m).max(match_index);
            let m = *m;
            self.next_index.insert(from, m + 1);
            let before = self.commit_index;
            self.advance_commit();
            if self.commit_index > before {
                self.broadcast_append(); // let followers learn the new commit index promptly
            } else if m < self.last_index() {
                self.send_append(from); // keep catching the follower up
            }
        } else {
            let next = self.next_index[&from];
            let new_next = (hint + 1).min(next.saturating_sub(1)).max(1);
            self.next_index.insert(from, new_next);
            self.send_append(from);
        }
    }

    /// Advance commit_index to the highest index replicated on a quorum whose
    /// entry is from the current term.
    fn advance_commit(&mut self) {
        let mut n = self.last_index();
        while n > self.commit_index {
            if self.term_at(n) == Some(self.term) {
                let replicas = 1 + self.match_index.values().filter(|&&m| m >= n).count();
                if replicas >= self.quorum() {
                    self.commit_index = n;
                    return;
                }
            }
            n -= 1;
        }
    }
}
