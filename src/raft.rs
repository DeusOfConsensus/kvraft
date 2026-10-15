//! Raft consensus core.
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
//!
//! # Durability rule
//!
//! `term`, `voted_for` and the log are saved through the [`Storage`] trait at
//! the END of every public entry point (`tick`, `step`, `propose`), before it
//! returns. Outgoing messages only leave the node through `drain_messages`,
//! which the driver calls afterwards, so a node can never reply to (or vote
//! for) anyone with state that a crash could still erase. Doing this at one
//! choke point, instead of in every handler, means a forgotten save in a new
//! handler cannot silently break safety.
//!
//! Hard state is saved before the log, so after a crash between the two the
//! stored term is never older than the stored log (checked on load).
//!
//! # What a restart loses
//!
//! `commit_index`, `last_applied` and all leader/candidate bookkeeping are
//! volatile. A restarted node comes back as a follower with `commit_index = 0`
//! and re-learns the commit point from the leader. Consequently
//! `drain_committed` replays the log from the start, which is correct for a
//! state machine that also lives only in memory. (Snapshots, stage 5, will
//! shorten that replay.)

use crate::storage::{HardState, Storage};
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
    /// "Would you vote for me if I started an election for `term`?"
    /// Asking changes nothing on the receiver: no term bump, no vote cast.
    PreVote {
        term: Term,
        last_log_index: Index,
        last_log_term: Term,
    },
    /// When granted, `term` echoes the proposed term. When rejected, `term` is
    /// the responder's own term, so a stale pre-candidate can learn it is behind.
    PreVoteReply {
        term: Term,
        granted: bool,
    },
    AppendEntries {
        term: Term,
        prev_log_index: Index,
        prev_log_term: Term,
        entries: Vec<Entry>,
        leader_commit: Index,
        /// ReadIndex round the leader had reached when it sent this. Followers
        /// echo it back; see `read_index`.
        round: u64,
    },
    AppendEntriesReply {
        term: Term,
        success: bool,
        /// Echo of the request's `round`.
        round: u64,
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
    /// Polling for support before spending a term. Only used when `pre_vote` is on.
    PreCandidate,
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
    /// Ask peers for pre-votes before incrementing the term. Stops a node that
    /// is cut off from the cluster from inflating its term and forcing a
    /// needless election when it rejoins.
    pub pre_vote: bool,
    /// A leader that has not heard from a majority within an election timeout
    /// steps down instead of lingering as a leader that can commit nothing.
    pub check_quorum: bool,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            election_ticks: 10,
            heartbeat_ticks: 2,
            max_entries_per_append: 64,
            seed: 1,
            pre_vote: true,
            check_quorum: true,
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

#[derive(Debug, PartialEq, Eq)]
pub enum ReadError {
    NotLeader {
        leader_hint: Option<NodeId>,
    },
    /// Leader, but it has not yet committed an entry of its own term, so its
    /// `commit_index` may still lag the true commit point. Retry shortly.
    NotReady,
}

/// A linearizable read the leader has confirmed: once the state machine has
/// applied everything up to `index`, answering `id` is safe.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadState {
    pub id: u64,
    pub index: Index,
}

/// The fields of an incoming `AppendEntries`, bundled for the handler.
struct AppendReq {
    term: Term,
    prev_idx: Index,
    prev_term: Term,
    entries: Vec<Entry>,
    leader_commit: Index,
    round: u64,
}

struct PendingRead {
    id: u64,
    index: Index,
    /// Needs a quorum to have acknowledged a heartbeat of at least this round.
    round: u64,
}

pub struct Node<S: Storage> {
    id: NodeId,
    peers: Vec<NodeId>,
    cfg: Config,
    storage: S,

    role: Role,
    // --- persistent (saved via `storage`) ---
    term: Term,
    voted_for: Option<NodeId>,
    log: Vec<Entry>, // log[i-1] holds index i
    // --- volatile ---
    leader_id: Option<NodeId>,
    commit_index: Index,
    last_applied: Index,

    // leader-only
    next_index: BTreeMap<NodeId, Index>,
    match_index: BTreeMap<NodeId, Index>,
    /// Peers that have replied since the last quorum check (CheckQuorum).
    recent_active: BTreeSet<NodeId>,
    check_quorum_elapsed: u64,
    /// ReadIndex: bumped when a read arrives; stamped on every AppendEntries.
    round: u64,
    /// Highest round each peer has echoed back in this leadership.
    acked_round: BTreeMap<NodeId, u64>,
    pending_reads: Vec<PendingRead>,
    ready_reads: Vec<ReadState>,

    // candidate-only
    votes: BTreeSet<NodeId>,
    prevotes: BTreeSet<NodeId>,

    election_elapsed: u64,
    election_timeout: u64,
    heartbeat_elapsed: u64,

    rng: u64,
    outbox: Vec<Envelope>,

    // what `storage` is known to hold
    persisted_hs: HardState,
    /// Lowest log index changed since the last save, if any.
    log_dirty_from: Option<Index>,
}

impl<S: Storage> Node<S> {
    /// Create a node, restoring `term`, `voted_for` and the log from `storage`.
    /// Pass a fresh storage for a brand new node, or the surviving storage to
    /// model a restart after a crash.
    pub fn new(id: NodeId, peers: Vec<NodeId>, cfg: Config, mut storage: S) -> Self {
        let (hs, log) = storage.load();
        let last_log_term = log.last().map_or(0, |e| e.term);
        assert!(
            last_log_term <= hs.term,
            "corrupt state: log holds term {last_log_term} but stored term is only {}",
            hs.term
        );
        let rng = (cfg.seed.wrapping_mul(0x9E37_79B9_7F4A_7C15)
            ^ id.wrapping_mul(0xBF58_476D_1CE4_E5B9))
            | 1;
        let mut n = Node {
            id,
            peers,
            cfg,
            storage,
            role: Role::Follower,
            term: hs.term,
            voted_for: hs.voted_for,
            log,
            leader_id: None,
            commit_index: 0,
            last_applied: 0,
            next_index: BTreeMap::new(),
            match_index: BTreeMap::new(),
            recent_active: BTreeSet::new(),
            check_quorum_elapsed: 0,
            round: 0,
            acked_round: BTreeMap::new(),
            pending_reads: Vec::new(),
            ready_reads: Vec::new(),
            votes: BTreeSet::new(),
            prevotes: BTreeSet::new(),
            election_elapsed: 0,
            election_timeout: 0,
            heartbeat_elapsed: 0,
            rng,
            outbox: Vec::new(),
            persisted_hs: hs,
            log_dirty_from: None,
        };
        n.reset_election_timer();
        n
    }

    /// Consume the node and hand back its storage: this is "the process died,
    /// the disk survived".
    pub fn into_storage(self) -> S {
        self.storage
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
    pub fn voted_for(&self) -> Option<NodeId> {
        self.voted_for
    }
    pub fn leader_id(&self) -> Option<NodeId> {
        self.leader_id
    }
    pub fn commit_index(&self) -> Index {
        self.commit_index
    }
    /// Highest log index handed out by `drain_committed` so far.
    pub fn last_applied(&self) -> Index {
        self.last_applied
    }
    pub fn last_index(&self) -> Index {
        self.log.len() as Index
    }
    pub fn log(&self) -> &[Entry] {
        &self.log
    }

    // ---------- driving the node ----------

    pub fn tick(&mut self) {
        self.tick_inner();
        self.persist();
    }

    pub fn propose(&mut self, data: Vec<u8>) -> Result<(Term, Index), ProposeError> {
        assert!(!data.is_empty(), "empty data is reserved for leader no-ops");
        if self.role != Role::Leader {
            return Err(ProposeError::NotLeader {
                leader_hint: self.leader_id,
            });
        }
        self.push_entry(Entry {
            term: self.term,
            data,
        });
        let index = self.last_index();
        self.advance_commit(); // single-node cluster commits immediately
        self.broadcast_append();
        self.persist();
        Ok((self.term, index))
    }

    /// Start a linearizable read, identified by the caller's `id`.
    ///
    /// ReadIndex: remember the current `commit_index`, then prove we are still
    /// the leader by getting a quorum to acknowledge a heartbeat sent AFTER the
    /// read arrived. (A deposed leader that has not noticed yet cannot get that
    /// quorum, because a majority has already moved to a higher term.) The
    /// confirmed read appears in `drain_read_states`; the caller answers it once
    /// its state machine has applied up to the reported index.
    ///
    /// Nothing is written to the log, so reads cost one heartbeat round trip
    /// and no disk write.
    pub fn read_index(&mut self, id: u64) -> Result<(), ReadError> {
        if self.role != Role::Leader {
            return Err(ReadError::NotLeader {
                leader_hint: self.leader_id,
            });
        }
        // Until we commit something in our own term, commit_index may be behind
        // writes the previous leader already acknowledged.
        if self.term_at(self.commit_index) != Some(self.term) {
            return Err(ReadError::NotReady);
        }
        self.round += 1;
        self.pending_reads.push(PendingRead {
            id,
            index: self.commit_index,
            round: self.round,
        });
        self.broadcast_append();
        self.advance_reads(); // a single-node cluster is its own quorum
        self.persist();
        Ok(())
    }

    /// Reads confirmed since the last call. They are dropped, and never
    /// reported, if this node loses leadership first.
    pub fn drain_read_states(&mut self) -> Vec<ReadState> {
        std::mem::take(&mut self.ready_reads)
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
            Message::PreVote {
                term,
                last_log_index,
                last_log_term,
            } => self.handle_pre_vote(from, term, last_log_index, last_log_term),
            Message::PreVoteReply { term, granted } => {
                self.handle_pre_vote_reply(from, term, granted)
            }
            Message::AppendEntries {
                term,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                round,
            } => self.handle_append_entries(
                from,
                AppendReq {
                    term,
                    prev_idx: prev_log_index,
                    prev_term: prev_log_term,
                    entries,
                    leader_commit,
                    round,
                },
            ),
            Message::AppendEntriesReply {
                term,
                success,
                round,
                match_index,
                hint,
            } => self.handle_append_reply(from, term, success, round, match_index, hint),
        }
        self.persist();
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

    // ---------- persistence ----------

    /// Make everything that changed since the last call durable. Called once at
    /// the end of each public entry point, never from inside a handler.
    fn persist(&mut self) {
        let hs = HardState {
            term: self.term,
            voted_for: self.voted_for,
        };
        if hs != self.persisted_hs {
            self.storage.save_hard_state(hs);
            self.persisted_hs = hs;
        }
        if let Some(from) = self.log_dirty_from.take() {
            self.storage
                .replace_log_suffix(from, &self.log[(from - 1) as usize..]);
        }
    }

    fn mark_log_dirty(&mut self, index: Index) {
        self.log_dirty_from = Some(self.log_dirty_from.map_or(index, |d| d.min(index)));
    }

    fn push_entry(&mut self, e: Entry) {
        self.log.push(e);
        let idx = self.last_index();
        self.mark_log_dirty(idx);
    }

    // ---------- helpers ----------

    fn tick_inner(&mut self) {
        if self.role == Role::Leader {
            if self.cfg.check_quorum {
                self.check_quorum_elapsed += 1;
                if self.check_quorum_elapsed >= self.cfg.election_ticks {
                    let active = self.recent_active.len() + 1; // +1: ourselves
                    self.recent_active.clear();
                    self.check_quorum_elapsed = 0;
                    if active < self.quorum() {
                        // We can no longer commit anything. Stop pretending to lead.
                        self.become_follower(self.term, None);
                        return;
                    }
                }
            }
            self.heartbeat_elapsed += 1;
            if self.heartbeat_elapsed >= self.cfg.heartbeat_ticks {
                self.heartbeat_elapsed = 0;
                self.broadcast_append();
            }
        } else {
            self.election_elapsed += 1;
            if self.election_elapsed >= self.election_timeout {
                if self.cfg.pre_vote {
                    self.start_pre_vote();
                } else {
                    self.start_election();
                }
            }
        }
    }

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
        self.pending_reads.clear();
        self.ready_reads.clear();
        self.reset_election_timer();
    }

    /// Phase 1 of an election: poll peers WITHOUT touching `term` or `voted_for`.
    /// Only if a majority says "yes, I would vote for you" do we spend a term.
    fn start_pre_vote(&mut self) {
        self.role = Role::PreCandidate;
        self.leader_id = None;
        self.prevotes.clear();
        self.prevotes.insert(self.id);
        self.reset_election_timer();

        if self.prevotes.len() >= self.quorum() {
            self.start_election();
            return;
        }
        let proposed = self.term + 1;
        let (lli, llt) = (self.last_index(), self.last_term());
        for p in self.peers.clone() {
            self.send(
                p,
                Message::PreVote {
                    term: proposed,
                    last_log_index: lli,
                    last_log_term: llt,
                },
            );
        }
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
        self.recent_active.clear();
        self.check_quorum_elapsed = 0;
        self.round = 0;
        self.acked_round.clear();
        self.pending_reads.clear();
        self.ready_reads.clear();
        let next = self.last_index() + 1;
        for p in self.peers.clone() {
            self.next_index.insert(p, next);
            self.match_index.insert(p, 0);
        }
        // A leader may only commit entries from its own term by counting replicas
        // (Raft paper, section 5.4.2). The no-op gives it one to commit, which
        // transitively commits everything before it.
        self.push_entry(Entry {
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

    /// Answer "would you vote for me at `term`?" without changing any state.
    fn handle_pre_vote(&mut self, from: NodeId, term: Term, lli: Index, llt: Term) {
        // The proposed term must actually be ahead of ours.
        if term <= self.term {
            self.send(
                from,
                Message::PreVoteReply {
                    term: self.term,
                    granted: false,
                },
            );
            return;
        }
        // Leader lease: if we have a working leader (or are one), refuse to help
        // anyone start an election. This is what keeps a rejoining node from
        // dethroning a healthy leader.
        let has_live_leader = self.role == Role::Leader
            || (self.leader_id.is_some() && self.election_elapsed < self.cfg.election_ticks);
        let up_to_date = (llt, lli) >= (self.last_term(), self.last_index());
        let grant = !has_live_leader && up_to_date;
        self.send(
            from,
            Message::PreVoteReply {
                term: if grant { term } else { self.term },
                granted: grant,
            },
        );
    }

    fn handle_pre_vote_reply(&mut self, from: NodeId, term: Term, granted: bool) {
        if !granted {
            // A rejection carries the responder's term; if it is ahead, we are stale.
            if term > self.term {
                self.become_follower(term, None);
            }
            return;
        }
        // Ignore grants for a proposal that is no longer current.
        if self.role != Role::PreCandidate || term != self.term + 1 {
            return;
        }
        self.prevotes.insert(from);
        if self.prevotes.len() >= self.quorum() {
            self.start_election();
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
                round: self.round,
            },
        );
    }

    fn handle_append_entries(&mut self, from: NodeId, req: AppendReq) {
        let AppendReq {
            term,
            prev_idx,
            prev_term,
            entries,
            leader_commit,
            round,
        } = req;
        if term < self.term {
            self.send(
                from,
                Message::AppendEntriesReply {
                    term: self.term,
                    success: false,
                    round,
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
                    round,
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
                    self.push_entry(e);
                }
                None => self.push_entry(e),
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
                round,
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
        round: u64,
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
        // Any same-term reply, success or not, proves `from` can still talk to us.
        self.recent_active.insert(from);
        // A same-term reply also proves `from` accepted us as leader when it
        // processed the request carrying `round`.
        let acked = self.acked_round.entry(from).or_insert(0);
        *acked = (*acked).max(round);
        self.advance_reads();
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

    /// Promote pending reads whose round a quorum has acknowledged.
    fn advance_reads(&mut self) {
        let quorum = self.quorum();
        let acked = &self.acked_round;
        let ready = &mut self.ready_reads;
        self.pending_reads.retain(|r| {
            let confirmed = 1 + acked.values().filter(|&&a| a >= r.round).count() >= quorum;
            if confirmed {
                ready.push(ReadState {
                    id: r.id,
                    index: r.index,
                });
            }
            !confirmed
        });
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
