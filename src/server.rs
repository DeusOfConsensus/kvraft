//! One replica of the key-value service: a Raft node plus the state machine,
//! with the glue that turns client requests into replies.
//!
//! Like the Raft core it does no I/O: the driver feeds it `submit`, `step` and
//! `tick`, and collects `drain_messages` (to peers) and `drain_replies` (to
//! clients).
//!
//! # Writes
//!
//! `submit` encodes the command, proposes it, and remembers "when log index N
//! commits, answer this client". When `drain_committed` hands the entry back,
//! the server applies it to [`Kv`] and answers. The answer is only given if the
//! entry that committed at N really is that client's command; if a new leader
//! overwrote N, the client is told to retry (it will then hit the session table
//! and, if the command did commit elsewhere, get its saved response).
//!
//! # Reads
//!
//! `Get` is never logged. It goes through ReadIndex (`Node::read_index`) and is
//! answered from the local state machine once everything up to the confirmed
//! read index has been applied.
//!
//! # Losing leadership
//!
//! The moment this node is no longer leader of the term a request arrived in,
//! all its waiting requests are answered `Retry`. Retrying is always safe
//! because writes are deduplicated by the session table and reads change
//! nothing.

use crate::kv::{ClientId, Command, Kv, Op, Resp};
use crate::raft::{Config, Envelope, Index, Message, Node, NodeId, ReadError, Role, Term};
use crate::storage::Storage;
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The operation's result.
    Done(Resp),
    /// This node is not the leader; try `hint` if there is one.
    NotLeader(Option<NodeId>),
    /// Nothing is wrong, but the answer is not available from here right now.
    Retry,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Reply {
    pub client: ClientId,
    pub seq: u64,
    pub outcome: Outcome,
}

struct WriteWait {
    client: ClientId,
    seq: u64,
    term: Term,
}

struct ReadWait {
    client: ClientId,
    seq: u64,
    key: Vec<u8>,
    term: Term,
    /// Set once the leader has confirmed it: answer when applied >= index.
    index: Option<Index>,
}

pub struct KvServer<S: Storage> {
    node: Node<S>,
    kv: Kv,
    writes: BTreeMap<Index, WriteWait>,
    reads: BTreeMap<u64, ReadWait>,
    next_read: u64,
    replies: Vec<Reply>,
}

impl<S: Storage> KvServer<S> {
    pub fn new(id: NodeId, peers: Vec<NodeId>, cfg: Config, storage: S) -> Self {
        KvServer {
            node: Node::new(id, peers, cfg, storage),
            kv: Kv::new(),
            writes: BTreeMap::new(),
            reads: BTreeMap::new(),
            next_read: 1,
            replies: Vec::new(),
        }
    }

    pub fn into_storage(self) -> S {
        self.node.into_storage()
    }

    pub fn node(&self) -> &Node<S> {
        &self.node
    }
    pub fn kv(&self) -> &Kv {
        &self.kv
    }
    pub fn id(&self) -> NodeId {
        self.node.id()
    }
    /// Requests accepted but not yet answered.
    pub fn waiting(&self) -> usize {
        self.writes.len() + self.reads.len()
    }

    pub fn tick(&mut self) {
        self.node.tick();
        self.pump();
    }

    pub fn step(&mut self, from: NodeId, msg: Message) {
        self.node.step(from, msg);
        self.pump();
    }

    pub fn drain_messages(&mut self) -> Vec<Envelope> {
        self.node.drain_messages()
    }

    pub fn drain_replies(&mut self) -> Vec<Reply> {
        std::mem::take(&mut self.replies)
    }

    /// A client request. The reply (possibly immediate) arrives via `drain_replies`.
    pub fn submit(&mut self, client: ClientId, seq: u64, op: Op) {
        match op {
            Op::Get { key } => {
                let id = self.next_read;
                self.next_read += 1;
                match self.node.read_index(id) {
                    Ok(()) => {
                        self.reads.insert(
                            id,
                            ReadWait {
                                client,
                                seq,
                                key,
                                term: self.node.term(),
                                index: None,
                            },
                        );
                    }
                    Err(ReadError::NotLeader { leader_hint }) => {
                        self.reply(client, seq, Outcome::NotLeader(leader_hint))
                    }
                    Err(ReadError::NotReady) => self.reply(client, seq, Outcome::Retry),
                }
            }
            op => {
                // Already executed (a retry whose reply was lost)? Answer from
                // the session table without growing the log.
                if let Some(resp) = self.kv.session_lookup(client, seq) {
                    self.reply(client, seq, Outcome::Done(resp));
                } else {
                    let cmd = Command { client, seq, op };
                    match self.node.propose(cmd.encode()) {
                        Ok((term, index)) => {
                            self.writes.insert(index, WriteWait { client, seq, term });
                        }
                        Err(crate::raft::ProposeError::NotLeader { leader_hint }) => {
                            self.reply(client, seq, Outcome::NotLeader(leader_hint))
                        }
                    }
                }
            }
        }
        self.pump();
    }

    fn reply(&mut self, client: ClientId, seq: u64, outcome: Outcome) {
        self.replies.push(Reply {
            client,
            seq,
            outcome,
        });
    }

    /// Apply what committed, then answer whoever is waiting. Called after every
    /// input, so waiting requests are resolved as early as possible.
    fn pump(&mut self) {
        for c in self.node.drain_committed() {
            let Some(cmd) = Command::decode(&c.data) else {
                continue; // identical on every replica, so harmless
            };
            let resp = self.kv.apply(&cmd);
            if let Some(w) = self.writes.remove(&c.index) {
                let outcome = if w.client == cmd.client && w.seq == cmd.seq {
                    Outcome::Done(resp)
                } else {
                    Outcome::Retry // our proposal was overwritten by another leader's entry
                };
                self.reply(w.client, w.seq, outcome);
            }
        }
        for rs in self.node.drain_read_states() {
            if let Some(w) = self.reads.get_mut(&rs.id) {
                w.index = Some(rs.index);
            }
        }

        let applied = self.node.last_applied();
        let leading = self.node.role() == Role::Leader;
        let term = self.node.term();

        // Anything proposed in a leadership we no longer hold, or whose index is
        // already behind us without our command having appeared, will not be answered.
        let dead_writes: Vec<Index> = self
            .writes
            .iter()
            .filter(|(i, w)| !leading || w.term != term || **i <= applied)
            .map(|(i, _)| *i)
            .collect();
        for i in dead_writes {
            let w = self.writes.remove(&i).unwrap();
            self.reply(w.client, w.seq, Outcome::Retry);
        }

        let ids: Vec<u64> = self.reads.keys().copied().collect();
        for id in ids {
            let w = &self.reads[&id];
            if !leading || w.term != term {
                let w = self.reads.remove(&id).unwrap();
                self.reply(w.client, w.seq, Outcome::Retry);
            } else if w.index.is_some_and(|i| i <= applied) {
                let w = self.reads.remove(&id).unwrap();
                let resp = self.kv.read(&w.key);
                self.reply(w.client, w.seq, Outcome::Done(resp));
            }
        }
    }
}
