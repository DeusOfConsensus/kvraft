//! A blocking client for the cluster, using only `std`.
//!
//! It implements the contract the server's exactly-once guarantee depends on:
//!
//! * **one outstanding request**, with `seq` increasing by one per operation;
//! * **every retry of an operation reuses the same `(client, seq)`**, across
//!   reconnects and across different servers. That is what lets the replicated
//!   session table recognise a retry of a write that already committed.
//!
//! Redirects: a follower answers `NotLeader(hint)`; the client moves to the
//! hinted node. Connection errors move it to the next server. It keeps trying
//! until `deadline`, then gives up with [`ClientError::Unknown`]: the operation
//! may or may not have taken effect, and the caller must treat it that way.

use crate::kv::{ClientId, Command, Op, Resp};
use crate::raft::NodeId;
use crate::server::Outcome;
use crate::wire::{self, MAX_FRAME, Status};
use std::collections::BTreeMap;
use std::hash::{BuildHasher, Hasher, RandomState};
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::time::{Duration, Instant};

#[derive(Debug)]
pub enum ClientError {
    /// No answer before the deadline. The operation may or may not have been applied.
    Unknown,
    /// The server says this request is older than the client's latest one.
    /// Cannot happen if the client is used as documented.
    Stale,
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientError::Unknown => write!(
                f,
                "timed out: the operation may or may not have been applied"
            ),
            ClientError::Stale => write!(f, "server reported the request as stale"),
        }
    }
}

impl std::error::Error for ClientError {}

pub struct Client {
    servers: Vec<(NodeId, SocketAddr)>,
    id: ClientId,
    seq: u64,
    current: usize,
    conn: Option<TcpStream>,
    /// Socket read/write timeout for one attempt.
    pub attempt_timeout: Duration,
    /// Total time `execute` keeps trying.
    pub deadline: Duration,
}

impl Client {
    /// A client with a fresh random identity.
    pub fn new(servers: Vec<(NodeId, SocketAddr)>) -> Self {
        Self::with_id(servers, random_id())
    }

    /// Reusing an id continues that client's session: `first_seq` must be
    /// greater than any seq it used before.
    pub fn with_id(servers: Vec<(NodeId, SocketAddr)>, id: ClientId) -> Self {
        assert!(!servers.is_empty(), "need at least one server");
        Client {
            servers,
            id,
            seq: 0,
            current: 0,
            conn: None,
            attempt_timeout: Duration::from_secs(3),
            deadline: Duration::from_secs(30),
        }
    }

    pub fn id(&self) -> ClientId {
        self.id
    }

    /// Run one operation to completion (or the deadline).
    pub fn execute(&mut self, op: Op) -> Result<Resp, ClientError> {
        self.seq += 1;
        let seq = self.seq;
        let frame = wire::frame(&wire::encode_request(&Command {
            client: self.id,
            seq,
            op,
        }));
        let give_up = Instant::now() + self.deadline;
        while Instant::now() < give_up {
            match self.attempt(&frame, seq) {
                Ok(Outcome::Done(Resp::Stale)) => return Err(ClientError::Stale),
                Ok(Outcome::Done(resp)) => return Ok(resp),
                Ok(Outcome::NotLeader(hint)) => {
                    match hint.and_then(|h| self.servers.iter().position(|(id, _)| *id == h)) {
                        Some(i) if i != self.current => {
                            self.current = i;
                            self.conn = None; // the old connection still points at the old server
                        }
                        _ => self.advance(),
                    }
                    std::thread::sleep(Duration::from_millis(20));
                }
                Ok(Outcome::Retry) => {
                    self.advance();
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(_) => {
                    self.conn = None;
                    self.advance();
                    std::thread::sleep(Duration::from_millis(50));
                }
            }
        }
        Err(ClientError::Unknown)
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<Resp, ClientError> {
        self.execute(Op::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        })
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, ClientError> {
        match self.execute(Op::Get { key: key.to_vec() })? {
            Resp::Value(v) => Ok(v),
            _ => Err(ClientError::Stale),
        }
    }

    fn advance(&mut self) {
        self.current = (self.current + 1) % self.servers.len();
        self.conn = None;
    }

    fn attempt(&mut self, frame: &[u8], seq: u64) -> io::Result<Outcome> {
        let addr = self.servers[self.current].1;
        if self.conn.is_none() {
            let s = TcpStream::connect_timeout(&addr, Duration::from_secs(1))?;
            s.set_nodelay(true)?;
            s.set_read_timeout(Some(self.attempt_timeout))?;
            s.set_write_timeout(Some(self.attempt_timeout))?;
            self.conn = Some(s);
        }
        let s = self.conn.as_mut().unwrap();
        s.write_all(frame)?;
        let buf = read_frame(s)?;
        let rep = wire::decode_reply(&buf)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad reply"))?;
        if rep.client != self.id || rep.seq != seq {
            // Cannot happen on a connection we use one request at a time, but if
            // the stream is ever out of step, throw it away rather than guess.
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "mismatched reply",
            ));
        }
        Ok(rep.outcome)
    }
}

/// Ask one server about itself. A fresh short-lived connection.
pub fn status(addr: SocketAddr, timeout: Duration) -> io::Result<Status> {
    let mut s = TcpStream::connect_timeout(&addr, timeout)?;
    s.set_read_timeout(Some(timeout))?;
    s.set_write_timeout(Some(timeout))?;
    s.write_all(&wire::frame(&wire::encode_status_request()))?;
    let buf = read_frame(&mut s)?;
    wire::decode_status(&buf)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad status reply"))
}

fn read_frame(s: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut len = [0u8; 4];
    s.read_exact(&mut len)?;
    let len = u32::from_le_bytes(len) as usize;
    if len > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame too large",
        ));
    }
    let mut buf = vec![0u8; len];
    s.read_exact(&mut buf)?;
    Ok(buf)
}

fn random_id() -> u64 {
    // `RandomState` is seeded from the OS, which is all the randomness we need.
    let id = RandomState::new().build_hasher().finish();
    id.max(1)
}

/// Parse `1=127.0.0.1:7001,2=host:7002` into an id -> address map.
pub fn parse_addr_map(s: &str) -> Result<BTreeMap<NodeId, SocketAddr>, String> {
    let mut out = BTreeMap::new();
    for part in s.split(',').filter(|p| !p.is_empty()) {
        let (id, addr) = part
            .split_once('=')
            .ok_or_else(|| format!("expected ID=HOST:PORT, got {part:?}"))?;
        let id: NodeId = id
            .trim()
            .parse()
            .map_err(|_| format!("bad node id {id:?}"))?;
        let addr = addr
            .trim()
            .to_socket_addrs()
            .map_err(|e| format!("cannot resolve {addr:?}: {e}"))?
            .next()
            .ok_or_else(|| format!("{addr:?} resolved to nothing"))?;
        if out.insert(id, addr).is_some() {
            return Err(format!("node id {id} listed twice"));
        }
    }
    if out.is_empty() {
        return Err("empty address list".into());
    }
    Ok(out)
}
