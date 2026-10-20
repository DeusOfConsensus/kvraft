//! The network driver: turns the pure [`KvServer`] into a running process.
//!
//! # Shape: one actor, many I/O tasks
//!
//! ```text
//!   peer listener  --> handle_peer   --\
//!   client listener--> handle_client --+--> inbox --> ACTOR (owns KvServer) --+--> peer link tasks --> TCP
//!   ticker --------------------------/                                       +--> oneshot replies --> handle_client
//! ```
//!
//! The actor is the only code that touches the Raft node and the state
//! machine, so none of the stage 1-3 logic needs a lock, and the guarantees it
//! was tested under (single-threaded, inputs processed one at a time) still
//! hold. The actor does no network I/O of its own: it hands bytes to per-peer
//! queues with `try_send`, which never blocks.
//!
//! # Rules that keep it safe
//!
//! * **Persist before send** is preserved: `KvServer::step`/`tick`/`submit`
//!   save to disk before returning, and only afterwards does the actor drain
//!   messages onto the network.
//! * **Messages may be lost.** Raft tolerates it, so a full or broken peer
//!   queue drops messages instead of blocking the actor. A reconnecting link
//!   first discards its backlog, so a recovered peer is not flooded with stale
//!   heartbeats.
//! * **Frames are bounded** (`wire::MAX_FRAME`) and decoded strictly; a peer or
//!   client that sends garbage is disconnected, never trusted.
//! * **Peers are identified once**, in the hello frame, and must be members of
//!   the configured cluster.
//!
//! # Known limits (deliberate, see README)
//!
//! No TLS or authentication: anyone who can reach the port can speak the
//! protocol. The WAL is written (and fsynced) from the actor thread, so a slow
//! disk stalls the node, which is the safe failure.

use crate::raft::{Config, NodeId};
use crate::server::{KvServer, Outcome, Reply};
use crate::storage::FileStorage;
use crate::wire::{self, MAX_FRAME, Status};
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{MissedTickBehavior, interval, sleep, timeout};

/// How long a client request may wait for the cluster before it is told to retry.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const PEER_QUEUE: usize = 256;
const INBOX: usize = 1024;

pub struct NodeConfig {
    pub id: NodeId,
    /// The OTHER nodes' peer-port addresses.
    pub peers: BTreeMap<NodeId, SocketAddr>,
    pub data_dir: PathBuf,
    /// Wall-clock length of one Raft tick. Election timeout is 10-20 ticks and
    /// the heartbeat every 2 with the default `Config`.
    pub tick: Duration,
    pub raft: Config,
}

enum Event {
    Peer {
        from: NodeId,
        msg: crate::raft::Message,
    },
    Client {
        cmd: crate::kv::Command,
        reply: oneshot::Sender<Reply>,
    },
    Status {
        reply: oneshot::Sender<Status>,
    },
}

/// Run a node until an I/O error ends it. Listeners are passed in already bound
/// so the caller controls (and can learn) the ports.
pub async fn run(
    cfg: NodeConfig,
    peer_listener: TcpListener,
    client_listener: TcpListener,
) -> io::Result<()> {
    std::fs::create_dir_all(&cfg.data_dir)?;
    let storage = FileStorage::open(cfg.data_dir.join("wal.log"))?;
    let peer_ids: Vec<NodeId> = cfg.peers.keys().copied().collect();
    let mut server = KvServer::new(cfg.id, peer_ids.clone(), cfg.raft.clone(), storage);

    let (tx, mut rx) = mpsc::channel::<Event>(INBOX);

    // Outgoing links, one task per peer.
    let mut links: BTreeMap<NodeId, mpsc::Sender<Vec<u8>>> = BTreeMap::new();
    for (&pid, &addr) in &cfg.peers {
        let (ltx, lrx) = mpsc::channel(PEER_QUEUE);
        links.insert(pid, ltx);
        tokio::spawn(peer_sender(cfg.id, addr, lrx));
    }

    // Incoming connections.
    let known: Arc<BTreeSet<NodeId>> = Arc::new(peer_ids.into_iter().collect());
    tokio::spawn(accept_peers(peer_listener, tx.clone(), known));
    tokio::spawn(accept_clients(client_listener, tx.clone()));
    drop(tx);

    let mut pending: HashMap<(u64, u64), VecDeque<oneshot::Sender<Reply>>> = HashMap::new();
    let mut ticker = interval(cfg.tick);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = ticker.tick() => server.tick(),
            ev = rx.recv() => match ev {
                None => return Ok(()),
                Some(Event::Peer { from, msg }) => server.step(from, msg),
                Some(Event::Client { cmd, reply }) => {
                    pending.entry((cmd.client, cmd.seq)).or_default().push_back(reply);
                    server.submit(cmd.client, cmd.seq, cmd.op);
                }
                Some(Event::Status { reply }) => {
                    let n = server.node();
                    let _ = reply.send(Status {
                        id: n.id(),
                        role: n.role(),
                        term: n.term(),
                        leader: n.leader_id(),
                        commit_index: n.commit_index(),
                        last_applied: n.last_applied(),
                    });
                }
            }
        }
        // State is durable by now; only now may anything leave the node.
        for env in server.drain_messages() {
            if let Some(link) = links.get(&env.to) {
                let _ = link.try_send(wire::frame(&wire::encode_message(&env.msg)));
            }
        }
        for r in server.drain_replies() {
            let key = (r.client, r.seq);
            if let Some(q) = pending.get_mut(&key) {
                if let Some(sender) = q.pop_front() {
                    let _ = sender.send(r);
                }
                if q.is_empty() {
                    pending.remove(&key);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Frames
// ---------------------------------------------------------------------------

async fn read_frame(s: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut len = [0u8; 4];
    s.read_exact(&mut len).await?;
    let len = u32::from_le_bytes(len) as usize;
    if len > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame too large",
        ));
    }
    let mut buf = vec![0u8; len];
    s.read_exact(&mut buf).await?;
    Ok(buf)
}

fn bad(msg: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

// ---------------------------------------------------------------------------
// Outgoing peer link
// ---------------------------------------------------------------------------

async fn peer_sender(my_id: NodeId, addr: SocketAddr, mut rx: mpsc::Receiver<Vec<u8>>) {
    loop {
        // (Re)connect.
        let mut stream = loop {
            match timeout(Duration::from_secs(1), TcpStream::connect(addr)).await {
                Ok(Ok(s)) => break s,
                _ => sleep(Duration::from_millis(200)).await,
            }
        };
        let _ = stream.set_nodelay(true);
        // Whatever piled up while we were disconnected is stale.
        while rx.try_recv().is_ok() {}
        let hello = wire::frame(&wire::encode_hello(my_id));
        if stream.write_all(&hello).await.is_err() {
            continue;
        }
        loop {
            let Some(bytes) = rx.recv().await else { return };
            match timeout(Duration::from_secs(2), stream.write_all(&bytes)).await {
                Ok(Ok(())) => {}
                _ => break, // broken or stuck: drop this message and reconnect
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Incoming peers
// ---------------------------------------------------------------------------

async fn accept_peers(l: TcpListener, tx: mpsc::Sender<Event>, known: Arc<BTreeSet<NodeId>>) {
    loop {
        let Ok((s, _)) = l.accept().await else {
            sleep(Duration::from_millis(50)).await;
            continue;
        };
        let (tx, known) = (tx.clone(), known.clone());
        tokio::spawn(async move {
            let _ = handle_peer(s, tx, known).await;
        });
    }
}

async fn handle_peer(
    mut s: TcpStream,
    tx: mpsc::Sender<Event>,
    known: Arc<BTreeSet<NodeId>>,
) -> io::Result<()> {
    let _ = s.set_nodelay(true);
    let hello = timeout(Duration::from_secs(5), read_frame(&mut s))
        .await
        .map_err(|_| bad("no hello"))??;
    let from = wire::decode_hello(&hello).ok_or_else(|| bad("bad hello"))?;
    if !known.contains(&from) {
        return Err(bad("hello from a node outside the cluster"));
    }
    loop {
        let buf = read_frame(&mut s).await?;
        let msg = wire::decode_message(&buf).ok_or_else(|| bad("bad raft message"))?;
        if tx.send(Event::Peer { from, msg }).await.is_err() {
            return Ok(());
        }
    }
}

// ---------------------------------------------------------------------------
// Clients
// ---------------------------------------------------------------------------

async fn accept_clients(l: TcpListener, tx: mpsc::Sender<Event>) {
    loop {
        let Ok((s, _)) = l.accept().await else {
            sleep(Duration::from_millis(50)).await;
            continue;
        };
        let tx = tx.clone();
        tokio::spawn(async move {
            let _ = handle_client(s, tx).await;
        });
    }
}

async fn handle_client(mut s: TcpStream, tx: mpsc::Sender<Event>) -> io::Result<()> {
    let _ = s.set_nodelay(true);
    loop {
        let buf = read_frame(&mut s).await?;
        let out = if wire::is_status_request(&buf) {
            let (otx, orx) = oneshot::channel();
            tx.send(Event::Status { reply: otx })
                .await
                .map_err(|_| bad("node stopped"))?;
            let st = orx.await.map_err(|_| bad("node stopped"))?;
            wire::encode_status(&st)
        } else {
            let cmd = wire::decode_request(&buf).ok_or_else(|| bad("bad request"))?;
            let (client, seq) = (cmd.client, cmd.seq);
            let (otx, orx) = oneshot::channel();
            tx.send(Event::Client { cmd, reply: otx })
                .await
                .map_err(|_| bad("node stopped"))?;
            let reply = match timeout(REQUEST_TIMEOUT, orx).await {
                Ok(Ok(r)) => r,
                Ok(Err(_)) => return Err(bad("node stopped")),
                // Nobody answered in time: tell the client to try again.
                Err(_) => Reply {
                    client,
                    seq,
                    outcome: Outcome::Retry,
                },
            };
            wire::encode_reply(&reply)
        };
        s.write_all(&wire::frame(&out)).await?;
    }
}
