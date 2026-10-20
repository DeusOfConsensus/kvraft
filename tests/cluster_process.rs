//! Real processes, real sockets, real `kill -9`.
//!
//! These start the actual `kvraft-server` binary three times, talk to it with
//! the real client, and break it the way production does. Everything stage 1-3
//! proved in simulation has to hold again here, over TCP, with the file WAL.

mod common;

use common::Rng;
use kvraft::client::{Client, ClientError, status};
use kvraft::kv::{Op, Resp};
use kvraft::linearizability::{HistOp, Verdict, check};
use kvraft::raft::Role;
use kvraft::wire;
use std::io::Write;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

static NEXT: AtomicU64 = AtomicU64::new(0);

fn free_addr() -> SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap()
}

struct Cluster {
    dir: PathBuf,
    peer: Vec<SocketAddr>,
    client: Vec<SocketAddr>,
    kids: Vec<Option<Child>>,
}

impl Cluster {
    fn new(n: usize) -> Cluster {
        let dir = std::env::temp_dir().join(format!(
            "kvraft-it-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        let mut c = Cluster {
            dir,
            peer: (0..n).map(|_| free_addr()).collect(),
            client: (0..n).map(|_| free_addr()).collect(),
            kids: (0..n).map(|_| None).collect(),
        };
        for i in 0..n {
            c.start(i);
        }
        c
    }

    fn start(&mut self, i: usize) {
        assert!(self.kids[i].is_none(), "node {} already running", i + 1);
        let peers: Vec<String> = self
            .peer
            .iter()
            .enumerate()
            .map(|(j, a)| format!("{}={}", j + 1, a))
            .collect();
        let child = Command::new(env!("CARGO_BIN_EXE_kvraft-server"))
            .args(["--id", &(i + 1).to_string()])
            .args(["--peers", &peers.join(",")])
            .args(["--client-listen", &self.client[i].to_string()])
            .args([
                "--data-dir",
                self.dir.join(format!("n{}", i + 1)).to_str().unwrap(),
            ])
            .args(["--tick-ms", "25"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn kvraft-server");
        self.kids[i] = Some(child);
    }

    /// SIGKILL: no shutdown code runs, exactly like a power cut for the process.
    fn kill(&mut self, i: usize) {
        if let Some(mut k) = self.kids[i].take() {
            let _ = k.kill();
            let _ = k.wait();
        }
    }

    fn alive(&self) -> Vec<usize> {
        (0..self.kids.len())
            .filter(|&i| self.kids[i].is_some())
            .collect()
    }

    fn servers(&self) -> Vec<(u64, SocketAddr)> {
        self.client
            .iter()
            .enumerate()
            .map(|(i, a)| (i as u64 + 1, *a))
            .collect()
    }

    fn client(&self) -> Client {
        let mut c = Client::new(self.servers());
        c.deadline = Duration::from_secs(20);
        c
    }

    fn status_of(&self, i: usize) -> Option<wire::Status> {
        status(self.client[i], Duration::from_millis(300)).ok()
    }

    /// A leader that a majority of the live nodes agree on.
    fn wait_leader(&self) -> u64 {
        let deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < deadline {
            let sts: Vec<wire::Status> = self
                .alive()
                .into_iter()
                .filter_map(|i| self.status_of(i))
                .collect();
            if let Some(l) = sts
                .iter()
                .filter(|s| s.role == Role::Leader)
                .max_by_key(|s| s.term)
            {
                let agree = sts
                    .iter()
                    .filter(|s| s.leader == Some(l.id) && s.term == l.term)
                    .count();
                if agree > self.kids.len() / 2 {
                    return l.id;
                }
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        panic!("no stable leader within 20s");
    }

    /// Every live node has applied everything its leader committed.
    fn wait_caught_up(&self) {
        let deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < deadline {
            let sts: Vec<wire::Status> = self
                .alive()
                .into_iter()
                .filter_map(|i| self.status_of(i))
                .collect();
            if sts.len() == self.alive().len()
                && sts.iter().all(|s| s.last_applied == sts[0].last_applied)
                && sts.iter().all(|s| s.commit_index == s.last_applied)
            {
                return;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        panic!("live nodes never agreed on how much is applied");
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for i in 0..self.kids.len() {
            self.kill(i);
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn b(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

fn put_ok(c: &mut Client, k: &str, v: &str) {
    assert_eq!(c.put(k.as_bytes(), v.as_bytes()).unwrap(), Resp::Ok);
}

fn get(c: &mut Client, k: &str) -> Option<String> {
    c.get(k.as_bytes())
        .unwrap()
        .map(|v| String::from_utf8(v).unwrap())
}

// ---------------------------------------------------------------------------

#[test]
fn a_real_cluster_stores_and_reads_back() {
    let cl = Cluster::new(3);
    cl.wait_leader();
    let mut c = cl.client();
    put_ok(&mut c, "name", "kene");
    assert_eq!(get(&mut c, "name").as_deref(), Some("kene"));
    assert_eq!(get(&mut c, "missing"), None);
    assert_eq!(
        c.execute(Op::Append {
            key: b("log"),
            value: b("a")
        })
        .unwrap(),
        Resp::Value(Some(b("a")))
    );
    assert_eq!(
        c.execute(Op::Append {
            key: b("log"),
            value: b("b")
        })
        .unwrap(),
        Resp::Value(Some(b("ab")))
    );
    assert_eq!(
        c.execute(Op::Cas {
            key: b("name"),
            expected: Some(b("kene")),
            new: b("asmodeus")
        })
        .unwrap(),
        Resp::Cas(true)
    );
    assert_eq!(get(&mut c, "name").as_deref(), Some("asmodeus"));
}

#[test]
fn killing_the_leader_loses_nothing_and_the_cluster_keeps_serving() {
    let mut cl = Cluster::new(3);
    let leader = cl.wait_leader();
    let mut c = cl.client();
    for i in 0..20 {
        put_ok(&mut c, &format!("k{i}"), &format!("v{i}"));
    }

    cl.kill(leader as usize - 1);
    let new_leader = cl.wait_leader();
    assert_ne!(new_leader, leader);

    // Everything acknowledged before the kill is still there, and writes work.
    for i in 0..20 {
        assert_eq!(get(&mut c, &format!("k{i}")), Some(format!("v{i}")));
    }
    put_ok(&mut c, "after", "kill");

    // The dead node comes back from its WAL and catches up.
    cl.start(leader as usize - 1);
    cl.wait_caught_up();
    cl.wait_leader();
    assert_eq!(get(&mut c, "after").as_deref(), Some("kill"));
    let st = cl
        .status_of(leader as usize - 1)
        .expect("restarted node answers");
    assert!(
        st.last_applied >= 21,
        "restarted node replayed its log: {st:?}"
    );
}

#[test]
fn every_node_can_die_at_once_and_the_data_comes_back_from_disk() {
    let mut cl = Cluster::new(3);
    cl.wait_leader();
    let mut c = cl.client();
    for i in 0..30 {
        put_ok(&mut c, &format!("k{i}"), &format!("v{i}"));
    }
    // A client does one append, and will repeat that very request after the restart.
    let mut d = cl.client();
    let append_x = Op::Append {
        key: b("log"),
        value: b("x"),
    };
    assert_eq!(
        d.execute(append_x.clone()).unwrap(),
        Resp::Value(Some(b("x")))
    );
    let d_id = d.id();
    for i in 0..3 {
        cl.kill(i);
    }
    for i in 0..3 {
        cl.start(i);
    }
    cl.wait_leader();
    let mut c2 = cl.client();
    for i in 0..30 {
        assert_eq!(get(&mut c2, &format!("k{i}")), Some(format!("v{i}")));
    }
    // The session table came back with the log: the same client identity
    // re-sending the same (client, seq) is recognised, not applied twice.
    let mut d2 = Client::with_id(cl.servers(), d_id);
    d2.deadline = Duration::from_secs(20);
    assert_eq!(d2.execute(append_x).unwrap(), Resp::Value(Some(b("x"))));
    assert_eq!(
        get(&mut c2, "log").as_deref(),
        Some("x"),
        "applied exactly once"
    );
}

#[test]
fn without_a_majority_nothing_is_served_and_nothing_is_wrong_afterwards() {
    let mut cl = Cluster::new(3);
    let leader = cl.wait_leader();
    let mut c = cl.client();
    put_ok(&mut c, "k", "before");

    // Leave only the old leader running: it still believes it leads, but it
    // can no longer reach a majority.
    let keep = leader as usize - 1;
    for i in 0..3 {
        if i != keep {
            cl.kill(i);
        }
    }
    let mut lonely = cl.client();
    lonely.deadline = Duration::from_secs(3);
    // A read must not be answered from a node that cannot prove it is the leader.
    assert!(matches!(lonely.get(b"k"), Err(ClientError::Unknown)));

    // Bring one back: a majority again, and the committed value is intact.
    let other = (keep + 1) % 3;
    let _ = lonely;
    cl.start(other);
    cl.wait_leader();
    assert_eq!(get(&mut c, "k").as_deref(), Some("before"));
    put_ok(&mut c, "k", "after");
    assert_eq!(get(&mut c, "k").as_deref(), Some("after"));
}

#[test]
fn garbage_on_both_ports_does_not_hurt_the_cluster() {
    let cl = Cluster::new(3);
    cl.wait_leader();
    let mut c = cl.client();
    put_ok(&mut c, "k", "v");

    let terms_before: Vec<u64> = (0..3).map(|i| cl.status_of(i).unwrap().term).collect();
    // A VALID Raft message from a node that is not in the cluster, claiming an
    // enormous term. If it were accepted, every node would jump to that term.
    let intruder = [
        wire::frame(&wire::encode_hello(99)),
        wire::frame(&wire::encode_message(&kvraft::raft::Message::RequestVote {
            term: 1_000_000,
            last_log_index: 1_000,
            last_log_term: 1_000_000,
        })),
    ]
    .concat();

    // A bad hello followed by a perfectly valid, disruptive message.
    let no_hello = [
        wire::frame(b"this is not a hello"),
        wire::frame(&wire::encode_message(&kvraft::raft::Message::RequestVote {
            term: 1_000_000,
            last_log_index: 1_000,
            last_log_term: 1_000_000,
        })),
    ]
    .concat();

    let mut huge = u32::MAX.to_le_bytes().to_vec();
    huge.extend_from_slice(b"whatever");
    let junk: Vec<Vec<u8>> = vec![
        huge,
        wire::frame(b"\xff\xff\xff\xff"),
        wire::frame(&[]),
        b"GET / HTTP/1.1\r\n\r\n".to_vec(),
        [wire::frame(&wire::encode_hello(99)), wire::frame(b"x")].concat(), // not a member
        [
            wire::frame(&wire::encode_hello(2)),
            wire::frame(b"\x05junk"),
        ]
        .concat(), // member, bad message
        intruder,
        no_hello,
        wire::frame(&wire::encode_hello(2))
            .into_iter()
            .chain(5_000_000u32.to_le_bytes())
            .collect(),
    ];
    let mut held = Vec::new();
    for addrs in [&cl.peer, &cl.client] {
        for &a in addrs.iter() {
            for j in &junk {
                if let Ok(mut s) = TcpStream::connect(a) {
                    let _ = s.write_all(j);
                    held.push(s); // keep some connections open and silent
                }
            }
            // And one that connects and says nothing at all.
            if let Ok(s) = TcpStream::connect(a) {
                held.push(s);
            }
        }
    }
    std::thread::sleep(Duration::from_millis(300));

    // A frame claiming to be one byte over the limit must be refused at once,
    // from its length prefix alone: the server must not sit waiting for 16 MiB
    // that will never come (or worse, allocate for it).
    for (name, addr, hello) in [
        (
            "peer",
            cl.peer[0],
            Some(wire::frame(&wire::encode_hello(2))),
        ),
        ("client", cl.client[0], None),
    ] {
        let mut s = TcpStream::connect(addr).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        if let Some(h) = hello {
            s.write_all(&h).unwrap();
        }
        s.write_all(&((wire::MAX_FRAME as u32) + 1).to_le_bytes())
            .unwrap();
        let mut one = [0u8; 1];
        match std::io::Read::read(&mut s, &mut one) {
            Ok(0) => {} // closed cleanly
            Err(e)
                if e.kind() != std::io::ErrorKind::WouldBlock
                    && e.kind() != std::io::ErrorKind::TimedOut => {} // reset
            other => panic!("{name} port kept an over-limit frame open: {other:?}"),
        }
    }

    for i in 0..3 {
        assert!(
            cl.status_of(i).is_some(),
            "node {} stopped answering",
            i + 1
        );
    }
    for (i, before) in terms_before.iter().enumerate() {
        let t = cl.status_of(i).unwrap().term;
        assert!(
            t < 1_000 && t >= *before,
            "node {} jumped to term {t} after a message from a non-member",
            i + 1
        );
    }
    put_ok(&mut c, "k2", "v2");
    assert_eq!(get(&mut c, "k").as_deref(), Some("v"));
    assert_eq!(get(&mut c, "k2").as_deref(), Some("v2"));
    drop(held);
}

#[test]
fn the_cli_works_against_a_real_cluster() {
    let cl = Cluster::new(3);
    cl.wait_leader();
    let servers: Vec<String> = cl
        .servers()
        .iter()
        .map(|(i, a)| format!("{i}={a}"))
        .collect();
    let servers = servers.join(",");
    let run = |args: &[&str]| {
        let out = Command::new(env!("CARGO_BIN_EXE_kvraft-cli"))
            .args(["--servers", &servers])
            .args(args)
            .output()
            .unwrap();
        (
            out.status.code().unwrap(),
            String::from_utf8_lossy(&out.stdout).trim().to_string(),
        )
    };
    assert_eq!(run(&["put", "who", "kene"]), (0, "OK".into()));
    assert_eq!(run(&["get", "who"]), (0, "kene".into()));
    assert_eq!(run(&["get", "nobody"]), (0, "(nil)".into()));
    assert_eq!(run(&["append", "l", "a"]), (0, "a".into()));
    assert_eq!(run(&["append", "l", "b"]), (0, "ab".into()));
    assert_eq!(
        run(&["cas", "who", "kene", "asmodeus"]),
        (0, "swapped".into())
    );
    assert_eq!(run(&["cas", "who", "kene", "x"]), (1, "not swapped".into()));
    assert_eq!(run(&["cas-absent", "who", "x"]), (1, "not swapped".into()));
    assert_eq!(run(&["del", "who"]), (0, "deleted".into()));
    assert_eq!(run(&["del", "who"]), (0, "(not found)".into()));
    let (code, text) = run(&["status"]);
    assert_eq!(code, 0);
    assert_eq!(text.matches("leader,").count(), 1, "{text}");
    assert_eq!(run(&["frobnicate"]).0, 2);
}

// ---------------------------------------------------------------------------
// The big one: real processes, kill -9 during traffic, history checked.
// ---------------------------------------------------------------------------

fn random_op(rng: &mut Rng, n: u64, client: u64) -> Op {
    let key = format!("k{}", rng.below(4)).into_bytes();
    let val = format!("<{client}.{n}>").into_bytes(); // unique per operation
    match rng.below(10) {
        0..=3 => Op::Get { key },
        4..=5 => Op::Put { key, value: val },
        6 => Op::Delete { key },
        7..=8 => Op::Append { key, value: val },
        _ => Op::Cas {
            key,
            expected: if rng.below(2) == 0 {
                None
            } else {
                Some(b("never"))
            },
            new: val,
        },
    }
}

#[test]
fn clients_see_a_linearizable_store_while_nodes_are_killed_and_restarted() {
    let mut cl = Cluster::new(3);
    cl.wait_leader();

    let t0 = Instant::now();
    let stop = Arc::new(AtomicBool::new(false));
    let history: Arc<Mutex<Vec<HistOp>>> = Arc::new(Mutex::new(Vec::new()));

    let mut threads = Vec::new();
    for cid in 0..4u64 {
        let mut client = cl.client();
        client.deadline = Duration::from_secs(25);
        let (stop, history) = (stop.clone(), history.clone());
        threads.push(std::thread::spawn(move || {
            let mut rng = Rng::new(1000 + cid);
            let mut n = 0;
            while !stop.load(Ordering::SeqCst) && n < 1000 {
                n += 1;
                let op = random_op(&mut rng, n, cid);
                let invoke = t0.elapsed().as_micros() as u64;
                let result = client.execute(op.clone());
                let ret = t0.elapsed().as_micros() as u64;
                let (ret, resp) = match result {
                    Ok(r) => (Some(ret), Some(r)),
                    // Unknown outcome: it may have taken effect at any later time.
                    Err(_) => (None, None),
                };
                history.lock().unwrap().push(HistOp {
                    client: client.id(),
                    op,
                    invoke,
                    ret,
                    resp,
                });
                std::thread::sleep(Duration::from_millis(5 + rng.below(20)));
            }
        }));
    }

    // Chaos: one node at a time is killed with SIGKILL and brought back.
    let mut rng = Rng::new(7);
    for round in 0..common::seeds(6) {
        // Every other round targets the leader, so operations really do span a failover.
        let victim = if round % 2 == 0 {
            cl.wait_leader() as usize - 1
        } else {
            rng.below(3) as usize
        };
        cl.kill(victim);
        std::thread::sleep(Duration::from_millis(400 + rng.below(600)));
        cl.start(victim);
        std::thread::sleep(Duration::from_millis(600 + rng.below(600)));
    }
    stop.store(true, Ordering::SeqCst);
    for t in threads {
        t.join().unwrap();
    }

    cl.wait_leader();
    cl.wait_caught_up();

    let h = history.lock().unwrap().clone();
    let slowest = h
        .iter()
        .filter_map(|o| o.ret.map(|r| r - o.invoke))
        .max()
        .unwrap_or(0);
    assert!(
        slowest > 150_000,
        "no operation took longer than {slowest}us, so none spanned a failover; the test is too weak"
    );
    let done = h.iter().filter(|o| o.resp.is_some()).count();
    let pending = h.len() - done;
    assert!(
        done > 100,
        "only {done} operations completed; the test is too weak"
    );
    assert!(pending <= 6, "{pending} operations never got an answer");
    match check(&h) {
        Verdict::Linearizable => {}
        Verdict::Violation { key, detail } => panic!(
            "NOT LINEARIZABLE on key {:?}\n{detail}",
            String::from_utf8_lossy(&key)
        ),
        Verdict::Inconclusive { key } => {
            panic!("checker gave up on key {:?}", String::from_utf8_lossy(&key))
        }
    }
    eprintln!("checked {done} completed and {pending} pending operations");
}
