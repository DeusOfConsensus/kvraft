//! Command-line client.
//!
//!   kvraft-cli --servers 1=127.0.0.1:8001,2=127.0.0.1:8002,3=127.0.0.1:8003 put name kene
//!   kvraft-cli --servers ... get name
//!
//! The server list can also come from the KVRAFT_SERVERS environment variable.

use kvraft::client::{Client, ClientError, parse_addr_map, status};
use kvraft::kv::{Op, Resp};
use kvraft::raft::Role;
use std::time::Duration;

const USAGE: &str = "usage: kvraft-cli [--servers 1=HOST:PORT,...] COMMAND
commands:
  put KEY VALUE            store VALUE under KEY
  get KEY                  print the value, or (nil)
  del KEY                  delete; prints whether it existed
  append KEY VALUE         append; prints the new whole value
  cas KEY EXPECTED NEW     set NEW only if the value is EXPECTED
  cas-absent KEY NEW       set NEW only if KEY does not exist
  status                   role, term and leader of every server";

fn fail(msg: &str) -> ! {
    eprintln!("error: {msg}\n{USAGE}");
    std::process::exit(2);
}

fn show(v: &[u8]) -> String {
    String::from_utf8_lossy(v).into_owned()
}

fn main() {
    let mut servers = std::env::var("KVRAFT_SERVERS").ok();
    let mut rest: Vec<String> = Vec::new();
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--servers" => {
                servers = Some(
                    args.next()
                        .unwrap_or_else(|| fail("--servers needs a value")),
                )
            }
            "-h" | "--help" => {
                println!("{USAGE}");
                return;
            }
            _ => rest.push(a),
        }
    }
    let servers = servers.unwrap_or_else(|| fail("give --servers or set KVRAFT_SERVERS"));
    let map = parse_addr_map(&servers).unwrap_or_else(|e| fail(&e));
    let Some((cmd, args)) = rest.split_first() else {
        fail("missing command");
    };
    let b = |s: &String| s.as_bytes().to_vec();

    let op = match (cmd.as_str(), args) {
        ("status", []) => {
            let mut any = false;
            for (id, addr) in &map {
                match status(*addr, Duration::from_secs(1)) {
                    Ok(st) => {
                        any = true;
                        let role = match st.role {
                            Role::Leader => "leader",
                            Role::Follower => "follower",
                            Role::Candidate => "candidate",
                            Role::PreCandidate => "pre-candidate",
                        };
                        println!(
                            "node {id} @ {addr}: {role}, term {}, leader {}, commit {}, applied {}",
                            st.term,
                            st.leader.map_or("none".to_string(), |l| l.to_string()),
                            st.commit_index,
                            st.last_applied
                        );
                    }
                    Err(e) => println!("node {id} @ {addr}: unreachable ({e})"),
                }
            }
            std::process::exit(if any { 0 } else { 1 });
        }
        ("put", [k, v]) => Op::Put {
            key: b(k),
            value: b(v),
        },
        ("get", [k]) => Op::Get { key: b(k) },
        ("del", [k]) => Op::Delete { key: b(k) },
        ("append", [k, v]) => Op::Append {
            key: b(k),
            value: b(v),
        },
        ("cas", [k, e, n]) => Op::Cas {
            key: b(k),
            expected: Some(b(e)),
            new: b(n),
        },
        ("cas-absent", [k, n]) => Op::Cas {
            key: b(k),
            expected: None,
            new: b(n),
        },
        _ => fail(&format!("bad command or wrong number of arguments: {cmd}")),
    };

    let mut client = Client::new(map.into_iter().collect());
    match client.execute(op) {
        Ok(Resp::Ok) => println!("OK"),
        Ok(Resp::Value(Some(v))) => println!("{}", show(&v)),
        Ok(Resp::Value(None)) => println!("(nil)"),
        Ok(Resp::Deleted(d)) => println!("{}", if d { "deleted" } else { "(not found)" }),
        Ok(Resp::Cas(true)) => println!("swapped"),
        Ok(Resp::Cas(false)) => {
            println!("not swapped");
            std::process::exit(1);
        }
        Ok(Resp::Stale) | Err(ClientError::Stale) => {
            eprintln!("error: stale request");
            std::process::exit(4);
        }
        Err(e @ ClientError::Unknown) => {
            eprintln!("error: {e}");
            std::process::exit(3);
        }
    }
}
