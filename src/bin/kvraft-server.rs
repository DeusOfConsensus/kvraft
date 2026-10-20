//! One kvraft node.
//!
//!   kvraft-server --id 1 \
//!       --peers 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003 \
//!       --client-listen 127.0.0.1:8001 \
//!       --data-dir ./data1
//!
//! `--peers` is the SAME list on every node and includes this node: its own
//! entry is the address it listens on for the other nodes.

use kvraft::client::parse_addr_map;
use kvraft::net::{NodeConfig, run};
use kvraft::raft::Config;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const USAGE: &str = "usage: kvraft-server --id N --peers 1=HOST:PORT,2=HOST:PORT,... \
--client-listen HOST:PORT --data-dir DIR [--tick-ms 50]";

fn fail(msg: &str) -> ! {
    eprintln!("error: {msg}\n{USAGE}");
    std::process::exit(2);
}

fn main() {
    let mut id = None;
    let mut peers = None;
    let mut client_listen: Option<SocketAddr> = None;
    let mut data_dir = None;
    let mut tick_ms = 50u64;

    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let mut value = || {
            args.next()
                .unwrap_or_else(|| fail(&format!("{flag} needs a value")))
        };
        match flag.as_str() {
            "--id" => id = Some(value().parse::<u64>().unwrap_or_else(|_| fail("bad --id"))),
            "--peers" => peers = Some(parse_addr_map(&value()).unwrap_or_else(|e| fail(&e))),
            "--client-listen" => {
                client_listen = Some(
                    value()
                        .parse()
                        .unwrap_or_else(|_| fail("bad --client-listen")),
                )
            }
            "--data-dir" => data_dir = Some(PathBuf::from(value())),
            "--tick-ms" => tick_ms = value().parse().unwrap_or_else(|_| fail("bad --tick-ms")),
            "-h" | "--help" => {
                println!("{USAGE}");
                return;
            }
            other => fail(&format!("unknown argument {other}")),
        }
    }
    let id = id.unwrap_or_else(|| fail("--id is required"));
    let mut peers = peers.unwrap_or_else(|| fail("--peers is required"));
    let client_listen = client_listen.unwrap_or_else(|| fail("--client-listen is required"));
    let data_dir = data_dir.unwrap_or_else(|| fail("--data-dir is required"));
    if tick_ms == 0 {
        fail("--tick-ms must be positive");
    }
    let Some(peer_listen) = peers.remove(&id) else {
        fail("--peers must include this node's own id");
    };

    // Election timers are randomized per process, so two nodes started in the
    // same instant (or restarted together) still desynchronize.
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(1, |d| d.as_nanos() as u64);
    let cfg = NodeConfig {
        id,
        peers,
        data_dir,
        tick: Duration::from_millis(tick_ms),
        raft: Config {
            seed: nanos ^ id.wrapping_mul(0x9E37_79B9_7F4A_7C15),
            ..Config::default()
        },
    };

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let result = rt.block_on(async {
        let peer_l = tokio::net::TcpListener::bind(peer_listen).await?;
        let client_l = tokio::net::TcpListener::bind(client_listen).await?;
        eprintln!(
            "kvraft-server {id}: peers on {}, clients on {}",
            peer_l.local_addr()?,
            client_l.local_addr()?
        );
        run(cfg, peer_l, client_l).await
    });
    if let Err(e) = result {
        eprintln!("kvraft-server {id}: fatal: {e}");
        std::process::exit(1);
    }
}
