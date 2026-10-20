# kvraft

A replicated key-value store built on Raft, in Rust, built from scratch as a distributed systems learning
project. The consensus core, state machine, simulators and checker use only `std`; the network driver
(stage 4) adds one dependency, `tokio`.

## Design rule

The Raft core (`src/raft.rs`) is a **state machine with no I/O of its own**: no sockets, no threads, no clock.
It is driven by `tick()`, `step(from, msg)` and `propose(data)`, and it reads and writes durable state only
through a `Storage` trait. Outputs are `drain_messages()` and `drain_committed()`.

That makes a whole cluster reproducible from one seed, including crashes.

## Layout

```
src/raft.rs        Raft core: elections (with PreVote), replication, CheckQuorum
src/storage.rs     Storage trait, MemStorage, FileStorage (CRC-framed write-ahead log)
src/sim.rs         deterministic Raft-only simulator: delay, loss, partitions, crash/restart
src/kv.rs          KV state machine: command codec, client sessions (exactly-once), digest
src/server.rs      one replica: Raft node + KV + request/reply glue, ReadIndex reads
src/kvsim.rs       deterministic whole-service simulator: replicas, lossy network, retrying clients
src/linearizability.rs  Wing-Gong/Lowe checker with an independent model
src/wire.rs        bytes on the network: framing, Raft message codec, client request/reply codec
src/net.rs         tokio driver: one actor owns the KvServer; peer links, client connections, ticker
src/client.rs      blocking std-only client: redirects, retries with the same (client, seq)
src/bin/kvraft-server.rs  one node as a process
src/bin/kvraft-cli.rs     command-line client (put/get/del/append/cas/status)
scripts/run-local-cluster.sh  start/stop a 3-node cluster on localhost (Unix)
tests/raft_sim.rs      scenario tests + partition/loss fuzz
tests/raft_churn.rs    leader-churn fuzz
tests/raft_crash.rs    restart tests + two crash fuzzes
tests/raft_prevote.rs  PreVote and CheckQuorum
tests/storage_wal.rs   WAL format, torn writes at every byte, corruption
tests/kv_state_machine.rs   codec and session semantics
tests/linearizability_checker.rs  the checker tested on its own (accepts, rejects, cross-check)
tests/kv_server.rs     server and cluster scenarios, including the deposed-leader read
tests/kv_linearizable.rs  chaos fuzz: partitions + crashes + loss, history must be linearizable
tests/wire_codec.rs    codecs: round trips, truncation, garbage never panics
tests/client_args.rs   address-list parsing
tests/cluster_process.rs  REAL processes: kill -9, full restart, garbage input, CLI, chaos + checker
tests/common/mod.rs    shared helpers
```

## What the simulator asserts on every tick

- **Election safety**: at most one leader per term.
- **State machine safety**: a committed entry is never different on any node, ever.
- **Leader completeness**: a leader of term T holds every entry committed before T.

## Status

| Stage | Scope | State |
|---|---|---|
| 1 | Election, replication, commit, simulator, fuzz | done |
| 2 | Durable state (WAL), crash/restart in sim, PreVote, CheckQuorum | done |
| 3 | KV state machine, linearizable reads/writes, client request dedup | done |
| 4 | tokio + TCP transport, multi-process cluster, CLI client | done |
| 5 | Snapshots, log compaction, membership changes | |

## Running a real cluster

Three terminals (Windows PowerShell and Unix are the same; use `--release` for speed). Run each from the
project folder:

```
cargo run --release --bin kvraft-server -- --id 1 --peers 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003 --client-listen 127.0.0.1:8001 --data-dir data/node1
cargo run --release --bin kvraft-server -- --id 2 --peers 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003 --client-listen 127.0.0.1:8002 --data-dir data/node2
cargo run --release --bin kvraft-server -- --id 3 --peers 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003 --client-listen 127.0.0.1:8003 --data-dir data/node3
```

Then in a fourth terminal:

```
# Unix:        export KVRAFT_SERVERS=1=127.0.0.1:8001,2=127.0.0.1:8002,3=127.0.0.1:8003
# PowerShell:  $env:KVRAFT_SERVERS="1=127.0.0.1:8001,2=127.0.0.1:8002,3=127.0.0.1:8003"
cargo run --release --bin kvraft-cli -- status
cargo run --release --bin kvraft-cli -- put name kene
cargo run --release --bin kvraft-cli -- get name
```

Now press Ctrl-C in the terminal of the node that `status` shows as leader, run `status` again (a new leader
appears within a second or two), `get name` (still there), then start that node again and watch it catch up.
On Unix, `./scripts/run-local-cluster.sh` starts all three in the background (`... stop` kills them).

## Running the tests

```
cargo test                  # about 100 tests, ~1 min (first build downloads tokio), about 20 s
cargo test --release
```

Heavy randomized sweep (1000 seeds per fuzz test):

```
KVRAFT_FUZZ_SEEDS=1000 cargo test --release          # macOS / Linux
$env:KVRAFT_FUZZ_SEEDS=1000; cargo test --release    # Windows PowerShell
```

## Known gaps

- The Figure 8 safety rule (commit only current-term entries by counting replicas) is implemented but **not
  covered by any test**: deleting it does not make a test fail.
- The WAL grows without bound (compaction arrives with snapshots, stage 5).
- On Windows, `FileStorage` does not fsync the directory entry of a newly created WAL file.
- A corrupted length field mid-file is indistinguishable from a torn tail.
- Any I/O error panics (fail-stop, deliberately).
- The simulator crashes nodes between ticks and right after a message is handled, not between the two
  storage calls inside one save, and its in-memory storage never loses data.
- Stage 3 mutation results (13 deliberate bugs): 10 caught. The 3 that survive are *equivalent mutants*:
  - "read index = 0" and "serve a read before applied >= index": `KvServer::pump` always applies every
    committed entry before it answers, so the check is a defensive guard that can never be false here.
  - "pending reads survive step-down": they are also cleared in `become_leader`, and `advance_reads` only
    runs on a leader, so they can never be promoted while a follower.
- Stage 4 limits:
  - No TLS and no authentication: anyone who can reach a port can speak the protocol. Bind to localhost
    or a private network only.
  - The cluster membership is fixed at start (`--peers`); changing it is stage 5.
  - The WAL is fsynced from the actor thread, so a slow disk stalls the whole node (the safe failure).
  - A client request abandoned by its connection stays in the actor's reply table until the server
    answers it, which it always eventually does (answer, or Retry once leadership is lost).
  - `tests/cluster_process.rs` uses real ports and processes and takes several seconds.
  - Stage 4 mutation results (7 deliberate bugs): 6 caught. The survivor, "client accepts a reply whose
    seq does not match", guards a state that cannot occur (one request per connection at a time).
- Sessions are never expired and allow one outstanding request per client.
- The simulator's clients are in-process; there is no real network until stage 4.
- Stage 3 has its own simulator (`kvsim.rs`) beside `sim.rs` instead of one generic simulator. It checks
  election safety and replica agreement, not Raft's leader-completeness invariant.

## Why a no-op on election?

A new leader appends an empty entry in its own term. Raft only lets a leader commit by counting replicas
for entries of its *current* term (paper section 5.4.2); committing the no-op transitively commits
everything before it. See `become_leader`.
