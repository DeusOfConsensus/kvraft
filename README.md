# kvraft

A replicated key-value store built on Raft, in Rust, built from scratch as a distributed systems project.

## Design rule

The Raft core (`src/raft.rs`) is a **pure state machine**: no sockets, threads, disk or wall clock.
Inputs are `tick()`, `step(from, msg)` and `propose(data)`; outputs are `drain_messages()` and `drain_committed()`.

That makes the whole cluster reproducible from one seed. `src/sim.rs` runs N nodes over a simulated
network (random 1-3 tick latency, message loss, node isolation) and asserts after every tick:

- **Election safety**: at most one leader per term.
- **State machine safety**: a committed log entry is never different on any node, ever.

A failing seed replays exactly. Run `cargo test` to see it.

## Why a no-op on election?

A new leader appends an empty entry in its own term. Raft only lets a leader commit by counting replicas
for entries of its *current* term (paper section 5.4.2); committing the no-op transitively commits
everything before it. See `become_leader`.
