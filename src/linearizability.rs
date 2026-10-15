//! A linearizability checker for the key-value store.
//!
//! A history is linearizable if every operation can be given a single instant,
//! somewhere between its invocation and its response, such that executing the
//! operations in the order of those instants on a plain sequential store gives
//! exactly the responses the clients saw. In other words: the system behaved
//! like one machine that ran each operation atomically.
//!
//! # Method
//!
//! Wing & Gong's search with Lowe's memoization:
//!   * Operations on different keys never interact, so each key is checked on
//!     its own (linearizability is *local*, Herlihy & Wing 1990).
//!   * Depth-first: repeatedly pick an operation that is allowed to go next
//!     (every operation that finished before it started is already placed),
//!     check its recorded response against the model, recurse.
//!   * The outcome depends only on (set of placed operations, model state), so
//!     failed combinations are remembered and never explored twice.
//!
//! # Operations without a response
//!
//! A client that never heard back (it crashed, or the run ended) does not know
//! whether its operation took effect. Such an operation may be placed anywhere
//! after its invocation, or not at all. Pending reads have no effect and no
//! observed value, so they are dropped.
//!
//! # The model is deliberately separate from `Kv`
//!
//! `model_step` below re-states the store's semantics on its own. If it shared
//! code with `kv.rs`, a bug in `kv.rs` would be baked into the oracle and the
//! checker could never see it.

use crate::kv::{Op, Resp};
use std::collections::{BTreeMap, HashSet};

/// One client operation as observed from the outside.
#[derive(Clone, Debug)]
pub struct HistOp {
    pub client: u64,
    pub op: Op,
    /// Time of the first attempt to send it.
    pub invoke: u64,
    /// Time the client got its answer; `None` if it never did.
    pub ret: Option<u64>,
    /// The answer; `Some` exactly when `ret` is `Some`.
    pub resp: Option<Resp>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Verdict {
    Linearizable,
    /// No valid ordering exists for the operations on `key`.
    Violation {
        key: Vec<u8>,
        detail: String,
    },
    /// The search budget ran out before it could decide.
    Inconclusive {
        key: Vec<u8>,
    },
}

/// Default budget: search steps per key.
pub const DEFAULT_LIMIT: u64 = 20_000_000;

pub fn check(history: &[HistOp]) -> Verdict {
    check_with_limit(history, DEFAULT_LIMIT)
}

pub fn check_with_limit(history: &[HistOp], limit: u64) -> Verdict {
    let mut by_key: BTreeMap<Vec<u8>, Vec<&HistOp>> = BTreeMap::new();
    for h in history {
        let pending_read = h.resp.is_none() && matches!(h.op, Op::Get { .. });
        if !pending_read {
            by_key.entry(h.op.key().to_vec()).or_default().push(h);
        }
    }
    for (key, mut ops) in by_key {
        ops.sort_by_key(|h| (h.invoke, h.ret.unwrap_or(u64::MAX), h.client));
        match check_key(&ops, limit) {
            Ok(true) => {}
            Ok(false) => {
                let detail = describe(&ops);
                return Verdict::Violation { key, detail };
            }
            Err(()) => return Verdict::Inconclusive { key },
        }
    }
    Verdict::Linearizable
}

// ---------------------------------------------------------------------------
// The sequential model: one register per key.
// ---------------------------------------------------------------------------

type State = Option<Vec<u8>>;

/// Run `op` against `state`. Returns the new state if the recorded response
/// (`None` = no response seen) is what a sequential store would have answered,
/// or `None` if it is not.
fn model_step(state: &State, op: &Op, resp: Option<&Resp>) -> Option<State> {
    let ok = |expected: Resp| resp.is_none_or(|r| *r == expected);
    match op {
        Op::Put { value, .. } => ok(Resp::Ok).then(|| Some(value.clone())),
        Op::Get { .. } => ok(Resp::Value(state.clone())).then(|| state.clone()),
        Op::Delete { .. } => ok(Resp::Deleted(state.is_some())).then_some(None),
        Op::Append { value, .. } => {
            let mut v = state.clone().unwrap_or_default();
            v.extend_from_slice(value);
            let next = Some(v);
            ok(Resp::Value(next.clone())).then_some(next)
        }
        Op::Cas { expected, new, .. } => {
            let matches = state == expected;
            let next = if matches {
                Some(new.clone())
            } else {
                state.clone()
            };
            ok(Resp::Cas(matches)).then_some(next)
        }
    }
}

// ---------------------------------------------------------------------------
// Search
// ---------------------------------------------------------------------------

#[derive(Clone, PartialEq, Eq, Hash)]
struct Bits(Vec<u64>);

impl Bits {
    fn new(n: usize) -> Self {
        Bits(vec![0; n.div_ceil(64)])
    }
    fn has(&self, i: usize) -> bool {
        self.0[i / 64] & (1 << (i % 64)) != 0
    }
    fn set(&mut self, i: usize) {
        self.0[i / 64] |= 1 << (i % 64);
    }
    fn clear(&mut self, i: usize) {
        self.0[i / 64] &= !(1 << (i % 64));
    }
    fn subset_of(&self, other: &Bits) -> bool {
        self.0.iter().zip(&other.0).all(|(a, b)| a & !b == 0)
    }
}

struct Search<'a> {
    ops: &'a [&'a HistOp],
    /// before[i]: the operations that finished strictly before op i started,
    /// so they must be placed before it.
    before: Vec<Bits>,
    memo: HashSet<(Bits, State)>,
    steps: u64,
    limit: u64,
}

fn check_key(ops: &[&HistOp], limit: u64) -> Result<bool, ()> {
    let n = ops.len();
    let mut before = vec![Bits::new(n); n];
    for (i, b) in before.iter_mut().enumerate() {
        for (j, other) in ops.iter().enumerate() {
            if j != i && other.ret.is_some_and(|r| r < ops[i].invoke) {
                b.set(j);
            }
        }
    }
    let completed = ops.iter().filter(|h| h.resp.is_some()).count();
    let mut s = Search {
        ops,
        before,
        memo: HashSet::new(),
        steps: 0,
        limit,
    };
    s.dfs(&mut Bits::new(n), &None, completed)
}

impl Search<'_> {
    fn dfs(&mut self, done: &mut Bits, state: &State, remaining: usize) -> Result<bool, ()> {
        if remaining == 0 {
            return Ok(true); // anything still pending simply never took effect
        }
        self.steps += 1;
        if self.steps > self.limit {
            return Err(());
        }
        if !self.memo.insert((done.clone(), state.clone())) {
            return Ok(false); // already explored from here and it failed
        }
        for i in 0..self.ops.len() {
            if done.has(i) || !self.before[i].subset_of(done) {
                continue;
            }
            let op = self.ops[i];
            let Some(next) = model_step(state, &op.op, op.resp.as_ref()) else {
                continue;
            };
            done.set(i);
            let rem = remaining - usize::from(op.resp.is_some());
            let found = self.dfs(done, &next, rem);
            done.clear(i);
            if found? {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

// ---------------------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------------------

fn show(b: &[u8]) -> String {
    match std::str::from_utf8(b) {
        Ok(s) if s.chars().all(|c| !c.is_control()) => format!("{s:?}"),
        _ => format!("{b:?}"),
    }
}

fn show_opt(v: &Option<Vec<u8>>) -> String {
    v.as_ref().map_or("absent".to_string(), |b| show(b))
}

fn show_op(op: &Op) -> String {
    match op {
        Op::Put { value, .. } => format!("Put({})", show(value)),
        Op::Get { .. } => "Get".to_string(),
        Op::Delete { .. } => "Delete".to_string(),
        Op::Append { value, .. } => format!("Append({})", show(value)),
        Op::Cas { expected, new, .. } => {
            format!("Cas({} -> {})", show_opt(expected), show(new))
        }
    }
}

fn show_resp(r: &Resp) -> String {
    match r {
        Resp::Ok => "Ok".to_string(),
        Resp::Value(v) => format!("Value({})", show_opt(v)),
        Resp::Deleted(b) => format!("Deleted({b})"),
        Resp::Cas(b) => format!("Cas({b})"),
        Resp::Stale => "Stale".to_string(),
    }
}

fn describe(ops: &[&HistOp]) -> String {
    let mut out = format!(
        "no valid order exists for key {}; its {} operation(s) were:\n",
        show(ops[0].op.key()),
        ops.len()
    );
    for h in ops.iter().take(80) {
        let ret = h.ret.map_or("never".to_string(), |r| r.to_string());
        let resp = h
            .resp
            .as_ref()
            .map_or("(no response)".to_string(), show_resp);
        out.push_str(&format!(
            "  client {:>2}  [{:>5}, {:>5}]  {}  ->  {}\n",
            h.client,
            h.invoke,
            ret,
            show_op(&h.op),
            resp
        ));
    }
    if ops.len() > 80 {
        out.push_str(&format!("  ... and {} more\n", ops.len() - 80));
    }
    out
}
