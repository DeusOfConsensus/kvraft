//! Durable storage for the state Raft must not forget across a crash.
//!
//! Raft's safety depends on three things surviving a restart:
//!   * `current_term`  - so a node never goes back to an older term
//!   * `voted_for`     - so a node never votes twice in one term
//!   * the log         - so a node never forgets an entry it acknowledged
//!
//! CONTRACT: when a `Storage` method returns, the data is durable (it will be
//! there after a crash). Implementations fail-stop (panic) on I/O errors: a
//! Raft node that cannot persist must stop, not keep voting and acknowledging.
//!
//! Two implementations:
//!   * `MemStorage`  - in memory; the simulator uses it, and "crash" means the
//!     `Node` is dropped while its `MemStorage` is kept.
//!   * `FileStorage` - a write-ahead log (WAL) on disk.

use crate::raft::{Entry, Index, NodeId, Term};
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// The small piece of state that changes on elections.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HardState {
    pub term: Term,
    pub voted_for: Option<NodeId>,
}

pub trait Storage {
    /// Everything durably saved so far. Safe to call more than once.
    fn load(&mut self) -> (HardState, Vec<Entry>);

    /// Durably record the current term and vote.
    fn save_hard_state(&mut self, hs: HardState);

    /// Durably replace the log suffix: keep entries `1..from`, drop the rest,
    /// then append `entries` starting at index `from`. `from` is 1-based and
    /// must be at most `len + 1` (equal to `len + 1` is a plain append).
    fn replace_log_suffix(&mut self, from: Index, entries: &[Entry]);
}

// ---------------------------------------------------------------------------
// In-memory storage
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Default)]
pub struct MemStorage {
    hs: HardState,
    log: Vec<Entry>,
}

impl MemStorage {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Storage for MemStorage {
    fn load(&mut self) -> (HardState, Vec<Entry>) {
        (self.hs, self.log.clone())
    }

    fn save_hard_state(&mut self, hs: HardState) {
        self.hs = hs;
    }

    fn replace_log_suffix(&mut self, from: Index, entries: &[Entry]) {
        assert!(from >= 1, "log indices are 1-based");
        assert!(
            from as usize <= self.log.len() + 1,
            "suffix would leave a hole in the log"
        );
        self.log.truncate((from - 1) as usize);
        self.log.extend_from_slice(entries);
    }
}

// ---------------------------------------------------------------------------
// CRC32 (IEEE 802.3), no dependencies
// ---------------------------------------------------------------------------

const fn make_crc_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut c = i as u32;
        let mut k = 0;
        while k < 8 {
            c = if c & 1 != 0 {
                0xEDB8_8320 ^ (c >> 1)
            } else {
                c >> 1
            };
            k += 1;
        }
        table[i] = c;
        i += 1;
    }
    table
}

static CRC_TABLE: [u32; 256] = make_crc_table();

pub fn crc32(data: &[u8]) -> u32 {
    let mut c = !0u32;
    for &b in data {
        c = CRC_TABLE[((c ^ b as u32) & 0xFF) as usize] ^ (c >> 8);
    }
    !c
}

// ---------------------------------------------------------------------------
// WAL on disk
// ---------------------------------------------------------------------------
//
// The file is a sequence of records, each framed as:
//
//     [len: u32 LE][crc32 of payload: u32 LE][payload: len bytes]
//
// Payload kinds (first byte):
//     1  HardState   term:u64, has_vote:u8, vote:u64
//     2  Suffix      from:u64, count:u32, then per entry: term:u64, dlen:u32, data
//
// A "Suffix" record is truncate-then-append in ONE record, so the pair can
// never be half-applied: a record is either wholly present (checksum good) or
// ignored.
//
// RECOVERY RULE. A crash mid-write leaves a partial LAST record. On load we
// replay records until one is incomplete or has a bad checksum AND ends at
// end-of-file; that tail is treated as never written and cut off. A bad
// checksum with more data AFTER it is not a torn write, it is corruption of
// data that was once acknowledged, so we panic rather than silently drop it.
//
// KNOWN LIMITATION: a corrupted length field in the middle of the file can
// make a record look like it runs past end-of-file, which is indistinguishable
// from a torn tail. Real systems add per-block checksums or a separate header
// for this; we note it and move on.
//
// The WAL also grows without bound. Snapshots and compaction come in stage 5.

const REC_HARD_STATE: u8 = 1;
const REC_SUFFIX: u8 = 2;
const FRAME_HEADER: usize = 8;

pub struct FileStorage {
    path: PathBuf,
    file: File,
    loaded: bool,
}

impl FileStorage {
    /// Open (creating if needed) the WAL at `path`. Call `load` before writing.
    pub fn open(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let existed = path.exists();
        // Not append mode on purpose: truncating a torn tail needs write access,
        // which Windows' append mode does not grant. We manage the position
        // ourselves and always write at the end.
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        if !existed {
            sync_parent_dir(&path)?;
        }
        Ok(FileStorage {
            path,
            file,
            loaded: false,
        })
    }

    fn append_record(&mut self, payload: &[u8]) {
        assert!(self.loaded, "FileStorage: call load() before writing");
        let mut rec = Vec::with_capacity(FRAME_HEADER + payload.len());
        rec.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        rec.extend_from_slice(&crc32(payload).to_le_bytes());
        rec.extend_from_slice(payload);
        // One write call, then fsync: when we return, the record is on disk.
        self.file.write_all(&rec).expect("WAL write failed");
        self.file.sync_data().expect("WAL fsync failed");
    }
}

#[cfg(unix)]
fn sync_parent_dir(path: &Path) -> std::io::Result<()> {
    // A new file's directory entry must itself be fsynced to survive a crash.
    if let Some(parent) = path.parent() {
        let dir = if parent.as_os_str().is_empty() {
            Path::new(".")
        } else {
            parent
        };
        File::open(dir)?.sync_all()?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn sync_parent_dir(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> &'a [u8] {
        let end = self.pos + n;
        assert!(
            end <= self.buf.len(),
            "malformed WAL record despite a valid checksum"
        );
        let s = &self.buf[self.pos..end];
        self.pos = end;
        s
    }
    fn u8(&mut self) -> u8 {
        self.take(1)[0]
    }
    fn u32(&mut self) -> u32 {
        u32::from_le_bytes(self.take(4).try_into().unwrap())
    }
    fn u64(&mut self) -> u64 {
        u64::from_le_bytes(self.take(8).try_into().unwrap())
    }
}

fn apply_record(payload: &[u8], hs: &mut HardState, log: &mut Vec<Entry>) {
    let mut r = Reader {
        buf: payload,
        pos: 0,
    };
    match r.u8() {
        REC_HARD_STATE => {
            let term = r.u64();
            let has_vote = r.u8();
            let vote = r.u64();
            *hs = HardState {
                term,
                voted_for: if has_vote == 1 { Some(vote) } else { None },
            };
        }
        REC_SUFFIX => {
            let from = r.u64();
            let count = r.u32();
            assert!(
                from >= 1 && from as usize <= log.len() + 1,
                "WAL suffix record leaves a hole in the log"
            );
            log.truncate((from - 1) as usize);
            for _ in 0..count {
                let term = r.u64();
                let dlen = r.u32() as usize;
                let data = r.take(dlen).to_vec();
                log.push(Entry { term, data });
            }
        }
        other => panic!("unknown WAL record type {other}"),
    }
    assert_eq!(r.pos, payload.len(), "trailing bytes in WAL record");
}

impl Storage for FileStorage {
    fn load(&mut self) -> (HardState, Vec<Entry>) {
        let bytes = std::fs::read(&self.path).expect("WAL read failed");
        let mut hs = HardState::default();
        let mut log = Vec::new();
        let mut pos = 0usize;

        while pos < bytes.len() {
            if bytes.len() - pos < FRAME_HEADER {
                break; // torn header
            }
            let len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
            let crc = u32::from_le_bytes(bytes[pos + 4..pos + 8].try_into().unwrap());
            let end = pos.saturating_add(FRAME_HEADER).saturating_add(len);
            if end > bytes.len() {
                break; // torn payload
            }
            let payload = &bytes[pos + FRAME_HEADER..end];
            if crc32(payload) != crc {
                if end == bytes.len() {
                    break; // garbled final record: treat as torn
                }
                panic!(
                    "WAL corrupted at byte offset {pos} with data after it; \
                     refusing to continue rather than drop acknowledged records"
                );
            }
            apply_record(payload, &mut hs, &mut log);
            pos = end;
        }

        if pos < bytes.len() {
            // Cut off the torn tail so new records don't follow garbage.
            self.file.set_len(pos as u64).expect("WAL truncate failed");
            self.file.sync_data().expect("WAL fsync failed");
        }
        self.file
            .seek(SeekFrom::Start(pos as u64))
            .expect("WAL seek failed");
        self.loaded = true;
        (hs, log)
    }

    fn save_hard_state(&mut self, hs: HardState) {
        let mut p = Vec::with_capacity(18);
        p.push(REC_HARD_STATE);
        p.extend_from_slice(&hs.term.to_le_bytes());
        match hs.voted_for {
            Some(v) => {
                p.push(1);
                p.extend_from_slice(&v.to_le_bytes());
            }
            None => {
                p.push(0);
                p.extend_from_slice(&0u64.to_le_bytes());
            }
        }
        self.append_record(&p);
    }

    fn replace_log_suffix(&mut self, from: Index, entries: &[Entry]) {
        assert!(from >= 1, "log indices are 1-based");
        let mut p = Vec::new();
        p.push(REC_SUFFIX);
        p.extend_from_slice(&from.to_le_bytes());
        p.extend_from_slice(&(entries.len() as u32).to_le_bytes());
        for e in entries {
            p.extend_from_slice(&e.term.to_le_bytes());
            p.extend_from_slice(&(e.data.len() as u32).to_le_bytes());
            p.extend_from_slice(&e.data);
        }
        self.append_record(&p);
    }
}
