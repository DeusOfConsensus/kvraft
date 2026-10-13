use kvraft::raft::Entry;
use kvraft::storage::{FileStorage, HardState, MemStorage, Storage, crc32};
use std::path::PathBuf;

/// Unique scratch file per test, removed on drop. No external crates needed.
struct TempWal(PathBuf);

impl TempWal {
    fn new(name: &str) -> Self {
        let p = std::env::temp_dir().join(format!("kvraft-{}-{}.wal", std::process::id(), name));
        let _ = std::fs::remove_file(&p);
        TempWal(p)
    }
}

impl Drop for TempWal {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn e(term: u64, data: &[u8]) -> Entry {
    Entry {
        term,
        data: data.to_vec(),
    }
}

#[test]
fn crc32_matches_the_standard_check_value() {
    // The universally published CRC-32/IEEE check value.
    assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    assert_eq!(crc32(b""), 0);
}

#[test]
fn mem_storage_round_trip_and_suffix_replace() {
    let mut s = MemStorage::new();
    s.save_hard_state(HardState {
        term: 3,
        voted_for: Some(2),
    });
    s.replace_log_suffix(1, &[e(1, b"a"), e(1, b"b"), e(2, b"c")]);
    s.replace_log_suffix(3, &[e(3, b"C")]); // overwrite the tail
    let (hs, log) = s.load();
    assert_eq!(hs.term, 3);
    assert_eq!(hs.voted_for, Some(2));
    assert_eq!(log, vec![e(1, b"a"), e(1, b"b"), e(3, b"C")]);
}

#[test]
fn file_storage_survives_reopen() {
    let t = TempWal::new("reopen");
    {
        let mut s = FileStorage::open(&t.0).unwrap();
        assert_eq!(s.load(), (HardState::default(), vec![]));
        s.save_hard_state(HardState {
            term: 5,
            voted_for: Some(7),
        });
        s.replace_log_suffix(1, &[e(1, b"x"), e(2, b"y")]);
        s.replace_log_suffix(2, &[e(5, b"Y"), e(5, b"z")]);
        s.save_hard_state(HardState {
            term: 6,
            voted_for: None,
        });
    }
    let mut s = FileStorage::open(&t.0).unwrap();
    let (hs, log) = s.load();
    assert_eq!(
        hs,
        HardState {
            term: 6,
            voted_for: None
        }
    );
    assert_eq!(log, vec![e(1, b"x"), e(5, b"Y"), e(5, b"z")]);
}

/// The key durability property: if the process dies mid-write, the file can end
/// at ANY byte. Whatever the cut point, load must succeed and return exactly the
/// state after the last record that was written completely, never garbage and
/// never a half-applied record.
#[test]
fn torn_write_at_every_byte_recovers_to_last_complete_record() {
    let t = TempWal::new("torn");
    let mut checkpoints: Vec<(u64, HardState, Vec<Entry>)> = Vec::new();

    {
        let mut s = FileStorage::open(&t.0).unwrap();
        s.load();
        let mut log: Vec<Entry> = Vec::new();
        let len = |p: &PathBuf| std::fs::metadata(p).unwrap().len();

        let mut hs = HardState {
            term: 1,
            voted_for: Some(1),
        };
        s.save_hard_state(hs);
        checkpoints.push((len(&t.0), hs, log.clone()));

        log.extend([e(1, b"alpha"), e(1, b"beta")]);
        s.replace_log_suffix(1, &log.clone());
        checkpoints.push((len(&t.0), hs, log.clone()));

        // Truncate-and-append in one record: the case that must never half-apply.
        log.truncate(1);
        log.extend([e(2, b"BETA"), e(2, b"gamma")]);
        s.replace_log_suffix(2, &log[1..]);
        checkpoints.push((len(&t.0), hs, log.clone()));

        hs = HardState {
            term: 2,
            voted_for: None,
        };
        s.save_hard_state(hs);
        checkpoints.push((len(&t.0), hs, log.clone()));
    }

    let full = std::fs::read(&t.0).unwrap();
    assert_eq!(full.len() as u64, checkpoints.last().unwrap().0);

    for cut in 0..=full.len() {
        let cut_wal = TempWal::new(&format!("torn-cut-{cut}"));
        std::fs::write(&cut_wal.0, &full[..cut]).unwrap();

        let (want_hs, want_log) = checkpoints
            .iter()
            .rev()
            .find(|(end, _, _)| *end as usize <= cut)
            .map(|(_, h, l)| (*h, l.clone()))
            .unwrap_or((HardState::default(), vec![]));

        let mut s = FileStorage::open(&cut_wal.0).unwrap();
        let (hs, log) = s.load();
        assert_eq!(hs, want_hs, "hard state wrong when cut at byte {cut}");
        assert_eq!(log, want_log, "log wrong when cut at byte {cut}");
    }
}

#[test]
fn appending_after_a_torn_tail_does_not_follow_garbage() {
    let t = TempWal::new("after-torn");
    {
        let mut s = FileStorage::open(&t.0).unwrap();
        s.load();
        s.replace_log_suffix(1, &[e(1, b"keep")]);
        s.replace_log_suffix(2, &[e(1, b"will-be-torn")]);
    }
    // Chop the last record in half.
    let full = std::fs::read(&t.0).unwrap();
    std::fs::write(&t.0, &full[..full.len() - 7]).unwrap();

    {
        let mut s = FileStorage::open(&t.0).unwrap();
        let (_, log) = s.load();
        assert_eq!(log, vec![e(1, b"keep")]);
        s.replace_log_suffix(2, &[e(2, b"fresh")]); // must land cleanly after "keep"
    }
    let mut s = FileStorage::open(&t.0).unwrap();
    let (_, log) = s.load();
    assert_eq!(log, vec![e(1, b"keep"), e(2, b"fresh")]);
}

#[test]
fn bad_checksum_on_final_record_is_treated_as_torn() {
    let t = TempWal::new("garbled-tail");
    {
        let mut s = FileStorage::open(&t.0).unwrap();
        s.load();
        s.replace_log_suffix(1, &[e(1, b"ok")]);
        s.replace_log_suffix(2, &[e(1, b"garbled")]);
    }
    let mut bytes = std::fs::read(&t.0).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xFF; // flip bits in the final record's payload
    std::fs::write(&t.0, &bytes).unwrap();

    let mut s = FileStorage::open(&t.0).unwrap();
    let (_, log) = s.load();
    assert_eq!(log, vec![e(1, b"ok")]);
}

#[test]
#[should_panic(expected = "WAL corrupted")]
fn corruption_in_the_middle_is_fatal_not_silently_dropped() {
    let t = TempWal::new("mid-corrupt");
    {
        let mut s = FileStorage::open(&t.0).unwrap();
        s.load();
        s.replace_log_suffix(1, &[e(1, b"first")]);
        s.replace_log_suffix(2, &[e(1, b"second")]);
    }
    let mut bytes = std::fs::read(&t.0).unwrap();
    bytes[10] ^= 0xFF; // inside the FIRST record, with a whole record after it
    std::fs::write(&t.0, &bytes).unwrap();

    let mut s = FileStorage::open(&t.0).unwrap();
    s.load();
}
