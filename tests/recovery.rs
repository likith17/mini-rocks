//! Phase 1: WAL + memtable + recovery.

mod common;

use std::collections::BTreeMap;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::ops::Bound::{self, Excluded, Included, Unbounded};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;

use common::{Rng, TempDir};
use minirocks::wal::{encoded_len, wal_file_name};
use minirocks::{Db, Error, Options, SyncPolicy, WalRecoveryMode};

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

fn opts(sync: SyncPolicy, wal_recovery: WalRecoveryMode) -> Options {
    Options { sync, wal_recovery, ..Options::default() }
}

fn wal_path(dir: &Path) -> std::path::PathBuf {
    dir.join(wal_file_name(1))
}

/// Check the database against the model: full scan plus a point read of
/// every key in `universe` (so deleted keys are checked too).
fn assert_matches(db: &Db, model: &Model, universe: &[Vec<u8>], ctx: &str) {
    let scanned: Model = db.scan(Unbounded, Unbounded).unwrap().into_iter().collect();
    assert_eq!(&scanned, model, "scan mismatch ({ctx})");
    for k in universe {
        assert_eq!(db.get(k).unwrap().as_ref(), model.get(k), "get({k:?}) mismatch ({ctx})");
    }
}

#[derive(Clone)]
enum Op {
    Put(Vec<u8>, Vec<u8>),
    Del(Vec<u8>),
}

impl Op {
    fn apply_model(&self, m: &mut Model) {
        match self {
            Op::Put(k, v) => {
                m.insert(k.clone(), v.clone());
            }
            Op::Del(k) => {
                m.remove(k);
            }
        }
    }
    fn apply_db(&self, db: &Db) {
        match self {
            Op::Put(k, v) => db.put(k, v).unwrap(),
            Op::Del(k) => db.delete(k).unwrap(),
        }
    }
    fn encoded_len(&self) -> usize {
        match self {
            Op::Put(k, v) => encoded_len(k.len(), v.len()),
            Op::Del(k) => encoded_len(k.len(), 0),
        }
    }
}

/// A fixed mix of puts, overwrites, deletes, an empty key and empty values.
fn scripted_ops() -> Vec<Op> {
    let mut ops = Vec::new();
    for i in 0..60u32 {
        let key = format!("k{:02}", i % 17).into_bytes();
        if i % 5 == 4 {
            ops.push(Op::Del(format!("k{:02}", (i + 15) % 17).into_bytes()));
        } else {
            let value = vec![b'a' + (i % 26) as u8; (i as usize * 7) % 40];
            ops.push(Op::Put(key, value));
        }
    }
    ops.push(Op::Put(Vec::new(), b"empty key".to_vec()));
    ops
}

fn universe_of(ops: &[Op]) -> Vec<Vec<u8>> {
    let mut u: Vec<Vec<u8>> = ops
        .iter()
        .map(|op| match op {
            Op::Put(k, _) | Op::Del(k) => k.clone(),
        })
        .collect();
    u.push(b"never-written".to_vec());
    u.sort();
    u.dedup();
    u
}

// ---------------------------------------------------------------- basics

#[test]
fn put_get_delete_scan() {
    let dir = TempDir::new("basic");
    let db = Db::open(dir.path(), Options::default()).unwrap();
    db.put(b"b", b"2").unwrap();
    db.put(b"a", b"1").unwrap();
    db.put(b"c", b"3").unwrap();
    db.put(b"d", b"4").unwrap();
    assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
    db.put(b"a", b"1'").unwrap();
    assert_eq!(db.get(b"a").unwrap(), Some(b"1'".to_vec()));
    db.delete(b"c").unwrap();
    db.delete(b"missing").unwrap();
    assert_eq!(db.get(b"c").unwrap(), None);
    assert_eq!(db.get(b"zzz").unwrap(), None);

    let keys = |s: Bound<&[u8]>, e: Bound<&[u8]>| -> Vec<Vec<u8>> {
        db.scan(s, e).unwrap().into_iter().map(|(k, _)| k).collect()
    };
    let v = |xs: &[&str]| xs.iter().map(|x| x.as_bytes().to_vec()).collect::<Vec<_>>();
    assert_eq!(keys(Unbounded, Unbounded), v(&["a", "b", "d"]));
    assert_eq!(keys(Included(b"b"), Unbounded), v(&["b", "d"]));
    assert_eq!(keys(Excluded(b"b"), Unbounded), v(&["d"]));
    assert_eq!(keys(Unbounded, Excluded(b"d")), v(&["a", "b"]));
    assert_eq!(keys(Included(b"b"), Included(b"d")), v(&["b", "d"]));
    assert_eq!(keys(Included(b"c"), Included(b"c")), v(&[])); // tombstone hidden
    assert_eq!(keys(Included(b"d"), Included(b"a")), v(&[])); // inverted
}

#[test]
fn rejects_oversized_keys_and_values() {
    let dir = TempDir::new("limits");
    let db = Db::open(dir.path(), Options::default()).unwrap();
    let big_key = vec![0u8; minirocks::MAX_KEY_LEN + 1];
    assert!(matches!(db.put(&big_key, b"v"), Err(Error::InvalidArgument(_))));
    assert!(matches!(db.delete(&big_key), Err(Error::InvalidArgument(_))));
    let big_value = vec![0u8; minirocks::MAX_VALUE_LEN + 1];
    assert!(matches!(db.put(b"k", &big_value), Err(Error::InvalidArgument(_))));
    // Nothing was logged for the rejected writes.
    assert_eq!(fs::metadata(wal_path(dir.path())).unwrap().len(), 0);
}

#[test]
fn rejects_every_n_zero() {
    let dir = TempDir::new("everyn0");
    let r = Db::open(dir.path(), opts(SyncPolicy::EveryN(0), WalRecoveryMode::TolerateTornTail));
    assert!(matches!(r, Err(Error::InvalidArgument(_))));
}

#[test]
fn state_survives_reopen_and_sequence_continues() {
    let dir = TempDir::new("reopen");
    let ops = scripted_ops();
    let universe = universe_of(&ops);
    let mut model = Model::new();
    {
        let db = Db::open(dir.path(), Options::default()).unwrap();
        for op in &ops {
            op.apply_db(&db);
            op.apply_model(&mut model);
        }
    }
    for round in 0..3 {
        let db = Db::open(dir.path(), Options::default()).unwrap();
        let r = db.recovery_report();
        assert_eq!(r.bytes_discarded, 0);
        assert_eq!(r.discard_reason, None);
        assert_matches(&db, &model, &universe, &format!("reopen {round}"));
        // A write after reopen must win over the replayed history, which
        // requires the sequence number to continue past the replayed ones.
        let op = Op::Put(b"k03".to_vec(), format!("round {round}").into_bytes());
        op.apply_db(&db);
        op.apply_model(&mut model);
    }
}

#[test]
fn all_sync_policies_recover_after_clean_close() {
    for sync in [SyncPolicy::Always, SyncPolicy::EveryN(7), SyncPolicy::Never] {
        let dir = TempDir::new("policies");
        let o = opts(sync, WalRecoveryMode::TolerateTornTail);
        let mut model = Model::new();
        {
            let db = Db::open(dir.path(), o.clone()).unwrap();
            for i in 0..100u32 {
                let k = format!("{i:03}").into_bytes();
                db.put(&k, &i.to_le_bytes()).unwrap();
                model.insert(k, i.to_le_bytes().to_vec());
            }
            db.sync().unwrap();
        }
        let db = Db::open(dir.path(), o).unwrap();
        assert_matches(&db, &model, &[], &format!("{sync:?}"));
    }
}

#[test]
fn concurrent_writers_and_readers() {
    let dir = TempDir::new("concurrent");
    let db = Arc::new(Db::open(dir.path(), opts(SyncPolicy::Never, WalRecoveryMode::TolerateTornTail)).unwrap());
    let mut handles = Vec::new();
    for t in 0..4u32 {
        let db = Arc::clone(&db);
        handles.push(std::thread::spawn(move || {
            for i in 0..500u32 {
                db.put(format!("t{t}-{i:04}").as_bytes(), &i.to_le_bytes()).unwrap();
                // Read-your-own-write from this thread.
                assert!(db.get(format!("t{t}-{i:04}").as_bytes()).unwrap().is_some());
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(db.scan(Unbounded, Unbounded).unwrap().len(), 2000);
    drop(db);
    let db = Db::open(dir.path(), Options::default()).unwrap();
    assert_eq!(db.scan(Unbounded, Unbounded).unwrap().len(), 2000);
    assert_eq!(db.recovery_report().records_replayed, 2000);
}

/// Random ops against a model, with reopens sprinkled in, for each policy.
#[test]
fn randomized_model_check_with_reopens() {
    for (seed, sync) in [(1, SyncPolicy::Always), (2, SyncPolicy::EveryN(16)), (3, SyncPolicy::Never)] {
        let dir = TempDir::new("model");
        let o = opts(sync, WalRecoveryMode::TolerateTornTail);
        let mut rng = Rng::new(seed);
        let mut model = Model::new();
        let universe: Vec<Vec<u8>> = (0..64).map(|i| format!("key{i:02}").into_bytes()).collect();
        let mut db = Db::open(dir.path(), o.clone()).unwrap();
        for step in 0..3000 {
            let key = &universe[rng.below(universe.len() as u64) as usize];
            match rng.below(100) {
                0..=59 => {
                    let len = rng.below(64) as usize;
                    let v = rng.bytes(len);
                    db.put(key, &v).unwrap();
                    model.insert(key.clone(), v);
                }
                60..=89 => {
                    db.delete(key).unwrap();
                    model.remove(key);
                }
                90..=97 => {
                    let a = &universe[rng.below(universe.len() as u64) as usize];
                    let got = db.scan(Included(key), Excluded(a)).unwrap();
                    let want: Vec<_> = if key < a {
                        model.range(key.clone()..a.clone()).map(|(k, v)| (k.clone(), v.clone())).collect()
                    } else {
                        Vec::new()
                    };
                    assert_eq!(got, want, "scan mismatch at step {step}");
                }
                _ => {
                    drop(db);
                    db = Db::open(dir.path(), o.clone()).unwrap();
                    assert_eq!(db.recovery_report().bytes_discarded, 0);
                    assert_matches(&db, &model, &universe, &format!("seed {seed} step {step}"));
                }
            }
        }
    }
}

// --------------------------------------------------- torn / corrupt WALs

/// Write the scripted ops, then for EVERY possible crash point (every byte
/// length the WAL could have been cut to) recover and check that:
///   * exactly the ops whose records are wholly inside the cut are applied,
///   * the discarded byte count is exact,
///   * writes made after recovery are themselves durable (the torn tail was
///     truncated, not left in front of new records).
#[test]
fn recovers_correct_prefix_at_every_truncation_point() {
    let ops = scripted_ops();
    let universe = universe_of(&ops);

    let src = TempDir::new("trunc-src");
    let mut ends = vec![0usize]; // ends[k] = WAL length after k ops
    let mut models = vec![Model::new()]; // models[k] = state after k ops
    {
        let db = Db::open(src.path(), Options::default()).unwrap();
        for op in &ops {
            op.apply_db(&db);
            let mut m = models.last().unwrap().clone();
            op.apply_model(&mut m);
            models.push(m);
            ends.push(ends.last().unwrap() + op.encoded_len());
        }
    }
    let full = fs::read(wal_path(src.path())).unwrap();
    assert_eq!(full.len(), *ends.last().unwrap(), "record size formula disagrees with file");

    let work = TempDir::new("trunc-work");
    for cut in 0..=full.len() {
        let k = ends.iter().rposition(|&e| e <= cut).unwrap();
        fs::write(wal_path(work.path()), &full[..cut]).unwrap();
        {
            let db = Db::open(work.path(), Options::default()).unwrap();
            let r = db.recovery_report();
            assert_eq!(r.records_replayed, k as u64, "cut {cut}");
            assert_eq!(r.bytes_discarded, (cut - ends[k]) as u64, "cut {cut}");
            assert_eq!(r.discard_reason.is_some(), cut != ends[k], "cut {cut}");
            assert_matches(&db, &models[k], &universe, &format!("cut {cut}"));
            db.put(b"after-crash", b"x").unwrap();
        }
        let db = Db::open(work.path(), Options::default()).unwrap();
        assert_eq!(db.recovery_report().bytes_discarded, 0, "cut {cut}: second open");
        let mut expect = models[k].clone();
        expect.insert(b"after-crash".to_vec(), b"x".to_vec());
        assert_matches(&db, &expect, &universe, &format!("cut {cut}, after re-write"));
    }
}

/// Ten puts of `key{i}` -> `value{i}`. Returns record start offsets.
fn write_ten(dir: &Path) -> Vec<usize> {
    let db = Db::open(dir, Options::default()).unwrap();
    let mut offsets = Vec::new();
    let mut off = 0;
    for i in 0..10 {
        let (k, v) = (format!("key{i}"), format!("value{i}"));
        db.put(k.as_bytes(), v.as_bytes()).unwrap();
        offsets.push(off);
        off += encoded_len(k.len(), v.len());
    }
    offsets
}

fn present_keys(db: &Db) -> Vec<usize> {
    (0..10).filter(|i| db.get(format!("key{i}").as_bytes()).unwrap().is_some()).collect()
}

#[test]
fn bit_flip_in_last_record_drops_only_that_record() {
    let dir = TempDir::new("flip-last");
    let offsets = write_ten(dir.path());
    let mut bytes = fs::read(wal_path(dir.path())).unwrap();
    let last = *offsets.last().unwrap();
    bytes[last + 15] ^= 0x40; // inside the payload
    fs::write(wal_path(dir.path()), &bytes).unwrap();

    let db = Db::open(dir.path(), Options::default()).unwrap();
    assert_eq!(present_keys(&db), (0..9).collect::<Vec<_>>());
    let r = db.recovery_report();
    assert_eq!(r.bytes_discarded, (bytes.len() - last) as u64);
    assert!(r.discard_reason.as_deref().unwrap().contains("checksum mismatch"));
    drop(db);
    assert_eq!(fs::metadata(wal_path(dir.path())).unwrap().len(), last as u64);
}

#[test]
fn bit_flip_mid_log_is_refused_by_default_and_file_untouched() {
    let dir = TempDir::new("flip-mid");
    let offsets = write_ten(dir.path());
    let mut bytes = fs::read(wal_path(dir.path())).unwrap();
    bytes[offsets[4] + 15] ^= 0x01;
    fs::write(wal_path(dir.path()), &bytes).unwrap();

    match Db::open(dir.path(), Options::default()) {
        Err(Error::Corruption(msg)) => {
            assert!(msg.contains("checksum mismatch"), "{msg}");
            assert!(msg.contains(&format!("offset {}", offsets[5])), "{msg}");
        }
        Err(e) => panic!("expected Corruption, got {e}"),
        Ok(_) => panic!("expected Corruption, got a database"),
    }
    // Refusing must not destroy evidence.
    assert_eq!(fs::read(wal_path(dir.path())).unwrap(), bytes);

    // PointInTime explicitly accepts the loss and recovers the prefix.
    let pit = opts(SyncPolicy::Always, WalRecoveryMode::PointInTime);
    let db = Db::open(dir.path(), pit).unwrap();
    assert_eq!(present_keys(&db), vec![0, 1, 2, 3]);
    assert_eq!(db.recovery_report().bytes_discarded, (bytes.len() - offsets[4]) as u64);
    db.put(b"key9", b"again").unwrap();
    drop(db);

    // The log is clean again, so the strict default opens.
    let db = Db::open(dir.path(), Options::default()).unwrap();
    assert_eq!(present_keys(&db), vec![0, 1, 2, 3, 9]);
}

#[test]
fn corrupted_length_field_mid_log_is_detected() {
    let dir = TempDir::new("len-mid");
    let offsets = write_ten(dir.path());
    let mut bytes = fs::read(wal_path(dir.path())).unwrap();
    // Point the length of record 2 past the end of the file.
    bytes[offsets[2] + 4..offsets[2] + 8].copy_from_slice(&100_000u32.to_le_bytes());
    fs::write(wal_path(dir.path()), &bytes).unwrap();
    assert!(matches!(Db::open(dir.path(), Options::default()), Err(Error::Corruption(_))));
}

#[test]
fn garbage_tails_are_discarded() {
    let mut rng = Rng::new(99);
    let tails: Vec<(&str, Vec<u8>)> = vec![
        ("zeros", vec![0u8; 4096]),
        ("ones", vec![0xFF; 100]),
        ("random", rng.bytes(10_000)),
        ("short header", vec![1, 2, 3]),
        ("header claiming huge record", {
            let mut h = vec![0u8; 9];
            h[4..8].copy_from_slice(&u32::MAX.to_le_bytes());
            h
        }),
        ("header claiming a record past EOF", {
            let mut h = vec![0u8; 20];
            h[4..8].copy_from_slice(&500u32.to_le_bytes());
            h
        }),
    ];
    for (name, tail) in tails {
        let dir = TempDir::new("garbage");
        write_ten(dir.path());
        let mut f = fs::OpenOptions::new().append(true).open(wal_path(dir.path())).unwrap();
        f.write_all(&tail).unwrap();
        drop(f);
        let db = Db::open(dir.path(), Options::default()).unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(present_keys(&db), (0..10).collect::<Vec<_>>(), "{name}");
        assert_eq!(db.recovery_report().bytes_discarded, tail.len() as u64, "{name}");
    }
}

#[test]
fn damaged_older_wal_is_corruption() {
    // Only the newest WAL may have a torn tail. Fake an older WAL by moving
    // a damaged log to number 1 and starting a fresh number 2.
    let dir = TempDir::new("old-wal");
    write_ten(dir.path());
    let p1 = wal_path(dir.path());
    let len = fs::metadata(&p1).unwrap().len();
    fs::OpenOptions::new().write(true).open(&p1).unwrap().set_len(len - 3).unwrap();
    {
        let mut w = minirocks::wal::WalWriter::open(&dir.path().join(wal_file_name(2)), SyncPolicy::Always).unwrap();
        w.append(minirocks::wal::RecordRef::Put { seq: 100, key: b"later", value: b"x" }).unwrap();
    }
    assert!(matches!(Db::open(dir.path(), Options::default()), Err(Error::Corruption(_))));

    // PointInTime recovers WAL 1's prefix and sets WAL 2 aside, not deletes it.
    let db = Db::open(dir.path(), opts(SyncPolicy::Always, WalRecoveryMode::PointInTime)).unwrap();
    assert_eq!(present_keys(&db), (0..9).collect::<Vec<_>>());
    assert_eq!(db.get(b"later").unwrap(), None);
    assert!(dir.path().join("000002.wal.discarded").exists());
    assert!(!dir.path().join(wal_file_name(2)).exists());
}

// ----------------------------------------------------- real process kill

/// Child side of `kill_during_writes`: write forever, printing "ACK <i>"
/// after each put returns. Run only when spawned by the parent test.
#[test]
#[ignore]
fn crash_child_writer() {
    let Ok(dir) = std::env::var("MINIROCKS_CHILD_DIR") else { return };
    let mut options = Options::default();
    if let Ok(size) = std::env::var("MINIROCKS_CHILD_MEMTABLE") {
        options.memtable_size = size.parse().unwrap();
    }
    let db = Db::open(&dir, options).unwrap();
    let start: u64 = std::env::var("MINIROCKS_CHILD_START").unwrap().parse().unwrap();
    let mut out = std::io::stdout().lock();
    for i in start.. {
        db.put(format!("key{i:08}").as_bytes(), format!("value{i}").as_bytes()).unwrap();
        writeln!(out, "ACK {i}").unwrap();
        out.flush().unwrap();
    }
}

/// Spawn a writer process, kill it after a random number of acknowledged
/// writes, recover, and check every acknowledged write is present. A smoke
/// test only; the full crash harness is phase 6.
#[test]
fn kill_during_writes() {
    kill_cycles("kill", None, 5, 7);
}

/// Same, with a 2 KiB memtable so kills regularly land during a WAL rotation
/// or background flush.
#[test]
fn kill_during_writes_with_frequent_flushes() {
    kill_cycles("kill-flush", Some(2048), 10, 8);
}

fn kill_cycles(tag: &str, memtable_size: Option<usize>, cycles: usize, seed: u64) {
    let dir = TempDir::new(tag);
    let exe = std::env::current_exe().unwrap();
    let mut rng = Rng::new(seed);
    let mut next = 0u64; // first key the next child writes
    let mut acked_total = 0u64;
    for cycle in 0..cycles {
        let kill_after = 20 + rng.below(300);
        let mut cmd = Command::new(&exe);
        if let Some(size) = memtable_size {
            cmd.env("MINIROCKS_CHILD_MEMTABLE", size.to_string());
        }
        let mut child = cmd
            .args(["crash_child_writer", "--exact", "--ignored", "--nocapture", "--test-threads=1"])
            .env("MINIROCKS_CHILD_DIR", dir.path())
            .env("MINIROCKS_CHILD_START", next.to_string())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
        let mut last_ack = None;
        let mut seen = 0;
        while seen < kill_after {
            let line = lines.next().expect("child exited early").unwrap();
            if let Some(n) = line.strip_prefix("ACK ") {
                last_ack = Some(n.parse::<u64>().unwrap());
                seen += 1;
            }
        }
        child.kill().unwrap();
        // ACKs already in the pipe were acknowledged before the kill.
        for line in lines.map_while(|l| l.ok()) {
            if let Some(n) = line.strip_prefix("ACK ") {
                last_ack = Some(n.parse::<u64>().unwrap());
            }
        }
        child.wait().unwrap();
        let last_ack = last_ack.unwrap();

        let db = Db::open(dir.path(), Options::default()).unwrap();
        for i in 0..=last_ack {
            assert_eq!(
                db.get(format!("key{i:08}").as_bytes()).unwrap(),
                Some(format!("value{i}").into_bytes()),
                "cycle {cycle}: acknowledged key {i} missing"
            );
        }
        // At most one unacknowledged write (the one in flight) may also be
        // present, and it must be complete.
        let all = db.scan(Unbounded, Unbounded).unwrap();
        assert!(all.len() as u64 == last_ack + 1 || all.len() as u64 == last_ack + 2, "cycle {cycle}");
        acked_total += last_ack + 1 - next;
        next = all.len() as u64;
    }
    let ssts = fs::read_dir(dir.path())
        .unwrap()
        .filter(|e| e.as_ref().unwrap().path().extension().is_some_and(|x| x == "sst"))
        .count();
    eprintln!("{tag}: {cycles} kills, {acked_total} acknowledged writes verified, {ssts} SSTables at end");
}
