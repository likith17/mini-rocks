//! Phase 4: manifest and restart.

mod common;

use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::ops::Bound::Unbounded;
use std::path::{Path, PathBuf};

use common::{Rng, TempDir};
use minirocks::filename::{manifest_file_name, parse_file_name, sst_file_name, wal_file_name, FileKind, CURRENT};
use minirocks::manifest::{read_current, read_manifest};
use minirocks::{Db, Error, Options, SyncPolicy};

fn small() -> Options {
    Options { sync: SyncPolicy::Never, memtable_size: 4096, block_size: 512, ..Options::default() }
}

fn files(dir: &Path, kind: FileKind) -> Vec<u64> {
    let mut v: Vec<u64> = fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| parse_file_name(e.unwrap().file_name().to_str()?))
        .filter(|&(_, k)| k == kind)
        .map(|(n, _)| n)
        .collect();
    v.sort();
    v
}

fn current_manifest(dir: &Path) -> PathBuf {
    dir.join(manifest_file_name(read_current(dir).unwrap().unwrap()))
}

/// Write `n` keys (with a small memtable, so several flushes) and close.
fn populate(dir: &Path, n: u32) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let db = Db::open(dir, small()).unwrap();
    let mut model = BTreeMap::new();
    for i in 0..n {
        let (k, v) = (format!("key{i:05}").into_bytes(), format!("value{i}").into_bytes());
        db.put(&k, &v).unwrap();
        model.insert(k, v);
    }
    db.flush().unwrap();
    model
}

fn contents(db: &Db) -> BTreeMap<Vec<u8>, Vec<u8>> {
    db.scan(Unbounded, Unbounded).unwrap().into_iter().collect()
}

#[test]
fn manifest_tracks_every_flush_and_matches_the_directory() {
    let dir = TempDir::new("mf-tracks");
    let db = Db::open(dir.path(), small()).unwrap();
    for i in 0..2000u32 {
        db.put(format!("key{i:05}").as_bytes(), &[0u8; 20]).unwrap();
    }
    db.flush().unwrap();
    // Close first: a background compaction may be swapping files, and
    // closing waits for it (and releases the replaced tables' files).
    drop(db);
    let state = read_manifest(&current_manifest(dir.path())).unwrap().state;
    let listed: Vec<u64> = state.files.keys().copied().collect();
    assert_eq!(listed, files(dir.path(), FileKind::Sst));
    let db = Db::open(dir.path(), Options { auto_compaction: false, ..small() }).unwrap();
    assert_eq!(listed.len(), db.sst_count());
    assert_eq!(db.recovery_report().orphan_ssts_removed, 0);
    for (&n, &size) in &state.files {
        assert_eq!(fs::metadata(dir.path().join(sst_file_name(n))).unwrap().len(), size);
    }
    // Every WAL older than the log number has been deleted.
    assert!(files(dir.path(), FileKind::Wal).iter().all(|&w| w >= state.log_number));
}

#[test]
fn restart_cycles_preserve_data_and_keep_one_small_manifest() {
    let dir = TempDir::new("mf-cycles");
    let mut model = BTreeMap::new();
    let mut rng = Rng::new(5);
    let mut manifest_sizes = Vec::new();
    for cycle in 0..30u32 {
        let db = Db::open(dir.path(), small()).unwrap();
        assert_eq!(contents(&db), model, "cycle {cycle}");
        assert_eq!(files(dir.path(), FileKind::Manifest).len(), 1, "cycle {cycle}: stale manifests left behind");
        for _ in 0..200 {
            let k = format!("key{:04}", rng.below(500)).into_bytes();
            if rng.below(4) == 0 {
                db.delete(&k).unwrap();
                model.remove(&k);
            } else {
                let len = rng.below(40) as usize;
                let v = rng.bytes(len);
                db.put(&k, &v).unwrap();
                model.insert(k, v);
            }
        }
        drop(db);
        manifest_sizes.push(fs::metadata(current_manifest(dir.path())).unwrap().len());
    }
    // Rewritten as a snapshot on every open, so its size reflects the live
    // tables plus one session's edits, not the number of restarts. (With
    // compaction the table count stays small too.)
    assert!(manifest_sizes.iter().all(|&s| s < 400), "{manifest_sizes:?}");
    let (first, last) = manifest_sizes.split_at(10);
    let avg = |v: &[u64]| v.iter().sum::<u64>() as f64 / v.len() as f64;
    assert!(avg(&last[10..]) < avg(first) * 2.0, "manifest grows with restarts: {manifest_sizes:?}");
}

/// Crash after the SSTable was written and renamed but before its manifest
/// edit was committed: the SSTable is an orphan and its data is still in the
/// WAL.
#[test]
fn uncommitted_sst_is_an_orphan_and_removed() {
    let dir = TempDir::new("mf-orphan");
    let model = populate(dir.path(), 300);
    // Plant a complete, valid SSTable the manifest has never heard of, with
    // contents that would corrupt the database if it were treated as live.
    {
        let other = TempDir::new("mf-orphan-src");
        let db = Db::open(other.path(), small()).unwrap();
        db.put(b"key00001", b"WRONG").unwrap();
        db.flush().unwrap();
        drop(db);
        let src = other.path().join(sst_file_name(files(other.path(), FileKind::Sst)[0]));
        fs::copy(src, dir.path().join(sst_file_name(999))).unwrap();
    }
    let db = Db::open(dir.path(), small()).unwrap();
    assert_eq!(db.recovery_report().orphan_ssts_removed, 1);
    assert!(!dir.path().join(sst_file_name(999)).exists());
    assert_eq!(contents(&db), model);
}

#[test]
fn missing_live_sst_is_corruption() {
    let dir = TempDir::new("mf-missing");
    populate(dir.path(), 300);
    let victim = files(dir.path(), FileKind::Sst)[0];
    fs::remove_file(dir.path().join(sst_file_name(victim))).unwrap();
    match Db::open(dir.path(), small()) {
        Err(Error::Corruption(m)) => assert!(m.contains("missing"), "{m}"),
        other => panic!("expected Corruption, got {:?}", other.err()),
    }
}

#[test]
fn resized_live_sst_is_corruption() {
    let dir = TempDir::new("mf-resized");
    populate(dir.path(), 300);
    let path = dir.path().join(sst_file_name(files(dir.path(), FileKind::Sst)[0]));
    // Append bytes: the footer moves, but the size check catches it before
    // anything else does.
    fs::OpenOptions::new().append(true).open(&path).unwrap().write_all(b"extra").unwrap();
    assert!(matches!(Db::open(dir.path(), small()), Err(Error::Corruption(_))));
}

#[test]
fn torn_manifest_tail_is_ignored() {
    let dir = TempDir::new("mf-torn");
    let model = populate(dir.path(), 300);
    let mpath = current_manifest(dir.path());
    let good = fs::read(&mpath).unwrap();
    for tail in [vec![0u8; 3], vec![0xAB; 50], {
        // A frame header claiming more bytes than exist: a torn append.
        let mut h = 100u32.to_le_bytes().to_vec();
        h.splice(0..0, [1, 2, 3, 4]);
        h.extend_from_slice(&[9; 20]);
        h
    }] {
        let mut bytes = good.clone();
        bytes.extend_from_slice(&tail);
        fs::write(&mpath, &bytes).unwrap();
        let db = Db::open(dir.path(), small()).unwrap();
        assert_eq!(db.recovery_report().manifest_torn_bytes, tail.len() as u64);
        assert_eq!(contents(&db), model);
        drop(db);
        // The next open wrote a fresh manifest; re-plant on it.
        let new_path = current_manifest(dir.path());
        assert_ne!(new_path, mpath);
        fs::copy(&new_path, &mpath).unwrap();
        fs::write(dir.path().join(CURRENT), format!("{}\n", mpath.file_name().unwrap().to_str().unwrap())).unwrap();
        fs::remove_file(&new_path).unwrap();
    }
}

#[test]
fn every_truncation_of_the_manifest_tail_edit_recovers() {
    // Truncate the manifest at every point inside its last edit. The last
    // edit is one flush; losing it must leave a consistent older state, with
    // that flush's data recovered from its WAL instead.
    let dir = TempDir::new("mf-trunc-src");
    let model = {
        let db = Db::open(dir.path(), Options { memtable_size: 1 << 20, ..small() }).unwrap();
        let mut model = BTreeMap::new();
        for round in 0..3u32 {
            for i in 0..50u32 {
                let (k, v) = (format!("k{i:03}").into_bytes(), format!("r{round}").into_bytes());
                db.put(&k, &v).unwrap();
                model.insert(k, v);
            }
            if round < 2 {
                db.flush().unwrap();
            }
        }
        model
    };
    // Simulate a crash during the commit of the final flush. Do the flush,
    // then rebuild the directory as the crash would have left it: the new
    // SSTable written, the WAL not yet deleted, and the manifest's final
    // edit cut at every possible length.
    let wal_before = files(dir.path(), FileKind::Wal);
    assert_eq!(wal_before.len(), 1);
    let wal_bytes = fs::read(dir.path().join(wal_file_name(wal_before[0]))).unwrap();
    {
        let db = Db::open(dir.path(), Options { memtable_size: 1 << 20, ..small() }).unwrap();
        db.flush().unwrap();
    }
    let mpath = current_manifest(dir.path());
    let manifest_after = fs::read(&mpath).unwrap();
    let sst_after = files(dir.path(), FileKind::Sst);
    let snapshot_dir = TempDir::new("mf-trunc-work");
    // The reopen wrote the manifest as one snapshot frame; the flush's edit
    // is the frame after it.
    let snapshot_len = 8 + u32::from_le_bytes(manifest_after[4..8].try_into().unwrap()) as usize;
    assert!(snapshot_len < manifest_after.len());
    for cut in snapshot_len..manifest_after.len() {
        let work = snapshot_dir.path().join(format!("cut{cut}"));
        fs::create_dir_all(&work).unwrap();
        for n in &sst_after {
            fs::copy(dir.path().join(sst_file_name(*n)), work.join(sst_file_name(*n))).unwrap();
        }
        // The WAL the final flush would have deleted, still present.
        fs::write(work.join(wal_file_name(wal_before[0])), &wal_bytes).unwrap();
        let mname = mpath.file_name().unwrap();
        fs::write(work.join(mname), &manifest_after[..cut]).unwrap();
        fs::write(work.join(CURRENT), format!("{}\n", mname.to_str().unwrap())).unwrap();

        let db = Db::open(&work, Options { memtable_size: 1 << 20, ..small() }).unwrap();
        let r = db.recovery_report();
        assert_eq!(contents(&db), model, "cut {cut}");
        assert_eq!(r.orphan_ssts_removed, 1, "cut {cut}: the uncommitted SSTable");
        assert_eq!(r.records_replayed, 50, "cut {cut}: final round from the WAL");
    }
}

#[test]
fn bad_current_is_corruption() {
    let dir = TempDir::new("mf-current");
    populate(dir.path(), 100);
    let current = fs::read(dir.path().join(CURRENT)).unwrap();

    fs::write(dir.path().join(CURRENT), "MANIFEST-424242\n").unwrap();
    assert!(matches!(Db::open(dir.path(), small()), Err(Error::Corruption(_))));

    fs::write(dir.path().join(CURRENT), "garbage").unwrap();
    assert!(matches!(Db::open(dir.path(), small()), Err(Error::Corruption(_))));

    fs::write(dir.path().join(CURRENT), current).unwrap();
    Db::open(dir.path(), small()).unwrap();
}

/// Regression test for crash-harness failure #1 (EXPERIMENTS.md): a crash
/// after the first manifest was written but before CURRENT was created made
/// every later open fail with "manifest files exist but CURRENT is missing".
#[test]
fn crash_before_first_current_does_not_brick_the_database() {
    // Variant A: the first manifest is complete (the crash hit just before
    // CURRENT was written). It describes the state, so use it.
    let dir = TempDir::new("mf-first-current");
    let model = populate(dir.path(), 300);
    fs::remove_file(dir.path().join(CURRENT)).unwrap();
    let db = Db::open(dir.path(), small()).unwrap();
    assert!(db.recovery_report().recovered_without_current.is_some());
    assert_eq!(contents(&db), model);
    drop(db);
    assert!(read_current(dir.path()).unwrap().is_some(), "CURRENT recreated");

    // Variant B: the very first open was killed while its manifest was
    // being written, after the WAL had been replayed into nothing: a torn
    // manifest, no CURRENT, and data only in the WAL.
    let dir = TempDir::new("mf-first-torn");
    let mut model = BTreeMap::new();
    {
        let mut w = minirocks::wal::WalWriter::open(&dir.path().join(wal_file_name(1)), SyncPolicy::Always).unwrap();
        for i in 0..20u64 {
            let (k, v) = (format!("k{i:02}").into_bytes(), b"v".to_vec());
            w.append(minirocks::wal::RecordRef::Put { seq: i + 1, key: &k, value: &v }).unwrap();
            model.insert(k, v);
        }
    }
    fs::write(dir.path().join(manifest_file_name(2)), [7u8, 0, 0]).unwrap();
    let db = Db::open(dir.path(), small()).unwrap();
    assert_eq!(db.recovery_report().recovered_without_current, None);
    assert_eq!(contents(&db), model);
    drop(db);
    assert_eq!(files(dir.path(), FileKind::Manifest).len(), 1, "torn manifest cleaned up");
}

#[test]
fn leftover_current_tmp_is_removed() {
    let dir = TempDir::new("mf-current-tmp");
    let model = populate(dir.path(), 100);
    fs::write(dir.path().join("CURRENT.tmp"), "MANIFEST-9999").unwrap();
    let db = Db::open(dir.path(), small()).unwrap();
    assert_eq!(db.recovery_report().temp_files_removed, 1);
    assert!(!dir.path().join("CURRENT.tmp").exists());
    assert_eq!(contents(&db), model);
}

#[test]
fn mid_manifest_corruption_is_refused() {
    let dir = TempDir::new("mf-mid");
    {
        let db = Db::open(dir.path(), small()).unwrap();
        for i in 0..600u32 {
            db.put(format!("key{i:05}").as_bytes(), &[1u8; 20]).unwrap();
        }
        db.flush().unwrap();
    }
    let mpath = current_manifest(dir.path());
    let mut bytes = fs::read(&mpath).unwrap();
    assert!(bytes.len() > 40, "expect several edits");
    bytes[12] ^= 0x01; // inside the first (snapshot) edit
    fs::write(&mpath, &bytes).unwrap();
    match Db::open(dir.path(), small()) {
        Err(Error::Corruption(m)) => assert!(m.contains("followed by a valid one"), "{m}"),
        other => panic!("expected Corruption, got {:?}", other.err()),
    }
}

#[test]
fn directory_without_manifest_is_adopted() {
    // A database written before phase 4 has SSTables and WALs but no
    // manifest. Simulate one by deleting the manifest and CURRENT.
    let dir = TempDir::new("mf-legacy");
    let model = populate(dir.path(), 500);
    let tables = files(dir.path(), FileKind::Sst).len();
    fs::remove_file(current_manifest(dir.path())).unwrap();
    fs::remove_file(dir.path().join(CURRENT)).unwrap();
    let db = Db::open(dir.path(), small()).unwrap();
    assert_eq!(db.recovery_report().ssts_adopted_without_manifest, tables);
    assert_eq!(contents(&db), model);
    assert!(read_current(dir.path()).unwrap().is_some());
}
