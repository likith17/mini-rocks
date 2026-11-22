//! Phase 2: SSTables and flushing.

mod common;

use std::collections::BTreeMap;
use std::fs;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::path::Path;
use std::sync::Arc;

use common::{Rng, TempDir};
use minirocks::filename::{parse_file_name, sst_file_name, wal_file_name, FileKind};
use minirocks::{Db, Error, Options, SyncPolicy};

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

fn small(memtable_size: usize) -> Options {
    Options { memtable_size, block_size: 256, ..Options::default() }
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

fn assert_matches(db: &Db, model: &Model, universe: &[Vec<u8>], ctx: &str) {
    let scanned: Model = db.scan(Unbounded, Unbounded).unwrap().into_iter().collect();
    assert_eq!(&scanned, model, "scan mismatch ({ctx})");
    for k in universe {
        assert_eq!(db.get(k).unwrap().as_ref(), model.get(k), "get({k:?}) ({ctx})");
    }
}

#[test]
fn flush_writes_sst_and_deletes_its_wal() {
    let dir = TempDir::new("flush-basic");
    let mut model = Model::new();
    {
        let db = Db::open(dir.path(), small(1 << 20)).unwrap();
        for i in 0..500u32 {
            let (k, v) = (format!("key{i:05}").into_bytes(), format!("value{i}").into_bytes());
            db.put(&k, &v).unwrap();
            model.insert(k, v);
        }
        assert_eq!(db.sst_count(), 0);
        db.flush().unwrap();
        assert_eq!(db.sst_count(), 1);
        // Only the fresh, empty WAL remains.
        let wals = files(dir.path(), FileKind::Wal);
        assert_eq!(wals.len(), 1);
        assert_eq!(fs::metadata(dir.path().join(wal_file_name(wals[0]))).unwrap().len(), 0);
        assert_matches(&db, &model, &[], "after flush");
        db.flush().unwrap(); // empty memtable: no new SSTable
        assert_eq!(db.sst_count(), 1);
    }
    let db = Db::open(dir.path(), small(1 << 20)).unwrap();
    let r = db.recovery_report();
    assert_eq!((r.sst_files, r.records_replayed), (1, 0));
    assert_matches(&db, &model, &[], "after reopen");
}

#[test]
fn automatic_flushes_and_layered_reads() {
    // A 4 KiB memtable flushes every ~60 writes, so keys are spread over
    // many SSTables, and overwrites and deletes shadow older layers.
    let dir = TempDir::new("flush-layers");
    let db = Db::open(dir.path(), small(4096)).unwrap();
    let mut model = Model::new();
    let universe: Vec<Vec<u8>> = (0..200).map(|i| format!("k{i:03}").into_bytes()).collect();
    for round in 0..10 {
        for (i, k) in universe.iter().enumerate() {
            if (i + round) % 3 == 0 {
                db.delete(k).unwrap();
                model.remove(k);
            } else {
                let v = format!("r{round}-{i}").into_bytes();
                db.put(k, &v).unwrap();
                model.insert(k.clone(), v);
            }
        }
    }
    db.flush().unwrap();
    assert!(db.sst_count() >= 10, "only {} SSTables", db.sst_count());
    assert_matches(&db, &model, &universe, "layered");

    // Bounded scans across layers.
    let a = &universe[37];
    let b = &universe[151];
    let got = db.scan(Excluded(a), Included(b)).unwrap();
    let want: Vec<_> = model
        .range::<Vec<u8>, _>((Excluded(a), Included(b)))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    assert_eq!(got, want);
}

#[test]
fn randomized_model_check_with_flushes_and_reopens() {
    for seed in 1..=3u64 {
        let dir = TempDir::new("flush-model");
        let o = Options { sync: SyncPolicy::Never, ..small(2048 * seed as usize) };
        let mut rng = Rng::new(seed);
        let mut model = Model::new();
        let universe: Vec<Vec<u8>> = (0..100).map(|i| format!("key{i:03}").into_bytes()).collect();
        let mut db = Db::open(dir.path(), o.clone()).unwrap();
        for step in 0..5000 {
            let key = &universe[rng.below(100) as usize];
            match rng.below(1000) {
                0..=599 => {
                    let len = rng.below(100) as usize;
                    let v = rng.bytes(len);
                    db.put(key, &v).unwrap();
                    model.insert(key.clone(), v);
                }
                600..=899 => {
                    db.delete(key).unwrap();
                    model.remove(key);
                }
                900..=979 => {
                    assert_eq!(db.get(key).unwrap().as_ref(), model.get(key), "seed {seed} step {step}");
                }
                980..=989 => db.flush().unwrap(),
                _ => {
                    drop(db);
                    db = Db::open(dir.path(), o.clone()).unwrap();
                    assert_eq!(db.recovery_report().bytes_discarded, 0);
                    assert_matches(&db, &model, &universe, &format!("seed {seed} step {step}"));
                }
            }
        }
        assert_matches(&db, &model, &universe, &format!("seed {seed} end"));
        assert!(db.sst_count() > 5, "seed {seed}: only {} SSTables", db.sst_count());
    }
}

#[test]
fn concurrent_readers_during_flushes() {
    let dir = TempDir::new("flush-concurrent");
    let db = Arc::new(Db::open(dir.path(), Options { sync: SyncPolicy::Never, ..small(8192) }).unwrap());
    let mut handles = Vec::new();
    for t in 0..4u32 {
        let db = Arc::clone(&db);
        handles.push(std::thread::spawn(move || {
            for i in 0..1000u32 {
                let k = format!("t{t}-{i:04}").into_bytes();
                db.put(&k, &i.to_le_bytes()).unwrap();
                // Every earlier write of this thread must stay visible while
                // its memtable is frozen, flushed and swapped for an SSTable.
                let j = i / 2;
                let kj = format!("t{t}-{j:04}").into_bytes();
                assert_eq!(db.get(&kj).unwrap(), Some(j.to_le_bytes().to_vec()), "t{t} i{i}");
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    assert!(db.sst_count() > 10);
    assert_eq!(db.scan(Unbounded, Unbounded).unwrap().len(), 4000);
}

// ------------------------------------------------- crash windows of a flush

/// Crash after the SSTable was installed but before its WAL was deleted.
/// Replaying that WAL naively would put the old value ABOVE the newer
/// SSTable; recovery must skip records the SSTables already hold.
#[test]
fn stale_wal_after_flush_is_skipped_not_replayed() {
    let dir = TempDir::new("flush-stale-wal");
    {
        let db = Db::open(dir.path(), small(1 << 20)).unwrap();
        db.put(b"k", b"v1").unwrap();
        db.put(b"only-in-old-wal", b"x").unwrap();
    }
    let stale_name = wal_file_name(files(dir.path(), FileKind::Wal)[0]);
    let stale = fs::read(dir.path().join(&stale_name)).unwrap();
    {
        let db = Db::open(dir.path(), small(1 << 20)).unwrap();
        db.flush().unwrap(); // SST holds k=v1; WAL deleted
        db.put(b"k", b"v2").unwrap();
        db.flush().unwrap(); // newer SST holds k=v2
    }
    assert!(!dir.path().join(&stale_name).exists());
    fs::write(dir.path().join(&stale_name), &stale).unwrap(); // the "crash"

    let db = Db::open(dir.path(), small(1 << 20)).unwrap();
    let r = db.recovery_report().clone();
    assert_eq!(db.get(b"k").unwrap(), Some(b"v2".to_vec()));
    assert_eq!(db.get(b"only-in-old-wal").unwrap(), Some(b"x".to_vec()));
    assert_eq!(r.records_already_flushed, 2);
    assert_eq!(r.records_replayed, 0);
    assert_eq!(r.wal_files_deleted, 1);
    assert!(!dir.path().join(&stale_name).exists());
}

/// Crash while the SSTable was being written: a partial `.sst.tmp` exists,
/// and the data is still in the WAL.
#[test]
fn leftover_temp_sst_is_removed_and_data_recovered_from_wal() {
    let dir = TempDir::new("flush-tmp");
    {
        let db = Db::open(dir.path(), small(1 << 20)).unwrap();
        for i in 0..50u32 {
            db.put(format!("k{i}").as_bytes(), b"v").unwrap();
        }
    }
    fs::write(dir.path().join("000099.sst.tmp"), b"half an sstable").unwrap();
    let db = Db::open(dir.path(), small(1 << 20)).unwrap();
    assert_eq!(db.recovery_report().temp_files_removed, 1);
    assert!(!dir.path().join("000099.sst.tmp").exists());
    assert_eq!(db.scan(Unbounded, Unbounded).unwrap().len(), 50);
    // New files are numbered past the leftover, so names never collide.
    db.flush().unwrap();
    assert!(files(dir.path(), FileKind::Sst).iter().all(|&n| n > 99));
}

/// Crash after rotation but before the flush finished: two WALs, both with
/// unflushed data and no SSTable. Both must be replayed and both kept until
/// the recovered memtable is flushed.
#[test]
fn two_unflushed_wals_are_both_replayed() {
    let dir = TempDir::new("flush-two-wals");
    {
        let db = Db::open(dir.path(), small(1 << 20)).unwrap();
        db.put(b"a", b"1").unwrap();
        db.put(b"b", b"1").unwrap();
    }
    // Simulate the rotation: a newer WAL that overwrites `a`.
    {
        let mut w = minirocks::wal::WalWriter::open(&dir.path().join(wal_file_name(5)), SyncPolicy::Always).unwrap();
        w.append(minirocks::wal::RecordRef::Put { seq: 3, key: b"a", value: b"2" }).unwrap();
    }
    let db = Db::open(dir.path(), small(1 << 20)).unwrap();
    assert_eq!(db.recovery_report().records_replayed, 3);
    assert_eq!(db.get(b"a").unwrap(), Some(b"2".to_vec()));
    assert_eq!(db.get(b"b").unwrap(), Some(b"1".to_vec()));
    assert_eq!(files(dir.path(), FileKind::Wal).len(), 2);
    db.put(b"c", b"1").unwrap();
    db.flush().unwrap();
    assert_eq!(files(dir.path(), FileKind::Wal).len(), 1, "both old WALs deleted after flush");
    drop(db);
    let db = Db::open(dir.path(), small(1 << 20)).unwrap();
    assert_eq!(db.scan(Unbounded, Unbounded).unwrap().len(), 3);
}

/// An SSTable that is damaged after being installed is real corruption:
/// the database refuses to open (truncation, footer) or the read touching
/// the damaged block fails (data block). It never returns wrong data.
#[test]
fn damaged_sst_is_reported_not_ignored() {
    let dir = TempDir::new("flush-damaged");
    {
        let db = Db::open(dir.path(), small(1 << 20)).unwrap();
        for i in 0..300u32 {
            db.put(format!("key{i:04}").as_bytes(), &[7u8; 20]).unwrap();
        }
        db.flush().unwrap();
    }
    let sst = dir.path().join(sst_file_name(files(dir.path(), FileKind::Sst)[0]));
    let good = fs::read(&sst).unwrap();

    // Truncated: open fails.
    fs::write(&sst, &good[..good.len() / 2]).unwrap();
    assert!(matches!(Db::open(dir.path(), small(1 << 20)), Err(Error::Corruption(_))));

    // A flipped byte in the first data block: open succeeds, the read of a
    // key in that block fails, keys in other blocks still read correctly.
    let mut bad = good.clone();
    bad[10] ^= 0xFF;
    fs::write(&sst, &bad).unwrap();
    let db = Db::open(dir.path(), small(1 << 20)).unwrap();
    assert!(matches!(db.get(b"key0000"), Err(Error::Corruption(_))));
    assert_eq!(db.get(b"key0299").unwrap(), Some(vec![7u8; 20]));
    assert!(matches!(db.scan(Unbounded, Unbounded), Err(Error::Corruption(_))));
}

#[test]
fn rejects_invalid_options() {
    let dir = TempDir::new("flush-opts");
    for o in [
        Options { memtable_size: 0, ..Options::default() },
        Options { block_size: 0, ..Options::default() },
        Options { bloom_bits_per_key: 0, ..Options::default() },
        Options { bloom_bits_per_key: 65, ..Options::default() },
    ] {
        assert!(matches!(Db::open(dir.path(), o), Err(Error::InvalidArgument(_))));
    }
}
