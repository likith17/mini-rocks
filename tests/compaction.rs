//! Phase 5: size-tiered compaction.

mod common;

use std::collections::BTreeMap;
use std::fs;
use std::ops::Bound::Unbounded;
use std::path::Path;

use common::{Rng, TempDir};
use minirocks::filename::{manifest_file_name, parse_file_name, sst_file_name, FileKind, CURRENT};
use minirocks::manifest::read_current;
use minirocks::{Db, Options, SyncPolicy};

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

fn opts(memtable_size: usize) -> Options {
    Options { sync: SyncPolicy::Never, memtable_size, block_size: 1024, ..Options::default() }
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

fn contents(db: &Db) -> Model {
    db.scan(Unbounded, Unbounded).unwrap().into_iter().collect()
}

fn copy_dir(from: &Path, to: &Path) {
    fs::create_dir_all(to).unwrap();
    for e in fs::read_dir(from).unwrap() {
        let e = e.unwrap();
        fs::copy(e.path(), to.join(e.file_name())).unwrap();
    }
}

#[test]
fn size_tiered_compaction_bounds_table_count() {
    let dir = TempDir::new("cp-bound");
    let db = Db::open(dir.path(), opts(8 * 1024)).unwrap();
    let mut model = Model::new();
    let mut rng = Rng::new(1);
    for _ in 0..40_000 {
        let k = format!("key{:06}", rng.below(20_000)).into_bytes();
        let v = rng.bytes(20);
        db.put(&k, &v).unwrap();
        model.insert(k, v);
    }
    db.flush().unwrap();
    db.run_pending_compactions().unwrap();
    let s = db.write_stats();
    let sizes = db.sst_sizes();
    eprintln!(
        "40k random writes: {} flushes, {} compactions merging {} tables; {} live tables {:?}; \
         write amplification {:.2} (user {} B, WAL {} B, flush {} B, compaction {} B)",
        s.flushes,
        s.compactions,
        s.compaction_input_tables,
        sizes.len(),
        sizes,
        s.write_amplification(),
        s.user_bytes,
        s.wal_bytes,
        s.flush_bytes,
        s.compaction_bytes_written
    );
    assert!(s.flushes > 50, "{} flushes", s.flushes);
    assert!(s.compactions > 10);
    // Each settled tier holds fewer than min_merge (4) tables.
    assert!(sizes.len() < 4 * 5, "{} tables", sizes.len());
    assert_eq!(contents(&db), model);
    for (k, v) in model.iter().step_by(97) {
        assert_eq!(db.get(k).unwrap().as_ref(), Some(v));
    }
}

#[test]
fn randomized_model_check_with_compactions_and_reopens() {
    for seed in 1..=3u64 {
        let dir = TempDir::new("cp-model");
        let o = opts(2048 * seed as usize);
        let mut db = Db::open(dir.path(), o.clone()).unwrap();
        let mut rng = Rng::new(seed);
        let mut model = Model::new();
        for step in 0..15_000 {
            let key = format!("key{:03}", rng.below(300)).into_bytes();
            match rng.below(1000) {
                0..=579 => {
                    let len = rng.below(60) as usize;
                    let v = rng.bytes(len);
                    db.put(&key, &v).unwrap();
                    model.insert(key, v);
                }
                580..=899 => {
                    db.delete(&key).unwrap();
                    model.remove(&key);
                }
                900..=979 => assert_eq!(db.get(&key).unwrap().as_ref(), model.get(&key), "seed {seed} step {step}"),
                980..=989 => assert_eq!(contents(&db), model, "seed {seed} step {step}"),
                990..=994 => db.compact().unwrap(),
                _ => {
                    drop(db);
                    db = Db::open(dir.path(), o.clone()).unwrap();
                    assert_eq!(db.recovery_report().orphan_ssts_removed, 0, "clean close leaves no orphans");
                    assert_eq!(contents(&db), model, "seed {seed} step {step} after reopen");
                }
            }
        }
        assert!(db.write_stats().compactions > 0 || db.recovery_report().sst_files > 0);
        assert_eq!(contents(&db), model);
    }
}

/// A tombstone may be dropped only when no older table could hold the value
/// it deletes. Merging newer tables that sit above an older one must keep it.
#[test]
fn tombstones_are_kept_unless_compaction_reaches_the_oldest_table() {
    let dir = TempDir::new("cp-tombstones");
    // One big, old table with 2000 keys.
    {
        let db = Db::open(dir.path(), Options { auto_compaction: false, ..opts(1 << 20) }).unwrap();
        for i in 0..2000u32 {
            db.put(format!("key{i:05}").as_bytes(), &[9u8; 100]).unwrap();
        }
        db.flush().unwrap();
    }
    // Four small, newer tables of tombstones for half the keys. A 16 KiB
    // memtable holds each batch of 250 deletes, and makes 16 KiB the size
    // floor, so the ~220 KB table is in a different tier from the ~4 KB ones.
    let db = Db::open(dir.path(), Options { auto_compaction: false, ..opts(16 * 1024) }).unwrap();
    for t in 0..4u32 {
        for i in (t * 250..(t + 1) * 250).map(|i| i * 2) {
            db.delete(format!("key{i:05}").as_bytes()).unwrap();
        }
        db.flush().unwrap();
    }
    assert_eq!(db.sst_count(), 5);
    let before = db.sst_bytes();
    db.run_pending_compactions().unwrap();
    // The four tombstone tables merged; the big table is in a different tier.
    assert_eq!(db.sst_count(), 2);
    assert_eq!(db.write_stats().compactions, 1);
    for i in 0..2000u32 {
        let got = db.get(format!("key{i:05}").as_bytes()).unwrap();
        assert_eq!(got.is_some(), i % 2 == 1, "key{i:05}: deleted value resurfaced or live value lost");
    }
    // A major compaction reaches the bottom, so tombstones and the values
    // they shadow both disappear.
    db.compact().unwrap();
    assert_eq!(db.sst_count(), 1);
    assert!(db.sst_bytes() < before / 2 + 1000, "{} -> {}", before, db.sst_bytes());
    assert_eq!(contents(&db).len(), 1000);
    drop(db);
    let db = Db::open(dir.path(), opts(1 << 20)).unwrap();
    assert_eq!(contents(&db).len(), 1000);
    assert!(db.get(b"key00000").unwrap().is_none());
}

#[test]
fn major_compaction_reclaims_overwritten_space() {
    let dir = TempDir::new("cp-space");
    let db = Db::open(dir.path(), Options { auto_compaction: false, ..opts(64 * 1024) }).unwrap();
    let mut logical = 0u64;
    for round in 0..10u32 {
        for i in 0..2000u32 {
            let (k, v) = (format!("key{i:05}"), format!("round{round:02}-{}", "x".repeat(50)));
            db.put(k.as_bytes(), v.as_bytes()).unwrap();
            if round == 9 {
                logical += (k.len() + v.len()) as u64;
            }
        }
    }
    db.flush().unwrap();
    let before = db.sst_bytes();
    db.compact().unwrap();
    let after = db.sst_bytes();
    eprintln!(
        "10 overwrites of 2000 keys: logical {logical} B; SSTables {before} B before compaction \
         (space amp {:.2}), {after} B after (space amp {:.2})",
        before as f64 / logical as f64,
        after as f64 / logical as f64
    );
    assert!(before as f64 / logical as f64 > 8.0);
    assert!((after as f64 / logical as f64) < 1.3);
    assert!(contents(&db).values().all(|v| v.starts_with(b"round09")));
}

#[test]
fn open_iterator_survives_compaction_and_files_go_when_it_drops() {
    let dir = TempDir::new("cp-iter");
    let db = Db::open(dir.path(), Options { auto_compaction: false, ..opts(4096) }).unwrap();
    for i in 0..3000u32 {
        db.put(format!("key{i:05}").as_bytes(), b"v1").unwrap();
    }
    db.flush().unwrap();
    let inputs = files(dir.path(), FileKind::Sst);
    assert!(inputs.len() > 4);
    let mut it = db.iter(Unbounded, Unbounded).unwrap();
    assert_eq!(it.next().unwrap().unwrap().0, b"key00000");

    for i in 0..3000u32 {
        db.put(format!("key{i:05}").as_bytes(), b"v2").unwrap();
    }
    db.compact().unwrap();
    assert_eq!(db.sst_count(), 1);
    // The old tables are out of the manifest but still open for the iterator.
    assert!(inputs.iter().all(|n| dir.path().join(sst_file_name(*n)).exists()));

    let rest: Vec<_> = it.by_ref().map(Result::unwrap).collect();
    assert_eq!(rest.len(), 2999);
    assert!(rest.iter().all(|(_, v)| v == b"v1"), "iterator must keep its snapshot");
    drop(it);
    assert!(inputs.iter().all(|n| !dir.path().join(sst_file_name(*n)).exists()), "obsolete files not deleted");
    assert!(contents(&db).values().all(|v| v == b"v2"));
}

// --------------------------------------------- crash points of a compaction

/// Lay out four tables, then compact them. Returns (directory before the
/// compaction, directory after, model).
fn before_and_after_compaction(tag: &str) -> (TempDir, TempDir, Model) {
    let before = TempDir::new(&format!("{tag}-before"));
    let mut model = Model::new();
    {
        let db = Db::open(before.path(), Options { auto_compaction: false, ..opts(1 << 20) }).unwrap();
        for t in 0..4u32 {
            for i in 0..300u32 {
                let k = format!("key{:04}", (i * 7 + t * 13) % 500).into_bytes();
                if (i + t) % 5 == 0 {
                    db.delete(&k).unwrap();
                    model.remove(&k);
                } else {
                    let v = format!("t{t}-{i}").into_bytes();
                    db.put(&k, &v).unwrap();
                    model.insert(k, v);
                }
            }
            db.flush().unwrap();
        }
    }
    let after = TempDir::new(&format!("{tag}-after"));
    copy_dir(before.path(), after.path());
    {
        let db = Db::open(after.path(), Options { auto_compaction: false, ..opts(1 << 20) }).unwrap();
        assert_eq!(db.sst_count(), 4);
        db.compact().unwrap();
        assert_eq!(db.sst_count(), 1);
    }
    (before, after, model)
}

/// Truncate the manifest at every byte of the compaction's commit edit, with
/// both the inputs and the output present on disk. Before the edit is whole
/// the inputs are live and the output an orphan; once whole, the reverse.
/// The contents must be exact either way.
#[test]
fn crash_at_every_point_of_the_compaction_commit() {
    let (before, after, model) = before_and_after_compaction("cp-commit");
    let inputs = files(before.path(), FileKind::Sst);
    let outputs = files(after.path(), FileKind::Sst);
    assert_eq!((inputs.len(), outputs.len()), (4, 1));

    let mname = manifest_file_name(read_current(after.path()).unwrap().unwrap());
    let manifest = fs::read(after.path().join(&mname)).unwrap();
    // Frames: [open snapshot][compaction edit]. Find where the last starts.
    let mut off = 0;
    let mut starts = Vec::new();
    while off < manifest.len() {
        starts.push(off);
        off += 8 + u32::from_le_bytes(manifest[off + 4..off + 8].try_into().unwrap()) as usize;
    }
    assert_eq!(starts.len(), 2, "expected snapshot + compaction edit");
    let edit_start = starts[1];

    let work = TempDir::new("cp-commit-work");
    for cut in edit_start..=manifest.len() {
        let d = work.path().join(format!("cut{cut}"));
        fs::create_dir_all(&d).unwrap();
        for n in &inputs {
            fs::copy(before.path().join(sst_file_name(*n)), d.join(sst_file_name(*n))).unwrap();
        }
        fs::copy(after.path().join(sst_file_name(outputs[0])), d.join(sst_file_name(outputs[0]))).unwrap();
        for e in fs::read_dir(after.path()).unwrap() {
            let name = e.unwrap().file_name();
            if name.to_str().unwrap().ends_with(".wal") {
                fs::copy(after.path().join(&name), d.join(&name)).unwrap();
            }
        }
        fs::write(d.join(&mname), &manifest[..cut]).unwrap();
        fs::write(d.join(CURRENT), format!("{mname}\n")).unwrap();

        let db = Db::open(&d, Options { auto_compaction: false, ..opts(1 << 20) }).unwrap();
        let committed = cut == manifest.len();
        assert_eq!(db.recovery_report().orphan_ssts_removed, if committed { 4 } else { 1 }, "cut {cut}");
        assert_eq!(db.sst_count(), if committed { 1 } else { 4 }, "cut {cut}");
        assert_eq!(contents(&db), model, "cut {cut}");
    }
}

#[test]
fn compaction_preserves_data_across_restart() {
    let (_before, after, model) = before_and_after_compaction("cp-restart");
    let db = Db::open(after.path(), opts(1 << 20)).unwrap();
    assert_eq!(db.sst_count(), 1);
    assert_eq!(db.recovery_report().orphan_ssts_removed, 0);
    assert_eq!(contents(&db), model);
    // All tombstones were dropped by the major compaction.
    for (k, v) in &model {
        assert_eq!(db.get(k).unwrap().as_ref(), Some(v));
    }
}

#[test]
fn sequence_numbers_survive_compaction_dropping_the_newest_entries() {
    // The newest writes are deletes; a major compaction drops them, so the
    // tables alone would under-report the last sequence number. After
    // restart, a new write must still beat everything older.
    let dir = TempDir::new("cp-seq");
    {
        let db = Db::open(dir.path(), Options { auto_compaction: false, ..opts(1 << 20) }).unwrap();
        db.put(b"a", b"old").unwrap();
        db.put(b"b", b"x").unwrap();
        for _ in 0..100 {
            db.delete(b"b").unwrap();
        }
        db.compact().unwrap();
    }
    {
        let db = Db::open(dir.path(), opts(1 << 20)).unwrap();
        db.put(b"a", b"new").unwrap();
        db.flush().unwrap();
        db.compact().unwrap();
        assert_eq!(db.get(b"a").unwrap(), Some(b"new".to_vec()));
    }
    let db = Db::open(dir.path(), opts(1 << 20)).unwrap();
    assert_eq!(db.get(b"a").unwrap(), Some(b"new".to_vec()));
    assert_eq!(db.get(b"b").unwrap(), None);
}
