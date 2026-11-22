//! Phase 3: read path, Bloom filters, merge iterator.

mod common;

use std::collections::BTreeMap;
use std::fs;
use std::ops::Bound::{self, Excluded, Included, Unbounded};

use common::{Rng, TempDir};
use minirocks::filename::{parse_file_name, FileKind};
use minirocks::{Db, Error, Options, SyncPolicy};

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

fn small(memtable_size: usize) -> Options {
    Options { sync: SyncPolicy::Never, memtable_size, block_size: 512, ..Options::default() }
}

/// Fill a database whose SSTables each span the whole key space (keys are
/// written in a shuffled order), so key-range checks can't skip anything
/// and only the Bloom filters can.
fn shuffled_db(dir: &std::path::Path, n: u64) -> (Db, Vec<Vec<u8>>) {
    let db = Db::open(dir, small(16 * 1024)).unwrap();
    let mut keys: Vec<Vec<u8>> = (0..n).map(|i| format!("key{i:06}").into_bytes()).collect();
    let mut rng = Rng::new(42);
    for i in (1..keys.len()).rev() {
        keys.swap(i, rng.below(i as u64 + 1) as usize);
    }
    for k in &keys {
        db.put(k, &[1u8; 32]).unwrap();
    }
    db.flush().unwrap();
    (db, keys)
}

#[test]
fn bloom_filters_skip_tables_for_absent_keys() {
    let dir = TempDir::new("rp-bloom-absent");
    let (db, _) = shuffled_db(dir.path(), 20_000);
    let tables = db.sst_count() as u64;
    assert!(tables >= 20, "only {tables} SSTables");

    let before = db.stats();
    let lookups = 5_000u64;
    for i in 0..lookups {
        // Between existing keys, so inside every table's key range.
        assert_eq!(db.get(format!("key{:06}x", i * 3).as_bytes()).unwrap(), None);
    }
    let s = db.stats();
    let checks = (s.bloom_skips - before.bloom_skips) + (s.bloom_false_positives - before.bloom_false_positives);
    let fps = s.bloom_false_positives - before.bloom_false_positives;
    let range_skips = s.range_skips - before.range_skips;
    let rate = fps as f64 / checks as f64;
    let blocks = s.data_blocks_read - before.data_blocks_read;
    eprintln!(
        "absent keys: {lookups} gets over {tables} tables -> {range_skips} range skips, \
         {checks} filter checks, {fps} false positives ({:.2}%), {blocks} data blocks read",
        rate * 100.0
    );
    // An absent key is checked against every table: by range or by filter.
    assert_eq!(checks + range_skips, lookups * tables);
    // Shuffled keys: nearly every table spans nearly the whole key space.
    assert!(range_skips < lookups * tables / 20, "{range_skips} range skips");
    assert!(rate < 0.02, "false-positive rate {rate}");
    // One block read per false positive, none otherwise.
    assert_eq!(blocks, fps);
}

#[test]
fn present_keys_probe_about_one_table() {
    let dir = TempDir::new("rp-bloom-present");
    let (db, keys) = shuffled_db(dir.path(), 20_000);
    let before = db.stats();
    for k in keys.iter().step_by(4) {
        assert_eq!(db.get(k).unwrap(), Some(vec![1u8; 32]));
    }
    let s = db.stats();
    let gets = keys.len().div_ceil(4) as u64;
    let probes = s.sst_probes - before.sst_probes;
    let fps = s.bloom_false_positives - before.bloom_false_positives;
    let filtered = s.bloom_skips - before.bloom_skips + fps;
    eprintln!(
        "present keys: {gets} gets -> {probes} table probes ({:.3} per get), {fps} false positives \
         out of {filtered} filter rejections+FPs in newer tables",
        probes as f64 / gets as f64
    );
    // Exactly one probe per get finds the key; every other probe is a false
    // positive in a newer table.
    assert_eq!(probes - fps, gets);
    // Without filters a get would read ~tables/2 tables; the extra probes
    // must be explained by a ~1% false-positive rate.
    assert!((fps as f64) < filtered as f64 * 0.02, "{fps} FPs of {filtered}");
}

#[test]
fn key_range_skips_tables_for_sequential_data() {
    let dir = TempDir::new("rp-range");
    let db = Db::open(dir.path(), small(16 * 1024)).unwrap();
    for i in 0..10_000u32 {
        db.put(format!("key{i:06}").as_bytes(), &[0u8; 32]).unwrap();
    }
    db.flush().unwrap();
    let tables = db.sst_count() as u64;
    let before = db.stats();
    assert_eq!(db.get(b"key000500").unwrap(), Some(vec![0u8; 32]));
    let s = db.stats();
    // Tables don't overlap, and the key was written early, so every newer
    // table is ruled out by key range alone and exactly one table is read.
    // (The search stops there; older tables are never consulted.)
    let range_skips = s.range_skips - before.range_skips;
    assert_eq!(s.sst_probes - before.sst_probes, 1);
    assert_eq!(s.bloom_skips - before.bloom_skips, 0);
    assert!(range_skips >= tables - 3 && range_skips < tables, "{range_skips} of {tables}");
    assert_eq!(s.data_blocks_read - before.data_blocks_read, 1);

    // A bounded scan opens only the tables it overlaps.
    let before = db.stats();
    let got = db.scan(Included(b"key001000"), Excluded(b"key001100")).unwrap();
    assert_eq!(got.len(), 100);
    assert!(db.stats().scan_range_skips - before.scan_range_skips >= tables - 3);
}

/// Bounds chosen at random from a set that includes exact keys, between-keys
/// and out-of-range values.
fn random_bound(rng: &mut Rng) -> Bound<Vec<u8>> {
    let k = format!("key{:03}{}", rng.below(130), if rng.below(2) == 0 { "" } else { "m" }).into_bytes();
    match rng.below(5) {
        0 => Unbounded,
        1 | 2 => Included(k),
        _ => Excluded(k),
    }
}

#[test]
fn iterator_matches_model_across_all_layers() {
    for seed in 1..=4u64 {
        let dir = TempDir::new("rp-iter-model");
        let mut db = Db::open(dir.path(), small(1024 * seed as usize)).unwrap();
        let mut rng = Rng::new(seed);
        let mut model = Model::new();
        for step in 0..6000 {
            let key = format!("key{:03}", rng.below(120)).into_bytes();
            match rng.below(100) {
                0..=54 => {
                    let len = rng.below(50) as usize;
                    let v = rng.bytes(len);
                    db.put(&key, &v).unwrap();
                    model.insert(key, v);
                }
                55..=79 => {
                    db.delete(&key).unwrap();
                    model.remove(&key);
                }
                80..=97 => {
                    let (s, e) = (random_bound(&mut rng), random_bound(&mut rng));
                    let (sb, eb) = (s.as_ref().map(Vec::as_slice), e.as_ref().map(Vec::as_slice));
                    let got: Vec<_> = db.iter(sb, eb).unwrap().map(Result::unwrap).collect();
                    let inverted = match (&s, &e) {
                        (Included(a), Included(b)) => a > b,
                        (Included(a) | Excluded(a), Excluded(b)) | (Excluded(a), Included(b)) => a >= b,
                        _ => false,
                    };
                    let want: Vec<_> = if inverted {
                        Vec::new()
                    } else {
                        model.range::<Vec<u8>, _>((s.clone(), e.clone())).map(|(k, v)| (k.clone(), v.clone())).collect()
                    };
                    assert_eq!(got, want, "seed {seed} step {step} bounds {s:?}..{e:?}");
                }
                98 => db.flush().unwrap(),
                _ => {
                    drop(db);
                    db = Db::open(dir.path(), small(1024 * seed as usize)).unwrap();
                }
            }
        }
        assert!(db.sst_count() > 10, "seed {seed}: {} SSTables", db.sst_count());
    }
}

#[test]
fn iterator_is_a_snapshot() {
    let dir = TempDir::new("rp-snapshot");
    let db = Db::open(dir.path(), small(2048)).unwrap();
    for i in 0..300u32 {
        db.put(format!("k{i:03}").as_bytes(), b"old").unwrap();
    }
    db.delete(b"k100").unwrap();
    let mut it = db.iter(Unbounded, Unbounded).unwrap();
    let first: Vec<_> = it.by_ref().take(10).map(Result::unwrap).collect();
    assert_eq!(first[0], (b"k000".to_vec(), b"old".to_vec()));

    // Mutate everything after the iterator was created, including forcing
    // the memtable it copied from through a flush.
    for i in 0..300u32 {
        db.put(format!("k{i:03}").as_bytes(), b"new").unwrap();
    }
    db.put(b"k100", b"resurrected").unwrap();
    db.delete(b"k200").unwrap();
    db.put(b"k999", b"added").unwrap();
    db.flush().unwrap();

    let rest: Vec<_> = it.map(Result::unwrap).collect();
    assert_eq!(rest.len(), 289); // 300 - 1 deleted - 10 already taken
    assert!(rest.iter().all(|(_, v)| v == b"old"));
    assert!(!rest.iter().any(|(k, _)| k == b"k100" || k == b"k999"));
    assert!(rest.iter().any(|(k, _)| k == b"k200"));

    // Now: 300 keys, minus k200, plus k999.
    let now: Vec<_> = db.scan(Unbounded, Unbounded).unwrap();
    assert_eq!(now.len(), 300);
    assert!(!now.iter().any(|(k, _)| k == b"k200"));
    assert!(now.iter().all(|(k, v)| k == b"k100" || k == b"k999" || v == b"new"));
}

#[test]
fn iterator_reports_corruption_once_then_ends() {
    let dir = TempDir::new("rp-corrupt");
    {
        let db = Db::open(dir.path(), small(1 << 20)).unwrap();
        for i in 0..500u32 {
            db.put(format!("k{i:04}").as_bytes(), &[3u8; 40]).unwrap();
        }
        db.flush().unwrap();
    }
    let sst = fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap())
        .find(|e| parse_file_name(e.file_name().to_str().unwrap()).is_some_and(|(_, k)| k == FileKind::Sst))
        .unwrap()
        .path();
    let mut bytes = fs::read(&sst).unwrap();
    bytes[5000] ^= 0x01; // a data block in the middle of the file
    fs::write(&sst, &bytes).unwrap();

    let db = Db::open(dir.path(), small(1 << 20)).unwrap();
    let results: Vec<_> = db.iter(Unbounded, Unbounded).unwrap().collect();
    let first_err = results.iter().position(Result::is_err).expect("corruption not reported");
    assert_eq!(first_err, results.len() - 1, "iterator must stop after the error");
    assert!(matches!(results[first_err], Err(Error::Corruption(_))));
    // Everything returned before the error is correct.
    for (i, r) in results[..first_err].iter().enumerate() {
        assert_eq!(r.as_ref().unwrap(), &(format!("k{i:04}").into_bytes(), vec![3u8; 40]));
    }
}

#[test]
fn tombstones_in_newer_tables_shadow_older_tables_in_get_and_scan() {
    let dir = TempDir::new("rp-tombstones");
    let db = Db::open(dir.path(), small(1 << 20)).unwrap();
    for i in 0..100u32 {
        db.put(format!("k{i:03}").as_bytes(), b"v").unwrap();
    }
    db.flush().unwrap();
    for i in (0..100u32).step_by(2) {
        db.delete(format!("k{i:03}").as_bytes()).unwrap();
    }
    db.flush().unwrap();
    db.put(b"k010", b"back").unwrap(); // memtable overrides a tombstone
    assert_eq!(db.sst_count(), 2);
    assert_eq!(db.get(b"k000").unwrap(), None);
    assert_eq!(db.get(b"k001").unwrap(), Some(b"v".to_vec()));
    assert_eq!(db.get(b"k010").unwrap(), Some(b"back".to_vec()));
    let keys: Vec<_> = db.scan(Unbounded, Unbounded).unwrap().into_iter().map(|(k, _)| k).collect();
    assert_eq!(keys.len(), 51);
    assert!(keys.contains(&b"k010".to_vec()));
    assert!(!keys.contains(&b"k012".to_vec()));
}
