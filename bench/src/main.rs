//! MiniRocks vs. sled benchmarks.
//!
//! ```text
//! cargo run --release -p minirocks-bench -- all --n 1000000 --reps 3
//! ```
//!
//! `all` runs each engine `--reps` times, each run in a **fresh child
//! process** (`run --engine X`) on a fresh directory, and prints the median
//! of every metric (plus min..max). A run executes these phases in order:
//!
//! | phase       | what                                                         |
//! |-------------|--------------------------------------------------------------|
//! | fillseq     | N puts, keys in ascending order, fresh DB                    |
//! | fillrandom  | N puts, keys uniform in [0, N) with repeats, fresh DB        |
//! | settle      | flush + finish background compaction (MiniRocks) / flush (sled) |
//! | readrandom  | R gets of random keys in [0, N) (≈63% exist)                 |
//! | readmissing | R gets of keys that were never written                       |
//! | seekrandom  | S scans of 100 items from a random start key                 |
//! | scanall     | one full scan                                                 |
//! | seekhot     | S scans again after 50k more puts, with no flush (data in the memtable) |
//! | compact     | full compaction (MiniRocks only; sled has none)              |
//! | fillsync    | M puts with an fsync before each acknowledgement, fresh DB   |
//!
//! Keys are 16 bytes, values 100 bytes of pseudo-random data. Bytes written
//! to disk come from the OS's per-process I/O counters (bytes this process
//! passed to write calls), measured the same way for both engines.

use std::collections::BTreeMap;
use std::fs;
use std::ops::Bound::{Included, Unbounded};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

use minirocks::{Db, Options, SyncPolicy};

const KEY_LEN: usize = 16;

fn mix(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn key(k: u64) -> [u8; KEY_LEN] {
    let mut out = [0u8; KEY_LEN];
    out.copy_from_slice(format!("{k:016}").as_bytes());
    out
}

fn missing_key(k: u64) -> Vec<u8> {
    // Sorts between existing keys, so it is inside every table's key range:
    // only a Bloom filter (or an index probe) can rule it out.
    format!("{k:016}x").into_bytes()
}

fn value(seed: u64, len: usize) -> Vec<u8> {
    let mut v = Vec::with_capacity(len);
    let mut s = seed;
    while v.len() < len {
        s = mix(s);
        v.extend_from_slice(&s.to_le_bytes());
    }
    v.truncate(len);
    v
}

// --------------------------------------------------------------- engines

trait Engine {
    fn put(&self, k: &[u8], v: &[u8]);
    fn get(&self, k: &[u8]) -> Option<Vec<u8>>;
    /// Read up to `n` items starting at `start`; returns how many.
    fn scan(&self, start: &[u8], n: usize) -> usize;
    /// Make everything written so far durable and let background work
    /// (flushes, compactions) finish.
    fn settle(&self);
    /// Full compaction, if the engine has one.
    fn compact(&self) -> bool;
    /// Engine-internal counters worth reporting (MiniRocks only).
    fn internals(&self) -> Vec<(&'static str, f64)> {
        Vec::new()
    }
}

struct MiniRocks(Db);

impl Engine for MiniRocks {
    fn put(&self, k: &[u8], v: &[u8]) {
        self.0.put(k, v).unwrap()
    }
    fn get(&self, k: &[u8]) -> Option<Vec<u8>> {
        self.0.get(k).unwrap()
    }
    fn scan(&self, start: &[u8], n: usize) -> usize {
        let mut count = 0;
        for item in self.0.iter(Included(start), Unbounded).unwrap().take(n) {
            item.unwrap();
            count += 1;
        }
        count
    }
    fn settle(&self) {
        self.0.flush().unwrap();
        self.0.run_pending_compactions().unwrap();
        self.0.sync().unwrap();
    }
    fn compact(&self) -> bool {
        self.0.compact().unwrap();
        true
    }
    fn internals(&self) -> Vec<(&'static str, f64)> {
        let w = self.0.write_stats();
        let r = self.0.stats();
        vec![
            ("internal_write_amp", w.write_amplification()),
            ("tables", self.0.sst_count() as f64),
            ("gets", r.gets as f64),
            ("sst_probes", r.sst_probes as f64),
            ("bloom_skips", r.bloom_skips as f64),
            ("range_skips", r.range_skips as f64),
            ("data_blocks_read", r.data_blocks_read as f64),
            ("compactions", w.compactions as f64),
        ]
    }
}

struct Sled {
    db: sled::Db,
    sync_each: bool,
}

impl Engine for Sled {
    fn put(&self, k: &[u8], v: &[u8]) {
        self.db.insert(k, v).unwrap();
        if self.sync_each {
            self.db.flush().unwrap();
        }
    }
    fn get(&self, k: &[u8]) -> Option<Vec<u8>> {
        self.db.get(k).unwrap().map(|v| v.to_vec())
    }
    fn scan(&self, start: &[u8], n: usize) -> usize {
        let mut count = 0;
        for item in self.db.range(start..).take(n) {
            item.unwrap();
            count += 1;
        }
        count
    }
    fn settle(&self) {
        self.db.flush().unwrap();
    }
    fn compact(&self) -> bool {
        false
    }
}

fn open(engine: &str, dir: &Path, sync_each: bool) -> Box<dyn Engine> {
    match engine {
        // Defaults, except the sync policy: `Never` means each write goes to
        // the OS page cache (no fsync), which matches sled's default of
        // fsyncing in the background. `Always` fsyncs before each ack.
        "minirocks" => {
            let sync = if sync_each { SyncPolicy::Always } else { SyncPolicy::Never };
            Box::new(MiniRocks(Db::open(dir, Options { sync, ..Options::default() }).unwrap()))
        }
        // sled defaults: 1 GiB page cache, background flush every 500 ms.
        "sled" => Box::new(Sled { db: sled::Config::new().path(dir).open().unwrap(), sync_each }),
        other => panic!("unknown engine {other}"),
    }
}

// ----------------------------------------------------------- measurement

#[cfg(windows)]
fn bytes_written() -> u64 {
    #[repr(C)]
    #[derive(Default)]
    struct IoCounters {
        read_ops: u64,
        write_ops: u64,
        other_ops: u64,
        read_bytes: u64,
        write_bytes: u64,
        other_bytes: u64,
    }
    extern "system" {
        fn GetCurrentProcess() -> isize;
        fn GetProcessIoCounters(process: isize, counters: *mut IoCounters) -> i32;
    }
    let mut c = IoCounters::default();
    // SAFETY: GetCurrentProcess returns a pseudo-handle that needs no
    // closing; `c` is a correctly laid out IO_COUNTERS.
    let ok = unsafe { GetProcessIoCounters(GetCurrentProcess(), &mut c) };
    assert!(ok != 0, "GetProcessIoCounters failed");
    c.write_bytes
}

#[cfg(not(windows))]
fn bytes_written() -> u64 {
    // Bytes passed to write(2) and friends, the same notion as on Windows.
    let s = fs::read_to_string("/proc/self/io").expect("/proc/self/io");
    s.lines().find_map(|l| l.strip_prefix("wchar: ")).unwrap().trim().parse().unwrap()
}

fn dir_size(dir: &Path) -> u64 {
    let mut total = 0;
    for e in fs::read_dir(dir).unwrap().flatten() {
        let md = e.metadata().unwrap();
        total += if md.is_dir() { dir_size(&e.path()) } else { md.len() };
    }
    total
}

/// One metric line: `R <name> <value>`.
fn report(name: &str, value: f64) {
    println!("R {name} {value}");
}

fn timed<T>(f: impl FnOnce() -> T) -> (T, f64) {
    let t = Instant::now();
    let r = f();
    (r, t.elapsed().as_secs_f64())
}

struct RunArgs {
    engine: String,
    dir: PathBuf,
    n: u64,
    reads: u64,
    scans: u64,
    sync_n: u64,
    value_len: usize,
}

fn run(a: RunArgs) {
    let _ = fs::remove_dir_all(&a.dir);
    fs::create_dir_all(&a.dir).unwrap();
    let user_bytes = |ops: u64| (ops * (KEY_LEN + a.value_len) as u64) as f64;

    // fillseq
    {
        let dir = a.dir.join("seq");
        let db = open(&a.engine, &dir, false);
        let w0 = bytes_written();
        let ((), secs) = timed(|| {
            for k in 0..a.n {
                db.put(&key(k), &value(k, a.value_len));
            }
        });
        report("fillseq.ops_per_sec", a.n as f64 / secs);
        let ((), settle) = timed(|| db.settle());
        report("fillseq.settle_sec", settle);
        report("fillseq.write_amp", (bytes_written() - w0) as f64 / user_bytes(a.n));
        for (name, v) in db.internals() {
            if name == "internal_write_amp" {
                report("fillseq.internal_write_amp", v);
            }
        }
        drop(db);
        let _ = fs::remove_dir_all(&dir);
    }

    // fillrandom + settle
    let dir = a.dir.join("random");
    let db = open(&a.engine, &dir, false);
    let mut latest: BTreeMap<u64, u64> = BTreeMap::new(); // key -> value seed
    let w0 = bytes_written();
    let ((), secs) = timed(|| {
        for i in 0..a.n {
            let k = mix(i ^ 0xA5A5) % a.n;
            db.put(&key(k), &value(i, a.value_len));
            latest.insert(k, i);
        }
    });
    report("fillrandom.ops_per_sec", a.n as f64 / secs);
    let ((), settle) = timed(|| db.settle());
    report("fillrandom.settle_sec", settle);
    let written = bytes_written() - w0;
    report("fillrandom.write_amp", written as f64 / user_bytes(a.n));
    let before_reads: BTreeMap<&str, f64> = db.internals().into_iter().collect();
    if let Some(v) = before_reads.get("internal_write_amp") {
        report("fillrandom.internal_write_amp", *v);
        report("fillrandom.tables_after_settle", before_reads["tables"]);
        report("fillrandom.compactions", before_reads["compactions"]);
    }
    let live = latest.len() as u64;
    report("fillrandom.unique_keys", live as f64);
    let logical = user_bytes(live);
    report("space_amp.before_compaction", dir_size(&dir) as f64 / logical);

    // readrandom / readmissing
    let ((hits, _), secs) = timed(|| {
        let mut hits = 0u64;
        for i in 0..a.reads {
            // An independent stream (not `i ^ const`, which over i < 2^20
            // just permutes the fill's inputs and made every read a hit in
            // the first run).
            let k = mix(mix(i).wrapping_add(0xDEAD_BEEF)) % a.n;
            let got = db.get(&key(k));
            if let Some(&seed) = latest.get(&k) {
                assert_eq!(got.as_deref(), Some(value(seed, a.value_len).as_slice()), "wrong value for key {k}");
                hits += 1;
            } else {
                assert!(got.is_none());
            }
        }
        (hits, ())
    });
    report("readrandom.ops_per_sec", a.reads as f64 / secs);
    report("readrandom.hit_rate", hits as f64 / a.reads as f64);
    let after_reads: BTreeMap<&str, f64> = db.internals().into_iter().collect();
    if let Some(gets) = after_reads.get("gets") {
        let per_get = |k: &str| (after_reads[k] - before_reads[k]) / (gets - before_reads["gets"]);
        report("readrandom.sst_probes_per_get", per_get("sst_probes"));
        report("readrandom.data_blocks_per_get", per_get("data_blocks_read"));
        report("readrandom.tables_checked_per_get", per_get("sst_probes") + per_get("bloom_skips") + per_get("range_skips"));
    }
    let (misses, secs) = timed(|| (0..a.reads).filter(|&i| db.get(&missing_key(mix(i) % a.n)).is_none()).count());
    assert_eq!(misses as u64, a.reads);
    report("readmissing.ops_per_sec", a.reads as f64 / secs);

    // seekrandom (100 items) and full scan
    let (items, secs) = timed(|| (0..a.scans).map(|i| db.scan(&key(mix(i ^ 0x77) % a.n), 100)).sum::<usize>());
    report("seekrandom.scans_per_sec", a.scans as f64 / secs);
    report("seekrandom.items_per_sec", items as f64 / secs);
    let (all, secs) = timed(|| db.scan(&[], usize::MAX));
    assert_eq!(all as u64, live);
    report("scanall.items_per_sec", all as f64 / secs);

    // seekhot: same scans with fresh data sitting in the memtable
    let extra = 50_000.min(a.n);
    for i in 0..extra {
        let k = mix(i ^ 0x1234_5678) % a.n;
        db.put(&key(k), &value(a.n + i, a.value_len));
        latest.insert(k, a.n + i);
    }
    let (_, secs) = timed(|| (0..a.scans).map(|i| db.scan(&key(mix(i ^ 0x99) % a.n), 100)).sum::<usize>());
    report("seekhot.scans_per_sec", a.scans as f64 / secs);

    // compaction: space amplification afterwards
    db.settle();
    let live = latest.len() as u64;
    let (did, secs) = timed(|| db.compact());
    if did {
        report("compact.sec", secs);
        report("space_amp.after_compaction", dir_size(&dir) as f64 / user_bytes(live));
    }
    drop(db);
    let _ = fs::remove_dir_all(&dir);

    // fillsync: fsync before every acknowledgement
    {
        let dir = a.dir.join("sync");
        let db = open(&a.engine, &dir, true);
        let ((), secs) = timed(|| {
            for k in 0..a.sync_n {
                db.put(&key(mix(k) % a.n), &value(k, a.value_len));
            }
        });
        report("fillsync.ops_per_sec", a.sync_n as f64 / secs);
        drop(db);
    }
    let _ = fs::remove_dir_all(&a.dir);
}

// ---------------------------------------------------------------- driver

fn arg(args: &[String], name: &str) -> Option<String> {
    args.iter().position(|a| a == name).and_then(|i| args.get(i + 1)).cloned()
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let num = |name: &str, default: u64| arg(&args, name).map_or(default, |s| s.parse().unwrap());
    let n = num("--n", 1_000_000);
    let reads = num("--reads", 200_000);
    let scans = num("--scans", 20_000);
    let sync_n = num("--sync-n", 5_000);
    let value_len = num("--value-len", 100) as usize;
    let base = arg(&args, "--dir").map(PathBuf::from).unwrap_or_else(|| std::env::temp_dir().join("minirocks-bench"));

    match args.first().map(String::as_str) {
        Some("run") => run(RunArgs {
            engine: arg(&args, "--engine").expect("--engine"),
            dir: base,
            n,
            reads,
            scans,
            sync_n,
            value_len,
        }),
        Some("all") => {
            let reps = num("--reps", 3);
            let engines = arg(&args, "--engines").unwrap_or_else(|| "minirocks,sled".into());
            println!("# n={n} reads={reads} scans={scans} sync_n={sync_n} key={KEY_LEN}B value={value_len}B reps={reps}");
            let mut results: BTreeMap<(String, String), Vec<f64>> = BTreeMap::new();
            let mut order: Vec<String> = Vec::new();
            for rep in 0..reps {
                for engine in engines.split(',') {
                    let dir = base.join(format!("{engine}-{rep}"));
                    let t = Instant::now();
                    let out = Command::new(std::env::current_exe().unwrap())
                        .args(["run", "--engine", engine, "--dir"])
                        .arg(&dir)
                        .args(["--n", &n.to_string(), "--reads", &reads.to_string(), "--scans", &scans.to_string()])
                        .args(["--sync-n", &sync_n.to_string(), "--value-len", &value_len.to_string()])
                        .output()
                        .unwrap();
                    assert!(out.status.success(), "{engine} run failed:\n{}", String::from_utf8_lossy(&out.stderr));
                    for line in String::from_utf8_lossy(&out.stdout).lines() {
                        if let Some(rest) = line.strip_prefix("R ") {
                            let (name, v) = rest.split_once(' ').unwrap();
                            if !order.iter().any(|o| o == name) {
                                order.push(name.to_string());
                            }
                            results.entry((name.to_string(), engine.to_string())).or_default().push(v.parse().unwrap());
                            println!("  rep {rep} {engine:<9} {name:<30} {v}");
                        }
                    }
                    println!("  rep {rep} {engine} done in {:.1} s", t.elapsed().as_secs_f64());
                }
            }
            println!("\n| metric | {} |", engines.replace(',', " | "));
            println!("|---|{}|", engines.split(',').map(|_| "---").collect::<Vec<_>>().join("|"));
            for name in &order {
                let cells: Vec<String> = engines
                    .split(',')
                    .map(|e| match results.get(&(name.clone(), e.to_string())) {
                        Some(v) => fmt_cell(v),
                        None => "n/a".into(),
                    })
                    .collect();
                println!("| {name} | {} |", cells.join(" | "));
            }
        }
        _ => eprintln!("usage: minirocks-bench all [--n N --reps R ...] | run --engine minirocks|sled ..."),
    }
}

/// Median, with the range across repetitions.
fn fmt_cell(v: &[f64]) -> String {
    let mut s = v.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let med = s[s.len() / 2];
    let f = |x: f64| if x >= 1000.0 { format!("{:.0}", x) } else if x >= 10.0 { format!("{x:.1}") } else { format!("{x:.2}") };
    if s.len() > 1 {
        format!("{} ({}–{})", f(med), f(s[0]), f(s[s.len() - 1]))
    } else {
        f(med)
    }
}
