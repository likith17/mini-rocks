//! MiniRocks crash-consistency harness.
//!
//! ```text
//! cargo run --release --features failpoints --bin crashtest -- --cycles 1000
//! ```
//!
//! Each cycle:
//!
//! 1. Spawn a child process that opens the database and runs `--threads`
//!    writer threads. Thread `t` performs operations `t.start, t.start+1, ...`,
//!    each a deterministic function of (seed, t, index), and prints
//!    `A <t> <index>` after each one returns `Ok`. A printed line means an
//!    acknowledged write.
//! 2. Kill the child in one of three ways: after a random number of ACKs,
//!    after a random delay (which can land inside recovery), or by arming a
//!    named crash point in the engine (mid-flush, mid-compaction, torn WAL or
//!    manifest write, mid-recovery, ...). Kills are TerminateProcess/SIGKILL,
//!    and crash points `exit()` with no destructors: nothing is flushed or
//!    synced on the way out.
//! 3. Sometimes inject a fault into the files: cut or garble the WAL tail,
//!    plant a partial `.sst.tmp`, or truncate or bit-flip a live SSTable.
//! 4. Copy the directory and open the copy. For every thread, the recovered
//!    state must equal the model after exactly the acknowledged operations,
//!    or those plus the one in flight. Point reads must agree with scans. The
//!    original directory is left as the crash left it, so the next child has
//!    to recover it (and may itself be killed while doing so).
//!
//! Exits non-zero on the first failure, keeping the failing directory.

use std::collections::BTreeMap;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::ops::Bound::{Excluded, Included};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use minirocks::filename::{parse_file_name, sst_file_name, temp_sst_file_name, wal_file_name, FileKind};
use minirocks::manifest::{read_current, read_manifest};
use minirocks::{Db, Error, Options, SyncPolicy, WalRecoveryMode};

// ------------------------------------------------------------ determinism

fn mix(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = mix(self.0);
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

type Map = BTreeMap<Vec<u8>, Vec<u8>>;

struct Op {
    key: Vec<u8>,
    value: Option<Vec<u8>>,
}

fn key_prefix(t: usize) -> String {
    format!("t{t:02}:")
}

/// Operation `i` of thread `t`: 80% puts, 20% deletes; half the keys hit a
/// hot 10% of the key space, so overwrites and deletes of live keys are
/// common. Values carry `t:i:` so a misplaced value is recognisable.
fn gen_op(seed: u64, t: usize, i: u64, keys: u64) -> Op {
    let h = mix(seed ^ mix(((t as u64) << 48) ^ mix(i)));
    let k = if h & 1 == 0 { (h >> 8) % (keys / 10).max(1) } else { (h >> 8) % keys };
    let key = format!("{}k{k:05}", key_prefix(t)).into_bytes();
    let h2 = mix(h);
    if h2 % 100 < 20 {
        return Op { key, value: None };
    }
    let len = (mix(h2) % 160) as usize;
    let mut v = format!("{t}:{i}:").into_bytes();
    v.extend((0..len).map(|j| b'a' + (mix(h2 ^ j as u64) % 26) as u8));
    Op { key, value: Some(v) }
}

fn apply(model: &mut BTreeMap<Vec<u8>, Vec<u8>>, op: Op) {
    match op.value {
        Some(v) => {
            model.insert(op.key, v);
        }
        None => {
            model.remove(&op.key);
        }
    }
}

// ------------------------------------------------------------------ child

struct ChildArgs {
    dir: PathBuf,
    seed: u64,
    keys: u64,
    starts: Vec<u64>,
    sync: SyncPolicy,
    memtable: usize,
}

fn child_options(sync: SyncPolicy, memtable: usize) -> Options {
    Options { sync, memtable_size: memtable, block_size: 1024, ..Options::default() }
}

fn run_child(a: ChildArgs) -> ! {
    // Any panic (an Err from the engine, typically) kills the whole process
    // with a distinctive code so the driver reports it.
    std::panic::set_hook(Box::new(|info| {
        eprintln!("CHILD PANIC: {info}");
        std::process::exit(101);
    }));
    let db = Arc::new(Db::open(&a.dir, child_options(a.sync, a.memtable)).expect("open"));
    {
        let mut out = std::io::stdout().lock();
        writeln!(out, "OPENED").unwrap();
        out.flush().unwrap();
    }
    let handles: Vec<_> = a
        .starts
        .iter()
        .enumerate()
        .map(|(t, &start)| {
            let db = Arc::clone(&db);
            let (seed, keys) = (a.seed, a.keys);
            std::thread::spawn(move || {
                for i in start.. {
                    let op = gen_op(seed, t, i, keys);
                    match &op.value {
                        Some(v) => db.put(&op.key, v).expect("put"),
                        None => db.delete(&op.key).expect("delete"),
                    }
                    let mut out = std::io::stdout().lock();
                    writeln!(out, "A {t} {i}").unwrap();
                    out.flush().unwrap();
                }
            })
        })
        .collect();
    for h in handles {
        let _ = h.join();
    }
    std::process::exit(0)
}

// ----------------------------------------------------------------- driver

/// Crash points and how many hits to let pass before firing (upper bound).
const CRASHPOINTS: &[(&str, u64)] = &[
    ("wal.append.torn", 3000),
    ("rotate.after_new_wal", 6),
    ("sst.write_block", 40),
    ("sst.before_rename", 6),
    ("sst.after_rename", 6),
    ("flush.before_manifest", 6),
    ("flush.after_manifest", 6),
    ("flush.before_wal_delete", 6),
    ("compaction.before_manifest", 3),
    ("compaction.after_manifest", 3),
    ("compaction.before_obsolete", 3),
    ("manifest.append.torn", 8),
    ("manifest.create.before_current", 1),
    ("current.before_rename", 1),
    ("recovery.after_wal_truncate", 1),
    ("recovery.after_orphan_delete", 1),
    ("recovery.before_old_manifest_delete", 1),
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fault {
    WalTruncate,
    WalGarbage,
    WalTailFlip,
    TempSst,
    SstTruncate,
    SstFlip,
}

const FAULTS: [Fault; 6] =
    [Fault::WalTruncate, Fault::WalGarbage, Fault::WalTailFlip, Fault::TempSst, Fault::SstTruncate, Fault::SstFlip];

#[derive(Default)]
struct Report {
    cycles: u64,
    kills_by_ack: u64,
    kills_by_time: u64,
    kills_before_open: u64,
    crashpoints_fired: BTreeMap<&'static str, u64>,
    crashpoints_not_reached: u64,
    faults: BTreeMap<String, u64>,
    faults_detected_at_open: u64,
    faults_detected_at_read: u64,
    sst_probes_skipped: u64,
    point_in_time_recoveries: u64,
    acked_ops: u64,
    in_flight_applied: u64,
    in_flight_absent: u64,
    acked_ops_lost_to_injected_faults: u64,
    verified_keys: u64,
    max_tables_seen: usize,
}

impl Report {
    fn print(&self, elapsed: Duration) {
        println!("--- crashtest report after {} cycles ({:.0} s) ---", self.cycles, elapsed.as_secs_f64());
        println!("kills after N acks:            {}", self.kills_by_ack);
        println!("kills after random delay:      {} ({} before the child finished opening)", self.kills_by_time, self.kills_before_open);
        let fired: u64 = self.crashpoints_fired.values().sum();
        println!("crash points fired:            {fired} (armed but not reached, then killed: {})", self.crashpoints_not_reached);
        for (name, n) in &self.crashpoints_fired {
            println!("    {name:<38} {n}");
        }
        let injected: u64 = self.faults.values().sum();
        println!("faults injected:               {injected}");
        for (name, n) in &self.faults {
            println!("    {name:<38} {n}");
        }
        println!(
            "  corrupt SSTables detected at open: {}, at read: {} (skipped, no live SSTable yet: {})",
            self.faults_detected_at_open, self.faults_detected_at_read, self.sst_probes_skipped
        );
        println!("  PointInTime recoveries after mid-log WAL damage: {}", self.point_in_time_recoveries);
        println!("acknowledged ops verified:     {}", self.acked_ops);
        println!("  in-flight op found applied: {}, absent: {}", self.in_flight_applied, self.in_flight_absent);
        println!("  acked ops lost ONLY to injected WAL damage (allowed): {}", self.acked_ops_lost_to_injected_faults);
        println!("key comparisons:               {}", self.verified_keys);
        println!("max live SSTables seen:        {}", self.max_tables_seen);
    }
}

struct Config {
    cycles: u64,
    seed: u64,
    threads: usize,
    keys: u64,
    dir: PathBuf,
}

fn parse_args() -> Result<Option<ChildArgs>, Config> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let get = |name: &str| args.iter().position(|a| a == name).and_then(|i| args.get(i + 1)).cloned();
    if args.first().map(String::as_str) == Some("child") {
        let starts = get("--starts").unwrap().split(',').map(|s| s.parse().unwrap()).collect();
        let sync = match get("--sync").unwrap().as_str() {
            "always" => SyncPolicy::Always,
            "every8" => SyncPolicy::EveryN(8),
            _ => SyncPolicy::Never,
        };
        return Ok(Some(ChildArgs {
            dir: get("--dir").unwrap().into(),
            seed: get("--seed").unwrap().parse().unwrap(),
            keys: get("--keys").unwrap().parse().unwrap(),
            starts,
            sync,
            memtable: get("--memtable").unwrap().parse().unwrap(),
        }));
    }
    let seed = get("--seed").map(|s| s.parse().unwrap()).unwrap_or(1);
    Err(Config {
        cycles: get("--cycles").map(|s| s.parse().unwrap()).unwrap_or(200),
        seed,
        threads: get("--threads").map(|s| s.parse().unwrap()).unwrap_or(4),
        keys: get("--keys").map(|s| s.parse().unwrap()).unwrap_or(3000),
        dir: get("--dir").map(PathBuf::from).unwrap_or_else(|| std::env::temp_dir().join(format!("minirocks-crashtest-{seed}"))),
    })
}

fn main() {
    let cfg = match parse_args() {
        Ok(Some(child)) => run_child(child),
        Ok(None) => unreachable!(),
        Err(cfg) => cfg,
    };
    if let Err(msg) = drive(&cfg) {
        eprintln!("\nFAILURE: {msg}");
        std::process::exit(1);
    }
}

struct ThreadModel {
    applied: u64,
    map: BTreeMap<Vec<u8>, Vec<u8>>,
}

/// What the driver has read from the child's stdout.
struct Progress {
    last_ack: Vec<Option<u64>>,
    opened: bool,
    acks: u64,
}

impl Progress {
    fn handle(&mut self, line: &str) {
        if line == "OPENED" {
            self.opened = true;
        } else if let Some(rest) = line.strip_prefix("A ") {
            let (t, i) = rest.split_once(' ').expect("complete ACK line");
            let (t, i): (usize, u64) = (t.parse().unwrap(), i.parse().unwrap());
            // Per-thread ACKs are printed in order; never move backwards.
            self.last_ack[t] = Some(self.last_ack[t].map_or(i, |prev| prev.max(i)));
            self.acks += 1;
        }
    }
}

fn drive(cfg: &Config) -> Result<(), String> {
    let _ = fs::remove_dir_all(&cfg.dir);
    fs::create_dir_all(&cfg.dir).map_err(|e| e.to_string())?;
    let db_dir = cfg.dir.join("db");
    let probe_dir = cfg.dir.join("probe");
    let exe = std::env::current_exe().unwrap();
    let mut rng = Rng(cfg.seed);
    let mut models: Vec<ThreadModel> = (0..cfg.threads).map(|_| ThreadModel { applied: 0, map: BTreeMap::new() }).collect();
    let mut report = Report::default();
    let started = Instant::now();
    println!(
        "crashtest: seed {} cycles {} threads {} keys/thread {} dir {}",
        cfg.seed,
        cfg.cycles,
        cfg.threads,
        cfg.keys,
        cfg.dir.display()
    );

    for cycle in 0..cfg.cycles {
        let ctx = |m: String| format!("cycle {cycle} (seed {}): {m}", cfg.seed);
        let sync = match rng.below(10) {
            0..=1 => "always",
            2 => "every8",
            _ => "never",
        };
        let memtable = [4096usize, 16 * 1024, 64 * 1024][rng.below(3) as usize];
        let starts: Vec<String> = models.iter().map(|m| m.applied.to_string()).collect();

        // ---- 1-2. run and kill the child
        let mode = rng.below(100);
        let mut cmd = Command::new(&exe);
        cmd.args(["child", "--dir"])
            .arg(&db_dir)
            .args(["--seed", &cfg.seed.to_string(), "--keys", &cfg.keys.to_string()])
            .args(["--starts", &starts.join(","), "--sync", sync, "--memtable", &memtable.to_string()])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut armed = None;
        if mode >= 70 {
            let (name, max) = CRASHPOINTS[rng.below(CRASHPOINTS.len() as u64) as usize];
            let n = 1 + rng.below(max);
            cmd.env("MINIROCKS_CRASHPOINT", format!("{name}:{n}"));
            armed = Some(name);
        }
        let mut child = cmd.spawn().map_err(|e| ctx(format!("spawn: {e}")))?;
        let (tx, rx) = mpsc::channel::<String>();
        let stdout = child.stdout.take().unwrap();
        let reader = std::thread::spawn(move || {
            // Only newline-terminated lines count. A kill can cut the last
            // line short ("A 3 12" from "A 3 1234"), which would report an
            // ACK index smaller than the real one. An op whose ACK line was
            // cut off counts as in flight, which the verifier allows for.
            // (Harness bug found in campaign 1: `lines()` returned the
            // fragment and parsing panicked.)
            let mut r = BufReader::new(stdout);
            let mut buf = Vec::new();
            loop {
                buf.clear();
                match r.read_until(b'\n', &mut buf) {
                    Ok(0) | Err(_) => break,
                    Ok(_) if buf.ends_with(b"\n") => {
                        let line = String::from_utf8_lossy(&buf).trim_end().to_string();
                        if tx.send(line).is_err() {
                            break;
                        }
                    }
                    Ok(_) => break, // partial final line
                }
            }
        });
        let stderr = child.stderr.take().unwrap();
        let err_reader = std::thread::spawn(move || {
            let mut s = String::new();
            let _ = std::io::Read::read_to_string(&mut BufReader::new(stderr), &mut s);
            s
        });

        let mut p = Progress { last_ack: vec![None; cfg.threads], opened: false, acks: 0 };
        if mode < 40 {
            // Kill after N acknowledged writes.
            let target = 1 + rng.below(20_000);
            while p.acks < target {
                match rx.recv_timeout(Duration::from_secs(30)) {
                    Ok(line) => p.handle(&line),
                    Err(_) => break, // child died (e.g. panic) or stalled
                }
            }
            report.kills_by_ack += 1;
        } else if mode < 70 {
            // Kill after a random delay. Half are short enough to land in
            // process start-up or inside open() (i.e. during recovery).
            let ms = if rng.chance(50) { rng.below(60) } else { rng.below(400) };
            std::thread::sleep(Duration::from_millis(ms));
            report.kills_by_time += 1;
        } else {
            // Crash point: wait for the child to die by itself, or give up.
            let deadline = Instant::now() + Duration::from_secs(20);
            loop {
                if matches!(child.try_wait(), Ok(Some(_))) || Instant::now() > deadline || p.acks > 150_000 {
                    break;
                }
                match rx.recv_timeout(Duration::from_millis(50)) {
                    Ok(line) => p.handle(&line),
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
                }
            }
        }
        let _ = child.kill();
        let status = child.wait().map_err(|e| ctx(e.to_string()))?;
        reader.join().unwrap();
        // ACKs already in the pipe were printed before the process died.
        for line in rx.try_iter() {
            p.handle(&line);
        }
        let Progress { last_ack, opened, .. } = p;
        let stderr_text = err_reader.join().unwrap();
        if stderr_text.contains("CHILD PANIC") || status.code() == Some(101) {
            return Err(ctx(format!("child failed (sync {sync}, memtable {memtable}):\n{stderr_text}")));
        }
        match (armed, stderr_text.contains("CRASHPOINT")) {
            (Some(name), true) => *report.crashpoints_fired.entry(name).or_default() += 1,
            (Some(_), false) => report.crashpoints_not_reached += 1,
            _ => {}
        }
        if (40..70).contains(&mode) && !opened {
            report.kills_before_open += 1;
        }

        // ---- 3. maybe inject a fault into the real directory
        let mut window = 0u64; // acknowledged ops per thread that may be lost
        let mut fault = None;
        if rng.chance(25) {
            let mut f = FAULTS[rng.below(FAULTS.len() as u64) as usize];
            // WAL damage can destroy up to ~8 acknowledged records per thread.
            // The model can only rewind within this cycle, so require that
            // every thread acknowledged more than that this cycle.
            let acked_this_cycle = |t: usize| last_ack[t].map_or(0, |a| a + 1 - models[t].applied);
            if matches!(f, Fault::WalTruncate | Fault::WalTailFlip) && (0..cfg.threads).any(|t| acked_this_cycle(t) < 10) {
                f = Fault::WalGarbage;
            }
            fault = Some(f);
            *report.faults.entry(format!("{f:?}")).or_default() += 1;
            match f {
                Fault::WalTruncate => {
                    if let Some(p) = newest_wal(&db_dir) {
                        let len = fs::metadata(&p).unwrap().len();
                        let cut = 1 + rng.below(120.min(len.max(1)));
                        if len > 0 {
                            set_len(&p, len.saturating_sub(cut));
                            // Each record is >= 21 bytes; partial ones count too.
                            window = cut / 21 + 2;
                        }
                    }
                }
                Fault::WalGarbage => {
                    if let Some(p) = newest_wal(&db_dir) {
                        let n = 1 + rng.below(200) as usize;
                        let junk: Vec<u8> = (0..n).map(|_| rng.next() as u8).collect();
                        fs::OpenOptions::new().append(true).open(&p).unwrap().write_all(&junk).unwrap();
                    }
                }
                Fault::WalTailFlip => {
                    if let Some(p) = newest_wal(&db_dir) {
                        let mut bytes = fs::read(&p).unwrap();
                        if bytes.len() >= 64 {
                            let at = bytes.len() - 1 - rng.below(64) as usize;
                            bytes[at] ^= 1 << rng.below(8);
                            fs::write(&p, &bytes).unwrap();
                            window = 64 / 21 + 2;
                            // If the damaged record is followed by a valid
                            // one, the default mode refuses to open: an
                            // operator would choose PointInTime. Do that on
                            // the real directory, like an operator would.
                            if let Err(Error::Corruption(_)) = Db::open(&db_dir, verify_options(WalRecoveryMode::TolerateTornTail)) {
                                Db::open(&db_dir, verify_options(WalRecoveryMode::PointInTime))
                                    .map_err(|e| ctx(format!("PointInTime recovery failed: {e}")))?;
                                report.point_in_time_recoveries += 1;
                            }
                        }
                    }
                }
                Fault::TempSst => {
                    let tmp = db_dir.join(temp_sst_file_name(900_000 + cycle));
                    let junk: Vec<u8> = (0..rng.below(5000)).map(|_| rng.next() as u8).collect();
                    fs::write(tmp, junk).unwrap();
                }
                // Applied to a separate probe copy below.
                Fault::SstTruncate | Fault::SstFlip => {}
            }
        }

        // ---- 4. verify on a copy
        copy_dir(&db_dir, &probe_dir);
        let db = Db::open(&probe_dir, verify_options(WalRecoveryMode::TolerateTornTail))
            .map_err(|e| ctx(format!("recovery failed after kill (fault {fault:?}): {e}")))?;
        if fault == Some(Fault::TempSst) && db.recovery_report().temp_files_removed == 0 {
            return Err(ctx("planted .sst.tmp was not removed".into()));
        }
        report.max_tables_seen = report.max_tables_seen.max(db.sst_count());
        for (t, model) in models.iter_mut().enumerate() {
            let start = model.applied;
            let c0 = last_ack[t].map_or(start, |a| a + 1); // all acked ops applied
            let c1 = c0 + 1; // ... plus the one in flight
            let lo = c0.saturating_sub(window).max(start);
            let state = thread_state(&db, t).map_err(|e| ctx(format!("thread {t} scan: {e}")))?;
            let found = find_match(cfg, t, model, lo, c1, &state).ok_or_else(|| {
                ctx(format!(
                    "thread {t}: recovered state matches no allowed prefix (acked through {:?}, window {window}, fault {fault:?}). {}",
                    last_ack[t],
                    describe_diff(cfg, t, model, c0, &state)
                ))
            })?;
            report.acked_ops += c0 - start;
            if found.0 == c1 {
                report.in_flight_applied += 1;
            } else if found.0 == c0 {
                report.in_flight_absent += 1;
            } else {
                report.acked_ops_lost_to_injected_faults += c0 - found.0;
            }
            // Point reads must agree with the scan (Bloom filters, key range
            // checks and the merge iterator are separate code paths).
            for _ in 0..50 {
                let k = format!("{}k{:05}", key_prefix(t), rng.below(cfg.keys)).into_bytes();
                let got = db.get(&k).map_err(|e| ctx(format!("get: {e}")))?;
                if got.as_ref() != found.1.get(&k) {
                    return Err(ctx(format!("thread {t}: get({}) = {:?}, scan/model say {:?}", show(&k), got.map(|v| show(&v)), found.1.get(&k).map(|v| show(v)))));
                }
            }
            report.verified_keys += state.len() as u64 + 50;
            model.applied = found.0;
            model.map = found.1;
        }
        drop(db);

        // ---- corrupt-SSTable probes: must be detected, never returned
        if let Some(f @ (Fault::SstTruncate | Fault::SstFlip)) = fault {
            copy_dir(&db_dir, &probe_dir);
            // Let recovery settle the probe's file set first.
            drop(Db::open(&probe_dir, verify_options(WalRecoveryMode::TolerateTornTail)).map_err(|e| ctx(e.to_string()))?);
            let live = live_ssts(&probe_dir);
            if let Some(&victim) = live.get(rng.below(live.len().max(1) as u64) as usize) {
                let p = probe_dir.join(sst_file_name(victim));
                let mut bytes = fs::read(&p).unwrap();
                if f == Fault::SstTruncate {
                    bytes.truncate(rng.below(bytes.len() as u64) as usize);
                } else {
                    let at = rng.below(bytes.len() as u64) as usize;
                    bytes[at] ^= 1 << rng.below(8);
                }
                fs::write(&p, &bytes).unwrap();
                probe_corrupt(cfg, &probe_dir, &models, &mut report).map_err(ctx)?;
            } else {
                report.sst_probes_skipped += 1;
            }
        }

        report.cycles += 1;
        if (cycle + 1) % 50 == 0 {
            let total: u64 = models.iter().map(|m| m.applied).sum();
            println!(
                "  cycle {:>5}: {} ops applied in total, {} acked verified, {} crash points fired, {:.0} s",
                cycle + 1,
                total,
                report.acked_ops,
                report.crashpoints_fired.values().sum::<u64>(),
                started.elapsed().as_secs_f64()
            );
        }
    }
    report.print(started.elapsed());
    println!("RESULT: PASS ({} cycles, 0 failures)", report.cycles);
    let _ = fs::remove_dir_all(&cfg.dir);
    Ok(())
}

/// Find the largest j in [lo, hi] such that the state equals the model
/// after operations 0..j. Returns j and that model.
fn find_match(cfg: &Config, t: usize, model: &ThreadModel, lo: u64, hi: u64, state: &Map) -> Option<(u64, Map)> {
    let mut m = model.map.clone();
    for i in model.applied..lo {
        apply(&mut m, gen_op(cfg.seed, t, i, cfg.keys));
    }
    let mut best = None;
    for j in lo..=hi {
        if &m == state {
            best = Some((j, m.clone()));
        }
        if j < hi {
            apply(&mut m, gen_op(cfg.seed, t, j, cfg.keys));
        }
    }
    best
}

fn describe_diff(cfg: &Config, t: usize, model: &ThreadModel, c0: u64, state: &BTreeMap<Vec<u8>, Vec<u8>>) -> String {
    let mut m = model.map.clone();
    for i in model.applied..c0 {
        apply(&mut m, gen_op(cfg.seed, t, i, cfg.keys));
    }
    let mut diffs = Vec::new();
    for k in m.keys().chain(state.keys()) {
        if m.get(k) != state.get(k) && diffs.len() < 5 {
            diffs.push(format!("{}: expected {:?}, got {:?}", show(k), m.get(k).map(|v| show(v)), state.get(k).map(|v| show(v))));
        }
    }
    diffs.dedup();
    format!("model has {} keys, db has {}; first differences: {}", m.len(), state.len(), diffs.join("; "))
}

fn show(b: &[u8]) -> String {
    let s = String::from_utf8_lossy(b);
    if s.len() > 24 {
        format!("{}..", &s[..24])
    } else {
        s.into_owned()
    }
}

fn thread_state(db: &Db, t: usize) -> minirocks::Result<BTreeMap<Vec<u8>, Vec<u8>>> {
    let lo = key_prefix(t).into_bytes();
    let hi = format!("t{t:02};").into_bytes(); // ';' sorts right after ':'
    Ok(db.scan(Included(&lo), Excluded(&hi))?.into_iter().collect())
}

/// A damaged SSTable must surface as `Corruption`, at open or on the read
/// that touches it. Any read that succeeds must return correct data.
fn probe_corrupt(cfg: &Config, dir: &Path, models: &[ThreadModel], report: &mut Report) -> Result<(), String> {
    let o = Options { auto_compaction: false, ..verify_options(WalRecoveryMode::TolerateTornTail) };
    let db = match Db::open(dir, o) {
        Err(Error::Corruption(_)) => {
            report.faults_detected_at_open += 1;
            return Ok(());
        }
        Err(e) => return Err(format!("damaged SSTable: open failed with a non-corruption error: {e}")),
        Ok(db) => db,
    };
    let mut detected = false;
    for (t, model) in models.iter().enumerate() {
        match thread_state(&db, t) {
            Err(Error::Corruption(_)) => detected = true,
            Err(e) => return Err(format!("damaged SSTable: scan failed with {e}")),
            Ok(state) if state != model.map => {
                return Err(format!("damaged SSTable: thread {t} scan returned WRONG DATA without an error"));
            }
            Ok(_) => {}
        }
        for k in model.map.keys().step_by(7) {
            match db.get(k) {
                Err(Error::Corruption(_)) => detected = true,
                Err(e) => return Err(format!("damaged SSTable: get failed with {e}")),
                Ok(v) if v.as_ref() != model.map.get(k) => {
                    return Err(format!("damaged SSTable: get({}) returned WRONG DATA without an error", show(k)));
                }
                Ok(_) => {}
            }
        }
    }
    let _ = cfg;
    if !detected {
        return Err("damaged SSTable: damage went undetected by open, scans and gets".into());
    }
    report.faults_detected_at_read += 1;
    Ok(())
}

fn verify_options(wal_recovery: WalRecoveryMode) -> Options {
    Options { sync: SyncPolicy::Never, wal_recovery, memtable_size: 64 * 1024, block_size: 1024, ..Options::default() }
}

fn newest_wal(dir: &Path) -> Option<PathBuf> {
    files(dir, FileKind::Wal).last().map(|n| dir.join(wal_file_name(*n)))
}

fn live_ssts(dir: &Path) -> Vec<u64> {
    let Ok(Some(n)) = read_current(dir) else { return Vec::new() };
    match read_manifest(&dir.join(minirocks::filename::manifest_file_name(n))) {
        Ok(c) => c.state.files.keys().copied().collect(),
        Err(_) => Vec::new(),
    }
}

fn files(dir: &Path, kind: FileKind) -> Vec<u64> {
    let mut v: Vec<u64> = fs::read_dir(dir)
        .map(|it| {
            it.filter_map(|e| parse_file_name(e.ok()?.file_name().to_str()?))
                .filter(|&(_, k)| k == kind)
                .map(|(n, _)| n)
                .collect()
        })
        .unwrap_or_default();
    v.sort();
    v
}

fn set_len(p: &Path, len: u64) {
    fs::OpenOptions::new().write(true).open(p).unwrap().set_len(len).unwrap();
}

fn copy_dir(from: &Path, to: &Path) {
    let _ = fs::remove_dir_all(to);
    fs::create_dir_all(to).unwrap();
    if let Ok(it) = fs::read_dir(from) {
        for e in it.flatten() {
            fs::copy(e.path(), to.join(e.file_name())).unwrap();
        }
    }
}
