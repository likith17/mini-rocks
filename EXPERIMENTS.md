# Experiments

Every crash test and measurement run so far, with its configuration and
result. All numbers are measured; none are estimated. Raw logs of the crash
campaigns are in `experiments/`.

**Machine (all runs):** Intel Core Ultra 9 185H (16 cores / 22 threads),
31.5 GB RAM, Samsung MZVL21T0HCLR NVMe SSD, NTFS, Windows 11 Home
10.0.26200, rustc 1.85.0, `--release` builds. Databases live under the
system temp directory on the same SSD.

---

## Benchmarks: MiniRocks vs sled (phase 7)

### Setup

* **Baseline: sled 0.34.7**, defaults: a 1 GiB in-memory page cache, and a
  background flush and fsync every 500 ms. **RocksDB was the first choice
  but doesn't build on this machine.** `rust-rocksdb` needs libclang (for
  `bindgen`) and a C++ toolchain, and neither is installed. See DECISIONS.md
  §11.
* **MiniRocks: defaults** (4 MiB memtable, 4 KiB blocks, 10-bit Bloom
  filters, size-tiered compaction: min 4, max 32, ratio 3). For the
  non-fsync workloads it uses `SyncPolicy::Never`. For `fillsync` it uses
  `SyncPolicy::Always`, and sled calls `flush()` after every insert.
* **Data:** 16-byte keys and 100-byte pseudo-random (incompressible) values,
  so 116 user bytes per entry. N = 1,000,000. 200,000 point reads; 20,000
  scans of 100 items; 5,000 fsynced writes.
* **Method:** each engine runs 3 times, each run in a **fresh process** on a
  fresh directory, alternating engines. Cells show the median, with the
  range across the 3 runs in brackets.
* **Bytes written** come from the OS's per-process I/O counters
  (`GetProcessIoCounters` → `WriteTransferCount`: bytes passed to write
  calls). That measures both engines the same way. For MiniRocks, the
  engine's own counters (`WriteStats`) agree to the last digit.
* The whole dataset (~116 MB) fits in RAM, so **reads never touch the
  device**: these are in-memory read numbers for both engines.
* To reproduce:
  `cargo run --release -p minirocks-bench -- all --n 1000000 --reps 3`.
  Raw output: `experiments/bench-1m.log`.

### Results (median of 3, range in brackets)

| Workload | MiniRocks | sled | MiniRocks vs sled |
|---|---|---|---|
| fillseq, ops/s | 340,568 (335,485–351,723) | 312,799 (281,536–320,604) | **1.09× faster** |
| fillrandom, ops/s | 293,626 (284,193–305,820) | 174,023 (169,938–174,061) | **1.69× faster** |
| fillsync (fsync per write), ops/s | 11,266 (11,238–12,013) | 9,464 (8,044–9,586) | **1.19× faster** |
| readrandom (63% hits), ops/s | 230,918 (225,047–238,130) | 412,704 (404,774–414,978) | **1.79× slower** |
| readmissing, ops/s | 2,134,545 (2,122,741–2,190,979) | 688,930 (687,287–702,932) | **3.10× faster** |
| seekrandom (100 items), scans/s | 23,977 (22,922–24,825) | 26,697 (26,356–27,161) | **1.11× slower** |
| scanall, items/s | 4,084,876 (4,047,111–4,312,036) | 2,868,252 (2,837,603–2,897,070) | **1.42× faster** |
| seekhot (fresh data in memtable), scans/s | 664 (646–665) | 25,381 (24,835–25,519) | **38× slower** |
| Write amp, fillseq | 4.22 | 8.76 | lower |
| Write amp, fillrandom | 3.97 | 4.02 | about equal |
| Space amp after fillrandom (before compaction) | 1.22 | 3.45 | lower |
| Space amp after full compaction | 0.96 | n/a (sled has no compaction call) | |
| Full compaction time | 0.32 s | n/a | |

Write and space amplification are deterministic for MiniRocks (identical in
all 3 runs). The definitions:
* **Write amp:** bytes written to disk ÷ (N × 116 user bytes). Measured
  after a settle (MiniRocks: flush plus pending compactions; sled:
  `flush()`).
* **Space amp:** directory size ÷ (live keys × 116 bytes). 631,869 of the 1M
  random keys are unique.
* **Space amp below 1.0** after compaction comes from prefix compression of
  the 16-digit keys.

MiniRocks internals during fillrandom: 12 compactions, 3 live tables after
settling. During readrandom, a lookup checked 2.55 tables on average, made
0.65 table probes and read 0.65 data blocks.

### Where MiniRocks is slower, and why

1. **Point reads of present keys: 1.79× slower.**
   - **What the counters show:** probes per get (0.65) equal the hit rate
     (0.63) plus Bloom false positives (0.02), so each lookup reads **at
     most one data block**. The filters are doing their job.
   - **Where the time goes:** a missing-key get (no block read) costs 1 /
     2.13M = **0.47 µs**. The readrandom average is 1 / 231k = **4.33 µs**.
     Taking out the misses' share leaves (4.33 − 0.37 × 0.47) / 0.65 ≈
     **6.4 µs per data-block read**. That breaks down as a `pread`
     syscall, a CRC32 over the ~4 KiB block, a fresh `Vec` allocation and a
     block decode.
   - **Why sled is faster here:** it answers from its own in-memory cache
     with no syscall, in about 2.4 µs.
   - **The fix** would be a block cache of decoded, already-verified blocks
     (DECISIONS.md §7, "Not done"). Phase 3 deferred it to be measured, and
     this is the measurement.
2. **Scans with fresh data in the memtable: 38× slower.**
   - **Cause:** this is the snapshot design from DECISIONS.md §7. Each
     iterator copies the active memtable's part of its range. A scan from a
     random key with no end bound copies everything from that key on, up to
     4 MiB (~25,000 entries), to return 100 items. About 1.5 ms per scan.
   - **Evidence it's the memtable:** the same scans after a flush
     (`seekrandom`) run at 24k/s.
   - **The fix:** a multi-version memtable (sequence-number snapshots), or
     freezing the memtable on iterator creation.
3. **Short scans after settling: 1.11× slower.** Each scan opens a cursor on
   every overlapping table (3 here), reads one block from each, and merges
   through a heap. Every block read pays the same costs as in item 1.

### Where MiniRocks is faster, and the caveats

* **Writes.**
  - The likely reason for the fillrandom difference (1.69×), which I have
    not profiled inside sled: sled updates its on-disk tree structure on
    every insert, while an LSM only appends to a log and a memtable.
  - **Caveat on durability:** with these settings, sled keeps acknowledged
    writes in *its own* memory for up to 500 ms, so a process crash can lose
    them. MiniRocks `SyncPolicy::Never` hands each record to the OS before
    acknowledging it, so it survives a process crash (verified by the
    phase 6 harness). Even so MiniRocks is faster, but the two configurations
    don't promise the same thing.
* **fillsync (fsync per write).** Both are bound by `FlushFileBuffers`, about
  89 µs per call on this NVMe drive. MiniRocks writes one small record per
  fsync. Neither engine batches concurrent writers (group commit), and only
  one writer thread was used.
* **Missing keys (3.10×).** The Bloom filters rule out every table without
  reading a block.
* **Full scans (1.42×).** Sequential block reads with prefix-compressed keys,
  merged over just 3 tables.
* **Space amplification (1.22 vs 3.45).** Size-tiered compaction had merged
  most overwritten versions by the time the tree settled.
* **Write amplification, fillseq (4.22 vs 8.76).** Even so, 4.22 is high
  for *sequential* data. Size-tiered compaction rewrites non-overlapping
  tables when it merges a tier, whereas leveled compaction would just *move*
  such files to the next level ("trivial move"). That would leave only the
  WAL (137 bytes on disk per 116 user bytes, 1.18×) and the flush (≈1.0×),
  about 2.2 in total. This is a cost of the strategy, noted in DECISIONS.md
  §9.

### Run history

| Log | Note |
|---|---|
| `experiments/bench-1m-run1-readrandom-all-hits.log` | First full run. **Flawed readrandom:** read keys came from `mix(i ^ 0x5A5A)` and fill keys from `mix(i ^ 0xA5A5)`. For i < 2²⁰, XOR with a constant only permutes the same inputs, so the hit rate was 1.00, not the intended 0.63. Its readrandom figures (152k vs 407k ops/s, 2.7×) measure present-key reads only. All other metrics match run 2 within noise. Not used in the table above. |
| `experiments/bench-1m.log` | Run 2: independent read stream (hit rate 0.63), plus internal counters. **Used above.** |

---

## Crash-consistency campaigns (phase 6)

Harness design and limits: DECISIONS.md §10. To reproduce:

```
cargo build --release --features failpoints --bin crashtest
target/release/crashtest --cycles 1000 --seed 1 --threads 4 --keys 3000
target/release/crashtest --cycles 600  --seed 2 --threads 1 --keys 300
```

Kill timing and the order of background work aren't deterministic, so the
same seed yields the same workload and fault plan but not the same crash
points.

### Summary

| Run | Config | Cycles | Acked ops verified | Crash points fired | Faults injected | Result |
|---|---|---|---|---|---|---|
| 0 | seed 1, 4 threads × 3000 keys | 1 | 0 | 1 | 0 | **ENGINE FAILURE #1**, fixed |
| 1a | seed 1, 4 threads × 3000 keys | ~110 | ~660,000 (to cycle 100) | 25 by cycle 100 | n/a | **HARNESS BUG #1**: driver panicked; fixed |
| 1b | seed 2, 1 thread × 300 keys | ~100 | ~2,030,000 (to cycle 100) | 24 by cycle 100 | n/a | stopped by me: same harness bug |
| 2a | seed 1, 4 threads × 3000 keys | **1000** | **6,453,526** | 268 (all 17 points) | 255 | **PASS**, 0 failures |
| 2b | seed 2, 1 thread × 300 keys | **600** | **13,189,009** | 120 (all 17 points) | 145 | **PASS**, 0 failures |

**Total since the fix: 1,600 crash cycles, 19,642,535 acknowledged
operations verified, 388 targeted crash points, 400 injected faults, 0
failures.** Runs 1a and 1b had found no engine failure up to the point
they stopped.

### Failure #1 (engine): a crash before the first `CURRENT` bricked the database

* **Found by:** run 0, cycle 0, about 0.1 s into the campaign. The child was
  killed during the database's very first `open()`.
* **Symptom:** every later open failed with `corruption: manifest files
  exist but CURRENT is missing; refusing to guess which is live`.
* **Cause:** `open()` writes and fsyncs a new `MANIFEST-N`, then switches
  `CURRENT` to it. On the first open there is no previous `CURRENT`, so a
  crash between those steps leaves a manifest with no `CURRENT`. Phase 4
  recovery treated that as corruption. The same window exists when adopting
  a directory written before the manifest existed. No data was lost, but the
  database could not be opened without manual repair.
* **Fix** (`src/db.rs`, recovery step 2): MiniRocks never deletes `CURRENT`
  and only replaces it by atomic rename, so a missing `CURRENT` alongside
  manifests can only mean that first switch was interrupted. Recovery now:
  - uses the newest manifest that reads back validly (a newer stray manifest
    is either a complete snapshot of the same state, or torn);
  - if none is valid, adopts the SSTables on disk, which is safe because no
    flush can run before `CURRENT` exists.

  `RecoveryReport::recovered_without_current` reports this case.
* **Regression test:**
  `tests/manifest.rs::crash_before_first_current_does_not_brick_the_database`
  covers both variants (complete first manifest, torn first manifest). The
  phase 4 test that had asserted the buggy behaviour ("missing CURRENT →
  Corruption") was narrowed to a bad `CURRENT` (one that names a missing
  file, or holds garbage). That is still corruption.
* **Afterwards:** in runs 2a and 2b, `manifest.create.before_current` fired
  30 times and `current.before_rename` 32 times, all recovered correctly.

### Harness bug #1: a cut-off ACK line crashed the driver

* **Found by:** run 1a, after cycle 100. The driver panicked: `called
  Option::unwrap() on a None value` while parsing child output.
* **Cause:** a kill can cut the child's last stdout line short. `lines()`
  hands back the fragment (for example `A 3`), and parsing it panicked. Worse,
  a fragment like `A 3 12` cut from `A 3 1234` would have parsed as a
  **smaller** ACK index than the real one. That would have caused a false
  failure, or, if the parse is lenient, a weaker check.
* **Fix:** only newline-terminated lines count. An operation whose ACK line
  was cut off is treated as in flight, which the verifier already allows.
  Per-thread ACK indices are also never allowed to move backwards.
* No engine change. Run 1b used the same driver code, so I stopped it and
  restarted both campaigns as runs 2a and 2b.

### Run 2a: seed 1, 4 writer threads, 3,000 keys per thread (938 s)

```
kills after N acks:            393
kills after random delay:      321 (37 before the child finished opening)
crash points fired:            268 (armed but not reached, then killed: 18)
    compaction.after_manifest              15
    compaction.before_manifest             19
    compaction.before_obsolete             14
    current.before_rename                  22
    flush.after_manifest                   21
    flush.before_manifest                  14
    flush.before_wal_delete                 7
    manifest.append.torn                   17
    manifest.create.before_current         20
    recovery.after_orphan_delete            7
    recovery.after_wal_truncate             2
    recovery.before_old_manifest_delete    17
    rotate.after_new_wal                   15
    sst.after_rename                       13
    sst.before_rename                      20
    sst.write_block                        24
    wal.append.torn                        21
faults injected:               255
    SstFlip 46, SstTruncate 39, TempSst 43, WalGarbage 46, WalTailFlip 37, WalTruncate 44
  corrupt SSTables detected at open: 40, at read: 44 (skipped, no live SSTable yet: 1)
  PointInTime recoveries after mid-log WAL damage: 3
acknowledged ops verified:     6453526
  in-flight op found applied: 686, absent: 3275
  acked ops lost ONLY to injected WAL damage (allowed): 40
key comparisons:               9715478
max live SSTables seen:        33
RESULT: PASS (1000 cycles, 0 failures)
```

### Run 2b: seed 2, 1 writer thread, 300 keys (906 s)

A small key space means heavy overwrites and deletes, so compaction works
on many shadowed values and tombstones.

```
kills after N acks:            226
kills after random delay:      184 (39 before the child finished opening)
crash points fired:            120 (armed but not reached, then killed: 70)
    compaction.after_manifest               5
    compaction.before_manifest              6
    compaction.before_obsolete              6
    current.before_rename                  10
    flush.after_manifest                   10
    flush.before_manifest                   6
    flush.before_wal_delete                 7
    manifest.append.torn                   10
    manifest.create.before_current         10
    recovery.after_orphan_delete            3
    recovery.after_wal_truncate             1
    recovery.before_old_manifest_delete    12
    rotate.after_new_wal                    3
    sst.after_rename                        4
    sst.before_rename                       6
    sst.write_block                         9
    wal.append.torn                        12
faults injected:               145
    SstFlip 21, SstTruncate 25, TempSst 28, WalGarbage 33, WalTailFlip 21, WalTruncate 17
  corrupt SSTables detected at open: 26, at read: 20 (skipped, no live SSTable yet: 0)
  PointInTime recoveries after mid-log WAL damage: 4
acknowledged ops verified:     13189009
  in-flight op found applied: 270, absent: 316
  acked ops lost ONLY to injected WAL damage (allowed): 14
key comparisons:               173401
max live SSTables seen:        9
RESULT: PASS (600 cycles, 0 failures)
```

### Reading the numbers

* **"Armed but not reached" (18 and 70):** the child was killed after 20 s
  or 150,000 ACKs without reaching the armed point. This happens most for
  recovery points (`recovery.after_wal_truncate` needs a damaged WAL to
  truncate) and high counts of `compaction.*`. Those cycles still count as
  ordinary kills and were verified.
* **"In-flight op found applied / absent":** both outcomes occur, so the
  verifier really is exercising the "plus one in flight" allowance in both
  directions.
* **"Acked ops lost ONLY to injected WAL damage" (40 and 14):** acknowledged
  writes destroyed *on purpose*, by truncating or flipping WAL bytes after
  the fact. Each loss was a clean per-thread suffix: no reordering, no
  corruption. Without injected damage, **no acknowledged operation was ever
  missing.**
* **Corrupt SSTables:** 130 of 131 probes were detected, at open (footer,
  index, filter, meta, size check) or on the read that touched the damaged
  block. One was skipped because no live SSTable existed yet. No damaged
  table ever returned wrong data.
* **Kills during recovery:** 76 time-based kills landed before the child
  finished `open()`. Five crash points also run only inside `open()`: the
  three `recovery.*` points, `manifest.create.before_current` and
  `current.before_rename`. Together they fired 104 times (68 in 2a, 36 in
  2b). That is how the campaign covered crashes during recovery.

### What these runs do not show

These runs say nothing about OS crashes or power loss. A process kill keeps
the page cache, so the runs pass whatever the fsync placement, and 70% of
cycles used `SyncPolicy::Never`. Torn writes within a single `write()`,
`ENOSPC` and I/O errors are also not tested. See DECISIONS.md §10.

---

## Earlier experiments (phases 1–5, from the test suite)

These come from `cargo test --release` on the same machine. Phase 7 will
redo the performance-related ones at benchmark scale.

| # | Phase | What | Config | Result |
|---|---|---|---|---|
| E1 | 1 | Process-kill smoke test (`tests/recovery.rs::kill_during_writes`) | 5 kills, `SyncPolicy::Always`, 4 MiB memtable | 559 acked writes verified, 0 lost |
| E2 | 1 | WAL cut at every byte (`recovers_correct_prefix_at_every_truncation_point`) | 61 ops, ~1,900 cut points | exact prefix recovered at every cut; writes after recovery durable |
| E3 | 2 | Kill test with frequent flushes | 10 kills, 2 KiB memtable | 1,761 acked writes verified, 56 SSTables at end |
| E4 | 2 | Every truncation and every byte flip of an SSTable | unit tests in `sstable.rs` | all truncations fail to open; every flip detected at open or read |
| E5 | 3 | Bloom filter on absent keys (`tests/read_path.rs`) | 20,000 shuffled keys, 109 overlapping SSTables, 10 bits/key | **0.88% false positives** (theory 0.82%); 0.95 data blocks read per absent-key get instead of 109 table reads |
| E6 | 3 | Bloom filter on present keys | same | 1.49 table probes per get (vs ~55 without filters) |
| E7 | 4 | Manifest cut at every byte of a flush edit | 3 flushes | correct state at every cut; orphan SSTable removed, WAL replayed |
| E8 | 4 | Cost of the snapshot manifest on open | ~4,000 opens in E2 | E2 went from 20 s to 52 s: **~8 ms extra per open** (file create + 2 fsyncs + rename) |
| E9 | 5 | Write amplification under random writes (`tests/compaction.rs`) | 40,000 writes over 20,000 keys, 29-byte entries, 8 KiB memtable | **6.69×** = WAL 1.72× + flush 1.17× + compaction 3.80×; 373 flushes, ~120 compactions, 5 tables at end |
| E10 | 5 | Space amplification from overwrites | 10 overwrites of 2,000 keys | **10.55×** before compaction, **1.06×** after a major compaction |
| E11 | 5 | Manifest cut at every byte of a compaction edit | 4 inputs → 1 output | correct state at every cut; orphan counts exact (1 before commit, 4 after) |
| E12 | 5 | Kill test with flushes and compaction | 10 kills, 2 KiB memtable, auto-compaction on | 1,762 acked writes verified, 1 SSTable at end |

### Mutation checks (do the tests catch real bugs?)

Each mutation was applied temporarily, the tests run, and the change
reverted.

| Phase | Mutation | Caught by |
|---|---|---|
| 1 | Don't truncate the torn WAL tail | 3 tests |
| 1 | Restart sequence numbers at 1 after reopen | 2 tests |
| 2 | Replay WAL records already in an SSTable | `stale_wal_after_flush_is_skipped_not_replayed` |
| 3 | Merge iterator prefers the *oldest* source on ties | 2 model tests |
| 5 | Drop tombstones in non-bottommost compactions | 2 tests (deleted values resurfaced) |
| 5 | Order SSTables by file number instead of data age | 2 tests |
