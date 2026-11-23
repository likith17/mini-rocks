# MiniRocks

A persistent key-value store built on a log-structured merge tree (LSM tree),
written from scratch in Rust. Its one non-negotiable goal is that **an
acknowledged write is never lost, whenever the process dies**.

* Write-ahead log with per-record checksums and a configurable fsync policy
* Memtable flushed in the background to immutable, checksummed SSTables with
  Bloom filters
* Size-tiered background compaction
* A manifest (with an atomically replaced `CURRENT`), so a restart rebuilds
  exactly the committed state
* Crash recovery that discards torn writes and refuses to guess about real
  corruption
* **A crash-consistency harness**: 1,600 kill-and-recover cycles, 19.6M
  acknowledged operations verified, 400 injected faults, 0 failures after
  fixing the one engine bug it found (see [Crash testing](#crash-testing))
* Measured benchmarks against [sled](https://github.com/spacejam/sled),
  including where MiniRocks is slower and why

The only runtime dependency is `crc32fast`. Design rationale is in
[DECISIONS.md](DECISIONS.md); every crash test and benchmark run, with
configuration and results, is in [EXPERIMENTS.md](EXPERIMENTS.md).

```rust
use std::ops::Bound::{Included, Unbounded};
use minirocks::{Db, Options, SyncPolicy};

let db = Db::open("my-db", Options { sync: SyncPolicy::Always, ..Options::default() })?;
db.put(b"apple", b"red")?;
db.put(b"cherry", b"dark red")?;
db.delete(b"apple")?;
assert_eq!(db.get(b"cherry")?, Some(b"dark red".to_vec()));

for item in db.iter(Included(b"b".as_slice()), Unbounded)? {
    let (key, value) = item?;
    // keys >= "b", ascending
}
```

(The same example runs as a doctest in `src/lib.rs`.)

---

## How an LSM tree works, in plain language

Imagine keeping a big, sorted address book on disk. Updating a sorted file in
place is slow: inserting "Baker" means shifting everything after it. An LSM
tree avoids ever editing a file in place:

1. **Write it down first.** Every change is appended to the end of a log
   file, the *write-ahead log* (WAL). Appending is the cheapest thing a disk
   can do. If the power fails, the log still has the change.
2. **Keep recent changes in memory, sorted.** The same change goes into an
   in-memory sorted table, the *memtable*. Reads check it first.
3. **When memory fills up, write it out once, sorted.** The memtable is
   written to disk as a new, immutable sorted file, an *SSTable* (Sorted
   String Table). The log for those changes can then be thrown away.
4. **Reads look newest-first.** A lookup checks the memtable, then SSTables
   from newest to oldest, and stops at the first hit. A *delete* is recorded
   as a "tombstone" that hides older values. Each SSTable carries a *Bloom
   filter*, a small bit array that can say "this key is definitely not in
   this file", so most files are skipped without being read.
5. **Merge files in the background.** Over time there are many SSTables, and
   old versions of keys pile up. *Compaction* merges several files into one,
   keeping only the newest version of each key and dropping deleted ones.
6. **Keep a ledger of which files count.** Files are created and deleted all
   the time, so a small *manifest* log records which SSTables are live. After
   a crash, the manifest (not whatever happens to be in the directory)
   defines the database.

The trade: writes are fast (append plus an in-memory insert), and reads do a
bit more work (several places to look). The engine pays for both in
background merging: each byte is rewritten a few times (*write
amplification*), and old versions take disk space until they're merged away
(*space amplification*).

---

## Architecture

```
          put / delete                                       get / iter
               │                                                  │
               ▼                                                  ▼
 ┌───────────────────────────────┐   ┌────────────────────────────────────────────┐
 │ 1. WAL append  NNNNNN.wal     │   │ active memtable → immutable memtable       │
 │    one write() per record,    │   │ → SSTables, newest data first:             │
 │    fsync per SyncPolicy       │   │    skip if key outside [smallest, largest] │
 └──────────────┬────────────────┘   │    skip if Bloom filter says "absent"      │
                ▼                    │    else: index lookup + read one block     │
 ┌───────────────────────────────┐   │ iter(): k-way heap merge over all layers   │
 │ 2. active memtable (BTreeMap) │   └────────────────────────────────────────────┘
 └──────────────┬────────────────┘
                │ full (memtable_size, default 4 MiB):
                │ fsync old WAL, start new WAL, freeze memtable
                ▼
 ┌───────────────────────────────┐   ┌────────────────────────────────────────────┐
 │ immutable memtable (≤ 1;      │──▶│ FLUSH THREAD                               │
 │ writers stall if busy)        │   │ write .sst.tmp → fsync → rename → fsync dir│
 └───────────────────────────────┘   │ → manifest edit {add sst, log_number} ◀── commit point
                                     │ → install for readers → delete old WAL     │
                                     └─────────────────────┬──────────────────────┘
                                                           ▼
 ┌──────────────────────────────────────────────────────────────────────────────────┐
 │ SSTables  NNNNNN.sst   (ordered by data age; sequence ranges never interleave)   │
 │   newest ─────────────────────────────────────────────────────────▶ oldest       │
 │   [1×][1×][1×]     [4×][4×][4×]         [16×][16×]        [64×]   (× memtable)  │
 │   └─ tier 0 ─┘     └─ tier 1 ─┘         └ tier 2 ┘        tier 3                 │
 └───────────────────────────────────┬──────────────────────────────────────────────┘
                                     │ ≥ 4 age-adjacent tables of similar size
                                     ▼
                     ┌──────────────────────────────────────────┐
                     │ COMPACTION THREAD                        │
                     │ merge → .sst.tmp → fsync → rename        │
                     │ → manifest edit {add out, delete inputs} ◀── commit point
                     │ → swap for readers; inputs deleted when  │
                     │   their last reader/iterator drops       │
                     └──────────────────────────────────────────┘

 MANIFEST-NNNNNN : append-only log of version edits (live SSTables, log number,
                   last sequence); rewritten as one snapshot edit on every open
 CURRENT         : names the live manifest; replaced only by atomic rename
```

| Module | Role |
|---|---|
| `src/wal.rs` | WAL record format, writer (with sync policy and poisoning), reader |
| `src/memtable.rs` | `BTreeMap` memtable with sequence numbers and tombstones |
| `src/block.rs`, `src/sstable.rs` | Prefix-compressed blocks; SSTable writer, reader, iterator |
| `src/bloom.rs`, `src/hash.rs` | Bloom filter; XXH64 (stable across builds, since filters persist) |
| `src/iter.rs` | k-way merge iterator (`MergeIter` for compaction, `DbIter` for users) |
| `src/manifest.rs` | Version edits, manifest log, `CURRENT` |
| `src/compaction.rs` | Size-tiered picking policy |
| `src/db.rs` | Recovery, write path, flush and compaction threads, read path |
| `src/crashpoint.rs` | Named crash points (feature `failpoints`; compiled out otherwise) |
| `src/bin/crashtest.rs` | Crash-consistency harness |
| `bench/` | Benchmarks (separate crate; sled is only a dependency there) |

---

## Durability guarantees

A write is **acknowledged** when `put`/`delete` returns `Ok(())`.

| `SyncPolicy` | What happens before `Ok` | Process crash (kill -9, panic) | OS crash / power loss |
|---|---|---|---|
| `Always` (default) | record written **and `fsync`ed** (`sync_data`) | survives | **survives**, if the disk honours flush requests |
| `EveryN(n)` | written; fsync every n-th write | survives | the last ≤ n−1 acknowledged writes may be lost |
| `Never` | written to the OS (no fsync; synced on `Db::sync()` and clean close) | survives | anything since the OS last wrote back may be lost |

Precisely:

* **Every policy survives a process crash.** Each record reaches the OS in a
  single `write()` before `Ok` is returned, and nothing is buffered in user
  space. The crash harness checks this.
* **A write that returned an error, or was in flight during a crash, is all
  or nothing.** After recovery it is either fully present or fully absent,
  never partial (per-record CRC).
* **Under `Always`, readers never see a write that isn't durable:** the
  memtable is updated only after the fsync.
* **A failed write or fsync poisons the database:** all further writes return
  `Error::Poisoned` until it is reopened. After a failed fsync, retrying
  could "succeed" with nothing on disk.
* **Recovery discards only a torn tail.** If an invalid WAL record is
  followed by a valid one, the damage isn't a crash artefact. `open` returns
  `Error::Corruption` and modifies nothing, unless you opt into
  `WalRecoveryMode::PointInTime`.
* **New files and renames** are followed by a directory fsync on Unix. On
  Windows, `std` can't fsync a directory, so MiniRocks relies on NTFS's
  metadata journal (see [Known limitations](#known-limitations)).

Full reasoning: [DECISIONS.md §1](DECISIONS.md#1-durability-contract-and-fsync-policy-phase-1).

---

## File formats

All integers are little-endian. A directory contains `NNNNNN.wal`,
`NNNNNN.sst`, `MANIFEST-NNNNNN` and `CURRENT`. WALs, SSTables and manifests
share one increasing number space.

**WAL record** ([DECISIONS.md §2](DECISIONS.md#2-wal-record-format-phase-1))

```
crc32 u32 | len u32 | kind u8 | seq u64 | key_len u32 | key | value
            └──────────── crc32 covers len..end ───────────────────┘
kind: 1 = put, 2 = delete (no value). Value length = len − 12 − key_len.
```

**SSTable** ([DECISIONS.md §5](DECISIONS.md#5-sstable-format-phase-2))

```
[data block]* [filter block] [meta block] [index block] [footer, 64 bytes]
every block is followed by crc32(block) u32

data block entry: shared varint | non_shared varint | value_len varint
                  | key suffix | tag varint (seq << 8 | kind) | value
                  …then restart offsets u32[] and their count u32
                  (full key every 16 entries; prefix-compressed otherwise)
filter block:     Bloom bit array | k u8   (one filter per file; XXH64,
                  double hashing; 10 bits/key ≈ 0.8% false positives)
meta block:       varints num_entries, num_tombstones, min_seq, max_seq,
                  then length-prefixed smallest_key, largest_key
index block:      one entry per data block: last key → varint offset, size
footer:           filter, index, meta handles (offset u64, size u64) ×3
                  | version u32 | crc32 of the above u32 | magic "MiniRock" u64
```

**Manifest** ([DECISIONS.md §8](DECISIONS.md#8-manifest-phase-4)): a log of
records framed as `crc32 | len | payload`. The payload is a list of
`tag varint, fields`:

| tag | meaning |
|---|---|
| 1 | log number: WALs below it are obsolete |
| 2 | next file number |
| 3 | add SSTable (number, file size) |
| 4 | delete SSTable (number) |
| 5 | last sequence number flushed |

`CURRENT` holds one line, `MANIFEST-NNNNNN`.

---

## How to run

Requires stable Rust (developed with 1.85).

```powershell
cargo test --release
```

This runs 83 tests: unit, integration and doctest. They include WAL and
manifest truncation at every byte offset, every-byte corruption of
SSTables, model-checked random workloads with flushes, compactions and
reopens, and process-kill tests.

**Crash-consistency harness** (needs the `failpoints` feature):

```powershell
cargo build --release --features failpoints --bin crashtest
```
```powershell
.\target\release\crashtest.exe --cycles 1000 --seed 1 --threads 4 --keys 3000
```

**Benchmarks** (MiniRocks vs sled, each engine in its own process, median
of `--reps`):

```powershell
cargo run --release -p minirocks-bench -- all --n 1000000 --reps 3
```

---

## Crash testing

`crashtest` runs a child process with several writer threads. Each thread
prints an ACK after every acknowledged operation, and the driver kills the
child in one of three ways:

* after a random number of ACKs;
* after a random delay, sometimes during recovery itself;
* at one of 17 named **crash points** inside the engine: torn WAL and
  manifest writes, every step of a flush and of a compaction, the `CURRENT`
  swap, and recovery steps.

Operations are a deterministic function of (seed, thread, index), so the
driver keeps an exact model. Every recovery must produce precisely "all
acknowledged writes, plus at most the one in flight per thread". Between
cycles the driver also injects faults into the files:
* WAL tail truncation, bit flips and garbage;
* partial `.sst.tmp` files;
* truncated or bit-flipped SSTables, which must be detected and must never
  return wrong data.

| Run | Cycles | Acked ops verified | Crash points fired | Faults injected | Result |
|---|---|---|---|---|---|
| seed 1, 4 threads | 1,000 | 6,453,526 | 268 | 255 | **pass** |
| seed 2, 1 thread, small key space | 600 | 13,189,009 | 120 | 145 | **pass** |

**Bugs found and fixed:**

1. **Engine:** a crash between writing the first manifest and creating
   `CURRENT` made the database impossible to open. Found on cycle 0. Fixed
   in recovery, with a regression test.
2. **Harness:** a kill could cut the last ACK line short, which crashed the
   driver and could have under-reported an acknowledgement.

Details in [EXPERIMENTS.md](EXPERIMENTS.md#crash-consistency-campaigns-phase-6).

What this does **not** cover: OS crashes and power loss. A killed process
keeps its data in the OS page cache, so this harness can't tell whether
fsyncs are in the right places. See [Known limitations](#known-limitations).

---

## Benchmark results

1M keys (16-byte keys, 100-byte random values), each engine run 3 times in
a fresh process, median shown. Machine: Intel Core Ultra 9 185H, 31.5 GB RAM,
Samsung NVMe SSD, NTFS, Windows 11. The dataset fits in RAM, so reads are
CPU- and syscall-bound for both engines. MiniRocks uses default settings
with `SyncPolicy::Never` (except fillsync); sled uses its defaults.

| Workload | MiniRocks | sled | MiniRocks vs sled |
|---|---|---|---|
| fillseq, ops/s | 340,568 | 312,799 | 1.09× faster |
| fillrandom, ops/s | 293,626 | 174,023 | 1.69× faster |
| fillsync (fsync per write), ops/s | 11,266 | 9,464 | 1.19× faster |
| readrandom (63% hits), ops/s | 230,918 | 412,704 | **1.79× slower** |
| readmissing, ops/s | 2,134,545 | 688,930 | 3.10× faster |
| seekrandom (100 items), scans/s | 23,977 | 26,697 | **1.11× slower** |
| scanall, items/s | 4,084,876 | 2,868,252 | 1.42× faster |
| seekhot (fresh data in memtable), scans/s | 664 | 25,381 | **38× slower** |
| write amplification, fillseq / fillrandom | 4.22 / 3.97 | 8.76 / 4.02 | |
| space amplification before / after full compaction | 1.22 / 0.96 | 3.45 / n/a | |

### Comparison: where MiniRocks loses, and why

* **Point reads of existing keys: 1.79× slower.**
  - **What the counters show:** each lookup reads at most one data block
    (0.65 per get, which is the 0.63 hit rate plus 0.02 Bloom false
    positives), so the filters work.
  - **Where the time goes:** a block read costs about 6.4 µs (a `pread`
    syscall, a CRC over ~4 KiB, an allocation and a decode). sled answers
    from its 1 GiB in-memory cache in about 2.4 µs.
  - **The fix** is a block cache, which MiniRocks doesn't have.
* **Scans while fresh data sits in the memtable: 38× slower.** For snapshot
  consistency, an iterator copies the active memtable's part of its range.
  A scan from a random key with no end bound copies up to 4 MiB to return
  100 items. After a flush, the same scans run at 24k/s.
* **Short scans: 1.11× slower.** Each scan opens a cursor on every
  overlapping table and reads a block from each.

### Where MiniRocks wins

* **Writes.** The LSM write path is an append plus an in-memory insert.
  **Caveat:** sled keeps acknowledged writes in its own memory for up to
  500 ms, so a process crash can lose them. MiniRocks never loses
  acknowledged writes on a process crash, even with `Never`.
* **Missing keys.** Bloom filters rule out every table without a block read.
* **Space amplification.** Compaction had merged overwritten versions.

### Not compared: RocksDB

RocksDB is the natural baseline, but `rust-rocksdb` needs libclang and a C++
toolchain that this machine doesn't have. Adding it is one more `Engine`
implementation in `bench/src/main.rs` once LLVM is installed.

Full analysis and run history:
[EXPERIMENTS.md](EXPERIMENTS.md#benchmarks-minirocks-vs-sled-phase-7).

---

## Known limitations

**Durability and safety**
* **Power-loss behaviour is argued, not tested.** The crash harness kills
  processes, which keeps the page cache. Proving fsync placement needs a
  block-device fault injector or VM snapshots.
* **No directory fsync on Windows.** File creation and renames rely on NTFS
  metadata journaling. On Unix the directory is fsynced.
* **Only tested on Windows 11 / NTFS.** The Unix code paths
  (`read_exact_at`, directory fsync) compile in but haven't been run.
* **No lock file.** Opening the same directory from two processes at once
  will corrupt it.
* **The error and poisoning paths aren't exercised.** That covers disk-full,
  I/O errors and failed fsyncs.
* **Durability depends on the disk honouring flushes.** Consumer SSDs with
  volatile caches and no power-loss protection can drop acknowledged writes.
* **One false positive in strict WAL recovery.** If a stored value contains
  the bytes of a valid WAL record and that write is torn, the default mode
  refuses to open. `PointInTime` opens it.

**Performance**
* **No block cache.** Every data-block read is a syscall plus a checksum
  (the main cause of the 1.79× point-read gap).
* **Iterators copy the active memtable's range** (the 38× `seekhot` gap). A
  multi-version memtable would fix this.
* **No group commit.** Under `Always`, every write pays its own fsync
  (~89 µs here), even with concurrent writers.
* **Writers are serialised by one mutex** around the WAL and memtable.
* **No compression.**
* **Opening costs about 8 ms extra on Windows**, for the manifest snapshot
  rewrite (2 fsyncs and a rename).

**Compaction**
* **Size-tiered only.**
  - Transient space amplification is high, and a merge needs free space
    equal to its inputs.
  - Sequential loads are rewritten instead of trivially moved (write amp
    4.22 for `fillseq`; leveled compaction would give about 2.2).
* **No write stall when compaction falls behind**, and no rate limiting.
* **A running compaction can't be cancelled**, so closing the database waits
  for it.

**Features and limits**
* **No write batches, transactions, snapshots API, TTL or column families.**
* **Keys ≤ 64 KiB, values ≤ 32 MiB.**
* **Recovery reads each WAL into memory whole** (bounded by the memtable
  size). Resynchronising after mid-log damage is worst-case quadratic in the
  damaged region.
* **The manifest is compacted only on open.** A flush adds about 10 bytes.
* **Benchmarks:** sled only; the dataset fits in RAM; single machine.

---

## Repository layout

```
src/            engine (lib) + src/bin/crashtest.rs (harness)
tests/          integration tests per phase: recovery, flush, read_path,
                manifest, compaction
bench/          benchmark crate (MiniRocks vs sled)
experiments/    raw logs of crash campaigns and benchmark runs
DECISIONS.md    design decisions, alternatives, trade-offs
EXPERIMENTS.md  every crash-test and benchmark run, with config and results
```
