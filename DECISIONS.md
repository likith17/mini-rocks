# Design decisions

Each entry records what was chosen, what was rejected, and why. Sections
are added phase by phase.

---

## 1. Durability contract and fsync policy (phase 1)

**A write is acknowledged when `put`/`delete` returns `Ok(())`.**

Under every policy, the WAL record goes to the OS in a single `write` call
before the method returns, with no user-space buffer. So **in every mode,
killing the process (kill -9, panic, OOM kill) never loses an acknowledged
write**: the bytes are already in the kernel page cache, and the kernel
writes them out whether or not the process is still alive.

The policies differ only in what survives an **OS crash or power loss**:

| `SyncPolicy` | When `sync_data` runs | Survives power loss |
|---|---|---|
| `Always` (default) | before every acknowledgement | every acknowledged write\* |
| `EveryN(n)` | on every n-th write | all but the last ≤ n-1 acknowledged writes |
| `Never` | only on `Db::sync()` and clean shutdown (drop) | anything since the OS last flushed (Linux default: ~30 s of dirty pages) |

\* Only if the storage device honours flush requests. Consumer disks with a
volatile write cache and no power-loss protection can still lose data. No
software can fix that.

Platform mapping: `File::sync_data` is `fdatasync` on Linux, `fcntl(F_FULLFSYNC)`
on macOS, and `FlushFileBuffers` on Windows.

**Visibility vs. durability.** Writes go into the memtable only after the WAL
append (and the sync, if the policy calls for one) returns. Under `Always`,
readers can therefore never see a write that isn't durable. Under
`EveryN`/`Never`, a reader can see a write that a later power loss erases.

**A failed write or fsync poisons the database.** Every further write
returns `Error::Poisoned` until the database is reopened. Two reasons:

1. A partial `write` may leave half a record on disk. A later record
   appended after it would sit behind garbage, where recovery never reaches
   it, so an acknowledged write would be lost.
2. After a failed `fsync`, Linux may drop the dirty pages and clear the error
   ("fsyncgate", PostgreSQL 2018). A retried fsync can then "succeed" with
   nothing on disk. The only safe response is to stop and recover from what
   actually reached the disk.

A write that returned an error, or was in flight during a crash, is
**indeterminate**: after recovery it is either entirely present or entirely
absent, never partial (the per-record checksum guarantees that).

**New files.** A newly created WAL is `sync_all`'d, and on Unix its directory
is fsynced, before any write that depends on it is acknowledged. On Windows,
`std` can't open a directory handle, so the directory sync is a no-op. That
leaves us relying on NTFS's metadata journal for the new directory entry.
This is a known limitation.

**Rejected:** group commit (batching concurrent writers into one fsync). It
is the right answer for throughput under `Always`. It can be added later
without changing the on-disk format, and its absence will show up honestly
in the phase 7 benchmarks.

## 2. WAL record format (phase 1)

```
+----------+----------+---------+--------------------------------------+
| crc32 u32| len u32  | kind u8 | seq u64 | key_len u32 | key | value  |
+----------+----------+---------+--------------------------------------+
            \__________ covered by crc32 _____________________________/
```

Little-endian. kind: 1 = put, 2 = delete. The value length is implicit.

* **The CRC covers the length field.** LevelDB covers only type and payload.
  Here, a bit flip in `len` fails the checksum, rather than producing a
  plausible-looking length that sends the reader to the wrong offset.
* **One record per write, no 32 KiB blocks** (LevelDB/RocksDB split the log
  into blocks with fragment headers). Blocks let a reader resynchronise after
  corruption and keep records from straddling sectors. We do resync by brute
  force instead (see §3), and the WAL is small (bounded by the memtable), so
  the simpler format wins.
* **CRC32 (IEEE) via `crc32fast`**, not CRC32C. Both detect all 1–3 bit
  errors and all burst errors up to 32 bits in records this size. `crc32fast`
  is hardware-accelerated and has a single tiny transitive dependency
  (`cfg-if`). It is the only runtime dependency.
* **Sequence numbers are stored per record.** Recovery resumes at max+1, so a
  write after restart always beats replayed history. Later phases use them
  to order duplicate keys across SSTables during compaction.
* Limits: keys ≤ 64 KiB, values ≤ 32 MiB, payload ≤ 64 MiB. During recovery,
  any length above the payload cap is treated as corruption at once.

## 3. Crash recovery (phase 1)

On open, every `NNNNNN.wal` is replayed in numeric order. Each one is read
record by record until the end, or until the first record that is
**incomplete** (it runs past EOF) or **corrupt** (CRC mismatch, impossible
length, or malformed payload).

Where reading stops, the question is: *torn tail or real damage?* A crash
can only tear the **end** of the newest log. So recovery scans every byte
offset after the bad record, looking for any record that checksums correctly:

* **None found, and it's the newest WAL → torn tail.** The file is truncated
  to the last valid record, and the truncation is fsynced. Truncating
  matters: without it, new records would be appended after the garbage and
  lost on the next recovery. The `every truncation point` test fails without
  it, which I checked by disabling the truncate.
* **A valid record found later, or the damage is in an older WAL → mid-log
  corruption.** The default mode, `TolerateTornTail`, returns
  `Error::Corruption` **and modifies nothing**. Silently dropping the records
  after the damage would break "never lose acknowledged writes".
  `PointInTime` mode recovers the longest valid prefix, truncates, and
  renames any later WALs to `*.wal.discarded`. It never deletes them.

Why both modes: under `Always`, at most one record is ever unsynced, so a
legitimate crash can't produce damage followed by valid records. Under
`EveryN`/`Never`, the OS may write unsynced pages out of order during a power
loss, so valid-after-invalid *is* possible without hardware fault, and those
writes were never promised durable. Users of those policies should open
with `PointInTime` after a power loss.

**Known false positive:** if a value contains the bytes of a valid MiniRocks
WAL record (say, someone stores a WAL file as a value) and the write of that
value is torn, the scan finds the embedded "record" and strict mode refuses
to open. `PointInTime` opens it. Random garbage gives a false match with
probability about 2⁻³² per byte offset.

**Cost:** each WAL is read into memory whole. The resync scan is worst-case
quadratic in the size of the damaged tail. Both are bounded by the memtable
size, since a WAL is rotated at every flush (§6).

## 4. Memtable data structure (phase 1)

**Chosen:** `BTreeMap<Vec<u8>, Entry>` behind an `RwLock`. `Entry` holds the
sequence number and `Option<value>`, where `None` is a tombstone.

**Rejected:** a skip list (LevelDB/RocksDB's choice).

The skip list's real advantage is *concurrency*. RocksDB's version allows
lock-free reads, and concurrent inserts, while a writer is active. In
MiniRocks, writes are already serialised by the WAL append (one log, one
order), so concurrent memtable inserts would buy little. A safe Rust
concurrent skip list means either `unsafe` code or a crate like
`crossbeam-skiplist`, and neither seemed justified yet.

A `BTreeMap` gives:
* ordered iteration for range scans and for writing sorted SSTables at flush
  time (phase 2);
* better cache behaviour than a pointer-chasing skip list (B = 6 nodes hold
  up to 11 keys);
* O(log n) everything, from the standard library, already well tested.

**Cost:** a writer holds the write lock briefly for each insert, which blocks
readers for that time. Phase 7 benchmarks will show whether this matters. If
it does, the fix is an immutable memtable plus an active one (needed for
background flush anyway) or a concurrent skip list.

Only the newest version of each key is kept. There are no snapshots or MVCC,
so older versions in the memtable would never be read.

Memory accounting is approximate: key + value + 48 bytes per entry. It
exists only to decide when the memtable is "full" (phase 2).

## 5. SSTable format (phase 2)

```
[data block]* [filter block] [meta block] [index block] [footer: 64 bytes]
```

Every block is followed by a CRC32 of its contents. The full layout is in
the doc comment of `src/sstable.rs`.

* **Data blocks (~4 KiB, configurable).** Keys are prefix-compressed. Every
  16th key is a *restart point* stored in full, so a seek binary-searches the
  restart points and then scans at most 16 entries. This is LevelDB's
  scheme. It is worth having because LSM keys are usually long and share
  prefixes (`user:000123`), and fewer bytes per key means less write and
  space amplification. **Rejected:** fixed-width entries with an offset array
  (simpler, but no prefix sharing) and block compression (snappy/zstd would
  add a dependency, and phase 7 should first measure what uncompressed
  costs).
* **Each entry stores its sequence number and kind**, as a varint
  `seq << 8 | kind` (put/delete). Tombstones are real entries: a delete has
  to shadow older SSTables until compaction can prove nothing older remains.
* **Index block: one entry per data block, keyed by the block's *last* key.**
  A lookup finds the first block whose last key is ≥ the target, so one
  binary search and one block read. LevelDB stores a shortened separator
  instead of the last key to save index space. That is an optimisation I
  left out.
* **One Bloom filter for the whole file**, not one per block like LevelDB. A
  phase 3 lookup asks "could this file have the key?" once per file, which
  is exactly the question a whole-file filter answers. 10 bits/key and
  k = round(10·ln 2) = 7 give a theoretical 0.82% false-positive rate (unit
  test: < 1.5% measured). Probes use double hashing on one XXH64. XXH64 is
  implemented in-crate (60 lines, checked against reference vectors) because
  the filter is persisted and `std`'s hasher may change between Rust
  releases. One more dependency would also work, but the code is small and
  stable.
* **Meta block:** entry count, tombstone count, min/max sequence number,
  smallest and largest key. `max_seq` drives recovery (§6). The key range
  will let range scans and compaction skip files.
* **Footer:** three block handles, format version, a CRC over the footer, and
  the magic number `"MiniRock"` in the **last** 8 bytes. A truncated file has
  the wrong bytes where the magic should be, so it fails immediately. A test
  checks every possible truncation length.
* **Verification policy:** footer, index, filter and meta blocks are checked
  when the file is opened. Data blocks are checked on every read. A flipped
  byte anywhere is reported as `Corruption` and never returned as data. A
  test flips every byte of a table to check this. Data blocks are not
  pre-verified at open (RocksDB's `paranoid_checks`), because that would read
  every table in full on startup.
* Reads use positional I/O (`pread` / `seek_read`), so concurrent readers
  share one file handle without a cursor lock. There is no block cache yet,
  so every data-block read hits the OS page cache. Phase 3/7 will measure
  this.

## 6. Flush protocol and its crash points (phase 2)

When the active memtable reaches `memtable_size` (default 4 MiB), the next
writer, while holding the writer lock:

1. fsyncs the current WAL, so it is complete on disk whatever the sync
   policy;
2. opens a new WAL (new file number, directory fsynced);
3. moves the memtable into the single *immutable* slot and signals the
   flush thread.

The flush thread then:

4. writes `NNNNNN.sst.tmp`, fsyncs it, renames it to `NNNNNN.sst`, and
   fsyncs the directory;
5. publishes the new table to readers and clears the immutable slot;
6. deletes the old WAL(s).

Readers always look at memtable → immutable → SSTables (newest first). The
SSTable list is copy-on-write (`Arc<Vec<…>>`), so a reader holds the lock
only long enough to clone two `Arc`s and does its I/O without it.

**Why one immutable memtable and a write stall.** If the memtable fills again
while the previous flush is still running, writers wait. More immutable
memtables would smooth out bursts, but memory would grow without bound if
the disk can't keep up. The stall makes backpressure explicit. RocksDB's
default is 2 write buffers; this can grow later.

| Crash during | State on disk | Recovery |
|---|---|---|
| 1–3 | old WAL (synced) + maybe a new empty WAL | both WALs replayed; nothing lost |
| 4, before rename | `.sst.tmp` + both WALs | `.tmp` deleted (never referenced); WALs replayed |
| after 4, before 6 | complete `.sst` + its WAL | WAL records with `seq ≤ max_seq` of any SSTable are **skipped**; WAL deleted |
| after 6 | `.sst` + new WAL | normal |

The skip by sequence number in the third row is essential. Replaying a
stale WAL would put old values into the memtable, the *newest* layer, above
newer SSTables. `stale_wal_after_flush_is_skipped_not_replayed` checks
exactly this, and I confirmed it fails when the skip is disabled. The rule
is sound because each flush covers a contiguous run of sequence numbers, and
the memtable's highest sequence number is always present (it is the latest
write to its key).

**A flush failure poisons writes** (`Error::Poisoned`), the same as a WAL
failure. Reads keep working from the immutable memtable. Reopening retries
from the WALs.

**How live SSTables are found in phase 2: listing the directory.** This is
correct only because SSTables are never deleted yet: every `*.sst` is live,
and a higher number means newer. Compaction (phase 5) breaks this, because
inputs and outputs would both exist after a crash. The manifest (phase 4)
replaces the listing then.

**Recovery and older WALs.** An older WAL is still never allowed a torn
tail (§3). Step 1 guarantees it was synced before any newer WAL was written.

## 7. Read path (phase 3)

**Point lookups** check memtable → immutable memtable → SSTables newest to
oldest and stop at the first entry for the key. A tombstone stops the search
too: it means "deleted", not "keep looking". Before reading an SSTable, two
free in-memory checks run in order:

1. **Key range**: skip if the key is outside `[smallest_key, largest_key]`
   (from the meta block). This decides everything when data arrives in key
   order, because SSTables then don't overlap.
2. **Bloom filter**: skip if the filter says "definitely absent". This is
   what matters when SSTables overlap (random writes, and after size-tiered
   compaction).

Only then is the in-memory index binary-searched and one data block read.

Measured (`tests/read_path.rs`, 20,000 shuffled keys in 109 overlapping
SSTables, 10 bits/key):

| Workload | Without filters (would be) | Measured |
|---|---|---|
| absent key | 109 table reads | 0.95 data blocks per get (0.88% FP rate, theory 0.82%) |
| present key | ~55 table reads (half, on average) | 1.49 table probes per get |

The 0.49 extra probes per present key is exactly the false-positive rate
times the ~55 newer tables checked first. More tables means more false
positives, which is one reason compaction exists (phase 5).

**Range scans** use a k-way merge (`src/iter.rs`) over one cursor per layer,
using a min-heap keyed by `(key, source index)`, with sources ordered newest
first. For equal keys the newest source wins, and the cursors for that key
in older sources are advanced past it. Tombstones win the same way but
aren't returned. I checked that inverting the tie-break breaks the model
tests. Cost: O(log k) per step for k sources.

* **Snapshot semantics.** `Db::iter` holds `Arc`s to the immutable memtable
  and the SSTable list, so later flushes don't disturb it. The *active*
  memtable keeps only the newest version per key, so a later overwrite
  would destroy the version the iterator should see. The iterator therefore
  **copies the in-range part of the active memtable** at creation (bounded by
  `memtable_size`). **Rejected:** a multi-version memtable with
  sequence-number snapshots (RocksDB's approach). It is the right fix for
  cheap snapshots and huge memtables, but needs MVCC throughout and garbage
  collection of old versions.
* The immutable memtable never changes, so its cursor just re-seeks past the
  current key on each step (O(log n)). That avoids a self-referential borrow
  of a `BTreeMap` iterator.
* SSTables whose key range doesn't overlap the scan bounds are skipped
  without being opened. Bloom filters can't help with ranges.
* The iterator is lazy: one data block per SSTable cursor is in memory at a
  time. A read or checksum error is returned once as `Err` and the iterator
  then ends, so it can never silently skip data.
* `Db::scan` is now `iter(..).collect()`.

**Not done: a block cache.** Every data-block read is a `pread` plus a
checksum, served from the OS page cache when the data is hot. A user-space
LRU cache would save the syscall and the CRC on hot blocks. Whether that
matters is a phase 7 measurement, not a guess.

**Counters** (`Db::stats()`): gets, memtable hits, range skips, Bloom skips,
probes, false positives, scan range skips, data blocks read. These feed the
phase 7 benchmarks.

## 8. Placeholders for later phases

* Manifest format and atomic replacement (phase 4)
* Compaction strategy: size-tiered vs. leveled (phase 5)
