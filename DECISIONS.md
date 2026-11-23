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

**How live SSTables were found in phase 2: listing the directory.** This
was correct only because SSTables were never deleted: every `*.sst` was
live, and a higher number meant newer. Compaction breaks both assumptions.
Since phase 4 the manifest decides (§8).

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

## 8. Manifest (phase 4)

The manifest replaces "every `*.sst` in the directory is live" (§6) with an
explicit record, which compaction needs. After a compaction crash, both
the inputs and the output exist on disk, and only a commit record can say
which set is live.

**Format** (`src/manifest.rs`). `MANIFEST-NNNNNN` is an append-only log of
*version edits*. Each edit is framed as `crc32 | len | payload`, with the
CRC covering the length, as in the WAL. Payload tags:

* `log_number`: WALs below this are obsolete;
* `next_file_number`;
* `add(number, file_size)`;
* `delete(number)`.

An unknown tag is corruption, so old code never misreads a newer format.
`CURRENT` holds the active manifest's name and is replaced atomically
(`CURRENT.tmp`, fsync, rename, fsync the directory).

**Rejected:** rewriting a whole "list of live files" atomically on every
change. A flush would then cost a full file write and a rename instead of
one small append and one fsync, and the manifest grows with every compaction
too. The log-plus-snapshot design is LevelDB's, for the same reasons.

**Commit protocol for a flush** (extends §6):

1. Write the SSTable to `.tmp`, fsync it, rename it, fsync the directory.
2. Append `{add(sst, size), log_number = new WAL}` to the manifest and
   fsync. **This is the commit point.**
3. Install the table for readers.
4. Delete the old WAL(s).

| Crash between | On restart |
|---|---|
| 1 and 2 | The SSTable isn't in the manifest, so it is an **orphan** and deleted. Its WAL is ≥ log_number and is replayed. |
| 2 and 4 | The WAL is below log_number, so it is deleted unread. |
| inside 2 (torn edit) | The torn tail is ignored, so this is the same as the first row. |

A test cuts the manifest at **every byte** of the final flush edit and
checks each result: the orphan is removed, the WAL's 50 records are replayed,
and the contents are exact.

**Recovery is manifest-driven:**

* ~~No `CURRENT` but `MANIFEST-*` files exist → corruption.~~ **Changed in
  phase 6** after the crash harness showed it bricks a database whose first
  open is killed. See §10.
* `CURRENT` names a missing file, or holds garbage → corruption.
* A torn final edit → ignored (it was never committed). An invalid edit
  followed by a valid one → corruption. This is the same resync rule as the
  WAL (§3).
* A listed SSTable that is missing, or whose size differs from what the
  manifest recorded → corruption. It was fsynced before being committed, so
  this can't be a crash artefact.
* An SSTable on disk but not listed → orphan, deleted. Deleting is safe
  because nothing committed ever referred to it.
* No `CURRENT` and no manifests → a new database, or one written by phase
  2/3 code. Every SSTable present is adopted. This is correct for those
  versions because they never deleted an SSTable.

**Snapshot on every open.** `open` writes a new manifest holding one
snapshot edit of the recovered state, switches `CURRENT` to it, and deletes
the old manifest. A crash before the switch leaves the old `CURRENT` valid.
This bounds the manifest's size by the number of live tables, not the
history, and discards any torn tail. **Cost, measured:** about 8 ms more per
`open` on this Windows machine (a file create, two fsyncs, a rename). The
phase 1 truncation test, which opens the database about 4,000 times, went
from 20 s to 52 s. The manifest isn't rotated at runtime yet: a flush edit
is about 10 bytes, so even 100,000 flushes between restarts produce only
about 1 MB.

**SSTable order is now by data age (`max_seq`), not file number.** A size-tiered
compaction output gets a new, high file number but holds older data than
tables flushed while it ran. Ordering by number would let stale data shadow
new data. Ordering by `max_seq` is correct as long as compaction merges only
runs of tables adjacent in age, so sequence ranges never interleave. Phase 5
must guarantee that.

**The sequence-number skip from §6 stays** as a second line of defence. With
log numbers it shouldn't trigger (`records_already_flushed` is now 0 in the
stale-WAL test), but it costs nothing and is sound.

## 9. Compaction (phase 5)

### Strategy: size-tiered, and why not leveled (yet)

Every flush adds a table, and every table a point lookup might have to check
or a scan has to merge. Compaction merges tables to bound that count, drop
overwritten values, and drop tombstones. The two classic strategies trade
the three amplifications differently:

| | Size-tiered (chosen) | Leveled (LevelDB/RocksDB default) |
|---|---|---|
| How | When ~4 similar-sized tables exist, merge them into one ~4× larger | Levels L1..Ln, each 10× the last, each split into non-overlapping key ranges. An overflowing level pushes one file's key range into the next level's overlapping files. |
| **Write amp** | **Low**: each byte is rewritten about once per tier, ≈ log₄(N/memtable) times | **High**: each byte is rewritten about 10× per level (merged with ~10 overlapping files), ≈ 10 × levels |
| **Read amp** (point) | Higher: tables in a tier overlap, so several per tier may be checked (Bloom filters absorb most of it) | Low: at most one table per level can hold a key |
| **Space amp** | **High**: a key overwritten in every tier exists that many times, and a merge temporarily needs input + output space (up to 2× the largest tier) | Low: ~1.1× at steady state |
| Scans | Merge every overlapping table | Merge one table per level |

Size-tiered fits this project for three reasons:

1. It is the write-optimised end of the trade-off, which is what an LSM is
   for.
2. It is far simpler. One output file and no per-level key-range bookkeeping
   mean fewer places for a crash-consistency bug, which matters because the
   crash harness is the centrepiece.
3. Its weakness, space amplification, is exactly what the phase 7
   benchmarks measure. Leveled compaction would be the next step if reads or
   disk space dominated.

### Policy (`src/compaction.rs`)

* Tables are ordered by age (newest first). A **tier** is a maximal run of
  age-adjacent tables whose largest is ≤ `compaction_size_ratio` (3.0) × the
  smallest. Tables smaller than `memtable_size` count as `memtable_size`, so
  fresh flushes group together despite small size differences.
* A tier with ≥ `compaction_min_merge` (4) tables is merged, at most
  `compaction_max_merge` (32) at a time. If several tiers qualify, the one
  with the **fewest total bytes** goes first: it is the cheapest and cuts the
  table count fastest.
* **Only age-adjacent runs are merged.** That keeps the sequence ranges of
  all tables disjoint, which is what lets the read path order tables by
  `max_seq` (§8). A mutation test that orders by file number instead fails
  the model tests.
* **Tombstones are dropped only by a *bottommost* compaction**, one whose
  run includes the oldest table. Otherwise an older table might still hold
  the value the tombstone deletes, and dropping it would bring that value
  back. A mutation test that drops tombstones too eagerly makes deleted
  values resurface. Overwritten versions are always dropped, since there are
  no snapshots or MVCC (§4).
* `Db::compact()` is a manual major compaction: flush, merge everything, drop
  all tombstones. `Db::run_pending_compactions()` runs the background policy
  on the caller's thread, so tests and benchmarks can wait for the tree to
  settle.

### Running in the background

A dedicated compaction thread, separate from the flush thread, so a long
merge never delays a flush or stalls writers. Each successful flush (and
each open) asks it to check. It merges until no tier is ready.
`compaction_lock` is held from choosing the inputs to installing the output,
so two compactions (background and manual) never pick the same tables. A
flush can still add a newer table meanwhile: the installer filters out the
inputs, adds the output, and re-sorts, and contiguity holds because the new
table is newer than every input.

On drop, a compaction already running finishes; none start after that. This
can make closing the database slower. It is not cancellable yet.

### Commit protocol (same shape as a flush, §8)

1. Merge the inputs into `.sst.tmp`, fsync, rename, fsync the directory.
2. Append `{add(output), delete(inputs…)}` to the manifest and fsync. **This is
   the commit point.**
3. Swap the tables for readers.
4. Mark the inputs obsolete. Each input's file is deleted when its last
   `Arc<SstReader>` drops, so an open iterator keeps reading its snapshot
   and the file disappears when the iterator does (tested). A crash before
   the deletion leaves the inputs as orphans, which recovery removes.

| Crash between | On restart |
|---|---|
| 1 and 2 | The output is an orphan and deleted; the inputs are still live. |
| 2 and 4 | The inputs are orphans and deleted; the output is live. |
| inside 2 (torn edit) | Same as the first row. |

A test cuts the manifest at **every byte** of the compaction edit with all
five files present and checks the exact contents and orphan counts for
each cut.

**A new manifest field, `last_sequence`.** A major compaction can drop the
newest entries (tombstones), so "max seq over all tables" can go backwards.
The manifest now records the highest sequence number ever flushed, so a
write after restart always gets a higher number. This has its own test.

### Measured so far

Measurements from the tests; phase 7 will do this properly at scale:

* 40,000 random writes over 20,000 keys (29-byte entries, 8 KiB memtable):
  373 flushes and about 120 compactions merging about 485 tables, leaving 5
  tables. **Write amplification 6.69**: WAL 1.72× (the 21-byte record header
  is large next to a 29-byte entry), flushes 1.17×, compaction 3.80×. The
  compaction count varies slightly between runs, because background timing
  decides which tables exist at each pick.
* Ten overwrites of the same 2,000 keys: **space amplification 10.55 before
  compaction and 1.06 after** a major compaction.

### Not done

* **No write stall on table count.** If compaction can't keep up, tables
  pile up and reads slow down, but writes never block for it. (RocksDB slows
  writes at ~20 L0 files and stops them at ~36.)
* One output file per compaction: a full merge of a large tier produces one
  large file, and needs that much free space temporarily.
* No compaction rate limiting, so it competes freely with foreground I/O.

## 10. Crash-consistency harness (phase 6)

`src/bin/crashtest.rs`, run with
`cargo run --release --features failpoints --bin crashtest -- --cycles N --seed S`.
Results are in EXPERIMENTS.md.

### What it checks

The property under test is the durability contract from §1. After any
crash, recovery must produce exactly the state after **all acknowledged
writes**, optionally plus the **one write per thread that was in flight**.
Nothing older may be lost, nothing un-written may appear, and no value may
be corrupted. Reads must not fail. The one exception is a test that damages
an SSTable on purpose; there, every read must either return correct data or
fail with `Corruption`.

### Design choices

* **Real processes, real kills.** The writer is a child process, killed with
  `TerminateProcess` (Windows) or `SIGKILL`. In-process simulation would
  miss what matters, such as buffered data in user space or background
  threads dying mid-operation.
* **Acknowledgement over a pipe.** Each writer thread prints `A <thread>
  <index>` after a call returns `Ok`. Lines already in the pipe when the
  child dies were acknowledged before it died, so the driver reads them all.
* **A deterministic workload, so the model needs no communication.**
  Operation *i* of thread *t* is a pure function of (seed, t, i): 80% puts
  and 20% deletes over a hot/cold key distribution, with values of 5–170
  bytes tagged with `t:i`. The driver replays the same operations into an
  in-memory model. Threads use disjoint key prefixes, so each thread's state
  is checked on its own, which keeps the "plus one in flight" rule exact
  under concurrency.
* **Three kill methods per cycle:**
  1. after a random number of ACKs (up to 20,000);
  2. after a random delay, half of them under 60 ms, so they land in process
     start-up or inside `open()`, that is, *during recovery*;
  3. by arming a **named crash point** in the engine (`src/crashpoint.rs`,
     compiled in only with the `failpoints` feature).

  Random kills alone almost never land in a window a few microseconds wide,
  such as between an SSTable rename and its manifest commit. Crash points
  hit each window on purpose. A point exits via `process::exit`: no
  destructors run and nothing is synced, so for file state it is the same as
  `kill -9`. Points cover:
  - torn WAL and manifest writes (half the record written);
  - WAL rotation;
  - every step of a flush and of a compaction;
  - `CURRENT` replacement;
  - three steps of recovery itself.
* **Verify on a copy; leave the original crashed.** The driver copies the
  directory and recovers the copy. The original stays exactly as the crash
  left it, so the *next* child must recover it, and may be killed while
  doing so. That gives crash-during-recovery coverage for free.
* **Fault injection between cycles (25% of cycles):**
  - Destroying recent WAL bytes. **WAL tail truncation** (1–120 bytes) and a
    **bit flip in the last 64 bytes of the WAL** lose acknowledged writes on
    purpose. For these the driver accepts the state after any per-thread
    *prefix* that ends at most ⌈bytes/21⌉+2 operations early. That checks
    that damage loses only a suffix and never corrupts or reorders. A flip
    the default recovery mode refuses (a damaged record followed by a valid
    one) is recovered with `PointInTime`, as an operator would.
  - Harmless damage. **Garbage appended** to the WAL tail and a **partial
    `.sst.tmp`** must change nothing.
  - Damaged SSTables. **SSTable truncation** and an **SSTable bit flip** at
    a random byte are applied to a probe copy. They must be detected (at
    open or on the read that touches them), and no read may return wrong
    data. "Undetected" counts as a failure.
* **Every cycle also checks point reads.** Point reads must agree with scans,
  since Bloom filters, key-range checks and the merge iterator are separate
  code paths.
* **Sync policy and memtable size vary per cycle:** `Never` 70% of the time
  (fast, and process crashes don't depend on fsync), `Always` 20%,
  `EveryN(8)` 10%, with memtables of 4 KiB, 16 KiB or 64 KiB. Small
  memtables mean frequent flushes and compactions.

### What it found

One engine bug: a crash between writing the first manifest and creating
`CURRENT` left the database unopenable. That is now fixed and §8's rule for a
missing `CURRENT` has changed. It also found one harness bug (a cut-off ACK
line). Details in EXPERIMENTS.md.

**Changed rule, superseding §8:** a missing `CURRENT` alongside manifest
files is recovered, not refused. MiniRocks never deletes `CURRENT`, so the
only way to get there is a crash during its first creation. Recovery uses the
newest manifest that reads back validly. If none is valid (the first one was
torn), it adopts the SSTables on disk, which is safe because no flush runs
before `CURRENT` exists. A `CURRENT` that names a missing file, or holds
garbage, is still corruption.

### What it cannot check

* **Power loss and OS crashes.** A process kill leaves the OS page cache
  intact, so this harness cannot tell whether fsyncs are in the right places.
  That would need a filesystem-level fault injector (dm-flakey or
  CrashMonkey-style block-device recording) or VM snapshots. The fsync
  placement is argued in §1, §6, §8 and §9, and is not yet tested.
* Torn writes *within* a single `write()` call, as opposed to torn records,
  which are injected. The OS doesn't produce these on a process kill.
* Disk-full and I/O errors. The poisoning logic (§1) is not exercised.

## 11. Placeholders for later phases

* Benchmarks and comparison with RocksDB (phase 7)
