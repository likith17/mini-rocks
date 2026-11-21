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
size once flushing lands.

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

## 5. Placeholders for later phases

* SSTable format (phase 2)
* Bloom filter parameters (phase 3)
* Manifest format and atomic replacement (phase 4)
* Compaction strategy: size-tiered vs. leveled (phase 5)
