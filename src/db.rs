use std::fs;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::thread::JoinHandle;

use crate::error::{Error, Result};
use crate::filename::{self, FileKind};
use crate::fsutil;
use crate::iter::{DbIter, Source};
use crate::manifest::{self, ManifestState, ManifestWriter, VersionEdit};
use crate::memtable::{is_empty_range, Lookup, MemTable};
use crate::options::{Options, SyncPolicy, WalRecoveryMode};
use crate::sstable::{self, SstIter, SstReader};
use crate::wal::{self, Record, RecordRef, WalWriter};

/// Largest accepted key, in bytes.
pub const MAX_KEY_LEN: usize = 64 * 1024;
/// Largest accepted value, in bytes.
pub const MAX_VALUE_LEN: usize = 32 * 1024 * 1024;

/// What recovery did when the database was opened.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RecoveryReport {
    /// Edits replayed from the manifest named by `CURRENT`.
    pub manifest_edits_replayed: usize,
    /// Bytes of a torn final manifest edit that were ignored (an edit that
    /// was never committed, because a crash interrupted its write).
    pub manifest_torn_bytes: u64,
    /// Live SSTables opened, as listed by the manifest.
    pub sst_files: usize,
    /// SSTables present on disk but not in the manifest, deleted: a flush
    /// that crashed before committing its manifest edit.
    pub orphan_ssts_removed: usize,
    /// SSTables adopted from a directory without a manifest (one created by
    /// phase 2/3 code).
    pub ssts_adopted_without_manifest: usize,
    pub wal_files_replayed: usize,
    /// WAL records applied to the memtable.
    pub records_replayed: u64,
    /// WAL records skipped because an SSTable already holds them.
    pub records_already_flushed: u64,
    /// WALs deleted during recovery because SSTables already hold all their
    /// data (below the manifest's log number, or fully covered by sequence
    /// number).
    pub wal_files_deleted: usize,
    /// Leftover `*.sst.tmp` / `CURRENT.tmp` files from an interrupted write,
    /// deleted.
    pub temp_files_removed: usize,
    /// Bytes cut from WAL tails (torn or corrupt records, trailing garbage),
    /// plus the size of any later WAL files set aside in `PointInTime` mode.
    pub bytes_discarded: u64,
    /// Why bytes were discarded, if any were.
    pub discard_reason: Option<String>,
}

/// Read-path counters since the database was opened.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReadStats {
    /// Calls to [`Db::get`].
    pub gets: u64,
    /// Gets answered by the active or immutable memtable (value or tombstone).
    pub memtable_hits: u64,
    /// SSTables skipped because the key is outside their key range.
    pub range_skips: u64,
    /// SSTables skipped because their Bloom filter ruled the key out.
    pub bloom_skips: u64,
    /// SSTables actually searched (index lookup plus one data block read).
    pub sst_probes: u64,
    /// Probes where the filter said "maybe" but the key wasn't there. Over
    /// absent keys, `bloom_false_positives / (bloom_false_positives +
    /// bloom_skips)` is the measured false-positive rate.
    pub bloom_false_positives: u64,
    /// SSTables skipped by range scans because their key range doesn't
    /// overlap the scan.
    pub scan_range_skips: u64,
    /// Data blocks read from disk by the currently live SSTables.
    pub data_blocks_read: u64,
}

#[derive(Default)]
struct StatCounters {
    gets: AtomicU64,
    memtable_hits: AtomicU64,
    range_skips: AtomicU64,
    bloom_skips: AtomicU64,
    sst_probes: AtomicU64,
    bloom_false_positives: AtomicU64,
    scan_range_skips: AtomicU64,
}

fn bump(c: &AtomicU64) {
    c.fetch_add(1, Ordering::Relaxed);
}

/// A MiniRocks database. `Db` is `Send + Sync`; share it with `Arc`.
///
/// Data lives in three layers, newest first: the active memtable, at most
/// one immutable memtable being flushed, and SSTables. A key's newest
/// version is in the newest layer that mentions it.
pub struct Db {
    inner: Arc<Inner>,
    flusher: Option<JoinHandle<()>>,
}

struct Inner {
    dir: PathBuf,
    options: Options,
    /// Serialises writers, so WAL order equals sequence-number order.
    writer: Mutex<WriterState>,
    /// Signalled (with `writer` held) when a flush finishes or fails.
    flushed: Condvar,
    state: RwLock<ReadState>,
    /// Set if a background flush fails. Writes then fail until reopen.
    bg_error: Mutex<Option<String>>,
    flush_tx: Sender<FlushMsg>,
    /// Appends version edits; locked by whoever changes the set of live
    /// SSTables (the flush thread, and the compaction thread later).
    manifest: Mutex<ManifestWriter>,
    recovery: RecoveryReport,
    stats: StatCounters,
}

struct WriterState {
    wal: WalWriter,
    next_seq: u64,
    next_file_number: u64,
    /// WAL files holding the active memtable's data. More than one only
    /// right after recovery from a crash that interrupted a flush.
    mem_wals: Vec<u64>,
}

struct ReadState {
    mem: MemTable,
    imm: Option<Arc<Immutable>>,
    /// Newest first. Replaced wholesale (copy-on-write) so readers can hold
    /// a snapshot without holding the lock.
    ssts: Arc<Vec<Arc<SstReader>>>,
}

/// A frozen memtable waiting to be written to `sst_number`.
struct Immutable {
    mem: Arc<MemTable>,
    wals: Vec<u64>,
    sst_number: u64,
    /// The WAL started at rotation: once this flush commits, every older WAL
    /// is obsolete, so this becomes the manifest's log number.
    next_log_number: u64,
}

enum FlushMsg {
    Flush,
    Shutdown,
}

impl Db {
    /// Open (or create) the database in `dir`: read the manifest, open the
    /// SSTables it lists, replay WAL records not yet in an SSTable, write a
    /// fresh manifest, and start the flush thread.
    pub fn open(dir: impl AsRef<Path>, options: Options) -> Result<Db> {
        validate(&options)?;
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        let mut report = RecoveryReport::default();

        // 1. Inventory. Every number counts toward the next file number, so
        //    a new file can never reuse the name of an old one.
        let mut max_number = 0u64;
        let mut wal_numbers = Vec::new();
        let mut sst_numbers = Vec::new();
        let mut manifest_numbers = Vec::new();
        for (number, kind) in list_files(&dir)? {
            max_number = max_number.max(number);
            match kind {
                FileKind::Wal => wal_numbers.push(number),
                FileKind::Sst => sst_numbers.push(number),
                FileKind::Manifest => manifest_numbers.push(number),
                FileKind::TempSst => {
                    // Never renamed into place, so its data is still in a WAL.
                    fs::remove_file(dir.join(filename::temp_sst_file_name(number)))?;
                    report.temp_files_removed += 1;
                }
                FileKind::DiscardedWal => {}
            }
        }
        let current_tmp = dir.join(filename::CURRENT_TMP);
        if current_tmp.exists() {
            // Never renamed over CURRENT, so the old CURRENT still stands.
            fs::remove_file(&current_tmp)?;
            report.temp_files_removed += 1;
        }
        if report.temp_files_removed > 0 {
            fsutil::sync_dir(&dir)?;
        }
        wal_numbers.sort_unstable();

        // 2. The manifest decides which SSTables are live.
        let mut state = match manifest::read_current(&dir)? {
            Some(n) => {
                let path = dir.join(filename::manifest_file_name(n));
                if !path.exists() {
                    return Err(Error::Corruption(format!("CURRENT names {}, which does not exist", path.display())));
                }
                let contents = manifest::read_manifest(&path)?;
                report.manifest_edits_replayed = contents.edits;
                report.manifest_torn_bytes = contents.torn_bytes;
                contents.state
            }
            None if !manifest_numbers.is_empty() => {
                return Err(Error::Corruption(
                    "manifest files exist but CURRENT is missing; refusing to guess which is live".into(),
                ));
            }
            None => {
                // A new database, or one written before the manifest existed
                // (phases 2-3), where every SSTable present was live.
                let mut s = ManifestState::default();
                for &n in &sst_numbers {
                    s.files.insert(n, fs::metadata(dir.join(filename::sst_file_name(n)))?.len());
                }
                report.ssts_adopted_without_manifest = sst_numbers.len();
                s
            }
        };

        // 3. SSTables the manifest doesn't list were never committed (a
        //    flush crashed before its manifest edit), or were already
        //    superseded. Their data is elsewhere, so delete them.
        for &n in &sst_numbers {
            if !state.files.contains_key(&n) {
                fs::remove_file(dir.join(filename::sst_file_name(n)))?;
                report.orphan_ssts_removed += 1;
            }
        }

        // 4. Open the live SSTables. A missing, resized or damaged one is
        //    corruption: it was fsynced before its edit was committed.
        let mut ssts = Vec::new();
        let mut flushed_seq = 0u64;
        for (&n, &size) in &state.files {
            let path = dir.join(filename::sst_file_name(n));
            if !path.exists() {
                return Err(Error::Corruption(format!("live SSTable {} is missing", path.display())));
            }
            let t = SstReader::open(&path, n)?;
            if t.meta().file_size != size {
                return Err(Error::Corruption(format!(
                    "{}: manifest says {size} bytes, file has {}",
                    path.display(),
                    t.meta().file_size
                )));
            }
            flushed_seq = flushed_seq.max(t.meta().max_seq);
            ssts.push(Arc::new(t));
        }
        // Order by data age, newest first. Not by file number: a compaction
        // output (phase 5) gets a new, high number but holds older data than
        // tables flushed while it ran.
        ssts.sort_by_key(|t| std::cmp::Reverse((t.meta().max_seq, t.number())));
        report.sst_files = ssts.len();

        // 5. WALs below the manifest's log number hold only flushed data:
        //    a crash hit between committing a flush and deleting its WAL.
        let (obsolete, wal_numbers): (Vec<u64>, Vec<u64>) =
            wal_numbers.into_iter().partition(|&n| n < state.log_number);
        for &n in &obsolete {
            fs::remove_file(dir.join(wal::wal_file_name(n)))?;
            report.wal_files_deleted += 1;
        }

        // 6. WAL replay. Records at or below `flushed_seq` are already in an
        //    SSTable and must NOT be replayed: that would put stale values
        //    above newer SSTables. (With the manifest's log number this
        //    should never trigger; it is kept as a second line of defence.)
        let mut mem = MemTable::new();
        let mut max_seq = flushed_seq;
        let mut replayed: Vec<(u64, bool)> = Vec::new(); // (number, has unflushed records)
        for (i, &number) in wal_numbers.iter().enumerate() {
            let path = dir.join(wal::wal_file_name(number));
            let contents = wal::read_wal(&path)?;
            let is_last = i + 1 == wal_numbers.len();

            if let Some(reason) = contents.stop_reason {
                // Only the newest WAL can have a torn tail. Older ones were
                // fsynced before the writer switched away from them.
                let mid_log = contents.valid_record_after_corruption.is_some() || !is_last;
                if mid_log && options.wal_recovery == WalRecoveryMode::TolerateTornTail {
                    let detail = match contents.valid_record_after_corruption {
                        Some(off) => format!("a valid record follows at offset {off}"),
                        None => "it is not the newest WAL".to_string(),
                    };
                    return Err(Error::Corruption(format!(
                        "{}: {reason} at offset {}, but {detail}; refusing to discard \
                         possibly acknowledged writes (WalRecoveryMode::PointInTime \
                         recovers the valid prefix)",
                        path.display(),
                        contents.valid_len
                    )));
                }
            }

            let mut has_unflushed = false;
            for rec in contents.records {
                if rec.seq() <= flushed_seq {
                    report.records_already_flushed += 1;
                    continue;
                }
                has_unflushed = true;
                max_seq = max_seq.max(rec.seq());
                match rec {
                    Record::Put { seq, key, value } => mem.insert(key, seq, Some(value)),
                    Record::Delete { seq, key } => mem.insert(key, seq, None),
                }
                report.records_replayed += 1;
            }
            report.wal_files_replayed += 1;
            replayed.push((number, has_unflushed));

            if let Some(reason) = contents.stop_reason {
                // Cut the invalid tail so new records are appended directly
                // after the last valid one, where recovery will find them.
                wal::truncate(&path, contents.valid_len)?;
                report.bytes_discarded += contents.file_len - contents.valid_len;
                report.discard_reason = Some(format!(
                    "{}: {reason} at offset {}",
                    path.display(),
                    contents.valid_len
                ));
                // PointInTime: anything in later WALs comes after the damage.
                // Set the files aside rather than deleting them.
                for &later in &wal_numbers[i + 1..] {
                    let later_path = dir.join(wal::wal_file_name(later));
                    report.bytes_discarded += fs::metadata(&later_path)?.len();
                    let mut aside = later_path.clone().into_os_string();
                    aside.push(".discarded");
                    fs::rename(&later_path, aside)?;
                }
                if !is_last {
                    fsutil::sync_dir(&dir)?;
                }
                break;
            }
        }

        // 7. Keep appending to the newest WAL. Older WALs with nothing left
        //    to replay are fully covered by SSTables, so delete them.
        let mut next_file_number = state.next_file_number.max(max_number + 1);
        let active = match replayed.last() {
            Some(&(n, _)) => n,
            None => {
                next_file_number += 1;
                next_file_number - 1
            }
        };
        let mut mem_wals = Vec::new();
        for &(n, has_unflushed) in &replayed {
            if n == active || has_unflushed {
                mem_wals.push(n);
            } else {
                fs::remove_file(dir.join(wal::wal_file_name(n)))?;
                report.wal_files_deleted += 1;
            }
        }
        if report.wal_files_deleted > 0 {
            fsutil::sync_dir(&dir)?;
        }
        if mem_wals.is_empty() {
            mem_wals.push(active);
        }
        let wal = WalWriter::open(&dir.join(wal::wal_file_name(active)), options.sync)?;

        // 8. Start a fresh manifest holding one snapshot edit, switch CURRENT
        //    to it atomically, then delete the old manifest(s). This keeps
        //    the manifest from growing across restarts and drops any torn
        //    tail. A crash before the switch leaves the old CURRENT valid.
        state.log_number = *mem_wals.iter().min().unwrap();
        let manifest_number = next_file_number;
        next_file_number += 1;
        state.next_file_number = next_file_number;
        let manifest = ManifestWriter::create(&dir, manifest_number, &state)?;
        for &n in &manifest_numbers {
            fs::remove_file(dir.join(filename::manifest_file_name(n)))?;
        }
        if !manifest_numbers.is_empty() {
            fsutil::sync_dir(&dir)?;
        }

        let (flush_tx, flush_rx) = mpsc::channel();
        let inner = Arc::new(Inner {
            dir,
            options,
            writer: Mutex::new(WriterState { wal, next_seq: max_seq + 1, next_file_number, mem_wals }),
            flushed: Condvar::new(),
            state: RwLock::new(ReadState { mem, imm: None, ssts: Arc::new(ssts) }),
            bg_error: Mutex::new(None),
            flush_tx,
            manifest: Mutex::new(manifest),
            recovery: report,
            stats: StatCounters::default(),
        });
        let bg = Arc::clone(&inner);
        let flusher = std::thread::Builder::new()
            .name("minirocks-flush".into())
            .spawn(move || bg.flush_loop(flush_rx))?;
        Ok(Db { inner, flusher: Some(flusher) })
    }

    /// Set `key` to `value`. Durable per the configured [`SyncPolicy`] once
    /// this returns `Ok`.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        check_key(key)?;
        if value.len() > MAX_VALUE_LEN {
            return Err(Error::InvalidArgument(format!(
                "value is {} bytes; maximum is {MAX_VALUE_LEN}",
                value.len()
            )));
        }
        self.inner.write(key, Some(value))
    }

    /// Delete `key` by writing a tombstone. Deleting a missing key is not an
    /// error. Durable under the same rules as [`Db::put`].
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        check_key(key)?;
        self.inner.write(key, None)
    }

    /// Newest value for `key`. Searches the memtable, the immutable memtable,
    /// then SSTables newest to oldest, and stops at the first entry found (a
    /// tombstone means "deleted"). An SSTable is read only if the key is
    /// inside its key range *and* its Bloom filter says "maybe".
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let stats = &self.inner.stats;
        bump(&stats.gets);
        let (imm, ssts) = {
            let s = self.inner.state_read();
            match s.mem.get(key) {
                Lookup::Value(v) => {
                    bump(&stats.memtable_hits);
                    return Ok(Some(v.to_vec()));
                }
                Lookup::Deleted => {
                    bump(&stats.memtable_hits);
                    return Ok(None);
                }
                Lookup::NotFound => {}
            }
            (s.imm.clone(), Arc::clone(&s.ssts))
        };
        if let Some(imm) = imm {
            match imm.mem.get(key) {
                Lookup::Value(v) => {
                    bump(&stats.memtable_hits);
                    return Ok(Some(v.to_vec()));
                }
                Lookup::Deleted => {
                    bump(&stats.memtable_hits);
                    return Ok(None);
                }
                Lookup::NotFound => {}
            }
        }
        for t in ssts.iter() {
            if !t.key_in_range(key) {
                bump(&stats.range_skips);
                continue;
            }
            if !t.may_contain(key) {
                bump(&stats.bloom_skips);
                continue;
            }
            bump(&stats.sst_probes);
            match t.get(key)? {
                Some(entry) => return Ok(entry.value),
                None => bump(&stats.bloom_false_positives),
            }
        }
        Ok(None)
    }

    /// Iterate over live key-value pairs with keys inside the bounds, in
    /// ascending key order, from a snapshot taken now. Writes, deletes and
    /// flushes after this call are not visible to the iterator.
    ///
    /// The active memtable's part of the range is copied at creation (at
    /// most `memtable_size`); everything else is read lazily, one block at a
    /// time. SSTables whose key range doesn't overlap the bounds are skipped.
    pub fn iter(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Result<DbIter> {
        let owned_end = end.map(<[u8]>::to_vec);
        if is_empty_range(start, end) {
            return Ok(DbIter::new(Vec::new(), owned_end));
        }
        let (mem_entries, imm, ssts) = {
            let s = self.inner.state_read();
            let mem: Vec<_> = s.mem.range(start, end).map(|(k, e)| (k.to_vec(), e.clone())).collect();
            (mem, s.imm.clone(), Arc::clone(&s.ssts))
        };
        // Newest first: the merge breaks ties in favour of lower indices.
        let mut sources = vec![Source::copied(mem_entries)];
        if let Some(imm) = imm {
            sources.push(Source::frozen(Arc::clone(&imm.mem), start));
        }
        for t in ssts.iter() {
            if !t.overlaps(start, end) {
                bump(&self.inner.stats.scan_range_skips);
                continue;
            }
            let mut it = SstIter::new(Arc::clone(t));
            it.seek_bound(start)?;
            sources.push(Source::Sst(it));
        }
        Ok(DbIter::new(sources, owned_end))
    }

    /// All live key-value pairs inside the bounds, collected from [`Db::iter`].
    pub fn scan(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.iter(start, end)?.collect()
    }

    pub fn stats(&self) -> ReadStats {
        let c = &self.inner.stats;
        let load = |a: &AtomicU64| a.load(Ordering::Relaxed);
        ReadStats {
            gets: load(&c.gets),
            memtable_hits: load(&c.memtable_hits),
            range_skips: load(&c.range_skips),
            bloom_skips: load(&c.bloom_skips),
            sst_probes: load(&c.sst_probes),
            bloom_false_positives: load(&c.bloom_false_positives),
            scan_range_skips: load(&c.scan_range_skips),
            data_blocks_read: self.inner.state_read().ssts.iter().map(|t| t.blocks_read()).sum(),
        }
    }

    /// fsync the WAL now, making every acknowledged write durable regardless
    /// of the sync policy.
    pub fn sync(&self) -> Result<()> {
        self.inner.lock_writer().wal.sync()
    }

    /// Flush the active memtable to an SSTable and wait for it (and any
    /// flush already in progress) to finish.
    pub fn flush(&self) -> Result<()> {
        let inner = &self.inner;
        let mut w = inner.wait_for_imm(inner.lock_writer())?;
        if !inner.state_read().mem.is_empty() {
            inner.rotate(&mut w)?;
            drop(inner.wait_for_imm(w)?);
        }
        Ok(())
    }

    pub fn recovery_report(&self) -> &RecoveryReport {
        &self.inner.recovery
    }

    pub fn path(&self) -> &Path {
        &self.inner.dir
    }

    /// Number of live SSTables.
    pub fn sst_count(&self) -> usize {
        self.inner.state_read().ssts.len()
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        // Messages are handled in order, so a queued flush finishes first.
        let _ = self.inner.flush_tx.send(FlushMsg::Shutdown);
        if let Some(h) = self.flusher.take() {
            let _ = h.join();
        }
    }
}

impl Inner {
    fn write(&self, key: &[u8], value: Option<&[u8]>) -> Result<()> {
        let mut w = self.make_room(self.lock_writer())?;
        let seq = w.next_seq;
        let rec = match value {
            Some(value) => RecordRef::Put { seq, key, value },
            None => RecordRef::Delete { seq, key },
        };
        w.wal.append(rec)?;
        w.next_seq += 1;
        // Applied while still holding the writer lock, so the memtable sees
        // writes in sequence order and only after they are in the WAL.
        self.state_write().mem.insert(key.to_vec(), seq, value.map(<[u8]>::to_vec));
        Ok(())
    }

    /// Before a write: if the memtable is full, freeze it and start a flush.
    /// If the previous flush is still running, wait for it (write stall).
    fn make_room<'a>(&'a self, mut w: MutexGuard<'a, WriterState>) -> Result<MutexGuard<'a, WriterState>> {
        loop {
            self.check_bg_error()?;
            let (full, flushing) = {
                let s = self.state_read();
                (s.mem.approx_bytes() >= self.options.memtable_size, s.imm.is_some())
            };
            if !full {
                return Ok(w);
            }
            if flushing {
                w = self.flushed.wait(w).unwrap_or_else(|e| e.into_inner());
                continue;
            }
            self.rotate(&mut w)?;
            return Ok(w);
        }
    }

    /// Freeze the active memtable and switch to a fresh WAL. The caller holds
    /// the writer lock and has checked that no flush is in progress.
    fn rotate(&self, w: &mut WriterState) -> Result<()> {
        // The old WAL must be complete on disk before anything is written to
        // the new one. Otherwise an OS crash could leave it with a torn tail
        // in front of newer records, which recovery treats as corruption.
        w.wal.sync()?;
        let wal_number = w.next_file_number;
        let sst_number = wal_number + 1;
        let new_wal = WalWriter::open(&self.dir.join(wal::wal_file_name(wal_number)), self.options.sync)?;
        w.next_file_number += 2;
        w.wal = new_wal;
        let wals = std::mem::replace(&mut w.mem_wals, vec![wal_number]);
        {
            let mut s = self.state_write();
            let mem = std::mem::take(&mut s.mem);
            s.imm = Some(Arc::new(Immutable { mem: Arc::new(mem), wals, sst_number, next_log_number: wal_number }));
        }
        let _ = self.flush_tx.send(FlushMsg::Flush);
        Ok(())
    }

    fn wait_for_imm<'a>(&'a self, mut w: MutexGuard<'a, WriterState>) -> Result<MutexGuard<'a, WriterState>> {
        loop {
            self.check_bg_error()?;
            if self.state_read().imm.is_none() {
                return Ok(w);
            }
            w = self.flushed.wait(w).unwrap_or_else(|e| e.into_inner());
        }
    }

    fn flush_loop(&self, rx: Receiver<FlushMsg>) {
        while let Ok(FlushMsg::Flush) = rx.recv() {
            self.flush_imm();
        }
    }

    /// Write the immutable memtable to an SSTable, commit it to the manifest,
    /// install it, then delete the WALs it came from. Runs on the flush
    /// thread.
    fn flush_imm(&self) {
        let Some(imm) = self.state_read().imm.clone() else { return };
        let path = self.dir.join(filename::sst_file_name(imm.sst_number));
        let result = sstable::build_table(
            &self.dir,
            imm.sst_number,
            imm.mem.range(Bound::Unbounded, Bound::Unbounded),
            self.options.block_size,
            self.options.bloom_bits_per_key,
        )
        .and_then(|_| SstReader::open(&path, imm.sst_number))
        .and_then(|table| {
            // The commit point. Before this edit is synced, a crash leaves
            // the SSTable as an orphan (deleted on recovery) and the WAL
            // replays. After it, the WALs are obsolete.
            let edit = VersionEdit {
                log_number: Some(imm.next_log_number),
                next_file_number: Some(imm.sst_number + 1),
                added: vec![(imm.sst_number, table.meta().file_size)],
                deleted: Vec::new(),
            };
            self.manifest.lock().unwrap_or_else(|e| e.into_inner()).append(&edit)?;
            Ok(table)
        });

        match result {
            Ok(table) => {
                {
                    let mut s = self.state_write();
                    let mut ssts = Vec::with_capacity(s.ssts.len() + 1);
                    ssts.push(Arc::new(table));
                    ssts.extend(s.ssts.iter().cloned());
                    s.ssts = Arc::new(ssts);
                    s.imm = None;
                }
                // The SSTable is committed and installed, so the WALs are
                // redundant. If a crash hits before this, recovery deletes
                // them by log number. Failure to delete is harmless.
                for &n in &imm.wals {
                    let _ = fs::remove_file(self.dir.join(wal::wal_file_name(n)));
                }
                let _ = fsutil::sync_dir(&self.dir);
            }
            Err(e) => {
                *self.bg_error.lock().unwrap_or_else(|e| e.into_inner()) =
                    Some(format!("flush to SSTable {} failed: {e}", imm.sst_number));
            }
        }
        // Take the writer lock before notifying, so a writer that has just
        // seen `imm.is_some()` is already waiting and can't miss the wakeup.
        drop(self.lock_writer());
        self.flushed.notify_all();
    }

    fn check_bg_error(&self) -> Result<()> {
        match &*self.bg_error.lock().unwrap_or_else(|e| e.into_inner()) {
            Some(msg) => Err(Error::Poisoned(msg.clone())),
            None => Ok(()),
        }
    }

    fn lock_writer(&self) -> MutexGuard<'_, WriterState> {
        // A panic while holding the lock cannot leave the WAL half-updated in
        // memory (WalWriter poisons itself on I/O errors), so carry on.
        self.writer.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn state_read(&self) -> RwLockReadGuard<'_, ReadState> {
        self.state.read().unwrap_or_else(|e| e.into_inner())
    }

    fn state_write(&self) -> RwLockWriteGuard<'_, ReadState> {
        self.state.write().unwrap_or_else(|e| e.into_inner())
    }
}

fn validate(o: &Options) -> Result<()> {
    let bad = |m: &str| Err(Error::InvalidArgument(m.into()));
    if let SyncPolicy::EveryN(0) = o.sync {
        return bad("SyncPolicy::EveryN needs n >= 1");
    }
    if o.memtable_size == 0 {
        return bad("memtable_size must be > 0");
    }
    if o.block_size == 0 {
        return bad("block_size must be > 0");
    }
    if !(1..=64).contains(&o.bloom_bits_per_key) {
        return bad("bloom_bits_per_key must be in 1..=64");
    }
    Ok(())
}

fn check_key(key: &[u8]) -> Result<()> {
    if key.len() > MAX_KEY_LEN {
        return Err(Error::InvalidArgument(format!(
            "key is {} bytes; maximum is {MAX_KEY_LEN}",
            key.len()
        )));
    }
    Ok(())
}

/// Recognised files in `dir` (anything else is ignored).
fn list_files(dir: &Path) -> Result<Vec<(u64, FileKind)>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(dir)? {
        if let Some(f) = entry?.file_name().to_str().and_then(filename::parse_file_name) {
            files.push(f);
        }
    }
    Ok(files)
}
