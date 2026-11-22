use std::collections::BTreeMap;
use std::fs;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::thread::JoinHandle;

use crate::error::{Error, Result};
use crate::filename::{self, FileKind};
use crate::fsutil;
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
    /// SSTables found and opened.
    pub sst_files: usize,
    pub wal_files_replayed: usize,
    /// WAL records applied to the memtable.
    pub records_replayed: u64,
    /// WAL records skipped because an SSTable already holds them. This
    /// happens after a crash between installing an SSTable and deleting its
    /// WAL.
    pub records_already_flushed: u64,
    /// Fully flushed WALs deleted during recovery.
    pub wal_files_deleted: usize,
    /// Leftover `*.sst.tmp` files from an interrupted flush, deleted.
    pub temp_files_removed: usize,
    /// Bytes cut from WAL tails (torn or corrupt records, trailing garbage),
    /// plus the size of any later WAL files set aside in `PointInTime` mode.
    pub bytes_discarded: u64,
    /// Why bytes were discarded, if any were.
    pub discard_reason: Option<String>,
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
    recovery: RecoveryReport,
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
    mem: MemTable,
    wals: Vec<u64>,
    sst_number: u64,
}

enum FlushMsg {
    Flush,
    Shutdown,
}

impl Db {
    /// Open (or create) the database in `dir`: load SSTables, replay any WAL
    /// records they don't already hold, and start the flush thread.
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
        for (number, kind) in list_files(&dir)? {
            max_number = max_number.max(number);
            match kind {
                FileKind::Wal => wal_numbers.push(number),
                FileKind::Sst => sst_numbers.push(number),
                FileKind::TempSst => {
                    // Never renamed into place, so its data is still in a WAL.
                    fs::remove_file(dir.join(filename::temp_sst_file_name(number)))?;
                    report.temp_files_removed += 1;
                }
                FileKind::DiscardedWal => {}
            }
        }
        if report.temp_files_removed > 0 {
            fsutil::sync_dir(&dir)?;
        }
        wal_numbers.sort_unstable();
        sst_numbers.sort_unstable_by(|a, b| b.cmp(a));

        // 2. SSTables. A damaged SSTable is an error: it was fsynced before
        //    being renamed into place, so damage means real corruption.
        let mut ssts = Vec::new();
        let mut flushed_seq = 0u64;
        for &n in &sst_numbers {
            let t = SstReader::open(&dir.join(filename::sst_file_name(n)), n)?;
            flushed_seq = flushed_seq.max(t.meta().max_seq);
            ssts.push(Arc::new(t));
        }
        report.sst_files = ssts.len();

        // 3. WAL replay. Each flush covers every sequence number up to its
        //    SSTable's max_seq, so records at or below `flushed_seq` are
        //    already on disk in a newer form and must NOT be replayed:
        //    replaying them would put stale values above newer SSTables.
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

        // 4. Keep appending to the newest WAL. Older WALs with nothing left
        //    to replay are fully covered by SSTables, so delete them.
        let active = replayed.last().map_or(max_number + 1, |&(n, _)| n);
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

        let (flush_tx, flush_rx) = mpsc::channel();
        let inner = Arc::new(Inner {
            dir,
            options,
            writer: Mutex::new(WriterState {
                wal,
                next_seq: max_seq + 1,
                next_file_number: max_number.max(active) + 1,
                mem_wals,
            }),
            flushed: Condvar::new(),
            state: RwLock::new(ReadState { mem, imm: None, ssts: Arc::new(ssts) }),
            bg_error: Mutex::new(None),
            flush_tx,
            recovery: report,
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

    /// Newest value for `key`, searching memtable, immutable memtable, then
    /// SSTables newest to oldest, and stopping at the first entry found (a
    /// tombstone means "deleted").
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let (imm, ssts) = {
            let s = self.inner.state_read();
            match s.mem.get(key) {
                Lookup::Value(v) => return Ok(Some(v.to_vec())),
                Lookup::Deleted => return Ok(None),
                Lookup::NotFound => {}
            }
            (s.imm.clone(), Arc::clone(&s.ssts))
        };
        if let Some(imm) = imm {
            match imm.mem.get(key) {
                Lookup::Value(v) => return Ok(Some(v.to_vec())),
                Lookup::Deleted => return Ok(None),
                Lookup::NotFound => {}
            }
        }
        // Phase 2 reads every SSTable; Bloom-filter skipping comes in phase 3.
        for t in ssts.iter() {
            if let Some(entry) = t.get(key)? {
                return Ok(entry.value);
            }
        }
        Ok(None)
    }

    /// All live key-value pairs with keys inside the bounds, in ascending key
    /// order, from a consistent snapshot.
    ///
    /// Phase 2 implementation: overlays every layer (oldest first) into a
    /// map, so memory is proportional to the range. Phase 3 replaces this
    /// with a streaming merge iterator.
    pub fn scan(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if is_empty_range(start, end) {
            return Ok(Vec::new());
        }
        let (mem_entries, imm, ssts) = {
            let s = self.inner.state_read();
            let mem: Vec<_> = s.mem.range(start, end).map(|(k, e)| (k.to_vec(), e.value.clone())).collect();
            (mem, s.imm.clone(), Arc::clone(&s.ssts))
        };
        let mut merged: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();
        for t in ssts.iter().rev() {
            let mut it = SstIter::new(Arc::clone(t));
            it.seek_bound(start)?;
            while it.valid() && !past_end(it.key(), end) {
                merged.insert(it.key().to_vec(), it.entry()?.value);
                it.next()?;
            }
        }
        if let Some(imm) = imm {
            for (k, e) in imm.mem.range(start, end) {
                merged.insert(k.to_vec(), e.value.clone());
            }
        }
        merged.extend(mem_entries);
        Ok(merged.into_iter().filter_map(|(k, v)| v.map(|v| (k, v))).collect())
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
            s.imm = Some(Arc::new(Immutable { mem, wals, sst_number }));
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

    /// Write the immutable memtable to an SSTable, install it, then delete
    /// the WALs it came from. Runs on the flush thread.
    fn flush_imm(&self) {
        let Some(imm) = self.state_read().imm.clone() else { return };
        let result = sstable::build_table(
            &self.dir,
            imm.sst_number,
            imm.mem.range(Bound::Unbounded, Bound::Unbounded),
            self.options.block_size,
            self.options.bloom_bits_per_key,
        )
        .and_then(|_| SstReader::open(&self.dir.join(filename::sst_file_name(imm.sst_number)), imm.sst_number));

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
                // The SSTable is durable and installed, so the WALs are
                // redundant. If a crash hits before this, recovery skips their
                // records by sequence number. Failure to delete is harmless.
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

fn past_end(key: &[u8], end: Bound<&[u8]>) -> bool {
    match end {
        Bound::Included(e) => key > e,
        Bound::Excluded(e) => key >= e,
        Bound::Unbounded => false,
    }
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
