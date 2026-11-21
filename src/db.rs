use std::fs;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::error::{Error, Result};
use crate::fsutil;
use crate::memtable::{Lookup, MemTable};
use crate::options::{Options, SyncPolicy, WalRecoveryMode};
use crate::wal::{self, Record, RecordRef, WalWriter};

/// Largest accepted key, in bytes.
pub const MAX_KEY_LEN: usize = 64 * 1024;
/// Largest accepted value, in bytes.
pub const MAX_VALUE_LEN: usize = 32 * 1024 * 1024;

/// What recovery did when the database was opened.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RecoveryReport {
    pub wal_files_replayed: usize,
    pub records_replayed: u64,
    /// Bytes cut from WAL tails (torn or corrupt records, trailing garbage),
    /// plus the size of any later WAL files set aside in `PointInTime` mode.
    pub bytes_discarded: u64,
    /// Why bytes were discarded, if any were.
    pub discard_reason: Option<String>,
}

/// A MiniRocks database. `Db` is `Send + Sync`; share it with `Arc`.
pub struct Db {
    dir: PathBuf,
    /// Serialises writers so WAL order equals sequence-number order.
    writer: Mutex<WriterState>,
    mem: RwLock<MemTable>,
    recovery: RecoveryReport,
}

struct WriterState {
    wal: WalWriter,
    next_seq: u64,
}

impl Db {
    /// Open (or create) the database in `dir`, replaying the WAL.
    pub fn open(dir: impl AsRef<Path>, options: Options) -> Result<Db> {
        if let SyncPolicy::EveryN(0) = options.sync {
            return Err(Error::InvalidArgument("SyncPolicy::EveryN needs n >= 1".into()));
        }
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let wal_numbers = list_wal_files(&dir)?;
        let mut mem = MemTable::new();
        let mut report = RecoveryReport::default();
        let mut max_seq = 0u64;
        let mut active_wal = None;

        for (i, &number) in wal_numbers.iter().enumerate() {
            let path = dir.join(wal::wal_file_name(number));
            let contents = wal::read_wal(&path)?;
            let is_last = i + 1 == wal_numbers.len();

            if let Some(reason) = contents.stop_reason {
                // Only the newest WAL can have a torn tail; older ones were
                // complete when the writer moved past them.
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

            for rec in contents.records {
                max_seq = max_seq.max(rec.seq());
                match rec {
                    Record::Put { seq, key, value } => mem.insert(key, seq, Some(value)),
                    Record::Delete { seq, key } => mem.insert(key, seq, None),
                }
                report.records_replayed += 1;
            }
            report.wal_files_replayed += 1;
            active_wal = Some(number);

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
                if i + 1 < wal_numbers.len() {
                    fsutil::sync_dir(&dir)?;
                }
                break;
            }
        }

        let wal_path = dir.join(wal::wal_file_name(active_wal.unwrap_or(1)));
        let wal = WalWriter::open(&wal_path, options.sync)?;

        Ok(Db {
            dir,
            writer: Mutex::new(WriterState { wal, next_seq: max_seq + 1 }),
            mem: RwLock::new(mem),
            recovery: report,
        })
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
        let mut w = self.lock_writer();
        let seq = w.next_seq;
        w.wal.append(RecordRef::Put { seq, key, value })?;
        w.next_seq += 1;
        // Applied while still holding the writer lock, so the memtable sees
        // writes in sequence order and only after they are in the WAL.
        self.mem_write().insert(key.to_vec(), seq, Some(value.to_vec()));
        Ok(())
    }

    /// Delete `key` by writing a tombstone. Deleting a missing key is not an
    /// error. Durable under the same rules as [`Db::put`].
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        check_key(key)?;
        let mut w = self.lock_writer();
        let seq = w.next_seq;
        w.wal.append(RecordRef::Delete { seq, key })?;
        w.next_seq += 1;
        self.mem_write().insert(key.to_vec(), seq, None);
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(match self.mem_read().get(key) {
            Lookup::Value(v) => Some(v.to_vec()),
            Lookup::Deleted | Lookup::NotFound => None,
        })
    }

    /// All live key-value pairs with keys inside the bounds, in ascending key
    /// order. Results are copied out under a read lock, so they form a
    /// consistent snapshot. (Becomes a streaming merge iterator once
    /// SSTables exist.)
    pub fn scan(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mem = self.mem_read();
        Ok(mem
            .range(start, end)
            .filter_map(|(k, e)| e.value.as_ref().map(|v| (k.to_vec(), v.clone())))
            .collect())
    }

    /// fsync the WAL now, making every acknowledged write durable regardless
    /// of the sync policy.
    pub fn sync(&self) -> Result<()> {
        self.lock_writer().wal.sync()
    }

    pub fn recovery_report(&self) -> &RecoveryReport {
        &self.recovery
    }

    pub fn path(&self) -> &Path {
        &self.dir
    }

    fn lock_writer(&self) -> MutexGuard<'_, WriterState> {
        // A panic while holding the lock cannot leave the WAL half-updated in
        // memory (WalWriter poisons itself on I/O errors), so carry on.
        self.writer.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn mem_read(&self) -> RwLockReadGuard<'_, MemTable> {
        self.mem.read().unwrap_or_else(|e| e.into_inner())
    }

    fn mem_write(&self) -> RwLockWriteGuard<'_, MemTable> {
        self.mem.write().unwrap_or_else(|e| e.into_inner())
    }
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

/// WAL file numbers in `dir`, ascending.
fn list_wal_files(dir: &Path) -> Result<Vec<u64>> {
    let mut numbers = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if let Some(n) = entry.file_name().to_str().and_then(wal::parse_wal_file_name) {
            numbers.push(n);
        }
    }
    numbers.sort_unstable();
    Ok(numbers)
}
