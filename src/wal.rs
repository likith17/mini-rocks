//! Write-ahead log.
//!
//! Every mutation is appended here before it is applied to the memtable, so
//! the memtable can always be rebuilt after a crash.
//!
//! # Record format
//!
//! The log is a flat sequence of records. All integers are little-endian.
//!
//! ```text
//! +----------+----------+---------+----------------------------------------+
//! | crc32    | len      | kind    | payload (len bytes)                    |
//! | u32      | u32      | u8      | seq u64 | key_len u32 | key | value    |
//! +----------+----------+---------+----------------------------------------+
//!  \_ covers len, kind and payload _/
//! ```
//!
//! * `kind` is 1 for put, 2 for delete. A delete has no value bytes.
//! * The value length is implicit: `len - 12 - key_len`.
//! * The CRC covers the length field too, so a flipped bit in `len` is caught
//!   instead of sending the reader off to a wrong offset.
//!
//! A reader stops at the first record that is incomplete or fails its
//! checksum. Whether that is accepted as a torn tail or reported as
//! corruption is the caller's decision (see [`crate::WalRecoveryMode`]).

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};
use crate::fsutil;
use crate::options::SyncPolicy;

/// crc32 (4) + len (4) + kind (1).
pub const HEADER_LEN: usize = 9;
/// seq (8) + key_len (4).
pub const PAYLOAD_FIXED_LEN: usize = 12;
/// Upper bound on a record's payload. Lengths above this are treated as
/// corruption without reading further.
pub const MAX_PAYLOAD_LEN: usize = 64 * 1024 * 1024;

const KIND_PUT: u8 = 1;
const KIND_DELETE: u8 = 2;

/// Buffers bigger than this are released after a write instead of kept.
const MAX_RETAINED_BUF: usize = 1024 * 1024;

pub fn wal_file_name(number: u64) -> String {
    format!("{number:06}.wal")
}

pub fn parse_wal_file_name(name: &str) -> Option<u64> {
    let digits = name.strip_suffix(".wal")?;
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

/// A record to append, borrowing its key and value.
#[derive(Debug, Clone, Copy)]
pub enum RecordRef<'a> {
    Put { seq: u64, key: &'a [u8], value: &'a [u8] },
    Delete { seq: u64, key: &'a [u8] },
}

/// A decoded record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Record {
    Put { seq: u64, key: Vec<u8>, value: Vec<u8> },
    Delete { seq: u64, key: Vec<u8> },
}

impl Record {
    pub fn seq(&self) -> u64 {
        match self {
            Record::Put { seq, .. } | Record::Delete { seq, .. } => *seq,
        }
    }
}

/// Total on-disk size of a record with the given key and value lengths
/// (use `value_len = 0` for a delete).
pub fn encoded_len(key_len: usize, value_len: usize) -> usize {
    HEADER_LEN + PAYLOAD_FIXED_LEN + key_len + value_len
}

/// Append the encoding of `rec` to `buf`.
pub fn encode(rec: RecordRef<'_>, buf: &mut Vec<u8>) {
    let (kind, seq, key, value): (u8, u64, &[u8], &[u8]) = match rec {
        RecordRef::Put { seq, key, value } => (KIND_PUT, seq, key, value),
        RecordRef::Delete { seq, key } => (KIND_DELETE, seq, key, &[]),
    };
    let payload_len = PAYLOAD_FIXED_LEN + key.len() + value.len();
    assert!(payload_len <= MAX_PAYLOAD_LEN, "record too large");

    let start = buf.len();
    buf.reserve(HEADER_LEN + payload_len);
    buf.extend_from_slice(&[0; 4]); // crc placeholder
    buf.extend_from_slice(&(payload_len as u32).to_le_bytes());
    buf.push(kind);
    buf.extend_from_slice(&seq.to_le_bytes());
    buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
    buf.extend_from_slice(key);
    buf.extend_from_slice(value);
    let crc = crc32fast::hash(&buf[start + 4..]);
    buf[start..start + 4].copy_from_slice(&crc.to_le_bytes());
}

/// Result of decoding one record from the front of a byte slice.
#[derive(Debug, PartialEq, Eq)]
pub enum Decoded {
    /// A valid record occupying the first `len` bytes.
    Record { record: Record, len: usize },
    /// The slice ends before the record does.
    Incomplete,
    /// The bytes do not form a valid record.
    Corrupt(&'static str),
}

pub fn decode(data: &[u8]) -> Decoded {
    if data.len() < HEADER_LEN {
        return Decoded::Incomplete;
    }
    let stored_crc = u32::from_le_bytes(data[0..4].try_into().unwrap());
    let payload_len = u32::from_le_bytes(data[4..8].try_into().unwrap()) as usize;
    if payload_len > MAX_PAYLOAD_LEN {
        return Decoded::Corrupt("record length exceeds maximum");
    }
    let total = HEADER_LEN + payload_len;
    if data.len() < total {
        return Decoded::Incomplete;
    }
    if crc32fast::hash(&data[4..total]) != stored_crc {
        return Decoded::Corrupt("checksum mismatch");
    }
    // The checksum matched, so anything malformed below was written that way
    // (a bug or a format mismatch), not damaged afterwards.
    let kind = data[8];
    let payload = &data[HEADER_LEN..total];
    if payload.len() < PAYLOAD_FIXED_LEN {
        return Decoded::Corrupt("payload too short");
    }
    let seq = u64::from_le_bytes(payload[0..8].try_into().unwrap());
    let key_len = u32::from_le_bytes(payload[8..12].try_into().unwrap()) as usize;
    let rest = &payload[PAYLOAD_FIXED_LEN..];
    if key_len > rest.len() {
        return Decoded::Corrupt("key length exceeds payload");
    }
    let (key, value) = rest.split_at(key_len);
    let record = match kind {
        KIND_PUT => Record::Put {
            seq,
            key: key.to_vec(),
            value: value.to_vec(),
        },
        KIND_DELETE if value.is_empty() => Record::Delete {
            seq,
            key: key.to_vec(),
        },
        KIND_DELETE => return Decoded::Corrupt("delete record carries a value"),
        _ => return Decoded::Corrupt("unknown record kind"),
    };
    Decoded::Record { record, len: total }
}

/// Everything recovery learns from one WAL file.
#[derive(Debug)]
pub struct WalContents {
    /// Valid records, in log order.
    pub records: Vec<Record>,
    /// Length of the longest prefix made only of whole, valid records.
    pub valid_len: u64,
    pub file_len: u64,
    /// Why reading stopped before the end of the file, if it did.
    pub stop_reason: Option<&'static str>,
    /// Offset of a valid record found *after* the first invalid one. A torn
    /// write only damages the tail, so this means mid-log corruption.
    pub valid_record_after_corruption: Option<u64>,
}

/// Read and validate a whole WAL file.
///
/// The file is read into memory at once. That is fine because a WAL only
/// holds what fits in one memtable (bounded once flushing lands in phase 2).
pub fn read_wal(path: &Path) -> Result<WalContents> {
    let data = fs::read(path)?;
    let mut records = Vec::new();
    let mut off = 0usize;
    let mut stop_reason = None;
    while off < data.len() {
        match decode(&data[off..]) {
            Decoded::Record { record, len } => {
                records.push(record);
                off += len;
            }
            Decoded::Incomplete => {
                stop_reason = Some("incomplete record at end of file");
                break;
            }
            Decoded::Corrupt(reason) => {
                stop_reason = Some(reason);
                break;
            }
        }
    }

    // Look for any valid record past the damage, at every byte offset.
    // CRC32 gives a ~2^-32 false-positive rate per offset on random bytes;
    // see DECISIONS.md for the one realistic false positive.
    let valid_record_after_corruption = if stop_reason.is_some() {
        (off + 1..data.len())
            .find(|&o| matches!(decode(&data[o..]), Decoded::Record { .. }))
            .map(|o| o as u64)
    } else {
        None
    };

    Ok(WalContents {
        records,
        valid_len: off as u64,
        file_len: data.len() as u64,
        stop_reason,
        valid_record_after_corruption,
    })
}

/// Cut a WAL file back to `len` bytes and make the new length durable.
pub fn truncate(path: &Path, len: u64) -> Result<()> {
    let file = OpenOptions::new().write(true).open(path)?;
    file.set_len(len)?;
    file.sync_all()?;
    Ok(())
}

/// Appends records to a WAL file.
pub struct WalWriter {
    file: File,
    path: PathBuf,
    policy: SyncPolicy,
    len: u64,
    /// Writes handed to the OS since the last successful `sync_data`.
    unsynced: u32,
    buf: Vec<u8>,
    poisoned: Option<String>,
}

impl WalWriter {
    /// Open `path` for appending, creating it if needed.
    pub fn open(path: &Path, policy: SyncPolicy) -> Result<Self> {
        let existed = path.exists();
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        if !existed {
            // Make the new (empty) file and its directory entry durable
            // before any acknowledged write depends on them.
            file.sync_all()?;
            if let Some(dir) = path.parent() {
                fsutil::sync_dir(dir)?;
            }
        }
        let len = file.metadata()?.len();
        Ok(WalWriter {
            file,
            path: path.to_path_buf(),
            policy,
            len,
            unsynced: 0,
            buf: Vec::new(),
            poisoned: None,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Bytes in the file, including any not yet synced.
    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Append one record, fsyncing first if the policy calls for it. When
    /// this returns `Ok`, the record is acknowledged.
    pub fn append(&mut self, rec: RecordRef<'_>) -> Result<()> {
        self.check_poisoned()?;
        self.buf.clear();
        encode(rec, &mut self.buf);
        // One write call per record, no user-space buffering: once this
        // returns, the record is in the OS page cache and survives a crash of
        // this process.
        if let Err(e) = self.file.write_all(&self.buf) {
            // Part of the record may be on disk. Appending after it would put
            // acknowledged records behind garbage, where recovery would never
            // reach them, so refuse all further writes.
            self.poison(format!("WAL write failed: {e}"));
            return Err(Error::Io(e));
        }
        self.len += self.buf.len() as u64;
        if self.buf.capacity() > MAX_RETAINED_BUF {
            self.buf = Vec::new();
        }
        self.unsynced += 1;
        let sync_now = match self.policy {
            SyncPolicy::Always => true,
            SyncPolicy::EveryN(n) => self.unsynced >= n,
            SyncPolicy::Never => false,
        };
        if sync_now {
            self.sync()?;
        }
        Ok(())
    }

    /// Force everything written so far to stable storage.
    pub fn sync(&mut self) -> Result<()> {
        self.check_poisoned()?;
        if self.unsynced == 0 {
            return Ok(());
        }
        if let Err(e) = self.file.sync_data() {
            // After a failed fsync the kernel may have dropped the dirty
            // pages and cleared the error, so a retry can "succeed" without
            // the data being on disk. Treat it as fatal for this handle.
            self.poison(format!("WAL fsync failed: {e}"));
            return Err(Error::Io(e));
        }
        self.unsynced = 0;
        Ok(())
    }

    fn check_poisoned(&self) -> Result<()> {
        match &self.poisoned {
            Some(reason) => Err(Error::Poisoned(reason.clone())),
            None => Ok(()),
        }
    }

    fn poison(&mut self, reason: String) {
        self.poisoned = Some(reason);
    }
}

impl Drop for WalWriter {
    fn drop(&mut self) {
        // Clean shutdown: make buffered writes durable on a best-effort basis.
        if self.poisoned.is_none() && self.unsynced > 0 {
            let _ = self.file.sync_data();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enc(rec: RecordRef<'_>) -> Vec<u8> {
        let mut buf = Vec::new();
        encode(rec, &mut buf);
        buf
    }

    #[test]
    fn roundtrip_put_and_delete() {
        let put = enc(RecordRef::Put { seq: 7, key: b"key", value: b"value" });
        assert_eq!(put.len(), encoded_len(3, 5));
        assert_eq!(
            decode(&put),
            Decoded::Record {
                record: Record::Put { seq: 7, key: b"key".to_vec(), value: b"value".to_vec() },
                len: put.len()
            }
        );

        let del = enc(RecordRef::Delete { seq: 8, key: b"key" });
        assert_eq!(del.len(), encoded_len(3, 0));
        assert_eq!(
            decode(&del),
            Decoded::Record { record: Record::Delete { seq: 8, key: b"key".to_vec() }, len: del.len() }
        );
    }

    #[test]
    fn empty_key_and_value_roundtrip() {
        let put = enc(RecordRef::Put { seq: 1, key: b"", value: b"" });
        assert!(matches!(decode(&put), Decoded::Record { .. }));
    }

    #[test]
    fn every_truncation_is_incomplete() {
        let rec = enc(RecordRef::Put { seq: 1, key: b"abc", value: b"defgh" });
        for cut in 0..rec.len() {
            assert_eq!(decode(&rec[..cut]), Decoded::Incomplete, "cut at {cut}");
        }
    }

    #[test]
    fn every_single_bit_flip_is_detected() {
        let rec = enc(RecordRef::Put { seq: 42, key: b"abc", value: b"defgh" });
        for byte in 0..rec.len() {
            for bit in 0..8 {
                let mut damaged = rec.clone();
                damaged[byte] ^= 1 << bit;
                assert!(
                    !matches!(decode(&damaged), Decoded::Record { .. }),
                    "flip at byte {byte} bit {bit} went undetected"
                );
            }
        }
    }

    #[test]
    fn zeroed_region_is_not_a_record() {
        assert!(!matches!(decode(&[0u8; 64]), Decoded::Record { .. }));
    }

    #[test]
    fn huge_length_is_corrupt() {
        let mut rec = enc(RecordRef::Put { seq: 1, key: b"k", value: b"v" });
        rec[4..8].copy_from_slice(&u32::MAX.to_le_bytes());
        assert_eq!(decode(&rec), Decoded::Corrupt("record length exceeds maximum"));
    }

    #[test]
    fn file_names() {
        assert_eq!(wal_file_name(1), "000001.wal");
        assert_eq!(parse_wal_file_name("000001.wal"), Some(1));
        assert_eq!(parse_wal_file_name("1234567.wal"), Some(1234567));
        assert_eq!(parse_wal_file_name("+1.wal"), None);
        assert_eq!(parse_wal_file_name(".wal"), None);
        assert_eq!(parse_wal_file_name("000001.sst"), None);
    }
}
