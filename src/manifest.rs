//! The manifest: the authoritative record of which SSTables are live.
//!
//! `MANIFEST-NNNNNN` is an append-only log of [`VersionEdit`]s. Replaying
//! every edit in order gives the current [`ManifestState`]. `CURRENT` holds
//! the name of the active manifest and is only ever replaced atomically
//! (write `CURRENT.tmp`, fsync, rename, fsync the directory).
//!
//! # Record framing
//!
//! ```text
//! crc32 u32 | len u32 | payload (len bytes)      crc covers len || payload
//! ```
//!
//! # Edit payload
//!
//! A sequence of `tag varint, fields...`:
//!
//! | tag | meaning           | fields                         |
//! |-----|-------------------|--------------------------------|
//! | 1   | log number        | varint: WALs below this are obsolete |
//! | 2   | next file number  | varint                         |
//! | 3   | add SSTable       | varint number, varint file size |
//! | 4   | delete SSTable    | varint number                  |
//! | 5   | last sequence     | varint: highest sequence number in any SSTable |
//!
//! An unknown tag is corruption: older code must never misread a newer
//! format.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::coding::{put_varint64, Reader};
use crate::error::{Error, Result};
use crate::filename;
use crate::fsutil;

const HEADER_LEN: usize = 8;
const MAX_EDIT_LEN: usize = 64 * 1024 * 1024;

const TAG_LOG_NUMBER: u64 = 1;
const TAG_NEXT_FILE: u64 = 2;
const TAG_ADD_FILE: u64 = 3;
const TAG_DELETE_FILE: u64 = 4;
const TAG_LAST_SEQUENCE: u64 = 5;

/// One atomic change to the set of live files.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VersionEdit {
    /// WALs numbered below this hold only flushed data.
    pub log_number: Option<u64>,
    pub next_file_number: Option<u64>,
    /// Highest sequence number ever flushed. Needed because compaction can
    /// drop the newest entries (tombstones), so the tables alone may
    /// under-report it.
    pub last_sequence: Option<u64>,
    /// (SSTable number, file size in bytes).
    pub added: Vec<(u64, u64)>,
    pub deleted: Vec<u64>,
}

impl VersionEdit {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        if let Some(n) = self.log_number {
            put_varint64(&mut buf, TAG_LOG_NUMBER);
            put_varint64(&mut buf, n);
        }
        if let Some(n) = self.next_file_number {
            put_varint64(&mut buf, TAG_NEXT_FILE);
            put_varint64(&mut buf, n);
        }
        if let Some(n) = self.last_sequence {
            put_varint64(&mut buf, TAG_LAST_SEQUENCE);
            put_varint64(&mut buf, n);
        }
        for &(n, size) in &self.added {
            put_varint64(&mut buf, TAG_ADD_FILE);
            put_varint64(&mut buf, n);
            put_varint64(&mut buf, size);
        }
        for &n in &self.deleted {
            put_varint64(&mut buf, TAG_DELETE_FILE);
            put_varint64(&mut buf, n);
        }
        buf
    }

    pub fn decode(data: &[u8]) -> Option<VersionEdit> {
        let mut r = Reader::new(data);
        let mut e = VersionEdit::default();
        while !r.is_empty() {
            match r.varint64()? {
                TAG_LOG_NUMBER => e.log_number = Some(r.varint64()?),
                TAG_NEXT_FILE => e.next_file_number = Some(r.varint64()?),
                TAG_LAST_SEQUENCE => e.last_sequence = Some(r.varint64()?),
                TAG_ADD_FILE => e.added.push((r.varint64()?, r.varint64()?)),
                TAG_DELETE_FILE => e.deleted.push(r.varint64()?),
                _ => return None,
            }
        }
        Some(e)
    }
}

/// The result of replaying a manifest.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ManifestState {
    pub log_number: u64,
    pub next_file_number: u64,
    pub last_sequence: u64,
    /// Live SSTables: number -> file size.
    pub files: BTreeMap<u64, u64>,
}

impl ManifestState {
    pub fn apply(&mut self, edit: &VersionEdit) -> Result<()> {
        if let Some(n) = edit.log_number {
            self.log_number = self.log_number.max(n);
        }
        if let Some(n) = edit.next_file_number {
            self.next_file_number = self.next_file_number.max(n);
        }
        if let Some(n) = edit.last_sequence {
            self.last_sequence = self.last_sequence.max(n);
        }
        for &n in &edit.deleted {
            if self.files.remove(&n).is_none() {
                return Err(Error::Corruption(format!("manifest deletes SSTable {n}, which is not live")));
            }
        }
        for &(n, size) in &edit.added {
            if self.files.insert(n, size).is_some() {
                return Err(Error::Corruption(format!("manifest adds SSTable {n} twice")));
            }
            self.next_file_number = self.next_file_number.max(n + 1);
        }
        Ok(())
    }

    /// A single edit that recreates this state from nothing.
    pub fn snapshot(&self) -> VersionEdit {
        VersionEdit {
            log_number: Some(self.log_number),
            next_file_number: Some(self.next_file_number),
            last_sequence: Some(self.last_sequence),
            added: self.files.iter().map(|(&n, &s)| (n, s)).collect(),
            deleted: Vec::new(),
        }
    }
}

fn frame(edit: &VersionEdit) -> Vec<u8> {
    let payload = edit.encode();
    let mut buf = Vec::with_capacity(HEADER_LEN + payload.len());
    buf.extend_from_slice(&[0; 4]);
    buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    buf.extend_from_slice(&payload);
    let crc = crc32fast::hash(&buf[4..]);
    buf[..4].copy_from_slice(&crc.to_le_bytes());
    buf
}

enum Unframed {
    Edit(VersionEdit, usize),
    /// Incomplete or failed its checksum.
    Invalid,
    /// Checksum fine, payload unreadable: written that way, not torn.
    Malformed,
}

fn unframe(data: &[u8]) -> Unframed {
    if data.len() < HEADER_LEN {
        return Unframed::Invalid;
    }
    let crc = u32::from_le_bytes(data[0..4].try_into().unwrap());
    let len = u32::from_le_bytes(data[4..8].try_into().unwrap()) as usize;
    if len > MAX_EDIT_LEN || data.len() < HEADER_LEN + len {
        return Unframed::Invalid;
    }
    if crc32fast::hash(&data[4..HEADER_LEN + len]) != crc {
        return Unframed::Invalid;
    }
    match VersionEdit::decode(&data[HEADER_LEN..HEADER_LEN + len]) {
        Some(e) => Unframed::Edit(e, HEADER_LEN + len),
        None => Unframed::Malformed,
    }
}

/// What replaying a manifest file found.
#[derive(Debug)]
pub struct ManifestContents {
    pub state: ManifestState,
    pub edits: usize,
    /// Bytes of an invalid tail that was ignored (an edit torn by a crash).
    pub torn_bytes: u64,
}

/// Replay a manifest. A torn final edit is ignored: an edit takes effect
/// only once it is fully synced, and nothing it describes is acted on
/// before then. An invalid edit followed by a valid one is corruption.
pub fn read_manifest(path: &Path) -> Result<ManifestContents> {
    let data = fs::read(path)?;
    let corrupt = |m: String| Error::Corruption(format!("{}: {m}", path.display()));
    let mut state = ManifestState::default();
    let mut off = 0;
    let mut edits = 0;
    while off < data.len() {
        match unframe(&data[off..]) {
            Unframed::Edit(edit, len) => {
                state.apply(&edit).map_err(|e| corrupt(format!("edit {edits}: {e}")))?;
                edits += 1;
                off += len;
            }
            Unframed::Malformed => return Err(corrupt(format!("malformed edit at offset {off}"))),
            Unframed::Invalid => {
                if let Some(later) = (off + 1..data.len()).find(|&o| matches!(unframe(&data[o..]), Unframed::Edit(..))) {
                    return Err(corrupt(format!("invalid edit at offset {off} followed by a valid one at {later}")));
                }
                break;
            }
        }
    }
    // `edits == 0` means the file is empty or holds only a torn first edit:
    // its creation was interrupted. The caller decides what that means.
    Ok(ManifestContents { state, edits, torn_bytes: (data.len() - off) as u64 })
}

/// Appends edits to the active manifest.
pub struct ManifestWriter {
    file: File,
    number: u64,
    poisoned: bool,
}

impl ManifestWriter {
    /// Write a new manifest holding `state` as one snapshot edit, fsync it,
    /// and atomically point `CURRENT` at it.
    pub fn create(dir: &Path, number: u64, state: &ManifestState) -> Result<ManifestWriter> {
        let path = dir.join(filename::manifest_file_name(number));
        let mut file = OpenOptions::new().write(true).create_new(true).open(&path)?;
        file.write_all(&frame(&state.snapshot()))?;
        file.sync_all()?;
        fsutil::sync_dir(dir)?;
        crate::crashpoint::point("manifest.create.before_current");
        set_current(dir, number)?;
        Ok(ManifestWriter { file, number, poisoned: false })
    }

    pub fn number(&self) -> u64 {
        self.number
    }

    /// Append an edit and fsync. When this returns `Ok`, the edit is
    /// committed and survives any crash.
    pub fn append(&mut self, edit: &VersionEdit) -> Result<()> {
        if self.poisoned {
            return Err(Error::Poisoned("an earlier manifest write failed".into()));
        }
        let framed = frame(edit);
        if crate::crashpoint::torn("manifest.append.torn") {
            let _ = self.file.write_all(&framed[..framed.len() / 2]);
            crate::crashpoint::die("manifest.append.torn");
        }
        // As with the WAL, after a failed write or fsync the file's state
        // is unknown, so refuse all further edits.
        let result = self.file.write_all(&framed).and_then(|_| self.file.sync_data());
        if let Err(e) = result {
            self.poisoned = true;
            return Err(Error::Io(e));
        }
        Ok(())
    }
}

/// Atomically make `MANIFEST-number` the current manifest.
pub fn set_current(dir: &Path, number: u64) -> Result<()> {
    let tmp = dir.join(filename::CURRENT_TMP);
    let mut f = File::create(&tmp)?;
    writeln!(f, "{}", filename::manifest_file_name(number))?;
    f.sync_all()?;
    drop(f);
    crate::crashpoint::point("current.before_rename");
    fs::rename(&tmp, dir.join(filename::CURRENT))?;
    fsutil::sync_dir(dir)?;
    Ok(())
}

/// The manifest number named by `CURRENT`, or `None` if there is no
/// `CURRENT` file.
pub fn read_current(dir: &Path) -> Result<Option<u64>> {
    let path: PathBuf = dir.join(filename::CURRENT);
    let text = match fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    match text.strip_suffix('\n').and_then(filename::parse_file_name) {
        Some((n, filename::FileKind::Manifest)) => Ok(Some(n)),
        _ => Err(Error::Corruption(format!("CURRENT holds {text:?}, not a manifest name"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn edit_roundtrip() {
        let e = VersionEdit {
            log_number: Some(7),
            next_file_number: Some(12),
            last_sequence: Some(1 << 50),
            added: vec![(9, 4096), (11, 1 << 40)],
            deleted: vec![3, 5],
        };
        assert_eq!(VersionEdit::decode(&e.encode()), Some(e));
        assert_eq!(VersionEdit::decode(&[]), Some(VersionEdit::default()));
        assert_eq!(VersionEdit::decode(&[99, 1]), None);
        assert_eq!(VersionEdit::decode(&[TAG_ADD_FILE as u8, 1]), None); // truncated
    }

    #[test]
    fn apply_rejects_inconsistent_edits() {
        let mut s = ManifestState::default();
        s.apply(&VersionEdit { added: vec![(4, 10)], ..Default::default() }).unwrap();
        assert_eq!(s.next_file_number, 5);
        assert!(s.apply(&VersionEdit { added: vec![(4, 10)], ..Default::default() }).is_err());
        s.apply(&VersionEdit { deleted: vec![4], ..Default::default() }).unwrap();
        assert!(s.apply(&VersionEdit { deleted: vec![4], ..Default::default() }).is_err());
    }

    #[test]
    fn snapshot_recreates_state() {
        let mut s = ManifestState { log_number: 3, next_file_number: 20, last_sequence: 77, ..Default::default() };
        s.files.insert(5, 100);
        s.files.insert(8, 200);
        let mut t = ManifestState::default();
        t.apply(&s.snapshot()).unwrap();
        assert_eq!(s, t);
    }
}
