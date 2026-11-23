//! Sorted String Table: an immutable, sorted, checksummed file of entries.
//!
//! # Layout
//!
//! ```text
//! +--------------+-----+--------------+--------------+------------+-------------+--------+
//! | data block 0 | ... | data block n | filter block | meta block | index block | footer |
//! +--------------+-----+--------------+--------------+------------+-------------+--------+
//! ```
//!
//! Every block is followed by a 4-byte CRC32 of its contents (the *block
//! trailer*). A [`BlockHandle`] (offset, size) points at a block; `size`
//! excludes the trailer.
//!
//! * **Data blocks** (see `block.rs`): entries in ascending key order, about
//!   `block_size` bytes each. Value = user value; tag = `seq << 8 | kind`
//!   with kind 1 = put, 0 = delete (tombstone, empty value).
//! * **Filter block**: a Bloom filter over every key in the file (`bloom.rs`).
//! * **Meta block**: varints `num_entries, num_tombstones, min_seq, max_seq`,
//!   then length-prefixed `smallest_key`, `largest_key`.
//! * **Index block**: one entry per data block, in order. Key = the *last*
//!   key in that data block; value = varint offset and size of the block.
//!   Restart interval 1, so every index entry can be binary-searched.
//! * **Footer**: fixed 64 bytes, little-endian:
//!
//! ```text
//! filter_off u64 | filter_size u64 | index_off u64 | index_size u64 |
//! meta_off u64 | meta_size u64 | version u32 | crc32(previous 52 bytes) u32 |
//! magic u64
//! ```
//!
//! The magic number is at the very end, so a truncated file is recognised
//! immediately.

use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use crate::block::{Block, BlockBuilder, BlockIter, DATA_RESTART_INTERVAL};
use crate::bloom::{Bloom, BloomBuilder};
use crate::coding::{put_length_prefixed, put_varint64, Reader};
use crate::error::{Error, Result};
use crate::filename;
use crate::fsutil;
use crate::memtable::Entry;

pub const FOOTER_LEN: usize = 64;
/// "MiniRock" in ASCII.
pub const MAGIC: u64 = u64::from_be_bytes(*b"MiniRock");
pub const FORMAT_VERSION: u32 = 1;
const TRAILER_LEN: u64 = 4;

const KIND_DELETE: u64 = 0;
const KIND_PUT: u64 = 1;
/// Sequence numbers must fit in 56 bits to share the tag with the kind.
pub const MAX_SEQ: u64 = (1 << 56) - 1;

fn encode_tag(seq: u64, is_put: bool) -> u64 {
    debug_assert!(seq <= MAX_SEQ);
    (seq << 8) | if is_put { KIND_PUT } else { KIND_DELETE }
}

fn decode_tag(tag: u64) -> Result<(u64, bool)> {
    match tag & 0xFF {
        KIND_PUT => Ok((tag >> 8, true)),
        KIND_DELETE => Ok((tag >> 8, false)),
        k => Err(Error::Corruption(format!("unknown entry kind {k} in SSTable"))),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BlockHandle {
    pub offset: u64,
    pub size: u64,
}

impl BlockHandle {
    fn encode_to(&self, buf: &mut Vec<u8>) {
        put_varint64(buf, self.offset);
        put_varint64(buf, self.size);
    }

    fn decode(data: &[u8]) -> Option<BlockHandle> {
        let mut r = Reader::new(data);
        let h = BlockHandle { offset: r.varint64()?, size: r.varint64()? };
        r.is_empty().then_some(h)
    }

    fn end(&self) -> Option<u64> {
        self.offset.checked_add(self.size)?.checked_add(TRAILER_LEN)
    }
}

/// Summary of an SSTable, stored in its meta block.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SstMeta {
    pub num_entries: u64,
    pub num_tombstones: u64,
    pub min_seq: u64,
    pub max_seq: u64,
    pub smallest_key: Vec<u8>,
    pub largest_key: Vec<u8>,
    /// Total file size (not stored; filled in from the file).
    pub file_size: u64,
}

impl SstMeta {
    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        for v in [self.num_entries, self.num_tombstones, self.min_seq, self.max_seq] {
            put_varint64(&mut buf, v);
        }
        put_length_prefixed(&mut buf, &self.smallest_key);
        put_length_prefixed(&mut buf, &self.largest_key);
        buf
    }

    fn decode(data: &[u8]) -> Option<SstMeta> {
        let mut r = Reader::new(data);
        let meta = SstMeta {
            num_entries: r.varint64()?,
            num_tombstones: r.varint64()?,
            min_seq: r.varint64()?,
            max_seq: r.varint64()?,
            smallest_key: r.length_prefixed()?.to_vec(),
            largest_key: r.length_prefixed()?.to_vec(),
            file_size: 0,
        };
        r.is_empty().then_some(meta)
    }
}

// ------------------------------------------------------------------ writer

/// Streams entries (in strictly ascending key order) into a new SSTable.
pub struct SstWriter {
    out: BufWriter<File>,
    offset: u64,
    block_size: usize,
    data_block: BlockBuilder,
    index_block: BlockBuilder,
    bloom: BloomBuilder,
    last_key: Vec<u8>,
    meta: SstMeta,
}

impl SstWriter {
    pub fn create(path: &Path, block_size: usize, bloom_bits_per_key: usize) -> Result<SstWriter> {
        let file = OpenOptions::new().write(true).create(true).truncate(true).open(path)?;
        Ok(SstWriter {
            out: BufWriter::with_capacity(64 * 1024, file),
            offset: 0,
            block_size,
            data_block: BlockBuilder::new(DATA_RESTART_INTERVAL),
            index_block: BlockBuilder::new(1),
            bloom: BloomBuilder::new(bloom_bits_per_key),
            last_key: Vec::new(),
            meta: SstMeta { min_seq: u64::MAX, ..SstMeta::default() },
        })
    }

    /// Add an entry; `value = None` writes a tombstone.
    pub fn add(&mut self, key: &[u8], seq: u64, value: Option<&[u8]>) -> Result<()> {
        if self.meta.num_entries > 0 && key <= self.last_key.as_slice() {
            return Err(Error::InvalidArgument("SSTable keys must be strictly ascending".into()));
        }
        if seq > MAX_SEQ {
            return Err(Error::InvalidArgument(format!("sequence number {seq} exceeds 2^56-1")));
        }
        self.data_block.add(key, encode_tag(seq, value.is_some()), value.unwrap_or(&[]));
        self.bloom.add_key(key);

        let m = &mut self.meta;
        if m.num_entries == 0 {
            m.smallest_key = key.to_vec();
        }
        m.num_entries += 1;
        m.num_tombstones += value.is_none() as u64;
        m.min_seq = m.min_seq.min(seq);
        m.max_seq = m.max_seq.max(seq);
        self.last_key.clear();
        self.last_key.extend_from_slice(key);

        if self.data_block.estimated_size() >= self.block_size {
            self.flush_data_block()?;
        }
        Ok(())
    }

    fn flush_data_block(&mut self) -> Result<()> {
        if self.data_block.is_empty() {
            return Ok(());
        }
        let contents = self.data_block.finish();
        let handle = self.write_block(&contents)?;
        let mut encoded = Vec::new();
        handle.encode_to(&mut encoded);
        self.index_block.add(&self.last_key, 0, &encoded);
        Ok(())
    }

    fn write_block(&mut self, contents: &[u8]) -> Result<BlockHandle> {
        let crc = crc32fast::hash(contents);
        self.out.write_all(contents)?;
        self.out.write_all(&crc.to_le_bytes())?;
        let handle = BlockHandle { offset: self.offset, size: contents.len() as u64 };
        self.offset += contents.len() as u64 + TRAILER_LEN;
        Ok(handle)
    }

    /// Write the remaining blocks and footer, then fsync the file.
    pub fn finish(mut self) -> Result<SstMeta> {
        self.flush_data_block()?;
        if self.meta.num_entries == 0 {
            self.meta.min_seq = 0;
        }
        self.meta.largest_key = self.last_key.clone();

        let filter = self.bloom.finish();
        let filter_h = self.write_block(&filter)?;
        let meta_bytes = self.meta.encode();
        let meta_h = self.write_block(&meta_bytes)?;
        let index = self.index_block.finish();
        let index_h = self.write_block(&index)?;

        let mut footer = Vec::with_capacity(FOOTER_LEN);
        for h in [filter_h, index_h, meta_h] {
            footer.extend_from_slice(&h.offset.to_le_bytes());
            footer.extend_from_slice(&h.size.to_le_bytes());
        }
        footer.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        let crc = crc32fast::hash(&footer);
        footer.extend_from_slice(&crc.to_le_bytes());
        footer.extend_from_slice(&MAGIC.to_le_bytes());
        debug_assert_eq!(footer.len(), FOOTER_LEN);
        self.out.write_all(&footer)?;
        self.offset += FOOTER_LEN as u64;

        let file = self.out.into_inner().map_err(|e| Error::Io(e.into_error()))?;
        file.sync_all()?;
        self.meta.file_size = self.offset;
        Ok(self.meta)
    }
}

/// Write `entries` to `dir/NNNNNN.sst` atomically and durably. The data goes
/// to `NNNNNN.sst.tmp`, which is fsynced and renamed into place, and then the
/// directory is fsynced. A crash leaves either no `.sst` or a complete one.
pub fn build_table<'a>(
    dir: &Path,
    number: u64,
    entries: impl Iterator<Item = (&'a [u8], &'a Entry)>,
    block_size: usize,
    bloom_bits_per_key: usize,
) -> Result<SstMeta> {
    build_table_with(dir, number, block_size, bloom_bits_per_key, |w| {
        for (key, e) in entries {
            w.add(key, e.seq, e.value.as_deref())?;
        }
        Ok(())
    })
}

/// Like [`build_table`], with `fill` adding the entries (for sources that
/// can fail mid-stream, such as a compaction's merge).
pub fn build_table_with(
    dir: &Path,
    number: u64,
    block_size: usize,
    bloom_bits_per_key: usize,
    fill: impl FnOnce(&mut SstWriter) -> Result<()>,
) -> Result<SstMeta> {
    let tmp = dir.join(filename::temp_sst_file_name(number));
    let result = (|| -> Result<SstMeta> {
        let mut w = SstWriter::create(&tmp, block_size, bloom_bits_per_key)?;
        fill(&mut w)?;
        w.finish()
    })();
    match result {
        Ok(meta) => {
            fs::rename(&tmp, dir.join(filename::sst_file_name(number)))?;
            fsutil::sync_dir(dir)?;
            Ok(meta)
        }
        Err(e) => {
            let _ = fs::remove_file(&tmp);
            Err(e)
        }
    }
}

// ------------------------------------------------------------------ reader

/// An open SSTable. The index, filter and meta blocks are held in memory;
/// data blocks are read from disk (and checksummed) on every access.
pub struct SstReader {
    file: File,
    path: PathBuf,
    number: u64,
    /// (last key of block, handle) for each data block, ascending.
    index: Vec<(Vec<u8>, BlockHandle)>,
    bloom: Bloom,
    meta: SstMeta,
    /// Data blocks read from disk by this reader (for statistics).
    blocks_read: AtomicU64,
    /// Set once a compaction has removed this table from the manifest. The
    /// file is deleted when the last reference (an in-flight read or
    /// iterator) is dropped.
    obsolete: AtomicBool,
}

impl Drop for SstReader {
    fn drop(&mut self) {
        if *self.obsolete.get_mut() {
            // If this fails (or the process dies first), the file is an
            // orphan and recovery deletes it.
            let _ = fs::remove_file(&self.path);
        }
    }
}

impl SstReader {
    /// Open and validate an SSTable. Fails with `Corruption` if the footer,
    /// filter, meta or index block is damaged, or the file is truncated.
    pub fn open(path: &Path, number: u64) -> Result<SstReader> {
        let corrupt = |what: &str| Error::Corruption(format!("{}: {what}", path.display()));
        let file = File::open(path)?;
        let file_len = file.metadata()?.len();
        if file_len < FOOTER_LEN as u64 {
            return Err(corrupt("file is shorter than the footer (truncated?)"));
        }
        let data_end = file_len - FOOTER_LEN as u64;
        let mut footer = [0u8; FOOTER_LEN];
        fsutil::read_exact_at(&file, &mut footer, data_end)?;
        let u64_at = |i: usize| u64::from_le_bytes(footer[i..i + 8].try_into().unwrap());
        if u64_at(56) != MAGIC {
            return Err(corrupt("bad magic number (truncated or not an SSTable)"));
        }
        let stored_crc = u32::from_le_bytes(footer[52..56].try_into().unwrap());
        if crc32fast::hash(&footer[..52]) != stored_crc {
            return Err(corrupt("footer checksum mismatch"));
        }
        let version = u32::from_le_bytes(footer[48..52].try_into().unwrap());
        if version != FORMAT_VERSION {
            return Err(corrupt(&format!("unsupported format version {version}")));
        }
        let handle = |i: usize| BlockHandle { offset: u64_at(i * 16), size: u64_at(i * 16 + 8) };
        let (filter_h, index_h, meta_h) = (handle(0), handle(1), handle(2));
        for h in [filter_h, index_h, meta_h] {
            if h.end().is_none_or(|end| end > data_end) {
                return Err(corrupt("footer points outside the file"));
            }
        }

        let bloom = Bloom::decode(read_block(&file, path, filter_h)?)?;
        let mut meta = SstMeta::decode(&read_block(&file, path, meta_h)?).ok_or_else(|| corrupt("malformed meta block"))?;
        meta.file_size = file_len;

        let mut index = Vec::new();
        let mut it = Block::new(read_block(&file, path, index_h)?)?.into_iter();
        it.seek_to_first()?;
        while it.valid() {
            let h = BlockHandle::decode(it.value()).ok_or_else(|| corrupt("malformed index entry"))?;
            // Data blocks all come before the filter block.
            if h.end().is_none_or(|end| end > filter_h.offset) {
                return Err(corrupt("index entry points outside the data region"));
            }
            index.push((it.key().to_vec(), h));
            it.next()?;
        }
        if index.is_empty() != (meta.num_entries == 0) {
            return Err(corrupt("index and meta block disagree about emptiness"));
        }

        Ok(SstReader {
            file,
            path: path.to_path_buf(),
            number,
            index,
            bloom,
            meta,
            blocks_read: AtomicU64::new(0),
            obsolete: AtomicBool::new(false),
        })
    }

    pub fn number(&self) -> u64 {
        self.number
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn meta(&self) -> &SstMeta {
        &self.meta
    }

    pub fn num_data_blocks(&self) -> usize {
        self.index.len()
    }

    /// Bloom filter check: `false` means `key` is definitely not here.
    pub fn may_contain(&self, key: &[u8]) -> bool {
        self.bloom.may_contain(key)
    }

    /// Whether `key` lies within `[smallest_key, largest_key]`.
    pub fn key_in_range(&self, key: &[u8]) -> bool {
        self.meta.num_entries > 0 && self.meta.smallest_key.as_slice() <= key && key <= self.meta.largest_key.as_slice()
    }

    /// Whether any key in the table could fall inside the bounds.
    pub fn overlaps(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> bool {
        let (small, large) = (self.meta.smallest_key.as_slice(), self.meta.largest_key.as_slice());
        let start_ok = match start {
            Bound::Included(s) => large >= s,
            Bound::Excluded(s) => large > s,
            Bound::Unbounded => true,
        };
        let end_ok = match end {
            Bound::Included(e) => small <= e,
            Bound::Excluded(e) => small < e,
            Bound::Unbounded => true,
        };
        self.meta.num_entries > 0 && start_ok && end_ok
    }

    pub fn blocks_read(&self) -> u64 {
        self.blocks_read.load(Ordering::Relaxed)
    }

    /// Delete the file once the last reference to this reader is dropped.
    /// Call only after the table's removal is committed to the manifest.
    pub fn mark_obsolete(&self) {
        self.obsolete.store(true, Ordering::Relaxed);
    }

    /// Point lookup. `Ok(None)`: no entry for `key` in this file.
    /// `Ok(Some(entry))` with `entry.value == None`: a tombstone.
    /// Does not consult the Bloom filter; callers decide that.
    pub fn get(&self, key: &[u8]) -> Result<Option<Entry>> {
        let idx = self.index.partition_point(|(last, _)| last.as_slice() < key);
        if idx == self.index.len() {
            return Ok(None);
        }
        let mut it = self.load_block(idx)?;
        it.seek(key)?;
        if it.valid() && it.key() == key {
            let (seq, is_put) = decode_tag(it.tag())?;
            return Ok(Some(Entry { seq, value: is_put.then(|| it.value().to_vec()) }));
        }
        Ok(None)
    }

    fn load_block(&self, idx: usize) -> Result<BlockIter> {
        self.blocks_read.fetch_add(1, Ordering::Relaxed);
        Ok(Block::new(read_block(&self.file, &self.path, self.index[idx].1)?)?.into_iter())
    }
}

fn read_block(file: &File, path: &Path, h: BlockHandle) -> Result<Vec<u8>> {
    let size = usize::try_from(h.size).map_err(|_| Error::Corruption("block size overflows usize".into()))?;
    let mut buf = vec![0u8; size + TRAILER_LEN as usize];
    fsutil::read_exact_at(file, &mut buf, h.offset)?;
    let stored = u32::from_le_bytes(buf[size..].try_into().unwrap());
    buf.truncate(size);
    if crc32fast::hash(&buf) != stored {
        return Err(Error::Corruption(format!(
            "{}: checksum mismatch in block at offset {}",
            path.display(),
            h.offset
        )));
    }
    Ok(buf)
}

/// Forward iterator over an SSTable, tombstones included.
pub struct SstIter {
    table: Arc<SstReader>,
    next_block: usize,
    block: Option<BlockIter>,
}

impl SstIter {
    /// An unpositioned iterator; call `seek_to_first`, `seek` or `seek_bound`.
    pub fn new(table: Arc<SstReader>) -> SstIter {
        SstIter { table, next_block: 0, block: None }
    }

    pub fn seek_to_first(&mut self) -> Result<()> {
        self.next_block = 0;
        self.load_next_block()?;
        self.skip_exhausted_blocks()
    }

    /// Position at the first entry with key >= `target`.
    pub fn seek(&mut self, target: &[u8]) -> Result<()> {
        let idx = self.table.index.partition_point(|(last, _)| last.as_slice() < target);
        self.block = None;
        self.next_block = idx;
        if idx < self.table.index.len() {
            let mut it = self.table.load_block(idx)?;
            it.seek(target)?;
            self.block = Some(it);
            self.next_block = idx + 1;
        }
        self.skip_exhausted_blocks()
    }

    /// Position at the first entry satisfying a range's start bound.
    pub fn seek_bound(&mut self, start: Bound<&[u8]>) -> Result<()> {
        match start {
            Bound::Unbounded => self.seek_to_first(),
            Bound::Included(k) => self.seek(k),
            Bound::Excluded(k) => {
                self.seek(k)?;
                if self.valid() && self.key() == k {
                    self.next()?;
                }
                Ok(())
            }
        }
    }

    pub fn valid(&self) -> bool {
        self.block.as_ref().is_some_and(BlockIter::valid)
    }

    pub fn key(&self) -> &[u8] {
        self.block.as_ref().expect("iterator not valid").key()
    }

    pub fn entry(&self) -> Result<Entry> {
        let b = self.block.as_ref().expect("iterator not valid");
        let (seq, is_put) = decode_tag(b.tag())?;
        Ok(Entry { seq, value: is_put.then(|| b.value().to_vec()) })
    }

    /// Advance. A fallible cursor (every block read can fail its checksum),
    /// so deliberately not `Iterator`.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<()> {
        if let Some(b) = &mut self.block {
            b.next()?;
        }
        self.skip_exhausted_blocks()
    }

    fn load_next_block(&mut self) -> Result<()> {
        if self.next_block >= self.table.index.len() {
            self.block = None;
            return Ok(());
        }
        let mut it = self.table.load_block(self.next_block)?;
        it.seek_to_first()?;
        self.block = Some(it);
        self.next_block += 1;
        Ok(())
    }

    fn skip_exhausted_blocks(&mut self) -> Result<()> {
        while self.block.as_ref().is_some_and(|b| !b.valid()) {
            self.load_next_block()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::TempDir;

    fn entries(n: usize) -> Vec<(Vec<u8>, Entry)> {
        (0..n)
            .map(|i| {
                let value = if i % 7 == 3 { None } else { Some(format!("value-{i}-{}", "x".repeat(i % 50)).into_bytes()) };
                (format!("key{:06}", i * 2).into_bytes(), Entry { seq: 1000 + i as u64, value })
            })
            .collect()
    }

    fn build(dir: &Path, number: u64, es: &[(Vec<u8>, Entry)], block_size: usize) -> SstReader {
        build_table(dir, number, es.iter().map(|(k, e)| (k.as_slice(), e)), block_size, 10).unwrap();
        SstReader::open(&dir.join(filename::sst_file_name(number)), number).unwrap()
    }

    #[test]
    fn roundtrip_get_iterate_seek() {
        let dir = TempDir::new("sst-roundtrip");
        for (n, block_size) in [(0, 4096), (1, 4096), (500, 64), (500, 4096), (3000, 1024)] {
            let es = entries(n);
            let t = Arc::new(build(dir.path(), n as u64 + block_size as u64, &es, block_size));
            let m = t.meta();
            assert_eq!(m.num_entries, n as u64);
            assert_eq!(m.num_tombstones, es.iter().filter(|(_, e)| e.value.is_none()).count() as u64);
            if n > 0 {
                assert_eq!(m.smallest_key, es[0].0);
                assert_eq!(m.largest_key, es[n - 1].0);
                assert_eq!((m.min_seq, m.max_seq), (1000, 1000 + n as u64 - 1));
            }

            for (k, e) in &es {
                assert_eq!(t.get(k).unwrap().as_ref(), Some(e));
                assert!(t.may_contain(k));
            }
            for i in 0..n {
                let between = format!("key{:06}", i * 2 + 1).into_bytes();
                assert_eq!(t.get(&between).unwrap(), None);
            }
            assert_eq!(t.get(b"").unwrap(), None);
            assert_eq!(t.get(b"zzz").unwrap(), None);

            let mut it = SstIter::new(Arc::clone(&t));
            it.seek_to_first().unwrap();
            let mut seen = Vec::new();
            while it.valid() {
                seen.push((it.key().to_vec(), it.entry().unwrap()));
                it.next().unwrap();
            }
            assert_eq!(seen, es);

            for probe in [0, n / 3, n.saturating_sub(1), n] {
                let target = format!("key{:06}", probe * 2 + 1).into_bytes();
                it.seek(&target).unwrap();
                let expect = es.iter().find(|(k, _)| *k > target).map(|(k, _)| k.clone());
                assert_eq!(it.valid().then(|| it.key().to_vec()), expect);
            }
        }
    }

    #[test]
    fn seek_bound_excluded_skips_exact_match() {
        let dir = TempDir::new("sst-bound");
        let es = entries(100);
        let t = Arc::new(build(dir.path(), 1, &es, 128));
        let mut it = SstIter::new(t);
        it.seek_bound(Bound::Excluded(&es[10].0)).unwrap();
        assert_eq!(it.key(), es[11].0.as_slice());
        it.seek_bound(Bound::Included(&es[10].0)).unwrap();
        assert_eq!(it.key(), es[10].0.as_slice());
    }

    #[test]
    fn rejects_unsorted_input_and_leaves_no_temp_file() {
        let dir = TempDir::new("sst-unsorted");
        let e = Entry { seq: 1, value: Some(vec![]) };
        let bad = [(b"b".as_slice(), &e), (b"a".as_slice(), &e)];
        assert!(matches!(build_table(dir.path(), 1, bad.into_iter(), 4096, 10), Err(Error::InvalidArgument(_))));
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 0);
    }

    /// Every truncation of a valid SSTable must fail to open.
    #[test]
    fn every_truncation_fails_to_open() {
        let dir = TempDir::new("sst-trunc");
        build(dir.path(), 1, &entries(40), 256);
        let path = dir.path().join(filename::sst_file_name(1));
        let full = fs::read(&path).unwrap();
        let cut_path = dir.path().join("cut.sst");
        for cut in 0..full.len() {
            fs::write(&cut_path, &full[..cut]).unwrap();
            match SstReader::open(&cut_path, 2) {
                Err(Error::Corruption(_)) => {}
                Err(e) => panic!("cut {cut}: expected Corruption, got {e}"),
                Ok(_) => panic!("cut {cut}: truncated SSTable opened"),
            }
        }
    }

    /// Flip every byte of a small SSTable in turn. Each flip must be detected
    /// either at open or when reading/iterating; no read may return wrong data.
    #[test]
    fn every_byte_flip_is_detected() {
        let dir = TempDir::new("sst-flip");
        let es = entries(30);
        build(dir.path(), 1, &es, 200);
        let full = fs::read(dir.path().join(filename::sst_file_name(1))).unwrap();
        let path = dir.path().join("flipped.sst");
        let mut detected_at_open = 0;
        for i in 0..full.len() {
            let mut bytes = full.clone();
            bytes[i] ^= 0x10;
            fs::write(&path, &bytes).unwrap();
            let t = match SstReader::open(&path, 2) {
                Err(Error::Corruption(_)) => {
                    detected_at_open += 1;
                    continue;
                }
                Err(e) => panic!("byte {i}: unexpected error {e}"),
                Ok(t) => Arc::new(t),
            };
            // Opened: the damage must be in a data block, and surface as an
            // error on the read that touches it.
            let mut errors = 0;
            for (k, e) in &es {
                match t.get(k) {
                    Ok(got) => assert_eq!(got.as_ref(), Some(e), "byte {i}: wrong data for {k:?}"),
                    Err(Error::Corruption(_)) => errors += 1,
                    Err(other) => panic!("byte {i}: {other}"),
                }
            }
            assert!(errors > 0, "byte {i}: flip went undetected");
        }
        assert!(detected_at_open > 0);
    }
}
