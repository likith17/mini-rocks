//! In-memory write buffer.
//!
//! A `BTreeMap` from key to the newest entry for that key. Ordered, so range
//! scans and (in phase 2) flushing to a sorted SSTable are a plain in-order
//! walk. See DECISIONS.md for why this instead of a skip list.

use std::collections::btree_map;
use std::collections::BTreeMap;
use std::ops::Bound;

/// The newest write for one key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub seq: u64,
    /// `None` is a tombstone: the key was deleted.
    pub value: Option<Vec<u8>>,
}

/// Result of a point lookup. `Deleted` and `NotFound` differ on purpose: a
/// tombstone must stop the read path from looking in older SSTables.
#[derive(Debug, PartialEq, Eq)]
pub enum Lookup<'a> {
    Value(&'a [u8]),
    Deleted,
    NotFound,
}

/// Rough per-entry cost of the map node and `Entry` beyond key/value bytes.
/// Only used to decide when the memtable is "full".
const ENTRY_OVERHEAD: usize = 48;

#[derive(Debug, Default)]
pub struct MemTable {
    map: BTreeMap<Vec<u8>, Entry>,
    approx_bytes: usize,
}

impl MemTable {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a put (`Some(value)`) or delete (`None`). A write with a lower
    /// sequence number than the stored one is ignored, so applying records in
    /// any order converges to the newest.
    pub fn insert(&mut self, key: Vec<u8>, seq: u64, value: Option<Vec<u8>>) {
        let new_len = value.as_ref().map_or(0, Vec::len);
        match self.map.entry(key) {
            btree_map::Entry::Occupied(mut o) => {
                let e = o.get_mut();
                if seq < e.seq {
                    return;
                }
                let old_len = e.value.as_ref().map_or(0, Vec::len);
                self.approx_bytes = self.approx_bytes - old_len + new_len;
                *e = Entry { seq, value };
            }
            btree_map::Entry::Vacant(v) => {
                self.approx_bytes += v.key().len() + new_len + ENTRY_OVERHEAD;
                v.insert(Entry { seq, value });
            }
        }
    }

    pub fn get(&self, key: &[u8]) -> Lookup<'_> {
        match self.map.get(key) {
            Some(Entry { value: Some(v), .. }) => Lookup::Value(v),
            Some(Entry { value: None, .. }) => Lookup::Deleted,
            None => Lookup::NotFound,
        }
    }

    /// Entries (including tombstones) with keys in the given bounds, in
    /// ascending key order. An inverted or empty range yields nothing.
    pub fn range<'a>(
        &'a self,
        start: Bound<&'a [u8]>,
        end: Bound<&'a [u8]>,
    ) -> impl Iterator<Item = (&'a [u8], &'a Entry)> + 'a {
        // BTreeMap::range panics on an inverted range; check first.
        let inner = (!is_empty_range(start, end))
            .then(|| self.map.range::<[u8], _>((start, end)));
        inner.into_iter().flatten().map(|(k, e)| (k.as_slice(), e))
    }

    /// Number of distinct keys, tombstones included.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Approximate memory used by keys, values and per-entry overhead.
    pub fn approx_bytes(&self) -> usize {
        self.approx_bytes
    }
}

/// True if no key can satisfy both bounds.
pub fn is_empty_range(start: Bound<&[u8]>, end: Bound<&[u8]>) -> bool {
    use Bound::*;
    match (start, end) {
        (Included(s), Included(e)) => s > e,
        (Included(s), Excluded(e)) | (Excluded(s), Included(e)) | (Excluded(s), Excluded(e)) => {
            s >= e
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::Bound::*;

    #[test]
    fn newest_seq_wins_and_tombstones_shadow() {
        let mut m = MemTable::new();
        m.insert(b"a".to_vec(), 1, Some(b"1".to_vec()));
        m.insert(b"a".to_vec(), 3, Some(b"3".to_vec()));
        m.insert(b"a".to_vec(), 2, Some(b"2".to_vec())); // older, ignored
        assert_eq!(m.get(b"a"), Lookup::Value(b"3"));
        m.insert(b"a".to_vec(), 4, None);
        assert_eq!(m.get(b"a"), Lookup::Deleted);
        assert_eq!(m.get(b"b"), Lookup::NotFound);
    }

    #[test]
    fn size_accounting_tracks_overwrites() {
        let mut m = MemTable::new();
        m.insert(b"key".to_vec(), 1, Some(vec![0; 100]));
        let after_first = m.approx_bytes();
        assert_eq!(after_first, 3 + 100 + ENTRY_OVERHEAD);
        m.insert(b"key".to_vec(), 2, Some(vec![0; 10]));
        assert_eq!(m.approx_bytes(), 3 + 10 + ENTRY_OVERHEAD);
        m.insert(b"key".to_vec(), 3, None);
        assert_eq!(m.approx_bytes(), 3 + ENTRY_OVERHEAD);
    }

    #[test]
    fn inverted_ranges_are_empty_not_panics() {
        let mut m = MemTable::new();
        m.insert(b"b".to_vec(), 1, Some(vec![]));
        assert_eq!(m.range(Included(b"c"), Included(b"a")).count(), 0);
        assert_eq!(m.range(Excluded(b"b"), Excluded(b"b")).count(), 0);
        assert_eq!(m.range(Included(b"b"), Excluded(b"b")).count(), 0);
        assert_eq!(m.range(Included(b"b"), Included(b"b")).count(), 1);
        assert_eq!(m.range(Unbounded, Unbounded).count(), 1);
    }
}
