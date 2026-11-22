//! Streaming range scans: a k-way merge over every layer of the database.
//!
//! Sources are ordered newest first (active memtable, immutable memtable,
//! SSTables newest to oldest). A min-heap keyed by `(key, source index)`
//! yields the smallest key next, and for equal keys the newest source, whose
//! entry wins. The other sources holding that key are advanced past it.
//! Tombstones win like any other entry but are not returned.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::ops::Bound;
use std::sync::Arc;

use crate::error::Result;
use crate::memtable::{Entry, MemTable};
use crate::sstable::SstIter;

/// True if `key` lies beyond the `end` bound.
pub(crate) fn past_end(key: &[u8], end: Bound<&[u8]>) -> bool {
    match end {
        Bound::Included(e) => key > e,
        Bound::Excluded(e) => key >= e,
        Bound::Unbounded => false,
    }
}

/// One sorted input to the merge.
pub(crate) enum Source {
    /// Entries copied from the active memtable when the iterator was made.
    Copied { rest: std::vec::IntoIter<(Vec<u8>, Entry)>, current: Option<(Vec<u8>, Entry)> },
    /// A frozen memtable. It never changes, so the cursor re-seeks just past
    /// the current key on each step (O(log n)) instead of holding a borrow.
    Frozen { mem: Arc<MemTable>, current: Option<(Vec<u8>, Entry)> },
    Sst(SstIter),
}

impl Source {
    pub(crate) fn copied(entries: Vec<(Vec<u8>, Entry)>) -> Source {
        let mut rest = entries.into_iter();
        let current = rest.next();
        Source::Copied { rest, current }
    }

    pub(crate) fn frozen(mem: Arc<MemTable>, start: Bound<&[u8]>) -> Source {
        let current = mem.range(start, Bound::Unbounded).next().map(|(k, e)| (k.to_vec(), e.clone()));
        Source::Frozen { mem, current }
    }

    fn valid(&self) -> bool {
        match self {
            Source::Copied { current, .. } | Source::Frozen { current, .. } => current.is_some(),
            Source::Sst(it) => it.valid(),
        }
    }

    fn key(&self) -> &[u8] {
        match self {
            Source::Copied { current, .. } | Source::Frozen { current, .. } => &current.as_ref().unwrap().0,
            Source::Sst(it) => it.key(),
        }
    }

    fn entry(&self) -> Result<Entry> {
        match self {
            Source::Copied { current, .. } | Source::Frozen { current, .. } => Ok(current.as_ref().unwrap().1.clone()),
            Source::Sst(it) => it.entry(),
        }
    }

    fn next(&mut self) -> Result<()> {
        match self {
            Source::Copied { rest, current } => *current = rest.next(),
            Source::Frozen { mem, current } => {
                let after = current.as_ref().map(|(k, _)| k.as_slice()).unwrap();
                let next = mem.range(Bound::Excluded(after), Bound::Unbounded).next().map(|(k, e)| (k.to_vec(), e.clone()));
                *current = next;
            }
            Source::Sst(it) => it.next()?,
        }
        Ok(())
    }
}

/// Iterator over live `(key, value)` pairs in ascending key order, from a
/// snapshot taken when it was created. Yields `Err` once on a read or
/// checksum error, then ends.
pub struct DbIter {
    sources: Vec<Source>,
    heap: BinaryHeap<Reverse<(Vec<u8>, usize)>>,
    end: Bound<Vec<u8>>,
    done: bool,
}

impl DbIter {
    /// `sources` must be ordered newest first and already positioned at the
    /// range start.
    pub(crate) fn new(sources: Vec<Source>, end: Bound<Vec<u8>>) -> DbIter {
        let mut it = DbIter { sources, heap: BinaryHeap::new(), end, done: false };
        for i in 0..it.sources.len() {
            it.push_if_in_range(i);
        }
        it
    }

    fn push_if_in_range(&mut self, i: usize) {
        let s = &self.sources[i];
        if s.valid() && !past_end(s.key(), self.end.as_ref().map(Vec::as_slice)) {
            self.heap.push(Reverse((s.key().to_vec(), i)));
        }
    }

    fn advance(&mut self, i: usize) -> Result<()> {
        self.sources[i].next()?;
        self.push_if_in_range(i);
        Ok(())
    }

    fn step(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        while let Some(Reverse((key, i))) = self.heap.pop() {            let entry = self.sources[i].entry()?;
            self.advance(i)?;
            // Older versions of the same key in other sources are shadowed.
            while self.heap.peek().is_some_and(|Reverse((k, _))| *k == key) {
                let Reverse((_, j)) = self.heap.pop().unwrap();                self.advance(j)?;
            }
            if let Some(value) = entry.value {
                return Ok(Some((key, value)));
            }
        }
        Ok(None)
    }
}

impl Iterator for DbIter {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        match self.step() {
            Ok(Some(kv)) => Some(Ok(kv)),
            Ok(None) => {
                self.done = true;
                None
            }
            Err(e) => {
                self.done = true;
                Some(Err(e))
            }
        }
    }
}
