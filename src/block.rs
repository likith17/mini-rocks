//! Sorted key-value blocks, used for SSTable data and index blocks.
//!
//! ```text
//! entry*  restart_offset_0 u32 ... restart_offset_{n-1} u32  n u32
//!
//! entry := shared varint32 | non_shared varint32 | value_len varint32
//!          | key_suffix[non_shared] | tag varint64 | value[value_len]
//! ```
//!
//! Keys are prefix-compressed: each entry stores only the bytes that differ
//! from the previous key. Every `restart_interval` entries the full key is
//! stored again (`shared = 0`) and its offset recorded as a *restart point*,
//! so a seek can binary-search the restart points and then scan at most
//! `restart_interval` entries.

use crate::coding::{get_varint32, get_varint64, put_varint32, put_varint64};
use crate::error::{Error, Result};

pub const DATA_RESTART_INTERVAL: usize = 16;

fn corrupt(what: &str) -> Error {
    Error::Corruption(format!("malformed block: {what}"))
}

pub struct BlockBuilder {
    buf: Vec<u8>,
    restarts: Vec<u32>,
    interval: usize,
    counter: usize,
    last_key: Vec<u8>,
    entries: usize,
}

impl BlockBuilder {
    pub fn new(restart_interval: usize) -> Self {
        assert!(restart_interval >= 1);
        BlockBuilder {
            buf: Vec::new(),
            restarts: vec![0],
            interval: restart_interval,
            counter: 0,
            last_key: Vec::new(),
            entries: 0,
        }
    }

    /// Add an entry. Keys must be added in strictly ascending order (the
    /// caller enforces this).
    pub fn add(&mut self, key: &[u8], tag: u64, value: &[u8]) {
        let shared = if self.counter < self.interval {
            self.last_key.iter().zip(key).take_while(|(a, b)| a == b).count()
        } else {
            self.restarts.push(self.buf.len() as u32);
            self.counter = 0;
            0
        };
        put_varint32(&mut self.buf, shared as u32);
        put_varint32(&mut self.buf, (key.len() - shared) as u32);
        put_varint32(&mut self.buf, value.len() as u32);
        self.buf.extend_from_slice(&key[shared..]);
        put_varint64(&mut self.buf, tag);
        self.buf.extend_from_slice(value);
        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.counter += 1;
        self.entries += 1;
    }

    pub fn estimated_size(&self) -> usize {
        self.buf.len() + 4 * self.restarts.len() + 4
    }

    pub fn is_empty(&self) -> bool {
        self.entries == 0
    }

    /// Return the encoded block and reset the builder.
    pub fn finish(&mut self) -> Vec<u8> {
        let mut out = std::mem::take(&mut self.buf);
        for r in &self.restarts {
            out.extend_from_slice(&r.to_le_bytes());
        }
        out.extend_from_slice(&(self.restarts.len() as u32).to_le_bytes());
        self.restarts.clear();
        self.restarts.push(0);
        self.counter = 0;
        self.last_key.clear();
        self.entries = 0;
        out
    }
}

/// A decoded (validated-trailer) block.
pub struct Block {
    data: Vec<u8>,
    restarts_offset: usize,
    num_restarts: usize,
}

impl Block {
    pub fn new(data: Vec<u8>) -> Result<Block> {
        if data.len() < 4 {
            return Err(corrupt("shorter than its trailer"));
        }
        let num_restarts = u32::from_le_bytes(data[data.len() - 4..].try_into().unwrap()) as usize;
        let restarts_offset = num_restarts
            .checked_mul(4)
            .and_then(|r| data.len().checked_sub(4 + r))
            .ok_or_else(|| corrupt("restart array larger than block"))?;
        if num_restarts == 0 {
            return Err(corrupt("no restart points"));
        }
        let block = Block { data, restarts_offset, num_restarts };
        let mut prev = None;
        for i in 0..num_restarts {
            let r = block.restart_point(i);
            let in_range = r < restarts_offset || (i == 0 && r == 0);
            if !in_range || prev.is_some_and(|p| r <= p) {
                return Err(corrupt("restart point out of order or out of range"));
            }
            prev = Some(r);
        }
        Ok(block)
    }

    fn restart_point(&self, i: usize) -> usize {
        let at = self.restarts_offset + 4 * i;
        u32::from_le_bytes(self.data[at..at + 4].try_into().unwrap()) as usize
    }

    /// Full key stored at restart point `i` (where `shared` must be 0).
    fn restart_key(&self, i: usize) -> Result<&[u8]> {
        let data = &self.data[..self.restarts_offset];
        let mut p = self.restart_point(i);
        let mut next = || -> Result<usize> {
            let (v, n) = get_varint32(&data[p..]).ok_or_else(|| corrupt("bad varint"))?;
            p += n;
            Ok(v as usize)
        };
        let (shared, non_shared, _value_len) = (next()?, next()?, next()?);
        if shared != 0 {
            return Err(corrupt("restart entry has shared prefix"));
        }
        data.get(p..p + non_shared).ok_or_else(|| corrupt("key overruns block"))
    }

    pub fn into_iter(self) -> BlockIter {
        BlockIter {
            block: self,
            key: Vec::new(),
            tag: 0,
            value_start: 0,
            value_end: 0,
            next_offset: 0,
            valid: false,
        }
    }
}

/// Cursor over a block's entries. Starts unpositioned: call `seek_to_first`
/// or `seek`.
pub struct BlockIter {
    block: Block,
    key: Vec<u8>,
    tag: u64,
    value_start: usize,
    value_end: usize,
    next_offset: usize,
    valid: bool,
}

impl BlockIter {
    pub fn valid(&self) -> bool {
        self.valid
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn tag(&self) -> u64 {
        self.tag
    }

    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_start..self.value_end]
    }

    pub fn seek_to_first(&mut self) -> Result<()> {
        self.seek_to_restart(0);
        self.parse_next()
    }

    /// Position at the first entry with key >= `target`, or invalid if none.
    pub fn seek(&mut self, target: &[u8]) -> Result<()> {
        // Find the last restart point whose key is < target.
        let (mut left, mut right) = (0, self.block.num_restarts - 1);
        while left < right {
            let mid = (left + right).div_ceil(2);
            if self.block.restart_key(mid)? < target {
                left = mid;
            } else {
                right = mid - 1;
            }
        }
        self.seek_to_restart(left);
        loop {
            self.parse_next()?;
            if !self.valid || self.key.as_slice() >= target {
                return Ok(());
            }
        }
    }

    pub fn next(&mut self) -> Result<()> {
        debug_assert!(self.valid);
        self.parse_next()
    }

    fn seek_to_restart(&mut self, i: usize) {
        self.key.clear();
        self.next_offset = self.block.restart_point(i);
        self.valid = false;
    }

    fn parse_next(&mut self) -> Result<()> {
        self.valid = false;
        let limit = self.block.restarts_offset;
        let data = &self.block.data[..limit];
        let mut p = self.next_offset;
        if p >= limit {
            return Ok(()); // end of block
        }
        let mut varint = || -> Result<usize> {
            let (v, n) = get_varint32(&data[p..]).ok_or_else(|| corrupt("bad varint"))?;
            p += n;
            Ok(v as usize)
        };
        let (shared, non_shared, value_len) = (varint()?, varint()?, varint()?);
        if shared > self.key.len() {
            return Err(corrupt("shared prefix longer than previous key"));
        }
        let key_end = p.checked_add(non_shared).filter(|&e| e <= limit).ok_or_else(|| corrupt("key overruns block"))?;
        self.key.truncate(shared);
        self.key.extend_from_slice(&data[p..key_end]);
        p = key_end;
        let (tag, n) = get_varint64(&data[p..]).ok_or_else(|| corrupt("bad tag"))?;
        p += n;
        let value_end = p.checked_add(value_len).filter(|&e| e <= limit).ok_or_else(|| corrupt("value overruns block"))?;
        self.tag = tag;
        self.value_start = p;
        self.value_end = value_end;
        self.next_offset = value_end;
        self.valid = true;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build(keys: &[Vec<u8>], interval: usize) -> Block {
        let mut b = BlockBuilder::new(interval);
        for (i, k) in keys.iter().enumerate() {
            b.add(k, i as u64, format!("v{i}").as_bytes());
        }
        Block::new(b.finish()).unwrap()
    }

    fn keys(n: usize) -> Vec<Vec<u8>> {
        // Even numbers only, so odd ones are "between" keys for seeks.
        (0..n).map(|i| format!("user:{:06}", i * 2).into_bytes()).collect()
    }

    #[test]
    fn iterate_and_seek_for_various_intervals() {
        for interval in [1, 2, 16, 1000] {
            for n in [0, 1, 2, 17, 100] {
                let ks = keys(n);
                let mut it = build(&ks, interval).into_iter();
                it.seek_to_first().unwrap();
                for (i, k) in ks.iter().enumerate() {
                    assert!(it.valid());
                    assert_eq!(it.key(), k.as_slice());
                    assert_eq!(it.tag(), i as u64);
                    assert_eq!(it.value(), format!("v{i}").as_bytes());
                    it.next().unwrap();
                }
                assert!(!it.valid());

                let mut it = build(&ks, interval).into_iter();
                for i in 0..n * 2 + 1 {
                    let target = format!("user:{i:06}").into_bytes();
                    it.seek(&target).unwrap();
                    let expect = ks.iter().find(|k| **k >= target);
                    assert_eq!(it.valid().then(|| it.key().to_vec()).as_ref(), expect, "interval {interval} n {n} seek {i}");
                }
                it.seek(b"").unwrap();
                assert_eq!(it.valid(), n > 0);
                it.seek(b"zzz").unwrap();
                assert!(!it.valid());
            }
        }
    }

    #[test]
    fn prefix_compression_shrinks_blocks() {
        let ks = keys(100);
        let raw: usize = ks.iter().map(|k| k.len()).sum();
        let mut b = BlockBuilder::new(16);
        for k in &ks {
            b.add(k, 0, b"");
        }
        let encoded = b.finish().len();
        assert!(encoded < raw, "encoded {encoded} >= raw keys {raw}");
    }

    #[test]
    fn malformed_trailers_are_errors() {
        assert!(Block::new(vec![]).is_err());
        assert!(Block::new(vec![0, 0, 0, 0]).is_err()); // zero restarts
        assert!(Block::new(vec![0xFF, 0xFF, 0xFF, 0x7F]).is_err()); // huge count
        assert!(Block::new(vec![9, 0, 0, 0, 1, 0, 0, 0]).is_err()); // restart past entries
    }

    #[test]
    fn garbage_entries_error_instead_of_panicking() {
        let mut rng = 12345u64;
        for _ in 0..2000 {
            let len = (rng % 40) as usize;
            let mut data: Vec<u8> = (0..len)
                .map(|_| {
                    rng ^= rng << 13;
                    rng ^= rng >> 7;
                    rng ^= rng << 17;
                    rng as u8
                })
                .collect();
            data.extend_from_slice(&0u32.to_le_bytes());
            data.extend_from_slice(&1u32.to_le_bytes());
            if let Ok(block) = Block::new(data) {
                let mut it = block.into_iter();
                let mut steps = 0;
                if it.seek_to_first().is_ok() {
                    while it.valid() && steps < 100 {
                        if it.next().is_err() {
                            break;
                        }
                        steps += 1;
                    }
                }
                let _ = it.seek(b"m");
            }
        }
    }
}
