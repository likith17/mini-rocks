//! Bloom filter: one per SSTable, over every key in the file.
//!
//! Encoding: the bit array (a whole number of bytes), then one byte holding
//! the probe count `k`. Probe positions use double hashing on one XXH64:
//! `g_i = (h1 + i*h2) mod m`, where h1 is the low and h2 the high 32 bits
//! (Kirsch & Mitzenmacher, 2006).

use crate::error::{Error, Result};
use crate::hash::xxh64;

pub struct BloomBuilder {
    hashes: Vec<u64>,
    bits_per_key: usize,
}

impl BloomBuilder {
    pub fn new(bits_per_key: usize) -> Self {
        assert!(bits_per_key >= 1);
        BloomBuilder { hashes: Vec::new(), bits_per_key }
    }

    pub fn add_key(&mut self, key: &[u8]) {
        self.hashes.push(xxh64(key, 0));
    }

    pub fn finish(&self) -> Vec<u8> {
        // k = m/n * ln 2 minimises the false-positive rate.
        let k = ((self.bits_per_key as f64) * std::f64::consts::LN_2).round().clamp(1.0, 30.0) as u8;
        let nbytes = (self.hashes.len() * self.bits_per_key).max(64).div_ceil(8);
        let nbits = (nbytes * 8) as u64;
        let mut out = vec![0u8; nbytes];
        for &h in &self.hashes {
            for bit in probes(h, k, nbits) {
                out[bit / 8] |= 1 << (bit % 8);
            }
        }
        out.push(k);
        out
    }
}

pub struct Bloom {
    bits: Vec<u8>,
    k: u8,
}

impl Bloom {
    pub fn decode(mut data: Vec<u8>) -> Result<Bloom> {
        let k = data.pop().ok_or_else(|| Error::Corruption("empty bloom filter block".into()))?;
        if data.is_empty() || !(1..=30).contains(&k) {
            return Err(Error::Corruption(format!("bloom filter has k = {k}, {} bytes", data.len())));
        }
        Ok(Bloom { bits: data, k })
    }

    /// False means the key is definitely absent; true means "maybe".
    pub fn may_contain(&self, key: &[u8]) -> bool {
        let nbits = (self.bits.len() * 8) as u64;
        probes(xxh64(key, 0), self.k, nbits).all(|bit| self.bits[bit / 8] & (1 << (bit % 8)) != 0)
    }
}

fn probes(h: u64, k: u8, nbits: u64) -> impl Iterator<Item = usize> {
    let h1 = h & 0xFFFF_FFFF;
    let h2 = h >> 32;
    (0..k as u64).map(move |i| (h1.wrapping_add(i.wrapping_mul(h2)) % nbits) as usize)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_false_negatives_and_low_false_positive_rate() {
        let n = 10_000;
        let mut b = BloomBuilder::new(10);
        for i in 0..n {
            b.add_key(format!("key{i}").as_bytes());
        }
        let bloom = Bloom::decode(b.finish()).unwrap();
        for i in 0..n {
            assert!(bloom.may_contain(format!("key{i}").as_bytes()));
        }
        let trials = 100_000;
        let fp = (0..trials).filter(|i| bloom.may_contain(format!("absent{i}").as_bytes())).count();
        let rate = fp as f64 / trials as f64;
        // Theory for 10 bits/key, k=7: ~0.82%.
        assert!(rate < 0.015, "false-positive rate {rate}");
    }

    #[test]
    fn empty_filter_matches_nothing() {
        let bloom = Bloom::decode(BloomBuilder::new(10).finish()).unwrap();
        assert!(!bloom.may_contain(b"anything"));
    }

    #[test]
    fn rejects_bad_encodings() {
        assert!(Bloom::decode(vec![]).is_err());
        assert!(Bloom::decode(vec![7]).is_err());
        assert!(Bloom::decode(vec![0, 0, 99]).is_err());
    }
}
