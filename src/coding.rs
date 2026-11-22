//! Integer encodings shared by the on-disk formats.

/// Append `v` as an unsigned LEB128 varint (1–10 bytes).
pub fn put_varint64(buf: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        buf.push((v as u8) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}

pub fn put_varint32(buf: &mut Vec<u8>, v: u32) {
    put_varint64(buf, v as u64)
}

/// Decode a varint from the front of `data`: `(value, bytes consumed)`.
/// `None` if the data is truncated or the varint is over-long/overflows.
pub fn get_varint64(data: &[u8]) -> Option<(u64, usize)> {
    let mut result = 0u64;
    for (i, &b) in data.iter().enumerate().take(10) {
        let part = (b & 0x7f) as u64;
        if i == 9 && part > 1 {
            return None; // would overflow 64 bits
        }
        result |= part << (7 * i);
        if b & 0x80 == 0 {
            return Some((result, i + 1));
        }
    }
    None
}

pub fn get_varint32(data: &[u8]) -> Option<(u32, usize)> {
    let (v, n) = get_varint64(data)?;
    Some((u32::try_from(v).ok()?, n))
}

/// Append a varint length followed by the bytes.
pub fn put_length_prefixed(buf: &mut Vec<u8>, bytes: &[u8]) {
    put_varint64(buf, bytes.len() as u64);
    buf.extend_from_slice(bytes);
}

/// A cursor for decoding a byte slice, returning `None` on any overrun.
pub struct Reader<'a> {
    data: &'a [u8],
}

impl<'a> Reader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Reader { data }
    }

    pub fn varint64(&mut self) -> Option<u64> {
        let (v, n) = get_varint64(self.data)?;
        self.data = &self.data[n..];
        Some(v)
    }

    pub fn length_prefixed(&mut self) -> Option<&'a [u8]> {
        let len = usize::try_from(self.varint64()?).ok()?;
        if len > self.data.len() {
            return None;
        }
        let (head, tail) = self.data.split_at(len);
        self.data = tail;
        Some(head)
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_roundtrip_and_edges() {
        for v in [0u64, 1, 127, 128, 300, u32::MAX as u64, u64::MAX - 1, u64::MAX] {
            let mut buf = Vec::new();
            put_varint64(&mut buf, v);
            assert_eq!(get_varint64(&buf), Some((v, buf.len())));
            for cut in 0..buf.len() {
                assert_eq!(get_varint64(&buf[..cut]), None);
            }
        }
        assert_eq!(get_varint64(&[0xFF; 10]), None); // overflow
        assert_eq!(get_varint32(&[0xFF, 0xFF, 0xFF, 0xFF, 0x10]), None); // > u32::MAX
    }
}
