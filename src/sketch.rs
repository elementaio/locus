//! Probabilistic sketches — compact, mergeable summaries (DIFFERENTIATORS #5).
//!
//! First up: a Bloom filter for set-membership / dedup ("have I seen this id?").
//! Zero-deps: hashing uses `std`'s `DefaultHasher` (SipHash13 with fixed keys, so
//! it's deterministic across runs — essential for persistence and replication),
//! and k indices are derived from two hashes via double hashing.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// A classic Bloom filter: a bit array plus `k` hash probes.
pub struct Bloom {
    pub bits: Vec<u8>, // bit array (little-endian within each byte)
    pub k: u8,         // number of hash functions
    pub nbits: u64,    // usable bits (<= bits.len() * 8)
}

impl Bloom {
    /// Size a filter for `capacity` items at the target false-positive `error`
    /// rate, using the standard optimal `m` (bits) and `k` (hashes).
    pub fn with_capacity(capacity: usize, error: f64) -> Bloom {
        let capacity = capacity.max(1) as f64;
        let error = error.clamp(1e-9, 0.5);
        let ln2 = std::f64::consts::LN_2;
        let m = (-(capacity * error.ln()) / (ln2 * ln2)).ceil().max(8.0) as u64;
        let k = ((m as f64 / capacity) * ln2).round().clamp(1.0, 32.0) as u8;
        Bloom {
            bits: vec![0u8; m.div_ceil(8) as usize],
            k,
            nbits: m,
        }
    }

    /// Rebuild from raw parts (RDB load / AOF restore).
    pub fn from_raw(k: u8, nbits: u64, bits: Vec<u8>) -> Bloom {
        Bloom { bits, k, nbits }
    }

    fn two_hashes(item: &[u8]) -> (u64, u64) {
        let mut a = DefaultHasher::new();
        item.hash(&mut a);
        let mut b = DefaultHasher::new();
        0x9E37_79B9_7F4A_7C15u64.hash(&mut b); // distinct seed for the second hash
        item.hash(&mut b);
        (a.finish(), b.finish() | 1) // odd step so double-hashing covers the range
    }

    fn bit(&self, i: u64) -> bool {
        let idx = (i % self.nbits) as usize;
        self.bits[idx / 8] & (1 << (idx % 8)) != 0
    }

    fn set_bit(&mut self, i: u64) -> bool {
        let idx = (i % self.nbits) as usize;
        let (byte, mask) = (idx / 8, 1u8 << (idx % 8));
        let was = self.bits[byte] & mask != 0;
        self.bits[byte] |= mask;
        !was // true if this bit was newly set
    }

    /// Add an item; returns true if it was *probably new* (at least one bit flipped).
    pub fn add(&mut self, item: &[u8]) -> bool {
        let (h1, h2) = Self::two_hashes(item);
        let mut newly = false;
        for i in 0..self.k as u64 {
            let probe = h1.wrapping_add(i.wrapping_mul(h2));
            if self.set_bit(probe) {
                newly = true;
            }
        }
        newly
    }

    /// True if the item is *probably present* (all bits set); false = definitely absent.
    pub fn contains(&self, item: &[u8]) -> bool {
        let (h1, h2) = Self::two_hashes(item);
        (0..self.k as u64).all(|i| self.bit(h1.wrapping_add(i.wrapping_mul(h2))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bloom_membership() {
        let mut b = Bloom::with_capacity(1000, 0.01);
        assert!(b.add(b"alice")); // newly added
        assert!(!b.add(b"alice")); // already present -> not new
        assert!(b.contains(b"alice"));
        assert!(!b.contains(b"bob")); // definitely absent (no false negatives)
        b.add(b"bob");
        assert!(b.contains(b"bob"));
    }

    #[test]
    fn bloom_survives_raw_roundtrip() {
        let mut b = Bloom::with_capacity(100, 0.01);
        b.add(b"x");
        let r = Bloom::from_raw(b.k, b.nbits, b.bits.clone());
        assert!(r.contains(b"x"));
        assert_eq!(r.k, b.k);
        assert_eq!(r.nbits, b.nbits);
    }
}
