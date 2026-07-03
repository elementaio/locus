//! Probabilistic sketches — compact, mergeable summaries (DIFFERENTIATORS #5).
//!
//! First up: a Bloom filter for set-membership / dedup ("have I seen this id?").
//! Zero-deps: hashing uses `std`'s `DefaultHasher` (SipHash13 with fixed keys, so
//! it's deterministic across runs — essential for persistence and replication),
//! and k indices are derived from two hashes via double hashing.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Two independent hashes of an item, for double-hashing (`h1 + i*h2`). Uses
/// `std`'s fixed-key SipHash13, so results are deterministic across runs —
/// essential for persistence and replication. Shared by all sketches.
fn two_hashes(item: &[u8]) -> (u64, u64) {
    let mut a = DefaultHasher::new();
    item.hash(&mut a);
    let mut b = DefaultHasher::new();
    0x9E37_79B9_7F4A_7C15u64.hash(&mut b); // distinct seed for the second hash
    item.hash(&mut b);
    (a.finish(), b.finish() | 1) // odd step so double-hashing covers the range
}

/// A classic Bloom filter: a bit array plus `k` hash probes.
#[derive(Clone)]
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
        let (h1, h2) = two_hashes(item);
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
        let (h1, h2) = two_hashes(item);
        (0..self.k as u64).all(|i| self.bit(h1.wrapping_add(i.wrapping_mul(h2))))
    }
}

/// A Count-Min sketch: `depth` rows × `width` counters, one hash per row. Counts
/// over-estimate (never under-estimate) — the basis for "trending now".
#[derive(Clone)]
pub struct Cms {
    pub width: u32,
    pub depth: u32,
    pub counters: Vec<u32>, // row-major, len = width * depth (saturating)
}

impl Cms {
    pub fn with_dims(width: u32, depth: u32) -> Cms {
        let (width, depth) = (width.max(1), depth.max(1));
        Cms {
            width,
            depth,
            counters: vec![0u32; width as usize * depth as usize],
        }
    }

    /// Defaults: ~0.1% over-estimate error with high confidence (width 2000 × depth 5).
    pub fn default_sketch() -> Cms {
        Self::with_dims(2000, 5)
    }

    fn cell(&self, h1: u64, h2: u64, row: u32) -> usize {
        let probe = h1.wrapping_add((row as u64).wrapping_mul(h2));
        row as usize * self.width as usize + (probe % self.width as u64) as usize
    }

    /// Add `count` to an item's frequency; returns the new estimate (row min).
    pub fn incr(&mut self, item: &[u8], count: u32) -> u64 {
        let (h1, h2) = two_hashes(item);
        let mut est = u32::MAX;
        for row in 0..self.depth {
            let idx = self.cell(h1, h2, row);
            self.counters[idx] = self.counters[idx].saturating_add(count);
            est = est.min(self.counters[idx]);
        }
        est as u64
    }

    /// Estimated frequency of an item (row min).
    pub fn query(&self, item: &[u8]) -> u64 {
        let (h1, h2) = two_hashes(item);
        (0..self.depth)
            .map(|row| self.counters[self.cell(h1, h2, row)])
            .min()
            .unwrap_or(0) as u64
    }

    /// Counters as a little-endian byte blob (for raw restore / persistence).
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.counters.len() * 4);
        for c in &self.counters {
            out.extend_from_slice(&c.to_le_bytes());
        }
        out
    }

    pub fn from_bytes(width: u32, depth: u32, bytes: &[u8]) -> Option<Cms> {
        if bytes.len() != width as usize * depth as usize * 4 {
            return None;
        }
        let counters = bytes
            .chunks_exact(4)
            .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]))
            .collect();
        Some(Cms {
            width,
            depth,
            counters,
        })
    }
}

/// Top-K heavy hitters: a Count-Min for frequencies plus a `k`-slot table of the
/// current leaders. Approximate (rides on the CMS estimates) but compact.
#[derive(Clone)]
pub struct TopK {
    pub k: usize,
    pub cms: Cms,
    pub top: Vec<(Vec<u8>, u64)>, // current heavy hitters (item -> estimate), unordered
}

impl TopK {
    pub fn new(k: usize, width: u32, depth: u32) -> TopK {
        TopK {
            k: k.max(1),
            cms: Cms::with_dims(width, depth),
            top: Vec::new(),
        }
    }

    pub fn default_topk(k: usize) -> TopK {
        Self::new(k, 2000, 5)
    }

    /// Count an occurrence of `item`; returns the item it evicted from the
    /// leaderboard, if any.
    pub fn add(&mut self, item: &[u8]) -> Option<Vec<u8>> {
        let est = self.cms.incr(item, 1);
        if let Some(slot) = self.top.iter_mut().find(|(it, _)| it == item) {
            slot.1 = est;
            return None;
        }
        if self.top.len() < self.k {
            self.top.push((item.to_vec(), est));
            return None;
        }
        let (min_i, min_c) = self
            .top
            .iter()
            .enumerate()
            .map(|(i, (_, c))| (i, *c))
            .min_by_key(|(_, c)| *c)
            .unwrap();
        if est > min_c {
            Some(std::mem::replace(&mut self.top[min_i], (item.to_vec(), est)).0)
        } else {
            None
        }
    }

    /// Current leaders, highest count first.
    pub fn list(&self) -> Vec<Vec<u8>> {
        let mut v = self.top.clone();
        v.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        v.into_iter().map(|(it, _)| it).collect()
    }

    pub fn count(&self, item: &[u8]) -> u64 {
        self.cms.query(item)
    }

    /// Serialize the whole structure to an opaque blob (RDB / AOF restore).
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&(self.k as u32).to_le_bytes());
        out.extend_from_slice(&self.cms.width.to_le_bytes());
        out.extend_from_slice(&self.cms.depth.to_le_bytes());
        out.extend_from_slice(&self.cms.to_bytes());
        out.extend_from_slice(&(self.top.len() as u32).to_le_bytes());
        for (item, count) in &self.top {
            out.extend_from_slice(&(item.len() as u32).to_le_bytes());
            out.extend_from_slice(item);
            out.extend_from_slice(&count.to_le_bytes());
        }
        out
    }

    pub fn from_bytes(b: &[u8]) -> Option<TopK> {
        let mut p = 0;
        let u32_at = |b: &[u8], p: &mut usize| -> Option<u32> {
            let v = b.get(*p..*p + 4)?;
            *p += 4;
            Some(u32::from_le_bytes([v[0], v[1], v[2], v[3]]))
        };
        let k = u32_at(b, &mut p)? as usize;
        let width = u32_at(b, &mut p)?;
        let depth = u32_at(b, &mut p)?;
        let cms_len = width as usize * depth as usize * 4;
        let cms = Cms::from_bytes(width, depth, b.get(p..p + cms_len)?)?;
        p += cms_len;
        let n = u32_at(b, &mut p)? as usize;
        let mut top = Vec::with_capacity(n);
        for _ in 0..n {
            let l = u32_at(b, &mut p)? as usize;
            let item = b.get(p..p + l)?.to_vec();
            p += l;
            let cb = b.get(p..p + 8)?;
            p += 8;
            let count = u64::from_le_bytes(cb.try_into().ok()?);
            top.push((item, count));
        }
        Some(TopK { k, cms, top })
    }
}

/// Merge (mean, weight) points into t-digest centroids, bounding each centroid's
/// weight by the `q(1-q)` scale so the tails (q→0, q→1) keep fine resolution.
/// Input need not be sorted.
fn merge_centroids(mut pts: Vec<(f64, f64)>, compression: f64) -> Vec<(f64, f64)> {
    if pts.len() <= 1 {
        return pts;
    }
    pts.sort_by(|a, b| a.0.total_cmp(&b.0));
    let total: f64 = pts.iter().map(|(_, w)| w).sum();
    let mut merged: Vec<(f64, f64)> = Vec::new();
    let mut w_so_far = 0.0;
    let (mut cm, mut cw) = pts[0];
    for &(m, w) in &pts[1..] {
        let q = (w_so_far + cw + w / 2.0) / total;
        let bound = (4.0 * total * q * (1.0 - q) / compression).max(1.0);
        if cw + w <= bound {
            cm = (cm * cw + m * w) / (cw + w); // weighted mean
            cw += w;
        } else {
            merged.push((cm, cw));
            w_so_far += cw;
            cm = m;
            cw = w;
        }
    }
    merged.push((cm, cw));
    merged
}

fn lerp(x0: f64, y0: f64, x1: f64, y1: f64, x: f64) -> f64 {
    if x1 == x0 {
        y0
    } else {
        y0 + (y1 - y0) * (x - x0) / (x1 - x0)
    }
}

/// A t-digest: streaming quantile/percentile estimation, accurate at the tails
/// (live p99). Centroids plus an unmerged buffer; exact min/max are tracked.
#[derive(Clone)]
pub struct TDigest {
    pub centroids: Vec<(f64, f64)>, // (mean, weight), sorted by mean
    buffer: Vec<f64>,
    pub compression: f64,
    pub total: f64,
    pub min: f64,
    pub max: f64,
}

impl TDigest {
    pub fn new(compression: f64) -> TDigest {
        TDigest {
            centroids: Vec::new(),
            buffer: Vec::new(),
            compression: compression.max(20.0),
            total: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
        }
    }

    pub fn default_td() -> TDigest {
        Self::new(100.0)
    }

    pub fn add(&mut self, x: f64) {
        if !x.is_finite() {
            return;
        }
        self.buffer.push(x);
        self.total += 1.0;
        self.min = self.min.min(x);
        self.max = self.max.max(x);
        if self.buffer.len() as f64 >= self.compression {
            self.flush();
        }
    }

    fn flush(&mut self) {
        if self.buffer.is_empty() {
            return;
        }
        let mut pts = std::mem::take(&mut self.centroids);
        pts.extend(self.buffer.drain(..).map(|x| (x, 1.0)));
        self.centroids = merge_centroids(pts, self.compression);
    }

    /// The compressed centroid view including any buffered samples (read-only).
    fn view(&self) -> Vec<(f64, f64)> {
        if self.buffer.is_empty() {
            return self.centroids.clone();
        }
        let mut pts = self.centroids.clone();
        pts.extend(self.buffer.iter().map(|&x| (x, 1.0)));
        merge_centroids(pts, self.compression)
    }

    /// Estimated value at quantile `q` (0..1). Interpolates between centroid
    /// means, anchored at the exact min/max.
    pub fn quantile(&self, q: f64) -> f64 {
        let cs = self.view();
        if cs.is_empty() {
            return f64::NAN;
        }
        if cs.len() == 1 {
            return cs[0].0;
        }
        let target = q.clamp(0.0, 1.0) * self.total;
        let mut cum = 0.0;
        let centers: Vec<(f64, f64)> = cs
            .iter()
            .map(|&(m, w)| {
                let c = (cum + w / 2.0, m);
                cum += w;
                c
            })
            .collect();
        let last = centers.len() - 1;
        if target <= centers[0].0 {
            return lerp(0.0, self.min, centers[0].0, centers[0].1, target);
        }
        if target >= centers[last].0 {
            return lerp(
                centers[last].0,
                centers[last].1,
                self.total,
                self.max,
                target,
            );
        }
        for w in centers.windows(2) {
            if target >= w[0].0 && target <= w[1].0 {
                return lerp(w[0].0, w[0].1, w[1].0, w[1].1, target);
            }
        }
        centers[last].1
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let cs = self.view();
        let mut out = Vec::new();
        for f in [self.compression, self.total, self.min, self.max] {
            out.extend_from_slice(&f.to_le_bytes());
        }
        out.extend_from_slice(&(cs.len() as u32).to_le_bytes());
        for (m, w) in cs {
            out.extend_from_slice(&m.to_le_bytes());
            out.extend_from_slice(&w.to_le_bytes());
        }
        out
    }

    pub fn from_bytes(b: &[u8]) -> Option<TDigest> {
        let mut p = 0;
        let f64_at = |b: &[u8], p: &mut usize| -> Option<f64> {
            let v = b.get(*p..*p + 8)?;
            *p += 8;
            Some(f64::from_le_bytes(v.try_into().ok()?))
        };
        let compression = f64_at(b, &mut p)?;
        let total = f64_at(b, &mut p)?;
        let min = f64_at(b, &mut p)?;
        let max = f64_at(b, &mut p)?;
        let n = {
            let v = b.get(p..p + 4)?;
            p += 4;
            u32::from_le_bytes([v[0], v[1], v[2], v[3]]) as usize
        };
        let mut centroids = Vec::with_capacity(n);
        for _ in 0..n {
            let m = f64_at(b, &mut p)?;
            let w = f64_at(b, &mut p)?;
            centroids.push((m, w));
        }
        Some(TDigest {
            centroids,
            buffer: Vec::new(),
            compression,
            total,
            min,
            max,
        })
    }
}

/// A HyperLogLog (approximate distinct-count). Dense layout: 2^14 = 16384
/// one-byte registers (16 KB per key, ~0.81% standard error) — one byte per
/// register instead of Redis's packed 6 bits trades a little RAM for much
/// simpler (and std-only) code. Registers only ever grow, so merge = max,
/// which is what makes PFMERGE and multi-key PFCOUNT possible.
#[derive(Clone)]
pub struct Hll {
    pub regs: Vec<u8>, // REG_COUNT registers
}

const HLL_P: u32 = 14;
pub const HLL_REGS: usize = 1 << HLL_P; // 16384

impl Hll {
    pub fn new() -> Hll {
        Hll {
            regs: vec![0u8; HLL_REGS],
        }
    }

    pub fn from_raw(regs: Vec<u8>) -> Option<Hll> {
        if regs.len() != HLL_REGS {
            return None;
        }
        Some(Hll { regs })
    }

    /// Add an item; true if any register grew (the estimate may have changed).
    pub fn add(&mut self, item: &[u8]) -> bool {
        let (h, _) = two_hashes(item);
        let idx = (h >> (64 - HLL_P)) as usize; // top P bits pick the register
        // Rank = position of the first 1-bit in the remaining 50 bits (1-based).
        // The `| 1` sentinel bounds it at 64-P+1 when the tail is all zeros.
        let tail = (h << HLL_P) | 1;
        let rank = (tail.leading_zeros() + 1) as u8;
        if rank > self.regs[idx] {
            self.regs[idx] = rank;
            return true;
        }
        false
    }

    /// The cardinality estimate (raw HLL + linear counting for the small range;
    /// with a 64-bit hash no large-range correction is needed).
    pub fn count(&self) -> u64 {
        estimate(&self.regs)
    }

    /// Merge another HLL in (register-wise max); true if anything grew.
    pub fn merge(&mut self, other: &Hll) -> bool {
        let mut grew = false;
        for (a, b) in self.regs.iter_mut().zip(other.regs.iter()) {
            if *b > *a {
                *a = *b;
                grew = true;
            }
        }
        grew
    }
}

/// The standard HLL estimator over a register array (shared by single-key
/// counts and the multi-key union, which max-merges into a scratch array).
pub fn estimate(regs: &[u8]) -> u64 {
    let m = regs.len() as f64;
    let alpha = 0.7213 / (1.0 + 1.079 / m);
    let mut sum = 0.0;
    let mut zeros = 0u64;
    for &r in regs {
        sum += (2.0f64).powi(-i32::from(r)); // exact for every rank a u8 can hold
        if r == 0 {
            zeros += 1;
        }
    }
    let raw = alpha * m * m / sum;
    if raw <= 2.5 * m && zeros > 0 {
        // Small-range correction: linear counting is far more accurate here.
        (m * (m / zeros as f64).ln()).round() as u64
    } else {
        raw.round() as u64
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

    #[test]
    fn cms_counts_and_never_underestimates() {
        let mut c = Cms::default_sketch();
        assert_eq!(c.incr(b"a", 3), 3);
        assert_eq!(c.incr(b"a", 2), 5);
        assert_eq!(c.incr(b"b", 1), 1);
        assert!(c.query(b"a") >= 5); // over-estimate allowed, never under
        assert!(c.query(b"b") >= 1);
        assert_eq!(c.query(b"never-added"), 0); // (overwhelmingly likely with these dims)
    }

    #[test]
    fn cms_survives_byte_roundtrip() {
        let mut c = Cms::with_dims(64, 4);
        c.incr(b"x", 7);
        let r = Cms::from_bytes(c.width, c.depth, &c.to_bytes()).unwrap();
        assert_eq!(r.query(b"x"), 7);
    }

    #[test]
    fn tdigest_estimates_quantiles() {
        let mut t = TDigest::default_td();
        for i in 1..=1000 {
            t.add(i as f64);
        }
        // exact extremes; medians/percentiles within a small tolerance
        assert_eq!(t.quantile(0.0), 1.0);
        assert_eq!(t.quantile(1.0), 1000.0);
        assert!(
            (t.quantile(0.5) - 500.0).abs() < 15.0,
            "p50={}",
            t.quantile(0.5)
        );
        assert!(
            (t.quantile(0.99) - 990.0).abs() < 15.0,
            "p99={}",
            t.quantile(0.99)
        );
        // round-trip through the blob
        let r = TDigest::from_bytes(&t.to_bytes()).unwrap();
        assert!((r.quantile(0.5) - 500.0).abs() < 15.0);
        assert_eq!(r.quantile(1.0), 1000.0);
    }

    #[test]
    fn topk_tracks_heavy_hitters() {
        let mut t = TopK::default_topk(2);
        for _ in 0..5 {
            t.add(b"a");
        }
        for _ in 0..3 {
            t.add(b"b");
        }
        t.add(b"c"); // count 1 — shouldn't displace a(5)/b(3) in a k=2 board
        let list = t.list();
        assert_eq!(list, vec![b"a".to_vec(), b"b".to_vec()]);
        assert!(t.count(b"a") >= 5);
        // round-trip through the opaque blob
        let r = TopK::from_bytes(&t.to_bytes()).unwrap();
        assert_eq!(r.list(), list);
        assert_eq!(r.k, 2);
    }

    #[test]
    fn hll_small_range_is_near_exact() {
        // Linear counting keeps small cardinalities essentially exact.
        let mut h = Hll::new();
        for i in 0..100 {
            h.add(format!("item-{i}").as_bytes());
        }
        let e = h.count();
        assert!((95..=105).contains(&e), "estimate {e} for 100 distinct");
        // Re-adding the same items must not move the estimate.
        for i in 0..100 {
            assert!(!h.add(format!("item-{i}").as_bytes()));
        }
        assert_eq!(h.count(), e);
    }

    #[test]
    fn hll_large_range_within_error_bounds() {
        // 100k distinct at m=16384 → σ ≈ 0.81%; assert within 3σ (~2.5%).
        let mut h = Hll::new();
        for i in 0..100_000 {
            h.add(format!("user:{i}").as_bytes());
        }
        let e = h.count() as f64;
        assert!(
            (e - 100_000.0).abs() / 100_000.0 < 0.025,
            "estimate {e} off by more than 2.5%"
        );
    }

    #[test]
    fn hll_merge_is_union() {
        let (mut a, mut b) = (Hll::new(), Hll::new());
        for i in 0..5_000 {
            a.add(format!("a-{i}").as_bytes());
        }
        for i in 0..5_000 {
            b.add(format!("b-{i}").as_bytes());
        }
        // Overlap: half of b's items also in a.
        for i in 0..2_500 {
            a.add(format!("b-{i}").as_bytes());
        }
        assert!(a.merge(&b));
        let e = a.count() as f64;
        assert!(
            (e - 10_000.0).abs() / 10_000.0 < 0.03,
            "union estimate {e}, want ≈10000"
        );
        // Merging an already-covered HLL changes nothing.
        assert!(!a.merge(&b));
        // Raw round-trip (persistence path).
        let r = Hll::from_raw(a.regs.clone()).unwrap();
        assert_eq!(r.count(), a.count());
        assert!(Hll::from_raw(vec![0u8; 3]).is_none());
    }
}
