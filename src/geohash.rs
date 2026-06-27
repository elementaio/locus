//! Geohash spatial keys for the GEOSEARCH index (std-only).
//!
//! A point is encoded to a 52-bit Morton/geohash code: longitude and latitude are
//! each quantized to 26 bits over their ranges and bit-interleaved, so points that
//! are close in space are (mostly) close in code — and any square geohash cell is a
//! contiguous range of codes. That lets `GEOSEARCH` scan only the handful of cells
//! covering the query box (a `BTreeMap` range scan) instead of every geo key.
//!
//! The 52-bit cell id is also the natural **shard key** for future spatial
//! clustering (P6): sharding by a cell-id prefix keeps nearby points co-located.
//!
//! Scheme: bit `2i` = latitude bit `i`, bit `2i+1` = longitude bit `i`. It only has
//! to be self-consistent (encode vs. range), not byte-compatible with Redis.

const BITS: u32 = 26; // per dimension -> 52-bit code

/// Interleave the low `bits` of `a` (even positions) and `b` (odd positions).
fn interleave(a: u64, b: u64, bits: u32) -> u64 {
    let mut r = 0u64;
    for i in 0..bits {
        r |= ((a >> i) & 1) << (2 * i);
        r |= ((b >> i) & 1) << (2 * i + 1);
    }
    r
}

/// Quantize a normalized coordinate in [0,1) to `bits` bits.
fn quantize(norm: f64, bits: u32) -> u64 {
    let cells = (1u64 << bits) as f64;
    (norm.clamp(0.0, 0.999_999_9) * cells) as u64
}

fn lat_norm(lat: f64) -> f64 {
    (lat + 90.0) / 180.0
}
fn lon_norm(lon: f64) -> f64 {
    (lon + 180.0) / 360.0
}

/// Encode (lon, lat) to its 52-bit geohash cell id.
pub fn encode(lon: f64, lat: f64) -> u64 {
    let lat_b = quantize(lat_norm(lat), BITS);
    let lon_b = quantize(lon_norm(lon), BITS);
    interleave(lat_b, lon_b, BITS)
}

/// Inclusive `[lo, hi]` 52-bit code ranges whose union covers the lon/lat box.
/// A point inside the box always falls in one of these ranges (no false
/// negatives); a few points just outside may too (the caller refines exactly).
///
/// The box is assumed already clamped to valid, non-wrapping lon/lat by the
/// caller; pole/antimeridian cases fall back to a full scan upstream.
pub fn ranges_for_box(min_lon: f64, min_lat: f64, max_lon: f64, max_lat: f64) -> Vec<(u64, u64)> {
    let lat_span = (max_lat - min_lat).abs().max(1e-9);
    let lon_span = (max_lon - min_lon).abs().max(1e-9);
    // Choose a precision where one cell covers each span, so the box spans at most
    // ~2 cells per axis (≤4 cells total). min() keeps both axes covered.
    let step_lat = (180.0 / lat_span).log2().floor() as i64;
    let step_lon = (360.0 / lon_span).log2().floor() as i64;
    let step = step_lat.min(step_lon).clamp(1, BITS as i64) as u32;

    let cells = (1u64 << step) as f64;
    let cell_idx = |norm: f64| (norm.clamp(0.0, 0.999_999_9) * cells) as u64;
    let la0 = cell_idx(lat_norm(min_lat));
    let la1 = cell_idx(lat_norm(max_lat));
    let lo0 = cell_idx(lon_norm(min_lon));
    let lo1 = cell_idx(lon_norm(max_lon));

    let shift = 2 * (BITS - step); // low bits not covered by this precision
    let mut ranges = Vec::new();
    for la in la0..=la1 {
        for lo in lo0..=lo1 {
            let prefix = interleave(la, lo, step);
            let lo_code = prefix << shift;
            let hi_code = lo_code | ((1u64 << shift) - 1);
            ranges.push((lo_code, hi_code));
        }
    }
    ranges
}

// === cluster cells (cell-in-key sharding) ====================================
//
// A "cell" is a fixed-precision geohash prefix: the top `bits` of the 52-bit code
// (so `bits` is even — `bits/2` per axis). Cluster keys carry their cell as a
// `{hashtag}`, so points in one cell co-locate on one shard, and a bounded
// `GEOSEARCH` queries only the shards owning the cells its box covers.

/// The `bits`-wide cell id for a point (the top `bits` of its 52-bit geohash).
pub fn cell(lon: f64, lat: f64, bits: u32) -> u64 {
    let step = (bits / 2).clamp(1, BITS);
    let cells = (1u64 << step) as f64;
    let la = (lat_norm(lat).clamp(0.0, 0.999_999_9) * cells) as u64;
    let lo = (lon_norm(lon).clamp(0.0, 0.999_999_9) * cells) as u64;
    interleave(la, lo, step)
}

/// Distinct `bits`-wide cell ids covering the lon/lat box — the shards a bounded
/// `GEOSEARCH` must consult. Consistent with `cell`: a point in the box has its
/// cell in this set.
pub fn cells_for_box(
    min_lon: f64,
    min_lat: f64,
    max_lon: f64,
    max_lat: f64,
    bits: u32,
) -> Vec<u64> {
    let step = (bits / 2).clamp(1, BITS);
    let cells = (1u64 << step) as f64;
    let idx = |norm: f64| (norm.clamp(0.0, 0.999_999_9) * cells) as u64;
    let (la0, la1) = (idx(lat_norm(min_lat)), idx(lat_norm(max_lat)));
    let (lo0, lo1) = (idx(lon_norm(min_lon)), idx(lon_norm(max_lon)));
    let mut out = Vec::new();
    for la in la0..=la1 {
        for lo in lo0..=lo1 {
            out.push(interleave(la, lo, step));
        }
    }
    out.sort_unstable();
    out.dedup();
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn point_falls_within_its_box_ranges() {
        // A point inside a small box must be covered by one of the ranges.
        let (lon, lat) = (55.27, 25.20); // Dubai-ish
        let code = encode(lon, lat);
        let d = 0.05; // ~5 km box
        let ranges = ranges_for_box(lon - d, lat - d, lon + d, lat + d);
        assert!(
            ranges.iter().any(|&(lo, hi)| code >= lo && code <= hi),
            "point code {code} not in any range {ranges:?}"
        );
    }

    #[test]
    fn cell_is_covered_by_its_box_and_cover_set_is_small() {
        let bits = 20;
        let (lon, lat) = (55.27, 25.20);
        let c = cell(lon, lat, bits);
        // Nearby points share a coarse cell.
        assert_eq!(c, cell(lon + 0.0001, lat + 0.0001, bits));
        // A small box's cover set is small and includes the point's cell.
        let d = 0.05;
        let cells = cells_for_box(lon - d, lat - d, lon + d, lat + d, bits);
        assert!(cells.contains(&c));
        assert!(
            cells.len() <= 9,
            "small box -> few cells, got {}",
            cells.len()
        );
        // A far point's cell is not in that set.
        assert!(!cells.contains(&cell(-120.0, -40.0, bits)));
    }

    #[test]
    fn far_point_excluded_for_a_tight_box() {
        // A point far away should not fall in a tight box's ranges.
        let (lon, lat) = (0.0, 0.0);
        let d = 0.01;
        let ranges = ranges_for_box(lon - d, lat - d, lon + d, lat + d);
        let far = encode(120.0, -40.0);
        assert!(!ranges.iter().any(|&(lo, hi)| far >= lo && far <= hi));
    }

    #[test]
    fn ranges_are_bounded_and_ordered() {
        // A small box yields only a handful of cells, each lo <= hi.
        let ranges = ranges_for_box(10.0, 10.0, 10.2, 10.2);
        assert!(
            !ranges.is_empty() && ranges.len() <= 16,
            "got {}",
            ranges.len()
        );
        for (lo, hi) in ranges {
            assert!(lo <= hi);
        }
    }

    #[test]
    fn encode_is_deterministic_and_in_range() {
        assert_eq!(encode(55.27, 25.20), encode(55.27, 25.20));
        assert!(encode(180.0, 90.0) < (1u64 << 52));
        assert!(encode(-180.0, -90.0) < (1u64 << 52));
    }
}
