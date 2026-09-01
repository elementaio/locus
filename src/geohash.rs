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

/// Cells enumerated for one query box, at most. Each cell is one O(log n)
/// `BTreeMap` seek — vastly cheaper than the points an oversized cell sweeps
/// in — so the budget buys precision: 64 cells hold the scanned area to about
/// 1.3x the box, where the old "at most 4 cells" rule scanned 4-9x of it.
const MAX_CELLS: usize = 64;

/// Latitude and longitude cell-index ranges the box covers at a `bits`-wide
/// code prefix. Longitude gets `ceil(bits/2)` bits and latitude `floor(bits/2)`
/// — see `ranges_for_box` for why that asymmetry is the right one.
fn axis_cells(
    bits: u32,
    min_lon: f64,
    min_lat: f64,
    max_lon: f64,
    max_lat: f64,
) -> ((u64, u64), (u64, u64)) {
    let idx = |norm: f64, step: u32| -> u64 {
        let cells = (1u64 << step) as f64;
        (norm.clamp(0.0, 0.999_999_9) * cells) as u64
    };
    let (step_lat, step_lon) = (bits / 2, bits.div_ceil(2));
    (
        (
            idx(lat_norm(min_lat), step_lat),
            idx(lat_norm(max_lat), step_lat),
        ),
        (
            idx(lon_norm(min_lon), step_lon),
            idx(lon_norm(max_lon), step_lon),
        ),
    )
}

/// Inclusive `[lo, hi]` 52-bit code ranges whose union covers the lon/lat box.
/// A point inside the box always falls in one of these ranges (no false
/// negatives); a few points just outside may too (the caller refines exactly).
///
/// The box is assumed already clamped to valid, non-wrapping lon/lat by the
/// caller; pole/antimeridian cases fall back to a full scan upstream.
///
/// **Precision.** A cell here is a shared prefix of the interleaved code, so its
/// width is chosen in *bits of prefix*, not per axis. Because the interleave
/// puts longitude in the odd bit positions, an ODD prefix length gives longitude
/// one bit more precision than latitude — which is exactly the asymmetry the
/// coordinates need, longitude spanning 360 degrees against latitude's 180. The
/// previous version shared one `step` between the axes and every cell came out
/// twice as wide as it was tall.
///
/// The prefix is then taken as fine as `MAX_CELLS` allows, rather than coarse
/// enough that the box spans at most four cells. That earlier rule made a cell
/// 2-3x wider than the query in longitude — 4-9x its area — and on dense data at
/// a large radius one cell swallowed the whole dataset, collapsing the index
/// into a full scan (execution plan, item 4.2).
pub fn ranges_for_box(min_lon: f64, min_lat: f64, max_lon: f64, max_lat: f64) -> Vec<(u64, u64)> {
    // Finest prefix whose cover fits the budget. Coarsening by one bit halves
    // one axis's cell count, so the count is monotone in `bits` and the first
    // fit walking down from the finest is the best one. `bits == 0` covers the
    // globe in one cell, so the loop always settles.
    let (mut bits, mut la, mut lo) = (0u32, (0u64, 0u64), (0u64, 0u64));
    for b in (0..=2 * BITS).rev() {
        let (a, o) = axis_cells(b, min_lon, min_lat, max_lon, max_lat);
        let count = (a.1 - a.0 + 1) as u128 * (o.1 - o.0 + 1) as u128;
        if count <= MAX_CELLS as u128 {
            (bits, la, lo) = (b, a, o);
            break;
        }
    }

    let (step_lat, step_lon) = (bits / 2, bits.div_ceil(2));
    let shift = 2 * BITS - bits; // code bits the prefix leaves free
    let mut ranges = Vec::with_capacity(MAX_CELLS);
    for a in la.0..=la.1 {
        for o in lo.0..=lo.1 {
            let prefix = interleave(a << (BITS - step_lat), o << (BITS - step_lon), BITS);
            let span = if shift >= 64 {
                u64::MAX
            } else {
                (1u64 << shift) - 1
            };
            ranges.push((prefix, prefix | span));
        }
    }
    // Cells adjacent in code order merge into one seek — a box's cover is full
    // of such runs, so this typically halves the range count for free.
    ranges.sort_unstable();
    let mut merged: Vec<(u64, u64)> = Vec::with_capacity(ranges.len());
    for r in ranges {
        match merged.last_mut() {
            Some(last) if r.0 <= last.1.saturating_add(1) => last.1 = last.1.max(r.1),
            _ => merged.push(r),
        }
    }
    merged
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
    fn ranges_are_bounded_ordered_and_disjoint() {
        // Any box yields at most MAX_CELLS ranges, sorted, non-overlapping, and
        // each lo <= hi — the cover is a budget, not an open-ended enumeration.
        for (w, h) in [(0.0002, 0.0002), (0.2, 0.2), (7.0, 3.0), (300.0, 150.0)] {
            let ranges = ranges_for_box(10.0, 10.0, 10.0 + w, 10.0 + h);
            assert!(
                !ranges.is_empty() && ranges.len() <= MAX_CELLS,
                "{w}x{h} -> {} ranges",
                ranges.len()
            );
            for win in ranges.windows(2) {
                assert!(win[0].1 < win[1].0, "ranges overlap or touch: {win:?}");
            }
            for (lo, hi) in ranges {
                assert!(lo <= hi);
            }
        }
    }

    #[test]
    fn cover_is_tight_not_four_huge_cells() {
        // The defect this replaces: one cell 2-3x wider than the query box, so
        // the scan covered 4-9x the query area. With a cell budget the cover is
        // within ~1.5x of the box on both axes.
        for (clon, clat, w, h) in [
            (13.35, 38.25, 0.022, 0.024), // ~1 km at Palermo
            (13.35, 38.25, 0.436, 0.483), // ~20 km — the row that stalled the hub
            (10.0, 50.0, 0.109, 0.121),   // ~5 km
        ] {
            let (min_lon, min_lat) = (clon - w / 2.0, clat - h / 2.0);
            let (max_lon, max_lat) = (clon + w / 2.0, clat + h / 2.0);
            let ranges = ranges_for_box(min_lon, min_lat, max_lon, max_lat);
            // Total code span covered, as a fraction of the whole 52-bit space,
            // against the box's fraction of the globe. Morton codes are area-
            // preserving, so this ratio IS the scanned-area overshoot.
            let covered: u128 = ranges.iter().map(|&(lo, hi)| (hi - lo + 1) as u128).sum();
            let cover_frac = covered as f64 / (1u128 << 52) as f64;
            let box_frac = (w / 360.0) * (h / 180.0);
            let overshoot = cover_frac / box_frac;
            assert!(
                overshoot < 2.5,
                "box {w}x{h} at ({clon},{clat}) scans {overshoot:.1}x its own area"
            );
        }
    }

    #[test]
    fn longitude_gets_the_extra_bit() {
        // Longitude spans 360 degrees to latitude's 180, so at an odd prefix
        // length longitude must carry one bit more — otherwise every cell is
        // twice as wide as it is tall, which is what the old shared `step` did.
        // A square-in-degrees box must therefore cover a square-ish cell grid.
        let (la, lo) = axis_cells(17, 10.0, 10.0, 10.5, 10.5);
        let (n_la, n_lo) = (la.1 - la.0 + 1, lo.1 - lo.0 + 1);
        assert!(
            n_la.abs_diff(n_lo) <= 1,
            "square box covers {n_la} lat x {n_lo} lon cells — axes are skewed"
        );
    }

    #[test]
    fn every_point_in_the_box_is_covered() {
        // The contract: no false negatives. Sweep a grid of boxes and points.
        for &(clon, clat) in &[(13.35, 38.25), (0.0, 0.0), (-73.9, 40.7), (18.0, 69.6)] {
            for &d in &[0.0005_f64, 0.05, 0.4, 3.0] {
                let ranges = ranges_for_box(clon - d, clat - d, clon + d, clat + d);
                for i in 0..=8 {
                    for j in 0..=8 {
                        let lon = clon - d + 2.0 * d * (i as f64 / 8.0);
                        let lat = clat - d + 2.0 * d * (j as f64 / 8.0);
                        let code = encode(lon, lat);
                        assert!(
                            ranges.iter().any(|&(lo, hi)| code >= lo && code <= hi),
                            "({lon},{lat}) in box d={d} at ({clon},{clat}) is not covered"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn encode_is_deterministic_and_in_range() {
        assert_eq!(encode(55.27, 25.20), encode(55.27, 25.20));
        assert!(encode(180.0, 90.0) < (1u64 << 52));
        assert!(encode(-180.0, -90.0) < (1u64 << 52));
    }
}
