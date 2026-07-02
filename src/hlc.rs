//! Hybrid Logical Clock (HLC) for cross-shard changefeed ordering (std-only).
//!
//! Each change is stamped with a 64-bit HLC laid out as
//!   `physical_ms (48) | logical (8) | node (8)`.
//! The high 48 bits are wall-clock milliseconds; the middle 8 a per-ms logical
//! counter that breaks ties within a millisecond on one node; the low 8 a
//! stable node id so **no two shards ever produce the same HLC**. Packed this
//! way the `u64` sorts as `(physical, logical, node)`, so merging changefeeds
//! from many shards is a sort by HLC and a single-`u64` cursor never collapses
//! a cross-shard tie (which would drop a record when a COUNT split it).
//!
//! The clock is monotonic (never goes backward, even if the wall clock does)
//! and tracks physical time closely, so a global order across shards stays
//! within a bounded staleness of real time.
//!
//! (Pre-LXT3 snapshots used a 16-bit logical field and no node id; a `u64`
//! loaded from one is still a valid monotonic lower bound — `apply_extras` maxes
//! it in — so the first new stamp after an upgrade may jump, never go backward.)

use std::time::{SystemTime, UNIX_EPOCH};

const NODE_BITS: u32 = 8;
const LOG_BITS: u32 = 8;
const LOW_BITS: u32 = NODE_BITS + LOG_BITS; // 16: physical starts here
const NODE_MASK: u64 = (1 << NODE_BITS) - 1;
const LOG_MAX: u64 = (1 << LOG_BITS) - 1;

/// Wall-clock milliseconds since the Unix epoch (0 if the clock is before epoch).
pub fn phys_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Next HLC for a local event, given our last-issued one and this node's id
/// (0..=255). Monotonic: jumps to the wall clock when it has advanced (logical
/// reset, node id in the low bits), otherwise bumps the logical counter —
/// carrying into the physical part if a single millisecond overflows 2^8 events
/// on this node — while preserving the node id so the stamp stays globally
/// unique.
pub fn tick(last: u64, node: u64) -> u64 {
    let node = node & NODE_MASK;
    let phys = phys_now_ms();
    let last_phys = last >> LOW_BITS;
    if phys > last_phys {
        (phys << LOW_BITS) | node
    } else {
        let logical = ((last >> NODE_BITS) & LOG_MAX) + 1;
        if logical <= LOG_MAX {
            (last_phys << LOW_BITS) | (logical << NODE_BITS) | node
        } else {
            // Logical overflow in one ms: carry into physical, reset logical.
            ((last_phys + 1) << LOW_BITS) | node
        }
    }
}

/// A safe "no unseen event below this" floor for a (possibly idle) shard: the
/// later of our last HLC and one below the smallest stamp any new-millisecond
/// event could take (`phys << LOW_BITS`, node id 0). Reporting this as a merge
/// watermark lets an idle shard advance the global order without ever claiming
/// to have delivered an event it might still stamp: a future local event is
/// strictly above this floor, so `h <= watermark` can't skip it. The `-1` is
/// what closes the off-by-one where a first-event-of-ms stamp equalled a peer's
/// idle floor and was filtered out forever.
pub fn floor(last: u64) -> u64 {
    last.max((phys_now_ms() << LOW_BITS).saturating_sub(1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tick_is_strictly_monotonic() {
        for node in [0u64, 7, 255] {
            let mut h = 0;
            let mut prev = 0;
            for _ in 0..10_000 {
                h = tick(h, node);
                assert!(h > prev, "HLC must strictly increase: {h} <= {prev}");
                assert_eq!(h & NODE_MASK, node, "node id must survive every tick");
                prev = h;
            }
        }
    }

    #[test]
    fn tick_tracks_wall_clock() {
        let h = tick(0, 3);
        assert!(h >> LOW_BITS >= 1_700_000_000_000); // after 2023-11
    }

    #[test]
    fn distinct_nodes_never_collide_same_ms() {
        // Two shards stamping their first event in the same ms get different
        // HLCs — a single-u64 merge cursor can't collapse them into a tie.
        let a = tick(0, 1); // node 1, first-of-ms
        let b = tick(0, 2); // node 2, same ms
        assert_ne!(a, b);
        assert_eq!(a >> LOW_BITS, b >> LOW_BITS, "same physical ms");
    }

    #[test]
    fn floor_is_strictly_below_any_future_stamp() {
        // The off-by-one guard: a shard's reported floor must be < every stamp
        // it could next issue, for every node id.
        for node in [0u64, 1, 128, 255] {
            let f = floor(0);
            let next = tick(0, node);
            assert!(
                f < next,
                "floor {f} not below next stamp {next} (node {node})"
            );
        }
        // Never below an already-issued stamp.
        let h = tick(0, 5) + (1_000_000 << LOW_BITS);
        assert!(floor(h) >= h);
    }

    #[test]
    fn logical_overflow_carries_into_physical() {
        // Same-ms events beyond 2^8 on one node carry into the physical field
        // rather than corrupting the node id.
        let mut h = phys_now_ms() << LOW_BITS | 9; // node 9
        let start_phys = h >> LOW_BITS;
        for _ in 0..(LOG_MAX + 5) {
            let next = tick(h, 9);
            assert!(next > h);
            h = next;
        }
        assert!(h >> LOW_BITS > start_phys, "logical overflow should carry");
    }
}
