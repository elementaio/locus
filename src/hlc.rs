//! Hybrid Logical Clock (HLC) for cross-shard changefeed ordering (std-only).
//!
//! Each change is stamped with a 64-bit HLC: the high 48 bits are wall-clock
//! milliseconds, the low 16 a logical counter that breaks ties within a
//! millisecond. Packed this way the `u64` sorts exactly as `(physical, logical)`,
//! so merging changefeeds from many shards is just a sort by HLC. The clock is
//! monotonic (never goes backward, even if the wall clock does) and tracks
//! physical time closely, so a global order across shards stays within a bounded
//! staleness of real time.

use std::time::{SystemTime, UNIX_EPOCH};

const LOG_BITS: u32 = 16;

/// Wall-clock milliseconds since the Unix epoch (0 if the clock is before epoch).
pub fn phys_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Next HLC for a local event given our last-issued one. Monotonic: jumps to the
/// wall clock when it has advanced, otherwise bumps the logical counter (which
/// carries into the physical part if a single millisecond ever overflows 2^16).
pub fn tick(last: u64) -> u64 {
    let phys = phys_now_ms();
    if phys > last >> LOG_BITS {
        phys << LOG_BITS
    } else {
        last + 1
    }
}

/// A safe "no unseen event below this" floor for a (possibly idle) shard: the
/// later of our last HLC and the current wall clock. Reporting this as a merge
/// watermark lets an idle shard advance the global order instead of stalling it
/// (a new event can only be stamped at or after the current wall clock).
pub fn floor(last: u64) -> u64 {
    last.max(phys_now_ms() << LOG_BITS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tick_is_strictly_monotonic() {
        let mut h = 0;
        let mut prev = 0;
        for _ in 0..10_000 {
            h = tick(h);
            assert!(h > prev, "HLC must strictly increase: {h} <= {prev}");
            prev = h;
        }
    }

    #[test]
    fn tick_tracks_wall_clock() {
        let h = tick(0);
        // High bits are ~now in ms; sanity-check it's a plausible recent epoch ms.
        assert!(h >> LOG_BITS >= 1_700_000_000_000); // after 2023-11
    }

    #[test]
    fn floor_never_below_last() {
        let h = tick(0) + 1_000_000; // a clock far in the logical future
        assert!(floor(h) >= h);
        assert!(floor(0) > 0); // idle clock still advances to wall time
    }
}
