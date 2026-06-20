//! A tiny std-only leveled logger: timestamped, level-filtered lines to stderr.
//!
//! No third-party crate — just a level filter and a minimal UTC formatter over
//! `SystemTime`. The level is read once from `LOCUS_LOGLEVEL`
//! (error|warn|info|debug, default info). Lines look like:
//!   `2026-06-20T04:32:01.123Z INFO  replication: full sync complete`

use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

pub const ERROR: u8 = 0;
pub const WARN: u8 = 1;
pub const INFO: u8 = 2;
pub const DEBUG: u8 = 3;

static LEVEL: AtomicU8 = AtomicU8::new(INFO);

/// Read `LOCUS_LOGLEVEL` once at startup; unknown / unset defaults to info.
pub fn init() {
    if let Ok(v) = std::env::var("LOCUS_LOGLEVEL") {
        let lvl = match v.trim().to_ascii_lowercase().as_str() {
            "error" => ERROR,
            "warn" | "warning" => WARN,
            "debug" | "trace" => DEBUG,
            _ => INFO,
        };
        LEVEL.store(lvl, Ordering::Relaxed);
    }
}

pub fn error(msg: &str) {
    emit(ERROR, msg);
}
pub fn warn(msg: &str) {
    emit(WARN, msg);
}
pub fn info(msg: &str) {
    emit(INFO, msg);
}
pub fn debug(msg: &str) {
    emit(DEBUG, msg);
}

fn emit(level: u8, msg: &str) {
    if level <= LEVEL.load(Ordering::Relaxed) {
        let name = match level {
            ERROR => "ERROR",
            WARN => "WARN",
            INFO => "INFO",
            _ => "DEBUG",
        };
        eprintln!("{} {name:<5} {msg}", timestamp());
    }
}

/// Compact UTC timestamp `YYYY-MM-DDTHH:MM:SS.mmmZ` — std-only.
fn timestamp() -> String {
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let (secs, millis) = (ms.div_euclid(1000), ms.rem_euclid(1000));
    let days = secs.div_euclid(86_400);
    let tod = secs.rem_euclid(86_400);
    let (hh, mm, ss) = (tod / 3600, (tod % 3600) / 60, tod % 60);
    let (y, mo, d) = civil_from_days(days);
    format!("{y:04}-{mo:02}-{d:02}T{hh:02}:{mm:02}:{ss:02}.{millis:03}Z")
}

/// Civil (year, month, day) from days since the Unix epoch — Howard Hinnant's
/// algorithm, valid across the proleptic Gregorian calendar.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn civil_from_days_known_dates() {
        assert_eq!(civil_from_days(0), (1970, 1, 1)); // the epoch
        assert_eq!(civil_from_days(18_993), (2022, 1, 1)); // 2022-01-01
        assert_eq!(civil_from_days(-1), (1969, 12, 31)); // day before epoch
    }
}
