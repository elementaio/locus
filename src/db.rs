//! The keyspace and key expiration.
//!
//! Expiry is stored in a *separate* map (key -> expire-at unix-ms), exactly like
//! Redis. Two mechanisms keep it honest:
//!   * PASSIVE: on every access we check the key's deadline and delete it if due,
//!     so an expired key is never returned (this guarantees correctness).
//!   * ACTIVE: a periodic sampling pass (see `active_expire`) reclaims memory
//!     from keys that are never touched again.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// Wall-clock milliseconds since the Unix epoch (Redis uses wall time for TTLs).
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

pub struct Db {
    data: HashMap<Vec<u8>, Vec<u8>>,
    expires: HashMap<Vec<u8>, u64>, // key -> expire-at (unix ms)
}

impl Db {
    pub fn new() -> Self {
        Db {
            data: HashMap::new(),
            expires: HashMap::new(),
        }
    }

    /// PASSIVE expiry: if this key has a due deadline, delete it now.
    fn check_expiry(&mut self, key: &[u8]) {
        if let Some(&deadline) = self.expires.get(key) {
            if deadline <= now_ms() {
                self.data.remove(key);
                self.expires.remove(key);
            }
        }
    }

    pub fn get(&mut self, key: &[u8]) -> Option<&Vec<u8>> {
        self.check_expiry(key);
        self.data.get(key)
    }

    pub fn contains(&mut self, key: &[u8]) -> bool {
        self.check_expiry(key);
        self.data.contains_key(key)
    }

    pub fn remove(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        self.check_expiry(key);
        self.expires.remove(key);
        self.data.remove(key)
    }

    /// Insert/replace a value. Does NOT touch TTL — callers decide (plain SET
    /// clears it; INCR/APPEND preserve it), matching Redis semantics.
    pub fn set(&mut self, key: Vec<u8>, val: Vec<u8>) {
        self.data.insert(key, val);
    }

    /// Get a mutable handle to a value, creating an empty one if absent.
    pub fn entry_or_default(&mut self, key: &[u8]) -> &mut Vec<u8> {
        self.check_expiry(key);
        self.data.entry(key.to_vec()).or_default()
    }

    pub fn set_expire(&mut self, key: &[u8], at_ms: u64) {
        if self.data.contains_key(key) {
            self.expires.insert(key.to_vec(), at_ms);
        }
    }

    pub fn clear_expire(&mut self, key: &[u8]) -> bool {
        self.expires.remove(key).is_some()
    }

    pub fn expire_at(&mut self, key: &[u8]) -> Option<u64> {
        self.check_expiry(key);
        self.expires.get(key).copied()
    }

    /// ACTIVE expiry, Redis-style: sample some keys-with-TTL, delete the expired
    /// ones, and repeat while more than ~25% of a sample was expired (a hint that
    /// many remain). Bounded work per call. (True random sampling is a later
    /// refinement; HashMap iteration order is good enough here.)
    pub fn active_expire(&mut self) {
        let now = now_ms();
        loop {
            if self.expires.is_empty() {
                break;
            }
            let sample: Vec<Vec<u8>> = self.expires.keys().take(20).cloned().collect();
            let total = sample.len();
            let mut expired = 0usize;
            for k in &sample {
                if self.expires.get(k).map_or(false, |&t| t <= now) {
                    self.data.remove(k);
                    self.expires.remove(k);
                    expired += 1;
                }
            }
            if expired * 4 < total {
                break; // fewer than ~25% expired — likely done
            }
        }
    }
}
