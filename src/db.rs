//! The keyspace, typed values, and key expiration.
//!
//! A value is no longer just bytes — it's one of several Redis types. Commands
//! must check the type and return WRONGTYPE when it doesn't match.
//!
//! Expiry (key -> deadline) is kept in a separate map, with PASSIVE checking on
//! access and an ACTIVE sampling reaper (see `active_expire`).

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// A stored value. Each variant is a distinct Redis type.
pub enum Value {
    Str(Vec<u8>),
    List(VecDeque<Vec<u8>>),
    Hash(HashMap<Vec<u8>, Vec<u8>>),
    Set(HashSet<Vec<u8>>),
    /// Sorted set: member -> score. Kept correct-but-simple (sorted on demand);
    /// a skiplist for O(log n) rank/range is the documented later optimization.
    ZSet(HashMap<Vec<u8>, f64>),
    Stream(Stream),
    /// A geo point `(lon, lat)`. Each geo object is its own key (the geo-first
    /// model): a spatial index over these keys powers GEOSEARCH and, later, the
    /// region changefeed.
    Geo(f64, f64),
    /// A Bloom filter (probabilistic set membership / dedup).
    Bloom(crate::sketch::Bloom),
    /// A Count-Min sketch (probabilistic frequency / "trending").
    Cms(crate::sketch::Cms),
    /// A Top-K heavy-hitters sketch.
    TopK(crate::sketch::TopK),
    /// A t-digest (streaming quantiles / percentiles).
    TDigest(crate::sketch::TDigest),
}

/// A stream entry id: (milliseconds, sequence).
pub type StreamId = (u64, u64);

/// One stream entry: an id plus its field/value pairs.
pub type StreamEntry = (StreamId, Vec<(Vec<u8>, Vec<u8>)>);

/// An append-only stream of entries, ordered by id.
pub struct Stream {
    pub entries: Vec<StreamEntry>,
    pub last_id: StreamId,
}

impl Stream {
    pub fn new() -> Self {
        Stream {
            entries: Vec::new(),
            last_id: (0, 0),
        }
    }
}

impl Value {
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Str(_) => "string",
            Value::List(_) => "list",
            Value::Hash(_) => "hash",
            Value::Set(_) => "set",
            Value::ZSet(_) => "zset",
            Value::Stream(_) => "stream",
            Value::Geo(..) => "geo",
            Value::Bloom(_) => "bloom",
            Value::Cms(_) => "cms",
            Value::TopK(_) => "topk",
            Value::TDigest(_) => "tdigest",
        }
    }

    fn is_empty_collection(&self) -> bool {
        match self {
            Value::List(l) => l.is_empty(),
            Value::Hash(h) => h.is_empty(),
            Value::Set(s) => s.is_empty(),
            Value::ZSet(z) => z.is_empty(),
            Value::Stream(s) => s.entries.is_empty(),
            Value::Str(_)
            | Value::Geo(..)
            | Value::Bloom(_)
            | Value::Cms(_)
            | Value::TopK(_)
            | Value::TDigest(_) => false,
        }
    }
}

pub struct Db {
    data: HashMap<Vec<u8>, Value>,
    expires: HashMap<Vec<u8>, u64>,
    /// Keys removed by expiry (passive or active) since the last drain. The hub
    /// drains this to dirty any WATCHers of an expired key (a watched key that
    /// expires must abort EXEC, just like an explicit modification).
    expired: Vec<Vec<u8>>,
    /// Approximate memory accounting for `maxmemory`. `mem_used` is the running
    /// total; `sizes` is the last-known estimate per key so removals/updates can
    /// be applied as deltas. Estimates are deliberately coarse (see
    /// `estimate_size`) — enough to bound growth, not byte-exact like Redis.
    mem_used: usize,
    sizes: HashMap<Vec<u8>, usize>,
    /// The set of keys holding a geo point — the candidate set for GEOSEARCH.
    /// Maintained on insert and on every removal (via `forget_size`).
    geo_keys: HashSet<Vec<u8>>,
}

impl Db {
    pub fn new() -> Self {
        Db {
            data: HashMap::new(),
            expires: HashMap::new(),
            expired: Vec::new(),
            mem_used: 0,
            sizes: HashMap::new(),
            geo_keys: HashSet::new(),
        }
    }

    fn check_expiry(&mut self, key: &[u8]) {
        if let Some(&deadline) = self.expires.get(key)
            && deadline <= now_ms()
        {
            self.data.remove(key);
            self.expires.remove(key);
            self.forget_size(key);
            self.expired.push(key.to_vec());
        }
    }

    /// Drain the keys removed by expiry since the last call.
    pub fn take_expired(&mut self) -> Vec<Vec<u8>> {
        std::mem::take(&mut self.expired)
    }

    // --- memory accounting (for maxmemory / eviction) ---

    pub fn mem_used(&self) -> usize {
        self.mem_used
    }

    /// Recompute one key's estimated size and fold the delta into the total.
    /// The hub calls this after every write so in-place collection growth
    /// (LPUSH, SADD, …) is accounted for, not just whole-value inserts.
    pub fn resync_size(&mut self, key: &[u8]) {
        let new = self
            .data
            .get(key)
            .map(|v| estimate_size(key.len(), v))
            .unwrap_or(0);
        let old = self.sizes.get(key).copied().unwrap_or(0);
        if new == old {
            return;
        }
        self.mem_used = self.mem_used.saturating_sub(old).saturating_add(new);
        if new == 0 {
            self.sizes.remove(key);
        } else {
            self.sizes.insert(key.to_vec(), new);
        }
    }

    /// Drop per-key tracking when a key is removed (called from every removal
    /// path): memory accounting and the geo-key index.
    fn forget_size(&mut self, key: &[u8]) {
        if let Some(sz) = self.sizes.remove(key) {
            self.mem_used = self.mem_used.saturating_sub(sz);
        }
        self.geo_keys.remove(key);
    }

    /// Candidate keys for GEOSEARCH (those holding a geo point). The caller
    /// re-reads each via `get` (which skips expired keys).
    pub fn geo_keys(&self) -> Vec<Vec<u8>> {
        self.geo_keys.iter().cloned().collect()
    }

    /// Evict one arbitrary key (HashMap order — not true LRU/random; that's a
    /// later refinement). Returns the evicted key, or None if the keyspace is
    /// empty. Used by the hub's `maxmemory` eviction loop.
    pub fn evict_one(&mut self) -> Option<Vec<u8>> {
        let key = self.data.keys().next().cloned()?;
        self.data.remove(&key);
        self.expires.remove(&key);
        self.forget_size(&key);
        Some(key)
    }

    pub fn get(&mut self, key: &[u8]) -> Option<&Value> {
        self.check_expiry(key);
        self.data.get(key)
    }

    pub fn get_mut(&mut self, key: &[u8]) -> Option<&mut Value> {
        self.check_expiry(key);
        self.data.get_mut(key)
    }

    pub fn insert(&mut self, key: Vec<u8>, value: Value) {
        if matches!(value, Value::Geo(..)) {
            self.geo_keys.insert(key.clone());
        } else {
            self.geo_keys.remove(&key); // overwriting a geo key with another type
        }
        self.data.insert(key, value);
    }

    pub fn remove(&mut self, key: &[u8]) -> Option<Value> {
        self.check_expiry(key);
        self.expires.remove(key);
        self.forget_size(key);
        self.data.remove(key)
    }

    pub fn contains(&mut self, key: &[u8]) -> bool {
        self.check_expiry(key);
        self.data.contains_key(key)
    }

    pub fn type_name(&mut self, key: &[u8]) -> Option<&'static str> {
        self.check_expiry(key);
        self.data.get(key).map(|v| v.type_name())
    }

    /// Get a value for in-place mutation, creating it via `f` if absent.
    /// (If the key exists with a different type, the existing value is returned
    /// unchanged — callers must type-check the result.)
    pub fn get_or_insert_with(&mut self, key: &[u8], f: impl FnOnce() -> Value) -> &mut Value {
        self.check_expiry(key);
        self.data.entry(key.to_vec()).or_insert_with(f)
    }

    /// Delete the key if it now holds an empty collection (Redis removes empty
    /// lists/hashes/sets so they don't linger).
    pub fn remove_if_empty(&mut self, key: &[u8]) {
        let empty = self.data.get(key).is_some_and(|v| v.is_empty_collection());
        if empty {
            self.data.remove(key);
            self.expires.remove(key);
            self.forget_size(key);
        }
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
                if self.expires.get(k).is_some_and(|&t| t <= now) {
                    self.data.remove(k);
                    self.expires.remove(k);
                    self.forget_size(k);
                    self.expired.push(k.clone());
                    expired += 1;
                }
            }
            if expired * 4 < total {
                break;
            }
        }
    }

    /// Live (non-expired) keys, for KEYS/DBSIZE. Lazy: doesn't delete the
    /// expired keys it skips (the active reaper handles reclamation).
    pub fn live_keys(&self) -> Vec<Vec<u8>> {
        let now = now_ms();
        self.data
            .keys()
            .filter(|k| self.expires.get(*k).is_none_or(|&d| d > now))
            .cloned()
            .collect()
    }

    /// Count of live (non-expired) keys.
    pub fn dbsize(&self) -> usize {
        let now = now_ms();
        self.data
            .keys()
            .filter(|k| self.expires.get(*k).is_none_or(|&d| d > now))
            .count()
    }

    /// Remove every key (FLUSHDB/FLUSHALL). Cleared keys are pushed to the
    /// expired log so the hub dirties their WATCHers.
    pub fn clear(&mut self) {
        let keys: Vec<Vec<u8>> = self.data.keys().cloned().collect();
        self.expired.extend(keys);
        self.data.clear();
        self.expires.clear();
        self.sizes.clear();
        self.mem_used = 0;
        self.geo_keys.clear();
    }

    // --- persistence support (used by the RDB snapshot module) ---

    pub fn entries(&self) -> std::collections::hash_map::Iter<'_, Vec<u8>, Value> {
        self.data.iter()
    }

    pub fn raw_expire(&self, key: &[u8]) -> Option<u64> {
        self.expires.get(key).copied()
    }

    pub fn insert_with_expire(&mut self, key: Vec<u8>, value: Value, expire: Option<u64>) {
        if let Some(deadline) = expire {
            self.expires.insert(key.clone(), deadline);
        }
        if matches!(value, Value::Geo(..)) {
            self.geo_keys.insert(key.clone());
        }
        self.data.insert(key.clone(), value);
        self.resync_size(&key); // loaded data counts toward used memory
    }
}

/// A coarse estimate of a key+value's memory footprint, in bytes. Not byte-exact
/// (no allocator introspection in zero-deps `std`); a fixed per-key and
/// per-element overhead approximates allocation bookkeeping well enough to bound
/// growth under `maxmemory`.
fn estimate_size(key_len: usize, v: &Value) -> usize {
    const KEY_OVH: usize = 64; // HashMap entry + key/value headers
    const ELEM_OVH: usize = 16; // per collection element
    let val = match v {
        Value::Str(s) => s.len(),
        Value::List(l) => l.iter().map(|e| e.len() + ELEM_OVH).sum(),
        Value::Hash(h) => h.iter().map(|(k, vv)| k.len() + vv.len() + ELEM_OVH).sum(),
        Value::Set(s) => s.iter().map(|e| e.len() + ELEM_OVH).sum(),
        Value::ZSet(z) => z.keys().map(|m| m.len() + 8 + ELEM_OVH).sum(),
        Value::Stream(st) => st
            .entries
            .iter()
            .map(|(_, fields)| {
                fields
                    .iter()
                    .map(|(f, vv)| f.len() + vv.len() + ELEM_OVH)
                    .sum::<usize>()
                    + 24
            })
            .sum(),
        Value::Geo(..) => 16, // two f64
        Value::Bloom(b) => b.bits.len(),
        Value::Cms(c) => c.counters.len() * 4,
        Value::TopK(t) => {
            t.cms.counters.len() * 4 + t.top.iter().map(|(it, _)| it.len() + 16).sum::<usize>()
        }
        Value::TDigest(t) => t.centroids.len() * 16 + 32,
    };
    KEY_OVH + key_len + val
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_accounting_tracks_inserts_and_removals() {
        let mut db = Db::new();
        assert_eq!(db.mem_used(), 0);

        db.insert(b"k".to_vec(), Value::Str(b"hello".to_vec()));
        db.resync_size(b"k"); // the hub does this after each write
        let after_insert = db.mem_used();
        assert!(after_insert > 0);

        // Growing the value increases the estimate.
        db.insert(b"k".to_vec(), Value::Str(vec![b'x'; 1000]));
        db.resync_size(b"k");
        assert!(db.mem_used() > after_insert);

        // Removal returns to zero.
        db.remove(b"k");
        assert_eq!(db.mem_used(), 0);
    }

    #[test]
    fn expiry_and_eviction_free_memory() {
        let mut db = Db::new();
        for i in 0..50 {
            let k = format!("k{i}").into_bytes();
            db.insert(k.clone(), Value::Str(vec![b'v'; 100]));
            db.resync_size(&k);
        }
        let full = db.mem_used();
        assert!(full > 0);

        // Evicting arbitrary keys reduces the running total.
        assert!(db.evict_one().is_some());
        assert!(db.mem_used() < full);

        // Drain everything via eviction -> back to zero, then None.
        while db.evict_one().is_some() {}
        assert_eq!(db.mem_used(), 0);
        assert!(db.evict_one().is_none());
    }

    #[test]
    fn active_expire_decrements_memory() {
        let mut db = Db::new();
        let k = b"k".to_vec();
        db.insert(k.clone(), Value::Str(vec![b'v'; 100]));
        db.resync_size(&k);
        db.set_expire(&k, now_ms().saturating_sub(1)); // already expired
        db.active_expire();
        assert_eq!(db.mem_used(), 0);
        assert_eq!(db.take_expired(), vec![k]);
    }
}
