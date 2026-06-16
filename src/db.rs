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
}

impl Value {
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Str(_) => "string",
            Value::List(_) => "list",
            Value::Hash(_) => "hash",
            Value::Set(_) => "set",
            Value::ZSet(_) => "zset",
        }
    }

    fn is_empty_collection(&self) -> bool {
        match self {
            Value::List(l) => l.is_empty(),
            Value::Hash(h) => h.is_empty(),
            Value::Set(s) => s.is_empty(),
            Value::ZSet(z) => z.is_empty(),
            Value::Str(_) => false,
        }
    }
}

pub struct Db {
    data: HashMap<Vec<u8>, Value>,
    expires: HashMap<Vec<u8>, u64>,
}

impl Db {
    pub fn new() -> Self {
        Db {
            data: HashMap::new(),
            expires: HashMap::new(),
        }
    }

    fn check_expiry(&mut self, key: &[u8]) {
        if let Some(&deadline) = self.expires.get(key) {
            if deadline <= now_ms() {
                self.data.remove(key);
                self.expires.remove(key);
            }
        }
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
        self.data.insert(key, value);
    }

    pub fn remove(&mut self, key: &[u8]) -> Option<Value> {
        self.check_expiry(key);
        self.expires.remove(key);
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
        let empty = self.data.get(key).map_or(false, |v| v.is_empty_collection());
        if empty {
            self.data.remove(key);
            self.expires.remove(key);
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
                if self.expires.get(k).map_or(false, |&t| t <= now) {
                    self.data.remove(k);
                    self.expires.remove(k);
                    expired += 1;
                }
            }
            if expired * 4 < total {
                break;
            }
        }
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
        self.data.insert(key, value);
    }
}
