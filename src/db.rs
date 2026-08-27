//! The keyspace, typed values, and key expiration.
//!
//! A value is no longer just bytes — it's one of several Redis types. Commands
//! must check the type and return WRONGTYPE when it doesn't match.
//!
//! Expiry (key -> deadline) is kept in a separate map, with PASSIVE checking on
//! access and an ACTIVE sampling reaper (see `active_expire`).

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::geohash;

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Inline attributes on a geo object (`field -> value`), insertion-ordered and
/// typically tiny, so a `Vec` of pairs beats a map here.
pub type GeoAttrs = Vec<(Vec<u8>, Vec<u8>)>;

/// Total order over scores for the sorted-set index. Scores are finite-or-±inf
/// (never NaN — `parse_score` rejects it), so `total_cmp` is a valid total order.
#[derive(Clone, Copy, PartialEq)]
struct Score(f64);
impl Eq for Score {}
impl Ord for Score {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}
impl PartialOrd for Score {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A sorted set: `member -> score` for O(1) lookup, paired with an ordered index
/// of `(score, member)` for range/rank without re-sorting on every read. Mutate
/// only through `insert`/`remove` so the two stay in lock-step.
#[derive(Default, Clone)]
pub struct ZSet {
    map: HashMap<Vec<u8>, f64>,
    sorted: BTreeSet<(Score, Vec<u8>)>,
}

impl ZSet {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn len(&self) -> usize {
        self.map.len()
    }
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
    pub fn get(&self, member: &[u8]) -> Option<&f64> {
        self.map.get(member)
    }
    /// Set a member's score, returning its previous score. Keeps the ordered index
    /// consistent (removes the old (score, member) entry first).
    pub fn insert(&mut self, member: Vec<u8>, score: f64) -> Option<f64> {
        let old = self.map.insert(member.clone(), score);
        if let Some(o) = old {
            self.sorted.remove(&(Score(o), member.clone()));
        }
        self.sorted.insert((Score(score), member));
        old
    }
    pub fn remove(&mut self, member: &[u8]) -> Option<f64> {
        let old = self.map.remove(member)?;
        self.sorted.remove(&(Score(old), member.to_vec()));
        Some(old)
    }
    /// Unordered (member, score) pairs — for serialization and set algebra.
    pub fn iter(&self) -> impl Iterator<Item = (&Vec<u8>, &f64)> {
        self.map.iter()
    }
    /// 0-based ascending rank of a member, in O(rank) via the ordered index.
    pub fn rank(&self, member: &[u8]) -> Option<usize> {
        let score = *self.map.get(member)?;
        Some(self.sorted.range(..(Score(score), member.to_vec())).count())
    }
    /// Range by rank without materializing everything first. Collects only the
    /// requested range. For rev=false, iterates ascending; for rev=true, descending.
    pub fn range_by_rank(&self, start: i64, stop: i64, rev: bool) -> Vec<(Vec<u8>, f64)> {
        let len = self.sorted.len() as i64;
        let norm_start = if start < 0 {
            (len + start).max(0)
        } else {
            start.min(len)
        };
        let mut norm_stop = if stop < 0 {
            (len + stop).max(-1)
        } else {
            stop.min(len - 1)
        };
        if norm_stop >= len {
            norm_stop = len - 1;
        }

        if norm_start > norm_stop || norm_start >= len {
            return vec![];
        }

        let count = (norm_stop - norm_start + 1) as usize;
        if rev {
            self.sorted
                .iter()
                .rev()
                .skip(norm_start as usize)
                .take(count)
                .map(|(s, m)| (m.clone(), s.0))
                .collect()
        } else {
            self.sorted
                .iter()
                .skip(norm_start as usize)
                .take(count)
                .map(|(s, m)| (m.clone(), s.0))
                .collect()
        }
    }
    /// The first `n` members from whichever end the pop paths want. Walks only
    /// `n` entries of the ordered index, never the whole set.
    pub fn first_by_rank(&self, n: usize, from_high: bool) -> Vec<(Vec<u8>, f64)> {
        let take = |it: &mut dyn Iterator<Item = &(Score, Vec<u8>)>| -> Vec<(Vec<u8>, f64)> {
            it.take(n).map(|(s, m)| (m.clone(), s.0)).collect()
        };
        if from_high {
            take(&mut self.sorted.iter().rev())
        } else {
            take(&mut self.sorted.iter())
        }
    }
    /// Range by score without materializing everything first. Collects only
    /// members whose score falls within (lo, hi). Direction is always ascending by score.
    pub fn range_by_score(
        &self,
        lo: f64,
        lo_ex: bool,
        hi: f64,
        hi_ex: bool,
    ) -> Vec<(Vec<u8>, f64)> {
        self.sorted
            .iter()
            .skip_while(|(s, _)| if lo_ex { s.0 <= lo } else { s.0 < lo })
            .take_while(|(s, _)| if hi_ex { s.0 < hi } else { s.0 <= hi })
            .map(|(s, m)| (m.clone(), s.0))
            .collect()
    }
}

impl FromIterator<(Vec<u8>, f64)> for ZSet {
    fn from_iter<I: IntoIterator<Item = (Vec<u8>, f64)>>(iter: I) -> Self {
        let mut z = ZSet::new();
        for (m, s) in iter {
            z.insert(m, s);
        }
        z
    }
}

/// A stored value. Each variant is a distinct Redis type.
#[derive(Clone)]
pub enum Value {
    Str(Vec<u8>),
    List(VecDeque<Vec<u8>>),
    Hash(HashMap<Vec<u8>, Vec<u8>>),
    Set(HashSet<Vec<u8>>),
    /// Sorted set — a `member -> score` map plus an ordered `(score, member)`
    /// index for range/rank without re-sorting (see `ZSet`).
    ZSet(ZSet),
    Stream(Stream),
    /// A geo point `(lon, lat)` plus optional inline attributes (`field -> value`,
    /// insertion-ordered). Each geo object is its own key (the geo-first model): a
    /// spatial index over these keys powers GEOSEARCH, the attributes power
    /// combined `WHERE field=value` filters, and the region changefeed tracks them.
    Geo(f64, f64, GeoAttrs),
    /// A Bloom filter (probabilistic set membership / dedup).
    Bloom(crate::sketch::Bloom),
    /// A Count-Min sketch (probabilistic frequency / "trending").
    Cms(crate::sketch::Cms),
    /// A Top-K heavy-hitters sketch.
    TopK(crate::sketch::TopK),
    /// A t-digest (streaming quantiles / percentiles).
    TDigest(crate::sketch::TDigest),
    /// A HyperLogLog (approximate distinct-count; PFADD/PFCOUNT/PFMERGE).
    Hll(crate::sketch::Hll),
    /// A TIERED value: the real bytes live in the on-disk value-log (see
    /// `tier`); RAM keeps only this stub — key identity, TTL (in `expires`,
    /// as usual), the disk address, and the original type tag so TYPE answers
    /// without a disk read. Any access that needs the value transparently
    /// thaws it back into RAM.
    Tiered {
        seg: u32,
        off: u64,
        len: u32,
        vtag: u8,
    },
}

/// A stream entry id: (milliseconds, sequence).
pub type StreamId = (u64, u64);

/// One stream entry: an id plus its field/value pairs.
pub type StreamEntry = (StreamId, Vec<(Vec<u8>, Vec<u8>)>);

/// An append-only stream of entries, ordered by id.
#[derive(Clone)]
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
            Value::Hll(_) => "hll",
            // The stub remembers its original type, so TYPE stays disk-free.
            Value::Tiered { vtag, .. } => crate::rdb::tag_type_name(*vtag),
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
            | Value::TDigest(_)
            | Value::Hll(_)
            | Value::Tiered { .. } => false,
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
    /// Sampling pool for the active reaper: every key that ever gained a TTL,
    /// with lazy deletion (stale entries are dropped when drawn, and the pool
    /// is rebuilt when it grows well past `expires`). std's HashMap can't be
    /// indexed randomly, so this Vec is what makes O(1) *random* sampling
    /// possible — iterating `expires` from the front every cycle would sample
    /// the same keys forever and let everything behind them leak.
    expiry_pool: Vec<Vec<u8>>,
    /// Victim cache for `evict_one`: a random window of keys, refilled on
    /// demand, so eviction is random-ish instead of iteration-order-determined.
    evict_pool: Vec<Vec<u8>>,
    /// Replica mode: expiry is HIDE-don't-delete. A logically-expired key reads
    /// as absent but is NOT removed until the master streams the DEL. Deleting
    /// it locally (clock skew ahead of the master) then having the master
    /// extend its TTL leaves the two permanently diverged.
    replica_mode: bool,
    /// The disk tier (None = disabled). Attached by the hub from LOCUS_TIER;
    /// carried across a full-resync dataset swap (the tier is node-local).
    tier: Option<crate::tier::TierStore>,
    /// key → segment, for every tiered stub — the accounting that lets a
    /// segment be deleted the moment its last live stub dies.
    tiered_keys: HashMap<Vec<u8>, u32>,
    /// segment → live stub count.
    seg_live: HashMap<u32, u64>,
    /// Tiered entries that could not be read back (deleted/corrupt segment) —
    /// detected losses, surfaced in INFO. Should stay 0 in a healthy system.
    tier_lost: u64,
    /// Approximate memory accounting for `maxmemory`. `mem_used` is the running
    /// total; `sizes` is the last-known estimate per key so removals/updates can
    /// be applied as deltas. Estimates are deliberately coarse (see
    /// `estimate_size`) — enough to bound growth, not byte-exact like Redis.
    mem_used: usize,
    sizes: HashMap<Vec<u8>, usize>,
    /// Keys whose entry in `sizes` is stale because the value was written in
    /// place. Re-estimating a value costs O(elements) — doing it on every write
    /// made building any collection O(n²), which is what phase 2.1 of the
    /// execution plan measured at 60-268× Redis. So a write only *marks* its key
    /// here (O(1)) and `drain_dirty_sizes` folds the deltas in later, off the
    /// hot path. See `mark_size_dirty` for the accuracy contract.
    dirty_sizes: HashSet<Vec<u8>>,
    /// Spatial index for geo points: geohash cell id -> the keys in that cell,
    /// plus a key -> cell reverse map so updates/removals are exact. Maintained on
    /// insert and on every removal (via `forget_size`). GEOSEARCH range-scans only
    /// the cells covering the query box instead of every geo key.
    geo_index: BTreeMap<u64, HashSet<Vec<u8>>>,
    geo_cell: HashMap<Vec<u8>, u64>,
}

impl Db {
    pub fn new() -> Self {
        Db {
            data: HashMap::new(),
            expires: HashMap::new(),
            expired: Vec::new(),
            expiry_pool: Vec::new(),
            evict_pool: Vec::new(),
            replica_mode: false,
            tier: None,
            tiered_keys: HashMap::new(),
            seg_live: HashMap::new(),
            tier_lost: 0,
            mem_used: 0,
            sizes: HashMap::new(),
            dirty_sizes: HashSet::new(),
            geo_index: BTreeMap::new(),
            geo_cell: HashMap::new(),
        }
    }

    /// Add a geo point to the spatial index (caller has unindexed any prior entry).
    fn geo_reindex(&mut self, key: Vec<u8>, lon: f64, lat: f64) {
        let cell = geohash::encode(lon, lat);
        self.geo_index.entry(cell).or_default().insert(key.clone());
        self.geo_cell.insert(key, cell);
    }

    /// Remove a key from the spatial index (no-op if it isn't a geo point).
    fn geo_unindex(&mut self, key: &[u8]) {
        if let Some(cell) = self.geo_cell.remove(key)
            && let Some(set) = self.geo_index.get_mut(&cell)
        {
            set.remove(key);
            if set.is_empty() {
                self.geo_index.remove(&cell);
            }
        }
    }

    /// Candidate geo keys whose cell overlaps the lon/lat box — the GEOSEARCH
    /// fast path. The caller refines with the exact shape, so a few just-outside
    /// points are fine; there are never false negatives for an in-box point.
    pub fn geo_candidates(
        &self,
        min_lon: f64,
        min_lat: f64,
        max_lon: f64,
        max_lat: f64,
    ) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        for (lo, hi) in geohash::ranges_for_box(min_lon, min_lat, max_lon, max_lat) {
            for keys in self.geo_index.range(lo..=hi).map(|(_, v)| v) {
                out.extend(keys.iter().cloned());
            }
        }
        out
    }

    /// Set (or clear) replica mode. In replica mode the keyspace never expires
    /// keys on its own — only the master's streamed DELs remove them.
    pub fn set_replica_mode(&mut self, on: bool) {
        self.replica_mode = on;
    }

    // --- the disk tier (hot in RAM, warm on disk) ---------------------------

    /// Attach the node-local disk tier. The hub does this at boot (LOCUS_TIER)
    /// and re-attaches it across a full-resync dataset swap.
    pub fn attach_tier(&mut self, t: Option<crate::tier::TierStore>) {
        self.tier = t;
    }

    /// Detach the tier (to carry it across a dataset swap).
    pub fn take_tier(&mut self) -> Option<crate::tier::TierStore> {
        self.tier.take()
    }

    /// (enabled, segments, log bytes, live tiered keys, detected losses).
    pub fn tier_stats(&self) -> (bool, usize, u64, usize, u64) {
        match &self.tier {
            None => (false, 0, 0, 0, self.tier_lost),
            Some(t) => {
                let (segs, bytes) = t.stats();
                (true, segs, bytes, self.tiered_keys.len(), self.tier_lost)
            }
        }
    }

    /// Move `key`'s value to the disk tier, leaving a stub. Ok(true) = tiered
    /// (or already tiered — idempotent), Ok(false) = no such key.
    pub fn tier_key(&mut self, key: &[u8]) -> Result<bool, &'static str> {
        if self.tier.is_none() {
            return Err("tiering disabled (set LOCUS_TIER)");
        }
        self.check_expiry(key);
        if self.replica_mode && self.is_expired(key) {
            return Ok(false);
        }
        let expire = self.expires.get(key).copied();
        let entry = match self.data.get(key) {
            None => return Ok(false),
            Some(Value::Tiered { .. }) => return Ok(true),
            Some(v) => crate::rdb::dump_entry(key, v, expire),
        };
        let vtag = entry[if expire.is_some() { 9 } else { 1 }]; // [expire?][tag]...
        let (seg, off, len) = self
            .tier
            .as_mut()
            .unwrap()
            .append(&entry)
            .map_err(|_| "tier append failed")?;
        self.geo_unindex(key); // a tiered geo point leaves the spatial index
        self.data.insert(
            key.to_vec(),
            Value::Tiered {
                seg,
                off,
                len,
                vtag,
            },
        );
        self.note_tiered(key, seg);
        self.resync_size(key); // the stub is ~bytes, not the value
        Ok(true)
    }

    /// Recreate a stub directly (AOF-rewrite replay via TIERREF): the entry is
    /// already in the local value-log at this address.
    pub fn insert_tier_stub(&mut self, key: Vec<u8>, seg: u32, off: u64, len: u32, vtag: u8) {
        self.geo_unindex(&key);
        self.note_untiered(&key); // replacing whatever was there
        self.data.insert(
            key.clone(),
            Value::Tiered {
                seg,
                off,
                len,
                vtag,
            },
        );
        self.note_tiered(&key, seg);
        self.resync_size(&key);
    }

    /// Decode a tiered key's value from the log WITHOUT caching it back —
    /// for wire serialization (replication snapshots, slot migration), where a
    /// node-local stub must never cross to another node.
    pub fn tier_fetch(&self, key: &[u8], seg: u32, off: u64, len: u32) -> Option<Value> {
        let entry = self.tier.as_ref()?.read(seg, off, len)?;
        match crate::rdb::restore_entry(&entry) {
            Ok((k, v, _)) if k == key => Some(v),
            _ => None,
        }
    }

    /// If `key` is a tiered stub, load its value back into RAM (thaw). A read
    /// that can't be satisfied (deleted/corrupt segment) is a DETECTED loss:
    /// the key is removed, counted, and logged — never silent garbage.
    fn thaw_if_tiered(&mut self, key: &[u8]) {
        let (seg, off, len) = match self.data.get(key) {
            Some(Value::Tiered { seg, off, len, .. }) => (*seg, *off, *len),
            _ => return,
        };
        match self.tier_fetch(key, seg, off, len) {
            Some(v) => {
                self.note_untiered(key);
                if let Value::Geo(lon, lat, _) = &v {
                    self.geo_reindex(key.to_vec(), *lon, *lat);
                }
                self.data.insert(key.to_vec(), v);
                self.resync_size(key);
            }
            None => {
                self.tier_lost += 1;
                crate::log::error(&format!(
                    "tier: entry for key {:?} unreadable (segment {seg}) — treating as lost",
                    String::from_utf8_lossy(key)
                ));
                self.note_untiered(key);
                self.data.remove(key);
                self.expires.remove(key);
                self.forget_size(key);
            }
        }
    }

    fn note_tiered(&mut self, key: &[u8], seg: u32) {
        self.tiered_keys.insert(key.to_vec(), seg);
        *self.seg_live.entry(seg).or_insert(0) += 1;
    }

    /// A tiered stub died (expired/deleted/overwritten/thawed): decrement its
    /// segment's live count, and delete the segment the moment it empties.
    fn note_untiered(&mut self, key: &[u8]) {
        let Some(seg) = self.tiered_keys.remove(key) else {
            return;
        };
        if let Some(n) = self.seg_live.get_mut(&seg) {
            *n = n.saturating_sub(1);
            if *n == 0 {
                self.seg_live.remove(&seg);
                if let Some(t) = self.tier.as_mut() {
                    t.delete_segment(seg);
                }
            }
        }
    }

    /// Is `key` logically expired right now (deadline passed)?
    fn is_expired(&self, key: &[u8]) -> bool {
        self.expires.get(key).is_some_and(|&d| d <= now_ms())
    }

    fn check_expiry(&mut self, key: &[u8]) {
        // On a replica, reads HIDE an expired key (handled by callers via
        // is_expired) but never delete it — the master owns expiry timing.
        if self.replica_mode {
            return;
        }
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
    /// O(elements): it walks the whole value, which is why the write path marks
    /// instead of calling this (see `mark_size_dirty`). Still used directly
    /// wherever a key is installed *whole* — a snapshot load, a RESTORE, a
    /// tier/thaw — where one walk per key is inherent and there is no O(n²).
    ///
    /// Exact whenever it runs, stale entry or not: the delta is measured against
    /// `sizes[key]` (the last value folded into the total), so the invariant
    /// `mem_used == Σ sizes[key]` holds either way.
    pub fn resync_size(&mut self, key: &[u8]) {
        self.dirty_sizes.remove(key); // settled now — no need to redo it later
        let volatile = self.expires.contains_key(key);
        let new = self
            .data
            .get(key)
            .map(|v| estimate_size(key.len(), v, volatile))
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

    /// Note that `key`'s size estimate is stale — O(1), and it allocates a key
    /// copy only the first time a key goes dirty. This is what the hub calls
    /// after every write.
    ///
    /// **The accuracy contract.** `mem_used` has exactly two consumers, and both
    /// are made exact before they read it rather than on every write:
    ///
    /// - `INFO used_memory` drains fully first, so what an operator (or
    ///   `redis_exporter`) sees is never stale.
    /// - the `maxmemory` eviction gate drains fully once the estimate is near
    ///   the cap, so eviction decides on a settled number.
    ///
    /// Everywhere else the estimate simply catches up on the maintenance tick.
    /// This preserves the existing contract — a coarse figure that deliberately
    /// over-counts — while taking an O(elements) walk off every single write.
    pub fn mark_size_dirty(&mut self, key: &[u8]) {
        if !self.dirty_sizes.contains(key) {
            self.dirty_sizes.insert(key.to_vec());
        }
    }

    /// Is every key's size estimate up to date?
    pub fn sizes_settled(&self) -> bool {
        self.dirty_sizes.is_empty()
    }

    /// Re-estimate up to `budget` stale keys and fold their deltas into the
    /// total; returns how many were done. `usize::MAX` settles everything.
    ///
    /// The budget is what keeps one maintenance tick bounded: without it a burst
    /// that dirtied a very large number of keys would be paid for in a single
    /// sweep, and the hub thread owns the keyspace — a long sweep is a stall for
    /// every client.
    pub fn drain_dirty_sizes(&mut self, budget: usize) -> usize {
        if budget == 0 || self.dirty_sizes.is_empty() {
            return 0;
        }
        let batch: Vec<Vec<u8>> = if self.dirty_sizes.len() <= budget {
            std::mem::take(&mut self.dirty_sizes).into_iter().collect()
        } else {
            self.dirty_sizes.iter().take(budget).cloned().collect()
        };
        for key in &batch {
            self.resync_size(key); // also drops it from dirty_sizes
        }
        batch.len()
    }

    /// Drop per-key tracking when a key is removed (called from every removal
    /// path): memory accounting, the geo-key index, and tier accounting (a
    /// dead stub may empty — and thus delete — its log segment).
    fn forget_size(&mut self, key: &[u8]) {
        self.dirty_sizes.remove(key); // nothing left to re-estimate
        if let Some(sz) = self.sizes.remove(key) {
            self.mem_used = self.mem_used.saturating_sub(sz);
        }
        self.geo_unindex(key);
        self.note_untiered(key);
    }

    /// Candidate keys for GEOSEARCH (those holding a geo point). The caller
    /// re-reads each via `get` (which skips expired keys).
    pub fn geo_keys(&self) -> Vec<Vec<u8>> {
        self.geo_cell.keys().cloned().collect()
    }

    /// Evict one random-ish key (allkeys-random). Victims come from a small
    /// cache refilled from a RANDOM window of the keyspace: `.keys().next()`
    /// would hammer whatever sits at the front of HashMap iteration order and
    /// never touch the back. The O(n) skip to a random offset is amortized over
    /// a window of evictions. Returns None if the keyspace is empty.
    pub fn evict_one(&mut self) -> Option<Vec<u8>> {
        loop {
            match self.evict_pool.pop() {
                Some(key) if self.data.contains_key(&key) => {
                    self.data.remove(&key);
                    self.expires.remove(&key);
                    self.forget_size(&key);
                    return Some(key);
                }
                Some(_) => continue, // already gone (expired/deleted) — next
                None => {
                    if self.data.is_empty() {
                        return None;
                    }
                    let skip = crate::commands::rand_index(self.data.len());
                    self.evict_pool = self.data.keys().skip(skip).take(64).cloned().collect();
                }
            }
        }
    }

    pub fn get(&mut self, key: &[u8]) -> Option<&Value> {
        self.check_expiry(key);
        if self.replica_mode && self.is_expired(key) {
            return None; // hidden-but-present until the master's DEL
        }
        self.thaw_if_tiered(key);
        self.data.get(key)
    }

    pub fn get_mut(&mut self, key: &[u8]) -> Option<&mut Value> {
        self.check_expiry(key);
        if self.replica_mode && self.is_expired(key) {
            return None;
        }
        self.thaw_if_tiered(key);
        self.data.get_mut(key)
    }

    pub fn insert(&mut self, key: Vec<u8>, value: Value) {
        self.geo_unindex(&key); // drop any prior geo entry (overwrite/retype)
        self.note_untiered(&key); // overwriting a stub kills its log entry
        if let Value::Geo(lon, lat, _) = &value {
            self.geo_reindex(key.clone(), *lon, *lat);
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
        if self.replica_mode && self.is_expired(key) {
            return false;
        }
        self.data.contains_key(key)
    }

    pub fn type_name(&mut self, key: &[u8]) -> Option<&'static str> {
        self.check_expiry(key);
        if self.replica_mode && self.is_expired(key) {
            return None;
        }
        self.data.get(key).map(|v| v.type_name())
    }

    /// Get a value for in-place mutation, creating it via `f` if absent.
    /// (If the key exists with a different type, the existing value is returned
    /// unchanged — callers must type-check the result.)
    pub fn get_or_insert_with(&mut self, key: &[u8], f: impl FnOnce() -> Value) -> &mut Value {
        self.check_expiry(key);
        self.thaw_if_tiered(key); // mutating a tiered value rehydrates it
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
        if self.data.contains_key(key) && self.expires.insert(key.to_vec(), at_ms).is_none() {
            self.expiry_pool.push(key.to_vec()); // newly-volatile key -> sampleable
        }
    }

    pub fn clear_expire(&mut self, key: &[u8]) -> bool {
        self.expires.remove(key).is_some()
    }

    pub fn expire_at(&mut self, key: &[u8]) -> Option<u64> {
        self.check_expiry(key);
        self.expires.get(key).copied()
    }

    /// Active TTL reaper: sample RANDOM volatile keys (via the pool — HashMap
    /// iteration order would resample the same front group forever and leak
    /// everything behind it), delete the expired ones, and keep going while a
    /// quarter or more of each sample was expired (Redis's stop rule). Bounded
    /// per call so it can't stall the hub.
    pub fn active_expire(&mut self) {
        if self.replica_mode {
            return; // the master owns expiry timing; we wait for its DELs
        }
        let now = now_ms();
        // Lazy compaction: stale entries (TTL removed/overwritten keys) are
        // dropped as drawn; a full rebuild only when the pool is mostly stale.
        if self.expiry_pool.len() > self.expires.len().saturating_mul(4) + 64 {
            self.expiry_pool = self.expires.keys().cloned().collect();
        }
        for _round in 0..16 {
            if self.expires.is_empty() || self.expiry_pool.is_empty() {
                break;
            }
            let total = 20usize.min(self.expiry_pool.len());
            let mut expired = 0usize;
            for _ in 0..total {
                if self.expiry_pool.is_empty() {
                    break;
                }
                let i = crate::commands::rand_index(self.expiry_pool.len());
                let deadline = self.expires.get(&self.expiry_pool[i]).copied();
                match deadline {
                    None => {
                        // Stale pool entry (no longer volatile): drop it.
                        self.expiry_pool.swap_remove(i);
                    }
                    Some(t) if t <= now => {
                        let k = self.expiry_pool.swap_remove(i);
                        self.data.remove(&k);
                        self.expires.remove(&k);
                        self.forget_size(&k);
                        self.expired.push(k);
                        expired += 1;
                    }
                    Some(_) => {} // alive: leave it in the pool
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
        self.live_keys_iter().cloned().collect()
    }

    /// Borrowing iterator over live keys — lets SCAN walk the keyspace without
    /// cloning every key per call.
    pub fn live_keys_iter(&self) -> impl Iterator<Item = &Vec<u8>> + '_ {
        let now = now_ms();
        self.data
            .keys()
            .filter(move |k| self.expires.get(*k).is_none_or(|&d| d > now))
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
        self.expiry_pool.clear();
        self.evict_pool.clear();
        self.sizes.clear();
        self.dirty_sizes.clear();
        self.mem_used = 0;
        self.geo_index.clear();
        self.geo_cell.clear();
        // Every tiered stub just died with the keyspace: the whole log is dead.
        self.tiered_keys.clear();
        self.seg_live.clear();
        if let Some(t) = self.tier.as_mut()
            && let Err(e) = t.delete_all()
        {
            crate::log::warn(&format!("tier: flush could not reset the log: {e}"));
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
        self.geo_unindex(&key); // drop any prior geo entry (overwrite/retype)
        self.note_untiered(&key);
        if let Some(deadline) = expire
            && self.expires.insert(key.clone(), deadline).is_none()
        {
            self.expiry_pool.push(key.clone()); // newly-volatile key -> sampleable
        }
        if let Value::Geo(lon, lat, _) = &value {
            self.geo_reindex(key.clone(), *lon, *lat);
        }
        // A stub loaded from a snapshot re-enters the tier accounting, so its
        // segment's lifetime tracking survives a restart.
        if let Value::Tiered { seg, .. } = &value {
            let seg = *seg;
            self.data.insert(key.clone(), value);
            self.note_tiered(&key, seg);
            self.resync_size(&key);
            return;
        }
        self.data.insert(key.clone(), value);
        self.resync_size(&key); // loaded data counts toward used memory
    }
}

/// A coarse estimate of a key+value's memory footprint, in bytes. Not byte-exact
/// (no allocator introspection in zero-deps `std`); a fixed per-key and
/// per-element overhead approximates allocation bookkeeping well enough to bound
/// growth under `maxmemory`. Deliberately estimates HIGH rather than low: the
/// side-tables (`sizes`, `expires`), the zset's ordered index (a second copy of
/// every member), the geo spatial index, and HashMap load-factor slack all cost
/// real bytes — leaving them out makes eviction fire late and hands the finish
/// to the OOM killer.
fn estimate_size(key_len: usize, v: &Value, volatile: bool) -> usize {
    const KEY_OVH: usize = 64; // HashMap entry + key/value headers
    const ELEM_OVH: usize = 16; // per collection element
    let val = match v {
        Value::Str(s) => s.len(),
        Value::List(l) => l.iter().map(|e| e.len() + ELEM_OVH).sum(),
        Value::Hash(h) => h.iter().map(|(k, vv)| k.len() + vv.len() + ELEM_OVH).sum(),
        Value::Set(s) => s.iter().map(|e| e.len() + ELEM_OVH).sum(),
        // Both halves of the ZSet: the member->score map AND the (score, member)
        // ordered index, which duplicates every member's bytes.
        Value::ZSet(z) => z.iter().map(|(m, _)| 2 * (m.len() + 8 + ELEM_OVH)).sum(),
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
        // A geo point also lives in geo_cell (key copy + cell id) and in a
        // geo_index cell set (another key copy) — the product's hottest type
        // must not be its most under-counted.
        Value::Geo(_, _, attrs) => {
            16 + 2 * (key_len + ELEM_OVH)
                + 24
                + attrs.iter().map(|(f, v)| f.len() + v.len()).sum::<usize>()
        }
        Value::Bloom(b) => b.bits.len(),
        Value::Cms(c) => c.counters.len() * 4,
        Value::TopK(t) => {
            t.cms.counters.len() * 4 + t.top.iter().map(|(it, _)| it.len() + 16).sum::<usize>()
        }
        Value::TDigest(t) => t.centroids.len() * 16 + 32,
        Value::Hll(h) => h.regs.len(), // dense: 16 KB flat
        // The whole point: a tiered value costs RAM only for its stub.
        Value::Tiered { .. } => 32,
    };
    // sizes-map entry (key copy + usize) always; expires entry when volatile.
    let side = key_len + 24 + if volatile { key_len + 32 } else { 0 };
    let total = KEY_OVH + key_len + val + side;
    // HashMap load-factor / allocator slack: ~1.5x on average.
    total + total / 2
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

    /// Phase 2.1: the write path only *marks* a key's size estimate stale, and
    /// the total is folded in later. The deferred total must land on exactly the
    /// number the old eager path produced — same keyspace, same figure.
    #[test]
    fn deferred_size_accounting_converges_to_the_eager_total() {
        let build = |mut db: Db, defer: bool| -> Db {
            for i in 0..200u32 {
                let key = format!("k{}", i % 7).into_bytes();
                match db.get_mut(&key) {
                    Some(Value::Set(set)) => {
                        set.insert(format!("member-{i}").into_bytes());
                    }
                    _ => {
                        let mut set = std::collections::HashSet::new();
                        set.insert(format!("member-{i}").into_bytes());
                        db.insert(key.clone(), Value::Set(set));
                    }
                }
                if defer {
                    db.mark_size_dirty(&key);
                } else {
                    db.resync_size(&key);
                }
            }
            db
        };
        let eager = build(Db::new(), false);
        let mut deferred = build(Db::new(), true);

        // Before the drain the deferred total is behind — that is the whole point.
        assert!(deferred.mem_used() < eager.mem_used());
        assert!(!deferred.sizes_settled());

        deferred.drain_dirty_sizes(usize::MAX);
        assert!(deferred.sizes_settled());
        assert_eq!(
            deferred.mem_used(),
            eager.mem_used(),
            "a drained estimate must equal what per-write re-syncing produced"
        );

        // And it is idempotent: draining again changes nothing.
        deferred.drain_dirty_sizes(usize::MAX);
        assert_eq!(deferred.mem_used(), eager.mem_used());
    }

    /// The budget is what keeps one maintenance sweep bounded, so it has to
    /// actually stop — and the remainder has to survive to the next sweep.
    #[test]
    fn drain_dirty_sizes_honors_its_budget_and_finishes_later() {
        let mut db = Db::new();
        for i in 0..10u32 {
            let k = format!("k{i}").into_bytes();
            db.insert(k.clone(), Value::Str(vec![b'x'; 100]));
            db.mark_size_dirty(&k);
        }
        assert_eq!(db.mem_used(), 0, "nothing folded in yet");

        assert_eq!(db.drain_dirty_sizes(4), 4);
        let partial = db.mem_used();
        assert!(partial > 0 && !db.sizes_settled());

        assert_eq!(db.drain_dirty_sizes(usize::MAX), 6);
        assert!(db.sizes_settled());
        assert!(db.mem_used() > partial);

        // A settled keyspace drains to a no-op.
        assert_eq!(db.drain_dirty_sizes(usize::MAX), 0);

        // Marking the same key repeatedly costs one entry, not one per write.
        for _ in 0..100 {
            db.mark_size_dirty(b"k0");
        }
        assert_eq!(db.drain_dirty_sizes(usize::MAX), 1);
    }

    /// A key that dies before its pending re-estimate lands must take the
    /// pending entry with it — otherwise the drain re-visits a ghost, and the
    /// removal's own delta could be applied twice.
    #[test]
    fn removing_a_key_retires_its_pending_size() {
        let mut db = Db::new();
        db.insert(b"k".to_vec(), Value::Str(vec![b'x'; 100]));
        db.resync_size(b"k");
        let accounted = db.mem_used();
        assert!(accounted > 0);

        // Grow it in place, defer the estimate, then delete the key outright.
        db.insert(b"k".to_vec(), Value::Str(vec![b'x'; 10_000]));
        db.mark_size_dirty(b"k");
        db.remove(b"k");

        assert!(
            db.sizes_settled(),
            "a removed key has nothing to re-estimate"
        );
        assert_eq!(db.mem_used(), 0);
        db.drain_dirty_sizes(usize::MAX);
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
    fn tier_thaw_roundtrip_frees_and_restores_memory() {
        let base = format!(
            "{}/locus-dbtier-{}",
            std::env::temp_dir().display(),
            std::process::id()
        );
        let mut db = Db::new();
        db.attach_tier(Some(crate::tier::TierStore::open(&base, 0).unwrap()));
        let big = vec![b'x'; 10_000];
        db.insert(b"k".to_vec(), Value::Str(big.clone()));
        db.resync_size(b"k");
        let hot = db.mem_used();

        // TIER: memory drops to stub-size; TYPE still answers; EXISTS holds.
        assert_eq!(db.tier_key(b"k"), Ok(true));
        assert!(
            db.mem_used() < hot / 10,
            "stub should be tiny: {}",
            db.mem_used()
        );
        assert_eq!(db.type_name(b"k"), Some("string"));
        assert!(db.contains(b"k"));
        let (_, _, log_bytes, keys, lost) = db.tier_stats();
        assert!(log_bytes > 10_000 && keys == 1 && lost == 0);

        // GET transparently thaws the value back.
        match db.get(b"k") {
            Some(Value::Str(s)) => assert_eq!(s, &big),
            other => panic!("thaw failed: {:?}", other.map(|v| v.type_name())),
        }
        assert!(db.mem_used() >= hot, "thawed value costs RAM again");
        let (_, _, _, keys, _) = db.tier_stats();
        assert_eq!(keys, 0, "no tiered stubs after thaw");

        // Idempotence + missing key.
        assert_eq!(db.tier_key(b"k"), Ok(true));
        assert_eq!(db.tier_key(b"k"), Ok(true));
        assert_eq!(db.tier_key(b"absent"), Ok(false));
        db.clear(); // deletes the log segments too
    }

    #[test]
    fn tiered_geo_leaves_the_spatial_index_and_returns_on_thaw() {
        let base = format!(
            "{}/locus-dbtier-geo-{}",
            std::env::temp_dir().display(),
            std::process::id()
        );
        let mut db = Db::new();
        db.attach_tier(Some(crate::tier::TierStore::open(&base, 0).unwrap()));
        db.insert(b"g".to_vec(), Value::Geo(10.0, 10.0, vec![]));
        assert_eq!(db.geo_candidates(9.9, 9.9, 10.1, 10.1).len(), 1);
        // Tiered = archived: it leaves the live spatial index...
        assert_eq!(db.tier_key(b"g"), Ok(true));
        assert!(db.geo_candidates(9.9, 9.9, 10.1, 10.1).is_empty());
        // ...and re-enters it when thawed by a read.
        assert!(matches!(db.get(b"g"), Some(Value::Geo(..))));
        assert_eq!(db.geo_candidates(9.9, 9.9, 10.1, 10.1).len(), 1);
        db.clear();
    }

    #[test]
    fn expired_tiered_stub_empties_and_deletes_its_segment() {
        let base = format!(
            "{}/locus-dbtier-exp-{}",
            std::env::temp_dir().display(),
            std::process::id()
        );
        let mut db = Db::new();
        db.attach_tier(Some(crate::tier::TierStore::open(&base, 0).unwrap()));
        db.insert(b"k".to_vec(), Value::Str(vec![b'v'; 500]));
        db.set_expire(b"k", now_ms().saturating_sub(1));
        assert_eq!(db.tier_key(b"k"), Ok(false), "expired key can't be tiered");

        db.insert(b"k2".to_vec(), Value::Str(vec![b'v'; 500]));
        assert_eq!(db.tier_key(b"k2"), Ok(true));
        db.set_expire(b"k2", now_ms().saturating_sub(1));
        db.active_expire(); // reaps the stub -> its segment's live count hits 0
        let (_, _, _, keys, lost) = db.tier_stats();
        assert_eq!((keys, lost), (0, 0));
        assert!(db.get(b"k2").is_none());
        db.clear();
    }

    #[test]
    fn replica_mode_hides_but_keeps_expired_keys() {
        let mut db = Db::new();
        db.set_replica_mode(true);
        db.insert(b"k".to_vec(), Value::Str(b"v".to_vec()));
        db.set_expire(b"k", now_ms().saturating_sub(1)); // already past
        // Reads hide the logically-expired key...
        assert!(db.get(b"k").is_none());
        assert!(!db.contains(b"k"));
        // ...but the active reaper does NOT delete it (master owns expiry).
        db.active_expire();
        assert!(db.take_expired().is_empty(), "replica reaped a key itself");
        // The master's streamed DEL removes it and dirties WATCHers as normal.
        assert!(db.remove(b"k").is_some());
        // Back on a master, expiry deletes locally again.
        db.set_replica_mode(false);
        db.insert(b"k2".to_vec(), Value::Str(b"v".to_vec()));
        db.set_expire(b"k2", now_ms().saturating_sub(1));
        db.active_expire();
        assert_eq!(db.take_expired(), vec![b"k2".to_vec()]);
    }

    #[test]
    fn insert_with_expire_reindexes_geo_overwrite() {
        let mut db = Db::new();
        // Overwriting a geo key via the restore/migration path must MOVE its
        // spatial-index entry, not leave the old cell's entry behind forever.
        db.insert(b"g".to_vec(), Value::Geo(10.0, 10.0, vec![]));
        db.insert_with_expire(b"g".to_vec(), Value::Geo(-60.0, -30.0, vec![]), None);
        let old = db.geo_candidates(9.9, 9.9, 10.1, 10.1);
        assert!(!old.contains(&b"g".to_vec()), "stale entry at the old cell");
        let new = db.geo_candidates(-60.1, -30.1, -59.9, -29.9);
        assert!(new.contains(&b"g".to_vec()));
        // Retyping to a non-geo value must drop the key from the index entirely.
        db.insert_with_expire(b"g".to_vec(), Value::Str(b"x".to_vec()), None);
        assert!(db.geo_keys().is_empty());
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

    /// Phase 2.2: the range readers walk the ordered index instead of cloning the
    /// whole set. Their answers must match the materialise-then-slice path they
    /// replaced, so this pins them against a brute-force reference over every
    /// index and bound combination that used to be forgiving.
    #[test]
    fn zset_range_readers_match_a_materialised_reference() {
        let build = |pairs: &[(&str, f64)]| -> ZSet {
            let mut z = ZSet::new();
            for (m, s) in pairs {
                z.insert(m.as_bytes().to_vec(), *s);
            }
            z
        };
        // Distinct scores, score ties (member bytes break them), infinities, and
        // the degenerate single-member and empty sets.
        let sets = [
            build(&[
                ("a", 0.0),
                ("b", 10.0),
                ("c", 20.0),
                ("d", 30.0),
                ("e", 40.0),
            ]),
            build(&[("x", 1.0), ("y", 1.0), ("z", 1.0)]),
            build(&[
                ("lo", f64::NEG_INFINITY),
                ("mid", 0.0),
                ("hi", f64::INFINITY),
            ]),
            build(&[("only", 5.0)]),
            ZSet::new(),
        ];

        for z in &sets {
            // The reference: what the old path produced — everything, in order.
            let all: Vec<(Vec<u8>, f64)> = z.sorted.iter().map(|(s, m)| (m.clone(), s.0)).collect();
            let len = all.len();

            for start in -8i64..=8 {
                for stop in -8i64..=8 {
                    for rev in [false, true] {
                        // Reference: reverse first (as the old code did), then
                        // normalise indices against the same length, then slice.
                        let mut base = all.clone();
                        if rev {
                            base.reverse();
                        }
                        let s = if start < 0 { start + len as i64 } else { start }.max(0);
                        let mut e = if stop < 0 { stop + len as i64 } else { stop };
                        if e >= len as i64 {
                            e = len as i64 - 1;
                        }
                        let want: Vec<(Vec<u8>, f64)> = if len == 0 || s > e || s >= len as i64 {
                            vec![]
                        } else {
                            base[s as usize..=e as usize].to_vec()
                        };
                        assert_eq!(
                            z.range_by_rank(start, stop, rev),
                            want,
                            "range_by_rank({start}, {stop}, rev={rev}) on a {len}-member set"
                        );
                    }
                }
            }

            for lo in [
                f64::NEG_INFINITY,
                -1.0,
                0.0,
                1.0,
                10.0,
                20.0,
                40.0,
                f64::INFINITY,
            ] {
                for hi in [
                    f64::NEG_INFINITY,
                    -1.0,
                    0.0,
                    1.0,
                    10.0,
                    20.0,
                    40.0,
                    f64::INFINITY,
                ] {
                    for lo_ex in [false, true] {
                        for hi_ex in [false, true] {
                            let want: Vec<(Vec<u8>, f64)> = all
                                .iter()
                                .filter(|(_, s)| {
                                    let above = if lo_ex { *s > lo } else { *s >= lo };
                                    let below = if hi_ex { *s < hi } else { *s <= hi };
                                    above && below
                                })
                                .cloned()
                                .collect();
                            assert_eq!(
                                z.range_by_score(lo, lo_ex, hi, hi_ex),
                                want,
                                "range_by_score({lo}, ex={lo_ex}, {hi}, ex={hi_ex})"
                            );
                        }
                    }
                }
            }

            for n in 0..=len + 2 {
                for from_high in [false, true] {
                    let mut want = all.clone();
                    if from_high {
                        want.reverse();
                    }
                    want.truncate(n);
                    assert_eq!(
                        z.first_by_rank(n, from_high),
                        want,
                        "first_by_rank({n}, from_high={from_high})"
                    );
                }
            }
        }
    }
}
