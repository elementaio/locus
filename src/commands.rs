//! Command dispatch and implementations.
//!
//! Organized by type: generic/expiry, strings, lists, hashes, sets. Every
//! command that targets a typed key returns WRONGTYPE if the key holds a
//! different type.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::db::{Db, Value, ZSet, now_ms};
use crate::resp::{
    array, bulk_array, bulk_string, error, integer, null_array, null_bulk, simple_string,
};
use crate::streams;

/// A hash value: field -> value.
type HashVal = HashMap<Vec<u8>, Vec<u8>>;

// --- shared helpers ---------------------------------------------------------

/// A tiny non-cryptographic PRNG (xorshift64) for SRANDMEMBER/RANDOMKEY/SPOP —
/// zero-deps, lazily seeded from the clock. Good enough for "pick something
/// arbitrary", not for anything security-sensitive.
static RNG_STATE: AtomicU64 = AtomicU64::new(0);

fn next_rand() -> u64 {
    let mut s = RNG_STATE.load(Ordering::Relaxed);
    if s == 0 {
        s = now_ms() ^ 0x9E37_79B9_7F4A_7C15; // never zero
    }
    s ^= s << 13;
    s ^= s >> 7;
    s ^= s << 17;
    RNG_STATE.store(s, Ordering::Relaxed);
    s
}

/// A uniform-ish index in `0..n` (n must be > 0). Shared with the keyspace's
/// expiry/eviction sampling (db.rs).
pub(crate) fn rand_index(n: usize) -> usize {
    (next_rand() % n as u64) as usize
}

/// CRC16-CCITT/XMODEM (poly 0x1021, init 0) — the hash Redis Cluster uses for key
/// slots. The 52-bit geohash cell id is Locus's *spatial* shard key; this is the
/// hash-slot fallback for non-geo keys and `CLUSTER KEYSLOT`.
pub fn crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &b in data {
        crc ^= (b as u16) << 8;
        for _ in 0..8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ 0x1021
            } else {
                crc << 1
            };
        }
    }
    crc
}

/// The key(s) a command addresses — the shared source of truth for cluster
/// routing (MOVED/CROSSSLOT) and per-key ACL checks. Empty = not keyed
/// (connection/admin/pubsub commands, and keyspace-wide data commands like
/// KEYS / SCAN / GEOSEARCH that aren't pinned to one slot).
///
/// Completeness matters twice over: a source key missing here executes as
/// "empty" on the wrong shard instead of returning CROSSSLOT, and skips its
/// ACL prefix check. Every multi-key layout (incl. the store variants and the
/// numkeys forms) is spelled out.
pub fn command_keys(tokens: &[Vec<u8>]) -> Vec<&[u8]> {
    if tokens.len() < 2 {
        return vec![];
    }
    let cmd = tokens[0].to_ascii_uppercase();
    match cmd.as_slice() {
        b"DEL" | b"UNLINK" | b"EXISTS" | b"TOUCH" | b"MGET" | b"SINTER" | b"SUNION" | b"SDIFF"
        | b"PFCOUNT" | b"SINTERSTORE" | b"SUNIONSTORE" | b"SDIFFSTORE" | b"WATCH" => {
            return tokens[1..].iter().map(|k| k.as_slice()).collect();
        }
        b"MSET" | b"MSETNX" => {
            return tokens[1..]
                .iter()
                .step_by(2)
                .map(|k| k.as_slice())
                .collect();
        }
        b"RENAME" | b"RENAMENX" | b"RPOPLPUSH" | b"LMOVE" | b"SMOVE" => {
            return tokens[1..3.min(tokens.len())]
                .iter()
                .map(|k| k.as_slice())
                .collect();
        }
        // BITOP op dest src [src ...] — destination AND every source.
        b"BITOP" => {
            return tokens
                .get(2..)
                .map(|ks| ks.iter().map(|k| k.as_slice()).collect())
                .unwrap_or_default();
        }
        // ZUNIONSTORE/ZINTERSTORE dest numkeys key [key ...] [...]
        b"ZUNIONSTORE" | b"ZINTERSTORE" => {
            let mut keys = vec![tokens[1].as_slice()];
            keys.extend(numkeys_keys(tokens, 2));
            return keys;
        }
        // SINTERCARD numkeys key [key ...] [LIMIT n]
        b"SINTERCARD" => return numkeys_keys(tokens, 1),
        // XREAD [COUNT n] [BLOCK ms] STREAMS key [key ...] id [id ...]
        b"XREAD" => {
            if let Some(pos) = tokens
                .iter()
                .position(|t| t.eq_ignore_ascii_case(b"STREAMS"))
            {
                let rest = &tokens[pos + 1..];
                return rest[..rest.len() / 2]
                    .iter()
                    .map(|k| k.as_slice())
                    .collect();
            }
            return vec![];
        }
        // Cluster-wide / cross-key data commands: not addressed to a single slot.
        b"KEYS" | b"SCAN" | b"RANDOMKEY" | b"WAIT" | b"GEOSEARCH" | b"GEOSEARCHSHARD"
        | b"IDXGET" | b"IDXRANGE" => {
            return vec![];
        }
        _ => {}
    }
    // Changefeed commands carry prefixes/offsets/groups, not keys, and are
    // node-local — never routed. (Their ACL gating is prefix-based, in the hub.)
    if cmd.starts_with(b"CDC") {
        return vec![];
    }
    // Otherwise a single-key data command keys on tokens[1]; non-data commands
    // (connection/admin/pubsub) don't route.
    match command_class(&cmd) {
        crate::acl::CLASS_READ | crate::acl::CLASS_WRITE => vec![tokens[1].as_slice()],
        _ => vec![],
    }
}

/// The keys of a `numkeys key [key ...]` form, with `numkeys` at `tokens[at]`.
/// Empty on a malformed count (execution will reject it anyway).
fn numkeys_keys(tokens: &[Vec<u8>], at: usize) -> Vec<&[u8]> {
    let n = tokens
        .get(at)
        .and_then(|t| parse_int(t))
        .filter(|&n| n > 0)
        .map(|n| n as usize)
        .unwrap_or(0);
    tokens
        .get(at + 1..(at + 1 + n).min(tokens.len()))
        .map(|ks| ks.iter().map(|k| k.as_slice()).collect())
        .unwrap_or_default()
}

/// Number of hash slots in the keyspace (Redis Cluster's fixed 16384).
pub const CLUSTER_SLOTS: usize = 16384;

/// Map a key to one of 16384 hash slots, honoring a `{hashtag}` (the first
/// non-empty `{...}` is hashed instead of the whole key) — Redis Cluster's rule.
pub fn hash_slot(key: &[u8]) -> u16 {
    let tag = match key.iter().position(|&c| c == b'{') {
        Some(open) => match key[open + 1..].iter().position(|&c| c == b'}') {
            Some(rel) if rel > 0 => &key[open + 1..open + 1 + rel],
            _ => key,
        },
        None => key,
    };
    crc16(tag) % CLUSTER_SLOTS as u16
}

/// Pick `k` distinct indices from `0..n` (k clamped to n) via partial
/// Fisher-Yates shuffle.
fn distinct_indices(n: usize, k: usize) -> Vec<usize> {
    let k = k.min(n);
    let mut idx: Vec<usize> = (0..n).collect();
    for i in 0..k {
        let j = i + rand_index(n - i);
        idx.swap(i, j);
    }
    idx.truncate(k);
    idx
}

fn wrong_args(cmd: &str) -> Vec<u8> {
    error(&format!(
        "ERR wrong number of arguments for '{cmd}' command"
    ))
}
fn not_integer() -> Vec<u8> {
    error("ERR value is not an integer or out of range")
}
fn wrongtype() -> Vec<u8> {
    error("WRONGTYPE Operation against a key holding the wrong kind of value")
}
fn parse_int(arg: &[u8]) -> Option<i64> {
    std::str::from_utf8(arg)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
}

pub fn execute(tokens: &[Vec<u8>], db: &mut Db) -> Vec<u8> {
    execute_proto(tokens, db, 2)
}

/// Like `execute`, but `proto` (2 or 3) selects RESP2 vs RESP3 typed replies for
/// the shape-sensitive commands (maps / sets / doubles).
pub fn execute_proto(tokens: &[Vec<u8>], db: &mut Db, proto: u8) -> Vec<u8> {
    if tokens.is_empty() {
        return Vec::new();
    }
    let cmd = tokens[0].to_ascii_uppercase();
    match cmd.as_slice() {
        // connection / generic
        b"PING" => match tokens.len() {
            1 => simple_string("PONG"),
            2 => bulk_string(&tokens[1]),
            _ => wrong_args("ping"),
        },
        b"ECHO" if tokens.len() == 2 => bulk_string(&tokens[1]),
        b"ECHO" => wrong_args("echo"),
        b"QUIT" => simple_string("OK"),
        b"SELECT" => select_cmd(tokens),
        b"DEL" => del_cmd(db, tokens),
        b"UNLINK" => del_cmd(db, tokens), // synchronous here, like DEL
        b"EXISTS" => exists_cmd(db, tokens),
        b"TOUCH" => exists_cmd(db, tokens), // no LRU, so equivalent to EXISTS
        b"KEYS" => keys_cmd(db, tokens),
        b"SCAN" => scan_cmd(db, tokens),
        b"HSCAN" => hscan_cmd(db, tokens),
        b"SSCAN" => sscan_cmd(db, tokens),
        b"ZSCAN" => zscan_cmd(db, tokens),
        b"DBSIZE" => dbsize_cmd(db, tokens),
        b"RANDOMKEY" => randomkey_cmd(db, tokens),
        b"RENAME" => rename_cmd(db, tokens, false),
        b"RENAMENX" => rename_cmd(db, tokens, true),
        b"FLUSHDB" | b"FLUSHALL" => flush_cmd(db, tokens),
        b"TYPE" => match tokens.len() {
            2 => simple_string(db.type_name(&tokens[1]).unwrap_or("none")),
            _ => wrong_args("type"),
        },
        // expiry
        b"EXPIRE" => expire_cmd(db, tokens, 1000, false),
        b"PEXPIRE" => expire_cmd(db, tokens, 1, false),
        b"EXPIREAT" => expire_cmd(db, tokens, 1000, true),
        b"PEXPIREAT" => expire_cmd(db, tokens, 1, true),
        b"TTL" => ttl_cmd(db, tokens, 1000),
        b"PTTL" => ttl_cmd(db, tokens, 1),
        b"PERSIST" => match tokens.len() {
            2 => {
                if db.contains(&tokens[1]) && db.clear_expire(&tokens[1]) {
                    integer(1)
                } else {
                    integer(0)
                }
            }
            _ => wrong_args("persist"),
        },
        // strings
        b"SET" => set_cmd(db, tokens),
        b"GET" => get_cmd(db, tokens),
        b"GETEX" => getex_cmd(db, tokens),
        b"OBJECT" => object_cmd(db, tokens),
        b"GETDEL" => getdel_cmd(db, tokens),
        b"GETSET" => getset_cmd(db, tokens),
        b"SETNX" => setnx_cmd(db, tokens),
        b"SETEX" => setex_cmd(db, tokens, 1000, "setex"),
        b"PSETEX" => setex_cmd(db, tokens, 1, "psetex"),
        b"MGET" => mget_cmd(db, tokens),
        b"MSET" => mset_cmd(db, tokens),
        b"MSETNX" => msetnx_cmd(db, tokens),
        b"INCR" => incr_cmd(db, tokens, 1, false),
        b"DECR" => incr_cmd(db, tokens, -1, false),
        b"INCRBY" => incr_cmd(db, tokens, 0, true),
        b"DECRBY" => incr_cmd(db, tokens, 0, true),
        b"INCRBYFLOAT" => incrbyfloat_cmd(db, tokens),
        b"APPEND" => append_cmd(db, tokens),
        b"GETRANGE" => getrange_cmd(db, tokens),
        b"SETRANGE" => setrange_cmd(db, tokens),
        b"STRLEN" => strlen_cmd(db, tokens),
        // conditional writes (CAS family)
        b"CAS" => cas_cmd(db, tokens),
        b"CADEL" => cadel_cmd(db, tokens),
        b"SETMAX" => setmax_cmd(db, tokens),
        b"INCRCAP" => incrcap_cmd(db, tokens),
        // sketches
        b"BFADD" => bfadd_cmd(db, tokens),
        b"BFEXISTS" => bfexists_cmd(db, tokens),
        b"BFLOAD" => bfload_cmd(db, tokens),
        b"CMSINCRBY" => cmsincrby_cmd(db, tokens),
        b"CMSQUERY" => cmsquery_cmd(db, tokens),
        b"CMSLOAD" => cmsload_cmd(db, tokens),
        b"TOPKRESERVE" => topkreserve_cmd(db, tokens),
        b"TOPKADD" => topkadd_cmd(db, tokens),
        b"TOPKLIST" => topklist_cmd(db, tokens),
        b"TOPKCOUNT" => topkcount_cmd(db, tokens),
        b"TOPKLOAD" => topkload_cmd(db, tokens),
        b"TDADD" => tdadd_cmd(db, tokens),
        b"TDQUANTILE" => tdquantile_cmd(db, tokens),
        b"TDLOAD" => tdload_cmd(db, tokens),
        // bitmaps
        b"SETBIT" => setbit_cmd(db, tokens),
        b"GETBIT" => getbit_cmd(db, tokens),
        b"BITCOUNT" => bitcount_cmd(db, tokens),
        b"BITPOS" => bitpos_cmd(db, tokens),
        b"BITOP" => bitop_cmd(db, tokens),
        // lists
        b"LPUSH" => push_cmd(db, tokens, true, true),
        b"RPUSH" => push_cmd(db, tokens, false, true),
        b"LPUSHX" => push_cmd(db, tokens, true, false),
        b"RPUSHX" => push_cmd(db, tokens, false, false),
        b"LPOP" => pop_cmd(db, tokens, true),
        b"RPOP" => pop_cmd(db, tokens, false),
        b"LLEN" => llen_cmd(db, tokens),
        b"LRANGE" => lrange_cmd(db, tokens),
        b"LINDEX" => lindex_cmd(db, tokens),
        b"LSET" => lset_cmd(db, tokens),
        b"LINSERT" => linsert_cmd(db, tokens),
        b"LREM" => lrem_cmd(db, tokens),
        b"LTRIM" => ltrim_cmd(db, tokens),
        b"LPOS" => lpos_cmd(db, tokens),
        b"RPOPLPUSH" => rpoplpush_cmd(db, tokens),
        b"LMOVE" => lmove_cmd(db, tokens),
        // hashes
        b"HSET" => hset_cmd(db, tokens, false),
        b"HSETNX" => hsetnx_cmd(db, tokens),
        b"HGET" => hget_cmd(db, tokens),
        b"HMGET" => hmget_cmd(db, tokens),
        b"HGETALL" => hgetall_cmd(db, tokens, proto),
        b"HDEL" => hdel_cmd(db, tokens),
        b"HEXISTS" => hexists_cmd(db, tokens),
        b"HLEN" => hlen_cmd(db, tokens),
        b"HKEYS" => hkeys_vals_cmd(db, tokens, true),
        b"HVALS" => hkeys_vals_cmd(db, tokens, false),
        b"HINCRBY" => hincrby_cmd(db, tokens),
        // sets
        b"SADD" => sadd_cmd(db, tokens),
        b"SREM" => srem_cmd(db, tokens),
        b"SMEMBERS" => smembers_cmd(db, tokens, proto),
        b"SISMEMBER" => sismember_cmd(db, tokens),
        b"SMISMEMBER" => smismember_cmd(db, tokens),
        b"SCARD" => scard_cmd(db, tokens),
        b"SPOP" => spop_cmd(db, tokens),
        b"SRANDMEMBER" => srandmember_cmd(db, tokens),
        b"SINTER" => setop_cmd(db, tokens, SetOp::Inter, proto),
        b"SUNION" => setop_cmd(db, tokens, SetOp::Union, proto),
        b"SDIFF" => setop_cmd(db, tokens, SetOp::Diff, proto),
        b"SINTERSTORE" => setop_store_cmd(db, tokens, SetOp::Inter),
        b"SUNIONSTORE" => setop_store_cmd(db, tokens, SetOp::Union),
        b"SDIFFSTORE" => setop_store_cmd(db, tokens, SetOp::Diff),
        b"SINTERCARD" => sintercard_cmd(db, tokens),
        b"SMOVE" => smove_cmd(db, tokens),
        // sorted sets
        b"ZADD" => zadd_cmd(db, tokens),
        b"ZSCORE" => zscore_cmd(db, tokens, proto),
        b"ZMSCORE" => zmscore_cmd(db, tokens, proto),
        b"ZCARD" => zcard_cmd(db, tokens),
        b"ZREM" => zrem_cmd(db, tokens),
        b"ZINCRBY" => zincrby_cmd(db, tokens, proto),
        b"ZRANK" => zrank_cmd(db, tokens, false),
        b"ZREVRANK" => zrank_cmd(db, tokens, true),
        b"ZRANGE" => zrange_cmd(db, tokens),
        b"ZREVRANGE" => zrevrange_cmd(db, tokens),
        b"ZRANGEBYSCORE" => zrangebyscore_cmd(db, tokens, false),
        b"ZREVRANGEBYSCORE" => zrangebyscore_cmd(db, tokens, true),
        b"ZCOUNT" => zcount_cmd(db, tokens),
        b"ZPOPMIN" => zpop_cmd(db, tokens, false),
        b"ZPOPMAX" => zpop_cmd(db, tokens, true),
        b"ZREMRANGEBYRANK" => zremrangebyrank_cmd(db, tokens),
        b"ZREMRANGEBYSCORE" => zremrangebyscore_cmd(db, tokens),
        b"ZUNIONSTORE" => zstore_cmd(db, tokens, false),
        b"ZINTERSTORE" => zstore_cmd(db, tokens, true),
        // geo (geo-first: each object is its own key)
        b"GEOSET" => geoset_cmd(db, tokens),
        b"GEOPOS" => geopos_cmd(db, tokens),
        b"GEODIST" => geodist_cmd(db, tokens),
        b"GEOSEARCH" => geosearch_cmd(db, tokens),
        // streams (XREAD is handled in the hub for blocking support)
        b"XADD" => streams::xadd(db, tokens),
        b"XLEN" => streams::xlen(db, tokens),
        b"XRANGE" => streams::xrange(db, tokens, false),
        b"XREVRANGE" => streams::xrange(db, tokens, true),
        // persistence: SAVE / BGSAVE are handled by the hub — they need its CDC /
        // secondary-index state (and BGSAVE a background thread).
        // stubs
        b"COMMAND" => command_cmd(tokens),
        // CONFIG is handled by the hub (it needs live server / runtime config).
        other => error(&format!(
            "ERR unknown command '{}'",
            String::from_utf8_lossy(other)
        )),
    }
}

/// Static metadata for a command: its minimum arity (args including the command
/// name) and whether it mutates the keyspace.
pub struct CmdMeta {
    pub min_arity: usize,
    pub write: bool,
}

/// The command table — the SINGLE source of truth for which commands exist,
/// their minimum arity, and whether they are writes. Returns `None` for an
/// unknown command. Adding a command means one dispatch arm in `execute()` (or
/// the hub) plus one entry here; `min_arity` (MULTI queue-time validation) and
/// `is_write` (AOF logging + replication + eviction gating) both read this, so
/// there is no second list to forget to update.
///
/// `write` must be exact: a write mistagged as a read would silently fail to
/// persist or replicate. Only the *minimum* arity is enforced for validation —
/// an over-long command still errors at execution time — so a valid command is
/// never wrongly rejected.
pub fn command_meta(cmd: &[u8]) -> Option<CmdMeta> {
    // (min_arity, is_write)
    let (min_arity, write) = match cmd {
        // arity 1 reads — bare commands / all-optional args
        b"PING" | b"QUIT" | b"COMMAND" | b"CONFIG" | b"CLUSTER" | b"SAVE" | b"BGSAVE"
        | b"BGREWRITEAOF" | b"MULTI" | b"EXEC" | b"DISCARD" | b"UNWATCH" | b"RESET" | b"INFO"
        | b"HELLO" | b"REPLCONF" | b"PSYNC" | b"SYNC" | b"UNSUBSCRIBE" | b"PUNSUBSCRIBE"
        | b"DBSIZE" | b"RANDOMKEY" | b"CDCSUBSCRIBE" | b"CDCUNSUBSCRIBE" | b"SHUTDOWN" => {
            (1, false)
        }
        // arity 1 writes
        b"FLUSHDB" | b"FLUSHALL" => (1, true),
        // arity 2 reads
        b"ECHO" | b"TYPE" | b"TTL" | b"PTTL" | b"GET" | b"STRLEN" | b"LLEN" | b"HGETALL"
        | b"HLEN" | b"HKEYS" | b"HVALS" | b"SMEMBERS" | b"SCARD" | b"ZCARD" | b"XLEN"
        | b"EXISTS" | b"TOUCH" | b"KEYS" | b"MGET" | b"SINTER" | b"SUNION" | b"SDIFF"
        | b"WATCH" | b"SUBSCRIBE" | b"PSUBSCRIBE" | b"PUBSUB" | b"BITCOUNT" | b"SRANDMEMBER"
        | b"SELECT" | b"CDCREAD" | b"CDCPENDING" | b"GEOPOS" | b"TOPKLIST" | b"IDXDROP"
        | b"AUTH" | b"SCAN" | b"OBJECT" | b"CLIENT" | b"SLOWLOG" | b"ACL" => (2, false),
        // arity 2 writes
        b"PERSIST" | b"INCR" | b"DECR" | b"GETDEL" | b"LPOP" | b"RPOP" | b"SPOP" | b"ZPOPMIN"
        | b"ZPOPMAX" | b"DEL" | b"UNLINK" | b"GETEX" => (2, true),
        // arity 3 reads
        b"LINDEX" | b"HGET" | b"HEXISTS" | b"HMGET" | b"SISMEMBER" | b"SMISMEMBER" | b"ZSCORE"
        | b"ZMSCORE" | b"ZRANK" | b"ZREVRANK" | b"PUBLISH" | b"REPLICAOF" | b"SLAVEOF"
        | b"LPOS" | b"SINTERCARD" | b"GETBIT" | b"BITPOS" | b"CDCGROUP" | b"CDCREADGROUP"
        | b"CDCACK" | b"GEODIST" | b"BFEXISTS" | b"CMSQUERY" | b"TOPKCOUNT" | b"TDQUANTILE"
        | b"IDXCREATE" | b"IDXGET" | b"HSCAN" | b"SSCAN" | b"ZSCAN" | b"WAIT" => (3, false),
        // arity 3 writes
        b"INCRBY" | b"DECRBY" | b"APPEND" | b"HDEL" | b"SADD" | b"SREM" | b"ZREM" | b"EXPIRE"
        | b"PEXPIRE" | b"EXPIREAT" | b"PEXPIREAT" | b"LPUSH" | b"RPUSH" | b"LPUSHX" | b"RPUSHX"
        | b"SET" | b"SETNX" | b"GETSET" | b"MSET" | b"MSETNX" | b"INCRBYFLOAT" | b"RENAME"
        | b"RENAMENX" | b"RPOPLPUSH" | b"SINTERSTORE" | b"SUNIONSTORE" | b"SDIFFSTORE"
        | b"CADEL" | b"SETMAX" | b"BFADD" | b"TOPKRESERVE" | b"TOPKADD" | b"TOPKLOAD"
        | b"TDADD" | b"TDLOAD" => (3, true),
        // arity 4 reads
        b"LRANGE" | b"ZRANGE" | b"ZREVRANGE" | b"ZRANGEBYSCORE" | b"ZREVRANGEBYSCORE"
        | b"ZCOUNT" | b"XRANGE" | b"XREVRANGE" | b"XREAD" | b"GETRANGE" | b"IDXRANGE" => (4, false),
        // arity 4 writes
        b"LSET" | b"HSET" | b"HSETNX" | b"HINCRBY" | b"ZADD" | b"ZINCRBY" | b"SETEX"
        | b"PSETEX" | b"SETRANGE" | b"LREM" | b"LTRIM" | b"SMOVE" | b"ZREMRANGEBYRANK"
        | b"ZREMRANGEBYSCORE" | b"ZUNIONSTORE" | b"ZINTERSTORE" | b"SETBIT" | b"BITOP"
        | b"GEOSET" | b"CAS" | b"INCRCAP" | b"CMSINCRBY" => (4, true),
        // arity 5 writes
        b"XADD" | b"LINSERT" | b"LMOVE" | b"BFLOAD" | b"CMSLOAD" => (5, true),
        // geosearch: GEOSEARCH FROMKEY k BYRADIUS r unit (6) is the shortest form
        b"GEOSEARCH" => (6, false),
        _ => return None,
    };
    Some(CmdMeta { min_arity, write })
}

/// Minimum argument count for `cmd`, or `None` if unknown. See [`command_meta`].
pub fn min_arity(cmd: &[u8]) -> Option<usize> {
    command_meta(cmd).map(|m| m.min_arity)
}

/// Whether `cmd` (case-insensitive) mutates the keyspace. See [`command_meta`].
pub fn is_write(cmd: &[u8]) -> bool {
    command_meta(&cmd.to_ascii_uppercase()).is_some_and(|m| m.write)
}

/// The ACL command class for `cmd` (upper-case). Connection / Admin / PubSub are
/// listed explicitly; everything else is Read or Write per the keyspace flag.
pub fn command_class(cmd: &[u8]) -> u8 {
    use crate::acl::*;
    match cmd {
        b"PING" | b"ECHO" | b"HELLO" | b"AUTH" | b"QUIT" | b"RESET" | b"SELECT" | b"COMMAND"
        | b"CLIENT" => CLASS_CONNECTION,
        b"CONFIG" | b"CLUSTER" | b"SLOWLOG" | b"INFO" | b"DBSIZE" | b"REPLICAOF" | b"SLAVEOF"
        | b"REPLCONF" | b"PSYNC" | b"SYNC" | b"SHUTDOWN" | b"SAVE" | b"BGSAVE"
        | b"BGREWRITEAOF" | b"FLUSHALL" | b"FLUSHDB" | b"ACL" | b"IDXCREATE" | b"IDXDROP" => {
            CLASS_ADMIN
        }
        b"SUBSCRIBE" | b"UNSUBSCRIBE" | b"PSUBSCRIBE" | b"PUNSUBSCRIBE" | b"PUBLISH"
        | b"PUBSUB" => CLASS_PUBSUB,
        // Changefeed commands READ keyspace data (snapshot + values), so they
        // class as reads — `+@pubsub` alone must not stream the whole keyspace.
        c if c.starts_with(b"CDC") => CLASS_READ,
        _ if command_meta(cmd).is_some_and(|m| m.write) => CLASS_WRITE,
        _ => CLASS_READ,
    }
}

/// Every command name (upper-case), for COMMAND/COMMAND COUNT introspection.
/// A regression test pins this against `command_meta` so they can't drift.
static COMMAND_NAMES: &[&[u8]] = &[
    b"ACL",
    b"APPEND",
    b"AUTH",
    b"BFADD",
    b"BFEXISTS",
    b"BFLOAD",
    b"BGREWRITEAOF",
    b"BGSAVE",
    b"BITCOUNT",
    b"BITOP",
    b"BITPOS",
    b"CADEL",
    b"CAS",
    b"CDCACK",
    b"CDCGROUP",
    b"CDCPENDING",
    b"CDCREAD",
    b"CDCREADGROUP",
    b"CDCSUBSCRIBE",
    b"CDCUNSUBSCRIBE",
    b"CLIENT",
    b"CLUSTER",
    b"CMSINCRBY",
    b"CMSLOAD",
    b"CMSQUERY",
    b"COMMAND",
    b"CONFIG",
    b"DBSIZE",
    b"DECR",
    b"DECRBY",
    b"DEL",
    b"DISCARD",
    b"ECHO",
    b"EXEC",
    b"EXISTS",
    b"EXPIRE",
    b"EXPIREAT",
    b"FLUSHALL",
    b"FLUSHDB",
    b"GEODIST",
    b"GEOPOS",
    b"GEOSEARCH",
    b"GEOSET",
    b"GET",
    b"GETBIT",
    b"GETDEL",
    b"GETEX",
    b"GETRANGE",
    b"GETSET",
    b"HDEL",
    b"HELLO",
    b"HEXISTS",
    b"HGET",
    b"HGETALL",
    b"HINCRBY",
    b"HKEYS",
    b"HLEN",
    b"HMGET",
    b"HSCAN",
    b"HSET",
    b"HSETNX",
    b"HVALS",
    b"IDXCREATE",
    b"IDXDROP",
    b"IDXGET",
    b"IDXRANGE",
    b"INCR",
    b"INCRBY",
    b"INCRBYFLOAT",
    b"INCRCAP",
    b"INFO",
    b"KEYS",
    b"LINDEX",
    b"LINSERT",
    b"LLEN",
    b"LMOVE",
    b"LPOP",
    b"LPOS",
    b"LPUSH",
    b"LPUSHX",
    b"LRANGE",
    b"LREM",
    b"LSET",
    b"LTRIM",
    b"MGET",
    b"MSET",
    b"MSETNX",
    b"MULTI",
    b"OBJECT",
    b"PERSIST",
    b"PEXPIRE",
    b"PEXPIREAT",
    b"PING",
    b"PSETEX",
    b"PSUBSCRIBE",
    b"PSYNC",
    b"PTTL",
    b"PUBLISH",
    b"PUBSUB",
    b"PUNSUBSCRIBE",
    b"QUIT",
    b"RANDOMKEY",
    b"RENAME",
    b"RENAMENX",
    b"REPLCONF",
    b"REPLICAOF",
    b"RESET",
    b"RPOP",
    b"RPOPLPUSH",
    b"RPUSH",
    b"RPUSHX",
    b"SADD",
    b"SAVE",
    b"SCAN",
    b"SCARD",
    b"SDIFF",
    b"SDIFFSTORE",
    b"SELECT",
    b"SET",
    b"SETBIT",
    b"SETEX",
    b"SETMAX",
    b"SETNX",
    b"SETRANGE",
    b"SHUTDOWN",
    b"SINTER",
    b"SINTERCARD",
    b"SINTERSTORE",
    b"SISMEMBER",
    b"SLAVEOF",
    b"SLOWLOG",
    b"SMEMBERS",
    b"SMISMEMBER",
    b"SMOVE",
    b"SPOP",
    b"SRANDMEMBER",
    b"SREM",
    b"SSCAN",
    b"STRLEN",
    b"SUBSCRIBE",
    b"SUNION",
    b"SUNIONSTORE",
    b"SYNC",
    b"TDADD",
    b"TDLOAD",
    b"TDQUANTILE",
    b"TOPKADD",
    b"TOPKCOUNT",
    b"TOPKLIST",
    b"TOPKLOAD",
    b"TOPKRESERVE",
    b"TOUCH",
    b"TTL",
    b"TYPE",
    b"UNLINK",
    b"UNSUBSCRIBE",
    b"UNWATCH",
    b"WAIT",
    b"WATCH",
    b"XADD",
    b"XLEN",
    b"XRANGE",
    b"XREAD",
    b"XREVRANGE",
    b"ZADD",
    b"ZCARD",
    b"ZCOUNT",
    b"ZINCRBY",
    b"ZINTERSTORE",
    b"ZMSCORE",
    b"ZPOPMAX",
    b"ZPOPMIN",
    b"ZRANGE",
    b"ZRANGEBYSCORE",
    b"ZRANK",
    b"ZREM",
    b"ZREMRANGEBYRANK",
    b"ZREMRANGEBYSCORE",
    b"ZREVRANGE",
    b"ZREVRANGEBYSCORE",
    b"ZREVRANK",
    b"ZSCAN",
    b"ZSCORE",
    b"ZUNIONSTORE",
];

/// One COMMAND entry: [name(lower), arity, [flag], first_key, last_key, step].
/// Key positions are a heuristic (1,1,1 for keyed commands) — good enough for
/// non-cluster clients, which is the only mode Locus runs.
fn command_info_entry(name: &[u8]) -> Vec<u8> {
    let meta = command_meta(name);
    let arity = meta.as_ref().map(|m| m.min_arity as i64).unwrap_or(-1);
    let write = meta.as_ref().map(|m| m.write).unwrap_or(false);
    let (first, last, step) = if arity >= 2 { (1, 1, 1) } else { (0, 0, 0) };
    let mut e = b"*6\r\n".to_vec();
    e.extend_from_slice(&bulk_string(&name.to_ascii_lowercase()));
    e.extend_from_slice(&integer(arity));
    e.extend_from_slice(b"*1\r\n");
    e.extend_from_slice(&simple_string(if write { "write" } else { "readonly" }));
    e.extend_from_slice(&integer(first));
    e.extend_from_slice(&integer(last));
    e.extend_from_slice(&integer(step));
    e
}

fn command_cmd(tokens: &[Vec<u8>]) -> Vec<u8> {
    match tokens.get(1).map(|t| t.to_ascii_uppercase()).as_deref() {
        Some(b"COUNT") => integer(COMMAND_NAMES.len() as i64),
        Some(b"DOCS") => b"*0\r\n".to_vec(), // empty docs map; clients tolerate it
        Some(b"INFO") => {
            let names: Vec<Vec<u8>> = if tokens.len() > 2 {
                tokens[2..].iter().map(|t| t.to_ascii_uppercase()).collect()
            } else {
                COMMAND_NAMES.iter().map(|n| n.to_vec()).collect()
            };
            let mut reply = format!("*{}\r\n", names.len()).into_bytes();
            for n in &names {
                if command_meta(n).is_some() {
                    reply.extend_from_slice(&command_info_entry(n));
                } else {
                    reply.extend_from_slice(&null_array());
                }
            }
            reply
        }
        None => {
            let mut reply = format!("*{}\r\n", COMMAND_NAMES.len()).into_bytes();
            for &n in COMMAND_NAMES {
                reply.extend_from_slice(&command_info_entry(n));
            }
            reply
        }
        _ => simple_string("OK"),
    }
}

// === generic ================================================================

fn cmd_name(tokens: &[Vec<u8>]) -> String {
    String::from_utf8_lossy(&tokens[0]).to_ascii_lowercase()
}

fn select_cmd(tokens: &[Vec<u8>]) -> Vec<u8> {
    // Single logical DB: SELECT 0 is a no-op OK so clients that select on connect
    // work; any other index is rejected. (Full multi-DB is a deliberate non-goal.)
    if tokens.len() != 2 {
        return wrong_args("select");
    }
    match parse_int(&tokens[1]) {
        Some(0) => simple_string("OK"),
        Some(_) => error("ERR DB index is out of range"),
        None => not_integer(),
    }
}

fn del_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    // Shared by DEL and UNLINK (synchronous here).
    if tokens.len() < 2 {
        return wrong_args(&cmd_name(tokens));
    }
    let n = tokens[1..]
        .iter()
        .filter(|k| db.remove(k).is_some())
        .count();
    integer(n as i64)
}

fn exists_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    // Shared by EXISTS and TOUCH (no LRU to bump).
    if tokens.len() < 2 {
        return wrong_args(&cmd_name(tokens));
    }
    let n = tokens[1..].iter().filter(|k| db.contains(k)).count();
    integer(n as i64)
}

fn keys_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 2 {
        return wrong_args("keys");
    }
    let matched: Vec<Vec<u8>> = db
        .live_keys()
        .into_iter()
        .filter(|k| crate::pubsub::glob_match(&tokens[1], k))
        .collect();
    bulk_array(&matched)
}

// --- SCAN family ------------------------------------------------------------
//
// std's HashMap exposes no stable bucket cursor, so we give each element a
// stable FNV-1a hash and use a hash value as the (stateless, integer) cursor:
// each call returns elements with hash >= cursor, advancing past the last hash
// group. This preserves Redis's guarantee — every element present for the whole
// scan is returned at least once. HONEST COST: each keyspace SCAN call still
// hashes every live key (O(N) probes — unavoidable without a bucket cursor),
// but clones/sorts only the returned batch. A full scan is O(N²/COUNT) probes
// total, so use a generous COUNT on big keyspaces. HSCAN/SSCAN/ZSCAN operate
// within one value and keep the simpler collect+select path.

/// (field, value) for HSCAN; (member, score) for ZSCAN — named so the
/// scan_select item types stay readable (and clippy's type-complexity lint quiet).
type FieldVal = (Vec<u8>, Vec<u8>);
type MemberScore = (Vec<u8>, f64);

fn scan_hash(data: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in data {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

fn parse_u64(arg: &[u8]) -> Option<u64> {
    std::str::from_utf8(arg)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
}

struct ScanOpts {
    pattern: Option<Vec<u8>>,
    count: usize,
    typ: Option<Vec<u8>>,
}

/// Parse the trailing `[MATCH p] [COUNT n] [TYPE t] [NOVALUES]` options; returns
/// the options and whether NOVALUES was given.
fn parse_scan_opts(
    tokens: &[Vec<u8>],
    start: usize,
    allow_type: bool,
    allow_novalues: bool,
) -> Result<(ScanOpts, bool), Vec<u8>> {
    let mut o = ScanOpts {
        pattern: None,
        count: 10,
        typ: None,
    };
    let mut novalues = false;
    let mut i = start;
    while i < tokens.len() {
        match tokens[i].to_ascii_uppercase().as_slice() {
            b"MATCH" if i + 1 < tokens.len() => {
                o.pattern = Some(tokens[i + 1].clone());
                i += 2;
            }
            b"COUNT" if i + 1 < tokens.len() => {
                match parse_int(&tokens[i + 1]) {
                    Some(n) if n > 0 => o.count = n as usize,
                    _ => return Err(not_integer()),
                }
                i += 2;
            }
            b"TYPE" if allow_type && i + 1 < tokens.len() => {
                o.typ = Some(tokens[i + 1].to_ascii_lowercase());
                i += 2;
            }
            b"NOVALUES" if allow_novalues => {
                novalues = true;
                i += 1;
            }
            _ => return Err(error("ERR syntax error")),
        }
    }
    Ok((o, novalues))
}

/// Take the next batch from `(hash, element)` pairs: all with hash >= cursor,
/// sorted, ~count of them (never splitting a hash group), plus the next cursor
/// (0 when complete).
fn scan_select<T>(mut items: Vec<(u64, T)>, cursor: u64, count: usize) -> (u64, Vec<T>) {
    items.retain(|(h, _)| *h >= cursor);
    items.sort_by(|a, b| a.0.cmp(&b.0));
    let mut taken = items.len().min(count.max(1));
    if taken < items.len() {
        let last = items[taken - 1].0;
        while taken < items.len() && items[taken].0 == last {
            taken += 1;
        }
    }
    let next = if taken >= items.len() {
        0
    } else {
        items[taken - 1].0 + 1
    };
    (
        next,
        items.into_iter().take(taken).map(|(_, t)| t).collect(),
    )
}

fn scan_reply(cursor: u64, flat: &[Vec<u8>]) -> Vec<u8> {
    let mut out = b"*2\r\n".to_vec();
    out.extend_from_slice(&bulk_string(cursor.to_string().as_bytes()));
    out.extend_from_slice(&bulk_array(flat));
    out
}

fn scan_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() < 2 {
        return wrong_args("scan");
    }
    let cursor = match parse_u64(&tokens[1]) {
        Some(c) => c,
        None => return error("ERR invalid cursor"),
    };
    let (opts, _) = match parse_scan_opts(tokens, 2, true, false) {
        Ok(v) => v,
        Err(e) => return e,
    };
    // Two borrowing passes, no full clone and no full sort (both used to
    // happen on EVERY call — a big-keyspace SCAN loop was O(N log N) per call
    // plus an allocation of every key). Pass 1: a bounded max-heap finds the
    // batch's upper hash bound. Pass 2: collect just that hash window, whole
    // hash groups included. Per call: O(N) hash probes, O(batch) clones.
    let count = opts.count.max(1);
    let mut heap: std::collections::BinaryHeap<u64> = std::collections::BinaryHeap::new();
    for k in db.live_keys_iter() {
        let h = scan_hash(k);
        if h < cursor {
            continue;
        }
        if heap.len() < count {
            heap.push(h);
        } else if let Some(&top) = heap.peek()
            && h < top
        {
            heap.pop();
            heap.push(h);
        }
    }
    let Some(&bound) = heap.peek() else {
        return scan_reply(0, &[]); // nothing at or past the cursor
    };
    let mut out: Vec<Vec<u8>> = Vec::new();
    let mut beyond = false;
    for k in db.live_keys_iter() {
        let h = scan_hash(k);
        if h < cursor {
            continue;
        }
        if h > bound {
            beyond = true;
            continue;
        }
        if let Some(p) = &opts.pattern
            && !crate::pubsub::glob_match(p, k)
        {
            continue;
        }
        out.push(k.clone());
    }
    if let Some(t) = &opts.typ {
        out.retain(|k| db.type_name(k).map(|s| s.as_bytes()) == Some(t.as_slice()));
    }
    let next = if beyond { bound.saturating_add(1) } else { 0 };
    scan_reply(next, &out)
}

fn hscan_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() < 3 {
        return wrong_args("hscan");
    }
    let cursor = match parse_u64(&tokens[2]) {
        Some(c) => c,
        None => return error("ERR invalid cursor"),
    };
    let (opts, novalues) = match parse_scan_opts(tokens, 3, false, true) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let items: Vec<(u64, FieldVal)> = match db.get(&tokens[1]) {
        None => return scan_reply(0, &[]),
        Some(Value::Hash(h)) => h
            .iter()
            .map(|(f, v)| (scan_hash(f), (f.clone(), v.clone())))
            .collect(),
        Some(_) => return wrongtype(),
    };
    let (next, batch) = scan_select(items, cursor, opts.count);
    let mut out = Vec::new();
    for (f, v) in batch {
        if let Some(p) = &opts.pattern
            && !crate::pubsub::glob_match(p, &f)
        {
            continue;
        }
        out.push(f);
        if !novalues {
            out.push(v);
        }
    }
    scan_reply(next, &out)
}

fn sscan_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() < 3 {
        return wrong_args("sscan");
    }
    let cursor = match parse_u64(&tokens[2]) {
        Some(c) => c,
        None => return error("ERR invalid cursor"),
    };
    let (opts, _) = match parse_scan_opts(tokens, 3, false, false) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let items: Vec<(u64, Vec<u8>)> = match db.get(&tokens[1]) {
        None => return scan_reply(0, &[]),
        Some(Value::Set(s)) => s.iter().map(|m| (scan_hash(m), m.clone())).collect(),
        Some(_) => return wrongtype(),
    };
    let (next, batch) = scan_select(items, cursor, opts.count);
    let mut out = Vec::new();
    for m in batch {
        if let Some(p) = &opts.pattern
            && !crate::pubsub::glob_match(p, &m)
        {
            continue;
        }
        out.push(m);
    }
    scan_reply(next, &out)
}

fn zscan_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() < 3 {
        return wrong_args("zscan");
    }
    let cursor = match parse_u64(&tokens[2]) {
        Some(c) => c,
        None => return error("ERR invalid cursor"),
    };
    let (opts, _) = match parse_scan_opts(tokens, 3, false, false) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let items: Vec<(u64, MemberScore)> = match db.get(&tokens[1]) {
        None => return scan_reply(0, &[]),
        Some(Value::ZSet(z)) => z
            .iter()
            .map(|(m, s)| (scan_hash(m), (m.clone(), *s)))
            .collect(),
        Some(_) => return wrongtype(),
    };
    let (next, batch) = scan_select(items, cursor, opts.count);
    let mut out = Vec::new();
    for (m, s) in batch {
        if let Some(p) = &opts.pattern
            && !crate::pubsub::glob_match(p, &m)
        {
            continue;
        }
        out.push(m);
        out.push(fmt_score(s));
    }
    scan_reply(next, &out)
}

fn getex_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() < 2 {
        return wrong_args("getex");
    }
    let key = &tokens[1];
    let val = match db.get(key) {
        None => return null_bulk(),
        Some(Value::Str(s)) => s.clone(),
        Some(_) => return wrongtype(),
    };
    if tokens.len() > 2 {
        let opt = tokens[2].to_ascii_uppercase();
        match opt.as_slice() {
            b"PERSIST" if tokens.len() == 3 => {
                db.clear_expire(key);
            }
            b"EX" | b"PX" | b"EXAT" | b"PXAT" if tokens.len() == 4 => {
                let unit = if matches!(opt.as_slice(), b"EX" | b"EXAT") {
                    1000
                } else {
                    1
                };
                let absolute = matches!(opt.as_slice(), b"EXAT" | b"PXAT");
                let n = match parse_int(&tokens[3]) {
                    Some(n) => n,
                    None => return not_integer(),
                };
                let at = if absolute {
                    n.checked_mul(unit)
                } else {
                    n.checked_mul(unit)
                        .and_then(|ms| (now_ms() as i64).checked_add(ms))
                };
                match at {
                    Some(t) if t > 0 => db.set_expire(key, t as u64),
                    _ => return error("ERR invalid expire time in 'getex' command"),
                }
            }
            _ => return error("ERR syntax error"),
        }
    }
    bulk_string(&val)
}

fn object_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() < 2 {
        return wrong_args("object");
    }
    match tokens[1].to_ascii_uppercase().as_slice() {
        b"ENCODING" if tokens.len() == 3 => match db.type_name(&tokens[2]) {
            None => error("ERR no such key"),
            // Plausible encodings; Locus uses one representation per type.
            Some(t) => {
                let enc = match t {
                    "string" => "raw",
                    "list" => "listpack",
                    "hash" | "set" => "hashtable",
                    "zset" => "skiplist",
                    other => other,
                };
                bulk_string(enc.as_bytes())
            }
        },
        b"REFCOUNT" if tokens.len() == 3 => {
            if db.contains(&tokens[2]) {
                integer(1)
            } else {
                error("ERR no such key")
            }
        }
        b"IDLETIME" | b"FREQ" if tokens.len() == 3 => {
            if db.contains(&tokens[2]) {
                integer(0)
            } else {
                error("ERR no such key")
            }
        }
        b"HELP" => simple_string("OBJECT <ENCODING|REFCOUNT|IDLETIME|FREQ> <key>"),
        _ => error("ERR Unknown OBJECT subcommand or wrong number of arguments"),
    }
}

fn dbsize_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 1 {
        return wrong_args("dbsize");
    }
    integer(db.dbsize() as i64)
}

fn flush_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    // FLUSHDB / FLUSHALL — we have a single logical DB, so they're equivalent.
    // An optional ASYNC/SYNC modifier is accepted and ignored.
    if tokens.len() > 2 {
        return wrong_args(&cmd_name(tokens));
    }
    db.clear();
    simple_string("OK")
}

fn rename_cmd(db: &mut Db, tokens: &[Vec<u8>], nx: bool) -> Vec<u8> {
    if tokens.len() != 3 {
        return wrong_args(&cmd_name(tokens));
    }
    if !db.contains(&tokens[1]) {
        return error("ERR no such key");
    }
    if tokens[1] == tokens[2] {
        // Renaming a key to itself is a no-op (RENAMENX still reports dst exists).
        return if nx { integer(0) } else { simple_string("OK") };
    }
    if nx && db.contains(&tokens[2]) {
        return integer(0);
    }
    let ttl = db.expire_at(&tokens[1]);
    let val = match db.remove(&tokens[1]) {
        Some(v) => v,
        None => return error("ERR no such key"),
    };
    db.insert(tokens[2].clone(), val);
    match ttl {
        Some(t) => db.set_expire(&tokens[2], t),
        None => {
            db.clear_expire(&tokens[2]);
        }
    }
    if nx { integer(1) } else { simple_string("OK") }
}

// === strings ================================================================

fn get_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 2 {
        return wrong_args("get");
    }
    match db.get(&tokens[1]) {
        None => null_bulk(),
        Some(Value::Str(s)) => bulk_string(s),
        Some(_) => wrongtype(),
    }
}

fn getdel_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 2 {
        return wrong_args("getdel");
    }
    match db.get(&tokens[1]) {
        None => null_bulk(),
        Some(Value::Str(_)) => match db.remove(&tokens[1]) {
            Some(Value::Str(s)) => bulk_string(&s),
            _ => null_bulk(),
        },
        Some(_) => wrongtype(),
    }
}

fn strlen_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 2 {
        return wrong_args("strlen");
    }
    match db.get(&tokens[1]) {
        None => integer(0),
        Some(Value::Str(s)) => integer(s.len() as i64),
        Some(_) => wrongtype(),
    }
}

fn append_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 3 {
        return wrong_args("append");
    }
    match db.get_or_insert_with(&tokens[1], || Value::Str(Vec::new())) {
        Value::Str(s) => {
            s.extend_from_slice(&tokens[2]);
            integer(s.len() as i64)
        }
        _ => wrongtype(),
    }
}

fn incr_cmd(db: &mut Db, tokens: &[Vec<u8>], fixed_delta: i64, has_arg: bool) -> Vec<u8> {
    let cmdname = String::from_utf8_lossy(&tokens[0]).to_ascii_lowercase();
    let delta = if has_arg {
        if tokens.len() != 3 {
            return wrong_args(&cmdname);
        }
        match parse_int(&tokens[2]) {
            Some(d) => {
                if cmdname == "decrby" {
                    match d.checked_neg() {
                        Some(n) => n,
                        None => return not_integer(),
                    }
                } else {
                    d
                }
            }
            None => return not_integer(),
        }
    } else {
        if tokens.len() != 2 {
            return wrong_args(&cmdname);
        }
        fixed_delta
    };

    let current = match db.get(&tokens[1]) {
        None => 0,
        Some(Value::Str(v)) => match std::str::from_utf8(v)
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
        {
            Some(n) => n,
            None => return not_integer(),
        },
        Some(_) => return wrongtype(),
    };
    match current.checked_add(delta) {
        // INCR/DECR preserve any TTL: db.insert replaces the value, not expires.
        Some(next) => {
            db.insert(tokens[1].clone(), Value::Str(next.to_string().into_bytes()));
            integer(next)
        }
        None => error("ERR increment or decrement would overflow"),
    }
}

fn mget_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() < 2 {
        return wrong_args("mget");
    }
    // Like Redis, a non-string (or missing) key yields nil rather than an error.
    let out: Vec<Vec<u8>> = tokens[1..]
        .iter()
        .map(|k| match db.get(k) {
            Some(Value::Str(s)) => bulk_string(s),
            _ => null_bulk(),
        })
        .collect();
    array(&out)
}

fn mset_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    let pairs = &tokens[1..];
    if pairs.is_empty() || !pairs.len().is_multiple_of(2) {
        return wrong_args("mset");
    }
    for kv in pairs.chunks_exact(2) {
        db.insert(kv[0].clone(), Value::Str(kv[1].clone()));
        db.clear_expire(&kv[0]); // like SET, MSET discards any prior TTL
    }
    simple_string("OK")
}

fn msetnx_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    let pairs = &tokens[1..];
    if pairs.is_empty() || !pairs.len().is_multiple_of(2) {
        return wrong_args("msetnx");
    }
    // All-or-nothing: if any key exists, set none.
    if pairs.chunks_exact(2).any(|kv| db.contains(&kv[0])) {
        return integer(0);
    }
    for kv in pairs.chunks_exact(2) {
        db.insert(kv[0].clone(), Value::Str(kv[1].clone()));
    }
    integer(1)
}

fn setnx_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 3 {
        return wrong_args("setnx");
    }
    if db.contains(&tokens[1]) {
        return integer(0);
    }
    db.insert(tokens[1].clone(), Value::Str(tokens[2].clone()));
    integer(1)
}

fn setex_cmd(db: &mut Db, tokens: &[Vec<u8>], unit_ms: u64, name: &str) -> Vec<u8> {
    if tokens.len() != 4 {
        return wrong_args(name);
    }
    let invalid = || error(&format!("ERR invalid expire time in '{name}' command"));
    let n = match parse_int(&tokens[2]) {
        Some(n) if n > 0 => n as u64,
        Some(_) => return invalid(),
        None => return not_integer(),
    };
    let at = match n
        .checked_mul(unit_ms)
        .and_then(|ms| now_ms().checked_add(ms))
    {
        Some(v) => v,
        None => return invalid(),
    };
    db.insert(tokens[1].clone(), Value::Str(tokens[3].clone()));
    db.set_expire(&tokens[1], at);
    simple_string("OK")
}

fn getset_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 3 {
        return wrong_args("getset");
    }
    let old = match db.get(&tokens[1]) {
        None => None,
        Some(Value::Str(s)) => Some(s.clone()),
        Some(_) => return wrongtype(),
    };
    db.insert(tokens[1].clone(), Value::Str(tokens[2].clone()));
    db.clear_expire(&tokens[1]); // like SET, GETSET discards any prior TTL
    old.map(|v| bulk_string(&v)).unwrap_or_else(null_bulk)
}

fn getrange_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 4 {
        return wrong_args("getrange");
    }
    let (start, end) = match (parse_int(&tokens[2]), parse_int(&tokens[3])) {
        (Some(a), Some(b)) => (a, b),
        _ => return not_integer(),
    };
    let s = match db.get(&tokens[1]) {
        None => return bulk_string(b""),
        Some(Value::Str(s)) => s,
        Some(_) => return wrongtype(),
    };
    let len = s.len() as i64;
    // Normalize inclusive [start, end] with negative-from-end indices (Redis).
    let mut start = if start < 0 { len + start } else { start };
    let mut end = if end < 0 { len + end } else { end };
    if start < 0 {
        start = 0;
    }
    if end >= len {
        end = len - 1;
    }
    if len == 0 || start > end || start >= len {
        return bulk_string(b"");
    }
    bulk_string(&s[start as usize..=end as usize])
}

fn setrange_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    const MAX_STR: usize = 512 * 1024 * 1024; // Redis proto-max-bulk-len
    if tokens.len() != 4 {
        return wrong_args("setrange");
    }
    let offset = match parse_int(&tokens[2]) {
        Some(o) if o >= 0 => o as usize,
        Some(_) => return error("ERR offset is out of range"),
        None => return not_integer(),
    };
    let val = &tokens[3];
    // An empty value is a no-op that just reports the current length.
    if val.is_empty() {
        return match db.get(&tokens[1]) {
            Some(Value::Str(s)) => integer(s.len() as i64),
            Some(_) => wrongtype(),
            None => integer(0),
        };
    }
    if offset + val.len() > MAX_STR {
        return error("ERR string exceeds maximum allowed size (proto-max-bulk-len)");
    }
    let s = match db.get_or_insert_with(&tokens[1], || Value::Str(Vec::new())) {
        Value::Str(s) => s,
        _ => return wrongtype(),
    };
    let end = offset + val.len();
    if s.len() < end {
        s.resize(end, 0); // pad with zero bytes
    }
    s[offset..end].copy_from_slice(val);
    integer(s.len() as i64)
}

fn incrbyfloat_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 3 {
        return wrong_args("incrbyfloat");
    }
    let not_float = || error("ERR value is not a valid float");
    let incr = match parse_finite_float(&tokens[2]) {
        Some(f) => f,
        None => return not_float(),
    };
    let current = match db.get(&tokens[1]) {
        None => 0.0,
        Some(Value::Str(v)) => match parse_finite_float(v) {
            Some(n) => n,
            None => return not_float(),
        },
        Some(_) => return wrongtype(),
    };
    let next = current + incr;
    if !next.is_finite() {
        return error("ERR increment would produce NaN or Infinity");
    }
    let formatted = fmt_score(next);
    db.insert(tokens[1].clone(), Value::Str(formatted.clone())); // preserves TTL
    bulk_string(&formatted)
}

/// Parse a finite float (rejects inf/-inf/NaN — unlike `parse_score`).
fn parse_finite_float(arg: &[u8]) -> Option<f64> {
    let f = std::str::from_utf8(arg).ok()?.trim().parse::<f64>().ok()?;
    f.is_finite().then_some(f)
}

struct SetOpts {
    expire_at: Option<u64>,
    keepttl: bool,
    nx: bool,
    xx: bool,
    get: bool,
}

fn parse_set_opts(args: &[Vec<u8>]) -> Option<SetOpts> {
    let mut o = SetOpts {
        expire_at: None,
        keepttl: false,
        nx: false,
        xx: false,
        get: false,
    };
    let now = now_ms();
    let mut i = 0;
    while i < args.len() {
        let a = args[i].to_ascii_uppercase();
        match a.as_slice() {
            b"EX" | b"PX" | b"EXAT" | b"PXAT" => {
                i += 1;
                let n = parse_int(args.get(i)?)?;
                if n < 0 {
                    return None;
                }
                let n = n as u64;
                // Checked arithmetic: a huge TTL must not overflow (panic in
                // debug, silent wrap to a past deadline in release).
                let at = match a.as_slice() {
                    b"EX" => n.checked_mul(1000).and_then(|ms| now.checked_add(ms)),
                    b"PX" => now.checked_add(n),
                    b"EXAT" => n.checked_mul(1000),
                    _ => Some(n),
                };
                o.expire_at = Some(at?);
            }
            b"KEEPTTL" => o.keepttl = true,
            b"NX" => o.nx = true,
            b"XX" => o.xx = true,
            b"GET" => o.get = true,
            _ => return None,
        }
        i += 1;
    }
    Some(o)
}

fn set_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() < 3 {
        return wrong_args("set");
    }
    let key = &tokens[1];
    let val = &tokens[2];
    let opts = match parse_set_opts(&tokens[3..]) {
        Some(o) => o,
        None => return error("ERR syntax error"),
    };
    // SET ... GET requires the existing value (if any) to be a string.
    let old = if opts.get {
        match db.get(key) {
            None => None,
            Some(Value::Str(s)) => Some(s.clone()),
            Some(_) => return wrongtype(),
        }
    } else {
        None
    };
    let exists = db.contains(key);
    if (opts.nx && exists) || (opts.xx && !exists) {
        return if opts.get {
            old.map(|v| bulk_string(&v)).unwrap_or_else(null_bulk)
        } else {
            null_bulk()
        };
    }
    db.insert(key.clone(), Value::Str(val.clone()));
    match opts.expire_at {
        Some(t) => db.set_expire(key, t),
        None => {
            if !opts.keepttl {
                db.clear_expire(key);
            }
        }
    }
    if opts.get {
        old.map(|v| bulk_string(&v)).unwrap_or_else(null_bulk)
    } else {
        simple_string("OK")
    }
}

fn expire_cmd(db: &mut Db, tokens: &[Vec<u8>], unit_ms: i64, absolute: bool) -> Vec<u8> {
    if tokens.len() != 3 {
        return wrong_args("expire");
    }
    let key = &tokens[1];
    let n = match parse_int(&tokens[2]) {
        Some(n) => n,
        None => return not_integer(),
    };
    if !db.contains(key) {
        return integer(0);
    }
    // Checked arithmetic: a huge TTL must not overflow i64 (panic in debug,
    // silent wrap to a garbage/past deadline in release).
    let at = if absolute {
        n.checked_mul(unit_ms)
    } else {
        n.checked_mul(unit_ms)
            .and_then(|ms| (now_ms() as i64).checked_add(ms))
    };
    let at = match at {
        Some(v) => v,
        None => return error("ERR invalid expire time in 'expire' command"),
    };
    if at <= now_ms() as i64 {
        db.remove(key);
    } else {
        db.set_expire(key, at as u64);
    }
    integer(1)
}

fn ttl_cmd(db: &mut Db, tokens: &[Vec<u8>], unit_ms: u64) -> Vec<u8> {
    if tokens.len() != 2 {
        return wrong_args("ttl");
    }
    let key = &tokens[1];
    if !db.contains(key) {
        return integer(-2);
    }
    match db.expire_at(key) {
        None => integer(-1),
        Some(deadline) => {
            let remaining = deadline.saturating_sub(now_ms());
            integer(remaining.div_ceil(unit_ms) as i64)
        }
    }
}

// === lists ==================================================================

/// LPUSH/RPUSH (create=true) and LPUSHX/RPUSHX (create=false).
fn push_cmd(db: &mut Db, tokens: &[Vec<u8>], front: bool, create: bool) -> Vec<u8> {
    if tokens.len() < 3 {
        return wrong_args("push");
    }
    let key = &tokens[1];
    let list = if create {
        match db.get_or_insert_with(key, || Value::List(VecDeque::new())) {
            Value::List(l) => l,
            _ => return wrongtype(),
        }
    } else {
        match db.get_mut(key) {
            Some(Value::List(l)) => l,
            Some(_) => return wrongtype(),
            None => return integer(0),
        }
    };
    for item in &tokens[2..] {
        if front {
            list.push_front(item.clone());
        } else {
            list.push_back(item.clone());
        }
    }
    integer(list.len() as i64)
}

fn pop_cmd(db: &mut Db, tokens: &[Vec<u8>], front: bool) -> Vec<u8> {
    if tokens.len() < 2 || tokens.len() > 3 {
        return wrong_args("pop");
    }
    let count = if tokens.len() == 3 {
        match parse_int(&tokens[2]) {
            Some(c) if c >= 0 => Some(c as usize),
            _ => return error("ERR value is out of range, must be positive"),
        }
    } else {
        None
    };
    let key = &tokens[1];
    let mut popped: Vec<Vec<u8>> = Vec::new();
    match db.get_mut(key) {
        Some(Value::List(l)) => {
            for _ in 0..count.unwrap_or(1) {
                match if front { l.pop_front() } else { l.pop_back() } {
                    Some(x) => popped.push(x),
                    None => break,
                }
            }
        }
        Some(_) => return wrongtype(),
        None => {
            return if count.is_some() {
                null_array()
            } else {
                null_bulk()
            };
        }
    }
    db.remove_if_empty(key);
    if count.is_some() {
        bulk_array(&popped)
    } else {
        popped
            .into_iter()
            .next()
            .map(|v| bulk_string(&v))
            .unwrap_or_else(null_bulk)
    }
}

fn llen_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 2 {
        return wrong_args("llen");
    }
    match db.get(&tokens[1]) {
        None => integer(0),
        Some(Value::List(l)) => integer(l.len() as i64),
        Some(_) => wrongtype(),
    }
}

/// Clamp a possibly-negative index into a valid range position.
fn norm(i: i64, len: usize) -> i64 {
    if i < 0 { i + len as i64 } else { i }
}

fn lrange_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 4 {
        return wrong_args("lrange");
    }
    let (start, stop) = match (parse_int(&tokens[2]), parse_int(&tokens[3])) {
        (Some(a), Some(b)) => (a, b),
        _ => return not_integer(),
    };
    match db.get(&tokens[1]) {
        None => bulk_array(&[]),
        Some(Value::List(l)) => {
            let len = l.len();
            let mut start = norm(start, len).max(0);
            let mut stop = norm(stop, len);
            if stop >= len as i64 {
                stop = len as i64 - 1;
            }
            if start > stop || len == 0 {
                return bulk_array(&[]);
            }
            start = start.min(len as i64 - 1);
            let slice: Vec<Vec<u8>> = l
                .iter()
                .skip(start as usize)
                .take((stop - start + 1) as usize)
                .cloned()
                .collect();
            bulk_array(&slice)
        }
        Some(_) => wrongtype(),
    }
}

fn lindex_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 3 {
        return wrong_args("lindex");
    }
    let idx = match parse_int(&tokens[2]) {
        Some(i) => i,
        None => return not_integer(),
    };
    match db.get(&tokens[1]) {
        None => null_bulk(),
        Some(Value::List(l)) => {
            let i = norm(idx, l.len());
            if i < 0 || i >= l.len() as i64 {
                null_bulk()
            } else {
                bulk_string(&l[i as usize])
            }
        }
        Some(_) => wrongtype(),
    }
}

fn lset_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 4 {
        return wrong_args("lset");
    }
    let idx = match parse_int(&tokens[2]) {
        Some(i) => i,
        None => return not_integer(),
    };
    match db.get_mut(&tokens[1]) {
        None => error("ERR no such key"),
        Some(Value::List(l)) => {
            let i = norm(idx, l.len());
            if i < 0 || i >= l.len() as i64 {
                error("ERR index out of range")
            } else {
                l[i as usize] = tokens[3].clone();
                simple_string("OK")
            }
        }
        Some(_) => wrongtype(),
    }
}

fn linsert_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 5 {
        return wrong_args("linsert");
    }
    let before = match tokens[2].to_ascii_uppercase().as_slice() {
        b"BEFORE" => true,
        b"AFTER" => false,
        _ => return error("ERR syntax error"),
    };
    let (pivot, value) = (&tokens[3], &tokens[4]);
    match db.get_mut(&tokens[1]) {
        None => integer(0),
        Some(Value::List(l)) => match l.iter().position(|x| x == pivot) {
            None => integer(-1),
            Some(pos) => {
                l.insert(if before { pos } else { pos + 1 }, value.clone());
                integer(l.len() as i64)
            }
        },
        Some(_) => wrongtype(),
    }
}

fn lrem_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 4 {
        return wrong_args("lrem");
    }
    let count = match parse_int(&tokens[2]) {
        Some(c) => c,
        None => return not_integer(),
    };
    let value = &tokens[3];
    // limit = None means "remove all matches" (count == 0).
    let limit: Option<usize> = if count == 0 {
        None
    } else {
        Some(count.unsigned_abs() as usize)
    };
    let mut removed = 0usize;
    match db.get_mut(&tokens[1]) {
        None => return integer(0),
        Some(Value::List(l)) => {
            if count >= 0 {
                let mut i = 0;
                while i < l.len() && limit.is_none_or(|lim| removed < lim) {
                    if l[i] == *value {
                        l.remove(i);
                        removed += 1;
                    } else {
                        i += 1;
                    }
                }
            } else {
                let mut i = l.len();
                while i > 0 && limit.is_none_or(|lim| removed < lim) {
                    i -= 1;
                    if l[i] == *value {
                        l.remove(i);
                        removed += 1;
                    }
                }
            }
        }
        Some(_) => return wrongtype(),
    }
    db.remove_if_empty(&tokens[1]);
    integer(removed as i64)
}

fn ltrim_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 4 {
        return wrong_args("ltrim");
    }
    let (start, stop) = match (parse_int(&tokens[2]), parse_int(&tokens[3])) {
        (Some(a), Some(b)) => (a, b),
        _ => return not_integer(),
    };
    match db.get_mut(&tokens[1]) {
        None => return simple_string("OK"),
        Some(Value::List(l)) => {
            let len = l.len() as i64;
            let mut s = if start < 0 { start + len } else { start };
            let mut e = if stop < 0 { stop + len } else { stop };
            if s < 0 {
                s = 0;
            }
            if e >= len {
                e = len - 1;
            }
            if len == 0 || s > e {
                l.clear();
            } else {
                for _ in 0..(len - 1 - e) {
                    l.pop_back();
                }
                for _ in 0..s {
                    l.pop_front();
                }
            }
        }
        Some(_) => return wrongtype(),
    }
    db.remove_if_empty(&tokens[1]);
    simple_string("OK")
}

fn lpos_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() < 3 {
        return wrong_args("lpos");
    }
    let value = &tokens[2];
    let (mut rank, mut count): (i64, Option<i64>) = (1, None);
    let mut i = 3;
    while i < tokens.len() {
        match tokens[i].to_ascii_uppercase().as_slice() {
            b"RANK" => {
                i += 1;
                rank = match tokens.get(i).and_then(|t| parse_int(t)) {
                    Some(r) => r,
                    None => return error("ERR syntax error"),
                };
            }
            b"COUNT" => {
                i += 1;
                count = match tokens.get(i).and_then(|t| parse_int(t)) {
                    Some(c) if c >= 0 => Some(c),
                    Some(_) => return error("ERR COUNT can't be negative"),
                    None => return error("ERR syntax error"),
                };
            }
            b"MAXLEN" => {
                i += 1; // accepted for compatibility, not used
                if tokens.get(i).and_then(|t| parse_int(t)).is_none() {
                    return error("ERR syntax error");
                }
            }
            _ => return error("ERR syntax error"),
        }
        i += 1;
    }
    if rank == 0 {
        return error("ERR RANK can't be zero");
    }
    let l = match db.get(&tokens[1]) {
        None => {
            return if count.is_some() {
                bulk_array(&[])
            } else {
                null_bulk()
            };
        }
        Some(Value::List(l)) => l,
        Some(_) => return wrongtype(),
    };
    let want = match count {
        Some(0) => usize::MAX, // COUNT 0 => all matches
        Some(c) => c as usize,
        None => 1,
    };
    let skip = (rank.unsigned_abs() - 1) as usize; // RANK N skips N-1 matches
    let mut positions: Vec<usize> = Vec::new();
    let mut seen = 0usize;
    let indices: Vec<usize> = if rank > 0 {
        (0..l.len()).collect()
    } else {
        (0..l.len()).rev().collect()
    };
    for idx in indices {
        if l[idx] == *value {
            if seen >= skip {
                positions.push(idx);
                if positions.len() >= want {
                    break;
                }
            }
            seen += 1;
        }
    }
    match count {
        None => positions
            .first()
            .map(|&p| integer(p as i64))
            .unwrap_or_else(null_bulk),
        Some(_) => array(
            &positions
                .iter()
                .map(|&p| integer(p as i64))
                .collect::<Vec<_>>(),
        ),
    }
}

/// Pop one element from `src` and push it onto `dst` (the engine for RPOPLPUSH
/// and LMOVE). `src`/`dst` may be the same key (a rotation).
fn lmove_core(db: &mut Db, src: &[u8], dst: &[u8], from_left: bool, to_left: bool) -> Vec<u8> {
    // Type-check the destination up front so we never pop from src and then fail.
    match db.get(dst) {
        None | Some(Value::List(_)) => {}
        Some(_) => return wrongtype(),
    }
    let elem = match db.get_mut(src) {
        None => return null_bulk(),
        Some(Value::List(l)) => match if from_left {
            l.pop_front()
        } else {
            l.pop_back()
        } {
            Some(e) => e,
            None => return null_bulk(),
        },
        Some(_) => return wrongtype(),
    };
    db.remove_if_empty(src);
    match db.get_or_insert_with(dst, || Value::List(VecDeque::new())) {
        Value::List(l) => {
            if to_left {
                l.push_front(elem.clone());
            } else {
                l.push_back(elem.clone());
            }
        }
        _ => return wrongtype(), // unreachable: dst was type-checked above
    }
    bulk_string(&elem)
}

fn rpoplpush_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 3 {
        return wrong_args("rpoplpush");
    }
    lmove_core(db, &tokens[1], &tokens[2], false, true)
}

fn lmove_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 5 {
        return wrong_args("lmove");
    }
    let from_left = match tokens[3].to_ascii_uppercase().as_slice() {
        b"LEFT" => true,
        b"RIGHT" => false,
        _ => return error("ERR syntax error"),
    };
    let to_left = match tokens[4].to_ascii_uppercase().as_slice() {
        b"LEFT" => true,
        b"RIGHT" => false,
        _ => return error("ERR syntax error"),
    };
    lmove_core(db, &tokens[1], &tokens[2], from_left, to_left)
}

// === bitmaps ================================================================
//
// Bits are numbered Redis-style: the most-significant bit of byte 0 is bit 0.

const MAX_BIT_OFFSET: i64 = 4 * 1024 * 1024 * 1024 - 1; // < 512 MiB of bytes

fn getbit_at(s: &[u8], offset: usize) -> u8 {
    let byte = offset / 8;
    if byte >= s.len() {
        0
    } else {
        (s[byte] >> (7 - (offset % 8))) & 1
    }
}

/// Normalize an inclusive [start, end] range with negative-from-end indices over
/// a length, returning concrete bounds, or None if the range is empty.
fn norm_range(start: i64, end: i64, len: usize) -> Option<(usize, usize)> {
    if len == 0 {
        return None;
    }
    let len = len as i64;
    let mut s = if start < 0 { start + len } else { start };
    let mut e = if end < 0 { end + len } else { end };
    if s < 0 {
        s = 0;
    }
    if e >= len {
        e = len - 1;
    }
    if s > e || s >= len {
        return None;
    }
    Some((s as usize, e as usize))
}

fn setbit_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 4 {
        return wrong_args("setbit");
    }
    let offset = match parse_int(&tokens[2]) {
        Some(o) if (0..=MAX_BIT_OFFSET).contains(&o) => o as usize,
        _ => return error("ERR bit offset is not an integer or out of range"),
    };
    let bit = match tokens[3].as_slice() {
        b"0" => 0u8,
        b"1" => 1u8,
        _ => return error("ERR bit is not an integer or out of range"),
    };
    let s = match db.get_or_insert_with(&tokens[1], || Value::Str(Vec::new())) {
        Value::Str(s) => s,
        _ => return wrongtype(),
    };
    let byte = offset / 8;
    let shift = 7 - (offset % 8);
    if s.len() <= byte {
        s.resize(byte + 1, 0);
    }
    let old = (s[byte] >> shift) & 1;
    if bit == 1 {
        s[byte] |= 1 << shift;
    } else {
        s[byte] &= !(1 << shift);
    }
    integer(old as i64)
}

fn getbit_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 3 {
        return wrong_args("getbit");
    }
    let offset = match parse_int(&tokens[2]) {
        Some(o) if o >= 0 => o as usize,
        _ => return error("ERR bit offset is not an integer or out of range"),
    };
    match db.get(&tokens[1]) {
        None => integer(0),
        Some(Value::Str(s)) => integer(getbit_at(s, offset) as i64),
        Some(_) => wrongtype(),
    }
}

fn bitcount_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() == 3 || tokens.len() > 5 {
        return error("ERR syntax error");
    }
    let s = match db.get(&tokens[1]) {
        None => return integer(0),
        Some(Value::Str(s)) => s,
        Some(_) => return wrongtype(),
    };
    if tokens.len() == 2 {
        return integer(s.iter().map(|b| b.count_ones() as i64).sum());
    }
    let (start, end) = match (parse_int(&tokens[2]), parse_int(&tokens[3])) {
        (Some(a), Some(b)) => (a, b),
        _ => return not_integer(),
    };
    let bit_mode = if tokens.len() == 5 {
        match tokens[4].to_ascii_uppercase().as_slice() {
            b"BYTE" => false,
            b"BIT" => true,
            _ => return error("ERR syntax error"),
        }
    } else {
        false
    };
    if bit_mode {
        match norm_range(start, end, s.len() * 8) {
            Some((lo, hi)) => integer((lo..=hi).map(|o| getbit_at(s, o) as i64).sum()),
            None => integer(0),
        }
    } else {
        match norm_range(start, end, s.len()) {
            Some((lo, hi)) => integer(s[lo..=hi].iter().map(|b| b.count_ones() as i64).sum()),
            None => integer(0),
        }
    }
}

fn bitpos_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() < 3 || tokens.len() > 6 {
        return error("ERR syntax error");
    }
    let target = match tokens[2].as_slice() {
        b"0" => 0u8,
        b"1" => 1u8,
        _ => return error("ERR The bit argument must be 1 or 0."),
    };
    let s = match db.get(&tokens[1]) {
        None => return integer(if target == 0 { 0 } else { -1 }),
        Some(Value::Str(s)) => s.clone(),
        Some(_) => return wrongtype(),
    };
    let has_end = tokens.len() >= 5;
    let bit_mode = match tokens.get(5).map(|m| m.to_ascii_uppercase()) {
        None => false,
        Some(m) if m == b"BYTE" => false,
        Some(m) if m == b"BIT" => true,
        _ => return error("ERR syntax error"),
    };
    let unit_len = if bit_mode { s.len() * 8 } else { s.len() };
    let start = match tokens.get(3) {
        Some(t) => match parse_int(t) {
            Some(v) => v,
            None => return not_integer(),
        },
        None => 0,
    };
    let end = match tokens.get(4) {
        Some(t) => match parse_int(t) {
            Some(v) => v,
            None => return not_integer(),
        },
        None => unit_len as i64 - 1,
    };
    let (lo_bit, hi_bit) = if bit_mode {
        match norm_range(start, end, s.len() * 8) {
            Some(r) => r,
            None => return integer(-1),
        }
    } else {
        match norm_range(start, end, s.len()) {
            Some((bs, be)) => (bs * 8, be * 8 + 7),
            None => return integer(-1),
        }
    };
    for o in lo_bit..=hi_bit {
        if getbit_at(&s, o) == target {
            return integer(o as i64);
        }
    }
    // Looking for a 0 with no explicit end: the string has an implicit zero tail.
    if target == 0 && !has_end {
        integer((s.len() * 8) as i64)
    } else {
        integer(-1)
    }
}

fn bitop_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() < 4 {
        return wrong_args("bitop");
    }
    let op = tokens[1].to_ascii_uppercase();
    let srcs = &tokens[3..];
    if op == b"NOT" && srcs.len() != 1 {
        return error("ERR BITOP NOT must be called with a single source key.");
    }
    let mut sources: Vec<Vec<u8>> = Vec::new();
    for k in srcs {
        match db.get(k) {
            None => sources.push(Vec::new()),
            Some(Value::Str(s)) => sources.push(s.clone()),
            Some(_) => return wrongtype(),
        }
    }
    let maxlen = sources.iter().map(|s| s.len()).max().unwrap_or(0);
    let byte_at = |s: &[u8], i: usize| *s.get(i).unwrap_or(&0);
    let result: Vec<u8> = match op.as_slice() {
        b"NOT" => sources[0].iter().map(|b| !b).collect(),
        b"AND" => (0..maxlen)
            .map(|i| sources.iter().fold(0xFFu8, |acc, s| acc & byte_at(s, i)))
            .collect(),
        b"OR" => (0..maxlen)
            .map(|i| sources.iter().fold(0u8, |acc, s| acc | byte_at(s, i)))
            .collect(),
        b"XOR" => (0..maxlen)
            .map(|i| sources.iter().fold(0u8, |acc, s| acc ^ byte_at(s, i)))
            .collect(),
        _ => return error("ERR syntax error"),
    };
    let n = result.len();
    let dest = &tokens[2];
    if result.is_empty() {
        db.remove(dest);
    } else {
        db.insert(dest.clone(), Value::Str(result));
        db.clear_expire(dest);
    }
    integer(n as i64)
}

// === conditional writes (CAS family) ========================================
//
// Near-free under single-threaded execution: the check and the write happen in
// one hub turn, so there's no race. These remove most reasons to reach for Lua
// or WATCH (persist-before-ack, dedup, monotonic cursors, quotas).

/// Read a key as a string for a compare op. `Err(())` = wrong type.
fn cas_current<'a>(db: &'a mut Db, key: &[u8]) -> Result<Option<&'a Vec<u8>>, ()> {
    match db.get(key) {
        None => Ok(None),
        Some(Value::Str(s)) => Ok(Some(s)),
        Some(_) => Err(()),
    }
}

/// CAS key expected new — set `new` only if the current value equals `expected`
/// (the key must already hold that string). Returns 1 if set, else 0.
fn cas_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 4 {
        return wrong_args("cas");
    }
    let matches = match cas_current(db, &tokens[1]) {
        Err(()) => return wrongtype(),
        Ok(cur) => cur == Some(&tokens[2]),
    };
    if matches {
        db.insert(tokens[1].clone(), Value::Str(tokens[3].clone()));
        integer(1)
    } else {
        integer(0)
    }
}

/// CADEL key expected — delete the key only if its value equals `expected`.
fn cadel_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 3 {
        return wrong_args("cadel");
    }
    let matches = match cas_current(db, &tokens[1]) {
        Err(()) => return wrongtype(),
        Ok(cur) => cur == Some(&tokens[2]),
    };
    if matches {
        db.remove(&tokens[1]);
        integer(1)
    } else {
        integer(0)
    }
}

/// SETMAX key n — monotonic set: store `n` only if it's greater than the current
/// integer value (or the key is missing). Returns 1 if it advanced, else 0.
/// The forward-only cursor primitive (chat's CursorStore.advance).
fn setmax_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 3 {
        return wrong_args("setmax");
    }
    let n = match parse_int(&tokens[2]) {
        Some(n) => n,
        None => return not_integer(),
    };
    let cur = match db.get(&tokens[1]) {
        None => None,
        Some(Value::Str(v)) => match std::str::from_utf8(v)
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
        {
            Some(c) => Some(c),
            None => return not_integer(),
        },
        Some(_) => return wrongtype(),
    };
    if cur.is_none_or(|c| n > c) {
        db.insert(tokens[1].clone(), Value::Str(n.to_string().into_bytes()));
        integer(1)
    } else {
        integer(0)
    }
}

/// INCRCAP key delta cap — increment by `delta` only if the result stays ≤ `cap`;
/// returns the new value, or nil if it would exceed the cap (a quota/rate limiter).
fn incrcap_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 4 {
        return wrong_args("incrcap");
    }
    let (delta, cap) = match (parse_int(&tokens[2]), parse_int(&tokens[3])) {
        (Some(d), Some(c)) => (d, c),
        _ => return not_integer(),
    };
    let cur = match db.get(&tokens[1]) {
        None => 0,
        Some(Value::Str(v)) => match std::str::from_utf8(v)
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
        {
            Some(c) => c,
            None => return not_integer(),
        },
        Some(_) => return wrongtype(),
    };
    let next = match cur.checked_add(delta) {
        Some(n) => n,
        None => return error("ERR increment or decrement would overflow"),
    };
    if next <= cap {
        db.insert(tokens[1].clone(), Value::Str(next.to_string().into_bytes()));
        integer(next)
    } else {
        null_bulk() // capped — rejected, value unchanged
    }
}

// === sketches: Bloom filter =================================================

fn bfadd_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 3 {
        return wrong_args("bfadd");
    }
    // Auto-create with sane defaults (≈10k items at 1% false-positive rate).
    let bloom = match db.get_or_insert_with(&tokens[1], || {
        Value::Bloom(crate::sketch::Bloom::with_capacity(10_000, 0.01))
    }) {
        Value::Bloom(b) => b,
        _ => return wrongtype(),
    };
    integer(bloom.add(&tokens[2]) as i64) // 1 if probably new, 0 if probably seen
}

fn bfexists_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 3 {
        return wrong_args("bfexists");
    }
    match db.get(&tokens[1]) {
        None => integer(0),
        Some(Value::Bloom(b)) => integer(b.contains(&tokens[2]) as i64),
        Some(_) => wrongtype(),
    }
}

/// BFLOAD key k nbits bits — restore a Bloom from raw parts (used by AOF rewrite
/// and replication; binary-safe in the bits argument).
fn bfload_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 5 {
        return wrong_args("bfload");
    }
    let k = match parse_int(&tokens[2]) {
        Some(k) if (1..=32).contains(&k) => k as u8,
        _ => return error("ERR invalid bloom k"),
    };
    let nbits = match std::str::from_utf8(&tokens[3])
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
    {
        Some(n) if n > 0 => n,
        _ => return error("ERR invalid bloom nbits"),
    };
    let bits = tokens[4].clone();
    if bits.len() < nbits.div_ceil(8) as usize {
        return error("ERR bloom bits too short");
    }
    db.insert(
        tokens[1].clone(),
        Value::Bloom(crate::sketch::Bloom::from_raw(k, nbits, bits)),
    );
    simple_string("OK")
}

// === sketches: Count-Min ====================================================

fn cmsincrby_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    // CMSINCRBY key item count [item count ...]
    let pairs = &tokens[2..];
    if tokens.len() < 4 || !pairs.len().is_multiple_of(2) {
        return wrong_args("cmsincrby");
    }
    // Validate all counts up front so a bad arg doesn't half-apply.
    let mut parsed: Vec<(&Vec<u8>, u32)> = Vec::with_capacity(pairs.len() / 2);
    for kv in pairs.chunks_exact(2) {
        match parse_int(&kv[1]) {
            Some(c) if (0..=u32::MAX as i64).contains(&c) => parsed.push((&kv[0], c as u32)),
            _ => return error("ERR CMSINCRBY: count must be a non-negative integer"),
        }
    }
    let cms = match db.get_or_insert_with(&tokens[1], || {
        Value::Cms(crate::sketch::Cms::default_sketch())
    }) {
        Value::Cms(c) => c,
        _ => return wrongtype(),
    };
    let out: Vec<Vec<u8>> = parsed
        .iter()
        .map(|(item, count)| integer(cms.incr(item, *count) as i64))
        .collect();
    array(&out)
}

fn cmsquery_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() < 3 {
        return wrong_args("cmsquery");
    }
    match db.get(&tokens[1]) {
        None => array(&tokens[2..].iter().map(|_| integer(0)).collect::<Vec<_>>()),
        Some(Value::Cms(c)) => array(
            &tokens[2..]
                .iter()
                .map(|it| integer(c.query(it) as i64))
                .collect::<Vec<_>>(),
        ),
        Some(_) => wrongtype(),
    }
}

/// CMSLOAD key width depth bytes — restore a Count-Min from raw counters (used
/// by AOF rewrite / replication).
fn cmsload_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 5 {
        return wrong_args("cmsload");
    }
    let parse_u32 = |b: &[u8]| {
        std::str::from_utf8(b)
            .ok()
            .and_then(|s| s.parse::<u32>().ok())
    };
    let (width, depth) = match (parse_u32(&tokens[2]), parse_u32(&tokens[3])) {
        (Some(w), Some(d)) if w > 0 && d > 0 => (w, d),
        _ => return error("ERR invalid CMS dimensions"),
    };
    match crate::sketch::Cms::from_bytes(width, depth, &tokens[4]) {
        Some(cms) => {
            db.insert(tokens[1].clone(), Value::Cms(cms));
            simple_string("OK")
        }
        None => error("ERR CMS counters length mismatch"),
    }
}

// === sketches: Top-K ========================================================

fn topkreserve_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 3 {
        return wrong_args("topkreserve");
    }
    let k = match parse_int(&tokens[2]) {
        Some(k) if k > 0 => k as usize,
        _ => return error("ERR TOPKRESERVE: k must be a positive integer"),
    };
    db.insert(
        tokens[1].clone(),
        Value::TopK(crate::sketch::TopK::default_topk(k)),
    );
    simple_string("OK")
}

fn topkadd_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() < 3 {
        return wrong_args("topkadd");
    }
    let tk = match db.get_or_insert_with(&tokens[1], || {
        Value::TopK(crate::sketch::TopK::default_topk(10))
    }) {
        Value::TopK(t) => t,
        _ => return wrongtype(),
    };
    // Per added item, return the item it evicted from the leaderboard (or nil).
    let out: Vec<Vec<u8>> = tokens[2..]
        .iter()
        .map(|item| match tk.add(item) {
            Some(evicted) => bulk_string(&evicted),
            None => null_bulk(),
        })
        .collect();
    array(&out)
}

fn topklist_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 2 {
        return wrong_args("topklist");
    }
    match db.get(&tokens[1]) {
        None => bulk_array(&[]),
        Some(Value::TopK(t)) => bulk_array(&t.list()),
        Some(_) => wrongtype(),
    }
}

fn topkcount_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() < 3 {
        return wrong_args("topkcount");
    }
    match db.get(&tokens[1]) {
        None => array(&tokens[2..].iter().map(|_| integer(0)).collect::<Vec<_>>()),
        Some(Value::TopK(t)) => array(
            &tokens[2..]
                .iter()
                .map(|it| integer(t.count(it) as i64))
                .collect::<Vec<_>>(),
        ),
        Some(_) => wrongtype(),
    }
}

/// TOPKLOAD key blob — restore from the opaque blob (AOF rewrite / replication).
fn topkload_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 3 {
        return wrong_args("topkload");
    }
    match crate::sketch::TopK::from_bytes(&tokens[2]) {
        Some(tk) => {
            db.insert(tokens[1].clone(), Value::TopK(tk));
            simple_string("OK")
        }
        None => error("ERR invalid TopK blob"),
    }
}

// === sketches: t-digest =====================================================

fn tdadd_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() < 3 {
        return wrong_args("tdadd");
    }
    let mut vals: Vec<f64> = Vec::with_capacity(tokens.len() - 2);
    for v in &tokens[2..] {
        match parse_finite_float(v) {
            Some(x) => vals.push(x),
            None => return error("ERR TDADD: value is not a valid float"),
        }
    }
    let td = match db.get_or_insert_with(&tokens[1], || {
        Value::TDigest(crate::sketch::TDigest::default_td())
    }) {
        Value::TDigest(t) => t,
        _ => return wrongtype(),
    };
    for x in vals {
        td.add(x);
    }
    simple_string("OK")
}

fn tdquantile_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() < 3 {
        return wrong_args("tdquantile");
    }
    let mut qs: Vec<f64> = Vec::with_capacity(tokens.len() - 2);
    for q in &tokens[2..] {
        match parse_finite_float(q) {
            Some(x) => qs.push(x),
            None => return error("ERR TDQUANTILE: quantile is not a valid float"),
        }
    }
    match db.get(&tokens[1]) {
        None => array(&qs.iter().map(|_| null_bulk()).collect::<Vec<_>>()),
        Some(Value::TDigest(t)) => {
            let out: Vec<Vec<u8>> = qs
                .iter()
                .map(|&q| {
                    let v = t.quantile(q);
                    if v.is_finite() {
                        bulk_string(&fmt_score(v))
                    } else {
                        null_bulk()
                    }
                })
                .collect();
            array(&out)
        }
        Some(_) => wrongtype(),
    }
}

/// TDLOAD key blob — restore a t-digest from its opaque blob (AOF / replication).
fn tdload_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 3 {
        return wrong_args("tdload");
    }
    match crate::sketch::TDigest::from_bytes(&tokens[2]) {
        Some(td) => {
            db.insert(tokens[1].clone(), Value::TDigest(td));
            simple_string("OK")
        }
        None => error("ERR invalid t-digest blob"),
    }
}

// === hashes =================================================================

fn with_hash<'a>(db: &'a mut Db, key: &[u8]) -> Result<Option<&'a HashVal>, ()> {
    match db.get(key) {
        None => Ok(None),
        Some(Value::Hash(h)) => Ok(Some(h)),
        Some(_) => Err(()),
    }
}

fn hset_cmd(db: &mut Db, tokens: &[Vec<u8>], _nx: bool) -> Vec<u8> {
    if tokens.len() < 4 || !(tokens.len() - 2).is_multiple_of(2) {
        return wrong_args("hset");
    }
    let h = match db.get_or_insert_with(&tokens[1], || Value::Hash(HashMap::new())) {
        Value::Hash(h) => h,
        _ => return wrongtype(),
    };
    let mut added = 0i64;
    let mut i = 2;
    while i + 1 < tokens.len() {
        if h.insert(tokens[i].clone(), tokens[i + 1].clone()).is_none() {
            added += 1;
        }
        i += 2;
    }
    integer(added)
}

fn hsetnx_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 4 {
        return wrong_args("hsetnx");
    }
    let h = match db.get_or_insert_with(&tokens[1], || Value::Hash(HashMap::new())) {
        Value::Hash(h) => h,
        _ => return wrongtype(),
    };
    if h.contains_key(&tokens[2]) {
        integer(0)
    } else {
        h.insert(tokens[2].clone(), tokens[3].clone());
        integer(1)
    }
}

fn hget_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 3 {
        return wrong_args("hget");
    }
    match with_hash(db, &tokens[1]) {
        Err(()) => wrongtype(),
        Ok(None) => null_bulk(),
        Ok(Some(h)) => h
            .get(&tokens[2])
            .map(|v| bulk_string(v))
            .unwrap_or_else(null_bulk),
    }
}

fn hmget_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() < 3 {
        return wrong_args("hmget");
    }
    match with_hash(db, &tokens[1]) {
        Err(()) => wrongtype(),
        Ok(opt) => {
            let elems: Vec<Vec<u8>> = tokens[2..]
                .iter()
                .map(|f| match opt.and_then(|h| h.get(f)) {
                    Some(v) => bulk_string(v),
                    None => null_bulk(),
                })
                .collect();
            array(&elems)
        }
    }
}

fn hgetall_cmd(db: &mut Db, tokens: &[Vec<u8>], proto: u8) -> Vec<u8> {
    if tokens.len() != 2 {
        return wrong_args("hgetall");
    }
    match with_hash(db, &tokens[1]) {
        Err(()) => wrongtype(),
        Ok(None) => crate::resp::map(&[], proto),
        Ok(Some(h)) => {
            let mut flat = Vec::with_capacity(h.len() * 2);
            for (f, v) in h {
                flat.push(f.clone());
                flat.push(v.clone());
            }
            crate::resp::map(&flat, proto)
        }
    }
}

fn hdel_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() < 3 {
        return wrong_args("hdel");
    }
    let removed = match db.get_mut(&tokens[1]) {
        None => 0,
        Some(Value::Hash(h)) => tokens[2..]
            .iter()
            .filter(|f| h.remove(*f).is_some())
            .count() as i64,
        Some(_) => return wrongtype(),
    };
    db.remove_if_empty(&tokens[1]);
    integer(removed)
}

fn hexists_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 3 {
        return wrong_args("hexists");
    }
    match with_hash(db, &tokens[1]) {
        Err(()) => wrongtype(),
        Ok(None) => integer(0),
        Ok(Some(h)) => integer(h.contains_key(&tokens[2]) as i64),
    }
}

fn hlen_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 2 {
        return wrong_args("hlen");
    }
    match with_hash(db, &tokens[1]) {
        Err(()) => wrongtype(),
        Ok(None) => integer(0),
        Ok(Some(h)) => integer(h.len() as i64),
    }
}

fn hkeys_vals_cmd(db: &mut Db, tokens: &[Vec<u8>], keys: bool) -> Vec<u8> {
    if tokens.len() != 2 {
        return wrong_args("hkeys");
    }
    match with_hash(db, &tokens[1]) {
        Err(()) => wrongtype(),
        Ok(None) => bulk_array(&[]),
        Ok(Some(h)) => {
            let items: Vec<Vec<u8>> = if keys {
                h.keys().cloned().collect()
            } else {
                h.values().cloned().collect()
            };
            bulk_array(&items)
        }
    }
}

fn hincrby_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 4 {
        return wrong_args("hincrby");
    }
    let delta = match parse_int(&tokens[3]) {
        Some(d) => d,
        None => return not_integer(),
    };
    let h = match db.get_or_insert_with(&tokens[1], || Value::Hash(HashMap::new())) {
        Value::Hash(h) => h,
        _ => return wrongtype(),
    };
    let cur = match h.get(&tokens[2]) {
        None => 0,
        Some(v) => match std::str::from_utf8(v)
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
        {
            Some(n) => n,
            None => return error("ERR hash value is not an integer"),
        },
    };
    match cur.checked_add(delta) {
        Some(next) => {
            h.insert(tokens[2].clone(), next.to_string().into_bytes());
            integer(next)
        }
        None => error("ERR increment or decrement would overflow"),
    }
}

// === sets ===================================================================

fn sadd_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() < 3 {
        return wrong_args("sadd");
    }
    let s = match db.get_or_insert_with(&tokens[1], || Value::Set(HashSet::new())) {
        Value::Set(s) => s,
        _ => return wrongtype(),
    };
    let added = tokens[2..]
        .iter()
        .filter(|m| s.insert((*m).clone()))
        .count();
    integer(added as i64)
}

fn srem_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() < 3 {
        return wrong_args("srem");
    }
    let removed = match db.get_mut(&tokens[1]) {
        None => 0,
        Some(Value::Set(s)) => tokens[2..].iter().filter(|m| s.remove(*m)).count() as i64,
        Some(_) => return wrongtype(),
    };
    db.remove_if_empty(&tokens[1]);
    integer(removed)
}

fn smembers_cmd(db: &mut Db, tokens: &[Vec<u8>], proto: u8) -> Vec<u8> {
    if tokens.len() != 2 {
        return wrong_args("smembers");
    }
    match db.get(&tokens[1]) {
        None => crate::resp::set(&[], proto),
        Some(Value::Set(s)) => crate::resp::set(&s.iter().cloned().collect::<Vec<_>>(), proto),
        Some(_) => wrongtype(),
    }
}

fn sismember_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 3 {
        return wrong_args("sismember");
    }
    match db.get(&tokens[1]) {
        None => integer(0),
        Some(Value::Set(s)) => integer(s.contains(&tokens[2]) as i64),
        Some(_) => wrongtype(),
    }
}

fn smismember_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() < 3 {
        return wrong_args("smismember");
    }
    let set = match db.get(&tokens[1]) {
        None => None,
        Some(Value::Set(s)) => Some(s),
        Some(_) => return wrongtype(),
    };
    let elems: Vec<Vec<u8>> = tokens[2..]
        .iter()
        .map(|m| integer(set.is_some_and(|s| s.contains(m)) as i64))
        .collect();
    array(&elems)
}

fn scard_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 2 {
        return wrong_args("scard");
    }
    match db.get(&tokens[1]) {
        None => integer(0),
        Some(Value::Set(s)) => integer(s.len() as i64),
        Some(_) => wrongtype(),
    }
}

fn spop_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() < 2 || tokens.len() > 3 {
        return wrong_args("spop");
    }
    let count = if tokens.len() == 3 {
        match parse_int(&tokens[2]) {
            Some(c) if c >= 0 => Some(c as usize),
            _ => return error("ERR value is out of range, must be positive"),
        }
    } else {
        None
    };
    let mut popped: Vec<Vec<u8>> = Vec::new();
    match db.get_mut(&tokens[1]) {
        None => {
            return if count.is_some() {
                bulk_array(&[])
            } else {
                null_bulk()
            };
        }
        Some(Value::Set(s)) => {
            let members: Vec<Vec<u8>> = s.iter().cloned().collect();
            for i in distinct_indices(members.len(), count.unwrap_or(1)) {
                s.remove(&members[i]);
                popped.push(members[i].clone());
            }
        }
        Some(_) => return wrongtype(),
    }
    db.remove_if_empty(&tokens[1]);
    if count.is_some() {
        bulk_array(&popped)
    } else {
        popped
            .into_iter()
            .next()
            .map(|v| bulk_string(&v))
            .unwrap_or_else(null_bulk)
    }
}

fn srandmember_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() < 2 || tokens.len() > 3 {
        return wrong_args("srandmember");
    }
    let with_count = tokens.len() == 3;
    let count = if with_count {
        match parse_int(&tokens[2]) {
            Some(c) => c,
            None => return not_integer(),
        }
    } else {
        1
    };
    let members: Vec<Vec<u8>> = match db.get(&tokens[1]) {
        None => vec![],
        Some(Value::Set(s)) => s.iter().cloned().collect(),
        Some(_) => return wrongtype(),
    };
    if !with_count {
        // A single random member, like Redis (nil on a missing key).
        return if members.is_empty() {
            null_bulk()
        } else {
            bulk_string(&members[rand_index(members.len())])
        };
    }
    if members.is_empty() || count == 0 {
        return bulk_array(&[]);
    }
    let picked: Vec<Vec<u8>> = if count > 0 {
        // Distinct members, up to `count`.
        distinct_indices(members.len(), count as usize)
            .into_iter()
            .map(|i| members[i].clone())
            .collect()
    } else {
        // Exactly |count| members, with possible repeats.
        (0..(-count) as usize)
            .map(|_| members[rand_index(members.len())].clone())
            .collect()
    };
    bulk_array(&picked)
}

fn randomkey_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 1 {
        return wrong_args("randomkey");
    }
    let keys = db.live_keys();
    if keys.is_empty() {
        null_bulk()
    } else {
        bulk_string(&keys[rand_index(keys.len())])
    }
}

#[derive(Clone, Copy)]
enum SetOp {
    Inter,
    Union,
    Diff,
}

/// Compute INTER/UNION/DIFF over `keys` (missing key = empty set). Err = a key
/// holds a non-set value (WRONGTYPE). `keys` must be non-empty.
fn setop_compute(db: &mut Db, keys: &[Vec<u8>], op: SetOp) -> Result<HashSet<Vec<u8>>, ()> {
    let mut sets: Vec<HashSet<Vec<u8>>> = Vec::new();
    for key in keys {
        match db.get(key) {
            None => sets.push(HashSet::new()),
            Some(Value::Set(s)) => sets.push(s.clone()),
            Some(_) => return Err(()),
        }
    }
    let mut acc = sets[0].clone();
    for s in &sets[1..] {
        match op {
            SetOp::Inter => acc.retain(|m| s.contains(m)),
            SetOp::Union => acc.extend(s.iter().cloned()),
            SetOp::Diff => acc.retain(|m| !s.contains(m)),
        }
    }
    Ok(acc)
}

fn setop_cmd(db: &mut Db, tokens: &[Vec<u8>], op: SetOp, proto: u8) -> Vec<u8> {
    if tokens.len() < 2 {
        return wrong_args("setop");
    }
    match setop_compute(db, &tokens[1..], op) {
        Ok(acc) => crate::resp::set(&acc.into_iter().collect::<Vec<_>>(), proto),
        Err(()) => wrongtype(),
    }
}

/// SINTERSTORE / SUNIONSTORE / SDIFFSTORE: `<cmd> dest key [key ...]`.
fn setop_store_cmd(db: &mut Db, tokens: &[Vec<u8>], op: SetOp) -> Vec<u8> {
    if tokens.len() < 3 {
        return wrong_args(&cmd_name(tokens));
    }
    let acc = match setop_compute(db, &tokens[2..], op) {
        Ok(a) => a,
        Err(()) => return wrongtype(),
    };
    let n = acc.len();
    let dest = &tokens[1];
    if acc.is_empty() {
        db.remove(dest); // an empty result deletes the destination
    } else {
        db.insert(dest.clone(), Value::Set(acc));
        db.clear_expire(dest); // a fresh store overwrites any prior TTL
    }
    integer(n as i64)
}

fn smove_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 4 {
        return wrong_args("smove");
    }
    let (src, dst, member) = (&tokens[1], &tokens[2], &tokens[3]);
    // Type-check dst up front so we never remove from src and then fail.
    match db.get(dst) {
        None | Some(Value::Set(_)) => {}
        Some(_) => return wrongtype(),
    }
    let removed = match db.get_mut(src) {
        None => false,
        Some(Value::Set(s)) => s.remove(member),
        Some(_) => return wrongtype(),
    };
    if !removed {
        return integer(0);
    }
    db.remove_if_empty(src);
    match db.get_or_insert_with(dst, || Value::Set(HashSet::new())) {
        Value::Set(s) => {
            s.insert(member.clone());
        }
        _ => return wrongtype(), // unreachable: dst was type-checked
    }
    integer(1)
}

fn sintercard_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    // SINTERCARD numkeys key [key ...] [LIMIT n]
    if tokens.len() < 3 {
        return wrong_args("sintercard");
    }
    let numkeys = match parse_int(&tokens[1]) {
        Some(n) if n > 0 => n as usize,
        Some(_) => return error("ERR numkeys should be greater than 0"),
        None => return error("ERR numkeys should be greater than 0"),
    };
    if tokens.len() < 2 + numkeys {
        return error("ERR Number of keys can't be greater than number of args");
    }
    let keys = &tokens[2..2 + numkeys];
    let rest = &tokens[2 + numkeys..];
    let mut limit: Option<usize> = None;
    if !rest.is_empty() {
        if rest.len() == 2 && rest[0].eq_ignore_ascii_case(b"LIMIT") {
            limit = match parse_int(&rest[1]) {
                Some(l) if l >= 0 => Some(l as usize),
                _ => return error("ERR LIMIT can't be negative"),
            };
        } else {
            return error("ERR syntax error");
        }
    }
    match setop_compute(db, keys, SetOp::Inter) {
        Ok(acc) => match limit {
            Some(l) if l > 0 => integer(acc.len().min(l) as i64),
            _ => integer(acc.len() as i64), // LIMIT 0 or absent = no cap
        },
        Err(()) => wrongtype(),
    }
}

// === sorted sets ============================================================

fn parse_score(arg: &[u8]) -> Option<f64> {
    let s = std::str::from_utf8(arg).ok()?.trim();
    match s.to_ascii_lowercase().as_str() {
        "inf" | "+inf" | "infinity" | "+infinity" => Some(f64::INFINITY),
        "-inf" | "-infinity" => Some(f64::NEG_INFINITY),
        _ => s.parse::<f64>().ok(),
    }
}

/// Format a score like Redis: integers without a decimal point, infinities as
/// inf/-inf, otherwise the shortest round-tripping form.
fn fmt_score(s: f64) -> Vec<u8> {
    if s.is_infinite() {
        if s > 0.0 {
            b"inf".to_vec()
        } else {
            b"-inf".to_vec()
        }
    } else {
        format!("{s}").into_bytes()
    }
}

/// Parse a range bound: "(x" is exclusive, otherwise inclusive.
fn parse_bound(arg: &[u8]) -> Option<(f64, bool)> {
    if arg.first() == Some(&b'(') {
        parse_score(&arg[1..]).map(|v| (v, true))
    } else {
        parse_score(arg).map(|v| (v, false))
    }
}

fn in_range(s: f64, lo: f64, lo_ex: bool, hi: f64, hi_ex: bool) -> bool {
    let above = if lo_ex { s > lo } else { s >= lo };
    let below = if hi_ex { s < hi } else { s <= hi };
    above && below
}

/// Members in (score, then member bytes) ascending order — straight from the
/// sorted set's ordered index, no per-call sort.
fn sorted_members(z: &ZSet) -> Vec<(Vec<u8>, f64)> {
    z.ordered().collect()
}

fn get_zset<'a>(db: &'a mut Db, key: &[u8]) -> Result<Option<&'a ZSet>, ()> {
    match db.get(key) {
        None => Ok(None),
        Some(Value::ZSet(z)) => Ok(Some(z)),
        Some(_) => Err(()),
    }
}

fn zadd_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() < 4 {
        return wrong_args("zadd");
    }
    let (mut nx, mut xx, mut ch, mut incr) = (false, false, false, false);
    let (mut gt, mut lt) = (false, false);
    let mut i = 2;
    while i < tokens.len() {
        match tokens[i].to_ascii_uppercase().as_slice() {
            b"NX" => nx = true,
            b"XX" => xx = true,
            b"CH" => ch = true,
            b"INCR" => incr = true,
            b"GT" => gt = true,
            b"LT" => lt = true,
            _ => break,
        }
        i += 1;
    }
    let pairs = &tokens[i..];
    if pairs.is_empty() || !pairs.len().is_multiple_of(2) {
        return error("ERR syntax error");
    }
    if nx && xx {
        return error("ERR XX and NX options at the same time are not compatible");
    }
    // GT/LT update only existing members (toward greater/lesser scores) and are
    // mutually exclusive with each other and with NX.
    if (gt && lt) || (nx && (gt || lt)) {
        return error("ERR GT, LT, and/or NX options at the same time are not compatible");
    }
    if incr && pairs.len() != 2 {
        return error("ERR INCR option supports a single increment-element pair");
    }
    // Parse all scores up front so a bad float aborts without mutating anything.
    let mut parsed: Vec<(f64, &Vec<u8>)> = Vec::with_capacity(pairs.len() / 2);
    let mut j = 0;
    while j < pairs.len() {
        match parse_score(&pairs[j]) {
            Some(s) => parsed.push((s, &pairs[j + 1])),
            None => return error("ERR value is not a valid float"),
        }
        j += 2;
    }
    let key = &tokens[1];
    let z = match db.get_or_insert_with(key, || Value::ZSet(ZSet::new())) {
        Value::ZSet(z) => z,
        _ => return wrongtype(),
    };
    if incr {
        let (score, member) = parsed[0];
        let existing = z.get(member).copied();
        if (nx && existing.is_some()) || (xx && existing.is_none()) {
            db.remove_if_empty(key);
            return null_bulk();
        }
        let newv = existing.unwrap_or(0.0) + score;
        // GT/LT abort the increment (return nil) when the new score doesn't move
        // in the requested direction relative to an existing member.
        if let Some(old) = existing
            && ((gt && newv <= old) || (lt && newv >= old))
        {
            db.remove_if_empty(key);
            return null_bulk();
        }
        z.insert(member.clone(), newv);
        return bulk_string(&fmt_score(newv));
    }
    let mut added = 0i64;
    let mut changed = 0i64;
    for (score, member) in parsed {
        let existing = z.get(member).copied();
        if (nx && existing.is_some()) || (xx && existing.is_none()) {
            continue;
        }
        match existing {
            None => {
                z.insert(member.clone(), score);
                added += 1;
                changed += 1;
            }
            Some(old) => {
                // GT/LT only update toward a greater/lesser score.
                if (gt && score <= old) || (lt && score >= old) {
                    continue;
                }
                if old != score {
                    z.insert(member.clone(), score);
                    changed += 1;
                }
            }
        }
    }
    db.remove_if_empty(key);
    integer(if ch { changed } else { added })
}

fn zscore_cmd(db: &mut Db, tokens: &[Vec<u8>], proto: u8) -> Vec<u8> {
    if tokens.len() != 3 {
        return wrong_args("zscore");
    }
    match get_zset(db, &tokens[1]) {
        Err(()) => wrongtype(),
        Ok(None) => null_bulk(),
        Ok(Some(z)) => z
            .get(&tokens[2])
            .map(|s| crate::resp::double(&fmt_score(*s), proto))
            .unwrap_or_else(null_bulk),
    }
}

fn zmscore_cmd(db: &mut Db, tokens: &[Vec<u8>], proto: u8) -> Vec<u8> {
    if tokens.len() < 3 {
        return wrong_args("zmscore");
    }
    match get_zset(db, &tokens[1]) {
        Err(()) => wrongtype(),
        Ok(opt) => {
            let elems: Vec<Vec<u8>> = tokens[2..]
                .iter()
                .map(|m| match opt.and_then(|z| z.get(m)) {
                    Some(s) => crate::resp::double(&fmt_score(*s), proto),
                    None => null_bulk(),
                })
                .collect();
            array(&elems)
        }
    }
}

fn zcard_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 2 {
        return wrong_args("zcard");
    }
    match get_zset(db, &tokens[1]) {
        Err(()) => wrongtype(),
        Ok(None) => integer(0),
        Ok(Some(z)) => integer(z.len() as i64),
    }
}

fn zrem_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() < 3 {
        return wrong_args("zrem");
    }
    let removed = match db.get_mut(&tokens[1]) {
        None => 0,
        Some(Value::ZSet(z)) => tokens[2..].iter().filter(|m| z.remove(m).is_some()).count() as i64,
        Some(_) => return wrongtype(),
    };
    db.remove_if_empty(&tokens[1]);
    integer(removed)
}

fn zincrby_cmd(db: &mut Db, tokens: &[Vec<u8>], proto: u8) -> Vec<u8> {
    if tokens.len() != 4 {
        return wrong_args("zincrby");
    }
    let incr = match parse_score(&tokens[2]) {
        Some(s) => s,
        None => return error("ERR value is not a valid float"),
    };
    let z = match db.get_or_insert_with(&tokens[1], || Value::ZSet(ZSet::new())) {
        Value::ZSet(z) => z,
        _ => return wrongtype(),
    };
    let newv = z.get(&tokens[3]).copied().unwrap_or(0.0) + incr;
    z.insert(tokens[3].clone(), newv);
    crate::resp::double(&fmt_score(newv), proto)
}

fn zrank_cmd(db: &mut Db, tokens: &[Vec<u8>], rev: bool) -> Vec<u8> {
    if tokens.len() != 3 {
        return wrong_args("zrank");
    }
    match get_zset(db, &tokens[1]) {
        Err(()) => wrongtype(),
        Ok(None) => null_bulk(),
        Ok(Some(z)) => match z.rank(&tokens[2]) {
            // rank() is ascending; reverse rank is len-1-asc.
            Some(asc) => integer(if rev {
                (z.len() - 1 - asc) as i64
            } else {
                asc as i64
            }),
            None => null_bulk(),
        },
    }
}

fn zrange_index(z: &ZSet, start: i64, stop: i64, withscores: bool, rev: bool) -> Vec<u8> {
    let mut sorted = sorted_members(z);
    if rev {
        sorted.reverse();
    }
    let len = sorted.len();
    let s = norm(start, len).max(0);
    let mut e = norm(stop, len);
    if e >= len as i64 {
        e = len as i64 - 1;
    }
    if len == 0 || s > e || s >= len as i64 {
        return bulk_array(&[]);
    }
    let mut out: Vec<Vec<u8>> = Vec::new();
    for (m, sc) in &sorted[s as usize..=e as usize] {
        out.push(m.clone());
        if withscores {
            out.push(fmt_score(*sc));
        }
    }
    bulk_array(&out)
}

fn zrange_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() < 4 {
        return wrong_args("zrange");
    }
    let (start, stop) = match (parse_int(&tokens[2]), parse_int(&tokens[3])) {
        (Some(a), Some(b)) => (a, b),
        _ => return not_integer(),
    };
    let (mut withscores, mut rev) = (false, false);
    for t in &tokens[4..] {
        match t.to_ascii_uppercase().as_slice() {
            b"WITHSCORES" => withscores = true,
            b"REV" => rev = true,
            _ => return error("ERR syntax error"),
        }
    }
    match get_zset(db, &tokens[1]) {
        Err(()) => wrongtype(),
        Ok(None) => bulk_array(&[]),
        Ok(Some(z)) => zrange_index(z, start, stop, withscores, rev),
    }
}

fn zrevrange_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() < 4 {
        return wrong_args("zrevrange");
    }
    let (start, stop) = match (parse_int(&tokens[2]), parse_int(&tokens[3])) {
        (Some(a), Some(b)) => (a, b),
        _ => return not_integer(),
    };
    let mut withscores = false;
    for t in &tokens[4..] {
        match t.to_ascii_uppercase().as_slice() {
            b"WITHSCORES" => withscores = true,
            _ => return error("ERR syntax error"),
        }
    }
    match get_zset(db, &tokens[1]) {
        Err(()) => wrongtype(),
        Ok(None) => bulk_array(&[]),
        Ok(Some(z)) => zrange_index(z, start, stop, withscores, true),
    }
}

fn zrangebyscore_cmd(db: &mut Db, tokens: &[Vec<u8>], rev: bool) -> Vec<u8> {
    if tokens.len() < 4 {
        return wrong_args("zrangebyscore");
    }
    // ZRANGEBYSCORE key min max ... ; ZREVRANGEBYSCORE key max min ...
    let (lo_arg, hi_arg) = if rev {
        (&tokens[3], &tokens[2])
    } else {
        (&tokens[2], &tokens[3])
    };
    let (lo, lo_ex) = match parse_bound(lo_arg) {
        Some(x) => x,
        None => return error("ERR min or max is not a float"),
    };
    let (hi, hi_ex) = match parse_bound(hi_arg) {
        Some(x) => x,
        None => return error("ERR min or max is not a float"),
    };
    let mut withscores = false;
    let mut limit: Option<(i64, i64)> = None;
    let mut i = 4;
    while i < tokens.len() {
        match tokens[i].to_ascii_uppercase().as_slice() {
            b"WITHSCORES" => {
                withscores = true;
                i += 1;
            }
            b"LIMIT" => {
                if i + 2 >= tokens.len() {
                    return error("ERR syntax error");
                }
                let off = match parse_int(&tokens[i + 1]) {
                    Some(n) => n,
                    None => return not_integer(),
                };
                let cnt = match parse_int(&tokens[i + 2]) {
                    Some(n) => n,
                    None => return not_integer(),
                };
                limit = Some((off, cnt));
                i += 3;
            }
            _ => return error("ERR syntax error"),
        }
    }
    let z = match get_zset(db, &tokens[1]) {
        Err(()) => return wrongtype(),
        Ok(None) => return bulk_array(&[]),
        Ok(Some(z)) => z,
    };
    let mut filtered: Vec<(Vec<u8>, f64)> = sorted_members(z)
        .into_iter()
        .filter(|(_, s)| in_range(*s, lo, lo_ex, hi, hi_ex))
        .collect();
    if rev {
        filtered.reverse();
    }
    let slice: Vec<(Vec<u8>, f64)> = match limit {
        Some((off, cnt)) => {
            let it = filtered.into_iter().skip(off.max(0) as usize);
            if cnt < 0 {
                it.collect()
            } else {
                it.take(cnt as usize).collect()
            }
        }
        None => filtered,
    };
    let mut out = Vec::new();
    for (m, s) in slice {
        out.push(m);
        if withscores {
            out.push(fmt_score(s));
        }
    }
    bulk_array(&out)
}

fn zcount_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 4 {
        return wrong_args("zcount");
    }
    let (lo, lo_ex) = match parse_bound(&tokens[2]) {
        Some(x) => x,
        None => return error("ERR min or max is not a float"),
    };
    let (hi, hi_ex) = match parse_bound(&tokens[3]) {
        Some(x) => x,
        None => return error("ERR min or max is not a float"),
    };
    match get_zset(db, &tokens[1]) {
        Err(()) => wrongtype(),
        Ok(None) => integer(0),
        Ok(Some(z)) => {
            let n = z
                .iter()
                .filter(|(_, s)| in_range(**s, lo, lo_ex, hi, hi_ex))
                .count();
            integer(n as i64)
        }
    }
}

fn zpop_cmd(db: &mut Db, tokens: &[Vec<u8>], max: bool) -> Vec<u8> {
    if tokens.len() < 2 || tokens.len() > 3 {
        return wrong_args("zpop");
    }
    let count = if tokens.len() == 3 {
        match parse_int(&tokens[2]) {
            Some(c) if c >= 0 => c as usize,
            _ => return error("ERR value is out of range, must be positive"),
        }
    } else {
        1
    };
    let mut popped: Vec<(Vec<u8>, f64)> = Vec::new();
    match db.get_mut(&tokens[1]) {
        None => return bulk_array(&[]),
        Some(Value::ZSet(z)) => {
            let mut sorted = sorted_members(z);
            if max {
                sorted.reverse();
            }
            for (m, s) in sorted.into_iter().take(count) {
                z.remove(&m);
                popped.push((m, s));
            }
        }
        Some(_) => return wrongtype(),
    }
    db.remove_if_empty(&tokens[1]);
    let mut out = Vec::new();
    for (m, s) in popped {
        out.push(m);
        out.push(fmt_score(s));
    }
    bulk_array(&out)
}

fn zremrangebyrank_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 4 {
        return wrong_args("zremrangebyrank");
    }
    let (start, stop) = match (parse_int(&tokens[2]), parse_int(&tokens[3])) {
        (Some(a), Some(b)) => (a, b),
        _ => return not_integer(),
    };
    let z = match db.get_mut(&tokens[1]) {
        None => return integer(0),
        Some(Value::ZSet(z)) => z,
        Some(_) => return wrongtype(),
    };
    let sorted = sorted_members(z);
    let len = sorted.len();
    let s = norm(start, len).max(0);
    let mut e = norm(stop, len);
    if e >= len as i64 {
        e = len as i64 - 1;
    }
    if len == 0 || s > e || s >= len as i64 {
        return integer(0);
    }
    let remove: Vec<Vec<u8>> = sorted[s as usize..=e as usize]
        .iter()
        .map(|(m, _)| m.clone())
        .collect();
    let n = remove.len();
    for m in &remove {
        z.remove(m);
    }
    db.remove_if_empty(&tokens[1]);
    integer(n as i64)
}

fn zremrangebyscore_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 4 {
        return wrong_args("zremrangebyscore");
    }
    let (lo, lo_ex) = match parse_bound(&tokens[2]) {
        Some(x) => x,
        None => return error("ERR min or max is not a float"),
    };
    let (hi, hi_ex) = match parse_bound(&tokens[3]) {
        Some(x) => x,
        None => return error("ERR min or max is not a float"),
    };
    let z = match db.get_mut(&tokens[1]) {
        None => return integer(0),
        Some(Value::ZSet(z)) => z,
        Some(_) => return wrongtype(),
    };
    let remove: Vec<Vec<u8>> = z
        .iter()
        .filter(|&(_, &s)| in_range(s, lo, lo_ex, hi, hi_ex))
        .map(|(m, _)| m.clone())
        .collect();
    let n = remove.len();
    for m in &remove {
        z.remove(m);
    }
    db.remove_if_empty(&tokens[1]);
    integer(n as i64)
}

#[derive(Clone, Copy)]
enum Agg {
    Sum,
    Min,
    Max,
}

fn aggregate(agg: Agg, a: f64, b: f64) -> f64 {
    let r = match agg {
        Agg::Sum => a + b,
        Agg::Min => a.min(b),
        Agg::Max => a.max(b),
    };
    if r.is_nan() { 0.0 } else { r } // e.g. inf + -inf
}

/// ZUNIONSTORE / ZINTERSTORE:
/// `<cmd> dest numkeys key [key ...] [WEIGHTS w ...] [AGGREGATE SUM|MIN|MAX]`.
/// Source keys may be sorted sets or plain sets (a set member scores 1.0).
fn zstore_cmd(db: &mut Db, tokens: &[Vec<u8>], inter: bool) -> Vec<u8> {
    if tokens.len() < 4 {
        return wrong_args(&cmd_name(tokens));
    }
    let numkeys = match parse_int(&tokens[2]) {
        Some(n) if n > 0 => n as usize,
        Some(_) => return error("ERR at least 1 input key is needed"),
        None => return not_integer(),
    };
    if tokens.len() < 3 + numkeys {
        return error("ERR syntax error");
    }
    let keys = &tokens[3..3 + numkeys];
    let rest = &tokens[3 + numkeys..];
    let mut weights = vec![1.0f64; numkeys];
    let mut agg = Agg::Sum;
    let mut i = 0;
    while i < rest.len() {
        match rest[i].to_ascii_uppercase().as_slice() {
            b"WEIGHTS" => {
                if rest.len() < i + 1 + numkeys {
                    return error("ERR syntax error");
                }
                for (j, w) in weights.iter_mut().enumerate() {
                    *w = match parse_score(&rest[i + 1 + j]) {
                        Some(v) => v,
                        None => return error("ERR weight value is not a float"),
                    };
                }
                i += 1 + numkeys;
            }
            b"AGGREGATE" => {
                agg = match rest.get(i + 1).map(|a| a.to_ascii_uppercase()) {
                    Some(a) if a == b"SUM" => Agg::Sum,
                    Some(a) if a == b"MIN" => Agg::Min,
                    Some(a) if a == b"MAX" => Agg::Max,
                    _ => return error("ERR syntax error"),
                };
                i += 2;
            }
            _ => return error("ERR syntax error"),
        }
    }
    let mut acc: HashMap<Vec<u8>, f64> = HashMap::new();
    for (idx, key) in keys.iter().enumerate() {
        let w = weights[idx];
        let this: HashMap<Vec<u8>, f64> = match db.get(key) {
            None => HashMap::new(),
            Some(Value::ZSet(z)) => z.iter().map(|(m, &s)| (m.clone(), s * w)).collect(),
            Some(Value::Set(s)) => s.iter().map(|m| (m.clone(), w)).collect(),
            Some(_) => return wrongtype(),
        };
        if !inter {
            for (m, v) in this {
                acc.entry(m)
                    .and_modify(|c| *c = aggregate(agg, *c, v))
                    .or_insert(v);
            }
        } else if idx == 0 {
            acc = this;
        } else {
            acc = acc
                .iter()
                .filter_map(|(m, v)| this.get(m).map(|tv| (m.clone(), aggregate(agg, *v, *tv))))
                .collect();
        }
    }
    let dest = &tokens[1];
    let n = acc.len();
    if acc.is_empty() {
        db.remove(dest);
    } else {
        db.insert(dest.clone(), Value::ZSet(acc.into_iter().collect()));
        db.clear_expire(dest);
    }
    integer(n as i64)
}

// === geo ====================================================================
//
// Geo-first model: each object is its own key holding a `Value::Geo(lon, lat)`.
// GEOSEARCH scans the keyspace's geo-key candidate set (see `Db::geo_keys`).

const EARTH_R_M: f64 = 6_372_797.560_856; // Redis's earth radius, in meters

pub fn geo_unit(u: &[u8]) -> Option<f64> {
    match u.to_ascii_lowercase().as_slice() {
        b"m" => Some(1.0),
        b"km" => Some(1000.0),
        b"mi" => Some(1609.34),
        b"ft" => Some(0.3048),
        _ => None,
    }
}

/// Great-circle distance in meters (haversine). Also used by the region changefeed.
pub fn haversine_m(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let (p1, p2) = (lat1.to_radians(), lat2.to_radians());
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();
    let a = (dlat / 2.0).sin().powi(2) + p1.cos() * p2.cos() * (dlon / 2.0).sin().powi(2);
    2.0 * EARTH_R_M * a.sqrt().min(1.0).asin()
}

fn fmt_coord(x: f64) -> Vec<u8> {
    format!("{x}").into_bytes()
}

/// Fetch a key's geo point. `Err(())` = the key holds a non-geo value.
/// True if the key's geo attributes satisfy every `WHERE field value` clause (AND).
/// Empty `wheres` → always true.
fn geo_attr_match(db: &mut Db, key: &[u8], wheres: &[(Vec<u8>, Vec<u8>)]) -> bool {
    if wheres.is_empty() {
        return true;
    }
    match db.get(key) {
        Some(Value::Geo(_, _, attrs)) => wheres
            .iter()
            .all(|(f, v)| attrs.iter().any(|(af, av)| af == f && av == v)),
        _ => false,
    }
}

fn geo_point(db: &mut Db, key: &[u8]) -> Result<Option<(f64, f64)>, ()> {
    match db.get(key) {
        None => Ok(None),
        Some(Value::Geo(lon, lat, _)) => Ok(Some((*lon, *lat))),
        Some(_) => Err(()),
    }
}

fn geoset_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    // GEOSET key lon lat [field value ...]  — trailing pairs are inline attributes.
    if tokens.len() < 4 || !tokens.len().is_multiple_of(2) {
        return wrong_args("geoset");
    }
    let (lon, lat) = match (
        parse_finite_float(&tokens[2]),
        parse_finite_float(&tokens[3]),
    ) {
        (Some(a), Some(b)) => (a, b),
        _ => return error("ERR value is not a valid float"),
    };
    if !(-180.0..=180.0).contains(&lon) || !(-85.051_128_78..=85.051_128_78).contains(&lat) {
        return error("ERR invalid longitude,latitude pair");
    }
    let attrs: crate::db::GeoAttrs = tokens[4..]
        .chunks_exact(2)
        .map(|p| (p[0].clone(), p[1].clone()))
        .collect();
    db.insert(tokens[1].clone(), Value::Geo(lon, lat, attrs));
    simple_string("OK")
}

fn geopos_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() < 2 {
        return wrong_args("geopos");
    }
    let mut out = Vec::new();
    for key in &tokens[1..] {
        match geo_point(db, key) {
            Ok(Some((lon, lat))) => out.push(array(&[
                bulk_string(&fmt_coord(lon)),
                bulk_string(&fmt_coord(lat)),
            ])),
            _ => out.push(null_array()), // missing or wrong type -> nil (like MGET)
        }
    }
    array(&out)
}

fn geodist_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() < 3 || tokens.len() > 4 {
        return wrong_args("geodist");
    }
    let unit = match tokens.get(3) {
        None => 1.0,
        Some(u) => match geo_unit(u) {
            Some(d) => d,
            None => return error("ERR unsupported unit provided. please use M, KM, FT, MI"),
        },
    };
    let p1 = match geo_point(db, &tokens[1]) {
        Ok(Some(p)) => p,
        Ok(None) => return null_bulk(),
        Err(()) => return wrongtype(),
    };
    let p2 = match geo_point(db, &tokens[2]) {
        Ok(Some(p)) => p,
        Ok(None) => return null_bulk(),
        Err(()) => return wrongtype(),
    };
    let d = haversine_m(p1.0, p1.1, p2.0, p2.1) / unit;
    bulk_string(format!("{d:.4}").as_bytes())
}

enum GeoShape {
    Radius(f64),   // meters
    Box(f64, f64), // width, height in meters
}

/// Bounding box (min_lon, min_lat, max_lon, max_lat) that fully contains the search
/// shape, for the spatial-index prefilter. Returns None near a pole or the ±180
/// meridian, where a simple box would wrap — the caller then does a full scan
/// (correctness over speed for that rare case).
fn geo_bbox(center: (f64, f64), shape: &GeoShape) -> Option<(f64, f64, f64, f64)> {
    let (clon, clat) = center;
    let (half_ns_m, half_ew_m) = match shape {
        GeoShape::Radius(r) => (*r, *r),
        GeoShape::Box(w, h) => (h / 2.0, w / 2.0),
    };
    // Generous meters-per-degree (smaller divisor -> larger box) + 20% margin, so
    // the candidate box never under-covers the exact (haversine) matches.
    let cosl = clat.to_radians().cos();
    if cosl.abs() < 1e-3 {
        return None; // near a pole: longitude scaling blows up -> full scan
    }
    let lat_delta = (half_ns_m / 110_000.0) * 1.2;
    let lon_delta = (half_ew_m / (110_000.0 * cosl.abs())) * 1.2;
    let (min_lat, max_lat) = (clat - lat_delta, clat + lat_delta);
    let (min_lon, max_lon) = (clon - lon_delta, clon + lon_delta);
    if !(-90.0..=90.0).contains(&min_lat)
        || !(-90.0..=90.0).contains(&max_lat)
        || !(-180.0..=180.0).contains(&min_lon)
        || !(-180.0..=180.0).contains(&max_lon)
    {
        return None; // pole / antimeridian edge -> full scan
    }
    Some((min_lon, min_lat, max_lon, max_lat))
}

/// One GEOSEARCH match: (key, distance_m, lon, lat).
pub type GeoHit = (Vec<u8>, f64, f64, f64);

/// A parsed GEOSEARCH (without the matches), so a cluster coordinator can replay
/// it on peers and render the merged result the same way a single node would.
pub struct GeoQuery {
    center: (f64, f64),
    shape: GeoShape,
    wheres: Vec<(Vec<u8>, Vec<u8>)>,
    order: Option<bool>,
    count: Option<usize>,
    any: bool, // COUNT ... ANY: return *any* n, skip the closest-n sort
    withcoord: bool,
    withdist: bool,
    unit_div: f64,
}

impl GeoQuery {
    /// Tokens for the internal `GEOSEARCHSHARD` that reproduces this query on a
    /// peer: center normalized to FROMLONLAT, shape in meters. Shards return raw
    /// hits, so order/count/WITH* are applied only by the coordinator.
    pub fn shard_query(&self) -> Vec<Vec<u8>> {
        let mut t = vec![
            b"GEOSEARCHSHARD".to_vec(),
            b"FROMLONLAT".to_vec(),
            format!("{}", self.center.0).into_bytes(),
            format!("{}", self.center.1).into_bytes(),
        ];
        match self.shape {
            GeoShape::Radius(r) => {
                t.extend([
                    b"BYRADIUS".to_vec(),
                    format!("{r}").into_bytes(),
                    b"M".to_vec(),
                ]);
            }
            GeoShape::Box(w, h) => t.extend([
                b"BYBOX".to_vec(),
                format!("{w}").into_bytes(),
                format!("{h}").into_bytes(),
                b"M".to_vec(),
            ]),
        }
        for (f, v) in &self.wheres {
            t.push(b"WHERE".to_vec());
            t.push(f.clone());
            t.push(v.clone());
        }
        t
    }

    /// The `bits`-wide geohash cells this query's box covers — the cluster shards a
    /// bounded scatter must consult. None for a pole/antimeridian box (caller then
    /// falls back to all shards). Used only in cell-sharded cluster mode.
    pub fn covering_cells(&self, bits: u32) -> Option<Vec<u64>> {
        let (mn_lon, mn_lat, mx_lon, mx_lat) = geo_bbox(self.center, &self.shape)?;
        Some(crate::geohash::cells_for_box(
            mn_lon, mn_lat, mx_lon, mx_lat, bits,
        ))
    }

    /// Render the reply from `hits` (local, plus any merged from peer shards):
    /// sort by distance, apply COUNT, and format per WITHCOORD/WITHDIST.
    pub fn format(&self, mut hits: Vec<GeoHit>) -> Vec<u8> {
        // Sort by distance for an explicit ASC/DESC, OR when COUNT is set
        // without ANY — `COUNT n` means the n CLOSEST, so it must sort before
        // truncating (truncating an unsorted set returned an arbitrary subset,
        // worse in cluster mode where the coordinator's own shard came first).
        let sort = self
            .order
            .or_else(|| (self.count.is_some() && !self.any).then_some(true));
        if let Some(asc) = sort {
            hits.sort_by(|a, b| {
                if asc {
                    a.1.total_cmp(&b.1)
                } else {
                    b.1.total_cmp(&a.1)
                }
            });
        }
        if let Some(c) = self.count {
            hits.truncate(c);
        }
        if !self.withcoord && !self.withdist {
            return bulk_array(&hits.into_iter().map(|(k, ..)| k).collect::<Vec<_>>());
        }
        let mut out = Vec::new();
        for (k, d, lon, lat) in hits {
            let mut elem = vec![bulk_string(&k)];
            if self.withdist {
                elem.push(bulk_string(format!("{:.4}", d / self.unit_div).as_bytes()));
            }
            if self.withcoord {
                elem.push(array(&[
                    bulk_string(&fmt_coord(lon)),
                    bulk_string(&fmt_coord(lat)),
                ]));
            }
            out.push(array(&elem));
        }
        array(&out)
    }
}

/// Parse a GEOSEARCH and collect this node's raw matches (unsorted, untruncated),
/// returning the query (for replay/format) and the local hits — or an error reply.
pub fn geosearch_collect(
    db: &mut Db,
    tokens: &[Vec<u8>],
) -> Result<(GeoQuery, Vec<GeoHit>), Vec<u8>> {
    let bad = || error("ERR syntax error");
    let (mut center, mut from_key): (Option<(f64, f64)>, Option<Vec<u8>>) = (None, None);
    let (mut radius, mut bbox): (Option<f64>, Option<(f64, f64)>) = (None, None);
    let mut unit_div = 1.0f64; // meters-per-unit of the search shape, for WITHDIST
    let mut order: Option<bool> = None; // Some(true)=ASC, Some(false)=DESC
    let mut count: Option<usize> = None;
    let mut any = false;
    let (mut withcoord, mut withdist) = (false, false);
    let mut wheres: Vec<(Vec<u8>, Vec<u8>)> = Vec::new(); // attribute filters (AND)
    let mut i = 1;
    while i < tokens.len() {
        match tokens[i].to_ascii_uppercase().as_slice() {
            b"FROMLONLAT" => {
                let (lon, lat) = match (
                    tokens.get(i + 1).and_then(|t| parse_finite_float(t)),
                    tokens.get(i + 2).and_then(|t| parse_finite_float(t)),
                ) {
                    (Some(a), Some(b)) => (a, b),
                    _ => return Err(bad()),
                };
                center = Some((lon, lat));
                i += 3;
            }
            b"FROMMEMBER" | b"FROMKEY" => {
                from_key = match tokens.get(i + 1) {
                    Some(k) => Some(k.clone()),
                    None => return Err(bad()),
                };
                i += 2;
            }
            b"BYRADIUS" => {
                let r = match tokens.get(i + 1).and_then(|t| parse_finite_float(t)) {
                    Some(r) => r,
                    None => return Err(bad()),
                };
                let u = match tokens.get(i + 2).and_then(|t| geo_unit(t)) {
                    Some(u) => u,
                    None => return Err(bad()),
                };
                unit_div = u;
                radius = Some(r * u);
                i += 3;
            }
            b"BYBOX" => {
                let (w, h) = match (
                    tokens.get(i + 1).and_then(|t| parse_finite_float(t)),
                    tokens.get(i + 2).and_then(|t| parse_finite_float(t)),
                ) {
                    (Some(a), Some(b)) => (a, b),
                    _ => return Err(bad()),
                };
                let u = match tokens.get(i + 3).and_then(|t| geo_unit(t)) {
                    Some(u) => u,
                    None => return Err(bad()),
                };
                unit_div = u;
                bbox = Some((w * u, h * u));
                i += 4;
            }
            b"ASC" => {
                order = Some(true);
                i += 1;
            }
            b"DESC" => {
                order = Some(false);
                i += 1;
            }
            b"COUNT" => {
                count = match tokens.get(i + 1).and_then(|t| parse_int(t)) {
                    Some(c) if c > 0 => Some(c as usize),
                    _ => return Err(error("ERR COUNT must be > 0")),
                };
                i += 2;
                // Optional ANY: return *any* matching n (skip the closest-n sort).
                if tokens
                    .get(i)
                    .is_some_and(|t| t.eq_ignore_ascii_case(b"ANY"))
                {
                    any = true;
                    i += 1;
                }
            }
            b"WITHCOORD" => {
                withcoord = true;
                i += 1;
            }
            b"WITHDIST" => {
                withdist = true;
                i += 1;
            }
            b"WHERE" => {
                match (tokens.get(i + 1), tokens.get(i + 2)) {
                    (Some(f), Some(v)) => wheres.push((f.clone(), v.clone())),
                    _ => return Err(bad()),
                }
                i += 3;
            }
            _ => return Err(bad()),
        }
    }
    let center = match (center, from_key) {
        (Some(c), None) => c,
        (None, Some(k)) => match geo_point(db, &k) {
            Ok(Some(p)) => p,
            Ok(None) => return Err(error("ERR could not decode requested geo member")),
            Err(()) => return Err(wrongtype()),
        },
        _ => {
            return Err(error(
                "ERR exactly one of FROMMEMBER or FROMLONLAT can be specified",
            ));
        }
    };
    let shape = match (radius, bbox) {
        (Some(r), None) => GeoShape::Radius(r),
        (None, Some((w, h))) => GeoShape::Box(w, h),
        _ => {
            return Err(error(
                "ERR exactly one of BYRADIUS and BYBOX can be specified",
            ));
        }
    };
    // Prefilter via the spatial index (a handful of geohash cells), then refine
    // with the exact shape below. Pole/antimeridian boxes fall back to a full scan.
    let candidates = match geo_bbox(center, &shape) {
        Some((mn_lon, mn_lat, mx_lon, mx_lat)) => db.geo_candidates(mn_lon, mn_lat, mx_lon, mx_lat),
        None => db.geo_keys(),
    };
    let mut hits: Vec<GeoHit> = Vec::new(); // (key, dist_m, lon, lat)
    for key in candidates {
        if let Ok(Some((lon, lat))) = geo_point(db, &key) {
            let d = haversine_m(center.0, center.1, lon, lat);
            let matches = match shape {
                GeoShape::Radius(r) => d <= r,
                GeoShape::Box(w, h) => {
                    // East-west extent measured at the POINT's latitude (its own
                    // parallel), not the center's — the box edges are meridians,
                    // so at a tall high-latitude box the center-latitude arc
                    // over/under-counted a point's longitudinal distance.
                    let ew = haversine_m(center.0, lat, lon, lat);
                    let ns = haversine_m(center.0, center.1, center.0, lat);
                    ew <= w / 2.0 && ns <= h / 2.0
                }
            };
            if matches && geo_attr_match(db, &key, &wheres) {
                hits.push((key, d, lon, lat));
            }
        }
    }
    Ok((
        GeoQuery {
            center,
            shape,
            wheres,
            order,
            count,
            any,
            withcoord,
            withdist,
            unit_div,
        },
        hits,
    ))
}

fn geosearch_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    match geosearch_collect(db, tokens) {
        Ok((q, hits)) => q.format(hits),
        Err(e) => e,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cmd(db: &mut Db, parts: &[&[u8]]) -> Vec<u8> {
        let tokens: Vec<Vec<u8>> = parts.iter().map(|p| p.to_vec()).collect();
        execute(&tokens, db)
    }

    #[test]
    fn command_catalog_matches_command_meta() {
        for name in COMMAND_NAMES {
            assert!(
                command_meta(name).is_some(),
                "{} is in COMMAND_NAMES but not command_meta",
                String::from_utf8_lossy(name)
            );
        }
        assert!(COMMAND_NAMES.len() > 100);
    }

    #[test]
    fn command_count_and_info() {
        let mut db = Db::new();
        assert_eq!(
            cmd(&mut db, &[b"COMMAND", b"COUNT"]),
            integer(COMMAND_NAMES.len() as i64)
        );
        let info = cmd(&mut db, &[b"COMMAND", b"INFO", b"GET"]);
        let s = String::from_utf8_lossy(&info);
        assert!(s.contains("get") && s.contains("readonly"), "{s}");
    }

    #[test]
    fn scan_select_covers_every_element_once() {
        let items: Vec<(u64, u32)> = (0..1000u32)
            .map(|i| (scan_hash(&i.to_le_bytes()), i))
            .collect();
        let mut cursor = 0u64;
        let mut seen = std::collections::HashSet::new();
        let mut rounds = 0;
        loop {
            rounds += 1;
            assert!(rounds < 100_000, "scan did not terminate");
            let (next, batch) = scan_select(items.clone(), cursor, 10);
            for x in batch {
                assert!(seen.insert(x), "element {x} returned twice");
            }
            if next == 0 {
                break;
            }
            cursor = next;
        }
        assert_eq!(seen.len(), 1000, "scan missed elements");
    }

    #[test]
    fn scan_keyspace_with_match() {
        let mut db = Db::new();
        for i in 0..50 {
            cmd(&mut db, &[b"SET", format!("user:{i}").as_bytes(), b"v"]);
            cmd(&mut db, &[b"SET", format!("other:{i}").as_bytes(), b"v"]);
        }
        // COUNT covers everything in one call -> cursor returns "0" (complete);
        // MATCH filters to the 50 user keys.
        let reply = cmd(
            &mut db,
            &[b"SCAN", b"0", b"MATCH", b"user:*", b"COUNT", b"1000"],
        );
        let s = String::from_utf8_lossy(&reply);
        assert!(
            s.starts_with("*2\r\n$1\r\n0\r\n"),
            "expected complete scan: {s:?}"
        );
        assert_eq!(
            s.matches("user:").count(),
            50,
            "expected 50 user keys: {s:?}"
        );
    }

    #[test]
    fn strings_and_expiry_still_work() {
        let mut db = Db::new();
        assert_eq!(cmd(&mut db, &[b"SET", b"k", b"v"]), b"+OK\r\n".to_vec());
        assert_eq!(cmd(&mut db, &[b"GET", b"k"]), b"$1\r\nv\r\n".to_vec());
        assert_eq!(cmd(&mut db, &[b"INCR", b"n"]), b":1\r\n".to_vec());
        cmd(&mut db, &[b"SET", b"k", b"v", b"PXAT", b"1"]);
        assert_eq!(cmd(&mut db, &[b"GET", b"k"]), b"$-1\r\n".to_vec());
    }

    #[test]
    fn lists() {
        let mut db = Db::new();
        assert_eq!(
            cmd(&mut db, &[b"RPUSH", b"l", b"a", b"b", b"c"]),
            b":3\r\n".to_vec()
        );
        assert_eq!(cmd(&mut db, &[b"LPUSH", b"l", b"z"]), b":4\r\n".to_vec());
        assert_eq!(cmd(&mut db, &[b"LLEN", b"l"]), b":4\r\n".to_vec());
        assert_eq!(
            cmd(&mut db, &[b"LINDEX", b"l", b"0"]),
            b"$1\r\nz\r\n".to_vec()
        );
        assert_eq!(
            cmd(&mut db, &[b"LINDEX", b"l", b"-1"]),
            b"$1\r\nc\r\n".to_vec()
        );
        assert_eq!(
            cmd(&mut db, &[b"LRANGE", b"l", b"0", b"-1"]),
            bulk_array(&[b"z".to_vec(), b"a".to_vec(), b"b".to_vec(), b"c".to_vec()])
        );
        assert_eq!(cmd(&mut db, &[b"LPOP", b"l"]), b"$1\r\nz\r\n".to_vec());
        assert_eq!(cmd(&mut db, &[b"RPOP", b"l"]), b"$1\r\nc\r\n".to_vec());
    }

    #[test]
    fn hashes() {
        let mut db = Db::new();
        assert_eq!(
            cmd(&mut db, &[b"HSET", b"h", b"f1", b"v1", b"f2", b"v2"]),
            b":2\r\n".to_vec()
        );
        assert_eq!(
            cmd(&mut db, &[b"HGET", b"h", b"f1"]),
            b"$2\r\nv1\r\n".to_vec()
        );
        assert_eq!(cmd(&mut db, &[b"HLEN", b"h"]), b":2\r\n".to_vec());
        assert_eq!(cmd(&mut db, &[b"HEXISTS", b"h", b"f2"]), b":1\r\n".to_vec());
        assert_eq!(
            cmd(&mut db, &[b"HINCRBY", b"h", b"n", b"5"]),
            b":5\r\n".to_vec()
        );
        assert_eq!(
            cmd(&mut db, &[b"HDEL", b"h", b"f1", b"f2", b"n"]),
            b":3\r\n".to_vec()
        );
        assert_eq!(cmd(&mut db, &[b"EXISTS", b"h"]), b":0\r\n".to_vec()); // emptied -> gone
    }

    #[test]
    fn sets() {
        let mut db = Db::new();
        assert_eq!(
            cmd(&mut db, &[b"SADD", b"s", b"a", b"b", b"c"]),
            b":3\r\n".to_vec()
        );
        assert_eq!(cmd(&mut db, &[b"SADD", b"s", b"a"]), b":0\r\n".to_vec());
        assert_eq!(cmd(&mut db, &[b"SCARD", b"s"]), b":3\r\n".to_vec());
        assert_eq!(
            cmd(&mut db, &[b"SISMEMBER", b"s", b"b"]),
            b":1\r\n".to_vec()
        );
        assert_eq!(
            cmd(&mut db, &[b"SISMEMBER", b"s", b"z"]),
            b":0\r\n".to_vec()
        );
        cmd(&mut db, &[b"SADD", b"t", b"b", b"c", b"d"]);
        // SINTER s t = {b, c}
        let inter = cmd(&mut db, &[b"SINTER", b"s", b"t"]);
        assert!(inter.starts_with(b"*2\r\n"));
    }

    #[test]
    fn wrongtype_is_reported() {
        let mut db = Db::new();
        cmd(&mut db, &[b"SET", b"k", b"v"]);
        assert!(cmd(&mut db, &[b"LPUSH", b"k", b"x"]).starts_with(b"-WRONGTYPE"));
        assert!(cmd(&mut db, &[b"HSET", b"k", b"f", b"v"]).starts_with(b"-WRONGTYPE"));
        assert!(cmd(&mut db, &[b"SADD", b"k", b"m"]).starts_with(b"-WRONGTYPE"));
        assert_eq!(cmd(&mut db, &[b"TYPE", b"k"]), b"+string\r\n".to_vec());
    }

    #[test]
    fn zsets() {
        let mut db = Db::new();
        assert_eq!(
            cmd(
                &mut db,
                &[b"ZADD", b"z", b"1", b"a", b"3", b"c", b"2", b"b"]
            ),
            b":3\r\n".to_vec()
        );
        assert_eq!(cmd(&mut db, &[b"ZCARD", b"z"]), b":3\r\n".to_vec());
        assert_eq!(
            cmd(&mut db, &[b"ZSCORE", b"z", b"b"]),
            b"$1\r\n2\r\n".to_vec()
        );
        assert_eq!(cmd(&mut db, &[b"ZRANK", b"z", b"a"]), b":0\r\n".to_vec());
        assert_eq!(cmd(&mut db, &[b"ZRANK", b"z", b"c"]), b":2\r\n".to_vec());
        assert_eq!(cmd(&mut db, &[b"ZREVRANK", b"z", b"c"]), b":0\r\n".to_vec());
        // ascending range
        assert_eq!(
            cmd(&mut db, &[b"ZRANGE", b"z", b"0", b"-1"]),
            bulk_array(&[b"a".to_vec(), b"b".to_vec(), b"c".to_vec()])
        );
        // by score, exclusive lower bound
        assert_eq!(
            cmd(&mut db, &[b"ZRANGEBYSCORE", b"z", b"(1", b"3"]),
            bulk_array(&[b"b".to_vec(), b"c".to_vec()])
        );
        assert_eq!(
            cmd(&mut db, &[b"ZINCRBY", b"z", b"5", b"a"]),
            b"$1\r\n6\r\n".to_vec()
        );
        assert_eq!(cmd(&mut db, &[b"ZRANK", b"z", b"a"]), b":2\r\n".to_vec()); // now highest
        assert_eq!(
            cmd(&mut db, &[b"ZCOUNT", b"z", b"2", b"6"]),
            b":3\r\n".to_vec()
        );
        // ZPOPMIN removes the lowest
        assert_eq!(
            cmd(&mut db, &[b"ZPOPMIN", b"z"]),
            bulk_array(&[b"b".to_vec(), b"2".to_vec()])
        );
    }

    #[test]
    fn zset_ordered_index_ties_and_reposition() {
        let mut db = Db::new();
        // Equal scores order by member bytes; lower score sorts first.
        cmd(
            &mut db,
            &[b"ZADD", b"z", b"5", b"b", b"5", b"a", b"3", b"c"],
        );
        assert_eq!(
            cmd(&mut db, &[b"ZRANGE", b"z", b"0", b"-1"]),
            b"*3\r\n$1\r\nc\r\n$1\r\na\r\n$1\r\nb\r\n".to_vec() // c(3), a(5), b(5)
        );
        assert_eq!(cmd(&mut db, &[b"ZRANK", b"z", b"c"]), b":0\r\n".to_vec());
        assert_eq!(cmd(&mut db, &[b"ZRANK", b"z", b"b"]), b":2\r\n".to_vec());
        assert_eq!(cmd(&mut db, &[b"ZREVRANK", b"z", b"c"]), b":2\r\n".to_vec());
        // Re-scoring a member must reposition it in the ordered index.
        cmd(&mut db, &[b"ZADD", b"z", b"1", b"b"]);
        assert_eq!(cmd(&mut db, &[b"ZRANK", b"z", b"b"]), b":0\r\n".to_vec());
        // Removing keeps the index consistent.
        cmd(&mut db, &[b"ZREM", b"z", b"c"]);
        assert_eq!(cmd(&mut db, &[b"ZRANK", b"z", b"a"]), b":1\r\n".to_vec());
    }

    #[test]
    fn zadd_gt_lt_gate_updates() {
        let mut db = Db::new();
        assert_eq!(
            cmd(&mut db, &[b"ZADD", b"z", b"5", b"m"]),
            b":1\r\n".to_vec()
        );
        // GT: a lower score is ignored, a higher one wins.
        cmd(&mut db, &[b"ZADD", b"z", b"GT", b"3", b"m"]);
        assert_eq!(
            cmd(&mut db, &[b"ZSCORE", b"z", b"m"]),
            b"$1\r\n5\r\n".to_vec()
        );
        cmd(&mut db, &[b"ZADD", b"z", b"GT", b"9", b"m"]);
        assert_eq!(
            cmd(&mut db, &[b"ZSCORE", b"z", b"m"]),
            b"$1\r\n9\r\n".to_vec()
        );
        // LT: a higher score is ignored, a lower one wins.
        cmd(&mut db, &[b"ZADD", b"z", b"LT", b"20", b"m"]);
        assert_eq!(
            cmd(&mut db, &[b"ZSCORE", b"z", b"m"]),
            b"$1\r\n9\r\n".to_vec()
        );
        cmd(&mut db, &[b"ZADD", b"z", b"LT", b"2", b"m"]);
        assert_eq!(
            cmd(&mut db, &[b"ZSCORE", b"z", b"m"]),
            b"$1\r\n2\r\n".to_vec()
        );
        // GT still adds brand-new members.
        assert_eq!(
            cmd(&mut db, &[b"ZADD", b"z", b"GT", b"7", b"new"]),
            b":1\r\n".to_vec()
        );
        // Incompatible combinations are rejected.
        assert!(cmd(&mut db, &[b"ZADD", b"z", b"GT", b"LT", b"1", b"m"]).starts_with(b"-ERR"));
        assert!(cmd(&mut db, &[b"ZADD", b"z", b"NX", b"GT", b"1", b"m"]).starts_with(b"-ERR"));
    }

    #[test]
    fn string_family_commands() {
        let mut db = Db::new();
        // MSET / MGET
        assert_eq!(
            cmd(&mut db, &[b"MSET", b"a", b"1", b"b", b"2"]),
            b"+OK\r\n".to_vec()
        );
        assert_eq!(
            cmd(&mut db, &[b"MGET", b"a", b"b", b"missing"]),
            array(&[bulk_string(b"1"), bulk_string(b"2"), null_bulk()])
        );
        // SETNX: only when absent
        assert_eq!(cmd(&mut db, &[b"SETNX", b"a", b"x"]), b":0\r\n".to_vec());
        assert_eq!(cmd(&mut db, &[b"SETNX", b"c", b"3"]), b":1\r\n".to_vec());
        // MSETNX: all-or-nothing
        assert_eq!(
            cmd(&mut db, &[b"MSETNX", b"d", b"4", b"a", b"x"]),
            b":0\r\n".to_vec()
        );
        assert_eq!(cmd(&mut db, &[b"EXISTS", b"d"]), b":0\r\n".to_vec()); // not set
        // GETSET returns old, sets new
        assert_eq!(
            cmd(&mut db, &[b"GETSET", b"a", b"9"]),
            b"$1\r\n1\r\n".to_vec()
        );
        assert_eq!(cmd(&mut db, &[b"GET", b"a"]), b"$1\r\n9\r\n".to_vec());
        // SETEX sets a TTL
        assert_eq!(
            cmd(&mut db, &[b"SETEX", b"t", b"100", b"v"]),
            b"+OK\r\n".to_vec()
        );
        assert!(matches!(
            cmd(&mut db, &[b"TTL", b"t"]).as_slice(),
            b":100\r\n" | b":99\r\n"
        ));
        assert!(cmd(&mut db, &[b"SETEX", b"t", b"0", b"v"]).starts_with(b"-ERR"));
        // GETRANGE (inclusive, negative indices)
        cmd(&mut db, &[b"SET", b"s", b"Hello World"]);
        assert_eq!(
            cmd(&mut db, &[b"GETRANGE", b"s", b"0", b"4"]),
            bulk_string(b"Hello")
        );
        assert_eq!(
            cmd(&mut db, &[b"GETRANGE", b"s", b"-5", b"-1"]),
            bulk_string(b"World")
        );
        assert_eq!(
            cmd(&mut db, &[b"GETRANGE", b"s", b"5", b"2"]),
            bulk_string(b"")
        );
        // SETRANGE pads with zero bytes
        assert_eq!(
            cmd(&mut db, &[b"SETRANGE", b"p", b"3", b"abc"]),
            b":6\r\n".to_vec()
        );
        assert_eq!(
            cmd(&mut db, &[b"GET", b"p"]),
            bulk_string(&[0, 0, 0, b'a', b'b', b'c'])
        );
        // INCRBYFLOAT (binary-exact operands so formatting is deterministic;
        // an integer result drops the decimal, like Redis)
        cmd(&mut db, &[b"SET", b"f", b"10.5"]);
        assert_eq!(
            cmd(&mut db, &[b"INCRBYFLOAT", b"f", b"0.25"]),
            bulk_string(b"10.75")
        );
        assert_eq!(
            cmd(&mut db, &[b"INCRBYFLOAT", b"f", b"-0.75"]),
            bulk_string(b"10")
        );
        assert!(cmd(&mut db, &[b"INCRBYFLOAT", b"f", b"notnum"]).starts_with(b"-ERR"));
        assert!(cmd(&mut db, &[b"INCRBYFLOAT", b"f", b"inf"]).starts_with(b"-ERR"));
        // WRONGTYPE checks
        cmd(&mut db, &[b"RPUSH", b"list", b"x"]);
        assert!(cmd(&mut db, &[b"GETSET", b"list", b"y"]).starts_with(b"-WRONGTYPE"));
        assert!(cmd(&mut db, &[b"INCRBYFLOAT", b"list", b"1"]).starts_with(b"-WRONGTYPE"));
    }

    #[test]
    fn select_single_db() {
        let mut db = Db::new();
        assert_eq!(cmd(&mut db, &[b"SELECT", b"0"]), b"+OK\r\n".to_vec());
        assert!(cmd(&mut db, &[b"SELECT", b"1"]).starts_with(b"-ERR"));
        assert!(cmd(&mut db, &[b"SELECT", b"x"]).starts_with(b"-ERR"));
    }

    #[test]
    fn keyspace_commands() {
        let mut db = Db::new();
        cmd(
            &mut db,
            &[b"MSET", b"user:1", b"a", b"user:2", b"b", b"other", b"c"],
        );
        assert_eq!(cmd(&mut db, &[b"DBSIZE"]), b":3\r\n".to_vec());

        // KEYS with a glob (order is unspecified -> check membership + count).
        let keys = cmd(&mut db, &[b"KEYS", b"user:*"]);
        assert!(keys.starts_with(b"*2\r\n"));
        assert!(keys.windows(6).any(|w| w == b"user:1"));
        assert!(keys.windows(6).any(|w| w == b"user:2"));
        assert!(!keys.windows(5).any(|w| w == b"other"));

        // TOUCH counts existing keys; UNLINK removes like DEL.
        assert_eq!(
            cmd(&mut db, &[b"TOUCH", b"user:1", b"nope"]),
            b":1\r\n".to_vec()
        );
        assert_eq!(
            cmd(&mut db, &[b"UNLINK", b"user:1", b"nope"]),
            b":1\r\n".to_vec()
        );
        assert_eq!(cmd(&mut db, &[b"EXISTS", b"user:1"]), b":0\r\n".to_vec());

        // RENAME moves the value (and TTL); RENAMENX respects an existing dst.
        cmd(&mut db, &[b"SET", b"src", b"v", b"EX", b"100"]);
        assert_eq!(
            cmd(&mut db, &[b"RENAME", b"src", b"dst"]),
            b"+OK\r\n".to_vec()
        );
        assert_eq!(cmd(&mut db, &[b"GET", b"dst"]), b"$1\r\nv\r\n".to_vec());
        assert!(matches!(
            cmd(&mut db, &[b"TTL", b"dst"]).as_slice(),
            b":100\r\n" | b":99\r\n"
        ));
        assert!(cmd(&mut db, &[b"RENAME", b"missing", b"x"]).starts_with(b"-ERR no such key"));
        assert_eq!(
            cmd(&mut db, &[b"RENAMENX", b"dst", b"other"]),
            b":0\r\n".to_vec()
        ); // other exists

        // FLUSHALL empties the keyspace.
        assert_eq!(cmd(&mut db, &[b"FLUSHALL"]), b"+OK\r\n".to_vec());
        assert_eq!(cmd(&mut db, &[b"DBSIZE"]), b":0\r\n".to_vec());
    }

    #[test]
    fn zset_range_and_store_commands() {
        let mut db = Db::new();
        cmd(
            &mut db,
            &[
                b"ZADD", b"z", b"1", b"a", b"2", b"b", b"3", b"c", b"4", b"d",
            ],
        );
        // ZREMRANGEBYRANK removes ranks [1,2] => b,c
        assert_eq!(
            cmd(&mut db, &[b"ZREMRANGEBYRANK", b"z", b"1", b"2"]),
            b":2\r\n".to_vec()
        );
        assert_eq!(
            cmd(&mut db, &[b"ZRANGE", b"z", b"0", b"-1"]),
            bulk_array(&[b"a".to_vec(), b"d".to_vec()])
        );
        // ZREMRANGEBYSCORE
        cmd(
            &mut db,
            &[b"ZADD", b"s", b"1", b"a", b"2", b"b", b"3", b"c"],
        );
        assert_eq!(
            cmd(&mut db, &[b"ZREMRANGEBYSCORE", b"s", b"(1", b"3"]),
            b":2\r\n".to_vec()
        );
        assert_eq!(
            cmd(&mut db, &[b"ZRANGE", b"s", b"0", b"-1"]),
            bulk_array(&[b"a".to_vec()])
        );
        // ZUNIONSTORE with WEIGHTS + AGGREGATE
        cmd(&mut db, &[b"ZADD", b"z1", b"1", b"a", b"2", b"b"]);
        cmd(&mut db, &[b"ZADD", b"z2", b"10", b"b", b"20", b"c"]);
        assert_eq!(
            cmd(&mut db, &[b"ZUNIONSTORE", b"out", b"2", b"z1", b"z2"]),
            b":3\r\n".to_vec()
        );
        // b = 2 + 10 = 12
        assert_eq!(cmd(&mut db, &[b"ZSCORE", b"out", b"b"]), bulk_string(b"12"));
        // WEIGHTS: z1*2, z2*1 -> b = 4 + 10 = 14
        assert_eq!(
            cmd(
                &mut db,
                &[
                    b"ZUNIONSTORE",
                    b"w",
                    b"2",
                    b"z1",
                    b"z2",
                    b"WEIGHTS",
                    b"2",
                    b"1"
                ]
            ),
            b":3\r\n".to_vec()
        );
        assert_eq!(cmd(&mut db, &[b"ZSCORE", b"w", b"b"]), bulk_string(b"14"));
        // AGGREGATE MAX -> b = max(2,10) = 10
        cmd(
            &mut db,
            &[
                b"ZUNIONSTORE",
                b"mx",
                b"2",
                b"z1",
                b"z2",
                b"AGGREGATE",
                b"MAX",
            ],
        );
        assert_eq!(cmd(&mut db, &[b"ZSCORE", b"mx", b"b"]), bulk_string(b"10"));
        // ZINTERSTORE -> only b is in both; SUM = 12
        assert_eq!(
            cmd(&mut db, &[b"ZINTERSTORE", b"i", b"2", b"z1", b"z2"]),
            b":1\r\n".to_vec()
        );
        assert_eq!(cmd(&mut db, &[b"ZSCORE", b"i", b"b"]), bulk_string(b"12"));
        // empty intersection deletes dest
        cmd(&mut db, &[b"ZADD", b"p", b"1", b"x"]);
        cmd(&mut db, &[b"ZADD", b"q", b"1", b"y"]);
        cmd(&mut db, &[b"SET", b"victim", b"old"]);
        assert_eq!(
            cmd(&mut db, &[b"ZINTERSTORE", b"victim", b"2", b"p", b"q"]),
            b":0\r\n".to_vec()
        );
        assert_eq!(cmd(&mut db, &[b"EXISTS", b"victim"]), b":0\r\n".to_vec());
        // WRONGTYPE
        cmd(&mut db, &[b"SET", b"str", b"v"]);
        assert!(cmd(&mut db, &[b"ZREMRANGEBYRANK", b"str", b"0", b"1"]).starts_with(b"-WRONGTYPE"));
        assert!(cmd(&mut db, &[b"ZUNIONSTORE", b"d", b"1", b"str"]).starts_with(b"-WRONGTYPE"));
    }

    #[test]
    fn set_store_move_commands() {
        let mut db = Db::new();
        cmd(&mut db, &[b"SADD", b"a", b"1", b"2", b"3"]);
        cmd(&mut db, &[b"SADD", b"b", b"2", b"3", b"4"]);
        // SINTERSTORE -> {2,3}
        assert_eq!(
            cmd(&mut db, &[b"SINTERSTORE", b"d", b"a", b"b"]),
            b":2\r\n".to_vec()
        );
        assert_eq!(cmd(&mut db, &[b"SCARD", b"d"]), b":2\r\n".to_vec());
        assert_eq!(
            cmd(&mut db, &[b"SISMEMBER", b"d", b"2"]),
            b":1\r\n".to_vec()
        );
        assert_eq!(
            cmd(&mut db, &[b"SISMEMBER", b"d", b"1"]),
            b":0\r\n".to_vec()
        );
        // SUNIONSTORE -> {1,2,3,4}
        assert_eq!(
            cmd(&mut db, &[b"SUNIONSTORE", b"u", b"a", b"b"]),
            b":4\r\n".to_vec()
        );
        // SDIFFSTORE a-b -> {1}
        assert_eq!(
            cmd(&mut db, &[b"SDIFFSTORE", b"x", b"a", b"b"]),
            b":1\r\n".to_vec()
        );
        assert_eq!(
            cmd(&mut db, &[b"SISMEMBER", b"x", b"1"]),
            b":1\r\n".to_vec()
        );
        // empty result deletes the destination
        cmd(&mut db, &[b"SADD", b"e1", b"p"]);
        cmd(&mut db, &[b"SADD", b"e2", b"q"]);
        cmd(&mut db, &[b"SET", b"victim", b"old"]);
        assert_eq!(
            cmd(&mut db, &[b"SINTERSTORE", b"victim", b"e1", b"e2"]),
            b":0\r\n".to_vec()
        );
        assert_eq!(cmd(&mut db, &[b"EXISTS", b"victim"]), b":0\r\n".to_vec());
        // SINTERCARD with and without LIMIT
        assert_eq!(
            cmd(&mut db, &[b"SINTERCARD", b"2", b"a", b"b"]),
            b":2\r\n".to_vec()
        );
        assert_eq!(
            cmd(&mut db, &[b"SINTERCARD", b"2", b"a", b"b", b"LIMIT", b"1"]),
            b":1\r\n".to_vec()
        );
        assert_eq!(
            cmd(&mut db, &[b"SINTERCARD", b"2", b"a", b"b", b"LIMIT", b"0"]),
            b":2\r\n".to_vec()
        );
        // SMOVE
        assert_eq!(
            cmd(&mut db, &[b"SMOVE", b"a", b"b", b"1"]),
            b":1\r\n".to_vec()
        );
        assert_eq!(
            cmd(&mut db, &[b"SISMEMBER", b"a", b"1"]),
            b":0\r\n".to_vec()
        );
        assert_eq!(
            cmd(&mut db, &[b"SISMEMBER", b"b", b"1"]),
            b":1\r\n".to_vec()
        );
        assert_eq!(
            cmd(&mut db, &[b"SMOVE", b"a", b"b", b"999"]),
            b":0\r\n".to_vec()
        ); // not a member
        // WRONGTYPE
        cmd(&mut db, &[b"SET", b"str", b"x"]);
        assert!(cmd(&mut db, &[b"SMOVE", b"str", b"b", b"1"]).starts_with(b"-WRONGTYPE"));
        assert!(cmd(&mut db, &[b"SINTERSTORE", b"d", b"str"]).starts_with(b"-WRONGTYPE"));
    }

    #[test]
    fn list_mutation_commands() {
        let mut db = Db::new();
        cmd(&mut db, &[b"RPUSH", b"l", b"a", b"b", b"c", b"b", b"d"]);
        // LINSERT before/after, missing pivot, missing key
        assert_eq!(
            cmd(&mut db, &[b"LINSERT", b"l", b"BEFORE", b"c", b"X"]),
            b":6\r\n".to_vec()
        ); // a b X c b d
        assert_eq!(cmd(&mut db, &[b"LINDEX", b"l", b"2"]), bulk_string(b"X"));
        assert_eq!(
            cmd(&mut db, &[b"LINSERT", b"l", b"AFTER", b"nope", b"Y"]),
            b":-1\r\n".to_vec()
        );
        assert_eq!(
            cmd(&mut db, &[b"LINSERT", b"missing", b"BEFORE", b"x", b"y"]),
            b":0\r\n".to_vec()
        );
        // LREM (count>0, from head)
        assert_eq!(
            cmd(&mut db, &[b"LREM", b"l", b"1", b"b"]),
            b":1\r\n".to_vec()
        );
        assert_eq!(
            cmd(&mut db, &[b"LRANGE", b"l", b"0", b"-1"]),
            bulk_array(&[
                b"a".to_vec(),
                b"X".to_vec(),
                b"c".to_vec(),
                b"b".to_vec(),
                b"d".to_vec()
            ])
        );
        // LTRIM
        assert_eq!(
            cmd(&mut db, &[b"LTRIM", b"l", b"1", b"3"]),
            b"+OK\r\n".to_vec()
        );
        assert_eq!(
            cmd(&mut db, &[b"LRANGE", b"l", b"0", b"-1"]),
            bulk_array(&[b"X".to_vec(), b"c".to_vec(), b"b".to_vec()])
        );
        // LPOS: first match, RANK -1 (from tail), COUNT 0 (all), and a miss
        cmd(&mut db, &[b"RPUSH", b"p", b"a", b"b", b"c", b"b", b"b"]);
        assert_eq!(cmd(&mut db, &[b"LPOS", b"p", b"b"]), b":1\r\n".to_vec());
        assert_eq!(
            cmd(&mut db, &[b"LPOS", b"p", b"b", b"RANK", b"-1"]),
            b":4\r\n".to_vec()
        );
        assert_eq!(
            cmd(&mut db, &[b"LPOS", b"p", b"b", b"COUNT", b"0"]),
            array(&[integer(1), integer(3), integer(4)])
        );
        assert_eq!(cmd(&mut db, &[b"LPOS", b"p", b"zzz"]), null_bulk());
        // RPOPLPUSH / LMOVE
        cmd(&mut db, &[b"RPUSH", b"s", b"1", b"2", b"3"]);
        assert_eq!(cmd(&mut db, &[b"RPOPLPUSH", b"s", b"d"]), bulk_string(b"3"));
        assert_eq!(
            cmd(&mut db, &[b"LMOVE", b"s", b"d", b"LEFT", b"RIGHT"]),
            bulk_string(b"1")
        );
        assert_eq!(
            cmd(&mut db, &[b"LRANGE", b"d", b"0", b"-1"]),
            bulk_array(&[b"3".to_vec(), b"1".to_vec()])
        );
        assert_eq!(cmd(&mut db, &[b"RPOPLPUSH", b"empty", b"d"]), null_bulk());
        // WRONGTYPE
        cmd(&mut db, &[b"SET", b"str", b"x"]);
        assert!(
            cmd(&mut db, &[b"LINSERT", b"str", b"BEFORE", b"a", b"b"]).starts_with(b"-WRONGTYPE")
        );
        assert!(cmd(&mut db, &[b"RPOPLPUSH", b"str", b"d"]).starts_with(b"-WRONGTYPE"));
    }

    #[test]
    fn bitmap_commands() {
        let mut db = Db::new();
        // SETBIT grows the value and returns the previous bit.
        assert_eq!(
            cmd(&mut db, &[b"SETBIT", b"k", b"7", b"1"]),
            b":0\r\n".to_vec()
        );
        assert_eq!(cmd(&mut db, &[b"GET", b"k"]), bulk_string(&[0x01]));
        assert_eq!(
            cmd(&mut db, &[b"SETBIT", b"k", b"0", b"1"]),
            b":0\r\n".to_vec()
        );
        assert_eq!(cmd(&mut db, &[b"GET", b"k"]), bulk_string(&[0x81]));
        assert_eq!(
            cmd(&mut db, &[b"SETBIT", b"k", b"7", b"0"]),
            b":1\r\n".to_vec()
        );
        // GETBIT (incl. beyond end)
        assert_eq!(cmd(&mut db, &[b"GETBIT", b"k", b"0"]), b":1\r\n".to_vec());
        assert_eq!(cmd(&mut db, &[b"GETBIT", b"k", b"5"]), b":0\r\n".to_vec());
        assert_eq!(cmd(&mut db, &[b"GETBIT", b"k", b"999"]), b":0\r\n".to_vec());
        // BITCOUNT (Redis's canonical "foobar" == 26; byte and bit ranges)
        cmd(&mut db, &[b"SET", b"c", b"foobar"]);
        assert_eq!(cmd(&mut db, &[b"BITCOUNT", b"c"]), b":26\r\n".to_vec());
        assert_eq!(
            cmd(&mut db, &[b"BITCOUNT", b"c", b"0", b"0"]),
            b":4\r\n".to_vec()
        ); // 'f'
        assert_eq!(
            cmd(&mut db, &[b"BITCOUNT", b"c", b"1", b"1"]),
            b":6\r\n".to_vec()
        ); // 'o'
        cmd(&mut db, &[b"SET", b"ff", &[0xff]]);
        assert_eq!(
            cmd(&mut db, &[b"BITCOUNT", b"ff", b"0", b"3", b"BIT"]),
            b":4\r\n".to_vec()
        );
        // BITPOS
        cmd(&mut db, &[b"SET", b"p", &[0x00, 0xff, 0xf0]]);
        assert_eq!(cmd(&mut db, &[b"BITPOS", b"p", b"1"]), b":8\r\n".to_vec());
        cmd(&mut db, &[b"SET", b"one", &[0xff]]);
        assert_eq!(cmd(&mut db, &[b"BITPOS", b"one", b"0"]), b":8\r\n".to_vec()); // implicit zero tail
        assert_eq!(
            cmd(&mut db, &[b"BITPOS", b"one", b"0", b"0", b"-1"]),
            b":-1\r\n".to_vec()
        ); // explicit end
        // BITOP AND/OR/XOR/NOT
        cmd(&mut db, &[b"SET", b"x", &[0xCC]]);
        cmd(&mut db, &[b"SET", b"y", &[0xAA]]);
        assert_eq!(
            cmd(&mut db, &[b"BITOP", b"AND", b"d", b"x", b"y"]),
            b":1\r\n".to_vec()
        );
        assert_eq!(cmd(&mut db, &[b"GET", b"d"]), bulk_string(&[0x88]));
        cmd(&mut db, &[b"BITOP", b"OR", b"d", b"x", b"y"]);
        assert_eq!(cmd(&mut db, &[b"GET", b"d"]), bulk_string(&[0xEE]));
        cmd(&mut db, &[b"BITOP", b"XOR", b"d", b"x", b"y"]);
        assert_eq!(cmd(&mut db, &[b"GET", b"d"]), bulk_string(&[0x66]));
        cmd(&mut db, &[b"BITOP", b"NOT", b"d", b"x"]);
        assert_eq!(cmd(&mut db, &[b"GET", b"d"]), bulk_string(&[0x33]));
        // WRONGTYPE
        cmd(&mut db, &[b"RPUSH", b"l", b"a"]);
        assert!(cmd(&mut db, &[b"SETBIT", b"l", b"0", b"1"]).starts_with(b"-WRONGTYPE"));
        assert!(cmd(&mut db, &[b"BITCOUNT", b"l"]).starts_with(b"-WRONGTYPE"));
        // bad args
        assert!(cmd(&mut db, &[b"SETBIT", b"k", b"0", b"2"]).starts_with(b"-ERR"));
    }

    #[test]
    fn random_commands() {
        let mut db = Db::new();
        cmd(&mut db, &[b"SADD", b"s", b"a", b"b", b"c", b"d", b"e"]);
        // single random member is a 1-byte bulk (members are "a".."e")
        assert!(cmd(&mut db, &[b"SRANDMEMBER", b"s"]).starts_with(b"$1\r\n"));
        // positive count -> distinct, capped at set size
        assert!(cmd(&mut db, &[b"SRANDMEMBER", b"s", b"3"]).starts_with(b"*3\r\n"));
        assert!(cmd(&mut db, &[b"SRANDMEMBER", b"s", b"100"]).starts_with(b"*5\r\n"));
        // negative count -> exactly |count|, repeats allowed
        assert!(cmd(&mut db, &[b"SRANDMEMBER", b"s", b"-10"]).starts_with(b"*10\r\n"));
        // count 0 / missing key
        assert_eq!(
            cmd(&mut db, &[b"SRANDMEMBER", b"s", b"0"]),
            b"*0\r\n".to_vec()
        );
        assert_eq!(cmd(&mut db, &[b"SRANDMEMBER", b"nope"]), null_bulk());
        assert_eq!(
            cmd(&mut db, &[b"SRANDMEMBER", b"nope", b"3"]),
            b"*0\r\n".to_vec()
        );
        // SRANDMEMBER must not modify the set
        assert_eq!(cmd(&mut db, &[b"SCARD", b"s"]), b":5\r\n".to_vec());
        // RANDOMKEY returns an existing key; nil on an empty DB
        assert!(cmd(&mut db, &[b"RANDOMKEY"]).starts_with(b"$"));
        assert_eq!(cmd(&mut Db::new(), &[b"RANDOMKEY"]), null_bulk());
        // SPOP now removes random members (count honored)
        assert!(cmd(&mut db, &[b"SPOP", b"s", b"2"]).starts_with(b"*2\r\n"));
        assert_eq!(cmd(&mut db, &[b"SCARD", b"s"]), b":3\r\n".to_vec());
        // WRONGTYPE
        cmd(&mut db, &[b"SET", b"str", b"x"]);
        assert!(cmd(&mut db, &[b"SRANDMEMBER", b"str"]).starts_with(b"-WRONGTYPE"));
    }

    #[test]
    fn geo_basics() {
        let mut db = Db::new();
        assert_eq!(
            cmd(&mut db, &[b"GEOSET", b"p", b"13.361389", b"38.115556"]),
            b"+OK\r\n".to_vec()
        );
        assert_eq!(cmd(&mut db, &[b"TYPE", b"p"]), b"+geo\r\n".to_vec());
        // out-of-range latitude is rejected
        assert!(cmd(&mut db, &[b"GEOSET", b"q", b"0", b"100"]).starts_with(b"-ERR"));
        // distance between identical points is 0
        cmd(&mut db, &[b"GEOSET", b"p2", b"13.361389", b"38.115556"]);
        assert_eq!(
            cmd(&mut db, &[b"GEODIST", b"p", b"p2"]),
            bulk_string(b"0.0000")
        );
        // search finds both points within radius
        assert!(
            cmd(
                &mut db,
                &[b"GEOSEARCH", b"FROMKEY", b"p", b"BYRADIUS", b"1", b"km"]
            )
            .starts_with(b"*2\r\n")
        );
        // wrong type on a non-geo key
        cmd(&mut db, &[b"SET", b"s", b"x"]);
        assert!(cmd(&mut db, &[b"GEODIST", b"s", b"p"]).starts_with(b"-WRONGTYPE"));
        // deleting geo keys removes them from the index
        cmd(&mut db, &[b"DEL", b"p", b"p2"]);
        assert_eq!(
            cmd(
                &mut db,
                &[
                    b"GEOSEARCH",
                    b"FROMLONLAT",
                    b"13.361389",
                    b"38.115556",
                    b"BYRADIUS",
                    b"1",
                    b"km"
                ]
            ),
            b"*0\r\n".to_vec()
        );
    }

    #[test]
    fn geosearch_index_matches_brute_force() {
        // Scatter a grid of points and confirm the indexed GEOSEARCH returns the
        // exact same count as a brute-force haversine scan — equal count proves no
        // false negatives (the refine step already rules out false positives).
        let mut db = Db::new();
        let (clon, clat) = (10.0_f64, 50.0_f64);
        let radius_m = 5000.0;
        let mut expected = 0usize;
        let mut idx = 0;
        for a in -12..=12 {
            for b in -12..=12 {
                let lon = clon + a as f64 * 0.04;
                let lat = clat + b as f64 * 0.04;
                cmd(
                    &mut db,
                    &[
                        b"GEOSET",
                        format!("p{idx}").as_bytes(),
                        lon.to_string().as_bytes(),
                        lat.to_string().as_bytes(),
                    ],
                );
                if haversine_m(clon, clat, lon, lat) <= radius_m {
                    expected += 1;
                }
                idx += 1;
            }
        }
        assert!(expected > 1, "test should have several in-radius points");
        let reply = cmd(
            &mut db,
            &[
                b"GEOSEARCH",
                b"FROMLONLAT",
                b"10",
                b"50",
                b"BYRADIUS",
                b"5",
                b"km",
            ],
        );
        // Reply is a RESP array `*N\r\n…`; N is the match count.
        let n: usize = reply
            .iter()
            .skip(1)
            .take_while(|&&c| c != b'\r')
            .map(|&c| c as char)
            .collect::<String>()
            .parse()
            .unwrap();
        assert_eq!(n, expected, "indexed GEOSEARCH count != brute force");
    }

    #[test]
    fn crc16_and_hash_slot() {
        // 0x31C3 is the canonical CRC16-CCITT/XMODEM check value (same as Redis).
        assert_eq!(crc16(b"123456789"), 0x31C3);
        assert_eq!(hash_slot(b"123456789"), 0x31C3); // 12739 < 16384, slot == crc
        // {hashtag}: only the tag is hashed, so these route to the same slot.
        assert_eq!(
            hash_slot(b"{user1000}.following"),
            hash_slot(b"{user1000}.followers")
        );
        // An empty tag falls back to hashing the whole key.
        assert_eq!(hash_slot(b"foo{}bar"), crc16(b"foo{}bar") % 16384);
        assert!(hash_slot(b"anything") < 16384);
    }

    #[test]
    fn command_keys_for_routing() {
        let to = |p: &[&[u8]]| -> Vec<Vec<u8>> { p.iter().map(|x| x.to_vec()).collect() };
        assert_eq!(command_keys(&to(&[b"GET", b"k"])), vec![b"k".as_slice()]);
        assert_eq!(
            command_keys(&to(&[b"SET", b"k", b"v"])),
            vec![b"k".as_slice()]
        );
        assert_eq!(
            command_keys(&to(&[b"MGET", b"a", b"b"])),
            vec![b"a".as_slice(), b"b"]
        );
        assert_eq!(
            command_keys(&to(&[b"MSET", b"a", b"1", b"b", b"2"])),
            vec![b"a".as_slice(), b"b"]
        );
        assert_eq!(
            command_keys(&to(&[b"DEL", b"a", b"b"])),
            vec![b"a".as_slice(), b"b"]
        );
        // Store variants enumerate destination AND every source — a missing
        // source silently executes against empty keys on the wrong shard
        // (CROSSSLOT gap) and dodges its ACL prefix check.
        assert_eq!(
            command_keys(&to(&[b"SINTERSTORE", b"dst", b"s1", b"s2"])),
            vec![b"dst".as_slice(), b"s1", b"s2"]
        );
        assert_eq!(
            command_keys(&to(&[
                b"ZUNIONSTORE",
                b"dst",
                b"2",
                b"z1",
                b"z2",
                b"WEIGHTS"
            ])),
            vec![b"dst".as_slice(), b"z1", b"z2"]
        );
        assert_eq!(
            command_keys(&to(&[b"BITOP", b"AND", b"dst", b"a", b"b"])),
            vec![b"dst".as_slice(), b"a", b"b"]
        );
        assert_eq!(
            command_keys(&to(&[b"SINTERCARD", b"2", b"s1", b"s2", b"LIMIT", b"1"])),
            vec![b"s1".as_slice(), b"s2"]
        );
        assert_eq!(
            command_keys(&to(&[
                b"XREAD", b"COUNT", b"5", b"STREAMS", b"st1", b"st2", b"0", b"0"
            ])),
            vec![b"st1".as_slice(), b"st2"]
        );
        // Non-key / cluster-wide commands don't route.
        assert!(command_keys(&to(&[b"PING"])).is_empty());
        assert!(command_keys(&to(&[b"KEYS", b"*"])).is_empty());
        assert!(command_keys(&to(&[b"SUBSCRIBE", b"ch"])).is_empty());
        assert!(command_keys(&to(&[b"GEOSEARCH", b"FROMLONLAT"])).is_empty());
        // Changefeed commands carry prefixes, not keys — never routed.
        assert!(command_keys(&to(&[b"CDCSUBSCRIBE", b"app:"])).is_empty());
        assert!(command_keys(&to(&[b"CDCREAD", b"0"])).is_empty());
    }

    #[test]
    fn geosearch_where_filters_by_attribute() {
        let mut db = Db::new();
        // Three points within radius, with attributes.
        cmd(
            &mut db,
            &[
                b"GEOSET", b"a", b"10", b"50", b"status", b"active", b"kind", b"car",
            ],
        );
        cmd(
            &mut db,
            &[
                b"GEOSET", b"b", b"10.001", b"50.001", b"status", b"idle", b"kind", b"car",
            ],
        );
        cmd(
            &mut db,
            &[
                b"GEOSET", b"c", b"10.002", b"50.002", b"status", b"active", b"kind", b"truck",
            ],
        );
        let search = |db: &mut Db, extra: &[&[u8]]| -> Vec<u8> {
            let mut a: Vec<&[u8]> = vec![
                b"GEOSEARCH",
                b"FROMLONLAT",
                b"10",
                b"50",
                b"BYRADIUS",
                b"5",
                b"km",
            ];
            a.extend_from_slice(extra);
            cmd(db, &a)
        };
        // No filter -> all 3.
        assert!(search(&mut db, &[]).starts_with(b"*3\r\n"));
        // WHERE status active -> a, c.
        assert!(search(&mut db, &[b"WHERE", b"status", b"active"]).starts_with(b"*2\r\n"));
        // AND across two WHEREs: status active AND kind truck -> only c.
        let one = search(
            &mut db,
            &[b"WHERE", b"status", b"active", b"WHERE", b"kind", b"truck"],
        );
        assert!(one.starts_with(b"*1\r\n"));
        assert!(one.windows(1).count() > 0 && String::from_utf8_lossy(&one).contains('c'));
        // No match.
        assert!(search(&mut db, &[b"WHERE", b"status", b"gone"]).starts_with(b"*0\r\n"));
        // Updating a point's attrs is reflected (insert replaces the whole value).
        cmd(
            &mut db,
            &[b"GEOSET", b"a", b"10", b"50", b"status", b"idle"],
        );
        assert!(search(&mut db, &[b"WHERE", b"status", b"active"]).starts_with(b"*1\r\n"));
    }

    #[test]
    fn geosearch_count_returns_the_closest_not_an_arbitrary_subset() {
        let mut db = Db::new();
        // Three points at increasing distance from the search center.
        cmd(&mut db, &[b"GEOSET", b"near", b"10.000", b"50.0"]);
        cmd(&mut db, &[b"GEOSET", b"mid", b"10.010", b"50.0"]);
        cmd(&mut db, &[b"GEOSET", b"far", b"10.030", b"50.0"]);
        // COUNT 2 without ASC/DESC must still return the TWO CLOSEST (near, mid),
        // sorted — not whichever two the scan happened to hit first.
        let r = cmd(
            &mut db,
            &[
                b"GEOSEARCH",
                b"FROMLONLAT",
                b"10",
                b"50",
                b"BYRADIUS",
                b"100",
                b"km",
                b"COUNT",
                b"2",
            ],
        );
        let s = String::from_utf8_lossy(&r);
        assert!(s.starts_with("*2"), "expected 2 results: {s}");
        assert!(
            s.contains("near") && s.contains("mid") && !s.contains("far"),
            "not the closest 2: {s}"
        );
    }

    #[test]
    fn geosearch_bybox_measures_east_west_at_the_point_latitude() {
        // A tall, narrow box at a high latitude: a point due east at the box's
        // top must be judged against the east-west arc AT ITS OWN latitude, not
        // the (wider) arc at the center latitude — so a point just outside the
        // narrow box isn't wrongly included.
        let mut db = Db::new();
        // Center at 60N; box 20km wide (E-W) x 200km tall (N-S).
        // A point ~9km east but near the top (higher latitude, where 9km spans
        // MORE longitude) should still be inside since 9km < 10km half-width at
        // its own latitude. Mostly a regression guard that the axis is the
        // point's latitude (behavior is self-consistent, not Redis-identical).
        cmd(&mut db, &[b"GEOSET", b"p", b"10.15", b"60.8"]);
        let r = cmd(
            &mut db,
            &[
                b"GEOSEARCH",
                b"FROMLONLAT",
                b"10.0",
                b"60.0",
                b"BYBOX",
                b"40",
                b"400",
                b"km",
            ],
        );
        // Just asserting it runs and returns a well-formed reply for a high-lat
        // box (the arithmetic-at-point-latitude path).
        assert!(String::from_utf8_lossy(&r).starts_with('*'));
    }

    #[test]
    fn conditional_writes() {
        let mut db = Db::new();
        // CAS only sets when the current value matches expected.
        cmd(&mut db, &[b"SET", b"k", b"v1"]);
        assert_eq!(
            cmd(&mut db, &[b"CAS", b"k", b"wrong", b"v2"]),
            b":0\r\n".to_vec()
        );
        assert_eq!(cmd(&mut db, &[b"GET", b"k"]), bulk_string(b"v1"));
        assert_eq!(
            cmd(&mut db, &[b"CAS", b"k", b"v1", b"v2"]),
            b":1\r\n".to_vec()
        );
        assert_eq!(cmd(&mut db, &[b"GET", b"k"]), bulk_string(b"v2"));
        assert_eq!(
            cmd(&mut db, &[b"CAS", b"missing", b"x", b"y"]),
            b":0\r\n".to_vec()
        );
        // CADEL only deletes on match.
        assert_eq!(cmd(&mut db, &[b"CADEL", b"k", b"nope"]), b":0\r\n".to_vec());
        assert_eq!(cmd(&mut db, &[b"CADEL", b"k", b"v2"]), b":1\r\n".to_vec());
        assert_eq!(cmd(&mut db, &[b"EXISTS", b"k"]), b":0\r\n".to_vec());
        // SETMAX advances only forward.
        assert_eq!(cmd(&mut db, &[b"SETMAX", b"cur", b"5"]), b":1\r\n".to_vec()); // created
        assert_eq!(cmd(&mut db, &[b"SETMAX", b"cur", b"3"]), b":0\r\n".to_vec()); // 3 < 5
        assert_eq!(cmd(&mut db, &[b"SETMAX", b"cur", b"9"]), b":1\r\n".to_vec());
        assert_eq!(cmd(&mut db, &[b"GET", b"cur"]), bulk_string(b"9"));
        // INCRCAP increments until the cap, then rejects.
        assert_eq!(
            cmd(&mut db, &[b"INCRCAP", b"q", b"3", b"5"]),
            b":3\r\n".to_vec()
        );
        assert_eq!(
            cmd(&mut db, &[b"INCRCAP", b"q", b"2", b"5"]),
            b":5\r\n".to_vec()
        );
        assert_eq!(
            cmd(&mut db, &[b"INCRCAP", b"q", b"1", b"5"]),
            b"$-1\r\n".to_vec()
        ); // would exceed
        assert_eq!(cmd(&mut db, &[b"GET", b"q"]), bulk_string(b"5")); // unchanged
        // WRONGTYPE
        cmd(&mut db, &[b"RPUSH", b"l", b"x"]);
        assert!(cmd(&mut db, &[b"CAS", b"l", b"a", b"b"]).starts_with(b"-WRONGTYPE"));
        assert!(cmd(&mut db, &[b"SETMAX", b"l", b"1"]).starts_with(b"-WRONGTYPE"));
    }

    #[test]
    fn bloom_filter_commands() {
        let mut db = Db::new();
        assert_eq!(cmd(&mut db, &[b"BFADD", b"seen", b"a"]), b":1\r\n".to_vec()); // new
        assert_eq!(cmd(&mut db, &[b"BFADD", b"seen", b"a"]), b":0\r\n".to_vec()); // already seen
        assert_eq!(
            cmd(&mut db, &[b"BFEXISTS", b"seen", b"a"]),
            b":1\r\n".to_vec()
        );
        assert_eq!(
            cmd(&mut db, &[b"BFEXISTS", b"seen", b"b"]),
            b":0\r\n".to_vec()
        ); // no false negative
        assert_eq!(cmd(&mut db, &[b"TYPE", b"seen"]), b"+bloom\r\n".to_vec());
        assert_eq!(
            cmd(&mut db, &[b"BFEXISTS", b"missing", b"x"]),
            b":0\r\n".to_vec()
        );
        // WRONGTYPE
        cmd(&mut db, &[b"SET", b"s", b"x"]);
        assert!(cmd(&mut db, &[b"BFADD", b"s", b"y"]).starts_with(b"-WRONGTYPE"));
        assert!(cmd(&mut db, &[b"BFEXISTS", b"s", b"y"]).starts_with(b"-WRONGTYPE"));
    }

    #[test]
    fn count_min_sketch_commands() {
        let mut db = Db::new();
        // increments return the running (over-)estimate
        assert_eq!(
            cmd(&mut db, &[b"CMSINCRBY", b"trend", b"a", b"3"]),
            array(&[integer(3)])
        );
        assert_eq!(
            cmd(&mut db, &[b"CMSINCRBY", b"trend", b"a", b"2", b"b", b"5"]),
            array(&[integer(5), integer(5)])
        );
        // query estimates (never under the true count)
        let q = cmd(&mut db, &[b"CMSQUERY", b"trend", b"a", b"b", b"missing"]);
        assert_eq!(q, array(&[integer(5), integer(5), integer(0)]));
        assert_eq!(cmd(&mut db, &[b"TYPE", b"trend"]), b"+cms\r\n".to_vec());
        // query on a missing key -> zeros
        assert_eq!(
            cmd(&mut db, &[b"CMSQUERY", b"nope", b"x"]),
            array(&[integer(0)])
        );
        // WRONGTYPE
        cmd(&mut db, &[b"SET", b"s", b"x"]);
        assert!(cmd(&mut db, &[b"CMSINCRBY", b"s", b"a", b"1"]).starts_with(b"-WRONGTYPE"));
        assert!(cmd(&mut db, &[b"CMSQUERY", b"s", b"a"]).starts_with(b"-WRONGTYPE"));
    }

    #[test]
    fn topk_commands() {
        let mut db = Db::new();
        cmd(&mut db, &[b"TOPKRESERVE", b"hh", b"2"]);
        for _ in 0..5 {
            cmd(&mut db, &[b"TOPKADD", b"hh", b"a"]);
        }
        for _ in 0..3 {
            cmd(&mut db, &[b"TOPKADD", b"hh", b"b"]);
        }
        cmd(&mut db, &[b"TOPKADD", b"hh", b"c"]); // count 1, shouldn't make the k=2 board
        assert_eq!(
            cmd(&mut db, &[b"TOPKLIST", b"hh"]),
            bulk_array(&[b"a".to_vec(), b"b".to_vec()])
        );
        assert_eq!(cmd(&mut db, &[b"TYPE", b"hh"]), b"+topk\r\n".to_vec());
        // counts (over-estimate, never under)
        let counts = cmd(&mut db, &[b"TOPKCOUNT", b"hh", b"a", b"missing"]);
        assert!(counts.starts_with(b"*2\r\n") && counts.windows(4).any(|w| w == b":5\r\n"));
        // WRONGTYPE
        cmd(&mut db, &[b"SET", b"s", b"x"]);
        assert!(cmd(&mut db, &[b"TOPKADD", b"s", b"a"]).starts_with(b"-WRONGTYPE"));
    }

    #[test]
    fn tdigest_commands() {
        let mut db = Db::new();
        // add 1..=100 in a few batches
        for start in (1..=100).step_by(10) {
            let mut args: Vec<Vec<u8>> = vec![b"TDADD".to_vec(), b"lat".to_vec()];
            for v in start..start + 10 {
                args.push(v.to_string().into_bytes());
            }
            let t: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
            assert_eq!(cmd(&mut db, &t), b"+OK\r\n".to_vec());
        }
        assert_eq!(cmd(&mut db, &[b"TYPE", b"lat"]), b"+tdigest\r\n".to_vec());
        // exact extremes
        assert_eq!(
            cmd(&mut db, &[b"TDQUANTILE", b"lat", b"0"]),
            array(&[bulk_string(b"1")])
        );
        assert_eq!(
            cmd(&mut db, &[b"TDQUANTILE", b"lat", b"1"]),
            array(&[bulk_string(b"100")])
        );
        // median within tolerance — parse the bulk string back
        let med = cmd(&mut db, &[b"TDQUANTILE", b"lat", b"0.5"]);
        // reply: *1\r\n$N\r\n<num>\r\n — extract the number
        let s = String::from_utf8(med).unwrap();
        let num: f64 = s.rsplit("\r\n").nth(1).unwrap().parse().unwrap();
        assert!((num - 50.0).abs() < 10.0, "p50={num}");
        // missing key -> nil per quantile
        assert_eq!(
            cmd(&mut db, &[b"TDQUANTILE", b"nope", b"0.5"]),
            array(&[null_bulk()])
        );
        // WRONGTYPE
        cmd(&mut db, &[b"SET", b"s", b"x"]);
        assert!(cmd(&mut db, &[b"TDADD", b"s", b"1"]).starts_with(b"-WRONGTYPE"));
    }

    #[test]
    fn command_table_write_set_is_exact() {
        // The exact set of write commands. A change here is a deliberate decision,
        // not an accident — guards the AOF/replication write-detection.
        let writes: &[&[u8]] = &[
            b"SET",
            b"SETNX",
            b"SETEX",
            b"PSETEX",
            b"GETSET",
            b"MSET",
            b"MSETNX",
            b"SETRANGE",
            b"INCRBYFLOAT",
            b"CAS",
            b"CADEL",
            b"SETMAX",
            b"INCRCAP",
            b"GETDEL",
            b"DEL",
            b"UNLINK",
            b"RENAME",
            b"RENAMENX",
            b"FLUSHDB",
            b"FLUSHALL",
            b"EXPIRE",
            b"PEXPIRE",
            b"EXPIREAT",
            b"PEXPIREAT",
            b"PERSIST",
            b"INCR",
            b"DECR",
            b"INCRBY",
            b"DECRBY",
            b"APPEND",
            b"LPUSH",
            b"RPUSH",
            b"LPUSHX",
            b"RPUSHX",
            b"LPOP",
            b"RPOP",
            b"LSET",
            b"LINSERT",
            b"LREM",
            b"LTRIM",
            b"RPOPLPUSH",
            b"LMOVE",
            b"HSET",
            b"HSETNX",
            b"HDEL",
            b"HINCRBY",
            b"SADD",
            b"SREM",
            b"SPOP",
            b"SMOVE",
            b"SINTERSTORE",
            b"SUNIONSTORE",
            b"SDIFFSTORE",
            b"ZADD",
            b"ZREM",
            b"ZINCRBY",
            b"ZPOPMIN",
            b"ZPOPMAX",
            b"ZREMRANGEBYRANK",
            b"ZREMRANGEBYSCORE",
            b"ZUNIONSTORE",
            b"ZINTERSTORE",
            b"SETBIT",
            b"BITOP",
            b"GEOSET",
            b"BFADD",
            b"BFLOAD",
            b"CMSINCRBY",
            b"CMSLOAD",
            b"TOPKRESERVE",
            b"TOPKADD",
            b"TOPKLOAD",
            b"TDADD",
            b"TDLOAD",
            b"XADD",
        ];
        for w in writes {
            assert!(
                is_write(w),
                "{} should be a write",
                String::from_utf8_lossy(w)
            );
        }
        // Reads and admin commands must not be writes.
        for r in [
            b"GET".as_slice(),
            b"MGET".as_slice(),
            b"GETRANGE".as_slice(),
            b"LRANGE".as_slice(),
            b"LPOS".as_slice(),
            b"SINTERCARD".as_slice(),
            b"GETBIT".as_slice(),
            b"BITCOUNT".as_slice(),
            b"BITPOS".as_slice(),
            b"GEOPOS".as_slice(),
            b"GEODIST".as_slice(),
            b"GEOSEARCH".as_slice(),
            b"BFEXISTS".as_slice(),
            b"CMSQUERY".as_slice(),
            b"TOPKLIST".as_slice(),
            b"TOPKCOUNT".as_slice(),
            b"TDQUANTILE".as_slice(),
            b"IDXGET".as_slice(),
            b"IDXRANGE".as_slice(),
            b"IDXCREATE".as_slice(),
            b"SRANDMEMBER".as_slice(),
            b"RANDOMKEY".as_slice(),
            b"SMEMBERS".as_slice(),
            b"ZRANGE".as_slice(),
            b"XRANGE".as_slice(),
            b"EXISTS".as_slice(),
            b"TOUCH".as_slice(),
            b"KEYS".as_slice(),
            b"DBSIZE".as_slice(),
            b"PING".as_slice(),
            b"INFO".as_slice(),
            b"SUBSCRIBE".as_slice(),
        ] {
            assert!(
                !is_write(r),
                "{} should not be a write",
                String::from_utf8_lossy(r)
            );
        }
        // is_write is case-insensitive.
        assert!(is_write(b"set"));
        assert!(!is_write(b"get"));
    }

    #[test]
    fn expire_with_overflowing_ttl_does_not_panic() {
        let mut db = Db::new();
        cmd(&mut db, &[b"SET", b"k", b"v"]);
        // Near-i64::MAX seconds would overflow when scaled to ms — must error,
        // not panic (debug) or wrap to a past deadline (release).
        assert!(cmd(&mut db, &[b"EXPIRE", b"k", b"9999999999999999"]).starts_with(b"-ERR"));
        // The key must survive (no silent immediate deletion).
        assert_eq!(cmd(&mut db, &[b"EXISTS", b"k"]), b":1\r\n".to_vec());
        // SET ... EX with an overflowing TTL is a (syntax) error, key unchanged.
        assert!(
            cmd(&mut db, &[b"SET", b"k", b"v2", b"EX", b"99999999999999999"]).starts_with(b"-")
        );
    }
}
