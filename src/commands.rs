//! Command dispatch and implementations.
//!
//! Organized by type: generic/expiry, strings, lists, hashes, sets. Every
//! command that targets a typed key returns WRONGTYPE if the key holds a
//! different type.

use std::collections::{HashMap, HashSet, VecDeque};

use crate::db::{now_ms, Db, Value};
use crate::rdb;
use crate::resp::{
    array, bulk_array, bulk_string, error, integer, null_array, null_bulk, simple_string,
};
use crate::streams;

// --- shared helpers ---------------------------------------------------------

fn wrong_args(cmd: &str) -> Vec<u8> {
    error(&format!("ERR wrong number of arguments for '{cmd}' command"))
}
fn not_integer() -> Vec<u8> {
    error("ERR value is not an integer or out of range")
}
fn wrongtype() -> Vec<u8> {
    error("WRONGTYPE Operation against a key holding the wrong kind of value")
}
fn parse_int(arg: &[u8]) -> Option<i64> {
    std::str::from_utf8(arg).ok().and_then(|s| s.parse::<i64>().ok())
}

pub fn execute(tokens: &[Vec<u8>], db: &mut Db) -> Vec<u8> {
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
        b"DEL" => del_cmd(db, tokens),
        b"EXISTS" => exists_cmd(db, tokens),
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
        b"GETDEL" => getdel_cmd(db, tokens),
        b"INCR" => incr_cmd(db, tokens, 1, false),
        b"DECR" => incr_cmd(db, tokens, -1, false),
        b"INCRBY" => incr_cmd(db, tokens, 0, true),
        b"DECRBY" => incr_cmd(db, tokens, 0, true),
        b"APPEND" => append_cmd(db, tokens),
        b"STRLEN" => strlen_cmd(db, tokens),
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
        // hashes
        b"HSET" => hset_cmd(db, tokens, false),
        b"HSETNX" => hsetnx_cmd(db, tokens),
        b"HGET" => hget_cmd(db, tokens),
        b"HMGET" => hmget_cmd(db, tokens),
        b"HGETALL" => hgetall_cmd(db, tokens),
        b"HDEL" => hdel_cmd(db, tokens),
        b"HEXISTS" => hexists_cmd(db, tokens),
        b"HLEN" => hlen_cmd(db, tokens),
        b"HKEYS" => hkeys_vals_cmd(db, tokens, true),
        b"HVALS" => hkeys_vals_cmd(db, tokens, false),
        b"HINCRBY" => hincrby_cmd(db, tokens),
        // sets
        b"SADD" => sadd_cmd(db, tokens),
        b"SREM" => srem_cmd(db, tokens),
        b"SMEMBERS" => smembers_cmd(db, tokens),
        b"SISMEMBER" => sismember_cmd(db, tokens),
        b"SMISMEMBER" => smismember_cmd(db, tokens),
        b"SCARD" => scard_cmd(db, tokens),
        b"SPOP" => spop_cmd(db, tokens),
        b"SINTER" => setop_cmd(db, tokens, SetOp::Inter),
        b"SUNION" => setop_cmd(db, tokens, SetOp::Union),
        b"SDIFF" => setop_cmd(db, tokens, SetOp::Diff),
        // sorted sets
        b"ZADD" => zadd_cmd(db, tokens),
        b"ZSCORE" => zscore_cmd(db, tokens),
        b"ZMSCORE" => zmscore_cmd(db, tokens),
        b"ZCARD" => zcard_cmd(db, tokens),
        b"ZREM" => zrem_cmd(db, tokens),
        b"ZINCRBY" => zincrby_cmd(db, tokens),
        b"ZRANK" => zrank_cmd(db, tokens, false),
        b"ZREVRANK" => zrank_cmd(db, tokens, true),
        b"ZRANGE" => zrange_cmd(db, tokens),
        b"ZREVRANGE" => zrevrange_cmd(db, tokens),
        b"ZRANGEBYSCORE" => zrangebyscore_cmd(db, tokens, false),
        b"ZREVRANGEBYSCORE" => zrangebyscore_cmd(db, tokens, true),
        b"ZCOUNT" => zcount_cmd(db, tokens),
        b"ZPOPMIN" => zpop_cmd(db, tokens, false),
        b"ZPOPMAX" => zpop_cmd(db, tokens, true),
        // streams (XREAD is handled in the hub for blocking support)
        b"XADD" => streams::xadd(db, tokens),
        b"XLEN" => streams::xlen(db, tokens),
        b"XRANGE" => streams::xrange(db, tokens, false),
        b"XREVRANGE" => streams::xrange(db, tokens, true),
        // persistence
        b"SAVE" => match rdb::save(db, &rdb::configured_path()) {
            Ok(()) => simple_string("OK"),
            Err(e) => error(&format!("ERR {e}")),
        },
        b"BGSAVE" => match rdb::save(db, &rdb::configured_path()) {
            Ok(()) => simple_string("Background saving started"),
            Err(e) => error(&format!("ERR {e}")),
        },
        // stubs
        b"COMMAND" => b"*0\r\n".to_vec(),
        b"CONFIG" => match tokens.get(1).map(|t| t.to_ascii_uppercase()).as_deref() {
            Some(b"GET") => b"*0\r\n".to_vec(),
            _ => simple_string("OK"),
        },
        other => error(&format!(
            "ERR unknown command '{}'",
            String::from_utf8_lossy(other)
        )),
    }
}

// === generic ================================================================

fn del_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() < 2 {
        return wrong_args("del");
    }
    let n = tokens[1..].iter().filter(|k| db.remove(k).is_some()).count();
    integer(n as i64)
}

fn exists_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() < 2 {
        return wrong_args("exists");
    }
    let n = tokens[1..].iter().filter(|k| db.contains(k)).count();
    integer(n as i64)
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
        Some(Value::Str(v)) => match std::str::from_utf8(v).ok().and_then(|s| s.parse::<i64>().ok())
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
                o.expire_at = Some(match a.as_slice() {
                    b"EX" => now + n * 1000,
                    b"PX" => now + n,
                    b"EXAT" => n * 1000,
                    _ => n,
                });
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
    let at = if absolute {
        n * unit_ms
    } else {
        now_ms() as i64 + n * unit_ms
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
            integer(((remaining + unit_ms - 1) / unit_ms) as i64)
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
        None => return if count.is_some() { null_array() } else { null_bulk() },
    }
    db.remove_if_empty(key);
    if count.is_some() {
        bulk_array(&popped)
    } else {
        popped.into_iter().next().map(|v| bulk_string(&v)).unwrap_or_else(null_bulk)
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
    if i < 0 {
        i + len as i64
    } else {
        i
    }
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

// === hashes =================================================================

fn with_hash<'a>(db: &'a mut Db, key: &[u8]) -> Result<Option<&'a HashMap<Vec<u8>, Vec<u8>>>, ()> {
    match db.get(key) {
        None => Ok(None),
        Some(Value::Hash(h)) => Ok(Some(h)),
        Some(_) => Err(()),
    }
}

fn hset_cmd(db: &mut Db, tokens: &[Vec<u8>], _nx: bool) -> Vec<u8> {
    if tokens.len() < 4 || (tokens.len() - 2) % 2 != 0 {
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
        Ok(Some(h)) => h.get(&tokens[2]).map(|v| bulk_string(v)).unwrap_or_else(null_bulk),
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

fn hgetall_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 2 {
        return wrong_args("hgetall");
    }
    match with_hash(db, &tokens[1]) {
        Err(()) => wrongtype(),
        Ok(None) => bulk_array(&[]),
        Ok(Some(h)) => {
            let mut flat = Vec::with_capacity(h.len() * 2);
            for (f, v) in h {
                flat.push(f.clone());
                flat.push(v.clone());
            }
            bulk_array(&flat)
        }
    }
}

fn hdel_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() < 3 {
        return wrong_args("hdel");
    }
    let removed = match db.get_mut(&tokens[1]) {
        None => 0,
        Some(Value::Hash(h)) => tokens[2..].iter().filter(|f| h.remove(*f).is_some()).count() as i64,
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
        Some(v) => match std::str::from_utf8(v).ok().and_then(|s| s.parse::<i64>().ok()) {
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
    let added = tokens[2..].iter().filter(|m| s.insert((*m).clone())).count();
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

fn smembers_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 2 {
        return wrong_args("smembers");
    }
    match db.get(&tokens[1]) {
        None => bulk_array(&[]),
        Some(Value::Set(s)) => bulk_array(&s.iter().cloned().collect::<Vec<_>>()),
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
        .map(|m| integer(set.map_or(false, |s| s.contains(m)) as i64))
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
        None => return if count.is_some() { bulk_array(&[]) } else { null_bulk() },
        Some(Value::Set(s)) => {
            // Not cryptographically random — we take arbitrary members. (A true
            // random pick is a later refinement.)
            let take: Vec<Vec<u8>> = s.iter().take(count.unwrap_or(1)).cloned().collect();
            for m in take {
                s.remove(&m);
                popped.push(m);
            }
        }
        Some(_) => return wrongtype(),
    }
    db.remove_if_empty(&tokens[1]);
    if count.is_some() {
        bulk_array(&popped)
    } else {
        popped.into_iter().next().map(|v| bulk_string(&v)).unwrap_or_else(null_bulk)
    }
}

enum SetOp {
    Inter,
    Union,
    Diff,
}

fn setop_cmd(db: &mut Db, tokens: &[Vec<u8>], op: SetOp) -> Vec<u8> {
    if tokens.len() < 2 {
        return wrong_args("setop");
    }
    // Collect each key's set (missing key = empty set; wrong type = error).
    let mut sets: Vec<HashSet<Vec<u8>>> = Vec::new();
    for key in &tokens[1..] {
        match db.get(key) {
            None => sets.push(HashSet::new()),
            Some(Value::Set(s)) => sets.push(s.clone()),
            Some(_) => return wrongtype(),
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
    bulk_array(&acc.into_iter().collect::<Vec<_>>())
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

/// Members sorted by (score, then member bytes), ascending.
fn sorted_members(z: &HashMap<Vec<u8>, f64>) -> Vec<(Vec<u8>, f64)> {
    let mut v: Vec<(Vec<u8>, f64)> = z.iter().map(|(m, s)| (m.clone(), *s)).collect();
    v.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
    v
}

fn get_zset<'a>(db: &'a mut Db, key: &[u8]) -> Result<Option<&'a HashMap<Vec<u8>, f64>>, ()> {
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
    let mut i = 2;
    while i < tokens.len() {
        match tokens[i].to_ascii_uppercase().as_slice() {
            b"NX" => nx = true,
            b"XX" => xx = true,
            b"CH" => ch = true,
            b"INCR" => incr = true,
            b"GT" | b"LT" => {} // accepted, treated as no-op for now
            _ => break,
        }
        i += 1;
    }
    let pairs = &tokens[i..];
    if pairs.is_empty() || pairs.len() % 2 != 0 {
        return error("ERR syntax error");
    }
    if nx && xx {
        return error("ERR XX and NX options at the same time are not compatible");
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
    let z = match db.get_or_insert_with(key, || Value::ZSet(HashMap::new())) {
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
            Some(old) if old != score => {
                z.insert(member.clone(), score);
                changed += 1;
            }
            _ => {}
        }
    }
    db.remove_if_empty(key);
    integer(if ch { changed } else { added })
}

fn zscore_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 3 {
        return wrong_args("zscore");
    }
    match get_zset(db, &tokens[1]) {
        Err(()) => wrongtype(),
        Ok(None) => null_bulk(),
        Ok(Some(z)) => z
            .get(&tokens[2])
            .map(|s| bulk_string(&fmt_score(*s)))
            .unwrap_or_else(null_bulk),
    }
}

fn zmscore_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() < 3 {
        return wrong_args("zmscore");
    }
    match get_zset(db, &tokens[1]) {
        Err(()) => wrongtype(),
        Ok(opt) => {
            let elems: Vec<Vec<u8>> = tokens[2..]
                .iter()
                .map(|m| match opt.and_then(|z| z.get(m)) {
                    Some(s) => bulk_string(&fmt_score(*s)),
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
        Some(Value::ZSet(z)) => tokens[2..].iter().filter(|m| z.remove(*m).is_some()).count() as i64,
        Some(_) => return wrongtype(),
    };
    db.remove_if_empty(&tokens[1]);
    integer(removed)
}

fn zincrby_cmd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 4 {
        return wrong_args("zincrby");
    }
    let incr = match parse_score(&tokens[2]) {
        Some(s) => s,
        None => return error("ERR value is not a valid float"),
    };
    let z = match db.get_or_insert_with(&tokens[1], || Value::ZSet(HashMap::new())) {
        Value::ZSet(z) => z,
        _ => return wrongtype(),
    };
    let newv = z.get(&tokens[3]).copied().unwrap_or(0.0) + incr;
    z.insert(tokens[3].clone(), newv);
    bulk_string(&fmt_score(newv))
}

fn zrank_cmd(db: &mut Db, tokens: &[Vec<u8>], rev: bool) -> Vec<u8> {
    if tokens.len() != 3 {
        return wrong_args("zrank");
    }
    match get_zset(db, &tokens[1]) {
        Err(()) => wrongtype(),
        Ok(None) => null_bulk(),
        Ok(Some(z)) => {
            let mut sorted = sorted_members(z);
            if rev {
                sorted.reverse();
            }
            match sorted.iter().position(|(m, _)| m == &tokens[2]) {
                Some(i) => integer(i as i64),
                None => null_bulk(),
            }
        }
    }
}

fn zrange_index(z: &HashMap<Vec<u8>, f64>, start: i64, stop: i64, withscores: bool, rev: bool) -> Vec<u8> {
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
            let n = z.values().filter(|s| in_range(**s, lo, lo_ex, hi, hi_ex)).count();
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

#[cfg(test)]
mod tests {
    use super::*;

    fn cmd(db: &mut Db, parts: &[&[u8]]) -> Vec<u8> {
        let tokens: Vec<Vec<u8>> = parts.iter().map(|p| p.to_vec()).collect();
        execute(&tokens, db)
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
        assert_eq!(cmd(&mut db, &[b"RPUSH", b"l", b"a", b"b", b"c"]), b":3\r\n".to_vec());
        assert_eq!(cmd(&mut db, &[b"LPUSH", b"l", b"z"]), b":4\r\n".to_vec());
        assert_eq!(cmd(&mut db, &[b"LLEN", b"l"]), b":4\r\n".to_vec());
        assert_eq!(cmd(&mut db, &[b"LINDEX", b"l", b"0"]), b"$1\r\nz\r\n".to_vec());
        assert_eq!(cmd(&mut db, &[b"LINDEX", b"l", b"-1"]), b"$1\r\nc\r\n".to_vec());
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
        assert_eq!(cmd(&mut db, &[b"HSET", b"h", b"f1", b"v1", b"f2", b"v2"]), b":2\r\n".to_vec());
        assert_eq!(cmd(&mut db, &[b"HGET", b"h", b"f1"]), b"$2\r\nv1\r\n".to_vec());
        assert_eq!(cmd(&mut db, &[b"HLEN", b"h"]), b":2\r\n".to_vec());
        assert_eq!(cmd(&mut db, &[b"HEXISTS", b"h", b"f2"]), b":1\r\n".to_vec());
        assert_eq!(cmd(&mut db, &[b"HINCRBY", b"h", b"n", b"5"]), b":5\r\n".to_vec());
        assert_eq!(cmd(&mut db, &[b"HDEL", b"h", b"f1", b"f2", b"n"]), b":3\r\n".to_vec());
        assert_eq!(cmd(&mut db, &[b"EXISTS", b"h"]), b":0\r\n".to_vec()); // emptied -> gone
    }

    #[test]
    fn sets() {
        let mut db = Db::new();
        assert_eq!(cmd(&mut db, &[b"SADD", b"s", b"a", b"b", b"c"]), b":3\r\n".to_vec());
        assert_eq!(cmd(&mut db, &[b"SADD", b"s", b"a"]), b":0\r\n".to_vec());
        assert_eq!(cmd(&mut db, &[b"SCARD", b"s"]), b":3\r\n".to_vec());
        assert_eq!(cmd(&mut db, &[b"SISMEMBER", b"s", b"b"]), b":1\r\n".to_vec());
        assert_eq!(cmd(&mut db, &[b"SISMEMBER", b"s", b"z"]), b":0\r\n".to_vec());
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
            cmd(&mut db, &[b"ZADD", b"z", b"1", b"a", b"3", b"c", b"2", b"b"]),
            b":3\r\n".to_vec()
        );
        assert_eq!(cmd(&mut db, &[b"ZCARD", b"z"]), b":3\r\n".to_vec());
        assert_eq!(cmd(&mut db, &[b"ZSCORE", b"z", b"b"]), b"$1\r\n2\r\n".to_vec());
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
        assert_eq!(cmd(&mut db, &[b"ZINCRBY", b"z", b"5", b"a"]), b"$1\r\n6\r\n".to_vec());
        assert_eq!(cmd(&mut db, &[b"ZRANK", b"z", b"a"]), b":2\r\n".to_vec()); // now highest
        assert_eq!(cmd(&mut db, &[b"ZCOUNT", b"z", b"2", b"6"]), b":3\r\n".to_vec());
        // ZPOPMIN removes the lowest
        assert_eq!(
            cmd(&mut db, &[b"ZPOPMIN", b"z"]),
            bulk_array(&[b"b".to_vec(), b"2".to_vec()])
        );
    }
}
