//! Command dispatch and string/expiry command implementations.

use crate::db::{now_ms, Db};
use crate::resp::{bulk_string, error, integer, null_bulk, simple_string};

fn wrong_args(cmd: &str) -> Vec<u8> {
    error(&format!("ERR wrong number of arguments for '{cmd}' command"))
}

fn not_integer() -> Vec<u8> {
    error("ERR value is not an integer or out of range")
}

fn parse_int(arg: &[u8]) -> Option<i64> {
    std::str::from_utf8(arg).ok().and_then(|s| s.parse::<i64>().ok())
}

/// Run one parsed command against the keyspace and return its RESP reply.
/// Runs only on the single owner thread, so it sees no concurrency.
pub fn execute(tokens: &[Vec<u8>], db: &mut Db) -> Vec<u8> {
    if tokens.is_empty() {
        return Vec::new();
    }
    let cmd = tokens[0].to_ascii_uppercase();
    match cmd.as_slice() {
        b"PING" => match tokens.len() {
            1 => simple_string("PONG"),
            2 => bulk_string(&tokens[1]),
            _ => wrong_args("ping"),
        },
        b"ECHO" => match tokens.len() {
            2 => bulk_string(&tokens[1]),
            _ => wrong_args("echo"),
        },
        b"SET" => set_cmd(db, tokens),
        b"GET" => match tokens.len() {
            2 => match db.get(&tokens[1]) {
                Some(v) => bulk_string(v),
                None => null_bulk(),
            },
            _ => wrong_args("get"),
        },
        b"GETDEL" => match tokens.len() {
            2 => match db.remove(&tokens[1]) {
                Some(v) => bulk_string(&v),
                None => null_bulk(),
            },
            _ => wrong_args("getdel"),
        },
        b"DEL" => {
            if tokens.len() < 2 {
                wrong_args("del")
            } else {
                let n = tokens[1..].iter().filter(|k| db.remove(k).is_some()).count();
                integer(n as i64)
            }
        }
        b"EXISTS" => {
            if tokens.len() < 2 {
                wrong_args("exists")
            } else {
                let n = tokens[1..].iter().filter(|k| db.contains(k)).count();
                integer(n as i64)
            }
        }
        b"INCR" => match tokens.len() {
            2 => incr_by(db, &tokens[1], 1),
            _ => wrong_args("incr"),
        },
        b"DECR" => match tokens.len() {
            2 => incr_by(db, &tokens[1], -1),
            _ => wrong_args("decr"),
        },
        b"INCRBY" => match tokens.len() {
            3 => match parse_int(&tokens[2]) {
                Some(d) => incr_by(db, &tokens[1], d),
                None => not_integer(),
            },
            _ => wrong_args("incrby"),
        },
        b"DECRBY" => match tokens.len() {
            3 => match parse_int(&tokens[2]).and_then(|d| d.checked_neg()) {
                Some(neg) => incr_by(db, &tokens[1], neg),
                None => not_integer(),
            },
            _ => wrong_args("decrby"),
        },
        b"APPEND" => match tokens.len() {
            3 => {
                let e = db.entry_or_default(&tokens[1]);
                e.extend_from_slice(&tokens[2]);
                integer(e.len() as i64)
            }
            _ => wrong_args("append"),
        },
        b"STRLEN" => match tokens.len() {
            2 => integer(db.get(&tokens[1]).map(|v| v.len()).unwrap_or(0) as i64),
            _ => wrong_args("strlen"),
        },
        b"TYPE" => match tokens.len() {
            2 => {
                if db.contains(&tokens[1]) {
                    simple_string("string")
                } else {
                    simple_string("none")
                }
            }
            _ => wrong_args("type"),
        },
        // --- expiry family ---
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
        // --- stubs so interactive redis-cli connects cleanly ---
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

fn incr_by(db: &mut Db, key: &[u8], delta: i64) -> Vec<u8> {
    let current = match db.get(key) {
        None => 0,
        Some(v) => match std::str::from_utf8(v).ok().and_then(|s| s.parse::<i64>().ok()) {
            Some(n) => n,
            None => return not_integer(),
        },
    };
    match current.checked_add(delta) {
        Some(next) => {
            // INCR/DECR preserve any existing TTL — db.set doesn't touch expires.
            db.set(key.to_vec(), next.to_string().into_bytes());
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
                    _ => n, // PXAT
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
    let exists = db.contains(key);
    if (opts.nx && exists) || (opts.xx && !exists) {
        // Condition not met: GET returns the old value, otherwise nil.
        return if opts.get {
            db.get(key).map(|v| bulk_string(v)).unwrap_or_else(null_bulk)
        } else {
            null_bulk()
        };
    }
    let old = if opts.get { db.get(key).cloned() } else { None };
    db.set(key.clone(), val.clone());
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

/// EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT. `unit_ms` scales the argument; `absolute`
/// chooses between "from now" and "at this timestamp".
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
        db.remove(key); // deadline already passed — delete immediately
    } else {
        db.set_expire(key, at as u64);
    }
    integer(1)
}

/// TTL (seconds) / PTTL (ms): -2 no key, -1 no expiry, else remaining time.
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
            // Round up so a key with <1 unit left still reports 1, not 0.
            integer(((remaining + unit_ms - 1) / unit_ms) as i64)
        }
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
    fn set_get_and_nil() {
        let mut db = Db::new();
        assert_eq!(cmd(&mut db, &[b"SET", b"k", b"v"]), b"+OK\r\n".to_vec());
        assert_eq!(cmd(&mut db, &[b"GET", b"k"]), b"$1\r\nv\r\n".to_vec());
        assert_eq!(cmd(&mut db, &[b"GET", b"x"]), b"$-1\r\n".to_vec());
    }

    #[test]
    fn incr_decr_and_non_integer() {
        let mut db = Db::new();
        assert_eq!(cmd(&mut db, &[b"INCR", b"c"]), b":1\r\n".to_vec());
        assert_eq!(cmd(&mut db, &[b"INCRBY", b"c", b"9"]), b":10\r\n".to_vec());
        assert_eq!(cmd(&mut db, &[b"DECR", b"c"]), b":9\r\n".to_vec());
        cmd(&mut db, &[b"SET", b"s", b"abc"]);
        assert!(cmd(&mut db, &[b"INCR", b"s"]).starts_with(b"-ERR"));
    }

    #[test]
    fn passive_expiry_hides_past_deadline() {
        let mut db = Db::new();
        // PXAT 1 = expire at unix-ms 1 (far in the past) -> gone on next access.
        cmd(&mut db, &[b"SET", b"k", b"v", b"PXAT", b"1"]);
        assert_eq!(cmd(&mut db, &[b"GET", b"k"]), b"$-1\r\n".to_vec());
        assert_eq!(cmd(&mut db, &[b"EXISTS", b"k"]), b":0\r\n".to_vec());
    }

    #[test]
    fn expire_ttl_persist() {
        let mut db = Db::new();
        cmd(&mut db, &[b"SET", b"k", b"v"]);
        assert_eq!(cmd(&mut db, &[b"TTL", b"k"]), b":-1\r\n".to_vec()); // no expiry
        assert_eq!(cmd(&mut db, &[b"EXPIRE", b"k", b"100"]), b":1\r\n".to_vec());
        // TTL should be (0, 100]
        let ttl = cmd(&mut db, &[b"TTL", b"k"]);
        let s = String::from_utf8_lossy(&ttl);
        let n: i64 = s.trim_start_matches(':').trim_end().parse().unwrap();
        assert!(n > 0 && n <= 100, "ttl was {n}");
        assert_eq!(cmd(&mut db, &[b"PERSIST", b"k"]), b":1\r\n".to_vec());
        assert_eq!(cmd(&mut db, &[b"TTL", b"k"]), b":-1\r\n".to_vec());
        assert_eq!(cmd(&mut db, &[b"TTL", b"missing"]), b":-2\r\n".to_vec());
    }

    #[test]
    fn set_keepttl_and_overwrite_clears_ttl() {
        let mut db = Db::new();
        cmd(&mut db, &[b"SET", b"k", b"v", b"EX", b"100"]);
        // plain SET clears TTL
        cmd(&mut db, &[b"SET", b"k", b"v2"]);
        assert_eq!(cmd(&mut db, &[b"TTL", b"k"]), b":-1\r\n".to_vec());
        // SET ... KEEPTTL preserves it
        cmd(&mut db, &[b"SET", b"k", b"v3", b"EX", b"100"]);
        cmd(&mut db, &[b"SET", b"k", b"v4", b"KEEPTTL"]);
        let ttl = cmd(&mut db, &[b"TTL", b"k"]);
        assert!(ttl != b":-1\r\n".to_vec(), "KEEPTTL should preserve ttl");
    }
}
