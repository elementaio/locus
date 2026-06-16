//! AOF (append-only file) persistence + crash recovery.
//!
//! Every write command is appended to a log in RESP, and the log is replayed on
//! startup to rebuild the dataset. Three things make this correct:
//!
//!  * REPLAY IS TORN-TAIL TOLERANT: a crash can truncate the final command, so
//!    replay stops at the last *complete* command instead of erroring.
//!  * NON-DETERMINISM IS REWRITTEN AT LOG TIME: relative TTLs become absolute
//!    PEXPIREAT, and SPOP (which removes random members) is logged as the exact
//!    SREM it produced — so replaying never diverges from the original run.
//!  * FSYNC: we fsync ~once per second. (Real Redis does this on a background
//!    thread to avoid stalling the loop; we fsync inline here for simplicity.)
//!
//! Enabled by setting LOCUS_AOF (a path, or "1" for the default file).

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};

use crate::commands::execute;
use crate::db::{now_ms, Db, Value};
use crate::resp::{parse_command, Parsed};

pub const DEFAULT_PATH: &str = "locus.aof";

pub fn configured_path() -> Option<String> {
    match std::env::var("LOCUS_AOF") {
        Ok(v) if !v.is_empty() => Some(if matches!(v.as_str(), "1" | "on" | "yes") {
            DEFAULT_PATH.to_string()
        } else {
            v
        }),
        _ => None,
    }
}

/// Commands that modify the dataset (and so must be logged).
pub fn is_write(cmd: &[u8]) -> bool {
    matches!(
        cmd.to_ascii_uppercase().as_slice(),
        b"SET" | b"GETDEL" | b"DEL" | b"EXPIRE" | b"PEXPIRE" | b"EXPIREAT" | b"PEXPIREAT"
            | b"PERSIST" | b"INCR" | b"DECR" | b"INCRBY" | b"DECRBY" | b"APPEND"
            | b"LPUSH" | b"RPUSH" | b"LPUSHX" | b"RPUSHX" | b"LPOP" | b"RPOP" | b"LSET"
            | b"HSET" | b"HSETNX" | b"HDEL" | b"HINCRBY"
            | b"SADD" | b"SREM" | b"SPOP"
            | b"ZADD" | b"ZREM" | b"ZINCRBY" | b"ZPOPMIN" | b"ZPOPMAX"
    )
}

pub struct Aof {
    file: File,
    last_fsync: u64,
}

impl Aof {
    pub fn open(path: &str) -> io::Result<Aof> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Aof {
            file,
            last_fsync: now_ms(),
        })
    }

    pub fn append(&mut self, commands: &[Vec<Vec<u8>>]) -> io::Result<()> {
        let mut buf = Vec::new();
        for c in commands {
            encode_command(&mut buf, c);
        }
        self.file.write_all(&buf)
    }

    /// fsync at most once per second (the "everysec" policy).
    pub fn maybe_fsync(&mut self) {
        if now_ms().saturating_sub(self.last_fsync) >= 1000 {
            let _ = self.file.sync_data();
            self.last_fsync = now_ms();
        }
    }
}

fn encode_command(buf: &mut Vec<u8>, cmd: &[Vec<u8>]) {
    buf.extend_from_slice(format!("*{}\r\n", cmd.len()).as_bytes());
    for arg in cmd {
        buf.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
        buf.extend_from_slice(arg);
        buf.extend_from_slice(b"\r\n");
    }
}

/// Given a just-executed write command (+ its reply), return the command(s) to
/// log — rewriting non-deterministic ones into deterministic, replay-safe form.
pub fn entries_for(tokens: &[Vec<u8>], reply: &[u8], db: &mut Db) -> Vec<Vec<Vec<u8>>> {
    match tokens[0].to_ascii_uppercase().as_slice() {
        b"SET" => {
            let key = &tokens[1];
            // Log the RESULTING state (handles NX/XX no-ops) + absolute TTL.
            match db.get(key) {
                Some(Value::Str(v)) => {
                    let v = v.clone();
                    let mut out = vec![vec![b"SET".to_vec(), key.clone(), v]];
                    if let Some(t) = db.expire_at(key) {
                        out.push(pexpireat(key, t));
                    }
                    out
                }
                _ => vec![],
            }
        }
        b"EXPIRE" | b"PEXPIRE" | b"EXPIREAT" | b"PEXPIREAT" => {
            let key = &tokens[1];
            if !db.contains(key) {
                vec![vec![b"DEL".to_vec(), key.clone()]] // deadline already passed
            } else if let Some(t) = db.expire_at(key) {
                vec![pexpireat(key, t)]
            } else {
                vec![]
            }
        }
        b"SPOP" => {
            // Log the exact members removed (parsed from the reply), not SPOP.
            let popped = extract_bulks(reply);
            if popped.is_empty() {
                vec![]
            } else {
                let mut c = vec![b"SREM".to_vec(), tokens[1].clone()];
                c.extend(popped);
                vec![c]
            }
        }
        _ => vec![tokens.to_vec()],
    }
}

fn pexpireat(key: &[u8], at_ms: u64) -> Vec<Vec<u8>> {
    vec![b"PEXPIREAT".to_vec(), key.to_vec(), at_ms.to_string().into_bytes()]
}

fn local_crlf(buf: &[u8], from: usize) -> Option<usize> {
    let mut i = from;
    while i + 1 < buf.len() {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// Extract bulk strings from a reply that is a bulk string or array of them.
fn extract_bulks(reply: &[u8]) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    let mut i = 0;
    if reply.first() == Some(&b'*') {
        match local_crlf(reply, 0) {
            Some(nl) => i = nl + 2,
            None => return out,
        }
    }
    while i < reply.len() {
        if reply[i] != b'$' {
            break;
        }
        let nl = match local_crlf(reply, i) {
            Some(n) => n,
            None => break,
        };
        let len: i64 = std::str::from_utf8(&reply[i + 1..nl])
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(-1);
        i = nl + 2;
        if len < 0 {
            continue; // null bulk
        }
        let end = i + len as usize;
        if end > reply.len() {
            break;
        }
        out.push(reply[i..end].to_vec());
        i = end + 2; // skip trailing CRLF
    }
    out
}

/// Replay the AOF to rebuild the dataset. Tolerant of a truncated final command.
pub fn load(path: &str) -> io::Result<Db> {
    let mut data = Vec::new();
    match File::open(path) {
        Ok(mut f) => {
            f.read_to_end(&mut data)?;
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Db::new()),
        Err(e) => return Err(e),
    }
    let mut db = Db::new();
    let mut pos = 0;
    while pos < data.len() {
        match parse_command(&data[pos..]) {
            Parsed::Complete(tokens, consumed) => {
                if !tokens.is_empty() {
                    execute(&tokens, &mut db); // replay; no re-logging happens here
                }
                pos += consumed;
            }
            // Truncated or corrupt tail — stop at the last complete command.
            Parsed::Incomplete | Parsed::Error(_) => break,
        }
    }
    Ok(db)
}

/// Compact the AOF: rewrite it as the minimal set of commands that rebuilds the
/// current dataset (BGREWRITEAOF). temp -> fsync -> atomic rename.
pub fn rewrite(db: &Db, path: &str) -> io::Result<()> {
    let tmp = format!("{path}.tmp");
    {
        let file = File::create(&tmp)?;
        let mut buf = Vec::new();
        for (key, value) in db.entries() {
            for c in reconstruct(key, value) {
                encode_command(&mut buf, &c);
            }
            if let Some(t) = db.raw_expire(key) {
                encode_command(&mut buf, &pexpireat(key, t));
            }
        }
        let mut w = file;
        w.write_all(&buf)?;
        w.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    Ok(())
}

fn reconstruct(key: &[u8], value: &Value) -> Vec<Vec<Vec<u8>>> {
    let k = key.to_vec();
    match value {
        Value::Str(s) => vec![vec![b"SET".to_vec(), k, s.clone()]],
        Value::List(l) => {
            let mut c = vec![b"RPUSH".to_vec(), k];
            c.extend(l.iter().cloned());
            vec![c]
        }
        Value::Hash(h) => {
            let mut c = vec![b"HSET".to_vec(), k];
            for (f, v) in h {
                c.push(f.clone());
                c.push(v.clone());
            }
            vec![c]
        }
        Value::Set(s) => {
            let mut c = vec![b"SADD".to_vec(), k];
            c.extend(s.iter().cloned());
            vec![c]
        }
        Value::ZSet(z) => {
            let mut c = vec![b"ZADD".to_vec(), k];
            for (m, score) in z {
                c.push(fmt_score(*score));
                c.push(m.clone());
            }
            vec![c]
        }
    }
}

fn fmt_score(s: f64) -> Vec<u8> {
    if s.is_infinite() {
        if s > 0.0 { b"inf".to_vec() } else { b"-inf".to_vec() }
    } else {
        format!("{s}").into_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(db: &mut Db, parts: &[&[u8]]) -> Vec<u8> {
        let t: Vec<Vec<u8>> = parts.iter().map(|p| p.to_vec()).collect();
        execute(&t, db)
    }

    #[test]
    fn append_replay_roundtrip() {
        let path = "/tmp/locus_aof_test.aof";
        let _ = fs::remove_file(path);
        let mut a = Aof::open(path).unwrap();
        // Log a few commands by hand (as the owner would).
        a.append(&[vec![b"SET".to_vec(), b"k".to_vec(), b"v".to_vec()]]).unwrap();
        a.append(&[vec![b"RPUSH".to_vec(), b"l".to_vec(), b"a".to_vec(), b"b".to_vec()]]).unwrap();
        a.append(&[vec![b"INCR".to_vec(), b"c".to_vec()]]).unwrap();
        a.append(&[vec![b"INCR".to_vec(), b"c".to_vec()]]).unwrap();
        drop(a);

        let mut db = load(path).unwrap();
        assert_eq!(run(&mut db, &[b"GET", b"k"]), b"$1\r\nv\r\n".to_vec());
        assert_eq!(run(&mut db, &[b"LLEN", b"l"]), b":2\r\n".to_vec());
        assert_eq!(run(&mut db, &[b"GET", b"c"]), b"$1\r\n2\r\n".to_vec());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn torn_tail_is_tolerated() {
        let path = "/tmp/locus_aof_torn.aof";
        let _ = fs::remove_file(path);
        let mut a = Aof::open(path).unwrap();
        a.append(&[vec![b"SET".to_vec(), b"ok".to_vec(), b"1".to_vec()]]).unwrap();
        drop(a);
        // Simulate a crash mid-write: append a truncated command.
        let mut f = OpenOptions::new().append(true).open(path).unwrap();
        f.write_all(b"*3\r\n$3\r\nSET\r\n$4\r\nhalf").unwrap(); // no value/CRLF
        drop(f);

        let mut db = load(path).unwrap();
        assert_eq!(run(&mut db, &[b"GET", b"ok"]), b"$1\r\n1\r\n".to_vec());
        assert_eq!(run(&mut db, &[b"EXISTS", b"half"]), b":0\r\n".to_vec()); // torn cmd dropped
        let _ = fs::remove_file(path);
    }

    #[test]
    fn spop_is_logged_as_srem() {
        // SPOP reply -> SREM of the exact members
        let single = extract_bulks(b"$1\r\na\r\n");
        assert_eq!(single, vec![b"a".to_vec()]);
        let multi = extract_bulks(b"*2\r\n$1\r\na\r\n$1\r\nb\r\n");
        assert_eq!(multi, vec![b"a".to_vec(), b"b".to_vec()]);
    }
}
