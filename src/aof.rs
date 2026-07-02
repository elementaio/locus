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
use crate::db::{Db, Value, now_ms};
use crate::resp::{Parsed, parse_command};

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
/// Whether a command mutates the keyspace (and so must be logged/replicated).
/// Delegates to the single command table in `commands` so there is no separate
/// write-list to keep in sync.
pub fn is_write(cmd: &[u8]) -> bool {
    crate::commands::is_write(cmd)
}

/// When to fsync the AOF: `always` = after every write (safest, slowest),
/// `everysec` = at most once a second (Redis's default), `no` = never (let the
/// OS flush). Set via LOCUS_APPENDFSYNC.
#[derive(Clone, Copy, PartialEq)]
pub enum FsyncPolicy {
    Always,
    Everysec,
    No,
}

fn policy_from_env() -> FsyncPolicy {
    match std::env::var("LOCUS_APPENDFSYNC")
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "always" => FsyncPolicy::Always,
        "no" => FsyncPolicy::No,
        _ => FsyncPolicy::Everysec, // default, matches Redis
    }
}

pub struct Aof {
    file: File,
    last_fsync: u64,
    policy: FsyncPolicy,
    /// Latches false on the first failed append or fsync and stays false: the
    /// file now has a hole (an applied-but-unlogged write), so it can't be
    /// trusted again until a full rewrite replaces it. Read by INFO
    /// (`aof_last_write_status`) and the hub's write gate + recovery loop.
    healthy: bool,
}

impl Aof {
    pub fn open(path: &str) -> io::Result<Aof> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Aof {
            file,
            last_fsync: now_ms(),
            policy: policy_from_env(),
            healthy: true,
        })
    }

    /// False after any failed append/fsync — the log has a hole until rewritten.
    pub fn healthy(&self) -> bool {
        self.healthy
    }

    pub fn append(&mut self, commands: &[Vec<Vec<u8>>]) -> io::Result<()> {
        let mut buf = Vec::new();
        for c in commands {
            encode_command(&mut buf, c);
        }
        if let Err(e) = self.file.write_all(&buf) {
            self.healthy = false;
            crate::log::error(&format!("AOF append failed: {e}"));
            return Err(e);
        }
        if self.policy == FsyncPolicy::Always {
            self.do_fsync();
        }
        Ok(())
    }

    /// Under the `everysec` policy, fsync at most once per second. (`always` syncs
    /// inline in append; `no` never syncs here.)
    pub fn maybe_fsync(&mut self) {
        if self.policy == FsyncPolicy::Everysec && now_ms().saturating_sub(self.last_fsync) >= 1000
        {
            self.do_fsync();
        }
    }

    /// Force an fsync now, regardless of the everysec timer (graceful shutdown).
    pub fn fsync(&mut self) {
        self.do_fsync();
    }

    /// fsync the AOF, surfacing (not swallowing) a failure — a silently-dropped
    /// fsync error means "everysec" durability is quietly broken.
    fn do_fsync(&mut self) {
        if let Err(e) = self.file.sync_data() {
            self.healthy = false;
            crate::log::error(&format!("AOF fsync failed: {e}"));
        }
        self.last_fsync = now_ms();
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
            // Log the RESULTING state (handles NX/XX no-ops) + absolute TTL. Read
            // the deadline RAW (no passive expiry): if it's already in the past,
            // log a DEL so replay removes any prior value instead of resurrecting
            // it. Value and TTL go in ONE `SET ... PXAT` record — as two records,
            // a crash between them would replay the value without its deadline
            // and resurrect an immortal key.
            match db.raw_expire(key) {
                Some(t) if t <= now_ms() => vec![vec![b"DEL".to_vec(), key.clone()]],
                deadline => match db.get(key) {
                    Some(Value::Str(v)) => {
                        let mut c = vec![b"SET".to_vec(), key.clone(), v.clone()];
                        if let Some(t) = deadline {
                            c.push(b"PXAT".to_vec());
                            c.push(t.to_string().into_bytes());
                        }
                        vec![c]
                    }
                    _ => vec![],
                },
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
        b"GETEX" => {
            // Only a TTL-changing GETEX is a write; log the resulting deadline
            // (absolute), a PERSIST, or a DEL if it's already past.
            let key = &tokens[1];
            if tokens.len() <= 2 || !db.contains(key) {
                vec![]
            } else {
                match db.raw_expire(key) {
                    Some(t) if t <= now_ms() => vec![vec![b"DEL".to_vec(), key.clone()]],
                    Some(t) => vec![pexpireat(key, t)],
                    None => vec![vec![b"PERSIST".to_vec(), key.clone()]],
                }
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
        b"XADD" => {
            // Log the concrete generated id (from the reply), never "*".
            match extract_bulks(reply).into_iter().next() {
                Some(realid) if tokens.len() > 2 => {
                    let mut c = tokens.to_vec();
                    c[2] = realid;
                    vec![c]
                }
                _ => vec![],
            }
        }
        // CAS-family: only reaches here on success — log the concrete effect.
        b"CAS" => vec![vec![b"SET".to_vec(), tokens[1].clone(), tokens[3].clone()]],
        b"CADEL" => vec![vec![b"DEL".to_vec(), tokens[1].clone()]],
        b"SETMAX" | b"INCRCAP" => match db.get(&tokens[1]) {
            Some(Value::Str(v)) => vec![vec![b"SET".to_vec(), tokens[1].clone(), v.clone()]],
            _ => vec![],
        },
        _ => vec![tokens.to_vec()],
    }
}

fn pexpireat(key: &[u8], at_ms: u64) -> Vec<Vec<u8>> {
    vec![
        b"PEXPIREAT".to_vec(),
        key.to_vec(),
        at_ms.to_string().into_bytes(),
    ]
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

/// Replay the AOF to rebuild the dataset.
///
/// A crash can only ever truncate the FINAL command, so a short/incomplete tail
/// is tolerated (stop at the last complete command). But a `Parsed::Error` — a
/// structurally invalid frame — with more bytes after it is MID-FILE corruption
/// (a flipped byte, a bad disk), not a torn tail: silently stopping there would
/// hide an unbounded amount of still-present history. That is refused (unless
/// LOCUS_AOF_LOAD_TRUNCATED=yes), so the operator sees it instead of quietly
/// starting with half the data and appending after the hole.
pub fn load(path: &str) -> io::Result<Db> {
    let mut data = Vec::new();
    match File::open(path) {
        Ok(mut f) => {
            f.read_to_end(&mut data)?;
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Db::new()),
        Err(e) => return Err(e),
    }
    let allow_truncated = std::env::var("LOCUS_AOF_LOAD_TRUNCATED")
        .map(|v| matches!(v.trim(), "yes" | "1" | "on" | "true"))
        .unwrap_or(false);
    let total = data.len();
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
            // A structurally-invalid frame with bytes remaining after it can't
            // be a torn tail (a crash truncates, it doesn't corrupt the middle):
            // refuse rather than silently drop the rest of the history.
            Parsed::Error(msg) if !allow_truncated => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "AOF corruption at byte {pos}/{total}: {msg} (set LOCUS_AOF_LOAD_TRUNCATED=yes to load what precedes it)"
                    ),
                ));
            }
            // Truncated final command (or, with the override, a corrupt tail) —
            // stop at the last complete command.
            Parsed::Incomplete | Parsed::Error(_) => break,
        }
    }
    Ok(db)
}

/// Serialize the whole dataset as the minimal command set that rebuilds it — the
/// base image for an AOF rewrite (BGREWRITEAOF). Pure in-memory; the disk write
/// is done off the hub thread (see `write_tmp` / `finalize_rewrite`).
pub fn serialize_rewrite(db: &Db) -> Vec<u8> {
    let mut buf = Vec::new();
    for (key, value) in db.entries() {
        for c in reconstruct(key, value) {
            encode_command(&mut buf, &c);
        }
        if let Some(t) = db.raw_expire(key) {
            encode_command(&mut buf, &pexpireat(key, t));
        }
    }
    buf
}

/// Encode already-deterministic commands onto `buf` — used to capture writes that
/// land while an async rewrite's base image is being written off-thread.
pub fn encode_into(buf: &mut Vec<u8>, commands: &[Vec<Vec<u8>>]) {
    for c in commands {
        encode_command(buf, c);
    }
}

/// Write a rewrite's base image to a temp file and fsync it (runs off-thread).
pub fn write_tmp(tmp: &str, buf: &[u8]) -> io::Result<()> {
    let mut w = File::create(tmp)?;
    w.write_all(buf)?;
    w.sync_all()
}

/// Finish an async rewrite (on the hub): append the writes buffered during the
/// rewrite onto the base image, fsync, then atomically swap it into place.
pub fn finalize_rewrite(tmp: &str, path: &str, tail: &[u8]) -> io::Result<()> {
    if !tail.is_empty() {
        let mut f = OpenOptions::new().append(true).open(tmp)?;
        f.write_all(tail)?;
        f.sync_all()?;
    }
    fs::rename(tmp, path)?;
    crate::rdb::fsync_parent_dir(path); // make the rename durable
    Ok(())
}

/// Deterministic command(s) that rebuild `key` = `value` (+ absolute TTL) — the
/// durable/replicable form of a migrated key, so slot migration flows through
/// the same AOF + replication path as a client write instead of a raw insert.
pub fn restore_entries(key: &[u8], value: &Value, expire: Option<u64>) -> Vec<Vec<Vec<u8>>> {
    let mut cmds = reconstruct(key, value);
    if let Some(t) = expire {
        cmds.push(pexpireat(key, t));
    }
    cmds
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
            for (m, score) in z.iter() {
                c.push(fmt_score(*score));
                c.push(m.clone());
            }
            vec![c]
        }
        Value::Stream(s) => s
            .entries
            .iter()
            .map(|(id, fields)| {
                let mut c = vec![b"XADD".to_vec(), key.to_vec(), crate::streams::fmt_id(*id)];
                for (f, v) in fields {
                    c.push(f.clone());
                    c.push(v.clone());
                }
                c
            })
            .collect(),
        Value::Geo(lon, lat, attrs) => {
            let mut c = vec![
                b"GEOSET".to_vec(),
                k,
                format!("{lon}").into_bytes(),
                format!("{lat}").into_bytes(),
            ];
            for (f, v) in attrs {
                c.push(f.clone());
                c.push(v.clone());
            }
            vec![c]
        }
        // A sketch can't be rebuilt from its add-history; restore raw state.
        Value::Bloom(b) => vec![vec![
            b"BFLOAD".to_vec(),
            k,
            b.k.to_string().into_bytes(),
            b.nbits.to_string().into_bytes(),
            b.bits.clone(),
        ]],
        Value::Cms(c) => vec![vec![
            b"CMSLOAD".to_vec(),
            k,
            c.width.to_string().into_bytes(),
            c.depth.to_string().into_bytes(),
            c.to_bytes(),
        ]],
        Value::TopK(t) => vec![vec![b"TOPKLOAD".to_vec(), k, t.to_bytes()]],
        Value::TDigest(t) => vec![vec![b"TDLOAD".to_vec(), k, t.to_bytes()]],
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;

    fn run(db: &mut Db, parts: &[&[u8]]) -> Vec<u8> {
        let t: Vec<Vec<u8>> = parts.iter().map(|p| p.to_vec()).collect();
        execute(&t, db)
    }

    #[test]
    fn set_with_already_past_ttl_logs_del_not_a_stale_value() {
        let mut db = Db::new();
        // A prior value, then a SET that leaves an already-past deadline: replay
        // must DEL the key, not keep the stale value.
        db.insert_with_expire(b"k".to_vec(), Value::Str(b"v".to_vec()), Some(1));
        let toks: Vec<Vec<u8>> = [&b"SET"[..], b"k", b"v", b"PXAT", b"1"]
            .iter()
            .map(|s| s.to_vec())
            .collect();
        assert_eq!(
            entries_for(&toks, b"+OK\r\n", &mut db),
            vec![vec![b"DEL".to_vec(), b"k".to_vec()]]
        );
    }

    #[test]
    fn set_with_ttl_logs_one_atomic_record() {
        let mut db = Db::new();
        let future = now_ms() + 60_000;
        let toks: Vec<Vec<u8>> = [
            &b"SET"[..],
            b"k",
            b"v",
            b"PXAT",
            future.to_string().as_bytes(),
        ]
        .iter()
        .map(|s| s.to_vec())
        .collect();
        execute(&toks, &mut db);
        // ONE record carrying both value and deadline — never SET + PEXPIREAT,
        // where a torn tail between them would resurrect an immortal key.
        assert_eq!(
            entries_for(&toks, b"+OK\r\n", &mut db),
            vec![vec![
                b"SET".to_vec(),
                b"k".to_vec(),
                b"v".to_vec(),
                b"PXAT".to_vec(),
                future.to_string().into_bytes(),
            ]]
        );
    }

    #[test]
    fn append_replay_roundtrip() {
        let path = "/tmp/locus_aof_test.aof";
        let _ = fs::remove_file(path);
        let mut a = Aof::open(path).unwrap();
        // Log a few commands by hand (as the owner would).
        a.append(&[vec![b"SET".to_vec(), b"k".to_vec(), b"v".to_vec()]])
            .unwrap();
        a.append(&[vec![
            b"RPUSH".to_vec(),
            b"l".to_vec(),
            b"a".to_vec(),
            b"b".to_vec(),
        ]])
        .unwrap();
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
        a.append(&[vec![b"SET".to_vec(), b"ok".to_vec(), b"1".to_vec()]])
            .unwrap();
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
    fn mid_file_corruption_is_refused_but_torn_tail_is_not() {
        let path = "/tmp/locus_aof_midcorrupt.aof";
        let _ = fs::remove_file(path);
        let mut a = Aof::open(path).unwrap();
        a.append(&[vec![b"SET".to_vec(), b"a".to_vec(), b"1".to_vec()]])
            .unwrap();
        // A structurally-invalid frame in the MIDDLE, then a valid command
        // after it — this can't be a crash's torn tail (a crash truncates the
        // end), so replay must refuse rather than silently drop the rest.
        let mut f = OpenOptions::new().append(true).open(path).unwrap();
        f.write_all(b"*9\r\n+notabulk\r\n").unwrap(); // bad frame mid-file
        f.write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n")
            .unwrap();
        drop(f);
        assert!(load(path).is_err(), "mid-file corruption should be refused");

        // The override recovers everything up to the corruption.
        unsafe { std::env::set_var("LOCUS_AOF_LOAD_TRUNCATED", "yes") };
        let mut db = load(path).unwrap();
        assert_eq!(run(&mut db, &[b"GET", b"a"]), b"$1\r\n1\r\n".to_vec());
        unsafe { std::env::remove_var("LOCUS_AOF_LOAD_TRUNCATED") };
        let _ = fs::remove_file(path);
    }

    #[test]
    fn async_rewrite_base_plus_tail_roundtrips() {
        let path = "/tmp/locus_aof_rewrite.aof";
        let tmp = format!("{path}.tmp");
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(&tmp);
        // Base image captured before the (off-thread) rewrite.
        let mut db = Db::new();
        db.insert_with_expire(b"k".to_vec(), Value::Str(b"v".to_vec()), None);
        let base = serialize_rewrite(&db);
        write_tmp(&tmp, &base).unwrap();
        // A write that lands during the rewrite, captured as a tail and folded in.
        let mut tail = Vec::new();
        encode_into(
            &mut tail,
            &[vec![b"SET".to_vec(), b"k2".to_vec(), b"v2".to_vec()]],
        );
        finalize_rewrite(&tmp, path, &tail).unwrap();
        // Replaying the swapped-in file yields base + tail, nothing lost.
        let mut loaded = load(path).unwrap();
        assert_eq!(run(&mut loaded, &[b"GET", b"k"]), b"$1\r\nv\r\n".to_vec());
        assert_eq!(run(&mut loaded, &[b"GET", b"k2"]), b"$2\r\nv2\r\n".to_vec());
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
