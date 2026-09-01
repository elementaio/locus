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
//!  * FSYNC: under `everysec` (the default) we fsync ~once per second, and — as
//!    Redis does — on a DEDICATED THREAD, never on the hub. One hub thread owns
//!    the whole keyspace, so a `sync_data()` there stalls every client on the
//!    machine for as long as the disk takes. Under `always` the fsync stays
//!    inline and its failure is RETURNED, because that policy's whole promise is
//!    that the client is not told `+OK` until the bytes are down.
//!
//! Enabled by setting LOCUS_AOF (a path, or "1" for the default file).

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};

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

/// Debug-only fsync fault injection (`DEBUG AOFFSYNCFAIL 1`), so the durability
/// contract can be tested without a real full disk. Process-global because the
/// off-hub fsync thread has to see it too, and because it is set exactly once,
/// from the hub, in a debug build. Off in a release build: `DEBUG` refuses it,
/// the same way it refuses `DEBUG PANIC`.
static FSYNC_FAULT: AtomicBool = AtomicBool::new(false);

pub fn set_fsync_fault(on: bool) {
    FSYNC_FAULT.store(on, Ordering::Relaxed);
}

/// The one place an fsync actually happens — and the one place the injected
/// fault is honoured, so the real and the simulated failure take the same path.
fn sync_now(file: &File) -> io::Result<()> {
    if FSYNC_FAULT.load(Ordering::Relaxed) {
        return Err(io::Error::other(
            "injected fsync failure (DEBUG AOFFSYNCFAIL)",
        ));
    }
    file.sync_data()
}

/// What the fsync thread and its owner share.
struct SyncState {
    /// A sync has been asked for and not yet started. Coalescing: two requests
    /// arriving before the thread wakes are one fsync, which is exactly right —
    /// an fsync flushes everything written so far, so a queue of them would be
    /// wasted work, and an unbounded queue would be a leak.
    pending: bool,
    stop: bool,
    /// Completed fsyncs, and which thread ran the last one. Test observability:
    /// this is how `everysec_fsync_runs_off_the_calling_thread` proves the work
    /// left the hub instead of timing it and hoping.
    done: u64,
    #[cfg(test)]
    thread: Option<thread::ThreadId>,
}

/// The dedicated fsync thread for the `everysec` policy.
///
/// It holds its own `dup()` of the AOF fd (`try_clone`), so it can `sync_data()`
/// the same file description the hub is appending to without sharing the `File`
/// through a lock. fsync and append may run concurrently — an fsync flushes
/// whatever has been written *so far*, which is the everysec contract.
struct Syncer {
    state: Arc<(Mutex<SyncState>, Condvar)>,
    handle: Option<JoinHandle<()>>,
}

impl Syncer {
    /// Returns None if the fd could not be duplicated or the thread could not be
    /// spawned; the caller then falls back to syncing inline — a stall is bad,
    /// silently stopping fsyncs would be a durability lie.
    fn spawn(file: &File, healthy: Arc<AtomicBool>) -> Option<Syncer> {
        let dup = file
            .try_clone()
            .map_err(|e| crate::log::warn(&format!("AOF fsync thread: fd clone failed: {e}")))
            .ok()?;
        let state = Arc::new((
            Mutex::new(SyncState {
                pending: false,
                stop: false,
                done: 0,
                #[cfg(test)]
                thread: None,
            }),
            Condvar::new(),
        ));
        let shared = state.clone();
        let handle = thread::Builder::new()
            .name("locus-aof-fsync".into())
            .spawn(move || {
                let (lock, cv) = &*shared;
                loop {
                    let mut st = lock.lock().unwrap_or_else(|e| e.into_inner());
                    while !st.pending && !st.stop {
                        st = cv.wait(st).unwrap_or_else(|e| e.into_inner());
                    }
                    if st.stop && !st.pending {
                        break;
                    }
                    st.pending = false;
                    drop(st);
                    let res = sync_now(&dup);
                    let mut st = lock.lock().unwrap_or_else(|e| e.into_inner());
                    st.done += 1;
                    #[cfg(test)]
                    {
                        st.thread = Some(thread::current().id());
                    }
                    drop(st);
                    if let Err(e) = res {
                        // Latches the AOF unhealthy exactly as an inline failure
                        // does: the hub's write gate then rejects writes until a
                        // recovery rewrite replaces the file.
                        healthy.store(false, Ordering::Relaxed);
                        crate::log::error(&format!("AOF fsync failed: {e}"));
                    }
                    cv.notify_all();
                }
            })
            .map_err(|e| crate::log::warn(&format!("AOF fsync thread: spawn failed: {e}")))
            .ok()?;
        Some(Syncer {
            state,
            handle: Some(handle),
        })
    }

    /// Ask for an fsync and return immediately. This is the call that used to be
    /// a `sync_data()` on the hub.
    fn request(&self) {
        let (lock, cv) = &*self.state;
        let mut st = lock.lock().unwrap_or_else(|e| e.into_inner());
        st.pending = true;
        drop(st);
        cv.notify_all();
    }

    #[cfg(test)]
    fn done(&self) -> u64 {
        let (lock, _) = &*self.state;
        let st = lock.lock().unwrap_or_else(|e| e.into_inner());
        st.done
    }

    #[cfg(test)]
    fn last_thread(&self) -> Option<thread::ThreadId> {
        let (lock, _) = &*self.state;
        let st = lock.lock().unwrap_or_else(|e| e.into_inner());
        st.thread
    }
}

impl Drop for Syncer {
    fn drop(&mut self) {
        {
            let (lock, cv) = &*self.state;
            let mut st = lock.lock().unwrap_or_else(|e| e.into_inner());
            st.stop = true;
            drop(st);
            cv.notify_all();
        }
        // Join: an AOF is replaced on every rewrite, and a leaked thread per
        // rewrite would be a slow leak of both threads and fds. The cost is that
        // a rewrite completing while an fsync is in flight blocks the hub for
        // the rest of that one fsync — a bounded wait, once per rewrite, versus
        // an unbounded thread leak. (Detaching instead would also risk the fd
        // outliving the file the rewrite just replaced.)
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
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
    /// Shared with the fsync thread, which is the other thing that can fail.
    healthy: Arc<AtomicBool>,
    /// The off-hub fsync thread — `Some` only under `everysec`, and only if it
    /// started. `always` syncs inline (it must return the error), `no` never
    /// syncs here at all.
    syncer: Option<Syncer>,
}

impl Aof {
    pub fn open(path: &str) -> io::Result<Aof> {
        Aof::open_with_policy(path, policy_from_env())
    }

    /// `open` with the policy passed in rather than read from the environment —
    /// the tests need both policies in one process, and mutating a process-wide
    /// env var from parallel tests is a race, not a fixture.
    fn open_with_policy(path: &str, policy: FsyncPolicy) -> io::Result<Aof> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let healthy = Arc::new(AtomicBool::new(true));
        let syncer = if policy == FsyncPolicy::Everysec {
            Syncer::spawn(&file, healthy.clone())
        } else {
            None
        };
        Ok(Aof {
            file,
            last_fsync: now_ms(),
            policy,
            healthy,
            syncer,
        })
    }

    /// False after any failed append/fsync — the log has a hole until rewritten.
    pub fn healthy(&self) -> bool {
        self.healthy.load(Ordering::Relaxed)
    }

    /// Append commands to the log.
    ///
    /// Under `always` this includes the fsync, and **its failure is returned**.
    /// It used to be swallowed: `do_fsync` latched the AOF unhealthy but
    /// `append` still returned `Ok(())`, so the very write whose fsync had just
    /// failed was acked `+OK` and only the *next* one was refused by the health
    /// gate. `always` exists to promise that a client is never told a write is
    /// durable before it is; that promise was being broken on the one write that
    /// mattered. The caller turns an `Err` here into an error reply.
    pub fn append(&mut self, commands: &[Vec<Vec<u8>>]) -> io::Result<()> {
        let mut buf = Vec::new();
        for c in commands {
            encode_command(&mut buf, c);
        }
        if let Err(e) = self.file.write_all(&buf) {
            self.healthy.store(false, Ordering::Relaxed);
            crate::log::error(&format!("AOF append failed: {e}"));
            return Err(e);
        }
        if self.policy == FsyncPolicy::Always {
            self.sync_inline()?;
        }
        Ok(())
    }

    /// Under the `everysec` policy, ask the fsync thread to sync, at most once
    /// per second. Returns immediately: the disk work happens off the hub, which
    /// is the whole point — a `sync_data()` here blocked every client on the
    /// server for as long as the device took. (`always` syncs inline in
    /// `append`; `no` never syncs here.)
    ///
    /// The once-per-second bound is on the REQUEST, so a device slower than a
    /// second cannot queue up work: requests coalesce into one pending fsync.
    pub fn maybe_fsync(&mut self) {
        if self.policy != FsyncPolicy::Everysec || now_ms().saturating_sub(self.last_fsync) < 1000 {
            return;
        }
        self.last_fsync = now_ms();
        match &self.syncer {
            Some(s) => s.request(),
            // No thread (clone/spawn failed): keep the guarantee and pay the
            // stall. Dropping the fsync instead would silently downgrade
            // `everysec` to `no`.
            None => {
                let _ = self.sync_inline();
            }
        }
    }

    /// Force an fsync now, on THIS thread, and wait for it — graceful shutdown
    /// and the slot-migration commit points. Deliberately synchronous: the
    /// process is about to exit (or must not proceed), so "asked for" is not
    /// good enough, "on the disk" is. Syncing our own fd covers everything
    /// written so far, whatever the fsync thread is doing.
    pub fn fsync(&mut self) {
        let _ = self.sync_inline();
    }

    /// fsync on the calling thread, surfacing (not swallowing) a failure — a
    /// silently-dropped fsync error means durability is quietly broken.
    fn sync_inline(&mut self) -> io::Result<()> {
        self.last_fsync = now_ms();
        match sync_now(&self.file) {
            Ok(()) => Ok(()),
            Err(e) => {
                self.healthy.store(false, Ordering::Relaxed);
                crate::log::error(&format!("AOF fsync failed: {e}"));
                Err(e)
            }
        }
    }

    /// True when this policy acks a write only after the bytes are synced — the
    /// hub uses it to decide whether a failed append must become an error reply
    /// to the client that issued it.
    pub fn acks_after_fsync(&self) -> bool {
        self.policy == FsyncPolicy::Always
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

/// A changefeed consumer-group mutation replayed from the log.
///
/// Groups live on the hub, not in `Db` — the loader cannot apply these itself,
/// so it collects them in log order and hands them back for the hub to fold in
/// after the snapshot trailer (see `Hub::apply_cdc_group_ops`).
#[derive(Debug, PartialEq)]
pub enum CdcGroupOp {
    /// `CDCGROUP CREATE <name> <start>` — the start offset travels with it, so a
    /// replayed group is recreated at its original cursor origin, not at "now".
    Create {
        name: Vec<u8>,
        start: u64,
    },
    Destroy {
        name: Vec<u8>,
    },
}

/// Recognize a logged `CDCGROUP CREATE|DESTROY`. Anything else — including the
/// read verbs, which are never logged — returns `None`.
fn cdc_group_op(tokens: &[Vec<u8>]) -> Option<CdcGroupOp> {
    // Length first: an empty token list must not index.
    if tokens.len() < 3 || !tokens[0].eq_ignore_ascii_case(b"CDCGROUP") {
        return None;
    }
    let name = tokens[2].clone();
    if tokens[1].eq_ignore_ascii_case(b"CREATE") {
        // A CREATE without a resolved offset cannot be replayed faithfully
        // (there is no "now" at load time); treat it as offset 0 — the
        // conservative direction, since a group can only be re-delivered
        // records it may already have seen, never skip past unseen ones.
        let start = tokens
            .get(3)
            .and_then(|t| std::str::from_utf8(t).ok())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        Some(CdcGroupOp::Create { name, start })
    } else if tokens[1].eq_ignore_ascii_case(b"DESTROY") {
        Some(CdcGroupOp::Destroy { name })
    } else {
        None
    }
}

/// Replay the AOF to rebuild the dataset (plus the changefeed group
/// create/destroy ops it carries, which belong to the hub).
///
/// A crash can only ever truncate the FINAL command, so a short/incomplete tail
/// is tolerated (stop at the last complete command). But a `Parsed::Error` — a
/// structurally invalid frame — with more bytes after it is MID-FILE corruption
/// (a flipped byte, a bad disk), not a torn tail: silently stopping there would
/// hide an unbounded amount of still-present history. That is refused (unless
/// LOCUS_AOF_LOAD_TRUNCATED=yes), so the operator sees it instead of quietly
/// starting with half the data and appending after the hole.
pub fn load(path: &str) -> io::Result<(Db, Vec<CdcGroupOp>)> {
    let mut data = Vec::new();
    let mut groups = Vec::new();
    match File::open(path) {
        Ok(mut f) => {
            f.read_to_end(&mut data)?;
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok((Db::new(), groups)),
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
                    // CDCGROUP is the one logged command the keyspace knows
                    // nothing about: collect it for the hub instead of handing
                    // `execute` a command it would answer with "unknown".
                    match cdc_group_op(&tokens) {
                        Some(op) => groups.push(op),
                        None => {
                            execute(&tokens, &mut db); // replay; no re-logging here
                        }
                    }
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
    Ok((db, groups))
}

/// Serialize the whole dataset as the minimal command set that rebuilds it — the
/// base image for an AOF rewrite (BGREWRITEAOF). Pure in-memory; the disk write
/// is done off the hub thread (see `write_tmp` / `finalize_rewrite`).
///
/// `cdc_groups` is `(name, cursor)` for every live changefeed group. A rewrite
/// replaces the whole log, so without this every logged `CDCGROUP CREATE` would
/// be thrown away and group existence would stop being crash-durable the moment
/// the AOF was rewritten. Emitting the group at its *current* cursor (rather
/// than its original creation offset) also keeps the rewrite from rewinding a
/// group that a snapshot never covered.
pub fn serialize_rewrite(db: &Db, cdc_groups: &[(Vec<u8>, u64)]) -> Vec<u8> {
    let mut buf = Vec::new();
    for (key, value) in db.entries() {
        for c in reconstruct(key, value) {
            encode_command(&mut buf, &c);
        }
        if let Some(t) = db.raw_expire(key) {
            encode_command(&mut buf, &pexpireat(key, t));
        }
    }
    for (name, cursor) in cdc_groups {
        encode_command(&mut buf, &cdc_group_create(name, *cursor));
    }
    buf
}

/// The durable form of "this changefeed group exists, at this cursor" — the one
/// command shape written to the AOF and streamed to replicas for a group.
pub fn cdc_group_create(name: &[u8], start: u64) -> Vec<Vec<u8>> {
    vec![
        b"CDCGROUP".to_vec(),
        b"CREATE".to_vec(),
        name.to_vec(),
        start.to_string().into_bytes(),
    ]
}

/// The matching destroy form.
pub fn cdc_group_destroy(name: &[u8]) -> Vec<Vec<u8>> {
    vec![b"CDCGROUP".to_vec(), b"DESTROY".to_vec(), name.to_vec()]
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
        // A tiered stub: reference the LOCAL value-log entry directly. Valid
        // because segments are immutable and delete-only (never rewritten), and
        // the AOF never leaves this node. A key that dies later in the log
        // replays its death after this, so end-state stays exact.
        Value::Tiered {
            seg,
            off,
            len,
            vtag,
        } => vec![vec![
            b"TIERREF".to_vec(),
            k,
            seg.to_string().into_bytes(),
            off.to_string().into_bytes(),
            len.to_string().into_bytes(),
            vtag.to_string().into_bytes(),
        ]],
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
        Value::Hll(h) => vec![vec![b"PFLOAD".to_vec(), k, h.regs.clone()]],
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

    /// `FSYNC_FAULT` is process-global (the fsync thread has to see it), so the
    /// two tests that touch it must not run at the same time.
    fn fsync_test_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: Mutex<()> = Mutex::new(());
        LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

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

        let (mut db, _) = load(path).unwrap();
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

        let (mut db, _) = load(path).unwrap();
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
        let (mut db, _) = load(path).unwrap();
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
        let base = serialize_rewrite(&db, &[]);
        write_tmp(&tmp, &base).unwrap();
        // A write that lands during the rewrite, captured as a tail and folded in.
        let mut tail = Vec::new();
        encode_into(
            &mut tail,
            &[vec![b"SET".to_vec(), b"k2".to_vec(), b"v2".to_vec()]],
        );
        finalize_rewrite(&tmp, path, &tail).unwrap();
        // Replaying the swapped-in file yields base + tail, nothing lost.
        let (mut loaded, _) = load(path).unwrap();
        assert_eq!(run(&mut loaded, &[b"GET", b"k"]), b"$1\r\nv\r\n".to_vec());
        assert_eq!(run(&mut loaded, &[b"GET", b"k2"]), b"$2\r\nv2\r\n".to_vec());
        let _ = fs::remove_file(path);
    }

    /// 5b — a rewrite replaces the whole log, so the base image has to re-emit
    /// the changefeed groups or their existence stops being crash-durable the
    /// moment the AOF is rewritten. And the loader has to hand them back rather
    /// than feed them to `execute`, which knows nothing about groups.
    #[test]
    fn a_rewrite_carries_changefeed_groups_and_the_loader_returns_them() {
        let path = "/tmp/locus_aof_cdcgroups.aof";
        let tmp = format!("{path}.tmp");
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(&tmp);
        let mut db = Db::new();
        db.insert_with_expire(b"k".to_vec(), Value::Str(b"v".to_vec()), None);
        let base = serialize_rewrite(&db, &[(b"grp".to_vec(), 7)]);
        write_tmp(&tmp, &base).unwrap();
        // A group created DURING the rewrite lands in the folded-in tail.
        let mut tail = Vec::new();
        encode_into(&mut tail, &[cdc_group_create(b"during", 0)]);
        encode_into(&mut tail, &[cdc_group_destroy(b"grp")]);
        finalize_rewrite(&tmp, path, &tail).unwrap();

        let (mut loaded, ops) = load(path).unwrap();
        assert_eq!(run(&mut loaded, &[b"GET", b"k"]), b"$1\r\nv\r\n".to_vec());
        // In log order, cursor and all — the hub folds these in after the trailer.
        assert_eq!(
            ops,
            vec![
                CdcGroupOp::Create {
                    name: b"grp".to_vec(),
                    start: 7
                },
                CdcGroupOp::Create {
                    name: b"during".to_vec(),
                    start: 0
                },
                CdcGroupOp::Destroy {
                    name: b"grp".to_vec()
                },
            ]
        );
        let _ = fs::remove_file(path);
    }

    /// The read verbs are never logged — but if one ever were, it must not be
    /// mistaken for a group mutation on the way back in.
    #[test]
    fn only_cdcgroup_create_and_destroy_are_recognized_as_group_ops() {
        let cmd = |parts: &[&[u8]]| -> Vec<Vec<u8>> { parts.iter().map(|p| p.to_vec()).collect() };
        assert!(cdc_group_op(&cmd(&[b"CDCREADGROUP", b"grp", b"c1"])).is_none());
        assert!(cdc_group_op(&cmd(&[b"CDCACK", b"grp", b"1"])).is_none());
        assert!(cdc_group_op(&cmd(&[b"CDCCLAIM", b"grp", b"c1", b"0", b"1"])).is_none());
        assert!(cdc_group_op(&cmd(&[b"SET", b"CDCGROUP", b"x"])).is_none());
        assert!(cdc_group_op(&cmd(&[b"CDCGROUP", b"WAT", b"grp"])).is_none());
        assert!(cdc_group_op(&cmd(&[b"CDCGROUP", b"CREATE"])).is_none()); // too short
        assert!(cdc_group_op(&[]).is_none()); // and an empty frame must not index
        // Case-insensitive, as the wire is.
        assert_eq!(
            cdc_group_op(&cmd(&[b"cdcgroup", b"create", b"g", b"3"])),
            Some(CdcGroupOp::Create {
                name: b"g".to_vec(),
                start: 3
            })
        );
    }

    /// 3.4 — the `everysec` fsync must not run on the thread that asks for it.
    /// Timing this would be a race on a fast SSD; instead the worker records the
    /// thread that ran the sync, which is a fact, not a measurement.
    #[test]
    fn everysec_fsync_runs_off_the_calling_thread() {
        let path = "/tmp/locus_aof_offthread.aof";
        let _guard = fsync_test_lock();
        let _ = fs::remove_file(path);
        let mut a = Aof::open_with_policy(path, FsyncPolicy::Everysec).unwrap();
        a.append(&[vec![b"SET".to_vec(), b"k".to_vec(), b"v".to_vec()]])
            .unwrap();
        assert!(a.syncer.is_some(), "everysec must own an fsync thread");
        // Pretend a second has passed, then ask. The call must return without
        // having done the sync itself.
        a.last_fsync = now_ms().saturating_sub(2000);
        a.maybe_fsync();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while a.syncer.as_ref().unwrap().done() == 0 {
            assert!(std::time::Instant::now() < deadline, "fsync never happened");
            thread::sleep(std::time::Duration::from_millis(5));
        }
        assert_ne!(
            a.syncer.as_ref().unwrap().last_thread(),
            Some(thread::current().id()),
            "the fsync ran on the caller's thread — that is the hub stall"
        );
        assert!(a.healthy());
        // A second request inside the same second is not made at all.
        a.maybe_fsync();
        assert_eq!(a.syncer.as_ref().unwrap().done(), 1);
        drop(a); // joins the thread; a leak here would leak one per AOF rewrite
        let _ = fs::remove_file(path);
    }

    /// 3.3 — under `always`, a failed fsync must be RETURNED by the append that
    /// incurred it. Before this it latched the AOF unhealthy and returned Ok, so
    /// the write whose fsync had just failed was still acked.
    #[test]
    fn always_returns_the_fsync_error_on_the_write_that_failed() {
        let path = "/tmp/locus_aof_alwaysfail.aof";
        let _guard = fsync_test_lock();
        let _ = fs::remove_file(path);
        let mut a = Aof::open_with_policy(path, FsyncPolicy::Always).unwrap();
        assert!(a.acks_after_fsync());
        assert!(a.syncer.is_none(), "always syncs inline, not on a thread");
        a.append(&[vec![b"SET".to_vec(), b"a".to_vec(), b"1".to_vec()]])
            .expect("a healthy append is Ok");

        set_fsync_fault(true);
        let err = a
            .append(&[vec![b"SET".to_vec(), b"b".to_vec(), b"2".to_vec()]])
            .expect_err("a failed fsync under `always` must not report success");
        assert!(err.to_string().contains("injected fsync failure"), "{err}");
        assert!(!a.healthy(), "the log is latched unhealthy too");
        set_fsync_fault(false);
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
