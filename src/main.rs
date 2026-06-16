//! Locus — an in-memory, geo-first datastore that speaks the Redis protocol.
//!
//! Architecture (single-threaded execution, the Redis way):
//!   * each connection has a READER thread (read + parse) and a WRITER thread
//!     (drain an output channel to the socket);
//!   * one owner thread (the "hub") holds the keyspace, the pub/sub registry,
//!     and the replication state; it processes every command serially and routes
//!     replies, published messages, and replicated writes to clients.
//!
//! Milestones: M0 PONG · M1 RESP+SET/GET · M2 concurrency · M3 expiry ·
//! M4 lists/hashes/sets · M5 sorted sets · M6 RDB · M7 AOF · M8 pub/sub ·
//! M9 replication (full sync + command streaming).

mod aof;
mod commands;
mod db;
mod pubsub;
mod rdb;
mod resp;
mod streams;

use std::collections::{HashMap, HashSet};
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

use commands::execute;
use db::{now_ms, Db};
use pubsub::PubSub;
use resp::{parse_command, Parsed};

/// Reserved client id for commands replicated from a master.
const MASTER_ID: u64 = 0;

enum Msg {
    Connect { id: u64, out: mpsc::Sender<Vec<u8>> },
    Command { id: u64, tokens: Vec<Vec<u8>> },
    Disconnect { id: u64 },
    /// Replica received a full-sync snapshot; replace the whole dataset.
    ReplaceDb(Box<Db>),
}

fn main() -> io::Result<()> {
    let (tx, rx) = mpsc::channel::<Msg>();
    let hub_tx = tx.clone();
    thread::spawn(move || run_hub(rx, hub_tx));

    let port = std::env::var("LOCUS_PORT").unwrap_or_else(|_| "6379".to_string());
    let addr = format!("127.0.0.1:{port}");
    let listener = TcpListener::bind(&addr)?;
    println!("Locus listening on {addr}");

    let mut next_id: u64 = 1; // 0 is reserved for the master
    for stream in listener.incoming() {
        match stream {
            Ok(conn) => {
                let id = next_id;
                next_id += 1;
                let tx = tx.clone();
                thread::spawn(move || {
                    if let Err(e) = handle_conn(conn, id, tx) {
                        eprintln!("connection error: {e}");
                    }
                });
            }
            Err(e) => eprintln!("accept error: {e}"),
        }
    }
    Ok(())
}

// === the hub ================================================================

struct Hub {
    db: Db,
    aof: Option<aof::Aof>,
    aof_path: Option<String>,
    clients: HashMap<u64, mpsc::Sender<Vec<u8>>>,
    pubsub: PubSub,
    // replication
    replicas: HashSet<u64>,             // client ids receiving our write stream
    master: Option<(String, String)>,  // (host, port) if we are a replica
    replica_stop: Option<Arc<AtomicBool>>,
    tx: mpsc::Sender<Msg>,              // so we can spawn a replica sync thread
    // transactions
    txs: HashMap<u64, TxState>,
    watched_keys: HashMap<Vec<u8>, HashSet<u64>>,
    // blocking XREAD
    blocked: Vec<BlockedReader>,
}

/// A client parked on a blocking XREAD.
struct BlockedReader {
    id: u64,
    specs: Vec<(Vec<u8>, db::StreamId)>,
    count: Option<usize>,
    deadline: Option<u64>, // None = block forever
}

/// Per-client transaction state (MULTI/EXEC/WATCH).
#[derive(Default)]
struct TxState {
    in_multi: bool,
    queued: Vec<Vec<Vec<u8>>>,
    watched: Vec<Vec<u8>>,
    dirty: bool, // a watched key changed -> EXEC aborts
}

impl Hub {
    fn new(tx: mpsc::Sender<Msg>) -> Hub {
        let aof_path = aof::configured_path();
        let (db, aof) = match &aof_path {
            Some(path) => {
                let db = aof::load(path).unwrap_or_else(|e| {
                    eprintln!("AOF load failed: {e} — starting empty");
                    Db::new()
                });
                let aof = aof::Aof::open(path).map_err(|e| eprintln!("AOF open failed: {e}")).ok();
                (db, aof)
            }
            None => {
                let p = rdb::configured_path();
                let db = rdb::load(&p).unwrap_or_else(|e| {
                    eprintln!("RDB load failed: {e} — starting empty");
                    Db::new()
                });
                (db, None)
            }
        };
        Hub {
            db,
            aof,
            aof_path,
            clients: HashMap::new(),
            pubsub: PubSub::new(),
            replicas: HashSet::new(),
            master: None,
            replica_stop: None,
            tx,
            txs: HashMap::new(),
            watched_keys: HashMap::new(),
            blocked: Vec::new(),
        }
    }

    fn send(&self, id: u64, bytes: Vec<u8>) {
        if let Some(out) = self.clients.get(&id) {
            let _ = out.send(bytes);
        }
    }

    fn handle_command(&mut self, id: u64, tokens: Vec<Vec<u8>>) {
        if tokens.is_empty() {
            return;
        }
        let cmd = tokens[0].to_ascii_uppercase();

        if self.pubsub.total(id) > 0 && !allowed_in_subscribe_mode(&cmd) {
            return self.send(
                id,
                resp::error(&format!(
                    "ERR Can't execute '{}': only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
                    String::from_utf8_lossy(&cmd).to_ascii_lowercase()
                )),
            );
        }

        // In MULTI, queue everything except transaction-control commands.
        if self.txs.get(&id).map_or(false, |t| t.in_multi) && !is_tx_control(&cmd) {
            self.txs.get_mut(&id).unwrap().queued.push(tokens);
            return self.send(id, resp::simple_string("QUEUED"));
        }

        match cmd.as_slice() {
            // --- transactions ---
            b"MULTI" => {
                let tx = self.txs.entry(id).or_default();
                if tx.in_multi {
                    return self.send(id, resp::error("ERR MULTI calls can not be nested"));
                }
                tx.in_multi = true;
                self.send(id, resp::simple_string("OK"));
            }
            b"DISCARD" => {
                if !self.txs.get(&id).map_or(false, |t| t.in_multi) {
                    return self.send(id, resp::error("ERR DISCARD without MULTI"));
                }
                let t = self.txs.remove(&id).unwrap();
                self.unwatch_keys(&t.watched, id);
                self.send(id, resp::simple_string("OK"));
            }
            b"EXEC" => {
                if !self.txs.get(&id).map_or(false, |t| t.in_multi) {
                    return self.send(id, resp::error("ERR EXEC without MULTI"));
                }
                let t = self.txs.remove(&id).unwrap();
                self.unwatch_keys(&t.watched, id);
                if t.dirty {
                    self.send(id, resp::null_array()); // a watched key changed -> abort
                } else {
                    let mut replies = Vec::with_capacity(t.queued.len());
                    for q in t.queued {
                        replies.push(self.exec_one(id, q));
                    }
                    self.send(id, resp::array(&replies));
                }
            }
            b"WATCH" => {
                if tokens.len() < 2 {
                    return self.send(id, resp::error("ERR wrong number of arguments for 'watch' command"));
                }
                if self.txs.get(&id).map_or(false, |t| t.in_multi) {
                    return self.send(id, resp::error("ERR WATCH inside MULTI is not allowed"));
                }
                {
                    let tx = self.txs.entry(id).or_default();
                    for key in &tokens[1..] {
                        tx.watched.push(key.clone());
                    }
                }
                for key in &tokens[1..] {
                    self.watched_keys.entry(key.clone()).or_default().insert(id);
                }
                self.send(id, resp::simple_string("OK"));
            }
            b"UNWATCH" => {
                let watched: Vec<Vec<u8>> = self
                    .txs
                    .get_mut(&id)
                    .map(|t| {
                        t.dirty = false;
                        std::mem::take(&mut t.watched)
                    })
                    .unwrap_or_default();
                self.unwatch_keys(&watched, id);
                self.send(id, resp::simple_string("OK"));
            }
            // --- pub/sub ---
            b"SUBSCRIBE" => {
                if tokens.len() < 2 {
                    return self.send(id, resp::error("ERR wrong number of arguments for 'subscribe' command"));
                }
                for ch in &tokens[1..] {
                    let c = self.pubsub.subscribe(id, ch);
                    self.send(id, pubsub::subscribe_reply(ch, c));
                }
            }
            b"PSUBSCRIBE" => {
                if tokens.len() < 2 {
                    return self.send(id, resp::error("ERR wrong number of arguments for 'psubscribe' command"));
                }
                for pat in &tokens[1..] {
                    let c = self.pubsub.psubscribe(id, pat);
                    self.send(id, pubsub::psubscribe_reply(pat, c));
                }
            }
            b"UNSUBSCRIBE" => {
                let chans = if tokens.len() > 1 { tokens[1..].to_vec() } else { self.pubsub.channels_of(id) };
                if chans.is_empty() {
                    self.send(id, pubsub::unsubscribe_reply(None, 0));
                } else {
                    for ch in chans {
                        let c = self.pubsub.unsubscribe(id, &ch);
                        self.send(id, pubsub::unsubscribe_reply(Some(&ch), c));
                    }
                }
            }
            b"PUNSUBSCRIBE" => {
                let pats = if tokens.len() > 1 { tokens[1..].to_vec() } else { self.pubsub.patterns_of(id) };
                if pats.is_empty() {
                    self.send(id, pubsub::punsubscribe_reply(None, 0));
                } else {
                    for pat in pats {
                        let c = self.pubsub.punsubscribe(id, &pat);
                        self.send(id, pubsub::punsubscribe_reply(Some(&pat), c));
                    }
                }
            }
            b"PUBLISH" => {
                if tokens.len() != 3 {
                    return self.send(id, resp::error("ERR wrong number of arguments for 'publish' command"));
                }
                let n = self.pubsub.publish(&tokens[1], &tokens[2], &self.clients);
                self.send(id, resp::integer(n));
            }
            b"PUBSUB" => self.handle_pubsub_introspect(id, &tokens),
            b"XREAD" => self.handle_xread(id, &tokens),

            // --- replication ---
            b"REPLCONF" => self.send(id, resp::simple_string("OK")),
            b"PSYNC" | b"SYNC" => {
                self.send(id, b"+FULLRESYNC 0000000000000000000000000000000000000000 0\r\n".to_vec());
                let snap = rdb::serialize(&self.db);
                let mut bulk = format!("${}\r\n", snap.len()).into_bytes();
                bulk.extend_from_slice(&snap);
                self.send(id, bulk);
                self.replicas.insert(id);
                println!("replication: replica {id} attached ({} byte snapshot)", snap.len());
            }
            b"REPLICAOF" | b"SLAVEOF" => self.handle_replicaof(id, &tokens),
            b"INFO" => {
                let role = if self.master.is_some() { "slave" } else { "master" };
                let mut s = format!("# Replication\r\nrole:{role}\r\nconnected_slaves:{}\r\n", self.replicas.len());
                if let Some((h, p)) = &self.master {
                    s.push_str(&format!("master_host:{h}\r\nmaster_port:{p}\r\nmaster_link_status:up\r\n"));
                }
                self.send(id, resp::bulk_string(s.as_bytes()));
            }

            // --- persistence (owner-side) ---
            b"BGREWRITEAOF" => {
                let reply = match (&self.aof_path, self.aof.is_some()) {
                    (Some(path), true) => match aof::rewrite(&self.db, path) {
                        Ok(()) => {
                            self.aof = aof::Aof::open(path).ok();
                            resp::simple_string("Background append only file rewriting started")
                        }
                        Err(e) => resp::error(&format!("ERR {e}")),
                    },
                    _ => resp::error("ERR AOF is not enabled"),
                };
                self.send(id, reply);
            }

            // --- everything else (data commands) ---
            _ => {
                let is_xadd = cmd.as_slice() == b"XADD";
                let reply = self.exec_one(id, tokens);
                self.send(id, reply);
                if is_xadd {
                    self.serve_blocked();
                }
            }
        }
    }

    fn handle_xread(&mut self, id: u64, tokens: &[Vec<u8>]) {
        let req = match streams::parse_xread(tokens) {
            Ok(r) => r,
            Err(e) => return self.send(id, resp::error(&format!("ERR {e}"))),
        };
        let specs = streams::resolve_specs(&mut self.db, &req);
        if let Some(reply) = streams::xread_collect(&mut self.db, &specs, req.count) {
            return self.send(id, reply);
        }
        match req.block {
            Some(ms) => {
                let deadline = if ms == 0 { None } else { Some(now_ms() + ms) };
                self.blocked.push(BlockedReader { id, specs, count: req.count, deadline });
            }
            None => self.send(id, streams::nil()),
        }
    }

    /// After a stream gains entries, satisfy any parked readers that can now read.
    fn serve_blocked(&mut self) {
        let mut i = 0;
        while i < self.blocked.len() {
            let specs = self.blocked[i].specs.clone();
            let count = self.blocked[i].count;
            let rid = self.blocked[i].id;
            if let Some(reply) = streams::xread_collect(&mut self.db, &specs, count) {
                self.blocked.remove(i);
                self.send(rid, reply);
            } else {
                i += 1;
            }
        }
    }

    /// Time out parked readers whose BLOCK deadline has passed (reply nil).
    fn expire_blocked(&mut self) {
        let now = now_ms();
        let mut i = 0;
        while i < self.blocked.len() {
            match self.blocked[i].deadline {
                Some(d) if d <= now => {
                    let rid = self.blocked[i].id;
                    self.blocked.remove(i);
                    self.send(rid, streams::nil());
                }
                _ => i += 1,
            }
        }
    }

    /// Execute one data command: read-only check, run it, log to AOF, propagate
    /// to replicas, and mark any watching transactions dirty. Returns the reply.
    fn exec_one(&mut self, id: u64, tokens: Vec<Vec<u8>>) -> Vec<u8> {
        let cmd = tokens[0].to_ascii_uppercase();
        if self.master.is_some() && id != MASTER_ID && aof::is_write(&cmd) {
            return resp::error("READONLY You can't write against a read only replica.");
        }
        let reply = execute(&tokens, &mut self.db);
        let errored = reply.first() == Some(&b'-');
        if !errored && aof::is_write(&cmd) {
            // WATCH: any modification of a watched key dirties its transaction.
            for key in write_keys(&tokens) {
                if let Some(watchers) = self.watched_keys.get(key) {
                    let ids: Vec<u64> = watchers.iter().copied().collect();
                    for wid in ids {
                        if let Some(tx) = self.txs.get_mut(&wid) {
                            tx.dirty = true;
                        }
                    }
                }
            }
            let entries = aof::entries_for(&tokens, &reply, &mut self.db);
            if let Some(a) = self.aof.as_mut() {
                let _ = a.append(&entries);
            }
            // Propagate the deterministic form to every replica.
            if !self.replicas.is_empty() {
                for e in &entries {
                    let bytes = resp::command(e);
                    for rid in self.replicas.iter() {
                        if let Some(out) = self.clients.get(rid) {
                            let _ = out.send(bytes.clone());
                        }
                    }
                }
            }
        }
        if let Some(a) = self.aof.as_mut() {
            a.maybe_fsync();
        }
        reply
    }

    fn unwatch_keys(&mut self, watched: &[Vec<u8>], id: u64) {
        for key in watched {
            if let Some(set) = self.watched_keys.get_mut(key) {
                set.remove(&id);
                if set.is_empty() {
                    self.watched_keys.remove(key);
                }
            }
        }
    }

    fn handle_replicaof(&mut self, id: u64, tokens: &[Vec<u8>]) {
        if tokens.len() != 3 {
            return self.send(id, resp::error("ERR wrong number of arguments for 'replicaof' command"));
        }
        // Stop any existing replication link first.
        if let Some(flag) = self.replica_stop.take() {
            flag.store(true, Ordering::Relaxed);
        }
        if tokens[1].eq_ignore_ascii_case(b"NO") && tokens[2].eq_ignore_ascii_case(b"ONE") {
            self.master = None;
            println!("replication: promoted to master");
            return self.send(id, resp::simple_string("OK"));
        }
        let host = String::from_utf8_lossy(&tokens[1]).to_string();
        let port = String::from_utf8_lossy(&tokens[2]).to_string();
        self.master = Some((host.clone(), port.clone()));
        let stop = Arc::new(AtomicBool::new(false));
        self.replica_stop = Some(stop.clone());
        let addr = format!("{host}:{port}");
        let txc = self.tx.clone();
        thread::spawn(move || replica_sync(addr, txc, stop));
        println!("replication: now replicating from {host}:{port}");
        self.send(id, resp::simple_string("OK"));
    }

    fn handle_pubsub_introspect(&self, id: u64, tokens: &[Vec<u8>]) {
        let sub = tokens.get(1).map(|t| t.to_ascii_uppercase());
        let reply = match sub.as_deref() {
            Some(b"CHANNELS") => {
                let pat = tokens.get(2);
                let chans: Vec<Vec<u8>> = self
                    .pubsub
                    .active_channels()
                    .into_iter()
                    .filter(|c| pat.map_or(true, |p| pubsub::glob_match(p, c)))
                    .collect();
                resp::bulk_array(&chans)
            }
            Some(b"NUMSUB") => {
                let mut out = Vec::new();
                for ch in &tokens[2..] {
                    out.push(resp::bulk_string(ch));
                    out.push(resp::integer(self.pubsub.numsub(ch)));
                }
                resp::array(&out)
            }
            Some(b"NUMPAT") => resp::integer(self.pubsub.numpat()),
            _ => resp::error("ERR Unknown PUBSUB subcommand"),
        };
        self.send(id, reply);
    }
}

fn allowed_in_subscribe_mode(cmd: &[u8]) -> bool {
    matches!(
        cmd,
        b"SUBSCRIBE" | b"UNSUBSCRIBE" | b"PSUBSCRIBE" | b"PUNSUBSCRIBE" | b"PING" | b"QUIT" | b"RESET"
    )
}

fn is_tx_control(cmd: &[u8]) -> bool {
    matches!(cmd, b"MULTI" | b"EXEC" | b"DISCARD" | b"WATCH" | b"UNWATCH" | b"RESET")
}

/// Keys a write command modifies (for WATCH dirtying): all args for DEL,
/// otherwise the single key at position 1.
fn write_keys(tokens: &[Vec<u8>]) -> Vec<&[u8]> {
    match tokens[0].to_ascii_uppercase().as_slice() {
        b"DEL" => tokens[1..].iter().map(|k| k.as_slice()).collect(),
        _ => tokens.get(1).map(|k| vec![k.as_slice()]).unwrap_or_default(),
    }
}

fn run_hub(rx: mpsc::Receiver<Msg>, tx: mpsc::Sender<Msg>) {
    let mut hub = Hub::new(tx);
    loop {
        match rx.recv_timeout(Duration::from_millis(100)) {
            Ok(Msg::Connect { id, out }) => {
                hub.clients.insert(id, out);
            }
            Ok(Msg::Disconnect { id }) => {
                hub.clients.remove(&id);
                hub.pubsub.remove_client(id);
                hub.replicas.remove(&id);
                if let Some(t) = hub.txs.remove(&id) {
                    hub.unwatch_keys(&t.watched, id);
                }
                hub.blocked.retain(|r| r.id != id);
            }
            Ok(Msg::Command { id, tokens }) => hub.handle_command(id, tokens),
            Ok(Msg::ReplaceDb(db)) => {
                hub.db = *db;
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if let Some(a) = hub.aof.as_mut() {
                    a.maybe_fsync();
                }
                hub.db.active_expire();
                hub.expire_blocked();
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
}

// === replica side: connect to a master and apply its stream =================

fn replica_sync(addr: String, hub_tx: mpsc::Sender<Msg>, stop: Arc<AtomicBool>) {
    while !stop.load(Ordering::Relaxed) {
        if let Err(e) = try_sync(&addr, &hub_tx, &stop) {
            eprintln!("replication: link to {addr} dropped: {e}");
        }
        // Reconnect after a short delay (a real impl would PSYNC partial-resync).
        for _ in 0..10 {
            if stop.load(Ordering::Relaxed) {
                return;
            }
            thread::sleep(Duration::from_millis(100));
        }
    }
}

fn try_sync(addr: &str, hub_tx: &mpsc::Sender<Msg>, stop: &Arc<AtomicBool>) -> io::Result<()> {
    let mut stream = TcpStream::connect(addr)?;
    // Handshake: PING -> REPLCONF -> PSYNC.
    send_cmd(&mut stream, &[b"PING"])?;
    read_line(&mut stream)?;
    let myport = std::env::var("LOCUS_PORT").unwrap_or_else(|_| "6379".into());
    send_cmd(&mut stream, &[b"REPLCONF", b"listening-port", myport.as_bytes()])?;
    read_line(&mut stream)?;
    send_cmd(&mut stream, &[b"PSYNC", b"?", b"-1"])?;
    read_line(&mut stream)?; // +FULLRESYNC <id> <offset>

    // Full-sync snapshot: $<len>\r\n<bytes>
    let len = read_bulk_header(&mut stream)?;
    let mut snap = vec![0u8; len];
    stream.read_exact(&mut snap)?;
    let db = rdb::deserialize(&snap)?;
    if hub_tx.send(Msg::ReplaceDb(Box::new(db))).is_err() {
        return Ok(());
    }
    println!("replication: full sync complete ({len} bytes)");

    // Stream and apply the master's writes. The read timeout lets us notice a
    // stop request even when the master is idle.
    stream.set_read_timeout(Some(Duration::from_millis(200)))?;
    let mut inbuf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        if stop.load(Ordering::Relaxed) {
            return Ok(());
        }
        match stream.read(&mut chunk) {
            Ok(0) => return Ok(()),
            Ok(n) => {
                inbuf.extend_from_slice(&chunk[..n]);
                loop {
                    match parse_command(&inbuf) {
                        Parsed::Complete(tokens, consumed) => {
                            inbuf.drain(0..consumed);
                            if !tokens.is_empty()
                                && hub_tx.send(Msg::Command { id: MASTER_ID, tokens }).is_err()
                            {
                                return Ok(());
                            }
                        }
                        Parsed::Incomplete => break,
                        Parsed::Error(_) => return Ok(()),
                    }
                }
            }
            Err(e) if matches!(e.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut) => {}
            Err(e) => return Err(e),
        }
    }
}

fn send_cmd(s: &mut TcpStream, parts: &[&[u8]]) -> io::Result<()> {
    let owned: Vec<Vec<u8>> = parts.iter().map(|p| p.to_vec()).collect();
    s.write_all(&resp::command(&owned))
}

fn read_line(s: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut line = Vec::new();
    let mut b = [0u8; 1];
    loop {
        s.read_exact(&mut b)?;
        if b[0] == b'\n' {
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            break;
        }
        line.push(b[0]);
    }
    Ok(line)
}

fn read_bulk_header(s: &mut TcpStream) -> io::Result<usize> {
    let line = read_line(s)?;
    if line.first() != Some(&b'$') {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "expected bulk header"));
    }
    std::str::from_utf8(&line[1..])
        .ok()
        .and_then(|x| x.parse::<usize>().ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad bulk length"))
}

// === per-connection: reader thread (here) + writer thread (spawned) =========

fn handle_conn(conn: TcpStream, id: u64, tx: mpsc::Sender<Msg>) -> io::Result<()> {
    let peer = conn.peer_addr()?;
    println!("client connected: {peer}");

    let mut write_half = conn.try_clone()?;
    let (out_tx, out_rx) = mpsc::channel::<Vec<u8>>();
    let writer = thread::spawn(move || {
        while let Ok(bytes) = out_rx.recv() {
            if write_half.write_all(&bytes).is_err() {
                break;
            }
        }
    });

    if tx.send(Msg::Connect { id, out: out_tx.clone() }).is_err() {
        return Ok(());
    }

    let mut conn = conn;
    let mut inbuf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 4096];
    'read: loop {
        let n = match conn.read(&mut chunk) {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        inbuf.extend_from_slice(&chunk[..n]);
        loop {
            match parse_command(&inbuf) {
                Parsed::Incomplete => break,
                Parsed::Error(msg) => {
                    let _ = out_tx.send(resp::error(&format!("ERR Protocol error: {msg}")));
                    break 'read;
                }
                Parsed::Complete(tokens, consumed) => {
                    inbuf.drain(0..consumed);
                    if tx.send(Msg::Command { id, tokens }).is_err() {
                        break 'read;
                    }
                }
            }
        }
    }

    let _ = tx.send(Msg::Disconnect { id });
    drop(out_tx);
    let _ = writer.join();
    println!("client disconnected: {peer}");
    Ok(())
}
