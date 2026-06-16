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

use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;

use commands::execute;
use db::{Db, Value, now_ms};
use pubsub::PubSub;
use resp::{Parsed, parse_command};

/// Reserved client id for commands replicated from a master.
const MASTER_ID: u64 = 0;

enum Msg {
    Connect {
        id: u64,
        out: mpsc::Sender<Vec<u8>>,
    },
    Command {
        id: u64,
        tokens: Vec<Vec<u8>>,
    },
    Disconnect {
        id: u64,
    },
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
    replicas: HashSet<u64>,           // client ids receiving our write stream
    master: Option<(String, String)>, // (host, port) if we are a replica
    replica_stop: Option<Arc<AtomicBool>>,
    tx: mpsc::Sender<Msg>, // so we can spawn a replica sync thread
    // transactions
    txs: HashMap<u64, TxState>,
    watched_keys: HashMap<Vec<u8>, HashSet<u64>>,
    // blocking XREAD
    blocked: Vec<BlockedReader>,
    // negotiated RESP version per client (2 or 3)
    protos: HashMap<u64, u8>,
    // soft memory cap (bytes); None = unlimited
    maxmemory: Option<usize>,
    // changefeed subscribers: client id -> key prefix (empty = all keys)
    changefeeds: HashMap<u64, Vec<u8>>,
    // retained change-log (for CDCREAD catch-up); empty/unused when maxlen == 0
    cdc_log: VecDeque<ChangeRecord>,
    cdc_next_offset: u64,
    cdc_maxlen: usize,
}

/// One retained keyspace change, addressable by a monotonic offset.
struct ChangeRecord {
    offset: u64,
    event: Vec<u8>, // "write" | "del" | "expire"
    key: Vec<u8>,
    value: Option<Vec<u8>>, // new value for string writes; None otherwise
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
    dirty: bool,   // a watched key changed -> EXEC aborts (nil)
    aborted: bool, // a queued command was invalid -> EXEC aborts (EXECABORT)
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
                let aof = aof::Aof::open(path)
                    .map_err(|e| eprintln!("AOF open failed: {e}"))
                    .ok();
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
            protos: HashMap::new(),
            maxmemory: std::env::var("LOCUS_MAXMEMORY")
                .ok()
                .and_then(|s| parse_mem(&s))
                .filter(|&m| m > 0),
            changefeeds: HashMap::new(),
            cdc_log: VecDeque::new(),
            cdc_next_offset: 1, // offset 0 means "nothing yet"
            cdc_maxlen: std::env::var("LOCUS_CDC_MAXLEN")
                .ok()
                .and_then(|s| s.trim().parse::<usize>().ok())
                .unwrap_or(0),
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

        // A connection in pub/sub or changefeed "push mode" may only run the
        // push-control commands (plus PING/QUIT/RESET) — replies must not
        // interleave with pushed messages.
        let in_push_mode = self.pubsub.total(id) > 0 || self.changefeeds.contains_key(&id);
        if in_push_mode && !allowed_in_push_mode(&cmd) {
            return self.send(
                id,
                resp::error(&format!(
                    "ERR Can't execute '{}': only (P)SUBSCRIBE / (P)UNSUBSCRIBE / CDC(UN)SUBSCRIBE / PING / QUIT / RESET are allowed in this context",
                    String::from_utf8_lossy(&cmd).to_ascii_lowercase()
                )),
            );
        }

        // In MULTI, queue everything except transaction-control commands.
        // Validate at queue time (Redis semantics): an unknown command or one
        // with too few arguments is rejected now and flags the transaction so
        // EXEC fails with EXECABORT instead of running a half-valid batch.
        if self.txs.get(&id).is_some_and(|t| t.in_multi) && !is_tx_control(&cmd) {
            match commands::min_arity(&cmd) {
                None => {
                    self.txs.get_mut(&id).unwrap().aborted = true;
                    return self.send(
                        id,
                        resp::error(&format!(
                            "ERR unknown command '{}'",
                            String::from_utf8_lossy(&tokens[0])
                        )),
                    );
                }
                Some(min) if tokens.len() < min => {
                    self.txs.get_mut(&id).unwrap().aborted = true;
                    return self.send(
                        id,
                        resp::error(&format!(
                            "ERR wrong number of arguments for '{}' command",
                            String::from_utf8_lossy(&cmd).to_ascii_lowercase()
                        )),
                    );
                }
                _ => {}
            }
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
                if !self.txs.get(&id).is_some_and(|t| t.in_multi) {
                    return self.send(id, resp::error("ERR DISCARD without MULTI"));
                }
                let t = self.txs.remove(&id).unwrap();
                self.unwatch_keys(&t.watched, id);
                self.send(id, resp::simple_string("OK"));
            }
            b"EXEC" => {
                if !self.txs.get(&id).is_some_and(|t| t.in_multi) {
                    return self.send(id, resp::error("ERR EXEC without MULTI"));
                }
                let t = self.txs.remove(&id).unwrap();
                self.unwatch_keys(&t.watched, id);
                if t.aborted {
                    self.send(
                        id,
                        resp::error("EXECABORT Transaction discarded because of previous errors."),
                    );
                } else if t.dirty {
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
                    return self.send(
                        id,
                        resp::error("ERR wrong number of arguments for 'watch' command"),
                    );
                }
                if self.txs.get(&id).is_some_and(|t| t.in_multi) {
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
            b"RESET" => {
                // Abort any transaction and release its watches, exit subscribe
                // mode, and drop back to RESP2 — a clean per-connection reset.
                if let Some(t) = self.txs.remove(&id) {
                    self.unwatch_keys(&t.watched, id);
                }
                self.pubsub.remove_client(id);
                self.changefeeds.remove(&id);
                self.protos.insert(id, 2);
                self.send(id, resp::simple_string("RESET"));
            }
            // --- pub/sub ---
            b"SUBSCRIBE" => {
                if tokens.len() < 2 {
                    return self.send(
                        id,
                        resp::error("ERR wrong number of arguments for 'subscribe' command"),
                    );
                }
                for ch in &tokens[1..] {
                    let c = self.pubsub.subscribe(id, ch);
                    self.send(id, pubsub::subscribe_reply(ch, c));
                }
            }
            b"PSUBSCRIBE" => {
                if tokens.len() < 2 {
                    return self.send(
                        id,
                        resp::error("ERR wrong number of arguments for 'psubscribe' command"),
                    );
                }
                for pat in &tokens[1..] {
                    let c = self.pubsub.psubscribe(id, pat);
                    self.send(id, pubsub::psubscribe_reply(pat, c));
                }
            }
            b"UNSUBSCRIBE" => {
                let chans = if tokens.len() > 1 {
                    tokens[1..].to_vec()
                } else {
                    self.pubsub.channels_of(id)
                };
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
                let pats = if tokens.len() > 1 {
                    tokens[1..].to_vec()
                } else {
                    self.pubsub.patterns_of(id)
                };
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
                    return self.send(
                        id,
                        resp::error("ERR wrong number of arguments for 'publish' command"),
                    );
                }
                let n = self.pubsub.publish(&tokens[1], &tokens[2], &self.clients);
                self.send(id, resp::integer(n));
            }
            b"PUBSUB" => self.handle_pubsub_introspect(id, &tokens),
            b"CDCSUBSCRIBE" => self.handle_cdc_subscribe(id, &tokens),
            b"CDCUNSUBSCRIBE" => self.handle_cdc_unsubscribe(id),
            b"CDCREAD" => self.handle_cdc_read(id, &tokens),
            b"XREAD" => self.handle_xread(id, &tokens),
            b"HELLO" => self.handle_hello(id, &tokens),

            // --- replication ---
            b"REPLCONF" => self.send(id, resp::simple_string("OK")),
            b"PSYNC" | b"SYNC" => {
                self.send(
                    id,
                    b"+FULLRESYNC 0000000000000000000000000000000000000000 0\r\n".to_vec(),
                );
                let snap = rdb::serialize(&self.db);
                let mut bulk = format!("${}\r\n", snap.len()).into_bytes();
                bulk.extend_from_slice(&snap);
                self.send(id, bulk);
                self.replicas.insert(id);
                println!(
                    "replication: replica {id} attached ({} byte snapshot)",
                    snap.len()
                );
            }
            b"REPLICAOF" | b"SLAVEOF" => self.handle_replicaof(id, &tokens),
            b"INFO" => {
                let role = if self.master.is_some() {
                    "slave"
                } else {
                    "master"
                };
                let mut s = format!(
                    "# Memory\r\nused_memory:{}\r\nmaxmemory:{}\r\n# Replication\r\nrole:{role}\r\nconnected_slaves:{}\r\n",
                    self.db.mem_used(),
                    self.maxmemory.unwrap_or(0),
                    self.replicas.len()
                );
                if let Some((h, p)) = &self.master {
                    s.push_str(&format!(
                        "master_host:{h}\r\nmaster_port:{p}\r\nmaster_link_status:up\r\n"
                    ));
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
        let collected = streams::xread_collect(&mut self.db, &specs, req.count);
        self.dirty_expired_watchers();
        if let Some(reply) = collected {
            return self.send(id, reply);
        }
        match req.block {
            Some(ms) => {
                let deadline = if ms == 0 { None } else { Some(now_ms() + ms) };
                self.blocked.push(BlockedReader {
                    id,
                    specs,
                    count: req.count,
                    deadline,
                });
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

    /// CDCSUBSCRIBE [prefix] — enter changefeed push mode: send an atomic
    /// snapshot of the matching keyspace, then live-stream every matching change.
    /// Snapshot + registration happen in this single hub turn, so no change can
    /// slip between them (no gap, no dup) — that's the single-thread guarantee.
    fn handle_cdc_subscribe(&mut self, id: u64, tokens: &[Vec<u8>]) {
        let prefix = tokens.get(1).cloned().unwrap_or_default();
        self.changefeeds.insert(id, prefix.clone());
        let keys: Vec<Vec<u8>> = self
            .db
            .live_keys()
            .into_iter()
            .filter(|k| k.starts_with(&prefix))
            .collect();
        let n = keys.len();
        for k in keys {
            let val = match self.db.get(&k) {
                Some(Value::Str(s)) => resp::bulk_string(s),
                _ => resp::null_bulk(), // non-string: change-only, client re-fetches
            };
            self.send(
                id,
                resp::array(&[
                    resp::bulk_string(b"cdc-snapshot"),
                    resp::bulk_string(&k),
                    val,
                ]),
            );
        }
        // The high-water offset tells the subscriber where live changes resume
        // from, so it can CDCREAD that offset to catch up after a disconnect.
        let hwm = self.cdc_next_offset.saturating_sub(1);
        self.send(
            id,
            resp::array(&[
                resp::bulk_string(b"cdc-snapshot-done"),
                resp::integer(n as i64),
                resp::integer(hwm as i64),
            ]),
        );
    }

    fn handle_cdc_unsubscribe(&mut self, id: u64) {
        self.changefeeds.remove(&id);
        self.send(id, resp::array(&[resp::bulk_string(b"cdc-unsubscribe")]));
    }

    /// Record one keyspace change: assign an offset, push it live to matching
    /// changefeed subscribers (with the offset), and retain it in the ring for
    /// CDCREAD catch-up (when retention is enabled). No-op when there are neither
    /// subscribers nor retention, so it costs nothing unless the feature is used.
    fn record_change(&mut self, event: &[u8], key: &[u8], value: Option<Vec<u8>>) {
        if self.changefeeds.is_empty() && self.cdc_maxlen == 0 {
            return;
        }
        let offset = self.cdc_next_offset;
        self.cdc_next_offset += 1;
        if !self.changefeeds.is_empty() {
            let val = match &value {
                Some(v) => resp::bulk_string(v),
                None => resp::null_bulk(),
            };
            let msg = resp::array(&[
                resp::bulk_string(b"cdc-change"),
                resp::integer(offset as i64),
                resp::bulk_string(event),
                resp::bulk_string(key),
                val,
            ]);
            for (cid, prefix) in &self.changefeeds {
                if key.starts_with(prefix) {
                    self.send(*cid, msg.clone());
                }
            }
        }
        if self.cdc_maxlen > 0 {
            self.cdc_log.push_back(ChangeRecord {
                offset,
                event: event.to_vec(),
                key: key.to_vec(),
                value,
            });
            while self.cdc_log.len() > self.cdc_maxlen {
                self.cdc_log.pop_front();
            }
        }
    }

    /// CDCREAD <offset> [COUNT n] [PREFIX p] — pull retained changes after an
    /// offset (for catch-up after a disconnect). Non-blocking.
    fn handle_cdc_read(&mut self, id: u64, tokens: &[Vec<u8>]) {
        if tokens.len() < 2 {
            return self.send(
                id,
                resp::error("ERR wrong number of arguments for 'cdcread' command"),
            );
        }
        if self.cdc_maxlen == 0 {
            return self.send(
                id,
                resp::error("ERR changefeed retention disabled (set LOCUS_CDC_MAXLEN)"),
            );
        }
        let from = match std::str::from_utf8(&tokens[1])
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
        {
            Some(o) => o,
            None => {
                return self.send(
                    id,
                    resp::error("ERR value is not an integer or out of range"),
                );
            }
        };
        let mut count: Option<usize> = None;
        let mut prefix: Vec<u8> = Vec::new();
        let mut i = 2;
        while i < tokens.len() {
            match tokens[i].to_ascii_uppercase().as_slice() {
                b"COUNT" => {
                    i += 1;
                    count = match tokens
                        .get(i)
                        .and_then(|t| std::str::from_utf8(t).ok())
                        .and_then(|s| s.parse::<usize>().ok())
                    {
                        Some(c) => Some(c),
                        None => {
                            return self.send(
                                id,
                                resp::error("ERR value is not an integer or out of range"),
                            );
                        }
                    };
                }
                b"PREFIX" => {
                    i += 1;
                    prefix = match tokens.get(i) {
                        Some(p) => p.clone(),
                        None => return self.send(id, resp::error("ERR syntax error")),
                    };
                }
                _ => return self.send(id, resp::error("ERR syntax error")),
            }
            i += 1;
        }
        // If the oldest retained record is newer than from+1, records were
        // dropped — the consumer fell behind and must re-snapshot.
        if let Some(front) = self.cdc_log.front()
            && front.offset > from.saturating_add(1)
        {
            return self.send(
                id,
                resp::error("ERR offset out of range (changefeed history truncated)"),
            );
        }
        let mut out: Vec<Vec<u8>> = Vec::new();
        for rec in &self.cdc_log {
            if rec.offset > from && rec.key.starts_with(&prefix) {
                let val = match &rec.value {
                    Some(v) => resp::bulk_string(v),
                    None => resp::null_bulk(),
                };
                out.push(resp::array(&[
                    resp::integer(rec.offset as i64),
                    resp::bulk_string(&rec.event),
                    resp::bulk_string(&rec.key),
                    val,
                ]));
                if count.is_some_and(|c| c > 0 && out.len() >= c) {
                    break;
                }
            }
        }
        self.send(id, resp::array(&out));
    }

    /// Emit changefeed events for the keys a write touched (called from exec_one
    /// after a confirmed modification). `Del` if the key is now gone, else
    /// `Write` (with the new value for strings).
    fn emit_write_changes(&mut self, tokens: &[Vec<u8>]) {
        if self.changefeeds.is_empty() && self.cdc_maxlen == 0 {
            return;
        }
        let keys: Vec<Vec<u8>> = write_keys(tokens).iter().map(|k| k.to_vec()).collect();
        for key in keys {
            // Read the post-command state, then record (no db borrow held across send).
            let (event, value): (&[u8], Option<Vec<u8>>) = match self.db.get(&key) {
                Some(Value::Str(s)) => (b"write", Some(s.clone())),
                Some(_) => (b"write", None), // non-string: change-only, client re-fetches
                None => (b"del", None),
            };
            self.record_change(event, &key, value);
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

    /// RESP3 negotiation. Accepts HELLO [2|3]; replies with server info as a
    /// RESP3 map (proto 3) or RESP2 flat array (proto 2). Most reply types are
    /// identical across RESP2/RESP3, so we track the version but keep the
    /// existing encoders (full RESP3 typing of every reply is a later extension).
    fn handle_hello(&mut self, id: u64, tokens: &[Vec<u8>]) {
        let mut proto = 2u8;
        if let Some(v) = tokens.get(1) {
            match std::str::from_utf8(v)
                .ok()
                .and_then(|s| s.parse::<u8>().ok())
            {
                Some(2) => proto = 2,
                Some(3) => proto = 3,
                _ => return self.send(id, resp::error("NOPROTO unsupported protocol version")),
            }
        }
        self.protos.insert(id, proto);
        let role = if self.master.is_some() {
            "replica"
        } else {
            "master"
        };
        let fields: Vec<(&[u8], Vec<u8>)> = vec![
            (b"server", b"locus".to_vec()),
            (b"version", b"0.1.0".to_vec()),
            (b"proto", proto.to_string().into_bytes()),
            (b"id", id.to_string().into_bytes()),
            (b"mode", b"standalone".to_vec()),
            (b"role", role.as_bytes().to_vec()),
        ];
        let reply = if proto == 3 {
            let mut o = format!("%{}\r\n", fields.len()).into_bytes();
            for (k, v) in &fields {
                o.extend_from_slice(&resp::bulk_string(k));
                o.extend_from_slice(&resp::bulk_string(v));
            }
            o
        } else {
            let mut flat = Vec::new();
            for (k, v) in &fields {
                flat.push(k.to_vec());
                flat.push(v.clone());
            }
            resp::bulk_array(&flat)
        };
        self.send(id, reply);
    }

    /// Execute one data command: read-only check, run it, log to AOF, propagate
    /// to replicas, and mark any watching transactions dirty. Returns the reply.
    fn exec_one(&mut self, id: u64, tokens: Vec<Vec<u8>>) -> Vec<u8> {
        let cmd = tokens[0].to_ascii_uppercase();
        let is_write = aof::is_write(&cmd);
        if self.master.is_some() && id != MASTER_ID && is_write {
            return resp::error("READONLY You can't write against a read only replica.");
        }
        // maxmemory: on a master/standalone, free memory before a write; if the
        // cap still can't be met, reject the write instead of growing unbounded.
        // (Replicas don't evict on their own — the master streams the DELs.)
        if is_write && self.master.is_none() && !self.evict_if_needed() {
            return resp::error("OOM command not allowed when used memory > 'maxmemory'.");
        }
        let reply = execute(&tokens, &mut self.db);
        let errored = reply.first() == Some(&b'-');
        if !errored && is_write {
            // Keep the memory estimate in sync with whatever the command changed
            // (including in-place collection growth like LPUSH/SADD).
            for key in write_keys(&tokens) {
                self.db.resync_size(key);
            }
            // A write only counts as a modification if it actually changed the
            // dataset. A no-op write (e.g. DEL of a missing key) is not logged,
            // not replicated, and does not dirty a WATCHer.
            if write_modified(&cmd, &reply) {
                for key in write_keys(&tokens) {
                    self.dirty_watchers(key);
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
                // Feed the changefeed (same modified-key set as WATCH/AOF).
                self.emit_write_changes(&tokens);
            }
        }
        if let Some(a) = self.aof.as_mut() {
            a.maybe_fsync();
        }
        // A watched key removed by passive expiry during this command must also
        // abort its transaction.
        self.dirty_expired_watchers();
        reply
    }

    /// Mark every transaction watching `key` as dirty (its EXEC will abort).
    fn dirty_watchers(&mut self, key: &[u8]) {
        if let Some(watchers) = self.watched_keys.get(key) {
            let ids: Vec<u64> = watchers.iter().copied().collect();
            for wid in ids {
                if let Some(tx) = self.txs.get_mut(&wid) {
                    tx.dirty = true;
                }
            }
        }
    }

    /// Dirty the WATCHers of any key the keyspace expired since the last drain,
    /// and emit an `expire` change to changefeed subscribers.
    fn dirty_expired_watchers(&mut self) {
        for key in self.db.take_expired() {
            self.dirty_watchers(&key);
            self.record_change(b"expire", &key, None);
        }
    }

    /// Enforce `maxmemory` by evicting arbitrary keys until under the cap.
    /// Returns true if we are within budget (proceed), false if the cap cannot
    /// be met (the caller should reject the write with OOM). Each eviction is
    /// streamed to replicas/AOF as a DEL and dirties any WATCHers — just like a
    /// client-issued delete, so replicas and snapshots stay consistent.
    fn evict_if_needed(&mut self) -> bool {
        let max = match self.maxmemory {
            Some(m) => m,
            None => return true,
        };
        while self.db.mem_used() > max {
            match self.db.evict_one() {
                Some(key) => {
                    self.dirty_watchers(&key);
                    self.record_change(b"del", &key, None);
                    self.propagate(&[b"DEL".to_vec(), key]);
                }
                None => break, // keyspace empty — nothing left to evict
            }
        }
        self.db.mem_used() <= max
    }

    /// Append one command to the AOF and stream it to every replica.
    fn propagate(&mut self, tokens: &[Vec<u8>]) {
        let batch = vec![tokens.to_vec()];
        if let Some(a) = self.aof.as_mut() {
            let _ = a.append(&batch);
        }
        if !self.replicas.is_empty() {
            let bytes = resp::command(tokens);
            for rid in self.replicas.iter() {
                if let Some(out) = self.clients.get(rid) {
                    let _ = out.send(bytes.clone());
                }
            }
        }
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
            return self.send(
                id,
                resp::error("ERR wrong number of arguments for 'replicaof' command"),
            );
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
                    .filter(|c| pat.is_none_or(|p| pubsub::glob_match(p, c)))
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

fn allowed_in_push_mode(cmd: &[u8]) -> bool {
    matches!(
        cmd,
        b"SUBSCRIBE"
            | b"UNSUBSCRIBE"
            | b"PSUBSCRIBE"
            | b"PUNSUBSCRIBE"
            | b"CDCSUBSCRIBE"
            | b"CDCUNSUBSCRIBE"
            | b"PING"
            | b"QUIT"
            | b"RESET"
    )
}

fn is_tx_control(cmd: &[u8]) -> bool {
    matches!(
        cmd,
        b"MULTI" | b"EXEC" | b"DISCARD" | b"WATCH" | b"UNWATCH" | b"RESET"
    )
}

/// Keys a write command modifies (for WATCH dirtying + memory resync). Most
/// commands touch a single key at position 1; the multi-key writes are spelled
/// out. FLUSHDB/FLUSHALL touch every key — handled separately via the keyspace's
/// expired-key log, not here.
fn write_keys(tokens: &[Vec<u8>]) -> Vec<&[u8]> {
    match tokens[0].to_ascii_uppercase().as_slice() {
        b"DEL" | b"UNLINK" => tokens[1..].iter().map(|k| k.as_slice()).collect(),
        // MSET key val key val ... -> the keys are at odd positions.
        b"MSET" | b"MSETNX" => tokens[1..]
            .iter()
            .step_by(2)
            .map(|k| k.as_slice())
            .collect(),
        // src dst -> both source and destination change.
        b"RENAME" | b"RENAMENX" | b"RPOPLPUSH" | b"LMOVE" | b"SMOVE" => {
            tokens[1..3].iter().map(|k| k.as_slice()).collect()
        }
        // BITOP op dest src... -> the destination is at position 2.
        b"BITOP" => tokens
            .get(2)
            .map(|k| vec![k.as_slice()])
            .unwrap_or_default(),
        _ => tokens
            .get(1)
            .map(|k| vec![k.as_slice()])
            .unwrap_or_default(),
    }
}

/// Did a successful write actually change its key? Used so no-op writes don't
/// log to the AOF, replicate, or dirty a WATCHer (Redis signals a key modified
/// only when it really changed). Conservative by design: when in doubt this
/// returns `true` — a spurious WATCH abort is harmless, a *missed* one is not,
/// and dropping a real write from the AOF/replicas would corrupt state. So a
/// reply pattern is treated as "no change" only where it is unambiguous.
fn write_modified(cmd: &[u8], reply: &[u8]) -> bool {
    let zero = reply.starts_with(b":0\r\n"); // integer 0
    let nil = reply == b"$-1\r\n"; // null bulk
    match cmd {
        // "count of elements changed" commands: 0 means nothing changed.
        b"DEL" | b"UNLINK" | b"SREM" | b"HDEL" | b"ZREM" | b"SADD" | b"HSETNX" | b"LPUSHX"
        | b"RPUSHX" | b"PERSIST" | b"EXPIRE" | b"PEXPIRE" | b"EXPIREAT" | b"PEXPIREAT"
        | b"SETNX" | b"MSETNX" | b"RENAMENX" | b"LREM" | b"SMOVE" | b"ZREMRANGEBYRANK"
        | b"ZREMRANGEBYSCORE" => !zero,
        // ZADD: 0 added/changed, or nil from an aborted INCR (NX/XX/GT/LT).
        b"ZADD" => !(zero || nil),
        // LINSERT: 0 (no key) or -1 (pivot not found) means nothing was inserted.
        b"LINSERT" => !(zero || reply.starts_with(b":-1\r\n")),
        // Conditional write / pop-and-move / delete: nil means it didn't happen.
        b"SET" | b"GETDEL" | b"RPOPLPUSH" | b"LMOVE" => !nil,
        _ => true,
    }
}

/// Parse a `LOCUS_MAXMEMORY` value into bytes. Accepts a plain integer or a
/// `kb`/`mb`/`gb` suffix (decimal, case-insensitive): `0` / "" disable the cap.
fn parse_mem(s: &str) -> Option<usize> {
    let s = s.trim().to_ascii_lowercase();
    if s.is_empty() {
        return None;
    }
    let (num, mult) = if let Some(n) = s.strip_suffix("gb") {
        (n, 1024 * 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("mb") {
        (n, 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("kb") {
        (n, 1024)
    } else if let Some(n) = s.strip_suffix('b') {
        (n, 1)
    } else {
        (s.as_str(), 1)
    };
    num.trim().parse::<usize>().ok().map(|n| n * mult)
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
                hub.protos.remove(&id);
                hub.changefeeds.remove(&id);
            }
            Ok(Msg::Command { id, tokens }) => hub.handle_command(id, tokens),
            Ok(Msg::ReplaceDb(db)) => {
                hub.db = *db;
                // A replica that just loaded a full-sync snapshot may now be able
                // to satisfy readers parked on a blocking XREAD.
                hub.serve_blocked();
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if let Some(a) = hub.aof.as_mut() {
                    a.maybe_fsync();
                }
                hub.db.active_expire();
                hub.dirty_expired_watchers();
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
    // Bound the handshake + snapshot reads so a master that accepts the TCP
    // connection but never replies can't hang this thread forever (a stuck read
    // errors out, replica_sync retries, and REPLICAOF NO ONE can take effect).
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    // Handshake: PING -> REPLCONF -> PSYNC.
    send_cmd(&mut stream, &[b"PING"])?;
    read_line(&mut stream)?;
    let myport = std::env::var("LOCUS_PORT").unwrap_or_else(|_| "6379".into());
    send_cmd(
        &mut stream,
        &[b"REPLCONF", b"listening-port", myport.as_bytes()],
    )?;
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
                                && hub_tx
                                    .send(Msg::Command {
                                        id: MASTER_ID,
                                        tokens,
                                    })
                                    .is_err()
                            {
                                return Ok(());
                            }
                        }
                        Parsed::Incomplete => break,
                        Parsed::Error(_) => return Ok(()),
                    }
                }
            }
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) => {}
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
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "expected bulk header",
        ));
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

    if tx
        .send(Msg::Connect {
            id,
            out: out_tx.clone(),
        })
        .is_err()
    {
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
        // Parse all complete commands in this batch from a moving offset, then
        // drain once — O(batch) instead of O(batch^2) under heavy pipelining.
        let mut pos = 0;
        loop {
            match parse_command(&inbuf[pos..]) {
                Parsed::Incomplete => break,
                Parsed::Error(msg) => {
                    let _ = out_tx.send(resp::error(&format!("ERR Protocol error: {msg}")));
                    break 'read;
                }
                Parsed::Complete(tokens, consumed) => {
                    pos += consumed;
                    if tx.send(Msg::Command { id, tokens }).is_err() {
                        break 'read;
                    }
                }
            }
        }
        if pos > 0 {
            inbuf.drain(0..pos);
        }
    }

    let _ = tx.send(Msg::Disconnect { id });
    drop(out_tx);
    let _ = writer.join();
    println!("client disconnected: {peer}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn min_arity_knows_commands_and_minimums() {
        assert_eq!(commands::min_arity(b"GET"), Some(2));
        assert_eq!(commands::min_arity(b"SET"), Some(3));
        assert_eq!(commands::min_arity(b"XADD"), Some(5));
        assert_eq!(commands::min_arity(b"PING"), Some(1));
        assert_eq!(commands::min_arity(b"NOTACOMMAND"), None);
    }

    #[test]
    fn write_modified_detects_noops() {
        // No-op replies -> not a modification.
        assert!(!write_modified(b"DEL", b":0\r\n"));
        assert!(!write_modified(b"SADD", b":0\r\n"));
        assert!(!write_modified(b"PERSIST", b":0\r\n"));
        assert!(!write_modified(b"ZADD", b":0\r\n"));
        assert!(!write_modified(b"ZADD", b"$-1\r\n")); // aborted INCR
        assert!(!write_modified(b"SET", b"$-1\r\n")); // NX/XX failed
        assert!(!write_modified(b"GETDEL", b"$-1\r\n"));
        // Real modifications.
        assert!(write_modified(b"DEL", b":1\r\n"));
        assert!(write_modified(b"SADD", b":2\r\n"));
        assert!(write_modified(b"SET", b"+OK\r\n"));
        assert!(write_modified(b"INCR", b":0\r\n")); // INCR result of 0 IS a change
        assert!(write_modified(b"APPEND", b":0\r\n")); // created empty key
        assert!(write_modified(b"HSET", b":0\r\n")); // overwrote existing field
    }

    #[test]
    fn parse_mem_handles_suffixes() {
        assert_eq!(parse_mem("0"), Some(0));
        assert_eq!(parse_mem("1024"), Some(1024));
        assert_eq!(parse_mem("1kb"), Some(1024));
        assert_eq!(parse_mem("2MB"), Some(2 * 1024 * 1024));
        assert_eq!(parse_mem("1gb"), Some(1024 * 1024 * 1024));
        assert_eq!(parse_mem("512b"), Some(512));
        assert_eq!(parse_mem(""), None);
        assert_eq!(parse_mem("notanumber"), None);
    }
}
