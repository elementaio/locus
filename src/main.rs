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

use std::collections::{HashMap, HashSet};
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

use commands::execute;
use db::Db;
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

        match cmd.as_slice() {
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
                // A replica is read-only for normal clients.
                if self.master.is_some() && id != MASTER_ID && aof::is_write(&cmd) {
                    return self.send(id, resp::error("READONLY You can't write against a read only replica."));
                }
                let reply = execute(&tokens, &mut self.db);
                let errored = reply.first() == Some(&b'-');
                if !errored && aof::is_write(&tokens[0]) {
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
                self.send(id, reply);
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
