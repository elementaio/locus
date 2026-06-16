//! Locus — an in-memory, geo-first datastore that speaks the Redis protocol.
//!
//! Architecture (single-threaded execution, the Redis way):
//!   * each connection has a READER thread (read + parse) and a WRITER thread
//!     (drain an output channel to the socket);
//!   * one owner thread (the "hub") holds the keyspace AND the pub/sub registry,
//!     processes every command serially, and routes replies + published messages
//!     to clients' output channels.
//!
//! Because all command execution funnels through the hub, every command is
//! atomic by construction — no locks on the data.
//!
//! Milestones: M0 PONG · M1 RESP+SET/GET · M2 concurrency · M3 expiry ·
//! M4 lists/hashes/sets · M5 sorted sets · M6 RDB · M7 AOF · M8 pub/sub.

mod aof;
mod commands;
mod db;
mod pubsub;
mod rdb;
mod resp;

use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use commands::execute;
use db::Db;
use pubsub::PubSub;
use resp::{parse_command, Parsed};

const ADDR: &str = "127.0.0.1:6379";

/// Messages from connection threads to the hub.
enum Msg {
    Connect { id: u64, out: mpsc::Sender<Vec<u8>> },
    Command { id: u64, tokens: Vec<Vec<u8>> },
    Disconnect { id: u64 },
}

fn main() -> io::Result<()> {
    let (tx, rx) = mpsc::channel::<Msg>();
    thread::spawn(move || run_hub(rx));

    let listener = TcpListener::bind(ADDR)?;
    println!("Locus listening on {ADDR}");

    let mut next_id: u64 = 1;
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

// === the hub: keyspace + pub/sub, single-threaded ===========================

struct Hub {
    db: Db,
    aof: Option<aof::Aof>,
    aof_path: Option<String>,
    clients: HashMap<u64, mpsc::Sender<Vec<u8>>>,
    pubsub: PubSub,
}

impl Hub {
    fn new() -> Hub {
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

        // When in "subscribe mode", only a small set of commands is allowed.
        if self.pubsub.total(id) > 0 && !allowed_in_subscribe_mode(&cmd) {
            self.send(
                id,
                resp::error(&format!(
                    "ERR Can't execute '{}': only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
                    String::from_utf8_lossy(&cmd).to_ascii_lowercase()
                )),
            );
            return;
        }

        match cmd.as_slice() {
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
                    return self.send(id, resp::error("ERR wrong number of arguments for 'publish' command"));
                }
                let n = self.pubsub.publish(&tokens[1], &tokens[2], &self.clients);
                self.send(id, resp::integer(n));
            }
            b"PUBSUB" => self.handle_pubsub_introspect(id, &tokens),
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
            _ => {
                let reply = execute(&tokens, &mut self.db);
                if let Some(a) = self.aof.as_mut() {
                    let errored = reply.first() == Some(&b'-');
                    if !errored && aof::is_write(&tokens[0]) {
                        let entries = aof::entries_for(&tokens, &reply, &mut self.db);
                        let _ = a.append(&entries);
                    }
                    a.maybe_fsync();
                }
                self.send(id, reply);
            }
        }
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

fn run_hub(rx: mpsc::Receiver<Msg>) {
    let mut hub = Hub::new();
    loop {
        match rx.recv_timeout(Duration::from_millis(100)) {
            Ok(Msg::Connect { id, out }) => {
                hub.clients.insert(id, out);
            }
            Ok(Msg::Disconnect { id }) => {
                hub.clients.remove(&id);
                hub.pubsub.remove_client(id);
            }
            Ok(Msg::Command { id, tokens }) => hub.handle_command(id, tokens),
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

// === per-connection: reader thread (here) + writer thread (spawned) =========

fn handle_conn(conn: TcpStream, id: u64, tx: mpsc::Sender<Msg>) -> io::Result<()> {
    let peer = conn.peer_addr()?;
    println!("client connected: {peer}");

    // Writer thread: owns a clone of the socket's write side and drains the
    // output channel. ALL bytes to the client (command replies AND pub/sub
    // pushes) go through here, so there's a single writer per socket.
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

    // Reader loop: parse commands and forward them to the hub (no waiting for
    // replies — they come back asynchronously via the writer, which also gives
    // us pipelining for free).
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

    // Cleanup: tell the hub we're gone (it drops its output-channel clone), drop
    // our clone, and the writer exits once both senders are gone.
    let _ = tx.send(Msg::Disconnect { id });
    drop(out_tx);
    let _ = writer.join();
    println!("client disconnected: {peer}");
    Ok(())
}
