//! Locus — an in-memory, geo-first datastore that speaks the Redis protocol.
//!
//! Architecture (single-threaded execution, the Redis way):
//!   * one thread per connection handles I/O (read, parse, write);
//!   * every parsed command is sent over a channel to ONE owner thread that
//!     holds the keyspace and runs commands serially — atomic by construction.
//!
//! Milestones so far: M0 PONG · M1 RESP+SET/GET · M2 concurrency+strings ·
//! M3 key expiry (passive + active).

mod commands;
mod db;
mod resp;

use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use commands::execute;
use db::Db;
use resp::{error, parse_command, Parsed};

const ADDR: &str = "127.0.0.1:6379";

/// One unit of work for the keyspace owner: a parsed command + where to reply.
struct Request {
    tokens: Vec<Vec<u8>>,
    reply_tx: mpsc::Sender<Vec<u8>>,
}

fn main() -> io::Result<()> {
    let (cmd_tx, cmd_rx) = mpsc::channel::<Request>();
    thread::spawn(move || run_keyspace(cmd_rx));

    let listener = TcpListener::bind(ADDR)?;
    println!("Locus listening on {ADDR}");

    for stream in listener.incoming() {
        match stream {
            Ok(conn) => {
                let tx = cmd_tx.clone();
                thread::spawn(move || {
                    if let Err(e) = handle_conn(conn, tx) {
                        eprintln!("connection error: {e}");
                    }
                });
            }
            Err(e) => eprintln!("accept error: {e}"),
        }
    }
    Ok(())
}

/// The single keyspace owner: runs commands one at a time, and — when idle or
/// periodically — runs an active-expiration pass to reclaim TTL'd memory.
fn run_keyspace(rx: mpsc::Receiver<Request>) {
    let mut db = Db::new();
    let mut since_expire = 0u32;
    loop {
        match rx.recv_timeout(Duration::from_millis(100)) {
            Ok(req) => {
                let reply = execute(&req.tokens, &mut db);
                let _ = req.reply_tx.send(reply);
                since_expire += 1;
                if since_expire >= 1000 {
                    db.active_expire();
                    since_expire = 0;
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                db.active_expire();
                since_expire = 0;
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
}

fn handle_conn(mut conn: TcpStream, cmd_tx: mpsc::Sender<Request>) -> io::Result<()> {
    let peer = conn.peer_addr()?;
    println!("client connected: {peer}");
    let (reply_tx, reply_rx) = mpsc::channel::<Vec<u8>>();

    let mut inbuf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let n = conn.read(&mut chunk)?;
        if n == 0 {
            println!("client disconnected: {peer}");
            return Ok(());
        }
        inbuf.extend_from_slice(&chunk[..n]);

        loop {
            match parse_command(&inbuf) {
                Parsed::Incomplete => break,
                Parsed::Error(msg) => {
                    let _ = conn.write_all(&error(&format!("ERR Protocol error: {msg}")));
                    return Ok(());
                }
                Parsed::Complete(tokens, consumed) => {
                    inbuf.drain(0..consumed);
                    let req = Request {
                        tokens,
                        reply_tx: reply_tx.clone(),
                    };
                    if cmd_tx.send(req).is_err() {
                        return Ok(());
                    }
                    match reply_rx.recv() {
                        Ok(reply) if !reply.is_empty() => conn.write_all(&reply)?,
                        Ok(_) => {}
                        Err(_) => return Ok(()),
                    }
                }
            }
        }
    }
}
