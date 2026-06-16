//! Locus — M2: concurrent clients + more string commands.
//!
//! M1 served one client at a time. Now many clients connect at once — and we
//! meet the central question of the whole project: how do concurrent clients
//! share one keyspace without races?
//!
//! THE NAÏVE ANSWER (and why we don't use it): wrap the map in
//! `Arc<Mutex<HashMap>>` and let every connection thread lock it. The borrow
//! checker will happily allow this — but now command execution is a lock
//! free-for-all, exactly the contention Redis was designed to avoid.
//!
//! THE REDIS-FAITHFUL ANSWER (what we do): spawn a thread per connection for
//! I/O (read + parse + write), but route every parsed command through ONE owner
//! thread that holds the single `HashMap`. Commands are executed one at a time,
//! in arrival order — atomic by construction, no locks on the data. The channel
//! is the only shared thing, and it serializes everything.
//!
//!   client thread ──parse──▶ [cmd + reply-channel] ──▶ owner thread (the map)
//!   client thread ◀──────────────── reply ────────────────────┘
//!
//! (We use std threads, not tokio — zero dependencies, and it makes the
//! single-owner model explicit. Swapping blocking-thread-per-connection for an
//! async event loop is a later *performance* step, not a correctness one.)

use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc;
use std::thread;

const ADDR: &str = "127.0.0.1:6379";
const MAX_ARRAY: i64 = 1024 * 1024;
const MAX_BULK_LEN: i64 = 512 * 1024 * 1024;

/// The keyspace: raw bytes -> raw bytes (binary-safe, like Redis).
type Store = HashMap<Vec<u8>, Vec<u8>>;

/// One unit of work sent from a connection thread to the keyspace owner:
/// the parsed command, plus a channel to send the reply back on.
struct Request {
    tokens: Vec<Vec<u8>>,
    reply_tx: mpsc::Sender<Vec<u8>>,
}

// ===========================================================================
// RESP parsing — the resumable state machine (unchanged from M1)
// ===========================================================================

#[derive(Debug)]
enum Parsed {
    Complete(Vec<Vec<u8>>, usize),
    Incomplete,
    Error(String),
}

enum Count {
    Ok(i64, usize),
    Incomplete,
    Bad,
}

fn find_crlf(buf: &[u8], from: usize) -> Option<usize> {
    let mut i = from;
    while i + 1 < buf.len() {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return Some(i);
        }
        i += 1;
    }
    None
}

fn read_count(buf: &[u8], pos: usize) -> Count {
    match find_crlf(buf, pos + 1) {
        None => Count::Incomplete,
        Some(cr) => match std::str::from_utf8(&buf[pos + 1..cr])
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
        {
            Some(n) => Count::Ok(n, cr + 2),
            None => Count::Bad,
        },
    }
}

fn parse_command(buf: &[u8]) -> Parsed {
    if buf.is_empty() {
        return Parsed::Incomplete;
    }
    if buf[0] != b'*' {
        return parse_inline(buf);
    }
    let (count, mut pos) = match read_count(buf, 0) {
        Count::Ok(n, p) => (n, p),
        Count::Incomplete => return Parsed::Incomplete,
        Count::Bad => return Parsed::Error("invalid multibulk length".into()),
    };
    if count <= 0 {
        return Parsed::Complete(Vec::new(), pos);
    }
    if count > MAX_ARRAY {
        return Parsed::Error("invalid multibulk length".into());
    }
    let mut tokens: Vec<Vec<u8>> = Vec::with_capacity(count as usize);
    for _ in 0..count {
        if pos >= buf.len() {
            return Parsed::Incomplete;
        }
        if buf[pos] != b'$' {
            return Parsed::Error(format!("expected '$', got '{}'", buf[pos] as char));
        }
        let (len, after_len) = match read_count(buf, pos) {
            Count::Ok(n, p) => (n, p),
            Count::Incomplete => return Parsed::Incomplete,
            Count::Bad => return Parsed::Error("invalid bulk length".into()),
        };
        if len < 0 || len > MAX_BULK_LEN {
            return Parsed::Error("invalid bulk length".into());
        }
        let len = len as usize;
        if after_len + len + 2 > buf.len() {
            return Parsed::Incomplete;
        }
        let data = buf[after_len..after_len + len].to_vec();
        let crlf_at = after_len + len;
        if &buf[crlf_at..crlf_at + 2] != b"\r\n" {
            return Parsed::Error("expected CRLF after bulk string".into());
        }
        tokens.push(data);
        pos = crlf_at + 2;
    }
    Parsed::Complete(tokens, pos)
}

fn parse_inline(buf: &[u8]) -> Parsed {
    match find_crlf(buf, 0) {
        None => Parsed::Incomplete,
        Some(cr) => {
            let tokens = buf[..cr]
                .split(|b| matches!(b, b' ' | b'\t'))
                .filter(|s| !s.is_empty())
                .map(|s| s.to_vec())
                .collect();
            Parsed::Complete(tokens, cr + 2)
        }
    }
}

// ===========================================================================
// RESP reply encoders
// ===========================================================================

fn simple_string(s: &str) -> Vec<u8> {
    format!("+{s}\r\n").into_bytes()
}

fn error(s: &str) -> Vec<u8> {
    format!("-{s}\r\n").into_bytes()
}

/// An integer reply: `:<n>\r\n` (used by INCR, DEL, EXISTS, STRLEN, ...).
fn integer(n: i64) -> Vec<u8> {
    format!(":{n}\r\n").into_bytes()
}

fn bulk_string(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(data.len() + 16);
    out.extend_from_slice(format!("${}\r\n", data.len()).as_bytes());
    out.extend_from_slice(data);
    out.extend_from_slice(b"\r\n");
    out
}

fn null_bulk() -> Vec<u8> {
    b"$-1\r\n".to_vec()
}

// ===========================================================================
// Command execution — runs only on the single owner thread
// ===========================================================================

/// INCR/DECR/INCRBY/DECRBY core: read the value as a base-10 integer (missing
/// key counts as 0), apply `delta`, store the result back as text.
fn incr_by(store: &mut Store, key: &[u8], delta: i64) -> Vec<u8> {
    let current = match store.get(key) {
        None => 0,
        Some(v) => match std::str::from_utf8(v).ok().and_then(|s| s.parse::<i64>().ok()) {
            Some(n) => n,
            None => return error("ERR value is not an integer or out of range"),
        },
    };
    match current.checked_add(delta) {
        Some(next) => {
            store.insert(key.to_vec(), next.to_string().into_bytes());
            integer(next)
        }
        None => error("ERR increment or decrement would overflow"),
    }
}

/// Parse an argument as a base-10 i64, or return None.
fn parse_int(arg: &[u8]) -> Option<i64> {
    std::str::from_utf8(arg).ok().and_then(|s| s.parse::<i64>().ok())
}

fn wrong_args(cmd: &str) -> Vec<u8> {
    error(&format!("ERR wrong number of arguments for '{cmd}' command"))
}

fn execute(tokens: &[Vec<u8>], store: &mut Store) -> Vec<u8> {
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
        b"SET" => match tokens.len() {
            3 => {
                store.insert(tokens[1].clone(), tokens[2].clone());
                simple_string("OK")
            }
            _ => wrong_args("set"), // SET options (EX/NX/...) arrive in M3
        },
        b"GET" => match tokens.len() {
            2 => match store.get(&tokens[1]) {
                Some(v) => bulk_string(v),
                None => null_bulk(),
            },
            _ => wrong_args("get"),
        },
        b"GETDEL" => match tokens.len() {
            2 => match store.remove(&tokens[1]) {
                Some(v) => bulk_string(&v),
                None => null_bulk(),
            },
            _ => wrong_args("getdel"),
        },
        b"DEL" => {
            if tokens.len() < 2 {
                wrong_args("del")
            } else {
                let removed = tokens[1..].iter().filter(|k| store.remove(*k).is_some()).count();
                integer(removed as i64)
            }
        }
        b"EXISTS" => {
            if tokens.len() < 2 {
                wrong_args("exists")
            } else {
                let n = tokens[1..].iter().filter(|k| store.contains_key(*k)).count();
                integer(n as i64)
            }
        }
        b"INCR" => match tokens.len() {
            2 => incr_by(store, &tokens[1], 1),
            _ => wrong_args("incr"),
        },
        b"DECR" => match tokens.len() {
            2 => incr_by(store, &tokens[1], -1),
            _ => wrong_args("decr"),
        },
        b"INCRBY" => match tokens.len() {
            3 => match parse_int(&tokens[2]) {
                Some(d) => incr_by(store, &tokens[1], d),
                None => error("ERR value is not an integer or out of range"),
            },
            _ => wrong_args("incrby"),
        },
        b"DECRBY" => match tokens.len() {
            3 => match parse_int(&tokens[2]).and_then(|d| d.checked_neg()) {
                Some(neg) => incr_by(store, &tokens[1], neg),
                None => error("ERR value is not an integer or out of range"),
            },
            _ => wrong_args("decrby"),
        },
        b"APPEND" => match tokens.len() {
            3 => {
                let entry = store.entry(tokens[1].clone()).or_default();
                entry.extend_from_slice(&tokens[2]);
                integer(entry.len() as i64)
            }
            _ => wrong_args("append"),
        },
        b"STRLEN" => match tokens.len() {
            2 => integer(store.get(&tokens[1]).map(|v| v.len()).unwrap_or(0) as i64),
            _ => wrong_args("strlen"),
        },
        b"TYPE" => match tokens.len() {
            2 => {
                if store.contains_key(&tokens[1]) {
                    simple_string("string")
                } else {
                    simple_string("none")
                }
            }
            _ => wrong_args("type"),
        },
        // Stubs so an interactive `redis-cli` session connects cleanly.
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

// ===========================================================================
// Server — thread per connection, one owner thread for the keyspace
// ===========================================================================

fn main() -> io::Result<()> {
    // The owner channel: every connection sends Requests here; one thread owns
    // the map and drains them serially.
    let (cmd_tx, cmd_rx) = mpsc::channel::<Request>();
    thread::spawn(move || run_keyspace(cmd_rx));

    let listener = TcpListener::bind(ADDR)?;
    println!("Locus M2 listening on {ADDR} — concurrent clients, serialized execution");

    for stream in listener.incoming() {
        match stream {
            Ok(conn) => {
                // Each client gets its own thread for I/O; commands still funnel
                // through the single owner, so execution stays serialized.
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

/// The single keyspace owner: receives commands from all connections and runs
/// them one at a time against the one map. This is "single-threaded execution."
fn run_keyspace(rx: mpsc::Receiver<Request>) {
    let mut store: Store = HashMap::new();
    while let Ok(req) = rx.recv() {
        let reply = execute(&req.tokens, &mut store);
        // If the client vanished, its reply channel is closed — just drop it.
        let _ = req.reply_tx.send(reply);
    }
}

/// One client connection: accumulate bytes, drain complete commands, hand each
/// to the owner thread, and write back the reply it returns.
fn handle_conn(mut conn: TcpStream, cmd_tx: mpsc::Sender<Request>) -> io::Result<()> {
    let peer = conn.peer_addr()?;
    println!("client connected: {peer}");

    // This connection's private reply mailbox, reused for every command.
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
                    // Hand the command to the owner and wait for its reply.
                    let req = Request {
                        tokens,
                        reply_tx: reply_tx.clone(),
                    };
                    if cmd_tx.send(req).is_err() {
                        return Ok(()); // owner gone; nothing more we can do
                    }
                    match reply_rx.recv() {
                        Ok(reply) => {
                            if !reply.is_empty() {
                                conn.write_all(&reply)?;
                            }
                        }
                        Err(_) => return Ok(()),
                    }
                }
            }
        }
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn complete(buf: &[u8]) -> (Vec<Vec<u8>>, usize) {
        match parse_command(buf) {
            Parsed::Complete(t, n) => (t, n),
            other => panic!("expected Complete, got {other:?}"),
        }
    }

    #[test]
    fn parses_a_set_command() {
        let buf = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        let (tokens, consumed) = complete(buf);
        assert_eq!(tokens, vec![b"SET".to_vec(), b"foo".to_vec(), b"bar".to_vec()]);
        assert_eq!(consumed, buf.len());
    }

    #[test]
    fn every_prefix_is_incomplete() {
        let full = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        for i in 0..full.len() {
            assert!(
                matches!(parse_command(&full[..i]), Parsed::Incomplete),
                "prefix of length {i} should be Incomplete"
            );
        }
    }

    #[test]
    fn byte_by_byte_then_completes() {
        let full = b"*1\r\n$4\r\nPING\r\n";
        let mut buf = Vec::new();
        for (i, b) in full.iter().enumerate() {
            buf.push(*b);
            if i + 1 == full.len() {
                assert!(matches!(parse_command(&buf), Parsed::Complete(_, _)));
            } else {
                assert!(matches!(parse_command(&buf), Parsed::Incomplete));
            }
        }
    }

    #[test]
    fn pipelined_commands_drain_in_order() {
        let buf = b"*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n";
        let (t1, n1) = complete(buf);
        assert_eq!(t1, vec![b"PING".to_vec()]);
        let (t2, n2) = complete(&buf[n1..]);
        assert_eq!(t2, vec![b"ECHO".to_vec(), b"hi".to_vec()]);
        assert_eq!(n1 + n2, buf.len());
    }

    #[test]
    fn bad_framing_is_an_error() {
        assert!(matches!(parse_command(b"*1\r\n+OK\r\n"), Parsed::Error(_)));
    }

    #[test]
    fn set_get_roundtrip_and_nil() {
        let mut s = Store::new();
        assert_eq!(
            execute(&[b"SET".to_vec(), b"k".to_vec(), b"v".to_vec()], &mut s),
            b"+OK\r\n".to_vec()
        );
        assert_eq!(execute(&[b"GET".to_vec(), b"k".to_vec()], &mut s), b"$1\r\nv\r\n".to_vec());
        assert_eq!(execute(&[b"GET".to_vec(), b"x".to_vec()], &mut s), b"$-1\r\n".to_vec());
    }

    #[test]
    fn incr_creates_increments_and_rejects_non_int() {
        let mut s = Store::new();
        assert_eq!(execute(&[b"INCR".to_vec(), b"c".to_vec()], &mut s), b":1\r\n".to_vec());
        assert_eq!(execute(&[b"INCR".to_vec(), b"c".to_vec()], &mut s), b":2\r\n".to_vec());
        assert_eq!(
            execute(&[b"INCRBY".to_vec(), b"c".to_vec(), b"10".to_vec()], &mut s),
            b":12\r\n".to_vec()
        );
        execute(&[b"SET".to_vec(), b"k".to_vec(), b"abc".to_vec()], &mut s);
        assert!(execute(&[b"INCR".to_vec(), b"k".to_vec()], &mut s).starts_with(b"-ERR"));
    }

    #[test]
    fn del_exists_append_strlen_type() {
        let mut s = Store::new();
        execute(&[b"SET".to_vec(), b"a".to_vec(), b"1".to_vec()], &mut s);
        execute(&[b"SET".to_vec(), b"b".to_vec(), b"2".to_vec()], &mut s);
        assert_eq!(
            execute(&[b"EXISTS".to_vec(), b"a".to_vec(), b"b".to_vec(), b"z".to_vec()], &mut s),
            b":2\r\n".to_vec()
        );
        assert_eq!(
            execute(&[b"DEL".to_vec(), b"a".to_vec(), b"z".to_vec()], &mut s),
            b":1\r\n".to_vec()
        );
        assert_eq!(execute(&[b"APPEND".to_vec(), b"s".to_vec(), b"hel".to_vec()], &mut s), b":3\r\n".to_vec());
        assert_eq!(execute(&[b"APPEND".to_vec(), b"s".to_vec(), b"lo".to_vec()], &mut s), b":5\r\n".to_vec());
        assert_eq!(execute(&[b"STRLEN".to_vec(), b"s".to_vec()], &mut s), b":5\r\n".to_vec());
        assert_eq!(execute(&[b"TYPE".to_vec(), b"s".to_vec()], &mut s), b"+string\r\n".to_vec());
        assert_eq!(execute(&[b"TYPE".to_vec(), b"missing".to_vec()], &mut s), b"+none\r\n".to_vec());
    }
}
