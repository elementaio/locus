//! Locus — M1: RESP parser + ECHO + SET/GET on an in-memory map.
//!
//! Now the reply depends on what was sent. We parse the Redis wire protocol
//! (RESP2) into command tokens and run real commands against a `HashMap`:
//!
//!   $ cargo run
//!   $ redis-cli -p 6379 set foo bar   # -> OK
//!   $ redis-cli -p 6379 get foo       # -> "bar"
//!
//! Two ideas carry the whole project and both live here:
//!
//! 1. THE RESUMABLE PARSER. TCP is a byte *stream*: one read() may return half
//!    a command, exactly one, or several glued together. So the parser is a
//!    state machine over a per-connection buffer — feed bytes in, get back
//!    zero-or-more complete commands plus the partial tail we keep for later.
//!    It NEVER assumes a read() == a command. (See the unit tests at the bottom,
//!    which feed it one byte at a time.)
//!
//! 2. THE SINGLE KEYSPACE OWNER. `main` owns the one `HashMap` and handles
//!    connections one at a time, so every command runs to completion with no
//!    interleaving — atomicity for free, no locks. (Many concurrent clients is
//!    M2, where tokio arrives; for M1 a serial loop keeps the lesson clean.)

use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};

/// 6379 is Redis's port, so the real `redis-cli` finds us with no flags.
/// (6379 spells "MERZ" on a phone keypad — antirez's old joke.)
const ADDR: &str = "127.0.0.1:6379";

/// DoS guards: reject absurd declared sizes before allocating (HARD-PARTS §1).
/// These mirror Redis's defaults.
const MAX_ARRAY: i64 = 1024 * 1024;
const MAX_BULK_LEN: i64 = 512 * 1024 * 1024;

/// The keyspace: raw bytes -> raw bytes. Redis keys and values are binary-safe,
/// so we store `Vec<u8>`, not `String`.
type Store = HashMap<Vec<u8>, Vec<u8>>;

// ===========================================================================
// RESP parsing  — the resumable state machine
// ===========================================================================

/// The result of trying to parse one command from the front of a buffer.
#[derive(Debug)]
enum Parsed {
    /// A full command: its tokens, and how many bytes it consumed (so the
    /// caller can drop that prefix and try to parse the next one).
    Complete(Vec<Vec<u8>>, usize),
    /// Not enough bytes yet — keep the buffer untouched and read more.
    Incomplete,
    /// The stream is malformed; framing is lost, so the caller must close.
    Error(String),
}

/// Reading a length-prefixed line like `*3\r\n` or `$5\r\n`.
enum Count {
    Ok(i64, usize), // value, index just past the CRLF
    Incomplete,     // no CRLF yet
    Bad,            // CRLF present but the digits don't parse
}

/// Find the next `\r\n` at or after `from`; returns the index of the `\r`.
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

/// Read the integer on the line whose type byte is at `pos` (e.g. the `3` in
/// `*3\r\n`). Returns where parsing should continue (just past the CRLF).
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

/// Try to parse exactly one command from the front of `buf`.
fn parse_command(buf: &[u8]) -> Parsed {
    if buf.is_empty() {
        return Parsed::Incomplete;
    }
    // A real client sends a multibulk (array of bulk strings) starting with '*'.
    // Anything else is treated as an INLINE command (telnet, raw `PING\r\n`).
    if buf[0] != b'*' {
        return parse_inline(buf);
    }

    // Header: *<count>\r\n
    let (count, mut pos) = match read_count(buf, 0) {
        Count::Ok(n, p) => (n, p),
        Count::Incomplete => return Parsed::Incomplete,
        Count::Bad => return Parsed::Error("invalid multibulk length".into()),
    };
    if count <= 0 {
        // Empty/null array — a no-op request; consume it and move on.
        return Parsed::Complete(Vec::new(), pos);
    }
    if count > MAX_ARRAY {
        return Parsed::Error("invalid multibulk length".into());
    }

    // Then `count` bulk strings: $<len>\r\n<bytes>\r\n
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
        // We need the payload AND its trailing CRLF all present.
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

/// Inline fallback: split the first line on whitespace into tokens.
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

/// A bulk string: `$<len>\r\n<bytes>\r\n`. Binary-safe (length-prefixed).
fn bulk_string(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(data.len() + 16);
    out.extend_from_slice(format!("${}\r\n", data.len()).as_bytes());
    out.extend_from_slice(data);
    out.extend_from_slice(b"\r\n");
    out
}

/// The null bulk string `$-1\r\n` — Redis's "nil" (e.g. GET on a missing key).
fn null_bulk() -> Vec<u8> {
    b"$-1\r\n".to_vec()
}

// ===========================================================================
// Command execution
// ===========================================================================

/// Run one parsed command against the keyspace and return its RESP reply.
/// Command names are matched case-insensitively (redis-cli may send `set`).
fn execute(tokens: &[Vec<u8>], store: &mut Store) -> Vec<u8> {
    if tokens.is_empty() {
        return Vec::new(); // nothing to do, no reply
    }
    let cmd = tokens[0].to_ascii_uppercase();
    match cmd.as_slice() {
        b"PING" => match tokens.len() {
            1 => simple_string("PONG"),
            2 => bulk_string(&tokens[1]), // PING <msg> echoes the message
            _ => error("ERR wrong number of arguments for 'ping' command"),
        },
        b"ECHO" => match tokens.len() {
            2 => bulk_string(&tokens[1]),
            _ => error("ERR wrong number of arguments for 'echo' command"),
        },
        b"SET" => match tokens.len() {
            // SET options (EX/NX/...) arrive in M3; M1 is the bare form.
            3 => {
                store.insert(tokens[1].clone(), tokens[2].clone());
                simple_string("OK")
            }
            _ => error("ERR wrong number of arguments for 'set' command"),
        },
        b"GET" => match tokens.len() {
            2 => match store.get(&tokens[1]) {
                Some(v) => bulk_string(v),
                None => null_bulk(),
            },
            _ => error("ERR wrong number of arguments for 'get' command"),
        },
        // Minimal stub so an interactive `redis-cli` session starts cleanly
        // (it sends COMMAND DOCS on connect). Real implementation is later.
        b"COMMAND" => b"*0\r\n".to_vec(),
        other => error(&format!(
            "ERR unknown command '{}'",
            String::from_utf8_lossy(other)
        )),
    }
}

// ===========================================================================
// Server
// ===========================================================================

fn main() -> io::Result<()> {
    let listener = TcpListener::bind(ADDR)?;
    // THE single keyspace owner: one map, owned here, mutated by one connection
    // at a time. Serialized execution = atomicity without locks.
    let mut store: Store = HashMap::new();
    println!("Locus M1 listening on {ADDR} — try: redis-cli -p 6379 set foo bar");

    for stream in listener.incoming() {
        match stream {
            Ok(conn) => {
                if let Err(e) = handle_conn(conn, &mut store) {
                    eprintln!("connection error: {e}");
                }
            }
            Err(e) => eprintln!("accept error: {e}"),
        }
    }
    Ok(())
}

/// Handle one client: accumulate bytes and drain every complete command the
/// buffer holds. This is where the resumable parser earns its keep.
fn handle_conn(mut conn: TcpStream, store: &mut Store) -> io::Result<()> {
    let peer = conn.peer_addr()?;
    println!("client connected: {peer}");

    let mut inbuf: Vec<u8> = Vec::new(); // persists across reads — the partial tail lives here
    let mut chunk = [0u8; 4096];
    loop {
        let n = conn.read(&mut chunk)?;
        if n == 0 {
            println!("client disconnected: {peer}");
            return Ok(());
        }
        inbuf.extend_from_slice(&chunk[..n]);

        // Drain as many COMPLETE commands as the buffer currently holds.
        loop {
            match parse_command(&inbuf) {
                Parsed::Incomplete => break, // keep the partial tail, go read more
                Parsed::Error(msg) => {
                    // Framing is lost — reply once and close the connection.
                    let _ = conn.write_all(&error(&format!("ERR Protocol error: {msg}")));
                    return Ok(());
                }
                Parsed::Complete(tokens, consumed) => {
                    let reply = execute(&tokens, store);
                    if !reply.is_empty() {
                        conn.write_all(&reply)?;
                    }
                    // Drop the bytes we just consumed. (O(n) shift; fine for M1,
                    // optimized later — see ROADMAP M12.)
                    inbuf.drain(0..consumed);
                }
            }
        }
    }
}

// ===========================================================================
// Tests — the parser must survive split and pipelined input (HARD-PARTS §1)
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
        assert_eq!(
            tokens,
            vec![b"SET".to_vec(), b"foo".to_vec(), b"bar".to_vec()]
        );
        assert_eq!(consumed, buf.len());
    }

    #[test]
    fn every_prefix_is_incomplete() {
        // The crux: no proper prefix of a command may parse as complete.
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
        // Feed one byte at a time: Incomplete until the very last byte lands.
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
        // Two commands in one buffer must parse one after the other.
        let buf = b"*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n";
        let (t1, n1) = complete(buf);
        assert_eq!(t1, vec![b"PING".to_vec()]);
        let (t2, n2) = complete(&buf[n1..]);
        assert_eq!(t2, vec![b"ECHO".to_vec(), b"hi".to_vec()]);
        assert_eq!(n1 + n2, buf.len());
    }

    #[test]
    fn inline_command_is_accepted() {
        let (tokens, consumed) = complete(b"PING\r\n");
        assert_eq!(tokens, vec![b"PING".to_vec()]);
        assert_eq!(consumed, 6);
    }

    #[test]
    fn bad_framing_is_an_error() {
        // Says 1 element, but the element isn't a bulk string.
        assert!(matches!(parse_command(b"*1\r\n+OK\r\n"), Parsed::Error(_)));
    }

    #[test]
    fn set_get_roundtrip_and_nil() {
        let mut store = Store::new();
        assert_eq!(
            execute(&[b"SET".to_vec(), b"k".to_vec(), b"v".to_vec()], &mut store),
            b"+OK\r\n".to_vec()
        );
        assert_eq!(
            execute(&[b"GET".to_vec(), b"k".to_vec()], &mut store),
            b"$1\r\nv\r\n".to_vec()
        );
        assert_eq!(
            execute(&[b"GET".to_vec(), b"missing".to_vec()], &mut store),
            b"$-1\r\n".to_vec()
        );
    }
}
