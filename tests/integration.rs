//! End-to-end tests that drive the real server over TCP.
//!
//! These spawn the compiled `locus` binary on an ephemeral port and talk to it
//! with a minimal RESP client — exercising the reader/hub/writer threading and
//! the actual wire protocol, which the in-process unit tests cannot. The focus
//! is transaction correctness (MULTI/EXEC/WATCH), plus smoke coverage of
//! pipelining, pub/sub, blocking XREAD, and a replication round-trip.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::thread::sleep;
use std::time::{Duration, Instant};

/// A running server child; killed and its RDB file removed on drop.
struct Server {
    child: Child,
    port: u16,
    rdb: String,
}

impl Server {
    fn start() -> Server {
        Server::start_inner(&[])
    }

    fn start_inner(extra_env: &[(&str, &str)]) -> Server {
        let port = free_port();
        let rdb = format!(
            "{}/locus-test-{}-{}.rdb",
            std::env::temp_dir().display(),
            std::process::id(),
            port
        );
        let _ = std::fs::remove_file(&rdb);
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_locus"));
        cmd.env("LOCUS_PORT", port.to_string())
            .env("LOCUS_RDB", &rdb)
            .env_remove("LOCUS_AOF")
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        for (k, v) in extra_env {
            cmd.env(k, v);
        }
        let child = cmd.spawn().expect("spawn locus");
        let server = Server { child, port, rdb };
        server.wait_ready();
        server
    }

    fn wait_ready(&self) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            if TcpStream::connect(("127.0.0.1", self.port)).is_ok() {
                return;
            }
            sleep(Duration::from_millis(25));
        }
        panic!("server on port {} never came up", self.port);
    }

    fn connect(&self) -> Conn {
        let stream = TcpStream::connect(("127.0.0.1", self.port)).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        Conn {
            reader: BufReader::new(stream.try_clone().unwrap()),
            stream,
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_file(&self.rdb);
    }
}

/// Bind to port 0 to let the OS pick a free port, then release it for the child.
fn free_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

/// A minimal RESP client connection.
struct Conn {
    stream: TcpStream,
    reader: BufReader<TcpStream>,
}

impl Conn {
    /// Send one command (no reply read) — used to park a blocking command.
    fn send(&mut self, args: &[&str]) {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            out.extend_from_slice(a.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        self.stream.write_all(&out).unwrap();
    }

    /// Send a command and read exactly one reply, rendered as a readable string.
    fn cmd(&mut self, args: &[&str]) -> String {
        self.send(args);
        self.read_reply()
    }

    fn read_reply(&mut self) -> String {
        let mut line = Vec::new();
        self.reader.read_until(b'\n', &mut line).unwrap();
        while line.last() == Some(&b'\n') || line.last() == Some(&b'\r') {
            line.pop();
        }
        let (tag, rest) = line.split_at(1);
        let rest = String::from_utf8_lossy(rest).to_string();
        match tag[0] {
            b'+' => rest,               // simple string -> "OK"
            b'-' => format!("-{rest}"), // error keeps the leading '-'
            b':' => rest,               // integer -> "5"
            b'$' => {
                let n: i64 = rest.parse().unwrap();
                if n < 0 {
                    return "(nil)".to_string();
                }
                let mut buf = vec![0u8; n as usize + 2]; // data + CRLF
                self.reader.read_exact(&mut buf).unwrap();
                String::from_utf8_lossy(&buf[..n as usize]).to_string()
            }
            b'*' | b'%' => {
                let n: i64 = rest.parse().unwrap();
                if n < 0 {
                    return "(nil)".to_string();
                }
                let count = if tag[0] == b'%' { n * 2 } else { n };
                let items: Vec<String> = (0..count).map(|_| self.read_reply()).collect();
                format!("[{}]", items.join(", "))
            }
            other => panic!("unexpected reply tag {other:?} in {line:?}"),
        }
    }
}

/// Pull a numeric field out of an INFO reply (e.g. "used_memory").
fn info_field(c: &mut Conn, field: &str) -> u64 {
    let info = c.cmd(&["INFO"]);
    info.split("\r\n")
        .find_map(|l| l.strip_prefix(field)?.strip_prefix(':'))
        .unwrap_or_else(|| panic!("field {field} not found in INFO:\n{info}"))
        .trim()
        .parse()
        .unwrap()
}

// === pipelining / basics ====================================================

#[test]
fn pipelining_preserves_order() {
    let s = Server::start();
    let mut c = s.connect();
    // Two commands in one write; replies must come back in order.
    c.send(&["SET", "k", "v"]);
    c.send(&["GET", "k"]);
    assert_eq!(c.read_reply(), "OK");
    assert_eq!(c.read_reply(), "v");
    assert_eq!(c.cmd(&["INCR", "n"]), "1");
    assert_eq!(c.cmd(&["INCR", "n"]), "2");
}

// === transactions: MULTI/EXEC ===============================================

#[test]
fn multi_exec_runs_the_batch() {
    let s = Server::start();
    let mut c = s.connect();
    assert_eq!(c.cmd(&["MULTI"]), "OK");
    assert_eq!(c.cmd(&["SET", "k", "v"]), "QUEUED");
    assert_eq!(c.cmd(&["INCR", "n"]), "QUEUED");
    assert_eq!(c.cmd(&["EXEC"]), "[OK, 1]");
    assert_eq!(c.cmd(&["GET", "k"]), "v");
}

#[test]
fn exec_aborts_on_unknown_command() {
    let s = Server::start();
    let mut c = s.connect();
    c.cmd(&["MULTI"]);
    assert!(c.cmd(&["NOTACOMMAND"]).starts_with("-ERR unknown command"));
    // A valid command after the bad one still queues...
    assert_eq!(c.cmd(&["SET", "k", "v"]), "QUEUED");
    // ...but EXEC aborts the whole transaction.
    assert!(c.cmd(&["EXEC"]).starts_with("-EXECABORT"));
    // And the queued SET did NOT run.
    assert_eq!(c.cmd(&["GET", "k"]), "(nil)");
}

#[test]
fn exec_aborts_on_arity_error() {
    let s = Server::start();
    let mut c = s.connect();
    c.cmd(&["MULTI"]);
    assert!(
        c.cmd(&["GET"])
            .starts_with("-ERR wrong number of arguments")
    );
    assert!(c.cmd(&["EXEC"]).starts_with("-EXECABORT"));
}

// === transactions: WATCH ====================================================

#[test]
fn watch_aborts_when_another_client_changes_the_key() {
    let s = Server::start();
    let (mut c1, mut c2) = (s.connect(), s.connect());
    c1.cmd(&["SET", "k", "1"]);
    assert_eq!(c1.cmd(&["WATCH", "k"]), "OK");
    c1.cmd(&["MULTI"]);
    c1.cmd(&["SET", "k", "2"]);
    // Another client modifies the watched key before EXEC.
    assert_eq!(c2.cmd(&["SET", "k", "99"]), "OK");
    // EXEC must abort (null array).
    assert_eq!(c1.cmd(&["EXEC"]), "(nil)");
    assert_eq!(c1.cmd(&["GET", "k"]), "99");
}

#[test]
fn watch_aborts_when_the_key_expires() {
    let s = Server::start();
    let mut c = s.connect();
    c.cmd(&["SET", "k", "v", "PX", "60"]);
    assert_eq!(c.cmd(&["WATCH", "k"]), "OK");
    // Let the key expire; the active reaper (≈100 ms idle tick) should fire and
    // dirty the WATCHer even without any access to the key.
    sleep(Duration::from_millis(400));
    c.cmd(&["MULTI"]);
    c.cmd(&["SET", "k", "v2"]);
    assert_eq!(
        c.cmd(&["EXEC"]),
        "(nil)",
        "expiry of a watched key must abort EXEC"
    );
}

#[test]
fn noop_write_does_not_abort_watch() {
    let s = Server::start();
    let (mut c1, mut c2) = (s.connect(), s.connect());
    // k does not exist; WATCH it.
    assert_eq!(c1.cmd(&["WATCH", "k"]), "OK");
    // A no-op write on the watched key by another client (DEL of a missing key
    // returns :0) must NOT dirty the transaction.
    assert_eq!(c2.cmd(&["DEL", "k"]), "0");
    c1.cmd(&["MULTI"]);
    c1.cmd(&["SET", "k", "v"]);
    assert_eq!(
        c1.cmd(&["EXEC"]),
        "[OK]",
        "a no-op write must not abort WATCH"
    );
}

// === pub/sub ================================================================

#[test]
fn pubsub_delivers_to_subscribers() {
    let s = Server::start();
    let (mut sub, mut pubr) = (s.connect(), s.connect());
    assert_eq!(sub.cmd(&["SUBSCRIBE", "ch"]), "[subscribe, ch, 1]");
    // Give the subscription a moment to register on the hub.
    sleep(Duration::from_millis(50));
    assert_eq!(pubr.cmd(&["PUBLISH", "ch", "hello"]), "1");
    assert_eq!(sub.read_reply(), "[message, ch, hello]");
}

// === blocking XREAD =========================================================

#[test]
fn blocking_xread_wakes_on_xadd() {
    let s = Server::start();
    let (mut reader, mut writer) = (s.connect(), s.connect());
    writer.cmd(&["XADD", "s", "1-1", "f", "v0"]);
    // Park a blocking read for entries after the current tail.
    reader.send(&["XREAD", "BLOCK", "0", "STREAMS", "s", "$"]);
    sleep(Duration::from_millis(100)); // ensure it's parked
    writer.cmd(&["XADD", "s", "2-2", "f", "v1"]);
    let reply = reader.read_reply();
    assert!(
        reply.contains("v1"),
        "blocked XREAD should receive the new entry, got {reply}"
    );
}

// === maxmemory / eviction ===================================================

#[test]
fn maxmemory_evicts_to_stay_bounded() {
    let cap_bytes: u64 = 50 * 1024;
    let s = Server::start_inner(&[("LOCUS_MAXMEMORY", "50kb")]);
    let mut c = s.connect();
    assert_eq!(info_field(&mut c, "maxmemory"), cap_bytes);

    // Write far more than the cap; eviction must make room so writes keep
    // succeeding and used_memory stays near the limit (not 500×500 bytes).
    let value = "x".repeat(500);
    for i in 0..500 {
        assert_eq!(c.cmd(&["SET", &format!("key:{i}"), &value]), "OK");
    }
    let used = info_field(&mut c, "used_memory");
    assert!(
        used <= cap_bytes + 4096,
        "used_memory {used} should stay near the {cap_bytes}-byte cap"
    );
    assert_eq!(c.cmd(&["PING"]), "PONG"); // still responsive
}

// === replication ============================================================

#[test]
fn replica_receives_writes_from_master() {
    let master = Server::start();
    let replica = Server::start();
    let (mut m, mut r) = (master.connect(), replica.connect());
    assert_eq!(
        r.cmd(&["REPLICAOF", "127.0.0.1", &master.port.to_string()]),
        "OK"
    );
    // Wait for the full-sync handshake to complete.
    sleep(Duration::from_millis(400));
    m.cmd(&["SET", "foo", "bar"]);
    // Poll the replica until the write streams through (or time out).
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if r.cmd(&["GET", "foo"]) == "bar" {
            break;
        }
        assert!(Instant::now() < deadline, "write never replicated");
        sleep(Duration::from_millis(50));
    }
    // Replicas reject writes.
    assert!(r.cmd(&["SET", "x", "y"]).starts_with("-READONLY"));
}
