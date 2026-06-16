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

#[test]
fn flushall_aborts_watch() {
    let s = Server::start();
    let (mut c1, mut c2) = (s.connect(), s.connect());
    c1.cmd(&["SET", "k", "1"]);
    assert_eq!(c1.cmd(&["WATCH", "k"]), "OK");
    c1.cmd(&["MULTI"]);
    c1.cmd(&["SET", "k", "2"]);
    // FLUSHALL removes the watched key on another connection.
    assert_eq!(c2.cmd(&["FLUSHALL"]), "OK");
    assert_eq!(
        c1.cmd(&["EXEC"]),
        "(nil)",
        "FLUSHALL of a watched key must abort EXEC"
    );
}

#[test]
fn keys_and_dbsize_over_the_wire() {
    let s = Server::start();
    let mut c = s.connect();
    c.cmd(&["MSET", "a:1", "x", "a:2", "y", "b:1", "z"]);
    assert_eq!(c.cmd(&["DBSIZE"]), "3");
    let keys = c.cmd(&["KEYS", "a:*"]);
    assert!(
        keys.contains("a:1") && keys.contains("a:2") && !keys.contains("b:1"),
        "got {keys}"
    );
}

// === changefeed =============================================================

#[test]
fn changefeed_snapshot_then_live_changes() {
    let s = Server::start();
    let mut w = s.connect();
    w.cmd(&["SET", "user:1", "alice"]);
    w.cmd(&["SET", "other", "x"]); // outside the subscribed prefix

    let mut feed = s.connect();
    feed.send(&["CDCSUBSCRIBE", "user:"]);
    // Atomic snapshot: only user:1, then a done marker with the count.
    let snap = feed.read_reply();
    assert!(
        snap.contains("cdc-snapshot") && snap.contains("user:1") && snap.contains("alice"),
        "snapshot entry: {snap}"
    );
    // snapshot-done now carries the count and the high-water offset (0 = none yet,
    // since the pre-subscribe writes weren't retained on a default server).
    assert_eq!(feed.read_reply(), "[cdc-snapshot-done, 1, 0]");

    // Live change on a matching key is pushed with its new value.
    w.cmd(&["SET", "user:2", "bob"]);
    let chg = feed.read_reply();
    assert!(
        chg.contains("cdc-change")
            && chg.contains("write")
            && chg.contains("user:2")
            && chg.contains("bob"),
        "change: {chg}"
    );

    // A non-matching write is filtered out; the next delivered event is the del.
    w.cmd(&["SET", "other", "y"]);
    w.cmd(&["DEL", "user:1"]);
    let del = feed.read_reply();
    assert!(
        del.contains("cdc-change") && del.contains("del") && del.contains("user:1"),
        "del: {del}"
    );

    // Push mode: a normal data command on the feed connection is rejected.
    assert!(feed.cmd(&["GET", "user:2"]).starts_with("-ERR"));
    // ...but CDCUNSUBSCRIBE leaves push mode and restores normal commands.
    assert_eq!(feed.cmd(&["CDCUNSUBSCRIBE"]), "[cdc-unsubscribe]");
    assert_eq!(feed.cmd(&["GET", "user:2"]), "bob");
}

#[test]
fn changefeed_offsets_retention_and_catchup() {
    let s = Server::start_inner(&[("LOCUS_CDC_MAXLEN", "100")]);
    let mut w = s.connect();
    w.cmd(&["SET", "a", "1"]); // offset 1
    w.cmd(&["SET", "b", "2"]); // offset 2
    w.cmd(&["DEL", "a"]); // offset 3

    let mut rd = s.connect();
    // Full read from 0 returns all retained records in order, with offsets+values.
    assert_eq!(
        rd.cmd(&["CDCREAD", "0"]),
        "[[1, write, a, 1], [2, write, b, 2], [3, del, a, (nil)]]"
    );
    // COUNT limits; PREFIX filters; catch-up reads only what's after an offset.
    assert_eq!(
        rd.cmd(&["CDCREAD", "0", "COUNT", "1"]),
        "[[1, write, a, 1]]"
    );
    assert_eq!(
        rd.cmd(&["CDCREAD", "0", "PREFIX", "b"]),
        "[[2, write, b, 2]]"
    );
    assert_eq!(rd.cmd(&["CDCREAD", "2"]), "[[3, del, a, (nil)]]");
    // Retention disabled on a default server -> error.
    let s2 = Server::start();
    assert!(s2.connect().cmd(&["CDCREAD", "0"]).starts_with("-ERR"));
}

#[test]
fn changefeed_offset_out_of_range_when_truncated() {
    let s = Server::start_inner(&[("LOCUS_CDC_MAXLEN", "2")]);
    let mut w = s.connect();
    for i in 0..5 {
        w.cmd(&["SET", &format!("k{i}"), "v"]); // offsets 1..5, ring keeps last 2 (4,5)
    }
    let mut rd = s.connect();
    // Reading from 0 fails: records 1..3 were dropped (consumer fell behind).
    assert!(
        rd.cmd(&["CDCREAD", "0"])
            .starts_with("-ERR offset out of range")
    );
    // Reading from a still-retained offset works.
    assert_eq!(rd.cmd(&["CDCREAD", "4"]), "[[5, write, k4, v]]");
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

#[test]
fn select_zero_ok_others_rejected() {
    let s = Server::start();
    let mut c = s.connect();
    assert_eq!(c.cmd(&["SELECT", "0"]), "OK");
    assert!(c.cmd(&["SELECT", "1"]).starts_with("-ERR"));
    assert!(c.cmd(&["SELECT", "nope"]).starts_with("-ERR"));
}

// === replication ============================================================

#[test]
fn replica_pointed_at_silent_master_stays_responsive() {
    let replica = Server::start();
    // A "master" that accepts the TCP connection but never replies, so the
    // replica's handshake stalls — the read-timeout must keep it from hanging.
    let silent = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = silent.local_addr().unwrap().port();
    std::thread::spawn(move || {
        let mut held = Vec::new();
        for s in silent.incoming().flatten() {
            held.push(s); // keep connections open, never write
        }
    });
    let mut c = replica.connect();
    assert_eq!(c.cmd(&["REPLICAOF", "127.0.0.1", &port.to_string()]), "OK");
    sleep(Duration::from_millis(200));
    // The replica stays fully responsive despite the stalled handshake; it's in
    // read-only mode, and can be promoted back without hanging.
    assert_eq!(c.cmd(&["PING"]), "PONG");
    assert!(c.cmd(&["SET", "k", "v"]).starts_with("-READONLY"));
    assert_eq!(c.cmd(&["REPLICAOF", "NO", "ONE"]), "OK");
    assert_eq!(c.cmd(&["SET", "k", "v"]), "OK"); // promoted -> writable again
}

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
