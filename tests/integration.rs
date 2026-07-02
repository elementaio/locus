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
use std::sync::atomic::{AtomicU64, Ordering};
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
        // Unique RDB path per spawned server (avoids cross-test interference).
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let rdb = format!(
            "{}/locus-test-{}-{}.rdb",
            std::env::temp_dir().display(),
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed),
        );
        let _ = std::fs::remove_file(&rdb);
        Server::spawn_at(rdb, extra_env)
    }

    /// Spawn against a caller-chosen RDB path WITHOUT clearing it first, so a
    /// second server can read what a first one persisted (restart tests).
    fn start_with_rdb(rdb: &str) -> Server {
        Server::spawn_at(rdb.to_string(), &[])
    }

    fn spawn_at(rdb: String, extra_env: &[(&str, &str)]) -> Server {
        // LOCUS_PORT=0 -> the OS picks a free port; we read the real one back
        // from the server's stdout. No bind-then-drop race between tests.
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_locus"));
        cmd.env("LOCUS_PORT", "0")
            .env("LOCUS_RDB", &rdb)
            .env_remove("LOCUS_AOF")
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        for (k, v) in extra_env {
            cmd.env(k, v);
        }
        let mut child = cmd.spawn().expect("spawn locus");
        let stdout = child.stdout.take().expect("child stdout");
        let mut reader = BufReader::new(stdout);
        // First line: "Locus listening on 127.0.0.1:<port>".
        let port = loop {
            let mut line = String::new();
            if reader.read_line(&mut line).expect("read stdout") == 0 {
                panic!("server exited before it started listening");
            }
            if line.contains("listening")
                && let Some(p) = line
                    .rsplit(':')
                    .next()
                    .and_then(|s| s.trim().parse::<u16>().ok())
            {
                break p;
            }
        };
        // Drain the rest of stdout so the child never blocks on a full pipe.
        std::thread::spawn(move || {
            let mut sink = Vec::new();
            let _ = reader.read_to_end(&mut sink);
        });
        Server { child, port, rdb }
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

    /// Wait (with a timeout) for the server to exit on its own — e.g. after a
    /// SHUTDOWN command or a signal — and return its exit status.
    fn wait_exit(&mut self) -> std::process::ExitStatus {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(status) = self.child.try_wait().expect("try_wait") {
                return status;
            }
            assert!(Instant::now() < deadline, "server did not exit in time");
            sleep(Duration::from_millis(20));
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
            b'*' | b'%' | b'~' | b'>' => {
                let n: i64 = rest.parse().unwrap();
                if n < 0 {
                    return "(nil)".to_string();
                }
                let count = if tag[0] == b'%' { n * 2 } else { n };
                let items: Vec<String> = (0..count).map(|_| self.read_reply()).collect();
                format!("[{}]", items.join(", "))
            }
            b',' => rest,                // RESP3 double
            b'_' => "(nil)".to_string(), // RESP3 null
            b'#' => rest,                // RESP3 boolean (t / f)
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

#[test]
fn conditional_writes_over_the_wire() {
    let s = Server::start();
    let mut c = s.connect();
    c.cmd(&["SET", "lock", "owner-a"]);
    assert_eq!(c.cmd(&["CAS", "lock", "owner-b", "owner-c"]), "0"); // wrong owner
    assert_eq!(c.cmd(&["CAS", "lock", "owner-a", "owner-c"]), "1"); // right owner
    assert_eq!(c.cmd(&["GET", "lock"]), "owner-c");
    // monotonic cursor
    assert_eq!(c.cmd(&["SETMAX", "cursor", "10"]), "1");
    assert_eq!(c.cmd(&["SETMAX", "cursor", "4"]), "0");
    assert_eq!(c.cmd(&["GET", "cursor"]), "10");
    // quota
    assert_eq!(c.cmd(&["INCRCAP", "quota", "1", "2"]), "1");
    assert_eq!(c.cmd(&["INCRCAP", "quota", "1", "2"]), "2");
    assert_eq!(c.cmd(&["INCRCAP", "quota", "1", "2"]), "(nil)"); // capped
}

#[test]
fn bloom_filter_dedup() {
    let s = Server::start();
    let mut c = s.connect();
    assert_eq!(c.cmd(&["BFADD", "seen", "msg-1"]), "1"); // first time
    assert_eq!(c.cmd(&["BFADD", "seen", "msg-1"]), "0"); // duplicate
    assert_eq!(c.cmd(&["BFEXISTS", "seen", "msg-1"]), "1");
    assert_eq!(c.cmd(&["BFEXISTS", "seen", "msg-2"]), "0");
    assert_eq!(c.cmd(&["TYPE", "seen"]), "bloom");
}

#[test]
fn tdigest_percentiles() {
    let s = Server::start();
    let mut c = s.connect();
    // add 1..=1000 in batches
    for start in (1..=1000).step_by(50) {
        let mut args = vec!["TDADD".to_string(), "lat".to_string()];
        for v in start..start + 50 {
            args.push(v.to_string());
        }
        let a: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        assert_eq!(c.cmd(&a), "OK");
    }
    assert_eq!(c.cmd(&["TYPE", "lat"]), "tdigest");
    // exact extremes
    assert_eq!(c.cmd(&["TDQUANTILE", "lat", "0"]), "[1]");
    assert_eq!(c.cmd(&["TDQUANTILE", "lat", "1"]), "[1000]");
    // p99 within tolerance
    let r = c.cmd(&["TDQUANTILE", "lat", "0.99"]); // "[<num>]"
    let num: f64 = r.trim_matches(|ch| ch == '[' || ch == ']').parse().unwrap();
    assert!((num - 990.0).abs() < 20.0, "p99={num}");
}

#[test]
fn topk_heavy_hitters() {
    let s = Server::start();
    let mut c = s.connect();
    assert_eq!(c.cmd(&["TOPKRESERVE", "hh", "2"]), "OK");
    for _ in 0..5 {
        c.cmd(&["TOPKADD", "hh", "rust"]);
    }
    for _ in 0..3 {
        c.cmd(&["TOPKADD", "hh", "go"]);
    }
    c.cmd(&["TOPKADD", "hh", "zig"]); // 1 occurrence — off the k=2 board
    assert_eq!(c.cmd(&["TOPKLIST", "hh"]), "[rust, go]");
    assert_eq!(c.cmd(&["TYPE", "hh"]), "topk");
}

#[test]
fn count_min_trending() {
    let s = Server::start();
    let mut c = s.connect();
    assert_eq!(c.cmd(&["CMSINCRBY", "trend", "rust", "5"]), "[5]");
    assert_eq!(
        c.cmd(&["CMSINCRBY", "trend", "rust", "2", "go", "1"]),
        "[7, 1]"
    );
    assert_eq!(
        c.cmd(&["CMSQUERY", "trend", "rust", "go", "zig"]),
        "[7, 1, 0]"
    );
    assert_eq!(c.cmd(&["TYPE", "trend"]), "cms");
}

#[test]
fn secondary_index_query_by_field() {
    let s = Server::start();
    let mut c = s.connect();
    // objects are hashes
    c.cmd(&["HSET", "u:1", "city", "NYC", "age", "30"]);
    c.cmd(&["HSET", "u:2", "city", "LA", "age", "25"]);
    c.cmd(&["HSET", "u:3", "city", "NYC", "age", "40"]);
    // index existing data by the "city" field, then equality query
    assert_eq!(c.cmd(&["IDXCREATE", "by_city", "city"]), "OK");
    let nyc = c.cmd(&["IDXGET", "by_city", "NYC"]);
    assert!(
        nyc.contains("u:1") && nyc.contains("u:3") && !nyc.contains("u:2"),
        "{nyc}"
    );
    // auto-maintained: a new hash is indexed; a changed value re-buckets
    c.cmd(&["HSET", "u:4", "city", "NYC"]);
    assert!(c.cmd(&["IDXGET", "by_city", "NYC"]).contains("u:4"));
    c.cmd(&["HSET", "u:1", "city", "LA"]); // moves u:1 NYC -> LA
    assert!(!c.cmd(&["IDXGET", "by_city", "NYC"]).contains("u:1"));
    assert!(c.cmd(&["IDXGET", "by_city", "LA"]).contains("u:1"));
    // delete drops it from the index (no drift)
    c.cmd(&["DEL", "u:3"]);
    assert!(!c.cmd(&["IDXGET", "by_city", "NYC"]).contains("u:3"));
    // range query (lexicographic) over a numeric-looking field
    assert_eq!(c.cmd(&["IDXCREATE", "by_age", "age"]), "OK");
    let r = c.cmd(&["IDXRANGE", "by_age", "20", "35"]); // 25 (u:2), 30 (u:1)
    assert!(
        r.contains("u:2") && r.contains("u:1") && !r.contains("u:3"),
        "{r}"
    );
    // errors + lifecycle
    assert!(c.cmd(&["IDXGET", "nope", "x"]).starts_with("-ERR"));
    assert_eq!(c.cmd(&["IDXDROP", "by_city"]), "1");
    assert_eq!(c.cmd(&["IDXDROP", "by_city"]), "0");
}

// === geo ====================================================================

#[test]
fn geo_search_and_distance() {
    let s = Server::start();
    let mut c = s.connect();
    // Palermo & Catania — Redis's canonical example.
    assert_eq!(
        c.cmd(&["GEOSET", "Palermo", "13.361389", "38.115556"]),
        "OK"
    );
    assert_eq!(
        c.cmd(&["GEOSET", "Catania", "15.087269", "37.502669"]),
        "OK"
    );
    assert_eq!(c.cmd(&["TYPE", "Palermo"]), "geo");

    // GEOPOS round-trips the stored coordinates; missing -> nil.
    let pos = c.cmd(&["GEOPOS", "Palermo", "missing"]);
    assert!(
        pos.contains("13.3613") && pos.contains("38.1155") && pos.contains("(nil)"),
        "geopos: {pos}"
    );

    // GEODIST ~166 km (within tolerance of Redis's 166274.1516 m).
    let m: f64 = c.cmd(&["GEODIST", "Palermo", "Catania"]).parse().unwrap();
    assert!((m - 166274.0).abs() < 500.0, "dist m: {m}");
    let km: f64 = c
        .cmd(&["GEODIST", "Palermo", "Catania", "km"])
        .parse()
        .unwrap();
    assert!((km - 166.27).abs() < 1.0, "dist km: {km}");
    assert_eq!(c.cmd(&["GEODIST", "Palermo", "nope"]), "(nil)");

    // GEOSEARCH BYRADIUS: 200km finds both (ASC by distance), 100km only Palermo.
    assert_eq!(
        c.cmd(&[
            "GEOSEARCH",
            "FROMKEY",
            "Palermo",
            "BYRADIUS",
            "200",
            "km",
            "ASC"
        ]),
        "[Palermo, Catania]"
    );
    assert_eq!(
        c.cmd(&[
            "GEOSEARCH",
            "FROMLONLAT",
            "13.361389",
            "38.115556",
            "BYRADIUS",
            "100",
            "km"
        ]),
        "[Palermo]"
    );
    // WITHDIST: closest first, distance in the search unit (km).
    let wd = c.cmd(&[
        "GEOSEARCH",
        "FROMKEY",
        "Palermo",
        "BYRADIUS",
        "200",
        "km",
        "ASC",
        "WITHDIST",
    ]);
    assert!(wd.starts_with("[[Palermo, 0.0000]"), "withdist: {wd}");
    // BYBOX (400km square) covers both.
    let bx = c.cmd(&[
        "GEOSEARCH",
        "FROMKEY",
        "Palermo",
        "BYBOX",
        "400",
        "400",
        "km",
        "ASC",
    ]);
    assert!(
        bx.contains("Palermo") && bx.contains("Catania"),
        "bybox: {bx}"
    );

    // WRONGTYPE on a non-geo key.
    c.cmd(&["SET", "str", "x"]);
    assert!(
        c.cmd(&["GEODIST", "str", "Palermo"])
            .starts_with("-WRONGTYPE")
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

#[test]
fn changefeed_consumer_groups() {
    let s = Server::start_inner(&[("LOCUS_CDC_MAXLEN", "100")]);
    let mut w = s.connect();
    w.cmd(&["SET", "a", "1"]); // offset 1
    w.cmd(&["SET", "b", "2"]); // offset 2
    w.cmd(&["SET", "c", "3"]); // offset 3

    let mut g = s.connect();
    assert_eq!(g.cmd(&["CDCGROUP", "CREATE", "grp", "0"]), "OK"); // from start
    // Load-balanced: c1 takes the first two, c2 takes the next — disjoint.
    assert_eq!(
        g.cmd(&["CDCREADGROUP", "grp", "c1", "COUNT", "2"]),
        "[[1, write, a, 1], [2, write, b, 2]]"
    );
    assert_eq!(g.cmd(&["CDCREADGROUP", "grp", "c2"]), "[[3, write, c, 3]]");
    assert_eq!(g.cmd(&["CDCREADGROUP", "grp", "c1"]), "[]"); // nothing new

    // 3 delivered + unacked.
    assert!(g.cmd(&["CDCPENDING", "grp"]).starts_with("[3,"));
    assert_eq!(g.cmd(&["CDCACK", "grp", "1", "2"]), "2");
    assert!(g.cmd(&["CDCPENDING", "grp"]).starts_with("[1,"));

    // A new write is delivered to whoever reads next.
    w.cmd(&["SET", "d", "4"]); // offset 4
    assert_eq!(g.cmd(&["CDCREADGROUP", "grp", "c2"]), "[[4, write, d, 4]]");

    // Errors + lifecycle.
    assert!(
        g.cmd(&["CDCREADGROUP", "nope", "c1"])
            .starts_with("-NOGROUP")
    );
    assert!(
        g.cmd(&["CDCGROUP", "CREATE", "grp"])
            .starts_with("-BUSYGROUP")
    );
    assert_eq!(g.cmd(&["CDCGROUP", "DESTROY", "grp"]), "1");
    assert_eq!(g.cmd(&["CDCGROUP", "DESTROY", "grp"]), "0");
}

#[test]
fn changefeed_region_rejects_invalid_geometry() {
    let s = Server::start();
    let mut c = s.connect();
    // A non-positive or non-finite radius (and out-of-range lon/lat) matches
    // nothing and used to be accepted, silently never firing.
    assert!(
        c.cmd(&["CDCSUBSCRIBE", "REGION", "0", "0", "0", "km"])
            .starts_with("-ERR")
    );
    assert!(
        c.cmd(&["CDCSUBSCRIBE", "REGION", "0", "0", "-5", "km"])
            .starts_with("-ERR")
    );
    assert!(
        c.cmd(&["CDCSUBSCRIBE", "REGION", "0", "0", "nan", "km"])
            .starts_with("-ERR")
    );
    assert!(
        c.cmd(&["CDCSUBSCRIBE", "REGION", "999", "0", "5", "km"])
            .starts_with("-ERR")
    );
    // A valid one is accepted (snapshot-done marker).
    let ok = c.cmd(&["CDCSUBSCRIBE", "REGION", "0", "0", "5", "km"]);
    assert!(
        ok.contains("cdc-snapshot-done"),
        "valid region rejected: {ok}"
    );
}

#[test]
fn changefeed_region_geofencing() {
    let s = Server::start();
    let mut w = s.connect();
    w.cmd(&["GEOSET", "in1", "0.0", "0.0"]); // at the region center
    w.cmd(&["GEOSET", "out1", "10.0", "10.0"]); // ~1500 km away

    let mut feed = s.connect();
    feed.send(&["CDCSUBSCRIBE", "REGION", "0", "0", "50", "km"]);
    // Snapshot: only the in-region object, then the done marker.
    let snap = feed.read_reply();
    assert!(
        snap.contains("cdc-snapshot") && snap.contains("in1"),
        "snap: {snap}"
    );
    assert_eq!(feed.read_reply(), "[cdc-snapshot-done, 1, 0]");

    // A new object enters the circle -> write.
    w.cmd(&["GEOSET", "in2", "0.1", "0.1"]); // ~15 km from center
    let enter = feed.read_reply();
    assert!(
        enter.contains("cdc-change") && enter.contains("write") && enter.contains("in2"),
        "enter: {enter}"
    );

    // A change outside the region is filtered out; moving in1 away -> leave (del).
    w.cmd(&["GEOSET", "out2", "20", "20"]); // outside -> no message
    w.cmd(&["GEOSET", "in1", "30", "30"]); // was inside, now far -> leave
    let leave = feed.read_reply();
    assert!(
        leave.contains("cdc-change") && leave.contains("del") && leave.contains("in1"),
        "leave: {leave}"
    );

    // Deleting an in-region object -> leave (del).
    w.cmd(&["DEL", "in2"]);
    let del = feed.read_reply();
    assert!(del.contains("del") && del.contains("in2"), "del: {del}");
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

// === resource limits (output buffer / query buffer / hub backpressure) ======

#[test]
fn slow_pubsub_consumer_is_disconnected_not_oom() {
    // Tiny pubsub output cap: a subscriber that stops reading must be dropped
    // once its queued bytes exceed the cap, instead of growing server memory.
    let s = Server::start_inner(&[("LOCUS_OUTBUF_PUBSUB", "64kb")]);
    let mut stalled = s.connect();
    assert_eq!(
        stalled.cmd(&["SUBSCRIBE", "flood"]),
        "[subscribe, flood, 1]"
    );
    // From here on the subscriber never reads — its queue can only grow.

    let mut publisher = s.connect();
    let payload = "x".repeat(4096);
    for _ in 0..200 {
        // Publishing must keep succeeding regardless of the stalled subscriber.
        let n = publisher.cmd(&["PUBLISH", "flood", &payload]);
        assert!(n == "0" || n == "1", "PUBLISH failed: {n}");
    }
    // The hub shuts the slow consumer's socket; once its reader files the
    // disconnect, the channel has no subscribers left.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let numsub = publisher.cmd(&["PUBSUB", "NUMSUB", "flood"]);
        if numsub.contains(", 0") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "slow consumer never dropped: {numsub}"
        );
        sleep(Duration::from_millis(50));
    }
    assert_eq!(publisher.cmd(&["PING"]), "PONG"); // server healthy throughout
}

#[test]
fn query_buffer_limit_closes_oversized_command() {
    // A client dribbling one huge command may buffer at most the query-buffer
    // limit; past it the server replies with a protocol error and disconnects.
    let s = Server::start_inner(&[("LOCUS_QUERYBUF_LIMIT", "128kb")]);
    let mut c = s.connect();
    let mut buf = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$524288\r\n".to_vec(); // declares 512 KiB
    buf.extend(vec![b'x'; 200_000]); // over the 128 KiB cap, still incomplete
    c.stream.write_all(&buf).unwrap();
    let reply = c.read_reply();
    assert!(
        reply.contains("query buffer"),
        "expected query-buffer error, got: {reply}"
    );
    // The connection is then closed (EOF, possibly after a reset).
    let mut tmp = [0u8; 16];
    let n = c.stream.read(&mut tmp).unwrap_or(0);
    assert_eq!(n, 0, "connection should be closed after the limit error");
    // And the server is unharmed.
    let mut c2 = s.connect();
    assert_eq!(c2.cmd(&["PING"]), "PONG");
}

#[test]
fn bounded_hub_queue_backpressures_pipeline_without_loss() {
    // With a tiny hub input queue, a huge one-shot pipeline must still execute
    // fully and in order — producers block (backpressure), nothing is dropped.
    let s = Server::start_inner(&[("LOCUS_HUB_QUEUE", "64")]);
    let mut c = s.connect();
    let mut buf = Vec::new();
    for i in 0..10_000 {
        let (k, v) = (format!("k{i}"), format!("v{i}"));
        buf.extend_from_slice(
            format!(
                "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                k.len(),
                k,
                v.len(),
                v
            )
            .as_bytes(),
        );
    }
    c.stream.write_all(&buf).unwrap();
    for i in 0..10_000 {
        assert_eq!(c.read_reply(), "OK", "SET #{i} failed");
    }
    assert_eq!(c.cmd(&["GET", "k9999"]), "v9999");
}

#[test]
fn maintenance_runs_under_sustained_load() {
    // XREAD BLOCK deadlines and active TTL expiry are driven by the hub's
    // maintenance sweep. A steady command stream must not starve it (the old
    // shape only swept when the queue went idle for 100ms — never, under load).
    let s = Server::start();
    let mut blocked = s.connect();
    blocked.cmd(&["XADD", "st", "1-1", "f", "v"]);
    blocked.send(&["XREAD", "BLOCK", "300", "STREAMS", "st", "$"]);

    // Saturate the hub with cheap commands from another connection for ~1.2s.
    let mut busy = s.connect();
    busy.cmd(&["SET", "ttl-key", "v"]);
    busy.cmd(&["PEXPIRE", "ttl-key", "150"]);
    let start = Instant::now();
    while start.elapsed() < Duration::from_millis(1200) {
        busy.send(&["PING"]);
        busy.read_reply();
    }
    // The parked reader must have been timed out near its 300ms deadline
    // (its nil reply is already waiting), despite the hub never going idle.
    let reply = blocked.read_reply();
    assert_eq!(reply, "(nil)", "BLOCK deadline starved: {reply}");
    // And the TTL key must have been actively reaped meanwhile (EXISTS would
    // also passively delete it — used_memory-level reap is what we saturated).
    assert_eq!(busy.cmd(&["EXISTS", "ttl-key"]), "0");
}

#[test]
fn aof_write_status_is_reported_in_info() {
    // The INFO plumbing for AOF health (drives the MISCONF write gate and the
    // recovery rewrite; the failure path itself needs fault injection).
    let aof = format!(
        "{}/locus-aof-status-{}.aof",
        std::env::temp_dir().display(),
        std::process::id()
    );
    let _ = std::fs::remove_file(&aof);
    let s = Server::start_inner(&[("LOCUS_AOF", aof.as_str())]);
    let mut c = s.connect();
    assert_eq!(c.cmd(&["SET", "a", "1"]), "OK");
    let info = c.cmd(&["INFO"]);
    assert!(
        info.contains("aof_enabled:1") && info.contains("aof_last_write_status:ok"),
        "AOF health missing from INFO"
    );
    drop(s);
    let _ = std::fs::remove_file(&aof);
}

// === disk tier ==============================================================

/// Remove a test's tier segment files ({base}.NNNNNN).
fn cleanup_tier(base: &str) {
    if let Some(dir) = std::path::Path::new(base).parent()
        && let Ok(entries) = std::fs::read_dir(dir)
    {
        let prefix = format!(
            "{}.",
            std::path::Path::new(base)
                .file_name()
                .unwrap()
                .to_string_lossy()
        );
        for e in entries.flatten() {
            if e.file_name().to_string_lossy().starts_with(&prefix) {
                let _ = std::fs::remove_file(e.path());
            }
        }
    }
}

#[test]
fn disk_tier_thaws_reads_and_survives_restart() {
    let rdb = format!(
        "{}/locus-tier-int-{}.rdb",
        std::env::temp_dir().display(),
        std::process::id()
    );
    let tier_base = format!("{rdb}.tier");
    let _ = std::fs::remove_file(&rdb);
    cleanup_tier(&tier_base);
    let env: &[(&str, &str)] = &[("LOCUS_TIER", "1")];

    let mut s = Server::spawn_at(rdb.clone(), env);
    let mut c = s.connect();
    let big = "x".repeat(50_000);
    c.cmd(&["SET", "archive", &big]);
    c.cmd(&["SET", "hot", "stay"]);
    let before = info_field(&mut c, "used_memory");

    // TIER moves the value to disk; RAM drops; TYPE/EXISTS stay disk-free.
    assert_eq!(c.cmd(&["TIER", "archive"]), "1");
    assert_eq!(c.cmd(&["TIER", "archive"]), "1"); // idempotent
    assert_eq!(c.cmd(&["TIER", "missing"]), "0");
    assert!(info_field(&mut c, "used_memory") < before - 40_000);
    assert_eq!(info_field(&mut c, "tier_keys"), 1);
    assert_eq!(c.cmd(&["TYPE", "archive"]), "string");
    assert_eq!(c.cmd(&["EXISTS", "archive"]), "1");

    // A read transparently thaws it.
    assert_eq!(c.cmd(&["GET", "archive"]), big);
    assert_eq!(info_field(&mut c, "tier_keys"), 0);

    // Tier again, restart gracefully (RDB carries the stub), thaw after boot.
    assert_eq!(c.cmd(&["TIER", "archive"]), "1");
    c.send(&["SHUTDOWN"]);
    s.wait_exit();
    let s2 = Server::spawn_at(rdb.clone(), env);
    let mut c2 = s2.connect();
    assert_eq!(info_field(&mut c2, "tier_keys"), 1, "stub survives restart");
    assert_eq!(
        c2.cmd(&["GET", "archive"]),
        big,
        "thaw from the log after boot"
    );
    assert_eq!(c2.cmd(&["GET", "hot"]), "stay");
    assert_eq!(info_field(&mut c2, "tier_lost"), 0);
    drop(s2);
    cleanup_tier(&tier_base);
}

#[test]
fn disk_tier_survives_kill9_with_aof_and_rewrite() {
    let tmp = std::env::temp_dir();
    let pid = std::process::id();
    let rdb = format!("{}/locus-tier-aof-{pid}.rdb", tmp.display());
    let aof = format!("{}/locus-tier-aof-{pid}.aof", tmp.display());
    let tier_base = format!("{rdb}.tier");
    for f in [&rdb, &aof] {
        let _ = std::fs::remove_file(f);
    }
    cleanup_tier(&tier_base);
    let env: &[(&str, &str)] = &[("LOCUS_TIER", "1"), ("LOCUS_AOF", aof.as_str())];

    let mut s = Server::spawn_at(rdb.clone(), env);
    let mut c = s.connect();
    let big = "y".repeat(30_000);
    c.cmd(&["SET", "cold", &big]);
    assert_eq!(c.cmd(&["TIER", "cold"]), "1");
    // Rewrite folds the stub as a TIERREF (a local value-log reference).
    c.cmd(&["BGREWRITEAOF"]);
    sleep(Duration::from_millis(300));
    c.cmd(&["SET", "after", "rewrite"]);

    s.child.kill().unwrap(); // ungraceful — AOF replay must rebuild everything
    let _ = s.child.wait();
    let s2 = Server::spawn_at(rdb.clone(), env);
    let mut c2 = s2.connect();
    assert_eq!(
        info_field(&mut c2, "tier_keys"),
        1,
        "TIERREF replay restores the stub"
    );
    assert_eq!(c2.cmd(&["GET", "cold"]), big);
    assert_eq!(c2.cmd(&["GET", "after"]), "rewrite");
    assert_eq!(info_field(&mut c2, "tier_lost"), 0);
    drop(s2);
    let _ = std::fs::remove_file(&aof);
    cleanup_tier(&tier_base);
}

#[test]
fn disk_tier_replicates_as_a_command() {
    // TIER replicates AS the command: each node tiers into its OWN local log
    // (stubs never cross the wire; a full sync ships full values).
    let m = Server::start_inner(&[("LOCUS_TIER", "1")]);
    let r = Server::start_inner(&[("LOCUS_TIER", "1")]);
    let (mut cm, mut cr) = (m.connect(), r.connect());
    cm.cmd(&["SET", "seed", "before-sync"]);
    assert_eq!(cm.cmd(&["TIER", "seed"]), "1"); // tiered BEFORE the replica joins
    cr.cmd(&["REPLICAOF", "127.0.0.1", &m.port.to_string()]);
    wait_info(&mut cr, "master_link_status", "up", "never synced");
    // The full sync shipped the FULL value (a stub is meaningless off-node).
    assert_eq!(cr.cmd(&["GET", "seed"]), "before-sync");

    cm.cmd(&["SET", "arch", "tier-me"]);
    let deadline = Instant::now() + Duration::from_secs(3);
    while cr.cmd(&["GET", "arch"]) != "tier-me" {
        assert!(Instant::now() < deadline, "write never replicated");
        sleep(Duration::from_millis(30));
    }
    assert_eq!(cm.cmd(&["TIER", "arch"]), "1");
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if info_field(&mut cr, "tier_keys") == 1 {
            break; // the replica tiered its own copy into its own log
        }
        assert!(Instant::now() < deadline, "TIER never replicated");
        sleep(Duration::from_millis(30));
    }
    assert_eq!(cr.cmd(&["GET", "arch"]), "tier-me"); // thaw on the replica
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

// === auth + graceful shutdown ===============================================

#[test]
fn auth_required_blocks_until_authenticated() {
    let s = Server::start_inner(&[("LOCUS_REQUIREPASS", "s3cret")]);
    let mut c = s.connect();
    // Unauthenticated: every data command is refused with NOAUTH.
    assert!(c.cmd(&["GET", "k"]).starts_with("-NOAUTH"));
    assert!(c.cmd(&["SET", "k", "v"]).starts_with("-NOAUTH"));
    // A wrong password is rejected and does NOT authenticate the connection.
    assert!(c.cmd(&["AUTH", "wrong"]).starts_with("-WRONGPASS"));
    assert!(c.cmd(&["GET", "k"]).starts_with("-NOAUTH"));
    // The right password unlocks the connection.
    assert_eq!(c.cmd(&["AUTH", "s3cret"]), "OK");
    assert_eq!(c.cmd(&["SET", "k", "v"]), "OK");
    assert_eq!(c.cmd(&["GET", "k"]), "v");
    // The `AUTH default <pass>` form works too, on a fresh connection.
    let mut c2 = s.connect();
    assert_eq!(c2.cmd(&["AUTH", "default", "s3cret"]), "OK");
    assert_eq!(c2.cmd(&["GET", "k"]), "v");
}

#[test]
fn hello_auth_authenticates_on_connect() {
    let s = Server::start_inner(&[("LOCUS_REQUIREPASS", "pw")]);
    let mut c = s.connect();
    // A bare HELLO while unauthenticated is refused (no server-info leak).
    assert!(c.cmd(&["HELLO", "3"]).starts_with("-NOAUTH"));
    // HELLO with an AUTH clause authenticates and returns the server map.
    let reply = c.cmd(&["HELLO", "3", "AUTH", "default", "pw"]);
    assert!(reply.contains("proto"), "expected HELLO map, got {reply}");
    // ...and the connection is now usable.
    assert_eq!(c.cmd(&["SET", "k", "v"]), "OK");
    // A wrong password in HELLO is rejected and does not upgrade the connection.
    let mut c2 = s.connect();
    assert!(
        c2.cmd(&["HELLO", "3", "AUTH", "default", "nope"])
            .starts_with("-WRONGPASS")
    );
}

#[test]
fn auth_with_no_password_set_is_an_error() {
    let s = Server::start();
    let mut c = s.connect();
    // No requirepass configured: AUTH has nothing to check against.
    assert!(c.cmd(&["AUTH", "whatever"]).starts_with("-ERR"));
    // ...and commands work without any AUTH (loopback, no protected-mode block).
    assert_eq!(c.cmd(&["SET", "k", "v"]), "OK");
}

#[test]
fn shutdown_command_persists_and_exits_cleanly() {
    let mut s = Server::start();
    let mut c = s.connect();
    assert_eq!(c.cmd(&["SET", "k", "v"]), "OK");
    // SHUTDOWN drains, fsyncs, writes a final snapshot, and exits 0.
    c.send(&["SHUTDOWN"]);
    assert!(
        s.wait_exit().success(),
        "graceful shutdown should exit with status 0"
    );
}

#[test]
fn bgsave_is_async_and_writes_the_snapshot() {
    let s = Server::start();
    let mut c = s.connect();
    assert_eq!(c.cmd(&["SET", "persisted", "yes"]), "OK");
    assert_eq!(c.cmd(&["BGSAVE"]), "Background saving started");
    // The hub stays responsive (the write+fsync ran off-thread, not inline)...
    assert_eq!(c.cmd(&["PING"]), "PONG");
    // ...and the snapshot lands on disk shortly after.
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if std::fs::metadata(&s.rdb)
            .map(|m| m.len() > 0)
            .unwrap_or(false)
        {
            break;
        }
        assert!(Instant::now() < deadline, "BGSAVE never wrote the snapshot");
        sleep(Duration::from_millis(20));
    }
}

#[test]
fn secondary_index_survives_restart() {
    let rdb = format!(
        "{}/locus-idx-restart-{}.rdb",
        std::env::temp_dir().display(),
        std::process::id()
    );
    let _ = std::fs::remove_file(&rdb);
    // Round 1: hashes + an index, then SAVE (and a graceful SHUTDOWN, which also
    // persists), so the index definition lands in the snapshot trailer.
    {
        let mut s = Server::start_with_rdb(&rdb);
        let mut c = s.connect();
        c.cmd(&["HSET", "user:1", "city", "doha"]);
        c.cmd(&["HSET", "user:2", "city", "doha"]);
        c.cmd(&["IDXCREATE", "by_city", "city"]);
        assert_eq!(c.cmd(&["SAVE"]), "OK");
        c.send(&["SHUTDOWN"]);
        assert!(s.wait_exit().success());
        std::mem::forget(s); // keep the RDB on disk for round 2
    }
    // Round 2: a fresh server on the same RDB rebuilt the index from its stored
    // definition — IDXGET works without re-running IDXCREATE.
    {
        let s = Server::start_with_rdb(&rdb);
        let mut c = s.connect();
        let r = c.cmd(&["IDXGET", "by_city", "doha"]);
        assert!(
            r.contains("user:1") && r.contains("user:2"),
            "index not restored across restart: {r}"
        );
    }
    let _ = std::fs::remove_file(&rdb);
}

#[test]
fn hlc_stamps_survive_restart() {
    let rdb = format!(
        "{}/locus-hlc-restart-{}.rdb",
        std::env::temp_dir().display(),
        std::process::id()
    );
    let _ = std::fs::remove_file(&rdb);
    let cdc_env = [("LOCUS_CDC_MAXLEN", "100")];
    // Round 1: make changes, capture the HLC-ordered merged feed, persist.
    let before = {
        let mut s = Server::spawn_at(rdb.clone(), &cdc_env);
        let mut c = s.connect();
        c.cmd(&["SET", "a", "1"]);
        c.cmd(&["SET", "b", "2"]);
        let feed = c.cmd(&["CLUSTER", "CDCMERGE", "0", "COUNT", "10"]);
        assert!(feed.contains('a') && feed.contains('b'), "feed: {feed}");
        assert_eq!(c.cmd(&["SAVE"]), "OK");
        c.send(&["SHUTDOWN"]);
        assert!(s.wait_exit().success());
        std::mem::forget(s); // keep the RDB for round 2
        feed
    };
    // Round 2: a fresh server on the same RDB renders an identical feed — HLC
    // stamps were persisted (had they reset to 0, the hlc fields would differ).
    {
        let s = Server::spawn_at(rdb.clone(), &cdc_env);
        let mut c = s.connect();
        let after = c.cmd(&["CLUSTER", "CDCMERGE", "0", "COUNT", "10"]);
        assert_eq!(before, after, "HLC-ordered feed changed across restart");
    }
    let _ = std::fs::remove_file(&rdb);
}

#[test]
fn aof_recovers_all_acked_writes_after_kill9() {
    let rdb = format!(
        "{}/locus-crash-{}.rdb",
        std::env::temp_dir().display(),
        std::process::id()
    );
    let aof = format!(
        "{}/locus-crash-{}.aof",
        std::env::temp_dir().display(),
        std::process::id()
    );
    let _ = std::fs::remove_file(&rdb);
    let _ = std::fs::remove_file(&aof);
    // Round 1: AOF on; write 50 keys, then SIGKILL with no graceful shutdown.
    // Each acked SET was write_all'd to the AOF (into the kernel), so a process
    // kill (not a power loss) must lose none of them.
    {
        let mut s = Server::spawn_at(rdb.clone(), &[("LOCUS_AOF", aof.as_str())]);
        let mut c = s.connect();
        for i in 0..50 {
            assert_eq!(c.cmd(&["SET", &format!("k{i}"), &format!("v{i}")]), "OK");
        }
        s.child.kill().unwrap(); // SIGKILL
        let _ = s.child.wait();
    }
    // Round 2: restart on the same AOF — every write replays, uncorrupted, with
    // no extra or missing keys.
    {
        let mut s = Server::spawn_at(rdb.clone(), &[("LOCUS_AOF", aof.as_str())]);
        let mut c = s.connect();
        for i in 0..50 {
            assert_eq!(c.cmd(&["GET", &format!("k{i}")]), format!("v{i}"));
        }
        assert_eq!(c.cmd(&["DBSIZE"]), "50");
        let _ = s.child.kill();
        let _ = s.child.wait();
    }
    let _ = std::fs::remove_file(&rdb);
    let _ = std::fs::remove_file(&aof);
}

#[test]
fn replica_authenticates_to_a_password_protected_master() {
    let master = Server::start_inner(&[("LOCUS_REQUIREPASS", "mpw")]);
    let replica = Server::start_inner(&[("LOCUS_MASTERAUTH", "mpw")]);
    let (mut m, mut r) = (master.connect(), replica.connect());
    // Our admin connection must AUTH to the master before it can write.
    assert_eq!(m.cmd(&["AUTH", "mpw"]), "OK");
    assert_eq!(
        r.cmd(&["REPLICAOF", "127.0.0.1", &master.port.to_string()]),
        "OK"
    );
    sleep(Duration::from_millis(400));
    m.cmd(&["SET", "foo", "bar"]);
    // The replica AUTHed with the right masterauth, so the write streams through.
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if r.cmd(&["GET", "foo"]) == "bar" {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "write never replicated with masterauth"
        );
        sleep(Duration::from_millis(50));
    }
}

#[test]
fn replica_with_wrong_masterauth_never_syncs() {
    let master = Server::start_inner(&[("LOCUS_REQUIREPASS", "mpw")]);
    let replica = Server::start_inner(&[("LOCUS_MASTERAUTH", "wrong")]);
    let (mut m, mut r) = (master.connect(), replica.connect());
    assert_eq!(m.cmd(&["AUTH", "mpw"]), "OK");
    assert_eq!(
        r.cmd(&["REPLICAOF", "127.0.0.1", &master.port.to_string()]),
        "OK"
    );
    m.cmd(&["SET", "foo", "bar"]);
    // Wrong masterauth: the master rejects AUTH, so no snapshot or stream is ever
    // shipped — the dataset is not siphoned and the value never appears.
    sleep(Duration::from_millis(700));
    assert_eq!(r.cmd(&["GET", "foo"]), "(nil)");
}

#[test]
fn replica_gets_secondary_index_from_full_sync() {
    let master = Server::start();
    let replica = Server::start();
    let mut m = master.connect();
    m.cmd(&["HSET", "u:1", "city", "doha"]);
    m.cmd(&["IDXCREATE", "by_city", "city"]);
    let mut r = replica.connect();
    assert_eq!(
        r.cmd(&["REPLICAOF", "127.0.0.1", &master.port.to_string()]),
        "OK"
    );
    // The snapshot trailer carries the index definition; the replica rebuilds it
    // from the replicated keyspace, so IDXGET works without re-running IDXCREATE.
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if r.cmd(&["IDXGET", "by_city", "doha"]).contains("u:1") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "index not replicated via full-sync"
        );
        sleep(Duration::from_millis(50));
    }
}

// === CONFIG / INFO ==========================================================

#[test]
fn config_get_returns_live_values_and_set_applies() {
    let s = Server::start();
    let mut c = s.connect();
    assert_eq!(c.cmd(&["CONFIG", "GET", "appendonly"]), "[appendonly, no]");
    assert_eq!(c.cmd(&["CONFIG", "GET", "maxmemory"]), "[maxmemory, 0]");
    // Glob matches multiple params.
    let g = c.cmd(&["CONFIG", "GET", "maxmemory*"]);
    assert!(
        g.contains("maxmemory") && g.contains("maxmemory-policy"),
        "glob CONFIG GET: {g}"
    );
    // CONFIG SET applies live and reads back.
    assert_eq!(c.cmd(&["CONFIG", "SET", "maxmemory", "100mb"]), "OK");
    assert_eq!(
        c.cmd(&["CONFIG", "GET", "maxmemory"]),
        format!("[maxmemory, {}]", 100 * 1024 * 1024)
    );
}

#[test]
fn info_has_standard_sections() {
    let s = Server::start();
    let mut c = s.connect();
    c.cmd(&["SET", "k", "v"]);
    let info = c.cmd(&["INFO"]);
    for needle in [
        "# Server",
        "redis_version:",
        "# Clients",
        "connected_clients:",
        "# Memory",
        "used_memory:",
        "# Persistence",
        "# Stats",
        "total_commands_processed:",
        "# Replication",
        "role:master",
        "# Keyspace",
        "db0:keys=1",
    ] {
        assert!(info.contains(needle), "INFO missing {needle:?}:\n{info}");
    }
}

#[test]
fn getex_object_and_client_verbs() {
    let s = Server::start();
    let mut c = s.connect();
    c.cmd(&["SET", "k", "v"]);
    // GETEX returns the value and can set then clear the TTL.
    assert_eq!(c.cmd(&["GETEX", "k"]), "v");
    assert_eq!(c.cmd(&["GETEX", "k", "EX", "1000"]), "v");
    assert!(c.cmd(&["TTL", "k"]).parse::<i64>().unwrap() > 0);
    assert_eq!(c.cmd(&["GETEX", "k", "PERSIST"]), "v");
    assert_eq!(c.cmd(&["TTL", "k"]), "-1");
    // OBJECT ENCODING reports a plausible per-type encoding.
    assert_eq!(c.cmd(&["OBJECT", "ENCODING", "k"]), "raw");
    c.cmd(&["RPUSH", "l", "a"]);
    assert_eq!(c.cmd(&["OBJECT", "ENCODING", "l"]), "listpack");
    // CLIENT ID / SETNAME / GETNAME / SETINFO.
    assert!(c.cmd(&["CLIENT", "ID"]).parse::<i64>().is_ok());
    assert_eq!(c.cmd(&["CLIENT", "SETNAME", "app1"]), "OK");
    assert_eq!(c.cmd(&["CLIENT", "GETNAME"]), "app1");
    assert_eq!(c.cmd(&["CLIENT", "SETINFO", "lib-name", "ioredis"]), "OK");
}

#[test]
fn slowlog_records_and_resets() {
    // Threshold 0 -> every command is logged, so the test is deterministic.
    let s = Server::start_inner(&[("LOCUS_SLOWLOG_US", "0")]);
    let mut c = s.connect();
    c.cmd(&["SET", "k", "v"]);
    c.cmd(&["GET", "k"]);
    let len: i64 = c.cmd(&["SLOWLOG", "LEN"]).parse().unwrap();
    assert!(len >= 2, "slowlog len {len}");
    let got = c.cmd(&["SLOWLOG", "GET", "1"]);
    assert!(got.starts_with("[["), "slowlog get: {got}"); // array of entry-arrays
    assert_eq!(c.cmd(&["SLOWLOG", "RESET"]), "OK");
    // Only commands issued after RESET remain.
    let after: i64 = c.cmd(&["SLOWLOG", "LEN"]).parse().unwrap();
    assert!(after <= 1, "slowlog len after reset: {after}");
}

#[test]
fn resp3_typed_replies() {
    let s = Server::start();
    let mut c = s.connect();
    assert!(c.cmd(&["HELLO", "3"]).contains("proto")); // negotiate RESP3
    c.cmd(&["HSET", "h", "f", "v"]);
    c.cmd(&["SADD", "st", "a"]);
    c.cmd(&["ZADD", "z", "1.5", "m"]);
    assert_eq!(c.cmd(&["HGETALL", "h"]), "[f, v]"); // map (%)
    assert_eq!(c.cmd(&["SMEMBERS", "st"]), "[a]"); // set (~)
    assert_eq!(c.cmd(&["ZSCORE", "z", "m"]), "1.5"); // double (,)
    // CONFIG GET is a map in RESP3 too.
    assert_eq!(c.cmd(&["CONFIG", "GET", "appendonly"]), "[appendonly, no]");
    // Set-ops return RESP3 set frames (~); scores return doubles (,).
    c.cmd(&["SADD", "s1", "a", "b", "c"]);
    c.cmd(&["SADD", "s2", "b", "c", "d"]);
    let inter = c.cmd(&["SINTER", "s1", "s2"]); // ~ set, order-independent
    assert!(inter.contains('b') && inter.contains('c') && !inter.contains('a'));
    assert_eq!(c.cmd(&["ZINCRBY", "z", "0.5", "m"]), "2"); // double (,)
    assert_eq!(c.cmd(&["ZMSCORE", "z", "m", "absent"]), "[2, (nil)]"); // doubles + null
}

#[test]
fn acl_user_least_privilege() {
    let s = Server::start();
    let mut admin = s.connect(); // default user, unrestricted (open mode)
    // A read-only user scoped to the app:* keys.
    assert_eq!(
        admin.cmd(&["ACL", "SETUSER", "alice", "on", ">pw", "+@read", "~app:"]),
        "OK"
    );
    admin.cmd(&["SET", "app:k", "v"]);
    admin.cmd(&["SET", "other", "v"]);

    let mut a = s.connect();
    assert_eq!(a.cmd(&["AUTH", "alice", "pw"]), "OK");
    assert_eq!(a.cmd(&["GET", "app:k"]), "v"); // read inside the prefix: allowed
    assert!(a.cmd(&["SET", "app:k", "x"]).starts_with("-NOPERM")); // write: no @write
    assert!(a.cmd(&["GET", "other"]).starts_with("-NOPERM")); // key outside prefix

    // Wrong password is rejected.
    let mut b = s.connect();
    assert!(b.cmd(&["AUTH", "alice", "wrong"]).starts_with("-WRONGPASS"));

    // The default user is unrestricted; introspection works.
    assert_eq!(admin.cmd(&["ACL", "WHOAMI"]), "default");
    assert!(admin.cmd(&["ACL", "USERS"]).contains("alice"));
}

#[test]
fn acl_checks_every_key_not_just_the_first() {
    let s = Server::start();
    let mut admin = s.connect();
    assert_eq!(
        admin.cmd(&[
            "ACL", "SETUSER", "bob", "on", ">pw", "+@read", "+@write", "~app:"
        ]),
        "OK"
    );
    admin.cmd(&["SET", "secret:x", "s3cr3t"]);

    let mut b = s.connect();
    assert_eq!(b.cmd(&["AUTH", "bob", "pw"]), "OK");
    // Every key past the first used to be unchecked — cross-tenant write/read.
    assert!(
        b.cmd(&["MSET", "app:a", "1", "secret:y", "2"])
            .starts_with("-NOPERM")
    );
    assert!(b.cmd(&["MGET", "app:a", "secret:x"]).starts_with("-NOPERM"));
    assert!(
        b.cmd(&["RENAME", "app:a", "secret:x"])
            .starts_with("-NOPERM")
    );
    assert!(b.cmd(&["DEL", "app:a", "secret:x"]).starts_with("-NOPERM"));
    assert!(
        b.cmd(&["SINTERSTORE", "app:dst", "app:s1", "secret:x"])
            .starts_with("-NOPERM")
    );
    // Fully in-prefix multi-key commands still work.
    assert_eq!(b.cmd(&["MSET", "app:a", "1", "app:b", "2"]), "OK");
    assert_eq!(b.cmd(&["MGET", "app:a", "app:b"]), "[1, 2]");
    // Keyspace-wide readers can't be prefix-filtered: scoped users are denied.
    assert!(b.cmd(&["KEYS", "*"]).starts_with("-NOPERM"));
    assert!(b.cmd(&["SCAN", "0"]).starts_with("-NOPERM"));
    assert!(b.cmd(&["RANDOMKEY"]).starts_with("-NOPERM"));
    // And the secret is still there, unread and unrenamed.
    assert_eq!(admin.cmd(&["GET", "secret:x"]), "s3cr3t");
}

#[test]
fn cdc_requires_read_class_and_respects_key_prefix() {
    let s = Server::start_inner(&[("LOCUS_CDC_MAXLEN", "100")]);
    let mut admin = s.connect();
    admin.cmd(&["SET", "secret:x", "classified"]);
    // A pubsub-only user: CDC used to class as pubsub and stream the whole
    // keyspace (snapshot + live values) to exactly this kind of user.
    assert_eq!(
        admin.cmd(&[
            "ACL", "SETUSER", "pubber", "on", ">pw", "+@pubsub", "allkeys"
        ]),
        "OK"
    );
    let mut p = s.connect();
    assert_eq!(p.cmd(&["AUTH", "pubber", "pw"]), "OK");
    assert!(p.cmd(&["CDCSUBSCRIBE"]).starts_with("-NOPERM"));

    // A read user scoped to app:* may follow app:* changes — nothing wider.
    assert_eq!(
        admin.cmd(&["ACL", "SETUSER", "watcher", "on", ">pw", "+@read", "~app:"]),
        "OK"
    );
    let mut w = s.connect();
    assert_eq!(w.cmd(&["AUTH", "watcher", "pw"]), "OK");
    assert!(w.cmd(&["CDCSUBSCRIBE"]).starts_with("-NOPERM")); // all keys
    assert!(w.cmd(&["CDCSUBSCRIBE", "sec"]).starts_with("-NOPERM")); // outside
    assert!(
        w.cmd(&["CDCSUBSCRIBE", "REGION", "10", "10", "5", "km"])
            .starts_with("-NOPERM")
    ); // regions span the keyspace
    assert!(w.cmd(&["CDCREAD", "0"]).starts_with("-NOPERM")); // unfiltered read
    let sub = w.cmd(&["CDCSUBSCRIBE", "app:orders:"]); // inside: allowed
    assert!(
        sub.contains("cdc-snapshot-done"),
        "expected snapshot: {sub}"
    );
}

#[test]
fn wait_ignores_forged_replconf_acks() {
    let s = Server::start();
    let mut writer = s.connect();
    writer.cmd(&["SET", "k", "v"]);
    // A plain client forges a huge ack; WAIT must not count it (it never
    // PSYNCed — only real replicas' acks satisfy quorums).
    let mut forger = s.connect();
    forger.send(&["REPLCONF", "ACK", "999999999"]); // no reply by design
    sleep(Duration::from_millis(100));
    assert_eq!(writer.cmd(&["WAIT", "1", "150"]), "0");
}

#[test]
fn replica_kill9_restarts_as_replica_with_exactly_the_masters_data() {
    // The Frankenstein scenario: a node with its OWN prior AOF data becomes a
    // replica, is kill -9'd, and restarts. It must come back (a) holding
    // exactly the master's dataset — not a replay of old-data + stream — and
    // (b) AS a read-only replica, not a writable master.
    let tmp = std::env::temp_dir();
    let pid = std::process::id();
    let aof = format!("{}/locus-t2-replica-{pid}.aof", tmp.display());
    let role = format!("{}/locus-t2-replica-{pid}.role", tmp.display());
    for f in [&aof, &role] {
        let _ = std::fs::remove_file(f);
    }

    let master = Server::start();
    let mut m = master.connect();
    m.cmd(&["SET", "shared", "from-master"]);

    // The replica-to-be first lives as a standalone with its own data.
    let env: &[(&str, &str)] = &[
        ("LOCUS_AOF", aof.as_str()),
        ("LOCUS_ROLE_FILE", role.as_str()),
    ];
    let mut replica = Server::start_inner(env);
    {
        let mut r = replica.connect();
        r.cmd(&["SET", "junk", "pre-sync-data"]);
        assert_eq!(
            r.cmd(&["REPLICAOF", "127.0.0.1", &master.port.to_string()]),
            "OK"
        );
        let deadline = Instant::now() + Duration::from_secs(3);
        while r.cmd(&["GET", "shared"]) != "from-master" {
            assert!(Instant::now() < deadline, "never synced");
            sleep(Duration::from_millis(30));
        }
        // A post-sync streamed write, so the rebuilt AOF gets a tail too.
        m.cmd(&["SET", "streamed", "later"]);
        let deadline = Instant::now() + Duration::from_secs(3);
        while r.cmd(&["GET", "streamed"]) != "later" {
            assert!(Instant::now() < deadline, "stream never applied");
            sleep(Duration::from_millis(30));
        }
    }
    // kill -9 (Child::kill is SIGKILL) and restart on the same AOF+role files.
    replica.child.kill().unwrap();
    let _ = replica.child.wait();
    let restarted = Server::start_inner(env);
    let mut r = restarted.connect();
    // (a) Exactly the master's dataset: junk must NOT have been resurrected.
    assert_eq!(r.cmd(&["GET", "shared"]), "from-master");
    assert_eq!(r.cmd(&["GET", "streamed"]), "later");
    assert_eq!(
        r.cmd(&["GET", "junk"]),
        "(nil)",
        "pre-sync data resurrected"
    );
    // (b) Still a replica: role persisted, writes rejected.
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let info = r.cmd(&["INFO"]);
        if info.contains("role:slave") {
            break;
        }
        assert!(Instant::now() < deadline, "did not resume replica role");
        sleep(Duration::from_millis(50));
    }
    assert!(r.cmd(&["SET", "w", "1"]).starts_with("-READONLY"));
    for f in [&aof, &role] {
        let _ = std::fs::remove_file(f);
    }
}

/// Poll INFO until `field` equals `want` (or fail after the deadline).
fn wait_info(c: &mut Conn, field: &str, want: &str, what: &str) {
    let deadline = Instant::now() + Duration::from_secs(4);
    loop {
        let info = c.cmd(&["INFO"]);
        let got = info
            .split("\r\n")
            .find_map(|l| l.strip_prefix(field)?.strip_prefix(':'))
            .unwrap_or("")
            .trim()
            .to_string();
        if got == want {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{what}: {field}={got}, want {want}"
        );
        sleep(Duration::from_millis(40));
    }
}

#[test]
fn demoted_master_neither_double_counts_nor_keeps_replicas() {
    // A master with an attached replica gets demoted under a NEW master. Its
    // old replica must be cut loose (that stream's history ended), and every
    // command it now applies must advance its offset ONCE — the old code
    // counted twice (apply path + leftover streaming path), inflating the
    // offset sentinels use to pick promotion candidates.
    let a = Server::start(); // master, then demoted
    let b = Server::start(); // a's replica
    let c = Server::start(); // the new master
    let (mut ca, mut cb, mut cc) = (a.connect(), b.connect(), c.connect());

    ca.cmd(&["SET", "seed", "1"]);
    cb.cmd(&["REPLICAOF", "127.0.0.1", &a.port.to_string()]);
    wait_info(&mut cb, "master_link_status", "up", "b never synced from a");
    wait_info(&mut ca, "connected_slaves", "1", "a never saw its replica");

    // Demote A under C. B must be disconnected by the role fence.
    cc.cmd(&["SET", "c-seed", "1"]);
    ca.cmd(&["REPLICAOF", "127.0.0.1", &c.port.to_string()]);
    wait_info(&mut ca, "master_link_status", "up", "a never synced from c");
    wait_info(&mut ca, "connected_slaves", "0", "a kept its old replica");

    // Stream a batch through C and compare offsets: A's must track C's
    // exactly (single-counted), never run ahead of it.
    for i in 0..50 {
        cc.cmd(&["SET", &format!("k{i}"), "v"]);
    }
    let deadline = Instant::now() + Duration::from_secs(4);
    loop {
        let (offa, offc) = (
            info_field(&mut ca, "master_repl_offset"),
            info_field(&mut cc, "master_repl_offset"),
        );
        assert!(
            offa <= offc,
            "demoted master's offset ({offa}) ran past its master's ({offc}) — double-counted"
        );
        if offa == offc && offc > 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "offsets never converged: a={offa} c={offc}"
        );
        sleep(Duration::from_millis(40));
    }
}

#[test]
fn promotion_rotates_the_replication_id() {
    let m = Server::start();
    let r = Server::start();
    let (mut cm, mut cr) = (m.connect(), r.connect());
    cm.cmd(&["SET", "k", "v"]);
    cr.cmd(&["REPLICAOF", "127.0.0.1", &m.port.to_string()]);
    wait_info(&mut cr, "master_link_status", "up", "never synced");
    // After the full sync the replica carries the master's replid.
    let synced = cr.cmd(&["INFO"]);
    let old_id = synced
        .split("\r\n")
        .find_map(|l| l.strip_prefix("master_replid:"))
        .unwrap()
        .to_string();
    // Promotion mints a NEW replid: this node's history diverges from the old
    // master's unseen tail, so a downstream PSYNC quoting the old id must
    // FULLRESYNC instead of getting a CONTINUE from a different stream.
    assert_eq!(cr.cmd(&["REPLICAOF", "NO", "ONE"]), "OK");
    let promoted = cr.cmd(&["INFO"]);
    let new_id = promoted
        .split("\r\n")
        .find_map(|l| l.strip_prefix("master_replid:"))
        .unwrap()
        .to_string();
    assert_ne!(old_id, new_id, "replid must rotate on promotion");
    assert!(promoted.contains("role:master"));
}

#[test]
fn chained_replication_is_refused() {
    // C tries to replicate from B, which is itself a replica of A. B's applied
    // stream has its own byte numbering, so sub-replica acks would be fiction:
    // B must refuse the PSYNC and C's link must stay down.
    let a = Server::start();
    let b = Server::start();
    let c = Server::start();
    let (mut ca, mut cb, mut cc) = (a.connect(), b.connect(), c.connect());
    ca.cmd(&["SET", "k", "v"]);
    cb.cmd(&["REPLICAOF", "127.0.0.1", &a.port.to_string()]);
    wait_info(&mut cb, "master_link_status", "up", "b never synced");

    cc.cmd(&["REPLICAOF", "127.0.0.1", &b.port.to_string()]);
    sleep(Duration::from_millis(700)); // give the sync loop a few attempts
    let info = cc.cmd(&["INFO"]);
    assert!(
        info.contains("master_link_status:down"),
        "chained sync should be refused: {info}"
    );
    wait_info(&mut cb, "connected_slaves", "0", "b accepted a sub-replica");
}

#[test]
fn replica_loses_keys_when_the_master_expires_them() {
    let master = Server::start();
    let replica = Server::start();
    let (mut m, mut r) = (master.connect(), replica.connect());
    assert_eq!(
        r.cmd(&["REPLICAOF", "127.0.0.1", &master.port.to_string()]),
        "OK"
    );
    sleep(Duration::from_millis(400)); // full sync
    m.cmd(&["SET", "k", "v", "PX", "200"]);
    // The write replicates...
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if r.cmd(&["GET", "k"]) == "v" {
            break;
        }
        assert!(Instant::now() < deadline, "key never replicated");
        sleep(Duration::from_millis(20));
    }
    // ...then the master expires it and streams a DEL; the replica converges to
    // empty rather than holding a stale key (the divergence REPL-1 fixes).
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if r.cmd(&["GET", "k"]) == "(nil)" {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "expiry not reflected on the replica"
        );
        sleep(Duration::from_millis(50));
    }
}

#[test]
fn master_reports_real_replid_and_advancing_offset() {
    let master = Server::start();
    let replica = Server::start();
    let mut m = master.connect();
    let mut r = replica.connect();
    assert_eq!(
        r.cmd(&["REPLICAOF", "127.0.0.1", &master.port.to_string()]),
        "OK"
    );
    sleep(Duration::from_millis(400)); // full sync, replica registered
    let field = |info: &str, key: &str| -> String {
        info.split("\r\n")
            .find_map(|l| l.strip_prefix(key).and_then(|x| x.strip_prefix(':')))
            .unwrap_or("")
            .trim()
            .to_string()
    };
    let info0 = m.cmd(&["INFO"]);
    let replid = field(&info0, "master_replid");
    assert_eq!(
        replid.len(),
        40,
        "replid should be 40 hex chars: {replid:?}"
    );
    assert_ne!(replid, "0".repeat(40), "replid must not be all zeros");
    let off0: u64 = field(&info0, "master_repl_offset").parse().unwrap();
    // A replicated write advances the offset.
    m.cmd(&["SET", "k", "v"]);
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let off1: u64 = field(&m.cmd(&["INFO"]), "master_repl_offset")
            .parse()
            .unwrap();
        if off1 > off0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "offset did not advance on a write"
        );
        sleep(Duration::from_millis(50));
    }
}

#[test]
fn replica_reports_link_up_and_its_own_offset() {
    let master = Server::start();
    let replica = Server::start();
    let mut m = master.connect();
    let mut r = replica.connect();
    assert_eq!(
        r.cmd(&["REPLICAOF", "127.0.0.1", &master.port.to_string()]),
        "OK"
    );
    sleep(Duration::from_millis(400));
    m.cmd(&["SET", "k", "v"]);
    let field = |info: &str, key: &str| -> String {
        info.split("\r\n")
            .find_map(|l| l.strip_prefix(key).and_then(|x| x.strip_prefix(':')))
            .unwrap_or("")
            .trim()
            .to_string()
    };
    // The replica reports the link up and its applied offset advancing as it
    // consumes the master's stream.
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let info = r.cmd(&["INFO"]);
        let link = field(&info, "master_link_status");
        let off: u64 = field(&info, "master_repl_offset").parse().unwrap_or(0);
        if link == "up" && off > 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "replica link/offset wrong: link={link} off={off}"
        );
        sleep(Duration::from_millis(50));
    }
}

#[test]
fn wait_counts_replicas_that_acked() {
    let master = Server::start();
    let replica = Server::start();
    let mut m = master.connect();
    let mut r = replica.connect();
    // No replicas required -> returns immediately.
    assert_eq!(m.cmd(&["WAIT", "0", "100"]), "0");
    assert_eq!(
        r.cmd(&["REPLICAOF", "127.0.0.1", &master.port.to_string()]),
        "OK"
    );
    sleep(Duration::from_millis(400));
    m.cmd(&["SET", "k", "v"]);
    // The replica acks the write within the timeout, so WAIT 1 returns 1.
    assert_eq!(m.cmd(&["WAIT", "1", "2000"]), "1");
    // Asking for more replicas than exist times out and returns the real count.
    assert_eq!(m.cmd(&["WAIT", "5", "300"]), "1");
}

#[test]
fn bgrewriteaof_is_async_and_loses_no_writes() {
    let aof = format!(
        "{}/locus-asyncaof-{}.aof",
        std::env::temp_dir().display(),
        std::process::id()
    );
    let _ = std::fs::remove_file(&aof);
    let _ = std::fs::remove_file(format!("{aof}.tmp"));
    {
        let s = Server::start_inner(&[("LOCUS_AOF", &aof)]);
        let mut c = s.connect();
        for i in 0..50 {
            c.cmd(&["SET", &format!("k{i}"), &i.to_string()]);
        }
        // Async: returns immediately, before the base image is on disk.
        assert_eq!(
            c.cmd(&["BGREWRITEAOF"]),
            "Background append only file rewriting started"
        );
        // Writes that race the rewrite must be folded into the new file.
        for i in 50..100 {
            c.cmd(&["SET", &format!("k{i}"), &i.to_string()]);
        }
        sleep(Duration::from_millis(300)); // let the rewrite finalize + swap
        assert_eq!(c.cmd(&["DBSIZE"]), "100");
    }
    // Restart against the same AOF: every write replays, none lost in the swap.
    let s2 = Server::start_inner(&[("LOCUS_AOF", &aof)]);
    let mut c2 = s2.connect();
    assert_eq!(c2.cmd(&["DBSIZE"]), "100");
    assert_eq!(c2.cmd(&["GET", "k0"]), "0");
    assert_eq!(c2.cmd(&["GET", "k99"]), "99");
    drop(s2);
    let _ = std::fs::remove_file(&aof);
    let _ = std::fs::remove_file(format!("{aof}.tmp"));
}

fn role(c: &mut Conn) -> String {
    c.cmd(&["INFO"])
        .split("\r\n")
        .find_map(|l| l.strip_prefix("role:"))
        .unwrap_or("")
        .trim()
        .to_string()
}

#[test]
fn sentinel_promotes_replica_and_repoints_on_master_death() {
    let master = Server::start();
    let r1 = Server::start();
    let r2 = Server::start();
    let mport = master.port;
    r1.connect()
        .cmd(&["REPLICAOF", "127.0.0.1", &mport.to_string()]);
    r2.connect()
        .cmd(&["REPLICAOF", "127.0.0.1", &mport.to_string()]);
    sleep(Duration::from_millis(500)); // initial sync
    master.connect().cmd(&["SET", "k", "v"]);
    sleep(Duration::from_millis(400)); // replicas apply + ack offsets

    // Run the same binary as a sentinel monitoring the master + both replicas.
    let mut sentinel = Command::new(env!("CARGO_BIN_EXE_locus"))
        .env("LOCUS_SENTINEL", format!("127.0.0.1:{mport}"))
        .env(
            "LOCUS_SENTINEL_REPLICAS",
            format!("127.0.0.1:{},127.0.0.1:{}", r1.port, r2.port),
        )
        .env("LOCUS_SENTINEL_DOWN_AFTER_MS", "700")
        .env("LOCUS_SENTINEL_INTERVAL_MS", "200")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn sentinel");

    drop(master); // kill the master

    // A replica is promoted to master; the other is repointed to it and resyncs.
    let deadline = Instant::now() + Duration::from_secs(12);
    let (newm, follower) = loop {
        if role(&mut r1.connect()) == "master" {
            break (&r1, &r2);
        }
        if role(&mut r2.connect()) == "master" {
            break (&r2, &r1);
        }
        assert!(
            Instant::now() < deadline,
            "sentinel did not promote a replica"
        );
        sleep(Duration::from_millis(200));
    };
    // A write to the new master must reach the repointed follower.
    newm.connect().cmd(&["SET", "after", "failover"]);
    let propagated = loop {
        if follower.connect().cmd(&["GET", "after"]) == "failover" {
            break true;
        }
        if Instant::now() > deadline {
            break false;
        }
        sleep(Duration::from_millis(200));
    };
    let _ = sentinel.kill();
    let _ = sentinel.wait();
    assert!(
        propagated,
        "follower was not repointed to / synced from the new master"
    );
}

#[test]
fn replicaof_epoch_fence_rejects_stale_directives() {
    // The load-bearing failover-safety fence: once a node has accepted a
    // promotion at epoch N, a REPLICAOF carrying an OLDER epoch (a sentinel
    // that missed the failover) is rejected — this is what stops a resurrected
    // old master from demoting the legitimate new one and eating post-failover
    // writes. A stray peer (never a real target) plays the "old master".
    let node = Server::start();
    let stray = Server::start();
    let mut c = node.connect();

    // Promote at epoch 5 (as a sentinel's failover would).
    assert_eq!(c.cmd(&["REPLICAOF", "NO", "ONE", "EPOCH", "5"]), "OK");
    assert!(c.cmd(&["INFO"]).contains("config_epoch:5"));

    // A stale sentinel (epoch 3) tries to repoint us at the old master: rejected.
    let stale = c.cmd(&[
        "REPLICAOF",
        "127.0.0.1",
        &stray.port.to_string(),
        "EPOCH",
        "3",
    ]);
    assert!(
        stale.starts_with("-STALEEPOCH"),
        "stale epoch not fenced: {stale}"
    );
    assert!(c.cmd(&["INFO"]).contains("role:master")); // still master

    // An equal-or-newer epoch is honored (the real, current failover).
    assert_eq!(
        c.cmd(&[
            "REPLICAOF",
            "127.0.0.1",
            &stray.port.to_string(),
            "EPOCH",
            "6"
        ]),
        "OK"
    );
    assert!(c.cmd(&["INFO"]).contains("config_epoch:6"));
    // A manual REPLICAOF (no epoch) is always trusted (operator override).
    assert_eq!(c.cmd(&["REPLICAOF", "NO", "ONE"]), "OK");
}

#[test]
fn sentinel_holds_failover_without_quorum() {
    let master = Server::start();
    let r1 = Server::start();
    r1.connect()
        .cmd(&["REPLICAOF", "127.0.0.1", &master.port.to_string()]);
    sleep(Duration::from_millis(500));
    master.connect().cmd(&["SET", "k", "v"]);
    sleep(Duration::from_millis(300));

    // Quorum of 2 but only one replica exists -> failover can never be confirmed.
    let mut sentinel = Command::new(env!("CARGO_BIN_EXE_locus"))
        .env("LOCUS_SENTINEL", format!("127.0.0.1:{}", master.port))
        .env("LOCUS_SENTINEL_REPLICAS", format!("127.0.0.1:{}", r1.port))
        .env("LOCUS_SENTINEL_DOWN_AFTER_MS", "500")
        .env("LOCUS_SENTINEL_INTERVAL_MS", "200")
        .env("LOCUS_SENTINEL_QUORUM", "2")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn sentinel");

    drop(master); // kill the master
    sleep(Duration::from_secs(3)); // well past down-after + several poll cycles

    // Unconfirmed: the lone replica must NOT have been promoted.
    let still_slave = role(&mut r1.connect());
    let _ = sentinel.kill();
    let _ = sentinel.wait();
    assert_eq!(
        still_slave, "slave",
        "sentinel promoted without the configured quorum"
    );
}

/// Grab an OS-assigned free port, then release it (brief race, fine for a test).
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[test]
fn two_sentinels_agree_and_promote_exactly_once() {
    let master = Server::start();
    let r1 = Server::start();
    let r2 = Server::start();
    let mport = master.port;
    r1.connect()
        .cmd(&["REPLICAOF", "127.0.0.1", &mport.to_string()]);
    r2.connect()
        .cmd(&["REPLICAOF", "127.0.0.1", &mport.to_string()]);
    sleep(Duration::from_millis(500));
    master.connect().cmd(&["SET", "k", "v"]);
    sleep(Duration::from_millis(400));

    let sp1 = free_port();
    let sp2 = free_port();
    let spawn = |my: u16, peer: u16| {
        Command::new(env!("CARGO_BIN_EXE_locus"))
            .env("LOCUS_SENTINEL", format!("127.0.0.1:{mport}"))
            .env(
                "LOCUS_SENTINEL_REPLICAS",
                format!("127.0.0.1:{},127.0.0.1:{}", r1.port, r2.port),
            )
            .env("LOCUS_SENTINEL_PORT", my.to_string())
            .env("LOCUS_SENTINEL_PEERS", format!("127.0.0.1:{peer}"))
            .env("LOCUS_SENTINEL_DOWN_AFTER_MS", "700")
            .env("LOCUS_SENTINEL_INTERVAL_MS", "200")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn sentinel")
    };
    let mut s1 = spawn(sp1, sp2);
    let mut s2 = spawn(sp2, sp1);

    drop(master); // kill the master

    // Exactly one replica is promoted — never both (no split-brain), even though
    // two sentinels race. The follower is repointed and resyncs.
    let deadline = Instant::now() + Duration::from_secs(14);
    let (newm, follower) = loop {
        let m1 = role(&mut r1.connect()) == "master";
        let m2 = role(&mut r2.connect()) == "master";
        assert!(!(m1 && m2), "split-brain: both replicas were promoted");
        if m1 {
            break (&r1, &r2);
        }
        if m2 {
            break (&r2, &r1);
        }
        assert!(Instant::now() < deadline, "no promotion by either sentinel");
        sleep(Duration::from_millis(200));
    };
    newm.connect().cmd(&["SET", "after", "failover"]);
    let propagated = loop {
        if follower.connect().cmd(&["GET", "after"]) == "failover" {
            break true;
        }
        if Instant::now() > deadline {
            break false;
        }
        sleep(Duration::from_millis(200));
    };
    let _ = s1.kill();
    let _ = s1.wait();
    let _ = s2.kill();
    let _ = s2.wait();
    assert!(
        propagated,
        "follower not repointed/synced to the new master"
    );
}

#[test]
fn cluster_introspection_standalone() {
    let s = Server::start();
    let mut c = s.connect();
    // Standalone: cluster disabled, so clients fall back to single-node.
    assert!(c.cmd(&["CLUSTER", "INFO"]).contains("cluster_enabled:0"));
    // KEYSLOT computes a slot in range; a hashtag routes keys together.
    let slot: i64 = c.cmd(&["CLUSTER", "KEYSLOT", "foo"]).parse().unwrap();
    assert!((0..16384).contains(&slot));
    assert_eq!(
        c.cmd(&["CLUSTER", "KEYSLOT", "{x}a"]),
        c.cmd(&["CLUSTER", "KEYSLOT", "{x}b"])
    );
    // MYID is the 40-hex node id; no slots assigned.
    assert_eq!(c.cmd(&["CLUSTER", "MYID"]).len(), 40);
    assert_eq!(c.cmd(&["CLUSTER", "SLOTS"]), "[]");
}

#[test]
fn cluster_routing_moved_and_crossslot() {
    // This node owns slots 0-8191; a peer owns the rest.
    let s = Server::start_inner(&[
        ("LOCUS_CLUSTER_ENABLED", "1"),
        ("LOCUS_CLUSTER_ANNOUNCE", "127.0.0.1:7000"),
        (
            "LOCUS_CLUSTER_NODES",
            "127.0.0.1:7000 0-8191;127.0.0.1:7001 8192-16383",
        ),
    ]);
    let mut c = s.connect();
    assert!(c.cmd(&["CLUSTER", "INFO"]).contains("cluster_enabled:1"));
    assert!(c.cmd(&["CLUSTER", "SLOTS"]).contains("7000")); // reports ownership

    // Classify keys by slot, then check routing matches ownership.
    let slot =
        |c: &mut Conn, k: &str| -> i64 { c.cmd(&["CLUSTER", "KEYSLOT", k]).parse().unwrap() };
    let (mut ours, mut theirs) = (None, None);
    for k in ["a", "b", "c", "d", "e", "f", "g", "foo", "bar", "baz"] {
        if slot(&mut c, k) <= 8191 {
            ours.get_or_insert(k);
        } else {
            theirs.get_or_insert(k);
        }
    }
    let (ours, theirs) = (ours.unwrap(), theirs.unwrap());

    assert_eq!(c.cmd(&["SET", ours, "v"]), "OK"); // our slot -> served
    let moved = c.cmd(&["SET", theirs, "v"]); // peer's slot -> MOVED
    assert!(moved.starts_with("-MOVED"), "{moved}");
    assert!(moved.contains("127.0.0.1:7001"));
    // Keys spanning two slots -> CROSSSLOT.
    let cs = c.cmd(&["MGET", ours, theirs]);
    assert!(cs.starts_with("-CROSSSLOT"), "{cs}");
}

fn spawn_cluster_node(port: u16, nodes: &str) -> Child {
    spawn_cluster_node_cells(port, nodes, 0)
}

/// Spawn a cluster node with extra env (for cluster-secret / peer-timeout tests).
fn spawn_cluster_node_env(port: u16, nodes: &str, extra: &[(&str, &str)]) -> Child {
    let rdb = format!(
        "{}/locus-clue-{}-{}.rdb",
        std::env::temp_dir().display(),
        std::process::id(),
        port
    );
    let _ = std::fs::remove_file(&rdb);
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_locus"));
    cmd.env("LOCUS_PORT", port.to_string())
        .env("LOCUS_RDB", &rdb)
        .env_remove("LOCUS_AOF")
        .env("LOCUS_CLUSTER_ENABLED", "1")
        .env("LOCUS_CLUSTER_ANNOUNCE", format!("127.0.0.1:{port}"))
        .env("LOCUS_CLUSTER_NODES", nodes)
        .env("LOCUS_CDC_MAXLEN", "1000")
        .env("LOCUS_CLUSTER_GOSSIP_MS", "200")
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    for (k, v) in extra {
        cmd.env(k, v);
    }
    let mut child = cmd.spawn().expect("spawn cluster node");
    let mut reader = BufReader::new(child.stdout.take().unwrap());
    loop {
        let mut l = String::new();
        assert!(reader.read_line(&mut l).unwrap() > 0, "node exited early");
        if l.contains("listening on") {
            break;
        }
    }
    std::thread::spawn(move || {
        let mut sink = Vec::new();
        let _ = reader.read_to_end(&mut sink);
    });
    child
}

/// Spawn a cluster node on a fixed port with the given topology; wait until it
/// listens. `cell_bits > 0` turns on cell-in-key sharding (bounded GEOSEARCH).
fn spawn_cluster_node_cells(port: u16, nodes: &str, cell_bits: u32) -> Child {
    let rdb = format!(
        "{}/locus-clu-{}-{}.rdb",
        std::env::temp_dir().display(),
        std::process::id(),
        port
    );
    let _ = std::fs::remove_file(&rdb);
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_locus"));
    cmd.env("LOCUS_PORT", port.to_string())
        .env("LOCUS_RDB", &rdb)
        .env_remove("LOCUS_AOF")
        .env("LOCUS_CLUSTER_ENABLED", "1")
        .env("LOCUS_CLUSTER_ANNOUNCE", format!("127.0.0.1:{port}"))
        .env("LOCUS_CLUSTER_NODES", nodes)
        .env("LOCUS_CDC_MAXLEN", "1000") // retain changes for CLUSTER CDCMERGE
        .env("LOCUS_CLUSTER_GOSSIP_MS", "200") // fast topology convergence in tests
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    if cell_bits > 0 {
        cmd.env("LOCUS_CLUSTER_CELL_BITS", cell_bits.to_string());
    }
    let mut child = cmd.spawn().expect("spawn cluster node");
    let mut reader = BufReader::new(child.stdout.take().unwrap());
    loop {
        let mut l = String::new();
        assert!(reader.read_line(&mut l).unwrap() > 0, "node exited early");
        if l.contains("listening on") {
            break;
        }
    }
    std::thread::spawn(move || {
        let mut sink = Vec::new();
        let _ = reader.read_to_end(&mut sink);
    });
    child
}

fn conn_to(port: u16) -> Conn {
    let stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    Conn {
        reader: BufReader::new(stream.try_clone().unwrap()),
        stream,
    }
}

#[test]
fn cluster_dbsize_sums_all_shards() {
    let (p1, p2) = (free_port(), free_port());
    let nodes = format!("127.0.0.1:{p1} 0-8191;127.0.0.1:{p2} 8192-16383");
    let mut n1 = spawn_cluster_node(p1, &nodes);
    let mut n2 = spawn_cluster_node(p2, &nodes);
    let mut c1 = conn_to(p1);
    let mut c2 = conn_to(p2);

    // Place each key on its owning node (no MOVED, since we target the owner).
    let total = 24;
    for i in 0..total {
        let k = format!("key{i}");
        let slot: i64 = c1.cmd(&["CLUSTER", "KEYSLOT", &k]).parse().unwrap();
        let owner = if slot <= 8191 { &mut c1 } else { &mut c2 };
        assert_eq!(owner.cmd(&["SET", &k, "v"]), "OK");
    }
    // DBSIZE from either node sums both shards (local + peer via XDBSIZE).
    assert_eq!(c1.cmd(&["DBSIZE"]).parse::<i64>().unwrap(), total);
    assert_eq!(c2.cmd(&["DBSIZE"]).parse::<i64>().unwrap(), total);

    let _ = n1.kill();
    let _ = n1.wait();
    let _ = n2.kill();
    let _ = n2.wait();
}

#[test]
fn cluster_geosearch_scatter_gather() {
    let (p1, p2) = (free_port(), free_port());
    let nodes = format!("127.0.0.1:{p1} 0-8191;127.0.0.1:{p2} 8192-16383");
    let mut n1 = spawn_cluster_node(p1, &nodes);
    let mut n2 = spawn_cluster_node(p2, &nodes);
    let mut c1 = conn_to(p1);
    let mut c2 = conn_to(p2);

    // Geo points clustered near (10,50) but with names that name-shard across both
    // nodes. Each is stored on its owner; a node2-owned one proves the gather.
    let total = 20;
    let mut a_node2_key = None;
    for i in 0..total {
        let k = format!("pt{i}");
        let lon = format!("{:.5}", 10.0 + (i as f64) * 0.0005); // all within a few km
        let slot: i64 = c1.cmd(&["CLUSTER", "KEYSLOT", &k]).parse().unwrap();
        let owner = if slot <= 8191 { &mut c1 } else { &mut c2 };
        if slot > 8191 {
            a_node2_key = Some(k.clone());
        }
        assert_eq!(owner.cmd(&["GEOSET", &k, &lon, "50.0"]), "OK");
    }
    let node2_key = a_node2_key.expect("test needs at least one point on node2");

    // GEOSEARCH on node1 scatter-gathers both shards -> all points, incl. node2's.
    let r = c1.cmd(&[
        "GEOSEARCH",
        "FROMLONLAT",
        "10.0",
        "50.0",
        "BYRADIUS",
        "50",
        "km",
    ]);
    let n = if r == "[]" {
        0
    } else {
        r.trim_matches(|c| c == '[' || c == ']')
            .split(", ")
            .filter(|s| !s.is_empty())
            .count()
    };
    assert_eq!(
        n, total,
        "scatter-gather should return all shards' points: {r}"
    );
    assert!(r.contains(&node2_key), "missing a node2-owned point: {r}");

    let _ = n1.kill();
    let _ = n1.wait();
    let _ = n2.kill();
    let _ = n2.wait();
}

#[test]
fn cluster_geosearch_bounded_by_cell() {
    let (p1, p2) = (free_port(), free_port());
    let nodes = format!("127.0.0.1:{p1} 0-8191;127.0.0.1:{p2} 8192-16383");
    let mut n1 = spawn_cluster_node_cells(p1, &nodes, 20);
    let mut n2 = spawn_cluster_node_cells(p2, &nodes, 20);
    let mut c1 = conn_to(p1);
    let mut c2 = conn_to(p2);

    // Place a geo point under a {cell}-tagged key on its owning node, so a region
    // co-locates and a bounded GEOSEARCH can still find it across shards.
    fn place(c1: &mut Conn, c2: &mut Conn, id: &str, lon: &str, lat: &str) -> String {
        let cell = c1.cmd(&["CLUSTER", "CELL", lon, lat]);
        let key = format!("{{{cell}}}{id}");
        let slot: i64 = c1.cmd(&["CLUSTER", "KEYSLOT", &key]).parse().unwrap();
        let owner = if slot <= 8191 { c1 } else { c2 };
        assert_eq!(owner.cmd(&["GEOSET", &key, lon, lat]), "OK");
        key
    }
    let near1 = place(&mut c1, &mut c2, "a", "10.00", "50.00");
    let near2 = place(&mut c1, &mut c2, "b", "10.02", "50.00");
    let _far = place(&mut c1, &mut c2, "z", "-100.0", "40.0");

    // Bounded scatter still finds both nearby points (wherever they shard) and
    // excludes the far one.
    let r = c1.cmd(&[
        "GEOSEARCH",
        "FROMLONLAT",
        "10.0",
        "50.0",
        "BYRADIUS",
        "20",
        "km",
    ]);
    assert!(r.contains(&near1), "missing near1: {r}");
    assert!(r.contains(&near2), "missing near2: {r}");
    assert!(!r.contains("}z"), "far point should be excluded: {r}");

    let _ = n1.kill();
    let _ = n1.wait();
    let _ = n2.kill();
    let _ = n2.wait();
}

#[test]
fn cluster_down_for_gap_then_setslot_serves() {
    let p1 = free_port();
    // Topology with a deliberate gap: we own 0-10000, 10001-16383 are unassigned.
    let nodes = format!("127.0.0.1:{p1} 0-10000");
    let mut n1 = spawn_cluster_node(p1, &nodes);
    let mut c1 = conn_to(p1);

    // A key landing in the gap can't be served -> CLUSTERDOWN.
    let mut gap = None;
    for i in 0..200 {
        let k = format!("k{i}");
        let slot: i64 = c1.cmd(&["CLUSTER", "KEYSLOT", &k]).parse().unwrap();
        if slot > 10000 {
            gap = Some((k, slot));
            break;
        }
    }
    let (gk, gslot) = gap.expect("need a key in the unassigned range");
    let r = c1.cmd(&["SET", &gk, "v"]);
    assert!(r.contains("CLUSTERDOWN"), "expected CLUSTERDOWN, got {r}");

    // Assign that slot to ourselves at runtime -> the key is now served.
    let me = format!("127.0.0.1:{p1}");
    assert_eq!(
        c1.cmd(&["CLUSTER", "SETSLOT", &gslot.to_string(), "NODE", &me]),
        "OK"
    );
    assert_eq!(c1.cmd(&["SET", &gk, "v"]), "OK");
    assert_eq!(c1.cmd(&["GET", &gk]), "v");

    let _ = n1.kill();
    let _ = n1.wait();
}

#[test]
fn cluster_migrateslot_moves_keys_zero_loss() {
    let (p1, p2) = (free_port(), free_port());
    let nodes = format!("127.0.0.1:{p1} 0-8191;127.0.0.1:{p2} 8192-16383");
    let mut n1 = spawn_cluster_node(p1, &nodes);
    let mut n2 = spawn_cluster_node(p2, &nodes);
    let mut c1 = conn_to(p1);
    let mut c2 = conn_to(p2);

    // A key owned by node1 (slot 0-8191), with a value.
    let mut found = None;
    for i in 0..300 {
        let k = format!("m{i}");
        let s: i64 = c1.cmd(&["CLUSTER", "KEYSLOT", &k]).parse().unwrap();
        if s <= 8191 {
            found = Some((k, s));
            break;
        }
    }
    let (k, slot) = found.expect("need a node1-owned key");
    assert_eq!(c1.cmd(&["SET", &k, "hello"]), "OK");

    // Migrate that slot to node2; propagate ownership to node2 (operator step).
    let dst = format!("127.0.0.1:{p2}");
    let moved: i64 = c1
        .cmd(&["CLUSTER", "MIGRATESLOT", &slot.to_string(), &dst])
        .parse()
        .unwrap();
    assert!(moved >= 1, "expected >=1 key moved, got {moved}");
    assert_eq!(
        c2.cmd(&["CLUSTER", "SETSLOT", &slot.to_string(), "NODE", &dst]),
        "OK"
    );

    // Zero loss: the value lives on node2 now; node1 redirects there (no stale copy).
    assert_eq!(c2.cmd(&["GET", &k]), "hello");
    let r1 = c1.cmd(&["GET", &k]);
    assert!(
        r1.contains("MOVED") && r1.contains(&dst),
        "expected MOVED {dst}, got {r1}"
    );

    let _ = n1.kill();
    let _ = n1.wait();
    let _ = n2.kill();
    let _ = n2.wait();
}

#[test]
fn cluster_works_with_requirepass_via_cluster_secret() {
    // Secured + clustered must coexist (T3-8): with a client password set, the
    // internal RPCs authenticate with the cluster secret, so a cross-shard
    // GEOSEARCH still reaches every shard instead of going local-only.
    let (p1, p2) = (free_port(), free_port());
    let nodes = format!("127.0.0.1:{p1} 0-8191;127.0.0.1:{p2} 8192-16383");
    let env: &[(&str, &str)] = &[
        ("LOCUS_REQUIREPASS", "clientpw"),
        ("LOCUS_CLUSTER_SECRET", "meshsecret"),
    ];
    let mut n1 = spawn_cluster_node_env(p1, &nodes, env);
    let mut n2 = spawn_cluster_node_env(p2, &nodes, env);
    let mut c1 = conn_to(p1);
    let mut c2 = conn_to(p2);
    assert_eq!(c1.cmd(&["AUTH", "clientpw"]), "OK");
    assert_eq!(c2.cmd(&["AUTH", "clientpw"]), "OK");

    // Put a geo point on whichever shard owns it (route via KEYSLOT).
    let place = |c1: &mut Conn, c2: &mut Conn, k: &str, lon: &str, lat: &str| {
        let s: i64 = c1.cmd(&["CLUSTER", "KEYSLOT", k]).parse().unwrap();
        let c = if s <= 8191 { c1 } else { c2 };
        assert_eq!(c.cmd(&["GEOSET", k, lon, lat]), "OK");
    };
    // Two nearby points that hash to different shards (try a handful).
    place(&mut c1, &mut c2, "loc:a", "51.53", "25.28");
    place(&mut c1, &mut c2, "loc:b", "51.531", "25.281");

    // A cross-shard GEOSEARCH gathers from both shards (needs the mesh auth).
    let r = c1.cmd(&[
        "GEOSEARCH",
        "FROMLONLAT",
        "51.53",
        "25.28",
        "BYRADIUS",
        "5",
        "km",
        "ASC",
    ]);
    assert!(
        r.contains("loc:a") && r.contains("loc:b"),
        "cross-shard GEOSEARCH under requirepass missed a shard: {r}"
    );

    // A wrong/absent mesh secret path: the client password alone still gates.
    let mut bad = conn_to(p1);
    assert!(bad.cmd(&["GET", "loc:a"]).starts_with("-NOAUTH"));

    for n in [&mut n1, &mut n2] {
        let _ = n.kill();
        let _ = n.wait();
    }
}

#[test]
fn cluster_geosearch_fromkey_requires_a_local_center() {
    // GEOSEARCH FROMKEY resolves the center from a key that may live on another
    // shard (GEOSEARCH is keyless for routing). In cluster mode a center key
    // this node doesn't own is rejected toward FROMLONLAT rather than silently
    // searching a stale/absent local copy (T3-12).
    let (p1, p2) = (free_port(), free_port());
    let nodes = format!("127.0.0.1:{p1} 0-8191;127.0.0.1:{p2} 8192-16383");
    let mut n1 = spawn_cluster_node(p1, &nodes);
    let mut n2 = spawn_cluster_node(p2, &nodes);
    let mut c1 = conn_to(p1);
    let mut c2 = conn_to(p2);

    // Find a center key owned by node2, place it there.
    let mut center = None;
    for i in 0..300 {
        let k = format!("ctr{i}");
        let slot: i64 = c1.cmd(&["CLUSTER", "KEYSLOT", &k]).parse().unwrap();
        if slot > 8191 {
            center = Some(k);
            break;
        }
    }
    let center = center.unwrap();
    c2.cmd(&["GEOSET", &center, "51.5", "25.3"]);

    // FROMKEY that center on node1 (which doesn't own it) -> CROSSSLOT error.
    let r = c1.cmd(&["GEOSEARCH", "FROMKEY", &center, "BYRADIUS", "5", "km"]);
    assert!(
        r.starts_with("-CROSSSLOT"),
        "expected CROSSSLOT for a remote center key, got: {r}"
    );
    // On node2 (the owner) it resolves fine.
    let r2 = c2.cmd(&["GEOSEARCH", "FROMKEY", &center, "BYRADIUS", "5", "km"]);
    assert!(!r2.starts_with('-'), "owner should resolve FROMKEY: {r2}");

    for n in [&mut n1, &mut n2] {
        let _ = n.kill();
        let _ = n.wait();
    }
}

#[test]
fn cluster_geosearch_errors_when_a_shard_is_down() {
    // A silently partial "nearest" result is dangerous: with a shard
    // unreachable, cross-shard GEOSEARCH errors (CLUSTERDOWN) by default rather
    // than returning fewer hits (T3-9).
    let (p1, p2) = (free_port(), free_port());
    let nodes = format!("127.0.0.1:{p1} 0-8191;127.0.0.1:{p2} 8192-16383");
    let mut n1 = spawn_cluster_node(p1, &nodes);
    let mut n2 = spawn_cluster_node(p2, &nodes);
    let mut c1 = conn_to(p1);
    c1.cmd(&["GEOSET", "here", "51.53", "25.28"]);

    // Kill shard 2, then a cross-shard GEOSEARCH from shard 1 must error.
    let _ = n2.kill();
    let _ = n2.wait();
    sleep(Duration::from_millis(100));
    let r = c1.cmd(&[
        "GEOSEARCH",
        "FROMLONLAT",
        "51.53",
        "25.28",
        "BYRADIUS",
        "5",
        "km",
    ]);
    assert!(
        r.starts_with("-CLUSTERDOWN"),
        "expected CLUSTERDOWN with a shard down, got: {r}"
    );

    let _ = n1.kill();
    let _ = n1.wait();
}

#[test]
fn cluster_migrated_keys_survive_a_destination_restart() {
    // Migration durability (T3-5): a key moved into a shard is logged to that
    // shard's AOF, so a dst crash after the migration doesn't lose it. Topology
    // is re-learned from the surviving source's gossip after restart.
    let (p1, p2) = (free_port(), free_port());
    let nodes = format!("127.0.0.1:{p1} 0-8191;127.0.0.1:{p2} 8192-16383");
    let aof2 = format!(
        "{}/locus-migdur-{}-{}.aof",
        std::env::temp_dir().display(),
        std::process::id(),
        p2
    );
    let _ = std::fs::remove_file(&aof2);
    let env2: &[(&str, &str)] = &[("LOCUS_AOF", aof2.as_str())];
    let mut n1 = spawn_cluster_node(p1, &nodes);
    let mut n2 = spawn_cluster_node_env(p2, &nodes, env2);
    let mut c1 = conn_to(p1);
    let mut c2 = conn_to(p2);

    // A node1-owned key, migrated to node2 (which has AOF on).
    let (mut k, mut slot) = (String::new(), 0i64);
    for i in 0..300 {
        let key = format!("d{i}");
        let s: i64 = c1.cmd(&["CLUSTER", "KEYSLOT", &key]).parse().unwrap();
        if s <= 8191 {
            k = key;
            slot = s;
            break;
        }
    }
    c1.cmd(&["SET", &k, "durable"]);
    let dst = format!("127.0.0.1:{p2}");
    let moved: i64 = c1
        .cmd(&["CLUSTER", "MIGRATESLOT", &slot.to_string(), &dst])
        .parse()
        .unwrap();
    assert!(moved >= 1);
    c2.cmd(&["CLUSTER", "SETSLOT", &slot.to_string(), "NODE", &dst]);
    assert_eq!(c2.cmd(&["GET", &k]), "durable");

    // kill -9 node2 and restart it on the same AOF. Its topology reverts to env
    // (node1 owns the slot) but node1's gossip re-teaches it that it owns the
    // migrated slot; the migrated key must still be present from the AOF.
    n2.kill().unwrap();
    let _ = n2.wait();
    let mut n2b = spawn_cluster_node_env(p2, &nodes, env2);
    let mut c2b = conn_to(p2);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let r = c2b.cmd(&["GET", &k]);
        if r == "durable" {
            break; // gossip re-taught ownership + AOF preserved the value
        }
        assert!(
            Instant::now() < deadline,
            "migrated key lost or ownership never reconverged: {r}"
        );
        sleep(Duration::from_millis(150));
    }

    let _ = n1.kill();
    let _ = n1.wait();
    let _ = n2b.kill();
    let _ = n2b.wait();
    let _ = std::fs::remove_file(&aof2);
}

#[test]
fn cluster_reassign_repoints_a_nodes_slots() {
    let (p1, p2) = (free_port(), free_port());
    // node1 owns 0-8191, node2 owns 8192-16383. Only node1 is run here; REASSIGN
    // edits node1's own owner map (the per-shard-failover primitive).
    let nodes = format!("127.0.0.1:{p1} 0-8191;127.0.0.1:{p2} 8192-16383");
    let mut n1 = spawn_cluster_node(p1, &nodes);
    let mut c1 = conn_to(p1);

    // A node2-owned key currently redirects there.
    let mut k2 = None;
    for i in 0..300 {
        let k = format!("k{i}");
        let s: i64 = c1.cmd(&["CLUSTER", "KEYSLOT", &k]).parse().unwrap();
        if s > 8191 {
            k2 = Some(k);
            break;
        }
    }
    let k = k2.expect("need a node2-owned key");
    let dst2 = format!("127.0.0.1:{p2}");
    assert!(c1.cmd(&["GET", &k]).contains(&dst2));

    // Reassign all of node2's slots to node1 -> node1 now serves them.
    let me = format!("127.0.0.1:{p1}");
    let moved: i64 = c1
        .cmd(&["CLUSTER", "REASSIGN", &dst2, &me])
        .parse()
        .unwrap();
    assert!(moved > 0, "expected slots reassigned, got {moved}");
    assert_eq!(c1.cmd(&["SET", &k, "v"]), "OK");
    assert_eq!(c1.cmd(&["GET", &k]), "v");

    let _ = n1.kill();
    let _ = n1.wait();
}

#[test]
fn cluster_per_shard_failover_reassigns_slots() {
    let (pm, pr) = (free_port(), free_port());
    // A single shard: master M owns all slots; R is M's cluster-enabled replica.
    let topo = format!("127.0.0.1:{pm} 0-16383");
    let mut m = spawn_cluster_node(pm, &topo);
    let mut r = spawn_cluster_node(pr, &topo);
    conn_to(pr).cmd(&["REPLICAOF", "127.0.0.1", &pm.to_string()]);
    sleep(Duration::from_millis(500)); // initial sync
    conn_to(pm).cmd(&["SET", "k", "v"]); // M owns k; replicates to R
    sleep(Duration::from_millis(400));

    // Sentinel monitors the shard and knows the cluster nodes to reassign.
    let mut sentinel = Command::new(env!("CARGO_BIN_EXE_locus"))
        .env("LOCUS_SENTINEL", format!("127.0.0.1:{pm}"))
        .env("LOCUS_SENTINEL_REPLICAS", format!("127.0.0.1:{pr}"))
        .env(
            "LOCUS_SENTINEL_CLUSTER_NODES",
            format!("127.0.0.1:{pm},127.0.0.1:{pr}"),
        )
        .env("LOCUS_SENTINEL_DOWN_AFTER_MS", "700")
        .env("LOCUS_SENTINEL_INTERVAL_MS", "200")
        .env("LOCUS_SENTINEL_QUORUM", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn sentinel");

    let _ = m.kill(); // master dies
    let _ = m.wait();

    // After promotion + REASSIGN, R owns the shard's slots and serves the data
    // directly (no MOVED) — a per-shard failover with the routing updated.
    let deadline = Instant::now() + Duration::from_secs(12);
    let took_over = loop {
        if conn_to(pr).cmd(&["GET", "k"]) == "v" {
            break true;
        }
        if Instant::now() > deadline {
            break false;
        }
        sleep(Duration::from_millis(200));
    };
    let _ = sentinel.kill();
    let _ = sentinel.wait();
    let _ = r.kill();
    let _ = r.wait();
    assert!(
        took_over,
        "R did not take over the shard's slots after failover"
    );
}

#[test]
fn cluster_cdcmerge_orders_changes_across_shards() {
    let (p1, p2) = (free_port(), free_port());
    let nodes = format!("127.0.0.1:{p1} 0-8191;127.0.0.1:{p2} 8192-16383");
    let mut n1 = spawn_cluster_node(p1, &nodes); // CDC retention is on by default here
    let mut n2 = spawn_cluster_node(p2, &nodes);
    let mut c1 = conn_to(p1);
    let mut c2 = conn_to(p2);

    // Two keys per node, by slot ownership.
    let (mut a, mut b) = (Vec::new(), Vec::new());
    for i in 0..600 {
        let k = format!("e{i}");
        let s: i64 = c1.cmd(&["CLUSTER", "KEYSLOT", &k]).parse().unwrap();
        if s <= 8191 && a.len() < 2 {
            a.push(k);
        } else if s > 8191 && b.len() < 2 {
            b.push(k);
        }
        if a.len() == 2 && b.len() == 2 {
            break;
        }
    }
    assert!(a.len() == 2 && b.len() == 2, "need 2 keys per node");

    // Write interleaved across shards, spaced >1ms so the HLC physical part orders
    // them deterministically: a0(n1), b0(n2), a1(n1), b1(n2).
    let order = [&a[0], &b[0], &a[1], &b[1]];
    c1.cmd(&["SET", &a[0], "1"]);
    sleep(Duration::from_millis(8));
    c2.cmd(&["SET", &b[0], "2"]);
    sleep(Duration::from_millis(8));
    c1.cmd(&["SET", &a[1], "3"]);
    sleep(Duration::from_millis(8));
    c2.cmd(&["SET", &b[1], "4"]);
    sleep(Duration::from_millis(8)); // let the watermark advance past the last write

    // The merged feed from node1 includes both shards' changes in global HLC order.
    let r = c1.cmd(&["CLUSTER", "CDCMERGE", "0", "COUNT", "100"]);
    let pos: Vec<usize> = order
        .iter()
        .map(|k| {
            r.find(k.as_str())
                .unwrap_or_else(|| panic!("missing {k} in {r}"))
        })
        .collect();
    for w in pos.windows(2) {
        assert!(w[0] < w[1], "cross-shard changes out of HLC order: {r}");
    }

    let _ = n1.kill();
    let _ = n1.wait();
    let _ = n2.kill();
    let _ = n2.wait();
}

#[test]
fn cluster_cdcmerge_holds_watermark_for_downed_shard() {
    let (p1, p2) = (free_port(), free_port());
    let nodes = format!("127.0.0.1:{p1} 0-8191;127.0.0.1:{p2} 8192-16383");
    let mut n1 = spawn_cluster_node(p1, &nodes);
    let mut n2 = spawn_cluster_node(p2, &nodes);
    let mut c1 = conn_to(p1);

    // Two node1-owned keys (node2 is killed below, so writes must go to node1).
    let mut keys = Vec::new();
    for i in 0..400 {
        let k = format!("w{i}");
        let s: i64 = c1.cmd(&["CLUSTER", "KEYSLOT", &k]).parse().unwrap();
        if s <= 8191 {
            keys.push(k);
        }
        if keys.len() == 2 {
            break;
        }
    }
    let (early, late) = (&keys[0], &keys[1]);

    // Write `early`, then merge once so node1 learns node2's floor (both up).
    c1.cmd(&["SET", early, "1"]);
    let r0 = c1.cmd(&["CLUSTER", "CDCMERGE", "0", "COUNT", "10"]);
    assert!(
        r0.contains(early.as_str()),
        "early not merged while up: {r0}"
    );

    // node2 dies, then a later write lands above node2's last-known floor.
    let _ = n2.kill();
    let _ = n2.wait();
    sleep(Duration::from_millis(25));
    c1.cmd(&["SET", late, "2"]);

    // node2 is down but was seen, so the watermark is held at its last floor:
    // `late` (stamped after) is withheld, while `early` still delivers.
    let r1 = c1.cmd(&["CLUSTER", "CDCMERGE", "0", "COUNT", "10"]);
    assert!(
        r1.contains(early.as_str()),
        "early should still deliver: {r1}"
    );
    assert!(
        !r1.contains(late.as_str()),
        "late write must be held below the downed shard's watermark: {r1}"
    );

    let _ = n1.kill();
    let _ = n1.wait();
}

#[test]
fn cluster_cdcmerge_releases_watermark_after_peer_timeout() {
    // A shard held the watermark while down (previous test). But it must not
    // stall the global feed FOREVER: past LOCUS_CDC_PEER_TIMEOUT_MS the merge
    // releases it so writes on the surviving shard keep flowing (the dead
    // shard's buffered changes rejoin best-effort when it returns).
    let (p1, p2) = (free_port(), free_port());
    let nodes = format!("127.0.0.1:{p1} 0-8191;127.0.0.1:{p2} 8192-16383");
    let env: &[(&str, &str)] = &[("LOCUS_CDC_PEER_TIMEOUT_MS", "300")];
    let mut n1 = spawn_cluster_node_env(p1, &nodes, env);
    let mut n2 = spawn_cluster_node_env(p2, &nodes, env);
    let mut c1 = conn_to(p1);

    let mut keys = Vec::new();
    for i in 0..400 {
        let k = format!("t{i}");
        let s: i64 = c1.cmd(&["CLUSTER", "KEYSLOT", &k]).parse().unwrap();
        if s <= 8191 {
            keys.push(k);
        }
        if keys.len() == 2 {
            break;
        }
    }
    let (early, late) = (&keys[0], &keys[1]);

    c1.cmd(&["SET", early, "1"]);
    let _ = c1.cmd(&["CLUSTER", "CDCMERGE", "0", "COUNT", "10"]); // learn n2's floor
    let _ = n2.kill();
    let _ = n2.wait();
    sleep(Duration::from_millis(25));
    c1.cmd(&["SET", late, "2"]);

    // Wait past the peer timeout, then the withheld `late` write is released.
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let r = c1.cmd(&["CLUSTER", "CDCMERGE", "0", "COUNT", "10"]);
        if r.contains(late.as_str()) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "dead shard stalled the feed past its timeout: {r}"
        );
        sleep(Duration::from_millis(100));
    }

    let _ = n1.kill();
    let _ = n1.wait();
}

#[test]
fn cluster_topology_gossip_converges() {
    let (p1, p2) = (free_port(), free_port());
    let nodes = format!("127.0.0.1:{p1} 0-8191;127.0.0.1:{p2} 8192-16383");
    let mut n1 = spawn_cluster_node(p1, &nodes);
    let mut n2 = spawn_cluster_node(p2, &nodes);
    let mut c1 = conn_to(p1);
    let mut c2 = conn_to(p2);

    // A key node2 owns. Before any change, node2 serves it locally (no redirect).
    let mut k2 = None;
    for i in 0..400 {
        let k = format!("g{i}");
        let s: i64 = c1.cmd(&["CLUSTER", "KEYSLOT", &k]).parse().unwrap();
        if s > 8191 {
            k2 = Some(k);
            break;
        }
    }
    let k = k2.expect("need a node2-owned key");
    let (a1, a2) = (format!("127.0.0.1:{p1}"), format!("127.0.0.1:{p2}"));

    // Reassign node2's slots to node1 on node1 ONLY (no manual push to node2).
    assert!(
        c1.cmd(&["CLUSTER", "REASSIGN", &a2, &a1])
            .parse::<i64>()
            .unwrap()
            > 0
    );

    // Gossip carries node1's higher-epoch ownership to node2, which then redirects
    // the key to node1 — convergence with no operator action on node2.
    let deadline = Instant::now() + Duration::from_secs(6);
    let converged = loop {
        let r = c2.cmd(&["GET", &k]);
        if r.contains("MOVED") && r.contains(&a1) {
            break true;
        }
        if Instant::now() > deadline {
            break false;
        }
        sleep(Duration::from_millis(150));
    };
    assert!(converged, "node2 did not adopt node1's topology via gossip");

    let _ = n1.kill();
    let _ = n1.wait();
    let _ = n2.kill();
    let _ = n2.wait();
}

#[test]
fn resp3_pubsub_uses_push_frames() {
    fn drain(s: &mut TcpStream) -> Vec<u8> {
        s.set_read_timeout(Some(Duration::from_millis(300)))
            .unwrap();
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        while let Ok(n) = s.read(&mut chunk) {
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
        }
        buf
    }
    let contains = |hay: &[u8], needle: &[u8]| hay.windows(needle.len()).any(|w| w == needle);

    let s = Server::start();
    // RESP3 subscriber (raw, so we can inspect the frame tag).
    let mut sub3 = TcpStream::connect(("127.0.0.1", s.port)).unwrap();
    send_resp(&mut sub3, &[b"HELLO", b"3"]);
    send_resp(&mut sub3, &[b"SUBSCRIBE", b"ch"]);
    // RESP2 subscriber.
    let mut sub2 = TcpStream::connect(("127.0.0.1", s.port)).unwrap();
    send_resp(&mut sub2, &[b"SUBSCRIBE", b"ch"]);
    sleep(Duration::from_millis(200)); // let both subscriptions register

    s.connect().cmd(&["PUBLISH", "ch", "hi"]);
    sleep(Duration::from_millis(100));

    // RESP3 gets a push frame (>), RESP2 the legacy array (*).
    assert!(
        contains(&drain(&mut sub3), b">3\r\n$7\r\nmessage"),
        "RESP3 subscriber should receive a push (>) frame"
    );
    assert!(
        contains(&drain(&mut sub2), b"*3\r\n$7\r\nmessage"),
        "RESP2 subscriber should receive an array (*) frame"
    );
}

// === partial resync (PSYNC CONTINUE) =========================================

fn send_resp(s: &mut TcpStream, args: &[&[u8]]) {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        out.extend_from_slice(a);
        out.extend_from_slice(b"\r\n");
    }
    s.write_all(&out).unwrap();
}

fn read_line_raw(s: &mut TcpStream) -> Vec<u8> {
    let mut line = Vec::new();
    let mut b = [0u8; 1];
    loop {
        s.read_exact(&mut b).unwrap();
        if b[0] == b'\n' {
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            return line;
        }
        line.push(b[0]);
    }
}

/// Read one RESP array-of-bulks command from the replication stream, returning
/// (args, bytes_consumed) — bytes_consumed matches the master's offset accounting.
fn read_command_raw(s: &mut TcpStream) -> (Vec<Vec<u8>>, usize) {
    let mut consumed = 0;
    let hdr = read_line_raw(s);
    consumed += hdr.len() + 2; // + CRLF
    assert_eq!(hdr.first(), Some(&b'*'), "expected array, got {hdr:?}");
    let n: usize = std::str::from_utf8(&hdr[1..]).unwrap().parse().unwrap();
    let mut args = Vec::new();
    for _ in 0..n {
        let lh = read_line_raw(s);
        consumed += lh.len() + 2;
        assert_eq!(lh.first(), Some(&b'$'));
        let l: usize = std::str::from_utf8(&lh[1..]).unwrap().parse().unwrap();
        let mut buf = vec![0u8; l + 2];
        s.read_exact(&mut buf).unwrap();
        consumed += l + 2;
        args.push(buf[..l].to_vec());
    }
    (args, consumed)
}

/// Connect as a replica and full-sync; returns (stream, replid, offset).
fn replica_full_sync(port: u16) -> (TcpStream, String, u64) {
    let mut s = TcpStream::connect(("127.0.0.1", port)).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    send_resp(&mut s, &[b"PSYNC", b"?", b"-1"]);
    let line = read_line_raw(&mut s); // +FULLRESYNC <replid> <offset>
    let text = String::from_utf8_lossy(&line);
    let mut parts = text.trim_start_matches('+').split_whitespace();
    assert_eq!(parts.next(), Some("FULLRESYNC"));
    let replid = parts.next().unwrap().to_string();
    let offset: u64 = parts.next().unwrap().parse().unwrap();
    // Snapshot bulk: $<len>\r\n<len bytes> (no trailing CRLF).
    let hdr = read_line_raw(&mut s);
    assert_eq!(hdr.first(), Some(&b'$'));
    let len: usize = std::str::from_utf8(&hdr[1..]).unwrap().parse().unwrap();
    let mut snap = vec![0u8; len];
    s.read_exact(&mut snap).unwrap();
    (s, replid, offset)
}

#[test]
fn replica_partial_resync_after_reconnect() {
    let server = Server::start();
    let mut writer = server.connect();

    // Attach as a replica and full-sync (this activates the backlog).
    let (mut repl, replid, off0) = replica_full_sync(server.port);

    // A write streams to us; track our processed offset.
    assert_eq!(writer.cmd(&["SET", "a", "1"]), "OK");
    let (cmd1, n1) = read_command_raw(&mut repl);
    assert_eq!(cmd1[0], b"SET");
    let my_offset = off0 + n1 as u64;

    // Drop the link, but the master keeps the offset + backlog advancing.
    drop(repl);
    assert_eq!(writer.cmd(&["SET", "b", "2"]), "OK"); // missed while "disconnected"
    sleep(Duration::from_millis(100));

    // Reconnect with our last offset -> partial resync, no full snapshot.
    let mut repl2 = TcpStream::connect(("127.0.0.1", server.port)).unwrap();
    repl2
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    send_resp(
        &mut repl2,
        &[
            b"PSYNC",
            replid.as_bytes(),
            my_offset.to_string().as_bytes(),
        ],
    );
    let line = read_line_raw(&mut repl2);
    assert!(
        line.starts_with(b"+CONTINUE"),
        "expected +CONTINUE, got {:?}",
        String::from_utf8_lossy(&line)
    );
    // The write we missed is delivered from the backlog — nothing lost.
    let (buffered, _) = read_command_raw(&mut repl2);
    assert_eq!(buffered[0], b"SET");
    assert_eq!(buffered[1], b"b");
    assert_eq!(buffered[2], b"2");
}

// === native TLS (only built/run under `cargo test --features tls`) ===========

#[cfg(feature = "tls")]
mod tls_e2e {
    use super::*;
    use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
    use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
    use rustls::{
        ClientConfig, ClientConnection, DigitallySignedStruct, SignatureScheme, StreamOwned,
    };
    use std::sync::Arc;

    const CERT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/test-cert.pem");
    const KEY: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/test-key.pem");

    // Test-only: accept any server cert (we're exercising the server's TLS
    // termination + RESP round-trip, not client-side verification).
    #[derive(Debug)]
    struct NoVerify;
    impl ServerCertVerifier for NoVerify {
        fn verify_server_cert(
            &self,
            _: &CertificateDer,
            _: &[CertificateDer],
            _: &ServerName,
            _: &[u8],
            _: UnixTime,
        ) -> Result<ServerCertVerified, rustls::Error> {
            Ok(ServerCertVerified::assertion())
        }
        fn verify_tls12_signature(
            &self,
            _: &[u8],
            _: &CertificateDer,
            _: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            Ok(HandshakeSignatureValid::assertion())
        }
        fn verify_tls13_signature(
            &self,
            _: &[u8],
            _: &CertificateDer,
            _: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            Ok(HandshakeSignatureValid::assertion())
        }
        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            vec![
                SignatureScheme::RSA_PSS_SHA256,
                SignatureScheme::RSA_PSS_SHA384,
                SignatureScheme::RSA_PSS_SHA512,
                SignatureScheme::RSA_PKCS1_SHA256,
                SignatureScheme::ECDSA_NISTP256_SHA256,
                SignatureScheme::ED25519,
            ]
        }
    }

    /// Spawn the server with a TLS listener and return (child, tls_port).
    fn spawn_tls_server() -> (Child, u16) {
        let rdb = format!(
            "{}/locus-tls-{}.rdb",
            std::env::temp_dir().display(),
            std::process::id()
        );
        let _ = std::fs::remove_file(&rdb);
        let mut child = Command::new(env!("CARGO_BIN_EXE_locus"))
            .env("LOCUS_PORT", "0")
            .env("LOCUS_TLS_PORT", "0")
            .env("LOCUS_TLS_CERT", CERT)
            .env("LOCUS_TLS_KEY", KEY)
            .env("LOCUS_RDB", &rdb)
            .env_remove("LOCUS_AOF")
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn locus tls");
        let stdout = child.stdout.take().unwrap();
        let mut reader = BufReader::new(stdout);
        let port = loop {
            let mut line = String::new();
            assert!(
                reader.read_line(&mut line).unwrap() > 0,
                "server exited early"
            );
            if line.contains("TLS listening")
                && let Some(p) = line.rsplit(':').next().and_then(|s| s.trim().parse().ok())
            {
                break p;
            }
        };
        std::thread::spawn(move || {
            let mut sink = Vec::new();
            let _ = reader.read_to_end(&mut sink);
        });
        (child, port)
    }

    #[test]
    fn tls_handshake_and_resp_roundtrip() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let (mut child, port) = spawn_tls_server();

        let config = ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerify))
            .with_no_client_auth();
        let name = ServerName::try_from("localhost".to_string()).unwrap();
        let conn = ClientConnection::new(Arc::new(config), name).unwrap();
        let tcp = TcpStream::connect(("127.0.0.1", port)).unwrap();
        tcp.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut tls = StreamOwned::new(conn, tcp);

        // PING over TLS -> +PONG
        tls.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();
        let mut buf = [0u8; 64];
        let n = tls.read(&mut buf).unwrap();
        assert!(
            buf[..n].starts_with(b"+PONG"),
            "expected +PONG over TLS, got {:?}",
            String::from_utf8_lossy(&buf[..n])
        );

        // A real write/read round-trip over the encrypted channel.
        tls.write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n")
            .unwrap();
        let n = tls.read(&mut buf).unwrap();
        assert!(buf[..n].starts_with(b"+OK"));
        tls.write_all(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n").unwrap();
        let n = tls.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"$1\r\nv\r\n");

        let _ = child.kill();
        let _ = child.wait();
    }
}
