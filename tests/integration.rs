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
