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
