//! The performance harness — the measurements behind phase 2 of the execution
//! plan, kept in the repository so every number in it is reproducible with one
//! command:
//!
//! ```text
//! cargo test --release --test perf -- --ignored --nocapture
//! ```
//!
//! Zero-dep, exactly like `tests/integration.rs`: it spawns the compiled
//! `locus` binary and drives it over a raw socket. When a `redis-server` is on
//! the machine it spawns one too and prints both columns side by side; when
//! there is none it prints the Locus column alone and says so. A missing Redis
//! never fails the suite.
//!
//! The test is `#[ignore]`d, so the five-command commit loop and CI are
//! unaffected — you opt in with `--ignored`.
//!
//! **The floor assertions are the point.** They are ratios, not absolute
//! throughputs, so they do not flake on a loaded machine: writing into a big
//! collection must not cost dramatically more than writing into an empty one.
//! On a healthy engine that ratio is ~1x. Before phase 2.1 it measured 25-47x,
//! because the hub recomputed a value's whole size after every write — which is
//! precisely what these floors now guard against coming back.
//!
//! Sizes can be shrunk for a quick run — `LOCUS_PERF_N` (collection size,
//! default 200_000) and `LOCUS_PERF_LIST` (list size, default 500_000). Set
//! `LOCUS_PERF_NO_REDIS=1` to skip the comparison column.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

// === knobs ==================================================================

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

/// Members/fields/keys in the "big collection" workloads.
fn n_big() -> usize {
    env_usize("LOCUS_PERF_N", 200_000)
}

/// Elements in the big list (the plan measures RPUSH into a 500k list).
fn n_list() -> usize {
    env_usize("LOCUS_PERF_LIST", 500_000)
}

/// Ops in a short probe — "write this many more into that collection".
const PROBE: usize = 10_000;

/// Commands written before the replies are read back — the pipelined-client
/// shape. Deep enough to keep the server busy, small enough that neither socket
/// buffer can fill and deadlock.
const CHUNK: usize = 1_000;

/// Connections in the concurrent SET/GET workload (redis-benchmark's default).
const CONNS: usize = 50;

/// Ops in the concurrent SET/GET workload, across all connections.
const CONN_OPS: usize = 100_000;

// === the servers ============================================================

/// Hand out a TCP port from a fixed window *below* every platform's ephemeral
/// range, walked by a process-wide counter and sliced by pid — the same shape
/// `tests/integration.rs::free_port` uses, and for the same reason: bind-`:0`-
/// then-drop loses a race against anything else asking the kernel for a port.
/// Only `redis-server` needs this; the Locus child is told `LOCUS_PORT=0` and
/// reports back the port it actually got.
fn free_port() -> u16 {
    const BASE: u32 = 20_000;
    const SLICE: u32 = 96;
    const SLICES: u32 = 128;
    const SPAN: u32 = SLICE * SLICES;
    static NEXT: AtomicU64 = AtomicU64::new(0);

    let start = (std::process::id() % SLICES) * SLICE;
    for _ in 0..SPAN {
        let n = (NEXT.fetch_add(1, Ordering::Relaxed) % u64::from(SPAN)) as u32;
        let port = (BASE + (start + n) % SPAN) as u16;
        if let Ok(listener) = TcpListener::bind(("127.0.0.1", port)) {
            drop(listener);
            return port;
        }
    }
    panic!("no free port in {}..{}", BASE, BASE + SPAN);
}

/// A running `locus`, killed and cleaned up on drop.
struct Locus {
    child: Child,
    port: u16,
    rdb: String,
}

impl Locus {
    fn start() -> Locus {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let rdb = format!(
            "{}/locus-perf-{}-{}.rdb",
            std::env::temp_dir().display(),
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed),
        );
        let _ = std::fs::remove_file(&rdb);
        let mut child = Command::new(env!("CARGO_BIN_EXE_locus"))
            .env("LOCUS_PORT", "0")
            .env("LOCUS_RDB", &rdb)
            .env_remove("LOCUS_AOF")
            .env_remove("LOCUS_MAXMEMORY")
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn locus");
        let stdout = child.stdout.take().expect("child stdout");
        let mut reader = BufReader::new(stdout);
        let port = loop {
            let mut line = String::new();
            if reader.read_line(&mut line).expect("read stdout") == 0 {
                panic!("locus exited before it started listening");
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
        thread::spawn(move || {
            let mut sink = Vec::new();
            let _ = reader.read_to_end(&mut sink);
        });
        Locus { child, port, rdb }
    }
}

impl Drop for Locus {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_file(&self.rdb);
    }
}

/// A running `redis-server`, or `None` when the machine has no Redis. Snapshots
/// and the AOF are off so the comparison measures the engine, not the disk.
struct Redis {
    child: Child,
    port: u16,
    dir: String,
}

impl Redis {
    fn start() -> Option<Redis> {
        if std::env::var("LOCUS_PERF_NO_REDIS").is_ok() {
            return None;
        }
        Command::new("redis-server")
            .arg("--version")
            .output()
            .ok()?;
        let port = free_port();
        let dir = format!(
            "{}/locus-perf-redis-{}-{}",
            std::env::temp_dir().display(),
            std::process::id(),
            port
        );
        std::fs::create_dir_all(&dir).ok()?;
        let child = Command::new("redis-server")
            .args([
                "--port",
                &port.to_string(),
                "--bind",
                "127.0.0.1",
                "--save",
                "",
                "--appendonly",
                "no",
                "--protected-mode",
                "no",
                "--dir",
                &dir,
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .ok()?;
        let r = Redis { child, port, dir };
        // Wait for the listener; give up (and skip the column) if it never comes.
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            if TcpStream::connect(("127.0.0.1", port)).is_ok() {
                return Some(r);
            }
            thread::sleep(Duration::from_millis(50));
        }
        None
    }
}

impl Drop for Redis {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

// === the client =============================================================

/// A minimal RESP client. Replies are parsed only far enough to consume them
/// exactly (and to notice an error), which is all a throughput probe needs.
struct Conn {
    stream: TcpStream,
    reader: BufReader<TcpStream>,
}

impl Conn {
    fn open(port: u16) -> Conn {
        let stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        stream.set_nodelay(true).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(300)))
            .unwrap();
        Conn {
            reader: BufReader::with_capacity(1 << 16, stream.try_clone().unwrap()),
            stream,
        }
    }

    /// Consume exactly one reply; returns its header line (without CRLF) so the
    /// caller can spot a `-ERR`. Bulk and aggregate bodies are read and dropped.
    fn read_reply(&mut self) -> Vec<u8> {
        let mut line = Vec::new();
        let n = self
            .reader
            .read_until(b'\n', &mut line)
            .expect("read reply");
        assert!(n > 0, "server closed the connection mid-benchmark");
        while matches!(line.last(), Some(b'\n') | Some(b'\r')) {
            line.pop();
        }
        let count = |line: &[u8]| -> i64 {
            std::str::from_utf8(&line[1..])
                .unwrap_or("0")
                .trim()
                .parse()
                .unwrap_or(0)
        };
        match line.first().copied() {
            Some(b'$') | Some(b'=') => {
                let n = count(&line);
                if n >= 0 {
                    let mut buf = vec![0u8; n as usize + 2];
                    self.reader.read_exact(&mut buf).expect("read bulk body");
                }
            }
            Some(b'*') | Some(b'~') | Some(b'>') => {
                for _ in 0..count(&line).max(0) {
                    self.read_reply();
                }
            }
            Some(b'%') => {
                for _ in 0..(count(&line).max(0) * 2) {
                    self.read_reply();
                }
            }
            _ => {}
        }
        line
    }

    fn expect_ok(reply: &[u8]) {
        assert!(
            reply.first() != Some(&b'-'),
            "command failed: {}",
            String::from_utf8_lossy(reply)
        );
    }

    /// One command, one reply — the request/response shape.
    fn cmd(&mut self, args: &[&[u8]]) {
        let mut buf = Vec::new();
        encode_into(&mut buf, args);
        self.stream.write_all(&buf).expect("write");
        let reply = self.read_reply();
        Self::expect_ok(&reply);
    }

    /// Send a pre-encoded batch chunk by chunk and read every reply back.
    /// Encoding happens outside this call on purpose: the elapsed time is the
    /// server's throughput, not the client's ability to format bytes.
    fn run(&mut self, batch: &Batch) -> Duration {
        let t0 = Instant::now();
        let mut start = 0usize;
        for &(end, n) in &batch.chunks {
            self.stream
                .write_all(&batch.buf[start..end])
                .expect("write");
            for _ in 0..n {
                let reply = self.read_reply();
                Self::expect_ok(&reply);
            }
            start = end;
        }
        t0.elapsed()
    }

    /// Repeat one command until either bound is hit; returns (ops, elapsed).
    /// Latency probes need this: a 133 ms `GEOSEARCH` and a 40 us one cannot
    /// share a fixed op count without either flaking or taking four minutes.
    fn probe(&mut self, args: &[&[u8]], max_ops: usize, budget: Duration) -> (usize, Duration) {
        let mut buf = Vec::new();
        encode_into(&mut buf, args);
        let t0 = Instant::now();
        let mut ops = 0;
        while ops < max_ops && t0.elapsed() < budget {
            self.stream.write_all(&buf).expect("write");
            let reply = self.read_reply();
            Self::expect_ok(&reply);
            ops += 1;
        }
        (ops, t0.elapsed())
    }
}

fn encode_into(buf: &mut Vec<u8>, args: &[&[u8]]) {
    buf.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
    for a in args {
        buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        buf.extend_from_slice(a);
        buf.extend_from_slice(b"\r\n");
    }
}

/// A pre-encoded run of commands, split into pipeline chunks.
struct Batch {
    buf: Vec<u8>,
    chunks: Vec<(usize, usize)>, // (end offset, commands in this chunk)
}

impl Batch {
    /// Build `n` commands, `f` filling in the arguments of the i-th.
    fn build<F>(n: usize, mut f: F) -> Batch
    where
        F: FnMut(usize, &mut Vec<Vec<u8>>),
    {
        let mut buf = Vec::with_capacity(n * 48);
        let mut chunks = Vec::with_capacity(n / CHUNK + 1);
        let mut in_chunk = 0;
        let mut args: Vec<Vec<u8>> = Vec::new();
        for i in 0..n {
            args.clear();
            f(i, &mut args);
            let refs: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
            encode_into(&mut buf, &refs);
            in_chunk += 1;
            if in_chunk == CHUNK {
                chunks.push((buf.len(), in_chunk));
                in_chunk = 0;
            }
        }
        if in_chunk > 0 {
            chunks.push((buf.len(), in_chunk));
        }
        Batch { buf, chunks }
    }
}

fn ops_per_sec(n: usize, d: Duration) -> f64 {
    n as f64 / d.as_secs_f64().max(f64::MIN_POSITIVE)
}

// === the workloads ==========================================================

#[derive(Clone, Copy, PartialEq)]
enum Flavor {
    /// Locus: one key *is* one point (`GEOSET key lon lat`).
    Locus,
    /// Redis: points are members of one key (`GEOADD key lon lat member`).
    Redis,
}

/// One measured row. Every figure is ops/s; `latency` marks the rows where the
/// per-op millisecond figure is the one that matters, and is printed too.
struct Metric {
    name: String,
    ops: f64,
    latency: bool,
}

fn metric(name: &str, ops: f64) -> Metric {
    Metric {
        name: name.to_string(),
        ops,
        latency: false,
    }
}

fn latency(name: &str, ops: f64) -> Metric {
    Metric {
        name: name.to_string(),
        ops,
        latency: true,
    }
}

/// A stable, spread-out point cloud over roughly one city (0.5 deg x 0.5 deg,
/// about 44 km x 55 km at this latitude) — dense enough that a 1 km query has
/// real neighbours and a 20 km query covers most of the set.
const GEO_LON0: f64 = 13.10;
const GEO_LAT0: f64 = 38.00;
const GEO_SPAN: f64 = 0.5;

fn geo_point(i: usize) -> (f64, f64) {
    // A tiny LCG: reproducible, and not a grid (a grid would flatter the index).
    let h = (i as u64)
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    let a = ((h >> 11) & 0xFFFF) as f64 / 65_536.0;
    let b = ((h >> 33) & 0xFFFF) as f64 / 65_536.0;
    (GEO_LON0 + a * GEO_SPAN, GEO_LAT0 + b * GEO_SPAN)
}

fn member(i: usize) -> Vec<u8> {
    format!("m{i:09}").into_bytes()
}

/// Arguments for one collection write — the same command shape for all four
/// types, so "into an empty one" and "into a big one" differ only in the target.
fn collection_args(verb: &[u8], key: &str, i: usize, args: &mut Vec<Vec<u8>>) {
    args.push(verb.to_vec());
    args.push(key.as_bytes().to_vec());
    if verb == b"ZADD" {
        args.push(i.to_string().into_bytes());
        args.push(member(i));
    } else if verb == b"HSET" {
        args.push(member(i));
        args.push(b"v".to_vec());
    } else {
        args.push(member(i));
    }
}

/// Run the whole suite against one server. Rows come back in a fixed order so
/// the two targets can be zipped into one table.
fn run_suite(port: u16, flavor: Flavor) -> Vec<Metric> {
    let n = n_big();
    let nl = n_list();
    let mut out = Vec::new();
    let mut c = Conn::open(port);

    // --- SET / GET across CONNS connections, one in flight per connection ---
    let per = (CONN_OPS / CONNS).max(1);
    for (label, verb) in [("SET", b"SET".as_slice()), ("GET", b"GET".as_slice())] {
        let t0 = Instant::now();
        let handles: Vec<_> = (0..CONNS)
            .map(|t| {
                thread::spawn(move || {
                    let mut c = Conn::open(port);
                    for i in 0..per {
                        let key = format!("k:{t}:{i}");
                        if verb == b"SET" {
                            c.cmd(&[verb, key.as_bytes(), b"xxxxxxxxxxxxxxxx"]);
                        } else {
                            c.cmd(&[verb, key.as_bytes()]);
                        }
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        out.push(metric(
            &format!("{label} / {CONNS} conn"),
            ops_per_sec(per * CONNS, t0.elapsed()),
        ));
    }
    c.cmd(&[b"FLUSHALL"]);

    // --- SET into N separate keys: the control ------------------------------
    // The decisive comparison in the plan. Each write here touches a *small*
    // value, so per-write work cannot grow — whatever this costs is the floor
    // the four collection builds below should also sit near.
    let batch = Batch::build(n, |i, args| {
        args.push(b"SET".to_vec());
        args.push(format!("key:{i:09}").into_bytes());
        args.push(b"xxxxxxxxxxxxxxxx".to_vec());
    });
    out.push(metric(
        "SET -> N separate keys",
        ops_per_sec(n, c.run(&batch)),
    ));
    drop(batch);
    c.cmd(&[b"FLUSHALL"]);

    // --- the four big-collection builds ------------------------------------
    // Each type is measured three ways: the whole build from empty (the plan's
    // headline row), a short probe into a *fresh empty* collection, and the
    // same probe into the finished big one. The last two are the ratio the
    // floors assert on — same command, same count, only the target differs.
    for (label, verb, big, small, count) in [
        ("SADD", b"SADD".as_slice(), "set:big", "set:small", n),
        ("ZADD", b"ZADD".as_slice(), "zset:big", "zset:small", n),
        ("HSET", b"HSET".as_slice(), "hash:big", "hash:small", n),
        ("RPUSH", b"RPUSH".as_slice(), "list:big", "list:small", nl),
    ] {
        let batch = Batch::build(PROBE, |i, a| collection_args(verb, small, i, a));
        let empty = ops_per_sec(PROBE, c.run(&batch));
        drop(batch);

        let batch = Batch::build(count, |i, a| collection_args(verb, big, i, a));
        let build = ops_per_sec(count, c.run(&batch));
        drop(batch);

        let batch = Batch::build(PROBE, |i, a| collection_args(verb, big, count + i, a));
        let large = ops_per_sec(PROBE, c.run(&batch));
        drop(batch);

        out.push(metric(&format!("{label} -> build {count}, one key"), build));
        out.push(metric(&format!("{label} into an empty collection"), empty));
        out.push(metric(
            &format!("{label} into the {count}-element one"),
            large,
        ));

        // The big zset is alive right now — read ranges out of it before the
        // flush. (Fixing those reads is session 3; this only measures them.)
        if verb == b"ZADD" {
            let budget = Duration::from_secs(3);
            let (ops, d) = c.probe(&[b"ZRANGE", b"zset:big", b"0", b"9"], 5_000, budget);
            out.push(latency("ZRANGE key 0 9", ops_per_sec(ops, d)));
            let args: &[&[u8]] = &[b"ZRANGEBYSCORE", b"zset:big", b"1000", b"1010"];
            let (ops, d) = c.probe(args, 5_000, budget);
            out.push(latency("ZRANGEBYSCORE 1000 1010", ops_per_sec(ops, d)));
        }
        c.cmd(&[b"FLUSHALL"]);
    }

    // --- geo: ingest N points, then query at 1 km and 20 km ------------------
    let batch = Batch::build(n, |i, args| {
        let (lon, lat) = geo_point(i);
        match flavor {
            Flavor::Locus => {
                args.push(b"GEOSET".to_vec());
                args.push(format!("p:{i:09}").into_bytes());
                args.push(format!("{lon:.6}").into_bytes());
                args.push(format!("{lat:.6}").into_bytes());
            }
            Flavor::Redis => {
                args.push(b"GEOADD".to_vec());
                args.push(b"geo".to_vec());
                args.push(format!("{lon:.6}").into_bytes());
                args.push(format!("{lat:.6}").into_bytes());
                args.push(member(i));
            }
        }
    });
    out.push(metric(
        "GEO ingest, N points",
        ops_per_sec(n, c.run(&batch)),
    ));
    drop(batch);

    let clon = format!("{:.6}", GEO_LON0 + GEO_SPAN / 2.0);
    let clat = format!("{:.6}", GEO_LAT0 + GEO_SPAN / 2.0);
    for (label, radius) in [("1 km", "1"), ("20 km", "20")] {
        let args: Vec<&[u8]> = match flavor {
            Flavor::Locus => vec![
                b"GEOSEARCH",
                b"FROMLONLAT",
                clon.as_bytes(),
                clat.as_bytes(),
                b"BYRADIUS",
                radius.as_bytes(),
                b"km",
                b"ASC",
                b"COUNT",
                b"10",
            ],
            Flavor::Redis => vec![
                b"GEOSEARCH",
                b"geo",
                b"FROMLONLAT",
                clon.as_bytes(),
                clat.as_bytes(),
                b"BYRADIUS",
                radius.as_bytes(),
                b"km",
                b"ASC",
                b"COUNT",
                b"10",
            ],
        };
        let (ops, d) = c.probe(&args, 2_000, Duration::from_secs(3));
        out.push(latency(
            &format!("GEOSEARCH {label} COUNT 10"),
            ops_per_sec(ops, d),
        ));
    }
    c.cmd(&[b"FLUSHALL"]);
    out
}

// === reporting ==============================================================

fn fmt_ops(v: f64) -> String {
    if v >= 1000.0 {
        format!("{v:>11.0}")
    } else {
        format!("{v:>11.1}")
    }
}

fn print_table(locus: &[Metric], redis: Option<&[Metric]>) {
    let (n, nl) = (n_big(), n_list());
    println!();
    println!("=== Locus perf harness — N={n}, list={nl}, {CONNS} conns, pipeline {CHUNK} ===");
    println!();
    if redis.is_some() {
        println!(
            "| {:<38} | {:>11} | {:>11} | {:>6} |",
            "Operation (ops/s)", "Locus", "Redis", "Gap"
        );
        println!("|{:-<40}|{:->13}|{:->13}|{:->8}|", "", "", "", "");
    } else {
        println!(
            "| {:<38} | {:>11} |   (no redis-server on this machine)",
            "Operation (ops/s)", "Locus"
        );
        println!("|{:-<40}|{:->13}|", "", "");
    }
    for (i, m) in locus.iter().enumerate() {
        let lat = if m.latency {
            let ms = 1000.0 / m.ops.max(f64::MIN_POSITIVE);
            format!("   {ms:.3} ms/op")
        } else {
            String::new()
        };
        match redis.and_then(|r| r.get(i)) {
            Some(r) => {
                let gap = format!("{:.1}x", r.ops / m.ops.max(f64::MIN_POSITIVE));
                let (lo, re) = (fmt_ops(m.ops), fmt_ops(r.ops));
                println!("| {:<38} | {lo} | {re} | {gap:>6} |{lat}", m.name);
            }
            None => {
                let lo = fmt_ops(m.ops);
                println!("| {:<38} | {lo} |{lat}", m.name);
            }
        }
    }
    println!();
}

fn find<'a>(m: &'a [Metric], name: &str) -> &'a Metric {
    m.iter()
        .find(|x| x.name.starts_with(name))
        .unwrap_or_else(|| panic!("no metric named {name}"))
}

// === the test ===============================================================

/// Print the table, then assert the floors.
///
/// One test, not several: these workloads build multi-hundred-megabyte
/// collections and saturate a core, so two of them running concurrently (which
/// is what `cargo test` does by default) would measure each other rather than
/// the server.
///
/// The floors are deliberately ratios, not absolute throughputs. A ratio does
/// not flake when the machine is busy, and the defect they guard against is
/// exactly a ratio: per-write work that grows with the collection. 5x is
/// generous — a healthy engine sits near 1x and the phase-2.1 defect measured
/// 25-47x — which makes this a regression alarm rather than a benchmark.
#[test]
#[ignore = "perf harness: cargo test --release --test perf -- --ignored --nocapture"]
fn perf_table() {
    let locus = Locus::start();
    let l = run_suite(locus.port, Flavor::Locus);
    let r = Redis::start().map(|r| run_suite(r.port, Flavor::Redis));
    print_table(&l, r.as_deref());

    let mut failures = Vec::new();
    for label in ["SADD", "ZADD", "HSET", "RPUSH"] {
        let empty = find(&l, &format!("{label} into an empty")).ops;
        let large = find(&l, &format!("{label} into the")).ops;
        let ratio = empty / large.max(f64::MIN_POSITIVE);
        if ratio > 5.0 {
            failures.push(format!(
                "{label} into a big collection is {ratio:.1}x slower than into an empty one \
                 ({large:.0} vs {empty:.0} ops/s)"
            ));
        }
    }
    // A catastrophic-regression tripwire, an order of magnitude below what this
    // model does, so it fires on a broken build and not on a busy machine.
    let set = find(&l, "SET / ").ops;
    if set < 5_000.0 {
        failures.push(format!(
            "SET across {CONNS} connections fell to {set:.0} ops/s"
        ));
    }
    assert!(
        failures.is_empty(),
        "perf floors breached — per-write work is growing with the collection:\n  {}",
        failures.join("\n  ")
    );
}
