//! **The differential harness** — part A of phase 5.2, the credibility gate.
//!
//! Randomized command sequences are executed twice: once against the `locusdb`
//! engine **in this process** (a plain [`Db`] plus `execute` — no server, no
//! ports, no threads) and once against a real `redis-server` over a socket. The
//! two replies are compared after a written-down set of normalizations, and any
//! divergence that is not on that list fails the run and prints the seed, the
//! whole sequence, and both replies.
//!
//! The asymmetry is the point. Driving Locus in-process removes every port
//! race, every timing variable and every connection-layer effect from *our*
//! side of the diff, so a difference is a difference in the **engine**. It also
//! means this half never exercises the hub, replication or the connection
//! layer — `tests/fault.rs` covers those over a real socket.
//!
//! ```text
//! cargo test --test differential                      # the smoke subset
//! cargo test --test differential -- --ignored --nocapture   # the long run + the coverage report
//! ```
//!
//! Zero-dep, exactly like `tests/perf.rs` and `tests/integration.rs`.
//! `redis-server` is discovered on `PATH`; when there is none, every test here
//! prints why and passes. A missing Redis never fails the suite.
//!
//! Knobs: `LOCUS_DIFF_SEQS` / `LOCUS_DIFF_LEN` (sequences and commands per
//! sequence in the `--ignored` run), `LOCUS_DIFF_SEED` (start seed — a failure
//! prints the seed that reproduces it), `LOCUS_DIFF_NO_REDIS=1` to skip.

use locusdb::{Db, execute};
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// === the PRNG ===============================================================

/// xorshift64. Seeded, so a failing run reproduces from its printed seed.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Rng {
        // Any odd, non-zero state; xorshift is degenerate at 0.
        Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }
    fn pick<'a>(&mut self, xs: &[&'a str]) -> &'a str {
        xs[self.below(xs.len())]
    }
    fn int(&mut self, lo: i64, hi: i64) -> i64 {
        lo + (self.next_u64() % (hi - lo + 1) as u64) as i64
    }
    /// True `pct` percent of the time.
    fn chance(&mut self, pct: u64) -> bool {
        self.next_u64() % 100 < pct
    }
}

// === RESP replies ===========================================================

#[derive(Clone, PartialEq)]
enum Reply {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Vec<u8>),
    Nil,
    Array(Vec<Reply>),
}

/// One reply, from anything that reads bytes. `&[u8]` is `BufRead`, so the same
/// parser handles the in-process byte buffer and the socket — the two sides of
/// the diff are decoded by identical code, which is what makes the comparison
/// meaningful.
fn read_reply<R: BufRead>(r: &mut R) -> Reply {
    let mut line = Vec::new();
    let n = r.read_until(b'\n', &mut line).expect("read reply");
    assert!(n > 0, "connection closed mid-reply");
    while matches!(line.last(), Some(b'\n') | Some(b'\r')) {
        line.pop();
    }
    assert!(!line.is_empty(), "empty reply line");
    let (tag, rest) = line.split_at(1);
    let text = String::from_utf8_lossy(rest).to_string();
    let count = |t: &str| -> i64 { t.trim().parse().expect("reply length") };
    match tag[0] {
        b'+' => Reply::Simple(text),
        b'-' => Reply::Error(text),
        b':' => Reply::Int(count(&text)),
        b'$' | b'=' => {
            let len = count(&text);
            if len < 0 {
                return Reply::Nil;
            }
            let mut buf = vec![0u8; len as usize + 2];
            r.read_exact(&mut buf).expect("bulk body");
            buf.truncate(len as usize);
            Reply::Bulk(buf)
        }
        b'*' | b'~' | b'>' => {
            let n = count(&text);
            if n < 0 {
                return Reply::Nil;
            }
            Reply::Array((0..n).map(|_| read_reply(r)).collect())
        }
        b'%' => {
            let n = count(&text);
            Reply::Array((0..n.max(0) * 2).map(|_| read_reply(r)).collect())
        }
        b'_' => Reply::Nil,
        b',' | b'#' => Reply::Bulk(text.into_bytes()),
        other => panic!("unexpected reply tag {:?}", other as char),
    }
}

fn show_bytes(b: &[u8]) -> String {
    String::from_utf8(b.to_vec()).unwrap_or_else(|_| format!("{b:x?}"))
}

/// A stable rendering, used both for failure output and as the sort key when a
/// reply is compared as a multiset.
fn render(r: &Reply) -> String {
    match r {
        Reply::Simple(s) => format!("+{s}"),
        Reply::Error(s) => format!("-{s}"),
        Reply::Int(i) => format!(":{i}"),
        Reply::Bulk(b) => format!("${}", show_bytes(b)),
        Reply::Nil => "(nil)".into(),
        Reply::Array(v) => format!("[{}]", v.iter().map(render).collect::<Vec<_>>().join(", ")),
    }
}

fn show_cmd(c: &[Vec<u8>]) -> String {
    c.iter()
        .map(|a| {
            let s = show_bytes(a);
            if s.is_empty() || s.contains(' ') {
                format!("{s:?}")
            } else {
                s
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

// === the reference server ===================================================

/// This harness's id in `free_port`'s slice map: the command differential.
const HARNESS: u32 = 2;

/// Hand out a TCP port from a fixed window *below* every platform's ephemeral
/// range, so the kernel never allocates from it on its own.
///
/// The window is sliced two ways, and both matter. The **low two bits of the
/// slice index are the harness's own id** (`HARNESS` below), so the four test
/// binaries `cargo test` runs *concurrently* — integration, fault, differential,
/// perf — can never draw the same number as each other however their pids fall.
/// That is a guarantee, not a probability, and it is what session 8 needed: it
/// added a third and fourth server-spawning binary to a suite that previously
/// leaned on consecutive pids landing in different slices. The **rest of the
/// index is the pid** (mod 64), which separates two concurrent `cargo test`
/// processes.
///
/// Within a slice a process-wide counter walks the numbers, so no two callers
/// here are given the same one. Each candidate is still bind-checked, in case
/// something unrelated on the machine holds it.
///
/// The old shape — bind `:0`, keep the number, drop the listener — left a race
/// the suite lost about one run in four: between the drop and the child's own
/// bind the kernel could hand that ephemeral port to anything else asking for
/// `:0`, and the child then died at startup on `EADDRINUSE`.
fn free_port() -> u16 {
    // 16_384..32_768: above the crowded low ports, below Linux's ephemeral
    // range (32_768) and macOS's (49_152), so the kernel never draws from it.
    const BASE: u32 = 16_384;
    const SLICE: u32 = 64; // ports per slice; the busiest harness draws ~35
    const HARNESSES: u32 = 4; // integration, perf, differential, fault
    const GROUPS: u32 = 64; // pid groups
    const SLICES: u32 = GROUPS * HARNESSES; // 256
    const SPAN: u32 = SLICE * SLICES; // 16_384
    static NEXT: AtomicU64 = AtomicU64::new(0);

    let slice = (std::process::id() % GROUPS) * HARNESSES + HARNESS;
    let start = slice * SLICE;
    for _ in 0..SPAN {
        let n = (NEXT.fetch_add(1, Ordering::Relaxed) % u64::from(SPAN)) as u32;
        let port = (BASE + (start + n) % SPAN) as u16;
        // Dropped at once — the child re-binds it. Safe: this process will not
        // hand the number out again, no sibling harness draws from this slice,
        // and the kernel does not allocate from this window.
        if let Ok(listener) = TcpListener::bind(("127.0.0.1", port)) {
            drop(listener);
            return port;
        }
    }
    panic!("no free port in {}..{}", BASE, BASE + SPAN);
}

/// A `redis-server` child plus the connection to it. Killed on drop.
struct Redis {
    child: Child,
    dir: String,
    stream: TcpStream,
    reader: BufReader<TcpStream>,
    version: String,
}

impl Redis {
    /// `None` when the machine has no `redis-server` — the caller then skips.
    fn start() -> Option<Redis> {
        if std::env::var("LOCUS_DIFF_NO_REDIS").is_ok() {
            return None;
        }
        let out = Command::new("redis-server")
            .arg("--version")
            .output()
            .ok()?;
        let version = String::from_utf8_lossy(&out.stdout).trim().to_string();
        let port = free_port();
        let dir = format!(
            "{}/locus-diff-{}-{}",
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
        let mut child = child;
        let deadline = Instant::now() + Duration::from_secs(10);
        let connected = loop {
            if let Ok(stream) = TcpStream::connect(("127.0.0.1", port)) {
                break Some(stream);
            }
            if Instant::now() >= deadline {
                break None;
            }
            std::thread::sleep(Duration::from_millis(50));
        };
        let Some(stream) = connected else {
            // It never came up. Leave nothing behind and let the caller skip.
            let _ = child.kill();
            let _ = child.wait();
            let _ = std::fs::remove_dir_all(&dir);
            return None;
        };
        stream.set_nodelay(true).expect("nodelay");
        stream
            .set_read_timeout(Some(Duration::from_secs(30)))
            .expect("read timeout");
        let reader = BufReader::with_capacity(1 << 16, stream.try_clone().expect("clone"));
        let mut r = Redis {
            child,
            dir,
            stream,
            reader,
            version,
        };
        // A listening socket is not a ready server. Handshake before handing it
        // out, so a reference engine that accepted the connection and then went
        // away shows up here as a clean skip rather than as a `ConnectionReset`
        // in the middle of somebody's assertions.
        if r.run(&Cmd::new("PING").done()) != Reply::Simple("PONG".into()) {
            return None;
        }
        Some(r)
    }

    fn run(&mut self, args: &[Vec<u8>]) -> Reply {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            out.extend_from_slice(a);
            out.extend_from_slice(b"\r\n");
        }
        self.stream.write_all(&out).expect("write to redis");
        read_reply(&mut self.reader)
    }
}

impl Drop for Redis {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Run the same command on both engines.
fn both(db: &mut Db, redis: &mut Redis, args: &[Vec<u8>]) -> (Reply, Reply) {
    let bytes = execute(args, db);
    let mut cur: &[u8] = &bytes;
    let mine = read_reply(&mut cur);
    assert!(
        cur.is_empty(),
        "locus emitted trailing bytes after one reply for `{}`: {:?}",
        show_cmd(args),
        show_bytes(cur)
    );
    let theirs = redis.run(args);
    (mine, theirs)
}

fn skip(why: &str) {
    println!("SKIPPED: {why}");
}

/// Hold this for the length of a test that spawns a `redis-server`.
///
/// Rust runs a binary's tests on parallel threads, so without it every test here
/// has its own reference server alive at once — and stacked on top of the other
/// harnesses' children during a full `cargo test`, that is enough process and
/// descriptor pressure to get one of them killed mid-assertion (observed once in
/// ten full runs, as a `ConnectionReset` inside `read_reply`). One at a time
/// costs nothing here: the whole default set finishes in well under a second.
/// Same shape as `aof.rs`'s fsync-fault lock, and for the same kind of reason.
fn one_reference_server_at_a_time() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

// === the normalization allow-list ===========================================
//
// Every entry here is a difference that is *legitimate* — the two engines are
// both correct and the protocol does not pin the byte. Each one is justified in
// the table in the session-8 delivery report, and each is asserted rather than
// masked: `Unordered` still requires the same multiset, `Floats` still requires
// the same number, `Ttl*` still requires the same sign and a value inside the
// tolerance, and an error still requires the same error **code**. Anything not
// on this list is a divergence and fails the run.

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Norm {
    /// Byte for byte.
    Exact,
    /// Leaf bulks that both parse as a float compare numerically (`3` vs `3.0`).
    Floats,
    /// The top-level array is a multiset — hash/set iteration order is not
    /// specified by the protocol and both engines are free to choose it.
    Unordered,
    /// The top-level array is a multiset of *adjacent pairs* (`HGETALL`), so
    /// field/value association is still checked.
    UnorderedPairs,
    /// A TTL in seconds: same sign, and within a tolerance of wall clock.
    TtlSeconds,
    /// A TTL in milliseconds.
    TtlMillis,
}

/// The rule for one command. `args` is inspected because the same command can
/// need different treatment (`ZRANGE … WITHSCORES` returns doubles; plain
/// `ZRANGE` does not).
fn norm_for(args: &[Vec<u8>]) -> Norm {
    let cmd = String::from_utf8_lossy(&args[0]).to_ascii_uppercase();
    let has = |w: &str| {
        args[1..]
            .iter()
            .any(|a| a.eq_ignore_ascii_case(w.as_bytes()))
    };
    match cmd.as_str() {
        "TTL" => Norm::TtlSeconds,
        "PTTL" => Norm::TtlMillis,
        "HGETALL" => Norm::UnorderedPairs,
        "HKEYS" | "HVALS" | "SMEMBERS" | "SINTER" | "SUNION" | "SDIFF" | "KEYS" => Norm::Unordered,
        "ZSCORE" | "ZMSCORE" | "ZINCRBY" | "INCRBYFLOAT" | "ZPOPMIN" | "ZPOPMAX" | "ZMPOP" => {
            Norm::Floats
        }
        "ZADD" if has("INCR") => Norm::Floats,
        "ZRANGE" | "ZREVRANGE" | "ZRANGEBYSCORE" | "ZREVRANGEBYSCORE" if has("WITHSCORES") => {
            Norm::Floats
        }
        _ => Norm::Exact,
    }
}

fn as_f64(b: &[u8]) -> Option<f64> {
    std::str::from_utf8(b).ok()?.trim().parse::<f64>().ok()
}

fn float_close(a: f64, b: f64) -> bool {
    if a.is_nan() && b.is_nan() {
        return true;
    }
    if a.is_infinite() || b.is_infinite() {
        return a == b;
    }
    (a - b).abs() <= 1e-9 * a.abs().max(b.abs()).max(1.0)
}

/// What a comparison turned up, beyond pass/fail — collected so the run can
/// report *how often* each normalization actually fired. A rule that never
/// fires is dead weight and should come off the list; a rule that fires
/// constantly is worth a look.
#[derive(Default)]
struct Notes {
    /// How often each rule was *consulted* — a comparison of that shape that
    /// the rule could have relaxed. Reported alongside the fire counts, because
    /// "the float rule never fired" only means something once you know the
    /// float rule was reached 40,000 times.
    float_seen: u64,
    error_seen: u64,
    float_format: u64,
    error_text: u64,
    reorder: u64,
    ttl_slack: u64,
}

impl Notes {
    fn merge(&mut self, o: &Notes) {
        self.float_seen += o.float_seen;
        self.error_seen += o.error_seen;
        self.float_format += o.float_format;
        self.error_text += o.error_text;
        self.reorder += o.reorder;
        self.ttl_slack += o.ttl_slack;
    }
    fn total(&self) -> u64 {
        self.float_format + self.error_text + self.reorder + self.ttl_slack
    }
}

fn same(norm: Norm, a: &Reply, b: &Reply, n: &mut Notes) -> bool {
    match (a, b) {
        (Reply::Nil, Reply::Nil) => true,
        (Reply::Simple(x), Reply::Simple(y)) => x == y,
        (Reply::Error(x), Reply::Error(y)) => {
            // The code (`ERR`, `WRONGTYPE`, `NOSCRIPT`, …) is the part the
            // protocol and every client library actually key on; the human
            // sentence after it is not specified. Codes must match exactly.
            let code = |s: &String| s.split_whitespace().next().unwrap_or("").to_string();
            n.error_seen += 1;
            if code(x) != code(y) {
                return false;
            }
            if x != y {
                n.error_text += 1;
            }
            true
        }
        (Reply::Int(x), Reply::Int(y)) => match norm {
            Norm::TtlSeconds | Norm::TtlMillis => {
                if *x < 0 || *y < 0 {
                    return x == y;
                }
                let tol = if norm == Norm::TtlSeconds { 2 } else { 2_000 };
                if x != y {
                    n.ttl_slack += 1;
                }
                (x - y).abs() <= tol
            }
            _ => x == y,
        },
        (Reply::Bulk(x), Reply::Bulk(y)) => {
            if norm == Norm::Floats && as_f64(x).is_some() && as_f64(y).is_some() {
                n.float_seen += 1;
            }
            if x == y {
                return true;
            }
            if norm == Norm::Floats
                && let (Some(fx), Some(fy)) = (as_f64(x), as_f64(y))
            {
                if float_close(fx, fy) {
                    n.float_format += 1;
                    return true;
                }
                return false;
            }
            false
        }
        (Reply::Array(x), Reply::Array(y)) => {
            if x.len() != y.len() {
                return false;
            }
            match norm {
                Norm::Unordered => {
                    let mut xs: Vec<String> = x.iter().map(render).collect();
                    let mut ys: Vec<String> = y.iter().map(render).collect();
                    xs.sort();
                    ys.sort();
                    if xs != ys {
                        return false;
                    }
                    if x.iter().map(render).collect::<Vec<_>>()
                        != y.iter().map(render).collect::<Vec<_>>()
                    {
                        n.reorder += 1;
                    }
                    true
                }
                Norm::UnorderedPairs => {
                    let pairs = |v: &Vec<Reply>| {
                        let mut p: Vec<String> = v
                            .chunks(2)
                            .map(|c| c.iter().map(render).collect::<Vec<_>>().join("\u{0}"))
                            .collect();
                        p.sort();
                        p
                    };
                    if pairs(x) != pairs(y) {
                        return false;
                    }
                    if x.iter().map(render).collect::<Vec<_>>()
                        != y.iter().map(render).collect::<Vec<_>>()
                    {
                        n.reorder += 1;
                    }
                    true
                }
                _ => x.iter().zip(y).all(|(p, q)| same(norm, p, q, n)),
            }
        }
        _ => false,
    }
}

/// A divergence, rendered the way the brief asks for: seed, sequence, both
/// replies.
struct Divergence {
    seed: u64,
    step: usize,
    seq: Vec<Vec<Vec<u8>>>,
    cmd: Vec<Vec<u8>>,
    locus: String,
    redis: String,
    what: String,
}

impl Divergence {
    fn report(&self) -> String {
        let mut s = format!(
            "\n=== DIVERGENCE (seed {}, step {}) ===\n{}\n  command : {}\n  locus   : {}\n  redis   : {}\n  sequence ({} commands):\n",
            self.seed,
            self.step,
            self.what,
            show_cmd(&self.cmd),
            self.locus,
            self.redis,
            self.seq.len()
        );
        for (i, c) in self.seq.iter().enumerate() {
            s.push_str(&format!("    {:4}  {}\n", i, show_cmd(c)));
        }
        s
    }
}

// === the sequence generator =================================================
//
// Randomized but *valid*: arity and option grammar are always well-formed, so a
// sequence explores real behaviour rather than the parser's error paths. Type
// collisions are deliberate, though — 15% of operations reach for a key another
// family owns, which is what makes `WRONGTYPE` a diffed reply rather than a
// thing that never happens.

struct Cmd(Vec<Vec<u8>>);

impl Cmd {
    fn new(name: &str) -> Cmd {
        Cmd(vec![name.as_bytes().to_vec()])
    }
    fn s(mut self, a: &str) -> Cmd {
        self.0.push(a.as_bytes().to_vec());
        self
    }
    fn n(mut self, a: i64) -> Cmd {
        self.0.push(a.to_string().into_bytes());
        self
    }
    fn b(mut self, a: &[u8]) -> Cmd {
        self.0.push(a.to_vec());
        self
    }
    fn done(self) -> Vec<Vec<u8>> {
        self.0
    }
}

const NKEYS: usize = 12;
const FAMILIES: usize = 6; // string, list, hash, set, zset, bitmap

const MEMBERS: [&str; 10] = [
    // Deliberately never numeric: `Norm::Floats` compares any leaf that parses
    // as a float numerically, so a member named "1" and one named "1.0" would
    // be masked into equality. Keeping members non-numeric removes that hole.
    "alpha", "beta", "gamma", "delta", "eps", "zeta", "eta", "theta", "iota", "kappa",
];

const VALUES: [&str; 12] = [
    "1",
    "2",
    "-3",
    "0",
    "10",
    "12345",
    "3.5",
    "abc",
    "",
    "xyzzy",
    "hello world",
    "\u{1}\u{2}bin",
];

/// A key this family owns 85% of the time; anything 15% of the time.
fn key(rng: &mut Rng, fam: usize) -> String {
    if rng.chance(85) {
        let slots: Vec<usize> = (0..NKEYS).filter(|i| i % FAMILIES == fam).collect();
        format!("d{}", slots[rng.below(slots.len())])
    } else {
        format!("d{}", rng.below(NKEYS))
    }
}

fn any_key(rng: &mut Rng) -> String {
    format!("d{}", rng.below(NKEYS))
}

fn member(rng: &mut Rng) -> String {
    rng.pick(&MEMBERS).to_string()
}

fn value(rng: &mut Rng) -> Vec<u8> {
    if rng.chance(4) {
        return vec![0xff, 0xfe, 0x00, b'z']; // not UTF-8: the wire is binary
    }
    if rng.chance(3) {
        return vec![b'a'; 300];
    }
    rng.pick(&VALUES).as_bytes().to_vec()
}

/// Scores: mostly small integers so ties happen often (tie-break order is a
/// real thing to diff), some halves, occasionally an infinity.
fn score(rng: &mut Rng) -> String {
    match rng.below(10) {
        0 => "inf".into(),
        1 => "-inf".into(),
        2..=3 => format!("{}.5", rng.int(-5, 5)),
        _ => rng.int(-6, 6).to_string(),
    }
}

/// A score-range bound for ZRANGEBYSCORE / ZCOUNT.
fn bound(rng: &mut Rng) -> String {
    match rng.below(6) {
        0 => "-inf".into(),
        1 => "+inf".into(),
        2 => format!("({}", rng.int(-6, 6)),
        _ => rng.int(-6, 6).to_string(),
    }
}

fn idx(rng: &mut Rng) -> i64 {
    if rng.chance(10) {
        rng.int(-100, 100)
    } else {
        rng.int(-6, 6)
    }
}

fn gen_command(rng: &mut Rng, now_s: i64) -> Vec<Vec<u8>> {
    match rng.below(7) {
        0 => gen_generic(rng, now_s),
        1 => gen_string(rng),
        2 => gen_list(rng),
        3 => gen_hash(rng),
        4 => gen_set(rng),
        5 => gen_zset(rng),
        _ => gen_bitmap(rng),
    }
}

fn gen_generic(rng: &mut Rng, now_s: i64) -> Vec<Vec<u8>> {
    let k = any_key(rng);
    let k2 = any_key(rng);
    match rng.below(17) {
        0 => Cmd::new("DEL").s(&k).s(&k2).done(),
        1 => Cmd::new("UNLINK").s(&k).done(),
        2 => Cmd::new("EXISTS").s(&k).s(&k2).s(&k).done(),
        3 => Cmd::new("TYPE").s(&k).done(),
        4 => Cmd::new("RENAME").s(&k).s(&k2).done(),
        5 => Cmd::new("RENAMENX").s(&k).s(&k2).done(),
        6 => {
            let c = Cmd::new("COPY").s(&k).s(&k2);
            if rng.chance(50) { c.s("REPLACE") } else { c }.done()
        }
        7 => Cmd::new("DBSIZE").done(),
        8 => Cmd::new("TOUCH").s(&k).done(),
        9 => Cmd::new("KEYS")
            .s(rng.pick(&["*", "d*", "d1*", "*1", "d[0-3]"]))
            .done(),
        // Expiry. Relative TTLs are always long enough that nothing can fire
        // between the two engines' executions of the same command; the *short*
        // path is covered deterministically by EXPIREAT-in-the-past below,
        // which deletes on both sides with no timing window at all.
        10 => Cmd::new("EXPIRE").s(&k).n(rng.int(1_000, 100_000)).done(),
        11 => Cmd::new("PEXPIRE")
            .s(&k)
            .n(rng.int(1_000_000, 100_000_000))
            .done(),
        12 => Cmd::new("EXPIREAT")
            .s(&k)
            .n(if rng.chance(25) {
                now_s - 100
            } else {
                now_s + rng.int(1_000, 100_000)
            })
            .done(),
        13 => Cmd::new("PEXPIREAT")
            .s(&k)
            .n(if rng.chance(25) {
                (now_s - 100) * 1000
            } else {
                (now_s + rng.int(1_000, 100_000)) * 1000
            })
            .done(),
        14 => Cmd::new("TTL").s(&k).done(),
        15 => Cmd::new("PTTL").s(&k).done(),
        _ => Cmd::new("PERSIST").s(&k).done(),
    }
}

fn gen_string(rng: &mut Rng) -> Vec<Vec<u8>> {
    let k = key(rng, 0);
    let v = value(rng);
    match rng.below(20) {
        0 => {
            let mut c = Cmd::new("SET").s(&k).b(&v);
            // NX and XX are generated mutually exclusively — both at once is
            // not valid SET grammar, and the odd combinations live in
            // `edge_cases` instead so they cannot cut a random run short.
            match rng.below(4) {
                0 => c = c.s("NX"),
                1 => c = c.s("XX"),
                _ => {}
            }
            match rng.below(5) {
                0 => c = c.s("EX").n(rng.int(1_000, 100_000)),
                1 => c = c.s("PX").n(rng.int(1_000_000, 100_000_000)),
                2 => c = c.s("KEEPTTL"),
                _ => {}
            }
            if rng.chance(25) {
                c = c.s("GET");
            }
            c.done()
        }
        1 => Cmd::new("GET").s(&k).done(),
        2 => Cmd::new("GETDEL").s(&k).done(),
        3 => Cmd::new("GETSET").s(&k).b(&v).done(),
        4 => Cmd::new("SETNX").s(&k).b(&v).done(),
        5 => Cmd::new("SETEX")
            .s(&k)
            .n(rng.int(1_000, 100_000))
            .b(&v)
            .done(),
        6 => Cmd::new("PSETEX")
            .s(&k)
            .n(rng.int(1_000_000, 100_000_000))
            .b(&v)
            .done(),
        7 => Cmd::new("MGET")
            .s(&k)
            .s(&any_key(rng))
            .s(&any_key(rng))
            .done(),
        8 => Cmd::new("MSET")
            .s(&k)
            .b(&v)
            .s(&key(rng, 0))
            .b(&value(rng))
            .done(),
        9 => Cmd::new("MSETNX")
            .s(&k)
            .b(&v)
            .s(&key(rng, 0))
            .b(&value(rng))
            .done(),
        10 => Cmd::new("INCR").s(&k).done(),
        11 => Cmd::new("DECR").s(&k).done(),
        12 => Cmd::new("INCRBY").s(&k).n(rng.int(-50, 50)).done(),
        13 => Cmd::new("DECRBY").s(&k).n(rng.int(-50, 50)).done(),
        // INCRBYFLOAT is deliberately *not* generated — see
        // `incrbyfloat_precision_is_f64_not_long_double`. Its result is also its
        // stored value, so a rendering that differs in the 17th decimal poisons
        // every later GET/STRLEN/APPEND on that key and the sequence stops
        // saying anything. It is covered by fixed cases in `edge_cases` instead.
        // `SET … EXAT` takes the slot: absolute expiry, otherwise ungenerated.
        14 => Cmd::new("SET")
            .s(&k)
            .b(&v)
            .s("EXAT")
            .n(4_102_444_800)
            .done(),
        15 => Cmd::new("APPEND").s(&k).b(&v).done(),
        16 => Cmd::new("GETRANGE").s(&k).n(idx(rng)).n(idx(rng)).done(),
        17 => Cmd::new("SETRANGE").s(&k).n(rng.int(0, 24)).b(&v).done(),
        18 => Cmd::new("STRLEN").s(&k).done(),
        _ => match rng.below(3) {
            0 => Cmd::new("GETEX").s(&k).done(),
            1 => Cmd::new("GETEX").s(&k).s("PERSIST").done(),
            _ => Cmd::new("GETEX")
                .s(&k)
                .s("EX")
                .n(rng.int(1_000, 100_000))
                .done(),
        },
    }
}

fn gen_list(rng: &mut Rng) -> Vec<Vec<u8>> {
    let k = key(rng, 1);
    let k2 = key(rng, 1);
    let v = value(rng);
    match rng.below(16) {
        0 => Cmd::new("LPUSH").s(&k).b(&v).b(&value(rng)).done(),
        1 => Cmd::new("RPUSH").s(&k).b(&v).b(&value(rng)).done(),
        2 => Cmd::new("LPUSHX").s(&k).b(&v).done(),
        3 => Cmd::new("RPUSHX").s(&k).b(&v).done(),
        4 => {
            let c = Cmd::new("LPOP").s(&k);
            if rng.chance(40) {
                c.n(rng.int(0, 3))
            } else {
                c
            }
            .done()
        }
        5 => {
            let c = Cmd::new("RPOP").s(&k);
            if rng.chance(40) {
                c.n(rng.int(0, 3))
            } else {
                c
            }
            .done()
        }
        6 => Cmd::new("LLEN").s(&k).done(),
        7 => Cmd::new("LRANGE").s(&k).n(idx(rng)).n(idx(rng)).done(),
        8 => Cmd::new("LINDEX").s(&k).n(idx(rng)).done(),
        9 => Cmd::new("LSET").s(&k).n(idx(rng)).b(&v).done(),
        10 => Cmd::new("LINSERT")
            .s(&k)
            .s(if rng.chance(50) { "BEFORE" } else { "AFTER" })
            .b(&value(rng))
            .b(&v)
            .done(),
        11 => Cmd::new("LREM").s(&k).n(rng.int(-3, 3)).b(&v).done(),
        12 => Cmd::new("LTRIM").s(&k).n(idx(rng)).n(idx(rng)).done(),
        13 => {
            let mut c = Cmd::new("LPOS").s(&k).b(&v);
            if rng.chance(40) {
                let r = rng.int(-3, 3);
                c = c.s("RANK").n(if r == 0 { 1 } else { r });
            }
            if rng.chance(40) {
                c = c.s("COUNT").n(rng.int(0, 3));
            }
            c.done()
        }
        14 => {
            if rng.chance(50) {
                Cmd::new("RPOPLPUSH").s(&k).s(&k2).done()
            } else {
                Cmd::new("LMOVE")
                    .s(&k)
                    .s(&k2)
                    .s(if rng.chance(50) { "LEFT" } else { "RIGHT" })
                    .s(if rng.chance(50) { "LEFT" } else { "RIGHT" })
                    .done()
            }
        }
        _ => {
            let mut c = Cmd::new("LMPOP").n(2).s(&k).s(&k2).s(if rng.chance(50) {
                "LEFT"
            } else {
                "RIGHT"
            });
            if rng.chance(50) {
                c = c.s("COUNT").n(rng.int(1, 3));
            }
            c.done()
        }
    }
}

fn gen_hash(rng: &mut Rng) -> Vec<Vec<u8>> {
    let k = key(rng, 2);
    let f = member(rng);
    let v = value(rng);
    match rng.below(11) {
        0 => Cmd::new("HSET")
            .s(&k)
            .s(&f)
            .b(&v)
            .s(&member(rng))
            .b(&value(rng))
            .done(),
        1 => Cmd::new("HSETNX").s(&k).s(&f).b(&v).done(),
        2 => Cmd::new("HGET").s(&k).s(&f).done(),
        3 => Cmd::new("HMGET").s(&k).s(&f).s(&member(rng)).done(),
        4 => Cmd::new("HGETALL").s(&k).done(),
        5 => Cmd::new("HDEL").s(&k).s(&f).s(&member(rng)).done(),
        6 => Cmd::new("HEXISTS").s(&k).s(&f).done(),
        7 => Cmd::new("HLEN").s(&k).done(),
        8 => Cmd::new("HKEYS").s(&k).done(),
        9 => Cmd::new("HVALS").s(&k).done(),
        _ => Cmd::new("HINCRBY").s(&k).s(&f).n(rng.int(-20, 20)).done(),
    }
}

fn gen_set(rng: &mut Rng) -> Vec<Vec<u8>> {
    let k = key(rng, 3);
    let k2 = key(rng, 3);
    let m = member(rng);
    match rng.below(12) {
        0 => Cmd::new("SADD").s(&k).s(&m).s(&member(rng)).done(),
        1 => Cmd::new("SREM").s(&k).s(&m).done(),
        2 => Cmd::new("SMEMBERS").s(&k).done(),
        3 => Cmd::new("SISMEMBER").s(&k).s(&m).done(),
        4 => Cmd::new("SMISMEMBER").s(&k).s(&m).s(&member(rng)).done(),
        5 => Cmd::new("SCARD").s(&k).done(),
        6 => Cmd::new(rng.pick(&["SINTER", "SUNION", "SDIFF"]))
            .s(&k)
            .s(&k2)
            .done(),
        7 => Cmd::new(rng.pick(&["SINTERSTORE", "SUNIONSTORE", "SDIFFSTORE"]))
            .s(&key(rng, 3))
            .s(&k)
            .s(&k2)
            .done(),
        8 => {
            let mut c = Cmd::new("SINTERCARD").n(2).s(&k).s(&k2);
            if rng.chance(50) {
                c = c.s("LIMIT").n(rng.int(0, 3));
            }
            c.done()
        }
        9 => Cmd::new("SMOVE").s(&k).s(&k2).s(&m).done(),
        10 => Cmd::new("SADD").s(&k).s(&m).done(),
        _ => Cmd::new("SREM").s(&k).s(&m).s(&member(rng)).done(),
    }
}

fn gen_zset(rng: &mut Rng) -> Vec<Vec<u8>> {
    let k = key(rng, 4);
    let k2 = key(rng, 4);
    let m = member(rng);
    match rng.below(19) {
        0 => {
            let mut c = Cmd::new("ZADD").s(&k);
            match rng.below(4) {
                0 => c = c.s("NX"),
                1 => c = c.s("XX"),
                _ => {}
            }
            match rng.below(4) {
                0 => c = c.s("GT"),
                1 => c = c.s("LT"),
                _ => {}
            }
            if rng.chance(30) {
                c = c.s("CH");
            }
            c.s(&score(rng)).s(&m).s(&score(rng)).s(&member(rng)).done()
        }
        1 => Cmd::new("ZADD").s(&k).s("INCR").s(&score(rng)).s(&m).done(),
        2 => Cmd::new("ZSCORE").s(&k).s(&m).done(),
        3 => Cmd::new("ZMSCORE").s(&k).s(&m).s(&member(rng)).done(),
        4 => Cmd::new("ZCARD").s(&k).done(),
        5 => Cmd::new("ZREM").s(&k).s(&m).s(&member(rng)).done(),
        6 => Cmd::new("ZINCRBY").s(&k).s(&score(rng)).s(&m).done(),
        7 => Cmd::new("ZRANK").s(&k).s(&m).done(),
        8 => Cmd::new("ZREVRANK").s(&k).s(&m).done(),
        9 => {
            let c = Cmd::new("ZRANGE").s(&k).n(idx(rng)).n(idx(rng));
            if rng.chance(50) { c.s("WITHSCORES") } else { c }.done()
        }
        10 => {
            let c = Cmd::new("ZREVRANGE").s(&k).n(idx(rng)).n(idx(rng));
            if rng.chance(50) { c.s("WITHSCORES") } else { c }.done()
        }
        11 | 12 => {
            let rev = rng.chance(50);
            let (lo, hi) = (bound(rng), bound(rng));
            let mut c = if rev {
                Cmd::new("ZREVRANGEBYSCORE").s(&k).s(&hi).s(&lo)
            } else {
                Cmd::new("ZRANGEBYSCORE").s(&k).s(&lo).s(&hi)
            };
            if rng.chance(50) {
                c = c.s("WITHSCORES");
            }
            if rng.chance(40) {
                c = c.s("LIMIT").n(rng.int(0, 3)).n(rng.int(-1, 4));
            }
            c.done()
        }
        13 => Cmd::new("ZCOUNT")
            .s(&k)
            .s(&bound(rng))
            .s(&bound(rng))
            .done(),
        14 => {
            let c = Cmd::new(if rng.chance(50) { "ZPOPMIN" } else { "ZPOPMAX" }).s(&k);
            if rng.chance(40) {
                c.n(rng.int(1, 3))
            } else {
                c
            }
            .done()
        }
        15 => Cmd::new("ZREMRANGEBYRANK")
            .s(&k)
            .n(idx(rng))
            .n(idx(rng))
            .done(),
        16 => Cmd::new("ZREMRANGEBYSCORE")
            .s(&k)
            .s(&bound(rng))
            .s(&bound(rng))
            .done(),
        17 => {
            let mut c = Cmd::new(if rng.chance(50) {
                "ZUNIONSTORE"
            } else {
                "ZINTERSTORE"
            })
            .s(&key(rng, 4))
            .n(2)
            .s(&k)
            .s(&k2);
            if rng.chance(40) {
                c = c.s("WEIGHTS").n(rng.int(1, 3)).n(rng.int(1, 3));
            }
            if rng.chance(40) {
                c = c.s("AGGREGATE").s(rng.pick(&["SUM", "MIN", "MAX"]));
            }
            c.done()
        }
        _ => {
            let mut c =
                Cmd::new("ZMPOP")
                    .n(2)
                    .s(&k)
                    .s(&k2)
                    .s(if rng.chance(50) { "MIN" } else { "MAX" });
            if rng.chance(50) {
                c = c.s("COUNT").n(rng.int(1, 3));
            }
            c.done()
        }
    }
}

fn gen_bitmap(rng: &mut Rng) -> Vec<Vec<u8>> {
    let k = key(rng, 5);
    match rng.below(8) {
        0 | 1 => Cmd::new("SETBIT")
            .s(&k)
            .n(rng.int(0, 300))
            .n(rng.int(0, 1))
            .done(),
        2 => Cmd::new("GETBIT").s(&k).n(rng.int(0, 300)).done(),
        3 => Cmd::new("BITCOUNT").s(&k).done(),
        4 => {
            let c = Cmd::new("BITCOUNT").s(&k).n(idx(rng)).n(idx(rng));
            if rng.chance(50) {
                c.s(if rng.chance(50) { "BYTE" } else { "BIT" })
            } else {
                c
            }
            .done()
        }
        5 => {
            let mut c = Cmd::new("BITPOS").s(&k).n(rng.int(0, 1));
            if rng.chance(60) {
                c = c.n(idx(rng));
                if rng.chance(60) {
                    c = c.n(idx(rng));
                    if rng.chance(50) {
                        c = c.s(if rng.chance(50) { "BYTE" } else { "BIT" });
                    }
                }
            }
            c.done()
        }
        6 => Cmd::new("BITOP")
            .s(rng.pick(&["AND", "OR", "XOR"]))
            .s(&key(rng, 5))
            .s(&key(rng, 5))
            .s(&key(rng, 5))
            .done(),
        _ => Cmd::new("BITOP")
            .s("NOT")
            .s(&key(rng, 5))
            .s(&key(rng, 5))
            .done(),
    }
}

// === full-iteration checks and the end-of-sequence state diff ===============

/// A canonical rendering of a number, so `3` and `3.0` collapse before the
/// values are *sorted* (the multiset comparisons cannot use `Norm::Floats`,
/// which only reaches leaves after the ordering is already fixed).
fn canon_num(s: &str) -> String {
    match s.trim().parse::<f64>() {
        Ok(f) => format!("{f:e}"),
        Err(_) => s.to_string(),
    }
}

fn as_text(r: &Reply) -> String {
    match r {
        Reply::Bulk(b) => show_bytes(b),
        Reply::Simple(s) => s.clone(),
        Reply::Int(i) => i.to_string(),
        Reply::Nil => "(nil)".into(),
        other => render(other),
    }
}

type Run<'a> = &'a mut dyn FnMut(&[Vec<u8>]) -> Reply;

/// Drive a `SCAN`-family cursor to completion and return the sorted, deduped
/// set of what came back.
///
/// This is the honest way to diff the scan family: the cursor is an opaque,
/// engine-private encoding (Locus's and Redis's have nothing in common), and
/// the *only* thing the protocol promises is that a full iteration returns
/// every element that was present throughout it. Nothing mutates during the
/// loop here, so "at least once" collapses to exact set equality.
fn scan_all(run: Run, cmd: &str, key: Option<&str>, pairs: bool, numeric: bool) -> Vec<String> {
    let mut cursor = "0".to_string();
    let mut out: Vec<String> = Vec::new();
    for _ in 0..10_000 {
        let mut c = Cmd::new(cmd);
        if let Some(k) = key {
            c = c.s(k);
        }
        c = c.s(&cursor).s("COUNT").n(7);
        let reply = run(&c.done());
        let items = match &reply {
            Reply::Error(e) => {
                return vec![format!(
                    "<error {}>",
                    e.split_whitespace().next().unwrap_or("")
                )];
            }
            Reply::Array(v) if v.len() == 2 => v,
            other => return vec![format!("<malformed scan reply {}>", render(other))],
        };
        cursor = as_text(&items[0]);
        if let Reply::Array(els) = &items[1] {
            if pairs {
                for ch in els.chunks(2) {
                    let a = as_text(&ch[0]);
                    let b = ch.get(1).map(as_text).unwrap_or_default();
                    let b = if numeric { canon_num(&b) } else { b };
                    out.push(format!("{a}\u{0}{b}"));
                }
            } else {
                out.extend(els.iter().map(as_text));
            }
        }
        if cursor == "0" {
            out.sort();
            out.dedup();
            return out;
        }
    }
    vec!["<scan never terminated>".into()]
}

/// The whole keyspace, canonically. Compared at the end of every sequence: a
/// run where every *reply* matched but the surviving state did not is still a
/// divergence, and only this catches it.
fn state_dump(run: Run) -> Vec<String> {
    let mut keys: Vec<String> = match run(&Cmd::new("KEYS").s("*").done()) {
        Reply::Array(v) => v.iter().map(as_text).collect(),
        other => return vec![format!("<KEYS returned {}>", render(&other))],
    };
    keys.sort();
    let mut out = Vec::with_capacity(keys.len() + 1);
    out.push(format!(
        "dbsize={}",
        as_text(&run(&Cmd::new("DBSIZE").done()))
    ));
    for k in keys {
        let ty = as_text(&run(&Cmd::new("TYPE").s(&k).done()));
        // The exact TTL is wall-clock and already diffed (with slack) by the
        // TTL/PTTL commands; here only the *class* is compared, which is stable.
        let ttl = match run(&Cmd::new("TTL").s(&k).done()) {
            Reply::Int(-1) => "persistent",
            Reply::Int(-2) => "gone",
            _ => "volatile",
        };
        let body = match ty.as_str() {
            "string" => as_text(&run(&Cmd::new("GET").s(&k).done())),
            "list" => match run(&Cmd::new("LRANGE").s(&k).n(0).n(-1).done()) {
                Reply::Array(v) => v.iter().map(as_text).collect::<Vec<_>>().join("|"),
                o => render(&o),
            },
            "hash" => match run(&Cmd::new("HGETALL").s(&k).done()) {
                Reply::Array(v) => {
                    let mut p: Vec<String> = v
                        .chunks(2)
                        .map(|c| {
                            format!(
                                "{}={}",
                                as_text(&c[0]),
                                c.get(1).map(as_text).unwrap_or_default()
                            )
                        })
                        .collect();
                    p.sort();
                    p.join("|")
                }
                o => render(&o),
            },
            "set" => match run(&Cmd::new("SMEMBERS").s(&k).done()) {
                Reply::Array(v) => {
                    let mut m: Vec<String> = v.iter().map(as_text).collect();
                    m.sort();
                    m.join("|")
                }
                o => render(&o),
            },
            "zset" => match run(&Cmd::new("ZRANGE").s(&k).n(0).n(-1).s("WITHSCORES").done()) {
                Reply::Array(v) => v
                    .chunks(2)
                    .map(|c| {
                        format!(
                            "{}={}",
                            as_text(&c[0]),
                            canon_num(&c.get(1).map(as_text).unwrap_or_default())
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("|"),
                o => render(&o),
            },
            other => format!("<untyped {other}>"),
        };
        out.push(format!("{k} {ty} {ttl} {body}"));
    }
    out
}

fn locus_runner(db: &mut Db) -> impl FnMut(&[Vec<u8>]) -> Reply + '_ {
    move |args: &[Vec<u8>]| {
        let bytes = execute(args, db);
        let mut cur: &[u8] = &bytes;
        read_reply(&mut cur)
    }
}

// === the driver =============================================================

struct Outcome {
    commands: u64,
    notes: Notes,
    /// Set when the sequence ended early on a divergence the plan already owns
    /// (see `known_open_divergences`) rather than running to completion.
    known_open: u64,
}

/// Is this divergence the P3-batch's open `NaN`-in-sorted-sets item?
///
/// Recognized, counted and reported — never silently skipped, and never masked
/// into "equal" either: the sequence *stops* here, because the two keyspaces
/// have genuinely diverged and everything after would be noise. When the
/// P3-batch lands this stops matching and the sequences simply run longer.
fn is_known_open(args: &[Vec<u8>], mine: &Reply, theirs: &Reply) -> bool {
    let cmd = String::from_utf8_lossy(&args[0]).to_ascii_uppercase();
    if cmd != "ZADD" && cmd != "ZINCRBY" {
        return false;
    }
    let Reply::Error(e) = theirs else {
        return false;
    };
    if !e.contains("NaN") && !e.contains("not a valid float") {
        return false;
    }
    match mine {
        Reply::Bulk(b) => as_f64(b).is_some_and(|f| f.is_nan()),
        Reply::Int(_) => true, // ZADD accepted the score Redis refused
        _ => false,
    }
}

/// One randomized sequence. Stops at the first divergence — once the two
/// keyspaces differ, every later reply is noise.
fn run_sequence(seed: u64, len: usize, redis: &mut Redis) -> Result<Outcome, Box<Divergence>> {
    let mut db = Db::new();
    assert!(matches!(
        redis.run(&Cmd::new("FLUSHALL").done()),
        Reply::Simple(_)
    ));
    let mut rng = Rng::new(seed);
    let now_s = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let mut seq: Vec<Vec<Vec<u8>>> = Vec::with_capacity(len);
    let mut notes = Notes::default();
    let mut commands = 0u64;

    let fail = |seed: u64,
                seq: &Vec<Vec<Vec<u8>>>,
                cmd: Vec<Vec<u8>>,
                locus: String,
                redis: String,
                what: String| {
        Box::new(Divergence {
            seed,
            step: seq.len(),
            seq: seq.clone(),
            cmd,
            locus,
            redis,
            what,
        })
    };

    for step in 0..len {
        let args = gen_command(&mut rng, now_s);
        let (mine, theirs) = both(&mut db, redis, &args);
        commands += 1;
        let norm = norm_for(&args);
        if !same(norm, &mine, &theirs, &mut notes) {
            if is_known_open(&args, &mine, &theirs) {
                return Ok(Outcome {
                    commands,
                    notes,
                    known_open: 1,
                });
            }
            let cmd = args.clone();
            seq.push(args);
            return Err(fail(
                seed,
                &seq,
                cmd,
                render(&mine),
                render(&theirs),
                format!("reply mismatch (normalization: {norm:?})"),
            ));
        }
        seq.push(args);

        // A full scan sweep partway through and again at the end: cheap, and it
        // exercises the cursor over a keyspace that is actually populated.
        if step % 64 == 63 || step + 1 == len {
            for (cmd, k, pairs, numeric) in [
                ("SCAN", None, false, false),
                ("HSCAN", Some("d2"), true, false),
                ("SSCAN", Some("d3"), false, false),
                ("ZSCAN", Some("d4"), true, true),
            ] {
                let a = scan_all(&mut locus_runner(&mut db), cmd, k, pairs, numeric);
                let b = scan_all(&mut |x: &[Vec<u8>]| redis.run(x), cmd, k, pairs, numeric);
                commands += 2;
                if a != b {
                    let probe = Cmd::new(cmd)
                        .s(k.unwrap_or(""))
                        .s("<full iteration>")
                        .done();
                    return Err(fail(
                        seed,
                        &seq,
                        probe,
                        format!("{a:?}"),
                        format!("{b:?}"),
                        format!("full {cmd} iteration returned a different set"),
                    ));
                }
            }
        }
    }

    let mine = state_dump(&mut locus_runner(&mut db));
    let theirs = state_dump(&mut |x: &[Vec<u8>]| redis.run(x));
    if mine != theirs {
        let first = mine
            .iter()
            .zip(theirs.iter())
            .find(|(a, b)| a != b)
            .map(|(a, b)| (a.clone(), b.clone()))
            .unwrap_or_else(|| (format!("{mine:?}"), format!("{theirs:?}")));
        return Err(fail(
            seed,
            &seq,
            Cmd::new("<end-of-sequence keyspace dump>").done(),
            first.0,
            first.1,
            "the replies all matched but the surviving keyspace did not".into(),
        ));
    }

    Ok(Outcome {
        commands,
        notes,
        known_open: 0,
    })
}

/// `sequences` runs from `first_seed`; returns (commands, notes) or panics with
/// the divergence report.
///
/// By default the first divergence stops the run — that is what you want in CI.
/// `LOCUS_DIFF_ALL=1` keeps going instead and reports one example of each
/// *distinct* divergence at the end, which is what you want when triaging a
/// batch of them: without it you fix one, re-run, and find the next.
fn drive(redis: &mut Redis, first_seed: u64, sequences: u64, len: usize) -> (u64, Notes, u64) {
    let collect = std::env::var("LOCUS_DIFF_ALL").is_ok();
    let mut total = 0u64;
    let mut known_open = 0u64;
    let mut notes = Notes::default();
    let mut seen: Vec<(String, String)> = Vec::new();
    for i in 0..sequences {
        match run_sequence(first_seed.wrapping_add(i), len, redis) {
            Ok(o) => {
                total += o.commands;
                known_open += o.known_open;
                notes.merge(&o.notes);
            }
            Err(d) => {
                let report = format!(
                    "{}\nreproduce with: LOCUS_DIFF_SEED={} LOCUS_DIFF_SEQS=1 LOCUS_DIFF_LEN={} \
                     cargo test --test differential -- --ignored --nocapture differential_randomized",
                    d.report(),
                    d.seed,
                    len
                );
                if !collect {
                    panic!("{report}");
                }
                // Signature: the command name plus both rendered replies, so
                // twenty hits of one bug collapse to one line.
                let sig = format!(
                    "{} | {} | {}",
                    show_bytes(&d.cmd[0]),
                    d.locus.chars().take(40).collect::<String>(),
                    d.redis.chars().take(40).collect::<String>()
                );
                if !seen.iter().any(|(s, _)| *s == sig) {
                    seen.push((sig, report));
                }
            }
        }
    }
    if !seen.is_empty() {
        panic!(
            "{} distinct divergences over {sequences} sequences:\n{}",
            seen.len(),
            seen.iter()
                .map(|(_, r)| r.as_str())
                .collect::<Vec<_>>()
                .join("\n")
        );
    }
    (total, notes, known_open)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

// === the tests ==============================================================

/// The subset that runs in the default `cargo test`: enough to catch a
/// regression in a changed command, small enough not to slow the commit loop.
#[test]
fn differential_smoke() {
    let _serial = one_reference_server_at_a_time();
    let Some(mut redis) = Redis::start() else {
        skip("no redis-server on PATH — the command differential needs a reference engine");
        return;
    };
    let (commands, notes, known_open) = drive(&mut redis, 1, 6, 60);
    println!(
        "differential smoke: 6 sequences x 60 commands = {commands} executed against {} \
         (normalizations fired: {}, known-open stops: {known_open})",
        redis.version,
        notes.total()
    );
}

/// The long randomized run. Opt in with `--ignored`.
#[test]
#[ignore]
fn differential_randomized() {
    let _serial = one_reference_server_at_a_time();
    let Some(mut redis) = Redis::start() else {
        skip("no redis-server on PATH — the command differential needs a reference engine");
        return;
    };
    let seqs = env_usize("LOCUS_DIFF_SEQS", 400) as u64;
    let len = env_usize("LOCUS_DIFF_LEN", 200);
    let seed = env_usize("LOCUS_DIFF_SEED", 1) as u64;
    let t0 = Instant::now();
    let (commands, notes, known_open) = drive(&mut redis, seed, seqs, len);
    println!(
        "\ndifferential: {seqs} sequences x {len} commands, {commands} executed in {:.1}s \
         against {}\n  seeds {}..{}\n  normalizations that fired: floats {}, error text {}, \
         reordered multiset {}, ttl slack {}\n  ... out of {} float-shaped and {} error \
         comparisons reached\n  sequences stopped early on a known-open plan item (NaN in \
         sorted sets): {known_open}",
        t0.elapsed().as_secs_f64(),
        redis.version,
        seed,
        seed + seqs - 1,
        notes.float_format,
        notes.error_text,
        notes.reorder,
        notes.ttl_slack,
        notes.float_seen,
        notes.error_seen,
    );
}

/// Hand-picked awkward-but-valid commands, each in its own flushed keyspace so
/// one divergence cannot hide the next. The randomized generator deliberately
/// stays inside ordinary grammar; these are the corners it would take a very
/// long time to stumble into.
#[test]
fn edge_cases() {
    let _serial = one_reference_server_at_a_time();
    let Some(mut redis) = Redis::start() else {
        skip("no redis-server on PATH — the command differential needs a reference engine");
        return;
    };
    let cases: &[(&str, &[&[&str]])] = &[
        (
            "empty value round trip",
            &[
                &["SET", "k", ""],
                &["GET", "k"],
                &["STRLEN", "k"],
                &["APPEND", "k", "x"],
            ],
        ),
        (
            "incr overflow",
            &[
                &["SET", "k", "9223372036854775807"],
                &["INCR", "k"],
                &["GET", "k"],
            ],
        ),
        (
            "incr on non-numeric",
            &[
                &["SET", "k", "abc"],
                &["INCR", "k"],
                &["INCRBYFLOAT", "k", "1.5"],
            ],
        ),
        (
            "incrbyfloat formatting",
            &[
                &["SET", "k", "10.5"],
                &["INCRBYFLOAT", "k", "0.1"],
                &["INCRBYFLOAT", "k", "-5"],
                &["GET", "k"],
            ],
        ),
        (
            "incrbyfloat is rendered the human way, not the score way",
            &[
                &["SET", "k", "-2.251"],
                &["INCRBYFLOAT", "k", "-5.25"],
                &["GET", "k"],
                &["SET", "j", "0.1"],
                &["INCRBYFLOAT", "j", "0.2"],
                &["SET", "b", "1e20"],
                &["INCRBYFLOAT", "b", "1"],
                &["SET", "s", "0.000001"],
                &["INCRBYFLOAT", "s", "0.0000001"],
                &["SET", "n", "12345678901234567"],
                &["INCRBYFLOAT", "n", "1"],
                &["SET", "z", "5.0e3"],
                &["INCRBYFLOAT", "z", "2.0e2"],
                &["INCRBYFLOAT", "fresh", "-9.25"],
                &["INCRBYFLOAT", "fresh", "0.25"],
            ],
        ),
        (
            "zscore keeps the shortest round-trip rendering",
            &[
                &["ZADD", "z", "0.30000000000000004", "m"],
                &["ZSCORE", "z", "m"],
                &["ZADD", "z", "3", "n"],
                &["ZSCORE", "z", "n"],
                &["ZINCRBY", "z", "0.1", "n"],
                &["ZRANGE", "z", "0", "-1", "WITHSCORES"],
            ],
        ),
        (
            "setrange past the end",
            &[
                &["SETRANGE", "k", "5", "hi"],
                &["STRLEN", "k"],
                &["GET", "k"],
                &["GETRANGE", "k", "0", "-1"],
            ],
        ),
        (
            "getrange out of range",
            &[
                &["SET", "k", "hello"],
                &["GETRANGE", "k", "10", "20"],
                &["GETRANGE", "k", "-100", "-90"],
                &["GETRANGE", "k", "-3", "-1"],
                &["GETRANGE", "k", "3", "1"],
            ],
        ),
        (
            "wrongtype across families",
            &[
                &["LPUSH", "k", "a"],
                &["GET", "k"],
                &["INCR", "k"],
                &["SADD", "k", "m"],
                &["HSET", "k", "f", "v"],
                &["ZADD", "k", "1", "m"],
                &["SETBIT", "k", "3", "1"],
                &["GETRANGE", "k", "0", "-1"],
            ],
        ),
        (
            "expire on a missing key",
            &[
                &["EXPIRE", "nope", "100"],
                &["TTL", "nope"],
                &["PERSIST", "nope"],
                &["PTTL", "nope"],
            ],
        ),
        (
            "expireat in the past deletes",
            &[
                &["SET", "k", "v"],
                &["EXPIREAT", "k", "1"],
                &["EXISTS", "k"],
                &["DBSIZE"],
                &["KEYS", "*"],
                &["TYPE", "k"],
            ],
        ),
        (
            "set keepttl keeps it",
            &[
                &["SET", "k", "v", "EX", "10000"],
                &["SET", "k", "w", "KEEPTTL"],
                &["TTL", "k"],
                &["SET", "k", "x"],
                &["TTL", "k"],
            ],
        ),
        (
            "set get on wrong type",
            &[&["LPUSH", "k", "a"], &["SET", "k", "v", "GET"]],
        ),
        ("set nx and xx together", &[&["SET", "k", "v", "NX", "XX"]]),
        (
            "negative and zero expire",
            &[
                &["SET", "k", "v"],
                &["EXPIRE", "k", "-1"],
                &["EXISTS", "k"],
                &["SET", "j", "v"],
                &["EXPIRE", "j", "0"],
                &["EXISTS", "j"],
            ],
        ),
        (
            "copy onto itself",
            &[
                &["SET", "k", "v"],
                &["COPY", "k", "k"],
                &["COPY", "k", "k", "REPLACE"],
            ],
        ),
        (
            "rename to itself",
            &[
                &["SET", "k", "v"],
                &["RENAME", "k", "k"],
                &["GET", "k"],
                &["RENAMENX", "k", "k"],
            ],
        ),
        (
            "lpos rank zero is an error",
            &[
                &["RPUSH", "k", "a", "b", "a"],
                &["LPOS", "k", "a", "RANK", "0"],
                &["LPOS", "k", "a", "RANK", "-1"],
                &["LPOS", "k", "a", "COUNT", "0"],
            ],
        ),
        (
            "ltrim empties and deletes",
            &[
                &["RPUSH", "k", "a", "b"],
                &["LTRIM", "k", "5", "10"],
                &["EXISTS", "k"],
                &["TYPE", "k"],
            ],
        ),
        (
            "lset out of range",
            &[
                &["RPUSH", "k", "a"],
                &["LSET", "k", "9", "b"],
                &["LSET", "nope", "0", "b"],
            ],
        ),
        (
            "linsert missing pivot",
            &[
                &["RPUSH", "k", "a"],
                &["LINSERT", "k", "BEFORE", "zz", "b"],
                &["LINSERT", "nope", "BEFORE", "a", "b"],
            ],
        ),
        (
            "lpop count zero and negative",
            &[
                &["RPUSH", "k", "a", "b"],
                &["LPOP", "k", "0"],
                &["LPOP", "k", "99"],
                &["EXISTS", "k"],
            ],
        ),
        (
            "hincrby overflow and non-numeric",
            &[
                &["HSET", "k", "f", "9223372036854775807"],
                &["HINCRBY", "k", "f", "1"],
                &["HSET", "k", "g", "abc"],
                &["HINCRBY", "k", "g", "1"],
            ],
        ),
        (
            "hdel empties the hash",
            &[
                &["HSET", "k", "f", "v"],
                &["HDEL", "k", "f"],
                &["EXISTS", "k"],
                &["HGETALL", "k"],
                &["HLEN", "k"],
            ],
        ),
        (
            "srem empties the set",
            &[
                &["SADD", "k", "m"],
                &["SREM", "k", "m"],
                &["EXISTS", "k"],
                &["SCARD", "k"],
            ],
        ),
        (
            "smove to itself and to a missing set",
            &[
                &["SADD", "a", "m"],
                &["SMOVE", "a", "a", "m"],
                &["SMEMBERS", "a"],
                &["SMOVE", "a", "b", "m"],
                &["SMEMBERS", "b"],
                &["EXISTS", "a"],
            ],
        ),
        (
            "sintercard limit zero means unlimited",
            &[
                &["SADD", "a", "x", "y", "z"],
                &["SADD", "b", "x", "y"],
                &["SINTERCARD", "2", "a", "b", "LIMIT", "0"],
                &["SINTERCARD", "2", "a", "b", "LIMIT", "1"],
            ],
        ),
        (
            "set ops with a missing key",
            &[
                &["SADD", "a", "x"],
                &["SINTER", "a", "nope"],
                &["SUNION", "a", "nope"],
                &["SDIFF", "nope", "a"],
                &["SINTERSTORE", "d", "a", "nope"],
                &["EXISTS", "d"],
            ],
        ),
        (
            "zadd gt lt nx interactions",
            &[
                &["ZADD", "k", "5", "m"],
                &["ZADD", "k", "GT", "CH", "3", "m"],
                &["ZADD", "k", "GT", "CH", "9", "m"],
                &["ZADD", "k", "LT", "CH", "1", "m"],
                &["ZSCORE", "k", "m"],
                &["ZADD", "k", "NX", "CH", "100", "m"],
                &["ZSCORE", "k", "m"],
            ],
        ),
        (
            "zadd incr with nx on an existing member",
            &[
                &["ZADD", "k", "1", "m"],
                &["ZADD", "k", "NX", "INCR", "5", "m"],
                &["ZADD", "k", "XX", "INCR", "5", "nope"],
                &["ZSCORE", "k", "m"],
            ],
        ),
        (
            "zset score tie breaks lexically",
            &[
                &["ZADD", "k", "1", "b", "1", "a", "1", "c"],
                &["ZRANGE", "k", "0", "-1"],
                &["ZPOPMIN", "k"],
                &["ZPOPMAX", "k"],
                &["ZRANGE", "k", "0", "-1", "WITHSCORES"],
            ],
        ),
        (
            "zrangebyscore exclusive and infinite",
            &[
                &["ZADD", "k", "1", "a", "2", "b", "3", "c"],
                &["ZRANGEBYSCORE", "k", "(1", "3"],
                &["ZRANGEBYSCORE", "k", "-inf", "+inf"],
                &["ZRANGEBYSCORE", "k", "(1", "(3"],
                &["ZRANGEBYSCORE", "k", "3", "1"],
                &["ZCOUNT", "k", "(1", "+inf"],
            ],
        ),
        (
            "zrangebyscore limit negative count",
            &[
                &["ZADD", "k", "1", "a", "2", "b", "3", "c"],
                &["ZRANGEBYSCORE", "k", "-inf", "+inf", "LIMIT", "1", "-1"],
                &["ZRANGEBYSCORE", "k", "-inf", "+inf", "LIMIT", "5", "2"],
            ],
        ),
        (
            "zstore over a plain set",
            &[
                &["SADD", "s", "x", "y"],
                &["ZADD", "z", "3", "x"],
                &["ZUNIONSTORE", "d", "2", "s", "z"],
                &["ZRANGE", "d", "0", "-1", "WITHSCORES"],
                &["ZINTERSTORE", "e", "2", "s", "z", "AGGREGATE", "MAX"],
                &["ZRANGE", "e", "0", "-1", "WITHSCORES"],
            ],
        ),
        (
            "zstore weights and empty result",
            &[
                &["ZADD", "a", "1", "x"],
                &["ZUNIONSTORE", "d", "2", "a", "nope", "WEIGHTS", "2", "3"],
                &["ZRANGE", "d", "0", "-1", "WITHSCORES"],
                &["ZINTERSTORE", "e", "2", "a", "nope"],
                &["EXISTS", "e"],
            ],
        ),
        (
            "zremrangebyrank whole set",
            &[
                &["ZADD", "k", "1", "a", "2", "b"],
                &["ZREMRANGEBYRANK", "k", "0", "-1"],
                &["EXISTS", "k"],
                &["ZCARD", "k"],
            ],
        ),
        (
            "setbit far out then bitcount",
            &[
                &["SETBIT", "k", "100", "1"],
                &["STRLEN", "k"],
                &["BITCOUNT", "k"],
                &["BITCOUNT", "k", "0", "-1", "BIT"],
                &["BITPOS", "k", "1"],
                &["BITPOS", "k", "0"],
                &["GETBIT", "k", "1000"],
            ],
        ),
        (
            "bitpos on an all-ones string",
            &[
                &["SET", "k", "\u{ff}\u{ff}"],
                &["BITPOS", "k", "0"],
                &["BITPOS", "k", "0", "0"],
                &["BITPOS", "k", "0", "0", "-1"],
                &["BITPOS", "k", "1", "2"],
            ],
        ),
        (
            "bitop not and mismatched lengths",
            &[
                &["SET", "a", "abc"],
                &["SET", "b", "z"],
                &["BITOP", "XOR", "d", "a", "b"],
                &["STRLEN", "d"],
                &["BITOP", "NOT", "n", "a"],
                &["STRLEN", "n"],
                &["BITOP", "AND", "e", "a", "nope"],
                &["EXISTS", "e"],
            ],
        ),
        (
            "bitcount empty range",
            &[
                &["SET", "k", "foobar"],
                &["BITCOUNT", "k", "1", "1"],
                &["BITCOUNT", "k", "0", "0"],
                &["BITCOUNT", "k", "5", "30", "BIT"],
                &["BITCOUNT", "k", "-5", "-1"],
            ],
        ),
        (
            "mset then mget missing",
            &[
                &["MSET", "a", "1", "b", "2"],
                &["MGET", "a", "nope", "b"],
                &["MSETNX", "b", "9", "c", "9"],
                &["GET", "b"],
                &["EXISTS", "c"],
            ],
        ),
        (
            "type after every family",
            &[
                &["SET", "s", "v"],
                &["RPUSH", "l", "v"],
                &["HSET", "h", "f", "v"],
                &["SADD", "t", "m"],
                &["ZADD", "z", "1", "m"],
                &["TYPE", "s"],
                &["TYPE", "l"],
                &["TYPE", "h"],
                &["TYPE", "t"],
                &["TYPE", "z"],
                &["TYPE", "nope"],
            ],
        ),
        (
            "getex removes and keeps the ttl",
            &[
                &["SET", "k", "v", "EX", "10000"],
                &["GETEX", "k"],
                &["TTL", "k"],
                &["GETEX", "k", "PERSIST"],
                &["TTL", "k"],
                &["GETEX", "nope", "PERSIST"],
            ],
        ),
        (
            "lmpop and zmpop on empties",
            &[
                &["LMPOP", "2", "nope", "alsonope", "LEFT"],
                &["ZMPOP", "2", "nope", "alsonope", "MIN"],
                &["RPUSH", "l", "a", "b"],
                &["LMPOP", "2", "nope", "l", "RIGHT", "COUNT", "5"],
            ],
        ),
        (
            "negative bit and byte ranges",
            &[
                &["SET", "k", "a"],
                &["BITCOUNT", "k", "-5", "-2"],
                &["BITPOS", "k", "1", "-1", "-3"],
                &["BITCOUNT", "k", "-1", "-3"],
                &["BITCOUNT", "k", "-1", "-3", "BIT"],
                &["BITPOS", "k", "1", "-1", "-3", "BIT"],
                &["GETRANGE", "k", "-100", "-90"],
                &["GETRANGE", "k", "-1", "-3"],
            ],
        ),
        (
            "a missing move source answers before the destination type",
            &[
                &["SET", "str", "v"],
                &["RPOPLPUSH", "nolist", "str"],
                &["LMOVE", "nolist", "str", "LEFT", "LEFT"],
                &["SMOVE", "noset", "str", "m"],
                &["RPUSH", "list", "a"],
                &["RPOPLPUSH", "list", "str"],
                &["LRANGE", "list", "0", "-1"],
            ],
        ),
        (
            "a self move keeps the ttl",
            &[
                &["RPUSH", "l", "one"],
                &["EXPIRE", "l", "1000"],
                &["RPOPLPUSH", "l", "l"],
                &["TTL", "l"],
                &["LMOVE", "l", "l", "LEFT", "RIGHT"],
                &["TTL", "l"],
                &["SADD", "s", "m"],
                &["EXPIRE", "s", "1000"],
                &["SMOVE", "s", "s", "m"],
                &["TTL", "s"],
                &["SCARD", "s"],
                &["RPOPLPUSH", "l", "dst"],
                &["EXISTS", "l"],
                &["TTL", "dst"],
            ],
        ),
        (
            "integers have exactly one spelling",
            &[
                &["SET", "k", "0"],
                &["APPEND", "k", "2"],
                &["DECR", "k"],
                &["GET", "k"],
                &["SET", "p", "+1"],
                &["INCR", "p"],
                &["SET", "z", "-0"],
                &["INCR", "z"],
                &["HSET", "h", "f", "02"],
                &["HINCRBY", "h", "f", "1"],
                &["RPUSH", "l", "a"],
                &["LRANGE", "l", "+0", "-1"],
                &["GETRANGE", "k", "00", "1"],
                &["EXPIRE", "k", "+100"],
            ],
        ),
        (
            "glob character classes",
            &[
                &["MSET", "a1", "1", "a2", "2", "b1", "3", "a-1", "4"],
                &["KEYS", "a[12]"],
                &["KEYS", "a[^1]"],
                &["KEYS", "a[0-9]"],
                &["KEYS", "[ab]1"],
                &["KEYS", "a[1-]"],
                &["KEYS", r"a\-1"],
                &["KEYS", "*[0-9]"],
                &["KEYS", "?1"],
                &["KEYS", "*"],
            ],
        ),
    ];

    let mut failures = Vec::new();
    for (name, script) in cases {
        let mut db = Db::new();
        assert!(matches!(
            redis.run(&Cmd::new("FLUSHALL").done()),
            Reply::Simple(_)
        ));
        let mut notes = Notes::default();
        for step in script.iter() {
            let args: Vec<Vec<u8>> = step.iter().map(|s| s.as_bytes().to_vec()).collect();
            let (mine, theirs) = both(&mut db, &mut redis, &args);
            if !same(norm_for(&args), &mine, &theirs, &mut notes) {
                failures.push(format!(
                    "  [{name}] {}\n      locus: {}\n      redis: {}",
                    show_cmd(&args),
                    render(&mine),
                    render(&theirs)
                ));
                break; // state has diverged; the rest of this case is noise
            }
        }
        // The surviving keyspace has to agree too.
        let a = state_dump(&mut locus_runner(&mut db));
        let b = state_dump(&mut |x: &[Vec<u8>]| redis.run(x));
        if a != b {
            failures.push(format!(
                "  [{name}] end state differs\n      locus: {a:?}\n      redis: {b:?}"
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {} edge cases diverged:\n{}",
        failures.len(),
        cases.len(),
        failures.join("\n")
    );
    println!("edge cases: {} scripts, no divergences", cases.len());
}

/// `INCRBYFLOAT` is the one command whose exact bytes we deliberately do not
/// match, and this pins how far the agreement goes.
///
/// Redis accumulates it in C `long double` — 80-bit on x86, and *not* 80-bit on
/// arm64, so Redis does not even agree with itself across machines — and renders
/// the result with 17 decimals. Locus has `f64` and pure `std` (identity rule
/// 1), so past f64's 15 guaranteed significant digits the two must part; and
/// because the result is also the stored value, one different digit changes
/// every later `GET`, `STRLEN` and `APPEND` on that key. That is why the
/// randomized generator does not emit it.
///
/// What is asserted here is what *does* hold and is worth having: both engines
/// agree on the **number**, and Locus's rendering is the clean one — never
/// longer than 17 significant digits, no exponent, no trailing zeros.
#[test]
fn incrbyfloat_precision_is_f64_not_long_double() {
    let _serial = one_reference_server_at_a_time();
    let Some(mut redis) = Redis::start() else {
        skip("no redis-server on PATH — the command differential needs a reference engine");
        return;
    };
    // Values chosen so the *last* one is past f64: "12342.7510" is not exactly
    // representable, and x87 carries the residue into the 17th decimal.
    let cases: &[(&str, &str)] = &[
        ("12345", "-2.25"),
        ("12342.7510", "-9.25"),
        ("-2.251", "-5.25"),
        ("0.1", "0.2"),
        ("1e20", "1"),
        ("0.000001", "0.0000001"),
    ];
    let mut exact = 0;
    for (start, incr) in cases {
        let mut db = Db::new();
        assert!(matches!(
            redis.run(&Cmd::new("FLUSHALL").done()),
            Reply::Simple(_)
        ));
        both(&mut db, &mut redis, &Cmd::new("SET").s("k").s(start).done());
        let (mine, theirs) = both(
            &mut db,
            &mut redis,
            &Cmd::new("INCRBYFLOAT").s("k").s(incr).done(),
        );
        let (Reply::Bulk(a), Reply::Bulk(b)) = (&mine, &theirs) else {
            panic!(
                "INCRBYFLOAT {start} {incr}: {} / {}",
                render(&mine),
                render(&theirs)
            );
        };
        let (fa, fb) = (
            as_f64(a).expect("locus reply parses as a float"),
            as_f64(b).expect("redis reply parses as a float"),
        );
        assert!(
            (fa - fb).abs() <= 1e-12 * fa.abs().max(1.0),
            "INCRBYFLOAT {start} {incr}: the two engines disagree on the NUMBER, not just its \
             rendering — locus {fa}, redis {fb}"
        );
        let text = show_bytes(a);
        assert!(
            !text.contains('e') && !text.contains('E'),
            "locus rendered {text} in exponent notation; Redis never does"
        );
        assert!(
            !text.contains('.') || !text.ends_with('0'),
            "locus left a trailing zero in {text}"
        );
        let digits = text.chars().filter(|c| c.is_ascii_digit()).count();
        assert!(
            digits <= 21,
            "locus rendered {digits} digits in {text} — more precision than f64 has"
        );
        if a == b {
            exact += 1;
        }
    }
    println!(
        "INCRBYFLOAT: {exact} of {} cases byte-identical to Redis; the rest agree on the value \
         and differ only past f64's precision (Redis uses x87 long double here)",
        cases.len()
    );
}

/// Divergences the execution plan already owns, pinned rather than fixed.
///
/// The harness re-found the P3-batch's `NaN`-in-sorted-sets item (both halves of
/// it, independently). Fixing it belongs to that batch, not to a test session —
/// so instead of failing, the *current* behaviour is asserted here. When the
/// P3-batch lands, this test goes red and says so, and whoever fixed it promotes
/// the case into `edge_cases`. A known bug that no test mentions is the one that
/// quietly stops being known.
#[test]
fn known_open_divergences() {
    let _serial = one_reference_server_at_a_time();
    let Some(mut redis) = Redis::start() else {
        skip("no redis-server on PATH — the command differential needs a reference engine");
        return;
    };
    let promoted = "this now matches Redis — the P3-batch `NaN` item has landed. \
                    Move the case into `edge_cases` and delete it from here.";

    // P3-batch, item 1: "`ZADD z nan m1` is accepted and stored".
    let mut db = Db::new();
    assert!(matches!(
        redis.run(&Cmd::new("FLUSHALL").done()),
        Reply::Simple(_)
    ));
    let (mine, theirs) = both(
        &mut db,
        &mut redis,
        &Cmd::new("ZADD").s("k").s("nan").s("m").done(),
    );
    assert_eq!(render(&mine), ":1", "ZADD k nan m: {promoted}");
    assert!(
        matches!(theirs, Reply::Error(_)),
        "redis stopped rejecting a NaN score: {}",
        render(&theirs)
    );
    assert_eq!(
        render(&execute_reply(
            &mut db,
            &Cmd::new("ZSCORE").s("k").s("m").done()
        )),
        "$NaN",
        "the stored score is no longer NaN: {promoted}"
    );

    // P3-batch, item 1 again: "`ZINCRBY` with `inf` then `-inf` yields `NaN`".
    let mut db = Db::new();
    assert!(matches!(
        redis.run(&Cmd::new("FLUSHALL").done()),
        Reply::Simple(_)
    ));
    both(
        &mut db,
        &mut redis,
        &Cmd::new("ZADD").s("k").s("inf").s("m").done(),
    );
    let (mine, theirs) = both(
        &mut db,
        &mut redis,
        &Cmd::new("ZINCRBY").s("k").s("-inf").s("m").done(),
    );
    assert_eq!(render(&mine), "$NaN", "ZINCRBY inf + -inf: {promoted}");
    assert!(
        matches!(theirs, Reply::Error(_)),
        "redis stopped rejecting a NaN result: {}",
        render(&theirs)
    );
    println!(
        "known-open divergences still open (plan P3-batch, `NaN` breaches sorted sets): \
         ZADD nan, ZINCRBY inf/-inf"
    );
}

/// The commands whose *reply* is deliberately unspecified. They are excluded
/// from the sequence generator — a `SPOP` that picks a different member on each
/// side desynchronizes the two keyspaces and every later reply becomes noise —
/// so their contract is checked here as a property instead of a diff, which is
/// the only honest way to cover them at all.
#[test]
fn nondeterministic_commands_agree_on_their_contract() {
    let _serial = one_reference_server_at_a_time();
    let Some(mut redis) = Redis::start() else {
        skip("no redis-server on PATH — the command differential needs a reference engine");
        return;
    };
    let mut db = Db::new();
    assert!(matches!(
        redis.run(&Cmd::new("FLUSHALL").done()),
        Reply::Simple(_)
    ));
    let all: Vec<String> = MEMBERS.iter().map(|m| m.to_string()).collect();
    let mut add = Cmd::new("SADD").s("s");
    for m in &all {
        add = add.s(m);
    }
    let add = add.done();
    both(&mut db, &mut redis, &add);

    let members = |r: &Reply| -> Vec<String> {
        match r {
            Reply::Array(v) => v.iter().map(as_text).collect(),
            Reply::Bulk(b) => vec![show_bytes(b)],
            _ => vec![],
        }
    };
    let card = |r: &Reply| -> i64 {
        match r {
            Reply::Int(i) => *i,
            _ => -1,
        }
    };

    // SRANDMEMBER with a positive count: distinct members of the set, count
    // capped at the cardinality. Both engines, same contract, different picks.
    for n in [1i64, 3, 50] {
        let q = Cmd::new("SRANDMEMBER").s("s").n(n).done();
        let (mine, theirs) = both(&mut db, &mut redis, &q);
        for (who, r) in [("locus", &mine), ("redis", &theirs)] {
            let got = members(r);
            assert_eq!(
                got.len(),
                (n as usize).min(all.len()),
                "{who} SRANDMEMBER s {n} returned {} members",
                got.len()
            );
            let mut sorted = got.clone();
            sorted.sort();
            sorted.dedup();
            assert_eq!(
                sorted.len(),
                got.len(),
                "{who} SRANDMEMBER s {n} repeated a member"
            );
            assert!(
                got.iter().all(|m| all.contains(m)),
                "{who} invented a member"
            );
        }
    }
    // A negative count: exactly |count| members, repeats allowed.
    let q = Cmd::new("SRANDMEMBER").s("s").n(-15).done();
    let (mine, theirs) = both(&mut db, &mut redis, &q);
    for (who, r) in [("locus", &mine), ("redis", &theirs)] {
        let got = members(r);
        assert_eq!(
            got.len(),
            15,
            "{who} SRANDMEMBER s -15 returned {}",
            got.len()
        );
        assert!(
            got.iter().all(|m| all.contains(m)),
            "{who} invented a member"
        );
    }

    // SPOP removes exactly one member that was in the set. After this the two
    // keyspaces legitimately differ, so it is the last thing done to `s`.
    let q = Cmd::new("SPOP").s("s").done();
    let (mine, theirs) = both(&mut db, &mut redis, &q);
    let after_l = card(&execute_reply(&mut db, &Cmd::new("SCARD").s("s").done()));
    let after_r = card(&redis.run(&Cmd::new("SCARD").s("s").done()));
    for (who, r, after) in [("locus", &mine, after_l), ("redis", &theirs, after_r)] {
        let got = members(r);
        assert_eq!(got.len(), 1, "{who} SPOP returned {got:?}");
        assert!(all.contains(&got[0]), "{who} SPOP invented a member");
        assert_eq!(after, all.len() as i64 - 1, "{who} SPOP did not remove one");
    }

    // RANDOMKEY returns a key that exists (or nil on an empty keyspace).
    for cmds in [
        vec![Cmd::new("FLUSHALL").done(), Cmd::new("RANDOMKEY").done()],
        vec![
            Cmd::new("SET").s("only").s("v").done(),
            Cmd::new("RANDOMKEY").done(),
        ],
    ] {
        let mut last = (Reply::Nil, Reply::Nil);
        for c in &cmds {
            last = both(&mut db, &mut redis, c);
        }
        let (mine, theirs) = last;
        assert_eq!(
            render(&mine),
            render(&theirs),
            "RANDOMKEY disagreed on a keyspace with at most one key"
        );
    }
    println!("nondeterministic commands: SPOP / SRANDMEMBER / RANDOMKEY contracts hold on both");
}

fn execute_reply(db: &mut Db, args: &[Vec<u8>]) -> Reply {
    let bytes = execute(args, db);
    let mut cur: &[u8] = &bytes;
    read_reply(&mut cur)
}

/// What the differential does *not* cover, printed rather than assumed. For
/// every Redis command in the shared type families, say whether the Locus
/// engine implements it — probed by asking `execute` and looking for
/// `unknown command`, so the answer cannot drift out of date.
#[test]
#[ignore]
fn coverage_report() {
    let _serial = one_reference_server_at_a_time();
    let Some(mut redis) = Redis::start() else {
        skip("no redis-server on PATH — the coverage report is measured against one");
        return;
    };
    let cats = [
        "string",
        "list",
        "hash",
        "set",
        "sortedset",
        "bitmap",
        "keyspace",
    ];
    let mut db = Db::new();
    let mut total_yes = 0;
    let mut total_no = 0;
    println!("\n=== differential coverage vs {} ===", redis.version);
    for cat in cats {
        let reply = redis.run(
            &Cmd::new("COMMAND")
                .s("LIST")
                .s("FILTERBY")
                .s("ACLCAT")
                .s(cat)
                .done(),
        );
        let mut names: Vec<String> = match reply {
            Reply::Array(v) => v.iter().map(|r| as_text(r).to_uppercase()).collect(),
            other => {
                println!("  {cat}: COMMAND LIST unavailable ({})", render(&other));
                continue;
            }
        };
        names.sort();
        let (mut yes, mut no) = (Vec::new(), Vec::new());
        for n in names {
            // Redis names a container command's subcommand `OBJECT|ENCODING`;
            // sent as one token that is of course unknown, so split it back into
            // the tokens a client would actually write, and add a key so the
            // probe reaches the real arm rather than an arity guard.
            let mut tokens: Vec<Vec<u8>> = n.split('|').map(|p| p.as_bytes().to_vec()).collect();
            if tokens.len() > 1 {
                tokens.push(b"probe-key".to_vec());
            }
            let probe = execute_reply(&mut db, &tokens);
            let unknown = |e: &str| {
                let e = e.to_ascii_lowercase();
                e.contains("unknown command") || e.contains("unknown") && e.contains("subcommand")
            };
            match probe {
                Reply::Error(e) if unknown(&e) => no.push(n),
                _ => yes.push(n),
            }
        }
        total_yes += yes.len();
        total_no += no.len();
        println!(
            "\n  {cat}: {} of {} implemented",
            yes.len(),
            yes.len() + no.len()
        );
        println!("    in:  {}", yes.join(" "));
        println!("    out: {}", no.join(" "));
    }
    println!(
        "\n  total across the shared families: {total_yes} implemented, {total_no} not.\n\
         \n  Excluded from the randomized sequences on purpose:\n\
         \x20   SPOP / SRANDMEMBER / RANDOMKEY  — the reply is unspecified; a different pick\n\
         \x20     desynchronizes the two keyspaces. Covered by a contract test instead.\n\
         \x20   SCAN / HSCAN / SSCAN / ZSCAN    — the cursor is engine-private. Covered by\n\
         \x20     comparing a *full iteration*, which is the only thing the protocol promises.\n\
         \x20   INCRBYFLOAT                     — Redis accumulates in x87 `long double`; we have\n\
         \x20     f64 and pure std. Identical wherever f64 reaches, divergent past it, and the\n\
         \x20     result is the stored value — so one divergent digit poisons the whole key.\n\
         \x20   OBJECT ENCODING / DEBUG / MEMORY — internal representation, divergent by design.\n\
         \x20   EXPIRE NX|XX|GT|LT              — Locus's EXPIRE takes no flags yet; that is an\n\
         \x20     open P3-batch item, not a finding of this harness.\n\
         \x20   Blocking forms (BLPOP, BZPOPMIN, …) — served by the hub, not the engine; the\n\
         \x20     library answers them non-blocking. tests/integration.rs covers the real ones.\n\
         \x20   Whole categories Locus does not chase (scripting, modules, functions, pubsub,\n\
         \x20     cluster, transactions) — see identity rule 4 in CLAUDE.md."
    );
}
