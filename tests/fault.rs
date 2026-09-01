//! **The fault-injection harness** — part B of phase 5.2.
//!
//! `tests/differential.rs` diffs the *engine* in-process, which is exactly what
//! makes it stable — and exactly why it can never see the hub, the replication
//! stream, failover or a slot migration. This half covers those: a real `locus`
//! binary over a real socket, a fault injected into the middle of the path, and
//! an assertion about what must still be true afterwards.
//!
//! These are correctness assertions under fault, not benchmarks.
//!
//! **Known-unsafe paths are asserted, not failed.** `docs/DEPLOYMENT.md` says
//! plainly what failover does *not* guarantee — a partitioned old master is
//! never fenced and its writes are silently discarded on reconciliation. A
//! harness that failed there would just be re-reporting a documented decision.
//! So the documented behaviour is pinned instead: if it ever changes, in either
//! direction, the test says so.
//!
//! ```text
//! cargo test --test fault
//! ```

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::sleep;
use std::time::{Duration, Instant};

// === the harness ============================================================

/// This harness's id in `free_port`'s slice map: fault injection.
const HARNESS: u32 = 3;

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

struct Server {
    child: Option<Child>,
    port: u16,
    rdb: String,
}

impl Server {
    fn start() -> Server {
        Server::start_env(&[])
    }

    fn start_env(extra: &[(&str, &str)]) -> Server {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let rdb = format!(
            "{}/locus-fault-{}-{}.rdb",
            std::env::temp_dir().display(),
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed),
        );
        let _ = std::fs::remove_file(&rdb);
        Server::spawn_at(&rdb, extra)
    }

    fn spawn_at(rdb: &str, extra: &[(&str, &str)]) -> Server {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_locus"));
        cmd.env("LOCUS_PORT", "0")
            .env("LOCUS_RDB", rdb)
            .env_remove("LOCUS_AOF")
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        for (k, v) in extra {
            cmd.env(k, v);
        }
        let mut child = cmd.spawn().expect("spawn locus");
        let stdout = child.stdout.take().expect("child stdout");
        let mut reader = BufReader::new(stdout);
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
        std::thread::spawn(move || {
            let mut sink = Vec::new();
            let _ = reader.read_to_end(&mut sink);
        });
        Server {
            child: Some(child),
            port,
            rdb: rdb.to_string(),
        }
    }

    /// Spawn on a *fixed* port, for a node that has to come back at the address
    /// its replicas and sentinels already know.
    fn spawn_on(port: u16, rdb: &str, extra: &[(&str, &str)]) -> Server {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_locus"));
        cmd.env("LOCUS_PORT", port.to_string())
            .env("LOCUS_RDB", rdb)
            .env_remove("LOCUS_AOF")
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        for (k, v) in extra {
            cmd.env(k, v);
        }
        let mut child = cmd.spawn().expect("spawn locus");
        let stdout = child.stdout.take().expect("child stdout");
        let mut reader = BufReader::new(stdout);
        loop {
            let mut line = String::new();
            if reader.read_line(&mut line).expect("read stdout") == 0 {
                let status = child.wait().ok();
                panic!("server on fixed port {port} exited early (status {status:?})");
            }
            if line.contains("listening") {
                break;
            }
        }
        std::thread::spawn(move || {
            let mut sink = Vec::new();
            let _ = reader.read_to_end(&mut sink);
        });
        Server {
            child: Some(child),
            port,
            rdb: rdb.to_string(),
        }
    }

    fn connect(&self) -> Conn {
        Conn::to(self.port)
    }

    /// SIGKILL — the fault. No SHUTDOWN, no snapshot, no goodbye.
    fn kill9(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.kill9();
        let _ = std::fs::remove_file(&self.rdb);
    }
}

struct Conn {
    stream: TcpStream,
    reader: BufReader<TcpStream>,
}

impl Conn {
    fn to(port: u16) -> Conn {
        let stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        stream.set_nodelay(true).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        Conn {
            reader: BufReader::new(stream.try_clone().unwrap()),
            stream,
        }
    }

    fn try_to(port: u16) -> Option<Conn> {
        let stream = TcpStream::connect(("127.0.0.1", port)).ok()?;
        stream.set_nodelay(true).ok()?;
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .ok()?;
        Some(Conn {
            reader: BufReader::new(stream.try_clone().ok()?),
            stream,
        })
    }

    fn encode(args: &[&str]) -> Vec<u8> {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            out.extend_from_slice(a.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        out
    }

    fn cmd(&mut self, args: &[&str]) -> String {
        self.try_cmd(args)
            .unwrap_or_else(|| panic!("connection died running {args:?}"))
    }

    /// `None` when the server is gone — which, in this file, is usually the
    /// point rather than an error.
    fn try_cmd(&mut self, args: &[&str]) -> Option<String> {
        self.stream.write_all(&Self::encode(args)).ok()?;
        self.read_reply()
    }

    fn read_reply(&mut self) -> Option<String> {
        let mut line = Vec::new();
        if self.reader.read_until(b'\n', &mut line).ok()? == 0 {
            return None;
        }
        while matches!(line.last(), Some(b'\n') | Some(b'\r')) {
            line.pop();
        }
        if line.is_empty() {
            return None;
        }
        let (tag, rest) = line.split_at(1);
        let rest = String::from_utf8_lossy(rest).to_string();
        match tag[0] {
            b'+' | b':' => Some(rest),
            b'-' => Some(format!("-{rest}")),
            b'$' => {
                let n: i64 = rest.parse().ok()?;
                if n < 0 {
                    return Some("(nil)".into());
                }
                let mut buf = vec![0u8; n as usize + 2];
                self.reader.read_exact(&mut buf).ok()?;
                Some(String::from_utf8_lossy(&buf[..n as usize]).to_string())
            }
            b'*' | b'%' | b'~' | b'>' => {
                let n: i64 = rest.parse().ok()?;
                if n < 0 {
                    return Some("(nil)".into());
                }
                let count = if tag[0] == b'%' { n * 2 } else { n };
                let items: Vec<String> = (0..count).map(|_| self.read_reply().unwrap()).collect();
                Some(format!("[{}]", items.join(", ")))
            }
            b',' | b'#' => Some(rest),
            b'_' => Some("(nil)".into()),
            other => panic!("unexpected reply tag {:?}", other as char),
        }
    }
}

fn info_field(c: &mut Conn, field: &str) -> String {
    let info = c.cmd(&["INFO"]);
    info.split("\r\n")
        .find_map(|l| l.strip_prefix(field)?.strip_prefix(':'))
        .unwrap_or("")
        .trim()
        .to_string()
}

fn role(port: u16) -> String {
    match Conn::try_to(port) {
        Some(mut c) => info_field(&mut c, "role"),
        None => "unreachable".into(),
    }
}

/// Poll until `f` is true, or fail with `msg`.
fn wait_until(deadline: Duration, msg: &str, mut f: impl FnMut() -> bool) {
    let end = Instant::now() + deadline;
    loop {
        if f() {
            return;
        }
        assert!(Instant::now() < end, "{msg}");
        sleep(Duration::from_millis(50));
    }
}

// === replication ============================================================

/// Kill the master mid-stream. Whatever the replica ends up holding must be a
/// **consistent prefix** of the write order — never a hole, never a write from
/// the future.
///
/// The fault is a SIGKILL landing in the middle of a stream of acknowledged
/// writes, so the replica is cut off at an arbitrary byte of an arbitrary
/// command. Replication is asynchronous, so the replica is allowed to be
/// *behind*; it is not allowed to be *inconsistent*.
#[test]
fn replication_master_killed_mid_stream_leaves_a_consistent_prefix() {
    const N: usize = 1500;
    let mut master = Server::start();
    let replica = Server::start();
    replica
        .connect()
        .cmd(&["REPLICAOF", "127.0.0.1", &master.port.to_string()]);
    wait_until(Duration::from_secs(10), "replica never linked up", || {
        info_field(&mut replica.connect(), "master_link_status") == "up"
    });

    // A writer at full speed, one command at a time so "acked" is exact.
    let mut w = master.connect();
    let writer = std::thread::spawn(move || {
        let mut acked = 0usize;
        for i in 0..N {
            match w.try_cmd(&["SET", &format!("k:{i}"), &i.to_string()]) {
                Some(r) if r == "OK" => acked += 1,
                _ => break, // the master is gone: stop counting
            }
        }
        acked
    });

    sleep(Duration::from_millis(120)); // let the stream get going
    master.kill9(); // <-- the fault
    let acked = writer.join().expect("writer thread");
    assert!(acked > 0, "no write was acknowledged before the kill");

    // Give the replica time to drain whatever bytes were already in flight.
    sleep(Duration::from_millis(500));
    let mut r = replica.connect();

    // Read the whole range back in one shot and find the boundary.
    let keys: Vec<String> = (0..N).map(|i| format!("k:{i}")).collect();
    let mut present = vec![false; N];
    for (chunk, base) in keys.chunks(200).zip((0..N).step_by(200)) {
        let mut args: Vec<&str> = vec!["MGET"];
        args.extend(chunk.iter().map(|s| s.as_str()));
        let reply = r.cmd(&args);
        let inner = reply.trim_start_matches('[').trim_end_matches(']');
        for (j, v) in inner.split(", ").enumerate() {
            present[base + j] = v != "(nil)";
        }
    }
    let applied = present.iter().take_while(|p| **p).count();
    let hole = present[applied..].iter().position(|p| *p);
    assert!(
        hole.is_none(),
        "the replica has a HOLE: it applied k:{} but not k:{applied} — replication is not a \
         consistent prefix",
        applied + hole.unwrap(),
    );
    assert!(
        applied <= acked,
        "the replica applied {applied} writes but only {acked} were ever acknowledged — it is \
         ahead of the master"
    );
    // And the values themselves have to be right, not just present.
    if applied > 0 {
        let last = applied - 1;
        assert_eq!(r.cmd(&["GET", &format!("k:{last}")]), last.to_string());
    }
    println!(
        "replication under SIGKILL: {acked} writes acknowledged, {applied} applied on the \
         replica — a prefix, no holes"
    );
}

// The raw replication-stream client. A replica that is *ours* lets the test see
// the master's byte offsets directly, which is the only way to assert that a
// partial resync resumes at exactly the right byte.

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

/// One array-of-bulks command off the replication stream, with the byte count
/// the master charged for it.
fn read_command_raw(s: &mut TcpStream) -> (Vec<Vec<u8>>, usize) {
    let mut consumed = 0;
    let hdr = read_line_raw(s);
    consumed += hdr.len() + 2;
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

fn psync(port: u16, replid: &str, offset: &str) -> (TcpStream, Vec<u8>) {
    let mut s = TcpStream::connect(("127.0.0.1", port)).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    send_resp(&mut s, &[b"PSYNC", replid.as_bytes(), offset.as_bytes()]);
    let line = read_line_raw(&mut s);
    (s, line)
}

/// Drop a replication link mid-stream, keep writing, reconnect at the last byte
/// we actually processed, and assert the resumed stream replays **exactly** the
/// commands we missed — no gap, no duplicate, in order.
///
/// The existing integration test proves `+CONTINUE` happens after one missed
/// write. This one injects the drop into a live stream and reassembles the whole
/// command log across the seam, which is the property a replica's correctness
/// actually rests on.
#[test]
fn replication_link_dropped_mid_stream_resumes_at_the_exact_byte() {
    const BEFORE: usize = 40;
    const MISSED: usize = 60;
    const AFTER: usize = 40;
    let server = Server::start();
    let mut writer = server.connect();

    let (mut repl, line) = psync(server.port, "?", "-1");
    let text = String::from_utf8_lossy(&line).to_string();
    let mut parts = text.trim_start_matches('+').split_whitespace();
    assert_eq!(parts.next(), Some("FULLRESYNC"), "got {text}");
    let replid = parts.next().unwrap().to_string();
    let mut offset: u64 = parts.next().unwrap().parse().unwrap();
    // The snapshot bulk: $<len>\r\n<len bytes>, no trailing CRLF.
    let hdr = read_line_raw(&mut repl);
    let len: usize = std::str::from_utf8(&hdr[1..]).unwrap().parse().unwrap();
    let mut snap = vec![0u8; len];
    repl.read_exact(&mut snap).unwrap();

    let mut seen: Vec<String> = Vec::new();
    for i in 0..BEFORE {
        assert_eq!(
            writer.cmd(&["SET", &format!("s:{i}"), &i.to_string()]),
            "OK"
        );
        let (cmd, n) = read_command_raw(&mut repl);
        offset += n as u64;
        seen.push(String::from_utf8_lossy(&cmd[1]).to_string());
    }

    // <-- the fault: the link dies, mid-stream, at a known byte.
    drop(repl);
    for i in BEFORE..BEFORE + MISSED {
        assert_eq!(
            writer.cmd(&["SET", &format!("s:{i}"), &i.to_string()]),
            "OK"
        );
    }

    let (mut repl2, line) = psync(server.port, &replid, &offset.to_string());
    assert!(
        line.starts_with(b"+CONTINUE"),
        "expected +CONTINUE at offset {offset}, got {}",
        String::from_utf8_lossy(&line)
    );
    for _ in 0..MISSED {
        let (cmd, _) = read_command_raw(&mut repl2);
        seen.push(String::from_utf8_lossy(&cmd[1]).to_string());
    }
    for i in BEFORE + MISSED..BEFORE + MISSED + AFTER {
        assert_eq!(
            writer.cmd(&["SET", &format!("s:{i}"), &i.to_string()]),
            "OK"
        );
        let (cmd, _) = read_command_raw(&mut repl2);
        seen.push(String::from_utf8_lossy(&cmd[1]).to_string());
    }

    let expected: Vec<String> = (0..BEFORE + MISSED + AFTER)
        .map(|i| format!("s:{i}"))
        .collect();
    assert_eq!(
        seen, expected,
        "the stream across the drop is not the issued sequence"
    );
    println!(
        "replication link drop: {} commands reassembled across the seam, exact and in order",
        seen.len()
    );
}

/// A replica that fell too far behind must be told to start over, not handed a
/// stream with a hole in it. Overrun the 4 MiB backlog while disconnected and
/// assert the reconnect is refused a partial resync.
#[test]
fn replication_reconnect_past_the_backlog_is_refused_a_partial_resync() {
    let server = Server::start();
    let mut writer = server.connect();

    let (mut repl, line) = psync(server.port, "?", "-1");
    let text = String::from_utf8_lossy(&line).to_string();
    let mut parts = text.trim_start_matches('+').split_whitespace();
    assert_eq!(parts.next(), Some("FULLRESYNC"), "got {text}");
    let replid = parts.next().unwrap().to_string();
    let mut offset: u64 = parts.next().unwrap().parse().unwrap();
    let hdr = read_line_raw(&mut repl);
    let len: usize = std::str::from_utf8(&hdr[1..]).unwrap().parse().unwrap();
    let mut snap = vec![0u8; len];
    repl.read_exact(&mut snap).unwrap();

    assert_eq!(writer.cmd(&["SET", "anchor", "0"]), "OK");
    let (_, n) = read_command_raw(&mut repl);
    offset += n as u64;
    drop(repl); // <-- the fault

    // Push more than the 4 MiB backlog through while we are away.
    let big = "x".repeat(64 * 1024);
    for i in 0..80 {
        assert_eq!(writer.cmd(&["SET", &format!("big:{i}"), &big]), "OK");
    }

    let (_repl2, line) = psync(server.port, &replid, &offset.to_string());
    assert!(
        line.starts_with(b"+FULLRESYNC"),
        "a replica whose offset fell out of the backlog was NOT sent for a full resync — it got \
         {}, which would leave it with a silent hole",
        String::from_utf8_lossy(&line)
    );
    println!("backlog overrun: the stale replica is sent for a full resync, not a partial one");
}

// === failover ===============================================================

/// A child process that is killed on drop — the sentinels.
struct Proc(Child);

impl Drop for Proc {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn_sentinel(master: u16, replicas: &[u16], my_port: u16, peer: u16) -> Proc {
    let list = replicas
        .iter()
        .map(|p| format!("127.0.0.1:{p}"))
        .collect::<Vec<_>>()
        .join(",");
    Proc(
        Command::new(env!("CARGO_BIN_EXE_locus"))
            .env("LOCUS_SENTINEL", format!("127.0.0.1:{master}"))
            .env("LOCUS_SENTINEL_REPLICAS", list)
            .env("LOCUS_SENTINEL_PORT", my_port.to_string())
            .env("LOCUS_SENTINEL_PEERS", format!("127.0.0.1:{peer}"))
            .env("LOCUS_SENTINEL_PEER_SECRET", "fault-harness-secret")
            .env("LOCUS_SENTINEL_DOWN_AFTER_MS", "700")
            .env("LOCUS_SENTINEL_INTERVAL_MS", "200")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn sentinel"),
    )
}

/// Kill the master under a live write load and assert the two things failover
/// actually promises: **exactly one** replica is promoted (never both, at any
/// moment we look), and **every write `WAIT` acknowledged** is on the survivor.
///
/// `WAIT 2` — both replicas — is deliberate. `WAIT 1` would not be a real
/// invariant here: the replica that acked need not be the one promoted, and
/// `docs/DEPLOYMENT.md` is explicit that asynchronous replication can lose an
/// unacknowledged write on promotion. The harness counts those losses and
/// reports them rather than failing, because they are the documented behaviour.
#[test]
fn failover_promotes_once_and_keeps_every_wait_acknowledged_write() {
    let master = Server::start();
    let r1 = Server::start();
    let r2 = Server::start();
    let mport = master.port;
    for r in [&r1, &r2] {
        r.connect()
            .cmd(&["REPLICAOF", "127.0.0.1", &mport.to_string()]);
    }
    for r in [&r1, &r2] {
        wait_until(Duration::from_secs(10), "replica never linked up", || {
            info_field(&mut r.connect(), "master_link_status") == "up"
        });
    }

    // Write until the master dies, recording exactly what was durable.
    let mut w = master.connect();
    let writer = std::thread::spawn(move || {
        let (mut durable, mut unacked) = (Vec::new(), 0usize);
        for i in 0..4000 {
            match w.try_cmd(&["SET", &format!("w:{i}"), &i.to_string()]) {
                Some(r) if r == "OK" => {}
                _ => break,
            }
            match w.try_cmd(&["WAIT", "2", "60"]) {
                Some(n) if n.parse::<i64>().unwrap_or(0) >= 2 => durable.push(i),
                Some(_) => unacked += 1,
                None => break,
            }
        }
        (durable, unacked)
    });

    let (sp1, sp2) = (free_port(), free_port());
    let _s1 = spawn_sentinel(mport, &[r1.port, r2.port], sp1, sp2);
    let _s2 = spawn_sentinel(mport, &[r1.port, r2.port], sp2, sp1);
    sleep(Duration::from_millis(600)); // let the sentinels see a healthy master

    let mut master = master;
    master.kill9(); // <-- the fault
    let (durable, unacked) = writer.join().expect("writer thread");
    assert!(
        !durable.is_empty(),
        "no write reached both replicas before the kill — the test proved nothing"
    );

    // Exactly one promotion. Checked on every poll, not just at the end: a
    // split-brain that healed before we looked would still be a split-brain.
    let deadline = Instant::now() + Duration::from_secs(20);
    let (new_master, follower) = loop {
        let m1 = role(r1.port) == "master";
        let m2 = role(r2.port) == "master";
        assert!(!(m1 && m2), "SPLIT BRAIN: both replicas are masters");
        if m1 {
            break (&r1, &r2);
        }
        if m2 {
            break (&r2, &r1);
        }
        assert!(Instant::now() < deadline, "no promotion within 20s");
        sleep(Duration::from_millis(100));
    };

    // Every WAIT-acknowledged write survived the promotion.
    let mut c = new_master.connect();
    let mut lost = Vec::new();
    for chunk in durable.chunks(200) {
        let keys: Vec<String> = chunk.iter().map(|i| format!("w:{i}")).collect();
        let mut args: Vec<&str> = vec!["MGET"];
        args.extend(keys.iter().map(|s| s.as_str()));
        let reply = c.cmd(&args);
        let inner = reply.trim_start_matches('[').trim_end_matches(']');
        for (j, v) in inner.split(", ").enumerate() {
            if v == "(nil)" {
                lost.push(chunk[j]);
            }
        }
    }
    assert!(
        lost.is_empty(),
        "{} write(s) that WAIT 2 acknowledged are GONE from the promoted master (first: w:{}) — \
         acknowledged writes must survive a promotion",
        lost.len(),
        lost[0]
    );

    // Still one master after the dust settles, and the follower re-attaches.
    assert_eq!(role(new_master.port), "master");
    wait_until(
        Duration::from_secs(20),
        "the surviving follower was never repointed at the new master",
        || role(follower.port) == "slave",
    );
    assert_ne!(
        role(new_master.port),
        role(follower.port),
        "both nodes settled into the same role"
    );
    println!(
        "failover: exactly one promotion; {} WAIT-2 acknowledged writes all survived \
         ({unacked} writes were acked by the master but not by both replicas — the documented \
         asynchronous-replication window)",
        durable.len()
    );
}

/// **A documented-unsafe path, pinned rather than failed.**
///
/// `docs/DEPLOYMENT.md`: "A partitioned old master is never fenced. It goes on
/// accepting writes while cut off, and those writes are silently discarded when
/// it is reconciled back to a replica."
///
/// This asserts exactly that, in both halves: the resurrected old master *does*
/// take a write (no fencing), and that write *is* gone once the sentinel
/// reconciles it. If either half ever changes — fencing arrives, or the discard
/// stops being silent — this test fails and says which, which is the whole point
/// of writing a known limitation down as an assertion instead of a paragraph.
#[test]
fn resurrected_old_master_is_not_fenced_and_its_writes_are_discarded() {
    let mport = free_port();
    let rdb = format!(
        "{}/locus-fault-oldmaster-{}.rdb",
        std::env::temp_dir().display(),
        std::process::id()
    );
    let _ = std::fs::remove_file(&rdb);
    let mut master = Server::spawn_on(mport, &rdb, &[]);
    let r1 = Server::start();
    let r2 = Server::start();
    for r in [&r1, &r2] {
        r.connect()
            .cmd(&["REPLICAOF", "127.0.0.1", &mport.to_string()]);
    }
    for r in [&r1, &r2] {
        wait_until(Duration::from_secs(10), "replica never linked up", || {
            info_field(&mut r.connect(), "master_link_status") == "up"
        });
    }
    master.connect().cmd(&["SET", "before", "failover"]);

    let (sp1, sp2) = (free_port(), free_port());
    let _s1 = spawn_sentinel(mport, &[r1.port, r2.port], sp1, sp2);
    let _s2 = spawn_sentinel(mport, &[r1.port, r2.port], sp2, sp1);
    sleep(Duration::from_millis(600));

    master.kill9(); // <-- the fault: the master "partitions away"
    let deadline = Instant::now() + Duration::from_secs(20);
    let new_master = loop {
        if role(r1.port) == "master" {
            break &r1;
        }
        if role(r2.port) == "master" {
            break &r2;
        }
        assert!(Instant::now() < deadline, "no promotion within 20s");
        sleep(Duration::from_millis(100));
    };

    // It comes back at the same address, still believing it is the master.
    let revived = Server::spawn_on(mport, &rdb, &[]);
    let mut c = revived.connect();
    assert_eq!(
        info_field(&mut c, "role"),
        "master",
        "the returned old master came back as a replica — it is being fenced now, which is \
         BETTER than documented: update docs/DEPLOYMENT.md and this test"
    );
    let took_it = c.try_cmd(&["SET", "orphan", "written-while-cut-off"]);
    assert_eq!(
        took_it.as_deref(),
        Some("OK"),
        "the resurrected old master refused the write — it is fenced now, which contradicts \
         docs/DEPLOYMENT.md's stated limitation"
    );

    // ...and the sentinel reconciles it, taking that write with it, silently.
    wait_until(
        Duration::from_secs(25),
        "the returned old master was never reconciled into a replica",
        || role(mport) == "slave",
    );
    wait_until(
        Duration::from_secs(15),
        "the reconciled old master never resynced from the new master",
        || {
            Conn::try_to(mport)
                .map(|mut c| info_field(&mut c, "master_link_status") == "up")
                .unwrap_or(false)
        },
    );
    let mut c = Conn::try_to(mport).expect("old master reachable");
    assert_eq!(
        c.cmd(&["GET", "orphan"]),
        "(nil)",
        "the write the unfenced old master accepted SURVIVED reconciliation — that is a change \
         from the documented behaviour and needs a look either way"
    );
    assert_eq!(
        c.cmd(&["GET", "before"]),
        "failover",
        "data written before the failover did not survive"
    );
    assert_eq!(new_master.connect().cmd(&["GET", "orphan"]), "(nil)");
    let _ = std::fs::remove_file(&rdb);
    println!(
        "documented-unsafe path pinned: the resurrected old master was NOT fenced (it accepted a \
         write), and that write was silently discarded on reconciliation — exactly what \
         docs/DEPLOYMENT.md says"
    );
}

// === resharding =============================================================

/// A cluster node on a fixed port — its peers are named on the command line, so
/// it cannot be given `LOCUS_PORT=0` and asked afterwards.
fn cluster_node(port: u16, nodes: &str, rdb: &str) -> Server {
    Server::spawn_on(
        port,
        rdb,
        &[
            ("LOCUS_CLUSTER_ENABLED", "1"),
            ("LOCUS_CLUSTER_ANNOUNCE", &format!("127.0.0.1:{port}")),
            ("LOCUS_CLUSTER_NODES", nodes),
            ("LOCUS_CLUSTER_GOSSIP_MS", "200"),
        ],
    )
}

/// A cluster client: follows `MOVED`, waits out `CLUSTERDOWN`, and counts a
/// write only when it has an `+OK` in hand.
struct ClusterClient {
    conns: Vec<Conn>,
    ports: Vec<u16>,
    at: usize,
}

impl ClusterClient {
    fn new(ports: &[u16]) -> ClusterClient {
        ClusterClient {
            conns: ports.iter().map(|p| Conn::to(*p)).collect(),
            ports: ports.to_vec(),
            at: 0,
        }
    }

    /// `Some(reply)` once a node actually answered; `None` if it never settled.
    fn route(&mut self, args: &[&str]) -> Option<String> {
        for _ in 0..40 {
            let reply = self.conns[self.at].try_cmd(args)?;
            if let Some(rest) = reply.strip_prefix("-MOVED ") {
                // "-MOVED <slot> <host:port>" — follow it.
                let addr = rest.split_whitespace().nth(1).unwrap_or("");
                let port: u16 = addr.rsplit(':').next()?.parse().ok()?;
                self.at = self.ports.iter().position(|p| *p == port)?;
                continue;
            }
            if reply.starts_with("-CLUSTERDOWN") {
                // The slot is mid-handover and no node claims it yet. A real
                // client retries; so do we.
                sleep(Duration::from_millis(5));
                continue;
            }
            return Some(reply);
        }
        None
    }
}

/// Migrate a slot **while it is being written to** and assert the two things
/// resharding promises: no acknowledged key is lost, and none is left duplicated
/// on the source.
///
/// The fault is the concurrency itself — `CLUSTER MIGRATESLOT` copies the slot
/// and flips ownership under a writer that never stops, so every acknowledged
/// write lands either just before or just after the handover, and the harness
/// does not get to know which.
#[test]
fn reshard_under_concurrent_writes_loses_and_duplicates_nothing() {
    let (p1, p2) = (free_port(), free_port());
    let nodes = format!("127.0.0.1:{p1} 0-8191;127.0.0.1:{p2} 8192-16383");
    let rdb1 = format!(
        "{}/locus-fault-c1-{}-{p1}.rdb",
        std::env::temp_dir().display(),
        std::process::id()
    );
    let rdb2 = format!(
        "{}/locus-fault-c2-{}-{p2}.rdb",
        std::env::temp_dir().display(),
        std::process::id()
    );
    let _ = std::fs::remove_file(&rdb1);
    let _ = std::fs::remove_file(&rdb2);
    let _n1 = cluster_node(p1, &nodes, &rdb1);
    let _n2 = cluster_node(p2, &nodes, &rdb2);

    // One hashtag, so every key the writer touches is in the slot being moved.
    let mut c1 = Conn::to(p1);
    let (tag, slot) = (0..500)
        .find_map(|i| {
            let t = format!("t{i}");
            let s: i64 = c1
                .cmd(&["CLUSTER", "KEYSLOT", &format!("{{{t}}}:0")])
                .parse()
                .unwrap();
            (s <= 8191).then_some((t, s))
        })
        .expect("a hashtag owned by node 1");

    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop_w = stop.clone();
    let tag_w = tag.clone();
    let writer = std::thread::spawn(move || {
        let mut cc = ClusterClient::new(&[p1, p2]);
        let mut acked: Vec<usize> = Vec::new();
        let mut i = 0usize;
        while !stop_w.load(Ordering::Relaxed) && i < 20_000 {
            let k = format!("{{{tag_w}}}:{i}");
            match cc.route(&["SET", &k, &i.to_string()]) {
                Some(r) if r == "OK" => acked.push(i),
                Some(_) | None => {}
            }
            i += 1;
        }
        acked
    });

    sleep(Duration::from_millis(200)); // let a body of keys accumulate
    let dst = format!("127.0.0.1:{p2}");
    let moved = c1.cmd(&["CLUSTER", "MIGRATESLOT", &slot.to_string(), &dst]); // <-- the fault
    let moved: i64 = moved
        .parse()
        .unwrap_or_else(|_| panic!("MIGRATESLOT failed: {moved}"));
    assert_eq!(
        Conn::to(p2).cmd(&["CLUSTER", "SETSLOT", &slot.to_string(), "NODE", &dst]),
        "OK"
    );
    sleep(Duration::from_millis(200)); // keep writing across the handover
    stop.store(true, Ordering::Relaxed);
    let acked = writer.join().expect("writer thread");
    assert!(
        acked.len() > moved as usize,
        "the writer ({} acked) did not outlive the migration ({moved} keys moved) — the test \
         did not actually overlap them",
        acked.len()
    );

    // Not one acknowledged key may be missing, and every value must be its own.
    let mut cc = ClusterClient::new(&[p1, p2]);
    let mut lost = Vec::new();
    for i in &acked {
        let k = format!("{{{tag}}}:{i}");
        match cc.route(&["GET", &k]) {
            Some(v) if v == i.to_string() => {}
            _ => lost.push(*i),
        }
    }
    assert!(
        lost.is_empty(),
        "{} of {} acknowledged writes did not survive the slot migration (first: {{{tag}}}:{}) — \
         resharding is documented as zero-loss",
        lost.len(),
        acked.len(),
        lost[0]
    );

    // And no duplicate: the source must redirect rather than answer from a copy
    // it kept. A stale local copy is how a reshard silently forks a key.
    let mut src = Conn::to(p1);
    for i in acked.iter().take(50) {
        let k = format!("{{{tag}}}:{i}");
        let r = src.cmd(&["GET", &k]);
        assert!(
            r.starts_with("-MOVED") && r.contains(&dst),
            "the source still answers for {k} after handing the slot over ({r}) — that is a \
             duplicated key, not a migrated one"
        );
    }
    println!(
        "reshard under load: {} writes acknowledged across the migration of slot {slot} \
         ({moved} keys moved by MIGRATESLOT), none lost, none duplicated on the source",
        acked.len()
    );
    let _ = std::fs::remove_file(&rdb1);
    let _ = std::fs::remove_file(&rdb2);
}

/// Kill the destination *before* the migration and assert the slot does not
/// evaporate: `MIGRATESLOT` fails, ownership stays where it was, and every key
/// is still served by the source.
///
/// The failure mode this guards is the bad one — a half-migration that deletes
/// the source copy before the destination has it.
#[test]
fn reshard_to_an_unreachable_destination_keeps_every_key_on_the_source() {
    let (p1, p2) = (free_port(), free_port());
    let nodes = format!("127.0.0.1:{p1} 0-8191;127.0.0.1:{p2} 8192-16383");
    let rdb1 = format!(
        "{}/locus-fault-d1-{}-{p1}.rdb",
        std::env::temp_dir().display(),
        std::process::id()
    );
    let rdb2 = format!(
        "{}/locus-fault-d2-{}-{p2}.rdb",
        std::env::temp_dir().display(),
        std::process::id()
    );
    let _ = std::fs::remove_file(&rdb1);
    let _ = std::fs::remove_file(&rdb2);
    let _n1 = cluster_node(p1, &nodes, &rdb1);
    let mut n2 = cluster_node(p2, &nodes, &rdb2);

    let mut c1 = Conn::to(p1);
    let (tag, slot) = (0..500)
        .find_map(|i| {
            let t = format!("t{i}");
            let s: i64 = c1
                .cmd(&["CLUSTER", "KEYSLOT", &format!("{{{t}}}:0")])
                .parse()
                .unwrap();
            (s <= 8191).then_some((t, s))
        })
        .expect("a hashtag owned by node 1");
    for i in 0..200 {
        assert_eq!(
            c1.cmd(&["SET", &format!("{{{tag}}}:{i}"), &i.to_string()]),
            "OK"
        );
    }

    n2.kill9(); // <-- the fault: the destination is gone before the move starts
    let reply = c1.cmd(&[
        "CLUSTER",
        "MIGRATESLOT",
        &slot.to_string(),
        &format!("127.0.0.1:{p2}"),
    ]);
    assert!(
        reply.starts_with('-') && reply.contains("no keys removed"),
        "MIGRATESLOT with a dead destination must fail *and* say it kept the source copy; it          said: {reply}"
    );

    // Nothing moved, so nothing may be missing — and the source must still own
    // the slot rather than having handed it to a node that never got the data.
    for i in 0..200 {
        assert_eq!(
            c1.cmd(&["GET", &format!("{{{tag}}}:{i}")]),
            i.to_string(),
            "key {{{tag}}}:{i} was lost by a migration that failed"
        );
    }
    // CLUSTER SLOTS renders each owner as [host, port], so match on the pair.
    let slots = c1.cmd(&["CLUSTER", "SLOTS"]);
    assert!(
        slots.contains(&format!("127.0.0.1, {p1}")),
        "the source stopped owning its slots after a failed migration: {slots}"
    );
    println!(
        "reshard to a dead destination: MIGRATESLOT refused ({}), all 200 keys still served by \
         the source",
        reply.split_whitespace().next().unwrap_or("")
    );
    let _ = std::fs::remove_file(&rdb1);
    let _ = std::fs::remove_file(&rdb2);
}
