//! Sentinel-lite: automatic failover monitor (std-only, zero-dependency).
//!
//! Run the same `locus` binary as a monitor by setting `LOCUS_SENTINEL` to the
//! master's `host:port`. The sentinel periodically PINGs the master; if it stays
//! unreachable for `down-after`, the sentinel promotes the most up-to-date replica
//! (highest `master_repl_offset`) with `REPLICAOF NO ONE` and repoints the others
//! at it. While the master is healthy it also *reconciles* — any node that isn't
//! replicating from the current master (e.g. a returned old master) is pointed
//! back, which keeps a flapping node from causing split-brain.
//!
//! Before failing over it requires *corroboration* on two axes:
//!   * replica corroboration — a quorum of replicas must also report their master
//!     link down (guards a single sentinel partitioned from the master); and
//!   * inter-sentinel agreement (when peers are configured) — a majority of
//!     sentinels must see the master down, and only the *leader* (the lowest id
//!     among the down-seeing sentinels) performs the promotion. The majority gate
//!     stops a partitioned minority; the leader rule stops two sentinels promoting
//!     different replicas. This is the orchestration-hook tier — a bully-style
//!     election over a tiny line protocol, not full Raft epochs.
//!
//! Config (env):
//!   LOCUS_SENTINEL            master host:port to monitor (enables sentinel mode)
//!   LOCUS_SENTINEL_REPLICAS   comma-separated replica host:port list
//!   LOCUS_SENTINEL_AUTH       password presented to monitored nodes (optional)
//!   LOCUS_SENTINEL_DOWN_AFTER_MS   master-down grace before failover (default 5000)
//!   LOCUS_SENTINEL_INTERVAL_MS     poll interval (default 1000)
//!   LOCUS_SENTINEL_QUORUM     replicas that must confirm down before failover (default 1)
//!   LOCUS_SENTINEL_PORT       listen port for peer-sentinel "is the master down?" queries
//!   LOCUS_SENTINEL_PEERS      comma-separated peer sentinel host:port list
//!   LOCUS_SENTINEL_ID         this sentinel's id for leader election (default 127.0.0.1:PORT)

use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use crate::log;

const IO_TIMEOUT: Duration = Duration::from_millis(800);

/// Entry point for sentinel mode (never returns). Caller checks `LOCUS_SENTINEL`.
pub fn run() -> io::Result<()> {
    let master0 = std::env::var("LOCUS_SENTINEL").unwrap_or_default();
    let auth = std::env::var("LOCUS_SENTINEL_AUTH")
        .ok()
        .filter(|s| !s.is_empty());
    let down_after = env_ms("LOCUS_SENTINEL_DOWN_AFTER_MS", 5000);
    let interval = env_ms("LOCUS_SENTINEL_INTERVAL_MS", 1000);
    // How many replicas must ALSO report their master link down before we fail
    // over — corroboration that guards against a partitioned sentinel acting
    // alone. 0 = trust this sentinel's view only. Keep it <= replica count.
    let quorum = std::env::var("LOCUS_SENTINEL_QUORUM")
        .ok()
        .and_then(|s| s.trim().parse::<usize>().ok())
        .unwrap_or(1);

    // Peer sentinels for inter-sentinel agreement (optional). When configured,
    // failover also requires a majority of sentinels to see the master down and
    // this sentinel to be the leader. `master_down` is published to peers via a
    // tiny line server on LOCUS_SENTINEL_PORT.
    let peers: Vec<String> = std::env::var("LOCUS_SENTINEL_PEERS")
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    let sentinel_port = std::env::var("LOCUS_SENTINEL_PORT")
        .ok()
        .filter(|s| !s.is_empty());
    let my_id = std::env::var("LOCUS_SENTINEL_ID").ok().unwrap_or_else(|| {
        sentinel_port
            .as_deref()
            .map(|p| format!("127.0.0.1:{p}"))
            .unwrap_or_default()
    });
    let master_down = Arc::new(AtomicBool::new(false));
    if let Some(port) = sentinel_port.clone() {
        let flag = master_down.clone();
        thread::spawn(move || serve_peers(&port, flag));
    }

    // The full node set is the master plus every configured replica.
    let mut nodes: Vec<String> = vec![master0.clone()];
    for r in std::env::var("LOCUS_SENTINEL_REPLICAS")
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
    {
        if !nodes.contains(&r) {
            nodes.push(r);
        }
    }

    let mut master = master0;
    let mut down_since: Option<Instant> = None;
    log::info(&format!(
        "sentinel: monitoring master {master} + {} replica(s); down-after {down_after}ms, replica-quorum {quorum}, {} peer sentinel(s)",
        nodes.len() - 1,
        peers.len()
    ));

    loop {
        std::thread::sleep(Duration::from_millis(interval));
        let auth = auth.as_deref();

        if alive(&master, auth) {
            down_since = None;
            master_down.store(false, Ordering::Relaxed);
            reconcile(&nodes, &master, auth);
        } else {
            let since = *down_since.get_or_insert_with(Instant::now);
            log::warn(&format!("sentinel: master {master} unreachable"));
            if since.elapsed() >= Duration::from_millis(down_after) {
                master_down.store(true, Ordering::Relaxed); // let peers corroborate
                if !confirmed_down(&nodes, &master, auth, quorum) {
                    log::warn(
                        "sentinel: holding failover — replicas still reach the master (likely a local partition)",
                    );
                } else if !sentinel_leader(&my_id, &peers) {
                    log::warn(
                        "sentinel: holding failover — no sentinel majority, or another sentinel leads",
                    );
                } else if let Some(new_master) = failover(&nodes, &master, auth) {
                    log::info(&format!(
                        "sentinel: +switch-master {master} -> {new_master}"
                    ));
                    master = new_master;
                    down_since = None;
                    master_down.store(false, Ordering::Relaxed);
                }
            }
        }
    }
}

/// Ensure every node other than `master` is a replica of `master`; repoint any
/// that isn't (a fresh replica, or an old master that just came back).
fn reconcile(nodes: &[String], master: &str, auth: Option<&str>) {
    let (mh, mp) = split_hostport(master);
    for n in nodes.iter().filter(|n| n.as_str() != master) {
        let Some(inf) = info(n, auth) else { continue }; // unreachable -> skip
        let role = field(&inf, "role");
        let host = field(&inf, "master_host");
        let port = field(&inf, "master_port");
        let aligned = role.as_deref() == Some("slave")
            && host.as_deref() == Some(mh)
            && port.as_deref() == Some(mp);
        if !aligned {
            log::info(&format!("sentinel: repointing {n} -> {master}"));
            let _ = command(n, auth, &["REPLICAOF", mh, mp]);
        }
    }
}

/// Promote the most up-to-date reachable replica and repoint the rest at it.
fn failover(nodes: &[String], old_master: &str, auth: Option<&str>) -> Option<String> {
    let mut best: Option<(String, u64)> = None;
    for n in nodes.iter().filter(|n| n.as_str() != old_master) {
        if let Some(inf) = info(n, auth) {
            let off = field(&inf, "master_repl_offset")
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(0);
            if best.as_ref().is_none_or(|(_, b)| off > *b) {
                best = Some((n.clone(), off));
            }
        }
    }
    let (winner, off) = best?;
    log::info(&format!("sentinel: promoting {winner} (offset {off})"));
    command(&winner, auth, &["REPLICAOF", "NO", "ONE"]).ok()?;
    let (wh, wp) = split_hostport(&winner);
    for n in nodes
        .iter()
        .filter(|n| n.as_str() != winner && n.as_str() != old_master)
    {
        let _ = command(n, auth, &["REPLICAOF", wh, wp]); // best-effort
    }
    Some(winner)
}

/// Corroborate that the master is really down (not just unreachable from this
/// sentinel) by counting replicas that report their own master link as down.
/// Guards against a partitioned sentinel triggering a needless failover.
fn confirmed_down(nodes: &[String], master: &str, auth: Option<&str>, quorum: usize) -> bool {
    if quorum == 0 {
        return true; // trust this sentinel's view alone
    }
    let confirms = nodes
        .iter()
        .filter(|n| n.as_str() != master)
        .filter_map(|n| info(n, auth))
        .filter(|inf| field(inf, "master_link_status").as_deref() == Some("down"))
        .count();
    confirms >= quorum
}

// === inter-sentinel agreement ================================================

/// With peers configured, return true only if a majority of sentinels currently
/// see the master down AND this sentinel is the leader (lowest id among the
/// down-seeing sentinels). Single-sentinel (no peers) → always true (replica
/// corroboration alone governs).
fn sentinel_leader(my_id: &str, peers: &[String]) -> bool {
    if peers.is_empty() {
        return true;
    }
    // Down-seeing set: self (we only call this once we've decided down) + reachable
    // peers answering "1".
    let mut down_seeing = vec![my_id.to_string()];
    for p in peers {
        if peer_isdown(p) == Some(true) {
            down_seeing.push(p.clone());
        }
    }
    let majority = peers.len().div_ceil(2) + 1; // > half of (peers + self)
    if down_seeing.len() < majority {
        return false; // not enough sentinels agree (partition / flap)
    }
    // Deterministic leader: the lowest id among down-seeing sentinels acts; the
    // rest defer, so exactly one promotion happens.
    down_seeing.iter().min().map(|s| s.as_str()) == Some(my_id)
}

/// Ask a peer sentinel whether it currently sees the master down. None = peer
/// unreachable (so it can't count toward the majority).
fn peer_isdown(peer: &str) -> Option<bool> {
    let go = || -> io::Result<bool> {
        let mut s = connect(peer, None)?;
        s.write_all(b"ISDOWN\n")?;
        Ok(read_line(&mut s)? == b"1")
    };
    go().ok()
}

/// Serve peer "ISDOWN" / "PING" queries on a line protocol, reporting our own
/// view of the master via the shared `master_down` flag.
fn serve_peers(port: &str, master_down: Arc<AtomicBool>) {
    let listener = match TcpListener::bind(format!("0.0.0.0:{port}")) {
        Ok(l) => l,
        Err(e) => return log::error(&format!("sentinel: peer listener bind failed: {e}")),
    };
    log::info(&format!("sentinel: peer agreement listening on :{port}"));
    for stream in listener.incoming().flatten() {
        let flag = master_down.clone();
        thread::spawn(move || {
            let mut s = stream;
            let _ = s.set_read_timeout(Some(IO_TIMEOUT));
            if let Ok(line) = read_line(&mut s) {
                let reply: &[u8] = match line.as_slice() {
                    b"ISDOWN" if flag.load(Ordering::Relaxed) => b"1\n",
                    b"ISDOWN" => b"0\n",
                    b"PING" => b"PONG\n",
                    _ => b"-ERR\n",
                };
                let _ = s.write_all(reply);
            }
        });
    }
}

// === tiny RESP client ========================================================

fn connect(addr: &str, auth: Option<&str>) -> io::Result<TcpStream> {
    let sa = addr
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "bad addr"))?;
    let mut s = TcpStream::connect_timeout(&sa, IO_TIMEOUT)?;
    s.set_read_timeout(Some(IO_TIMEOUT))?;
    s.set_write_timeout(Some(IO_TIMEOUT))?;
    if let Some(pw) = auth {
        send(&mut s, &["AUTH", pw])?;
        read_reply(&mut s)?;
    }
    Ok(s)
}

fn send(s: &mut TcpStream, args: &[&str]) -> io::Result<()> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        out.extend_from_slice(a.as_bytes());
        out.extend_from_slice(b"\r\n");
    }
    s.write_all(&out)
}

/// Read one reply, returning its textual payload (bulk body, or the line after a
/// `+`/`-`/`:` tag). Sufficient for PING / INFO / REPLICAOF.
fn read_reply(s: &mut TcpStream) -> io::Result<String> {
    let line = read_line(s)?;
    if line.is_empty() {
        return Ok(String::new());
    }
    match line[0] {
        b'$' => {
            let n: i64 = std::str::from_utf8(&line[1..])
                .ok()
                .and_then(|x| x.trim().parse().ok())
                .unwrap_or(-1);
            if n < 0 {
                return Ok(String::new());
            }
            let mut body = vec![0u8; n as usize + 2]; // payload + CRLF
            s.read_exact(&mut body)?;
            body.truncate(n as usize);
            Ok(String::from_utf8_lossy(&body).to_string())
        }
        _ => Ok(String::from_utf8_lossy(&line[1..]).to_string()),
    }
}

fn read_line(s: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        if s.read(&mut byte)? == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "eof"));
        }
        if byte[0] == b'\n' {
            if buf.last() == Some(&b'\r') {
                buf.pop();
            }
            return Ok(buf);
        }
        buf.push(byte[0]);
    }
}

fn alive(addr: &str, auth: Option<&str>) -> bool {
    let go = || -> io::Result<bool> {
        let mut s = connect(addr, auth)?;
        send(&mut s, &["PING"])?;
        Ok(read_reply(&mut s)?.eq_ignore_ascii_case("PONG"))
    };
    go().unwrap_or(false)
}

fn info(addr: &str, auth: Option<&str>) -> Option<String> {
    let mut s = connect(addr, auth).ok()?;
    send(&mut s, &["INFO"]).ok()?;
    read_reply(&mut s).ok()
}

fn command(addr: &str, auth: Option<&str>, args: &[&str]) -> io::Result<String> {
    let mut s = connect(addr, auth)?;
    send(&mut s, args)?;
    read_reply(&mut s)
}

// === helpers =================================================================

/// Pull a `field:value` out of an INFO body.
fn field(info: &str, key: &str) -> Option<String> {
    info.lines().find_map(|l| {
        l.strip_prefix(key)
            .and_then(|r| r.strip_prefix(':'))
            .map(|v| v.trim().to_string())
    })
}

/// Split `host:port` on the last colon (IPv6-naive, like the rest of Locus).
fn split_hostport(addr: &str) -> (&str, &str) {
    match addr.rsplit_once(':') {
        Some((h, p)) => (h, p),
        None => (addr, "6379"),
    }
}

fn env_ms(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_info_fields() {
        let body =
            "role:slave\r\nmaster_host:127.0.0.1\r\nmaster_port:6379\r\nmaster_repl_offset:42\r\n";
        assert_eq!(field(body, "role").as_deref(), Some("slave"));
        assert_eq!(field(body, "master_port").as_deref(), Some("6379"));
        assert_eq!(field(body, "master_repl_offset").as_deref(), Some("42"));
        assert_eq!(field(body, "absent"), None);
    }

    #[test]
    fn splits_host_and_port() {
        assert_eq!(split_hostport("127.0.0.1:6379"), ("127.0.0.1", "6379"));
        assert_eq!(split_hostport("localhost"), ("localhost", "6379"));
    }
}
