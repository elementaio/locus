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
//! This is deliberately a single-sentinel design (no inter-sentinel quorum yet) —
//! the orchestration-hook tier from the roadmap, not embedded Raft. Pair it with
//! your supervisor for redundancy, or run one per failure domain.
//!
//! Config (env):
//!   LOCUS_SENTINEL            master host:port to monitor (enables sentinel mode)
//!   LOCUS_SENTINEL_REPLICAS   comma-separated replica host:port list
//!   LOCUS_SENTINEL_AUTH       password presented to monitored nodes (optional)
//!   LOCUS_SENTINEL_DOWN_AFTER_MS   master-down grace before failover (default 5000)
//!   LOCUS_SENTINEL_INTERVAL_MS     poll interval (default 1000)

use std::io::{self, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
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
        "sentinel: monitoring master {master} + {} replica(s); down-after {down_after}ms",
        nodes.len() - 1
    ));

    loop {
        std::thread::sleep(Duration::from_millis(interval));
        let auth = auth.as_deref();

        if alive(&master, auth) {
            down_since = None;
            reconcile(&nodes, &master, auth);
        } else {
            let since = *down_since.get_or_insert_with(Instant::now);
            log::warn(&format!("sentinel: master {master} unreachable"));
            if since.elapsed() >= Duration::from_millis(down_after)
                && let Some(new_master) = failover(&nodes, &master, auth)
            {
                log::info(&format!(
                    "sentinel: +switch-master {master} -> {new_master}"
                ));
                master = new_master;
                down_since = None;
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
