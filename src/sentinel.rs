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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::log;

const IO_TIMEOUT: Duration = Duration::from_millis(800);

/// The failover decision every sentinel converges on: who the master is and the
/// epoch that decision was made at. Shared between the poll loop and the peer
/// server (which answers `GETMASTER` and accepts `SWITCH`), and persisted so a
/// restart resumes the last known decision instead of reverting to env.
#[derive(Clone)]
struct View {
    master: String,
    epoch: u64,
}

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

    // Cluster nodes to notify on failover: after promoting a replica we broadcast
    // CLUSTER REASSIGN <old> <new> to each so the cluster routes the dead master's
    // slots to its successor (per-shard failover). Empty = plain (non-cluster) HA.
    let cluster_nodes: Vec<String> = std::env::var("LOCUS_SENTINEL_CLUSTER_NODES")
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

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

    // Boot view: the persisted decision, then whatever the live nodes report
    // (a node saying role:master with a higher config epoch wins) — so a
    // restarted sentinel doesn't revert to a stale env master after a failover
    // it wasn't around for. Falls back to the env master at epoch 0.
    let state_path = std::env::var("LOCUS_SENTINEL_STATE").ok();
    let boot = persisted_view(state_path.as_deref()).unwrap_or(View {
        master: master0,
        epoch: 0,
    });
    let view = Arc::new(Mutex::new(derive_view(&nodes, auth.as_deref(), boot)));
    {
        let v = view.lock().unwrap();
        log::info(&format!(
            "sentinel: monitoring master {} (epoch {}) + {} replica(s); down-after {down_after}ms, replica-quorum {quorum}, {} peer sentinel(s)",
            v.master,
            v.epoch,
            nodes.len() - 1,
            peers.len()
        ));
    }
    if let Some(port) = sentinel_port.clone() {
        let flag = master_down.clone();
        let shared = view.clone();
        thread::spawn(move || serve_peers(&port, flag, shared));
    }

    let mut down_since: Option<Instant> = None;
    loop {
        std::thread::sleep(Duration::from_millis(interval));
        let auth = auth.as_deref();
        // Adopt any higher-epoch decision a peer sentinel pushed us via SWITCH.
        adopt_peer_switch(&view, &peers);
        let (master, epoch) = {
            let v = view.lock().unwrap();
            (v.master.clone(), v.epoch)
        };

        if alive(&master, auth) {
            down_since = None;
            master_down.store(false, Ordering::Relaxed);
            reconcile(&nodes, &master, epoch, auth);
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
                } else {
                    // The new epoch is above everything we know (self + every
                    // reachable node + peer sentinel), so the promotion this
                    // failover issues supersedes any concurrent one, and data
                    // nodes reject stale REPLICAOFs against it.
                    let new_epoch = next_epoch(epoch, &nodes, &peers, auth) + 1;
                    if let Some(new_master) = failover(&nodes, &master, new_epoch, auth) {
                        log::info(&format!(
                            "sentinel: +switch-master {master} -> {new_master} (epoch {new_epoch})"
                        ));
                        if !cluster_nodes.is_empty() {
                            reassign_cluster(&cluster_nodes, &master, &new_master, auth);
                        }
                        let updated = View {
                            master: new_master,
                            epoch: new_epoch,
                        };
                        *view.lock().unwrap() = updated.clone();
                        persist_view(state_path.as_deref(), &updated);
                        broadcast_switch(&peers, &updated);
                        down_since = None;
                        master_down.store(false, Ordering::Relaxed);
                    }
                }
            }
        }
    }
}

/// Reconcile the boot view with what the live nodes report: if a reachable node
/// declares itself master at a config epoch >= our known epoch, adopt it. This
/// is how a (re)started sentinel learns about a failover that happened while it
/// was down instead of trusting a stale env/persisted master.
fn derive_view(nodes: &[String], auth: Option<&str>, mut best: View) -> View {
    for n in nodes {
        if let Some(inf) = info(n, auth)
            && field(&inf, "role").as_deref() == Some("master")
        {
            let ep = field(&inf, "config_epoch")
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(0);
            if ep >= best.epoch {
                best = View {
                    master: n.clone(),
                    epoch: ep,
                };
            }
        }
    }
    best
}

/// The epoch a new failover must exceed: the max across our view, every node's
/// config epoch, and every peer sentinel's current epoch.
fn next_epoch(mine: u64, nodes: &[String], peers: &[String], auth: Option<&str>) -> u64 {
    let mut hi = mine;
    for n in nodes {
        if let Some(inf) = info(n, auth) {
            hi = hi.max(
                field(&inf, "config_epoch")
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(0),
            );
        }
    }
    for p in peers {
        if let Some((_, ep)) = peer_getmaster(p) {
            hi = hi.max(ep);
        }
    }
    hi
}

/// Pull any higher-epoch switch-master decision our peers hold into our view
/// (so a sentinel that wasn't the failover leader still converges).
fn adopt_peer_switch(view: &Arc<Mutex<View>>, peers: &[String]) {
    for p in peers {
        if let Some((m, ep)) = peer_getmaster(p) {
            let mut v = view.lock().unwrap();
            if ep > v.epoch {
                log::info(&format!(
                    "sentinel: adopting peer switch-master -> {m} (epoch {ep})"
                ));
                *v = View {
                    master: m,
                    epoch: ep,
                };
            }
        }
    }
}

/// Ensure every node other than `master` is a replica of `master`; repoint any
/// that isn't (a fresh replica, or an old master that just came back). Every
/// REPLICAOF carries our config epoch, so a data node rejects a stale
/// sentinel's repointing (the fence that stops a returned old master from
/// demoting the real one — see the STALEEPOCH guard in the data node).
fn reconcile(nodes: &[String], master: &str, epoch: u64, auth: Option<&str>) {
    let (mh, mp) = split_hostport(master);
    let ep = epoch.to_string();
    for n in nodes.iter().filter(|n| !same_node(n, master)) {
        let Some(inf) = info(n, auth) else { continue }; // unreachable -> skip
        let role = field(&inf, "role");
        let host = field(&inf, "master_host");
        let port = field(&inf, "master_port");
        // Compare resolved addresses, not raw strings: "localhost" and
        // "127.0.0.1" name the same node and must not trigger an endless
        // repoint + full-resync loop every interval.
        let aligned = role.as_deref() == Some("slave")
            && host
                .zip(port)
                .is_some_and(|(h, p)| same_node(&format!("{h}:{p}"), &format!("{mh}:{mp}")));
        if !aligned {
            log::info(&format!(
                "sentinel: repointing {n} -> {master} (epoch {epoch})"
            ));
            let _ = command(n, auth, &["REPLICAOF", mh, mp, "EPOCH", &ep]);
        }
    }
}

/// Promote the most up-to-date reachable replica and repoint the rest at it,
/// carrying the new config epoch through every directive.
fn failover(nodes: &[String], old_master: &str, epoch: u64, auth: Option<&str>) -> Option<String> {
    let mut best: Option<(String, u64)> = None;
    for n in nodes.iter().filter(|n| !same_node(n, old_master)) {
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
    let ep = epoch.to_string();
    log::info(&format!(
        "sentinel: promoting {winner} (offset {off}, epoch {epoch})"
    ));
    command(&winner, auth, &["REPLICAOF", "NO", "ONE", "EPOCH", &ep]).ok()?;
    let (wh, wp) = split_hostport(&winner);
    for n in nodes
        .iter()
        .filter(|n| !same_node(n, &winner) && !same_node(n, old_master))
    {
        let _ = command(n, auth, &["REPLICAOF", wh, wp, "EPOCH", &ep]); // best-effort
    }
    Some(winner)
}

/// Broadcast `CLUSTER REASSIGN <old> <new>` to every cluster node so each repoints
/// the dead master's slots to the promoted replica (best-effort; the dead node and
/// any partitioned node just fail and are reconciled when they return).
fn reassign_cluster(cluster_nodes: &[String], old: &str, new: &str, auth: Option<&str>) {
    for n in cluster_nodes {
        match command(n, auth, &["CLUSTER", "REASSIGN", old, new]) {
            Ok(_) => log::info(&format!("sentinel: reassigned {old} slots -> {new} on {n}")),
            Err(_) => log::warn(&format!("sentinel: REASSIGN on {n} failed (unreachable)")),
        }
    }
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

/// Ask a peer sentinel for its current switch-master view: `(master, epoch)`.
/// None = unreachable or no answer.
fn peer_getmaster(peer: &str) -> Option<(String, u64)> {
    let go = || -> io::Result<Option<(String, u64)>> {
        let mut s = connect(peer, None)?;
        s.write_all(b"GETMASTER\n")?;
        let line = read_line(&mut s)?;
        let text = String::from_utf8_lossy(&line);
        Ok(text.rsplit_once(' ').and_then(|(m, e)| {
            let ep = e.trim().parse::<u64>().ok()?;
            (!m.is_empty()).then(|| (m.to_string(), ep))
        }))
    };
    go().ok().flatten()
}

/// Push our switch-master decision to every peer so they converge even if they
/// weren't the failover leader (best-effort; adopt_peer_switch is the pull side
/// that closes any gap on the next tick).
fn broadcast_switch(peers: &[String], view: &View) {
    for p in peers {
        let go = || -> io::Result<()> {
            let mut s = connect(p, None)?;
            s.write_all(format!("SWITCH {} {}\n", view.master, view.epoch).as_bytes())?;
            let _ = read_line(&mut s);
            Ok(())
        };
        if go().is_err() {
            log::warn(&format!(
                "sentinel: SWITCH to peer {p} failed (unreachable)"
            ));
        }
    }
}

/// Serve peer queries on a line protocol: ISDOWN / PING (down-corroboration),
/// GETMASTER (our current switch-master view), SWITCH (adopt a higher-epoch
/// decision from the failover leader).
fn serve_peers(port: &str, master_down: Arc<AtomicBool>, view: Arc<Mutex<View>>) {
    let listener = match TcpListener::bind(format!("0.0.0.0:{port}")) {
        Ok(l) => l,
        Err(e) => return log::error(&format!("sentinel: peer listener bind failed: {e}")),
    };
    log::info(&format!("sentinel: peer agreement listening on :{port}"));
    for stream in listener.incoming().flatten() {
        let flag = master_down.clone();
        let view = view.clone();
        thread::spawn(move || {
            let mut s = stream;
            let _ = s.set_read_timeout(Some(IO_TIMEOUT));
            let Ok(line) = read_line(&mut s) else { return };
            let reply: Vec<u8> = if line == b"ISDOWN" {
                if flag.load(Ordering::Relaxed) {
                    b"1\n".to_vec()
                } else {
                    b"0\n".to_vec()
                }
            } else if line == b"PING" {
                b"PONG\n".to_vec()
            } else if line == b"GETMASTER" {
                let v = view.lock().unwrap();
                format!("{} {}\n", v.master, v.epoch).into_bytes()
            } else if let Some(rest) = line.strip_prefix(b"SWITCH ") {
                // SWITCH <master> <epoch>: adopt if strictly newer.
                let text = String::from_utf8_lossy(rest);
                if let Some((m, ep)) = text
                    .rsplit_once(' ')
                    .and_then(|(m, e)| Some((m.to_string(), e.trim().parse::<u64>().ok()?)))
                {
                    let mut v = view.lock().unwrap();
                    if ep > v.epoch {
                        *v = View {
                            master: m,
                            epoch: ep,
                        };
                    }
                    b"OK\n".to_vec()
                } else {
                    b"-ERR\n".to_vec()
                }
            } else {
                b"-ERR\n".to_vec()
            };
            let _ = s.write_all(&reply);
        });
    }
}

/// Load a persisted `(master, epoch)` view, if any (survives sentinel restart).
fn persisted_view(path: Option<&str>) -> Option<View> {
    let content = std::fs::read_to_string(path?).ok()?;
    let (m, e) = content.trim().rsplit_once(' ')?;
    Some(View {
        master: m.to_string(),
        epoch: e.trim().parse().ok()?,
    })
}

/// Persist the current switch-master decision so a restart resumes it.
fn persist_view(path: Option<&str>, view: &View) {
    if let Some(p) = path
        && let Err(e) = std::fs::write(p, format!("{} {}\n", view.master, view.epoch))
    {
        log::warn(&format!("sentinel: could not persist view: {e}"));
    }
}

/// Whether two `host:port` strings name the same node, resolving each so
/// "localhost:6379" and "127.0.0.1:6379" compare equal.
fn same_node(a: &str, b: &str) -> bool {
    if a == b {
        return true;
    }
    let resolve = |s: &str| s.to_socket_addrs().ok().and_then(|mut it| it.next());
    match (resolve(a), resolve(b)) {
        (Some(x), Some(y)) => x == y,
        _ => false,
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
