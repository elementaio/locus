//! Locus — an in-memory, geo-first datastore that speaks the Redis protocol.
//!
//! Architecture (single-threaded execution, the Redis way):
//!   * each connection has a READER thread (read + parse) and a WRITER thread
//!     (drain an output channel to the socket);
//!   * one owner thread (the "hub") holds the keyspace, the pub/sub registry,
//!     and the replication state; it processes every command serially and routes
//!     replies, published messages, and replicated writes to clients.
//!
//! Milestones: M0 PONG · M1 RESP+SET/GET · M2 concurrency · M3 expiry ·
//! M4 lists/hashes/sets · M5 sorted sets · M6 RDB · M7 AOF · M8 pub/sub ·
//! M9 replication (full sync + command streaming).

mod acl;
mod aof;
mod commands;
mod db;
mod geohash;
mod log;
mod pubsub;
mod rdb;
mod resp;
mod sentinel;
mod sketch;
mod streams;
#[cfg(feature = "tls")]
mod tls;

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use commands::execute_proto;
use db::{Db, Value, now_ms};
use pubsub::PubSub;
use resp::{Parsed, parse_command};

/// Reserved client id for commands replicated from a master.
const MASTER_ID: u64 = 0;

/// Cap on the replication backlog ring (bytes). A replica that fell further behind
/// than this must take a full resync. ~4 MiB mirrors Redis's repl-backlog-size.
const REPL_BACKLOG_MAX: usize = 4 * 1024 * 1024;

/// Returned to a non-loopback client when protected mode is active (no password
/// set). Mirrors Redis's protected-mode guidance.
const PROTECTED_MODE_MSG: &str = "DENIED Locus is running in protected mode because protected mode is enabled and no password is set. To use Locus from a non-loopback address, set a password (LOCUS_REQUIREPASS / requirepass), or disable protected mode with LOCUS_PROTECTED_MODE=no — only on a trusted network or behind TLS.";

enum Msg {
    Connect {
        id: u64,
        out: mpsc::Sender<Vec<u8>>,
        loopback: bool,
    },
    Command {
        id: u64,
        tokens: Vec<Vec<u8>>,
    },
    Disconnect {
        id: u64,
    },
    /// Replica received a full-sync snapshot; replace the whole dataset plus the
    /// CDC / secondary-index state carried in the snapshot trailer.
    ReplaceDb(Box<Db>, Box<rdb::Extras>, String, u64),
    /// The replica's link to its master dropped (clear master_link_status).
    MasterLinkDown,
    /// An async BGREWRITEAOF finished writing its base image off-thread; carries
    /// the temp path on success (to finalize) or an error string.
    AofRewriteDone(Result<String, String>),
}

/// Set by the SIGTERM/SIGINT handler so the hub can persist and exit cleanly.
static SHUTDOWN: AtomicBool = AtomicBool::new(false);

// std has no signal API, so bind the platform libc's signal(2) via FFI. This
// adds NO third-party crate — it's the C runtime the binary already links.
unsafe extern "C" {
    fn signal(signum: i32, handler: extern "C" fn(i32)) -> usize;
}

extern "C" fn on_signal(_sig: i32) {
    // Async-signal-safe: just flip an atomic; the hub does the real work.
    SHUTDOWN.store(true, Ordering::SeqCst);
}

/// Install SIGINT (2) and SIGTERM (15) handlers for graceful shutdown.
fn install_signal_handlers() {
    // SAFETY: the handler only performs an atomic store (async-signal-safe);
    // registering it has no other effects.
    unsafe {
        signal(2, on_signal);
        signal(15, on_signal);
    }
}

fn main() -> io::Result<()> {
    log::init();
    // Sentinel mode: run as a failover monitor instead of a data node (never
    // returns). Enabled by pointing LOCUS_SENTINEL at a master's host:port.
    if std::env::var("LOCUS_SENTINEL").is_ok_and(|v| !v.is_empty()) {
        return sentinel::run();
    }
    let (tx, rx) = mpsc::channel::<Msg>();
    let hub_tx = tx.clone();
    thread::spawn(move || run_hub(rx, hub_tx));
    install_signal_handlers();

    let port = std::env::var("LOCUS_PORT").unwrap_or_else(|_| "6379".to_string());
    // Bind to loopback by default (Locus has no AUTH/TLS — don't expose it by
    // accident). Set LOCUS_BIND=0.0.0.0 to listen on all interfaces, as the
    // Docker image does so a published port is reachable.
    let bind = std::env::var("LOCUS_BIND").unwrap_or_else(|_| "127.0.0.1".to_string());
    let listener = TcpListener::bind(format!("{bind}:{port}"))?;
    // Print the ACTUAL bound address (port may be OS-assigned when LOCUS_PORT=0).
    println!("Locus listening on {}", listener.local_addr()?);

    // Cap concurrent connections (Redis-style maxclients) so a connection flood
    // can't exhaust threads/memory. A per-connection RAII guard decrements the
    // live count when each connection's thread ends.
    let max_clients: usize = std::env::var("LOCUS_MAXCLIENTS")
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(10_000);
    let conns = Arc::new(AtomicUsize::new(0));
    // Connection ids are handed out from a shared counter (the TLS listener, when
    // built in, pulls from the same sequence). 0 is reserved for the master.
    let next_id = Arc::new(AtomicU64::new(1));

    #[cfg(feature = "tls")]
    spawn_tls_listener(&bind, &tx, &conns, &next_id, max_clients);

    for stream in listener.incoming() {
        match stream {
            Ok(mut conn) => {
                if conns.fetch_add(1, Ordering::Relaxed) >= max_clients {
                    conns.fetch_sub(1, Ordering::Relaxed);
                    let _ = conn.write_all(b"-ERR max number of clients reached\r\n");
                    log::warn(&format!(
                        "rejected connection: max clients ({max_clients}) reached"
                    ));
                    continue;
                }
                let id = next_id.fetch_add(1, Ordering::Relaxed);
                let tx = tx.clone();
                let guard = ConnGuard(conns.clone());
                thread::spawn(move || {
                    let _guard = guard;
                    if let Err(e) = handle_conn(conn, id, tx) {
                        log::warn(&format!("connection error: {e}"));
                    }
                });
            }
            Err(e) => log::warn(&format!("accept error: {e}")),
        }
    }
    Ok(())
}

/// Decrements the live-connection counter when a connection's thread ends, so the
/// maxclients cap reflects real disconnects (including early returns / panics).
struct ConnGuard(Arc<AtomicUsize>);
impl Drop for ConnGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Bind the optional TLS listener (LOCUS_TLS_PORT) and accept on a background
/// thread, sharing the connection-id counter and maxclients cap with plaintext.
#[cfg(feature = "tls")]
fn spawn_tls_listener(
    bind: &str,
    tx: &mpsc::Sender<Msg>,
    conns: &Arc<AtomicUsize>,
    next_id: &Arc<AtomicU64>,
    max_clients: usize,
) {
    let port = match std::env::var("LOCUS_TLS_PORT") {
        Ok(p) if !p.trim().is_empty() => p,
        _ => return, // TLS compiled in but not enabled
    };
    let config = match tls::server_config() {
        Ok(c) => c,
        Err(e) => return log::error(&format!("TLS disabled: {e}")),
    };
    let listener = match TcpListener::bind(format!("{bind}:{port}")) {
        Ok(l) => l,
        Err(e) => return log::error(&format!("TLS bind failed: {e}")),
    };
    let addr = listener
        .local_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| format!("{bind}:{port}"));
    println!("Locus TLS listening on {addr}");

    let (tx, conns, next_id) = (tx.clone(), conns.clone(), next_id.clone());
    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(conn) = stream else { continue };
            if conns.fetch_add(1, Ordering::Relaxed) >= max_clients {
                conns.fetch_sub(1, Ordering::Relaxed);
                continue; // can't send a plaintext error on a TLS socket
            }
            let id = next_id.fetch_add(1, Ordering::Relaxed);
            let tx = tx.clone();
            let guard = ConnGuard(conns.clone());
            let config = config.clone();
            thread::spawn(move || {
                let _guard = guard;
                if let Err(e) = tls::handle_tls_conn(conn, id, tx, config) {
                    log::warn(&format!("tls connection error: {e}"));
                }
            });
        }
    });
}

// === the hub ================================================================

struct Hub {
    db: Db,
    aof: Option<aof::Aof>,
    aof_path: Option<String>,
    // Some(buf) while an async BGREWRITEAOF runs: captures writes that land
    // during the rewrite so they can be folded into the new file (no loss).
    aof_rewrite_buf: Option<Vec<u8>>,
    clients: HashMap<u64, mpsc::Sender<Vec<u8>>>,
    pubsub: PubSub,
    // replication
    replicas: HashSet<u64>,           // client ids receiving our write stream
    master: Option<(String, String)>, // (host, port) if we are a replica
    replica_stop: Option<Arc<AtomicBool>>,
    tx: mpsc::Sender<Msg>, // so we can spawn a replica sync thread
    // transactions
    txs: HashMap<u64, TxState>,
    watched_keys: HashMap<Vec<u8>, HashSet<u64>>,
    // blocking XREAD
    blocked: Vec<BlockedReader>,
    // negotiated RESP version per client (2 or 3)
    protos: HashMap<u64, u8>,
    // soft memory cap (bytes); None = unlimited
    maxmemory: Option<usize>,
    // changefeed subscribers: client id -> filter (key prefix or geo region)
    changefeeds: HashMap<u64, CdcFilter>,
    // retained change-log (for CDCREAD catch-up); empty/unused when maxlen == 0
    cdc_log: VecDeque<ChangeRecord>,
    cdc_next_offset: u64,
    cdc_maxlen: usize,
    // changefeed consumer groups (load-balanced read mode), by group name
    cdc_groups: HashMap<Vec<u8>, CdcGroup>,
    // secondary indexes over a hash field, by index name (in-memory)
    indexes: HashMap<Vec<u8>, SecondaryIndex>,
    // authentication: the active shared secret (None = open) and the set of
    // client ids that have authenticated. Checked in the single-threaded hub, so
    // there is no locking and no auth-state race.
    requirepass: Option<Vec<u8>>,
    authed: HashSet<u64>,
    // protected mode: refuse non-loopback clients while no password is set.
    protected_mode: bool,
    loopback: HashSet<u64>, // client ids whose peer is a loopback address
    // password this node presents to its master (replica side); None = none.
    masterauth: Option<Vec<u8>>,
    // true while a background save's write+fsync is running off the hub thread.
    bgsave_in_progress: Arc<AtomicBool>,
    // observability: process start (uptime) + a cheap command counter for INFO.
    start: Instant,
    commands_processed: u64,
    // per-client name set via CLIENT SETNAME (for CLIENT GETNAME / LIST).
    client_names: HashMap<u64, Vec<u8>>,
    // SLOWLOG: a bounded ring of commands slower than the threshold (newest first).
    slowlog: VecDeque<SlowEntry>,
    slowlog_next_id: u64,
    slowlog_threshold_us: i64, // < 0 disables logging
    slowlog_max_len: usize,
    // ACL: named users + each connection's current user (default user is implicit).
    users: HashMap<Vec<u8>, acl::User>,
    current_user: HashMap<u64, Vec<u8>>,
    // replication identity + stream position (bytes streamed to replicas).
    replid: String,
    master_repl_offset: u64,
    master_link_up: bool, // replica side: is the link to our master up?
    // master side: a ring of the recent replication stream so a briefly-dropped
    // replica can PSYNC partial-resync (CONTINUE) instead of a full snapshot.
    // Invariant: repl_backlog_start + repl_backlog.len() == master_repl_offset.
    repl_backlog: VecDeque<u8>,
    repl_backlog_start: u64,
    repl_active: bool, // a replica has attached -> keep offset+backlog advancing through gaps
    // master side: each replica's last-acked offset + clients parked on WAIT.
    replica_acks: HashMap<u64, u64>,
    waiting: Vec<WaitReq>,
}

/// One slow-log entry.
struct SlowEntry {
    id: u64,
    time_secs: u64,
    micros: u64,
    args: Vec<Vec<u8>>,
}

/// A secondary index over one hash field: a sorted field-value → keys map, plus
/// a reverse key → indexed-value map so a key can be re-indexed in place. Kept
/// in sync with every write in the same hub turn (no drift, no crash-time GC).
struct SecondaryIndex {
    field: Vec<u8>,
    forward: BTreeMap<Vec<u8>, HashSet<Vec<u8>>>,
    reverse: HashMap<Vec<u8>, Vec<u8>>,
}

/// One retained keyspace change, addressable by a monotonic offset.
struct ChangeRecord {
    offset: u64,
    event: Vec<u8>, // "write" | "del" | "expire"
    key: Vec<u8>,
    value: Option<Vec<u8>>, // new value for string writes; None otherwise
}

/// What a changefeed subscriber wants: keys under a prefix, or geo keys inside a
/// circular region (live geofencing). A region tracks its current members so it
/// can emit enter (`write`) and leave (`del`) transitions.
enum CdcFilter {
    Prefix(Vec<u8>),
    Region {
        lon: f64,
        lat: f64,
        radius_m: f64,
        members: HashSet<Vec<u8>>,
    },
}

/// A changefeed consumer group: a shared cursor over the log plus a pending list
/// (delivered-but-unacked offsets → the consumer that got them). In-memory only.
#[derive(Default)]
struct CdcGroup {
    last_delivered: u64,
    pending: HashMap<u64, Vec<u8>>,
}

/// A client parked on a blocking XREAD.
struct BlockedReader {
    id: u64,
    specs: Vec<(Vec<u8>, db::StreamId)>,
    count: Option<usize>,
    deadline: Option<u64>, // None = block forever
}

/// A client parked on WAIT until enough replicas ack `target` (or it times out).
struct WaitReq {
    id: u64,
    target: u64,
    numreplicas: usize,
    deadline: Option<u64>, // None = block forever (WAIT ... 0)
}

/// Per-client transaction state (MULTI/EXEC/WATCH).
#[derive(Default)]
struct TxState {
    in_multi: bool,
    queued: Vec<Vec<Vec<u8>>>,
    watched: Vec<Vec<u8>>,
    dirty: bool,   // a watched key changed -> EXEC aborts (nil)
    aborted: bool, // a queued command was invalid -> EXEC aborts (EXECABORT)
}

impl Hub {
    fn new(tx: mpsc::Sender<Msg>) -> Hub {
        let aof_path = aof::configured_path();
        let (db, aof, extras) = match &aof_path {
            Some(path) => {
                let db = aof::load(path).unwrap_or_else(|e| {
                    log::warn(&format!("AOF load failed: {e} — starting empty"));
                    Db::new()
                });
                let aof = aof::Aof::open(path)
                    .map_err(|e| log::warn(&format!("AOF open failed: {e}")))
                    .ok();
                // CDC/index state lives in the RDB trailer even in AOF mode
                // (SAVE/BGSAVE write it); the keyspace itself comes from the AOF.
                let extras = rdb::load_with_extras(&rdb::configured_path())
                    .map(|(_, x)| x)
                    .unwrap_or_default();
                (db, aof, extras)
            }
            None => {
                let p = rdb::configured_path();
                let (db, extras) = rdb::load_with_extras(&p).unwrap_or_else(|e| {
                    log::warn(&format!("RDB load failed: {e} — starting empty"));
                    (Db::new(), rdb::Extras::default())
                });
                (db, None, extras)
            }
        };
        let mut hub = Hub {
            db,
            aof,
            aof_path,
            aof_rewrite_buf: None,
            clients: HashMap::new(),
            pubsub: PubSub::new(),
            replicas: HashSet::new(),
            master: None,
            replica_stop: None,
            tx,
            txs: HashMap::new(),
            watched_keys: HashMap::new(),
            blocked: Vec::new(),
            protos: HashMap::new(),
            maxmemory: std::env::var("LOCUS_MAXMEMORY")
                .ok()
                .and_then(|s| parse_mem(&s))
                .filter(|&m| m > 0),
            changefeeds: HashMap::new(),
            cdc_log: VecDeque::new(),
            cdc_next_offset: 1, // offset 0 means "nothing yet"
            cdc_maxlen: std::env::var("LOCUS_CDC_MAXLEN")
                .ok()
                .and_then(|s| s.trim().parse::<usize>().ok())
                .unwrap_or(0),
            cdc_groups: HashMap::new(),
            indexes: HashMap::new(),
            requirepass: std::env::var("LOCUS_REQUIREPASS")
                .ok()
                .map(String::into_bytes)
                .filter(|p| !p.is_empty()),
            authed: HashSet::new(),
            protected_mode: std::env::var("LOCUS_PROTECTED_MODE")
                .map(|v| {
                    !matches!(
                        v.trim().to_ascii_lowercase().as_str(),
                        "no" | "false" | "0" | "off"
                    )
                })
                .unwrap_or(true),
            loopback: HashSet::new(),
            masterauth: std::env::var("LOCUS_MASTERAUTH")
                .ok()
                .map(String::into_bytes)
                .filter(|p| !p.is_empty()),
            bgsave_in_progress: Arc::new(AtomicBool::new(false)),
            start: Instant::now(),
            commands_processed: 0,
            client_names: HashMap::new(),
            slowlog: VecDeque::new(),
            slowlog_next_id: 0,
            slowlog_threshold_us: std::env::var("LOCUS_SLOWLOG_US")
                .ok()
                .and_then(|s| s.trim().parse().ok())
                .unwrap_or(10_000),
            slowlog_max_len: std::env::var("LOCUS_SLOWLOG_MAXLEN")
                .ok()
                .and_then(|s| s.trim().parse().ok())
                .unwrap_or(128),
            users: HashMap::new(),
            current_user: HashMap::new(),
            replid: acl::hex32(&acl::sha256(
                format!("{}-{}", std::process::id(), now_ms()).as_bytes(),
            ))[..40]
                .to_string(),
            master_repl_offset: 0,
            master_link_up: false,
            repl_backlog: VecDeque::new(),
            repl_backlog_start: 0,
            repl_active: false,
            replica_acks: HashMap::new(),
            waiting: Vec::new(),
        };
        hub.apply_extras(extras);
        hub
    }

    /// Restore CDC + secondary-index state loaded from a snapshot trailer.
    fn apply_extras(&mut self, x: rdb::Extras) {
        self.cdc_next_offset = x.cdc_next_offset.max(self.cdc_next_offset);
        self.cdc_log = x
            .cdc_log
            .into_iter()
            .map(|r| ChangeRecord {
                offset: r.offset,
                event: r.event,
                key: r.key,
                value: r.value,
            })
            .collect();
        self.cdc_groups = x
            .cdc_groups
            .into_iter()
            .map(|g| {
                (
                    g.name,
                    CdcGroup {
                        last_delivered: g.last_delivered,
                        pending: g.pending.into_iter().collect(),
                    },
                )
            })
            .collect();
        for (name, field) in x.index_defs {
            self.add_index(name, field);
        }
    }

    /// Snapshot the CDC + secondary-index state for persistence.
    fn build_extras(&self) -> rdb::Extras {
        rdb::Extras {
            cdc_next_offset: self.cdc_next_offset,
            cdc_log: self
                .cdc_log
                .iter()
                .map(|r| rdb::CdcRec {
                    offset: r.offset,
                    event: r.event.clone(),
                    key: r.key.clone(),
                    value: r.value.clone(),
                })
                .collect(),
            cdc_groups: self
                .cdc_groups
                .iter()
                .map(|(name, g)| rdb::CdcGrp {
                    name: name.clone(),
                    last_delivered: g.last_delivered,
                    pending: g.pending.iter().map(|(o, c)| (*o, c.clone())).collect(),
                })
                .collect(),
            index_defs: self
                .indexes
                .iter()
                .map(|(name, ix)| (name.clone(), ix.field.clone()))
                .collect(),
        }
    }

    /// Create a secondary index over `field`, populated from the current
    /// keyspace. Used by IDXCREATE and by snapshot restore.
    fn add_index(&mut self, name: Vec<u8>, field: Vec<u8>) {
        let mut ix = SecondaryIndex {
            field: field.clone(),
            forward: BTreeMap::new(),
            reverse: HashMap::new(),
        };
        for k in self.db.live_keys() {
            if let Some(Value::Hash(h)) = self.db.get(&k)
                && let Some(v) = h.get(&field)
            {
                let v = v.clone();
                ix.forward.entry(v.clone()).or_default().insert(k.clone());
                ix.reverse.insert(k, v);
            }
        }
        self.indexes.insert(name, ix);
    }

    fn send(&self, id: u64, bytes: Vec<u8>) {
        if let Some(out) = self.clients.get(&id) {
            let _ = out.send(bytes);
        }
    }

    /// Flush the AOF and (unless NOSAVE) write a final snapshot, then exit 0.
    /// Used by SIGTERM/SIGINT and the SHUTDOWN command so a restart loses nothing
    /// beyond the configured fsync window.
    fn persist_and_exit(&mut self, save: bool) -> ! {
        if let Some(a) = self.aof.as_mut() {
            a.fsync();
        }
        if save {
            let extras = self.build_extras();
            if let Err(e) = rdb::save_with_extras(&self.db, &extras, &rdb::configured_path()) {
                log::error(&format!("shutdown: final save failed: {e}"));
            }
        }
        log::info("shutting down");
        std::process::exit(0);
    }

    /// Runtime config as (name, value) pairs, sourced from live hub state and the
    /// env knobs. Read by CONFIG GET and INFO.
    fn config_params(&self) -> Vec<(&'static str, String)> {
        let policy = if self.maxmemory.is_some() {
            "allkeys-random" // Locus evicts arbitrary keys
        } else {
            "noeviction"
        };
        let appendfsync = std::env::var("LOCUS_APPENDFSYNC")
            .ok()
            .map(|s| s.trim().to_ascii_lowercase())
            .filter(|s| matches!(s.as_str(), "always" | "no" | "everysec"))
            .unwrap_or_else(|| "everysec".into());
        vec![
            ("maxmemory", self.maxmemory.unwrap_or(0).to_string()),
            ("maxmemory-policy", policy.to_string()),
            (
                "appendonly",
                if self.aof.is_some() { "yes" } else { "no" }.to_string(),
            ),
            ("appendfsync", appendfsync),
            ("save", String::new()),
            (
                "maxclients",
                std::env::var("LOCUS_MAXCLIENTS").unwrap_or_else(|_| "10000".into()),
            ),
            (
                "timeout",
                std::env::var("LOCUS_TIMEOUT").unwrap_or_else(|_| "0".into()),
            ),
            (
                "requirepass",
                self.requirepass
                    .as_ref()
                    .map(|p| String::from_utf8_lossy(p).into_owned())
                    .unwrap_or_default(),
            ),
            (
                "bind",
                std::env::var("LOCUS_BIND").unwrap_or_else(|_| "127.0.0.1".into()),
            ),
            (
                "port",
                std::env::var("LOCUS_PORT").unwrap_or_else(|_| "6379".into()),
            ),
        ]
    }

    /// CLUSTER introspection for a standalone node. We report cluster_enabled:0 so
    /// cluster-aware clients fall back to standalone, but answer KEYSLOT/MYID/SLOTS
    /// usefully. This is the routing seam (the slot model) that P6 builds on.
    fn handle_cluster(&mut self, id: u64, tokens: &[Vec<u8>]) {
        match tokens.get(1).map(|t| t.to_ascii_uppercase()).as_deref() {
            Some(b"INFO") => {
                let body = "cluster_enabled:0\r\ncluster_state:ok\r\n\
                    cluster_slots_assigned:0\r\ncluster_slots_ok:0\r\n\
                    cluster_slots_pfail:0\r\ncluster_slots_fail:0\r\n\
                    cluster_known_nodes:1\r\ncluster_size:0\r\n\
                    cluster_current_epoch:0\r\ncluster_my_epoch:0\r\n\
                    cluster_stats_messages_sent:0\r\ncluster_stats_messages_received:0\r\n";
                self.send(id, resp::bulk_string(body.as_bytes()));
            }
            Some(b"MYID") => {
                let myid = self.replid.clone();
                self.send(id, resp::bulk_string(myid.as_bytes()));
            }
            // Not clustered: no slots/shards assigned.
            Some(b"SLOTS") | Some(b"SHARDS") | Some(b"LINKS") => self.send(id, resp::array(&[])),
            Some(b"NODES") => {
                // One line for ourselves (myself,master), no slots — matches the
                // shape cluster-aware clients parse.
                let line = format!(
                    "{} 127.0.0.1:0@0 myself,master - 0 0 0 connected\n",
                    self.replid
                );
                self.send(id, resp::bulk_string(line.as_bytes()));
            }
            Some(b"KEYSLOT") if tokens.len() == 3 => {
                let slot = commands::hash_slot(&tokens[2]) as i64;
                self.send(id, resp::integer(slot));
            }
            Some(b"COUNTKEYSINSLOT") => self.send(id, resp::integer(0)),
            Some(b"RESET") => self.send(id, resp::simple_string("OK")),
            _ => self.send(
                id,
                resp::error("ERR Unknown CLUSTER subcommand or wrong number of arguments"),
            ),
        }
    }

    fn handle_config(&mut self, id: u64, tokens: &[Vec<u8>]) {
        match tokens.get(1).map(|t| t.to_ascii_uppercase()).as_deref() {
            Some(b"GET") if tokens.len() >= 3 => {
                let params = self.config_params();
                let mut out: Vec<Vec<u8>> = Vec::new();
                for (name, val) in &params {
                    if tokens[2..]
                        .iter()
                        .any(|pat| pubsub::glob_match(pat, name.as_bytes()))
                    {
                        out.push(name.as_bytes().to_vec());
                        out.push(val.clone().into_bytes());
                    }
                }
                let proto = self.protos.get(&id).copied().unwrap_or(2);
                self.send(id, resp::map(&out, proto));
            }
            Some(b"SET") if tokens.len() >= 4 => {
                let param = tokens[2].to_ascii_lowercase();
                match param.as_slice() {
                    b"maxmemory" => {
                        self.maxmemory =
                            parse_mem(&String::from_utf8_lossy(&tokens[3])).filter(|&m| m > 0);
                    }
                    b"requirepass" => {
                        // Rotation: new auth attempts use the new secret; existing
                        // authenticated sessions keep their access (Redis semantics).
                        self.requirepass = if tokens[3].is_empty() {
                            None
                        } else {
                            Some(tokens[3].clone())
                        };
                    }
                    // Accept (and no-op) other known params so clients don't error.
                    _ => {}
                }
                self.send(id, resp::simple_string("OK"));
            }
            Some(b"RESETSTAT") => {
                self.commands_processed = 0;
                self.send(id, resp::simple_string("OK"));
            }
            Some(b"REWRITE") => self.send(id, resp::simple_string("OK")),
            _ => self.send(
                id,
                resp::error("ERR Unknown CONFIG subcommand or wrong number of arguments"),
            ),
        }
    }

    fn handle_client(&mut self, id: u64, tokens: &[Vec<u8>]) {
        match tokens.get(1).map(|t| t.to_ascii_uppercase()).as_deref() {
            Some(b"ID") => self.send(id, resp::integer(id as i64)),
            Some(b"SETNAME") if tokens.len() == 3 => {
                self.client_names.insert(id, tokens[2].clone());
                self.send(id, resp::simple_string("OK"));
            }
            Some(b"GETNAME") => {
                let name = self.client_names.get(&id).cloned().unwrap_or_default();
                self.send(id, resp::bulk_string(&name));
            }
            // Drivers send CLIENT SETINFO lib-name/lib-ver on connect; accept it.
            Some(b"SETINFO") => self.send(id, resp::simple_string("OK")),
            Some(b"NO-EVICT") | Some(b"NO-TOUCH") => self.send(id, resp::simple_string("OK")),
            Some(b"LIST") => {
                let mut s = String::new();
                for &cid in self.clients.keys() {
                    let name = self
                        .client_names
                        .get(&cid)
                        .map(|n| String::from_utf8_lossy(n).into_owned())
                        .unwrap_or_default();
                    s.push_str(&format!("id={cid} name={name} db=0\n"));
                }
                self.send(id, resp::bulk_string(s.as_bytes()));
            }
            _ => self.send(
                id,
                resp::error("ERR Unknown CLIENT subcommand or wrong number of arguments"),
            ),
        }
    }

    fn push_slowlog(&mut self, micros: u64, args: Vec<Vec<u8>>) {
        let entry = SlowEntry {
            id: self.slowlog_next_id,
            time_secs: now_ms() / 1000,
            micros,
            args,
        };
        self.slowlog_next_id += 1;
        self.slowlog.push_front(entry);
        while self.slowlog.len() > self.slowlog_max_len {
            self.slowlog.pop_back();
        }
    }

    fn handle_slowlog(&mut self, id: u64, tokens: &[Vec<u8>]) {
        match tokens.get(1).map(|t| t.to_ascii_uppercase()).as_deref() {
            Some(b"GET") => {
                let n = tokens
                    .get(2)
                    .and_then(|t| std::str::from_utf8(t).ok())
                    .and_then(|s| s.parse::<i64>().ok());
                let take = match n {
                    Some(x) if x >= 0 => (x as usize).min(self.slowlog.len()),
                    _ => self.slowlog.len(),
                };
                let mut reply = format!("*{take}\r\n").into_bytes();
                for e in self.slowlog.iter().take(take) {
                    // [id, timestamp, exec_micros, [args], client_addr, client_name]
                    reply.extend_from_slice(b"*6\r\n");
                    reply.extend_from_slice(&resp::integer(e.id as i64));
                    reply.extend_from_slice(&resp::integer(e.time_secs as i64));
                    reply.extend_from_slice(&resp::integer(e.micros as i64));
                    reply.extend_from_slice(&resp::bulk_array(&e.args));
                    reply.extend_from_slice(&resp::bulk_string(b""));
                    reply.extend_from_slice(&resp::bulk_string(b""));
                }
                self.send(id, reply);
            }
            Some(b"LEN") => self.send(id, resp::integer(self.slowlog.len() as i64)),
            Some(b"RESET") => {
                self.slowlog.clear();
                self.send(id, resp::simple_string("OK"));
            }
            Some(b"HELP") => self.send(
                id,
                resp::simple_string("SLOWLOG GET [count] | LEN | RESET | HELP"),
            ),
            _ => self.send(
                id,
                resp::error("ERR Unknown SLOWLOG subcommand or wrong number of arguments"),
            ),
        }
    }

    fn handle_acl(&mut self, id: u64, tokens: &[Vec<u8>]) {
        match tokens.get(1).map(|t| t.to_ascii_uppercase()).as_deref() {
            Some(b"SETUSER") if tokens.len() >= 3 => {
                let name = tokens[2].clone();
                let mut user = self.users.get(&name).cloned().unwrap_or_default();
                for rule in &tokens[3..] {
                    if user.apply(rule).is_err() {
                        return self.send(
                            id,
                            resp::error(&format!(
                                "ERR Error in ACL SETUSER modifier '{}'",
                                String::from_utf8_lossy(rule)
                            )),
                        );
                    }
                }
                self.users.insert(name, user);
                self.send(id, resp::simple_string("OK"));
            }
            Some(b"GETUSER") if tokens.len() == 3 => match self.users.get(&tokens[2]) {
                Some(u) => self.send(id, resp::bulk_array(&u.describe())),
                None => self.send(id, resp::null_array()),
            },
            Some(b"DELUSER") if tokens.len() >= 3 => {
                let mut n = 0;
                for name in &tokens[2..] {
                    if name.as_slice() != b"default" && self.users.remove(name).is_some() {
                        n += 1;
                    }
                }
                self.send(id, resp::integer(n));
            }
            Some(b"LIST") => {
                let mut out: Vec<Vec<u8>> = Vec::new();
                for (name, u) in &self.users {
                    out.push(
                        format!(
                            "user {} {}",
                            String::from_utf8_lossy(name),
                            if u.enabled { "on" } else { "off" }
                        )
                        .into_bytes(),
                    );
                }
                self.send(id, resp::bulk_array(&out));
            }
            Some(b"USERS") => {
                let mut out: Vec<Vec<u8>> = self.users.keys().cloned().collect();
                out.push(b"default".to_vec());
                self.send(id, resp::bulk_array(&out));
            }
            Some(b"WHOAMI") => {
                let name = self
                    .current_user
                    .get(&id)
                    .cloned()
                    .unwrap_or_else(|| b"default".to_vec());
                self.send(id, resp::bulk_string(&name));
            }
            Some(b"CAT") => {
                let cats: Vec<Vec<u8>> = ["read", "write", "admin", "connection", "pubsub"]
                    .iter()
                    .map(|c| c.as_bytes().to_vec())
                    .collect();
                self.send(id, resp::bulk_array(&cats));
            }
            _ => self.send(
                id,
                resp::error("ERR Unknown ACL subcommand or wrong number of arguments"),
            ),
        }
    }

    /// Build the INFO report (the sections redis_exporter and clients expect).
    fn render_info(&self) -> Vec<u8> {
        let used = self.db.mem_used();
        let role = if self.master.is_some() {
            "slave"
        } else {
            "master"
        };
        let ver = env!("CARGO_PKG_VERSION");
        let mut s = String::new();
        s.push_str("# Server\r\n");
        s.push_str(&format!("redis_version:{ver}\r\n"));
        s.push_str("redis_mode:standalone\r\n");
        s.push_str(&format!("locus_version:{ver}\r\n"));
        s.push_str(&format!(
            "os:{} {}\r\n",
            std::env::consts::OS,
            std::env::consts::ARCH
        ));
        s.push_str(&format!("process_id:{}\r\n", std::process::id()));
        s.push_str(&format!(
            "tcp_port:{}\r\n",
            std::env::var("LOCUS_PORT").unwrap_or_else(|_| "6379".into())
        ));
        s.push_str(&format!(
            "uptime_in_seconds:{}\r\n",
            self.start.elapsed().as_secs()
        ));
        s.push_str("# Clients\r\n");
        s.push_str(&format!("connected_clients:{}\r\n", self.clients.len()));
        s.push_str(&format!("blocked_clients:{}\r\n", self.blocked.len()));
        s.push_str("# Memory\r\n");
        s.push_str(&format!("used_memory:{used}\r\n"));
        s.push_str(&format!(
            "used_memory_human:{:.2}M\r\n",
            used as f64 / (1024.0 * 1024.0)
        ));
        s.push_str(&format!("maxmemory:{}\r\n", self.maxmemory.unwrap_or(0)));
        s.push_str(&format!(
            "maxmemory_policy:{}\r\n",
            if self.maxmemory.is_some() {
                "allkeys-random"
            } else {
                "noeviction"
            }
        ));
        s.push_str("mem_fragmentation_ratio:1.00\r\n");
        s.push_str("# Persistence\r\n");
        s.push_str("loading:0\r\n");
        s.push_str(&format!(
            "aof_enabled:{}\r\n",
            if self.aof.is_some() { 1 } else { 0 }
        ));
        s.push_str(&format!(
            "rdb_bgsave_in_progress:{}\r\n",
            self.bgsave_in_progress.load(Ordering::Relaxed) as u8
        ));
        s.push_str("aof_last_write_status:ok\r\n");
        s.push_str("rdb_last_bgsave_status:ok\r\n");
        s.push_str("# Stats\r\n");
        s.push_str(&format!(
            "total_commands_processed:{}\r\n",
            self.commands_processed
        ));
        s.push_str("# Replication\r\n");
        s.push_str(&format!("role:{role}\r\n"));
        s.push_str(&format!("connected_slaves:{}\r\n", self.replicas.len()));
        s.push_str(&format!("master_replid:{}\r\n", self.replid));
        s.push_str(&format!(
            "master_repl_offset:{}\r\n",
            self.master_repl_offset
        ));
        if let Some((h, p)) = &self.master {
            let link = if self.master_link_up { "up" } else { "down" };
            s.push_str(&format!(
                "master_host:{h}\r\nmaster_port:{p}\r\nmaster_link_status:{link}\r\n"
            ));
        }
        s.push_str("# Keyspace\r\n");
        let keys = self.db.dbsize();
        if keys > 0 {
            s.push_str(&format!("db0:keys={keys},expires=0,avg_ttl=0\r\n"));
        }
        resp::bulk_string(s.as_bytes())
    }

    fn handle_command(&mut self, id: u64, tokens: Vec<Vec<u8>>) {
        if tokens.is_empty() {
            return;
        }
        let cmd = tokens[0].to_ascii_uppercase();
        self.commands_processed = self.commands_processed.wrapping_add(1);
        // On a replica, applying the master's stream advances our offset by the
        // same bytes the master counted (canonical RESP encoding is identical).
        if id == MASTER_ID {
            self.master_repl_offset += resp::command(&tokens).len() as u64;
        }

        // Protected mode: if no password is set, refuse non-loopback clients so an
        // accidentally-exposed instance (e.g. the 0.0.0.0 Docker image) isn't wide
        // open. Setting a password — or LOCUS_PROTECTED_MODE=no — lifts this. QUIT
        // is allowed so a remote client can disconnect cleanly.
        if self.protected_mode
            && self.requirepass.is_none()
            && id != MASTER_ID
            && !self.loopback.contains(&id)
            && cmd.as_slice() != b"QUIT"
        {
            return self.send(id, resp::error(PROTECTED_MODE_MSG));
        }

        // Authentication gate: when a password is set, a connection must AUTH
        // before running anything but the connection-setup commands. The master
        // replication stream (id 0) is internally trusted.
        if self.requirepass.is_some()
            && id != MASTER_ID
            && !self.authed.contains(&id)
            && !is_no_auth(&cmd)
        {
            return self.send(id, resp::error("NOAUTH Authentication required."));
        }

        // ACL: a named user is restricted to its allowed command classes + key
        // prefix. The implicit "default" user (and open mode) has no entry here
        // and stays unrestricted.
        if id != MASTER_ID
            && let Some(uname) = self.current_user.get(&id)
            && let Some(user) = self.users.get(uname)
        {
            let class = commands::command_class(&cmd);
            if !user.allows_class(class) {
                return self.send(
                    id,
                    resp::error(&format!(
                        "NOPERM User {} has no permissions to run the '{}' command",
                        String::from_utf8_lossy(uname),
                        String::from_utf8_lossy(&cmd).to_ascii_lowercase()
                    )),
                );
            }
            if (class == acl::CLASS_READ || class == acl::CLASS_WRITE)
                && tokens.len() >= 2
                && !user.allows_key(&tokens[1])
            {
                return self.send(id, resp::error("NOPERM No permissions to access a key"));
            }
        }

        // A connection in pub/sub or changefeed "push mode" may only run the
        // push-control commands (plus PING/QUIT/RESET) — replies must not
        // interleave with pushed messages.
        let in_push_mode = self.pubsub.total(id) > 0 || self.changefeeds.contains_key(&id);
        if in_push_mode && !allowed_in_push_mode(&cmd) {
            return self.send(
                id,
                resp::error(&format!(
                    "ERR Can't execute '{}': only (P)SUBSCRIBE / (P)UNSUBSCRIBE / CDC(UN)SUBSCRIBE / PING / QUIT / RESET are allowed in this context",
                    String::from_utf8_lossy(&cmd).to_ascii_lowercase()
                )),
            );
        }

        // In MULTI, queue everything except transaction-control commands.
        // Validate at queue time (Redis semantics): an unknown command or one
        // with too few arguments is rejected now and flags the transaction so
        // EXEC fails with EXECABORT instead of running a half-valid batch.
        if self.txs.get(&id).is_some_and(|t| t.in_multi) && !is_tx_control(&cmd) {
            match commands::min_arity(&cmd) {
                None => {
                    self.txs.get_mut(&id).unwrap().aborted = true;
                    return self.send(
                        id,
                        resp::error(&format!(
                            "ERR unknown command '{}'",
                            String::from_utf8_lossy(&tokens[0])
                        )),
                    );
                }
                Some(min) if tokens.len() < min => {
                    self.txs.get_mut(&id).unwrap().aborted = true;
                    return self.send(
                        id,
                        resp::error(&format!(
                            "ERR wrong number of arguments for '{}' command",
                            String::from_utf8_lossy(&cmd).to_ascii_lowercase()
                        )),
                    );
                }
                _ => {}
            }
            self.txs.get_mut(&id).unwrap().queued.push(tokens);
            return self.send(id, resp::simple_string("QUEUED"));
        }

        match cmd.as_slice() {
            // --- transactions ---
            b"MULTI" => {
                let tx = self.txs.entry(id).or_default();
                if tx.in_multi {
                    return self.send(id, resp::error("ERR MULTI calls can not be nested"));
                }
                tx.in_multi = true;
                self.send(id, resp::simple_string("OK"));
            }
            b"DISCARD" => {
                if !self.txs.get(&id).is_some_and(|t| t.in_multi) {
                    return self.send(id, resp::error("ERR DISCARD without MULTI"));
                }
                let t = self.txs.remove(&id).unwrap();
                self.unwatch_keys(&t.watched, id);
                self.send(id, resp::simple_string("OK"));
            }
            b"EXEC" => {
                if !self.txs.get(&id).is_some_and(|t| t.in_multi) {
                    return self.send(id, resp::error("ERR EXEC without MULTI"));
                }
                let t = self.txs.remove(&id).unwrap();
                self.unwatch_keys(&t.watched, id);
                if t.aborted {
                    self.send(
                        id,
                        resp::error("EXECABORT Transaction discarded because of previous errors."),
                    );
                } else if t.dirty {
                    self.send(id, resp::null_array()); // a watched key changed -> abort
                } else {
                    let mut replies = Vec::with_capacity(t.queued.len());
                    for q in t.queued {
                        replies.push(self.exec_one(id, q));
                    }
                    self.send(id, resp::array(&replies));
                }
            }
            b"WATCH" => {
                if tokens.len() < 2 {
                    return self.send(
                        id,
                        resp::error("ERR wrong number of arguments for 'watch' command"),
                    );
                }
                if self.txs.get(&id).is_some_and(|t| t.in_multi) {
                    return self.send(id, resp::error("ERR WATCH inside MULTI is not allowed"));
                }
                {
                    let tx = self.txs.entry(id).or_default();
                    for key in &tokens[1..] {
                        tx.watched.push(key.clone());
                    }
                }
                for key in &tokens[1..] {
                    self.watched_keys.entry(key.clone()).or_default().insert(id);
                }
                self.send(id, resp::simple_string("OK"));
            }
            b"UNWATCH" => {
                let watched: Vec<Vec<u8>> = self
                    .txs
                    .get_mut(&id)
                    .map(|t| {
                        t.dirty = false;
                        std::mem::take(&mut t.watched)
                    })
                    .unwrap_or_default();
                self.unwatch_keys(&watched, id);
                self.send(id, resp::simple_string("OK"));
            }
            b"RESET" => {
                // Abort any transaction and release its watches, exit subscribe
                // mode, and drop back to RESP2 — a clean per-connection reset.
                if let Some(t) = self.txs.remove(&id) {
                    self.unwatch_keys(&t.watched, id);
                }
                self.pubsub.remove_client(id);
                self.changefeeds.remove(&id);
                self.protos.insert(id, 2);
                self.send(id, resp::simple_string("RESET"));
            }
            // --- pub/sub ---
            b"SUBSCRIBE" => {
                if tokens.len() < 2 {
                    return self.send(
                        id,
                        resp::error("ERR wrong number of arguments for 'subscribe' command"),
                    );
                }
                let proto = self.protos.get(&id).copied().unwrap_or(2);
                for ch in &tokens[1..] {
                    let c = self.pubsub.subscribe(id, ch);
                    self.send(id, pubsub::subscribe_reply(ch, c, proto));
                }
            }
            b"PSUBSCRIBE" => {
                if tokens.len() < 2 {
                    return self.send(
                        id,
                        resp::error("ERR wrong number of arguments for 'psubscribe' command"),
                    );
                }
                let proto = self.protos.get(&id).copied().unwrap_or(2);
                for pat in &tokens[1..] {
                    let c = self.pubsub.psubscribe(id, pat);
                    self.send(id, pubsub::psubscribe_reply(pat, c, proto));
                }
            }
            b"UNSUBSCRIBE" => {
                let chans = if tokens.len() > 1 {
                    tokens[1..].to_vec()
                } else {
                    self.pubsub.channels_of(id)
                };
                let proto = self.protos.get(&id).copied().unwrap_or(2);
                if chans.is_empty() {
                    self.send(id, pubsub::unsubscribe_reply(None, 0, proto));
                } else {
                    for ch in chans {
                        let c = self.pubsub.unsubscribe(id, &ch);
                        self.send(id, pubsub::unsubscribe_reply(Some(&ch), c, proto));
                    }
                }
            }
            b"PUNSUBSCRIBE" => {
                let pats = if tokens.len() > 1 {
                    tokens[1..].to_vec()
                } else {
                    self.pubsub.patterns_of(id)
                };
                let proto = self.protos.get(&id).copied().unwrap_or(2);
                if pats.is_empty() {
                    self.send(id, pubsub::punsubscribe_reply(None, 0, proto));
                } else {
                    for pat in pats {
                        let c = self.pubsub.punsubscribe(id, &pat);
                        self.send(id, pubsub::punsubscribe_reply(Some(&pat), c, proto));
                    }
                }
            }
            b"PUBLISH" => {
                if tokens.len() != 3 {
                    return self.send(
                        id,
                        resp::error("ERR wrong number of arguments for 'publish' command"),
                    );
                }
                let n = self
                    .pubsub
                    .publish(&tokens[1], &tokens[2], &self.clients, &self.protos);
                self.send(id, resp::integer(n));
            }
            b"PUBSUB" => self.handle_pubsub_introspect(id, &tokens),
            b"CDCSUBSCRIBE" => self.handle_cdc_subscribe(id, &tokens),
            b"CDCUNSUBSCRIBE" => self.handle_cdc_unsubscribe(id),
            b"CDCREAD" => self.handle_cdc_read(id, &tokens),
            b"IDXCREATE" => self.handle_idx_admin(id, &tokens, true),
            b"IDXDROP" => self.handle_idx_admin(id, &tokens, false),
            b"IDXGET" => self.handle_idx_get(id, &tokens),
            b"IDXRANGE" => self.handle_idx_range(id, &tokens),
            b"CDCGROUP" => self.handle_cdc_group(id, &tokens),
            b"CDCREADGROUP" => self.handle_cdc_readgroup(id, &tokens),
            b"CDCACK" => self.handle_cdc_ack(id, &tokens),
            b"CDCPENDING" => self.handle_cdc_pending(id, &tokens),
            b"XREAD" => self.handle_xread(id, &tokens),
            b"HELLO" => self.handle_hello(id, &tokens),
            b"AUTH" => self.handle_auth(id, &tokens),

            // --- replication ---
            b"REPLCONF" => {
                // A replica's periodic `REPLCONF ACK <offset>` gets no reply; any
                // other REPLCONF (e.g. listening-port) just acks OK.
                if tokens
                    .get(1)
                    .is_some_and(|s| s.eq_ignore_ascii_case(b"ACK"))
                {
                    if let Some(off) = tokens
                        .get(2)
                        .and_then(|t| std::str::from_utf8(t).ok())
                        .and_then(|s| s.parse::<u64>().ok())
                    {
                        self.replica_acks.insert(id, off);
                        self.check_waits();
                    }
                } else {
                    self.send(id, resp::simple_string("OK"));
                }
            }
            b"WAIT" => self.handle_wait(id, &tokens),
            b"PSYNC" | b"SYNC" => {
                // PSYNC <replid> <offset>: a partial resync is possible iff the
                // replid matches ours and the requested offset is still covered by
                // the backlog. Otherwise fall back to a full snapshot.
                let req_off = tokens
                    .get(2)
                    .and_then(|t| std::str::from_utf8(t).ok())
                    .and_then(|s| s.trim().parse::<u64>().ok());
                let replid_ok = tokens
                    .get(1)
                    .is_some_and(|r| r.as_slice() == self.replid.as_bytes());
                let can_continue = self.repl_active
                    && replid_ok
                    && req_off.is_some_and(|o| {
                        o >= self.repl_backlog_start && o <= self.master_repl_offset
                    });

                if can_continue {
                    let off = req_off.unwrap();
                    let from = (off - self.repl_backlog_start) as usize;
                    let tail: Vec<u8> = self.repl_backlog.iter().skip(from).copied().collect();
                    self.send(id, format!("+CONTINUE {}\r\n", self.replid).into_bytes());
                    if !tail.is_empty() {
                        self.send(id, tail);
                    }
                    self.replicas.insert(id);
                    self.replica_acks.insert(id, off);
                    log::info(&format!(
                        "replication: replica {id} partial resync from offset {off} (+{} buffered bytes)",
                        self.master_repl_offset - off
                    ));
                } else {
                    self.send(
                        id,
                        format!(
                            "+FULLRESYNC {} {}\r\n",
                            self.replid, self.master_repl_offset
                        )
                        .into_bytes(),
                    );
                    let mut snap = rdb::serialize(&self.db);
                    rdb::append_extras(&mut snap, &self.build_extras());
                    let mut bulk = format!("${}\r\n", snap.len()).into_bytes();
                    bulk.extend_from_slice(&snap);
                    self.send(id, bulk);
                    self.replicas.insert(id);
                    self.replica_acks.insert(id, self.master_repl_offset);
                    // Activate the backlog at the current offset on the first attach.
                    if !self.repl_active {
                        self.repl_active = true;
                        self.repl_backlog_start = self.master_repl_offset;
                    }
                    log::info(&format!(
                        "replication: replica {id} full resync ({} byte snapshot)",
                        snap.len()
                    ));
                }
            }
            b"REPLICAOF" | b"SLAVEOF" => self.handle_replicaof(id, &tokens),
            b"INFO" => self.send(id, self.render_info()),
            b"CONFIG" => self.handle_config(id, &tokens),
            b"CLUSTER" => self.handle_cluster(id, &tokens),
            b"CLIENT" => self.handle_client(id, &tokens),
            b"SLOWLOG" => self.handle_slowlog(id, &tokens),
            b"ACL" => self.handle_acl(id, &tokens),

            // --- persistence (owner-side) ---
            b"BGREWRITEAOF" => {
                let reply = if self.aof_rewrite_buf.is_some() {
                    resp::error("ERR Background append only file rewriting already in progress")
                } else if let (Some(path), true) = (self.aof_path.clone(), self.aof.is_some()) {
                    // Serialize the base image on the hub (fast, in-memory), then
                    // write+fsync it off-thread. Writes that land meanwhile are
                    // captured in aof_rewrite_buf and folded in on completion.
                    let tmp = format!("{path}.tmp");
                    let buf = aof::serialize_rewrite(&self.db);
                    self.aof_rewrite_buf = Some(Vec::new());
                    let tx = self.tx.clone();
                    thread::spawn(move || {
                        let res = aof::write_tmp(&tmp, &buf)
                            .map(|()| tmp)
                            .map_err(|e| e.to_string());
                        let _ = tx.send(Msg::AofRewriteDone(res));
                    });
                    resp::simple_string("Background append only file rewriting started")
                } else {
                    resp::error("ERR AOF is not enabled")
                };
                self.send(id, reply);
            }
            b"SAVE" => {
                let extras = self.build_extras();
                let reply = match rdb::save_with_extras(&self.db, &extras, &rdb::configured_path())
                {
                    Ok(()) => resp::simple_string("OK"),
                    Err(e) => resp::error(&format!("ERR {e}")),
                };
                self.send(id, reply);
            }
            b"BGSAVE" => {
                if self.bgsave_in_progress.load(Ordering::Relaxed) {
                    self.send(id, resp::error("ERR Background save already in progress"));
                } else {
                    // Serialize on the hub (a consistent point-in-time snapshot),
                    // then write + fsync off-thread so the disk I/O doesn't stall
                    // the command loop. This makes the "started" reply truthful.
                    let mut bytes = rdb::serialize(&self.db);
                    rdb::append_extras(&mut bytes, &self.build_extras());
                    let path = rdb::configured_path();
                    let flag = self.bgsave_in_progress.clone();
                    flag.store(true, Ordering::Relaxed);
                    thread::spawn(move || {
                        match rdb::write_snapshot(&bytes, &path) {
                            Ok(()) => log::info("background save complete"),
                            Err(e) => log::error(&format!("background save failed: {e}")),
                        }
                        flag.store(false, Ordering::Relaxed);
                    });
                    self.send(id, resp::simple_string("Background saving started"));
                }
            }
            b"SHUTDOWN" => {
                let nosave = tokens
                    .get(1)
                    .is_some_and(|a| a.eq_ignore_ascii_case(b"NOSAVE"));
                self.persist_and_exit(!nosave);
            }

            // --- everything else (data commands) ---
            _ => {
                let is_xadd = cmd.as_slice() == b"XADD";
                let reply = self.exec_one(id, tokens);
                self.send(id, reply);
                if is_xadd {
                    self.serve_blocked();
                }
            }
        }
    }

    fn handle_xread(&mut self, id: u64, tokens: &[Vec<u8>]) {
        let req = match streams::parse_xread(tokens) {
            Ok(r) => r,
            Err(e) => return self.send(id, resp::error(&format!("ERR {e}"))),
        };
        let specs = streams::resolve_specs(&mut self.db, &req);
        let collected = streams::xread_collect(&mut self.db, &specs, req.count);
        self.dirty_expired_watchers();
        if let Some(reply) = collected {
            return self.send(id, reply);
        }
        match req.block {
            Some(ms) => {
                let deadline = if ms == 0 { None } else { Some(now_ms() + ms) };
                self.blocked.push(BlockedReader {
                    id,
                    specs,
                    count: req.count,
                    deadline,
                });
            }
            None => self.send(id, streams::nil()),
        }
    }

    /// After a stream gains entries, satisfy any parked readers that can now read.
    fn serve_blocked(&mut self) {
        let mut i = 0;
        while i < self.blocked.len() {
            let specs = self.blocked[i].specs.clone();
            let count = self.blocked[i].count;
            let rid = self.blocked[i].id;
            if let Some(reply) = streams::xread_collect(&mut self.db, &specs, count) {
                self.blocked.remove(i);
                self.send(rid, reply);
            } else {
                i += 1;
            }
        }
    }

    /// CDCSUBSCRIBE [prefix] | CDCSUBSCRIBE REGION <lon> <lat> <radius> <unit>
    /// — enter changefeed push mode: send an atomic snapshot of the matching
    /// keyspace, then live-stream every matching change. Snapshot + registration
    /// happen in one hub turn, so no change can slip between them (no gap/dup).
    fn handle_cdc_subscribe(&mut self, id: u64, tokens: &[Vec<u8>]) {
        if tokens
            .get(1)
            .is_some_and(|t| t.eq_ignore_ascii_case(b"REGION"))
        {
            return self.handle_cdc_subscribe_region(id, tokens);
        }
        let prefix = tokens.get(1).cloned().unwrap_or_default();
        let keys: Vec<Vec<u8>> = self
            .db
            .live_keys()
            .into_iter()
            .filter(|k| k.starts_with(&prefix))
            .collect();
        let n = keys.len();
        for k in keys {
            let val = match self.db.get(&k) {
                Some(Value::Str(s)) => resp::bulk_string(s),
                _ => resp::null_bulk(), // non-string: change-only, client re-fetches
            };
            self.send(
                id,
                resp::array(&[
                    resp::bulk_string(b"cdc-snapshot"),
                    resp::bulk_string(&k),
                    val,
                ]),
            );
        }
        self.changefeeds.insert(id, CdcFilter::Prefix(prefix));
        self.send_snapshot_done(id, n);
    }

    /// Live geofencing: snapshot the geo keys currently inside the circle, then
    /// stream enter/move (`write`) and leave (`del`) transitions.
    fn handle_cdc_subscribe_region(&mut self, id: u64, tokens: &[Vec<u8>]) {
        // CDCSUBSCRIBE REGION <lon> <lat> <radius> <unit>
        let parsed = (|| {
            let lon = std::str::from_utf8(tokens.get(2)?)
                .ok()?
                .parse::<f64>()
                .ok()?;
            let lat = std::str::from_utf8(tokens.get(3)?)
                .ok()?
                .parse::<f64>()
                .ok()?;
            let r = std::str::from_utf8(tokens.get(4)?)
                .ok()?
                .parse::<f64>()
                .ok()?;
            let unit = commands::geo_unit(tokens.get(5)?)?;
            Some((lon, lat, r * unit))
        })();
        let (lon, lat, radius_m) = match parsed {
            Some(p) if tokens.len() == 6 => p,
            _ => return self.send(id, resp::error("ERR syntax error")),
        };
        let mut members = HashSet::new();
        let mut n = 0;
        for k in self.db.geo_keys() {
            let point = match self.db.get(&k) {
                Some(Value::Geo(klon, klat, _)) => Some((*klon, *klat)),
                _ => None,
            };
            if let Some((klon, klat)) = point
                && commands::haversine_m(lon, lat, klon, klat) <= radius_m
            {
                self.send(
                    id,
                    resp::array(&[
                        resp::bulk_string(b"cdc-snapshot"),
                        resp::bulk_string(&k),
                        resp::bulk_string(format!("{klon},{klat}").as_bytes()),
                    ]),
                );
                members.insert(k);
                n += 1;
            }
        }
        self.changefeeds.insert(
            id,
            CdcFilter::Region {
                lon,
                lat,
                radius_m,
                members,
            },
        );
        self.send_snapshot_done(id, n);
    }

    /// `snapshot-done` carries the count and the high-water offset, so a dropped
    /// subscriber can CDCREAD that offset to catch up before resubscribing.
    fn send_snapshot_done(&self, id: u64, count: usize) {
        let hwm = self.cdc_next_offset.saturating_sub(1);
        self.send(
            id,
            resp::array(&[
                resp::bulk_string(b"cdc-snapshot-done"),
                resp::integer(count as i64),
                resp::integer(hwm as i64),
            ]),
        );
    }

    fn handle_cdc_unsubscribe(&mut self, id: u64) {
        self.changefeeds.remove(&id);
        self.send(id, resp::array(&[resp::bulk_string(b"cdc-unsubscribe")]));
    }

    /// Record one keyspace change: assign an offset, push it live to matching
    /// changefeed subscribers (with the offset), and retain it in the ring for
    /// CDCREAD catch-up (when retention is enabled). No-op when there are neither
    /// subscribers nor retention, so it costs nothing unless the feature is used.
    fn record_change(&mut self, event: &[u8], key: &[u8], value: Option<Vec<u8>>) {
        if self.changefeeds.is_empty() && self.cdc_maxlen == 0 {
            return;
        }
        let offset = self.cdc_next_offset;
        self.cdc_next_offset += 1;
        if !self.changefeeds.is_empty() {
            let val = match &value {
                Some(v) => resp::bulk_string(v),
                None => resp::null_bulk(),
            };
            let msg = resp::array(&[
                resp::bulk_string(b"cdc-change"),
                resp::integer(offset as i64),
                resp::bulk_string(event),
                resp::bulk_string(key),
                val,
            ]);
            let mut has_region = false;
            for (cid, filter) in &self.changefeeds {
                match filter {
                    CdcFilter::Prefix(p) if key.starts_with(p) => self.send(*cid, msg.clone()),
                    CdcFilter::Prefix(_) => {}
                    CdcFilter::Region { .. } => has_region = true,
                }
            }
            if has_region {
                self.push_region_changes(key, offset);
            }
        }
        if self.cdc_maxlen > 0 {
            self.cdc_log.push_back(ChangeRecord {
                offset,
                event: event.to_vec(),
                key: key.to_vec(),
                value,
            });
            while self.cdc_log.len() > self.cdc_maxlen {
                self.cdc_log.pop_front();
            }
        }
    }

    /// Emit enter/leave transitions to region (geofence) subscribers for a key
    /// that just changed. Enter/move -> `write` with `"lon,lat"`; a key that left
    /// the circle (moved out, deleted, or expired) -> `del`.
    fn push_region_changes(&mut self, key: &[u8], offset: u64) {
        let point = match self.db.get(key) {
            Some(Value::Geo(lon, lat, _)) => Some((*lon, *lat)),
            _ => None,
        };
        let ids: Vec<u64> = self
            .changefeeds
            .iter()
            .filter(|(_, f)| matches!(f, CdcFilter::Region { .. }))
            .map(|(id, _)| *id)
            .collect();
        for cid in ids {
            // Decide the transition under a short mutable borrow, then send.
            let action = {
                let Some(CdcFilter::Region {
                    lon,
                    lat,
                    radius_m,
                    members,
                }) = self.changefeeds.get_mut(&cid)
                else {
                    continue;
                };
                let inside = point
                    .is_some_and(|(kl, ka)| commands::haversine_m(*lon, *lat, kl, ka) <= *radius_m);
                let was = members.contains(key);
                if inside {
                    members.insert(key.to_vec());
                    Some(true) // enter or move-within
                } else if was {
                    members.remove(key);
                    Some(false) // left the region
                } else {
                    None
                }
            };
            match action {
                Some(true) => {
                    let (kl, ka) = point.unwrap();
                    self.send(
                        cid,
                        resp::array(&[
                            resp::bulk_string(b"cdc-change"),
                            resp::integer(offset as i64),
                            resp::bulk_string(b"write"),
                            resp::bulk_string(key),
                            resp::bulk_string(format!("{kl},{ka}").as_bytes()),
                        ]),
                    );
                }
                Some(false) => self.send(
                    cid,
                    resp::array(&[
                        resp::bulk_string(b"cdc-change"),
                        resp::integer(offset as i64),
                        resp::bulk_string(b"del"),
                        resp::bulk_string(key),
                        resp::null_bulk(),
                    ]),
                ),
                None => {}
            }
        }
    }

    /// Re-index a key against every secondary index, from its post-write state.
    /// Idempotent and drift-free: remove the key's old bucket entry, then add it
    /// to the bucket for its current field value (if it's a hash with that field).
    fn reindex_key(&mut self, key: &[u8]) {
        if self.indexes.is_empty() {
            return;
        }
        let names: Vec<Vec<u8>> = self.indexes.keys().cloned().collect();
        for name in names {
            let field = self.indexes[&name].field.clone();
            let newval = match self.db.get(key) {
                Some(Value::Hash(h)) => h.get(&field).cloned(),
                _ => None,
            };
            let ix = self.indexes.get_mut(&name).unwrap();
            if let Some(old) = ix.reverse.remove(key)
                && let Some(set) = ix.forward.get_mut(&old)
            {
                set.remove(key);
                if set.is_empty() {
                    ix.forward.remove(&old);
                }
            }
            if let Some(v) = newval {
                ix.forward
                    .entry(v.clone())
                    .or_default()
                    .insert(key.to_vec());
                ix.reverse.insert(key.to_vec(), v);
            }
        }
    }

    /// IDXCREATE <name> <field> | IDXDROP <name>.
    fn handle_idx_admin(&mut self, id: u64, tokens: &[Vec<u8>], create: bool) {
        if (create && tokens.len() != 3) || (!create && tokens.len() != 2) {
            return self.send(id, resp::error("ERR wrong number of arguments"));
        }
        if !create {
            let removed = self.indexes.remove(&tokens[1]).is_some();
            return self.send(id, resp::integer(removed as i64));
        }
        if self.indexes.contains_key(&tokens[1]) {
            return self.send(id, resp::error("ERR index already exists"));
        }
        // Build the index from the current keyspace (then writes keep it in sync).
        self.add_index(tokens[1].clone(), tokens[2].clone());
        self.send(id, resp::simple_string("OK"));
    }

    /// IDXGET <name> <value> — keys whose indexed field equals `value`.
    fn handle_idx_get(&mut self, id: u64, tokens: &[Vec<u8>]) {
        if tokens.len() != 3 {
            return self.send(id, resp::error("ERR wrong number of arguments"));
        }
        let keys: Vec<Vec<u8>> = match self.indexes.get(&tokens[1]) {
            None => return self.send(id, resp::error("ERR no such index")),
            Some(ix) => ix
                .forward
                .get(&tokens[2])
                .map(|s| s.iter().cloned().collect())
                .unwrap_or_default(),
        };
        self.send(id, resp::bulk_array(&keys));
    }

    /// IDXRANGE <name> <min> <max> [COUNT n] — keys whose field is in [min, max]
    /// (lexicographic), in field order, capped at COUNT.
    fn handle_idx_range(&mut self, id: u64, tokens: &[Vec<u8>]) {
        if tokens.len() != 4 && tokens.len() != 6 {
            return self.send(id, resp::error("ERR wrong number of arguments"));
        }
        let count = if tokens.len() == 6 {
            if !tokens[4].eq_ignore_ascii_case(b"COUNT") {
                return self.send(id, resp::error("ERR syntax error"));
            }
            match std::str::from_utf8(&tokens[5])
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
            {
                Some(c) => Some(c),
                None => {
                    return self.send(
                        id,
                        resp::error("ERR value is not an integer or out of range"),
                    );
                }
            }
        } else {
            None
        };
        let ix = match self.indexes.get(&tokens[1]) {
            None => return self.send(id, resp::error("ERR no such index")),
            Some(ix) => ix,
        };
        let mut out: Vec<Vec<u8>> = Vec::new();
        for (_, keys) in ix.forward.range(tokens[2].clone()..=tokens[3].clone()) {
            for k in keys {
                out.push(k.clone());
                if count.is_some_and(|c| out.len() >= c) {
                    return self.send(id, resp::bulk_array(&out));
                }
            }
        }
        self.send(id, resp::bulk_array(&out));
    }

    /// CDCREAD <offset> [COUNT n] [PREFIX p] — pull retained changes after an
    /// offset (for catch-up after a disconnect). Non-blocking.
    fn handle_cdc_read(&mut self, id: u64, tokens: &[Vec<u8>]) {
        if tokens.len() < 2 {
            return self.send(
                id,
                resp::error("ERR wrong number of arguments for 'cdcread' command"),
            );
        }
        if self.cdc_maxlen == 0 {
            return self.send(
                id,
                resp::error("ERR changefeed retention disabled (set LOCUS_CDC_MAXLEN)"),
            );
        }
        let from = match std::str::from_utf8(&tokens[1])
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
        {
            Some(o) => o,
            None => {
                return self.send(
                    id,
                    resp::error("ERR value is not an integer or out of range"),
                );
            }
        };
        let mut count: Option<usize> = None;
        let mut prefix: Vec<u8> = Vec::new();
        let mut i = 2;
        while i < tokens.len() {
            match tokens[i].to_ascii_uppercase().as_slice() {
                b"COUNT" => {
                    i += 1;
                    count = match tokens
                        .get(i)
                        .and_then(|t| std::str::from_utf8(t).ok())
                        .and_then(|s| s.parse::<usize>().ok())
                    {
                        Some(c) => Some(c),
                        None => {
                            return self.send(
                                id,
                                resp::error("ERR value is not an integer or out of range"),
                            );
                        }
                    };
                }
                b"PREFIX" => {
                    i += 1;
                    prefix = match tokens.get(i) {
                        Some(p) => p.clone(),
                        None => return self.send(id, resp::error("ERR syntax error")),
                    };
                }
                _ => return self.send(id, resp::error("ERR syntax error")),
            }
            i += 1;
        }
        // If the oldest retained record is newer than from+1, records were
        // dropped — the consumer fell behind and must re-snapshot.
        if let Some(front) = self.cdc_log.front()
            && front.offset > from.saturating_add(1)
        {
            return self.send(
                id,
                resp::error("ERR offset out of range (changefeed history truncated)"),
            );
        }
        let mut out: Vec<Vec<u8>> = Vec::new();
        for rec in &self.cdc_log {
            if rec.offset > from && rec.key.starts_with(&prefix) {
                let val = match &rec.value {
                    Some(v) => resp::bulk_string(v),
                    None => resp::null_bulk(),
                };
                out.push(resp::array(&[
                    resp::integer(rec.offset as i64),
                    resp::bulk_string(&rec.event),
                    resp::bulk_string(&rec.key),
                    val,
                ]));
                if count.is_some_and(|c| c > 0 && out.len() >= c) {
                    break;
                }
            }
        }
        self.send(id, resp::array(&out));
    }

    /// CDCGROUP CREATE <group> [<offset>|$|0] | CDCGROUP DESTROY <group>.
    fn handle_cdc_group(&mut self, id: u64, tokens: &[Vec<u8>]) {
        if tokens.len() < 3 {
            return self.send(
                id,
                resp::error("ERR wrong number of arguments for 'cdcgroup' command"),
            );
        }
        let group = tokens[2].clone();
        match tokens[1].to_ascii_uppercase().as_slice() {
            b"CREATE" => {
                if self.cdc_maxlen == 0 {
                    return self.send(
                        id,
                        resp::error("ERR changefeed retention disabled (set LOCUS_CDC_MAXLEN)"),
                    );
                }
                if self.cdc_groups.contains_key(&group) {
                    return self.send(id, resp::error("BUSYGROUP changefeed group already exists"));
                }
                // Start offset: default / "$" = only new changes; "0" = all retained.
                let hwm = self.cdc_next_offset.saturating_sub(1);
                let start = match tokens.get(3) {
                    None => hwm,
                    Some(o) if o.as_slice() == b"$" => hwm,
                    Some(o) => match std::str::from_utf8(o)
                        .ok()
                        .and_then(|s| s.parse::<u64>().ok())
                    {
                        Some(n) => n,
                        None => {
                            return self.send(
                                id,
                                resp::error("ERR value is not an integer or out of range"),
                            );
                        }
                    },
                };
                self.cdc_groups.insert(
                    group,
                    CdcGroup {
                        last_delivered: start,
                        pending: HashMap::new(),
                    },
                );
                self.send(id, resp::simple_string("OK"));
            }
            b"DESTROY" => {
                let removed = self.cdc_groups.remove(&group).is_some();
                self.send(id, resp::integer(removed as i64));
            }
            _ => self.send(id, resp::error("ERR syntax error")),
        }
    }

    /// CDCREADGROUP <group> <consumer> [COUNT n] — deliver the next un-delivered
    /// records to this consumer (load-balanced across the group) and track them
    /// as pending until acked.
    fn handle_cdc_readgroup(&mut self, id: u64, tokens: &[Vec<u8>]) {
        if tokens.len() < 3 {
            return self.send(
                id,
                resp::error("ERR wrong number of arguments for 'cdcreadgroup' command"),
            );
        }
        let mut count: Option<usize> = None;
        if tokens.len() >= 5 && tokens[3].eq_ignore_ascii_case(b"COUNT") {
            count = match std::str::from_utf8(&tokens[4])
                .ok()
                .and_then(|s| s.parse().ok())
            {
                Some(c) => Some(c),
                None => {
                    return self.send(
                        id,
                        resp::error("ERR value is not an integer or out of range"),
                    );
                }
            };
        }
        let consumer = tokens[2].clone();
        let last = match self.cdc_groups.get(&tokens[1]) {
            Some(g) => g.last_delivered,
            None => return self.send(id, resp::error("NOGROUP No such changefeed group")),
        };
        // Scan the log for new records (no group borrow held during the scan).
        let mut out: Vec<Vec<u8>> = Vec::new();
        let mut offsets: Vec<u64> = Vec::new();
        let mut maxoff = last;
        for rec in &self.cdc_log {
            if rec.offset > last {
                let val = match &rec.value {
                    Some(v) => resp::bulk_string(v),
                    None => resp::null_bulk(),
                };
                out.push(resp::array(&[
                    resp::integer(rec.offset as i64),
                    resp::bulk_string(&rec.event),
                    resp::bulk_string(&rec.key),
                    val,
                ]));
                offsets.push(rec.offset);
                maxoff = rec.offset;
                if count.is_some_and(|c| c > 0 && out.len() >= c) {
                    break;
                }
            }
        }
        if let Some(g) = self.cdc_groups.get_mut(&tokens[1]) {
            g.last_delivered = maxoff;
            for off in offsets {
                g.pending.insert(off, consumer.clone());
            }
        }
        self.send(id, resp::array(&out));
    }

    /// CDCACK <group> <offset> [offset ...] — acknowledge delivery (drop from PEL).
    fn handle_cdc_ack(&mut self, id: u64, tokens: &[Vec<u8>]) {
        if tokens.len() < 3 {
            return self.send(
                id,
                resp::error("ERR wrong number of arguments for 'cdcack' command"),
            );
        }
        let g = match self.cdc_groups.get_mut(&tokens[1]) {
            Some(g) => g,
            None => return self.send(id, resp::error("NOGROUP No such changefeed group")),
        };
        let mut acked = 0i64;
        for off in &tokens[2..] {
            if let Some(n) = std::str::from_utf8(off)
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                && g.pending.remove(&n).is_some()
            {
                acked += 1;
            }
        }
        self.send(id, resp::integer(acked));
    }

    /// CDCPENDING <group> — total pending plus a per-consumer breakdown.
    fn handle_cdc_pending(&mut self, id: u64, tokens: &[Vec<u8>]) {
        if tokens.len() != 2 {
            return self.send(
                id,
                resp::error("ERR wrong number of arguments for 'cdcpending' command"),
            );
        }
        let g = match self.cdc_groups.get(&tokens[1]) {
            Some(g) => g,
            None => return self.send(id, resp::error("NOGROUP No such changefeed group")),
        };
        let mut counts: HashMap<&Vec<u8>, usize> = HashMap::new();
        for c in g.pending.values() {
            *counts.entry(c).or_insert(0) += 1;
        }
        let per: Vec<Vec<u8>> = counts
            .iter()
            .map(|(c, n)| resp::array(&[resp::bulk_string(c), resp::integer(*n as i64)]))
            .collect();
        self.send(
            id,
            resp::array(&[resp::integer(g.pending.len() as i64), resp::array(&per)]),
        );
    }

    /// Emit changefeed events for the keys a write touched (called from exec_one
    /// after a confirmed modification). `Del` if the key is now gone, else
    /// `Write` (with the new value for strings).
    fn emit_write_changes(&mut self, tokens: &[Vec<u8>]) {
        if self.changefeeds.is_empty() && self.cdc_maxlen == 0 {
            return;
        }
        let keys: Vec<Vec<u8>> = write_keys(tokens).iter().map(|k| k.to_vec()).collect();
        for key in keys {
            // Read the post-command state, then record (no db borrow held across send).
            let (event, value): (&[u8], Option<Vec<u8>>) = match self.db.get(&key) {
                Some(Value::Str(s)) => (b"write", Some(s.clone())),
                Some(_) => (b"write", None), // non-string: change-only, client re-fetches
                None => (b"del", None),
            };
            self.record_change(event, &key, value);
        }
    }

    /// Time out parked readers whose BLOCK deadline has passed (reply nil).
    fn expire_blocked(&mut self) {
        let now = now_ms();
        let mut i = 0;
        while i < self.blocked.len() {
            match self.blocked[i].deadline {
                Some(d) if d <= now => {
                    let rid = self.blocked[i].id;
                    self.blocked.remove(i);
                    self.send(rid, streams::nil());
                }
                _ => i += 1,
            }
        }
    }

    /// WAIT numreplicas timeout — reply with the count of replicas that have
    /// acked the offset as of this command, blocking up to `timeout` ms (0 =
    /// forever) until that many catch up.
    fn handle_wait(&mut self, id: u64, tokens: &[Vec<u8>]) {
        let parse = |t: Option<&Vec<u8>>| -> Option<u64> {
            std::str::from_utf8(t?).ok()?.trim().parse().ok()
        };
        let (numreplicas, timeout) = match (parse(tokens.get(1)), parse(tokens.get(2))) {
            (Some(n), Some(t)) => (n as usize, t),
            _ => {
                return self.send(
                    id,
                    resp::error("ERR wrong number of arguments for 'wait' command"),
                );
            }
        };
        let target = self.master_repl_offset;
        let acked = self.count_acked(target);
        if acked >= numreplicas {
            return self.send(id, resp::integer(acked as i64));
        }
        let deadline = if timeout == 0 {
            None
        } else {
            Some(now_ms() + timeout)
        };
        self.waiting.push(WaitReq {
            id,
            target,
            numreplicas,
            deadline,
        });
    }

    fn count_acked(&self, target: u64) -> usize {
        self.replica_acks.values().filter(|&&o| o >= target).count()
    }

    /// Reply to any parked WAITs whose replica quorum is now met or that timed out.
    fn check_waits(&mut self) {
        let now = now_ms();
        let mut i = 0;
        while i < self.waiting.len() {
            let w = &self.waiting[i];
            let acked = self.count_acked(w.target);
            if acked >= w.numreplicas || w.deadline.is_some_and(|d| d <= now) {
                let rid = w.id;
                self.waiting.remove(i);
                self.send(rid, resp::integer(acked as i64));
            } else {
                i += 1;
            }
        }
    }

    /// RESP3 negotiation. Accepts HELLO [2|3]; replies with server info as a
    /// RESP3 map (proto 3) or RESP2 flat array (proto 2). Most reply types are
    /// identical across RESP2/RESP3, so we track the version but keep the
    /// existing encoders (full RESP3 typing of every reply is a later extension).
    fn handle_auth(&mut self, id: u64, tokens: &[Vec<u8>]) {
        // AUTH <password>  or  AUTH <username> <password>. Only the implicit
        // "default" user exists today, so any other username is rejected.
        let pass = match tokens.len() {
            2 => &tokens[1],
            3 if tokens[1].eq_ignore_ascii_case(b"default") => &tokens[2],
            // A named (non-default) user authenticates against the ACL table.
            3 => return self.auth_named_user(id, &tokens[1], &tokens[2]),
            _ => {
                return self.send(
                    id,
                    resp::error("ERR wrong number of arguments for 'auth' command"),
                );
            }
        };
        match &self.requirepass {
            None => self.send(
                id,
                resp::error(
                    "ERR Client sent AUTH, but no password is set. Did you mean AUTH <username> <password>?",
                ),
            ),
            Some(secret) if ct_eq(secret, pass) => {
                self.authed.insert(id);
                self.send(id, resp::simple_string("OK"));
            }
            Some(_) => self.send(
                id,
                resp::error("WRONGPASS invalid username-password pair or user is disabled."),
            ),
        }
    }

    /// Authenticate as a named ACL user and bind the connection to it.
    fn auth_named_user(&mut self, id: u64, name: &[u8], pass: &[u8]) {
        match self.users.get(name) {
            Some(u) if u.enabled && u.check_password(pass) => {
                self.authed.insert(id);
                self.current_user.insert(id, name.to_vec());
                self.send(id, resp::simple_string("OK"));
            }
            _ => self.send(
                id,
                resp::error("WRONGPASS invalid username-password pair or user is disabled."),
            ),
        }
    }

    fn handle_hello(&mut self, id: u64, tokens: &[Vec<u8>]) {
        let mut proto = 2u8;
        if let Some(v) = tokens.get(1) {
            match std::str::from_utf8(v)
                .ok()
                .and_then(|s| s.parse::<u8>().ok())
            {
                Some(2) => proto = 2,
                Some(3) => proto = 3,
                _ => return self.send(id, resp::error("NOPROTO unsupported protocol version")),
            }
        }
        // Optional `AUTH <user> <pass>` (and accepted-but-ignored `SETNAME
        // <name>`) clauses, so clients can authenticate and select the protocol
        // in one round-trip (redis-cli / ioredis do this on connect).
        let mut i = 2;
        while i < tokens.len() {
            if tokens[i].eq_ignore_ascii_case(b"AUTH") && i + 2 < tokens.len() {
                let ok = match &self.requirepass {
                    None => {
                        return self.send(
                            id,
                            resp::error(
                                "ERR Client sent AUTH, but no password is set. Did you mean AUTH <username> <password>?",
                            ),
                        );
                    }
                    Some(secret) => {
                        tokens[i + 1].eq_ignore_ascii_case(b"default")
                            && ct_eq(secret, &tokens[i + 2])
                    }
                };
                if ok {
                    self.authed.insert(id);
                } else {
                    return self.send(
                        id,
                        resp::error(
                            "WRONGPASS invalid username-password pair or user is disabled.",
                        ),
                    );
                }
                i += 3;
            } else if tokens[i].eq_ignore_ascii_case(b"SETNAME") && i + 1 < tokens.len() {
                i += 2; // client name accepted and ignored (CLIENT SETNAME is later)
            } else {
                return self.send(id, resp::error("ERR Syntax error in HELLO"));
            }
        }
        // If a password is required and the client hasn't authenticated (here or
        // via a prior AUTH), refuse the upgrade — Redis HELLO semantics.
        if self.requirepass.is_some() && !self.authed.contains(&id) {
            return self.send(
                id,
                resp::error(
                    "NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time",
                ),
            );
        }
        self.protos.insert(id, proto);
        let role = if self.master.is_some() {
            "replica"
        } else {
            "master"
        };
        let fields: Vec<(&[u8], Vec<u8>)> = vec![
            (b"server", b"locus".to_vec()),
            (b"version", b"0.1.0".to_vec()),
            (b"proto", proto.to_string().into_bytes()),
            (b"id", id.to_string().into_bytes()),
            (b"mode", b"standalone".to_vec()),
            (b"role", role.as_bytes().to_vec()),
        ];
        let reply = if proto == 3 {
            let mut o = format!("%{}\r\n", fields.len()).into_bytes();
            for (k, v) in &fields {
                o.extend_from_slice(&resp::bulk_string(k));
                o.extend_from_slice(&resp::bulk_string(v));
            }
            o
        } else {
            let mut flat = Vec::new();
            for (k, v) in &fields {
                flat.push(k.to_vec());
                flat.push(v.clone());
            }
            resp::bulk_array(&flat)
        };
        self.send(id, reply);
    }

    /// Execute one data command: read-only check, run it, log to AOF, propagate
    /// to replicas, and mark any watching transactions dirty. Returns the reply.
    fn exec_one(&mut self, id: u64, tokens: Vec<Vec<u8>>) -> Vec<u8> {
        let cmd = tokens[0].to_ascii_uppercase();
        let is_write = aof::is_write(&cmd);
        if self.master.is_some() && id != MASTER_ID && is_write {
            return resp::error("READONLY You can't write against a read only replica.");
        }
        // maxmemory: on a master/standalone, free memory before a write; if the
        // cap still can't be met, reject the write instead of growing unbounded.
        // (Replicas don't evict on their own — the master streams the DELs.)
        if is_write && self.master.is_none() && !self.evict_if_needed() {
            return resp::error("OOM command not allowed when used memory > 'maxmemory'.");
        }
        let proto = self.protos.get(&id).copied().unwrap_or(2);
        let reply = execute_proto(&tokens, &mut self.db, proto);
        let errored = reply.first() == Some(&b'-');
        if !errored && is_write {
            // Keep the memory estimate in sync with whatever the command changed
            // (including in-place collection growth like LPUSH/SADD).
            for key in write_keys(&tokens) {
                self.db.resync_size(key);
                self.reindex_key(key); // keep secondary indexes in sync (no drift)
            }
            // A write only counts as a modification if it actually changed the
            // dataset. A no-op write (e.g. DEL of a missing key) is not logged,
            // not replicated, and does not dirty a WATCHer.
            if write_modified(&cmd, &reply) {
                for key in write_keys(&tokens) {
                    self.dirty_watchers(key);
                }
                let entries = aof::entries_for(&tokens, &reply, &mut self.db);
                if let Some(a) = self.aof.as_mut() {
                    let _ = a.append(&entries);
                }
                // Mirror into the rewrite buffer so an in-flight BGREWRITEAOF
                // doesn't lose writes made while its base image is being written.
                if let Some(buf) = self.aof_rewrite_buf.as_mut() {
                    aof::encode_into(buf, &entries);
                }
                // Propagate the deterministic form to every replica.
                for e in &entries {
                    self.replicate(resp::command(e));
                }
                // Feed the changefeed (same modified-key set as WATCH/AOF).
                self.emit_write_changes(&tokens);
            }
        }
        if let Some(a) = self.aof.as_mut() {
            a.maybe_fsync();
        }
        // A watched key removed by passive expiry during this command must also
        // abort its transaction.
        self.dirty_expired_watchers();
        reply
    }

    /// Mark every transaction watching `key` as dirty (its EXEC will abort).
    fn dirty_watchers(&mut self, key: &[u8]) {
        if let Some(watchers) = self.watched_keys.get(key) {
            let ids: Vec<u64> = watchers.iter().copied().collect();
            for wid in ids {
                if let Some(tx) = self.txs.get_mut(&wid) {
                    tx.dirty = true;
                }
            }
        }
    }

    /// Dirty the WATCHers of any key the keyspace expired since the last drain,
    /// and emit an `expire` change to changefeed subscribers.
    fn dirty_expired_watchers(&mut self) {
        let is_master = self.master.is_none();
        for key in self.db.take_expired() {
            self.dirty_watchers(&key);
            self.record_change(b"expire", &key, None);
            self.reindex_key(&key); // drop expired key from secondary indexes
            // The master is authoritative for expiry: stream a DEL to replicas
            // (and the AOF) so they delete the key on our schedule instead of
            // expiring independently (which would let the two diverge).
            if is_master {
                self.propagate(&[b"DEL".to_vec(), key]);
            }
        }
    }

    /// Enforce `maxmemory` by evicting arbitrary keys until under the cap.
    /// Returns true if we are within budget (proceed), false if the cap cannot
    /// be met (the caller should reject the write with OOM). Each eviction is
    /// streamed to replicas/AOF as a DEL and dirties any WATCHers — just like a
    /// client-issued delete, so replicas and snapshots stay consistent.
    fn evict_if_needed(&mut self) -> bool {
        let max = match self.maxmemory {
            Some(m) => m,
            None => return true,
        };
        while self.db.mem_used() > max {
            match self.db.evict_one() {
                Some(key) => {
                    self.dirty_watchers(&key);
                    self.record_change(b"del", &key, None);
                    self.reindex_key(&key); // drop evicted key from secondary indexes
                    self.propagate(&[b"DEL".to_vec(), key]);
                }
                None => break, // keyspace empty — nothing left to evict
            }
        }
        self.db.mem_used() <= max
    }

    /// Append one command to the AOF and stream it to every replica.
    fn propagate(&mut self, tokens: &[Vec<u8>]) {
        let batch = vec![tokens.to_vec()];
        if let Some(a) = self.aof.as_mut() {
            let _ = a.append(&batch);
        }
        self.replicate(resp::command(tokens));
    }

    /// Stream one already-encoded command to every replica, append it to the
    /// backlog, and advance the replication offset (the running byte position in
    /// the write stream). Once a replica has ever attached (`repl_active`) we keep
    /// advancing + buffering even with no replica currently connected, so a
    /// reconnecting replica's offset still reflects what it missed.
    fn replicate(&mut self, bytes: Vec<u8>) {
        if !self.repl_active {
            return; // replication never started; nothing to track yet
        }
        self.master_repl_offset += bytes.len() as u64;
        self.repl_backlog.extend(bytes.iter().copied());
        while self.repl_backlog.len() > REPL_BACKLOG_MAX {
            self.repl_backlog.pop_front();
            self.repl_backlog_start += 1;
        }
        for rid in self.replicas.iter() {
            if let Some(out) = self.clients.get(rid) {
                let _ = out.send(bytes.clone());
            }
        }
    }

    fn unwatch_keys(&mut self, watched: &[Vec<u8>], id: u64) {
        for key in watched {
            if let Some(set) = self.watched_keys.get_mut(key) {
                set.remove(&id);
                if set.is_empty() {
                    self.watched_keys.remove(key);
                }
            }
        }
    }

    fn handle_replicaof(&mut self, id: u64, tokens: &[Vec<u8>]) {
        if tokens.len() != 3 {
            return self.send(
                id,
                resp::error("ERR wrong number of arguments for 'replicaof' command"),
            );
        }
        // Stop any existing replication link first.
        if let Some(flag) = self.replica_stop.take() {
            flag.store(true, Ordering::Relaxed);
        }
        if tokens[1].eq_ignore_ascii_case(b"NO") && tokens[2].eq_ignore_ascii_case(b"ONE") {
            self.master = None;
            log::info("replication: promoted to master");
            return self.send(id, resp::simple_string("OK"));
        }
        let host = String::from_utf8_lossy(&tokens[1]).to_string();
        let port = String::from_utf8_lossy(&tokens[2]).to_string();
        self.master = Some((host.clone(), port.clone()));
        self.master_link_up = false; // until the full sync completes
        let stop = Arc::new(AtomicBool::new(false));
        self.replica_stop = Some(stop.clone());
        let addr = format!("{host}:{port}");
        let txc = self.tx.clone();
        let masterauth = self.masterauth.clone();
        thread::spawn(move || replica_sync(addr, masterauth, txc, stop));
        log::info(&format!("replication: now replicating from {host}:{port}"));
        self.send(id, resp::simple_string("OK"));
    }

    fn handle_pubsub_introspect(&self, id: u64, tokens: &[Vec<u8>]) {
        let sub = tokens.get(1).map(|t| t.to_ascii_uppercase());
        let reply = match sub.as_deref() {
            Some(b"CHANNELS") => {
                let pat = tokens.get(2);
                let chans: Vec<Vec<u8>> = self
                    .pubsub
                    .active_channels()
                    .into_iter()
                    .filter(|c| pat.is_none_or(|p| pubsub::glob_match(p, c)))
                    .collect();
                resp::bulk_array(&chans)
            }
            Some(b"NUMSUB") => {
                let mut out = Vec::new();
                for ch in &tokens[2..] {
                    out.push(resp::bulk_string(ch));
                    out.push(resp::integer(self.pubsub.numsub(ch)));
                }
                resp::array(&out)
            }
            Some(b"NUMPAT") => resp::integer(self.pubsub.numpat()),
            _ => resp::error("ERR Unknown PUBSUB subcommand"),
        };
        self.send(id, reply);
    }
}

fn allowed_in_push_mode(cmd: &[u8]) -> bool {
    matches!(
        cmd,
        b"SUBSCRIBE"
            | b"UNSUBSCRIBE"
            | b"PSUBSCRIBE"
            | b"PUNSUBSCRIBE"
            | b"CDCSUBSCRIBE"
            | b"CDCUNSUBSCRIBE"
            | b"PING"
            | b"QUIT"
            | b"RESET"
    )
}

fn is_tx_control(cmd: &[u8]) -> bool {
    matches!(
        cmd,
        b"MULTI" | b"EXEC" | b"DISCARD" | b"WATCH" | b"UNWATCH" | b"RESET"
    )
}

/// Commands a not-yet-authenticated connection may run when a password is set:
/// the connection-setup verbs needed to perform (or precede) AUTH.
fn is_no_auth(cmd: &[u8]) -> bool {
    matches!(cmd, b"AUTH" | b"HELLO" | b"QUIT" | b"RESET")
}

/// Constant-time equality: folds the whole comparison (including a length
/// mismatch) into one accumulator and always scans the longer slice, so AUTH
/// latency doesn't reveal how much of the secret matched.
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    let mut diff: u8 = if a.len() == b.len() { 0 } else { 1 };
    let n = a.len().max(b.len());
    for i in 0..n {
        let x = a.get(i).copied().unwrap_or(0);
        let y = b.get(i).copied().unwrap_or(0);
        diff |= x ^ y;
    }
    diff == 0
}

/// Idle read timeout (LOCUS_TIMEOUT seconds); 0/unset means no timeout, matching
/// the Redis default. Drops connections that go silent (basic slow-loris guard).
fn idle_timeout() -> Option<Duration> {
    std::env::var("LOCUS_TIMEOUT")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .filter(|&s| s > 0)
        .map(Duration::from_secs)
}

/// Keys a write command modifies (for WATCH dirtying + memory resync). Most
/// commands touch a single key at position 1; the multi-key writes are spelled
/// out. FLUSHDB/FLUSHALL touch every key — handled separately via the keyspace's
/// expired-key log, not here.
fn write_keys(tokens: &[Vec<u8>]) -> Vec<&[u8]> {
    match tokens[0].to_ascii_uppercase().as_slice() {
        b"DEL" | b"UNLINK" => tokens[1..].iter().map(|k| k.as_slice()).collect(),
        // MSET key val key val ... -> the keys are at odd positions.
        b"MSET" | b"MSETNX" => tokens[1..]
            .iter()
            .step_by(2)
            .map(|k| k.as_slice())
            .collect(),
        // src dst -> both source and destination change.
        b"RENAME" | b"RENAMENX" | b"RPOPLPUSH" | b"LMOVE" | b"SMOVE" => {
            tokens[1..3].iter().map(|k| k.as_slice()).collect()
        }
        // BITOP op dest src... -> the destination is at position 2.
        b"BITOP" => tokens
            .get(2)
            .map(|k| vec![k.as_slice()])
            .unwrap_or_default(),
        _ => tokens
            .get(1)
            .map(|k| vec![k.as_slice()])
            .unwrap_or_default(),
    }
}

/// Did a successful write actually change its key? Used so no-op writes don't
/// log to the AOF, replicate, or dirty a WATCHer (Redis signals a key modified
/// only when it really changed). Conservative by design: when in doubt this
/// returns `true` — a spurious WATCH abort is harmless, a *missed* one is not,
/// and dropping a real write from the AOF/replicas would corrupt state. So a
/// reply pattern is treated as "no change" only where it is unambiguous.
fn write_modified(cmd: &[u8], reply: &[u8]) -> bool {
    let zero = reply.starts_with(b":0\r\n"); // integer 0
    let nil = reply == b"$-1\r\n"; // null bulk
    match cmd {
        // "count of elements changed" commands: 0 means nothing changed.
        b"DEL" | b"UNLINK" | b"SREM" | b"HDEL" | b"ZREM" | b"SADD" | b"HSETNX" | b"LPUSHX"
        | b"RPUSHX" | b"PERSIST" | b"EXPIRE" | b"PEXPIRE" | b"EXPIREAT" | b"PEXPIREAT"
        | b"SETNX" | b"MSETNX" | b"RENAMENX" | b"LREM" | b"SMOVE" | b"ZREMRANGEBYRANK"
        | b"ZREMRANGEBYSCORE" | b"CAS" | b"CADEL" | b"SETMAX" | b"BFADD" => !zero,
        // ZADD: 0 added/changed, or nil from an aborted INCR (NX/XX/GT/LT).
        b"ZADD" => !(zero || nil),
        // LINSERT: 0 (no key) or -1 (pivot not found) means nothing was inserted.
        b"LINSERT" => !(zero || reply.starts_with(b":-1\r\n")),
        // Conditional write / pop-and-move / delete: nil means it didn't happen.
        b"SET" | b"GETDEL" | b"RPOPLPUSH" | b"LMOVE" | b"INCRCAP" => !nil,
        _ => true,
    }
}

/// Parse a `LOCUS_MAXMEMORY` value into bytes. Accepts a plain integer or a
/// `kb`/`mb`/`gb` suffix (decimal, case-insensitive): `0` / "" disable the cap.
fn parse_mem(s: &str) -> Option<usize> {
    let s = s.trim().to_ascii_lowercase();
    if s.is_empty() {
        return None;
    }
    let (num, mult) = if let Some(n) = s.strip_suffix("gb") {
        (n, 1024 * 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("mb") {
        (n, 1024 * 1024)
    } else if let Some(n) = s.strip_suffix("kb") {
        (n, 1024)
    } else if let Some(n) = s.strip_suffix('b') {
        (n, 1)
    } else {
        (s.as_str(), 1)
    };
    num.trim().parse::<usize>().ok().map(|n| n * mult)
}

/// A bounded copy of a command's args for the slow log (Redis caps at 32 args,
/// 128 bytes each, to keep the ring small).
fn slowlog_snapshot(tokens: &[Vec<u8>]) -> Vec<Vec<u8>> {
    tokens
        .iter()
        .take(32)
        .map(|t| {
            if t.len() > 128 {
                let mut s = t[..125].to_vec();
                s.extend_from_slice(b"...");
                s
            } else {
                t.clone()
            }
        })
        .collect()
}

fn run_hub(rx: mpsc::Receiver<Msg>, tx: mpsc::Sender<Msg>) {
    let mut hub = Hub::new(tx);
    loop {
        match rx.recv_timeout(Duration::from_millis(100)) {
            Ok(Msg::Connect { id, out, loopback }) => {
                hub.clients.insert(id, out);
                if loopback {
                    hub.loopback.insert(id);
                }
            }
            Ok(Msg::Disconnect { id }) => {
                hub.clients.remove(&id);
                hub.pubsub.remove_client(id);
                hub.replicas.remove(&id);
                if let Some(t) = hub.txs.remove(&id) {
                    hub.unwatch_keys(&t.watched, id);
                }
                hub.blocked.retain(|r| r.id != id);
                hub.replica_acks.remove(&id);
                hub.waiting.retain(|w| w.id != id);
                hub.protos.remove(&id);
                hub.changefeeds.remove(&id);
                hub.authed.remove(&id);
                hub.loopback.remove(&id);
                hub.client_names.remove(&id);
                hub.current_user.remove(&id);
            }
            Ok(Msg::Command { id, tokens }) => {
                if hub.slowlog_threshold_us >= 0 {
                    let snap = slowlog_snapshot(&tokens);
                    let t0 = Instant::now();
                    hub.handle_command(id, tokens);
                    let us = t0.elapsed().as_micros() as i64;
                    if us >= hub.slowlog_threshold_us {
                        hub.push_slowlog(us as u64, snap);
                    }
                } else {
                    hub.handle_command(id, tokens);
                }
            }
            Ok(Msg::ReplaceDb(db, extras, replid, offset)) => {
                hub.db = *db;
                hub.apply_extras(*extras); // CDC offsets/log/groups + rebuilt indexes
                // Adopt the master's replication id + offset (Redis semantics) so
                // our INFO and future reconnects line up with the master's stream.
                hub.replid = replid;
                hub.master_repl_offset = offset;
                hub.master_link_up = true;
                // A replica that just loaded a full-sync snapshot may now be able
                // to satisfy readers parked on a blocking XREAD.
                hub.serve_blocked();
            }
            Ok(Msg::MasterLinkDown) => hub.master_link_up = false,
            Ok(Msg::AofRewriteDone(res)) => {
                // Fold the writes buffered during the rewrite into the new file,
                // then swap it in. On any failure we keep the old AOF (which still
                // holds those writes), so durability is never broken.
                let tail = hub.aof_rewrite_buf.take().unwrap_or_default();
                match (res, hub.aof_path.clone()) {
                    (Ok(tmp), Some(path)) => match aof::finalize_rewrite(&tmp, &path, &tail) {
                        Ok(()) => {
                            hub.aof = aof::Aof::open(&path).ok();
                            log::info("AOF rewrite complete");
                        }
                        Err(e) => {
                            let _ = std::fs::remove_file(&tmp);
                            log::error(&format!("AOF rewrite finalize failed: {e}"));
                        }
                    },
                    (Ok(tmp), None) => {
                        let _ = std::fs::remove_file(&tmp);
                    }
                    (Err(e), _) => log::error(&format!("AOF rewrite failed: {e}")),
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if SHUTDOWN.load(Ordering::Relaxed) {
                    // Drain anything already queued, then persist and exit.
                    while let Ok(msg) = rx.try_recv() {
                        if let Msg::Command { id, tokens } = msg {
                            hub.handle_command(id, tokens);
                        }
                    }
                    hub.persist_and_exit(true);
                }
                if let Some(a) = hub.aof.as_mut() {
                    a.maybe_fsync();
                }
                // Only the master actively expires keys; a replica waits for the
                // master's DELs, so the two never diverge on expiry timing.
                if hub.master.is_none() {
                    hub.db.active_expire();
                }
                hub.dirty_expired_watchers();
                hub.expire_blocked();
                hub.check_waits();
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
}

// === replica side: connect to a master and apply its stream =================

/// Parse `+FULLRESYNC <replid> <offset>` into (replid, offset). Best-effort.
fn parse_fullresync(line: &[u8]) -> (String, u64) {
    let s = String::from_utf8_lossy(line);
    let mut parts = s.trim_start_matches('+').split_whitespace();
    let _ = parts.next(); // "FULLRESYNC"
    let replid = parts.next().unwrap_or("").to_string();
    let offset = parts.next().and_then(|o| o.parse().ok()).unwrap_or(0);
    (replid, offset)
}

fn replica_sync(
    addr: String,
    masterauth: Option<Vec<u8>>,
    hub_tx: mpsc::Sender<Msg>,
    stop: Arc<AtomicBool>,
) {
    // Remembered (replid, processed-offset) so a reconnect can PSYNC for a partial
    // resync (CONTINUE) instead of pulling a full snapshot again.
    let mut known: Option<(String, u64)> = None;
    while !stop.load(Ordering::Relaxed) {
        if let Err(e) = try_sync(&addr, masterauth.as_deref(), &hub_tx, &stop, &mut known) {
            log::warn(&format!("replication: link to {addr} dropped: {e}"));
        }
        let _ = hub_tx.send(Msg::MasterLinkDown);
        // Reconnect after a short delay; the next try_sync will attempt a partial
        // resync using `known`.
        for _ in 0..10 {
            if stop.load(Ordering::Relaxed) {
                return;
            }
            thread::sleep(Duration::from_millis(100));
        }
    }
}

fn try_sync(
    addr: &str,
    masterauth: Option<&[u8]>,
    hub_tx: &mpsc::Sender<Msg>,
    stop: &Arc<AtomicBool>,
    known: &mut Option<(String, u64)>,
) -> io::Result<()> {
    let mut stream = TcpStream::connect(addr)?;
    // Bound the handshake + snapshot reads so a master that accepts the TCP
    // connection but never replies can't hang this thread forever (a stuck read
    // errors out, replica_sync retries, and REPLICAOF NO ONE can take effect).
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    // Handshake: AUTH (if the master needs a password) -> PING -> REPLCONF ->
    // PSYNC. AUTH must come first: a password-protected master rejects every
    // other command from an unauthenticated link.
    if let Some(pass) = masterauth {
        send_cmd(&mut stream, &[b"AUTH", pass])?;
        let reply = read_line(&mut stream)?;
        if reply.first() == Some(&b'-') {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!("master rejected AUTH: {}", String::from_utf8_lossy(&reply)),
            ));
        }
    }
    send_cmd(&mut stream, &[b"PING"])?;
    read_line(&mut stream)?;
    let myport = std::env::var("LOCUS_PORT").unwrap_or_else(|_| "6379".into());
    send_cmd(
        &mut stream,
        &[b"REPLCONF", b"listening-port", myport.as_bytes()],
    )?;
    read_line(&mut stream)?;
    // Request a partial resync when we have prior state, else a full resync.
    match known.as_ref() {
        Some((rid, off)) => send_cmd(
            &mut stream,
            &[b"PSYNC", rid.as_bytes(), off.to_string().as_bytes()],
        )?,
        None => send_cmd(&mut stream, &[b"PSYNC", b"?", b"-1"])?,
    }
    let resync = read_line(&mut stream)?; // +FULLRESYNC <id> <off> | +CONTINUE <id>
    let session_replid: String;
    let mut applied: u64;
    if resync.starts_with(b"+CONTINUE") {
        // Partial resync: keep our dataset; the master streams only what we missed.
        let line = String::from_utf8_lossy(&resync);
        let cont_id = line
            .trim_start_matches('+')
            .split_whitespace()
            .nth(1)
            .unwrap_or("");
        session_replid = if cont_id.is_empty() {
            known.as_ref().map(|(r, _)| r.clone()).unwrap_or_default()
        } else {
            cont_id.to_string()
        };
        applied = known.as_ref().map(|(_, o)| *o).unwrap_or(0);
        log::info(&format!(
            "replication: partial resync (CONTINUE) at offset {applied}"
        ));
    } else {
        // Full resync: replace the whole dataset from the snapshot.
        let (replid, offset) = parse_fullresync(&resync);
        let len = read_bulk_header(&mut stream)?;
        let mut snap = vec![0u8; len];
        stream.read_exact(&mut snap)?;
        let (db, extras) = rdb::deserialize_with_extras(&snap)?;
        if hub_tx
            .send(Msg::ReplaceDb(
                Box::new(db),
                Box::new(extras),
                replid.clone(),
                offset,
            ))
            .is_err()
        {
            return Ok(());
        }
        session_replid = replid;
        applied = offset;
        log::info(&format!("replication: full sync complete ({len} bytes)"));
    }
    *known = Some((session_replid.clone(), applied));

    // Stream and apply the master's writes. The read timeout lets us notice a
    // stop request even when the master is idle.
    stream.set_read_timeout(Some(Duration::from_millis(200)))?;
    let mut inbuf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        if stop.load(Ordering::Relaxed) {
            return Ok(());
        }
        match stream.read(&mut chunk) {
            Ok(0) => return Ok(()),
            Ok(n) => {
                inbuf.extend_from_slice(&chunk[..n]);
                loop {
                    match parse_command(&inbuf) {
                        Parsed::Complete(tokens, consumed) => {
                            inbuf.drain(0..consumed);
                            applied += consumed as u64;
                            if !tokens.is_empty()
                                && hub_tx
                                    .send(Msg::Command {
                                        id: MASTER_ID,
                                        tokens,
                                    })
                                    .is_err()
                            {
                                return Ok(());
                            }
                        }
                        Parsed::Incomplete => break,
                        Parsed::Error(_) => return Ok(()),
                    }
                }
                *known = Some((session_replid.clone(), applied));
                let off = applied.to_string();
                let _ = send_cmd(&mut stream, &[b"REPLCONF", b"ACK", off.as_bytes()]);
            }
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                // Idle: keep the master's view of our offset fresh (drives WAIT).
                let off = applied.to_string();
                let _ = send_cmd(&mut stream, &[b"REPLCONF", b"ACK", off.as_bytes()]);
            }
            Err(e) => return Err(e),
        }
    }
}

fn send_cmd(s: &mut TcpStream, parts: &[&[u8]]) -> io::Result<()> {
    let owned: Vec<Vec<u8>> = parts.iter().map(|p| p.to_vec()).collect();
    s.write_all(&resp::command(&owned))
}

fn read_line(s: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut line = Vec::new();
    let mut b = [0u8; 1];
    loop {
        s.read_exact(&mut b)?;
        if b[0] == b'\n' {
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            break;
        }
        line.push(b[0]);
    }
    Ok(line)
}

fn read_bulk_header(s: &mut TcpStream) -> io::Result<usize> {
    let line = read_line(s)?;
    if line.first() != Some(&b'$') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "expected bulk header",
        ));
    }
    std::str::from_utf8(&line[1..])
        .ok()
        .and_then(|x| x.parse::<usize>().ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad bulk length"))
}

// === per-connection: reader thread (here) + writer thread (spawned) =========

/// Outcome of feeding a read buffer through the parser, shared by the plaintext
/// and TLS connection handlers.
pub(crate) enum Dispatch {
    /// Parsed cleanly; `dispatched` is true if ≥1 command went to the hub.
    /// (Only the TLS handler reads it — to decide whether to await replies.)
    Ok {
        #[cfg_attr(not(feature = "tls"), allow(dead_code))]
        dispatched: bool,
    },
    /// A malformed frame — the caller should send these bytes and disconnect.
    ProtocolError(Vec<u8>),
    /// The hub channel is gone — disconnect.
    HubGone,
}

/// Parse every complete command in `inbuf`, forward each to the hub, and drain
/// what was consumed. O(batch), not O(batch^2), under heavy pipelining.
pub(crate) fn dispatch_commands(inbuf: &mut Vec<u8>, id: u64, tx: &mpsc::Sender<Msg>) -> Dispatch {
    let mut pos = 0;
    let mut dispatched = false;
    let result = loop {
        match parse_command(&inbuf[pos..]) {
            Parsed::Incomplete => break Dispatch::Ok { dispatched },
            Parsed::Error(msg) => {
                break Dispatch::ProtocolError(resp::error(&format!("ERR Protocol error: {msg}")));
            }
            Parsed::Complete(tokens, consumed) => {
                pos += consumed;
                if !tokens.is_empty() {
                    if tx.send(Msg::Command { id, tokens }).is_err() {
                        break Dispatch::HubGone;
                    }
                    dispatched = true;
                }
            }
        }
    };
    if pos > 0 {
        inbuf.drain(0..pos);
    }
    result
}

fn handle_conn(conn: TcpStream, id: u64, tx: mpsc::Sender<Msg>) -> io::Result<()> {
    let peer = conn.peer_addr()?;
    let is_loopback = peer.ip().is_loopback();
    // Commands/replies are small — disable Nagle for latency. An optional idle
    // read timeout (LOCUS_TIMEOUT) drops silent / slow-loris connections.
    let _ = conn.set_nodelay(true);
    if let Some(t) = idle_timeout() {
        let _ = conn.set_read_timeout(Some(t));
    }
    log::debug(&format!("client connected: {peer}"));

    let mut write_half = conn.try_clone()?;
    let (out_tx, out_rx) = mpsc::channel::<Vec<u8>>();
    let writer = thread::spawn(move || {
        while let Ok(bytes) = out_rx.recv() {
            if write_half.write_all(&bytes).is_err() {
                break;
            }
        }
    });

    if tx
        .send(Msg::Connect {
            id,
            out: out_tx.clone(),
            loopback: is_loopback,
        })
        .is_err()
    {
        return Ok(());
    }

    let mut conn = conn;
    let mut inbuf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let n = match conn.read(&mut chunk) {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        inbuf.extend_from_slice(&chunk[..n]);
        match dispatch_commands(&mut inbuf, id, &tx) {
            Dispatch::Ok { .. } => {}
            Dispatch::ProtocolError(e) => {
                let _ = out_tx.send(e);
                break;
            }
            Dispatch::HubGone => break,
        }
    }

    let _ = tx.send(Msg::Disconnect { id });
    drop(out_tx);
    let _ = writer.join();
    log::debug(&format!("client disconnected: {peer}"));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn min_arity_knows_commands_and_minimums() {
        assert_eq!(commands::min_arity(b"GET"), Some(2));
        assert_eq!(commands::min_arity(b"SET"), Some(3));
        assert_eq!(commands::min_arity(b"XADD"), Some(5));
        assert_eq!(commands::min_arity(b"PING"), Some(1));
        assert_eq!(commands::min_arity(b"AUTH"), Some(2));
        assert_eq!(commands::min_arity(b"NOTACOMMAND"), None);
    }

    #[test]
    fn ct_eq_matches_only_equal_slices() {
        assert!(ct_eq(b"secret", b"secret"));
        assert!(ct_eq(b"", b""));
        assert!(!ct_eq(b"secret", b"secrxt")); // same length, one byte differs
        assert!(!ct_eq(b"secret", b"secre")); // shorter
        assert!(!ct_eq(b"secret", b"secrets")); // longer
        assert!(!ct_eq(b"", b"x"));
    }

    #[test]
    fn write_modified_detects_noops() {
        // No-op replies -> not a modification.
        assert!(!write_modified(b"DEL", b":0\r\n"));
        assert!(!write_modified(b"SADD", b":0\r\n"));
        assert!(!write_modified(b"PERSIST", b":0\r\n"));
        assert!(!write_modified(b"ZADD", b":0\r\n"));
        assert!(!write_modified(b"ZADD", b"$-1\r\n")); // aborted INCR
        assert!(!write_modified(b"SET", b"$-1\r\n")); // NX/XX failed
        assert!(!write_modified(b"GETDEL", b"$-1\r\n"));
        // Real modifications.
        assert!(write_modified(b"DEL", b":1\r\n"));
        assert!(write_modified(b"SADD", b":2\r\n"));
        assert!(write_modified(b"SET", b"+OK\r\n"));
        assert!(write_modified(b"INCR", b":0\r\n")); // INCR result of 0 IS a change
        assert!(write_modified(b"APPEND", b":0\r\n")); // created empty key
        assert!(write_modified(b"HSET", b":0\r\n")); // overwrote existing field
    }

    #[test]
    fn parse_mem_handles_suffixes() {
        assert_eq!(parse_mem("0"), Some(0));
        assert_eq!(parse_mem("1024"), Some(1024));
        assert_eq!(parse_mem("1kb"), Some(1024));
        assert_eq!(parse_mem("2MB"), Some(2 * 1024 * 1024));
        assert_eq!(parse_mem("1gb"), Some(1024 * 1024 * 1024));
        assert_eq!(parse_mem("512b"), Some(512));
        assert_eq!(parse_mem(""), None);
        assert_eq!(parse_mem("notanumber"), None);
    }
}
