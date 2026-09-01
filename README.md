# Locus

**The reactive, geo-first datastore that speaks Redis.**

[![CI](https://github.com/elementaio/locus/actions/workflows/ci.yml/badge.svg)](https://github.com/elementaio/locus/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-2024-orange.svg)](https://www.rust-lang.org/)

Point any Redis client at Locus — then get what a vanilla Redis can't cleanly give you:

- a reliable, ordered **[changefeed](docs/CHANGEFEED.md)** — snapshot + live deltas, offsets, and
  at-least-once consumer groups with redelivery (keyspace notifications done right);
- **[geo-first](docs/GEO.md)** objects with `GEOSEARCH` and **live geofencing**;
- mergeable **[sketches](docs/SKETCHES.md)** — Bloom, HyperLogLog, Count-Min, Top-K, t-digest;
- atomic **CAS** write verbs and a drift-free **secondary index** (query by field).

**Why it can do this:** every command runs on a single hub thread, so Locus sees each mutation's
*ordered before/after at one point* — which makes a gap-free change-log and live queries **natural,
not bolted on**. It ships as one small static binary with **zero third-party dependencies** (just the
Rust standard library) — a real supply-chain and reproducibility win, but the *how*, not the pitch.

```console
$ redis-cli -p 6379 SET hello world          # …it's the Redis you already know,
OK
$ redis-cli -p 6379 CDCSUBSCRIBE app:         # …that also streams every change back to you,
$ redis-cli -p 6379 GEOSEARCH fleet FROMLONLAT 55.27 25.2 BYRADIUS 5 km ASC   # …and is geo-native.
```

> **Why not just Redis?** If you only need a cache or a KV store, use Redis — it's superb. Reach for
> Locus when you want the *reactive + spatial* layer (live change-streams, geofencing, sketches) in
> one dependency-free binary, with Redis-compatible wire access so your existing clients and tooling
> just work.

> **Status:** pre-1.0, actively hardening toward production. **Done:** AUTH + ACL + protected-mode,
> durable persistence (crash-tested), the full reactive/geo differentiator set, broad driver/ops
> compatibility (`SCAN`, `INFO`, `redis_exporter`, RESP3), correct replication (`WAIT`, no expiry
> divergence, **partial-resync** on reconnect), **automatic failover** (built-in sentinel), **TLS**
> (sidecar, or in-process via the optional `tls` feature), **horizontal spatial clustering**
> (cell-in-key sharding, bounded cross-shard `GEOSEARCH`, live resharding, per-shard failover, a global
> HLC-ordered changefeed), a **disk tier** for archives, **work queues** (blocking pops), and a
> three-reviewer adversarial hardening pass with every finding fixed and regression-tested. The
> **default build stays 100% dependency-free** — the `tls` feature is the only thing that pulls a
> crate, and only when you ask. ~18k lines of `std`-only Rust.

---

## Features

**Redis-compatible core**

- **Data types:** strings, lists, hashes, sets, sorted sets, streams, bitmaps — broad per-type command
  coverage with `WRONGTYPE` checks. ~200 commands; see [docs/COMMANDS.md](docs/COMMANDS.md).
- **Iteration & introspection:** real incremental `SCAN`/`HSCAN`/`SSCAN`/`ZSCAN`, `COMMAND`/`COMMAND
  DOCS`, `OBJECT ENCODING`, `CLIENT`, `GETEX` — off-the-shelf clients connect without fallbacks.
- **Key expiration:** `SET … EX/PX/EXAT/PXAT/NX/XX/KEEPTTL`, `EXPIRE`/`TTL`/`PERSIST`, passive + active.
- **`maxmemory` + eviction:** soft cap with key eviction and `OOM` rejection.
- **Transactions:** `MULTI`/`EXEC`/`DISCARD`, `WATCH`/`UNWATCH` (EXECABORT + WATCH-on-expiry).
- **Streams:** `XADD`/`XRANGE`/`XREAD`, including **blocking `XREAD`**.
- **Work queues:** **blocking pops** — `BLPOP`/`BRPOP`/`BLMOVE`/`BZPOPMIN`/`BZPOPMAX` (+ `LMPOP`/`ZMPOP`).
- **Protocol:** RESP2 **and RESP3** typed replies (maps/sets/doubles) + **push frames** for pub/sub on
  `HELLO 3`; pipelining.

**Security & operations** *(safe on a trusted network)*

- **AUTH + ACL:** `requirepass`, **protected mode** (no accidental `0.0.0.0` exposure), and a simple
  **multi-user ACL** (`ACL SETUSER` with command classes, key prefixes, and **pub/sub channel
  patterns**) — least-privilege users, with real revocation: `ACL DELUSER` / `SETUSER … off` closes
  that identity's live sessions.
- **TLS:** a sidecar (zero-dep default), or **in-process** via the optional `tls` build feature
  (rustls) — the default build pulls in nothing. See the TLS note below.
- **Observability:** a full `INFO` (works with `redis_exporter`), `SLOWLOG`, `CONFIG GET/SET`,
  structured leveled logging, graceful `SIGTERM` shutdown (drain → fsync → final save).
- **Crash containment:** every command runs inside a panic boundary, so a bug in one command costs
  that one command (`-ERR internal error`, counted in `INFO` as `panics_recovered`) instead of the
  whole server.
- **Resource safety:** per-connection read timeout, `TCP_NODELAY`, a max-connections cap.

**Durability**

- **Snapshots + AOF:** RDB-style binary snapshots and an append-only file with crash-safe,
  torn-tail-tolerant replay, configurable `appendfsync`, and `BGREWRITEAOF` compaction.
  Directory-fsync'd renames; **fuzz- and `kill -9` crash-recovery-tested.**
- **Automatic snapshot cadence:** Redis-style save points (`LOCUS_SAVE`, on by default) fire a
  `BGSAVE` on their own, so a crash without the AOF costs one window — not everything since the last
  manual `SAVE`.
- **Checksummed snapshots:** every snapshot carries a CRC-32 footer. Bit-rot is *refused* at startup
  with a clear error instead of loading as data; pre-0.8.0 snapshots still open.
- **`appendfsync` that tells the truth:** `everysec` fsyncs on a dedicated thread, never stalling the
  hub; `always` returns `-MISCONF` for a write whose fsync failed rather than acking it `+OK`.
- **`BGSAVE` is honest about its cost:** the write+fsync is off-thread, but serialization holds the hub
  (`fork()` is unsafe with 2N+ threads). The stall is measured and published as
  `rdb_last_bgsave_hub_stall_us` — and the answer at scale is
  [snapshotting on a replica](docs/DEPLOYMENT.md#backing-up-from-a-replica-recommended-at-scale).
- **Disk tier — "RAM for live data, NVMe for archives":** `TIER key` moves a value to an on-disk
  value-log, leaving a ~100-byte stub; any read **thaws it back transparently** (API unchanged). TTL,
  RDB/AOF, and replication all keep working. Turns "keep 30 days of history" from a RAM bill into a
  disk one (`LOCUS_TIER`).

**Replication & high availability**

- `REPLICAOF` master/replica: full-sync snapshot + live command streaming, read-only replicas, real
  replication IDs + offsets, authenticated links (`masterauth`), and **`WAIT`** for ack-based
  durability. Expiry is master-authoritative, so replicas never diverge on timing. A briefly-dropped
  replica reconnects with a **partial resync** (`PSYNC` `CONTINUE` over a backlog ring) — no full
  snapshot when it only missed a little.
- **Automatic failover:** the same binary runs as a built-in **sentinel** (`LOCUS_SENTINEL`) that
  promotes the most up-to-date replica when the master dies and repoints the rest — no external
  orchestrator; run several sentinels for quorum-based agreement. It is built for a **trusted
  network** and is **not partition-safe** — see
  [High availability](#high-availability--automatic-failover) for exactly what it does and does not
  guarantee.

**Reactive + geo differentiators**

- **[Changefeed](docs/CHANGEFEED.md):** `CDCSUBSCRIBE` (snapshot + live deltas, no gap/dup), offsets +
  `CDCREAD` catch-up, and **at-least-once** consumer groups — a delivered record stays pending until it
  is acked, and if its consumer dies another one recovers it (`CDCREADGROUP … 0` under the same name,
  `CDCCLAIM`/`CDCAUTOCLAIM` from a different one). A reliable, ordered keyspace feed: the group's
  existence is logged to the AOF and replicated, so it survives a `kill -9` and a failover, and its
  pending list is written into the snapshot and handed to a replica on sync.
- **[Geo-first](docs/GEO.md):** `GEOSET`/`GEOPOS`/`GEODIST`/`GEOSEARCH` (backed by a **geohash spatial
  index** → sub-linear radius/box queries, and `COUNT n` is a true **nearest-neighbour search** that
  stops as soon as it has the n closest — 0.08 ms for a top-10 at 20 km over 200k points) with
  **combined attribute filters** (`GEOSEARCH … WHERE status active`), plus **live geofencing** via
  `CDCSUBSCRIBE REGION`.
- **[Sketches](docs/SKETCHES.md):** Bloom (dedup), HyperLogLog (distinct counts), Count-Min
  (trending), Top-K (heavy hitters), t-digest (live percentiles).
- **CAS verbs:** `CAS`/`CADEL`/`SETMAX`/`INCRCAP` — atomic check-and-write.
- **Secondary index:** `IDXCREATE`/`IDXGET`/`IDXRANGE` — query by hash field, auto-maintained (no drift).

**Zero dependencies.** Pure `std`; one small static binary; reproducible builds.

---

## What do you build with it?

The shapes Locus is good at, each one or two commands deep:

| Job | How |
|---|---|
| **Work queue / background jobs** | producers `RPUSH`, workers `BLPOP` (blocking, FIFO-fair across workers); `BLMOVE` for the reliable-queue pattern; `LMPOP` for batch drains |
| **Cache / sessions** | `SET … EX`, `GETEX`, `maxmemory` + eviction — the classic role, minus a second moving part |
| **Rate limits & quotas** | `INCRCAP` (atomic increment-with-cap: one verb, no script), `CAS` for optimistic writes, `SETNX + EX` for idempotency keys and locks |
| **Live dashboards / sync** | `CDCSUBSCRIBE prefix` — snapshot **then** every change, gap-free with offsets and consumer groups; UIs stop polling |
| **Work queues over the keyspace** | `CDCGROUP` + `CDCREADGROUP` fan a change feed across N workers, at-least-once: `CDCAUTOCLAIM` recovers whatever a dead worker was holding |
| **Fleet / delivery / anything moving** | `GEOSET` with attributes, `GEOSEARCH … WHERE status active`, live geofences via `CDCSUBSCRIBE REGION` |
| **Analytics counters** | `PFADD`/`PFCOUNT` daily uniques in 16 KB, `TOPKADD` trending, `CMSINCRBY` frequencies, `TDADD`/`TDQUANTILE` live p99s — all mergeable across shards/days |
| **Dedup** | `BFADD` — "have I seen this id?" in constant memory |
| **Query-by-field** | `IDXCREATE`/`IDXGET` — find hashes by a field's value without maintaining your own reverse sets |
| **Archives on a budget** | finish a record → `TIER` it to the value-log; reads thaw transparently; RAM holds only the live working set |

One binary covers the cache, the queue, the pub/sub bus, the geo index, and the analytics
counters — the usual "Redis + a queue + a tile server + a metrics store" sprawl, without the sprawl.

For **Node**, the [`locusdb`](clients/node) npm client adds typed verbs and the reactive
changefeed/geofence as events (`feed.on('change', …)`, `fence.on('enter', …)`) — the push API a stock
driver can't surface. See [docs/COMMANDS.md](docs/COMMANDS.md) for the full reference,
[docs/CLIENTS.md](docs/CLIENTS.md) for driving Locus from Node/Python (any Redis client works), the guides above for the differentiators,
[docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) for running it in production (TLS, persistence, failover), and
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for how it works inside.

---

## Quick start

Requires a recent Rust toolchain (edition 2024). The official `redis-cli` / `redis-benchmark` are handy
for driving it (`brew install redis` on macOS) but not required to build.

```console
cargo run                 # debug, listens on 127.0.0.1:6379
cargo run --release       # optimized

redis-cli -p 6379 ping
redis-cli -p 6379 zadd board 100 alice 50 bob
redis-cli -p 6379 zrange board 0 -1 withscores

cargo test                # unit + end-to-end integration tests
```

### Install (Docker / prebuilt binary)

```console
# Docker — RESP on 6379. Set a password: protected mode (on by default) refuses
# non-loopback clients without one, and Docker connections are non-loopback.
docker run -p 6379:6379 -e LOCUS_REQUIREPASS=change-me ghcr.io/elementaio/locus:latest
# persist across restarts:
docker run -p 6379:6379 -e LOCUS_REQUIREPASS=change-me \
  -v locus-data:/data -e LOCUS_RDB=/data/locus.rdb ghcr.io/elementaio/locus:latest
# then: redis-cli -a change-me PING
```

(For a throwaway on a trusted network you can `-e LOCUS_PROTECTED_MODE=no` instead —
but prefer the password; it's one flag.)

Or grab a prebuilt static binary from the [latest release](https://github.com/elementaio/locus/releases/latest)
(Linux x86_64/aarch64, macOS x86_64/aarch64). With a Rust toolchain, install from crates.io (the crate
is `locusdb`; the installed command is `locus`):

```console
cargo install locusdb && locus
```

### Embedding Locus as a library

The package ships two targets: the `locus` **binary** (the server) and the `locusdb` **library** (the
engine). The engine is the keyspace, the command implementations, the RESP codec, the persistence
formats, the spatial index and the sketches — everything except the server. Commands go in as argument
vectors and replies come back as encoded RESP bytes, the same bytes the server would put on a socket,
so anything that can drive a Redis client can drive Locus in-process: no socket, no threads, no server.

```rust
use locusdb::{Db, execute, resp};

let mut db = Db::new();

execute(&[b"GEOSET".to_vec(), b"driver:7".to_vec(), b"13.36".to_vec(), b"38.11".to_vec()], &mut db);
let near = execute(
    &[b"GEOSEARCH".to_vec(), b"FROMLONLAT".to_vec(), b"13.36".to_vec(), b"38.11".to_vec(),
      b"BYRADIUS".to_vec(), b"5".to_vec(), b"km".to_vec(), b"ASC".to_vec()],
    &mut db,
);
assert_eq!(near, resp::bulk_array(&[b"driver:7".to_vec()]));
```

`execute` answers in RESP2; `execute_proto(tokens, &mut db, 3)` selects RESP3 for the shape-sensitive
commands (maps, sets, doubles). `Db` is a plain owned value with `&mut self` methods and **no interior
locking** — the server does not need any, because one thread owns the keyspace by design — so an
embedder sharing a `Db` across threads supplies its own mutual exclusion. Commands that only mean
something to a *server* — replication, `SUBSCRIBE`, blocking pops, the changefeed's group plumbing —
are handled by the binary's hub, not by `execute`. Expiry is lazy on read plus an active sweep the
server drives from its maintenance tick, so an embedder that wants keys to actually disappear calls
`Db::active_expire()` periodically.

`tests/embedding.rs` in the repository is a working example.

### Configuration

Configured entirely through environment variables (minimal config by design):

| Variable | Default | Meaning |
|---|---|---|
| `LOCUS_BIND` | `127.0.0.1` | Interface to bind. Loopback by default; the Docker image sets `0.0.0.0` (protected mode then guards it until a password is set) |
| `LOCUS_PORT` | `6379` | TCP port |
| `LOCUS_REQUIREPASS` | _(off)_ | Require `AUTH <password>` before any command |
| `LOCUS_MASTERAUTH` | _(off)_ | Password a replica presents to its master |
| `LOCUS_PROTECTED_MODE` | `on` | Refuse non-loopback clients when no password is set; `no` to disable |
| `LOCUS_MAXCLIENTS` | `10000` | Max concurrent connections |
| `LOCUS_TIMEOUT` | `0` | Idle-connection timeout in seconds (`0` = off) |
| `LOCUS_RDB` | `locus.rdb` | RDB snapshot path |
| `LOCUS_SAVE` | `3600 1 300 100 60 10000` | Automatic snapshot cadence — `<seconds> <changes>` pairs, Redis's `save`. A `BGSAVE` fires when any pair is met. `""` (or `no`) disables it; `CONFIG SET save "…"` retunes it live |
| `LOCUS_AOF` | _(off)_ | Path (or `1`) to enable append-only persistence |
| `LOCUS_APPENDFSYNC` | `everysec` | AOF fsync policy: `always` / `everysec` / `no` |
| `LOCUS_AOF_ON_WRITE_ERROR` | `stop` | On a failed AOF append/fsync, reject writes until a recovery rewrite succeeds; `continue` to keep serving (durability at risk) |
| `LOCUS_AOF_LOAD_TRUNCATED` | `no` | `yes` loads everything up to a mid-file corruption instead of refusing to start (a torn tail is always tolerated) |
| `LOCUS_MAXMEMORY` | _(unlimited)_ | Soft cap; `kb`/`mb`/`gb` (e.g. `256mb`). Master evicts; `OOM` if still over |
| `LOCUS_TIER` | _(off)_ | Disk tier: path (or `1` = beside the RDB). `TIER key` moves a value to an on-disk value-log, leaving a stub; reads thaw it back — RAM for live data, NVMe for archives |
| `LOCUS_TIER_SEG_MB` | `512` | Value-log segment size; segments are immutable and deleted whole when their last live entry dies |
| `LOCUS_OUTBUF_NORMAL` / `_REPLICA` / `_PUBSUB` | `0` / `256mb` / `32mb` | Per-client output-buffer cap; a client over its cap is disconnected (slow-consumer OOM guard) |
| `LOCUS_QUERYBUF_LIMIT` | `1gb` | Max bytes a connection may buffer assembling one command (pre-`AUTH` memory guard) |
| `LOCUS_HUB_QUEUE` | `65536` | Bounded hub input queue — a pipelining flood backpressures its reader instead of growing memory |
| `LOCUS_CDC_MAXLEN` | _(off)_ | Retained changefeed log size for `CDCREAD` catch-up / consumer groups |
| `LOCUS_CDC_MAXBYTES` | `64mb` | Byte cap on the retained changefeed log (counts toward `used_memory`) |
| `LOCUS_CDC_PEL_MAX` | `100000` | Per-group pending-entries cap (a never-acking consumer can't grow memory unbounded) |
| `LOCUS_SLOWLOG_US` | `10000` | Log commands slower than this (µs); `<0` disables |
| `LOCUS_SLOWLOG_MAXLEN` | `128` | Max entries retained in the `SLOWLOG` ring |
| `LOCUS_LOGLEVEL` | `info` | `error` / `warn` / `info` / `debug` |
| `LOCUS_REPLICAOF` | _(off)_ | Boot as a replica of `host port` / `host:port`; else the persisted role resumes |
| `LOCUS_ROLE_FILE` | `<rdb>.role` | Where the node's role + config epoch persist across restarts |
| `LOCUS_CLUSTER_ENABLED` | `off` | Enable cluster routing (`MOVED`/`CROSSSLOT`) |
| `LOCUS_CLUSTER_ANNOUNCE` | `LOCUS_BIND:PORT` | This node's address in the cluster |
| `LOCUS_CLUSTER_NODES` | _(self owns all)_ | Topology: `host:port 0-5460;host:port 5461-10922;…` |
| `LOCUS_CLUSTER_STATE` | `<rdb>.cluster` | Where runtime slot ownership persists (survives a full-cluster restart) |
| `LOCUS_CLUSTER_SECRET` | _(off)_ | Shared secret internal cluster RPCs present; lets `requirepass` and clustering coexist |
| `LOCUS_CLUSTER_ALLOW_PARTIAL` | `no` | `yes` lets a cross-shard `GEOSEARCH` return partial results when a shard is down (else `CLUSTERDOWN`) |
| `LOCUS_CLUSTER_CELL_BITS` | `0` (off) | Cell-in-key spatial sharding: bits of geohash per cell; >0 makes `GEOSEARCH` a bounded scatter (`CLUSTER CELL` gives the tag) |
| `LOCUS_CLUSTER_GOSSIP_MS` | `1000` | Topology anti-entropy interval — how often a node pulls peers' slot maps to converge on ownership changes |
| `LOCUS_CDC_PEER_TIMEOUT_MS` | `30000` | How long a down shard holds the `CLUSTER CDCMERGE` watermark before it's released |
| `LOCUS_NODE_ID` | _(derived)_ | This node's id (0–255) for globally-unique changefeed stamps; derived from the announce address if unset |

### Security & replication in 30 seconds

```console
# require a password
LOCUS_REQUIREPASS=s3cret cargo run --release
redis-cli -p 6379 -a s3cret ping

# a least-privilege, read-only user scoped to app:* keys
redis-cli -p 6379 -a s3cret ACL SETUSER reader on '>pw' +@read '~app:' '&app:*'

# master + replica, then WAIT for the write to reach 1 replica
redis-cli -p 6380 replicaof 127.0.0.1 6379
redis-cli -p 6379 set foo bar
redis-cli -p 6379 wait 1 1000        # -> (integer) 1
```

> **TLS:** two options. (1) The **zero-dependency default**: run Locus behind a TLS proxy/sidecar
> (stunnel, ghostunnel, nginx `stream`) — see [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md). (2) **In-process
> TLS** via the optional build feature (keeps the default build dependency-free):
>
> ```console
> cargo build --release --features tls
> LOCUS_TLS_PORT=6380 LOCUS_TLS_CERT=server.crt LOCUS_TLS_KEY=server.key \
>   LOCUS_REQUIREPASS=$PW target/release/locus      # plaintext on 6379 (loopback) + TLS on 6380
> redis-cli --tls -p 6380 -a $PW ping
> ```
>
> The `tls` feature uses rustls (pure-Rust, `ring` provider — no OpenSSL/C); the default build pulls in
> nothing.

### High availability — automatic failover

The same `locus` binary runs as a lightweight **sentinel** (set `LOCUS_SENTINEL`) that monitors a
master and, if it dies, automatically promotes the most up-to-date replica and repoints the others —
no external orchestrator required. While the master is healthy it also reconciles stray nodes (e.g. a
returned old master) back to replicas, narrowing the split-brain window.

> **What this is, and what it is not.** Failover here is an *orchestration hook* for a trusted
> network, not a consensus protocol. Read
> [the guarantees](#what-failover-guarantees--and-what-it-does-not) before you depend on it.

```console
# monitor a master + its replicas; promote on failure
LOCUS_SENTINEL=127.0.0.1:6379 \
LOCUS_SENTINEL_REPLICAS=127.0.0.1:6380,127.0.0.1:6381 \
LOCUS_SENTINEL_DOWN_AFTER_MS=5000 \
  cargo run --release
```

| Variable | Default | Meaning |
|---|---|---|
| `LOCUS_SENTINEL` | _(off)_ | Master `host:port` to monitor — **enables sentinel mode** for this process |
| `LOCUS_SENTINEL_REPLICAS` | _(empty)_ | Comma-separated replica `host:port` list |
| `LOCUS_SENTINEL_AUTH` | _(off)_ | Password presented to the monitored nodes |
| `LOCUS_SENTINEL_DOWN_AFTER_MS` | `5000` | How long the master must be unreachable before failover |
| `LOCUS_SENTINEL_INTERVAL_MS` | `1000` | Health-check poll interval |
| `LOCUS_SENTINEL_QUORUM` | `1` | Replicas that must *also* report the master link down before failover (corroboration; keep ≤ replica count) |
| `LOCUS_SENTINEL_PORT` | _(off)_ | Listen port for peer-sentinel agreement (enables multi-sentinel mode) |
| `LOCUS_SENTINEL_PEERS` | _(empty)_ | Comma-separated peer sentinel `host:port` list |
| `LOCUS_SENTINEL_PEER_SECRET` | _(off)_ | Shared secret required on **every** peer verb. **Mandatory** whenever `LOCUS_SENTINEL_PORT` or `LOCUS_SENTINEL_PEERS` is set — the sentinel refuses to start without it |
| `LOCUS_SENTINEL_PEER_BIND` | `127.0.0.1` | Address the peer listener binds. Loopback by default; widen it deliberately |
| `LOCUS_SENTINEL_ID` | `127.0.0.1:PORT` | This sentinel's id for leader election |
| `LOCUS_SENTINEL_STATE` | _(off)_ | File to persist the current `(master, epoch)` decision so a restart doesn't revert to the env master |

Before promoting, the sentinel requires **corroboration** — a quorum of replicas must also report their
master link down — so a sentinel merely partitioned from the master won't trigger a needless failover.

**Run several sentinels for HA** (so failover survives a sentinel dying): give each a `LOCUS_SENTINEL_PORT`
and list the others in `LOCUS_SENTINEL_PEERS`. A failover then also needs a **majority of sentinels** to
agree the master is down, and only the **leader** (lowest id among the down-seeing sentinels) performs
the promotion — the majority gate stops a partitioned *minority*, the leader rule stops two sentinels
promoting different replicas in the same round. (Bully-style election over a tiny line protocol — not
Raft, and not an equivalent of it.)

The sentinels talk over a small **authenticated** control plane: give every sentinel the same
`LOCUS_SENTINEL_PEER_SECRET`, which is required on every verb. It listens on **loopback only** unless
you set `LOCUS_SENTINEL_PEER_BIND`, and a sentinel configured with peers but no secret **refuses to
start**. The secret is a shared bearer token over a cleartext line protocol: it stops a stranger from
driving the control plane, but it is not channel-bound, so put the peer plane on a trusted network or
inside a tunnel (WireGuard, stunnel, a service mesh) — never on the open internet.

```console
# sentinel A (run B symmetrically with PORT/PEERS swapped)
LOCUS_SENTINEL=master:6379 LOCUS_SENTINEL_REPLICAS=r1:6379,r2:6379 \
LOCUS_SENTINEL_PORT=26379 LOCUS_SENTINEL_PEERS=sentinelB:26379 \
LOCUS_SENTINEL_PEER_SECRET=$PEER_SECRET \
  cargo run --release
```

#### What failover guarantees — and what it does not

Stated plainly, because the difference matters when you are choosing what to run this on.

**It does:** detect a dead master and promote the most up-to-date *reachable* replica automatically;
require replica corroboration and a sentinel majority first, so a single partitioned sentinel does not
act alone; stamp every promotion with a config epoch that data nodes use to reject a stale sentinel's
`REPLICAOF`; and reconcile a returned old master back to being a replica.

**It does not:**

- **It is not partition-safe.** The two gates narrow the window; they do not close it. An
  *asymmetric* partition — sentinels that can reach each other but not the master, while the master is
  still reachable by clients — can still produce a **double promotion**.
- **A partitioned old master is never fenced.** It keeps accepting writes while cut off, and those
  writes are **silently discarded** when it is reconciled back to a replica. Fence it at the network
  or orchestrator layer if that loss is unacceptable.
- **Replication is asynchronous**, so an unacknowledged write can be lost on promotion. Use `WAIT` to
  bound that window.
- **Epochs are wall-clock HLC stamps**, not coordinated consensus numbers; large clock skew across
  nodes can invert an ordering.

If you need partition-safety, run failover from an orchestrator that provides it (a Kubernetes
operator, Consul, etcd) and use the sentinel's building blocks — `REPLICAOF`, config epochs, `WAIT`,
`CLUSTER REASSIGN` — rather than its automatic mode.

See [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) for the full HA topology.

---

## Architecture

```
        ┌── reader thread ──┐                          ┌─────────────────────────┐
client ─┤  parse RESP       │── command ──▶  channel ──▶│      hub (1 thread)     │
        │                   │                           │  • keyspace (the data)  │
        └── writer thread ◀─┘◀── reply/message ─ channel │  • pub/sub + changefeed │
                                                         │  • replication state    │
                                                         │  • transactions / ACL   │
                                                         └─────────────────────────┘
```

A single **hub thread** owns all mutable state and runs every command serially — atomicity comes from
the architecture, not from locks — and, crucially, it observes every mutation at one ordered point,
which is what makes the reliable changefeed and live geo-queries possible. Each connection gets a
**reader** and **writer** thread; persistence and replication sit **off the hot path**. Full details in
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

That box splits cleanly in two, and so does the crate: everything *inside* the hub — the keyspace, the
command implementations, the codec, persistence, the spatial index — is the `locusdb` **library**;
everything *around* it — the hub thread itself, the reader/writer threads, replication, cluster and
sentinel — is the `locus` **binary**. See [Embedding Locus as a library](#embedding-locus-as-a-library).

---

## Performance

Locus prioritizes **clarity and predictable single-threaded semantics** over peak throughput. The
numbers below come from the harness in the repository, so you can reproduce them on your own machine
in one command — against a real `redis-server` if you have one installed:

```console
cargo test --release --test perf -- --ignored --nocapture
```

Release build, both servers on one 8-core machine, `redis-server 8.8.0` for comparison:

| Operation | Locus | Redis 8.8 | Gap |
|---|---:|---:|---:|
| `SET` / `GET`, 50 connections | 69k / 81k ops/s | 99k / 110k ops/s | 1.3–1.4× |
| `SET` → 200k separate keys (pipelined) | 132k ops/s | 596k ops/s | 4.5× |
| `SADD` → 200k members of one set | 131k ops/s | 609k ops/s | 4.6× |
| `ZADD` → 200k members of one sorted set | 91k ops/s | 428k ops/s | 4.7× |
| `HSET` → 200k fields of one hash | 139k ops/s | 526k ops/s | 3.8× |
| `RPUSH` → a 500k-element list | 157k ops/s | 815k ops/s | 5.2× |
| `ZRANGE key 0 9` on a 200k sorted set | 11.6k ops/s · 0.087 ms | 18.7k ops/s | 1.6× |
| `ZRANGEBYSCORE` on a 200k sorted set | 11.1k ops/s · 0.090 ms | 18.9k ops/s | 1.7× |
| `GEOSET` ingest, 200k points | 102k ops/s | 219k ops/s | 2.1× |
| `GEOSEARCH` 1 km `COUNT 10` | 0.34 ms | 0.19 ms | 1.8× |
| `GEOSEARCH` 20 km `COUNT 10` | 0.082 ms | 57 ms | **696× faster** |

Writing into a large collection costs the same as writing a fresh key (131k vs 132k ops/s above) — the
per-write work does not grow with the collection, which is what the harness's floor assertions pin
down. Run-to-run spread on a desktop is ±10–25%; treat the ratios, not the absolute figures, as the
signal.

Sorted-set range reads were the last large gap and are now closed. Until 0.7.0 `ZRANGE` and
`ZRANGEBYSCORE` materialized the entire sorted set before slicing it, so returning ten members from a
200k-member zset cost ~38 ms — and on a single-hub design that is a stall for every client, not just
the caller, which capped the whole server at about 26 such queries per second. The read path now walks
the ordered index that was already being maintained on every write: the same query costs 0.087 ms, a
446× improvement that brings it to 1.6× Redis. Replies are byte-for-byte unchanged, checked by
replaying an 11,658-command corpus against both the old and new binaries and diffing the raw protocol
output. `ZRANK` is the remaining O(n) read; it is O(rank) rather than O(log n), which matters only for
high-ranked members of a large set.

`GEOSEARCH` was the other single-hub stall, and 0.9.0 closes it. The spatial index chose one cell
coarse enough that the query box spanned at most four of them, which made every cell 2–3× wider than
the query and, on dense data at a large radius, made a *single* cell swallow the whole dataset — the
index quietly degenerated into a full scan, 181 ms of hub time for a top-10 at 20 km. The cover is now
up to 64 fine cells (each an `O(log n)` seek) with longitude carrying the extra bit it needs, and
`COUNT n` became a real nearest-neighbour search: since every point inside a circle of radius ρ is
nearer than every point outside it, the query probes outward — ρ = r/64, r/8, then the full shape — and
stops at the first radius that already holds n matches. The 20 km top-10 went from 181 ms to 0.082 ms,
and no longer costs more than the 1 km one. A query with **no** `COUNT` still returns every match and
therefore still costs what its own result costs (103,450 members in ~190 ms) — bound wide searches with
`COUNT`.

Throughput is otherwise bounded by the single-hub design (one channel hop per command) — the deliberate
price of lock-free, serially-consistent execution, and the very property (one ordered point) that makes
the changefeed and live geo-queries possible. The path to more is **horizontal** — spatial sharding
across nodes (P6), each shard its own single-threaded hub — rather than threading the hub itself.

---

## Project status & roadmap

**Production-readiness so far:** safe on a trusted network (AUTH/ACL/protected-mode/limits), durable
(async snapshots, AOF + crash-recovery, persisted/replicated reactive state), observable
(`INFO`/`SLOWLOG`/`redis_exporter`), driver-compatible (`SCAN`/`COMMAND`/`CONFIG`/RESP3 incl. pub/sub
push), with correct replication (real offsets, `WAIT`, partial-resync, no expiry divergence) and
**automatic failover** (built-in sentinel — for a trusted network; not partition-safe, see
[the guarantees](#what-failover-guarantees--and-what-it-does-not)) — plus the reactive/geo
differentiator set, now with a **geohash-indexed `GEOSEARCH` + `WHERE` filters**, ordered-index sorted
sets, and a CRC16 routing seam.

**Shipped — the flagship milestone:** horizontal **spatial clustering** (P6), Locus's flagship lane.
It includes: **static hash-slot routing** (`MOVED`/`CROSSSLOT`, `CLUSTER SLOTS/NODES/KEYSLOT`), the
**inter-node transport** layer (cluster-wide `DBSIZE`), **cross-shard scatter-gather `GEOSEARCH`** (one
global result merged by distance), and **cell-in-key spatial sharding** — name geo keys `{cell}id`
(`cell` from `CLUSTER CELL lon lat`) so a region co-locates on one shard, and `GEOSEARCH` becomes a
**bounded** scatter that only consults the shards whose cells the query covers — the Tile38-beating lane.
Resharding is **live and zero-loss**: `CLUSTER MIGRATESLOT slot dst` copies a slot's keys to another node
(two-phase — copy-all then commit), and `CLUSTER SETSLOT slot NODE addr` repoints ownership at runtime
(`CLUSTERDOWN` covers an unowned slot). Topology changes **converge automatically** — each is stamped with
an HLC epoch and a background **anti-entropy gossip** (`LOCUS_CLUSTER_GOSSIP_MS`) pulls peers' maps and
adopts the higher epoch, so a change made on one node reaches the rest without pushing to each. **Per-shard
failover** reuses the built-in sentinel: set
`LOCUS_SENTINEL_CLUSTER_NODES` and, when a shard's master dies, the sentinel promotes its replica and
broadcasts `CLUSTER REASSIGN old new` so the cluster routes the dead master's slots to the successor. And
the changefeed goes **cross-shard**: every change is stamped with a hybrid logical clock, and `CLUSTER
CDCMERGE` merges all shards' feeds into one **global, HLC-ordered** stream with a watermark that bounds
staleness. Thread-per-core, replica chaining, and numbered multi-DB are explicit non-goals (the first two
fold into clustering; prefer key-prefix namespacing over multi-DB).

**Explicit non-goals:** scripting/`EVAL`, an embedded HTTP `/metrics` endpoint (`INFO` + `redis_exporter`
instead), and active-active replication.

---

## Building & testing

```console
cargo build --release      # optimized binary at target/release/locus (zero dependencies)
cargo build --release --features tls   # opt-in: in-process TLS via rustls
cargo build --lib          # just the embeddable engine (the `locusdb` library target)
cargo test                 # unit + integration (parser fuzz, crash-recovery, replication, ACL, …)
cargo test --features tls  # also runs the TLS handshake / round-trip tests
cargo clippy               # lints (clippy-clean under -D warnings)
cargo fmt                  # formatting

# the performance harness — #[ignore]d, so it never runs in the normal suite.
# Spawns a server, drives it over a raw socket, and prints the table below;
# it spawns a redis-server too when the machine has one, and skips that
# column cleanly when it doesn't.
cargo test --release --test perf -- --ignored --nocapture
```

## Contributing

Contributions are welcome — see [CONTRIBUTING.md](CONTRIBUTING.md). The codebase is intentionally small
and readable; a new command is generally one match arm plus a focused function and a test.

## License

[MIT](LICENSE) © 2026 Emad Jumaah.

## Acknowledgements

Locus is a study in, and homage to, the elegance of **Redis** and Salvatore Sanfilippo's (antirez)
design philosophy: simplicity as a feature, single-threaded determinism, and data structures as a
service. It is an independent implementation and is not affiliated with or endorsed by Redis Ltd.
