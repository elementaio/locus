# Locus

**The reactive, geo-first datastore that speaks Redis.**

[![CI](https://github.com/intenttext/locus/actions/workflows/ci.yml/badge.svg)](https://github.com/intenttext/locus/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-2024-orange.svg)](https://www.rust-lang.org/)

Point any Redis client at Locus — then get what a vanilla Redis can't cleanly give you:

- a reliable, ordered **[changefeed](docs/CHANGEFEED.md)** — snapshot + live deltas, offsets, consumer
  groups (keyspace notifications done right);
- **[geo-first](docs/GEO.md)** objects with `GEOSEARCH` and **live geofencing**;
- mergeable **[sketches](docs/SKETCHES.md)** — Bloom, Count-Min, Top-K, t-digest;
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
> divergence, **partial-resync** on reconnect), **automatic failover** (built-in sentinel), and **TLS**
> (sidecar, or in-process via the optional `tls` feature). **Not yet:** horizontal
> clustering. The **default build stays 100% dependency-free** — the `tls` feature is the only thing
> that pulls a crate, and only when you ask. ~10k lines of `std`-only Rust.

---

## Features

**Redis-compatible core**

- **Data types:** strings, lists, hashes, sets, sorted sets, streams, bitmaps — broad per-type command
  coverage with `WRONGTYPE` checks. ~180 commands; see [docs/COMMANDS.md](docs/COMMANDS.md).
- **Iteration & introspection:** real incremental `SCAN`/`HSCAN`/`SSCAN`/`ZSCAN`, `COMMAND`/`COMMAND
  DOCS`, `OBJECT ENCODING`, `CLIENT`, `GETEX` — off-the-shelf clients connect without fallbacks.
- **Key expiration:** `SET … EX/PX/EXAT/PXAT/NX/XX/KEEPTTL`, `EXPIRE`/`TTL`/`PERSIST`, passive + active.
- **`maxmemory` + eviction:** soft cap with key eviction and `OOM` rejection.
- **Transactions:** `MULTI`/`EXEC`/`DISCARD`, `WATCH`/`UNWATCH` (EXECABORT + WATCH-on-expiry).
- **Streams:** `XADD`/`XRANGE`/`XREAD`, including **blocking `XREAD`**.
- **Protocol:** RESP2 **and RESP3** typed replies (maps/sets/doubles) + **push frames** for pub/sub on
  `HELLO 3`; pipelining.

**Security & operations** *(safe on a trusted network)*

- **AUTH + ACL:** `requirepass`, **protected mode** (no accidental `0.0.0.0` exposure), and a simple
  **multi-user ACL** (`ACL SETUSER` with command classes + key prefixes) — least-privilege users.
- **TLS:** a sidecar (zero-dep default), or **in-process** via the optional `tls` build feature
  (rustls) — the default build pulls in nothing. See the TLS note below.
- **Observability:** a full `INFO` (works with `redis_exporter`), `SLOWLOG`, `CONFIG GET/SET`,
  structured leveled logging, graceful `SIGTERM` shutdown (drain → fsync → final save).
- **Resource safety:** per-connection read timeout, `TCP_NODELAY`, a max-connections cap.

**Durability**

- **Snapshots + AOF:** RDB-style binary snapshots (truly-async `BGSAVE`) and an append-only file with
  crash-safe, torn-tail-tolerant replay, configurable `appendfsync`, and `BGREWRITEAOF` compaction.
  Directory-fsync'd renames; **fuzz- and `kill -9` crash-recovery-tested.**

**Replication & high availability**

- `REPLICAOF` master/replica: full-sync snapshot + live command streaming, read-only replicas, real
  replication IDs + offsets, authenticated links (`masterauth`), and **`WAIT`** for ack-based
  durability. Expiry is master-authoritative, so replicas never diverge on timing. A briefly-dropped
  replica reconnects with a **partial resync** (`PSYNC` `CONTINUE` over a backlog ring) — no full
  snapshot when it only missed a little.
- **Automatic failover:** the same binary runs as a built-in **sentinel** (`LOCUS_SENTINEL`) that
  promotes the most up-to-date replica when the master dies and repoints the rest — no external
  orchestrator; run several sentinels for quorum-based agreement. See
  [High availability](#high-availability--automatic-failover).

**Reactive + geo differentiators**

- **[Changefeed](docs/CHANGEFEED.md):** `CDCSUBSCRIBE` (snapshot + live deltas, no gap/dup), offsets +
  `CDCREAD` catch-up, and consumer groups — a reliable, ordered keyspace feed (persisted + replicated).
- **[Geo-first](docs/GEO.md):** `GEOSET`/`GEOPOS`/`GEODIST`/`GEOSEARCH` (backed by a **geohash spatial
  index** → sub-linear radius/box queries) with **combined attribute filters** (`GEOSEARCH … WHERE
  status active`), plus **live geofencing** via `CDCSUBSCRIBE REGION`.
- **[Sketches](docs/SKETCHES.md):** Bloom (dedup), Count-Min (trending), Top-K (heavy hitters),
  t-digest (live percentiles).
- **CAS verbs:** `CAS`/`CADEL`/`SETMAX`/`INCRCAP` — atomic check-and-write.
- **Secondary index:** `IDXCREATE`/`IDXGET`/`IDXRANGE` — query by hash field, auto-maintained (no drift).

**Zero dependencies.** Pure `std`; one small static binary; reproducible builds.

See [docs/COMMANDS.md](docs/COMMANDS.md) for the full reference, [docs/CLIENTS.md](docs/CLIENTS.md) for
driving Locus from Node/Python (any Redis client works), the guides above for the differentiators,
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
# Docker — RESP on 6379
docker run -p 6379:6379 ghcr.io/intenttext/locus:latest
# persist across restarts:
docker run -p 6379:6379 -v locus-data:/data -e LOCUS_RDB=/data/locus.rdb ghcr.io/intenttext/locus:latest
```

Or grab a prebuilt static binary from the [latest release](https://github.com/intenttext/locus/releases/latest)
(Linux x86_64/aarch64, macOS x86_64/aarch64). With a Rust toolchain, install from crates.io (the crate
is `locusdb`; the installed command is `locus`):

```console
cargo install locusdb && locus
```

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
| `LOCUS_AOF` | _(off)_ | Path (or `1`) to enable append-only persistence |
| `LOCUS_APPENDFSYNC` | `everysec` | AOF fsync policy: `always` / `everysec` / `no` |
| `LOCUS_MAXMEMORY` | _(unlimited)_ | Soft cap; `kb`/`mb`/`gb` (e.g. `256mb`). Master evicts; `OOM` if still over |
| `LOCUS_CDC_MAXLEN` | _(off)_ | Retained changefeed log size for `CDCREAD` catch-up / consumer groups |
| `LOCUS_SLOWLOG_US` | `10000` | Log commands slower than this (µs); `<0` disables |
| `LOCUS_LOGLEVEL` | `info` | `error` / `warn` / `info` / `debug` |
| `LOCUS_CLUSTER_ENABLED` | `off` | Enable cluster routing (`MOVED`/`CROSSSLOT`) |
| `LOCUS_CLUSTER_ANNOUNCE` | `LOCUS_BIND:PORT` | This node's address in the cluster |
| `LOCUS_CLUSTER_NODES` | _(self owns all)_ | Topology: `host:port 0-5460;host:port 5461-10922;…` |
| `LOCUS_CLUSTER_CELL_BITS` | `0` (off) | Cell-in-key spatial sharding: bits of geohash per cell; >0 makes `GEOSEARCH` a bounded scatter (`CLUSTER CELL` gives the tag) |

### Security & replication in 30 seconds

```console
# require a password
LOCUS_REQUIREPASS=s3cret cargo run --release
redis-cli -p 6379 -a s3cret ping

# a least-privilege, read-only user scoped to app:* keys
redis-cli -p 6379 -a s3cret ACL SETUSER reader on '>pw' +@read '~app:'

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
returned old master) back to replicas, reducing split-brain risk.

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
| `LOCUS_SENTINEL_ID` | `127.0.0.1:PORT` | This sentinel's id for leader election |

Before promoting, the sentinel requires **corroboration** — a quorum of replicas must also report their
master link down — so a sentinel merely partitioned from the master won't trigger a needless failover.

**Run several sentinels for HA** (so failover survives a sentinel dying): give each a `LOCUS_SENTINEL_PORT`
and list the others in `LOCUS_SENTINEL_PEERS`. A failover then also needs a **majority of sentinels** to
agree the master is down, and only the **leader** (lowest id among the down-seeing sentinels) performs
the promotion — the majority gate stops a partitioned minority, the leader rule stops two sentinels
promoting different replicas. (Bully-style election over a tiny line protocol — not full Raft.)

```console
# sentinel A (run B symmetrically with PORT/PEERS swapped)
LOCUS_SENTINEL=master:6379 LOCUS_SENTINEL_REPLICAS=r1:6379,r2:6379 \
LOCUS_SENTINEL_PORT=26379 LOCUS_SENTINEL_PEERS=sentinelB:26379 \
  cargo run --release
```

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

---

## Performance

Locus prioritizes **clarity and predictable single-threaded semantics** over peak throughput. Measured
with the official `redis-benchmark` (release build, single instance):

| Mode | Throughput (approx) |
|---|---|
| Non-pipelined (`-c 50`) | ~8k–12k ops/sec per command type |
| Pipelined (`-P 16`) | ~58k SET / ~80k GET ops/sec |

Throughput is bounded by the single-hub design (one channel hop per command) — the deliberate price of
lock-free, serially-consistent execution, and the very property (one ordered point) that makes the
changefeed and live geo-queries possible. The path to more is **horizontal** — spatial sharding across
nodes (P6), each shard its own single-threaded hub — rather than threading the hub itself.

---

## Project status & roadmap

**Production-readiness so far:** safe on a trusted network (AUTH/ACL/protected-mode/limits), durable
(async snapshots, AOF + crash-recovery, persisted/replicated reactive state), observable
(`INFO`/`SLOWLOG`/`redis_exporter`), driver-compatible (`SCAN`/`COMMAND`/`CONFIG`/RESP3 incl. pub/sub
push), with correct replication (real offsets, `WAIT`, partial-resync, no expiry divergence) and
**automatic failover** (built-in sentinel) — plus the reactive/geo differentiator set, now with a
**geohash-indexed `GEOSEARCH` + `WHERE` filters**, ordered-index sorted sets, and a CRC16 routing seam.

**In progress — the last milestone:** horizontal **spatial clustering** (P6), Locus's flagship lane.
Landed so far: **static hash-slot routing** (`MOVED`/`CROSSSLOT`, `CLUSTER SLOTS/NODES/KEYSLOT`), the
**inter-node transport** layer (cluster-wide `DBSIZE`), **cross-shard scatter-gather `GEOSEARCH`** (one
global result merged by distance), and **cell-in-key spatial sharding** — name geo keys `{cell}id`
(`cell` from `CLUSTER CELL lon lat`) so a region co-locates on one shard, and `GEOSEARCH` becomes a
**bounded** scatter that only consults the shards whose cells the query covers — the Tile38-beating lane.
Resharding is **live and zero-loss**: `CLUSTER MIGRATESLOT slot dst` copies a slot's keys to another node
(two-phase — copy-all then commit), and `CLUSTER SETSLOT slot NODE addr` repoints ownership at runtime
(`CLUSTERDOWN` covers an unowned slot). **Per-shard failover** reuses the built-in sentinel: set
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
cargo test                 # unit + integration (parser fuzz, crash-recovery, replication, ACL, …)
cargo test --features tls  # also runs the TLS handshake / round-trip tests
cargo clippy               # lints (clippy-clean under -D warnings)
cargo fmt                  # formatting
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
