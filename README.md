# Locus

**An in-memory datastore that speaks the Redis protocol — written from scratch in Rust, with zero dependencies.**

[![CI](https://github.com/intenttext/locus/actions/workflows/ci.yml/badge.svg)](https://github.com/intenttext/locus/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-2024-orange.svg)](https://www.rust-lang.org/)

Locus is a single-binary, RESP-compatible key-value/data-structure server. You can drive it with the
real `redis-cli`, point existing Redis clients at it, and benchmark it with `redis-benchmark`. It's
built around the same core idea that makes Redis elegant — **one thread executes commands serially, so
every command is atomic by construction** — and it ships with no third-party crates: just the Rust
standard library.

On top of that Redis-compatible core, Locus adds a **reactive, geo-first** layer that a vanilla Redis
can't cleanly offer — because the single-threaded hub sees every mutation's before/after at one ordered
point:

- a reliable, ordered **[changefeed](docs/CHANGEFEED.md)** (snapshot + live deltas, offsets, consumer
  groups) — keyspace notifications done right;
- **[geo-first](docs/GEO.md)** objects with `GEOSEARCH` and **live geofencing** over the changefeed;
- mergeable **[sketches](docs/SKETCHES.md)** (Bloom, Count-Min, Top-K, t-digest);
- **CAS** write verbs and a drift-free **secondary index** (query by field).

> **Status:** pre-1.0 and **not yet production-hardened** (no AUTH/TLS; single node). It is a faithful,
> readable implementation with a complete data-type core *and* the full reactive/geo differentiator set
> — a solid foundation rather than a drop-in production Redis. ~8k lines of `std`-only Rust, 9 modules.

```console
$ cargo run
Locus listening on 127.0.0.1:6379

$ redis-cli -p 6379 set hello world
OK
$ redis-cli -p 6379 get hello
"world"
```

---

## Features

**Redis-compatible core**

- **Data types:** strings, lists, hashes, sets, sorted sets, streams, bitmaps — broad per-type command
  coverage with `WRONGTYPE` checks. ~115 commands; see [docs/COMMANDS.md](docs/COMMANDS.md).
- **Key expiration:** `SET ... EX/PX/EXAT/PXAT/NX/XX/KEEPTTL`, `EXPIRE`/`TTL`/`PERSIST`, with both
  passive (on-access) and active (background sampling) expiry.
- **`maxmemory` + eviction:** soft memory cap with arbitrary-key eviction and `OOM` rejection.
- **Persistence:** RDB-style binary snapshots (`SAVE`/`BGSAVE`) and an append-only file (AOF) with
  crash-safe, torn-tail-tolerant replay and `BGREWRITEAOF` compaction.
- **Replication:** `REPLICAOF` master/replica with full-sync snapshot transfer + live command
  streaming; read-only replicas; `INFO`.
- **Pub/Sub:** `SUBSCRIBE`/`PSUBSCRIBE`/`PUBLISH` with glob patterns.
- **Transactions:** `MULTI`/`EXEC`/`DISCARD` and `WATCH`/`UNWATCH` (with correct EXECABORT + WATCH-on-expiry).
- **Streams:** `XADD`/`XRANGE`/`XREAD`, including **blocking `XREAD`**.
- **Protocol:** RESP2 + `HELLO` RESP3 negotiation; pipelining.

**Reactive + geo differentiators**

- **[Changefeed](docs/CHANGEFEED.md):** `CDCSUBSCRIBE` (snapshot + live deltas, no gap/dup), offsets +
  `CDCREAD` catch-up, and consumer groups — a reliable, ordered keyspace feed.
- **[Geo-first](docs/GEO.md):** `GEOSET`/`GEOPOS`/`GEODIST`/`GEOSEARCH`, plus **live geofencing** via
  `CDCSUBSCRIBE REGION`.
- **[Sketches](docs/SKETCHES.md):** Bloom (dedup), Count-Min (trending), Top-K (heavy hitters),
  t-digest (live percentiles).
- **CAS verbs:** `CAS`/`CADEL`/`SETMAX`/`INCRCAP` — atomic check-and-write.
- **Secondary index:** `IDXCREATE`/`IDXGET`/`IDXRANGE` — query by hash field, auto-maintained (no drift).

**Zero dependencies.** Pure `std`, ~8k lines across 9 modules.

See [docs/COMMANDS.md](docs/COMMANDS.md) for the full command reference, the guides above for the
differentiators, and [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for how it works inside.

---

## Quick start

Requires a recent Rust toolchain (edition 2024). The official `redis-cli` / `redis-benchmark` are handy
for driving it (`brew install redis` on macOS) but not required to build.

```console
# build & run
cargo run                 # debug, listens on 127.0.0.1:6379
cargo run --release       # optimized

# drive it with the real redis-cli
redis-cli -p 6379 ping
redis-cli -p 6379 rpush mylist a b c
redis-cli -p 6379 lrange mylist 0 -1
redis-cli -p 6379 zadd board 100 alice 50 bob
redis-cli -p 6379 zrange board 0 -1 withscores

# run the tests
cargo test
```

### Configuration

Locus is configured entirely through environment variables (minimal config by design):

| Variable | Default | Meaning |
|---|---|---|
| `LOCUS_PORT` | `6379` | TCP port to listen on |
| `LOCUS_RDB` | `locus.rdb` | RDB snapshot file path |
| `LOCUS_AOF` | _(off)_ | Set to a path (or `1`) to enable append-only persistence |
| `LOCUS_MAXMEMORY` | _(unlimited)_ | Soft memory cap; accepts bytes or `kb`/`mb`/`gb` (e.g. `256mb`). Over the cap, a master evicts keys; writes get `OOM` only if the cap still can't be met |
| `LOCUS_CDC_MAXLEN` | _(off)_ | Retained changefeed log size (records) for `CDCREAD` catch-up / consumer groups; `0`/unset = off (live `CDCSUBSCRIBE` still works) |

```console
LOCUS_AOF=1 cargo run --release          # durable, append-only mode
LOCUS_PORT=6380 cargo run --release      # run a second instance (e.g. a replica)
```

### Replication in 30 seconds

```console
# terminal 1 — master
LOCUS_PORT=6379 cargo run --release

# terminal 2 — replica
LOCUS_PORT=6380 cargo run --release
redis-cli -p 6380 replicaof 127.0.0.1 6379

# terminal 3
redis-cli -p 6379 set foo bar
redis-cli -p 6380 get foo        # -> "bar"  (replicated)
redis-cli -p 6380 set x y        # -> READONLY (replicas reject writes)
```

---

## Architecture

```
        ┌── reader thread ──┐                         ┌─────────────────────────┐
client ─┤  parse RESP       │── command ──▶  channel ─▶│      hub (1 thread)     │
        │                   │                          │  • keyspace (the data)  │
        └── writer thread ◀─┘◀── reply/message ── channel │  • pub/sub registry  │
                                                        │  • replication state    │
                                                        │  • transactions         │
                                                        │  • blocking readers      │
                                                        └─────────────────────────┘
```

- Each connection gets a **reader thread** (parse the resumable RESP stream) and a **writer thread**
  (drain an output channel to the socket).
- A single **hub thread** owns all mutable state and runs every command serially — so atomicity comes
  from the architecture, not from locks. Replies, published messages, and replicated writes all flow
  back through clients' output channels.
- Persistence and replication sit **off the hot path**: snapshots and the append-only log are written
  alongside, never blocking reads.

Full details, including the persistence formats and the design philosophy, are in
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
lock-free, serially-consistent execution. The path to more is **thread-per-core / shared-nothing
sharding** (each shard its own single-threaded hub), which is on the roadmap rather than retrofitted in.

---

## Project status & roadmap

The Redis-compatible core was built in twelve milestones (M0–M12); the reactive + geo differentiator
layer followed. See [docs/ROADMAP.md](docs/ROADMAP.md) for the full ledger.

**Implemented:** the Redis-compatible core (data types incl. bitmaps · expiry · `maxmemory` · RDB ·
AOF + recovery · pub/sub · replication · transactions · streams · RESP3 negotiation · pipelining) **plus**
the differentiators (changefeed with offsets/groups/geofencing · geo-first index · Bloom/Count-Min/Top-K/
t-digest sketches · CAS verbs · secondary index).

**Deliberately deferred:** AUTH/TLS; replication's deep tail (PSYNC partial resync, backlog, `WAIT`,
failover); a skiplist for O(log n) sorted-set ops; a real S2/R-tree geo index + combined filters +
spatial clustering; thread-per-core execution; multiple logical DBs.

The next major arc is **geo phase 3** — a real spatial index, combined attribute filters, and the
horizontal **spatial clustering** that nobody in the in-memory-geo space has packaged simply.

---

## Building & testing

```console
cargo build --release      # the optimized binary at target/release/locus
cargo test                 # unit tests (parser, commands, persistence, ...)
cargo clippy               # lints (the codebase is clippy-clean)
cargo fmt                  # formatting
```

## Contributing

Contributions are welcome — see [CONTRIBUTING.md](CONTRIBUTING.md). The codebase is intentionally small
and readable; new commands generally mean one match arm plus a focused function and a test.

## License

[MIT](LICENSE) © 2026 Emad Jumaah.

## Acknowledgements

Locus is a study in, and homage to, the elegance of **Redis** and Salvatore Sanfilippo's (antirez)
design philosophy: simplicity as a feature, single-threaded determinism, and data structures as a
service. It is an independent implementation and is not affiliated with or endorsed by Redis Ltd.
