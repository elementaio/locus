# Roadmap

Locus was built in twelve incremental milestones, each one shippable and verified against the real
`redis-cli` before moving on. The git history has one commit per milestone.

## Built (M0 → M12)

| # | Milestone |
|---|---|
| M0 | TCP server that replies `PONG` |
| M1 | Resumable RESP parser + `PING`/`ECHO`/`SET`/`GET` |
| M2 | Concurrency: thread-per-connection + single keyspace owner; more string commands |
| M3 | Key expiry — passive (on access) + active (background sampling) |
| M4 | Typed values — lists, hashes, sets (+ `WRONGTYPE`) |
| M5 | Sorted sets |
| M6 | RDB-style snapshot persistence |
| M7 | AOF (append-only file) + crash recovery |
| M8 | Pub/Sub (+ the async connection model) |
| M9 | Replication — full sync + command streaming |
| M10 | Transactions — `MULTI`/`EXEC`/`WATCH` |
| M11 | Streams — `XADD`/`XRANGE`/`XREAD` + blocking |
| M12 | RESP3 (`HELLO`), pipelining, benchmarking |

## Built — beyond the core (the differentiator layer)

On top of M0–M12, the reactive + geo-first vision is now implemented:

| Area | What |
|---|---|
| Hardening | TTL-overflow fix, transaction correctness (EXECABORT, WATCH-on-expiry, no-op-WATCH), `maxmemory` + eviction, parser DoS bounds, single command table |
| Command coverage | strings, keyspace, lists, sets, sorted sets, bitmaps, random — a broad Redis-compatible surface |
| **Changefeed** | `CDCSUBSCRIBE` (snapshot + live, no gap/dup), offsets + `CDCREAD` catch-up, consumer groups |
| **Geo** | `GEOSET`/`GEOPOS`/`GEODIST`/`GEOSEARCH` + **live geofencing** (`CDCSUBSCRIBE REGION`) |
| **Sketches** | Bloom, Count-Min, Top-K, t-digest |
| **CAS verbs** | `CAS`/`CADEL`/`SETMAX`/`INCRCAP` |
| **Secondary index** | `IDXCREATE`/`IDXGET`/`IDXRANGE` — query by field, auto-maintained |

See [CHANGEFEED.md](CHANGEFEED.md), [GEO.md](GEO.md), [SKETCHES.md](SKETCHES.md).

## Shipped since (P0–P5 hardening)

- **Security:** AUTH + protected mode + a simple multi-user **ACL**; conn limits, idle timeout.
- **Durability:** async `BGSAVE`/`BGREWRITEAOF`, `appendfsync`, persisted+replicated reactive state, crash tests.
- **Replication:** real replid/offset, **`WAIT`**, **PSYNC partial-resync** over a backlog ring, no expiry divergence.
- **HA + TLS:** built-in **sentinel** auto-failover (quorum + inter-sentinel agreement); TLS via sidecar or the optional `tls` feature.
- **Compat/observability:** `SCAN`/`COMMAND`/`CONFIG`/`SLOWLOG`/`INFO` (works with `redis_exporter`), RESP3 typed replies.
- **Geo depth:** geohash **spatial index** (sub-linear `GEOSEARCH`) + combined `WHERE` attribute filters.
- **Sorted sets:** ordered index (std `BTreeSet`) for range/rank without re-sorting on read.

## Deferred (known, intentional)

- A finer **S2/R-tree** geo index + keyset pagination (a geohash index already makes `GEOSEARCH` sub-linear).
- **Full RESP3 typing** of every reply (typed maps/sets/doubles + pub/sub push frames done; a few niche replies remain).
- **Native in-process TLS by default** (it's an opt-in feature; the default build stays zero-dependency).
- The big one: horizontal **spatial clustering** (P6) — the flagship, done last; **spatial-first vs hash-slot-first** is the open call.

## Dismissed (won't do — with the reasoning)

- **Thread-per-core / shared-nothing hubs** — fights the single-thread identity (one ordered point powers the changefeed + geo) and overlaps clustering's cross-shard ordering. Scale is **horizontal** (P6), not vertical.
- **Replica chaining (sub-replicas)** — niche read-fan-out; risk to the working replication offset path; P6 sharding is the scale lane.
- **Numbered multiple DBs (`SELECT n`)** — Redis discourages it and Cluster bans DB>0, so it wouldn't compose with P6; use **key-prefix namespacing** (cluster-safe). `SELECT 0` stays for connect-compat.
- **Scripting/`EVAL`**, an embedded HTTP `/metrics` endpoint, active-active replication.

## Distribution (shipped in v0.2.0)

- **GitHub Releases** with prebuilt static binaries (Linux x86_64/aarch64, macOS x86_64/arm64).
- **Docker image** — `ghcr.io/intenttext/locus` (multi-tag, public).
- **crates.io** — `cargo install locusdb` (crate `locusdb`; the command stays `locus`).
- Any **Redis client** works over RESP; the custom verbs go through each client's raw-command API
  (see [CLIENTS.md](CLIENTS.md)).

## Next major arc — geo phase 3 & clustering

- A real **S2-cell / R-tree** spatial index (sub-linear `GEOSEARCH`) with **combined attribute filters**
  (`nearby AND status=…`) and keyset pagination.
- **Spatial clustering** — horizontal sharding that preserves locality — the empty market intersection
  (in-memory · geo-first · clustered) that even Tile38 leaves open.

## Ecosystem & smaller follow-ups

- A thin **reactive client wrapper** (TypeScript/npm first) for the changefeed/geofence *push* API —
  `feed.on('change', …)` / `locus.geofence(…)` — wrapping an existing client, not reimplementing RESP.
  A Python helper if there's demand. (Standard clients already work today; this is DX sugar.)
- Adopt-later primitives: per-element TTL, per-command durability, time-based changefeed retention.
- Release tooling: bump release actions once upstream ships Node-24 builds (current deprecation warnings
  are cosmetic); optional Docker Hub mirror.
