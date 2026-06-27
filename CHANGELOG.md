# Changelog

All notable changes to Locus are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.0] — 2026-06-27

The production-hardening + clustering release. On top of the reactive/geo core (0.2.0), Locus becomes
safe to operate, durable under crashes, correctly replicated, highly available, TLS-capable, and —
the flagship — **horizontally clustered with spatial locality**. The default build is still 100%
dependency-free; the optional `tls` feature is the only thing that pulls a crate.

### Added — security & access control
- **`AUTH` / `requirepass`** with a constant-time compare, `NOAUTH` gating, and a `HELLO AUTH` clause.
- **Protected mode** (`LOCUS_PROTECTED_MODE`) — refuses non-loopback traffic without a password, closing
  the accidental-exposure hole; **replica `masterauth`** closes the unauthenticated-`PSYNC` siphon.
- **ACL** — users, five command classes (read/write/admin/connection/pubsub) and key-prefix rules,
  layered additively over `requirepass` (vendored SHA-256). `ACL SETUSER/GETUSER/DELUSER/LIST/USERS/
  WHOAMI/CAT`.
- **Connection limits** — `LOCUS_MAXCLIENTS` cap and `LOCUS_TIMEOUT` idle timeout; `TCP_NODELAY`.

### Added — durability
- **Async `BGSAVE` and `BGREWRITEAOF`** — serialize on the hub, write/fsync off-thread, fold in writes
  buffered during the rewrite; the old file is kept on failure.
- **`appendfsync`** (`always`/`everysec`/`no`), directory fsync after rename, surfaced AOF fsync errors,
  and end-to-end `kill -9` crash-recovery tests. CDC + secondary-index state persists in an RDB trailer.

### Added — replication v2
- Stable 40-hex **replid** and a byte-accurate **`master_repl_offset`**; `INFO replication` reports both.
- **`WAIT numreplicas timeout`** with `REPLCONF ACK` and per-replica acked-offset tracking.
- **PSYNC partial-resync** over a 4 MiB backlog ring (`+CONTINUE` when covered, else `+FULLRESYNC`).
- No replica **expiry divergence** — the master streams a `DEL` for every expired key; real
  `master_link_status`.

### Added — high availability
- **Built-in sentinel** (`LOCUS_SENTINEL=…`) — health-checks the master and promotes the most
  up-to-date replica, repointing the rest, with replica-quorum corroboration and anti-split-brain
  reconciliation.
- **Inter-sentinel agreement** — multiple sentinels (`LOCUS_SENTINEL_PEERS`/`_PORT`/`_ID`) require a
  majority to see the master down, and only the elected leader promotes (no dual promotion).

### Added — TLS (optional)
- **In-process TLS** via the opt-in `tls` cargo feature (rustls + ring; no OpenSSL/C). `LOCUS_TLS_PORT`/
  `_CERT`/`_KEY` add a TLS listener alongside plaintext. The **default build stays zero-dependency**;
  a sidecar (ghostunnel/stunnel) remains documented for those who want it.

### Added — compatibility & observability
- **`SCAN`/`HSCAN`/`SSCAN`/`ZSCAN`** (stable cursor, `MATCH`/`COUNT`/`TYPE`/`NOVALUES`), real
  **`CONFIG GET/SET`**, fleshed-out **`INFO`** (works with `redis_exporter`), **`COMMAND`(`/COUNT/DOCS/
  INFO`)**, **`SLOWLOG`**, `OBJECT`, `CLIENT`, `GETEX`.
- **RESP3 typed replies** — maps (`HGETALL`, `CONFIG GET`), sets (`SMEMBERS`, `SINTER`/`SUNION`/`SDIFF`),
  doubles (`ZSCORE`/`ZINCRBY`/`ZMSCORE`), and **pub/sub push frames**.

### Added — geo & data-structure depth
- **Geohash spatial index** — a `BTreeMap` over 52-bit cells makes `GEOSEARCH` sub-linear (was a linear
  scan); **`WHERE field value`** attribute filters; `GEOSET` stores inline attributes.
- **Ordered-index sorted sets** — a `BTreeSet` companion index gives range/rank without re-sorting on read.

### Added — horizontal spatial clustering (the flagship)
- **Hash-slot routing** — CRC16 slots with `{hashtag}`, `MOVED`/`CROSSSLOT`/`CLUSTERDOWN`,
  `CLUSTER SLOTS/SHARDS/NODES/KEYSLOT`.
- **Cell-in-key spatial sharding** — `LOCUS_CLUSTER_CELL_BITS` + `{cell}id` keys (`CLUSTER CELL lon lat`)
  co-locate a region on one shard, so `GEOSEARCH` is a **bounded** cross-shard scatter (only the covering
  shards), not a full fan-out. Cross-shard scatter is parallelized (bounded to ~one peer timeout).
- **Live, zero-loss resharding** — `CLUSTER MIGRATESLOT slot dst` (two-phase copy-then-commit),
  `CLUSTER SETSLOT slot NODE addr`; changes are HLC-epoch-stamped and **converge via anti-entropy gossip**
  (`LOCUS_CLUSTER_GOSSIP_MS`) without pushing to every node.
- **Per-shard failover** — the sentinel (`LOCUS_SENTINEL_CLUSTER_NODES`) broadcasts `CLUSTER REASSIGN`
  after promotion so a dead master's slots follow its replica.
- **Global changefeed** — every change carries a **hybrid logical clock** (persisted across restarts);
  `CLUSTER CDCMERGE since-hlc` merges all shards' feeds in HLC order up to a watermark that bounds
  staleness (and holds for a downed shard).

### Changed
- Crate description and version reflect the reactive/geo/clustered scope. `~14k` lines of std-only Rust
  across 15 modules. CI now also lints and tests the `tls` feature.

## [0.2.0] — 2026-06-16

The reactive + geo-first release. On top of the Redis-compatible core (0.1.0), Locus gains its
differentiator layer — a reliable changefeed, a geo-first spatial model with live geofencing,
mergeable probabilistic sketches, conditional-write verbs, and an auto-maintained secondary index —
plus transaction-correctness fixes and `maxmemory` eviction. Still pre-1.0 and not production-hardened
(no AUTH/TLS; bind to a trusted network).

### Added (distribution)
- **`LOCUS_BIND`** — configurable listen interface (default `127.0.0.1`, so Locus isn't exposed by
  accident given it has no AUTH/TLS). An official **Docker image** (`ghcr.io/intenttext/locus`, sets
  `LOCUS_BIND=0.0.0.0`) and **prebuilt static binaries** (Linux/macOS, x86_64/arm64) are now published
  per release.

### Added (sketches — mergeable probabilistic summaries)
- **Bloom filter** `BFADD` / `BFEXISTS` (+ internal `BFLOAD` for AOF rewrite/replication) — dedup /
  set membership ("seen this id?"). Zero-deps (std `DefaultHasher` + double hashing), auto-sized, RDB/AOF
  persistent. First of the a-la-carte sketch family.
- **Count-Min sketch** `CMSINCRBY` / `CMSQUERY` (+ internal `CMSLOAD`) — frequency estimation
  ("trending now"); over-estimates, never under. Auto-sized (2000×5), RDB/AOF persistent.
- **Top-K sketch** `TOPKRESERVE` / `TOPKADD` / `TOPKLIST` / `TOPKCOUNT` (+ internal `TOPKLOAD`) —
  heavy hitters on top of Count-Min + a k-slot leaderboard; RDB/AOF persistent (opaque blob).
- **t-digest** `TDADD` / `TDQUANTILE` (+ internal `TDLOAD`) — streaming quantiles / percentiles
  (live p99), accurate at the tails via the `q(1-q)` scale; exact min/max. Completes the sketch family.

### Added (secondary index — query by field)
- **`IDXCREATE` / `IDXDROP` / `IDXGET` / `IDXRANGE`** — index a hash field for equality and
  lexicographic-range queries. Auto-maintained on every write/expiry/eviction in the same hub turn, so
  the index never drifts from the data (the single-threaded guarantee). In-memory; equality + range +
  COUNT (no query language — by design).

### Added (conditional writes — the CAS primitive)
- **CAS family** `CAS key expected new`, `CADEL key expected`, `SETMAX key n` (monotonic cursor),
  `INCRCAP key delta cap` (quota). Atomic check-and-write under single-threaded execution — no WATCH/Lua.
  Logged to the AOF as their concrete effect (`SET`/`DEL`) so replay/replication stay deterministic.

### Added (geo — the geo-first differentiator)
- **Live geofencing** — `CDCSUBSCRIBE REGION <lon> <lat> <radius> <unit>`: an atomic snapshot of the geo
  keys inside the circle, then a live stream as keys **enter/move** (`write`) and **leave** (`del` — on
  move-out, delete, or expire). The geo index + changefeed converge: a *region* filter on the per-key
  feed *is* geofencing. Per-subscriber membership tracking gives proper enter/leave transitions.
- **Geo commands** `GEOSET`, `GEOPOS`, `GEODIST`, `GEOSEARCH` (`BYRADIUS`/`BYBOX`, `FROMLONLAT`/`FROMKEY`,
  `ASC`/`DESC`, `COUNT`, `WITHCOORD`/`WITHDIST`). Geo-first model: each object is its own key
  (`Value::Geo`), with a geo-key index for search and full RDB/AOF persistence. Haversine distance.
  (Next: live region geofencing over the changefeed; a real S2/R-tree index with combined filters.)

### Added (changefeed — the reactive differentiator)
- **`CDCSUBSCRIBE [prefix]` / `CDCUNSUBSCRIBE`** — a reliable, ordered keyspace changefeed: an atomic
  snapshot of matching keys followed by a live stream of every change (`write`/`del`/`expire`), with
  no gap or duplication (guaranteed by single-threaded execution). Values are inlined for string keys.
  Fed from the same modification choke points as WATCH/AOF/replication, so it never misses a write and
  never reports a no-op. The foundation for live-query and geofencing.
- **Changefeed consumer groups** — `CDCGROUP CREATE|DESTROY`, `CDCREADGROUP <group> <consumer>`
  (load-balanced: each record delivered to one consumer), `CDCACK`, `CDCPENDING`. In-memory; built on
  the retained log/offsets. The second of the change-log's two read modes (broadcast + load-balanced).
- **Changefeed offsets + retention + `CDCREAD`** — every change carries a monotonic offset;
  `CDCREAD <offset> [COUNT n] [PREFIX p]` pulls retained changes after an offset for reconnect catch-up.
  Retention is opt-in via `LOCUS_CDC_MAXLEN` (a ring buffer); falling behind the retained window returns
  `offset out of range`. `CDCSUBSCRIBE`'s `snapshot-done` now reports the high-water offset, and live
  `cdc-change` messages now include their offset.

### Added (commands)
- String commands: `MGET`, `MSET`, `MSETNX`, `SETNX`, `SETEX`, `PSETEX`, `GETSET`, `GETRANGE`,
  `SETRANGE`, `INCRBYFLOAT`.
- Keyspace commands: `KEYS`, `DBSIZE`, `RENAME`, `RENAMENX`, `TOUCH`, `UNLINK`, `FLUSHDB`, `FLUSHALL`.
- List commands: `LINSERT`, `LREM`, `LTRIM`, `LPOS`, `RPOPLPUSH`, `LMOVE`.
- Set commands: `SMOVE`, `SINTERSTORE`, `SUNIONSTORE`, `SDIFFSTORE`, `SINTERCARD`.
- Sorted-set commands: `ZREMRANGEBYRANK`, `ZREMRANGEBYSCORE`, `ZUNIONSTORE`, `ZINTERSTORE`
  (with `WEIGHTS`/`AGGREGATE`; set sources score 1.0).
- Bitmap commands: `SETBIT`, `GETBIT`, `BITCOUNT` (incl. `BYTE`/`BIT` ranges), `BITPOS`, `BITOP`.
- Randomized commands: `SRANDMEMBER` (negative count = with repeats), `RANDOMKEY`, backed by a small
  zero-deps xorshift PRNG. `SPOP` now selects truly random members (was arbitrary iteration order).

### Fixed
- WATCH now dirties **all** keys touched by multi-key writes (`MSET`/`MSETNX`/`RENAME`) and by
  `FLUSHDB`/`FLUSHALL`, not just the first key.

### Changed (internal)
- Consolidated command metadata (existence, minimum arity, write-or-read) into a single
  `commands::command_meta` table — the one source of truth. `aof::is_write` now delegates to it,
  removing the separate hand-maintained write allowlist that could silently drift (a forgotten entry
  meant a write that wasn't persisted or replicated). A regression-lock test pins the write set.

### Added
- **`maxmemory` + eviction** (`LOCUS_MAXMEMORY`, accepts `kb`/`mb`/`gb` suffixes). Approximate memory
  accounting bounds dataset growth; when over the cap a master evicts arbitrary keys (streamed to
  replicas/AOF as `DEL`) and rejects a write with `OOM` only if the cap still can't be met. Replicas
  don't self-evict — the master drives deletions. `INFO` now reports a `# Memory` section
  (`used_memory`, `maxmemory`).

### Added
- `SELECT` — single logical DB: `SELECT 0` returns OK (so clients that select on connect work);
  other indexes are rejected. Full multi-DB is a deliberate non-goal.

### Fixed (replication)
- The replica handshake now uses a read timeout, so a master that accepts the TCP connection but
  never replies can no longer hang the replication thread (and `REPLICAOF NO ONE` can take effect).

### Fixed (transactions)
- **`WATCH` now aborts `EXEC` when a watched key expires** (passive or active reaper), not only on an
  explicit write — matching Redis optimistic-concurrency semantics.
- **`MULTI` validates commands at queue time**: an unknown command or one with too few arguments now
  flags the transaction so `EXEC` returns `EXECABORT` instead of running a half-valid batch.
- **No-op writes no longer abort `WATCH`** (and are no longer logged to the AOF or replicated): e.g.
  `DEL` of a missing key or `SADD` of an existing member no longer spuriously dirties a transaction.

### Fixed
- **TTL integer overflow** in `EXPIRE`/`PEXPIRE`/`EXPIREAT`/`PEXPIREAT` and `SET … EX/PX/EXAT/PXAT`:
  very large TTLs now error cleanly instead of panicking (debug) or wrapping to a past deadline and
  silently deleting the key (release).
- **`ZADD GT`/`LT`** now gate score updates (and `INCR`) correctly instead of being silently ignored;
  incompatible flag combinations (`GT`+`LT`, `NX`+`GT`/`LT`) are rejected.

### Testing
- Added an end-to-end integration harness (`tests/integration.rs`) that spawns the real server and
  drives it over TCP: pipelining, MULTI/EXEC, EXECABORT, WATCH (change + expiry), no-op-WATCH,
  pub/sub, blocking `XREAD`, and a replication round-trip.

### Added
- **`RESET`** command — aborts `MULTI`, releases `WATCH`es, exits subscribe mode, drops to RESP2.

### Security / hardening
- RESP parser bounds untrusted input: capped eager pre-allocation for large `*N` array headers, and a
  64 KiB limit on un-terminated inline requests (prevents per-connection unbounded buffer growth).

### Fixed (replication)
- A replica that just loaded a full-sync snapshot now re-evaluates clients parked on blocking `XREAD`.

## [0.1.0] — 2026-06-16

Initial release. Built in twelve incremental milestones (M0–M12); the git history has one commit per
milestone. Zero third-party dependencies (pure `std`).

### Added
- **Data types:** strings, lists, hashes, sets, sorted sets, streams (with `WRONGTYPE` checks).
- **Key expiry:** `SET EX/PX/EXAT/PXAT/NX/XX/KEEPTTL`, `EXPIRE`/`PEXPIRE`/`EXPIREAT`/`PEXPIREAT`,
  `TTL`/`PTTL`, `PERSIST` — passive (on-access) and active (background sampling).
- **Persistence:** RDB-style binary snapshots (`SAVE`/`BGSAVE`, temp→fsync→rename) and an append-only
  file (AOF) with crash-safe, torn-tail-tolerant replay, deterministic command rewriting, and
  `BGREWRITEAOF` compaction.
- **Replication:** `REPLICAOF` master/replica — full-sync snapshot transfer + live command streaming,
  read-only replicas, `INFO replication`.
- **Pub/Sub:** `SUBSCRIBE`/`UNSUBSCRIBE`/`PSUBSCRIBE`/`PUNSUBSCRIBE`/`PUBLISH`/`PUBSUB` with glob patterns.
- **Transactions:** `MULTI`/`EXEC`/`DISCARD` and `WATCH`/`UNWATCH` optimistic locking.
- **Streams:** `XADD`/`XLEN`/`XRANGE`/`XREVRANGE`/`XREAD`, including blocking `XREAD`.
- **Protocol:** RESP2 + `HELLO` RESP3 negotiation; pipelining.

### Known limitations / deferred
- Streams consumer groups; PSYNC partial resync, replication backlog, `WAIT`, automatic failover;
  a skiplist for O(log n) sorted-set ops; full RESP3 typing of every reply; thread-per-core execution.
- No authentication or TLS yet — bind to a trusted network only.

[Unreleased]: https://github.com/intenttext/locus/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/intenttext/locus/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/intenttext/locus/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/intenttext/locus/releases/tag/v0.1.0
