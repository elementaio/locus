# Changelog

All notable changes to Locus are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/intenttext/locus/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/intenttext/locus/releases/tag/v0.1.0
