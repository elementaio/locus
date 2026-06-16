# Changelog

All notable changes to Locus are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- **TTL integer overflow** in `EXPIRE`/`PEXPIRE`/`EXPIREAT`/`PEXPIREAT` and `SET … EX/PX/EXAT/PXAT`:
  very large TTLs now error cleanly instead of panicking (debug) or wrapping to a past deadline and
  silently deleting the key (release).
- **`ZADD GT`/`LT`** now gate score updates (and `INCR`) correctly instead of being silently ignored;
  incompatible flag combinations (`GT`+`LT`, `NX`+`GT`/`LT`) are rejected.

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
