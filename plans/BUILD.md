# Build Status — Locus is implemented (M0 → M12)

The full roadmap is built and committed in [locus/](locus/) — a working in-memory,
Redis-protocol datastore in **Rust with zero third-party dependencies** (pure `std`).
~3,750 lines across 8 modules, 16 unit tests, every milestone verified against the real
`redis-cli` / `redis-benchmark`.

## Run it

```bash
cd locus
cargo run                                   # listens on 127.0.0.1:6379
redis-cli -p 6379 ping                      # -> PONG
redis-cli -p 6379 set foo bar && redis-cli -p 6379 get foo

# options (env vars)
LOCUS_PORT=6380 cargo run                    # different port
LOCUS_AOF=1 cargo run                        # enable append-only persistence
LOCUS_RDB=/path/snap.rdb cargo run           # snapshot file location

cargo test                                   # 16 tests
cargo build --release && redis-benchmark -p 6379 -t set,get -P 16 -q
```

## Milestones

| # | Milestone | What works |
|---|---|---|
| M0 | TCP + PING | server on :6379, `redis-cli ping` |
| M1 | RESP parser + SET/GET | resumable parser (byte-split + pipelined safe) |
| M2 | Concurrency | thread-per-conn + single keyspace owner; atomic under load |
| M3 | Expiry | SET EX/PX/EXAT/NX/XX/KEEPTTL, EXPIRE/TTL/PERSIST, passive + active |
| M4 | Lists/Hashes/Sets | full per-type command sets + WRONGTYPE |
| M5 | Sorted sets | ZADD/ZRANGE/ZRANGEBYSCORE/ZRANK/ZPOPMIN… |
| M6 | RDB snapshot | SAVE/BGSAVE, temp→fsync→rename, load on startup |
| M7 | AOF | append + replay, torn-tail tolerant, SPOP/TTL rewritten, BGREWRITEAOF |
| M8 | Pub/Sub | SUBSCRIBE/PSUBSCRIBE/PUBLISH, glob patterns, async writer threads |
| M9 | Replication | REPLICAOF, full sync + live stream, read-only replica, INFO |
| M10 | Transactions | MULTI/EXEC/DISCARD, WATCH optimistic locking |
| M11 | Streams | XADD/XRANGE/XREAD + **blocking** XREAD |
| M12 | RESP3 + bench | HELLO 2/3, pipelining, runs under redis-benchmark |

## Architecture (the Redis lesson, in Rust)

- **Single-threaded execution:** one hub thread owns the keyspace, pub/sub registry,
  replication, transactions, and blocking-reader state. Every command runs serially →
  atomic by construction, no locks on data.
- **Per connection:** a reader thread (parse) + a writer thread (drain an output channel).
  Replies, published messages, and replicated writes all flow through the writer.
- **Persistence:** RDB (binary snapshot) and AOF (command log) — both off the hot path.
- Modules: `resp` (wire) · `db` (keyspace + types + expiry) · `commands` (dispatch) ·
  `rdb` · `aof` · `pubsub` · `streams` · `main` (hub + connections).

## What's intentionally deferred (the honest tail)

Consumer groups (streams); PSYNC partial resync / replication backlog / WAIT / failover;
the skiplist for O(log n) zset ops; full RESP3 typing of every reply; thread-per-core for
multi-core throughput. And the **geo-first + reactive** differentiators (see
[DIFFERENTIATORS.md](DIFFERENTIATORS.md)) — the next phase, now that the Redis-compatible
core exists.
