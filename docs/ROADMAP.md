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

## Deferred (known, intentional)

These are real features left out to keep each milestone complete and the codebase honest:

- Streams **consumer groups** (`XGROUP`/`XREADGROUP`/`XACK`)
- Replication's deep tail: **PSYNC partial resync**, replication backlog, `WAIT`, automatic **failover**
- A **skiplist** for O(log n) sorted-set rank/range (currently correct sort-on-demand)
- **Full RESP3 typing** of every reply (we negotiate `HELLO` but keep RESP2-compatible encoders)
- **Thread-per-core** execution for multi-core throughput

## Future direction

Locus is evolving toward a **geo-first, reactive** datastore:

- **First-class geospatial indexing** with combined attribute filters and sort-by-distance — the kind
  of "nearby + filter + sort + paginate" query that's painful elsewhere.
- A **change-log / changefeed** primitive: subscribe to a key-prefix (or region) and receive a snapshot
  followed by live deltas.
- **Compare-and-swap** write verbs and **mergeable probabilistic sketches** (Count-Min, Top-K,
  t-digest) as first-class, dependency-light primitives.

The Redis-compatible core in this repo is the foundation those build on.
