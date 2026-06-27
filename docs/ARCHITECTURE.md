# Architecture

Locus is small on purpose. This document explains how it's put together and the design choices behind
it. The whole server is ~14k lines of `std`-only Rust across 15 modules — a Redis-compatible core, a
reactive/geo differentiator layer, and a spatial-clustering layer, all on one single-threaded hub.

## Design philosophy

Locus follows the principles that make Redis elegant:

1. **Single-threaded command execution.** One thread runs every command to completion, in arrival
   order. Atomicity is a property of the *architecture*, not of locks — there are no mutexes or atomics
   on the data path, and no data races to reason about.
2. **Everything in RAM.** Operations work directly on in-memory structures; persistence happens off to
   the side, never on the request path.
3. **Data structures as a service.** General-purpose primitives (strings, lists, hashes, sets, sorted
   sets, streams) that applications compose — not use-case-specific features.
4. **A small, dependency-free, readable codebase.** Zero third-party crates. Every module fits in your
   head.
5. **Minimal configuration.** A handful of environment variables; the server behaves the same
   regardless of how it's configured.

## The threading model

```
        ┌── reader thread ──┐                         ┌─────────────────────────┐
client ─┤  read + parse     │── Msg::Command ─▶ chan ─▶│      hub (1 thread)     │
        │                   │                          │  owns ALL mutable state │
        └── writer thread ◀─┘◀──── reply bytes ─── chan │                         │
                                                        └─────────────────────────┘
```

- **Reader thread (per connection):** reads bytes and runs the resumable RESP parser. TCP is a byte
  stream, so a single `read()` may contain half a command, exactly one, or many pipelined together —
  the parser is a state machine over an accumulating buffer that yields zero-or-more complete commands
  and preserves the partial tail. Parsed commands are forwarded to the hub over a channel; the reader
  never waits for a reply, which is what gives us pipelining.
- **Writer thread (per connection):** owns a clone of the socket's write half and drains an output
  channel. *Every* byte sent to a client — command replies, pub/sub messages, replicated writes — goes
  through this one writer, so writes never interleave.
- **The hub (one thread for the whole server):** owns the keyspace, the pub/sub registry, replication
  state, per-client transaction state, parked blocking-readers, the changefeed (subscribers + retained
  log + consumer groups), and the secondary indexes. It processes one message at a time: a new
  connection, a command, a disconnect, or a replica snapshot. Because all command execution funnels
  through here, it's serialized — atomic by construction. This single choke point is also *why* the
  reactive layer is correct: every mutation passes one ordered point that holds old+new state, so the
  changefeed, the geo-key index, and the secondary indexes can be kept in lockstep with the data with no
  races and no drift.

This is the Redis bet expressed in Rust: trade multi-core throughput for simplicity and predictable
latency. The borrow checker actively reinforces it — the moment you reach for shared mutable state you
feel why the single-owner model is cleaner.

## Modules

| Module | Responsibility |
|---|---|
| `resp` | RESP2 wire protocol: the resumable parser and the reply encoders. |
| `db` | The keyspace, the typed `Value` enum, key expiration (passive + active), memory accounting, and the geo-key index. |
| `commands` | Command dispatch, the single command table (arity + write-flag), and the per-type implementations (strings, keyspace, lists, sets, sorted sets, bitmaps, geo, sketches, CAS). |
| `streams` | The stream type and its commands (XADD/XRANGE/XREAD). |
| `sketch` | Probabilistic sketches: Bloom, Count-Min, Top-K, t-digest. |
| `pubsub` | The publish/subscribe registry, glob matching, and message encoders. |
| `rdb` | Binary snapshot serialization (save/load + in-memory serialize for replication; per-entry dump/restore for slot migration). |
| `aof` | The append-only log: append, replay, rewrite, and command rewriting for determinism. |
| `geohash` | 52-bit interleaved geohash: point encode, box→cell ranges (spatial index), and the cluster cell id (cell-in-key sharding). |
| `hlc` | Hybrid logical clock: the monotonic stamp for changefeed records and slot-ownership epochs (cross-shard ordering). |
| `acl` | Users, classes, and key-prefix rules (vendored SHA-256) layered over `requirepass`. |
| `sentinel` | Failover monitor mode: health checks, quorum/inter-sentinel agreement, promotion, and cluster slot reassignment. |
| `tls` | Optional (`tls` feature) in-process TLS via rustls; the default build never compiles it. |
| `log` | The std-only timestamped leveled logger. |
| `main` | The hub, the connection threads, replication, the changefeed, secondary indexes, and the cluster layer (routing, scatter-gather, gossip). |

## Values and expiry

A key maps to a `Value` — one of `Str`, `List`, `Hash`, `Set`, `ZSet`, `Stream`, `Geo` (a lon/lat
point), `Bloom`, `Cms`, `TopK`, or `TDigest`. Commands type-check and return `WRONGTYPE` on a mismatch.
Empty collections are removed automatically (as in Redis). Every value kind serializes in the RDB
format and survives AOF rewrite (sketches restore via a raw-load command, since they can't be rebuilt
from their add-history).

Expiry deadlines live in a separate map (`key -> expire-at-ms`):

- **Passive:** every access checks the key's deadline and deletes it if due, so an expired key is never
  returned. This guarantees correctness.
- **Active:** a periodic sampling pass (Redis-style: sample ~20 keys-with-TTL, delete the expired ones,
  repeat while a sample is >25% expired) reclaims memory from keys that are never touched again.

## Persistence

Two independent, optional mechanisms, both off the hot path:

- **RDB (snapshot):** `SAVE`/`BGSAVE` serialize the whole dataset to a compact length-prefixed binary
  file. The write is crash-safe by construction — temp file → `fsync` → atomic rename — so a
  half-written snapshot can never replace a good one. Loaded on startup.
- **AOF (append-only file):** every write command is appended in RESP and replayed on startup. Three
  things make it correct:
  - **Torn-tail tolerance:** a crash can truncate the final command; replay stops at the last *complete*
    command instead of erroring.
  - **Determinism:** non-deterministic commands are rewritten at log time so replay can't diverge —
    relative TTLs become absolute `PEXPIREAT`, `SPOP`'s random removal becomes the exact `SREM` it
    produced, and `XADD *` is logged with the concrete generated id.
  - **fsync policy:** the log is fsynced about once per second (the "everysec" trade-off).
  - `BGREWRITEAOF` compacts the log to the minimal set of commands that rebuilds the current dataset.

When AOF is enabled it is the source of truth on startup; otherwise the RDB snapshot is loaded.

## Replication

- **Master:** on `PSYNC`, sends `+FULLRESYNC` followed by an in-memory snapshot as a bulk string, then
  registers the connection as a replica. Every subsequent write is streamed to replicas in the same
  deterministic form used by the AOF.
- **Replica:** `REPLICAOF host port` spawns a background sync thread that performs the handshake
  (`PING` → `REPLCONF` → `PSYNC`), loads the snapshot, then applies the live command stream. Replicas
  are read-only for normal clients (`-READONLY`) and reconnect automatically if the link drops.

Replication is asynchronous — fast, but not strongly consistent (an acknowledged write can be lost if
the master fails before propagating). The deeper machinery (PSYNC partial resync, replication backlog,
`WAIT`, automatic failover) is intentionally deferred.

## Transactions

Per-client state tracks a `MULTI` queue and a set of `WATCH`ed keys. Queued commands reply `+QUEUED`;
`EXEC` runs them with no interleaving (the hub is single-threaded, so this is free). `WATCH` registers
keys in a watched-keys index; any client that modifies a watched key marks the watcher's transaction
dirty, and `EXEC` then aborts to nil. As in Redis, there is no rollback on a runtime error mid-`EXEC`.

## Blocking reads

`XREAD ... BLOCK` parks the client in the hub (it is *not* replied to immediately). When a matching
`XADD` arrives, the hub re-checks parked readers and wakes any that can now read; on the hub's periodic
tick, readers past their `BLOCK` deadline are woken with nil. The same parking pattern is how blocking
commands avoid tying up a thread per blocked client.

## Memory accounting & eviction

`maxmemory` (`LOCUS_MAXMEMORY`) bounds dataset growth. The keyspace keeps an approximate `used_memory`
total: a per-key size estimate is folded in by `resync_size` after every write (so in-place collection
growth counts) and dropped on every removal path. Before a write on a master, the hub evicts arbitrary
keys until under the cap; if it still can't fit, the write is rejected with `OOM`. Evictions are
streamed to replicas/AOF as `DEL` and dirty WATCHers — exactly like a client delete, so snapshots and
replicas stay consistent. Accounting is deliberately coarse (no allocator introspection in zero-deps
`std`) — enough to bound growth, not byte-exact. `INFO` exposes `used_memory` and `maxmemory`.

## The changefeed (reactive layer)

The hub records every keyspace mutation through one `record_change` call, fed from the **same choke
points** as WATCH/AOF/replication (writes via the modified-key set, plus expiry and eviction). From that
one ordered stream it serves three consumption modes — broadcast push (`CDCSUBSCRIBE`), offset-addressed
pull (`CDCREAD`, backed by an optional retained ring), and load-balanced consumer groups
(`CDCREADGROUP`) — and live geofencing (`CDCSUBSCRIBE REGION`). Because subscriber registration and the
initial snapshot happen in the same hub turn, snapshot-then-tail is gap-free and dup-free without
offsets. Full details in [CHANGEFEED.md](CHANGEFEED.md).

## Geo

A geo object is its own key holding a `Geo(lon, lat, attrs)` value. The keyspace keeps a **geohash
spatial index** — a `BTreeMap<u64 cell, keys>` over 52-bit interleaved geohash cells — so `GEOSEARCH`
range-scans only the handful of cells covering the query box (sub-linear) and then refines by true
haversine distance, with optional `WHERE` attribute filters. Because geo writes flow through the
changefeed like any other, a *region* filter yields live geofencing. The same cell id is the cluster
shard key (see Clustering). See [GEO.md](GEO.md).

## Sketches

Bloom, Count-Min, Top-K, and t-digest live in the `sketch` module as `Value` variants. They're
zero-dep (hashing via `std`'s fixed-key `DefaultHasher`, so they're deterministic across runs),
auto-sized, and RDB/AOF-persistent. AOF rewrite restores each via a raw-load command, since a sketch
can't be rebuilt from its add-history. See [SKETCHES.md](SKETCHES.md).

## Secondary indexes

A named index over a hash field is `{ forward: BTreeMap<value → keys>, reverse: key → value }`. After
every write (and on expiry/eviction) the hub re-indexes the touched key from its current state — remove
the old bucket entry, add the new one — in the same hub turn as the write. So the index can never drift
from the data and there is no crash-time reconciliation, the failure mode hand-rolled Redis indexes
suffer. `IDXGET` is an equality lookup; `IDXRANGE` is a lexicographic range over the `BTreeMap`.
In-memory (rebuilt by `IDXCREATE` after a restart).

## Conditional writes (CAS)

`CAS`/`CADEL`/`SETMAX`/`INCRCAP` are atomic check-and-write: because the check and the write happen in
one hub turn, there's no race and no need for `WATCH`/Lua. They log their concrete *effect* to the AOF
(`SET`/`DEL`) so replay and replication stay deterministic.

## Clustering (horizontal spatial sharding)

Cluster mode adds a layer *around* the hub, never inside it — the hub stays single-threaded and
oblivious. A `Cluster` struct holds a 16384-slot `owner` map (from `LOCUS_CLUSTER_NODES`) plus a per-slot
HLC `epoch`.

- **Routing.** Before executing a key command, the hub maps its keys to a CRC16 slot (honoring
  `{hashtag}`) and returns `MOVED`/`CROSSSLOT`/`CLUSTERDOWN` if it isn't the owner. Cluster-aware clients
  follow the redirect — no proxy.
- **Spatial sharding.** With `cell_bits` set, geo keys carry their geohash cell as the hashtag, so a
  region co-locates on one shard. `GEOSEARCH` computes the cells its box covers, maps them to owners, and
  **scatters only to those shards** (bounded fan-out); without cell mode it scatters to all and merges.
- **Inter-node transport.** A tiny RESP client (`cluster_call_int`/`_array`) with short timeouts talks to
  peers. Scatter is **parallelized** (one short-lived thread per peer, joined within ~one timeout) so a
  slow shard can't stall the hub for `peers × timeout`; replies stay synchronous, so connection ordering
  is preserved. Internal verbs `GEOSEARCHSHARD`, `XCDCSINCE`, `XRESTORE`, `XDBSIZE` serve these.
- **Live resharding.** `MIGRATESLOT` dumps a slot's keys (`rdb::dump_entry`) to the destination
  (`XRESTORE`) in a two-phase copy-then-commit, so a key never exists nowhere. `SETSLOT`/`REASSIGN`
  repoint ownership directly.
- **Convergence.** Each ownership change bumps the slot's HLC epoch; a background gossip thread pulls
  peers' `CLUSTER GOSSIP` maps and adopts higher-epoch entries (last-writer-wins), so changes propagate
  without touching every node. The sentinel's `REASSIGN` broadcast gives fast failover; gossip is the
  backstop.
- **Cross-shard changefeed.** Every change gets an HLC stamp (the hub's one ordered point makes it
  monotonic). `CLUSTER CDCMERGE` merges all shards' feeds in HLC order up to a watermark — the min HLC
  floor across reachable shards — which bounds staleness; a previously-seen shard that goes down holds the
  watermark so order is never violated.

What's intentionally *not* here: gossip-based membership/consensus (Raft) — topology comes from config +
epoch anti-entropy, keeping the zero-dependency, no-consensus stance.
