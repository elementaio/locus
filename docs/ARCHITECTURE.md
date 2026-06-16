# Architecture

Locus is small on purpose. This document explains how it's put together and the design choices behind
it. The whole server is ~3.8k lines of `std`-only Rust across 8 modules.

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
  state, per-client transaction state, and parked blocking-readers. It processes one message at a time:
  a new connection, a command, a disconnect, or a replica snapshot. Because all command execution
  funnels through here, it's serialized — atomic by construction.

This is the Redis bet expressed in Rust: trade multi-core throughput for simplicity and predictable
latency. The borrow checker actively reinforces it — the moment you reach for shared mutable state you
feel why the single-owner model is cleaner.

## Modules

| Module | Responsibility |
|---|---|
| `resp` | RESP2 wire protocol: the resumable parser and the reply encoders. |
| `db` | The keyspace, the typed `Value` enum, and key expiration (passive + active). |
| `commands` | Command dispatch and the per-type implementations (strings, lists, hashes, sets, sorted sets). |
| `streams` | The stream type and its commands (XADD/XRANGE/XREAD). |
| `pubsub` | The publish/subscribe registry, glob matching, and message encoders. |
| `rdb` | Binary snapshot serialization (save/load + in-memory serialize for replication). |
| `aof` | The append-only log: append, replay, rewrite, and command rewriting for determinism. |
| `main` | The hub, the connection threads, and replication. |

## Values and expiry

A key maps to a `Value` — one of `Str`, `List`, `Hash`, `Set`, `ZSet`, or `Stream`. Commands type-check
and return `WRONGTYPE` on a mismatch. Empty collections are removed automatically (as in Redis).

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
