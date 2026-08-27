# The Roadmap — M0 → M12

Each milestone is validated by the **real `redis-cli`**. Build in Rust on a current-thread tokio
runtime (or one task owning the keyspace via an `mpsc` channel). Start with
[00-first-step.md](00-first-step.md).

---

## M0 — TCP server that PONGs (the dopamine hit)
**Goal:** the smallest possible end-to-end win — the real `redis-cli` talks to code you wrote.
**Effort:** 1–3 hours (a single evening).

**Deliverable:** A Rust project (`cargo new`) listening on TCP `:6379` that accepts a connection and
replies `+PONG\r\n` to *anything* — reply hardcoded, no parsing.
Validate: `redis-cli -p 6379 ping` prints `PONG`.

**Key concepts:** Raw TCP listen/accept/read/write loop; the RESP simple-string frame (`+...\r\n`)
just enough to fake one reply; the realization that a server is "read bytes, write bytes" and the
real client already trusts you.

---

## M1 — RESP parser + ECHO + SET/GET on an in-memory map
**Goal:** make the reply actually depend on what was sent — the conceptual core of the whole project.
**Effort:** 1 full day (weekend day 1).

**Deliverable:** A real *resumable* RESP2 parser decoding Array-of-Bulk-Strings into command tokens;
dispatch for `PING`/`ECHO`/`SET`/`GET` against an in-memory `HashMap` owned by one task.
`redis-cli set foo bar` then `get foo` returns `bar`.

**Key concepts:** Binary protocol parsing as a **state machine over an accumulating buffer** (you
don't own TCP message boundaries); RESP's 5 types and length-prefixed framing; command dispatch;
*"a database is a hashmap behind a network protocol."* **The single most important milestone** —
every source treats RESP + SET/GET as the core.

---

## M2 — More string commands + concurrency model
**Goal:** handle many simultaneous clients and *feel* why Redis serializes commands.
**Effort:** 1 day.

**Deliverable:** Multiple concurrent clients; add `DEL`, `EXISTS`, `INCR`/`DECR` (atomic; error on
non-int), `APPEND`, `STRLEN`, `GETDEL`, `TYPE`, plus stubs for `COMMAND DOCS` and `CONFIG GET` so
`redis-cli` stops complaining. Keep the single-owner-keyspace (or current-thread) model so every
command is atomic by construction.

**Key concepts:** The concurrency model and the atomicity tradeoff — in Rust you'll feel the borrow
checker push back on `Arc<Mutex<HashMap>>`, and **that friction IS the lesson** in why Redis chose
single-threaded. RESP integer and error reply encoding. Keep every command O(1)-ish: one O(N)
command blocks every client at once.

---

## M3 — Key expiry: passive + active (your first real systems algorithm)
**Goal:** reclaim memory from TTL'd keys without scanning everything — a genuine control problem.
**Effort:** 1 day.

**Deliverable:** `SET` with `PX`/`EX`/`EXAT`/`PXAT`, plus `EXPIRE`/`PEXPIRE`/`TTL`/`PTTL`/`PERSIST`.
- **Passive:** on access, an expired key is deleted and returns nil.
- **Active:** a background task samples ~20 random TTL keys, deletes expired ones, repeats if >25%
  were expired, all under a time budget.

**Key concepts:** Probabilistic sampling vs. full scan; the memory-vs-CPU tradeoff; monotonic vs.
wall-clock time and TTL precision. The active-expire **sampling algorithm** is one of the hard parts
BEAM would have hidden — lean in. (Replica-side expiry rules come later with M9; single-node ignores them.)

---

## M4 — Data types: Lists, Hashes, Sets (+ blocking BLPOP)
**Goal:** a value is a *typed object*, not just a string — and meet your first blocking command.
**Effort:** 2–3 days (BLPOP is a meaty sub-project).

**Deliverable:**
- Lists: `RPUSH`/`LPUSH`/`LRANGE` (with negative indices)/`LLEN`/`LPOP`/`RPOP`/`LINDEX`
- Hashes: `HSET`/`HGET`/`HGETALL`/`HDEL`/`HLEN`
- Sets: `SADD`/`SMEMBERS`/`SISMEMBER`/`SCARD`/`SREM`
- `WRONGTYPE` errors; optional `BLPOP` that parks a client until an element arrives or times out.

**Key concepts:** Typed value objects; the `WRONGTYPE` invariant; data-structure choice per type.
`BLPOP` teaches a **blocking-command state machine** — in Rust this is a notify/wait across tasks
(in C it's condition variables); a clean systems lesson either way.

---

## M5 — Sorted sets / skiplist (the algorithmic deep end)
**Goal:** maintain elements ordered by score AND give O(log n) rank/range — why a hashmap isn't enough.
**Effort:** 2–4 days (more for a real skiplist).

**Deliverable:** `ZADD`/`ZSCORE`/`ZRANK`/`ZRANGE`/`ZRANGEBYSCORE`/`ZCARD`/`ZREM`/`ZINCRBY`.
Get it correct first with *any* ordered structure, then upgrade to the real dual index: a **skiplist
ordered by score PLUS a hashtable member→score**, kept perfectly in sync.

**Key concepts:** The most algorithmically rich type and a **genuine hard part**: the dual
skiplist + hashtable index (build it dual from day one — retrofitting the second index is painful),
probabilistic level assignment, span counts for O(log n) `ZRANK`. Ordered-index design recurs in every
database. Geo (`GEOADD`/`GEOSEARCH`) is an optional extension on top via geohash scoring.

---

## M6 — RDB-style snapshot persistence
**Goal:** survive a restart; design an on-disk binary format and a consistent snapshot strategy.
**Effort:** 2–3 days.

**Deliverable:** `SAVE`/`BGSAVE`: serialize the whole dataset to a binary file and load it on startup.
Start with your own length-prefixed format covering string/list/hash/set/zset + per-key expiry;
optionally match enough real RDB that `redis-server` can load your file. **Write to temp, fsync,
atomic-rename** — a half-written file must never replace a good one.

**Key concepts:** Binary serialization, type tags, variable-length encoding, endianness; snapshot
**isolation**. Real Redis `fork()`s for copy-on-write; in Rust on a single-threaded runtime you instead
capture a consistent serializable view (or snapshot the structure) and write it from a background task
— a deliberate contrast to fork+COW. First milestone where "survives a crash" is real.

---

## M7 — AOF (append-only file) + crash recovery
**Goal:** bounded, documented durability — the most important concept in all of databases.
**Effort:** 2–3 days.

**Deliverable:** Append every **write** command (filter reads!) to a log in RESP; replay on startup.
fsync policy (`always`/`everysec`/`no`) with `everysec` on a **background thread**; AOF
rewrite/compaction (collapse to a minimal command set) via base+incremental tied by an
atomically-swapped manifest; torn-tail-tolerant loader (scan to last complete command).

**Key concepts:** Write-ahead logging; fsync-frequency vs. data-loss-window tradeoff; log compaction
without losing concurrent writes; rewriting non-deterministic commands (`SPOP`/`EXPIRE`) to their
*effects*. A **genuinely hard part**: recovery code is broken until a crash-injection harness (SIGKILL
at thousands of points, esp. mid-rewrite/mid-fsync) proves it loses at most the promised window and
never corrupts. See [HARD-PARTS.md](HARD-PARTS.md).

---

## M8 — Pub/Sub
**Goal:** fan-out messaging and a connection state machine — directly transferable to Pulsar.
**Effort:** 1–2 days.

**Deliverable:** `SUBSCRIBE`/`UNSUBSCRIBE`/`PSUBSCRIBE`/`PUBLISH`, plus "subscribed mode" (a subscribed
client may only run `SUBSCRIBE`/`UNSUBSCRIBE`/`PING`/`QUIT`). `PUBLISH` routes to every subscriber of a
channel and returns the subscriber count.

**Key concepts:** Channel registry → subscriber routing; pattern matching; the subscribed-mode state
machine. This is **literally the fan-out pattern at the heart of your Pulsar and chat engine** — direct,
transferable practice. (Caveat to internalize: classic pub/sub is at-most-once with no persistence;
Streams are the durable answer.)

---

## M9 — Replication (master + replica)
**Goal:** the deepest distributed-systems lesson — full-sync-then-stream, offsets, acks.
**Effort:** 4–7 days (the hardest milestone).

**Deliverable:** A replica started with `--replicaof` does the handshake
(`PING`→`REPLCONF`→`PSYNC`), receives a full RDB snapshot, loads it, then applies a live stream of
writes. Master tracks a replication offset, propagates writes, handles `REPLCONF GETACK` and `WAIT`
(block until N replicas ack an offset), and reports `INFO replication`.

**Key concepts:** A **genuinely hard part**: the full-sync seam (snapshot + backlog-buffered diff with
no gap/dup), replication IDs + offsets (change the ID on promotion so a stale replica can't silently
diverge), deterministic effect-propagation (reuse your AOF rules), and the honest truth that **async
replication is NOT strongly consistent** — acknowledged writes can be lost on failover; `WAIT` bounds
but doesn't eliminate this. Also: replicas must **not** expire keys autonomously — the master
propagates explicit `DEL`s. Test under a partition simulator; a two-node demo proves almost nothing.

---

## M10 — Transactions (MULTI/EXEC/WATCH)
**Goal:** atomicity and optimistic concurrency control — database transactions in miniature.
**Effort:** 2–3 days.

**Deliverable:** `MULTI` (queue), `EXEC` (run the batch atomically, no interleaving), `DISCARD`, and
`WATCH`/`UNWATCH` optimistic locking (`EXEC` aborts to nil if any watched key changed since `WATCH`).
Distinguish queue-time errors from runtime errors per Redis semantics; per-connection transaction state.

**Key concepts:** Command queueing; all-or-nothing execution (which your serialized keyspace gives
almost for free); `WATCH` = compare-and-swap (check-version-then-commit). Note Redis transactions have
**no rollback** — a runtime error doesn't undo earlier commands; only queue-time syntax errors abort the
batch. Contrast with SQL semantics.

---

## M11 — Streams (XADD/XRANGE/XREAD + blocking + groups)
**Goal:** append-only log structures and ID-ordering — a mini Kafka inside your server.
**Effort:** 3–5 days.

**Deliverable:** `XADD` (auto and partial ms-seq IDs), `XLEN`, `XRANGE` (`-` and `+` bounds), `XREAD`
(single/multi-stream), blocking `XREAD` (`BLOCK` timeout, `$` for new-only). Optional consumer groups
(`XGROUP`/`XREADGROUP`/`XACK`).

**Key concepts:** Monotonic ms-sequence ID generation/ordering; blocking reads against a growing log
(same dopamine as `BLPOP`, richer structure). **Directly relevant to the firehose/event nature of
Pulsar.** (Real Redis backs this with a radix-tree-of-listpacks + a PEL for consumer groups — both
advanced/optional; a sorted structure is fine first.)

---

## M12 — RESP3, pipelining, and benchmarking (make it FAST and real)
**Goal:** turn "works" into "real" with protocol versioning and measured performance engineering.
**Effort:** 3–5 days, open-ended.

**Deliverable:** RESP3 support (`HELLO` command + map/set/double/big-number/push types) so modern
clients negotiate up; verify pipelining (many commands per recv buffer — your state-machine parser
already handles this); then run the **official** `redis-benchmark -p 6379 -t set,get,incr -n 100000 -P 16`,
find bottlenecks, optimize (avoid per-command allocation via buffer pools, batch socket writes,
paginate big replies, lazy-free big deletes).

**Key concepts:** Protocol versioning; pipelining = throughput from amortizing round-trips;
**performance engineering** with coordinated-omission-correct measurement (wrk2/HdrHistogram for
p99/p999, not mean ops/sec). A **genuinely hard part** surfaces here: **tail latency** — in a
single-threaded loop, one O(N) command or one big synchronous free freezes every client at once, which
is exactly why Redis is in a non-GC language and why you chose Rust. This is also where Redis's compact
encodings (listpack/intset/quicklist) would buy memory fidelity if you pursue wire-compatibility — an
optional deep extension.

---

## Where to stop

You do **not** need M9–M12 to have built "something like Redis." A faithful single node through
**M6/M7** (data types + snapshot + AOF) is already a real, restart-surviving, `redis-cli`-compatible
in-memory database you'd be proud of. M8 (pub/sub) is a quick, high-value win that transfers straight to
your Pulsar work. M9+ are the distributed-systems graduate course — take them when you want that, not
because the list says so. **Defer distributed complexity; ship the brilliant single node first.**
