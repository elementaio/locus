# Hard Parts — Here Be Dragons

What's *deceptively easy* (looks hard, isn't) vs. *genuinely hard* (looks easy, isn't). Read this
before each relevant milestone so you respect the difficulty where it actually lives. The throughline:
**a toy passes basic GET/SET; a real database survives crashes, partitions, and adversarial inputs
without losing or corrupting data.**

---

## 1. RESP parsing — deceptively easy (relevant: M1, M12)

The grammar is trivial. The trap is that **you don't own TCP message boundaries.** One `read()` can
return half a command, one-and-a-half, or 50 pipelined together.

- Build a **resumable state machine** over a persistent per-connection buffer that yields
  zero-or-more complete commands and **preserves the partial tail** — NOT a line reader.
- Edge cases that bite: bulk strings spanning many TCP segments; inline (non-RESP) commands; **protocol-
  error desync policy** (once framing is lost you can't trust the stream — reply error and close); huge
  declared lengths as a **DoS** (enforce a max bulk length *before* allocating).
- **Test:** feed every command 1 byte at a time, then all at once, then in random splits — output must
  be byte-identical.

---

## 2. Data-type encodings & conversion thresholds — genuinely hard (relevant: M4, M5, M12)

This is **THE line between toy and real**, and it disguises itself as a deferrable optimization. Redis
stores each type compactly while small (listpack/intset) and promotes past exact, config-driven
thresholds (`hash-max-listpack-entries 128`, `-value 64`; `set-max-intset-entries 512`).

- `OBJECT ENCODING` **exposes** the encoding, so tests and tooling *assert* on it — getting it wrong is
  a **visible compatibility bug**, not a perf nuance.
- Memory efficiency — Redis's actual selling point at scale — lives almost entirely here.
- **Decide your fidelity goal up front:** *functional* (native structures, several-x heavier, "wrong"
  encoding) vs. *wire-compatible* (implement the real compact byte-buffer structures and exact
  promotion rules).
- The **sorted set needs two coordinated indexes** (skiplist + hashtable) kept perfectly in sync —
  **build it dual from day one;** retrofitting the second index is painful.

---

## 3. Key expiration — easy core, subtle everywhere else (relevant: M3, then M9)

Lazy (delete-on-access) is genuinely easy. Everything around it is subtle.

- **Active expiration is an adaptive control problem** — probabilistic sampling under a CPU-time
  budget: aggressive enough to bound stale memory, gentle enough not to spike latency.
- **The real dragon appears at M9 (replication):** replicas must **not** expire keys autonomously (or
  you'd read a key on the master and get nil on a replica, breaking consistency). Only the master
  expires and propagates an explicit `DEL`; replicas hide-but-don't-delete logically-expired keys for
  reads. Getting this wrong produces **non-deterministic, replication-dependent data loss** that's
  nearly impossible to reproduce.
- Pin expiry observation time at command/script start for determinism.

---

## 4. AOF durability & crash recovery — genuinely hard (relevant: M7)

Where careers learn humility. **fsync is the whole ballgame.**

- Append-without-fsync just writes to the OS page cache — a power loss loses it. So `everysec` (lose up
  to ~1s) must run fsync on a **background thread**, never the event loop.
- **Torn writes:** a crash mid-append leaves a truncated final command; recovery must scan to the last
  complete command and truncate — not refuse to start, not replay garbage.
- **AOF rewrite is the deep hazard:** writes keep arriving *during* compaction, so you buffer them and
  stitch base+incremental via an **atomically-swapped manifest.** A single mis-order or lost diff buffer
  **silently drops writes** you won't discover until a crash months later.
- Non-deterministic commands (`SPOP`/`EXPIRE`) must be logged as their **concrete effects** or replay
  diverges.
- **Mandatory, brutal acceptance test:** a harness that `SIGKILL`s at thousands of points (esp.
  mid-rewrite/mid-fsync) and verifies you lose at most the promised window and **never** corrupt.
  *Untested recovery code is, statistically, broken recovery code.*

---

## 5. Replication consistency — genuinely hard (relevant: M9)

A one-paragraph idea hiding a distributed-systems thesis.

- **Async (the default) is fast but NOT strongly consistent** — an ack'd write can be lost if the
  master dies before propagating; `WAIT` only *bounds*, not eliminates, this.
- **The seam is the danger:** initial sync needs a consistent base (snapshot) PLUS every write during
  the transfer (backlog-buffered, streamed after) with **no gap and no duplicate** at the join point.
- **PSYNC partial resync** needs replication IDs + offsets, and the **ID-changes-on-failover** logic (so
  a stale replica with the same offset under a new history doesn't silently diverge) is genuinely subtle.
- The stream must be **deterministic** (reuse your AOF effect-propagation rules).
- **Be explicit and honest** about your consistency model; document the data-loss window on failover.
- **Test under a network-partition / packet-loss simulator** — a happy-path two-node test proves almost
  nothing.

---

## 6. Tail latency / GC & allocation stalls — the canonical "looks easy, is brutal" (relevant: M12)

This is *the* reason Redis is in C and *the* reason you chose Rust over Go/Elixir.

- A single-threaded loop means **any** pause (stop-the-world GC, a big allocation, a synchronous free of
  a giant key) freezes **every client at once** — turning a 10ms pause into 10ms added to *thousands* of
  in-flight requests.
- A database is millions of small long-lived objects — the **pathological case for tracing GCs.**
- Specific traps: building a huge reply (`SMEMBERS` on a million elements) spikes allocation; deleting a
  giant key synchronously stalls the loop (Redis added `UNLINK`/lazyfree precisely for this).
- **Measure the tail** (p99/p999) with coordinated-omission-correct tooling (wrk2/HdrHistogram), **never
  the mean.** Engineer to avoid per-op allocation (buffer/object pools, pre-sized buffers), cap any
  single command's cost (paginate big replies via cursors, lazy-free big deletes on a background thread).

---

## 7. Concurrency correctness if you ever deviate from single-threaded — genuinely hard

The single thread is Redis's secret weapon: every command atomic by construction, MULTI/EXEC and scripts
serializable for free, zero data races on the keyspace.

- The moment you add threads **for command execution** to use more cores, you inherit
  distributed-systems-in-one-process: the shared keyspace needs locking or sharding, every atomicity
  guarantee clients depend on needs careful synchronization, and you get races that only appear under
  production contention.
- **Default to single-threaded execution — it's a FEATURE.** If you must scale cores, prefer in order:
  1. independent share-nothing **shards**,
  2. then **I/O-only threads** (read/parse/write) with execution still serialized,
  3. then isolated **background jobs** (fsync, lazy-free) with a tiny audited handoff.
- Only as a last resort share the keyspace — and if you do, design the model *before* writing code and
  test with a race detector and **linearizability checks**, not unit tests.

---

## The toy → real checklist

You've crossed from toy to real when:

- [ ] The parser is byte-identical under 1-byte-at-a-time, all-at-once, and random-split input.
- [ ] A protocol error closes the connection cleanly instead of desyncing the stream.
- [ ] A max-bulk-length cap rejects adversarial huge declarations before allocating.
- [ ] `OBJECT ENCODING` matches Redis for your supported types (if you chose wire-fidelity).
- [ ] A crash-injection harness proves AOF recovery loses ≤ the promised window and never corrupts.
- [ ] Replication is tested under simulated partitions, and your consistency model is documented.
- [ ] p99/p999 latency is measured (coordinated-omission-correct), and no single command can freeze the
      loop unbounded.
