# Locus

> A Redis-like in-memory database, built from scratch — evolving toward a **geo-first** store
> (see [LANDSCAPE.md](LANDSCAPE.md), [CLUSTER.md](CLUSTER.md), [DIFFERENTIATORS.md](DIFFERENTIATORS.md)).
>
> **Name:** Locus (Latin, "a place"). **Port:** 6379. **Language:** Rust, zero dependencies.
>
> ✅ **Built: M0–M12 are implemented and committed in [locus/](locus/)** — see [BUILD.md](BUILD.md).
> ~3,750 lines, 16 tests, verified against the real `redis-cli`/`redis-benchmark`. The docs below are
> the original plan; the code now exists.

---

## Why this is the perfect project for you

A database is **a hashmap behind a network protocol.** That's the whole secret, and it's
why this is approachable: the very first weekend, the *real* `redis-cli` — software that has
no idea you exist — will connect to ~40 lines you wrote and print `PONG`. Then
`SET foo bar` / `GET foo` will round-trip through your code.

That moment ("a real Redis client is talking to **my** server") is the hook, and it never
stops paying out, because **every milestone is validated by unmodified official Redis tooling**
(`redis-cli`, `redis-benchmark`) that doubles as your free conformance suite. You're not
building a toy that grades itself — you're building something the Redis ecosystem accepts as Redis.

You love Redis for the right reason: it's the rare piece of infrastructure whose entire mental
model fits in your head — *one thread, one command at a time, atomic by construction, data in RAM,
~10 well-understood data structures* — and it never betrays that trust. That's not magic to admire
from afar; it's a set of design decisions you can **reproduce**. antirez maintained Redis solo for a
decade because he treated **simplicity as the feature, not a phase to grow out of.** Hold that line
and you'll end up with something elegant, fast, real, and unmistakably yours.

---

## The decision: build it in Rust

The research genuinely disagreed here, so here's the decisive call **for your stated goal**
(learn systems internals by building "something like Redis"):

| Option | Score | Verdict |
|---|---|---|
| **Rust** | **9/10** | **The pick.** C-class performance (no GC, predictable tail latency) so the artifact can become *genuinely fast and real*; the borrow checker eliminates the memory-bug class that stalls newcomers in C — and, crucially, **makes the lesson for you**: every time you reach for `Arc<Mutex<HashMap>>` it forces you to confront *why* Redis is single-threaded. Plus [tokio mini-redis](https://github.com/tokio-rs/mini-redis) is a complete, readable RESP skeleton to extend. |
| C | 7/10 | The "truth serum" — real Redis *is* single-threaded C, so lesson and artifact are identical. Choose only if "maximally educational, I accept segfaults" is your definition of success. |
| Zig | 7/10 | Connoisseur's pick (explicit allocators, io_uring) but pre-1.0 churn + thin ecosystem drag on a newcomer. |
| Go | 6/10 | The trap: fastest path to a working `PING` tonight, but **you already know Go**, and its runtime *hides the netpoller and GC* — the two lessons that define Redis. Use Go fluency to **read** mini-redis with confidence, then build in Rust. |
| Elixir/BEAM | 5/10 | Do **not** make this the main vehicle. BEAM is the philosophical *opposite* of Redis (millions of share-nothing processes vs. one thread owning one keyspace) and hands you concurrency + ETS *for free* — skipping the exact bootcamp you came for. It's the perfect **side realization** though: a BEAM ETS/GenServer KV store is the right architecture for a *clustered, fault-tolerant* state store — closer to what your chat engine and Pulsar actually *need* from Redis than to Redis's internals. |

**Build it on a current-thread tokio runtime** (or one task owning the map, clients talking to it over
an `mpsc` channel) to faithfully mirror Redis's single-threaded core — then later shard it to *feel*
the multi-core tradeoff firsthand.

---

## The roadmap at a glance

Full detail in [ROADMAP.md](ROADMAP.md). Start with [00-first-step.md](00-first-step.md) **this weekend.**

| # | Milestone | Effort | The lesson |
|---|---|---|---|
| **M0** | TCP server that PONGs | 1–3 hrs | A server is "read bytes, write bytes" — and the real client already trusts you |
| **M1** | RESP parser + ECHO + SET/GET | 1 day | A database is a hashmap behind a protocol; parsing as a state machine |
| **M2** | More string cmds + concurrency model | 1 day | *Why* Redis serializes commands (the borrow checker teaches it) |
| **M3** | Key expiry: passive + active | 1 day | Your first real systems algorithm: probabilistic sampling under a time budget |
| **M4** | Lists, Hashes, Sets (+ blocking BLPOP) | 2–3 days | Typed value objects; the WRONGTYPE invariant; blocking-command state machine |
| **M5** | Sorted sets / skiplist | 2–4 days | The algorithmic deep end: dual skiplist + hashtable index |
| **M6** | RDB-style snapshot persistence | 2–3 days | Binary serialization + snapshot isolation; survive a restart |
| **M7** | AOF + crash recovery | 2–3 days | Write-ahead logging; fsync vs data-loss-window — *the* database concept |
| **M8** | Pub/Sub | 1–2 days | Fan-out messaging — **literally the Pulsar/chat pattern** |
| **M9** | Replication (master + replica) | 4–7 days | The deepest distributed-systems lesson; the hardest milestone |
| **M10** | Transactions (MULTI/EXEC/WATCH) | 2–3 days | Atomicity + optimistic concurrency control |
| **M11** | Streams (XADD/XREAD + groups) | 3–5 days | Append-only logs — a mini Kafka inside your server |
| **M12** | RESP3, pipelining, benchmarking | 3–5 days | Turn "works" into "real": tail-latency performance engineering |

> This ladder closely mirrors the [CodeCrafters "Build Your Own Redis"](https://app.codecrafters.io/courses/redis/)
> stage groupings — you can use their test suite as a structured backbone.

---

## Files in this folder

| File | What's in it |
|---|---|
| [README.md](README.md) | This — the pitch, the language decision, the overview |
| [ROADMAP.md](ROADMAP.md) | The full M0–M12 milestone ladder with deliverables and concepts |
| [00-first-step.md](00-first-step.md) | **Start here.** The weekend spec for M0+M1, with wire protocol + pseudocode |
| [DESIGN-PRINCIPLES.md](DESIGN-PRINCIPLES.md) | The 8 principles that keep it elegant like Redis — your north star |
| [HARD-PARTS.md](HARD-PARTS.md) | "Here be dragons" — what's deceptively easy vs. genuinely hard |
| [REFERENCE.md](REFERENCE.md) | RESP cheat sheet, data-type encodings, durability matrix, all resources |
| [LANDSCAPE.md](LANDSCAPE.md) | Competitive landscape — who else solved in-memory geo, and the white space |
| [CLUSTER.md](CLUSTER.md) | Cluster-mode design — spatial sharding + the BEAM/Rust polyglot split |
| [DIFFERENTIATORS.md](DIFFERENTIATORS.md) | What Locus has that Redis doesn't — the build/skip list, ranked |
| [INTEGRATION.md](INTEGRATION.md) | How Locus + the BEAM chat engine work hand in hand (port → primitive map) |
| [COMMANDS.md](COMMANDS.md) | The command surface — the useful Redis subset + geo/reactive identity, and what's skipped |

---

## Start this weekend

```bash
brew install redis            # gives you redis-cli + redis-benchmark = your test harness
cargo new myredis && cd myredis
# ... follow 00-first-step.md ...
# Saturday: redis-cli -p 6379 ping   -> PONG
# Sunday:   redis-cli -p 6379 set foo bar && redis-cli -p 6379 get foo  -> bar
```

**The one rule, forever:** never write your own client. Drive every milestone with the official
`redis-cli`. The unmodified tool is both your motivation engine and your free conformance test.

---

## The north star (don't lose this)

> **Be a data-structures server, not a database.** When tempted to add something, ask:
> *"Is this a new general-purpose data structure, or a use-case-specific feature?"*
> A leaderboard is a use case — say no. A sorted set is a primitive — say yes.

That one discipline is how a tiny codebase serves infinite use cases. It's why Redis is loved.
See [DESIGN-PRINCIPLES.md](DESIGN-PRINCIPLES.md) for all eight.
