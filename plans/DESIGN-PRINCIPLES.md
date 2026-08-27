# Design Principles — How to Stay Elegant Like Redis

These are the decisions that make Redis loved: small enough to hold in your head, fast because it's
simple, and trustworthy because it never surprises you. They're not accidents to admire from afar —
they're reproducible. Treat this file as your north star; re-read it whenever the codebase starts to
feel heavy.

> antirez maintained Redis solo for over a decade because he treated **simplicity as the feature, not
> a phase to grow out of.** Every principle below is in service of that.

---

## 1. Be a data-structures server, not a database — "Lego for programmers"

Ship general-purpose, **orthogonal** primitives (strings, lists, hashes, sets, sorted sets, streams)
and let users **compose** solutions. A sorted set turns a painful leaderboard query into one O(log n)
command; a HyperLogLog turns cardinality estimation into a 12KB key. Because the server exposes
composable building blocks rather than opinionated end-to-end features, one engine serves caching,
queues, rate limiting, sessions, pub/sub, and leaderboards **without growing a feature for each.**

**How to preserve it:** When tempted to add a feature, ask:
> *"Is this a new general-purpose data structure, or a use-case-specific feature?"*

A leaderboard is a use case — **say no.** A sorted set is a primitive — **say yes.** Make each new type
orthogonal (composable, not overlapping). Keep the operation set per type small and based on the
structure's natural algorithmic strengths (push/pop on lists, rank/range on sorted sets). Never bake a
specific application's policy into a command.

---

## 2. Single-threaded command execution — atomicity from architecture, not locks

Run exactly **one command at a time on one core.** Every command becomes atomic by construction — no
interleaving, no race, no lock, no atomic, no memory barrier, no lock-free structure to get subtly
wrong. The entire concurrency model is "commands are serialized." This is the decision that makes the
codebase readable by one person and makes correctness reasoning *trivial*.

antirez accepts leaving multi-core throughput on the table because the CPU is almost never the
bottleneck — **memory bandwidth and the network are** — and the simplicity is worth more than the cores.

**How to preserve it:** Keep the command-execution path single-threaded and deterministic. In Rust,
model it as a current-thread runtime or one task owning the keyspace. The default answer to "should
this run on another thread?" is **no.** Quarantine threads to clearly bounded, embarrassingly-parallel
side jobs only (fsync, defragmentation, deletion of large objects). Never let two threads touch the
keyspace concurrently. "Make the core multi-threaded for throughput" is a last resort that requires
overwhelming evidence the network/memory bottleneck has actually been removed.

---

## 3. Everything in RAM — disk is secondary and off the request path

Operate on in-memory structures directly; never put a disk seek on the request path. Redis is fast not
because of clever tricks but because the vast majority of requests are **pure memory operations** — no
page cache, no B-tree seeks, no query planner. This is also what keeps the abstraction *honest*: an
in-memory skip list behaves exactly like the textbook says, with predictable O(log n) latency, because
nothing is secretly hitting disk.

**How to preserve it:** Keep the working set in memory and the hot path free of disk I/O entirely.
Resist "just spill cold data to disk transparently" — it reintroduces all the unpredictability you were
avoiding (cache misses, tail latency, eviction policies) and quietly turns clean structures into a
half-database. If you need disk, make it **explicit and off the request path** (snapshots, replication),
never a transparent tier the user can't reason about.

---

## 4. One small, dependency-light, human-readable codebase that fits in one person's head

Redis is a single self-contained codebase with essentially no third-party dependencies — which is why
it compiles to one binary that runs anywhere and stayed maintainable by one author for a decade.
antirez treats source code as **communication**: it "should be read other than being executed, since it
is written by humans for other humans." The smallness is not a stage before growth — it *is* the
feature. A system one person fully understands has fewer bugs and never accumulates "nobody knows why
this is here" rot.

**How to preserve it:** Treat **"the code you don't write"** as the highest-value code — every line
avoided can't break, confuse, or demand maintenance. Keep dependencies near zero (hand-write the RESP
parser; it's ~150 lines and that's the point). Ship a single static binary. Comment **WHY** (the
non-obvious tradeoff), not WHAT. Periodically ask: *"Can a competent engineer read this module top to
bottom and understand it in an afternoon?"* If not, you've already lost the property. Guard the line
count like a budget.

---

## 5. Durability is a dial, not a mandate

Persistence is offered as **orthogonal mechanisms the user opts into**: RDB point-in-time snapshots
(compact, fast, may lose recent writes) and AOF append-only command logs (more durable, larger),
usable alone, together, or **not at all** (pure cache). Durability is a spectrum with real tradeoffs
(throughput vs. data-loss window vs. file size) — let the user pick their point on it instead of paying
for a one-size-fits-all WAL.

**How to preserve it:** Make durability composable and off the default cost path — the in-memory core
should not know or care whether persistence is enabled. Offer a **small number** of clearly-explained
options (snapshot vs. log vs. none) rather than a dozen tunables. Never make persistence a precondition
for using the data structures. A snapshot is a fork-and-dump; an AOF is appending the commands you
already received — both stay simple by design.

---

## 6. A human-readable, trivially-implementable wire protocol (RESP)

RESP is a deliberate compromise between *simple to implement*, *fast to parse*, and *human-readable*.
You can `telnet` in and type commands by hand. antirez's insight:

> "If carefully designed, a simple human-readable protocol is not the bottleneck in client-server
> communication, and the simplicity of the design is a major advantage in creating a healthy client
> libraries ecosystem."

Because anyone can write a client in an afternoon, Redis got high-quality clients in **every language**
— the protocol's simplicity is what bootstrapped the ecosystem that made Redis ubiquitous.
Length-prefixed framing keeps it fast (no delimiter scanning, no escaping) while staying readable.

**How to preserve it:** Design the protocol so a new client can be written in an afternoon. Keep it
text-friendly and inspectable with telnet/netcat. Use prefixed lengths for speed without sacrificing
readability. Treat the ecosystem (every-language client coverage) as a first-class design goal — a
protocol only its author can implement is a moat against your *own* adoption.

---

## 7. Predictable, minimal configuration — every server behaves the same

Default to working well with **zero tuning.** Add a knob ONLY for a genuine, irreconcilable tradeoff
the user must own (durability level, max memory) — never to paper over an indecision in design, and
**never let config change command semantics or correctness.** Config may change the
performance/durability envelope, not the behavior. This gives client authors **one** target.

---

## 8. Defer distributed-systems complexity — ship the brilliant single node first

Make the single node so good most users never need more. Then add replication/sharding as **layers on
top** that the command-execution path doesn't even know about — never woven through the core. Prefer
simple, understandable schemes (async replication, hash slots) over heavyweight consensus you'll spend
years debugging.

This ordering is *why* Redis stayed maintainable: premature distribution poisons the readable core with
coordination logic touching every command.

---

## Anti-patterns to avoid (the temptations that betray the elegance)

- **Feature creep** — adding use-case features instead of general-purpose primitives (see #1).
- **Premature distributed-systems complexity** — building clustering/consensus before the single node
  is great (see #8).
- **Threads everywhere** — reaching for multi-threaded execution before proving CPU is the bottleneck
  (see #2).
- **Config sprawl** — a knob for every indecision, knobs that change behavior/correctness (see #7).
- **Transparent disk tiers** — silently spilling to disk and destroying latency predictability (see #3).
- **Dependency creep** — pulling a framework for something you could hand-write in 150 readable lines
  (see #4).
