# Reference — Protocol, Encodings, Durability, Languages, Resources

Quick-reference tables and the curated link set. Keep this open while building.

---

## Why Redis is fast (the compounding stack — not one trick)

1. **Everything in RAM** — no disk seeks/page faults on the hot path; lookups are pointer chases at
   memory speed. RAM is ~100,000× faster than disk for random access. *The single biggest factor.*
   Durability is offloaded to async RDB snapshots + the AOF log, off the request path.
2. **Single-threaded command execution** — one thread owns all data structures, so **zero** locks,
   mutexes, atomics, or cache-line contention; no context switches, no lock convoys, no deadlocks. For
   microsecond ops, coordination cost would dwarf the work. Also makes operations atomic for free.
3. **I/O multiplexing (epoll/kqueue)** — one thread handles tens of thousands of connections by blocking
   once in `epoll_wait` and waking only for ready fds (O(ready), not O(connections)). No
   thread-per-connection overhead.
4. **Efficient, adaptive encodings** — small collections use compact, cache-friendly, contiguous,
   pointer-free layouts (listpack/intset/embstr) that fit in CPU cache and avoid allocator overhead and
   pointer chasing.

---

## RESP2 wire format cheat sheet

`SET key value` on the wire:
```
*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
```

| Prefix | Type | Example | Null form |
|---|---|---|---|
| `+` | simple string | `+OK\r\n` | — |
| `-` | error | `-ERR unknown command\r\n` | — |
| `:` | integer | `:1000\r\n` | — |
| `$` | bulk string | `$5\r\nhello\r\n` | `$-1\r\n` |
| `*` | array | `*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n` | `*-1\r\n` |

- **Length-prefixed + CRLF-terminated** → the parser never scans for delimiters inside payloads
  (binary-safe, fast).
- **Inline commands:** if the first byte is not `*`, parse the line as space-separated args (telnet
  `PING\r\n` → `+PONG\r\n`). Real clients use multibulk.
- **RESP3** (Redis 6+, opt-in via `HELLO 3` as the first command) is a superset adding `_` null,
  `#` boolean, `,` double, `(` big number, `%` map, `~` set, `>` push, etc. Implement in M12.

---

## Data-type encodings (the M4/M5/M12 fidelity work)

Each type has a compact "small" encoding that **promotes** past config thresholds. `OBJECT ENCODING`
exposes the current one — tooling asserts on it.

| Type | Small encoding | Large encoding | Notes |
|---|---|---|---|
| **String** | `int` (long inline; 0–9999 shared) · `embstr` (≤44B, robj+SDS in one alloc, immutable) | `raw` (separate SDS alloc) | |
| **List** | `listpack` (single contiguous pack) | `quicklist` (doubly-linked list of listpacks, interior nodes optionally LZF-compressed) | |
| **Hash** | `listpack` (alternating field,value; linear scan) | `hashtable` (chained dict w/ incremental rehashing) | |
| **Set** | `intset` (sorted fixed-width int array, binary search, 16/32/64-bit auto-upgrade) · `listpack` (small mixed/string) | `hashtable` (dict with NULL values) | |
| **Sorted set** | `listpack` ((member,score) pairs inline) | `skiplist` (probabilistic multi-level, ordered by score) **+ companion dict** (member→score) | **Build dual from day one** |
| **Stream** | — | `rax` (radix tree) keyed by entry ID; leaves pack entries into delta-encoded listpacks (~100 entries / 4096B per node) + PEL for consumer groups | Advanced/optional |
| **Bitmap / Bitfield / HLL** | stored **inside a String** (SDS buffer) | — | Bitmap = raw bit-array view; HLL = opaque blob w/ magic header |
| **Geo** | stored as a **Sorted Set** | — | (lon,lat) interleaved into a 52-bit geohash integer used as the ZSET score |

The main keyspace is a **dict (hashtable) with incremental rehashing** — it grows/shrinks by migrating
a few buckets per operation so a resize never stalls the loop.

---

## Durability & distribution — what to build vs. skip

| Feature | Needed for MVP? | Difficulty | Milestone |
|---|---|---|---|
| RDB snapshots (point-in-time) | ✅ Yes | Medium (fork + copy-on-write) | M6 |
| AOF + rewrite/compaction | ➖ No (but high value) | Medium→Hard | M7 |
| fsync policies (always/everysec/no) | ➖ No | Easy→Medium | M7 |
| Hybrid RDB+AOF | ➖ No | Hard | post-M7 |
| Pub/Sub | ➖ No (easy & transferable) | Easy→Medium | M8 |
| Async master/replica replication | ➖ No | Medium | M9 |
| PSYNC / partial resync + backlog | ➖ No | Hard | M9 |
| Read replicas | ➖ No | Easy once replication exists | M9 |
| Transactions (MULTI/EXEC/WATCH) | ➖ No | Medium | M10 |
| Keyspace notifications | ➖ No | Medium | post-M8 |
| Lua scripting / Functions | ➖ No | Hard (embed an interpreter) | optional |
| Redis Sentinel (HA) | ➖ No | Hard (small distributed system) | optional |
| Redis Cluster (16384 slots, gossip, MOVED/ASK) | ➖ No | **Hardest item** | optional |

**Durability tradeoff in one line:** RDB = compact, fast, may lose recent writes (snapshot interval);
AOF = larger, more durable, loss window = your fsync policy. Offer **snapshot vs. log vs. none** and let
the user pick — don't force one WAL on everyone.

---

## Language comparison (for *this* goal: learn systems internals)

| Language | Fit | What it teaches | The catch |
|---|---|---|---|
| **Rust** | **9/10 — the pick** | ~90% of the real Redis lessons; highest realistic ceiling (C-class perf, predictable tail latency); cleanest on-ramp (mini-redis) | Borrow-checker learning curve — *but that friction IS the single-threaded lesson* |
| **C** | 7/10 — truth serum | The lesson and the artifact are *identical* to real Redis | Segfaults, manual everything, slow going for a newcomer |
| **Zig** | 7/10 — connoisseur | C's depth + modern ergonomics, explicit allocators, io_uring | Pre-1.0 churn + thin ecosystem = constant Googling |
| **Go** | 6/10 — the trap | Fastest to a working toy | You already know it, and the runtime **hides the netpoller and GC** — the two lessons that define Redis |
| **Elixir/BEAM** | 5/10 — wrong tool *here* | Deep BEAM + distributed-systems lessons (which you're already learning) | Philosophically *opposite* to Redis; hands you concurrency + ETS for free, skipping the bootcamp you came for |

**Recommendation:**
- **Learn fastest given you know Go:** use that fluency to *read* mini-redis with confidence, then build
  in **Rust**.
- **Serious/fast result:** **Rust.**
- **Most interesting given your BEAM journey:** keep BEAM for what it genuinely owns — a clustered,
  fault-tolerant state store (closer to what your chat engine + Pulsar *need* from Redis than to Redis's
  internals). Build the *Redis-learning* project in Rust; let BEAM teach you the distributed half of the
  world. A quick Go warm-up tonight is fine — just don't let it become the project.

---

## Resources (curated, in order of usefulness)

| Resource | What | Link |
|---|---|---|
| **CodeCrafters — Build Your Own Redis** | THE canonical interactive challenge: ~115 test-driven stages (base + RDB, AOF, replication, streams, transactions, lists, pub/sub, sorted sets, geo). Their suite drives your server with the real protocol = a conformance test. Rust is first-class. This M0–M12 ladder mirrors its groupings. | https://app.codecrafters.io/courses/redis/ |
| **CodeCrafters `course-definition.yml`** | The free, authoritative YAML of every stage + extension in order — a detailed checklist even without a paid account. | https://github.com/codecrafters-io/build-your-own-redis/blob/main/course-definition.yml |
| **Tokio mini-redis** | **Your skeleton.** Complete, readable, heavily-commented RESP client+server. Study its Framing and Shared-state chapters first — exactly the M1–M2 patterns. Extend it rather than start from zero. | https://github.com/tokio-rs/mini-redis |
| **RESP protocol spec (RESP2+RESP3)** | Official wire-protocol spec — keep open during M1, M2, M12. | https://redis.io/docs/latest/develop/reference/protocol-spec/ |
| **Redis command reference** | Per-command exact semantics + reply types — ground truth for every milestone's edge cases. | https://redis.io/docs/latest/commands/ |
| **redis-cli + redis-benchmark** | Not a tutorial — your test harness + motivation engine. `brew install redis`, then drive every milestone with the official tools. **Never write your own client.** | https://redis.io/docs/latest/operate/oss_and_stack/management/cli/ |
| **Build Your Own Redis w/ C/C++ (free book)** | Best-in-class for the low-level lessons your runtime hides: manual event loop, hashtable internals, AVL→sorted set, timers/TTL, thread pool. Use as a reference for "what is the async runtime doing under the hood," or a focused C/Zig side-quest on one structure (e.g. the M5 skiplist). | https://build-your-own.org/redis/ |
| **John Crickett — Coding Challenges: Redis** | Concise 7-step language-agnostic challenge ending on benchmarking vs. real Redis. A lightweight alternative spec confirming the same on-ramp. | https://codingchallenges.fyi/challenges/challenge-redis/ |
| **Redis persistence docs (RDB/AOF)** | Reference for M6–M7: fork+COW snapshots, AOF rewrite, fsync policies + exact data-loss windows, the hybrid. | https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/ |
| **Redis OBJECT ENCODING** | The encodings + exact promotion thresholds — essential for the M5/M12 fidelity work. | https://redis.io/docs/latest/commands/object-encoding/ |
| **build-your-own-x (master list)** | Aggregator for sibling deep-dives (event loop, KV store, database) when you want to go deeper on one subsystem. | https://github.com/codecrafters-io/build-your-own-x |
