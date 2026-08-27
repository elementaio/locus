# Differentiators — What Locus Has That Redis Doesn't

The output of a 10-domain research sweep (query, reactive/CDC, durability/txn, memory/tiering, data
models, messaging, TTL, multi-tenancy, observability, scale-up) scored on **gap-realness × usefulness ×
elegance-fit**, with feature-creep traps deliberately separated out.

> **The one-line thesis:** Locus's spine is **reactive + real-time + geo**, and it's coherent (not three
> bolt-ons) because all three rest on **one keystone primitive: an ordered change-log with offsets.**
> Single-threaded + in-RAM aren't constraints here — they're the *source of the advantage*: every command
> funnels through one execution point holding old+new values, so the change-log is **race-free and ordered
> by construction**, which is exactly what Redis cannot do (its keyspace notifications are lossy,
> after-the-fact, value-less Pub/Sub split across three overlapping subsystems).

> **The discipline that keeps it elegant, repeated everywhere:** **ship the PRIMITIVE, refuse the POLICY.**

---

## ✅ BUILD — the differentiators (adopt-core)

| Capability | Real / Useful / Elegant | Why it wins | Keep-it-elegant guard |
|---|---|---|---|
| **1. Unified ordered change-log** (CDC + reliable fan-out + stream, *one* primitive) | 10 / 10 / 9 | The keystone. Six separate gaps (durable CDC, reliable fan-out, snapshot+tail, rich expiry events, delayed delivery, the stream type) collapse into ONE log. Race-free & offset-addressed *because* of single-threaded execution. Lets the user **delete Pulsar's hand-rolled replay and the chat engine's bespoke fan-out** — the store provides them. | Build ONE log, not three. Two read modes (broadcast / load-balanced group), monotonic offsets, RAM-ring replay (fsync = opt-in dial). Emit **raw** mutations + offsets; clients filter. No server-side transforms beyond prefix/region filter. |
| **2. Live-query / changefeed** (prefix or spatial region + scalar predicate → snapshot + coalesced deltas) | 9 / 9 / 8 | **The "I'm switching from Redis" moment.** Redis has no "subscribe to a result set, get pushed deltas" — people leave for RethinkDB/Convex/Firebase. Snapshot-then-tail is *correct* (no gap/dupe) because offsets exist. A changefeed over a **geo region = live geofencing** → the Tile38-beating feature. Maps 1:1 to Pulsar topics + chat channels. | Hard scope: prefix/region + **single scalar predicate** + snapshot + coalesce. REFUSE joins/aggregations/SQL-like predicate trees (that's Materialize/ReQL = a database). |
| **3. Geo-first spatial index** (R-tree / S2-cell) with combined attribute filters, sort-by-distance, keyset pagination | 9 / 10 / 9 | The product's reason to exist; the empty market intersection (elegant + in-memory + geo-FIRST + combined filters + spatially clustered). **Same ordered-index machinery as #4**, so geo and find-by-value are *one* primitive. | `nearby + filter + sort + cursor` as a primitive, not a "find restaurants" feature. Combined filter = AND of equality/range only, never a query planner. Index by cell ID from day one (per CLUSTER.md). |
| **4. Secondary-index primitive** (query-by-field-value, auto-maintained, keyset pagination) | 9 / 8 / 8 | Consolidates three query gaps. The killer argument: hand-rolled Redis indexes **drift on crash** (no rollback, GC-your-own-dangling-pointers); single-threaded co-locates write+index into **one atomic step — no drift by construction.** | Equality + range + simple AND + cursor. REFUSE full-text/stemming/fuzzy/scoring/aggregation/query-language (that's the Redis Query Engine). |
| **5. Mergeable probabilistic sketches** (Count-Min, Top-K, t-digest, Bloom) | 7 / 8 / 10 | **Best elegance-to-value ratio in the whole set** — HyperLogLog's blessed siblings. Deliver Pulsar's needs natively: Count-Min/Top-K = "trending now", t-digest = live P99, Bloom = "seen this id" dedup. **Subsume most of a time-series type.** Mergeability = the right primitive for the eventual spatial cluster. | Ship a-la-carte, one at a time (lead: Count-Min + Top-K + t-digest). Tiny command set each. Pointed at the command stream → free scan-free hot-key detection. |
| **6. Conditional / CAS write verbs** (CAS, SET IFEQ, capped-INCR, conditional-EXPIRE) | 8 / 8 / 9 | **Near-free** under single-threaded execution, and it removes the #1 reason people drop into Lua ("write B only if A still equals X"). Powers chat's persist-before-ack, dedup, per-device cursor CAS. Lets transactions stay **thin** (kills most of WATCH/MVCC's reason to exist). | ONLY equality/inequality/exists CAS + capped-INCR + conditional-EXPIRE. REFUSE a general ConditionExpression grammar (DynamoDB's is huge → slippery slope to a query engine). |

---

## 🕐 ADOPT-LATER — right idea, wrong time (sequencing, not creep)

Design these in **when their substrate milestone lands**, not bolted on early:

| Capability | Why later |
|---|---|
| **Unified per-element TTL** across all collections (one mechanism, beats Redis's hash-only 7.4 version) | Needs core data types (M4/M5) solid + a per-collection TTL-indexed structure + element-walking active-expiry. |
| **Per-command durability flag** (`fsync THIS write before ack`, group-commit) | Principle #5 to its logical limit — but needs the AOF/WAL milestone (M7) + group-commit so one thread isn't throttled to disk IOPS. |
| **Time-based log retention** (`MAXAGE` / "keep last 24h", vs count-based MAXLEN) | Obvious cheap win — but only once the change-log (#1) exists. Ship as a property of the log. |
| **Native delayed/scheduled delivery** (`deliver-at` scalar on a log entry) | Internalizes the sorted-set-poller pattern. Rides on the log. Guard: a single deliver-at scalar — NO cron/recurrence/calendars. |
| **Rich expiry events** (emit the dead value into the change-log; kills the shadow-key hack) | A property of #1, not standalone. Frame as "reliable event with value," not hard-real-time deletion. |

---

## ⛔ SKIP — feature-creep traps (deliberate non-goals)

Each is tempting and each **betrays a named principle.** Skipping them *is* the product.

| Trap | Why tempting | Why skip |
|---|---|---|
| **Server-side triggers / reactive hooks / IVM** | Pulsar/chat are reactive; "run my logic on write" sounds perfect | Arbitrary logic stalls the single thread (betrays #2); IVM = "a database, not a data-structures server" (#1). RedisGears tried it, **discontinued**. Substitute: the changefeed (#2) → react in the *client* (what Pulsar/chat already do). |
| **SSD / flash tiering** (transparent disk for data > RAM) | Most economically painful Redis gap (RAM ~50× SSD/GB) | Principle #3 + the named "transparent disk tier" anti-pattern. Reintroduces cache misses & tail-latency unpredictability — a half-database you can't reason about. Multi-month subsystem. If ever: opt-in module behind a flag. |
| **Multi-core / thread-per-core keyspace** | Single-core ~400–650K ops/sec ceiling is real; Dragonfly's headline RPS | Doubles core complexity (per-thread loops + deadlock-free VLL multi-key coordinator; naive sharding silently breaks MSET/MULTI atomicity — betrays #2 *worse*). antirez: bottleneck is memory/network, not CPU. For scale, prefer **spatial sharding** (additive layer). |
| **Full query engine / full-text** (inverted index, stemming, scoring, aggregation, DSL) | After a lightweight index, "just add full-text" feels natural | The heavy subsystem + query *language* is the "database, not data-structures-server" line (#1); a planner betrays #4. Stop at equality+range+AND+cursor. Full-text = Elasticsearch's job (Pulsar already earmarks pgvector/Qdrant). |
| **Vector / ANN type** (HNSW + quantization) | GenAI/RAG wave; Pulsar v2 wants semantic search | Research-grade engineering, hard to keep small, and **forces multi-threading** (Redis broke single-threaded for VSIM — the exact #2 betrayal). Someone else's job (Qdrant/Milvus/pgvector). |
| **Full job-queue** (DLQ + retries/backoff + cron + priority + rate-limit) | Redis ships no queue, so Sidekiq/BullMQ/Celery exist | Retry/backoff/cron/priority-fairness are **application policy**, not primitives (#1). Ship the orthogonal primitives (log + deliver-at + delivery-count + CAS); let userland compose BullMQ. (Kafka/SQS deliberately omit priority.) |
| **Active-active multi-master replication** | Globally-distributed products want local-latency writes everywhere | Multi-master + conflict-resolution + tombstone-GC = the distributed complexity #8 defers; multi-quarter burden on a single-node-first store. Keep the **CRDT types** (experiment, single-node, merge-ready); drop the replication subsystem. |
| **Embedded HTTP `/metrics` web stack** | Every k8s deploy bolts on redis_exporter | Embedding an HTTP framework pulls deps + config surface (betrays #4/#7). Elegant equivalent: emit metrics as **structured RESP3 from a `STATS` command** (zero deps, telnet-inspectable); thin external shim does HTTP. |

---

## 🧪 EXPERIMENT — build only if a concrete need appears

CRDT data *types* (PN-counter/OR-Set/LWW-register, single-node, merge-ready) · per-prefix/namespace
accounting (O(1) cost-by-tenant) · always-on hot-key sketch · transparent value compression (LZ4/Zstd) ·
size/cost-aware eviction · `STATS` RESP3 metrics · pattern-bucketed slow-op profiler · minimal
time-series (likely *subsumed by sketches*) · sliding TTL · per-namespace memory quotas.

---

## How it folds into the roadmap

The change-log reframes three existing milestones into **one unifying primitive**:

- **M8 (Pub/Sub) + M11 (Streams) + keyspace-notifications → ONE ordered change-log** with two read modes.
  Don't build three overlapping subsystems the way Redis did (a genuine wart).
- **Changefeed + geo index** = the two flagship differentiators layered on top.
- **CAS verbs ship early** (M2-ish) — cheap, and they unblock the chat engine's persist-before-ack.
- **Sketches** ship a-la-carte alongside the core types.
- **Per-element TTL / per-command durability / log retention** = adopt-later, designed in when M5/M7/the
  change-log land.

**Net:** geo-first, reactive, real-time, with mergeable analytics — the combination that makes someone
say *"I'm switching from Redis to Locus"* without Locus ever becoming a database.
