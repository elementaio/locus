# Design note — the unified change-log primitive

> **Status: v1 (push changefeed) + v2 (offsets/retention/CDCREAD) MERGED/IMPLEMENTED.**
> - v1 (PR #12, merged): push changefeed `CDCSUBSCRIBE`/`CDCUNSUBSCRIBE` — snapshot + live deltas,
>   no gap/dup via single-threaded execution.
> - v2 (PR #13): every change carries a monotonic **offset**; opt-in retained ring (`LOCUS_CDC_MAXLEN`);
>   **`CDCREAD <offset>`** for reconnect catch-up; `snapshot-done` reports the high-water offset;
>   offset-out-of-range when a consumer falls behind.
> - v3 (PR #14): **consumer groups** (`CDCGROUP`/`CDCREADGROUP`/`CDCACK`/`CDCPENDING`) — the
>   load-balanced read mode, with a per-group cursor + pending list. Both read modes now exist.
> Next phase: **geo index → geo-region changefeed** (live geofencing) — the flagship; design-first.
> Original proposal + open questions retained below for the record.

## 1. Why this, why now

The command-coverage work made Locus a credible Redis-protocol KV store. It did **not** make it
*different*. The moat (your own thesis) is **reactive + real-time + geo on one ordered change-log
with offsets** — the thing Redis structurally can't do well, because its keyspace notifications are
lossy, after-the-fact, value-less Pub/Sub split across three subsystems.

The unlock is the architecture we already have: **every write funnels through one point** (`exec_one`
in the single hub thread) that holds the key, knows the event, and — crucially — runs serially. So a
change-log built there is **race-free and totally ordered by construction.** That is the property
Redis pays a fortune (and still fails) to approximate.

This primitive later carries the two flagship features: **live-query / changefeed** (#2) and
**geofencing on a region** (#3). Build the log right and those become layers, not rewrites.

## 2. Scope of the MVP (what I'd build first)

A **global, in-memory, offset-addressed log of keyspace mutations**, readable with one blocking
pull command. Opt-in (zero cost when off). Explicitly NOT the changefeed or consumer groups yet —
those are phases 2–3 once the log's shape is proven.

### Data model
```
ChangeRecord {
  offset: u64,        // monotonic, never reused, gap-free
  key:    Vec<u8>,
  event:  Write | Del | Expire,   // (Evict folds into Del)
  // value: Option<Vec<u8>>       // PHASE 2 — see Open question Q1
}
```
Held in a `VecDeque<ChangeRecord>` ring with a max length; a `next_offset: u64` counter on the hub.

### Where records are appended (reusing existing choke points)
- **Writes:** in `exec_one`, we already compute `write_modified` and iterate `write_keys`. For each
  *actually-modified* key, append a record (`Del` if the key is now absent, else `Write`). This means
  no-op writes don't pollute the log — same correctness we built for WATCH/AOF.
- **Expiry:** `dirty_expired_watchers` already drains the keys expiry removed → append `Expire`.
- **Eviction:** `evict_if_needed` already deletes keys → append `Del`.

So the log hooks the *same three places* we already maintain. No new bookkeeping scattered around.

### Command surface (MVP — Locus-native, clearly not Redis)
```
CDCREAD <offset> [COUNT n] [BLOCK ms] [PREFIX p]
```
- `<offset>`: `0` = from the oldest retained record; `$` = only changes after now; or a concrete
  offset to resume from.
- Returns an array of `[offset, key, event]` (value added in phase 2), in order.
- `BLOCK ms` parks the caller until new records arrive (reuses the blocked-reader machinery that
  already backs blocking `XREAD`).
- `PREFIX p`: server-side key-prefix filter (the only transform we allow — see non-goals).
- If `<offset>` is older than the oldest retained record → error `ERR offset out of range`
  (Kafka-style "you fell behind"), so consumers know to re-snapshot rather than silently miss data.

That single command gives **snapshot-then-tail with no gap/dup**: because the hub is single-threaded,
a client can read the current keyspace (e.g. `KEYS`/`SCAN`) and the current max offset atomically,
then `CDCREAD <that offset>` to get every subsequent change with zero gap. That property *is* the
changefeed foundation.

### Retention & enablement
- **Opt-in:** `LOCUS_CDC_MAXLEN=<n>` (records) enables it; unset/`0` = off, **zero overhead** on the
  write path for users who don't want it.
- Ring drops oldest when over `MAXLEN`. (`MAXAGE`/time-based retention = later, per DIFFERENTIATORS
  "adopt-later".)
- Durability (fsync the log) = later; MVP is RAM-only replayable tail.
- Counts toward `maxmemory` accounting (it's real memory).

## 3. Relationship to existing Pub/Sub and Streams

Keep both as-is for Redis compatibility. The change-log is the **new keystone**, not a replacement of
the wire-compatible commands. Over time the changefeed/geo features ride on the log; we do **not**
retrofit pub/sub or streams on top of it (that risk isn't worth it). The DIFFERENTIATORS "collapse
three subsystems into one" applies to *new* reactive features, not to rewriting shipped commands.

## 4. Phasing

1. **MVP (this note):** global log + `CDCREAD` (pull, blocking, prefix filter, offset-out-of-range).
   Opt-in. Records = key + event + offset.
2. **Values + push mode:** add the new value to records (Q1); add a broadcast push subscription
   (`CDC SUBSCRIBE`) for fire-hose consumers that don't want to manage offsets.
3. **Changefeed:** `CDCWATCH PREFIX p` → atomic snapshot of matching keys + coalesced live deltas.
   This is the "I'm switching from Redis" feature.
4. **Consumer groups:** load-balanced read mode (the second of the two read modes).
5. **Geo:** region filter instead of prefix → live geofencing (needs the geo index first).

## 5. Explicit non-goals (guarding the elegance, per DIFFERENTIATORS)

- **No server-side transforms** beyond prefix (later: region) filtering. No projections, no joins, no
  aggregation, no query language. Clients filter/shape. (That's the "database, not a
  data-structures-server" line.)
- **No triggers / run-my-code-on-write.** Arbitrary logic stalls the single thread (RedisGears was
  discontinued for this). React in the *client* via the changefeed.
- No per-key fan-out config, no transforms, no DLQ/retry policy in the log itself.

## 6. Open questions (need your call before I build)

- **Q1 — values in records?** MVP options: (a) **key + event only** (consumer re-fetches; simplest,
  cheapest, great for cache-invalidation) or (b) **include the new value** (true CDC, needed for
  changefeed-with-data and geofencing payloads, but bigger records + value-capture for every type).
  My lean: **start with (a)**, add value in phase 2 — but if your chat/Pulsar use needs the value in
  the event from day one, we do (b). Which fits your near-term consumers?
- **Q2 — command name?** `CDCREAD` vs. reusing the stream surface (`XREAD ... STREAMS __changelog__`).
  My lean: **dedicated `CDC*` commands** — cleaner semantics, and it signals "this is the
  differentiator," not a Redis command. OK?
- **Q3 — what counts as a logged event?** Just data mutations (my plan), or also TTL *set* (not only
  expiry firing)? My lean: data mutations + expiry/eviction deletions; `EXPIRE`-setting is itself a
  write so it's already covered.
- **Q4 — enablement default?** Opt-in via `LOCUS_CDC_MAXLEN` (my lean, zero default overhead) vs. on
  by default with a sane cap. I strongly prefer opt-in for v0.
- **Q5 — scope check:** is the **pull/`CDCREAD` MVP** the right first slice, or do you want me to go
  straight to the **changefeed (snapshot+deltas)** since that's the headline demo? The changefeed
  needs the log underneath either way, so MVP-first is lower-risk; but if you want the flashy demo
  sooner I can fold phase 3 into the first PR.

---

**Recommendation:** build the MVP exactly as in §2 (answers: Q1=a, Q2=CDC\*, Q3=mutations+expiry/evict,
Q4=opt-in, Q5=MVP-first), ~1 focused PR with unit + integration tests, then iterate to values →
changefeed. Tell me which of Q1–Q5 you'd change and I'll start.
