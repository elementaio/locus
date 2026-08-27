# Improvement Plan — Hardening v0.1 → the geo-first reactive vision

> A living, checklist-driven plan for continuing to improve the repo. Born from a full code
> audit + assessment on **2026-06-16** (at commit `9327026`, v0.1.0). Update the checkboxes as
> work lands. Pairs with [ROADMAP.md](ROADMAP.md) (the M0–M12 build) and
> [DIFFERENTIATORS.md](DIFFERENTIATORS.md) (where it heads).

**Guiding principle (from DIFFERENTIATORS):** *ship the primitive, refuse the policy.* And the
sequencing truth: **earn the geo/reactive vision by hardening the single node first.** A geo-first
store that can OOM-kill itself or has untested transactions is a hard sell.

---

## ✅ v0.2.0 SHIPPED & DISTRIBUTED — PRs #23–#26 at `8a1cf5f` (2026-06-16)

The differentiator arc is documented and the project is released on all three channels.

| PR | What |
|---|---|
| #23 | docs full pass — new guides CHANGEFEED/GEO/SKETCHES; refreshed README/ARCHITECTURE/ROADMAP |
| #24 | release plumbing — Cargo `0.2.0`, CHANGELOG `[0.2.0]`, Dockerfile, `.github/workflows/release.yml`, **`LOCUS_BIND`** (Docker binds 0.0.0.0) |
| #25 | crate renamed to **`locusdb`** + `[[bin]] name="locus"` (name "locus" was taken on crates.io) |
| #26 | **CLIENTS.md** — drive Locus from any Redis client + custom-verb snippets (Node/Python) |

**Released v0.2.0 — all live & verified:**
- **GitHub Release** `v0.2.0` + 4 prebuilt static binaries (linux x86_64/aarch64-musl, macOS x86_64/arm64) with `.sha256`.
- **Docker** `ghcr.io/elementaio/locus:{0.2.0,0.2,latest}` — **public**, anonymous pull verified.
- **crates.io** `locusdb v0.2.0` — `cargo install locusdb` → `locus` binary, verified.

Baseline on `main`: **47 unit + 27 integration**, clippy `-D warnings` + fmt clean. (crates.io token used
for the publish was revoked after.)

### Remaining / next plan (priority order)
1. **Geo phase 3** — real **S2-cell / R-tree** spatial index (sub-linear `GEOSEARCH`) + **combined
   attribute filters** (`nearby AND status=…`) + keyset pagination. The flagship; its own session.
2. **Spatial clustering** — horizontal sharding that preserves locality. The Tile38-beating lane
   (see [CLUSTER.md](CLUSTER.md)).
3. **Reactive client wrapper** — thin TypeScript/npm lib over the changefeed/geofence *push* API
   (`feed.on('change', …)` / `locus.geofence(…)`), wrapping ioredis. Python helper if demanded.
   (Standard clients already work via raw commands — this is DX sugar.)
4. **Adopt-later primitives** — per-element TTL, per-command durability, time-based changefeed retention.
5. **Deferred core hardening** — PSYNC partial resync / backlog / `WAIT` / failover; skiplist for O(log n)
   zset ops; **AUTH/ACL/TLS**; multi-DB; full RESP3 typing; thread-per-core. (Needed before "production".)
6. **Release tooling** — bump release actions when upstream ships Node-24 builds (current warnings are
   cosmetic); optional Docker Hub mirror. Next version: bump Cargo + CHANGELOG, tag `vX.Y.Z`, the
   workflow does the rest (needs a fresh crates.io token via `cargo login`).

---

## ✅ DIFFERENTIATOR ARC COMPLETE — PRs #1–#16 at `bafee5f`

After the hardening + command coverage (PRs #1–#10), the reactive + geo differentiators landed:
- **#11** replica handshake timeout + `SELECT` (single DB)
- **#12** changefeed push (`CDCSUBSCRIBE`/`CDCUNSUBSCRIBE`) — snapshot + live, no gap/dup
- **#13** changefeed offsets + retained ring + `CDCREAD` catch-up
- **#14** changefeed consumer groups (`CDCGROUP`/`CDCREADGROUP`/`CDCACK`/`CDCPENDING`) — load-balanced read mode
- **#15** geo phase 1 — `Value::Geo`, geo-key index, `GEOSET`/`GEOPOS`/`GEODIST`/`GEOSEARCH`, RDB/AOF
- **#16** geo phase 2 — **live geofencing** `CDCSUBSCRIBE REGION …` (the flagship; geo + changefeed converged)

**The DIFFERENTIATORS thesis is now real on a single node:** reactive ordered change-log (two read
modes) + geo-first index + live geofencing + **CAS verbs (#17)**. Baseline on `main`: **37 unit +
22 integration**, clippy `-D warnings` + fmt clean.

- **#17** CAS family — `CAS`/`CADEL`/`SETMAX`/`INCRCAP` (chat-engine cursor/dedup/quota primitives).
- **#18** sketches: **Bloom filter** (`BFADD`/`BFEXISTS`, `src/sketch.rs`) — first of the a-la-carte family.
- **#19** sketches: **Count-Min** (`CMSINCRBY`/`CMSQUERY`) — frequency / "trending now".
- **#20** sketches: **Top-K** (`TOPKRESERVE`/`TOPKADD`/`TOPKLIST`/`TOPKCOUNT`) — heavy hitters.
- **#21** sketches: **t-digest** (`TDADD`/`TDQUANTILE`) — live percentiles. Sketch family complete.
- **#22** **secondary index** (`IDXCREATE`/`IDXGET`/`IDXRANGE`) — query-by-field (#4). **ALL 6
  BUILD-core differentiators now done.**

Remaining: geo phase 3 (real S2/R-tree index + combined attribute filters + keyset pagination) →
spatial-clustering arc; adopt-later items (per-element TTL, per-command durability, log retention by age).
(real S2/R-tree index + combined attribute filters + keyset pagination) → spatial-clustering arc;
adopt-later (per-element TTL, per-command durability, time-based log retention).

---

## ✅ Merged to `main` (2026-06-16) — PRs #1–#8 at `d67b1a2`

All merged (linear fast-forward; branches deleted). Remote `elementaio/locus`.

| PR | What |
|---|---|
| #1 | WATCH-on-expiry, EXECABORT, no-op-WATCH + integration harness |
| #2 | `maxmemory` cap + eviction + OOM + `INFO # Memory` |
| #3 | single command table; kills the AOF write-list footgun |
| #4 | strings: MGET/MSET/MSETNX/SETNX/SETEX/PSETEX/GETSET/GETRANGE/SETRANGE/INCRBYFLOAT |
| #5 | keyspace: KEYS/DBSIZE/RENAME/RENAMENX/TOUCH/UNLINK/FLUSHDB/FLUSHALL |
| #6 | lists: LINSERT/LREM/LTRIM/LPOS/RPOPLPUSH/LMOVE |
| #7 | sets: SMOVE/SINTERSTORE/SUNIONSTORE/SDIFFSTORE/SINTERCARD |
| #8 | zsets: ZREMRANGEBYRANK/ZREMRANGEBYSCORE/ZUNIONSTORE/ZINTERSTORE |

Baseline on `main`: **32 unit + 13 integration**, clippy `-D warnings` + fmt clean.

**Next candidates:** a small zero-deps PRNG → `SRANDMEMBER`/`RANDOMKEY`/true-random `SPOP`; zset
**lex** commands (`ZRANGEBYLEX`/`ZLEXCOUNT`/`ZREMRANGEBYLEX`/`ZRANGESTORE`); **bitmaps**
(`SETBIT`/`GETBIT`/`BITCOUNT`/…); hashes (`HSTRLEN`/`HINCRBYFLOAT`/`HRANDFIELD`); or the remaining
Tier-0 follow-ups (true LRU/LFU eviction, replica handshake read-timeout, `EXPIRE … NX|XX|GT|LT`).

---

## ✅ Done (committed `9327026`)

- [x] **TTL integer overflow** — `EXPIRE`/`PEXPIRE`/`EXPIREAT`/`PEXPIREAT` and
      `SET … EX/PX/EXAT/PXAT` now use checked arithmetic. Huge TTLs error cleanly instead of
      panicking (debug) / wrapping to a past deadline and silently deleting the key (release).
- [x] **`ZADD GT`/`LT`** gate score updates + INCR correctly (were silent no-ops); reject
      incompatible `GT`+`LT` and `NX`+`GT`/`LT`.
- [x] **`RESET`** handler (abort MULTI, UNWATCH, exit subscribe mode, drop to RESP2) — was
      advertised in allow-lists/error text but unhandled.
- [x] **RESP parser hardening** — cap eager pre-alloc for large `*N` headers; bound un-terminated
      inline requests at 64 KiB (was per-connection unbounded memory → DoS).
- [x] **Replica** re-serves blocked `XREAD` readers after a full-sync `ReplaceDb`.
- [x] Tests for all of the above; docs + CHANGELOG updated; clippy/fmt clean.

---

## 🔴 Tier 0 — correctness & safety (do first)

### Open bugs (from the audit; not yet touched)

Bundle as a **"transaction correctness" PR** — ✅ DONE (branch `fix/transaction-correctness`):
- [x] **`WATCH` ignores key expiry (HIGH).** Fixed: `Db` records expired keys (`take_expired`); the
      hub drains them in `exec_one`, the active-reaper tick, and after XREAD to dirty WATCHers.
- [x] **`MULTI` lacks queue-time validation / no `EXECABORT` (HIGH/MED).** Fixed: `commands::min_arity`
      table + `TxState.aborted`; unknown/under-arity commands abort the tx → `EXEC` returns EXECABORT.
- [x] **Spurious `WATCH` aborts on no-op writes (MED).** Fixed: `write_modified(cmd, reply)` gates
      WATCH-dirty + AOF + replication; no-op replies (`:0` / nil for the relevant commands) are
      treated as no change. Conservative (defaults to "modified") so a real write is never dropped.
- [x] Integration harness (`tests/integration.rs`) verifies all of the above end-to-end over TCP.

Bundle as a **"replication/AOF durability" PR**:
- [ ] **AOF `SET`-with-past-TTL replay divergence (MED).** `aof::entries_for("SET")` re-reads DB
      state; a mid-log passive expiry can drop the key, so replay recreates it *without* expiry.
      Capture value + deadline atomically before any expiry-triggering access, or log a `DEL` when the
      resulting deadline is already past.
- [ ] **Replica re-derivation / double-logging (MED).** On a replica, master commands run through
      `exec_one` which re-runs `entries_for` (re-appending TTLs the master already sent). When
      `id == MASTER_ID`, log the master's tokens verbatim (already deterministic).
- [x] **Replica handshake has no read timeout (LOW/MED).** ✅ DONE (PR #11) — 5s read timeout set
      before the handshake; a silent master no longer hangs `replica_sync`.
- [ ] **`INFO master_link_status` hard-coded `up` (LOW).** Reflect real link state.

### New safety mechanisms

- [x] **`maxmemory` + eviction (the #1 operational risk).** ✅ DONE (branch `feat/maxmemory-eviction`,
      PR #2). `LOCUS_MAXMEMORY` (kb/mb/gb); approximate per-key accounting in `Db` (`mem_used`,
      `resync_size`, removal hooks); hub `evict_if_needed` evicts arbitrary keys (propagated as DEL +
      dirties WATCHers) and rejects writes with OOM if the cap can't be met; `INFO` `# Memory` section.
      *Follow-ups:* true LRU/LFU/sampled eviction (currently arbitrary HashMap order); `maxmemory-policy`
      config; byte-exact accounting.
- [x] **Kill the AOF allowlist footgun.** ✅ DONE (branch `chore/command-table`, PR #3). Single
      `commands::command_meta` table now drives existence + min-arity + write/read; `aof::is_write`
      delegates to it. Regression-lock test pins the write set. *Note:* still two touch-points per new
      command (dispatch arm + table entry), but the second list is gone and a missing entry is now
      caught by MULTI queue-time validation.

---

## 🟡 Tier 1 — credibility (cheap, high signal)

- [x] **Integration test harness.** ✅ DONE (PR #1) — `tests/integration.rs` spawns the server and
      drives it over TCP (pipelining, MULTI/EXEC, EXECABORT, WATCH, pub/sub, blocking XREAD,
      replication). Extended in later PRs (maxmemory bounds).
- [ ] **Fuzz `parse_command`** (`cargo-fuzz` or a property test): never panic, always make progress
      on adversarial bytes (huge counts, negative lengths, embedded NULs, interleaved inline/multibulk).
- [ ] **Trivial-but-missing commands** (great contributor on-ramps; each = match arm + fn + test):
      - [x] strings: `MSET`/`MGET`/`MSETNX`/`SETNX`/`SETEX`/`PSETEX`/`GETSET`/`INCRBYFLOAT`/`GETRANGE`/`SETRANGE` ✅ (PR #4)
      - [x] keyspace: `KEYS`/`DBSIZE`/`RENAME`/`RENAMENX`/`FLUSHDB`/`FLUSHALL`/`TOUCH`/`UNLINK` ✅ (PR #5; `COPY` deferred — needs `Value: Clone`)
      - [x] lists: `LINSERT`/`LREM`/`LTRIM`/`LPOS`/`RPOPLPUSH`/`LMOVE` ✅ (PR #6)
      - [x] sets: `SMOVE`/`SINTERSTORE`/`SUNIONSTORE`/`SDIFFSTORE`/`SINTERCARD` ✅ (PR #7; `SRANDMEMBER` deferred — needs PRNG)
      - [~] zsets: ✅ `ZREMRANGEBYRANK`/`ZREMRANGEBYSCORE`/`ZUNIONSTORE`/`ZINTERSTORE` (PR #8); still TODO: `ZRANGEBYLEX`/`ZLEXCOUNT`/`ZREMRANGEBYLEX`/`ZRANGESTORE`/`ZUNION`/`ZINTER`/`ZDIFF` (lex needs `[`/`(`/`+`/`-` bound parsing) **← NEXT (lex)**
      - [x] bitmaps: `SETBIT`/`GETBIT`/`BITCOUNT`/`BITPOS`/`BITOP` ✅ (PR #9)
      - [ ] hashes: `HSTRLEN`/`HRANDFIELD`/`HINCRBYFLOAT` **← NEXT**
      - [x] random: `SRANDMEMBER`/`RANDOMKEY` + true-random `SPOP` ✅ (PR #10; zero-deps xorshift PRNG)
- [ ] **Flesh out `INFO`** (memory / stats / keyspace / clients / persistence sections) — unlocks
      `redis_exporter` and real monitoring.
- [ ] **`EXPIRE … NX|XX|GT|LT`** flags (Redis 7+) — small compat win.

---

## 🟢 Tier 2 — depth

- [ ] **Skiplist for sorted sets** (M5 in ROADMAP) — O(log n) rank/range instead of sort-on-demand
      O(n log n). Also the substrate for geo.
- [ ] **`SELECT` / multiple logical DBs** — architectural: `Hub` owns a single `Db`; needs
      `Vec<Db>` + per-client DB index threaded through `execute()` and `ReplaceDb`.
- [ ] **PSYNC partial resync + replication backlog + offsets + `WAIT`** — today every reconnect
      re-ships the whole dataset (offset hard-coded `0`); no ack tracking.
- [ ] **Decouple hub liveness from the 100 ms recv tick** — active expiry / AOF fsync / BLOCK
      timeouts currently only fire on `recv_timeout` and can be starved under sustained load.
- [ ] **`CONFIG GET/SET`** real implementation + optional config file (currently env-only, stub CONFIG).
- [ ] **`CLIENT` (ID/LIST/KILL/SETNAME)** — hub has the client registry; needs per-client metadata.
- [ ] **`benches/`** — M12 claims benchmarking but there's no regression guard.

**Explicit non-goals (per DIFFERENTIATORS "SKIP"):** scripting/`EVAL` (breaks zero-deps), SSD
tiering, multi-core keyspace, full query engine/full-text, vector/ANN, full job-queue, active-active
replication, embedded HTTP `/metrics`. Skipping these *is* the product.

---

## 🧭 Then: the vision layer (only after Tier 0–1)

From [DIFFERENTIATORS.md](DIFFERENTIATORS.md) — build in this order, each on the prior substrate:

1. **Unified ordered change-log** (CDC + reliable fan-out + stream = *one* primitive, two read
   modes). Collapses pub/sub + streams + keyspace-notifications instead of rebuilding Redis's three
   overlapping subsystems. Race-free & offset-addressed *because* of single-threaded execution.
2. **CAS write verbs** (CAS / SET IFEQ / capped-INCR / conditional-EXPIRE) — near-free, unblocks the
   chat engine's persist-before-ack. Can land early.
3. **Mergeable sketches** (Count-Min, Top-K, t-digest, Bloom) — a-la-carte; best value/elegance.
4. **Secondary-index primitive** (query-by-field, auto-maintained, keyset pagination).
5. **Geo-first spatial index** (S2 cell / R-tree) + combined attribute filters + sort-by-distance.
   The flagship. Same ordered-index machinery as #4.
6. **Live-query / changefeed** (prefix or geo region + scalar predicate → snapshot + coalesced
   deltas) — the "I'm switching from Redis" moment; geofencing on live streams.

**Competitive wedge (LANDSCAPE):** Tile38 is the closest competitor; it does *not* cluster.
The empty intersection — *in-memory · Redis-simple · geo-first · combined-filters · **spatially
clustered*** — is Locus's lane. Beat Tile38 specifically on **spatial clustering** (your BEAM
clustering experience is the head start). See [CLUSTER.md](CLUSTER.md).

---

## 🎯 Perfect projects to use it in

- **The BEAM chat engine** (see [INTEGRATION.md](INTEGRATION.md)) — the killer fit. Storage ports map
  ~1:1 to Locus primitives; RESP means the planned Redix adapters point at it with a config line.
  Locus = fast *state*, not the live-delivery path.
- **A Pulsar-like firehose / event system** — change-log backbone + sketches (trending, live p99, dedup).
- **Location-aware features** — "people nearby", live location share, **geofencing** (once geo lands).
- **Live dashboards / collaborative apps** — once the changefeed exists.
- **Today (v0.1):** local dev / CI Redis substitute for the supported subset; internal services on a
  trusted network; teaching / portfolio artifact. **Not yet:** multi-tenant, internet-exposed, or
  loss-intolerant data (no AUTH/TLS, no maxmemory, open tx edge cases). Keep saying this in the README.

---

## Suggested next move

Start with the **transaction-correctness PR** (the three WATCH/EXEC bugs) *or* **`maxmemory` +
eviction** — both are Tier 0. The integration test harness (Tier 1) should land alongside whichever
comes first, so the concurrency-sensitive fixes are actually verified end-to-end.
