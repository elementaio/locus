# Locus → Production: The Plan

> **Goal:** take Locus from a strong pre-1.0 single-node datastore to a production-ready,
> highly-available, optionally-clustered system **without betraying its identity** (zero
> third-party crates, std-only, single static binary, single-threaded atomic hub).
>
> **Status:** living plan. Born from a full multi-agent code audit on **2026-06-19** (at
> `8a1cf5f`, v0.2.0). Pairs with [IMPROVEMENTS.md](IMPROVEMENTS.md) (the author's own gap
> tracker — this plan supersedes and sequences it), [ROADMAP.md](ROADMAP.md),
> [DIFFERENTIATORS.md](DIFFERENTIATORS.md), and [CLUSTER.md](CLUSTER.md).
>
> **Provenance:** 10 domain specs were designed against the live code. Six (auth, tls,
> cluster, observability, driver-compat, QA) were authored by parallel architect agents; four
> (durability, replication, HA, networking/DoS) were authored from the prior deep audit after a
> transient API rate-limit interrupted those agents. Work-item IDs, file:line anchors, effort,
> and exit gates are all concrete and ready to implement.

---

## 0. The shape of the journey (TL;DR)

Seven milestones. **Harden the single node first**, then make it *deep and fast*, and **do
clustering last** — you cannot cluster, fail over, or trust a node you haven't first made safe,
durable, observable, and capable on its own.

> **STATUS — 2026-06-27:** **P0–P5 are DONE and on `main`.** P0–P4 = security, durability,
> driver-compat + observability, replication-v2 (partial-resync + `WAIT`), HA (sentinel failover +
> quorum + inter-sentinel agreement — trusted-network only, **not** partition-safe; see the P4
> correction in §3), TLS (sidecar + optional `tls` feature). P5 = geohash spatial
> index, `GEOSEARCH … WHERE` filters, ordered-index sorted sets, CRC16 routing seam, RESP3 push. The
> default build is still 100% zero-dependency. **PERF-1 / REPL-6 / MULTIDB were dismissed** (see §3 P5).
> **Only P6 (spatial clustering) remains — the flagship, done last.**

| # | Milestone | What it unlocks | Exit gate (one-liner) | Status |
|---|-----------|-----------------|------------------------|--------|
| **P0** | **Stop the bleeding** | Safe on a *trusted network* | A stranger can't read or wipe it; survives SIGTERM; no slowloris | ✅ **done** |
| **P1** | **Trust the data** | Durability you can rely on | No acked-write loss beyond the fsync window; differentiator state survives restart; crash tests green | ✅ **done** |
| **P2** | **Look like real Redis** | Drop-in for off-the-shelf drivers + observability | `redis_exporter` works; SCAN/COMMAND/CONFIG/RESP3 real; no driver fallbacks | ✅ **done** |
| **P3** | **Replicate correctly** | Resumable, non-diverging replication | partial resync on reconnect; `WAIT`; no expiry divergence | ✅ **done** |
| **P4** | **Survive node failure** | Automatic failover + TLS | sentinel failover (quorum + inter-sentinel agreement; trusted-network, **not** partition-safe — see the P4 correction); native `--features tls` + sidecar | ◑ **mostly** |
| **P5** | **Depth & scale-up** | Deep geo + sharper single node | sub-linear GEOSEARCH + `WHERE` filters; ordered-index zsets; CRC16 routing seam; RESP3 push | ✅ **done** |
| **P6** | **Scale out** | Spatial-locality clustering (the flagship) | Bounded scatter-gather GEOSEARCH across shards; static-cell cluster solid | ⬜ **next (last)** |

**Honest total:** the single-node-hardened + HA + TLS base (**P0–P4**) is **done**. **P5** (geo
depth, skiplist, thread-per-core, replication tail, polish) is the next ~**2–4 months** and is all
std-only + independent of distribution. **P6** clustering is the multi-month flagship arc, done
**last** so it lands on a node that's already deep, fast, durable, and HA.

---

## 1. Strategic framing — the one decision that shapes everything

**Do not try to "become production-ready Redis."** That race is lost against Redis/Valkey/
DragonflyDB. "Great" for Locus is being **the best at the lane nobody else owns: a reactive,
geo-first datastore** (ordered changefeed + live geofencing + sketches + CAS + secondary index),
hardened enough to *trust* on a single node, with HA and clustering done pragmatically and
spatially.

Every item in this plan is judged by one test: *does it make the differentiated product
trustworthy, or is it parity-chasing?* Parity work (AUTH, SCAN, INFO, durability) is included
**only because production demands it** — not to chase Redis feature-for-feature.

**This thesis must be visible the instant someone lands on the repo — today it isn't.** The README
leads with the *implementation* — *"an in-memory datastore that speaks the Redis protocol — from
scratch in Rust, with zero dependencies"* — which reads like a Redis clone / learning project and
buries the reason-to-exist below the fold. **DOC-1:** rewrite the README (and the crates.io/GHCR
descriptions) to lead with **why Locus**:
- A headline that names the differentiated value (reactive **changefeed** + **live geofencing** +
  **mergeable sketches** + **CAS** + drift-free **secondary index**) and the insight that enables it:
  the single-threaded hub sees every mutation's ordered before/after at one point, so it can offer a
  reliable ordered change-log and live queries that a multi-threaded Redis can't cleanly.
- A short **"why not just Redis?" / when-to-use** section, and an honest "not yet" (pre-1.0) line.
- **"Zero-dependency, single static binary, from-scratch Rust" demoted to a supporting bullet** — it's
  the *how* and a genuine asset (supply chain, reproducibility), but not the pitch.
- A 30-second demo: *drive it with your Redis client, then `CDCSUBSCRIBE` / geofence over the changefeed.*
- Draft headline to iterate on: *"Locus — a Redis-compatible datastore with a reactive, geo-first core:
  an ordered changefeed, live geofencing, and mergeable sketches that vanilla Redis can't cleanly
  offer, in one zero-dependency binary."*

DOC-1 is **S effort, high signal, do-anytime** — it doesn't depend on any code milestone and is the
cheapest possible boost to how the project is perceived.

**Definition of "production-ready" / "done":**
- **1.0 (single-node):** safe to expose on a trusted network behind TLS, loses no acknowledged
  data beyond a documented fsync window, is observable via standard tooling, and runs
  off-the-shelf Redis drivers without fallbacks.
- **The vision (clustered):** the same, plus survives node failure and scales horizontally by
  *space*, with an explicit, tested consistency contract.

---

## 2. Identity & zero-dependency rulings (the constraints contract)

Production pressure collides with the zero-dep / std-only / single-binary identity at a handful
of points. These are the **binding rulings** for the whole plan. The principle, from
DIFFERENTIATORS: *ship the primitive, refuse the policy — and use an external shim before a
linked dependency.*

| Collision point | Ruling | Mechanism |
|---|---|---|
| **TLS** (std has no crypto — hand-rolling is a non-starter and would be *insecure*) | Tiered: sidecar/proxy is the **default, recommended** path; native TLS is **opt-in** | `(a)` documented stunnel/spiped/ghostunnel termination (zero code); `(b)` optional `tls` cargo feature pulling `rustls` only when enabled. Default `cargo install` / Docker stay literally zero-dep. |
| **Graceful shutdown** (std has no signal API) | Allowed — it's **FFI to the platform libc**, not a crate | `extern "C" { fn sigaction(...) }` + self-pipe. Adds **zero crates.io deps**. Also ship a `SHUTDOWN` command. State this in the README's identity claim. |
| **Password hashing** | Vendor it | ~80 LoC std-only SHA-256 in `src/acl.rs` with a known-answer test. No `sha2` crate; never the xorshift PRNG. |
| **Metrics export** | Uphold the non-goal | No embedded HTTP `/metrics`. Make `INFO` complete so `redis_exporter` works; optionally a `STATS` RESP3 command later. |
| **HA consensus / failover** | **Defer embedded Raft.** Ship hooks + Sentinel-lite | The **one real identity-pressure point.** In-binary static-config + gossip-lite + timeout failover for small clusters; external orchestration (k8s operator) for the rest; an optional control-plane sidecar is a *separate process, never a linked crate*. Document the data-loss window honestly. |
| **Cluster control plane** | In-binary first; sidecar optional | CRC16 / rendezvous-hash / cell math / HLC are each a few hundred lines of std-only Rust. The BEAM sidecar from CLUSTER.md is an *optional deployment topology*, not a dependency. |
| **Dev/CI tooling** (cargo-fuzz, sanitizers, criterion) | Allowed, isolated | Dev-only, in an excluded workspace or as toolchain features. A `--locked --offline` build check + SBOM **enforces** that the *shipped* artifact stays zero-dep. |

**Non-goals upheld** (per DIFFERENTIATORS "SKIP"): scripting/`EVAL`, embedded HTTP `/metrics`,
active-active replication, multi-core keyspace, SSD tiering, full query/full-text, vector/ANN.
**Revisit-for-production (small):** `SELECT`/multi-DB (optional), `COPY` (deferred, needs
`Value: Clone`), skiplist for zsets (perf, not correctness).

---

## 3. The milestones

Each milestone is a coherent, shippable capability with an **objective exit gate**. Work items
are pulled from the domain specs in [§4](#4-domain-specs-the-detail). Effort: **S** <1d · **M**
1–3d · **L** ~1.5wk · **XL** >1.5wk.

### P0 — Stop the bleeding *(safe on a trusted network)*
**Goal:** Locus can no longer be read or wiped by anyone who reaches the port; it survives a
container stop; it can't be trivially DoS'd. This is the highest value-per-effort phase.

| Item | E | What |
|---|---|---|
| AUTH-1 | M | `requirepass` + per-conn auth state + `AUTH` + NOAUTH gate |
| AUTH-2 | S | `HELLO 3 AUTH …` clause (drivers send it on connect) |
| AUTH-3 | M | Command-class taxonomy in `command_meta`; gate admin/dangerous verbs |
| AUTH-5 | M | **Protected mode** — closes the Docker `0.0.0.0`-with-no-password hole |
| AUTH-6 | M | Authenticate the replication link (stop the unauth `PSYNC` dataset siphon) |
| NET-1 | S | Per-connection read/idle timeout + `TCP_NODELAY` (kills slowloris) |
| NET-2 | M | Max-connection cap + `rejected_connections` (kills thread/mem exhaustion) |
| NET-7 | S | Safe-by-default bind story + Docker note (with AUTH-5) |
| OBS-9 | M | Structured leveled logging replacing ad-hoc `println!`/`eprintln!` |
| OBS-10 | L | **Graceful SIGTERM/SIGINT** → drain → fsync → final save → exit 0 |

**Exit gate:** a server bound to `0.0.0.0` with no password denies remote commands; with
`requirepass` set, every data command and `PSYNC` requires AUTH; idle/slow sockets and
connection floods are bounded; `kill -TERM` loses no data beyond the fsync window. *Est: 2–4 wk.*

### P1 — Trust the data *(durability)*
**Goal:** Locus is honest about durability and never silently drops data — **including the
differentiator state that currently vanishes on restart.**

| Item | E | What |
|---|---|---|
| DUR-1 | L | Truly-async `BGSAVE`/`BGREWRITEAOF` (snapshot on hub, I/O off-thread); stop lying |
| DUR-2 | S | `fsync` parent dir after rename; surface (stop swallowing) AOF fsync errors |
| DUR-3 | M | Configurable `appendfsync` (always/everysec/no) |
| DUR-4 | M | Fix AOF `SET`-with-past-TTL replay divergence (IMPROVEMENTS open bug) |
| DUR-6 | L | **Persist + version CDC log/offsets/groups + secondary indexes in RDB** |
| DUR-7 | S | Clean up orphaned `.tmp` from a crashed background save on startup |
| OBS-4 | L | Persistence INFO fields + in-progress flags + `LASTSAVE` |
| QA-1 | M | Property fuzz harness for `parse_command` (never panic / always progress) |
| QA-3 | M | Decoder property tests for RDB + AOF loaders (hostile/truncated input) |
| QA-4 | L | **Crash-recovery harness** — real `kill -9` mid-write, verify prefix-valid recovery |
| QA-12 | S | Commit `Cargo.lock` + `--locked --offline` build (enforces zero-dep) |

**Exit gate:** ≥50 kill-9/restart cycles across RDB+AOF prove no corruption and no lost
acked-fsynced write; a snapshot round-trips **all** state including CDC offsets and indexes;
fuzzers find no panic. *Est: 3–5 wk.*

### P2 — Look like real Redis *(driver compat + observability)*
**Goal:** off-the-shelf `redis-py`/`ioredis`/`node-redis`/`go-redis` connect and run without
falling back to slow paths; an operator can actually see what's happening.

| Item | E | What |
|---|---|---|
| COMPAT-1 | M | Thread per-client RESP2/3 version into reply encoding |
| COMPAT-2 | L | **Real incremental SCAN** cursor (drivers fall back to hub-blocking `KEYS` today) |
| COMPAT-3 | M | `HSCAN`/`SSCAN`/`ZSCAN` |
| COMPAT-4 | M | Real `COMMAND`/`COMMAND DOCS/COUNT/INFO` (clients probe these on connect) |
| COMPAT-5 | M | Real `CONFIG GET/SET` for the knobs that exist |
| COMPAT-6 | L | RESP3 typed replies (maps/sets/doubles/push) beyond `HELLO` |
| COMPAT-7 | L | `CLIENT` + per-client metadata registry |
| COMPAT-8 | M | `OBJECT ENCODING/REFCOUNT/IDLETIME` + `GETEX` |
| COMPAT-9 | S | Fix `HELLO` version (reports 0.1.0; build is 0.2.0) |
| COMPAT-10 | S | Publish `docs/COMPATIBILITY.md` matrix (supported/approximated/refused) |
| **DOC-1** | S | **Rewrite the README to lead with *why Locus*** (differentiators first; "zero-dep clone" demoted) — see §1. Do anytime; not blocked by code |
| OBS-1 | M | Metering core: per-command counters + latency histogram + ops/sec |
| OBS-2 | M | Flesh `INFO` into the six `redis_exporter` sections |
| OBS-5 | M | `SLOWLOG` on a bounded ring |
| OBS-6 | L | Real `CONFIG` registry + optional config file (default<file<env<runtime) |
| OBS-7 | M | Hub client registry (shared with NET-6 / COMPAT-7) |
| OBS-11 | M | `COMMAND`/`LASTSAVE`/`TIME`/minimal `DEBUG`; health/readiness contract |
| AUTH-4 | L | Optional simple ACL (command classes + key-prefix) — least-privilege |
| AUTH-7 | M | `CONFIG SET requirepass` + credential precedence + rotation |
| NET-3 | M | Bounded input buffer / pipeline depth |
| NET-4 | M | Bounded per-client output channel + slow-consumer policy |
| NET-6 | M | Shared per-client socket-handle registry (enables `CLIENT KILL`) |

**Exit gate:** `redis_exporter` reports `redis_up=1` with populated sections; a full scan returns
every live key once without materializing the keyspace; RESP3 replies are byte-verified against
real Redis; drivers connect with `HELLO 3` and hit zero unknown-command errors. *Est: 4–6 wk.*

### P3 — Replicate correctly *(replication v2)* — ✅ DONE (on `main`)
**Goal:** replication is resumable and non-diverging — the prerequisite for any HA.

> ✅ **Shipped:** REPL-1/2/3/4/5 + REPL-7 — partial-resync (`PSYNC … CONTINUE`) over a 4 MiB
> backlog ring, `WAIT`, real replid/offset, no expiry divergence, accurate `INFO`. **Remaining:**
> REPL-6 chaining (moved into P5) and the rigorous QA-5 divergence harness (targeted tests
> shipped; full randomized-churn oracle still pending).

| Item | E | What |
|---|---|---|
| REPL-1 | M | **Replica expiry role-guard** (`main.rs:1563`) + master DEL propagation (fixes divergence) |
| REPL-2 | M | Real replication ID + offset (replace hard-coded `0`); `master_repl_offset` |
| REPL-3 | L | Replication backlog ring + **PSYNC partial resync** (no more full-resync-on-blip) |
| REPL-4 | M | `WAIT numreplicas timeout` (ack-based durability) |
| REPL-5 | S | Accurate `INFO` replication fields (real link status, per-slave offset, lag) |
| REPL-7 | M | Verbatim master-stream logging (== DUR-5; stops replica double-logging) |
| REPL-6 | M | Replica chaining (sub-replicas) *(optional in P3, can slip to P4)* |
| QA-5 | L | Replication-divergence harness (master/replica byte-identical after churn) |

**Exit gate:** master and replica serialize to identical RDB bytes after randomized churn
including `SPOP`/`XADD *`/TTL; a network blip triggers partial (not full) resync; `WAIT 1 100`
behaves correctly. *Est: 3–5 wk.*

### P4 — Survive node failure *(HA + TLS)* — ✅ DONE (on `main`)
**Goal:** a node can die without data loss beyond a documented window or indefinite downtime;
the wire can be encrypted.

> ✅ **Shipped:** sentinel auto-failover (HA-1/HA-2) + replica-corroboration quorum +
> inter-sentinel agreement (HA-3 *in part*: majority + bully-style leader election over an
> authenticated peer plane) + `WAIT` bounded-loss (HA-5); TLS-1 sidecar guide + TLS-2 stream
> abstraction + TLS-3 optional `tls` (rustls) feature, default build still zero-dep.
> **Lighter than the full spec on:** HA-4 client redirection (documented, not automated), QA-6
> linearizability checker (targeted tests instead), COMPAT-11 push SDK (deferred to P5 polish).
>
> ⚠️ **Correction (session 2b, 2026-08-27).** This entry used to read
> "HA-3, majority + bully-style leader election → **no dual promotion**". **That claim was false
> and is withdrawn.** The code does not support it: the majority gate and the leader rule narrow
> the window, they do not close it. An *asymmetric* partition can still promote twice, and
> **HA-3's actual content — fencing the partitioned old master — was never built**: an old master
> keeps accepting writes while cut off and they are silently discarded on reconciliation. Epochs
> are wall-clock HLC stamps, not coordinated consensus numbers. So P4's failover is an
> orchestration hook for a **trusted network**, and every user-facing document now says exactly
> that. HA-3 is therefore **partially shipped**, and the P4 exit gate below ("no split-brain
> accepted writes; a fenced old master rejects writes") is **not met**. Closing it is a phase-6
> decision in [EXECUTION-PLAN-2026-08.md](EXECUTION-PLAN-2026-08.md), not a claim we get to make
> in the meantime.

| Item | E | What |
|---|---|---|
| HA-1 | M | Failover primitives: fencing epoch, read-only toggle, guarded promote |
| HA-2 | XL | **Sentinel-lite** monitor (quorum detect+promote) *and/or* documented k8s-operator hooks |
| HA-3 | L | Split-brain / fencing: epoch-stamped writes; demoted master rejects on higher epoch — **epoch stamping shipped; fencing a *partitioned* master did not** |
| HA-4 | M | Client failover UX: role-change notification + reconnect/redirection doc |
| HA-5 | S | `WAIT`-based bounded-loss contract (documented + tested) |
| TLS-2 | M | `Conn` transport abstraction (removes the `try_clone` duplex assumption) |
| TLS-3 | L | Optional `tls` cargo feature: native client + replica TLS via rustls (off by default) |
| TLS-1 | S | Document sidecar/proxy TLS termination (ships earlier; the default path) |
| COMPAT-11 | L | Thin push-only SDK (`@locus/reactive` TS + `locus-reactive` Py) over the CDC/geofence stream |
| QA-6 | XL | Linearizability checker + failover variant |

**Exit gate:** killing the master and promoting a replica completes with a documented,
*bounded* data-loss window and no split-brain accepted writes; a fenced old master rejects
writes; `cargo build --features tls` serves a real `redis-cli --tls`; the linearizability
checker passes across seeds incl. a failover scenario. *Est: 6–10 wk.*
**⚠️ Not met** on the fencing and split-brain halves — see the correction above.

### P5 — Depth & single-node scale-up — ✅ ESSENTIALLY COMPLETE (on `main`)
**Goal:** make the geo flagship *deep* and every single node sharper before distributing anything. All
std-only and independent of the cluster arc.

> ✅ **Shipped (2026-06-27):** GEO-IDX, GEO-FILT, ZSK-1, CLUSTER-1, RESP3-PUSH.

| Item | E | Status |
|---|---|---|
| GEO-IDX | L | ✅ geohash spatial index → sub-linear `GEOSEARCH` (cell-id doubles as the spatial shard key) |
| GEO-FILT | M | ✅ `GEOSEARCH … WHERE field value` (AND) over inline geo attributes |
| ZSK-1 | L | ✅ ordered `BTreeSet` index for sorted sets → range/rank without re-sorting |
| CLUSTER-1 | M | ✅ CRC16 `KEYSLOT` + `CLUSTER INFO/MYID/SLOTS/…` routing seam (P6 prep) |
| RESP3-PUSH | S | ✅ pub/sub push (`>`) frames on `HELLO 3` |
| GEO-FILT+ | M | ⬜ *(deferred)* finer S2/R-tree index + keyset pagination — optimization, not blocking |
| COMPAT-11 | L | ⬜ *(deferred)* thin push-only SDK (TS/Py) over the CDC/geofence stream |
| ~~PERF-1~~ | — | ❌ **DISMISSED** — thread-per-core fights the single-thread identity (one ordered point = the changefeed/geo enabler) and overlaps P6's cross-shard ordering. Horizontal (P6), not vertical, is the scale story. |
| ~~REPL-6~~ | — | ❌ **DISMISSED** — replica chaining is a niche read-fan-out win; risk to the working replication offset path; P6 sharding is the real scale lane. Revisit only if a many-read-replica need appears. |
| ~~MULTIDB~~ | — | ❌ **DISMISSED** — Redis discourages numbered DBs and Cluster bans DB>0, so it wouldn't compose with P6; big format/replication change for a legacy convenience. Use **key-prefix namespacing** instead (cluster-safe). `SELECT 0` stays for connect-compat. |

**Exit gate:** ✅ `GEOSEARCH` sub-linear + combined `WHERE` filter; sorted-set range/rank off an ordered
index; CRC16 routing seam in place. The deferred items (finer geo index, push SDK) are optimizations,
not blockers — **P5 is done; proceed to P6.**

### P6 — Scale out *(spatial-locality clustering — the flagship; LAST)*
**Goal:** the Tile38-beating lane — horizontal scale that shards by *space*, so `GEOSEARCH`
stays a bounded scatter-gather instead of an all-nodes fan-out. Done **last**, on a node that is
already deep (P5), fast (P5), durable (P1), and HA (P4).

> **Foundations already in place:** CLUSTER-1 (routing seam) + CLUSTER-2 (spatial cell index =
> GEO-IDX) ship in **P5**; CLUSTER-3 (persist+replicate CDC/index state) **is** DUR-6, done in
> **P1**. So P6's *true distribution* work (CLUSTER-4…11) starts on a node that already has the
> shard key, the routing seam, and durable reactive state.
>
> **Open design decision — confirm before starting P6:** spatial-first cells vs hash-slot-first
> (spatial as a layer on top). §6 recommends **spatial-first**, hash-slot as a degenerate mode.

| Item | E | What |
|---|---|---|
| CLUSTER-4 | M | Hybrid logical clock for cross-shard CDC ordering (the hard part) |
| CLUSTER-5 | L | std-only inter-node transport + framing (reuse RESP framing) |
| CLUSTER-6 | L | Rendezvous-hash cell→node map + `CLUSTER SLOTS/SHARDS/NODES` + MOVED/ASK |
| CLUSTER-7 | XL | Bounded scatter-gather GEOSEARCH + region-changefeed across shards, merge-by-distance |
| CLUSTER-8 | XL | Adaptive cell subdivision/merge (hot-spot balancing) — *last* |
| CLUSTER-9 | L | Per-shard failover integration (built on P3/P4) |
| CLUSTER-10 | L | Sharded sketches + secondary indexes (merge-at-read) |
| CLUSTER-11 | M | Cluster docs + tested consistency contract |

**Exit gate:** a static-cell N-node spatial cluster serves single-key ops with MOVED/ASK that
real cluster-aware clients follow; HRW reshuffles only ~1/N of cells on membership change;
cross-shard GEOSEARCH touches only intersecting shards and matches the single-node oracle;
cross-shard CDC delivers per-shard total order + HLC-monotone global order within a tested
staleness bound; cross-shard `MULTI` is cleanly rejected (`CROSSSLOT`). *Est: 4–8 mo.*

---

## 4. Domain specs (the detail)

Ten domains. Each lists its target state and work-item table. Items already mapped into
milestones above; this is the reference. *(Items authored from the deep audit, not the agent
run, are marked †: durability, repl, ha, netsafe.)*

### 4.1 Auth & access control
Make Locus safe to expose beyond loopback (modulo wire encryption, owned by TLS): `requirepass`,
optional simple ACL, command gating, protected mode, authenticated replication, runtime rotation.
All checks run in the single-threaded hub — race-free, no locking. *Detail in the agent spec; key
decisions: ship requirepass-first then a deliberately-simple class-based ACL (refuse full Redis-6
ACL grammar); vendor SHA-256; AUTH ships now with a documented cleartext caveat pending TLS.*

`AUTH-1`(M) requirepass core · `AUTH-2`(S) HELLO AUTH · `AUTH-3`(M) command classes ·
`AUTH-4`(L) simple ACL · `AUTH-5`(M) protected mode · `AUTH-6`(M) replication auth ·
`AUTH-7`(M) CONFIG/rotation · `AUTH-8`(M) tests + `INFO # Security` + docs.

### 4.2 TLS / transport security
Two-tiered: sidecar/proxy is the default zero-dep path; an optional `tls` cargo feature adds
native rustls for the client and replica links. The enabling refactor is a `Conn` abstraction
removing the `try_clone()` duplex assumption (`main.rs:1701`). At-rest encryption is explicitly
the OS/volume's job. **Never hand-roll TLS.**

`TLS-1`(S) sidecar docs · `TLS-2`(M) Conn abstraction · `TLS-3`(L) optional rustls feature ·
`TLS-4`(S) INFO tls_mode · `TLS-5`(S) at-rest scope-out doc.

### 4.3 Networking, resource limits & DoS safety †
Today: thread-per-connection (`main.rs:70-84`), reader blocks with **no timeout**
(`main.rs:1724`), unbounded input buffer, unbounded per-client output `mpsc`, no connection cap,
no `TCP_NODELAY`. Each is a DoS or exhaustion vector.

| Item | E | What | Anchors |
|---|---|---|---|
| `NET-1`† | S | Per-conn read + idle timeout; `TCP_NODELAY` | `main.rs:1697-1758`, `:1724` |
| `NET-2`† | M | Max-connection cap + graceful reject + `rejected_connections` | `main.rs:70-84` |
| `NET-3`† | M | Bounded input buffer / pipeline depth (cap unparsed bytes & queued cmds) | `main.rs:1722`, `resp.rs:7-10` |
| `NET-4`† | M | Bounded per-client output channel + slow-consumer policy (disconnect/flag) | `main.rs:1702-1709` |
| `NET-5`† | S | Configurable max bulk/request size (today 512 MiB, no socket-side mem bound) | `resp.rs:7-10` |
| `NET-6`† | M | Shared per-client socket-handle registry (enables `CLIENT KILL`, caps, timeouts) | `Msg::Connect`, `main.rs:1697` |
| `NET-7`† | S | Safe-by-default bind + Docker (with AUTH-5 protected mode) | `main.rs:64`, `Dockerfile` |
| `NET-8`† | M | DoS tests: slowloris, conn flood, giant pipeline, huge bulk | `tests/` |

**Decision:** keep thread-per-connection **+ hard caps** for the single-node 1.0. A poll/epoll
readiness reactor is an XL rewrite and std has no epoll wrapper (would need libc FFI or the `mio`
crate) — staying thread-per-conn also best preserves zero-dep. Revisit only if connection scale
(>~10k concurrent) ever demands it.

### 4.4 Durability & crash-safety †
Today: `BGSAVE`/`BGREWRITEAOF` are synchronous but reply "started" (`commands.rs:248-249`,
`main.rs:491`); AOF `fsync` result is swallowed (`aof.rs:69`) and the parent dir is never
fsynced; differentiator state (CDC log/offsets/groups `main.rs:111-119`, secondary indexes
`main.rs:118`) is RAM-only and lost on restart (`cdc_next_offset` resets to 1, `main.rs:221`).

| Item | E | What | Anchors |
|---|---|---|---|
| `DUR-1`† | L | Truly-async BG save/rewrite: serialize on hub (consistent by single-thread), write+fsync off-thread; in-progress flags; reject overlap | `commands.rs:248`, `main.rs:491`, `rdb.rs:27-49` |
| `DUR-2`† | S | `fsync` parent dir after rename; surface fsync errors → `aof_last_write_status` | `rdb.rs:27-49`, `aof.rs:58-72` |
| `DUR-3`† | M | Configurable `appendfsync` always/everysec/no (default everysec) | `aof.rs:58-72` |
| `DUR-4`† | M | Fix `SET`-with-past-TTL replay divergence (capture value+deadline atomically, or log DEL) | `aof.rs:86-144` |
| `DUR-5`† | M | Verbatim master-stream logging when `id==MASTER_ID` (stops replica double-logging) — *== REPL-7* | `main.rs:1280-1296`, `aof.rs:86` |
| `DUR-6`† | L | Persist + **version** CDC log/offsets/groups + indexes in RDB; restore on load — *== CLUSTER-3* | `rdb.rs:166/184`, `main.rs:131/118/221` |
| `DUR-7`† | S | Clean orphaned `.tmp` on startup (crashed BG save) | `rdb.rs`, `aof.rs` load |
| `DUR-8`† | M | RDB/AOF round-trip + crash-recovery tests (drives QA-4/QA-3) | `tests/` |

**Decisions:** capture-then-async-IO (single-thread makes the captured snapshot consistent —
no COW needed). Persist CDC offsets/groups (they *cannot* be rebuilt); persist secondary-index
*definitions* and rebuild contents on load (cheaper format). Add an RDB **version byte** so old
snapshots still load.

### 4.5 Replication v2 †
Today: async master→replica, but **full-resync-only** (offset hard-coded `0`, `main.rs:454-468`,
`:1604`), and the replica runs `active_expire()` unconditionally (`main.rs:1563`) → **divergence**.

| Item | E | What | Anchors |
|---|---|---|---|
| `REPL-1`† | M | **Replica expiry role-guard** + master DEL propagation for expired/evicted keys | `main.rs:1563`, `:1325` |
| `REPL-2`† | M | Real replid + offset; `master_repl_offset` advances per streamed byte | `main.rs:454-468`, `:1604` |
| `REPL-3`† | L | Backlog ring + **PSYNC partial resync** (CONTINUE) | `main.rs:454-468`, `:1574` |
| `REPL-4`† | M | `WAIT numreplicas timeout` + `REPLCONF ACK` | `main.rs` dispatch |
| `REPL-5`† | S | Accurate `INFO` replication (link status, per-slave offset, lag, read-only) | `main.rs:470-488` |
| `REPL-6`† | M | Replica chaining (sub-replicas) | replication wiring |
| `REPL-7`† | M | Verbatim master-stream logging (== DUR-5) | shared |
| `REPL-8`† | M | Replication-consistency tests (drives QA-5) | `tests/` |

**Decision:** **Locus-native, PSYNC-shaped** protocol — full vanilla-Redis-replica interop is a
non-goal (the differentiator state wouldn't replicate to stock Redis anyway), and a native
stream lets us carry CDC offsets / HLC. Document this clearly.

### 4.6 High availability & failover †
Today: **none.** No failover, Sentinel, quorum, or fencing. Master death = data loss (async) +
indefinite downtime. This is the one place the zero-dep/single-thread ethos is under real
pressure.

| Item | E | What |
|---|---|---|
| `HA-1`† | M | Failover primitives: fencing **epoch**, read-only toggle, guarded promote |
| `HA-2`† | XL | **Sentinel-lite** monitor (quorum detect+promote) and/or documented external-orchestration hooks |
| `HA-3`† | L | Split-brain / fencing: epoch-stamped writes; demoted master rejects on higher epoch |
| `HA-4`† | M | Client failover UX: role-change pub/sub event + reconnect/redirection doc |
| `HA-5`† | S | `WAIT`-based bounded data-loss contract (documented + tested) |
| `HA-6`† | L | Failover tests (drives QA-6 failover linearizability) |

**Decision (the big one):** ship **orchestration hooks + a minimal Sentinel-lite** for small
clusters; **explicitly defer embedded Raft** — it's multi-month, the single hardest correctness
risk, and the main identity-pressure point. Be honest in docs: until quorum election lands,
failover is operator-assisted / sidecar-driven with a **documented, bounded** data-loss window.

### 4.7 Observability, operations & lifecycle
Make the single-threaded hub the metering point (every command funnels through
`handle_command`/`exec_one`). Flesh `INFO` into six `redis_exporter` sections; add `SLOWLOG`,
per-command latency/ops, real `CONFIG GET/SET` + optional file, structured leveled logging,
graceful shutdown, `MONITOR` (config-gated, security-flagged), `CLIENT` registry, `COMMAND`
introspection. No embedded HTTP — honor the non-goal.

`OBS-1`(M) metering · `OBS-2`(M) INFO sections · `OBS-3`(S) real link status/readiness ·
`OBS-4`(L) persistence fields + async BG · `OBS-5`(M) SLOWLOG · `OBS-6`(L) CONFIG registry+file ·
`OBS-7`(M) client registry · `OBS-8`(M) MONITOR (gated) · `OBS-9`(M) logging ·
`OBS-10`(L) graceful shutdown · `OBS-11`(M) COMMAND/LASTSAVE/TIME/DEBUG/health.

### 4.8 Protocol & driver compatibility + SDK
Faithful-Redis core + raw custom verbs + one thin push SDK. Real incremental SCAN (the top
driver breaker), real COMMAND/CONFIG, end-to-end RESP3 typing, CLIENT/OBJECT/GETEX, a published
compatibility matrix, and `@locus/reactive` / `locus-reactive` for the changefeed/geofence push
surface only.

`COMPAT-1`(M) thread proto · `COMPAT-2`(L) SCAN · `COMPAT-3`(M) H/S/Z-SCAN · `COMPAT-4`(M) COMMAND ·
`COMPAT-5`(M) CONFIG · `COMPAT-6`(L) RESP3 typed · `COMPAT-7`(L) CLIENT · `COMPAT-8`(M) OBJECT/GETEX ·
`COMPAT-9`(S) HELLO version · `COMPAT-10`(S) COMPATIBILITY.md · `COMPAT-11`(L) push SDK.

### 4.9 Spatial-locality clustering
Shard by *space*, not key hash, so geo queries stay bounded. Lock the single-node seam first
(routing-oblivious keyspace, cell-ID index, durable CDC/index state), then a static-cell cluster
with HRW cell→node mapping and MOVED/ASK, then cross-shard merge-by-distance and adaptive
subdivision last. Generic hash-slot is a degenerate mode on the same router. Cross-shard CDC =
per-shard total order + HLC-monotone global order within a documented staleness bound.

`CLUSTER-1`(M) seam · `CLUSTER-2`(L) cell index · `CLUSTER-3`(L) persist state *(==DUR-6)* ·
`CLUSTER-4`(M) HLC · `CLUSTER-5`(L) inter-node transport · `CLUSTER-6`(L) HRW+CLUSTER cmds ·
`CLUSTER-7`(XL) scatter-gather geo · `CLUSTER-8`(XL) adaptive cells · `CLUSTER-9`(L) shard failover ·
`CLUSTER-10`(L) sharded sketches/indexes · `CLUSTER-11`(M) docs+contract.

### 4.10 Testing, verification & release hardening
A std-only QA program wired into CI as **milestone gates** — durability/repl/ha cannot be
declared done without their gating suites green.

`QA-1`(M) parser fuzz · `QA-2`(M) cargo-fuzz (dev-only) · `QA-3`(M) decoder fuzz ·
`QA-4`(L) crash-recovery · `QA-5`(L) repl divergence · `QA-6`(XL) linearizability ·
`QA-7`(L) soak/stress · `QA-8`(L) benches+guard · `QA-9`(S) MSRV · `QA-10`(M) multi-platform tests+nightly ·
`QA-11`(M) ASan/Miri/TSan · `QA-12`(S) `--locked` reproducible build · `QA-13`(M) cosign/SBOM/SLSA ·
`QA-14`(S) BG-async + INFO truthfulness tests · `QA-15`(S) gate-matrix doc.

---

## 5. QA gate matrix (what blocks "done")

| Milestone | Cannot ship until these are green |
|---|---|
| P0 | NET DoS tests (`NET-8`), graceful-shutdown test, auth integration tests (`AUTH-8`) |
| P1 | `QA-1` parser fuzz, `QA-3` decoder fuzz, `QA-4` crash-recovery (≥50 cycles), `QA-12` locked build, `QA-14` BG-async truthfulness |
| P2 | RESP3 golden byte-match, SCAN exactly-once property test, `redis_exporter` smoke, COMPATIBILITY.md drift-guard |
| P3 | `QA-5` master/replica byte-identical after churn, partial-resync test, `WAIT` test |
| P4 | `QA-6` linearizability incl. failover, fencing/split-brain test, `--features tls` redis-cli smoke |
| P5 | GEOSEARCH sub-linear + matches brute-force oracle, combined-filter test, zset O(log n) benchmark, thread-per-core throughput bench |
| P6 | scatter-gather vs brute-force oracle, HRW reshuffle bound, cross-shard CDC staleness bound, live-migration zero-loss |
| 1.0 release | `QA-9` MSRV, `QA-10` multi-platform, `QA-11` sanitizers, `QA-13` signed+SBOM+provenance, `QA-7` soak leak-guard |

---

## 6. Gaps, risks & decisions to confirm before starting

**Gaps the milestones must not forget** (surfaced as cross-cutting; fold into the nearest phase):
- **Backup/restore tooling & RDB format-version upgrade path** — DUR-6 changes the on-disk
  format; ship a version byte + a documented migration/restore story (P1).
- **Data-migration / rolling-upgrade path** between Locus versions (P3/P4).
- **`SELECT`/multi-DB** — architectural (`Hub` owns one `Db`); minor, optional, decide in P2.
- **Rate limiting / per-client quotas / audit logging** — beyond protected-mode; decide if in
  scope for multi-tenant (likely P2/P4).
- **Capacity planning + runbooks + ops docs** — required for a real "production" claim (P2→).
- **Clock assumptions** — the HLC (CLUSTER-4) and TTLs assume sane wall-clock; document NTP
  expectations and monotonic-vs-wall usage.
- **Supply-chain/CVE policy** — largely *moot* thanks to zero-dep; `QA-12`/`QA-13` turn that
  into a verified guarantee (a real asset — protect it).

**Top risks:**
| Risk | Mitigation |
|---|---|
| HA correctness (split-brain, lost writes) is genuinely hard solo | Defer Raft; ship hooks + Sentinel-lite + an **honest, tested** data-loss window; lean on k8s |
| RDB format change (DUR-6) breaks existing snapshots | Version byte + load-old-format path + migration test |
| Clustering scope (P6) balloons | Do it **last**; single-node seam (CLUSTER-1/GEO-IDX) ships in P5; static-cell cluster before adaptive subdivision; `CLUSTER-8` last |
| The `try_clone` duplex model fights TLS | `TLS-2` `Conn` abstraction is a prerequisite for `TLS-3` |
| Scope realism for a solo author | P0–P1 deliver most of the value in ~6–9 weeks; everything after is incremental and shippable |

**Decisions to confirm before you start** (recommendations in bold — all reversible):
1. **ACL richness:** requirepass-first, **then a simple class-based ACL** (refuse full Redis-6 ACL). 
2. **TLS posture:** **sidecar default + optional `tls` cargo feature** (never hand-roll). 
3. **HA approach:** **orchestration hooks + Sentinel-lite, defer Raft.** ← the consequential one. 
4. **Replication protocol:** **Locus-native PSYNC-shaped** (vanilla-Redis-replica interop = non-goal). 
5. **Clustering substrate:** **spatial-first, hash-slot as a degenerate mode** on the same router. 
6. **Cell encoding:** **geohash-prefix first** behind a `CellScheme` trait; add S2 before adaptive cells.

---

## 7. Honest effort & reality check

- **Single-node 1.0 (P0–P3):** ✅ **done.** This is the real "production-ready Locus" and the
  right place to declare 1.0.
- **+ HA + TLS (P4):** ✅ **done.** Crosses from "single node you trust" to "survives failure."
- **+ Depth & scale-up (P5):** ✅ **done** — geohash geo index + `WHERE` filters, ordered-index sorted
  sets, CRC16 routing seam, RESP3 push. (PERF-1/REPL-6/MULTIDB dismissed — niche/legacy or fold into P6.)
- **+ Clustering (P6):** ~**+6–12 months**. The flagship; its own arc; done **last** — the only milestone
  remaining. Upfront decision: **spatial-first vs hash-slot-first**.
- **Full vision:** P0–P5 are in; only the P6 clustering arc remains.

This is *ordinary hardening*, not research risk — every item is scoped and grounded. The
zero-dep identity survives the entire journey except at HA-consensus, where the plan
deliberately chooses pragmatism (hooks + Sentinel-lite) over a heroic, risky homegrown Raft.

---

## 8. Recommended first PR (start here)

**"P0 Phase 0: make Locus safe to expose"** — five small, independent, low-risk changes that move
Locus from *"never expose this"* to *"safe on a trusted network,"* and unblock everyone testing it:

1. `AUTH-1` — `requirepass` + `AUTH` + NOAUTH gate.
2. `AUTH-5` — protected mode (closes the Docker `0.0.0.0` hole).
3. `NET-1` — per-connection read/idle timeout + `TCP_NODELAY` (kills slowloris).
4. `NET-2` — max-connection cap (kills thread/memory exhaustion).
5. `OBS-10` — graceful SIGTERM (drain → fsync → final save).

Each is testable over the existing TCP integration harness, each is zero-dep, and together they
close the four scariest current holes. After that, `AUTH-6` (replication auth) and the rest of
P0, then P1 durability.

> When you're ready to implement, say the word and I'll start with this PR — or generate a
> focused implementation plan for any single milestone or work item.
