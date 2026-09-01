# CLAUDE.md — Locus

A zero-dependency, from-scratch Redis-protocol datastore in Rust, with a reactive changefeed and a
geo-first spatial model. One static binary, pure `std`.

---

## How we work — read this first

- **Project manager:** a standing manager session. It owns `plans/EXECUTION-PLAN-2026-08.md`, writes
  the session briefs in `plans/SESSIONS.md`, reviews every delivery, and makes the calls. It does not
  implement.
- **Execution sessions:** one session per brief. If you are one, read **this file**, then
  `plans/EXECUTION-PLAN-2026-08.md`, then **your own entry** in `plans/SESSIONS.md` — and nothing else
  unless your brief sends you there. Do only what your brief bounds. When done, write your delivery
  report into your own entry and stop. **Do not start the next session.**
- **Decisions belong to the manager.** If your brief turns out to be wrong, or the fix needs something
  outside your bounds, do the part you legitimately can, then write the question under a
  **`Decision needed`** heading in your report. Do not decide unilaterally and do not silently widen
  scope. A brief that was wrong is useful information — say so plainly; several past briefs were
  improved exactly this way.
- **Report honestly.** If a test fails, say so with the output. If you skipped something, say that. The
  manager independently re-verifies every delivery, so an optimistic report only costs a round trip.

---

## The identity — do not trade these away

These are settled. If a task seems to require breaking one, that is a **`Decision needed`**, not a
judgement call. Full reasoning in `plans/DESIGN-PRINCIPLES.md`.

1. **Zero third-party crates in the default build.** Pure `std`. `rustls` behind the optional `tls`
   feature is the only exception, and only when explicitly asked for. Binding the platform libc via FFI
   (as `signal` and `getrlimit` already do) is fine — that is the C runtime the binary already links,
   not a dependency.
2. **One hub thread owns the keyspace.** Every command executes serially at one point that holds the
   value before *and* after. This is the *source of the advantage* — it is what makes a gap-free,
   ordered changefeed true by construction rather than bolted on. Never "fix" it with locks, sharded
   hubs, or a thread-per-core rewrite. Scale is horizontal.
3. **One small static binary, configured by environment variables.** No config-file format, no plugin
   system, no numbered databases.
4. **We do not chase Redis parity.** Lua, modules, and whole categories are deliberately out. The bar
   is *per-type completeness* in what we keep, and skipping whole categories on purpose.
5. **Ship the primitive, refuse the policy.**

---

## Before every commit — the loop

The same one CI runs, on **both** feature sets. All five must pass; nothing red gets committed.

```
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo clippy --all-targets --features tls -- -D warnings
cargo test
cargo test --features tls
```

Performance work additionally runs the harness (`tests/perf.rs`, landed in session 2):
`cargo test --release --test perf -- --ignored --nocapture`.

## Commit rules

- **Commit direct to `main`.** No PRs, no feature branches.
- One commit group per session, prefixed by its phase: `fix(p1):`, `perf(p2):`, `harden(p3):`.
- **Docs move with the code, in the same commit** — README, `docs/`, `CHANGELOG.md`, and the version
  line in `Cargo.toml`. This is a hard project rule. There is no "docs pass later".
- **A regression test per numbered finding** — one that fails on the old code and passes on the new.
  Not one test for the whole session.

## Security note

`elementaio/locus` is a **public** repository, and `plans/` still holds working reproduction steps for
unpatched defects — so publication is gated **per file**, not all-or-nothing. The criterion (plan item
0.1, corrected in the session 1 review): *publish a document when nothing in it maps a defect that is
still exploitable.*

`.gitignore` enforces it fail-closed — `/plans/*` ignores everything and each published file is
un-ignored by name. **A note you add under `plans/` is private until someone lists it there**, which is
the manager's call, not a session's. Session 1b opened the gate for the design corpus (16 files); the
execution plan, the session ledger, the July audit and the July hardening review stay held until phase
6 is decided. Do not put an unfixed vulnerability's repro in a public commit message, CHANGELOG entry,
or issue.

---

## File map

| Path | What it is |
|---|---|
| `src/main.rs` | The hub, connections, replication, cluster, sentinel wiring — the server |
| `src/commands.rs` | The command table and every command implementation |
| `src/db.rs` | The keyspace, `Value`, `ZSet`, expiry, memory accounting, the geo index |
| `src/rdb.rs` · `src/aof.rs` | Snapshot and append-only persistence |
| `src/resp.rs` | RESP2/RESP3 parsing and encoding (resumable, adversarial-input hardened) |
| `src/geohash.rs` | 52-bit interleaved cell ids — the spatial index and the shard key |
| `src/tls.rs` | In-process TLS termination — **only** compiled under the optional `tls` feature |
| `src/sketch.rs` | Bloom, HyperLogLog, Count-Min, Top-K, t-digest |
| `src/pubsub.rs` · `src/streams.rs` · `src/acl.rs` · `src/tier.rs` · `src/sentinel.rs` · `src/hlc.rs` · `src/log.rs` | One subsystem each |
| `tests/integration.rs` | 104 end-to-end tests — failover, resharding, partial resync, crash recovery |
| `src/lib.rs` | The `locusdb` **library** — the engine (keyspace, commands, codec, persistence, geo, sketches), no server; `main.rs` is the thin `locus` binary over it |
| `src/util.rs` | Tiny shared helpers (`ct_eq`) used by both the library and the binary |
| `tests/perf.rs` · `tests/embedding.rs` | The perf harness (`#[ignore]`d, spawns a server) · the out-of-crate embedding test (proves the public API) |
| `tests/differential.rs` | The command differential — randomized sequences run against the engine in-process **and** a real `redis-server`, replies diffed (smoke subset by default, long run `#[ignore]`d) |
| `tests/fault.rs` | Fault injection over a spawned server — master SIGKILLed mid-stream, replication link dropped, failover raced, slot migrated under load |
| `plans/EXECUTION-PLAN-2026-08.md` | **The current plan.** What we are fixing and why |
| `plans/SESSIONS.md` | **The session ledger.** Briefs, delivery reports, manager reviews |
| `plans/DESIGN-PRINCIPLES.md` | The identity above, argued in full |
| `plans/IMPROVEMENTS-AUDIT-2026-07.md` | The July multi-agent audit — 84 findings, mostly still open |
| `docs/` | The published documentation (README's targets) |

## Current state — updated 2026-09-01 after session 8 (phase 5 complete)

v0.11.0, ~19,000 lines of Rust plus a test-harness suite (`std`-only except `src/tls.rs`, behind the
optional feature). 135 unit + 128 integration + 6 embedding + differential + fault tests green on both
feature sets. **Phases 0–5 are done, and phase 6 is decided (freeze).**

- **Pushed to public `origin`:** `v0.7.0` (phases 0–2 + the pulled-forward sentinel auth), `v0.8.0`
  (phase 3), `v0.9.0` (phase 4).
- **Tagged but NOT yet pushed:** `v0.10.0` (the library split) and `v0.11.0` (session 8's nine fixes).
  The owner pushes both after the session-8 review.

What each phase delivered:

- **Phase 1** — a panic boundary around the hub (`catch_unwind`: a command bug is one `-ERR`, not an
  outage) and the three live-verified ACL defects closed. Session 1b cleared the debt behind it.
- **Phase 2** — memory accounting is lazy (large-collection writes 60–268× → low single digits) and
  zset range reads walk the ordered index in place (`ZRANGE key 0 9` on 200k: ~34 ms → ~0.05 ms).
  Harness: `tests/perf.rs`.
- **Session 2b (security)** — the sentinel peer plane is authenticated; every doc claiming
  partition-safety was corrected.
- **Phase 3** — automatic save points (`LOCUS_SAVE`), an RDB CRC-32 footer (bad checksum refuses to
  start; old snapshots still load), `appendfsync=always` no longer acks a failed fsync, `everysec`
  fsync off the hub, and backup-from-a-replica documented (no `fork()` — unsafe with 2N threads).
- **Phase 4** — the changefeed's at-least-once promise is real (`CDCCLAIM`/`CDCAUTOCLAIM`, self-pending
  re-read; group existence AOF+replication-durable via `CDCGROUP CREATE`/`DESTROY`, `@write`-classed);
  and the spatial index was rewritten so large-radius `GEOSEARCH` no longer stalls the hub (20 km on
  200k: ~181 ms → ~0.08 ms, now faster than Redis). Session 5b also fixed a pre-existing durability bug
  (`propagate` writes — eviction/expiry/migration — were lost if they landed mid-`BGREWRITEAOF`).
- **Phase 5** — the engine is the **`locusdb` library** (`src/lib.rs`; the server is a thin binary over
  it, byte-identical, thin-LTO release); and a differential harness (`tests/differential.rs`, engine
  in-process vs `redis-server`, 2.2M commands clean) plus a fault-injection suite (`tests/fault.rs`)
  found and fixed **nine defects** — a self-move TTL-immortality bug, glob `[...]` classes, negative-
  range clamping, arity panics, and more. The fault suite found no product bug; the phase-6
  documented-unsafe path is pinned as an assertion.

**Note on the glob fix (session 8):** it widened ACL **channel** patterns (globs), `KEYS`/`SCAN`/
`PSUBSCRIBE` — **not** ACL key scope, which is a literal prefix (`starts_with`, acl.rs:184) and was
unaffected. A glob-looking key pattern (`~app:[0-9]*`) silently degrades to the prefix `app:[0-9]`
(fail-closed); documented in `docs/COMMANDS.md`.

**Phase 6 is DECIDED (2026-09-01): freeze & document (A).** The cluster/sentinel layer ships
documented-unsafe ("trusted network, operator-driven, not partition-safe") and gets **no further
investment now**. Do not start cluster/HA hardening: option (B) (min-replicas knob, off-hub
MIGRATESLOT, off-hub scatter, ASK) is recorded in the plan, ready only on a real customer need;
option (C) (consensus/Raft) is permanently off the table (identity).

**Next: phase 7 — the larger layer** (multi-tenancy, a Go client for Motus, the browser sync SDK).
Non-blocking loose ends: **session 3b** (`ZRANGEBYSCORE` low-bound `skip_while` → `range` seek) and the
**session 9** P3-batch (`NaN` in zsets, `EXPIRE` flags, `EXPIRETIME`/`HINCRBYFLOAT`/`ZRANGEBYLEX`/
`MEMORY USAGE`, observability counters, gate the `DEBUG` tests to debug builds, and whether ACL key
scope should learn real globs). See `plans/EXECUTION-PLAN-2026-08.md` and `plans/SESSIONS.md`.
