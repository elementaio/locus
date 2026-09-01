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
| `tests/perf.rs` | The perf harness — `#[ignore]`d; spawns a server (and a `redis-server`, if present) and prints the comparison table |
| `plans/EXECUTION-PLAN-2026-08.md` | **The current plan.** What we are fixing and why |
| `plans/SESSIONS.md` | **The session ledger.** Briefs, delivery reports, manager reviews |
| `plans/DESIGN-PRINCIPLES.md` | The identity above, argued in full |
| `plans/IMPROVEMENTS-AUDIT-2026-07.md` | The July multi-agent audit — 84 findings, mostly still open |
| `docs/` | The published documentation (README's targets) |

## Current state — updated 2026-09-01 after session 5b

v0.9.0 (unreleased), ~18,900 lines of Rust (`std`-only except `src/tls.rs`, which is behind the
optional feature), 120 unit + 126 integration tests green on both feature sets. **Phases 0–3 are done
and phase 4 is in progress.** Tagged and **pushed** to public `origin`: `v0.7.0` (phases 0–2 + the
pulled-forward sentinel security fix) and `v0.8.0` (phase 3). Phase-4 work (session 5) is committed on
`main` but unreleased.

- **Phase 1** — the hub has a panic boundary (`catch_unwind`; a command bug becomes one `-ERR`, not an
  outage), and the three live-verified ACL defects are closed. Session 1b cleared the debt behind it
  (the port flake, the ACL handshake gate, the release tags, the publication gate).
- **Phase 2 (perf)** — memory accounting is lazy now, so large-collection writes went from 60–268×
  Redis to low single digits (writing into a 200k-element collection costs the same as a fresh key);
  and zset range reads walk the ordered index in place, taking `ZRANGE key 0 9` on a 200k zset from
  ~34 ms to ~0.05 ms (446×). The perf harness is `tests/perf.rs`.
- **Session 2b (security)** — the sentinel peer plane is authenticated (loopback by default, shared
  secret on every verb, refuse-to-start without one), closing an unauthenticated `SWITCH`
  replication-takeover; and every doc that claimed partition-safety was corrected.
- **Phase 3 (durability)** — automatic save points (`LOCUS_SAVE`, on by default at Redis's cadence);
  a CRC-32 footer on every RDB (bad checksum refuses to start; pre-0.8.0 snapshots still load);
  `appendfsync=always` now returns `-MISCONF` instead of acking a failed fsync; `everysec` fsync moved
  off the hub; and `BGSAVE` stays on the hub (no `fork()` — unsafe with 2N threads) with the stall
  measured (`rdb_last_bgsave_hub_stall_us`) and a backup-from-a-replica procedure in `docs/DEPLOYMENT.md`.

`ZRANK` remains O(rank) — deliberately (no order-statistic tree in `std`; the bookkeeping would risk
the map-and-index lock-step invariant for a cold-path gain).

**Phase 4 is in progress ("make the flagship honest").** Session 5 landed item 4.1: the changefeed's
at-least-once promise is now real. A consumer that died between `CDCREADGROUP` and `CDCACK` used to
strand its records forever; now the PEL carries per-entry `{consumer, delivered_ms, delivery_count}`,
`CDCREADGROUP … 0` re-delivers a consumer's own pending, and `CDCCLAIM`/`CDCAUTOCLAIM` (min-idle-gated,
bounded scan) let a live consumer take over a dead one's work. The extras trailer versioned LXT2→LXT3
with a backward-compatible load. This is committed as **unreleased v0.9.0** (`Cargo.toml` is at 0.9.0,
the `[0.9.0]` CHANGELOG section is open) but **not tagged and not pushed** — v0.9.0 waits for the rest
of phase 4.

**Still open before the v0.9.0 tag:**
- **Session 6** — spatial-index precision (item 4.2): `GEOSEARCH` at a large radius still stalls the
  hub (~133 ms at 20 km on 200k points) because `ranges_for_box` picks cells too coarse.
- **Session 5c** — reclassify `CDCGROUP CREATE`/`DESTROY` from `@read` to `@write` (they mutate
  replicated state now); one-line BREAKING ACL change.
- **Session 3b** (anytime) — convert `ZRANGEBYSCORE`'s low-bound `skip_while` to a `range` seek.

Session 5b landed: `CDCGROUP CREATE`/`DESTROY` (only those) now propagate to the AOF and replication,
so a group survives an unclean stop and reaches a replica. It also fixed a **pre-existing** durability
bug — `propagate` (eviction/expiry/slot-migration writes) did not mirror into an in-flight
`BGREWRITEAOF` tail, so such writes landing mid-rewrite were lost on crash replay; now fixed for every
`propagate` site.

See `plans/EXECUTION-PLAN-2026-08.md` and `plans/SESSIONS.md`.

**The `node exited early` flake is fixed** (session 1b). `free_port()` no longer bind-races: it hands
out ports from a fixed window below every OS ephemeral range, walked by a process-wide counter and
sliced by pid, so neither another test nor another `cargo test` process can take a port out from under
a spawning node. If a cluster node ever does die at startup again, the panic now carries the child's
exit status and stderr instead of just "node exited early".

**Both test flakes are fixed.** `free_port()` above (session 1b), and
`disk_tier_survives_kill9_with_aof_and_rewrite` (session 2b, item 2b.4) — it no longer sleeps a fixed
300 ms for `BGREWRITEAOF`; it polls `aof_rewrite_in_progress` down to 0. The suite is green on both
feature sets with no known timing races.
