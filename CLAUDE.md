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

## Current state — updated 2026-08-27 after session 1b

v0.7.0, ~18,000 lines of Rust (`std`-only except `src/tls.rs`, which is behind the optional feature),
105 unit + 104 integration tests green. Phases 0 and 1 of the plan are done: the hub now has a panic
boundary, and the three live-verified ACL defects are closed. Session 1b then cleared the debt those
left behind — the port flake, the ACL handshake gate, the release tags, and the publication gate.

Still open, and the reason the plan continues: measured against `redis-server 8.8` on the same
machine, simple ops are within 1.2× and `GEOSEARCH` within 6×, but large-collection writes are 60–268×
slower and zset range reads 390–632× slower — both from two localized defects that sessions 2 and 3
own. See `plans/EXECUTION-PLAN-2026-08.md` for the full picture and the sequence.

**The `node exited early` flake is fixed** (session 1b). `free_port()` no longer bind-races: it hands
out ports from a fixed window below every OS ephemeral range, walked by a process-wide counter and
sliced by pid, so neither another test nor another `cargo test` process can take a port out from under
a spawning node. If a cluster node ever does die at startup again, the panic now carries the child's
exit status and stderr instead of just "node exited early".

**Still flaky, and not yet owned by any session:** `disk_tier_survives_kill9_with_aof_and_rewrite`
waits a fixed 300 ms for `BGREWRITEAOF` to land. That holds in a normal run but not on a heavily loaded
machine — measured 8 red in 24 with eight suites running at once, and 0 in 20 running normally. It is a
test-timing bug, not a product bug. Re-run before diagnosing, and say so in your report.
