# Changelog

All notable changes to Locus are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.11.0] — 2026-09-01

**The differential harness found these.** Phase 5.2 added two test harnesses — randomized command
sequences run against the `locusdb` engine in-process *and* a real `redis-server`, with the replies
diffed; plus fault injection over a spawned server. The first one found seven behavioural divergences
from Redis and two outright panics in its first afternoon, all of them fixed below. A single clean run is now 4,000
sequences of 500 commands — 2,245,423 executed, zero unexplained divergences.

### Fixed

- **A negative index that stayed negative was not clamped.** `GETRANGE key -100 -90` on `"hello"`
  returned `""` instead of `"h"`, and `BITCOUNT`/`BITPOS` with a range reaching before the start
  answered 0. Redis pins *both* ends of the range to the value, not just the start; `GETRANGE` had its
  own copy of the normalization, which is how one defect became two. `GETRANGE`, `BITCOUNT` and
  `BITPOS` now share one implementation — including the quirk that `GETRANGE` and `BITCOUNT` (but not
  `BITPOS`) discard an inverted all-negative range *before* wrapping it.
- **`RPOPLPUSH`/`LMOVE`/`SMOVE` answered `WRONGTYPE` for a missing source.** When the source did not
  exist and the destination held another type, Locus reported the destination's type where Redis
  answers nil (or 0 for `SMOVE`) — the missing source short-circuits first. The destination is still
  type-checked before anything is popped, so a move never removes and then fails.
- **A self-move made an expiring key immortal.** `RPOPLPUSH k k` on a one-element list — a rotate — and
  `SMOVE s s m` on a one-member set retired the key in the gap between the pop and the push, taking its
  TTL with it; the push then recreated a fresh, *persistent* key. A value that was supposed to expire
  never did. The push now happens before the emptied source is retired, which is Redis's order.
- **`SET` accepted contradictory options.** `SET k v NX XX` returned nil, two expire options let the
  last one win, an expire option alongside `KEEPTTL` was accepted, and `SET k v EX 0` set a key that was
  already expired when the `+OK` went out. All four are now `-ERR syntax error` / `-ERR invalid expire
  time in 'set' command`, as on Redis.
- **An integer had more than one accepted spelling.** Rust's `str::parse::<i64>` takes a leading `+`,
  leading zeros and `-0`; Redis's `string2ll` takes none of them. `DECR` on a value that `APPEND` had
  made `"02"` answered `1` instead of an error, and `LRANGE key +0 -1` was accepted. Stored values and
  arguments now use one strict parser. Numbers Locus writes itself are always canonical, so nothing
  internal changes — but a client that was sending `+5` or `007` will now get
  `-ERR value is not an integer or out of range`.
- **The glob matcher understood only `*` and `?`.** Every bracket pattern therefore matched *nothing*:
  `KEYS user:[0-9]*`, `SCAN … MATCH`, `HSCAN`/`SSCAN`/`ZSCAN`, `PSUBSCRIBE`, `CLIENT LIST` filters and —
  this is the one to check on upgrade — **ACL key and channel patterns**. An ACL grant written
  `&app:[0-9]*` was silently an empty grant; it now grants what it says. `[abc]`, `[a-z]`, `[^a]` and
  `\` escapes all work, matching Redis's `stringmatchlen` (quirks included: `[1-]` really is the range
  `'1'..=']'`).
- **`INCRBYFLOAT` used the sorted-set score formatter.** Redis has two double renderings and this
  command uses the other one — plain notation with trailing zeros trimmed, not shortest-round-trip — and
  the reply is also the stored value, so the difference stuck to the key. `INCRBYFLOAT` on `-2.251` by
  `-5.25` returned `-7.5009999999999994`; it now returns `-7.501`. `ZSCORE` keeps the shortest-round-trip
  rendering it already shared with Redis. One difference remains and is deliberate: Redis accumulates
  this command in C `long double` (80-bit on x86, plain `double` on arm64 — Redis does not agree with
  itself across machines), and Locus is pure `std`, so results needing more than f64's 15 significant
  digits differ in the last digits.

- **Two commands panicked on a bare command name.** `execute(&[b"BITCOUNT"])` and
  `execute(&[b"CMSINCRBY"])` indexed past the end of the argument list: `BITCOUNT`'s arity guard
  rejected 3 and more-than-5 arguments but never *fewer than 2*, and `CMSINCRBY` sliced `tokens[2..]`
  before checking anything. On the server the hub's panic boundary (0.7.0) turns that into one `-ERR`
  rather than an outage, but the `locusdb` **library** has no such boundary — `execute` is a plain
  function an embedder calls on its own thread — and since 0.10.0 that is a published API. Both are
  fixed, and a probe now calls **every** command name with one to seven arguments drawn from four
  filler alphabets, against an empty keyspace and one holding every value kind, so a missing arity
  guard cannot come back quietly. (Found by the differential harness's own coverage probe, which asks
  each command name whether the engine knows it by sending exactly that: the name and nothing else.)

### Added

- **`tests/differential.rs`** — the command-level differential harness. Randomized, seeded sequences
  over strings, lists, hashes, sets, sorted sets, bitmaps, expiry and the scan family, executed against
  a `Db` in-process and a `redis-server` over a socket, with the replies diffed after a written-down set
  of normalizations (unordered replies as multisets, errors by code, TTLs with wall-clock slack) — each
  of which is *counted*, so the run reports how often it actually had to relax a comparison. A failure
  prints the seed, the sequence, both replies and the command line that reproduces it. The default
  `cargo test` runs a smoke subset; `--ignored` runs the long one, plus a coverage report that probes
  which Redis commands in the shared families the engine implements.
- **`tests/fault.rs`** — the fault-injection harness. A real `locus` binary over a socket with a fault
  injected mid-path: the master SIGKILLed under load (the replica must hold a consistent prefix), a
  replication link dropped mid-stream (the resumed stream must replay exactly the missed commands), a
  replica whose offset fell out of the backlog (must be sent for a full resync, not handed a hole), a
  failover raced by two sentinels (exactly one promotion; every `WAIT`-acknowledged write survives), and
  a slot migrated under concurrent writes (nothing lost, nothing left duplicated on the source).
  Documented-unsafe paths are *asserted rather than failed*: `docs/DEPLOYMENT.md` says a returned old
  master is never fenced and its writes are silently discarded, and the harness pins exactly that.

- **The test harnesses' port windows are now provably disjoint.** `free_port` slices a fixed window
  below every platform's ephemeral range; the slice index used to come from the pid alone, which
  separated two concurrent `cargo test` *processes* but left the test binaries *within* one run relying
  on their pids happening to differ. With four server-spawning binaries running at once (this release
  added the third and fourth) that started losing — two of six full runs died on `EADDRINUSE` at
  startup. The slice index now carries the harness's own id in its low bits, so integration, perf,
  differential and fault cannot draw the same number as each other however their pids fall, and the
  window widened to 16384–32768 so pid separation was not traded away for it. The differential harness
  additionally holds a lock while it has a reference server up, so one `redis-server` exists at a time
  rather than one per test — a full concurrent run had otherwise managed to get one killed
  mid-assertion — and it handshakes with `PING` before handing the connection out, because a listening
  socket is not a ready server.

Both harnesses need a `redis-server` on `PATH` only where they compare against one, and skip cleanly —
printing why — when there isn't one. A missing Redis never fails the suite.

## [0.10.0] — 2026-09-01

- Internal: the release profile now enables thin LTO — the library split moved the hub→engine calls
  across a crate boundary once per command, and thin LTO restores cross-crate inlining on that path.
  No behaviour change.

**Locus is now embeddable.** The package builds two targets instead of one: the `locus` binary — the
server, unchanged — and a new `locusdb` library, the engine underneath it. Nothing about running Locus
as a server changes: same commands, same replies, same persistence, same binary name. This is a
structural release that opens three doors at once — embedding Locus in another Rust program, writing
fuzz targets against the parser and the keyspace, and testing internals from outside the crate (the
differential harness that comes next is built on it).

### Added

- **`locusdb` library target.** `src/lib.rs` exports the engine: the keyspace and command
  implementations, the RESP codec, the RDB/AOF formats, the geo index, the sketches, the ACL model, the
  changefeed's stream types and the cold tier. Curated at the crate root as `Db`, `Value`, `ZSet`,
  `execute`, `execute_proto`, `parse_command`, `Parsed`, `now_ms` and `ct_eq`, with every module also
  reachable by path.

  Commands go in as argument vectors and replies come back as encoded RESP bytes — the same bytes the
  server would put on a socket — so the engine runs entirely in-process, with no socket and no threads:

  ```rust
  use locusdb::{Db, execute, resp};

  let mut db = Db::new();
  execute(&[b"SET".to_vec(), b"k".to_vec(), b"v".to_vec()], &mut db);
  assert_eq!(execute(&[b"GET".to_vec(), b"k".to_vec()], &mut db), resp::bulk_string(b"v"));
  ```

  `Db` has no interior locking — one thread owns the keyspace by design — so an embedder sharing one
  across threads brings its own mutual exclusion. Commands that only mean something to a *server*
  (replication, `SUBSCRIBE`, blocking pops, the changefeed's group plumbing) stay with the binary's hub.
  See [Embedding Locus as a library](README.md#embedding-locus-as-a-library) and the worked example in
  `tests/embedding.rs`.
- **`Default` for `Db`, `Stream`, `Hll` and `PubSub`**, each delegating to the existing `new()`. These
  types are public API now, and the idiomatic constructor pair should exist for them.
- **A `util` module** holding `ct_eq`, the constant-time byte comparison. It was a private helper in
  `main.rs` that `sentinel` reached back into — the single symbol standing between the modules and a
  clean library split.

### Changed

- **The server keeps the whole server.** The hub thread, the connection reader/writer pair,
  replication, cluster, gossip and the sentinel wiring stay in the `locus` binary and are deliberately
  *not* exported: an embedder brings its own concurrency model. The optional `tls` module stays in the
  binary too, because it drives connections through that hub plumbing rather than through the keyspace.
- `cargo install locusdb` still installs a `locus` command, exactly as before; the crate simply now also
  works as a `cargo add locusdb` dependency.

## [0.9.0] — 2026-09-01

Both flagship claims, made honest. Locus positions itself on "a reliable, ordered changefeed" and on a
geo-first spatial model, and neither held up under a hard look: consumer groups wrote a pending-entries
list they never read back, and the spatial index chose cells so coarse that a large-radius `GEOSEARCH`
degenerated into a full scan. This release closes both. Existing snapshots and masters load unchanged;
nothing about the existing `CDCREADGROUP` / `CDCACK` flow or any `GEOSEARCH` reply changes shape.

### Fixed

- **A dead consumer's in-flight records are no longer lost.** `CDCREADGROUP` added delivered records to
  a group's pending list and `CDCACK` removed them, but nothing in the server could ever redeliver one.
  A consumer that crashed between the read and the ack held its records forever — delivered to nobody,
  acked by nobody — until the `LOCUS_CDC_PEL_MAX` cap silently evicted them. Group delivery was
  effectively **at-most-once** while being documented as reliable.

  Recovery now has two doors, and which one you use depends on who comes back:

  - **The same consumer restarts.** `CDCREADGROUP <group> <consumer> 0` (the `0` sentinel, or
    `FROMPENDING` — mirroring `XREADGROUP … 0`) returns that consumer's own still-pending entries in
    offset order instead of new ones, without moving the group cursor.
  - **A different consumer takes over.** `CDCCLAIM <group> <consumer> <min-idle-ms> <offset…>`
    transfers named pending entries, and `CDCAUTOCLAIM <group> <consumer> <min-idle-ms> <start>
    [COUNT n]` scans and claims in one call, returning `[next-start, [entries…]]` with `next-start = 0`
    once the scan reaches the end.

  Both are gated on `min-idle-ms`, which is the entire safety story: it is what stops a second worker
  taking a record the first is still processing. Set it above your slowest expected processing time.

- **A consumer group no longer vanishes on an unclean stop.** `CDCGROUP CREATE` and `CDCGROUP DESTROY`
  were classed as reads in the command table, so they reached neither the AOF nor the replication link
  and group state was snapshot-durable only. A group created since the last snapshot was simply *gone*
  after a `kill -9` — the next `CDCREADGROUP` answered `-NOGROUP` and that consumer silently stopped
  receiving — and a group created after a replica synced existed only on the master, so a failover
  lost it.

  Those two commands are now logged and replicated, carrying the **resolved** start offset so a replay
  or a replica rebuilds the group at the same cursor origin rather than at "now". Replay is idempotent
  in both directions: re-applying a `CREATE` for a group that already exists keeps the cursor and
  pending list it already has (it never rewinds one), and a `DESTROY` of a group that is not there is a
  no-op, not an error. An `BGREWRITEAOF` re-emits every live group into its base image, so a rewrite
  does not quietly undo this.

  **The read verbs still do not propagate, deliberately.** `CDCREADGROUP`, `CDCACK`, `CDCCLAIM` and
  `CDCAUTOCLAIM` move the cursor and the pending list on *every* group read; logging them would put a
  write in the AOF for each one. Those stay snapshot-durable, so an unclean stop replays a group from
  the last snapshot's position — a bounded **duplicate**, which is what at-least-once permits and what
  a consumer already has to tolerate. A vanished group was not a duplicate, which is why it was the
  half worth fixing.

- **`GEOSEARCH` no longer stalls the hub as the radius grows.** The spatial index picked a single cell
  precision coarse enough that the query box spanned at most four cells — which makes every cell 2–3×
  wider than the query in longitude, so the scan covered 4–9× the query's area (measured at **14.6×**
  for a 1 km box). On dense data at a large radius one cell swallowed the entire dataset and the index
  quietly became a full scan: **181 ms of hub time** for `BYRADIUS 20 km ASC COUNT 10` over 200,000
  points, which on a single-hub design is every client's stall, not just the caller's. A second,
  compounding bug shared one precision between the axes although longitude spans 360° to latitude's
  180°, so every cell came out twice as wide as it was tall.

  Three changes, none of which alter a single reply:

  - **The cover is up to 64 fine cells**, chosen as the finest geohash prefix that fits that budget,
    holding the scanned area to ~1.3× the box. Each extra cell is one `O(log n)` `BTreeMap` seek — far
    cheaper than the points a coarse cell sweeps in — and adjacent cells are merged into single seeks.
  - **Longitude gets the extra bit.** The 52-bit code interleaves longitude into the odd bit positions,
    so an *odd* prefix length gives it one bit more precision than latitude, which is exactly the
    asymmetry the coordinate ranges call for.
  - **`COUNT n` is a nearest-neighbour search.** Every point inside a circle of radius ρ is nearer than
    every point outside it, so an ascending `COUNT n` (the default when `COUNT` is given) probes outward
    — ρ = r/64, then r/8, then the full shape — and stops at the first radius that already holds n
    matches. `COUNT n DESC` asks for the n *farthest* and cannot probe, but now collects through a
    bounded heap instead of sorting every candidate.

  | Query, 200k points over one city | 0.8.0 | 0.9.0 |
  |---|---:|---:|
  | `BYRADIUS 20 km ASC COUNT 10` | 181 ms | **0.082 ms** |
  | `BYRADIUS 1 km ASC COUNT 10` | 1.56 ms | **0.34 ms** |
  | `BYRADIUS 20 km COUNT 1000` | 192 ms | **6.1 ms** |
  | `BYRADIUS 20 km COUNT 10 ANY` | 169 ms | **0.09 ms** |
  | `BYBOX 4×4 km ASC COUNT 10` | 10.0 ms | **0.14 ms** |
  | `BYRADIUS 1 km`, no `COUNT` (261 results) | 2.98 ms | **1.01 ms** |

  A query with **no `COUNT`** still returns every match and so still costs what its own result costs —
  `BYRADIUS 20 km` over that set returns 103,450 members in ~190 ms either way. Bound wide searches with
  `COUNT`. `tests/perf.rs` gained a ratio floor for this: a top-10 at 20 km must not cost dramatically
  more than a top-10 at 1 km, which is precisely the shape of the defect (it measured 133× before).

- **A tall box at a high latitude no longer loses matches along its poleward edge.** The candidate
  bounding box scaled longitude by the *center's* latitude, while the exact filter measures east-west on
  the *point's* own parallel — 22% shorter at the top of a 500 km-tall box at 80°N. The candidate box is
  now scaled by the box's most poleward latitude: never narrower than before, so nothing that used to be
  found can be lost. The old oversized cells happened to mask this; with a tight cover it would have
  become a dropped match.

- **`CDCSUBSCRIBE REGION` snapshots through the spatial index.** The live-geofence snapshot walked every
  geo key in the keyspace on the hub at every subscribe. It now uses the same candidate prefilter
  `GEOSEARCH` does, so subscribing to a neighbourhood costs the neighbourhood: a 1 km region over
  200,000 geo keys went from **141 ms to 3 ms**. Transitions, membership tracking and the snapshot
  contents are unchanged.

### Added

- **`CDCCLAIM`** and **`CDCAUTOCLAIM`** (above). Offsets that are not pending, or not idle long enough,
  are skipped rather than erroring — a claim sweep is a race by nature, and the loser should simply get
  fewer entries back. `CDCAUTOCLAIM` examines at most ten times `COUNT` entries per call and hands back
  a resume cursor: this runs on the hub, and a pending list at `LOCUS_CDC_PEL_MAX` full of not-yet-idle
  entries must not turn one command into a global stall.
- **Pending entries carry metadata.** Each one now records the owning consumer, the last delivery time
  and a delivery count (mirroring Redis's stream PEL) — the data a claim needs to exist at all.
- **`CDCPENDING <group> [COUNT n]`** surfaces it:
  `[total, [[consumer, count], …], [[offset, consumer, idle-ms, delivery-count], …]]`. **The first two
  elements are unchanged** — the per-entry detail is a third element appended, so existing readers keep
  working. It lists the oldest `n` entries (default 10, `COUNT 0` for all); bounded by default because a
  pending list runs to `LOCUS_CDC_PEL_MAX` entries and introspection must not become a stall either. A
  climbing idle time is a dead consumer; a climbing delivery count is a poison record.

### Changed

- **Snapshot/replication trailer format → `LXT3`.** The trailer that carries changefeed and index state
  gained the two new per-pending-entry fields. `LXT2` (0.8.0) and `LXT1` snapshots and full-resync
  payloads **still load**, with the delivery time defaulted to "unknown" — which reads as maximally
  idle, and that is the correct restore semantic: whoever held an entry before a restart is not coming
  back for it, so it is immediately claimable.

  **Upgrade replicas before masters.** A 0.9.0 replica reads a 0.8.0 master's full-resync payload; a
  0.8.0 replica cannot read a 0.9.0 master's, and will refuse it with `bad RDB trailer magic`.
- A pending entry whose record has since aged out of the retained log (`LOCUS_CDC_MAXLEN`) comes back
  from a re-read or a claim as `[offset, nil, nil, nil]`. The payload is genuinely gone and the consumer
  can only ack it — saying so plainly beats dropping the entry and making it look like it was never
  delivered. Size retention above your worst-case consumer downtime.

### BREAKING

- **`CDCGROUP CREATE` / `CDCGROUP DESTROY` now require `@write`** (they were `@read`); grant it to any
  user that provisions changefeed groups. Making them logged and replicated, above, made a command
  classed `@read` mutate durable, replicated state — and concretely, a read-only user could
  `CDCGROUP DESTROY` a group out from under every other consumer of it. Redis classes `XGROUP` the same
  way. **Consuming a group is unchanged and still `@read`**: `CDCREADGROUP`, `CDCACK`, `CDCPENDING`,
  `CDCCLAIM` and `CDCAUTOCLAIM` all stay where they were. The model is now: *consuming* a feed is a
  read, *provisioning* one is a write.

  **Migration** — for each named user that creates or destroys groups:

  ```
  ACL SETUSER <name> +@write        # a consumer that also provisions needs +@read +@write
  ```

### Known limits

- The pending list is still capped at `LOCUS_CDC_PEL_MAX` per group (default 100,000). At the cap the
  **oldest** unacked entries are dropped with a warning in the log: a consumer that never acks degrades
  to at-most-once rather than growing hub memory without bound. Unchanged from 0.8.0.
- **A clustered `GEOSEARCH` does not get the nearest-neighbour probe.** Shards are handed the query
  without its `COUNT` (they return raw hits for the coordinator to merge), so each shard still scans its
  own candidate box. The single-node path and the coordinating node's own hits do use the probe.
- **A group's cursor and pending list are snapshot-durable** (its *existence* is log-durable — see
  above). `CDCREADGROUP`, `CDCACK` and the claim verbs are not written to the AOF and not propagated
  over the replication link, so after a `kill -9` or a failover a group comes back at the position of
  the last snapshot: already-acked records can be redelivered. That is a duplicate, which at-least-once
  permits. Leave `LOCUS_SAVE` at its default cadence to keep the window small.

## [0.8.0] — 2026-09-01

Durability. Four holes between "we have persistence" and "your data is still there afterwards", plus
one honest measurement of the cost we are not paying `fork()` to avoid. Nothing here changes the wire
protocol or the data format in a way that needs a migration — snapshots written by 0.7.0 and earlier
load unchanged.

### Added

- **Automatic snapshot cadence — `LOCUS_SAVE`.** There was none: RDB was manual-only, so with the AOF
  off a crash lost everything written since somebody last typed `SAVE`. Save points are Redis's
  semantics exactly — whitespace-separated `<seconds> <changes>` pairs, and a `BGSAVE` fires as soon as
  **any** pair is satisfied (that many modifications *and* that long since the last save).

  **This is on by default**, at Redis's own default cadence, `3600 1 300 100 60 10000`: hourly if one
  thing changed, every five minutes if a hundred did, every minute if ten thousand did. A database that
  persisted nothing without being asked was the wrong default. Turn it off with `LOCUS_SAVE=""` (or
  `no`) for manual-only snapshots; retune live with `CONFIG SET save "900 1"`; read it back with
  `CONFIG GET save`. New in `INFO`: `rdb_changes_since_last_save`, `rdb_last_save_time`,
  `rdb_save_points`.
- **Snapshot integrity — a CRC-32 footer on every RDB.** A snapshot carried no checksum at all, so
  bit-rot loaded as valid data and a single corrupted length prefix could take the dataset with it.
  Every snapshot now ends in a 10-byte footer (magic + format version + CRC-32 of the whole file),
  written with the same `std`-only code the project's zero-dependency rule requires.

  A snapshot is never torn — it is written temp → `fsync` → atomic rename — so a checksum mismatch is
  not a partial write, it is *damage to a file that was once whole*. Locus therefore **refuses to
  start** on one, naming the file and telling you to restore a backup or move it aside, rather than
  quarantining it and serving an empty keyspace that clients would write into. Every other RDB load
  failure keeps the previous quarantine-and-start-empty behaviour: those could be a foreign file, this
  one cannot.
- **`INFO rdb_last_bgsave_hub_stall_us`** — how long the last `BGSAVE` held the hub. See *Known
  limits* below.
- **`DEBUG AOFFSYNCFAIL <0|1>`** (debug builds only, like `DEBUG PANIC`): makes every AOF `fsync` fail
  as a full disk would, so the durability contract below is tested rather than asserted.
- **`docs/DEPLOYMENT.md` — "Backing up from a replica"**: the measured cost of an on-master snapshot,
  why `fork()` is not the answer here, and a copy-pasteable replica backup + restore-drill procedure.

### Fixed

- **`appendfsync=always` acked writes whose `fsync` had failed.** `Aof::append` called the fsync,
  latched the log unhealthy when it failed — and returned `Ok(())` anyway. The health gate then
  rejected the *next* write, so the one write that actually lost its durability was the one write
  reported as durable, and the client that issued it was never told. That is the exact promise
  `always` exists to make. A failed append or fsync is now returned, and the client gets
  `-MISCONF … The write was applied in memory but is NOT durable`. (The master's replicated stream is
  exempt — a replica must apply its master's stream or diverge.)
- **The `everysec` fsync ran on the hub.** One thread owns the whole keyspace, so `sync_data()` there
  stalled *every client on the server* for as long as the device took — on a busy or degraded disk,
  once a second, for as long as it took. It now runs on a dedicated fsync thread holding its own `dup`
  of the log's descriptor; the hub only posts a request and returns. Requests coalesce, so a device
  slower than a second cannot queue work up, and the once-per-second guarantee and the health tracking
  are unchanged. The shutdown fsync (`SIGTERM`/`SHUTDOWN`) stays synchronous on purpose — the process
  is about to exit, so "asked for" is not good enough.
- **`INFO rdb_last_bgsave_status` was the literal string `ok`**, whatever had happened. It now reports
  the real outcome of the last background save, and a failed save puts its change count back so the
  next save point still fires instead of the changes being silently forgotten.
- **A manual `SAVE` did not reset the change counter**, so the first save point after one fired
  immediately for nothing. (New in this release, but fixed before it shipped.)

### Known limits (stated, not fixed)

- **`BGSAVE` serializes on the hub.** Only the write and `fsync` are off-thread; building the
  point-in-time image is not, and while it runs no client is served — measured at **53 ms** for 400k
  keys / 46 MB and **740 ms** for 1.2M keys / 144 MB (release build, M-series laptop). The number is
  now published as `INFO rdb_last_bgsave_hub_stall_us` so it is an alert, not a surprise.

  **Locus will not `fork()` to fix this.** Redis can because it is single-threaded — its child inherits
  a consistent allocator. Locus runs 2N+ threads, and a forked child that allocates can deadlock on an
  allocator lock held by a thread that did not cross the fork: a rare, unreproducible hang traded for a
  bounded, measurable stall. The supported answer at scale is to snapshot on a replica, which is now
  documented with a procedure. A chunked, copy-on-write-style serializer spread across maintenance
  ticks remains the eventual fix.

## [0.7.0] — 2026-08-27

Safety and performance recovery. Two halves. **Safety:** five defects, every one reproduced against a
running server — one that could take the whole node down, three in the access-control boundary, and an
unauthenticated inter-sentinel control plane that let one TCP line repoint a cluster's replication.
**Performance:** the two defects that accounted for every large gap against Redis — building a
collection was O(n²), and every sorted-set range read cloned the entire set. Collection writes are
28–99× faster and a zset top-10 is 446× faster, both now within a small multiple of Redis.
**Upgrading is recommended for anyone running named ACL users or a sentinel.**

### Security

- **The inter-sentinel control plane is authenticated, and no longer listens to the world.**
  `serve_peers` bound `0.0.0.0:$LOCUS_SENTINEL_PORT` and accepted every verb from any client with no
  credential at all — including `SWITCH <master> <epoch>`, which a sentinel adopts whenever the epoch
  beats its own. One unauthenticated TCP line therefore repointed a whole cluster's replication at a
  machine the attacker controlled, and the data followed. (`LOCUS_SENTINEL_AUTH` is the password
  presented to monitored *data nodes*; it never guarded this listener.)

  Two layers, both on by default:

  - **`LOCUS_SENTINEL_PEER_SECRET`** is required on **every** verb — `SWITCH`, but also `GETMASTER`
    (which hands out the topology) and `ISDOWN` (which feeds another sentinel's failover decision) —
    compared in constant time. Set the same value on every sentinel.
  - **`LOCUS_SENTINEL_PEER_BIND` defaults to `127.0.0.1`**, not `0.0.0.0`. Widen it deliberately, to a
    private address.

  *What the secret does and does not defend against, plainly:* it is a shared bearer token on a
  cleartext line protocol. It stops an unauthenticated stranger from driving the control plane — the
  hole it exists to close — but it is not channel-bound, so a passive on-path attacker can read it and
  replay a verb. Run the peer plane on a trusted network or inside a tunnel.

- **`ACL DELUSER` now revokes a live session instead of promoting it.** Deleting a user — the
  standard response to a leaked credential — left that user's open connections authenticated but
  *unmatched* in the ACL table, and the permission check fell through to the unrestricted `default`
  user. A confined session was therefore *widened* to the whole keyspace by the very act meant to
  end it. `ACL DELUSER` and `ACL SETUSER <name> off` now force-disconnect every connection bound to
  that identity, and the permission check fails closed for anything already in flight: an identity
  that no longer exists gets *no* permissions.
- **Pub/sub channels are now inside the ACL boundary.** Key scoping was never applied to channels, so
  a user confined to `~app:` could `SUBSCRIBE` to — and `PUBLISH` on — any other tenant's channel, and
  read every channel name out of `PUBSUB CHANNELS`. Users now carry channel patterns
  (`&pattern` / `allchannels` / `resetchannels`), enforced on `SUBSCRIBE`, `PSUBSCRIBE`, `PUBLISH`
  (RESP3 push included), with `PUBSUB CHANNELS`/`NUMSUB` filtered to the caller's own scope.
- **`CONFIG GET requirepass` no longer leaks the master password.** It returned the value in
  cleartext to any user, so a key-scoped user could read it, reconnect as `default`, and step
  straight out of its own confinement. `requirepass` and `masterauth` are now masked (name listed,
  value empty) for every user but `default`.
- **`AUTH <password>` now actually switches identity.** It replied `+OK` while leaving the connection
  bound to whatever named user it had — an identity change the client was told had happened but that
  never did. `AUTH <pw>` is `AUTH default <pw>`, in `AUTH` and in `HELLO … AUTH`.
- **`HELLO <proto> AUTH <user> <pass>` authenticates named ACL users.** The HELLO clause was a second,
  narrower copy of `AUTH` that only ever accepted `default`, so a valid named pair came back
  `-WRONGPASS`: a least-privilege user could complete a handshake but could not authenticate through
  one — which is how every modern driver connects. Both verbs now decide identity on a single shared
  path, so they cannot drift apart again.

### Added

- **A panic boundary around the hub.** One thread owns the keyspace, so an unwind out of any command
  used to kill it while the *process stayed alive*: the listener kept accepting and every connection
  was then silently dropped, with no log, no exit, and nothing for a supervisor to restart. Commands
  now run inside `catch_unwind` — the panicking client gets `-ERR internal error`, the panic is logged
  with its command name and source location, and the hub keeps serving. If the hub loop ever unwinds
  outside that boundary, the process aborts rather than lingering as a zombie.
  *Stated tradeoff:* a panic mid-command can leave one value partially mutated. That is strictly
  better than losing everything unpersisted, and unlike an outage it is counted and logged.
- **`INFO` reports `panics_recovered`** — alert on it being non-zero.
- **`INFO` reports `aof_rewrite_in_progress`** (0/1), mirroring `rdb_bgsave_in_progress`, so a rewrite's
  completion is observable rather than something to sleep on and hope for.
- **`DEBUG PANIC`** (debug builds only) so the boundary is testable end to end over the wire. A
  release binary refuses it.
- **`ACL GETUSER` reports a `channels` field** alongside `keys`.
- **Documentation:** `docs/COMMANDS.md` gains the *Access control (ACL)* section that
  `docs/DEPLOYMENT.md` had been linking to.

### BREAKING

- **A sentinel configured with peers but no peer secret refuses to start.** If either
  `LOCUS_SENTINEL_PORT` or `LOCUS_SENTINEL_PEERS` is set and `LOCUS_SENTINEL_PEER_SECRET` is not, the
  process logs the reason and exits non-zero instead of opening an unauthenticated control plane. Fail
  closed, the same stance the ACL took above.

  **Migration** — generate one secret and give it to every sentinel in the group:

  ```
  LOCUS_SENTINEL_PEER_SECRET=$(openssl rand -hex 32)   # same value on every sentinel
  ```

  Peers on other hosts also need `LOCUS_SENTINEL_PEER_BIND=<this sentinel's private address>`, since
  the listener is now loopback-only by default.
- **A named ACL user now starts with NO pub/sub channels**, matching Redis 7's `resetchannels`
  default. Commands, keys, and channels are three independent grants: `allkeys` is not a channel
  grant and neither is `+@all`, because a channel name is not a key name. The implicit `default` user
  is unaffected and keeps all channels.

  **Migration** — for each named user that uses pub/sub, grant the channels it needs:

  ```
  ACL SETUSER <name> '&app:*'      # scoped to one pattern (preferred)
  ACL SETUSER <name> allchannels   # or restore the pre-0.7.0 behaviour wholesale
  ```

  `PSUBSCRIBE` is checked against the **pattern itself, literally** (Redis's rule): a user granted
  `&news.*` may `PSUBSCRIBE news.*` but not `PSUBSCRIBE *`.
- **`ACL SETUSER <name> off` and `ACL DELUSER <name>` now close that user's live connections.** This
  is the point of the fix, but it is a behaviour change for anyone who relied on a disabled user's
  existing sessions continuing to work.

### Documentation — a guarantee we were not keeping

- **Locus's failover is not partition-safe, and the documents now say so.**
  `plans/PRODUCTION-READINESS.md` recorded HA-3 as shipped with "majority + bully-style leader
  election → **no dual promotion**". That claim was false. The majority gate and the leader rule
  narrow the window; they do not close it — an *asymmetric* partition (sentinels that reach each
  other but not the master, while clients still reach it) can still promote twice. HA-3's actual
  content, **fencing a partitioned old master, was never built**: such a master keeps accepting
  writes while cut off and they are silently discarded when it is reconciled back to a replica. Config
  epochs are wall-clock HLC stamps, not coordinated consensus numbers, so clock skew can invert an
  ordering.

  Nothing about the code changed here — only what we claim about it. README, `docs/DEPLOYMENT.md`
  (new *What failover guarantees — and what it does not*), `docs/ARCHITECTURE.md`, `docs/ROADMAP.md`
  and `plans/PRODUCTION-READINESS.md` now state the limits plainly: failover is an **orchestration
  hook for a trusted network** with a bounded, documented data-loss window. If you need
  partition-safety, drive failover from an orchestrator that provides it and use the building blocks
  (`REPLICAOF`, config epochs, `WAIT`, `CLUSTER REASSIGN`) instead of the automatic mode.

### Performance

- **Sorted-set range reads no longer clone the whole set.** `ZRANGE`, `ZRANGEBYSCORE` and the pop and
  remove variants all went through one helper that materialised *every* member — cloning the bytes of
  the entire sorted set — and then took the handful of elements the caller had asked for. Returning
  ten members from a 200k-member zset therefore cost ~34 ms, and because one thread owns the keyspace
  that is a **global** stall: the whole server was capped at ~26 leaderboard queries per second. The
  ordered index that makes this O(log n + m) was already built and maintained on every write; the read
  path simply was not using it. It does now, and the pop paths walk only the elements they remove.

  | Operation | Before | After | Speed-up | Gap to Redis |
  |---|---:|---:|---:|---:|
  | `ZRANGE key 0 9` on a 200k zset | 25.9 ops/s · 38.7 ms | **11,558 ops/s · 0.087 ms** | **446×** | 565× → **1.6×** |
  | `ZRANGEBYSCORE` on a 200k zset | 26.9 ops/s · 37.2 ms | **11,116 ops/s · 0.090 ms** | **413×** | 676× → **1.7×** |

  Replies are byte-for-byte what the old path produced — verified by replaying an 11,658-command
  corpus (every index pair across negative, out-of-range and inverted bounds; `REV`; `WITHSCORES`;
  exclusive and infinite score bounds; `LIMIT`; score ties; empty and single-member sets) against both
  binaries and diffing the raw RESP bytes.

  `ZRANK` is unchanged and still O(rank): `std` has no order-statistic tree, and adding the rank
  bookkeeping to `insert`/`remove` would put the map-and-index-in-lock-step invariant at risk for a
  gain on a cold path.

- **Building a collection is no longer O(n²).** The hub recomputed a key's whole memory-size estimate
  after *every* write, and that estimate walks every element of the value — so each O(1) write into a
  collection was really O(n), and filling one was O(n²). It ran whether or not `maxmemory` was set, so
  every deployment paid for a feature most do not use. A write now marks its key (O(1)) and the deltas
  are folded in on the 100 ms maintenance sweep. Measured on one 8-core machine against
  `redis-server 8.8.0`, at 200k members (500k for the list):

  | Operation | Before | After | Speed-up | Gap to Redis |
  |---|---:|---:|---:|---:|
  | `SADD` into a 200k-member set | 2,590 ops/s | 130,252 ops/s | **50×** | 209× → 4.3× |
  | `ZADD` into a 200k-member zset | 2,118 ops/s | 80,715 ops/s | **38×** | 187× → 4.9× |
  | `HSET` into a 200k-field hash | 1,430 ops/s | 141,191 ops/s | **99×** | 342× → 3.5× |
  | `RPUSH` into a 500k-element list | 1,693 ops/s | 162,305 ops/s | **96×** | 454× → 4.3× |
  | build a 200k-member set from empty | 4,714 ops/s | 131,088 ops/s | **28×** | 128× → 4.6× |
  | build a 500k-element list from empty | 4,058 ops/s | 156,929 ops/s | **39×** | 203× → 5.2× |

  Writing into a large collection now costs the same as writing a fresh key (130,252 vs 132,261 ops/s)
  — per-write work no longer grows with the collection, which is the property the harness asserts on.
  Reproduce it all with `cargo test --release --test perf -- --ignored --nocapture`.

  **Nothing observable was traded for it.** `INFO` settles every pending estimate before reporting, so
  `used_memory` is never stale; and the `maxmemory` gate carries a sound upper bound on the growth it
  has not folded in yet, so the cap holds exactly as before — it settles the estimate before deciding
  whenever `used_memory + bound` would exceed the cap.

### Fixed

- **`AUTH`, `HELLO`, `RESET` and `QUIT` are no longer gated by a user's ACL command classes.** All
  four class as `@connection`, so a least-privilege user — say `+@read ~app:` — was refused every one
  of them: it could not re-authenticate, and because most modern clients open a connection with
  `HELLO`, it could not complete a handshake at all. Redis marks exactly these four `no-auth`; Locus
  now does the same. Only the command-class gate is lifted — key scope, channel scope, and the
  fail-closed check for a deleted or disabled identity are unchanged, and the rest of `@connection`
  (`PING`, `ECHO`, `CLIENT`, `COMMAND`, `SELECT`) stays gated as before.

### Tests

- A regression test per finding in `tests/integration.rs`
  (`a_command_panic_is_contained_and_counted`,
  `acl_deluser_revokes_the_live_session_instead_of_promoting_it`, `acl_scopes_pubsub_channels`,
  `scoped_user_cannot_read_credentials_and_auth_switches_identity`), each confirmed to fail on 0.6.1,
  plus a unit test for the channel-scope rules in `src/acl.rs`.
- **A performance harness, `tests/perf.rs`** — `#[ignore]`d, so it never runs in the normal suite:
  `cargo test --release --test perf -- --ignored --nocapture`. Zero-dep like the rest of the tests, it
  spawns the built binary, drives it over a raw socket, and prints the measured table; where a
  `redis-server` is installed it spawns one too and prints both columns side by side, and where there
  is none it prints the Locus column alone rather than failing. Its assertions are deliberately
  *ratios* — a write into a 200k-element collection must stay within 5x of the same write into an
  empty one — so they catch per-write work that grows with the data instead of flaking on a loaded
  machine. `LOCUS_PERF_N` / `LOCUS_PERF_LIST` shrink the sizes for a quick run.
- Coverage for the zset range readers: `zset_range_readers_match_a_materialised_reference`
  (`src/db.rs`) checks every index pair, score bound and pop count against a brute-force reference
  over distinct-score, tied-score, infinite-score, single-member and empty sets;
  `zset_range_reads_keep_their_exact_replies` (`tests/integration.rs`) pins the command-level replies,
  and passes on 0.6.1 too — which is what makes it a semantics guard rather than a restatement of the
  new code. The harness gained a matching ratio floor (a top-10 from a big zset within 5× of a top-10
  from a small one) that measured 24.4× before the fix and 0.84× after.
- Regression coverage for the deferred memory accounting: `maxmemory_bounds_a_collection_growing_in_place`
  and `used_memory_reports_in_place_growth_without_waiting_for_a_sweep` in `tests/integration.rs`, plus
  `deferred_size_accounting_converges_to_the_eager_total`,
  `drain_dirty_sizes_honors_its_budget_and_finishes_later` and `removing_a_key_retires_its_pending_size`
  in `src/db.rs`.

## [0.6.1] — 2026-07-04

Maintenance release — the org move to **elementaio** plus docs/test polish. No behavior or
wire-protocol changes; the binary is functionally identical to 0.6.0.

### Changed
- **Migrated to the `elementaio` organization**: all repo links, CI/license badges, and the
  Docker image path now point at `github.com/elementaio/locus` and `ghcr.io/elementaio/locus`
  (also published to Docker Hub as `elementaio/locus`).
- **README**: disk tier and work queues added to the feature list, a use-case "recipes" section,
  and refreshed command/line counts.

### Tests
- Made the slow-pub/sub-consumer disconnect test kernel-buffer-proof (it no longer depends on
  socket send-buffer sizing, so it's deterministic across platforms/CI).

## [0.6.0] — 2026-07-03

Broadened general-purpose Redis surface — queues + uniques:

### Added
- **HyperLogLog**: `PFADD` / `PFCOUNT` (multi-key = union) / `PFMERGE`
  (+ internal `PFLOAD` for AOF rewrite). Dense 2^14 one-byte registers
  (16 KB per key, ~0.81% standard error), linear counting on the small range,
  register-wise max merge. Joins the sketch family (Bloom/CMS/TopK/t-digest);
  persists via RDB (tag 13) and AOF; new `hll` TYPE.
- **Blocking list/zset ops** — Locus as a work queue: `BLPOP` `BRPOP`
  `BLMOVE` `BZPOPMIN` `BZPOPMAX`. Fractional-second timeouts (`0` = forever);
  waiters served oldest-first; a served pop propagates to AOF/replicas as the
  same command applied non-blocking (deterministic), so replicas/replay never
  park; inside MULTI/EXEC they never block (immediate value or nil — Redis
  semantics); `INFO blocked_clients` includes parked pops.
- **Parity trio**: `LMPOP` / `ZMPOP` (pop from the first non-empty key,
  `COUNT` supported) and `COPY src dst [DB 0] [REPLACE]` (deep copy including
  TTL).

### Fixed
- An empty (nil) blocking pop is no longer written to the AOF or the
  replication stream (it changed nothing).

## [0.5.1] — 2026-07-03

Stream command parity — two standard Redis features go-redis emits that Locus rejected:

- **`XADD key [MAXLEN [=|~] count] …`** — trims the oldest entries to `count` after appending
  (both markers honored by exact trimming). Bounded streams without a separate `XTRIM` pass.
- **`XRANGE`/`XREVRANGE` `(id` exclusive bounds** — `(N-M` excludes that id, the cursor-paging idiom
  ("everything after this id").

Both surfaced building a bounded, cursor-paged event log on top of Locus; they make real stream
clients work unmodified.

## [0.5.0] — 2026-07-02

**The disk tier: RAM is for LIVE data.** New `TIER key` moves a key's value into a segmented,
append-only value-log on disk, leaving a ~stub in RAM (key + TTL + pointer + type). Reads
transparently **thaw** the value back; `TYPE`/`EXISTS`/TTLs never touch the disk. Segments are
immutable and delete-only — with TTL'd archives (the intended use), same-aged data dies together and
whole segments vanish; no compaction rewrites, so a persisted pointer can never silently move. Every
entry embeds its key, making a stale pointer a *detected*, logged loss (`tier_lost`), never garbage.
Still 100% dependency-free.

- **Semantics:** tiered = archived. A tiered geo key leaves the live spatial index (returns on thaw);
  tiering emits no changefeed event (bytes moved, meaning unchanged); `WATCH`ers are dirtied
  conservatively. `TIER` on a missing key → `:0`; idempotent on a stub.
- **Durability:** the value-log *is* the tiered value's durability (fsync per append). RDB snapshots
  carry stubs (tag 12); AOF logs `TIER` live and folds stubs as `TIERREF` (a local log reference —
  valid forever because segments never move) on rewrite; kill-9 tested for both paths.
- **Replication/cluster:** stubs never cross the wire — full-syncs and slot migrations ship full
  values (read-through); `TIER` replicates as the command, so each node tiers into its own log.
- **Config:** `LOCUS_TIER` (path, or `1` = beside the RDB), `LOCUS_TIER_SEG_MB` (segment size,
  default 512). INFO: `tier_enabled/segments/log_bytes/keys/lost`.
- **Why:** at delivery-scale (e.g. 250k orders/day) a 30-day archive is ~540 GB — that now costs
  NVMe, not RAM. The live working set stays in memory; the server class drops accordingly.

## [0.4.0] — 2026-07-02

The **adversarial-hardening** release. Three independent reviewers read Locus end-to-end; every
finding was fixed under a capability-gated plan (single-node → replication → cluster). The single-node
foundations were already sound — this release closes the resource-exhaustion, role-transition,
failover, cross-shard-merge, and migration edges that a demo and a single node never exercise but a
production cluster does. The default build stays 100% dependency-free. See
`plans/HARDENING-REVIEW-2026-07.md` for the full finding-by-finding ledger.

### Added — resource safety (single-node)
- **Per-client output-buffer limits** — `LOCUS_OUTBUF_NORMAL` / `_REPLICA` (256mb) / `_PUBSUB` (32mb):
  a stalled subscriber/replica is disconnected at its cap instead of growing server memory to OOM.
- **Query-buffer cap** (`LOCUS_QUERYBUF_LIMIT`, default 1gb) and a **resumable parse cursor** — a
  dribbled huge command can't hold unbounded memory pre-`AUTH`, and re-parsing an in-progress command
  is now O(new bytes), not O(N²).
- **Bounded hub input** (`LOCUS_HUB_QUEUE`, default 65536) — a pipelining flood backpressures its own
  reader instead of growing a shared queue without bound.
- **CDC log byte bound** (`LOCUS_CDC_MAXBYTES`, default 64mb) and it now counts toward `used_memory`;
  **consumer-group PEL bound** (`LOCUS_CDC_PEL_MAX`, default 100k).

### Changed — single-node correctness
- **Hub maintenance runs on a wall-clock cadence** — active expiry, `XREAD BLOCK` / `WAIT` deadlines,
  the `everysec` fsync, and `SIGTERM` no longer starve under a sustained command stream.
- **AOF write/fsync errors are surfaced** (`aof_last_write_status`) and, by default
  (`LOCUS_AOF_ON_WRITE_ERROR=stop`), reject writes until a recovery rewrite restores the log — a full
  disk no longer silently ACKs unlogged writes.
- **AOF mid-file corruption refuses to start** (vs. a torn tail, which is still tolerated);
  `LOCUS_AOF_LOAD_TRUNCATED=yes` recovers everything up to the corruption. A corrupt RDB/AOF at boot is
  moved aside, not overwritten. `SET … EX` logs one atomic record; `FLUSH` no longer DEL-storms the AOF.
- **Random expiry sampling and random eviction** (was iteration-order, which leaked whole cohorts);
  **memory estimate** now counts the zset ordered index, geo spatial index, and side-tables.
- **ACL checks every key** a command touches (was the first only — a real `MSET app:x secret:y`
  cross-prefix hole); **changefeed commands are read-class** and prefix-gated (`+@pubsub` no longer
  streams the whole keyspace); **`WAIT`** counts only real replicas' acks (forged/early acks rejected).

### Changed — replication & failover
- **Role transitions are fenced** — the backlog/acks/attached-replicas reset at every boundary, the
  offset is single-counted (a demoted master no longer inflates it), and the **replid rotates on
  promotion** so a stale `PSYNC` full-resyncs instead of continuing a different stream.
- **Replica role + config epoch persist** across restarts (`LOCUS_ROLE_FILE`, `LOCUS_REPLICAOF`) — a
  crashed replica resumes as a read-only replica, and its AOF is rebuilt from the resync snapshot (no
  Frankenstein merge). **Sync-session generations** drop a superseded master's stream.
- **Sentinel config epochs** — a promotion mints an epoch above every known one, data nodes reject a
  stale `REPLICAOF … EPOCH n` (`STALEEPOCH`), and the decision propagates + persists
  (`LOCUS_SENTINEL_STATE`); a restarted sentinel re-derives the master from live `INFO`. A resurrected
  old master can no longer demote the legitimate one. Replicas hide-but-keep expired keys (clock-skew
  divergence fixed).

### Changed — cluster (before you enable it)
- **Cross-shard CDC merge**: node id embedded in the HLC (globally-unique stamps), off-by-one watermark
  closed, truncation reports the last-returned floor, and a dead shard releases the watermark after
  `LOCUS_CDC_PEER_TIMEOUT_MS` (default 30s) — no lost or stalled records.
- **Slot migration is durable, replicated, and crash-safe** — routed through the AOF + replication path,
  fsynced before ownership flips, zombie copies purged (`CLUSTER FLUSHSLOT`), and coherent with the
  changefeed / WATCH / indexes. **Topology persists** (`LOCUS_CLUSTER_STATE`) so a full-cluster restart
  doesn't revert ownership to env.
- **Internal RPCs authenticate** (`LOCUS_CLUSTER_SECRET`) — secure and clustered coexist. A clustered
  `GEOSEARCH` **errors on an unreachable shard** (`LOCUS_CLUSTER_ALLOW_PARTIAL=yes` for best-effort)
  instead of silently returning fewer hits; `GEOSEARCH FROMKEY` is cluster-aware.
- **`GEOSEARCH COUNT n`** returns the n **closest** (add `ANY` for any-n); **`BYBOX`** measures
  east-west at the point's latitude; **`CDCSUBSCRIBE REGION`** rejects NaN/±inf/non-positive radius.

### Note — cross-node pub/sub
- `PUBLISH` / `CDCSUBSCRIBE` deliver **per-node**, not cluster-wide (only `CLUSTER CDCMERGE` is
  cross-shard). This matches Locus's per-region-stack model; see DEPLOYMENT.md §7. A drop-in Redis
  Cluster client expecting broadcast pub/sub should subscribe on the owning node.

## [0.3.0] — 2026-06-27

The production-hardening + clustering release. On top of the reactive/geo core (0.2.0), Locus becomes
safe to operate, durable under crashes, correctly replicated, highly available, TLS-capable, and —
the flagship — **horizontally clustered with spatial locality**. The default build is still 100%
dependency-free; the optional `tls` feature is the only thing that pulls a crate.

### Added — security & access control
- **`AUTH` / `requirepass`** with a constant-time compare, `NOAUTH` gating, and a `HELLO AUTH` clause.
- **Protected mode** (`LOCUS_PROTECTED_MODE`) — refuses non-loopback traffic without a password, closing
  the accidental-exposure hole; **replica `masterauth`** closes the unauthenticated-`PSYNC` siphon.
- **ACL** — users, five command classes (read/write/admin/connection/pubsub) and key-prefix rules,
  layered additively over `requirepass` (vendored SHA-256). `ACL SETUSER/GETUSER/DELUSER/LIST/USERS/
  WHOAMI/CAT`.
- **Connection limits** — `LOCUS_MAXCLIENTS` cap and `LOCUS_TIMEOUT` idle timeout; `TCP_NODELAY`.

### Added — durability
- **Async `BGSAVE` and `BGREWRITEAOF`** — serialize on the hub, write/fsync off-thread, fold in writes
  buffered during the rewrite; the old file is kept on failure.
- **`appendfsync`** (`always`/`everysec`/`no`), directory fsync after rename, surfaced AOF fsync errors,
  and end-to-end `kill -9` crash-recovery tests. CDC + secondary-index state persists in an RDB trailer.

### Added — replication v2
- Stable 40-hex **replid** and a byte-accurate **`master_repl_offset`**; `INFO replication` reports both.
- **`WAIT numreplicas timeout`** with `REPLCONF ACK` and per-replica acked-offset tracking.
- **PSYNC partial-resync** over a 4 MiB backlog ring (`+CONTINUE` when covered, else `+FULLRESYNC`).
- No replica **expiry divergence** — the master streams a `DEL` for every expired key; real
  `master_link_status`.

### Added — high availability
- **Built-in sentinel** (`LOCUS_SENTINEL=…`) — health-checks the master and promotes the most
  up-to-date replica, repointing the rest, with replica-quorum corroboration and anti-split-brain
  reconciliation.
- **Inter-sentinel agreement** — multiple sentinels (`LOCUS_SENTINEL_PEERS`/`_PORT`/`_ID`) require a
  majority to see the master down, and only the elected leader promotes (no dual promotion).
  > **Correction (0.7.0).** "No dual promotion" was overstated: the gate narrows the window, it does
  > not close it, and failover is not partition-safe. See *Documentation — a guarantee we were not
  > keeping* under [0.7.0]. The peer plane described here was also unauthenticated until 0.7.0.

### Added — TLS (optional)
- **In-process TLS** via the opt-in `tls` cargo feature (rustls + ring; no OpenSSL/C). `LOCUS_TLS_PORT`/
  `_CERT`/`_KEY` add a TLS listener alongside plaintext. The **default build stays zero-dependency**;
  a sidecar (ghostunnel/stunnel) remains documented for those who want it.

### Added — compatibility & observability
- **`SCAN`/`HSCAN`/`SSCAN`/`ZSCAN`** (stable cursor, `MATCH`/`COUNT`/`TYPE`/`NOVALUES`), real
  **`CONFIG GET/SET`**, fleshed-out **`INFO`** (works with `redis_exporter`), **`COMMAND`(`/COUNT/DOCS/
  INFO`)**, **`SLOWLOG`**, `OBJECT`, `CLIENT`, `GETEX`.
- **RESP3 typed replies** — maps (`HGETALL`, `CONFIG GET`), sets (`SMEMBERS`, `SINTER`/`SUNION`/`SDIFF`),
  doubles (`ZSCORE`/`ZINCRBY`/`ZMSCORE`), and **pub/sub push frames**.

### Added — geo & data-structure depth
- **Geohash spatial index** — a `BTreeMap` over 52-bit cells makes `GEOSEARCH` sub-linear (was a linear
  scan); **`WHERE field value`** attribute filters; `GEOSET` stores inline attributes.
- **Ordered-index sorted sets** — a `BTreeSet` companion index gives range/rank without re-sorting on read.

### Added — horizontal spatial clustering (the flagship)
- **Hash-slot routing** — CRC16 slots with `{hashtag}`, `MOVED`/`CROSSSLOT`/`CLUSTERDOWN`,
  `CLUSTER SLOTS/SHARDS/NODES/KEYSLOT`.
- **Cell-in-key spatial sharding** — `LOCUS_CLUSTER_CELL_BITS` + `{cell}id` keys (`CLUSTER CELL lon lat`)
  co-locate a region on one shard, so `GEOSEARCH` is a **bounded** cross-shard scatter (only the covering
  shards), not a full fan-out. Cross-shard scatter is parallelized (bounded to ~one peer timeout).
- **Live, zero-loss resharding** — `CLUSTER MIGRATESLOT slot dst` (two-phase copy-then-commit),
  `CLUSTER SETSLOT slot NODE addr`; changes are HLC-epoch-stamped and **converge via anti-entropy gossip**
  (`LOCUS_CLUSTER_GOSSIP_MS`) without pushing to every node.
- **Per-shard failover** — the sentinel (`LOCUS_SENTINEL_CLUSTER_NODES`) broadcasts `CLUSTER REASSIGN`
  after promotion so a dead master's slots follow its replica.
- **Global changefeed** — every change carries a **hybrid logical clock** (persisted across restarts);
  `CLUSTER CDCMERGE since-hlc` merges all shards' feeds in HLC order up to a watermark that bounds
  staleness (and holds for a downed shard).

### Changed
- Crate description and version reflect the reactive/geo/clustered scope. `~14k` lines of std-only Rust
  across 15 modules. CI now also lints and tests the `tls` feature.

## [0.2.0] — 2026-06-16

The reactive + geo-first release. On top of the Redis-compatible core (0.1.0), Locus gains its
differentiator layer — a reliable changefeed, a geo-first spatial model with live geofencing,
mergeable probabilistic sketches, conditional-write verbs, and an auto-maintained secondary index —
plus transaction-correctness fixes and `maxmemory` eviction. Still pre-1.0 and not production-hardened
(no AUTH/TLS; bind to a trusted network).

### Added (distribution)
- **`LOCUS_BIND`** — configurable listen interface (default `127.0.0.1`, so Locus isn't exposed by
  accident given it has no AUTH/TLS). An official **Docker image** (`ghcr.io/elementaio/locus`, sets
  `LOCUS_BIND=0.0.0.0`) and **prebuilt static binaries** (Linux/macOS, x86_64/arm64) are now published
  per release.

### Added (sketches — mergeable probabilistic summaries)
- **Bloom filter** `BFADD` / `BFEXISTS` (+ internal `BFLOAD` for AOF rewrite/replication) — dedup /
  set membership ("seen this id?"). Zero-deps (std `DefaultHasher` + double hashing), auto-sized, RDB/AOF
  persistent. First of the a-la-carte sketch family.
- **Count-Min sketch** `CMSINCRBY` / `CMSQUERY` (+ internal `CMSLOAD`) — frequency estimation
  ("trending now"); over-estimates, never under. Auto-sized (2000×5), RDB/AOF persistent.
- **Top-K sketch** `TOPKRESERVE` / `TOPKADD` / `TOPKLIST` / `TOPKCOUNT` (+ internal `TOPKLOAD`) —
  heavy hitters on top of Count-Min + a k-slot leaderboard; RDB/AOF persistent (opaque blob).
- **t-digest** `TDADD` / `TDQUANTILE` (+ internal `TDLOAD`) — streaming quantiles / percentiles
  (live p99), accurate at the tails via the `q(1-q)` scale; exact min/max. Completes the sketch family.

### Added (secondary index — query by field)
- **`IDXCREATE` / `IDXDROP` / `IDXGET` / `IDXRANGE`** — index a hash field for equality and
  lexicographic-range queries. Auto-maintained on every write/expiry/eviction in the same hub turn, so
  the index never drifts from the data (the single-threaded guarantee). In-memory; equality + range +
  COUNT (no query language — by design).

### Added (conditional writes — the CAS primitive)
- **CAS family** `CAS key expected new`, `CADEL key expected`, `SETMAX key n` (monotonic cursor),
  `INCRCAP key delta cap` (quota). Atomic check-and-write under single-threaded execution — no WATCH/Lua.
  Logged to the AOF as their concrete effect (`SET`/`DEL`) so replay/replication stay deterministic.

### Added (geo — the geo-first differentiator)
- **Live geofencing** — `CDCSUBSCRIBE REGION <lon> <lat> <radius> <unit>`: an atomic snapshot of the geo
  keys inside the circle, then a live stream as keys **enter/move** (`write`) and **leave** (`del` — on
  move-out, delete, or expire). The geo index + changefeed converge: a *region* filter on the per-key
  feed *is* geofencing. Per-subscriber membership tracking gives proper enter/leave transitions.
- **Geo commands** `GEOSET`, `GEOPOS`, `GEODIST`, `GEOSEARCH` (`BYRADIUS`/`BYBOX`, `FROMLONLAT`/`FROMKEY`,
  `ASC`/`DESC`, `COUNT`, `WITHCOORD`/`WITHDIST`). Geo-first model: each object is its own key
  (`Value::Geo`), with a geo-key index for search and full RDB/AOF persistence. Haversine distance.
  (Next: live region geofencing over the changefeed; a real S2/R-tree index with combined filters.)

### Added (changefeed — the reactive differentiator)
- **`CDCSUBSCRIBE [prefix]` / `CDCUNSUBSCRIBE`** — a reliable, ordered keyspace changefeed: an atomic
  snapshot of matching keys followed by a live stream of every change (`write`/`del`/`expire`), with
  no gap or duplication (guaranteed by single-threaded execution). Values are inlined for string keys.
  Fed from the same modification choke points as WATCH/AOF/replication, so it never misses a write and
  never reports a no-op. The foundation for live-query and geofencing.
- **Changefeed consumer groups** — `CDCGROUP CREATE|DESTROY`, `CDCREADGROUP <group> <consumer>`
  (load-balanced: each record delivered to one consumer), `CDCACK`, `CDCPENDING`. In-memory; built on
  the retained log/offsets. The second of the change-log's two read modes (broadcast + load-balanced).
- **Changefeed offsets + retention + `CDCREAD`** — every change carries a monotonic offset;
  `CDCREAD <offset> [COUNT n] [PREFIX p]` pulls retained changes after an offset for reconnect catch-up.
  Retention is opt-in via `LOCUS_CDC_MAXLEN` (a ring buffer); falling behind the retained window returns
  `offset out of range`. `CDCSUBSCRIBE`'s `snapshot-done` now reports the high-water offset, and live
  `cdc-change` messages now include their offset.

### Added (commands)
- String commands: `MGET`, `MSET`, `MSETNX`, `SETNX`, `SETEX`, `PSETEX`, `GETSET`, `GETRANGE`,
  `SETRANGE`, `INCRBYFLOAT`.
- Keyspace commands: `KEYS`, `DBSIZE`, `RENAME`, `RENAMENX`, `TOUCH`, `UNLINK`, `FLUSHDB`, `FLUSHALL`.
- List commands: `LINSERT`, `LREM`, `LTRIM`, `LPOS`, `RPOPLPUSH`, `LMOVE`.
- Set commands: `SMOVE`, `SINTERSTORE`, `SUNIONSTORE`, `SDIFFSTORE`, `SINTERCARD`.
- Sorted-set commands: `ZREMRANGEBYRANK`, `ZREMRANGEBYSCORE`, `ZUNIONSTORE`, `ZINTERSTORE`
  (with `WEIGHTS`/`AGGREGATE`; set sources score 1.0).
- Bitmap commands: `SETBIT`, `GETBIT`, `BITCOUNT` (incl. `BYTE`/`BIT` ranges), `BITPOS`, `BITOP`.
- Randomized commands: `SRANDMEMBER` (negative count = with repeats), `RANDOMKEY`, backed by a small
  zero-deps xorshift PRNG. `SPOP` now selects truly random members (was arbitrary iteration order).

### Fixed
- WATCH now dirties **all** keys touched by multi-key writes (`MSET`/`MSETNX`/`RENAME`) and by
  `FLUSHDB`/`FLUSHALL`, not just the first key.

### Changed (internal)
- Consolidated command metadata (existence, minimum arity, write-or-read) into a single
  `commands::command_meta` table — the one source of truth. `aof::is_write` now delegates to it,
  removing the separate hand-maintained write allowlist that could silently drift (a forgotten entry
  meant a write that wasn't persisted or replicated). A regression-lock test pins the write set.

### Added
- **`maxmemory` + eviction** (`LOCUS_MAXMEMORY`, accepts `kb`/`mb`/`gb` suffixes). Approximate memory
  accounting bounds dataset growth; when over the cap a master evicts arbitrary keys (streamed to
  replicas/AOF as `DEL`) and rejects a write with `OOM` only if the cap still can't be met. Replicas
  don't self-evict — the master drives deletions. `INFO` now reports a `# Memory` section
  (`used_memory`, `maxmemory`).

### Added
- `SELECT` — single logical DB: `SELECT 0` returns OK (so clients that select on connect work);
  other indexes are rejected. Full multi-DB is a deliberate non-goal.

### Fixed (replication)
- The replica handshake now uses a read timeout, so a master that accepts the TCP connection but
  never replies can no longer hang the replication thread (and `REPLICAOF NO ONE` can take effect).

### Fixed (transactions)
- **`WATCH` now aborts `EXEC` when a watched key expires** (passive or active reaper), not only on an
  explicit write — matching Redis optimistic-concurrency semantics.
- **`MULTI` validates commands at queue time**: an unknown command or one with too few arguments now
  flags the transaction so `EXEC` returns `EXECABORT` instead of running a half-valid batch.
- **No-op writes no longer abort `WATCH`** (and are no longer logged to the AOF or replicated): e.g.
  `DEL` of a missing key or `SADD` of an existing member no longer spuriously dirties a transaction.

### Fixed
- **TTL integer overflow** in `EXPIRE`/`PEXPIRE`/`EXPIREAT`/`PEXPIREAT` and `SET … EX/PX/EXAT/PXAT`:
  very large TTLs now error cleanly instead of panicking (debug) or wrapping to a past deadline and
  silently deleting the key (release).
- **`ZADD GT`/`LT`** now gate score updates (and `INCR`) correctly instead of being silently ignored;
  incompatible flag combinations (`GT`+`LT`, `NX`+`GT`/`LT`) are rejected.

### Testing
- Added an end-to-end integration harness (`tests/integration.rs`) that spawns the real server and
  drives it over TCP: pipelining, MULTI/EXEC, EXECABORT, WATCH (change + expiry), no-op-WATCH,
  pub/sub, blocking `XREAD`, and a replication round-trip.

### Added
- **`RESET`** command — aborts `MULTI`, releases `WATCH`es, exits subscribe mode, drops to RESP2.

### Security / hardening
- RESP parser bounds untrusted input: capped eager pre-allocation for large `*N` array headers, and a
  64 KiB limit on un-terminated inline requests (prevents per-connection unbounded buffer growth).

### Fixed (replication)
- A replica that just loaded a full-sync snapshot now re-evaluates clients parked on blocking `XREAD`.

## [0.1.0] — 2026-06-16

Initial release. Built in twelve incremental milestones (M0–M12); the git history has one commit per
milestone. Zero third-party dependencies (pure `std`).

### Added
- **Data types:** strings, lists, hashes, sets, sorted sets, streams (with `WRONGTYPE` checks).
- **Key expiry:** `SET EX/PX/EXAT/PXAT/NX/XX/KEEPTTL`, `EXPIRE`/`PEXPIRE`/`EXPIREAT`/`PEXPIREAT`,
  `TTL`/`PTTL`, `PERSIST` — passive (on-access) and active (background sampling).
- **Persistence:** RDB-style binary snapshots (`SAVE`/`BGSAVE`, temp→fsync→rename) and an append-only
  file (AOF) with crash-safe, torn-tail-tolerant replay, deterministic command rewriting, and
  `BGREWRITEAOF` compaction.
- **Replication:** `REPLICAOF` master/replica — full-sync snapshot transfer + live command streaming,
  read-only replicas, `INFO replication`.
- **Pub/Sub:** `SUBSCRIBE`/`UNSUBSCRIBE`/`PSUBSCRIBE`/`PUNSUBSCRIBE`/`PUBLISH`/`PUBSUB` with glob patterns.
- **Transactions:** `MULTI`/`EXEC`/`DISCARD` and `WATCH`/`UNWATCH` optimistic locking.
- **Streams:** `XADD`/`XLEN`/`XRANGE`/`XREVRANGE`/`XREAD`, including blocking `XREAD`.
- **Protocol:** RESP2 + `HELLO` RESP3 negotiation; pipelining.

### Known limitations / deferred
- Streams consumer groups; PSYNC partial resync, replication backlog, `WAIT`, automatic failover;
  a skiplist for O(log n) sorted-set ops; full RESP3 typing of every reply; thread-per-core execution.
- No authentication or TLS yet — bind to a trusted network only.

[Unreleased]: https://github.com/elementaio/locus/compare/v0.9.0...HEAD
[0.9.0]: https://github.com/elementaio/locus/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/elementaio/locus/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/elementaio/locus/compare/v0.6.1...v0.7.0
[0.6.1]: https://github.com/elementaio/locus/compare/v0.6.0...v0.6.1
[0.6.0]: https://github.com/elementaio/locus/compare/v0.5.1...v0.6.0
[0.5.1]: https://github.com/elementaio/locus/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/elementaio/locus/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/elementaio/locus/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/elementaio/locus/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/elementaio/locus/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/elementaio/locus/releases/tag/v0.1.0
