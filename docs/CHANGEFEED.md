# Changefeed — the reactive primitive

The changefeed is Locus's reliable, ordered, data-aware alternative to Redis keyspace notifications.
You subscribe to a slice of the keyspace and get an **atomic snapshot** of what's there now, then a
**live stream** of every change — with **no gap and no duplication**, guaranteed by single-threaded
execution.

It is the keystone the live-query and geofencing features build on. Think of it as *pub/sub, but for
data*: you don't publish to it — the keyspace itself feeds it.

| | Classic Pub/Sub | Changefeed |
|---|---|---|
| Subscribe to | a channel name | a set of data (key prefix → geo region) |
| Produced by | a client `PUBLISH` | the keyspace itself, on every write |
| Delivery | fire-and-forget, lossy | ordered, no gap / no dup |
| History | none | snapshot of current state, then deltas |
| Payload | whatever was published | the changed key + value, with an offset |

## Three read modes

### 1. Broadcast (push) — `CDCSUBSCRIBE`

```
CDCSUBSCRIBE [prefix]
```
The connection enters push mode (like `SUBSCRIBE`). You receive:

- a snapshot, one message per matching key: `["cdc-snapshot", <key>, <value>]`
- a completion marker: `["cdc-snapshot-done", <count>, <high-water-offset>]`
- then, live, for every change: `["cdc-change", <offset>, <write|del|expire>, <key>, <value>]`

`CDCUNSUBSCRIBE` leaves push mode. Values are inlined for string keys; for other types the event
signals the key changed and the client re-fetches.

Because the subscriber is registered **and** the snapshot taken in the same hub turn, no write can slip
between them — the snapshot-then-tail is gap-free and dup-free without any offsets.

```console
redis-cli SET user:1 alice
redis-cli CDCSUBSCRIBE user:        # prints the snapshot, then blocks for changes
# elsewhere:
redis-cli SET user:2 bob            # subscriber prints: cdc-change <off> write user:2 bob
redis-cli DEL user:1                # subscriber prints: cdc-change <off> del   user:1
```

### 2. Pull / catch-up — `CDCREAD`

```
CDCREAD <offset> [COUNT n] [PREFIX p]
```
Every change carries a **monotonic offset**. With retention enabled (`LOCUS_CDC_MAXLEN=<records>`),
recent changes are kept in a ring buffer; `CDCREAD` returns the changes after a given offset, so a
consumer that disconnected can resume from its last-seen offset:

1. on reconnect, `CDCREAD <last-offset>` to fill the gap, then
2. `CDCSUBSCRIBE` again for the live tail.

If the requested offset is older than the oldest retained record, `CDCREAD` returns
`offset out of range` — the signal to re-snapshot. Each entry is `[offset, event, key, value]`.

### 3. Consumer groups (load-balanced, at-least-once)

```
CDCGROUP CREATE <group> [offset|$|0]   # $ / default = only new; 0 = all retained
CDCGROUP DESTROY <group>
CDCREADGROUP <group> <consumer> [0|FROMPENDING] [COUNT n]
CDCACK <group> <offset> [offset ...]
CDCPENDING <group> [COUNT n]
CDCCLAIM <group> <consumer> <min-idle-ms> <offset> [offset ...]
CDCAUTOCLAIM <group> <consumer> <min-idle-ms> <start> [COUNT n]
```
A group is a shared cursor over the log plus a **pending-entries list** (the PEL). `CDCREADGROUP` hands
the next un-delivered records to the calling consumer — **disjoint** across the group, so N workers share
the feed. Delivered records stay pending until `CDCACK`ed. (Built on retention; requires
`LOCUS_CDC_MAXLEN`.)

Each pending entry records **who** holds it, **when** it was last delivered, and **how many times** it
has been delivered. That is what makes the next two paragraphs possible — and it is what "at-least-once"
actually costs. (Before 0.9.0 the pending list was recorded and never redelivered: a consumer that died
mid-processing took its in-flight records with it. See the [CHANGELOG](../CHANGELOG.md).)

**A restarted consumer recovers its own work.** `CDCREADGROUP <group> <consumer> 0` (the `0` sentinel, or
`FROMPENDING`; mirrors `XREADGROUP … 0`) returns that consumer's still-pending entries in offset order
instead of new ones. The group cursor does not move; each re-read bumps the entry's delivery count and
restarts its idle clock. This is the recovery path for a worker that crashed and came back **under the
same name**.

**A live worker takes over a dead one's work.** `CDCCLAIM` transfers the named pending entries to a new
consumer, but only those idle at least `min-idle-ms`; `CDCAUTOCLAIM` scans the PEL from `start` and
claims the first `COUNT` (default 100) idle entries in one call, returning `[next-start, [entries…]]` —
`next-start` is `0` when the scan reached the end, so a recovery sweep is a loop that ends on 0. Both
reset the idle clock and bump the delivery count.

The `min-idle-ms` guard is the whole safety story: it is what stops two workers processing the same
in-flight record. Set it comfortably above your slowest expected processing time. An offset that is not
pending, or not idle long enough, is skipped rather than erroring — a claim sweep is a race by nature,
and the loser should just get fewer entries back.

```console
# worker-1 read offsets 1 and 2, then died holding them.
redis-cli CDCPENDING workers
1) (integer) 2
2) 1) 1) "worker-1"
      2) (integer) 2
3) 1) 1) (integer) 1
      2) "worker-1"
      3) (integer) 94213     # idle ms — worker-1 has been gone 94 seconds
      4) (integer) 1         # delivered once
   ...
redis-cli CDCAUTOCLAIM workers worker-2 60000 0      # take everything idle > 60s
redis-cli CDCACK workers 1 2                         # worker-2 processed them
```

`CDCPENDING` returns `[total, [[consumer, count], …], [[offset, consumer, idle-ms, delivery-count], …]]`.
The first two elements are unchanged from 0.8.0. The third lists the **oldest `COUNT` entries** (default
10, `COUNT 0` for all) — bounded by default because a pending list runs to `LOCUS_CDC_PEL_MAX` entries and
an introspection command must not become a hub stall. A climbing idle time means a dead consumer; a
climbing delivery count means a poison record that keeps being redelivered and never acked.

**Two honest limits.** The PEL is capped at `LOCUS_CDC_PEL_MAX` entries per group; at the cap the *oldest*
unacked entries are dropped with a warning in the log, so a consumer that never acks degrades to
at-most-once rather than growing hub memory without bound. And a pending entry whose record has aged out
of the retained log (`LOCUS_CDC_MAXLEN`) comes back from a re-read or a claim as `[offset, nil, nil, nil]`
— the payload is genuinely gone, and saying so beats dropping the entry and making it look like it was
never delivered. Size retention above your worst-case consumer downtime.

**Where group state is durable.** Two different guarantees, deliberately:

- **A group's existence is log-durable and replicated.** `CDCGROUP CREATE` and `CDCGROUP DESTROY` are
  written to the AOF and streamed to replicas (with the resolved start offset, so a replay or a replica
  rebuilds the group at the same cursor origin, never at "now"). A group survives a `kill -9` and is
  present on a replica after a failover. Replay is idempotent: re-applying a `CREATE` for a group that
  already exists keeps the cursor and pending list it already has, and destroying a group that is not
  there is a no-op.
- **A group's cursor and pending list are snapshot-durable.** They ride in the snapshot trailer and the
  full-resync payload, so they survive a graceful restart (`SAVE` and `SHUTDOWN` both write them) and
  are handed to a replica when it syncs — but `CDCREADGROUP`, `CDCACK`, `CDCCLAIM` and `CDCAUTOCLAIM`
  are **not** logged or replicated, on purpose. They change on every group read, and logging them would
  put a write in the AOF for each one. So after a `kill -9` or a failover, the *position* is only as
  fresh as the last snapshot: already-acked records can come back. That is a duplicate, which is
  exactly what at-least-once permits and what your consumer must already tolerate — unlike a vanished
  group, which was a silent stop and is now fixed.

Leave `LOCUS_SAVE` at its default cadence to keep that duplicate window small.

Entries restored from a snapshot count as maximally idle — whoever held them before the restart is not
coming back for them — so they are claimable immediately.

## Geofencing — `CDCSUBSCRIBE REGION`

A *region* filter instead of a *prefix* filter turns the changefeed into live geofencing (see
[GEO.md](GEO.md)):
```
CDCSUBSCRIBE REGION <lon> <lat> <radius> <unit>
```
Snapshot of the geo keys inside the circle, then `cdc-change write` as keys **enter/move** and
`cdc-change del` as they **leave** (move out, are deleted, or expire). Each region subscriber tracks its
own membership so the enter/leave transitions are exact.

The snapshot takes the spatial index, not the geo keyspace: since v0.9.0 it uses the same candidate
prefilter as `GEOSEARCH`, so subscribing to a neighbourhood costs the neighbourhood, not the dataset
(a 1 km region over 200 000 geo keys: **141 ms → 3 ms**).

## How it stays correct

Every keyspace mutation funnels through the hub's `record_change`, fed from the **same modification
choke points** as WATCH / AOF / replication (writes, expiry, eviction). So the feed:

- never misses a real write, and never fires on a no-op (a `DEL` of a missing key emits nothing);
- is totally ordered (single thread assigns offsets);
- costs nothing when unused (no subscribers and `LOCUS_CDC_MAXLEN=0` → the hook returns immediately).

## Cross-shard (clustered) — `CLUSTER CDCMERGE`

In a cluster each shard has its own ordered feed. To get **one global feed**, every change is also stamped
with a **hybrid logical clock** (HLC: wall-clock ms in the high bits, a logical counter in the low bits, so
the `u64` sorts as `(physical, logical)` and stays close to real time). `CLUSTER CDCMERGE <since-hlc>
[COUNT n]` — sent to any node — gathers that node's changes plus every peer's (since `since-hlc`) and
returns `[hlc, event, key, value]` in **global HLC order**:

```
CLUSTER CDCMERGE 0 COUNT 100     # from the start
CLUSTER CDCMERGE 7493020168192   # continue past the last hlc you saw
```

It only emits changes at or below a **watermark** — the minimum HLC floor across reachable shards — so a
later read can never surface an earlier-stamped change (bounded staleness; an idle shard still advances its
floor to the wall clock, so it doesn't stall the merge). Each shard keeps its own total order; the merge
adds the HLC-monotone global order. Retention (`LOCUS_CDC_MAXLEN`) must be on. (HLC stamps are in-memory:
records reloaded from a snapshot sort before live ones until re-stamped.)

## Configuration

| Variable | Meaning |
|---|---|
| `LOCUS_CDC_MAXLEN` | retained change-log size (records) for `CDCREAD` / consumer groups / `CLUSTER CDCMERGE`; `0`/unset = off (push still works) |
| `LOCUS_CDC_MAXBYTES` | byte cap on the retained change-log (default `64mb`; counts toward `used_memory`) |
| `LOCUS_CDC_PEL_MAX` | per-group pending-entries cap (default `100000`; at the cap the oldest unacked entries are dropped with a warning) |

## Not goals

No server-side transforms beyond prefix/region filtering, and no run-code-on-write triggers — arbitrary
logic would stall the single thread. React in the client over the feed.
