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

## Two read modes

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

### 3. Consumer groups (load-balanced)

```
CDCGROUP CREATE <group> [offset|$|0]   # $ / default = only new; 0 = all retained
CDCGROUP DESTROY <group>
CDCREADGROUP <group> <consumer> [COUNT n]
CDCACK <group> <offset> [offset ...]
CDCPENDING <group>                     # -> [total, [[consumer, count], ...]]
```
A group is a shared cursor over the log plus a pending list. `CDCREADGROUP` hands the next un-delivered
records to the calling consumer — **disjoint** across the group, so N workers share the feed. Delivered
records are pending until `CDCACK`ed. (Built on retention; requires `LOCUS_CDC_MAXLEN`.)

## Geofencing — `CDCSUBSCRIBE REGION`

A *region* filter instead of a *prefix* filter turns the changefeed into live geofencing (see
[GEO.md](GEO.md)):
```
CDCSUBSCRIBE REGION <lon> <lat> <radius> <unit>
```
Snapshot of the geo keys inside the circle, then `cdc-change write` as keys **enter/move** and
`cdc-change del` as they **leave** (move out, are deleted, or expire). Each region subscriber tracks its
own membership so the enter/leave transitions are exact.

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

## Not goals

No server-side transforms beyond prefix/region filtering, and no run-code-on-write triggers — arbitrary
logic would stall the single thread. React in the client over the feed.
