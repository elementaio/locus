# locusdb

The Node.js client for [Locus](https://github.com/elementaio/locus). Any Redis
driver works against Locus for standard commands — this package adds the two
things a plain driver can't give you ergonomically:

1. **Typed helpers** for the differentiator verbs (geo, sketches, CAS, secondary
   index, cross-shard changefeed) so you get types and autocomplete instead of
   stringly-typed `redis.call(...)`.
2. **The reactive API** — a live **changefeed** and **geofence** delivered as
   events. Locus streams these as a custom push protocol that `ioredis` doesn't
   model, so the client owns a dedicated connection and parses the frames for you.

It wraps `ioredis` (exposed as `.redis`) — it does not reimplement RESP.

## Install

```bash
npm install locusdb ioredis
```

## Use

```ts
import { LocusClient } from "locusdb";

const locus = new LocusClient({ host: "127.0.0.1", port: 6379 });

// Standard Redis: the raw ioredis connection is right there.
await locus.redis.set("hello", "world");

// Differentiator verbs, typed:
await locus.geoSet("driver:7", 13.36, 38.11, { status: "free" });
const hits = await locus.geoSearch({
  fromLonLat: [13.4, 38.1],
  byRadius: [50, "km"],
  withDist: true,
  where: { status: "free" }, // attribute filter
});
// hits -> [{ key: "driver:7", dist: 3.67 }]

await locus.bfAdd("seen", "msg-42");        // 1 new, 0 probably-duplicate
await locus.cas("flag", "old", "new");      // atomic check-and-set
await locus.idxCreate("by_status", "status");
await locus.idxGet("by_status", "paid");    // -> ["order:1"]
```

### Live changefeed (push)

```ts
const feed = locus.changefeed("user:");      // optional key prefix
feed.on("ready", ({ count, offset }) => console.log("snapshot done", count));
feed.on("change", (c) => console.log(c.op, c.key, c.value)); // write | del | expire
// ...later
feed.close();
```

### Live geofencing

```ts
const fence = locus.geofence(13.4, 38.1, 5, "km");
fence.on("enter", (m) => console.log("entered", m.key, m.value)); // "lon,lat"
fence.on("move",  (m) => console.log("moved",   m.key, m.value));
fence.on("leave", (m) => console.log("left",    m.key));
```

Both run on their own connection; the snapshot (`"snapshot"` events, then
`"ready"`) precedes live updates, so there's no gap or duplicate.

### Cross-shard changefeed (clustered)

```ts
let since = 0;
for (;;) {
  const batch = await locus.clusterCdcMerge(since, 100); // global HLC order
  for (const c of batch) { handle(c); since = c.hlc; }
  if (!batch.length) await new Promise((r) => setTimeout(r, 250));
}
```

## Notes

- Pull-style changefeed (`cdcRead`) and consumer groups work over the normal
  connection and need server retention (`LOCUS_CDC_MAXLEN`). The push API above
  does not.
- For TLS, pass ioredis's `tls` option; the subscription connection honors it too.
- See the [changefeed](../../docs/CHANGEFEED.md) and [geo](../../docs/GEO.md) docs
  for semantics, and [COMMANDS.md](../../docs/COMMANDS.md) for the full surface.

Run the example against a local server: `node examples/quickstart.cjs` (after
`npm run build`).
