# Clients — using Locus from your app

Locus speaks the Redis **RESP** protocol, so **any Redis client works** — there is no Locus-specific
SDK to install. Point your existing client at Locus's host/port and every standard command behaves as
it does against Redis. The differentiator commands (changefeed, geo, sketches, CAS, secondary index)
aren't part of Redis, but every client can send them through its generic *raw command* API.

Examples below use Node ([ioredis](https://github.com/redis/ioredis)) and Python
([redis-py](https://github.com/redis/redis-py)); the same pattern applies to go-redis, Jedis/Lettuce, etc.

## Connect

```js
// Node
import Redis from 'ioredis';
const redis = new Redis({ host: '127.0.0.1', port: 6379 });
await redis.set('hello', 'world');
console.log(await redis.get('hello')); // "world"
```

```python
# Python
import redis
r = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True)
r.set('hello', 'world')
print(r.get('hello'))  # "world"
```

Standard data types, expiry, transactions, classic pub/sub, streams, and replication all work unchanged.

## Custom commands (the raw command API)

No special binding is needed — send the differentiator verbs as raw commands:

| Client | Send an arbitrary command |
|---|---|
| ioredis (Node) | `redis.call('GEOSET', 'p', '13.36', '38.11')` |
| node-redis (Node) | `client.sendCommand(['GEOSET', 'p', '13.36', '38.11'])` |
| redis-py (Python) | `r.execute_command('GEOSET', 'p', 13.36, 38.11)` |
| go-redis (Go) | `rdb.Do(ctx, "GEOSET", "p", 13.36, 38.11)` |

### Geo

```js
await redis.call('GEOSET', 'driver:7', '13.36', '38.11');
await redis.call('GEOSEARCH', 'FROMLONLAT', '13.4', '38.1', 'BYRADIUS', '50', 'km', 'ASC', 'WITHDIST');
```
```python
r.execute_command('GEOSET', 'driver:7', 13.36, 38.11)
r.execute_command('GEOSEARCH', 'FROMLONLAT', 13.4, 38.1, 'BYRADIUS', 50, 'km', 'ASC', 'WITHDIST')
```

### Sketches

```js
await redis.call('BFADD', 'seen', 'msg-42');        // 1 = new, 0 = probably duplicate
await redis.call('CMSINCRBY', 'trend', 'rust', '5');
await redis.call('TDADD', 'lat', '12', '18', '25');
await redis.call('TDQUANTILE', 'lat', '0.5', '0.99');
```

### Conditional writes (CAS)

```python
r.execute_command('CAS', 'flag', 'old', 'new')    # 1 if swapped, 0 otherwise
r.execute_command('SETMAX', 'cursor', 100)         # monotonic: store iff greater
r.execute_command('INCRCAP', 'quota', 1, 1000)     # increment unless it would exceed the cap
```

### Secondary index

```js
await redis.call('IDXCREATE', 'by_status', 'status');   // index the hash field "status"
await redis.call('HSET', 'order:1', 'status', 'paid');
await redis.call('IDXGET', 'by_status', 'paid');        // -> ['order:1']
await redis.call('IDXRANGE', 'by_status', 'a', 'z', 'COUNT', '100');
```

## Changefeed — two ways to consume

The [changefeed](CHANGEFEED.md) has a **pull** model (plain request/reply — easy with any client) and a
**push** model (a streaming connection — best with a dedicated socket).

### Pull (recommended for standard clients)

`CDCREAD` returns the changes after an offset, so you poll from your last-seen offset. Works with any
client. Requires retention on the server (`LOCUS_CDC_MAXLEN=<records>`).

```js
// Node — poll loop
let offset = 0;
for (;;) {
  const batch = await redis.call('CDCREAD', String(offset), 'COUNT', '100', 'PREFIX', 'user:');
  for (const [off, event, key, value] of batch) {
    handle(event, key, value);          // event: write | del | expire
    offset = Number(off) + 1;
  }
  if (batch.length === 0) await new Promise(r => setTimeout(r, 250));
}
```
```python
# Python — poll loop
offset = 0
while True:
    batch = r.execute_command('CDCREAD', offset, 'COUNT', 100, 'PREFIX', 'user:')
    for off, event, key, value in batch:
        handle(event, key, value)
        offset = int(off) + 1
    if not batch:
        time.sleep(0.25)
```

### Consumer groups (load-balanced pull)

Also plain request/reply — N workers share one feed, each record delivered once:

```python
r.execute_command('CDCGROUP', 'CREATE', 'workers', '$')   # $ = only new; 0 = all retained
while True:
    batch = r.execute_command('CDCREADGROUP', 'workers', 'worker-1', 'COUNT', 10)
    for off, event, key, value in batch:
        process(event, key, value)
        r.execute_command('CDCACK', 'workers', off)
```

### Push (live broadcast + geofencing)

`CDCSUBSCRIBE [prefix]` and `CDCSUBSCRIBE REGION <lon> <lat> <radius> <unit>` put the connection into a
streaming **push** mode: an atomic snapshot followed by live frames. Use a **dedicated connection** and
read frames as they arrive — don't reuse it for normal commands.

redis-py exposes this cleanly via a raw connection:

```python
conn = r.connection_pool.get_connection('cdc')
conn.send_command('CDCSUBSCRIBE', 'user:')
while True:
    msg = conn.read_response()
    # ['cdc-snapshot', key, value]
    # ['cdc-snapshot-done', count, high_water_offset]
    # ['cdc-change', offset, op, key, value]
    print(msg)

# Geofencing: same pattern, region filter instead of a prefix:
# conn.send_command('CDCSUBSCRIBE', 'REGION', lon, lat, radius, 'km')
```

In Node, ioredis doesn't surface a per-frame reader for custom push commands, so use the official
**[`locusdb-client`](../clients/node)** wrapper (below), which owns the dedicated connection and emits
events. (Or drop to a raw `net.Socket` with a small RESP reader, or prefer the **pull** model.)

## Node SDK — `locusdb-client`

A thin wrapper over ioredis that adds typed helpers for the differentiator verbs and the reactive
**changefeed / geofence** as events — the one thing a stock driver can't do. `npm install locusdb-client
ioredis`:

```js
import { LocusClient } from 'locusdb-client';
const locus = new LocusClient({ host: '127.0.0.1', port: 6379 });

await locus.geoSet('driver:7', 13.36, 38.11, { status: 'free' });
const hits = await locus.geoSearch({ fromLonLat: [13.4, 38.1], byRadius: [50, 'km'], withDist: true });

// live changefeed + geofencing (a dedicated connection under the hood):
locus.changefeed('user:').on('change', (c) => console.log(c.op, c.key, c.value));
locus.geofence(13.4, 38.1, 5, 'km')
  .on('enter', (m) => console.log('entered', m.key))
  .on('leave', (m) => console.log('left', m.key));
```

The raw ioredis connection stays available as `locus.redis` for any standard command. Full docs:
[clients/node/README.md](../clients/node/README.md).

## Quick testing with redis-cli

```console
redis-cli -p 6379 GEOSET p 13.36 38.11
redis-cli -p 6379 GEOSEARCH FROMKEY p BYRADIUS 200 km ASC
redis-cli -p 6379 CDCSUBSCRIBE user:      # streams snapshot, then live changes
```

## Notes

- **Security** — `AUTH`/`requirepass`, ACLs, and protected mode are available (pass `password` to your
  client); TLS via the optional `tls` build or a sidecar. The binary binds `127.0.0.1` by default; the
  Docker image sets `LOCUS_BIND=0.0.0.0`. See [DEPLOYMENT.md](DEPLOYMENT.md).
- Replies follow Redis conventions (RESP2-compatible encoders even after a RESP3 `HELLO`), so existing
  clients decode them without surprises.
- Full command reference: [COMMANDS.md](COMMANDS.md). Semantics of the differentiators:
  [CHANGEFEED.md](CHANGEFEED.md), [GEO.md](GEO.md), [SKETCHES.md](SKETCHES.md).
