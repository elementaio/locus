# Command Reference

The commands Locus implements today. Names are case-insensitive. Replies follow the
[RESP](https://redis.io/docs/latest/develop/reference/protocol-spec/) protocol, so any Redis client
works. This is a curated subset of Redis — the common, useful commands per type — not the full surface.

## Connection & server

| Command | Notes |
|---|---|
| `PING [msg]` | `PONG`, or echoes `msg` |
| `ECHO msg` | |
| `HELLO [2\|3]` | RESP version negotiation; returns server info |
| `INFO` | replication section (`role`, `connected_slaves`, master link) |
| `RESET` | abort MULTI, UNWATCH, exit subscribe mode, drop to RESP2 |
| `SELECT 0` | single logical DB; `SELECT 0` is OK, other indexes error |
| `QUIT` | |
| `COMMAND` / `CONFIG GET` | minimal stubs so clients connect cleanly |

## Generic / keyspace

| Command | Notes |
|---|---|
| `DEL key [key ...]` / `UNLINK key [key ...]` | returns count removed (UNLINK is synchronous here) |
| `EXISTS key [key ...]` / `TOUCH key [key ...]` | counts each occurrence (no LRU, so TOUCH == EXISTS) |
| `KEYS pattern` | keys matching a glob (`*`/`?`) |
| `DBSIZE` | number of keys |
| `RANDOMKEY` | a random key (nil if empty) |
| `RENAME key newkey` / `RENAMENX key newkey` | move value+TTL; RENAMENX fails if dest exists |
| `FLUSHDB` / `FLUSHALL` | empty the keyspace (single logical DB) |
| `TYPE key` | `string`/`list`/`hash`/`set`/`zset`/`stream`/`none` |
| `EXPIRE` / `PEXPIRE` / `EXPIREAT` / `PEXPIREAT key n` | set TTL |
| `TTL` / `PTTL key` | `-2` no key, `-1` no expiry |
| `PERSIST key` | remove TTL |

## Strings

| Command | Notes |
|---|---|
| `SET key val [EX\|PX\|EXAT\|PXAT n] [NX\|XX] [KEEPTTL] [GET]` | |
| `SETNX key val` / `GETSET key val` | set-if-absent / set-and-return-old |
| `SETEX key sec val` / `PSETEX key ms val` | set with TTL |
| `GET key` / `GETDEL key` | |
| `MGET key [key ...]` / `MSET key val [key val ...]` / `MSETNX key val [...]` | bulk get/set; MSETNX is all-or-nothing |
| `INCR` / `DECR` / `INCRBY` / `DECRBY` | integer, errors on non-int / overflow |
| `INCRBYFLOAT key incr` | float; rejects nan/inf |
| `APPEND key val` / `STRLEN key` | |
| `GETRANGE key start end` / `SETRANGE key offset val` | substring (inclusive, neg indices) / overwrite-pad |

## Lists

`LPUSH` `RPUSH` `LPUSHX` `RPUSHX` `LPOP [count]` `RPOP [count]` `LLEN` `LRANGE` (negative indices)
`LINDEX` `LSET` `LINSERT key BEFORE|AFTER pivot value` `LREM key count value` `LTRIM key start stop`
`LPOS key value [RANK r] [COUNT n]` `RPOPLPUSH src dst` `LMOVE src dst LEFT|RIGHT LEFT|RIGHT`

## Sketches (probabilistic)

Compact, mergeable summaries. First up: a Bloom filter for dedup / set membership.

| Command | Notes |
|---|---|
| `BFADD key item` | add an item → `1` if probably new, `0` if probably already seen |
| `BFEXISTS key item` | `1` if probably present, `0` if **definitely** absent (no false negatives) |
| `BFLOAD key k nbits bits` | restore raw bits (used by AOF rewrite / replication) |
| `CMSINCRBY key item count [item count ...]` | Count-Min: add to frequencies → new estimate per item |
| `CMSQUERY key item [item ...]` | estimated frequency per item (over-estimate, never under) |
| `CMSLOAD key width depth bytes` | restore raw counters (AOF rewrite / replication) |
| `TOPKRESERVE key k` | create a Top-K (heavy hitters) tracking the `k` most frequent items |
| `TOPKADD key item [item ...]` | count items → per item, the leaderboard item it evicted (or nil) |
| `TOPKLIST key` | current leaders, highest count first |
| `TOPKCOUNT key item [item ...]` | estimated count per item |
| `TOPKLOAD key bytes` | restore (AOF rewrite / replication) |
| `TDADD key value [value ...]` | t-digest: add numeric samples |
| `TDQUANTILE key q [q ...]` | estimated value at each quantile `q` (0..1); exact at min/max |
| `TDLOAD key bytes` | restore (AOF rewrite / replication) |

Auto-sized on first add (Bloom ≈10k @ 1% FPR; CMS 2000×5; TopK k=10; t-digest compression 100);
persist via RDB/AOF. The full a-la-carte sketch family is in place.

## Conditional writes (CAS family)

Check-and-write in a single atomic step (no WATCH/Lua needed) — for persist-before-ack, dedup,
monotonic cursors, and quotas.

| Command | Notes |
|---|---|
| `CAS key expected new` | set `new` iff the current value == `expected` (key must exist) → `1`/`0` |
| `CADEL key expected` | delete iff the current value == `expected` → `1`/`0` |
| `SETMAX key n` | monotonic: store `n` iff `n` > current integer (or missing) → `1` if advanced else `0` |
| `INCRCAP key delta cap` | increment iff result ≤ `cap`; returns the new value, or nil if it would exceed |

## Geo (geo-first)

Each geo object is its **own key** holding a point (the geo-first model, not Redis's members-in-a-zset).
A spatial index over geo keys powers search; persists via RDB/AOF.

| Command | Notes |
|---|---|
| `GEOSET key <lon> <lat>` | set/overwrite a key's point (lon ∈ ±180, lat ∈ ±85.05) |
| `GEOPOS key [key ...]` | `[lon, lat]` per key (nil if missing / not geo) |
| `GEODIST key1 key2 [m\|km\|mi\|ft]` | great-circle (haversine) distance |
| `GEOSEARCH FROMLONLAT lon lat \| FROMKEY key  BYRADIUS r unit \| BYBOX w h unit  [ASC\|DESC] [COUNT n] [WITHCOORD] [WITHDIST]` | keys within a radius/box, optionally sorted by distance |

(Live geofencing — `CDCSUBSCRIBE REGION …` over the changefeed — and a real S2/R-tree index with
combined attribute filters are the next phases.)

## Bitmaps

`SETBIT key offset 0|1` `GETBIT key offset` `BITCOUNT key [start end [BYTE\|BIT]]`
`BITPOS key 0|1 [start [end [BYTE\|BIT]]]` `BITOP AND\|OR\|XOR\|NOT dest key [key ...]`
(bit 0 = most-significant bit of byte 0, as in Redis)

## Hashes

`HSET` `HSETNX` `HGET` `HMGET` `HGETALL` `HDEL` `HEXISTS` `HLEN` `HKEYS` `HVALS` `HINCRBY`

## Sets

`SADD` `SREM` `SMEMBERS` `SISMEMBER` `SMISMEMBER` `SCARD` `SPOP [count]` (random)
`SRANDMEMBER key [count]` (negative count = with repeats) `SINTER` `SUNION` `SDIFF`
`SMOVE src dst member` `SINTERSTORE dst key...` `SUNIONSTORE dst key...` `SDIFFSTORE dst key...`
`SINTERCARD numkeys key... [LIMIT n]`

## Sorted sets

`ZADD [NX\|XX] [GT\|LT] [CH] [INCR]` `ZSCORE` `ZMSCORE` `ZCARD` `ZREM` `ZINCRBY` `ZRANK` `ZREVRANK`
`ZRANGE [WITHSCORES] [REV]` `ZREVRANGE` `ZRANGEBYSCORE` / `ZREVRANGEBYSCORE` (exclusive `(` bounds,
`inf`/`-inf`, `LIMIT`) `ZCOUNT` `ZPOPMIN [count]` `ZPOPMAX [count]`
`ZREMRANGEBYRANK key start stop` `ZREMRANGEBYSCORE key min max`
`ZUNIONSTORE dst numkeys key... [WEIGHTS w...] [AGGREGATE SUM|MIN|MAX]` `ZINTERSTORE` (same form;
sources may be sets, scoring 1.0)

## Streams

| Command | Notes |
|---|---|
| `XADD key <id\|*\|ms-*> field value [field value ...]` | monotonic `ms-seq` ids |
| `XLEN key` | |
| `XRANGE key start end [COUNT n]` | `-`/`+` bounds |
| `XREVRANGE key end start [COUNT n]` | |
| `XREAD [COUNT n] [BLOCK ms] STREAMS key... id...` | `$` = new-only; **blocking** supported |

_Consumer groups (`XGROUP`/`XREADGROUP`/`XACK`) are not yet implemented._

## Pub/Sub

`SUBSCRIBE` `UNSUBSCRIBE` `PSUBSCRIBE` `PUNSUBSCRIBE` `PUBLISH` `PUBSUB CHANNELS|NUMSUB|NUMPAT`
(glob `*`/`?` patterns)

## Changefeed (Locus-native, reactive)

A reliable, ordered alternative to keyspace notifications: subscribe to a key prefix and receive an
**atomic snapshot** of matching keys, then a live stream of every change — no gap, no dup (the
single-threaded hub guarantees it). The connection enters push mode (like pub/sub).

| Command | Notes |
|---|---|
| `CDCSUBSCRIBE [prefix]` | snapshot (`["cdc-snapshot", key, value]` …, then `["cdc-snapshot-done", count, offset]`), then live `["cdc-change", offset, write\|del\|expire, key, value]` |
| `CDCSUBSCRIBE REGION <lon> <lat> <radius> <unit>` | **live geofencing**: snapshot of geo keys in the circle, then live `write` as keys enter/move and `del` as they leave (move out / delete / expire); change value is `"lon,lat"` |
| `CDCUNSUBSCRIBE` | leave push mode |
| `CDCREAD <offset> [COUNT n] [PREFIX p]` | pull retained changes after `offset` (catch-up after a disconnect); each entry `[offset, event, key, value]` |
| `CDCGROUP CREATE <group> [offset\|$\|0]` / `CDCGROUP DESTROY <group>` | consumer group (load-balanced read mode); `$`/default = only new, `0` = all retained |
| `CDCREADGROUP <group> <consumer> [COUNT n]` | deliver the next un-delivered records to a consumer (disjoint across the group); tracked as pending until acked |
| `CDCACK <group> <offset> [offset ...]` | acknowledge delivery (drop from the pending list) |
| `CDCPENDING <group>` | `[total, [[consumer, count], …]]` |

Every change carries a **monotonic offset**. Retention for `CDCREAD` is opt-in via
`LOCUS_CDC_MAXLEN=<records>` (a ring buffer); reading from an offset older than what's retained returns
`offset out of range` so a consumer knows to re-snapshot. `CDCSUBSCRIBE`'s `snapshot-done` reports the
high-water offset, so a dropped subscriber can reconnect and `CDCREAD` that offset to catch up, then
resubscribe. Values are inlined for string keys; other types signal change-only (client re-fetches).
(Consumer groups / geo-region filters are the next phases.)

## Transactions

`MULTI` `EXEC` `DISCARD` `WATCH` `UNWATCH` — optimistic locking; no rollback on runtime error (as in
Redis).

## Persistence

`SAVE` `BGSAVE` `BGREWRITEAOF`

## Replication

`REPLICAOF host port` / `SLAVEOF` (and `REPLICAOF NO ONE`) · `REPLCONF` · `PSYNC` · `INFO`
