# Command Surface — What "Credible, Not a Side Project" Means

The bar: **not Redis 100%, not even close — but good enough that a stranger would deploy it.** That bar
is NOT "implement every Redis command." It's two specific disciplines:

1. **Per-type completeness.** If Locus supports hashes, it supports the *whole expected* hash command
   set — not three of them. A store that ships half a data type reads as a toy. Completeness is judged
   *within* a type, not *across* all of Redis.
2. **Skip whole categories, not random commands.** Drop entire areas that don't fit the identity
   (numbered DBs, Lua, modules) — cleanly and on purpose — while being best-in-class at the areas you
   keep (geo, reactive, the core structures).

Plus the identity layer: **geo and reactive aren't "extra" — they're the reason to exist**, and must be
the *best* commands in the product.

**Legend:** 🟢 core / table-stakes (implement fully) · ⭐ differentiator (Locus's identity) ·
🟡 secondary (after core) · ⛔ skip (deliberate non-goal)

---

## 🟢 Connection & server (small, table-stakes)
`PING` `ECHO` `HELLO` (RESP3) `AUTH` `CLIENT` (SETNAME/GETNAME/ID/NO-EVICT) `COMMAND` (DOCS/COUNT — stubs so clients init) `CONFIG` (GET/SET, minimal) `INFO` `DBSIZE` `FLUSHALL` `FLUSHDB` `SHUTDOWN` `STATS`⭐ (RESP3 structured metrics — no HTTP sidecar)

## 🟢 Generic key commands
`DEL` `UNLINK` `EXISTS` `EXPIRE` `PEXPIRE` `EXPIREAT` `PEXPIREAT` `EXPIRETIME` `PEXPIRETIME` `TTL` `PTTL` `PERSIST` `TYPE` `SCAN` `KEYS` (expected, document as O(N)) `RENAME` `RENAMENX` `RANDOMKEY` `TOUCH` `COPY` `OBJECT ENCODING` (tests assert on it)

## 🟢 Strings  — *the cache use case; implement fully*
`GET` `SET` (EX/PX/EXAT/PXAT/NX/XX/KEEPTTL/GET) `GETDEL` `GETEX` `SETNX` `SETEX` `PSETEX` `MGET` `MSET` `MSETNX` `APPEND` `STRLEN` `GETRANGE` `SETRANGE` `INCR` `DECR` `INCRBY` `DECRBY` `INCRBYFLOAT`

## 🟢 Hashes — *implement fully*
`HSET` `HSETNX` `HGET` `HMGET` `HGETALL` `HDEL` `HEXISTS` `HLEN` `HKEYS` `HVALS` `HINCRBY` `HINCRBYFLOAT` `HSCAN` `HRANDFIELD` · per-field TTL via the **unified per-element TTL** primitive (not Redis's bespoke `HEXPIRE`)

## 🟢 Lists — *implement fully, incl. blocking*
`LPUSH` `RPUSH` `LPUSHX` `RPUSHX` `LPOP` `RPOP` `LRANGE` `LLEN` `LINDEX` `LSET` `LINSERT` `LREM` `LTRIM` `LMOVE` `LPOS` `LMPOP` · blocking: `BLPOP` `BRPOP` `BLMOVE` `BLMPOP`

## 🟢 Sets — *implement fully, incl. set algebra*
`SADD` `SREM` `SMEMBERS` `SISMEMBER` `SMISMEMBER` `SCARD` `SPOP` `SRANDMEMBER` `SMOVE` `SSCAN` `SUNION`/`SUNIONSTORE` `SINTER`/`SINTERSTORE` `SINTERCARD` `SDIFF`/`SDIFFSTORE`

## 🟢 Sorted sets — *the leaderboard identity + the geo foundation; implement fully*
`ZADD` (GT/LT/NX/XX/CH/INCR) `ZSCORE` `ZMSCORE` `ZRANK` `ZREVRANK` `ZRANGE` (BYSCORE/BYLEX/REV/LIMIT) `ZRANGESTORE` `ZCARD` `ZCOUNT` `ZLEXCOUNT` `ZINCRBY` `ZREM` `ZREMRANGEBYRANK`/`BYSCORE`/`BYLEX` `ZPOPMIN` `ZPOPMAX` `BZPOPMIN` `BZPOPMAX` `ZMPOP` `BZMPOP` `ZRANDMEMBER` `ZUNION`/STORE `ZINTER`/STORE `ZINTERCARD` `ZDIFF`/STORE

## ⭐ Geo — *the reason to exist; must be the best commands in the product*
**Redis-compatible (table-stakes for a geo DB):** `GEOADD` `GEOPOS` `GEODIST` `GEOSEARCH` (BYRADIUS/BYBOX, ASC/DESC, COUNT, WITHCOORD/WITHDIST/WITHHASH) `GEOSEARCHSTORE` `GEOHASH`
**Locus extensions (the white space — beats Redis & Tile38):**
- `GEOSEARCH … FILTER field op value` — **combined attribute filters + sort-by-distance + keyset pagination** in one query (the unmet need)
- `GEOSEARCH … WITHINPOLYGON …` — polygon / arbitrary-region search
- `GEOFENCE` (+ live `SUBSCRIBE` over a region) — **live geofencing** = the Tile38-beater (rides the change-log)

## ⭐ Reactive: pub/sub, streams & the change-log — *the second identity pillar*
**Redis-compatible pub/sub (drop-in):** `SUBSCRIBE` `UNSUBSCRIBE` `PSUBSCRIBE` `PUNSUBSCRIBE` `PUBLISH` `PUBSUB`
**Redis-compatible streams:** `XADD` `XREAD` `XRANGE` `XREVRANGE` `XLEN` `XDEL` `XTRIM` `XINFO` · groups: `XGROUP` `XREADGROUP` `XACK` `XCLAIM` `XAUTOCLAIM` `XPENDING`
**Locus differentiator — unified change-log + live-query:**
- `SUBSCRIBE PREFIX <p>` / `SUBSCRIBE REGION <…>` `[WHERE field op value] [SNAPSHOT]` → **snapshot then coalesced deltas** (the changefeed — no Redis equivalent)
- one ordered log underneath pub/sub + streams + notifications (don't build three subsystems)

## ⭐ Conditional / CAS verbs — *near-free atomicity, replaces the Lua escape hatch*
`SET key val IFEQ <expected>` · `INCRBY … MAX <cap>` (capped) · `EXPIRE … IFGT/IFLT` (conditional) · compare-and-delete · versioned-key CAS

## ⭐ Probabilistic sketches — *native, not a module (RedisBloom is a bolt-on)*
HyperLogLog (Redis-compatible): `PFADD` `PFCOUNT` `PFMERGE` · **mergeable family:** `CMS.*` (Count-Min — trending) `TOPK.*` (heavy hitters) `TDIGEST.*` (live percentiles) `BF.*` (Bloom — dedup)

## 🟢 Transactions
`MULTI` `EXEC` `DISCARD` `WATCH` `UNWATCH` *(kept thin — CAS verbs above remove most of the need)*

## 🟡 Secondary — *useful, after the core lands*
Bitmaps: `SETBIT` `GETBIT` `BITCOUNT` `BITPOS` `BITFIELD` · `BITOP` · `SINTERCARD`/`OBJECT FREQ` niceties · time-based log retention (`MAXAGE`) · delayed delivery (`deliver-at`)

---

## ⛔ Deliberately NOT implementing (skip whole categories, cleanly)

| Skipped | Why |
|---|---|
| **Numbered DBs** (`SELECT` `SWAPDB` `MOVE`) | Use lightweight **namespaces** instead; numbered DBs are a discouraged Redis wart. |
| **Lua / Functions** (`EVAL` `EVALSHA` `FUNCTION`) | Embeds a second language; **CAS verbs** cover ~80% of why people reach for it. Deliberate non-goal. |
| **Modules** (`FT.*` RediSearch, `JSON.*` RedisJSON) | Full-text/document engine = "someone else's job" (Elasticsearch/Postgres). Our index stops at equality+range. |
| **Server-side triggers** (RedisGears `TFUNCTION`) | Stalls the single thread; Redis discontinued it. Use the changefeed → react in the client. |
| **Vector / ANN** (`VSIM`) | Research-grade, forces multi-threading (Redis broke single-thread for it). Qdrant/pgvector's job. |
| `DUMP`/`RESTORE`/`MIGRATE`, `DEBUG`, `MONITOR` | Niche or dangerous; not worth the surface. |
| Cluster protocol, `WAIT`, `REPLICAOF`, `FAILOVER` | Single brilliant node first; these arrive at M9 (spatial sharding), not now. |

---

## The credibility cut (what makes someone deploy it)

A stranger takes Locus seriously when it has, **complete:** strings + generic keys + expiry (the cache
job), the four collections (hashes/lists/sets/sorted-sets), **excellent geo** (combined filters +
geofence), the **changefeed**, **CAS verbs**, RESP2/RESP3 + pipelining, **persistence** (snapshot + AOF
so it's trusted), and basic observability (`INFO`/`OBJECT ENCODING`/`STATS`). That's ~M0–M8 + the geo
and reactive layers — and it's a *credible product*, not a side project, precisely because each kept
category is whole and the geo/reactive parts are best-in-class.

> Per-type completeness + a few deliberately-skipped categories + best-in-class geo & reactive
> = "an actual in-memory geo DB," not "a Redis clone with gaps."
