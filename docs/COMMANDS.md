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
| `QUIT` | |
| `COMMAND` / `CONFIG GET` | minimal stubs so clients connect cleanly |

## Generic / keyspace

| Command | Notes |
|---|---|
| `DEL key [key ...]` | returns count removed |
| `EXISTS key [key ...]` | counts each occurrence |
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
`LINDEX` `LSET`

## Hashes

`HSET` `HSETNX` `HGET` `HMGET` `HGETALL` `HDEL` `HEXISTS` `HLEN` `HKEYS` `HVALS` `HINCRBY`

## Sets

`SADD` `SREM` `SMEMBERS` `SISMEMBER` `SMISMEMBER` `SCARD` `SPOP [count]` `SINTER` `SUNION` `SDIFF`

## Sorted sets

`ZADD [NX\|XX] [GT\|LT] [CH] [INCR]` `ZSCORE` `ZMSCORE` `ZCARD` `ZREM` `ZINCRBY` `ZRANK` `ZREVRANK`
`ZRANGE [WITHSCORES] [REV]` `ZREVRANGE` `ZRANGEBYSCORE` / `ZREVRANGEBYSCORE` (exclusive `(` bounds,
`inf`/`-inf`, `LIMIT`) `ZCOUNT` `ZPOPMIN [count]` `ZPOPMAX [count]`

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

## Transactions

`MULTI` `EXEC` `DISCARD` `WATCH` `UNWATCH` — optimistic locking; no rollback on runtime error (as in
Redis).

## Persistence

`SAVE` `BGSAVE` `BGREWRITEAOF`

## Replication

`REPLICAOF host port` / `SLAVEOF` (and `REPLICAOF NO ONE`) · `REPLCONF` · `PSYNC` · `INFO`
