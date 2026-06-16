# Sketches — mergeable probabilistic summaries

Compact, fixed-size structures that answer approximate questions over unbounded streams. All four are
zero-dependency (hashing via `std`'s fixed-key `DefaultHasher`, so results are deterministic across
runs — essential for persistence and replication), auto-sized on first use, and persist via RDB/AOF.

Because a sketch can't be rebuilt from its add-history, AOF **rewrite** restores each via a raw-load
command (`BFLOAD`/`CMSLOAD`/`TOPKLOAD`/`TDLOAD`); the incremental AOF logs the normal add commands.

## Bloom filter — set membership / dedup

"Have I seen this id?" No false negatives; a small, tunable false-positive rate.

```
BFADD    key item     # -> 1 if probably new, 0 if probably already seen
BFEXISTS key item     # -> 1 probably present, 0 definitely absent
```
Auto-sized for ~10k items at 1% FPR. `TYPE` = `bloom`.

```console
redis-cli BFADD seen msg-42     # 1  (first time)
redis-cli BFADD seen msg-42     # 0  (duplicate)
redis-cli BFEXISTS seen msg-99  # 0  (never added)
```

## Count-Min — frequency / "trending"

Estimated event counts; over-estimates, never under.

```
CMSINCRBY key item count [item count ...]   # -> new estimate per item
CMSQUERY  key item [item ...]               # -> estimate per item
```
Auto-sized to width 2000 × depth 5. `TYPE` = `cms`.

```console
redis-cli CMSINCRBY trend rust 5 go 2   # [5, 2]
redis-cli CMSQUERY  trend rust zig      # [5, 0]
```

## Top-K — heavy hitters

The K most frequent items, on top of a Count-Min plus a k-slot leaderboard.

```
TOPKRESERVE key k                 # track the k most frequent (default k=10 on first TOPKADD)
TOPKADD     key item [item ...]   # -> per item, the leader it evicted (or nil)
TOPKLIST    key                   # current leaders, highest first
TOPKCOUNT   key item [item ...]   # estimated count per item
```
`TYPE` = `topk`.

```console
redis-cli TOPKRESERVE hot 3
redis-cli TOPKADD hot a a a b c    # a now leads
redis-cli TOPKLIST hot             # a, b, c
```

## t-digest — quantiles / percentiles

Streaming quantile estimation, accurate at the tails (live p99). Exact at min/max.

```
TDADD      key value [value ...]
TDQUANTILE key q [q ...]           # value at each quantile q in 0..1
```
Auto-sized (compression 100). `TYPE` = `tdigest`.

```console
# add a stream of latencies, then ask for p50 / p99
redis-cli TDADD lat 12 18 25 9 31 ...
redis-cli TDQUANTILE lat 0.5 0.99   # e.g. ["20", "120"]
```

Centroids are weight-bounded by the `q(1-q)` scale, so the tails keep fine resolution — the property
that makes p99 trustworthy while staying compact.

## Why these (and not a time-series type)

Per the design notes: Count-Min/Top-K give "trending now", t-digest gives live percentiles, and Bloom
gives dedup — together they subsume most of what a metrics/time-series type is reached for, at a tiny,
fixed memory cost, and they're naturally **mergeable** (the right shape for an eventual spatial cluster).
