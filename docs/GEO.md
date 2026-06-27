# Geo — geo-first spatial data

Locus is **geo-first**: a geo object is its *own key* holding a point, and a spatial index over those
keys powers search. This is deliberately *not* Redis's model (members of a sorted set, geohash scores).
The payoff is that geo and the [changefeed](CHANGEFEED.md) converge — a *region* filter on the per-key
changefeed is **live geofencing**, for free.

## Storing points

```
GEOSET key <lon> <lat>          # set/overwrite this key's point (like SET, but a geo value)
```
`lon ∈ [-180, 180]`, `lat ∈ [-85.05, 85.05]`. The key's `TYPE` is `geo`. Points persist in RDB/AOF and
are stored exactly (no geohash quantization), so `GEOPOS` round-trips what you set.

## Querying

```
GEOPOS  key [key ...]                       # -> [lon, lat] per key (nil if missing / not geo)
GEODIST key1 key2 [m|km|mi|ft]              # great-circle (haversine) distance
GEOSEARCH FROMLONLAT <lon> <lat> | FROMKEY <key>
          BYRADIUS <r> <unit> | BYBOX <w> <h> <unit>
          [ASC|DESC] [COUNT n] [WITHCOORD] [WITHDIST]
```

`GEOSEARCH` returns the keys within a radius or box of a center (given as a literal point or another
key), optionally sorted by distance and annotated:

```console
redis-cli GEOSET Palermo 13.361389 38.115556
redis-cli GEOSET Catania 15.087269 37.502669
redis-cli GEODIST  Palermo Catania km                       # -> "166.2742"
redis-cli GEOSEARCH FROMKEY Palermo BYRADIUS 200 km ASC     # -> Palermo, Catania
redis-cli GEOSEARCH FROMLONLAT 15 37 BYBOX 400 400 km WITHDIST WITHCOORD
```

## Live geofencing

```
CDCSUBSCRIBE REGION <lon> <lat> <radius> <unit>
```
A snapshot of the geo keys currently inside the circle, then a live stream as objects **enter/move**
(`cdc-change write`, value `"lon,lat"`) and **leave** (`cdc-change del` — moved out, deleted, or
expired). Each subscriber tracks its own in-region membership, so transitions are exact. See
[CHANGEFEED.md](CHANGEFEED.md).

```console
redis-cli GEOSET driver:7 0 0
redis-cli CDCSUBSCRIBE REGION 0 0 50 km     # snapshot: driver:7, then blocks
# elsewhere:
redis-cli GEOSET driver:8 0.1 0.1           # ~15 km in -> cdc-change write driver:8 0.1,0.1
redis-cli GEOSET driver:7 30 30             # moves out      -> cdc-change del   driver:7
```

## Internals & limits

- **Index:** geo points are kept in a **geohash spatial index** — each point's (lon, lat) is encoded to a
  52-bit interleaved cell id held in a `BTreeMap`, so `GEOSEARCH` range-scans only the handful of cells
  covering the query box and then refines those candidates by true haversine distance. This makes the
  common case (small radius) **sub-linear** instead of scanning every geo key. A box that straddles a pole
  or the ±180° meridian safely falls back to a full scan. The 52-bit cell id is also the future shard key
  for spatial clustering. *(A finer S2-cell / R-tree index and combined attribute filters are the next
  step; the query interface won't change.)*
- **Distance:** haversine with Redis's earth radius (6 372 797.560856 m); units `m`/`km`/`mi`/`ft`.
- **`BYBOX`** uses east-west and north-south distance from the center (an approximation that's good for
  modest boxes).
- **Roadmap:** combined attribute filters (`nearby AND status=…`), keyset pagination, and **spatial
  clustering** (the Tile38-beating part — horizontal sharding that preserves locality).
