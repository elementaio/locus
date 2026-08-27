# Design note — geo-first spatial index + region changefeed (for review)

> **Status: Model B chosen; PHASE 1 IMPLEMENTED** (PR #15). `Value::Geo`, geo-key index, haversine,
> `GEOSET`/`GEOPOS`/`GEODIST`/`GEOSEARCH` (BYRADIUS/BYBOX, FROMLONLAT/FROMKEY, ASC/DESC, COUNT,
> WITHCOORD/WITHDIST), RDB+AOF persistence. Brute-force-over-geo-keys candidate scan (the grid/S2/R-tree
> index is the phase-3 optimization; interface unchanged).
> **PHASE 2 IMPLEMENTED** (PR #16): `CDCSUBSCRIBE REGION <lon> <lat> <radius> <unit>` — live geofencing.
> Snapshot of in-region geo keys, then enter (`write`) / leave (`del`) transitions via per-subscriber
> membership tracking. The geo index + changefeed converged exactly as designed (region filter = prefix
> filter's spatial sibling).
> **Next: phase 3** — real S2/R-tree index + combined attribute filters + keyset pagination; then the
> spatial-clustering arc (the Tile38-beating part).

## 1. The goal and the payoff

Win the empty market intersection: *in-memory · Redis-simple · **geo-first** · combined filters ·
spatially clustered.* The killer demo is **live geofencing**: subscribe to a region and get a snapshot
of what's inside plus a live stream as objects **enter/move/leave** — i.e. the changefeed we just
built, but filtered by *space* instead of key prefix. Tile38 does geo but **doesn't cluster**; that's
the wedge (clustering is a later arc — this note is the single-node index + region changefeed).

## 2. THE FORK — how is a geo object modeled? (need your call)

This is the whole decision; everything else follows.

### Model A — Redis-compatible GEO (objects are *members* of one key)
`GEOADD fleet <lon> <lat> driver:1` stores `driver:1` as a **member** of the `fleet` sorted set, scored
by an interleaved geohash (exactly how Redis does it). `GEOSEARCH fleet ...` scans/decodes.
- ➕ Wire-compatible with Redis GEO; existing clients/tools work; reuses our `ZSet` type (little new storage).
- ➖ **Doesn't fit the changefeed.** Our changefeed is per-**key**; here all objects live in *one* key, so a
  position update is "key `fleet` changed" — no per-object granularity. Geofencing would need a separate
  member-level change mechanism — a second overlapping subsystem (the thing DIFFERENTIATORS says to refuse).
- ➖ It's the "geohash hack" the LANDSCAPE doc explicitly wants to beat.

### Model B — geo-first (objects are *keys*; geo is a secondary index)  ← my recommendation
Each object is its **own key** with a geo-point value: `GEOSET driver:1 <lon> <lat>` (or `SET` of a
point-typed value). A **spatial index** maps location → the set of geo keys. `GEOSEARCH BYRADIUS ...`
queries the index; the **region changefeed** is just our existing per-key changefeed with a *region*
predicate instead of a *prefix* one.
- ➕ **Geofencing falls out for free**: a position write to `driver:1` already flows through `exec_one`
  → the changefeed; we filter by "is this key's point in the region." One primitive, not two.
- ➕ This is the actual DIFFERENTIATORS thesis (geo = the secondary-index primitive #4, region = filter #3).
- ➕ Combined attribute filters later compose naturally (the key can carry other indexed fields).
- ➖ **Not** Redis-GEO wire-compatible (different model). New `GEO*` verbs, Locus-native.
- ➖ More new machinery (a geo value type + an index) than reusing `ZSet`.

**Recommendation: Model B.** The entire reason geo is the moat is the geofencing-changefeed, and that
only works cleanly if objects are keys. Redis-GEO-compat is a "chasing Redis" trap we already agreed to
skip. (If you want Redis-GEO-compat *too*, it can be a thin separate adapter later — but not the core.)

The rest of this note assumes **Model B**.

## 3. Data model (Model B)

- A new value kind: a **geo point** `(lon: f64, lat: f64)`. Store it as its own `Value::Geo` variant, or
  (simpler, zero new type) as a `Value::Str` holding the encoded point that `GEO*` commands parse. Lean:
  a real `Value::Geo(GeoPoint)` — clean typing + WRONGTYPE, and it's what the index keys off.
- A hub-level **spatial index**: `GeoIndex` mapping a coarse **cell id** (a lat/lon grid bucket, ~the
  poor-man's S2 cell) → `HashSet<key>`. Maintained in `exec_one` alongside the changefeed hook: on a
  geo write, update the key's cell; on delete/expire, remove it. Brute-force-within-candidate-cells for
  the query. (Phase 3 swaps the grid for true S2 cells / an R-tree; the *interface* stays.)
- Validation: lon ∈ [-180,180], lat ∈ [-85.05,85.05] (web-mercator clamp), like Redis.

## 4. Command surface (Model B, Locus-native)

Phase 1 (index + queries):
```
GEOSET   key <lon> <lat>                     # set/overwrite a point (a normal write -> changefeed)
GEOPOS   key [key ...]                        # -> [lon, lat] per key (nil if missing / not geo)
GEODIST  key1 key2 [m|km|mi|ft]              # great-circle distance (haversine)
GEOSEARCH FROMLONLAT <lon> <lat> | FROMKEY <key>
          BYRADIUS <r> <unit> | BYBOX <w> <h> <unit>
          [ASC|DESC] [COUNT n] [WITHCOORD] [WITHDIST]   # -> matching keys, optionally sorted by distance
```
Phase 2 (the flagship — region changefeed):
```
CDCSUBSCRIBE REGION <lon> <lat> <radius> <unit>   # snapshot of in-region geo keys, then live
                                                  # cdc-change as keys enter/move/leave the region
```
This reuses the changefeed machinery; the only new bit is a *region* matcher alongside the existing
*prefix* matcher, evaluated against each changed key's current point.

## 5. Distance & search

- **Haversine** for great-circle distance; units m/km/mi/ft. Brute-force phase 1: gather candidate keys
  from the index cells overlapping the query area, compute true distance, filter + optionally sort.
  Correct and simple; the grid prunes the obvious non-matches. (Exactly the "correct first, optimize
  the index later" path the codebase took for sorted sets.)
- Keyset pagination (`COUNT` + a cursor) is a phase-3 refinement.

## 6. Phasing

1. **Index + queries:** `Value::Geo`, the grid index (maintained in `exec_one`), `GEOSET`/`GEOPOS`/
   `GEODIST`/`GEOSEARCH` (BYRADIUS/BYBOX, ASC/DESC, COUNT, WITHCOORD/WITHDIST), haversine. Persists via
   RDB/AOF like any value; changefeed already emits geo-key writes.
2. **Region changefeed:** `CDCSUBSCRIBE REGION ...` → live geofencing. Snapshot in-region keys + stream
   enter/leave. (Enter/leave detection: on each geo change, test the key's new point against each
   region subscriber; "leave" = a key that *was* in-region is now outside or deleted.)
3. **Real index + combined filters + keyset pagination:** swap the grid for S2 cells / R-tree; AND geo
   with attribute predicates; cursor paging. Then the **clustering** arc (the Tile38-beating part).

## 7. Open questions (need your call before phase 1)

- **Q1 — Model A vs B?** My strong rec: **B** (geo-first, objects-as-keys), for the reasons in §2. Confirm,
  or tell me Redis-GEO-compat matters enough to do A (or both).
- **Q2 — value type:** dedicated `Value::Geo(lon,lat)` (clean typing, my lean) vs. encode-in-string
  (no new type, less clean). OK with a new `Value::Geo`?
- **Q3 — phase-1 scope:** just the index + `GEO*` queries first (my lean — lower risk, sets up the demo),
  or fold the **region changefeed** into the first PR so the geofencing demo lands immediately (bigger,
  but it's the wow)?

**Recommendation:** Model B, `Value::Geo`, phase-1 = index + `GEO*` queries (one PR), then region
changefeed (phase 2). Say "go with the recommendation" or adjust Q1–Q3 and I'll start.
