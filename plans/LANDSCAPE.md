# Competitive Landscape — Who Else Solved In-Memory Geo

The positioning check: if someone already nailed "in-memory + simple + geo-first + combined filters +
clustered," there's no white space. Conclusion: **several solved *parts*; nobody occupies the whole
intersection.** That empty intersection is Locus's lane.

---

## The field

| DB | In-memory? | Geo approach | Geo + attribute filters? | Sharded/clustered geo? | Simple/elegant? |
|---|---|---|---|---|---|
| **Tile38** | ✅ Yes | Real **R-tree** + geofencing | partial | ❌ leader-follower only | ✅ Redis-like |
| **Redis + RediSearch** | ✅ Yes | geohash + **GEOSHAPE** (polygons) | ✅ Yes (query engine) | ⚠️ Redis Cluster (hash-sharded) | ➖ heavy module stack |
| **Aerospike** | ✅ Yes (RAM/flash) | **Google S2** secondary index | ✅ Yes | ✅ Yes, distributed | ❌ big system |
| **SingleStore / VoltDB** | ✅ Memory-first SQL | GEOGRAPHY types, polygons | ✅ Yes (SQL) | ✅ Yes | ❌ heavy SQL |
| **Apache Ignite / Hazelcast** | ✅ In-memory grid | JTS / GEOMETRY index | ✅ Yes | ✅ Yes | ❌ JVM grid |
| **Elasticsearch / OpenSearch** | ⚠️ memory-cached, not in-RAM | **BKD-tree** geo_point/geo_shape | ✅ **best-in-class** | ✅ Yes | ❌ JVM, ops-heavy |
| **Qdrant** (Rust) | ✅ In-memory-capable | geo radius/polygon filters | ✅ (alongside vectors) | ✅ Yes | ➖ vector-first |
| **PostGIS** (disk reference) | ❌ disk + buffer cache | **GiST R-tree**, every op | ✅ Yes | ⚠️ Citus | ➖ full Postgres |

---

## The ones that matter most

### Tile38 — the closest thing to Locus
In-memory, Redis-protocol, a *real* R-tree (not the geohash hack), and live **geofencing**. Essentially
"the Locus idea, already shipping." **Study it line by line.** Its gap is the one we care about: it does
leader-follower replication, **not horizontal spatial sharding.** That gap is the wedge.

### Redis itself — the honest correction to "even Redis struggles"
Vanilla Redis `GEO` *does* struggle (geohash-on-sorted-set, no combined filters). But **RediSearch /
Redis Query Engine** quietly closed much of it: in-memory, combined `@location:[...]` + attribute
filters, and **GEOSHAPE polygons** (WITHIN/CONTAINS). So Redis *Stack* is actually decent here. The
catch: it's a heavy module stack where geo is a *secondary* concern, and clustering it falls back to
**hash-sharding that scatters spatial locality** (see [CLUSTER.md](CLUSTER.md)). Not geo-first, not
elegant.

### Aerospike — validates the S2 bet
A serious, distributed, in-memory-class DB whose geo index is built on **Google S2 cells** combined with
other secondary indexes. That a production system shards/indexes geo with S2 confirms our primitive
choice. But it's a large operational system; geo is a feature, not the identity.

### Elasticsearch — won the *query semantics*
Nobody does "nearby + filter + sort-by-distance + paginate" more completely (BKD-tree). But it's
memory-*hungry*, JVM, ops-heavy — not in-memory-Redis-fast, not simple.

### Qdrant — the Rust neighbor
Rust, in-memory-capable, geo filters alongside vector search. Worth reading for "how a modern Rust
engine structures in-memory filtering."

---

## The white space (Locus's lane)

No one occupies **all** of: *in-memory · Redis-simple · geo-FIRST · combined filters · horizontally
clustered.*

- **Tile38**: simple + in-memory + geo-first — **but doesn't cluster.**
- **Aerospike**: clustered + S2-geo + fast — **but not simple or geo-first.**
- **Redis Stack**: in-memory + combined filters — **but geo is bolted-on and clusters by hash (wrong
  for geo).**

The intersection — **elegant, geo-first, *spatially* clustered** — is empty.

---

## Your technical choices are externally validated

You're not guessing — you'd be assembling proven primitives into a combination nobody has packaged
simply:

| Choice | Validated by |
|---|---|
| **S2 cells** for spatial indexing/sharding | Aerospike |
| **R-tree** spatial index (not geohash hack) | Tile38, PostGIS |
| **Combined geo + attribute + sort + paginate** is the real need | RediSearch, Elasticsearch |
| **In-memory + Redis-protocol for geo** is viable & loved | Tile38 |
| **Geofencing on live streams** is a killer feature | Tile38 (and ties to Pulsar) |

---

## Strategic takeaway

Don't out-feature PostGIS or out-query Elasticsearch. **Win the intersection they can't reach without
becoming heavy:** Tile38's elegance + Aerospike's S2-sharding + RediSearch's combined queries, in one
in-memory, geo-first, simple-to-run engine. Beat Tile38 specifically on the thing it skipped —
**spatial clustering** — which is exactly where your existing BEAM clustering work gives you a head
start (see [CLUSTER.md](CLUSTER.md)).
