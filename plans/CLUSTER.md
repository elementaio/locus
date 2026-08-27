# Cluster Mode — Spatial Sharding Design

> **Status: direction, not a milestone yet.** This is M9+ territory. Per Principle #8, ship the
> brilliant single node first and add clustering as a *layer the command path doesn't know about.*
> The point of this doc is to lock the **seam** now so the single-node design doesn't paint us into a
> corner — not to build it early.

---

## Why Redis Cluster's model is wrong for geo

Redis Cluster shards by **key hash**: `CRC16(key) mod 16384` → one of 16384 "hash slots," slots
assigned to nodes, gossip bus for membership, `MOVED`/`ASK` redirects for cluster-aware clients.

The fatal flaw for geo: **hashing scatters spatially-adjacent points across random nodes.** Two
restaurants on the same street hash to different slots on different machines. So a "within 5km" query
must **fan out to *every* node**, because nearby points live everywhere. Hash sharding destroys spatial
locality — the one thing geo queries depend on. (This is why Tile38 doesn't do sharded clustering, and
why Redis Stack's geo degrades under Redis Cluster.)

---

## The right model: shard by *space*, not by key

Assign **regions of space** to nodes, using a cell ID as the shard key:

- **Google S2** (hierarchical cells via Hilbert curve on a projected sphere) — validated by Aerospike
- **Uber H3** (hierarchical hexagons) — even neighbor distances
- **Geohash prefix** — simplest, rectangular cells

Then:
- A radius query touches only the **few shards covering that area** (the cell + its neighbors) — a
  *bounded* scatter-gather, not all-nodes.
- Spatially-adjacent data lives together → most queries are single-shard.

### Hard part #1 — hot spots
Manhattan has millions of points; the Sahara cell has three. Static cells → wild load imbalance. Need
**adaptive subdivision**: split dense cells finer (quadtree-style, or S2 variable-level covering), merge
sparse ones. **This is the central hard problem of spatial sharding.**

### Hard part #2 — boundary queries & moving objects
- A search near a shard edge touches 2–4 shards → merge results by true distance.
- A moving object crossing a cell boundary triggers an **ownership handoff**; if it carries a geofence,
  a **notification reroute**. Hard, but bounded and well-defined.

---

## The synthesis: you've already built the control plane

Two assets make this unusually tractable.

### 1. Rendezvous-hash *cells*, not keys
Your chat engine already uses **rendezvous hashing (HRW)** for conversation owners + **`:syn`** process
groups. HRW beats Redis's fixed-slot scheme — minimal reshuffling when nodes join/leave. The move for
Locus: **rendezvous-hash spatial cell IDs (S2/H3) to nodes** instead of keys. Spatially-adjacent data
clusters, and the cell→node mapping stays stable across membership changes. Your existing clustering
code is ~80% of the control plane.

### 2. The polyglot split (the turn-one insight, made concrete)
- **Data plane = Rust** (single-threaded, in-memory, the hot path — the per-shard keyspace + spatial
  index).
- **Control plane = BEAM** (membership, cell ownership, failover, gossip) — *exactly* what BEAM is for,
  and *exactly* the code you've already written for the chat engine.

"Rewrite the right layer in the right language": Rust where you need predictable microsecond latency,
BEAM where you need supervision/coordination/distribution. Two runtimes, each doing what only it does
well. (If you'd rather stay single-language for v1, do the control plane in Rust too — but the BEAM
reuse is the elegant shortcut.)

---

## The free lunch: geo tolerates staleness

Per-shard async primary→replica replication (honest data-loss window on failover — see
[HARD-PARTS.md](HARD-PARTS.md) §5). The nice part: **geo workloads tolerate eventual consistency well.**
A point being 1 second stale in a "nearby" query almost never matters. So the
distributed-consistency story — brutal for a general KV store — is *materially easier* for a geo store.
A real architectural advantage you get **for free** by being geo-first.

---

## The seam to lock in now (single-node design constraints)

So that clustering stays a clean layer later, the single node should already:

1. **Keep the keyspace owner oblivious to sharding** — it answers "do this op on my data," nothing more.
2. **Index by cell ID internally** (even single-node), so the shard key already exists when you go
   distributed.
3. **Route at the edge** — a thin front layer maps a query's spatial extent → owning cell(s); on one
   node that's a no-op, on a cluster it's scatter-gather + merge-by-distance.
4. **Reuse M6/M7 snapshot + AOF** for the per-shard full-sync seam in replication.

Get those four right and "cluster mode" becomes additive, not a rewrite.

---

## Where it lands

This **reshapes M9** (replication → spatial clustering) rather than being a new appendage. It builds on
M5 (the spatial index) and M6/M7 (snapshot + AOF for full sync). Beating Tile38 on the one thing it
skipped — **spatial clustering** — is the headline differentiator, and your BEAM clustering work is the
head start. See [LANDSCAPE.md](LANDSCAPE.md) for why this intersection is empty.
