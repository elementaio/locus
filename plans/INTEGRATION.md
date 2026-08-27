# Integration — Locus + the Chat Engine (hand in hand)

How Locus clips into the existing BEAM chat engine (`~/projects/chat/server/apps/chat_engine`)
**without replacing it.** The engine already externalizes all storage behind *ports* (Elixir
behaviour contracts resolved at runtime) — Locus plugs into those seams.

> The engine's core ships **zero database code** (a firewall test forbids `:redix` in core). Storage
> lives behind ports, with in-memory reference adapters today. Locus becomes a **production adapter**.

---

## Three layers, joined at the ports

| Layer | Owns | Stays / changes |
|---|---|---|
| **BEAM chat engine** | Logic, protocol, **real-time fan-out (`:syn` + `:erpc`)**, clustering (rendezvous-hashed owners), sessions, supervision | **Unchanged.** Locus does NOT touch this. |
| **Postgres** | Durable system-of-record (cold, forever message history) | Stays. |
| **Locus** | Fast shared state behind the ports + the **hot recent tail** of the message log | New backend. |

The fan-out is BEAM-native and better than routing through any store — **Locus is not in the live
delivery path.** Locus is the fast *state*, not the nervous system.

---

## Port → Locus primitive map

Each port contract maps almost 1:1 onto a Locus primitive (the ports read like a spec for Locus):

| Chat-engine port | Contract | Locus primitive |
|---|---|---|
| `CursorStore.Port` | `advance/3`, monotonic forward-only | **CAS / capped-INCR** |
| `ConversationStore.Port` | membership + O(1) `member_count` | **Set + SCARD** |
| `PresenceStore.Port` | last-seen, lossy-tolerant | **value + TTL** |
| `ReceiptStore.Port` | read watermark + "seen by N" aggregate | **value + read** |
| `Persistence.Port` | idempotent `append`, `read_after`, `latest_seq` | **the change-log (offsets)** — hot tail in Locus, cold history in Postgres |

---

## The drop-in is nearly free (RESP)

The engine's production plan was "write Redis adapters with Redix." Because **Locus speaks the Redis
wire protocol**, those same Redix adapters (`SET`/`INCR`/`SADD`/`SCARD`/…) point at **Locus instead of
Redis with no client-library change** — Locus just needs the handful of core commands those adapters use
(all in M1–M4). Switching backends = a config line:

```elixir
config :chat_engine, cursor_store_adapter: Chat.Adapters.Redis.CursorStore   # Redix -> Locus host:port
```

No core changes; the engine calls `advance/3` exactly the same way.

---

## Staging (don't couple prematurely)

1. **Now:** Locus doesn't exist; engine runs on in-memory adapters. Nothing to do.
2. **First hand-in-hand:** when productionizing the engine, Locus (≈M2–M4: KV + sets + counters + CAS)
   becomes the backend for the fast-state ports — instead of standing up Redis.
3. **Later:** once Locus has the change-log (M8/M11), the persistence hot-tail + reconnect catch-up move
   onto it; Postgres keeps cold history.
4. **If chat adds location** ("people nearby", live location share): Locus serves it natively.

---

## The one rule

The ports mapping perfectly is **validation, not a license to tailor.** Build Locus as a clean, general,
geo-first store; let the chat engine adopt it via the existing port adapters. A primitive that fits chat
**and** Pulsar **and** a stranger's app is the goal — one tailored to chat is feature creep in disguise.
