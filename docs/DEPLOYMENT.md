# Deploying Locus in production

This guide covers running Locus safely outside a trusted laptop: **network exposure
& auth, TLS (via a sidecar), persistence, resource limits, monitoring, and
failover.** Locus is a single static binary configured entirely through environment
variables, so most of this is wiring, not code.

> **Scope.** Locus today is a hardened **single node** with master/replica
> replication. It does *not* yet do automatic failover or horizontal clustering
> (see [ROADMAP.md](ROADMAP.md)). Plan your topology accordingly: one writable
> master, optional read replicas, and an external supervisor for promotion.

---

## 1. Lock down the network

By default Locus binds `127.0.0.1` and runs in **protected mode** — if you bind a
public interface without setting a password, non-loopback clients are refused. Do
**not** disable that without setting auth first.

```bash
LOCUS_BIND=0.0.0.0              # expose beyond loopback (the Docker image does this)
LOCUS_REQUIREPASS=<strong-pw>  # REQUIRED before exposing; clients must AUTH
LOCUS_MAXCLIENTS=10000         # cap concurrent connections (FD-exhaustion guard)
LOCUS_TIMEOUT=300              # drop idle connections after N seconds (0 = never)
```

Checklist before exposing a port:

- [ ] `LOCUS_REQUIREPASS` set to a high-entropy secret (32+ random chars).
- [ ] Least-privilege **ACL** users for apps that don't need everything:
      `ACL SETUSER app on >apppw +@read +@write ~app:` (see [the ACL section of
      COMMANDS.md](COMMANDS.md)). Keep the unrestricted `default` user for admins only.
- [ ] Firewall / security group restricts the port to known clients.
- [ ] TLS in front of the port (next section) if traffic crosses any untrusted link.

---

## 2. TLS

Two options, depending on whether you want to keep the binary 100% dependency-free.

### Option 0 — in-process TLS (optional `tls` build feature)

If you build with `--features tls`, Locus terminates TLS itself via rustls (pure
Rust, `ring` provider — no OpenSSL/C). The **default build stays dependency-free**;
only this feature pulls in a crate.

```bash
cargo build --release --features tls
LOCUS_BIND=0.0.0.0 LOCUS_TLS_PORT=6380 \
LOCUS_TLS_CERT=/etc/locus/server.crt LOCUS_TLS_KEY=/etc/locus/server.key \
LOCUS_REQUIREPASS=$PW target/release/locus      # plaintext on 6379 + TLS on 6380
redis-cli --tls -p 6380 -a $PW ping
```

The TLS listener runs alongside the plaintext one (bind plaintext to loopback and
expose only the TLS port). Cert/key are PEM. This is the simplest path if you're
comfortable with the one optional dependency.

### Sidecar — keep the core zero-dependency

To keep even the running binary free of a TLS stack, terminate TLS in a **sidecar**
co-located with each Locus process (the default build does not terminate TLS). This
is exactly how many teams run plaintext services securely, and Locus's
loopback-by-default binding makes it clean: Locus listens only on `127.0.0.1`, the
sidecar owns the public TLS port and forwards to it.

```
client ──TLS──▶ sidecar (:6380)  ──plaintext on 127.0.0.1──▶  locus (:6379)
```

### Option A — ghostunnel (recommended; mutual TLS, modern)

```bash
# Locus stays on loopback:
LOCUS_BIND=127.0.0.1 LOCUS_PORT=6379 LOCUS_REQUIREPASS=$PW locus &

# ghostunnel terminates TLS on :6380 and forwards to Locus, requiring client certs:
ghostunnel server \
  --listen 0.0.0.0:6380 \
  --target 127.0.0.1:6379 \
  --cert server.crt --key server.key \
  --cacert ca.crt          # verify client certs (mTLS); omit for server-only TLS
```

Clients connect with TLS to `:6380` (e.g. `redis-cli --tls --cert client.crt
--key client.key --cacert ca.crt -p 6380 -a $PW PING`).

### Option B — stunnel (ubiquitous)

`stunnel.conf`:

```ini
[locus]
accept  = 0.0.0.0:6380
connect = 127.0.0.1:6379
cert    = /etc/locus/server.pem
; require + verify client certs:
; CAfile = /etc/locus/ca.pem
; verify = 2
```

### TLS for replication

A replica's link to its master is just another client connection, so it gets TLS
the same way: run a sidecar in **client mode** next to the replica that dials the
master's TLS port, and point `REPLICAOF` at the local sidecar.

```bash
# next to the replica: ghostunnel client terminates TLS to the master's :6380
ghostunnel client --listen 127.0.0.1:7000 --target master.example:6380 \
  --cert replica.crt --key replica.key --cacert ca.crt &

# the replica replicates through the local tunnel; masterauth carries the password
LOCUS_MASTERAUTH=$PW locus &
redis-cli -p 6379 REPLICAOF 127.0.0.1 7000
```

### docker-compose (server-side TLS sidecar)

```yaml
services:
  locus:
    image: ghcr.io/intenttext/locus:latest
    environment:
      LOCUS_BIND: 127.0.0.1          # loopback only; the sidecar fronts it
      LOCUS_REQUIREPASS: ${LOCUS_PW}
      LOCUS_RDB: /data/locus.rdb
      LOCUS_AOF: /data/locus.aof
    volumes: [ "locus-data:/data" ]
    network_mode: "service:tls"      # share the netns so 127.0.0.1 is shared
  tls:
    image: ghostunnel/ghostunnel:latest
    command: >
      server --listen 0.0.0.0:6380 --target 127.0.0.1:6379
      --cert /certs/server.crt --key /certs/server.key --cacert /certs/ca.crt
    ports: [ "6380:6380" ]
    volumes: [ "./certs:/certs:ro" ]
volumes: { locus-data: {} }
```

---

## 3. Persistence & backups

Enable both an append-only log (point-in-time durability) and snapshots
(fast restart / compact backups):

```bash
LOCUS_AOF=/data/locus.aof        # append-only persistence
LOCUS_APPENDFSYNC=everysec       # always | everysec (default) | no
LOCUS_RDB=/data/locus.rdb        # snapshot path
```

- **`appendfsync`:** `everysec` bounds data loss to ~1s on power loss (good default);
  `always` is safest (fsync per write) but slowest; `no` leaves flushing to the OS.
- **`BGREWRITEAOF`** compacts the AOF off the hot path (it no longer blocks the
  server) and is safe to run on a cron; writes during the rewrite are preserved.
- **Backups:** `BGSAVE` (async) writes a consistent snapshot via temp→fsync→rename;
  copy `locus.rdb` after it completes. The AOF replay is torn-tail-tolerant, so a
  crash mid-write loses only the truncated final command.
- Put `/data` on durable storage; the snapshot/AOF renames are directory-fsync'd so
  they survive a crash, but the underlying disk still has to be real.

---

## 4. Resource limits

- **Memory:** set `LOCUS_MAXMEMORY` (e.g. `LOCUS_MAXMEMORY=2gb`). Over the cap a
  master evicts keys; a write is rejected with `OOM` only if the cap still can't be
  met. Size the container/cgroup limit comfortably above this.
- **File descriptors:** ensure `ulimit -n` exceeds `LOCUS_MAXCLIENTS` plus headroom.
- **CPU:** the command hub is single-threaded by design — one fast core matters more
  than many. Pin/limit accordingly; throughput scales with replicas for reads.

---

## 5. Monitoring & lifecycle

- **Metrics:** `INFO` is Prometheus-scrapeable via
  [`redis_exporter`](https://github.com/oliver006/redis_exporter) — point it at
  Locus (with auth) and you get the standard Redis dashboards. Watch
  `connected_clients`, `used_memory`, `master_link_status` (on replicas),
  `master_repl_offset`, and `rdb_last_bgsave_status`.
- **Slow queries:** `SLOWLOG GET` (threshold via `LOCUS_SLOWLOG_US`, default 10ms).
- **Logs:** structured, leveled to stderr; set `LOCUS_LOGLEVEL=info` (or `debug`).
- **Health check:** `redis-cli -a $PW PING` → `PONG`.
- **Graceful shutdown:** `SIGTERM`/`SIGINT` drains in-flight work, fsyncs the AOF,
  and writes a final snapshot before exiting — so orchestrators can stop Locus
  cleanly. Give the container a stop-timeout long enough for the final save.

---

## 6. Replication & failover

```bash
# replica:
LOCUS_MASTERAUTH=$PW locus &
redis-cli -p 6379 REPLICAOF master.host 6379
```

Replicas are read-only, follow the master's key expiry (no timing divergence), and
report `master_link_status` + `master_repl_offset` in `INFO`. Use **`WAIT n
<timeout>`** after a critical write to block until `n` replicas have acknowledged
it — ack-based durability across nodes.

### Automatic failover — built-in sentinel

Run the same `locus` binary as a **sentinel** to get automatic failover, no external
orchestrator:

```bash
LOCUS_SENTINEL=master.host:6379 \
LOCUS_SENTINEL_REPLICAS=replica1:6379,replica2:6379 \
LOCUS_SENTINEL_AUTH=$PW \
LOCUS_SENTINEL_DOWN_AFTER_MS=5000 \
LOCUS_SENTINEL_QUORUM=1 \
  locus
```

It health-checks the master and, when it's been unreachable past `DOWN_AFTER_MS`
**and** a quorum of replicas confirm their link is down, promotes the most
up-to-date replica (`REPLICAOF NO ONE`) and repoints the rest. While the master is
healthy it reconciles stray nodes (e.g. a returned old master) back to replicas —
basic split-brain protection. It's a single-sentinel design today (inter-sentinel
agreement is on the roadmap), so run one per failure domain. Repoint **clients** via
your service discovery / proxy / DNS after a switch.

### Manual failover (runbook / fallback)

1. Detect master loss (health checks / your supervisor).
2. Pick the most up-to-date replica (highest `master_repl_offset`).
3. Promote it: `REPLICAOF NO ONE`.
4. Repoint the other replicas: `REPLICAOF <new-master> <port>`.
5. Repoint clients (via your service discovery / proxy / DNS).

To avoid split-brain, ensure the old master cannot keep taking writes after it's
declared dead (fence it at the network/orchestrator layer before promoting).
Horizontal clustering is on the roadmap.

---

## Environment variable reference

See the [Configuration table in the README](../README.md#configuration) for the
full list of `LOCUS_*` variables and defaults.
