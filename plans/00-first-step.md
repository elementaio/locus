# First Step — M0 + M1 in One Weekend

Goal: a TCP server on `:6379` that speaks just enough RESP for the **real `redis-cli`** to drive
`PING`, `SET`, and `GET` against an in-memory map.

- **Saturday (M0):** a ~40-line server that hardcodes `+PONG\r\n` so `redis-cli -p 6379 ping` prints
  `PONG`. The dopamine hit — *don't parse yet.*
- **Sunday (M1):** replace the fake reply with a real RESP parser, command dispatch, and an in-memory
  `HashMap`, so `redis-cli set foo bar` then `redis-cli get foo` returns `bar`.

```bash
brew install redis              # gives you redis-cli (your test harness)
cargo new myredis && cd myredis
cargo add tokio --features full  # or std::net for a sync first cut
```

---

## The wire protocol (RESP2 — all you need for M0/M1)

A request from `redis-cli` is almost always a **RESP Array of Bulk Strings**. `SET key value` arrives
on the socket as these exact bytes:

```
*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
```

Decode that to the command tokens `["SET", "key", "value"]`.

**Type prefixes** (first byte of each frame):

| Prefix | Type | Example |
|---|---|---|
| `+` | simple string | `+OK\r\n` |
| `-` | error | `-ERR msg\r\n` |
| `:` | integer | `:1000\r\n` |
| `$` | bulk string | `$5\r\nhello\r\n`  (NULL bulk = `$-1\r\n`) |
| `*` | array | `*3\r\n...` |

Everything is **CRLF-terminated and length-prefixed**, so you never scan for a delimiter inside a
payload — read the declared length, then expect CRLF. This is what makes it binary-safe and fast.

**Inline fallback:** if the first byte is **not** `*`, treat the line as space-separated tokens (this
is what makes telnet / `PING\r\n` work). Implement it — `redis-cli` uses multibulk, but it's a cheap,
useful safety net and tests reference it.

---

## Commands to implement for M1

| Command | Reply |
|---|---|
| `PING` | `+PONG\r\n` (or, if given an arg, echo it back as a bulk string) |
| `ECHO <x>` | the bulk string `x` → `$<len>\r\n<x>\r\n` |
| `SET <k> <v>` | store `k`→`v` in the map; reply `+OK\r\n` |
| `GET <k>` | the value as a bulk string, or NULL bulk `$-1\r\n` if absent |

Match command names **case-insensitively** (`redis-cli` may send `set` or `SET`).

---

## ⚠️ The one non-negotiable design point

**This is the lesson, not an optimization.** You do **not** own message boundaries. TCP is a byte
stream — a single `read()` may return half a command, exactly one, one-and-a-half, or 50 pipelined
commands glued together.

So the parser must be a **resumable state machine over a per-connection accumulating buffer**, NOT a
line reader. The contract:

> Feed bytes in → get back zero-or-more fully-parsed commands **plus the leftover partial tail** you
> keep for next time. If input is incomplete, return "need more bytes" and preserve the buffer.

A line-based reader will pass your manual GET/SET test on Saturday and then **corrupt or hang the
instant a command spans two reads or arrives pipelined.** Building it right *now* is the whole point of
doing this in Rust. (See [HARD-PARTS.md](HARD-PARTS.md) for the full RESP-parsing dragon.)

---

## Concurrency for M1

Stay faithful to Redis — **current-thread tokio runtime**. Accept connections in a loop, and either:

1. **(Cleanest, Redis-faithful)** Run one task per connection that sends parsed commands to a **single
   owner task holding the `HashMap`** over an `mpsc` channel. One logical owner of the keyspace,
   commands serialized → atomicity for free.
2. **(Simplest first cut)** Process commands inline on a single task.

Either way: **one logical owner of the keyspace, commands serialized.** Feel how that gives you
atomicity for free — no locks, no races. That *is* the Redis insight.

---

## Pseudocode (the resumable parser is the heart of it)

```rust
// One connection's lifecycle. The buffer persists across reads;
// NEVER parse a single read() in isolation.
struct Conn { inbuf: BytesBuffer }

async fn handle_conn(socket, keyspace_tx) {
    let mut conn = Conn { inbuf: empty };
    loop {
        let n = socket.read_into(conn.inbuf.tail());   // append new bytes
        if n == 0 { return; }                          // client closed
        loop {                                          // drain all COMPLETE commands held
            match try_parse_command(&mut conn.inbuf) {  // Complete | Incomplete | ProtoError
                Incomplete       => break,              // keep partial tail, go read more
                ProtoError(msg)  => { socket.write("-ERR " + msg + "\r\n"); return; } // framing lost -> close
                Complete(tokens) => {
                    let reply = execute(tokens, keyspace_tx);
                    socket.write(reply);
                }
            }
        }
    }
}

// RESP parser as a state machine over the buffer.
// Consumes bytes ONLY when a full frame is present.
fn try_parse_command(buf) -> ParseResult {
    if buf.is_empty() { return Incomplete; }
    if buf[0] != '*' {                                 // INLINE fallback (telnet, raw PING)
        let line = read_line_crlf(buf) ?? return Incomplete;
        return Complete(split_on_spaces(line));
    }
    // multibulk: *<count>\r\n then <count> bulk strings
    let count = read_integer_after('*', buf) ?? return Incomplete; // needs full "*N\r\n"
    let mut tokens = [];
    for _ in 0..count {
        expect('$');
        let len = read_integer_after('$', buf) ?? return Incomplete;
        if buf.remaining() < len + 2 { return Incomplete; }        // payload + CRLF not all here yet
        tokens.push(buf.take(len));
        buf.expect_crlf();
    }
    Complete(tokens)
}

fn execute(tokens, keyspace_tx) -> bytes {
    match uppercase(tokens[0]) {
        "PING" => if tokens.len() == 1 { "+PONG\r\n" } else { bulk(tokens[1]) },
        "ECHO" => bulk(tokens[1]),
        "SET"  => { keyspace_tx.send(Set(tokens[1], tokens[2])).await; "+OK\r\n" }
        "GET"  => {
            let v = keyspace_tx.send(Get(tokens[1])).await;
            v.map(bulk).unwrap_or("$-1\r\n")            // NULL bulk when absent
        }
        _      => "-ERR unknown command\r\n",
    }
}

fn bulk(s) -> bytes { "$" + len(s) + "\r\n" + s + "\r\n" }

// The single keyspace owner — commands serialized through here = atomicity for free.
async fn keyspace_owner(rx) {
    let mut map = HashMap::new();
    while let Some(msg) = rx.recv().await {
        match msg {
            Set(k, v) => { map.insert(k, v); reply(Ok); }
            Get(k)    => { reply(map.get(&k).cloned()); }
        }
    }
}
```

---

## Validation (do this for every milestone, forever)

**Never write your own client.** Drive your server with the official binary:

```bash
redis-cli -p 6379 ping            # -> PONG
redis-cli -p 6379 set foo bar     # -> OK
redis-cli -p 6379 get foo         # -> "bar"
redis-cli -p 6379 get nope        # -> (nil)
```

Then **stress the parser** — the test that separates real from toy: feed each command one byte at a
time, then all at once, then in random splits. Output must be byte-identical every time.

The unmodified official tool is both your motivation engine and your free conformance test. When M0+M1
work, you're off — open [ROADMAP.md](ROADMAP.md) and keep climbing.
