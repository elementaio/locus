# Contributing to Locus

Thanks for your interest! Locus is intentionally small and readable, which makes it a friendly codebase
to contribute to.

## Ground rules

- **Keep it dependency-free.** Locus uses only the Rust standard library, on purpose. Please don't add
  third-party crates without a discussion first (open an issue).
- **Keep it readable.** Favor clear code over clever code; match the surrounding style; comment the
  *why*, not the *what*.
- **Stay true to the design.** Command execution is single-threaded (atomic by construction). Avoid
  introducing locks or shared mutable state on the data path — see
  [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).
- **Know which half you're in.** The package builds two targets: `src/lib.rs` is the `locusdb`
  **library** (the engine — keyspace, commands, codec, persistence, spatial index, sketches; no threads,
  no sockets) and `src/main.rs` is the `locus` **binary** (the server — hub thread, connections,
  replication, cluster, sentinel). Engine code goes in the library and must not reach back into the
  binary; server code stays in the binary. Anything the library exports is public API now, so a `pub`
  there is a commitment.

## Before you open a PR

```console
cargo fmt --check     # formatting
cargo clippy          # must be warning-free
cargo test            # all tests pass
```

CI runs all three on every push and PR.

Changing anything on the write path? Run the performance harness before and after — it is `#[ignore]`d,
so it never slows the normal suite down, and it prints a table you can paste straight into the PR:

```console
cargo test --release --test perf -- --ignored --nocapture
```

It measures the cases where a mistake actually shows: writes into a large collection, range reads out of
a big sorted set, and `GEOSEARCH` over a dense point cloud — against a real `redis-server` when one is
installed. Its assertions are deliberately *ratios* (writing into a 200k-element collection must stay
within 5× of writing into an empty one), so they catch per-write work that grows with the data instead
of flaking on a busy machine. `LOCUS_PERF_N` and `LOCUS_PERF_LIST` shrink it for a quick check.

## Adding a command

Most commands are small:

1. Add a `match` arm in `commands.rs` (or the relevant module) that dispatches to a focused function.
2. Implement it; return the right RESP reply and a `WRONGTYPE` error if it targets the wrong type.
3. If it's a write, make sure it's listed in `aof::is_write` so it's persisted/replicated — and if it's
   non-deterministic (random or time-based), rewrite it to a deterministic form in `aof::entries_for`.
4. Add a unit test.
5. Update [docs/COMMANDS.md](docs/COMMANDS.md).

## Reporting bugs

Open an issue with the command(s) involved and the exact `redis-cli` reproduction. Bugs that show a
divergence from Redis's documented behavior are especially welcome.
