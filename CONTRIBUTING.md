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

## Before you open a PR

```console
cargo fmt --check     # formatting
cargo clippy          # must be warning-free
cargo test            # all tests pass
```

CI runs all three on every push and PR.

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
