<!-- Thanks for contributing to Locus! Keep changes small and focused. -->

## Summary

What this PR does and why.

Closes #<!-- issue number, if any -->

## Changes

-

## Checklist

- [ ] `cargo fmt --check` is clean
- [ ] `cargo clippy --all-targets -- -D warnings` is warning-free
- [ ] `cargo test` passes
- [ ] Added or updated tests for the change
- [ ] No new third-party dependencies (Locus is `std`-only by design — open an issue first if needed)
- [ ] If a new command: dispatch arm added, `WRONGTYPE` handled, and — for writes — listed in
      `aof::is_write` (and made deterministic in `aof::entries_for` if random/time-based)
- [ ] Updated docs ([docs/COMMANDS.md](../docs/COMMANDS.md) / [CHANGELOG.md](../CHANGELOG.md)) as needed

## Notes for reviewers

Anything non-obvious, trade-offs, or areas you'd like extra eyes on.
