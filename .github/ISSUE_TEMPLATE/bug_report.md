---
name: Bug report
about: A divergence from expected/Redis behavior, a crash, or incorrect output
title: "[bug] "
labels: bug
assignees: ""
---

## What happened

A clear description of the bug.

## Reproduction

The exact commands, ideally as `redis-cli` lines so they can be copy-pasted:

```console
$ redis-cli -p 6379 set foo bar
OK
$ redis-cli -p 6379 ...
```

## Expected behavior

What you expected to happen. If it differs from real Redis, a link to the Redis docs or the
`redis-cli` output from a real server is especially helpful.

## Environment

- Locus version or commit: <!-- e.g. v0.1.0 or git short SHA -->
- OS / arch: <!-- e.g. macOS 14 arm64, Ubuntu 24.04 x86_64 -->
- Rust version (`rustc --version`):
- Persistence mode: <!-- none / RDB / AOF -->, replication: <!-- standalone / master / replica -->

## Additional context

Logs, RDB/AOF state, or anything else relevant.
