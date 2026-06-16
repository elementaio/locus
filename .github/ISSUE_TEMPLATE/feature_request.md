---
name: Feature request
about: Suggest a command, behavior, or capability
title: "[feature] "
labels: enhancement
assignees: ""
---

## The idea

What you'd like Locus to do.

## Motivation

The problem it solves or the use case it enables.

## Proposed behavior

If it's a command, sketch the syntax and replies (matching Redis where one exists):

```console
$ redis-cli -p 6379 NEWCMD key arg
...
```

## Scope & fit

Locus is intentionally small, dependency-free (`std` only), and single-threaded on the data path —
see [docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md) and [CONTRIBUTING.md](../../CONTRIBUTING.md).
Note any tension with those constraints, or whether this is already on
[docs/ROADMAP.md](../../docs/ROADMAP.md).

## Alternatives considered

Other approaches you weighed, if any.
