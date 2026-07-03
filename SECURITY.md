# Security Policy

## Project maturity

Locus is a young, from-scratch datastore (v0.1.x). It is a faithful, readable implementation —
**not yet production-hardened**. Please do not expose it directly to untrusted networks or use it to
store sensitive data without your own review. In particular, like Redis, Locus has **no
authentication or transport encryption** of its own: it trusts every client that can reach its port.
Bind it to `127.0.0.1` (the default) or run it behind a trusted network boundary.

## Supported versions

Only the latest release line receives security fixes while the project is pre-1.0.

| Version | Supported |
|---|---|
| 0.1.x | ✅ |
| < 0.1 | ❌ |

## Reporting a vulnerability

**Please do not open a public issue for security problems.**

Report privately through either channel:

- **GitHub** — open a private advisory via the repository's
  [Security → Report a vulnerability](https://github.com/elementaio/locus/security/advisories/new)
  tab (preferred).
- **Email** — emadjumaah@gmail.com, with `[locus security]` in the subject.

Please include:

- a description of the issue and its impact,
- the affected version or commit,
- a minimal reproduction (the exact `redis-cli` commands or a payload), and
- any suggested fix, if you have one.

## What to expect

- **Acknowledgement** within a few days.
- An assessment and, for confirmed issues, a fix on a best-effort timeline appropriate to severity.
- Credit in the release notes and advisory once a fix ships, unless you prefer to remain anonymous.

This is a personal open-source project maintained on a best-effort basis; there is no formal SLA, but
security reports are taken seriously and prioritized over feature work.
