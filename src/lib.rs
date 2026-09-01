//! **Locus** — an in-memory, geo-first datastore that speaks the Redis protocol,
//! written from scratch in pure `std` with zero third-party crates.
//!
//! This crate is the *engine*: the keyspace, the command implementations, the
//! RESP codec, the persistence formats, the spatial index and the sketches. It
//! is what you embed. The `locus` **binary** in the same package wraps this
//! engine in the server — the single hub thread that owns the keyspace, the
//! per-connection reader/writer threads, replication, clustering and sentinel.
//! Those live in `src/main.rs` and are deliberately *not* part of the library:
//! an embedder brings its own concurrency model.
//!
//! # Embedding
//!
//! The whole engine is one [`Db`] plus [`execute`]. Commands go in as RESP
//! argument vectors and replies come back as encoded RESP bytes — the same bytes
//! the server would put on a socket — so anything that can drive a Redis client
//! can drive this in-process, with no socket and no threads.
//!
//! ```
//! use locusdb::{Db, execute, resp};
//!
//! let mut db = Db::new();
//!
//! let ok = execute(&[b"SET".to_vec(), b"city".to_vec(), b"Palermo".to_vec()], &mut db);
//! assert_eq!(ok, resp::simple_string("OK"));
//!
//! let got = execute(&[b"GET".to_vec(), b"city".to_vec()], &mut db);
//! assert_eq!(got, resp::bulk_string(b"Palermo"));
//! ```
//!
//! Replies are RESP2 by default; [`execute_proto`] takes an explicit protocol
//! version (2 or 3) for the shape-sensitive commands (maps, sets, doubles).
//!
//! # What the library does *not* do for you
//!
//! [`Db`] is a plain owned value with `&mut self` methods — there is no interior
//! locking, because the server does not need any (one thread owns the keyspace,
//! by design; see `plans/DESIGN-PRINCIPLES.md`). An embedder sharing a `Db`
//! across threads must supply its own mutual exclusion. Likewise, commands that
//! only mean something to a *server* — replication, `SUBSCRIBE`, blocking pops,
//! the changefeed's group plumbing — are handled by the binary's hub, not by
//! [`execute`], which answers them the way a bare keyspace can.
//!
//! Expiry is lazy on read plus an active sweep the server drives from its
//! maintenance tick; an embedder that wants keys to actually disappear should
//! call [`Db::active_expire`] periodically.

// The engine. Every module is public: this crate exists so that embedders,
// fuzz targets and out-of-crate tests can reach the internals the binary uses.
pub mod acl;
pub mod aof;
pub mod commands;
pub mod db;
pub mod geohash;
pub mod hlc;
pub mod log;
pub mod pubsub;
pub mod rdb;
pub mod resp;
pub mod sentinel;
pub mod sketch;
pub mod streams;
pub mod tier;
pub mod util;

// --- the curated surface ----------------------------------------------------
//
// Everything above is reachable by module path; these are the handful of names
// an embedder actually needs, lifted to the crate root.

pub use commands::{execute, execute_proto};
pub use db::{Db, Value, ZSet, now_ms};
pub use resp::{Parsed, parse_command};
pub use util::ct_eq;
