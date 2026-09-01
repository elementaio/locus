//! The library seam, exercised from outside the crate.
//!
//! Session 7 (phase 5, item 5.1) split Locus into a `locusdb` library — the
//! engine — and a `locus` binary — the server. This file is the first external
//! consumer of that library: it drives the keyspace entirely in-process, with
//! no socket, no hub thread and no server at all. If the public API ever stops
//! being enough to embed Locus, these tests stop compiling.
//!
//! Everything asserted here is the *engine's* behaviour. Commands that only
//! mean something to a server (replication, SUBSCRIBE, blocking pops, the
//! changefeed's group plumbing) belong to the binary's hub and are covered by
//! `tests/integration.rs`, which drives a real spawned server over TCP.

use locusdb::{Db, Value, db::now_ms, execute, execute_proto, geohash, resp};

/// Encode command tokens the way a caller naturally writes them.
fn cmd(parts: &[&str]) -> Vec<Vec<u8>> {
    parts.iter().map(|p| p.as_bytes().to_vec()).collect()
}

/// The headline: `SET k v` then `GET k`, in-process, through the public API.
#[test]
fn set_then_get_in_process() {
    let mut db = Db::new();

    assert_eq!(
        execute(&cmd(&["SET", "k", "v"]), &mut db),
        resp::simple_string("OK")
    );
    assert_eq!(
        execute(&cmd(&["GET", "k"]), &mut db),
        resp::bulk_string(b"v")
    );
    // A miss is a null bulk, not an empty one.
    assert_eq!(execute(&cmd(&["GET", "nope"]), &mut db), resp::null_bulk());
    // And the reply bytes really are the wire bytes the server would emit.
    assert_eq!(
        execute(&cmd(&["GET", "k"]), &mut db),
        b"$1\r\nv\r\n".to_vec()
    );
}

/// `Db::default()` is the same empty keyspace as `Db::new()` — the impl session
/// 7 added so embedders get the idiomatic constructor pair.
#[test]
fn db_default_matches_new() {
    let mut db = Db::default();
    assert_eq!(execute(&cmd(&["DBSIZE"]), &mut db), resp::integer(0));
    execute(&cmd(&["SET", "k", "v"]), &mut db);
    assert_eq!(execute(&cmd(&["DBSIZE"]), &mut db), resp::integer(1));
}

/// The codec is reachable too, so an embedder can feed the engine raw RESP —
/// exactly what a fuzz target or a proxy would do.
#[test]
fn raw_resp_bytes_drive_the_engine() {
    let mut db = Db::new();
    let wire = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$2\r\nhi\r\n";

    match resp::parse_command(wire) {
        resp::Parsed::Complete(tokens, used) => {
            assert_eq!(used, wire.len());
            assert_eq!(execute(&tokens, &mut db), resp::simple_string("OK"));
        }
        other => panic!("expected a complete command, got {other:?}"),
    }
    assert_eq!(
        execute(&cmd(&["GET", "k"]), &mut db),
        resp::bulk_string(b"hi")
    );

    // A partial frame is Incomplete, not an error — the resumable contract the
    // connection reader depends on.
    assert!(matches!(
        resp::parse_command(&wire[..10]),
        resp::Parsed::Incomplete
    ));
}

/// RESP3 typing is selectable per call, without a HELLO or a connection.
#[test]
fn execute_proto_selects_resp3_shapes() {
    let mut db = Db::new();
    execute(&cmd(&["HSET", "h", "f", "v"]), &mut db);

    let two = execute_proto(&cmd(&["HGETALL", "h"]), &mut db, 2);
    let three = execute_proto(&cmd(&["HGETALL", "h"]), &mut db, 3);
    assert_eq!(two, b"*2\r\n$1\r\nf\r\n$1\r\nv\r\n".to_vec());
    assert_eq!(three, b"%1\r\n$1\r\nf\r\n$1\r\nv\r\n".to_vec()); // map type
}

/// The flagship, embedded: the geo-first model and the spatial index, in-process.
#[test]
fn geo_index_works_embedded() {
    let mut db = Db::new();
    execute(
        &cmd(&["GEOSET", "Palermo", "13.361389", "38.115556"]),
        &mut db,
    );
    execute(
        &cmd(&["GEOSET", "Catania", "15.087269", "37.502669"]),
        &mut db,
    );

    let near = execute(
        &cmd(&[
            "GEOSEARCH",
            "FROMKEY",
            "Palermo",
            "BYRADIUS",
            "100",
            "km",
            "ASC",
        ]),
        &mut db,
    );
    assert_eq!(near, resp::bulk_array(&[b"Palermo".to_vec()]));

    let both = execute(
        &cmd(&[
            "GEOSEARCH",
            "FROMKEY",
            "Palermo",
            "BYRADIUS",
            "200",
            "km",
            "ASC",
        ]),
        &mut db,
    );
    assert_eq!(
        both,
        resp::bulk_array(&[b"Palermo".to_vec(), b"Catania".to_vec()])
    );

    // The 52-bit cell id the spatial index is keyed by is public as well — the
    // shard key, so a client-side router can compute it without the server.
    let palermo = geohash::encode(13.361389, 38.115556);
    let catania = geohash::encode(15.087269, 37.502669);
    assert_ne!(palermo, catania);
    // Same 10-bit prefix cell only if they were within the same coarse cell.
    assert_eq!(
        geohash::cell(13.361389, 38.115556, 52),
        palermo,
        "cell() at full precision is encode()"
    );
}

/// The keyspace itself is a plain owned value: an embedder can read it directly
/// instead of going through a command, which is the point of exporting `Value`.
#[test]
fn db_and_value_are_directly_inspectable() {
    let mut db = Db::new();
    execute(&cmd(&["RPUSH", "l", "a", "b", "c"]), &mut db);

    match db.get("l".as_bytes()) {
        Some(Value::List(items)) => {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], b"a");
        }
        other => panic!("expected a list, got {:?}", other.map(|v| v.type_name())),
    }

    // Expiry is data an embedder can see, and `now_ms` is the clock it is in.
    execute(&cmd(&["PEXPIRE", "l", "100000"]), &mut db);
    let at = db.expire_at("l".as_bytes()).expect("ttl set");
    assert!(at > now_ms(), "expiry should be in the future");
}
