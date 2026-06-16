//! RDB-style snapshot persistence.
//!
//! Serializes the whole keyspace (all types + per-key expiry) to a compact,
//! length-prefixed binary file, and loads it on startup. The write is crash-safe
//! by construction: we write a temp file, fsync it, then atomically rename over
//! the target — so a half-written snapshot can never replace a good one.
//!
//! (Real Redis fork()s and dumps via copy-on-write; here we serialize inline on
//! the owner thread. Same on-disk guarantee, simpler mechanism.)

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Write};

use crate::db::{Db, Stream, Value};

pub const DEFAULT_PATH: &str = "locus.rdb";
const MAGIC: &[u8; 9] = b"LOCUSRDB1";

/// Where to persist — overridable with the LOCUS_RDB env var (handy for tests).
pub fn configured_path() -> String {
    std::env::var("LOCUS_RDB").unwrap_or_else(|_| DEFAULT_PATH.to_string())
}

// --- save -------------------------------------------------------------------

pub fn save(db: &Db, path: &str) -> io::Result<()> {
    let tmp = format!("{path}.tmp");
    {
        let file = File::create(&tmp)?;
        let mut w = BufWriter::new(file);
        w.write_all(MAGIC)?;
        w.write_all(&(db.entries().count() as u64).to_le_bytes())?;
        for (key, value) in db.entries() {
            match db.raw_expire(key) {
                Some(deadline) => {
                    w.write_all(&[1])?;
                    w.write_all(&deadline.to_le_bytes())?;
                }
                None => w.write_all(&[0])?,
            }
            write_value(&mut w, key, value)?;
        }
        w.flush()?;
        w.get_ref().sync_all()?; // fsync the data before we rename
    }
    fs::rename(&tmp, path)?; // atomic replace
    Ok(())
}

fn write_bytes<W: Write>(w: &mut W, b: &[u8]) -> io::Result<()> {
    w.write_all(&(b.len() as u32).to_le_bytes())?;
    w.write_all(b)
}

fn write_value<W: Write>(w: &mut W, key: &[u8], v: &Value) -> io::Result<()> {
    let tag: u8 = match v {
        Value::Str(_) => 0,
        Value::List(_) => 1,
        Value::Hash(_) => 2,
        Value::Set(_) => 3,
        Value::ZSet(_) => 4,
        Value::Stream(_) => 5,
        Value::Geo(..) => 6,
        Value::Bloom(_) => 7,
    };
    w.write_all(&[tag])?;
    write_bytes(w, key)?;
    match v {
        Value::Str(s) => write_bytes(w, s)?,
        Value::List(l) => {
            w.write_all(&(l.len() as u32).to_le_bytes())?;
            for item in l {
                write_bytes(w, item)?;
            }
        }
        Value::Hash(h) => {
            w.write_all(&(h.len() as u32).to_le_bytes())?;
            for (f, val) in h {
                write_bytes(w, f)?;
                write_bytes(w, val)?;
            }
        }
        Value::Set(s) => {
            w.write_all(&(s.len() as u32).to_le_bytes())?;
            for m in s {
                write_bytes(w, m)?;
            }
        }
        Value::ZSet(z) => {
            w.write_all(&(z.len() as u32).to_le_bytes())?;
            for (m, score) in z {
                write_bytes(w, m)?;
                w.write_all(&score.to_le_bytes())?;
            }
        }
        Value::Stream(s) => {
            w.write_all(&(s.entries.len() as u32).to_le_bytes())?;
            w.write_all(&s.last_id.0.to_le_bytes())?;
            w.write_all(&s.last_id.1.to_le_bytes())?;
            for (id, fields) in &s.entries {
                w.write_all(&id.0.to_le_bytes())?;
                w.write_all(&id.1.to_le_bytes())?;
                w.write_all(&(fields.len() as u32).to_le_bytes())?;
                for (f, v) in fields {
                    write_bytes(w, f)?;
                    write_bytes(w, v)?;
                }
            }
        }
        Value::Geo(lon, lat) => {
            w.write_all(&lon.to_le_bytes())?;
            w.write_all(&lat.to_le_bytes())?;
        }
        Value::Bloom(b) => {
            w.write_all(&[b.k])?;
            w.write_all(&b.nbits.to_le_bytes())?;
            write_bytes(w, &b.bits)?;
        }
    }
    Ok(())
}

// --- load -------------------------------------------------------------------

/// Load a snapshot. A missing file yields an empty Db (first run).
pub fn load(path: &str) -> io::Result<Db> {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Db::new()),
        Err(e) => return Err(e),
    };
    let mut r = BufReader::new(file);
    let mut magic = [0u8; 9];
    r.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad RDB magic"));
    }
    let count = read_u64(&mut r)?;
    let mut db = Db::new();
    for _ in 0..count {
        let expire = if read_u8(&mut r)? == 1 {
            Some(read_u64(&mut r)?)
        } else {
            None
        };
        let tag = read_u8(&mut r)?;
        let key = read_bytes(&mut r)?;
        let value = read_value(&mut r, tag)?;
        db.insert_with_expire(key, value, expire);
    }
    Ok(db)
}

/// Serialize the whole dataset to an in-memory buffer (for replication sync).
pub fn serialize(db: &Db) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(MAGIC);
    buf.extend_from_slice(&(db.entries().count() as u64).to_le_bytes());
    for (key, value) in db.entries() {
        match db.raw_expire(key) {
            Some(deadline) => {
                buf.push(1);
                buf.extend_from_slice(&deadline.to_le_bytes());
            }
            None => buf.push(0),
        }
        write_value(&mut buf, key, value).expect("writing to a Vec is infallible");
    }
    buf
}

/// Rebuild a dataset from a serialized buffer (the replica side of sync).
pub fn deserialize(bytes: &[u8]) -> io::Result<Db> {
    let mut r: &[u8] = bytes;
    let mut magic = [0u8; 9];
    r.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad RDB magic"));
    }
    let count = read_u64(&mut r)?;
    let mut db = Db::new();
    for _ in 0..count {
        let expire = if read_u8(&mut r)? == 1 {
            Some(read_u64(&mut r)?)
        } else {
            None
        };
        let tag = read_u8(&mut r)?;
        let key = read_bytes(&mut r)?;
        let value = read_value(&mut r, tag)?;
        db.insert_with_expire(key, value, expire);
    }
    Ok(db)
}

fn read_value<R: Read>(r: &mut R, tag: u8) -> io::Result<Value> {
    Ok(match tag {
        0 => Value::Str(read_bytes(r)?),
        1 => {
            let n = read_u32(r)?;
            let mut l = VecDeque::with_capacity(n);
            for _ in 0..n {
                l.push_back(read_bytes(r)?);
            }
            Value::List(l)
        }
        2 => {
            let n = read_u32(r)?;
            let mut h = HashMap::with_capacity(n);
            for _ in 0..n {
                let f = read_bytes(r)?;
                let v = read_bytes(r)?;
                h.insert(f, v);
            }
            Value::Hash(h)
        }
        3 => {
            let n = read_u32(r)?;
            let mut s = HashSet::with_capacity(n);
            for _ in 0..n {
                s.insert(read_bytes(r)?);
            }
            Value::Set(s)
        }
        4 => {
            let n = read_u32(r)?;
            let mut z = HashMap::with_capacity(n);
            for _ in 0..n {
                let m = read_bytes(r)?;
                let score = read_f64(r)?;
                z.insert(m, score);
            }
            Value::ZSet(z)
        }
        5 => {
            let n = read_u32(r)?;
            let last_id = (read_u64(r)?, read_u64(r)?);
            let mut entries = Vec::with_capacity(n);
            for _ in 0..n {
                let id = (read_u64(r)?, read_u64(r)?);
                let fc = read_u32(r)?;
                let mut fields = Vec::with_capacity(fc);
                for _ in 0..fc {
                    let f = read_bytes(r)?;
                    let v = read_bytes(r)?;
                    fields.push((f, v));
                }
                entries.push((id, fields));
            }
            Value::Stream(Stream { entries, last_id })
        }
        6 => Value::Geo(read_f64(r)?, read_f64(r)?),
        7 => {
            let k = read_u8(r)?;
            let nbits = read_u64(r)?;
            let bits = read_bytes(r)?;
            Value::Bloom(crate::sketch::Bloom::from_raw(k, nbits, bits))
        }
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unknown type tag",
            ));
        }
    })
}

fn read_u8<R: Read>(r: &mut R) -> io::Result<u8> {
    let mut b = [0u8; 1];
    r.read_exact(&mut b)?;
    Ok(b[0])
}
fn read_u32<R: Read>(r: &mut R) -> io::Result<usize> {
    let mut b = [0u8; 4];
    r.read_exact(&mut b)?;
    Ok(u32::from_le_bytes(b) as usize)
}
fn read_u64<R: Read>(r: &mut R) -> io::Result<u64> {
    let mut b = [0u8; 8];
    r.read_exact(&mut b)?;
    Ok(u64::from_le_bytes(b))
}
fn read_f64<R: Read>(r: &mut R) -> io::Result<f64> {
    let mut b = [0u8; 8];
    r.read_exact(&mut b)?;
    Ok(f64::from_le_bytes(b))
}
fn read_bytes<R: Read>(r: &mut R) -> io::Result<Vec<u8>> {
    let n = read_u32(r)?;
    let mut b = vec![0u8; n];
    r.read_exact(&mut b)?;
    Ok(b)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::execute;

    #[test]
    fn snapshot_roundtrip() {
        let path = "/tmp/locus_rdb_test.rdb";
        let _ = fs::remove_file(path);
        let mut db = Db::new();
        let run = |db: &mut Db, parts: &[&[u8]]| {
            let t: Vec<Vec<u8>> = parts.iter().map(|p| p.to_vec()).collect();
            execute(&t, db);
        };
        run(&mut db, &[b"SET", b"s", b"hello"]);
        run(&mut db, &[b"RPUSH", b"l", b"a", b"b", b"c"]);
        run(&mut db, &[b"HSET", b"h", b"f", b"v"]);
        run(&mut db, &[b"SADD", b"st", b"x", b"y"]);
        run(&mut db, &[b"ZADD", b"z", b"1.5", b"m"]);
        run(&mut db, &[b"SET", b"e", b"v", b"EX", b"1000"]);
        run(&mut db, &[b"GEOSET", b"g", b"13.361389", b"38.115556"]);
        run(&mut db, &[b"BFADD", b"bf", b"alice"]);

        save(&db, path).unwrap();
        let mut loaded = load(path).unwrap();

        assert_eq!(
            execute(&to(&[b"GET", b"s"]), &mut loaded),
            b"$5\r\nhello\r\n".to_vec()
        );
        assert_eq!(
            execute(&to(&[b"LLEN", b"l"]), &mut loaded),
            b":3\r\n".to_vec()
        );
        assert_eq!(
            execute(&to(&[b"HGET", b"h", b"f"]), &mut loaded),
            b"$1\r\nv\r\n".to_vec()
        );
        assert_eq!(
            execute(&to(&[b"SCARD", b"st"]), &mut loaded),
            b":2\r\n".to_vec()
        );
        assert_eq!(
            execute(&to(&[b"ZSCORE", b"z", b"m"]), &mut loaded),
            b"$3\r\n1.5\r\n".to_vec()
        );
        // TTL survived (roughly)
        let ttl = execute(&to(&[b"TTL", b"e"]), &mut loaded);
        assert!(ttl.starts_with(b":") && ttl != b":-1\r\n".to_vec() && ttl != b":-2\r\n".to_vec());
        // geo point + bloom survived
        assert_eq!(
            execute(&to(&[b"TYPE", b"g"]), &mut loaded),
            b"+geo\r\n".to_vec()
        );
        assert_eq!(
            execute(&to(&[b"BFEXISTS", b"bf", b"alice"]), &mut loaded),
            b":1\r\n".to_vec()
        );
        let _ = fs::remove_file(path);
    }

    fn to(parts: &[&[u8]]) -> Vec<Vec<u8>> {
        parts.iter().map(|p| p.to_vec()).collect()
    }
}
