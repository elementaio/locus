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
use std::io::{self, BufWriter, Read, Write};

use crate::db::{Db, Stream, Value};

pub const DEFAULT_PATH: &str = "locus.rdb";
const MAGIC: &[u8; 9] = b"LOCUSRDB1";
/// Cap eager pre-allocation while loading so a hostile length/count prefix can't
/// force a huge up-front allocation (mirrors the RESP parser's ALLOC_CAP). The
/// collection still grows as elements are actually read.
const READ_ALLOC_CAP: usize = 1024;

/// Where to persist — overridable with the LOCUS_RDB env var (handy for tests).
pub fn configured_path() -> String {
    std::env::var("LOCUS_RDB").unwrap_or_else(|_| DEFAULT_PATH.to_string())
}

// --- save -------------------------------------------------------------------

/// Crash-safe write of a pre-serialized snapshot: temp file -> fsync -> atomic
/// rename -> directory fsync. Splitting this out lets BGSAVE serialize on the
/// hub (consistent) and hand the bytes to a background thread for the slow I/O.
pub fn write_snapshot(bytes: &[u8], path: &str) -> io::Result<()> {
    let tmp = format!("{path}.tmp");
    {
        let file = File::create(&tmp)?;
        let mut w = BufWriter::new(file);
        w.write_all(bytes)?;
        w.flush()?;
        w.get_ref().sync_all()?; // fsync the data before we rename
    }
    fs::rename(&tmp, path)?; // atomic replace
    fsync_parent_dir(path); // make the rename itself durable
    Ok(())
}

/// Best-effort fsync of the directory holding `path`, so a rename's metadata
/// survives a power loss too (the data file is already fsynced before the
/// rename). Logged, not fatal — some filesystems don't permit directory fsync.
/// Shared with the AOF rewrite path.
pub(crate) fn fsync_parent_dir(path: &str) {
    let parent = std::path::Path::new(path).parent();
    let dir = match parent {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => std::path::Path::new("."),
    };
    match File::open(dir) {
        Ok(d) => {
            if let Err(e) = d.sync_all() {
                crate::log::warn(&format!("fsync of dir {} failed: {e}", dir.display()));
            }
        }
        Err(e) => crate::log::warn(&format!("open dir {} for fsync failed: {e}", dir.display())),
    }
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
        Value::Cms(_) => 8,
        Value::TopK(_) => 9,
        Value::TDigest(_) => 10,
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
        Value::Cms(c) => {
            w.write_all(&c.width.to_le_bytes())?;
            w.write_all(&c.depth.to_le_bytes())?;
            write_bytes(w, &c.to_bytes())?;
        }
        Value::TopK(t) => write_bytes(w, &t.to_bytes())?, // self-describing blob
        Value::TDigest(t) => write_bytes(w, &t.to_bytes())?,
    }
    Ok(())
}

// --- load -------------------------------------------------------------------

/// Load a snapshot plus any trailing CDC / secondary-index state. The trailer is
/// optional: a snapshot written by `save` (or an older version) yields empty
/// extras. A missing file yields an empty Db (first run).
pub fn load_with_extras(path: &str) -> io::Result<(Db, Extras)> {
    let bytes = match fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            return Ok((Db::new(), Extras::default()));
        }
        Err(e) => return Err(e),
    };
    deserialize_with_extras(&bytes)
}

/// Shared keyspace reader: magic + count + entries.
fn read_db<R: Read>(r: &mut R) -> io::Result<Db> {
    let mut magic = [0u8; 9];
    r.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad RDB magic"));
    }
    let count = read_u64(r)?;
    let mut db = Db::new();
    for _ in 0..count {
        let expire = if read_u8(r)? == 1 {
            Some(read_u64(r)?)
        } else {
            None
        };
        let tag = read_u8(r)?;
        let key = read_bytes(r)?;
        let value = read_value(r, tag)?;
        db.insert_with_expire(key, value, expire);
    }
    Ok(db)
}

/// Parse a snapshot's keyspace and any optional trailing extras from a buffer.
pub fn deserialize_with_extras(bytes: &[u8]) -> io::Result<(Db, Extras)> {
    let mut r: &[u8] = bytes;
    let db = read_db(&mut r)?;
    let extras = read_extras(&mut r)?;
    Ok((db, extras))
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

// --- CDC / secondary-index trailer (optional, appended after the keyspace) ---

/// State that lives in the hub (not the Db) but must survive a restart: the
/// changefeed offset counter, the retained change-log, consumer-group cursors,
/// and secondary-index definitions (index contents are rebuilt from the keyspace
/// on load, so only the name+field are stored).
#[derive(Default)]
pub struct Extras {
    pub cdc_next_offset: u64,
    pub cdc_log: Vec<CdcRec>,
    pub cdc_groups: Vec<CdcGrp>,
    pub index_defs: Vec<(Vec<u8>, Vec<u8>)>, // (index name, hash field)
}

pub struct CdcRec {
    pub offset: u64,
    pub event: Vec<u8>,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

pub struct CdcGrp {
    pub name: Vec<u8>,
    pub last_delivered: u64,
    pub pending: Vec<(u64, Vec<u8>)>, // (offset, consumer)
}

const TRAILER_MAGIC: &[u8; 4] = b"LXT1";

fn put_bytes(buf: &mut Vec<u8>, b: &[u8]) {
    buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
    buf.extend_from_slice(b);
}

/// Append the trailer onto an already-serialized keyspace buffer.
pub fn append_extras(buf: &mut Vec<u8>, x: &Extras) {
    buf.extend_from_slice(TRAILER_MAGIC);
    buf.extend_from_slice(&x.cdc_next_offset.to_le_bytes());
    buf.extend_from_slice(&(x.cdc_log.len() as u32).to_le_bytes());
    for r in &x.cdc_log {
        buf.extend_from_slice(&r.offset.to_le_bytes());
        put_bytes(buf, &r.event);
        put_bytes(buf, &r.key);
        match &r.value {
            Some(v) => {
                buf.push(1);
                put_bytes(buf, v);
            }
            None => buf.push(0),
        }
    }
    buf.extend_from_slice(&(x.cdc_groups.len() as u32).to_le_bytes());
    for g in &x.cdc_groups {
        put_bytes(buf, &g.name);
        buf.extend_from_slice(&g.last_delivered.to_le_bytes());
        buf.extend_from_slice(&(g.pending.len() as u32).to_le_bytes());
        for (off, consumer) in &g.pending {
            buf.extend_from_slice(&off.to_le_bytes());
            put_bytes(buf, consumer);
        }
    }
    buf.extend_from_slice(&(x.index_defs.len() as u32).to_le_bytes());
    for (name, field) in &x.index_defs {
        put_bytes(buf, name);
        put_bytes(buf, field);
    }
}

/// Read the optional trailer. EOF at the marker (an older snapshot) yields empty
/// extras.
fn read_extras<R: Read>(r: &mut R) -> io::Result<Extras> {
    let mut marker = [0u8; 4];
    match r.read_exact(&mut marker) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(Extras::default()),
        Err(e) => return Err(e),
    }
    if &marker != TRAILER_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "bad RDB trailer magic",
        ));
    }
    let cdc_next_offset = read_u64(r)?;
    let mut cdc_log = Vec::new();
    for _ in 0..read_u32(r)? {
        let offset = read_u64(r)?;
        let event = read_bytes(r)?;
        let key = read_bytes(r)?;
        let value = if read_u8(r)? == 1 {
            Some(read_bytes(r)?)
        } else {
            None
        };
        cdc_log.push(CdcRec {
            offset,
            event,
            key,
            value,
        });
    }
    let mut cdc_groups = Vec::new();
    for _ in 0..read_u32(r)? {
        let name = read_bytes(r)?;
        let last_delivered = read_u64(r)?;
        let mut pending = Vec::new();
        for _ in 0..read_u32(r)? {
            let off = read_u64(r)?;
            pending.push((off, read_bytes(r)?));
        }
        cdc_groups.push(CdcGrp {
            name,
            last_delivered,
            pending,
        });
    }
    let mut index_defs = Vec::new();
    for _ in 0..read_u32(r)? {
        let name = read_bytes(r)?;
        let field = read_bytes(r)?;
        index_defs.push((name, field));
    }
    Ok(Extras {
        cdc_next_offset,
        cdc_log,
        cdc_groups,
        index_defs,
    })
}

/// Serialize the keyspace + extras and write it crash-safely.
pub fn save_with_extras(db: &Db, extras: &Extras, path: &str) -> io::Result<()> {
    let mut bytes = serialize(db);
    append_extras(&mut bytes, extras);
    write_snapshot(&bytes, path)
}

fn read_value<R: Read>(r: &mut R, tag: u8) -> io::Result<Value> {
    Ok(match tag {
        0 => Value::Str(read_bytes(r)?),
        1 => {
            let n = read_u32(r)?;
            let mut l = VecDeque::with_capacity(n.min(READ_ALLOC_CAP));
            for _ in 0..n {
                l.push_back(read_bytes(r)?);
            }
            Value::List(l)
        }
        2 => {
            let n = read_u32(r)?;
            let mut h = HashMap::with_capacity(n.min(READ_ALLOC_CAP));
            for _ in 0..n {
                let f = read_bytes(r)?;
                let v = read_bytes(r)?;
                h.insert(f, v);
            }
            Value::Hash(h)
        }
        3 => {
            let n = read_u32(r)?;
            let mut s = HashSet::with_capacity(n.min(READ_ALLOC_CAP));
            for _ in 0..n {
                s.insert(read_bytes(r)?);
            }
            Value::Set(s)
        }
        4 => {
            let n = read_u32(r)?;
            let mut z = HashMap::with_capacity(n.min(READ_ALLOC_CAP));
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
            let mut entries = Vec::with_capacity(n.min(READ_ALLOC_CAP));
            for _ in 0..n {
                let id = (read_u64(r)?, read_u64(r)?);
                let fc = read_u32(r)?;
                let mut fields = Vec::with_capacity(fc.min(READ_ALLOC_CAP));
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
        8 => {
            let width = read_u32(r)? as u32;
            let depth = read_u32(r)? as u32;
            let bytes = read_bytes(r)?;
            let cms = crate::sketch::Cms::from_bytes(width, depth, &bytes)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad CMS"))?;
            Value::Cms(cms)
        }
        9 => {
            let bytes = read_bytes(r)?;
            let tk = crate::sketch::TopK::from_bytes(&bytes)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad TopK"))?;
            Value::TopK(tk)
        }
        10 => {
            let bytes = read_bytes(r)?;
            let td = crate::sketch::TDigest::from_bytes(&bytes)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad TDigest"))?;
            Value::TDigest(td)
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
    // Read in bounded chunks rather than pre-allocating `n` (up to 4 GiB): a
    // truncated or hostile length then errors at EOF instead of OOM-aborting.
    let mut b = Vec::with_capacity(n.min(READ_ALLOC_CAP));
    let mut remaining = n;
    let mut chunk = [0u8; 8192];
    while remaining > 0 {
        let want = remaining.min(chunk.len());
        r.read_exact(&mut chunk[..want])?;
        b.extend_from_slice(&chunk[..want]);
        remaining -= want;
    }
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
        run(&mut db, &[b"CMSINCRBY", b"cm", b"x", b"7"]);
        run(&mut db, &[b"TOPKRESERVE", b"tk", b"3"]);
        run(&mut db, &[b"TOPKADD", b"tk", b"a", b"a", b"b"]);
        run(&mut db, &[b"TDADD", b"td", b"10", b"20", b"30"]);

        save_with_extras(&db, &Extras::default(), path).unwrap();
        let mut loaded = load_with_extras(path).unwrap().0;

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
        assert_eq!(
            execute(&to(&[b"CMSQUERY", b"cm", b"x"]), &mut loaded),
            b"*1\r\n:7\r\n".to_vec()
        );
        assert_eq!(
            execute(&to(&[b"TYPE", b"tk"]), &mut loaded),
            b"+topk\r\n".to_vec()
        );
        // a (count 2) ranks above b (count 1)
        assert_eq!(
            execute(&to(&[b"TOPKLIST", b"tk"]), &mut loaded),
            b"*2\r\n$1\r\na\r\n$1\r\nb\r\n".to_vec()
        );
        // t-digest survived (min is exact)
        assert_eq!(
            execute(&to(&[b"TDQUANTILE", b"td", b"0"]), &mut loaded),
            b"*1\r\n$2\r\n10\r\n".to_vec()
        );
        let _ = fs::remove_file(path);
    }

    fn to(parts: &[&[u8]]) -> Vec<Vec<u8>> {
        parts.iter().map(|p| p.to_vec()).collect()
    }

    #[test]
    fn extras_trailer_roundtrips() {
        let extras = Extras {
            cdc_next_offset: 42,
            cdc_log: vec![CdcRec {
                offset: 7,
                event: b"write".to_vec(),
                key: b"k".to_vec(),
                value: Some(b"v".to_vec()),
            }],
            cdc_groups: vec![CdcGrp {
                name: b"g".to_vec(),
                last_delivered: 5,
                pending: vec![(3, b"c1".to_vec())],
            }],
            index_defs: vec![(b"by_city".to_vec(), b"city".to_vec())],
        };
        let mut bytes = serialize(&Db::new());
        append_extras(&mut bytes, &extras);
        let (_, got) = deserialize_with_extras(&bytes).unwrap();
        assert_eq!(got.cdc_next_offset, 42);
        assert_eq!(got.cdc_log.len(), 1);
        assert_eq!(got.cdc_log[0].offset, 7);
        assert_eq!(got.cdc_log[0].value.as_deref(), Some(&b"v"[..]));
        assert_eq!(got.cdc_groups[0].pending, vec![(3u64, b"c1".to_vec())]);
        assert_eq!(
            got.index_defs,
            vec![(b"by_city".to_vec(), b"city".to_vec())]
        );
    }

    #[test]
    fn snapshot_without_trailer_yields_empty_extras() {
        // A plain serialized keyspace (the older format) has no trailer.
        let bytes = serialize(&Db::new());
        let (_, extras) = deserialize_with_extras(&bytes).unwrap();
        assert_eq!(extras.cdc_next_offset, 0);
        assert!(extras.index_defs.is_empty());
        assert!(extras.cdc_log.is_empty());
    }

    #[test]
    fn hostile_bulk_length_errors_not_aborts() {
        // count=1, no-expire, tag=0 (Str), key "k", value length = u32::MAX with
        // no data following: must error at EOF, never pre-allocate 4 GiB + abort.
        let mut b = Vec::new();
        b.extend_from_slice(MAGIC);
        b.extend_from_slice(&1u64.to_le_bytes());
        b.push(0); // no expire
        b.push(0); // tag = Str
        put_bytes(&mut b, b"k");
        b.extend_from_slice(&u32::MAX.to_le_bytes()); // value length
        assert!(deserialize_with_extras(&b).is_err());
    }

    #[test]
    fn hostile_collection_count_errors_not_aborts() {
        // A list declaring u32::MAX elements with none present.
        let mut b = Vec::new();
        b.extend_from_slice(MAGIC);
        b.extend_from_slice(&1u64.to_le_bytes());
        b.push(0);
        b.push(1); // tag = List
        put_bytes(&mut b, b"l");
        b.extend_from_slice(&u32::MAX.to_le_bytes()); // element count
        assert!(deserialize_with_extras(&b).is_err());
    }

    #[test]
    fn truncated_and_bad_magic_error() {
        let mut t = Vec::new();
        t.extend_from_slice(MAGIC);
        t.extend_from_slice(&5u64.to_le_bytes()); // claims 5 entries, none follow
        assert!(deserialize_with_extras(&t).is_err());
        assert!(deserialize_with_extras(b"NOTLOCUS-MAGIC").is_err());
    }
}
