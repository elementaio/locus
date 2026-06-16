//! Streams: an append-only log type (XADD/XLEN/XRANGE/XREVRANGE/XREAD).
//!
//! Each entry has a monotonic `ms-seq` id and a list of field/value pairs.
//! Blocking XREAD is handled in the hub (it must park the client); the rest is
//! pure data operations here. (Consumer groups are deferred.)

use crate::db::{Db, Stream, StreamId, Value, now_ms};
use crate::resp::{array, bulk_array, bulk_string, error, integer, null_array};

pub fn fmt_id(id: StreamId) -> Vec<u8> {
    format!("{}-{}", id.0, id.1).into_bytes()
}

fn parse_id(s: &[u8], default_seq: u64) -> Option<StreamId> {
    let txt = std::str::from_utf8(s).ok()?;
    match txt.split_once('-') {
        Some((ms, seq)) => Some((ms.parse().ok()?, seq.parse().ok()?)),
        None => Some((txt.parse().ok()?, default_seq)),
    }
}

fn parse_usize(arg: Option<&Vec<u8>>) -> Option<usize> {
    std::str::from_utf8(arg?).ok()?.parse().ok()
}

fn wrongtype() -> Vec<u8> {
    error("WRONGTYPE Operation against a key holding the wrong kind of value")
}

/// Compute the new entry id for XADD from the id argument and the last id.
fn gen_id(arg: &[u8], last: StreamId) -> Result<StreamId, String> {
    let txt = std::str::from_utf8(arg).map_err(|_| "Invalid stream ID".to_string())?;
    if txt == "*" {
        let ms = now_ms().max(last.0);
        let seq = if ms == last.0 { last.1 + 1 } else { 0 };
        return Ok((ms, seq));
    }
    let (ms_part, seq_part) = match txt.split_once('-') {
        Some((m, s)) => (m, Some(s)),
        None => (txt, None),
    };
    let ms: u64 = ms_part
        .parse()
        .map_err(|_| "Invalid stream ID specified as stream command argument".to_string())?;
    let id = match seq_part {
        None | Some("*") => {
            if ms == last.0 {
                (ms, last.1 + 1)
            } else if ms > last.0 {
                (ms, 0)
            } else {
                return Err(
                    "The ID specified in XADD is equal or smaller than the target stream top item"
                        .into(),
                );
            }
        }
        Some(s) => {
            let seq: u64 = s.parse().map_err(|_| {
                "Invalid stream ID specified as stream command argument".to_string()
            })?;
            (ms, seq)
        }
    };
    if id == (0, 0) {
        return Err("The ID specified in XADD must be greater than 0-0".into());
    }
    if id <= last && last != (0, 0) {
        return Err(
            "The ID specified in XADD is equal or smaller than the target stream top item".into(),
        );
    }
    Ok(id)
}

pub fn xadd(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    // XADD key id field value [field value ...]
    if tokens.len() < 5 || tokens.len().is_multiple_of(2) {
        return error("ERR wrong number of arguments for 'xadd' command");
    }
    let key = &tokens[1];
    // Determine the id without creating an empty stream on error.
    let last = match db.get(key) {
        Some(Value::Stream(s)) => s.last_id,
        Some(_) => return wrongtype(),
        None => (0, 0),
    };
    let id = match gen_id(&tokens[2], last) {
        Ok(id) => id,
        Err(e) => return error(&format!("ERR {e}")),
    };
    let fields: Vec<(Vec<u8>, Vec<u8>)> = tokens[3..]
        .chunks(2)
        .map(|c| (c[0].clone(), c[1].clone()))
        .collect();
    match db.get_or_insert_with(key, || Value::Stream(Stream::new())) {
        Value::Stream(s) => {
            s.entries.push((id, fields));
            s.last_id = id;
            bulk_string(&fmt_id(id))
        }
        _ => wrongtype(),
    }
}

pub fn xlen(db: &mut Db, tokens: &[Vec<u8>]) -> Vec<u8> {
    if tokens.len() != 2 {
        return error("ERR wrong number of arguments for 'xlen' command");
    }
    match db.get(&tokens[1]) {
        None => integer(0),
        Some(Value::Stream(s)) => integer(s.entries.len() as i64),
        Some(_) => wrongtype(),
    }
}

fn bound(arg: &[u8], is_end: bool) -> Option<StreamId> {
    match arg {
        b"-" => Some((0, 0)),
        b"+" => Some((u64::MAX, u64::MAX)),
        _ => parse_id(arg, if is_end { u64::MAX } else { 0 }),
    }
}

fn entry_reply(id: StreamId, fields: &[(Vec<u8>, Vec<u8>)]) -> Vec<u8> {
    let mut flat = Vec::with_capacity(fields.len() * 2);
    for (f, v) in fields {
        flat.push(f.clone());
        flat.push(v.clone());
    }
    let mut o = b"*2\r\n".to_vec();
    o.extend_from_slice(&bulk_string(&fmt_id(id)));
    o.extend_from_slice(&bulk_array(&flat));
    o
}

pub fn xrange(db: &mut Db, tokens: &[Vec<u8>], rev: bool) -> Vec<u8> {
    // XRANGE key start end [COUNT n] ; XREVRANGE key end start [COUNT n]
    if tokens.len() != 4 && tokens.len() != 6 {
        return error("ERR wrong number of arguments");
    }
    let count = if tokens.len() == 6 {
        if !tokens[4].eq_ignore_ascii_case(b"COUNT") {
            return error("ERR syntax error");
        }
        match parse_usize(tokens.get(5)) {
            Some(c) => Some(c),
            None => return error("ERR value is not an integer or out of range"),
        }
    } else {
        None
    };
    let (start_arg, end_arg) = if rev {
        (&tokens[3], &tokens[2])
    } else {
        (&tokens[2], &tokens[3])
    };
    let (start, end) = match (bound(start_arg, false), bound(end_arg, true)) {
        (Some(a), Some(b)) => (a, b),
        _ => return error("ERR Invalid stream ID specified as stream command argument"),
    };
    match db.get(&tokens[1]) {
        None => array(&[]),
        Some(Value::Stream(s)) => {
            let mut matched: Vec<Vec<u8>> = s
                .entries
                .iter()
                .filter(|(id, _)| *id >= start && *id <= end)
                .map(|(id, f)| entry_reply(*id, f))
                .collect();
            if rev {
                matched.reverse();
            }
            if let Some(c) = count {
                matched.truncate(c);
            }
            array(&matched)
        }
        Some(_) => wrongtype(),
    }
}

// --- XREAD (parsing + collecting; blocking lives in the hub) ----------------

pub enum IdSpec {
    Explicit(StreamId),
    New, // "$" — only entries arriving after now
}

pub struct XReadReq {
    pub count: Option<usize>,
    pub block: Option<u64>,
    pub streams: Vec<(Vec<u8>, IdSpec)>,
}

pub fn parse_xread(tokens: &[Vec<u8>]) -> Result<XReadReq, String> {
    let (mut count, mut block) = (None, None);
    let mut i = 1;
    loop {
        let kw = tokens.get(i).ok_or("syntax error")?.to_ascii_uppercase();
        match kw.as_slice() {
            b"COUNT" => {
                count = Some(parse_usize(tokens.get(i + 1)).ok_or("value is not an integer")?);
                i += 2;
            }
            b"BLOCK" => {
                let ms = std::str::from_utf8(tokens.get(i + 1).ok_or("syntax error")?)
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                    .ok_or("timeout is not an integer or out of range")?;
                block = Some(ms);
                i += 2;
            }
            b"STREAMS" => {
                i += 1;
                break;
            }
            _ => return Err("syntax error".into()),
        }
    }
    let rest = &tokens[i..];
    if rest.is_empty() || !rest.len().is_multiple_of(2) {
        return Err(
            "Unbalanced XREAD list of streams: for each stream key an ID or '$' must be specified."
                .into(),
        );
    }
    let n = rest.len() / 2;
    let mut streams = Vec::with_capacity(n);
    for j in 0..n {
        let key = rest[j].clone();
        let idarg = &rest[n + j];
        let spec = if idarg.as_slice() == b"$" {
            IdSpec::New
        } else {
            IdSpec::Explicit(parse_id(idarg, 0).ok_or("Invalid stream ID")?)
        };
        streams.push((key, spec));
    }
    Ok(XReadReq {
        count,
        block,
        streams,
    })
}

/// Resolve "$" specs to each stream's current last id (snapshot for blocking).
pub fn resolve_specs(db: &mut Db, req: &XReadReq) -> Vec<(Vec<u8>, StreamId)> {
    req.streams
        .iter()
        .map(|(key, spec)| {
            let after = match spec {
                IdSpec::Explicit(id) => *id,
                IdSpec::New => match db.get(key) {
                    Some(Value::Stream(s)) => s.last_id,
                    _ => (0, 0),
                },
            };
            (key.clone(), after)
        })
        .collect()
}

/// Collect entries strictly after each spec's id. Returns None if all empty.
pub fn xread_collect(
    db: &mut Db,
    specs: &[(Vec<u8>, StreamId)],
    count: Option<usize>,
) -> Option<Vec<u8>> {
    let mut out: Vec<Vec<u8>> = Vec::new();
    for (key, after) in specs {
        if let Some(Value::Stream(s)) = db.get(key) {
            let mut entries: Vec<Vec<u8>> = s
                .entries
                .iter()
                .filter(|(id, _)| id > after)
                .map(|(id, f)| entry_reply(*id, f))
                .collect();
            if let Some(c) = count {
                entries.truncate(c);
            }
            if !entries.is_empty() {
                let mut pair = b"*2\r\n".to_vec();
                pair.extend_from_slice(&bulk_string(key));
                pair.extend_from_slice(&array(&entries));
                out.push(pair);
            }
        }
    }
    if out.is_empty() {
        None
    } else {
        Some(array(&out))
    }
}

/// Used by the hub's non-blocking fall-through.
pub fn nil() -> Vec<u8> {
    null_array()
}
