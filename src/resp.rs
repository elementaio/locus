//! RESP2 wire protocol: the resumable parser and the reply encoders.
//!
//! The parser never assumes one read() == one command. It is a state machine
//! over a caller-owned buffer: feed bytes, get back one complete command + how
//! many bytes it consumed, or Incomplete (keep the tail and read more).

const MAX_ARRAY: i64 = 1024 * 1024;
const MAX_BULK_LEN: i64 = 512 * 1024 * 1024;
/// Cap eager pre-allocation so a small `*N` header can't force a huge `Vec`.
const ALLOC_CAP: usize = 1024;
/// An inline command line (non-`*` framing) is bounded like Redis (64 KiB) so a
/// client that never sends a CRLF can't grow the connection buffer without limit.
const MAX_INLINE_LEN: usize = 64 * 1024;

#[derive(Debug)]
pub enum Parsed {
    /// A full command: its tokens, and how many bytes it consumed.
    Complete(Vec<Vec<u8>>, usize),
    /// Not enough bytes yet — leave the buffer untouched.
    Incomplete,
    /// Malformed stream; framing is lost, the caller must close.
    Error(String),
}

enum Count {
    Ok(i64, usize),
    Incomplete,
    Bad,
}

fn find_crlf(buf: &[u8], from: usize) -> Option<usize> {
    let mut i = from;
    while i + 1 < buf.len() {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return Some(i);
        }
        i += 1;
    }
    None
}

fn read_count(buf: &[u8], pos: usize) -> Count {
    match find_crlf(buf, pos + 1) {
        None => Count::Incomplete,
        Some(cr) => match std::str::from_utf8(&buf[pos + 1..cr])
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
        {
            Some(n) => Count::Ok(n, cr + 2),
            None => Count::Bad,
        },
    }
}

/// Resumable validation state for one in-progress multibulk command. The reader
/// keeps one per connection so re-checking an incomplete command after each read
/// costs O(new bytes), not a re-scan (and re-copy) of the whole prefix — without
/// it, dribbling a huge multibulk in makes parsing O(N²) and pins a core.
///
/// Invariants the caller upholds: between `Incomplete` results the buffer is
/// only APPENDED to; after `Complete`/`Error` the cursor is reset (the parser
/// does this itself) and the consumed bytes may be drained.
#[derive(Default)]
pub struct Cursor {
    total: Option<i64>, // multibulk arg count, once its header line is complete
    done: i64,          // args fully validated so far
    pos: usize,         // byte offset where the next unvalidated element starts
}

/// Try to parse exactly one command from the front of `buf`.
pub fn parse_command(buf: &[u8]) -> Parsed {
    parse_command_cursor(buf, &mut Cursor::default())
}

/// [`parse_command`] with resumable state: validation progress survives an
/// `Incomplete` result, so the next call (after more bytes arrive) picks up
/// where this one stopped. Two phases: an arithmetic-only scan that validates
/// framing without copying, then — only once the whole command is present — one
/// pass that materializes the tokens.
pub fn parse_command_cursor(buf: &[u8], cur: &mut Cursor) -> Parsed {
    if buf.is_empty() {
        return Parsed::Incomplete;
    }
    if buf[0] != b'*' {
        return parse_inline(buf);
    }
    let fail = |cur: &mut Cursor, msg: &str| {
        *cur = Cursor::default();
        Parsed::Error(msg.into())
    };
    let total = match cur.total {
        Some(t) => t,
        None => match read_count(buf, 0) {
            Count::Ok(n, p) => {
                if n <= 0 {
                    return Parsed::Complete(Vec::new(), p);
                }
                if n > MAX_ARRAY {
                    return fail(cur, "invalid multibulk length");
                }
                cur.total = Some(n);
                cur.pos = p;
                n
            }
            Count::Incomplete => return Parsed::Incomplete,
            Count::Bad => return fail(cur, "invalid multibulk length"),
        },
    };
    while cur.done < total {
        if cur.pos >= buf.len() {
            return Parsed::Incomplete;
        }
        if buf[cur.pos] != b'$' {
            let msg = format!("expected '$', got '{}'", buf[cur.pos] as char);
            *cur = Cursor::default();
            return Parsed::Error(msg);
        }
        let (len, after_len) = match read_count(buf, cur.pos) {
            Count::Ok(n, p) => (n, p),
            Count::Incomplete => return Parsed::Incomplete,
            Count::Bad => return fail(cur, "invalid bulk length"),
        };
        if !(0..=MAX_BULK_LEN).contains(&len) {
            return fail(cur, "invalid bulk length");
        }
        let len = len as usize;
        if after_len + len + 2 > buf.len() {
            return Parsed::Incomplete;
        }
        if &buf[after_len + len..after_len + len + 2] != b"\r\n" {
            return fail(cur, "expected CRLF after bulk string");
        }
        cur.pos = after_len + len + 2;
        cur.done += 1;
    }
    // Fully validated: materialize the tokens in one pass. The re-walk is header
    // jumps only (offsets computed from the declared lengths), so it's O(args).
    let consumed = cur.pos;
    let mut tokens: Vec<Vec<u8>> = Vec::with_capacity((total as usize).min(ALLOC_CAP));
    let mut pos = match read_count(buf, 0) {
        Count::Ok(_, p) => p,
        _ => return fail(cur, "internal parse inconsistency"), // validated above
    };
    for _ in 0..total {
        let (len, after_len) = match read_count(buf, pos) {
            Count::Ok(n, p) => (n as usize, p),
            _ => return fail(cur, "internal parse inconsistency"), // validated above
        };
        tokens.push(buf[after_len..after_len + len].to_vec());
        pos = after_len + len + 2;
    }
    *cur = Cursor::default();
    Parsed::Complete(tokens, consumed)
}

fn parse_inline(buf: &[u8]) -> Parsed {
    match find_crlf(buf, 0) {
        None => {
            if buf.len() > MAX_INLINE_LEN {
                return Parsed::Error("too big inline request".into());
            }
            Parsed::Incomplete
        }
        Some(cr) => {
            let tokens = buf[..cr]
                .split(|b| matches!(b, b' ' | b'\t'))
                .filter(|s| !s.is_empty())
                .map(|s| s.to_vec())
                .collect();
            Parsed::Complete(tokens, cr + 2)
        }
    }
}

// --- Reply encoders ---------------------------------------------------------

pub fn simple_string(s: &str) -> Vec<u8> {
    format!("+{s}\r\n").into_bytes()
}

pub fn error(s: &str) -> Vec<u8> {
    format!("-{s}\r\n").into_bytes()
}

pub fn integer(n: i64) -> Vec<u8> {
    format!(":{n}\r\n").into_bytes()
}

/// A bulk string: `$<len>\r\n<bytes>\r\n` (binary-safe).
pub fn bulk_string(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(data.len() + 16);
    out.extend_from_slice(format!("${}\r\n", data.len()).as_bytes());
    out.extend_from_slice(data);
    out.extend_from_slice(b"\r\n");
    out
}

/// The null bulk string `$-1\r\n` — Redis's "nil".
pub fn null_bulk() -> Vec<u8> {
    b"$-1\r\n".to_vec()
}

/// The null array `*-1\r\n` (e.g. LPOP key count on a missing key).
pub fn null_array() -> Vec<u8> {
    b"*-1\r\n".to_vec()
}

/// An array of already-encoded elements: `*<n>\r\n` + each element verbatim.
/// Use this when elements are a mix (e.g. some bulk strings, some nils).
pub fn array(elements: &[Vec<u8>]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", elements.len()).into_bytes();
    for e in elements {
        out.extend_from_slice(e);
    }
    out
}

/// An array of bulk strings (the common case: LRANGE, SMEMBERS, HVALS, ...).
pub fn bulk_array(items: &[Vec<u8>]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", items.len()).into_bytes();
    for it in items {
        out.extend_from_slice(&bulk_string(it));
    }
    out
}

/// Encode a command as a RESP multibulk (used to stream commands to replicas).
pub fn command(parts: &[Vec<u8>]) -> Vec<u8> {
    bulk_array(parts)
}

// --- RESP3 typed replies (fall back to RESP2 shapes when proto < 3) ----------

/// A map from a flat `[k0, v0, k1, v1, ...]`: RESP3 `%N`, or a RESP2 flat array.
pub fn map(flat: &[Vec<u8>], proto: u8) -> Vec<u8> {
    if proto >= 3 {
        let mut out = format!("%{}\r\n", flat.len() / 2).into_bytes();
        for it in flat {
            out.extend_from_slice(&bulk_string(it));
        }
        out
    } else {
        bulk_array(flat)
    }
}

/// A set: RESP3 `~N`, or a RESP2 array.
pub fn set(items: &[Vec<u8>], proto: u8) -> Vec<u8> {
    if proto >= 3 {
        let mut out = format!("~{}\r\n", items.len()).into_bytes();
        for it in items {
            out.extend_from_slice(&bulk_string(it));
        }
        out
    } else {
        bulk_array(items)
    }
}

/// A double from already-formatted bytes: RESP3 `,<v>`, or a RESP2 bulk string.
pub fn double(s: &[u8], proto: u8) -> Vec<u8> {
    if proto >= 3 {
        let mut out = vec![b','];
        out.extend_from_slice(s);
        out.extend_from_slice(b"\r\n");
        out
    } else {
        bulk_string(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resp3_encoders_match_protocol() {
        let kv = [b"f".to_vec(), b"v".to_vec()];
        assert_eq!(map(&kv, 3), b"%1\r\n$1\r\nf\r\n$1\r\nv\r\n".to_vec());
        assert_eq!(map(&kv, 2), b"*2\r\n$1\r\nf\r\n$1\r\nv\r\n".to_vec());
        let items = [b"a".to_vec()];
        assert_eq!(set(&items, 3), b"~1\r\n$1\r\na\r\n".to_vec());
        assert_eq!(set(&items, 2), b"*1\r\n$1\r\na\r\n".to_vec());
        assert_eq!(double(b"1.5", 3), b",1.5\r\n".to_vec());
        assert_eq!(double(b"1.5", 2), b"$3\r\n1.5\r\n".to_vec());
    }

    fn complete(buf: &[u8]) -> (Vec<Vec<u8>>, usize) {
        match parse_command(buf) {
            Parsed::Complete(t, n) => (t, n),
            other => panic!("expected Complete, got {other:?}"),
        }
    }

    #[test]
    fn parses_a_set_command() {
        let buf = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        let (tokens, consumed) = complete(buf);
        assert_eq!(
            tokens,
            vec![b"SET".to_vec(), b"foo".to_vec(), b"bar".to_vec()]
        );
        assert_eq!(consumed, buf.len());
    }

    #[test]
    fn every_prefix_is_incomplete() {
        let full = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        for i in 0..full.len() {
            assert!(matches!(parse_command(&full[..i]), Parsed::Incomplete));
        }
    }

    #[test]
    fn pipelined_commands_drain_in_order() {
        let buf = b"*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n";
        let (t1, n1) = complete(buf);
        assert_eq!(t1, vec![b"PING".to_vec()]);
        let (t2, n2) = complete(&buf[n1..]);
        assert_eq!(t2, vec![b"ECHO".to_vec(), b"hi".to_vec()]);
        assert_eq!(n1 + n2, buf.len());
    }

    #[test]
    fn bad_framing_is_an_error() {
        assert!(matches!(parse_command(b"*1\r\n+OK\r\n"), Parsed::Error(_)));
    }

    #[test]
    fn inline_command_is_accepted() {
        let (tokens, consumed) = complete(b"PING\r\n");
        assert_eq!(tokens, vec![b"PING".to_vec()]);
        assert_eq!(consumed, 6);
    }

    #[test]
    fn unterminated_inline_is_bounded() {
        // No CRLF yet, under the cap -> wait for more bytes.
        assert!(matches!(parse_command(b"PING"), Parsed::Incomplete));
        // No CRLF and over the cap -> error (don't buffer unboundedly).
        let huge = vec![b'x'; MAX_INLINE_LEN + 1];
        assert!(matches!(parse_command(&huge), Parsed::Error(_)));
    }

    #[test]
    fn oversized_array_header_does_not_preallocate() {
        // A tiny header declaring a huge array must be Incomplete (waiting for
        // elements), not an eager multi-megabyte allocation, and not an error
        // until it actually exceeds MAX_ARRAY.
        assert!(matches!(parse_command(b"*1000000\r\n"), Parsed::Incomplete));
        assert!(matches!(parse_command(b"*1048577\r\n"), Parsed::Error(_)));
    }

    #[test]
    fn fuzz_parse_command_never_panics_and_makes_progress() {
        // Deterministic xorshift64 PRNG — no external crate.
        let mut state: u64 = 0x9e37_79b9_7f4a_7c15;
        let mut rng = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for _ in 0..100_000 {
            let len = (rng() % 80) as usize;
            let buf: Vec<u8> = (0..len)
                .map(|_| match rng() % 9 {
                    // Bias toward protocol-significant bytes so framing paths get hit.
                    0 => b'*',
                    1 => b'$',
                    2 => b'\r',
                    3 => b'\n',
                    4 => b'-',
                    5 => 0u8,
                    6 | 7 => b"0123456789"[(rng() % 10) as usize],
                    _ => (rng() % 256) as u8,
                })
                .collect();
            // Invariant 1: never panics. Invariant 2: a Complete consumes a
            // positive, in-bounds count (so a pipelined drain loop terminates).
            if let Parsed::Complete(_, consumed) = parse_command(&buf) {
                assert!(
                    consumed > 0 && consumed <= buf.len(),
                    "consumed {consumed} out of bounds for {buf:?}"
                );
            }
        }
    }

    #[test]
    fn cursor_resumes_and_matches_one_shot_parse() {
        // Feeding a command byte-by-byte through one cursor must land on exactly
        // the one-shot result, with validation progress carried between calls.
        let full = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        let mut cur = Cursor::default();
        for i in 0..full.len() {
            assert!(matches!(
                parse_command_cursor(&full[..i], &mut cur),
                Parsed::Incomplete
            ));
        }
        match parse_command_cursor(full, &mut cur) {
            Parsed::Complete(tokens, n) => {
                assert_eq!(
                    tokens,
                    vec![b"SET".to_vec(), b"foo".to_vec(), b"bar".to_vec()]
                );
                assert_eq!(n, full.len());
            }
            other => panic!("expected Complete, got {other:?}"),
        }
        // The cursor reset on Complete: it can parse a second command cleanly.
        match parse_command_cursor(b"*1\r\n$4\r\nPING\r\n", &mut cur) {
            Parsed::Complete(tokens, _) => assert_eq!(tokens, vec![b"PING".to_vec()]),
            other => panic!("expected Complete, got {other:?}"),
        }
    }

    #[test]
    fn fuzz_cursor_incremental_matches_one_shot() {
        // For random buffers, incrementally feeding prefixes through a cursor
        // must classify the full buffer identically to the stateless parser.
        let mut state: u64 = 0x1234_5678_9abc_def0;
        let mut rng = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for _ in 0..20_000 {
            let len = (rng() % 64) as usize;
            let buf: Vec<u8> = (0..len)
                .map(|_| match rng() % 8 {
                    0 => b'*',
                    1 => b'$',
                    2 => b'\r',
                    3 => b'\n',
                    4 | 5 => b"0123456789"[(rng() % 10) as usize],
                    _ => (rng() % 256) as u8,
                })
                .collect();
            let mut cur = Cursor::default();
            let mut incremental = Parsed::Incomplete;
            for i in 1..=buf.len() {
                incremental = parse_command_cursor(&buf[..i], &mut cur);
                if !matches!(incremental, Parsed::Incomplete) {
                    break;
                }
            }
            let one_shot = parse_command(&buf);
            match (&incremental, &one_shot) {
                (Parsed::Complete(a, n), Parsed::Complete(b, m)) => {
                    assert_eq!(a, b);
                    assert_eq!(n, m);
                }
                (Parsed::Error(_), Parsed::Error(_)) => {}
                // One-shot may see an error only visible past the point where
                // incremental completed a shorter prefix — impossible here since
                // both scan from offset 0; require the same classification.
                (Parsed::Incomplete, Parsed::Incomplete) => {}
                (a, b) => panic!("incremental {a:?} != one-shot {b:?} for {buf:?}"),
            }
        }
    }

    #[test]
    fn fuzz_pipeline_drain_always_terminates() {
        // The reader loop parses, drains `consumed`, and reparses the tail. On any
        // adversarial buffer this must make progress and terminate.
        let mut state: u64 = 0xdead_beef_cafe_f00d;
        let mut rng = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for _ in 0..20_000 {
            let len = (rng() % 96) as usize;
            let mut buf: Vec<u8> = (0..len)
                .map(|_| match rng() % 6 {
                    0 => b'*',
                    1 => b'$',
                    2 => b'\r',
                    3 => b'\n',
                    4 => b"0123456789"[(rng() % 10) as usize],
                    _ => (rng() % 256) as u8,
                })
                .collect();
            let mut guard = 0;
            loop {
                guard += 1;
                assert!(guard < 10_000, "drain did not terminate");
                match parse_command(&buf) {
                    Parsed::Complete(_, n) => {
                        assert!(n > 0 && n <= buf.len());
                        buf.drain(0..n);
                        if buf.is_empty() {
                            break;
                        }
                    }
                    Parsed::Incomplete | Parsed::Error(_) => break,
                }
            }
        }
    }
}
