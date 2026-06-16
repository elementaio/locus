//! RESP2 wire protocol: the resumable parser and the reply encoders.
//!
//! The parser never assumes one read() == one command. It is a state machine
//! over a caller-owned buffer: feed bytes, get back one complete command + how
//! many bytes it consumed, or Incomplete (keep the tail and read more).

const MAX_ARRAY: i64 = 1024 * 1024;
const MAX_BULK_LEN: i64 = 512 * 1024 * 1024;

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

/// Try to parse exactly one command from the front of `buf`.
pub fn parse_command(buf: &[u8]) -> Parsed {
    if buf.is_empty() {
        return Parsed::Incomplete;
    }
    if buf[0] != b'*' {
        return parse_inline(buf);
    }
    let (count, mut pos) = match read_count(buf, 0) {
        Count::Ok(n, p) => (n, p),
        Count::Incomplete => return Parsed::Incomplete,
        Count::Bad => return Parsed::Error("invalid multibulk length".into()),
    };
    if count <= 0 {
        return Parsed::Complete(Vec::new(), pos);
    }
    if count > MAX_ARRAY {
        return Parsed::Error("invalid multibulk length".into());
    }
    let mut tokens: Vec<Vec<u8>> = Vec::with_capacity(count as usize);
    for _ in 0..count {
        if pos >= buf.len() {
            return Parsed::Incomplete;
        }
        if buf[pos] != b'$' {
            return Parsed::Error(format!("expected '$', got '{}'", buf[pos] as char));
        }
        let (len, after_len) = match read_count(buf, pos) {
            Count::Ok(n, p) => (n, p),
            Count::Incomplete => return Parsed::Incomplete,
            Count::Bad => return Parsed::Error("invalid bulk length".into()),
        };
        if !(0..=MAX_BULK_LEN).contains(&len) {
            return Parsed::Error("invalid bulk length".into());
        }
        let len = len as usize;
        if after_len + len + 2 > buf.len() {
            return Parsed::Incomplete;
        }
        let data = buf[after_len..after_len + len].to_vec();
        let crlf_at = after_len + len;
        if &buf[crlf_at..crlf_at + 2] != b"\r\n" {
            return Parsed::Error("expected CRLF after bulk string".into());
        }
        tokens.push(data);
        pos = crlf_at + 2;
    }
    Parsed::Complete(tokens, pos)
}

fn parse_inline(buf: &[u8]) -> Parsed {
    match find_crlf(buf, 0) {
        None => Parsed::Incomplete,
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

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(tokens, vec![b"SET".to_vec(), b"foo".to_vec(), b"bar".to_vec()]);
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
}
