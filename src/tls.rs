//! Optional in-process TLS (the `tls` cargo feature).
//!
//! The default build is dependency-free; this module — and the `rustls` it uses —
//! are compiled **only** under `--features tls`. It terminates TLS for client
//! connections with rustls (pure-Rust, `ring` crypto provider — no OpenSSL/C),
//! reusing the same hub plumbing as plaintext connections.
//!
//! A TLS session is a single, stateful object that can't be split across the
//! reader/writer thread pair the plaintext path uses, so each TLS connection is
//! driven by one thread that interleaves reading client commands with draining
//! the hub's reply channel. (A small polling latency on server-initiated pushes is
//! the price; in-process TLS is an opt-in convenience — the sidecar in
//! docs/DEPLOYMENT.md stays the zero-dependency default.)
//!
//! Config: LOCUS_TLS_PORT (enables it), LOCUS_TLS_CERT, LOCUS_TLS_KEY (PEM paths).

use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, mpsc};
use std::time::Duration;

use rustls::pki_types::{
    CertificateDer, PrivateKeyDer, PrivatePkcs1KeyDer, PrivatePkcs8KeyDer, PrivateSec1KeyDer,
};
use rustls::{ServerConfig, ServerConnection, StreamOwned};

use crate::{Dispatch, Msg, dispatch_commands, log};

/// Build a rustls server config from the PEM cert/key named by LOCUS_TLS_CERT /
/// LOCUS_TLS_KEY. Returns a human error (logged by the caller) on any problem.
pub fn server_config() -> Result<Arc<ServerConfig>, String> {
    let cert_path =
        std::env::var("LOCUS_TLS_CERT").map_err(|_| "LOCUS_TLS_CERT not set".to_string())?;
    let key_path =
        std::env::var("LOCUS_TLS_KEY").map_err(|_| "LOCUS_TLS_KEY not set".to_string())?;
    let cert_pem = std::fs::read(&cert_path).map_err(|e| format!("reading {cert_path}: {e}"))?;
    let key_pem = std::fs::read(&key_path).map_err(|e| format!("reading {key_path}: {e}"))?;

    let certs = load_certs(&cert_pem)?;
    let key = load_key(&key_pem)?;
    // Install the ring provider once; ignore "already installed" on a second call.
    let _ = rustls::crypto::ring::default_provider().install_default();
    ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map(Arc::new)
        .map_err(|e| format!("tls config: {e}"))
}

/// Handle one TLS client connection: terminate TLS, then run the same
/// parse → dispatch → reply loop as plaintext, on a single thread.
pub fn handle_tls_conn(
    tcp: TcpStream,
    id: u64,
    tx: mpsc::Sender<Msg>,
    config: Arc<ServerConfig>,
) -> io::Result<()> {
    let peer = tcp.peer_addr()?;
    let is_loopback = peer.ip().is_loopback();
    let _ = tcp.set_nodelay(true);
    // A short read timeout lets us interleave reply/push draining with reads.
    let _ = tcp.set_read_timeout(Some(Duration::from_millis(50)));
    let conn = ServerConnection::new(config).map_err(io::Error::other)?;
    let mut tls = StreamOwned::new(conn, tcp);

    let (out_tx, out_rx) = mpsc::channel::<Vec<u8>>();
    if tx
        .send(Msg::Connect {
            id,
            out: out_tx.clone(),
            loopback: is_loopback,
        })
        .is_err()
    {
        return Ok(());
    }
    log::debug(&format!("tls client connected: {peer}"));

    let mut inbuf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 4096];
    'main: loop {
        // 1. Flush replies / server-initiated pushes (non-blocking).
        loop {
            match out_rx.try_recv() {
                Ok(b) => {
                    if tls.write_all(&b).is_err() {
                        break 'main;
                    }
                }
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => break 'main,
            }
        }
        let _ = tls.flush();

        // 2. Read + dispatch a batch (blocks up to the read timeout).
        match tls.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => {
                inbuf.extend_from_slice(&chunk[..n]);
                match dispatch_commands(&mut inbuf, id, &tx) {
                    Dispatch::Ok { dispatched } => {
                        // Deliver this batch's replies promptly (hub round-trip is
                        // ~µs) rather than waiting for the next poll tick.
                        if dispatched && !drain_replies(&out_rx, &mut tls) {
                            break;
                        }
                    }
                    Dispatch::ProtocolError(e) => {
                        let _ = tls.write_all(&e);
                        break;
                    }
                    Dispatch::HubGone => break,
                }
            }
            Err(ref e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) => {}
            Err(_) => break,
        }
    }

    let _ = tx.send(Msg::Disconnect { id });
    log::debug(&format!("tls client disconnected: {peer}"));
    Ok(())
}

/// Block briefly for the first reply of a just-dispatched batch, then drain the
/// rest. Returns false if the socket died. (Every client command replies, so the
/// timeout is a safety cap, not the common path.)
fn drain_replies(
    out_rx: &mpsc::Receiver<Vec<u8>>,
    tls: &mut StreamOwned<ServerConnection, TcpStream>,
) -> bool {
    match out_rx.recv_timeout(Duration::from_millis(250)) {
        Ok(b) => {
            if tls.write_all(&b).is_err() {
                return false;
            }
        }
        Err(mpsc::RecvTimeoutError::Timeout) => return true,
        Err(mpsc::RecvTimeoutError::Disconnected) => return false,
    }
    while let Ok(b) = out_rx.try_recv() {
        if tls.write_all(&b).is_err() {
            return false;
        }
    }
    tls.flush().is_ok()
}

// === PEM / base64 (std-only; avoids a separate pem-parsing dependency) ========

fn load_certs(pem: &[u8]) -> Result<Vec<CertificateDer<'static>>, String> {
    let ders = pem_blocks(pem, "CERTIFICATE");
    if ders.is_empty() {
        return Err("no CERTIFICATE block in the cert file".into());
    }
    Ok(ders.into_iter().map(CertificateDer::from).collect())
}

fn load_key(pem: &[u8]) -> Result<PrivateKeyDer<'static>, String> {
    if let Some(der) = pem_blocks(pem, "PRIVATE KEY").into_iter().next() {
        return Ok(PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(der)));
    }
    if let Some(der) = pem_blocks(pem, "RSA PRIVATE KEY").into_iter().next() {
        return Ok(PrivateKeyDer::Pkcs1(PrivatePkcs1KeyDer::from(der)));
    }
    if let Some(der) = pem_blocks(pem, "EC PRIVATE KEY").into_iter().next() {
        return Ok(PrivateKeyDer::Sec1(PrivateSec1KeyDer::from(der)));
    }
    Err("no PRIVATE KEY block in the key file".into())
}

/// Extract every base64 body between `-----BEGIN <label>-----` / `-----END …` and
/// decode each to DER.
fn pem_blocks(pem: &[u8], label: &str) -> Vec<Vec<u8>> {
    let text = String::from_utf8_lossy(pem);
    let begin = format!("-----BEGIN {label}-----");
    let end = format!("-----END {label}-----");
    let mut out = Vec::new();
    let mut rest = text.as_ref();
    while let Some(bi) = rest.find(&begin) {
        let after = &rest[bi + begin.len()..];
        let Some(ei) = after.find(&end) else { break };
        if let Ok(der) = base64_decode(&after.as_bytes()[..ei]) {
            out.push(der);
        }
        rest = &after[ei + end.len()..];
    }
    out
}

fn base64_decode(input: &[u8]) -> Result<Vec<u8>, String> {
    fn val(c: u8) -> Option<u8> {
        match c {
            b'A'..=b'Z' => Some(c - b'A'),
            b'a'..=b'z' => Some(c - b'a' + 26),
            b'0'..=b'9' => Some(c - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }
    let mut out = Vec::new();
    let mut buf = 0u32;
    let mut bits = 0u32;
    for &c in input {
        if c == b'=' || c.is_ascii_whitespace() {
            continue;
        }
        let v = val(c).ok_or_else(|| format!("invalid base64 byte 0x{c:02x}"))?;
        buf = (buf << 6) | v as u32;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((buf >> bits) as u8);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_roundtrip_known_vectors() {
        assert_eq!(base64_decode(b"").unwrap(), b"");
        assert_eq!(base64_decode(b"TWFu").unwrap(), b"Man");
        assert_eq!(base64_decode(b"aGVsbG8=").unwrap(), b"hello");
        assert_eq!(base64_decode(b"Zm9vYmE=").unwrap(), b"fooba");
        // whitespace (as in wrapped PEM) is ignored
        assert_eq!(base64_decode(b"aGVs\nbG8=").unwrap(), b"hello");
    }

    #[test]
    fn pem_blocks_extracts_bodies() {
        let pem = b"-----BEGIN CERTIFICATE-----\nTWFu\n-----END CERTIFICATE-----\n";
        let blocks = pem_blocks(pem, "CERTIFICATE");
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0], b"Man");
        assert!(pem_blocks(pem, "PRIVATE KEY").is_empty());
    }
}
