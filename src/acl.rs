//! A small, std-only access-control layer.
//!
//! Two pieces: a vendored SHA-256 (the one crypto primitive we need — to store
//! passwords as Redis does, never in cleartext) and a deliberately-SIMPLE user
//! model — coarse command *classes* plus an optional key prefix, far coarser
//! than Redis's full ACL selector grammar. "Ship the primitive, refuse the
//! policy." This layers on top of `requirepass`: the implicit `default` user is
//! unrestricted, and named users created via `ACL SETUSER` get least-privilege.

// === SHA-256 (FIPS 180-4), vendored so the core stays zero-dependency ========

const K: [u32; 64] = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];

pub fn sha256(data: &[u8]) -> [u8; 32] {
    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];
    let bitlen = (data.len() as u64).wrapping_mul(8);
    let mut msg = data.to_vec();
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&bitlen.to_be_bytes());

    for chunk in msg.chunks_exact(64) {
        let mut w = [0u32; 64];
        for (i, word) in w.iter_mut().enumerate().take(16) {
            let b = i * 4;
            *word = u32::from_be_bytes([chunk[b], chunk[b + 1], chunk[b + 2], chunk[b + 3]]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }
        let (mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh) =
            (h[0], h[1], h[2], h[3], h[4], h[5], h[6], h[7]);
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let t1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let t2 = s0.wrapping_add(maj);
            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(t1);
            d = c;
            c = b;
            b = a;
            a = t1.wrapping_add(t2);
        }
        for (hv, v) in h.iter_mut().zip([a, b, c, d, e, f, g, hh]) {
            *hv = hv.wrapping_add(v);
        }
    }
    let mut out = [0u8; 32];
    for (i, word) in h.iter().enumerate() {
        out[i * 4..i * 4 + 4].copy_from_slice(&word.to_be_bytes());
    }
    out
}

/// Lowercase hex of a 32-byte hash (ACL GETUSER reports the hash, never the pass).
pub fn hex32(b: &[u8; 32]) -> String {
    let mut s = String::with_capacity(64);
    for byte in b {
        s.push_str(&format!("{byte:02x}"));
    }
    s
}

/// Constant-time compare of two 32-byte hashes.
fn ct_eq32(a: &[u8; 32], b: &[u8; 32]) -> bool {
    let mut diff = 0u8;
    for i in 0..32 {
        diff |= a[i] ^ b[i];
    }
    diff == 0
}

// === command classes (the ACL's coarse permission unit) ======================

pub const CLASS_READ: u8 = 1 << 0;
pub const CLASS_WRITE: u8 = 1 << 1;
pub const CLASS_ADMIN: u8 = 1 << 2;
pub const CLASS_CONNECTION: u8 = 1 << 3;
pub const CLASS_PUBSUB: u8 = 1 << 4;
pub const CLASS_ALL: u8 = 0b1_1111;

pub fn class_from_name(name: &[u8]) -> Option<u8> {
    match name.to_ascii_lowercase().as_slice() {
        b"read" => Some(CLASS_READ),
        b"write" => Some(CLASS_WRITE),
        b"admin" => Some(CLASS_ADMIN),
        b"connection" => Some(CLASS_CONNECTION),
        b"pubsub" => Some(CLASS_PUBSUB),
        b"all" => Some(CLASS_ALL),
        _ => None,
    }
}

pub fn class_names(mask: u8) -> Vec<&'static str> {
    let mut v = Vec::new();
    for (bit, name) in [
        (CLASS_READ, "read"),
        (CLASS_WRITE, "write"),
        (CLASS_ADMIN, "admin"),
        (CLASS_CONNECTION, "connection"),
        (CLASS_PUBSUB, "pubsub"),
    ] {
        if mask & bit != 0 {
            v.push(name);
        }
    }
    v
}

// === user model ==============================================================

#[derive(Clone)]
pub struct User {
    pub enabled: bool,
    pub nopass: bool,
    pub passwords: Vec<[u8; 32]>,
    pub classes: u8,
    pub key_prefix: Option<Vec<u8>>, // None => all keys
}

impl User {
    /// A fresh, locked-down user: disabled, no password, no commands, no keys.
    pub fn new() -> User {
        User {
            enabled: false,
            nopass: false,
            passwords: Vec::new(),
            classes: 0,
            key_prefix: Some(b"\x00locus-no-keys\x00".to_vec()), // matches nothing
        }
    }

    pub fn check_password(&self, pass: &[u8]) -> bool {
        if self.nopass {
            return true;
        }
        let h = sha256(pass);
        self.passwords.iter().any(|p| ct_eq32(p, &h))
    }

    pub fn allows_class(&self, class: u8) -> bool {
        self.classes & class != 0
    }

    pub fn allows_key(&self, key: &[u8]) -> bool {
        match &self.key_prefix {
            None => true,
            Some(p) => key.starts_with(p),
        }
    }

    /// True when the user may touch the whole keyspace — required for the
    /// keyspace-wide data commands (KEYS/SCAN/GEOSEARCH/…) that a key prefix
    /// can't meaningfully filter.
    pub fn unrestricted_keys(&self) -> bool {
        self.key_prefix.is_none()
    }

    /// Apply one `ACL SETUSER` rule. Returns Err on an unrecognized rule.
    pub fn apply(&mut self, rule: &[u8]) -> Result<(), ()> {
        let lower = rule.to_ascii_lowercase();
        match lower.as_slice() {
            b"on" => self.enabled = true,
            b"off" => self.enabled = false,
            b"nopass" => {
                self.nopass = true;
                self.passwords.clear();
            }
            b"resetpass" => {
                self.nopass = false;
                self.passwords.clear();
            }
            b"allkeys" | b"~*" => self.key_prefix = None,
            b"resetkeys" => self.key_prefix = Some(b"\x00locus-no-keys\x00".to_vec()),
            b"allcommands" | b"+@all" => self.classes = CLASS_ALL,
            b"nocommands" | b"-@all" => self.classes = 0,
            b"reset" => *self = User::new(),
            _ if rule.starts_with(b">") => {
                self.nopass = false;
                self.passwords.push(sha256(&rule[1..]));
            }
            _ if lower.starts_with(b"+@") => {
                self.classes |= class_from_name(&rule[2..]).ok_or(())?
            }
            _ if lower.starts_with(b"-@") => {
                self.classes &= !class_from_name(&rule[2..]).ok_or(())?
            }
            _ if rule.starts_with(b"~") => {
                let p = rule[1..].strip_suffix(b"*").unwrap_or(&rule[1..]);
                self.key_prefix = Some(p.to_vec());
            }
            _ => return Err(()),
        }
        Ok(())
    }

    /// ACL GETUSER-style description lines (flags, classes, keys, hashes).
    pub fn describe(&self) -> Vec<Vec<u8>> {
        let flags = if self.enabled { "on" } else { "off" };
        let cmds = if self.classes == CLASS_ALL {
            "+@all".to_string()
        } else {
            class_names(self.classes)
                .iter()
                .map(|c| format!("+@{c}"))
                .collect::<Vec<_>>()
                .join(" ")
        };
        let keys = match &self.key_prefix {
            None => "~*".to_string(),
            Some(p) => format!("~{}*", String::from_utf8_lossy(p)),
        };
        let passinfo = if self.nopass {
            "nopass".to_string()
        } else {
            self.passwords
                .iter()
                .map(hex32)
                .collect::<Vec<_>>()
                .join(" ")
        };
        vec![
            b"flags".to_vec(),
            flags.as_bytes().to_vec(),
            b"passwords".to_vec(),
            passinfo.into_bytes(),
            b"commands".to_vec(),
            cmds.into_bytes(),
            b"keys".to_vec(),
            keys.into_bytes(),
        ]
    }
}

impl Default for User {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_known_answers() {
        assert_eq!(
            hex32(&sha256(b"abc")),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            hex32(&sha256(b"")),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn setuser_rules_and_checks() {
        let mut u = User::new();
        u.apply(b"on").unwrap();
        u.apply(b">secret").unwrap();
        u.apply(b"+@read").unwrap();
        u.apply(b"~app:").unwrap();
        assert!(u.enabled && u.check_password(b"secret") && !u.check_password(b"nope"));
        assert!(u.allows_class(CLASS_READ) && !u.allows_class(CLASS_WRITE));
        assert!(u.allows_key(b"app:1") && !u.allows_key(b"other"));
        u.apply(b"allcommands").unwrap();
        u.apply(b"allkeys").unwrap();
        assert!(u.allows_class(CLASS_WRITE) && u.allows_key(b"anything"));
        assert!(u.apply(b"+@bogus").is_err());
    }
}
