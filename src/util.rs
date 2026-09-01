//! Small helpers with no home of their own.
//!
//! Kept deliberately tiny: this is not a dumping ground. Something lands here
//! only when it is used by more than one module and belongs to none of them.

/// Constant-time equality: folds the whole comparison (including a length
/// mismatch) into one accumulator and always scans the longer slice, so AUTH
/// latency doesn't reveal how much of the secret matched.
///
/// Used by the client `AUTH`/`requirepass` path in the binary and by the
/// sentinel peer-plane shared-secret check in [`crate::sentinel`].
pub fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    let mut diff: u8 = if a.len() == b.len() { 0 } else { 1 };
    let n = a.len().max(b.len());
    for i in 0..n {
        let x = a.get(i).copied().unwrap_or(0);
        let y = b.get(i).copied().unwrap_or(0);
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ct_eq_matches_only_equal_slices() {
        assert!(ct_eq(b"secret", b"secret"));
        assert!(ct_eq(b"", b""));
        assert!(!ct_eq(b"secret", b"secrxt")); // same length, one byte differs
        assert!(!ct_eq(b"secret", b"secre")); // shorter
        assert!(!ct_eq(b"secret", b"secrets")); // longer
        assert!(!ct_eq(b"", b"x"));
    }
}
