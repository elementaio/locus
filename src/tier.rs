//! The disk tier: "hot in RAM, warm on disk" for a store whose RAM is for
//! LIVE data, not archives (std-only, zero-dependency).
//!
//! `TIER key` moves a key's VALUE into a segmented, append-only value-log on
//! disk, leaving a ~100-byte stub (key + TTL + disk pointer) in RAM. Any read
//! that needs the value transparently THAWS it back. The log is the tiered
//! values' durability (they are exactly what RDB/AOF no longer carry in full).
//!
//! Segments are IMMUTABLE once rotated and are only ever DELETED — when no
//! live stub references them anymore — never rewritten. That is the design's
//! load-bearing property: a (segment, offset) pointer persisted in a snapshot
//! or an AOF `TIERREF` can never silently point at reshuffled bytes. With
//! TTL'd workloads (the intended use: retention-bound archives), same-aged
//! data dies together, so whole segments naturally empty out and vanish.
//!
//! Each entry embeds its key (via the RDB `dump_entry` layout), so a thaw
//! validates identity before trusting bytes — a stale pointer (e.g. a very old
//! snapshot referencing a since-deleted generation) is a detected, logged loss,
//! never silent corruption.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};

use crate::log;

/// Rotate the active segment past this many bytes (LOCUS_TIER_SEG_MB).
const DEFAULT_SEG_BYTES: u64 = 512 * 1024 * 1024;

pub struct TierStore {
    base: String, // segment files are {base}.{seq:06}
    seg_max: u64,
    active_seq: u32,
    active: File,
    active_len: u64,
    /// Every existing segment's byte length (including the active one).
    segments: BTreeMap<u32, u64>,
}

impl TierStore {
    /// Open (or start) the value-log at `base`. Existing segments are kept as
    /// immutable history; appends always go to a fresh segment, so a previous
    /// run's files are never touched again.
    pub fn open(base: &str, seg_max_bytes: u64) -> io::Result<TierStore> {
        let mut segments = BTreeMap::new();
        let dir = std::path::Path::new(base)
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| std::path::Path::new("."));
        let prefix = format!(
            "{}.",
            std::path::Path::new(base)
                .file_name()
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_default()
        );
        if let Ok(entries) = fs::read_dir(dir) {
            for e in entries.flatten() {
                let name = e.file_name().to_string_lossy().into_owned();
                if let Some(seq) = name
                    .strip_prefix(&prefix)
                    .and_then(|s| s.parse::<u32>().ok())
                {
                    let len = e.metadata().map(|m| m.len()).unwrap_or(0);
                    segments.insert(seq, len);
                }
            }
        }
        let active_seq = segments.keys().max().copied().map_or(1, |m| m + 1);
        let active = OpenOptions::new()
            .create(true)
            .append(true)
            .open(seg_path(base, active_seq))?;
        segments.insert(active_seq, 0);
        Ok(TierStore {
            base: base.to_string(),
            seg_max: if seg_max_bytes > 0 {
                seg_max_bytes
            } else {
                DEFAULT_SEG_BYTES
            },
            active_seq,
            active,
            active_len: 0,
            segments,
        })
    }

    /// Append one entry (an `rdb::dump_entry` blob, which embeds the key) and
    /// fsync it — the log IS the value's durability from this moment. Returns
    /// its (segment, offset, length) address.
    pub fn append(&mut self, entry: &[u8]) -> io::Result<(u32, u64, u32)> {
        if self.active_len >= self.seg_max {
            self.rotate()?;
        }
        let (seq, off) = (self.active_seq, self.active_len);
        let len = entry.len() as u32;
        self.active.write_all(&len.to_le_bytes())?;
        self.active.write_all(entry)?;
        self.active.sync_data()?;
        self.active_len += 4 + entry.len() as u64;
        self.segments.insert(seq, self.active_len);
        Ok((seq, off, len))
    }

    /// Read the entry at (seg, off, len). The caller decodes and validates the
    /// embedded key; a missing segment or short read is a detected loss (None).
    pub fn read(&self, seg: u32, off: u64, len: u32) -> Option<Vec<u8>> {
        let mut f = File::open(seg_path(&self.base, seg)).ok()?;
        f.seek(SeekFrom::Start(off)).ok()?;
        let mut hdr = [0u8; 4];
        f.read_exact(&mut hdr).ok()?;
        if u32::from_le_bytes(hdr) != len {
            return None; // pointer disagrees with the log — refuse the bytes
        }
        let mut buf = vec![0u8; len as usize];
        f.read_exact(&mut buf).ok()?;
        Some(buf)
    }

    /// Delete a fully-dead segment (no live stub references it). The active
    /// segment is never deleted.
    pub fn delete_segment(&mut self, seg: u32) {
        if seg == self.active_seq {
            return;
        }
        if self.segments.remove(&seg).is_some() {
            if let Err(e) = fs::remove_file(seg_path(&self.base, seg)) {
                log::warn(&format!("tier: could not delete segment {seg}: {e}"));
            } else {
                log::info(&format!("tier: deleted empty segment {seg}"));
            }
        }
    }

    /// Delete every segment and start fresh (FLUSHALL of a tiered keyspace).
    pub fn delete_all(&mut self) -> io::Result<()> {
        let segs: Vec<u32> = self.segments.keys().copied().collect();
        for s in segs {
            if s != self.active_seq {
                self.delete_segment(s);
            }
        }
        // Recreate the active segment empty.
        let _ = fs::remove_file(seg_path(&self.base, self.active_seq));
        self.active = OpenOptions::new()
            .create(true)
            .append(true)
            .open(seg_path(&self.base, self.active_seq))?;
        self.active_len = 0;
        self.segments.insert(self.active_seq, 0);
        Ok(())
    }

    fn rotate(&mut self) -> io::Result<()> {
        let next = self.active_seq + 1;
        self.active = OpenOptions::new()
            .create(true)
            .append(true)
            .open(seg_path(&self.base, next))?;
        self.active_seq = next;
        self.active_len = 0;
        self.segments.insert(next, 0);
        Ok(())
    }

    /// The segment currently receiving appends (never deletable).
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn active_segment(&self) -> u32 {
        self.active_seq
    }

    /// (segment count, total log bytes) for INFO/ops.
    pub fn stats(&self) -> (usize, u64) {
        (self.segments.len(), self.segments.values().sum())
    }
}

fn seg_path(base: &str, seq: u32) -> String {
    format!("{base}.{seq:06}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_base(name: &str) -> String {
        format!(
            "{}/locus-tier-{}-{}",
            std::env::temp_dir().display(),
            std::process::id(),
            name
        )
    }

    #[test]
    fn append_read_roundtrip_and_identity() {
        let base = tmp_base("rt");
        let mut t = TierStore::open(&base, 0).unwrap();
        let (s1, o1, l1) = t.append(b"entry-one").unwrap();
        let (s2, o2, l2) = t.append(b"entry-two-longer").unwrap();
        assert_eq!(t.read(s1, o1, l1).as_deref(), Some(&b"entry-one"[..]));
        assert_eq!(
            t.read(s2, o2, l2).as_deref(),
            Some(&b"entry-two-longer"[..])
        );
        // A wrong length is refused, not misread.
        assert!(t.read(s1, o1, l1 + 1).is_none());
        let _ = t.delete_all();
    }

    #[test]
    fn rotation_and_empty_segment_deletion() {
        let base = tmp_base("rot");
        let mut t = TierStore::open(&base, 32).unwrap(); // tiny cap -> rotate fast
        let (s1, ..) = t.append(&[b'a'; 40]).unwrap();
        let (s2, o2, l2) = t.append(&[b'b'; 40]).unwrap();
        assert_ne!(s1, s2, "second append should land in a rotated segment");
        t.delete_segment(s1);
        assert!(t.read(s1, 0, 40).is_none(), "deleted segment gone");
        assert!(t.read(s2, o2, l2).is_some(), "later segment intact");
        // The active segment refuses deletion.
        let active = t.active_segment();
        t.delete_segment(active);
        assert!(t.append(b"still-writable").is_ok());
        let _ = t.delete_all();
    }

    #[test]
    fn reopen_never_touches_old_segments() {
        let base = tmp_base("reopen");
        let addr = {
            let mut t = TierStore::open(&base, 0).unwrap();
            t.append(b"survives").unwrap()
        };
        let t2 = TierStore::open(&base, 0).unwrap();
        assert!(
            t2.active_segment() > addr.0,
            "a new run appends to a fresh segment"
        );
        assert_eq!(
            t2.read(addr.0, addr.1, addr.2).as_deref(),
            Some(&b"survives"[..])
        );
        let mut t2 = t2;
        let _ = t2.delete_all();
        let _ = std::fs::remove_file(format!("{base}.{:06}", addr.0));
    }
}
