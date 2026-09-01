//! Publish/subscribe: a registry of who's listening, plus the message encoders.
//!
//! The owner thread holds one PubSub. Each client has an output channel (to its
//! writer thread); PUBLISH routes a message to every subscriber's channel.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::resp::bulk_string;

/// An encoded push frame shared across every subscriber it's delivered to.
pub type Frame = Arc<Vec<u8>>;

pub struct PubSub {
    channels: HashMap<Vec<u8>, HashSet<u64>>, // channel -> subscriber ids
    patterns: HashMap<Vec<u8>, HashSet<u64>>, // glob pattern -> subscriber ids
    counts: HashMap<u64, usize>,              // client -> total (chan + pattern) subscriptions
}

/// Delegates to [`PubSub::new`] — no channels, no patterns, no subscribers.
impl Default for PubSub {
    fn default() -> Self {
        Self::new()
    }
}

impl PubSub {
    pub fn new() -> Self {
        PubSub {
            channels: HashMap::new(),
            patterns: HashMap::new(),
            counts: HashMap::new(),
        }
    }

    /// Total subscriptions for a client (used to enforce "subscribe mode").
    pub fn total(&self, id: u64) -> usize {
        self.counts.get(&id).copied().unwrap_or(0)
    }

    pub fn subscribe(&mut self, id: u64, channel: &[u8]) -> usize {
        if self
            .channels
            .entry(channel.to_vec())
            .or_default()
            .insert(id)
        {
            *self.counts.entry(id).or_insert(0) += 1;
        }
        self.total(id)
    }

    pub fn psubscribe(&mut self, id: u64, pat: &[u8]) -> usize {
        if self.patterns.entry(pat.to_vec()).or_default().insert(id) {
            *self.counts.entry(id).or_insert(0) += 1;
        }
        self.total(id)
    }

    pub fn unsubscribe(&mut self, id: u64, channel: &[u8]) -> usize {
        if let Some(subs) = self.channels.get_mut(channel) {
            if subs.remove(&id)
                && let Some(c) = self.counts.get_mut(&id)
            {
                *c = c.saturating_sub(1);
            }
            if subs.is_empty() {
                self.channels.remove(channel);
            }
        }
        self.total(id)
    }

    pub fn punsubscribe(&mut self, id: u64, pat: &[u8]) -> usize {
        if let Some(subs) = self.patterns.get_mut(pat) {
            if subs.remove(&id)
                && let Some(c) = self.counts.get_mut(&id)
            {
                *c = c.saturating_sub(1);
            }
            if subs.is_empty() {
                self.patterns.remove(pat);
            }
        }
        self.total(id)
    }

    pub fn channels_of(&self, id: u64) -> Vec<Vec<u8>> {
        self.channels
            .iter()
            .filter(|(_, s)| s.contains(&id))
            .map(|(c, _)| c.clone())
            .collect()
    }

    pub fn patterns_of(&self, id: u64) -> Vec<Vec<u8>> {
        self.patterns
            .iter()
            .filter(|(_, s)| s.contains(&id))
            .map(|(p, _)| p.clone())
            .collect()
    }

    /// Drop a disconnected client from all subscriptions.
    pub fn remove_client(&mut self, id: u64) {
        self.channels.retain(|_, s| {
            s.remove(&id);
            !s.is_empty()
        });
        self.patterns.retain(|_, s| {
            s.remove(&id);
            !s.is_empty()
        });
        self.counts.remove(&id);
    }

    /// The `(subscriber, frame)` deliveries for a publish: channel subscribers
    /// plus matching pattern subscribers. Each frame is encoded ONCE per RESP
    /// proto in use and shared via `Arc` — never re-encoded per subscriber, so
    /// a large payload with many subscribers costs one allocation, not O(subs),
    /// on the single-threaded hub.
    pub fn deliveries(
        &self,
        channel: &[u8],
        payload: &[u8],
        protos: &HashMap<u64, u8>,
    ) -> Vec<(u64, Frame)> {
        let proto_of = |id: &u64| protos.get(id).copied().unwrap_or(2);
        let mut out: Vec<(u64, Frame)> = Vec::new();
        if let Some(subs) = self.channels.get(channel) {
            let (mut m2, mut m3): (Option<Frame>, Option<Frame>) = (None, None);
            for id in subs {
                let frame = if proto_of(id) >= 3 {
                    m3.get_or_insert_with(|| Arc::new(message(channel, payload, 3)))
                } else {
                    m2.get_or_insert_with(|| Arc::new(message(channel, payload, 2)))
                };
                out.push((*id, frame.clone()));
            }
        }
        for (pat, subs) in &self.patterns {
            if glob_match(pat, channel) {
                let (mut m2, mut m3): (Option<Frame>, Option<Frame>) = (None, None);
                for id in subs {
                    let frame = if proto_of(id) >= 3 {
                        m3.get_or_insert_with(|| Arc::new(pmessage(pat, channel, payload, 3)))
                    } else {
                        m2.get_or_insert_with(|| Arc::new(pmessage(pat, channel, payload, 2)))
                    };
                    out.push((*id, frame.clone()));
                }
            }
        }
        out
    }

    pub fn active_channels(&self) -> Vec<Vec<u8>> {
        self.channels.keys().cloned().collect()
    }
    pub fn numsub(&self, channel: &[u8]) -> i64 {
        self.channels
            .get(channel)
            .map(|s| s.len() as i64)
            .unwrap_or(0)
    }
    pub fn numpat(&self) -> i64 {
        self.patterns.len() as i64
    }
}

/// Match the single-character pattern token at `pat[p]` against `ch`, returning
/// the index just past the token when it matches. `pat[p]` is never `*` — the
/// caller handles that, because it is the only token that spans many characters.
///
/// The grammar is Redis's `stringmatchlen`, quirks included: a class runs to
/// the first unescaped `]`, `^` right after `[` negates it, `a-b` inside a class
/// is a range (endpoints swapped if reversed), `\` escapes the next byte both
/// inside and outside a class, and a trailing `\` is a literal backslash.
fn match_one(pat: &[u8], p: usize, ch: u8) -> Option<usize> {
    match pat[p] {
        b'?' => Some(p + 1),
        b'\\' => {
            if p + 1 < pat.len() {
                (pat[p + 1] == ch).then_some(p + 2)
            } else {
                (ch == b'\\').then_some(p + 1)
            }
        }
        b'[' => {
            let mut i = p + 1;
            let neg = i < pat.len() && pat[i] == b'^';
            if neg {
                i += 1;
            }
            let mut hit = false;
            while i < pat.len() {
                if pat[i] == b'\\' && i + 1 < pat.len() {
                    hit |= pat[i + 1] == ch;
                    i += 2;
                } else if pat[i] == b']' {
                    i += 1;
                    break;
                } else if i + 2 < pat.len() && pat[i + 1] == b'-' {
                    // Redis does not exclude `]` as a range endpoint, so `[1-]`
                    // really is the range '1'..=']'. Matching that exactly is
                    // the point of a differential harness.
                    let (lo, hi) = (pat[i].min(pat[i + 2]), pat[i].max(pat[i + 2]));
                    hit |= (lo..=hi).contains(&ch);
                    i += 3;
                } else {
                    hit |= pat[i] == ch;
                    i += 1;
                }
            }
            // An unterminated class simply ends with the pattern.
            (hit != neg).then_some(i)
        }
        c => (c == ch).then_some(p + 1),
    }
}

/// Glob matching for `KEYS`, the `SCAN` family's `MATCH`, `PSUBSCRIBE` and the
/// ACL key/channel patterns: `*`, `?`, `[...]` character classes (with `^`
/// negation and `a-z` ranges) and `\` escapes — Redis's `stringmatchlen`.
pub fn glob_match(pat: &[u8], text: &[u8]) -> bool {
    let (mut p, mut t) = (0usize, 0usize);
    let (mut star, mut mark) = (None, 0usize);
    while t < text.len() {
        if p < pat.len() && pat[p] == b'*' {
            star = Some(p);
            mark = t;
            p += 1;
        } else if p < pat.len()
            && let Some(next) = match_one(pat, p, text[t])
        {
            p = next;
            t += 1;
        } else if let Some(sp) = star {
            p = sp + 1;
            mark += 1;
            t = mark;
        } else {
            return false;
        }
    }
    while p < pat.len() && pat[p] == b'*' {
        p += 1;
    }
    p == pat.len()
}

// --- message encoders -------------------------------------------------------

/// Outer frame: a RESP3 push (`>`) on proto 3, else a RESP2 array (`*`). RESP3
/// clients use the push type to tell pub/sub traffic apart from command replies.
fn frame(proto: u8, n: usize) -> Vec<u8> {
    let tag = if proto >= 3 { '>' } else { '*' };
    format!("{tag}{n}\r\n").into_bytes()
}

fn kind_reply(kind: &[u8], channel: Option<&[u8]>, count: i64, proto: u8) -> Vec<u8> {
    let mut o = frame(proto, 3);
    o.extend_from_slice(&bulk_string(kind));
    match channel {
        Some(c) => o.extend_from_slice(&bulk_string(c)),
        None => o.extend_from_slice(b"$-1\r\n"),
    }
    o.extend_from_slice(format!(":{count}\r\n").as_bytes());
    o
}

pub fn subscribe_reply(channel: &[u8], count: usize, proto: u8) -> Vec<u8> {
    kind_reply(b"subscribe", Some(channel), count as i64, proto)
}
pub fn psubscribe_reply(pat: &[u8], count: usize, proto: u8) -> Vec<u8> {
    kind_reply(b"psubscribe", Some(pat), count as i64, proto)
}
pub fn unsubscribe_reply(channel: Option<&[u8]>, count: usize, proto: u8) -> Vec<u8> {
    kind_reply(b"unsubscribe", channel, count as i64, proto)
}
pub fn punsubscribe_reply(pat: Option<&[u8]>, count: usize, proto: u8) -> Vec<u8> {
    kind_reply(b"punsubscribe", pat, count as i64, proto)
}

pub fn message(channel: &[u8], payload: &[u8], proto: u8) -> Vec<u8> {
    let mut o = frame(proto, 3);
    o.extend_from_slice(&bulk_string(b"message"));
    o.extend_from_slice(&bulk_string(channel));
    o.extend_from_slice(&bulk_string(payload));
    o
}

pub fn pmessage(pattern: &[u8], channel: &[u8], payload: &[u8], proto: u8) -> Vec<u8> {
    let mut o = frame(proto, 4);
    o.extend_from_slice(&bulk_string(b"pmessage"));
    o.extend_from_slice(&bulk_string(pattern));
    o.extend_from_slice(&bulk_string(channel));
    o.extend_from_slice(&bulk_string(payload));
    o
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glob() {
        assert!(glob_match(b"news.*", b"news.tech"));
        assert!(glob_match(b"news.*", b"news."));
        assert!(!glob_match(b"news.*", b"sports.tech"));
        assert!(glob_match(b"h?llo", b"hello"));
        assert!(glob_match(b"*", b"anything"));
        assert!(!glob_match(b"h?llo", b"heello"));
    }

    /// Session 8, finding 8.4 — the matcher only knew `*` and `?`, so every
    /// bracket pattern matched **nothing**: `KEYS a[12]`, `SCAN MATCH`,
    /// `PSUBSCRIBE` and the ACL key/channel patterns all silently returned or
    /// granted an empty set. Expected values read off redis-server 8.8.
    #[test]
    fn glob_character_classes_match_the_way_redis_matches() {
        // A class, and its negation.
        assert!(glob_match(b"a[12]", b"a1") && glob_match(b"a[12]", b"a2"));
        assert!(!glob_match(b"a[12]", b"a3"));
        assert!(glob_match(b"a[^1]", b"a2") && !glob_match(b"a[^1]", b"a1"));
        // Ranges, including a reversed one (Redis swaps the endpoints).
        assert!(glob_match(b"a[0-9]", b"a7") && !glob_match(b"a[0-9]", b"ax"));
        assert!(glob_match(b"a[9-0]", b"a7"));
        assert!(glob_match(b"[ab]1", b"a1") && glob_match(b"[ab]1", b"b1"));
        // Redis does NOT treat `]` as ending a range's endpoint, so `[1-]` is
        // literally the range '1'..=']' — which contains '2'.
        assert!(glob_match(b"a[1-]", b"a1") && glob_match(b"a[1-]", b"a2"));
        // Escapes, inside a class and out.
        assert!(glob_match(br"a\-1", b"a-1") && !glob_match(br"a\-1", b"ax1"));
        assert!(glob_match(br"[\]]", b"]"));
        assert!(glob_match(br"\*", b"*") && !glob_match(br"\*", b"anything"));
        // Classes compose with the star's backtracking.
        assert!(glob_match(b"*[0-9]", b"user42"));
        assert!(!glob_match(b"*[0-9]", b"user"));
        assert!(glob_match(b"user:[0-9]*", b"user:1234:name"));
        assert!(!glob_match(b"user:[0-9]*", b"user:abc"));
        // And the plain cases still behave.
        assert!(glob_match(b"*", b"") && glob_match(b"", b""));
        assert!(!glob_match(b"", b"x"));
        assert!(glob_match(b"?", b"*")); // the ACL trap, unchanged
    }
}
