//! Publish/subscribe: a registry of who's listening, plus the message encoders.
//!
//! The owner thread holds one PubSub. Each client has an output channel (to its
//! writer thread); PUBLISH routes a message to every subscriber's channel.

use std::collections::{HashMap, HashSet};
use std::sync::mpsc::Sender;

use crate::resp::bulk_string;

pub struct PubSub {
    channels: HashMap<Vec<u8>, HashSet<u64>>, // channel -> subscriber ids
    patterns: HashMap<Vec<u8>, HashSet<u64>>, // glob pattern -> subscriber ids
    counts: HashMap<u64, usize>,              // client -> total (chan + pattern) subscriptions
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
        if self.channels.entry(channel.to_vec()).or_default().insert(id) {
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
                && let Some(c) = self.counts.get_mut(&id) {
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
                && let Some(c) = self.counts.get_mut(&id) {
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

    /// Deliver to channel subscribers and matching pattern subscribers; returns
    /// how many clients received it.
    pub fn publish(
        &self,
        channel: &[u8],
        payload: &[u8],
        clients: &HashMap<u64, Sender<Vec<u8>>>,
    ) -> i64 {
        let mut n = 0i64;
        if let Some(subs) = self.channels.get(channel) {
            let msg = message(channel, payload);
            for id in subs {
                if let Some(out) = clients.get(id)
                    && out.send(msg.clone()).is_ok() {
                        n += 1;
                    }
            }
        }
        for (pat, subs) in &self.patterns {
            if glob_match(pat, channel) {
                let msg = pmessage(pat, channel, payload);
                for id in subs {
                    if let Some(out) = clients.get(id)
                        && out.send(msg.clone()).is_ok() {
                            n += 1;
                        }
                }
            }
        }
        n
    }

    pub fn active_channels(&self) -> Vec<Vec<u8>> {
        self.channels.keys().cloned().collect()
    }
    pub fn numsub(&self, channel: &[u8]) -> i64 {
        self.channels.get(channel).map(|s| s.len() as i64).unwrap_or(0)
    }
    pub fn numpat(&self) -> i64 {
        self.patterns.len() as i64
    }
}

/// Glob matching for PSUBSCRIBE: supports `*` and `?` (and literals).
pub fn glob_match(pat: &[u8], text: &[u8]) -> bool {
    let (mut p, mut t) = (0usize, 0usize);
    let (mut star, mut mark) = (None, 0usize);
    while t < text.len() {
        if p < pat.len() && (pat[p] == b'?' || pat[p] == text[t]) {
            p += 1;
            t += 1;
        } else if p < pat.len() && pat[p] == b'*' {
            star = Some(p);
            mark = t;
            p += 1;
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

fn kind_reply(kind: &[u8], channel: Option<&[u8]>, count: i64) -> Vec<u8> {
    let mut o = b"*3\r\n".to_vec();
    o.extend_from_slice(&bulk_string(kind));
    match channel {
        Some(c) => o.extend_from_slice(&bulk_string(c)),
        None => o.extend_from_slice(b"$-1\r\n"),
    }
    o.extend_from_slice(format!(":{count}\r\n").as_bytes());
    o
}

pub fn subscribe_reply(channel: &[u8], count: usize) -> Vec<u8> {
    kind_reply(b"subscribe", Some(channel), count as i64)
}
pub fn psubscribe_reply(pat: &[u8], count: usize) -> Vec<u8> {
    kind_reply(b"psubscribe", Some(pat), count as i64)
}
pub fn unsubscribe_reply(channel: Option<&[u8]>, count: usize) -> Vec<u8> {
    kind_reply(b"unsubscribe", channel, count as i64)
}
pub fn punsubscribe_reply(pat: Option<&[u8]>, count: usize) -> Vec<u8> {
    kind_reply(b"punsubscribe", pat, count as i64)
}

pub fn message(channel: &[u8], payload: &[u8]) -> Vec<u8> {
    let mut o = b"*3\r\n".to_vec();
    o.extend_from_slice(&bulk_string(b"message"));
    o.extend_from_slice(&bulk_string(channel));
    o.extend_from_slice(&bulk_string(payload));
    o
}

pub fn pmessage(pattern: &[u8], channel: &[u8], payload: &[u8]) -> Vec<u8> {
    let mut o = b"*4\r\n".to_vec();
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
}
