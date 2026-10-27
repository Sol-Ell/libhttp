//! Generation-checked timer heap.
//!
//! One heap lives per reactor and is the only timer authority. Entries record
//! the connection key (`slot`, `generation`, `exchange_seq`) plus a
//! `timer_generation` that changes on every insert for the same key. A pop or
//! cancel only acts on entries whose key still maps to the entry's timer
//! generation, so a reinserted or cancelled timer's stale heap entry is
//! silently skipped.

use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap};
use std::time::Instant;

/// Identifies the connection (and, from M2, the exchange) a timer belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct TimerKey {
    pub(crate) slot: u32,
    pub(crate) generation: u32,
    pub(crate) exchange_seq: u32,
}

/// Handle to one inserted timer. Safe to pass around and cancel after it has
/// already fired: a stale token is a no-op.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TimerToken {
    pub(crate) key: TimerKey,
    pub(crate) timer_generation: u32,
}

/// A heap entry. Ordered by deadline, then by the identity fields so that the
/// ordering is total and deterministic even when two deadlines are equal.
struct TimerEntry {
    deadline: Instant,
    kind: u16,
    token: TimerToken,
}

impl PartialEq for TimerEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for TimerEntry {}

impl PartialOrd for TimerEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TimerEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.deadline
            .cmp(&other.deadline)
            .then_with(|| self.token.key.slot.cmp(&other.token.key.slot))
            .then_with(|| self.token.key.generation.cmp(&other.token.key.generation))
            .then_with(|| {
                self.token
                    .key
                    .exchange_seq
                    .cmp(&other.token.key.exchange_seq)
            })
            .then_with(|| {
                self.token
                    .timer_generation
                    .cmp(&other.token.timer_generation)
            })
            .then_with(|| self.kind.cmp(&other.kind))
    }
}

/// Generation-checked min-heap of reactor deadlines.
pub(crate) struct TimerHeap {
    heap: BinaryHeap<Reverse<TimerEntry>>,
    /// Live timer generation for each key. An entry is live only when this map
    /// still holds exactly its timer generation.
    live: HashMap<TimerKey, u32>,
    next_timer_generation: u32,
}

impl TimerHeap {
    pub(crate) fn new() -> Self {
        Self {
            heap: BinaryHeap::new(),
            live: HashMap::new(),
            next_timer_generation: 0,
        }
    }

    /// Inserts a deadline for `key`, replacing any timer currently live for
    /// that exact key. The returned token must be passed to [`Self::cancel`]
    /// to cancel it.
    pub(crate) fn insert(&mut self, key: TimerKey, deadline: Instant, kind: u16) -> TimerToken {
        self.next_timer_generation = self.next_timer_generation.wrapping_add(1);
        let token = TimerToken {
            key,
            timer_generation: self.next_timer_generation,
        };
        self.live.insert(key, token.timer_generation);
        self.heap.push(Reverse(TimerEntry {
            deadline,
            kind,
            token,
        }));
        token
    }

    /// Cancels `token` if it is still the live timer for its key. Cancelling a
    /// stale or already-fired token is a no-op.
    pub(crate) fn cancel(&mut self, token: TimerToken) {
        if self.live.get(&token.key) == Some(&token.timer_generation) {
            self.live.remove(&token.key);
        }
    }

    /// Earliest deadline among live entries, without popping anything.
    ///
    /// This scans the heap: a cancelled entry can sit above a live one, so the
    /// top of the heap is not necessarily the next live deadline.
    pub(crate) fn next_deadline(&self) -> Option<Instant> {
        self.heap
            .iter()
            .filter(|entry| self.is_live(&entry.0))
            .map(|entry| entry.0.deadline)
            .min()
    }

    /// Removes and returns every live entry whose deadline is at or before
    /// `now`, in deadline order. Stale entries are discarded as they surface.
    pub(crate) fn pop_expired(&mut self, now: Instant) -> Vec<(u16, TimerToken)> {
        let mut expired = Vec::new();
        while let Some(top) = self.heap.peek() {
            if top.0.deadline > now {
                break;
            }
            let Reverse(entry) = self.heap.pop().expect("heap was just peeked");
            if self.is_live(&entry) {
                // The timer is no longer pending once it fires; drop its live
                // marker so fired tokens do not accumulate in the map.
                self.live.remove(&entry.token.key);
                expired.push((entry.kind, entry.token));
            }
        }
        expired
    }

    fn is_live(&self, entry: &TimerEntry) -> bool {
        self.live.get(&entry.token.key) == Some(&entry.token.timer_generation)
    }
}

#[cfg(test)]
mod tests {
    use super::{TimerHeap, TimerKey};
    use std::time::{Duration, Instant};

    fn key(slot: u32) -> TimerKey {
        TimerKey {
            slot,
            generation: 0,
            exchange_seq: 0,
        }
    }

    #[test]
    fn pops_in_deadline_order() {
        let mut h = TimerHeap::new();
        let base = Instant::now();
        let later = h.insert(key(1), base + Duration::from_millis(20), 1);
        let sooner = h.insert(key(2), base + Duration::from_millis(10), 2);
        let got = h.pop_expired(base + Duration::from_millis(30));
        assert_eq!(got, vec![(2, sooner), (1, later)]);
    }

    #[test]
    fn cancel_skips_the_entry() {
        let mut h = TimerHeap::new();
        let t = h.insert(key(7), Instant::now(), 7);
        h.cancel(t);
        assert!(
            h.pop_expired(Instant::now() + Duration::from_secs(1))
                .is_empty()
        );
    }

    #[test]
    fn timer_reinsert_cancels_previous() {
        let mut h = TimerHeap::new();
        let old = h.insert(key(7), Instant::now(), 7);
        let new = h.insert(key(7), Instant::now() + Duration::from_secs(10), 7);
        let got = h.pop_expired(Instant::now() + Duration::from_secs(1));
        assert!(got.is_empty());
        let got = h.pop_expired(Instant::now() + Duration::from_secs(11));
        assert_eq!(got, vec![(7, new)]);
        assert_ne!(old.timer_generation, new.timer_generation);
    }

    #[test]
    fn stale_timer_generation_is_skipped() {
        let mut h = TimerHeap::new();
        let t = h.insert(key(7), Instant::now(), 7);
        h.cancel(t);
        let newer = h.insert(key(7), Instant::now(), 7);
        let got = h.pop_expired(Instant::now() + Duration::from_secs(1));
        assert_eq!(got, vec![(7, newer)]);
    }

    #[test]
    fn stale_token_cancel_does_not_cancel_the_reinsert() {
        let mut h = TimerHeap::new();
        let base = Instant::now();
        let old = h.insert(key(7), base, 7);
        let new = h.insert(key(7), base + Duration::from_secs(10), 7);
        h.cancel(old);
        let got = h.pop_expired(base + Duration::from_secs(11));
        assert_eq!(got, vec![(7, new)]);
    }

    #[test]
    fn cancelling_a_fired_token_does_not_cancel_a_newer_timer() {
        let mut h = TimerHeap::new();
        let base = Instant::now();
        let fired = h.insert(key(7), base, 7);
        assert_eq!(h.pop_expired(base), vec![(7, fired)]);
        let newer = h.insert(key(7), base + Duration::from_secs(10), 7);
        h.cancel(fired);
        let got = h.pop_expired(base + Duration::from_secs(11));
        assert_eq!(got, vec![(7, newer)]);
    }

    #[test]
    fn next_deadline_skips_cancelled_entries_without_popping() {
        let mut h = TimerHeap::new();
        let base = Instant::now();
        let sooner = h.insert(key(1), base + Duration::from_millis(10), 1);
        let later = h.insert(key(2), base + Duration::from_millis(20), 2);
        assert_eq!(h.next_deadline(), Some(base + Duration::from_millis(10)));
        h.cancel(sooner);
        assert_eq!(h.next_deadline(), Some(base + Duration::from_millis(20)));
        // Peeking does not consume the entry.
        assert_eq!(h.next_deadline(), Some(base + Duration::from_millis(20)));
        assert_eq!(
            h.pop_expired(base + Duration::from_millis(20)),
            vec![(2, later)]
        );
    }

    #[test]
    fn next_deadline_is_none_when_nothing_is_live() {
        let mut h = TimerHeap::new();
        assert_eq!(h.next_deadline(), None);
        let t = h.insert(key(3), Instant::now(), 3);
        h.cancel(t);
        assert_eq!(h.next_deadline(), None);
    }
}
