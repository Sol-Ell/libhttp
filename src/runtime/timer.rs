use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::time::Instant;

use super::slab::ConnId;

/// The deadline category attached to a timer entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum TimerKind {
    HeaderRead,
    BodyIdle,
    ReadStall,
    WriteStall,
    KeepAlive,
    TlsHandshake,
    Total,
    Deferred,
    Connect,
    ServerGrace,
}

/// One generation-checked timer entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct TimerEntry {
    pub(crate) deadline: Instant,
    pub(crate) conn: ConnId,
    pub(crate) seq: u64,
    pub(crate) kind: TimerKind,
}

/// The reactor's single timer authority.
#[derive(Default)]
pub(crate) struct TimerHeap {
    entries: BinaryHeap<Reverse<TimerEntry>>,
}

impl TimerHeap {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn insert(&mut self, entry: TimerEntry) {
        self.entries.push(Reverse(entry));
    }

    pub(crate) fn next_deadline(&self) -> Option<Instant> {
        self.entries.peek().map(|Reverse(entry)| entry.deadline)
    }

    pub(crate) fn pop_due(&mut self, now: Instant) -> Vec<TimerEntry> {
        let mut due = Vec::new();
        while let Some(Reverse(entry)) = self.entries.peek() {
            if entry.deadline > now {
                break;
            }
            due.push(self.entries.pop().unwrap().0);
        }
        due
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn clear(&mut self) {
        self.entries.clear();
    }
}

/// Returns the later of two optional deadlines.
pub(crate) fn later(a: Option<Instant>, b: Option<Instant>) -> Option<Instant> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn id() -> ConnId {
        ConnId {
            slot: 1,
            generation: 1,
        }
    }

    #[test]
    fn pops_in_deadline_order_and_keeps_future_entries() {
        let now = Instant::now();
        let mut heap = TimerHeap::new();
        heap.insert(TimerEntry {
            deadline: now + Duration::from_millis(10),
            conn: id(),
            seq: 1,
            kind: TimerKind::HeaderRead,
        });
        heap.insert(TimerEntry {
            deadline: now + Duration::from_millis(5),
            conn: id(),
            seq: 1,
            kind: TimerKind::BodyIdle,
        });
        heap.insert(TimerEntry {
            deadline: now + Duration::from_secs(1),
            conn: id(),
            seq: 1,
            kind: TimerKind::Total,
        });
        let due = heap.pop_due(now + Duration::from_millis(20));
        assert_eq!(due.len(), 2);
        assert_eq!(due[0].kind, TimerKind::BodyIdle);
        assert_eq!(due[1].kind, TimerKind::HeaderRead);
        assert_eq!(heap.len(), 1);
        assert!(heap.next_deadline().unwrap() > now);
    }

    #[test]
    fn empty_heap_has_no_deadline() {
        let heap = TimerHeap::new();
        assert!(heap.next_deadline().is_none());
    }
}
