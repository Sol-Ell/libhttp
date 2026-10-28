use std::collections::VecDeque;

use mio::Token;

/// A generation-checked connection slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct ConnId {
    pub(crate) slot: u32,
    pub(crate) generation: u32,
}

impl ConnId {
    pub(crate) fn token(self) -> Token {
        Token(((self.slot as usize) << 32) | self.generation as usize)
    }

    pub(crate) fn from_token(token: Token) -> Option<Self> {
        let raw = token.0;
        let slot = (raw >> 32) as u32;
        let generation = raw as u32;
        if generation == 0 {
            return None;
        }
        Some(Self { slot, generation })
    }
}

struct Slot<T> {
    generation: u32,
    value: Option<T>,
}

/// A simple slot table with generation-checked IDs.
pub(crate) struct Slab<T> {
    slots: Vec<Slot<T>>,
    free: VecDeque<u32>,
    len: usize,
}

impl<T> Slab<T> {
    pub(crate) fn new() -> Self {
        Self {
            slots: Vec::new(),
            free: VecDeque::new(),
            len: 0,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn insert(&mut self, value: T) -> ConnId {
        let slot = if let Some(slot) = self.free.pop_front() {
            slot
        } else {
            self.slots.push(Slot {
                generation: 0,
                value: None,
            });
            (self.slots.len() - 1) as u32
        };
        let entry = &mut self.slots[slot as usize];
        entry.generation = entry.generation.wrapping_add(1);
        if entry.generation == 0 {
            entry.generation = 1;
        }
        entry.value = Some(value);
        self.len += 1;
        ConnId {
            slot,
            generation: entry.generation,
        }
    }

    pub(crate) fn get(&self, id: ConnId) -> Option<&T> {
        let entry = self.slots.get(id.slot as usize)?;
        if entry.generation != id.generation {
            return None;
        }
        entry.value.as_ref()
    }

    pub(crate) fn get_mut(&mut self, id: ConnId) -> Option<&mut T> {
        let entry = self.slots.get_mut(id.slot as usize)?;
        if entry.generation != id.generation {
            return None;
        }
        entry.value.as_mut()
    }

    pub(crate) fn remove(&mut self, id: ConnId) -> Option<T> {
        let entry = self.slots.get_mut(id.slot as usize)?;
        if entry.generation != id.generation {
            return None;
        }
        let value = entry.value.take()?;
        entry.generation = entry.generation.wrapping_add(1);
        if entry.generation == 0 {
            entry.generation = 1;
        }
        self.free.push_back(id.slot);
        self.len -= 1;
        Some(value)
    }

    pub(crate) fn ids(&self) -> Vec<ConnId> {
        self.slots
            .iter()
            .enumerate()
            .filter_map(|(slot, entry)| {
                entry.value.as_ref()?;
                Some(ConnId {
                    slot: slot as u32,
                    generation: entry.generation,
                })
            })
            .collect()
    }

    pub(crate) fn values_mut(&mut self) -> impl Iterator<Item = &mut T> {
        self.slots
            .iter_mut()
            .filter_map(|entry| entry.value.as_mut())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generation_checks_stale_ids() {
        let mut slab = Slab::new();
        let first = slab.insert("a");
        assert_eq!(slab.get(first), Some(&"a"));
        assert_eq!(slab.remove(first), Some("a"));
        assert!(slab.get(first).is_none());
        let second = slab.insert("b");
        assert_eq!(first.slot, second.slot);
        assert_ne!(first.generation, second.generation);
        assert!(slab.get(first).is_none());
        assert_eq!(slab.get(second), Some(&"b"));
    }

    #[test]
    fn token_round_trip() {
        let id = ConnId {
            slot: 7,
            generation: 3,
        };
        assert_eq!(ConnId::from_token(id.token()), Some(id));
    }

    #[test]
    fn free_slots_are_reused() {
        let mut slab = Slab::new();
        let a = slab.insert(1);
        let b = slab.insert(2);
        slab.remove(a);
        let c = slab.insert(3);
        assert_eq!(c.slot, a.slot);
        assert_eq!(slab.get(b), Some(&2));
        assert_eq!(slab.get(c), Some(&3));
        assert_eq!(slab.len(), 2);
    }
}
