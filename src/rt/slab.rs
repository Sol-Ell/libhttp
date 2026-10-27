//! Generation-checked slab of connection slots.
//!
//! A [`ConnId`] is a slot index plus a generation. Removing a value bumps the
//! slot's generation before the slot is reused, so an id held from a previous
//! occupant is rejected instead of addressing the new one. The slab never
//! shrinks: freed slots stay on a free list and are reused last-in, first-out.

/// Identifies one occupant of a [`Slab`] slot.
///
/// The generation disambiguates a slot reused after its previous occupant was
/// removed: an id from before the removal never matches the new occupant.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ConnId {
    pub(crate) slot: u32,
    pub(crate) generation: u32,
}

struct Slot<T> {
    /// Generation of the value in `value`, or of the next value inserted into
    /// this slot while `value` is `None`.
    generation: u32,
    value: Option<T>,
}

/// Slot table that hands out generation-checked [`ConnId`]s.
pub(crate) struct Slab<T> {
    slots: Vec<Slot<T>>,
    /// Indices of vacant slots, most recently freed last.
    free: Vec<u32>,
    /// Number of occupied slots.
    live: usize,
}

impl<T> Slab<T> {
    pub(crate) fn new() -> Self {
        Self {
            slots: Vec::new(),
            free: Vec::new(),
            live: 0,
        }
    }

    /// Inserts `value` into a free slot and returns its generation-checked id.
    pub(crate) fn insert(&mut self, value: T) -> ConnId {
        let index = match self.free.pop() {
            Some(index) => index as usize,
            None => {
                self.slots.push(Slot {
                    generation: 0,
                    value: None,
                });
                self.slots.len() - 1
            }
        };

        let slot = &mut self.slots[index];
        debug_assert!(
            slot.value.is_none(),
            "free list pointed at an occupied slot"
        );
        slot.value = Some(value);
        self.live += 1;
        ConnId {
            slot: u32::try_from(index).expect("slab slot index exceeds u32"),
            generation: slot.generation,
        }
    }

    /// Returns the value at `id`, or `None` if the id is stale or out of range.
    pub(crate) fn get(&self, id: ConnId) -> Option<&T> {
        let slot = self.slots.get(id.slot as usize)?;
        if slot.generation != id.generation {
            return None;
        }
        slot.value.as_ref()
    }

    /// Returns the value at `id` mutably, or `None` if the id is stale or out of
    /// range.
    pub(crate) fn get_mut(&mut self, id: ConnId) -> Option<&mut T> {
        let slot = self.slots.get_mut(id.slot as usize)?;
        if slot.generation != id.generation {
            return None;
        }
        slot.value.as_mut()
    }

    /// Removes and returns the value at `id`, bumping the slot generation.
    ///
    /// Returns `None` for a stale or out-of-range id, leaving every occupant
    /// untouched.
    pub(crate) fn remove(&mut self, id: ConnId) -> Option<T> {
        let slot = self.slots.get_mut(id.slot as usize)?;
        if slot.generation != id.generation {
            return None;
        }
        let value = slot.value.take()?;
        slot.generation = slot.generation.wrapping_add(1);
        self.free.push(id.slot);
        self.live -= 1;
        Some(value)
    }

    /// Number of occupied slots.
    pub(crate) fn len(&self) -> usize {
        self.live
    }

    /// Whether the slab holds no values.
    pub(crate) fn is_empty(&self) -> bool {
        self.live == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slab_generation_prevents_stale_ids() {
        let mut s = Slab::new();
        let id = s.insert(7u32);
        assert_eq!(s.remove(id), Some(7));
        let id2 = s.insert(9u32);
        assert_eq!(id.slot, id2.slot);
        assert_ne!(id.generation, id2.generation);
        assert!(s.get(id).is_none());
        assert_eq!(s.get(id2), Some(&9));
    }

    #[test]
    fn get_mut_updates_and_len_tracks_live_slots() {
        let mut s = Slab::new();
        assert!(s.is_empty());
        let a = s.insert(1u32);
        let b = s.insert(2u32);
        assert_eq!(s.len(), 2);

        *s.get_mut(a).unwrap() = 10;
        assert_eq!(s.get(a), Some(&10));

        assert_eq!(s.remove(a), Some(10));
        assert_eq!(s.len(), 1);
        assert!(!s.is_empty());
        assert_eq!(s.remove(b), Some(2));
        assert!(s.is_empty());
        assert_eq!(s.len(), 0);
    }

    #[test]
    fn stale_ids_do_not_touch_reused_slots() {
        let mut s = Slab::new();
        let old = s.insert("old");
        assert_eq!(s.remove(old), Some("old"));
        let new = s.insert("new");
        assert_eq!(old.slot, new.slot);

        assert!(s.get(old).is_none());
        assert!(s.get_mut(old).is_none());
        assert_eq!(s.remove(old), None);
        assert_eq!(s.get(new), Some(&"new"));
    }

    #[test]
    fn out_of_range_ids_are_none() {
        let mut s = Slab::<u8>::new();
        let id = ConnId {
            slot: 7,
            generation: 0,
        };
        assert!(s.get(id).is_none());
        assert!(s.get_mut(id).is_none());
        assert!(s.remove(id).is_none());
    }
}
