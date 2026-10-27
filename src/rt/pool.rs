//! Bounded, cross-thread-safe pool of 16 KiB block allocations.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use super::block::{BLOCK_SIZE, Block, BlockInner};

/// A bounded pool of block allocations.
///
/// A pool never allocates more than `cap_bytes / BLOCK_SIZE` allocations. Free
/// allocations are reused; a block dropped on any thread returns its allocation
/// to the pool it came from, provided that pool is still alive.
pub(crate) struct BlockPool {
    cap_blocks: usize,
    state: Mutex<PoolState>,
}

#[derive(Default)]
struct PoolState {
    /// Allocations currently parked in the pool, ready to be reused.
    free: Vec<Arc<BlockInner>>,
    /// Allocations owned by this pool, whether parked or checked out.
    allocated: usize,
}

impl BlockPool {
    /// Creates a pool whose capacity is `cap_bytes`, rounded down to whole blocks.
    pub(crate) fn new(cap_bytes: usize) -> Arc<Self> {
        Arc::new(Self {
            cap_blocks: cap_bytes / BLOCK_SIZE,
            state: Mutex::new(PoolState::default()),
        })
    }

    /// Allocates a block, reusing a parked allocation when possible.
    ///
    /// Returns `None` once the pool has handed out all of its capacity. This
    /// method requires an `Arc<Self>` because returned blocks hold only a weak
    /// origin reference; the pool must be owned by an `Arc` for the block to
    /// find its way home.
    pub(crate) fn alloc(self: &Arc<Self>) -> Option<Block> {
        let reused = {
            let mut state = self.state();
            match state.free.pop() {
                Some(inner) => Some(inner),
                None => {
                    if state.allocated >= self.cap_blocks {
                        return None;
                    }
                    state.allocated += 1;
                    None
                }
            }
        };

        let inner = match reused {
            Some(inner) => inner,
            // Allocate outside the lock so concurrent allocations only contend
            // on the accounting, not on the 16 KiB allocation itself.
            None => BlockInner::new(Arc::downgrade(self)),
        };
        Some(Block::from_inner(inner))
    }

    /// Number of allocations currently parked in the pool.
    pub(crate) fn free_blocks(&self) -> usize {
        self.state().free.len()
    }

    /// Maximum number of allocations this pool may own.
    pub(crate) fn cap_blocks(&self) -> usize {
        self.cap_blocks
    }

    /// Returns an allocation to the pool.
    ///
    /// Called by the last [`Block`] range when it is dropped. The allocation is
    /// parked for reuse when there is room, and freed otherwise.
    pub(crate) fn recycle(&self, inner: Arc<BlockInner>) {
        let mut state = self.state();
        if state.free.len() < self.cap_blocks {
            state.free.push(inner);
        } else {
            // A returned allocation was checked out, so this is unreachable
            // while the accounting is consistent. If it ever happens, let the
            // allocation drop and keep the capacity counter truthful.
            state.allocated -= 1;
        }
    }

    fn state(&self) -> MutexGuard<'_, PoolState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}
