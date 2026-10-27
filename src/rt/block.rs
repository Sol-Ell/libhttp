//! Refcounted 16 KiB blocks.
//!
//! A [`Block`] is a range into a single 16 KiB allocation. Splitting a block
//! produces two ranges that share the allocation; the last range to be dropped
//! returns the allocation to the pool it came from.

use std::ptr::NonNull;
use std::slice;
use std::sync::{Arc, Weak};

use super::pool::BlockPool;

/// Size of one block allocation, in bytes.
pub(crate) const BLOCK_SIZE: usize = 16 * 1024;

/// The 16 KiB allocation shared by one or more [`Block`] ranges.
pub(crate) struct BlockInner {
    /// Owns the allocation. Kept as a raw pointer so the 16 KiB slot is freed
    /// exactly once, when the last `Arc` (and therefore this struct) drops.
    storage: NonNull<[u8; BLOCK_SIZE]>,
    origin: Weak<BlockPool>,
}

// SAFETY: `storage` owns the `Box` allocation handed to `BlockInner` and moving
// the struct moves that ownership. The allocation is never resized, moved or
// accessed as a whole while shared.
unsafe impl Send for BlockInner {}

// SAFETY: every `Block` that shares this allocation owns a distinct, disjoint
// sub-range (they are only produced by `split_at`, which partitions the parent
// range). Shared `BlockInner` access is confined to private pointer reads;
// `Block::as_slice`/`as_mut_slice` therefore never hand out overlapping
// references. A mutable reference requires `&mut Block`, and every concurrently
// live range is disjoint from it.
unsafe impl Sync for BlockInner {}

impl BlockInner {
    /// Allocates a fresh, zeroed 16 KiB allocation owned by `origin`.
    pub(crate) fn new(origin: Weak<BlockPool>) -> Arc<Self> {
        let storage: Box<[u8; BLOCK_SIZE]> = vec![0u8; BLOCK_SIZE]
            .into_boxed_slice()
            .try_into()
            .expect("allocation is exactly BLOCK_SIZE bytes");
        let storage = NonNull::new(Box::into_raw(storage)).expect("Box::into_raw is never null");
        Arc::new(Self { storage, origin })
    }

    fn data(&self) -> *const u8 {
        self.storage.as_ptr().cast::<u8>()
    }
}

impl Drop for BlockInner {
    fn drop(&mut self) {
        // SAFETY: `storage` was created by `Box::into_raw` in `new`; this is
        // the only place it is reconstructed, and it runs exactly once.
        unsafe { drop(Box::from_raw(self.storage.as_ptr())) };
    }
}

/// A refcounted range into a 16 KiB allocation.
pub(crate) struct Block {
    inner: Arc<BlockInner>,
    start: usize,
    end: usize,
}

impl Block {
    /// Allocates a block from `pool`, or returns `None` when the pool is at its cap.
    pub(crate) fn alloc(pool: &Arc<BlockPool>) -> Option<Block> {
        pool.alloc()
    }

    /// Wraps a freshly allocated or recycled allocation as the full 0..`BLOCK_SIZE` range.
    pub(crate) fn from_inner(inner: Arc<BlockInner>) -> Self {
        Self {
            inner,
            start: 0,
            end: BLOCK_SIZE,
        }
    }

    /// Length of this range in bytes.
    pub(crate) fn len(&self) -> usize {
        self.end - self.start
    }

    /// Whether this range contains no bytes.
    pub(crate) fn is_empty(&self) -> bool {
        self.start == self.end
    }

    /// The bytes in this range.
    pub(crate) fn as_slice(&self) -> &[u8] {
        // SAFETY: `data` points at a live `BLOCK_SIZE` allocation owned by
        // `BlockInner`, and `start..end` is a sub-range of it. The borrow of
        // `self` keeps the allocation alive; every other range of the same
        // allocation is disjoint.
        unsafe { slice::from_raw_parts(self.inner.data().add(self.start), self.len()) }
    }

    /// The bytes in this range, mutably.
    pub(crate) fn as_mut_slice(&mut self) -> &mut [u8] {
        let len = self.len();
        // SAFETY: as for `as_slice`, plus `&mut self` guarantees exclusive
        // access to this range. Any other `Block` sharing the allocation owns a
        // disjoint range, so the resulting reference cannot alias another live
        // reference.
        unsafe { slice::from_raw_parts_mut(self.inner.data().cast_mut().add(self.start), len) }
    }

    /// Splits this range at `mid` bytes from its start.
    ///
    /// Both halves share the underlying allocation. Panics only when
    /// `mid > self.len()`.
    pub(crate) fn split_at(self, mid: usize) -> (Block, Block) {
        assert!(
            mid <= self.len(),
            "split_at mid {mid} exceeds block length {}",
            self.len()
        );
        let split = self.start + mid;
        let left = Block {
            inner: Arc::clone(&self.inner),
            start: self.start,
            end: split,
        };
        let right = Block {
            inner: Arc::clone(&self.inner),
            start: split,
            end: self.end,
        };
        drop(self);
        (left, right)
    }
}

impl Drop for Block {
    fn drop(&mut self) {
        // The pool stores an `Arc` for every allocation it owns, so a strong
        // count of one means this is the last range. Hand a clone to the origin
        // pool; the clone keeps the allocation alive after our own `Arc` drops.
        if Arc::strong_count(&self.inner) != 1 {
            return;
        }
        if let Some(pool) = self.inner.origin.upgrade() {
            pool.recycle(Arc::clone(&self.inner));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{BLOCK_SIZE, Block};
    use crate::rt::pool::BlockPool;

    #[test]
    fn split_shares_one_allocation() {
        let pool = BlockPool::new(BLOCK_SIZE * 4);
        let mut block = pool.alloc().unwrap();
        block.as_mut_slice()[..6].copy_from_slice(b"abcdef");
        let (left, right) = block.split_at(3);
        assert_eq!(left.as_slice(), b"abc");
        // The brief's assertion compared the whole remainder (BLOCK_SIZE - 3
        // bytes) with `b"def"`; compare the meaningful prefix and the range
        // lengths instead, which is what "splits are ranges, not copies" means.
        assert_eq!(&right.as_slice()[..3], b"def");
        assert_eq!(right.len(), BLOCK_SIZE - 3);
        drop(left);
        assert_eq!(&right.as_slice()[..3], b"def");
    }

    #[test]
    fn pool_is_bounded_and_reuses_blocks() {
        let pool = BlockPool::new(BLOCK_SIZE * 2);
        let a = pool.alloc().unwrap();
        let _b = pool.alloc().unwrap();
        assert!(pool.alloc().is_none());
        drop(a);
        assert_eq!(pool.free_blocks(), 1);
        let _c = pool.alloc().unwrap();
    }

    #[test]
    fn block_returns_to_origin_pool_from_another_thread() {
        let pool = BlockPool::new(BLOCK_SIZE);
        let block = pool.alloc().unwrap();
        std::thread::spawn(move || drop(block)).join().unwrap();
        assert_eq!(pool.free_blocks(), 1);
    }

    #[test]
    fn split_ranges_return_to_origin_from_many_threads() {
        let pool = BlockPool::new(BLOCK_SIZE * 4);
        let mut blocks: Vec<Block> = (0..pool.cap_blocks())
            .map(|_| pool.alloc().unwrap())
            .collect();
        assert!(pool.alloc().is_none());

        let threads: Vec<_> = blocks
            .drain(..)
            .map(|block| {
                std::thread::spawn(move || {
                    let (left, right) = block.split_at(BLOCK_SIZE / 2);
                    drop(left);
                    drop(right);
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(pool.free_blocks(), pool.cap_blocks());
    }

    #[test]
    fn split_at_the_edges_yields_empty_ranges() {
        let pool = BlockPool::new(BLOCK_SIZE);

        let block = pool.alloc().unwrap();
        assert_eq!(block.len(), BLOCK_SIZE);
        let (left, right) = block.split_at(BLOCK_SIZE);
        assert_eq!(left.len(), BLOCK_SIZE);
        assert_eq!(left.as_slice().len(), BLOCK_SIZE);
        assert!(right.is_empty());
        assert!(right.as_slice().is_empty());
        drop((left, right));
        assert_eq!(pool.free_blocks(), 1);

        let block = pool.alloc().unwrap();
        let (left, right) = block.split_at(0);
        assert!(left.is_empty());
        assert_eq!(right.len(), BLOCK_SIZE);
        assert_eq!(right.as_slice().len(), BLOCK_SIZE);
    }

    #[test]
    #[should_panic(expected = "mid")]
    fn split_past_the_end_panics() {
        let pool = BlockPool::new(BLOCK_SIZE);
        let block = pool.alloc().unwrap();
        let _ = block.split_at(BLOCK_SIZE + 1);
    }

    #[test]
    fn block_and_pool_are_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Block>();
        assert_send_sync::<BlockPool>();
    }

    #[test]
    fn pool_cap_is_whole_blocks_and_starts_empty() {
        let pool = BlockPool::new(BLOCK_SIZE * 3 + BLOCK_SIZE / 2);
        assert_eq!(pool.cap_blocks(), 3);
        assert_eq!(pool.free_blocks(), 0);
        let blocks: Vec<Block> = (0..3).map(|_| pool.alloc().unwrap()).collect();
        assert!(pool.alloc().is_none());
        drop(blocks);
        assert_eq!(pool.free_blocks(), 3);
    }

    #[test]
    fn block_alloc_uses_the_pool_cap() {
        let pool = BlockPool::new(BLOCK_SIZE);
        let block = Block::alloc(&pool).unwrap();
        assert_eq!(block.len(), BLOCK_SIZE);
        assert!(Block::alloc(&pool).is_none());
        drop(block);
        assert!(Block::alloc(&pool).is_some());
    }

    #[test]
    fn block_outlives_its_origin_pool() {
        let block = {
            let pool = BlockPool::new(BLOCK_SIZE);
            pool.alloc().unwrap()
        };
        assert_eq!(block.len(), BLOCK_SIZE);
    }
}
