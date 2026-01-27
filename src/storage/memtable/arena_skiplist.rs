//! Lock-free arena-based skip list.
//!
//! This is a concurrent skip list implementation that allocates all nodes from
//! a PageArena. Based on the crossbeam-skiplist algorithm but adapted for arena
//! allocation (no reference counting, no epoch-based reclamation).
//!
//! # Key Differences from crossbeam-skiplist
//!
//! 1. **Arena allocation**: All nodes allocated from PageArena, no individual deallocation
//! 2. **No reference counting**: Nodes persist until arena is dropped
//! 3. **No epoch-based GC**: Memory is freed in bulk when arena is dropped
//! 4. **Simplified deletion**: Mark-based logical deletion, physical removal during traversal
//!
//! # Thread Safety
//!
//! The skip list is fully lock-free for reads and insertions. Concurrent access
//! from multiple threads is safe without external synchronization.
//!
//! # Lifetime Safety
//!
//! The skip list is tied to the arena's lifetime. The arena must outlive the skip list.
//!
//! # Deletion Semantics
//!
//! Deleted nodes are marked (using pointer tag bits) but not physically removed
//! until the arena is dropped. Searches skip over marked nodes.

// Skip list requires indexing into tower arrays at specific levels
#![allow(clippy::needless_range_loop)]

use crate::util::arena::PageArena;
use std::borrow::Borrow;
use std::cmp::Ordering as CmpOrdering;
use std::marker::PhantomData;
use std::mem;
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};

/// Maximum height of the skip list (same as crossbeam)
const MAX_HEIGHT: usize = 20;

/// Bit mask for the deletion mark in pointers
const MARK_BIT: usize = 1;

/// A lock-free skip list with arena allocation.
///
/// The lifetime `'a` ties this skip list to its arena - the arena must outlive
/// the skip list to ensure memory safety.
pub struct ArenaSkipList<'a, K: Ord + Send + Sync, V: Send + Sync> {
    /// Reference to the arena for allocating nodes (lifetime-bound)
    arena: &'a PageArena,

    /// Head node (sentinel with key/value set to defaults, has MAX_HEIGHT tower)
    head: *mut SkipNode<K, V>,

    /// Head of the "all allocated nodes" list for proper Drop
    /// This is a singly-linked list of all nodes (including unlinked/duplicate ones)
    all_nodes_head: AtomicPtr<SkipNode<K, V>>,

    /// Current maximum height in use (for optimization)
    max_height: AtomicU64,

    /// Random state for height generation (Xorshift)
    seed: AtomicU64,

    /// Number of entries (approximate, for statistics)
    len: AtomicU64,

    /// Marker for K and V ownership
    _marker: PhantomData<(&'a (), K, V)>,
}

// Safety: ArenaSkipList can be sent between threads and shared across threads because:
// 1. The `head` raw pointer points to arena-allocated memory that lives as long as the arena
// 2. All mutable access to the skip list structure uses atomic CAS operations
// 3. The lifetime 'a ensures the arena (and thus all node memory) outlives the skip list
// 4. K and V are required to be Send + Sync
//
// The `head: *mut SkipNode` field is why we need manual impls (raw pointers are !Send !Sync),
// but our usage is safe because the pointer remains valid for the arena's lifetime.
unsafe impl<'a, K: Ord + Send + Sync, V: Send + Sync> Send for ArenaSkipList<'a, K, V> {}
unsafe impl<'a, K: Ord + Send + Sync, V: Send + Sync> Sync for ArenaSkipList<'a, K, V> {}

/// A node in the skip list.
#[repr(C)]
struct SkipNode<K, V> {
    /// The key (uninitialized for head node)
    key: mem::MaybeUninit<K>,

    /// The value (uninitialized for head node)
    value: mem::MaybeUninit<V>,

    /// Height of this node (1..=MAX_HEIGHT)
    height: u8,

    /// Is this the sentinel head node?
    is_head: bool,

    /// Tower of next pointers (one per level).
    /// The low bit of each pointer is used as a deletion mark.
    tower: [AtomicPtr<SkipNode<K, V>>; MAX_HEIGHT],

    /// Next pointer in the "all allocated nodes" chain (for Drop)
    /// This is separate from the skip list structure and only used during cleanup.
    next_allocated: AtomicPtr<SkipNode<K, V>>,
}

impl<K, V> SkipNode<K, V> {
    /// Get the next pointer with the mark bit cleared.
    #[inline]
    fn unmark(ptr: *mut SkipNode<K, V>) -> *mut SkipNode<K, V> {
        (ptr as usize & !MARK_BIT) as *mut SkipNode<K, V>
    }

    /// Get a marked pointer (for deletion).
    #[inline]
    fn mark(ptr: *mut SkipNode<K, V>) -> *mut SkipNode<K, V> {
        (ptr as usize | MARK_BIT) as *mut SkipNode<K, V>
    }

    /// Check if a pointer is marked.
    #[inline]
    fn is_ptr_marked(ptr: *mut SkipNode<K, V>) -> bool {
        (ptr as usize & MARK_BIT) != 0
    }

    /// Check if node is marked at level 0 (primary deletion indicator)
    #[inline]
    fn is_deleted(&self) -> bool {
        Self::is_ptr_marked(self.tower[0].load(Ordering::Acquire))
    }

    /// Get key reference (unsafe: assumes key is initialized and node is not head)
    #[inline]
    unsafe fn key_ref(&self) -> &K {
        debug_assert!(!self.is_head);
        self.key.assume_init_ref()
    }

    /// Get value reference (unsafe: assumes value is initialized and node is not head)
    #[inline]
    unsafe fn value_ref(&self) -> &V {
        debug_assert!(!self.is_head);
        self.value.assume_init_ref()
    }
}

impl<'a, K, V> ArenaSkipList<'a, K, V>
where
    K: Ord + Send + Sync,
    V: Send + Sync,
{
    /// Create a new skip list using the given arena.
    ///
    /// The arena must outlive the skip list (enforced by lifetime parameter).
    pub fn new(arena: &'a PageArena) -> Self {
        // Allocate head node with MAX_HEIGHT
        let head = Self::alloc_head_node(arena);

        // Initialize with a random seed
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0x12345678)
            | 1;

        Self {
            arena,
            head,
            all_nodes_head: AtomicPtr::new(ptr::null_mut()),
            max_height: AtomicU64::new(1),
            seed: AtomicU64::new(seed),
            len: AtomicU64::new(0),
            _marker: PhantomData,
        }
    }

    /// Allocate the sentinel head node.
    fn alloc_head_node(arena: &PageArena) -> *mut SkipNode<K, V> {
        let size = mem::size_of::<SkipNode<K, V>>();
        let align = mem::align_of::<SkipNode<K, V>>();

        let ptr = arena
            .alloc(size, align)
            .expect("Failed to allocate skip list head")
            .as_ptr() as *mut SkipNode<K, V>;

        // SAFETY: `ptr` is freshly allocated from the arena with correct size and alignment
        // for `SkipNode<K, V>`. We have exclusive access to this memory. We use `ptr::write`
        // for the AtomicPtr fields because the memory is uninitialized. The `key` and `value`
        // fields are `MaybeUninit` so they don't need initialization. This is the head node
        // (sentinel), so `key` and `value` will never be read.
        unsafe {
            (*ptr).height = MAX_HEIGHT as u8;
            (*ptr).is_head = true;
            // Initialize all tower pointers to null
            for i in 0..MAX_HEIGHT {
                ptr::write(&mut (*ptr).tower[i], AtomicPtr::new(ptr::null_mut()));
            }
            // next_allocated is null for head (head is not in the all_nodes list)
            ptr::write(&mut (*ptr).next_allocated, AtomicPtr::new(ptr::null_mut()));
            // key and value are left uninitialized (MaybeUninit)
        }

        ptr
    }

    /// Allocate a new node from the arena and add it to the all_nodes chain.
    fn alloc_node(&self, key: K, value: V, height: usize) -> *mut SkipNode<K, V> {
        let size = mem::size_of::<SkipNode<K, V>>();
        let align = mem::align_of::<SkipNode<K, V>>();

        let ptr = self
            .arena
            .alloc(size, align)
            .expect("Failed to allocate skip list node")
            .as_ptr() as *mut SkipNode<K, V>;

        // SAFETY: `ptr` is freshly allocated from the arena with correct size and alignment
        // for `SkipNode<K, V>`. We have exclusive access to this memory until we publish it
        // via the CAS on `all_nodes_head`. We use `ptr::write` to initialize all fields
        // because the memory is uninitialized. The key and value are moved into the node.
        // After the CAS succeeds, other threads may access this node, but only after seeing
        // the Release barrier from the CAS.
        unsafe {
            ptr::write((*ptr).key.as_mut_ptr(), key);
            ptr::write((*ptr).value.as_mut_ptr(), value);
            (*ptr).height = height as u8;
            (*ptr).is_head = false;

            for i in 0..MAX_HEIGHT {
                ptr::write(&mut (*ptr).tower[i], AtomicPtr::new(ptr::null_mut()));
            }

            // Add to all_nodes chain (lock-free push to front)
            loop {
                let old_head = self.all_nodes_head.load(Ordering::Acquire);
                ptr::write(&mut (*ptr).next_allocated, AtomicPtr::new(old_head));

                if self
                    .all_nodes_head
                    .compare_exchange_weak(old_head, ptr, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    break;
                }
            }
        }

        ptr
    }

    /// Generate a random height for a new node.
    fn random_height(&self) -> usize {
        let mut num = self.seed.load(Ordering::Relaxed);
        num ^= num << 13;
        num ^= num >> 17;
        num ^= num << 5;
        self.seed.store(num, Ordering::Relaxed);

        let height = (num.trailing_zeros() as usize + 1).min(MAX_HEIGHT);
        let current_max = self.max_height.load(Ordering::Relaxed) as usize;
        height.min(current_max + 2)
    }

    /// Search for a key, returning predecessor and successor nodes at each level.
    ///
    /// Returns (found_node, preds, succs) where:
    /// - found_node: pointer to node with matching key (null if not found)
    /// - preds[i]: predecessor node at level i (never null - head is used for empty levels)
    /// - succs[i]: successor node at level i (may be null)
    #[allow(clippy::type_complexity)]
    fn search<Q>(
        &self,
        key: &Q,
    ) -> (
        *mut SkipNode<K, V>,
        [*mut SkipNode<K, V>; MAX_HEIGHT],
        [*mut SkipNode<K, V>; MAX_HEIGHT],
    )
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        // Initialize all preds to head (for levels above max_height)
        let mut preds = [self.head; MAX_HEIGHT];
        let mut succs = [ptr::null_mut(); MAX_HEIGHT];
        let mut found = ptr::null_mut();

        'search: loop {
            // Reset for retry - all levels above max_height use head as predecessor
            let mut level = self.max_height.load(Ordering::Relaxed) as usize;
            if level > MAX_HEIGHT {
                level = MAX_HEIGHT;
            }

            // For levels >= max_height, preds[i] should be head, succs[i] should be null
            // (already initialized correctly above, but reset on retry)
            for i in level..MAX_HEIGHT {
                preds[i] = self.head;
                succs[i] = ptr::null_mut();
            }

            let mut pred = self.head;

            while level > 0 {
                level -= 1;

                // SAFETY: `pred` is either `self.head` (valid for arena lifetime) or a node
                // we traversed to via atomic loads with Acquire ordering. Nodes are never
                // deallocated until the arena is dropped, so the pointer remains valid.
                let mut curr = unsafe { (*pred).tower[level].load(Ordering::Acquire) };

                loop {
                    // Check for null pointer (including marked null which is 0x1)
                    if curr.is_null() {
                        preds[level] = pred;
                        succs[level] = ptr::null_mut();
                        break;
                    }

                    // curr should never be marked since we mark outgoing pointers,
                    // not incoming pointers. But handle it defensively.
                    let curr_clean = SkipNode::unmark(curr);
                    if curr_clean.is_null() {
                        preds[level] = pred;
                        succs[level] = ptr::null_mut();
                        break;
                    }

                    // SAFETY: `curr_clean` is a valid, unmarked pointer to a node. We checked
                    // it's non-null above. Nodes are never deallocated until arena Drop.
                    let next = unsafe { (*curr_clean).tower[level].load(Ordering::Acquire) };

                    // If next is marked, curr_clean is being deleted.
                    // Try to help unlink it by updating pred to skip curr_clean.
                    if SkipNode::is_ptr_marked(next) {
                        let next_clean = SkipNode::unmark(next);
                        // CAS pred->curr to pred->next (unmarked)
                        // SAFETY: `pred` is valid (same reasoning as above). We perform a CAS
                        // to atomically unlink `curr_clean` from this level. This is the
                        // "helping" mechanism of lock-free deletion.
                        let result = unsafe {
                            (*pred).tower[level].compare_exchange(
                                curr_clean, // expect unmarked pointer to curr
                                next_clean, // set to unmarked pointer to next
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            )
                        };

                        match result {
                            Ok(_) => {
                                // Successfully unlinked, continue with next
                                curr = next_clean;
                                continue;
                            }
                            Err(_) => {
                                // CAS failed, restart search from this level
                                continue 'search;
                            }
                        }
                    }

                    // Compare keys
                    // SAFETY: `curr_clean` is valid and non-head (head nodes don't appear
                    // in the middle of the list). The key was initialized in `alloc_node`.
                    let cmp = unsafe { (*curr_clean).key_ref().borrow().cmp(key) };

                    match cmp {
                        CmpOrdering::Less => {
                            pred = curr_clean;
                            curr = next;
                        }
                        CmpOrdering::Equal => {
                            found = curr_clean;
                            preds[level] = pred;
                            succs[level] = curr_clean;
                            break;
                        }
                        CmpOrdering::Greater => {
                            preds[level] = pred;
                            succs[level] = curr_clean;
                            break;
                        }
                    }
                }
            }

            return (found, preds, succs);
        }
    }

    /// Insert a key-value pair into the skip list.
    ///
    /// If the key already exists, returns Some(&existing_value) without modifying.
    /// If this is a new key, returns None.
    pub fn insert(&self, key: K, value: V) -> Option<&V> {
        // First, check if the key already exists (without allocating)
        let (found, _, _) = self.search(&key);
        // SAFETY: If `found` is non-null, it points to a valid node returned by `search`.
        // Nodes are never deallocated, so the pointer remains valid. `is_deleted` and
        // `value_ref` both access fields that were properly initialized in `alloc_node`.
        if !found.is_null() && !unsafe { (*found).is_deleted() } {
            // Key exists, return existing value without allocating a new node
            return Some(unsafe { (*found).value_ref() });
        }

        // Key doesn't exist, allocate and insert
        let height = self.random_height();
        let new_node = self.alloc_node(key, value, height);

        loop {
            // SAFETY: `new_node` was just allocated by us and its key was initialized.
            let (found, preds, succs) = self.search(unsafe { (*new_node).key_ref() });

            // SAFETY: Same as above - if found is non-null, it's a valid node.
            if !found.is_null() && !unsafe { (*found).is_deleted() } {
                // Key was inserted by another thread while we were allocating
                // The node is already in all_nodes chain, it will be dropped properly
                // Return the existing value
                return Some(unsafe { (*found).value_ref() });
            }

            // Set up new node's tower pointers
            for level in 0..height {
                // SAFETY: `new_node` is our exclusively-owned node (not yet published).
                // We're setting up its tower pointers before linking it into the list.
                unsafe {
                    (*new_node).tower[level].store(succs[level], Ordering::Relaxed);
                }
            }

            // Try to link at level 0 (linearization point)
            let pred = preds[0];
            let succ = succs[0];

            // SAFETY: `pred` is a valid node from `search`. The CAS atomically publishes
            // `new_node` into the list. After success, other threads may access it.
            let result = unsafe {
                (*pred).tower[0].compare_exchange(
                    succ,
                    new_node,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
            };

            match result {
                Ok(_) => {
                    // Successfully inserted at level 0
                    // Now link at higher levels
                    for level in 1..height {
                        loop {
                            // SAFETY: `new_node` is now in the list (linked at level 0).
                            // Its key is valid and initialized.
                            let (_, preds, succs) = self.search(unsafe { (*new_node).key_ref() });

                            // Check if node was removed concurrently
                            // SAFETY: `new_node` is valid for arena lifetime.
                            if unsafe { (*new_node).is_deleted() } {
                                break;
                            }

                            let pred = preds[level];
                            let succ = succs[level];

                            // Update new_node's next pointer at this level
                            // SAFETY: `new_node` is valid; tower was initialized in `alloc_node`.
                            let current_next =
                                unsafe { (*new_node).tower[level].load(Ordering::Relaxed) };

                            if current_next != succ && !SkipNode::is_ptr_marked(current_next) {
                                // SAFETY: Updating our own node's tower pointer.
                                let cas_result = unsafe {
                                    (*new_node).tower[level].compare_exchange(
                                        current_next,
                                        succ,
                                        Ordering::AcqRel,
                                        Ordering::Acquire,
                                    )
                                };
                                if cas_result.is_err() {
                                    // SAFETY: Same as above.
                                    if unsafe { (*new_node).is_deleted() } {
                                        break;
                                    }
                                    continue;
                                }
                            }

                            // Try to link pred -> new_node at this level
                            // SAFETY: `pred` is a valid node from `search`.
                            let link_result = unsafe {
                                (*pred).tower[level].compare_exchange(
                                    succ,
                                    new_node,
                                    Ordering::AcqRel,
                                    Ordering::Acquire,
                                )
                            };

                            match link_result {
                                Ok(_) => break,
                                Err(_) => continue,
                            }
                        }
                    }

                    // Update max height
                    let mut current_max = self.max_height.load(Ordering::Relaxed) as usize;
                    while height > current_max {
                        match self.max_height.compare_exchange_weak(
                            current_max as u64,
                            height as u64,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        ) {
                            Ok(_) => break,
                            Err(actual) => current_max = actual as usize,
                        }
                    }

                    self.len.fetch_add(1, Ordering::Relaxed);
                    return None;
                }
                Err(_) => continue,
            }
        }
    }

    /// Get a reference to the value associated with a key.
    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let (found, _, _) = self.search(key);

        if found.is_null() {
            return None;
        }

        // SAFETY: `found` is a valid, non-null node returned by `search`. The node was
        // properly initialized in `alloc_node` and is never deallocated until arena Drop.
        // `is_deleted` checks the mark bit; `value_ref` returns the initialized value.
        if unsafe { (*found).is_deleted() } {
            return None;
        }

        Some(unsafe { (*found).value_ref() })
    }

    /// Check if the skip list contains a key.
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.get(key).is_some()
    }

    /// Remove a key from the skip list.
    ///
    /// Returns `true` if the key was found and removed, `false` otherwise.
    pub fn remove<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let (found, _, _) = self.search(key);

        if found.is_null() {
            return false;
        }

        // SAFETY: `found` is a valid, non-null node returned by `search`. The node's
        // fields were initialized in `alloc_node` and remain valid until arena Drop.
        if unsafe { (*found).is_deleted() } {
            return false;
        }

        // SAFETY: `found` is valid and `height` was set in `alloc_node`.
        let height = unsafe { (*found).height } as usize;

        // Mark from top to bottom
        for level in (1..height).rev() {
            loop {
                // SAFETY: `found` is valid; tower was initialized in `alloc_node`.
                let next = unsafe { (*found).tower[level].load(Ordering::Acquire) };

                if SkipNode::is_ptr_marked(next) {
                    break;
                }

                let marked = SkipNode::mark(next);
                // SAFETY: Marking our own tower pointer to indicate logical deletion.
                let result = unsafe {
                    (*found).tower[level].compare_exchange(
                        next,
                        marked,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                };

                if result.is_ok() {
                    break;
                }
            }
        }

        // Mark level 0 (linearization point)
        loop {
            // SAFETY: `found` is valid; this is the linearization point for deletion.
            let next = unsafe { (*found).tower[0].load(Ordering::Acquire) };

            if SkipNode::is_ptr_marked(next) {
                return false;
            }

            let marked = SkipNode::mark(next);
            // SAFETY: CAS on level 0 is the linearization point. Success means we
            // logically deleted the node. The node remains in memory until arena Drop.
            let result = unsafe {
                (*found).tower[0].compare_exchange(
                    next,
                    marked,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
            };

            match result {
                Ok(_) => {
                    self.len.fetch_sub(1, Ordering::Relaxed);
                    // Trigger a search to help unlink
                    let _ = self.search(key);
                    return true;
                }
                Err(_) => continue,
            }
        }
    }

    /// Get the approximate number of entries.
    pub fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed) as usize
    }

    /// Check if the skip list is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Create an iterator over the skip list entries in key order.
    pub fn iter(&self) -> Iter<'_, K, V> {
        Iter {
            list: self,
            current: ptr::null(),
            started: false,
        }
    }

    /// Create an iterator starting from the given key (inclusive).
    pub fn iter_from<Q>(&self, key: &Q) -> Iter<'_, K, V>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let (found, _, succs) = self.search(key);

        let start = if !found.is_null() { found } else { succs[0] };

        Iter {
            list: self,
            current: start,
            started: true,
        }
    }
}

/// An iterator over skip list entries.
pub struct Iter<'a, K: Ord + Send + Sync, V: Send + Sync> {
    list: &'a ArenaSkipList<'a, K, V>,
    current: *const SkipNode<K, V>,
    started: bool,
}

impl<'a, K, V> Iterator for Iter<'a, K, V>
where
    K: Ord + Send + Sync,
    V: Send + Sync,
{
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if !self.started {
                // Start from head's first successor
                // SAFETY: `self.list.head` is valid for the arena's lifetime.
                let first = unsafe { (*self.list.head).tower[0].load(Ordering::Acquire) };
                self.current = SkipNode::unmark(first);
                self.started = true;
            }

            if self.current.is_null() {
                return None;
            }

            let node = self.current;

            // Move to next
            // SAFETY: `node` is valid (checked non-null above). Nodes are never
            // deallocated until arena Drop. The iterator's lifetime is tied to
            // the skip list's lifetime, which is tied to the arena.
            let next = unsafe { (*node).tower[0].load(Ordering::Acquire) };
            self.current = if next.is_null() {
                ptr::null()
            } else {
                SkipNode::unmark(next)
            };

            // Skip marked (deleted) nodes
            // SAFETY: `node` is valid; `is_deleted` checks the mark bit.
            if unsafe { (*node).is_deleted() } {
                continue;
            }

            // SAFETY: `node` is a valid, non-head, non-deleted node. Its key and
            // value were initialized in `alloc_node` and remain valid.
            return Some(unsafe { ((*node).key_ref(), (*node).value_ref()) });
        }
    }
}

impl<'a, K, V> Drop for ArenaSkipList<'a, K, V>
where
    K: Ord + Send + Sync,
    V: Send + Sync,
{
    fn drop(&mut self) {
        // Drop keys and values from ALL allocated nodes (via the all_nodes chain)
        // This ensures we don't leak K/V even for nodes that were:
        // - Unlinked during search (physically removed from skip list)
        // - Never fully linked (duplicate key race)
        //
        // We use Acquire ordering to ensure we see all writes to the nodes.
        // This synchronizes with the Release in alloc_node's CAS that publishes nodes.
        let mut current = self.all_nodes_head.load(Ordering::Acquire);

        while !current.is_null() {
            let node = current;
            // SAFETY: `node` is valid - it was allocated from the arena and added to
            // the all_nodes chain in `alloc_node`. We use Acquire ordering to ensure
            // we see the fully initialized `next_allocated` pointer (synchronizes with
            // the Release in the CAS that published the node).
            let next = unsafe { (*node).next_allocated.load(Ordering::Acquire) };

            // Drop key and value (non-head nodes only)
            // SAFETY: For non-head nodes, key and value were initialized in `alloc_node`
            // using `ptr::write`. We're in Drop, so we have exclusive access. We drop
            // each node exactly once by traversing the all_nodes chain.
            if !unsafe { (*node).is_head } {
                unsafe {
                    ptr::drop_in_place((*node).key.as_mut_ptr());
                    ptr::drop_in_place((*node).value.as_mut_ptr());
                }
            }

            current = next;
        }
    }
}

#[cfg(test)]
#[allow(clippy::uninlined_format_args)]
mod tests {
    use super::*;
    use std::sync::Barrier;
    use std::thread;

    #[test]
    fn test_basic_insert_get() {
        let arena = PageArena::with_defaults();
        let list: ArenaSkipList<i64, String> = ArenaSkipList::new(&arena);

        assert!(list.is_empty());

        list.insert(1, "one".to_string());
        list.insert(2, "two".to_string());
        list.insert(3, "three".to_string());

        assert_eq!(list.len(), 3);
        assert!(!list.is_empty());

        assert_eq!(list.get(&1), Some(&"one".to_string()));
        assert_eq!(list.get(&2), Some(&"two".to_string()));
        assert_eq!(list.get(&3), Some(&"three".to_string()));
        assert_eq!(list.get(&4), None);
    }

    #[test]
    fn test_insert_duplicate() {
        let arena = PageArena::with_defaults();
        let list: ArenaSkipList<i64, i64> = ArenaSkipList::new(&arena);

        assert!(list.insert(1, 100).is_none());
        assert_eq!(list.insert(1, 200), Some(&100));

        assert_eq!(list.get(&1), Some(&100));
    }

    #[test]
    fn test_contains_key() {
        let arena = PageArena::with_defaults();
        let list: ArenaSkipList<i64, i64> = ArenaSkipList::new(&arena);

        list.insert(1, 100);
        list.insert(3, 300);

        assert!(list.contains_key(&1));
        assert!(!list.contains_key(&2));
        assert!(list.contains_key(&3));
    }

    #[test]
    fn test_remove() {
        let arena = PageArena::with_defaults();
        let list: ArenaSkipList<i64, i64> = ArenaSkipList::new(&arena);

        list.insert(1, 100);
        list.insert(2, 200);
        list.insert(3, 300);

        assert!(list.remove(&2));
        assert!(!list.remove(&2));
        assert!(!list.remove(&4));

        assert_eq!(list.get(&1), Some(&100));
        assert_eq!(list.get(&2), None);
        assert_eq!(list.get(&3), Some(&300));
    }

    #[test]
    fn test_iterator() {
        let arena = PageArena::with_defaults();
        let list: ArenaSkipList<i64, i64> = ArenaSkipList::new(&arena);

        list.insert(3, 300);
        list.insert(1, 100);
        list.insert(4, 400);
        list.insert(2, 200);

        let entries: Vec<_> = list.iter().collect();

        assert_eq!(entries.len(), 4);
        assert_eq!(entries[0], (&1, &100));
        assert_eq!(entries[1], (&2, &200));
        assert_eq!(entries[2], (&3, &300));
        assert_eq!(entries[3], (&4, &400));
    }

    #[test]
    fn test_iterator_from() {
        let arena = PageArena::with_defaults();
        let list: ArenaSkipList<i64, i64> = ArenaSkipList::new(&arena);

        for i in 1..=10 {
            list.insert(i, i * 100);
        }

        let entries: Vec<_> = list.iter_from(&5).collect();

        assert_eq!(entries.len(), 6);
        assert_eq!(entries[0], (&5, &500));
        assert_eq!(entries[5], (&10, &1000));
    }

    #[test]
    fn test_iterator_skips_deleted() {
        let arena = PageArena::with_defaults();
        let list: ArenaSkipList<i64, i64> = ArenaSkipList::new(&arena);

        list.insert(1, 100);
        list.insert(2, 200);
        list.insert(3, 300);
        list.insert(4, 400);

        list.remove(&2);
        list.remove(&4);

        let entries: Vec<_> = list.iter().collect();

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0], (&1, &100));
        assert_eq!(entries[1], (&3, &300));
    }

    #[test]
    fn test_many_entries() {
        let arena = PageArena::with_defaults();
        let list: ArenaSkipList<i64, i64> = ArenaSkipList::new(&arena);

        for i in 0..10_000 {
            list.insert(i, i * 10);
        }

        assert_eq!(list.len(), 10_000);

        assert_eq!(list.get(&0), Some(&0));
        assert_eq!(list.get(&5000), Some(&50_000));
        assert_eq!(list.get(&9999), Some(&99_990));
        assert_eq!(list.get(&10000), None);
    }

    #[test]
    fn test_string_keys() {
        let arena = PageArena::with_defaults();
        let list: ArenaSkipList<String, i64> = ArenaSkipList::new(&arena);

        list.insert("apple".to_string(), 1);
        list.insert("banana".to_string(), 2);
        list.insert("cherry".to_string(), 3);

        assert_eq!(list.get("apple"), Some(&1));
        assert_eq!(list.get("banana"), Some(&2));
        assert_eq!(list.get("cherry"), Some(&3));
        assert_eq!(list.get("date"), None);

        let keys: Vec<_> = list.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, vec!["apple", "banana", "cherry"]);
    }

    // ==================== Concurrent Tests ====================
    //
    // These tests use std::thread::scope to safely share the arena across threads.
    // The scope ensures all threads complete before the arena/list are dropped.

    #[test]
    fn test_concurrent_insert() {
        let arena = PageArena::with_defaults();
        let list: ArenaSkipList<i64, i64> = ArenaSkipList::new(&arena);

        let num_threads = 8;
        let inserts_per_thread = 1000;
        let barrier = Barrier::new(num_threads);

        thread::scope(|s| {
            for tid in 0..num_threads {
                let list = &list;
                let barrier = &barrier;

                s.spawn(move || {
                    barrier.wait();

                    for i in 0..inserts_per_thread {
                        let key = (tid * inserts_per_thread + i) as i64;
                        list.insert(key, key * 10);
                    }
                });
            }
        });

        let expected_count = num_threads * inserts_per_thread;
        assert_eq!(list.len(), expected_count);

        for i in 0..expected_count {
            let key = i as i64;
            assert_eq!(list.get(&key), Some(&(key * 10)), "Missing key {}", key);
        }
    }

    #[test]
    fn test_concurrent_insert_same_keys() {
        let arena = PageArena::with_defaults();
        let list: ArenaSkipList<i64, i64> = ArenaSkipList::new(&arena);

        let num_threads = 8;
        let num_keys = 100;
        let barrier = Barrier::new(num_threads);

        thread::scope(|s| {
            for tid in 0..num_threads {
                let list = &list;
                let barrier = &barrier;

                s.spawn(move || {
                    barrier.wait();

                    for i in 0..num_keys {
                        list.insert(i as i64, (tid * 1000 + i) as i64);
                    }
                });
            }
        });

        for i in 0..num_keys {
            assert!(list.contains_key(&(i as i64)), "Missing key {}", i);
        }
    }

    #[test]
    fn test_concurrent_get() {
        let arena = PageArena::with_defaults();
        let list: ArenaSkipList<i64, i64> = ArenaSkipList::new(&arena);

        for i in 0..1000 {
            list.insert(i, i * 10);
        }

        let num_threads = 8;
        let reads_per_thread = 10_000;
        let barrier = Barrier::new(num_threads);

        let total_found: usize = thread::scope(|s| {
            let handles: Vec<_> = (0..num_threads)
                .map(|_| {
                    let list = &list;
                    let barrier = &barrier;

                    s.spawn(move || {
                        barrier.wait();

                        let mut found = 0;
                        for i in 0..reads_per_thread {
                            let key = (i % 1000) as i64;
                            if list.get(&key).is_some() {
                                found += 1;
                            }
                        }
                        found
                    })
                })
                .collect();

            handles.into_iter().map(|h| h.join().unwrap()).sum()
        });

        assert_eq!(total_found, num_threads * reads_per_thread);
    }

    #[test]
    fn test_concurrent_insert_and_get() {
        let arena = PageArena::with_defaults();
        let list: ArenaSkipList<i64, i64> = ArenaSkipList::new(&arena);

        let num_writers = 4;
        let num_readers = 4;
        let ops_per_thread = 5000;
        let barrier = Barrier::new(num_writers + num_readers);

        thread::scope(|s| {
            // Writer threads
            for tid in 0..num_writers {
                let list = &list;
                let barrier = &barrier;

                s.spawn(move || {
                    barrier.wait();

                    for i in 0..ops_per_thread {
                        let key = (tid * ops_per_thread + i) as i64;
                        list.insert(key, key);
                    }
                });
            }

            // Reader threads
            for _ in 0..num_readers {
                let list = &list;
                let barrier = &barrier;

                s.spawn(move || {
                    barrier.wait();

                    for i in 0..ops_per_thread {
                        let key = (i % (num_writers * ops_per_thread)) as i64;
                        let _ = list.get(&key);
                    }
                });
            }
        });

        let expected = num_writers * ops_per_thread;
        assert_eq!(list.len(), expected);
    }

    #[test]
    fn test_concurrent_insert_and_remove() {
        let arena = PageArena::with_defaults();
        let list: ArenaSkipList<i64, i64> = ArenaSkipList::new(&arena);

        // Initial state: 1000 even keys (0, 2, 4, ..., 1998)
        for i in 0i64..1000 {
            list.insert(i * 2, i * 2);
        }

        let num_threads = 8usize;
        let barrier = Barrier::new(num_threads);

        // 4 inserter threads (tid 0,2,4,6) each insert 500 odd keys
        // 4 deleter threads (tid 1,3,5,7) each try to remove 500 even keys
        thread::scope(|s| {
            for tid in 0..num_threads {
                let list = &list;
                let barrier = &barrier;

                s.spawn(move || {
                    barrier.wait();

                    if tid % 2 == 0 {
                        // Inserter: insert odd keys
                        for i in 0..500i64 {
                            let key = ((tid / 2) as i64 * 500 + i) * 2 + 1;
                            list.insert(key, key);
                        }
                    } else {
                        // Deleter: remove even keys
                        for i in 0..500i64 {
                            let key = ((tid / 2) as i64 * 500 + i) * 2;
                            list.remove(&key);
                        }
                    }
                });
            }
        });

        // Verify postconditions
        let keys: Vec<_> = list.iter().map(|(k, _)| *k).collect();

        // All remaining keys should be odd (even keys were removed)
        for key in &keys {
            assert!(
                key % 2 == 1,
                "Found even key {} that should have been deleted",
                key
            );
        }

        // Should have exactly 2000 odd keys
        // tid 0: 1,3,5,...,999 (500 keys)
        // tid 2: 1001,1003,...,1999 (500 keys)
        // tid 4: 2001,2003,...,2999 (500 keys)
        // tid 6: 3001,3003,...,3999 (500 keys)
        assert_eq!(
            keys.len(),
            2000,
            "Expected 2000 odd keys, got {}",
            keys.len()
        );

        // Verify keys are in sorted order
        for i in 1..keys.len() {
            assert!(keys[i - 1] < keys[i], "Keys not sorted at index {}", i);
        }
    }

    #[test]
    fn test_concurrent_iteration() {
        let arena = PageArena::with_defaults();
        let list: ArenaSkipList<i64, i64> = ArenaSkipList::new(&arena);

        for i in 0..1000 {
            list.insert(i, i);
        }

        let num_threads = 4;
        let barrier = Barrier::new(num_threads + 1);

        let total: usize = thread::scope(|s| {
            // Writer thread
            let list_ref = &list;
            let barrier_ref = &barrier;
            s.spawn(move || {
                barrier_ref.wait();
                for i in 1000..2000 {
                    list_ref.insert(i, i);
                }
            });

            // Reader threads
            let handles: Vec<_> = (0..num_threads)
                .map(|_| {
                    let list = &list;
                    let barrier = &barrier;

                    s.spawn(move || {
                        barrier.wait();

                        let mut count = 0;
                        for _ in 0..10 {
                            let entries: Vec<_> = list.iter().collect();
                            count += entries.len();
                        }
                        count
                    })
                })
                .collect();

            handles.into_iter().map(|h| h.join().unwrap()).sum()
        });

        println!("Total items iterated: {}", total);
        assert!(total > 0);
    }

    #[test]
    fn test_stress() {
        let arena = PageArena::with_defaults();
        let list: ArenaSkipList<i64, i64> = ArenaSkipList::new(&arena);

        let num_threads = 16;
        let ops_per_thread = 10_000;
        let barrier = Barrier::new(num_threads);

        thread::scope(|s| {
            for tid in 0..num_threads {
                let list = &list;
                let barrier = &barrier;

                s.spawn(move || {
                    barrier.wait();

                    for i in 0..ops_per_thread {
                        let key = ((tid * 17 + i * 31) % 10000) as i64;

                        match i % 3 {
                            0 => {
                                list.insert(key, key);
                            }
                            1 => {
                                let _ = list.get(&key);
                            }
                            _ => {
                                let _ = list.remove(&key);
                            }
                        }
                    }
                });
            }
        });

        println!("Stress test: {} entries remaining", list.len());
    }

    // ==================== Edge Case Tests ====================

    #[test]
    fn test_empty_iteration() {
        let arena = PageArena::with_defaults();
        let list: ArenaSkipList<i64, i64> = ArenaSkipList::new(&arena);

        let entries: Vec<_> = list.iter().collect();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_single_entry() {
        let arena = PageArena::with_defaults();
        let list: ArenaSkipList<i64, i64> = ArenaSkipList::new(&arena);

        list.insert(42, 420);

        assert_eq!(list.len(), 1);
        assert_eq!(list.get(&42), Some(&420));

        let entries: Vec<_> = list.iter().collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0], (&42, &420));
    }

    #[test]
    fn test_remove_all() {
        let arena = PageArena::with_defaults();
        let list: ArenaSkipList<i64, i64> = ArenaSkipList::new(&arena);

        for i in 0..100 {
            list.insert(i, i);
        }

        for i in 0..100 {
            assert!(list.remove(&i));
        }

        assert_eq!(list.len(), 0);
        assert!(list.is_empty());

        let entries: Vec<_> = list.iter().collect();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_reverse_order_insert() {
        let arena = PageArena::with_defaults();
        let list: ArenaSkipList<i64, i64> = ArenaSkipList::new(&arena);

        for i in (0..100).rev() {
            list.insert(i, i * 10);
        }

        let keys: Vec<_> = list.iter().map(|(k, _)| *k).collect();
        let expected: Vec<i64> = (0..100).collect();
        assert_eq!(keys, expected);
    }

    // ==================== Additional Stress Tests ====================

    #[test]
    fn test_rapid_insert_delete_cycles() {
        // Stress test: rapidly insert and delete the same keys
        let arena = PageArena::with_defaults();
        let list: ArenaSkipList<i64, i64> = ArenaSkipList::new(&arena);

        let num_threads = 8;
        let cycles = 1000;
        let barrier = Barrier::new(num_threads);

        thread::scope(|s| {
            for tid in 0..num_threads {
                let list = &list;
                let barrier = &barrier;

                s.spawn(move || {
                    barrier.wait();

                    for cycle in 0..cycles {
                        let key = tid as i64;
                        let value = (cycle * 1000 + tid) as i64;

                        // Insert
                        let _ = list.insert(key, value);

                        // Read back (may or may not find it)
                        let _ = list.get(&key);

                        // Delete
                        let _ = list.remove(&key);

                        // Verify deletion
                        assert!(
                            !list.contains_key(&key),
                            "Key {} still exists after delete",
                            key
                        );
                    }
                });
            }
        });

        // All keys should be deleted
        assert!(
            list.is_empty(),
            "List should be empty but has {} entries",
            list.len()
        );
    }

    #[test]
    fn test_concurrent_get_during_delete() {
        // Stress test: concurrent gets while other threads are deleting
        let arena = PageArena::with_defaults();
        let list: ArenaSkipList<i64, i64> = ArenaSkipList::new(&arena);

        // Pre-populate with many keys
        for i in 0..5000 {
            list.insert(i, i * 10);
        }

        let num_deleters = 4;
        let num_readers = 4;
        let barrier = Barrier::new(num_deleters + num_readers);

        thread::scope(|s| {
            // Deleter threads
            for tid in 0..num_deleters {
                let list = &list;
                let barrier = &barrier;

                s.spawn(move || {
                    barrier.wait();

                    // Each deleter handles a range
                    let start = (tid * 1250) as i64;
                    let end = start + 1250;

                    for key in start..end {
                        let _ = list.remove(&key);
                    }
                });
            }

            // Reader threads
            for _ in 0..num_readers {
                let list = &list;
                let barrier = &barrier;

                s.spawn(move || {
                    barrier.wait();

                    // Read all keys multiple times
                    for _ in 0..10 {
                        for key in 0..5000i64 {
                            let _ = list.get(&key);
                        }
                    }
                });
            }
        });

        // All keys should be deleted
        assert!(
            list.is_empty(),
            "List should be empty but has {} entries",
            list.len()
        );
    }

    #[test]
    fn test_mixed_operations_stress() {
        // Comprehensive stress test with all operations mixed
        let arena = PageArena::with_defaults();
        let list: ArenaSkipList<i64, String> = ArenaSkipList::new(&arena);

        let num_threads = 16;
        let ops_per_thread = 5000;
        let barrier = Barrier::new(num_threads);

        thread::scope(|s| {
            for tid in 0..num_threads {
                let list = &list;
                let barrier = &barrier;

                s.spawn(move || {
                    barrier.wait();

                    let mut local_keys: Vec<i64> = Vec::new();

                    for i in 0..ops_per_thread {
                        let op = (tid + i) % 5;

                        match op {
                            0 | 1 => {
                                // Insert a new key
                                let key = (tid * ops_per_thread + i) as i64;
                                let value = format!("value_{}_{}", tid, i);
                                if list.insert(key, value).is_none() {
                                    local_keys.push(key);
                                }
                            }
                            2 => {
                                // Get a key (may or may not exist)
                                let key = ((tid + i) % 10000) as i64;
                                let _ = list.get(&key);
                            }
                            3 => {
                                // Delete a key from our local set
                                if !local_keys.is_empty() {
                                    let idx = i % local_keys.len();
                                    let key = local_keys.swap_remove(idx);
                                    let _ = list.remove(&key);
                                }
                            }
                            _ => {
                                // Iterate over a range
                                let start = (i % 1000) as i64;
                                let count = list.iter_from(&start).take(10).count();
                                assert!(count <= 10);
                            }
                        }
                    }
                });
            }
        });

        // Verify list is in a valid state
        let entries: Vec<_> = list.iter().collect();
        // Check keys are in sorted order
        for i in 1..entries.len() {
            assert!(
                entries[i - 1].0 < entries[i].0,
                "Keys not sorted: {} >= {}",
                entries[i - 1].0,
                entries[i].0
            );
        }
        println!(
            "Mixed stress test: {} entries remaining, all in sorted order",
            entries.len()
        );
    }

    #[test]
    fn test_height_stress() {
        // Stress test with many operations to exercise various node heights
        let arena = PageArena::with_defaults();
        let list: ArenaSkipList<i64, i64> = ArenaSkipList::new(&arena);

        // Insert many keys to generate various heights
        for i in 0..50_000 {
            list.insert(i, i);
        }

        assert_eq!(list.len(), 50_000);

        // Verify all keys are present and in order
        let mut prev: Option<i64> = None;
        let mut count = 0;
        for (k, v) in list.iter() {
            assert_eq!(*k, *v);
            if let Some(p) = prev {
                assert!(p < *k, "Keys not in order: {} >= {}", p, k);
            }
            prev = Some(*k);
            count += 1;
        }
        assert_eq!(count, 50_000);

        // Delete half the keys
        for i in (0..50_000).step_by(2) {
            assert!(list.remove(&i), "Failed to remove key {}", i);
        }

        // Verify remaining keys
        let entries: Vec<_> = list.iter().collect();
        assert_eq!(entries.len(), 25_000);

        for (i, (k, v)) in entries.iter().enumerate() {
            let expected = (i * 2 + 1) as i64;
            assert_eq!(
                **k, expected,
                "Expected key {} at index {}, got {}",
                expected, i, k
            );
            assert_eq!(**v, expected);
        }
    }

    // ==================== Memory Safety Tests ====================

    #[test]
    fn test_drop_runs_destructors() {
        use std::sync::atomic::AtomicUsize;

        // Track destructor calls
        static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);

        #[allow(dead_code)]
        struct DropTracker(i32);
        impl Drop for DropTracker {
            fn drop(&mut self) {
                DROP_COUNT.fetch_add(1, Ordering::SeqCst);
            }
        }

        DROP_COUNT.store(0, Ordering::SeqCst);

        {
            let arena = PageArena::with_defaults();
            let list: ArenaSkipList<i32, DropTracker> = ArenaSkipList::new(&arena);

            // Insert 100 items
            for i in 0..100 {
                list.insert(i, DropTracker(i));
            }

            // Remove 50 items (they should still be in all_nodes chain)
            for i in 0..50 {
                list.remove(&i);
            }

            // list and arena go out of scope here
        }

        // All 100 DropTrackers should have been dropped
        // (including the 50 that were "removed" - they're still in all_nodes chain)
        let drops = DROP_COUNT.load(Ordering::SeqCst);
        assert_eq!(drops, 100, "Expected 100 drops, got {}", drops);
    }

    #[test]
    fn test_duplicate_insert_no_leak() {
        use std::sync::atomic::AtomicUsize;

        static ALLOC_COUNT: AtomicUsize = AtomicUsize::new(0);
        static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);

        #[allow(dead_code)]
        struct LeakTracker(i32);
        impl LeakTracker {
            fn new(v: i32) -> Self {
                ALLOC_COUNT.fetch_add(1, Ordering::SeqCst);
                LeakTracker(v)
            }
        }
        impl Drop for LeakTracker {
            fn drop(&mut self) {
                DROP_COUNT.fetch_add(1, Ordering::SeqCst);
            }
        }

        ALLOC_COUNT.store(0, Ordering::SeqCst);
        DROP_COUNT.store(0, Ordering::SeqCst);

        {
            let arena = PageArena::with_defaults();
            let list: ArenaSkipList<i32, LeakTracker> = ArenaSkipList::new(&arena);

            // Insert 10 items
            for i in 0..10 {
                list.insert(i, LeakTracker::new(i));
            }

            // Try to insert duplicates 100 times
            // Note: The caller creates LeakTracker BEFORE we can check if key exists.
            // Values for duplicate keys are immediately dropped when insert() returns.
            // This tests that no values are leaked, not that fewer are created.
            for _ in 0..100 {
                for i in 0..10 {
                    list.insert(i, LeakTracker::new(i + 1000));
                }
            }
        }

        let allocs = ALLOC_COUNT.load(Ordering::SeqCst);
        let drops = DROP_COUNT.load(Ordering::SeqCst);

        // All allocated values should be dropped (no leak)
        // - 10 values in the list (dropped when list drops)
        // - 1000 duplicate values (dropped immediately when insert returns)
        assert_eq!(
            allocs, drops,
            "Memory leak: allocated {} but dropped {}",
            allocs, drops
        );
        // Verify total: 10 original + 1000 duplicates = 1010
        assert_eq!(allocs, 1010, "Expected 1010 allocations, got {}", allocs);
    }
}
