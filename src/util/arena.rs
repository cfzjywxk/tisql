//! Lock-free page-based arena allocator for memtable.
//!
//! Design principles:
//! - Pre-allocated page slots with lazy page allocation
//! - Bump pointer allocation within pages (lock-free with AtomicU64 + CAS)
//! - Lock-free page growth using CAS on AtomicPtr
//! - No individual deallocation - bulk free when arena dropped
//!
//! # Concurrency Design
//!
//! The key insight is that `(page_index, offset)` must be updated atomically
//! together to avoid race conditions during page transitions. We pack both
//! into a single `AtomicU64`:
//!
//! ```text
//! allocation_state: AtomicU64
//! ├── Upper 32 bits: page index
//! └── Lower 32 bits: offset within page
//! ```
//!
//! This allows atomic CAS on both values simultaneously.

use std::alloc::{self, Layout};
use std::mem;
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering};

/// Maximum number of pages in the arena.
/// With 2 MiB pages, this allows up to 16 GiB total.
const MAX_PAGES: usize = 8192;

/// Default page size: 2 MiB (huge page aligned)
pub const DEFAULT_PAGE_SIZE: usize = 2 * 1024 * 1024;

/// Minimum allocation alignment
const MIN_ALIGN: usize = 8;

/// Configuration for PageArena
#[derive(Debug, Clone)]
pub struct ArenaConfig {
    /// Size of each page in bytes (default: 2 MiB)
    pub page_size: usize,
    /// Initial number of pages to pre-allocate (default: 0, lazy allocation)
    pub initial_pages: usize,
    /// Maximum number of pages (default: MAX_PAGES)
    pub max_pages: usize,
}

impl Default for ArenaConfig {
    fn default() -> Self {
        Self {
            page_size: DEFAULT_PAGE_SIZE,
            initial_pages: 0,
            max_pages: MAX_PAGES,
        }
    }
}

impl ArenaConfig {
    /// Create config for a specific total size
    pub fn with_size(initial_size: usize, max_size: usize) -> Self {
        let page_size = DEFAULT_PAGE_SIZE;
        let initial_pages = initial_size.div_ceil(page_size);
        let max_pages = max_size.div_ceil(page_size).min(MAX_PAGES);
        Self {
            page_size,
            initial_pages,
            max_pages,
        }
    }
}

/// Packed allocation state: (page_index << 32) | offset
/// This allows atomic updates of both page index and offset together.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AllocState {
    page_idx: u32,
    offset: u32,
}

impl AllocState {
    #[inline]
    fn new(page_idx: u32, offset: u32) -> Self {
        Self { page_idx, offset }
    }

    #[inline]
    fn pack(self) -> u64 {
        ((self.page_idx as u64) << 32) | (self.offset as u64)
    }

    #[inline]
    fn unpack(val: u64) -> Self {
        Self {
            page_idx: (val >> 32) as u32,
            offset: val as u32,
        }
    }
}

/// Lock-free page-based arena allocator.
///
/// All allocations are bump-pointer style within pages. When a page is exhausted,
/// a new page is allocated using lock-free CAS. Pages are never individually freed;
/// all memory is released when the arena is dropped.
///
/// # Thread Safety
///
/// This allocator is fully lock-free and safe to use from multiple threads
/// concurrently. All operations use atomic primitives (CAS, fetch_add) without
/// any mutex or rwlock.
///
/// # Example
///
/// ```ignore
/// let arena = PageArena::new(ArenaConfig::default());
///
/// // Allocate raw bytes
/// let ptr = arena.alloc(100, 8);
///
/// // Allocate typed object
/// let obj: *mut MyStruct = arena.alloc_obj::<MyStruct>();
///
/// // Allocate with inline data (flexible array pattern)
/// let node: *mut Node = arena.alloc_with_data::<Node>(data_len);
/// ```
pub struct PageArena {
    /// Array of page pointers (null = not yet allocated)
    pages: [AtomicPtr<u8>; MAX_PAGES],

    /// Number of pages currently allocated (for statistics and iteration)
    page_count: AtomicUsize,

    /// Packed allocation state: (page_index << 32) | offset
    /// Updated atomically to avoid race conditions during page transitions.
    alloc_state: AtomicU64,

    /// Total bytes allocated (for statistics)
    allocated_bytes: AtomicU64,

    /// Configuration
    config: ArenaConfig,
}

// Safety: PageArena can be sent between threads and shared across threads because:
// 1. All page allocations use AtomicPtr with proper CAS operations
// 2. All state updates (alloc_state, page_count, allocated_bytes) are atomic
// 3. Once allocated, pages are never deallocated until Drop (which requires &mut self)
// 4. The pages array contains AtomicPtr which are themselves Send + Sync
//
// Note: These manual impls are technically redundant since all fields are Send + Sync,
// but they're kept explicit to document the thread-safety guarantees.
unsafe impl Send for PageArena {}
unsafe impl Sync for PageArena {}

impl PageArena {
    /// Create a new arena with the given configuration.
    pub fn new(config: ArenaConfig) -> Self {
        assert!(config.page_size > 0, "page_size must be positive");
        assert!(
            config.page_size.is_power_of_two(),
            "page_size must be power of two"
        );
        assert!(config.max_pages <= MAX_PAGES, "max_pages exceeds MAX_PAGES");
        assert!(
            config.page_size <= u32::MAX as usize,
            "page_size must fit in u32"
        );

        // Initialize all page pointers to null using safe array initialization
        let pages: [AtomicPtr<u8>; MAX_PAGES] =
            std::array::from_fn(|_| AtomicPtr::new(ptr::null_mut()));

        let arena = Self {
            pages,
            page_count: AtomicUsize::new(0),
            alloc_state: AtomicU64::new(AllocState::new(0, 0).pack()),
            allocated_bytes: AtomicU64::new(0),
            config,
        };

        // Pre-allocate initial pages if requested
        if arena.config.initial_pages > 0 {
            for i in 0..arena.config.initial_pages {
                if arena.try_allocate_page(i).is_none() {
                    panic!("Failed to allocate initial pages");
                }
            }
        }

        arena
    }

    /// Create a new arena with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(ArenaConfig::default())
    }

    /// Allocate `size` bytes with the given alignment.
    ///
    /// Returns a raw pointer to the allocated memory, or None if the arena
    /// is full (reached max_pages).
    ///
    /// # Safety
    ///
    /// The returned pointer is valid for the lifetime of the arena. The caller
    /// must ensure proper initialization before use.
    ///
    /// # Panics
    ///
    /// Panics if `size` is 0 or if `align` is not a power of two.
    pub fn alloc(&self, size: usize, align: usize) -> Option<NonNull<u8>> {
        assert!(size > 0, "allocation size must be positive");
        assert!(align.is_power_of_two(), "alignment must be power of two");

        let align = align.max(MIN_ALIGN);

        // Check for overflow in size + align - 1 (used for alignment calculation)
        let max_padding = align.checked_sub(1).expect("align is at least 1");
        let size_with_padding = size.checked_add(max_padding).unwrap_or(usize::MAX);

        // Check if allocation can fit in a page at all
        if size_with_padding > self.config.page_size {
            panic!(
                "Allocation size {} + align {} exceeds page size {}",
                size, align, self.config.page_size
            );
        }

        loop {
            // Load current allocation state atomically
            let state_val = self.alloc_state.load(Ordering::Acquire);
            let state = AllocState::unpack(state_val);
            let page_idx = state.page_idx as usize;
            let offset = state.offset as usize;

            // Ensure page is allocated
            let page_ptr = self.ensure_page_allocated(page_idx)?;

            // Calculate aligned offset and new offset (with overflow protection)
            let aligned_offset = align_up(offset, align);
            let new_offset = match aligned_offset.checked_add(size) {
                Some(n) => n,
                None => {
                    // Overflow - definitely won't fit, move to next page
                    let next_page_idx = page_idx + 1;
                    if next_page_idx >= self.config.max_pages {
                        return None;
                    }
                    self.try_allocate_page(next_page_idx)?;
                    let new_state = AllocState::new(next_page_idx as u32, 0);
                    let _ = self.alloc_state.compare_exchange_weak(
                        state_val,
                        new_state.pack(),
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    );
                    continue;
                }
            };

            if new_offset <= self.config.page_size {
                // Fits in current page, try CAS
                let new_state = AllocState::new(page_idx as u32, new_offset as u32);

                match self.alloc_state.compare_exchange_weak(
                    state_val,
                    new_state.pack(),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        // Success! Update statistics and return pointer
                        self.allocated_bytes
                            .fetch_add(size as u64, Ordering::Relaxed);
                        // SAFETY: `aligned_offset` is within `page_size` bounds (checked above:
                        // new_offset <= page_size), and `page_ptr` points to a valid allocation
                        // of exactly `page_size` bytes. The CAS success guarantees exclusive
                        // ownership of the byte range [aligned_offset, aligned_offset + size).
                        let ptr = unsafe { page_ptr.add(aligned_offset) };
                        return NonNull::new(ptr);
                    }
                    Err(_) => {
                        // CAS failed, another thread modified state, retry
                        continue;
                    }
                }
            } else {
                // Doesn't fit, try to move to next page
                let next_page_idx = page_idx + 1;

                if next_page_idx >= self.config.max_pages {
                    return None; // Arena is full
                }

                // Ensure next page is allocated before transitioning
                self.try_allocate_page(next_page_idx)?;

                // Try to atomically transition to next page with offset = 0
                let new_state = AllocState::new(next_page_idx as u32, 0);

                // Use CAS to transition - if it fails, someone else already did it
                let _ = self.alloc_state.compare_exchange_weak(
                    state_val,
                    new_state.pack(),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );

                // Whether CAS succeeded or failed, retry the allocation
                // If we succeeded, we'll allocate from the new page
                // If we failed, someone else advanced and we'll use their state
                continue;
            }
        }
    }

    /// Allocate memory for a typed object.
    ///
    /// Returns a raw pointer to uninitialized memory suitable for `T`.
    pub fn alloc_obj<T>(&self) -> Option<NonNull<T>> {
        let ptr = self.alloc(mem::size_of::<T>(), mem::align_of::<T>())?;
        Some(ptr.cast())
    }

    /// Allocate memory for a header struct followed by inline data bytes.
    ///
    /// This is the "flexible array member" pattern used for variable-length
    /// structures like:
    /// ```ignore
    /// struct Node {
    ///     header_field: u64,
    ///     data_len: u32,
    ///     // data bytes follow immediately after
    /// }
    /// ```
    ///
    /// # Arguments
    ///
    /// * `data_len` - Number of data bytes to allocate after the header
    ///
    /// # Returns
    ///
    /// Pointer to the header, with `data_len` bytes available immediately after.
    pub fn alloc_with_data<H>(&self, data_len: usize) -> Option<NonNull<H>> {
        let header_size = mem::size_of::<H>();
        let header_align = mem::align_of::<H>();

        // Total size: header + data (data follows header with no padding)
        // Use checked arithmetic to prevent overflow
        let total_size = header_size.checked_add(data_len)?;

        let ptr = self.alloc(total_size, header_align)?;
        Some(ptr.cast())
    }

    /// Allocate memory for a header struct followed by inline data bytes,
    /// with explicit data alignment.
    ///
    /// Use this when the data following the header needs specific alignment.
    ///
    /// # Panics
    ///
    /// Panics if `data_align` is zero or not a power of two.
    pub fn alloc_with_aligned_data<H>(
        &self,
        data_len: usize,
        data_align: usize,
    ) -> Option<NonNull<H>> {
        // Validate data_align
        assert!(data_align > 0, "data_align must be positive");
        assert!(
            data_align.is_power_of_two(),
            "data_align must be power of two"
        );

        let header_size = mem::size_of::<H>();
        let header_align = mem::align_of::<H>();

        // Ensure data starts at aligned offset
        let data_offset = align_up(header_size, data_align);

        // Use checked arithmetic to prevent overflow
        let total_size = data_offset.checked_add(data_len)?;

        // Use the stricter alignment
        let align = header_align.max(data_align);

        let ptr = self.alloc(total_size, align)?;
        Some(ptr.cast())
    }

    /// Ensure a page is allocated, return its pointer.
    fn ensure_page_allocated(&self, page_idx: usize) -> Option<*mut u8> {
        if page_idx >= self.config.max_pages {
            return None;
        }

        let page_ptr = self.pages[page_idx].load(Ordering::Acquire);
        if !page_ptr.is_null() {
            return Some(page_ptr);
        }

        // Page not allocated, try to allocate it
        self.try_allocate_page(page_idx)
    }

    /// Try to allocate a specific page if not already allocated.
    ///
    /// Returns Some(page_ptr) on success, None if arena is full.
    fn try_allocate_page(&self, page_idx: usize) -> Option<*mut u8> {
        if page_idx >= self.config.max_pages {
            return None;
        }

        // Check if already allocated
        let current = self.pages[page_idx].load(Ordering::Acquire);
        if !current.is_null() {
            return Some(current);
        }

        // Allocate new page
        let page_ptr = self.allocate_page_memory()?;

        // Try to install it with CAS
        match self.pages[page_idx].compare_exchange(
            ptr::null_mut(),
            page_ptr,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                // We installed the page, update page_count
                self.page_count.fetch_max(page_idx + 1, Ordering::AcqRel);
                Some(page_ptr)
            }
            Err(existing) => {
                // Another thread installed a page first, free ours and use theirs
                self.deallocate_page_memory(page_ptr);
                Some(existing)
            }
        }
    }

    /// Allocate raw memory for a page.
    fn allocate_page_memory(&self) -> Option<*mut u8> {
        let layout = Layout::from_size_align(self.config.page_size, self.config.page_size)
            .expect("Invalid page layout");

        // SAFETY: The layout is valid (page_size is checked to be a power of two and > 0
        // in `new()`). The layout's size and alignment are both `page_size`, which is
        // validated to be a valid power-of-two value.
        let ptr = unsafe { alloc::alloc(layout) };
        if ptr.is_null() {
            None
        } else {
            Some(ptr)
        }
    }

    /// Deallocate a page's memory.
    fn deallocate_page_memory(&self, ptr: *mut u8) {
        let layout = Layout::from_size_align(self.config.page_size, self.config.page_size)
            .expect("Invalid page layout");
        // SAFETY: `ptr` was allocated by `allocate_page_memory` with the same layout
        // (same page_size for both size and alignment). This is only called from Drop
        // or when a CAS race causes us to discard a duplicate page allocation.
        unsafe { alloc::dealloc(ptr, layout) };
    }

    /// Get total bytes allocated from this arena.
    pub fn allocated_bytes(&self) -> u64 {
        self.allocated_bytes.load(Ordering::Relaxed)
    }

    /// Get number of pages currently allocated.
    pub fn page_count(&self) -> usize {
        self.page_count.load(Ordering::Relaxed)
    }

    /// Get the page size.
    pub fn page_size(&self) -> usize {
        self.config.page_size
    }

    /// Get maximum number of pages.
    pub fn max_pages(&self) -> usize {
        self.config.max_pages
    }

    /// Check if the arena is full (cannot allocate more pages).
    pub fn is_full(&self) -> bool {
        let state = AllocState::unpack(self.alloc_state.load(Ordering::Relaxed));
        state.page_idx as usize >= self.config.max_pages - 1
            && state.offset as usize >= self.config.page_size
    }

    /// Get total capacity in bytes.
    pub fn capacity(&self) -> usize {
        self.config.max_pages * self.config.page_size
    }

    /// Get currently usable capacity (allocated pages).
    pub fn allocated_capacity(&self) -> usize {
        self.page_count.load(Ordering::Relaxed) * self.config.page_size
    }
}

impl Drop for PageArena {
    fn drop(&mut self) {
        // Deallocate all pages
        let page_count = *self.page_count.get_mut();
        for i in 0..page_count {
            let ptr = *self.pages[i].get_mut();
            if !ptr.is_null() {
                self.deallocate_page_memory(ptr);
            }
        }
    }
}

/// Align `value` up to the next multiple of `align`.
///
/// # Safety Requirements
///
/// - `align` must be a power of two (caller's responsibility)
/// - Returns `usize::MAX` on overflow (saturating behavior)
#[inline]
const fn align_up(value: usize, align: usize) -> usize {
    // Use saturating arithmetic to prevent overflow
    // align - 1 is safe because align is required to be >= 1 (power of two)
    match value.checked_add(align - 1) {
        Some(v) => v & !(align - 1),
        None => usize::MAX, // Saturate on overflow - caller will detect allocation failure
    }
}

#[cfg(test)]
#[allow(clippy::uninlined_format_args)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::{Arc, Barrier};
    use std::thread;

    #[test]
    fn test_basic_allocation() {
        let arena = PageArena::with_defaults();

        // Allocate some bytes
        let ptr1 = arena.alloc(100, 8).unwrap();
        let ptr2 = arena.alloc(200, 8).unwrap();
        let ptr3 = arena.alloc(50, 8).unwrap();

        // Pointers should be non-null and different
        assert_ne!(ptr1.as_ptr(), ptr2.as_ptr());
        assert_ne!(ptr2.as_ptr(), ptr3.as_ptr());

        // Check allocation statistics
        assert!(arena.allocated_bytes() >= 350);
        assert_eq!(arena.page_count(), 1);
    }

    #[test]
    fn test_alignment() {
        let arena = PageArena::with_defaults();

        // Allocate with different alignments
        let ptr1 = arena.alloc(1, 1).unwrap();
        let ptr2 = arena.alloc(1, 16).unwrap();
        let ptr3 = arena.alloc(1, 64).unwrap();
        let ptr4 = arena.alloc(1, 256).unwrap();

        // Check alignments (MIN_ALIGN is 8, so 1-byte align becomes 8)
        assert_eq!(ptr1.as_ptr() as usize % 8, 0);
        assert_eq!(ptr2.as_ptr() as usize % 16, 0);
        assert_eq!(ptr3.as_ptr() as usize % 64, 0);
        assert_eq!(ptr4.as_ptr() as usize % 256, 0);
    }

    #[test]
    fn test_alloc_obj() {
        #[repr(C)]
        struct TestStruct {
            a: u64,
            b: u32,
            c: u16,
        }

        let arena = PageArena::with_defaults();
        let ptr: NonNull<TestStruct> = arena.alloc_obj().unwrap();

        // Check alignment
        assert_eq!(ptr.as_ptr() as usize % mem::align_of::<TestStruct>(), 0);

        // Write and read back
        // SAFETY: `ptr` is freshly allocated with correct size/alignment for TestStruct.
        // We have exclusive access and the pointer is valid for the arena's lifetime.
        unsafe {
            ptr.as_ptr().write(TestStruct {
                a: 42,
                b: 123,
                c: 456,
            });
            let val = ptr.as_ptr().read();
            assert_eq!(val.a, 42);
            assert_eq!(val.b, 123);
            assert_eq!(val.c, 456);
        }
    }

    #[test]
    fn test_alloc_with_data() {
        #[repr(C)]
        struct Header {
            len: u32,
            flags: u32,
        }

        let arena = PageArena::with_defaults();
        let data = b"Hello, World!";
        let ptr: NonNull<Header> = arena.alloc_with_data(data.len()).unwrap();

        // SAFETY: `ptr` was allocated with `alloc_with_data(data.len())`, so the allocation
        // includes space for Header + data.len() bytes. We write the header first, then
        // copy data immediately after. The pointer arithmetic stays within bounds.
        unsafe {
            // Write header
            ptr.as_ptr().write(Header {
                len: data.len() as u32,
                flags: 0,
            });

            // Write data after header
            let data_ptr = (ptr.as_ptr() as *mut u8).add(mem::size_of::<Header>());
            std::ptr::copy_nonoverlapping(data.as_ptr(), data_ptr, data.len());

            // Read back
            let header = ptr.as_ptr().read();
            assert_eq!(header.len, data.len() as u32);

            let read_data = std::slice::from_raw_parts(data_ptr, data.len());
            assert_eq!(read_data, data);
        }
    }

    #[test]
    fn test_page_exhaustion_and_growth() {
        // Use small page size to test page transitions
        let config = ArenaConfig {
            page_size: 4096, // 4 KB pages
            initial_pages: 0,
            max_pages: 100, // Increased to allow all allocations
        };
        let arena = PageArena::new(config);

        // Allocate enough to span multiple pages
        let alloc_size = 1000;
        let mut ptrs = Vec::new();

        for _ in 0..50 {
            if let Some(ptr) = arena.alloc(alloc_size, 8) {
                ptrs.push(ptr);
            }
        }

        // Should have used multiple pages
        assert!(arena.page_count() > 1);

        // All pointers should be unique
        let unique: HashSet<_> = ptrs.iter().map(|p| p.as_ptr() as usize).collect();
        assert_eq!(unique.len(), ptrs.len());
    }

    #[test]
    fn test_arena_full() {
        let config = ArenaConfig {
            page_size: 1024,
            initial_pages: 0,
            max_pages: 2,
        };
        let arena = PageArena::new(config);

        // Fill up the arena
        let mut count = 0;
        while arena.alloc(100, 8).is_some() {
            count += 1;
            if count > 100 {
                panic!("Arena should have been full by now");
            }
        }

        assert!(count > 0, "Should have allocated at least some memory");
    }

    #[test]
    fn test_pre_allocation() {
        let config = ArenaConfig {
            page_size: 4096,
            initial_pages: 5,
            max_pages: 10,
        };
        let arena = PageArena::new(config);

        // Pages should be pre-allocated
        assert_eq!(arena.page_count(), 5);
    }

    // ==================== Concurrent Tests ====================

    #[test]
    fn test_concurrent_allocation_basic() {
        let arena = Arc::new(PageArena::with_defaults());
        let num_threads = 8;
        let allocs_per_thread = 1000;
        let barrier = Arc::new(Barrier::new(num_threads));

        let handles: Vec<_> = (0..num_threads)
            .map(|_| {
                let arena = Arc::clone(&arena);
                let barrier = Arc::clone(&barrier);

                thread::spawn(move || {
                    barrier.wait();

                    let mut ptrs = Vec::with_capacity(allocs_per_thread);
                    for i in 0..allocs_per_thread {
                        let size = 64 + (i % 128); // Varying sizes
                        let ptr = arena.alloc(size, 8).expect("allocation failed");
                        // Convert to usize for Send safety
                        ptrs.push((ptr.as_ptr() as usize, size));
                    }
                    ptrs
                })
            })
            .collect();

        // Collect all pointers (as usize, size)
        let mut all_ptrs: Vec<(usize, usize)> = Vec::new();
        for handle in handles {
            all_ptrs.extend(handle.join().unwrap());
        }

        // Verify all pointers are unique (no overlapping allocations)
        let total = all_ptrs.len();
        assert_eq!(total, num_threads * allocs_per_thread);

        // Check for overlaps by sorting and checking ranges
        all_ptrs.sort_by_key(|(ptr, _)| *ptr);

        for i in 1..all_ptrs.len() {
            let (prev_ptr, prev_size) = all_ptrs[i - 1];
            let (curr_ptr, _) = all_ptrs[i];

            let prev_end = prev_ptr + prev_size;

            assert!(
                prev_end <= curr_ptr,
                "Overlapping allocations detected: prev ends at {:#x}, curr starts at {:#x}",
                prev_end,
                curr_ptr
            );
        }
    }

    #[test]
    fn test_concurrent_allocation_stress() {
        let arena = Arc::new(PageArena::new(ArenaConfig {
            page_size: 64 * 1024, // 64 KB pages for faster page transitions
            initial_pages: 0,
            max_pages: 1000,
        }));

        let num_threads = 16;
        let allocs_per_thread = 10_000;
        let barrier = Arc::new(Barrier::new(num_threads));

        let handles: Vec<_> = (0..num_threads)
            .map(|tid| {
                let arena = Arc::clone(&arena);
                let barrier = Arc::clone(&barrier);

                thread::spawn(move || {
                    barrier.wait();

                    let mut successful = 0;
                    for i in 0..allocs_per_thread {
                        // Varying sizes and alignments
                        let size = 8 + ((tid * 17 + i * 31) % 256);
                        let align = 8 << ((tid + i) % 4); // 8, 16, 32, or 64

                        if let Some(ptr) = arena.alloc(size, align) {
                            // Verify alignment
                            assert_eq!(ptr.as_ptr() as usize % align, 0, "Misaligned allocation");

                            // Write a pattern to detect corruption
                            // SAFETY: `ptr` was just allocated with the given `size`.
                            // We only write to the first min(size, 8) bytes.
                            unsafe {
                                let pattern = ((tid as u8) << 4) | ((i & 0xF) as u8);
                                std::ptr::write_bytes(ptr.as_ptr(), pattern, size.min(8));
                            }

                            successful += 1;
                        }
                    }
                    successful
                })
            })
            .collect();

        let total_successful: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();

        println!(
            "Concurrent stress test: {} successful allocations, {} pages used, {} bytes allocated",
            total_successful,
            arena.page_count(),
            arena.allocated_bytes()
        );

        assert!(total_successful > 0);
    }

    #[test]
    fn test_concurrent_page_transitions() {
        // Test specifically for page boundary transitions under concurrency
        let page_size = 4096;
        let arena = Arc::new(PageArena::new(ArenaConfig {
            page_size,
            initial_pages: 0,
            max_pages: 200, // Enough pages for all allocations
        }));

        let num_threads = 8;
        // Allocate sizes that will frequently cause page transitions
        let alloc_size = page_size / 4;
        let allocs_per_thread = 100;
        let barrier = Arc::new(Barrier::new(num_threads));

        let handles: Vec<_> = (0..num_threads)
            .map(|_| {
                let arena = Arc::clone(&arena);
                let barrier = Arc::clone(&barrier);

                thread::spawn(move || {
                    barrier.wait();

                    let mut ptrs = Vec::new();
                    for _ in 0..allocs_per_thread {
                        if let Some(ptr) = arena.alloc(alloc_size, 8) {
                            // Convert to usize for Send safety
                            ptrs.push(ptr.as_ptr() as usize);
                        }
                    }
                    ptrs
                })
            })
            .collect();

        let all_ptrs: Vec<usize> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();

        // Verify uniqueness
        let unique: HashSet<_> = all_ptrs.iter().cloned().collect();
        assert_eq!(unique.len(), all_ptrs.len(), "Duplicate pointers detected");

        println!(
            "Page transition test: {} allocations across {} pages",
            all_ptrs.len(),
            arena.page_count()
        );
    }

    #[test]
    fn test_concurrent_mixed_sizes() {
        let arena = Arc::new(PageArena::with_defaults());
        let num_threads = 8;
        let barrier = Arc::new(Barrier::new(num_threads));

        let handles: Vec<_> = (0..num_threads)
            .map(|tid| {
                let arena = Arc::clone(&arena);
                let barrier = Arc::clone(&barrier);

                thread::spawn(move || {
                    barrier.wait();

                    let mut ptrs = Vec::new();

                    // Mix of small, medium, and large allocations
                    for i in 0..500 {
                        let size = match (tid + i) % 3 {
                            0 => 16 + (i % 48),     // Small: 16-63 bytes
                            1 => 256 + (i % 256),   // Medium: 256-511 bytes
                            _ => 1024 + (i % 1024), // Large: 1024-2047 bytes
                        };

                        if let Some(ptr) = arena.alloc(size, 8) {
                            // Write size at the beginning for verification
                            // SAFETY: `ptr` was allocated with at least `size` bytes,
                            // and size >= sizeof(usize) for all allocations in this test.
                            unsafe {
                                *(ptr.as_ptr() as *mut usize) = size;
                            }
                            ptrs.push((ptr.as_ptr() as usize, size));
                        }
                    }

                    // Verify written values
                    for (ptr_addr, expected_size) in &ptrs {
                        // SAFETY: We just wrote to these addresses above. The arena
                        // is still alive, so the pointers remain valid.
                        unsafe {
                            let actual_size = *(*ptr_addr as *const usize);
                            assert_eq!(actual_size, *expected_size, "Memory corruption detected");
                        }
                    }

                    ptrs.len()
                })
            })
            .collect();

        let total: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
        println!("Mixed sizes test: {} total allocations", total);
    }

    #[test]
    fn test_concurrent_write_and_verify() {
        // Each thread allocates memory, writes unique data, then all threads verify
        let arena = Arc::new(PageArena::with_defaults());
        let num_threads = 8;
        let allocs_per_thread = 500;
        let barrier = Arc::new(Barrier::new(num_threads));

        // Shared storage for pointers (as usize) and expected values
        let results: Arc<std::sync::Mutex<Vec<(usize, u64)>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));

        let handles: Vec<_> = (0..num_threads)
            .map(|tid| {
                let arena = Arc::clone(&arena);
                let barrier = Arc::clone(&barrier);
                let results = Arc::clone(&results);

                thread::spawn(move || {
                    barrier.wait();

                    let mut local_results = Vec::new();

                    for i in 0..allocs_per_thread {
                        let ptr = arena.alloc(8, 8).expect("allocation failed");
                        let value = ((tid as u64) << 32) | (i as u64);

                        // SAFETY: `ptr` was allocated with 8 bytes, sufficient for u64.
                        unsafe {
                            *(ptr.as_ptr() as *mut u64) = value;
                        }

                        // Convert to usize for Send safety
                        local_results.push((ptr.as_ptr() as usize, value));
                    }

                    // Add to shared results
                    results.lock().unwrap().extend(local_results);
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        // Verify all values
        let final_results = results.lock().unwrap();
        assert_eq!(final_results.len(), num_threads * allocs_per_thread);

        for (ptr_addr, expected) in final_results.iter() {
            // SAFETY: These addresses were written above and the arena is still alive.
            unsafe {
                let actual = *(*ptr_addr as *const u64);
                assert_eq!(
                    actual, *expected,
                    "Data corruption: expected {:#x}, got {:#x}",
                    expected, actual
                );
            }
        }
    }

    #[test]
    fn test_arena_statistics_concurrent() {
        let arena = Arc::new(PageArena::with_defaults());
        let num_threads = 8;
        let alloc_size = 100;
        let allocs_per_thread = 1000;
        let barrier = Arc::new(Barrier::new(num_threads));

        let handles: Vec<_> = (0..num_threads)
            .map(|_| {
                let arena = Arc::clone(&arena);
                let barrier = Arc::clone(&barrier);

                thread::spawn(move || {
                    barrier.wait();
                    for _ in 0..allocs_per_thread {
                        arena.alloc(alloc_size, 8).unwrap();
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        // Statistics should be consistent
        let total_allocs = num_threads * allocs_per_thread;
        let expected_bytes = total_allocs * alloc_size;

        // allocated_bytes should be at least expected (might be more due to alignment)
        assert!(
            arena.allocated_bytes() >= expected_bytes as u64,
            "allocated_bytes {} < expected {}",
            arena.allocated_bytes(),
            expected_bytes
        );
    }

    #[test]
    fn test_alignment_concurrent() {
        let arena = Arc::new(PageArena::with_defaults());
        let num_threads = 8;
        let barrier = Arc::new(Barrier::new(num_threads));

        let handles: Vec<_> = (0..num_threads)
            .map(|tid| {
                let arena = Arc::clone(&arena);
                let barrier = Arc::clone(&barrier);

                thread::spawn(move || {
                    barrier.wait();

                    for i in 0..1000 {
                        // Test various alignments
                        let align = 8 << (((tid * 7) + i) % 6); // 8, 16, 32, 64, 128, 256
                        let size = 1 + (i % 100);

                        let ptr = arena.alloc(size, align).expect("allocation failed");
                        assert_eq!(
                            ptr.as_ptr() as usize % align,
                            0,
                            "Thread {} iter {}: alignment {} not satisfied for ptr {:p}",
                            tid,
                            i,
                            align,
                            ptr.as_ptr()
                        );
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_alloc_obj_concurrent() {
        #[repr(C, align(64))]
        struct CacheAligned {
            value: AtomicU64,
            _pad: [u8; 56],
        }

        let arena = Arc::new(PageArena::with_defaults());
        let num_threads = 8;
        let allocs_per_thread = 500;
        let barrier = Arc::new(Barrier::new(num_threads));

        let handles: Vec<_> = (0..num_threads)
            .map(|tid| {
                let arena = Arc::clone(&arena);
                let barrier = Arc::clone(&barrier);

                thread::spawn(move || {
                    barrier.wait();

                    let mut ptrs = Vec::new();
                    for i in 0..allocs_per_thread {
                        let ptr: NonNull<CacheAligned> =
                            arena.alloc_obj().expect("allocation failed");

                        // Verify 64-byte alignment
                        assert_eq!(ptr.as_ptr() as usize % 64, 0);

                        // Initialize
                        // SAFETY: `ptr` was allocated with correct size/alignment for CacheAligned.
                        unsafe {
                            (*ptr.as_ptr())
                                .value
                                .store(((tid as u64) << 32) | (i as u64), Ordering::Relaxed);
                        }

                        ptrs.push(ptr.as_ptr() as usize);
                    }

                    // Verify values
                    for (i, ptr_addr) in ptrs.iter().enumerate() {
                        // SAFETY: These addresses point to valid CacheAligned structs we wrote above.
                        unsafe {
                            let expected = ((tid as u64) << 32) | (i as u64);
                            let actual = (*(*ptr_addr as *const CacheAligned))
                                .value
                                .load(Ordering::Relaxed);
                            assert_eq!(actual, expected);
                        }
                    }

                    ptrs.len()
                })
            })
            .collect();

        let total: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
        assert_eq!(total, num_threads * allocs_per_thread);
    }

    // ==================== Edge Case Tests ====================

    #[test]
    #[should_panic(expected = "allocation size must be positive")]
    fn test_zero_size_allocation() {
        let arena = PageArena::with_defaults();
        arena.alloc(0, 8);
    }

    #[test]
    #[should_panic(expected = "alignment must be power of two")]
    fn test_invalid_alignment() {
        let arena = PageArena::with_defaults();
        arena.alloc(100, 7); // 7 is not a power of two
    }

    #[test]
    #[should_panic(expected = "page_size must be power of two")]
    fn test_invalid_page_size() {
        let config = ArenaConfig {
            page_size: 1000, // Not a power of two
            initial_pages: 0,
            max_pages: 10,
        };
        PageArena::new(config);
    }

    #[test]
    fn test_exactly_page_size_allocation() {
        let page_size = 4096;
        let arena = PageArena::new(ArenaConfig {
            page_size,
            initial_pages: 0,
            max_pages: 10,
        });

        // Allocate exactly one page worth (minus alignment overhead)
        let ptr = arena.alloc(page_size - MIN_ALIGN, MIN_ALIGN);
        assert!(ptr.is_some());

        // Next allocation should go to a new page
        let ptr2 = arena.alloc(100, 8);
        assert!(ptr2.is_some());

        assert!(arena.page_count() >= 2);
    }

    #[test]
    fn test_many_small_allocations() {
        let arena = PageArena::with_defaults();

        // Many tiny allocations
        for _ in 0..100_000 {
            arena.alloc(8, 8).unwrap();
        }

        println!(
            "Many small allocations: {} bytes in {} pages",
            arena.allocated_bytes(),
            arena.page_count()
        );
    }

    #[test]
    fn test_drop_safety() {
        // Test that dropping arena doesn't cause issues
        for _ in 0..10 {
            let arena = PageArena::new(ArenaConfig {
                page_size: 4096,
                initial_pages: 5,
                max_pages: 20,
            });

            for _ in 0..1000 {
                arena.alloc(64, 8);
            }

            // Arena dropped here
        }
    }

    #[test]
    fn test_state_packing() {
        // Test that state packing/unpacking works correctly
        let state = AllocState::new(12345, 67890);
        let packed = state.pack();
        let unpacked = AllocState::unpack(packed);
        assert_eq!(unpacked.page_idx, 12345);
        assert_eq!(unpacked.offset, 67890);

        // Test edge cases
        let max_state = AllocState::new(u32::MAX, u32::MAX);
        let unpacked = AllocState::unpack(max_state.pack());
        assert_eq!(unpacked.page_idx, u32::MAX);
        assert_eq!(unpacked.offset, u32::MAX);
    }

    #[test]
    fn test_page_transition_atomicity() {
        // Test that page transitions are atomic by having many threads
        // allocate at page boundaries simultaneously
        let page_size = 1024;
        let arena = Arc::new(PageArena::new(ArenaConfig {
            page_size,
            initial_pages: 0,
            max_pages: 500,
        }));

        let num_threads = 16;
        let allocs_per_thread = 200;
        let barrier = Arc::new(Barrier::new(num_threads));

        // Allocate sizes that are exactly 1/2 page to maximize transitions
        let alloc_size = page_size / 2;

        let handles: Vec<_> = (0..num_threads)
            .map(|_| {
                let arena = Arc::clone(&arena);
                let barrier = Arc::clone(&barrier);

                thread::spawn(move || {
                    barrier.wait();

                    let mut addrs = Vec::new();
                    for _ in 0..allocs_per_thread {
                        if let Some(ptr) = arena.alloc(alloc_size, 8) {
                            addrs.push(ptr.as_ptr() as usize);
                        }
                    }
                    addrs
                })
            })
            .collect();

        let all_addrs: Vec<usize> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();

        // All addresses should be unique
        let unique: HashSet<_> = all_addrs.iter().cloned().collect();
        assert_eq!(
            unique.len(),
            all_addrs.len(),
            "Found {} duplicates out of {} allocations",
            all_addrs.len() - unique.len(),
            all_addrs.len()
        );

        println!(
            "Page transition atomicity test: {} allocations across {} pages",
            all_addrs.len(),
            arena.page_count()
        );
    }
}
