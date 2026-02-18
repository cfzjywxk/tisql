// Copyright 2024 TiSQL Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Aligned heap buffer for O_DIRECT I/O.
//!
//! O_DIRECT requires both the buffer address and its size to be aligned
//! to the filesystem's logical block size (typically 512 or 4096 bytes).
//! `AlignedBuf` provides a heap-allocated buffer with custom alignment.

use std::alloc::{self, Layout};
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;

/// Default alignment for O_DIRECT (4096 bytes = filesystem block size).
pub const DMA_ALIGNMENT: usize = 4096;

/// A heap-allocated buffer with custom alignment.
///
/// Uses `std::alloc` directly to control alignment. Implements `Deref<Target = [u8]>`
/// so existing code that works with `&[u8]` (e.g., `DataBlock::decode(&buf)`) works
/// unchanged.
///
/// # Safety
///
/// The buffer is properly aligned and the layout is stored for correct deallocation.
/// The `Send` and `Sync` impls are safe because `AlignedBuf` owns its memory
/// exclusively (like `Vec<u8>`).
pub struct AlignedBuf {
    ptr: NonNull<u8>,
    len: usize,
    layout: Layout,
}

// SAFETY: AlignedBuf owns its allocation exclusively, like Vec<u8>.
// No shared mutable state — safe to send across threads and share references.
unsafe impl Send for AlignedBuf {}
unsafe impl Sync for AlignedBuf {}

impl AlignedBuf {
    /// Create a new aligned buffer with uninitialized contents.
    ///
    /// # Panics
    ///
    /// Panics if allocation fails or if `alignment` is not a power of two.
    pub fn new(size: usize, alignment: usize) -> Self {
        assert!(
            alignment.is_power_of_two(),
            "alignment must be power of two"
        );
        // Ensure size is at least 1 to avoid zero-sized allocation
        let alloc_size = size.max(1);
        // SAFETY: alignment is power of two and alloc_size >= 1, so Layout::from_size_align
        // will succeed. We check the allocation result for null.
        let layout = Layout::from_size_align(alloc_size, alignment).expect("invalid layout");
        let ptr = unsafe { alloc::alloc(layout) };
        let ptr = NonNull::new(ptr).expect("allocation failed");
        Self {
            ptr,
            len: size,
            layout,
        }
    }

    /// Create a new aligned buffer initialized to zero.
    pub fn zeroed(size: usize, alignment: usize) -> Self {
        assert!(
            alignment.is_power_of_two(),
            "alignment must be power of two"
        );
        let alloc_size = size.max(1);
        let layout = Layout::from_size_align(alloc_size, alignment).expect("invalid layout");
        // SAFETY: same as new(), but uses alloc_zeroed.
        let ptr = unsafe { alloc::alloc_zeroed(layout) };
        let ptr = NonNull::new(ptr).expect("allocation failed");
        Self {
            ptr,
            len: size,
            layout,
        }
    }

    /// Create an aligned buffer from existing data.
    ///
    /// Copies `data` into a new aligned buffer.
    pub fn from_slice(data: &[u8], alignment: usize) -> Self {
        let mut buf = Self::new(data.len(), alignment);
        buf[..data.len()].copy_from_slice(data);
        buf
    }

    /// Length of the buffer in bytes.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Raw pointer to the buffer (for io_uring submission).
    #[inline]
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    /// Mutable raw pointer to the buffer (for io_uring submission).
    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    /// Return the alignment of this buffer.
    #[inline]
    pub fn alignment(&self) -> usize {
        self.layout.align()
    }
}

impl Deref for AlignedBuf {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        if self.len == 0 {
            return &[];
        }
        // SAFETY: ptr is valid for len bytes and properly aligned.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

impl DerefMut for AlignedBuf {
    #[inline]
    fn deref_mut(&mut self) -> &mut [u8] {
        if self.len == 0 {
            return &mut [];
        }
        // SAFETY: ptr is valid for len bytes, properly aligned, and we have exclusive access.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        if self.layout.size() > 0 {
            // SAFETY: ptr was allocated with this layout via alloc::alloc/alloc_zeroed.
            unsafe { alloc::dealloc(self.ptr.as_ptr(), self.layout) }
        }
    }
}

impl std::fmt::Debug for AlignedBuf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlignedBuf")
            .field("len", &self.len)
            .field("alignment", &self.layout.align())
            .finish()
    }
}

/// Round `value` up to the next multiple of `alignment`.
///
/// `alignment` must be a power of two.
#[inline]
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub fn align_up(value: u64, alignment: u64) -> u64 {
    (value + alignment - 1) & !(alignment - 1)
}

/// Round `value` down to the previous multiple of `alignment`.
///
/// `alignment` must be a power of two.
#[inline]
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub fn align_down(value: u64, alignment: u64) -> u64 {
    value & !(alignment - 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aligned_buf_new() {
        let buf = AlignedBuf::new(4096, DMA_ALIGNMENT);
        assert_eq!(buf.len(), 4096);
        assert!(!buf.is_empty());
        assert_eq!(buf.alignment(), DMA_ALIGNMENT);
        // Check alignment of pointer
        assert_eq!(buf.as_ptr() as usize % DMA_ALIGNMENT, 0);
    }

    #[test]
    fn test_aligned_buf_zeroed() {
        let buf = AlignedBuf::zeroed(1024, DMA_ALIGNMENT);
        assert_eq!(buf.len(), 1024);
        for &b in buf.iter() {
            assert_eq!(b, 0);
        }
    }

    #[test]
    fn test_aligned_buf_from_slice() {
        let data = b"hello world";
        let buf = AlignedBuf::from_slice(data, DMA_ALIGNMENT);
        assert_eq!(&buf[..], data.as_slice());
        assert_eq!(buf.as_ptr() as usize % DMA_ALIGNMENT, 0);
    }

    #[test]
    fn test_aligned_buf_deref_mut() {
        let mut buf = AlignedBuf::zeroed(16, DMA_ALIGNMENT);
        buf[0] = 0xFF;
        buf[15] = 0xAA;
        assert_eq!(buf[0], 0xFF);
        assert_eq!(buf[15], 0xAA);
    }

    #[test]
    fn test_aligned_buf_empty() {
        let buf = AlignedBuf::new(0, DMA_ALIGNMENT);
        assert!(buf.is_empty());
        assert_eq!(buf.len(), 0);
        assert_eq!(&*buf, &[] as &[u8]);
    }

    #[test]
    fn test_align_up() {
        assert_eq!(align_up(0, 4096), 0);
        assert_eq!(align_up(1, 4096), 4096);
        assert_eq!(align_up(4096, 4096), 4096);
        assert_eq!(align_up(4097, 4096), 8192);
    }

    #[test]
    fn test_align_down() {
        assert_eq!(align_down(0, 4096), 0);
        assert_eq!(align_down(1, 4096), 0);
        assert_eq!(align_down(4096, 4096), 4096);
        assert_eq!(align_down(4097, 4096), 4096);
    }

    #[test]
    fn test_aligned_buf_various_sizes() {
        for size in [1, 7, 512, 4096, 8192, 65536] {
            let buf = AlignedBuf::zeroed(size, DMA_ALIGNMENT);
            assert_eq!(buf.len(), size);
            assert_eq!(buf.as_ptr() as usize % DMA_ALIGNMENT, 0);
        }
    }

    #[test]
    fn test_aligned_buf_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<AlignedBuf>();
    }
}
