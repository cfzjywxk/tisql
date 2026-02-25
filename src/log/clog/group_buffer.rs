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

//! OceanBase-style clog group buffer.
//!
//! Producers reserve fixed slots in a preallocated circular buffer, serialize
//! directly into reserved memory, then mark slots FILLED. A single writer
//! thread flushes FILLED slots in order and reclaims slot+byte capacity.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;

use parking_lot::{Condvar, Mutex};
use tokio::sync::{oneshot, watch};

use crate::util::io::{AlignedBuf, DMA_ALIGNMENT};

const SLOT_FREE: u8 = 0;
const SLOT_ALLOCATED: u8 = 1;
const SLOT_FILLED: u8 = 2;
const SLOT_FLUSHING: u8 = 3;
const SLOT_CANCELLED: u8 = 4;

/// Group-buffer specific allocation errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum GroupBufferError {
    RecordTooLarge { size: usize, max: usize },
    BufferFull,
    SlotsFull,
    StreamFailed,
}

#[derive(Debug)]
struct AllocState {
    write_cursor: u64,
    alloc_slot_idx: u64,
}

#[derive(Debug)]
pub(crate) struct FlushSlot {
    pub slot_idx: u64,
    pub buf_offset: usize,
    pub buf_size: usize,
    pub skip_bytes: usize,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct FlushBatchMeta {
    pub data_size: usize,
}

#[derive(Debug)]
struct ClogSlot {
    state: AtomicU8,
    buf_offset: AtomicU64,
    buf_size: AtomicU32,
    skip_bytes: AtomicU32,
    waiter: Mutex<Option<oneshot::Sender<std::result::Result<(), String>>>>,
}

impl ClogSlot {
    fn new() -> Self {
        Self {
            state: AtomicU8::new(SLOT_FREE),
            buf_offset: AtomicU64::new(0),
            buf_size: AtomicU32::new(0),
            skip_bytes: AtomicU32::new(0),
            waiter: Mutex::new(None),
        }
    }
}

/// Preallocated circular producer buffer with fixed-size slot ring.
pub(crate) struct ClogGroupBuffer {
    _buffer: AlignedBuf,
    buffer_size: usize,
    buffer_ptr: *mut u8,

    alloc_mu: Mutex<AllocState>,
    slots: Box<[ClogSlot]>,
    max_slots: usize,

    flush_slot_idx: AtomicU64,
    reclaim_slot_idx: AtomicU64,
    reuse_cursor: AtomicU64,

    data_notify_mu: Mutex<()>,
    data_notify_cv: Condvar,
    data_epoch: AtomicU64,

    space_wait_mu: Mutex<()>,
    space_wait_cv: Condvar,
    space_epoch: AtomicU64,
    pub(crate) space_watch_tx: watch::Sender<u64>,

    shutdown: AtomicBool,
}

// SAFETY: `buffer_ptr` points to memory owned by `buffer` and never changes.
// Producer reservations are non-overlapping by construction and writer only reads
// slots after state transition to FILLED/FLUSHING, so concurrent raw-pointer
// access does not alias mutable ranges.
unsafe impl Send for ClogGroupBuffer {}
// SAFETY: shared access relies on slot-state synchronization and non-overlap
// guarantees described above.
unsafe impl Sync for ClogGroupBuffer {}

impl ClogGroupBuffer {
    pub(crate) fn new(buffer_size: usize, max_slots: usize) -> Arc<Self> {
        assert!(buffer_size > 0, "buffer_size must be > 0");
        assert!(max_slots > 0, "max_slots must be > 0");

        let mut buffer = AlignedBuf::zeroed(buffer_size, DMA_ALIGNMENT);
        let buffer_ptr = buffer.as_mut_ptr();
        let slots: Vec<ClogSlot> = (0..max_slots).map(|_| ClogSlot::new()).collect();
        let (space_watch_tx, _space_watch_rx) = watch::channel(0u64);

        Arc::new(Self {
            _buffer: buffer,
            buffer_size,
            buffer_ptr,
            alloc_mu: Mutex::new(AllocState {
                write_cursor: 0,
                alloc_slot_idx: 0,
            }),
            slots: slots.into_boxed_slice(),
            max_slots,
            flush_slot_idx: AtomicU64::new(0),
            reclaim_slot_idx: AtomicU64::new(0),
            reuse_cursor: AtomicU64::new(0),
            data_notify_mu: Mutex::new(()),
            data_notify_cv: Condvar::new(),
            data_epoch: AtomicU64::new(0),
            space_wait_mu: Mutex::new(()),
            space_wait_cv: Condvar::new(),
            space_epoch: AtomicU64::new(0),
            space_watch_tx,
            shutdown: AtomicBool::new(false),
        })
    }

    #[inline]
    pub(crate) fn max_slots(&self) -> usize {
        self.max_slots
    }

    pub(crate) fn alloc(
        self: &Arc<Self>,
        size: usize,
    ) -> std::result::Result<Reservation, GroupBufferError> {
        self.alloc_inner(size)
    }

    pub(crate) fn alloc_sync_wait(
        self: &Arc<Self>,
        size: usize,
    ) -> std::result::Result<Reservation, GroupBufferError> {
        loop {
            match self.alloc_inner(size) {
                Ok(resv) => return Ok(resv),
                Err(GroupBufferError::BufferFull | GroupBufferError::SlotsFull) => {
                    let seen = self.space_epoch.load(Ordering::Acquire);
                    if self.shutdown.load(Ordering::Acquire) {
                        return Err(GroupBufferError::StreamFailed);
                    }

                    let mut guard = self.space_wait_mu.lock();
                    while self.space_epoch.load(Ordering::Acquire) == seen
                        && !self.shutdown.load(Ordering::Acquire)
                    {
                        self.space_wait_cv.wait(&mut guard);
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }

    fn alloc_inner(
        self: &Arc<Self>,
        size: usize,
    ) -> std::result::Result<Reservation, GroupBufferError> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(GroupBufferError::StreamFailed);
        }
        if size > self.buffer_size {
            return Err(GroupBufferError::RecordTooLarge {
                size,
                max: self.buffer_size,
            });
        }
        if size > u32::MAX as usize {
            return Err(GroupBufferError::RecordTooLarge {
                size,
                max: u32::MAX as usize,
            });
        }

        let (wait_tx, wait_rx) = oneshot::channel();
        let mut alloc = self.alloc_mu.lock();

        if self.shutdown.load(Ordering::Acquire) {
            return Err(GroupBufferError::StreamFailed);
        }

        let reclaimed_slots = self.reclaim_slot_idx.load(Ordering::Acquire);
        if alloc.alloc_slot_idx.saturating_sub(reclaimed_slots) >= self.max_slots as u64 {
            return Err(GroupBufferError::SlotsFull);
        }

        let cursor = alloc.write_cursor;
        let mut skip = 0usize;
        let mut data_offset = (cursor % self.buffer_size as u64) as usize;
        if size > 0 && data_offset + size > self.buffer_size {
            skip = self.buffer_size - data_offset;
            data_offset = 0;
        }

        let effective = (skip + size) as u64;
        let reused = self.reuse_cursor.load(Ordering::Acquire);
        let used = alloc.write_cursor.saturating_sub(reused);
        if used + effective > self.buffer_size as u64 {
            return Err(GroupBufferError::BufferFull);
        }

        let slot_idx = alloc.alloc_slot_idx;
        alloc.alloc_slot_idx += 1;
        alloc.write_cursor += effective;

        let slot = self.slot(slot_idx);
        if slot
            .state
            .compare_exchange(
                SLOT_FREE,
                SLOT_ALLOCATED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            self.shutdown.store(true, Ordering::Release);
            return Err(GroupBufferError::StreamFailed);
        }

        slot.buf_offset.store(data_offset as u64, Ordering::Release);
        slot.buf_size.store(size as u32, Ordering::Release);
        slot.skip_bytes.store(skip as u32, Ordering::Release);
        *slot.waiter.lock() = Some(wait_tx);
        drop(alloc);

        Ok(Reservation {
            owner: Arc::clone(self),
            slot_idx,
            buf_offset: data_offset,
            buf_size: size,
            committed: false,
            waiter_rx: Some(wait_rx),
        })
    }

    pub(crate) fn set_shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        self.bump_data_epoch(true);
        self.bump_space_epoch();
    }

    pub(crate) fn clear_shutdown(&self) {
        self.shutdown.store(false, Ordering::Release);
        self.bump_space_epoch();
    }

    #[inline]
    pub(crate) fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }

    pub(crate) fn wait_for_data_or_shutdown(&self) {
        let mut guard = self.data_notify_mu.lock();
        loop {
            if self.shutdown.load(Ordering::Acquire) || self.head_state_ready() {
                return;
            }

            let seen = self.data_epoch.load(Ordering::Acquire);
            while self.data_epoch.load(Ordering::Acquire) == seen
                && !self.shutdown.load(Ordering::Acquire)
                && !self.head_state_ready()
            {
                self.data_notify_cv.wait(&mut guard);
            }
        }
    }

    fn head_state_ready(&self) -> bool {
        let head = self.flush_slot_idx.load(Ordering::Acquire);
        let slot = self.slot(head);
        matches!(
            slot.state.load(Ordering::Acquire),
            SLOT_FILLED | SLOT_CANCELLED
        )
    }

    pub(crate) fn drain_cancelled_head(&self, err: &str) -> bool {
        let mut drained = false;
        loop {
            let head = self.flush_slot_idx.load(Ordering::Acquire);
            let slot = self.slot(head);
            if slot.state.load(Ordering::Acquire) != SLOT_CANCELLED {
                break;
            }

            let skip = slot.skip_bytes.load(Ordering::Acquire) as u64;
            let size = slot.buf_size.load(Ordering::Acquire) as u64;

            if let Some(waiter) = slot.waiter.lock().take() {
                let _ = waiter.send(Err(err.to_string()));
            }

            slot.state.store(SLOT_FREE, Ordering::Release);

            self.flush_slot_idx.fetch_add(1, Ordering::AcqRel);
            self.reclaim_slot_idx.fetch_add(1, Ordering::AcqRel);
            self.reuse_cursor.fetch_add(skip + size, Ordering::AcqRel);
            drained = true;
        }

        if drained {
            self.bump_space_epoch();
        }
        drained
    }

    pub(crate) fn collect_filled_batch(&self, out: &mut Vec<FlushSlot>) -> Option<FlushBatchMeta> {
        out.clear();
        let mut slot_idx = self.flush_slot_idx.load(Ordering::Acquire);

        let mut data_size = 0usize;
        let mut expect_next_offset: Option<usize> = None;

        loop {
            let slot = self.slot(slot_idx);
            let state = slot.state.load(Ordering::Acquire);
            if state != SLOT_FILLED {
                break;
            }

            let buf_offset = slot.buf_offset.load(Ordering::Acquire) as usize;
            let buf_size = slot.buf_size.load(Ordering::Acquire) as usize;
            let skip_bytes = slot.skip_bytes.load(Ordering::Acquire) as usize;

            if !out.is_empty() && skip_bytes > 0 {
                break;
            }
            if let Some(expected) = expect_next_offset {
                if buf_size > 0 && buf_offset != expected {
                    break;
                }
            }

            slot.state.store(SLOT_FLUSHING, Ordering::Release);
            out.push(FlushSlot {
                slot_idx,
                buf_offset,
                buf_size,
                skip_bytes,
            });

            if buf_size > 0 {
                data_size += buf_size;
                expect_next_offset = Some(buf_offset + buf_size);
            }

            slot_idx += 1;
        }

        if out.is_empty() {
            None
        } else {
            Some(FlushBatchMeta { data_size })
        }
    }

    pub(crate) fn copy_to_staging(&self, batch: &[FlushSlot], staging: &mut AlignedBuf) -> usize {
        let mut written = 0usize;
        for slot in batch {
            if slot.buf_size == 0 {
                continue;
            }
            // SAFETY: slots in one flush batch are non-overlapping and contiguous in
            // source order. `staging` is owned by writer thread and sized by caller.
            unsafe {
                std::ptr::copy_nonoverlapping(
                    self.buffer_ptr.add(slot.buf_offset),
                    staging.as_mut_ptr().add(written),
                    slot.buf_size,
                );
            }
            written += slot.buf_size;
        }
        written
    }

    pub(crate) fn complete_batch(
        &self,
        batch: &[FlushSlot],
        result: std::result::Result<(), String>,
    ) {
        if batch.is_empty() {
            return;
        }

        let mut reclaimed_bytes = 0u64;
        for slot_desc in batch {
            let slot = self.slot(slot_desc.slot_idx);

            if let Some(waiter) = slot.waiter.lock().take() {
                let _ = waiter.send(result.clone());
            }

            slot.state.store(SLOT_FREE, Ordering::Release);
            reclaimed_bytes += (slot_desc.skip_bytes + slot_desc.buf_size) as u64;
        }

        self.flush_slot_idx
            .fetch_add(batch.len() as u64, Ordering::AcqRel);
        self.reclaim_slot_idx
            .fetch_add(batch.len() as u64, Ordering::AcqRel);
        self.reuse_cursor
            .fetch_add(reclaimed_bytes, Ordering::AcqRel);
        self.bump_space_epoch();
    }

    pub(crate) fn fail_all_pending(&self, err: &str) {
        let mut alloc = self.alloc_mu.lock();
        self.shutdown.store(true, Ordering::Release);

        for slot in self.slots.iter() {
            if let Some(waiter) = slot.waiter.lock().take() {
                let _ = waiter.send(Err(err.to_string()));
            }
            slot.state.store(SLOT_FREE, Ordering::Release);
            slot.buf_offset.store(0, Ordering::Release);
            slot.buf_size.store(0, Ordering::Release);
            slot.skip_bytes.store(0, Ordering::Release);
        }

        // Ensure new allocations start from a clean state on restart.
        self.flush_slot_idx.store(0, Ordering::Release);
        self.reclaim_slot_idx.store(0, Ordering::Release);
        self.reuse_cursor.store(0, Ordering::Release);

        alloc.write_cursor = 0;
        alloc.alloc_slot_idx = 0;
        drop(alloc);

        self.bump_data_epoch(true);
        self.bump_space_epoch();
    }

    pub(crate) fn reset_for_restart(&self) {
        let mut alloc = self.alloc_mu.lock();
        self.flush_slot_idx.store(0, Ordering::Release);
        self.reclaim_slot_idx.store(0, Ordering::Release);
        self.reuse_cursor.store(0, Ordering::Release);

        alloc.write_cursor = 0;
        alloc.alloc_slot_idx = 0;

        for slot in self.slots.iter() {
            *slot.waiter.lock() = None;
            slot.state.store(SLOT_FREE, Ordering::Release);
            slot.buf_offset.store(0, Ordering::Release);
            slot.buf_size.store(0, Ordering::Release);
            slot.skip_bytes.store(0, Ordering::Release);
        }
        drop(alloc);

        self.clear_shutdown();
        self.bump_data_epoch(true);
    }

    fn bump_data_epoch(&self, broadcast: bool) {
        // Serialize epoch publication with the waiter mutex to avoid
        // notify-before-wait lost-wakeup races in writer shutdown/sync paths.
        let _guard = self.data_notify_mu.lock();
        self.data_epoch.fetch_add(1, Ordering::AcqRel);
        if broadcast {
            self.data_notify_cv.notify_all();
        } else {
            self.data_notify_cv.notify_one();
        }
    }

    fn bump_space_epoch(&self) {
        let next = self.space_epoch.fetch_add(1, Ordering::AcqRel) + 1;
        self.space_watch_tx.send_replace(next);
        self.space_wait_cv.notify_all();
    }

    #[inline]
    fn slot(&self, slot_idx: u64) -> &ClogSlot {
        &self.slots[(slot_idx as usize) % self.max_slots]
    }
}

/// Producer reservation over a contiguous region in the circular buffer.
pub(crate) struct Reservation {
    owner: Arc<ClogGroupBuffer>,
    slot_idx: u64,
    buf_offset: usize,
    buf_size: usize,
    committed: bool,
    waiter_rx: Option<oneshot::Receiver<std::result::Result<(), String>>>,
}

impl Reservation {
    pub(crate) fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: allocation guarantees this reservation's range is unique and
        // disjoint from all other outstanding reservations.
        unsafe {
            std::slice::from_raw_parts_mut(
                self.owner.buffer_ptr.add(self.buf_offset),
                self.buf_size,
            )
        }
    }

    pub(crate) fn commit(mut self) -> oneshot::Receiver<std::result::Result<(), String>> {
        self.committed = true;
        let slot = self.owner.slot(self.slot_idx);
        if slot
            .state
            .compare_exchange(
                SLOT_ALLOCATED,
                SLOT_FILLED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            let msg = format!(
                "clog reservation commit state mismatch (slot={})",
                self.slot_idx
            );
            self.owner.fail_all_pending(&msg);
        } else {
            self.owner.bump_data_epoch(false);
        }
        self.waiter_rx.take().expect("reservation waiter missing")
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if self.committed {
            return;
        }

        let slot = self.owner.slot(self.slot_idx);
        if slot
            .state
            .compare_exchange(
                SLOT_ALLOCATED,
                SLOT_CANCELLED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            if let Some(waiter) = slot.waiter.lock().take() {
                let _ = waiter.send(Err("clog reservation cancelled".to_string()));
            }
            self.owner.bump_data_epoch(false);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_alloc_and_cancel_reclaims_capacity() {
        let gb = ClogGroupBuffer::new(64, 4);
        let r1 = gb.alloc(40).unwrap();
        let r2 = gb.alloc(24).unwrap();
        assert!(matches!(
            gb.alloc(1),
            Err(GroupBufferError::BufferFull | GroupBufferError::SlotsFull)
        ));

        drop(r1);
        assert!(gb.drain_cancelled_head("cancel"));

        // After reclaiming r1 capacity, a tiny reservation should fit again.
        let r3 = gb.alloc(1).unwrap();
        drop(r3);
        drop(r2);
    }

    #[test]
    fn test_record_too_large() {
        let gb = ClogGroupBuffer::new(1024, 8);
        let err = match gb.alloc(2048) {
            Ok(_) => panic!("expected RecordTooLarge"),
            Err(err) => err,
        };
        assert!(matches!(
            err,
            GroupBufferError::RecordTooLarge {
                size: 2048,
                max: 1024
            }
        ));
    }
}
