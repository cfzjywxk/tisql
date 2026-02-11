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

//! io_uring-backed async I/O for SST reads and writes.
//!
//! This module provides:
//! - `AlignedBuf`: heap buffer with custom alignment for O_DIRECT
//! - `DmaFile`: file descriptor opened with O_DIRECT (automatic fallback)
//! - `IoService`: dedicated thread with io_uring event loop
//! - `IoFuture`: dual-mode future (`.await` or `.wait()`) for I/O results
//!
//! ## Architecture
//!
//! ```text
//! Caller threads                 Dedicated IO thread
//! ─────────────                  ─────────────────────
//! io.read_at(fd,off,len)         io_uring event loop:
//!   → submit via crossbeam         recv IoRequest
//!   → .wait() or .await            submit to io_uring SQ
//! ← AlignedBuf                     poll io_uring CQ
//!                                   send result via oneshot
//! ```

mod aligned_buf;
mod dma_file;
mod service;

pub use aligned_buf::AlignedBuf;
pub use dma_file::DmaFile;
pub use service::{IoFuture, IoService};

/// Minimal single-future executor that blocks the current thread until completion.
///
/// Works in any context (inside or outside a tokio runtime). Uses thread parking
/// for efficient waiting — no busy-loop. The waker unparks the thread when the
/// future is ready.
///
/// Used by `LsmEngine::get_with_owner` (sync trait method) to call async SST I/O.
/// The scan path uses proper `.await`; this sync bridge is only for point lookups.
pub fn block_on_sync<F: std::future::Future>(fut: F) -> F::Output {
    use std::pin::pin;
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};

    struct ThreadWaker(std::thread::Thread);

    impl Wake for ThreadWaker {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }

    let waker = Waker::from(Arc::new(ThreadWaker(std::thread::current())));
    let mut cx = Context::from_waker(&waker);
    let mut fut = pin!(fut);

    loop {
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(val) => return val,
            Poll::Pending => std::thread::park(),
        }
    }
}
