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

//! Commit-LSN reservation tracker used by V2.6 flush/log-GC coordination.
//!
//! This structure tracks commit-path LSNs that are allocated but not yet fully
//! finalized in memtable state. Flush/log-GC boundaries must not advance past
//! the minimum reserved LSN.

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

#[cfg(feature = "failpoints")]
use fail::{eval as eval_failpoint, fail_point};
use parking_lot::Mutex;

use crate::lsn::LsnProvider;
use crate::types::{Lsn, Timestamp};

/// One reservation owned by a transaction.
#[derive(Debug, Clone)]
pub struct CommitReservation {
    pub txn_start_ts: Timestamp,
    pub lsn: Lsn,
    /// Observability-only timestamp.
    pub created_at: Instant,
}

#[derive(Default)]
struct ReservationsInner {
    by_txn_start_ts: HashMap<Timestamp, CommitReservation>,
    by_lsn: BTreeSet<Lsn>,
}

/// Lightweight counters/gauges for reservation health.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitReservationStats {
    pub active: usize,
    pub total_allocated: u64,
    pub total_released: u64,
    pub total_forced_released: u64,
    pub total_double_release: u64,
}

/// Reservation tracker.
pub struct CommitLsnReservations {
    inner: Mutex<ReservationsInner>,
    total_allocated: AtomicU64,
    total_released: AtomicU64,
    total_forced_released: AtomicU64,
    total_double_release: AtomicU64,
}

impl Default for CommitLsnReservations {
    fn default() -> Self {
        Self::new()
    }
}

impl CommitLsnReservations {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(ReservationsInner::default()),
            total_allocated: AtomicU64::new(0),
            total_released: AtomicU64::new(0),
            total_forced_released: AtomicU64::new(0),
            total_double_release: AtomicU64::new(0),
        }
    }

    /// Allocate and reserve in a single critical section.
    pub fn alloc_and_reserve(
        &self,
        txn_start_ts: Timestamp,
        lsn_provider: &dyn LsnProvider,
    ) -> Lsn {
        self.alloc_and_reserve_with(txn_start_ts, || lsn_provider.alloc_lsn())
    }

    /// Test/helper variant that provides a custom allocator.
    pub fn alloc_and_reserve_with<F>(&self, txn_start_ts: Timestamp, alloc_lsn: F) -> Lsn
    where
        F: FnOnce() -> Lsn,
    {
        let mut inner = self.inner.lock();
        let lsn = alloc_lsn();

        if let Some(prev) = inner.by_txn_start_ts.remove(&txn_start_ts) {
            let removed = inner.by_lsn.remove(&prev.lsn);
            debug_assert!(
                removed,
                "txn/lsn index mismatch for replaced txn_start_ts {txn_start_ts}"
            );
            tracing::warn!(
                txn_start_ts,
                prev_lsn = prev.lsn,
                new_lsn = lsn,
                "replacing existing commit reservation for txn"
            );
        }

        let inserted = inner.by_lsn.insert(lsn);
        assert!(
            inserted,
            "duplicate reserved LSN detected: {lsn}; refusing to continue"
        );

        inner.by_txn_start_ts.insert(
            txn_start_ts,
            CommitReservation {
                txn_start_ts,
                lsn,
                created_at: Instant::now(),
            },
        );

        self.total_allocated.fetch_add(1, Ordering::Relaxed);
        lsn
    }

    /// Test helper to reserve a fixed LSN without allocation.
    #[cfg(test)]
    pub fn reserve_with_lsn_for_test(&self, txn_start_ts: Timestamp, lsn: Lsn) {
        let mut inner = self.inner.lock();
        let inserted = inner.by_lsn.insert(lsn);
        assert!(inserted, "duplicate reserved LSN in test helper: {lsn}");
        inner.by_txn_start_ts.insert(
            txn_start_ts,
            CommitReservation {
                txn_start_ts,
                lsn,
                created_at: Instant::now(),
            },
        );
        self.total_allocated.fetch_add(1, Ordering::Relaxed);
    }

    /// Release a reservation from normal commit/rollback path.
    pub fn release(&self, txn_start_ts: Timestamp) -> Option<Lsn> {
        #[cfg(feature = "failpoints")]
        fail_point!("reservation_release_drop_v26", |_| {
            return None;
        });

        self.remove_internal(txn_start_ts, false)
    }

    /// Conditionally force-release reservation in sweep path.
    pub fn force_release_if_stale(&self, txn_start_ts: Timestamp, is_stale: bool) -> Option<Lsn> {
        #[cfg(feature = "failpoints")]
        let should_force = {
            let mut should_force = is_stale;
            if eval_failpoint("reservation_sweep_force_release_v26", |_| ()).is_some() {
                should_force = true;
            }
            should_force
        };
        #[cfg(not(feature = "failpoints"))]
        let should_force = is_stale;

        if !should_force {
            return None;
        }

        self.remove_internal(txn_start_ts, true)
    }

    fn remove_internal(&self, txn_start_ts: Timestamp, forced: bool) -> Option<Lsn> {
        let mut inner = self.inner.lock();
        let Some(reservation) = inner.by_txn_start_ts.remove(&txn_start_ts) else {
            self.total_double_release.fetch_add(1, Ordering::Relaxed);
            #[cfg(feature = "failpoints")]
            let _ = eval_failpoint("reservation_double_release_v26", |_| ());
            return None;
        };

        let removed = inner.by_lsn.remove(&reservation.lsn);
        assert!(
            removed,
            "reservation lsn index missing for txn_start_ts {}, lsn {}",
            reservation.txn_start_ts, reservation.lsn
        );

        if forced {
            self.total_forced_released.fetch_add(1, Ordering::Relaxed);
        }
        self.total_released.fetch_add(1, Ordering::Relaxed);

        Some(reservation.lsn)
    }

    pub fn min_reserved_lsn(&self) -> Option<Lsn> {
        self.inner.lock().by_lsn.iter().next().copied()
    }

    pub fn is_reserved(&self, txn_start_ts: Timestamp, lsn: Lsn) -> bool {
        self.inner
            .lock()
            .by_txn_start_ts
            .get(&txn_start_ts)
            .is_some_and(|res| res.lsn == lsn)
    }

    pub fn len(&self) -> usize {
        self.inner.lock().by_txn_start_ts.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn reserved_txn_start_ts(&self) -> Vec<Timestamp> {
        self.inner.lock().by_txn_start_ts.keys().copied().collect()
    }

    pub fn stats(&self) -> CommitReservationStats {
        CommitReservationStats {
            active: self.len(),
            total_allocated: self.total_allocated.load(Ordering::Relaxed),
            total_released: self.total_released.load(Ordering::Relaxed),
            total_forced_released: self.total_forced_released.load(Ordering::Relaxed),
            total_double_release: self.total_double_release.load(Ordering::Relaxed),
        }
    }
}

/// RAII guard for reservation lifecycle (used by commit path integration phases).
pub struct ReservationGuard<'a> {
    reservations: &'a CommitLsnReservations,
    txn_start_ts: Timestamp,
    released: bool,
}

impl<'a> ReservationGuard<'a> {
    pub fn new(reservations: &'a CommitLsnReservations, txn_start_ts: Timestamp) -> Self {
        Self {
            reservations,
            txn_start_ts,
            released: false,
        }
    }

    pub fn release(mut self) -> Option<Lsn> {
        let released = self.reservations.release(self.txn_start_ts);
        self.released = true;
        released
    }

    pub fn disarm(mut self) {
        self.released = true;
    }
}

impl Drop for ReservationGuard<'_> {
    fn drop(&mut self) {
        if !self.released {
            let _ = self.reservations.release(self.txn_start_ts);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lsn::AtomicLsnProvider;

    static TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn test_alloc_release_and_min() {
        let _guard = TEST_LOCK.lock().unwrap();
        let reservations = CommitLsnReservations::new();
        let provider = AtomicLsnProvider::with_start(10);

        let l1 = reservations.alloc_and_reserve(101, &provider);
        let l2 = reservations.alloc_and_reserve(102, &provider);

        assert_eq!(l1, 10);
        assert_eq!(l2, 11);
        assert_eq!(reservations.min_reserved_lsn(), Some(10));
        assert!(reservations.is_reserved(101, 10));

        assert_eq!(reservations.release(101), Some(10));
        assert_eq!(reservations.min_reserved_lsn(), Some(11));
        assert_eq!(reservations.release(102), Some(11));
        assert_eq!(reservations.min_reserved_lsn(), None);
    }

    #[test]
    fn test_double_release_stats() {
        let _guard = TEST_LOCK.lock().unwrap();
        let reservations = CommitLsnReservations::new();
        let provider = AtomicLsnProvider::with_start(1);

        let _ = reservations.alloc_and_reserve(1, &provider);
        assert_eq!(reservations.release(1), Some(1));
        assert_eq!(reservations.release(1), None);

        let stats = reservations.stats();
        assert_eq!(stats.total_allocated, 1);
        assert_eq!(stats.total_released, 1);
        assert_eq!(stats.total_double_release, 1);
    }

    #[test]
    fn test_force_release_if_stale() {
        let _guard = TEST_LOCK.lock().unwrap();
        let reservations = CommitLsnReservations::new();
        reservations.reserve_with_lsn_for_test(11, 50);
        reservations.reserve_with_lsn_for_test(12, 60);

        assert_eq!(reservations.force_release_if_stale(11, false), None);
        assert!(reservations.is_reserved(11, 50));

        assert_eq!(reservations.force_release_if_stale(11, true), Some(50));
        assert!(!reservations.is_reserved(11, 50));
        assert_eq!(reservations.min_reserved_lsn(), Some(60));

        let stats = reservations.stats();
        assert_eq!(stats.total_forced_released, 1);
    }

    #[test]
    fn test_reservation_guard_releases_on_drop() {
        let _guard = TEST_LOCK.lock().unwrap();
        let reservations = CommitLsnReservations::new();
        reservations.reserve_with_lsn_for_test(77, 123);

        {
            let _guard = ReservationGuard::new(&reservations, 77);
            assert!(reservations.is_reserved(77, 123));
        }

        assert!(!reservations.is_reserved(77, 123));
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn test_release_drop_failpoint_then_force_release() {
        let _guard = TEST_LOCK.lock().unwrap();
        let _scenario = fail::FailScenario::setup();
        let reservations = CommitLsnReservations::new();
        reservations.reserve_with_lsn_for_test(900, 900);

        fail::cfg("reservation_release_drop_v26", "return").unwrap();
        assert_eq!(reservations.release(900), None);
        assert!(reservations.is_reserved(900, 900));
        fail::cfg("reservation_release_drop_v26", "off").unwrap();

        assert_eq!(reservations.force_release_if_stale(900, true), Some(900));
        assert!(!reservations.is_reserved(900, 900));
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn test_double_release_failpoint() {
        let _guard = TEST_LOCK.lock().unwrap();
        let _scenario = fail::FailScenario::setup();
        let reservations = CommitLsnReservations::new();

        fail::cfg("reservation_double_release_v26", "panic").unwrap();
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = reservations.release(42);
        }));
        fail::cfg("reservation_double_release_v26", "off").unwrap();

        assert!(panic.is_err(), "double-release failpoint should panic");
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn test_sweep_force_release_failpoint() {
        let _guard = TEST_LOCK.lock().unwrap();
        let _scenario = fail::FailScenario::setup();
        let reservations = CommitLsnReservations::new();
        reservations.reserve_with_lsn_for_test(77, 700);

        fail::cfg("reservation_sweep_force_release_v26", "return").unwrap();
        assert_eq!(reservations.force_release_if_stale(77, false), Some(700));
        fail::cfg("reservation_sweep_force_release_v26", "off").unwrap();

        assert!(!reservations.is_reserved(77, 700));
    }
}
