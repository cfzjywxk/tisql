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

//! Backward-compatible LSM exports.
//!
//! `LsmEngine` implementation was mechanically extracted to `engine.rs` as the
//! first step of tablet separation. Keep this module as a compatibility facade
//! so existing imports (`crate::tablet::lsm::LsmEngine`) continue to work.
//! Re-export only the stable surface needed by existing call sites.

pub use super::engine::{LevelStats, LsmEngine, LsmStats, TieredMergeIterator};

/// Phase-1 compatibility alias. `TabletEngine` will evolve into a dedicated
/// tablet-local engine type in later phases.
pub type TabletEngine = LsmEngine;
