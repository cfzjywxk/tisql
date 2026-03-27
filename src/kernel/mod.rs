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

//! Inward-facing contracts that separate TiSQL subsystems.
//!
//! These modules define the stable seams used by the outer layers:
//! - `execution` hides row codecs and key encoding from SQL/executor callers
//! - `txn_storage` hides tablet routing and MVCC details from the transaction layer
//! - `manifest` hides tablet version internals from ilog persistence and recovery

pub(crate) mod execution;
pub(crate) mod manifest;
pub(crate) mod txn_storage;
