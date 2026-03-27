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

//! Log subsystem modules.
//!
//! `clog` is the transaction commit log (WAL), `ilog` is the LSM metadata
//! log (manifest/flush/compaction metadata) behind the neutral
//! `kernel::manifest::ManifestLog` boundary, and `lsn` provides shared
//! sequence allocation for both logs.

pub mod clog;
pub mod ilog;
pub mod lsn;
