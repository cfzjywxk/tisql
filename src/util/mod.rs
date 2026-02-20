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

// Utility modules for TiSQL

pub mod arena;
pub mod codec;
pub mod error;
pub mod fs;
pub mod hash;
pub mod io;
pub mod log;
pub mod mysql_text;
pub mod timing;

// Re-export commonly used items
pub use arena::{ArenaConfig, PageArena, DEFAULT_PAGE_SIZE};
pub use fs::{create_dir_durable, rename_durable, sync_dir, sync_file_and_dir};
pub use hash::stable_hash64;
pub use log::{init_logger, init_logger_from_env, is_level_enabled, LogLevel};
pub use timing::{Timer, TimerGuard};
