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

use crate::transaction::IsolationLevel;

/// Session configuration
#[derive(Clone, Debug)]
pub struct SessionConfig {
    pub default_schema: String,
    pub isolation_level: IsolationLevel,
    pub autocommit: bool,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            default_schema: "default".to_string(),
            isolation_level: IsolationLevel::default(),
            autocommit: true,
        }
    }
}

/// Session state
pub struct Session {
    pub id: u64,
    pub config: SessionConfig,
}

impl Session {
    pub fn new(id: u64) -> Self {
        Self {
            id,
            config: SessionConfig::default(),
        }
    }
}
