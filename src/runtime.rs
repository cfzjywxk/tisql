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

//! Runtime thread planning utilities.

/// Thread split across runtime roles.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RuntimeThreads {
    pub protocol: usize,
    pub worker: usize,
    pub background: usize,
    pub io: usize,
}

impl RuntimeThreads {
    /// Plan thread counts using available CPU count.
    pub fn detect() -> Self {
        let cpu = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        Self::plan(cpu)
    }

    /// Plan thread counts from explicit CPU count.
    pub fn plan(cpu: usize) -> Self {
        let cpu = cpu.max(1);
        let io = (cpu / 8).clamp(1, 2);
        let background = (cpu / 8).clamp(1, 2);
        let protocol = (cpu / 8).clamp(1, 4);
        let reserved = io + background + protocol;
        let worker = cpu.saturating_sub(reserved).max(1);
        Self {
            protocol,
            worker,
            background,
            io,
        }
    }
}

/// Optional per-role thread-count overrides.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RuntimeThreadOverrides {
    pub protocol: Option<usize>,
    pub worker: Option<usize>,
    pub background: Option<usize>,
    pub io: Option<usize>,
}

impl RuntimeThreadOverrides {
    /// Apply overrides to a planned split. Values <= 0 are normalized to 1.
    pub fn apply(self, planned: RuntimeThreads) -> RuntimeThreads {
        RuntimeThreads {
            protocol: self.protocol.unwrap_or(planned.protocol).max(1),
            worker: self.worker.unwrap_or(planned.worker).max(1),
            background: self.background.unwrap_or(planned.background).max(1),
            io: self.io.unwrap_or(planned.io).max(1),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plan_minimums() {
        let t = RuntimeThreads::plan(1);
        assert_eq!(t.protocol, 1);
        assert_eq!(t.worker, 1);
        assert_eq!(t.background, 1);
        assert_eq!(t.io, 1);
    }

    #[test]
    fn test_plan_large_cpu() {
        let t = RuntimeThreads::plan(32);
        assert_eq!(t.protocol, 4);
        assert_eq!(t.background, 2);
        assert_eq!(t.io, 2);
        assert_eq!(t.worker, 24);
    }

    #[test]
    fn test_overrides_apply() {
        let planned = RuntimeThreads::plan(8);
        let merged = RuntimeThreadOverrides {
            protocol: Some(2),
            worker: Some(6),
            background: None,
            io: Some(1),
        }
        .apply(planned);
        assert_eq!(merged.protocol, 2);
        assert_eq!(merged.worker, 6);
        assert_eq!(merged.io, 1);
        assert_eq!(merged.background, planned.background);
    }
}
