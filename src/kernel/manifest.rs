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

//! Neutral manifest contracts shared between tablet coordination and ilog.
//!
//! The types in this module intentionally describe committed snapshot state,
//! pending intents, and exact intent/commit matching without exposing
//! `tablet::Version`, `tablet::SstMeta`, or ilog record-layout details.

use std::future::Future;

use serde::{Deserialize, Serialize};

use crate::catalog::types::{Lsn, Timestamp};
use crate::util::error::Result;

pub type ManifestOpId = Lsn;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SstDesc {
    pub id: u64,
    pub level: u32,
    pub smallest_key: Vec<u8>,
    pub largest_key: Vec<u8>,
    pub file_size: u64,
    pub entry_count: u64,
    pub block_count: u32,
    pub min_ts: Timestamp,
    pub max_ts: Timestamp,
    pub created_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestSnapshot {
    pub levels: Vec<Vec<SstDesc>>,
    pub next_sst_id: u64,
    pub version_num: u64,
    pub flushed_lsn: Lsn,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ManifestIntentRequest {
    Flush {
        sst_id: u64,
        memtable_id: u64,
    },
    Compact {
        input_sst_ids: Vec<u64>,
        output_sst_ids: Vec<u64>,
        target_level: u32,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecordedManifestIntent {
    Flush {
        op_id: ManifestOpId,
        sst_id: u64,
        memtable_id: u64,
    },
    Compact {
        op_id: ManifestOpId,
        input_sst_ids: Vec<u64>,
        output_sst_ids: Vec<u64>,
        target_level: u32,
    },
}

impl RecordedManifestIntent {
    pub fn op_id(&self) -> ManifestOpId {
        match self {
            Self::Flush { op_id, .. } | Self::Compact { op_id, .. } => *op_id,
        }
    }

    pub fn output_sst_ids(&self) -> Vec<u64> {
        match self {
            Self::Flush { sst_id, .. } => vec![*sst_id],
            Self::Compact { output_sst_ids, .. } => output_sst_ids.clone(),
        }
    }

    pub fn from_request(op_id: ManifestOpId, req: &ManifestIntentRequest) -> Self {
        match req {
            ManifestIntentRequest::Flush {
                sst_id,
                memtable_id,
            } => Self::Flush {
                op_id,
                sst_id: *sst_id,
                memtable_id: *memtable_id,
            },
            ManifestIntentRequest::Compact {
                input_sst_ids,
                output_sst_ids,
                target_level,
            } => Self::Compact {
                op_id,
                input_sst_ids: input_sst_ids.clone(),
                output_sst_ids: output_sst_ids.clone(),
                target_level: *target_level,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ManifestCommitRequest {
    Flush {
        sst: SstDesc,
        flushed_lsn: Lsn,
    },
    Compact {
        deleted_ssts: Vec<(u32, u64)>,
        new_ssts: Vec<SstDesc>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecordedManifestCommit {
    Flush {
        op_id: ManifestOpId,
        sst: SstDesc,
        flushed_lsn: Lsn,
    },
    Compact {
        op_id: ManifestOpId,
        deleted_ssts: Vec<(u32, u64)>,
        new_ssts: Vec<SstDesc>,
    },
}

impl RecordedManifestCommit {
    pub fn op_id(&self) -> ManifestOpId {
        match self {
            Self::Flush { op_id, .. } | Self::Compact { op_id, .. } => *op_id,
        }
    }

    pub fn from_request(op_id: ManifestOpId, req: ManifestCommitRequest) -> Self {
        match req {
            ManifestCommitRequest::Flush { sst, flushed_lsn } => Self::Flush {
                op_id,
                sst,
                flushed_lsn,
            },
            ManifestCommitRequest::Compact {
                deleted_ssts,
                new_ssts,
            } => Self::Compact {
                op_id,
                deleted_ssts,
                new_ssts,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrivialMoveEdit {
    pub deleted_ssts: Vec<(u32, u64)>,
    pub new_ssts: Vec<SstDesc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestCheckpoint {
    pub snapshot: ManifestSnapshot,
    pub pending_intents: Vec<RecordedManifestIntent>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PreparedManifestOp {
    pub op_id: ManifestOpId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointCapture {
    pub snapshot: ManifestSnapshot,
    pub pending_intents: Vec<RecordedManifestIntent>,
    pub checkpoint_lsn: Lsn,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveredManifest {
    pub snapshot: ManifestSnapshot,
    pub pending_intents: Vec<RecordedManifestIntent>,
    pub orphan_sst_ids: Vec<u64>,
    pub max_lsn: Lsn,
    pub last_checkpoint_seq: u64,
}

pub trait ManifestLog: Send + Sync {
    fn append_intent<'a>(
        &'a self,
        req: &'a ManifestIntentRequest,
    ) -> impl Future<Output = Result<RecordedManifestIntent>> + Send + 'a;

    fn append_commit<'a>(
        &'a self,
        commit: &'a RecordedManifestCommit,
    ) -> impl Future<Output = Result<Lsn>> + Send + 'a;

    fn append_trivial_move<'a>(
        &'a self,
        edit: &'a TrivialMoveEdit,
    ) -> impl Future<Output = Result<Lsn>> + Send + 'a;

    fn write_checkpoint<'a>(
        &'a self,
        checkpoint: &'a ManifestCheckpoint,
    ) -> impl Future<Output = Result<Lsn>> + Send + 'a;

    fn needs_checkpoint(&self) -> bool;
    fn recover(&self) -> Result<RecoveredManifest>;
}

pub trait ManifestCoordinator: Send + Sync {
    fn register_intent(
        &self,
        req: ManifestIntentRequest,
    ) -> impl Future<Output = Result<PreparedManifestOp>> + Send + '_;

    fn commit_prepared(
        &self,
        op: PreparedManifestOp,
        commit: ManifestCommitRequest,
    ) -> impl Future<Output = Result<()>> + Send + '_;

    fn apply_trivial_move(
        &self,
        edit: TrivialMoveEdit,
    ) -> impl Future<Output = Result<()>> + Send + '_;

    fn checkpoint_and_capture(&self)
        -> impl Future<Output = Result<CheckpointCapture>> + Send + '_;
}
