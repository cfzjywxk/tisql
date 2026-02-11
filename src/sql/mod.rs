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

mod binder;
mod parser;
mod plan;

// Crate-internal types - not exposed publicly
// Parser, Binder, and LogicalPlan are encapsulated within SQLEngine
pub(crate) use binder::Binder;
pub(crate) use parser::Parser;
pub(crate) use plan::{AggFunc, BinaryOp, Expr, LogicalPlan, OrderByExpr, UnaryOp};
