// Copyright 2026 Neonexia
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

//! latiq-engine — engine-agnostic query contract (DuckLake-format targeted).
pub mod abort;
pub mod arrow_stream;
pub mod engine;
pub mod result;
pub mod sql;
pub use abort::AbortToken;
pub use arrow_stream::ArrowSink;
pub use engine::{EngineError, QueryEngine};
pub use result::{ExplainResult, QueryResult, ScanOp, SchemaSummary, TableInfo};
pub use sql::is_read_only;
