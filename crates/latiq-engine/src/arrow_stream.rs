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

//! Streaming Arrow output contract. The engine pushes the result schema once,
//! then each `RecordBatch` as it's produced, into a sink — so a large result is
//! never buffered in the engine. The sink decides what to do with batches
//! (encode to IPC for the wire, collect into JSON for an edge) and can stop early
//! by returning `Break` when its consumer is gone.
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use std::ops::ControlFlow;

pub trait ArrowSink: Send {
    /// Called exactly once, before any batch, with the result schema (available
    /// even for empty results).
    fn schema(&mut self, schema: SchemaRef) -> ControlFlow<()>;
    /// Called per batch. Return `Break` to stop the stream early (consumer gone).
    fn batch(&mut self, batch: RecordBatch) -> ControlFlow<()>;
}
