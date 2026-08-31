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

//! Neutral streamed-read result. `read_arrow` returns the schema up front (known
//! even for empty results) plus the batches as they arrive — so a large result is
//! never fully buffered. Arrow is a *data* representation here (like the JSON we
//! already produce), not a transport: the gRPC streaming lives in the adapter.
use crate::error::AgentError;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use latiq_common::QueryMeta;
use std::pin::Pin;
use tokio_stream::Stream;

/// A stream of record batches (or the first error encountered mid-stream).
pub type BatchStream = Pin<Box<dyn Stream<Item = Result<RecordBatch, AgentError>> + Send>>;

/// A read in progress: the schema now, the rows as they come. Drain `batches`
/// or drop the stream — the producer holds a read transaction (and a pool
/// connection) open for the whole stream, so a consumer that simply stops
/// reading keeps them held.
pub struct ArrowReadStream {
    pub schema: SchemaRef,
    pub batches: BatchStream,
    /// The engine's `QueryMeta` for this read, delivered once the engine is
    /// done producing batches — so a caller that drains the stream (the
    /// collecting edge) reports what the ENGINE said about the read, datasets
    /// included, instead of synthesising a meta from the rows it happened to
    /// see. `None` where no producer supplies one: a stream decoded from a
    /// peer's wire chunks carries no meta, and the peer that ran the query is
    /// the one that records its lineage anyway.
    pub meta: Option<tokio::sync::oneshot::Receiver<QueryMeta>>,
}

impl ArrowReadStream {
    /// A stream with no meta behind it — everything a producer that cannot
    /// speak for the engine should build.
    pub fn new(schema: SchemaRef, batches: BatchStream) -> Self {
        Self {
            schema,
            batches,
            meta: None,
        }
    }
}
