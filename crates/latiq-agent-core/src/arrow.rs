//! Neutral streamed-read result. `read_arrow` returns the schema up front (known
//! even for empty results) plus the batches as they arrive — so a large result is
//! never fully buffered. Arrow is a *data* representation here (like the JSON we
//! already produce), not a transport: the gRPC streaming lives in the adapter.
use crate::error::AgentError;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use std::pin::Pin;
use tokio_stream::Stream;

/// A stream of record batches (or the first error encountered mid-stream).
pub type BatchStream = Pin<Box<dyn Stream<Item = Result<RecordBatch, AgentError>> + Send>>;

pub struct ArrowReadStream {
    pub schema: SchemaRef,
    pub batches: BatchStream,
}
