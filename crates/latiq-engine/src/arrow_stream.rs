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
