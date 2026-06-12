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
