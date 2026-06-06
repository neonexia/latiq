//! latiq-engine — engine-agnostic query contract (DuckLake-format targeted).
pub mod abort;
pub mod engine;
pub mod result;
pub use abort::AbortToken;
pub use engine::{EngineError, QueryEngine};
pub use result::{ExplainResult, QueryResult, ScanOp, SchemaSummary, TableInfo};
