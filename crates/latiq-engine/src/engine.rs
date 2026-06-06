use crate::abort::AbortToken;
use crate::result::{ExplainResult, QueryResult, SchemaSummary};
use latiq_common::Identity;
use latiq_storage::PondLocation;

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("query parse error: {0}")]
    Parse(String),
    #[error("write to reserved schema _latiq is not allowed")]
    ReservedSchemaWrite,
    #[error("read_query received a non-read statement; use write_query")]
    ReadOnlyViolation,
    #[error("query was cancelled")]
    Cancelled,
    #[error("query timed out")]
    Timeout,
    #[error("engine error: {0}")]
    Engine(String),
}

/// Executes SQL against a pond's DuckLake storage. One implementation per engine
/// (DuckDB now; DataFusion later). Methods are blocking — callers run them on a
/// blocking thread. `abort` MUST interrupt execution and release engine resources
/// within a bounded window (see spec §6).
pub trait QueryEngine: Send + Sync {
    /// Initialize a freshly-created pond (create the `_latiq` views, load extensions).
    fn init_pond(&self, loc: &PondLocation) -> Result<(), EngineError>;
    /// Run a read-only query (SELECT / read-only metadata). Rejects writes.
    fn read_query(
        &self,
        loc: &PondLocation,
        sql: &str,
        abort: AbortToken,
    ) -> Result<QueryResult, EngineError>;
    /// Run a write/DDL query, transaction-wrapped with native attribution.
    fn write_query(
        &self,
        loc: &PondLocation,
        sql: &str,
        identity: &Identity,
        abort: AbortToken,
    ) -> Result<QueryResult, EngineError>;
    /// Plan a query without executing it.
    fn explain_query(&self, loc: &PondLocation, sql: &str) -> Result<ExplainResult, EngineError>;
    /// Summarize the pond's user tables (for describe_pond).
    fn describe_schema(&self, loc: &PondLocation) -> Result<SchemaSummary, EngineError>;
}
