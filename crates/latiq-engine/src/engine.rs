use crate::abort::AbortToken;
use crate::arrow_stream::ArrowSink;
use crate::result::{ExplainResult, QueryResult, SchemaSummary};
use latiq_common::Identity;
use latiq_storage::PondLocation;

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("query parse error: {0}")]
    Parse(String),
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
    /// Initialize a freshly-created pond (attach its DuckLake catalog, load extensions).
    fn init_pond(&self, loc: &PondLocation) -> Result<(), EngineError>;
    /// Run a read-only query (SELECT / read-only metadata). Rejects writes.
    fn read_query(
        &self,
        loc: &PondLocation,
        sql: &str,
        abort: AbortToken,
    ) -> Result<QueryResult, EngineError>;
    /// Run a read-only query, streaming results as Arrow `RecordBatch`es into
    /// `sink` (schema first, then batches) instead of materializing them. Rejects
    /// writes, like `read_query`. `abort` must stop the stream promptly.
    fn read_arrow(
        &self,
        loc: &PondLocation,
        sql: &str,
        abort: AbortToken,
        sink: &mut dyn ArrowSink,
    ) -> Result<(), EngineError>;
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
    /// Transient pull from an external catalog: on the pond's instance, LOAD the
    /// type's extensions + create its secrets, `ATTACH` it as `alias`, run `query`
    /// (a `CREATE TABLE … AS SELECT … FROM <alias>.…`), then `DETACH` + drop the
    /// secrets — regardless of success. Nothing about the catalog persists.
    fn pull_catalog(
        &self,
        loc: &PondLocation,
        catalog_type: &str,
        alias: &str,
        params: &std::collections::BTreeMap<String, String>,
        query: &str,
    ) -> Result<(), EngineError>;
    /// Transiently attach a catalog and list its `(schema.table)` entries.
    fn describe_catalog(
        &self,
        loc: &PondLocation,
        catalog_type: &str,
        alias: &str,
        params: &std::collections::BTreeMap<String, String>,
    ) -> Result<Vec<(String, String)>, EngineError>;
    /// Number of pond instances currently open/cached (for the node's
    /// `open_ponds` gauge). Cheap; default 0 for engines that don't cache.
    fn open_pond_count(&self) -> usize {
        0
    }
    /// Evict any cached engine state for a pond (called on drop_pond). After this
    /// the engine must hold no open handles to the pond's catalog/data files, so a
    /// subsequent storage delete leaves nothing dangling. Idempotent: forgetting an
    /// unknown pond is a no-op.
    fn forget_pond(&self, loc: &PondLocation);
}
