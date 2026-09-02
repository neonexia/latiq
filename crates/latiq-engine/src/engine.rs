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

//! The `QueryEngine` port: everything Latiq asks of a SQL engine, and nothing
//! about which one. Implemented by `latiq-engine-duckdb` today; the core depends
//! on this trait so a second engine is a new crate, not a change upstream.
use crate::abort::AbortToken;
use crate::arrow_stream::ArrowSink;
use crate::result::{ExplainResult, QueryResult, SchemaSummary};
use latiq_common::Identity;
use latiq_storage::PondLocation;

/// Why a statement did not produce a result. The variants an agent can act on
/// are distinct from the catch-all `Engine`, because each maps to a different
/// `ErrorKind` upstream.
///
/// **These variants say WHAT went wrong, never which engine call failed.** That
/// distinction is the whole reason the middle four exist. Classifying by call
/// site put `INSERT INTO nope` (rejected while *preparing*) in `Parse` and
/// `CREATE TABLE t` where `t` exists (rejected while *executing*) in `Engine` →
/// `internal` → "Retry; if it persists, report to your operator." Both are the
/// same kind of mistake, both are the caller's to fix, and which one an agent
/// was told depended on nothing but DuckDB's binder phasing. An engine adapter
/// must map its own error classes onto these; it must not map its call stack.
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    /// The statement is not valid SQL — a syntax error, and nothing else. Only
    /// this variant may become `parse_error`.
    #[error("query parse error: {0}")]
    Parse(String),
    /// The statement parses, but a name in it does not resolve against the
    /// pond's catalog — or already exists there. Table, column, schema,
    /// function: the fix is to look at what the pond actually has.
    #[error("catalog error: {0}")]
    Catalog(String),
    /// A value in the statement cannot be converted to the type it is being
    /// used as (`'notanint'` into an `INTEGER` column).
    #[error("conversion error: {0}")]
    Conversion(String),
    /// A value is well-typed but violates a constraint on the target table
    /// (primary key, unique, not null, check).
    #[error("constraint error: {0}")]
    Constraint(String),
    /// A data source named in the statement could not be read or written —
    /// a URL, an object-store path, a file. Outside the pond, and usually
    /// outside this deployment.
    #[error("source I/O error: {0}")]
    SourceIo(String),
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
    /// The engine's own version, e.g. `v1.5.3` — provenance about *what ran the
    /// query*, which the lineage trail records and a caller cannot obtain any
    /// other way (the core is engine-neutral by design). Read from the engine
    /// itself, never hard-coded, or it goes stale the first time we upgrade.
    /// Cheap: implementations cache it. Never fails — an engine that cannot say
    /// returns an empty string rather than breaking a query.
    fn version(&self) -> String;
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
    ///
    /// Returns the read's `QueryMeta` once the stream is done. A streamed read
    /// has no `QueryResult` to hang it on, so without this the caller that
    /// collects the batches would have to invent one — and would report no
    /// datasets for the whole CLI/SDK read path.
    fn read_arrow(
        &self,
        loc: &PondLocation,
        sql: &str,
        abort: AbortToken,
        sink: &mut dyn ArrowSink,
    ) -> Result<latiq_common::QueryMeta, EngineError>;
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
    /// Best-effort provenance for a statement **without running it**: a
    /// `QueryMeta` carrying only what it would read and write. `None` when the
    /// pond did not opt into lineage, when the engine cannot say, or when the
    /// statement touches nothing.
    ///
    /// This costs a bind, so it is for the one case where the statement's own
    /// execution produced no meta to read: a write that FAILED. Its intended
    /// target is precisely what makes a FAIL event worth having, and the normal
    /// paths must never call this — they get their datasets from the meta the
    /// query already returned.
    fn plan_datasets(&self, _loc: &PondLocation, _sql: &str) -> Option<latiq_common::QueryMeta> {
        None
    }
    /// Summarize the pond's user tables (for describe_pond).
    fn describe_schema(&self, loc: &PondLocation) -> Result<SchemaSummary, EngineError>;
    /// Transient pull from an external catalog: on the pond's instance, LOAD the
    /// type's extensions + create its secrets, `ATTACH` it as `alias`, run `query`
    /// (a `CREATE TABLE … AS SELECT … FROM <alias>.…`), then `DETACH` + drop the
    /// secrets — regardless of success. Nothing about the catalog persists.
    ///
    /// The meta carries the pull's `inputs`/`outputs` when the pond opted into
    /// lineage, and is empty otherwise (same gate as every other path). The
    /// external side is named while the catalog is still ATTACHED — after the
    /// detach nothing in the pond remembers where its rows came from, which is
    /// exactly why this edge is worth recording.
    fn pull_catalog(
        &self,
        loc: &PondLocation,
        catalog_type: &str,
        alias: &str,
        params: &std::collections::BTreeMap<String, String>,
        query: &str,
    ) -> Result<latiq_common::QueryMeta, EngineError>;
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
