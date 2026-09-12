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
    /// The statement parses and its names resolve, but it asks for a feature
    /// this engine/storage does not implement — a `PRIMARY KEY`, an index, a
    /// sequence, a generated column. Nothing is broken and nothing retries into
    /// success: the caller drops the clause or does not get it. Deliberately not
    /// `Parse` (the syntax was fine) and deliberately not `Engine` (nothing of
    /// ours failed, and an operator has nothing to fix).
    ///
    /// `feature` is the thing the engine NAMED as unsupported, lifted out of its
    /// own sentence so a client can branch on a value instead of parsing prose.
    /// `None` when the message names one in a shape we do not recognise — an
    /// unnamed feature is left unnamed rather than guessed at.
    #[error("unsupported feature: {message}")]
    Unsupported {
        message: String,
        feature: Option<String>,
    },
    /// The statement (or the pond, or the catalog attach) needs a capability
    /// this DEPLOYMENT has not provisioned — today, a DuckDB extension that is
    /// not in the node's cache, with downloads disabled on every request path.
    ///
    /// Its own variant because no other one's advice is true here: nothing of
    /// ours crashed (`Engine` → `internal` → "retry, then wake a human" is
    /// advice to repeat a call that cannot succeed), nothing about the call is
    /// wrong (so it is not `Unsupported`, whose fix is deleting a clause), and
    /// the pond's own files are fine. The caller cannot fix it and must not
    /// loop; an operator installs the capability.
    ///
    /// `capability` is the missing thing, NAMED — the extension, not a sentence
    /// about it — so a client branches on the value. It and `message` are built
    /// from one argument at one site (`instance::extension_not_cached`), so the
    /// prose and the value cannot disagree.
    #[error("capability unavailable: {message}")]
    CapabilityUnavailable { message: String, capability: String },
    /// A value or argument in the statement is not acceptable to the engine —
    /// out of range for its type, or invalid for the function or file it was
    /// passed to. Distinct from [`Self::Conversion`], which is specifically
    /// "this text is not that type": the fix here is the value or the argument,
    /// not a CAST.
    #[error("invalid input: {0}")]
    InvalidInput(String),
    /// A parameter the CALLER supplies — a catalog's `endpoint`, `metadata_path`,
    /// `data_path` — was not provided, so the engine was never asked to do
    /// anything. Nothing has run and nothing of ours failed.
    ///
    /// Its own variant because the two neighbours both lie about it. `Engine` →
    /// `internal` → "Retry; if it persists, report to your operator" is the
    /// Nexus-finding-8 shape exactly: the message already says *"requires --set
    /// metadata_path=<catalog-db>"*, so the caller is told to re-send an
    /// identical call that can never succeed and then to wake an operator who
    /// has nothing to fix. And it is not [`Self::InvalidInput`], whose advice is
    /// about a value the engine rejected (a CAST, a narrower type) — here there
    /// is no value at all. The message NAMES the parameter; supplying it is the
    /// whole fix.
    #[error("missing parameter: {0}")]
    MissingParameter(String),
    /// A parameter the CALLER supplied names something this engine does not
    /// offer — a catalog `type` that is not one of the supported set. Present,
    /// well-formed, and not a thing: the fix is a different value, and the
    /// message names the legal set.
    ///
    /// Split from [`Self::MissingParameter`] because "you left it out" and "that
    /// is not one of the choices" are different edits, and from
    /// [`Self::InvalidInput`] for the same reason that one is: this value never
    /// reached DuckDB, so DuckDB's advice about casts and ranges does not apply.
    #[error("unsupported parameter: {0}")]
    UnsupportedParameter(String),
    /// `attach_catalog` was asked for an alias this pond already has mounted.
    ///
    /// Its own variant, not [`Self::Catalog`], because the two have opposite
    /// fixes and `Catalog`'s advice ("look up what the pond actually has —
    /// describe_pond, SHOW TABLES") is about the pond's *tables* and answers
    /// nothing here. The alias carries as a VALUE so the envelope can name it
    /// without the message being parsed.
    ///
    /// Deliberately not silently idempotent: re-attaching over a live alias with
    /// different options or a different credential would swap what every
    /// in-flight statement in the pond resolves `lake.` against.
    #[error("a catalog is already attached as '{name}'")]
    CatalogAlreadyAttached { name: String },
    /// `detach_catalog` (or a statement) named an alias that is not attached —
    /// including after a node restart, which loses every attachment. Its own
    /// variant so the envelope's `suggest` can name `attach_catalog`.
    #[error("no catalog is attached as '{name}'")]
    CatalogNotAttached { name: String },
    /// The caller's own statement drove the transaction Latiq owns (`BEGIN` /
    /// `COMMIT` / `ROLLBACK`), or that transaction could not be closed. Its own
    /// variant because the retry advice is different from every other one here:
    /// part of the statement may already have committed.
    #[error("transaction error: {0}")]
    TransactionControl(String),
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
    ///
    /// `trace_id` is the calling request's ambient trace id, recorded alongside
    /// the identity so a snapshot can be joined to that request's lineage events
    /// and `latiq::access` records. It is passed rather than read here because
    /// engine calls run on a blocking thread, where the ambient trace scope (a
    /// task-local) is not visible. `None` means "no trace scope" and is recorded
    /// as an absent key, never as a placeholder.
    fn write_query(
        &self,
        loc: &PondLocation,
        sql: &str,
        identity: &Identity,
        trace_id: Option<&str>,
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
    /// Mount an external catalog on the pond and **leave it mounted**: LOAD the
    /// type's extensions, `CREATE SECRET` from `secrets`, `ATTACH … AS name`.
    ///
    /// The attachment lives on the pond's one DuckDB instance (invariant 7) and
    /// outlives the call, which is the whole point: two catalogs attached at
    /// once is what lets ordinary `write_query` SQL join across them. It is
    /// **not persisted** — a node restart loses it, and a query naming a
    /// vanished alias gets an actionable error pointing back here.
    ///
    /// Not a write to the pond, so no `Identity` and no attribution bracket: it
    /// mutates session/instance state, and nothing lands in the pond's DuckLake
    /// catalog until the caller's own `write_query` runs.
    ///
    /// `secrets` is [`latiq_common::Secret`]-typed all the way in, so an
    /// implementation cannot log or echo it by accident; the values may only be
    /// exposed inside the `CREATE SECRET` statement.
    fn attach_catalog(
        &self,
        loc: &PondLocation,
        catalog_type: &str,
        name: &str,
        options: &std::collections::BTreeMap<String, String>,
        secrets: &std::collections::BTreeMap<String, latiq_common::Secret>,
    ) -> Result<crate::result::AttachedCatalog, EngineError>;
    /// `DETACH` a catalog and **drop the secrets that were created for it**.
    /// Dropping the credential is half the contract, not a tidy-up: after this
    /// returns nothing on the node holds the caller's token.
    ///
    /// An alias that is not attached is a [`EngineError::Catalog`] — the caller
    /// believes something is mounted that is not, and saying so is more useful
    /// than an idempotent success.
    fn detach_catalog(&self, loc: &PondLocation, name: &str) -> Result<(), EngineError>;
    /// What is attached to this pond right now, in attach order.
    fn attached_catalogs(
        &self,
        loc: &PondLocation,
    ) -> Result<Vec<crate::result::AttachedCatalog>, EngineError>;
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
