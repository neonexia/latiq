//! AgentOps — the protocol-neutral agent operations. Composes ControlPlane +
//! PondStorage + QueryEngine. Engine calls (blocking DuckDB) run on the blocking
//! pool; cancellation flows through the in-flight registry's AbortToken.
use crate::arrow::ArrowReadStream;
use crate::control::ControlPlane;
use crate::error::AgentError;
use crate::forward::Forwarder;
use crate::inflight::InFlightRegistry;
use crate::types::{
    AllocateResult, CatalogInfo, DatasetInfo, DescribeResult, LoadDatasetResult, PondInfo,
    PullResult,
};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use latiq_common::ErrorKind;
use latiq_common::Identity;
use latiq_common::PondId;
use latiq_common::QueryMeta;
use latiq_common::{PondTier, ResourceLimits};
use latiq_engine::{ArrowSink, ExplainResult, QueryEngine, QueryResult};
use latiq_storage::PondStorage;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tracing::info;

#[derive(Debug, Clone)]
pub struct AgentConfig {
    pub inline_row_cap: usize,
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            inline_row_cap: 10_000,
        }
    }
}

#[derive(Clone)]
pub struct AgentOps {
    control: Arc<dyn ControlPlane>,
    storage: Arc<dyn PondStorage>,
    engine: Arc<dyn QueryEngine>,
    inflight: InFlightRegistry,
    config: AgentConfig,
    /// This node's own internal endpoint (registered with the control plane).
    /// `None` in single-node/in-process setups, where forwarding never applies.
    self_endpoint: Option<String>,
    /// Delegate for ponds owned by a different node. `None` = single-node: every
    /// pond is local, so the behavior is exactly as before forwarding existed.
    forwarder: Option<Arc<dyn Forwarder>>,
}

impl AgentOps {
    pub fn new(
        control: Arc<dyn ControlPlane>,
        storage: Arc<dyn PondStorage>,
        engine: Arc<dyn QueryEngine>,
        config: AgentConfig,
    ) -> Self {
        Self {
            control,
            storage,
            engine,
            inflight: InFlightRegistry::new(),
            config,
            self_endpoint: None,
            forwarder: None,
        }
    }

    /// Enable node-to-node forwarding: requests for ponds owned by a node other
    /// than `self_endpoint` are delegated to `forwarder`. Without this, all ponds
    /// are treated as local (single-node behavior).
    pub fn with_forwarding(mut self, self_endpoint: String, forwarder: Arc<dyn Forwarder>) -> Self {
        self.self_endpoint = Some(self_endpoint);
        self.forwarder = Some(forwarder);
        self
    }

    pub fn inflight(&self) -> &InFlightRegistry {
        &self.inflight
    }

    /// Pond instances open on this node (for the `latiq_node_open_ponds` gauge).
    pub fn open_pond_count(&self) -> usize {
        self.engine.open_pond_count()
    }

    /// If this node isn't the pond's owner (and forwarding is configured), return
    /// the delegate + owner endpoint to forward to. `None` → handle locally. A
    /// pond with no live owner (`node_endpoint == None`) also runs locally, so the
    /// local error path (e.g. storage/availability) surfaces normally.
    fn forward_target<'a>(&'a self, info: &'a PondInfo) -> Option<(&'a dyn Forwarder, &'a str)> {
        let fwd = self.forwarder.as_ref()?;
        let me = self.self_endpoint.as_deref()?;
        let owner = info.node_endpoint.as_deref()?;
        if owner == me {
            return None;
        }
        Some((fwd.as_ref(), owner))
    }

    fn parse_id(pond_id: &str) -> Result<PondId, AgentError> {
        PondId::parse(pond_id).map_err(|e| AgentError::internal(format!("bad pond id: {e}")))
    }

    pub async fn allocate_pond(
        &self,
        identity: &Identity,
        name: Option<String>,
        policy_json: &str,
        tier: &str,
        extensions: &[String],
    ) -> Result<AllocateResult, AgentError> {
        let info = self
            .control
            .create_pond(name, &identity.agent_id, policy_json, tier, extensions)
            .await?;
        // The control plane may place the pond on a different node than the one
        // that received this call. In that case, don't eagerly create storage here
        // (it would orphan files on the wrong node) — the owning node materializes
        // it lazily on first use (ensure_pond). Single-node: owner == self, so this
        // is skipped and the eager init below runs as before.
        if self.forward_target(&info).is_some() {
            self.audit(identity, "allocate_pond", Some(&info.pond_id), None, 0)
                .await;
            return Ok(AllocateResult {
                pond_id: info.pond_id,
                pond_name: info.name,
            });
        }
        let pid = Self::parse_id(&info.pond_id)?;
        let mut loc = self
            .storage
            .create_pond(pid)
            .map_err(|e| AgentError::internal(format!("storage: {e}")))?;
        loc.catalog_name = info.name.clone();
        loc.limits = tier_limits(&info.tier);
        loc.extensions = info.extensions.clone();

        let engine = self.engine.clone();
        let loc2 = loc.clone();
        let init = tokio::task::spawn_blocking(move || engine.init_pond(&loc2))
            .await
            .map_err(|e| AgentError::internal(format!("join: {e}")))?;
        if let Err(e) = init {
            // Compensate: roll back registry + storage.
            let _ = self.control.drop_pond(&info.pond_id).await;
            let _ = self.storage.drop_pond(pid);
            return Err(e.into());
        }
        self.audit(identity, "allocate_pond", Some(&info.pond_id), None, 0)
            .await;
        Ok(AllocateResult {
            pond_id: info.pond_id,
            pond_name: info.name,
        })
    }

    pub async fn describe_pond(
        &self,
        identity: &Identity,
        pond_ref: &str,
    ) -> Result<DescribeResult, AgentError> {
        let info = self.control.pond_info(pond_ref).await?;
        if let Some((fwd, owner)) = self.forward_target(&info) {
            info!(
                op = "describe_pond",
                pond = pond_ref,
                owner,
                "forwarding to owner node"
            );
            record_forward("describe");
            return fwd.describe(owner, identity, pond_ref).await;
        }
        info!(op = "describe_pond", pond = pond_ref, "processing locally");
        let pid = Self::parse_id(&info.pond_id)?;
        // ensure_pond materializes storage on first touch; attach under the
        // pond's registry name so introspection is scoped to this catalog.
        let mut loc = self
            .storage
            .ensure_pond(pid)
            .map_err(|e| AgentError::internal(format!("storage: {e}")))?;
        loc.catalog_name = info.name.clone();
        loc.limits = tier_limits(&info.tier);
        loc.extensions = info.extensions.clone();
        let engine = self.engine.clone();
        let loc2 = loc.clone();
        let schema = tokio::task::spawn_blocking(move || engine.describe_schema(&loc2))
            .await
            .map_err(|e| AgentError::internal(format!("join: {e}")))??;
        self.audit(identity, "describe_pond", Some(&info.pond_id), None, 0)
            .await;
        Ok(DescribeResult { pond: info, schema })
    }

    pub async fn list_ponds(&self, identity: &Identity) -> Result<Vec<PondInfo>, AgentError> {
        let ponds = self.control.list_ponds().await?;
        self.audit(identity, "list_ponds", None, None, 0).await;
        Ok(ponds)
    }

    pub async fn drop_pond(
        &self,
        identity: &Identity,
        pond_ref: &str,
        confirm: bool,
    ) -> Result<(), AgentError> {
        // drop_pond deletes the pond and ALL its data — require explicit confirm.
        // Every surface plumbs this flag; enforcing it here keeps the gate
        // consistent across MCP and the Data gRPC.
        if !confirm {
            return Err(AgentError::new(
                ErrorKind::MissingArgument,
                format!("drop_pond deletes pond '{pond_ref}' and all its data; set confirm=true to proceed"),
                "Re-issue drop_pond with confirm=true once you're certain.",
                "latiq://guidance",
            ));
        }
        let info = self.control.pond_info(pond_ref).await?;
        // Owned by another node → forward the drop so the owner evicts its engine
        // instance and deletes the files it actually holds.
        if let Some((fwd, owner)) = self.forward_target(&info) {
            info!(
                op = "drop_pond",
                pond = pond_ref,
                owner,
                "forwarding to owner node"
            );
            record_forward("drop");
            return fwd.drop_pond(owner, identity, pond_ref, confirm).await;
        }
        info!(op = "drop_pond", pond = pond_ref, "processing locally");
        let pond_id = info.pond_id.clone();
        let pid = Self::parse_id(&pond_id)?;
        // Tombstone the pond + cancel its in-flight ops. begin_drop also makes any
        // query that registers from here on get a pre-cancelled token, so one that
        // slipped past resolve_pond can't run against files we're about to delete.
        self.inflight.begin_drop(&pond_id);
        if let Err(e) = self.control.drop_pond(&pond_id).await {
            // Registry drop failed: the pond still exists — clear the tombstone so
            // it stays usable instead of permanently rejecting queries.
            self.inflight.end_drop(&pond_id);
            return Err(e);
        }
        // Evict the cached engine instance (closing its connection to the catalog)
        // BEFORE deleting the files out from under it. Best-effort: a pond that was
        // never queried has no location/instance to forget.
        if let Ok(loc) = self.storage.pond_location(pid) {
            self.engine.forget_pond(&loc);
        }
        let _ = self.storage.drop_pond(pid);
        self.inflight.end_drop(&pond_id);
        self.audit(identity, "drop_pond", Some(&pond_id), None, 0)
            .await;
        Ok(())
    }

    pub async fn read_query(
        &self,
        identity: &Identity,
        pond_ref: &str,
        sql: &str,
    ) -> Result<QueryResult, AgentError> {
        let res = self.run_query(pond_ref, sql, identity, false).await?;
        Ok(res)
    }

    pub async fn write_query(
        &self,
        identity: &Identity,
        pond_ref: &str,
        sql: &str,
    ) -> Result<QueryResult, AgentError> {
        let res = self.run_query(pond_ref, sql, identity, true).await?;
        Ok(res)
    }

    // ---- datasets + external catalogs -------------------------------------

    /// Browse/search datasets (in the built-in `latiq` catalog).
    pub async fn list_datasets(&self, query: &str) -> Result<Vec<DatasetInfo>, AgentError> {
        self.control.list_datasets(query).await
    }

    pub async fn get_dataset(&self, name: &str) -> Result<DatasetInfo, AgentError> {
        self.control.get_dataset(name).await
    }

    /// Browse/search external catalogs.
    pub async fn list_catalogs(&self, query: &str) -> Result<Vec<CatalogInfo>, AgentError> {
        self.control.list_catalogs(query).await
    }

    pub async fn get_catalog(&self, name: &str) -> Result<CatalogInfo, AgentError> {
        self.control.get_catalog(name).await
    }

    /// Copy a dataset's tables into a pond — one `CREATE OR REPLACE TABLE … AS
    /// SELECT * FROM read_*(uri)` per table, routed through the normal write path
    /// (so forwarding to the owning node is handled).
    pub async fn load_dataset(
        &self,
        identity: &Identity,
        pond_ref: &str,
        dataset: &str,
    ) -> Result<LoadDatasetResult, AgentError> {
        let ds = self.control.get_dataset(dataset).await?;
        // Each dataset loads into its own schema (named after the dataset) so its
        // tables are grouped and never collide with another dataset's tables.
        // Multi-table datasets (e.g. tpch) become tpch.lineitem, tpch.orders, …
        let schema = ds.name.clone();
        self.write_query(identity, pond_ref, &create_schema_sql(&schema))
            .await?;
        let mut loaded = Vec::with_capacity(ds.tables.len());
        for t in &ds.tables {
            let sql = dataset_load_sql(&schema, &t.table_name, &t.source_uri, &t.format);
            self.write_query(identity, pond_ref, &sql).await?;
            loaded.push(format!("{schema}.{}", t.table_name));
        }
        Ok(LoadDatasetResult {
            dataset: ds.name,
            schema,
            tables: loaded,
        })
    }

    /// Transient pull from an external catalog: resolve it, merge the pull-time
    /// `params` over its persisted locator params (pull wins), then on the pond's
    /// engine: attach (with creds) → run `query` (a CREATE TABLE …) → detach. The
    /// query's result table lands in the pond; nothing about the catalog persists.
    pub async fn catalog_pull(
        &self,
        identity: &Identity,
        pond_ref: &str,
        catalog: &str,
        query: &str,
        params: std::collections::BTreeMap<String, String>,
    ) -> Result<PullResult, AgentError> {
        let info = self.control.pond_info(pond_ref).await?;
        if let Some((fwd, owner)) = self.forward_target(&info) {
            info!(
                op = "catalog_pull",
                pond = pond_ref,
                owner,
                "forwarding to owner node"
            );
            record_forward("catalog_pull");
            return fwd
                .catalog_pull(owner, identity, pond_ref, catalog, query, params)
                .await;
        }
        let (loc, cat, merged) = self.prepare_pull(&info, catalog, params).await?;
        let engine = self.engine.clone();
        let (ty, alias, q) = (cat.r#type.clone(), cat.name.clone(), query.to_string());
        tokio::task::spawn_blocking(move || engine.pull_catalog(&loc, &ty, &alias, &merged, &q))
            .await
            .map_err(|e| AgentError::internal(format!("join: {e}")))??;
        self.audit(
            identity,
            "catalog_pull",
            Some(pond_ref),
            Some(query.to_string()),
            0,
        )
        .await;
        Ok(PullResult {
            catalog: cat.name,
            query: query.to_string(),
        })
    }

    /// Transiently attach a catalog on the pond and list its tables.
    pub async fn catalog_describe(
        &self,
        identity: &Identity,
        pond_ref: &str,
        catalog: &str,
        params: std::collections::BTreeMap<String, String>,
    ) -> Result<Vec<(String, String)>, AgentError> {
        let info = self.control.pond_info(pond_ref).await?;
        if let Some((fwd, owner)) = self.forward_target(&info) {
            info!(
                op = "catalog_describe",
                pond = pond_ref,
                owner,
                "forwarding to owner node"
            );
            record_forward("catalog_describe");
            return fwd
                .catalog_describe(owner, identity, pond_ref, catalog, params)
                .await;
        }
        let (loc, cat, merged) = self.prepare_pull(&info, catalog, params).await?;
        let engine = self.engine.clone();
        let (ty, alias) = (cat.r#type.clone(), cat.name.clone());
        tokio::task::spawn_blocking(move || engine.describe_catalog(&loc, &ty, &alias, &merged))
            .await
            .map_err(|e| AgentError::internal(format!("join: {e}")))?
            .map_err(Into::into)
    }

    /// Shared LOCAL setup for pull/describe (the caller has already resolved the
    /// pond and confirmed this node owns it — remote ponds are forwarded before
    /// reaching here): resolve the catalog and merge its locator params with the
    /// pull-time params (pull wins).
    async fn prepare_pull(
        &self,
        info: &PondInfo,
        catalog: &str,
        params: std::collections::BTreeMap<String, String>,
    ) -> Result<
        (
            latiq_storage::PondLocation,
            CatalogInfo,
            std::collections::BTreeMap<String, String>,
        ),
        AgentError,
    > {
        let cat = self.control.get_catalog(catalog).await?;
        let mut merged = cat.params.clone();
        merged.extend(params);
        let pid = Self::parse_id(&info.pond_id)?;
        let mut loc = self
            .storage
            .ensure_pond(pid)
            .map_err(|e| AgentError::internal(format!("storage: {e}")))?;
        loc.catalog_name = info.name.clone();
        loc.limits = tier_limits(&info.tier);
        loc.extensions = info.extensions.clone();
        Ok((loc, cat, merged))
    }

    /// Stream a read as Arrow batches. Local: drive the engine's `read_arrow` on
    /// the blocking pool, delivering the schema then batches over channels
    /// (bounded mpsc → backpressure). Remote: forward to the owning node. The
    /// schema is resolved before returning, so an empty result still carries
    /// columns and a pre-stream error (parse / pond-not-found) surfaces here
    /// rather than mid-stream.
    pub async fn read_arrow(
        &self,
        identity: &Identity,
        pond_ref: &str,
        sql: &str,
    ) -> Result<ArrowReadStream, AgentError> {
        let info = self.control.pond_info(pond_ref).await?;
        if let Some((fwd, owner)) = self.forward_target(&info) {
            info!(
                op = "read_arrow",
                pond = pond_ref,
                owner,
                "forwarding to owner node"
            );
            record_forward("read_arrow");
            return fwd.read_arrow(owner, identity, pond_ref, sql).await;
        }
        info!(op = "read_arrow", pond = pond_ref, "processing locally");
        record_query(&info.name, "read");
        metrics::gauge!("latiq_pond_inflight_queries", "pond" => info.name.clone()).increment(1.0);
        let pond_id = info.pond_id.clone();
        let pid = Self::parse_id(&pond_id)?;
        let mut loc = self
            .storage
            .ensure_pond(pid)
            .map_err(|e| AgentError::internal(format!("storage: {e}")))?;
        loc.catalog_name = info.name.clone();
        loc.limits = tier_limits(&info.tier);
        loc.extensions = info.extensions.clone();
        let (op_id, token) = self.inflight.register(Some(pond_id));

        let (schema_tx, schema_rx) = oneshot::channel::<Result<SchemaRef, AgentError>>();
        let (batch_tx, batch_rx) = mpsc::channel::<Result<RecordBatch, AgentError>>(4);

        let engine = self.engine.clone();
        let inflight = self.inflight.clone();
        let sql2 = sql.to_string();
        let pond_name = info.name.clone();
        tokio::task::spawn_blocking(move || {
            let mut sink = ChannelSink {
                schema_tx: Some(schema_tx),
                batch_tx,
            };
            let res = engine.read_arrow(&loc, &sql2, token, &mut sink);
            if let Err(e) = res {
                let ae = AgentError::from(e);
                // Deliver the error on whichever channel is still open: the schema
                // oneshot (no batches produced yet) or the batch stream.
                if let Some(stx) = sink.schema_tx.take() {
                    let _ = stx.send(Err(ae));
                } else {
                    let _ = sink.batch_tx.blocking_send(Err(ae));
                }
            }
            inflight.complete(&op_id);
            // Streaming done (or the consumer dropped) → release the live-load gauge.
            metrics::gauge!("latiq_pond_inflight_queries", "pond" => pond_name).decrement(1.0);
        });

        let schema = schema_rx
            .await
            .map_err(|_| AgentError::internal("arrow read produced no schema"))?
            .inspect_err(|e| record_error(&info.name, e))?;
        Ok(ArrowReadStream {
            schema,
            batches: Box::pin(ReceiverStream::new(batch_rx)),
        })
    }

    /// Read via the Arrow hop, then collect the batches into the neutral
    /// `{columns, rows}` `QueryResult` the JSON edges (Data gRPC, MCP) return —
    /// bounded by the inline cap. So MCP/CLI reads ride the same Arrow internal
    /// transport (no double-materialize on a forward) and only convert to JSON
    /// once here, at the edge.
    pub async fn read_collected(
        &self,
        identity: &Identity,
        pond_ref: &str,
        sql: &str,
    ) -> Result<QueryResult, AgentError> {
        let stream = self.read_arrow(identity, pond_ref, sql).await?;
        let columns: Vec<String> = stream
            .schema
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect();
        let mut rows: Vec<Vec<serde_json::Value>> = Vec::new();
        let mut batches = stream.batches;
        while let Some(b) = batches.next().await {
            append_batch_rows(&b?, &columns, &mut rows)?;
            if rows.len() > self.config.inline_row_cap {
                return Err(AgentError::result_cap_exceeded(
                    rows.len(),
                    self.config.inline_row_cap,
                ));
            }
        }
        let n = rows.len() as u64;
        Ok(QueryResult {
            columns,
            rows,
            meta: QueryMeta {
                rows: n,
                ..Default::default()
            },
        })
    }

    async fn run_query(
        &self,
        pond_ref: &str,
        sql: &str,
        identity: &Identity,
        write: bool,
    ) -> Result<QueryResult, AgentError> {
        let info = self.control.pond_info(pond_ref).await?;
        // The CLI sends every statement through write_query (it doesn't parse SQL;
        // the engine classifies it), and forwarding happens *before* execution — so
        // at this point we can't honestly say read vs write. Log a neutral "query".
        // Owned by another node → forward and relay. The owner audits + snapshots;
        // we just return its result, so attribution stays on the node that ran it.
        if let Some((fwd, owner)) = self.forward_target(&info) {
            info!(
                op = "query",
                pond = pond_ref,
                owner,
                "forwarding to owner node"
            );
            record_forward("query");
            return if write {
                fwd.write(owner, identity, pond_ref, sql).await
            } else {
                fwd.read(owner, identity, pond_ref, sql).await
            };
        }
        info!(op = "query", pond = pond_ref, "processing locally");
        record_query(&info.name, if write { "write" } else { "read" });
        metrics::gauge!("latiq_pond_inflight_queries", "pond" => info.name.clone()).increment(1.0);
        let pond_id = info.pond_id.clone();
        let pid = Self::parse_id(&pond_id)?;
        // ensure_pond materializes storage on first touch; attach the catalog
        // under the pond's registry name so callers query `<pond>.snapshots()`.
        let mut loc = self
            .storage
            .ensure_pond(pid)
            .map_err(|e| AgentError::internal(format!("storage: {e}")))?;
        loc.catalog_name = info.name.clone();
        loc.limits = tier_limits(&info.tier);
        loc.extensions = info.extensions.clone();
        let (op_id, token) = self.inflight.register(Some(pond_id.clone()));

        let engine = self.engine.clone();
        let loc2 = loc.clone();
        let sql2 = sql.to_string();
        let identity2 = identity.clone();
        let t0 = Instant::now();
        let out = tokio::task::spawn_blocking(move || {
            if write {
                engine.write_query(&loc2, &sql2, &identity2, token)
            } else {
                engine.read_query(&loc2, &sql2, token)
            }
        })
        .await
        .map_err(|e| AgentError::internal(format!("join: {e}")));
        self.inflight.complete(&op_id);
        metrics::gauge!("latiq_pond_inflight_queries", "pond" => info.name.clone()).decrement(1.0);

        let result = out?;
        let qr = match result {
            Ok(qr) => qr,
            Err(e) => {
                let ae = AgentError::from(e);
                record_error(&info.name, &ae);
                return Err(ae);
            }
        };
        if !write && qr.rows.len() > self.config.inline_row_cap {
            let ae = AgentError::result_cap_exceeded(qr.rows.len(), self.config.inline_row_cap);
            record_error(&info.name, &ae);
            return Err(ae);
        }
        let elapsed = t0.elapsed();
        record_query_duration(&info.name, if write { "write" } else { "read" }, elapsed);
        let op = if write { "write_query" } else { "read_query" };
        self.audit(
            identity,
            op,
            Some(&pond_id),
            Some(redact_sql(sql)),
            elapsed.as_millis() as u64,
        )
        .await;
        Ok(qr)
    }

    pub async fn explain_query(
        &self,
        identity: &Identity,
        pond_ref: &str,
        sql: &str,
    ) -> Result<ExplainResult, AgentError> {
        let info = self.control.pond_info(pond_ref).await?;
        if let Some((fwd, owner)) = self.forward_target(&info) {
            info!(
                op = "explain_query",
                pond = pond_ref,
                owner,
                "forwarding to owner node"
            );
            record_forward("explain");
            return fwd.explain(owner, identity, pond_ref, sql).await;
        }
        info!(op = "explain_query", pond = pond_ref, "processing locally");
        let pid = Self::parse_id(&info.pond_id)?;
        // ensure_pond materializes storage on first touch; attach under the
        // pond's registry name so the plan resolves names in this catalog.
        let mut loc = self
            .storage
            .ensure_pond(pid)
            .map_err(|e| AgentError::internal(format!("storage: {e}")))?;
        loc.catalog_name = info.name.clone();
        loc.limits = tier_limits(&info.tier);
        loc.extensions = info.extensions.clone();
        let engine = self.engine.clone();
        let loc2 = loc.clone();
        let sql2 = sql.to_string();
        let res = tokio::task::spawn_blocking(move || engine.explain_query(&loc2, &sql2))
            .await
            .map_err(|e| AgentError::internal(format!("join: {e}")))??;
        self.audit(
            identity,
            "explain_query",
            Some(info.pond_id.as_str()),
            Some(redact_sql(sql)),
            0,
        )
        .await;
        Ok(res)
    }

    /// Emit one access record as a structured trace on the `latiq::access`
    /// target. There is no audit store — operators grep the node log files (or
    /// ship them to their log stack; `LATIQ_LOG_FORMAT=json` makes the fields
    /// structured). Filter with e.g. `RUST_LOG=latiq::access=info` or by grepping
    /// the `latiq::access` target / the `agent=`/`op=` fields.
    ///
    /// Kept `async` so the (many) call sites are unchanged; the body is a
    /// non-blocking trace emit (no store, no await).
    async fn audit(
        &self,
        identity: &Identity,
        operation: &str,
        pond_id: Option<&str>,
        request_summary: Option<String>,
        duration_ms: u64,
    ) {
        info!(
            target: "latiq::access",
            agent = %identity.agent_id,
            verified = identity.verified,
            op = operation,
            pond = pond_id.unwrap_or("-"),
            duration_ms,
            summary = request_summary.as_deref().unwrap_or(""),
            "access",
        );
    }
}

/// Map a pond's tier name to its resource caps (unknown/empty → medium).
fn tier_limits(tier: &str) -> Option<ResourceLimits> {
    Some(PondTier::parse(tier).unwrap_or_default().limits())
}

/// Build the `CREATE OR REPLACE TABLE … AS SELECT * FROM read_*(uri)` for one
/// dataset table. `format` picks the DuckDB reader; `auto` infers from the URI
/// extension. The table name is quoted and the URI's single quotes are escaped
/// (the catalog is operator-curated, but we still don't let a stray quote break
/// out of the string literal).
/// Quote a SQL identifier, doubling embedded `"` so any dataset/table name is a
/// valid identifier. (Kept local — `agent-core` is engine-neutral.)
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn create_schema_sql(schema: &str) -> String {
    format!("CREATE SCHEMA IF NOT EXISTS {}", quote_ident(schema))
}

fn dataset_load_sql(schema: &str, table_name: &str, source_uri: &str, format: &str) -> String {
    let reader = match format.trim().to_lowercase().as_str() {
        "csv" => "read_csv_auto",
        "json" => "read_json_auto",
        "parquet" => "read_parquet",
        _ => {
            let u = source_uri.to_lowercase();
            if u.ends_with(".csv") {
                "read_csv_auto"
            } else if u.ends_with(".json") || u.ends_with(".ndjson") {
                "read_json_auto"
            } else {
                "read_parquet"
            }
        }
    };
    let table = format!("{}.{}", quote_ident(schema), quote_ident(table_name));
    let uri = source_uri.replace('\'', "''");
    format!("CREATE OR REPLACE TABLE {table} AS SELECT * FROM {reader}('{uri}')")
}

/// Per-pond query/error counters — recorded on the node that actually runs the
/// query (the local path, after the forward decision), labeled by pond name.
/// Counters give over-time load (`rate`/`increase` in Prometheus).
fn record_query(pond: &str, op: &'static str) {
    metrics::counter!("latiq_pond_queries_total", "pond" => pond.to_string(), "op" => op)
        .increment(1);
}
fn record_error(pond: &str, e: &AgentError) {
    metrics::counter!("latiq_pond_errors_total", "pond" => pond.to_string(), "kind" => format!("{:?}", e.envelope().kind))
        .increment(1);
}
/// Query wall-clock latency (engine execution on the owning node), in seconds —
/// the histogram for p50/p95/p99 (`histogram_quantile` in Prometheus). Recorded
/// where the query actually ran, labeled by pond + op.
fn record_query_duration(pond: &str, op: &'static str, elapsed: std::time::Duration) {
    metrics::histogram!("latiq_pond_query_duration_seconds", "pond" => pond.to_string(), "op" => op)
        .record(elapsed.as_secs_f64());
}
/// Count an operation forwarded to another node (multi-node path), by op. Lets
/// operators see how much traffic crosses node boundaries vs. runs locally.
fn record_forward(op: &'static str) {
    metrics::counter!("latiq_forwarded_total", "op" => op).increment(1);
}

/// Convert one Arrow `RecordBatch` to positional JSON rows aligned to `columns`.
/// Uses Arrow's JSON writer (column-keyed objects), then reshapes to arrays in
/// column order — a missing key (a null cell) becomes JSON null.
fn append_batch_rows(
    batch: &RecordBatch,
    columns: &[String],
    out: &mut Vec<Vec<serde_json::Value>>,
) -> Result<(), AgentError> {
    let mut buf = Vec::new();
    let mut w = arrow::json::ArrayWriter::new(&mut buf);
    w.write(batch)
        .map_err(|e| AgentError::internal(format!("arrow->json: {e}")))?;
    w.finish()
        .map_err(|e| AgentError::internal(format!("arrow->json: {e}")))?;
    let objs: Vec<serde_json::Map<String, serde_json::Value>> = serde_json::from_slice(&buf)
        .map_err(|e| AgentError::internal(format!("arrow->json parse: {e}")))?;
    for mut obj in objs {
        let mut row = Vec::with_capacity(columns.len());
        for c in columns {
            row.push(obj.remove(c).unwrap_or(serde_json::Value::Null));
        }
        out.push(row);
    }
    Ok(())
}

/// Bridges the engine's blocking Arrow output to async channels: the schema once
/// (oneshot), then batches (bounded mpsc → backpressure). A closed channel
/// (consumer dropped) returns `Break`, which stops the engine promptly.
struct ChannelSink {
    schema_tx: Option<oneshot::Sender<Result<SchemaRef, AgentError>>>,
    batch_tx: mpsc::Sender<Result<RecordBatch, AgentError>>,
}

impl ArrowSink for ChannelSink {
    fn schema(&mut self, schema: SchemaRef) -> ControlFlow<()> {
        if let Some(tx) = self.schema_tx.take() {
            if tx.send(Ok(schema)).is_err() {
                return ControlFlow::Break(()); // receiver gone
            }
        }
        ControlFlow::Continue(())
    }
    fn batch(&mut self, batch: RecordBatch) -> ControlFlow<()> {
        match self.batch_tx.blocking_send(Ok(batch)) {
            Ok(()) => ControlFlow::Continue(()),
            Err(_) => ControlFlow::Break(()),
        }
    }
}

/// Minimal SQL-shape redaction for the audit log: drop comments (so literals
/// hidden in them can't leak), then collapse quoted-string and numeric *literals*
/// to `?`. Identifiers that merely contain digits (`t1`, `events2`) are preserved.
/// (A full parser-based redactor is future work.)
fn redact_sql(sql: &str) -> String {
    let decommented = strip_sql_comments(sql);
    let mut out = String::with_capacity(decommented.len());
    let mut chars = decommented.chars().peekable();
    // Whether the previously emitted char was part of an identifier — a digit
    // right after one (e.g. the `1` in `t1`) is part of that identifier, not a
    // numeric literal, so it must not be collapsed.
    let mut prev_ident = false;
    while let Some(c) = chars.next() {
        match c {
            '\'' => {
                // String literal: consume to the closing quote, treating `''` as
                // an escaped quote (not the terminator).
                while let Some(n) = chars.next() {
                    if n == '\'' {
                        if chars.peek() == Some(&'\'') {
                            chars.next();
                        } else {
                            break;
                        }
                    }
                }
                out.push('?');
                prev_ident = false;
            }
            d if d.is_ascii_digit() && !prev_ident => {
                // Numeric literal: collapse the digit/decimal run.
                while matches!(chars.peek(), Some(n) if n.is_ascii_digit() || *n == '.') {
                    chars.next();
                }
                out.push('?');
                prev_ident = false;
            }
            other => {
                prev_ident = other.is_ascii_alphanumeric() || other == '_';
                out.push(other);
            }
        }
    }
    out
}

/// Strip SQL comments (`-- … EOL` and `/* … */`), leaving string literals — and
/// any `--`/`/*` *inside* them — untouched.
fn strip_sql_comments(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\'' => {
                // Copy the whole string literal verbatim (with `''` escapes).
                out.push('\'');
                while let Some(n) = chars.next() {
                    out.push(n);
                    if n == '\'' {
                        if chars.peek() == Some(&'\'') {
                            out.push(chars.next().unwrap());
                        } else {
                            break;
                        }
                    }
                }
            }
            '-' if chars.peek() == Some(&'-') => {
                // Line comment: drop to (and keep) the newline.
                for n in chars.by_ref() {
                    if n == '\n' {
                        out.push('\n');
                        break;
                    }
                }
            }
            '/' if chars.peek() == Some(&'*') => {
                chars.next(); // consume '*'
                let mut prev = '\0';
                for n in chars.by_ref() {
                    if prev == '*' && n == '/' {
                        break;
                    }
                    prev = n;
                }
                out.push(' '); // keep a token separator
            }
            other => out.push(other),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redaction_collapses_literals() {
        assert_eq!(
            redact_sql("SELECT * FROM events WHERE id = 47 AND sev = 'high'"),
            "SELECT * FROM events WHERE id = ? AND sev = ?"
        );
    }

    #[test]
    fn redaction_preserves_identifiers_with_digits() {
        // t1/t2/events2 are identifiers, not literals — they must survive.
        assert_eq!(
            redact_sql("SELECT t1.x FROM events2 AS t1 JOIN t2 ON t1.id = 5"),
            "SELECT t1.x FROM events2 AS t1 JOIN t2 ON t1.id = ?"
        );
    }

    #[test]
    fn redaction_strips_line_comment_so_literals_do_not_leak() {
        let r = redact_sql("SELECT 1 -- pw is 'hunter2'\nFROM t");
        assert!(!r.contains("hunter2"), "literal leaked via comment: {r}");
        assert!(!r.contains("pw is"), "comment text leaked: {r}");
        assert_eq!(r, "SELECT ? \nFROM t");
    }

    #[test]
    fn redaction_strips_block_comment() {
        let r = redact_sql("SELECT /* secret 'hunter2' */ id FROM t");
        assert!(
            !r.contains("hunter2"),
            "literal leaked via block comment: {r}"
        );
        assert!(!r.contains("secret"));
        // The block comment becomes whitespace; the statement shape survives.
        assert_eq!(
            r.split_whitespace().collect::<Vec<_>>(),
            ["SELECT", "id", "FROM", "t"]
        );
    }

    #[test]
    fn redaction_handles_escaped_quote_and_keeps_dashes_in_strings() {
        // `''` escape inside a literal, and a `--` that lives inside a string must
        // NOT be treated as a comment — the whole literal collapses to one `?`.
        assert_eq!(
            redact_sql("INSERT INTO t VALUES ('it''s -- not a comment')"),
            "INSERT INTO t VALUES (?)"
        );
    }
}
