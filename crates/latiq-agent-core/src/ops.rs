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

//! AgentOps — the protocol-neutral agent operations. Composes ControlPlane +
//! PondStorage + QueryEngine. Engine calls (blocking DuckDB) run on the blocking
//! pool; cancellation flows through the in-flight registry's AbortToken.
use crate::access::{outcome, ERROR, OK};
use crate::arrow::ArrowReadStream;
use crate::control::ControlPlane;
use crate::deadline::{Deadline, QueryControls};
use crate::error::AgentError;
use crate::forward::{Forwarder, Peer};
use crate::inflight::InFlightRegistry;
use crate::lineage::{QueryRecord, IN_PROCESS_NODE};
use crate::types::{
    AllocateResult, CatalogInfo, DatasetInfo, DescribeResult, LineagePage, LoadDatasetResult,
    PondInfo, PullResult,
};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use latiq_common::ErrorKind;
use latiq_common::Identity;
use latiq_common::PondId;
use latiq_common::QueryMeta;
use latiq_common::QueryTimeouts;
use latiq_common::{PondTier, ResourceLimits};
use latiq_engine::{AbortToken, ArrowSink, ExplainResult, QueryEngine, QueryResult};
use latiq_lineage::event::DurationMeaning;
use latiq_lineage::{EventSink, LineageWriter};
use latiq_storage::PondStorage;
use std::collections::HashMap;
use std::ops::ControlFlow;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::Instant;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tracing::info;

/// The `see` target for every lineage-shaped error — a real served resource
/// (`latiq-mcp`'s `resources.rs`), because a `see` that resolves to nothing is
/// worse than none at all.
const LINEAGE_RECIPE: &str = "latiq://recipes/lineage";

/// Where a pond's work must run, as `AgentOps::placement` decides it.
///
/// The distinction this type exists to make: **`Local` is a positive statement
/// of ownership**, never a default. Before it, "this node owns the pond" and
/// "nobody knows who owns the pond" were both `None` and both served locally —
/// so a node with no claim to a pond would create an empty one of its own and
/// answer a query out of it, indistinguishably from a pond that really was
/// empty.
enum Placement<'a> {
    /// Serve here: this node is the registered owner, or this process is not
    /// clustered at all (no forwarder / no advertised endpoint — the embedded
    /// SDK and single-node `dev.sh`), where every pond is by definition local.
    Local,
    /// Another node owns it; delegate over this forwarder to that peer.
    Forward(&'a dyn Forwarder, Peer<'a>),
    /// The pond exists but the registry names no node serving it. Refuse —
    /// `AgentError::pond_unavailable`.
    NoOwner,
}

/// Node-wide limits on what an op may return inline. The cap is the reason a
/// read has a bounded worst case at all, so raising it raises every surface's
/// per-request memory ceiling at once.
#[derive(Debug, Clone)]
pub struct AgentConfig {
    /// Rows a materialized (non-streaming) read may return before it fails with
    /// `result_cap_exceeded`. Streamed reads (`read_arrow`) are not capped.
    pub inline_row_cap: usize,
    /// How long a statement may run on this node: the default applied when a
    /// caller names no `timeout_ms`, and the maximum every request is clamped
    /// to. The maximum is the OPERATOR's protection — one DuckDB instance per
    /// pond means an unbounded query pins that pond for everyone.
    pub timeouts: QueryTimeouts,
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            inline_row_cap: 10_000,
            timeouts: QueryTimeouts::default(),
        }
    }
}

/// Every operation Latiq offers, expressed once and without a protocol. MCP,
/// Data gRPC and Stream gRPC are all inbound adapters onto this — a new surface
/// is a new adapter, never a change in here (invariant 5).
///
/// The public op methods are also the single place attribution happens: each
/// one audits (`access::record`) and, for a pond that opted in, emits lineage.
/// Doing that inside a helper instead would record the same operation twice
/// under two names. Cheap to `clone` (everything behind it is an `Arc`).
#[derive(Clone)]
pub struct AgentOps {
    control: Arc<dyn ControlPlane>,
    storage: Arc<dyn PondStorage>,
    engine: Arc<dyn QueryEngine>,
    inflight: InFlightRegistry,
    config: AgentConfig,
    /// This node's own stable id — the one it registered and heartbeats with,
    /// and the one the registry assigns ponds by. `None` in
    /// single-node/in-process setups, where forwarding never applies.
    ///
    /// **This, and never `self_endpoint`, is what ownership is decided on.**
    /// Taken from the node's config, never derived from the endpoint: two
    /// spellings of one address (a trailing slash, a hostname vs its IP, a node
    /// that was re-addressed) made a node conclude it was not the owner and dial
    /// itself, re-entering this same decision and forwarding again without
    /// bound (#89).
    self_node_id: Option<String>,
    /// This node's own internal endpoint (registered with the control plane).
    /// `None` in single-node/in-process setups, where forwarding never applies.
    /// An ADDRESS, used for two things only: dialling is done with the *owner's*
    /// endpoint, and this one names the node in `QueryMeta::served_by`.
    self_endpoint: Option<String>,
    /// Delegate for ponds owned by a different node. `None` = single-node: every
    /// pond is local, so the behavior is exactly as before forwarding existed.
    forwarder: Option<Arc<dyn Forwarder>>,
    /// One lineage writer per opted-in pond, built lazily on that pond's first
    /// emit and evicted by `drop_pond`. Per pond because a writer owns a
    /// directory (the pond's own `lineage/`) and a batch buffer; keyed by pond
    /// id and shared across `AgentOps` clones, so the batching is per pond and
    /// not per request. A pond that never opts in never gets an entry.
    ///
    /// An `RwLock` because every query of every lineage-enabled pond reads it
    /// and only a pond's FIRST emit writes it — a `Mutex` here would serialize
    /// lineage emission for the whole node behind one pond's map lookup.
    lineage_writers: Arc<RwLock<HashMap<String, Arc<LineageWriter>>>>,
    /// Poisoning is permanent, so the warning about it fires once per node
    /// rather than once per query (the same discipline `LineageWriter` uses for
    /// its own buffer lock).
    lineage_poison_warned: Arc<AtomicBool>,
    /// The node's optional OpenLineage HTTP backend, handed to every writer
    /// this node builds. A trait object, so this crate stays protocol-neutral
    /// (invariant 5): the transport is `latiq-lineage`'s feature-gated
    /// `HttpSink`, and nothing in here knows that HTTP is what it does.
    lineage_sink: Option<Arc<dyn EventSink>>,
    /// The engine's version, asked once here rather than per query: it goes on
    /// every lineage event, and a pond WITHOUT lineage must not pay so much as
    /// an allocation for a field it will never emit.
    engine_version: String,
}

impl AgentOps {
    pub fn new(
        control: Arc<dyn ControlPlane>,
        storage: Arc<dyn PondStorage>,
        engine: Arc<dyn QueryEngine>,
        config: AgentConfig,
    ) -> Self {
        Self {
            engine_version: engine.version(),
            control,
            storage,
            engine,
            inflight: InFlightRegistry::new(),
            config,
            self_node_id: None,
            self_endpoint: None,
            forwarder: None,
            lineage_writers: Arc::new(RwLock::new(HashMap::new())),
            lineage_poison_warned: Arc::new(AtomicBool::new(false)),
            lineage_sink: None,
        }
    }

    /// Also publish every lineage event this node records to `sink` — the
    /// optional OpenLineage HTTP backend.
    ///
    /// Purely additive: the pond's own files are written exactly as before, and
    /// a sink that is down, slow or dead cannot fail, slow or block a query
    /// (see `latiq_lineage::sink`). It is the durability answer for a pond that
    /// gets dropped, whose files go with it.
    pub fn with_lineage_sink(mut self, sink: Arc<dyn EventSink>) -> Self {
        self.lineage_sink = Some(sink);
        self
    }

    /// Enable node-to-node forwarding: requests for ponds owned by a node whose
    /// id is not `self_node_id` are delegated to `forwarder`, dialled at the
    /// owner's endpoint. Without this, all ponds are treated as local
    /// (single-node behavior).
    ///
    /// The two arguments are NOT interchangeable and the order matters:
    /// `self_node_id` is the id this node registered with (the identity it is
    /// known by), `self_endpoint` is the address peers dial it at. Ownership is
    /// decided on the first; only the second is ever dialled or advertised.
    pub fn with_forwarding(
        mut self,
        self_node_id: String,
        self_endpoint: String,
        forwarder: Arc<dyn Forwarder>,
    ) -> Self {
        self.self_node_id = Some(self_node_id);
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

    /// Ponds with a live lineage writer on this node — always 0 in a deployment
    /// where nobody opted in. A companion to `open_pond_count` (same shape, same
    /// purpose: an observable count of a per-pond resource this node holds).
    pub fn lineage_writer_count(&self) -> usize {
        self.lineage_writers_read().map(|m| m.len()).unwrap_or(0)
    }

    /// Write out every pond's buffered lineage events now.
    ///
    /// **This blocks** (the writer fsyncs), so it belongs on a shutdown path or
    /// a blocking task, never in a request handler. That is a property of THIS
    /// call, not of the query path: a query only ever buffers, and hands the
    /// occasional due batch to `spawn_blocking` without awaiting it (see
    /// `emit_lineage`), so no query ever waits behind an fsync.
    pub fn flush_lineage(&self) {
        let writers: Vec<Arc<LineageWriter>> = match self.lineage_writers_read() {
            Some(map) => map.values().cloned().collect(),
            None => return,
        };
        // Flushing outside the lock: an IO stall must not block the next emit.
        for writer in writers {
            writer.flush();
        }
    }

    /// Deliver whatever the optional lineage sink still has queued, giving up
    /// after `budget`.
    ///
    /// The companion to `flush_lineage` on the shutdown path, and it must run
    /// **after** it: the file flush can hand the sink nothing (the sink is fed
    /// at buffer time, not at write time), but ordering them this way means the
    /// cheap, always-correct half happens first and the bounded, best-effort
    /// half gets whatever budget is left.
    ///
    /// Awaits rather than blocks, and is bounded by the sink itself. A node
    /// with no backend configured returns immediately.
    pub async fn drain_lineage_sink(&self, budget: std::time::Duration) {
        if let Some(sink) = self.lineage_sink.as_deref() {
            sink.drain(budget).await;
        }
    }

    /// Emit this operation's lineage. Called from the PUBLIC op methods beside
    /// `self.audit(...)`, and only on the local path — see `crate::lineage`.
    ///
    /// The `lineage` check comes first and costs one bool: a pond that did not
    /// opt in must not reach the writer registry, the storage lookup, or any
    /// string formatting.
    ///
    /// Building the events is pure memory. The batch write is not — it fsyncs —
    /// so when one comes due it goes to the blocking pool, and is deliberately
    /// **not awaited**: the query has already produced its answer and must not
    /// wait on lineage IO, nor may that IO occupy the async worker it ran on.
    /// The events are in the buffer before this returns, so nothing is lost if
    /// that task is slow; the buffer is bounded, and `Drop` flushes what is left.
    fn emit_lineage(&self, rec: QueryRecord<'_>) {
        if !rec.info.lineage {
            return;
        }
        let Some(writer) = self.lineage_writer(rec.info) else {
            return; // nothing to do, and never anything to fail
        };
        if crate::lineage::record(&writer, self.serving_name(), rec) {
            tokio::task::spawn_blocking(move || writer.flush());
        }
    }

    /// This pond's writer, built on first use. The pond's `lineage_dir` is
    /// resolved once per pond here rather than once per query: by the time an op
    /// emits, its local path has already ensured the pond exists.
    ///
    /// The location is resolved with **no lock held** — for `LocalFs` it stats
    /// the pond directory, and one slow stat must not serialize emission for
    /// every other pond on the node. The cost is that two first emits for one
    /// pond can both build a writer; the insert keeps whichever landed first, so
    /// a pond never ends up with two writers batching into one directory.
    ///
    /// `None` on any failure — a pond whose location will not resolve loses its
    /// lineage, and never its query.
    fn lineage_writer(&self, info: &PondInfo) -> Option<Arc<LineageWriter>> {
        {
            let map = self.lineage_writers_read()?;
            if let Some(writer) = map.get(&info.pond_id) {
                return Some(writer.clone());
            }
        }
        let pid = Self::parse_id(&info.pond_id).ok()?;
        let loc = self
            .storage
            .pond_location(pid)
            .inspect_err(|e| {
                tracing::warn!(pond = %info.pond_id, %e, "no lineage for this pond: unresolved location");
            })
            .ok()?;
        let mut writer = LineageWriter::new(&loc.lineage_dir);
        if let Some(sink) = self.lineage_sink.clone() {
            writer = writer.with_sink(sink);
        }
        let writer = Arc::new(writer);
        let mut map = self.lineage_writers_write()?;
        Some(
            map.entry(info.pond_id.clone())
                .or_insert(writer) // a concurrent first emit wins; ours is dropped
                .clone(),
        )
    }

    /// Evict a dropped pond's writer, flushing it on the blocking pool (the
    /// flush fsyncs). Two reasons it cannot be skipped: the map would otherwise
    /// leak an entry per dropped pond for the life of the process, and a writer
    /// that outlived its pond would keep failing and requeueing batches into a
    /// deleted directory until it hit its capacity bound.
    ///
    /// What this does NOT promise: that the writer is gone by the time the
    /// caller deletes the files. `lineage_writer` hands out `Arc` clones, and one
    /// can be alive on an in-flight request's stack; that straggler's `Drop`
    /// flushes later, possibly into a directory that no longer exists, which the
    /// writer answers with a `warn!` and a dropped batch. Harmless, and cheaper
    /// than the refcount gymnastics that forcing last-drop would need — dropping
    /// a pond destroys its provenance either way (the HTTP sink is the
    /// durability answer).
    async fn evict_lineage_writer(&self, pond_id: &str) {
        let writer = self
            .lineage_writers_write()
            .and_then(|mut map| map.remove(pond_id));
        if let Some(writer) = writer {
            let _ = tokio::task::spawn_blocking(move || writer.flush()).await;
        }
    }

    fn lineage_writers_read(
        &self,
    ) -> Option<RwLockReadGuard<'_, HashMap<String, Arc<LineageWriter>>>> {
        match self.lineage_writers.read() {
            Ok(guard) => Some(guard),
            Err(_) => {
                self.warn_lineage_poisoned();
                None
            }
        }
    }

    fn lineage_writers_write(
        &self,
    ) -> Option<RwLockWriteGuard<'_, HashMap<String, Arc<LineageWriter>>>> {
        match self.lineage_writers.write() {
            Ok(guard) => Some(guard),
            Err(_) => {
                self.warn_lineage_poisoned();
                None
            }
        }
    }

    /// A panic elsewhere poisoned the registry, which disables lineage for the
    /// rest of the process. Said once, and said out loud: silence here is
    /// indistinguishable from a deployment where nobody opted in, and
    /// `lineage_writer_count` would report 0 for both.
    fn warn_lineage_poisoned(&self) {
        if !self.lineage_poison_warned.swap(true, Ordering::Relaxed) {
            tracing::warn!("lineage writer registry is poisoned; lineage is disabled on this node");
        }
    }

    /// Where this pond's work belongs. Three states, and the whole point of the
    /// enum is that the third one used to be indistinguishable from the first.
    ///
    /// A node may serve a pond only when it is **named** as the owner (or when
    /// this process is not clustered at all — see [`Placement`]). "The registry
    /// does not say who owns this" is NOT permission to serve it: the files are
    /// on another host, and `ensure_pond` here would happily create an empty
    /// pond of the same name and answer out of it.
    fn placement<'a>(&'a self, info: &'a PondInfo) -> Placement<'a> {
        // No forwarder / no identity of our own = single-node or the embedded
        // SDK. There is one node, it owns everything, and there is nothing to
        // forward to or to be wrong about. Checked FIRST so the embedded path
        // never reaches the ownership question.
        let (Some(fwd), Some(me), Some(my_address)) = (
            self.forwarder.as_ref(),
            self.self_node_id.as_deref(),
            self.self_endpoint.as_deref(),
        ) else {
            return Placement::Local;
        };
        // OWNERSHIP IS AN IDENTITY COMPARISON, never an address one (#89). An
        // empty id is not an id: a control plane that cannot name the owning
        // node leaves the question unanswered, and an unanswered question is
        // refused rather than resolved by falling back to the endpoint — that
        // fallback is precisely the bug, and it would reappear on whichever
        // deployment stopped filling the field.
        let owner_id = match info.node_id.as_deref().filter(|id| !id.is_empty()) {
            Some(id) => id,
            None => return Placement::NoOwner,
        };
        if owner_id == me {
            // Ours, whatever the registry has for our address — including
            // nothing at all, since we do not need to dial ourselves.
            return Placement::Local;
        }
        // Someone else's. NOW the endpoint matters, for the one thing it is
        // for: dialling. An empty endpoint is not an address (the owning node's
        // row is gone), so there is nobody to delegate to — the #88 refusal,
        // unchanged.
        match info.node_endpoint.as_deref().filter(|e| !e.is_empty()) {
            None => Placement::NoOwner,
            // Another node id advertising OUR address is a misconfiguration
            // (a copy-pasted `--advertise-addr`), and dialling it would be
            // dialling ourselves — #89's recursion by a second route. Refuse
            // loudly instead. This is the only surviving comparison of two
            // endpoints, and note what it is NOT: it can never make this node
            // the owner, only stop it from calling itself.
            Some(owner) if owner == my_address => {
                tracing::warn!(
                    pond = %info.pond_id,
                    owner = %owner_id,
                    me,
                    address = %my_address,
                    "refusing to forward: another node id advertises this node's own address"
                );
                Placement::NoOwner
            }
            Some(endpoint) => Placement::Forward(
                fwd.as_ref(),
                Peer {
                    node_id: owner_id,
                    endpoint,
                },
            ),
        }
    }

    /// `placement`, with the `NoOwner` refusal already turned into an audited
    /// error — so a call site is `if let Some((fwd, owner)) = self.route(..)? {
    /// forward } else { serve locally }` and cannot silently grow a fall-through
    /// for the unowned case.
    ///
    /// The refusal is recorded like every other rejection: an operator looking
    /// for why an agent's pond went quiet needs it on the same trail.
    async fn route<'a>(
        &'a self,
        identity: &Identity,
        op: &'static str,
        info: &'a PondInfo,
    ) -> Result<Option<(&'a dyn Forwarder, Peer<'a>)>, AgentError> {
        match self.placement(info) {
            Placement::Local => Ok(None),
            Placement::Forward(fwd, owner) => Ok(Some((fwd, owner))),
            Placement::NoOwner => Err(self
                .audit_err(
                    identity,
                    op,
                    Some(&info.pond_id),
                    None,
                    0,
                    AgentError::pond_unavailable(&info.name),
                )
                .await),
        }
    }

    /// What this node calls itself when it says "I ran this": its advertised
    /// internal endpoint, or `in-process` where there is none (single-node,
    /// embedded SDK). One definition for `QueryMeta::served_by` and the lineage
    /// event's `nodeId`, so an operator correlating the two never has to
    /// reconcile two spellings of the same node.
    ///
    /// Deliberately still the ENDPOINT and not `self_node_id`, even though
    /// ownership now routes on the id (#89): this is a shipped wire field whose
    /// stated purpose (#87) is to hand an operator something they can dial and
    /// grep, and the lineage facet's `nodeId` carries the same value for the
    /// same reason. The two answer different questions — "who is the owner" is
    /// an identity comparison, "where did this run" is an address — so the name
    /// here is `serving_name`, not `node_id`, and the id never leaks into it by
    /// accident.
    fn serving_name(&self) -> &str {
        self.self_endpoint.as_deref().unwrap_or(IN_PROCESS_NODE)
    }

    fn parse_id(pond_id: &str) -> Result<PondId, AgentError> {
        PondId::parse(pond_id).map_err(|e| AgentError::internal(format!("bad pond id: {e}")))
    }

    /// Make this pond's storage exist HERE and open its engine instance.
    ///
    /// Always `ensure_pond`, never `create_pond`: this is only ever reached
    /// through `materialize_pond`, whose contract is idempotent, and the control
    /// plane may call it more than once for the same pond (a retried create, a
    /// create racing the lazy `ensure_pond` on a query path). "Already there" is
    /// success.
    async fn materialize_here(&self, info: &PondInfo) -> Result<(), AgentError> {
        let pid = Self::parse_id(&info.pond_id)?;
        let mut loc = self
            .storage
            .ensure_pond(pid, info.lineage)
            .map_err(|e| AgentError::internal(format!("storage: {e}")))?;
        loc.catalog_name = info.name.clone();
        loc.limits = tier_limits(&info.tier);
        loc.extensions = info.extensions.clone();
        loc.lineage = info.lineage;

        let engine = self.engine.clone();
        let loc2 = loc.clone();
        tokio::task::spawn_blocking(move || engine.init_pond(&loc2))
            .await
            .map_err(|e| AgentError::internal(format!("join: {e}")))?
            .map_err(AgentError::from)
    }

    /// Ensure the pond is materialised on the node that owns it — the inbound
    /// half of **eager allocation**.
    ///
    /// Its caller is the **control plane**: `Control::CreatePondAssignment`
    /// places the pond and then calls this on the owning node before it reports
    /// the pond created (root `CLAUDE.md` invariant 3's lifecycle exception).
    /// `Forwarder::materialize_pond` is the outbound half, used when this call
    /// lands — through a gateway — on a node that does not own the pond.
    ///
    /// Internal by audience, not by mechanism: it is reached over the Data gRPC
    /// like every other op, and there is deliberately NO CLI or SDK command for
    /// it (invariants 1 and 2 — this is infrastructure talking to a node, not a
    /// user asking for anything).
    ///
    /// **Idempotent**: a pond whose storage already exists is a success. That is
    /// what makes it safe for the allocating node to retry, and what makes it
    /// harmless when it races the lazy `ensure_pond` on a query path.
    ///
    /// It routes like everything else rather than assuming it was called by a
    /// peer that already resolved the owner: behind a gateway the request can
    /// land anywhere, and a node that materialised a pond it does not own would
    /// create exactly the empty stray pond `Placement` exists to prevent.
    pub async fn materialize_pond(
        &self,
        identity: &Identity,
        pond_ref: &str,
    ) -> Result<(), AgentError> {
        let info = self
            .pond_info_audited(identity, "materialize_pond", pond_ref)
            .await?;
        if let Some((fwd, owner)) = self.route(identity, "materialize_pond", &info).await? {
            info!(
                op = "materialize_pond",
                pond = pond_ref,
                owner = owner.node_id,
                endpoint = owner.endpoint,
                "forwarding to owner node"
            );
            record_forward("materialize_pond");
            return fwd.materialize_pond(owner, identity, pond_ref).await;
        }
        info!(
            op = "materialize_pond",
            pond = pond_ref,
            "processing locally"
        );
        let started = Instant::now();
        let res = self.materialize_here(&info).await;
        self.audit(
            identity,
            "materialize_pond",
            Some(&info.pond_id),
            None,
            started.elapsed().as_millis() as u64,
            outcome(&res),
        )
        .await;
        res
    }

    /// Allocate a pond, recording the attempt either way. Allocation is the one
    /// op with no pond to name on failure (there is not one yet), so a failed
    /// record carries `pond=-`.
    ///
    /// **Allocation is eager and holistic** — but this method is not where that
    /// happens. `ControlPlane::create_pond` returns only once the pond's storage
    /// exists on the node the control plane placed it on, because the control
    /// plane materialises it (and rolls the placement back if it cannot). So
    /// there is nothing to do here beyond asking, and deliberately so: a node
    /// that also materialised would make a second, redundant call for every
    /// allocation, and `latiq pond create` and the SDK — which never come
    /// through `AgentOps` at all — would still be lazy.
    pub async fn allocate_pond(
        &self,
        identity: &Identity,
        name: Option<String>,
        policy_json: &str,
        tier: &str,
        extensions: &[String],
        lineage: bool,
    ) -> Result<AllocateResult, AgentError> {
        let started = Instant::now();
        let res = self
            .allocate_pond_inner(identity, name, policy_json, tier, extensions, lineage)
            .await;
        self.audit(
            identity,
            "allocate_pond",
            res.as_ref().ok().map(|r| r.pond_id.as_str()),
            None,
            started.elapsed().as_millis() as u64,
            outcome(&res),
        )
        .await;
        res
    }

    async fn allocate_pond_inner(
        &self,
        identity: &Identity,
        name: Option<String>,
        policy_json: &str,
        tier: &str,
        extensions: &[String],
        lineage: bool,
    ) -> Result<AllocateResult, AgentError> {
        let info = self
            .control
            .create_pond(
                name,
                &identity.agent_id,
                policy_json,
                tier,
                extensions,
                lineage,
            )
            .await?;
        // Nothing else. The control plane placed the pond AND materialised it on
        // the owner before answering; if it could not, it gave the registry row
        // back and this `?` already returned that error. Materialising again
        // here would be a second call to the same node for the same pond, and
        // materialising *locally* would be worse — this node may not own it, and
        // a greeter with its own copy is the empty pond every forwarded read
        // falls into.
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
        let info = self
            .pond_info_audited(identity, "describe_pond", pond_ref)
            .await?;
        if let Some((fwd, owner)) = self.route(identity, "describe_pond", &info).await? {
            info!(
                op = "describe_pond",
                pond = pond_ref,
                owner = owner.node_id,
                endpoint = owner.endpoint,
                "forwarding to owner node"
            );
            record_forward("describe_pond");
            return fwd.describe(owner, identity, pond_ref).await;
        }
        info!(op = "describe_pond", pond = pond_ref, "processing locally");
        let started = Instant::now();
        let res = self.describe_pond_local(&info).await;
        self.audit(
            identity,
            "describe_pond",
            Some(&info.pond_id),
            None,
            started.elapsed().as_millis() as u64,
            outcome(&res),
        )
        .await;
        Ok(DescribeResult {
            pond: info,
            schema: res?,
        })
    }

    /// NO LINEAGE EVENT: describe reads no data. It reports the pond's tables
    /// and columns from the catalog, so there is no run to attribute and no
    /// dataset was consumed or produced — an event here would add a run to
    /// every consumer's graph that touched nothing.
    ///
    /// The local half of `describe_pond` — split out so its failures are audited
    /// alongside its successes without the forwarded path double-recording (the
    /// owner audits what it actually ran).
    async fn describe_pond_local(
        &self,
        info: &PondInfo,
    ) -> Result<latiq_engine::SchemaSummary, AgentError> {
        let pid = Self::parse_id(&info.pond_id)?;
        // ensure_pond materializes storage on first touch; attach under the
        // pond's registry name so introspection is scoped to this catalog.
        //
        // THE LAZY FALLBACK, and why it survived eager allocation (it is the
        // same `ensure_pond` on every query path here). Eager allocation makes
        // this a no-op for ponds it created — the directory is already there —
        // but it is not the only way a pond row comes to exist: the direct
        // `CreatePondAssignment` path (`latiq pond create`, the SDK) writes a
        // row and no storage, ponds predate this change, and a compensation can
        // fail and leave a row behind. Dropping the fallback would turn each of
        // those into an outage instead of a first query that costs one mkdir.
        // What it costs now is a `stat` per query and the risk it always had:
        // this is why ownership must be decided BEFORE we get here (`route`),
        // since ensuring a pond on a node with no claim to it invents an empty
        // one.
        let mut loc = self
            .storage
            .ensure_pond(pid, info.lineage)
            .map_err(|e| AgentError::internal(format!("storage: {e}")))?;
        loc.catalog_name = info.name.clone();
        loc.limits = tier_limits(&info.tier);
        loc.extensions = info.extensions.clone();
        loc.lineage = info.lineage;
        let engine = self.engine.clone();
        let loc2 = loc.clone();
        Ok(
            tokio::task::spawn_blocking(move || engine.describe_schema(&loc2))
                .await
                .map_err(|e| AgentError::internal(format!("join: {e}")))??,
        )
    }

    pub async fn list_ponds(&self, identity: &Identity) -> Result<Vec<PondInfo>, AgentError> {
        let res = self.control.list_ponds().await;
        self.audit(identity, "list_ponds", None, None, 0, outcome(&res))
            .await;
        res
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
            // Recorded, not silently refused: "someone tried to drop this pond"
            // is exactly the kind of attempt an operator wants in the trail.
            return Err(self.audit_err(
                identity,
                "drop_pond",
                Some(pond_ref),
                None,
                0,
                AgentError::new(
                    ErrorKind::MissingArgument,
                    format!("drop_pond deletes pond '{pond_ref}' and all its data; set confirm=true to proceed"),
                    "Re-issue drop_pond with confirm=true once you're certain.",
                    "latiq://guidance",
                ),
            ).await);
        }
        let info = self
            .pond_info_audited(identity, "drop_pond", pond_ref)
            .await?;
        // Owned by another node → forward the drop so the owner evicts its engine
        // instance and deletes the files it actually holds.
        if let Some((fwd, owner)) = self.route(identity, "drop_pond", &info).await? {
            info!(
                op = "drop_pond",
                pond = pond_ref,
                owner = owner.node_id,
                endpoint = owner.endpoint,
                "forwarding to owner node"
            );
            record_forward("drop_pond");
            return fwd.drop_pond(owner, identity, pond_ref, confirm).await;
        }
        info!(op = "drop_pond", pond = pond_ref, "processing locally");
        let started = Instant::now();
        let pond_id = info.pond_id.clone();
        let pid = match Self::parse_id(&pond_id) {
            Ok(pid) => pid,
            Err(e) => {
                return Err(self
                    .audit_err(identity, "drop_pond", Some(&pond_id), None, 0, e)
                    .await)
            }
        };
        // Tombstone the pond + cancel its in-flight ops. begin_drop also makes any
        // query that registers from here on get a pre-cancelled token, so one that
        // slipped past resolve_pond can't run against files we're about to delete.
        self.inflight.begin_drop(&pond_id);
        if let Err(e) = self.control.drop_pond(&pond_id).await {
            // Registry drop failed: the pond still exists — clear the tombstone so
            // it stays usable instead of permanently rejecting queries.
            self.inflight.end_drop(&pond_id);
            return Err(self
                .audit_err(
                    identity,
                    "drop_pond",
                    Some(&pond_id),
                    None,
                    started.elapsed().as_millis() as u64,
                    e,
                )
                .await);
        }
        // Evict the cached engine instance (closing its connection to the catalog)
        // BEFORE deleting the files out from under it. Best-effort: a pond that was
        // never queried has no location/instance to forget.
        if let Ok(loc) = self.storage.pond_location(pid) {
            self.engine.forget_pond(&loc);
        }
        // Same order, same reason, for the pond's lineage writer: let it go
        // BEFORE the files it writes into are deleted.
        self.evict_lineage_writer(&pond_id).await;
        let _ = self.storage.drop_pond(pid);
        // Again, because a query that slipped past `begin_drop` can re-insert a
        // writer in the window between the two. This narrows that window rather
        // than closing it — a re-insert after this line still leaves an entry
        // pointing at a deleted directory — but it costs a map lookup and turns
        // "leaks for the life of the process" into a race you have to win.
        self.evict_lineage_writer(&pond_id).await;
        self.inflight.end_drop(&pond_id);
        self.audit(
            identity,
            "drop_pond",
            Some(&pond_id),
            None,
            started.elapsed().as_millis() as u64,
            OK,
        )
        .await;
        Ok(())
    }

    pub async fn read_query(
        &self,
        identity: &Identity,
        pond_ref: &str,
        sql: &str,
    ) -> Result<QueryResult, AgentError> {
        self.read_query_with(identity, pond_ref, sql, QueryControls::none())
            .await
    }

    /// [`read_query`](Self::read_query) with the caller's execution controls —
    /// its requested `timeout_ms` and its own cancellation source.
    pub async fn read_query_with(
        &self,
        identity: &Identity,
        pond_ref: &str,
        sql: &str,
        controls: QueryControls,
    ) -> Result<QueryResult, AgentError> {
        let res = self
            .run_query(pond_ref, sql, identity, false, controls)
            .await?;
        Ok(res)
    }

    pub async fn write_query(
        &self,
        identity: &Identity,
        pond_ref: &str,
        sql: &str,
    ) -> Result<QueryResult, AgentError> {
        self.write_query_with(identity, pond_ref, sql, QueryControls::none())
            .await
    }

    /// [`write_query`](Self::write_query) with the caller's execution controls.
    pub async fn write_query_with(
        &self,
        identity: &Identity,
        pond_ref: &str,
        sql: &str,
        controls: QueryControls,
    ) -> Result<QueryResult, AgentError> {
        let res = self
            .run_query(pond_ref, sql, identity, true, controls)
            .await?;
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
    ///
    /// Its component writes are each audited as `write_query` (with the redacted
    /// SQL), so the data movement was never invisible — but the operation the
    /// caller actually asked for was, and "which dataset was pulled into this
    /// pond" is not reconstructable from N `CREATE TABLE` shapes. This records
    /// the op itself, at completion: it is a bounded server-side sequence, and a
    /// partial load that failed on table 3 of 5 must not read as a clean one.
    pub async fn load_dataset(
        &self,
        identity: &Identity,
        pond_ref: &str,
        dataset: &str,
    ) -> Result<LoadDatasetResult, AgentError> {
        let started = Instant::now();
        let res = self.load_dataset_inner(identity, pond_ref, dataset).await;
        self.audit(
            identity,
            "load_dataset",
            Some(pond_ref),
            Some(dataset.to_string()),
            started.elapsed().as_millis() as u64,
            outcome(&res),
        )
        .await;
        res
    }

    async fn load_dataset_inner(
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
        let info = self
            .pond_info_audited(identity, "catalog_pull", pond_ref)
            .await?;
        if let Some((fwd, owner)) = self.route(identity, "catalog_pull", &info).await? {
            info!(
                op = "catalog_pull",
                pond = pond_ref,
                owner = owner.node_id,
                endpoint = owner.endpoint,
                "forwarding to owner node"
            );
            record_forward("catalog_pull");
            return fwd
                .catalog_pull(owner, identity, pond_ref, catalog, query, params)
                .await;
        }
        let started = Instant::now();
        let res = self.catalog_pull_local(&info, catalog, query, params).await;
        let duration_ms = started.elapsed().as_millis() as u64;
        self.audit(
            identity,
            "catalog_pull",
            Some(pond_ref),
            Some(query.to_string()),
            duration_ms,
            outcome(&res),
        )
        .await;
        // The one op whose INPUT is not in the pond, and the edge with the most
        // provenance value: the catalog is detached before this returns, so
        // nothing in the pond afterwards remembers where its rows came from.
        // A failure carries no datasets — the plan bound against a catalog that
        // is gone by now, and re-binding it would attach it again.
        self.emit_lineage(QueryRecord {
            identity,
            info: &info,
            op: "catalog_pull",
            sql: query,
            duration_ms,
            meaning: DurationMeaning::Completion,
            error: res.as_ref().err(),
            meta: res.as_ref().ok().map(|(_, meta)| meta),
            engine_version: &self.engine_version,
        });
        res.map(|(pull, _)| pull)
    }

    /// The local half of `catalog_pull` (see `describe_pond_local` for why it is
    /// split out). Returns the engine's meta alongside the result: the pull's
    /// two sides can only be named while the catalog is attached, so the
    /// emitter cannot go looking for them afterwards.
    async fn catalog_pull_local(
        &self,
        info: &PondInfo,
        catalog: &str,
        query: &str,
        params: std::collections::BTreeMap<String, String>,
    ) -> Result<(PullResult, QueryMeta), AgentError> {
        let (loc, cat, merged) = self.prepare_pull(info, catalog, params).await?;
        let engine = self.engine.clone();
        let (ty, alias, q) = (cat.r#type.clone(), cat.name.clone(), query.to_string());
        let meta = tokio::task::spawn_blocking(move || {
            engine.pull_catalog(&loc, &ty, &alias, &merged, &q)
        })
        .await
        .map_err(|e| AgentError::internal(format!("join: {e}")))??;
        Ok((
            PullResult {
                catalog: cat.name,
                query: query.to_string(),
            },
            meta,
        ))
    }

    /// Transiently attach a catalog on the pond and list its tables.
    pub async fn catalog_describe(
        &self,
        identity: &Identity,
        pond_ref: &str,
        catalog: &str,
        params: std::collections::BTreeMap<String, String>,
    ) -> Result<Vec<(String, String)>, AgentError> {
        let info = self
            .pond_info_audited(identity, "catalog_describe", pond_ref)
            .await?;
        if let Some((fwd, owner)) = self.route(identity, "catalog_describe", &info).await? {
            info!(
                op = "catalog_describe",
                pond = pond_ref,
                owner = owner.node_id,
                endpoint = owner.endpoint,
                "forwarding to owner node"
            );
            record_forward("catalog_describe");
            return fwd
                .catalog_describe(owner, identity, pond_ref, catalog, params)
                .await;
        }
        // This attaches an EXTERNAL catalog on the pond's engine and reads its
        // table list — a real access to a real system, not a registry lookup, so
        // it belongs on the trail like `catalog_pull`.
        let started = Instant::now();
        let res = self.catalog_describe_local(&info, catalog, params).await;
        self.audit(
            identity,
            "catalog_describe",
            Some(&info.pond_id),
            Some(catalog.to_string()),
            started.elapsed().as_millis() as u64,
            outcome(&res),
        )
        .await;
        res
    }

    /// The local half of `catalog_describe` (see `describe_pond_local`).
    async fn catalog_describe_local(
        &self,
        info: &PondInfo,
        catalog: &str,
        params: std::collections::BTreeMap<String, String>,
    ) -> Result<Vec<(String, String)>, AgentError> {
        let (loc, cat, merged) = self.prepare_pull(info, catalog, params).await?;
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
            .ensure_pond(pid, info.lineage)
            .map_err(|e| AgentError::internal(format!("storage: {e}")))?;
        loc.catalog_name = info.name.clone();
        loc.limits = tier_limits(&info.tier);
        loc.extensions = info.extensions.clone();
        loc.lineage = info.lineage;
        Ok((loc, cat, merged))
    }

    /// Stream a read as Arrow batches. Local: drive the engine's `read_arrow` on
    /// the blocking pool, delivering the schema then batches over channels
    /// (bounded mpsc → backpressure). Remote: forward to the owning node. The
    /// schema is resolved before returning, so an empty result still carries
    /// columns and a pre-stream error (parse / pond-not-found) surfaces here
    /// rather than mid-stream.
    ///
    /// AUDIT TIMING — the access record is emitted when the stream is
    /// ESTABLISHED, not when it finishes.
    ///
    /// Establishment is the moment the access is authorized and rows begin to
    /// flow, and it is reached exactly once, here, on the server, before any
    /// byte reaches the client. Completion is not: when a stream ends, and
    /// whether that end is observed at all, is controlled by the consumer. Two
    /// consequences decide it. A read held open for an hour would be invisible
    /// for that hour, so "who is reading this pond right now" could not be
    /// answered from the trail at the one time it matters. And a consumer that
    /// drops mid-stream is noticed in different places on the local and
    /// forwarded paths (`decode_arrow_stream` simply returns when its receiver
    /// is gone), so a completion-time record would be reliable on one path and
    /// not the other. An audit record must not be contingent on the behaviour
    /// of the party being audited.
    ///
    /// The cost is paid in two fields, and it is the right trade: `duration_ms`
    /// measures ESTABLISHMENT (pond resolution, planning, first schema) and not
    /// the life of the stream, and `outcome` says whether the read STARTED — a
    /// read that dies mid-stream still leaves an `ok` record, because the access
    /// it records did happen. Row counts are deliberately not claimed here for
    /// the same reason. The non-streaming `read_collected` runs entirely
    /// server-side, so it can and does record at completion instead.
    pub async fn read_arrow(
        &self,
        identity: &Identity,
        pond_ref: &str,
        sql: &str,
    ) -> Result<ArrowReadStream, AgentError> {
        self.read_arrow_with(identity, pond_ref, sql, QueryControls::none())
            .await
    }

    /// [`read_arrow`](Self::read_arrow) with the caller's execution controls.
    ///
    /// The deadline covers the WHOLE stream, not just its establishment: a
    /// consumer that stops reading holds a DuckLake snapshot pinned (see
    /// `read_arrow_local`), so "the query finished" is the last batch, not the
    /// first schema.
    pub async fn read_arrow_with(
        &self,
        identity: &Identity,
        pond_ref: &str,
        sql: &str,
        controls: QueryControls,
    ) -> Result<ArrowReadStream, AgentError> {
        let info = self
            .pond_info_audited(identity, "read_arrow", pond_ref)
            .await?;
        if let Some((fwd, owner)) = self.route(identity, "read_arrow", &info).await? {
            info!(
                op = "read_arrow",
                pond = pond_ref,
                owner = owner.node_id,
                endpoint = owner.endpoint,
                "forwarding to owner node"
            );
            record_forward("read_arrow");
            // The owner audits the read it actually ran, as everywhere else —
            // and enforces the timeout, under its own policy (see `run_query`).
            return fwd
                .read_arrow(owner, identity, pond_ref, sql, controls.timeout_ms)
                .await;
        }
        info!(op = "read_arrow", pond = pond_ref, "processing locally");
        let started = Instant::now();
        let res = self.read_arrow_local(&info, sql, controls).await;
        let duration_ms = started.elapsed().as_millis() as u64;
        self.audit(
            identity,
            "read_arrow",
            Some(&info.pond_id),
            Some(redact_sql(sql)),
            duration_ms,
            outcome(&res),
        )
        .await;
        // Lineage records establishment for the same reason the audit does, and
        // labels the duration as such: there is no row count and no completion
        // time to claim here, and a `completion` label would be a lie.
        self.emit_lineage(QueryRecord {
            identity,
            info: &info,
            op: "read_arrow",
            sql,
            duration_ms,
            meaning: DurationMeaning::Establishment,
            error: res.as_ref().err(),
            meta: None,
            engine_version: &self.engine_version,
        });
        res
    }

    /// The local half of `read_arrow`: everything from the engine call onward.
    /// Returning as soon as the schema is known is what makes the establishment
    /// -time audit above possible.
    async fn read_arrow_local(
        &self,
        info: &PondInfo,
        sql: &str,
        controls: QueryControls,
    ) -> Result<ArrowReadStream, AgentError> {
        record_query(&info.name, "read");
        metrics::gauge!("latiq_pond_inflight_queries", "pond" => info.name.clone()).increment(1.0);
        let pond_id = info.pond_id.clone();
        let pid = Self::parse_id(&pond_id)?;
        let mut loc = self
            .storage
            .ensure_pond(pid, info.lineage)
            .map_err(|e| AgentError::internal(format!("storage: {e}")))?;
        loc.catalog_name = info.name.clone();
        loc.limits = tier_limits(&info.tier);
        loc.extensions = info.extensions.clone();
        loc.lineage = info.lineage;
        let (op_id, token) = self.inflight.register(Some(pond_id));
        // The guard MOVES into the blocking producer below, so the deadline
        // lives until the STREAM ends rather than until this call returns —
        // which is the whole point here: a streamed read returns at the first
        // schema and can then run for minutes. (Drop fires the abort token, so
        // handing it to the caller's future instead would cut every read at its
        // first batch.)
        let deadline = Deadline::arm(&token, self.config.timeouts, &controls);

        let (schema_tx, schema_rx) = oneshot::channel::<Result<SchemaRef, AgentError>>();
        let (batch_tx, batch_rx) = mpsc::channel::<Result<RecordBatch, AgentError>>(4);
        // The engine's meta is only complete when the last batch has been
        // produced, so it rides its own channel rather than the schema oneshot:
        // a collecting caller drains the stream first and reads it after.
        let (meta_tx, meta_rx) = oneshot::channel::<QueryMeta>();

        let engine = self.engine.clone();
        let inflight = self.inflight.clone();
        let sql2 = sql.to_string();
        let pond_name = info.name.clone();
        // Captured here, in async context: the blocking producer below needs it
        // to await the consumer without owning a runtime.
        let rt = tokio::runtime::Handle::current();
        // The sink's own handle on cancellation. The engine gets `token`; the
        // sink needs it too, because a producer parked on a full channel is the
        // one place an interrupt cannot reach (see `ChannelSink::send_batch`).
        let sink_abort = token.clone();
        tokio::task::spawn_blocking(move || {
            let t0 = Instant::now();
            let mut sink = ChannelSink {
                schema_tx: Some(schema_tx),
                batch_tx,
                rt,
                abort: sink_abort,
            };
            let res = engine.read_arrow(&loc, &sql2, token, &mut sink);
            match res {
                Ok(mut meta) => {
                    meta.timeout_ms = deadline.effective_ms();
                    // Sent before the sink (and so `batch_tx`) is dropped, so a
                    // caller that waits for the end of the stream never waits
                    // for the meta.
                    let _ = meta_tx.send(meta);
                }
                Err(e) => {
                    // Through the deadline: only it can tell our expiry from
                    // somebody's cancel (see `run_query_local`).
                    let ae = deadline.classify(e);
                    // Deliver the error on whichever channel is still open: the
                    // schema oneshot (no batches produced yet) or the batch
                    // stream.
                    if let Some(stx) = sink.schema_tx.take() {
                        let _ = stx.send(Err(ae));
                    } else {
                        // Same bounded wait as a batch: this must not park
                        // forever on a stalled consumer either. The `biased`
                        // select still delivers the error whenever the channel
                        // has room, cancelled or not.
                        let _ = sink.send_batch(Err(ae));
                    }
                }
            }
            inflight.complete(&op_id);
            // Record latency here too — the Arrow stream is the CLI/SDK's primary
            // read path, so the duration histogram would otherwise miss most reads.
            record_query_duration(&pond_name, "read", t0.elapsed());
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
            meta: Some(meta_rx),
            // This node is running the engine, so this node is the one serving
            // it. Set here and not in the meta the blocking producer sends,
            // because the streaming adapter must announce it on its first chunk
            // — long before the last batch resolves that meta.
            served_by: self.serving_name().to_string(),
        })
    }

    /// Read via the Arrow hop, then collect the batches into the neutral
    /// `{columns, rows}` `QueryResult` the JSON edges (Data gRPC, MCP) return —
    /// bounded by the inline cap. So MCP/CLI reads ride the same Arrow internal
    /// transport (no double-materialize on a forward) and only convert to JSON
    /// once here, at the edge.
    ///
    /// Audited at COMPLETION, unlike `read_arrow`: the collection runs entirely
    /// server-side, so nothing about the record depends on the client still
    /// being there. That buys a true `duration_ms` and an `outcome` that
    /// accounts for the whole read — including a result that blows the inline
    /// cap, which is a read that returned no data to anyone.
    ///
    /// Recorded as `read_query`, the RPC the caller actually invoked (the
    /// rejection records on that RPC use the same name), not as the internal
    /// Arrow hop it happens to ride.
    pub async fn read_collected(
        &self,
        identity: &Identity,
        pond_ref: &str,
        sql: &str,
    ) -> Result<QueryResult, AgentError> {
        self.read_collected_with(identity, pond_ref, sql, QueryControls::none())
            .await
    }

    /// [`read_collected`](Self::read_collected) with the caller's execution
    /// controls — the entry point both agent-facing read surfaces use.
    pub async fn read_collected_with(
        &self,
        identity: &Identity,
        pond_ref: &str,
        sql: &str,
        controls: QueryControls,
    ) -> Result<QueryResult, AgentError> {
        let info = self
            .pond_info_audited(identity, "read_query", pond_ref)
            .await?;
        if let Some((fwd, owner)) = self.route(identity, "read_query", &info).await? {
            info!(
                op = "read_query",
                pond = pond_ref,
                owner = owner.node_id,
                endpoint = owner.endpoint,
                "forwarding to owner node"
            );
            record_forward("read_query");
            // The owner audits the read it ran; we only collect its stream.
            let stream = fwd
                .read_arrow(owner, identity, pond_ref, sql, controls.timeout_ms)
                .await?;
            return self.collect_stream(stream).await;
        }
        info!(op = "read_query", pond = pond_ref, "processing locally");
        let started = Instant::now();
        let res = match self.read_arrow_local(&info, sql, controls).await {
            Ok(stream) => self.collect_stream(stream).await,
            Err(e) => Err(e),
        };
        let duration_ms = started.elapsed().as_millis() as u64;
        self.audit(
            identity,
            "read_query",
            Some(&info.pond_id),
            Some(redact_sql(sql)),
            duration_ms,
            outcome(&res),
        )
        .await;
        // Emitted here, in the public method, and NOT in `read_arrow_local` —
        // which this shares with `read_arrow`, so an emitter down there would
        // record one read twice, under two different ops.
        self.emit_lineage(QueryRecord {
            identity,
            info: &info,
            op: "read_query",
            sql,
            duration_ms,
            meaning: DurationMeaning::Completion,
            error: res.as_ref().err(),
            meta: res.as_ref().ok().map(|r| &r.meta),
            engine_version: &self.engine_version,
        });
        res
    }

    /// Drain an Arrow stream into the neutral `QueryResult`, bounded by the
    /// inline cap.
    async fn collect_stream(&self, stream: ArrowReadStream) -> Result<QueryResult, AgentError> {
        let columns: Vec<String> = stream
            .schema
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect();
        let mut rows: Vec<Vec<serde_json::Value>> = Vec::new();
        let mut batches = stream.batches;
        let meta = stream.meta;
        let served_by = stream.served_by;
        while let Some(b) = batches.next().await {
            append_batch_rows(&b?, &columns, &mut rows)?;
            if rows.len() > self.config.inline_row_cap {
                // `rows.len()` here is an ARROW BATCH BOUNDARY, not the result's
                // size — the stream is abandoned at this point and the rest was
                // never counted. Reporting it named a number that was wrong for
                // every result bigger than one batch past the cap, and wrong in
                // the most misleading direction: it always looked *just* over.
                return Err(AgentError::result_cap_exceeded_unknown(
                    self.config.inline_row_cap,
                ));
            }
        }
        let n = rows.len() as u64;
        // The ENGINE's meta, not a fabricated one: this is the CLI/SDK read
        // path, so a synthesised meta here would strip every dataset the engine
        // extracted and `read_collected` would emit dataset-less lineage even
        // once the plan extraction works. `rows` is still ours — it counts what
        // was actually collected, which is what the caller is being handed.
        //
        // A meta only fails to arrive when the producer could not speak for the
        // engine (a stream decoded from a peer's chunks), and then an empty one
        // is the honest answer: this node did not run the query.
        let mut meta = match meta {
            Some(rx) => rx.await.unwrap_or_default(),
            None => QueryMeta::default(),
        };
        meta.rows = n;
        // From the STREAM, not from this node: on a forwarded read the stream
        // was produced by the peer and carries the peer's name, which is the
        // one the caller must see. Assigning `self.serving_name()` here is exactly
        // the bug the field exists to catch.
        meta.served_by = served_by;
        Ok(QueryResult {
            columns,
            rows,
            meta,
        })
    }

    async fn run_query(
        &self,
        pond_ref: &str,
        sql: &str,
        identity: &Identity,
        write: bool,
        controls: QueryControls,
    ) -> Result<QueryResult, AgentError> {
        let op = if write { "write_query" } else { "read_query" };
        let info = self.pond_info_audited(identity, op, pond_ref).await?;
        // The CLI sends every statement through write_query (it doesn't parse SQL;
        // the engine classifies it), and forwarding happens *before* execution — so
        // at this point we can't honestly say read vs write. Log a neutral "query".
        // Owned by another node → forward and relay. The owner audits + snapshots;
        // we just return its result, so attribution stays on the node that ran it.
        if let Some((fwd, owner)) = self.route(identity, op, &info).await? {
            info!(
                op = "query",
                pond = pond_ref,
                owner = owner.node_id,
                endpoint = owner.endpoint,
                "forwarding to owner node"
            );
            // The neutral `query` above is about the SQL (we cannot say read vs
            // write before the engine classifies it); the metric label is about
            // the RPC the caller invoked, which we do know — `op` is the whole
            // value of the label.
            record_forward(if write { "write_query" } else { "read_query" });
            // The REQUESTED timeout crosses the hop, not our effective one: the
            // owner runs the query, so the owner's default and the owner's
            // ceiling are the ones that apply, and it reports what it applied in
            // the meta it relays back. Resolving it here would let a greeter
            // node's policy override the policy of the node actually at risk.
            return if write {
                fwd.write(owner, identity, pond_ref, sql, controls.timeout_ms)
                    .await
            } else {
                fwd.read(owner, identity, pond_ref, sql, controls.timeout_ms)
                    .await
            };
        }
        info!(op = "query", pond = pond_ref, "processing locally");
        let t0 = Instant::now();
        // What the statement's plan said it would touch, filled in only when
        // the statement failed and so produced no meta of its own.
        let mut planned = None;
        let res = self
            .run_query_local(&info, sql, identity, write, controls, &mut planned)
            .await;
        let duration_ms = t0.elapsed().as_millis() as u64;
        self.audit(
            identity,
            op,
            Some(&info.pond_id),
            Some(redact_sql(sql)),
            duration_ms,
            outcome(&res),
        )
        .await;
        self.emit_lineage(QueryRecord {
            identity,
            info: &info,
            op,
            sql,
            duration_ms,
            meaning: DurationMeaning::Completion,
            error: res.as_ref().err(),
            // The result's meta when there is one; the plan's when the
            // statement failed — a FAIL event that names the table the write
            // was aiming at is the one a reader actually needs.
            meta: res.as_ref().ok().map(|r| &r.meta).or(planned.as_ref()),
            engine_version: &self.engine_version,
        });
        res
    }

    /// The local half of `run_query` (see `describe_pond_local` for why it is
    /// split out): every way this can fail is now on the access trail, where
    /// before only the successes were.
    ///
    /// `planned` is an out-parameter rather than part of the return type
    /// because it is filled in exactly where the return type cannot carry it:
    /// a write that FAILED still knows what it meant to touch, and that is the
    /// event where knowing the intended target matters most.
    async fn run_query_local(
        &self,
        info: &PondInfo,
        sql: &str,
        identity: &Identity,
        write: bool,
        controls: QueryControls,
        planned: &mut Option<QueryMeta>,
    ) -> Result<QueryResult, AgentError> {
        record_query(&info.name, if write { "write" } else { "read" });
        metrics::gauge!("latiq_pond_inflight_queries", "pond" => info.name.clone()).increment(1.0);
        let pond_id = info.pond_id.clone();
        let pid = Self::parse_id(&pond_id)?;
        // ensure_pond materializes storage on first touch; attach the catalog
        // under the pond's registry name so callers query `<pond>.snapshots()`.
        let mut loc = self
            .storage
            .ensure_pond(pid, info.lineage)
            .map_err(|e| AgentError::internal(format!("storage: {e}")))?;
        loc.catalog_name = info.name.clone();
        loc.limits = tier_limits(&info.tier);
        loc.extensions = info.extensions.clone();
        loc.lineage = info.lineage;
        let (op_id, token) = self.inflight.register(Some(pond_id.clone()));
        // Armed BEFORE the engine call and dropped after it. The guard owns the
        // watcher task, so a query that finishes on time leaves nothing behind
        // — and because Drop also fires the abort token, a caller that hangs up
        // mid-await takes its detached engine call down with it rather than
        // leaving one running with no deadline at all.
        let deadline = Deadline::arm(&token, self.config.timeouts, &controls);

        let engine = self.engine.clone();
        let loc2 = loc.clone();
        let sql2 = sql.to_string();
        let identity2 = identity.clone();
        let t0 = Instant::now();
        let out = tokio::task::spawn_blocking(move || {
            let res = if write {
                engine.write_query(&loc2, &sql2, &identity2, token)
            } else {
                engine.read_query(&loc2, &sql2, token)
            };
            // Only for a FAILED write, and only when the pond opted into
            // lineage (the engine enforces that gate itself): a healthy query
            // reports its datasets through its own meta and must not pay for a
            // second bind. On the same blocking thread, because engine calls
            // block.
            let planned = match (&res, write) {
                (Err(_), true) => engine.plan_datasets(&loc2, &sql2),
                _ => None,
            };
            (res, planned)
        })
        .await
        .map_err(|e| AgentError::internal(format!("join: {e}")));
        self.inflight.complete(&op_id);
        metrics::gauge!("latiq_pond_inflight_queries", "pond" => info.name.clone()).decrement(1.0);

        let (result, from_plan) = out?;
        *planned = from_plan;
        let mut qr = match result {
            Ok(qr) => qr,
            Err(e) => {
                // Through the deadline, never `AgentError::from` directly: the
                // engine reports our expiry and somebody's cancel as the SAME
                // `Cancelled` (both are one `INTERRUPT`), and this is the only
                // place that knows which of the two happened.
                let ae = deadline.classify(e);
                record_error(&info.name, &ae);
                return Err(ae);
            }
        };
        if !write && qr.rows.len() > self.config.inline_row_cap {
            // The EXACT count, and this is the one path entitled to say it: the
            // engine materialized the whole result before we looked, so
            // `rows.len()` is the result's size and not a point we gave up at.
            // The streaming collector says `more than {cap}` instead.
            let ae = AgentError::result_cap_exceeded(qr.rows.len(), self.config.inline_row_cap);
            record_error(&info.name, &ae);
            return Err(ae);
        }
        record_query_duration(
            &info.name,
            if write { "write" } else { "read" },
            t0.elapsed(),
        );
        // Stamped on the LOCAL path only, and this is the whole discipline
        // behind the field: the forwarded path returns the peer's result
        // untouched further up, so a relayed meta keeps the owner's name and
        // never acquires the greeter's.
        qr.meta.served_by = self.serving_name().to_string();
        // Stamped on the LOCAL path for the same reason as `served_by`: the node
        // that RAN the statement is the one whose policy applied. Reporting it
        // on every success is what makes a clamp visible — an agent that asked
        // for 30 minutes on a node capped at 5 can see it got 5.
        qr.meta.timeout_ms = deadline.effective_ms();
        Ok(qr)
    }

    pub async fn explain_query(
        &self,
        identity: &Identity,
        pond_ref: &str,
        sql: &str,
    ) -> Result<ExplainResult, AgentError> {
        let info = self
            .pond_info_audited(identity, "explain_query", pond_ref)
            .await?;
        if let Some((fwd, owner)) = self.route(identity, "explain_query", &info).await? {
            info!(
                op = "explain_query",
                pond = pond_ref,
                owner = owner.node_id,
                endpoint = owner.endpoint,
                "forwarding to owner node"
            );
            record_forward("explain_query");
            return fwd.explain(owner, identity, pond_ref, sql).await;
        }
        info!(op = "explain_query", pond = pond_ref, "processing locally");
        let started = Instant::now();
        let res = self.explain_query_local(&info, sql).await;
        self.audit(
            identity,
            "explain_query",
            Some(info.pond_id.as_str()),
            Some(redact_sql(sql)),
            started.elapsed().as_millis() as u64,
            outcome(&res),
        )
        .await;
        res
    }

    /// NO LINEAGE EVENT: explain executes nothing. The statement is planned and
    /// discarded, so no data moved and no snapshot exists to version — the
    /// access trail records the attempt, which is the right place for it.
    ///
    /// The local half of `explain_query` (see `describe_pond_local`).
    async fn explain_query_local(
        &self,
        info: &PondInfo,
        sql: &str,
    ) -> Result<ExplainResult, AgentError> {
        let pid = Self::parse_id(&info.pond_id)?;
        // ensure_pond materializes storage on first touch; attach under the
        // pond's registry name so the plan resolves names in this catalog.
        let mut loc = self
            .storage
            .ensure_pond(pid, info.lineage)
            .map_err(|e| AgentError::internal(format!("storage: {e}")))?;
        loc.catalog_name = info.name.clone();
        loc.limits = tier_limits(&info.tier);
        loc.extensions = info.extensions.clone();
        loc.lineage = info.lineage;
        let engine = self.engine.clone();
        let loc2 = loc.clone();
        let sql2 = sql.to_string();
        Ok(
            tokio::task::spawn_blocking(move || engine.explain_query(&loc2, &sql2))
                .await
                .map_err(|e| AgentError::internal(format!("join: {e}")))??,
        )
    }

    /// A page of the pond's OpenLineage trail, newest first.
    ///
    /// The read half of lineage, and protocol-neutral like everything else here
    /// — the MCP `get_lineage` tool is an adapter over this, and a second
    /// surface would be another adapter and not a change in here.
    ///
    /// **The paging contract.** `since` is an INCLUSIVE lower bound ("what has
    /// happened since I last looked") and `before` an EXCLUSIVE upper one (the
    /// backward cursor: pass the oldest `eventTime` you received to get the
    /// next older page). A page is never cut in the middle of one `eventTime`,
    /// so walking `before` backwards visits every event exactly once — see
    /// `latiq_lineage::read_newest` for the one degenerate exception.
    ///
    /// Two things it does NOT do, on purpose:
    ///
    /// - **No DuckDB.** The events are JSONL on the owning node's disk and are
    ///   returned verbatim; nothing is attached, and no Latiq object is created
    ///   in the pond's catalog (invariant 6).
    /// - **No filtering or aggregation** beyond the bounds + caps. A caller
    ///   that wants either can run `read_json_auto` over `lineage_dir`, which
    ///   is why the page carries it.
    pub async fn get_lineage(
        &self,
        identity: &Identity,
        pond_ref: &str,
        limit: usize,
        since: Option<&str>,
        before: Option<&str>,
    ) -> Result<LineagePage, AgentError> {
        let info = self
            .pond_info_audited(identity, "get_lineage", pond_ref)
            .await?;
        // Validated before the forward decision: the owner would refuse it
        // identically, and a peer hop to earn the same error is pure latency.
        if limit == 0 {
            return Err(self
                .audit_err(
                    identity,
                    "get_lineage",
                    Some(&info.pond_id),
                    None,
                    0,
                    AgentError::new(
                        ErrorKind::InvalidValue,
                        "`limit` must be at least 1.".to_string(),
                        format!(
                            "Omit `limit` for the default page, or pass a positive count \
                             (clamped to {}, and the page reports the value applied as \
                             `limit_applied`).",
                            latiq_lineage::MAX_LIMIT
                        ),
                        LINEAGE_RECIPE,
                    ),
                )
                .await);
        }
        // Clamped HERE, once, and before the forward: the page reports the
        // applied value (`limit_applied`), so the owner must be asked for the
        // number the caller will be told about. Clamping rather than refusing is
        // deliberate — the cap protects the caller's own context — but a clamp
        // nobody can see is the failure this whole class of bug is about.
        let limit = limit.min(latiq_lineage::MAX_LIMIT);
        // The events are FILES on the node that ran the queries, so the owner
        // is the only node that can answer — forwarded exactly like every other
        // pond-scoped op, token replay included.
        if let Some((fwd, owner)) = self.route(identity, "get_lineage", &info).await? {
            info!(
                op = "get_lineage",
                pond = pond_ref,
                owner = owner.node_id,
                endpoint = owner.endpoint,
                "forwarding to owner node"
            );
            record_forward("get_lineage");
            return fwd
                .get_lineage(owner, identity, pond_ref, limit, since, before)
                .await;
        }
        info!(op = "get_lineage", pond = pond_ref, "processing locally");
        let started = Instant::now();
        let res = self.get_lineage_local(&info, limit, since, before).await;
        self.audit(
            identity,
            "get_lineage",
            Some(info.pond_id.as_str()),
            None,
            started.elapsed().as_millis() as u64,
            outcome(&res),
        )
        .await;
        res
    }

    async fn get_lineage_local(
        &self,
        info: &PondInfo,
        limit: usize,
        since: Option<&str>,
        before: Option<&str>,
    ) -> Result<LineagePage, AgentError> {
        // "We were never recording" is a different answer from "nothing
        // happened", and a caller that cannot tell them apart will read an
        // empty list as proof the data appeared from nowhere. So: an error,
        // with the one action that fixes it.
        if !info.lineage {
            return Err(AgentError::new(
                ErrorKind::InvalidValue,
                format!(
                    "Pond '{}' does not record lineage — it was allocated without it, and that is \
                     fixed for the pond's lifetime. No events exist for it, which is NOT the same \
                     as nothing having happened here.",
                    info.name
                ),
                "Allocate a new pond with lineage=true and do the work there; an existing pond \
                 cannot be switched on.",
                LINEAGE_RECIPE,
            ));
        }
        let pid = Self::parse_id(&info.pond_id)?;
        // This node OWNS the pond by the time we get here (a pond with no
        // registered owner was refused at the routing decision, and every other
        // remote case was forwarded), so a location that will not resolve means
        // the pond was never materialized on the node that holds it — nothing
        // has run against it yet, and there is no trail to read.
        let loc = self.storage.pond_location(pid).map_err(|_| {
            AgentError::new(
                ErrorKind::Storage,
                format!(
                    "Pond '{}' has no lineage directory on the node that owns it — nothing has \
                     been recorded for it yet.",
                    info.name
                ),
                "Run a query against the pond first; its events appear once something has \
                 happened in it.",
                LINEAGE_RECIPE,
            )
        })?;

        // The caller's own query may still be in the writer's buffer (a batch
        // is 64 events, and one operation contributes 2), and a get_lineage
        // that cannot see the query the agent just ran is the first thing it
        // will try and the first thing that looks broken. Only THIS pond's
        // writer is flushed: flushing every pond on the node to answer a
        // question about one would put unrelated fsyncs on this call. It goes
        // to the blocking pool for the same reason the emit path does.
        self.flush_pond_lineage(&info.pond_id).await;

        let dir = loc.lineage_dir.clone();
        let (since, before) = (since.map(str::to_string), before.map(str::to_string));
        let page = tokio::task::spawn_blocking(move || {
            latiq_lineage::read_newest(
                std::path::Path::new(&dir),
                latiq_lineage::PageRequest {
                    limit,
                    since: since.as_deref(),
                    before: before.as_deref(),
                },
            )
        })
        .await
        .map_err(|e| AgentError::internal(format!("join: {e}")))?
        .map_err(|e| match e {
            latiq_lineage::ReadError::BadTimestamp { field, value } => AgentError::new(
                ErrorKind::InvalidValue,
                format!("`{field}` is not an RFC-3339 timestamp: '{value}'."),
                "Pass an RFC-3339 instant, e.g. since='2026-08-14T10:00:00Z', or omit it.",
                LINEAGE_RECIPE,
            ),
            latiq_lineage::ReadError::Io(io) => AgentError::of_kind(
                ErrorKind::Storage,
                format!("The pond's lineage directory could not be read: {io}"),
            ),
        })?;

        Ok(LineagePage {
            pond_id: info.pond_id.clone(),
            pond_name: info.name.clone(),
            lineage_dir: loc.lineage_dir,
            events: page.events,
            limit_applied: limit,
            truncated: page.truncated,
            malformed_lines: page.malformed_lines,
            unreadable_files: page.unreadable_files,
        })
    }

    /// Write out ONE pond's buffered events, on the blocking pool (the writer
    /// fsyncs). A pond with no writer on this node has nothing buffered, so
    /// there is nothing to do and nothing to fail.
    async fn flush_pond_lineage(&self, pond_id: &str) {
        let writer = self
            .lineage_writers_read()
            .and_then(|map| map.get(pond_id).cloned());
        if let Some(writer) = writer {
            let _ = tokio::task::spawn_blocking(move || writer.flush()).await;
        }
    }

    /// Emit one access record on the `latiq::access` target (see the `access`
    /// module for the field contract and how operators read it).
    ///
    /// `outcome` is not optional: auditing only successes leaves an operator
    /// with a systematically incomplete picture of agent activity while the
    /// Admin surface — same target, same field names — gives them a complete one
    /// for operators. Every audited op records both outcomes.
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
        outcome: &str,
    ) {
        crate::access::record(
            identity,
            operation,
            pond_id,
            request_summary.as_deref(),
            duration_ms,
            outcome,
        );
    }

    /// Audit a failure and hand the error back, so an error path is a one-liner
    /// rather than a block that is easy to forget to add.
    async fn audit_err(
        &self,
        identity: &Identity,
        operation: &str,
        pond: Option<&str>,
        summary: Option<String>,
        duration_ms: u64,
        e: AgentError,
    ) -> AgentError {
        self.audit(identity, operation, pond, summary, duration_ms, ERROR)
            .await;
        e
    }

    /// `control.pond_info`, recording a FAILED lookup on the access trail. A
    /// pond that does not resolve is the most common way an agent op dies, and
    /// without this it would leave no record at all — an operator would see
    /// only the ops that got far enough to succeed.
    async fn pond_info_audited(
        &self,
        identity: &Identity,
        operation: &str,
        pond_ref: &str,
    ) -> Result<PondInfo, AgentError> {
        match self.control.pond_info(pond_ref).await {
            Ok(info) => Ok(info),
            Err(e) => Err(self
                .audit_err(identity, operation, Some(pond_ref), None, 0, e)
                .await),
        }
    }
}

/// Map a pond's tier name to its resource caps. `None` means "apply nothing" —
/// either the `none` tier, or a tier with no caps — and the engine leaves its
/// own defaults in force.
///
/// The `unwrap_or_default()` is a floor for a row we cannot re-ask about, NOT a
/// validation policy: an unknown tier is refused at creation
/// (`Registry::create_pond`) and at re-tiering (`Registry::set_pond_tier`), so
/// nothing can write one any more. It survives for a row that predates that, and
/// running such a pond at medium is the safe reading — the alternative is a pond
/// that cannot be queried at all. What it must never do again is decide the tier
/// for a name the caller just typed; that is why the check moved to the registry.
fn tier_limits(tier: &str) -> Option<ResourceLimits> {
    PondTier::parse(tier).unwrap_or_default().limits()
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
    // Use the snake_case wire name (not Debug's PascalCase) so the label matches
    // the kind clients/logs see and dashboards can join on it.
    metrics::counter!("latiq_pond_errors_total", "pond" => pond.to_string(), "kind" => e.envelope().kind.as_str())
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
///
/// `op` is the op as the CALLER invoked it — the same name the access trail
/// records — so a spike here can be grepped there. It is emphatically NOT the
/// internal hop the op happens to ride: `read_collected` forwards over the
/// Arrow stream and used to be counted as `read_arrow`, which merged it with
/// the genuinely streaming RPC and left `read_query` looking like it never
/// crossed a node.
///
/// Called at the forward decision and nowhere else. Allocation has no counter:
/// this node does not dial anyone to allocate — it asks the control plane, and
/// the control plane is what reaches the owner. The only allocation-related
/// forward that can happen here is `materialize_pond`, when the control plane's
/// call lands (through a gateway) on a node that does not own the pond, and it
/// is counted under that name because that is the op being relayed.
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
    /// The runtime this blocking producer belongs to, so it can await a send
    /// without a runtime of its own. Captured before `spawn_blocking`.
    rt: tokio::runtime::Handle,
    /// The operation's cancellation token — see [`ChannelSink::send_batch`].
    abort: AbortToken,
}

impl ChannelSink {
    /// Hand one item to the consumer, waking either when it makes room **or**
    /// when the operation is cancelled.
    ///
    /// A plain `blocking_send` on this bounded channel is unrecoverable: the
    /// engine holds a read-only transaction — and therefore a pinned DuckLake
    /// snapshot and a pool connection — open across the whole batch stream, and
    /// `run_with_abort`'s watcher cancels by interrupting DuckDB. A producer
    /// parked in the channel is not in DuckDB, so the interrupt does nothing
    /// and a client that stays connected but stops reading pins that snapshot
    /// against expiry/cleanup for as long as it likes. Selecting on the abort
    /// token is what makes cancellation actually reach a stalled stream, so the
    /// transaction rolls back and the connection is released.
    ///
    /// **Still open, and pre-existing:** nothing imposes an *absolute* deadline
    /// on a slow-but-live consumer. Until one does, a reader that keeps the
    /// stream alive can hold a pool slot for as long as it likes, and with
    /// `(cores*2).clamp(4,32)` such readers per pond later reads wait on the
    /// pool's untimed condvar. What the deadline should be, and whether it is
    /// configurable, is a policy question that has not been decided.
    fn send_batch(&self, item: Result<RecordBatch, AgentError>) -> ControlFlow<()> {
        self.rt.block_on(async {
            tokio::select! {
                // Biased: with capacity free and a cancel racing, delivering the
                // batch we already produced is the better of two valid answers.
                biased;
                sent = self.batch_tx.send(item) => match sent {
                    Ok(()) => ControlFlow::Continue(()),
                    Err(_) => ControlFlow::Break(()), // receiver gone
                },
                _ = self.abort.cancelled() => ControlFlow::Break(()),
            }
        })
    }
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
        self.send_batch(Ok(batch))
    }
}

/// DuckDB type names whose parentheses hold a **type modifier** — a width, a
/// precision, a scale — rather than a value. Digits inside the parens that
/// directly follow one of these are part of the *type*, so redacting them turns
/// `DECIMAL(10,2)` into DDL that no longer says what the column is.
///
/// Verified against DuckDB (every entry here is accepted as `CREATE TABLE
/// x(c <name>(n))`; the ones it rejects — `TIME`, `BLOB`, `DOUBLE`, `TIMESTAMPTZ`,
/// `TIMESTAMP_S/_MS/_NS` — are deliberately absent, since a paren after them is
/// not a type modifier at all).
///
/// **`INTERVAL` is deliberately excluded.** DuckDB parses `INTERVAL (5) DAY` as a
/// value expression (`to_days(5)`), so preserving digits there would be a
/// redaction hole. Losing an interval's precision is the cheaper mistake.
const PARAMETERISED_TYPES: &[&str] = &[
    "DECIMAL",
    "DEC",
    "NUMERIC",
    "VARCHAR",
    "NVARCHAR",
    "CHAR",
    "CHARACTER",
    "VARYING", // the second word of `CHARACTER VARYING(n)`
    "BPCHAR",
    "TEXT",
    "STRING",
    "TIMESTAMP",
    "DATETIME",
    "BIT",
    "BITSTRING",
    "FLOAT",
];

fn is_parameterised_type(word: &str) -> bool {
    PARAMETERISED_TYPES
        .iter()
        .any(|t| word.eq_ignore_ascii_case(t))
}

/// Minimal SQL-shape redaction for the audit log and the lineage `SQLJobFacet`
/// (one redactor, so provenance can never carry a literal the trail hid): drop comments (so literals
/// hidden in them can't leak), then collapse quoted-string and numeric *literals*
/// to `?`. Identifiers that merely contain digits (`t1`, `events2`) are preserved,
/// and so are the numbers inside a **type specification** (`DECIMAL(10,2)`,
/// `VARCHAR(64)`) — those describe a schema, not a value, and a DDL statement
/// whose types were collapsed is neither readable nor re-runnable. Values inside
/// a DDL statement (a CTAS predicate) are still collapsed: the exemption is
/// scoped to one paren directly after a type keyword, never to a statement kind.
/// (A full parser-based redactor is future work.)
pub(crate) fn redact_sql(sql: &str) -> String {
    let decommented = strip_sql_comments(sql);
    let mut out = String::with_capacity(decommented.len());
    let mut chars = decommented.chars().peekable();
    // Whether the previously emitted char was part of an identifier — a digit
    // right after one (e.g. the `1` in `t1`) is part of that identifier, not a
    // numeric literal, so it must not be collapsed.
    let mut prev_ident = false;
    // The identifier run currently being emitted, and whether the one that just
    // ended names a parameterised type (so the next `(` opens type modifiers).
    let mut word = String::new();
    let mut type_word_pending = false;
    // Inside `…(` of a type specification, where digits are the type, not a
    // value. Type modifiers do not nest, so one flag and the next `)` suffice.
    let mut in_type_params = false;
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
                word.clear();
                type_word_pending = false;
            }
            d if d.is_ascii_digit() && !prev_ident && !in_type_params => {
                // Numeric literal: collapse the digit/decimal run.
                while matches!(chars.peek(), Some(n) if n.is_ascii_digit() || *n == '.') {
                    chars.next();
                }
                out.push('?');
                prev_ident = false;
                word.clear();
                type_word_pending = false;
            }
            other => {
                let ident_char = other.is_ascii_alphanumeric() || other == '_';
                prev_ident = ident_char;
                if ident_char {
                    word.push(other);
                } else {
                    if !word.is_empty() {
                        type_word_pending = is_parameterised_type(&word);
                        word.clear();
                    }
                    match other {
                        '(' => {
                            in_type_params = type_word_pending;
                            type_word_pending = false;
                        }
                        ')' => {
                            in_type_params = false;
                            type_word_pending = false;
                        }
                        // Whitespace between the keyword and its `(` is allowed;
                        // anything else ends the type specification.
                        c if !c.is_whitespace() => type_word_pending = false,
                        _ => {}
                    }
                }
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
    fn redaction_keeps_the_width_of_a_type_specification() {
        // Regression pin: `DECIMAL(10,2)` used to record as `DECIMAL(?,?)`, so
        // the DDL in the trail was neither readable as a schema nor re-runnable
        // and the column's type could not be recovered by a reader.
        assert_eq!(
            redact_sql("CREATE TABLE orders(id INTEGER, customer VARCHAR, amount DECIMAL(10,2))"),
            "CREATE TABLE orders(id INTEGER, customer VARCHAR, amount DECIMAL(10,2))"
        );
        // Same for a cast, and for a width written with a space before the paren.
        assert_eq!(
            redact_sql("SELECT CAST(x AS DECIMAL (18, 4)), CAST(y AS VARCHAR(64)) FROM t"),
            "SELECT CAST(x AS DECIMAL (18, 4)), CAST(y AS VARCHAR(64)) FROM t"
        );
    }

    #[test]
    fn redaction_collapses_values_in_a_ddl_with_a_predicate() {
        // The case that makes "skip redaction for DDL" wrong: a CTAS is DDL, but
        // its predicate carries user values that must still be redacted.
        assert_eq!(
            redact_sql(
                "CREATE TABLE big AS SELECT * FROM orders \
                 WHERE amount > 9999 AND customer = 'acme'"
            ),
            "CREATE TABLE big AS SELECT * FROM orders WHERE amount > ? AND customer = ?"
        );
    }

    #[test]
    fn redaction_collapses_inserted_values() {
        assert_eq!(
            redact_sql("INSERT INTO t VALUES (1, 'acme', 100.50)"),
            "INSERT INTO t VALUES (?, ?, ?)"
        );
    }

    #[test]
    fn redaction_collapses_an_interval_parenthesised_value() {
        // `INTERVAL (5) DAY` is a *value* expression in DuckDB, not a type
        // modifier, so INTERVAL is deliberately not on the type-keyword list.
        assert_eq!(
            redact_sql("SELECT * FROM t WHERE d > now() - INTERVAL (9999) DAY"),
            "SELECT * FROM t WHERE d > now() - INTERVAL (?) DAY"
        );
    }

    #[test]
    fn redaction_does_not_treat_a_call_as_a_type_specification() {
        // Only the type keywords open a preserved paren; a function call whose
        // argument happens to be numeric is still a value.
        assert_eq!(
            redact_sql("SELECT count(1), round(amount, 2) FROM t WHERE id = 7"),
            "SELECT count(?), round(amount, ?) FROM t WHERE id = ?"
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

    /// `tier_limits` is the seam between a pond's **persisted tier name** and
    /// the caps the engine applies, and it is assigned at six call sites. It had
    /// no test: a regression collapsing every tier to `medium` would have been
    /// invisible, because the two tests that read `threads` back out of DuckDB
    /// build their `ResourceLimits` by hand and never go through this mapping.
    #[test]
    fn tier_limits_maps_every_persisted_tier_name_to_its_caps() {
        let ladder = [
            PondTier::None,
            PondTier::XSmall,
            PondTier::Small,
            PondTier::Medium,
            PondTier::Large,
            PondTier::XLarge,
        ];
        for t in ladder {
            assert_eq!(
                tier_limits(t.as_str()),
                t.limits(),
                "`{}` must resolve to its own caps, not another tier's",
                t.as_str()
            );
        }
        // Anti-vacuity: the loop above passes trivially if every tier resolved
        // to the same thing, so pin that the tiers really are distinct — this is
        // the "everything fell back to medium" regression.
        let distinct: std::collections::BTreeSet<_> = ladder
            .iter()
            .map(|t| tier_limits(t.as_str()).map(|l| (l.memory_bytes, l.cores)))
            .collect();
        assert_eq!(
            distinct.len(),
            ladder.len(),
            "tiers collapsed onto each other"
        );

        // The uncapped tier is the one that must map to "apply nothing" — a
        // fallback to `medium` here would silently re-cap an operator-granted
        // uncapped pond.
        assert_eq!(tier_limits("none"), None);
        assert_eq!(tier_limits("uncapped"), None, "the documented alias too");

        // Unknown / empty / whitespace fall back to medium, which is documented
        // behaviour: a pond whose tier string we cannot read stays capped rather
        // than becoming uncapped.
        let medium = PondTier::Medium.limits();
        assert!(medium.is_some(), "medium must cap, or the fallback is moot");
        for unknown in ["", "   ", "huge", "unlimited", "MEDIUM-ish"] {
            assert_eq!(
                tier_limits(unknown),
                medium,
                "`{unknown}` must fall back to medium, never to uncapped"
            );
        }
        // Case and padding are the real shapes a persisted/CLI-supplied name
        // takes, and they must reach the tier, not the fallback.
        assert_eq!(tier_limits(" X-LARGE "), PondTier::XLarge.limits());
        assert_ne!(
            tier_limits(" X-LARGE "),
            medium,
            "a recognised tier must not resolve to the fallback"
        );
    }
}
