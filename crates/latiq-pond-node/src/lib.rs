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

//! latiq-pond-node — wires the agent (MCP) + data (gRPC) inbound surfaces to one
//! `AgentOps` (engine + storage + gRPC ControlPlane client); registers with the
//! control plane and heartbeats.
pub mod data_service;
pub mod forward_client;
pub mod grpc_control;
pub mod stream_service;
pub mod wire;

pub use data_service::DataService;
pub use forward_client::GrpcForwarder;
pub use grpc_control::GrpcControlPlane;
pub use stream_service::StreamService;

use latiq_agent_core::{AgentConfig, AgentOps};
use latiq_auth::Verifier;
use latiq_common::QueryTimeouts;
use latiq_engine_duckdb::DuckEngine;
use latiq_mcp::serve_mcp;
use latiq_proto::v1::control_client::ControlClient;
use latiq_proto::v1::data_server::DataServer;
use latiq_proto::v1::stream_server::StreamServer;
use latiq_proto::v1::{HeartbeatRequest, RegisterNodeRequest};
use latiq_storage::LocalFs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Server;

/// Everything one pond node needs to come up. The three address fields are
/// distinct on purpose and easy to confuse: `mcp_addr`/`data_addr` are what this
/// process binds, `internal_endpoint` is what peer nodes dial, and
/// `public_mcp_url` is what clients dial.
pub struct PondNodeConfig {
    pub node_id: String,
    /// Address to serve the agent MCP surface on.
    pub mcp_addr: SocketAddr,
    /// Address to serve the Data/Query gRPC surface on (CLI/SDK).
    pub data_addr: SocketAddr,
    /// Internal endpoint advertised to the control plane (single-node routing).
    pub internal_endpoint: String,
    /// The public URL agents use to reach this node's MCP endpoint, e.g.
    /// `https://latiq.example.com/mcp`. Published as the RFC 9728 `resource`
    /// identifier and in the 401 challenge, so it MUST be the address clients
    /// actually dial -- behind a gateway that is the gateway's URL, not this
    /// node's. Distinct from `internal_endpoint` (`--advertise-addr`), which is
    /// the internal address peer nodes use to forward pond requests. `None`
    /// derives it from `internal_endpoint`, which is correct only when clients
    /// reach nodes directly.
    pub public_mcp_url: Option<String>,
    /// Control-plane Control gRPC endpoint, e.g. `http://127.0.0.1:9090`.
    pub control_endpoint: String,
    /// Local-FS root for pond storage.
    pub data_dir: PathBuf,
    /// Address to serve Prometheus `/metrics` on (None = metrics off).
    pub metrics_addr: Option<SocketAddr>,
    /// When set, every surface on this node requires a verified bearer token.
    /// `None` keeps the relaxed identity of the embedded and dev paths.
    pub auth: Option<latiq_auth::AuthConfig>,
    /// An OpenLineage-compatible receiver to ALSO post events to, e.g.
    /// `http://marquez:5000/api/v1/lineage`. The full endpoint, not a base URL.
    ///
    /// Purely additive and off by default: lineage-enabled ponds always write
    /// their own files, and this is what makes those events outlive the pond
    /// that produced them (dropping a pond destroys its local trail). A backend
    /// that is down, slow or dead can never fail, slow or block a query.
    pub lineage_backend_url: Option<String>,
    /// How long a statement may run on this node: the default applied when a
    /// caller names no `timeout_ms`, and the hard maximum every request is
    /// clamped to.
    ///
    /// The maximum is the OPERATOR's protection, not the agent's: one DuckDB
    /// instance per pond means one unbounded query pins that pond for every
    /// other agent in it. A request above it is clamped rather than refused —
    /// the query still runs, at the ceiling — and the response's
    /// `_meta.timeout_ms` reports what was applied so the clamp is visible.
    pub timeouts: QueryTimeouts,
}

/// Install the standard + optional DuckDB extensions into the local cache so a
/// node can run offline. Used to **bake** extensions into the container image at
/// build time (`latiq warm-extensions`): INSTALL hits the network here, once, so
/// the runtime node never needs it. Errors if a required extension can't install.
pub fn warm_extensions() -> Result<(), Box<dyn std::error::Error>> {
    latiq_engine_duckdb::ensure_standard_extensions()?;
    latiq_engine_duckdb::warm_optional_extensions();
    Ok(())
}

/// Periodically refresh this node's resource + load gauges. The per-pond
/// in-flight gauge is maintained inline in AgentOps; this samples the node-level
/// gauges + process CPU/memory.
/// `sink` is the node's optional lineage backend, if one is configured: its
/// POSTs happen on a task nobody awaits, so these three gauges are the only way
/// an operator can answer "is anything actually leaving this node?" -- and, more
/// to the point, "is the backend keeping up?", which `..._dropped` climbing is
/// the only signal of.
pub fn spawn_node_collector(ops: Arc<AgentOps>, sink: Option<Arc<latiq_lineage::HttpSink>>) {
    tokio::spawn(async move {
        let mut sampler = latiq_metrics::ProcessSampler::new();
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            latiq_metrics::record_process_gauges(&mut sampler);
            metrics::gauge!("latiq_inflight_queries").set(ops.inflight().len() as f64);
            metrics::gauge!("latiq_node_open_ponds").set(ops.open_pond_count() as f64);
            if let Some(sink) = sink.as_deref() {
                metrics::gauge!("latiq_lineage_sink_events_submitted").set(sink.submitted() as f64);
                metrics::gauge!("latiq_lineage_sink_events_posted").set(sink.posted() as f64);
                metrics::gauge!("latiq_lineage_sink_events_dropped").set(sink.dropped() as f64);
            }
        }
    });
}

/// Build the pond node's `AgentOps` (gRPC control + local storage + DuckDB
/// engine) and register the node with the control plane.
pub async fn build_ops(
    node_id: &str,
    mcp_endpoint: &str,
    internal_endpoint: &str,
    control_endpoint: &str,
    data_dir: &std::path::Path,
    lineage_sink: Option<Arc<dyn latiq_lineage::EventSink>>,
    timeouts: QueryTimeouts,
) -> anyhow::Result<Arc<AgentOps>> {
    let mut reg = ControlClient::connect(control_endpoint.to_string()).await?;
    reg.register_node(RegisterNodeRequest {
        node_id: node_id.to_string(),
        mcp_endpoint: mcp_endpoint.to_string(),
        internal_endpoint: internal_endpoint.to_string(),
        capacity: 100,
    })
    .await?;

    let control = Arc::new(GrpcControlPlane::connect(control_endpoint.to_string()).await?);
    let storage = Arc::new(LocalFs::new(data_dir));
    let engine = Arc::new(DuckEngine::new());
    // Forward requests for ponds this node doesn't own to the owning node. The
    // identity it compares against is `node_id` — the SAME id it just registered
    // with above, so the registry's `ponds.node_id` and this value are two reads
    // of one thing. The endpoint comes along only as the address peers dial and
    // as this node's `served_by`; it is never compared (#89: two spellings of
    // one address made a node forward into itself, unboundedly).
    let mut ops = AgentOps::new(
        control,
        storage,
        engine,
        AgentConfig {
            timeouts,
            ..AgentConfig::default()
        },
    )
    .with_forwarding(
        node_id.to_string(),
        internal_endpoint.to_string(),
        Arc::new(GrpcForwarder::new()),
    );
    // `None` = no backend configured, which is the default: lineage-enabled
    // ponds still write their own files, and nothing is posted anywhere.
    if let Some(sink) = lineage_sink {
        ops = ops.with_lineage_sink(sink);
    }
    Ok(Arc::new(ops))
}

/// Serve the Data/Query gRPC surface on `addr`. `verifier` is built once at
/// startup and shared — never per request.
///
/// `metadata_url` is the RFC 9728 protected-resource document advertised in the
/// `WWW-Authenticate` challenge on a rejection. On a pond node that document is
/// served by the MCP surface, so the URL points there (see `run_pond_node`);
/// `None` means no challenge is attached.
pub async fn serve_data(
    addr: SocketAddr,
    ops: Arc<AgentOps>,
    verifier: Option<Arc<Verifier>>,
    metadata_url: Option<String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Data (unary JSON, CLI/SDK) + Stream (server-streaming Arrow, SDK + the
    // node-to-node read forward) share one port — both plain tonic. Both get the
    // same verifier: a surface left unauthenticated is the whole node's auth.
    Server::builder()
        .add_service(DataServer::new(
            DataService::new(ops.clone())
                .with_verifier(verifier.clone())
                .with_metadata_url(metadata_url.as_deref()),
        ))
        .add_service(StreamServer::new(
            StreamService::new(ops)
                .with_verifier(verifier)
                .with_metadata_url(metadata_url.as_deref()),
        ))
        .serve(addr)
        .await?;
    Ok(())
}

/// Run a pond node: register with the control plane, start a heartbeat loop, and
/// serve the agent MCP surface + the Data/Query gRPC surface (blocks).
///
/// **Serves until the process ends.** This is the entry point for EMBEDDED
/// callers -- `latiq-sdk`'s `LocalCluster` and, through it, the Python wheel --
/// so it must not install a signal handler: a library that replaces a host
/// process's `SIGTERM` disposition can stop `kill` from terminating that
/// process, and would let a signal aimed at the host silently tear down the
/// embedded node while the host runs on. The binary drives shutdown explicitly
/// through [`run_pond_node_until`].
pub async fn run_pond_node(cfg: PondNodeConfig) -> anyhow::Result<()> {
    // `pending()` never resolves, so the `select!` inside can only be decided
    // by the server future -- exactly the behaviour this has always had.
    run_pond_node_until(cfg, std::future::pending::<()>()).await
}

/// [`run_pond_node`], stopping when `shutdown` resolves.
///
/// The seam the `latiq` binary uses to hand in SIGTERM/Ctrl-C. It lives here
/// rather than in this crate's own signal handling on purpose: **whoever owns
/// the process owns its signals**, and that is the binary, never a library a
/// host embedded.
///
/// On shutdown it flushes buffered lineage under a bound -- see
/// [`shutdown_lineage`], which is explicit that this is not a graceful drain.
pub async fn run_pond_node_until(
    cfg: PondNodeConfig,
    shutdown: impl std::future::Future<Output = ()> + Send,
) -> anyhow::Result<()> {
    // Latiq is built on the ducklake + httpfs extensions — ensure they load BEFORE
    // we register or serve, so a node that can't function never joins the cluster.
    // Fail fast with a clear error rather than degrading per pond.
    tokio::task::spawn_blocking(latiq_engine_duckdb::ensure_standard_extensions)
        .await
        .map_err(|e| anyhow::anyhow!("extension check failed to run: {e}"))?
        .map_err(|e| {
            anyhow::anyhow!(
                "Latiq requires the ducklake and httpfs DuckDB extensions but could \
                 not load them (bake them into the deployment image): {e}"
            )
        })?;

    // Built ONCE, before anything serves: a bad auth config must stop the node
    // loudly rather than let it come up accepting unauthenticated callers.
    let verifier = match cfg.auth.clone() {
        Some(auth) => Some(Arc::new(Verifier::new(auth).map_err(|e| {
            anyhow::anyhow!("auth is configured but the verifier could not be built: {e}")
        })?)),
        None => None,
    };

    // Resolved BEFORE the node registers or serves, like the verifier: a bad
    // public URL means every client's discovery fails with an error that points
    // nowhere near this setting, so it must stop the node loudly instead.
    let mcp_public_url = latiq_mcp::resolve_public_mcp_url(
        cfg.public_mcp_url.as_deref(),
        &cfg.internal_endpoint,
        cfg.mcp_addr,
    )
    .map_err(|e| anyhow::anyhow!(e))?;

    // Built BEFORE the node registers or serves, like the verifier and the
    // public URL: a malformed backend URL must stop the node loudly, not become
    // a warning on every query for the life of the process. `None` = no backend
    // configured, the default.
    //
    // Kept as the concrete type as well as the neutral one: the collector
    // publishes its counters, while `AgentOps` must only ever see `EventSink`.
    let lineage_sink: Option<Arc<latiq_lineage::HttpSink>> =
        match cfg.lineage_backend_url.as_deref() {
            Some(url) => Some(Arc::new(
                latiq_lineage::HttpSink::new(url).map_err(|e| anyhow::anyhow!("{e}"))?,
            )),
            None => None,
        };

    let mcp_endpoint = format!("http://{}/mcp", cfg.mcp_addr);
    let ops = build_ops(
        &cfg.node_id,
        &mcp_endpoint,
        &cfg.internal_endpoint,
        &cfg.control_endpoint,
        &cfg.data_dir,
        lineage_sink
            .clone()
            .map(|s| s as Arc<dyn latiq_lineage::EventSink>),
        cfg.timeouts,
    )
    .await?;

    // Warm the OPTIONAL extension cache once, in the background — the dev stand-in
    // for image-baking (a no-op when the image is pre-baked). Best-effort: keeps
    // per-pond LOADs download-free without blocking serving or the create path.
    tokio::task::spawn_blocking(|| {
        tracing::info!("warming optional DuckDB extension cache");
        latiq_engine_duckdb::warm_optional_extensions();
    });

    // Prometheus /metrics + the gauge collector (if a metrics port is configured).
    if let Some(metrics_addr) = cfg.metrics_addr {
        let handle = latiq_metrics::init_recorder();
        metrics::gauge!("latiq_build_info", "version" => env!("CARGO_PKG_VERSION")).set(1.0);
        spawn_node_collector(ops.clone(), lineage_sink.clone());
        tokio::spawn(async move {
            if let Err(e) = latiq_metrics::serve_metrics(metrics_addr, handle).await {
                eprintln!("metrics server error: {e}");
            }
        });
    }

    // Heartbeat loop (reconnects on failure so a control-plane blip doesn't
    // silently drop the node from the cluster).
    let hb_endpoint = cfg.control_endpoint.clone();
    let hb_node = cfg.node_id.clone();
    tokio::spawn(async move {
        let mut client: Option<ControlClient<tonic::transport::Channel>> = None;
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;
            if client.is_none() {
                client = ControlClient::connect(hb_endpoint.clone()).await.ok();
            }
            if let Some(c) = client.as_mut() {
                if c.heartbeat(HeartbeatRequest {
                    node_id: hb_node.clone(),
                    pond_count: 0,
                })
                .await
                .is_err()
                {
                    client = None; // drop + reconnect next tick
                }
            }
        }
    });

    // Serve both surfaces concurrently.
    let data_ops = ops.clone();
    let data_addr = cfg.data_addr;
    let data_verifier = verifier.clone();
    // ASSUMPTION: the RFC 9728 document lives on this node's MCP surface, so the
    // Data/Stream challenge points there. Both surfaces derive it from ONE
    // string — `mcp_public_url`, the URL clients actually dial (see
    // `resolve_public_mcp_url`) — never the socket we bound: every compose file
    // we ship binds 0.0.0.0, and two challenges naming different documents would
    // be worse than one wrong one.
    // `None` when no verifier is configured: a node that never opted into auth
    // must not advertise an authorization server.
    let data_metadata_url = verifier
        .as_ref()
        .map(|_| latiq_mcp::protected_resource_metadata_url(&mcp_public_url));
    tokio::spawn(async move {
        if let Err(e) = serve_data(data_addr, data_ops, data_verifier, data_metadata_url).await {
            eprintln!("data gRPC server error: {e}");
        }
    });

    println!(
        "pond-node '{}' serving MCP at {mcp_endpoint} and Data gRPC at http://{}",
        cfg.node_id, cfg.data_addr
    );
    println!("  pond storage: {}", cfg.data_dir.display());
    if let Some(url) = cfg.lineage_backend_url.as_deref() {
        println!("  lineage:      also posting events to {url}");
    }
    // The MCP surface gets the SAME verifier as Data/Stream (a surface left
    // unauthenticated is the whole node's auth) and the SAME advertised URL, so
    // the two challenges can never point at different documents.
    let served = ops.clone();
    let result = tokio::select! {
        r = serve_mcp(cfg.mcp_addr, ops, verifier, Some(mcp_public_url)) => {
            r.map_err(|e| anyhow::anyhow!("mcp server error: {e}"))
        }
        _ = shutdown => {
            tracing::info!("shutdown requested; flushing buffered lineage");
            Ok(())
        }
    };

    shutdown_lineage(served).await;
    result
}

/// The most the node will spend on its shutdown work before exiting anyway.
///
/// Bounded because everything in it can stall: the file flush fsyncs (an NFS
/// mount or an EIO makes that unbounded) and shares a blocking pool with batch
/// writes, extension warming and `getaddrinfo`, while the sink drain is waiting
/// on a network. An operator's escape from a node that will not die used to be
/// a second Ctrl-C -- which is gone the moment anything installs a signal
/// handler -- so the bound has to come from us. **Losing some events is
/// strictly better than a node that will not die.**
///
/// It is a CEILING, not a cost: a node with no backend configured drains
/// instantly and a healthy one nearly so, so this is only ever spent when
/// something is already stuck. That is what makes it affordable to exceed
/// `latiq_lineage`'s `POST_TIMEOUT`, which it must: the drain cannot outrun a
/// POST that is already in flight, so a budget below that ceiling would mean
/// one hung backend at shutdown discards the entire backlog -- the exact case
/// the drain exists for. The `const` assertion below keeps the two composed.
/// The upper bound is the orchestrator's termination grace period (30s by
/// default on k8s), which SIGKILLs the node regardless.
const SHUTDOWN_BUDGET: Duration = Duration::from_secs(15);

/// The relationship above, enforced rather than described.
const _: () = assert!(
    SHUTDOWN_BUDGET.as_secs() > latiq_lineage::sink::POST_TIMEOUT.as_secs(),
    "SHUTDOWN_BUDGET must exceed the sink's POST_TIMEOUT, or the drain cannot outlast one \
     in-flight POST and a hung backend loses the whole backlog"
);

/// Land what lineage is buffered, then go. **This is not a graceful drain**, and
/// the difference matters: the Data/Stream gRPC server is a separate spawned
/// task that keeps accepting, and in-flight MCP requests are cancelled with the
/// server future rather than allowed to finish. What this promises is narrower
/// and worth exactly its cost -- everything already buffered at signal time
/// lands. A query that completes after the flush records events that are lost,
/// as it would have been before this existed.
///
/// Two halves, in this order and for a reason:
///
/// 1. `flush_lineage` writes each pond's buffered batch to its own files. It
///    does BLOCKING, fsyncing io, so it runs on the blocking pool. `Drop`
///    already covers a teardown that drops the last `AgentOps` -- but SIGTERM
///    drops nothing, it ends the process, and up to one batch per pond (64
///    events) would go with it. That batch is the last few queries before the
///    node went down, which is precisely the window an incident asks about.
/// 2. `drain_lineage_sink` posts what the optional HTTP backend still has
///    queued -- up to 1024 events, i.e. everything that piled up while the
///    backend was down. Without it the shutdown flush would save the local
///    copy of exactly the events the sink exists to get OFF the node.
async fn shutdown_lineage(ops: Arc<AgentOps>) {
    let deadline = tokio::time::Instant::now() + SHUTDOWN_BUDGET;
    let flushing = ops.clone();
    if tokio::time::timeout_at(
        deadline,
        tokio::task::spawn_blocking(move || flushing.flush_lineage()),
    )
    .await
    .is_err()
    {
        tracing::warn!(
            budget_ms = SHUTDOWN_BUDGET.as_millis() as u64,
            "lineage files did not finish flushing before the shutdown budget ran out; exiting \
             anyway with some events unwritten"
        );
        return;
    }
    // Whatever is left of the budget. The sink bounds itself with it and warns
    // about anything it could not post, so there is nothing to log here.
    ops.drain_lineage_sink(deadline.saturating_duration_since(tokio::time::Instant::now()))
        .await;
}

#[cfg(test)]
mod tests {
    /// The embedded path must install NO signal handler.
    ///
    /// `run_pond_node` is not only the binary's node path -- `latiq-sdk`'s
    /// `LocalCluster::start`, and therefore the Python wheel, calls it. A
    /// library that installs a process-wide handler replaces its host's
    /// `SIG_DFL` disposition without emulating it, so `kill <pid>` on a Python
    /// process that merely embedded a pond can stop terminating it, and a
    /// signal aimed at the host silently tears down the embedded node while the
    /// host runs on. The handler therefore lives in `crates/latiq/src/main.rs`.
    ///
    /// A source guard and not a runtime one, stated plainly: proving it at
    /// runtime means reading the process's `sigaction` disposition before and
    /// after starting an embedded node, which needs a live control plane, a
    /// bound port and `libc` -- for a property whose only failure mode is
    /// someone reintroducing these three tokens into this crate. The
    /// counter-assertions below are what stop it passing vacuously.
    #[test]
    fn the_embedded_node_path_installs_no_signal_handler() {
        // Only the NON-test half: this module names the forbidden tokens as
        // string literals, and a guard that searches itself always matches.
        let src = include_str!("lib.rs")
            .split_once("#[cfg(test)]")
            .expect("this file has a #[cfg(test)] module, and this test is in it")
            .0;
        for token in ["tokio::signal", "SignalKind", "ctrl_c"] {
            assert!(
                !src.contains(token),
                "`{token}` is back in latiq-pond-node: signal handling belongs to the binary                  (crates/latiq/src/main.rs), because this crate is also what the embedded SDK                  and the Python wheel run in a host process"
            );
        }
        // Anti-vacuity, both directions. The seam must exist (or the guard is
        // trivially satisfied by a crate that never shuts down at all), and the
        // haystack must be the real file.
        assert!(
            src.contains("pub async fn run_pond_node_until("),
            "the shutdown seam the binary drives is gone; without it the handler has nowhere              to live but here"
        );
        assert!(
            src.contains("async fn shutdown_lineage("),
            "the shutdown work itself is gone, so this guard is protecting nothing"
        );
    }
}
