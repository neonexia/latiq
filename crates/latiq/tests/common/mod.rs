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

//! Full-stack test harness: starts a real control-plane (Control + Admin gRPC)
//! and a real pond-node (Data gRPC + MCP, wired through GrpcControlPlane) in
//! one process over loopback, with no port races. This is the ONLY place the
//! two-process path (GrpcControlPlane / build_ops) is exercised.
#![allow(dead_code)]
use latiq_control_plane::admin_service::AdminService;
use latiq_control_plane::control_service::ControlService;
use latiq_control_plane::Registry;
use latiq_mcp::serve_mcp_with_listener;
use latiq_pond_node::{build_ops, DataService, StreamService};
use latiq_proto::v1::admin_server::AdminServer;
use latiq_proto::v1::control_client::ControlClient;
use latiq_proto::v1::control_server::ControlServer;
use latiq_proto::v1::data_server::DataServer;
use latiq_proto::v1::stream_server::StreamServer;
use latiq_proto::v1::RegisterNodeRequest;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Endpoint, Server};

pub struct TestStack {
    pub data_endpoint: String,
    pub admin_endpoint: String,
    pub control_endpoint: String,
    pub mcp_endpoint: String,
    _tmp: tempfile::TempDir,
}

/// One pond node in a multi-node stack. `internal_endpoint == data_endpoint` —
/// it's the address the registry stores and a peer dials to forward.
pub struct NodeStack {
    pub node_id: String,
    pub data_endpoint: String,
    pub mcp_endpoint: String,
    pub internal_endpoint: String,
    _tmp: tempfile::TempDir,
}

impl NodeStack {
    /// This node's `LocalFs` root — the directory a pond materialised HERE
    /// appears under, as `<root>/<pond-id>`. The one way a test can tell "the
    /// owner created the pond" from "somebody said it did".
    pub fn data_dir(&self) -> &std::path::Path {
        self._tmp.path()
    }

    /// Whether this node holds storage for `pond_id` — `LocalFs::pond_exists`
    /// read from the outside, so the assertion does not depend on the code path
    /// under test to answer.
    pub fn holds_pond(&self, pond_id: &str) -> bool {
        self.data_dir().join(pond_id).exists()
    }
}

/// A node the registry knows about that nothing is listening for: registered
/// with a real, then closed, loopback port.
///
/// This is how a test pins WHICH node a pond is placed on. Placement is
/// `ORDER BY random()` over active nodes, so the only deterministic cluster is
/// one with a single node in the pool — and `revive` can then bring that node up
/// at the very address the registry already published, which is "the node came
/// back" without any second placement to be lucky about.
pub struct GhostNode {
    pub node_id: String,
    pub internal_endpoint: String,
    port: u16,
}

/// A control plane plus N pond nodes, all in-process over loopback.
pub struct MultiStack {
    pub control_endpoint: String,
    pub admin_endpoint: String,
    pub nodes: Vec<NodeStack>,
}

impl MultiStack {
    /// A node that does NOT own `owner_endpoint` — a deliberate "wrong" greeter,
    /// to force a forward. Panics if every node is the owner (need >= 2 nodes).
    pub fn other_than(&self, owner_endpoint: &str) -> &NodeStack {
        self.nodes
            .iter()
            .find(|n| n.internal_endpoint != owner_endpoint)
            .expect("need a node other than the owner (start_stack_n(>=2))")
    }
}

async fn bind() -> (TcpListener, u16) {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    (l, port)
}

async fn wait_connectable(endpoint: &str) {
    for _ in 0..100 {
        if let Ok(ep) = Endpoint::from_shared(endpoint.to_string()) {
            if ep.connect().await.is_ok() {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
    panic!("endpoint never became connectable: {endpoint}");
}

/// The Control service exactly as `serve_control_plane` builds it: WITH the
/// outbound materializer, and replaying the caller's bearer token iff this
/// control plane is authenticated.
///
/// The harness must not diverge from the real wiring here. A control plane
/// without a materializer silently reverts every create path to lazy
/// allocation, and the whole eager-allocation suite would go green while
/// proving nothing.
fn control_service(registry: Registry, replay_bearer: bool) -> ControlService {
    ControlService::new(
        registry,
        Some(std::sync::Arc::new(
            latiq_control_plane::node_client::GrpcNodeMaterializer::new(),
        )),
    )
    .replaying_bearer(replay_bearer)
}

/// Start the control plane (Control + Admin gRPC) and return its endpoints.
async fn start_control_plane() -> (String, String) {
    start_control_plane_with_auth(None).await
}

/// Start the control plane, optionally requiring verified bearer tokens on its
/// Admin surface. `start_control_plane` stays auth-free so every pre-existing
/// test keeps exercising the relaxed path.
pub async fn start_control_plane_with_auth(
    auth: Option<latiq_auth::AuthConfig>,
) -> (String, String) {
    let verifier = auth.map(|cfg| {
        std::sync::Arc::new(latiq_auth::Verifier::new(cfg).expect("build test verifier"))
    });
    let registry = Registry::open(None).unwrap();
    let (control_l, control_port) = bind().await;
    let (admin_l, admin_port) = bind().await;
    // Same derivation `serve_control_plane` uses: the well-known path on the
    // address the Admin surface answers at.
    let metadata_url = verifier
        .as_ref()
        .map(|_| format!("http://127.0.0.1:{admin_port}/.well-known/oauth-protected-resource"));

    let r1 = registry.clone();
    let replay = verifier.is_some();
    tokio::spawn(async move {
        Server::builder()
            .add_service(ControlServer::new(control_service(r1, replay)))
            .serve_with_incoming(TcpListenerStream::new(control_l))
            .await
            .unwrap();
    });
    tokio::spawn(async move {
        Server::builder()
            .add_service(AdminServer::new(
                AdminService::new(registry)
                    .with_verifier(verifier)
                    .with_metadata_url(metadata_url.as_deref()),
            ))
            .serve_with_incoming(TcpListenerStream::new(admin_l))
            .await
            .unwrap();
    });
    let control_endpoint = format!("http://127.0.0.1:{control_port}");
    let admin_endpoint = format!("http://127.0.0.1:{admin_port}");
    wait_connectable(&control_endpoint).await;
    (control_endpoint, admin_endpoint)
}

/// A control plane serving Control + Admin on ONE port, the shape
/// `serve_control_plane` (and therefore `latiq serve`) really has. The two-port
/// variant above exists so a test can drive them independently; the CLI cannot,
/// because it has a single `$LATIQ_SERVER`. Returns that one endpoint.
pub async fn start_control_plane_one_port(auth: Option<latiq_auth::AuthConfig>) -> String {
    let verifier = auth.map(|cfg| {
        std::sync::Arc::new(latiq_auth::Verifier::new(cfg).expect("build test verifier"))
    });
    let registry = Registry::open(None).unwrap();
    let (listener, port) = bind().await;
    let metadata_url = verifier
        .as_ref()
        .map(|_| format!("http://127.0.0.1:{port}/.well-known/oauth-protected-resource"));

    let r1 = registry.clone();
    let replay = verifier.is_some();
    tokio::spawn(async move {
        Server::builder()
            .add_service(ControlServer::new(control_service(r1, replay)))
            .add_service(AdminServer::new(
                AdminService::new(registry)
                    .with_verifier(verifier)
                    .with_metadata_url(metadata_url.as_deref()),
            ))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    let endpoint = format!("http://127.0.0.1:{port}");
    wait_connectable(&endpoint).await;
    endpoint
}

/// Start one pond node (real GrpcControlPlane + forwarding) against `control`.
async fn start_node(node_id: &str, control_endpoint: &str) -> NodeStack {
    start_node_with_auth(node_id, control_endpoint, None).await
}

/// The control plane on its own, with NO pond nodes. Returns the (control,
/// admin) endpoints.
///
/// Paired with `add_node`, this is what lets a test build a cluster node by
/// node: placement is `ORDER BY random()`, so the only reliable way to pin which
/// node owns a pond is to allocate while just one node exists and then add the
/// peer that will forward. That in turn is what makes ASYMMETRIC clusters
/// testable — a greeter and an owner that trust different issuers, or only one
/// of which requires a token at all.
pub async fn start_control_plane_only() -> (String, String) {
    start_control_plane().await
}

/// Add a pond node to an already-running control plane, with its own auth config.
pub async fn add_node(
    node_id: &str,
    control_endpoint: &str,
    auth: Option<latiq_auth::AuthConfig>,
) -> NodeStack {
    start_node_with_auth(node_id, control_endpoint, auth).await
}

/// Start one pond node, optionally requiring verified bearer tokens on its Data,
/// Stream and MCP surfaces.
async fn start_node_with_auth(
    node_id: &str,
    control_endpoint: &str,
    auth: Option<latiq_auth::AuthConfig>,
) -> NodeStack {
    start_node_inner(node_id, control_endpoint, auth, true, None).await
}

/// A pond node that serves normally but is left OUT of the control plane's
/// placement pool (it never registers). Everything else about it — the same
/// `AgentOps`, the same forwarder, the same surfaces — is what `build_ops`
/// builds; the registration it skips only decides whether the control plane may
/// place ponds on it.
///
/// It exists so a test can say which node owns a pond without racing
/// `ORDER BY random()`: with exactly one registered node, every allocation this
/// greeter takes is placed on that node, and every one of them must be
/// materialised over the wire.
pub async fn add_greeter_node(node_id: &str, control_endpoint: &str) -> NodeStack {
    start_node_inner(node_id, control_endpoint, None, false, None).await
}

/// `add_greeter_node`, requiring a verified bearer token on its surfaces.
pub async fn add_greeter_node_with_auth(
    node_id: &str,
    control_endpoint: &str,
    auth: latiq_auth::AuthConfig,
) -> NodeStack {
    start_node_inner(node_id, control_endpoint, Some(auth), false, None).await
}

async fn start_node_inner(
    node_id: &str,
    control_endpoint: &str,
    auth: Option<latiq_auth::AuthConfig>,
    register: bool,
    data_listener: Option<TcpListener>,
) -> NodeStack {
    let verifier = auth.map(|cfg| {
        std::sync::Arc::new(latiq_auth::Verifier::new(cfg).expect("build test verifier"))
    });
    let tmp = tempfile::tempdir().unwrap();
    let (mcp_l, mcp_port) = bind().await;
    let (data_l, data_port) = match data_listener {
        Some(l) => {
            let port = l.local_addr().unwrap().port();
            (l, port)
        }
        None => bind().await,
    };
    let mcp_endpoint = format!("http://127.0.0.1:{mcp_port}/mcp");
    let data_endpoint = format!("http://127.0.0.1:{data_port}");
    let internal_endpoint = data_endpoint.clone();

    let mcp_verifier = verifier.clone();
    // Same derivation `run_pond_node` uses: the RFC 9728 document is served by
    // this node's MCP surface, so the Data/Stream challenge points there.
    let metadata_url = verifier
        .as_ref()
        .map(|_| format!("http://127.0.0.1:{mcp_port}/.well-known/oauth-protected-resource"));
    let ops = if register {
        build_ops(
            node_id,
            &mcp_endpoint,
            &internal_endpoint,
            control_endpoint,
            tmp.path(),
            // No lineage backend in the harness: the sink is covered where it
            // can be observed (`latiq-agent-core/tests/agent_ops.rs`), and a
            // full-stack node posting to nowhere would prove nothing this does
            // not.
            None,
        )
        .await
        .expect("build pond-node ops")
    } else {
        // `build_ops` minus its one RegisterNode call. Deliberately assembled
        // from the same public pieces in the same order, so a node built here
        // and one built there differ in the registration and nothing else.
        std::sync::Arc::new(
            latiq_agent_core::AgentOps::new(
                std::sync::Arc::new(
                    latiq_pond_node::GrpcControlPlane::connect(control_endpoint.to_string())
                        .await
                        .expect("connect control plane"),
                ),
                std::sync::Arc::new(latiq_storage::LocalFs::new(tmp.path())),
                std::sync::Arc::new(latiq_engine_duckdb::DuckEngine::new()),
                latiq_agent_core::AgentConfig::default(),
            )
            .with_forwarding(
                node_id.to_string(),
                internal_endpoint.clone(),
                std::sync::Arc::new(latiq_pond_node::GrpcForwarder::new()),
            ),
        )
    };

    let data_ops = ops.clone();
    tokio::spawn(async move {
        Server::builder()
            .add_service(DataServer::new(
                DataService::new(data_ops.clone())
                    .with_verifier(verifier.clone())
                    .with_metadata_url(metadata_url.as_deref()),
            ))
            .add_service(StreamServer::new(
                StreamService::new(data_ops)
                    .with_verifier(verifier)
                    .with_metadata_url(metadata_url.as_deref()),
            ))
            .serve_with_incoming(TcpListenerStream::new(data_l))
            .await
            .unwrap();
    });
    tokio::spawn(async move {
        serve_mcp_with_listener(mcp_l, ops, mcp_verifier, None)
            .await
            .unwrap();
    });

    wait_connectable(&data_endpoint).await;
    NodeStack {
        node_id: node_id.to_string(),
        data_endpoint,
        mcp_endpoint,
        internal_endpoint,
        _tmp: tmp,
    }
}

impl GhostNode {
    /// Bring the ghost up for real, at the address the registry already
    /// published for it and under the same node id — so the pond the registry
    /// placed on it is now on a node that answers.
    pub async fn revive(&self, control_endpoint: &str) -> NodeStack {
        // The port was ours, closed, and is claimed again here: a `bind` that
        // fails means someone else took it in between, which we want to hear
        // about loudly rather than as a mystery connection error later.
        let listener = TcpListener::bind(("127.0.0.1", self.port))
            .await
            .unwrap_or_else(|e| panic!("ghost port {} could not be reclaimed: {e}", self.port));
        let node =
            start_node_inner(&self.node_id, control_endpoint, None, true, Some(listener)).await;
        assert_eq!(
            node.internal_endpoint, self.internal_endpoint,
            "the revived node must answer at the address the registry already has"
        );
        node
    }
}

/// Register a node with the control plane that nothing is listening for. The
/// port is bound to learn a free one and then released, so a peer dialling it
/// gets a refusal now — and `GhostNode::revive` can take it back later.
pub async fn register_ghost_node(control_endpoint: &str, node_id: &str) -> GhostNode {
    let (listener, port) = bind().await;
    drop(listener);
    let internal_endpoint = format!("http://127.0.0.1:{port}");
    let mut c = ControlClient::connect(control_endpoint.to_string())
        .await
        .unwrap();
    c.register_node(RegisterNodeRequest {
        node_id: node_id.to_string(),
        mcp_endpoint: format!("http://127.0.0.1:{port}/mcp"),
        internal_endpoint: internal_endpoint.clone(),
        capacity: 100,
    })
    .await
    .expect("register the ghost node");
    GhostNode {
        node_id: node_id.to_string(),
        internal_endpoint,
        port,
    }
}

/// Start the full stack with a single pond node (the common case).
pub async fn start_stack() -> TestStack {
    let (control_endpoint, admin_endpoint) = start_control_plane().await;
    let node = start_node("node-test", &control_endpoint).await;
    TestStack {
        data_endpoint: node.data_endpoint.clone(),
        admin_endpoint,
        control_endpoint,
        mcp_endpoint: node.mcp_endpoint.clone(),
        _tmp: node._tmp,
    }
}

/// Start the full stack with a single pond node whose Data + Stream surfaces
/// require a verified bearer token. `start_stack` stays auth-free so every
/// pre-existing test keeps exercising the relaxed path.
pub async fn start_stack_with_auth(auth: latiq_auth::AuthConfig) -> TestStack {
    // The control plane is authenticated too, because a real one is: it is what
    // calls the node's `MaterializePond` to create a pond, and only an
    // authenticated control plane replays the caller's token on that hop. A
    // relaxed control plane here would make every allocation on this stack fail
    // at the node with `Unauthenticated`.
    let (control_endpoint, admin_endpoint) =
        start_control_plane_with_auth(Some(auth.clone())).await;
    let node = start_node_with_auth("node-test", &control_endpoint, Some(auth)).await;
    TestStack {
        data_endpoint: node.data_endpoint.clone(),
        admin_endpoint,
        control_endpoint,
        mcp_endpoint: node.mcp_endpoint.clone(),
        _tmp: node._tmp,
    }
}

/// The whole stack authenticated the way a deployment really is: Control +
/// Admin on ONE port behind one verifier, plus a pond node behind the same one.
/// `control_endpoint == admin_endpoint` here, which is what an SDK or CLI client
/// sees — it has a single server address and cannot aim Admin somewhere else.
/// The two-port `start_stack_with_auth` leaves the control plane relaxed, so an
/// Admin call from a client would land on the Control port and fail as
/// `Unimplemented` rather than `Unauthenticated`: it cannot see a client that
/// forgets its token on the Admin channel.
pub async fn start_stack_one_port_with_auth(auth: latiq_auth::AuthConfig) -> TestStack {
    let endpoint = start_control_plane_one_port(Some(auth.clone())).await;
    let node = start_node_with_auth("node-test", &endpoint, Some(auth)).await;
    TestStack {
        data_endpoint: node.data_endpoint.clone(),
        admin_endpoint: endpoint.clone(),
        control_endpoint: endpoint,
        mcp_endpoint: node.mcp_endpoint.clone(),
        _tmp: node._tmp,
    }
}

/// `n` pond nodes, all requiring the same verified bearer token — the shape a
/// real cluster has, and what makes the forward hop's own auth testable.
pub async fn start_stack_n_with_auth(n: usize, auth: latiq_auth::AuthConfig) -> MultiStack {
    // Authenticated control plane: see `start_stack_with_auth`.
    let (control_endpoint, admin_endpoint) =
        start_control_plane_with_auth(Some(auth.clone())).await;
    let mut nodes = Vec::with_capacity(n);
    for i in 0..n {
        nodes.push(
            start_node_with_auth(&format!("node-{i}"), &control_endpoint, Some(auth.clone())).await,
        );
    }
    MultiStack {
        control_endpoint,
        admin_endpoint,
        nodes,
    }
}

/// Start the full stack with `n` pond nodes registered with one control plane.
/// Used to exercise node-to-node forwarding: create a pond, learn its owner, then
/// deliberately drive a different node.
pub async fn start_stack_n(n: usize) -> MultiStack {
    let (control_endpoint, admin_endpoint) = start_control_plane().await;
    let mut nodes = Vec::with_capacity(n);
    for i in 0..n {
        nodes.push(start_node(&format!("node-{i}"), &control_endpoint).await);
    }
    MultiStack {
        control_endpoint,
        admin_endpoint,
        nodes,
    }
}
