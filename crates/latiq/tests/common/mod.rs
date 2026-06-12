//! Full-stack test harness: starts a real control-plane (Control + Admin gRPC)
//! and a real pond-node (Data gRPC + MCP, wired through GrpcControlPlane) in
//! one process over loopback, with no port races. This is the ONLY place the
//! two-process path (GrpcControlPlane / build_ops) is exercised.
#![allow(dead_code)]
use latiq_control_plane::admin_service::AdminService;
use latiq_control_plane::control_service::ControlService;
use latiq_control_plane::Registry;
use latiq_mcp::serve_mcp_with_listener;
use latiq_pond_node::{build_ops, DataService};
use latiq_proto::v1::admin_server::AdminServer;
use latiq_proto::v1::control_server::ControlServer;
use latiq_proto::v1::data_server::DataServer;
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

/// Start the control plane (Control + Admin gRPC) and return its endpoints.
async fn start_control_plane() -> (String, String) {
    let registry = Registry::open(None).unwrap();
    let (control_l, control_port) = bind().await;
    let (admin_l, admin_port) = bind().await;

    let r1 = registry.clone();
    tokio::spawn(async move {
        Server::builder()
            .add_service(ControlServer::new(ControlService::new(r1)))
            .serve_with_incoming(TcpListenerStream::new(control_l))
            .await
            .unwrap();
    });
    tokio::spawn(async move {
        Server::builder()
            .add_service(AdminServer::new(AdminService::new(registry)))
            .serve_with_incoming(TcpListenerStream::new(admin_l))
            .await
            .unwrap();
    });
    let control_endpoint = format!("http://127.0.0.1:{control_port}");
    let admin_endpoint = format!("http://127.0.0.1:{admin_port}");
    wait_connectable(&control_endpoint).await;
    (control_endpoint, admin_endpoint)
}

/// Start one pond node (real GrpcControlPlane + forwarding) against `control`.
async fn start_node(node_id: &str, control_endpoint: &str) -> NodeStack {
    let tmp = tempfile::tempdir().unwrap();
    let (mcp_l, mcp_port) = bind().await;
    let (data_l, data_port) = bind().await;
    let mcp_endpoint = format!("http://127.0.0.1:{mcp_port}/mcp");
    let data_endpoint = format!("http://127.0.0.1:{data_port}");
    let internal_endpoint = data_endpoint.clone();

    let ops = build_ops(
        node_id,
        &mcp_endpoint,
        &internal_endpoint,
        control_endpoint,
        tmp.path(),
    )
    .await
    .expect("build pond-node ops");

    let data_ops = ops.clone();
    tokio::spawn(async move {
        Server::builder()
            .add_service(DataServer::new(DataService::new(data_ops)))
            .serve_with_incoming(TcpListenerStream::new(data_l))
            .await
            .unwrap();
    });
    tokio::spawn(async move {
        serve_mcp_with_listener(mcp_l, ops).await.unwrap();
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
