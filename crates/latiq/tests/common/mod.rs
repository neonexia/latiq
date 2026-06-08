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

/// Start the full stack and return its endpoints.
pub async fn start_stack() -> TestStack {
    // --- control plane ---
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
    let r2 = registry.clone();
    tokio::spawn(async move {
        Server::builder()
            .add_service(AdminServer::new(AdminService::new(r2)))
            .serve_with_incoming(TcpListenerStream::new(admin_l))
            .await
            .unwrap();
    });
    let control_endpoint = format!("http://127.0.0.1:{control_port}");
    let admin_endpoint = format!("http://127.0.0.1:{admin_port}");
    wait_connectable(&control_endpoint).await;

    // --- pond node (real GrpcControlPlane path) ---
    let tmp = tempfile::tempdir().unwrap();
    let (mcp_l, mcp_port) = bind().await;
    let (data_l, data_port) = bind().await;
    let mcp_endpoint = format!("http://127.0.0.1:{mcp_port}/mcp");
    let data_endpoint = format!("http://127.0.0.1:{data_port}");
    let internal_endpoint = format!("http://127.0.0.1:{data_port}");

    let ops = build_ops(
        "node-test",
        &mcp_endpoint,
        &internal_endpoint,
        &control_endpoint,
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
    let mcp_ops = ops.clone();
    tokio::spawn(async move {
        serve_mcp_with_listener(mcp_l, mcp_ops).await.unwrap();
    });

    wait_connectable(&data_endpoint).await;

    TestStack {
        data_endpoint,
        admin_endpoint,
        control_endpoint,
        mcp_endpoint,
        _tmp: tmp,
    }
}
