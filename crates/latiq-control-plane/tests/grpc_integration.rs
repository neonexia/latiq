//! Starts both gRPC surfaces in-process over a shared in-memory Registry and
//! drives them with the generated clients (the M4 integration gate).
use latiq_control_plane::Registry;
use latiq_proto::v1::admin_client::AdminClient;
use latiq_proto::v1::admin_server::AdminServer;
use latiq_proto::v1::control_client::ControlClient;
use latiq_proto::v1::control_server::ControlServer;
use latiq_proto::v1::*;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

#[tokio::test]
async fn control_and_admin_surfaces_work() {
    let registry = Registry::open(None).unwrap();

    // Bind both surfaces on ephemeral ports without a drop/rebind race.
    let control_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let control_port = control_listener.local_addr().unwrap().port();
    let admin_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let admin_port = admin_listener.local_addr().unwrap().port();

    let r1 = registry.clone();
    tokio::spawn(async move {
        Server::builder()
            .add_service(ControlServer::new(
                latiq_control_plane::control_service::ControlService::new(r1),
            ))
            .serve_with_incoming(TcpListenerStream::new(control_listener))
            .await
            .unwrap();
    });
    let r2 = registry.clone();
    tokio::spawn(async move {
        Server::builder()
            .add_service(AdminServer::new(
                latiq_control_plane::admin_service::AdminService::new(r2),
            ))
            .serve_with_incoming(TcpListenerStream::new(admin_listener))
            .await
            .unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let mut control = ControlClient::connect(format!("http://127.0.0.1:{control_port}"))
        .await
        .unwrap();
    let mut admin = AdminClient::connect(format!("http://127.0.0.1:{admin_port}"))
        .await
        .unwrap();

    control
        .register_node(RegisterNodeRequest {
            node_id: "node-a".into(),
            mcp_endpoint: "http://n:8080/mcp".into(),
            internal_endpoint: "http://n:9092".into(),
            capacity: 100,
        })
        .await
        .unwrap();

    let created = control
        .create_pond_assignment(CreatePondAssignmentRequest {
            name: "incident-1".into(),
            owner_identity: "agent-x".into(),
            policy_json: "{}".into(),
            tier: "medium".into(),
            extensions: vec![],
            description: "incident triage scratch".into(),
            lineage: false,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(created.assigned_node_endpoint, "http://n:9092");

    // Description round-trips through both read surfaces (admin pond_list + control info).
    let summary = admin
        .pond_list(PondListRequest {})
        .await
        .unwrap()
        .into_inner()
        .ponds;
    assert_eq!(
        summary
            .iter()
            .find(|p| p.name == "incident-1")
            .unwrap()
            .description,
        "incident triage scratch"
    );
    let info = control
        .get_pond_info(GetPondInfoRequest {
            pond_ref: "incident-1".into(),
        })
        .await
        .unwrap()
        .into_inner()
        .pond
        .unwrap();
    assert_eq!(info.description, "incident triage scratch");

    let loc = control
        .get_pond_location(GetPondLocationRequest {
            pond_ref: "incident-1".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(loc.pond_id, created.pond_id);

    let nodes = admin
        .list_nodes(ListNodesRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(nodes.nodes.len(), 1);

    admin
        .policy_set(PolicySetRequest {
            key: "query_timeout_seconds".into(),
            value: "45".into(),
        })
        .await
        .unwrap();
    let pol = admin
        .policy_get(PolicyGetRequest {})
        .await
        .unwrap()
        .into_inner();
    assert!(pol.policy_json.contains("\"45\""));
}

/// Allocating a pond when no node is registered must surface as a precondition
/// failure (no host available), NOT NotFound — NotFound is reserved for a
/// missing pond and the client maps it to `pond_not_found` (review #13).
#[tokio::test]
async fn error_contract_allocate_with_no_node_is_precondition_not_notfound() {
    let registry = Registry::open(None).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        Server::builder()
            .add_service(ControlServer::new(
                latiq_control_plane::control_service::ControlService::new(registry),
            ))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let mut control = ControlClient::connect(format!("http://127.0.0.1:{port}"))
        .await
        .unwrap();

    // No register_node call: the registry has zero nodes.
    let status = control
        .create_pond_assignment(CreatePondAssignmentRequest {
            name: "orphan".into(),
            owner_identity: "agent-x".into(),
            policy_json: "{}".into(),
            tier: "medium".into(),
            extensions: vec![],
            description: String::new(),
            lineage: false,
        })
        .await
        .expect_err("allocate with no node must fail");
    assert_eq!(
        status.code(),
        tonic::Code::FailedPrecondition,
        "no-node allocate must be FailedPrecondition, not {:?}",
        status.code()
    );
}

/// An `AuthConfig` the verifier must refuse: plaintext http to a non-loopback
/// host, i.e. signing keys fetched over a channel anyone can rewrite.
fn unusable_auth_config() -> latiq_auth::AuthConfig {
    latiq_auth::AuthConfig {
        audience: "latiq".to_string(),
        issuers: vec![latiq_auth::IssuerConfig {
            issuer: "https://idp.example/realms/latiq".to_string(),
            jwks_uri: Some("http://idp.example/jwks".to_string()),
        }],
    }
}

#[tokio::test]
async fn auth_bad_config_fails_startup_instead_of_degrading() {
    // The worst failure mode this design has is a control plane that was ASKED
    // for verification, could not build a verifier, and served anyway with none.
    // Both entry points must refuse to serve at all.
    let registry = Registry::open(None).unwrap();
    let addr = {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        l.local_addr().unwrap()
    };

    let err =
        latiq_control_plane::serve_admin(addr, registry.clone(), Some(unusable_auth_config()))
            .await
            .expect_err("serve_admin must not start with an unusable auth config");
    assert!(
        err.to_string().contains("jwks"),
        "the startup failure must say what was wrong: {err}"
    );

    let err = latiq_control_plane::serve_control_plane(
        addr,
        registry.clone(),
        Some(unusable_auth_config()),
    )
    .await
    .expect_err("serve_control_plane must not start with an unusable auth config");
    assert!(err.to_string().contains("jwks"), "{err}");

    // The port is still free: nothing bound before the config was rejected.
    tokio::net::TcpListener::bind(addr)
        .await
        .expect("a refused startup must not have bound the port");
}
