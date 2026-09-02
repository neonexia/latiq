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
                latiq_control_plane::control_service::ControlService::new(r1, None),
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
                latiq_control_plane::control_service::ControlService::new(registry, None),
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
        // The point of this fixture: the default guard refuses it. Turning the
        // escape on here would make the config usable and the test vacuous.
        allow_insecure_jwks: false,
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

/// Centralised pond creation: `CreatePondAssignment` places the pond, asks the
/// owning node to materialise it, and gives the placement back if that fails.
///
/// This is where the eager-allocation guarantees live now. They used to be
/// pinned in `latiq-agent-core/tests/agent_ops.rs::forwarding` against a fake
/// `ControlPlane` + `Forwarder`, because the allocating NODE materialised. It
/// does not any more — every create path (MCP, Data gRPC, `latiq pond create`,
/// the SDK) funnels through this one RPC — so the guarantees are asserted here,
/// against the code that actually carries them.
///
/// The service is driven directly rather than over a socket: the orchestration
/// under test is entirely inside the handler, and a `Request` built here carries
/// metadata exactly as one off the wire does. The full-stack proofs against real
/// pond nodes are in `crates/latiq/tests/forwarding.rs::eager_allocation`.
mod materialize {
    use latiq_common::{ErrorEnvelope, ErrorKind};
    use latiq_control_plane::control_service::ControlService;
    use latiq_control_plane::node_client::{CallerAuth, GrpcNodeMaterializer, NodeMaterializer};
    use latiq_control_plane::Registry;
    use latiq_proto::v1::control_server::Control;
    use latiq_proto::v1::*;
    use std::sync::{Arc, Mutex};
    use tonic::Request;

    /// One `MaterializePond` the control plane made.
    #[derive(Clone, Debug)]
    struct Call {
        endpoint: String,
        pond_id: String,
        agent_id: Option<String>,
        bearer: Option<String>,
    }

    /// What a fake node does when the control plane reaches it.
    enum Behaviour {
        Ok,
        /// Refuse, with this cause text.
        Fail(String),
        /// Refuse — after deleting the pond's registry row, so the control
        /// plane's compensating `drop_pond` finds nothing to drop. This is how
        /// the ORPHAN branch is reached without a fake registry: the row really
        /// is gone underneath the handler, which is one of the two ways a real
        /// rollback fails (the other, an unwell registry, produces the same
        /// `Err` from the same call).
        FailAfterDeletingTheRow(Registry),
    }

    struct FakeNode {
        calls: Mutex<Vec<Call>>,
        behaviour: Behaviour,
    }

    impl FakeNode {
        fn new(behaviour: Behaviour) -> Arc<Self> {
            Arc::new(Self {
                calls: Mutex::new(Vec::new()),
                behaviour,
            })
        }
        fn calls(&self) -> Vec<Call> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[tonic::async_trait]
    impl NodeMaterializer for FakeNode {
        async fn materialize(
            &self,
            endpoint: &str,
            pond_id: &str,
            caller: &CallerAuth,
        ) -> Result<(), String> {
            self.calls.lock().unwrap().push(Call {
                endpoint: endpoint.to_string(),
                pond_id: pond_id.to_string(),
                agent_id: caller.agent_id.clone(),
                bearer: caller.bearer.clone(),
            });
            match &self.behaviour {
                Behaviour::Ok => Ok(()),
                Behaviour::Fail(cause) => Err(cause.clone()),
                Behaviour::FailAfterDeletingTheRow(registry) => {
                    registry
                        .drop_pond(pond_id)
                        .expect("the fixture must really delete the row");
                    Err("node exploded".to_string())
                }
            }
        }
    }

    /// A registry with one active node at `endpoint`.
    fn registry_with_node(endpoint: &str) -> Registry {
        let r = Registry::open(None).unwrap();
        r.register_node("node-a", "http://node-a:8080/mcp", endpoint, 100)
            .unwrap();
        r
    }

    fn create(name: &str) -> CreatePondAssignmentRequest {
        CreatePondAssignmentRequest {
            name: name.into(),
            owner_identity: "agent-x".into(),
            policy_json: "{}".into(),
            tier: "medium".into(),
            extensions: vec![],
            description: String::new(),
            lineage: false,
        }
    }

    /// The structured envelope the Control surface puts in `Status::details`.
    /// Asserting on the code alone would not tell this failure apart from any
    /// other `FailedPrecondition` the RPC can return.
    fn envelope(s: &tonic::Status) -> ErrorEnvelope {
        serde_json::from_slice(s.details())
            .unwrap_or_else(|e| panic!("every Control error carries an envelope ({e}): {s:?}"))
    }

    fn pond_names(registry: &Registry) -> Vec<String> {
        registry
            .list_ponds()
            .unwrap()
            .into_iter()
            .map(|p| p.name)
            .collect()
    }

    #[tokio::test]
    async fn pond_lifecycle_create_materializes_on_the_owning_node() {
        // THE claim. Before this, `CreatePondAssignment` wrote a registry row and
        // returned: `latiq pond create` and the SDK reported a pond that existed
        // nowhere until somebody's first query happened to materialise it, on a
        // node that might never have been reachable.
        let registry = registry_with_node("http://node-a:9092");
        let node = FakeNode::new(Behaviour::Ok);
        let svc = ControlService::new(registry.clone(), Some(node.clone()));

        let created = svc
            .create_pond_assignment(Request::new(create("eager")))
            .await
            .expect("create succeeds")
            .into_inner();

        let calls = node.calls();
        assert_eq!(
            calls.len(),
            1,
            "the owner must be asked to materialise, exactly once: {calls:?}"
        );
        assert_eq!(
            calls[0].endpoint, "http://node-a:9092",
            "…at the address the registry holds for the node it placed the pond on"
        );
        assert_eq!(
            calls[0].pond_id, created.pond_id,
            "…about the pond it just created, not some other one: reaching the \
             right node about the wrong pond leaves the caller's pond just as unreal"
        );
        assert_eq!(pond_names(&registry), vec!["eager".to_string()]);
    }

    #[tokio::test]
    async fn pond_lifecycle_create_rolls_back_when_the_node_cannot_materialize() {
        // The compensation. The registry row is written BEFORE the node is
        // reached, so a failure there has to give it back — otherwise failing
        // fast would just be a new way to burn a name for ever.
        let registry = registry_with_node("http://node-a:9092");
        let node = FakeNode::new(Behaviour::Fail("connection refused".into()));
        let svc = ControlService::new(registry.clone(), Some(node.clone()));

        let status = svc
            .create_pond_assignment(Request::new(create("doomed")))
            .await
            .expect_err("a pond nobody could create must not be reported as created");
        assert_eq!(node.calls().len(), 1, "it did try");
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        let env = envelope(&status);
        assert_eq!(env.kind, ErrorKind::PondUnavailable, "{}", env.message);
        assert!(
            env.message.contains("was NOT created") && env.message.contains("rolled back"),
            "the caller must be told the pond does not exist AND that nothing was \
             left behind; a bare storage error reads as 'maybe it half-worked': {}",
            env.message
        );
        assert!(
            env.message.contains("http://node-a:9092"),
            "…and which node could not do it: {}",
            env.message
        );
        assert!(
            env.message.contains("connection refused"),
            "…and why, in the node's own words: {}",
            env.message
        );
        assert!(
            env.suggest.contains("Retry"),
            "a fully compensated attempt should just be retried: {}",
            env.suggest
        );
        // The compensation itself, read from the registry rather than inferred
        // from the error text.
        assert!(
            pond_names(&registry).is_empty(),
            "the registry row must be gone: {:?}",
            pond_names(&registry)
        );
    }

    #[tokio::test]
    async fn pond_lifecycle_create_reuses_the_name_after_a_rolled_back_attempt() {
        // What the rollback is FOR, and the assertion the registry check alone
        // cannot make: the caller takes the error's advice (retry the same name)
        // and it works, rather than meeting a NameConflict for a pond nothing
        // can show it.
        let registry = registry_with_node("http://node-a:9092");
        let down = ControlService::new(
            registry.clone(),
            Some(FakeNode::new(Behaviour::Fail("node is down".into()))),
        );
        down.create_pond_assignment(Request::new(create("retry-me")))
            .await
            .expect_err("the owner is down");

        let up = ControlService::new(registry.clone(), Some(FakeNode::new(Behaviour::Ok)));
        up.create_pond_assignment(Request::new(create("retry-me")))
            .await
            .expect("the same name must be free again");
        assert_eq!(pond_names(&registry), vec!["retry-me".to_string()]);
    }

    #[tokio::test]
    async fn pond_lifecycle_create_reports_the_orphan_when_the_rollback_also_fails() {
        // Row written, materialise failed, rollback failed: a name that may still
        // resolve to a pond with no storage. Telling this caller to "retry with
        // the same name" would send it into a NameConflict it cannot explain, so
        // the guidance changes — and an operator has to hear about it (the
        // `error!` in `ControlService::compensate`, which names the pond id).
        let registry = registry_with_node("http://node-a:9092");
        let node = FakeNode::new(Behaviour::FailAfterDeletingTheRow(registry.clone()));
        let svc = ControlService::new(registry.clone(), Some(node));

        let status = svc
            .create_pond_assignment(Request::new(create("stranded")))
            .await
            .expect_err("still a failure");
        let env = envelope(&status);
        assert_eq!(env.kind, ErrorKind::PondUnavailable);
        assert!(
            env.message.contains("may still exist"),
            "the caller must be told the row may have survived: {}",
            env.message
        );
        assert!(
            !env.message.contains("has been rolled back"),
            "and must NOT be told it was rolled back, which is the one fact that \
             decides whether the name is safe to reuse: {}",
            env.message
        );
        assert!(
            env.suggest.contains("DIFFERENT name") && env.suggest.contains("pond forget"),
            "…and given the operator escape hatch rather than a retry that will \
             conflict: {}",
            env.suggest
        );
    }

    #[tokio::test]
    async fn pond_lifecycle_create_rolls_back_when_the_owner_has_no_address() {
        // A node registered with no internal endpoint (or one whose row was
        // rewritten to nonsense) can host nothing: there is no address to
        // materialise at. The REAL client is used here, because the property is
        // its own — an unusable endpoint must fail immediately with a cause the
        // caller can read, not hang for the connect timeout or panic.
        let registry = registry_with_node("");
        let svc = ControlService::new(
            registry.clone(),
            Some(Arc::new(GrpcNodeMaterializer::new())),
        );

        let status = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            svc.create_pond_assignment(Request::new(create("addressless"))),
        )
        .await
        .expect("an unusable endpoint must fail fast, not wait on a connect")
        .expect_err("a pond placed on a node with no address must not be created");
        let env = envelope(&status);
        assert_eq!(env.kind, ErrorKind::PondUnavailable, "{}", env.message);
        assert!(env.message.contains("was NOT created"), "{}", env.message);
        assert!(
            pond_names(&registry).is_empty(),
            "and the row is gone: {:?}",
            pond_names(&registry)
        );
    }

    #[tokio::test]
    async fn auth_create_replays_the_callers_bearer_only_when_authenticated() {
        // The control plane relays this call to a pond node that verifies tokens
        // on its own authority, so the caller's own `authorization` must cross —
        // and must cross ONLY from a control plane that was configured with an
        // issuer. An unauthenticated one must not capture whatever header a
        // client happens to send (one meant for an upstream gateway, say) and
        // present it on an internal channel; that is the same rule the
        // node-to-node forwarder keeps.
        let mut req = Request::new(create("authed"));
        req.metadata_mut()
            .insert("authorization", "Bearer tok-123".parse().unwrap());
        req.metadata_mut()
            .insert("latiq-agent-id", "claimed-leaf".parse().unwrap());

        let mut seen = Vec::new();
        for authenticated in [true, false] {
            let registry = registry_with_node("http://node-a:9092");
            let node = FakeNode::new(Behaviour::Ok);
            let svc =
                ControlService::new(registry, Some(node.clone())).replaying_bearer(authenticated);
            let mut r = Request::new(create("authed"));
            *r.metadata_mut() = req.metadata().clone();
            svc.create_pond_assignment(r).await.unwrap();
            let call = node.calls().pop().expect("the node was called");
            seen.push((authenticated, call));
        }

        let authed = &seen[0].1;
        assert_eq!(
            authed.bearer.as_deref(),
            Some("Bearer tok-123"),
            "an authenticated control plane replays the caller's token verbatim, \
             so the node re-verifies it and reaches the same subject"
        );
        let relaxed = &seen[1].1;
        assert_eq!(
            relaxed.bearer, None,
            "an unauthenticated control plane must not present a header it never verified"
        );
        // Anti-vacuity: the identity itself still crosses in BOTH modes, so the
        // `None` above is the bearer gate working and not the whole request
        // arriving stripped of metadata.
        for (authenticated, call) in &seen {
            assert_eq!(
                call.agent_id.as_deref(),
                Some("claimed-leaf"),
                "the claimed leaf is attribution, not authority, and rides either \
                 way (authenticated={authenticated})"
            );
        }
    }

    #[tokio::test]
    async fn pond_lifecycle_metadata_reads_still_answer_when_every_node_is_down() {
        // Why invariant 3 exists, and the line the lifecycle exception must not
        // cross: creating a pond may now fail because a node is unreachable, but
        // `pond list` / `describe` must not — they are the calls an operator
        // makes precisely BECAUSE the cluster is unwell. If an outbound node
        // call ever leaked onto a read path, this is what would catch it.
        let registry = registry_with_node("http://node-a:9092");
        let svc = ControlService::new(registry.clone(), Some(FakeNode::new(Behaviour::Ok)));
        let created = svc
            .create_pond_assignment(Request::new(create("survivor")))
            .await
            .unwrap()
            .into_inner();

        // Now every node is unreachable.
        let down = ControlService::new(
            registry,
            Some(FakeNode::new(Behaviour::Fail("connection refused".into()))),
        );
        let listed = down
            .list_ponds(Request::new(ListPondsRequest {}))
            .await
            .expect("list_ponds must not depend on a node being up")
            .into_inner()
            .ponds;
        assert_eq!(
            listed.iter().map(|p| p.name.as_str()).collect::<Vec<_>>(),
            vec!["survivor"]
        );
        let info = down
            .get_pond_info(Request::new(GetPondInfoRequest {
                pond_ref: "survivor".into(),
            }))
            .await
            .expect("get_pond_info must not depend on a node being up")
            .into_inner()
            .pond
            .expect("a pond");
        assert_eq!(info.pond_id, created.pond_id);

        // …and the same control plane really cannot create right now, so the two
        // reads above passed with a genuinely unreachable cluster.
        down.create_pond_assignment(Request::new(create("newcomer")))
            .await
            .expect_err("creating IS allowed to fail when the node is down");
    }
}
