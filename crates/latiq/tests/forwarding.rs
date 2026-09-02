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

//! Full-stack node-to-node forwarding: with two pond nodes behind one control
//! plane, a request sent to a node that doesn't own the pond is forwarded to the
//! owner and the result relayed back, indistinguishable from a local one. We
//! never rely on which node the registry picks — we resolve the owner, then
//! deliberately drive the *other* node.
mod common;

use common::{start_stack_n, start_stack_n_with_auth, MultiStack, NodeStack};
use latiq_proto::v1::control_client::ControlClient;
use latiq_proto::v1::data_client::DataClient;
use latiq_proto::v1::stream_client::StreamClient;
use latiq_proto::v1::*;
use tonic::{Code, Request};

fn req<T>(msg: T, agent: &str) -> Request<T> {
    let mut r = Request::new(msg);
    r.metadata_mut()
        .insert("latiq-agent-id", agent.parse().unwrap());
    r
}
fn q(pond: &str, sql: &str) -> QueryRequest {
    QueryRequest {
        pond: pond.into(),
        sql: sql.into(),
    }
}
async fn client(ep: &str) -> DataClient<tonic::transport::Channel> {
    DataClient::connect(ep.to_string()).await.unwrap()
}

/// Allocate `name` via node 0, then resolve which node the control plane placed
/// it on. Returns the owner's internal endpoint.
async fn allocate_and_locate(stack: &MultiStack, name: &str) -> String {
    let mut c0 = client(&stack.nodes[0].data_endpoint).await;
    c0.allocate_pond(req(
        AllocatePondRequest {
            name: name.into(),
            policy_json: String::new(),
            tier: String::new(),
            lineage: false,
        },
        "alice",
    ))
    .await
    .unwrap();
    let mut ctl = ControlClient::connect(stack.control_endpoint.clone())
        .await
        .unwrap();
    ctl.get_pond_location(GetPondLocationRequest {
        pond_ref: name.into(),
    })
    .await
    .unwrap()
    .into_inner()
    .node_endpoint
}

fn rows(json: &str) -> serde_json::Value {
    serde_json::from_str::<serde_json::Value>(json).unwrap()["rows"].clone()
}

/// `_meta.served_by` — the node that actually EXECUTED the statement. Missing
/// is a failure, not an empty string: a response that does not say who served
/// it is exactly the state these tests exist to rule out.
fn served_by(json: &str) -> String {
    let v: serde_json::Value = serde_json::from_str(json).unwrap();
    v["_meta"]["served_by"]
        .as_str()
        .unwrap_or_else(|| panic!("every query response must carry _meta.served_by: {json}"))
        .to_string()
}

#[tokio::test]
async fn forwarding_read_happy() {
    let stack = start_stack_n(2).await;
    let owner = allocate_and_locate(&stack, "fwd").await;
    // Drive the NON-owner: every op below must be forwarded to `owner`.
    let mut n = client(&stack.other_than(&owner).data_endpoint).await;

    n.write_query(req(q("fwd", "CREATE TABLE t(i INTEGER)"), "alice"))
        .await
        .unwrap();
    n.write_query(req(q("fwd", "INSERT INTO t VALUES (1),(2),(3)"), "alice"))
        .await
        .unwrap();
    let r = n
        .read_query(req(q("fwd", "SELECT count(*) AS n FROM t"), "alice"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(rows(&r.json)[0][0], 3);
}

#[tokio::test]
async fn forwarding_write_then_read_consistent() {
    let stack = start_stack_n(2).await;
    let owner = allocate_and_locate(&stack, "fwd2").await;
    let mut non_owner = client(&stack.other_than(&owner).data_endpoint).await;
    let mut owner_c = client(&owner).await;

    // Write through the non-owner (forwarded)...
    non_owner
        .write_query(req(q("fwd2", "CREATE TABLE t AS SELECT 42 AS v"), "alice"))
        .await
        .unwrap();
    // ...and read it back directly from the owner (local). Same data both ways.
    let direct = owner_c
        .read_query(req(q("fwd2", "SELECT v FROM t"), "alice"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(rows(&direct.json)[0][0], 42);
    let forwarded = non_owner
        .read_query(req(q("fwd2", "SELECT v FROM t"), "alice"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(rows(&forwarded.json)[0][0], 42);
}

#[tokio::test]
async fn forwarding_served_by_names_the_owner_not_the_greeter() {
    // The response-level proof that a forward actually happened. Every other
    // test in this file passes if forwarding silently degrades to "the greeter
    // lazily creates its own empty pond and serves the request locally" —
    // because a greeter serving its own pond returns a plausible answer to
    // every one of them. This assertion is the one that cannot.
    let stack = start_stack_n(2).await;
    let owner = allocate_and_locate(&stack, "sb").await;
    let greeter = stack.other_than(&owner).data_endpoint.clone();
    assert_ne!(
        greeter, owner,
        "the two endpoints must differ or the equalities below prove nothing"
    );
    let mut o = client(&owner).await;
    let mut g = client(&greeter).await;

    // Direct: the node that received it is the node that ran it.
    let direct = o
        .write_query(req(q("sb", "CREATE TABLE t AS SELECT 7 AS i"), "alice"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(served_by(&direct.json), owner);

    // Forwarded: the OWNER, from a request the greeter received.
    let fwd_write = g
        .write_query(req(q("sb", "INSERT INTO t VALUES (8)"), "alice"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        served_by(&fwd_write.json),
        owner,
        "a forwarded write is executed by the owner and must say so"
    );
    let fwd_read = g
        .read_query(req(q("sb", "SELECT count(*) AS n FROM t"), "alice"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(rows(&fwd_read.json)[0][0], 2, "the read saw both writes");
    assert_eq!(
        served_by(&fwd_read.json),
        owner,
        "a forwarded read is executed by the owner and must say so"
    );
}

/// Drain an Arrow read from `endpoint` and return every non-empty `served_by`
/// the peer put on a chunk. A `Vec` rather than an `Option` so a test can pin
/// how MANY chunks claimed to serve the read: exactly one must, and a stream
/// that names its server on every chunk is as wrong as one that names it never.
async fn stream_served_by(endpoint: &str, pond: &str) -> Vec<String> {
    let mut sc = StreamClient::connect(endpoint.to_string()).await.unwrap();
    let mut st = sc
        .read_arrow(req(q(pond, "SELECT i FROM t"), "alice"))
        .await
        .unwrap()
        .into_inner();
    let mut named = Vec::new();
    let mut chunks = 0;
    while let Some(c) = st.message().await.unwrap() {
        chunks += 1;
        if !c.served_by.is_empty() {
            named.push(c.served_by);
        }
    }
    assert!(
        chunks > 1,
        "the read must produce a batch beyond the schema chunk, or 'exactly one \
         chunk names the server' would be true of a one-chunk stream by default"
    );
    named
}

#[tokio::test]
async fn forwarding_served_by_rides_the_arrow_stream_from_the_owner() {
    // The streaming path has no `_meta` to carry the answer in: the owner puts
    // its name on the first chunk and the greeter relays it. Without this, a
    // forwarded stream would be indistinguishable from a local one.
    let stack = start_stack_n(2).await;
    let owner = allocate_and_locate(&stack, "sbs").await;
    let greeter = stack.other_than(&owner).data_endpoint.clone();
    assert_ne!(greeter, owner, "or the equalities below prove nothing");
    let mut o = client(&owner).await;
    o.write_query(req(
        q("sbs", "CREATE TABLE t AS SELECT i FROM range(3) t(i)"),
        "alice",
    ))
    .await
    .unwrap();

    assert_eq!(
        stream_served_by(&owner, "sbs").await,
        vec![owner.clone()],
        "a direct stream is served by the node dialled, said once"
    );
    assert_eq!(
        stream_served_by(&greeter, "sbs").await,
        vec![owner.clone()],
        "a forwarded stream reports the OWNER, relayed verbatim and said once"
    );
}

#[tokio::test]
async fn forwarding_attribution_preserved_across_hop() {
    // A forwarded write must be attributed to the original agent (identity rides
    // the hop), visible in the owner's native DuckLake snapshots.
    let stack = start_stack_n(2).await;
    let owner = allocate_and_locate(&stack, "fwd3").await;
    let mut n = client(&stack.other_than(&owner).data_endpoint).await;

    n.write_query(req(q("fwd3", "CREATE TABLE t(i INTEGER)"), "carol"))
        .await
        .unwrap();
    let authors = n
        .read_query(req(
            q(
                "fwd3",
                "SELECT DISTINCT author FROM ducklake_snapshots('fwd3')",
            ),
            "carol",
        ))
        .await
        .unwrap()
        .into_inner();
    let r = rows(&authors.json);
    let found = r.as_array().unwrap().iter().any(|row| row[0] == "carol");
    assert!(found, "forwarded write should be attributed to carol: {r}");
}

#[tokio::test]
async fn forwarding_describe_and_drop() {
    let stack = start_stack_n(2).await;
    let owner = allocate_and_locate(&stack, "fwd4").await;
    let mut n = client(&stack.other_than(&owner).data_endpoint).await;

    n.write_query(req(q("fwd4", "CREATE TABLE t(i INTEGER)"), "alice"))
        .await
        .unwrap();
    // describe forwarded → sees the table created on the owner.
    let d = n
        .describe_pond(req(
            DescribePondRequest {
                pond: "fwd4".into(),
            },
            "alice",
        ))
        .await
        .unwrap()
        .into_inner();
    let v: serde_json::Value = serde_json::from_str(&d.json).unwrap();
    assert_eq!(v["pond"]["name"], "fwd4");

    // drop forwarded → the pond is gone from the registry.
    n.drop_pond(req(
        DropPondRequest {
            pond: "fwd4".into(),
            confirm: true,
        },
        "alice",
    ))
    .await
    .unwrap();
    let err = n
        .read_query(req(q("fwd4", "SELECT 1"), "alice"))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound);
}

#[tokio::test]
async fn forwarding_pond_not_found_propagates() {
    // A non-existent pond has no owner to forward to: the greeter resolves it
    // against the control plane and returns NotFound directly.
    let stack = start_stack_n(2).await;
    let mut n = client(&stack.nodes[1].data_endpoint).await;
    let err = n
        .read_query(req(q("ghost", "SELECT 1"), "alice"))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound);
}

#[tokio::test]
async fn forwarding_engine_error_propagates_across_hop() {
    // An error raised by the engine on the OWNER must survive the forward hop:
    // status_to_error rebuilds an AgentError from the peer Status, then the
    // greeter re-encodes it for the caller (envelope carried in details).
    let stack = start_stack_n(2).await;
    let owner = allocate_and_locate(&stack, "fwd6").await;
    let mut n = client(&stack.other_than(&owner).data_endpoint).await;
    let err = n
        .read_query(req(q("fwd6", "SELECT * FROM does_not_exist"), "alice"))
        .await
        .unwrap_err();
    assert!(
        !err.message().is_empty(),
        "owner's error message should cross the hop"
    );
    assert!(
        !err.details().is_empty(),
        "structured envelope should cross the hop"
    );
}

#[tokio::test]
async fn forwarding_carries_the_verified_subject_across_the_hop() {
    // The node-to-node hop must not downgrade a verified caller to a claimed
    // one: the greeter forwards the ORIGINAL bearer token and the owner verifies
    // it itself, so the owner's DuckLake author is the token's subject. (A hop
    // that re-injected only `latiq-agent-id` would silently record "dave".)
    let idp = latiq_auth::test_support::TestIdp::start().await;
    let stack = start_stack_n_with_auth(2, idp.auth_config()).await;
    let token = idp.mint("svc-dave", "latiq", &idp.issuer, 300);
    let owner = allocate_and_locate_authed(&stack, "fwdauth", &token).await;
    let mut n = client(&stack.other_than(&owner).data_endpoint).await;

    n.write_query(bearer_req(
        q("fwdauth", "CREATE TABLE t(i INTEGER)"),
        "dave",
        &token,
    ))
    .await
    .unwrap();
    let r = n
        .read_query(bearer_req(
            q(
                "fwdauth",
                "SELECT DISTINCT author FROM ducklake_snapshots('fwdauth')",
            ),
            "dave",
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    let authors = rows(&r.json);
    let list = authors.as_array().unwrap();
    assert!(
        list.iter().any(|row| row[0] == "svc-dave"),
        "forwarded write should keep the verified subject: {authors}"
    );
    assert!(
        !list.iter().any(|row| row[0] == "dave"),
        "the claimed leaf must not become the author across the hop: {authors}"
    );
}

fn bearer_req<T>(msg: T, agent: &str, token: &str) -> Request<T> {
    let mut r = req(msg, agent);
    r.metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    r
}

/// `allocate_and_locate`, but presenting a bearer token on the auth'd stack.
async fn allocate_and_locate_authed(stack: &MultiStack, name: &str, token: &str) -> String {
    let mut c0 = client(&stack.nodes[0].data_endpoint).await;
    c0.allocate_pond(bearer_req(
        AllocatePondRequest {
            name: name.into(),
            policy_json: String::new(),
            tier: String::new(),
            lineage: false,
        },
        "dave",
        token,
    ))
    .await
    .unwrap();
    let mut ctl = ControlClient::connect(stack.control_endpoint.clone())
        .await
        .unwrap();
    ctl.get_pond_location(GetPondLocationRequest {
        pond_ref: name.into(),
    })
    .await
    .unwrap()
    .into_inner()
    .node_endpoint
}

// ---------------------------------------------------------------------------
// Auth across the node hop. Placement is random, so ownership is pinned the only
// reliable way: allocate while a single node exists, THEN add the peer that will
// forward. That is what makes an asymmetric greeter/owner pair testable.
// ---------------------------------------------------------------------------

/// An owner-first cluster: `owner` is the sole node when `pond` is allocated (so
/// it certainly owns it), and `greeter` is added afterwards to forward to it.
struct HopPair {
    greeter: NodeStack,
    _owner: NodeStack,
}

async fn hop_pair(
    pond: &str,
    owner_auth: Option<latiq_auth::AuthConfig>,
    greeter_auth: Option<latiq_auth::AuthConfig>,
    alloc_token: Option<&str>,
) -> HopPair {
    let (control, _admin) = common::start_control_plane_only().await;
    let owner = common::add_node("owner", &control, owner_auth).await;
    let mut oc = client(&owner.data_endpoint).await;
    let msg = AllocatePondRequest {
        name: pond.into(),
        policy_json: String::new(),
        tier: String::new(),
        lineage: false,
    };
    let r = match alloc_token {
        Some(t) => bearer_req(msg, "dave", t),
        None => req(msg, "dave"),
    };
    oc.allocate_pond(r).await.unwrap();
    let greeter = common::add_node("greeter", &control, greeter_auth).await;
    HopPair {
        greeter,
        _owner: owner,
    }
}

#[tokio::test]
async fn forwarding_does_not_leak_a_client_authorization_header_without_auth() {
    // A node with NO verifier must not capture whatever `authorization` header a
    // client happens to send — one meant for an upstream gateway, say — and
    // replay it to a peer over the internal channel. The owner here REQUIRES a
    // token, so if the greeter had forwarded the (perfectly valid) header, this
    // write would succeed. It must not.
    let idp = latiq_auth::test_support::TestIdp::start().await;
    let token = idp.mint("svc-dave", "latiq", &idp.issuer, 300);
    let pair = hop_pair("leak", Some(idp.auth_config()), None, Some(&token)).await;
    let mut n = client(&pair.greeter.data_endpoint).await;

    let err = n
        .write_query(bearer_req(
            q("leak", "CREATE TABLE t(i INTEGER)"),
            "dave",
            &token,
        ))
        .await
        .unwrap_err();
    assert!(
        err.message().contains("a bearer token is required"),
        "the client's header must not cross the hop from an unauthenticated node: {err:?}"
    );
}

#[tokio::test]
async fn forwarding_without_any_token_fails_at_the_owner() {
    // The unset-task-local path: the greeter requires nothing, the owner does.
    // The hop must fail hard rather than quietly forwarding an unauthenticated
    // request that the owner would then have to trust.
    let idp = latiq_auth::test_support::TestIdp::start().await;
    let token = idp.mint("svc-dave", "latiq", &idp.issuer, 300);
    let pair = hop_pair("notok", Some(idp.auth_config()), None, Some(&token)).await;
    let mut n = client(&pair.greeter.data_endpoint).await;

    let err = n
        .write_query(req(q("notok", "CREATE TABLE t(i INTEGER)"), "dave"))
        .await
        .unwrap_err();
    assert!(
        err.message().contains("a bearer token is required"),
        "{err:?}"
    );
}

#[tokio::test]
async fn forwarding_token_the_owner_rejects_surfaces_as_unauthenticated() {
    // Genuine RE-VERIFICATION at the owner: the greeter trusts issuers A and B,
    // the owner only A. A token from B satisfies the greeter and is replayed —
    // and the owner rejects it on its own authority. The greeter cannot vouch
    // for it.
    //
    // The CODE is what a client branches on, so the peer's `Unauthenticated`
    // must survive the hop as `Unauthenticated` (via `ErrorKind::Unauthenticated`
    // in the envelope) rather than falling into `status_to_error`'s catch-all as
    // `Internal` — which reads as a crash and hides the one failure a client can
    // act on by re-minting its token.
    let idp_a = latiq_auth::test_support::TestIdp::start().await;
    let idp_b = latiq_auth::test_support::TestIdp::start_alt().await;
    let both = latiq_auth::AuthConfig {
        audience: "latiq".into(),
        allow_insecure_jwks: false,
        issuers: vec![
            idp_a.auth_config().issuers[0].clone(),
            idp_b.auth_config().issuers[0].clone(),
        ],
    };
    let token_a = idp_a.mint("svc-a", "latiq", &idp_a.issuer, 300);
    let token_b = idp_b.mint("svc-b", "latiq", &idp_b.issuer, 300);
    let pair = hop_pair(
        "reverify",
        Some(idp_a.auth_config()),
        Some(both),
        Some(&token_a),
    )
    .await;
    let mut n = client(&pair.greeter.data_endpoint).await;

    // Sanity: issuer A is fine end to end, so the failure below is about issuer
    // B and not about the hop being broken.
    n.write_query(bearer_req(
        q("reverify", "CREATE TABLE t(i INTEGER)"),
        "dave",
        &token_a,
    ))
    .await
    .unwrap();

    let err = n
        .write_query(bearer_req(
            q("reverify", "CREATE TABLE u(i INTEGER)"),
            "dave",
            &token_b,
        ))
        .await
        .unwrap_err();
    assert_eq!(
        err.code(),
        Code::Unauthenticated,
        "a peer's Unauthenticated must stay actionable across the hop"
    );
    assert!(
        err.message().contains("the bearer token was rejected"),
        "the owner's own rejection should cross the hop: {err:?}"
    );
    // …and the kind travels in the envelope, so a client reading `kind` (rather
    // than the gRPC code) branches the same way.
    let env: latiq_common::ErrorEnvelope =
        serde_json::from_slice(err.details()).expect("the envelope rides the Status details");
    assert_eq!(env.kind, latiq_common::ErrorKind::Unauthenticated);
}

#[tokio::test]
async fn forwarding_concurrent_tokens_stay_isolated_per_request() {
    // The token rides a task-local. Twenty concurrent forwarded writes under two
    // different subjects must each be attributed to their OWN token — nothing
    // would otherwise catch a refactor that hoisted it into shared state.
    let idp = latiq_auth::test_support::TestIdp::start().await;
    let stack = start_stack_n_with_auth(2, idp.auth_config()).await;
    let alice = idp.mint("svc-alice", "latiq", &idp.issuer, 300);
    let bob = idp.mint("svc-bob", "latiq", &idp.issuer, 300);

    // One pond per request, each allocated (and therefore placed) independently,
    // then driven through a node chosen without regard to ownership.
    let mut tasks = Vec::new();
    for i in 0..20 {
        let (token, subject) = if i % 2 == 0 {
            (alice.clone(), "svc-alice")
        } else {
            (bob.clone(), "svc-bob")
        };
        let pond = format!("conc{i}");
        let ep0 = stack.nodes[0].data_endpoint.clone();
        let ep1 = stack.nodes[1].data_endpoint.clone();
        tasks.push(tokio::spawn(async move {
            let mut c0 = client(&ep0).await;
            c0.allocate_pond(bearer_req(
                AllocatePondRequest {
                    name: pond.clone(),
                    policy_json: String::new(),
                    tier: String::new(),
                    lineage: false,
                },
                "dave",
                &token,
            ))
            .await
            .unwrap();
            // Drive the OTHER node half the time, so roughly half of these are
            // real forwards.
            let mut c = client(if i % 4 < 2 { &ep0 } else { &ep1 }).await;
            c.write_query(bearer_req(
                q(&pond, "CREATE TABLE t(i INTEGER)"),
                "dave",
                &token,
            ))
            .await
            .unwrap();
            let r = c
                .read_query(bearer_req(
                    q(
                        &pond,
                        &format!("SELECT DISTINCT author FROM ducklake_snapshots('{pond}')"),
                    ),
                    "dave",
                    &token,
                ))
                .await
                .unwrap()
                .into_inner();
            let authors = rows(&r.json);
            let list = authors.as_array().unwrap();
            assert!(
                list.iter().any(|row| row[0] == subject),
                "pond {pond} should be authored by {subject}: {authors}"
            );
            let other = if subject == "svc-alice" {
                "svc-bob"
            } else {
                "svc-alice"
            };
            assert!(
                !list.iter().any(|row| row[0] == other),
                "pond {pond} must not pick up the concurrent request's token: {authors}"
            );
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
}

/// REGRESSION PIN — issue #89, end to end. A node decided whether it owned a
/// pond by string-comparing its own advertised endpoint against the endpoint
/// the registry stored for the owning node. When those two strings named the
/// same node in different spellings, the node concluded it was not the owner,
/// dialled the owner — itself — over gRPC, and re-entered the same decision,
/// forwarding again with nothing to bound it.
///
/// Ownership is now decided on `node_id`, the stable id this node registered
/// with, which is also how the registry assigns ponds. The drift is produced
/// the way a real one is: the same node id re-registers under a differently
/// spelled address (here a trailing slash; a hostname vs its IP or a
/// re-addressed node do the same), which the registry's upsert accepts.
///
/// What the in-process pin in `latiq-agent-core` cannot prove and this does:
/// that the id survives the registry → Control gRPC → `GrpcControlPlane` trip,
/// and that the id a node registers with is the id it routes on.
#[tokio::test]
async fn forwarding_serves_locally_when_the_registrys_endpoint_spelling_drifted() {
    let stack = start_stack_n(1).await;
    let node = &stack.nodes[0];
    let owner = allocate_and_locate(&stack, "drift").await;
    assert_eq!(
        owner, node.internal_endpoint,
        "one node, so the pond must be placed on it"
    );

    let mut ctl = ControlClient::connect(stack.control_endpoint.clone())
        .await
        .unwrap();
    ctl.register_node(RegisterNodeRequest {
        node_id: node.node_id.clone(),
        mcp_endpoint: node.mcp_endpoint.clone(),
        internal_endpoint: format!("{}/", node.internal_endpoint),
        capacity: 100,
    })
    .await
    .unwrap();
    // The drift is real, and the assertion below is not vacuous: the registry
    // now hands out a spelling this node does not use for itself.
    let drifted = ctl
        .get_pond_location(GetPondLocationRequest {
            pond_ref: "drift".into(),
        })
        .await
        .unwrap()
        .into_inner()
        .node_endpoint;
    assert_ne!(
        drifted, node.internal_endpoint,
        "the registry must now name this node by a different string"
    );

    // Bounded on purpose: the failure this pins is UNBOUNDED self-forwarding,
    // which does not return an error — it recurses. A timeout is the only way
    // the test fails cleanly when the mechanism breaks.
    let deadline = std::time::Duration::from_secs(20);
    let mut c = client(&node.data_endpoint).await;
    let w = tokio::time::timeout(
        deadline,
        c.write_query(req(q("drift", "CREATE TABLE t AS SELECT 7 AS v"), "alice")),
    )
    .await
    .expect("a node that forwards into itself never answers")
    .unwrap()
    .into_inner();
    assert_eq!(
        served_by(&w.json),
        node.internal_endpoint,
        "the node that holds the pond must serve it, not relay to itself"
    );
    let r = tokio::time::timeout(
        deadline,
        c.read_query(req(q("drift", "SELECT v FROM t"), "alice")),
    )
    .await
    .expect("a node that forwards into itself never answers")
    .unwrap()
    .into_inner();
    // The write landed in the one real pond, and the read found it there — a
    // node serving an empty pond of its own would answer this differently.
    assert_eq!(rows(&r.json)[0][0], 7);
}

/// Eager, holistic allocation: an allocation only succeeds once the pond's
/// storage exists on the node that owns it.
///
/// The fixture in all three tests is a **greeter that is not in the placement
/// pool** plus exactly one registered node, so every pond these allocations
/// create is placed on that one node and must cross the wire to be
/// materialised. Anything less deterministic would leave the core claim decided
/// by `ORDER BY random()`.
mod eager_allocation {
    use super::*;
    use common::{add_greeter_node, add_node, register_ghost_node, start_control_plane_only};

    // `Err` is tonic's `Status`, whose size the RPC surface fixes for us — the
    // lint's suggestion (box it) would mean unboxing at every call site here.
    #[allow(clippy::result_large_err)]
    async fn allocate(endpoint: &str, name: &str) -> Result<String, tonic::Status> {
        let mut c = client(endpoint).await;
        c.allocate_pond(req(
            AllocatePondRequest {
                name: name.into(),
                policy_json: String::new(),
                tier: String::new(),
                lineage: false,
            },
            "alice",
        ))
        .await
        .map(|r| r.into_inner().pond_id)
    }

    /// The structured `ErrorEnvelope` the Data surface puts in `Status::details`
    /// — asserting on the code alone would not distinguish this failure from any
    /// other `FailedPrecondition`.
    fn envelope(s: &tonic::Status) -> latiq_common::ErrorEnvelope {
        serde_json::from_slice(s.details())
            .unwrap_or_else(|e| panic!("every Data error carries an envelope ({e}): {s:?}"))
    }

    async fn pond_names(control_endpoint: &str) -> Vec<String> {
        let mut ctl = ControlClient::connect(control_endpoint.to_string())
            .await
            .unwrap();
        ctl.list_ponds(ListPondsRequest {})
            .await
            .unwrap()
            .into_inner()
            .ponds
            .into_iter()
            .map(|p| p.name)
            .collect()
    }

    #[tokio::test]
    async fn pond_lifecycle_allocation_materializes_storage_on_the_owner() {
        // THE claim. Before eager allocation this returned the same pond id with
        // no directory on any node: the owner materialised it lazily, on a write
        // that might arrive minutes later or never.
        let (control, _admin) = start_control_plane_only().await;
        let owner = add_node("owner", &control, None).await;
        let greeter = add_greeter_node("greeter", &control).await;

        let pond_id = allocate(&greeter.data_endpoint, "eager")
            .await
            .expect("allocation succeeds")
            .to_string();

        assert!(
            owner.holds_pond(&pond_id),
            "the owning node must hold the pond's storage the moment allocate returns"
        );
        assert!(
            !greeter.holds_pond(&pond_id),
            "and the node that merely took the call must hold nothing — a greeter with \
             its own copy is the empty pond every forwarded read would fall into"
        );
    }

    #[tokio::test]
    async fn pond_lifecycle_allocation_rolls_back_when_the_owner_is_unreachable() {
        // The compensation. The registry row is written BEFORE the owner is
        // reached, so a failure at the owner has to give it back — otherwise
        // failing fast would just be a new way to burn a name for ever.
        let (control, _admin) = start_control_plane_only().await;
        let ghost = register_ghost_node(&control, "gone").await;
        let greeter = add_greeter_node("greeter", &control).await;

        let status = allocate(&greeter.data_endpoint, "doomed")
            .await
            .expect_err("a pond nobody could create must not be reported as created");
        let env = envelope(&status);
        assert_eq!(
            env.kind,
            latiq_common::ErrorKind::PondUnavailable,
            "{}",
            env.message
        );
        assert!(
            env.message.contains("was NOT created") && env.message.contains("rolled back"),
            "an agent must be told the pond does not exist AND that nothing was left \
             behind; a bare storage error reads as 'maybe it half-worked': {}",
            env.message
        );
        assert!(
            env.message.contains(&ghost.internal_endpoint),
            "and which node could not be reached: {}",
            env.message
        );
        // The compensation itself, read from the registry rather than inferred
        // from the error text.
        assert!(
            !pond_names(&control).await.contains(&"doomed".to_string()),
            "the registry row must be gone: {:?}",
            pond_names(&control).await
        );
    }

    #[tokio::test]
    async fn pond_lifecycle_allocation_reuses_the_name_after_a_rolled_back_attempt() {
        // What the rollback is FOR, and the assertion the registry check alone
        // cannot make: the agent takes the error's advice (retry the same name)
        // and it works, rather than meeting a NameConflict with a pond it cannot
        // see in list_ponds.
        let (control, _admin) = start_control_plane_only().await;
        let ghost = register_ghost_node(&control, "gone").await;
        let greeter = add_greeter_node("greeter", &control).await;

        let status = allocate(&greeter.data_endpoint, "retry-me")
            .await
            .expect_err("the owner is down");
        assert_eq!(
            envelope(&status).kind,
            latiq_common::ErrorKind::PondUnavailable
        );

        // The node comes back at the address the registry already published.
        let owner = ghost.revive(&control).await;
        let pond_id = allocate(&greeter.data_endpoint, "retry-me")
            .await
            .expect("the same name must be free again");
        assert!(
            owner.holds_pond(&pond_id),
            "and the retry really materialised, on the node that is back"
        );
    }

    #[tokio::test]
    async fn pond_lifecycle_materialize_pond_rpc_is_idempotent() {
        // The contract the allocating node relies on to be able to retry, and
        // what lets the lazy ensure-on-first-use fallback coexist with this
        // rather than race it into a conflict.
        let stack = start_stack_n(1).await;
        let owner = &stack.nodes[0];
        let pond_id = allocate(&owner.data_endpoint, "twice").await.unwrap();
        assert!(owner.holds_pond(&pond_id), "allocated locally, eagerly");

        let mut c = client(&owner.data_endpoint).await;
        for attempt in 1..=2 {
            c.materialize_pond(req(
                MaterializePondRequest {
                    pond: pond_id.clone(),
                },
                "alice",
            ))
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "materialising an existing pond is a success, not a conflict ({attempt}): {e}"
                )
            });
        }
        assert!(owner.holds_pond(&pond_id), "and the pond is still there");
    }

    #[tokio::test]
    async fn auth_allocation_replays_the_callers_token_to_the_owner() {
        // The hop eager allocation added is an AUTHENTICATED hop. The
        // `allocate_pond` handler used to be the one handler outside `traced`,
        // documented as safe precisely because allocation never forwarded; now
        // it does, and without that scope the forwarder has no token to replay,
        // so the owner refuses and every allocation placed on a peer fails with
        // `Unauthenticated` — in authenticated deployments only, which is the
        // worst place for a regression to hide.
        let idp = latiq_auth::test_support::TestIdp::start().await;
        let (control, _admin) = start_control_plane_only().await;
        let owner = add_node("owner", &control, Some(idp.auth_config())).await;
        let greeter =
            common::add_greeter_node_with_auth("greeter", &control, idp.auth_config()).await;
        let token = idp.mint("svc-dave", "latiq", &idp.issuer, 300);

        let mut c = client(&greeter.data_endpoint).await;
        let pond_id = c
            .allocate_pond(bearer_req(
                AllocatePondRequest {
                    name: "authed".into(),
                    policy_json: String::new(),
                    tier: String::new(),
                    lineage: false,
                },
                "dave",
                &token,
            ))
            .await
            .expect("the caller's own token must cross the hop")
            .into_inner()
            .pond_id;
        assert!(
            owner.holds_pond(&pond_id),
            "and the owner really materialised it under that token"
        );
    }
}
