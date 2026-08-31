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
