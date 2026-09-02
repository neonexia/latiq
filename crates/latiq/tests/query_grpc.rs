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

//! Full-stack feature tests for the Data/Query gRPC surface (CLI/SDK path),
//! driven through a real control-plane + pond-node (GrpcControlPlane). Test
//! names are prefixed by feature so `cargo test <feature>` targets them.
mod common;

use common::{start_stack, start_stack_with_auth};
use latiq_common::ErrorEnvelope;
use latiq_proto::v1::data_client::DataClient;
use latiq_proto::v1::*;
use tonic::{Code, Request};

fn req<T>(msg: T, agent: &str) -> Request<T> {
    let mut r = Request::new(msg);
    r.metadata_mut()
        .insert("latiq-agent-id", agent.parse().unwrap());
    r
}

fn alloc(name: &str) -> AllocatePondRequest {
    AllocatePondRequest {
        name: name.into(),
        policy_json: String::new(),
        tier: String::new(),
        lineage: false,
    }
}
fn q(pond: &str, sql: &str) -> QueryRequest {
    QueryRequest {
        pond: pond.into(),
        sql: sql.into(),
        timeout_ms: 0,
    }
}
/// Decode the ErrorEnvelope from a tonic Status' details.
fn envelope(status: &tonic::Status) -> ErrorEnvelope {
    serde_json::from_slice(status.details()).expect("status details carry an ErrorEnvelope")
}

async fn client(ep: &str) -> DataClient<tonic::transport::Channel> {
    DataClient::connect(ep.to_string()).await.unwrap()
}

#[tokio::test]
async fn pond_lifecycle_tier_recorded_and_described() {
    let s = start_stack().await;
    let mut c = client(&s.data_endpoint).await;
    c.allocate_pond(req(
        AllocatePondRequest {
            name: "big".into(),
            policy_json: String::new(),
            tier: "large".into(),
            lineage: false,
        },
        "a",
    ))
    .await
    .unwrap();
    let d = c
        .describe_pond(req(DescribePondRequest { pond: "big".into() }, "a"))
        .await
        .unwrap()
        .into_inner();
    let v: serde_json::Value = serde_json::from_str(&d.json).unwrap();
    assert_eq!(v["pond"]["tier"], "large", "tier recorded + described");
}

/// The uncapped tier is an **operator grant**: an uncapped pond can starve every
/// other pond on its node, so a workload must not be able to assign it to itself
/// at allocate time. The rule lives in the registry, but the registry is not the
/// surface an SDK or CLI reaches — so it is asserted here, on the wire, and on
/// the *kind*, not merely on "it errored". A normal tier allocating in the same
/// test is what stops this passing because allocate is broken outright.
#[tokio::test]
async fn policy_tier_none_is_refused_over_data_grpc() {
    let s = start_stack().await;
    let mut c = client(&s.data_endpoint).await;
    let with_tier = |name: &str, tier: &str| AllocatePondRequest {
        name: name.into(),
        policy_json: String::new(),
        tier: tier.into(),
        lineage: false,
    };
    // Every spelling the parser accepts, or the guard is bypassable by alias.
    for tier in ["none", "uncapped", " NONE "] {
        let status = c
            .allocate_pond(req(with_tier("greedy", tier), "a"))
            .await
            .err()
            .unwrap_or_else(|| panic!("tier `{tier}` must not be self-assignable"));
        let env = envelope(&status);
        assert_eq!(
            env.kind,
            latiq_common::ErrorKind::InvalidValue,
            "tier `{tier}`: an operator-only tier is an invalid VALUE — not an \
             unknown tier, and not an internal error. That distinction is the \
             whole point: a caller must be able to tell `none exists but is not \
             yours` from `no such tier`."
        );
        assert!(
            env.message.contains("set-tier"),
            "tier `{tier}`: the message must name the operator escape hatch, got: {}",
            env.message
        );
    }
    // The refusal is about the tier, not about allocate.
    c.allocate_pond(req(with_tier("polite", "small"), "a"))
        .await
        .expect("a normal tier must still allocate");
}

#[tokio::test]
async fn result_encoding_arrow_edge_renders_date_and_nested() {
    // The Data read path now collects from the Arrow hop and renders to JSON at
    // the edge. Confirm common types still produce sane JSON (a date string, a
    // nested array) rather than garbage.
    let s = start_stack().await;
    let mut c = client(&s.data_endpoint).await;
    c.allocate_pond(req(alloc("enc"), "a")).await.unwrap();
    let resp = c
        .read_query(req(
            q("enc", "SELECT DATE '2021-03-04' AS d, [10,20,30] AS arr"),
            "a",
        ))
        .await
        .unwrap()
        .into_inner();
    let v: serde_json::Value = serde_json::from_str(&resp.json).unwrap();
    let row0 = &v["rows"][0];
    assert_eq!(v["columns"], serde_json::json!(["d", "arr"]));
    assert!(
        row0[0].as_str().is_some_and(|s| s.contains("2021")),
        "date should render as a readable string, got {}",
        row0[0]
    );
    assert_eq!(row0[1], serde_json::json!([10, 20, 30]), "nested array");
}

#[tokio::test]
async fn result_encoding_renders_timestamptz_named_zone() {
    // Regression: a TIMESTAMP WITH TIME ZONE comes back as Arrow Timestamp(_,
    // Some("UTC")) — a *named* zone (DuckLake's snapshots() does exactly this).
    // The arrow->json path needs arrow's `chrono-tz` feature, or it errors with
    // "only offset based timezones supported without chrono-tz feature".
    let s = start_stack().await;
    let mut c = client(&s.data_endpoint).await;
    c.allocate_pond(req(alloc("tz"), "a")).await.unwrap();
    let resp = c
        .read_query(req(
            q("tz", "SELECT TIMESTAMPTZ '2024-01-02 03:04:05+00' AS ts"),
            "a",
        ))
        .await
        .unwrap()
        .into_inner();
    let v: serde_json::Value = serde_json::from_str(&resp.json).unwrap();
    // The point is that the named-zone timestamp *renders* (no chrono-tz error).
    // The exact value is session-timezone-dependent (icu shifts TIMESTAMPTZ to the
    // local zone — e.g. -08:00 yields 2024-01-01T19:04:05), so assert only that it
    // came back as a non-empty timestamp-ish string, not a specific wall-clock date.
    assert!(
        v["rows"][0][0]
            .as_str()
            .is_some_and(|s| s.contains("2024") && s.contains(':')),
        "timestamptz should render as a readable string, got {}",
        v["rows"][0][0]
    );
}

#[tokio::test]
async fn pond_lifecycle_happy() {
    let s = start_stack().await;
    let mut c = client(&s.data_endpoint).await;

    let a = c
        .allocate_pond(req(alloc("demo"), "alice"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(a.pond_name, "demo");
    assert!(!a.pond_id.is_empty());

    let d = c
        .describe_pond(req(
            DescribePondRequest {
                pond: "demo".into(),
            },
            "alice",
        ))
        .await
        .unwrap()
        .into_inner();
    let v: serde_json::Value = serde_json::from_str(&d.json).unwrap();
    assert_eq!(v["pond"]["name"], "demo");

    c.drop_pond(req(
        DropPondRequest {
            pond: "demo".into(),
            confirm: true,
        },
        "alice",
    ))
    .await
    .unwrap();

    // describe after drop → NotFound
    let err = c
        .describe_pond(req(
            DescribePondRequest {
                pond: "demo".into(),
            },
            "alice",
        ))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound);
}

#[tokio::test]
async fn pond_lifecycle_drop_requires_confirm() {
    let s = start_stack().await;
    let mut c = client(&s.data_endpoint).await;
    c.allocate_pond(req(alloc("keepme"), "alice"))
        .await
        .unwrap();

    // confirm=false → the destructive drop is refused with a structured error.
    let err = c
        .drop_pond(req(
            DropPondRequest {
                pond: "keepme".into(),
                confirm: false,
            },
            "alice",
        ))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
    assert_eq!(
        envelope(&err).kind,
        latiq_common::ErrorKind::MissingArgument
    );

    // The pond survives the refused drop.
    c.describe_pond(req(
        DescribePondRequest {
            pond: "keepme".into(),
        },
        "alice",
    ))
    .await
    .expect("pond must still exist after an un-confirmed drop");
}

#[tokio::test]
async fn pond_lifecycle_duplicate_name_conflicts() {
    let s = start_stack().await;
    let mut c = client(&s.data_endpoint).await;
    c.allocate_pond(req(alloc("dup"), "alice")).await.unwrap();
    let err = c.allocate_pond(req(alloc("dup"), "bob")).await.unwrap_err();
    assert_eq!(err.code(), Code::AlreadyExists);
    assert_eq!(envelope(&err).kind, latiq_common::ErrorKind::NameConflict);
}

#[tokio::test]
async fn sql_read_write_happy() {
    let s = start_stack().await;
    let mut c = client(&s.data_endpoint).await;
    c.allocate_pond(req(alloc("p"), "alice")).await.unwrap();

    c.write_query(req(
        q("p", "CREATE TABLE t(id INTEGER, note VARCHAR)"),
        "alice",
    ))
    .await
    .unwrap();
    c.write_query(req(q("p", "INSERT INTO t VALUES (1,'a'),(2,'b')"), "alice"))
        .await
        .unwrap();

    let r = c
        .read_query(req(q("p", "SELECT id, note FROM t ORDER BY id"), "alice"))
        .await
        .unwrap()
        .into_inner();
    let v: serde_json::Value = serde_json::from_str(&r.json).unwrap();
    assert_eq!(v["columns"], serde_json::json!(["id", "note"]));
    assert_eq!(v["rows"].as_array().unwrap().len(), 2);
    assert_eq!(v["rows"][1][1], "b");
    assert_eq!(v["statement"], "read_query");
}

#[tokio::test]
async fn sql_read_write_read_rejects_writes() {
    let s = start_stack().await;
    let mut c = client(&s.data_endpoint).await;
    c.allocate_pond(req(alloc("p"), "alice")).await.unwrap();
    let err = c
        .read_query(req(q("p", "INSERT INTO t VALUES (1)"), "alice"))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
    assert_eq!(
        envelope(&err).kind,
        latiq_common::ErrorKind::ReadOnlyViolation
    );
}

#[tokio::test]
async fn attribution_records_the_writer_identity() {
    let s = start_stack().await;
    let mut c = client(&s.data_endpoint).await;
    c.allocate_pond(req(alloc("p"), "alice")).await.unwrap();
    c.write_query(req(q("p", "CREATE TABLE t(id INTEGER)"), "bob"))
        .await
        .unwrap();
    c.write_query(req(q("p", "INSERT INTO t VALUES (1)"), "bob"))
        .await
        .unwrap();

    let r = c
        .read_query(req(
            q("p", "SELECT DISTINCT author FROM ducklake_snapshots('p')"),
            "viewer",
        ))
        .await
        .unwrap()
        .into_inner();
    let v: serde_json::Value = serde_json::from_str(&r.json).unwrap();
    let authors: Vec<_> = v["rows"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|row| row[0].as_str())
        .collect();
    assert!(authors.contains(&"bob"), "got {authors:?}");
}

#[tokio::test]
async fn result_encoding_carries_meta() {
    let s = start_stack().await;
    let mut c = client(&s.data_endpoint).await;
    c.allocate_pond(req(alloc("p"), "alice")).await.unwrap();
    let r = c
        .read_query(req(q("p", "SELECT 1 AS x"), "alice"))
        .await
        .unwrap()
        .into_inner();
    let v: serde_json::Value = serde_json::from_str(&r.json).unwrap();
    assert_eq!(v["status"], "ok");
    assert_eq!(v["_meta"]["rows"], 1);
    assert!(v["_meta"].get("duration_ms").is_some());
}

#[tokio::test]
async fn error_contract_pond_not_found_carries_envelope() {
    let s = start_stack().await;
    let mut c = client(&s.data_endpoint).await;
    let err = c
        .read_query(req(q("ghost", "SELECT 1"), "alice"))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound);
    let e = envelope(&err);
    assert_eq!(e.kind, latiq_common::ErrorKind::PondNotFound);
    assert!(!e.suggest.is_empty());
    assert!(e.see.starts_with("latiq://"));
}

#[tokio::test]
async fn identity_defaults_to_anonymous_when_metadata_absent() {
    let s = start_stack().await;
    let mut c = client(&s.data_endpoint).await;
    // No latiq-agent-id metadata.
    c.allocate_pond(Request::new(alloc("anon"))).await.unwrap();
    c.write_query(Request::new(q("anon", "CREATE TABLE t(id INTEGER)")))
        .await
        .unwrap();
    c.write_query(Request::new(q("anon", "INSERT INTO t VALUES (1)")))
        .await
        .unwrap();
    let r = c
        .read_query(Request::new(q(
            "anon",
            "SELECT DISTINCT author FROM ducklake_snapshots('anon')",
        )))
        .await
        .unwrap()
        .into_inner();
    let v: serde_json::Value = serde_json::from_str(&r.json).unwrap();
    let authors: Vec<_> = v["rows"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|row| row[0].as_str())
        .collect();
    assert!(authors.contains(&"anonymous"), "got {authors:?}");
}

// ---------------------------------------------------------------------------
// auth_* — the Data + Stream gRPC surfaces as an OAuth 2.1 resource server.
// Verification only: nothing here gates on WHO the caller is.
// ---------------------------------------------------------------------------

/// A request carrying both the claimed leaf and an `authorization` bearer token.
fn bearer_req<T>(msg: T, agent: &str, token: &str) -> Request<T> {
    let mut r = req(msg, agent);
    r.metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    r
}

/// The distinct authors recorded in a pond's native DuckLake snapshots.
fn authors_of(json: &str) -> Vec<String> {
    let v: serde_json::Value = serde_json::from_str(json).unwrap();
    v["rows"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|row| row[0].as_str().map(String::from))
        .collect()
}

const AUTHORS_SQL: &str = "SELECT DISTINCT author FROM ducklake_snapshots";

#[tokio::test]
async fn auth_absent_config_keeps_relaxed_identity() {
    // The embedded/dev path: no issuer configured means behave exactly as
    // before — no token required, and the claimed leaf is the author. Asserted
    // rather than assumed: every existing deployment depends on it.
    let s = start_stack().await;
    let mut c = client(&s.data_endpoint).await;
    c.allocate_pond(req(alloc("noauth"), "dana")).await.unwrap();
    c.write_query(req(q("noauth", "CREATE TABLE t(i INTEGER)"), "dana"))
        .await
        .unwrap();
    let r = c
        .read_query(req(
            q("noauth", &format!("{AUTHORS_SQL}('noauth')")),
            "dana",
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(
        authors_of(&r.json).iter().any(|a| a == "dana"),
        "relaxed identity should still attribute to the claimed leaf"
    );
}

/// The whole rejection matrix for the Data and Stream surfaces, on ONE stack:
/// `(surface, credential) -> (Unauthenticated, an RFC 9728 discovery challenge,
/// no leak of what we trust)`. This was four tests and four
/// `start_stack_with_auth` startups for what is one table; the matrix IS the
/// property, so it reads better as one.
///
/// RFC 9728 discovery is the reason the challenge is asserted on every row: a
/// Data/Stream client that is turned away has no other in-band way to learn
/// WHERE to get a token, and the challenge in the Status' metadata is the whole
/// handshake we participate in. The `for (why, ...)` shape is borrowed from
/// `tests/mcp.rs` — every assertion names the row it fired on.
#[tokio::test]
async fn auth_rejects_every_bad_credential_on_both_surfaces() {
    let idp = latiq_auth::test_support::TestIdp::start().await;
    let s = start_stack_with_auth(idp.auth_config()).await;
    let mut c = client(&s.data_endpoint).await;
    let mut sc = latiq_proto::v1::stream_client::StreamClient::connect(s.data_endpoint.clone())
        .await
        .unwrap();

    let good = idp.mint("svc-dana", "latiq", &idp.issuer, 300);
    // A real pond, so a row that got through would succeed — which is what makes
    // the rejections below about the CREDENTIAL and not about the request.
    c.allocate_pond(bearer_req(alloc("st"), "dana", &good))
        .await
        .unwrap();
    let expired = idp.mint("svc-dana", "latiq", &idp.issuer, -60);

    let credentials: [(&str, Option<&str>); 3] = [
        ("no token at all", None),
        ("a token that is not a JWT", Some("not-a-jwt")),
        (
            "an expired token from the real issuer",
            Some(expired.as_str()),
        ),
    ];

    for surface in ["data", "stream"] {
        let mut challenges: Vec<String> = Vec::new();
        for (why, token) in credentials {
            let request = match token {
                Some(t) => bearer_req(q("st", "SELECT 1"), "dana", t),
                None => req(q("st", "SELECT 1"), "dana"),
            };
            let err = if surface == "data" {
                c.read_query(request).await.unwrap_err()
            } else {
                sc.read_arrow(request).await.unwrap_err()
            };

            assert_eq!(err.code(), Code::Unauthenticated, "{surface}: {why}");

            // The rejection must not tell an unauthenticated caller which
            // issuers we trust or where their keys live.
            let msg = err.message().to_lowercase();
            assert!(
                !msg.contains(&idp.issuer.to_lowercase()) && !msg.contains("jwks"),
                "{surface}: {why} — the rejection leaks issuers or the JWKS uri: {msg}"
            );

            let challenge = err
                .metadata()
                .get("www-authenticate")
                .unwrap_or_else(|| {
                    panic!("{surface}: {why} — a rejection must advertise where to get a token")
                })
                .to_str()
                .unwrap()
                .to_string();
            assert!(
                challenge.starts_with(r#"Bearer resource_metadata=""#)
                    && challenge.contains("/.well-known/oauth-protected-resource"),
                "{surface}: {why} — got {challenge}"
            );
            challenges.push(challenge);
        }
        // Identical for every rejection: the client's recovery (go back to the
        // authorization server) does not depend on HOW the credential failed.
        assert!(
            challenges.windows(2).all(|w| w[0] == w[1]),
            "{surface}: the challenge must not vary by failure mode: {challenges:?}"
        );
    }

    // ...and both surfaces let a good token through, which is what proves the
    // rows above are not a surface that refuses everything.
    c.read_query(bearer_req(q("st", "SELECT 1"), "dana", &good))
        .await
        .unwrap();
    sc.read_arrow(bearer_req(q("st", "SELECT 1"), "dana", &good))
        .await
        .unwrap();
}

/// With no verifier there is nothing to discover, so no challenge is attached —
/// a node that never opted into auth must not start advertising an auth server.
#[tokio::test]
async fn auth_absent_config_sends_no_challenge() {
    let s = start_stack().await;
    let mut c = client(&s.data_endpoint).await;
    let err = c
        .describe_pond(req(
            DescribePondRequest {
                pond: "nope".into(),
            },
            "dana",
        ))
        .await
        .unwrap_err();
    assert!(err.metadata().get("www-authenticate").is_none());
}

#[tokio::test]
async fn auth_accepts_a_valid_token_and_marks_identity_verified() {
    let idp = latiq_auth::test_support::TestIdp::start().await;
    let s = start_stack_with_auth(idp.auth_config()).await;
    let mut c = client(&s.data_endpoint).await;
    let token = idp.mint("svc-dana", "latiq", &idp.issuer, 300);

    c.allocate_pond(bearer_req(alloc("ok"), "dana", &token))
        .await
        .unwrap();
    c.write_query(bearer_req(
        q("ok", "CREATE TABLE t(i INTEGER)"),
        "dana",
        &token,
    ))
    .await
    .unwrap();
    let r = c
        .read_query(bearer_req(
            q("ok", &format!("{AUTHORS_SQL}('ok')")),
            "dana",
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    let authors = authors_of(&r.json);
    // The proof that the identity was VERIFIED and not merely accepted: the
    // DuckLake commit author is the token's SUBJECT, not the claimed leaf.
    assert!(
        authors.iter().any(|a| a == "svc-dana"),
        "author should be the verified subject, got {authors:?}"
    );
    assert!(
        !authors.iter().any(|a| a == "dana"),
        "the claimed leaf must not be the author for a verified caller: {authors:?}"
    );
}

// ---------------------------------------------------------------------------
/// Arrow streaming over the same Data/Stream surface, driven the way a stock
/// Arrow client drives it: `StreamDecoder` over the raw IPC chunks, exactly
/// what `pyarrow.ipc` does. Driven against the NON-owner node, so it also
/// exercises the node-to-node forward and the past-the-cap streaming the unary
/// JSON path can't do.
///
/// A submodule rather than a separate binary: same surface, same harness, and a
/// binary costs a statically linked DuckDB (~130-160 MB). It keeps its own `req`
/// because it needs the multi-node `MultiStack` fixture, not this file's stack.
// ---------------------------------------------------------------------------
mod arrow_stream {
    use crate::common::{start_stack_n, MultiStack};
    use arrow::buffer::Buffer;
    use arrow::ipc::reader::StreamDecoder;
    use latiq_proto::v1::control_client::ControlClient;
    use latiq_proto::v1::data_client::DataClient;
    use latiq_proto::v1::stream_client::StreamClient;
    use latiq_proto::v1::*;
    use tonic::Request;

    fn req<T>(msg: T, agent: &str) -> Request<T> {
        let mut r = Request::new(msg);
        r.metadata_mut()
            .insert("latiq-agent-id", agent.parse().unwrap());
        r
    }

    async fn seed_and_locate(stack: &MultiStack, pond: &str, rows: usize) -> String {
        let mut d0 = DataClient::connect(stack.nodes[0].data_endpoint.clone())
            .await
            .unwrap();
        d0.allocate_pond(req(
            AllocatePondRequest {
                name: pond.into(),
                policy_json: String::new(),
                tier: String::new(),
                lineage: false,
            },
            "a",
        ))
        .await
        .unwrap();
        d0.write_query(req(
            QueryRequest {
                pond: pond.into(),
                sql: format!("CREATE TABLE t AS SELECT * FROM range({rows}) r(i)"),
                timeout_ms: 0,
            },
            "a",
        ))
        .await
        .unwrap();
        let mut ctl = ControlClient::connect(stack.control_endpoint.clone())
            .await
            .unwrap();
        ctl.get_pond_location(GetPondLocationRequest {
            pond_ref: pond.into(),
        })
        .await
        .unwrap()
        .into_inner()
        .node_endpoint
    }

    /// Decode an ArrowChunk stream the way any Arrow client would: feed bytes to a
    /// StreamDecoder, collecting (row_count, column-0 name, batch_count).
    async fn drain(mut s: tonic::Streaming<ArrowChunk>) -> (usize, String, usize) {
        let mut decoder = StreamDecoder::new();
        let (mut rows, mut batches) = (0usize, 0usize);
        while let Some(chunk) = s.message().await.unwrap() {
            let mut buf = Buffer::from_vec(chunk.ipc);
            while !buf.is_empty() {
                match decoder.decode(&mut buf).unwrap() {
                    Some(batch) => {
                        rows += batch.num_rows();
                        batches += 1;
                    }
                    None => break,
                }
            }
        }
        let col0 = decoder
            .schema()
            .expect("schema decoded")
            .field(0)
            .name()
            .clone();
        (rows, col0, batches)
    }

    #[tokio::test]
    async fn arrow_stream_forwarded_past_cap() {
        // 25k rows > the 10k JSON inline cap: the unary path would reject this, the
        // stream path delivers it all.
        let stack = start_stack_n(2).await;
        let owner = seed_and_locate(&stack, "big", 25_000).await;

        let mut sc = StreamClient::connect(stack.other_than(&owner).data_endpoint.clone())
            .await
            .unwrap();
        let streaming = sc
            .read_arrow(req(
                QueryRequest {
                    pond: "big".into(),
                    sql: "SELECT i FROM t".into(),
                    timeout_ms: 0,
                },
                "a",
            ))
            .await
            .unwrap()
            .into_inner();

        let (rows, col0, batches) = drain(streaming).await;
        assert_eq!(rows, 25_000, "all rows streamed past the inline cap");
        assert_eq!(col0, "i");
        assert!(batches > 1, "result arrived as multiple batches (streamed)");
    }

    #[tokio::test]
    async fn arrow_stream_local_and_empty() {
        let stack = start_stack_n(2).await;
        let owner = seed_and_locate(&stack, "loc", 10).await;

        // Hit the OWNER directly (local path) and an empty result (schema only).
        let mut sc = StreamClient::connect(owner).await.unwrap();
        let streaming = sc
            .read_arrow(req(
                QueryRequest {
                    pond: "loc".into(),
                    sql: "SELECT i FROM t WHERE i < 0".into(),
                    timeout_ms: 0,
                },
                "a",
            ))
            .await
            .unwrap()
            .into_inner();
        let (rows, col0, _) = drain(streaming).await;
        assert_eq!(rows, 0, "empty result");
        assert_eq!(col0, "i", "schema present even with zero rows");
    }
}

// ---------------------------------------------------------------------------
/// Lineage is a **per-pond opt-in, off by default**: chosen when the pond is
/// created, stored in the control-plane registry, reported by describe, and
/// fixed for the pond's lifetime. Two ponds on one node can differ.
///
/// A submodule rather than a separate binary (tests/CLAUDE.md rule 5): same
/// surface, same harness, and a binary costs a statically linked DuckDB.
// ---------------------------------------------------------------------------
mod lineage {
    use super::{client, req};
    use latiq_proto::v1::control_client::ControlClient;
    use latiq_proto::v1::*;

    fn alloc(name: &str, lineage: bool) -> AllocatePondRequest {
        AllocatePondRequest {
            name: name.into(),
            policy_json: String::new(),
            tier: String::new(),
            lineage,
        }
    }

    /// The `pond` half of a describe response.
    async fn described(ep: &str, pond: &str) -> serde_json::Value {
        let mut c = client(ep).await;
        let d = c
            .describe_pond(req(DescribePondRequest { pond: pond.into() }, "a"))
            .await
            .expect("describe")
            .into_inner();
        let v: serde_json::Value = serde_json::from_str(&d.json).expect("describe returns JSON");
        v["pond"].clone()
    }

    #[tokio::test]
    async fn lineage_is_off_by_default() {
        // Lineage costs disk (every read and write emits) and per-query plan
        // extraction, so the default deployment must pay nothing: a pond
        // allocated without asking for it reports lineage disabled.
        let s = crate::common::start_stack().await;
        let mut c = client(&s.data_endpoint).await;
        c.allocate_pond(req(alloc("plain", false), "a"))
            .await
            .expect("allocate");

        assert_eq!(
            described(&s.data_endpoint, "plain").await["lineage"],
            serde_json::Value::Bool(false),
            "a pond allocated without asking for lineage must report it disabled"
        );
    }

    #[tokio::test]
    async fn lineage_enabled_at_creation_is_reported_by_describe() {
        let s = crate::common::start_stack().await;
        let mut c = client(&s.data_endpoint).await;
        c.allocate_pond(req(alloc("traced", true), "a"))
            .await
            .expect("allocate");

        assert_eq!(
            described(&s.data_endpoint, "traced").await["lineage"],
            serde_json::Value::Bool(true),
            "describe must report the lineage the pond was created with"
        );

        // ...and it is a property of the pond in the REGISTRY, not something the
        // node that served the allocate remembers. Re-read it from the control
        // plane, which never saw the original request, so this cannot pass on a
        // value cached in the node's memory.
        let mut cp = ControlClient::connect(s.control_endpoint.clone())
            .await
            .expect("control plane");
        let info = cp
            .get_pond_info(GetPondInfoRequest {
                pond_ref: "traced".into(),
            })
            .await
            .expect("get_pond_info")
            .into_inner()
            .pond
            .expect("pond info");
        assert!(
            info.lineage,
            "lineage must survive the round trip through the control-plane registry"
        );

        // Two ponds on one node differ: the flag is per pond, not per node.
        let mut c = client(&s.data_endpoint).await;
        c.allocate_pond(req(alloc("untraced", false), "a"))
            .await
            .expect("allocate");
        assert_eq!(
            described(&s.data_endpoint, "untraced").await["lineage"],
            serde_json::Value::Bool(false),
            "a second pond on the same node keeps its own setting"
        );
    }

    /// A DESIGN pin, not a behaviour test. Lineage is fixed for the pond's
    /// lifetime: turning it on later would leave a gap at the start of the
    /// pond's history that reads as "nothing happened" rather than "we were not
    /// recording" — worse than having no lineage at all. So there is
    /// deliberately NO enable/disable RPC on any surface, and this fails the
    /// build if one appears. Reversing it needs a deliberate backfill story.
    ///
    /// It pins the SETTER, not the word: `Data.GetLineage` reads the trail (and
    /// is what lets a node forward an agent's `get_lineage` to the pond's
    /// owner), which takes nothing away from the invariant. The guard was
    /// widened from "no rpc name contains lineage" to "no rpc name MUTATES
    /// lineage" for exactly that reason — more precise about the real property,
    /// not merely quieter.
    #[test]
    fn lineage_setting_is_fixed_for_the_pond_lifetime() {
        const DATA: &str = include_str!("../../latiq-proto/proto/latiq/v1/data.proto");
        const CONTROL: &str = include_str!("../../latiq-proto/proto/latiq/v1/control.proto");
        const ADMIN: &str = include_str!("../../latiq-proto/proto/latiq/v1/admin.proto");

        let mut rpcs = 0usize;
        for (file, src) in [
            ("data.proto", DATA),
            ("control.proto", CONTROL),
            ("admin.proto", ADMIN),
        ] {
            for line in src.lines() {
                let Some(rest) = line.trim().strip_prefix("rpc ") else {
                    continue;
                };
                rpcs += 1;
                let method = rest.split('(').next().unwrap_or_default().trim();
                let lower = method.to_ascii_lowercase();
                let mutates = ["set", "enable", "disable", "update", "toggle", "configure"]
                    .iter()
                    .any(|verb| lower.starts_with(verb));
                assert!(
                    !(lower.contains("lineage") && mutates),
                    "{file} declares `rpc {method}`: lineage is chosen at pond creation \
                     and fixed for the pond's lifetime — enabling it later would leave a \
                     hole at the start of the provenance record"
                );
            }
        }

        // Anti-vacuity (tests/CLAUDE.md rule 3): a guard that scanned nothing —
        // renamed files, a changed `rpc` spelling — would pass while guarding
        // nothing. Pin that every declared RPC was seen, and that the flag it is
        // guarding really is on the wire at creation time.
        assert_eq!(rpcs, 36, "every declared RPC must have been scanned");
        // The widened guard must still bite: a setter spelled any of the ways
        // above is refused. Without this the loosening could have been a
        // silent removal.
        for forbidden in ["SetLineage", "EnableLineage", "UpdateLineage"] {
            let lower = forbidden.to_ascii_lowercase();
            assert!(
                ["set", "enable", "disable", "update", "toggle", "configure"]
                    .iter()
                    .any(|verb| lower.starts_with(verb))
                    && lower.contains("lineage"),
                "the guard would not have caught `rpc {forbidden}`"
            );
        }
        assert!(
            DATA.contains("bool lineage"),
            "the Data surface must carry the lineage flag at allocate + describe"
        );
        assert!(
            CONTROL.contains("bool lineage"),
            "the Control surface must carry it too — the CLI's `pond create` goes \
             straight to the control plane, bypassing AgentOps"
        );
    }
}

/// Query timeouts on the Data surface. A node whose policy is milliseconds
/// rather than minutes, so the deadline can actually be observed inside a test's
/// wall clock — the mechanism is the same at either scale.
mod timeouts {
    use super::*;
    use common::start_stack_with_timeouts;
    use latiq_common::{ErrorKind, QueryTimeouts};

    /// Slow on purpose and cheap on purpose: a generated range, so nothing has
    /// to be written first and no table's size decides whether the test is
    /// flaky. It streams, so DuckDB's interrupt lands inside it.
    const SLOW_SQL: &str = "SELECT count(*) FROM range(0, 100000000000) t(i) WHERE i % 999983 = 0";

    fn timed(pond: &str, sql: &str, timeout_ms: u64) -> QueryRequest {
        QueryRequest {
            pond: pond.into(),
            sql: sql.into(),
            timeout_ms,
        }
    }

    async fn pond(c: &mut DataClient<tonic::transport::Channel>, name: &str) {
        c.allocate_pond(req(alloc(name), "a")).await.unwrap();
    }

    fn meta(json: &str) -> serde_json::Value {
        let v: serde_json::Value = serde_json::from_str(json).unwrap();
        v["_meta"].clone()
    }

    #[tokio::test]
    async fn cancellation_a_slow_read_times_out_and_names_both_numbers() {
        // 400ms default, 2000ms ceiling: the query below cannot finish in either.
        let s = start_stack_with_timeouts(QueryTimeouts::new(400, 2_000).unwrap()).await;
        let mut c = client(&s.data_endpoint).await;
        pond(&mut c, "slow").await;

        let started = std::time::Instant::now();
        let e = c
            .read_query(req(q("slow", SLOW_SQL), "a"))
            .await
            .expect_err("a query that cannot finish in 400 ms must be cut");
        let env = envelope(&e);
        assert_eq!(
            env.kind,
            ErrorKind::QueryTimeout,
            "not a generic engine error, and not query_cancelled: nobody cancelled this"
        );
        assert_eq!(
            e.code(),
            Code::DeadlineExceeded,
            "the gRPC code a client branches on"
        );
        // Both numbers, because both decide the agent's next move: what it got,
        // and how much more it is allowed to ask for.
        assert!(
            env.message.contains("400 ms"),
            "the message must name the timeout that was applied: {}",
            env.message
        );
        assert!(
            env.message.contains("2000 ms"),
            "the message must name the node's ceiling: {}",
            env.message
        );
        assert!(
            env.suggest.contains("timeout_ms") && env.suggest.contains("2000"),
            "below the ceiling, retrying with a larger timeout_ms is a real lever: {}",
            env.suggest
        );
        assert_eq!(env.see, "latiq://troubleshooting/timeouts");
        assert!(
            started.elapsed() < std::time::Duration::from_secs(20),
            "the deadline must actually cut the query, not merely be reported after it \
             finished on its own (took {:?})",
            started.elapsed()
        );

        // The real damage a botched interrupt does is a wedged pooled
        // connection. The next reader of the SAME pond must get a right answer.
        let ok = c
            .read_query(req(q("slow", "SELECT 41 + 1 AS v"), "a"))
            .await
            .expect("the pond's pooled connection must survive a timeout")
            .into_inner();
        let v: serde_json::Value = serde_json::from_str(&ok.json).unwrap();
        assert_eq!(v["rows"][0][0], 42);
    }

    #[tokio::test]
    async fn cancellation_a_slow_write_times_out_too() {
        let s = start_stack_with_timeouts(QueryTimeouts::new(400, 2_000).unwrap()).await;
        let mut c = client(&s.data_endpoint).await;
        pond(&mut c, "slowwrite").await;

        let e = c
            .write_query(req(
                q(
                    "slowwrite",
                    "CREATE TABLE t AS SELECT i FROM range(0, 100000000000) t(i) WHERE i % 999983 = 0",
                ),
                "a",
            ))
            .await
            .expect_err("the write path gets the same deadline as the read path");
        assert_eq!(envelope(&e).kind, ErrorKind::QueryTimeout);
        // A write cut mid-transaction is where a wedged connection would show:
        // the ROLLBACK runs against a connection that was just interrupted.
        let ok = c
            .write_query(req(
                q("slowwrite", "CREATE TABLE fine AS SELECT 1 AS a"),
                "a",
            ))
            .await
            .expect("the pond must still accept writes after a write timed out");
        assert!(!ok.into_inner().json.is_empty());
    }

    #[tokio::test]
    async fn cancellation_a_request_above_the_maximum_is_clamped_not_refused() {
        let s = start_stack_with_timeouts(QueryTimeouts::new(400, 2_000).unwrap()).await;
        let mut c = client(&s.data_endpoint).await;
        pond(&mut c, "clamped").await;

        // Half an hour, on a node that allows two seconds.
        let r = c
            .read_query(req(timed("clamped", "SELECT 1 AS v", 1_800_000), "a"))
            .await
            .expect("an over-max ask is CLAMPED, never refused — the query still runs")
            .into_inner();
        assert_eq!(
            meta(&r.json)["timeout_ms"],
            2_000,
            "_meta must report the ceiling that was applied, or the clamp is silent \
             and the agent is baffled when its query dies early"
        );
    }

    #[tokio::test]
    async fn cancellation_a_request_below_the_maximum_is_honoured_exactly() {
        let s = start_stack_with_timeouts(QueryTimeouts::new(400, 2_000).unwrap()).await;
        let mut c = client(&s.data_endpoint).await;
        pond(&mut c, "honoured").await;

        let r = c
            .read_query(req(timed("honoured", "SELECT 1 AS v", 777), "a"))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            meta(&r.json)["timeout_ms"],
            777,
            "a request inside the ceiling is applied verbatim — neither clamped \
             nor replaced by the node default"
        );

        // And with nothing asked for, the node's default is what applied.
        let r = c
            .read_query(req(q("honoured", "SELECT 1 AS v"), "a"))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(meta(&r.json)["timeout_ms"], 400, "the node's default");
    }

    #[tokio::test]
    async fn cancellation_a_write_reports_the_effective_timeout_too() {
        let s = start_stack_with_timeouts(QueryTimeouts::new(400, 2_000).unwrap()).await;
        let mut c = client(&s.data_endpoint).await;
        pond(&mut c, "wmeta").await;
        let r = c
            .write_query(req(
                timed("wmeta", "CREATE TABLE t AS SELECT 1 AS a", 999),
                "a",
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(meta(&r.json)["timeout_ms"], 999);
    }
}
