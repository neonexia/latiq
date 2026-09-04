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

//! The `latiq::access` trail, captured.
//!
//! Every action -- operator (Admin gRPC) and agent/client (Data + Stream gRPC)
//! alike -- must land in the SAME searchable stream with the SAME field set and
//! the SAME `outcome`, including the ones that FAIL and the ones that are turned
//! away at the door. Succeeding is not enough: an unattributed `policy_set` is a
//! gap, and so is a `read_query` that recorded nothing because it was rejected.
//!
//! Before this, `AgentOps::audit` fired only after a success and the Data
//! surface's auth rejections recorded nothing, while the Admin surface recorded
//! every attempt with `outcome`. One searchable stream, two meanings: an
//! operator filtering it saw a complete picture of operator activity and a
//! systematically incomplete one of everything else.
//!
//! ## Why this is its own binary (and why the tests share it)
//!
//! Capturing `tracing` output needs a subscriber installed as the *process*
//! default, because callsite interest is cached process-wide the first time a
//! callsite is hit: a sibling test that touches `latiq::access` before any
//! subscriber exists caches "never", and the capture then sees an empty buffer.
//!
//! That argument buys ONE SUBSCRIBER PER BINARY, not one test per binary. The
//! tests below install the identical subscriber, so it is installed once behind
//! a `OnceLock` (`set_global_default` panics on a second call) and they share
//! the buffer, each searching for its own distinct needles. Every lookup is
//! narrowed by the RPC it is about AND by something only its own test emits --
//! its agent claim, its pond, its trace id. The RPC alone is not enough: two
//! tests both allocate a pond and both write, and `find` returns whichever
//! record the interleaving happened to put first. See also `rejected: invalid
//! token`, which both SURFACES emit within one test.
mod common;

use latiq_client::LatiqClient;
use latiq_proto::v1::admin_client::AdminClient;
use latiq_proto::v1::control_client::ControlClient;
use latiq_proto::v1::data_client::DataClient;
use latiq_proto::v1::stream_client::StreamClient;
use latiq_proto::v1::*;
use std::sync::{Arc, Mutex, OnceLock};
use tonic::Request;

/// A `tracing` writer that collects everything into a shared buffer.
#[derive(Clone, Default)]
struct CapturedLog(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for CapturedLog {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLog {
    type Writer = Self;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Install the capture subscriber exactly once, and hand back the shared buffer.
/// Called as the FIRST statement of every test in this binary, so it is always
/// in place before any `latiq::access` callsite is reached.
fn captured() -> CapturedLog {
    static CAPTURED: OnceLock<CapturedLog> = OnceLock::new();
    CAPTURED
        .get_or_init(|| {
            let captured = CapturedLog::default();
            tracing::subscriber::set_global_default(
                tracing_subscriber::fmt()
                    .with_writer(captured.clone())
                    .with_max_level(tracing::Level::INFO)
                    .with_ansi(false)
                    .finish(),
            )
            .expect("nothing else in this binary installs a subscriber");
            captured
        })
        .clone()
}

fn bearer_req<T>(msg: T, agent: &str, token: &str) -> Request<T> {
    let mut r = Request::new(msg);
    r.metadata_mut()
        .insert("latiq-agent-id", agent.parse().unwrap());
    r.metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    r
}

#[tokio::test]
async fn auth_admin_actions_are_attributed_in_the_access_trail() {
    let captured = captured();

    let idp = latiq_auth::test_support::TestIdp::start().await;
    let (_control, admin_endpoint) =
        common::start_control_plane_with_auth(Some(idp.auth_config())).await;
    let mut admin = AdminClient::connect(admin_endpoint.clone()).await.unwrap();
    let token = idp.mint("svc-ops", "latiq", &idp.issuer, 300);

    admin
        .policy_set(bearer_req(
            PolicySetRequest {
                key: "query_timeout_seconds".into(),
                value: "45".into(),
            },
            "opsbot",
            &token,
        ))
        .await
        .unwrap();
    admin
        .catalog_add(bearer_req(
            CatalogAddRequest {
                catalog: Some(CatalogMsg {
                    name: "audited".into(),
                    r#type: "iceberg".into(),
                    params: Default::default(),
                    description: String::new(),
                    tags: vec![],
                    created_by: String::new(),
                    created_at: String::new(),
                }),
            },
            "opsbot",
            &token,
        ))
        .await
        .unwrap();

    // A FAILING action, to prove the trail distinguishes it from a real one.
    admin
        .dataset_remove(bearer_req(
            DatasetRemoveRequest {
                name: "never-existed".into(),
            },
            "opsbot",
            &token,
        ))
        .await
        .unwrap_err();

    // ...and two rejected callers, who leave a record precisely BECAUSE they
    // were rejected: a surface whose job is "record who tried" must not go
    // silent on the attempts worth reading.
    let mut anon = AdminClient::connect(admin_endpoint.clone()).await.unwrap();
    anon.policy_get(PolicyGetRequest {}).await.unwrap_err();
    anon.policy_get(bearer_req(PolicyGetRequest {}, "intruder", "not-a-jwt"))
        .await
        .unwrap_err();

    let log = String::from_utf8(captured.0.lock().unwrap().clone()).unwrap();
    let access = |needle: &str| -> String {
        log.lines()
            .filter(|l| l.contains("latiq::access"))
            .find(|l| l.contains(needle))
            .unwrap_or_else(|| panic!("no access record matching {needle}: {log}"))
            .to_string()
    };
    let access_rpc = |rpc: &str, needle: &str| -> String {
        let op = format!("op={rpc:?}");
        log.lines()
            .filter(|l| l.contains("latiq::access"))
            .find(|l| l.contains(&op) && l.contains(needle))
            .unwrap_or_else(|| panic!("no {op} access record matching {needle}: {log}"))
            .to_string()
    };

    for op in ["policy_set", "catalog_add"] {
        let line = log
            .lines()
            .filter(|l| l.contains("latiq::access"))
            // Quoted because `op` is a string field -- exactly how `ops.rs`
            // renders it for agent actions.
            .find(|l| l.contains(&format!("op={op:?}")))
            .unwrap_or_else(|| panic!("operator action {op} must be on the access trail: {log}"));
        assert!(
            line.contains("outcome=\"ok\""),
            "a successful action must be marked as such: {line}"
        );
        assert!(
            line.contains("agent=opsbot"),
            "the claimed leaf must still be recorded: {line}"
        );
        assert!(
            line.contains("subject=svc-ops"),
            "the access trail must carry the verified subject: {line}"
        );
        assert!(
            line.contains(&format!("issuer={}", idp.issuer)),
            "the access trail must carry the issuer: {line}"
        );
        assert!(
            line.contains("verified=true"),
            "the access trail must mark the pair as verified: {line}"
        );
        // Same field set as `AgentOps::audit`, so one grep finds operator and
        // agent actions alike.
        assert!(
            line.contains("pond=\"-\"")
                && line.contains("duration_ms=")
                && line.contains("summary="),
            "operator records must carry the same fields as agent records: {line}"
        );
        // `trace_id` included, and `-` on purpose: an Admin call is answered by
        // the control plane alone and has no second record on another node to
        // be joined to. Omitting the field instead would make a `trace_id=`
        // grep skip operator actions entirely -- one searchable stream with two
        // field sets is two streams.
        assert!(
            line.contains("trace_id=\"-\""),
            "the operator twin must carry trace_id too: {line}"
        );
    }

    // A failed removal must NOT read like a successful one. Without `outcome`
    // the two records are byte-identical and the trail is confidently wrong.
    let failed = access("op=\"dataset_remove\"");
    assert!(
        failed.contains("outcome=\"error\""),
        "a failed action must be marked failed: {failed}"
    );
    assert!(
        failed.contains("dataset=never-existed"),
        "the attempted target is still recorded: {failed}"
    );

    let no_token = access("rejected: no token");
    assert!(
        no_token.contains("op=\"policy_get\"") && no_token.contains("outcome=\"error\""),
        "a tokenless attempt must name the RPC it targeted: {no_token}"
    );
    assert!(
        no_token.contains("verified=false") && no_token.contains("subject= "),
        "a rejected caller has no verified identity: {no_token}"
    );
    // Narrowed by RPC: the Data surface in this same binary also emits
    // "rejected: invalid token", and an unnarrowed `find` could return its line.
    let bad_token = access_rpc("policy_get", "rejected: invalid token");
    assert!(
        bad_token.contains("op=\"policy_get\"") && bad_token.contains("outcome=\"error\""),
        "a forged-token attempt must be recorded: {bad_token}"
    );
    assert!(
        bad_token.contains("agent=intruder") && bad_token.contains("verified=false"),
        "the claim is all a rejected caller has, and it is not authority: {bad_token}"
    );
}

#[tokio::test]
async fn auth_data_surface_records_failures_and_rejections_like_admin_does() {
    let captured = captured();

    let idp = latiq_auth::test_support::TestIdp::start().await;
    let stack = common::start_stack_with_auth(idp.auth_config()).await;
    let mut data = DataClient::connect(stack.data_endpoint.clone())
        .await
        .unwrap();
    let token = idp.mint("svc-analyst", "latiq", &idp.issuer, 300);

    // A successful action, to anchor the "ok" shape.
    data.allocate_pond(bearer_req(
        AllocatePondRequest {
            name: "trail".into(),
            policy_json: String::new(),
            tier: String::new(),
            lineage: false,
        },
        "agent-7",
        &token,
    ))
    .await
    .unwrap();

    // A FAILING action by an authenticated caller: the pond does not exist, so
    // the op dies at resolution. Recording only successes made this invisible.
    data.write_query(bearer_req(
        QueryRequest {
            pond: "never-existed".into(),
            sql: "CREATE TABLE t(i INTEGER)".into(),
            timeout_ms: 0,
        },
        "agent-7",
        &token,
    ))
    .await
    .unwrap_err();

    // A STREAMING read whose stream the client abandons without reading a byte.
    // The record must exist anyway, and must already be there by the time the
    // client could have consumed anything: the pond was opened, the query ran
    // and rows were flowing. This is what pins the establishment-time choice —
    // the record is complete before the consumer has any say in it.
    let mut stream_client = StreamClient::connect(stack.data_endpoint.clone())
        .await
        .unwrap();
    let abandoned = stream_client
        .read_arrow(bearer_req(
            QueryRequest {
                pond: "trail".into(),
                sql: "SELECT * FROM range(50000)".into(),
                timeout_ms: 0,
            },
            "agent-7",
            &token,
        ))
        .await
        .expect("the read is established before the first chunk is consumed");
    drop(abandoned);

    // A collected (non-streaming) read, which is the `read_query` RPC.
    data.read_query(bearer_req(
        QueryRequest {
            pond: "trail".into(),
            sql: "SELECT 1 AS one".into(),
            timeout_ms: 0,
        },
        "agent-7",
        &token,
    ))
    .await
    .unwrap();

    // ...and one that blows the inline cap. It is knowable ONLY after collecting
    // every row, so an `error` here is what proves `read_collected` records at
    // completion rather than at establishment (where it looked fine).
    data.read_query(bearer_req(
        QueryRequest {
            pond: "trail".into(),
            sql: "SELECT * FROM range(50000)".into(),
            timeout_ms: 0,
        },
        "agent-7",
        &token,
    ))
    .await
    .unwrap_err();

    // The two paths that reached the engine with no record at all until now.
    data.catalog_describe(bearer_req(
        CatalogDescribeRequest {
            pond: "never-existed".into(),
            catalog: "nope".into(),
            params: Default::default(),
        },
        "agent-7",
        &token,
    ))
    .await
    .unwrap_err();
    data.load_dataset(bearer_req(
        LoadDatasetRequest {
            pond: "trail".into(),
            dataset: "never-existed".into(),
        },
        "agent-7",
        &token,
    ))
    .await
    .unwrap_err();

    // A refused drop (no confirm): an ATTEMPT to delete a pond and all its data
    // is precisely the kind of thing an operator wants in the trail.
    data.drop_pond(bearer_req(
        DropPondRequest {
            pond: "trail".into(),
            confirm: false,
        },
        "agent-7",
        &token,
    ))
    .await
    .unwrap_err();

    // ...and callers turned away at the door, on both the Data and the Stream
    // surface. A surface whose job is "record who did what" must not go silent
    // on the attempts most worth reading.
    let mut anon = DataClient::connect(stack.data_endpoint.clone())
        .await
        .unwrap();
    anon.read_query(Request::new(QueryRequest {
        pond: "trail".into(),
        sql: "SELECT 1".into(),
        timeout_ms: 0,
    }))
    .await
    .unwrap_err();
    anon.read_query(bearer_req(
        QueryRequest {
            pond: "trail".into(),
            sql: "SELECT 1".into(),
            timeout_ms: 0,
        },
        "intruder",
        "not-a-jwt",
    ))
    .await
    .unwrap_err();
    let mut stream = StreamClient::connect(stack.data_endpoint.clone())
        .await
        .unwrap();
    stream
        .read_arrow(Request::new(QueryRequest {
            pond: "trail".into(),
            sql: "SELECT 1".into(),
            timeout_ms: 0,
        }))
        .await
        .unwrap_err();

    let log = String::from_utf8(captured.0.lock().unwrap().clone()).unwrap();
    let access = |needle: &str| -> String {
        log.lines()
            .filter(|l| l.contains("latiq::access"))
            .find(|l| l.contains(needle))
            .unwrap_or_else(|| panic!("no access record matching {needle}: {log}"))
            .to_string()
    };
    let access_rpc = |rpc: &str, needle: &str| -> String {
        let op = format!("op={rpc:?}");
        log.lines()
            .filter(|l| l.contains("latiq::access"))
            .find(|l| l.contains(&op) && l.contains(needle))
            .unwrap_or_else(|| panic!("no {op} access record matching {needle}: {log}"))
            .to_string()
    };

    // Narrowed by agent, not just RPC: another test in this binary allocates a
    // pond too, and its record would otherwise be a legal answer to this find.
    let allocated = access_rpc("allocate_pond", "agent=agent-7");
    assert!(
        allocated.contains("outcome=\"ok\""),
        "a successful action must be marked as such: {allocated}"
    );
    assert!(
        allocated.contains("subject=svc-analyst") && allocated.contains("verified=true"),
        "the verified subject is what makes the record authority: {allocated}"
    );

    // A failed write must NOT read like a successful one. Without `outcome` an
    // op that never touched the data is byte-identical to one that did.
    // Same reason, narrowed by the pond this test is about.
    let failed = access_rpc("write_query", "pond=\"never-existed\"");
    assert!(
        failed.contains("outcome=\"error\""),
        "a failed query must be marked failed: {failed}"
    );
    assert!(
        failed.contains("pond=\"never-existed\""),
        "the attempted target is still recorded: {failed}"
    );
    assert!(
        failed.contains("agent=agent-7") && failed.contains("subject=svc-analyst"),
        "a failed action is still attributed: {failed}"
    );

    // ---- the streaming read path, previously invisible entirely ------------
    let all = |needle: &str| -> Vec<String> {
        log.lines()
            .filter(|l| l.contains("latiq::access") && l.contains(needle))
            .map(|l| l.to_string())
            .collect()
    };

    // Narrowed by this test's own VERIFIED subject, not by the RPC alone: the
    // MCP trace test in this binary also reaches `read_arrow` (a forwarded MCP
    // read rides the Arrow hop), and its record would otherwise be a legal
    // answer to this find — the same trap the file's header documents.
    let streamed = access_rpc("read_arrow", "subject=svc-analyst");
    assert!(
        streamed.contains("outcome=\"ok\""),
        "an established stream is an access that happened, however it ends: {streamed}"
    );
    assert!(
        streamed.contains("subject=svc-analyst") && streamed.contains("verified=true"),
        "the bulk-read path must carry the verified reader: {streamed}"
    );
    // A locally-executed op records the resolved pond id, never the placeholder.
    assert!(
        streamed.contains("pond=\"") && !streamed.contains("pond=\"-\""),
        "the stream record must name the pond read: {streamed}"
    );
    // The redacted SQL shape is part of the documented field set, and a read
    // record without it does not say WHAT was read.
    assert!(
        streamed.contains("range"),
        "the redacted SQL shape must be recorded: {streamed}"
    );

    // `read_query` is recorded under the RPC the caller invoked, not under the
    // internal Arrow hop it rides — so it matches the rejection records above.
    let reads = all("op=\"read_query\"");
    assert!(
        reads.iter().any(|l| l.contains("outcome=\"ok\"")),
        "the successful collected read must be recorded: {reads:?}"
    );
    let capped = reads
        .iter()
        .find(|l| l.contains("outcome=\"error\""))
        .unwrap_or_else(|| {
            panic!("a read that blew the inline cap must be recorded as failed: {reads:?}")
        });
    assert!(
        capped.contains("range"),
        "the failed read still records what was attempted: {capped}"
    );

    let described = access("op=\"catalog_describe\"");
    assert!(
        described.contains("outcome=\"error\"") && described.contains("pond=\"never-existed\""),
        "an external-catalog describe must be on the trail: {described}"
    );
    let loaded = access("op=\"load_dataset\"");
    assert!(
        loaded.contains("outcome=\"error\"") && loaded.contains("summary=\"never-existed\""),
        "a dataset load must name the dataset it attempted: {loaded}"
    );

    let refused = access("op=\"drop_pond\"");
    assert!(
        refused.contains("outcome=\"error\"") && refused.contains("pond=\"trail\""),
        "a refused drop is an attempt worth recording: {refused}"
    );

    // The rejections, in the same shape the Admin surface already writes.
    let no_token = access("rejected: no bearer token");
    assert!(
        no_token.contains("op=\"read_query\"") && no_token.contains("outcome=\"error\""),
        "a tokenless attempt must name the RPC it targeted: {no_token}"
    );
    assert!(
        no_token.contains("verified=false") && no_token.contains("subject= "),
        "a rejected caller has no verified identity: {no_token}"
    );
    // Narrowed by RPC: the Admin surface in this same binary also emits
    // "rejected: invalid token", and an unnarrowed `find` could return its line.
    let bad_token = access_rpc("read_query", "rejected: invalid token");
    assert!(
        bad_token.contains("op=\"read_query\"") && bad_token.contains("outcome=\"error\""),
        "a forged-token attempt must be recorded: {bad_token}"
    );
    assert!(
        bad_token.contains("agent=intruder") && bad_token.contains("verified=false"),
        "the claim is all a rejected caller has, and it is not authority: {bad_token}"
    );
    // The Stream surface shares the guard, so it must share the record too --
    // otherwise the streaming read path (the SDK's primary one) is a blind spot.
    // Narrowed to the REJECTION: the same op now also has a successful record
    // above, and a filter that matched either would prove nothing.
    let rejected_stream = log
        .lines()
        .filter(|l| l.contains("latiq::access"))
        .find(|l| l.contains("op=\"read_arrow\"") && l.contains("rejected: no bearer token"))
        .unwrap_or_else(|| panic!("a rejected read_arrow must be recorded: {log}"));
    assert!(
        rejected_stream.contains("outcome=\"error\"") && rejected_stream.contains("verified=false"),
        "the Stream surface must record rejections like the Data surface: {rejected_stream}"
    );
}

/// A request tagged with the caller's own W3C `traceparent` and agent claim. No
/// token: this stack runs relaxed (the default), and identity is not what is
/// under test here.
///
/// The header is a real `traceparent` — the surfaces parse it strictly and mint
/// a FRESH trace for anything malformed, so a made-up id would make this test
/// pass or fail for reasons that have nothing to do with propagation.
fn traced_req<T>(msg: T, agent: &str, trace_id: &str) -> Request<T> {
    let mut r = Request::new(msg);
    r.metadata_mut()
        .insert("latiq-agent-id", agent.parse().unwrap());
    r.metadata_mut().insert(
        "traceparent",
        format!("00-{trace_id}-00f067aa0ba902b7-01")
            .parse()
            .unwrap(),
    );
    r
}

#[tokio::test]
async fn access_trail_trace_id_ties_a_forwarded_op_to_the_request_that_started_it() {
    // A forwarded op is AUDITED BY THE OWNER — the greeter returns before its
    // own audit, deliberately, so attribution stays on the node that ran the
    // query. That leaves the operator with a record on a node the client never
    // dialled, and until now nothing in it referred back to the request: the
    // access trail had no field an operator could follow across the hop.
    //
    // `trace_id` is that field. The client mints one, the greeter scopes it,
    // the forwarder replays it in the W3C `traceparent`, and the owner records
    // it — so the owner's record is reachable from the greeter's `forwarding to
    // owner node` log and from the client's own id.
    let captured = captured();
    let stack = common::start_stack_n(2).await;

    // 32 lowercase hex digits: a W3C trace-id. Distinct in the last digits so a
    // record carrying the wrong one is obvious in the failure message.
    const FORWARDED: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaa0001";
    const DIRECT: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbb0002";

    let mut n0 = DataClient::connect(stack.nodes[0].data_endpoint.clone())
        .await
        .unwrap();
    n0.allocate_pond(traced_req(
        AllocatePondRequest {
            name: "traced".into(),
            policy_json: String::new(),
            tier: String::new(),
            lineage: false,
        },
        "alice",
        "cccccccccccccccccccccccccccc0000",
    ))
    .await
    .unwrap();
    let owner = ControlClient::connect(stack.control_endpoint.clone())
        .await
        .unwrap()
        .get_pond_location(GetPondLocationRequest {
            pond_ref: "traced".into(),
        })
        .await
        .unwrap()
        .into_inner()
        .node_endpoint;
    let greeter = stack.other_than(&owner).data_endpoint.clone();
    assert_ne!(greeter, owner, "the request must actually cross a node hop");

    // Through the greeter: forwarded, and audited on the owner.
    DataClient::connect(greeter)
        .await
        .unwrap()
        .write_query(traced_req(
            QueryRequest {
                pond: "traced".into(),
                sql: "CREATE TABLE t(i INTEGER)".into(),
                timeout_ms: 0,
            },
            "alice",
            FORWARDED,
        ))
        .await
        .unwrap();
    // Straight to the owner: same pond, same op, a different id — so a trace_id
    // that were a constant, or copied from the wrong request, is visible.
    DataClient::connect(owner)
        .await
        .unwrap()
        .write_query(traced_req(
            QueryRequest {
                pond: "traced".into(),
                sql: "INSERT INTO t VALUES (1)".into(),
                timeout_ms: 0,
            },
            "alice",
            DIRECT,
        ))
        .await
        .unwrap();

    let log = String::from_utf8(captured.0.lock().unwrap().clone()).unwrap();
    let writes: Vec<&str> = log
        .lines()
        .filter(|l| l.contains("latiq::access") && l.contains("op=\"write_query\""))
        .filter(|l| l.contains("agent=alice"))
        .collect();
    assert_eq!(
        writes.len(),
        2,
        "one record per write, both on the owner: {log}"
    );
    for id in [FORWARDED, DIRECT] {
        let needle = format!("trace_id=\"{id}\"");
        let found: Vec<_> = writes.iter().filter(|l| l.contains(&needle)).collect();
        assert_eq!(
            found.len(),
            1,
            "exactly one record must carry {id}; the forwarded one proves the \
             caller's id survived the hop instead of the owner minting a fresh \
             one: {writes:?}"
        );
        assert!(
            found[0].contains("outcome=\"ok\""),
            "and it is the record of the write that landed: {}",
            found[0]
        );
    }
}

/// The join #101 exists for: ONE agent request, and the three records it leaves
/// behind, all carrying the same id — **on the forwarded path**, where the node
/// the agent dialled and the node that ran the query are different processes'
/// worth of logs.
///
/// This is the surface that had NO trace at all: `grep -i trace
/// crates/latiq-mcp/src/` returned zero hits, so every agent call logged
/// `trace_id="-"`, emitted `traceId: null` in its lineage, and could not be
/// joined to the node that answered it. Which is backwards — agents are the
/// audience the product is for, and the gRPC clients were the traced ones.
///
/// Nothing here is fabricated: a real MCP client sends a real `traceparent` to a
/// node that does not own the pond, the request really is forwarded, and the
/// three ids are read back out of a response, a captured log line and a lineage
/// file that the run itself produced.
#[tokio::test]
async fn access_trail_one_agent_request_joins_its_response_its_log_and_its_lineage() {
    let captured = captured();
    let stack = common::start_stack_n(2).await;

    // 32 lowercase hex digits each — real W3C trace ids, because the surfaces
    // parse strictly and mint a FRESH trace for anything malformed. A made-up
    // id would make this test pass or fail for reasons unrelated to propagation.
    const SETUP: &str = "11111111111111111111111111110001";
    const READ: &str = "22222222222222222222222222220002";
    const FAILED: &str = "33333333333333333333333333330003";
    const POND: &str = "traced-mcp";

    // Allocate WITH lineage (fixed at allocate; there is no switching it on) and
    // create a table, through node 0.
    let setup = LatiqClient::connect_traced(
        &stack.nodes[0].mcp_endpoint,
        Some("tracer".into()),
        None,
        Some(format!("00-{SETUP}-00f067aa0ba902b7-01")),
    )
    .await
    .unwrap();
    let mut args = serde_json::Map::new();
    args.insert("name".into(), POND.into());
    args.insert("lineage".into(), true.into());
    let allocated = setup.call_tool("allocate_pond", args).await.unwrap();
    assert!(
        !allocated.is_error,
        "allocate failed: {:?}",
        allocated.value
    );
    let out = setup
        .write(POND, "CREATE TABLE t(i INTEGER); INSERT INTO t VALUES (1)")
        .await
        .unwrap();
    assert!(!out.is_error, "write failed: {:?}", out.value);
    setup.close().await.unwrap();

    // Find the owner, and dial the OTHER node — so the read really crosses a hop.
    let owner = ControlClient::connect(stack.control_endpoint.clone())
        .await
        .unwrap()
        .get_pond_location(GetPondLocationRequest {
            pond_ref: POND.into(),
        })
        .await
        .unwrap()
        .into_inner()
        .node_endpoint;
    let greeter = stack.other_than(&owner);
    assert_ne!(
        greeter.data_endpoint, owner,
        "the agent must dial a node that does NOT own the pond, or the hop this \
         test is about never happens"
    );

    let agent = LatiqClient::connect_traced(
        &greeter.mcp_endpoint,
        Some("tracer".into()),
        None,
        Some(format!("00-{READ}-00f067aa0ba902b7-01")),
    )
    .await
    .unwrap();
    let read = agent.query(POND, "SELECT i FROM t").await.unwrap();
    assert!(!read.is_error, "read failed: {:?}", read.value);

    // 1. THE RESPONSE. The greeter answers the agent, so this is the greeter's
    //    view of the id — and it must be the agent's own, not a fresh one.
    let meta = &read.value["_meta"];
    assert_eq!(
        meta["trace_id"].as_str(),
        Some(READ),
        "the agent must be able to cite the id of its own request: {meta}"
    );
    assert_eq!(
        meta["served_by"].as_str(),
        Some(owner.as_str()),
        "and this must be the FORWARDED case — the owner ran it, the greeter \
         answered: {meta}"
    );
    // …and the SPAN, not only the trace. `trace_id` alone leaves a collector
    // with a flat set — every record of this request with no parent/child edge
    // between the greeter and the owner. `traceparent` is the parent a caller
    // building that tree attaches to, so it has to arrive on the wire, be
    // well-formed, and carry the caller's own trace.
    let read_tp = meta["traceparent"]
        .as_str()
        .unwrap_or_else(|| panic!("the result must carry a traceparent: {meta}"));
    let read_span = assert_traceparent(read_tp, READ);
    // OURS, never the caller's: a span id we echoed back would let a caller
    // forge our spans, and would nest the work under itself.
    assert_ne!(
        read_span, "00f067aa0ba902b7",
        "the span must be the node's own, not the one the client sent: {read_tp}"
    );

    // 2. THE LOGS, on BOTH SIDES of the hop — the half the issue is really
    //    about. The greeter and the owner are separate node instances with
    //    separate scopes; the only thing that can make them agree is the
    //    `traceparent` the greeter stamped on the forward.
    let log = String::from_utf8(captured.0.lock().unwrap().clone()).unwrap();

    // The GREETER: an MCP span it entered for this request (it entered none at
    // all before #101 — `grep -i trace crates/latiq-mcp/src/` was empty), on the
    // line where it decides to forward.
    let forwarded = log
        .lines()
        .filter(|l| l.contains("forwarding to owner node"))
        .find(|l| l.contains(&format!("mcp{{name=\"read_query\" trace_id={READ}}}")))
        .unwrap_or_else(|| panic!("the greeter's MCP span must carry {READ}:\n{log}"));
    assert!(
        forwarded.contains(&format!("endpoint=\"{owner}\"")),
        "and it must be forwarding to the pond's actual owner: {forwarded}"
    );

    // The OWNER: the access record, written by the node that ran the query (the
    // greeter returns before its own audit, so attribution stays there). Its op
    // is `read_arrow`, not `read_query` — a forwarded read rides the Arrow hop —
    // which is exactly why an operator needs the ID rather than the op name to
    // follow one request across the two.
    let record = log
        .lines()
        .filter(|l| l.contains("latiq::access") && l.contains("op=\"read_arrow\""))
        .find(|l| l.contains(&format!("trace_id=\"{READ}\"")))
        .unwrap_or_else(|| {
            panic!("no access record on the owner carries the agent's id {READ}:\n{log}")
        });
    assert!(
        record.contains("outcome=\"ok\"") && record.contains("agent=tracer"),
        "and it is the record of the read that landed: {record}"
    );
    assert!(
        record.contains("summary=\"SELECT i FROM t\""),
        "and of THIS read, not another test's: {record}"
    );

    // 3. THE LINEAGE, written by the owner's emitter from the same scope. Read
    //    it back through the agent's own tool rather than off disk: this is the
    //    id an agent asking "where did this come from?" actually gets.
    let mut page_args = serde_json::Map::new();
    page_args.insert("pond".into(), POND.into());
    let page = agent.call_tool("get_lineage", page_args).await.unwrap();
    assert!(!page.is_error, "get_lineage failed: {:?}", page.value);
    let events = page.value["events"]
        .as_array()
        .unwrap_or_else(|| panic!("a lineage page has events: {}", page.value));
    let traced: Vec<&serde_json::Value> = events
        .iter()
        .filter(|e| e["run"]["facets"]["latiq_query"]["traceId"].as_str() == Some(READ))
        .collect();
    assert!(
        !traced.is_empty(),
        "the read's lineage must carry the agent's id — it used to be null for \
         every MCP call: {events:#?}"
    );
    assert!(
        traced
            .iter()
            .any(|e| e["run"]["facets"]["latiq_query"]["op"].as_str() == Some("read_arrow")),
        "and specifically the run of THIS read — recorded under the op the owner \
         actually executed, which is the Arrow hop: {traced:#?}"
    );

    // 4. A FAILED call cites its id too. An agent that cannot name the request
    //    that failed cannot ask anyone about it — which is the whole reason the
    //    field is on the envelope and not only in our logs.
    let failing = LatiqClient::connect_traced(
        &greeter.mcp_endpoint,
        Some("tracer".into()),
        None,
        Some(format!("00-{FAILED}-00f067aa0ba902b7-01")),
    )
    .await
    .unwrap();
    let err = failing
        .query(POND, "SELECT * FROM no_such_table")
        .await
        .unwrap();
    assert!(err.is_error, "the statement must fail: {:?}", err.value);
    assert_eq!(
        err.value["kind"].as_str(),
        Some("catalog_error"),
        "asserting the kind, so this cannot pass for an unrelated failure: {}",
        err.value
    );
    assert_eq!(
        err.value["trace_id"].as_str(),
        Some(FAILED),
        "the envelope must carry the failed request's own id: {}",
        err.value
    );
    // A different id from the successful read, so a constant — or an id copied
    // from the wrong request — is visible here.
    assert_ne!(err.value["trace_id"].as_str(), Some(READ));
    // The envelope carries the span too, under the failed request's own trace.
    let err_tp = err.value["traceparent"]
        .as_str()
        .unwrap_or_else(|| panic!("the envelope must carry a traceparent: {}", err.value));
    let err_span = assert_traceparent(err_tp, FAILED);
    assert_ne!(
        err_span, read_span,
        "two requests are two spans — a constant here would join everything to \
         one node's work: {err_tp} vs {read_tp}"
    );

    agent.close().await.unwrap();
    failing.close().await.unwrap();
}

/// A W3C `traceparent` we returned: version `00`, the expected trace id, a real
/// 16-hex span, and the sampled flag we propagate. Returns the span id.
///
/// Asserted field by field rather than with `starts_with`, because the value's
/// whole purpose is to be parsed by somebody else's collector — a header that is
/// nearly right is a header that silently joins nothing.
fn assert_traceparent(header: &str, trace_id: &str) -> String {
    let parts: Vec<&str> = header.split('-').collect();
    assert_eq!(
        parts.len(),
        4,
        "a version-00 traceparent has 4 fields: {header}"
    );
    assert_eq!(parts[0], "00", "we emit the version we implement: {header}");
    assert_eq!(
        parts[1], trace_id,
        "the traceparent must carry the SAME trace as `trace_id`, or the two \
         halves of one join disagree: {header}"
    );
    assert_eq!(parts[2].len(), 16, "span id is 16 hex digits: {header}");
    assert!(
        parts[2]
            .chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()),
        "the spec is lowercase hex only: {header}"
    );
    assert_eq!(parts[3], "01", "the caller sent sampled=01: {header}");
    parts[2].to_string()
}
