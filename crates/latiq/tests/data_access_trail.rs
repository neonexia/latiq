//! Agent/client actions on the Data + Stream gRPC must land in the SAME
//! `latiq::access` stream operator actions do, with the SAME `outcome` field —
//! including the ones that FAIL and the ones that are turned away at the door.
//!
//! Before this, `AgentOps::audit` fired only after a success and the Data
//! surface's auth rejections recorded nothing, while the Admin surface recorded
//! every attempt with `outcome`. One searchable stream, two meanings: an
//! operator filtering it saw a complete picture of operator activity and a
//! systematically incomplete one of everything else.
//!
//! Its own test binary on purpose: capturing `tracing` output needs a subscriber
//! installed as the *process* default, because callsite interest is cached
//! process-wide the first time a callsite is hit. Same shape as
//! `admin_access_trail.rs` and `latiq-agent-core/tests/access_trail.rs`.
mod common;

use latiq_proto::v1::data_client::DataClient;
use latiq_proto::v1::stream_client::StreamClient;
use latiq_proto::v1::*;
use std::sync::{Arc, Mutex};
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

fn bearer_req<T>(msg: T, agent: &str, token: &str) -> Request<T> {
    let mut r = Request::new(msg);
    r.metadata_mut()
        .insert("latiq-agent-id", agent.parse().unwrap());
    r.metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    r
}

#[tokio::test]
async fn auth_data_surface_records_failures_and_rejections_like_admin_does() {
    let captured = CapturedLog::default();
    tracing::subscriber::set_global_default(
        tracing_subscriber::fmt()
            .with_writer(captured.clone())
            .with_max_level(tracing::Level::INFO)
            .with_ansi(false)
            .finish(),
    )
    .expect("this binary runs one test, so nothing else installs a subscriber");

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
    }))
    .await
    .unwrap_err();
    anon.read_query(bearer_req(
        QueryRequest {
            pond: "trail".into(),
            sql: "SELECT 1".into(),
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

    let allocated = access("op=\"allocate_pond\"");
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
    let failed = access("op=\"write_query\"");
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
    let bad_token = access("rejected: invalid token");
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
    let streamed = log
        .lines()
        .filter(|l| l.contains("latiq::access"))
        .find(|l| l.contains("op=\"read_arrow\""))
        .unwrap_or_else(|| panic!("a rejected read_arrow must be recorded: {log}"));
    assert!(
        streamed.contains("outcome=\"error\"") && streamed.contains("rejected: no bearer token"),
        "the Stream surface must record rejections like the Data surface: {streamed}"
    );
}
