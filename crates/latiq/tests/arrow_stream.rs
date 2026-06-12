//! Full-stack Arrow streaming: a standard gRPC client (the SDK's shape) calling
//! the Stream service `ReadArrow`, decoding the Arrow IPC chunks with a stock
//! `StreamDecoder` (exactly what `pyarrow.ipc` does). Driven against the NON-owner
//! node, so it also exercises the node-to-node forward and the past-the-cap
//! streaming the unary JSON path can't do.
mod common;

use arrow::buffer::Buffer;
use arrow::ipc::reader::StreamDecoder;
use common::{start_stack_n, MultiStack};
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
        },
        "a",
    ))
    .await
    .unwrap();
    d0.write_query(req(
        QueryRequest {
            pond: pond.into(),
            sql: format!("CREATE TABLE t AS SELECT * FROM range({rows}) r(i)"),
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
