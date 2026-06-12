//! The Stream gRPC surface: server-streaming Arrow IPC for reads — for the SDK
//! (Arrow → pandas) and the internal node-to-node read forward. It shares the
//! Data port (plain tonic, no Flight), so nginx's data upstream already fronts
//! it and the forwarder reaches it at the same `internal_endpoint`.
//!
//! It encodes `AgentOps::read_arrow`'s `RecordBatch` stream into ONE Arrow IPC
//! stream (schema message first, then batches), chunked into `ArrowChunk`s —
//! nothing is buffered: each batch becomes a chunk as it arrives.
use crate::data_service::{identity_of, to_status};
use arrow::ipc::writer::StreamWriter;
use latiq_agent_core::{AgentOps, ArrowReadStream};
use latiq_proto::v1::stream_server::Stream as StreamSvc;
use latiq_proto::v1::{ArrowChunk, QueryRequest};
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status};

pub struct StreamService {
    ops: Arc<AgentOps>,
}

impl StreamService {
    pub fn new(ops: Arc<AgentOps>) -> Self {
        Self { ops }
    }
}

type ChunkStream = Pin<Box<dyn Stream<Item = Result<ArrowChunk, Status>> + Send>>;

#[tonic::async_trait]
impl StreamSvc for StreamService {
    type ReadArrowStream = ChunkStream;

    async fn read_arrow(
        &self,
        req: Request<QueryRequest>,
    ) -> Result<Response<Self::ReadArrowStream>, Status> {
        let id = identity_of(&req);
        let tid = crate::data_service::trace_id_of(&req);
        let r = req.into_inner();
        let ops = self.ops.clone();
        // Resolving the read (incl. pond-not-found / parse errors and the schema)
        // happens before we return — and so does any forward — so the trace id
        // must be ambient here; the batch encoding runs after, untraced.
        let read = crate::data_service::traced("read_arrow", tid, async move {
            ops.read_arrow(&id, &r.pond, &r.sql)
                .await
                .map_err(to_status)
        })
        .await?;
        let (tx, rx) = mpsc::channel::<Result<ArrowChunk, Status>>(4);
        tokio::spawn(encode_to_chunks(read, tx));
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

fn ipc_status(e: arrow::error::ArrowError) -> Status {
    Status::internal(format!("arrow ipc encode: {e}"))
}

/// Encode schema + batches into a single IPC stream, emitting each freshly-written
/// chunk. `get_mut` + `mem::take` drain what the writer just appended to its Vec.
async fn encode_to_chunks(read: ArrowReadStream, tx: mpsc::Sender<Result<ArrowChunk, Status>>) {
    let mut writer = match StreamWriter::try_new(Vec::<u8>::new(), read.schema.as_ref()) {
        Ok(w) => w,
        Err(e) => {
            let _ = tx.send(Err(ipc_status(e))).await;
            return;
        }
    };
    // try_new already wrote the schema message into the buffer.
    let schema_bytes = std::mem::take(writer.get_mut());
    if tx.send(Ok(ArrowChunk { ipc: schema_bytes })).await.is_err() {
        return;
    }

    let mut batches = read.batches;
    while let Some(b) = batches.next().await {
        match b {
            Ok(batch) => {
                if let Err(e) = writer.write(&batch) {
                    let _ = tx.send(Err(ipc_status(e))).await;
                    return;
                }
                let chunk = std::mem::take(writer.get_mut());
                if tx.send(Ok(ArrowChunk { ipc: chunk })).await.is_err() {
                    return;
                }
            }
            Err(ae) => {
                let _ = tx.send(Err(to_status(ae))).await;
                return;
            }
        }
    }
    if writer.finish().is_ok() {
        let tail = std::mem::take(writer.get_mut());
        if !tail.is_empty() {
            let _ = tx.send(Ok(ArrowChunk { ipc: tail })).await;
        }
    }
}
