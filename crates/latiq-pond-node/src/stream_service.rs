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

//! The Stream gRPC surface: server-streaming Arrow IPC for reads — for the SDK
//! (Arrow → pandas) and the internal node-to-node read forward. It shares the
//! Data port (plain tonic, no Flight), so nginx's data upstream already fronts
//! it and the forwarder reaches it at the same `internal_endpoint`.
//!
//! It encodes `AgentOps::read_arrow`'s `RecordBatch` stream into ONE Arrow IPC
//! stream (schema message first, then batches), chunked into `ArrowChunk`s —
//! nothing is buffered: each batch becomes a chunk as it arrives.
use crate::data_service::{challenge_of, identity_of, to_status};
use arrow::ipc::writer::StreamWriter;
use latiq_agent_core::{AgentOps, ArrowReadStream};
use latiq_auth::Verifier;
use latiq_proto::v1::stream_server::Stream as StreamSvc;
use latiq_proto::v1::{ArrowChunk, QueryRequest};
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status};

/// The streaming half of the CLI/SDK surface, and the node-to-node read hop.
/// Serves the same data as [`crate::DataService`] to the same audience, so it
/// must be configured with the same verifier — forgetting it here leaves an
/// unauthenticated way to read every pond.
pub struct StreamService {
    ops: Arc<AgentOps>,
    verifier: Option<Arc<Verifier>>,
    /// The `WWW-Authenticate` value handed back on a rejection, built once.
    challenge: Option<String>,
}

impl StreamService {
    pub fn new(ops: Arc<AgentOps>) -> Self {
        Self {
            ops,
            verifier: None,
            challenge: None,
        }
    }

    /// Require verified bearer tokens on this surface — the Stream surface is
    /// the easy one to forget, and it reads the same data the Data surface does.
    pub fn with_verifier(mut self, verifier: Option<Arc<Verifier>>) -> Self {
        self.verifier = verifier;
        self
    }

    /// The RFC 9728 protected-resource metadata URL to advertise on a rejection —
    /// the same one the Data surface advertises; they share a port and an issuer.
    pub fn with_metadata_url(mut self, metadata_url: Option<&str>) -> Self {
        self.challenge = challenge_of(metadata_url);
        self
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
        let (id, tok) = identity_of(
            self.verifier.as_ref(),
            self.challenge.as_deref(),
            &req,
            "read_arrow",
        )
        .await?;
        let ctx = crate::data_service::trace_of(&req);
        let r = req.into_inner();
        let ops = self.ops.clone();
        // Resolving the read (incl. pond-not-found / parse errors and the schema)
        // happens before we return — and so does any forward — so the trace
        // context must be ambient here.
        let read = crate::data_service::traced("read_arrow", ctx.clone(), tok, async move {
            ops.read_arrow_with(
                &id,
                &r.pond,
                &r.sql,
                crate::data_service::controls_of(r.timeout_ms),
            )
            .await
            .map_err(to_status)
        })
        .await?;
        let (tx, rx) = mpsc::channel::<Result<ArrowChunk, Status>>(4);
        // RE-SCOPED, not merely spawned. The batch encoding runs after the
        // handler has returned, on a task of its own — task-locals do not cross
        // a `spawn` — so everything that can still go wrong here (an IPC encode
        // failure, and every mid-stream engine error the batches carry) used to
        // be emitted with no trace id at all. That is the half of a streaming
        // read most likely to need explaining, and it was the untraced half.
        tokio::spawn(latiq_agent_core::with_trace(
            ctx,
            encode_to_chunks(read, tx),
        ));
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
    // Who is serving this read rides the FIRST chunk, beside the schema: a
    // forwarding peer resolves its stream as soon as the schema is known, so a
    // name delivered any later would arrive after it had already answered.
    if tx
        .send(Ok(ArrowChunk {
            ipc: schema_bytes,
            served_by: read.served_by.clone(),
            traceparent: read.traceparent.clone(),
        }))
        .await
        .is_err()
    {
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
                // `served_by` and `traceparent` only on the first chunk (above):
                // repeating them on every batch would put a node name and a span
                // on the wire per batch for facts that cannot change mid-stream.
                if tx
                    .send(Ok(ArrowChunk {
                        ipc: chunk,
                        served_by: String::new(),
                        traceparent: String::new(),
                    }))
                    .await
                    .is_err()
                {
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
            let _ = tx
                .send(Ok(ArrowChunk {
                    ipc: tail,
                    served_by: String::new(),
                    traceparent: String::new(),
                }))
                .await;
        }
    }
}
