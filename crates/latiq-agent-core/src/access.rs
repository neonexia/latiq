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

//! The `latiq::access` trace target — the operator's record of who did what.
//!
//! There is no audit store: operators grep the node log files (or ship them to
//! their log stack; `LATIQ_LOG_FORMAT=json` makes the fields structured). Filter
//! with e.g. `RUST_LOG=latiq::access=info`, or by grepping the `latiq::access`
//! target / the `op=`/`pond=` fields.
//!
//! To follow ONE request, filter on `trace_id=` — the W3C trace id, 32 hex
//! digits, the same value the request's `traceparent` carried and the same one
//! its lineage events and its error envelope report. It is the field that makes
//! the trail work in a cluster: a request that lands on a node which does not
//! own the pond is forwarded, and it is the OWNER that records it — the greeter
//! returns before its own audit, so attribution stays on the node that ran the
//! op. That leaves a record on a node the client never dialled, and the trace
//! id is the only thing tying it back to the request that caused it (every
//! outbound hop stamps `traceparent`, so the greeter's spans and the owner's
//! record agree). `-` where no trace scope is in force: the Data/Stream
//! surfaces' auth rejections, which are recorded at the door — before the
//! handler enters the trace scope — and so cannot be followed.
//!
//! To ask *who* did something, filter on `subject=` **together with**
//! `verified=true`: `agent=` is the caller's own claim and carries no authority
//! (it is empty of meaning for authorization, useful only for correlating one
//! agent's activity). `subject=`/`issuer=` are empty when `verified=false`.
//!
//! The emitter lives here, in the protocol-neutral core, rather than in each
//! surface, because the trail is only searchable if every producer writes the
//! SAME fields with the SAME meaning: `AgentOps` and the pond node's Data/Stream
//! adapter both call this. (The control plane's Admin surface keeps a local twin
//! — it holds no `AgentOps` — whose fields are deliberately identical.)
use latiq_common::Identity;

/// The `outcome` field's two values. An audit record that does not say whether
/// the action LANDED is worse than none: a rejected `drop_pond` would read
/// byte-identically to a real one.
pub const OK: &str = "ok";
pub const ERROR: &str = "error";

/// `ok`/`error` for a completed op.
pub fn outcome<T, E>(res: &Result<T, E>) -> &'static str {
    if res.is_ok() {
        OK
    } else {
        ERROR
    }
}

/// Emit one access record. `pond` is `-` where the action is not about one pond
/// (or where it never got far enough to resolve one).
///
/// The trace id is read from the ambient scope rather than passed in: every
/// caller would otherwise have to remember to thread it, and a producer that
/// forgot would emit a record that looks complete and is unjoinable.
pub fn record(
    identity: &Identity,
    op: &str,
    pond: Option<&str>,
    summary: Option<&str>,
    duration_ms: u64,
    outcome: &str,
) {
    let trace_id = crate::trace::current_trace_id();
    tracing::info!(
        target: "latiq::access",
        agent = %identity.agent_id,          // CLAIMED. never authority.
        subject = %identity.subject,         // verified, or "" when not
        issuer = %identity.issuer,
        verified = identity.verified,        // scopes subject/issuer, NOT agent
        op,
        pond = pond.unwrap_or("-"),
        trace_id = trace_id.as_deref().unwrap_or("-"), // one request, across nodes
        duration_ms,
        summary = summary.unwrap_or(""),
        outcome,                             // ok | error — did it LAND?
        "access",
    );
}
