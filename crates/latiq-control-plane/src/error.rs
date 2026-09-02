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

//! Control-plane error type.
use latiq_common::{ErrorEnvelope, ErrorKind};
use tonic::{Code, Status};

/// Registry-level failures. Each variant maps to one `ErrorKind` + gRPC code via
/// `envelope()`/`to_status`, so the in-process and over-the-wire paths give a
/// caller identical guidance.
#[derive(Debug, thiserror::Error)]
pub enum ControlPlaneError {
    #[error("pond name already exists: {0}")]
    NameConflict(String),
    #[error("pond not found: {0}")]
    PondNotFound(String),
    #[error("node not found: {0}")]
    NodeNotFound(String),
    /// No active node is available to host a pond (allocate-time availability),
    /// distinct from a node-lookup miss — different gRPC code (review #13).
    #[error("no pond node available: {0}")]
    NoNodeAvailable(String),
    #[error("dataset not found: {0}")]
    DatasetNotFound(String),
    #[error("catalog not found: {0}")]
    CatalogNotFound(String),
    /// `forget_pond` on a pond a live node is still serving — the operator
    /// wants `pond drop`, which deletes the data instead of orphaning it.
    #[error("pond '{pond}' is still owned by active node {node_id}")]
    PondStillOwned { pond: String, node_id: String },
    /// A pond was placed on a node that could not materialise its storage, so
    /// **there is no pond**. `compensated` says whether the registry row was
    /// successfully given back — the whole difference between "retry freely" and
    /// "an operator has work to do".
    #[error("pond '{name}' could not be materialized on {owner}: {cause}")]
    AllocationNotMaterialized {
        name: String,
        owner: String,
        cause: String,
        compensated: bool,
    },
    #[error("invalid request: {0}")]
    Invalid(String),
    #[error("storage error: {0}")]
    Storage(String),
}

impl ControlPlaneError {
    /// Guidance-first envelope for this error — the same `ErrorEnvelope` the Data
    /// gRPC and CLI already speak, built from the central per-kind defaults
    /// (`latiq-common`). The `{0}` payloads are bare refs (e.g. a pond name), so
    /// we phrase a real message here instead of leaking the ref as the message.
    pub fn envelope(&self) -> ErrorEnvelope {
        match self {
            ControlPlaneError::NameConflict(name) => {
                ErrorEnvelope::for_kind(ErrorKind::NameConflict, format!("Name '{name}' is taken."))
            }
            ControlPlaneError::PondNotFound(r) => ErrorEnvelope::for_kind(
                ErrorKind::PondNotFound,
                format!("Pond '{r}' does not exist."),
            ),
            // No node available to host the pond is an availability/precondition
            // failure, not a missing pond — so it is NOT PondNotFound (review #13).
            ControlPlaneError::NoNodeAvailable(m) => ErrorEnvelope::new(
                ErrorKind::Internal,
                format!("No pond node is available: {m}"),
                "Ensure a pond node is registered and healthy with the control plane, then retry.",
                "latiq://troubleshooting",
            ),
            // A node-lookup miss (e.g. `node describe <bad-id>`) — the node simply
            // isn't registered; this is a not-found, not an outage.
            ControlPlaneError::NodeNotFound(n) => ErrorEnvelope::new(
                ErrorKind::Internal,
                format!("Node '{n}' is not registered."),
                "Run `latiq node list` to see registered nodes.",
                "latiq://troubleshooting",
            ),
            ControlPlaneError::DatasetNotFound(r) => ErrorEnvelope::for_kind(
                ErrorKind::DatasetNotFound,
                format!("Dataset '{r}' is not in the catalog."),
            ),
            ControlPlaneError::CatalogNotFound(r) => ErrorEnvelope::new(
                ErrorKind::DatasetNotFound,
                format!("Catalog '{r}' is not registered."),
                "Call list_catalogs to see registered catalogs.",
                "latiq://guidance",
            ),
            // Not an argument the operator can correct — the pond and the verb
            // are both fine, the CLUSTER is in a state this verb is not for.
            ControlPlaneError::PondStillOwned { pond, node_id } => ErrorEnvelope::new(
                ErrorKind::InvalidValue,
                format!(
                    "Pond '{pond}' is still owned by node '{node_id}', which is registered and \
                     active — forgetting it would orphan data a live node is serving."
                ),
                "Use `latiq pond drop <pond> --confirm`, which deletes the pond AND its data via \
                 the owning node. `pond forget` is only for a pond whose node is gone.",
                "latiq://guidance",
            ),
            // Same `PondUnavailable` kind as the stranded-pond refusal, and for
            // the same underlying reason (the node that should hold this pond's
            // files cannot be reached) — but with its own message and `suggest`,
            // because the kind's default advice is `pond forget`, which is wrong
            // here: the registry row is normally already gone and the caller's
            // next move is to retry, not to fetch an operator.
            //
            // `compensated` is stated in the MESSAGE rather than left for the
            // reader to infer, because a retrying caller has to know whether the
            // failed attempt left a row behind holding its name.
            ControlPlaneError::AllocationNotMaterialized {
                name,
                owner,
                cause,
                compensated: true,
            } => ErrorEnvelope::new(
                ErrorKind::PondUnavailable,
                format!(
                    "Pond '{name}' was NOT created: the node it was assigned to ({owner}) could \
                     not materialise its storage ({cause}). The assignment has been rolled back, \
                     so the name is free and nothing was left behind."
                ),
                "Retry allocate_pond (or `latiq pond create`) — the failed attempt left nothing \
                 behind, so the same name is free. If it keeps failing, that node is down: report \
                 it to your operator.",
                ErrorKind::PondUnavailable.default_see(),
            ),
            ControlPlaneError::AllocationNotMaterialized {
                name,
                owner,
                cause,
                compensated: false,
            } => ErrorEnvelope::new(
                ErrorKind::PondUnavailable,
                format!(
                    "Pond '{name}' was NOT created: the node it was assigned to ({owner}) could \
                     not materialise its storage ({cause}), AND the assignment could not be \
                     rolled back — a registry row named '{name}' may still exist with no storage \
                     behind it."
                ),
                "Retry under a DIFFERENT name; the original may still be taken. Ask an operator \
                 to remove the stranded record with `latiq pond forget <pond> --confirm` (it \
                 deletes the registry row only, never data).",
                ErrorKind::PondUnavailable.default_see(),
            ),
            ControlPlaneError::Invalid(m) => {
                ErrorEnvelope::for_kind(ErrorKind::InvalidValue, m.clone())
            }
            ControlPlaneError::Storage(m) => {
                ErrorEnvelope::for_kind(ErrorKind::Storage, format!("storage error: {m}"))
            }
        }
    }

    /// The gRPC code for this error. `NoNodeAvailable` is a precondition (no host
    /// to place a pond), distinct from `NodeNotFound` (a lookup miss = NotFound).
    fn code(&self) -> Code {
        match self {
            ControlPlaneError::NameConflict(_) => Code::AlreadyExists,
            ControlPlaneError::PondNotFound(_)
            | ControlPlaneError::DatasetNotFound(_)
            | ControlPlaneError::CatalogNotFound(_)
            | ControlPlaneError::NodeNotFound(_) => Code::NotFound,
            // The request was well-formed and the placement was fine; the
            // CLUSTER could not carry it out. Same code the Data surface gives
            // `PondUnavailable`, so a client branches on one thing across both.
            ControlPlaneError::NoNodeAvailable(_)
            | ControlPlaneError::PondStillOwned { .. }
            | ControlPlaneError::AllocationNotMaterialized { .. } => Code::FailedPrecondition,
            ControlPlaneError::Invalid(_) => Code::InvalidArgument,
            ControlPlaneError::Storage(_) => Code::Internal,
        }
    }
}

/// Map a control-plane error to a tonic `Status` carrying the `ErrorEnvelope` in
/// `details` (same contract as the Data gRPC), so the CLI renders guidance — not
/// a bare ref — on every Control/Admin call.
pub fn to_status(e: ControlPlaneError) -> Status {
    let code = e.code();
    let env = e.envelope();
    let details = serde_json::to_vec(&env).unwrap_or_default();
    Status::with_details(code, env.message.clone(), details.into())
}

impl From<duckdb::Error> for ControlPlaneError {
    fn from(e: duckdb::Error) -> Self {
        ControlPlaneError::Storage(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pond_not_found_envelope_is_guidance_first_not_bare_ref() {
        let env = ControlPlaneError::PondNotFound("ridex".into()).envelope();
        assert_eq!(env.kind, ErrorKind::PondNotFound);
        assert_eq!(env.message, "Pond 'ridex' does not exist."); // not just "ridex"
        assert!(env.suggest.contains("list_ponds"));
        assert_eq!(env.see, "latiq://troubleshooting/pond-not-found");
    }

    #[test]
    fn to_status_attaches_decodable_envelope_and_preserves_codes() {
        // The CLI decodes Status.details — it must round-trip to the envelope.
        let st = to_status(ControlPlaneError::PondNotFound("x".into()));
        assert_eq!(st.code(), Code::NotFound);
        let env: ErrorEnvelope = serde_json::from_slice(st.details()).unwrap();
        assert_eq!(env.kind, ErrorKind::PondNotFound);

        // No-node-available (allocate-time) is a precondition (review #13).
        let st = to_status(ControlPlaneError::NoNodeAvailable("none".into()));
        assert_eq!(st.code(), Code::FailedPrecondition);
        let env: ErrorEnvelope = serde_json::from_slice(st.details()).unwrap();
        assert_eq!(env.kind, ErrorKind::Internal);
        assert!(env.message.contains("No pond node is available"));

        // A node-lookup miss (describe a bad id) is NotFound, not a precondition.
        let st = to_status(ControlPlaneError::NodeNotFound("bad-id".into()));
        assert_eq!(st.code(), Code::NotFound);
        let env: ErrorEnvelope = serde_json::from_slice(st.details()).unwrap();
        assert!(env.message.contains("'bad-id' is not registered"));
    }
}
