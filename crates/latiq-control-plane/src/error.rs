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
use latiq_common::{facts, ErrorEnvelope, ErrorKind};
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
            ControlPlaneError::NameConflict(name) => ErrorEnvelope::rendered(
                ErrorKind::NameConflict,
                "Name '{name}' is taken.",
                facts! { "name" => name.as_str() },
            ),
            ControlPlaneError::PondNotFound(r) => ErrorEnvelope::rendered(
                ErrorKind::PondNotFound,
                "Pond '{pond}' does not exist.",
                facts! { "pond" => r.as_str() },
            ),
            // No node available to host the pond is an availability/precondition
            // failure, not a missing pond — so it is NOT PondNotFound (review #13).
            //
            // The KIND is still `internal`, and that is an open question rather
            // than an endorsement: nothing of ours crashed, so `internal`'s "a
            // fault of ours" reading is not strictly true. But no shipped kind
            // fits either — `pond_unavailable` is about a pond that EXISTS (none
            // does here), and `capability_unavailable` is explicitly scoped to a
            // provisioned capability and carries `latiq warm-extensions` advice
            // that would be wrong. Its two control fields are already right for
            // this failure (`operator` fixes it; `as_is` is honest, because a
            // node registering a moment later makes the identical call succeed),
            // so re-kinding it is a decision to take deliberately, not in passing.
            //
            // What DID change: the operator-facing instruction moved from
            // `suggest` into `message`. An `internal` envelope carrying bespoke
            // advice is an envelope whose four control fields disagree with its
            // prose (see `assert_opaque_kinds_keep_canonical_guidance`), and the
            // advice loses nothing by being prose — it was never a *next call*,
            // which is what `suggest` is for. The old `see` pointed at
            // `latiq://troubleshooting`, the index, which taught nothing.
            ControlPlaneError::NoNodeAvailable(m) => ErrorEnvelope::for_kind(
                ErrorKind::Internal,
                format!(
                    "No pond node is available to host a pond: {m}. A pond node must be \
                     registered and heartbeating with this control plane before one can be \
                     created — `latiq node list` shows which are registered."
                ),
            ),
            // A node-lookup miss (e.g. `node describe <bad-id>`) — the node
            // simply isn't registered; this is a not-found, not an outage.
            //
            // Deliberately NOT `Internal`, which it was. Nothing of ours failed:
            // the caller named a node id that does not exist, and the fix is a
            // different id. `internal` made the envelope contradict itself —
            // `audience: operator` and `retryable: as_is` (re-send the identical
            // wrong id, then wake somebody) sitting next to a `suggest` that
            // names a call the CALLER makes to find the right one. `InvalidValue`
            // is `agent` + `after_change`, which is what the suggest below has
            // always described. There is no `node_not_found` kind and this does
            // not earn one: the value is wrong and the caller corrects it, which
            // is the whole of what `invalid_value` means.
            //
            // The gRPC code is unchanged (`code()` still says `NotFound`) — that
            // is the transport's answer to "was there a record", a different
            // question from "who fixes this".
            ControlPlaneError::NodeNotFound(n) => ErrorEnvelope::rendered_with(
                ErrorKind::InvalidValue,
                "Node '{node_id}' is not registered with this control plane.",
                facts! { "node_id" => n.as_str() },
                "Run `latiq node list` to see the registered nodes, then retry with an id from \
                 that list.",
                ErrorKind::InvalidValue.default_see(),
            ),
            ControlPlaneError::DatasetNotFound(r) => ErrorEnvelope::rendered(
                ErrorKind::DatasetNotFound,
                "Dataset '{dataset}' is not in the catalog.",
                facts! { "dataset" => r.as_str() },
            ),
            ControlPlaneError::CatalogNotFound(r) => ErrorEnvelope::rendered_with(
                ErrorKind::DatasetNotFound,
                "Catalog '{catalog}' is not registered.",
                facts! { "catalog" => r.as_str() },
                "Call list_catalogs to see registered catalogs.",
                "latiq://guidance",
            ),
            // Not an argument the operator can correct — the pond and the verb
            // are both fine, the CLUSTER is in a state this verb is not for.
            ControlPlaneError::PondStillOwned { pond, node_id } => ErrorEnvelope::rendered_with(
                ErrorKind::InvalidValue,
                "Pond '{pond}' is still owned by node '{node_id}', which is registered and active \
                 — forgetting it would orphan data a live node is serving.",
                facts! { "pond" => pond.as_str(), "node_id" => node_id.as_str() },
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
            } => ErrorEnvelope::rendered_with(
                ErrorKind::PondUnavailable,
                "Pond '{name}' was NOT created: the node it was assigned to ({node_endpoint}) \
                 could not materialise its storage ({cause}). The assignment has been rolled \
                 back, so the name is free and nothing was left behind.",
                facts! {
                    "name" => name.as_str(),
                    "node_endpoint" => owner.as_str(),
                    "cause" => cause.as_str(),
                    "compensated" => true,
                },
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
            } => ErrorEnvelope::rendered_with(
                ErrorKind::PondUnavailable,
                "Pond '{name}' was NOT created: the node it was assigned to ({node_endpoint}) \
                 could not materialise its storage ({cause}), AND the assignment could not be \
                 rolled back — a registry row named '{name}' may still exist with no storage \
                 behind it.",
                facts! {
                    "name" => name.as_str(),
                    "node_endpoint" => owner.as_str(),
                    "cause" => cause.as_str(),
                    "compensated" => false,
                },
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
    to_status_traced(e, None)
}

/// As [`to_status`], stamping the request's trace id on the envelope so the
/// caller can cite the id of its own failed call.
///
/// A separate entry point rather than an ambient read: the control plane keeps
/// no trace scope (see `trace_meta`), so the id has to arrive from the handler
/// that read it off the request.
pub fn to_status_traced(e: ControlPlaneError, trace_id: Option<String>) -> Status {
    let code = e.code();
    let env = e
        .envelope()
        .with_trace_id(trace_id.filter(|t| !t.is_empty()));
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

    /// Every `ControlPlaneError`, once — the list this file's other tests
    /// spot-check three of.
    ///
    /// The three `internal` defects this repo has shipped were all found by a
    /// person, never by a guard, and the reason is structural: every existing
    /// `error_contract_*` guard iterates `ErrorKind::ALL` and inspects the KIND
    /// TABLE (`default_suggest`, `audience`, `retryable`). None of them looks at
    /// a constructed envelope. But a construction site chooses its own kind and
    /// may override `suggest`/`see`, so the entire class of bug — an envelope
    /// whose control fields contradict its own prose — is invisible to a guard
    /// over the taxonomy. `NodeNotFound` sat here as `internal` (`operator`,
    /// `as_is`) beside a `suggest` naming a call the CALLER makes, and every
    /// kind-table guard was green the whole time.
    ///
    /// So this drives the real `envelope()` for every variant and asserts the
    /// coherence rules the kind table is already held to, at the layer where
    /// they can actually be broken.
    #[test]
    fn error_contract_every_control_plane_error_builds_a_coherent_envelope() {
        use latiq_common::{Audience, Retryable};

        let cases = [
            ControlPlaneError::NameConflict("taken".into()),
            ControlPlaneError::PondNotFound("ridex".into()),
            ControlPlaneError::NodeNotFound("bad-id".into()),
            ControlPlaneError::NoNodeAvailable("no active nodes".into()),
            ControlPlaneError::DatasetNotFound("tpch".into()),
            ControlPlaneError::CatalogNotFound("lake".into()),
            ControlPlaneError::PondStillOwned {
                pond: "p1".into(),
                node_id: "n1".into(),
            },
            ControlPlaneError::AllocationNotMaterialized {
                name: "p1".into(),
                owner: "http://n1:7002".into(),
                cause: "connect error".into(),
                compensated: true,
            },
            ControlPlaneError::AllocationNotMaterialized {
                name: "p1".into(),
                owner: "http://n1:7002".into(),
                cause: "connect error".into(),
                compensated: false,
            },
            ControlPlaneError::Invalid("tier 'huge' is not one of small/medium/large".into()),
            ControlPlaneError::Storage("disk full".into()),
        ];
        // Anti-vacuity: every variant is driven (both `compensated` shapes,
        // because they build different envelopes). A variant added without a
        // guidance decision fails here rather than shipping.
        assert_eq!(cases.len(), 11, "a ControlPlaneError variant is undriven");

        let mut operator = 0;
        let mut agent = 0;
        for e in cases {
            let label = format!("{e:?}");
            let env = e.envelope();

            // 1. `audience` must agree with the advice actually attached to THIS
            //    envelope — not with the advice its kind would have defaulted to.
            match env.audience {
                Audience::Operator => {
                    operator += 1;
                    assert!(
                        env.suggest.contains("operator"),
                        "{label}: nobody the caller can reach fixes this, so the advice must \
                         name who does: {}",
                        env.suggest
                    );
                }
                Audience::Agent => {
                    agent += 1;
                    assert!(
                        !env.suggest.starts_with("Retry; if it persists"),
                        "{label}: this is the caller's to fix, so the advice must not be the \
                         operator hand-off: {}",
                        env.suggest
                    );
                }
            }

            // 2. An unclassified failure may not carry classified advice. This is
            //    the funnel invariant (`latiq-common`'s
            //    `assert_opaque_kinds_keep_canonical_guidance`) restated where a
            //    reader of this file will meet it: `internal` means we do not
            //    know, and a site that knows the next step has classified the
            //    failure and owes it a kind.
            if env.kind == ErrorKind::Internal {
                assert_eq!(
                    env.suggest,
                    ErrorKind::Internal.default_suggest(),
                    "{label}"
                );
                assert_eq!(env.retryable, Retryable::AsIs, "{label}");
                assert_eq!(env.audience, Audience::Operator, "{label}");
            }

            // 3. `see` must teach about THIS failure. `latiq://troubleshooting`
            //    is the index: it resolves, so a "does the resource exist" guard
            //    stays green while the reader lands on a menu of other agents'
            //    problems. Both former `internal` envelopes here pointed at it.
            assert!(env.see.starts_with("latiq://"), "{label}: {}", env.see);
            assert_ne!(
                env.see, "latiq://troubleshooting",
                "{label}: the index is not a page about this failure"
            );

            // 4. An actionable with nothing to read or no next call is not one.
            assert!(
                !env.message.is_empty() && !env.suggest.is_empty(),
                "{label}"
            );
            for text in [&env.message, &env.suggest] {
                assert!(
                    !text.contains('{'),
                    "{label}: an unresolved placeholder reached the caller: {text}"
                );
            }
        }
        // Anti-vacuity for the branch counters: both arms ran, so neither
        // assertion above passed by never being reached.
        assert_eq!(operator, 4, "NoNodeAvailable, Storage, both AllocationNot…");
        assert_eq!(agent, 7);
    }

    /// Regression pin for the audit. `NodeNotFound` is a node id the caller got
    /// wrong; it is not a fault of ours, and every control field now says so.
    ///
    /// It shipped as `internal`: `audience: operator` (nobody for the caller to
    /// reach), `retryable: as_is` (re-send the identical wrong id) — next to a
    /// `suggest` telling the caller to go and look the right one up. Three
    /// fields saying stop, one saying carry on.
    #[test]
    fn error_contract_an_unknown_node_id_is_the_callers_to_correct() {
        use latiq_common::{Audience, Retryable};
        let env = ControlPlaneError::NodeNotFound("bad-id".into()).envelope();
        assert_eq!(env.kind, ErrorKind::InvalidValue);
        assert_eq!(env.audience, Audience::Agent);
        assert_eq!(
            env.retryable,
            Retryable::AfterChange,
            "the id is what was wrong, so the same call with a different one is the fix"
        );
        assert!(
            env.suggest.contains("latiq node list"),
            "and the advice must name how to find a real one: {}",
            env.suggest
        );
        // The id rides as a VALUE, not only inside the sentence.
        assert_eq!(
            env.facts.get("node_id"),
            Some(&latiq_common::Fact::Text("bad-id".into()))
        );
        // The transport's answer is unchanged: "was there a record" is a
        // different question from "who fixes this".
        assert_eq!(
            to_status(ControlPlaneError::NodeNotFound("bad-id".into())).code(),
            Code::NotFound
        );
    }
}
