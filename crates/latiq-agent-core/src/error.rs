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

//! Agent-facing errors carrying a structured `ErrorEnvelope`.
use latiq_common::{ErrorEnvelope, ErrorKind};
use latiq_engine::EngineError;

/// The core's one error type: a newtype over [`ErrorEnvelope`], so an error that
/// crosses a node hop or a surface boundary keeps the kind and guidance it was
/// created with rather than being re-derived at each layer.
#[derive(Debug, Clone)]
pub struct AgentError(ErrorEnvelope);

impl AgentError {
    pub fn new(
        kind: ErrorKind,
        message: impl Into<String>,
        suggest: impl Into<String>,
        see: impl Into<String>,
    ) -> Self {
        AgentError(ErrorEnvelope::new(kind, message, suggest, see))
    }

    /// Wrap an already-built envelope (e.g. one decoded from a gRPC `Status`'s
    /// details, or produced by `ControlPlaneError::envelope()`), so every surface
    /// carries the same guidance rather than re-deriving it.
    pub fn from_envelope(env: ErrorEnvelope) -> Self {
        AgentError(env)
    }

    pub fn envelope(&self) -> &ErrorEnvelope {
        &self.0
    }

    pub fn into_envelope(self) -> ErrorEnvelope {
        self.0
    }

    /// Build from `kind`'s canonical suggest/see defaults (the single source in
    /// `latiq-common`); pass only the specific message.
    pub fn of_kind(kind: ErrorKind, message: impl Into<String>) -> Self {
        AgentError(ErrorEnvelope::for_kind(kind, message))
    }

    pub fn pond_not_found(pond_ref: &str) -> Self {
        Self::of_kind(
            ErrorKind::PondNotFound,
            format!("Pond '{pond_ref}' does not exist."),
        )
    }

    /// The pond resolves, but the registry names no node that is serving it —
    /// see [`ErrorKind::PondUnavailable`]. Raised INSTEAD of falling through to
    /// a local execution: the node that received the request does not hold this
    /// pond's files, and serving it here would create an empty pond of the same
    /// name and answer with plausible, empty results.
    pub fn pond_unavailable(pond_ref: &str) -> Self {
        Self::of_kind(
            ErrorKind::PondUnavailable,
            format!(
                "Pond '{pond_ref}' exists, but the node that owns it is not registered with this \
                 deployment — no node is currently serving it, and this node does not hold its \
                 data."
            ),
        )
    }

    /// Allocation could not materialise the pond on the node the control plane
    /// placed it on, so **there is no pond**.
    ///
    /// Same [`ErrorKind::PondUnavailable`] as the stranded-pond refusal, and for
    /// the same underlying reason (the node that should hold this pond's files
    /// cannot be reached), but with its own message and `suggest`: the kind's
    /// default advice is `pond forget`, which is wrong here — the registry row
    /// is normally already gone, and an agent's next move is to retry rather
    /// than to fetch an operator.
    ///
    /// `compensated` is the whole difference between "retry freely" and "an
    /// operator has work to do", so it is stated in the message rather than left
    /// for the reader to assume. A retrying agent has to know whether the failed
    /// attempt left a row behind holding its name.
    pub fn allocation_not_materialized(
        name: &str,
        owner: &str,
        cause: &AgentError,
        compensated: bool,
    ) -> Self {
        let cause = &cause.envelope().message;
        if compensated {
            Self::new(
                ErrorKind::PondUnavailable,
                format!(
                    "Pond '{name}' was NOT created: the node it was assigned to ({owner}) could \
                     not materialise its storage ({cause}). The assignment has been rolled back, \
                     so the name is free and nothing was left behind."
                ),
                "Retry allocate_pond — the failed attempt left nothing behind, so the same name is \
                 free. If it keeps failing, that node is down: report it to your operator.",
                ErrorKind::PondUnavailable.default_see(),
            )
        } else {
            Self::new(
                ErrorKind::PondUnavailable,
                format!(
                    "Pond '{name}' was NOT created: the node it was assigned to ({owner}) could \
                     not materialise its storage ({cause}), AND the assignment could not be rolled \
                     back — a registry row named '{name}' may still exist with no storage behind it."
                ),
                "Retry allocate_pond under a DIFFERENT name; the original may still be taken. Ask \
                 an operator to remove the stranded record with `latiq pond forget <pond> \
                 --confirm` (it deletes the registry row only, never data).",
                ErrorKind::PondUnavailable.default_see(),
            )
        }
    }

    pub fn name_conflict(name: &str) -> Self {
        Self::of_kind(
            ErrorKind::NameConflict,
            format!("A pond named '{name}' already exists."),
        )
    }

    pub fn result_cap_exceeded(rows: usize, cap: usize) -> Self {
        Self::of_kind(
            ErrorKind::ResultCapExceeded,
            format!("Result has {rows} rows, over the inline cap of {cap}."),
        )
    }

    pub fn dataset_not_found(reference: &str) -> Self {
        Self::of_kind(
            ErrorKind::DatasetNotFound,
            format!("Dataset '{reference}' is not in the catalog."),
        )
    }

    pub fn unsupported_extension(message: impl Into<String>) -> Self {
        // Bespoke suggest (extension-specific), so not the InvalidValue default.
        Self::new(
            ErrorKind::InvalidValue,
            message,
            "Request only signed/official extensions baked into this deployment; see latiq://guidance for the supported set.",
            "latiq://guidance",
        )
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::of_kind(ErrorKind::Internal, message)
    }
}

impl std::fmt::Display for AgentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.message)
    }
}

impl std::error::Error for AgentError {}

impl From<EngineError> for AgentError {
    fn from(e: EngineError) -> Self {
        match e {
            EngineError::ReadOnlyViolation => AgentError::of_kind(
                ErrorKind::ReadOnlyViolation,
                "read_query received a statement that is not read-only.",
            ),
            EngineError::Cancelled => {
                AgentError::of_kind(ErrorKind::QueryCancelled, "The query was cancelled.")
            }
            EngineError::Timeout => {
                AgentError::of_kind(ErrorKind::QueryTimeout, "The query exceeded the timeout.")
            }
            EngineError::Parse(m) => {
                AgentError::of_kind(ErrorKind::ParseError, format!("SQL parse error: {m}"))
            }
            EngineError::Engine(m) => AgentError::internal(format!("engine error: {m}")),
        }
    }
}
