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

    /// The node cut the query on its deadline. Names BOTH numbers — the timeout
    /// that was actually in effect (which may be a clamped version of what the
    /// caller asked for) and the node's ceiling — because those two are what
    /// decide the agent's next move, and it can obtain neither any other way.
    ///
    /// The `suggest` covers the three levers, and drops the one that is not
    /// available: at the ceiling there is no larger `timeout_ms` to retry with,
    /// and telling an agent to ask for one would send it round a loop it cannot
    /// win. That case is the tier's problem, not the timeout's.
    pub fn query_timeout(effective_ms: u64, max_ms: u64) -> Self {
        let at_ceiling = effective_ms >= max_ms;
        let suggest = if at_ceiling {
            "This ran at the node's maximum, so a larger timeout_ms is not available. Narrow the \
             query — add a WHERE on a selective column, a LIMIT, or fewer columns — or aggregate \
             server-side (GROUP BY/count/sum) instead of scanning. If the work is genuinely this \
             large, it is too big for this pond's tier: ask an operator to re-tier the pond."
                .to_string()
        } else {
            format!(
                "Retry with a larger timeout_ms (this node allows up to {max_ms}), or narrow the \
                 query — add a WHERE on a selective column, a LIMIT, or fewer columns — or \
                 aggregate server-side (GROUP BY/count/sum). If it still times out at {max_ms} \
                 ms, the query is too large for this pond's tier: ask an operator to re-tier it."
            )
        };
        Self::new(
            ErrorKind::QueryTimeout,
            format!(
                "Query stopped after {effective_ms} ms — the timeout in effect for this request. \
                 This node allows up to {max_ms} ms."
            ),
            suggest,
            ErrorKind::QueryTimeout.default_see(),
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
