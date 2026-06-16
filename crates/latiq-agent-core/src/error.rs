//! Agent-facing errors carrying a structured `ErrorEnvelope`.
use latiq_common::{ErrorEnvelope, ErrorKind};
use latiq_engine::EngineError;

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
