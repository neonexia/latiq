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

    pub fn pond_not_found(pond_ref: &str) -> Self {
        Self::new(
            ErrorKind::PondNotFound,
            format!("Pond '{pond_ref}' does not exist."),
            "Call list_ponds to see available ponds, or allocate_pond to create one.",
            "latiq://troubleshooting/pond-not-found",
        )
    }

    pub fn name_conflict(name: &str) -> Self {
        Self::new(
            ErrorKind::NameConflict,
            format!("A pond named '{name}' already exists."),
            "Choose a different name, or omit name to let Latiq generate one.",
            "latiq://guidance",
        )
    }

    pub fn result_cap_exceeded(rows: usize, cap: usize) -> Self {
        Self::new(
            ErrorKind::ResultCapExceeded,
            format!("Result has {rows} rows, over the inline cap of {cap}."),
            "Narrow with WHERE/LIMIT, aggregate server-side (GROUP BY/count/sum), or materialize with CREATE TABLE AS SELECT.",
            "latiq://troubleshooting/large-results",
        )
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(
            ErrorKind::Internal,
            message,
            "Retry; if it persists, report to your operator.",
            "latiq://troubleshooting",
        )
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
            EngineError::ReadOnlyViolation => AgentError::new(
                ErrorKind::ReadOnlyViolation,
                "read_query received a statement that is not read-only.",
                "Use write_query for INSERT/UPDATE/DELETE/DDL; read_query is for SELECT.",
                "latiq://dialect",
            ),
            EngineError::Cancelled => AgentError::new(
                ErrorKind::QueryCancelled,
                "The query was cancelled.",
                "Re-issue the query if you still need the result.",
                "latiq://troubleshooting",
            ),
            EngineError::Timeout => AgentError::new(
                ErrorKind::QueryTimeout,
                "The query exceeded the timeout.",
                "Narrow the query (WHERE/LIMIT) or aggregate server-side, then retry.",
                "latiq://troubleshooting/timeouts",
            ),
            EngineError::Parse(m) => AgentError::new(
                ErrorKind::ParseError,
                format!("SQL parse error: {m}"),
                "Check the SQL syntax against the supported dialect.",
                "latiq://dialect",
            ),
            EngineError::Engine(m) => AgentError::internal(format!("engine error: {m}")),
        }
    }
}
