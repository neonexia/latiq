//! Control-plane error type.
use latiq_common::{ErrorEnvelope, ErrorKind};
use tonic::{Code, Status};

#[derive(Debug, thiserror::Error)]
pub enum ControlPlaneError {
    #[error("pond name already exists: {0}")]
    NameConflict(String),
    #[error("pond not found: {0}")]
    PondNotFound(String),
    #[error("node not found: {0}")]
    NodeNotFound(String),
    #[error("dataset not found: {0}")]
    DatasetNotFound(String),
    #[error("catalog not found: {0}")]
    CatalogNotFound(String),
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
            ControlPlaneError::NodeNotFound(m) => ErrorEnvelope::new(
                ErrorKind::Internal,
                format!("No pond node is available: {m}"),
                "Ensure a pond node is registered and healthy with the control plane, then retry.",
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
            ControlPlaneError::Invalid(m) => {
                ErrorEnvelope::for_kind(ErrorKind::InvalidValue, m.clone())
            }
            ControlPlaneError::Storage(m) => {
                ErrorEnvelope::for_kind(ErrorKind::Storage, format!("storage error: {m}"))
            }
        }
    }

    /// The gRPC code for this error. Kept distinct from the envelope kind because
    /// NodeNotFound is a precondition (no host), not NotFound (review #13).
    fn code(&self) -> Code {
        match self {
            ControlPlaneError::NameConflict(_) => Code::AlreadyExists,
            ControlPlaneError::PondNotFound(_)
            | ControlPlaneError::DatasetNotFound(_)
            | ControlPlaneError::CatalogNotFound(_) => Code::NotFound,
            ControlPlaneError::NodeNotFound(_) => Code::FailedPrecondition,
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

        // No-node-available stays a precondition (review #13), with Internal-kind
        // guidance (there is no "unavailable" kind).
        let st = to_status(ControlPlaneError::NodeNotFound("none".into()));
        assert_eq!(st.code(), Code::FailedPrecondition);
        let env: ErrorEnvelope = serde_json::from_slice(st.details()).unwrap();
        assert_eq!(env.kind, ErrorKind::Internal);
    }
}
