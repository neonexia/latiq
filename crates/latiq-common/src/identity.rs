//! Agent identity. Slice 0+ is relaxed: identity is *claimed*, never verified.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Identity {
    pub agent_id: String,
    pub verified: bool,
}

impl Identity {
    /// Build a claimed (unverified) identity, defaulting to "anonymous" when absent/empty.
    pub fn claimed(header: Option<&str>) -> Self {
        let agent_id = match header.map(str::trim) {
            Some(s) if !s.is_empty() => s.to_string(),
            _ => "anonymous".to_string(),
        };
        Self { agent_id, verified: false }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uses_header_when_present() {
        let id = Identity::claimed(Some("agent-incident-bot"));
        assert_eq!(id.agent_id, "agent-incident-bot");
        assert!(!id.verified);
    }

    #[test]
    fn defaults_to_anonymous() {
        assert_eq!(Identity::claimed(None).agent_id, "anonymous");
        assert_eq!(Identity::claimed(Some("   ")).agent_id, "anonymous");
    }
}
