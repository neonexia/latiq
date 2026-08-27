//! Caller identity. Each field knows whether it was verified: `subject` and
//! `issuer` come from a validated IdP token; `agent_id` is ALWAYS claimed and
//! must never carry authority. See docs/identity.md.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Identity {
    /// The claimed leaf agent instance. Never verified. Attribution only.
    pub agent_id: String,
    /// The IdP's `sub`. Empty unless `verified`.
    pub subject: String,
    /// The `iss` of the token that produced `subject`. Empty unless `verified`.
    /// Carried separately so subjects from different issuers cannot collide.
    pub issuer: String,
    pub verified: bool,
}

impl Identity {
    /// Build a claimed (unverified) identity, defaulting to "anonymous" when absent/empty.
    pub fn claimed(header: Option<&str>) -> Self {
        let agent_id = match header.map(str::trim) {
            Some(s) if !s.is_empty() => s.to_string(),
            _ => "anonymous".to_string(),
        };
        Self {
            agent_id,
            subject: String::new(),
            issuer: String::new(),
            verified: false,
        }
    }

    /// Build a verified identity from validated token claims. The leaf agent id
    /// stays claimed; absent, it falls back to the subject.
    pub fn verified(subject: &str, issuer: &str, claimed_agent: Option<&str>) -> Self {
        let agent_id = match claimed_agent.map(str::trim) {
            Some(s) if !s.is_empty() => s.to_string(),
            _ => subject.to_string(),
        };
        Self {
            agent_id,
            subject: subject.to_string(),
            issuer: issuer.to_string(),
            verified: true,
        }
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

    #[test]
    fn claimed_has_no_verified_fields() {
        let id = Identity::claimed(Some("agent-7"));
        assert_eq!(id.agent_id, "agent-7");
        assert!(!id.verified);
        assert_eq!(id.subject, "");
        assert_eq!(id.issuer, "");
    }

    #[test]
    fn verified_carries_subject_and_issuer() {
        let id = Identity::verified(
            "svc-orchestrator",
            "https://idp.example/realms/latiq",
            Some("agent-7"),
        );
        assert!(id.verified);
        assert_eq!(id.subject, "svc-orchestrator");
        assert_eq!(id.issuer, "https://idp.example/realms/latiq");
        assert_eq!(id.agent_id, "agent-7");
    }

    #[test]
    fn verified_agent_id_falls_back_to_subject() {
        // The leaf is optional; without it the subject is the best attribution
        // we have -- "anonymous" would be a lie for a verified caller.
        let id = Identity::verified("svc-orchestrator", "https://idp.example", None);
        assert_eq!(id.agent_id, "svc-orchestrator");
    }
}
