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

//! Caller identity. Each field knows whether it was verified: `subject` and
//! `issuer` come from a validated IdP token; `agent_id` is ALWAYS claimed and
//! must never carry authority. See docs/identity.md.
use serde::Serialize;

// No `Deserialize`: an `Identity` must never be reconstructed from a wire
// payload. It is produced by a token verifier or by `claimed()`, never parsed --
// otherwise attacker-controlled JSON could mint a fully-verified principal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
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
        let subject = subject.trim();
        let issuer = issuer.trim();
        debug_assert!(
            !subject.is_empty(),
            "verified identity requires a non-empty subject"
        );
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

    #[test]
    fn verified_trims_the_claimed_agent() {
        let id = Identity::verified(
            "svc-orchestrator",
            "https://idp.example",
            Some("  agent-7  "),
        );
        assert_eq!(id.agent_id, "agent-7");
    }

    /// `docs/identity.md` describes it as STRUCTURAL that `Identity` derives
    /// `Serialize` but not `Deserialize`: parsing one from a wire payload would
    /// let attacker-controlled JSON mint a fully-verified principal -- any
    /// `subject`, any `issuer`, `verified: true`. Until now that was a comment
    /// and nothing else, so adding `Deserialize` "for convenience" reopened a
    /// documented privilege-escalation path with every test still green.
    ///
    /// A source-level assertion because the thing being guarded is a code shape
    /// (the derive list), and there is no runtime behaviour to observe -- the
    /// whole point is that the impl must not EXIST. It cannot pass vacuously:
    /// both `expect`s fail loudly if the struct or its derive line is renamed
    /// or moved, rather than silently finding nothing.
    #[test]
    fn identity_is_serialize_only_and_never_deserialize() {
        // Only the NON-test half of the file: this test names the very impl it
        // forbids as a string literal, and searching itself would both match
        // that literal and let one written here mask a real one.
        let code = include_str!("identity.rs")
            .split_once("#[cfg(test)]")
            .expect("this file has a #[cfg(test)] module, and this test is in it")
            .0;
        let before = code
            .split_once("\npub struct Identity {")
            .expect("the guarded type is declared as `pub struct Identity {`")
            .0;
        let derives = before
            .rsplit_once("#[derive(")
            .expect("`Identity` must carry a derive list")
            .1
            .split_once(")]")
            .expect("the derive list must be closed")
            .0;
        // Token-wise, not `contains`: "Deserialize" contains "Serialize", so a
        // substring check would call a Deserialize-only struct compliant.
        let derived: Vec<&str> = derives.split(',').map(str::trim).collect();
        assert!(
            derived.contains(&"Serialize"),
            "`Identity` must stay serializable -- the access trail and the \
             adapters render it. Got: {derived:?}"
        );
        assert!(
            !derived.iter().any(|d| d.ends_with("Deserialize")),
            "`Identity` must NEVER be deserializable: attacker-controlled JSON \
             would mint a verified principal. Produce one with `verified()` (from \
             a token verifier) or `claimed()`. See docs/identity.md. Got: {derived:?}"
        );
        // ...and not by hand, either.
        assert!(
            !code.contains("Deserialize for Identity"),
            "a hand-written `Deserialize` impl reopens the same hole the derive \
             was kept off for"
        );
    }

    #[test]
    fn verified_trims_subject_and_issuer() {
        let id = Identity::verified("  svc-orchestrator  ", "  https://idp.example  ", None);
        assert_eq!(id.subject, "svc-orchestrator");
        assert_eq!(id.issuer, "https://idp.example");
        assert_eq!(id.agent_id, "svc-orchestrator");
    }
}
