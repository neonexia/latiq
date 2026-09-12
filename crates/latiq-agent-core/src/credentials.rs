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

//! How an `attach_catalog` call gets the credential the external catalog wants.
//!
//! **Three modes, one resolver.** They are mutually exclusive by construction —
//! the caller picks one and the other two are then not askable — because a call
//! that supplied both an explicit secret and a `secret_ref` has not said which
//! it means, and picking one for it is exactly the silent substitution
//! invariant 13(b) forbids.
//!
//! | mode | the caller supplies | the credential is |
//! |---|---|---|
//! | [`CredentialMode::Explicit`] | `--secret key=value` | those values |
//! | [`CredentialMode::Passthrough`] | *nothing* | the caller's **own bearer** |
//! | [`CredentialMode::Ref`] | `--secret-ref <uri>` | whatever the URI's backend returns |
//! | [`CredentialMode::None`] | *nothing*, and there is no bearer to pass | absent — and SAID to be |
//!
//! `passthrough` is the interesting one and the dominant case in practice:
//! Iceberg REST, Unity Catalog and Snowflake External OAuth all consume a bearer
//! directly, so the token the agent already presented to Latiq **is** the
//! catalog credential and no state is stored anywhere. It is read from
//! [`crate::bearer::current_bearer`] — deliberately not from [`Identity`], which
//! carries attribution and must never carry a credential (invariant 9).
//!
//! `ref` treats the URI as **opaque**: the scheme selects a backend and the rest
//! is the backend's own business. That is what keeps adding Vault to one
//! implementation of [`SecretRefResolver`] rather than a change to every
//! surface. A scheme nobody registered is `capability_unavailable` /
//! `after_provisioning`, which is precisely what that kind is for — the call was
//! RIGHT and this deployment is missing a piece.
//!
//! [`Identity`]: latiq_common::Identity
use crate::error::AgentError;
use latiq_common::{facts, ErrorKind, Secret};
use std::collections::BTreeMap;
use std::sync::Arc;

/// The URI scheme a `secret_ref` must carry, and the env-var namespace the
/// built-in backend reads from.
const ENV_SCHEME: &str = "env";

/// Prefix every env var the [`EnvSecretRefResolver`] will look at must carry.
///
/// The bound matters: without it `env://PATH` would be a credential reference,
/// and any caller could mount arbitrary process environment into a `CREATE
/// SECRET`. With it, the reachable set is exactly what an operator deliberately
/// exported under this name — which is also why an empty result is
/// `after_provisioning` rather than "not found".
pub const ENV_SECRET_PREFIX: &str = "LATIQ_CATALOG_SECRET_";

/// Which of the three modes an attach actually ran under. Reported back on the
/// attach response, never inferred by the caller: a `passthrough` that found no
/// bearer resolves to [`Self::None`] and **says so**, instead of looking
/// identical to one that applied the caller's token.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum CredentialMode {
    Explicit,
    Passthrough,
    Ref,
    /// No credential was applied. Correct and common for a local DuckLake
    /// catalog, which authenticates to nothing.
    None,
}

impl CredentialMode {
    pub const ALL: &'static [CredentialMode] = &[
        CredentialMode::Explicit,
        CredentialMode::Passthrough,
        CredentialMode::Ref,
        CredentialMode::None,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Explicit => "explicit",
            Self::Passthrough => "passthrough",
            Self::Ref => "ref",
            Self::None => "none",
        }
    }
}

/// What the caller asked for, after the mutual-exclusion check. Build it with
/// [`CredentialSpec::from_request`] — the constructor is the check.
#[derive(Debug, Clone)]
pub enum CredentialSpec {
    Explicit(BTreeMap<String, Secret>),
    Ref(String),
    Passthrough,
}

impl CredentialSpec {
    /// Decide the mode from what arrived on the wire, refusing anything
    /// ambiguous by NAMING the legal shapes.
    ///
    /// `secrets` empty + `secret_ref` empty is not an omission to be filled in
    /// later: it is the caller choosing `passthrough`, which is a real mode with
    /// a real credential behind it.
    pub fn from_request(
        secrets: BTreeMap<String, Secret>,
        secret_ref: Option<String>,
    ) -> Result<Self, AgentError> {
        let secret_ref = secret_ref.filter(|r| !r.is_empty());
        match (secrets.is_empty(), secret_ref) {
            (false, Some(_)) => Err(AgentError::of_kind(
                ErrorKind::InvalidValue,
                "Supply exactly one credential mode: `secrets` (explicit values), `secret_ref` \
                 (a vault/env URI), or NEITHER — which means passthrough, where your own bearer \
                 token is used as the catalog credential. Both `secrets` and `secret_ref` were \
                 given, and Latiq will not choose between them.",
            )),
            (false, None) => Ok(Self::Explicit(secrets)),
            (true, Some(r)) => Ok(Self::Ref(r)),
            (true, None) => Ok(Self::Passthrough),
        }
    }
}

/// Dereferences one `secret_ref` scheme into a credential map.
///
/// One method, on purpose: adding Vault, AWS Secrets Manager or a file backend
/// is a new implementation of this, registered in [`CredentialResolvers`] — not
/// a new field on the attach request, a new proto message, and a new argument on
/// four surfaces.
#[async_trait::async_trait]
pub trait SecretRefResolver: Send + Sync {
    /// The URI scheme this backend claims, without `://` (`env`, `vault`, …).
    fn scheme(&self) -> &'static str;
    /// Resolve the WHOLE uri (scheme included — a backend may care about the
    /// authority/path split, and Latiq deliberately does not parse it for them).
    async fn resolve(&self, uri: &str) -> Result<BTreeMap<String, Secret>, AgentError>;
}

/// The backends this node has been configured with. Empty is a legitimate
/// deployment: `explicit` and `passthrough` need none of this.
#[derive(Default, Clone)]
pub struct CredentialResolvers {
    by_scheme: BTreeMap<&'static str, Arc<dyn SecretRefResolver>>,
}

impl CredentialResolvers {
    pub fn new() -> Self {
        Self::default()
    }

    /// The set a node gets unless it says otherwise: the `env://` backend, which
    /// needs no configuration and no network, and is the cheapest way for an
    /// operator to hand a node a credential an agent must not hold.
    pub fn with_defaults() -> Self {
        Self::new().register(Arc::new(EnvSecretRefResolver))
    }

    pub fn register(mut self, r: Arc<dyn SecretRefResolver>) -> Self {
        self.by_scheme.insert(r.scheme(), r);
        self
    }

    pub fn schemes(&self) -> Vec<&'static str> {
        self.by_scheme.keys().copied().collect()
    }

    /// Dereference `uri`, or say which schemes this deployment does serve.
    pub async fn resolve(&self, uri: &str) -> Result<BTreeMap<String, Secret>, AgentError> {
        let Some((scheme, _)) = uri.split_once("://") else {
            return Err(AgentError::rendered(
                ErrorKind::InvalidValue,
                "secret_ref '{secret_ref}' is not a URI: it must be '<scheme>://<location>', \
                 e.g. 'env://lake'.",
                facts! { "secret_ref" => uri },
            ));
        };
        match self.by_scheme.get(scheme) {
            Some(r) => r.resolve(uri).await,
            // `capability_unavailable` / `after_provisioning`: the call is
            // correct and this deployment has no backend for that scheme.
            // Retrying it changes nothing and an agent cannot install one — so
            // the canonical suggest ("run latiq warm-extensions") would be wrong
            // here, and the bespoke one names what an operator actually does.
            None => {
                let available = if self.by_scheme.is_empty() {
                    "none".to_string()
                } else {
                    self.schemes().join(", ")
                };
                Err(AgentError::rendered_with(
                    ErrorKind::CapabilityUnavailable,
                    "No credential backend is configured for the '{capability}' scheme on this \
                     node (configured: {available}).",
                    facts! { "capability" => format!("{scheme}://"), "available" => available },
                    "Stop and report this — you cannot configure a credential backend from here, \
                     and the identical call will keep failing until somebody does. Tell whoever \
                     is orchestrating you which scheme is missing (it is in `facts.capability`). \
                     If you legitimately hold the credential values yourself, send \
                     attach_catalog again with `secrets` instead of `secret_ref`; if you hold a \
                     bearer the catalog accepts, send it with neither and it will be used.",
                    ErrorKind::CapabilityUnavailable.default_see(),
                ))
            }
        }
    }
}

/// The built-in `env://` backend: `env://<name>` reads every environment
/// variable called `LATIQ_CATALOG_SECRET_<NAME>_<KEY>` and yields `<key>`
/// lowercased.
///
/// So `env://lake`, with `LATIQ_CATALOG_SECRET_LAKE_TOKEN=…` exported on the
/// node, resolves to `{token: …}`.
///
/// It is the cheapest real backend, and real is the point: it proves the trait
/// is a boundary a backend can live behind rather than a shape invented around
/// one implementation. The values never leave the node — they go straight into
/// [`Secret`] and out through one `CREATE SECRET`.
pub struct EnvSecretRefResolver;

impl EnvSecretRefResolver {
    /// The env-var prefix `env://<name>` reads under.
    fn prefix_for(name: &str) -> String {
        format!(
            "{ENV_SECRET_PREFIX}{}_",
            name.to_uppercase().replace('-', "_")
        )
    }
}

#[async_trait::async_trait]
impl SecretRefResolver for EnvSecretRefResolver {
    fn scheme(&self) -> &'static str {
        ENV_SCHEME
    }

    async fn resolve(&self, uri: &str) -> Result<BTreeMap<String, Secret>, AgentError> {
        let name = uri.trim_start_matches("env://").trim_matches('/');
        if name.is_empty() {
            return Err(AgentError::of_kind(
                ErrorKind::InvalidValue,
                format!(
                    "secret_ref 'env://' names nothing. Use 'env://<name>', which reads every \
                     {ENV_SECRET_PREFIX}<NAME>_<KEY> variable exported on the node."
                ),
            ));
        }
        let prefix = Self::prefix_for(name);
        let out: BTreeMap<String, Secret> = std::env::vars()
            .filter_map(|(k, v)| {
                k.strip_prefix(&prefix)
                    .filter(|key| !key.is_empty())
                    .map(|key| (key.to_lowercase(), Secret::new(v)))
            })
            .collect();
        if out.is_empty() {
            // Not `DatasetNotFound`-shaped and not the caller's typo to fix
            // blind: from where the agent stands, the reference was legitimate
            // and the node has not been provisioned with it. Same kind, same
            // escalation, a suggest that names the variable an operator exports.
            return Err(AgentError::rendered_with(
                ErrorKind::CapabilityUnavailable,
                "No credential is provisioned for '{capability}' on this node: no {prefix}* \
                 environment variable is set.",
                facts! { "capability" => uri, "prefix" => prefix.clone() },
                "Stop and report this — you cannot export an environment variable on the node, \
                 and the identical call will keep failing until somebody does. Tell whoever is \
                 orchestrating you which reference is missing (it is in `facts.capability`); an \
                 operator exports the variables in `facts.prefix` on the pond node and restarts \
                 it. If you hold the credential values yourself, send attach_catalog again with \
                 `secrets` instead.",
                ErrorKind::CapabilityUnavailable.default_see(),
            ));
        }
        Ok(out)
    }
}

/// Turn what the caller asked for into the credential map the attacher builds
/// its `CREATE SECRET` from, plus the mode that was actually applied.
///
/// `bearer` is the caller's own token (`current_bearer()`), passed in rather
/// than read here so this stays a pure function of its arguments — the ambient
/// scope belongs to the request, and a test that fabricated one would be
/// asserting its own fixture.
pub async fn resolve_credentials(
    type_: &str,
    spec: CredentialSpec,
    resolvers: &CredentialResolvers,
    bearer: Option<String>,
) -> Result<(BTreeMap<String, Secret>, CredentialMode), AgentError> {
    let (secrets, mode) = match spec {
        CredentialSpec::Explicit(s) => (s, CredentialMode::Explicit),
        CredentialSpec::Ref(uri) => (resolvers.resolve(&uri).await?, CredentialMode::Ref),
        CredentialSpec::Passthrough => {
            // The type decides whether a bearer is even a credential here; a
            // DuckLake attach authenticates to an object store with SigV4 keys
            // and has nothing to put a token in.
            let param = latiq_common::catalog::lookup(type_).and_then(|s| s.bearer_param);
            match (param, bearer.filter(|b| !b.is_empty())) {
                (Some(p), Some(tok)) => (
                    BTreeMap::from([(p.to_string(), Secret::new(tok))]),
                    CredentialMode::Passthrough,
                ),
                // Nothing to pass through. Reported as `none`, not as
                // `passthrough`: the two are different facts and only one of
                // them means "your token is in force".
                _ => (BTreeMap::new(), CredentialMode::None),
            }
        }
    };
    // Whatever the mode, the KEYS must be ones this catalog type reads. A
    // resolver backend that answers with `api_key` for an iceberg catalog has
    // handed us a credential that would be silently ignored, and "attached with
    // no auth" must never be the quiet outcome of a provisioning mistake.
    latiq_common::catalog::check_secrets(type_, secrets.keys().map(|k| k.as_str())).map_err(
        |r| {
            AgentError::of_kind(
                ErrorKind::InvalidValue,
                format!("{r} (credential mode: {})", mode.as_str()),
            )
        },
    )?;
    Ok((secrets, mode))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secrets(kv: &[(&str, &str)]) -> BTreeMap<String, Secret> {
        kv.iter()
            .map(|(k, v)| (k.to_string(), Secret::new(*v)))
            .collect()
    }

    /// The three modes are mutually exclusive, and the refusal NAMES the legal
    /// shapes (invariant 13b) instead of silently preferring one.
    #[test]
    fn catalog_credentials_two_modes_at_once_are_refused_naming_the_legal_set() {
        let Err(e) = CredentialSpec::from_request(
            secrets(&[("token", "t")]),
            Some("env://lake".to_string()),
        ) else {
            panic!("supplying both an explicit secret and a secret_ref must be refused");
        };
        let env = e.envelope();
        assert_eq!(env.kind, ErrorKind::InvalidValue);
        for shape in ["secrets", "secret_ref", "passthrough"] {
            assert!(
                env.message.contains(shape),
                "the refusal must name every legal shape, and '{shape}' is missing: {}",
                env.message
            );
        }
        // …and the credential itself is not quoted back in the refusal.
        assert!(!env.message.contains('t') || !env.message.contains("\"t\""));
    }

    /// Absence is a CHOICE here, not an omission: nothing supplied means
    /// passthrough, which is the mode with a real credential behind it.
    #[test]
    fn catalog_credentials_supplying_nothing_selects_passthrough() {
        assert!(matches!(
            CredentialSpec::from_request(BTreeMap::new(), None).unwrap(),
            CredentialSpec::Passthrough
        ));
        // An EMPTY secret_ref is proto3's "unset", not a URI the caller chose —
        // the one place empty-means-absent is legitimate (invariant 13c).
        assert!(matches!(
            CredentialSpec::from_request(BTreeMap::new(), Some(String::new())).unwrap(),
            CredentialSpec::Passthrough
        ));
        assert!(matches!(
            CredentialSpec::from_request(secrets(&[("token", "t")]), None).unwrap(),
            CredentialSpec::Explicit(_)
        ));
        assert!(matches!(
            CredentialSpec::from_request(BTreeMap::new(), Some("vault://x".into())).unwrap(),
            CredentialSpec::Ref(_)
        ));
    }

    /// Passthrough puts the CALLER's bearer in the key the catalog type reads,
    /// and reports that it did.
    #[tokio::test]
    async fn catalog_credentials_passthrough_uses_the_callers_bearer() {
        let (s, mode) = resolve_credentials(
            "iceberg",
            CredentialSpec::Passthrough,
            &CredentialResolvers::with_defaults(),
            Some("caller-token".into()),
        )
        .await
        .unwrap();
        assert_eq!(mode, CredentialMode::Passthrough);
        assert_eq!(
            s.get("token").map(|v| v.expose()),
            Some("caller-token"),
            "the bearer must land in the type's own bearer_param"
        );
    }

    /// A passthrough with nothing to pass through reports `none` — the whole
    /// point of the fourth variant. Reporting `passthrough` would tell an agent
    /// its token was in force when no credential was applied at all, which is
    /// the "short answer that looks complete" invariant 13 forbids.
    #[tokio::test]
    async fn catalog_credentials_passthrough_without_a_bearer_reports_none() {
        let r = CredentialResolvers::with_defaults();
        for bearer in [None, Some(String::new())] {
            let (s, mode) = resolve_credentials("iceberg", CredentialSpec::Passthrough, &r, bearer)
                .await
                .unwrap();
            assert_eq!(mode, CredentialMode::None);
            assert!(s.is_empty());
        }
        // …and a type with no bearer credential reports `none` even WITH a
        // bearer, because there is nothing for the token to be.
        let (s, mode) = resolve_credentials(
            "ducklake",
            CredentialSpec::Passthrough,
            &r,
            Some("caller-token".into()),
        )
        .await
        .unwrap();
        assert_eq!(mode, CredentialMode::None);
        assert!(s.is_empty(), "a ducklake attach has no bearer to carry");
    }

    /// An unregistered scheme is `capability_unavailable`/`after_provisioning` —
    /// the call was right, the deployment is missing a backend — and its
    /// suggest names what to do INSTEAD, not "retry".
    #[tokio::test]
    async fn error_contract_an_unsupported_secret_ref_scheme_escalates_rather_than_retrying() {
        let r = CredentialResolvers::with_defaults();
        let Err(e) = r.resolve("vault://team/lake").await else {
            panic!("no vault backend is registered, so this must be refused");
        };
        let env = e.envelope();
        assert_eq!(env.kind, ErrorKind::CapabilityUnavailable);
        assert_eq!(env.audience, latiq_common::Audience::Operator);
        assert_eq!(env.retryable, latiq_common::Retryable::AfterProvisioning);
        assert_eq!(
            env.facts.get("capability").map(|f| f.to_string()),
            Some("vault://".to_string()),
            "the missing capability must be a VALUE, not only prose: {env:?}"
        );
        // The canonical capability_unavailable suggest talks about
        // `latiq warm-extensions`, which is the wrong instrument here — so this
        // site carries its own, and it must name a call that can work.
        assert!(
            env.suggest.contains("secrets"),
            "the suggest must name the move that succeeds: {}",
            env.suggest
        );
        assert!(
            !env.suggest.contains("warm-extensions"),
            "a missing credential backend is not a missing extension: {}",
            env.suggest
        );
        // The one scheme this deployment DOES serve is named.
        assert!(env.message.contains("env"), "{}", env.message);
    }

    #[tokio::test]
    async fn error_contract_a_secret_ref_that_is_not_a_uri_is_the_callers_to_fix() {
        let Err(e) = CredentialResolvers::with_defaults().resolve("lake").await else {
            panic!("a bare name is not a URI");
        };
        let env = e.envelope();
        assert_eq!(env.kind, ErrorKind::InvalidValue);
        assert_eq!(env.audience, latiq_common::Audience::Agent);
        assert!(env.message.contains("env://lake"), "{}", env.message);
    }

    /// The `env://` backend against real process environment — the cheapest
    /// real backend, driven for real rather than through a fake.
    #[tokio::test]
    async fn catalog_credentials_env_backend_reads_the_operators_variables() {
        // Unique per test process; `std::env::set_var` is process-global.
        let name = "envtest";
        std::env::set_var("LATIQ_CATALOG_SECRET_ENVTEST_TOKEN", "from-the-node");
        std::env::set_var("LATIQ_CATALOG_SECRET_ENVTEST_S3_ACCESS_KEY", "AK");
        // Deliberately outside the namespace: it must NOT be reachable.
        std::env::set_var("ENVTEST_TOKEN", "not-mine");
        let got = EnvSecretRefResolver
            .resolve(&format!("env://{name}"))
            .await
            .unwrap();
        assert_eq!(
            got.get("token").map(|s| s.expose()),
            Some("from-the-node"),
            "the key is the suffix, lowercased"
        );
        assert_eq!(got.get("s3_access_key").map(|s| s.expose()), Some("AK"));
        assert_eq!(got.len(), 2, "only the namespaced variables: {got:?}");
        std::env::remove_var("LATIQ_CATALOG_SECRET_ENVTEST_TOKEN");
        std::env::remove_var("LATIQ_CATALOG_SECRET_ENVTEST_S3_ACCESS_KEY");
        std::env::remove_var("ENVTEST_TOKEN");
    }

    /// A reference to a credential nobody provisioned is the same escalation as
    /// a scheme nobody registered — an agent must stop, not loop.
    #[tokio::test]
    async fn error_contract_an_unprovisioned_env_ref_names_the_variable_to_export() {
        let Err(e) = EnvSecretRefResolver.resolve("env://nothing-here").await else {
            panic!("an unprovisioned reference must be refused");
        };
        let env = e.envelope();
        assert_eq!(env.kind, ErrorKind::CapabilityUnavailable);
        assert_eq!(env.retryable, latiq_common::Retryable::AfterProvisioning);
        assert_eq!(
            env.facts.get("prefix").map(|f| f.to_string()),
            Some("LATIQ_CATALOG_SECRET_NOTHING_HERE_".to_string()),
            "an operator must be told the exact variable name: {env:?}"
        );
    }

    /// A backend that answers with a key the catalog type never reads would
    /// attach with NO authentication and look like a success. Refused instead,
    /// naming the mode so the operator knows which side to fix.
    #[tokio::test]
    async fn catalog_credentials_a_key_the_type_never_reads_is_refused_not_ignored() {
        let Err(e) = resolve_credentials(
            "iceberg",
            CredentialSpec::Explicit(secrets(&[("api_key", "x")])),
            &CredentialResolvers::with_defaults(),
            None,
        )
        .await
        else {
            panic!("iceberg reads no `api_key`; attaching unauthenticated must not be the quiet outcome");
        };
        let env = e.envelope();
        assert_eq!(env.kind, ErrorKind::InvalidValue);
        assert!(env.message.contains("api_key"), "{}", env.message);
        assert!(
            env.message.contains("token"),
            "name the real set: {}",
            env.message
        );
        assert!(
            env.message.contains("explicit"),
            "name the mode: {}",
            env.message
        );
        // The credential VALUE is never quoted back.
        assert!(!env.message.contains("\"x\""), "{}", env.message);
    }

    /// Every mode has a distinct wire name, and the set is closed — the response
    /// field is what a caller branches on.
    #[test]
    fn catalog_credentials_every_mode_has_a_distinct_wire_name() {
        let names: std::collections::BTreeSet<&str> =
            CredentialMode::ALL.iter().map(|m| m.as_str()).collect();
        assert_eq!(names.len(), CredentialMode::ALL.len());
        assert_eq!(
            serde_json::to_string(&CredentialMode::Passthrough).unwrap(),
            "\"passthrough\""
        );
    }
}
