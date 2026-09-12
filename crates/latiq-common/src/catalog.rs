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

//! External-catalog type metadata: which **locator options** and which
//! **credentials** each type takes, what a caller may name one, and which DuckDB
//! extensions its attacher needs.
//!
//! **The option/secret split is the security boundary.** An `--option` value is
//! locator metadata (endpoint, warehouse, region, …): listable, loggable, and
//! echoed back by `list_attached_catalogs`. A `--secret` value is a credential:
//! `latiq_common::Secret`-typed end to end, never logged and returned by no
//! surface. A credential key passed as an option is REFUSED naming `--secret`
//! rather than dropped, and an unknown key is refused naming the legal set —
//! because at attach time there is no later call to re-supply anything at, so
//! what is dropped is simply not applied. (`filter_params` keeps the older
//! drop-at-registration behaviour for the control-plane registry, where the
//! entry is discovery metadata and a credential must never be persisted.)
//!
//! The attacher (`latiq-engine-duckdb`) maps the two sets onto
//! `CREATE SECRET`/`ATTACH`.
use std::collections::BTreeMap;

/// One supported external-catalog type. Adding a type is a row in [`TYPES`]
/// plus an attacher arm in `latiq-engine-duckdb`; nothing else keys off the
/// type name.
pub struct CatalogTypeSpec {
    pub name: &'static str,
    /// Params that MAY be persisted at `catalog add`, and the complete set an
    /// `attach_catalog` call may pass as `--option` (locator metadata only —
    /// never credentials). Anything else is dropped before storage, and refused
    /// at attach.
    pub allowed_params: &'static [&'static str],
    /// Keys this type accepts as CREDENTIALS — supplied through `--secret`,
    /// never `--option`. The split is the whole security boundary: an `--option`
    /// value is listable, loggable and echoed by `list_attached_catalogs`, and a
    /// token that arrived through it would be too.
    ///
    /// Named per type rather than by a global "looks credential-shaped" guess,
    /// so a refusal can say *which* flag the key belongs on instead of matching
    /// a substring.
    pub secret_params: &'static [&'static str],
    /// Which credential key the CALLER'S OWN BEARER fills in `passthrough` mode
    /// — the one the catalog's own API consumes. `None` for a type with no
    /// bearer-shaped credential, where passthrough resolves to nothing at all
    /// (and says so, rather than pretending a credential was applied).
    pub bearer_param: Option<&'static str>,
    /// DuckDB extensions the attacher needs (must be baked into the image).
    pub required_extensions: &'static [&'static str],
}

/// Supported external-catalog types. Iceberg first; add a row + an attacher impl
/// in `latiq-engine-duckdb` to support a new type.
pub const TYPES: &[CatalogTypeSpec] = &[
    CatalogTypeSpec {
        name: "iceberg",
        // endpoint + warehouse locate the REST catalog; s3_endpoint/s3_region
        // locate the storage backend.
        allowed_params: &["endpoint", "warehouse", "s3_endpoint", "s3_region"],
        secret_params: &["token", "s3_access_key", "s3_secret_key"],
        // Iceberg REST (Polaris, Unity, vendor-hosted) consumes a bearer
        // directly, which is what makes `passthrough` the dominant mode: the
        // caller's own token IS the catalog credential.
        bearer_param: Some("token"),
        required_extensions: &["iceberg", "httpfs"],
    },
    CatalogTypeSpec {
        name: "ducklake",
        // A DuckLake catalog = a metadata DB + a data path. Local (file metadata +
        // local/S3 data) needs no credentials; remote (postgres metadata, S3 data)
        // brings its S3 keys in through `--secret`.
        allowed_params: &["metadata_path", "data_path", "s3_endpoint", "s3_region"],
        secret_params: &["s3_access_key", "s3_secret_key"],
        // A DuckLake attach authenticates to an object store with SigV4 keys,
        // never to an API with a bearer. There is nothing for the caller's token
        // to be, so passthrough here resolves to no credential.
        bearer_param: None,
        required_extensions: &["ducklake", "httpfs"],
    },
];

/// Human description of what an attached catalog may be called. Same legal set
/// as a pond name, for the same reason — it becomes a SQL catalog identifier an
/// agent has to type (`FROM lake.sales.orders`) — but its own sentence, because
/// the fix ("pick another alias") is not the pond's.
pub const ALIAS_RULE: &str = "1-64 characters, letters, digits, `_` or `-` only (it becomes the \
                              catalog's SQL namespace, e.g. `FROM lake.sales.orders`)";

/// Catalog names DuckDB already uses for itself. Attaching over one of them
/// either fails obscurely or shadows something the engine needs, so it is
/// refused by name up front.
pub const RESERVED_ALIASES: &[&str] = &["memory", "system", "temp", "main"];

/// Check a caller-supplied catalog alias. `Ok(())` or the reason it is refused.
pub fn validate_alias(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err(format!(
            "catalog name must not be empty — it is the alias you will write in SQL. A name is \
             {ALIAS_RULE}."
        ));
    }
    if name.len() > crate::pond_name::MAX_LEN {
        return Err(format!(
            "catalog name is {} characters, over the {}-character maximum. A name is {ALIAS_RULE}.",
            name.len(),
            crate::pond_name::MAX_LEN,
        ));
    }
    if let Some(bad) = name
        .chars()
        .find(|c| !(c.is_ascii_alphanumeric() || *c == '_' || *c == '-'))
    {
        return Err(format!(
            "catalog name '{name}' contains '{bad}', which is not allowed. A name is {ALIAS_RULE}."
        ));
    }
    if RESERVED_ALIASES.contains(&name.to_lowercase().as_str()) {
        return Err(format!(
            "catalog name '{name}' is reserved by DuckDB (reserved: {}). Choose another alias.",
            RESERVED_ALIASES.join(", ")
        ));
    }
    Ok(())
}

/// Why an attach-time `--option` key was refused. Both variants name the fix;
/// neither is a "close enough" substitution (invariant 13b).
#[derive(Debug, PartialEq, Eq)]
pub enum OptionRejection {
    /// The key is a credential for this type. It has a home — `--secret` — and
    /// naming it is the whole fix. Refused rather than accepted-and-used,
    /// because an option is echoed back by `list_attached_catalogs`.
    Credential { key: String, type_: String },
    /// The key is not one this type knows. Refused rather than dropped: a
    /// dropped typo is a locator the caller thinks it set, and the attach then
    /// fails against the wrong endpoint (or silently succeeds against a default
    /// one).
    Unknown {
        key: String,
        type_: String,
        allowed: Vec<&'static str>,
    },
}

impl std::fmt::Display for OptionRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Credential { key, type_ } => write!(
                f,
                "'{key}' is a credential for catalog type '{type_}' and must be supplied with \
                 --secret {key}=<value> (MCP/gRPC: the `secrets` field), never --option: an \
                 option is echoed back by list_attached_catalogs"
            ),
            Self::Unknown {
                key,
                type_,
                allowed,
            } => write!(
                f,
                "unknown option '{key}' for catalog type '{type_}' (supported: {})",
                allowed.join(", ")
            ),
        }
    }
}

/// Check every attach-time `--option` key against the type's allowlist.
///
/// Unlike [`filter_params`] — which serves `catalog add`, where a dropped
/// credential was the registry's security boundary — this REFUSES rather than
/// drops, because at attach time there is no later call to re-supply anything
/// at: what is dropped here is simply not applied.
///
/// An unknown TYPE is left permissive; the attacher refuses it by name with the
/// legal set, which is a better message than one about its options.
pub fn check_options<'a>(
    type_: &str,
    keys: impl Iterator<Item = &'a str>,
) -> Result<(), OptionRejection> {
    let Some(spec) = lookup(type_) else {
        return Ok(());
    };
    for k in keys {
        if spec.allowed_params.contains(&k) {
            continue;
        }
        return Err(if spec.secret_params.contains(&k) {
            OptionRejection::Credential {
                key: k.to_string(),
                type_: type_.to_string(),
            }
        } else {
            OptionRejection::Unknown {
                key: k.to_string(),
                type_: type_.to_string(),
                allowed: spec.allowed_params.to_vec(),
            }
        });
    }
    Ok(())
}

/// Check every attach-time `--secret` key against the type's credential set.
/// Symmetric to [`check_options`] and refused for the same reason: a secret key
/// this type never reads is a credential the caller believes is in force.
pub fn check_secrets<'a>(
    type_: &str,
    keys: impl Iterator<Item = &'a str>,
) -> Result<(), OptionRejection> {
    let Some(spec) = lookup(type_) else {
        return Ok(());
    };
    for k in keys {
        if spec.secret_params.contains(&k) {
            continue;
        }
        return Err(OptionRejection::Unknown {
            key: k.to_string(),
            type_: type_.to_string(),
            allowed: spec.secret_params.to_vec(),
        });
    }
    Ok(())
}

pub fn lookup(type_: &str) -> Option<&'static CatalogTypeSpec> {
    TYPES.iter().find(|t| t.name == type_)
}

pub fn is_known_type(type_: &str) -> bool {
    lookup(type_).is_some()
}

/// Filter REGISTRY params to the type's allowlist. Returns `(kept, dropped)`.
/// Unknown types are left permissive — the attach call fails loudly later.
///
/// This serves `catalog add`, whose entry is discovery metadata an agent reads
/// to learn what to pass to `attach_catalog`. Dropping (rather than refusing) a
/// credential is right there and nowhere else: the registry must never persist
/// one, and the caller is told which keys were dropped and where they belong.
/// At ATTACH time the same key is refused — see [`check_options`].
pub fn filter_params(
    type_: &str,
    params: &BTreeMap<String, String>,
) -> (BTreeMap<String, String>, Vec<String>) {
    match lookup(type_) {
        Some(spec) => {
            let mut kept = BTreeMap::new();
            let mut dropped = Vec::new();
            for (k, v) in params {
                if spec.allowed_params.contains(&k.as_str()) {
                    kept.insert(k.clone(), v.clone());
                } else {
                    dropped.push(k.clone());
                }
            }
            dropped.sort();
            (kept, dropped)
        }
        None => (params.clone(), Vec::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iceberg_is_known_and_drops_credentials_at_add() {
        assert!(is_known_type("iceberg"));
        assert!(!is_known_type("nope"));
        let params = BTreeMap::from([
            ("endpoint".to_string(), "https://polaris/api".to_string()),
            ("warehouse".to_string(), "prod".to_string()),
            ("token".to_string(), "secret-bearer".to_string()),
            ("s3_secret_key".to_string(), "AKIA…".to_string()),
        ]);
        let (kept, dropped) = filter_params("iceberg", &params);
        assert!(kept.contains_key("endpoint") && kept.contains_key("warehouse"));
        // Credentials never persist.
        assert!(!kept.contains_key("token") && !kept.contains_key("s3_secret_key"));
        assert_eq!(
            dropped,
            vec!["s3_secret_key".to_string(), "token".to_string()]
        );
    }

    /// The `--option` / `--secret` split is the security boundary of the attach
    /// surface: an option is echoed back by `list_attached_catalogs`, a secret
    /// never is. A credential that arrived through `--option` would therefore be
    /// readable through a surface, which is precisely the property `Secret`
    /// exists to hold — so it is refused, and the refusal NAMES the flag it
    /// belongs on rather than saying "invalid".
    #[test]
    fn a_credential_key_passed_as_an_option_is_refused_naming_secret() {
        let Err(rejection) = check_options("iceberg", ["endpoint", "token"].into_iter()) else {
            panic!("a credential key must not be accepted as an option");
        };
        assert_eq!(
            rejection,
            OptionRejection::Credential {
                key: "token".into(),
                type_: "iceberg".into()
            },
            "the refusal must say it is a CREDENTIAL, not merely unknown — the two have \
             different fixes"
        );
        let msg = rejection.to_string();
        assert!(
            msg.contains("--secret token="),
            "the message is the whole instruction here: {msg}"
        );
        // Every type's own credential set is covered, so a type added later
        // cannot leave a credential accepted as an option.
        let mut checked = 0;
        for t in TYPES {
            for cred in t.secret_params {
                assert!(
                    matches!(
                        check_options(t.name, [*cred].into_iter()),
                        Err(OptionRejection::Credential { .. })
                    ),
                    "catalog type '{}' accepts its credential '{cred}' as an option",
                    t.name
                );
                checked += 1;
            }
        }
        assert!(checked >= 5, "the loop did no work: {checked} credentials");
    }

    /// An unknown option is REFUSED naming the legal set, never dropped
    /// (invariant 13(c)): a dropped typo is a locator the caller thinks it set,
    /// and the attach then runs against a default endpoint instead of theirs.
    #[test]
    fn an_unknown_option_is_refused_naming_the_legal_set() {
        let Err(rejection) = check_options("iceberg", ["endpont"].into_iter()) else {
            panic!("a typo'd option must be refused");
        };
        let msg = rejection.to_string();
        assert!(msg.contains("endpont"), "{msg}");
        for allowed in lookup("iceberg").unwrap().allowed_params {
            assert!(
                msg.contains(allowed),
                "the refusal must name the legal set, and '{allowed}' is missing: {msg}"
            );
        }
        // The happy direction, so the test cannot pass by refusing everything.
        assert!(check_options("iceberg", ["endpoint", "warehouse"].into_iter()).is_ok());
    }

    /// The mirror: a secret key the type never reads is a credential the caller
    /// believes is in force. Refused, with the type's real credential set named.
    #[test]
    fn an_unknown_secret_is_refused_and_a_locator_is_not_a_secret() {
        // A LOCATOR passed as a secret: it would never reach the attach at all.
        let Err(rejection) = check_secrets("iceberg", ["endpoint"].into_iter()) else {
            panic!("a locator must not be accepted as a secret");
        };
        let msg = rejection.to_string();
        assert!(msg.contains("token"), "name the real credential set: {msg}");
        assert!(check_secrets("iceberg", ["token", "s3_access_key"].into_iter()).is_ok());
        // ducklake has no bearer credential at all, so `token` is unknown there
        // — the per-type set is real, not a shared list.
        assert!(check_secrets("ducklake", ["token"].into_iter()).is_err());
        assert!(check_secrets("ducklake", ["s3_access_key"].into_iter()).is_ok());
    }

    /// `bearer_param` is what `passthrough` fills. A type that names one must
    /// name a key it actually accepts as a credential, or passthrough would
    /// build a secret the attacher then refuses.
    #[test]
    fn a_types_bearer_param_is_one_of_its_own_credentials() {
        let mut with_bearer = 0;
        for t in TYPES {
            if let Some(b) = t.bearer_param {
                assert!(
                    t.secret_params.contains(&b),
                    "catalog type '{}' fills passthrough into '{b}', which is not one of its \
                     credentials {:?}",
                    t.name,
                    t.secret_params
                );
                with_bearer += 1;
            }
        }
        assert_eq!(
            with_bearer, 1,
            "iceberg is the one bearer-consuming type today; a new one must be considered here"
        );
    }

    /// The alias becomes a SQL catalog identifier, so its legal set must stay
    /// EQUAL to a pond name's — an agent that can name a pond must be able to
    /// name a catalog with the same characters, and a rule that drifted would
    /// make one of the two sentences a lie. Asserted as an equality over real
    /// candidates rather than by comparing the two prose strings (which differ
    /// on purpose).
    #[test]
    fn a_catalog_alias_takes_exactly_the_characters_a_pond_name_does() {
        let mut compared = 0;
        for candidate in [
            "lake",
            "my_lake",
            "my-lake-2",
            "0",
            "a b",
            "a/b",
            "a.b",
            "a\"b",
            "",
            "x".repeat(crate::pond_name::MAX_LEN).as_str(),
            "x".repeat(crate::pond_name::MAX_LEN + 1).as_str(),
        ] {
            // `main`/`memory` are refused as aliases and accepted as pond names,
            // so they are deliberately not in the list above — the shared rule is
            // about CHARACTERS and LENGTH, and the reserved set is the one
            // documented difference (asserted separately below).
            assert_eq!(
                validate_alias(candidate).is_ok(),
                crate::pond_name::validate(candidate).is_ok(),
                "'{candidate}' is legal as one identifier and not the other"
            );
            compared += 1;
        }
        assert_eq!(compared, 11, "the loop skipped a candidate");
    }

    /// The one documented difference from a pond name: DuckDB's own catalog
    /// names. Attaching over `memory` shadows something the engine needs, so it
    /// is refused BY NAME with the reserved set rather than failing inside
    /// DuckDB with a message about an unrelated statement.
    #[test]
    fn a_reserved_duckdb_catalog_name_is_refused_naming_the_set() {
        for reserved in RESERVED_ALIASES {
            let Err(msg) = validate_alias(reserved) else {
                panic!("'{reserved}' must not be usable as a catalog alias");
            };
            assert!(msg.contains("reserved"), "{msg}");
            // Case is not a loophole: DuckDB resolves catalog names
            // case-insensitively, so `MEMORY` is the same shadow.
            assert!(validate_alias(&reserved.to_uppercase()).is_err());
        }
        assert!(!RESERVED_ALIASES.is_empty());
        assert!(
            validate_alias("lake").is_ok(),
            "a normal alias still passes"
        );
    }

    /// An unknown type is left permissive on purpose: the attacher refuses it by
    /// name with the supported set, which is a more useful message than one
    /// about the options of a type that does not exist.
    #[test]
    fn an_unknown_type_defers_to_the_attachers_own_refusal() {
        assert!(check_options("snowflake", ["anything"].into_iter()).is_ok());
        assert!(check_secrets("snowflake", ["anything"].into_iter()).is_ok());
    }
}
