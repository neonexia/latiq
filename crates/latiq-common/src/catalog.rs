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

//! External-catalog type metadata: which `--set` params each type may
//! **persist** at `catalog add`, and which DuckDB extensions its attacher needs.
//!
//! The allowlist is the security boundary (see swarm's invariant #1): only
//! *locator metadata* (endpoint, warehouse, region, …) is persisted; anything
//! credential-shaped (`token`, `s3_secret_key`, …) is dropped at registration
//! and may only be supplied at `pull`/`describe` time. The attacher
//! (`latiq-engine-duckdb`) maps the merged param set to `CREATE SECRET`/`ATTACH`.
use std::collections::BTreeMap;

/// One supported external-catalog type. Adding a type is a row in [`TYPES`]
/// plus an attacher arm in `latiq-engine-duckdb`; nothing else keys off the
/// type name.
pub struct CatalogTypeSpec {
    pub name: &'static str,
    /// Params that MAY be persisted at `catalog add` (locator metadata only —
    /// never credentials). Anything else is dropped before storage.
    pub allowed_params: &'static [&'static str],
    /// DuckDB extensions the attacher needs (must be baked into the image).
    pub required_extensions: &'static [&'static str],
}

/// Supported external-catalog types. Iceberg first; add a row + an attacher impl
/// in `latiq-engine-duckdb` to support a new type.
pub const TYPES: &[CatalogTypeSpec] = &[
    CatalogTypeSpec {
        name: "iceberg",
        // endpoint + warehouse locate the REST catalog; s3_endpoint/s3_region
        // locate the storage backend. Credentials (token, s3_access_key,
        // s3_secret_key) are deliberately NOT here — they ride in at
        // pull/describe and are never stored.
        allowed_params: &["endpoint", "warehouse", "s3_endpoint", "s3_region"],
        required_extensions: &["iceberg", "httpfs"],
    },
    CatalogTypeSpec {
        name: "ducklake",
        // A DuckLake catalog = a metadata DB + a data path. Local (file metadata +
        // local/S3 data) needs no credentials; remote (postgres metadata, S3 data)
        // brings its creds in at pull (s3_access_key/s3_secret_key/etc.).
        allowed_params: &["metadata_path", "data_path", "s3_endpoint", "s3_region"],
        required_extensions: &["ducklake", "httpfs"],
    },
];

pub fn lookup(type_: &str) -> Option<&'static CatalogTypeSpec> {
    TYPES.iter().find(|t| t.name == type_)
}

pub fn is_known_type(type_: &str) -> bool {
    lookup(type_).is_some()
}

/// Filter add-time params to the type's allowlist. Returns `(kept, dropped)`.
/// Unknown types are left permissive — the attach call fails loudly later.
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
}
