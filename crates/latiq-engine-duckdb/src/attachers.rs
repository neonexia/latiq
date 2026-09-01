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

//! Per-type external-catalog attachers: map a catalog's merged params into the
//! DuckDB `LOAD` + `CREATE SECRET` + `ATTACH` SQL. Everything is transient — the
//! pull path runs `load → secrets → attach → <query> → detach + drop secrets`, so
//! no credential ever persists (latiq stores none). Iceberg first; add a match
//! arm + the type's row in `latiq_common::catalog` to support a new type.
use crate::instance::quote_ident;
use latiq_engine::EngineError;
use std::collections::BTreeMap;

/// The SQL to mount an external catalog, and to tear it back down.
pub struct AttachPlan {
    pub alias: String,
    /// How the SOURCE identifies itself, for lineage: the catalog's own locator,
    /// scheme included and otherwise unmodified (`ducklake:<metadata>`, the
    /// iceberg REST endpoint + warehouse). `alias` is a pond-local name nobody
    /// else can join on, so a dataset pulled from here is filed under this
    /// instead — the same rule `latiq_common::DatasetRef::external` follows for
    /// a file or an object. Two warehouses behind one endpoint are two
    /// catalogs, so the warehouse is part of it.
    pub namespace: String,
    /// `INSTALL …; LOAD …;` for the type's extensions.
    pub load: Vec<String>,
    /// `(secret_name, CREATE SECRET …)` — dropped on detach.
    pub secrets: Vec<(String, String)>,
    /// `ATTACH … AS <alias> (…)`.
    pub attach: String,
}

impl AttachPlan {
    /// `DETACH` + drop every secret this plan created.
    pub fn teardown(&self) -> Vec<String> {
        let mut out = vec![format!("DETACH {}", quote_ident(&self.alias))];
        for (name, _) in &self.secrets {
            out.push(format!("DROP SECRET IF EXISTS {name}"));
        }
        out
    }
}

fn esc(v: &str) -> String {
    v.replace('\'', "''")
}

fn err(msg: impl Into<String>) -> EngineError {
    EngineError::Engine(msg.into())
}

/// Build the attach plan for a catalog `type_`, mounting it as `alias`, from the
/// merged (add ⊕ pull) params.
pub fn plan(
    type_: &str,
    alias: &str,
    params: &BTreeMap<String, String>,
) -> Result<AttachPlan, EngineError> {
    let load = latiq_common::catalog::lookup(type_)
        .map(|s| {
            s.required_extensions
                .iter()
                .map(|e| format!("INSTALL {e}; LOAD {e};"))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    match type_ {
        "iceberg" => iceberg(alias, params, load),
        "ducklake" => ducklake(alias, params, load),
        other => Err(err(format!(
            "unsupported catalog type '{other}' (supported: iceberg, ducklake)"
        ))),
    }
}

/// A DuckLake catalog: `ATTACH 'ducklake:<metadata>' AS <alias> (DATA_PATH …,
/// READ_ONLY)`. Optional S3 storage creds (for data on MinIO/S3) build an `s3`
/// secret. We only ever read from it, so the attach is read-only.
fn ducklake(
    alias: &str,
    params: &BTreeMap<String, String>,
    load: Vec<String>,
) -> Result<AttachPlan, EngineError> {
    let metadata = params
        .get("metadata_path")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| err("ducklake catalog requires --set metadata_path=<catalog-db>"))?;
    let data_path = params
        .get("data_path")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| err("ducklake catalog requires --set data_path=<data-dir-or-s3-uri>"))?;
    let mut secrets: Vec<(String, String)> = Vec::new();
    if let Some(line) = s3_secret_line(alias, params) {
        secrets.push(line);
    }
    let attach = format!(
        "ATTACH 'ducklake:{}' AS {} (DATA_PATH '{}', READ_ONLY)",
        esc(metadata),
        quote_ident(alias),
        esc(data_path),
    );
    Ok(AttachPlan {
        alias: alias.to_string(),
        namespace: format!("ducklake:{metadata}"),
        load,
        secrets,
        attach,
    })
}

/// Build an `s3` secret from `s3_access_key`/`s3_secret_key` (+ endpoint/region)
/// for catalogs whose storage backend is MinIO/S3. SigV4 keys ride in at pull.
///
/// **One implementation for every catalog type** with an S3-backed store
/// (`ducklake`; `iceberg` behind MinIO/Polaris). It was duplicated per type,
/// which meant a fix to the escaping — the injection-adjacent part, since both
/// keys are caller-supplied — could land on one copy and silently miss the
/// other.
fn s3_secret_line(alias: &str, params: &BTreeMap<String, String>) -> Option<(String, String)> {
    let k = params.get("s3_access_key").filter(|s| !s.is_empty())?;
    let s = params.get("s3_secret_key").filter(|s| !s.is_empty())?;
    let name = format!("_latiq_{alias}_s3");
    let mut lines = vec![
        "TYPE s3".to_string(),
        format!("KEY_ID '{}'", esc(k)),
        format!("SECRET '{}'", esc(s)),
        "URL_STYLE 'path'".to_string(),
    ];
    if let Some(region) = params.get("s3_region").filter(|s| !s.is_empty()) {
        lines.push(format!("REGION '{}'", esc(region)));
    }
    if let Some(ep) = params.get("s3_endpoint").filter(|s| !s.is_empty()) {
        // DuckDB's s3 ENDPOINT wants host:port; the scheme rides in USE_SSL.
        let (host, ssl) = if let Some(rest) = ep.strip_prefix("http://") {
            (rest, "false")
        } else if let Some(rest) = ep.strip_prefix("https://") {
            (rest, "true")
        } else {
            (ep.as_str(), "true")
        };
        lines.push(format!("ENDPOINT '{}'", esc(host)));
        lines.push(format!("USE_SSL {ssl}"));
    }
    Some((
        name.clone(),
        format!("CREATE OR REPLACE SECRET {name} ({})", lines.join(", ")),
    ))
}

/// Iceberg REST catalog (Polaris / vendor-hosted). The bearer rides in via the
/// `token` param at pull time → an `iceberg` secret. Optional S3 storage backend
/// creds (SigV4) build an `s3` secret. See swarm's `IcebergHandler` + DuckDB's
/// iceberg REST-catalog docs.
fn iceberg(
    alias: &str,
    params: &BTreeMap<String, String>,
    load: Vec<String>,
) -> Result<AttachPlan, EngineError> {
    let endpoint = params
        .get("endpoint")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| err("iceberg catalog requires --set endpoint=<rest-url>"))?;
    let warehouse = params
        .get("warehouse")
        .map(|s| s.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or("warehouse");
    let mut secrets: Vec<(String, String)> = Vec::new();

    // Catalog auth: the caller's bearer (or an explicit --set token=…).
    let secret_clause = if let Some(tok) = params.get("token").filter(|s| !s.is_empty()) {
        let name = format!("_latiq_{alias}_iceberg");
        secrets.push((
            name.clone(),
            format!(
                "CREATE OR REPLACE SECRET {name} (TYPE iceberg, TOKEN '{}')",
                esc(tok)
            ),
        ));
        format!(", SECRET {name}")
    } else {
        ", AUTHORIZATION_TYPE none".to_string()
    };

    // Optional S3 storage backend (e.g. MinIO behind Polaris). SigV4 keys —
    // same builder as every other S3-backed catalog type, see `s3_secret_line`.
    if let Some(line) = s3_secret_line(alias, params) {
        secrets.push(line);
    }

    let attach = format!(
        "ATTACH '{}' AS {} (TYPE iceberg, ENDPOINT '{}'{})",
        esc(warehouse),
        quote_ident(alias),
        esc(endpoint),
        secret_clause,
    );
    Ok(AttachPlan {
        alias: alias.to_string(),
        namespace: format!("{}/{warehouse}", endpoint.trim_end_matches('/')),
        load,
        secrets,
        attach,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(kv: &[(&str, &str)]) -> BTreeMap<String, String> {
        kv.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn iceberg_with_token_builds_secret_and_attach() {
        let p = params(&[
            ("endpoint", "https://polaris/api/catalog"),
            ("warehouse", "prod"),
            ("token", "bear'er"),
        ]);
        let plan = plan("iceberg", "lake", &p).unwrap();
        assert!(plan.load.iter().any(|s| s.contains("iceberg")));
        assert_eq!(plan.secrets.len(), 1);
        assert!(plan.secrets[0].1.contains("TYPE iceberg"));
        assert!(
            plan.secrets[0].1.contains("bear''er"),
            "single quotes escaped"
        );
        assert!(plan.attach.contains("ATTACH 'prod' AS \"lake\""));
        assert!(plan
            .attach
            .contains("ENDPOINT 'https://polaris/api/catalog'"));
        assert!(plan.attach.contains("SECRET _latiq_lake_iceberg"));
        assert_eq!(plan.teardown()[0], "DETACH \"lake\"");
    }

    #[test]
    fn iceberg_without_token_uses_authorization_none() {
        let p = params(&[("endpoint", "https://x/api"), ("warehouse", "w")]);
        let plan = plan("iceberg", "lake", &p).unwrap();
        assert!(plan.secrets.is_empty());
        assert!(plan.attach.contains("AUTHORIZATION_TYPE none"));
    }

    /// S3 params common to the secret-line tests. The "secrets" here are
    /// obviously synthetic; nothing real is ever put in a test fixture.
    fn s3_params(extra: &[(&str, &str)]) -> BTreeMap<String, String> {
        let mut p = params(&[("s3_access_key", "AK"), ("s3_secret_key", "SK")]);
        p.extend(
            extra
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string())),
        );
        p
    }

    /// The key and the secret are caller-supplied and land inside SQL string
    /// literals, so this is the injection boundary: a `'` must be doubled (and
    /// nothing else may be), or the literal ends early and the rest of the
    /// value becomes SQL. Backslash is deliberately NOT touched — DuckDB's
    /// single-quoted literals have no backslash escape, so escaping it would
    /// corrupt a key that legitimately contains one.
    #[test]
    fn s3_secret_escapes_quotes_only_and_cannot_inject_clauses() {
        let p = s3_params(&[("s3_access_key", "a'b\\c"), ("s3_secret_key", "x'; DROP")]);
        let (name, sql) = s3_secret_line("lake", &p).expect("both keys present");
        assert_eq!(name, "_latiq_lake_s3");
        assert!(
            sql.contains("KEY_ID 'a''b\\c'"),
            "the quote must be doubled and the backslash left alone"
        );
        assert!(
            sql.contains("SECRET 'x''; DROP'"),
            "a quote in the secret must stay inside the literal"
        );
        // The injection property itself: whatever the values contain, the
        // statement still declares exactly one secret with exactly one of each
        // clause. A single un-doubled quote would let `; DROP` out and change
        // these counts.
        for clause in ["CREATE OR REPLACE SECRET", "TYPE s3", "KEY_ID", "SECRET '"] {
            assert_eq!(
                sql.matches(clause).count(),
                1,
                "hostile key material must not add a second `{clause}` clause"
            );
        }
    }

    /// `http://` means plaintext and `https://` means TLS; a bare host is
    /// assumed TLS. Getting this backwards either fails to connect to a MinIO
    /// dev endpoint or — the bad direction — sends SigV4 credentials in the
    /// clear to an endpoint the operator wrote as `https://`.
    #[test]
    fn s3_secret_use_ssl_follows_the_endpoint_scheme_and_strips_it() {
        for (endpoint, host, ssl) in [
            ("http://minio:9000", "minio:9000", "false"),
            ("https://s3.example.com", "s3.example.com", "true"),
            ("s3.example.com", "s3.example.com", "true"),
        ] {
            let p = s3_params(&[("s3_endpoint", endpoint)]);
            let (_, sql) = s3_secret_line("lake", &p).unwrap();
            assert!(
                sql.contains(&format!("ENDPOINT '{host}'")),
                "{endpoint}: the scheme must be stripped from ENDPOINT"
            );
            assert!(
                sql.contains(&format!("USE_SSL {ssl}")),
                "{endpoint}: expected USE_SSL {ssl}"
            );
        }
        // No endpoint at all: neither clause is emitted, so DuckDB's own AWS
        // default endpoint stands rather than being pinned to an empty host.
        let bare = s3_secret_line("lake", &s3_params(&[])).unwrap().1;
        assert!(!bare.contains("ENDPOINT"), "no endpoint param, no ENDPOINT");
        assert!(!bare.contains("USE_SSL"));
    }

    #[test]
    fn s3_secret_region_is_optional_and_escaped() {
        let with = s3_secret_line("lake", &s3_params(&[("s3_region", "eu-west-1")]))
            .unwrap()
            .1;
        assert!(with.contains("REGION 'eu-west-1'"));
        // Empty is treated as absent, not as `REGION ''` (which DuckDB would
        // take as a real, wrong region).
        let empty = s3_secret_line("lake", &s3_params(&[("s3_region", "")]))
            .unwrap()
            .1;
        assert!(!empty.contains("REGION"), "an empty region must be omitted");
    }

    #[test]
    fn s3_secret_needs_both_keys() {
        assert!(s3_secret_line("lake", &params(&[])).is_none());
        assert!(s3_secret_line("lake", &params(&[("s3_access_key", "AK")])).is_none());
        assert!(s3_secret_line("lake", &params(&[("s3_secret_key", "SK")])).is_none());
        // Present-but-empty is not "supplied": half a credential must not build
        // a secret that then fails obscurely inside DuckDB.
        assert!(s3_secret_line(
            "lake",
            &params(&[("s3_access_key", "AK"), ("s3_secret_key", "")])
        )
        .is_none());
    }

    /// The two S3-backed catalog types must build the **same** secret from the
    /// same params. This block was duplicated once; the regression it guards is
    /// a fix landing on one copy only.
    #[test]
    fn s3_secret_is_identical_across_catalog_types() {
        let mut ducklake_params = s3_params(&[("s3_endpoint", "http://minio:9000")]);
        ducklake_params.insert("metadata_path".into(), "ducklake:m.db".into());
        ducklake_params.insert("data_path".into(), "s3://b/d".into());
        let mut iceberg_params = s3_params(&[("s3_endpoint", "http://minio:9000")]);
        iceberg_params.insert("endpoint".into(), "https://polaris/api".into());

        let d = plan("ducklake", "lake", &ducklake_params).unwrap();
        let i = plan("iceberg", "lake", &iceberg_params).unwrap();
        let s3_of = |p: &AttachPlan| {
            p.secrets
                .iter()
                .find(|(n, _)| n.ends_with("_s3"))
                .expect("an s3 secret")
                .clone()
        };
        assert_eq!(
            s3_of(&d),
            s3_of(&i),
            "both catalog types must share one s3 secret builder"
        );
        // …and it really is built (not two identically-absent secrets).
        assert!(s3_of(&d).1.contains("USE_SSL false"));
    }

    #[test]
    fn missing_endpoint_errors_and_unknown_type_errors() {
        assert!(plan("iceberg", "x", &params(&[])).is_err());
        assert!(plan("snowflake", "x", &params(&[("endpoint", "y")])).is_err());
    }
}
