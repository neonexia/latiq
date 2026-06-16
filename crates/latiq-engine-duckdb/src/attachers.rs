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
        load,
        secrets,
        attach,
    })
}

/// Build an `s3` secret from `s3_access_key`/`s3_secret_key` (+ endpoint/region)
/// for catalogs whose storage backend is MinIO/S3. SigV4 keys ride in at pull.
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

    // Optional S3 storage backend (e.g. MinIO behind Polaris). SigV4 keys.
    if let (Some(k), Some(s)) = (
        params.get("s3_access_key").filter(|s| !s.is_empty()),
        params.get("s3_secret_key").filter(|s| !s.is_empty()),
    ) {
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
        secrets.push((
            name.clone(),
            format!("CREATE OR REPLACE SECRET {name} ({})", lines.join(", ")),
        ));
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

    #[test]
    fn missing_endpoint_errors_and_unknown_type_errors() {
        assert!(plan("iceberg", "x", &params(&[])).is_err());
        assert!(plan("snowflake", "x", &params(&[("endpoint", "y")])).is_err());
    }
}
