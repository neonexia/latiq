//! DuckDB extension catalog.
//!
//! The **standard** set is always available and loaded on every pond: `parquet`
//! and `json` are statically linked into the binary (duckdb cargo features),
//! while `ducklake` and `httpfs` are loaded from the deployment image's extension
//! directory. The **optional** set is the signed/official extensions an agent may
//! request at pond creation; the engine `LOAD`s them from the image and **never
//! installs in the pond path** (a missing one fails fast). Community/unsigned
//! extensions are intentionally excluded — to add one you bake it into the image
//! and upgrade Latiq.

/// Always present and loaded on every pond (informational + validation: a request
/// naming one of these is a no-op, not an error).
pub const STANDARD: &[&str] = &["ducklake", "httpfs", "parquet", "json"];

/// Signed/official extensions an agent may request via `pond create --extensions`.
/// Anything not here (typos, community/unsigned extensions) is rejected.
pub const OPTIONAL: &[&str] = &["spatial", "fts", "icu", "inet"];

/// Validate + normalize a requested extension list against the [`OPTIONAL`]
/// allowlist: lowercases/trims, drops blanks and standard-set names (already
/// always-loaded), dedups, and rejects anything unknown/community. Returns the
/// clean list, or a human error naming the offender and the allowed set.
pub fn validate(requested: &[String]) -> Result<Vec<String>, String> {
    let mut out: Vec<String> = Vec::new();
    for raw in requested {
        let name = raw.trim().to_lowercase();
        if name.is_empty() || STANDARD.contains(&name.as_str()) {
            continue;
        }
        if !OPTIONAL.contains(&name.as_str()) {
            return Err(format!(
                "unknown or unsupported extension '{name}'. Allowed: {}. \
                 Community/unsigned extensions aren't supported — bake new ones \
                 into the deployment image and upgrade Latiq.",
                OPTIONAL.join(", ")
            ));
        }
        if !out.contains(&name) {
            out.push(name);
        }
    }
    Ok(out)
}

/// Parse the comma-separated form used by the CLI flag and registry storage.
pub fn parse_csv(s: &str) -> Vec<String> {
    s.split(',')
        .map(|x| x.trim().to_string())
        .filter(|x| !x.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_allowed_and_dedups_and_normalizes() {
        let got = validate(&["Spatial".into(), "fts".into(), "SPATIAL".into()]).unwrap();
        assert_eq!(got, vec!["spatial".to_string(), "fts".to_string()]);
    }

    #[test]
    fn ignores_standard_set_names() {
        // parquet/json/ducklake are always loaded; naming them is a harmless no-op.
        assert_eq!(
            validate(&["parquet".into(), "spatial".into()]).unwrap(),
            vec!["spatial"]
        );
    }

    #[test]
    fn rejects_unknown_or_community() {
        let err = validate(&["lance".into()]).unwrap_err();
        assert!(err.contains("lance"), "{err}");
        assert!(err.contains("spatial"), "names the allowed set: {err}");
    }

    #[test]
    fn parse_csv_trims_and_drops_blanks() {
        assert_eq!(parse_csv(" spatial , ,fts "), vec!["spatial", "fts"]);
        assert!(parse_csv("").is_empty());
    }
}
