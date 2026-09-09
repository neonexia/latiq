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

//! DuckDB extension catalog — the one list, and the text that advertises it.
//!
//! **Install broadly, load narrowly.** Installing an extension costs the node
//! DISK (a file under `~/.duckdb/extensions/<version>/<platform>/`, baked into
//! the image by `latiq warm-extensions`); *loading* one costs a pond MEMORY, and
//! there is one DuckDB instance per pond (invariant 7). So the always-loaded set
//! — what every pond pays for — stays at the handful the whole product is built
//! on, while the requestable set is installed on every node and `LOAD`ed only
//! into a pond that asked: a pond that never wants `spatial` never pays for it,
//! and a pond that does never waits for a download.
//!
//! Two of the always-loaded five (`parquet`, `json`) are statically linked into
//! the binary via duckdb cargo features and are present with no `INSTALL` at all.
//! CSV is in DuckDB's core and is not an extension at all — which is why it is
//! not in this table and why the advertising text names it separately.
//!
//! **Signed/official only.** Everything here installs from DuckDB's *core*
//! extension repository — `INSTALL <name>` with no `FROM community`. Community
//! extensions are deliberately excluded (they are signed by their authors, not
//! by DuckDB) and [`validate`] says so when it rejects one. To add one you would
//! bake it into the image and upgrade Latiq.
//!
//! **This table is the only list.** `latiq://dialect` renders its capability
//! section from [`EXTENSIONS`], so the advertisement cannot fall behind the
//! code (`latiq-mcp`'s `mcp_resources_the_dialect_page_advertises_every_shipped_extension`).
//! Adding a row is how a capability gets advertised; there is no second copy to
//! remember.

use std::sync::LazyLock;

/// Which question an extension answers. The buckets are how the capability is
/// *advertised* (`latiq://dialect` groups by them), because "what can this pond
/// read?", "what can it reach?" and "where can the bytes live?" are different
/// questions an agent asks at different moments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Bucket {
    /// **How catalog metadata is fetched** — Iceberg, DuckLake.
    Catalog,
    /// **Where the bytes physically sit** — http(s), S3.
    Transport,
    /// **What the files look like** — Parquet, JSON, GeoJSON.
    Format,
    /// **What the engine can compute** — types, indexes and functions that are
    /// neither a wire nor a file. A fourth bucket on purpose: calling `icu` or
    /// `fts` a "format" in an agent-facing document would be a lie.
    Engine,
}

impl Bucket {
    pub const ALL: &'static [Bucket] = &[
        Bucket::Catalog,
        Bucket::Transport,
        Bucket::Format,
        Bucket::Engine,
    ];

    /// The heading `latiq://dialect` renders this bucket under.
    pub fn title(self) -> &'static str {
        match self {
            Bucket::Catalog => "Catalogs it can reach",
            Bucket::Transport => "Where the bytes can live",
            Bucket::Format => "Formats it can read",
            Bucket::Engine => "What the engine can compute",
        }
    }
}

/// One shipped DuckDB extension.
pub struct Ext {
    /// The `INSTALL`/`LOAD` name, and the name an agent passes to
    /// `allocate_pond { extensions: [...] }` / `pond create --extensions`.
    pub name: &'static str,
    pub bucket: Bucket,
    /// `true` → loaded into **every** pond (the [`STANDARD`] set): always there,
    /// naming it in a request is a harmless no-op. `false` → installed on the
    /// node and `LOAD`ed only for a pond that asked for it (the [`OPTIONAL`]
    /// set).
    pub always_loaded: bool,
    /// What this lets a pond read or reach, in one agent-facing clause. This is
    /// the text `latiq://dialect` publishes verbatim — write it for a model, not
    /// for us.
    pub what: &'static str,
}

/// **The** list. The two sets below, and the capability section of
/// `latiq://dialect`, are derived from it.
pub const EXTENSIONS: &[Ext] = &[
    // ---- catalog / protocol ------------------------------------------------
    Ext {
        name: "ducklake",
        bucket: Bucket::Catalog,
        always_loaded: true,
        what: "the pond's own storage format, and other DuckLake catalogs",
    },
    Ext {
        // NOT in either set: `iceberg` is pulled in by the catalog TYPE that
        // needs it (`latiq_common::catalog::TYPES`), and `warm_extensions`
        // installs it at node startup from there. It is listed here so the
        // advertisement can name it — `always_loaded: false` and NOT in
        // `OPTIONAL`, because a pond does not request it: `attachers.rs` LOADs
        // it on the transient attach that `pull_catalog` performs.
        name: "iceberg",
        bucket: Bucket::Catalog,
        always_loaded: false,
        what: "Apache Iceberg tables via a REST catalog (pull_catalog)",
    },
    // ---- transport ---------------------------------------------------------
    Ext {
        name: "httpfs",
        bucket: Bucket::Transport,
        always_loaded: true,
        what: "http(s):// URLs and s3:// object storage, read straight from SQL",
    },
    // ---- format ------------------------------------------------------------
    Ext {
        name: "parquet",
        bucket: Bucket::Format,
        always_loaded: true,
        what: "Parquet files (statically linked; always present)",
    },
    Ext {
        name: "json",
        bucket: Bucket::Format,
        always_loaded: true,
        what: "JSON and newline-delimited JSON (statically linked)",
    },
    Ext {
        name: "spatial",
        bucket: Bucket::Format,
        always_loaded: false,
        what: "geospatial: GeoJSON/Shapefile/GeoPackage and the ST_* functions",
    },
    // ---- engine ------------------------------------------------------------
    Ext {
        name: "icu",
        bucket: Bucket::Engine,
        always_loaded: true,
        what: "time zones (TIMESTAMPTZ arithmetic) and Unicode collations",
    },
    Ext {
        name: "fts",
        bucket: Bucket::Engine,
        always_loaded: false,
        what: "full-text search: BM25 ranking over a text column",
    },
    Ext {
        name: "inet",
        bucket: Bucket::Engine,
        always_loaded: false,
        what: "the INET type: IP addresses and CIDR containment",
    },
];

/// Extensions that are neither always-loaded nor agent-requestable: the node
/// installs them because a *catalog type* needs them, and the attacher LOADs
/// them for the transient attach. `iceberg` is the only one — `ducklake` and
/// `httpfs`, its co-requisites, are already standard.
///
/// They are in [`EXTENSIONS`] so the capability can be advertised, and excluded
/// from [`OPTIONAL`] so `allocate_pond { extensions: ["iceberg"] }` is still
/// refused: loading it into a pond buys nothing, because catalogs are attached
/// transiently by `pull_catalog` and never queried live.
fn is_catalog_driven(name: &str) -> bool {
    crate::catalog::TYPES
        .iter()
        .any(|t| t.required_extensions.contains(&name))
}

/// Always present and loaded on every pond (informational + validation: a request
/// naming one of these is a no-op, not an error). Derived from [`EXTENSIONS`].
pub static STANDARD: LazyLock<Vec<&'static str>> =
    LazyLock::new(|| names_where(|e| e.always_loaded));

/// Signed/official extensions an agent may request via `allocate_pond`
/// (`pond create --extensions`). Installed on every node at startup — and baked
/// into the image — but `LOAD`ed only into a pond that asks. Anything not here
/// (typos, community/unsigned extensions) is rejected.
pub static OPTIONAL: LazyLock<Vec<&'static str>> =
    LazyLock::new(|| names_where(|e| !e.always_loaded && !is_catalog_driven(e.name)));

/// Extensions the node installs because a **catalog type** declares them
/// ([`crate::catalog::TYPES`]), not because a pond can request them.
///
/// They are warmed at node startup for the same reason [`OPTIONAL`] is:
/// `attachers.rs` emits `LOAD <ext>;` for the transient attach a `pull_catalog`
/// performs, with autoinstall **off**, so an extension that was not warmed does
/// not download at exactly the moment an agent is waiting on the call — it
/// fails the call, naming `latiq warm-extensions`. (That site used to emit
/// `INSTALL <ext>; LOAD <ext>;`, which is precisely the download this list
/// exists to prevent.) Before this, `iceberg` was only ever installed by the
/// first attach that needed it —
/// present on a long-lived dev node by accident, absent on a fresh one — while
/// the image's own comment claimed it was baked in.
///
/// **It also carries transitive dependencies**, which is not a nicety. Measured
/// against the pinned DuckDB: `INSTALL iceberg` installs *only* `iceberg`, and
/// `LOAD iceberg` then autoinstalls `avro` — over the network, at load time. So
/// a node that warmed `iceberg` alone still downloads on its first
/// `pull_catalog`, and (now that `autoinstall_known_extensions` is **off** on
/// every pond connection) could not load iceberg there at all. Warming the
/// closure is what makes the offline claim true; the guard is
/// `latiq-engine-duckdb`'s
/// `a_warmed_cache_loads_every_extension_we_ever_load_without_the_network`,
/// which warms into a scratch `extension_directory` so the ambient cache cannot
/// hide a gap.
pub static CATALOG_DRIVEN: LazyLock<Vec<&'static str>> = LazyLock::new(|| {
    let mut out: Vec<&'static str> = Vec::new();
    for t in crate::catalog::TYPES {
        for e in t.required_extensions {
            for name in [*e].into_iter().chain(transitive_deps(e).iter().copied()) {
                if !STANDARD.contains(&name) && !out.contains(&name) {
                    out.push(name);
                }
            }
        }
    }
    out
});

/// What DuckDB itself pulls in when an extension is LOADed. Deliberately not in
/// [`crate::catalog::TYPES`]`::required_extensions`, which is the list the
/// attacher `LOAD`s: DuckDB resolves this one on its own once the file is on
/// disk, so we must make sure it is on disk and must not pretend to own its
/// load order.
fn transitive_deps(name: &str) -> &'static [&'static str] {
    match name {
        "iceberg" => &["avro"],
        _ => &[],
    }
}

fn names_where(pred: fn(&Ext) -> bool) -> Vec<&'static str> {
    EXTENSIONS
        .iter()
        .filter(|e| pred(e))
        .map(|e| e.name)
        .collect()
}

/// The extensions in one bucket, in declaration order — what `latiq://dialect`
/// renders under that bucket's heading.
pub fn in_bucket(bucket: Bucket) -> impl Iterator<Item = &'static Ext> {
    EXTENSIONS.iter().filter(move |e| e.bucket == bucket)
}

/// Look up one extension by its `INSTALL` name.
pub fn lookup(name: &str) -> Option<&'static Ext> {
    EXTENSIONS.iter().find(|e| e.name == name)
}

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
    use std::collections::BTreeSet;

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

    /// `iceberg` is advertised and really installed, and is still NOT something
    /// a pond may request: it is loaded for the transient attach `pull_catalog`
    /// makes, and loading it into a pond buys nothing. Advertising a capability
    /// must not silently widen the allowlist.
    #[test]
    fn a_catalog_driven_extension_is_advertised_but_not_pond_requestable() {
        assert!(lookup("iceberg").is_some(), "iceberg must be advertisable");
        assert!(!OPTIONAL.contains(&"iceberg"));
        assert!(!STANDARD.contains(&"iceberg"));
        let err = validate(&["iceberg".into()]).unwrap_err();
        assert!(
            err.contains("iceberg") && err.contains("Allowed:"),
            "the refusal must name it and the legal set: {err}"
        );
    }

    #[test]
    fn parse_csv_trims_and_drops_blanks() {
        assert_eq!(parse_csv(" spatial , ,fts "), vec!["spatial", "fts"]);
        assert!(parse_csv("").is_empty());
    }

    /// The derived sets are pinned by NAME. Widening the always-loaded set is
    /// what every pond pays memory for, and widening the optional set is what
    /// the image pays disk for — both are deliberate acts, not a diff nobody
    /// noticed.
    #[test]
    fn the_shipped_sets_are_exactly_these() {
        let all: BTreeSet<&str> = EXTENSIONS.iter().map(|e| e.name).collect();
        assert_eq!(all.len(), EXTENSIONS.len(), "a name is listed twice");
        for s in STANDARD.iter() {
            assert!(!OPTIONAL.contains(s), "{s} is in both sets");
        }
        // `parquet`/`json` are statically linked; `ducklake` is the pond itself;
        // `httpfs` is how data arrives; `icu` is what makes TIMESTAMPTZ
        // arithmetic (DuckLake's own snapshot maintenance) bind.
        assert_eq!(
            *STANDARD,
            vec!["ducklake", "httpfs", "parquet", "json", "icu"],
            "STANDARD changed — every pond now pays memory for this."
        );
        assert_eq!(
            *OPTIONAL,
            vec!["spatial", "fts", "inet"],
            "OPTIONAL changed — every image now pays disk for this."
        );
    }

    /// Every extension a catalog type needs is in the one table (so it is
    /// advertised) and in [`CATALOG_DRIVEN`] (so the node installs it at
    /// startup instead of on some agent's first `pull_catalog`).
    #[test]
    fn every_catalog_types_extension_is_shipped_and_warmed() {
        let mut checked = 0;
        for t in crate::catalog::TYPES {
            for e in t.required_extensions {
                assert!(
                    lookup(e).is_some(),
                    "catalog type '{}' needs '{e}', which is in no shipped set — \
                     it would install on first attach, over the network",
                    t.name
                );
                assert!(
                    STANDARD.contains(e) || CATALOG_DRIVEN.contains(e),
                    "'{e}' is not warmed at node startup"
                );
                checked += 1;
            }
        }
        assert!(checked >= 4, "the loop skipped: only {checked} checked");
        // `avro` is here and in no other list on purpose: DuckDB autoinstalls it
        // when `iceberg` LOADs, so the node must have it on disk — but a pond
        // cannot request it and it is not advertised, because nothing in Latiq
        // gives a pond a way to use it.
        assert_eq!(*CATALOG_DRIVEN, vec!["iceberg", "avro"]);
        assert!(
            lookup("avro").is_none() && !OPTIONAL.contains(&"avro"),
            "avro is a transitive dependency, not a capability we offer"
        );
    }

    /// Every extension carries a non-empty, agent-facing blurb, because that
    /// text IS the advertisement (`latiq://dialect` renders it verbatim). An
    /// entry with no `what` ships as a bare name a model cannot rank.
    #[test]
    fn every_extension_describes_what_it_unlocks() {
        for e in EXTENSIONS {
            assert!(
                e.what.len() > 15,
                "{} has no usable description: {:?}",
                e.name,
                e.what
            );
            assert!(
                e.name == e.name.to_lowercase() && !e.name.is_empty(),
                "{} must be the lowercase INSTALL name",
                e.name
            );
        }
        // Every bucket is populated — an empty heading on the dialect page reads
        // as "Latiq cannot do this at all".
        for b in Bucket::ALL {
            assert!(in_bucket(*b).next().is_some(), "bucket {b:?} is empty");
        }
    }
}
