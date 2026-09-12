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

//! [`Secret`] — a credential value that cannot be printed, logged or serialized
//! by accident.
//!
//! Catalog credentials travel from an `attach_catalog` call, through the
//! protocol-neutral core, into one `CREATE SECRET` statement on the pond's
//! DuckDB connection. Every hop in between formats things: `tracing` renders
//! `?args` with `Debug`, `err_envelope` serializes `facts` with `Serialize`, a
//! `format!` in an error message uses `Display`. A bare `String` leaks at any of
//! them, and it leaks silently — nobody reviews a log line for a token that was
//! never supposed to be there.
//!
//! So the value is only readable through [`Secret::expose`], which is greppable.
//! There are exactly **two** call sites in the workspace, both deliberate:
//!
//! 1. `latiq-engine-duckdb::attachers` — building the one `CREATE SECRET`;
//! 2. `latiq-pond-node::forward_client` — the internal node-to-node hop, which
//!    has to carry the value to the node that will build that statement.
//!
//! `secret_is_exposed_only_where_it_has_to_be` pins the set, by file. A third
//! site is not forbidden by taste: every one of them is a place the value can be
//! `format!`ed, logged or serialized by the next person editing that function,
//! and the count is what makes adding one a decision rather than an accident.
use std::fmt;

/// A credential value. `Debug`, `Display` and `Serialize` all render
/// [`Secret::MASK`]; the real value comes out only via [`Secret::expose`].
///
/// Deliberately NOT `Copy`/`Deref<Target = str>`: both would hand the inner
/// string to any function taking a `&str`, which is the leak this type exists to
/// prevent.
#[derive(Clone, PartialEq, Eq, serde::Deserialize)]
#[serde(transparent)]
pub struct Secret(String);

impl Secret {
    /// What every formatting impl renders instead of the value.
    pub const MASK: &'static str = "<redacted>";

    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// The real value. **The only way out.** Call it at the point of use (the
    /// SQL statement being built) and never bind the result to anything that
    /// outlives the statement.
    pub fn expose(&self) -> &str {
        &self.0
    }

    /// Whether this carries anything at all. An empty credential is not a
    /// credential: `--secret token=` is the caller supplying nothing, and
    /// building `TOKEN ''` from it fails inside DuckDB with a message about
    /// authentication rather than about the argument.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(Self::MASK)
    }
}

impl fmt::Display for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(Self::MASK)
    }
}

impl serde::Serialize for Secret {
    /// Masked, not skipped: a field that vanishes from a response reads as
    /// "there was no credential", which is a different (and wrong) statement
    /// than "there is one and you may not see it".
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(Self::MASK)
    }
}

impl From<String> for Secret {
    fn from(v: String) -> Self {
        Self(v)
    }
}

impl From<&str> for Secret {
    fn from(v: &str) -> Self {
        Self(v.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The three formatting surfaces a credential actually escapes through:
    /// a `tracing` field (`Debug`), a `format!` in a message (`Display`), and an
    /// error envelope or tool response (`Serialize`).
    #[test]
    fn secret_is_masked_on_every_formatting_surface() {
        let s = Secret::new("hunter2");
        assert_eq!(format!("{s:?}"), Secret::MASK);
        assert_eq!(format!("{s}"), Secret::MASK);
        assert_eq!(serde_json::to_string(&s).unwrap(), "\"<redacted>\"");
        // Nested exactly as it travels: a map of them inside a response body.
        let map = std::collections::BTreeMap::from([("token".to_string(), s.clone())]);
        let rendered = serde_json::to_string(&map).unwrap();
        assert!(
            !rendered.contains("hunter2"),
            "a map of secrets must not render its values: {rendered}"
        );
        // …and the value is still reachable where it is needed.
        assert_eq!(s.expose(), "hunter2");
    }

    /// A `Debug` on the CONTAINER must not undo the mask — this is how the leak
    /// actually happens (`debug!(?args)` on a struct holding the map).
    #[test]
    fn secret_stays_masked_inside_a_derived_debug() {
        #[derive(Debug)]
        struct Attach {
            #[allow(dead_code)]
            alias: String,
            #[allow(dead_code)]
            secrets: std::collections::BTreeMap<String, Secret>,
        }
        let a = Attach {
            alias: "lake".into(),
            secrets: std::collections::BTreeMap::from([("token".into(), Secret::new("hunter2"))]),
        };
        let rendered = format!("{a:?}");
        assert!(rendered.contains("lake"), "the alias is not a secret");
        assert!(
            !rendered.contains("hunter2"),
            "a derived Debug must inherit the mask: {rendered}"
        );
    }

    /// **The containment guard: where the mask may be taken off.**
    ///
    /// A `Secret` protects the value everywhere except at an `expose()` call, so
    /// the set of those calls IS the attack surface — each one is a place the
    /// next person editing that function can `format!`, log or serialize the
    /// raw string. Two are needed and no more (see the module docs). Pinning the
    /// set by FILE, rather than counting calls, keeps it meaningful when one of
    /// those files legitimately gains a second call to the same statement
    /// builder.
    ///
    /// A `#[cfg(test)]` grep rather than an integration test (tests/CLAUDE.md
    /// rule 5): it needs no runtime deps, and every integration binary in this
    /// workspace statically links a bundled DuckDB.
    #[test]
    fn secret_is_exposed_only_where_it_has_to_be() {
        // `crates/latiq-common/src` → the workspace's `crates/`.
        let crates = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("crates/")
            .to_path_buf();
        /// The only files allowed to call `Secret::expose`, and why.
        const ALLOWED: &[(&str, &str)] = &[
            (
                "latiq-engine-duckdb/src/attachers.rs",
                "builds the one CREATE SECRET statement",
            ),
            (
                "latiq-pond-node/src/forward_client.rs",
                "carries it over the internal node-to-node hop to the node that \
                 builds that statement",
            ),
        ];

        /// Every `.rs` file under `dir`, as `(path-relative-to-crates, source)`.
        fn sources(dir: &std::path::Path, out: &mut Vec<(String, String)>, root: &std::path::Path) {
            for entry in std::fs::read_dir(dir).expect("readable").flatten() {
                let path = entry.path();
                if path.is_dir() {
                    sources(&path, out, root);
                } else if path.extension().is_some_and(|e| e == "rs") {
                    let rel = path
                        .strip_prefix(root)
                        .unwrap_or(&path)
                        .display()
                        .to_string();
                    out.push((rel, std::fs::read_to_string(&path).unwrap_or_default()));
                }
            }
        }
        let mut files = Vec::new();
        sources(&crates, &mut files, &crates);
        // Anti-vacuity (tests/CLAUDE.md rule 3): a walk that found nothing — a
        // moved directory, a changed layout — would pass this test perfectly.
        assert!(
            files.len() > 50,
            "the source walk found only {} files, so it is guarding nothing",
            files.len()
        );

        let mut found: Vec<String> = Vec::new();
        for (path, src) in &files {
            // Skip this file (it names `expose` throughout, including here) and
            // test modules, which legitimately read a value back to assert it.
            if path.ends_with("secret.rs") {
                continue;
            }
            let code = src.split("#[cfg(test)]").next().unwrap_or(src);
            if !code.contains(".expose()") {
                continue;
            }
            let normalised = path.replace('\\', "/");
            assert!(
                ALLOWED.iter().any(|(f, _)| normalised.ends_with(f)),
                "{normalised} calls Secret::expose. That is the ONE place a \
                 credential can be logged, formatted or serialized by accident — \
                 if this file really needs it, add it to ALLOWED with the reason, \
                 deliberately. Currently allowed: {ALLOWED:?}"
            );
            found.push(normalised);
        }
        // …and the allowed set is not stale: every entry must still be a real
        // call site, or the list is documenting a defence nobody has.
        for (file, why) in ALLOWED {
            assert!(
                found.iter().any(|f| f.ends_with(file)),
                "{file} is listed as needing Secret::expose ({why}), but no longer \
                 calls it — remove it, or the list stops meaning anything: {found:?}"
            );
        }
    }

    /// Deserialization is transparent — a JSON string becomes a `Secret` — so an
    /// inbound adapter never has to touch the raw value to build one.
    #[test]
    fn secret_deserializes_from_a_bare_string() {
        let s: Secret = serde_json::from_str("\"tok\"").unwrap();
        assert_eq!(s.expose(), "tok");
        assert!(Secret::new("").is_empty());
        assert!(!s.is_empty());
    }
}
