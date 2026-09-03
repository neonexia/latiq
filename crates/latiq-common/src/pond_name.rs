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

//! What a pond may be called.
//!
//! A pond's name is not a label: it is ATTACHed as the DuckLake catalog
//! identifier for the pond (`PondLocation::catalog_name`), and it is what an
//! agent types when it qualifies a table. It is quoted at the ATTACH, so a
//! hostile name cannot inject SQL — but `a b/c` still became a real pond whose
//! name no agent could write in a query without knowing to quote it, and whose
//! `/` reads as a path separator to every human who sees it.
//!
//! So the legal set is deliberately narrow and boring: **ASCII letters, digits,
//! `_` and `-`, 1–64 characters**. It admits the generated default (a UUID) and
//! every name in the suite, and it is the same set an identifier can take
//! unquoted in most dialects except for the leading digit and `-`, which the
//! UUID default forces.
//!
//! Latiq does NOT trim, lowercase, or otherwise repair a name: the pond you
//! asked for is the pond you get, or you get told why not.

/// Longest pond name. Not a storage limit (the directory is the pond's id) —
/// it is a limit on what an agent has to carry in its context to name a table.
pub const MAX_LEN: usize = 64;

/// Human description of the legal set, for error messages and tool schemas.
/// One string so a schema and an error can never disagree about the rule.
pub const RULE: &str =
    "1-64 characters, letters, digits, `_` or `-` only (it is used as the pond's SQL catalog name)";

/// Check a caller-supplied pond name. `Ok(())` or the reason it is refused.
///
/// The empty string is a refusal, not a "generate one for me": omitting the
/// name is how you ask for a generated one, and a surface that can tell the two
/// apart must not collapse them (proto3 cannot, and normalises `""` to "absent"
/// at that boundary only).
pub fn validate(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err(format!(
            "pond name must not be empty — omit `name` entirely to have Latiq generate one. \
             A name is {RULE}."
        ));
    }
    if name.len() > MAX_LEN {
        return Err(format!(
            "pond name is {} characters, over the {MAX_LEN}-character maximum. A name is {RULE}.",
            name.len()
        ));
    }
    if let Some(bad) = name
        .chars()
        .find(|c| !(c.is_ascii_alphanumeric() || *c == '_' || *c == '-'))
    {
        return Err(format!(
            "pond name '{name}' contains '{bad}', which is not allowed. A name is {RULE}."
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_the_names_ponds_actually_get() {
        // The generated default is a UUID: digits lead and `-` separates, so
        // both must be legal or Latiq could not name its own ponds.
        for ok in [
            "3f2a9c1e-0000-4a00-8000-abcdefabcdef",
            "sales",
            "incident-9",
            "load_2026",
            "A",
            &"x".repeat(MAX_LEN),
        ] {
            assert_eq!(validate(ok), Ok(()), "'{ok}' must be a legal pond name");
        }
    }

    /// The observed failures: an empty name silently became the pond's UUID, and
    /// `a b/c` became a real pond carrying a path separator into a catalog
    /// identifier.
    #[test]
    fn refuses_empty_and_says_how_to_get_a_generated_name() {
        let err = validate("").unwrap_err();
        assert!(
            err.contains("omit `name`"),
            "an empty name must point at the way to ask for a generated one: {err}"
        );
    }

    #[test]
    fn refuses_an_illegal_character_and_names_it() {
        for (name, bad) in [("a b/c", ' '), ("sales/2026", '/'), ("t\"x", '"')] {
            let err = validate(name).unwrap_err();
            assert!(
                err.contains(&format!("'{bad}'")),
                "must name the offending character '{bad}', got: {err}"
            );
            assert!(err.contains(RULE), "must state the rule, got: {err}");
        }
    }

    #[test]
    fn refuses_an_over_long_name_and_gives_both_numbers() {
        let err = validate(&"x".repeat(MAX_LEN + 1)).unwrap_err();
        assert!(err.contains(&(MAX_LEN + 1).to_string()), "{err}");
        assert!(err.contains(&MAX_LEN.to_string()), "{err}");
    }
}
