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

//! Pond resource tiers. A pond is created at a tier (default `medium`); the tier
//! maps to a `ResourceLimits` that the engine applies as caps on the pond's
//! DuckDB instance (`memory_limit` + `threads` — instance-global, one instance
//! per pond). These are caps, not reservations: a small pond's queries simply
//! can't exceed its budget; nothing is pre-allocated.
//!
//! NOTE (non-k8s only): in-process caps are the isolation model when one node
//! process hosts many ponds on a shared host. Under Kubernetes the boundary is
//! the pod's cgroup — there the tier should map to pod sizing (requests/limits)
//! and DuckDB should use the full pod, NOT be capped again in-process below the
//! cgroup (double-capping strands pod resources). Gate these `SET` caps to the
//! non-k8s path when the k8s slice lands.
use serde::{Deserialize, Serialize};

/// Engine-neutral resource caps for one pond's instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceLimits {
    /// DuckDB `memory_limit`, in bytes.
    pub memory_bytes: u64,
    /// CPU core budget for this pond — the number of cores its queries may use
    /// in parallel. Applied internally as DuckDB's `threads` setting (a cap, not
    /// a reservation).
    pub cores: u32,
}

const MB: u64 = 1024 * 1024;
const GB: u64 = 1024 * MB;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum PondTier {
    /// **No Latiq caps.** Nothing is applied to the pond's engine instance, so the
    /// engine's own defaults govern it (DuckDB: `threads` = every core,
    /// `memory_limit` ~80% of RAM). Explicitly named `none` rather than
    /// "unlimited" because Latiq isn't granting anything — it is declining to cap,
    /// and what that means is the engine's business.
    ///
    /// Operator-assignable only (`latiq pond set-tier … --tier none`): an uncapped
    /// pond can starve every other pond on its node, so it must not be something a
    /// workload assigns itself at allocate time.
    None,
    XSmall,
    Small,
    #[default]
    Medium,
    Large,
    XLarge,
}

/// Every tier name, canonical spelling, smallest to largest — `none` last
/// because it is not on the ladder. The ONE list every "expected …" message and
/// every schema `enum` is built from, so a tier that is added cannot be added to
/// the parser and forgotten in the error text (`set_pond_tier` once listed five
/// of the six it accepted).
pub const ALL: &[&str] = &["x-small", "small", "medium", "large", "x-large", "none"];

/// The tiers a caller may ask for at CREATE time. `none` is excluded because it
/// is an operator grant (see [`PondTier::None`]) — asking for it is refused with
/// its own message, which says how to obtain it.
pub const CREATABLE: &[&str] = &["x-small", "small", "medium", "large", "x-large"];

impl PondTier {
    pub fn as_str(&self) -> &'static str {
        match self {
            PondTier::None => "none",
            PondTier::XSmall => "x-small",
            PondTier::Small => "small",
            PondTier::Medium => "medium",
            PondTier::Large => "large",
            PondTier::XLarge => "x-large",
        }
    }

    /// Parse a tier name, case- and whitespace-insensitively. `None` for an
    /// unknown or empty name — the caller decides whether that is an error or a
    /// fall back to the default; this never silently picks a tier.
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_lowercase().as_str() {
            "none" | "uncapped" => Some(PondTier::None),
            "x-small" | "xsmall" => Some(PondTier::XSmall),
            "small" => Some(PondTier::Small),
            "medium" => Some(PondTier::Medium),
            "large" => Some(PondTier::Large),
            "x-large" | "xlarge" => Some(PondTier::XLarge),
            _ => None,
        }
    }

    /// Resolve the tier a CREATE path was asked for.
    ///
    /// Three outcomes, and the middle one is why this exists: an empty name is
    /// "the caller said nothing" (proto3 cannot express an absent string) and
    /// takes the default; a KNOWN name is honoured; an unknown name is an
    /// **error naming the whole set**, never a silent fall back to the default.
    /// It was the fall back that let `tier: "gigantic"` create a medium pond
    /// that then reported `"tier": "gigantic"` from `describe_pond` forever.
    ///
    /// `none` parses here and is deliberately NOT rejected here — it is refused
    /// one layer up with a message that says how an operator grants it, so
    /// "that tier is not yours" stays distinguishable from "no such tier".
    pub fn parse_for_create(s: &str) -> Result<Self, String> {
        if s.trim().is_empty() {
            return Ok(Self::default());
        }
        Self::parse(s).ok_or_else(|| {
            format!(
                "unknown tier '{}'. Allowed at creation: {}.",
                s.trim(),
                CREATABLE.join(", ")
            )
        })
    }

    /// Resource caps for this tier, or `None` for [`PondTier::None`] — which the
    /// engine reads as "apply nothing", leaving its own defaults in force.
    /// Tweak here to retune all ponds at a tier.
    pub fn limits(&self) -> Option<ResourceLimits> {
        match self {
            PondTier::None => None,
            PondTier::XSmall => Some(ResourceLimits {
                memory_bytes: 512 * MB,
                cores: 1,
            }),
            PondTier::Small => Some(ResourceLimits {
                memory_bytes: GB,
                cores: 2,
            }),
            PondTier::Medium => Some(ResourceLimits {
                memory_bytes: 4 * GB,
                cores: 4,
            }),
            PondTier::Large => Some(ResourceLimits {
                memory_bytes: 16 * GB,
                cores: 8,
            }),
            PondTier::XLarge => Some(ResourceLimits {
                memory_bytes: 32 * GB,
                cores: 16,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_medium() {
        assert_eq!(PondTier::default(), PondTier::Medium);
        assert_eq!(PondTier::default().as_str(), "medium");
    }

    #[test]
    fn parse_round_trips_and_tolerates_case() {
        for t in [
            PondTier::XSmall,
            PondTier::Small,
            PondTier::Medium,
            PondTier::Large,
            PondTier::XLarge,
        ] {
            assert_eq!(PondTier::parse(t.as_str()), Some(t));
        }
        assert_eq!(PondTier::parse(" X-LARGE "), Some(PondTier::XLarge));
        assert_eq!(PondTier::parse("xlarge"), Some(PondTier::XLarge));
        assert_eq!(PondTier::parse("x-small"), Some(PondTier::XSmall));
        assert_eq!(PondTier::parse("huge"), None);
    }

    /// The create path must never turn a typo into a pond: `larg` is not
    /// `large`, and the pond that used to result behaved as medium while
    /// `describe_pond` reported `larg` for the rest of its life.
    #[test]
    fn parse_for_create_rejects_an_unknown_tier_and_names_the_allowed_set() {
        for bad in ["gigantic", "larg", "smal", "unlimited"] {
            let err = PondTier::parse_for_create(bad)
                .expect_err("an unknown tier must not resolve to a default");
            assert!(err.contains(bad), "must name the offender, got: {err}");
            for t in CREATABLE {
                assert!(err.contains(t), "must name '{t}', got: {err}");
            }
        }
    }

    /// Empty is the proto3 "unset" only. Everything else is a real answer.
    #[test]
    fn parse_for_create_takes_the_default_only_for_an_empty_name() {
        assert_eq!(PondTier::parse_for_create(""), Ok(PondTier::Medium));
        assert_eq!(PondTier::parse_for_create("   "), Ok(PondTier::Medium));
        assert_eq!(PondTier::parse_for_create(" LARGE "), Ok(PondTier::Large));
        // `none` parses; refusing it is the caller's job, so the two failures
        // ("not yours" vs "no such tier") can carry different messages.
        assert_eq!(PondTier::parse_for_create("none"), Ok(PondTier::None));
    }

    /// The name lists exist so no message can drift out of sync with the parser.
    /// Anti-vacuity: the counts are pinned, so a tier added to the enum without
    /// being added here fails this test rather than silently going unlisted.
    #[test]
    fn the_name_lists_match_what_the_parser_accepts() {
        assert_eq!(ALL.len(), 6, "every tier must be listed");
        assert_eq!(CREATABLE.len(), 5, "every tier but `none` is creatable");
        for name in ALL {
            let t = PondTier::parse(name).unwrap_or_else(|| panic!("'{name}' must parse"));
            assert_eq!(t.as_str(), *name, "'{name}' must be the canonical spelling");
        }
        assert!(!CREATABLE.contains(&"none"), "`none` is an operator grant");
        for name in CREATABLE {
            assert!(ALL.contains(name), "'{name}' missing from ALL");
        }
    }

    /// Every rung must be strictly bigger than the one below it in **both**
    /// dimensions. x-large once had the same core count as large, so asking for
    /// the top tier bought memory but no extra compute — a ladder that is
    /// monotonic in one dimension only is a silent trap for whoever picks a tier.
    #[test]
    fn limits_increase_with_tier() {
        let ladder = [
            PondTier::XSmall,
            PondTier::Small,
            PondTier::Medium,
            PondTier::Large,
            PondTier::XLarge,
        ];
        for pair in ladder.windows(2) {
            let (lo, hi) = (pair[0].limits().unwrap(), pair[1].limits().unwrap());
            let (ln, hn) = (pair[0].as_str(), pair[1].as_str());
            assert!(
                hi.memory_bytes > lo.memory_bytes,
                "{hn} must have more memory than {ln}"
            );
            assert!(hi.cores > lo.cores, "{hn} must have more cores than {ln}");
        }
    }

    #[test]
    fn none_tier_applies_no_caps() {
        // `none` is the ONLY tier without limits: the engine then applies nothing
        // and its own defaults govern the pond. Every other tier must cap.
        assert_eq!(PondTier::None.limits(), None);
        for t in [
            PondTier::XSmall,
            PondTier::Small,
            PondTier::Medium,
            PondTier::Large,
            PondTier::XLarge,
        ] {
            assert!(t.limits().is_some(), "{} must cap", t.as_str());
        }
        // Round-trips by its explicit name (not "unlimited" — Latiq grants
        // nothing, it declines to cap).
        assert_eq!(PondTier::parse("none"), Some(PondTier::None));
        assert_eq!(PondTier::parse(" NONE "), Some(PondTier::None));
        assert_eq!(PondTier::None.as_str(), "none");
        assert_eq!(PondTier::parse("unlimited"), None);
        // ...and is never the default: a pond you don't think about stays capped.
        assert_ne!(PondTier::default(), PondTier::None);
    }
}
