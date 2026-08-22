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

    /// Parse a tier name; unknown/empty falls back to the default (`medium`).
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
