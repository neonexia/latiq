//! Pond resource tiers. A pond is created at a tier (default `medium`); the tier
//! maps to a `ResourceLimits` that the engine applies as caps on the pond's
//! DuckDB instance (`memory_limit` + `threads` — instance-global, one instance
//! per pond). These are caps, not reservations: a small pond's queries simply
//! can't exceed its budget; nothing is pre-allocated.
use serde::{Deserialize, Serialize};

/// Engine-neutral resource caps for one pond's instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceLimits {
    /// DuckDB `memory_limit`, in bytes.
    pub memory_bytes: u64,
    /// DuckDB `threads` (intra-query parallelism for this pond).
    pub threads: u32,
}

const MB: u64 = 1024 * 1024;
const GB: u64 = 1024 * MB;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum PondTier {
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
            "x-small" | "xsmall" => Some(PondTier::XSmall),
            "small" => Some(PondTier::Small),
            "medium" => Some(PondTier::Medium),
            "large" => Some(PondTier::Large),
            "x-large" | "xlarge" => Some(PondTier::XLarge),
            _ => None,
        }
    }

    /// Resource caps for this tier. Tweak here to retune all ponds at a tier.
    pub fn limits(&self) -> ResourceLimits {
        match self {
            PondTier::XSmall => ResourceLimits {
                memory_bytes: 512 * MB,
                threads: 1,
            },
            PondTier::Small => ResourceLimits {
                memory_bytes: GB,
                threads: 2,
            },
            PondTier::Medium => ResourceLimits {
                memory_bytes: 4 * GB,
                threads: 4,
            },
            PondTier::Large => ResourceLimits {
                memory_bytes: 16 * GB,
                threads: 8,
            },
            PondTier::XLarge => ResourceLimits {
                memory_bytes: 32 * GB,
                threads: 8,
            },
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

    #[test]
    fn limits_increase_with_tier() {
        assert!(PondTier::XSmall.limits().memory_bytes < PondTier::Small.limits().memory_bytes);
        assert!(PondTier::Small.limits().memory_bytes < PondTier::Medium.limits().memory_bytes);
        assert!(PondTier::Large.limits().threads > PondTier::Medium.limits().threads);
    }
}
