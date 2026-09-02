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

//! The node's query-timeout policy: a default for callers that ask for nothing,
//! and a hard maximum that is the OPERATOR's protection.
//!
//! The maximum exists because there is one DuckDB instance per pond (root
//! `CLAUDE.md` invariant 7) — without a ceiling one agent can pin a pond's
//! instance indefinitely, which is precisely the isolation that invariant buys.
//! A request above the maximum is **clamped, not refused**: the query still
//! runs, at the ceiling, and `QueryMeta::timeout_ms` reports what was applied so
//! the clamp is never silent.

/// Node default when a caller asks for no `timeout_ms` (30s). The same number
/// the control-plane registry has always seeded `query_timeout_seconds` with,
/// so the two planes do not disagree about what "normal" means.
pub const DEFAULT_QUERY_TIMEOUT_MS: u64 = 30_000;

/// Node ceiling (5 minutes). Long enough for a genuine analytical scan on a
/// large tier, short enough that one abandoned query cannot hold a pond's only
/// DuckDB instance for an operator's whole shift.
pub const MAX_QUERY_TIMEOUT_MS: u64 = 300_000;

/// What this node will allow a single statement to run for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryTimeouts {
    /// Applied when the caller names no `timeout_ms`.
    pub default_ms: u64,
    /// The ceiling every request is clamped to.
    pub max_ms: u64,
}

impl Default for QueryTimeouts {
    fn default() -> Self {
        Self {
            default_ms: DEFAULT_QUERY_TIMEOUT_MS,
            max_ms: MAX_QUERY_TIMEOUT_MS,
        }
    }
}

impl QueryTimeouts {
    /// Validate an operator's pair ONCE, at startup, before anything binds —
    /// the same discipline as the verifier and `--lineage-backend-url`. A node
    /// that would apply a nonsensical timeout must fail to start, not discover
    /// it on some agent's first query.
    pub fn new(default_ms: u64, max_ms: u64) -> Result<Self, String> {
        if default_ms == 0 {
            return Err(
                "--query-timeout-ms must be greater than 0 (a zero timeout would cancel every \
                 query immediately)"
                    .into(),
            );
        }
        if max_ms == 0 {
            return Err(
                "--query-timeout-max-ms must be greater than 0 (a zero maximum would cancel every \
                 query immediately)"
                    .into(),
            );
        }
        if default_ms > max_ms {
            return Err(format!(
                "--query-timeout-ms ({default_ms}) must not exceed --query-timeout-max-ms \
                 ({max_ms}): the default is clamped to the maximum, so this node would silently \
                 apply {max_ms} ms to every query"
            ));
        }
        Ok(Self { default_ms, max_ms })
    }

    /// The timeout actually applied to one request: the node default when the
    /// caller asked for none, otherwise the caller's value clamped to `max_ms`.
    ///
    /// A caller-supplied `0` is treated as "not specified" rather than as "no
    /// timeout": proto3 cannot tell an unset `uint64` from a zero one, and the
    /// alternative reading — an unbounded query — is the one thing the ceiling
    /// exists to prevent.
    pub fn effective(&self, requested_ms: Option<u64>) -> u64 {
        match requested_ms {
            None | Some(0) => self.default_ms,
            Some(ms) => ms.min(self.max_ms),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn effective_honours_a_request_within_the_maximum() {
        let t = QueryTimeouts::new(30_000, 300_000).unwrap();
        assert_eq!(t.effective(Some(1_234)), 1_234, "honoured exactly");
        assert_eq!(t.effective(Some(300_000)), 300_000, "the maximum itself");
    }

    #[test]
    fn effective_clamps_a_request_above_the_maximum_rather_than_refusing_it() {
        let t = QueryTimeouts::new(30_000, 300_000).unwrap();
        assert_eq!(
            t.effective(Some(1_800_000)),
            300_000,
            "a 30-minute ask runs at the ceiling; it is never rejected"
        );
    }

    #[test]
    fn effective_falls_back_to_the_default_for_an_absent_or_zero_request() {
        let t = QueryTimeouts::new(30_000, 300_000).unwrap();
        assert_eq!(t.effective(None), 30_000);
        // proto3 cannot distinguish unset from 0, and reading 0 as "unbounded"
        // would hand any caller the one thing the ceiling exists to prevent.
        assert_eq!(t.effective(Some(0)), 30_000);
    }

    #[test]
    fn rejects_a_default_above_the_maximum() {
        let e = QueryTimeouts::new(300_001, 300_000).expect_err("default > max is not startable");
        assert!(
            e.contains("--query-timeout-ms") && e.contains("--query-timeout-max-ms"),
            "the error must name BOTH flags so an operator knows which to change: {e}"
        );
    }

    #[test]
    fn rejects_zero_on_either_flag() {
        let e = QueryTimeouts::new(0, 300_000).expect_err("a zero default is not startable");
        assert!(e.contains("--query-timeout-ms"), "{e}");
        let e = QueryTimeouts::new(30_000, 0).expect_err("a zero maximum is not startable");
        assert!(e.contains("--query-timeout-max-ms"), "{e}");
    }

    #[test]
    fn the_defaults_are_a_valid_pair() {
        // The shipped constants must survive their own validation, or the node
        // cannot start without flags.
        let d = QueryTimeouts::default();
        assert_eq!(
            QueryTimeouts::new(DEFAULT_QUERY_TIMEOUT_MS, MAX_QUERY_TIMEOUT_MS).unwrap(),
            d
        );
        assert!(d.default_ms < d.max_ms);
    }
}
