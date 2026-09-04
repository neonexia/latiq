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

//! W3C Trace Context (`traceparent`): the value type every surface parses,
//! formats and propagates.
//!
//! It lives in the kernel rather than beside the task-local scope in
//! `latiq-agent-core` because BOTH sides of the system need it and only one of
//! them can depend on the other: the pond node scopes a trace per request, and
//! the control plane reads a `traceparent` off its Control/Admin requests and
//! stamps one onto the node hop it makes. A second copy of this parser in the
//! control plane is exactly the drift the repo keeps paying for elsewhere —
//! and this one decides what we are willing to believe from a caller.
//!
//! **One spelling, everywhere.** This replaced a custom `latiq-trace-id` header
//! that existed on the gRPC surfaces and nowhere else — MCP, Admin and the
//! control-plane -> node hop were untraced, which is backwards for a product
//! whose primary consumer is an agent. Two names for one concept is a bug in
//! waiting, and an external collector or simulator already speaks the standard
//! one.
//!
//! **An inbound `traceparent` is attribution-grade, not authority-grade** — the
//! same standing as `latiq-agent-id` (root invariant 9). It is recorded and
//! propagated; it is never consulted for an access decision, and a malformed
//! one is replaced rather than rejected (a trace id is not worth failing a
//! query over). What we never do is *trust* it: the span id is always ours, so
//! a caller cannot make our spans claim to be someone else's.
use crate::PondId;

/// The W3C `traceparent` version this code emits. `00` is the only version
/// defined; a higher one is parsed leniently (see [`TraceContext::parse`]).
const VERSION: &str = "00";

/// One request's place in a trace: the id shared by everything the request
/// touches, plus **our own** span within it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceContext {
    /// 32 lowercase hex digits. The join key for logs, lineage and errors.
    trace_id: String,
    /// 16 lowercase hex digits — the span WE are, minted here on every inbound
    /// request. Never the caller's: an outbound `traceparent` names this as the
    /// parent of the next hop, which is what makes the hop a child of our work
    /// rather than a sibling of the caller's.
    span_id: String,
    /// The caller's sampling decision, propagated unchanged. We make no
    /// sampling decision of our own (nothing here samples), so passing it
    /// through is the only honest thing to do with it.
    sampled: bool,
}

/// 32 lowercase hex digits from a v4 UUID — no extra dependency, and the same
/// entropy source pond ids already use.
fn hex32() -> String {
    PondId::new().to_string().replace('-', "")
}

impl TraceContext {
    /// A brand-new trace: this request is the root.
    pub fn new() -> Self {
        Self {
            trace_id: hex32(),
            span_id: hex32()[..16].to_string(),
            sampled: true,
        }
    }

    /// Parse an inbound `traceparent`, keeping the caller's trace id and minting
    /// our own span id under it. `None` when the header is not a `traceparent`
    /// we can honour — callers mint a fresh trace instead of failing, because a
    /// broken trace header is not a reason to refuse a query.
    ///
    /// Rejects, per the W3C spec: a wrong field count, a non-hex or wrong-length
    /// id, an ALL-ZERO trace or parent id (the spec's explicit "invalid"
    /// sentinel), and version `ff`. A version above `00` is accepted and its
    /// first three fields read — that is what the spec's forward-compatibility
    /// rule requires, and refusing it would break us against a future caller.
    pub fn parse(header: &str) -> Option<Self> {
        let mut parts = header.trim().split('-');
        let version = parts.next()?;
        let trace_id = parts.next()?;
        let parent_id = parts.next()?;
        let flags = parts.next()?;
        // A `00` traceparent has exactly four fields; a future version may append
        // more, which we ignore rather than reject.
        if version == "00" && parts.next().is_some() {
            return None;
        }
        if !is_lower_hex(version, 2) || version == "ff" {
            return None;
        }
        if !is_lower_hex(trace_id, 32) || trace_id.bytes().all(|b| b == b'0') {
            return None;
        }
        if !is_lower_hex(parent_id, 16) || parent_id.bytes().all(|b| b == b'0') {
            return None;
        }
        if !is_lower_hex(flags, 2) {
            return None;
        }
        let sampled = u8::from_str_radix(flags, 16).ok()? & 0x01 == 0x01;
        Some(Self {
            trace_id: trace_id.to_string(),
            // OURS. See the field's doc: the caller's parent id is what we are a
            // child of, not something we re-present as our own.
            span_id: hex32()[..16].to_string(),
            sampled,
        })
    }

    /// The trace id an inbound header carried, or a fresh trace when it carried
    /// none we could honour. The one entry point every adapter uses, so
    /// "malformed means fresh" is decided once.
    pub fn inbound(header: Option<&str>) -> Self {
        header.and_then(Self::parse).unwrap_or_default()
    }

    /// The join key: 32 lowercase hex digits.
    pub fn trace_id(&self) -> &str {
        &self.trace_id
    }

    /// The `traceparent` header value to put on an outbound hop.
    pub fn traceparent(&self) -> String {
        let flags = if self.sampled { "01" } else { "00" };
        format!("{VERSION}-{}-{}-{flags}", self.trace_id, self.span_id)
    }
}

impl Default for TraceContext {
    fn default() -> Self {
        Self::new()
    }
}

fn is_lower_hex(s: &str, len: usize) -> bool {
    s.len() == len
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trace_a_fresh_context_is_a_valid_traceparent_we_can_reparse() {
        let ctx = TraceContext::new();
        assert_eq!(ctx.trace_id().len(), 32);
        let header = ctx.traceparent();
        // Round-tripping keeps the TRACE id (that is the join key) and mints a
        // new span id (we are a new span). Both halves matter.
        let child = TraceContext::parse(&header).expect("our own header must parse");
        assert_eq!(child.trace_id(), ctx.trace_id());
        assert_ne!(
            child.span_id, ctx.span_id,
            "a hop is a new span, not a copy"
        );
        assert!(header.starts_with("00-"));
    }

    #[test]
    fn trace_an_inbound_id_is_kept_and_the_span_is_ours() {
        // The known-good example from the W3C spec.
        let ctx = TraceContext::parse("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
            .expect("the spec's own example must parse");
        assert_eq!(ctx.trace_id(), "4bf92f3577b34da6a3ce929d0e0e4736");
        assert!(ctx.sampled);
        assert_ne!(
            ctx.span_id, "00f067aa0ba902b7",
            "the caller's span is what we are a CHILD of; re-presenting it as \
             ours would let a caller forge our spans"
        );
        assert_eq!(
            ctx.traceparent(),
            format!("00-4bf92f3577b34da6a3ce929d0e0e4736-{}-01", ctx.span_id)
        );
    }

    #[test]
    fn trace_the_sampling_flag_is_propagated_not_invented() {
        // Nothing here samples, so the caller's decision is passed through
        // unchanged in both directions rather than being overwritten with ours.
        let unsampled =
            TraceContext::parse("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00").unwrap();
        assert!(!unsampled.sampled);
        assert!(unsampled.traceparent().ends_with("-00"));
    }

    /// Each header below is wrong in exactly ONE way, so a rejection can only be
    /// attributed to the check under test.
    #[test]
    fn trace_a_malformed_traceparent_is_refused_rather_than_half_trusted() {
        let good = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        assert!(TraceContext::parse(good).is_some(), "the baseline is valid");
        let cases = [
            ("too few fields", "00-4bf92f3577b34da6a3ce929d0e0e4736-01"),
            (
                "a trailing field on a version-00 header",
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra",
            ),
            (
                "version ff, which the spec forbids",
                "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            ),
            (
                "a short trace id",
                "00-4bf92f3577b34da6a3ce929d0e0e473-00f067aa0ba902b7-01",
            ),
            (
                "an UPPERCASE trace id (the spec is lowercase-only)",
                "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",
            ),
            (
                "the all-zero trace id, the spec's invalid sentinel",
                "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
            ),
            (
                "the all-zero parent id",
                "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
            ),
            (
                "a non-hex flag",
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-0z",
            ),
            ("not a traceparent at all", "hello"),
            ("empty", ""),
        ];
        for (why, header) in cases {
            assert!(TraceContext::parse(header).is_none(), "{why}: {header}");
            // And the adapter path never fails: it mints a fresh trace instead,
            // because a broken trace header must not cost a caller its query.
            let fresh = TraceContext::inbound(Some(header));
            assert_eq!(fresh.trace_id().len(), 32, "{why}");
            assert_ne!(
                fresh.trace_id(),
                "4bf92f3577b34da6a3ce929d0e0e4736",
                "{why}"
            );
        }
    }

    /// A version above `00` is honoured, not refused — the spec's
    /// forward-compatibility rule. Refusing it would silently start a NEW trace
    /// for every request from a future-version caller, which looks like working
    /// tracing and joins nothing.
    #[test]
    fn trace_a_future_version_is_honoured_and_answered_in_version_00() {
        let ctx = TraceContext::parse(
            "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-somethingnew",
        )
        .expect("a higher version's first three fields are readable");
        assert_eq!(ctx.trace_id(), "4bf92f3577b34da6a3ce929d0e0e4736");
        assert!(
            ctx.traceparent().starts_with("00-"),
            "we emit the version we implement"
        );
    }
}
