//! latiq-lineage — OpenLineage events and the file writer that persists them.
//!
//! PROTOCOL-NEUTRAL, like `latiq-agent-core` which depends on it: no MCP, gRPC
//! or HTTP types appear here. Events are values; the writer's only sink is the
//! pond's own `lineage` directory.
//!
//! Two invariants this crate exists to hold:
//!
//! 1. **The events are real OpenLineage** (core spec `2-0-2`), not a
//!    Latiq-shaped struct with OpenLineage words in it. Facets are the standard
//!    ones wherever a standard one exists; ours are prefixed `latiq` and carry
//!    a `_schemaURL` naming their schema in `spec/`. That URL is a stable
//!    **identifier** for the facet's shape, not a live document — consumers do
//!    not dereference it (confirmed against Marquez and DataHub), and the repo
//!    it names is private. The whole shape is pinned by `tests/lineage.rs`
//!    against the schemas in `spec/`.
//! 2. **Emission can never hurt a query.** `record()` is called from the query
//!    hot path: it serializes and buffers, returns `()`, and cannot fail or
//!    panic. Every failure below it is a `warn!` and a dropped event.
pub mod event;
pub mod writer;

pub use event::{Dataset, EventType, Job, ParentClaim, Run, RunEvent};
pub use writer::LineageWriter;
