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

//! latiq-lineage — OpenLineage events and the file writer that persists them.
//!
//! PROTOCOL-NEUTRAL, like `latiq-agent-core` which depends on it: no MCP, gRPC
//! or HTTP types appear here. Events are values; the writer's default sink is
//! the pond's own `lineage` directory.
//!
//! The **one exception** is [`sink::HttpSink`], which is HTTP by definition. It
//! is behind the `http-sink` Cargo feature that only `latiq-pond-node` enables,
//! so with the feature off this crate does not even depend on `reqwest` — the
//! neutrality of everything else is enforced by Cargo, not by convention. What
//! the writer and `latiq-agent-core` see is [`sink::EventSink`], a trait over
//! `&str` with no transport in it.
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
pub mod reader;
pub mod sink;
pub mod writer;

pub use event::{Dataset, EventType, Job, ParentClaim, Run, RunEvent};
pub use reader::{read_newest, EventPage, PageRequest, ReadError, MAX_LIMIT};
pub use sink::EventSink;
#[cfg(feature = "http-sink")]
pub use sink::HttpSink;
pub use writer::LineageWriter;
