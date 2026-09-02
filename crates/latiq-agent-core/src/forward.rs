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

//! Node-to-node forwarding. When a node receives a pond request it doesn't own,
//! it delegates to the owning node through a `Forwarder`. This trait is
//! **protocol-neutral** (invariant 5): the core only knows the owner's endpoint
//! string and the neutral result types — the actual transport (the Data gRPC
//! client) lives in the pond-node adapter that implements this. It mirrors the
//! `ControlPlane` trait: the core abstracts "a remote node" the same way it
//! already abstracts "the registry".
use crate::arrow::ArrowReadStream;
use crate::error::AgentError;
use crate::types::{DescribeResult, LineagePage, PullResult};
use latiq_common::Identity;
use latiq_engine::{ExplainResult, QueryResult};
use std::collections::BTreeMap;

/// The node an op is being delegated to: who it is, and where to dial it.
///
/// Both halves travel together because a failure to reach the owner has to be
/// reportable, and neither half alone reports it. The endpoint says what was
/// dialled; the **node id** is what an operator needs to act on it (`latiq node
/// list`), and it is the field the registry decides ownership by. Copy, so
/// passing it costs the same as the `&str` it replaced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Peer<'a> {
    /// The owning node's registered id — an identity, never an address.
    pub node_id: &'a str,
    /// The address to dial it at (`node_endpoint` in the registry).
    pub endpoint: &'a str,
}

/// Runs one op on the node that owns the pond. Every method takes the owning
/// [`Peer`], so the core never holds a connection — the implementation does.
#[async_trait::async_trait]
pub trait Forwarder: Send + Sync {
    /// `timeout_ms` is what the CALLER asked for, relayed unresolved: the owner
    /// runs the statement, so the owner's default and the owner's ceiling are
    /// the ones that apply, and the owner reports what it actually applied in
    /// the meta it sends back. A greeter node that resolved it here would
    /// impose its own policy on the node that carries the risk.
    async fn read(
        &self,
        peer: Peer<'_>,
        identity: &Identity,
        pond: &str,
        sql: &str,
        timeout_ms: Option<u64>,
    ) -> Result<QueryResult, AgentError>;

    /// Stream a read from the owning node as Arrow batches (the Arrow internal
    /// hop). Used by `read_arrow` when the pond is remote.
    async fn read_arrow(
        &self,
        peer: Peer<'_>,
        identity: &Identity,
        pond: &str,
        sql: &str,
        timeout_ms: Option<u64>,
    ) -> Result<ArrowReadStream, AgentError>;

    async fn write(
        &self,
        peer: Peer<'_>,
        identity: &Identity,
        pond: &str,
        sql: &str,
        timeout_ms: Option<u64>,
    ) -> Result<QueryResult, AgentError>;

    async fn explain(
        &self,
        peer: Peer<'_>,
        identity: &Identity,
        pond: &str,
        sql: &str,
    ) -> Result<ExplainResult, AgentError>;

    async fn describe(
        &self,
        peer: Peer<'_>,
        identity: &Identity,
        pond: &str,
    ) -> Result<DescribeResult, AgentError>;

    /// Read a page of the pond's lineage from the owning node. Forwarded like
    /// every other pond-scoped op, and for a sharper reason than most: the
    /// events are FILES on the node that ran the queries, so a peer has nothing
    /// of its own to answer with. `since` is inclusive, `before` exclusive.
    async fn get_lineage(
        &self,
        peer: Peer<'_>,
        identity: &Identity,
        pond: &str,
        limit: usize,
        since: Option<&str>,
        before: Option<&str>,
    ) -> Result<LineagePage, AgentError>;

    async fn drop_pond(
        &self,
        peer: Peer<'_>,
        identity: &Identity,
        pond: &str,
        confirm: bool,
    ) -> Result<(), AgentError>;

    /// Make the pond's storage exist on the owning node, and its engine open it.
    ///
    /// The node-to-node half of **eager allocation**: the control plane picks
    /// the owner and writes the registry row, then the node that took the
    /// allocate call reaches through here so the owner materialises the pond
    /// before the caller is told it has one. An allocation that returns success
    /// therefore means a pond that can accept data — and one that cannot be
    /// materialised fails now, with the registry row given back, instead of
    /// deferring the failure to the agent's first INSERT.
    ///
    /// **Idempotent by contract.** The op it drives is "ensure", not "create":
    /// a pond that already exists is success, never a conflict. That is what
    /// makes it safe to retry, and what lets the lazy `ensure_pond` on the query
    /// paths stay as a fallback without the two fighting.
    async fn materialize_pond(
        &self,
        peer: Peer<'_>,
        identity: &Identity,
        pond: &str,
    ) -> Result<(), AgentError>;

    /// Transient pull from an external catalog on the owning node. The runtime
    /// `params` (incl. any credentials) ride the gRPC hop and are dropped after
    /// the attach/detach on the owner — nothing about the catalog persists.
    async fn catalog_pull(
        &self,
        peer: Peer<'_>,
        identity: &Identity,
        pond: &str,
        catalog: &str,
        query: &str,
        params: BTreeMap<String, String>,
    ) -> Result<PullResult, AgentError>;

    /// List an external catalog's tables on the owning node (transient attach).
    async fn catalog_describe(
        &self,
        peer: Peer<'_>,
        identity: &Identity,
        pond: &str,
        catalog: &str,
        params: BTreeMap<String, String>,
    ) -> Result<Vec<(String, String)>, AgentError>;
}
