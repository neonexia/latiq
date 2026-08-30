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

#[async_trait::async_trait]
pub trait Forwarder: Send + Sync {
    async fn read(
        &self,
        endpoint: &str,
        identity: &Identity,
        pond: &str,
        sql: &str,
    ) -> Result<QueryResult, AgentError>;

    /// Stream a read from the owning node as Arrow batches (the Arrow internal
    /// hop). Used by `read_arrow` when the pond is remote.
    async fn read_arrow(
        &self,
        endpoint: &str,
        identity: &Identity,
        pond: &str,
        sql: &str,
    ) -> Result<ArrowReadStream, AgentError>;

    async fn write(
        &self,
        endpoint: &str,
        identity: &Identity,
        pond: &str,
        sql: &str,
    ) -> Result<QueryResult, AgentError>;

    async fn explain(
        &self,
        endpoint: &str,
        identity: &Identity,
        pond: &str,
        sql: &str,
    ) -> Result<ExplainResult, AgentError>;

    async fn describe(
        &self,
        endpoint: &str,
        identity: &Identity,
        pond: &str,
    ) -> Result<DescribeResult, AgentError>;

    /// Read a page of the pond's lineage from the owning node. Forwarded like
    /// every other pond-scoped op, and for a sharper reason than most: the
    /// events are FILES on the node that ran the queries, so a peer has nothing
    /// of its own to answer with. `since` is inclusive, `before` exclusive.
    async fn get_lineage(
        &self,
        endpoint: &str,
        identity: &Identity,
        pond: &str,
        limit: usize,
        since: Option<&str>,
        before: Option<&str>,
    ) -> Result<LineagePage, AgentError>;

    async fn drop_pond(
        &self,
        endpoint: &str,
        identity: &Identity,
        pond: &str,
        confirm: bool,
    ) -> Result<(), AgentError>;

    /// Transient pull from an external catalog on the owning node. The runtime
    /// `params` (incl. any credentials) ride the gRPC hop and are dropped after
    /// the attach/detach on the owner — nothing about the catalog persists.
    async fn catalog_pull(
        &self,
        endpoint: &str,
        identity: &Identity,
        pond: &str,
        catalog: &str,
        query: &str,
        params: BTreeMap<String, String>,
    ) -> Result<PullResult, AgentError>;

    /// List an external catalog's tables on the owning node (transient attach).
    async fn catalog_describe(
        &self,
        endpoint: &str,
        identity: &Identity,
        pond: &str,
        catalog: &str,
        params: BTreeMap<String, String>,
    ) -> Result<Vec<(String, String)>, AgentError>;
}
