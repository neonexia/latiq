//! Node-to-node forwarding. When a node receives a pond request it doesn't own,
//! it delegates to the owning node through a `Forwarder`. This trait is
//! **protocol-neutral** (invariant 5): the core only knows the owner's endpoint
//! string and the neutral result types — the actual transport (the Data gRPC
//! client) lives in the pond-node adapter that implements this. It mirrors the
//! `ControlPlane` trait: the core abstracts "a remote node" the same way it
//! already abstracts "the registry".
use crate::error::AgentError;
use crate::types::DescribeResult;
use latiq_common::Identity;
use latiq_engine::{ExplainResult, QueryResult};

#[async_trait::async_trait]
pub trait Forwarder: Send + Sync {
    async fn read(
        &self,
        endpoint: &str,
        identity: &Identity,
        pond: &str,
        sql: &str,
    ) -> Result<QueryResult, AgentError>;

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

    async fn drop_pond(
        &self,
        endpoint: &str,
        identity: &Identity,
        pond: &str,
        confirm: bool,
    ) -> Result<(), AgentError>;
}
