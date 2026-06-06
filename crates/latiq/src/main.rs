//! latiq — single binary. Server roles (control-plane, pond-node), the operator
//! admin CLI (Admin gRPC), and the agent client CLI (MCP client).
use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use latiq_client::{CallOutcome, LatiqClient};
use latiq_control_plane::{serve_admin, serve_control, Registry};
use latiq_pond_node::{run_pond_node, PondNodeConfig};
use latiq_proto::v1::admin_client::AdminClient;
use latiq_proto::v1::*;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "latiq", version, about = "Agent-native data pond")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the control-plane server (Control + Admin gRPC over a DuckDB registry).
    ControlPlane(ControlPlaneArgs),
    /// Run a pond-node server (MCP surface + control-plane client).
    PondNode(PondNodeArgs),
    /// Operator: node administration (Admin gRPC).
    #[command(subcommand)]
    Node(NodeCmd),
    /// Operator: policy administration (Admin gRPC).
    #[command(subcommand)]
    Policy(PolicyCmd),
    /// Operator: audit access (Admin gRPC).
    #[command(subcommand)]
    Audit(AuditCmd),
    /// Agent client: pond lifecycle (MCP).
    #[command(subcommand)]
    Pond(PondCmd),
    /// Agent client: run a read-only SQL query (MCP).
    Query(QueryArgs),
    /// Agent client: run a write/DDL SQL statement (MCP).
    Write(QueryArgs),
    /// Agent client: plan a query without running it (MCP).
    Explain(QueryArgs),
}

#[derive(Args)]
struct ControlPlaneArgs {
    #[arg(long, default_value = "127.0.0.1:9090")]
    control_addr: String,
    #[arg(long, default_value = "127.0.0.1:9091")]
    admin_addr: String,
    /// Registry DuckDB file (in-memory if omitted).
    #[arg(long)]
    db: Option<PathBuf>,
}

#[derive(Args)]
struct PondNodeArgs {
    #[arg(long, default_value = "node-1")]
    node_id: String,
    #[arg(long, default_value = "127.0.0.1:8080")]
    mcp_addr: String,
    #[arg(long, default_value = "http://127.0.0.1:9090")]
    control: String,
    #[arg(long, default_value = "./latiq-data")]
    data_dir: PathBuf,
}

#[derive(Subcommand)]
enum NodeCmd {
    List,
    Describe { node_id: String },
}

#[derive(Subcommand)]
enum PolicyCmd {
    Show,
    Set { key: String, value: String },
}

#[derive(Subcommand)]
enum AuditCmd {
    Tail {
        #[arg(long, default_value_t = 20)]
        limit: u32,
    },
    Search {
        identity: String,
    },
}

#[derive(Subcommand)]
enum PondCmd {
    Create {
        #[arg(long)]
        name: Option<String>,
    },
    List,
    Describe {
        pond: String,
    },
    Drop {
        pond: String,
    },
}

#[derive(Args)]
struct QueryArgs {
    /// Pond id or name.
    #[arg(long)]
    pond: String,
    /// The SQL statement.
    sql: String,
    /// MCP endpoint.
    #[arg(long, default_value = "http://127.0.0.1:8080/mcp")]
    endpoint: String,
    /// Claimed agent identity.
    #[arg(long)]
    agent_id: Option<String>,
}

const ADMIN_ENDPOINT: &str = "http://127.0.0.1:9091";
const MCP_ENDPOINT: &str = "http://127.0.0.1:8080/mcp";

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::ControlPlane(a) => run_control_plane(a).await,
        Command::PondNode(a) => run_pond(a).await,
        Command::Node(cmd) => run_node_admin(cmd).await,
        Command::Policy(cmd) => run_policy_admin(cmd).await,
        Command::Audit(cmd) => run_audit_admin(cmd).await,
        Command::Pond(cmd) => run_pond_client(cmd).await,
        Command::Query(a) => run_query(a, "read").await,
        Command::Write(a) => run_query(a, "write").await,
        Command::Explain(a) => run_query(a, "explain").await,
    }
}

async fn run_control_plane(a: ControlPlaneArgs) -> Result<()> {
    let registry = Registry::open(a.db.as_deref())?;
    let c_addr = a.control_addr.parse()?;
    let admin_addr = a.admin_addr.parse()?;
    println!(
        "control-plane: Control gRPC on {}, Admin gRPC on {} (db: {})",
        a.control_addr,
        a.admin_addr,
        a.db.as_deref()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "in-memory".into())
    );
    let r2 = registry.clone();
    tokio::try_join!(serve_control(c_addr, registry), serve_admin(admin_addr, r2))
        .map_err(|e| anyhow::anyhow!("server error: {e}"))?;
    Ok(())
}

async fn run_pond(a: PondNodeArgs) -> Result<()> {
    let mcp_addr = a.mcp_addr.parse()?;
    run_pond_node(PondNodeConfig {
        node_id: a.node_id,
        mcp_addr,
        internal_endpoint: format!("http://{}", a.mcp_addr),
        control_endpoint: a.control,
        data_dir: a.data_dir,
    })
    .await
}

async fn run_node_admin(cmd: NodeCmd) -> Result<()> {
    let mut c = AdminClient::connect(ADMIN_ENDPOINT).await?;
    match cmd {
        NodeCmd::List => {
            let nodes = c.list_nodes(ListNodesRequest {}).await?.into_inner().nodes;
            for n in nodes {
                println!(
                    "{}\t{}\tponds={}\t{}",
                    n.node_id, n.state, n.pond_count, n.mcp_endpoint
                );
            }
        }
        NodeCmd::Describe { node_id } => {
            let n = c
                .describe_node(DescribeNodeRequest { node_id })
                .await?
                .into_inner()
                .node;
            println!("{}", serde_json::to_string_pretty(&node_to_json(n))?);
        }
    }
    Ok(())
}

async fn run_policy_admin(cmd: PolicyCmd) -> Result<()> {
    let mut c = AdminClient::connect(ADMIN_ENDPOINT).await?;
    match cmd {
        PolicyCmd::Show => {
            let p = c.policy_get(PolicyGetRequest {}).await?.into_inner();
            println!("{}", p.policy_json);
        }
        PolicyCmd::Set { key, value } => {
            c.policy_set(PolicySetRequest { key, value }).await?;
            println!("ok");
        }
    }
    Ok(())
}

async fn run_audit_admin(cmd: AuditCmd) -> Result<()> {
    let mut c = AdminClient::connect(ADMIN_ENDPOINT).await?;
    let entries = match cmd {
        AuditCmd::Tail { limit } => {
            c.audit_tail(AuditTailRequest { limit })
                .await?
                .into_inner()
                .entries
        }
        AuditCmd::Search { identity } => {
            c.audit_search(AuditSearchRequest {
                identity,
                since: String::new(),
            })
            .await?
            .into_inner()
            .entries
        }
    };
    for e in entries {
        println!(
            "{}\t{}\tverified={}\t{}\tpond={}\t{}ms",
            e.ts, e.agent_identity, e.verified, e.operation, e.pond_id, e.duration_ms
        );
    }
    Ok(())
}

async fn run_pond_client(cmd: PondCmd) -> Result<()> {
    let client = LatiqClient::connect(MCP_ENDPOINT, None).await?;
    let outcome = match cmd {
        PondCmd::Create { name } => client.allocate_pond(name.as_deref()).await?,
        PondCmd::List => client.list_ponds().await?,
        PondCmd::Describe { pond } => client.describe_pond(&pond).await?,
        PondCmd::Drop { pond } => client.drop_pond(&pond).await?,
    };
    print_outcome(&outcome);
    client.close().await?;
    Ok(())
}

async fn run_query(a: QueryArgs, kind: &str) -> Result<()> {
    let client = LatiqClient::connect(&a.endpoint, a.agent_id).await?;
    let outcome = match kind {
        "write" => client.write(&a.pond, &a.sql).await?,
        "explain" => client.explain(&a.pond, &a.sql).await?,
        _ => client.query(&a.pond, &a.sql).await?,
    };
    print_outcome(&outcome);
    client.close().await?;
    Ok(())
}

fn print_outcome(o: &CallOutcome) {
    if o.is_error {
        eprintln!(
            "error [{}]: {}",
            o.value
                .get("kind")
                .and_then(|v| v.as_str())
                .unwrap_or("error"),
            o.value
                .get("message")
                .and_then(|v| v.as_str())
                .unwrap_or("")
        );
        if let Some(s) = o.value.get("suggest").and_then(|v| v.as_str()) {
            eprintln!("  suggest: {s}");
        }
        if let Some(s) = o.value.get("see").and_then(|v| v.as_str()) {
            eprintln!("  see: {s}");
        }
    } else {
        println!(
            "{}",
            serde_json::to_string_pretty(&o.value).unwrap_or_else(|_| o.value.to_string())
        );
    }
}

fn node_to_json(n: Option<NodeInfo>) -> serde_json::Value {
    match n {
        Some(n) => serde_json::json!({
            "node_id": n.node_id,
            "mcp_endpoint": n.mcp_endpoint,
            "state": n.state,
            "pond_count": n.pond_count,
        }),
        None => serde_json::Value::Null,
    }
}
