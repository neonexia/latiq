//! latiq — single binary. Server roles (control-plane, pond-node), plus the
//! operator/dev CLI. The CLI is a gRPC client (NOT an agent): data ops go to
//! the pond node's Data gRPC; metadata reads + admin go to the control plane's
//! Admin gRPC. MCP is the agent-only surface and the CLI never uses it.
use anyhow::{anyhow, Result};
use clap::{Args, Parser, Subcommand};
use latiq_common::ErrorEnvelope;
use latiq_control_plane::{serve_admin, serve_control, Registry};
use latiq_pond_node::{run_pond_node, PondNodeConfig};
use latiq_proto::v1::admin_client::AdminClient;
use latiq_proto::v1::data_client::DataClient;
use latiq_proto::v1::*;
use std::path::PathBuf;
use tonic::transport::Channel;
use tonic::{Request, Status};

const DATA_ENDPOINT: &str = "http://127.0.0.1:8081";
const ADMIN_ENDPOINT: &str = "http://127.0.0.1:9091";

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
    /// Run a pond-node server (MCP for agents + Data gRPC for CLI/SDK).
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
    /// Pond lifecycle (Data gRPC; `list` reads the control plane).
    #[command(subcommand)]
    Pond(PondCmd),
    /// Run a read-only SQL query (Data gRPC).
    Query(QueryArgs),
    /// Run a write/DDL SQL statement (Data gRPC).
    Write(QueryArgs),
    /// Plan a query without running it (Data gRPC).
    Explain(QueryArgs),
}

#[derive(Args)]
struct ControlPlaneArgs {
    #[arg(long, default_value = "127.0.0.1:9090")]
    control_addr: String,
    #[arg(long, default_value = "127.0.0.1:9091")]
    admin_addr: String,
    #[arg(long)]
    db: Option<PathBuf>,
}

#[derive(Args)]
struct PondNodeArgs {
    #[arg(long, default_value = "node-1")]
    node_id: String,
    #[arg(long, default_value = "127.0.0.1:8080")]
    mcp_addr: String,
    #[arg(long, default_value = "127.0.0.1:8081")]
    data_addr: String,
    #[arg(long, default_value = "http://127.0.0.1:9090")]
    control: String,
    #[arg(long, default_value = "./latiq-data")]
    data_dir: PathBuf,
}

#[derive(Subcommand)]
enum NodeCmd {
    List(AdminConn),
    Describe {
        node_id: String,
        #[command(flatten)]
        conn: AdminConn,
    },
}

#[derive(Subcommand)]
enum PolicyCmd {
    Show(AdminConn),
    Set {
        key: String,
        value: String,
        #[command(flatten)]
        conn: AdminConn,
    },
}

#[derive(Subcommand)]
enum AuditCmd {
    Tail {
        #[arg(long, default_value_t = 20)]
        limit: u32,
        #[command(flatten)]
        conn: AdminConn,
    },
    Search {
        identity: String,
        #[command(flatten)]
        conn: AdminConn,
    },
}

#[derive(Subcommand)]
enum PondCmd {
    Create {
        #[arg(long)]
        name: Option<String>,
        #[command(flatten)]
        conn: DataConn,
    },
    /// List ponds (reads the control-plane registry — works even if pond nodes are down).
    List(AdminConn),
    Describe {
        pond: String,
        #[command(flatten)]
        conn: DataConn,
    },
    Drop {
        pond: String,
        #[arg(long)]
        confirm: bool,
        #[command(flatten)]
        conn: DataConn,
    },
}

/// Connection options for Data gRPC (pond node).
#[derive(Args)]
struct DataConn {
    #[arg(long, default_value = DATA_ENDPOINT)]
    endpoint: String,
    #[arg(long)]
    agent_id: Option<String>,
}

/// Connection options for Admin gRPC (control plane).
#[derive(Args)]
struct AdminConn {
    #[arg(long = "admin", default_value = ADMIN_ENDPOINT)]
    endpoint: String,
}

#[derive(Args)]
struct QueryArgs {
    #[arg(long)]
    pond: String,
    sql: String,
    #[command(flatten)]
    conn: DataConn,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::ControlPlane(a) => run_control_plane(a).await,
        Command::PondNode(a) => run_pond(a).await,
        Command::Node(cmd) => run_node_admin(cmd).await,
        Command::Policy(cmd) => run_policy_admin(cmd).await,
        Command::Audit(cmd) => run_audit_admin(cmd).await,
        Command::Pond(cmd) => run_pond_cmd(cmd).await,
        Command::Query(a) => run_query(a, QueryKind::Read).await,
        Command::Write(a) => run_query(a, QueryKind::Write).await,
        Command::Explain(a) => run_query(a, QueryKind::Explain).await,
    }
}

// ---- server roles -------------------------------------------------------

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
        .map_err(|e| anyhow!("server error: {e}"))?;
    Ok(())
}

async fn run_pond(a: PondNodeArgs) -> Result<()> {
    run_pond_node(PondNodeConfig {
        node_id: a.node_id,
        mcp_addr: a.mcp_addr.parse()?,
        data_addr: a.data_addr.parse()?,
        internal_endpoint: format!("http://{}", a.data_addr),
        control_endpoint: a.control,
        data_dir: a.data_dir,
    })
    .await
}

// ---- gRPC clients (friendly connection errors) --------------------------

async fn data_client(endpoint: &str) -> Result<DataClient<Channel>> {
    DataClient::connect(endpoint.to_string()).await.map_err(|_| {
        anyhow!("could not reach the pond node Data API at {endpoint}. Is it running? Start the stack with ./dev.sh (or `latiq pond-node`).")
    })
}

async fn admin_client(endpoint: &str) -> Result<AdminClient<Channel>> {
    AdminClient::connect(endpoint.to_string()).await.map_err(|_| {
        anyhow!("could not reach the control plane Admin API at {endpoint}. Is it running? Start it with `latiq control-plane` (or ./dev.sh).")
    })
}

fn with_id<T>(msg: T, agent_id: &Option<String>) -> Request<T> {
    let mut r = Request::new(msg);
    if let Some(id) = agent_id {
        if let Ok(v) = id.parse() {
            r.metadata_mut().insert("latiq-agent-id", v);
        }
    }
    r
}

// ---- data CLI (pond node) ----------------------------------------------

enum QueryKind {
    Read,
    Write,
    Explain,
}

async fn run_query(a: QueryArgs, kind: QueryKind) -> Result<()> {
    let mut c = data_client(&a.conn.endpoint).await?;
    let msg = || QueryRequest {
        pond: a.pond.clone(),
        sql: a.sql.clone(),
    };
    let res = match kind {
        QueryKind::Read => c.read_query(with_id(msg(), &a.conn.agent_id)).await,
        QueryKind::Write => c.write_query(with_id(msg(), &a.conn.agent_id)).await,
        QueryKind::Explain => c.explain_query(with_id(msg(), &a.conn.agent_id)).await,
    };
    print_json_result(res)
}

async fn run_pond_cmd(cmd: PondCmd) -> Result<()> {
    match cmd {
        PondCmd::Create { name, conn } => {
            let mut c = data_client(&conn.endpoint).await?;
            let res = c
                .allocate_pond(with_id(
                    AllocatePondRequest {
                        name: name.unwrap_or_default(),
                        policy_json: String::new(),
                    },
                    &conn.agent_id,
                ))
                .await;
            match res {
                Ok(r) => {
                    let r = r.into_inner();
                    println!(
                        "{}",
                        serde_json::json!({"pond_id": r.pond_id, "pond_name": r.pond_name})
                    );
                    Ok(())
                }
                Err(st) => print_status(&st),
            }
        }
        PondCmd::Describe { pond, conn } => {
            let mut c = data_client(&conn.endpoint).await?;
            print_json_result(
                c.describe_pond(with_id(DescribePondRequest { pond }, &conn.agent_id))
                    .await,
            )
        }
        PondCmd::Drop {
            pond,
            confirm,
            conn,
        } => {
            let mut c = data_client(&conn.endpoint).await?;
            match c
                .drop_pond(with_id(
                    DropPondRequest {
                        pond: pond.clone(),
                        confirm,
                    },
                    &conn.agent_id,
                ))
                .await
            {
                Ok(_) => {
                    println!("{}", serde_json::json!({"status": "dropped", "pond": pond}));
                    Ok(())
                }
                Err(st) => print_status(&st),
            }
        }
        PondCmd::List(conn) => {
            let mut c = admin_client(&conn.endpoint).await?;
            let ponds = c.pond_list(PondListRequest {}).await?.into_inner().ponds;
            for p in ponds {
                println!(
                    "{}\t{}\towner={}\t{}",
                    p.pond_id, p.name, p.owner, p.created_at
                );
            }
            Ok(())
        }
    }
}

// ---- admin CLI (control plane) -----------------------------------------

async fn run_node_admin(cmd: NodeCmd) -> Result<()> {
    match cmd {
        NodeCmd::List(conn) => {
            let mut c = admin_client(&conn.endpoint).await?;
            for n in c.list_nodes(ListNodesRequest {}).await?.into_inner().nodes {
                println!(
                    "{}\t{}\tponds={}\t{}",
                    n.node_id, n.state, n.pond_count, n.mcp_endpoint
                );
            }
            Ok(())
        }
        NodeCmd::Describe { node_id, conn } => {
            let mut c = admin_client(&conn.endpoint).await?;
            let n = c
                .describe_node(DescribeNodeRequest { node_id })
                .await?
                .into_inner()
                .node;
            println!("{}", serde_json::to_string_pretty(&node_to_json(n))?);
            Ok(())
        }
    }
}

async fn run_policy_admin(cmd: PolicyCmd) -> Result<()> {
    match cmd {
        PolicyCmd::Show(conn) => {
            let mut c = admin_client(&conn.endpoint).await?;
            println!(
                "{}",
                c.policy_get(PolicyGetRequest {})
                    .await?
                    .into_inner()
                    .policy_json
            );
            Ok(())
        }
        PolicyCmd::Set { key, value, conn } => {
            let mut c = admin_client(&conn.endpoint).await?;
            c.policy_set(PolicySetRequest { key, value }).await?;
            println!("ok");
            Ok(())
        }
    }
}

async fn run_audit_admin(cmd: AuditCmd) -> Result<()> {
    let (entries, _) = match cmd {
        AuditCmd::Tail { limit, conn } => {
            let mut c = admin_client(&conn.endpoint).await?;
            (
                c.audit_tail(AuditTailRequest { limit })
                    .await?
                    .into_inner()
                    .entries,
                (),
            )
        }
        AuditCmd::Search { identity, conn } => {
            let mut c = admin_client(&conn.endpoint).await?;
            (
                c.audit_search(AuditSearchRequest {
                    identity,
                    since: String::new(),
                })
                .await?
                .into_inner()
                .entries,
                (),
            )
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

// ---- rendering ----------------------------------------------------------

/// Print a Data gRPC `JsonResponse` (pretty) or render its structured error.
fn print_json_result(res: Result<tonic::Response<JsonResponse>, Status>) -> Result<()> {
    match res {
        Ok(r) => {
            let json = r.into_inner().json;
            match serde_json::from_str::<serde_json::Value>(&json) {
                Ok(v) => println!("{}", serde_json::to_string_pretty(&v).unwrap_or(json)),
                Err(_) => println!("{json}"),
            }
            Ok(())
        }
        Err(st) => print_status(&st),
    }
}

/// Render a structured error envelope (from Status details) or the raw status.
fn print_status(st: &Status) -> Result<()> {
    if let Ok(env) = serde_json::from_slice::<ErrorEnvelope>(st.details()) {
        let kind = serde_json::to_value(env.kind)
            .ok()
            .and_then(|v| v.as_str().map(str::to_string))
            .unwrap_or_else(|| "error".into());
        eprintln!("error [{kind}]: {}", env.message);
        if !env.suggest.is_empty() {
            eprintln!("  suggest: {}", env.suggest);
        }
        if !env.see.is_empty() {
            eprintln!("  see: {}", env.see);
        }
    } else {
        eprintln!("error: {}", st.message());
    }
    std::process::exit(1);
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
