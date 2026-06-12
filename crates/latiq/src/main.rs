//! latiq — single binary. `serve` runs the control plane; `node add` runs a pond
//! node; the remaining commands are a gRPC client. The CLI talks to the control
//! plane (its single entry point, addressed by the `LATIQ_CONTROL` env var),
//! resolves which node hosts a pond, then runs data ops **node-direct** (the
//! control plane is never in the data path). MCP is the agent-only surface and
//! the CLI never uses it.
use anyhow::{anyhow, Result};
use clap::{Args, Parser, Subcommand};
use latiq_common::ErrorEnvelope;
use latiq_control_plane::{serve_control_plane, Registry};
use latiq_pond_node::{run_pond_node, PondNodeConfig};
use latiq_proto::v1::admin_client::AdminClient;
use latiq_proto::v1::control_client::ControlClient;
use latiq_proto::v1::data_client::DataClient;
use latiq_proto::v1::*;
use std::net::SocketAddr;
use std::path::PathBuf;
use tonic::transport::Channel;
use tonic::{Request, Status};

const DEFAULT_CONTROL: &str = "http://127.0.0.1:51400";

#[derive(Parser)]
#[command(name = "latiq", version, about = "Agent-native data pond")]
#[command(
    after_help = "Env: $LATIQ_CONTROL = control plane address (default http://127.0.0.1:51400); \
$LATIQ_ROOT = data root for serve/node add (default ~/.latiq); \
$LATIQ_GATEWAY = send data ops to this front door (e.g. nginx over several nodes) \
instead of resolving the owning node — the greeter forwards."
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the control plane (registry + Control/Admin gRPC on one port).
    Serve(ServeArgs),
    /// Pond nodes: run one (`add`) or inspect registered ones (`list`/`describe`).
    #[command(subcommand)]
    Node(NodeCmd),
    /// Pond lifecycle (create/list/describe/drop) via the control plane.
    #[command(subcommand)]
    Pond(PondCmd),
    /// Run a SQL statement (read or write) against a pond.
    Query(QueryArgs),
    /// Load curated sample datasets (public DuckLake/standard data) into a pond.
    #[command(subcommand)]
    Dataset(DatasetCmd),
}

#[derive(Subcommand)]
enum DatasetCmd {
    /// List the available sample datasets.
    List,
    /// Load a dataset (or --all) into a pond — one CREATE TABLE per table.
    Load {
        /// Dataset name (omit and pass --all to load every dataset).
        name: Option<String>,
        /// Load every dataset.
        #[arg(long)]
        all: bool,
        #[arg(short, long)]
        pond: String,
        #[arg(short, long)]
        agent_id: Option<String>,
    },
}

#[derive(Args)]
struct ServeArgs {
    /// Port for the Control + Admin gRPC surfaces.
    #[arg(short, long, default_value_t = 51400)]
    port: u16,
    /// Data root; the registry lives at <root>/registry.duckdb (default ~/.latiq).
    #[arg(short, long)]
    root: Option<PathBuf>,
}

#[derive(Subcommand)]
enum NodeCmd {
    /// Start a pond node and register it with the control plane.
    Add(NodeAddArgs),
    /// List registered pond nodes.
    List,
    /// Describe a registered pond node.
    Describe { node_id: String },
}

#[derive(Args)]
struct NodeAddArgs {
    #[arg(long, default_value = "node-1")]
    node_id: String,
    /// Data/Query gRPC port. MCP (agents) is served on port + 1.
    #[arg(short, long, default_value_t = 51401)]
    port: u16,
    /// Data root; pond storage lives under <root>/ponds (default ~/.latiq).
    #[arg(short, long)]
    root: Option<PathBuf>,
}

#[derive(Subcommand)]
enum PondCmd {
    /// Allocate a pond. The control plane picks a node; you don't pass an address.
    Create {
        #[arg(short, long)]
        name: Option<String>,
        /// Owner identity recorded for the pond (relaxed; defaults to anonymous).
        #[arg(short, long)]
        agent_id: Option<String>,
    },
    /// List ponds (control-plane registry; works even if nodes are down).
    List,
    Describe {
        pond: String,
    },
    Drop {
        pond: String,
        #[arg(long)]
        confirm: bool,
    },
}

#[derive(Clone, Copy, clap::ValueEnum)]
enum Format {
    /// Aligned text table (default).
    Tabular,
    /// Raw JSON ({columns, rows, statement, status, _meta}).
    Json,
}

impl std::fmt::Display for Format {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Format::Tabular => "tabular",
            Format::Json => "json",
        })
    }
}

#[derive(Args)]
struct QueryArgs {
    #[arg(short, long)]
    pond: String,
    sql: String,
    /// Identity attributed to your writes (relaxed; defaults to anonymous).
    #[arg(short, long)]
    agent_id: Option<String>,
    /// Output format for read results.
    #[arg(short, long, value_enum, default_value_t = Format::Tabular)]
    format: Format,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Serve(a) => run_serve(a).await,
        Command::Node(NodeCmd::Add(a)) => run_node_add(a).await,
        Command::Node(NodeCmd::List) => node_list().await,
        Command::Node(NodeCmd::Describe { node_id }) => node_describe(node_id).await,
        Command::Pond(cmd) => run_pond_cmd(cmd).await,
        Command::Query(a) => run_query(a).await,
        Command::Dataset(cmd) => run_dataset_cmd(cmd).await,
    }
}

// ---- sample datasets ----------------------------------------------------

/// A curated sample dataset: one or more tables, each loaded from a public URL.
/// Data is NOT stored in the repo — these are standard public datasets.
struct SampleDataset {
    name: &'static str,
    description: &'static str,
    tables: &'static [(&'static str, &'static str)], // (table name, source URL)
}

const DATASETS: &[SampleDataset] = &[
    SampleDataset {
        name: "startrek",
        description: "Star Trek Season 1 scripts — CSV, ~2 KB",
        tables: &[(
            "startrek",
            "https://blobs.duckdb.org/data/Star_Trek-Season_1.csv",
        )],
    },
    SampleDataset {
        name: "holdings",
        description: "Example stock holdings — CSV, ~300 B",
        tables: &[("holdings", "https://duckdb.org/data/holdings.csv")],
    },
    SampleDataset {
        name: "tpch",
        description: "TPC-H scale 0.01 — 8 tables, Parquet, a few MB",
        tables: &[
            (
                "lineitem",
                "https://shell.duckdb.org/data/tpch/0_01/parquet/lineitem.parquet",
            ),
            (
                "orders",
                "https://shell.duckdb.org/data/tpch/0_01/parquet/orders.parquet",
            ),
            (
                "customer",
                "https://shell.duckdb.org/data/tpch/0_01/parquet/customer.parquet",
            ),
            (
                "part",
                "https://shell.duckdb.org/data/tpch/0_01/parquet/part.parquet",
            ),
            (
                "supplier",
                "https://shell.duckdb.org/data/tpch/0_01/parquet/supplier.parquet",
            ),
            (
                "partsupp",
                "https://shell.duckdb.org/data/tpch/0_01/parquet/partsupp.parquet",
            ),
            (
                "nation",
                "https://shell.duckdb.org/data/tpch/0_01/parquet/nation.parquet",
            ),
            (
                "region",
                "https://shell.duckdb.org/data/tpch/0_01/parquet/region.parquet",
            ),
        ],
    },
    SampleDataset {
        name: "taxi",
        description: "NYC yellow-taxi, Apr 2019 — Parquet, ~127 MB (large)",
        tables: &[("taxi", "https://blobs.duckdb.org/data/taxi_2019_04.parquet")],
    },
];

/// The DuckDB reader for a URL, picked by extension.
fn read_fn(url: &str) -> String {
    let u = url.to_lowercase();
    if u.ends_with(".csv") {
        format!("read_csv_auto('{url}')")
    } else if u.ends_with(".json") || u.ends_with(".ndjson") {
        format!("read_json_auto('{url}')")
    } else {
        format!("read_parquet('{url}')")
    }
}

async fn run_dataset_cmd(cmd: DatasetCmd) -> Result<()> {
    match cmd {
        DatasetCmd::List => {
            for d in DATASETS {
                let tn = d.tables.len();
                println!(
                    "{:<10} {} table{}  {}",
                    d.name,
                    tn,
                    if tn == 1 { " " } else { "s" },
                    d.description
                );
            }
            Ok(())
        }
        DatasetCmd::Load {
            name,
            all,
            pond,
            agent_id,
        } => {
            let selected: Vec<&SampleDataset> = if all {
                DATASETS.iter().collect()
            } else {
                let n = name.ok_or_else(|| {
                    anyhow!("pass a dataset name or --all (see `latiq dataset list`)")
                })?;
                vec![DATASETS
                    .iter()
                    .find(|d| d.name == n)
                    .ok_or_else(|| anyhow!("unknown dataset '{n}'; see `latiq dataset list`"))?]
            };
            // Resolve the pond's node once; load each table node-direct.
            let node = data_target(&pond).await?;
            let mut c = data_client(&node).await?;
            for d in selected {
                for (table, url) in d.tables {
                    let sql = format!(
                        "CREATE OR REPLACE TABLE \"{table}\" AS SELECT * FROM {}",
                        read_fn(url)
                    );
                    print!("  {} → {table} … ", d.name);
                    use std::io::Write;
                    std::io::stdout().flush().ok();
                    match c
                        .write_query(with_id(
                            QueryRequest {
                                pond: pond.clone(),
                                sql,
                            },
                            &agent_id,
                        ))
                        .await
                    {
                        Ok(_) => println!("ok"),
                        Err(st) => {
                            println!("FAILED");
                            return print_status(&st);
                        }
                    }
                }
            }
            Ok(())
        }
    }
}

/// Data root: $LATIQ_ROOT if set, else ~/.latiq. The registry lives at
/// `<root>/registry.duckdb` and pond storage under `<root>/ponds`. The `--root`
/// flag overrides both.
fn default_root() -> PathBuf {
    if let Some(r) = std::env::var_os("LATIQ_ROOT") {
        return PathBuf::from(r);
    }
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".latiq")
}

/// Control plane address — from $LATIQ_CONTROL, else the loopback default.
fn control_addr() -> String {
    std::env::var("LATIQ_CONTROL").unwrap_or_else(|_| DEFAULT_CONTROL.to_string())
}

// ---- server roles -------------------------------------------------------

async fn run_serve(a: ServeArgs) -> Result<()> {
    let root = a.root.unwrap_or_else(default_root);
    std::fs::create_dir_all(&root)?;
    let root = std::fs::canonicalize(&root).unwrap_or(root);
    let db = root.join("registry.duckdb");
    let registry = Registry::open(Some(db.as_path()))?;
    let addr: SocketAddr = format!("127.0.0.1:{}", a.port).parse()?;
    println!("control plane: Control + Admin gRPC on {addr}");
    println!("  registry: {}", db.display());
    serve_control_plane(addr, registry)
        .await
        .map_err(|e| anyhow!("server error: {e}"))?;
    Ok(())
}

async fn run_node_add(a: NodeAddArgs) -> Result<()> {
    let root = a.root.unwrap_or_else(default_root);
    std::fs::create_dir_all(&root)?;
    let root = std::fs::canonicalize(&root).unwrap_or(root);
    let data_addr = format!("127.0.0.1:{}", a.port);
    let mcp_addr = format!("127.0.0.1:{}", a.port + 1);
    run_pond_node(PondNodeConfig {
        node_id: a.node_id,
        mcp_addr: mcp_addr.parse()?,
        data_addr: data_addr.parse()?,
        internal_endpoint: format!("http://{data_addr}"),
        control_endpoint: control_addr(),
        data_dir: root.join("ponds"),
    })
    .await
}

// ---- gRPC clients (friendly connection errors) --------------------------

async fn control_client() -> Result<ControlClient<Channel>> {
    let addr = control_addr();
    ControlClient::connect(addr.clone()).await.map_err(|_| {
        anyhow!("could not reach the control plane at {addr}. Is it running? Start it with `latiq serve` (or set $LATIQ_CONTROL).")
    })
}

async fn admin_client() -> Result<AdminClient<Channel>> {
    let addr = control_addr();
    AdminClient::connect(addr.clone()).await.map_err(|_| {
        anyhow!("could not reach the control plane at {addr}. Is it running? Start it with `latiq serve` (or set $LATIQ_CONTROL).")
    })
}

async fn data_client(endpoint: &str) -> Result<DataClient<Channel>> {
    DataClient::connect(endpoint.to_string()).await.map_err(|_| {
        anyhow!("could not reach the pond node at {endpoint}. Is it up? Start one with `latiq node add`.")
    })
}

/// Where the CLI sends data ops for `pond_ref`. Default: resolve the owning node
/// via the control plane and hit it directly. If `$LATIQ_GATEWAY` is set (e.g. an
/// nginx front door over several nodes), send everything there and let the greeter
/// node forward — the same single-front-door model agents use over MCP.
async fn data_target(pond_ref: &str) -> Result<String> {
    match std::env::var("LATIQ_GATEWAY") {
        Ok(gw) if !gw.is_empty() => Ok(gw),
        _ => resolve_node(pond_ref).await,
    }
}

/// Ask the control plane which node's Data gRPC hosts `pond_ref`, so data ops go
/// node-direct. The control plane is only consulted for routing, never the data.
async fn resolve_node(pond_ref: &str) -> Result<String> {
    let mut c = control_client().await?;
    let loc = c
        .get_pond_location(GetPondLocationRequest {
            pond_ref: pond_ref.to_string(),
        })
        .await
        .map_err(|st| anyhow!("pond '{pond_ref}': {}", st.message()))?
        .into_inner();
    Ok(loc.node_endpoint)
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

// ---- query (data; node-direct) ------------------------------------------

async fn run_query(a: QueryArgs) -> Result<()> {
    let node = data_target(&a.pond).await?;
    let mut c = data_client(&node).await?;
    // One `query` for everything: the write path runs DDL/DML and, for a plain
    // SELECT, falls through to a read (no snapshot). Reads are still cap-bounded.
    let res = c
        .write_query(with_id(
            QueryRequest {
                pond: a.pond.clone(),
                sql: a.sql.clone(),
            },
            &a.agent_id,
        ))
        .await;
    match a.format {
        Format::Json => print_json_result(res),
        Format::Tabular => print_table_result(res),
    }
}

// ---- pond lifecycle -----------------------------------------------------

async fn run_pond_cmd(cmd: PondCmd) -> Result<()> {
    match cmd {
        PondCmd::Create { name, agent_id } => {
            // Pure control-plane op: the registry assigns a (random) node; the
            // node materializes storage lazily on first query.
            let mut c = control_client().await?;
            let owner = agent_id.unwrap_or_else(|| "anonymous".into());
            match c
                .create_pond_assignment(CreatePondAssignmentRequest {
                    name: name.unwrap_or_default(),
                    owner_identity: owner,
                    policy_json: "{}".into(),
                })
                .await
            {
                Ok(r) => {
                    let pond_id = r.into_inner().pond_id;
                    let pond_name = c
                        .get_pond_info(GetPondInfoRequest {
                            pond_ref: pond_id.clone(),
                        })
                        .await
                        .ok()
                        .and_then(|x| x.into_inner().pond)
                        .map(|p| p.name)
                        .unwrap_or_default();
                    println!(
                        "{}",
                        serde_json::json!({"pond_id": pond_id, "pond_name": pond_name})
                    );
                    Ok(())
                }
                Err(st) => print_status(&st),
            }
        }
        PondCmd::List => {
            let mut c = admin_client().await?;
            let ponds = c.pond_list(PondListRequest {}).await?.into_inner().ponds;
            for p in ponds {
                println!(
                    "{}\t{}\towner={}\t{}",
                    p.pond_id, p.name, p.owner, p.created_at
                );
            }
            Ok(())
        }
        PondCmd::Describe { pond } => {
            let node = data_target(&pond).await?;
            let mut c = data_client(&node).await?;
            print_json_result(
                c.describe_pond(Request::new(DescribePondRequest { pond }))
                    .await,
            )
        }
        PondCmd::Drop { pond, confirm } => {
            let node = data_target(&pond).await?;
            let mut c = data_client(&node).await?;
            match c
                .drop_pond(Request::new(DropPondRequest {
                    pond: pond.clone(),
                    confirm,
                }))
                .await
            {
                Ok(_) => {
                    println!("{}", serde_json::json!({"status": "dropped", "pond": pond}));
                    Ok(())
                }
                Err(st) => print_status(&st),
            }
        }
    }
}

// ---- node admin (control plane) -----------------------------------------

async fn node_list() -> Result<()> {
    let mut c = admin_client().await?;
    for n in c.list_nodes(ListNodesRequest {}).await?.into_inner().nodes {
        println!(
            "{}\t{}\tponds={}\t{}",
            n.node_id, n.state, n.pond_count, n.mcp_endpoint
        );
    }
    Ok(())
}

async fn node_describe(node_id: String) -> Result<()> {
    let mut c = admin_client().await?;
    let n = c
        .describe_node(DescribeNodeRequest { node_id })
        .await?
        .into_inner()
        .node;
    println!("{}", serde_json::to_string_pretty(&node_to_json(n))?);
    Ok(())
}

// ---- rendering ----------------------------------------------------------

/// Render a query result as a text table (the CLI default); falls back to JSON
/// for non-row payloads, and prints a short status line for writes.
fn print_table_result(res: Result<tonic::Response<JsonResponse>, Status>) -> Result<()> {
    match res {
        Ok(r) => {
            let json = r.into_inner().json;
            let Ok(v) = serde_json::from_str::<serde_json::Value>(&json) else {
                println!("{json}");
                return Ok(());
            };
            match (
                v.get("columns").and_then(|c| c.as_array()),
                v.get("rows").and_then(|r| r.as_array()),
            ) {
                (Some(cols), Some(rows)) if !cols.is_empty() => print_table(cols, rows),
                (Some(_), Some(_)) => {
                    // A write: no result set. Report the snapshot it produced.
                    match v.pointer("/_meta/snapshot_id") {
                        Some(s) if !s.is_null() => println!("ok (snapshot {s})"),
                        _ => println!("ok"),
                    }
                }
                _ => println!("{}", serde_json::to_string_pretty(&v).unwrap_or(json)),
            }
            Ok(())
        }
        Err(st) => print_status(&st),
    }
}

fn cell_str(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Null => String::new(),
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        other => other.to_string(),
    }
}

fn print_table(columns: &[serde_json::Value], rows: &[serde_json::Value]) {
    let headers: Vec<String> = columns
        .iter()
        .map(|c| c.as_str().unwrap_or("").to_string())
        .collect();
    let body: Vec<Vec<String>> = rows
        .iter()
        .map(|r| {
            r.as_array()
                .map(|cells| cells.iter().map(cell_str).collect())
                .unwrap_or_default()
        })
        .collect();
    let mut widths: Vec<usize> = headers.iter().map(|h| h.chars().count()).collect();
    for row in &body {
        for (i, c) in row.iter().enumerate() {
            if i < widths.len() {
                widths[i] = widths[i].max(c.chars().count());
            }
        }
    }
    let line = |cells: &[String]| {
        cells
            .iter()
            .enumerate()
            .map(|(i, c)| format!("{:width$}", c, width = widths.get(i).copied().unwrap_or(0)))
            .collect::<Vec<_>>()
            .join(" | ")
    };
    let rule = widths
        .iter()
        .map(|w| "-".repeat(*w))
        .collect::<Vec<_>>()
        .join("-+-");
    println!();
    println!("{rule}");
    println!("{}", line(&headers));
    println!("{rule}");
    for row in &body {
        println!("{}", line(row));
    }
    println!(
        "({} row{})",
        body.len(),
        if body.len() == 1 { "" } else { "s" }
    );
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_fn_picks_reader_by_extension() {
        assert!(read_fn("https://x/a.parquet").starts_with("read_parquet("));
        assert!(read_fn("https://x/a.csv").starts_with("read_csv_auto("));
        assert!(read_fn("https://x/a.json").starts_with("read_json_auto("));
        // unknown extension defaults to parquet
        assert!(read_fn("https://x/a").starts_with("read_parquet("));
    }

    #[test]
    fn dataset_catalog_is_well_formed() {
        let mut names = std::collections::HashSet::new();
        for d in DATASETS {
            assert!(names.insert(d.name), "duplicate dataset name '{}'", d.name);
            assert!(!d.tables.is_empty(), "dataset '{}' has no tables", d.name);
            for (table, url) in d.tables {
                assert!(!table.is_empty(), "{} has an empty table name", d.name);
                assert!(
                    url.starts_with("https://"),
                    "{}.{table} url must be https (public): {url}",
                    d.name
                );
            }
        }
    }
}
