//! latiq — single binary. `serve` runs the control plane; `node add` runs a pond
//! node; the remaining commands are a gRPC client. The CLI talks to the control
//! plane (its single entry point, addressed by the `LATIQ_SERVER` env var),
//! resolves which node hosts a pond, then runs data ops **node-direct** (the
//! control plane is never in the data path). MCP is the agent-only surface and
//! the CLI never uses it.
use anyhow::{anyhow, Result};
use clap::{Args, Parser, Subcommand};
use latiq_common::tier::{PondTier, ResourceLimits};
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

/// Shown in `--help` for every client command that talks to the control plane.
/// The control-plane address has no flag — it is set via `$LATIQ_SERVER`.
const SERVER_HELP: &str = "\
ENVIRONMENT:
  LATIQ_SERVER  Control-plane address the CLI connects to. Set this first, e.g.
                `export LATIQ_SERVER=http://host:51400` (default http://127.0.0.1:51400).";

/// As `SERVER_HELP`, plus the optional query front door — used by `query` and
/// `pond describe`, which run against a pond node.
const QUERY_HELP: &str = "\
ENVIRONMENT:
  LATIQ_SERVER         Control-plane address the CLI connects to. Set this first,
                       e.g. `export LATIQ_SERVER=http://host:51400` (default
                       http://127.0.0.1:51400).
  LATIQ_QUERY_GATEWAY  Optional data front door (e.g. an nginx LB over several
                       nodes). If set, the query is sent there and the greeter node
                       forwards to the pond's owner; otherwise the CLI connects to
                       the owning node directly.";

#[derive(Parser)]
#[command(name = "latiq", version, about = "Agent-native data pond")]
#[command(after_help = QUERY_HELP)]
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
    /// Dataset catalog: add/list/search/remove entries, and load them into a pond.
    #[command(subcommand)]
    Dataset(DatasetCmd),
    /// System snapshot: nodes (state + heartbeat age), ponds, tiers.
    Stats(StatsArgs),
}

#[derive(Args)]
#[command(after_help = SERVER_HELP)]
struct StatsArgs {
    /// Output format (tabular dashboard or raw json).
    #[arg(short, long, value_enum, default_value_t = Format::Tabular)]
    format: Format,
}

#[derive(Subcommand)]
#[command(after_help = SERVER_HELP)]
enum DatasetCmd {
    /// Add (or replace) a dataset in the catalog. Operator action.
    Add {
        /// Full reference "<namespace>.<name>", e.g. `hf.acme.sales`.
        reference: String,
        /// A table to include, `name=source_uri` (repeatable).
        #[arg(short, long = "table", value_name = "NAME=URI", required = true)]
        tables: Vec<String>,
        /// Reader format for the tables: parquet | csv | json | auto (inferred).
        #[arg(short, long, default_value = "auto")]
        format: String,
        /// Human description.
        #[arg(short, long, default_value = "")]
        description: String,
        /// Searchable tag (repeatable).
        #[arg(long = "tag")]
        tags: Vec<String>,
    },
    /// List/search datasets. Query: `#tag`, `prefix*` (ref glob), or a substring.
    List { query: Option<String> },
    /// Remove a dataset from the catalog. Operator action.
    Remove { reference: String },
    /// Load a dataset's tables into a pond (one table each).
    Load {
        /// Dataset reference, e.g. `latiq.sample.tpch`.
        reference: String,
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
    /// Prometheus /metrics port (default: control port + 1000).
    #[arg(long)]
    metrics_port: Option<u16>,
}

#[derive(Subcommand)]
#[command(after_help = SERVER_HELP)]
enum NodeCmd {
    /// Start a pond node and register it with the control plane.
    Add(NodeAddArgs),
    /// List registered pond nodes.
    List,
    /// Describe a registered pond node.
    Describe { node_id: String },
}

#[derive(Args)]
#[command(after_help = SERVER_HELP)]
struct NodeAddArgs {
    #[arg(long, default_value = "node-1")]
    node_id: String,
    /// Data/Query gRPC port. MCP (agents) is served on port + 1.
    #[arg(short, long, default_value_t = 51401)]
    port: u16,
    /// Data root; pond storage lives under <root>/ponds (default ~/.latiq).
    #[arg(short, long)]
    root: Option<PathBuf>,
    /// Prometheus /metrics port (default: data port + 1000).
    #[arg(long)]
    metrics_port: Option<u16>,
}

#[derive(Subcommand)]
#[command(after_help = SERVER_HELP)]
enum PondCmd {
    /// Allocate a pond. The control plane picks a node; you don't pass an address.
    Create {
        #[arg(short, long)]
        name: Option<String>,
        /// Resource tier: x-small | small | medium | large | x-large (caps the
        /// pond's memory + CPU). Defaults to medium.
        #[arg(short, long, default_value = "medium")]
        tier: String,
        /// Comma-separated DuckDB extensions to load on the pond, e.g.
        /// `--extensions spatial,fts`. Must be baked into the deployment image;
        /// signed/official extensions only (no community extensions).
        #[arg(short, long)]
        extensions: Option<String>,
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
#[command(after_help = QUERY_HELP)]
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
        Command::Stats(a) => run_stats(a).await,
    }
}

// ---- dataset catalog ----------------------------------------------------

/// Split a full reference `"<namespace>.<name>"` on the last dot.
fn split_ref(reference: &str) -> Result<(String, String)> {
    match reference.rsplit_once('.') {
        Some((ns, name)) if !ns.is_empty() && !name.is_empty() => {
            Ok((ns.to_string(), name.to_string()))
        }
        _ => Err(anyhow!(
            "dataset reference must be '<namespace>.<name>', e.g. hf.acme.sales"
        )),
    }
}

async fn run_dataset_cmd(cmd: DatasetCmd) -> Result<()> {
    match cmd {
        DatasetCmd::Add {
            reference,
            tables,
            format,
            description,
            tags,
        } => {
            let (namespace, name) = split_ref(&reference)?;
            let mut table_msgs = Vec::with_capacity(tables.len());
            for t in &tables {
                let (tn, uri) = t
                    .split_once('=')
                    .ok_or_else(|| anyhow!("--table must be NAME=URI (got '{t}')"))?;
                table_msgs.push(DatasetTableMsg {
                    table_name: tn.trim().to_string(),
                    source_uri: uri.trim().to_string(),
                    format: format.clone(),
                });
            }
            let mut c = admin_client().await?;
            let r = c
                .dataset_add(DatasetAddRequest {
                    dataset: Some(DatasetMsg {
                        r#ref: format!("{namespace}.{name}"),
                        namespace,
                        name,
                        description,
                        tags,
                        tables: table_msgs,
                        created_by: "anonymous".into(),
                        created_at: String::new(),
                    }),
                })
                .await
                .map_err(|st| anyhow!("{}", st.message()))?
                .into_inner();
            println!("added {}", r.r#ref);
            Ok(())
        }
        DatasetCmd::List { query } => {
            let mut c = admin_client().await?;
            let datasets = c
                .dataset_list(DatasetListRequest {
                    query: query.unwrap_or_default(),
                })
                .await
                .map_err(|st| anyhow!("{}", st.message()))?
                .into_inner()
                .datasets;
            print_dataset_table(&datasets);
            Ok(())
        }
        DatasetCmd::Remove { reference } => {
            let mut c = admin_client().await?;
            c.dataset_remove(DatasetRemoveRequest {
                r#ref: reference.clone(),
            })
            .await
            .map_err(|st| anyhow!("{}", st.message()))?;
            println!("removed {reference}");
            Ok(())
        }
        DatasetCmd::Load {
            reference,
            pond,
            agent_id,
        } => {
            // Load runs on the pond node (Data gRPC): it resolves the dataset via
            // the control plane and materializes each table into the pond.
            let node = data_target(&pond).await?;
            let mut c = data_client(&node).await?;
            print!("loading {reference} into {pond} … ");
            use std::io::Write;
            std::io::stdout().flush().ok();
            match c
                .load_dataset(with_id(
                    LoadDatasetRequest {
                        pond: pond.clone(),
                        dataset_ref: reference,
                    },
                    &agent_id,
                ))
                .await
            {
                Ok(resp) => {
                    let v: serde_json::Value =
                        serde_json::from_str(&resp.into_inner().json).unwrap_or_default();
                    let tables = v
                        .get("tables")
                        .and_then(|t| t.as_array())
                        .map(|a| a.len())
                        .unwrap_or(0);
                    println!("ok ({tables} table{})", if tables == 1 { "" } else { "s" });
                    Ok(())
                }
                Err(st) => {
                    println!("FAILED");
                    print_status(&st)
                }
            }
        }
    }
}

/// Render `dataset list` as an aligned table: ref, tags, table count, description.
fn print_dataset_table(datasets: &[DatasetMsg]) {
    use std::io::IsTerminal;
    let tty = std::io::stdout().is_terminal();
    let (dim, rst) = if tty {
        ("\x1b[2m", "\x1b[0m")
    } else {
        ("", "")
    };
    if datasets.is_empty() {
        println!("{dim}no datasets{rst}");
        return;
    }
    let rows: Vec<[String; 4]> = datasets
        .iter()
        .map(|d| {
            [
                d.r#ref.clone(),
                d.tags.join(","),
                format!("{}", d.tables.len()),
                d.description.clone(),
            ]
        })
        .collect();
    let header = ["DATASET", "TAGS", "TABLES", "DESCRIPTION"];
    let mut w = header.map(|h| h.len());
    for r in &rows {
        for (i, cell) in r.iter().enumerate() {
            w[i] = w[i].max(cell.len());
        }
    }
    let fmt_row = |c: &[String; 4]| {
        format!(
            "{:<rw0$}  {:<rw1$}  {:>rw2$}  {}",
            c[0],
            c[1],
            c[2],
            c[3],
            rw0 = w[0],
            rw1 = w[1],
            rw2 = w[2],
        )
    };
    println!(
        "{dim}{:<rw0$}  {:<rw1$}  {:>rw2$}  {}{rst}",
        header[0],
        header[1],
        header[2],
        header[3],
        rw0 = w[0],
        rw1 = w[1],
        rw2 = w[2],
    );
    for r in &rows {
        println!("{}", fmt_row(r));
    }
}

/// Default data root when `--root` is not given: ~/.latiq. The registry lives at
/// `<root>/registry.duckdb` and pond storage under `<root>/ponds`. Pass `--root`
/// to override.
fn default_root() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".latiq")
}

/// Control-plane address the CLI connects to — from `$LATIQ_SERVER`, else the
/// loopback default. Set `LATIQ_SERVER` before running any client command (it has
/// no flag; the CLI's single entry point is the control plane).
fn control_addr() -> String {
    std::env::var("LATIQ_SERVER").unwrap_or_else(|_| DEFAULT_CONTROL.to_string())
}

// ---- server roles -------------------------------------------------------

/// Initialize structured logging for the long-running server roles (serve /
/// node add). Honors `$RUST_LOG` (e.g. `RUST_LOG=latiq_agent_core=debug`),
/// defaulting to `info`. Idempotent and only for servers — CLI client commands
/// stay quiet. dev.sh redirects each node's output to `<root>/logs/node-N.log`.
fn init_tracing() {
    use tracing_subscriber::{fmt, EnvFilter};
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    // LATIQ_LOG_FORMAT=json → structured JSON logs (for Loki/ELK/Datadog); else
    // the human-readable format. RUST_LOG still controls level.
    let json = std::env::var("LATIQ_LOG_FORMAT").is_ok_and(|v| v.eq_ignore_ascii_case("json"));
    if json {
        let _ = fmt()
            .json()
            .with_env_filter(filter)
            .with_target(true)
            .try_init();
    } else {
        let _ = fmt().with_env_filter(filter).with_target(true).try_init();
    }
}

/// Resolve the metrics address (`--metrics-port`, default main port + 1000) and
/// start the Prometheus recorder + `/metrics` server. Returns the address logged
/// in the banner.
fn start_metrics(main_port: u16, metrics_port: Option<u16>) -> Result<SocketAddr> {
    let addr: SocketAddr =
        format!("127.0.0.1:{}", metrics_port.unwrap_or(main_port + 1000)).parse()?;
    let handle = latiq_metrics::init_recorder();
    metrics::gauge!("latiq_build_info", "version" => env!("CARGO_PKG_VERSION")).set(1.0);
    tokio::spawn(async move {
        if let Err(e) = latiq_metrics::serve_metrics(addr, handle).await {
            eprintln!("metrics server error: {e}");
        }
    });
    Ok(addr)
}

async fn run_serve(a: ServeArgs) -> Result<()> {
    init_tracing();
    let root = a.root.unwrap_or_else(default_root);
    std::fs::create_dir_all(&root)?;
    let root = std::fs::canonicalize(&root).unwrap_or(root);
    let db = root.join("registry.duckdb");
    let registry = Registry::open(Some(db.as_path()))?;
    let addr: SocketAddr = format!("127.0.0.1:{}", a.port).parse()?;
    let metrics_addr = start_metrics(a.port, a.metrics_port)?;
    latiq_control_plane::spawn_system_collector(registry.clone());
    println!("control plane: Control + Admin gRPC on {addr}");
    println!("  registry: {}", db.display());
    println!("  metrics:  http://{metrics_addr}/metrics");
    serve_control_plane(addr, registry)
        .await
        .map_err(|e| anyhow!("server error: {e}"))?;
    Ok(())
}

async fn run_node_add(a: NodeAddArgs) -> Result<()> {
    init_tracing();
    let root = a.root.unwrap_or_else(default_root);
    std::fs::create_dir_all(&root)?;
    let root = std::fs::canonicalize(&root).unwrap_or(root);
    let data_addr = format!("127.0.0.1:{}", a.port);
    let mcp_addr = format!("127.0.0.1:{}", a.port + 1);
    let metrics_addr: SocketAddr =
        format!("127.0.0.1:{}", a.metrics_port.unwrap_or(a.port + 1000)).parse()?;
    run_pond_node(PondNodeConfig {
        node_id: a.node_id,
        mcp_addr: mcp_addr.parse()?,
        data_addr: data_addr.parse()?,
        internal_endpoint: format!("http://{data_addr}"),
        control_endpoint: control_addr(),
        data_dir: root.join("ponds"),
        metrics_addr: Some(metrics_addr),
    })
    .await
}

// ---- gRPC clients (friendly connection errors) --------------------------

async fn control_client() -> Result<ControlClient<Channel>> {
    let addr = control_addr();
    ControlClient::connect(addr.clone()).await.map_err(|_| {
        anyhow!("could not reach the control plane at {addr}. Is it running, and is $LATIQ_SERVER set to its address? Start one with `latiq serve`.")
    })
}

async fn admin_client() -> Result<AdminClient<Channel>> {
    let addr = control_addr();
    AdminClient::connect(addr.clone()).await.map_err(|_| {
        anyhow!("could not reach the control plane at {addr}. Is it running, and is $LATIQ_SERVER set to its address? Start one with `latiq serve`.")
    })
}

async fn data_client(endpoint: &str) -> Result<DataClient<Channel>> {
    DataClient::connect(endpoint.to_string()).await.map_err(|_| {
        anyhow!("could not reach the pond node at {endpoint}. Is it up? Start one with `latiq node add`.")
    })
}

/// Where the CLI sends data ops for `pond_ref`. Default: resolve the owning node
/// via the control plane and hit it directly. If `$LATIQ_QUERY_GATEWAY` is set
/// (e.g. an nginx front door over several nodes), send everything there and let the
/// greeter node forward — the same single-front-door model agents use over MCP.
async fn data_target(pond_ref: &str) -> Result<String> {
    match std::env::var("LATIQ_QUERY_GATEWAY") {
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
    let msg = with_id(
        QueryRequest {
            pond: a.pond.clone(),
            sql: a.sql.clone(),
        },
        &a.agent_id,
    );
    // Still one `query` command — but route by statement so reads ride the Arrow
    // streaming hop (ReadQuery) and writes are attributed/snapshotted (WriteQuery).
    let res = if latiq_engine::is_read_only(&a.sql) {
        c.read_query(msg).await
    } else {
        c.write_query(msg).await
    };
    match a.format {
        Format::Json => print_json_result(res),
        Format::Tabular => print_table_result(res),
    }
}

// ---- pond lifecycle -----------------------------------------------------

async fn run_pond_cmd(cmd: PondCmd) -> Result<()> {
    match cmd {
        PondCmd::Create {
            name,
            tier,
            extensions,
            agent_id,
        } => {
            // Validate the requested extensions against the allowlist before we
            // call the control plane, so a typo/community name fails locally.
            let exts = match latiq_common::extensions::validate(
                &latiq_common::extensions::parse_csv(&extensions.unwrap_or_default()),
            ) {
                Ok(e) => e,
                Err(msg) => return Err(anyhow!("{msg}")),
            };
            // Pure control-plane op: the registry assigns a (random) node; the
            // node materializes storage lazily on first query.
            let mut c = control_client().await?;
            let owner = agent_id.unwrap_or_else(|| "anonymous".into());
            match c
                .create_pond_assignment(CreatePondAssignmentRequest {
                    name: name.unwrap_or_default(),
                    owner_identity: owner,
                    policy_json: "{}".into(),
                    tier,
                    extensions: exts,
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
            print_pond_list_table(&ponds);
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
            "{}\t{}\tponds={}\tbeat={}s ago\t{}",
            n.node_id, n.state, n.pond_count, n.heartbeat_age_seconds, n.mcp_endpoint
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
            "last_heartbeat": n.last_heartbeat,
            "heartbeat_age_seconds": n.heartbeat_age_seconds,
        }),
        None => serde_json::Value::Null,
    }
}

// ---- stats (system snapshot) --------------------------------------------

async fn run_stats(a: StatsArgs) -> Result<()> {
    let mut c = admin_client().await?;
    let nodes = c.list_nodes(ListNodesRequest {}).await?.into_inner().nodes;
    let ponds = c.pond_list(PondListRequest {}).await?.into_inner().ponds;

    let active = nodes.iter().filter(|n| n.state == "active").count();
    let down = nodes.len() - active;
    let mut counts: std::collections::HashMap<PondTier, usize> = Default::default();
    for p in &ponds {
        let t = PondTier::parse(&p.tier).unwrap_or_default();
        *counts.entry(t).or_default() += 1;
    }
    // Tier rows in canonical size order (smallest → largest), only tiers in use,
    // each carrying its resource caps.
    let tier_rows: Vec<(PondTier, usize, ResourceLimits)> = [
        PondTier::XSmall,
        PondTier::Small,
        PondTier::Medium,
        PondTier::Large,
        PondTier::XLarge,
    ]
    .into_iter()
    .filter_map(|t| counts.get(&t).map(|&n| (t, n, t.limits())))
    .collect();

    match a.format {
        Format::Json => {
            let v = serde_json::json!({
                "nodes": { "total": nodes.len(), "active": active, "down": down },
                "ponds": {
                    "total": ponds.len(),
                    "by_tier": tier_rows.iter().map(|(t, n, l)| serde_json::json!({
                        "tier": t.as_str(),
                        "count": n,
                        "memory_bytes": l.memory_bytes,
                        "cores": l.cores,
                    })).collect::<Vec<_>>(),
                },
                "node_detail": nodes.iter().map(|n| serde_json::json!({
                    "node_id": n.node_id, "state": n.state, "pond_count": n.pond_count,
                    "heartbeat_age_seconds": n.heartbeat_age_seconds, "mcp_endpoint": n.mcp_endpoint,
                })).collect::<Vec<_>>(),
            });
            println!("{}", serde_json::to_string_pretty(&v)?);
        }
        Format::Tabular => print_stats_dashboard(&nodes, &ponds, active, down, &tier_rows),
    }
    Ok(())
}

/// Render `pond list` as an aligned table: one row per pond, with its resource
/// tier and the caps that tier maps to (memory + cores), plus owning node.
fn print_pond_list_table(ponds: &[PondSummary]) {
    use std::io::IsTerminal;
    let tty = std::io::stdout().is_terminal();
    let (dim, rst) = if tty {
        ("\x1b[2m", "\x1b[0m")
    } else {
        ("", "")
    };

    if ponds.is_empty() {
        println!("{dim}no ponds{rst}");
        return;
    }

    // Header + each row's cells, so column widths fit the actual content.
    let header = [
        "NAME", "TIER", "MEMORY", "CORES", "NODE", "OWNER", "POND ID",
    ];
    let rows: Vec<[String; 7]> = ponds
        .iter()
        .map(|p| {
            let tier = PondTier::parse(&p.tier).unwrap_or_default();
            let l = tier.limits();
            [
                p.name.clone(),
                tier.as_str().to_string(),
                fmt_bytes(l.memory_bytes),
                l.cores.to_string(),
                p.node_id.clone(),
                p.owner.clone(),
                p.pond_id.clone(),
            ]
        })
        .collect();

    let mut w = header.map(|h| h.len());
    for r in &rows {
        for (i, cell) in r.iter().enumerate() {
            w[i] = w[i].max(cell.len());
        }
    }

    let fmt_row = |cells: &[String; 7]| {
        cells
            .iter()
            .enumerate()
            .map(|(i, c)| format!("{c:<width$}", width = w[i]))
            .collect::<Vec<_>>()
            .join("  ")
            .trim_end()
            .to_string()
    };
    let header_strs: [String; 7] = header.map(|h| h.to_string());
    println!("{dim}{}{rst}", fmt_row(&header_strs));
    for r in &rows {
        println!("{}", fmt_row(r));
    }
}

/// Humanize a byte cap for the dashboard (e.g. `512 MB`, `4 GB`).
fn fmt_bytes(bytes: u64) -> String {
    const MB: u64 = 1024 * 1024;
    const GB: u64 = 1024 * MB;
    if bytes >= GB && bytes.is_multiple_of(GB) {
        format!("{} GB", bytes / GB)
    } else if bytes >= MB {
        format!("{} MB", bytes / MB)
    } else {
        format!("{bytes} B")
    }
}

fn print_stats_dashboard(
    nodes: &[NodeInfo],
    ponds: &[PondSummary],
    active: usize,
    down: usize,
    tier_rows: &[(PondTier, usize, ResourceLimits)],
) {
    use std::io::IsTerminal;
    let tty = std::io::stdout().is_terminal();
    let (bold, dim, green, red, rst) = if tty {
        ("\x1b[1m", "\x1b[2m", "\x1b[32m", "\x1b[1;31m", "\x1b[0m")
    } else {
        ("", "", "", "", "")
    };
    let bar = "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━";
    let down_c = if down > 0 { red } else { dim };

    println!();
    println!("{bold} {bar}{rst}");
    println!("{bold}  latiq{rst} {dim}· system snapshot{rst}");
    println!("{bold} {bar}{rst}");
    println!();
    println!(
        "   {dim}nodes{rst}  {} total · {green}{active} active{rst} · {down_c}{down} down{rst}",
        nodes.len()
    );
    println!("   {dim}ponds{rst}  {} total", ponds.len());
    if !tier_rows.is_empty() {
        println!();
        println!("   {dim}TIER       PONDS   MEMORY   CORES{rst}");
        for (tier, n, limits) in tier_rows {
            println!(
                "   {:<10} {:>5}  {:>7}  {:>5}",
                tier.as_str(),
                n,
                fmt_bytes(limits.memory_bytes),
                limits.cores
            );
        }
    }
    println!();
    println!("   {dim}NODE        STATE   PONDS  LAST BEAT    ENDPOINT{rst}");
    for n in nodes {
        let sc = if n.state == "active" { green } else { red };
        println!(
            "   {:<11} {sc}{:<6}{rst} {:>5}  {:>7}s ago  {}",
            n.node_id, n.state, n.pond_count, n.heartbeat_age_seconds, n.mcp_endpoint
        );
    }
    println!();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_ref_takes_last_dot_as_name() {
        assert_eq!(
            split_ref("latiq.sample.tpch").unwrap(),
            ("latiq.sample".to_string(), "tpch".to_string())
        );
        assert_eq!(
            split_ref("hf.acme").unwrap(),
            ("hf".to_string(), "acme".to_string())
        );
        assert!(split_ref("nodot").is_err());
        assert!(split_ref(".name").is_err());
        assert!(split_ref("ns.").is_err());
    }
}
