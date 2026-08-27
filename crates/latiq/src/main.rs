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
#[cfg(feature = "server")]
use latiq_control_plane::{serve_control_plane, Registry};
#[cfg(feature = "server")]
use latiq_pond_node::{run_pond_node, PondNodeConfig};
use latiq_proto::v1::admin_client::AdminClient;
use latiq_proto::v1::control_client::ControlClient;
use latiq_proto::v1::data_client::DataClient;
use latiq_proto::v1::*;
#[cfg(feature = "server")]
use std::net::SocketAddr;
#[cfg(feature = "server")]
use std::path::PathBuf;
use tonic::transport::Channel;
use tonic::{Request, Status};

/// Shown in the memory/cores columns for the `none` tier: Latiq applies no caps,
/// so the engine's defaults apply rather than a number we could print.
const UNCAPPED: &str = "engine default";

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
    #[cfg(feature = "server")]
    Serve(ServeArgs),
    /// Pond nodes: run one (`add`) or inspect registered ones (`list`/`describe`).
    #[command(subcommand)]
    Node(NodeCmd),
    /// Pond lifecycle (create/list/describe/drop) via the control plane.
    #[command(subcommand)]
    Pond(PondCmd),
    /// Run a SQL statement (read or write) against a pond.
    Query(QueryArgs),
    /// Datasets (simple files in the `latiq` catalog): add/list/load/remove.
    #[command(subcommand)]
    Dataset(DatasetCmd),
    /// External catalogs (iceberg/…): add/list/describe/pull/remove.
    #[command(subcommand)]
    Catalog(CatalogCmd),
    /// System snapshot: nodes (state + heartbeat age), ponds, tiers.
    Stats(StatsArgs),
    /// Pre-install DuckDB extensions into the local cache (image-bake step, run at
    /// container build so nodes start offline). Not a day-to-day command.
    #[cfg(feature = "server")]
    #[command(hide = true)]
    WarmExtensions,
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
    /// Add (or replace) a dataset. Operator action.
    Add {
        /// Dataset name (a bare identifier), e.g. `sales`.
        name: String,
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
    /// List/search datasets. Query: `#tag`, `prefix*`, or a substring.
    List { query: Option<String> },
    /// Remove a dataset. Operator action.
    Remove { name: String },
    /// Load a dataset's tables into a pond, under a schema named after the dataset.
    Load {
        name: String,
        #[arg(short, long)]
        pond: String,
        #[arg(short, long)]
        agent_id: Option<String>,
        /// OAuth bearer token presented to the server (`Authorization: Bearer`).
        /// Only needed where the deployment configures an issuer; unset means the
        /// relaxed (claimed-identity) path.
        #[arg(long, env = "LATIQ_TOKEN")]
        token: Option<String>,
    },
}

#[derive(Subcommand)]
#[command(after_help = SERVER_HELP)]
enum CatalogCmd {
    /// Register (or replace) an external catalog. Operator action. `--set` carries
    /// locator params (credentials are dropped here — pass them at pull/describe).
    Add {
        /// Catalog name (a bare identifier), e.g. `lake`.
        name: String,
        /// Catalog type: iceberg.
        #[arg(short, long)]
        r#type: String,
        /// Config param `key=value` (repeatable), e.g. `--set endpoint=...`.
        #[arg(short, long = "set", value_name = "KEY=VALUE")]
        set: Vec<String>,
        #[arg(short, long, default_value = "")]
        description: String,
        #[arg(long = "tag")]
        tags: Vec<String>,
    },
    /// List/search catalogs. Query: `#tag`, `prefix*`, or a substring.
    List { query: Option<String> },
    /// List a catalog's tables (transient attach on a pond). `--set` for creds.
    Describe {
        name: String,
        #[arg(short, long)]
        pond: String,
        #[arg(short, long = "set", value_name = "KEY=VALUE")]
        set: Vec<String>,
        #[arg(short, long)]
        agent_id: Option<String>,
        /// OAuth bearer token presented to the server (`Authorization: Bearer`).
        /// Only needed where the deployment configures an issuer; unset means the
        /// relaxed (claimed-identity) path.
        #[arg(long, env = "LATIQ_TOKEN")]
        token: Option<String>,
    },
    /// Pull from a catalog into a pond: transient attach → run the query → detach.
    Pull {
        name: String,
        #[arg(short, long)]
        pond: String,
        /// SQL that materializes into the pond, e.g.
        /// `CREATE TABLE t AS SELECT * FROM <catalog>.schema.table WHERE …`.
        #[arg(short, long)]
        query: String,
        #[arg(short, long = "set", value_name = "KEY=VALUE")]
        set: Vec<String>,
        #[arg(short, long)]
        agent_id: Option<String>,
        /// OAuth bearer token presented to the server (`Authorization: Bearer`).
        /// Only needed where the deployment configures an issuer; unset means the
        /// relaxed (claimed-identity) path.
        #[arg(long, env = "LATIQ_TOKEN")]
        token: Option<String>,
    },
    /// Remove a catalog. Operator action.
    Remove { name: String },
}

#[cfg(feature = "server")]
#[derive(Args)]
struct ServeArgs {
    /// Port for the Control + Admin gRPC surfaces.
    #[arg(short, long, default_value_t = 51400)]
    port: u16,
    /// Host/interface to bind. Defaults to loopback; use 0.0.0.0 in containers so
    /// pond nodes and Prometheus on other hosts can reach it.
    #[arg(long, default_value = "127.0.0.1")]
    bind: String,
    /// Data root; the registry lives at <root>/registry.duckdb (default ~/.latiq).
    #[arg(short, long)]
    root: Option<PathBuf>,
    /// Prometheus /metrics port (default: control port + 1000).
    #[arg(long)]
    metrics_port: Option<u16>,
    /// Trusted OIDC issuer URL. Repeatable: pass it more than once, or set
    /// LATIQ_AUTH_ISSUER to a comma-separated list, to trust several IdPs (the
    /// usual case being a workforce IdP for operators plus a workload IdP for
    /// agents). Any issuer here turns on verification for every surface on this
    /// process. None = relaxed claimed identity (dev / embedded).
    #[arg(long, env = "LATIQ_AUTH_ISSUER", value_delimiter = ',')]
    auth_issuer: Vec<String>,
    /// The audience this deployment expects in a token (`aud`). Required
    /// whenever an issuer is set: without it, a token minted for any other
    /// service that trusts the same IdP would be accepted here. One value for
    /// all issuers -- the audience names US, not who vouched for the caller.
    #[arg(long, env = "LATIQ_AUTH_AUDIENCE")]
    auth_audience: Option<String>,
    /// Explicit JWKS URL, overriding the default derived from the issuer. Only
    /// valid with exactly ONE --auth-issuer, since it cannot be matched to a
    /// particular issuer otherwise. Needed for split-horizon deployments where
    /// the issuer identifier is not a reachable address.
    #[arg(long, env = "LATIQ_AUTH_JWKS_URI")]
    auth_jwks_uri: Option<String>,
}

#[derive(Subcommand)]
#[command(after_help = SERVER_HELP)]
enum NodeCmd {
    /// Start a pond node and register it with the control plane.
    #[cfg(feature = "server")]
    Add(NodeAddArgs),
    /// List registered pond nodes.
    List,
    /// Describe a registered pond node.
    Describe { node_id: String },
}

#[cfg(feature = "server")]
#[derive(Args)]
#[command(after_help = SERVER_HELP)]
struct NodeAddArgs {
    #[arg(long, default_value = "node-1")]
    node_id: String,
    /// Data/Query gRPC port. MCP (agents) is served on port + 1.
    #[arg(short, long, default_value_t = 51401)]
    port: u16,
    /// Host/interface to bind. Defaults to loopback; use 0.0.0.0 in containers.
    #[arg(long, default_value = "127.0.0.1")]
    bind: String,
    /// host:port other nodes/agents use to reach this node, advertised to the
    /// control plane for query forwarding. Defaults to 127.0.0.1:<port>; set it to
    /// this node's container/pod hostname (e.g. pond-node-1:51401) in multi-host
    /// deployments, or forwarding lands on the wrong host.
    #[arg(long)]
    advertise_addr: Option<String>,
    /// Data root; pond storage lives under <root>/ponds (default ~/.latiq).
    #[arg(short, long)]
    root: Option<PathBuf>,
    /// Prometheus /metrics port (default: data port + 1000).
    #[arg(long)]
    metrics_port: Option<u16>,
    /// Trusted OIDC issuer URL. Repeatable: pass it more than once, or set
    /// LATIQ_AUTH_ISSUER to a comma-separated list, to trust several IdPs (the
    /// usual case being a workforce IdP for operators plus a workload IdP for
    /// agents). Any issuer here turns on verification for every surface on this
    /// process. None = relaxed claimed identity (dev / embedded).
    #[arg(long, env = "LATIQ_AUTH_ISSUER", value_delimiter = ',')]
    auth_issuer: Vec<String>,
    /// The audience this deployment expects in a token (`aud`). Required
    /// whenever an issuer is set: without it, a token minted for any other
    /// service that trusts the same IdP would be accepted here. One value for
    /// all issuers -- the audience names US, not who vouched for the caller.
    #[arg(long, env = "LATIQ_AUTH_AUDIENCE")]
    auth_audience: Option<String>,
    /// Explicit JWKS URL, overriding the default derived from the issuer. Only
    /// valid with exactly ONE --auth-issuer, since it cannot be matched to a
    /// particular issuer otherwise. Needed for split-horizon deployments where
    /// the issuer identifier is not a reachable address.
    #[arg(long, env = "LATIQ_AUTH_JWKS_URI")]
    auth_jwks_uri: Option<String>,
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
        /// Free-text description of what this pond is for, so other agents can
        /// discover it (shown in `pond list`/`describe`).
        #[arg(short, long)]
        description: Option<String>,
    },
    /// List ponds (control-plane registry; works even if nodes are down).
    List {
        /// Output format. `json` includes each pond's owning `node_id`.
        #[arg(short, long, value_enum, default_value_t = Format::Tabular)]
        format: Format,
    },
    Describe {
        pond: String,
    },
    Drop {
        pond: String,
        #[arg(long)]
        confirm: bool,
    },
    /// Change a pond's resource tier after creation (its memory + CPU caps).
    /// Takes effect on the pond's next query — in-flight queries finish under
    /// the old caps.
    SetTier {
        /// Pond name or id.
        pond: String,
        /// x-small | small | medium | large | x-large, or `none` to apply no caps
        /// at all (the engine's own defaults then govern the pond — DuckDB uses
        /// every core and ~80% of RAM). `none` is operator-only and cannot be
        /// requested when the pond is created.
        #[arg(short, long)]
        tier: String,
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
    /// OAuth bearer token presented to the server (`Authorization: Bearer`).
    /// Only needed where the deployment configures an issuer; unset means the
    /// relaxed (claimed-identity) path.
    #[arg(long, env = "LATIQ_TOKEN")]
    token: Option<String>,
    /// Output format for read results.
    #[arg(short, long, value_enum, default_value_t = Format::Tabular)]
    format: Format,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        #[cfg(feature = "server")]
        Command::Serve(a) => run_serve(a).await,
        #[cfg(feature = "server")]
        Command::Node(NodeCmd::Add(a)) => run_node_add(a).await,
        Command::Node(NodeCmd::List) => node_list().await,
        Command::Node(NodeCmd::Describe { node_id }) => node_describe(node_id).await,
        Command::Pond(cmd) => run_pond_cmd(cmd).await,
        Command::Query(a) => run_query(a).await,
        Command::Dataset(cmd) => run_dataset_cmd(cmd).await,
        Command::Catalog(cmd) => run_catalog_cmd(cmd).await,
        Command::Stats(a) => run_stats(a).await,
        #[cfg(feature = "server")]
        Command::WarmExtensions => {
            latiq_pond_node::warm_extensions().map_err(|e| anyhow!("warm extensions: {e}"))?;
            println!("DuckDB extensions warmed into the local cache.");
            Ok(())
        }
    }
}

// ---- datasets + external catalogs ---------------------------------------

/// Parse repeatable `key=value` flags into a map (used by `--set`).
fn parse_kv(items: &[String], flag: &str) -> Result<std::collections::HashMap<String, String>> {
    let mut out = std::collections::HashMap::new();
    for it in items {
        let (k, v) = it
            .split_once('=')
            .ok_or_else(|| anyhow!("{flag} must be KEY=VALUE (got '{it}')"))?;
        out.insert(k.trim().to_string(), v.trim().to_string());
    }
    Ok(out)
}

async fn run_dataset_cmd(cmd: DatasetCmd) -> Result<()> {
    match cmd {
        DatasetCmd::Add {
            name,
            tables,
            format,
            description,
            tags,
        } => {
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
                        name,
                        description,
                        tags,
                        tables: table_msgs,
                        created_by: "anonymous".into(),
                        created_at: String::new(),
                    }),
                })
                .await
                .map_err(render_status)?
                .into_inner();
            println!("added {}", r.name);
            Ok(())
        }
        DatasetCmd::List { query } => {
            let mut c = admin_client().await?;
            let datasets = c
                .dataset_list(DatasetListRequest {
                    query: query.unwrap_or_default(),
                })
                .await
                .map_err(render_status)?
                .into_inner()
                .datasets;
            let rows: Vec<[String; 4]> = datasets
                .iter()
                .map(|d| {
                    [
                        d.name.clone(),
                        d.tags.join(","),
                        d.tables.len().to_string(),
                        d.description.clone(),
                    ]
                })
                .collect();
            print_kv_table(
                &["NAME", "TAGS", "TABLES", "DESCRIPTION"],
                &rows,
                2,
                "no datasets",
            );
            Ok(())
        }
        DatasetCmd::Remove { name } => {
            let mut c = admin_client().await?;
            c.dataset_remove(DatasetRemoveRequest { name: name.clone() })
                .await
                .map_err(render_status)?;
            println!("removed {name}");
            Ok(())
        }
        DatasetCmd::Load {
            name,
            pond,
            agent_id,
            token,
        } => {
            let node = data_target(&pond).await?;
            let mut c = data_client(&node).await?;
            print!("loading {name} into {pond} … ");
            use std::io::Write;
            std::io::stdout().flush().ok();
            match c
                .load_dataset(with_id(
                    LoadDatasetRequest {
                        pond: pond.clone(),
                        dataset: name,
                    },
                    &agent_id,
                    &token,
                ))
                .await
            {
                Ok(resp) => {
                    let v: serde_json::Value =
                        serde_json::from_str(&resp.into_inner().json).unwrap_or_default();
                    let n = v
                        .get("tables")
                        .and_then(|t| t.as_array())
                        .map_or(0, |a| a.len());
                    println!("ok ({n} table{})", if n == 1 { "" } else { "s" });
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

async fn run_catalog_cmd(cmd: CatalogCmd) -> Result<()> {
    match cmd {
        CatalogCmd::Add {
            name,
            r#type,
            set,
            description,
            tags,
        } => {
            let params = parse_kv(&set, "--set")?;
            let mut c = admin_client().await?;
            let r = c
                .catalog_add(CatalogAddRequest {
                    catalog: Some(CatalogMsg {
                        name,
                        r#type,
                        params,
                        description,
                        tags,
                        created_by: "anonymous".into(),
                        created_at: String::new(),
                    }),
                })
                .await
                .map_err(render_status)?
                .into_inner();
            println!("added {}", r.name);
            if !r.dropped_params.is_empty() {
                // Credentials never persist — they're dropped here and passed at pull.
                println!(
                    "  (not stored, pass at pull: {})",
                    r.dropped_params.join(", ")
                );
            }
            Ok(())
        }
        CatalogCmd::List { query } => {
            let mut c = admin_client().await?;
            let catalogs = c
                .catalog_list(CatalogListRequest {
                    query: query.unwrap_or_default(),
                })
                .await
                .map_err(render_status)?
                .into_inner()
                .catalogs;
            let rows: Vec<[String; 4]> = catalogs
                .iter()
                .map(|c| {
                    [
                        c.name.clone(),
                        c.r#type.clone(),
                        c.tags.join(","),
                        c.description.clone(),
                    ]
                })
                .collect();
            print_kv_table(
                &["NAME", "TYPE", "TAGS", "DESCRIPTION"],
                &rows,
                99,
                "no catalogs",
            );
            Ok(())
        }
        CatalogCmd::Describe {
            name,
            pond,
            set,
            agent_id,
            token,
        } => {
            let params = parse_kv(&set, "--set")?;
            let node = data_target(&pond).await?;
            let mut c = data_client(&node).await?;
            let resp = c
                .catalog_describe(with_id(
                    CatalogDescribeRequest {
                        pond,
                        catalog: name,
                        params,
                    },
                    &agent_id,
                    &token,
                ))
                .await
                .map_err(render_status)?
                .into_inner();
            println!("{}", resp.json);
            Ok(())
        }
        CatalogCmd::Pull {
            name,
            pond,
            query,
            set,
            agent_id,
            token,
        } => {
            let params = parse_kv(&set, "--set")?;
            let node = data_target(&pond).await?;
            let mut c = data_client(&node).await?;
            print!("pulling from {name} into {pond} … ");
            use std::io::Write;
            std::io::stdout().flush().ok();
            match c
                .catalog_pull(with_id(
                    CatalogPullRequest {
                        pond,
                        catalog: name,
                        query,
                        params,
                    },
                    &agent_id,
                    &token,
                ))
                .await
            {
                Ok(_) => {
                    println!("ok");
                    Ok(())
                }
                Err(st) => {
                    println!("FAILED");
                    print_status(&st)
                }
            }
        }
        CatalogCmd::Remove { name } => {
            let mut c = admin_client().await?;
            c.catalog_remove(CatalogRemoveRequest { name: name.clone() })
                .await
                .map_err(render_status)?;
            println!("removed {name}");
            Ok(())
        }
    }
}

/// Render an aligned table. `right_col` is the index to right-align (use a large
/// number for none); the last column is left unpadded.
fn print_kv_table<const N: usize>(
    header: &[&str; N],
    rows: &[[String; N]],
    right_col: usize,
    empty: &str,
) {
    use std::io::IsTerminal;
    let (dim, rst) = if std::io::stdout().is_terminal() {
        ("\x1b[2m", "\x1b[0m")
    } else {
        ("", "")
    };
    if rows.is_empty() {
        println!("{dim}{empty}{rst}");
        return;
    }
    let mut w = [0usize; N];
    for (i, h) in header.iter().enumerate() {
        w[i] = h.len();
    }
    for r in rows {
        for (i, cell) in r.iter().enumerate() {
            w[i] = w[i].max(cell.len());
        }
    }
    let fmt = |cells: &[String; N]| -> String {
        let mut s = String::new();
        for (i, cell) in cells.iter().enumerate() {
            if i > 0 {
                s.push_str("  ");
            }
            if i == N - 1 {
                s.push_str(cell);
            } else if i == right_col {
                s.push_str(&format!("{cell:>width$}", width = w[i]));
            } else {
                s.push_str(&format!("{cell:<width$}", width = w[i]));
            }
        }
        s
    };
    let head: [String; N] = std::array::from_fn(|i| header[i].to_string());
    println!("{dim}{}{rst}", fmt(&head));
    for r in rows {
        println!("{}", fmt(r));
    }
}

/// Default data root when `--root` is not given: ~/.latiq. The registry lives at
/// `<root>/registry.duckdb` and pond storage under `<root>/ponds`. Pass `--root`
/// to override.
#[cfg(feature = "server")]
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

/// The `internal_endpoint` a pond node advertises to the control plane for query
/// forwarding. `--advertise-addr` takes a host or host:port (a bare host gets the
/// data `port` appended); `http://` is added if absent. Without the flag we keep
/// the historical loopback default so single-host runs are unchanged.
#[cfg(feature = "server")]
fn advertise_endpoint(advertise_addr: Option<&str>, port: u16) -> String {
    let raw = match advertise_addr {
        Some(a) if a.contains(':') => a.to_string(),
        Some(host) => format!("{host}:{port}"),
        None => format!("127.0.0.1:{port}"),
    };
    if raw.starts_with("http://") || raw.starts_with("https://") {
        raw
    } else {
        format!("http://{raw}")
    }
}

// ---- server roles -------------------------------------------------------

/// Initialize structured logging for the long-running server roles (serve /
/// node add). Honors `$RUST_LOG` (e.g. `RUST_LOG=latiq_agent_core=debug`),
/// defaulting to `info`. Idempotent and only for servers — CLI client commands
/// stay quiet. dev.sh redirects each node's output to `<root>/logs/node-N.log`.
#[cfg(feature = "server")]
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
#[cfg(feature = "server")]
fn start_metrics(bind: &str, main_port: u16, metrics_port: Option<u16>) -> Result<SocketAddr> {
    let addr: SocketAddr = format!(
        "{bind}:{}",
        metrics_port.unwrap_or(main_port.saturating_add(1000))
    )
    .parse()?;
    let handle = latiq_metrics::init_recorder();
    metrics::gauge!("latiq_build_info", "version" => env!("CARGO_PKG_VERSION")).set(1.0);
    tokio::spawn(async move {
        if let Err(e) = latiq_metrics::serve_metrics(addr, handle).await {
            eprintln!("metrics server error: {e}");
        }
    });
    Ok(addr)
}

/// A blank value means "not set". Compose always passes the variable through
/// (possibly empty), so an empty string must mean auth off, not a broken issuer.
fn non_blank(v: Option<String>) -> Option<String> {
    v.map(|s| s.trim().to_string()).filter(|s| !s.is_empty())
}

/// Build the auth config the server surfaces verify against, from the three
/// `--auth-*` flags. `None` = the relaxed (claimed-identity) path this binary
/// has always taken; there is no partial state in between.
///
/// Both errors here are refusals to guess: an issuer without an audience would
/// accept any token minted for any service that trusts the same IdP, and a JWKS
/// uri that cannot be matched to one issuer would silently be applied to the
/// wrong one. Either is a security hole, so neither gets a default.
#[cfg(feature = "server")]
fn auth_config(
    issuers: Vec<String>,
    audience: Option<String>,
    jwks_uri: Option<String>,
) -> Result<Option<latiq_auth::AuthConfig>> {
    let issuers: Vec<String> = issuers
        .into_iter()
        .filter_map(|i| non_blank(Some(i)))
        .collect();
    if issuers.is_empty() {
        return Ok(None);
    }
    let Some(audience) = non_blank(audience) else {
        return Err(anyhow!(
            "--auth-issuer is set but --auth-audience is not. Without an audience a token minted \
             for any other service that trusts the same issuer would be accepted here. Set \
             --auth-audience (or $LATIQ_AUTH_AUDIENCE) to the audience this deployment expects."
        ));
    };
    let jwks_uri = non_blank(jwks_uri);
    if jwks_uri.is_some() && issuers.len() > 1 {
        return Err(anyhow!(
            "--auth-jwks-uri cannot be used with {} issuers: it names ONE issuer's key set and \
             there is no way to tell which. Configure a single issuer, or drop the flag and let \
             each issuer's JWKS be discovered from its own URL.",
            issuers.len()
        ));
    }
    Ok(Some(latiq_auth::AuthConfig {
        audience,
        issuers: issuers
            .into_iter()
            .map(|issuer| latiq_auth::IssuerConfig {
                issuer,
                // Only ever `Some` in the single-issuer case, guarded above.
                jwks_uri: jwks_uri.clone(),
            })
            .collect(),
    }))
}

#[cfg(feature = "server")]
async fn run_serve(a: ServeArgs) -> Result<()> {
    init_tracing();
    // Resolved BEFORE anything binds: an incoherent auth config must stop the
    // process, never quietly downgrade it to unauthenticated.
    let auth = auth_config(a.auth_issuer, a.auth_audience, a.auth_jwks_uri)?;
    let root = a.root.unwrap_or_else(default_root);
    std::fs::create_dir_all(&root)?;
    let root = std::fs::canonicalize(&root).unwrap_or(root);
    let db = root.join("registry.duckdb");
    let registry = Registry::open(Some(db.as_path()))?;
    let addr: SocketAddr = format!("{}:{}", a.bind, a.port).parse()?;
    let metrics_addr = start_metrics(&a.bind, a.port, a.metrics_port)?;
    latiq_control_plane::spawn_system_collector(registry.clone());
    println!("control plane: Control + Admin gRPC on {addr}");
    println!("  registry: {}", db.display());
    println!("  metrics:  http://{metrics_addr}/metrics");
    match &auth {
        Some(cfg) => println!(
            "  auth:     verifying tokens for audience '{}' from {}",
            cfg.audience,
            cfg.issuers
                .iter()
                .map(|i| i.issuer.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ),
        // No issuer configured: identity stays relaxed (claimed, anonymous by
        // default) exactly as it was before the flags existed.
        None => println!("  auth:     off (relaxed claimed identity)"),
    }
    serve_control_plane(addr, registry, auth)
        .await
        .map_err(|e| anyhow!("server error: {e}"))?;
    Ok(())
}

#[cfg(feature = "server")]
async fn run_node_add(a: NodeAddArgs) -> Result<()> {
    init_tracing();
    // Resolved BEFORE the node registers or serves, for the same reason as
    // `run_serve`: a node that comes up with a half-configured verifier is a
    // node accepting unauthenticated callers.
    let auth = auth_config(a.auth_issuer, a.auth_audience, a.auth_jwks_uri)?;
    let root = a.root.unwrap_or_else(default_root);
    std::fs::create_dir_all(&root)?;
    let root = std::fs::canonicalize(&root).unwrap_or(root);
    let data_addr = format!("{}:{}", a.bind, a.port);
    let mcp_addr = format!("{}:{}", a.bind, a.port + 1);
    let metrics_addr: SocketAddr = format!(
        "{}:{}",
        a.bind,
        a.metrics_port.unwrap_or(a.port.saturating_add(1000))
    )
    .parse()?;
    // The endpoint OTHER nodes use to reach us (stored in the registry for
    // forwarding). Defaults to loopback so single-host runs are unchanged; in
    // containers `--advertise-addr pond-node-1:51401` makes forwarding routable.
    let advertise = advertise_endpoint(a.advertise_addr.as_deref(), a.port);
    run_pond_node(PondNodeConfig {
        node_id: a.node_id,
        mcp_addr: mcp_addr.parse()?,
        data_addr: data_addr.parse()?,
        internal_endpoint: advertise,
        control_endpoint: control_addr(),
        data_dir: root.join("ponds"),
        metrics_addr: Some(metrics_addr),
        // `None` unless --auth-issuer was given: no flags means the relaxed
        // (claimed) identity path this node has always run.
        auth,
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
        .map_err(render_status)?
        .into_inner();
    Ok(loc.node_endpoint)
}

/// The bearer credential to present, if any. Blank is absent for the same reason
/// as the server flags: `LATIQ_TOKEN=` in a compose file or a `.env` must not
/// become an `Authorization: Bearer ` header that is rejected as malformed.
fn bearer_of(token: &Option<String>) -> Option<String> {
    non_blank(token.clone())
}

/// The metadata every data op carries: the CLAIMED agent id, and — when the
/// deployment requires it — the bearer token that actually proves who we are.
///
/// The token rides gRPC metadata per request rather than the channel: a `Channel`
/// is shared and cached, metadata is not.
fn with_id<T>(msg: T, agent_id: &Option<String>, token: &Option<String>) -> Request<T> {
    let mut r = Request::new(msg);
    if let Some(id) = agent_id {
        if let Ok(v) = id.parse() {
            r.metadata_mut().insert("latiq-agent-id", v);
        }
    }
    if let Some(t) = bearer_of(token) {
        if let Ok(v) = format!("Bearer {t}").parse() {
            r.metadata_mut().insert("authorization", v);
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
        &a.token,
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
            description,
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
                    description: description.unwrap_or_default(),
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
        PondCmd::List { format } => {
            let mut c = admin_client().await?;
            let ponds = c.pond_list(PondListRequest {}).await?.into_inner().ponds;
            match format {
                Format::Json => {
                    let v: Vec<_> = ponds
                        .iter()
                        .map(|p| {
                            serde_json::json!({
                                "pond_id": p.pond_id, "name": p.name, "owner": p.owner,
                                "node_id": p.node_id, "tier": p.tier, "created_at": p.created_at,
                                "description": p.description,
                            })
                        })
                        .collect();
                    println!("{}", serde_json::to_string_pretty(&v)?);
                }
                Format::Tabular => print_pond_list_table(&ponds),
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
        PondCmd::SetTier { pond, tier } => {
            let mut c = admin_client().await?;
            let r = c
                .pond_set_tier(PondSetTierRequest {
                    pond: pond.clone(),
                    tier: tier.clone(),
                })
                .await
                .map_err(render_status)?
                .into_inner();
            println!(
                "{}",
                serde_json::json!({"pond": r.pond, "tier": r.tier,
                                   "note": "applies on the pond's next query"})
            );
            Ok(())
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

/// Render a gRPC error and exit(1): the structured `ErrorEnvelope` from
/// `Status.details` (every surface now attaches one) — kind + message + suggest +
/// see — or the raw status if there's no envelope. Never returns.
fn print_status(st: &Status) -> ! {
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

/// `.map_err(render_status)` for any gRPC call: render the error's envelope and
/// exit. Use at every call site so guidance (not a bare status string) reaches
/// the user. Returns `anyhow::Error` only to satisfy `map_err`; it never returns.
fn render_status(st: Status) -> anyhow::Error {
    print_status(&st)
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
    let tier_rows: Vec<(PondTier, usize, Option<ResourceLimits>)> = [
        PondTier::XSmall,
        PondTier::Small,
        PondTier::Medium,
        PondTier::Large,
        PondTier::XLarge,
        PondTier::None,
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
                        // null for the `none` tier: Latiq applies no caps, so
                        // the engine's own defaults are in force.
                        "memory_bytes": l.map(|x| x.memory_bytes),
                        "cores": l.map(|x| x.cores),
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
        "NAME",
        "TIER",
        "MEMORY",
        "CORES",
        "NODE",
        "OWNER",
        "POND ID",
        "DESCRIPTION",
    ];
    let rows: Vec<[String; 8]> = ponds
        .iter()
        .map(|p| {
            let tier = PondTier::parse(&p.tier).unwrap_or_default();
            let l = tier.limits();
            [
                p.name.clone(),
                tier.as_str().to_string(),
                l.map_or_else(|| UNCAPPED.into(), |l| fmt_bytes(l.memory_bytes)),
                l.map_or_else(|| UNCAPPED.into(), |l| l.cores.to_string()),
                p.node_id.clone(),
                p.owner.clone(),
                p.pond_id.clone(),
                p.description.clone(),
            ]
        })
        .collect();

    let mut w = header.map(|h| h.len());
    for r in &rows {
        for (i, cell) in r.iter().enumerate() {
            w[i] = w[i].max(cell.len());
        }
    }

    let fmt_row = |cells: &[String; 8]| {
        cells
            .iter()
            .enumerate()
            .map(|(i, c)| format!("{c:<width$}", width = w[i]))
            .collect::<Vec<_>>()
            .join("  ")
            .trim_end()
            .to_string()
    };
    let header_strs: [String; 8] = header.map(|h| h.to_string());
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
    tier_rows: &[(PondTier, usize, Option<ResourceLimits>)],
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
                limits.map_or_else(|| UNCAPPED.into(), |l| fmt_bytes(l.memory_bytes)),
                limits.map_or_else(|| UNCAPPED.into(), |l| l.cores.to_string())
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
    fn parse_kv_splits_on_first_equals() {
        let m = parse_kv(
            &[
                "endpoint=https://x?a=b".to_string(),
                "warehouse=prod".to_string(),
            ],
            "--set",
        )
        .unwrap();
        assert_eq!(m["endpoint"], "https://x?a=b");
        assert_eq!(m["warehouse"], "prod");
        assert!(parse_kv(&["noequals".to_string()], "--set").is_err());
    }

    #[cfg(feature = "server")]
    #[test]
    fn advertise_endpoint_defaults_and_overrides() {
        // No flag → historical loopback default (single-host behavior unchanged).
        assert_eq!(advertise_endpoint(None, 51401), "http://127.0.0.1:51401");
        // Bare host → the data port is appended (the container case).
        assert_eq!(
            advertise_endpoint(Some("pond-node-1"), 51401),
            "http://pond-node-1:51401"
        );
        // host:port is taken verbatim (different advertised port than bound).
        assert_eq!(
            advertise_endpoint(Some("10.0.0.5:9000"), 51401),
            "http://10.0.0.5:9000"
        );
        // An explicit scheme is preserved.
        assert_eq!(
            advertise_endpoint(Some("http://node-a:51401"), 51401),
            "http://node-a:51401"
        );
    }

    #[test]
    fn non_blank_treats_empty_and_whitespace_as_absent() {
        // Compose interpolation (`${LATIQ_AUTH_ISSUER:-}`) always SETS the
        // variable, so clap hands us `Some("")`. That must mean "auth off", not
        // "an issuer named empty string" — otherwise a plain `docker compose up`
        // comes up rejecting every request.
        assert_eq!(non_blank(None), None);
        assert_eq!(non_blank(Some(String::new())), None);
        assert_eq!(non_blank(Some("   ".into())), None);
        assert_eq!(non_blank(Some("\t\n".into())), None);
        // Surrounding whitespace is stripped, not rejected.
        assert_eq!(
            non_blank(Some(" https://idp ".into())),
            Some("https://idp".to_string())
        );
        assert_eq!(
            non_blank(Some("https://idp".into())),
            Some("https://idp".to_string())
        );
    }

    #[cfg(feature = "server")]
    #[test]
    fn auth_config_is_absent_when_no_issuer_is_set() {
        // The default path: no flags, no env → no verifier anywhere.
        assert!(auth_config(vec![], None, None).unwrap().is_none());
        // An EMPTY issuer env var is the same as an unset one.
        assert!(auth_config(vec![String::new()], None, None)
            .unwrap()
            .is_none());
        assert!(auth_config(vec!["  ".into()], Some("latiq".into()), None)
            .unwrap()
            .is_none());
    }

    #[cfg(feature = "server")]
    #[test]
    fn auth_config_requires_an_audience_with_an_issuer() {
        let e = auth_config(vec!["https://idp".into()], None, None)
            .expect_err("an issuer without an audience must fail fast");
        assert!(e.to_string().contains("--auth-audience"), "got {e}");
        // A blank audience is an absent one.
        assert!(auth_config(vec!["https://idp".into()], Some("  ".into()), None).is_err());
    }

    #[cfg(feature = "server")]
    #[test]
    fn auth_config_refuses_a_jwks_uri_with_several_issuers() {
        let e = auth_config(
            vec!["https://a".into(), "https://b".into()],
            Some("latiq".into()),
            Some("https://a/jwks".into()),
        )
        .expect_err("an ambiguous jwks uri must fail fast");
        assert!(e.to_string().contains("--auth-jwks-uri"), "got {e}");
    }

    #[cfg(feature = "server")]
    #[test]
    fn auth_config_builds_issuers_in_order() {
        let cfg = auth_config(
            vec!["https://a".into(), " https://b ".into(), String::new()],
            Some(" latiq ".into()),
            None,
        )
        .unwrap()
        .expect("issuers configured");
        assert_eq!(cfg.audience, "latiq");
        let names: Vec<_> = cfg.issuers.iter().map(|i| i.issuer.clone()).collect();
        assert_eq!(
            names,
            vec!["https://a".to_string(), "https://b".to_string()]
        );
        assert!(cfg.issuers.iter().all(|i| i.jwks_uri.is_none()));

        // One issuer + an explicit JWKS uri: unambiguous, so it is attached.
        let cfg = auth_config(
            vec!["https://a".into()],
            Some("latiq".into()),
            Some("https://a/keys".into()),
        )
        .unwrap()
        .expect("issuers configured");
        assert_eq!(cfg.issuers[0].jwks_uri.as_deref(), Some("https://a/keys"));
    }

    #[test]
    fn token_flag_reads_the_env_var() {
        // Declarative check that `--token` is wired to LATIQ_TOKEN and that a
        // blank value is not turned into an empty bearer credential.
        assert_eq!(bearer_of(&None), None);
        assert_eq!(bearer_of(&Some("  ".into())), None);
        assert_eq!(bearer_of(&Some(" abc ".into())), Some("abc".to_string()));
    }
}
