//! latiq — single binary. Server roles (control-plane, pond-node) + admin CLI.
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "latiq", version, about = "Agent-native data pond")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the control-plane server.
    ControlPlane,
    /// Run a pond-node server.
    PondNode,
    /// Operator: node administration.
    #[command(subcommand)]
    Node(NodeCmd),
    /// Operator: policy administration.
    #[command(subcommand)]
    Policy(PolicyCmd),
    /// Operator: audit access.
    #[command(subcommand)]
    Audit(AuditCmd),
}

#[derive(Subcommand)]
enum NodeCmd { List, Describe { node_id: String } }
#[derive(Subcommand)]
enum PolicyCmd { Show, Set { key: String, value: String } }
#[derive(Subcommand)]
enum AuditCmd { Tail, Search { identity: String } }

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Command::ControlPlane => println!("control-plane: not yet implemented (M4)"),
        Command::PondNode => println!("pond-node: not yet implemented (M6)"),
        Command::Node(_) | Command::Policy(_) | Command::Audit(_) =>
            println!("admin CLI: not yet implemented (M4/M6)"),
    }
}
