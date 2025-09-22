use clap::{Parser, Subcommand};
use env_logger::Env;
use std::io::Write;

use crate::store::Store;

mod cmd;
mod common;
mod dashboard;
mod fees;
mod routes;
mod sling;
mod store;

#[derive(Parser)]
#[command(name = "lightdash")]
#[command(about = "Lightning Network channel management dashboard")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Display the main dashboard
    Dashboard {
        /// Directory for dashboard files
        directory: String,
    },
    /// Calculate and display routing information
    Routes,
    /// Execute sling jobs for rebalancing
    Sling,
    /// Execute fee adjustments
    Fees,
}

fn main() {
    init_logging();
    let cli = Cli::parse();
    let store = Store::new();

    match cli.command {
        Commands::Dashboard { directory } => {
            log::debug!("Dashboard directory: {}", directory);
            dashboard::run_dashboard(&store, directory);
        }
        Commands::Routes => {
            routes::run_routes(&store);
        }
        Commands::Sling => {
            sling::run_sling(&store);
        }
        Commands::Fees => {
            fees::run_fees(&store);
        }
    }
}

fn init_logging() {
    let mut builder = env_logger::Builder::from_env(Env::default().default_filter_or("info"));
    if let Ok(s) = std::env::var("RUST_LOG_STYLE") {
        if s == "SYSTEMD" {
            builder.format(|buf, record| {
                let level = match record.level() {
                    log::Level::Error => 3,
                    log::Level::Warn => 4,
                    log::Level::Info => 6,
                    log::Level::Debug => 7,
                    log::Level::Trace => 7,
                };
                writeln!(buf, "<{}>{}: {}", level, record.target(), record.args())
            });
        }
    }

    builder.init();
}
