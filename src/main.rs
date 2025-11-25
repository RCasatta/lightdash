use clap::{Parser, Subcommand};
use env_logger::Env;
use std::io::Write;

use crate::store::Store;

mod channels;
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
        /// Minimum number of channels a node must have to be included
        #[arg(long, default_value = "1")]
        min_channels: usize,
        /// Path to JSON file with node uptime data (format: {node_id: {avail: float}})
        #[arg(long)]
        availdb: Option<String>,
    },
    /// Generate routing analysis page
    Routes {
        /// Directory for routes output
        directory: String,
    },
    /// Execute sling jobs for rebalancing
    Sling,
    /// Execute fee adjustments
    Fees,
    /// Display channels information
    Channels {
        /// Path to directory with channel fee history
        #[arg(long)]
        path: String,
        /// Output directory for CSV files
        #[arg(long)]
        output_dir: String,
    },
}

fn main() {
    init_logging();
    let cli = Cli::parse();

    match cli.command {
        Commands::Dashboard {
            directory,
            min_channels,
            availdb,
        } => {
            let store = Store::new(availdb);
            log::debug!("Dashboard directory: {}", directory);
            dashboard::run_dashboard(&store, directory, min_channels);
        }
        Commands::Routes { directory } => {
            let store = Store::new(None);

            for i in [1000, 10_000, 100_000, 1_000_000, 10_000_000] {
                routes::run_routes(&store, &directory, i);
            }
        }
        Commands::Sling => {
            let store = Store::new(None);

            sling::run_sling(&store);
        }
        Commands::Fees => {
            let store = Store::new(None);

            fees::run_fees(&store);
        }
        Commands::Channels { path, output_dir } => {
            channels::run_channels(path.as_str(), output_dir.as_str());
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
