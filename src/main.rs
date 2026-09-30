use clap::{Parser, Subcommand};
use env_logger::Env;
use std::io::Write;

use crate::store::Store;

mod cmd;
mod common;
mod dashboard2;
mod fees;
mod history;
mod htlc;
mod lnplus;
mod routes;
mod sling;
mod snapshot;
mod snapshot_metadata;
mod store;

#[derive(Parser)]
#[command(name = "lightdash")]
#[command(about = "Lightning Network channel management dashboard")]
struct Cli {
    /// Execute lightning-cli on a remote host through SSH
    #[arg(long, global = true, value_name = "USER@HOST")]
    ssh: Option<String>,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Generate the dashboard site from a snapshot directory
    Dashboard2 {
        /// Directory containing manifest.json and snapshot data files
        snapshot_directory: String,
        /// Directory for the generated site
        directory: String,
    },
    /// Export a versioned analytical snapshot as JSON and JSONL files
    Snapshot {
        /// Directory for snapshot files
        directory: String,
        /// Override the availdb path; remote when --ssh is used
        #[arg(long)]
        availdb: Option<String>,
        /// Override the processed history directory; remote when --ssh is used
        #[arg(long, conflicts_with = "without_history")]
        history_directory: Option<String>,
        /// Generate the snapshot without processed channel history
        #[arg(long)]
        without_history: bool,
        /// Override the processed routes cache directory; remote when --ssh is used
        #[arg(long, conflicts_with = "without_routes")]
        routes_directory: Option<String>,
        /// Generate the snapshot without cached route analysis
        #[arg(long)]
        without_routes: bool,
        /// Generate the snapshot without fetching LN+ Liquidity Pool offers
        #[arg(long)]
        without_lnplus: bool,
    },
    /// Process raw listchannels and listfunds archives into normalized history datasets
    History {
        #[command(subcommand)]
        command: HistoryCommands,
    },
    /// Maintain the cached route analysis used by snapshots
    Routes {
        #[command(subcommand)]
        command: RoutesCommands,
    },
    /// Execute sling jobs for rebalancing
    Sling,
    /// Execute fee adjustments
    Fees {
        /// Override the availdb path; remote when --ssh is used
        #[arg(long)]
        availdb: Option<String>,
    },
    /// Adjust HTLC max on channels where local balance is lower than current htlc max
    Htlc,
}

#[derive(Subcommand)]
enum HistoryCommands {
    /// Rebuild all processed history datasets from the raw archives
    Rebuild {
        /// Directory containing channels/ and funds/ raw archive directories
        #[arg(long, default_value = "/var/lib/lightdash/history/raw")]
        raw_directory: String,
        /// Directory for normalized processed history datasets
        #[arg(long, default_value = "/var/lib/lightdash/history/processed")]
        output_directory: String,
    },
    /// Stream the processed history manifest and datasets as a tar archive
    Export {
        /// Directory containing processed history datasets
        #[arg(long, default_value = "/var/lib/lightdash/history/processed")]
        directory: String,
    },
}

#[derive(Subcommand)]
enum RoutesCommands {
    /// Refresh the durable route-analysis cache
    Refresh {
        /// Directory for processed route-analysis datasets
        #[arg(long, default_value = "/var/lib/lightdash/routes/processed")]
        directory: String,
    },
    /// Stream the cached route-analysis artifact as JSON
    Export {
        /// Directory containing processed route-analysis datasets
        #[arg(long, default_value = "/var/lib/lightdash/routes/processed")]
        directory: String,
        /// Recompute the cache first when it is at least 24 hours old
        #[arg(long)]
        refresh_if_stale: bool,
    },
}

fn main() {
    init_logging();
    log::info!("Lightdash starting");
    let cli = Cli::parse();
    if let Err(e) = cmd::configure_ssh(cli.ssh) {
        error_panic!("configuring SSH command mode failed: {e}");
    }

    match cli.command {
        Commands::Dashboard2 {
            snapshot_directory,
            directory,
        } => {
            if let Err(e) = dashboard2::run_dashboard2(&snapshot_directory, &directory) {
                error_panic!("creating dashboard2 in `{directory}` failed: {e}");
            }
        }
        Commands::Snapshot {
            directory,
            availdb,
            history_directory,
            without_history,
            routes_directory,
            without_routes,
            without_lnplus,
        } => {
            let store = Store::new(availdb);
            if let Err(e) = snapshot::run_snapshot(
                &store,
                &directory,
                history_directory.as_deref(),
                without_history,
                routes_directory.as_deref(),
                without_routes,
                without_lnplus,
            ) {
                error_panic!("creating snapshot in `{directory}` failed: {e}");
            }
        }
        Commands::History { command } => match command {
            HistoryCommands::Rebuild {
                raw_directory,
                output_directory,
            } => {
                if let Err(e) = history::run_rebuild(&raw_directory, &output_directory) {
                    error_panic!("rebuilding historical datasets failed: {e}");
                }
            }
            HistoryCommands::Export { directory } => {
                if let Err(e) = history::run_export(&directory) {
                    error_panic!("exporting historical datasets failed: {e}");
                }
            }
        },
        Commands::Routes { command } => match command {
            RoutesCommands::Refresh { directory } => {
                if let Err(e) = routes::run_cache_refresh(&directory) {
                    error_panic!("refreshing cached route analysis failed: {e}");
                }
            }
            RoutesCommands::Export {
                directory,
                refresh_if_stale,
            } => {
                if let Err(e) = routes::run_export(&directory, refresh_if_stale) {
                    error_panic!("exporting cached route analysis failed: {e}");
                }
            }
        },
        Commands::Sling => {
            let store = Store::new(None);

            sling::run_sling(&store);
        }
        Commands::Fees { availdb } => {
            let store = Store::new(availdb);

            fees::run_fees(&store);
        }
        Commands::Htlc => {
            htlc::run_htlc();
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

/// Macro that logs an error and panics with the same message.
/// This is useful because error logs are more easily seen in systemd logs.
macro_rules! error_panic {
    ($($arg:tt)*) => {
        {
            let msg = format!($($arg)*);
            log::error!("{}", msg);
            panic!("{}", msg);
        }
    };
}
pub(crate) use error_panic;
