use clap::{Parser, Subcommand};
use env_logger::Env;
use std::io::Write;

use crate::store::Store;

mod channels;
mod cmd;
mod common;
mod dashboard;
mod fees;
mod funds;
mod htlc;
mod lnplus;
mod routes;
mod sling;
mod snapshot;
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
    /// Display the main dashboard
    Dashboard {
        /// Directory for dashboard files
        directory: String,
        /// Minimum number of channels a node must have to be included
        #[arg(long, default_value = "10")]
        min_channels: usize,
        /// Override the availdb path; remote when --ssh is used
        #[arg(long)]
        availdb: Option<String>,
        /// Base URL where funds charts are served, e.g. /auth/funds-charts
        #[arg(long)]
        funds_charts_url: Option<String>,
    },
    /// Export a versioned analytical snapshot as JSON and JSONL files
    Snapshot {
        /// Directory for snapshot files
        directory: String,
        /// Override the availdb path; remote when --ssh is used
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
    Fees {
        /// Override the availdb path; remote when --ssh is used
        #[arg(long)]
        availdb: Option<String>,
    },
    /// Display channels information
    Channels {
        /// Path to directory with channel fee history
        #[arg(long)]
        path: String,
        /// Output directory for CSV files
        #[arg(long)]
        output_dir: String,
    },
    /// Display funds information
    Funds {
        /// Path to directory with listfunds history
        #[arg(long)]
        path: String,
        /// Output directory for charts
        #[arg(long)]
        output_dir: String,
    },
    /// Adjust HTLC max on channels where local balance is lower than current htlc max
    Htlc,
    /// Fetch data from LightningNetwork.Plus API
    LnPlus {
        /// Output directory for JSON files
        #[arg(long, default_value = ".")]
        output_dir: String,
    },
}

fn main() {
    init_logging();
    let cli = Cli::parse();
    if let Err(e) = cmd::configure_ssh(cli.ssh) {
        error_panic!("configuring SSH command mode failed: {e}");
    }

    match cli.command {
        Commands::Dashboard {
            directory,
            min_channels,
            availdb,
            funds_charts_url,
        } => {
            let store = Store::new(availdb);
            log::debug!("Dashboard directory: {}", directory);
            dashboard::run_dashboard(&store, directory, min_channels, funds_charts_url);
        }
        Commands::Snapshot { directory, availdb } => {
            let store = Store::new(availdb);
            if let Err(e) = snapshot::run_snapshot(&store, &directory) {
                error_panic!("creating snapshot in `{directory}` failed: {e}");
            }
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
        Commands::Fees { availdb } => {
            let store = Store::new(availdb);

            fees::run_fees(&store);
        }
        Commands::Channels { path, output_dir } => {
            channels::run_channels(path.as_str(), output_dir.as_str());
        }
        Commands::Funds { path, output_dir } => {
            funds::run_funds(path.as_str(), output_dir.as_str());
        }
        Commands::Htlc => {
            htlc::run_htlc();
        }
        Commands::LnPlus { output_dir } => {
            lnplus::run_lnplus(&output_dir);
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
