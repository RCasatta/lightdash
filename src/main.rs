use clap::{Parser, Subcommand};

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
    let cli = Cli::parse();

    match cli.command {
        Commands::Dashboard { directory } => {
            println!("Dashboard directory: {}", directory);
            dashboard::run_dashboard(directory);
        }
        Commands::Routes => {
            routes::run_routes();
        }
        Commands::Sling => {
            sling::run_sling();
        }
        Commands::Fees => {
            fees::run_fees();
        }
    }
}
