use clap::{Parser, Subcommand};

mod cmd;
mod common;
mod dashboard;
mod fees;
mod routes;
mod sling;

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
        #[arg(short, long)]
        directory: Option<String>,
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
            // Handle directory argument if provided
            if let Some(dir) = directory {
                println!("Dashboard directory: {}", dir);
            }
            dashboard::run_dashboard();
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
