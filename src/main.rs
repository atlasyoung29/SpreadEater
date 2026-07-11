#![allow(dead_code)]

// Test comment for notification check
mod auth;
mod books;
mod config;
mod discovery;
mod models;
mod monitor;
mod persistence;
mod reporting;
mod runtime;
mod strategy;
mod trading;
mod watchdog;

use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing_subscriber::{fmt, EnvFilter};

use std::sync::Arc;

use crate::auth::ApiCredentials;
use crate::config::{Config, RunMode};
use crate::monitor::ErrorLogger;
use crate::persistence::FileArchive;
use crate::runtime::{print_replay_summary, LiveEngine, Orchestrator, ReplayEngine};

#[derive(Parser)]
#[command(name = "spreadeater")]
#[command(about = "Polymarket hedged liquidity rewards bot")]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Config file path (JSON)
    #[arg(long, default_value = "config.json")]
    config: String,
}

#[derive(Subcommand)]
enum Commands {
    /// Run a single shadow discovery + evaluation cycle
    Once,
    /// Run shadow mode in a continuous loop
    Run,
    /// Show default config
    ShowConfig,
    /// Verify API credentials work (dry-run auth check)
    AuthCheck,
    /// Run live dry-run: full pipeline with auth but no real orders
    DryRun,
    /// Run live dry-run in a continuous loop
    DryRunLoop,
    /// Run LIVE: real order placement, fill-triggered hedging, kill switches
    Live {
        /// Run in dry-run mode (simulated orders, no real trades)
        #[arg(long)]
        dry_run: bool,
    },
    /// Export a session file to CSV for spreadsheet analysis
    Export {
        /// Path to a session JSON file (default: latest in archive)
        #[arg(long)]
        session: Option<String>,
        /// Output CSV path (default: derived from session name)
        #[arg(long, short)]
        output: Option<String>,
    },
    /// Replay archived sessions with current parameters for sensitivity analysis
    Replay {
        /// Path to a session file or the archive directory
        #[arg(long, default_value = "./data/archive")]
        path: String,
        /// Override competition multiplier for testing
        #[arg(long)]
        competition_multiplier: Option<f64>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    // Load .env if present
    let _ = dotenv::dotenv();

    // Initialize tracing
    fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::ShowConfig => {
            let config = Config::default();
            println!("{}", serde_json::to_string_pretty(&config)?);
        }
        Commands::Once => {
            let config = load_config(&cli.config).await?;
            let orchestrator = Orchestrator::new(config).await?;
            let reports = orchestrator.run_shadow_cycle().await?;

            println!("\n--- Summary ---");
            println!("Markets evaluated: {}", reports.len());
            println!(
                "Would trade: {}",
                reports.iter().filter(|r| r.would_trade).count()
            );
            println!(
                "Would not trade: {}",
                reports.iter().filter(|r| !r.would_trade).count()
            );
        }
        Commands::Run => {
            let config = load_config(&cli.config).await?;
            let orchestrator = Orchestrator::new(config).await?;
            orchestrator.run_shadow_loop().await?;
        }
        Commands::AuthCheck => {
            let config = load_config(&cli.config).await?;
            let creds = ApiCredentials::from_env()?;
            creds.validate()?;

            let orchestrator = Orchestrator::with_auth(config, creds, true).await?;
            orchestrator.auth_check().await?;
            println!("Auth check passed.");
        }
        Commands::DryRun => {
            let mut config = load_config(&cli.config).await?;
            config.mode = RunMode::Live;
            let creds = ApiCredentials::from_env()?;
            creds.validate()?;

            let orchestrator = Orchestrator::with_auth(config, creds, true).await?;
            let reports = orchestrator.run_live_cycle().await?;

            println!("\n--- Dry Run Summary ---");
            println!("Markets evaluated: {}", reports.len());
            println!(
                "Would trade: {}",
                reports.iter().filter(|r| r.would_trade).count()
            );
        }
        Commands::DryRunLoop => {
            let mut config = load_config(&cli.config).await?;
            config.mode = RunMode::Live;
            let creds = ApiCredentials::from_env()?;
            creds.validate()?;

            let orchestrator = Orchestrator::with_auth(config, creds, true).await?;
            orchestrator.run_live_loop().await?;
        }
        Commands::Live { dry_run } => {
            let mut config = load_config(&cli.config).await?;
            config.mode = RunMode::Live;
            let creds = ApiCredentials::from_env()?;
            creds.validate()?;

            if dry_run {
                println!("*** LIVE ENGINE (dry-run) — Simulated orders, no real trades ***");
            } else {
                if creds.private_key.is_none() {
                    anyhow::bail!(
                        "POLY_PRIVATE_KEY is required for live mode. Add it to your .env file.\n\
                        Get it from MetaMask: Account Details > Show Private Key"
                    );
                }
                println!("*** LIVE MODE — Real orders will be placed ***");
            }
            println!("Press Ctrl+C to stop.\n");

            let error_logger = Arc::new(ErrorLogger::new("./data"));
            let engine =
                LiveEngine::new(config, creds, dry_run, error_logger, cli.config.clone()).await?;
            engine.run().await?;
        }
        Commands::Export { session, output } => {
            let session_path = match session {
                Some(p) => std::path::PathBuf::from(p),
                None => {
                    let archive = FileArchive::new("./data/archive").await?;
                    let sessions = archive.list_sessions().await?;
                    sessions
                        .last()
                        .cloned()
                        .ok_or_else(|| anyhow::anyhow!("No session files found in archive"))?
                }
            };

            let reports = FileArchive::load_session(&session_path).await?;

            let out_path = match output {
                Some(p) => std::path::PathBuf::from(p),
                None => session_path.with_extension("csv"),
            };

            crate::reporting::export::export_session_csv(&reports, &out_path)?;
            println!(
                "Exported {} markets to {}",
                reports.len(),
                out_path.display()
            );
        }
        Commands::Replay {
            path,
            competition_multiplier,
        } => {
            let config = load_config(&cli.config).await?;
            let mut engine = ReplayEngine::new(config.clone());

            if let Some(mult) = competition_multiplier {
                engine = engine.with_competition_multiplier(
                    rust_decimal::Decimal::from_f64_retain(mult)
                        .unwrap_or(rust_decimal::Decimal::new(15, 1)),
                );
            }

            let p = std::path::Path::new(&path);
            if p.is_file() {
                let summary = engine.replay_session(p).await?;
                print_replay_summary(&summary);
            } else {
                let archive = FileArchive::new(&path).await?;
                let summaries = engine.replay_directory(&archive).await?;
                for summary in &summaries {
                    print_replay_summary(summary);
                }
                println!("\nTotal: {} sessions replayed", summaries.len());
            }
        }
    }

    Ok(())
}

async fn load_config(path: &str) -> Result<Config> {
    match tokio::fs::read_to_string(path).await {
        Ok(contents) => {
            // Strip // line comments so config.json can be documented inline
            let stripped: String = contents
                .lines()
                .map(|line| {
                    // Find // that isn't inside a quoted string
                    let mut in_string = false;
                    let mut prev_char = ' ';
                    for (i, ch) in line.char_indices() {
                        if ch == '"' && prev_char != '\\' {
                            in_string = !in_string;
                        } else if ch == '/' && prev_char == '/' && !in_string {
                            return &line[..i - 1];
                        }
                        prev_char = ch;
                    }
                    line
                })
                .collect::<Vec<_>>()
                .join("\n");
            let config: Config = serde_json::from_str(&stripped)?;
            tracing::info!(path = %path, "Config loaded");
            Ok(config)
        }
        Err(_) => {
            tracing::warn!(
                path = %path,
                "Config file not found, using defaults"
            );
            Ok(Config::default())
        }
    }
}
