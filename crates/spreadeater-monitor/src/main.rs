use std::path::PathBuf;

use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use sqlx::postgres::PgPoolOptions;
use tracing_subscriber::{fmt, EnvFilter};

use spreadeater_monitor::api::serve_api;
use spreadeater_monitor::config::{MonitorConfig, TuiConfig};
use spreadeater_monitor::ingestor::{LogIngestor, RebuildStats};
use spreadeater_monitor::logs::BotLogTailer;
use spreadeater_monitor::projector::PostgresProjector;
use spreadeater_monitor::store::LiveBroadcaster;
use spreadeater_monitor::tui::run_tui;

#[derive(Debug, Parser)]
#[command(name = "spreadeater-monitor")]
#[command(about = "SpreadEater Monitor")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Serve(CommonArgs),
    Rebuild(CommonArgs),
    Tui(TuiArgs),
}

#[derive(Debug, Clone, Args)]
struct CommonArgs {
    #[arg(long, env = "DATABASE_URL")]
    database_url: Option<String>,
    #[arg(long, default_value = "./data/events")]
    event_log_dir: PathBuf,
    #[arg(long, default_value = "127.0.0.1:8080")]
    bind: String,
    #[arg(long, default_value = "crates/spreadeater-monitor/web/dist")]
    web_dist: PathBuf,
    #[arg(long, env = "SPREADEATER_BOT_CONFIG", default_value = "config.json")]
    bot_config_path: PathBuf,
    #[arg(
        long,
        env = "SPREADEATER_BOT_LOG",
        default_value = "./data/logs/spreadeater-bot.log"
    )]
    bot_log_path: PathBuf,
}

#[derive(Debug, Clone, Args)]
struct TuiArgs {
    #[arg(long, default_value = "http://127.0.0.1:8080")]
    api_base_url: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Serve(args) => serve(args).await?,
        Commands::Rebuild(args) => rebuild(args).await?,
        Commands::Tui(args) => run_tui(TuiConfig::resolve(args.api_base_url)).await?,
    }

    Ok(())
}

async fn serve(args: CommonArgs) -> Result<()> {
    let config = MonitorConfig::resolve(
        args.database_url,
        args.event_log_dir,
        args.bind,
        args.web_dist,
        args.bot_config_path,
        args.bot_log_path,
    )?;

    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&config.database_url)
        .await?;

    let projector = PostgresProjector::new(pool.clone());
    projector.migrate().await?;

    let broadcaster = LiveBroadcaster::new(512);
    let ingestor = LogIngestor::new(
        projector.clone(),
        config.event_log_dir.clone(),
        Some(broadcaster.clone()),
    );
    let bot_log_tailer = BotLogTailer::new(
        pool.clone(),
        config.bot_log_path.clone(),
        Some(broadcaster.clone()),
    );

    let ingestor_task = tokio::spawn(async move { ingestor.run().await });
    let bot_log_task = tokio::spawn(async move { bot_log_tailer.run().await });
    let api_task = tokio::spawn(async move { serve_api(pool, broadcaster, config).await });

    tokio::select! {
        result = ingestor_task => result??,
        result = bot_log_task => result??,
        result = api_task => result??,
    }

    Ok(())
}

async fn rebuild(args: CommonArgs) -> Result<()> {
    let config = MonitorConfig::resolve(
        args.database_url,
        args.event_log_dir,
        args.bind,
        args.web_dist,
        args.bot_config_path,
        args.bot_log_path,
    )?;

    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&config.database_url)
        .await?;

    let projector = PostgresProjector::new(pool);
    projector.migrate().await?;

    let ingestor = LogIngestor::new(projector, config.event_log_dir, None);
    let stats = ingestor.rebuild().await?;
    print_rebuild_stats(&stats);

    Ok(())
}

fn print_rebuild_stats(stats: &RebuildStats) {
    println!("events_processed: {}", stats.events_processed);
    println!("files_processed: {}", stats.files_processed);
    println!("duration_ms: {}", stats.duration_ms);
    println!(
        "last_run_id: {}",
        stats.last_run_id.as_deref().unwrap_or("n/a")
    );
}
