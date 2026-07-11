use anyhow::{Context, Result};
use chrono::Utc;
use rust_decimal::Decimal;
use spreadeater_core::{EventEnvelope, EventProducer};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::{self, Duration};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::auth::{ApiCredentials, RequestSigner};
use crate::books::{BookManager, BookRestClient};
use crate::config::Config;
use crate::discovery::{filter_and_reconcile, DiscoveryClient};
use crate::models::{
    CanonicalMarket, DecisionReport, LiveOrder, QuoteCandidate, QuoteLeg, QuoteStatus,
};
use crate::monitor::{emitters, BoundedEventQueue, JsonlFileWriter};
use crate::persistence::FileArchive;
use crate::reporting::shadow::{build_decision_report, log_decision_report};
use crate::strategy::{
    apply_hedgeability_gate, compute_dynamic_size, compute_hedgeability, compute_quote_set,
    compute_score_proxy, compute_viability,
};
use crate::trading::{PositionManager, RiskManager, TradingClient};

pub struct Orchestrator {
    config: Config,
    discovery: DiscoveryClient,
    book_rest: BookRestClient,
    book_manager: Arc<BookManager>,
    archive: FileArchive,
    // Stage 2: optional live trading components
    trading_client: Option<Arc<TradingClient>>,
    position_manager: Option<Arc<PositionManager>>,
    risk_manager: Option<Arc<RiskManager>>,
    event_producer: Option<Arc<dyn EventProducer>>,
    run_id: String,
    mode: String,
}

impl Orchestrator {
    /// Create orchestrator for shadow mode (no auth needed).
    pub async fn new(config: Config) -> Result<Self> {
        let discovery = DiscoveryClient::new(config.discovery.clone());
        let book_rest = BookRestClient::new(config.discovery.clob_base_url.clone());
        let book_manager = Arc::new(BookManager::new());
        let archive = FileArchive::new(&config.persistence.archive_dir).await?;
        let run_id = format!("run_{}", Utc::now().format("%Y%m%d_%H%M%S"));
        let mode = "shadow".to_string();
        let event_producer = Self::build_event_producer(&config, &run_id).await?;

        Ok(Self {
            config,
            discovery,
            book_rest,
            book_manager,
            archive,
            trading_client: None,
            position_manager: None,
            risk_manager: None,
            event_producer,
            run_id,
            mode,
        })
    }

    /// Create orchestrator with authentication for live/dry-run mode.
    pub async fn with_auth(
        config: Config,
        credentials: ApiCredentials,
        dry_run: bool,
    ) -> Result<Self> {
        let discovery = DiscoveryClient::new(config.discovery.clone());
        let book_rest = BookRestClient::new(config.discovery.clob_base_url.clone());
        let book_manager = Arc::new(BookManager::new());
        let archive = FileArchive::new(&config.persistence.archive_dir).await?;

        let signer = RequestSigner::new(credentials.clone());
        let funder = credentials
            .funder
            .as_deref()
            .unwrap_or(&credentials.address);
        let trading_client = Arc::new(TradingClient::new(
            config.discovery.clob_base_url.clone(),
            signer,
            credentials.private_key.as_deref(),
            funder,
            &credentials.api_key,
            dry_run,
        )?);

        let position_manager = Arc::new(PositionManager::new(
            config.discovery.data_api_base_url.clone(),
            funder.to_string(),
        ));

        let risk_manager = Arc::new(RiskManager::new(config.risk.clone()));
        let run_id = format!("run_{}", Utc::now().format("%Y%m%d_%H%M%S"));
        let mode = if dry_run {
            "dry-run".to_string()
        } else {
            "live".to_string()
        };
        let event_producer = Self::build_event_producer(&config, &run_id).await?;

        let mode_label = if dry_run { "dry-run" } else { "LIVE" };
        info!(mode = mode_label, run_id = %run_id, "Orchestrator initialized with auth");

        Ok(Self {
            config,
            discovery,
            book_rest,
            book_manager,
            archive,
            trading_client: Some(trading_client),
            position_manager: Some(position_manager),
            risk_manager: Some(risk_manager),
            event_producer,
            run_id,
            mode,
        })
    }

    /// Verify auth credentials work by fetching open orders.
    pub async fn auth_check(&self) -> Result<()> {
        let client = self
            .trading_client
            .as_ref()
            .context("Auth check requires trading client")?;

        info!("Running auth check: fetching open orders...");
        let orders = client.get_open_orders(None).await?;
        info!(open_orders = orders.len(), "Auth check passed");

        if let Some(pm) = &self.position_manager {
            info!("Fetching positions...");
            pm.sync_positions().await?;
            let state = pm.get_state().await;
            info!(positions = state.positions.len(), "Positions synced");
        }

        Ok(())
    }

    /// Run the full shadow-mode pipeline once.
    pub async fn run_shadow_cycle(&self) -> Result<Vec<DecisionReport>> {
        info!("=== Starting shadow discovery cycle ===");

        let markets = self
            .discovery
            .fetch_sampling_markets()
            .await
            .context("Discovery failed")?;

        let filter_result = filter_and_reconcile(markets, self.config.discovery.min_daily_reward);

        info!(
            admitted = filter_result.admitted.len(),
            rejected = filter_result.rejected.len(),
            "Discovery cycle complete"
        );

        if filter_result.admitted.is_empty() {
            warn!("No markets passed filters");
            return Ok(Vec::new());
        }

        let mut reports = Vec::new();
        for market in &filter_result.admitted {
            match self.evaluate_market(market).await {
                Ok(report) => {
                    log_decision_report(&report);
                    if let Err(e) = self.archive.save_decision_report(&report).await {
                        error!(
                            condition_id = %market.condition_id,
                            error = %e,
                            "Failed to archive decision report"
                        );
                    }
                    reports.push(report);
                }
                Err(e) => {
                    error!(
                        condition_id = %market.condition_id,
                        error = %e,
                        "Failed to evaluate market"
                    );
                }
            }
        }

        if let Err(e) = self.archive.save_session(&reports).await {
            error!(error = %e, "Failed to save session");
        }

        let would_trade = reports.iter().filter(|r| r.would_trade).count();
        info!(
            total = reports.len(),
            would_trade = would_trade,
            would_not_trade = reports.len() - would_trade,
            "=== Shadow cycle complete ==="
        );

        Ok(reports)
    }

    /// Run live cycle: shadow evaluation + position/order awareness.
    pub async fn run_live_cycle(&self) -> Result<Vec<DecisionReport>> {
        info!("=== Starting live evaluation cycle ===");
        let cycle_id = Uuid::new_v4().to_string();

        let risk = self
            .risk_manager
            .as_ref()
            .context("Live mode requires risk manager")?;
        let pm = self
            .position_manager
            .as_ref()
            .context("Live mode requires position manager")?;
        let tc = self
            .trading_client
            .as_ref()
            .context("Live mode requires trading client")?;

        // Check global halt
        if risk.is_globally_halted().await {
            warn!("Global halt active, skipping cycle");
            return Ok(Vec::new());
        }

        // Sync positions
        if let Err(e) = pm.sync_positions().await {
            error!(error = %e, "Failed to sync positions");
        }

        // Check hedge timeouts
        risk.check_hedge_timeouts().await;

        // Fetch open orders
        let open_orders = match tc.get_open_orders(None).await {
            Ok(orders) => {
                info!(count = orders.len(), "Open orders fetched");
                orders
            }
            Err(e) => {
                error!(error = %e, "Failed to fetch open orders");
                Vec::new()
            }
        };
        let order_committed = order_committed_capital(&open_orders);
        let position_committed = pm.total_position_cost().await;
        let committed_capital = order_committed + position_committed;
        let api_balance = if tc.is_dry_run() {
            None
        } else {
            match tc.get_balance().await {
                Ok(balance) => Some(balance),
                Err(err) => {
                    warn!(error = %err, "Failed to fetch API balance for monitor event");
                    None
                }
            }
        };
        let available_budget = api_balance.unwrap_or(Decimal::from(10000)) - committed_capital;

        // Discover and filter markets
        let markets = self
            .discovery
            .fetch_sampling_markets()
            .await
            .context("Discovery failed")?;

        let filter_result = filter_and_reconcile(markets, self.config.discovery.min_daily_reward);

        info!(
            admitted = filter_result.admitted.len(),
            rejected = filter_result.rejected.len(),
            "Discovery complete"
        );

        if filter_result.admitted.is_empty() {
            warn!("No markets passed filters");
            return Ok(Vec::new());
        }

        // Evaluate each market with position/risk awareness
        let mut reports = Vec::new();
        for market in &filter_result.admitted {
            if !risk.is_market_tradable(&market.condition_id).await {
                info!(
                    condition_id = %market.condition_id,
                    "Skipping halted market"
                );
                continue;
            }

            match self.evaluate_market(market).await {
                Ok(report) => {
                    let trace_ids = build_quote_trace_ids(&report.candidate_quotes);
                    if let Some(pos) = pm.get_position(&market.condition_id).await {
                        info!(
                            condition_id = %market.condition_id,
                            yes_size = %pos.yes_size,
                            no_size = %pos.no_size,
                            net_exposure = %pos.net_exposure(),
                            "Position context"
                        );
                        risk.update_market_exposure(&market.condition_id, &pos)
                            .await;
                    }

                    let market_orders: Vec<_> = open_orders
                        .iter()
                        .filter(|o| o.condition_id == market.condition_id && o.is_active())
                        .collect();
                    if !market_orders.is_empty() {
                        info!(
                            condition_id = %market.condition_id,
                            active_orders = market_orders.len(),
                            "Existing orders on this market"
                        );
                    }

                    log_decision_report(&report);
                    if let Err(e) = self.archive.save_decision_report(&report).await {
                        error!(
                            condition_id = %market.condition_id,
                            error = %e,
                            "Failed to archive report"
                        );
                    }

                    self.emit_event(emitters::build_decision_evaluated(
                        &self.run_id,
                        &cycle_id,
                        &self.mode,
                        "orchestrator",
                        market,
                        &report,
                        committed_capital,
                        Some(self.config.strategy.score_proxy.competition_multiplier),
                        api_balance,
                        Some(available_budget),
                        emitters::DecisionRankingContext::default(),
                    ));

                    for candidate in &report.candidate_quotes {
                        let trace_id = trace_ids
                            .get(&candidate.leg)
                            .map(String::as_str)
                            .unwrap_or("");
                        let event = if candidate.status == QuoteStatus::Approved {
                            emitters::build_quote_approved(
                                &self.run_id,
                                &cycle_id,
                                trace_id,
                                &self.mode,
                                "orchestrator",
                                market,
                                candidate,
                            )
                        } else {
                            emitters::build_quote_rejected(
                                &self.run_id,
                                &cycle_id,
                                trace_id,
                                &self.mode,
                                "orchestrator",
                                market,
                                candidate,
                            )
                        };
                        self.emit_event(event);
                    }
                    reports.push(report);
                }
                Err(e) => {
                    error!(
                        condition_id = %market.condition_id,
                        error = %e,
                        "Failed to evaluate market"
                    );
                }
            }
        }

        if let Err(e) = self.archive.save_session(&reports).await {
            error!(error = %e, "Failed to save session");
        }

        let would_trade = reports.iter().filter(|r| r.would_trade).count();
        let dry_label = if tc.is_dry_run() { " (dry-run)" } else { "" };
        info!(
            total = reports.len(),
            would_trade = would_trade,
            "=== Live cycle complete{} ===",
            dry_label
        );

        Ok(reports)
    }

    /// Evaluate a single market: bootstrap books, compute quotes, check hedgeability.
    async fn evaluate_market(&self, market: &CanonicalMarket) -> Result<DecisionReport> {
        let (yes_book, no_book) = self
            .book_rest
            .fetch_both_books(&market.yes_token_id, &market.no_token_id)
            .await
            .context("Book bootstrap failed")?;

        self.book_manager.insert_snapshot(yes_book.clone()).await;
        self.book_manager.insert_snapshot(no_book.clone()).await;

        // Shadow mode: no real budget tracking, use a large ceiling so scoring
        // is unconstrained (shadow evaluations never place real orders).
        let dynamic_size = compute_dynamic_size(
            &yes_book,
            &no_book,
            &market.reward_config,
            &self.config.strategy.score_proxy,
            market.reward_config.min_size,
            Decimal::from(10_000),
        );

        let mut quote_set = compute_quote_set(
            market,
            &yes_book,
            &no_book,
            &self.config.strategy,
            true,
            Some(dynamic_size),
        );

        let mut hedge_reports = Vec::new();
        for candidate in &mut quote_set.candidates {
            let report =
                compute_hedgeability(candidate, &yes_book, &no_book, &self.config.strategy);
            apply_hedgeability_gate(candidate, &report);
            hedge_reports.push(report);
        }

        let score_proxy = compute_score_proxy(
            &quote_set,
            &yes_book,
            &no_book,
            &market.reward_config,
            &self.config.strategy.score_proxy,
        );

        let (viability, is_viable) = compute_viability(
            market,
            &quote_set,
            &hedge_reports,
            &self.config.strategy,
            &score_proxy,
            dynamic_size,
        );

        let report = build_decision_report(
            market,
            &quote_set,
            &hedge_reports,
            Some(viability),
            is_viable,
            &score_proxy,
        );

        Ok(report)
    }

    /// Run shadow mode in a loop with configurable interval.
    pub async fn run_shadow_loop(&self) -> Result<()> {
        let interval = Duration::from_secs(self.config.discovery.poll_interval_secs);
        info!(
            interval_secs = interval.as_secs(),
            "Starting shadow mode loop"
        );

        loop {
            match self.run_shadow_cycle().await {
                Ok(reports) => {
                    info!(reports = reports.len(), "Cycle completed successfully");
                }
                Err(e) => {
                    error!(error = %e, "Cycle failed, will retry next interval");
                }
            }

            info!(sleep_secs = interval.as_secs(), "Sleeping until next cycle");
            time::sleep(interval).await;
        }
    }

    /// Run live mode in a loop.
    pub async fn run_live_loop(&self) -> Result<()> {
        let interval = Duration::from_secs(self.config.discovery.poll_interval_secs);
        let dry_label = self
            .trading_client
            .as_ref()
            .map(|tc| if tc.is_dry_run() { " (dry-run)" } else { "" })
            .unwrap_or("");
        info!(
            interval_secs = interval.as_secs(),
            "Starting live mode loop{}", dry_label
        );

        loop {
            match self.run_live_cycle().await {
                Ok(reports) => {
                    info!(reports = reports.len(), "Live cycle completed");
                }
                Err(e) => {
                    error!(error = %e, "Live cycle failed, will retry");
                }
            }

            if let Some(risk) = &self.risk_manager {
                risk.check_hedge_timeouts().await;
            }

            info!(sleep_secs = interval.as_secs(), "Sleeping until next cycle");
            time::sleep(interval).await;
        }
    }

    async fn build_event_producer(
        config: &Config,
        run_id: &str,
    ) -> Result<Option<Arc<dyn EventProducer>>> {
        if !config.observability.enabled {
            return Ok(None);
        }

        let writer = Arc::new(
            JsonlFileWriter::new(&config.observability.event_log_dir, run_id)
                .await
                .context("Failed to initialize observability log writer")?,
        );
        let producer: Arc<dyn EventProducer> = Arc::new(BoundedEventQueue::new(writer));
        Ok(Some(producer))
    }

    fn emit_event(&self, event: EventEnvelope) {
        let Some(producer) = &self.event_producer else {
            return;
        };

        match producer.emit(event) {
            Ok(true) => {}
            Ok(false) => warn!("Dropping monitor event: queue is full"),
            Err(err) => warn!(error = %err, "Failed to enqueue monitor event"),
        }

        if producer.is_degraded() {
            let depth = producer.queue_depth();
            let degraded = emitters::build_monitor_degraded(
                &self.run_id,
                &self.mode,
                "event_producer",
                "writer degraded",
                Some((depth.critical + depth.normal) as u64),
            );
            let _ = producer.emit(degraded);
        }
    }
}

fn build_quote_trace_ids(candidates: &[QuoteCandidate]) -> HashMap<QuoteLeg, String> {
    candidates
        .iter()
        .map(|candidate| (candidate.leg, Uuid::new_v4().to_string()))
        .collect()
}

fn order_committed_capital(open_orders: &[LiveOrder]) -> Decimal {
    open_orders
        .iter()
        .filter(|order| order.is_active() && order.side == crate::models::Side::Buy)
        .map(|order| order.price * order.remaining_size())
        .fold(Decimal::ZERO, |total, value| total + value)
}
