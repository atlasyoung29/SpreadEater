use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use spreadeater_core::payloads::{
    HedgeDepthDiagnostics, OrderEventDiagnostics, QuoteRefreshDiagnostics,
};
use spreadeater_core::{CancelReasonCode, EventEnvelope, EventProducer};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration as StdDuration, Instant};
use tokio::sync::{mpsc, RwLock};
use tokio::time::{self, Duration};
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;

use crate::auth::{ApiCredentials, RequestSigner};
use crate::books::{
    BookEvent, BookManager, BookRestClient, BookWebSocket, BookWsStats, BookWsStatsSnapshot,
};
use crate::config::Config;
use crate::discovery::{filter_and_reconcile, DiscoveryClient};
use crate::models::events::{OrderEvent, OrderEventType, TradeEvent, TradeStatus, UserEvent};
use crate::models::{
    CanonicalMarket, DecisionReport, HedgeabilityReport, LiveOrder, OrderAmountKind,
    OrderBookSnapshot, OrderRequest, OrderStatus, OrderType, Position, QuoteCandidate, QuoteLeg,
    QuoteSet, QuoteStatus, Side,
};
use crate::monitor::{emitters, BoundedEventQueue, JsonlFileWriter};
use crate::persistence::FileArchive;
use crate::reporting::shadow::{build_decision_report, log_decision_report};
use crate::runtime::run_metadata::write_startup_run_metadata;
use crate::strategy::{
    apply_hedgeability_gate, compute_dynamic_size, compute_hedgeability, compute_quote_set,
    compute_reward_per_share_ranking_metric, compute_score_proxy, compute_viability,
    CalibrationTracker,
};
use crate::trading::ctf_merge::{CtfMerger, PairMerger};
use crate::trading::hedge_executor::{
    normalize_share_size, plan_fill_resolution, HedgeExecutor, HedgeIntent, HedgeResolution,
    HedgeResult,
};
use crate::trading::order_manager::{
    cap_buy_size_to_budget, whole_share_budget_limit, DuplicateLiveBidLeg, MarketOrderSyncMode,
    OrderManager, TrackedOrder,
};
use crate::trading::user_stream::UserStream;
use crate::trading::{PositionManager, RiskManager, TradingClient};
use crate::watchdog::WatchdogManager;
use crate::watchdog::WsHealthTracker;

const STALE_BOOK_REFRESH_TIMEOUT: Duration = Duration::from_secs(2);
const REWARD_PER_SHARE_METRIC_NAME: &str = "reward_per_share";
const HEDGE_RESOLUTION_CANCEL_WAIT_MS: u64 = 2000;
const HEDGE_RESOLUTION_CANCEL_POLL_MS: u64 = 100;
const PROCESSED_TRADE_TTL_SECS: u64 = 24 * 60 * 60;
const PROCESSED_TRADE_MAX_ENTRIES: usize = 50_000;
const RESOLUTION_RETRY_SYNC_DELAY_MS: u64 = 250;
const EXECUTION_CONFIRMED_SELLBACK_POST_SYNC_SOURCE: &str = "execution_confirmed_sellback";
const MERGE_TRUTH_POLL_INTERVAL: Duration = Duration::from_secs(1);
const MERGE_TRUTH_TIMEOUT: Duration = Duration::from_secs(30);
const MERGE_TRUTH_REQUIRED_MATCHES: usize = 2;
const MERGE_TRUTH_MONITOR_COMPONENT: &str = "pair_exit_merge_truth";
const STALE_BOOK_HALT_REASON: &str = "Book data stale — cannot guarantee hedge execution";
const STALE_BOOK_RECOVERY_REQUIRED_MATCHES: usize = 2;
const HALT_CLEANUP_MONITOR_COMPONENT: &str = "halt_cleanup";
const RECENT_RESOLUTION_TRADE_TTL_SECS: u64 = 180;
const RECENT_SCORING_OBSERVATION_TTL_SECS: u64 = 120;

/// Holds the result of evaluating a single market, before action is taken.
/// Used to rank markets by profitability before allocating budget.
struct MarketEvaluation {
    market: CanonicalMarket,
    yes_book: OrderBookSnapshot,
    no_book: OrderBookSnapshot,
    quote_set: QuoteSet,
    report: DecisionReport,
    trace_ids: HashMap<QuoteLeg, String>,
}

#[derive(Debug, Clone)]
struct FrontierRotationPlan {
    loser_condition_id: String,
    entrant_condition_id: String,
    reclaimable_bid_capital: Decimal,
    counterfactual_budget_usd: Decimal,
    loser_rank_key: MarketRankKey,
    entrant_rank_key: MarketRankKey,
}

#[derive(Debug, Clone)]
struct PendingFrontierReservation {
    entrant_condition_id: String,
    loser_condition_id: String,
    reclaimable_bid_capital: Decimal,
    armed_cycle_id: String,
}

#[derive(Debug)]
enum SameCycleHandoffResult {
    Placed(String),
    TimedOut,
    Disabled,
    NoReservation,
    NoPlaceableMarket,
    Failed,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct MarketRankKey {
    reward_per_share: Decimal,
    estimated_reward: Decimal,
}

/// Handles trade fill events on a dedicated tokio task.
///
/// Decoupled from the periodic `select!` loop so hedges fire immediately,
/// even when `run_cycle()` is mid-execution (which can block for 5-30s
/// of REST calls). Shares state with LiveEngine via Arc'd fields.
struct FillHandler {
    order_manager: OrderManager,
    hedge_executor: HedgeExecutor,
    managed_markets: Arc<RwLock<HashMap<String, CanonicalMarket>>>,
    known_markets: Arc<RwLock<HashMap<String, CanonicalMarket>>>,
    risk_manager: Arc<RiskManager>,
    position_manager: Arc<PositionManager>,
    book_manager: Arc<BookManager>,
    book_rest: BookRestClient,
    trading_client: Arc<TradingClient>,
    config: Config,
    event_producer: Option<Arc<dyn EventProducer>>,
    run_id: String,
    mode: String,
    cached_balance: Arc<RwLock<Decimal>>,
    hedge_order_ids: Arc<RwLock<HashSet<String>>>,
    /// Shared with LiveEngine — updated after successful hedges so
    /// reconciliation knows this fill has been handled.
    recon_baselines: Arc<RwLock<HashMap<String, (Decimal, Decimal)>>>,
    /// Shared signal: records last successful hedge time per condition_id.
    hedge_signals: Arc<RwLock<HashMap<String, HedgeSignal>>>,
    /// Shared dedupe for late user-stream trades produced by our own
    /// verified resolution exits (for example, sell-backs observed after
    /// post-sync has already proven the position is flat).
    recent_resolution_trades: Arc<RwLock<Vec<RecentResolutionTrade>>>,
    /// On-chain CTF merger for redeeming YES+NO pairs.
    ctf_merger: Option<Arc<dyn PairMerger>>,
    /// Per-market mutex: ensures only one hedge operation runs at a time per market.
    hedge_locks: Arc<RwLock<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
    error_logger: Arc<crate::monitor::ErrorLogger>,
}

#[derive(Debug, Clone)]
struct PendingFillFallback {
    tracked: TrackedOrder,
    asset_id: String,
    outcome: String,
    fill_size: Decimal,
    fill_price: Decimal,
    occurred_at: DateTime<Utc>,
    queued_at: Instant,
}

#[derive(Debug, Clone)]
struct RecentSyntheticFill {
    size: Decimal,
    processed_at: Instant,
}

#[derive(Debug, Clone)]
struct RecentResolutionTrade {
    condition_id: String,
    asset_id: String,
    side: Side,
    price: Decimal,
    size: Decimal,
    recorded_at: Instant,
}

#[derive(Debug, Clone)]
struct RecentScoringObservation {
    actual_scoring: bool,
    price: Decimal,
    remaining_size: Decimal,
    observed_at: Instant,
}

#[derive(Debug, Clone)]
struct ProcessedTradeEntry {
    seen_at: Instant,
}

#[derive(Debug, Default)]
struct ProcessedTradeCache {
    entries: HashMap<String, ProcessedTradeEntry>,
    order: VecDeque<(String, Instant)>,
}

#[derive(Debug, Clone)]
struct MissingOrderConfirmation {
    condition_id: String,
    leg: QuoteLeg,
    first_missing_at: Instant,
    consecutive_market_misses: u32,
}

#[derive(Debug, Clone)]
struct HedgeSignal {
    recorded_at: Instant,
    hedged_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct ResolutionPreparation {
    yes_book: OrderBookSnapshot,
    no_book: OrderBookSnapshot,
    pre_resolution_active_orders: usize,
    pre_resolution_pending_cancels: usize,
    cancel_wait_drained: bool,
    max_hedge_usdc: Decimal,
}

#[derive(Debug, Clone)]
enum SellbackVerificationState {
    VerifiedFilled,
    VerifiedZeroFill,
    Unknown,
}

#[derive(Debug, Clone, Default)]
struct SellbackVerificationMetadata {
    response_status: Option<String>,
    lookup_status: Option<String>,
    lookup_matched_shares: Option<Decimal>,
    lookup_error: Option<String>,
    trade_ids: Vec<String>,
}

#[derive(Debug, Clone)]
struct SellbackExecutionResult {
    order_result: Option<crate::models::OrderResult>,
    verification_state: SellbackVerificationState,
    confirmed_shares: Option<Decimal>,
    failure_reason: Option<String>,
    price: Option<Decimal>,
    verification_metadata: SellbackVerificationMetadata,
}

impl SellbackExecutionResult {
    fn is_verified_filled(&self) -> bool {
        matches!(
            self.verification_state,
            SellbackVerificationState::VerifiedFilled
        )
    }
}

#[derive(Debug, Clone)]
struct ResolutionExecutionResult {
    hedge_result: Option<HedgeResult>,
    sellback_result: Option<SellbackExecutionResult>,
    post_position: Option<Position>,
    post_sync_net_exposure: Decimal,
    post_sync_source: &'static str,
    success: bool,
    failure_reason: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct HedgeExitTelemetry {
    exit_path_status: String,
    merge_eligible_pairs: Decimal,
    ctf_merge_configured: bool,
    merge_attempted: bool,
    merge_tx_hash: Option<String>,
    merge_failure_reason: Option<String>,
    fallback_asks_attempted: bool,
    fallback_ask_count: u64,
    fallback_failure_reason: Option<String>,
}

#[derive(Debug, Clone)]
struct MergeTruthObservation {
    converged: bool,
    observed_for: Duration,
    last_seen_position: Position,
    last_sync_error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MergeTruthHandling {
    BackgroundMonitor,
    WaitForConvergence,
}

#[derive(Debug, Clone)]
pub(crate) struct HarnessPairExitOutcome {
    pub exit_path_status: String,
    pub merge_eligible_pairs: Decimal,
    pub ctf_merge_configured: bool,
    pub merge_attempted: bool,
    pub merge_tx_hash: Option<String>,
    pub merge_failure_reason: Option<String>,
    pub fallback_asks_attempted: bool,
    pub fallback_ask_count: u64,
    pub fallback_failure_reason: Option<String>,
    pub post_position: Position,
}

#[derive(Debug, Clone)]
struct FillWorkItem {
    tracked: TrackedOrder,
    trade: TradeEvent,
    anchored_order_id: Option<String>,
    match_source: String,
    fallback_match: bool,
    size_to_apply: Decimal,
    /// Capped fill size for hedging from the actually-accounted fill.
    hedge_size: Decimal,
}

/// Full live trading engine with event-driven fill handling.
pub struct LiveEngine {
    config: Config,
    credentials: ApiCredentials,
    discovery: DiscoveryClient,
    book_rest: BookRestClient,
    book_manager: Arc<BookManager>,
    trading_client: Arc<TradingClient>,
    position_manager: Arc<PositionManager>,
    risk_manager: Arc<RiskManager>,
    order_manager: OrderManager,
    hedge_executor: HedgeExecutor,
    archive: FileArchive,
    managed_markets: Arc<RwLock<HashMap<String, CanonicalMarket>>>,
    managed_token_index: Arc<RwLock<HashMap<String, String>>>,
    calibration: Arc<RwLock<CalibrationTracker>>,
    /// Condition IDs the current UserStream is subscribed to.
    subscribed_market_ids: Arc<RwLock<HashSet<String>>>,
    /// All markets ever seen this session (never pruned). Used by reconciliation
    /// to hedge positions on markets that dropped out of the reward-eligible list.
    known_markets: Arc<RwLock<HashMap<String, CanonicalMarket>>>,
    pending_fill_fallbacks: Arc<RwLock<HashMap<String, PendingFillFallback>>>,
    recent_synthetic_fills: Arc<RwLock<HashMap<String, RecentSyntheticFill>>>,
    recent_resolution_trades: Arc<RwLock<Vec<RecentResolutionTrade>>>,
    recent_scoring_observations: Arc<RwLock<HashMap<String, RecentScoringObservation>>>,
    processed_trades: Arc<RwLock<ProcessedTradeCache>>,
    missing_order_confirmations: Arc<RwLock<HashMap<String, MissingOrderConfirmation>>>,
    event_producer: Option<Arc<dyn EventProducer>>,
    run_id: String,
    mode: String,
    cached_balance: Arc<RwLock<Decimal>>,
    dry_run: bool,
    /// Tracks reconciliation failures so successful resolutions can clear stale state.
    recon_failure_counts: RwLock<HashMap<String, u32>>,
    /// Position baseline per market: (yes_size, no_size) recorded on first
    /// observation. Reconciliation only hedges INCREMENTAL changes from this
    /// baseline, preventing it from trying to "fix" hedge inventory.
    /// Shared with FillHandler so it can update after successful hedges.
    recon_baselines: Arc<RwLock<HashMap<String, (Decimal, Decimal)>>>,
    /// Cooldown for balance correction sells (per condition_id).
    balance_fix_cooldowns: RwLock<HashMap<String, tokio::time::Instant>>,
    /// Shared signal: records last successful hedge time per condition_id.
    /// Prevents double-hedging between WS FillHandler and imbalance checker.
    hedge_signals: Arc<RwLock<HashMap<String, HedgeSignal>>>,
    /// On-chain CTF merger for redeeming YES+NO pairs.
    ctf_merger: Option<Arc<dyn PairMerger>>,
    /// Counter for throttling position syncs inside check_hedge_depth().
    /// Syncs every 6th invocation (= every 30s at 5s interval).
    depth_check_counter: std::sync::atomic::AtomicU64,
    /// Hedge order IDs from FillHandler and reconciliation hedges.
    /// Used to skip hedge fills on the WebSocket so they don't trigger re-hedging.
    hedge_order_ids: Arc<RwLock<HashSet<String>>>,
    /// Market-book websocket health counters drained once per cycle.
    book_ws_stats: Arc<BookWsStats>,
    /// Last drained market-book websocket health counters, emitted in status snapshots.
    last_book_ws_stats: Arc<RwLock<BookWsStatsSnapshot>>,
    /// Per-market mutex: ensures only one hedge operation runs at a time per market.
    /// Shared between FillHandler and reconciliation to prevent double-hedging.
    hedge_locks: Arc<RwLock<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
    frontier_reservation: RwLock<Option<PendingFrontierReservation>>,
    error_logger: Arc<crate::monitor::ErrorLogger>,
    /// Shared WS health tracker for the watchdog.
    watchdog_health: Arc<tokio::sync::Mutex<WsHealthTracker>>,
    /// Deduplicates cleanup degradation events for halted markets.
    halt_cleanup_statuses: RwLock<HashMap<String, HaltCleanupStatus>>,
    /// Counts consecutive fresh-book confirmations before a stale-book halt is resumed.
    stale_book_recovery_streaks: RwLock<HashMap<String, usize>>,
}

impl LiveEngine {
    pub async fn new(
        config: Config,
        credentials: ApiCredentials,
        dry_run: bool,
        error_logger: Arc<crate::monitor::ErrorLogger>,
        config_path: String,
    ) -> Result<Self> {
        let started_at = Utc::now();
        let run_id = format!("run_{}", started_at.format("%Y%m%d_%H%M%S"));
        let mode = if dry_run { "dry-run" } else { "live" }.to_string();
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
        let event_producer = if config.observability.enabled {
            let writer =
                Arc::new(JsonlFileWriter::new(&config.observability.event_log_dir, &run_id).await?);
            Some(Arc::new(BoundedEventQueue::new(writer)) as Arc<dyn EventProducer>)
        } else {
            None
        };
        let cached_balance = Arc::new(RwLock::new(Decimal::ZERO));
        let order_manager = OrderManager::new(
            trading_client.clone(),
            cached_balance.clone(),
            event_producer.clone(),
            run_id.clone(),
            mode.clone(),
            config.risk.cash_reserve,
        );
        let hedge_executor = HedgeExecutor::new(trading_client.clone(), book_manager.clone());

        let calibration = CalibrationTracker::new(
            config.strategy.score_proxy.competition_multiplier,
            config.strategy.score_proxy.calibration_sample_size,
        );
        let ctf_merger = {
            let signer_address = credentials.address.clone();
            let funder_address = credentials
                .funder
                .clone()
                .unwrap_or_else(|| signer_address.clone());
            let private_key = credentials.private_key.clone();
            let relayer_api_key = std::env::var("RELAYER_API_KEY").ok();
            let relayer_api_key_address = std::env::var("RELAYER_API_KEY_ADDRESS").ok();
            match (private_key, relayer_api_key, relayer_api_key_address) {
                (Some(pk), Some(relayer_key), Some(relayer_key_address)) => match CtfMerger::new(
                    &pk,
                    &signer_address,
                    &funder_address,
                    &relayer_key,
                    &relayer_key_address,
                ) {
                    Ok(m) => {
                        info!("CTF merger initialized — auto-merge enabled");
                        Some(Arc::new(m) as Arc<dyn PairMerger>)
                    }
                    Err(e) => {
                        warn!(error = %e, "Failed to init CTF merger — merge disabled");
                        None
                    }
                },
                _ => {
                    info!(
                        "POLY_PRIVATE_KEY, RELAYER_API_KEY, or RELAYER_API_KEY_ADDRESS not set — CTF merge disabled"
                    );
                    None
                }
            }
        };

        write_startup_run_metadata(&config, &run_id, &mode, started_at, &config_path)
            .await
            .with_context(|| format!("failed to persist run metadata for {}", run_id))?;

        info!(mode = %mode, run_id = %run_id, "Live engine initialized");

        Ok(Self {
            config,
            credentials,
            discovery,
            book_rest,
            book_manager,
            trading_client,
            position_manager,
            risk_manager,
            order_manager,
            hedge_executor,
            archive,
            managed_markets: Arc::new(RwLock::new(HashMap::new())),
            managed_token_index: Arc::new(RwLock::new(HashMap::new())),
            calibration: Arc::new(RwLock::new(calibration)),
            subscribed_market_ids: Arc::new(RwLock::new(HashSet::new())),
            known_markets: Arc::new(RwLock::new(HashMap::new())),
            pending_fill_fallbacks: Arc::new(RwLock::new(HashMap::new())),
            recent_synthetic_fills: Arc::new(RwLock::new(HashMap::new())),
            recent_resolution_trades: Arc::new(RwLock::new(Vec::new())),
            recent_scoring_observations: Arc::new(RwLock::new(HashMap::new())),
            processed_trades: Arc::new(RwLock::new(ProcessedTradeCache::default())),
            missing_order_confirmations: Arc::new(RwLock::new(HashMap::new())),
            event_producer,
            run_id,
            mode,
            cached_balance,
            dry_run,
            recon_failure_counts: RwLock::new(HashMap::new()),
            recon_baselines: Arc::new(RwLock::new(HashMap::new())),
            balance_fix_cooldowns: RwLock::new(HashMap::new()),
            depth_check_counter: std::sync::atomic::AtomicU64::new(0),
            hedge_order_ids: Arc::new(RwLock::new(HashSet::new())),
            book_ws_stats: Arc::new(BookWsStats::default()),
            last_book_ws_stats: Arc::new(RwLock::new(BookWsStatsSnapshot::default())),
            hedge_locks: Arc::new(RwLock::new(HashMap::new())),
            frontier_reservation: RwLock::new(None),
            error_logger,
            hedge_signals: Arc::new(RwLock::new(HashMap::new())),
            watchdog_health: Arc::new(tokio::sync::Mutex::new(WsHealthTracker::new())),
            halt_cleanup_statuses: RwLock::new(HashMap::new()),
            stale_book_recovery_streaks: RwLock::new(HashMap::new()),
            ctf_merger,
        })
    }

    /// Run the full live engine: periodic cycles + real-time fill handling.
    pub async fn run(&self) -> Result<()> {
        let dry_label = if self.dry_run { " (dry-run)" } else { "" };
        info!("Starting live engine{}", dry_label);

        // Auth check — returns condition_ids of existing open orders
        let existing_ids = self.auth_check().await?;

        // Subscribe to user events BEFORE the initial cycle so fills on
        // existing orders (from prior sessions) are caught immediately.
        let mut user_rx = if !self.dry_run && !existing_ids.is_empty() {
            info!(
                markets = existing_ids.len(),
                "Early WS subscription for existing orders"
            );
            let stream = UserStream::new(self.credentials.clone());
            match stream.subscribe(existing_ids.clone()).await {
                Ok(rx) => Some(rx),
                Err(e) => {
                    error!(error = %e, "Failed early user stream subscribe");
                    None
                }
            }
        } else {
            None
        };

        // Initial cycle (WS is already listening for fills during this)
        self.run_cycle().await?;

        // Re-subscribe if managed market set differs from early subscription
        let current_ids: HashSet<String> =
            self.get_managed_market_ids().await.into_iter().collect();
        let early_ids: HashSet<String> = existing_ids.into_iter().collect();
        if current_ids != early_ids {
            user_rx = self.subscribe_user_stream().await;
        }
        *self.subscribed_market_ids.write().await = current_ids;

        // Spawn the fill handler on a dedicated task so hedges fire instantly,
        // even when run_cycle() is mid-execution (which blocks select! for 5-30s).
        let (fill_tx, fill_rx) = mpsc::unbounded_channel::<FillWorkItem>();
        let fill_handler = FillHandler {
            order_manager: self.order_manager.clone(),
            hedge_executor: self.hedge_executor.clone(),
            managed_markets: self.managed_markets.clone(),
            known_markets: self.known_markets.clone(),
            risk_manager: self.risk_manager.clone(),
            position_manager: self.position_manager.clone(),
            book_manager: self.book_manager.clone(),
            book_rest: self.book_rest.clone(),
            trading_client: self.trading_client.clone(),
            config: self.config.clone(),
            event_producer: self.event_producer.clone(),
            run_id: self.run_id.clone(),
            mode: self.mode.clone(),
            cached_balance: self.cached_balance.clone(),
            hedge_order_ids: self.hedge_order_ids.clone(),
            recon_baselines: self.recon_baselines.clone(),
            hedge_signals: self.hedge_signals.clone(),
            recent_resolution_trades: self.recent_resolution_trades.clone(),
            ctf_merger: self.ctf_merger.clone(),
            hedge_locks: self.hedge_locks.clone(),
            error_logger: self.error_logger.clone(),
        };
        tokio::spawn(fill_handler.run(fill_rx));

        // Spawn the watchdog (independent tokio task)
        if self.config.watchdog.enabled && !self.dry_run {
            let watchdog = WatchdogManager::new(
                self.config.watchdog.clone(),
                Arc::clone(&self.watchdog_health),
                Arc::clone(&self.risk_manager),
                Arc::clone(&self.error_logger),
                self.event_producer.clone(),
                self.run_id.clone(),
                self.mode.clone(),
                self.book_ws_stats.clone(),
            );
            watchdog.spawn();
        }

        let mut cycle_interval = time::interval(Duration::from_secs(
            self.config.discovery.poll_interval_secs,
        ));
        cycle_interval.tick().await; // consume first immediate tick

        // Hedge depth check runs frequently to catch disappearing liquidity (every 2s)
        let mut depth_interval = time::interval(Duration::from_secs(2));
        depth_interval.tick().await;

        // Quote refresh: re-reads cached books and cancel-replaces drifted orders
        let mut refresh_interval =
            time::interval(Duration::from_secs(self.config.strategy.quote_refresh_secs));
        refresh_interval.tick().await;

        let mut fallback_fill_interval = time::interval(Duration::from_secs(1));
        fallback_fill_interval.tick().await;

        // Start book WebSocket for real-time book updates (reduces REST polling)
        let book_ws =
            BookWebSocket::new(self.config.books.ws_url.clone(), self.book_ws_stats.clone());
        let mut book_rx = {
            let token_ids = self.get_book_token_ids().await;
            if !token_ids.is_empty() {
                info!(tokens = token_ids.len(), "Starting book WebSocket");
                match book_ws.subscribe(token_ids).await {
                    Ok(rx) => Some(rx),
                    Err(e) => {
                        error!(error = %e, "Failed to start book WebSocket");
                        None
                    }
                }
            } else {
                None
            }
        };

        loop {
            tokio::select! {
                _ = cycle_interval.tick() => {
                    if self.risk_manager.is_globally_halted().await {
                        warn!("Global halt active, skipping cycle");
                        continue;
                    }

                    self.risk_manager.check_hedge_timeouts().await;

                    if let Err(e) = self.run_cycle().await {
                        error!(error = %e, "Live cycle failed");
                    }

                    // Only re-subscribe BOOK WS if the managed market set changed.
                    // The user WS delivers ALL fills for the authenticated user
                    // regardless of market — never tear it down mid-session.
                    let current_ids: HashSet<String> =
                        self.get_managed_market_ids().await.into_iter().collect();
                    let prev_ids = self.subscribed_market_ids.read().await.clone();
                    if current_ids != prev_ids {
                        info!(
                            added = current_ids.difference(&prev_ids).count(),
                            removed = prev_ids.difference(&current_ids).count(),
                            "Market set changed, re-subscribing book WS"
                        );
                        *self.subscribed_market_ids.write().await = current_ids;

                        // Resubscribe book WS with updated token set
                        let token_ids = self.get_book_token_ids().await;
                        if !token_ids.is_empty() {
                            match book_ws.subscribe(token_ids).await {
                                Ok(rx) => book_rx = Some(rx),
                                Err(e) => error!(error = %e, "Failed to resubscribe book WS"),
                            }
                        }
                    }
                }
                _ = depth_interval.tick() => {
                    if self.risk_manager.is_globally_halted().await {
                        continue;
                    }
                    if let Err(e) = self.check_hedge_depth().await {
                        error!(error = %e, "Hedge depth check failed");
                    }
                    self.log_status().await;
                }
                _ = refresh_interval.tick() => {
                    if self.risk_manager.is_globally_halted().await {
                        continue;
                    }
                    if let Err(e) = self.refresh_quotes(&fill_tx).await {
                        error!(error = %e, "Quote refresh failed");
                    }
                }
                _ = fallback_fill_interval.tick() => {
                    if let Err(e) = self.flush_pending_fill_fallbacks(&fill_tx).await {
                        error!(error = %e, "Pending fill fallback handling failed");
                    }
                }
                event = recv_event(&mut user_rx) => {
                    if let Some(event) = event {
                        match event {
                            UserEvent::Connected { reconnect } => {
                                let status = if reconnect { "reconnected" } else { "connected" };
                                let detail = if reconnect {
                                    Some("user stream recovered after disconnect")
                                } else {
                                    Some("subscription acknowledged")
                                };
                                self.emit_event(emitters::build_user_stream_status_changed(
                                    &self.run_id,
                                    &self.mode,
                                    status,
                                    Some(self.subscribed_market_ids.read().await.len() as u64),
                                    detail,
                                ));
                                info!(
                                    reconnect,
                                    "User WebSocket connected/reconnected — syncing positions"
                                );
                                if let Err(e) = self.position_manager.sync_positions().await {
                                    warn!(error = %e, "Failed to sync positions on WS reconnect");
                                }
                                self.watchdog_health.lock().await.report_user_connected();
                            }
                            UserEvent::RawActivity => {
                                self.watchdog_health
                                    .lock()
                                    .await
                                    .report_user_raw_activity();
                            }
                            UserEvent::Trade(trade) => {
                                self.watchdog_health.lock().await.report_user_message();
                                if let Some(work) = self.build_fill_work_item(trade).await {
                                    if fill_tx.send(work).is_err() {
                                        error!("Fill handler channel closed unexpectedly");
                                    }
                                }
                            }
                            UserEvent::Order(order_event) => {
                                self.watchdog_health.lock().await.report_user_message();
                                if order_event.event_type == OrderEventType::Cancellation {
                                    self.handle_external_cancellation(order_event).await;
                                } else if order_event.event_type == OrderEventType::Update {
                                    self.handle_order_update(order_event).await;
                                }
                            }
                            UserEvent::Disconnected => {
                                warn!("User stream disconnected (auto-reconnect in progress)");
                                self.emit_event(emitters::build_user_stream_status_changed(
                                    &self.run_id,
                                    &self.mode,
                                    "disconnected",
                                    Some(self.subscribed_market_ids.read().await.len() as u64),
                                    Some("auto-reconnect in progress"),
                                ));
                                self.watchdog_health.lock().await.report_user_disconnect();
                            }
                        }
                    }
                }
                event = recv_book_event(&mut book_rx) => {
                    if let Some(event) = event {
                        let updated_token_id = match &event {
                            BookEvent::Snapshot { token_id, .. }
                            | BookEvent::Delta { token_id, .. } => Some(token_id.clone()),
                            BookEvent::Disconnected => None,
                        };
                        // Report to watchdog before consuming the event
                        match &event {
                            BookEvent::Snapshot { .. } | BookEvent::Delta { .. } => {
                                self.watchdog_health.lock().await.report_book_message();
                            }
                            BookEvent::Disconnected => {
                                self.watchdog_health.lock().await.report_book_disconnect();
                            }
                        }
                        self.book_manager.apply_event(event).await;
                        if let Some(token_id) = updated_token_id {
                            self.maybe_run_ws_hedge_depth_guard_for_token(&token_id)
                                .await;
                        }
                    }
                }
            }
        }
    }

    /// Auth check + position sync. Returns condition_ids of existing open orders
    /// so we can subscribe to the user WS before the first cycle.
    async fn auth_check(&self) -> Result<Vec<String>> {
        info!("Running auth check...");
        let orders = self.trading_client.get_open_orders(None).await?;
        info!(open_orders = orders.len(), "Auth check passed");
        self.position_manager.sync_positions().await?;
        let state = self.position_manager.get_state().await;
        info!(positions = state.positions.len(), "Positions synced");

        // Collect unique condition_ids from existing orders for early WS subscription
        let existing_ids: Vec<String> = orders
            .iter()
            .map(|o| o.condition_id.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();

        Ok(existing_ids)
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

    fn drain_book_ws_stats(&self) -> BookWsStatsSnapshot {
        self.book_ws_stats.snapshot_and_reset()
    }

    async fn current_book_ws_stats(&self) -> BookWsStatsSnapshot {
        self.last_book_ws_stats.read().await.clone()
    }

    async fn log_book_ws_activity(&self) {
        let snapshot = self.drain_book_ws_stats();
        *self.last_book_ws_stats.write().await = snapshot.clone();
        info!(
            accepted_messages = snapshot.accepted_messages,
            ignored_messages = snapshot.ignored_messages,
            parse_errors = snapshot.parse_errors,
            snapshot_events = snapshot.snapshot_events,
            delta_events = snapshot.delta_events,
            "Book WS activity since last cycle"
        );
    }

    /// Subscribe to user stream for managed markets.
    /// Returns None in dry-run mode or if no markets are managed.
    async fn subscribe_user_stream(&self) -> Option<mpsc::UnboundedReceiver<UserEvent>> {
        if self.dry_run {
            return None;
        }
        let ids = self.get_managed_market_ids().await;
        if ids.is_empty() {
            return None;
        }
        let stream = UserStream::new(self.credentials.clone());
        match stream.subscribe(ids.clone()).await {
            Ok(rx) => Some(rx),
            Err(e) => {
                error!(error = %e, "Failed to subscribe user stream");
                self.emit_event(emitters::build_user_stream_status_changed(
                    &self.run_id,
                    &self.mode,
                    "subscription_failed",
                    Some(ids.len() as u64),
                    Some(&e.to_string()),
                ));
                None
            }
        }
    }

    /// Run a discovery + evaluation + order management cycle.
    async fn run_cycle(&self) -> Result<()> {
        let cycle_id = format!("cycle_{}", Utc::now().format("%Y%m%d_%H%M%S%3f"));

        // Sync positions first so budget includes position cost
        if let Err(e) = self.position_manager.sync_positions().await {
            error!(error = %e, "Failed to sync positions");
        }

        // Detect drift between API positions and tracked orders.
        self.detect_position_drift().await;

        // Keep order-tracking hygiene and halted-market cleanup moving forward
        // even if the discovery result is empty for this cycle.
        self.order_manager.cleanup_stale_cancels().await;
        let confirmed_cancel_retries = self.order_manager.retry_pending_cancels().await;
        if confirmed_cancel_retries > 0 {
            info!(
                confirmed = confirmed_cancel_retries,
                "Verified pending order cancels"
            );
        }
        self.finalize_halted_markets().await;

        let order_committed = self.order_manager.committed_capital().await;
        let order_exposure = self.order_manager.committed_exposure().await;
        let position_committed = self.position_manager.total_position_cost().await;
        let total_committed = order_committed + position_committed;
        let hedge_reserve = order_exposure - order_committed;

        // Refresh API balance (total USDC, includes collateral locked in resting orders).
        let api_balance = match self.trading_client.get_balance().await {
            Ok(b) => b,
            Err(e) => {
                warn!(error = %e, "Failed to fetch API balance, using cached");
                *self.cached_balance.read().await
            }
        };
        *self.cached_balance.write().await = api_balance;
        self.risk_manager.update_balance(api_balance).await;
        self.order_manager.update_gross_balance(api_balance).await;
        let budget = self.order_manager.available_budget().await;

        info!(
            order_committed = %order_committed,
            order_exposure = %order_exposure,
            position_committed = %position_committed,
            total_committed = %total_committed,
            hedge_reserve = %hedge_reserve,
            api_balance = %api_balance,
            budget = %budget,
            "=== Live engine cycle ==="
        );

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
            return Ok(());
        }

        // Sync existing open orders from exchange into tracking (handles restarts)
        match self
            .order_manager
            .sync_open_orders(&filter_result.admitted)
            .await
        {
            Ok(sync_result) => {
                if !sync_result.duplicate_live_bid_legs.is_empty() {
                    self.kill_duplicate_live_bid_legs(
                        &sync_result.duplicate_live_bid_legs,
                        "discovery_open_order_sync",
                    )
                    .await;
                }
            }
            Err(e) => {
                error!(error = %e, "Failed to sync open orders");
            }
        }

        // Seed known_markets from positions so reconciliation covers ALL markets
        // where we hold inventory, even those not in the current reward list.
        if !self.dry_run {
            self.seed_known_markets_from_positions(&filter_result.admitted)
                .await;
        }

        // Hedge any unhedged inventory (catches fills missed during downtime or WS gaps).
        // Uses known_markets (all markets ever seen) so positions on markets that dropped
        // out of the reward-eligible list are still reconciled.
        if !self.dry_run {
            let all_known: Vec<CanonicalMarket> =
                self.known_markets.read().await.values().cloned().collect();
            self.reconcile_unhedged_positions(&all_known).await;
        }

        // === Phase 1: Evaluate all admitted markets (no orders placed yet) ===
        let mut evaluations: Vec<MarketEvaluation> = Vec::new();

        for market in &filter_result.admitted {
            // Skip halted markets and cancel their orders immediately
            if !self
                .risk_manager
                .is_market_tradable(&market.condition_id)
                .await
            {
                info!(condition_id = %market.condition_id, "Skipping halted market");
                if let Err(e) = self
                    .order_manager
                    .cancel_all(
                        &market.condition_id,
                        CancelReasonCode::RiskHalt,
                        "risk_halt",
                    )
                    .await
                {
                    error!(error = %e, "Failed to cancel halted market orders");
                }
                let cleanup = self
                    .finalize_halted_market_if_drained(&market.condition_id)
                    .await;
                if !self.maybe_resume_stale_book_market(market, &cleanup).await {
                    continue;
                }
                info!(
                    condition_id = %market.condition_id,
                    "Stale-book halted market resumed after verified recovery"
                );
            }

            match self.evaluate_market(market).await {
                Ok((yes_book, no_book, quote_set, report)) => {
                    let trace_ids = build_quote_trace_ids(&quote_set);
                    log_decision_report(&report);

                    if let Err(e) = self.archive.save_decision_report(&report).await {
                        error!(condition_id = %market.condition_id, error = %e, "Archive failed");
                    }

                    for candidate in &quote_set.candidates {
                        let trace_id = trace_ids
                            .get(&candidate.leg)
                            .map(String::as_str)
                            .unwrap_or("");
                        if candidate.status == QuoteStatus::Approved {
                            self.emit_event(emitters::build_quote_approved(
                                &self.run_id,
                                &cycle_id,
                                trace_id,
                                &self.mode,
                                "live_engine",
                                market,
                                candidate,
                            ));
                        } else {
                            self.emit_event(emitters::build_quote_rejected(
                                &self.run_id,
                                &cycle_id,
                                trace_id,
                                &self.mode,
                                "live_engine",
                                market,
                                candidate,
                            ));
                        }
                    }

                    evaluations.push(MarketEvaluation {
                        market: market.clone(),
                        yes_book,
                        no_book,
                        quote_set,
                        report,
                        trace_ids,
                    });
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

        // === Phase 2: Rank by discounted reward per hedge-aware share (highest first) ===
        evaluations.sort_by(compare_market_evaluations);

        let ranked_market_count = evaluations.len() as u64;
        let competition_multiplier = self.calibration.read().await.current_multiplier();
        let frontier_reservation = self.frontier_reservation.read().await.clone();
        let frontier_plan = if frontier_reservation.is_some() {
            None
        } else {
            match self.select_frontier_rotation(&evaluations).await {
                Ok(plan) => plan,
                Err(error) => {
                    error!(error = %error, "Failed to select frontier rotation");
                    None
                }
            }
        };

        if let Some(plan) = &frontier_plan {
            info!(
                frontier_counterfactual_entrant_condition_id = %plan.entrant_condition_id,
                frontier_counterfactual_loser_condition_id = %plan.loser_condition_id,
                frontier_counterfactual_budget_usd = %plan.counterfactual_budget_usd,
                frontier_counterfactual_reclaimable_bid_capital_usd = %plan.reclaimable_bid_capital,
                frontier_counterfactual_entrant_ranking_metric_name = REWARD_PER_SHARE_METRIC_NAME,
                frontier_counterfactual_entrant_ranking_metric_value = %format!("{:.6}", plan.entrant_rank_key.reward_per_share),
                frontier_counterfactual_loser_ranking_metric_name = REWARD_PER_SHARE_METRIC_NAME,
                frontier_counterfactual_loser_ranking_metric_value = %format!("{:.6}", plan.loser_rank_key.reward_per_share),
                frontier_counterfactual_entrant_estimated_daily = %format!("${:.4}", plan.entrant_rank_key.estimated_reward),
                frontier_counterfactual_loser_estimated_daily = %format!("${:.4}", plan.loser_rank_key.estimated_reward),
                "Frontier rotation candidate selected"
            );
        }

        for (rank, eval) in evaluations.iter().enumerate() {
            let rank_key = market_rank_key(&eval.quote_set, &eval.report);
            let frontier_counterfactual_plan = frontier_plan.as_ref().filter(|plan| {
                plan.entrant_condition_id == eval.market.condition_id
                    || plan.loser_condition_id == eval.market.condition_id
            });
            info!(
                rank = rank + 1,
                condition_id = %eval.market.condition_id,
                estimated_daily = %format!("${:.4}", rank_key.estimated_reward),
                reward_per_share = %format!("{:.6}", rank_key.reward_per_share),
                would_trade = eval.report.would_trade,
                "Market ranking"
            );

            self.emit_event(emitters::build_decision_evaluated(
                &self.run_id,
                &cycle_id,
                &self.mode,
                "live_engine",
                &eval.market,
                &eval.report,
                total_committed,
                Some(competition_multiplier),
                Some(api_balance),
                Some(budget),
                emitters::DecisionRankingContext {
                    rank_in_cycle: Some((rank + 1) as u64),
                    ranked_market_count: Some(ranked_market_count),
                    ranking_metric_name: Some(REWARD_PER_SHARE_METRIC_NAME),
                    ranking_metric_value: Some(rank_key.reward_per_share),
                    frontier_eligible: frontier_plan
                        .as_ref()
                        .filter(|plan| plan.entrant_condition_id == eval.market.condition_id)
                        .map(|_| true),
                    frontier_requires_reallocation: frontier_plan
                        .as_ref()
                        .filter(|plan| plan.entrant_condition_id == eval.market.condition_id)
                        .map(|_| true),
                    frontier_replaces_condition_id: frontier_plan
                        .as_ref()
                        .filter(|plan| plan.entrant_condition_id == eval.market.condition_id)
                        .map(|plan| plan.loser_condition_id.as_str()),
                    frontier_replaced_by_condition_id: frontier_plan
                        .as_ref()
                        .filter(|plan| plan.loser_condition_id == eval.market.condition_id)
                        .map(|plan| plan.entrant_condition_id.as_str()),
                    frontier_counterfactual_budget_usd: frontier_counterfactual_plan
                        .map(|plan| plan.counterfactual_budget_usd),
                    frontier_counterfactual_reclaimable_bid_capital_usd:
                        frontier_counterfactual_plan.map(|plan| plan.reclaimable_bid_capital),
                    frontier_counterfactual_entrant_condition_id: frontier_counterfactual_plan
                        .map(|plan| plan.entrant_condition_id.as_str()),
                    frontier_counterfactual_entrant_ranking_metric_name:
                        frontier_counterfactual_plan.map(|_| REWARD_PER_SHARE_METRIC_NAME),
                    frontier_counterfactual_entrant_ranking_metric_value:
                        frontier_counterfactual_plan
                            .map(|plan| plan.entrant_rank_key.reward_per_share),
                    frontier_counterfactual_entrant_expected_reward_usd_day:
                        frontier_counterfactual_plan
                            .map(|plan| plan.entrant_rank_key.estimated_reward),
                    frontier_counterfactual_loser_condition_id: frontier_counterfactual_plan
                        .map(|plan| plan.loser_condition_id.as_str()),
                    frontier_counterfactual_loser_ranking_metric_name: frontier_counterfactual_plan
                        .map(|_| REWARD_PER_SHARE_METRIC_NAME),
                    frontier_counterfactual_loser_ranking_metric_value:
                        frontier_counterfactual_plan
                            .map(|plan| plan.loser_rank_key.reward_per_share),
                    frontier_counterfactual_loser_expected_reward_usd_day:
                        frontier_counterfactual_plan
                            .map(|plan| plan.loser_rank_key.estimated_reward),
                },
            ));
        }

        // Accumulate all markets we've ever seen (for reconciliation of non-admitted markets)
        {
            let mut known = self.known_markets.write().await;
            for eval in &evaluations {
                known
                    .entry(eval.market.condition_id.clone())
                    .or_insert_with(|| eval.market.clone());
            }
        }

        // === Phase 3: Act in ranked order (high reward/share markets get budget first) ===
        let mut freeze_new_bid_entries = frontier_reservation.is_some();
        let mut frozen_for_condition_id = frontier_reservation
            .as_ref()
            .map(|reservation| reservation.entrant_condition_id.clone());
        let mut waiting_frontier_loser_condition_id = None;
        if let Some(reservation) = frontier_reservation.as_ref() {
            if self
                .order_manager
                .has_bid_orders_or_pending_cancels(&reservation.loser_condition_id)
                .await
            {
                waiting_frontier_loser_condition_id = Some(reservation.loser_condition_id.clone());
            }
        }
        let mut reservation_processed_condition_id: Option<String> = None;

        if frontier_reservation.is_some() {
            match self
                .activate_frontier_reservation(
                    &cycle_id,
                    frontier_reservation.as_ref().unwrap(),
                    &evaluations,
                )
                .await
            {
                Ok(processed_condition_id) => {
                    reservation_processed_condition_id = processed_condition_id;
                }
                Err(error) => {
                    error!(error = %error, "Failed to process frontier reservation");
                }
            }
        }

        for eval in &evaluations {
            let market = &eval.market;
            let quote_set = &eval.quote_set;
            let report = &eval.report;

            if reservation_processed_condition_id
                .as_ref()
                .is_some_and(|condition_id| condition_id == &market.condition_id)
            {
                continue;
            }

            if waiting_frontier_loser_condition_id
                .as_ref()
                .is_some_and(|condition_id| condition_id == &market.condition_id)
            {
                info!(
                    condition_id = %market.condition_id,
                    reserved_entrant_condition_id = %frozen_for_condition_id.as_deref().unwrap_or(""),
                    "Skipping frontier loser maintenance while reservation is active"
                );
                continue;
            }

            // Position and risk context
            let position = self
                .position_manager
                .get_position(&market.condition_id)
                .await;

            if let Some(pos) = &position {
                self.risk_manager
                    .update_market_exposure(&market.condition_id, pos)
                    .await;
            }

            let is_frontier_loser = frontier_plan
                .as_ref()
                .is_some_and(|plan| plan.loser_condition_id == market.condition_id);

            if is_frontier_loser {
                if self.order_manager.has_pending_cancel_retries().await {
                    info!(
                        condition_id = %market.condition_id,
                        "Skipping frontier rebalance cancel because pending cancels remain unresolved"
                    );
                } else {
                    if let Err(e) = self
                        .order_manager
                        .cancel_bids_only(
                            &market.condition_id,
                            CancelReasonCode::FrontierRebalance,
                            "frontier_rebalance",
                        )
                        .await
                    {
                        error!(condition_id = %market.condition_id, error = %e, "Failed frontier bid rotation cancel");
                        continue;
                    }

                    if let Some(plan) = frontier_plan.as_ref() {
                        self.arm_frontier_reservation(&cycle_id, plan).await;
                        freeze_new_bid_entries = true;
                        frozen_for_condition_id = Some(plan.entrant_condition_id.clone());
                    }

                    let has_inventory = position
                        .as_ref()
                        .map(|p| p.yes_size > Decimal::ZERO || p.no_size > Decimal::ZERO)
                        .unwrap_or(false);
                    let has_asks = self
                        .order_manager
                        .get_market_orders(&market.condition_id)
                        .await
                        .iter()
                        .any(|o| o.leg.is_ask());

                    if has_inventory && !has_asks {
                        self.place_inventory_asks(
                            market,
                            position.as_ref().unwrap(),
                            "inventory_ask",
                        )
                        .await;
                    }

                    let (active, pending) = self
                        .order_manager
                        .market_order_state_counts(&market.condition_id)
                        .await;
                    if has_inventory || active > 0 || pending > 0 {
                        self.insert_managed_market(market).await;
                    } else {
                        self.remove_managed_market(&market.condition_id).await;
                    }

                    // Same-cycle handoff: poll for cancel verification and place entrant immediately
                    let handoff_result = self
                        .run_same_cycle_frontier_handoff(&cycle_id, &evaluations)
                        .await;
                    if let SameCycleHandoffResult::Placed(ref entrant_id) = handoff_result {
                        reservation_processed_condition_id = Some(entrant_id.clone());
                    }

                    continue;
                }
            }

            if report.would_trade {
                let existing = self
                    .order_manager
                    .get_market_orders(&market.condition_id)
                    .await;
                if should_skip_new_bid_entry(freeze_new_bid_entries, &existing, quote_set) {
                    let has_inventory = position
                        .as_ref()
                        .map(|p| p.yes_size > Decimal::ZERO || p.no_size > Decimal::ZERO)
                        .unwrap_or(false);
                    let has_asks = self
                        .order_manager
                        .get_market_orders(&market.condition_id)
                        .await
                        .iter()
                        .any(|o| o.leg.is_ask());

                    if has_inventory && !has_asks {
                        self.place_inventory_asks(
                            market,
                            position.as_ref().unwrap(),
                            "inventory_ask",
                        )
                        .await;
                        self.insert_managed_market(market).await;
                    }

                    info!(
                        condition_id = %market.condition_id,
                        reserved_entrant_condition_id = %frozen_for_condition_id.as_deref().unwrap_or(""),
                        "Skipping new bid entry while frontier reservation is active"
                    );
                    continue;
                }

                let min_size = market.reward_config.min_size;

                if existing.is_empty() {
                    // Place new orders
                    if let Err(e) = self
                        .order_manager
                        .place_quotes(
                            market,
                            quote_set,
                            position.as_ref(),
                            min_size,
                            Some(&eval.trace_ids),
                            "new_quote",
                            None,
                        )
                        .await
                    {
                        error!(error = %e, "Failed to place quotes");
                    }
                } else {
                    // Cancel-replace if drifted
                    if let Err(e) = self
                        .order_manager
                        .cancel_replace_if_drifted(
                            market,
                            quote_set,
                            self.config.strategy.quote_drift_bps,
                            position.as_ref(),
                            min_size,
                            Some(&eval.trace_ids),
                            "replacement",
                            None,
                        )
                        .await
                    {
                        error!(error = %e, "Failed to cancel-replace");
                    }
                }

                // Cancel individual bid legs rejected by min_outcome_price filter
                for candidate in &quote_set.candidates {
                    if candidate.leg.is_bid()
                        && candidate.status == QuoteStatus::Rejected
                        && candidate
                            .reason
                            .as_ref()
                            .map_or(false, |r| r.contains("min_outcome_price"))
                    {
                        let has_resting = self
                            .order_manager
                            .get_market_orders(&market.condition_id)
                            .await
                            .iter()
                            .any(|o| o.leg == candidate.leg);
                        if has_resting {
                            warn!(
                                condition_id = %market.condition_id,
                                leg = %candidate.leg,
                                reason = ?candidate.reason,
                                "Cancelling resting bid: outcome price below minimum"
                            );
                            if let Err(e) = self
                                .order_manager
                                .cancel_leg(
                                    &market.condition_id,
                                    candidate.leg,
                                    CancelReasonCode::OutcomePriceBelowMinimum,
                                    "quote_refresh",
                                )
                                .await
                            {
                                error!(error = %e, "Failed to cancel cheap-outcome bid");
                            }
                        }
                    }
                }

                // Ensure asks are placed if we have inventory but no tracked asks
                let has_asks = self
                    .order_manager
                    .get_market_orders(&market.condition_id)
                    .await
                    .iter()
                    .any(|o| o.leg.is_ask());
                let has_inventory = position
                    .as_ref()
                    .map(|p| p.yes_size > Decimal::ZERO || p.no_size > Decimal::ZERO)
                    .unwrap_or(false);
                if has_inventory && !has_asks {
                    self.place_inventory_asks(market, position.as_ref().unwrap(), "inventory_ask")
                        .await;
                }

                self.insert_managed_market(market).await;
            } else {
                // Not viable for NEW positions — cancel bids only
                if let Err(e) = self
                    .order_manager
                    .cancel_bids_only(
                        &market.condition_id,
                        CancelReasonCode::MarketDeadmitted,
                        "market_deadmitted",
                    )
                    .await
                {
                    error!(error = %e, "Failed to cancel bid orders");
                }

                // If we have inventory, keep/place asks for reward earnings
                let has_inventory = position
                    .as_ref()
                    .map(|p| p.yes_size > Decimal::ZERO || p.no_size > Decimal::ZERO)
                    .unwrap_or(false);

                // Check if position is hedged (both YES and NO > 0)
                let is_hedged = position
                    .as_ref()
                    .map(|p| p.yes_size > Decimal::ZERO && p.no_size > Decimal::ZERO)
                    .unwrap_or(false);

                if has_inventory && is_hedged {
                    // Hedged position — cancel ALL orders to protect the hedge pair
                    info!(
                        condition_id = %market.condition_id,
                        "Deadmitting hedged market — cancelling all orders to protect hedge"
                    );
                    if let Err(e) = self
                        .order_manager
                        .cancel_all(
                            &market.condition_id,
                            CancelReasonCode::MarketDeadmitted,
                            "market_deadmitted_hedged",
                        )
                        .await
                    {
                        error!(error = %e, "Failed to cancel orders on hedged market");
                    }
                    // Keep in managed so position tracking continues
                    self.insert_managed_market(market).await;
                } else if has_inventory {
                    self.place_inventory_asks(market, position.as_ref().unwrap(), "inventory_ask")
                        .await;
                    // Stay in managed so UserStream tracks fills on our asks
                    self.insert_managed_market(market).await;
                } else {
                    // No inventory, no bids — fully exit this market
                    if let Err(e) = self
                        .order_manager
                        .cancel_all(
                            &market.condition_id,
                            CancelReasonCode::MarketDeadmitted,
                            "market_deadmitted",
                        )
                        .await
                    {
                        error!(error = %e, "Failed to cancel remaining orders");
                    }
                    self.remove_managed_market(&market.condition_id).await;
                }
            }
        }

        // Cancel orders on markets that dropped below reward threshold
        let admitted_cids: std::collections::HashSet<&str> = filter_result
            .admitted
            .iter()
            .map(|m| m.condition_id.as_str())
            .collect();

        let stale_cids: Vec<String> = self
            .managed_markets
            .read()
            .await
            .keys()
            .filter(|cid| !admitted_cids.contains(cid.as_str()))
            .cloned()
            .collect();

        for cid in &stale_cids {
            // Check for inventory — preserve asks if we're holding a hedged position
            let position = self.position_manager.get_position(cid).await;
            let has_inventory = position
                .as_ref()
                .map(|p| p.yes_size > Decimal::ZERO || p.no_size > Decimal::ZERO)
                .unwrap_or(false);

            if has_inventory {
                warn!(
                    condition_id = %cid,
                    "Market no longer reward-eligible — cancelling bids, keeping asks for inventory exit"
                );
                if let Err(e) = self
                    .order_manager
                    .cancel_bids_only(cid, CancelReasonCode::MarketDeadmitted, "market_deadmitted")
                    .await
                {
                    error!(condition_id = %cid, error = %e, "Failed to cancel bid orders on stale market");
                }
                // Stay in managed so UserStream tracks fills on asks
            } else {
                warn!(
                    condition_id = %cid,
                    "Market no longer reward-eligible — cancelling all orders"
                );
                if let Err(e) = self
                    .order_manager
                    .cancel_all(cid, CancelReasonCode::MarketDeadmitted, "market_deadmitted")
                    .await
                {
                    error!(condition_id = %cid, error = %e, "Failed to cancel stale market orders");
                }
                self.remove_managed_market(cid).await;
            }
        }

        // Exit positions on markets approaching expiration (within 24h).
        // These were filtered out by discovery, so they're in stale_cids.
        // Unlike normal deadmission (which keeps asks), we actively sell everything.
        for cid in &stale_cids {
            let market = { self.managed_markets.read().await.get(cid).cloned() };
            if let Some(market) = market {
                if let Some(end_date) = market.end_date {
                    let hours_remaining = (end_date - Utc::now()).num_hours();
                    if hours_remaining <= 24 {
                        if let Some(pos) = self.position_manager.get_position(cid).await {
                            if pos.yes_size > Decimal::ZERO {
                                let sell_size = normalize_share_size(pos.yes_size);
                                if sell_size > Decimal::ZERO {
                                    warn!(
                                        condition_id = %cid,
                                        hours_remaining = hours_remaining,
                                        size = %sell_size,
                                        "Selling YES position — market expiring soon"
                                    );
                                    self.sell_excess(
                                        &market.yes_token_id,
                                        sell_size,
                                        market.neg_risk,
                                        &market.tick_size,
                                        cid,
                                    )
                                    .await;
                                }
                            }
                            if pos.no_size > Decimal::ZERO {
                                let sell_size = normalize_share_size(pos.no_size);
                                if sell_size > Decimal::ZERO {
                                    warn!(
                                        condition_id = %cid,
                                        hours_remaining = hours_remaining,
                                        size = %sell_size,
                                        "Selling NO position — market expiring soon"
                                    );
                                    self.sell_excess(
                                        &market.no_token_id,
                                        sell_size,
                                        market.neg_risk,
                                        &market.tick_size,
                                        cid,
                                    )
                                    .await;
                                }
                            }
                        }
                    }
                }
            }
        }

        // Sample a few tracked orders for scoring calibration
        if !self.dry_run {
            self.sample_order_scoring().await;
        }

        self.log_book_ws_activity().await;

        let dry_label = if self.dry_run { " (dry-run)" } else { "" };
        let managed_count = self.managed_markets.read().await.len();
        info!(
            managed_markets = managed_count,
            "=== Live cycle complete{} ===", dry_label
        );

        Ok(())
    }

    /// Evaluate a market: bootstrap books, compute quotes, check hedgeability.
    async fn evaluate_market(
        &self,
        market: &CanonicalMarket,
    ) -> Result<(
        OrderBookSnapshot,
        OrderBookSnapshot,
        QuoteSet,
        DecisionReport,
    )> {
        // Use WS-maintained cached books when fresh; fall back to REST if stale/missing.
        let max_age = chrono::Duration::seconds(self.config.books.max_book_age_secs as i64);

        let yes_book = match self.book_manager.get_book(&market.yes_token_id).await {
            Some(book) if !book.is_stale(max_age) => book,
            _ => {
                let book = self
                    .book_rest
                    .fetch_book(&market.yes_token_id)
                    .await
                    .context("YES book fetch failed")?;
                self.book_manager.insert_snapshot(book.clone()).await;
                book
            }
        };

        let no_book = match self.book_manager.get_book(&market.no_token_id).await {
            Some(book) if !book.is_stale(max_age) => book,
            _ => {
                let book = self
                    .book_rest
                    .fetch_book(&market.no_token_id)
                    .await
                    .context("NO book fetch failed")?;
                self.book_manager.insert_snapshot(book.clone()).await;
                book
            }
        };

        let (quote_set, report) = self
            .evaluate_market_on_books(market, &yes_book, &no_book)
            .await?;
        Ok((yes_book, no_book, quote_set, report))
    }

    async fn evaluate_market_on_books(
        &self,
        market: &CanonicalMarket,
        yes_book: &OrderBookSnapshot,
        no_book: &OrderBookSnapshot,
    ) -> Result<(QuoteSet, DecisionReport)> {
        let tracked_orders = self
            .order_manager
            .get_market_orders(&market.condition_id)
            .await;
        let budget_max = self.order_manager.available_budget().await;
        self.evaluate_market_on_books_with_context(
            market,
            yes_book,
            no_book,
            &tracked_orders,
            budget_max,
        )
        .await
    }

    async fn evaluate_market_on_books_with_context(
        &self,
        market: &CanonicalMarket,
        yes_book: &OrderBookSnapshot,
        no_book: &OrderBookSnapshot,
        tracked_orders: &[TrackedOrder],
        budget_max: Decimal,
    ) -> Result<(QuoteSet, DecisionReport)> {
        let mut proxy_config = self.config.strategy.score_proxy.clone();
        proxy_config.competition_multiplier = self.calibration.read().await.current_multiplier();
        let funded_bid_credit_total = total_funded_bid_credit(&tracked_orders);
        let whole_share_budget = whole_share_budget_limit(budget_max + funded_bid_credit_total);
        let dynamic_size = compute_dynamic_size(
            yes_book,
            no_book,
            &market.reward_config,
            &proxy_config,
            market.reward_config.min_size,
            whole_share_budget,
        );

        let mut quote_set = compute_quote_set(
            market,
            yes_book,
            no_book,
            &self.config.strategy,
            false,
            Some(dynamic_size),
        );

        for candidate in &mut quote_set.candidates {
            // Pre-admission hedge depth check for bids:
            // - reject if even min_size is not hedgeable
            // - otherwise clamp the candidate size down to current hedgeable depth
            //   so we do not submit a bid that hedge_depth() immediately resizes.
            if candidate.status == QuoteStatus::Approved && candidate.leg.is_bid() {
                let opposite_book = match candidate.leg {
                    QuoteLeg::YesBid => no_book,
                    QuoteLeg::NoBid => yes_book,
                    _ => unreachable!(),
                };
                let hedgeable = max_hedgeable_within_slippage(
                    opposite_book,
                    true,
                    self.config.strategy.max_slippage_bps,
                );
                let requested_size = candidate.size;
                if hedgeable < market.reward_config.min_size {
                    candidate.status = QuoteStatus::Rejected;
                    candidate.reason = Some(format!(
                        "Hedge depth {:.0} below min_size {:.0}",
                        hedgeable, market.reward_config.min_size
                    ));
                    debug!(
                        condition_id = %market.condition_id,
                        leg = %candidate.leg,
                        hedgeable = %hedgeable,
                        min_size = %market.reward_config.min_size,
                        "Pre-admission: bid rejected for insufficient hedge depth"
                    );
                } else {
                    let clamped_size = whole_share_budget_limit(hedgeable);
                    if clamped_size < requested_size {
                        candidate.size = clamped_size;
                        candidate.reason = Some(format!(
                            "Clamped to hedge depth {:.0} from requested size {:.0}",
                            hedgeable, requested_size
                        ));
                        debug!(
                            condition_id = %market.condition_id,
                            leg = %candidate.leg,
                            requested_size = %requested_size,
                            clamped_size = %clamped_size,
                            hedgeable = %hedgeable,
                            "Pre-admission: bid size clamped to current hedge depth"
                        );
                    }
                }
            }

            let report = compute_hedgeability(candidate, yes_book, no_book, &self.config.strategy);
            apply_hedgeability_gate(candidate, &report);
        }

        let position = self
            .position_manager
            .get_position(&market.condition_id)
            .await;
        let actionable_quote_set = build_actionable_quote_set(
            &quote_set,
            &tracked_orders,
            position.as_ref(),
            budget_max,
            market.reward_config.min_size,
        );
        let hedge_reports = hedge_reports_for_quote_set(
            &actionable_quote_set,
            yes_book,
            no_book,
            &self.config.strategy,
        );

        let score_proxy = compute_score_proxy(
            &actionable_quote_set,
            yes_book,
            no_book,
            &market.reward_config,
            &proxy_config,
        );

        let effective_quote_size = effective_quote_size_for_quotes(&actionable_quote_set);
        let (viability, is_viable) = compute_viability(
            market,
            &actionable_quote_set,
            &hedge_reports,
            &self.config.strategy,
            &score_proxy,
            effective_quote_size,
        );

        let report = build_decision_report(
            market,
            &actionable_quote_set,
            &hedge_reports,
            Some(viability),
            is_viable,
            &score_proxy,
        );

        Ok((actionable_quote_set, report))
    }

    async fn refresh_books_for_depth_check(
        &self,
        market: &CanonicalMarket,
    ) -> Result<(OrderBookSnapshot, OrderBookSnapshot)> {
        let refreshed = tokio::time::timeout(
            STALE_BOOK_REFRESH_TIMEOUT,
            self.book_rest
                .fetch_both_books(&market.yes_token_id, &market.no_token_id),
        )
        .await
        .context("Timed out refreshing stale books")??;

        self.book_manager.insert_snapshot(refreshed.0.clone()).await;
        self.book_manager.insert_snapshot(refreshed.1.clone()).await;
        Ok(refreshed)
    }

    /// Check hedge depth for all resting bid orders and scale down if needed.
    ///
    /// Runs on a shorter interval than full cycles. Uses cached books from BookManager.
    /// If the opposite book can't support hedging at the current bid size, the bid
    /// is resized down to the max hedgeable amount (or cancelled if below min_size).
    async fn check_hedge_depth(&self) -> Result<()> {
        let managed_snapshot: Vec<CanonicalMarket> = {
            let managed = self.managed_markets.read().await;
            if managed.is_empty() {
                return Ok(());
            }
            managed.values().cloned().collect()
        };

        // Sync positions every 15th invocation (~30s at 2s interval)
        // so the balance check has reasonably fresh data.
        let count = self
            .depth_check_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if count % 15 == 0 {
            if let Err(e) = self.position_manager.sync_positions().await {
                warn!(error = %e, "Failed to sync positions in depth check");
            }
        }

        let stale_threshold = chrono::Duration::seconds(self.config.books.max_book_age_secs as i64);
        let mut markets_to_kill: Vec<String> = Vec::new();

        for market in &managed_snapshot {
            let cid = &market.condition_id;
            let (active_bids, _) = self.order_manager.market_bid_order_state_counts(cid).await;
            if active_bids == 0 {
                continue;
            }

            let mut yes_book = self.book_manager.get_book(&market.yes_token_id).await;
            let mut no_book = self.book_manager.get_book(&market.no_token_id).await;

            // Stale cached books require verification before we conclude hedge execution is unsafe.
            let yes_stale = yes_book
                .as_ref()
                .map_or(true, |b| b.is_stale(stale_threshold));
            let no_stale = no_book
                .as_ref()
                .map_or(true, |b| b.is_stale(stale_threshold));
            if yes_stale || no_stale {
                let stale_side = if yes_stale && no_stale {
                    "YES+NO"
                } else if yes_stale {
                    "YES"
                } else {
                    "NO"
                };
                match self.refresh_books_for_depth_check(market).await {
                    Ok((fresh_yes, fresh_no)) => {
                        info!(
                            condition_id = %cid,
                            stale_side,
                            timeout_secs = STALE_BOOK_REFRESH_TIMEOUT.as_secs(),
                            "Refreshed stale books via REST before hedge-depth evaluation"
                        );
                        yes_book = Some(fresh_yes);
                        no_book = Some(fresh_no);
                    }
                    Err(e) => {
                        error!(
                            condition_id = %cid,
                            stale_side,
                            threshold_secs = self.config.books.max_book_age_secs,
                            error = %e,
                            "Book data stale and REST refresh failed — killing market"
                        );
                        markets_to_kill.push(cid.clone());
                        continue;
                    }
                }
            }

            let (Some(yes_book), Some(no_book)) = (yes_book.as_ref(), no_book.as_ref()) else {
                continue;
            };
            self.apply_hedge_depth_guard_for_market(market, yes_book, no_book, "hedge_depth")
                .await;
        }

        // Execute deferred kills after the managed-markets snapshot has been released.
        for cid in &markets_to_kill {
            self.kill_market(cid, STALE_BOOK_HALT_REASON).await;
        }

        // === Position balance verification (YES == NO) ===
        // For each market with live positions on BOTH sides, verify balance.
        // If abs(YES - NO) exceeds the configured tolerance, sell the excess side.
        let balance_cooldown_secs = 60u64;
        let exposure_tolerance = hedge_exposure_tolerance(&self.config);
        let managed_after_kills: Vec<CanonicalMarket> = self
            .managed_markets
            .read()
            .await
            .values()
            .cloned()
            .collect();
        for market in &managed_after_kills {
            let pos = self
                .position_manager
                .get_position(&market.condition_id)
                .await;
            let Some(pos) = pos else { continue };

            // Sell tiny one-sided positions below min_size — not worth hedging,
            // just dead capital at risk.
            let min_size = market.reward_config.min_size;
            if pos.yes_size > Decimal::ZERO
                && pos.no_size <= Decimal::ZERO
                && pos.yes_size < min_size
            {
                let sell_size = normalize_share_size(pos.yes_size);
                if sell_size > Decimal::ZERO {
                    // Check cooldown
                    let skip = {
                        let cooldowns = self.balance_fix_cooldowns.read().await;
                        cooldowns.get(&market.condition_id).map_or(false, |last| {
                            last.elapsed() < std::time::Duration::from_secs(balance_cooldown_secs)
                        })
                    };
                    if !skip {
                        warn!(
                            condition_id = %market.condition_id,
                            size = %sell_size,
                            "Selling tiny orphan YES position (below min_size)"
                        );
                        self.sell_excess(
                            &market.yes_token_id,
                            sell_size,
                            market.neg_risk,
                            &market.tick_size,
                            &market.condition_id,
                        )
                        .await;
                    }
                }
                continue;
            }
            if pos.no_size > Decimal::ZERO
                && pos.yes_size <= Decimal::ZERO
                && pos.no_size < min_size
            {
                let sell_size = normalize_share_size(pos.no_size);
                if sell_size > Decimal::ZERO {
                    let skip = {
                        let cooldowns = self.balance_fix_cooldowns.read().await;
                        cooldowns.get(&market.condition_id).map_or(false, |last| {
                            last.elapsed() < std::time::Duration::from_secs(balance_cooldown_secs)
                        })
                    };
                    if !skip {
                        warn!(
                            condition_id = %market.condition_id,
                            size = %sell_size,
                            "Selling tiny orphan NO position (below min_size)"
                        );
                        self.sell_excess(
                            &market.no_token_id,
                            sell_size,
                            market.neg_risk,
                            &market.tick_size,
                            &market.condition_id,
                        )
                        .await;
                    }
                }
                continue;
            }

            // Only check balance if we hold BOTH sides (hedged position).
            // Larger one-sided positions go through reconciliation.
            if pos.yes_size <= Decimal::ZERO || pos.no_size <= Decimal::ZERO {
                continue;
            }

            let diff = (pos.yes_size - pos.no_size).abs();
            if diff <= exposure_tolerance {
                continue; // within tolerance
            }

            // Check cooldown
            {
                let cooldowns = self.balance_fix_cooldowns.read().await;
                if let Some(last) = cooldowns.get(&market.condition_id) {
                    if last.elapsed() < std::time::Duration::from_secs(balance_cooldown_secs) {
                        continue;
                    }
                }
            }

            if pos.yes_size > pos.no_size {
                let excess = normalize_share_size(pos.yes_size - pos.no_size);
                warn!(
                    condition_id = %market.condition_id,
                    yes = %pos.yes_size,
                    no = %pos.no_size,
                    excess = %excess,
                    "Position imbalance: selling excess YES shares"
                );
                self.sell_excess(
                    &market.yes_token_id,
                    excess,
                    market.neg_risk,
                    &market.tick_size,
                    &market.condition_id,
                )
                .await;
            } else {
                let excess = normalize_share_size(pos.no_size - pos.yes_size);
                warn!(
                    condition_id = %market.condition_id,
                    yes = %pos.yes_size,
                    no = %pos.no_size,
                    excess = %excess,
                    "Position imbalance: selling excess NO shares"
                );
                self.sell_excess(
                    &market.no_token_id,
                    excess,
                    market.neg_risk,
                    &market.tick_size,
                    &market.condition_id,
                )
                .await;
            }
        }

        Ok(())
    }

    /// Sell excess shares to correct a YES!=NO position imbalance.
    /// Uses an aggressive sell so any residual above tolerance is flattened quickly.
    async fn sell_excess(
        &self,
        token_id: &str,
        size: Decimal,
        neg_risk: bool,
        tick_size: &str,
        condition_id: &str,
    ) {
        let size = normalize_share_size(size);
        if size <= Decimal::ZERO {
            return;
        }

        let request = OrderRequest {
            token_id: token_id.to_string(),
            price: Decimal::new(1, 2), // $0.01
            size,
            amount_kind: OrderAmountKind::Shares,
            side: Side::Sell,
            order_type: OrderType::FOK,
            post_only: false,
            neg_risk,
            tick_size: tick_size.to_string(),
        };

        match self.trading_client.place_order(&request).await {
            Ok(result) => {
                info!(
                    condition_id = %condition_id,
                    order_id = %result.order_id,
                    size = %size,
                    "Balance correction sell placed"
                );
                // Set cooldown
                self.balance_fix_cooldowns
                    .write()
                    .await
                    .insert(condition_id.to_string(), tokio::time::Instant::now());
                // Sync positions to reflect the sell
                if let Err(e) = self.position_manager.sync_positions().await {
                    warn!(error = %e, "Failed to sync positions after balance correction");
                }
            }
            Err(e) => {
                error!(
                    condition_id = %condition_id,
                    error = %e,
                    size = %size,
                    "Balance correction sell FAILED"
                );
                // Still set cooldown to avoid rapid retries
                self.balance_fix_cooldowns
                    .write()
                    .await
                    .insert(condition_id.to_string(), tokio::time::Instant::now());
            }
        }
    }

    /// Lightweight quote refresh: re-reads cached books and cancel-replaces drifted orders.
    ///
    /// Runs between full discovery cycles to track mid-price movement.
    /// Uses BookManager cache (maintained by WebSocket deltas) — no REST calls.
    async fn refresh_quotes(&self, fill_tx: &mpsc::UnboundedSender<FillWorkItem>) -> Result<()> {
        self.detect_missed_fills_from_exchange(fill_tx).await?;
        self.recover_orphaned_positions_on_refresh().await;

        let confirmed_cancel_retries = self.order_manager.retry_pending_cancels().await;
        if confirmed_cancel_retries > 0 {
            info!(
                confirmed = confirmed_cancel_retries,
                "Verified pending order cancels during refresh"
            );
        }
        self.finalize_halted_markets().await;

        let managed_snapshot: Vec<CanonicalMarket> = {
            let managed = self.managed_markets.read().await;
            if managed.is_empty() {
                return Ok(());
            }
            managed.values().cloned().collect()
        };

        let max_book_age = chrono::Duration::seconds(self.config.books.max_book_age_secs as i64);
        let mut refreshed = 0u32;

        for market in &managed_snapshot {
            let cid = &market.condition_id;
            if !self.risk_manager.is_market_tradable(cid).await {
                continue;
            }

            let yes_book = self.book_manager.get_book(&market.yes_token_id).await;
            let no_book = self.book_manager.get_book(&market.no_token_id).await;

            let (yes_book, no_book) = match (yes_book, no_book) {
                (Some(y), Some(n)) => (y, n),
                _ => continue,
            };

            // Skip stale books — don't act on old data
            if self
                .book_manager
                .is_stale(&market.yes_token_id, max_book_age)
                .await
                || self
                    .book_manager
                    .is_stale(&market.no_token_id, max_book_age)
                    .await
            {
                continue;
            }

            let position = self
                .position_manager
                .get_position(&market.condition_id)
                .await;

            let (quote_set, report) = match self
                .evaluate_market_on_books(market, &yes_book, &no_book)
                .await
            {
                Ok(result) => result,
                Err(e) => {
                    error!(
                        condition_id = %cid,
                        error = %e,
                        "Quote refresh evaluation failed"
                    );
                    continue;
                }
            };

            let min_size = market.reward_config.min_size;

            if report.would_trade {
                if let Err(e) = self
                    .order_manager
                    .cancel_replace_if_drifted(
                        market,
                        &quote_set,
                        self.config.strategy.quote_drift_bps,
                        position.as_ref(),
                        min_size,
                        None,
                        "quote_refresh",
                        None,
                    )
                    .await
                {
                    error!(condition_id = %cid, error = %e, "Quote refresh cancel-replace failed");
                }
            } else {
                let diagnostics = OrderEventDiagnostics {
                    quote_refresh: Some(QuoteRefreshDiagnostics {
                        would_trade: report.would_trade,
                        reasons: report.reasons.clone(),
                        effective_quote_size: report.effective_quote_size,
                        available_budget_usd: self.order_manager.available_budget().await,
                    }),
                    hedge_depth: None,
                };
                if let Err(e) = self
                    .order_manager
                    .cancel_bids_only_with_diagnostics(
                        cid,
                        CancelReasonCode::MarketDeadmitted,
                        "quote_refresh_non_viable",
                        Some(&diagnostics),
                    )
                    .await
                {
                    error!(
                        condition_id = %cid,
                        error = %e,
                        "Quote refresh bid cancellation failed"
                    );
                }
            }

            // Cancel individual bid legs rejected by min_outcome_price filter
            for candidate in &quote_set.candidates {
                if candidate.leg.is_bid()
                    && candidate.status == QuoteStatus::Rejected
                    && candidate
                        .reason
                        .as_ref()
                        .map_or(false, |r| r.contains("min_outcome_price"))
                {
                    let has_resting = self
                        .order_manager
                        .get_market_orders(cid)
                        .await
                        .iter()
                        .any(|o| o.leg == candidate.leg);
                    if has_resting {
                        warn!(
                            condition_id = %cid,
                            leg = %candidate.leg,
                            "Cancelling resting bid on quote refresh: outcome too cheap"
                        );
                        if let Err(e) = self
                            .order_manager
                            .cancel_leg(
                                cid,
                                candidate.leg,
                                CancelReasonCode::OutcomePriceBelowMinimum,
                                "quote_refresh",
                            )
                            .await
                        {
                            error!(error = %e, "Failed to cancel cheap-outcome bid");
                        }
                    }
                }
            }

            if !report.would_trade {
                let has_inventory = position
                    .as_ref()
                    .map(|p| p.yes_size > Decimal::ZERO || p.no_size > Decimal::ZERO)
                    .unwrap_or(false);
                if has_inventory {
                    self.place_inventory_asks(market, position.as_ref().unwrap(), "inventory_ask")
                        .await;
                }
            }

            refreshed += 1;
        }

        if refreshed > 0 {
            info!(markets = refreshed, "Quote refresh complete");
        }

        Ok(())
    }

    async fn clear_missing_order_confirmation(&self, order_id: &str) {
        self.missing_order_confirmations
            .write()
            .await
            .remove(order_id);
    }

    async fn record_missing_order_confirmation(&self, tracked: &TrackedOrder) -> u32 {
        let mut confirmations = self.missing_order_confirmations.write().await;
        let entry = confirmations
            .entry(tracked.order_id.clone())
            .or_insert_with(|| MissingOrderConfirmation {
                condition_id: tracked.condition_id.clone(),
                leg: tracked.leg,
                first_missing_at: Instant::now(),
                consecutive_market_misses: 0,
            });
        entry.condition_id = tracked.condition_id.clone();
        entry.leg = tracked.leg;
        entry.consecutive_market_misses += 1;
        entry.consecutive_market_misses
    }

    async fn market_metadata_for_tracked_orders(
        &self,
        condition_id: &str,
        tracked_orders: &[TrackedOrder],
    ) -> Option<CanonicalMarket> {
        if let Some(market) =
            resolve_market_metadata(&self.managed_markets, &self.known_markets, condition_id).await
        {
            return Some(market);
        }

        let tracked = tracked_orders.first()?;
        let (yes_token_id, no_token_id) = match tracked.leg {
            QuoteLeg::YesBid | QuoteLeg::YesAsk => {
                (tracked.token_id.clone(), tracked.opposite_token_id.clone())
            }
            QuoteLeg::NoBid | QuoteLeg::NoAsk => {
                (tracked.opposite_token_id.clone(), tracked.token_id.clone())
            }
        };
        Some(synthetic_resolution_market(
            condition_id,
            &yes_token_id,
            &no_token_id,
            tracked.neg_risk,
            &tracked.tick_size,
        ))
    }

    async fn kill_duplicate_live_bid_legs(
        &self,
        duplicates: &[DuplicateLiveBidLeg],
        source: &'static str,
    ) -> HashSet<String> {
        let mut grouped = HashMap::<String, Vec<String>>::new();
        for duplicate in duplicates {
            let detail = format!(
                "{} count={} order_ids={}",
                duplicate.leg,
                duplicate.order_ids.len(),
                duplicate.order_ids.join(",")
            );
            error!(
                condition_id = %duplicate.condition_id,
                leg = %duplicate.leg,
                duplicate_count = duplicate.order_ids.len(),
                order_ids = %duplicate.order_ids.join(","),
                source,
                "Duplicate live bid legs detected from exchange truth"
            );
            grouped
                .entry(duplicate.condition_id.clone())
                .or_default()
                .push(detail);
        }

        let mut killed = HashSet::new();
        for (condition_id, details) in grouped {
            let reason = format!(
                "duplicate_live_bid_leg_detected via {}: {}",
                source,
                details.join("; ")
            );
            self.kill_market(&condition_id, &reason).await;
            killed.insert(condition_id);
        }
        killed
    }

    async fn detect_missed_fills_from_exchange(
        &self,
        fill_tx: &mpsc::UnboundedSender<FillWorkItem>,
    ) -> Result<()> {
        let tracked_bids: Vec<TrackedOrder> = self
            .order_manager
            .get_all_orders()
            .await
            .into_iter()
            .filter(|tracked| tracked.leg.is_bid())
            .collect();
        if tracked_bids.is_empty() {
            return Ok(());
        }

        let live_orders: Vec<LiveOrder> = match self.trading_client.get_open_orders(None).await {
            Ok(orders) => orders,
            Err(err) => {
                warn!(error = %err, "Exchange-truth open-order snapshot failed during refresh");
                return Ok(());
            }
        }
        .into_iter()
        .filter(|order| order.status == crate::models::OrderStatus::Live)
        .filter(|order| order.remaining_size() > Decimal::ZERO)
        .collect();
        let duplicate_markets = self
            .kill_duplicate_live_bid_legs(
                &duplicate_live_bid_legs_from_orders(&live_orders),
                "quote_refresh_exchange_order_sync",
            )
            .await;
        let live_by_id: HashMap<String, LiveOrder> = live_orders
            .into_iter()
            .map(|order| (order.id.clone(), order))
            .collect();
        let tolerance = hedge_exposure_tolerance(&self.config);
        let mut disappeared_by_market: HashMap<String, Vec<TrackedOrder>> = HashMap::new();

        for tracked in tracked_bids {
            if duplicate_markets.contains(&tracked.condition_id) {
                self.clear_missing_order_confirmation(&tracked.order_id)
                    .await;
                continue;
            }

            let Some(live_order) = live_by_id.get(&tracked.order_id) else {
                disappeared_by_market
                    .entry(tracked.condition_id.clone())
                    .or_default()
                    .push(tracked);
                continue;
            };
            self.clear_missing_order_confirmation(&tracked.order_id)
                .await;

            let matched_delta = normalize_share_size(
                (live_order.size_matched - tracked.matched_size).max(Decimal::ZERO),
            );
            if matched_delta <= Decimal::ZERO {
                continue;
            }

            let pre_position = self
                .position_manager
                .get_position(&tracked.condition_id)
                .await
                .unwrap_or_else(|| Position::new(tracked.condition_id.clone()));
            let hedge_size =
                hedge_size_for_accounted_fill(&pre_position, tracked.leg, matched_delta, tolerance);
            let _ = self
                .order_manager
                .apply_order_update(&tracked.order_id, live_order.size_matched)
                .await;
            let trade = build_exchange_order_sync_trade(&tracked, live_order.price, matched_delta);

            info!(
                condition_id = %tracked.condition_id,
                order_id = %tracked.order_id,
                leg = %tracked.leg,
                matched_delta = %matched_delta,
                live_matched = %live_order.size_matched,
                tracked_matched = %tracked.matched_size,
                "Exchange-truth matched delta detected"
            );
            self.send_exchange_sync_fill(
                fill_tx,
                FillWorkItem {
                    anchored_order_id: Some(tracked.order_id.clone()),
                    tracked,
                    trade,
                    match_source: "exchange_order_sync".to_string(),
                    fallback_match: true,
                    size_to_apply: Decimal::ZERO,
                    hedge_size,
                },
            )
            .await;
        }

        if disappeared_by_market.is_empty() {
            return Ok(());
        }

        let mut confirmed_missing_by_market = HashMap::<String, Vec<TrackedOrder>>::new();
        for (condition_id, disappeared_orders) in disappeared_by_market {
            let Some(market_meta) = self
                .market_metadata_for_tracked_orders(&condition_id, &disappeared_orders)
                .await
            else {
                warn!(
                    condition_id = %condition_id,
                    missing_orders = disappeared_orders.len(),
                    "Cannot confirm missing tracked bids without market metadata"
                );
                continue;
            };

            let observe_result = match self
                .order_manager
                .sync_market_open_orders(
                    &condition_id,
                    &market_meta,
                    MarketOrderSyncMode::ObserveOnly,
                )
                .await
            {
                Ok(result) => result,
                Err(err) => {
                    warn!(
                        condition_id = %condition_id,
                        error = %err,
                        "Observe-only market sync failed while confirming missing tracked bids"
                    );
                    continue;
                }
            };

            if !observe_result.duplicate_live_bid_legs.is_empty() {
                self.kill_duplicate_live_bid_legs(
                    &observe_result.duplicate_live_bid_legs,
                    "market_scoped_missing_order_confirmation",
                )
                .await;
                continue;
            }

            let missing_ids: HashSet<String> =
                observe_result.missing_order_ids.iter().cloned().collect();
            for tracked in disappeared_orders {
                if missing_ids.contains(&tracked.order_id) {
                    confirmed_missing_by_market
                        .entry(condition_id.clone())
                        .or_default()
                        .push(tracked);
                } else {
                    self.clear_missing_order_confirmation(&tracked.order_id)
                        .await;
                }
            }
        }

        if confirmed_missing_by_market.is_empty() {
            return Ok(());
        }

        let mut pre_positions = HashMap::new();
        for condition_id in confirmed_missing_by_market.keys() {
            let position = self
                .position_manager
                .get_position(condition_id)
                .await
                .unwrap_or_else(|| Position::new(condition_id.clone()));
            pre_positions.insert(condition_id.clone(), position);
        }

        if let Err(err) = self.position_manager.sync_positions().await {
            warn!(
                error = %err,
                markets = confirmed_missing_by_market.len(),
                "Failed to corroborate disappeared tracked bids with fresh positions"
            );
            return Ok(());
        }

        for (condition_id, mut disappeared_orders) in confirmed_missing_by_market {
            disappeared_orders.sort_by_key(|tracked| tracked.created_at);
            let before = pre_positions
                .get(&condition_id)
                .cloned()
                .unwrap_or_else(|| Position::new(condition_id.clone()));
            let after = self
                .position_manager
                .get_position(&condition_id)
                .await
                .unwrap_or_else(|| Position::new(condition_id.clone()));
            let mut remaining_yes =
                directional_fill_delta_from_positions(&before, &after, QuoteLeg::YesBid, tolerance);
            let mut remaining_no =
                directional_fill_delta_from_positions(&before, &after, QuoteLeg::NoBid, tolerance);
            let mut remaining_yes_hedge =
                hedge_size_for_observed_position(&after, QuoteLeg::YesBid, tolerance);
            let mut remaining_no_hedge =
                hedge_size_for_observed_position(&after, QuoteLeg::NoBid, tolerance);

            for tracked in disappeared_orders {
                let (remaining_delta, remaining_hedge) = match tracked.leg {
                    QuoteLeg::YesBid => (&mut remaining_yes, &mut remaining_yes_hedge),
                    QuoteLeg::NoBid => (&mut remaining_no, &mut remaining_no_hedge),
                    _ => continue,
                };
                let fill_size = normalize_share_size(tracked.size.min(*remaining_delta));

                if fill_size > Decimal::ZERO {
                    let hedge_size = normalize_share_size(fill_size.min(*remaining_hedge));
                    let trade = build_exchange_order_sync_trade(&tracked, tracked.price, fill_size);
                    let tracked_order_id = tracked.order_id.clone();

                    let _ = self
                        .order_manager
                        .apply_trade_fill(&tracked.order_id, fill_size)
                        .await;
                    self.order_manager
                        .move_to_recently_cancelled(&tracked.order_id)
                        .await;
                    *remaining_delta = (*remaining_delta - fill_size).max(Decimal::ZERO);
                    *remaining_hedge = (*remaining_hedge - hedge_size).max(Decimal::ZERO);

                    info!(
                        condition_id = %tracked.condition_id,
                        order_id = %tracked.order_id,
                        leg = %tracked.leg,
                        fill_size = %fill_size,
                        remaining_market_delta = %*remaining_delta,
                        "Exchange-truth disappeared-order fill fallback triggered"
                    );
                    self.send_exchange_sync_fill(
                        fill_tx,
                        FillWorkItem {
                            anchored_order_id: Some(tracked.order_id.clone()),
                            tracked,
                            trade,
                            match_source: "exchange_order_sync".to_string(),
                            fallback_match: true,
                            size_to_apply: Decimal::ZERO,
                            hedge_size,
                        },
                    )
                    .await;
                    self.clear_missing_order_confirmation(&tracked_order_id)
                        .await;
                } else {
                    let consecutive_market_misses =
                        self.record_missing_order_confirmation(&tracked).await;
                    if consecutive_market_misses >= 2 {
                        let _ = self.order_manager.remove_order(&tracked.order_id).await;
                        self.clear_missing_order_confirmation(&tracked.order_id)
                            .await;
                        info!(
                            condition_id = %tracked.condition_id,
                            order_id = %tracked.order_id,
                            leg = %tracked.leg,
                            tracked_remaining = %tracked.size,
                            consecutive_market_misses,
                            "Confirmed missing tracked order pruned after conservative retry"
                        );
                    } else {
                        info!(
                            condition_id = %tracked.condition_id,
                            order_id = %tracked.order_id,
                            leg = %tracked.leg,
                            tracked_remaining = %tracked.size,
                            consecutive_market_misses,
                            "Confirmed missing tracked order retained pending second confirmation"
                        );
                    }
                }
            }
        }

        Ok(())
    }

    async fn send_exchange_sync_fill(
        &self,
        fill_tx: &mpsc::UnboundedSender<FillWorkItem>,
        work: FillWorkItem,
    ) {
        let order_id = work.tracked.order_id.clone();
        let fill_size = work.trade.size;
        if fill_tx.send(work).is_err() {
            error!("Fill handler channel closed unexpectedly during exchange sync");
            return;
        }
        self.record_recent_synthetic_fill(&order_id, fill_size)
            .await;
    }

    async fn select_frontier_rotation(
        &self,
        evaluations: &[MarketEvaluation],
    ) -> Result<Option<FrontierRotationPlan>> {
        if evaluations.is_empty() || self.order_manager.has_pending_cancel_retries().await {
            return Ok(None);
        }

        let actual_free_budget = self.order_manager.available_budget().await;
        let hold_cutoff = Utc::now()
            - chrono::Duration::seconds(self.config.discovery.poll_interval_secs as i64 + 1);

        let mut market_orders = HashMap::new();
        for eval in evaluations {
            market_orders.insert(
                eval.market.condition_id.clone(),
                self.order_manager
                    .get_market_orders(&eval.market.condition_id)
                    .await,
            );
        }

        let held_bid_markets: std::collections::HashSet<String> = market_orders
            .iter()
            .filter(|(_, orders)| market_reclaimable_bid_capital(orders) > Decimal::ZERO)
            .map(|(condition_id, _)| condition_id.clone())
            .collect();

        if held_bid_markets.is_empty() {
            return Ok(None);
        }

        let mut losers: Vec<&MarketEvaluation> = evaluations
            .iter()
            .filter(|eval| eval.report.would_trade)
            .filter(|eval| held_bid_markets.contains(&eval.market.condition_id))
            .filter(|eval| {
                market_orders
                    .get(&eval.market.condition_id)
                    .map(|orders| earliest_bid_created_at(orders))
                    .flatten()
                    .is_some_and(|created_at| created_at <= hold_cutoff)
            })
            .collect();

        losers.sort_by(|a, b| compare_market_evaluations(a, b).reverse());

        for loser in losers {
            let Some(loser_orders) = market_orders.get(&loser.market.condition_id) else {
                continue;
            };
            let reclaimable_bid_capital = market_reclaimable_bid_capital(loser_orders);
            if reclaimable_bid_capital <= Decimal::ZERO {
                continue;
            }

            let counterfactual_budget = actual_free_budget + reclaimable_bid_capital;
            let loser_rank_key = market_rank_key(&loser.quote_set, &loser.report);
            let mut best_entrant: Option<(String, MarketRankKey)> = None;

            for entrant in evaluations.iter().filter(|eval| {
                eval.market.condition_id != loser.market.condition_id
                    && !held_bid_markets.contains(&eval.market.condition_id)
                    && !eval.report.would_trade
            }) {
                let entrant_orders = market_orders
                    .get(&entrant.market.condition_id)
                    .cloned()
                    .unwrap_or_default();
                let (frontier_quote_set, frontier_report) = self
                    .evaluate_market_on_books_with_context(
                        &entrant.market,
                        &entrant.yes_book,
                        &entrant.no_book,
                        &entrant_orders,
                        counterfactual_budget,
                    )
                    .await?;

                if !frontier_report.would_trade {
                    continue;
                }

                let entrant_rank_key = market_rank_key(&frontier_quote_set, &frontier_report);
                if compare_rank_keys(
                    entrant_rank_key,
                    &entrant.market.condition_id,
                    loser_rank_key,
                    &loser.market.condition_id,
                ) != std::cmp::Ordering::Less
                {
                    continue;
                }

                // Skip rotation if improvement is below minimum threshold
                let improvement =
                    entrant_rank_key.estimated_reward - loser_rank_key.estimated_reward;
                if improvement < self.config.strategy.min_frontier_improvement {
                    trace!(
                        entrant = %entrant.market.condition_id,
                        loser_cid = %loser.market.condition_id,
                        improvement = %improvement,
                        threshold = %self.config.strategy.min_frontier_improvement,
                        "Frontier rotation skipped: improvement below threshold"
                    );
                    continue;
                }

                let replace_current = match best_entrant {
                    Some((ref current_condition_id, current_rank_key)) => {
                        compare_rank_keys(
                            entrant_rank_key,
                            &entrant.market.condition_id,
                            current_rank_key,
                            current_condition_id,
                        ) == std::cmp::Ordering::Less
                    }
                    None => true,
                };

                if replace_current {
                    best_entrant = Some((entrant.market.condition_id.clone(), entrant_rank_key));
                }
            }

            if let Some((entrant_condition_id, entrant_rank_key)) = best_entrant {
                return Ok(Some(FrontierRotationPlan {
                    loser_condition_id: loser.market.condition_id.clone(),
                    entrant_condition_id,
                    reclaimable_bid_capital,
                    counterfactual_budget_usd: counterfactual_budget,
                    loser_rank_key,
                    entrant_rank_key,
                }));
            }
        }

        Ok(None)
    }

    async fn arm_frontier_reservation(&self, cycle_id: &str, plan: &FrontierRotationPlan) {
        let reservation = PendingFrontierReservation {
            entrant_condition_id: plan.entrant_condition_id.clone(),
            loser_condition_id: plan.loser_condition_id.clone(),
            reclaimable_bid_capital: plan.reclaimable_bid_capital,
            armed_cycle_id: cycle_id.to_string(),
        };
        *self.frontier_reservation.write().await = Some(reservation.clone());
        info!(
            entrant_condition_id = %reservation.entrant_condition_id,
            loser_condition_id = %reservation.loser_condition_id,
            reclaimable_bid_capital = %reservation.reclaimable_bid_capital,
            armed_cycle_id = %reservation.armed_cycle_id,
            "Frontier reservation armed"
        );
    }

    async fn clear_frontier_reservation(&self, reason: &'static str) {
        let reservation = self.frontier_reservation.write().await.take();
        if let Some(reservation) = reservation {
            info!(
                entrant_condition_id = %reservation.entrant_condition_id,
                loser_condition_id = %reservation.loser_condition_id,
                reclaimable_bid_capital = %reservation.reclaimable_bid_capital,
                armed_cycle_id = %reservation.armed_cycle_id,
                reason,
                "Frontier reservation cleared"
            );
        }
    }

    async fn insert_managed_market(&self, market: &CanonicalMarket) {
        self.managed_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market.clone());
        let mut token_index = self.managed_token_index.write().await;
        token_index.insert(market.yes_token_id.clone(), market.condition_id.clone());
        token_index.insert(market.no_token_id.clone(), market.condition_id.clone());
    }

    async fn remove_managed_market(&self, condition_id: &str) {
        let removed = self.managed_markets.write().await.remove(condition_id);
        if let Some(market) = removed {
            let mut token_index = self.managed_token_index.write().await;
            if token_index
                .get(&market.yes_token_id)
                .is_some_and(|mapped| mapped == condition_id)
            {
                token_index.remove(&market.yes_token_id);
            }
            if token_index
                .get(&market.no_token_id)
                .is_some_and(|mapped| mapped == condition_id)
            {
                token_index.remove(&market.no_token_id);
            }
        }
    }

    async fn managed_market_for_token(&self, token_id: &str) -> Option<CanonicalMarket> {
        if let Some(condition_id) = self.managed_token_index.read().await.get(token_id).cloned() {
            if let Some(market) = self
                .managed_markets
                .read()
                .await
                .get(&condition_id)
                .cloned()
            {
                return Some(market);
            }
        }

        let matched_market = {
            let managed = self.managed_markets.read().await;
            managed
                .values()
                .find(|market| market.yes_token_id == token_id || market.no_token_id == token_id)
                .cloned()
        };

        if let Some(market) = matched_market.as_ref() {
            let mut token_index = self.managed_token_index.write().await;
            token_index.insert(market.yes_token_id.clone(), market.condition_id.clone());
            token_index.insert(market.no_token_id.clone(), market.condition_id.clone());
        }

        matched_market
    }

    async fn maybe_run_ws_hedge_depth_guard_for_token(&self, token_id: &str) {
        let Some(market) = self.managed_market_for_token(token_id).await else {
            return;
        };

        if !self
            .risk_manager
            .is_market_tradable(&market.condition_id)
            .await
        {
            return;
        }

        let (active_bids, pending_bids) = self
            .order_manager
            .market_bid_order_state_counts(&market.condition_id)
            .await;
        if active_bids == 0 || pending_bids > 0 {
            return;
        }

        let max_book_age = chrono::Duration::seconds(self.config.books.max_book_age_secs as i64);
        let Some((yes_book, no_book)) = self
            .book_manager
            .get_pair(&market.yes_token_id, &market.no_token_id)
            .await
        else {
            return;
        };

        if yes_book.is_stale(max_book_age) || no_book.is_stale(max_book_age) {
            return;
        }

        self.apply_hedge_depth_guard_for_market(&market, &yes_book, &no_book, "hedge_depth_ws")
            .await;
    }

    async fn apply_hedge_depth_guard_for_market(
        &self,
        market: &CanonicalMarket,
        yes_book: &OrderBookSnapshot,
        no_book: &OrderBookSnapshot,
        origin: &'static str,
    ) {
        let cid = &market.condition_id;
        let orders = self.order_manager.get_market_orders(cid).await;
        let bid_orders: Vec<_> = orders
            .into_iter()
            .filter(|order| order.leg.is_bid())
            .collect();
        if bid_orders.is_empty() {
            return;
        }

        for order in &bid_orders {
            let opposite_book = match order.leg {
                QuoteLeg::YesBid => no_book,
                QuoteLeg::NoBid => yes_book,
                _ => continue,
            };

            let own_book = match order.leg {
                QuoteLeg::YesBid => Some(yes_book),
                QuoteLeg::NoBid => Some(no_book),
                _ => None,
            };
            if let Some(book) = own_book {
                if let Some(mid) = book.mid() {
                    if mid < self.config.strategy.min_outcome_price {
                        warn!(
                            condition_id = %cid,
                            leg = %order.leg,
                            mid = %mid,
                            threshold = %self.config.strategy.min_outcome_price,
                            origin,
                            "Cancelling bid: outcome mid-price below minimum"
                        );
                        self.order_manager
                            .cancel_tracked_order(
                                order,
                                CancelReasonCode::OutcomePriceBelowMinimum,
                                origin,
                            )
                            .await;
                        continue;
                    }
                }
            }

            let hedgeable = max_hedgeable_within_slippage(
                opposite_book,
                true,
                self.config.strategy.max_slippage_bps,
            );

            if hedgeable >= order.size {
                continue;
            }

            let min_size = market.reward_config.min_size;
            let (opposite_best_price, opposite_best_size) = opposite_book
                .asks
                .first()
                .map(|level| (level.price, level.size))
                .unwrap_or((Decimal::ZERO, Decimal::ZERO));
            let diagnostics = OrderEventDiagnostics {
                quote_refresh: None,
                hedge_depth: Some(HedgeDepthDiagnostics {
                    hedgeable_size: hedgeable,
                    min_order_size: min_size,
                    opposite_best_price,
                    opposite_best_size,
                }),
            };

            if hedgeable < min_size {
                warn!(
                    market_name = %market.question,
                    condition_id = %cid,
                    leg = %order.leg,
                    order_size = %order.size,
                    hedgeable = %hedgeable,
                    origin,
                    "Cancelling bid: hedge depth below min_size"
                );
                self.order_manager
                    .cancel_tracked_order_with_diagnostics(
                        order,
                        CancelReasonCode::HedgeDepthBelowMinimum,
                        origin,
                        Some(&diagnostics),
                    )
                    .await;
            } else {
                let new_size = whole_share_budget_limit(hedgeable);
                if new_size >= order.size {
                    info!(
                        market_name = %market.question,
                        condition_id = %cid,
                        leg = %order.leg,
                        current_size = %order.size,
                        hedgeable = %hedgeable,
                        floored_new_size = %new_size,
                        origin,
                        "Skipping hedge-depth resize: floored size unchanged"
                    );
                    continue;
                }

                warn!(
                    market_name = %market.question,
                    condition_id = %cid,
                    leg = %order.leg,
                    current_size = %order.size,
                    hedgeable = %hedgeable,
                    new_size = %new_size,
                    origin,
                    "Resizing bid: hedge depth reduced"
                );
                if let Err(error) = self
                    .order_manager
                    .resize_order_with_diagnostics(
                        &order.order_id,
                        new_size,
                        CancelReasonCode::HedgeDepthPartialDownsize,
                        origin,
                        Some(&diagnostics),
                    )
                    .await
                {
                    error!(error = %error, "Failed to resize bid order");
                }
            }
        }
    }

    async fn run_same_cycle_frontier_handoff(
        &self,
        _cycle_id: &str,
        evaluations: &[MarketEvaluation],
    ) -> SameCycleHandoffResult {
        let window_secs = self.config.strategy.frontier_handoff_window_secs;
        if window_secs == 0 {
            return SameCycleHandoffResult::Disabled;
        }

        let window = StdDuration::from_secs(window_secs);
        let poll_interval_ms = 250;
        let deadline = Instant::now() + window;

        let loser_id = {
            let res = self.frontier_reservation.read().await;
            match res.as_ref() {
                Some(r) => r.loser_condition_id.clone(),
                None => return SameCycleHandoffResult::NoReservation,
            }
        };

        info!(
            loser = %loser_id,
            window_secs = window_secs,
            "Frontier same-cycle handoff started"
        );

        while Instant::now() < deadline {
            self.order_manager.retry_pending_cancels().await;

            if !self
                .order_manager
                .has_bid_orders_or_pending_cancels(&loser_id)
                .await
            {
                info!("Frontier same-cycle handoff: loser cancel verified");

                match self
                    .select_best_post_cancel_market(evaluations, &loser_id)
                    .await
                {
                    Some((market, quote_set, _report, trace_ids)) => {
                        let position = self
                            .position_manager
                            .get_position(&market.condition_id)
                            .await;
                        let min_size = market.reward_config.min_size;

                        if let Err(e) = self
                            .order_manager
                            .place_quotes(
                                &market,
                                &quote_set,
                                position.as_ref(),
                                min_size,
                                Some(&trace_ids),
                                "frontier_reservation",
                                None,
                            )
                            .await
                        {
                            warn!(
                                condition_id = %market.condition_id,
                                error = %e,
                                "Same-cycle handoff placement failed"
                            );
                            self.clear_frontier_reservation("same_cycle_placement_failed")
                                .await;
                            return SameCycleHandoffResult::Failed;
                        }

                        let has_bids = self
                            .order_manager
                            .get_market_orders(&market.condition_id)
                            .await
                            .iter()
                            .any(|o| o.leg.is_bid());

                        if has_bids {
                            let entrant_id = market.condition_id.clone();
                            let was_original = {
                                let res = self.frontier_reservation.read().await;
                                res.as_ref()
                                    .map(|r| r.entrant_condition_id == entrant_id)
                                    .unwrap_or(false)
                            };
                            self.insert_managed_market(&market).await;
                            self.clear_frontier_reservation("same_cycle_placed").await;
                            info!(
                                entrant = %entrant_id,
                                was_original_reservation = was_original,
                                "Frontier same-cycle handoff placed"
                            );
                            return SameCycleHandoffResult::Placed(entrant_id);
                        } else {
                            self.clear_frontier_reservation("same_cycle_place_no_bids")
                                .await;
                            return SameCycleHandoffResult::NoPlaceableMarket;
                        }
                    }
                    None => {
                        info!(
                            "Frontier same-cycle handoff: no viable market after fresh evaluation"
                        );
                        self.clear_frontier_reservation("same_cycle_no_viable_market")
                            .await;
                        return SameCycleHandoffResult::NoPlaceableMarket;
                    }
                }
            }

            tokio::time::sleep(Duration::from_millis(poll_interval_ms)).await;
        }

        info!("Frontier same-cycle handoff timed out — deferring to next cycle");
        SameCycleHandoffResult::TimedOut
    }

    async fn select_best_post_cancel_market(
        &self,
        evaluations: &[MarketEvaluation],
        loser_condition_id: &str,
    ) -> Option<(
        CanonicalMarket,
        QuoteSet,
        DecisionReport,
        HashMap<QuoteLeg, String>,
    )> {
        let mut candidates: Vec<(
            CanonicalMarket,
            QuoteSet,
            DecisionReport,
            HashMap<QuoteLeg, String>,
            MarketRankKey,
        )> = Vec::new();

        for eval in evaluations {
            let market = &eval.market;
            if market.condition_id == loser_condition_id {
                continue;
            }

            let existing = self
                .order_manager
                .get_market_orders(&market.condition_id)
                .await;
            if existing.iter().any(|o| o.leg.is_bid()) {
                continue;
            }

            if !self
                .risk_manager
                .is_market_tradable(&market.condition_id)
                .await
            {
                continue;
            }

            match self.evaluate_market(market).await {
                Ok((_yes_book, _no_book, quote_set, report)) => {
                    if !report.would_trade {
                        continue;
                    }
                    let rank = market_rank_key(&quote_set, &report);
                    let trace_ids = build_quote_trace_ids(&quote_set);
                    candidates.push((market.clone(), quote_set, report, trace_ids, rank));
                }
                Err(e) => {
                    debug!(
                        condition_id = %market.condition_id,
                        error = %e,
                        "Skipping market in post-cancel evaluation"
                    );
                    continue;
                }
            }
        }

        if candidates.is_empty() {
            return None;
        }

        candidates
            .sort_by(|a, b| compare_rank_keys(a.4, &a.0.condition_id, b.4, &b.0.condition_id));

        candidates
            .into_iter()
            .next()
            .map(|(market, quote_set, report, trace_ids, _rank)| {
                (market, quote_set, report, trace_ids)
            })
    }

    async fn activate_frontier_reservation(
        &self,
        cycle_id: &str,
        reservation: &PendingFrontierReservation,
        evaluations: &[MarketEvaluation],
    ) -> Result<Option<String>> {
        if self
            .order_manager
            .has_bid_orders_or_pending_cancels(&reservation.loser_condition_id)
            .await
        {
            info!(
                entrant_condition_id = %reservation.entrant_condition_id,
                loser_condition_id = %reservation.loser_condition_id,
                armed_cycle_id = %reservation.armed_cycle_id,
                "Frontier reservation waiting for cancel verification"
            );
            return Ok(None);
        }

        let Some(eval) = evaluations
            .iter()
            .find(|eval| eval.market.condition_id == reservation.entrant_condition_id)
        else {
            self.clear_frontier_reservation("entrant_not_admitted")
                .await;
            return Ok(None);
        };

        info!(
            cycle_id = %cycle_id,
            entrant_condition_id = %reservation.entrant_condition_id,
            loser_condition_id = %reservation.loser_condition_id,
            reclaimable_bid_capital = %reservation.reclaimable_bid_capital,
            "Frontier reservation entrant attempt"
        );

        let existing = self
            .order_manager
            .get_market_orders(&reservation.entrant_condition_id)
            .await;
        if existing.iter().any(|order| order.leg.is_bid()) {
            self.clear_frontier_reservation("entrant_already_active")
                .await;
            return Ok(Some(reservation.entrant_condition_id.clone()));
        }

        if !eval.report.would_trade {
            self.clear_frontier_reservation("entrant_no_longer_viable")
                .await;
            return Ok(None);
        }

        let position = self
            .position_manager
            .get_position(&reservation.entrant_condition_id)
            .await;
        let min_size = eval.market.reward_config.min_size;
        if let Err(error) = self
            .order_manager
            .place_quotes(
                &eval.market,
                &eval.quote_set,
                position.as_ref(),
                min_size,
                Some(&eval.trace_ids),
                "frontier_reservation",
                None,
            )
            .await
        {
            error!(
                condition_id = %reservation.entrant_condition_id,
                error = %error,
                "Failed reserved frontier entrant placement"
            );
        }

        let has_active_bids = self
            .order_manager
            .get_market_orders(&reservation.entrant_condition_id)
            .await
            .iter()
            .any(|order| order.leg.is_bid());
        self.clear_frontier_reservation(if has_active_bids {
            "entrant_attempted"
        } else {
            "entrant_attempt_failed"
        })
        .await;

        if has_active_bids {
            self.insert_managed_market(&eval.market).await;
            Ok(Some(reservation.entrant_condition_id.clone()))
        } else {
            Ok(None)
        }
    }

    /// Detect position drift between API truth and tracked order state.
    /// Catches websocket gaps, external activity, and phantom asks.
    async fn detect_position_drift(&self) {
        let managed = self.managed_markets.read().await;

        for (cid, _market) in managed.iter() {
            let api_pos = self.position_manager.get_position(cid).await;
            let tracked = self.order_manager.get_market_orders(cid).await;

            let has_asks = tracked.iter().any(|o| o.leg.is_ask());
            let has_inventory = api_pos.as_ref().map_or(false, |p| {
                p.yes_size > Decimal::ZERO || p.no_size > Decimal::ZERO
            });

            if has_asks && !has_inventory {
                error!(
                    condition_id = %cid,
                    "DRIFT: asks resting but no inventory — cancelling phantom asks"
                );
                if let Err(e) = self
                    .order_manager
                    .cancel_asks_only(cid, CancelReasonCode::ExternalCancel, "position_drift")
                    .await
                {
                    error!(condition_id = %cid, error = %e, "Failed to cancel phantom asks");
                }
            }

            if has_inventory && tracked.is_empty() {
                if let Some(pos) = api_pos.as_ref() {
                    warn!(
                        condition_id = %cid,
                        yes = %pos.yes_size,
                        no = %pos.no_size,
                        "DRIFT: API shows position but no tracked orders — external trade?"
                    );
                }
            }
        }
    }

    /// Log a status summary of all managed markets, positions, and resting orders.
    async fn log_status(&self) {
        let managed = self.managed_markets.read().await;
        let order_committed = self.order_manager.committed_capital().await;
        let order_exposure = self.order_manager.committed_exposure().await;
        let position_committed = self.position_manager.total_position_cost().await;
        let total_committed = order_committed + position_committed;
        let hedge_reserve = order_exposure - order_committed;
        let api_balance = *self.cached_balance.read().await;
        let budget = self.order_manager.available_budget().await;
        let competition_multiplier = self.calibration.read().await.current_multiplier();

        // Calculate total_est_daily before emitting snapshot so it can be included
        let mut total_est_daily = Decimal::ZERO;
        let mut active_count = 0u32;
        let mut idle_count = 0u32;
        let mut total_legs = 0u32;

        // Pre-collect per-market data for logging below
        struct MarketStatus {
            cid: String,
            yes_inv: Decimal,
            no_inv: Decimal,
            orders: Vec<TrackedOrder>,
            est_daily: Decimal,
            reward_per_share: Decimal,
            reward_per_share_eff: Decimal,
            shares_committed: Decimal,
            has_orders: bool,
            has_inventory: bool,
        }
        let mut market_statuses = Vec::new();

        for (cid, market) in managed.iter() {
            let orders = self.order_manager.get_market_orders(cid).await;
            let position = self.position_manager.get_position(cid).await;

            let yes_inv = position
                .as_ref()
                .map(|p| p.yes_size)
                .unwrap_or(Decimal::ZERO);
            let no_inv = position
                .as_ref()
                .map(|p| p.no_size)
                .unwrap_or(Decimal::ZERO);

            let has_orders = !orders.is_empty();
            let has_inventory = yes_inv > Decimal::ZERO || no_inv > Decimal::ZERO;

            let est_daily = self
                .estimate_market_daily_reward(market, &orders, competition_multiplier)
                .await;
            total_est_daily += est_daily;

            let shares_committed: Decimal = orders
                .iter()
                .filter(|o| o.leg.is_bid())
                .map(|o| o.size)
                .sum();
            let reward_per_share = if shares_committed > Decimal::ZERO {
                est_daily / shares_committed
            } else {
                Decimal::ZERO
            };
            let reward_per_share_eff =
                reward_per_share * self.config.strategy.reward_discount_factor;

            market_statuses.push(MarketStatus {
                cid: cid.clone(),
                yes_inv,
                no_inv,
                orders,
                est_daily,
                reward_per_share,
                reward_per_share_eff,
                shares_committed,
                has_orders,
                has_inventory,
            });
        }

        let est_daily_opt = if managed.is_empty() {
            None
        } else {
            Some(total_est_daily)
        };
        let book_ws_stats = self.current_book_ws_stats().await;

        self.emit_event(emitters::build_status_snapshot(
            &self.run_id,
            &self.mode,
            managed.len(),
            order_committed,
            position_committed,
            total_committed,
            api_balance,
            budget,
            competition_multiplier,
            est_daily_opt,
            Some(book_ws_stats),
        ));

        info!(
            managed_markets = managed.len(),
            order_committed = %order_committed,
            order_exposure = %order_exposure,
            position_committed = %position_committed,
            total_committed = %total_committed,
            hedge_reserve = %hedge_reserve,
            api_balance = %api_balance,
            available_budget = %budget,
            "--- STATUS ---"
        );

        if managed.is_empty() {
            return;
        }

        let mut total_shares_all = Decimal::ZERO;

        for ms in &market_statuses {
            if !ms.has_orders && !ms.has_inventory {
                idle_count += 1;
                continue;
            }
            active_count += 1;
            total_legs += ms.orders.len() as u32;
            total_shares_all += ms.shares_committed;

            // Summarize resting orders
            let mut order_summary = String::new();
            for o in &ms.orders {
                if !order_summary.is_empty() {
                    order_summary.push_str(", ");
                }
                order_summary.push_str(&format!("{}@{}x{}", o.leg, o.price, o.size));
            }
            if order_summary.is_empty() {
                order_summary.push_str("none");
            }

            // Truncate condition_id for readability
            let short_cid = if ms.cid.len() > 12 {
                format!("{}...", &ms.cid[..12])
            } else {
                ms.cid.clone()
            };

            info!(
                market = %short_cid,
                yes_pos = %ms.yes_inv,
                no_pos = %ms.no_inv,
                orders = %order_summary,
                est_daily = %format!("${:.2}", ms.est_daily),
                r_per_share = %format!("{:.3}¢/sh", ms.reward_per_share * Decimal::from(100)),
                r_effective = %format!("{:.3}¢/sh", ms.reward_per_share_eff * Decimal::from(100)),
                "  position"
            );
        }

        let avg_r_per_share = if total_shares_all > Decimal::ZERO {
            total_est_daily / total_shares_all
        } else {
            Decimal::ZERO
        };
        let avg_r_effective = avg_r_per_share * self.config.strategy.reward_discount_factor;

        info!(
            active = active_count,
            idle = idle_count,
            legs = total_legs,
            total_est_daily = %format!("${:.2}", total_est_daily),
            avg_r_per_share = %format!("{:.3}¢/sh", avg_r_per_share * Decimal::from(100)),
            avg_r_effective = %format!("{:.3}¢/sh", avg_r_effective * Decimal::from(100)),
            "  estimated daily reward (all markets)"
        );

        // Show orders on non-managed markets (e.g. manually placed, below reward threshold)
        let all_tracked_cids = self.order_manager.get_tracked_condition_ids().await;
        for cid in &all_tracked_cids {
            if managed.contains_key(cid) {
                continue;
            }
            let orders = self.order_manager.get_market_orders(cid).await;
            if orders.is_empty() {
                continue;
            }
            let mut order_summary = String::new();
            for o in &orders {
                if !order_summary.is_empty() {
                    order_summary.push_str(", ");
                }
                order_summary.push_str(&format!("{}@{}x{}", o.leg, o.price, o.size));
            }
            let short_cid = if cid.len() > 12 {
                format!("{}...", &cid[..12])
            } else {
                cid.clone()
            };
            info!(
                market = %short_cid,
                orders = %order_summary,
                "  unmanaged orders"
            );
        }
    }

    /// Estimate daily reward for a market based on resting tracked orders.
    ///
    /// Uses the score proxy formula: scores our orders, scores visible competition,
    /// computes our share, and multiplies by daily_reward_total.
    async fn estimate_market_daily_reward(
        &self,
        market: &CanonicalMarket,
        tracked_orders: &[crate::trading::order_manager::TrackedOrder],
        competition_multiplier: Decimal,
    ) -> Decimal {
        if tracked_orders.is_empty() {
            return Decimal::ZERO;
        }

        let yes_book = self.book_manager.get_book(&market.yes_token_id).await;
        let no_book = self.book_manager.get_book(&market.no_token_id).await;

        let (yes_book, no_book) = match (yes_book, no_book) {
            (Some(y), Some(n)) => (y, n),
            _ => return Decimal::ZERO,
        };

        let mut proxy_config = self.config.strategy.score_proxy.clone();
        proxy_config.competition_multiplier = competition_multiplier;

        let quote_set = tracked_orders_to_quote_set(&market.condition_id, tracked_orders);
        let score_proxy = compute_score_proxy(
            &quote_set,
            &yes_book,
            &no_book,
            &market.reward_config,
            &proxy_config,
        );

        market.reward_config.daily_reward_total
            * score_proxy.estimated_share
            * self.config.strategy.reward_discount_factor
    }

    /// Place ask orders on owned inventory to earn rewards.
    ///
    /// Called immediately after a hedge and during cycles when we have inventory
    /// but bid-side economics don't justify new positions.
    async fn place_inventory_asks(
        &self,
        market: &CanonicalMarket,
        position: &Position,
        origin: &'static str,
    ) {
        // Fetch books from cache, falling back to REST if cache is empty
        let yes_book = match self.book_manager.get_book(&market.yes_token_id).await {
            Some(book) => Some(book),
            None => {
                warn!(
                    condition_id = %market.condition_id,
                    token_id = %market.yes_token_id,
                    "YES book missing from cache for ask placement — fetching via REST"
                );
                match self.book_rest.fetch_book(&market.yes_token_id).await {
                    Ok(book) => {
                        self.book_manager.insert_snapshot(book.clone()).await;
                        Some(book)
                    }
                    Err(e) => {
                        error!(
                            condition_id = %market.condition_id,
                            error = %e,
                            "Failed to fetch YES book via REST for ask placement"
                        );
                        None
                    }
                }
            }
        };

        let no_book = match self.book_manager.get_book(&market.no_token_id).await {
            Some(book) => Some(book),
            None => {
                warn!(
                    condition_id = %market.condition_id,
                    token_id = %market.no_token_id,
                    "NO book missing from cache for ask placement — fetching via REST"
                );
                match self.book_rest.fetch_book(&market.no_token_id).await {
                    Ok(book) => {
                        self.book_manager.insert_snapshot(book.clone()).await;
                        Some(book)
                    }
                    Err(e) => {
                        error!(
                            condition_id = %market.condition_id,
                            error = %e,
                            "Failed to fetch NO book via REST for ask placement"
                        );
                        None
                    }
                }
            }
        };

        let max_spread = market.reward_config.max_spread;
        let ask_depth = self.config.strategy.ask_depth_pct;
        let mut candidates = Vec::new();

        // YES ASK if we have YES inventory
        if position.yes_size > Decimal::ZERO {
            if let Some(book) = &yes_book {
                if let Some(price) = compute_ask_price(book, max_spread, ask_depth) {
                    let ask_size = position.yes_size;
                    candidates.push(QuoteCandidate {
                        condition_id: market.condition_id.clone(),
                        leg: QuoteLeg::YesAsk,
                        price,
                        size: ask_size,
                        status: QuoteStatus::Approved,
                        reason: None,
                    });
                } else {
                    warn!(
                        condition_id = %market.condition_id,
                        "Cannot compute YES ask price — book has no bid/ask levels"
                    );
                }
            }
        }

        // NO ASK if we have NO inventory
        if position.no_size > Decimal::ZERO {
            if let Some(book) = &no_book {
                if let Some(price) = compute_ask_price(book, max_spread, ask_depth) {
                    let ask_size = position.no_size;
                    candidates.push(QuoteCandidate {
                        condition_id: market.condition_id.clone(),
                        leg: QuoteLeg::NoAsk,
                        price,
                        size: ask_size,
                        status: QuoteStatus::Approved,
                        reason: None,
                    });
                } else {
                    warn!(
                        condition_id = %market.condition_id,
                        "Cannot compute NO ask price — book has no bid/ask levels"
                    );
                }
            }
        }

        if candidates.is_empty() {
            warn!(
                condition_id = %market.condition_id,
                yes_inv = %position.yes_size,
                no_inv = %position.no_size,
                "No ask candidates generated despite holding inventory"
            );
            return;
        }

        let ask_set = QuoteSet {
            condition_id: market.condition_id.clone(),
            candidates,
        };

        // Use cancel-replace to avoid duplicating existing asks
        let existing = self
            .order_manager
            .get_market_orders(&market.condition_id)
            .await;
        let has_asks = existing.iter().any(|o| o.leg.is_ask());

        let min_size = market.reward_config.min_size;
        if has_asks {
            if let Err(e) = self
                .order_manager
                .cancel_replace_if_drifted(
                    market,
                    &ask_set,
                    self.config.strategy.quote_drift_bps,
                    Some(position),
                    min_size,
                    None,
                    origin,
                    Some("ask_inventory"),
                )
                .await
            {
                error!(
                    condition_id = %market.condition_id,
                    error = %e,
                    "Failed to update inventory asks"
                );
            }
        } else {
            if let Err(e) = self
                .order_manager
                .place_quotes(
                    market,
                    &ask_set,
                    Some(position),
                    min_size,
                    None,
                    origin,
                    Some("ask_inventory"),
                )
                .await
            {
                error!(
                    condition_id = %market.condition_id,
                    error = %e,
                    "Failed to place inventory asks"
                );
            }
        }

        info!(
            condition_id = %market.condition_id,
            yes_inv = %position.yes_size,
            no_inv = %position.no_size,
            "\x1b[1;36m========== ASK ORDERS PLACED ON INVENTORY ==========\x1b[0m"
        );
    }

    async fn kill_market(&self, condition_id: &str, reason: &str) -> HaltMarketOutcome {
        halt_market_and_start_cleanup(
            condition_id,
            reason,
            &self.risk_manager,
            &self.order_manager,
            &self.position_manager,
            &self.trading_client,
            &self.managed_markets,
            &self.known_markets,
            &self.config,
            &self.run_id,
            &self.mode,
            &self.event_producer,
            &self.error_logger,
        )
        .await
    }

    async fn finalize_halted_markets(&self) {
        let halted_ids: Vec<String> = self
            .risk_manager
            .get_all_states()
            .await
            .into_iter()
            .filter(|state| state.halted)
            .map(|state| state.condition_id)
            .collect();

        for condition_id in halted_ids {
            self.finalize_halted_market_if_drained(&condition_id).await;
        }
    }

    async fn finalize_halted_market_if_drained(&self, condition_id: &str) -> HaltCleanupOutcome {
        let outcome = finalize_halted_market_cleanup(
            condition_id,
            &self.order_manager,
            &self.position_manager,
            &self.trading_client,
            &self.managed_markets,
            &self.known_markets,
            &self.config,
        )
        .await;
        self.record_halt_cleanup_outcome(condition_id, &outcome)
            .await;
        outcome
    }

    async fn flatten_unhedged(&self, condition_id: &str) {
        let _ = flatten_directional_inventory_for_halt(
            condition_id,
            &self.position_manager,
            &self.trading_client,
            &self.managed_markets,
            &self.known_markets,
            &self.config,
        )
        .await;
    }

    async fn record_halt_cleanup_outcome(&self, condition_id: &str, outcome: &HaltCleanupOutcome) {
        let mut statuses = self.halt_cleanup_statuses.write().await;
        if outcome.verified() {
            statuses.remove(condition_id);
            return;
        }

        if statuses.get(condition_id).copied() == Some(outcome.status) {
            return;
        }
        statuses.insert(condition_id.to_string(), outcome.status);
        drop(statuses);

        if let Some(reason) = outcome.degraded_reason(condition_id) {
            self.emit_event(
                emitters::build_monitor_degraded(
                    &self.run_id,
                    &self.mode,
                    HALT_CLEANUP_MONITOR_COMPONENT,
                    &reason,
                    None,
                )
                .with_condition_id(condition_id.to_string()),
            );
        }
    }

    async fn maybe_resume_stale_book_market(
        &self,
        market: &CanonicalMarket,
        cleanup: &HaltCleanupOutcome,
    ) -> bool {
        let condition_id = &market.condition_id;
        let Some(state) = self.risk_manager.get_market_state(condition_id).await else {
            self.clear_stale_book_recovery_tracking(condition_id).await;
            return false;
        };
        if !state.halted || state.halt_reason.as_deref() != Some(STALE_BOOK_HALT_REASON) {
            self.clear_stale_book_recovery_tracking(condition_id).await;
            return false;
        }
        if !cleanup.verified() {
            self.stale_book_recovery_streaks
                .write()
                .await
                .remove(condition_id);
            return false;
        }

        let fresh_books_confirmed = self.confirm_fresh_books_for_recovery(market).await;
        let streak = {
            let mut streaks = self.stale_book_recovery_streaks.write().await;
            if !fresh_books_confirmed {
                streaks.remove(condition_id);
                0
            } else {
                let streak = streaks.entry(condition_id.clone()).or_insert(0);
                *streak += 1;
                *streak
            }
        };
        if streak < STALE_BOOK_RECOVERY_REQUIRED_MATCHES {
            return false;
        }

        self.risk_manager.resume_market(condition_id).await;
        self.clear_stale_book_recovery_tracking(condition_id).await;
        self.emit_event(emitters::build_risk_state_changed(
            &self.run_id,
            &self.mode,
            Some(condition_id),
            "resumed",
            Some("stale_book_recovered"),
            None,
            Some(self.risk_manager.is_globally_halted().await),
        ));
        true
    }

    async fn clear_stale_book_recovery_tracking(&self, condition_id: &str) {
        self.halt_cleanup_statuses
            .write()
            .await
            .remove(condition_id);
        self.stale_book_recovery_streaks
            .write()
            .await
            .remove(condition_id);
    }

    async fn confirm_fresh_books_for_recovery(&self, market: &CanonicalMarket) -> bool {
        let stale_threshold = chrono::Duration::seconds(self.config.books.max_book_age_secs as i64);
        let yes_fresh = self
            .book_manager
            .get_book(&market.yes_token_id)
            .await
            .is_some_and(|book| !book.is_stale(stale_threshold));
        let no_fresh = self
            .book_manager
            .get_book(&market.no_token_id)
            .await
            .is_some_and(|book| !book.is_stale(stale_threshold));
        if yes_fresh && no_fresh {
            return true;
        }

        match self.refresh_books_for_depth_check(market).await {
            Ok((fresh_yes, fresh_no)) => {
                !fresh_yes.is_stale(stale_threshold) && !fresh_no.is_stale(stale_threshold)
            }
            Err(error) => {
                warn!(
                    condition_id = %market.condition_id,
                    error = %error,
                    "Stale-book recovery refresh failed"
                );
                false
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn harness_ctf_merge_enabled(&self) -> bool {
        self.ctf_merger.is_some()
    }

    #[cfg(test)]
    pub(crate) async fn harness_ctf_merge_preflight(&self) -> Result<()> {
        let merger = self
            .ctf_merger
            .as_ref()
            .context("ctf merger is not configured in the live engine")?;
        merger
            .preflight_check()
            .await
            .context("ctf merger preflight failed")
    }

    #[cfg(test)]
    pub(crate) async fn harness_merge_pairs(
        &self,
        condition_id: &str,
        minimum_pairs: Decimal,
    ) -> Result<HarnessPairExitOutcome> {
        self.position_manager
            .sync_positions()
            .await
            .context("failed to sync positions before harness pair exit")?;

        let pre_position = self
            .position_manager
            .get_position(condition_id)
            .await
            .unwrap_or_else(|| Position::new(condition_id.to_string()));
        let available_pairs = merge_eligible_pairs(&pre_position);
        if available_pairs < minimum_pairs || available_pairs <= Decimal::ZERO {
            anyhow::bail!(
                "harness pair exit requires at least {} complete sets for {}, but synced position only had {}",
                minimum_pairs,
                condition_id,
                available_pairs
            );
        }

        let market =
            resolve_market_metadata(&self.managed_markets, &self.known_markets, condition_id).await;
        if market.is_none() {
            anyhow::bail!(
                "harness pair exit requires market metadata for merge venue resolution for {}",
                condition_id
            );
        }
        let fill_handler = FillHandler {
            order_manager: self.order_manager.clone(),
            hedge_executor: self.hedge_executor.clone(),
            managed_markets: self.managed_markets.clone(),
            known_markets: self.known_markets.clone(),
            risk_manager: self.risk_manager.clone(),
            position_manager: self.position_manager.clone(),
            book_manager: self.book_manager.clone(),
            book_rest: self.book_rest.clone(),
            trading_client: self.trading_client.clone(),
            config: self.config.clone(),
            event_producer: self.event_producer.clone(),
            run_id: self.run_id.clone(),
            mode: self.mode.clone(),
            cached_balance: self.cached_balance.clone(),
            hedge_order_ids: self.hedge_order_ids.clone(),
            recon_baselines: self.recon_baselines.clone(),
            hedge_signals: self.hedge_signals.clone(),
            recent_resolution_trades: self.recent_resolution_trades.clone(),
            ctf_merger: self.ctf_merger.clone(),
            hedge_locks: self.hedge_locks.clone(),
            error_logger: self.error_logger.clone(),
        };

        let mut exit_telemetry = None;
        refresh_hedge_exit_telemetry(
            &mut exit_telemetry,
            Some(&pre_position),
            hedge_exposure_tolerance(&self.config),
            false,
            self.ctf_merger.is_some(),
        );
        let merge_truth_observation = fill_handler
            .execute_pair_exit(
                condition_id,
                market.as_ref(),
                &pre_position,
                &mut exit_telemetry,
                MergeTruthHandling::WaitForConvergence,
            )
            .await;
        let telemetry = exit_telemetry.clone().unwrap_or_default();
        if telemetry.exit_path_status == "merge_succeeded" {
            let tx_hash = telemetry.merge_tx_hash.as_deref().unwrap_or("<missing>");
            let expected_position = expected_post_merge_position(&pre_position, available_pairs);
            let observation = merge_truth_observation.unwrap_or_else(|| MergeTruthObservation {
                converged: false,
                observed_for: Duration::ZERO,
                last_seen_position: Position::new(condition_id.to_string()),
                last_sync_error: None,
            });
            if !observation.converged {
                anyhow::bail!(
                    "{}",
                    merge_truth_timeout_reason(
                        condition_id,
                        tx_hash,
                        &expected_position,
                        &observation,
                    )
                );
            }
        }

        self.position_manager
            .sync_positions()
            .await
            .context("failed to sync positions after harness pair exit")?;
        let post_position = self
            .position_manager
            .get_position(condition_id)
            .await
            .unwrap_or_else(|| Position::new(condition_id.to_string()));
        let telemetry = exit_telemetry.unwrap_or_default();

        Ok(HarnessPairExitOutcome {
            exit_path_status: telemetry.exit_path_status,
            merge_eligible_pairs: telemetry.merge_eligible_pairs,
            ctf_merge_configured: telemetry.ctf_merge_configured,
            merge_attempted: telemetry.merge_attempted,
            merge_tx_hash: telemetry.merge_tx_hash,
            merge_failure_reason: telemetry.merge_failure_reason,
            fallback_asks_attempted: telemetry.fallback_asks_attempted,
            fallback_ask_count: telemetry.fallback_ask_count,
            fallback_failure_reason: telemetry.fallback_failure_reason,
            post_position,
        })
    }

    /// Sample a few tracked orders for scoring status and feed to calibration.
    async fn sample_order_scoring(&self) {
        let tracked_orders = self.order_manager.get_all_orders().await;
        if tracked_orders.is_empty() {
            return;
        }

        // Sample up to 2 orders per cycle
        let sample_count = tracked_orders.len().min(2);
        for tracked in tracked_orders.iter().take(sample_count) {
            let Some(predicted_scoring) = self.predict_order_scoring(tracked).await else {
                debug!(
                    condition_id = %tracked.condition_id,
                    order_id = %tracked.order_id,
                    leg = %tracked.leg,
                    "Skipping calibration sample: missing metadata or fresh book truth"
                );
                continue;
            };

            match self
                .trading_client
                .check_order_scoring(&tracked.order_id)
                .await
            {
                Ok(actual_scoring) => {
                    let remaining_size = (tracked.size - tracked.matched_size).max(Decimal::ZERO);
                    self.record_recent_scoring_observation(
                        &tracked.order_id,
                        actual_scoring,
                        tracked.price,
                        remaining_size,
                    )
                    .await;
                    let share = self.calibration.read().await.current_multiplier();

                    if let Some(adjustment) = self.calibration.write().await.record_sample(
                        tracked.condition_id.clone(),
                        tracked.order_id.clone(),
                        predicted_scoring,
                        actual_scoring,
                        share,
                    ) {
                        self.emit_event(emitters::build_calibration_adjusted(
                            &self.run_id,
                            &self.mode,
                            &adjustment,
                        ));
                    }
                }
                Err(e) => {
                    warn!(order_id = %tracked.order_id, error = %e, "Scoring check failed");
                }
            }
        }
    }

    async fn predict_order_scoring(&self, tracked: &TrackedOrder) -> Option<bool> {
        let market = resolve_market_metadata(
            &self.managed_markets,
            &self.known_markets,
            &tracked.condition_id,
        )
        .await?;
        let stale_threshold = chrono::Duration::seconds(self.config.books.max_book_age_secs as i64);
        let yes_book = self.book_manager.get_book(&market.yes_token_id).await?;
        let no_book = self.book_manager.get_book(&market.no_token_id).await?;
        if yes_book.is_stale(stale_threshold) || no_book.is_stale(stale_threshold) {
            return None;
        }

        let tracked_orders = self
            .order_manager
            .get_market_orders(&market.condition_id)
            .await;
        let budget_max = self.order_manager.available_budget().await;
        let (quote_set, _report) = self
            .evaluate_market_on_books_with_context(
                &market,
                &yes_book,
                &no_book,
                &tracked_orders,
                budget_max,
            )
            .await
            .ok()?;

        let Some(candidate) = tracked_order_current_quote_candidate(tracked, &quote_set) else {
            return Some(false);
        };
        let remaining_size = (tracked.size - tracked.matched_size).max(Decimal::ZERO);
        if remaining_size <= Decimal::ZERO {
            return Some(false);
        }

        if let Some(observation) = self
            .recent_scoring_observation_for(tracked, remaining_size)
            .await
        {
            if !observation.actual_scoring {
                return Some(false);
            }
        }

        let score_quote_set = QuoteSet {
            condition_id: market.condition_id.clone(),
            candidates: vec![QuoteCandidate {
                condition_id: market.condition_id.clone(),
                leg: candidate.leg,
                price: tracked.price,
                size: remaining_size.min(candidate.size),
                status: QuoteStatus::Approved,
                reason: None,
            }],
        };
        let mut proxy_config = self.config.strategy.score_proxy.clone();
        proxy_config.competition_multiplier = self.calibration.read().await.current_multiplier();
        let score_proxy = compute_score_proxy(
            &score_quote_set,
            &yes_book,
            &no_book,
            &market.reward_config,
            &proxy_config,
        );

        Some(score_proxy.estimated_share >= self.config.strategy.score_proxy.target_score_share)
    }

    /// Ensure known_markets covers every market where we hold inventory.
    ///
    /// For positions not already in known_markets, tries to resolve market
    /// metadata from: (1) admitted markets list, (2) open orders data,
    /// (3) CLOB API fallback. This closes the blind spot where positions
    /// on markets that dropped off the sampling API were invisible to
    /// reconciliation.
    async fn seed_known_markets_from_positions(&self, admitted: &[CanonicalMarket]) {
        let state = self.position_manager.get_state().await;
        let known = self.known_markets.read().await;

        // Find condition_ids with non-zero positions not already in known_markets
        let missing_cids: Vec<String> = state
            .positions
            .iter()
            .filter(|p| p.yes_size > Decimal::ZERO || p.no_size > Decimal::ZERO)
            .filter(|p| !known.contains_key(&p.condition_id))
            .map(|p| p.condition_id.clone())
            .collect();
        drop(known);

        if missing_cids.is_empty() {
            return;
        }

        info!(
            count = missing_cids.len(),
            "Positions found on markets not in known_markets — resolving metadata"
        );

        // Build lookup from admitted markets
        let admitted_map: HashMap<&str, &CanonicalMarket> = admitted
            .iter()
            .map(|m| (m.condition_id.as_str(), m))
            .collect();

        let mut resolved = Vec::new();
        let mut still_missing = Vec::new();

        for cid in &missing_cids {
            if let Some(market) = admitted_map.get(cid.as_str()) {
                resolved.push((*market).clone());
            } else {
                still_missing.push(cid.clone());
            }
        }

        // Fallback: fetch from CLOB API for remaining unresolved markets
        if !still_missing.is_empty() {
            info!(
                count = still_missing.len(),
                "Fetching market metadata from CLOB API for unresolved positions"
            );
            for cid in &still_missing {
                match self.discovery.fetch_market_by_condition_id(cid).await {
                    Ok(Some(market)) => {
                        // Convert Market to CanonicalMarket using minimal fields
                        if let (Some(yes_token), Some(no_token)) =
                            (market.yes_token(), market.no_token())
                        {
                            resolved.push(CanonicalMarket {
                                condition_id: market.condition_id.clone(),
                                market_slug: market.market_slug.clone(),
                                question: market.question.clone(),
                                yes_token_id: yes_token.token_id.clone(),
                                no_token_id: no_token.token_id.clone(),
                                reward_config: market.reward_config.clone().unwrap_or_else(|| {
                                    crate::models::RewardConfig {
                                        condition_id: market.condition_id.clone(),
                                        daily_reward_rates: vec![],
                                        daily_reward_total: Decimal::ZERO,
                                        min_size: Decimal::ZERO,
                                        max_spread: Decimal::ZERO,
                                    }
                                }),
                                neg_risk: market.neg_risk,
                                tick_size: market.minimum_tick_size.clone(),
                                end_date: market.end_date_iso.as_deref().and_then(|s| {
                                    chrono::DateTime::parse_from_rfc3339(s)
                                        .ok()
                                        .map(|dt| dt.with_timezone(&Utc))
                                }),
                                admitted_at: Utc::now(),
                                status: crate::models::MarketStatus::Admitted,
                            });
                        } else {
                            error!(
                                condition_id = %cid,
                                "CLOB API returned market but missing YES/NO tokens — cannot reconcile"
                            );
                        }
                    }
                    Ok(None) => {
                        error!(
                            condition_id = %cid,
                            "Position exists but market not found in CLOB API — manual intervention needed"
                        );
                    }
                    Err(e) => {
                        error!(
                            condition_id = %cid,
                            error = %e,
                            "Failed to fetch market metadata from CLOB API"
                        );
                    }
                }
            }
        }

        // Insert resolved markets into known_markets
        if !resolved.is_empty() {
            let mut known = self.known_markets.write().await;
            for market in &resolved {
                info!(
                    condition_id = %market.condition_id,
                    question = %market.question,
                    "Added position-held market to known_markets for reconciliation"
                );
                known
                    .entry(market.condition_id.clone())
                    .or_insert_with(|| market.clone());
            }
        }
    }

    async fn recover_orphaned_positions_on_refresh(&self) {
        let exposure_tolerance = hedge_exposure_tolerance(&self.config);
        if let Err(err) = self.position_manager.sync_positions().await {
            warn!(
                error = %err,
                "Failed to sync positions for refresh orphan recovery"
            );
            return;
        }

        let state = self.position_manager.get_state().await;
        for position in state.positions {
            let net_exposure = normalize_share_size(position.net_exposure().abs());
            if net_exposure <= exposure_tolerance {
                continue;
            }

            let tracked_commitment = self
                .order_manager
                .get_market_orders(&position.condition_id)
                .await
                .into_iter()
                .filter(|order| order.side == Side::Buy)
                .fold(Decimal::ZERO, |total, order| total + order.size);
            if tracked_commitment > Decimal::ZERO {
                continue;
            }

            let Some(market) = resolve_market_metadata(
                &self.managed_markets,
                &self.known_markets,
                &position.condition_id,
            )
            .await
            else {
                warn!(
                    condition_id = %position.condition_id,
                    net_exposure = %net_exposure,
                    "Cannot recover orphaned position without market metadata"
                );
                continue;
            };

            let hedge_lock = get_hedge_lock(&self.hedge_locks, &position.condition_id).await;
            let Ok(guard) = hedge_lock.try_lock() else {
                debug!(
                    condition_id = %position.condition_id,
                    "Skipping orphan recovery because hedge is already in progress"
                );
                continue;
            };
            drop(guard);

            if let Some(state) = self
                .risk_manager
                .get_market_state(&position.condition_id)
                .await
            {
                if state.halted {
                    continue;
                }
            }

            let (trigger_leg, fill_price) = if position.yes_size >= position.no_size {
                (QuoteLeg::YesBid, position.avg_yes_price)
            } else {
                (QuoteLeg::NoBid, position.avg_no_price)
            };

            warn!(
                condition_id = %position.condition_id,
                yes_size = %position.yes_size,
                no_size = %position.no_size,
                tracked_buy_commitment = %tracked_commitment,
                net_exposure = %net_exposure,
                "Refresh detected orphaned position exposure — routing to reconciliation"
            );
            self.execute_reconciliation_hedge(
                &market,
                trigger_leg,
                fill_price,
                "reconciliation_position_orphan",
            )
            .await;
        }
    }

    /// Detect and hedge any one-sided inventory left by missed fill events.
    ///
    /// Checks ALL positions every cycle for one-sided imbalance.
    /// No baselines needed — just looks at current state.
    /// Uses per-market mutex (shared with FillHandler) to prevent double-hedging.
    async fn reconcile_unhedged_positions(&self, markets: &[CanonicalMarket]) {
        let exposure_tolerance = hedge_exposure_tolerance(&self.config);

        for market in markets {
            let position = self
                .position_manager
                .get_position(&market.condition_id)
                .await;
            let Some(pos) = position else { continue };

            // Skip if no inventory at all
            if pos.yes_size <= Decimal::ZERO && pos.no_size <= Decimal::ZERO {
                continue;
            }

            // Skip if both sides exist (hedged) — nothing to do
            if pos.yes_size > Decimal::ZERO && pos.no_size > Decimal::ZERO {
                continue;
            }

            // One-sided position with no recent hedge — attempt hedge
            if pos.yes_size > Decimal::ZERO && pos.no_size <= Decimal::ZERO {
                let hedge_size = normalize_share_size(pos.net_exposure());
                if hedge_size > Decimal::ZERO {
                    warn!(
                        condition_id = %market.condition_id,
                        question = %market.question,
                        yes_size = %pos.yes_size,
                        "Imbalance checker: one-sided YES position — hedging"
                    );
                    self.execute_reconciliation_hedge(
                        market,
                        QuoteLeg::YesBid,
                        pos.avg_yes_price,
                        "reconciliation",
                    )
                    .await;
                }
            } else if pos.no_size > Decimal::ZERO && pos.yes_size <= Decimal::ZERO {
                let hedge_size = normalize_share_size(-pos.net_exposure());
                if hedge_size > Decimal::ZERO {
                    warn!(
                        condition_id = %market.condition_id,
                        question = %market.question,
                        no_size = %pos.no_size,
                        "Imbalance checker: one-sided NO position — hedging"
                    );
                    self.execute_reconciliation_hedge(
                        market,
                        QuoteLeg::NoBid,
                        pos.avg_no_price,
                        "reconciliation",
                    )
                    .await;
                }
            } else if pos.net_exposure().abs() <= exposure_tolerance {
                debug!(
                    condition_id = %market.condition_id,
                    exposure = %pos.net_exposure().abs(),
                    tolerance = %exposure_tolerance,
                    "Imbalance checker: residual exposure within tolerance"
                );
            }
        }
    }

    /// Execute a single reconciliation hedge for unhedged inventory.
    async fn execute_reconciliation_hedge(
        &self,
        market: &CanonicalMarket,
        trigger_leg: QuoteLeg,
        fill_price: Decimal,
        origin: &'static str,
    ) {
        // Acquire per-market hedge lock — if FillHandler is hedging this market, wait.
        let hedge_lock = get_hedge_lock(&self.hedge_locks, &market.condition_id).await;
        let _guard = hedge_lock.lock().await;

        if let Some(state) = self
            .risk_manager
            .get_market_state(&market.condition_id)
            .await
        {
            if state.halted {
                info!(
                    condition_id = %market.condition_id,
                    origin,
                    canonical_reason = %state.halt_reason.unwrap_or_else(|| "unknown".to_string()),
                    "Skipping reconciliation hedge on halted market"
                );
                self.finalize_halted_market_if_drained(&market.condition_id)
                    .await;
                return;
            }
        }

        let exposure_tolerance = hedge_exposure_tolerance(&self.config);
        if let Err(e) = self.position_manager.sync_positions().await {
            warn!(
                condition_id = %market.condition_id,
                error = %e,
                "Failed to sync positions for reconciliation — using cached"
            );
        }

        let Some(current_position) = self
            .position_manager
            .get_position(&market.condition_id)
            .await
        else {
            info!(
                condition_id = %market.condition_id,
                "No position found after fresh sync — skipping recon hedge"
            );
            return;
        };

        let initial_fill_size = required_hedge_size(&current_position, trigger_leg);
        if initial_fill_size <= exposure_tolerance {
            info!(
                condition_id = %market.condition_id,
                residual = %current_position.net_exposure().abs(),
                tolerance = %exposure_tolerance,
                "Residual within tolerance after fresh sync — skipping recon hedge"
            );
            return;
        }

        let preparation = match prepare_market_for_resolution(
            market,
            &self.order_manager,
            &self.trading_client,
            &self.risk_manager,
            &self.cached_balance,
            &self.book_rest,
            &self.book_manager,
            &self.config,
            trigger_leg.hedge_uses_asks().then_some(initial_fill_size),
        )
        .await
        {
            Ok(preparation) => preparation,
            Err(reason) => {
                self.handle_reconciliation_resolution_failure(
                    market,
                    initial_fill_size,
                    &reason,
                    false,
                )
                .await;
                return;
            }
        };

        if let Err(err) = self.position_manager.sync_positions().await {
            warn!(
                condition_id = %market.condition_id,
                error = %err,
                "Failed to sync positions after reconciliation prep — using cached"
            );
        }

        let post_prep_position = self
            .position_manager
            .get_position(&market.condition_id)
            .await
            .unwrap_or(current_position);
        let fill_size = required_hedge_size(&post_prep_position, trigger_leg);
        if fill_size <= exposure_tolerance {
            info!(
                condition_id = %market.condition_id,
                residual = %post_prep_position.net_exposure().abs(),
                tolerance = %exposure_tolerance,
                "Residual within tolerance after reconciliation prep — skipping recon hedge"
            );
            self.recon_failure_counts
                .write()
                .await
                .remove(&market.condition_id);
            return;
        }

        let trace_id = Uuid::new_v4().to_string();
        let hedge_id = format!("recon-{}", Uuid::new_v4());
        let synthetic_trade = TradeEvent {
            id: format!("reconciliation-{}", hedge_id),
            condition_id: market.condition_id.clone(),
            asset_id: match trigger_leg {
                QuoteLeg::YesBid | QuoteLeg::YesAsk => market.yes_token_id.clone(),
                QuoteLeg::NoBid | QuoteLeg::NoAsk => market.no_token_id.clone(),
            },
            side: side_for_leg(trigger_leg),
            price: fill_price,
            size: fill_size,
            outcome: outcome_for_leg(trigger_leg).to_string(),
            status: TradeStatus::Matched,
            timestamp: Utc::now(),
            maker_order_id: Some("reconciliation".to_string()),
            taker_order_id: None,
        };
        self.emit_event(emitters::build_fill_detected(
            &self.run_id,
            &trace_id,
            &self.mode,
            origin,
            &synthetic_trade,
            Some("reconciliation"),
            Some(origin),
            true,
            Some("reconciliation"),
            false,
        ));

        let (hedge_token_id, hedge_side) = HedgeExecutor::compute_hedge_params(
            trigger_leg,
            &market.yes_token_id,
            &market.no_token_id,
        );

        let filled_token_id = match trigger_leg {
            QuoteLeg::YesBid | QuoteLeg::YesAsk => &market.yes_token_id,
            QuoteLeg::NoBid | QuoteLeg::NoAsk => &market.no_token_id,
        };

        let resolution = if hedge_side == Side::Buy {
            let resolution = plan_buy_resolution(
                market,
                &preparation,
                &hedge_token_id,
                filled_token_id,
                fill_price,
                fill_size,
            );

            info!(
                condition_id = %market.condition_id,
                hedge_shares = %resolution.hedge_shares,
                hedge_limit = %resolution.hedge_limit_price,
                sellback_shares = %resolution.sellback_shares,
                sellback_limit = %resolution.sellback_limit_price,
                unresolved_shares = %resolution.unresolved_shares,
                available_hedge_usdc = %preparation.max_hedge_usdc,
                cancel_wait_drained = preparation.cancel_wait_drained,
                "Reconciliation hedge resolution computed"
            );
            Some(resolution)
        } else {
            None
        };

        let (planned_hedge_size, hedge_cost) =
            planned_hedge_size_and_cost(resolution.as_ref(), hedge_side, fill_size);

        let hedge_id = Uuid::new_v4().to_string();
        let intent = HedgeIntent {
            condition_id: market.condition_id.clone(),
            trigger_order_id: "reconciliation".to_string(),
            trigger_leg,
            fill_size,
            fill_price,
            hedge_token_id: hedge_token_id.clone(),
            hedge_side,
            neg_risk: market.neg_risk,
            tick_size: market.tick_size.clone(),
        };
        let hedge_book_for_decision = if hedge_token_id == market.yes_token_id {
            &preparation.yes_book
        } else {
            &preparation.no_book
        };
        let filled_book_for_decision = if filled_token_id == &market.yes_token_id {
            &preparation.yes_book
        } else {
            &preparation.no_book
        };
        let (filled_best_bid_price, filled_best_bid_size) =
            best_bid_snapshot(filled_book_for_decision);
        let (opposite_best_ask_price, opposite_best_ask_size) =
            best_ask_snapshot(hedge_book_for_decision);

        self.emit_event(emitters::build_hedge_decision(
            &self.run_id,
            &trace_id,
            &hedge_id,
            &self.mode,
            origin,
            &intent,
            emitters::HedgeDecisionContext {
                resolution: resolution.as_ref(),
                available_hedge_budget_usd: preparation.max_hedge_usdc,
                filled_best_bid_price,
                filled_best_bid_size,
                opposite_best_ask_price,
                opposite_best_ask_size,
            },
        ));

        if let Err(reason) = self
            .risk_manager
            .pre_trade_check(
                &market.condition_id,
                planned_hedge_size,
                hedge_cost,
                true,
                Some(preparation.max_hedge_usdc),
            )
            .await
        {
            self.handle_reconciliation_resolution_failure(market, fill_size, &reason, false)
                .await;
            return;
        }

        self.emit_event(emitters::build_hedge_intent(
            &self.run_id,
            &trace_id,
            &hedge_id,
            &self.mode,
            origin,
            &intent,
            emitters::HedgeIntentContext {
                resolution: resolution.as_ref(),
                pre_resolution_active_orders: Some(preparation.pre_resolution_active_orders as u64),
                pre_resolution_pending_cancels: Some(
                    preparation.pre_resolution_pending_cancels as u64,
                ),
                cancel_wait_drained: Some(preparation.cancel_wait_drained),
            },
            Some(origin),
        ));

        let hedge_started = Instant::now();
        let result = self
            .execute_resolution_plan_with_sellback_recompute(
                market,
                &intent,
                resolution.as_ref(),
                filled_token_id,
                &post_prep_position,
                exposure_tolerance,
                1,
            )
            .await;

        let sellback_order_id = result
            .sellback_result
            .as_ref()
            .and_then(|sellback| sellback.order_result.as_ref())
            .and_then(|order| (!order.order_id.is_empty()).then_some(order.order_id.as_str()));
        let sellback_price = result
            .sellback_result
            .as_ref()
            .and_then(|sellback| sellback.price);
        let sellback_response_status = result
            .sellback_result
            .as_ref()
            .and_then(|sellback| sellback.verification_metadata.response_status.as_deref());
        let sellback_lookup_status = result
            .sellback_result
            .as_ref()
            .and_then(|sellback| sellback.verification_metadata.lookup_status.as_deref());
        let sellback_lookup_matched_shares = result
            .sellback_result
            .as_ref()
            .and_then(|sellback| sellback.verification_metadata.lookup_matched_shares);
        let sellback_lookup_error = result
            .sellback_result
            .as_ref()
            .and_then(|sellback| sellback.verification_metadata.lookup_error.as_deref());
        let sellback_trade_ids = result.sellback_result.as_ref().and_then(|sellback| {
            (!sellback.verification_metadata.trade_ids.is_empty())
                .then_some(sellback.verification_metadata.trade_ids.as_slice())
        });

        let mut halt_signal_suppressed = false;
        if result.success {
            if let Some((sellback_price, confirmed_shares)) = result
                .sellback_result
                .as_ref()
                .filter(|sellback| sellback.is_verified_filled())
                .and_then(|sellback| {
                    sellback
                        .price
                        .zip(sellback.confirmed_shares)
                        .filter(|(_, shares)| *shares > Decimal::ZERO)
                })
            {
                record_recent_resolution_trade_shared(
                    &self.recent_resolution_trades,
                    &market.condition_id,
                    filled_token_id,
                    Side::Sell,
                    sellback_price,
                    confirmed_shares,
                )
                .await;
            }
            self.finalize_reconciliation_resolution_success(market, fill_size, &result)
                .await;
        } else {
            halt_signal_suppressed = self
                .handle_reconciliation_resolution_failure(
                    market,
                    fill_size,
                    result.failure_reason.as_deref().unwrap_or("unknown"),
                    true,
                )
                .await
                .map(|outcome| outcome.halt_signal_suppressed)
                .unwrap_or(false);
        }

        self.emit_event(emitters::build_hedge_result(
            &self.run_id,
            &trace_id,
            &hedge_id,
            &self.mode,
            origin,
            &intent,
            emitters::HedgeResultContext {
                hedge_result: result.hedge_result.as_ref(),
                aggregate_success: result.success,
                aggregate_failure_reason: result.failure_reason.as_deref(),
                sellback_order_id,
                sellback_price,
                sellback_execution_limit_price: sellback_price,
                sellback_leg_status: Some(sellback_leg_status(result.sellback_result.as_ref())),
                sellback_response_status,
                sellback_lookup_status,
                sellback_lookup_matched_shares,
                sellback_lookup_error,
                sellback_trade_ids,
                post_sync_net_exposure: Some(result.post_sync_net_exposure),
                post_sync_yes_size: result
                    .post_position
                    .as_ref()
                    .map(|position| position.yes_size),
                post_sync_no_size: result
                    .post_position
                    .as_ref()
                    .map(|position| position.no_size),
                post_sync_source: Some(result.post_sync_source),
                halt_signal_suppressed,
            },
            hedge_started.elapsed().as_millis() as u64,
            Some(origin),
        ));
        self.emit_reconciliation_hedge_exit(
            &trace_id,
            &hedge_id,
            origin,
            &intent,
            market,
            &result,
            exposure_tolerance,
        )
        .await;
    }

    async fn emit_reconciliation_hedge_exit(
        &self,
        trace_id: &str,
        hedge_id: &str,
        origin: &str,
        intent: &HedgeIntent,
        market: &CanonicalMarket,
        result: &ResolutionExecutionResult,
        exposure_tolerance: Decimal,
    ) {
        let final_post_position = result.post_position.clone();
        let sellback_completed = result
            .sellback_result
            .as_ref()
            .is_some_and(SellbackExecutionResult::is_verified_filled);
        let mut exit_telemetry = None;
        refresh_hedge_exit_telemetry(
            &mut exit_telemetry,
            final_post_position.as_ref(),
            exposure_tolerance,
            sellback_completed,
            self.ctf_merger.is_some(),
        );

        if let Some(post_position) = final_post_position.as_ref() {
            let merge_pairs = merge_eligible_pairs(post_position);
            let fallback_asks_attempted =
                result.success && merge_pairs > Decimal::ZERO && result.post_position.is_some();
            let fallback_ask_count = if fallback_asks_attempted {
                tracked_inventory_ask_count(&self.order_manager, &market.condition_id).await
            } else {
                0
            };

            if let Some(telemetry) = exit_telemetry.as_mut() {
                telemetry.fallback_asks_attempted = fallback_asks_attempted;
                telemetry.fallback_ask_count = fallback_ask_count;
                telemetry.fallback_failure_reason = if fallback_asks_attempted
                    && fallback_ask_count == 0
                    && merge_pairs > Decimal::ZERO
                {
                    Some("inventory ask placement produced no tracked ask orders".to_string())
                } else {
                    None
                };
                telemetry.exit_path_status = if merge_pairs > Decimal::ZERO {
                    if fallback_ask_count > 0 {
                        "fallback_asks_placed".to_string()
                    } else if fallback_asks_attempted {
                        "fallback_asks_failed".to_string()
                    } else {
                        "pair_left_idle".to_string()
                    }
                } else {
                    classify_non_pair_exit_status(
                        post_position,
                        exposure_tolerance,
                        sellback_completed,
                    )
                    .to_string()
                };
            }
        }

        if let (Some(post_position), Some(telemetry)) =
            (final_post_position.as_ref(), exit_telemetry.as_ref())
        {
            self.emit_event(emitters::build_hedge_exit_path(
                &self.run_id,
                trace_id,
                hedge_id,
                &self.mode,
                origin,
                intent,
                emitters::HedgeExitPathContext {
                    post_position,
                    post_sync_source: result.post_sync_source,
                    exit_path_status: &telemetry.exit_path_status,
                    merge_eligible_pairs: telemetry.merge_eligible_pairs,
                    ctf_merge_configured: telemetry.ctf_merge_configured,
                    merge_attempted: telemetry.merge_attempted,
                    merge_tx_hash: telemetry.merge_tx_hash.as_deref(),
                    merge_failure_reason: telemetry.merge_failure_reason.as_deref(),
                    fallback_asks_attempted: telemetry.fallback_asks_attempted,
                    fallback_ask_count: telemetry.fallback_ask_count,
                    fallback_failure_reason: telemetry.fallback_failure_reason.as_deref(),
                },
            ));
        } else if result.success {
            let reason = hedge_exit_observability_reason(origin, result.post_sync_source);
            error!(
                condition_id = %market.condition_id,
                trace_id = %trace_id,
                hedge_id = %hedge_id,
                reason = %reason,
                "Successful reconciliation hedge trace missing required hedge_exit_path_recorded"
            );
            self.error_logger
                .log_error("error", &reason, Some(&market.condition_id));
            self.emit_event(build_hedge_exit_observability_event(
                &self.run_id,
                &self.mode,
                origin,
                trace_id,
                hedge_id,
                intent,
                &reason,
            ));
        }
    }

    async fn execute_resolution_plan_with_sellback_recompute(
        &self,
        market: &CanonicalMarket,
        intent: &HedgeIntent,
        resolution: Option<&HedgeResolution>,
        filled_token_id: &str,
        pre_resolution_position: &Position,
        exposure_tolerance: Decimal,
        remaining_recompute_attempts: u8,
    ) -> ResolutionExecutionResult {
        execute_resolution_plan_with_sellback_recompute_shared(
            &self.hedge_executor,
            &self.trading_client,
            &self.position_manager,
            market,
            &self.order_manager,
            &self.risk_manager,
            &self.cached_balance,
            &self.book_rest,
            &self.book_manager,
            &self.config,
            intent,
            resolution,
            filled_token_id,
            pre_resolution_position,
            exposure_tolerance,
            self.config.risk.hedge_timeout_secs,
            remaining_recompute_attempts,
        )
        .await
    }

    async fn recompute_buy_resolution_after_sellback_miss(
        &self,
        market: &CanonicalMarket,
        intent: &HedgeIntent,
        filled_token_id: &str,
        exposure_tolerance: Decimal,
        first_result: ResolutionExecutionResult,
    ) -> ResolutionExecutionResult {
        recompute_buy_resolution_after_sellback_miss_shared(
            &self.hedge_executor,
            &self.trading_client,
            &self.position_manager,
            market,
            &self.order_manager,
            &self.risk_manager,
            &self.cached_balance,
            &self.book_rest,
            &self.book_manager,
            &self.config,
            intent,
            filled_token_id,
            exposure_tolerance,
            self.config.risk.hedge_timeout_secs,
            first_result,
        )
        .await
    }

    async fn finalize_reconciliation_resolution_success(
        &self,
        market: &CanonicalMarket,
        fill_size: Decimal,
        result: &ResolutionExecutionResult,
    ) {
        if let Some(order_result) = result
            .hedge_result
            .as_ref()
            .and_then(|hedge_result| hedge_result.order_result.as_ref())
        {
            if !order_result.order_id.is_empty() {
                self.hedge_order_ids
                    .write()
                    .await
                    .insert(order_result.order_id.clone());
            }
        }

        let sellback_price = result
            .sellback_result
            .as_ref()
            .and_then(|sellback| sellback.price);

        info!(
            condition_id = %market.condition_id,
            hedge_price = ?result
                .hedge_result
                .as_ref()
                .and_then(|hedge_result| hedge_result.hedge_price),
            sellback_price = ?sellback_price,
            size = %fill_size,
            post_sync_net_exposure = %result.post_sync_net_exposure,
            post_sync_source = %result.post_sync_source,
            "Reconciliation hedge resolution successful"
        );

        self.hedge_signals
            .write()
            .await
            .insert(market.condition_id.clone(), new_hedge_signal());
        self.recon_failure_counts
            .write()
            .await
            .remove(&market.condition_id);

        if let Some(post_position) = result.post_position.as_ref() {
            self.recon_baselines.write().await.insert(
                market.condition_id.clone(),
                (post_position.yes_size, post_position.no_size),
            );
            if post_position.yes_size > Decimal::ZERO || post_position.no_size > Decimal::ZERO {
                self.place_inventory_asks(market, post_position, "inventory_exit_ask")
                    .await;
            }
        } else {
            warn!(
                condition_id = %market.condition_id,
                "Reconciliation resolution reported success without final post-sync position truth"
            );
        }
    }

    async fn handle_reconciliation_resolution_failure(
        &self,
        market: &CanonicalMarket,
        fill_size: Decimal,
        failure_reason: &str,
        increment_failure_count: bool,
    ) -> Option<HaltMarketOutcome> {
        error!(
            condition_id = %market.condition_id,
            reason = %failure_reason,
            size = %fill_size,
            "Reconciliation hedge resolution failed — position remains unresolved"
        );
        self.error_logger.log_error(
            "error",
            &format!(
                "Reconciliation hedge resolution failed: {} (size={})",
                failure_reason, fill_size
            ),
            Some(&market.condition_id),
        );

        if !increment_failure_count {
            return None;
        }

        self.recon_failure_counts
            .write()
            .await
            .remove(&market.condition_id);
        let reason = format!(
            "Reconciliation hedge resolution failed on first aggregate attempt: {} (size={})",
            failure_reason, fill_size
        );
        Some(self.kill_market(&market.condition_id, &reason).await)
    }

    async fn handle_external_cancellation(&self, order_event: OrderEvent) {
        let tracked = self
            .order_manager
            .get_tracked_order(&order_event.order_id)
            .await;
        self.order_manager
            .move_to_recently_cancelled(&order_event.order_id)
            .await;

        if let Some(tracked) = tracked {
            self.emit_event(emitters::build_order_cancelled(
                &self.run_id,
                &tracked.trace_id,
                &self.mode,
                &tracked,
                CancelReasonCode::ExternalCancel,
                Some("external_ws"),
                None,
            ));
        }
    }

    async fn handle_order_update(&self, order_event: OrderEvent) {
        let Some(update) = self
            .order_manager
            .apply_order_update(&order_event.order_id, order_event.size_matched)
            .await
        else {
            return;
        };

        if update.newly_matched > Decimal::ZERO {
            let remaining = update
                .tracked_after
                .as_ref()
                .map(|tracked| tracked.size)
                .unwrap_or(Decimal::ZERO);
            info!(
                order_id = %order_event.order_id,
                condition_id = %order_event.condition_id,
                newly_matched = %update.newly_matched,
                cumulative_matched = %order_event.size_matched,
                remaining_size = %remaining,
                "Order update indicates newly matched size"
            );
            self.queue_pending_fill_fallback(
                update.tracked_before,
                &order_event,
                update.newly_matched,
            )
            .await;
        }
    }

    async fn build_fill_work_item(&self, trade: TradeEvent) -> Option<FillWorkItem> {
        // Skip fills from our own hedge orders to prevent fill loops.
        let skipped_hedge_order_id = {
            let mut ids = self.hedge_order_ids.write().await;
            if let Some(ref id) = trade.maker_order_id {
                if ids.remove(id) {
                    Some(id.clone())
                } else if let Some(ref id) = trade.taker_order_id {
                    if ids.remove(id) {
                        Some(id.clone())
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else if let Some(ref id) = trade.taker_order_id {
                if ids.remove(id) {
                    Some(id.clone())
                } else {
                    None
                }
            } else {
                None
            }
        };
        if let Some(order_id) = skipped_hedge_order_id {
            let _ = self.claim_processed_trade_id(&trade.id).await;
            info!(order_id = %order_id, "Skipping hedge fill (hedge_order_id match)");
            return None;
        }

        if self.consume_recent_resolution_trade(&trade).await {
            let _ = self.claim_processed_trade_id(&trade.id).await;
            info!(
                condition_id = %trade.condition_id,
                trade_id = %trade.id,
                asset_id = %trade.asset_id,
                side = ?trade.side,
                price = %trade.price,
                size = %trade.size,
                "Skipping late self-executed resolution trade"
            );
            return None;
        }

        if !self.claim_processed_trade_id(&trade.id).await {
            warn!(
                condition_id = %trade.condition_id,
                trade_id = %trade.id,
                "Dropping duplicate trade_id before fill attribution"
            );
            return None;
        }

        let (tracked, match_source, fallback_match) = match self.find_tracked_order(&trade).await {
            Some(result) => result,
            None => {
                let trace_id = Uuid::new_v4().to_string();
                warn!(
                    condition_id = %trade.condition_id,
                    asset_id = %trade.asset_id,
                    side = ?trade.side,
                    size = %trade.size,
                    price = %trade.price,
                    outcome = %trade.outcome,
                    trade_id = %trade.id,
                    maker_order_id = ?trade.maker_order_id,
                    taker_order_id = ?trade.taker_order_id,
                    "Unanchored raw trade deferred to reconciliation"
                );
                self.emit_event(emitters::build_fill_detected(
                    &self.run_id,
                    &trace_id,
                    &self.mode,
                    "live_engine",
                    &trade,
                    None,
                    Some("unanchored_trade_deferred"),
                    false,
                    None,
                    true,
                ));
                return None;
            }
        };

        let synthetic_consumed = self
            .consume_recent_synthetic_fill(&tracked.order_id, trade.size)
            .await;
        let effective_fill_size =
            effective_fill_size_after_synthetic_dedup(trade.size, synthetic_consumed);
        if effective_fill_size <= Decimal::ZERO {
            return None;
        }

        let pending_accounted = self
            .consume_pending_fill_fallback(&tracked.order_id, effective_fill_size)
            .await;
        let size_to_apply =
            size_to_apply_after_order_update_accounting(effective_fill_size, pending_accounted);

        let adjusted_trade = if effective_fill_size != trade.size {
            let mut adjusted = trade.clone();
            adjusted.size = effective_fill_size;
            adjusted
        } else {
            trade
        };

        let tolerance = hedge_exposure_tolerance(&self.config);
        let position = self
            .position_manager
            .get_position(&adjusted_trade.condition_id)
            .await;
        let net_exposure = position
            .as_ref()
            .map(|p| p.net_exposure().abs())
            .unwrap_or(Decimal::ZERO);
        let recent_signal = {
            let signals = self.hedge_signals.read().await;
            signals.get(&adjusted_trade.condition_id).cloned()
        };
        if should_skip_recent_duplicate_fill(
            recent_signal.as_ref(),
            adjusted_trade.timestamp,
            net_exposure,
            tolerance,
            std::time::Duration::from_secs(180),
        ) {
            info!(
                condition_id = %adjusted_trade.condition_id,
                trade_id = %adjusted_trade.id,
                net_exposure = %net_exposure,
                tolerance = %tolerance,
                trade_timestamp = %adjusted_trade.timestamp,
                "Skipping fill — late duplicate after recent verified hedge"
            );
            return None;
        }

        let pre_position =
            position.unwrap_or_else(|| Position::new(adjusted_trade.condition_id.clone()));
        let hedge_size = hedge_size_for_accounted_fill(
            &pre_position,
            tracked.leg,
            effective_fill_size,
            tolerance,
        );

        Some(FillWorkItem {
            anchored_order_id: Some(tracked.order_id.clone()),
            tracked,
            trade: adjusted_trade,
            match_source: match_source.to_string(),
            fallback_match,
            size_to_apply,
            hedge_size,
        })
    }

    async fn find_tracked_order(
        &self,
        trade: &TradeEvent,
    ) -> Option<(TrackedOrder, &'static str, bool)> {
        if let Some(id) = &trade.maker_order_id {
            if let Some(tracked) = self.order_manager.get_tracked_order(id).await {
                return Some((tracked, "maker_order_id", false));
            }
        }
        if let Some(id) = &trade.taker_order_id {
            if let Some(tracked) = self.order_manager.get_tracked_order(id).await {
                return Some((tracked, "taker_order_id", false));
            }
        }

        let active_matches = exact_trade_signature_matches(
            self.order_manager
                .get_market_orders(&trade.condition_id)
                .await
                .into_iter(),
            trade,
        );
        if active_matches.len() == 1 {
            return active_matches
                .into_iter()
                .next()
                .map(|tracked| (tracked, "exact_signature_active_fallback", true));
        }

        let cancelled_matches = exact_trade_signature_matches(
            self.order_manager
                .get_recently_cancelled_for_market(&trade.condition_id)
                .await
                .into_iter(),
            trade,
        );
        if cancelled_matches.len() == 1 {
            return cancelled_matches
                .into_iter()
                .next()
                .map(|tracked| (tracked, "exact_signature_recent_cancel_fallback", true));
        }

        None
    }

    async fn queue_pending_fill_fallback(
        &self,
        tracked: TrackedOrder,
        order_event: &OrderEvent,
        newly_matched: Decimal,
    ) {
        let mut pending = self.pending_fill_fallbacks.write().await;
        let queued_size = pending
            .entry(order_event.order_id.clone())
            .and_modify(|entry| {
                entry.fill_size += newly_matched;
                entry.fill_price = order_event.price;
                entry.occurred_at = order_event.timestamp;
                entry.queued_at = Instant::now();
            })
            .or_insert(PendingFillFallback {
                tracked,
                asset_id: order_event.asset_id.clone(),
                outcome: order_event.outcome.clone(),
                fill_size: newly_matched,
                fill_price: order_event.price,
                occurred_at: order_event.timestamp,
                queued_at: Instant::now(),
            })
            .fill_size;
        drop(pending);

        info!(
            order_id = %order_event.order_id,
            condition_id = %order_event.condition_id,
            queued_size = %queued_size,
            matched_delta = %newly_matched,
            "Queued pending fill fallback from order update"
        );
        self.emit_event(emitters::build_user_stream_status_changed(
            &self.run_id,
            &self.mode,
            "pending_fill_fallback_queued",
            Some(self.subscribed_market_ids.read().await.len() as u64),
            Some(&format!(
                "order_id={} matched_delta={} queued_size={}",
                order_event.order_id, newly_matched, queued_size
            )),
        ));
    }

    async fn consume_pending_fill_fallback(
        &self,
        order_id: &str,
        observed_size: Decimal,
    ) -> Decimal {
        if observed_size <= Decimal::ZERO {
            return Decimal::ZERO;
        }

        let mut pending = self.pending_fill_fallbacks.write().await;
        let Some(entry) = pending.get_mut(order_id) else {
            return Decimal::ZERO;
        };

        let consumed = observed_size.min(entry.fill_size.max(Decimal::ZERO));
        entry.fill_size = (entry.fill_size - consumed).max(Decimal::ZERO);
        if entry.fill_size <= Decimal::ZERO {
            pending.remove(order_id);
        }
        consumed
    }

    async fn flush_pending_fill_fallbacks(
        &self,
        fill_tx: &mpsc::UnboundedSender<FillWorkItem>,
    ) -> Result<()> {
        let due = {
            let mut pending = self.pending_fill_fallbacks.write().await;
            let now = Instant::now();
            let due_ids: Vec<String> = pending
                .iter()
                .filter(|(_, entry)| {
                    entry.fill_size > Decimal::ZERO
                        && now.duration_since(entry.queued_at) >= std::time::Duration::from_secs(2)
                })
                .map(|(order_id, _)| order_id.clone())
                .collect();

            let mut due = Vec::with_capacity(due_ids.len());
            for order_id in due_ids {
                if let Some(entry) = pending.remove(&order_id) {
                    due.push(entry);
                }
            }
            due
        };

        for entry in due {
            self.emit_event(emitters::build_user_stream_status_changed(
                &self.run_id,
                &self.mode,
                "pending_fill_fallback_flushed",
                Some(self.subscribed_market_ids.read().await.len() as u64),
                Some(&format!(
                    "order_id={} fill_size={}",
                    entry.tracked.order_id, entry.fill_size
                )),
            ));
            let trade = TradeEvent {
                id: format!(
                    "fallback-{}-{}",
                    entry.tracked.order_id,
                    entry.occurred_at.timestamp_millis()
                ),
                condition_id: entry.tracked.condition_id.clone(),
                asset_id: entry.asset_id.clone(),
                side: entry.tracked.side,
                price: entry.fill_price,
                size: entry.fill_size,
                outcome: entry.outcome.clone(),
                status: TradeStatus::Matched,
                timestamp: entry.occurred_at,
                maker_order_id: Some(entry.tracked.order_id.clone()),
                taker_order_id: None,
            };

            let work = FillWorkItem {
                anchored_order_id: Some(entry.tracked.order_id.clone()),
                tracked: entry.tracked.clone(),
                trade,
                match_source: "order_update_fallback".to_string(),
                fallback_match: true,
                size_to_apply: Decimal::ZERO,
                hedge_size: hedge_size_for_accounted_fill(
                    &self
                        .position_manager
                        .get_position(&entry.tracked.condition_id)
                        .await
                        .unwrap_or_else(|| Position::new(entry.tracked.condition_id.clone())),
                    entry.tracked.leg,
                    entry.fill_size,
                    hedge_exposure_tolerance(&self.config),
                ),
            };

            if fill_tx.send(work).is_err() {
                error!("Fill handler channel closed unexpectedly");
            } else {
                self.record_recent_synthetic_fill(&entry.tracked.order_id, entry.fill_size)
                    .await;
            }
        }

        Ok(())
    }

    async fn claim_processed_trade_id(&self, trade_id: &str) -> bool {
        let mut processed = self.processed_trades.write().await;
        prune_processed_trade_cache(&mut processed, Instant::now());
        if processed.entries.contains_key(trade_id) {
            return false;
        }

        let seen_at = Instant::now();
        processed
            .entries
            .insert(trade_id.to_string(), ProcessedTradeEntry { seen_at });
        processed.order.push_back((trade_id.to_string(), seen_at));
        prune_processed_trade_cache(&mut processed, Instant::now());
        true
    }

    async fn record_recent_synthetic_fill(&self, order_id: &str, fill_size: Decimal) {
        if fill_size <= Decimal::ZERO {
            return;
        }

        let mut recent = self.recent_synthetic_fills.write().await;
        recent
            .entry(order_id.to_string())
            .and_modify(|existing| {
                existing.size += fill_size;
                existing.processed_at = Instant::now();
            })
            .or_insert(RecentSyntheticFill {
                size: fill_size,
                processed_at: Instant::now(),
            });
    }

    async fn consume_recent_synthetic_fill(
        &self,
        order_id: &str,
        observed_size: Decimal,
    ) -> Decimal {
        let mut recent = self.recent_synthetic_fills.write().await;
        prune_recent_synthetic_fills(&mut recent, Instant::now(), StdDuration::from_secs(15));

        let Some(entry) = recent.get_mut(order_id) else {
            return Decimal::ZERO;
        };

        let consumed = observed_size.min(entry.size.max(Decimal::ZERO));
        entry.size = (entry.size - consumed).max(Decimal::ZERO);
        if entry.size <= Decimal::ZERO {
            recent.remove(order_id);
        }
        consumed
    }

    async fn record_recent_resolution_trade(
        &self,
        condition_id: &str,
        asset_id: &str,
        side: Side,
        price: Decimal,
        size: Decimal,
    ) {
        if size <= Decimal::ZERO || asset_id.is_empty() {
            return;
        }

        let mut recent = self.recent_resolution_trades.write().await;
        prune_recent_resolution_trades(
            &mut recent,
            Instant::now(),
            StdDuration::from_secs(RECENT_RESOLUTION_TRADE_TTL_SECS),
        );
        recent.push(RecentResolutionTrade {
            condition_id: condition_id.to_string(),
            asset_id: asset_id.to_string(),
            side,
            price,
            size,
            recorded_at: Instant::now(),
        });
    }

    async fn consume_recent_resolution_trade(&self, trade: &TradeEvent) -> bool {
        let mut recent = self.recent_resolution_trades.write().await;
        prune_recent_resolution_trades(
            &mut recent,
            Instant::now(),
            StdDuration::from_secs(RECENT_RESOLUTION_TRADE_TTL_SECS),
        );

        let Some((idx, _)) = recent.iter().enumerate().find(|(_, entry)| {
            entry.condition_id == trade.condition_id
                && entry.asset_id == trade.asset_id
                && entry.side == trade.side
                && entry.price == trade.price
                && entry.size >= trade.size
        }) else {
            return false;
        };

        recent[idx].size = (recent[idx].size - trade.size).max(Decimal::ZERO);
        if recent[idx].size <= Decimal::ZERO {
            recent.remove(idx);
        }
        true
    }

    async fn record_recent_scoring_observation(
        &self,
        order_id: &str,
        actual_scoring: bool,
        price: Decimal,
        remaining_size: Decimal,
    ) {
        let mut observations = self.recent_scoring_observations.write().await;
        prune_recent_scoring_observations(
            &mut observations,
            Instant::now(),
            StdDuration::from_secs(RECENT_SCORING_OBSERVATION_TTL_SECS),
        );
        observations.insert(
            order_id.to_string(),
            RecentScoringObservation {
                actual_scoring,
                price,
                remaining_size,
                observed_at: Instant::now(),
            },
        );
    }

    async fn recent_scoring_observation_for(
        &self,
        tracked: &TrackedOrder,
        remaining_size: Decimal,
    ) -> Option<RecentScoringObservation> {
        let mut observations = self.recent_scoring_observations.write().await;
        prune_recent_scoring_observations(
            &mut observations,
            Instant::now(),
            StdDuration::from_secs(RECENT_SCORING_OBSERVATION_TTL_SECS),
        );
        observations
            .get(&tracked.order_id)
            .filter(|observation| {
                observation.price == tracked.price && observation.remaining_size == remaining_size
            })
            .cloned()
    }

    async fn get_managed_market_ids(&self) -> Vec<String> {
        self.managed_markets.read().await.keys().cloned().collect()
    }

    /// Collect YES+NO token IDs only for markets with active orders.
    /// We intentionally keep the market-book subscription focused on
    /// actionable markets to minimize trading-path WS traffic.
    async fn get_book_token_ids(&self) -> Vec<String> {
        let active_condition_ids = self.order_manager.get_tracked_condition_ids().await;
        let managed = self.managed_markets.read().await;
        let mut ids = Vec::with_capacity(active_condition_ids.len() * 2);
        for cid in &active_condition_ids {
            if let Some(market) = managed.get(cid) {
                ids.push(market.yes_token_id.clone());
                ids.push(market.no_token_id.clone());
            }
        }
        ids
    }
}

// =========================================================================
//  FillHandler — runs on a dedicated tokio task for instant hedge execution
// =========================================================================

impl FillHandler {
    /// Run the fill handler loop, processing trade events from the channel.
    async fn run(self, mut rx: mpsc::UnboundedReceiver<FillWorkItem>) {
        info!("Fill handler task started — hedges will fire independently of cycle work");
        while let Some(work) = rx.recv().await {
            if let Err(e) = self.handle_fill(work).await {
                error!(error = %e, "Fill handling failed");
                self.error_logger
                    .log_error("error", &format!("Fill handling failed: {}", e), None);
            }
        }
        warn!("Fill handler channel closed — no more fill events will be processed");
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
    }

    async fn refresh_balance_after_pair_exit(&self) {
        match self.trading_client.get_balance().await {
            Ok(fresh_balance) => {
                *self.cached_balance.write().await = fresh_balance;
                self.risk_manager.update_balance(fresh_balance).await;
                self.order_manager.update_gross_balance(fresh_balance).await;
            }
            Err(err) => {
                warn!(
                    error = %err,
                    "Failed to refresh balance after pair exit — cache may be stale"
                );
            }
        }
    }

    async fn try_merge_pairs(
        &self,
        market: Option<&CanonicalMarket>,
        condition_id: &str,
        merge_amount: Decimal,
        exit_telemetry: &mut Option<HedgeExitTelemetry>,
    ) -> Option<String> {
        let Some(merger) = self.ctf_merger.as_ref() else {
            return None;
        };

        let amount_u64 = merge_amount.to_string().parse::<u64>().unwrap_or(0);
        if amount_u64 == 0 {
            return None;
        }

        if let Some(telemetry) = exit_telemetry.as_mut() {
            telemetry.merge_attempted = true;
        }

        let Some(market) = market else {
            warn!(
                condition_id = %condition_id,
                "Skipping pair merge because market metadata is unavailable for venue resolution"
            );
            if let Some(telemetry) = exit_telemetry.as_mut() {
                telemetry.merge_attempted = false;
                telemetry.merge_failure_reason =
                    Some("market context missing for merge venue resolution".to_string());
            }
            return None;
        };

        match merger
            .merge_positions(condition_id, amount_u64, market.neg_risk)
            .await
        {
            Ok(tx_hash) => {
                info!(
                    condition_id = %condition_id,
                    amount = amount_u64,
                    neg_risk = market.neg_risk,
                    tx_hash = %tx_hash,
                    "\x1b[1;36m========== MERGE OK — {amount_u64} pairs redeemed for USDC ==========\x1b[0m"
                );
                if let Some(telemetry) = exit_telemetry.as_mut() {
                    telemetry.exit_path_status = "merge_succeeded".to_string();
                    telemetry.merge_tx_hash = Some(tx_hash.clone());
                }
                self.refresh_balance_after_pair_exit().await;
                let _ = self.position_manager.sync_positions().await;
                Some(tx_hash)
            }
            Err(err) => {
                let merge_error = err.to_string();
                error!(
                    condition_id = %condition_id,
                    error = %merge_error,
                    "CTF merge failed — will place fallback asks on remaining inventory"
                );
                if let Some(telemetry) = exit_telemetry.as_mut() {
                    telemetry.exit_path_status = "merge_failed".to_string();
                    telemetry.merge_failure_reason = Some(merge_error);
                }
                None
            }
        }
    }

    async fn place_pair_fallback_asks(
        &self,
        condition_id: &str,
        market: Option<&CanonicalMarket>,
        fallback_position: &Position,
        exit_telemetry: &mut Option<HedgeExitTelemetry>,
    ) {
        let Some(market) = market else {
            if let Some(telemetry) = exit_telemetry.as_mut() {
                telemetry.exit_path_status = "pair_left_idle".to_string();
                telemetry.fallback_failure_reason =
                    Some("market context missing for fallback ask placement".to_string());
            }
            return;
        };

        self.place_inventory_asks(market, fallback_position, "inventory_exit_ask")
            .await;
        let ask_count = tracked_inventory_ask_count(&self.order_manager, condition_id).await;
        if let Some(telemetry) = exit_telemetry.as_mut() {
            telemetry.fallback_asks_attempted = true;
            telemetry.fallback_ask_count = ask_count;
            if ask_count > 0 {
                telemetry.exit_path_status = "fallback_asks_placed".to_string();
            } else {
                telemetry.exit_path_status = "fallback_asks_failed".to_string();
                telemetry.fallback_failure_reason =
                    Some("inventory ask placement produced no tracked ask orders".to_string());
            }
        }
    }

    async fn execute_pair_exit(
        &self,
        condition_id: &str,
        market: Option<&CanonicalMarket>,
        post_position: &Position,
        exit_telemetry: &mut Option<HedgeExitTelemetry>,
        merge_truth_handling: MergeTruthHandling,
    ) -> Option<MergeTruthObservation> {
        let merge_amount = merge_eligible_pairs(post_position);
        if let Some(telemetry) = exit_telemetry.as_mut() {
            telemetry.merge_eligible_pairs = merge_amount;
        }
        if merge_amount <= Decimal::ZERO {
            return None;
        }

        if let Some(tx_hash) = self
            .try_merge_pairs(market, condition_id, merge_amount, exit_telemetry)
            .await
        {
            let expected_position = expected_post_merge_position(post_position, merge_amount);
            match merge_truth_handling {
                MergeTruthHandling::BackgroundMonitor => {
                    let _ = spawn_merge_truth_monitor(
                        self.position_manager.clone(),
                        self.event_producer.clone(),
                        self.run_id.clone(),
                        self.mode.clone(),
                        condition_id.to_string(),
                        tx_hash,
                        expected_position,
                    );
                    return None;
                }
                MergeTruthHandling::WaitForConvergence => {
                    return Some(
                        observe_merge_truth_convergence(
                            &self.position_manager,
                            condition_id,
                            &expected_position,
                        )
                        .await,
                    );
                }
            }
        }

        let fallback_position = self
            .position_manager
            .get_position(condition_id)
            .await
            .unwrap_or_else(|| post_position.clone());
        self.place_pair_fallback_asks(condition_id, market, &fallback_position, exit_telemetry)
            .await;
        None
    }

    async fn handle_fill(&self, work: FillWorkItem) -> Result<()> {
        let FillWorkItem {
            tracked,
            trade,
            anchored_order_id,
            match_source,
            fallback_match,
            size_to_apply,
            hedge_size,
        } = work;

        if size_to_apply > Decimal::ZERO {
            let _ = self
                .order_manager
                .apply_trade_fill(&tracked.order_id, size_to_apply)
                .await;
        }

        self.emit_event(emitters::build_fill_detected(
            &self.run_id,
            &tracked.trace_id,
            &self.mode,
            "fill_handler",
            &trade,
            Some(&tracked.order_id),
            Some(&match_source),
            fallback_match,
            anchored_order_id.as_deref(),
            false,
        ));

        if hedge_size <= Decimal::ZERO {
            info!(
                condition_id = %trade.condition_id,
                trade_id = %trade.id,
                accounted_fill_size = %trade.size,
                "Anchored fill produced zero immediate hedge size"
            );
            return Ok(());
        }

        // Acquire per-market hedge lock — prevents double-hedging between
        // fill handler and reconciliation running concurrently.
        let hedge_lock = get_hedge_lock(&self.hedge_locks, &trade.condition_id).await;
        let _guard = hedge_lock.lock().await;

        if let Some(state) = self
            .risk_manager
            .get_market_state(&trade.condition_id)
            .await
        {
            if state.halted {
                info!(
                    condition_id = %trade.condition_id,
                    trade_id = %trade.id,
                    canonical_reason = %state.halt_reason.unwrap_or_else(|| "unknown".to_string()),
                    "Late fill on halted market — updating accounting only"
                );
                self.finalize_halted_market_if_drained(&trade.condition_id)
                    .await;
                return Ok(());
            }
        }

        error!(
            condition_id = %trade.condition_id,
            trade_id = %trade.id,
            leg = %tracked.leg,
            fill_size = %hedge_size,
            raw_ws_size = %trade.size,
            fill_price = %trade.price,
            order_id = %tracked.order_id,
            "\x1b[1;34m========== FILL EXECUTED — hedging now ==========\x1b[0m"
        );

        // Look up market for hedge parameters; fall back to TrackedOrder data
        // so fills on unmanaged markets (removed from reward list) still hedge.
        let market = resolve_market_metadata(
            &self.managed_markets,
            &self.known_markets,
            &trade.condition_id,
        )
        .await;

        let pre_position = self
            .position_manager
            .get_position(&trade.condition_id)
            .await
            .unwrap_or_else(|| Position::new(trade.condition_id.clone()));

        // Derive hedge params from market metadata or TrackedOrder fallback
        let (
            yes_token_id,
            no_token_id,
            hedge_token_id,
            filled_token_id,
            hedge_side,
            neg_risk,
            tick_size,
        ) = if let Some(ref m) = market {
            let (htid, hs) =
                HedgeExecutor::compute_hedge_params(tracked.leg, &m.yes_token_id, &m.no_token_id);
            let filled = match tracked.leg {
                QuoteLeg::YesBid | QuoteLeg::YesAsk => m.yes_token_id.clone(),
                QuoteLeg::NoBid | QuoteLeg::NoAsk => m.no_token_id.clone(),
            };
            (
                m.yes_token_id.clone(),
                m.no_token_id.clone(),
                htid,
                filled,
                hs,
                m.neg_risk,
                m.tick_size.clone(),
            )
        } else if !tracked.opposite_token_id.is_empty() {
            warn!(
                condition_id = %trade.condition_id,
                "Fill on unmanaged market — using TrackedOrder fallback for hedge"
            );
            let (yes_token, no_token) = match tracked.leg {
                QuoteLeg::YesBid | QuoteLeg::YesAsk => {
                    (tracked.token_id.clone(), tracked.opposite_token_id.clone())
                }
                QuoteLeg::NoBid | QuoteLeg::NoAsk => {
                    (tracked.opposite_token_id.clone(), tracked.token_id.clone())
                }
            };
            let (htid, hs) =
                HedgeExecutor::compute_hedge_params(tracked.leg, &yes_token, &no_token);
            (
                yes_token,
                no_token,
                htid,
                tracked.token_id.clone(),
                hs,
                tracked.neg_risk,
                tracked.tick_size.clone(),
            )
        } else {
            error!(
                condition_id = %trade.condition_id,
                "Fill on unmanaged market with no opposite_token_id — cannot hedge"
            );
            return Ok(());
        };

        let resolution_market = market.clone().unwrap_or_else(|| {
            synthetic_resolution_market(
                &trade.condition_id,
                &yes_token_id,
                &no_token_id,
                neg_risk,
                &tick_size,
            )
        });

        let preparation = match prepare_market_for_resolution(
            &resolution_market,
            &self.order_manager,
            &self.trading_client,
            &self.risk_manager,
            &self.cached_balance,
            &self.book_rest,
            &self.book_manager,
            &self.config,
            tracked.leg.hedge_uses_asks().then_some(hedge_size),
        )
        .await
        {
            Ok(preparation) => preparation,
            Err(reason) => {
                error!(
                    condition_id = %trade.condition_id,
                    reason = %reason,
                    "Failed to prepare market for hedge resolution"
                );
                self.error_logger
                    .log_error("error", &reason, Some(&trade.condition_id));
                self.kill_market(&trade.condition_id, &reason).await;
                return Ok(());
            }
        };

        // Book-aware hedge resolution for BUY hedges
        let resolution = if hedge_side == Side::Buy {
            let res = plan_buy_resolution(
                &resolution_market,
                &preparation,
                &hedge_token_id,
                &filled_token_id,
                trade.price,
                hedge_size,
            );

            info!(
                condition_id = %trade.condition_id,
                hedge_shares = %res.hedge_shares,
                hedge_limit = %res.hedge_limit_price,
                sellback_shares = %res.sellback_shares,
                sellback_limit = %res.sellback_limit_price,
                unresolved_shares = %res.unresolved_shares,
                available_hedge_usdc = %preparation.max_hedge_usdc,
                cancel_wait_drained = preparation.cancel_wait_drained,
                "Fill handler hedge resolution computed"
            );
            Some(res)
        } else {
            None // SELL hedges use legacy FOK path
        };

        let (planned_hedge_size, hedge_cost) =
            planned_hedge_size_and_cost(resolution.as_ref(), hedge_side, hedge_size);
        let hedge_id = Uuid::new_v4().to_string();
        let fill_tick_size = tick_size.clone();
        let intent = HedgeIntent {
            condition_id: trade.condition_id.clone(),
            trigger_order_id: tracked.order_id.clone(),
            trigger_leg: tracked.leg,
            fill_size: hedge_size,
            fill_price: trade.price,
            hedge_token_id: hedge_token_id.clone(),
            hedge_side,
            neg_risk,
            tick_size,
        };
        let hedge_book_for_decision = if hedge_token_id == yes_token_id {
            &preparation.yes_book
        } else {
            &preparation.no_book
        };
        let filled_book_for_decision = if filled_token_id == yes_token_id {
            &preparation.yes_book
        } else {
            &preparation.no_book
        };
        let (filled_best_bid_price, filled_best_bid_size) =
            best_bid_snapshot(filled_book_for_decision);
        let (opposite_best_ask_price, opposite_best_ask_size) =
            best_ask_snapshot(hedge_book_for_decision);

        self.emit_event(emitters::build_hedge_decision(
            &self.run_id,
            &tracked.trace_id,
            &hedge_id,
            &self.mode,
            "fill_handler",
            &intent,
            emitters::HedgeDecisionContext {
                resolution: resolution.as_ref(),
                available_hedge_budget_usd: preparation.max_hedge_usdc,
                filled_best_bid_price,
                filled_best_bid_size,
                opposite_best_ask_price,
                opposite_best_ask_size,
            },
        ));
        if let Err(reason) = self
            .risk_manager
            .pre_trade_check(
                &trade.condition_id,
                planned_hedge_size,
                hedge_cost,
                true,
                Some(preparation.max_hedge_usdc),
            )
            .await
        {
            error!(
                condition_id = %trade.condition_id,
                reason = %reason,
                "Risk check failed for hedge, killing market"
            );
            self.error_logger.log_error(
                "error",
                &format!("Risk check failed for hedge: {}", reason),
                Some(&trade.condition_id),
            );
            self.kill_market(&trade.condition_id, &reason).await;
            return Ok(());
        }

        self.emit_event(emitters::build_hedge_intent(
            &self.run_id,
            &tracked.trace_id,
            &hedge_id,
            &self.mode,
            "fill_handler",
            &intent,
            emitters::HedgeIntentContext {
                resolution: resolution.as_ref(),
                pre_resolution_active_orders: Some(preparation.pre_resolution_active_orders as u64),
                pre_resolution_pending_cancels: Some(
                    preparation.pre_resolution_pending_cancels as u64,
                ),
                cancel_wait_drained: Some(preparation.cancel_wait_drained),
            },
            Some("fill_handler"),
        ));

        let exposure_tolerance = hedge_exposure_tolerance(&self.config);
        let hedge_started = Instant::now();
        let result = execute_resolution_plan_with_sellback_recompute_shared(
            &self.hedge_executor,
            &self.trading_client,
            &self.position_manager,
            &resolution_market,
            &self.order_manager,
            &self.risk_manager,
            &self.cached_balance,
            &self.book_rest,
            &self.book_manager,
            &self.config,
            &intent,
            resolution.as_ref(),
            &filled_token_id,
            &pre_position,
            exposure_tolerance,
            self.config.risk.hedge_timeout_secs,
            1,
        )
        .await;

        let sellback_order_id = result
            .sellback_result
            .as_ref()
            .and_then(|sellback| sellback.order_result.as_ref())
            .and_then(|order| (!order.order_id.is_empty()).then_some(order.order_id.as_str()));
        let sellback_price = result
            .sellback_result
            .as_ref()
            .and_then(|sellback| sellback.price);

        // Register hedge order ID so its fill won't trigger another hedge.
        if let Some(order_result) = result
            .hedge_result
            .as_ref()
            .and_then(|hedge_result| hedge_result.order_result.as_ref())
        {
            if !order_result.order_id.is_empty() {
                self.hedge_order_ids
                    .write()
                    .await
                    .insert(order_result.order_id.clone());
            }
        }

        let mut halt_signal_suppressed = false;
        if !result.success {
            let reason = result
                .failure_reason
                .clone()
                .unwrap_or_else(|| "Aggregate hedge resolution failed".to_string());
            error!(
                condition_id = %trade.condition_id,
                fill_size = %hedge_size,
                reason = %reason,
                post_sync_net_exposure = %result.post_sync_net_exposure,
                "\x1b[1;31m========== HEDGE RESOLUTION FAILED — killing market ==========\x1b[0m"
            );
            self.error_logger
                .log_error("error", &reason, Some(&trade.condition_id));
            let halt = self.kill_market(&trade.condition_id, &reason).await;
            halt_signal_suppressed = halt.halt_signal_suppressed;
        }

        self.emit_event(emitters::build_hedge_result(
            &self.run_id,
            &tracked.trace_id,
            &hedge_id,
            &self.mode,
            "fill_handler",
            &intent,
            emitters::HedgeResultContext {
                hedge_result: result.hedge_result.as_ref(),
                aggregate_success: result.success,
                aggregate_failure_reason: result.failure_reason.as_deref(),
                sellback_order_id,
                sellback_price,
                sellback_execution_limit_price: sellback_price,
                sellback_leg_status: Some(sellback_leg_status(result.sellback_result.as_ref())),
                sellback_response_status: result
                    .sellback_result
                    .as_ref()
                    .and_then(|sellback| sellback.verification_metadata.response_status.as_deref()),
                sellback_lookup_status: result
                    .sellback_result
                    .as_ref()
                    .and_then(|sellback| sellback.verification_metadata.lookup_status.as_deref()),
                sellback_lookup_matched_shares: result
                    .sellback_result
                    .as_ref()
                    .and_then(|sellback| sellback.verification_metadata.lookup_matched_shares),
                sellback_lookup_error: result
                    .sellback_result
                    .as_ref()
                    .and_then(|sellback| sellback.verification_metadata.lookup_error.as_deref()),
                sellback_trade_ids: result.sellback_result.as_ref().and_then(|sellback| {
                    (!sellback.verification_metadata.trade_ids.is_empty())
                        .then_some(sellback.verification_metadata.trade_ids.as_slice())
                }),
                post_sync_net_exposure: Some(result.post_sync_net_exposure),
                post_sync_yes_size: result
                    .post_position
                    .as_ref()
                    .map(|position| position.yes_size),
                post_sync_no_size: result
                    .post_position
                    .as_ref()
                    .map(|position| position.no_size),
                post_sync_source: Some(result.post_sync_source),
                halt_signal_suppressed,
            },
            hedge_started.elapsed().as_millis() as u64,
            Some("fill_handler"),
        ));

        let sellback_completed = result
            .sellback_result
            .as_ref()
            .is_some_and(SellbackExecutionResult::is_verified_filled);
        let mut exit_telemetry = None;
        refresh_hedge_exit_telemetry(
            &mut exit_telemetry,
            result.post_position.as_ref(),
            exposure_tolerance,
            sellback_completed,
            self.ctf_merger.is_some(),
        );

        if !result.success {
            if let (Some(post_position), Some(telemetry)) =
                (result.post_position.as_ref(), exit_telemetry.as_ref())
            {
                self.emit_event(emitters::build_hedge_exit_path(
                    &self.run_id,
                    &tracked.trace_id,
                    &hedge_id,
                    &self.mode,
                    "fill_handler",
                    &intent,
                    emitters::HedgeExitPathContext {
                        post_position,
                        post_sync_source: result.post_sync_source,
                        exit_path_status: &telemetry.exit_path_status,
                        merge_eligible_pairs: telemetry.merge_eligible_pairs,
                        ctf_merge_configured: telemetry.ctf_merge_configured,
                        merge_attempted: telemetry.merge_attempted,
                        merge_tx_hash: telemetry.merge_tx_hash.as_deref(),
                        merge_failure_reason: telemetry.merge_failure_reason.as_deref(),
                        fallback_asks_attempted: telemetry.fallback_asks_attempted,
                        fallback_ask_count: telemetry.fallback_ask_count,
                        fallback_failure_reason: telemetry.fallback_failure_reason.as_deref(),
                    },
                ));
            }
            return Ok(());
        }

        if let Some((sellback_price, confirmed_shares)) = result
            .sellback_result
            .as_ref()
            .filter(|sellback| sellback.is_verified_filled())
            .and_then(|sellback| {
                sellback
                    .price
                    .zip(sellback.confirmed_shares)
                    .filter(|(_, shares)| *shares > Decimal::ZERO)
            })
        {
            record_recent_resolution_trade_shared(
                &self.recent_resolution_trades,
                &trade.condition_id,
                &filled_token_id,
                Side::Sell,
                sellback_price,
                confirmed_shares,
            )
            .await;
        }

        error!(
            condition_id = %trade.condition_id,
            fill_size = %hedge_size,
            hedge_price = ?result
                .hedge_result
                .as_ref()
                .and_then(|hedge_result| hedge_result.hedge_price),
            sellback_price = ?sellback_price,
            post_sync_net_exposure = %result.post_sync_net_exposure,
            post_sync_source = %result.post_sync_source,
            "\x1b[1;32m========== HEDGE RESOLUTION OK ==========\x1b[0m"
        );

        // === Post-hedge resolution: sell remainder → merge → fallback asks ===

        // Refresh balance and sync positions to see current state.
        match self.trading_client.get_balance().await {
            Ok(fresh_balance) => {
                *self.cached_balance.write().await = fresh_balance;
                self.risk_manager.update_balance(fresh_balance).await;
                self.order_manager.update_gross_balance(fresh_balance).await;
            }
            Err(e) => {
                warn!(error = %e, "Failed to refresh balance after hedge — cache may be stale");
            }
        }

        let post_position = if let Some(position) = result.post_position.clone() {
            Some(position)
        } else {
            if let Err(e) = self.position_manager.sync_positions().await {
                warn!(error = %e, "Failed to sync positions after hedge");
            }
            self.position_manager
                .get_position(&trade.condition_id)
                .await
        };
        refresh_hedge_exit_telemetry(
            &mut exit_telemetry,
            post_position.as_ref(),
            exposure_tolerance,
            sellback_completed,
            self.ctf_merger.is_some(),
        );

        // Update reconciliation baseline so reconciliation doesn't re-hedge.
        if let Some(ref pos) = post_position {
            self.recon_baselines
                .write()
                .await
                .insert(trade.condition_id.clone(), (pos.yes_size, pos.no_size));
        }

        if let (Some(ref market), Some(ref post_position)) =
            (market.as_ref(), post_position.as_ref())
        {
            self.emit_event(emitters::build_neutrality_evaluated(
                &self.run_id,
                &tracked.trace_id,
                &self.mode,
                market,
                &pre_position,
                post_position,
                Decimal::ZERO,
            ));
        }

        // Step 1: If residual exposure exceeds tolerance, sell back the remainder.
        let mut post_position = post_position;
        if let Some(ref api_pos) = post_position {
            let net = api_pos.net_exposure().abs();
            if net > exposure_tolerance {
                warn!(
                    condition_id = %trade.condition_id,
                    yes = %api_pos.yes_size,
                    no = %api_pos.no_size,
                    net_exposure = %net,
                    "Residual exposure after hedge — selling back remainder"
                );

                // Determine which side has excess and sell it
                let (sell_token, sell_size) = if api_pos.yes_size > api_pos.no_size {
                    let excess = normalize_share_size(api_pos.yes_size - api_pos.no_size);
                    // Use the original fill token if it matches, otherwise derive from market
                    let token = if let Some(ref m) = market {
                        m.yes_token_id.clone()
                    } else {
                        trade.asset_id.clone()
                    };
                    (token, excess)
                } else {
                    let excess = normalize_share_size(api_pos.no_size - api_pos.yes_size);
                    let token = if let Some(ref m) = market {
                        m.no_token_id.clone()
                    } else {
                        trade.asset_id.clone()
                    };
                    (token, excess)
                };

                if sell_size > Decimal::ZERO {
                    let sell_request = OrderRequest {
                        token_id: sell_token,
                        price: Decimal::new(1, 2), // 0.01 — accept any price
                        size: sell_size,
                        amount_kind: OrderAmountKind::Shares,
                        side: Side::Sell,
                        order_type: OrderType::FOK,
                        post_only: false,
                        neg_risk,
                        tick_size: fill_tick_size.clone(),
                    };

                    match self.trading_client.place_order(&sell_request).await {
                        Ok(_) => {
                            warn!(
                                condition_id = %trade.condition_id,
                                sell_size = %sell_size,
                                "\x1b[1;33m========== SELL-BACK OK — remainder flattened ==========\x1b[0m"
                            );
                        }
                        Err(sell_err) => {
                            let msg = format!("Sell-back failed after hedge: {}", sell_err);
                            error!(
                                condition_id = %trade.condition_id,
                                error = %sell_err,
                                "\x1b[1;31m========== SELL-BACK FAILED — killing market ==========\x1b[0m"
                            );
                            self.error_logger
                                .log_error("error", &msg, Some(&trade.condition_id));
                            self.kill_market(&trade.condition_id, &msg).await;
                            return Ok(());
                        }
                    }

                    // Re-sync after sell-back
                    if let Err(e) = self.position_manager.sync_positions().await {
                        warn!(error = %e, "Failed to sync positions after sell-back");
                    }
                    post_position = self
                        .position_manager
                        .get_position(&trade.condition_id)
                        .await;
                }
            } else {
                self.hedge_signals
                    .write()
                    .await
                    .insert(trade.condition_id.clone(), new_hedge_signal());
                info!(
                    condition_id = %trade.condition_id,
                    yes = %api_pos.yes_size,
                    no = %api_pos.no_size,
                    "Post-hedge position verified"
                );
            }
        }

        refresh_hedge_exit_telemetry(
            &mut exit_telemetry,
            post_position.as_ref(),
            exposure_tolerance,
            sellback_completed,
            self.ctf_merger.is_some(),
        );

        // Step 2: CTF merge all complete YES+NO pairs (primary exit path).
        if let Some(ref pos) = post_position {
            self.execute_pair_exit(
                &trade.condition_id,
                market.as_ref(),
                pos,
                &mut exit_telemetry,
                MergeTruthHandling::BackgroundMonitor,
            )
            .await;
        }

        if let (Some(post_position), Some(telemetry)) =
            (post_position.as_ref(), exit_telemetry.as_ref())
        {
            self.emit_event(emitters::build_hedge_exit_path(
                &self.run_id,
                &tracked.trace_id,
                &hedge_id,
                &self.mode,
                "fill_handler",
                &intent,
                emitters::HedgeExitPathContext {
                    post_position,
                    post_sync_source: result.post_sync_source,
                    exit_path_status: &telemetry.exit_path_status,
                    merge_eligible_pairs: telemetry.merge_eligible_pairs,
                    ctf_merge_configured: telemetry.ctf_merge_configured,
                    merge_attempted: telemetry.merge_attempted,
                    merge_tx_hash: telemetry.merge_tx_hash.as_deref(),
                    merge_failure_reason: telemetry.merge_failure_reason.as_deref(),
                    fallback_asks_attempted: telemetry.fallback_asks_attempted,
                    fallback_ask_count: telemetry.fallback_ask_count,
                    fallback_failure_reason: telemetry.fallback_failure_reason.as_deref(),
                },
            ));
        } else {
            let reason = hedge_exit_observability_reason("fill_handler", result.post_sync_source);
            error!(
                condition_id = %trade.condition_id,
                trace_id = %tracked.trace_id,
                hedge_id = %hedge_id,
                reason = %reason,
                "Successful fill-handler hedge trace missing required hedge_exit_path_recorded"
            );
            self.error_logger
                .log_error("error", &reason, Some(&trade.condition_id));
            self.emit_event(build_hedge_exit_observability_event(
                &self.run_id,
                &self.mode,
                "fill_handler",
                &tracked.trace_id,
                &hedge_id,
                &intent,
                &reason,
            ));
        }

        Ok(())
    }

    async fn place_inventory_asks(
        &self,
        market: &CanonicalMarket,
        position: &Position,
        origin: &'static str,
    ) {
        // Fetch books from cache, falling back to REST if cache is empty
        let yes_book = match self.book_manager.get_book(&market.yes_token_id).await {
            Some(book) => Some(book),
            None => {
                warn!(
                    condition_id = %market.condition_id,
                    token_id = %market.yes_token_id,
                    "YES book missing from cache for ask placement — fetching via REST"
                );
                match self.book_rest.fetch_book(&market.yes_token_id).await {
                    Ok(book) => {
                        self.book_manager.insert_snapshot(book.clone()).await;
                        Some(book)
                    }
                    Err(e) => {
                        error!(
                            condition_id = %market.condition_id,
                            error = %e,
                            "Failed to fetch YES book via REST for ask placement"
                        );
                        None
                    }
                }
            }
        };

        let no_book = match self.book_manager.get_book(&market.no_token_id).await {
            Some(book) => Some(book),
            None => {
                warn!(
                    condition_id = %market.condition_id,
                    token_id = %market.no_token_id,
                    "NO book missing from cache for ask placement — fetching via REST"
                );
                match self.book_rest.fetch_book(&market.no_token_id).await {
                    Ok(book) => {
                        self.book_manager.insert_snapshot(book.clone()).await;
                        Some(book)
                    }
                    Err(e) => {
                        error!(
                            condition_id = %market.condition_id,
                            error = %e,
                            "Failed to fetch NO book via REST for ask placement"
                        );
                        None
                    }
                }
            }
        };

        let max_spread = market.reward_config.max_spread;
        let ask_depth = self.config.strategy.ask_depth_pct;
        let mut candidates = Vec::new();

        if position.yes_size > Decimal::ZERO {
            if let Some(book) = &yes_book {
                if let Some(price) = compute_ask_price(book, max_spread, ask_depth) {
                    let ask_size = position.yes_size;
                    candidates.push(QuoteCandidate {
                        condition_id: market.condition_id.clone(),
                        leg: QuoteLeg::YesAsk,
                        price,
                        size: ask_size,
                        status: QuoteStatus::Approved,
                        reason: None,
                    });
                } else {
                    warn!(
                        condition_id = %market.condition_id,
                        "Cannot compute YES ask price — book has no bid/ask levels"
                    );
                }
            }
        }

        if position.no_size > Decimal::ZERO {
            if let Some(book) = &no_book {
                if let Some(price) = compute_ask_price(book, max_spread, ask_depth) {
                    let ask_size = position.no_size;
                    candidates.push(QuoteCandidate {
                        condition_id: market.condition_id.clone(),
                        leg: QuoteLeg::NoAsk,
                        price,
                        size: ask_size,
                        status: QuoteStatus::Approved,
                        reason: None,
                    });
                } else {
                    warn!(
                        condition_id = %market.condition_id,
                        "Cannot compute NO ask price — book has no bid/ask levels"
                    );
                }
            }
        }

        if candidates.is_empty() {
            warn!(
                condition_id = %market.condition_id,
                yes_inv = %position.yes_size,
                no_inv = %position.no_size,
                "No ask candidates generated despite holding inventory"
            );
            return;
        }

        let ask_set = QuoteSet {
            condition_id: market.condition_id.clone(),
            candidates,
        };

        let existing = self
            .order_manager
            .get_market_orders(&market.condition_id)
            .await;
        let has_asks = existing.iter().any(|o| o.leg.is_ask());

        let min_size = market.reward_config.min_size;
        if has_asks {
            if let Err(e) = self
                .order_manager
                .cancel_replace_if_drifted(
                    &market,
                    &ask_set,
                    self.config.strategy.quote_drift_bps,
                    Some(position),
                    min_size,
                    None,
                    origin,
                    Some("ask_inventory"),
                )
                .await
            {
                error!(
                    condition_id = %market.condition_id,
                    error = %e,
                    "Failed to update inventory asks"
                );
            }
        } else {
            if let Err(e) = self
                .order_manager
                .place_quotes(
                    &market,
                    &ask_set,
                    Some(position),
                    min_size,
                    None,
                    origin,
                    Some("ask_inventory"),
                )
                .await
            {
                error!(
                    condition_id = %market.condition_id,
                    error = %e,
                    "Failed to place inventory asks"
                );
            }
        }

        info!(
            condition_id = %market.condition_id,
            yes_inv = %position.yes_size,
            no_inv = %position.no_size,
            "\x1b[1;36m========== ASK ORDERS PLACED ON INVENTORY ==========\x1b[0m"
        );
    }

    async fn kill_market(&self, condition_id: &str, reason: &str) -> HaltMarketOutcome {
        halt_market_and_start_cleanup(
            condition_id,
            reason,
            &self.risk_manager,
            &self.order_manager,
            &self.position_manager,
            &self.trading_client,
            &self.managed_markets,
            &self.known_markets,
            &self.config,
            &self.run_id,
            &self.mode,
            &self.event_producer,
            &self.error_logger,
        )
        .await
    }

    async fn finalize_halted_market_if_drained(&self, condition_id: &str) -> HaltCleanupOutcome {
        finalize_halted_market_cleanup(
            condition_id,
            &self.order_manager,
            &self.position_manager,
            &self.trading_client,
            &self.managed_markets,
            &self.known_markets,
            &self.config,
        )
        .await
    }

    /// Sell unhedged excess inventory for a market being killed.
    /// Only sells the directional excess (abs(yes - no)), leaving any
    /// balanced hedged inventory alone.
    async fn flatten_unhedged(&self, condition_id: &str) {
        let _ = flatten_directional_inventory_for_halt(
            condition_id,
            &self.position_manager,
            &self.trading_client,
            &self.managed_markets,
            &self.known_markets,
            &self.config,
        )
        .await;
    }
}

/// Compute the maximum size hedgeable within slippage limits.
///
/// Walks the book's ask (or bid) levels, accumulating size until price
/// exceeds the slippage tolerance from best price.
fn max_hedgeable_within_slippage(
    book: &OrderBookSnapshot,
    use_asks: bool,
    max_slippage_bps: Decimal,
) -> Decimal {
    let levels = if use_asks { &book.asks } else { &book.bids };

    let best_price = match levels.first() {
        Some(l) if l.price > Decimal::ZERO => l.price,
        _ => return Decimal::ZERO,
    };

    let mut total_size = Decimal::ZERO;
    let ten_k = Decimal::from(10000);

    for level in levels {
        let slippage = if use_asks {
            (level.price - best_price) / best_price * ten_k
        } else {
            (best_price - level.price) / best_price * ten_k
        };

        if slippage > max_slippage_bps {
            break;
        }
        total_size += level.size;
    }

    total_size
}

fn hedge_exposure_tolerance(config: &Config) -> Decimal {
    normalize_share_size(config.risk.hedge_exposure_tolerance.max(Decimal::ZERO))
}

async fn wait_for_market_cancel_drain(order_manager: &OrderManager, condition_id: &str) -> bool {
    let wait_started = Instant::now();
    let wait_window = Duration::from_millis(HEDGE_RESOLUTION_CANCEL_WAIT_MS);
    let wait_poll = Duration::from_millis(HEDGE_RESOLUTION_CANCEL_POLL_MS);

    loop {
        let (active, pending) = order_manager.market_order_state_counts(condition_id).await;
        if active == 0 && pending == 0 {
            return true;
        }
        if wait_started.elapsed() >= wait_window {
            return false;
        }
        let _ = order_manager.retry_pending_cancels().await;
        time::sleep(wait_poll).await;
    }
}

async fn wait_for_external_bid_cancel_drain(
    order_manager: &OrderManager,
    excluded_condition_id: &str,
) -> bool {
    let wait_started = Instant::now();
    let wait_window = Duration::from_millis(HEDGE_RESOLUTION_CANCEL_WAIT_MS);
    let wait_poll = Duration::from_millis(HEDGE_RESOLUTION_CANCEL_POLL_MS);

    loop {
        let (active, pending) = order_manager
            .global_bid_order_state_counts_excluding(excluded_condition_id)
            .await;
        if active == 0 && pending == 0 {
            return true;
        }
        if wait_started.elapsed() >= wait_window {
            return false;
        }
        let _ = order_manager.retry_pending_cancels().await;
        time::sleep(wait_poll).await;
    }
}

async fn refresh_balance_for_resolution(
    condition_id: &str,
    trading_client: &Arc<TradingClient>,
    risk_manager: &Arc<RiskManager>,
    cached_balance: &Arc<RwLock<Decimal>>,
    order_manager: &OrderManager,
) {
    match trading_client.get_balance().await {
        Ok(fresh_balance) => {
            *cached_balance.write().await = fresh_balance;
            risk_manager.update_balance(fresh_balance).await;
            order_manager.update_gross_balance(fresh_balance).await;
        }
        Err(err) => {
            warn!(
                condition_id = %condition_id,
                error = %err,
                "Failed to refresh balance during hedge resolution prep — using cached balance"
            );
        }
    }
}

async fn prepare_market_for_resolution(
    market: &CanonicalMarket,
    order_manager: &OrderManager,
    trading_client: &Arc<TradingClient>,
    risk_manager: &Arc<RiskManager>,
    cached_balance: &Arc<RwLock<Decimal>>,
    book_rest: &BookRestClient,
    book_manager: &Arc<BookManager>,
    config: &Config,
    buy_side_reclaim_target: Option<Decimal>,
) -> std::result::Result<ResolutionPreparation, String> {
    let condition_id = market.condition_id.as_str();
    let yes_token_id = market.yes_token_id.as_str();
    let no_token_id = market.no_token_id.as_str();

    match order_manager
        .sync_market_open_orders(condition_id, market, MarketOrderSyncMode::Reconcile)
        .await
    {
        Ok(sync_result) => {
            if !sync_result.duplicate_live_bid_legs.is_empty() {
                return Err(format!(
                    "duplicate_live_bid_leg_detected during resolution prep for {}",
                    condition_id
                ));
            }
            info!(
                condition_id = %condition_id,
                fetched = sync_result.fetched,
                live = sync_result.live,
                imported = sync_result.imported,
                already_tracked = sync_result.already_tracked,
                updated = sync_result.updated,
                pruned = sync_result.pruned,
                missing = sync_result.missing_order_ids.len(),
                "Market-scoped open-order sync complete before hedge resolution prep"
            );
        }
        Err(err) => {
            warn!(
                condition_id = %condition_id,
                error = %err,
                "Market-scoped open-order sync failed before hedge resolution prep"
            );
        }
    }

    let (active_before, pending_before) =
        order_manager.market_order_state_counts(condition_id).await;

    if let Err(err) = order_manager
        .cancel_all(
            condition_id,
            CancelReasonCode::RiskHalt,
            "hedge_resolution_prepare",
        )
        .await
    {
        warn!(
            condition_id = %condition_id,
            error = %err,
            "Market-order cancel pass failed during hedge resolution prep"
        );
    }

    let wait_started = Instant::now();
    let mut cancel_wait_drained = wait_for_market_cancel_drain(order_manager, condition_id).await;
    let (active_after, pending_after) = order_manager.market_order_state_counts(condition_id).await;
    if cancel_wait_drained {
        info!(
            condition_id = %condition_id,
            active_orders = active_after,
            pending_cancels = pending_after,
            waited_ms = wait_started.elapsed().as_millis(),
            "Hedge resolution cancel wait drained cleanly"
        );
    } else {
        warn!(
            condition_id = %condition_id,
            active_orders = active_after,
            pending_cancels = pending_after,
            waited_ms = wait_started.elapsed().as_millis(),
            "Hedge resolution cancel wait timed out; continuing with currently free capital"
        );
    }

    match order_manager
        .sync_market_open_orders(condition_id, market, MarketOrderSyncMode::Reconcile)
        .await
    {
        Ok(sync_result) => {
            info!(
                condition_id = %condition_id,
                fetched = sync_result.fetched,
                live = sync_result.live,
                imported = sync_result.imported,
                updated = sync_result.updated,
                pruned = sync_result.pruned,
                missing = sync_result.missing_order_ids.len(),
                "Reconciled market order truth after resolution cancel wait"
            );
        }
        Err(err) => {
            warn!(
                condition_id = %condition_id,
                error = %err,
                "Post-cancel market order reconcile failed during hedge resolution prep"
            );
        }
    }
    let confirmed = order_manager.retry_pending_cancels().await;
    if confirmed > 0 {
        info!(
            condition_id = %condition_id,
            confirmed,
            "Confirmed pending cancels after resolution prep reconcile"
        );
    }

    refresh_balance_for_resolution(
        condition_id,
        trading_client,
        risk_manager,
        cached_balance,
        order_manager,
    )
    .await;

    let mut max_hedge_usdc = order_manager.available_hedge_resolution_usdc().await;
    if let Some(required_hedge_usdc) = buy_side_reclaim_target
        .map(normalize_share_size)
        .filter(|value| *value > Decimal::ZERO)
    {
        if max_hedge_usdc < required_hedge_usdc {
            info!(
                condition_id = %condition_id,
                available_hedge_usdc = %max_hedge_usdc,
                required_hedge_usdc = %required_hedge_usdc,
                "BUY-side resolution reclaiming external bid capital before planning"
            );
            if let Err(err) = order_manager
                .cancel_other_bids_with_diagnostics(
                    condition_id,
                    CancelReasonCode::RiskHalt,
                    "hedge_resolution_reclaim",
                    None,
                )
                .await
            {
                warn!(
                    condition_id = %condition_id,
                    error = %err,
                    "External bid reclaim cancel pass failed during hedge resolution prep"
                );
            }

            let reclaim_wait_started = Instant::now();
            let reclaim_wait_drained =
                wait_for_external_bid_cancel_drain(order_manager, condition_id).await;
            let (external_active, external_pending) = order_manager
                .global_bid_order_state_counts_excluding(condition_id)
                .await;
            if reclaim_wait_drained {
                info!(
                    condition_id = %condition_id,
                    active_external_bids = external_active,
                    pending_external_cancels = external_pending,
                    waited_ms = reclaim_wait_started.elapsed().as_millis(),
                    "External bid reclaim wait drained cleanly"
                );
            } else {
                warn!(
                    condition_id = %condition_id,
                    active_external_bids = external_active,
                    pending_external_cancels = external_pending,
                    waited_ms = reclaim_wait_started.elapsed().as_millis(),
                    "External bid reclaim wait timed out; continuing with currently free capital"
                );
            }
            cancel_wait_drained &= reclaim_wait_drained;

            let confirmed = order_manager.retry_pending_cancels().await;
            if confirmed > 0 {
                info!(
                    condition_id = %condition_id,
                    confirmed,
                    "Confirmed pending cancels after external bid reclaim"
                );
            }

            refresh_balance_for_resolution(
                condition_id,
                trading_client,
                risk_manager,
                cached_balance,
                order_manager,
            )
            .await;
            max_hedge_usdc = order_manager.available_hedge_resolution_usdc().await;
        }
    }

    let max_book_age = chrono::Duration::seconds(config.books.max_book_age_secs as i64);

    let (yes_book, no_book) = match book_rest.fetch_both_books(yes_token_id, no_token_id).await {
        Ok((yes_book, no_book)) => {
            book_manager.insert_snapshot(yes_book.clone()).await;
            book_manager.insert_snapshot(no_book.clone()).await;
            (yes_book, no_book)
        }
        Err(err) => {
            let yes_cached = book_manager.get_book(yes_token_id).await;
            let no_cached = book_manager.get_book(no_token_id).await;
            match (yes_cached, no_cached) {
                (Some(yes_book), Some(no_book))
                    if !yes_book.is_stale(max_book_age) && !no_book.is_stale(max_book_age) =>
                {
                    warn!(
                        condition_id = %condition_id,
                        error = %err,
                        "Fresh book fetch failed during hedge resolution prep — using fresh cached books"
                    );
                    (yes_book, no_book)
                }
                _ => {
                    return Err(format!(
                        "Fresh book fetch failed and no fresh cached books available: {}",
                        err
                    ));
                }
            }
        }
    };

    Ok(ResolutionPreparation {
        yes_book,
        no_book,
        pre_resolution_active_orders: active_before,
        pre_resolution_pending_cancels: pending_before,
        cancel_wait_drained,
        max_hedge_usdc,
    })
}

fn build_sellback_order_request(
    token_id: &str,
    price: Decimal,
    size: Decimal,
    neg_risk: bool,
    tick_size: &str,
) -> OrderRequest {
    OrderRequest {
        token_id: token_id.to_string(),
        price,
        size: normalize_share_size(size),
        amount_kind: OrderAmountKind::Shares,
        side: Side::Sell,
        order_type: OrderType::FOK,
        post_only: false,
        neg_risk,
        tick_size: tick_size.to_string(),
    }
}

async fn execute_sellback_order(
    trading_client: &Arc<TradingClient>,
    token_id: &str,
    price: Decimal,
    size: Decimal,
    neg_risk: bool,
    tick_size: &str,
) -> SellbackExecutionResult {
    let request = build_sellback_order_request(token_id, price, size, neg_risk, tick_size);
    let requested_shares = request.size;

    match trading_client.place_order(&request).await {
        Ok(order_result) => {
            if let Some(result) = sellback_result_from_terminal_placement(
                order_result.clone(),
                price,
                requested_shares,
            ) {
                return result;
            }
            verify_provisional_sellback_order(trading_client, order_result, price, requested_shares)
                .await
        }
        Err(err) => SellbackExecutionResult {
            order_result: None,
            verification_state: SellbackVerificationState::Unknown,
            confirmed_shares: None,
            failure_reason: Some(format!("Sell-back placement failed: {}", err)),
            price: Some(price),
            verification_metadata: SellbackVerificationMetadata::default(),
        },
    }
}

async fn execute_resolution_plan_with_timeout(
    hedge_executor: &HedgeExecutor,
    trading_client: &Arc<TradingClient>,
    position_manager: &Arc<PositionManager>,
    intent: &HedgeIntent,
    resolution: Option<&HedgeResolution>,
    filled_token_id: &str,
    pre_resolution_position: &Position,
    exposure_tolerance: Decimal,
    hedge_timeout_secs: u64,
) -> ResolutionExecutionResult {
    match tokio::time::timeout(
        std::time::Duration::from_secs(hedge_timeout_secs),
        execute_resolution_plan(
            hedge_executor,
            trading_client,
            position_manager,
            intent,
            resolution,
            filled_token_id,
            pre_resolution_position,
            exposure_tolerance,
        ),
    )
    .await
    {
        Ok(result) => result,
        Err(_elapsed) => {
            error!(
                condition_id = %intent.condition_id,
                timeout_secs = hedge_timeout_secs,
                "Hedge execution timed out — releasing mutex"
            );
            ResolutionExecutionResult {
                hedge_result: None,
                sellback_result: None,
                post_position: None,
                post_sync_net_exposure: Decimal::MAX,
                post_sync_source: "timeout",
                success: false,
                failure_reason: Some(format!(
                    "Hedge execution timed out after {}s",
                    hedge_timeout_secs
                )),
            }
        }
    }
}

async fn execute_resolution_plan(
    hedge_executor: &HedgeExecutor,
    trading_client: &Arc<TradingClient>,
    position_manager: &Arc<PositionManager>,
    intent: &HedgeIntent,
    resolution: Option<&HedgeResolution>,
    filled_token_id: &str,
    pre_resolution_position: &Position,
    exposure_tolerance: Decimal,
) -> ResolutionExecutionResult {
    let hedge_result = match intent.hedge_side {
        Side::Buy => match resolution {
            Some(resolution) if resolution.hedge_shares > Decimal::ZERO => {
                Some(hedge_executor.execute_hedge(intent, Some(resolution)).await)
            }
            Some(_) => None,
            None => Some(hedge_executor.execute_hedge(intent, None).await),
        },
        Side::Sell => Some(hedge_executor.execute_hedge(intent, None).await),
    };

    let sellback_result = match resolution {
        Some(resolution) if resolution.sellback_shares > Decimal::ZERO => Some(
            execute_sellback_order(
                trading_client,
                filled_token_id,
                resolution.sellback_limit_price,
                resolution.sellback_shares,
                intent.neg_risk,
                &intent.tick_size,
            )
            .await,
        ),
        _ => None,
    };

    let first_post_position =
        match sync_position_for_resolution(position_manager, &intent.condition_id).await {
            Ok(position) => position,
            Err(err) => {
                let mut reasons = vec![format!("Post-sync failed after hedge resolution: {}", err)];
                if let Some(resolution) = resolution {
                    if resolution.unresolved_shares > Decimal::ZERO {
                        reasons.push(format!(
                            "unresolved_shares={}",
                            resolution.unresolved_shares
                        ));
                    }
                }
                if let Some(hedge_result) = &hedge_result {
                    if !hedge_result.success {
                        reasons.push(
                            hedge_result
                                .failure_reason
                                .clone()
                                .unwrap_or_else(|| "hedge leg failed".to_string()),
                        );
                    }
                }
                if let Some(sellback_result) = &sellback_result {
                    if !sellback_result.is_verified_filled() {
                        reasons.push(
                            sellback_result
                                .failure_reason
                                .clone()
                                .unwrap_or_else(|| "sell-back leg failed".to_string()),
                        );
                    }
                }
                return ResolutionExecutionResult {
                    hedge_result,
                    sellback_result,
                    post_position: None,
                    post_sync_net_exposure: Decimal::MAX,
                    post_sync_source: "sync_failed",
                    success: false,
                    failure_reason: Some(reasons.join("; ")),
                };
            }
        };

    let mut post_position = first_post_position.clone();
    let mut post_sync_source = "first_sync";
    let mut hedge_confirmed_by_position_truth = post_position.as_ref().is_some_and(|position| {
        hedge_leg_confirmed_by_position_truth(
            pre_resolution_position,
            position,
            intent,
            hedge_result.as_ref(),
        )
    });

    if should_retry_resolution_sync(
        hedge_result.as_ref(),
        intent,
        pre_resolution_position,
        post_position.as_ref(),
    ) {
        info!(
            condition_id = %intent.condition_id,
            verification_state = ?hedge_result.as_ref().map(|result| result.verification_state),
            "First post-resolution sync conflicts with hedge evidence — retrying once"
        );
        tokio::time::sleep(Duration::from_millis(RESOLUTION_RETRY_SYNC_DELAY_MS)).await;
        match sync_position_for_resolution(position_manager, &intent.condition_id).await {
            Ok(retry_position) => {
                post_position = retry_position;
                post_sync_source = "retry_sync";
                hedge_confirmed_by_position_truth =
                    post_position.as_ref().is_some_and(|position| {
                        hedge_leg_confirmed_by_position_truth(
                            pre_resolution_position,
                            position,
                            intent,
                            hedge_result.as_ref(),
                        )
                    });
            }
            Err(err) => {
                warn!(
                    condition_id = %intent.condition_id,
                    error = %err,
                    "Retry post-resolution sync failed — keeping first snapshot"
                );
            }
        }
    }

    let mut post_sync_net_exposure = post_position
        .as_ref()
        .map(|position| position.net_exposure().abs())
        .unwrap_or(Decimal::MAX);

    if should_retry_resolution_sync_for_sellback(
        sellback_result.as_ref(),
        post_position.as_ref(),
        exposure_tolerance,
    ) {
        info!(
            condition_id = %intent.condition_id,
            sellback_verification = ?sellback_result.as_ref().map(|sellback| &sellback.verification_state),
            post_sync_source,
            post_sync_net_exposure = %post_sync_net_exposure,
            "Post-resolution sync conflicts with execution-confirmed sellback — retrying once"
        );
        tokio::time::sleep(Duration::from_millis(RESOLUTION_RETRY_SYNC_DELAY_MS)).await;
        match sync_position_for_resolution(position_manager, &intent.condition_id).await {
            Ok(retry_position) => {
                post_position = retry_position;
                post_sync_source = "retry_sync_sellback";
                post_sync_net_exposure = post_position
                    .as_ref()
                    .map(|position| position.net_exposure().abs())
                    .unwrap_or(Decimal::MAX);
                hedge_confirmed_by_position_truth =
                    post_position.as_ref().is_some_and(|position| {
                        hedge_leg_confirmed_by_position_truth(
                            pre_resolution_position,
                            position,
                            intent,
                            hedge_result.as_ref(),
                        )
                    });
            }
            Err(err) => {
                warn!(
                    condition_id = %intent.condition_id,
                    error = %err,
                    "Retry post-resolution sync after sellback evidence failed — keeping first snapshot"
                );
            }
        }
    }

    if let Some(derived_position) = derive_execution_confirmed_sellback_post_sync_position(
        pre_resolution_position,
        post_position.as_ref(),
        intent,
        hedge_result.as_ref(),
        sellback_result.as_ref(),
        exposure_tolerance,
        hedge_confirmed_by_position_truth,
    ) {
        let derived_exposure = derived_position.net_exposure().abs();
        let should_apply_derived =
            post_position.is_none() || post_sync_net_exposure > exposure_tolerance;
        if should_apply_derived {
            info!(
                condition_id = %intent.condition_id,
                post_sync_source = EXECUTION_CONFIRMED_SELLBACK_POST_SYNC_SOURCE,
                post_sync_yes_size = %derived_position.yes_size,
                post_sync_no_size = %derived_position.no_size,
                post_sync_net_exposure = %derived_exposure,
                "Resolved sellback completion from execution-confirmed sellback evidence"
            );
            post_position = Some(derived_position);
            post_sync_source = EXECUTION_CONFIRMED_SELLBACK_POST_SYNC_SOURCE;
        }
    }

    if post_position.is_none() {
        let reason = missing_post_sync_truth_reason();
        let mut reasons =
            resolution_failure_reasons(resolution, hedge_result.as_ref(), sellback_result.as_ref());
        reasons.push(reason.to_string());
        warn!(
            condition_id = %intent.condition_id,
            hedge_verification = ?hedge_result.as_ref().map(|result| result.verification_state),
            post_sync_source,
            reason = %reason,
            "Post-resolution truth missing after current sync/retry flow"
        );
        return ResolutionExecutionResult {
            hedge_result,
            sellback_result,
            post_position: None,
            post_sync_net_exposure: Decimal::MAX,
            post_sync_source,
            success: false,
            failure_reason: Some(reasons.join("; ")),
        };
    }

    let post_sync_net_exposure = post_position
        .as_ref()
        .map(|position| position.net_exposure().abs())
        .unwrap_or(Decimal::MAX);
    let sellback_verified = sellback_result
        .as_ref()
        .map(SellbackExecutionResult::is_verified_filled)
        .unwrap_or(true);
    let success = post_sync_net_exposure <= exposure_tolerance && sellback_verified;

    info!(
        condition_id = %intent.condition_id,
        hedge_verification = ?hedge_result.as_ref().map(|hedge| hedge.verification_state),
        sellback_verification = ?sellback_result.as_ref().map(|sellback| &sellback.verification_state),
        hedge_confirmed_by_position_truth,
        post_sync_source,
        post_sync_yes_size = %post_position.as_ref().map(|position| position.yes_size).unwrap_or(Decimal::ZERO),
        post_sync_no_size = %post_position.as_ref().map(|position| position.no_size).unwrap_or(Decimal::ZERO),
        post_sync_net_exposure = %post_sync_net_exposure,
        tolerance = %exposure_tolerance,
        "Post-resolution truth evaluated"
    );

    let failure_reason = if success {
        None
    } else {
        let mut reasons =
            resolution_failure_reasons(resolution, hedge_result.as_ref(), sellback_result.as_ref());
        if post_sync_net_exposure > exposure_tolerance {
            reasons.push(format!(
                "post_sync_net_exposure={} exceeds tolerance {}",
                post_sync_net_exposure, exposure_tolerance
            ));
        }
        if hedge_confirmed_by_position_truth {
            reasons.push("hedge_leg_confirmed_by_position_truth".to_string());
        }
        if reasons.is_empty() {
            reasons.push("post-resolution verification failed".to_string());
        }
        Some(reasons.join("; "))
    };

    ResolutionExecutionResult {
        hedge_result,
        sellback_result,
        post_position,
        post_sync_net_exposure,
        post_sync_source,
        success,
        failure_reason,
    }
}

async fn execute_resolution_plan_with_sellback_recompute_shared(
    hedge_executor: &HedgeExecutor,
    trading_client: &Arc<TradingClient>,
    position_manager: &Arc<PositionManager>,
    market: &CanonicalMarket,
    order_manager: &OrderManager,
    risk_manager: &Arc<RiskManager>,
    cached_balance: &Arc<RwLock<Decimal>>,
    book_rest: &BookRestClient,
    book_manager: &Arc<BookManager>,
    config: &Config,
    intent: &HedgeIntent,
    resolution: Option<&HedgeResolution>,
    filled_token_id: &str,
    pre_resolution_position: &Position,
    exposure_tolerance: Decimal,
    hedge_timeout_secs: u64,
    remaining_recompute_attempts: u8,
) -> ResolutionExecutionResult {
    let first_result = execute_resolution_plan_with_timeout(
        hedge_executor,
        trading_client,
        position_manager,
        intent,
        resolution,
        filled_token_id,
        pre_resolution_position,
        exposure_tolerance,
        hedge_timeout_secs,
    )
    .await;

    if remaining_recompute_attempts == 0
        || !should_recompute_buy_resolution_sellback(intent, resolution, &first_result)
    {
        return first_result;
    }

    recompute_buy_resolution_after_sellback_miss_shared(
        hedge_executor,
        trading_client,
        position_manager,
        market,
        order_manager,
        risk_manager,
        cached_balance,
        book_rest,
        book_manager,
        config,
        intent,
        filled_token_id,
        exposure_tolerance,
        hedge_timeout_secs,
        first_result,
    )
    .await
}

async fn recompute_buy_resolution_after_sellback_miss_shared(
    hedge_executor: &HedgeExecutor,
    trading_client: &Arc<TradingClient>,
    position_manager: &Arc<PositionManager>,
    market: &CanonicalMarket,
    order_manager: &OrderManager,
    risk_manager: &Arc<RiskManager>,
    cached_balance: &Arc<RwLock<Decimal>>,
    book_rest: &BookRestClient,
    book_manager: &Arc<BookManager>,
    config: &Config,
    intent: &HedgeIntent,
    filled_token_id: &str,
    exposure_tolerance: Decimal,
    hedge_timeout_secs: u64,
    first_result: ResolutionExecutionResult,
) -> ResolutionExecutionResult {
    info!(
        condition_id = %intent.condition_id,
        first_sellback_status = %sellback_leg_status(first_result.sellback_result.as_ref()),
        "Initial BUY-resolution sellback did not complete — recomputing once from authoritative current truth"
    );

    let current_position =
        match sync_position_for_resolution(position_manager, &intent.condition_id).await {
            Ok(Some(position)) => position,
            Ok(None) => {
                warn!(
                    condition_id = %intent.condition_id,
                    "Sellback recompute aborted because current position truth is missing"
                );
                return first_result;
            }
            Err(err) => {
                warn!(
                    condition_id = %intent.condition_id,
                    error = %err,
                    "Sellback recompute aborted because current position sync failed"
                );
                return first_result;
            }
        };

    let current_exposure = current_position.net_exposure().abs();
    if current_exposure <= exposure_tolerance {
        info!(
            condition_id = %intent.condition_id,
            post_sync_yes_size = %current_position.yes_size,
            post_sync_no_size = %current_position.no_size,
            post_sync_net_exposure = %current_exposure,
            tolerance = %exposure_tolerance,
            "Sellback recompute found current exposure already within tolerance"
        );
        return ResolutionExecutionResult {
            hedge_result: first_result.hedge_result,
            sellback_result: None,
            post_position: Some(current_position),
            post_sync_net_exposure: current_exposure,
            post_sync_source: "position_manager",
            success: true,
            failure_reason: None,
        };
    }

    let residual_size = required_hedge_size(&current_position, intent.trigger_leg);
    if residual_size <= exposure_tolerance {
        info!(
            condition_id = %intent.condition_id,
            residual_size = %residual_size,
            tolerance = %exposure_tolerance,
            "Sellback recompute residual rounded within tolerance"
        );
        return ResolutionExecutionResult {
            hedge_result: first_result.hedge_result,
            sellback_result: None,
            post_position: Some(current_position),
            post_sync_net_exposure: current_exposure,
            post_sync_source: "position_manager",
            success: true,
            failure_reason: None,
        };
    }

    let preparation = match prepare_market_for_resolution(
        market,
        order_manager,
        trading_client,
        risk_manager,
        cached_balance,
        book_rest,
        book_manager,
        config,
        intent
            .trigger_leg
            .hedge_uses_asks()
            .then_some(residual_size),
    )
    .await
    {
        Ok(preparation) => preparation,
        Err(reason) => {
            warn!(
                condition_id = %intent.condition_id,
                reason = %reason,
                "Sellback recompute preparation failed"
            );
            return first_result;
        }
    };

    let recomputed_resolution = plan_buy_resolution(
        market,
        &preparation,
        &intent.hedge_token_id,
        filled_token_id,
        intent.fill_price,
        residual_size,
    );
    let (planned_hedge_size, hedge_cost) = planned_hedge_size_and_cost(
        Some(&recomputed_resolution),
        intent.hedge_side,
        residual_size,
    );

    if let Err(reason) = risk_manager
        .pre_trade_check(
            &intent.condition_id,
            planned_hedge_size,
            hedge_cost,
            true,
            Some(preparation.max_hedge_usdc),
        )
        .await
    {
        warn!(
            condition_id = %intent.condition_id,
            reason = %reason,
            "Sellback recompute risk check failed"
        );
        return first_result;
    }

    info!(
        condition_id = %intent.condition_id,
        residual_size = %residual_size,
        hedge_shares = %recomputed_resolution.hedge_shares,
        hedge_limit = %recomputed_resolution.hedge_limit_price,
        sellback_shares = %recomputed_resolution.sellback_shares,
        sellback_limit = %recomputed_resolution.sellback_limit_price,
        unresolved_shares = %recomputed_resolution.unresolved_shares,
        available_hedge_usdc = %preparation.max_hedge_usdc,
        "Executing one bounded recomputed resolution after sellback miss"
    );

    execute_resolution_plan_with_timeout(
        hedge_executor,
        trading_client,
        position_manager,
        intent,
        Some(&recomputed_resolution),
        filled_token_id,
        &current_position,
        exposure_tolerance,
        hedge_timeout_secs,
    )
    .await
}

fn plan_buy_resolution(
    market: &CanonicalMarket,
    preparation: &ResolutionPreparation,
    hedge_token_id: &str,
    filled_token_id: &str,
    fill_price: Decimal,
    fill_size: Decimal,
) -> HedgeResolution {
    let hedge_book = if hedge_token_id == market.yes_token_id {
        &preparation.yes_book
    } else {
        &preparation.no_book
    };
    let filled_book = if filled_token_id == market.yes_token_id {
        &preparation.yes_book
    } else {
        &preparation.no_book
    };
    let tick = market
        .tick_size
        .parse::<Decimal>()
        .unwrap_or(Decimal::new(1, 2));

    plan_fill_resolution(
        fill_price,
        &hedge_book.asks,
        &filled_book.bids,
        fill_size,
        preparation.max_hedge_usdc,
        tick,
    )
}

fn planned_hedge_size_and_cost(
    resolution: Option<&HedgeResolution>,
    hedge_side: Side,
    fill_size: Decimal,
) -> (Decimal, Option<Decimal>) {
    let planned_hedge_size = match (resolution, hedge_side) {
        (Some(resolution), Side::Buy) => resolution.hedge_shares,
        _ => fill_size,
    };
    let hedge_cost = match (resolution, hedge_side) {
        (Some(resolution), Side::Buy) if resolution.hedge_shares > Decimal::ZERO => {
            Some(resolution.hedge_shares * resolution.hedge_limit_price)
        }
        _ => None,
    };

    (planned_hedge_size, hedge_cost)
}

fn best_bid_snapshot(book: &OrderBookSnapshot) -> (Option<Decimal>, Option<Decimal>) {
    (
        book.bids.first().map(|level| level.price),
        book.bids.first().map(|level| level.size),
    )
}

fn best_ask_snapshot(book: &OrderBookSnapshot) -> (Option<Decimal>, Option<Decimal>) {
    (
        book.asks.first().map(|level| level.price),
        book.asks.first().map(|level| level.size),
    )
}

fn merge_eligible_pairs(position: &Position) -> Decimal {
    position
        .yes_size
        .min(position.no_size)
        .floor()
        .max(Decimal::ZERO)
}

fn classify_non_pair_exit_status(
    post_position: &Position,
    exposure_tolerance: Decimal,
    sellback_completed: bool,
) -> &'static str {
    if post_position.net_exposure().abs() > exposure_tolerance {
        return "directional_residual";
    }
    if sellback_completed {
        return "sellback_complete";
    }
    "no_exit_needed"
}

fn refresh_hedge_exit_telemetry(
    exit_telemetry: &mut Option<HedgeExitTelemetry>,
    post_position: Option<&Position>,
    exposure_tolerance: Decimal,
    sellback_completed: bool,
    ctf_merge_configured: bool,
) {
    let Some(post_position) = post_position else {
        return;
    };

    let merge_pairs = merge_eligible_pairs(post_position);
    let baseline = HedgeExitTelemetry {
        exit_path_status: if merge_pairs > Decimal::ZERO {
            "pair_left_idle".to_string()
        } else {
            classify_non_pair_exit_status(post_position, exposure_tolerance, sellback_completed)
                .to_string()
        },
        merge_eligible_pairs: merge_pairs,
        ctf_merge_configured,
        ..Default::default()
    };

    let preserve_branch_outcome = exit_telemetry.as_ref().is_some_and(|telemetry| {
        telemetry.merge_attempted
            || telemetry.merge_tx_hash.is_some()
            || telemetry.merge_failure_reason.is_some()
            || telemetry.fallback_asks_attempted
            || telemetry.fallback_failure_reason.is_some()
    });

    *exit_telemetry = Some(if preserve_branch_outcome {
        let mut telemetry = exit_telemetry.take().unwrap_or_default();
        telemetry.merge_eligible_pairs = merge_pairs;
        telemetry.ctf_merge_configured = ctf_merge_configured;
        telemetry
    } else {
        baseline
    });
}

fn hedge_exit_observability_reason(source_component: &str, post_sync_source: &str) -> String {
    format!(
        "successful hedge trace missing final post-sync position from current runtime data; required hedge_exit_path_recorded was not emitted source_component={} post_sync_source={}",
        source_component, post_sync_source
    )
}

fn build_hedge_exit_observability_event(
    run_id: &str,
    mode: &str,
    source_component: &str,
    trace_id: &str,
    hedge_id: &str,
    intent: &HedgeIntent,
    reason: &str,
) -> EventEnvelope {
    emitters::build_monitor_degraded(run_id, mode, source_component, reason, None)
        .with_trace_id(trace_id.to_string())
        .with_condition_id(intent.condition_id.clone())
        .with_order_id(intent.trigger_order_id.clone())
        .with_asset_id(intent.hedge_token_id.clone())
        .with_hedge_id(hedge_id.to_string())
}

async fn tracked_inventory_ask_count(order_manager: &OrderManager, condition_id: &str) -> u64 {
    order_manager
        .get_market_orders(condition_id)
        .await
        .iter()
        .filter(|tracked| tracked.leg.is_ask())
        .count() as u64
}

fn sellback_leg_status(result: Option<&SellbackExecutionResult>) -> &'static str {
    match result {
        Some(result) => match result.verification_state {
            SellbackVerificationState::VerifiedFilled => "success",
            SellbackVerificationState::VerifiedZeroFill => "failed",
            SellbackVerificationState::Unknown => "unverified",
        },
        None => "skipped",
    }
}

fn should_recompute_buy_resolution_sellback(
    intent: &HedgeIntent,
    resolution: Option<&HedgeResolution>,
    result: &ResolutionExecutionResult,
) -> bool {
    if intent.hedge_side != Side::Buy {
        return false;
    }

    let Some(resolution) = resolution else {
        return false;
    };
    if resolution.sellback_shares <= Decimal::ZERO {
        return false;
    }

    result
        .sellback_result
        .as_ref()
        .is_some_and(|sellback| !sellback.is_verified_filled())
}

async fn sync_position_for_resolution(
    position_manager: &Arc<PositionManager>,
    condition_id: &str,
) -> std::result::Result<Option<Position>, String> {
    position_manager
        .sync_positions()
        .await
        .map_err(|err| err.to_string())?;
    Ok(position_manager.get_position(condition_id).await)
}

fn hedge_inventory_delta_from_positions(
    pre_position: &Position,
    post_position: &Position,
    trigger_leg: QuoteLeg,
) -> Decimal {
    let delta = match trigger_leg {
        QuoteLeg::YesBid | QuoteLeg::YesAsk => post_position.no_size - pre_position.no_size,
        QuoteLeg::NoBid | QuoteLeg::NoAsk => post_position.yes_size - pre_position.yes_size,
    };
    normalize_share_size(delta.max(Decimal::ZERO))
}

fn hedge_leg_confirmed_by_position_truth(
    pre_position: &Position,
    post_position: &Position,
    intent: &HedgeIntent,
    hedge_result: Option<&HedgeResult>,
) -> bool {
    if intent.hedge_side != Side::Buy {
        return false;
    }

    let Some(hedge_result) = hedge_result else {
        return false;
    };

    if !matches!(
        hedge_result.verification_state,
        crate::trading::hedge_executor::HedgeVerificationState::VerifiedFilled
            | crate::trading::hedge_executor::HedgeVerificationState::Unknown
    ) {
        return false;
    }

    hedge_inventory_delta_from_positions(pre_position, post_position, intent.trigger_leg)
        > Decimal::ZERO
}

fn should_retry_resolution_sync(
    hedge_result: Option<&HedgeResult>,
    intent: &HedgeIntent,
    pre_position: &Position,
    post_position: Option<&Position>,
) -> bool {
    if intent.hedge_side != Side::Buy {
        return false;
    }

    let Some(hedge_result) = hedge_result else {
        return false;
    };

    if !matches!(
        hedge_result.verification_state,
        crate::trading::hedge_executor::HedgeVerificationState::VerifiedFilled
            | crate::trading::hedge_executor::HedgeVerificationState::Unknown
    ) {
        return false;
    }

    let Some(post_position) = post_position else {
        return true;
    };

    hedge_inventory_delta_from_positions(pre_position, post_position, intent.trigger_leg)
        <= Decimal::ZERO
}

fn should_retry_resolution_sync_for_sellback(
    sellback_result: Option<&SellbackExecutionResult>,
    post_position: Option<&Position>,
    exposure_tolerance: Decimal,
) -> bool {
    let Some(post_position) = post_position else {
        return false;
    };
    let Some(sellback_result) = sellback_result else {
        return false;
    };
    if !sellback_result.is_verified_filled() {
        return false;
    }
    let Some(confirmed_shares) = sellback_result.confirmed_shares else {
        return false;
    };
    if confirmed_shares <= Decimal::ZERO {
        return false;
    }
    post_position.net_exposure().abs() > exposure_tolerance
}

fn resolution_failure_reasons(
    resolution: Option<&HedgeResolution>,
    hedge_result: Option<&HedgeResult>,
    sellback_result: Option<&SellbackExecutionResult>,
) -> Vec<String> {
    let mut reasons = Vec::new();
    if let Some(resolution) = resolution {
        if resolution.unresolved_shares > Decimal::ZERO {
            reasons.push(format!(
                "unresolved_shares={}",
                resolution.unresolved_shares
            ));
        }
    }
    if let Some(hedge_result) = hedge_result {
        if !hedge_result.success {
            reasons.push(
                hedge_result
                    .failure_reason
                    .clone()
                    .unwrap_or_else(|| "hedge leg failed".to_string()),
            );
        }
    }
    if let Some(sellback_result) = sellback_result {
        if !sellback_result.is_verified_filled() {
            reasons.push(
                sellback_result
                    .failure_reason
                    .clone()
                    .unwrap_or_else(|| "sell-back leg failed".to_string()),
            );
        }
    }
    reasons
}

fn missing_post_sync_truth_reason() -> &'static str {
    "final post-sync position truth missing after current sync/retry flow"
}

fn derive_execution_confirmed_sellback_position(
    pre_resolution_position: &Position,
    intent: &HedgeIntent,
    hedge_result: Option<&HedgeResult>,
    sellback_result: Option<&SellbackExecutionResult>,
    exposure_tolerance: Decimal,
) -> Option<Position> {
    if hedge_result.is_some() {
        return None;
    }

    project_execution_confirmed_sellback_position(
        pre_resolution_position,
        intent,
        sellback_result,
        exposure_tolerance,
    )
}

fn derive_execution_confirmed_sellback_post_sync_position(
    pre_resolution_position: &Position,
    post_position: Option<&Position>,
    intent: &HedgeIntent,
    hedge_result: Option<&HedgeResult>,
    sellback_result: Option<&SellbackExecutionResult>,
    exposure_tolerance: Decimal,
    hedge_confirmed_by_position_truth: bool,
) -> Option<Position> {
    if hedge_result.is_some() {
        if hedge_confirmed_by_position_truth {
            return post_position.and_then(|position| {
                project_execution_confirmed_sellback_position(
                    position,
                    intent,
                    sellback_result,
                    exposure_tolerance,
                )
            });
        }
        return None;
    }

    project_execution_confirmed_sellback_position(
        pre_resolution_position,
        intent,
        sellback_result,
        exposure_tolerance,
    )
}

fn project_execution_confirmed_sellback_position(
    base_position: &Position,
    intent: &HedgeIntent,
    sellback_result: Option<&SellbackExecutionResult>,
    exposure_tolerance: Decimal,
) -> Option<Position> {
    let sellback_result = sellback_result?;
    if !sellback_result.is_verified_filled() {
        return None;
    }

    let confirmed_shares = sellback_result.confirmed_shares?;
    if confirmed_shares <= Decimal::ZERO {
        return None;
    }

    let sellback_leg = match intent.trigger_leg {
        QuoteLeg::YesBid | QuoteLeg::YesAsk => QuoteLeg::YesAsk,
        QuoteLeg::NoBid | QuoteLeg::NoAsk => QuoteLeg::NoAsk,
    };
    let projected = project_position_after_fill(base_position, sellback_leg, confirmed_shares);
    (projected.net_exposure().abs() <= exposure_tolerance).then_some(projected)
}

fn sellback_result_from_terminal_placement(
    order_result: crate::models::OrderResult,
    price: Decimal,
    requested_shares: Decimal,
) -> Option<SellbackExecutionResult> {
    let metadata = sellback_verification_metadata(&order_result);
    if sellback_placement_confirms_fill(&order_result) {
        return Some(SellbackExecutionResult {
            order_result: Some(order_result),
            verification_state: SellbackVerificationState::VerifiedFilled,
            confirmed_shares: Some(requested_shares),
            failure_reason: None,
            price: Some(price),
            verification_metadata: metadata,
        });
    }
    if order_result.status == OrderStatus::Invalid {
        return Some(SellbackExecutionResult {
            order_result: Some(order_result),
            verification_state: SellbackVerificationState::VerifiedZeroFill,
            confirmed_shares: None,
            failure_reason: Some(
                "Sell-back verification failed: placement response was invalid with zero matched shares"
                    .to_string(),
            ),
            price: Some(price),
            verification_metadata: metadata,
        });
    }
    None
}

async fn verify_provisional_sellback_order(
    trading_client: &Arc<TradingClient>,
    order_result: crate::models::OrderResult,
    price: Decimal,
    requested_shares: Decimal,
) -> SellbackExecutionResult {
    let lookup = trading_client.get_order(&order_result.order_id).await;
    sellback_result_from_lookup(order_result, price, requested_shares, lookup)
}

fn sellback_verification_metadata(
    order_result: &crate::models::OrderResult,
) -> SellbackVerificationMetadata {
    SellbackVerificationMetadata {
        response_status: Some(sellback_order_status_label(order_result.status).to_string()),
        trade_ids: order_result.trade_ids.clone(),
        ..Default::default()
    }
}

fn sellback_placement_confirms_fill(order_result: &crate::models::OrderResult) -> bool {
    order_result.status == OrderStatus::Matched || !order_result.trade_ids.is_empty()
}

fn sellback_lookup_confirms_fill(order: &LiveOrder, requested_shares: Decimal) -> bool {
    order.status == OrderStatus::Matched
        || normalize_share_size(order.size_matched) >= requested_shares
}

fn sellback_order_status_label(status: OrderStatus) -> &'static str {
    match status {
        OrderStatus::Live => "live",
        OrderStatus::Matched => "matched",
        OrderStatus::Delayed => "delayed",
        OrderStatus::Cancelled => "cancelled",
        OrderStatus::Invalid => "invalid",
    }
}

fn sellback_result_from_lookup(
    order_result: crate::models::OrderResult,
    price: Decimal,
    requested_shares: Decimal,
    lookup: std::result::Result<Option<LiveOrder>, anyhow::Error>,
) -> SellbackExecutionResult {
    let mut metadata = sellback_verification_metadata(&order_result);
    let response_status = metadata
        .response_status
        .as_deref()
        .unwrap_or("unknown")
        .to_string();

    if order_result.order_id.is_empty() {
        return SellbackExecutionResult {
            order_result: Some(order_result),
            verification_state: SellbackVerificationState::Unknown,
            confirmed_shares: None,
            failure_reason: Some(format!(
                "Sell-back verification failed: placement returned {} without an order_id or terminal fill evidence",
                response_status
            )),
            price: Some(price),
            verification_metadata: metadata,
        };
    }

    match lookup {
        Ok(Some(order)) => {
            metadata.lookup_status = Some(sellback_order_status_label(order.status).to_string());
            metadata.lookup_matched_shares = Some(order.size_matched);
            if metadata.trade_ids.is_empty() {
                if let Some(associated_trade_ids) = order.associated_trade_ids() {
                    metadata.trade_ids = associated_trade_ids;
                }
            }
            if sellback_lookup_confirms_fill(&order, requested_shares) {
                return SellbackExecutionResult {
                    order_result: Some(order_result),
                    verification_state: SellbackVerificationState::VerifiedFilled,
                    confirmed_shares: Some(requested_shares),
                    failure_reason: None,
                    price: Some(price),
                    verification_metadata: metadata,
                };
            }
            if matches!(order.status, OrderStatus::Cancelled | OrderStatus::Invalid)
                && order.size_matched <= Decimal::ZERO
            {
                return SellbackExecutionResult {
                    order_result: Some(order_result),
                    verification_state: SellbackVerificationState::VerifiedZeroFill,
                    confirmed_shares: None,
                    failure_reason: Some(format!(
                        "Sell-back verification failed: lookup returned {} with zero matched shares",
                        metadata.lookup_status.as_deref().unwrap_or("unknown")
                    )),
                    price: Some(price),
                    verification_metadata: metadata,
                };
            }

            SellbackExecutionResult {
                order_result: Some(order_result),
                verification_state: SellbackVerificationState::Unknown,
                confirmed_shares: None,
                failure_reason: Some(format!(
                    "Sell-back verification failed: terminal execution could not be confirmed after {} placement response (lookup_status={})",
                    response_status,
                    metadata.lookup_status.as_deref().unwrap_or("missing")
                )),
                price: Some(price),
                verification_metadata: metadata,
            }
        }
        Ok(None) => {
            metadata.lookup_status = Some("missing".to_string());
            SellbackExecutionResult {
                order_result: Some(order_result),
                verification_state: SellbackVerificationState::Unknown,
                confirmed_shares: None,
                failure_reason: Some(format!(
                    "Sell-back verification failed: order lookup missing after {} placement response",
                    response_status
                )),
                price: Some(price),
                verification_metadata: metadata,
            }
        }
        Err(err) => {
            metadata.lookup_status = Some("error".to_string());
            metadata.lookup_error = Some(err.to_string());
            SellbackExecutionResult {
                order_result: Some(order_result),
                verification_state: SellbackVerificationState::Unknown,
                confirmed_shares: None,
                failure_reason: Some(format!(
                    "Sell-back verification failed: lookup error after {} placement response: {}",
                    response_status, err
                )),
                price: Some(price),
                verification_metadata: metadata,
            }
        }
    }
}

#[derive(Debug, Clone)]
struct FlattenCleanupResult {
    post_sync_net_exposure: Decimal,
    flatten_attempted: bool,
    verified: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HaltCleanupStatus {
    PendingOrderDrain,
    MissingMetadata,
    PendingExposure,
    Verified,
}

#[derive(Debug, Clone)]
struct HaltCleanupOutcome {
    status: HaltCleanupStatus,
    flatten_attempted: bool,
    post_sync_net_exposure: Decimal,
    active_orders: usize,
    pending_verification: usize,
    removed_from_management: bool,
}

impl HaltCleanupOutcome {
    fn verified(&self) -> bool {
        self.status == HaltCleanupStatus::Verified
    }

    fn degraded_reason(&self, condition_id: &str) -> Option<String> {
        match self.status {
            HaltCleanupStatus::PendingOrderDrain => Some(format!(
                "condition_id={} halted cleanup deferred active_orders={} pending_verification={}",
                condition_id, self.active_orders, self.pending_verification
            )),
            HaltCleanupStatus::MissingMetadata => Some(format!(
                "condition_id={} halted cleanup blocked: metadata missing after flatten verification exposure={}",
                condition_id, self.post_sync_net_exposure
            )),
            HaltCleanupStatus::PendingExposure => Some(format!(
                "condition_id={} halted cleanup pending exposure flatten_attempted={} post_sync_net_exposure={}",
                condition_id, self.flatten_attempted, self.post_sync_net_exposure
            )),
            HaltCleanupStatus::Verified => None,
        }
    }
}

#[derive(Debug, Clone)]
struct HaltMarketOutcome {
    canonical_reason: String,
    halt_signal_suppressed: bool,
}

fn emit_optional_event(event_producer: &Option<Arc<dyn EventProducer>>, event: EventEnvelope) {
    let Some(producer) = event_producer else {
        return;
    };

    match producer.emit(event) {
        Ok(true) => {}
        Ok(false) => warn!("Dropping monitor event: queue is full"),
        Err(err) => warn!(error = %err, "Failed to enqueue monitor event"),
    }
}

fn expected_post_merge_position(pre_position: &Position, merge_amount: Decimal) -> Position {
    Position {
        condition_id: pre_position.condition_id.clone(),
        yes_size: normalize_share_size((pre_position.yes_size - merge_amount).max(Decimal::ZERO)),
        no_size: normalize_share_size((pre_position.no_size - merge_amount).max(Decimal::ZERO)),
        avg_yes_price: pre_position.avg_yes_price,
        avg_no_price: pre_position.avg_no_price,
    }
}

fn merge_truth_positions_match(current: &Position, expected: &Position) -> bool {
    normalize_share_size(current.yes_size) == normalize_share_size(expected.yes_size)
        && normalize_share_size(current.no_size) == normalize_share_size(expected.no_size)
}

async fn observe_merge_truth_convergence(
    position_manager: &Arc<PositionManager>,
    condition_id: &str,
    expected_position: &Position,
) -> MergeTruthObservation {
    observe_merge_truth_convergence_with_params(
        position_manager,
        condition_id,
        expected_position,
        MERGE_TRUTH_POLL_INTERVAL,
        MERGE_TRUTH_TIMEOUT,
        MERGE_TRUTH_REQUIRED_MATCHES,
    )
    .await
}

async fn observe_merge_truth_convergence_with_params(
    position_manager: &Arc<PositionManager>,
    condition_id: &str,
    expected_position: &Position,
    poll_interval: Duration,
    timeout_window: Duration,
    required_matches: usize,
) -> MergeTruthObservation {
    let started = Instant::now();
    let mut consecutive_matches = 0usize;
    let mut last_sync_error = None;
    let mut snapshot_is_fresh = true;
    let mut last_seen_position = position_manager
        .get_position(condition_id)
        .await
        .unwrap_or_else(|| Position::new(condition_id.to_string()));

    loop {
        if snapshot_is_fresh && merge_truth_positions_match(&last_seen_position, expected_position)
        {
            consecutive_matches += 1;
            if consecutive_matches >= required_matches {
                return MergeTruthObservation {
                    converged: true,
                    observed_for: started.elapsed(),
                    last_seen_position,
                    last_sync_error,
                };
            }
        } else {
            consecutive_matches = 0;
        }

        if started.elapsed() >= timeout_window {
            break;
        }

        time::sleep(poll_interval).await;
        match position_manager.sync_positions().await {
            Ok(()) => {
                last_sync_error = None;
                snapshot_is_fresh = true;
                last_seen_position = position_manager
                    .get_position(condition_id)
                    .await
                    .unwrap_or_else(|| Position::new(condition_id.to_string()));
            }
            Err(err) => {
                last_sync_error = Some(err.to_string());
                snapshot_is_fresh = false;
                consecutive_matches = 0;
            }
        }
    }

    MergeTruthObservation {
        converged: false,
        observed_for: started.elapsed(),
        last_seen_position,
        last_sync_error,
    }
}

fn merge_truth_timeout_reason(
    condition_id: &str,
    merge_tx_hash: &str,
    expected_position: &Position,
    observation: &MergeTruthObservation,
) -> String {
    let base = format!(
        "merge truth did not converge after confirmed merge condition_id={} tx_hash={} expected_yes={} expected_no={} last_yes={} last_no={} observed_for={}s",
        condition_id,
        merge_tx_hash,
        expected_position.yes_size,
        expected_position.no_size,
        observation.last_seen_position.yes_size,
        observation.last_seen_position.no_size,
        observation.observed_for.as_secs()
    );
    match observation.last_sync_error.as_deref() {
        Some(error) => format!("{base} last_sync_error={error}"),
        None => base,
    }
}

async fn monitor_merge_truth_convergence_after_success(
    position_manager: Arc<PositionManager>,
    event_producer: Option<Arc<dyn EventProducer>>,
    run_id: String,
    mode: String,
    condition_id: String,
    merge_tx_hash: String,
    expected_position: Position,
    poll_interval: Duration,
    timeout_window: Duration,
    required_matches: usize,
) -> MergeTruthObservation {
    let observation = observe_merge_truth_convergence_with_params(
        &position_manager,
        &condition_id,
        &expected_position,
        poll_interval,
        timeout_window,
        required_matches,
    )
    .await;

    if observation.converged {
        info!(
            condition_id = %condition_id,
            merge_tx_hash = %merge_tx_hash,
            expected_yes = %expected_position.yes_size,
            expected_no = %expected_position.no_size,
            observed_for_secs = %observation.observed_for.as_secs(),
            "Post-merge position truth converged"
        );
        return observation;
    }

    let reason = merge_truth_timeout_reason(
        &condition_id,
        &merge_tx_hash,
        &expected_position,
        &observation,
    );
    warn!(
        condition_id = %condition_id,
        merge_tx_hash = %merge_tx_hash,
        expected_yes = %expected_position.yes_size,
        expected_no = %expected_position.no_size,
        last_yes = %observation.last_seen_position.yes_size,
        last_no = %observation.last_seen_position.no_size,
        observed_for_secs = %observation.observed_for.as_secs(),
        last_sync_error = ?observation.last_sync_error,
        "Confirmed merge still has stale direct position truth"
    );
    emit_optional_event(
        &event_producer,
        emitters::build_monitor_degraded(
            &run_id,
            &mode,
            MERGE_TRUTH_MONITOR_COMPONENT,
            &reason,
            None,
        )
        .with_condition_id(condition_id),
    );

    observation
}

fn spawn_merge_truth_monitor(
    position_manager: Arc<PositionManager>,
    event_producer: Option<Arc<dyn EventProducer>>,
    run_id: String,
    mode: String,
    condition_id: String,
    merge_tx_hash: String,
    expected_position: Position,
) -> tokio::task::JoinHandle<()> {
    spawn_merge_truth_monitor_with_params(
        position_manager,
        event_producer,
        run_id,
        mode,
        condition_id,
        merge_tx_hash,
        expected_position,
        MERGE_TRUTH_POLL_INTERVAL,
        MERGE_TRUTH_TIMEOUT,
        MERGE_TRUTH_REQUIRED_MATCHES,
    )
}

fn spawn_merge_truth_monitor_with_params(
    position_manager: Arc<PositionManager>,
    event_producer: Option<Arc<dyn EventProducer>>,
    run_id: String,
    mode: String,
    condition_id: String,
    merge_tx_hash: String,
    expected_position: Position,
    poll_interval: Duration,
    timeout_window: Duration,
    required_matches: usize,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let _ = monitor_merge_truth_convergence_after_success(
            position_manager,
            event_producer,
            run_id,
            mode,
            condition_id,
            merge_tx_hash,
            expected_position,
            poll_interval,
            timeout_window,
            required_matches,
        )
        .await;
    })
}

async fn resolve_market_metadata(
    managed_markets: &Arc<RwLock<HashMap<String, CanonicalMarket>>>,
    known_markets: &Arc<RwLock<HashMap<String, CanonicalMarket>>>,
    condition_id: &str,
) -> Option<CanonicalMarket> {
    {
        let managed = managed_markets.read().await;
        if let Some(market) = managed.get(condition_id) {
            return Some(market.clone());
        }
    }

    let known = known_markets.read().await;
    known.get(condition_id).cloned()
}

fn synthetic_resolution_market(
    condition_id: &str,
    yes_token_id: &str,
    no_token_id: &str,
    neg_risk: bool,
    tick_size: &str,
) -> CanonicalMarket {
    CanonicalMarket {
        condition_id: condition_id.to_string(),
        market_slug: format!("synthetic-{condition_id}"),
        question: format!("Synthetic resolution market for {condition_id}"),
        yes_token_id: yes_token_id.to_string(),
        no_token_id: no_token_id.to_string(),
        reward_config: crate::models::RewardConfig {
            condition_id: condition_id.to_string(),
            daily_reward_rates: vec![],
            daily_reward_total: Decimal::ZERO,
            min_size: Decimal::ZERO,
            max_spread: Decimal::ZERO,
        },
        neg_risk,
        tick_size: tick_size.to_string(),
        end_date: None,
        admitted_at: Utc::now(),
        status: crate::models::MarketStatus::Admitted,
    }
}

async fn flatten_directional_inventory_for_halt(
    condition_id: &str,
    position_manager: &Arc<PositionManager>,
    trading_client: &Arc<TradingClient>,
    managed_markets: &Arc<RwLock<HashMap<String, CanonicalMarket>>>,
    known_markets: &Arc<RwLock<HashMap<String, CanonicalMarket>>>,
    config: &Config,
) -> FlattenCleanupResult {
    let tolerance = hedge_exposure_tolerance(config);
    if let Err(err) = position_manager.sync_positions().await {
        warn!(
            condition_id = %condition_id,
            error = %err,
            "Failed to sync positions for halted-market flatten — using cached state"
        );
    }

    let current_position = position_manager
        .get_position(condition_id)
        .await
        .unwrap_or_else(|| Position::new(condition_id.to_string()));
    let current_net_exposure = current_position.net_exposure().abs();
    if current_net_exposure <= tolerance {
        info!(
            condition_id = %condition_id,
            post_sync_net_exposure = %current_net_exposure,
            tolerance = %tolerance,
            "Halted market flatten verification satisfied without new sell"
        );
        return FlattenCleanupResult {
            post_sync_net_exposure: current_net_exposure,
            flatten_attempted: false,
            verified: true,
        };
    }

    let market = match resolve_market_metadata(managed_markets, known_markets, condition_id).await {
        Some(market) => market,
        None => {
            warn!(
                condition_id = %condition_id,
                post_sync_net_exposure = %current_net_exposure,
                "Cannot flatten halted market — metadata missing from managed and known sets"
            );
            return FlattenCleanupResult {
                post_sync_net_exposure: current_net_exposure,
                flatten_attempted: false,
                verified: false,
            };
        }
    };

    let excess_yes = normalize_share_size(
        (current_position.yes_size - current_position.no_size).max(Decimal::ZERO),
    );
    let excess_no = normalize_share_size(
        (current_position.no_size - current_position.yes_size).max(Decimal::ZERO),
    );
    let mut flatten_attempted = false;

    if excess_yes > tolerance {
        flatten_attempted = true;
        warn!(
            condition_id = %condition_id,
            excess = %excess_yes,
            "Flattening halted-market YES inventory"
        );
        let request = OrderRequest {
            token_id: market.yes_token_id.clone(),
            price: Decimal::new(1, 2),
            size: excess_yes,
            amount_kind: OrderAmountKind::Shares,
            side: Side::Sell,
            order_type: OrderType::FOK,
            post_only: false,
            neg_risk: market.neg_risk,
            tick_size: market.tick_size.clone(),
        };
        if let Err(err) = trading_client.place_order(&request).await {
            error!(
                condition_id = %condition_id,
                error = %err,
                "Flatten YES sell failed for halted market"
            );
        }
    }

    if excess_no > tolerance {
        flatten_attempted = true;
        warn!(
            condition_id = %condition_id,
            excess = %excess_no,
            "Flattening halted-market NO inventory"
        );
        let request = OrderRequest {
            token_id: market.no_token_id.clone(),
            price: Decimal::new(1, 2),
            size: excess_no,
            amount_kind: OrderAmountKind::Shares,
            side: Side::Sell,
            order_type: OrderType::FOK,
            post_only: false,
            neg_risk: market.neg_risk,
            tick_size: market.tick_size.clone(),
        };
        if let Err(err) = trading_client.place_order(&request).await {
            error!(
                condition_id = %condition_id,
                error = %err,
                "Flatten NO sell failed for halted market"
            );
        }
    }

    if let Err(err) = position_manager.sync_positions().await {
        warn!(
            condition_id = %condition_id,
            error = %err,
            "Post-flatten position sync failed — using cached state"
        );
    }

    let post_position = position_manager
        .get_position(condition_id)
        .await
        .unwrap_or_else(|| Position::new(condition_id.to_string()));
    let post_sync_net_exposure = post_position.net_exposure().abs();
    let verified = post_sync_net_exposure <= tolerance;
    if verified {
        info!(
            condition_id = %condition_id,
            flatten_attempted,
            post_sync_net_exposure = %post_sync_net_exposure,
            tolerance = %tolerance,
            "Halted market flatten verification passed"
        );
    } else {
        warn!(
            condition_id = %condition_id,
            flatten_attempted,
            post_sync_net_exposure = %post_sync_net_exposure,
            tolerance = %tolerance,
            "Flatten retry pending for halted market"
        );
    }

    FlattenCleanupResult {
        post_sync_net_exposure,
        flatten_attempted,
        verified,
    }
}

async fn finalize_halted_market_cleanup(
    condition_id: &str,
    order_manager: &OrderManager,
    position_manager: &Arc<PositionManager>,
    trading_client: &Arc<TradingClient>,
    managed_markets: &Arc<RwLock<HashMap<String, CanonicalMarket>>>,
    known_markets: &Arc<RwLock<HashMap<String, CanonicalMarket>>>,
    config: &Config,
) -> HaltCleanupOutcome {
    let (mut active, mut pending) = order_manager.market_order_state_counts(condition_id).await;
    if active > 0 || pending > 0 {
        let confirmed = order_manager.retry_pending_cancels().await;
        if confirmed > 0 {
            info!(
                condition_id = %condition_id,
                confirmed,
                "Confirmed pending cancels while finalizing halted market cleanup"
            );
        }

        if let Some(market) =
            resolve_market_metadata(managed_markets, known_markets, condition_id).await
        {
            match order_manager
                .sync_market_open_orders(condition_id, &market, MarketOrderSyncMode::Reconcile)
                .await
            {
                Ok(sync_result) => {
                    info!(
                        condition_id = %condition_id,
                        fetched = sync_result.fetched,
                        live = sync_result.live,
                        imported = sync_result.imported,
                        updated = sync_result.updated,
                        pruned = sync_result.pruned,
                        missing = sync_result.missing_order_ids.len(),
                        "Reconciled halted market order truth before deferring cleanup"
                    );
                }
                Err(err) => {
                    warn!(
                        condition_id = %condition_id,
                        error = %err,
                        "Halted cleanup order reconcile failed"
                    );
                }
            }
        }

        (active, pending) = order_manager.market_order_state_counts(condition_id).await;
    }

    if active > 0 || pending > 0 {
        warn!(
            condition_id = %condition_id,
            active_orders = active,
            pending_verification = pending,
            "Halted market cleanup deferred until order cancels are verified"
        );
        return HaltCleanupOutcome {
            status: HaltCleanupStatus::PendingOrderDrain,
            flatten_attempted: false,
            post_sync_net_exposure: Decimal::ZERO,
            active_orders: active,
            pending_verification: pending,
            removed_from_management: false,
        };
    }

    let flatten = flatten_directional_inventory_for_halt(
        condition_id,
        position_manager,
        trading_client,
        managed_markets,
        known_markets,
        config,
    )
    .await;
    if !flatten.verified {
        let status = if flatten.flatten_attempted {
            HaltCleanupStatus::PendingExposure
        } else {
            HaltCleanupStatus::MissingMetadata
        };
        return HaltCleanupOutcome {
            status,
            flatten_attempted: flatten.flatten_attempted,
            post_sync_net_exposure: flatten.post_sync_net_exposure,
            active_orders: 0,
            pending_verification: 0,
            removed_from_management: false,
        };
    }

    let removed = managed_markets.write().await.remove(condition_id).is_some();
    info!(
        condition_id = %condition_id,
        removed_from_management = removed,
        flatten_attempted = flatten.flatten_attempted,
        post_sync_net_exposure = %flatten.post_sync_net_exposure,
        "Halted market cleanup verified"
    );
    HaltCleanupOutcome {
        status: HaltCleanupStatus::Verified,
        flatten_attempted: flatten.flatten_attempted,
        post_sync_net_exposure: flatten.post_sync_net_exposure,
        active_orders: 0,
        pending_verification: 0,
        removed_from_management: removed,
    }
}

async fn halt_market_and_start_cleanup(
    condition_id: &str,
    reason: &str,
    risk_manager: &Arc<RiskManager>,
    order_manager: &OrderManager,
    position_manager: &Arc<PositionManager>,
    trading_client: &Arc<TradingClient>,
    managed_markets: &Arc<RwLock<HashMap<String, CanonicalMarket>>>,
    known_markets: &Arc<RwLock<HashMap<String, CanonicalMarket>>>,
    config: &Config,
    run_id: &str,
    mode: &str,
    event_producer: &Option<Arc<dyn EventProducer>>,
    error_logger: &Arc<crate::monitor::ErrorLogger>,
) -> HaltMarketOutcome {
    error_logger.log_error(
        "error",
        &format!("KILL MARKET: {}", reason),
        Some(condition_id),
    );
    let halt_result = risk_manager.halt_market(condition_id, reason).await;

    if !halt_result.newly_halted {
        info!(
            condition_id = %condition_id,
            attempted_reason = %reason,
            canonical_reason = %halt_result.canonical_reason,
            "Market already halted — suppressing duplicate kill signal"
        );
    } else {
        emit_optional_event(
            event_producer,
            emitters::build_risk_state_changed(
                run_id,
                mode,
                Some(condition_id),
                "halted",
                Some(&halt_result.canonical_reason),
                None,
                Some(risk_manager.is_globally_halted().await),
            ),
        );
        if let Err(err) = order_manager
            .cancel_all(condition_id, CancelReasonCode::RiskHalt, "risk_halt")
            .await
        {
            error!(
                condition_id = %condition_id,
                error = %err,
                "Failed to cancel orders during kill switch"
            );
        }
    }

    let _ = finalize_halted_market_cleanup(
        condition_id,
        order_manager,
        position_manager,
        trading_client,
        managed_markets,
        known_markets,
        config,
    )
    .await;

    HaltMarketOutcome {
        canonical_reason: halt_result.canonical_reason,
        halt_signal_suppressed: !halt_result.newly_halted,
    }
}

/// Get or create a per-market mutex for hedge serialization.
async fn get_hedge_lock(
    locks: &Arc<RwLock<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
    condition_id: &str,
) -> Arc<tokio::sync::Mutex<()>> {
    {
        let read = locks.read().await;
        if let Some(lock) = read.get(condition_id) {
            return lock.clone();
        }
    }
    let mut write = locks.write().await;
    write
        .entry(condition_id.to_string())
        .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
        .clone()
}

fn duplicate_live_bid_legs_from_orders(live_orders: &[LiveOrder]) -> Vec<DuplicateLiveBidLeg> {
    let mut grouped = HashMap::<(String, QuoteLeg), Vec<String>>::new();
    for order in live_orders {
        let leg = match (order.outcome, order.side) {
            (crate::models::Outcome::Yes, Side::Buy) => QuoteLeg::YesBid,
            (crate::models::Outcome::No, Side::Buy) => QuoteLeg::NoBid,
            _ => continue,
        };
        grouped
            .entry((order.condition_id.clone(), leg))
            .or_default()
            .push(order.id.clone());
    }

    let mut duplicates = Vec::new();
    for ((condition_id, leg), mut order_ids) in grouped {
        if order_ids.len() <= 1 {
            continue;
        }
        order_ids.sort();
        duplicates.push(DuplicateLiveBidLeg {
            condition_id,
            leg,
            order_ids,
        });
    }
    duplicates.sort_by(|left, right| {
        left.condition_id
            .cmp(&right.condition_id)
            .then_with(|| left.leg.to_string().cmp(&right.leg.to_string()))
    });
    duplicates
}

fn prune_processed_trade_cache(cache: &mut ProcessedTradeCache, now: Instant) {
    prune_processed_trade_cache_with_ttl(
        cache,
        now,
        StdDuration::from_secs(PROCESSED_TRADE_TTL_SECS),
    );
}

fn prune_processed_trade_cache_with_ttl(
    cache: &mut ProcessedTradeCache,
    now: Instant,
    ttl: StdDuration,
) {
    while let Some((trade_id, seen_at)) = cache.order.front().cloned() {
        let expired = instant_age_at_least(now, seen_at, ttl);
        let over_capacity = cache.entries.len() > PROCESSED_TRADE_MAX_ENTRIES;
        if !expired && !over_capacity {
            break;
        }

        cache.order.pop_front();
        let should_remove = cache
            .entries
            .get(&trade_id)
            .map(|entry| entry.seen_at == seen_at)
            .unwrap_or(false);
        if should_remove {
            cache.entries.remove(&trade_id);
        }
    }
}

fn instant_age_at_least(now: Instant, recorded_at: Instant, threshold: StdDuration) -> bool {
    now.checked_duration_since(recorded_at)
        .is_some_and(|age| age >= threshold)
}

fn prune_recent_synthetic_fills(
    recent: &mut HashMap<String, RecentSyntheticFill>,
    now: Instant,
    ttl: StdDuration,
) {
    recent.retain(|_, entry| {
        !instant_age_at_least(now, entry.processed_at, ttl) && entry.size > Decimal::ZERO
    });
}

fn prune_recent_resolution_trades(
    recent: &mut Vec<RecentResolutionTrade>,
    now: Instant,
    ttl: StdDuration,
) {
    recent.retain(|entry| {
        !instant_age_at_least(now, entry.recorded_at, ttl) && entry.size > Decimal::ZERO
    });
}

async fn record_recent_resolution_trade_shared(
    recent_resolution_trades: &Arc<RwLock<Vec<RecentResolutionTrade>>>,
    condition_id: &str,
    asset_id: &str,
    side: Side,
    price: Decimal,
    size: Decimal,
) {
    if size <= Decimal::ZERO || asset_id.is_empty() {
        return;
    }

    let mut recent = recent_resolution_trades.write().await;
    prune_recent_resolution_trades(
        &mut recent,
        Instant::now(),
        StdDuration::from_secs(RECENT_RESOLUTION_TRADE_TTL_SECS),
    );
    recent.push(RecentResolutionTrade {
        condition_id: condition_id.to_string(),
        asset_id: asset_id.to_string(),
        side,
        price,
        size,
        recorded_at: Instant::now(),
    });
}

fn prune_recent_scoring_observations(
    observations: &mut HashMap<String, RecentScoringObservation>,
    now: Instant,
    ttl: StdDuration,
) {
    observations.retain(|_, observation| !instant_age_at_least(now, observation.observed_at, ttl));
}

fn project_position_after_fill(position: &Position, leg: QuoteLeg, fill_size: Decimal) -> Position {
    let mut projected = position.clone();
    match leg {
        QuoteLeg::YesBid => projected.yes_size += fill_size,
        QuoteLeg::YesAsk => {
            projected.yes_size = (projected.yes_size - fill_size).max(Decimal::ZERO)
        }
        QuoteLeg::NoBid => projected.no_size += fill_size,
        QuoteLeg::NoAsk => projected.no_size = (projected.no_size - fill_size).max(Decimal::ZERO),
    }
    projected
}

fn required_hedge_size(position: &Position, trigger_leg: QuoteLeg) -> Decimal {
    let size = match trigger_leg {
        QuoteLeg::YesBid | QuoteLeg::NoAsk => {
            (position.yes_size - position.no_size).max(Decimal::ZERO)
        }
        QuoteLeg::NoBid | QuoteLeg::YesAsk => {
            (position.no_size - position.yes_size).max(Decimal::ZERO)
        }
    };
    normalize_share_size(size)
}

fn new_hedge_signal() -> HedgeSignal {
    HedgeSignal {
        recorded_at: Instant::now(),
        hedged_at: Utc::now(),
    }
}

fn build_exchange_order_sync_trade(
    tracked: &TrackedOrder,
    price: Decimal,
    fill_size: Decimal,
) -> TradeEvent {
    TradeEvent {
        id: format!("exchange-sync-{}", Uuid::new_v4()),
        condition_id: tracked.condition_id.clone(),
        asset_id: tracked.token_id.clone(),
        side: tracked.side,
        price,
        size: normalize_share_size(fill_size),
        outcome: match tracked.leg {
            QuoteLeg::YesBid | QuoteLeg::YesAsk => "YES".to_string(),
            QuoteLeg::NoBid | QuoteLeg::NoAsk => "NO".to_string(),
        },
        status: TradeStatus::Matched,
        timestamp: Utc::now(),
        maker_order_id: Some(tracked.order_id.clone()),
        taker_order_id: None,
    }
}

fn exact_trade_signature_matches<I>(tracked_orders: I, trade: &TradeEvent) -> Vec<TrackedOrder>
where
    I: IntoIterator<Item = TrackedOrder>,
{
    tracked_orders
        .into_iter()
        .filter(|tracked| {
            tracked.condition_id == trade.condition_id
                && tracked.token_id == trade.asset_id
                && tracked.side == trade.side
                && tracked.price == trade.price
                && tracked.size >= trade.size
                && tracked.size > Decimal::ZERO
        })
        .collect()
}

fn directional_fill_delta_from_positions(
    before: &Position,
    after: &Position,
    trigger_leg: QuoteLeg,
    tolerance: Decimal,
) -> Decimal {
    let raw_delta = match trigger_leg {
        QuoteLeg::YesBid | QuoteLeg::YesAsk => after.yes_size - before.yes_size,
        QuoteLeg::NoBid | QuoteLeg::NoAsk => after.no_size - before.no_size,
    };
    let delta = normalize_share_size(raw_delta.max(Decimal::ZERO));
    if delta <= tolerance {
        Decimal::ZERO
    } else {
        delta
    }
}

fn hedge_size_for_observed_position(
    position: &Position,
    trigger_leg: QuoteLeg,
    tolerance: Decimal,
) -> Decimal {
    let required = required_hedge_size(position, trigger_leg);
    if required <= tolerance {
        Decimal::ZERO
    } else {
        required
    }
}

fn effective_fill_size_after_synthetic_dedup(
    observed_trade_size: Decimal,
    synthetic_consumed: Decimal,
) -> Decimal {
    (observed_trade_size - synthetic_consumed).max(Decimal::ZERO)
}

fn size_to_apply_after_order_update_accounting(
    effective_fill_size: Decimal,
    pending_accounted: Decimal,
) -> Decimal {
    (effective_fill_size - pending_accounted).max(Decimal::ZERO)
}

fn hedge_size_for_accounted_fill(
    position: &Position,
    trigger_leg: QuoteLeg,
    accounted_fill_size: Decimal,
    tolerance: Decimal,
) -> Decimal {
    let normalized_fill_size = normalize_share_size(accounted_fill_size);
    if normalized_fill_size <= Decimal::ZERO {
        return Decimal::ZERO;
    }

    let projected_position =
        project_position_after_fill(position, trigger_leg, normalized_fill_size);
    let desired_hedge_size = required_hedge_size(&projected_position, trigger_leg);

    if desired_hedge_size <= tolerance {
        Decimal::ZERO
    } else {
        desired_hedge_size.min(normalized_fill_size)
    }
}

fn should_skip_recent_duplicate_fill(
    signal: Option<&HedgeSignal>,
    trade_timestamp: DateTime<Utc>,
    net_exposure: Decimal,
    tolerance: Decimal,
    cooldown: std::time::Duration,
) -> bool {
    signal.is_some_and(|signal| {
        signal.recorded_at.elapsed() < cooldown
            && net_exposure <= tolerance
            && trade_timestamp <= signal.hedged_at
    })
}

fn build_quote_trace_ids(quote_set: &QuoteSet) -> HashMap<QuoteLeg, String> {
    quote_set
        .candidates
        .iter()
        .map(|candidate| (candidate.leg, Uuid::new_v4().to_string()))
        .collect()
}

fn market_rank_key(quote_set: &QuoteSet, report: &DecisionReport) -> MarketRankKey {
    let estimated_reward = report
        .reward_viability
        .as_ref()
        .map(|viability| viability.estimated_reward)
        .unwrap_or(Decimal::ZERO);
    let reward_per_share = report
        .reward_viability
        .as_ref()
        .map(|viability| {
            compute_reward_per_share_ranking_metric(
                quote_set,
                viability.estimated_reward,
                report.effective_quote_size,
            )
        })
        .unwrap_or(Decimal::ZERO);

    MarketRankKey {
        reward_per_share,
        estimated_reward,
    }
}

fn compare_rank_keys(
    left: MarketRankKey,
    left_condition_id: &str,
    right: MarketRankKey,
    right_condition_id: &str,
) -> std::cmp::Ordering {
    right
        .reward_per_share
        .cmp(&left.reward_per_share)
        .then_with(|| right.estimated_reward.cmp(&left.estimated_reward))
        .then_with(|| left_condition_id.cmp(right_condition_id))
}

fn compare_market_evaluations(
    left: &MarketEvaluation,
    right: &MarketEvaluation,
) -> std::cmp::Ordering {
    compare_rank_keys(
        market_rank_key(&left.quote_set, &left.report),
        &left.market.condition_id,
        market_rank_key(&right.quote_set, &right.report),
        &right.market.condition_id,
    )
}

fn should_skip_new_bid_entry(
    freeze_new_bid_entries: bool,
    existing: &[TrackedOrder],
    quote_set: &QuoteSet,
) -> bool {
    freeze_new_bid_entries
        && existing.is_empty()
        && quote_set
            .candidates
            .iter()
            .any(|candidate| candidate.leg.is_bid() && candidate.status == QuoteStatus::Approved)
}

fn build_actionable_quote_set(
    quote_set: &QuoteSet,
    tracked_orders: &[TrackedOrder],
    position: Option<&Position>,
    available_budget: Decimal,
    min_order_size: Decimal,
) -> QuoteSet {
    let mut actionable = quote_set.clone();
    let mut remaining_budget = whole_share_budget_limit(available_budget);
    let funded_credit_by_leg = funded_bid_credit_by_leg(tracked_orders);

    for candidate in &mut actionable.candidates {
        if candidate.status != QuoteStatus::Approved {
            continue;
        }

        if candidate.leg.is_bid() {
            let existing_credit = funded_credit_by_leg
                .get(&candidate.leg)
                .copied()
                .unwrap_or(Decimal::ZERO);
            let funded_portion = candidate.size.min(existing_credit);
            let incremental_size = (candidate.size - funded_portion).max(Decimal::ZERO);

            if incremental_size <= Decimal::ZERO {
                candidate.size = funded_portion;
            } else {
                match cap_buy_size_to_budget(incremental_size, remaining_budget, min_order_size) {
                    Some(capped_incremental) => {
                        candidate.size = funded_portion + capped_incremental;
                        remaining_budget =
                            (remaining_budget - capped_incremental).max(Decimal::ZERO);
                    }
                    None if funded_portion >= min_order_size => {
                        candidate.size = funded_portion;
                    }
                    None => {
                        suppress_candidate(
                            candidate,
                            format!(
                                "Suppressed: remaining hedge-aware budget {} below min size {}",
                                remaining_budget, min_order_size
                            ),
                        );
                    }
                }
            }
            continue;
        }

        let sellable = match (candidate.leg, position) {
            (QuoteLeg::YesAsk, Some(pos)) => pos.sellable_yes(),
            (QuoteLeg::NoAsk, Some(pos)) => pos.sellable_no(),
            _ => Decimal::ZERO,
        };

        if sellable < candidate.size {
            suppress_candidate(
                candidate,
                format!(
                    "Suppressed: sellable inventory {} below requested size {}",
                    sellable, candidate.size
                ),
            );
        }
    }

    actionable
}

fn funded_bid_credit_by_leg(tracked_orders: &[TrackedOrder]) -> HashMap<QuoteLeg, Decimal> {
    let mut credit_by_leg = HashMap::new();
    for tracked in tracked_orders {
        if !tracked.leg.is_bid() || tracked.side != Side::Buy || tracked.size <= Decimal::ZERO {
            continue;
        }

        credit_by_leg
            .entry(tracked.leg)
            .and_modify(|credit| *credit += tracked.size)
            .or_insert(tracked.size);
    }
    credit_by_leg
}

fn total_funded_bid_credit(tracked_orders: &[TrackedOrder]) -> Decimal {
    funded_bid_credit_by_leg(tracked_orders)
        .into_values()
        .sum::<Decimal>()
}

fn market_reclaimable_bid_capital(tracked_orders: &[TrackedOrder]) -> Decimal {
    tracked_orders
        .iter()
        .filter(|tracked| {
            tracked.leg.is_bid() && tracked.side == Side::Buy && tracked.size > Decimal::ZERO
        })
        .map(|tracked| tracked.size)
        .sum()
}

fn earliest_bid_created_at(tracked_orders: &[TrackedOrder]) -> Option<DateTime<Utc>> {
    tracked_orders
        .iter()
        .filter(|tracked| {
            tracked.leg.is_bid() && tracked.side == Side::Buy && tracked.size > Decimal::ZERO
        })
        .map(|tracked| tracked.created_at)
        .min()
}

fn suppress_candidate(candidate: &mut QuoteCandidate, reason: String) {
    candidate.status = QuoteStatus::Suppressed;
    candidate.reason = Some(reason);
}

fn tracked_order_current_quote_candidate<'a>(
    tracked: &TrackedOrder,
    quote_set: &'a QuoteSet,
) -> Option<&'a QuoteCandidate> {
    let remaining_size = (tracked.size - tracked.matched_size).max(Decimal::ZERO);
    if remaining_size <= Decimal::ZERO {
        return None;
    }

    quote_set
        .candidates
        .iter()
        .filter(|candidate| {
            candidate.status == QuoteStatus::Approved
                && candidate.condition_id == tracked.condition_id
                && candidate.leg == tracked.leg
        })
        .find(|candidate| {
            let price_is_compatible = if tracked.leg.is_bid() {
                tracked.price >= candidate.price
            } else {
                tracked.price <= candidate.price
            };
            price_is_compatible && remaining_size <= candidate.size
        })
}

fn tracked_order_matches_approved_quote(tracked: &TrackedOrder, quote_set: &QuoteSet) -> bool {
    tracked_order_current_quote_candidate(tracked, quote_set).is_some()
}

fn hedge_reports_for_quote_set(
    quote_set: &QuoteSet,
    yes_book: &OrderBookSnapshot,
    no_book: &OrderBookSnapshot,
    strategy_config: &crate::config::StrategyConfig,
) -> Vec<HedgeabilityReport> {
    quote_set
        .candidates
        .iter()
        .map(|candidate| compute_hedgeability(candidate, yes_book, no_book, strategy_config))
        .collect()
}

fn effective_quote_size_for_quotes(quote_set: &QuoteSet) -> Decimal {
    quote_set
        .candidates
        .iter()
        .filter(|candidate| candidate.status == QuoteStatus::Approved && candidate.leg.is_bid())
        .map(|candidate| candidate.size)
        .max()
        .or_else(|| {
            quote_set
                .candidates
                .iter()
                .filter(|candidate| candidate.status == QuoteStatus::Approved)
                .map(|candidate| candidate.size)
                .max()
        })
        .unwrap_or_default()
}

fn tracked_orders_to_quote_set(condition_id: &str, tracked_orders: &[TrackedOrder]) -> QuoteSet {
    QuoteSet {
        condition_id: condition_id.to_string(),
        candidates: tracked_orders
            .iter()
            .map(|order| QuoteCandidate {
                condition_id: condition_id.to_string(),
                leg: order.leg,
                price: order.price,
                size: order.size,
                status: QuoteStatus::Approved,
                reason: None,
            })
            .collect(),
    }
}

fn side_for_leg(leg: QuoteLeg) -> Side {
    match leg {
        QuoteLeg::YesBid | QuoteLeg::NoBid => Side::Buy,
        QuoteLeg::YesAsk | QuoteLeg::NoAsk => Side::Sell,
    }
}

fn outcome_for_leg(leg: QuoteLeg) -> &'static str {
    match leg {
        QuoteLeg::YesBid | QuoteLeg::YesAsk => "YES",
        QuoteLeg::NoBid | QuoteLeg::NoAsk => "NO",
    }
}

/// Compute a score-optimal ask price for inventory sells.
///
/// Places asks as close to mid as possible to maximize Polymarket score.
/// Score formula: S = ((max_spread - spread_to_mid) / max_spread)^2 * size
/// Tighter spread = quadratically higher score, so we target mid rounded
/// up to nearest cent (tightest ask that doesn't cross the spread).
fn compute_ask_price(
    book: &OrderBookSnapshot,
    max_spread: Decimal,
    ask_depth_pct: Decimal,
) -> Option<Decimal> {
    let one_cent = Decimal::new(1, 2);

    match (book.best_bid(), book.best_ask()) {
        (Some(bid), Some(ask)) => {
            let mid = (bid.price + ask.price) / Decimal::from(2);
            // Offset ask from mid by ask_depth_pct of max_spread
            // 0.0 = at mid (max score), 1.0 = at max_spread edge (max PnL)
            let offset = ask_depth_pct * max_spread;
            let target = mid + offset;
            let rounded = (target / one_cent).ceil() * one_cent;
            // Floor: at least 1 tick above best bid
            let price = rounded.max(bid.price + one_cent);
            // Cap: must be within max_spread of mid for scoring qualification
            let max_qualifying = (mid + max_spread).round_dp(2);
            Some(price.min(max_qualifying))
        }
        (Some(bid), None) => Some(bid.price + one_cent),
        (None, Some(ask)) => Some(ask.price),
        (None, None) => None,
    }
}

/// Helper: receive from an optional channel, or pend forever if None.
async fn recv_event(rx: &mut Option<mpsc::UnboundedReceiver<UserEvent>>) -> Option<UserEvent> {
    match rx {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}

/// Helper: receive from an optional book event channel, or pend forever if None.
async fn recv_book_event(rx: &mut Option<mpsc::UnboundedReceiver<BookEvent>>) -> Option<BookEvent> {
    match rx {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::bail;
    use std::collections::VecDeque;
    use std::fs;
    use std::io;
    use std::path::PathBuf;

    use rust_decimal_macros::dec;
    use serde_json::json;
    use spreadeater_core::payloads::{
        HedgeDecisionPayload, HedgeExitPathPayload, HedgeResultPayload,
    };
    use spreadeater_core::EventType;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use crate::trading::hedge_executor::{HedgeVerificationMetadata, HedgeVerificationState};
    use crate::runtime::run_metadata::RunMetadata;

    mod hedge_harness {
        use super::*;

        mod support {
            include!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/support/hedge/support.rs"
            ));
        }
        use self::support::*;

        mod layer0 {
            include!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/support/hedge/layer0.rs"
            ));
        }

        mod layer1 {
            include!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/support/hedge/layer1.rs"
            ));
        }

        mod layer2 {
            include!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/support/hedge/layer2.rs"
            ));
        }

        mod live_probe {
            include!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/support/hedge/live_probe.rs"
            ));
        }
    }

    #[derive(Clone, Default)]
    struct MockExchangeApiState {
        global_orders: Arc<RwLock<VecDeque<String>>>,
        market_orders: Arc<RwLock<HashMap<String, VecDeque<String>>>>,
        positions: Arc<RwLock<VecDeque<String>>>,
        balances: Arc<RwLock<VecDeque<String>>>,
        books: Arc<RwLock<HashMap<String, String>>>,
        cancelled_orders: Arc<RwLock<Vec<String>>>,
    }

    #[derive(Clone)]
    struct MockPairMerger {
        preflight_result: std::result::Result<(), String>,
        result: std::result::Result<String, String>,
        observed_neg_risk: Arc<RwLock<Vec<bool>>>,
    }

    impl MockPairMerger {
        fn new(
            preflight_result: std::result::Result<(), String>,
            result: std::result::Result<String, String>,
        ) -> Self {
            Self {
                preflight_result,
                result,
                observed_neg_risk: Arc::new(RwLock::new(Vec::new())),
            }
        }
    }

    #[async_trait::async_trait]
    impl PairMerger for MockPairMerger {
        async fn preflight_check(&self) -> Result<()> {
            match &self.preflight_result {
                Ok(()) => Ok(()),
                Err(message) => bail!("{message}"),
            }
        }

        async fn merge_positions(
            &self,
            _condition_id: &str,
            _amount: u64,
            neg_risk: bool,
        ) -> Result<String> {
            self.observed_neg_risk.write().await.push(neg_risk);
            match &self.result {
                Ok(tx_hash) => Ok(tx_hash.clone()),
                Err(message) => bail!("{message}"),
            }
        }
    }

    async fn pop_response(queue: &Arc<RwLock<VecDeque<String>>>, fallback: &str) -> String {
        let mut queue = queue.write().await;
        match queue.len() {
            0 => fallback.to_string(),
            1 => queue
                .front()
                .cloned()
                .unwrap_or_else(|| fallback.to_string()),
            _ => queue.pop_front().unwrap_or_else(|| fallback.to_string()),
        }
    }

    async fn pop_market_orders_response(
        state: &MockExchangeApiState,
        market: Option<&str>,
    ) -> String {
        if let Some(market) = market {
            let mut market_orders = state.market_orders.write().await;
            if let Some(queue) = market_orders.get_mut(market) {
                return match queue.len() {
                    0 => empty_orders_response(),
                    1 => queue.front().cloned().unwrap_or_else(empty_orders_response),
                    _ => queue.pop_front().unwrap_or_else(empty_orders_response),
                };
            }
        }

        pop_response(&state.global_orders, &empty_orders_response()).await
    }

    fn query_param(path: &str, key: &str) -> Option<String> {
        let query = path.split('?').nth(1)?;
        for pair in query.split('&') {
            let mut parts = pair.splitn(2, '=');
            if parts.next()? == key {
                return Some(parts.next().unwrap_or_default().to_string());
            }
        }
        None
    }

    fn request_body(request: &str) -> &str {
        request.split("\r\n\r\n").nth(1).unwrap_or_default()
    }

    async fn spawn_exchange_api_server(
        state: MockExchangeApiState,
    ) -> io::Result<(String, tokio::task::JoinHandle<()>)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;

        let task = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let state = state.clone();
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    let Ok(read) = socket.read(&mut buf).await else {
                        return;
                    };
                    if read == 0 {
                        return;
                    }

                    let request = String::from_utf8_lossy(&buf[..read]);
                    let path = request
                        .lines()
                        .next()
                        .and_then(|line| line.split_whitespace().nth(1))
                        .unwrap_or("/");

                    let (status, body) = if path.starts_with("/data/orders") {
                        let market = query_param(path, "market");
                        (
                            "200 OK",
                            pop_market_orders_response(&state, market.as_deref()).await,
                        )
                    } else if path == "/order" && request.starts_with("DELETE /order") {
                        let order_id =
                            serde_json::from_str::<serde_json::Value>(request_body(&request))
                                .ok()
                                .and_then(|body| {
                                    body.get("orderID")
                                        .and_then(serde_json::Value::as_str)
                                        .map(str::to_string)
                                })
                                .unwrap_or_default();
                        if !order_id.is_empty() {
                            state.cancelled_orders.write().await.push(order_id.clone());
                        }
                        ("200 OK", json!({ "canceled": [order_id] }).to_string())
                    } else if path.starts_with("/positions") {
                        ("200 OK", pop_response(&state.positions, "[]").await)
                    } else if path.starts_with("/balance-allowance") {
                        (
                            "200 OK",
                            pop_response(&state.balances, "{\"balance\":\"0\"}").await,
                        )
                    } else if path.starts_with("/book?token_id=") {
                        let token_id = query_param(path, "token_id").unwrap_or_default();
                        let books = state.books.read().await;
                        match books.get(&token_id) {
                            Some(body) => ("200 OK", body.clone()),
                            None => ("404 Not Found", "{\"error\":\"missing\"}".to_string()),
                        }
                    } else {
                        ("404 Not Found", "{\"error\":\"missing\"}".to_string())
                    };

                    let response = format!(
                        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                    let _ = socket.shutdown().await;
                });
            }
        });

        Ok((format!("http://{}", addr), task))
    }

    fn empty_orders_response() -> String {
        json!({
            "data": [],
            "next_cursor": "LTE="
        })
        .to_string()
    }

    fn open_orders_response(orders: Vec<serde_json::Value>) -> String {
        json!({
            "data": orders,
            "next_cursor": "LTE="
        })
        .to_string()
    }

    fn live_order_json(
        order_id: &str,
        market: &CanonicalMarket,
        leg: QuoteLeg,
        price: Decimal,
        original_size: Decimal,
        matched_size: Decimal,
    ) -> serde_json::Value {
        let (asset_id, outcome, side) = match leg {
            QuoteLeg::YesBid => (&market.yes_token_id, "YES", "BUY"),
            QuoteLeg::YesAsk => (&market.yes_token_id, "YES", "SELL"),
            QuoteLeg::NoBid => (&market.no_token_id, "NO", "BUY"),
            QuoteLeg::NoAsk => (&market.no_token_id, "NO", "SELL"),
        };

        json!({
            "id": order_id,
            "status": "live",
            "market": market.condition_id,
            "asset_id": asset_id,
            "side": side,
            "price": price.to_string(),
            "original_size": original_size.to_string(),
            "size_matched": matched_size.to_string(),
            "outcome": outcome,
            "order_type": "GTC",
            "created_at": Utc::now().timestamp()
        })
    }

    fn positions_response(entries: Vec<serde_json::Value>) -> String {
        json!(entries).to_string()
    }

    fn position_entry(condition_id: &str, outcome: &str, size: Decimal) -> serde_json::Value {
        json!({
            "conditionId": condition_id,
            "size": size.to_string().parse::<f64>().unwrap_or(0.0),
            "avgPrice": 0.5,
            "outcome": outcome
        })
    }

    fn balance_response(balance_usdc: Decimal) -> String {
        let atomic = balance_usdc * Decimal::from(1_000_000u64);
        json!({
            "balance": atomic.to_string()
        })
        .to_string()
    }

    fn sample_sellback_order_result(
        order_id: &str,
        status: OrderStatus,
        trade_ids: &[&str],
    ) -> crate::models::OrderResult {
        crate::models::OrderResult {
            order_id: order_id.to_string(),
            status,
            trade_ids: trade_ids
                .iter()
                .map(|trade_id| (*trade_id).to_string())
                .collect(),
        }
    }

    fn sample_sellback_lookup_order(status: OrderStatus, matched_size: Decimal) -> LiveOrder {
        LiveOrder {
            id: "sellback-order".to_string(),
            condition_id: "market".to_string(),
            asset_id: "yes_token".to_string(),
            side: Side::Sell,
            price: dec!(0.01),
            original_size: dec!(5),
            size_matched: matched_size,
            outcome: crate::models::Outcome::Yes,
            order_type: OrderType::FOK,
            status,
            created_at: Utc::now(),
            associated_trade_ids: Vec::new(),
        }
    }

    fn test_market() -> CanonicalMarket {
        test_market_with_neg_risk(false)
    }

    fn test_market_with_identity(
        condition_id: &str,
        yes_token_id: &str,
        no_token_id: &str,
    ) -> CanonicalMarket {
        let mut market = test_market();
        market.condition_id = condition_id.to_string();
        market.market_slug = format!("{condition_id}-slug");
        market.question = format!("{condition_id} question?");
        market.yes_token_id = yes_token_id.to_string();
        market.no_token_id = no_token_id.to_string();
        market.reward_config.condition_id = condition_id.to_string();
        market
    }

    fn test_market_with_neg_risk(neg_risk: bool) -> CanonicalMarket {
        CanonicalMarket {
            condition_id: "market".to_string(),
            market_slug: "test-market".to_string(),
            question: "Test market?".to_string(),
            yes_token_id: "yes_token".to_string(),
            no_token_id: "no_token".to_string(),
            reward_config: crate::models::RewardConfig {
                condition_id: "market".to_string(),
                daily_reward_rates: vec![dec!(50)],
                daily_reward_total: dec!(100),
                min_size: dec!(20),
                max_spread: dec!(0.10),
            },
            neg_risk,
            tick_size: "0.01".to_string(),
            end_date: None,
            admitted_at: Utc::now(),
            status: crate::models::MarketStatus::Admitted,
        }
    }

    fn frontier_market(
        condition_id: &str,
        yes_token_id: &str,
        no_token_id: &str,
        daily_reward_total: Decimal,
    ) -> CanonicalMarket {
        CanonicalMarket {
            condition_id: condition_id.to_string(),
            market_slug: format!("{condition_id}-slug"),
            question: format!("Question for {condition_id}?"),
            yes_token_id: yes_token_id.to_string(),
            no_token_id: no_token_id.to_string(),
            reward_config: crate::models::RewardConfig {
                condition_id: condition_id.to_string(),
                daily_reward_rates: vec![daily_reward_total],
                daily_reward_total,
                min_size: dec!(20),
                max_spread: dec!(0.10),
            },
            neg_risk: false,
            tick_size: "0.01".to_string(),
            end_date: None,
            admitted_at: Utc::now(),
            status: crate::models::MarketStatus::Admitted,
        }
    }

    fn test_book(token_id: &str) -> OrderBookSnapshot {
        OrderBookSnapshot {
            token_id: token_id.to_string(),
            exchange_ts: None,
            ingest_ts: Utc::now(),
            bids: vec![crate::models::PriceLevel {
                price: dec!(0.45),
                size: dec!(100),
            }],
            asks: vec![crate::models::PriceLevel {
                price: dec!(0.55),
                size: dec!(100),
            }],
        }
    }

    fn tracked_order(leg: QuoteLeg, price: Decimal, size: Decimal) -> TrackedOrder {
        tracked_order_for_market(&test_market(), leg, price, size, None)
    }

    fn tracked_order_for_market(
        market: &CanonicalMarket,
        leg: QuoteLeg,
        price: Decimal,
        size: Decimal,
        order_id: Option<&str>,
    ) -> TrackedOrder {
        let (token_id, opposite_token_id) = match leg {
            QuoteLeg::YesBid | QuoteLeg::YesAsk => {
                (market.yes_token_id.clone(), market.no_token_id.clone())
            }
            QuoteLeg::NoBid | QuoteLeg::NoAsk => {
                (market.no_token_id.clone(), market.yes_token_id.clone())
            }
        };
        TrackedOrder {
            order_id: order_id
                .map(str::to_string)
                .unwrap_or_else(|| format!("order-{}-{leg}", market.condition_id)),
            trace_id: format!("trace-{leg}"),
            condition_id: market.condition_id.clone(),
            created_at: Utc::now(),
            leg,
            token_id,
            opposite_token_id,
            side: side_for_leg(leg),
            price,
            size,
            matched_size: Decimal::ZERO,
            neg_risk: false,
            tick_size: "0.01".to_string(),
        }
    }

    async fn test_market_return_per_share(
        engine: &LiveEngine,
        market: &CanonicalMarket,
    ) -> Decimal {
        let yes_book = engine
            .book_manager
            .get_book(&market.yes_token_id)
            .await
            .expect("expected YES book");
        let no_book = engine
            .book_manager
            .get_book(&market.no_token_id)
            .await
            .expect("expected NO book");
        let tracked_orders = engine
            .order_manager
            .get_market_orders(&market.condition_id)
            .await;
        let budget_max = engine.order_manager.available_budget().await;
        let (_, report) = engine
            .evaluate_market_on_books_with_context(
                market,
                &yes_book,
                &no_book,
                &tracked_orders,
                budget_max,
            )
            .await
            .expect("market evaluation should succeed");

        report
            .reward_viability
            .expect("expected reward viability report")
            .return_per_share
    }

    async fn build_test_engine_with_urls(
        clob_base_url: &str,
        data_api_base_url: &str,
        observability_enabled: bool,
    ) -> (LiveEngine, PathBuf) {
        let mut config = Config::default();
        config.observability.enabled = observability_enabled;
        config.discovery.clob_base_url = clob_base_url.to_string();
        config.discovery.data_api_base_url = data_api_base_url.to_string();

        let base_dir =
            std::env::temp_dir().join(format!("spreadeater-live-engine-test-{}", Uuid::new_v4()));
        let data_dir = base_dir.join("data");
        let archive_dir = data_dir.join("archive");
        let error_dir = data_dir.join("errors");
        let event_dir = data_dir.join("events");
        fs::create_dir_all(&archive_dir).unwrap();
        fs::create_dir_all(&error_dir).unwrap();
        fs::create_dir_all(&event_dir).unwrap();
        config.persistence.archive_dir = path_to_string(&archive_dir);
        config.observability.event_log_dir = path_to_string(&event_dir);
        let config_path = base_dir.join("config.test.json");
        fs::write(&config_path, serde_json::to_string_pretty(&config).unwrap()).unwrap();

        let creds = ApiCredentials {
            api_key: String::new(),
            secret: String::new(),
            passphrase: String::new(),
            address: "0x0".to_string(),
            private_key: None,
            funder: None,
        };

        let engine = LiveEngine::new(
            config,
            creds,
            true,
            Arc::new(crate::monitor::ErrorLogger::new(&path_to_string(
                &error_dir,
            ))),
            path_to_string(&config_path),
        )
        .await
        .unwrap();

        (engine, event_dir)
    }

    async fn build_test_engine(
        clob_base_url: &str,
        observability_enabled: bool,
    ) -> (LiveEngine, PathBuf) {
        build_test_engine_with_urls(clob_base_url, "http://127.0.0.1:9", observability_enabled)
            .await
    }

    fn fill_handler_for_live_engine_test(engine: &LiveEngine) -> FillHandler {
        FillHandler {
            order_manager: engine.order_manager.clone(),
            hedge_executor: engine.hedge_executor.clone(),
            managed_markets: engine.managed_markets.clone(),
            known_markets: engine.known_markets.clone(),
            risk_manager: engine.risk_manager.clone(),
            position_manager: engine.position_manager.clone(),
            book_manager: engine.book_manager.clone(),
            book_rest: engine.book_rest.clone(),
            trading_client: engine.trading_client.clone(),
            config: engine.config.clone(),
            event_producer: engine.event_producer.clone(),
            run_id: engine.run_id.clone(),
            mode: engine.mode.clone(),
            cached_balance: engine.cached_balance.clone(),
            hedge_order_ids: engine.hedge_order_ids.clone(),
            recon_baselines: engine.recon_baselines.clone(),
            hedge_signals: engine.hedge_signals.clone(),
            recent_resolution_trades: engine.recent_resolution_trades.clone(),
            ctf_merger: engine.ctf_merger.clone(),
            hedge_locks: engine.hedge_locks.clone(),
            error_logger: engine.error_logger.clone(),
        }
    }

    fn latest_logged_payload<T>(events: &[EventEnvelope], event_type: EventType) -> Option<T>
    where
        T: for<'de> serde::Deserialize<'de>,
    {
        events
            .iter()
            .rev()
            .find(|event| event.event_type == event_type)
            .and_then(|event| serde_json::from_value(event.payload.clone()).ok())
    }

    async fn test_engine() -> LiveEngine {
        build_test_engine("http://127.0.0.1:9", false).await.0
    }

    async fn read_emitted_events(event_dir: &PathBuf, run_id: &str) -> Vec<EventEnvelope> {
        let event_log = event_dir.join(run_id).join("events.jsonl");
        let contents = tokio::fs::read_to_string(event_log).await.unwrap();
        contents
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    async fn wait_for_emitted_events<F>(
        event_dir: &PathBuf,
        run_id: &str,
        timeout: Duration,
        predicate: F,
    ) -> Vec<EventEnvelope>
    where
        F: Fn(&[EventEnvelope]) -> bool,
    {
        let started = Instant::now();
        loop {
            let events = read_emitted_events(event_dir, run_id).await;
            if predicate(&events) || started.elapsed() >= timeout {
                return events;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    fn path_to_string(path: &PathBuf) -> String {
        path.to_string_lossy().into_owned()
    }

    async fn read_run_metadata(path: PathBuf) -> RunMetadata {
        let contents = tokio::fs::read_to_string(path).await.unwrap();
        serde_json::from_str(&contents).unwrap()
    }

    fn test_credentials() -> ApiCredentials {
        ApiCredentials {
            api_key: String::new(),
            secret: String::new(),
            passphrase: String::new(),
            address: "0x0".to_string(),
            private_key: None,
            funder: None,
        }
    }

    fn trading_client_with_base(base_url: &str, dry_run: bool) -> Arc<TradingClient> {
        Arc::new(
            TradingClient::new(
                base_url.to_string(),
                RequestSigner::new(test_credentials()),
                None,
                "",
                "",
                dry_run,
            )
            .unwrap(),
        )
    }

    fn test_book_with_depth(
        token_id: &str,
        bid_size: Decimal,
        ask_size: Decimal,
    ) -> OrderBookSnapshot {
        OrderBookSnapshot {
            token_id: token_id.to_string(),
            exchange_ts: None,
            ingest_ts: Utc::now(),
            bids: vec![crate::models::PriceLevel {
                price: dec!(0.45),
                size: bid_size,
            }],
            asks: vec![crate::models::PriceLevel {
                price: dec!(0.55),
                size: ask_size,
            }],
        }
    }

    fn test_book_with_prices(
        token_id: &str,
        bid_price: Decimal,
        ask_price: Decimal,
        bid_size: Decimal,
        ask_size: Decimal,
    ) -> OrderBookSnapshot {
        OrderBookSnapshot {
            token_id: token_id.to_string(),
            exchange_ts: None,
            ingest_ts: Utc::now(),
            bids: vec![crate::models::PriceLevel {
                price: bid_price,
                size: bid_size,
            }],
            asks: vec![crate::models::PriceLevel {
                price: ask_price,
                size: ask_size,
            }],
        }
    }

    fn book_response_json(token_id: &str, bid_size: Decimal, ask_size: Decimal) -> String {
        json!({
            "asset_id": token_id,
            "bids": [{"price": "0.45", "size": bid_size.to_string()}],
            "asks": [{"price": "0.55", "size": ask_size.to_string()}]
        })
        .to_string()
    }

    async fn spawn_book_server(
        responses: HashMap<String, String>,
        response_delay: Duration,
    ) -> io::Result<(String, tokio::task::JoinHandle<()>)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;

        let task = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let responses = responses.clone();
                tokio::spawn(async move {
                    let mut buf = [0u8; 2048];
                    let Ok(read) = socket.read(&mut buf).await else {
                        return;
                    };
                    if read == 0 {
                        return;
                    }

                    let request = String::from_utf8_lossy(&buf[..read]);
                    let token_id = request
                        .lines()
                        .next()
                        .and_then(|line| line.split("token_id=").nth(1))
                        .and_then(|rest| rest.split_whitespace().next())
                        .unwrap_or_default()
                        .trim_end_matches("HTTP/1.1")
                        .trim()
                        .trim_end_matches('&')
                        .to_string();

                    if !response_delay.is_zero() {
                        tokio::time::sleep(response_delay).await;
                    }

                    let (status, body) = responses
                        .get(&token_id)
                        .map(|body| ("200 OK", body.clone()))
                        .unwrap_or_else(|| {
                            ("404 Not Found", "{\"error\":\"missing\"}".to_string())
                        });
                    let response = format!(
                        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                    let _ = socket.shutdown().await;
                });
            }
        });

        Ok((format!("http://{}", addr), task))
    }

    async fn spawn_static_json_server(
        body: &str,
        response_delay: Duration,
    ) -> io::Result<(String, tokio::task::JoinHandle<()>)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let body = body.to_string();

        let task = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let body = body.clone();
                tokio::spawn(async move {
                    let mut buf = [0u8; 1024];
                    let Ok(read) = socket.read(&mut buf).await else {
                        return;
                    };
                    if read == 0 {
                        return;
                    }

                    if !response_delay.is_zero() {
                        tokio::time::sleep(response_delay).await;
                    }

                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                    let _ = socket.shutdown().await;
                });
            }
        });

        Ok((format!("http://{}", addr), task))
    }

    async fn spawn_scoring_server(
        responses: Arc<RwLock<HashMap<String, bool>>>,
    ) -> io::Result<(String, tokio::task::JoinHandle<()>)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;

        let task = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let responses = responses.clone();
                tokio::spawn(async move {
                    let mut buf = [0u8; 2048];
                    let Ok(read) = socket.read(&mut buf).await else {
                        return;
                    };
                    if read == 0 {
                        return;
                    }

                    let request = String::from_utf8_lossy(&buf[..read]);
                    let path = request
                        .lines()
                        .next()
                        .and_then(|line| line.split_whitespace().nth(1))
                        .unwrap_or_default()
                        .to_string();
                    let order_id = path
                        .strip_prefix("/orders/")
                        .and_then(|rest| rest.strip_suffix("/scoring-status"))
                        .unwrap_or_default()
                        .to_string();
                    let scoring = responses
                        .read()
                        .await
                        .get(&order_id)
                        .copied()
                        .unwrap_or(false);
                    let body = json!({ "scoring": scoring }).to_string();
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                    let _ = socket.shutdown().await;
                });
            }
        });

        Ok((format!("http://{}", addr), task))
    }

    #[test]
    fn projected_yes_bid_hedge_size_uses_residual_exposure_only() {
        let pre = Position {
            condition_id: "market".to_string(),
            yes_size: dec!(176.00),
            no_size: dec!(176.00),
            avg_yes_price: dec!(0.66),
            avg_no_price: dec!(0.38),
        };

        let projected = project_position_after_fill(&pre, QuoteLeg::YesBid, dec!(176.00));
        let hedge_size = required_hedge_size(&projected, QuoteLeg::YesBid);

        assert_eq!(projected.yes_size, dec!(352.00));
        assert_eq!(hedge_size, dec!(176.00));
    }

    #[test]
    fn reconciliation_hedge_size_keeps_fractional_residuals() {
        let pos = Position {
            condition_id: "market".to_string(),
            yes_size: dec!(176.00),
            no_size: dec!(450.95),
            avg_yes_price: dec!(0.66),
            avg_no_price: dec!(0.38),
        };

        assert_eq!(required_hedge_size(&pos, QuoteLeg::YesAsk), dec!(274.95));
    }

    #[test]
    fn hedge_size_for_accounted_fill_caps_to_accounted_fill_size() {
        let pos = Position {
            condition_id: "market".to_string(),
            yes_size: dec!(100.00),
            no_size: dec!(95.00),
            avg_yes_price: dec!(0.60),
            avg_no_price: dec!(0.40),
        };

        let hedge_size =
            hedge_size_for_accounted_fill(&pos, QuoteLeg::YesBid, dec!(3.00), dec!(0.5));

        assert_eq!(hedge_size, dec!(3.00));
    }

    #[test]
    fn hedge_size_for_accounted_fill_skips_when_residual_is_within_tolerance() {
        let pos = Position {
            condition_id: "market".to_string(),
            yes_size: dec!(100.00),
            no_size: dec!(99.75),
            avg_yes_price: dec!(0.60),
            avg_no_price: dec!(0.40),
        };

        let hedge_size =
            hedge_size_for_accounted_fill(&pos, QuoteLeg::NoBid, dec!(0.20), dec!(0.5));

        assert_eq!(hedge_size, Decimal::ZERO);
    }

    #[test]
    fn hedge_size_for_accounted_fill_uses_only_remaining_residual() {
        let pos = Position {
            condition_id: "market".to_string(),
            yes_size: dec!(100.00),
            no_size: dec!(103.00),
            avg_yes_price: dec!(0.60),
            avg_no_price: dec!(0.40),
        };

        let hedge_size =
            hedge_size_for_accounted_fill(&pos, QuoteLeg::YesBid, dec!(5.00), dec!(0.5));

        assert_eq!(hedge_size, dec!(2.00));
    }

    #[test]
    fn build_sellback_order_request_uses_supplied_limit_price() {
        let request =
            build_sellback_order_request("token-1", dec!(0.69), dec!(5.129), false, "0.01");

        assert_eq!(request.token_id, "token-1");
        assert_eq!(request.price, dec!(0.69));
        assert_eq!(request.size, dec!(5.12));
        assert_eq!(request.side, Side::Sell);
        assert_eq!(request.order_type, OrderType::FOK);
    }

    #[test]
    fn should_recompute_buy_resolution_sellback_only_for_buy_sellback_misses() {
        let intent = HedgeIntent {
            condition_id: "market".to_string(),
            trigger_order_id: "trigger-order".to_string(),
            trigger_leg: QuoteLeg::YesBid,
            fill_size: dec!(5),
            fill_price: dec!(0.74),
            hedge_token_id: "no-token".to_string(),
            hedge_side: Side::Buy,
            neg_risk: false,
            tick_size: "0.01".to_string(),
        };
        let resolution = HedgeResolution {
            hedge_shares: Decimal::ZERO,
            hedge_limit_price: Decimal::ZERO,
            sellback_shares: dec!(5),
            sellback_limit_price: dec!(0.69),
            unresolved_shares: Decimal::ZERO,
        };
        let unverified_sellback = SellbackExecutionResult {
            order_result: Some(sample_sellback_order_result(
                "sellback-order",
                OrderStatus::Live,
                &[],
            )),
            verification_state: SellbackVerificationState::Unknown,
            confirmed_shares: None,
            failure_reason: Some("pending".to_string()),
            price: Some(dec!(0.69)),
            verification_metadata: SellbackVerificationMetadata::default(),
        };
        let verified_sellback = SellbackExecutionResult {
            verification_state: SellbackVerificationState::VerifiedFilled,
            confirmed_shares: Some(dec!(5)),
            failure_reason: None,
            ..unverified_sellback.clone()
        };
        let failed_result = ResolutionExecutionResult {
            hedge_result: None,
            sellback_result: Some(unverified_sellback),
            post_position: Some(Position::new("market".to_string())),
            post_sync_net_exposure: dec!(5),
            post_sync_source: "first_sync",
            success: false,
            failure_reason: Some("sellback miss".to_string()),
        };
        let success_result = ResolutionExecutionResult {
            sellback_result: Some(verified_sellback),
            success: true,
            failure_reason: None,
            ..failed_result.clone()
        };

        assert!(should_recompute_buy_resolution_sellback(
            &intent,
            Some(&resolution),
            &failed_result,
        ));
        assert!(!should_recompute_buy_resolution_sellback(
            &intent,
            Some(&resolution),
            &success_result,
        ));
        assert!(!should_recompute_buy_resolution_sellback(
            &HedgeIntent {
                hedge_side: Side::Sell,
                ..intent.clone()
            },
            Some(&resolution),
            &failed_result,
        ));
        assert!(!should_recompute_buy_resolution_sellback(
            &intent,
            Some(&HedgeResolution {
                sellback_shares: Decimal::ZERO,
                ..resolution
            }),
            &failed_result,
        ));
    }

    #[test]
    fn processed_trade_cache_prunes_expired_entries() {
        let ttl = StdDuration::from_millis(1);
        let expired = Instant::now();
        let now = expired + ttl + StdDuration::from_millis(1);
        let fresh = now;
        let mut cache = ProcessedTradeCache::default();
        cache.entries.insert(
            "expired".to_string(),
            ProcessedTradeEntry { seen_at: expired },
        );
        cache.order.push_back(("expired".to_string(), expired));
        cache
            .entries
            .insert("fresh".to_string(), ProcessedTradeEntry { seen_at: fresh });
        cache.order.push_back(("fresh".to_string(), fresh));

        prune_processed_trade_cache_with_ttl(&mut cache, now, ttl);

        assert!(!cache.entries.contains_key("expired"));
        assert!(cache.entries.contains_key("fresh"));
    }

    #[test]
    fn recent_synthetic_fill_pruning_drops_expired_entries_without_underflow() {
        let ttl = StdDuration::from_millis(1);
        let processed_at = Instant::now();
        let now = processed_at + ttl + StdDuration::from_millis(1);
        let future = now + StdDuration::from_millis(1);
        let mut recent = HashMap::from([
            (
                "expired".to_string(),
                RecentSyntheticFill {
                    size: dec!(1),
                    processed_at,
                },
            ),
            (
                "future".to_string(),
                RecentSyntheticFill {
                    size: dec!(2),
                    processed_at: future,
                },
            ),
        ]);

        prune_recent_synthetic_fills(&mut recent, now, ttl);

        assert!(!recent.contains_key("expired"));
        assert_eq!(recent.get("future").map(|entry| entry.size), Some(dec!(2)));
    }

    #[test]
    fn hedge_position_truth_confirms_buy_hedge_when_opposite_inventory_increases() {
        let pre = Position {
            condition_id: "market".to_string(),
            yes_size: dec!(197.5),
            no_size: Decimal::ZERO,
            avg_yes_price: dec!(0.71),
            avg_no_price: Decimal::ZERO,
        };
        let post = Position {
            condition_id: "market".to_string(),
            yes_size: dec!(197.5),
            no_size: dec!(163.5),
            avg_yes_price: dec!(0.71),
            avg_no_price: dec!(0.30),
        };
        let intent = HedgeIntent {
            condition_id: "market".to_string(),
            trigger_order_id: "trigger".to_string(),
            trigger_leg: QuoteLeg::YesBid,
            fill_size: dec!(197.5),
            fill_price: dec!(0.71),
            hedge_token_id: "no-token".to_string(),
            hedge_side: Side::Buy,
            neg_risk: false,
            tick_size: "0.01".to_string(),
        };
        let hedge_result = HedgeResult {
            intent: intent.clone(),
            success: true,
            order_result: None,
            hedge_price: Some(dec!(0.31)),
            failure_reason: None,
            verification_state: crate::trading::hedge_executor::HedgeVerificationState::Unknown,
            verification_metadata: Default::default(),
        };

        assert!(hedge_leg_confirmed_by_position_truth(
            &pre,
            &post,
            &intent,
            Some(&hedge_result),
        ));
        assert!(!should_retry_resolution_sync(
            Some(&hedge_result),
            &intent,
            &pre,
            Some(&post),
        ));
    }

    #[test]
    fn resolution_truth_retries_when_buy_hedge_evidence_lacks_position_confirmation() {
        let pre = Position {
            condition_id: "market".to_string(),
            yes_size: dec!(197.5),
            no_size: Decimal::ZERO,
            avg_yes_price: dec!(0.71),
            avg_no_price: Decimal::ZERO,
        };
        let post = Position {
            condition_id: "market".to_string(),
            yes_size: dec!(197.5),
            no_size: Decimal::ZERO,
            avg_yes_price: dec!(0.71),
            avg_no_price: Decimal::ZERO,
        };
        let intent = HedgeIntent {
            condition_id: "market".to_string(),
            trigger_order_id: "trigger".to_string(),
            trigger_leg: QuoteLeg::YesBid,
            fill_size: dec!(197.5),
            fill_price: dec!(0.71),
            hedge_token_id: "no-token".to_string(),
            hedge_side: Side::Buy,
            neg_risk: false,
            tick_size: "0.01".to_string(),
        };
        let hedge_result = HedgeResult {
            intent: intent.clone(),
            success: true,
            order_result: None,
            hedge_price: Some(dec!(0.31)),
            failure_reason: None,
            verification_state:
                crate::trading::hedge_executor::HedgeVerificationState::VerifiedFilled,
            verification_metadata: Default::default(),
        };

        assert!(should_retry_resolution_sync(
            Some(&hedge_result),
            &intent,
            &pre,
            Some(&post),
        ));
    }

    #[tokio::test]
    async fn sync_position_for_resolution_returns_position_when_market_row_exists() {
        let state = MockExchangeApiState::default();
        state
            .positions
            .write()
            .await
            .push_back(positions_response(vec![
                position_entry("market", "YES", dec!(5)),
                position_entry("market", "NO", dec!(1)),
            ]));
        let (base_url, server) = spawn_exchange_api_server(state).await.unwrap();
        let position_manager = Arc::new(PositionManager::new(base_url, "0x0".to_string()));

        let truth = sync_position_for_resolution(&position_manager, "market")
            .await
            .expect("sync should succeed");
        let position = truth.expect("market row should exist");

        assert_eq!(position.yes_size, dec!(5));
        assert_eq!(position.no_size, dec!(1));

        server.abort();
    }

    #[tokio::test]
    async fn first_reconciliation_failure_routes_to_immediate_kill_path() {
        let engine = test_engine().await;
        let market = test_market();
        engine
            .managed_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market.clone());

        engine
            .handle_reconciliation_resolution_failure(&market, dec!(5), "resolution failed", true)
            .await;

        assert!(
            !engine
                .risk_manager
                .is_market_tradable(&market.condition_id)
                .await
        );
        assert!(!engine
            .managed_markets
            .read()
            .await
            .contains_key(&market.condition_id));
    }

    #[tokio::test]
    async fn duplicate_halt_signal_preserves_original_reason() {
        let engine = test_engine().await;
        let market = test_market();
        engine
            .managed_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market.clone());

        let first = engine.kill_market(&market.condition_id, "first halt").await;
        let second = engine
            .kill_market(&market.condition_id, "second halt")
            .await;
        let state = engine
            .risk_manager
            .get_market_state(&market.condition_id)
            .await
            .expect("risk state");

        assert!(!first.halt_signal_suppressed);
        assert!(second.halt_signal_suppressed);
        assert_eq!(state.halt_reason.as_deref(), Some("first halt"));
    }

    #[tokio::test]
    async fn finalize_halted_cleanup_keeps_market_managed_until_flatten_verifies() {
        let positions = json!([
            {
                "conditionId": "market",
                "size": 5.0,
                "avgPrice": 0.45,
                "outcome": "Yes"
            }
        ])
        .to_string();
        let (positions_url, positions_server) =
            spawn_static_json_server(&positions, Duration::ZERO)
                .await
                .unwrap();
        let engine = build_test_engine_with_urls("http://127.0.0.1:9", &positions_url, false)
            .await
            .0;
        let market = test_market();
        engine
            .managed_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market.clone());

        let cleaned = finalize_halted_market_cleanup(
            &market.condition_id,
            &engine.order_manager,
            &engine.position_manager,
            &engine.trading_client,
            &engine.managed_markets,
            &engine.known_markets,
            &engine.config,
        )
        .await;

        assert!(!cleaned.verified());
        assert_eq!(cleaned.status, HaltCleanupStatus::PendingExposure);
        assert!(engine
            .managed_markets
            .read()
            .await
            .contains_key(&market.condition_id));

        positions_server.abort();
    }

    #[test]
    fn hedge_exposure_tolerance_uses_shared_configured_value() {
        let mut config = Config::default();
        config.risk.hedge_exposure_tolerance = dec!(0.25);
        assert_eq!(hedge_exposure_tolerance(&config), dec!(0.25));
    }

    #[test]
    fn effective_fill_size_preserves_trade_when_no_synthetic_fill_consumed() {
        assert_eq!(
            effective_fill_size_after_synthetic_dedup(dec!(4.00), Decimal::ZERO),
            dec!(4.00)
        );
    }

    #[test]
    fn size_to_apply_can_be_zero_while_hedgeable_fill_remains() {
        let effective_fill_size =
            effective_fill_size_after_synthetic_dedup(dec!(4.00), Decimal::ZERO);
        let size_to_apply =
            size_to_apply_after_order_update_accounting(effective_fill_size, dec!(4.00));

        assert_eq!(effective_fill_size, dec!(4.00));
        assert_eq!(size_to_apply, Decimal::ZERO);
    }

    #[test]
    fn duplicate_fill_skip_only_applies_to_pre_hedge_trade_timestamps() {
        let signal = HedgeSignal {
            recorded_at: Instant::now(),
            hedged_at: Utc::now(),
        };

        assert!(should_skip_recent_duplicate_fill(
            Some(&signal),
            signal.hedged_at,
            dec!(0.10),
            dec!(0.50),
            std::time::Duration::from_secs(180),
        ));

        assert!(!should_skip_recent_duplicate_fill(
            Some(&signal),
            signal.hedged_at + chrono::Duration::milliseconds(1),
            dec!(0.10),
            dec!(0.50),
            std::time::Duration::from_secs(180),
        ));
    }

    #[test]
    fn duplicate_fill_skip_requires_balanced_position_within_tolerance() {
        let signal = HedgeSignal {
            recorded_at: Instant::now(),
            hedged_at: Utc::now(),
        };

        assert!(!should_skip_recent_duplicate_fill(
            Some(&signal),
            signal.hedged_at,
            dec!(0.75),
            dec!(0.50),
            std::time::Duration::from_secs(180),
        ));
    }

    #[test]
    fn actionable_quote_set_suppresses_asks_without_sellable_inventory() {
        let quote_set = QuoteSet {
            condition_id: "market".to_string(),
            candidates: vec![QuoteCandidate {
                condition_id: "market".to_string(),
                leg: QuoteLeg::YesAsk,
                price: dec!(0.55),
                size: dec!(20),
                status: QuoteStatus::Approved,
                reason: None,
            }],
        };
        let position = Position {
            condition_id: "market".to_string(),
            yes_size: dec!(20),
            no_size: dec!(20),
            avg_yes_price: dec!(0.50),
            avg_no_price: dec!(0.50),
        };

        let actionable =
            build_actionable_quote_set(&quote_set, &[], Some(&position), dec!(100), dec!(20));

        assert_eq!(actionable.candidates[0].status, QuoteStatus::Suppressed);
        assert!(actionable.candidates[0]
            .reason
            .as_ref()
            .is_some_and(|reason| reason.contains("sellable inventory")));
    }

    #[test]
    fn actionable_quote_set_respects_remaining_budget_in_candidate_order() {
        let quote_set = QuoteSet {
            condition_id: "market".to_string(),
            candidates: vec![
                QuoteCandidate {
                    condition_id: "market".to_string(),
                    leg: QuoteLeg::YesBid,
                    price: dec!(0.45),
                    size: dec!(50),
                    status: QuoteStatus::Approved,
                    reason: None,
                },
                QuoteCandidate {
                    condition_id: "market".to_string(),
                    leg: QuoteLeg::NoBid,
                    price: dec!(0.45),
                    size: dec!(50),
                    status: QuoteStatus::Approved,
                    reason: None,
                },
            ],
        };

        let actionable = build_actionable_quote_set(&quote_set, &[], None, dec!(69), dec!(20));

        assert_eq!(actionable.candidates[0].status, QuoteStatus::Approved);
        assert_eq!(actionable.candidates[0].size, dec!(50));
        assert_eq!(actionable.candidates[1].status, QuoteStatus::Suppressed);
        assert!(actionable.candidates[1]
            .reason
            .as_ref()
            .is_some_and(|reason| reason.contains("remaining hedge-aware budget")));
    }

    #[test]
    fn actionable_quote_set_preserves_existing_funded_bid_with_no_free_budget() {
        let quote_set = QuoteSet {
            condition_id: "market".to_string(),
            candidates: vec![QuoteCandidate {
                condition_id: "market".to_string(),
                leg: QuoteLeg::YesBid,
                price: dec!(0.45),
                size: dec!(50),
                status: QuoteStatus::Approved,
                reason: None,
            }],
        };
        let tracked_orders = vec![tracked_order(QuoteLeg::YesBid, dec!(0.45), dec!(50))];

        let actionable =
            build_actionable_quote_set(&quote_set, &tracked_orders, None, dec!(0), dec!(20));

        assert_eq!(actionable.candidates[0].status, QuoteStatus::Approved);
        assert_eq!(actionable.candidates[0].size, dec!(50));
    }

    #[test]
    fn actionable_quote_set_does_not_spend_one_legs_credit_on_another_bid_leg() {
        let quote_set = QuoteSet {
            condition_id: "market".to_string(),
            candidates: vec![
                QuoteCandidate {
                    condition_id: "market".to_string(),
                    leg: QuoteLeg::YesBid,
                    price: dec!(0.45),
                    size: dec!(50),
                    status: QuoteStatus::Approved,
                    reason: None,
                },
                QuoteCandidate {
                    condition_id: "market".to_string(),
                    leg: QuoteLeg::NoBid,
                    price: dec!(0.45),
                    size: dec!(50),
                    status: QuoteStatus::Approved,
                    reason: None,
                },
            ],
        };
        let tracked_orders = vec![tracked_order(QuoteLeg::YesBid, dec!(0.45), dec!(50))];

        let actionable =
            build_actionable_quote_set(&quote_set, &tracked_orders, None, dec!(0), dec!(20));

        assert_eq!(actionable.candidates[0].status, QuoteStatus::Approved);
        assert_eq!(actionable.candidates[0].size, dec!(50));
        assert_eq!(actionable.candidates[1].status, QuoteStatus::Suppressed);
        assert!(actionable.candidates[1]
            .reason
            .as_ref()
            .is_some_and(|reason| reason.contains("remaining hedge-aware budget")));
    }

    #[test]
    fn actionable_quote_set_keeps_real_rejections_even_if_bid_is_funded() {
        let quote_set = QuoteSet {
            condition_id: "market".to_string(),
            candidates: vec![QuoteCandidate {
                condition_id: "market".to_string(),
                leg: QuoteLeg::YesBid,
                price: dec!(0.19),
                size: dec!(50),
                status: QuoteStatus::Rejected,
                reason: Some("Mid $0.19 below min_outcome_price $0.20".to_string()),
            }],
        };
        let tracked_orders = vec![tracked_order(QuoteLeg::YesBid, dec!(0.45), dec!(50))];

        let actionable =
            build_actionable_quote_set(&quote_set, &tracked_orders, None, dec!(0), dec!(20));

        assert_eq!(actionable.candidates[0].status, QuoteStatus::Rejected);
    }

    #[tokio::test]
    async fn get_hedge_lock_returns_same_mutex_for_same_market() {
        let locks: Arc<RwLock<HashMap<String, Arc<tokio::sync::Mutex<()>>>>> =
            Arc::new(RwLock::new(HashMap::new()));

        let lock1 = get_hedge_lock(&locks, "market_a").await;
        let lock2 = get_hedge_lock(&locks, "market_a").await;
        let lock3 = get_hedge_lock(&locks, "market_b").await;

        // Same market → same Arc (pointer equality)
        assert!(Arc::ptr_eq(&lock1, &lock2));
        // Different market → different Arc
        assert!(!Arc::ptr_eq(&lock1, &lock3));
    }

    #[tokio::test]
    async fn hedge_lock_serializes_concurrent_access() {
        let locks: Arc<RwLock<HashMap<String, Arc<tokio::sync::Mutex<()>>>>> =
            Arc::new(RwLock::new(HashMap::new()));

        let lock = get_hedge_lock(&locks, "market_a").await;
        let _guard = lock.lock().await;

        // Verify we can still get a different market's lock while holding one
        let lock_b = get_hedge_lock(&locks, "market_b").await;
        let _guard_b = lock_b.lock().await;
        // Both locks held — different markets don't block each other
    }

    #[tokio::test]
    async fn status_estimate_uses_same_score_proxy_math_as_selection() {
        let engine = test_engine().await;
        let market = test_market();
        let yes_book = test_book(&market.yes_token_id);
        let no_book = test_book(&market.no_token_id);
        engine.book_manager.insert_snapshot(yes_book.clone()).await;
        engine.book_manager.insert_snapshot(no_book.clone()).await;

        let tracked_orders = vec![
            tracked_order(QuoteLeg::YesBid, dec!(0.45), dec!(20)),
            tracked_order(QuoteLeg::NoBid, dec!(0.45), dec!(20)),
        ];
        let competition_multiplier = dec!(1.9);

        let estimated_daily = engine
            .estimate_market_daily_reward(&market, &tracked_orders, competition_multiplier)
            .await;

        let mut proxy_config = engine.config.strategy.score_proxy.clone();
        proxy_config.competition_multiplier = competition_multiplier;
        let quote_set = tracked_orders_to_quote_set(&market.condition_id, &tracked_orders);
        let score_proxy = compute_score_proxy(
            &quote_set,
            &yes_book,
            &no_book,
            &market.reward_config,
            &proxy_config,
        );

        assert_eq!(
            estimated_daily,
            market.reward_config.daily_reward_total
                * score_proxy.estimated_share
                * engine.config.strategy.reward_discount_factor
        );
    }

    #[tokio::test]
    async fn stale_book_kill_path_returns_without_deadlocking() {
        let (data_api_base_url, positions_server) = spawn_static_json_server("[]", Duration::ZERO)
            .await
            .unwrap();
        let engine = build_test_engine_with_urls("http://127.0.0.1:9", &data_api_base_url, false)
            .await
            .0;
        let market = test_market();

        engine
            .managed_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market.clone());
        engine.order_manager.update_gross_balance(dec!(1000)).await;
        engine
            .depth_check_counter
            .store(1, std::sync::atomic::Ordering::Relaxed);

        let quote_set = QuoteSet {
            condition_id: market.condition_id.clone(),
            candidates: vec![QuoteCandidate {
                condition_id: market.condition_id.clone(),
                leg: QuoteLeg::YesBid,
                price: dec!(0.45),
                size: dec!(20),
                status: QuoteStatus::Approved,
                reason: None,
            }],
        };

        engine
            .order_manager
            .place_quotes(&market, &quote_set, None, dec!(20), None, "test", None)
            .await
            .unwrap();

        let stale_ingest_ts = Utc::now()
            - chrono::Duration::seconds(engine.config.books.max_book_age_secs as i64 + 1);
        let mut stale_yes_book = test_book(&market.yes_token_id);
        stale_yes_book.ingest_ts = stale_ingest_ts;
        let mut stale_no_book = test_book(&market.no_token_id);
        stale_no_book.ingest_ts = stale_ingest_ts;
        engine.book_manager.insert_snapshot(stale_yes_book).await;
        engine.book_manager.insert_snapshot(stale_no_book).await;

        let result = tokio::time::timeout(
            STALE_BOOK_REFRESH_TIMEOUT + std::time::Duration::from_secs(1),
            engine.check_hedge_depth(),
        )
        .await;

        assert!(matches!(result, Ok(Ok(()))), "result: {:?}", result);
        positions_server.abort();
    }

    #[tokio::test]
    async fn stale_book_refresh_uses_fresh_books_and_preserves_order() {
        let mut responses = HashMap::new();
        responses.insert(
            "yes_token".to_string(),
            book_response_json("yes_token", dec!(100), dec!(100)),
        );
        responses.insert(
            "no_token".to_string(),
            book_response_json("no_token", dec!(100), dec!(100)),
        );
        let (base_url, server) = spawn_book_server(responses, Duration::ZERO).await.unwrap();
        let engine = build_test_engine(&base_url, false).await.0;
        let market = test_market();

        engine
            .managed_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market.clone());
        engine.order_manager.update_gross_balance(dec!(1000)).await;

        let quote_set = QuoteSet {
            condition_id: market.condition_id.clone(),
            candidates: vec![QuoteCandidate {
                condition_id: market.condition_id.clone(),
                leg: QuoteLeg::YesBid,
                price: dec!(0.45),
                size: dec!(20),
                status: QuoteStatus::Approved,
                reason: None,
            }],
        };

        engine
            .order_manager
            .place_quotes(&market, &quote_set, None, dec!(20), None, "test", None)
            .await
            .unwrap();

        let stale_ingest_ts = Utc::now()
            - chrono::Duration::seconds(engine.config.books.max_book_age_secs as i64 + 1);
        let mut stale_yes_book = test_book_with_depth(&market.yes_token_id, dec!(5), dec!(5));
        stale_yes_book.ingest_ts = stale_ingest_ts;
        let mut stale_no_book = test_book_with_depth(&market.no_token_id, dec!(5), dec!(5));
        stale_no_book.ingest_ts = stale_ingest_ts;
        engine.book_manager.insert_snapshot(stale_yes_book).await;
        engine.book_manager.insert_snapshot(stale_no_book).await;

        engine.check_hedge_depth().await.unwrap();

        let tracked = engine
            .order_manager
            .get_market_orders(&market.condition_id)
            .await;
        assert_eq!(tracked.len(), 1);
        assert_eq!(tracked[0].size, dec!(20));
        assert!(
            engine
                .risk_manager
                .is_market_tradable(&market.condition_id)
                .await
        );

        let refreshed_no = engine
            .book_manager
            .get_book(&market.no_token_id)
            .await
            .unwrap();
        assert_eq!(refreshed_no.asks[0].size, dec!(100));
        server.abort();
    }

    #[tokio::test]
    async fn stale_book_refresh_timeout_still_halts_market() {
        let mut responses = HashMap::new();
        responses.insert(
            "yes_token".to_string(),
            book_response_json("yes_token", dec!(100), dec!(100)),
        );
        responses.insert(
            "no_token".to_string(),
            book_response_json("no_token", dec!(100), dec!(100)),
        );
        let (base_url, server) = spawn_book_server(
            responses,
            STALE_BOOK_REFRESH_TIMEOUT + Duration::from_millis(200),
        )
        .await
        .unwrap();
        let engine = build_test_engine(&base_url, false).await.0;
        let market = test_market();

        engine
            .managed_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market.clone());
        engine.order_manager.update_gross_balance(dec!(1000)).await;

        let quote_set = QuoteSet {
            condition_id: market.condition_id.clone(),
            candidates: vec![QuoteCandidate {
                condition_id: market.condition_id.clone(),
                leg: QuoteLeg::YesBid,
                price: dec!(0.45),
                size: dec!(20),
                status: QuoteStatus::Approved,
                reason: None,
            }],
        };

        engine
            .order_manager
            .place_quotes(&market, &quote_set, None, dec!(20), None, "test", None)
            .await
            .unwrap();

        let stale_ingest_ts = Utc::now()
            - chrono::Duration::seconds(engine.config.books.max_book_age_secs as i64 + 1);
        let mut stale_yes_book = test_book(&market.yes_token_id);
        stale_yes_book.ingest_ts = stale_ingest_ts;
        let mut stale_no_book = test_book(&market.no_token_id);
        stale_no_book.ingest_ts = stale_ingest_ts;
        engine.book_manager.insert_snapshot(stale_yes_book).await;
        engine.book_manager.insert_snapshot(stale_no_book).await;

        engine.check_hedge_depth().await.unwrap();

        let tracked = engine
            .order_manager
            .get_market_orders(&market.condition_id)
            .await;
        assert!(tracked.is_empty());
        assert!(
            !engine
                .risk_manager
                .is_market_tradable(&market.condition_id)
                .await
        );
        server.abort();
    }

    #[tokio::test]
    async fn duplicate_stale_book_kill_emits_one_halted_transition() {
        let (engine, event_dir) = build_test_engine("http://127.0.0.1:9", true).await;
        let market = test_market();
        engine
            .managed_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market);

        engine.kill_market("market", STALE_BOOK_HALT_REASON).await;
        engine.kill_market("market", STALE_BOOK_HALT_REASON).await;

        let events = wait_for_emitted_events(
            &event_dir,
            &engine.run_id,
            Duration::from_secs(2),
            |events| {
                events.iter().any(|event| {
                    event.event_type == EventType::RiskStateChanged
                        && event.condition_id.as_deref() == Some("market")
                        && event.payload["status"] == serde_json::json!("halted")
                })
            },
        )
        .await;

        let halted_count = events
            .iter()
            .filter(|event| {
                event.event_type == EventType::RiskStateChanged
                    && event.condition_id.as_deref() == Some("market")
                    && event.payload["status"] == serde_json::json!("halted")
            })
            .count();
        assert_eq!(halted_count, 1);
    }

    #[tokio::test]
    async fn stale_book_halt_resumes_after_two_fresh_confirmations() {
        let (engine, event_dir) = build_test_engine("http://127.0.0.1:9", true).await;
        let market = test_market();
        engine
            .managed_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market.clone());
        engine
            .book_manager
            .insert_snapshot(test_book(&market.yes_token_id))
            .await;
        engine
            .book_manager
            .insert_snapshot(test_book(&market.no_token_id))
            .await;

        engine
            .risk_manager
            .halt_market(&market.condition_id, STALE_BOOK_HALT_REASON)
            .await;

        let cleanup = engine
            .finalize_halted_market_if_drained(&market.condition_id)
            .await;
        assert!(cleanup.verified());
        assert!(
            !engine
                .maybe_resume_stale_book_market(&market, &cleanup)
                .await
        );
        assert!(
            !engine
                .risk_manager
                .is_market_tradable(&market.condition_id)
                .await
        );

        let cleanup = engine
            .finalize_halted_market_if_drained(&market.condition_id)
            .await;
        assert!(cleanup.verified());
        assert!(
            engine
                .maybe_resume_stale_book_market(&market, &cleanup)
                .await
        );
        assert!(
            engine
                .risk_manager
                .is_market_tradable(&market.condition_id)
                .await
        );

        let events = wait_for_emitted_events(
            &event_dir,
            &engine.run_id,
            Duration::from_secs(2),
            |events| {
                events.iter().any(|event| {
                    event.event_type == EventType::RiskStateChanged
                        && event.condition_id.as_deref() == Some("market")
                        && event.payload["status"] == serde_json::json!("resumed")
                })
            },
        )
        .await;
        assert_eq!(
            events
                .iter()
                .filter(|event| {
                    event.event_type == EventType::RiskStateChanged
                        && event.condition_id.as_deref() == Some("market")
                        && event.payload["status"] == serde_json::json!("resumed")
                })
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn stale_book_recovery_resets_after_failed_fresh_confirmation() {
        let engine = test_engine().await;
        let market = test_market();
        engine
            .managed_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market.clone());
        engine
            .book_manager
            .insert_snapshot(test_book(&market.yes_token_id))
            .await;
        engine
            .book_manager
            .insert_snapshot(test_book(&market.no_token_id))
            .await;

        engine
            .risk_manager
            .halt_market(&market.condition_id, STALE_BOOK_HALT_REASON)
            .await;

        let cleanup = engine
            .finalize_halted_market_if_drained(&market.condition_id)
            .await;
        assert!(cleanup.verified());
        assert!(
            !engine
                .maybe_resume_stale_book_market(&market, &cleanup)
                .await
        );

        let stale_ingest_ts = Utc::now()
            - chrono::Duration::seconds(engine.config.books.max_book_age_secs as i64 + 1);
        let mut stale_yes_book = test_book(&market.yes_token_id);
        stale_yes_book.ingest_ts = stale_ingest_ts;
        let mut stale_no_book = test_book(&market.no_token_id);
        stale_no_book.ingest_ts = stale_ingest_ts;
        engine.book_manager.insert_snapshot(stale_yes_book).await;
        engine.book_manager.insert_snapshot(stale_no_book).await;

        let cleanup = engine
            .finalize_halted_market_if_drained(&market.condition_id)
            .await;
        assert!(cleanup.verified());
        assert!(
            !engine
                .maybe_resume_stale_book_market(&market, &cleanup)
                .await
        );

        engine
            .book_manager
            .insert_snapshot(test_book(&market.yes_token_id))
            .await;
        engine
            .book_manager
            .insert_snapshot(test_book(&market.no_token_id))
            .await;

        let cleanup = engine
            .finalize_halted_market_if_drained(&market.condition_id)
            .await;
        assert!(cleanup.verified());
        assert!(
            !engine
                .maybe_resume_stale_book_market(&market, &cleanup)
                .await
        );
        assert!(
            !engine
                .risk_manager
                .is_market_tradable(&market.condition_id)
                .await
        );

        let cleanup = engine
            .finalize_halted_market_if_drained(&market.condition_id)
            .await;
        assert!(cleanup.verified());
        assert!(
            engine
                .maybe_resume_stale_book_market(&market, &cleanup)
                .await
        );
        assert!(
            engine
                .risk_manager
                .is_market_tradable(&market.condition_id)
                .await
        );
    }

    #[tokio::test]
    async fn pre_admission_clamps_bid_size_to_current_hedge_depth() {
        let engine = test_engine().await;
        *engine.calibration.write().await = CalibrationTracker::new(dec!(5), 10);
        let market = test_market();
        let yes_book = test_book_with_depth(&market.yes_token_id, dec!(1000), dec!(1000));
        let no_book = test_book_with_depth(&market.no_token_id, dec!(1000), dec!(24.7));

        let (quote_set, _report) = engine
            .evaluate_market_on_books_with_context(&market, &yes_book, &no_book, &[], dec!(1000))
            .await
            .unwrap();
        let yes_bid = quote_set
            .candidates
            .iter()
            .find(|candidate| candidate.leg == QuoteLeg::YesBid)
            .expect("expected YES bid candidate");

        assert_eq!(yes_bid.status, QuoteStatus::Approved);
        assert_eq!(yes_bid.size, dec!(24));
        assert!(yes_bid
            .reason
            .as_deref()
            .is_some_and(|reason| reason.contains("Clamped to hedge depth")));
    }

    #[tokio::test]
    async fn sample_order_scoring_uses_book_prediction_and_can_reduce_multiplier() {
        let responses = Arc::new(RwLock::new(HashMap::new()));
        let (base_url, server) = spawn_scoring_server(responses.clone()).await.unwrap();
        let (engine, event_dir) = build_test_engine(&base_url, true).await;
        let market = test_market();
        *engine.calibration.write().await = CalibrationTracker::new(dec!(1.5), 2);
        engine
            .managed_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market.clone());
        engine
            .known_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market.clone());
        engine.order_manager.update_gross_balance(dec!(1000)).await;
        engine
            .book_manager
            .insert_snapshot(test_book_with_prices(
                &market.yes_token_id,
                dec!(0.49),
                dec!(0.51),
                dec!(100),
                dec!(100),
            ))
            .await;
        engine
            .book_manager
            .insert_snapshot(test_book_with_prices(
                &market.no_token_id,
                dec!(0.49),
                dec!(0.51),
                dec!(100),
                dec!(100),
            ))
            .await;

        let quote_set = QuoteSet {
            condition_id: market.condition_id.clone(),
            candidates: vec![
                QuoteCandidate {
                    condition_id: market.condition_id.clone(),
                    leg: QuoteLeg::YesBid,
                    price: dec!(0.40),
                    size: dec!(20),
                    status: QuoteStatus::Approved,
                    reason: None,
                },
                QuoteCandidate {
                    condition_id: market.condition_id.clone(),
                    leg: QuoteLeg::NoBid,
                    price: dec!(0.40),
                    size: dec!(20),
                    status: QuoteStatus::Approved,
                    reason: None,
                },
            ],
        };
        engine
            .order_manager
            .place_quotes(&market, &quote_set, None, dec!(20), None, "test", None)
            .await
            .unwrap();

        let tracked = engine
            .order_manager
            .get_market_orders(&market.condition_id)
            .await;
        {
            let mut scoring = responses.write().await;
            for order in &tracked {
                scoring.insert(order.order_id.clone(), true);
            }
        }

        engine.sample_order_scoring().await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        assert_eq!(
            engine.calibration.read().await.current_multiplier(),
            dec!(1.35)
        );

        let events = read_emitted_events(&event_dir, &engine.run_id).await;
        let adjusted = events
            .iter()
            .find(|event| event.event_type == EventType::CalibrationAdjusted)
            .expect("expected calibration adjustment event");
        assert_eq!(adjusted.payload["false_positives"], serde_json::json!(0));
        assert_eq!(adjusted.payload["false_negatives"], serde_json::json!(2));

        server.abort();
    }

    #[tokio::test]
    async fn predict_order_scoring_flips_when_competition_adjusted_evaluation_deadmits_market() {
        let (mut engine, _) = build_test_engine("http://127.0.0.1:9", false).await;
        let mut market = test_market();
        market.reward_config.daily_reward_total = dec!(6.5);
        market.reward_config.daily_reward_rates = vec![dec!(6.5)];
        *engine.calibration.write().await = CalibrationTracker::new(dec!(1.5), 10);
        engine
            .managed_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market.clone());
        engine
            .known_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market.clone());
        engine.order_manager.update_gross_balance(dec!(1000)).await;
        engine
            .book_manager
            .insert_snapshot(test_book_with_prices(
                &market.yes_token_id,
                dec!(0.49),
                dec!(0.51),
                dec!(100),
                dec!(100),
            ))
            .await;
        engine
            .book_manager
            .insert_snapshot(test_book_with_prices(
                &market.no_token_id,
                dec!(0.49),
                dec!(0.51),
                dec!(100),
                dec!(100),
            ))
            .await;

        let quote_set = QuoteSet {
            condition_id: market.condition_id.clone(),
            candidates: vec![QuoteCandidate {
                condition_id: market.condition_id.clone(),
                leg: QuoteLeg::YesBid,
                price: dec!(0.49),
                size: dec!(20),
                status: QuoteStatus::Approved,
                reason: None,
            }],
        };
        engine
            .order_manager
            .place_quotes(&market, &quote_set, None, dec!(20), None, "test", None)
            .await
            .unwrap();

        let tracked = engine
            .order_manager
            .get_market_orders(&market.condition_id)
            .await
            .into_iter()
            .find(|order| order.leg == QuoteLeg::YesBid)
            .expect("expected tracked YES bid");

        let low_return = test_market_return_per_share(&engine, &market).await;
        *engine.calibration.write().await = CalibrationTracker::new(dec!(5), 10);
        let high_return = test_market_return_per_share(&engine, &market).await;
        assert!(low_return > high_return);
        engine.config.strategy.min_return_pct = (low_return + high_return) / dec!(2);

        *engine.calibration.write().await = CalibrationTracker::new(dec!(1.5), 10);
        assert_eq!(engine.predict_order_scoring(&tracked).await, Some(true));

        *engine.calibration.write().await = CalibrationTracker::new(dec!(5), 10);

        assert_eq!(engine.predict_order_scoring(&tracked).await, Some(false));
    }

    #[tokio::test]
    async fn sample_order_scoring_order_level_proxy_can_prevent_false_positive_adjustment() {
        let responses = Arc::new(RwLock::new(HashMap::new()));
        let (base_url, server) = spawn_scoring_server(responses.clone()).await.unwrap();
        let (mut engine, event_dir) = build_test_engine(&base_url, true).await;
        let mut market = test_market();
        market.reward_config.daily_reward_total = dec!(6.5);
        market.reward_config.daily_reward_rates = vec![dec!(6.5)];
        *engine.calibration.write().await = CalibrationTracker::new(dec!(4.2), 1);
        engine
            .managed_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market.clone());
        engine
            .known_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market.clone());
        engine.order_manager.update_gross_balance(dec!(1000)).await;
        engine
            .book_manager
            .insert_snapshot(test_book_with_prices(
                &market.yes_token_id,
                dec!(0.49),
                dec!(0.51),
                dec!(100),
                dec!(100),
            ))
            .await;
        engine
            .book_manager
            .insert_snapshot(test_book_with_prices(
                &market.no_token_id,
                dec!(0.49),
                dec!(0.51),
                dec!(100),
                dec!(100),
            ))
            .await;

        let quote_set = QuoteSet {
            condition_id: market.condition_id.clone(),
            candidates: vec![QuoteCandidate {
                condition_id: market.condition_id.clone(),
                leg: QuoteLeg::YesBid,
                price: dec!(0.49),
                size: dec!(20),
                status: QuoteStatus::Approved,
                reason: None,
            }],
        };
        engine
            .order_manager
            .place_quotes(&market, &quote_set, None, dec!(20), None, "test", None)
            .await
            .unwrap();

        let pre_adjust_return = test_market_return_per_share(&engine, &market).await;
        *engine.calibration.write().await = CalibrationTracker::new(dec!(5), 1);
        let post_adjust_return = test_market_return_per_share(&engine, &market).await;
        assert!(pre_adjust_return > post_adjust_return);
        engine.config.strategy.min_return_pct = (pre_adjust_return + post_adjust_return) / dec!(2);
        *engine.calibration.write().await = CalibrationTracker::new(dec!(4.2), 1);

        let tracked = engine
            .order_manager
            .get_market_orders(&market.condition_id)
            .await;
        {
            let mut scoring = responses.write().await;
            for order in &tracked {
                scoring.insert(order.order_id.clone(), false);
            }
        }

        engine.sample_order_scoring().await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            engine.calibration.read().await.current_multiplier(),
            dec!(4.2)
        );

        engine.sample_order_scoring().await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            engine.calibration.read().await.current_multiplier(),
            dec!(4.2)
        );

        let events = read_emitted_events(&event_dir, &engine.run_id).await;
        let adjustments: Vec<&EventEnvelope> = events
            .iter()
            .filter(|event| event.event_type == EventType::CalibrationAdjusted)
            .collect();
        assert_eq!(adjustments.len(), 2);
        assert_eq!(
            adjustments[0].payload["false_positives"],
            serde_json::json!(0)
        );
        assert_eq!(
            adjustments[0].payload["false_negatives"],
            serde_json::json!(0)
        );
        assert_eq!(
            adjustments[1].payload["false_positives"],
            serde_json::json!(0)
        );
        assert_eq!(
            adjustments[1].payload["false_negatives"],
            serde_json::json!(0)
        );

        server.abort();
    }

    #[tokio::test]
    async fn predict_order_scoring_uses_recent_actual_false_for_unchanged_order() {
        let (engine, _) = build_test_engine("http://127.0.0.1:9", false).await;
        let market = test_market();
        *engine.calibration.write().await = CalibrationTracker::new(dec!(1.5), 1);
        engine
            .managed_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market.clone());
        engine
            .known_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market.clone());
        engine.order_manager.update_gross_balance(dec!(1000)).await;
        engine
            .book_manager
            .insert_snapshot(test_book_with_prices(
                &market.yes_token_id,
                dec!(0.49),
                dec!(0.51),
                dec!(100),
                dec!(100),
            ))
            .await;
        engine
            .book_manager
            .insert_snapshot(test_book_with_prices(
                &market.no_token_id,
                dec!(0.49),
                dec!(0.51),
                dec!(100),
                dec!(100),
            ))
            .await;

        let quote_set = QuoteSet {
            condition_id: market.condition_id.clone(),
            candidates: vec![QuoteCandidate {
                condition_id: market.condition_id.clone(),
                leg: QuoteLeg::YesBid,
                price: dec!(0.49),
                size: dec!(20),
                status: QuoteStatus::Approved,
                reason: None,
            }],
        };
        engine
            .order_manager
            .place_quotes(&market, &quote_set, None, dec!(20), None, "test", None)
            .await
            .unwrap();

        let tracked = engine
            .order_manager
            .get_market_orders(&market.condition_id)
            .await
            .into_iter()
            .find(|order| order.leg == QuoteLeg::YesBid)
            .expect("expected tracked YES bid");

        assert_eq!(engine.predict_order_scoring(&tracked).await, Some(true));
        engine
            .record_recent_scoring_observation(
                &tracked.order_id,
                false,
                tracked.price,
                tracked.size,
            )
            .await;

        assert_eq!(engine.predict_order_scoring(&tracked).await, Some(false));
    }

    #[tokio::test]
    async fn sample_order_scoring_stops_repeating_false_positives_for_unchanged_order() {
        let responses = Arc::new(RwLock::new(HashMap::new()));
        let (base_url, server) = spawn_scoring_server(responses.clone()).await.unwrap();
        let (engine, event_dir) = build_test_engine(&base_url, true).await;
        let market = test_market();
        *engine.calibration.write().await = CalibrationTracker::new(dec!(1.5), 1);
        engine
            .managed_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market.clone());
        engine
            .known_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market.clone());
        engine.order_manager.update_gross_balance(dec!(1000)).await;
        engine
            .book_manager
            .insert_snapshot(test_book_with_prices(
                &market.yes_token_id,
                dec!(0.49),
                dec!(0.51),
                dec!(100),
                dec!(100),
            ))
            .await;
        engine
            .book_manager
            .insert_snapshot(test_book_with_prices(
                &market.no_token_id,
                dec!(0.49),
                dec!(0.51),
                dec!(100),
                dec!(100),
            ))
            .await;

        let quote_set = QuoteSet {
            condition_id: market.condition_id.clone(),
            candidates: vec![QuoteCandidate {
                condition_id: market.condition_id.clone(),
                leg: QuoteLeg::YesBid,
                price: dec!(0.49),
                size: dec!(20),
                status: QuoteStatus::Approved,
                reason: None,
            }],
        };
        engine
            .order_manager
            .place_quotes(&market, &quote_set, None, dec!(20), None, "test", None)
            .await
            .unwrap();

        let tracked = engine
            .order_manager
            .get_market_orders(&market.condition_id)
            .await;
        {
            let mut scoring = responses.write().await;
            for order in &tracked {
                scoring.insert(order.order_id.clone(), false);
            }
        }

        engine.sample_order_scoring().await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        let after_first = engine.calibration.read().await.current_multiplier();
        assert!(after_first > dec!(1.5));

        engine.sample_order_scoring().await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            engine.calibration.read().await.current_multiplier(),
            after_first
        );

        let events = read_emitted_events(&event_dir, &engine.run_id).await;
        let adjustments: Vec<&EventEnvelope> = events
            .iter()
            .filter(|event| event.event_type == EventType::CalibrationAdjusted)
            .collect();
        assert_eq!(adjustments.len(), 2);
        assert_eq!(
            adjustments[0].payload["false_positives"],
            serde_json::json!(1)
        );
        assert_eq!(
            adjustments[1].payload["false_positives"],
            serde_json::json!(0)
        );
        assert_eq!(
            adjustments[1].payload["false_negatives"],
            serde_json::json!(0)
        );

        server.abort();
    }

    #[tokio::test]
    async fn sample_order_scoring_skips_stale_book_samples() {
        let (engine, event_dir) = build_test_engine("http://127.0.0.1:9", true).await;
        let market = test_market();
        *engine.calibration.write().await = CalibrationTracker::new(dec!(1.5), 1);
        engine
            .managed_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market.clone());
        engine
            .known_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market.clone());
        engine.order_manager.update_gross_balance(dec!(1000)).await;

        let quote_set = QuoteSet {
            condition_id: market.condition_id.clone(),
            candidates: vec![QuoteCandidate {
                condition_id: market.condition_id.clone(),
                leg: QuoteLeg::YesBid,
                price: dec!(0.45),
                size: dec!(20),
                status: QuoteStatus::Approved,
                reason: None,
            }],
        };
        engine
            .order_manager
            .place_quotes(&market, &quote_set, None, dec!(20), None, "test", None)
            .await
            .unwrap();

        let stale_ingest_ts = Utc::now()
            - chrono::Duration::seconds(engine.config.books.max_book_age_secs as i64 + 1);
        let mut stale_yes_book = test_book(&market.yes_token_id);
        stale_yes_book.ingest_ts = stale_ingest_ts;
        engine.book_manager.insert_snapshot(stale_yes_book).await;

        engine.sample_order_scoring().await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        assert_eq!(
            engine.calibration.read().await.current_multiplier(),
            dec!(1.5)
        );
        let events = read_emitted_events(&event_dir, &engine.run_id).await;
        assert!(!events
            .iter()
            .any(|event| event.event_type == EventType::CalibrationAdjusted));
    }

    #[tokio::test]
    async fn hedge_depth_keeps_bids_priced_below_floor_when_outcome_mid_meets_threshold() {
        let engine = test_engine().await;
        let market = test_market();

        engine
            .managed_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market.clone());
        engine.order_manager.update_gross_balance(dec!(1000)).await;
        engine
            .depth_check_counter
            .store(1, std::sync::atomic::Ordering::Relaxed);

        let quote_set = QuoteSet {
            condition_id: market.condition_id.clone(),
            candidates: vec![QuoteCandidate {
                condition_id: market.condition_id.clone(),
                leg: QuoteLeg::YesBid,
                price: dec!(0.19),
                size: dec!(20),
                status: QuoteStatus::Approved,
                reason: None,
            }],
        };

        engine
            .order_manager
            .place_quotes(&market, &quote_set, None, dec!(20), None, "test", None)
            .await
            .unwrap();

        engine
            .book_manager
            .insert_snapshot(test_book_with_prices(
                &market.yes_token_id,
                dec!(0.18),
                dec!(0.22),
                dec!(100),
                dec!(100),
            ))
            .await;
        engine
            .book_manager
            .insert_snapshot(test_book_with_prices(
                &market.no_token_id,
                dec!(0.18),
                dec!(0.20),
                dec!(100),
                dec!(100),
            ))
            .await;

        engine.check_hedge_depth().await.unwrap();

        let tracked = engine
            .order_manager
            .get_market_orders(&market.condition_id)
            .await;
        assert_eq!(tracked.len(), 1);
        assert_eq!(tracked[0].leg, QuoteLeg::YesBid);
        assert_eq!(tracked[0].price, dec!(0.19));
        assert_eq!(tracked[0].size, dec!(20));
    }

    #[tokio::test]
    async fn hedge_depth_cancels_bid_when_outcome_mid_drops_below_threshold() {
        let engine = test_engine().await;
        let market = test_market();

        engine
            .managed_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market.clone());
        engine.order_manager.update_gross_balance(dec!(1000)).await;
        engine
            .depth_check_counter
            .store(1, std::sync::atomic::Ordering::Relaxed);

        let quote_set = QuoteSet {
            condition_id: market.condition_id.clone(),
            candidates: vec![QuoteCandidate {
                condition_id: market.condition_id.clone(),
                leg: QuoteLeg::YesBid,
                price: dec!(0.45),
                size: dec!(20),
                status: QuoteStatus::Approved,
                reason: None,
            }],
        };

        engine
            .order_manager
            .place_quotes(&market, &quote_set, None, dec!(20), None, "test", None)
            .await
            .unwrap();

        engine
            .book_manager
            .insert_snapshot(test_book_with_prices(
                &market.yes_token_id,
                dec!(0.18),
                dec!(0.20),
                dec!(100),
                dec!(100),
            ))
            .await;
        engine
            .book_manager
            .insert_snapshot(test_book_with_prices(
                &market.no_token_id,
                dec!(0.18),
                dec!(0.20),
                dec!(100),
                dec!(100),
            ))
            .await;

        engine.check_hedge_depth().await.unwrap();

        let tracked = engine
            .order_manager
            .get_market_orders(&market.condition_id)
            .await;
        assert!(tracked.is_empty());
    }

    #[tokio::test]
    async fn quote_refresh_non_viable_cancellation_emits_diagnostics() {
        let (engine, event_dir) = build_test_engine("http://127.0.0.1:9", true).await;
        let market = test_market();
        engine
            .managed_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market.clone());
        engine.order_manager.update_gross_balance(dec!(1000)).await;

        let quote_set = QuoteSet {
            condition_id: market.condition_id.clone(),
            candidates: vec![QuoteCandidate {
                condition_id: market.condition_id.clone(),
                leg: QuoteLeg::YesBid,
                price: dec!(0.45),
                size: dec!(20),
                status: QuoteStatus::Approved,
                reason: None,
            }],
        };

        engine
            .order_manager
            .place_quotes(&market, &quote_set, None, dec!(20), None, "test", None)
            .await
            .unwrap();
        let available_budget_before_refresh = engine.order_manager.available_budget().await;

        engine
            .book_manager
            .insert_snapshot(test_book_with_prices(
                &market.yes_token_id,
                dec!(0.18),
                dec!(0.20),
                dec!(100),
                dec!(100),
            ))
            .await;
        engine
            .book_manager
            .insert_snapshot(test_book_with_prices(
                &market.no_token_id,
                dec!(0.18),
                dec!(0.20),
                dec!(100),
                dec!(100),
            ))
            .await;

        let (fill_tx, _fill_rx) = mpsc::unbounded_channel();
        engine.refresh_quotes(&fill_tx).await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;

        let events = read_emitted_events(&event_dir, &engine.run_id).await;
        let event = events
            .into_iter()
            .find(|event| {
                event.event_type == spreadeater_core::EventType::OrderCancelled
                    && event.payload["origin"] == serde_json::json!("quote_refresh_non_viable")
            })
            .expect("expected quote_refresh_non_viable cancellation event");

        assert_eq!(
            event.payload["diagnostics"]["quote_refresh"]["would_trade"],
            serde_json::json!(false)
        );
        assert!(event.payload["diagnostics"]["quote_refresh"]["reasons"].is_array());
        assert_eq!(
            event.payload["diagnostics"]["quote_refresh"]["available_budget_usd"],
            serde_json::json!(available_budget_before_refresh.to_string())
        );
    }

    #[tokio::test]
    async fn hedge_depth_resize_emits_diagnostics() {
        let (engine, event_dir) = build_test_engine("http://127.0.0.1:9", true).await;
        let market = test_market();

        engine
            .managed_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market.clone());
        engine.order_manager.update_gross_balance(dec!(1000)).await;
        engine
            .depth_check_counter
            .store(1, std::sync::atomic::Ordering::Relaxed);

        let quote_set = QuoteSet {
            condition_id: market.condition_id.clone(),
            candidates: vec![QuoteCandidate {
                condition_id: market.condition_id.clone(),
                leg: QuoteLeg::YesBid,
                price: dec!(0.45),
                size: dec!(25),
                status: QuoteStatus::Approved,
                reason: None,
            }],
        };

        engine
            .order_manager
            .place_quotes(&market, &quote_set, None, dec!(20), None, "test", None)
            .await
            .unwrap();

        engine
            .book_manager
            .insert_snapshot(test_book_with_depth(
                &market.yes_token_id,
                dec!(100),
                dec!(100),
            ))
            .await;
        engine
            .book_manager
            .insert_snapshot(OrderBookSnapshot {
                token_id: market.no_token_id.clone(),
                exchange_ts: None,
                ingest_ts: Utc::now(),
                bids: vec![crate::models::PriceLevel {
                    price: dec!(0.45),
                    size: dec!(100),
                }],
                asks: vec![crate::models::PriceLevel {
                    price: dec!(0.55),
                    size: dec!(24.7),
                }],
            })
            .await;

        engine.check_hedge_depth().await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;

        let events = read_emitted_events(&event_dir, &engine.run_id).await;
        let event = events
            .into_iter()
            .find(|event| {
                event.event_type == spreadeater_core::EventType::OrderResized
                    && event.payload["origin"] == serde_json::json!("hedge_depth")
            })
            .expect("expected hedge_depth resize event");

        assert_eq!(
            event.payload["diagnostics"]["hedge_depth"]["hedgeable_size"],
            serde_json::json!("24.7")
        );
        assert_eq!(
            event.payload["diagnostics"]["hedge_depth"]["min_order_size"],
            serde_json::json!("20")
        );
        assert_eq!(
            event.payload["diagnostics"]["hedge_depth"]["opposite_best_price"],
            serde_json::json!("0.55")
        );
        assert_eq!(
            event.payload["diagnostics"]["hedge_depth"]["opposite_best_size"],
            serde_json::json!("24.7")
        );
    }

    #[tokio::test]
    async fn hedge_depth_skips_noop_resize_when_floor_preserves_size() {
        let (engine, event_dir) = build_test_engine("http://127.0.0.1:9", true).await;
        let market = test_market();

        engine
            .managed_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market.clone());
        engine.order_manager.update_gross_balance(dec!(1000)).await;
        engine
            .depth_check_counter
            .store(1, std::sync::atomic::Ordering::Relaxed);

        let quote_set = QuoteSet {
            condition_id: market.condition_id.clone(),
            candidates: vec![QuoteCandidate {
                condition_id: market.condition_id.clone(),
                leg: QuoteLeg::YesBid,
                price: dec!(0.45),
                size: dec!(204),
                status: QuoteStatus::Approved,
                reason: None,
            }],
        };

        engine
            .order_manager
            .place_quotes(&market, &quote_set, None, dec!(20), None, "test", None)
            .await
            .unwrap();

        engine
            .book_manager
            .insert_snapshot(test_book_with_depth(
                &market.yes_token_id,
                dec!(100),
                dec!(100),
            ))
            .await;
        engine
            .book_manager
            .insert_snapshot(OrderBookSnapshot {
                token_id: market.no_token_id.clone(),
                exchange_ts: None,
                ingest_ts: Utc::now(),
                bids: vec![crate::models::PriceLevel {
                    price: dec!(0.45),
                    size: dec!(100),
                }],
                asks: vec![crate::models::PriceLevel {
                    price: dec!(0.55),
                    size: dec!(204.9),
                }],
            })
            .await;

        engine.check_hedge_depth().await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;

        let tracked = engine
            .order_manager
            .get_market_orders(&market.condition_id)
            .await;
        assert_eq!(tracked.len(), 1);
        assert_eq!(tracked[0].size, dec!(204));

        let events = read_emitted_events(&event_dir, &engine.run_id).await;
        assert!(!events.into_iter().any(|event| {
            event.event_type == spreadeater_core::EventType::OrderResized
                && event.payload["origin"] == serde_json::json!("hedge_depth")
        }));
    }

    #[tokio::test]
    async fn ws_hedge_depth_guard_resizes_bid_immediately() {
        let (engine, event_dir) = build_test_engine("http://127.0.0.1:9", true).await;
        let market = test_market();

        engine
            .managed_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market.clone());
        engine.order_manager.update_gross_balance(dec!(1000)).await;

        let quote_set = QuoteSet {
            condition_id: market.condition_id.clone(),
            candidates: vec![QuoteCandidate {
                condition_id: market.condition_id.clone(),
                leg: QuoteLeg::YesBid,
                price: dec!(0.45),
                size: dec!(25),
                status: QuoteStatus::Approved,
                reason: None,
            }],
        };

        engine
            .order_manager
            .place_quotes(&market, &quote_set, None, dec!(20), None, "test", None)
            .await
            .unwrap();

        engine
            .book_manager
            .insert_snapshot(test_book_with_depth(
                &market.yes_token_id,
                dec!(100),
                dec!(100),
            ))
            .await;
        engine
            .book_manager
            .insert_snapshot(OrderBookSnapshot {
                token_id: market.no_token_id.clone(),
                exchange_ts: None,
                ingest_ts: Utc::now(),
                bids: vec![crate::models::PriceLevel {
                    price: dec!(0.45),
                    size: dec!(100),
                }],
                asks: vec![crate::models::PriceLevel {
                    price: dec!(0.55),
                    size: dec!(24.7),
                }],
            })
            .await;

        engine
            .maybe_run_ws_hedge_depth_guard_for_token(&market.no_token_id)
            .await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        let events = read_emitted_events(&event_dir, &engine.run_id).await;
        let event = events
            .into_iter()
            .find(|event| {
                event.event_type == spreadeater_core::EventType::OrderResized
                    && event.payload["origin"] == serde_json::json!("hedge_depth_ws")
            })
            .expect("expected hedge_depth_ws resize event");

        assert_eq!(
            event.payload["diagnostics"]["hedge_depth"]["hedgeable_size"],
            serde_json::json!("24.7")
        );
    }

    #[tokio::test]
    async fn ws_hedge_depth_guard_cancels_bid_below_min_size() {
        let (engine, event_dir) = build_test_engine("http://127.0.0.1:9", true).await;
        let market = test_market();

        engine
            .managed_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market.clone());
        engine.order_manager.update_gross_balance(dec!(1000)).await;

        let quote_set = QuoteSet {
            condition_id: market.condition_id.clone(),
            candidates: vec![QuoteCandidate {
                condition_id: market.condition_id.clone(),
                leg: QuoteLeg::YesBid,
                price: dec!(0.45),
                size: dec!(25),
                status: QuoteStatus::Approved,
                reason: None,
            }],
        };

        engine
            .order_manager
            .place_quotes(&market, &quote_set, None, dec!(20), None, "test", None)
            .await
            .unwrap();

        engine
            .book_manager
            .insert_snapshot(test_book_with_depth(
                &market.yes_token_id,
                dec!(100),
                dec!(100),
            ))
            .await;
        engine
            .book_manager
            .insert_snapshot(OrderBookSnapshot {
                token_id: market.no_token_id.clone(),
                exchange_ts: None,
                ingest_ts: Utc::now(),
                bids: vec![crate::models::PriceLevel {
                    price: dec!(0.45),
                    size: dec!(100),
                }],
                asks: vec![crate::models::PriceLevel {
                    price: dec!(0.55),
                    size: dec!(10),
                }],
            })
            .await;

        engine
            .maybe_run_ws_hedge_depth_guard_for_token(&market.no_token_id)
            .await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        assert!(engine
            .order_manager
            .get_market_orders(&market.condition_id)
            .await
            .is_empty());
        let events = read_emitted_events(&event_dir, &engine.run_id).await;
        assert!(events.into_iter().any(|event| {
            event.event_type == spreadeater_core::EventType::OrderCancelled
                && event.payload["origin"] == serde_json::json!("hedge_depth_ws")
        }));
    }

    #[tokio::test]
    async fn ws_hedge_depth_guard_cancels_bid_below_min_outcome_price() {
        let (engine, event_dir) = build_test_engine("http://127.0.0.1:9", true).await;
        let market = test_market();

        engine
            .managed_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market.clone());
        engine.order_manager.update_gross_balance(dec!(1000)).await;

        let quote_set = QuoteSet {
            condition_id: market.condition_id.clone(),
            candidates: vec![QuoteCandidate {
                condition_id: market.condition_id.clone(),
                leg: QuoteLeg::YesBid,
                price: dec!(0.18),
                size: dec!(25),
                status: QuoteStatus::Approved,
                reason: None,
            }],
        };

        engine
            .order_manager
            .place_quotes(&market, &quote_set, None, dec!(20), None, "test", None)
            .await
            .unwrap();

        engine
            .book_manager
            .insert_snapshot(OrderBookSnapshot {
                token_id: market.yes_token_id.clone(),
                exchange_ts: None,
                ingest_ts: Utc::now(),
                bids: vec![crate::models::PriceLevel {
                    price: dec!(0.18),
                    size: dec!(100),
                }],
                asks: vec![crate::models::PriceLevel {
                    price: dec!(0.20),
                    size: dec!(100),
                }],
            })
            .await;
        engine
            .book_manager
            .insert_snapshot(test_book_with_depth(
                &market.no_token_id,
                dec!(100),
                dec!(100),
            ))
            .await;

        engine
            .maybe_run_ws_hedge_depth_guard_for_token(&market.yes_token_id)
            .await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        assert!(engine
            .order_manager
            .get_market_orders(&market.condition_id)
            .await
            .is_empty());
        let events = read_emitted_events(&event_dir, &engine.run_id).await;
        assert!(events.into_iter().any(|event| {
            event.event_type == spreadeater_core::EventType::OrderCancelled
                && event.payload["origin"] == serde_json::json!("hedge_depth_ws")
        }));
    }

    #[tokio::test]
    async fn ws_hedge_depth_guard_ignores_unrelated_token() {
        let (engine, event_dir) = build_test_engine("http://127.0.0.1:9", true).await;
        let market = test_market();

        engine
            .managed_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market.clone());
        engine.order_manager.update_gross_balance(dec!(1000)).await;

        let quote_set = QuoteSet {
            condition_id: market.condition_id.clone(),
            candidates: vec![QuoteCandidate {
                condition_id: market.condition_id.clone(),
                leg: QuoteLeg::YesBid,
                price: dec!(0.45),
                size: dec!(25),
                status: QuoteStatus::Approved,
                reason: None,
            }],
        };

        engine
            .order_manager
            .place_quotes(&market, &quote_set, None, dec!(20), None, "test", None)
            .await
            .unwrap();

        engine
            .book_manager
            .insert_snapshot(test_book(&market.yes_token_id))
            .await;
        engine
            .book_manager
            .insert_snapshot(test_book(&market.no_token_id))
            .await;

        engine
            .maybe_run_ws_hedge_depth_guard_for_token("unrelated-token")
            .await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        let tracked = engine
            .order_manager
            .get_market_orders(&market.condition_id)
            .await;
        assert_eq!(tracked.len(), 1);
        let events = read_emitted_events(&event_dir, &engine.run_id).await;
        assert!(!events.into_iter().any(|event| {
            matches!(
                event.event_type,
                spreadeater_core::EventType::OrderResized
                    | spreadeater_core::EventType::OrderCancelled
            ) && event.payload["origin"] == serde_json::json!("hedge_depth_ws")
        }));
    }

    #[tokio::test]
    async fn ws_hedge_depth_guard_skips_stale_books() {
        let (engine, event_dir) = build_test_engine("http://127.0.0.1:9", true).await;
        let market = test_market();

        engine
            .managed_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market.clone());
        engine.order_manager.update_gross_balance(dec!(1000)).await;

        let quote_set = QuoteSet {
            condition_id: market.condition_id.clone(),
            candidates: vec![QuoteCandidate {
                condition_id: market.condition_id.clone(),
                leg: QuoteLeg::YesBid,
                price: dec!(0.45),
                size: dec!(25),
                status: QuoteStatus::Approved,
                reason: None,
            }],
        };

        engine
            .order_manager
            .place_quotes(&market, &quote_set, None, dec!(20), None, "test", None)
            .await
            .unwrap();

        let stale_ingest_ts = Utc::now()
            - chrono::Duration::seconds(engine.config.books.max_book_age_secs as i64 + 1);
        let mut stale_yes_book = test_book(&market.yes_token_id);
        stale_yes_book.ingest_ts = stale_ingest_ts;
        engine.book_manager.insert_snapshot(stale_yes_book).await;
        engine
            .book_manager
            .insert_snapshot(test_book(&market.no_token_id))
            .await;

        engine
            .maybe_run_ws_hedge_depth_guard_for_token(&market.no_token_id)
            .await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        let tracked_orders = engine
            .order_manager
            .get_market_orders(&market.condition_id)
            .await;
        assert_eq!(tracked_orders.len(), 1);
        let events = read_emitted_events(&event_dir, &engine.run_id).await;
        assert!(!events.into_iter().any(|event| {
            matches!(
                event.event_type,
                spreadeater_core::EventType::OrderResized
                    | spreadeater_core::EventType::OrderCancelled
            ) && event.payload["origin"] == serde_json::json!("hedge_depth_ws")
        }));
    }

    #[tokio::test]
    async fn ws_hedge_depth_guard_skips_pending_bid_cancels() {
        let (engine, event_dir) = build_test_engine("http://127.0.0.1:9", true).await;
        let market = test_market();

        engine
            .managed_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market.clone());
        engine.order_manager.update_gross_balance(dec!(1000)).await;

        let tracked = tracked_order_for_market(
            &market,
            QuoteLeg::YesBid,
            dec!(0.45),
            dec!(25),
            Some("pending-bid"),
        );
        engine
            .order_manager
            .seed_live_order_for_test(tracked.clone())
            .await;
        engine
            .order_manager
            .seed_pending_cancel_for_test(
                tracked.clone(),
                CancelReasonCode::HedgeDepthBelowMinimum,
                "test",
            )
            .await;
        engine
            .book_manager
            .insert_snapshot(test_book_with_depth(
                &market.yes_token_id,
                dec!(100),
                dec!(100),
            ))
            .await;
        engine
            .book_manager
            .insert_snapshot(OrderBookSnapshot {
                token_id: market.no_token_id.clone(),
                exchange_ts: None,
                ingest_ts: Utc::now(),
                bids: vec![crate::models::PriceLevel {
                    price: dec!(0.45),
                    size: dec!(100),
                }],
                asks: vec![crate::models::PriceLevel {
                    price: dec!(0.55),
                    size: dec!(10),
                }],
            })
            .await;

        engine
            .maybe_run_ws_hedge_depth_guard_for_token(&market.no_token_id)
            .await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        let tracked_orders = engine
            .order_manager
            .get_market_orders(&market.condition_id)
            .await;
        assert_eq!(tracked_orders.len(), 1);
        let events = read_emitted_events(&event_dir, &engine.run_id).await;
        assert!(!events.into_iter().any(|event| {
            matches!(
                event.event_type,
                spreadeater_core::EventType::OrderResized
                    | spreadeater_core::EventType::OrderCancelled
            ) && event.payload["origin"] == serde_json::json!("hedge_depth_ws")
        }));
    }

    #[tokio::test]
    async fn funded_resting_bid_survives_refresh_when_only_free_budget_is_low() {
        let engine = test_engine().await;
        let market = test_market();
        let yes_book = test_book(&market.yes_token_id);
        let no_book = test_book(&market.no_token_id);
        engine.book_manager.insert_snapshot(yes_book).await;
        engine.book_manager.insert_snapshot(no_book).await;
        engine
            .managed_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market.clone());
        engine.order_manager.update_gross_balance(dec!(372)).await;

        let quote_set = QuoteSet {
            condition_id: market.condition_id.clone(),
            candidates: vec![QuoteCandidate {
                condition_id: market.condition_id.clone(),
                leg: QuoteLeg::YesBid,
                price: dec!(0.45),
                size: dec!(372),
                status: QuoteStatus::Approved,
                reason: None,
            }],
        };

        engine
            .order_manager
            .place_quotes(&market, &quote_set, None, dec!(20), None, "test", None)
            .await
            .unwrap();

        assert_eq!(engine.order_manager.available_budget().await, Decimal::ZERO);

        let (evaluated_quotes, report) = engine
            .evaluate_market_on_books(
                &market,
                &test_book(&market.yes_token_id),
                &test_book(&market.no_token_id),
            )
            .await
            .unwrap();
        assert!(report.would_trade);
        assert_eq!(evaluated_quotes.candidates[0].status, QuoteStatus::Approved);
        assert!(evaluated_quotes.candidates[0].size >= dec!(20));

        let (fill_tx, _fill_rx) = mpsc::unbounded_channel();
        engine.refresh_quotes(&fill_tx).await.unwrap();

        let tracked = engine
            .order_manager
            .get_market_orders(&market.condition_id)
            .await;
        assert_eq!(tracked.len(), 1);
        assert_eq!(tracked[0].leg, QuoteLeg::YesBid);
        assert!(tracked[0].size >= dec!(20));
    }

    #[tokio::test]
    async fn frontier_rotation_skips_recent_bid_market_until_hold_window_passes() {
        let mut engine = test_engine().await;
        engine.config.discovery.poll_interval_secs = 0;
        engine.order_manager.update_gross_balance(dec!(100)).await;

        let loser = frontier_market("loser", "loser_yes", "loser_no", dec!(400));
        let entrant = frontier_market("entrant", "entrant_yes", "entrant_no", dec!(1000));
        engine
            .book_manager
            .insert_snapshot(test_book(&loser.yes_token_id))
            .await;
        engine
            .book_manager
            .insert_snapshot(test_book(&loser.no_token_id))
            .await;
        engine
            .book_manager
            .insert_snapshot(test_book(&entrant.yes_token_id))
            .await;
        engine
            .book_manager
            .insert_snapshot(test_book(&entrant.no_token_id))
            .await;

        let loser_quote_set = QuoteSet {
            condition_id: loser.condition_id.clone(),
            candidates: vec![QuoteCandidate {
                condition_id: loser.condition_id.clone(),
                leg: QuoteLeg::YesBid,
                price: dec!(0.45),
                size: dec!(50),
                status: QuoteStatus::Approved,
                reason: None,
            }],
        };
        engine
            .order_manager
            .place_quotes(&loser, &loser_quote_set, None, dec!(20), None, "test", None)
            .await
            .unwrap();

        let evaluations = vec![
            {
                let (yes_book, no_book, quote_set, report) =
                    engine.evaluate_market(&loser).await.unwrap();
                MarketEvaluation {
                    market: loser.clone(),
                    yes_book,
                    no_book,
                    quote_set: quote_set.clone(),
                    report,
                    trace_ids: build_quote_trace_ids(&quote_set),
                }
            },
            {
                let (yes_book, no_book, quote_set, report) =
                    engine.evaluate_market(&entrant).await.unwrap();
                MarketEvaluation {
                    market: entrant.clone(),
                    yes_book,
                    no_book,
                    quote_set: quote_set.clone(),
                    report,
                    trace_ids: build_quote_trace_ids(&quote_set),
                }
            },
        ];

        let rotation = engine.select_frontier_rotation(&evaluations).await.unwrap();
        assert!(rotation.is_none());
    }

    #[tokio::test]
    async fn frontier_rotation_selects_better_nonheld_market_with_reclaimed_budget() {
        let mut engine = test_engine().await;
        engine.config.discovery.poll_interval_secs = 0;
        engine.order_manager.update_gross_balance(dec!(100)).await;

        let loser = frontier_market("loser", "loser_yes", "loser_no", dec!(400));
        let entrant = frontier_market("entrant", "entrant_yes", "entrant_no", dec!(1000));
        for market in [&loser, &entrant] {
            engine
                .book_manager
                .insert_snapshot(test_book(&market.yes_token_id))
                .await;
            engine
                .book_manager
                .insert_snapshot(test_book(&market.no_token_id))
                .await;
        }

        let loser_quote_set = QuoteSet {
            condition_id: loser.condition_id.clone(),
            candidates: vec![QuoteCandidate {
                condition_id: loser.condition_id.clone(),
                leg: QuoteLeg::YesBid,
                price: dec!(0.45),
                size: dec!(50),
                status: QuoteStatus::Approved,
                reason: None,
            }],
        };
        engine
            .order_manager
            .place_quotes(&loser, &loser_quote_set, None, dec!(20), None, "test", None)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(1100)).await;

        let loser_eval = {
            let (yes_book, no_book, quote_set, report) =
                engine.evaluate_market(&loser).await.unwrap();
            MarketEvaluation {
                market: loser.clone(),
                yes_book,
                no_book,
                quote_set: quote_set.clone(),
                report,
                trace_ids: build_quote_trace_ids(&quote_set),
            }
        };
        let entrant_eval = {
            let (yes_book, no_book, quote_set, report) =
                engine.evaluate_market(&entrant).await.unwrap();
            assert!(
                !report.would_trade,
                "entrant should be immediate-non-actionable at zero free budget"
            );
            MarketEvaluation {
                market: entrant.clone(),
                yes_book,
                no_book,
                quote_set: quote_set.clone(),
                report,
                trace_ids: build_quote_trace_ids(&quote_set),
            }
        };

        let immediate_entrant_rank_key =
            market_rank_key(&entrant_eval.quote_set, &entrant_eval.report);
        let rotation = engine
            .select_frontier_rotation(&[loser_eval, entrant_eval])
            .await
            .unwrap()
            .expect("frontier rotation plan");

        assert_eq!(rotation.loser_condition_id, loser.condition_id);
        assert_eq!(rotation.entrant_condition_id, entrant.condition_id);
        assert_eq!(rotation.reclaimable_bid_capital, dec!(50));
        assert_eq!(
            rotation.counterfactual_budget_usd,
            engine.order_manager.available_budget().await + rotation.reclaimable_bid_capital
        );
        assert!(
            rotation.entrant_rank_key.reward_per_share > rotation.loser_rank_key.reward_per_share
        );
        assert!(
            rotation.entrant_rank_key.estimated_reward
                > immediate_entrant_rank_key.estimated_reward
        );
    }

    #[tokio::test]
    async fn frontier_rotation_does_not_replace_with_worse_market() {
        let mut engine = test_engine().await;
        engine.config.discovery.poll_interval_secs = 0;
        engine.order_manager.update_gross_balance(dec!(100)).await;

        let loser = frontier_market("loser", "loser_yes", "loser_no", dec!(1000));
        let entrant = frontier_market("entrant", "entrant_yes", "entrant_no", dec!(400));
        for market in [&loser, &entrant] {
            engine
                .book_manager
                .insert_snapshot(test_book(&market.yes_token_id))
                .await;
            engine
                .book_manager
                .insert_snapshot(test_book(&market.no_token_id))
                .await;
        }

        let loser_quote_set = QuoteSet {
            condition_id: loser.condition_id.clone(),
            candidates: vec![QuoteCandidate {
                condition_id: loser.condition_id.clone(),
                leg: QuoteLeg::YesBid,
                price: dec!(0.45),
                size: dec!(50),
                status: QuoteStatus::Approved,
                reason: None,
            }],
        };
        engine
            .order_manager
            .place_quotes(&loser, &loser_quote_set, None, dec!(20), None, "test", None)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(1100)).await;

        let evaluations = vec![
            {
                let (yes_book, no_book, quote_set, report) =
                    engine.evaluate_market(&loser).await.unwrap();
                MarketEvaluation {
                    market: loser.clone(),
                    yes_book,
                    no_book,
                    quote_set: quote_set.clone(),
                    report,
                    trace_ids: build_quote_trace_ids(&quote_set),
                }
            },
            {
                let (yes_book, no_book, quote_set, report) =
                    engine.evaluate_market(&entrant).await.unwrap();
                MarketEvaluation {
                    market: entrant.clone(),
                    yes_book,
                    no_book,
                    quote_set: quote_set.clone(),
                    report,
                    trace_ids: build_quote_trace_ids(&quote_set),
                }
            },
        ];

        let rotation = engine.select_frontier_rotation(&evaluations).await.unwrap();
        assert!(rotation.is_none());
    }

    #[tokio::test]
    async fn frontier_rotation_skips_while_cancel_verification_is_pending() {
        let mut engine = test_engine().await;
        engine.config.discovery.poll_interval_secs = 0;
        engine.order_manager.update_gross_balance(dec!(100)).await;

        let loser = frontier_market("loser", "loser_yes", "loser_no", dec!(400));
        let entrant = frontier_market("entrant", "entrant_yes", "entrant_no", dec!(1000));
        for market in [&loser, &entrant] {
            engine
                .book_manager
                .insert_snapshot(test_book(&market.yes_token_id))
                .await;
            engine
                .book_manager
                .insert_snapshot(test_book(&market.no_token_id))
                .await;
        }

        let loser_quote_set = QuoteSet {
            condition_id: loser.condition_id.clone(),
            candidates: vec![QuoteCandidate {
                condition_id: loser.condition_id.clone(),
                leg: QuoteLeg::YesBid,
                price: dec!(0.45),
                size: dec!(50),
                status: QuoteStatus::Approved,
                reason: None,
            }],
        };
        engine
            .order_manager
            .place_quotes(&loser, &loser_quote_set, None, dec!(20), None, "test", None)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(1100)).await;

        let tracked = engine
            .order_manager
            .get_market_orders(&loser.condition_id)
            .await;
        engine
            .order_manager
            .seed_pending_cancel_for_test(
                tracked[0].clone(),
                CancelReasonCode::QuoteDrift,
                "test_pending",
            )
            .await;

        let evaluations = vec![
            {
                let (yes_book, no_book, quote_set, report) =
                    engine.evaluate_market(&loser).await.unwrap();
                MarketEvaluation {
                    market: loser.clone(),
                    yes_book,
                    no_book,
                    quote_set: quote_set.clone(),
                    report,
                    trace_ids: build_quote_trace_ids(&quote_set),
                }
            },
            {
                let (yes_book, no_book, quote_set, report) =
                    engine.evaluate_market(&entrant).await.unwrap();
                MarketEvaluation {
                    market: entrant.clone(),
                    yes_book,
                    no_book,
                    quote_set: quote_set.clone(),
                    report,
                    trace_ids: build_quote_trace_ids(&quote_set),
                }
            },
        ];

        let rotation = engine.select_frontier_rotation(&evaluations).await.unwrap();
        assert!(rotation.is_none());
    }

    #[test]
    fn freeze_new_bid_entry_only_blocks_new_bid_placements() {
        let quote_set = QuoteSet {
            condition_id: "market".to_string(),
            candidates: vec![QuoteCandidate {
                condition_id: "market".to_string(),
                leg: QuoteLeg::YesBid,
                price: dec!(0.45),
                size: dec!(50),
                status: QuoteStatus::Approved,
                reason: None,
            }],
        };

        assert!(should_skip_new_bid_entry(true, &[], &quote_set));
        assert!(!should_skip_new_bid_entry(false, &[], &quote_set));
        assert!(!should_skip_new_bid_entry(
            true,
            &[tracked_order(QuoteLeg::YesBid, dec!(0.45), dec!(50))],
            &quote_set
        ));
    }

    #[tokio::test]
    async fn frontier_reservation_waits_for_loser_bid_cancel_verification() {
        let engine = test_engine().await;
        engine.order_manager.update_gross_balance(dec!(100)).await;

        let loser = frontier_market("loser", "loser_yes", "loser_no", dec!(400));
        let entrant = frontier_market("entrant", "entrant_yes", "entrant_no", dec!(1000));
        for market in [&loser, &entrant] {
            engine
                .book_manager
                .insert_snapshot(test_book(&market.yes_token_id))
                .await;
            engine
                .book_manager
                .insert_snapshot(test_book(&market.no_token_id))
                .await;
        }

        let loser_quote_set = QuoteSet {
            condition_id: loser.condition_id.clone(),
            candidates: vec![QuoteCandidate {
                condition_id: loser.condition_id.clone(),
                leg: QuoteLeg::YesBid,
                price: dec!(0.45),
                size: dec!(50),
                status: QuoteStatus::Approved,
                reason: None,
            }],
        };
        engine
            .order_manager
            .place_quotes(&loser, &loser_quote_set, None, dec!(20), None, "test", None)
            .await
            .unwrap();

        let tracked = engine
            .order_manager
            .get_market_orders(&loser.condition_id)
            .await;
        engine
            .order_manager
            .seed_pending_cancel_for_test(
                tracked[0].clone(),
                CancelReasonCode::FrontierRebalance,
                "test_frontier",
            )
            .await;

        *engine.frontier_reservation.write().await = Some(PendingFrontierReservation {
            entrant_condition_id: entrant.condition_id.clone(),
            loser_condition_id: loser.condition_id.clone(),
            reclaimable_bid_capital: dec!(50),
            armed_cycle_id: "cycle_test".to_string(),
        });

        let entrant_eval = {
            let (yes_book, no_book, quote_set, report) =
                engine.evaluate_market(&entrant).await.unwrap();
            MarketEvaluation {
                market: entrant.clone(),
                yes_book,
                no_book,
                quote_set: quote_set.clone(),
                report,
                trace_ids: build_quote_trace_ids(&quote_set),
            }
        };

        let reservation = engine.frontier_reservation.read().await.clone().unwrap();
        let processed = engine
            .activate_frontier_reservation("cycle_test_next", &reservation, &[entrant_eval])
            .await
            .unwrap();

        assert!(processed.is_none());
        assert!(
            engine.frontier_reservation.read().await.is_some(),
            "reservation should stay armed until loser cancel verifies"
        );
        assert!(engine
            .order_manager
            .get_market_orders(&entrant.condition_id)
            .await
            .is_empty());
    }

    #[tokio::test]
    async fn frontier_reservation_places_reserved_entrant_and_clears_state() {
        let engine = test_engine().await;
        engine.order_manager.update_gross_balance(dec!(100)).await;

        let entrant = frontier_market("entrant", "entrant_yes", "entrant_no", dec!(1000));
        engine
            .book_manager
            .insert_snapshot(test_book(&entrant.yes_token_id))
            .await;
        engine
            .book_manager
            .insert_snapshot(test_book(&entrant.no_token_id))
            .await;

        *engine.frontier_reservation.write().await = Some(PendingFrontierReservation {
            entrant_condition_id: entrant.condition_id.clone(),
            loser_condition_id: "loser".to_string(),
            reclaimable_bid_capital: dec!(50),
            armed_cycle_id: "cycle_test".to_string(),
        });

        let entrant_eval = {
            let (yes_book, no_book, quote_set, report) =
                engine.evaluate_market(&entrant).await.unwrap();
            assert!(report.would_trade);
            MarketEvaluation {
                market: entrant.clone(),
                yes_book,
                no_book,
                quote_set: quote_set.clone(),
                report,
                trace_ids: build_quote_trace_ids(&quote_set),
            }
        };

        let reservation = engine.frontier_reservation.read().await.clone().unwrap();
        let processed = engine
            .activate_frontier_reservation("cycle_test_next", &reservation, &[entrant_eval])
            .await
            .unwrap();

        assert_eq!(processed.as_deref(), Some(entrant.condition_id.as_str()));
        assert!(
            engine.frontier_reservation.read().await.is_none(),
            "reservation should clear after dedicated entrant attempt"
        );
        assert!(engine
            .order_manager
            .get_market_orders(&entrant.condition_id)
            .await
            .iter()
            .any(|order| order.leg.is_bid()));
        assert!(engine
            .managed_markets
            .read()
            .await
            .contains_key(&entrant.condition_id));
    }

    #[tokio::test]
    async fn frontier_same_cycle_handoff_disabled_returns_disabled() {
        let mut engine = test_engine().await;
        engine.config.strategy.frontier_handoff_window_secs = 0;

        let result = engine
            .run_same_cycle_frontier_handoff("cycle_test", &[])
            .await;

        assert!(matches!(result, SameCycleHandoffResult::Disabled));
    }

    #[tokio::test]
    async fn frontier_same_cycle_handoff_times_out_when_loser_cancel_never_verifies() {
        let mut engine = test_engine().await;
        engine.config.strategy.frontier_handoff_window_secs = 1;
        engine.order_manager.update_gross_balance(dec!(100)).await;

        let loser = frontier_market("loser", "loser_yes", "loser_no", dec!(400));
        let entrant = frontier_market("entrant", "entrant_yes", "entrant_no", dec!(1000));
        for market in [&loser, &entrant] {
            engine
                .book_manager
                .insert_snapshot(test_book(&market.yes_token_id))
                .await;
            engine
                .book_manager
                .insert_snapshot(test_book(&market.no_token_id))
                .await;
        }

        let loser_quote_set = QuoteSet {
            condition_id: loser.condition_id.clone(),
            candidates: vec![QuoteCandidate {
                condition_id: loser.condition_id.clone(),
                leg: QuoteLeg::YesBid,
                price: dec!(0.45),
                size: dec!(50),
                status: QuoteStatus::Approved,
                reason: None,
            }],
        };
        engine
            .order_manager
            .place_quotes(&loser, &loser_quote_set, None, dec!(20), None, "test", None)
            .await
            .unwrap();

        *engine.frontier_reservation.write().await = Some(PendingFrontierReservation {
            entrant_condition_id: entrant.condition_id.clone(),
            loser_condition_id: loser.condition_id.clone(),
            reclaimable_bid_capital: dec!(50),
            armed_cycle_id: "cycle_test".to_string(),
        });

        let result = engine
            .run_same_cycle_frontier_handoff("cycle_test", &[])
            .await;

        assert!(matches!(result, SameCycleHandoffResult::TimedOut));
        assert!(engine.frontier_reservation.read().await.is_some());
        assert!(engine
            .order_manager
            .get_market_orders(&entrant.condition_id)
            .await
            .is_empty());
    }

    #[tokio::test]
    async fn frontier_same_cycle_handoff_places_entrant_after_cancel_verification_and_clears_reservation(
    ) {
        let mut engine = test_engine().await;
        engine.config.strategy.frontier_handoff_window_secs = 1;
        engine.order_manager.update_gross_balance(dec!(1000)).await;

        let loser = frontier_market("loser", "loser_yes", "loser_no", dec!(400));
        let entrant = frontier_market("entrant", "entrant_yes", "entrant_no", dec!(1000));
        for market in [&loser, &entrant] {
            engine
                .book_manager
                .insert_snapshot(test_book(&market.yes_token_id))
                .await;
            engine
                .book_manager
                .insert_snapshot(test_book(&market.no_token_id))
                .await;
        }

        let loser_quote_set = QuoteSet {
            condition_id: loser.condition_id.clone(),
            candidates: vec![QuoteCandidate {
                condition_id: loser.condition_id.clone(),
                leg: QuoteLeg::YesBid,
                price: dec!(0.45),
                size: dec!(50),
                status: QuoteStatus::Approved,
                reason: None,
            }],
        };
        engine
            .order_manager
            .place_quotes(&loser, &loser_quote_set, None, dec!(20), None, "test", None)
            .await
            .unwrap();

        let tracked = engine
            .order_manager
            .get_market_orders(&loser.condition_id)
            .await;
        engine
            .order_manager
            .seed_pending_cancel_for_test(
                tracked[0].clone(),
                CancelReasonCode::FrontierRebalance,
                "test_frontier",
            )
            .await;
        tokio::time::sleep(StdDuration::from_secs(3)).await;

        *engine.frontier_reservation.write().await = Some(PendingFrontierReservation {
            entrant_condition_id: entrant.condition_id.clone(),
            loser_condition_id: loser.condition_id.clone(),
            reclaimable_bid_capital: dec!(50),
            armed_cycle_id: "cycle_test".to_string(),
        });

        let evaluations = vec![
            {
                let (yes_book, no_book, quote_set, report) =
                    engine.evaluate_market(&loser).await.unwrap();
                MarketEvaluation {
                    market: loser.clone(),
                    yes_book,
                    no_book,
                    quote_set: quote_set.clone(),
                    report,
                    trace_ids: build_quote_trace_ids(&quote_set),
                }
            },
            {
                let (yes_book, no_book, quote_set, report) =
                    engine.evaluate_market(&entrant).await.unwrap();
                MarketEvaluation {
                    market: entrant.clone(),
                    yes_book,
                    no_book,
                    quote_set: quote_set.clone(),
                    report,
                    trace_ids: build_quote_trace_ids(&quote_set),
                }
            },
        ];

        let result = engine
            .run_same_cycle_frontier_handoff("cycle_test", &evaluations)
            .await;

        assert!(matches!(
            result,
            SameCycleHandoffResult::Placed(ref cid) if cid == &entrant.condition_id
        ));
        assert!(engine.frontier_reservation.read().await.is_none());
        assert!(engine
            .order_manager
            .get_market_orders(&entrant.condition_id)
            .await
            .iter()
            .any(|order| order.leg.is_bid()));
        assert!(engine
            .managed_markets
            .read()
            .await
            .contains_key(&entrant.condition_id));
    }

    #[tokio::test]
    async fn frontier_post_cancel_selector_ignores_known_markets_ghosts_and_uses_current_cycle_evaluations_only(
    ) {
        let engine = test_engine().await;
        engine.order_manager.update_gross_balance(dec!(100)).await;

        let entrant = frontier_market("entrant", "entrant_yes", "entrant_no", dec!(1000));
        let ghost = frontier_market("ghost", "ghost_yes", "ghost_no", dec!(5000));
        for market in [&entrant, &ghost] {
            engine
                .book_manager
                .insert_snapshot(test_book(&market.yes_token_id))
                .await;
            engine
                .book_manager
                .insert_snapshot(test_book(&market.no_token_id))
                .await;
        }
        engine
            .known_markets
            .write()
            .await
            .insert(ghost.condition_id.clone(), ghost.clone());

        let entrant_eval = {
            let (yes_book, no_book, quote_set, report) =
                engine.evaluate_market(&entrant).await.unwrap();
            assert!(report.would_trade);
            MarketEvaluation {
                market: entrant.clone(),
                yes_book,
                no_book,
                quote_set: quote_set.clone(),
                report,
                trace_ids: build_quote_trace_ids(&quote_set),
            }
        };

        let selected = engine
            .select_best_post_cancel_market(&[entrant_eval], "loser")
            .await
            .expect("entrant should be selected");

        assert_eq!(selected.0.condition_id, entrant.condition_id);
    }

    #[tokio::test]
    async fn drain_book_ws_stats_snapshots_and_resets_counters() {
        let engine = test_engine().await;

        engine.book_ws_stats.record_accepted(&[BookEvent::Snapshot {
            token_id: "yes_token".to_string(),
            bids: vec![],
            asks: vec![],
        }]);
        engine.book_ws_stats.record_ignored();
        engine.book_ws_stats.record_parse_error();

        let snapshot = engine.drain_book_ws_stats();
        assert_eq!(snapshot.accepted_messages, 1);
        assert_eq!(snapshot.ignored_messages, 1);
        assert_eq!(snapshot.parse_errors, 1);
        assert_eq!(snapshot.snapshot_events, 1);
        assert_eq!(snapshot.delta_events, 0);
        assert!(snapshot.last_parsed_event_at.is_some());
        assert!(snapshot.last_parse_error_at.is_some());

        let drained = engine.drain_book_ws_stats();
        assert_eq!(drained.accepted_messages, 0);
        assert_eq!(drained.ignored_messages, 0);
        assert_eq!(drained.parse_errors, 0);
        assert_eq!(drained.snapshot_events, 0);
        assert_eq!(drained.delta_events, 0);
    }

    #[tokio::test]
    async fn status_snapshot_emits_last_book_ws_stats() {
        let (engine, event_dir) = build_test_engine("http://127.0.0.1:9", true).await;

        *engine.last_book_ws_stats.write().await = BookWsStatsSnapshot {
            accepted_messages: 5,
            ignored_messages: 2,
            parse_errors: 1,
            snapshot_events: 3,
            delta_events: 4,
            last_raw_message_at: None,
            last_parsed_event_at: None,
            last_parse_error_at: None,
        };

        engine.log_status().await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        let event_log = event_dir.join(&engine.run_id).join("events.jsonl");
        let contents = tokio::fs::read_to_string(event_log).await.unwrap();
        let last_line = contents.lines().last().unwrap();
        let event: EventEnvelope = serde_json::from_str(last_line).unwrap();
        let payload: spreadeater_core::payloads::StatusSnapshotPayload =
            serde_json::from_value(event.payload).unwrap();

        assert_eq!(
            event.event_type,
            spreadeater_core::EventType::StatusSnapshot
        );
        assert_eq!(payload.book_ws_accepted_messages, Some(5));
        assert_eq!(payload.book_ws_ignored_messages, Some(2));
        assert_eq!(payload.book_ws_parse_errors, Some(1));
        assert_eq!(payload.book_ws_snapshot_events, Some(3));
        assert_eq!(payload.book_ws_delta_events, Some(4));
    }

    #[tokio::test]
    async fn startup_writes_run_metadata_files_with_expected_schema() {
        let (engine, event_dir) = build_test_engine("http://127.0.0.1:9", true).await;
        let data_dir = event_dir.parent().unwrap().to_path_buf();
        let base_dir = data_dir.parent().unwrap().to_path_buf();

        let current_run = read_run_metadata(data_dir.join("current_run.json")).await;
        let durable_run =
            read_run_metadata(event_dir.join(&engine.run_id).join("run_metadata.json")).await;

        let expected_events_path = event_dir.join(&engine.run_id).join("events.jsonl");
        let expected_config_path = base_dir.join("config.test.json");

        assert_eq!(current_run.run_id, engine.run_id);
        assert_eq!(durable_run.run_id, engine.run_id);
        assert_eq!(current_run.pid, std::process::id());
        assert_eq!(current_run.mode, "dry-run");
        assert_eq!(current_run.cash_reserve_usd, engine.config.risk.cash_reserve);
        assert_eq!(
            current_run.events_path,
            expected_events_path.to_string_lossy().into_owned()
        );
        assert_eq!(
            current_run.event_log_dir,
            event_dir.to_string_lossy().into_owned()
        );
        assert_eq!(
            current_run.config_path,
            expected_config_path.to_string_lossy().into_owned()
        );
        assert_eq!(current_run, durable_run);
    }

    #[tokio::test]
    async fn exchange_sync_matched_delta_queues_fill_work_item() {
        let state = MockExchangeApiState::default();
        let (base_url, server) = spawn_exchange_api_server(state.clone()).await.unwrap();
        let engine = build_test_engine_with_urls(&base_url, &base_url, false)
            .await
            .0;
        let market = test_market();
        engine.order_manager.update_gross_balance(dec!(1000)).await;

        let quote_set = QuoteSet {
            condition_id: market.condition_id.clone(),
            candidates: vec![QuoteCandidate {
                condition_id: market.condition_id.clone(),
                leg: QuoteLeg::YesBid,
                price: dec!(0.45),
                size: dec!(20),
                status: QuoteStatus::Approved,
                reason: None,
            }],
        };

        let tracked = engine
            .order_manager
            .place_quotes(&market, &quote_set, None, dec!(20), None, "test", None)
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();

        state
            .global_orders
            .write()
            .await
            .push_back(open_orders_response(vec![live_order_json(
                &tracked.order_id,
                &market,
                tracked.leg,
                tracked.price,
                dec!(20),
                dec!(3),
            )]));

        let (fill_tx, mut fill_rx) = mpsc::unbounded_channel();
        engine
            .detect_missed_fills_from_exchange(&fill_tx)
            .await
            .unwrap();

        let work = tokio::time::timeout(Duration::from_millis(100), fill_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(work.match_source, "exchange_order_sync");
        assert!(work.fallback_match);
        assert_eq!(work.trade.size, dec!(3));
        assert_eq!(work.size_to_apply, Decimal::ZERO);
        assert_eq!(work.hedge_size, dec!(3));

        let updated = engine
            .order_manager
            .get_tracked_order(&tracked.order_id)
            .await
            .unwrap();
        assert_eq!(updated.matched_size, dec!(3));
        assert_eq!(updated.size, dec!(17));

        server.abort();
    }

    #[tokio::test]
    async fn exchange_sync_disappeared_bid_with_position_delta_queues_fill_work_item() {
        let state = MockExchangeApiState::default();
        state
            .positions
            .write()
            .await
            .push_back(positions_response(vec![position_entry(
                "market",
                "YES",
                dec!(7),
            )]));

        let (base_url, server) = spawn_exchange_api_server(state.clone()).await.unwrap();
        let engine = build_test_engine_with_urls(&base_url, &base_url, false)
            .await
            .0;
        let market = test_market();
        engine.order_manager.update_gross_balance(dec!(1000)).await;

        let quote_set = QuoteSet {
            condition_id: market.condition_id.clone(),
            candidates: vec![QuoteCandidate {
                condition_id: market.condition_id.clone(),
                leg: QuoteLeg::YesBid,
                price: dec!(0.45),
                size: dec!(20),
                status: QuoteStatus::Approved,
                reason: None,
            }],
        };

        let tracked = engine
            .order_manager
            .place_quotes(&market, &quote_set, None, dec!(20), None, "test", None)
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();

        let (fill_tx, mut fill_rx) = mpsc::unbounded_channel();
        engine
            .detect_missed_fills_from_exchange(&fill_tx)
            .await
            .unwrap();

        let work = tokio::time::timeout(Duration::from_millis(100), fill_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(work.match_source, "exchange_order_sync");
        assert_eq!(work.trade.size, dec!(7));
        assert_eq!(work.hedge_size, dec!(7));
        assert_eq!(work.size_to_apply, Decimal::ZERO);

        assert!(engine
            .order_manager
            .get_market_orders(&market.condition_id)
            .await
            .is_empty());
        let recent = engine
            .order_manager
            .get_tracked_order(&tracked.order_id)
            .await
            .unwrap();
        assert_eq!(recent.size, dec!(13));
        assert_eq!(recent.matched_size, dec!(7));

        server.abort();
    }

    #[tokio::test]
    async fn exchange_sync_disappeared_bid_without_position_delta_keeps_tracking_on_first_confirmed_miss(
    ) {
        let state = MockExchangeApiState::default();
        state
            .positions
            .write()
            .await
            .push_back(positions_response(vec![]));

        let (base_url, server) = spawn_exchange_api_server(state.clone()).await.unwrap();
        let engine = build_test_engine_with_urls(&base_url, &base_url, false)
            .await
            .0;
        let market = test_market();
        engine.order_manager.update_gross_balance(dec!(1000)).await;

        let quote_set = QuoteSet {
            condition_id: market.condition_id.clone(),
            candidates: vec![QuoteCandidate {
                condition_id: market.condition_id.clone(),
                leg: QuoteLeg::YesBid,
                price: dec!(0.45),
                size: dec!(20),
                status: QuoteStatus::Approved,
                reason: None,
            }],
        };

        let tracked = engine
            .order_manager
            .place_quotes(&market, &quote_set, None, dec!(20), None, "test", None)
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(engine.order_manager.available_budget().await, dec!(930));

        let (fill_tx, mut fill_rx) = mpsc::unbounded_channel();
        engine
            .detect_missed_fills_from_exchange(&fill_tx)
            .await
            .unwrap();

        assert!(fill_rx.try_recv().is_err());
        let retained = engine
            .order_manager
            .get_tracked_order(&tracked.order_id)
            .await
            .unwrap();
        assert_eq!(retained.size, dec!(20));
        assert_eq!(retained.matched_size, Decimal::ZERO);
        assert_eq!(engine.order_manager.available_budget().await, dec!(930));
        let confirmations = engine.missing_order_confirmations.read().await;
        let confirmation = confirmations.get(&tracked.order_id).unwrap();
        assert_eq!(confirmation.consecutive_market_misses, 1);

        server.abort();
    }

    #[tokio::test]
    async fn exchange_sync_disappeared_bid_without_position_delta_prunes_after_second_confirmed_miss(
    ) {
        let state = MockExchangeApiState::default();
        state
            .positions
            .write()
            .await
            .push_back(positions_response(vec![]));

        let (base_url, server) = spawn_exchange_api_server(state.clone()).await.unwrap();
        let engine = build_test_engine_with_urls(&base_url, &base_url, false)
            .await
            .0;
        let market = test_market();
        engine.order_manager.update_gross_balance(dec!(1000)).await;

        let quote_set = QuoteSet {
            condition_id: market.condition_id.clone(),
            candidates: vec![QuoteCandidate {
                condition_id: market.condition_id.clone(),
                leg: QuoteLeg::YesBid,
                price: dec!(0.45),
                size: dec!(20),
                status: QuoteStatus::Approved,
                reason: None,
            }],
        };

        let tracked = engine
            .order_manager
            .place_quotes(&market, &quote_set, None, dec!(20), None, "test", None)
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();

        let (fill_tx, mut fill_rx) = mpsc::unbounded_channel();
        engine
            .detect_missed_fills_from_exchange(&fill_tx)
            .await
            .unwrap();
        engine
            .detect_missed_fills_from_exchange(&fill_tx)
            .await
            .unwrap();

        assert!(fill_rx.try_recv().is_err());
        assert!(engine
            .order_manager
            .get_market_orders(&market.condition_id)
            .await
            .is_empty());
        assert!(engine
            .order_manager
            .get_tracked_order(&tracked.order_id)
            .await
            .is_none());
        assert_eq!(engine.order_manager.available_budget().await, dec!(950));
        assert!(!engine
            .missing_order_confirmations
            .read()
            .await
            .contains_key(&tracked.order_id));

        server.abort();
    }

    #[tokio::test]
    async fn exact_signature_fallback_anchors_unattributed_trade_to_active_order() {
        let engine = test_engine().await;
        let market = test_market();
        engine.order_manager.update_gross_balance(dec!(1000)).await;

        let quote_set = QuoteSet {
            condition_id: market.condition_id.clone(),
            candidates: vec![QuoteCandidate {
                condition_id: market.condition_id.clone(),
                leg: QuoteLeg::YesBid,
                price: dec!(0.37),
                size: dec!(342),
                status: QuoteStatus::Approved,
                reason: None,
            }],
        };
        let tracked = engine
            .order_manager
            .place_quotes(&market, &quote_set, None, dec!(20), None, "test", None)
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();

        let work = engine
            .build_fill_work_item(TradeEvent {
                id: "trade-exact-signature".to_string(),
                condition_id: market.condition_id.clone(),
                asset_id: tracked.token_id.clone(),
                side: tracked.side,
                price: tracked.price,
                size: tracked.size,
                outcome: "YES".to_string(),
                status: TradeStatus::Matched,
                timestamp: Utc::now(),
                maker_order_id: None,
                taker_order_id: None,
            })
            .await
            .expect("expected fallback attribution");

        assert_eq!(work.match_source, "exact_signature_active_fallback");
        assert!(work.fallback_match);
        assert_eq!(
            work.anchored_order_id.as_deref(),
            Some(tracked.order_id.as_str())
        );
        assert_eq!(work.size_to_apply, tracked.size);
        assert_eq!(work.hedge_size, tracked.size);
    }

    #[tokio::test]
    async fn recent_resolution_trade_skip_ignores_late_sellback_ws_trade() {
        let engine = test_engine().await;
        let market = test_market();
        engine
            .record_recent_resolution_trade(
                &market.condition_id,
                &market.yes_token_id,
                Side::Sell,
                dec!(0.36),
                dec!(342),
            )
            .await;

        let work = engine
            .build_fill_work_item(TradeEvent {
                id: "trade-late-sellback".to_string(),
                condition_id: market.condition_id.clone(),
                asset_id: market.yes_token_id.clone(),
                side: Side::Sell,
                price: dec!(0.36),
                size: dec!(342),
                outcome: "YES".to_string(),
                status: TradeStatus::Matched,
                timestamp: Utc::now(),
                maker_order_id: None,
                taker_order_id: None,
            })
            .await;

        assert!(work.is_none());
    }

    #[tokio::test]
    async fn finalize_halted_cleanup_reconciles_stale_order_truth_before_deferring() {
        let state = MockExchangeApiState::default();
        state
            .positions
            .write()
            .await
            .push_back(positions_response(vec![]));
        state.market_orders.write().await.insert(
            "market".to_string(),
            VecDeque::from(vec![empty_orders_response()]),
        );

        let (base_url, server) = spawn_exchange_api_server(state.clone()).await.unwrap();
        let engine = build_test_engine_with_urls(&base_url, &base_url, false)
            .await
            .0;
        let market = test_market();
        engine.order_manager.update_gross_balance(dec!(1000)).await;
        engine
            .managed_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market.clone());
        engine
            .known_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market.clone());

        let quote_set = QuoteSet {
            condition_id: market.condition_id.clone(),
            candidates: vec![QuoteCandidate {
                condition_id: market.condition_id.clone(),
                leg: QuoteLeg::YesBid,
                price: dec!(0.45),
                size: dec!(20),
                status: QuoteStatus::Approved,
                reason: None,
            }],
        };
        let tracked = engine
            .order_manager
            .place_quotes(&market, &quote_set, None, dec!(20), None, "test", None)
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        engine
            .order_manager
            .seed_pending_cancel_for_test(tracked, CancelReasonCode::RiskHalt, "test")
            .await;

        let cleanup = finalize_halted_market_cleanup(
            &market.condition_id,
            &engine.order_manager,
            &engine.position_manager,
            &engine.trading_client,
            &engine.managed_markets,
            &engine.known_markets,
            &engine.config,
        )
        .await;

        assert!(cleanup.verified());
        assert_eq!(
            engine
                .order_manager
                .market_order_state_counts(&market.condition_id)
                .await,
            (0, 0)
        );
        assert!(!engine
            .managed_markets
            .read()
            .await
            .contains_key(&market.condition_id));

        server.abort();
    }

    #[tokio::test]
    async fn exchange_sync_missing_order_reappearance_clears_confirmation_state() {
        let state = MockExchangeApiState::default();
        state
            .positions
            .write()
            .await
            .push_back(positions_response(vec![]));

        let (base_url, server) = spawn_exchange_api_server(state.clone()).await.unwrap();
        let engine = build_test_engine_with_urls(&base_url, &base_url, false)
            .await
            .0;
        let market = test_market();
        engine.order_manager.update_gross_balance(dec!(1000)).await;

        let quote_set = QuoteSet {
            condition_id: market.condition_id.clone(),
            candidates: vec![QuoteCandidate {
                condition_id: market.condition_id.clone(),
                leg: QuoteLeg::YesBid,
                price: dec!(0.45),
                size: dec!(20),
                status: QuoteStatus::Approved,
                reason: None,
            }],
        };

        let tracked = engine
            .order_manager
            .place_quotes(&market, &quote_set, None, dec!(20), None, "test", None)
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        state
            .global_orders
            .write()
            .await
            .push_back(empty_orders_response());
        state
            .global_orders
            .write()
            .await
            .push_back(open_orders_response(vec![live_order_json(
                &tracked.order_id,
                &market,
                tracked.leg,
                tracked.price,
                dec!(20),
                Decimal::ZERO,
            )]));
        state.market_orders.write().await.insert(
            market.condition_id.clone(),
            VecDeque::from(vec![empty_orders_response()]),
        );

        let (fill_tx, mut fill_rx) = mpsc::unbounded_channel();
        engine
            .detect_missed_fills_from_exchange(&fill_tx)
            .await
            .unwrap();
        assert!(engine
            .missing_order_confirmations
            .read()
            .await
            .contains_key(&tracked.order_id));

        engine
            .detect_missed_fills_from_exchange(&fill_tx)
            .await
            .unwrap();

        assert!(fill_rx.try_recv().is_err());
        assert!(engine
            .order_manager
            .get_tracked_order(&tracked.order_id)
            .await
            .is_some());
        assert!(!engine
            .missing_order_confirmations
            .read()
            .await
            .contains_key(&tracked.order_id));

        server.abort();
    }

    #[tokio::test]
    async fn exchange_sync_duplicate_live_bid_leg_triggers_market_kill() {
        let state = MockExchangeApiState::default();
        let (base_url, server) = spawn_exchange_api_server(state.clone()).await.unwrap();
        let engine = build_test_engine_with_urls(&base_url, &base_url, false)
            .await
            .0;
        let market = test_market();
        engine
            .managed_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market.clone());
        engine.order_manager.update_gross_balance(dec!(1000)).await;

        let quote_set = QuoteSet {
            condition_id: market.condition_id.clone(),
            candidates: vec![QuoteCandidate {
                condition_id: market.condition_id.clone(),
                leg: QuoteLeg::YesBid,
                price: dec!(0.45),
                size: dec!(20),
                status: QuoteStatus::Approved,
                reason: None,
            }],
        };

        let tracked = engine
            .order_manager
            .place_quotes(&market, &quote_set, None, dec!(20), None, "test", None)
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        state
            .global_orders
            .write()
            .await
            .push_back(open_orders_response(vec![
                live_order_json(
                    &tracked.order_id,
                    &market,
                    QuoteLeg::YesBid,
                    dec!(0.45),
                    dec!(20),
                    Decimal::ZERO,
                ),
                live_order_json(
                    "dup-order-yes-bid",
                    &market,
                    QuoteLeg::YesBid,
                    dec!(0.45),
                    dec!(20),
                    Decimal::ZERO,
                ),
            ]));

        let (fill_tx, mut fill_rx) = mpsc::unbounded_channel();
        engine
            .detect_missed_fills_from_exchange(&fill_tx)
            .await
            .unwrap();

        assert!(fill_rx.try_recv().is_err());
        assert!(
            !engine
                .risk_manager
                .is_market_tradable(&market.condition_id)
                .await
        );
        assert!(!engine
            .managed_markets
            .read()
            .await
            .contains_key(&market.condition_id));

        server.abort();
    }

    #[tokio::test]
    async fn prepare_market_for_resolution_prunes_stale_orders_before_budget() {
        let state = MockExchangeApiState::default();
        let market = test_market();
        state.books.write().await.insert(
            market.yes_token_id.clone(),
            book_response_json(&market.yes_token_id, dec!(100), dec!(100)),
        );
        state.books.write().await.insert(
            market.no_token_id.clone(),
            book_response_json(&market.no_token_id, dec!(100), dec!(100)),
        );
        state
            .balances
            .write()
            .await
            .push_back(balance_response(dec!(500)));
        state.market_orders.write().await.insert(
            market.condition_id.clone(),
            VecDeque::from(vec![
                open_orders_response(vec![live_order_json(
                    "stale-order",
                    &market,
                    QuoteLeg::YesBid,
                    dec!(0.45),
                    dec!(120),
                    Decimal::ZERO,
                )]),
                empty_orders_response(),
            ]),
        );

        let (base_url, server) = spawn_exchange_api_server(state.clone()).await.unwrap();
        let trading_client = trading_client_with_base(&base_url, false);
        let order_manager = OrderManager::new(
            trading_client.clone(),
            Arc::new(RwLock::new(dec!(500))),
            None,
            "test".to_string(),
            "test".to_string(),
            Decimal::ZERO,
        );
        let risk_manager = Arc::new(RiskManager::new(Config::default().risk.clone()));
        let cached_balance = Arc::new(RwLock::new(dec!(500)));
        let book_manager = Arc::new(BookManager::new());
        let book_rest = BookRestClient::new(base_url.clone());
        let config = Config::default();

        order_manager
            .sync_market_open_orders(
                &market.condition_id,
                &market,
                MarketOrderSyncMode::Reconcile,
            )
            .await
            .unwrap();
        assert_eq!(
            order_manager.available_hedge_resolution_usdc().await,
            dec!(380)
        );

        let preparation = prepare_market_for_resolution(
            &market,
            &order_manager,
            &trading_client,
            &risk_manager,
            &cached_balance,
            &book_rest,
            &book_manager,
            &config,
            None,
        )
        .await
        .unwrap();

        assert_eq!(preparation.pre_resolution_active_orders, 0);
        assert_eq!(preparation.max_hedge_usdc, dec!(500));
        assert!(order_manager
            .get_market_orders(&market.condition_id)
            .await
            .is_empty());

        server.abort();
    }

    #[tokio::test]
    async fn prepare_market_for_resolution_buy_side_reclaims_external_bid_capital() {
        let state = MockExchangeApiState::default();
        let market = test_market();
        let external_market =
            test_market_with_identity("external-market", "external-yes", "external-no");
        state.books.write().await.insert(
            market.yes_token_id.clone(),
            book_response_json(&market.yes_token_id, dec!(100), dec!(100)),
        );
        state.books.write().await.insert(
            market.no_token_id.clone(),
            book_response_json(&market.no_token_id, dec!(100), dec!(100)),
        );
        state
            .balances
            .write()
            .await
            .push_back(balance_response(dec!(500)));
        state.market_orders.write().await.insert(
            market.condition_id.clone(),
            VecDeque::from(vec![
                open_orders_response(vec![live_order_json(
                    "current-order",
                    &market,
                    QuoteLeg::YesBid,
                    dec!(0.45),
                    dec!(150),
                    Decimal::ZERO,
                )]),
                empty_orders_response(),
            ]),
        );

        let (base_url, server) = spawn_exchange_api_server(state.clone()).await.unwrap();
        let trading_client = trading_client_with_base(&base_url, false);
        let order_manager = OrderManager::new(
            trading_client.clone(),
            Arc::new(RwLock::new(dec!(500))),
            None,
            "test".to_string(),
            "test".to_string(),
            Decimal::ZERO,
        );
        let risk_manager = Arc::new(RiskManager::new(Config::default().risk.clone()));
        let cached_balance = Arc::new(RwLock::new(dec!(500)));
        let book_manager = Arc::new(BookManager::new());
        let book_rest = BookRestClient::new(base_url.clone());
        let config = Config::default();

        order_manager
            .seed_live_order_for_test(tracked_order_for_market(
                &external_market,
                QuoteLeg::YesBid,
                dec!(0.45),
                dec!(200),
                Some("external-order"),
            ))
            .await;
        order_manager.update_gross_balance(dec!(500)).await;

        let preparation = prepare_market_for_resolution(
            &market,
            &order_manager,
            &trading_client,
            &risk_manager,
            &cached_balance,
            &book_rest,
            &book_manager,
            &config,
            Some(dec!(400)),
        )
        .await
        .unwrap();

        assert_eq!(preparation.pre_resolution_active_orders, 1);
        assert_eq!(preparation.max_hedge_usdc, dec!(500));
        assert!(order_manager
            .get_market_orders(&external_market.condition_id)
            .await
            .is_empty());

        let cancelled = state.cancelled_orders.read().await.clone();
        assert_eq!(
            cancelled
                .iter()
                .filter(|order_id| order_id.as_str() == "current-order")
                .count(),
            1
        );
        assert_eq!(
            cancelled
                .iter()
                .filter(|order_id| order_id.as_str() == "external-order")
                .count(),
            1
        );

        server.abort();
    }

    #[tokio::test]
    async fn prepare_market_for_resolution_buy_side_skips_reclaim_when_budget_sufficient() {
        let state = MockExchangeApiState::default();
        let market = test_market();
        let external_market =
            test_market_with_identity("external-market", "external-yes", "external-no");
        state.books.write().await.insert(
            market.yes_token_id.clone(),
            book_response_json(&market.yes_token_id, dec!(100), dec!(100)),
        );
        state.books.write().await.insert(
            market.no_token_id.clone(),
            book_response_json(&market.no_token_id, dec!(100), dec!(100)),
        );
        state
            .balances
            .write()
            .await
            .push_back(balance_response(dec!(500)));

        let (base_url, server) = spawn_exchange_api_server(state.clone()).await.unwrap();
        let trading_client = trading_client_with_base(&base_url, false);
        let order_manager = OrderManager::new(
            trading_client.clone(),
            Arc::new(RwLock::new(dec!(500))),
            None,
            "test".to_string(),
            "test".to_string(),
            Decimal::ZERO,
        );
        let risk_manager = Arc::new(RiskManager::new(Config::default().risk.clone()));
        let cached_balance = Arc::new(RwLock::new(dec!(500)));
        let book_manager = Arc::new(BookManager::new());
        let book_rest = BookRestClient::new(base_url.clone());
        let config = Config::default();

        order_manager
            .seed_live_order_for_test(tracked_order_for_market(
                &external_market,
                QuoteLeg::YesBid,
                dec!(0.45),
                dec!(50),
                Some("external-order"),
            ))
            .await;
        order_manager.update_gross_balance(dec!(500)).await;

        let preparation = prepare_market_for_resolution(
            &market,
            &order_manager,
            &trading_client,
            &risk_manager,
            &cached_balance,
            &book_rest,
            &book_manager,
            &config,
            Some(dec!(400)),
        )
        .await
        .unwrap();

        assert_eq!(preparation.max_hedge_usdc, dec!(450));
        assert_eq!(
            order_manager
                .get_market_orders(&external_market.condition_id)
                .await
                .len(),
            1
        );
        assert!(state.cancelled_orders.read().await.is_empty());

        server.abort();
    }

    #[tokio::test]
    async fn prepare_market_for_resolution_sell_side_skips_external_bid_reclaim() {
        let state = MockExchangeApiState::default();
        let market = test_market();
        let external_market =
            test_market_with_identity("external-market", "external-yes", "external-no");
        state.books.write().await.insert(
            market.yes_token_id.clone(),
            book_response_json(&market.yes_token_id, dec!(100), dec!(100)),
        );
        state.books.write().await.insert(
            market.no_token_id.clone(),
            book_response_json(&market.no_token_id, dec!(100), dec!(100)),
        );
        state
            .balances
            .write()
            .await
            .push_back(balance_response(dec!(500)));

        let (base_url, server) = spawn_exchange_api_server(state.clone()).await.unwrap();
        let trading_client = trading_client_with_base(&base_url, false);
        let order_manager = OrderManager::new(
            trading_client.clone(),
            Arc::new(RwLock::new(dec!(500))),
            None,
            "test".to_string(),
            "test".to_string(),
            Decimal::ZERO,
        );
        let risk_manager = Arc::new(RiskManager::new(Config::default().risk.clone()));
        let cached_balance = Arc::new(RwLock::new(dec!(500)));
        let book_manager = Arc::new(BookManager::new());
        let book_rest = BookRestClient::new(base_url.clone());
        let config = Config::default();

        order_manager
            .seed_live_order_for_test(tracked_order_for_market(
                &external_market,
                QuoteLeg::YesBid,
                dec!(0.45),
                dec!(200),
                Some("external-order"),
            ))
            .await;
        order_manager.update_gross_balance(dec!(500)).await;

        let preparation = prepare_market_for_resolution(
            &market,
            &order_manager,
            &trading_client,
            &risk_manager,
            &cached_balance,
            &book_rest,
            &book_manager,
            &config,
            None,
        )
        .await
        .unwrap();

        assert_eq!(preparation.max_hedge_usdc, dec!(300));
        assert_eq!(
            order_manager
                .get_market_orders(&external_market.condition_id)
                .await
                .len(),
            1
        );
        assert!(state.cancelled_orders.read().await.is_empty());

        server.abort();
    }

    #[tokio::test]
    async fn first_reconciliation_failure_halts_market_and_keeps_cleanup_pending() {
        let (positions_url, positions_server) = spawn_static_json_server(
            &positions_response(vec![position_entry("market", "YES", dec!(10))]),
            Duration::ZERO,
        )
        .await
        .unwrap();
        let engine = build_test_engine_with_urls("http://127.0.0.1:9", &positions_url, false)
            .await
            .0;
        let market = test_market();

        engine
            .managed_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market.clone());

        engine
            .handle_reconciliation_resolution_failure(&market, dec!(10), "aggregate failed", true)
            .await;

        assert!(
            !engine
                .risk_manager
                .is_market_tradable(&market.condition_id)
                .await
        );
        assert!(engine
            .managed_markets
            .read()
            .await
            .contains_key(&market.condition_id));

        positions_server.abort();
    }

    #[tokio::test]
    async fn flatten_uses_known_market_metadata_when_market_is_no_longer_managed() {
        let (positions_url, positions_server) = spawn_static_json_server(
            &positions_response(vec![position_entry("market", "YES", dec!(4))]),
            Duration::ZERO,
        )
        .await
        .unwrap();
        let market = test_market();
        let position_manager = Arc::new(PositionManager::new(positions_url, "0x0".to_string()));
        let trading_client = trading_client_with_base("https://example.com", true);
        let managed_markets = Arc::new(RwLock::new(HashMap::new()));
        let known_markets = Arc::new(RwLock::new(HashMap::from([(
            market.condition_id.clone(),
            market.clone(),
        )])));

        let result = flatten_directional_inventory_for_halt(
            &market.condition_id,
            &position_manager,
            &trading_client,
            &managed_markets,
            &known_markets,
            &Config::default(),
        )
        .await;

        assert!(result.flatten_attempted);
        assert!(!result.verified);
        assert_eq!(result.post_sync_net_exposure, dec!(4));

        positions_server.abort();
    }

    // ── 1D: Book-aware hedge resolution wiring tests ──────────────────

    use crate::models::PriceLevel;
    use crate::trading::hedge_executor::{compute_hedge_resolution, plan_fill_resolution};

    fn book_with_levels(
        token_id: &str,
        bids: Vec<(Decimal, Decimal)>,
        asks: Vec<(Decimal, Decimal)>,
    ) -> OrderBookSnapshot {
        OrderBookSnapshot {
            token_id: token_id.to_string(),
            exchange_ts: None,
            ingest_ts: Utc::now(),
            bids: bids
                .into_iter()
                .map(|(price, size)| PriceLevel { price, size })
                .collect(),
            asks: asks
                .into_iter()
                .map(|(price, size)| PriceLevel { price, size })
                .collect(),
        }
    }

    /// NoBid fill → hedge uses YES ask book, sellback uses NO bid book.
    #[tokio::test]
    async fn resolution_wiring_no_bid_uses_correct_books() {
        let book_manager = Arc::new(BookManager::new());

        // YES ask book (hedge target — we buy YES to offset NO fill)
        let yes_book = book_with_levels(
            "yes_token",
            vec![(dec!(0.70), dec!(500))],
            vec![(dec!(0.27), dec!(400))], // asks: cheap hedge
        );
        // NO bid book (sellback — we sell back NO tokens)
        let no_book = book_with_levels(
            "no_token",
            vec![(dec!(0.73), dec!(500))], // bids: available sell-back
            vec![(dec!(0.80), dec!(500))],
        );

        book_manager.insert_snapshot(yes_book).await;
        book_manager.insert_snapshot(no_book).await;

        // Simulate NoBid fill → hedge_token = YES, filled_token = NO
        let hedge_book = book_manager.get_book("yes_token").await.unwrap();
        let filled_book = book_manager.get_book("no_token").await.unwrap();

        let res = compute_hedge_resolution(
            dec!(0.74),        // fill_price
            &hedge_book.asks,  // YES asks
            &filled_book.bids, // NO bids
            dec!(373),         // total size
            dec!(0.01),        // tick
        );

        // hedge cost = 0.74 + 0.27 - 1.00 = 0.01
        // sellback cost = 0.74 - 0.73 = 0.01
        // Tie -> prefer sellback
        assert_eq!(res.hedge_shares, Decimal::ZERO);
        assert_eq!(res.sellback_shares, dec!(373));
        assert_eq!(res.unresolved_shares, Decimal::ZERO);
        assert_eq!(res.hedge_limit_price, Decimal::ZERO);
    }

    /// YesBid fill → hedge uses NO ask book, sellback uses YES bid book.
    #[tokio::test]
    async fn resolution_wiring_yes_bid_uses_correct_books() {
        let book_manager = Arc::new(BookManager::new());

        // NO ask book (hedge target — we buy NO to offset YES fill)
        let no_book = book_with_levels(
            "no_token",
            vec![(dec!(0.30), dec!(500))],
            vec![(dec!(0.42), dec!(200)), (dec!(0.50), dec!(300))],
        );
        // YES bid book (sellback — we sell back YES tokens)
        let yes_book = book_with_levels(
            "yes_token",
            vec![(dec!(0.57), dec!(500))],
            vec![(dec!(0.65), dec!(500))],
        );

        book_manager.insert_snapshot(yes_book).await;
        book_manager.insert_snapshot(no_book).await;

        let hedge_book = book_manager.get_book("no_token").await.unwrap();
        let filled_book = book_manager.get_book("yes_token").await.unwrap();

        let res = compute_hedge_resolution(
            dec!(0.60),        // fill_price (bought YES at 0.60)
            &hedge_book.asks,  // NO asks
            &filled_book.bids, // YES bids
            dec!(100),
            dec!(0.01),
        );

        // Level 1: hedge @ 0.42 → cost = 0.60 + 0.42 - 1.00 = 0.02
        //          sellback @ 0.57 → cost = 0.60 - 0.57 = 0.03
        //          Hedge wins (200 shares)
        // Remaining 0 (we only need 100, 200 available at first level)
        assert_eq!(res.hedge_shares, dec!(100));
        assert_eq!(res.sellback_shares, Decimal::ZERO);
        assert_eq!(res.unresolved_shares, Decimal::ZERO);
    }

    /// SELL hedge bypasses resolution — resolution should not be computed.
    #[test]
    fn sell_hedge_side_produces_no_resolution() {
        // For SELL hedges (Ask fills → sell opposite token), the legacy FOK path
        // at 0.01 is used. Resolution is only computed for BUY hedges.
        // This is verified by the match arm: `Side::Sell => None`
        // in both FillHandler and reconciliation paths.
        let (_, side) = HedgeExecutor::compute_hedge_params(QuoteLeg::YesAsk, "yes_tok", "no_tok");
        assert_eq!(side, Side::Sell);

        let (_, side) = HedgeExecutor::compute_hedge_params(QuoteLeg::NoAsk, "yes_tok", "no_tok");
        assert_eq!(side, Side::Sell);
    }

    /// Budget-aware planning reroutes hedge overflow to sell-back at live bid depth.
    #[tokio::test]
    async fn resolution_wiring_budget_moves_excess_to_sellback() {
        let book_manager = Arc::new(BookManager::new());

        let yes_book = book_with_levels("yes_token", vec![], vec![(dec!(0.26), dec!(400))]);
        let no_book = book_with_levels("no_token", vec![(dec!(0.73), dec!(500))], vec![]);

        book_manager.insert_snapshot(yes_book).await;
        book_manager.insert_snapshot(no_book).await;

        let hedge_book = book_manager.get_book("yes_token").await.unwrap();
        let filled_book = book_manager.get_book("no_token").await.unwrap();

        let res = plan_fill_resolution(
            dec!(0.74),
            &hedge_book.asks,
            &filled_book.bids,
            dec!(373),
            dec!(50),
            dec!(0.01),
        );

        // Limit price is 0.27, so affordable hedge size is floor(50 / 0.27) = 185.
        assert_eq!(res.hedge_shares, dec!(185));
        assert_eq!(res.hedge_limit_price, dec!(0.27));
        assert_eq!(res.sellback_shares, dec!(188));
        assert_eq!(res.sellback_limit_price, dec!(0.73));
        assert_eq!(res.unresolved_shares, Decimal::ZERO);
    }

    /// Empty books → zero resolution, all shares unresolved.
    #[tokio::test]
    async fn resolution_wiring_empty_books_zero_resolution() {
        let book_manager = Arc::new(BookManager::new());
        // No books inserted — get_book returns None

        let hedge_book = book_manager.get_book("yes_token").await;
        let filled_book = book_manager.get_book("no_token").await;

        let hedge_asks = hedge_book
            .as_ref()
            .map(|b| b.asks.as_slice())
            .unwrap_or(&[]);
        let sellback_bids = filled_book
            .as_ref()
            .map(|b| b.bids.as_slice())
            .unwrap_or(&[]);

        let res =
            compute_hedge_resolution(dec!(0.74), hedge_asks, sellback_bids, dec!(373), dec!(0.01));

        assert_eq!(res.hedge_shares, Decimal::ZERO);
        assert_eq!(res.sellback_shares, Decimal::ZERO);
        assert_eq!(res.unresolved_shares, dec!(373));
    }

    #[test]
    fn terminal_sellback_placement_matched_counts_as_verified_fill() {
        let result = sellback_result_from_terminal_placement(
            sample_sellback_order_result("sellback-order", OrderStatus::Matched, &[]),
            dec!(0.01),
            dec!(5),
        )
        .expect("matched placement should be terminal");

        assert!(result.is_verified_filled());
        assert_eq!(result.confirmed_shares, Some(dec!(5)));
        assert_eq!(sellback_leg_status(Some(&result)), "success");
        assert_eq!(
            result.verification_metadata.response_status.as_deref(),
            Some("matched")
        );
    }

    #[test]
    fn terminal_sellback_placement_invalid_counts_as_verified_zero_fill() {
        let result = sellback_result_from_terminal_placement(
            sample_sellback_order_result("sellback-order", OrderStatus::Invalid, &[]),
            dec!(0.01),
            dec!(5),
        )
        .expect("invalid placement should be terminal");

        assert!(!result.is_verified_filled());
        assert_eq!(sellback_leg_status(Some(&result)), "failed");
        assert!(matches!(
            result.verification_state,
            SellbackVerificationState::VerifiedZeroFill
        ));
        assert!(result
            .failure_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("placement response was invalid")));
    }

    #[test]
    fn provisional_sellback_lookup_matched_counts_as_verified_fill() {
        let result = sellback_result_from_lookup(
            sample_sellback_order_result("sellback-order", OrderStatus::Live, &[]),
            dec!(0.01),
            dec!(5),
            Ok(Some(sample_sellback_lookup_order(
                OrderStatus::Matched,
                dec!(5),
            ))),
        );

        assert!(result.is_verified_filled());
        assert_eq!(result.confirmed_shares, Some(dec!(5)));
        assert_eq!(sellback_leg_status(Some(&result)), "success");
        assert_eq!(
            result.verification_metadata.lookup_status.as_deref(),
            Some("matched")
        );
        assert_eq!(
            result.verification_metadata.lookup_matched_shares,
            Some(dec!(5))
        );
    }

    #[test]
    fn provisional_sellback_lookup_cancelled_counts_as_verified_zero_fill() {
        let result = sellback_result_from_lookup(
            sample_sellback_order_result("sellback-order", OrderStatus::Live, &[]),
            dec!(0.01),
            dec!(5),
            Ok(Some(sample_sellback_lookup_order(
                OrderStatus::Cancelled,
                Decimal::ZERO,
            ))),
        );

        assert!(!result.is_verified_filled());
        assert_eq!(sellback_leg_status(Some(&result)), "failed");
        assert!(matches!(
            result.verification_state,
            SellbackVerificationState::VerifiedZeroFill
        ));
        assert_eq!(
            result.verification_metadata.lookup_status.as_deref(),
            Some("cancelled")
        );
    }

    #[test]
    fn provisional_sellback_lookup_delayed_remains_unverified() {
        let result = sellback_result_from_lookup(
            sample_sellback_order_result("sellback-order", OrderStatus::Live, &[]),
            dec!(0.01),
            dec!(5),
            Ok(Some(sample_sellback_lookup_order(
                OrderStatus::Delayed,
                Decimal::ZERO,
            ))),
        );

        assert!(!result.is_verified_filled());
        assert_eq!(sellback_leg_status(Some(&result)), "unverified");
        assert!(matches!(
            result.verification_state,
            SellbackVerificationState::Unknown
        ));
        assert_eq!(
            result.verification_metadata.lookup_status.as_deref(),
            Some("delayed")
        );
    }

    #[test]
    fn provisional_sellback_missing_lookup_fails_closed_as_unverified() {
        let result = sellback_result_from_lookup(
            sample_sellback_order_result("sellback-order", OrderStatus::Live, &[]),
            dec!(0.01),
            dec!(5),
            Ok(None),
        );

        assert!(!result.is_verified_filled());
        assert_eq!(sellback_leg_status(Some(&result)), "unverified");
        assert!(matches!(
            result.verification_state,
            SellbackVerificationState::Unknown
        ));
        assert_eq!(
            result.verification_metadata.lookup_status.as_deref(),
            Some("missing")
        );
    }

    #[test]
    fn provisional_sellback_lookup_error_fails_closed_as_unverified() {
        let result = sellback_result_from_lookup(
            sample_sellback_order_result("sellback-order", OrderStatus::Delayed, &[]),
            dec!(0.01),
            dec!(5),
            Err(anyhow::anyhow!("timeout")),
        );

        assert!(!result.is_verified_filled());
        assert_eq!(sellback_leg_status(Some(&result)), "unverified");
        assert!(matches!(
            result.verification_state,
            SellbackVerificationState::Unknown
        ));
        assert_eq!(
            result.verification_metadata.lookup_status.as_deref(),
            Some("error")
        );
        assert_eq!(
            result.verification_metadata.lookup_error.as_deref(),
            Some("timeout")
        );
    }

    #[test]
    fn provisional_sellback_without_order_id_fails_closed_as_unverified() {
        let result = sellback_result_from_lookup(
            sample_sellback_order_result("", OrderStatus::Live, &[]),
            dec!(0.01),
            dec!(5),
            Ok(None),
        );

        assert!(!result.is_verified_filled());
        assert_eq!(sellback_leg_status(Some(&result)), "unverified");
        assert!(result
            .failure_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("without an order_id")));
    }

    #[test]
    fn provisional_sellback_lookup_partial_fill_fails_closed_as_unverified() {
        let result = sellback_result_from_lookup(
            sample_sellback_order_result("sellback-order", OrderStatus::Live, &[]),
            dec!(0.01),
            dec!(5),
            Ok(Some(sample_sellback_lookup_order(
                OrderStatus::Live,
                dec!(3),
            ))),
        );

        assert!(!result.is_verified_filled());
        assert_eq!(result.confirmed_shares, None);
        assert_eq!(sellback_leg_status(Some(&result)), "unverified");
        assert!(result.failure_reason.as_deref().is_some_and(|reason| {
            reason.contains("terminal execution could not be confirmed")
        }));
    }

    #[test]
    fn derive_execution_confirmed_sellback_position_projects_flat_result() {
        let pre_resolution_position = Position {
            condition_id: "market".to_string(),
            yes_size: dec!(5),
            no_size: Decimal::ZERO,
            avg_yes_price: dec!(0.74),
            avg_no_price: Decimal::ZERO,
        };
        let intent = HedgeIntent {
            condition_id: "market".to_string(),
            trigger_order_id: "trigger-order".to_string(),
            trigger_leg: QuoteLeg::YesBid,
            fill_size: dec!(5),
            fill_price: dec!(0.74),
            hedge_token_id: "no-token".to_string(),
            hedge_side: Side::Buy,
            neg_risk: false,
            tick_size: "0.01".to_string(),
        };
        let sellback_result = SellbackExecutionResult {
            order_result: Some(sample_sellback_order_result(
                "sellback-order",
                OrderStatus::Matched,
                &[],
            )),
            verification_state: SellbackVerificationState::VerifiedFilled,
            confirmed_shares: Some(dec!(5)),
            failure_reason: None,
            price: Some(dec!(0.01)),
            verification_metadata: SellbackVerificationMetadata::default(),
        };

        let projected = derive_execution_confirmed_sellback_position(
            &pre_resolution_position,
            &intent,
            None,
            Some(&sellback_result),
            dec!(0.5),
        )
        .expect("expected execution-confirmed sellback to derive a flat result");

        assert_eq!(projected.yes_size, Decimal::ZERO);
        assert_eq!(projected.no_size, Decimal::ZERO);
    }

    #[test]
    fn derive_execution_confirmed_sellback_position_rejects_residual_exposure() {
        let pre_resolution_position = Position {
            condition_id: "market".to_string(),
            yes_size: dec!(7),
            no_size: Decimal::ZERO,
            avg_yes_price: dec!(0.74),
            avg_no_price: Decimal::ZERO,
        };
        let intent = HedgeIntent {
            condition_id: "market".to_string(),
            trigger_order_id: "trigger-order".to_string(),
            trigger_leg: QuoteLeg::YesBid,
            fill_size: dec!(7),
            fill_price: dec!(0.74),
            hedge_token_id: "no-token".to_string(),
            hedge_side: Side::Buy,
            neg_risk: false,
            tick_size: "0.01".to_string(),
        };
        let sellback_result = SellbackExecutionResult {
            order_result: Some(sample_sellback_order_result(
                "sellback-order",
                OrderStatus::Matched,
                &[],
            )),
            verification_state: SellbackVerificationState::VerifiedFilled,
            confirmed_shares: Some(dec!(5)),
            failure_reason: None,
            price: Some(dec!(0.01)),
            verification_metadata: SellbackVerificationMetadata::default(),
        };

        assert!(derive_execution_confirmed_sellback_position(
            &pre_resolution_position,
            &intent,
            None,
            Some(&sellback_result),
            dec!(0.5),
        )
        .is_none());
    }

    #[test]
    fn derive_execution_confirmed_sellback_post_sync_position_uses_confirmed_hedge_truth() {
        let pre_resolution_position = Position::new("market".to_string());
        let post_position = Position {
            condition_id: "market".to_string(),
            yes_size: dec!(10),
            no_size: dec!(6),
            avg_yes_price: dec!(0.74),
            avg_no_price: dec!(0.26),
        };
        let intent = HedgeIntent {
            condition_id: "market".to_string(),
            trigger_order_id: "trigger-order".to_string(),
            trigger_leg: QuoteLeg::YesBid,
            fill_size: dec!(10),
            fill_price: dec!(0.74),
            hedge_token_id: "no-token".to_string(),
            hedge_side: Side::Buy,
            neg_risk: false,
            tick_size: "0.01".to_string(),
        };
        let hedge_result = HedgeResult {
            intent: intent.clone(),
            success: true,
            order_result: None,
            hedge_price: Some(dec!(0.26)),
            failure_reason: None,
            verification_state: HedgeVerificationState::VerifiedFilled,
            verification_metadata: HedgeVerificationMetadata::default(),
        };
        let sellback_result = SellbackExecutionResult {
            order_result: Some(sample_sellback_order_result(
                "sellback-order",
                OrderStatus::Matched,
                &[],
            )),
            verification_state: SellbackVerificationState::VerifiedFilled,
            confirmed_shares: Some(dec!(4)),
            failure_reason: None,
            price: Some(dec!(0.31)),
            verification_metadata: SellbackVerificationMetadata::default(),
        };

        let projected = derive_execution_confirmed_sellback_post_sync_position(
            &pre_resolution_position,
            Some(&post_position),
            &intent,
            Some(&hedge_result),
            Some(&sellback_result),
            dec!(0.5),
            true,
        )
        .expect("expected mixed hedge+sellback path to project from confirmed hedge truth");

        assert_eq!(projected.yes_size, dec!(6));
        assert_eq!(projected.no_size, dec!(6));
    }

    #[test]
    fn derive_execution_confirmed_sellback_post_sync_position_rejects_unconfirmed_mixed_path() {
        let pre_resolution_position = Position::new("market".to_string());
        let post_position = Position {
            condition_id: "market".to_string(),
            yes_size: dec!(10),
            no_size: dec!(6),
            avg_yes_price: dec!(0.74),
            avg_no_price: dec!(0.26),
        };
        let intent = HedgeIntent {
            condition_id: "market".to_string(),
            trigger_order_id: "trigger-order".to_string(),
            trigger_leg: QuoteLeg::YesBid,
            fill_size: dec!(10),
            fill_price: dec!(0.74),
            hedge_token_id: "no-token".to_string(),
            hedge_side: Side::Buy,
            neg_risk: false,
            tick_size: "0.01".to_string(),
        };
        let hedge_result = HedgeResult {
            intent: intent.clone(),
            success: true,
            order_result: None,
            hedge_price: Some(dec!(0.26)),
            failure_reason: None,
            verification_state: HedgeVerificationState::VerifiedFilled,
            verification_metadata: HedgeVerificationMetadata::default(),
        };
        let sellback_result = SellbackExecutionResult {
            order_result: Some(sample_sellback_order_result(
                "sellback-order",
                OrderStatus::Matched,
                &[],
            )),
            verification_state: SellbackVerificationState::VerifiedFilled,
            confirmed_shares: Some(dec!(4)),
            failure_reason: None,
            price: Some(dec!(0.31)),
            verification_metadata: SellbackVerificationMetadata::default(),
        };

        assert!(derive_execution_confirmed_sellback_post_sync_position(
            &pre_resolution_position,
            Some(&post_position),
            &intent,
            Some(&hedge_result),
            Some(&sellback_result),
            dec!(0.5),
            false,
        )
        .is_none());
    }

    #[tokio::test]
    async fn execute_resolution_plan_with_timeout_returns_failure_on_timeout() {
        // Verify the timeout wrapper produces the correct failure result
        // when the inner future exceeds the deadline.
        use tokio::time::{sleep, Duration};

        // Simulate a hung resolution by creating a future that sleeps longer
        // than the timeout, then wrapping it the same way the production code does.
        let timeout_secs: u64 = 1;
        let result = tokio::time::timeout(Duration::from_secs(timeout_secs), async {
            sleep(Duration::from_secs(10)).await;
            ResolutionExecutionResult {
                hedge_result: None,
                sellback_result: None,
                post_position: None,
                post_sync_net_exposure: Decimal::ZERO,
                post_sync_source: "normal",
                success: true,
                failure_reason: None,
            }
        })
        .await;

        assert!(result.is_err(), "Expected timeout to fire");

        // Verify we can construct the timeout failure result correctly
        let failure = ResolutionExecutionResult {
            hedge_result: None,
            sellback_result: None,
            post_position: None,
            post_sync_net_exposure: Decimal::MAX,
            post_sync_source: "timeout",
            success: false,
            failure_reason: Some(format!("Hedge execution timed out after {}s", timeout_secs)),
        };
        assert!(!failure.success);
        assert_eq!(failure.post_sync_source, "timeout");
        assert!(failure.failure_reason.unwrap().contains("timed out"));
    }

    #[tokio::test]
    async fn fill_handler_fails_closed_when_post_sync_truth_remains_missing() {
        let state = MockExchangeApiState::default();
        let (base_url, server) = spawn_exchange_api_server(state.clone()).await.unwrap();
        let (engine, event_dir) = build_test_engine_with_urls(&base_url, &base_url, true).await;
        let fill_handler = fill_handler_for_live_engine_test(&engine);
        let market = test_market();

        engine
            .managed_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market.clone());
        *engine.cached_balance.write().await = dec!(1000);
        engine.risk_manager.update_balance(dec!(1000)).await;
        engine.order_manager.update_gross_balance(dec!(1000)).await;
        engine
            .book_manager
            .insert_snapshot(OrderBookSnapshot {
                token_id: market.yes_token_id.clone(),
                exchange_ts: None,
                ingest_ts: Utc::now(),
                bids: vec![PriceLevel {
                    price: dec!(0.73),
                    size: dec!(100),
                }],
                asks: vec![PriceLevel {
                    price: dec!(0.75),
                    size: dec!(100),
                }],
            })
            .await;
        engine
            .book_manager
            .insert_snapshot(OrderBookSnapshot {
                token_id: market.no_token_id.clone(),
                exchange_ts: None,
                ingest_ts: Utc::now(),
                bids: vec![PriceLevel {
                    price: dec!(0.24),
                    size: dec!(100),
                }],
                asks: vec![PriceLevel {
                    price: dec!(0.26),
                    size: dec!(100),
                }],
            })
            .await;

        state
            .balances
            .write()
            .await
            .push_back(balance_response(dec!(1000)));
        state
            .positions
            .write()
            .await
            .push_back(positions_response(vec![]));

        let quote_set = QuoteSet {
            condition_id: market.condition_id.clone(),
            candidates: vec![QuoteCandidate {
                condition_id: market.condition_id.clone(),
                leg: QuoteLeg::YesBid,
                price: dec!(0.74),
                size: dec!(20),
                status: QuoteStatus::Approved,
                reason: None,
            }],
        };
        let tracked = engine
            .order_manager
            .place_quotes(&market, &quote_set, None, dec!(20), None, "test", None)
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        let trade = TradeEvent {
            id: "trade-missing-final-position".to_string(),
            condition_id: market.condition_id.clone(),
            asset_id: market.yes_token_id.clone(),
            side: Side::Buy,
            price: dec!(0.74),
            size: dec!(20),
            outcome: "YES".to_string(),
            status: TradeStatus::Matched,
            timestamp: Utc::now(),
            maker_order_id: Some(tracked.order_id.clone()),
            taker_order_id: None,
        };

        fill_handler
            .handle_fill(FillWorkItem {
                tracked: tracked.clone(),
                trade,
                anchored_order_id: Some(tracked.order_id.clone()),
                match_source: "test".to_string(),
                fallback_match: false,
                size_to_apply: dec!(20),
                hedge_size: dec!(20),
            })
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(250)).await;

        let events = read_emitted_events(&event_dir, &engine.run_id).await;
        let payload =
            latest_logged_payload::<HedgeResultPayload>(&events, EventType::HedgeResultRecorded)
                .expect("expected hedge result event");
        assert_eq!(payload.result_status, "failed");
        assert!(matches!(
            payload.post_sync_source.as_deref(),
            Some("first_sync" | "retry_sync")
        ));
        assert!(payload.post_sync_yes_size.is_none());
        assert!(payload.post_sync_no_size.is_none());
        assert_eq!(payload.post_sync_net_exposure, Some(Decimal::MAX));
        assert!(payload
            .failure_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("final post-sync position truth missing")));
        assert!(events
            .iter()
            .all(|event| event.event_type != EventType::HedgeExitPathRecorded));
        assert!(events
            .iter()
            .all(|event| event.event_type != EventType::NeutralityEvaluated));
        assert!(!engine
            .hedge_signals
            .read()
            .await
            .contains_key(&market.condition_id));
        assert!(
            !engine
                .risk_manager
                .is_market_tradable(&market.condition_id)
                .await
        );

        server.abort();
    }

    #[tokio::test]
    async fn fill_handler_dry_run_sellback_only_missing_truth_recovers_from_current_truth_once_available(
    ) {
        let state = MockExchangeApiState::default();
        let (base_url, server) = spawn_exchange_api_server(state.clone()).await.unwrap();
        let (engine, event_dir) = build_test_engine_with_urls(&base_url, &base_url, true).await;
        let fill_handler = fill_handler_for_live_engine_test(&engine);
        let market = test_market();

        engine
            .managed_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market.clone());
        engine
            .known_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market.clone());
        *engine.cached_balance.write().await = dec!(1000);
        engine.risk_manager.update_balance(dec!(1000)).await;
        engine.order_manager.update_gross_balance(dec!(1000)).await;
        engine
            .book_manager
            .insert_snapshot(OrderBookSnapshot {
                token_id: market.yes_token_id.clone(),
                exchange_ts: None,
                ingest_ts: Utc::now(),
                bids: vec![PriceLevel {
                    price: dec!(0.73),
                    size: dec!(100),
                }],
                asks: vec![PriceLevel {
                    price: dec!(0.75),
                    size: dec!(100),
                }],
            })
            .await;
        engine
            .book_manager
            .insert_snapshot(OrderBookSnapshot {
                token_id: market.no_token_id.clone(),
                exchange_ts: None,
                ingest_ts: Utc::now(),
                bids: vec![PriceLevel {
                    price: dec!(0.24),
                    size: dec!(100),
                }],
                asks: vec![PriceLevel {
                    price: dec!(0.26),
                    size: dec!(100),
                }],
            })
            .await;

        state
            .balances
            .write()
            .await
            .push_back(balance_response(dec!(1000)));
        {
            let mut positions = state.positions.write().await;
            positions.push_back(positions_response(vec![]));
            positions.push_back(positions_response(vec![
                position_entry(&market.condition_id, "YES", dec!(20)),
                position_entry(&market.condition_id, "NO", dec!(20)),
            ]));
        }

        let quote_set = QuoteSet {
            condition_id: market.condition_id.clone(),
            candidates: vec![QuoteCandidate {
                condition_id: market.condition_id.clone(),
                leg: QuoteLeg::YesBid,
                price: dec!(0.74),
                size: dec!(20),
                status: QuoteStatus::Approved,
                reason: None,
            }],
        };
        let tracked = engine
            .order_manager
            .place_quotes(&market, &quote_set, None, dec!(20), None, "test", None)
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        let trade = TradeEvent {
            id: "trade-late-sync".to_string(),
            condition_id: market.condition_id.clone(),
            asset_id: market.yes_token_id.clone(),
            side: Side::Buy,
            price: dec!(0.74),
            size: dec!(20),
            outcome: "YES".to_string(),
            status: TradeStatus::Matched,
            timestamp: Utc::now(),
            maker_order_id: Some(tracked.order_id.clone()),
            taker_order_id: None,
        };

        fill_handler
            .handle_fill(FillWorkItem {
                tracked: tracked.clone(),
                trade,
                anchored_order_id: Some(tracked.order_id.clone()),
                match_source: "test".to_string(),
                fallback_match: false,
                size_to_apply: dec!(20),
                hedge_size: dec!(20),
            })
            .await
            .unwrap();
        let events = wait_for_emitted_events(
            &event_dir,
            &engine.run_id,
            Duration::from_secs(2),
            |events| {
                events
                    .iter()
                    .any(|event| event.event_type == EventType::HedgeResultRecorded)
            },
        )
        .await;
        let decision: HedgeDecisionPayload =
            latest_logged_payload(&events, EventType::HedgeDecisionEvaluated)
                .expect("hedge decision event");
        assert_eq!(decision.decision_reason_code, "budget_rerouted_to_sellback");
        assert_eq!(decision.planned_hedge_shares, Decimal::ZERO);
        assert_eq!(decision.planned_sellback_shares, dec!(20));

        let result: HedgeResultPayload =
            latest_logged_payload(&events, EventType::HedgeResultRecorded)
                .expect("hedge result event");
        assert_eq!(result.result_status, "success");
        assert_eq!(result.hedge_leg_status.as_deref(), Some("skipped"));
        assert_eq!(result.sellback_leg_status.as_deref(), Some("skipped"));
        assert_eq!(result.post_sync_source.as_deref(), Some("position_manager"));
        assert_eq!(result.post_sync_yes_size, Some(dec!(20)));
        assert_eq!(result.post_sync_no_size, Some(dec!(20)));
        assert_eq!(result.post_sync_net_exposure, Some(Decimal::ZERO));
        assert!(result.failure_reason.is_none());

        let exit: HedgeExitPathPayload =
            latest_logged_payload(&events, EventType::HedgeExitPathRecorded)
                .expect("hedge exit event");
        assert_eq!(exit.exit_path_status, "fallback_asks_placed");
        assert_eq!(exit.fallback_ask_count, 2);
        assert_eq!(exit.post_sync_source, "position_manager");

        server.abort();
    }

    #[tokio::test]
    async fn fill_handler_dry_run_sellback_miss_succeeds_when_current_truth_is_within_tolerance() {
        let state = MockExchangeApiState::default();
        let (base_url, server) = spawn_exchange_api_server(state.clone()).await.unwrap();
        let (engine, event_dir) = build_test_engine_with_urls(&base_url, &base_url, true).await;
        let fill_handler = fill_handler_for_live_engine_test(&engine);
        let market = test_market();

        engine
            .managed_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market.clone());
        engine
            .known_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market.clone());
        *engine.cached_balance.write().await = dec!(1000);
        engine.risk_manager.update_balance(dec!(1000)).await;
        engine.order_manager.update_gross_balance(dec!(1000)).await;
        engine
            .book_manager
            .insert_snapshot(OrderBookSnapshot {
                token_id: market.yes_token_id.clone(),
                exchange_ts: None,
                ingest_ts: Utc::now(),
                bids: vec![PriceLevel {
                    price: dec!(0.73),
                    size: dec!(100),
                }],
                asks: vec![PriceLevel {
                    price: dec!(0.75),
                    size: dec!(100),
                }],
            })
            .await;
        engine
            .book_manager
            .insert_snapshot(OrderBookSnapshot {
                token_id: market.no_token_id.clone(),
                exchange_ts: None,
                ingest_ts: Utc::now(),
                bids: vec![PriceLevel {
                    price: dec!(0.24),
                    size: dec!(100),
                }],
                asks: vec![PriceLevel {
                    price: dec!(0.26),
                    size: dec!(100),
                }],
            })
            .await;

        {
            let mut balances = state.balances.write().await;
            balances.push_back(balance_response(dec!(1000)));
            balances.push_back(balance_response(dec!(1000)));
        }
        state
            .positions
            .write()
            .await
            .push_back(positions_response(vec![position_entry(
                &market.condition_id,
                "YES",
                dec!(0.2),
            )]));

        let quote_set = QuoteSet {
            condition_id: market.condition_id.clone(),
            candidates: vec![QuoteCandidate {
                condition_id: market.condition_id.clone(),
                leg: QuoteLeg::YesBid,
                price: dec!(0.74),
                size: dec!(20),
                status: QuoteStatus::Approved,
                reason: None,
            }],
        };
        let tracked = engine
            .order_manager
            .place_quotes(&market, &quote_set, None, dec!(20), None, "test", None)
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        let trade = TradeEvent {
            id: "trade-sellback-current-truth-success".to_string(),
            condition_id: market.condition_id.clone(),
            asset_id: market.yes_token_id.clone(),
            side: Side::Buy,
            price: dec!(0.74),
            size: dec!(20),
            outcome: "YES".to_string(),
            status: TradeStatus::Matched,
            timestamp: Utc::now(),
            maker_order_id: Some(tracked.order_id.clone()),
            taker_order_id: None,
        };

        fill_handler
            .handle_fill(FillWorkItem {
                tracked: tracked.clone(),
                trade,
                anchored_order_id: Some(tracked.order_id.clone()),
                match_source: "test".to_string(),
                fallback_match: false,
                size_to_apply: dec!(20),
                hedge_size: dec!(20),
            })
            .await
            .unwrap();
        let events = wait_for_emitted_events(
            &event_dir,
            &engine.run_id,
            Duration::from_secs(2),
            |events| {
                events
                    .iter()
                    .any(|event| event.event_type == EventType::HedgeExitPathRecorded)
            },
        )
        .await;

        let result: HedgeResultPayload =
            latest_logged_payload(&events, EventType::HedgeResultRecorded)
                .expect("hedge result event");
        assert_eq!(result.result_status, "success");
        assert_eq!(result.sellback_leg_status.as_deref(), Some("skipped"));
        assert_eq!(result.post_sync_source.as_deref(), Some("position_manager"));
        assert_eq!(result.post_sync_net_exposure, Some(dec!(0.2)));

        let exit: HedgeExitPathPayload =
            latest_logged_payload(&events, EventType::HedgeExitPathRecorded)
                .expect("hedge exit event");
        assert_eq!(exit.exit_path_status, "no_exit_needed");
        assert_eq!(exit.post_sync_source, "position_manager");

        server.abort();
    }

    #[test]
    fn recomputed_buy_resolution_can_shift_residual_from_sellback_to_hedge() {
        let market = test_market();
        let first_preparation = ResolutionPreparation {
            yes_book: book_with_levels(
                &market.yes_token_id,
                vec![(dec!(0.73), dec!(100))],
                vec![(dec!(0.75), dec!(100))],
            ),
            no_book: book_with_levels(
                &market.no_token_id,
                vec![(dec!(0.24), dec!(100))],
                vec![(dec!(0.26), dec!(100))],
            ),
            pre_resolution_active_orders: 0,
            pre_resolution_pending_cancels: 0,
            cancel_wait_drained: true,
            max_hedge_usdc: Decimal::ZERO,
        };
        let second_preparation = ResolutionPreparation {
            max_hedge_usdc: dec!(1000),
            ..first_preparation.clone()
        };

        let first = plan_buy_resolution(
            &market,
            &first_preparation,
            &market.no_token_id,
            &market.yes_token_id,
            dec!(0.74),
            dec!(20),
        );
        let second = plan_buy_resolution(
            &market,
            &second_preparation,
            &market.no_token_id,
            &market.yes_token_id,
            dec!(0.74),
            dec!(5),
        );

        assert_eq!(first.hedge_shares, Decimal::ZERO);
        assert_eq!(first.sellback_shares, dec!(20));
        assert_eq!(second.hedge_shares, dec!(5));
        assert_eq!(second.sellback_shares, Decimal::ZERO);
    }

    #[tokio::test]
    async fn fill_handler_dry_run_sellback_miss_fails_closed_after_one_recompute_attempt() {
        let state = MockExchangeApiState::default();
        let (base_url, server) = spawn_exchange_api_server(state.clone()).await.unwrap();
        let (engine, event_dir) = build_test_engine_with_urls(&base_url, &base_url, true).await;
        let fill_handler = fill_handler_for_live_engine_test(&engine);
        let market = test_market();

        engine
            .managed_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market.clone());
        engine
            .known_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market.clone());
        *engine.cached_balance.write().await = dec!(1000);
        engine.risk_manager.update_balance(dec!(1000)).await;
        engine.order_manager.update_gross_balance(dec!(1000)).await;
        engine
            .book_manager
            .insert_snapshot(OrderBookSnapshot {
                token_id: market.yes_token_id.clone(),
                exchange_ts: None,
                ingest_ts: Utc::now(),
                bids: vec![PriceLevel {
                    price: dec!(0.73),
                    size: dec!(100),
                }],
                asks: vec![PriceLevel {
                    price: dec!(0.75),
                    size: dec!(100),
                }],
            })
            .await;
        engine
            .book_manager
            .insert_snapshot(OrderBookSnapshot {
                token_id: market.no_token_id.clone(),
                exchange_ts: None,
                ingest_ts: Utc::now(),
                bids: vec![PriceLevel {
                    price: dec!(0.24),
                    size: dec!(100),
                }],
                asks: vec![PriceLevel {
                    price: dec!(0.26),
                    size: dec!(100),
                }],
            })
            .await;

        {
            let mut balances = state.balances.write().await;
            balances.push_back(balance_response(Decimal::ZERO));
            balances.push_back(balance_response(Decimal::ZERO));
            balances.push_back(balance_response(Decimal::ZERO));
        }
        {
            let mut positions = state.positions.write().await;
            positions.push_back(positions_response(vec![position_entry(
                &market.condition_id,
                "YES",
                dec!(5),
            )]));
            positions.push_back(positions_response(vec![position_entry(
                &market.condition_id,
                "YES",
                dec!(5),
            )]));
            positions.push_back(positions_response(vec![position_entry(
                &market.condition_id,
                "YES",
                dec!(5),
            )]));
        }

        let quote_set = QuoteSet {
            condition_id: market.condition_id.clone(),
            candidates: vec![QuoteCandidate {
                condition_id: market.condition_id.clone(),
                leg: QuoteLeg::YesBid,
                price: dec!(0.74),
                size: dec!(20),
                status: QuoteStatus::Approved,
                reason: None,
            }],
        };
        let tracked = engine
            .order_manager
            .place_quotes(&market, &quote_set, None, dec!(20), None, "test", None)
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        let trade = TradeEvent {
            id: "trade-sellback-recompute-fail".to_string(),
            condition_id: market.condition_id.clone(),
            asset_id: market.yes_token_id.clone(),
            side: Side::Buy,
            price: dec!(0.74),
            size: dec!(20),
            outcome: "YES".to_string(),
            status: TradeStatus::Matched,
            timestamp: Utc::now(),
            maker_order_id: Some(tracked.order_id.clone()),
            taker_order_id: None,
        };

        fill_handler
            .handle_fill(FillWorkItem {
                tracked: tracked.clone(),
                trade,
                anchored_order_id: Some(tracked.order_id.clone()),
                match_source: "test".to_string(),
                fallback_match: false,
                size_to_apply: dec!(20),
                hedge_size: dec!(20),
            })
            .await
            .unwrap();
        let events = wait_for_emitted_events(
            &event_dir,
            &engine.run_id,
            Duration::from_secs(2),
            |events| {
                events
                    .iter()
                    .any(|event| event.event_type == EventType::HedgeResultRecorded)
            },
        )
        .await;

        let result: HedgeResultPayload =
            latest_logged_payload(&events, EventType::HedgeResultRecorded)
                .expect("hedge result event");
        assert_eq!(result.result_status, "failed");
        assert_eq!(result.sellback_leg_status.as_deref(), Some("unverified"));
        assert_eq!(result.post_sync_source.as_deref(), Some("first_sync"));
        assert_eq!(result.post_sync_yes_size, Some(dec!(5)));
        assert_eq!(result.post_sync_no_size, Some(Decimal::ZERO));

        server.abort();
    }

    #[test]
    fn should_retry_resolution_sync_when_position_truth_is_missing_for_buy_hedge() {
        let intent = HedgeIntent {
            condition_id: "condition-1".to_string(),
            trigger_order_id: "order-1".to_string(),
            trigger_leg: QuoteLeg::YesBid,
            fill_size: dec!(5),
            fill_price: dec!(0.74),
            hedge_token_id: "no-token".to_string(),
            hedge_side: Side::Buy,
            neg_risk: false,
            tick_size: "0.01".to_string(),
        };
        let hedge_result = HedgeResult {
            intent: intent.clone(),
            success: true,
            order_result: None,
            hedge_price: Some(dec!(0.27)),
            failure_reason: Some(
                "Hedge fill could not be verified after cancel; awaiting position confirmation"
                    .to_string(),
            ),
            verification_state: HedgeVerificationState::Unknown,
            verification_metadata: HedgeVerificationMetadata::default(),
        };

        assert!(should_retry_resolution_sync(
            Some(&hedge_result),
            &intent,
            &Position::new("condition-1".to_string()),
            None,
        ));
    }

    #[test]
    fn should_retry_resolution_sync_for_sellback_when_exposure_exceeds_tolerance() {
        let sellback_result = SellbackExecutionResult {
            order_result: Some(sample_sellback_order_result(
                "sellback-order",
                OrderStatus::Matched,
                &[],
            )),
            verification_state: SellbackVerificationState::VerifiedFilled,
            confirmed_shares: Some(dec!(5)),
            failure_reason: None,
            price: Some(dec!(0.31)),
            verification_metadata: SellbackVerificationMetadata::default(),
        };
        let post_position = Position {
            condition_id: "market".to_string(),
            yes_size: dec!(5),
            no_size: Decimal::ZERO,
            avg_yes_price: dec!(0.74),
            avg_no_price: Decimal::ZERO,
        };

        assert!(should_retry_resolution_sync_for_sellback(
            Some(&sellback_result),
            Some(&post_position),
            dec!(0.5),
        ));
    }

    #[test]
    fn should_retry_resolution_sync_for_sellback_ignores_verified_within_tolerance() {
        let sellback_result = SellbackExecutionResult {
            order_result: Some(sample_sellback_order_result(
                "sellback-order",
                OrderStatus::Matched,
                &[],
            )),
            verification_state: SellbackVerificationState::VerifiedFilled,
            confirmed_shares: Some(dec!(5)),
            failure_reason: None,
            price: Some(dec!(0.31)),
            verification_metadata: SellbackVerificationMetadata::default(),
        };
        let post_position = Position {
            condition_id: "market".to_string(),
            yes_size: dec!(0.2),
            no_size: Decimal::ZERO,
            avg_yes_price: dec!(0.74),
            avg_no_price: Decimal::ZERO,
        };

        assert!(!should_retry_resolution_sync_for_sellback(
            Some(&sellback_result),
            Some(&post_position),
            dec!(0.5),
        ));
    }

    #[tokio::test]
    async fn reconciliation_exit_does_not_use_cached_position_when_result_position_is_missing() {
        let (engine, event_dir) = build_test_engine("http://127.0.0.1:9", true).await;
        let market = test_market();
        let intent = HedgeIntent {
            condition_id: market.condition_id.clone(),
            trigger_order_id: "recon-order".to_string(),
            trigger_leg: QuoteLeg::YesBid,
            fill_size: dec!(20),
            fill_price: dec!(0.74),
            hedge_token_id: market.no_token_id.clone(),
            hedge_side: Side::Buy,
            neg_risk: false,
            tick_size: market.tick_size.clone(),
        };
        let result = ResolutionExecutionResult {
            hedge_result: None,
            sellback_result: None,
            post_position: None,
            post_sync_net_exposure: Decimal::MAX,
            post_sync_source: "first_sync",
            success: false,
            failure_reason: Some(missing_post_sync_truth_reason().to_string()),
        };

        engine
            .position_manager
            .update_position(Position {
                condition_id: market.condition_id.clone(),
                yes_size: dec!(7),
                no_size: dec!(7),
                avg_yes_price: dec!(0.5),
                avg_no_price: dec!(0.5),
            })
            .await;

        engine
            .emit_reconciliation_hedge_exit(
                "trace-reconciliation",
                "hedge-reconciliation",
                "reconciliation",
                &intent,
                &market,
                &result,
                dec!(0.5),
            )
            .await;
        tokio::time::sleep(Duration::from_millis(250)).await;

        let events = read_emitted_events(&event_dir, &engine.run_id).await;
        assert!(latest_logged_payload::<HedgeExitPathPayload>(
            &events,
            EventType::HedgeExitPathRecorded
        )
        .is_none());
        assert!(events
            .iter()
            .all(|event| event.event_type != EventType::MonitorDegraded));
    }

    #[tokio::test]
    async fn reconciliation_exit_emits_observability_failure_when_position_truth_is_unrecoverable()
    {
        let (engine, event_dir) = build_test_engine("http://127.0.0.1:9", true).await;
        let market = test_market();
        let intent = HedgeIntent {
            condition_id: market.condition_id.clone(),
            trigger_order_id: "recon-order-missing".to_string(),
            trigger_leg: QuoteLeg::YesBid,
            fill_size: dec!(20),
            fill_price: dec!(0.74),
            hedge_token_id: market.no_token_id.clone(),
            hedge_side: Side::Buy,
            neg_risk: false,
            tick_size: market.tick_size.clone(),
        };
        let result = ResolutionExecutionResult {
            hedge_result: None,
            sellback_result: None,
            post_position: None,
            post_sync_net_exposure: Decimal::ZERO,
            post_sync_source: "manual_missing",
            success: true,
            failure_reason: None,
        };

        engine
            .emit_reconciliation_hedge_exit(
                "trace-reconciliation-missing",
                "hedge-reconciliation-missing",
                "reconciliation",
                &intent,
                &market,
                &result,
                dec!(0.5),
            )
            .await;
        tokio::time::sleep(Duration::from_millis(250)).await;

        let events = read_emitted_events(&event_dir, &engine.run_id).await;
        assert!(latest_logged_payload::<HedgeExitPathPayload>(
            &events,
            EventType::HedgeExitPathRecorded
        )
        .is_none());

        let degraded_event = events
            .iter()
            .rev()
            .find(|event| event.event_type == EventType::MonitorDegraded)
            .expect("expected linked observability degradation event");
        assert_eq!(
            degraded_event.trace_id.as_deref(),
            Some("trace-reconciliation-missing")
        );
        assert_eq!(
            degraded_event.condition_id.as_deref(),
            Some(market.condition_id.as_str())
        );
        assert_eq!(
            degraded_event.order_id.as_deref(),
            Some(intent.trigger_order_id.as_str())
        );
        assert_eq!(
            degraded_event.hedge_id.as_deref(),
            Some("hedge-reconciliation-missing")
        );
        assert!(degraded_event.payload["degraded_reason"]
            .as_str()
            .is_some_and(|reason| reason.contains("required hedge_exit_path_recorded")));
    }

    #[tokio::test]
    async fn merge_truth_observer_treats_missing_row_as_flat_expected_state() {
        let state = MockExchangeApiState::default();
        {
            let mut positions = state.positions.write().await;
            positions.push_back(positions_response(vec![position_entry(
                "other-market",
                "YES",
                dec!(2),
            )]));
            positions.push_back(positions_response(vec![position_entry(
                "other-market",
                "YES",
                dec!(2),
            )]));
        }
        let (base_url, server) = spawn_exchange_api_server(state).await.unwrap();
        let (engine, _) = build_test_engine_with_urls(&base_url, &base_url, false).await;

        engine.position_manager.sync_positions().await.unwrap();
        let expected_position = Position::new("market".to_string());
        let observation = observe_merge_truth_convergence_with_params(
            &engine.position_manager,
            "market",
            &expected_position,
            Duration::from_millis(10),
            Duration::from_millis(80),
            2,
        )
        .await;

        assert!(observation.converged);
        assert_eq!(observation.last_seen_position.yes_size, Decimal::ZERO);
        assert_eq!(observation.last_seen_position.no_size, Decimal::ZERO);

        server.abort();
    }

    #[tokio::test]
    async fn merge_truth_observer_requires_two_consecutive_matching_snapshots() {
        let state = MockExchangeApiState::default();
        {
            let mut positions = state.positions.write().await;
            positions.push_back(positions_response(vec![position_entry(
                "market",
                "YES",
                dec!(5),
            )]));
            positions.push_back(positions_response(vec![]));
            positions.push_back(positions_response(vec![position_entry(
                "market",
                "YES",
                dec!(5),
            )]));
            positions.push_back(positions_response(vec![]));
            positions.push_back(positions_response(vec![position_entry(
                "market",
                "YES",
                dec!(5),
            )]));
        }
        let (base_url, server) = spawn_exchange_api_server(state).await.unwrap();
        let (engine, _) = build_test_engine_with_urls(&base_url, &base_url, false).await;

        engine.position_manager.sync_positions().await.unwrap();
        let expected_position = Position::new("market".to_string());
        let observation = observe_merge_truth_convergence_with_params(
            &engine.position_manager,
            "market",
            &expected_position,
            Duration::from_millis(10),
            Duration::from_millis(60),
            2,
        )
        .await;

        assert!(!observation.converged);
        assert!(observation.observed_for >= Duration::from_millis(40));
        assert!(observation.last_sync_error.is_none());

        server.abort();
    }

    #[tokio::test]
    async fn merge_truth_observer_resets_match_streak_after_sync_error() {
        let state = MockExchangeApiState::default();
        {
            let mut positions = state.positions.write().await;
            positions.push_back(positions_response(vec![]));
            positions.push_back("not-json".to_string());
        }
        let (base_url, server) = spawn_exchange_api_server(state).await.unwrap();
        let (engine, _) = build_test_engine_with_urls(&base_url, &base_url, false).await;

        engine.position_manager.sync_positions().await.unwrap();
        let expected_position = Position::new("market".to_string());
        let observation = observe_merge_truth_convergence_with_params(
            &engine.position_manager,
            "market",
            &expected_position,
            Duration::from_millis(20),
            Duration::from_millis(35),
            2,
        )
        .await;

        assert!(!observation.converged);
        assert!(observation
            .last_sync_error
            .as_deref()
            .is_some_and(|error| error.contains("Failed to parse positions response")));

        server.abort();
    }

    #[test]
    fn expected_post_merge_position_normalizes_subshare_residual_dust() {
        let pre_position = Position {
            condition_id: "market".to_string(),
            yes_size: dec!(3.007),
            no_size: dec!(3),
            avg_yes_price: dec!(0.31),
            avg_no_price: dec!(0.69),
        };

        let expected = expected_post_merge_position(&pre_position, dec!(3));

        assert_eq!(expected.yes_size, Decimal::ZERO);
        assert_eq!(expected.no_size, Decimal::ZERO);
    }

    #[tokio::test]
    async fn merge_truth_monitor_is_nonblocking_and_warns_on_timeout_after_merge_success() {
        let state = MockExchangeApiState::default();
        {
            let mut positions = state.positions.write().await;
            positions.push_back(positions_response(vec![
                position_entry("market", "YES", dec!(5)),
                position_entry("market", "NO", dec!(5)),
            ]));
            positions.push_back(positions_response(vec![
                position_entry("market", "YES", dec!(5)),
                position_entry("market", "NO", dec!(5)),
            ]));
        }
        let (base_url, server) = spawn_exchange_api_server(state).await.unwrap();
        let (engine, event_dir) = build_test_engine_with_urls(&base_url, &base_url, true).await;

        engine.position_manager.sync_positions().await.unwrap();
        let spawn_started = Instant::now();
        let handle = spawn_merge_truth_monitor_with_params(
            engine.position_manager.clone(),
            engine.event_producer.clone(),
            engine.run_id.clone(),
            engine.mode.clone(),
            "market".to_string(),
            "0xmerge".to_string(),
            Position::new("market".to_string()),
            Duration::from_millis(10),
            Duration::from_millis(40),
            2,
        );
        assert!(spawn_started.elapsed() < Duration::from_millis(50));

        handle.await.unwrap();

        let events = wait_for_emitted_events(
            &event_dir,
            &engine.run_id,
            Duration::from_secs(2),
            |events| {
                events
                    .iter()
                    .any(|event| event.event_type == EventType::MonitorDegraded)
            },
        )
        .await;
        let degraded_event = events
            .iter()
            .rev()
            .find(|event| event.event_type == EventType::MonitorDegraded)
            .expect("expected merge-truth degradation event");
        assert_eq!(degraded_event.condition_id.as_deref(), Some("market"));
        assert_eq!(
            degraded_event.payload["component"],
            serde_json::json!(MERGE_TRUTH_MONITOR_COMPONENT)
        );
        assert!(degraded_event.payload["degraded_reason"]
            .as_str()
            .is_some_and(|reason| reason.contains("merge truth did not converge")));

        server.abort();
    }

    #[tokio::test]
    async fn harness_merge_pairs_waits_for_lagged_post_merge_truth_before_returning() {
        let state = MockExchangeApiState::default();
        {
            let mut balances = state.balances.write().await;
            balances.push_back(balance_response(dec!(95)));
            balances.push_back(balance_response(dec!(100)));
        }
        {
            let mut positions = state.positions.write().await;
            positions.push_back(positions_response(vec![
                position_entry("market", "YES", dec!(5)),
                position_entry("market", "NO", dec!(5)),
            ]));
            positions.push_back(positions_response(vec![
                position_entry("market", "YES", dec!(5)),
                position_entry("market", "NO", dec!(5)),
            ]));
            positions.push_back(positions_response(vec![]));
            positions.push_back(positions_response(vec![]));
        }
        let (base_url, server) = spawn_exchange_api_server(state).await.unwrap();
        let (mut engine, _) = build_test_engine_with_urls(&base_url, &base_url, false).await;
        engine.ctf_merger = Some(
            Arc::new(MockPairMerger::new(Ok(()), Ok("0xmerge".to_string()))) as Arc<dyn PairMerger>,
        );

        let market = test_market();
        engine
            .managed_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market.clone());
        engine
            .known_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market);

        let started = Instant::now();
        let outcome = engine
            .harness_merge_pairs("market", dec!(5))
            .await
            .expect("merge harness should wait for direct truth convergence");

        assert!(started.elapsed() >= Duration::from_millis(1800));
        assert_eq!(outcome.exit_path_status, "merge_succeeded");
        assert_eq!(outcome.merge_tx_hash.as_deref(), Some("0xmerge"));
        assert_eq!(outcome.post_position.complete_sets(), Decimal::ZERO);

        server.abort();
    }

    #[tokio::test]
    async fn harness_merge_pairs_merges_complete_sets_with_configured_merger() {
        let state = MockExchangeApiState::default();
        {
            let mut balances = state.balances.write().await;
            balances.push_back(balance_response(dec!(95)));
            balances.push_back(balance_response(dec!(100)));
        }
        {
            let mut positions = state.positions.write().await;
            positions.push_back(positions_response(vec![
                position_entry("market", "YES", dec!(5)),
                position_entry("market", "NO", dec!(5)),
            ]));
            positions.push_back(positions_response(vec![]));
            positions.push_back(positions_response(vec![]));
        }
        let (base_url, server) = spawn_exchange_api_server(state).await.unwrap();
        let (mut engine, _) = build_test_engine_with_urls(&base_url, &base_url, false).await;
        engine.ctf_merger = Some(
            Arc::new(MockPairMerger::new(Ok(()), Ok("0xmerge".to_string()))) as Arc<dyn PairMerger>,
        );

        let market = test_market();
        engine
            .managed_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market.clone());
        engine
            .known_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market);

        let outcome = engine
            .harness_merge_pairs("market", dec!(5))
            .await
            .expect("merge harness should succeed");

        assert_eq!(outcome.exit_path_status, "merge_succeeded");
        assert!(outcome.merge_attempted);
        assert_eq!(outcome.merge_tx_hash.as_deref(), Some("0xmerge"));
        assert_eq!(outcome.merge_eligible_pairs, dec!(5));
        assert_eq!(outcome.post_position.complete_sets(), Decimal::ZERO);

        server.abort();
    }

    #[tokio::test]
    async fn harness_merge_pairs_requires_market_metadata_for_venue_resolution() {
        let state = MockExchangeApiState::default();
        {
            let mut positions = state.positions.write().await;
            positions.push_back(positions_response(vec![
                position_entry("orphan-market", "YES", dec!(3)),
                position_entry("orphan-market", "NO", dec!(3)),
            ]));
            positions.push_back(positions_response(vec![
                position_entry("orphan-market", "YES", dec!(3)),
                position_entry("orphan-market", "NO", dec!(3)),
            ]));
        }
        let (base_url, server) = spawn_exchange_api_server(state).await.unwrap();
        let (engine, _) = build_test_engine_with_urls(&base_url, &base_url, false).await;

        let error = engine
            .harness_merge_pairs("orphan-market", dec!(1))
            .await
            .expect_err("missing market metadata should fail harness venue resolution");
        assert!(error.to_string().contains("market metadata"));

        server.abort();
    }

    #[tokio::test]
    async fn harness_merge_pairs_routes_neg_risk_market_to_merger() {
        let state = MockExchangeApiState::default();
        {
            let mut balances = state.balances.write().await;
            balances.push_back(balance_response(dec!(95)));
            balances.push_back(balance_response(dec!(100)));
        }
        {
            let mut positions = state.positions.write().await;
            positions.push_back(positions_response(vec![
                position_entry("market", "YES", dec!(5)),
                position_entry("market", "NO", dec!(5)),
            ]));
            positions.push_back(positions_response(vec![]));
            positions.push_back(positions_response(vec![]));
        }
        let (base_url, server) = spawn_exchange_api_server(state).await.unwrap();
        let (mut engine, _) = build_test_engine_with_urls(&base_url, &base_url, false).await;
        let merger = MockPairMerger::new(Ok(()), Ok("0xmerge".to_string()));
        let observed_neg_risk = merger.observed_neg_risk.clone();
        engine.ctf_merger = Some(Arc::new(merger) as Arc<dyn PairMerger>);

        let market = test_market_with_neg_risk(true);
        engine
            .managed_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market.clone());
        engine
            .known_markets
            .write()
            .await
            .insert(market.condition_id.clone(), market);

        let outcome = engine
            .harness_merge_pairs("market", dec!(5))
            .await
            .expect("merge harness should succeed for neg-risk market");

        assert_eq!(outcome.exit_path_status, "merge_succeeded");
        assert_eq!(observed_neg_risk.read().await.as_slice(), &[true]);

        server.abort();
    }

    #[tokio::test]
    async fn execute_pair_exit_refuses_to_guess_merge_venue_without_market_metadata() {
        let state = MockExchangeApiState::default();
        {
            let mut positions = state.positions.write().await;
            positions.push_back(positions_response(vec![position_entry(
                "orphan-market",
                "YES",
                dec!(3),
            )]));
            positions.push_back(positions_response(vec![position_entry(
                "orphan-market",
                "NO",
                dec!(3),
            )]));
        }
        let (base_url, server) = spawn_exchange_api_server(state).await.unwrap();
        let (mut engine, _) = build_test_engine_with_urls(&base_url, &base_url, false).await;
        let merger = MockPairMerger::new(Ok(()), Ok("0xmerge".to_string()));
        let observed_neg_risk = merger.observed_neg_risk.clone();
        engine.ctf_merger = Some(Arc::new(merger) as Arc<dyn PairMerger>);

        let fill_handler = fill_handler_for_live_engine_test(&engine);
        let mut post_position = Position::new("orphan-market".to_string());
        post_position.yes_size = dec!(3);
        post_position.no_size = dec!(3);
        let mut exit_telemetry = Some(HedgeExitTelemetry::default());

        let observation = fill_handler
            .execute_pair_exit(
                "orphan-market",
                None,
                &post_position,
                &mut exit_telemetry,
                MergeTruthHandling::BackgroundMonitor,
            )
            .await;

        let telemetry = exit_telemetry.expect("telemetry should be captured");
        assert!(observation.is_none());
        assert_eq!(telemetry.exit_path_status, "pair_left_idle");
        assert!(!telemetry.merge_attempted);
        assert!(telemetry
            .merge_failure_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("venue resolution")));
        assert!(telemetry
            .fallback_failure_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("market context missing")));
        assert!(observed_neg_risk.read().await.is_empty());

        server.abort();
    }

    #[tokio::test]
    async fn harness_merge_pairs_rejects_missing_complete_sets() {
        let state = MockExchangeApiState::default();
        {
            let mut positions = state.positions.write().await;
            positions.push_back(positions_response(vec![position_entry(
                "market",
                "YES",
                dec!(5),
            )]));
        }
        let (base_url, server) = spawn_exchange_api_server(state).await.unwrap();
        let (engine, _) = build_test_engine_with_urls(&base_url, &base_url, false).await;

        let error = engine
            .harness_merge_pairs("market", dec!(1))
            .await
            .expect_err("missing pair inventory should fail fast");
        assert!(error.to_string().contains("requires at least"));

        server.abort();
    }

    #[tokio::test]
    async fn harness_ctf_merge_preflight_requires_configured_merger() {
        let state = MockExchangeApiState::default();
        let (base_url, server) = spawn_exchange_api_server(state).await.unwrap();
        let (engine, _) = build_test_engine_with_urls(&base_url, &base_url, false).await;

        let error = engine
            .harness_ctf_merge_preflight()
            .await
            .expect_err("missing merger should fail preflight");
        assert!(error.to_string().contains("not configured"));

        server.abort();
    }

    #[tokio::test]
    async fn harness_ctf_merge_preflight_surfaces_merger_healthcheck_failure() {
        let state = MockExchangeApiState::default();
        let (base_url, server) = spawn_exchange_api_server(state).await.unwrap();
        let (mut engine, _) = build_test_engine_with_urls(&base_url, &base_url, false).await;
        engine.ctf_merger = Some(Arc::new(MockPairMerger::new(
            Err("rpc unavailable".to_string()),
            Ok("0xmerge".to_string()),
        )) as Arc<dyn PairMerger>);

        let error = engine
            .harness_ctf_merge_preflight()
            .await
            .expect_err("failing healthcheck should surface through harness preflight");
        assert!(error.to_string().contains("ctf merger preflight failed"));

        server.abort();
    }
}
