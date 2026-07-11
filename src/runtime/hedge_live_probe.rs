//! Operator-only Layer 3 paired live hedge probe.
//!
//! Acquires the trigger-side position live, then routes that real inventory
//! through the downstream hedge path before cleaning the market back to the
//! pre-probe baseline.

use std::str::FromStr;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use chrono::Utc;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use spreadeater_core::{EventEnvelope, EventProducer, Priority};
use tokio::time::{sleep, Duration};
use uuid::Uuid;

use crate::auth::{validate_token_id_uint256_str, ApiCredentials, RequestSigner};
use crate::books::BookRestClient;
use crate::config::Config;
use crate::discovery::DiscoveryClient;
use crate::models::{
    CanonicalMarket, Market, MarketStatus, OrderAmountKind, OrderBookSnapshot, OrderRequest,
    OrderResult, OrderStatus, OrderType, Outcome, Position, QuoteLeg, RewardConfig, Side,
};
use crate::monitor::ErrorLogger;
use crate::runtime::hedge_harness_support::{
    build_observed_outcome, deserialize_optional_decimal, serialize_optional_decimal,
    InMemoryEventCollector, ObservedHedgeOutcome,
};
use crate::runtime::live_engine::{
    hedge_exposure_tolerance, LiveEngine, ScopedLiveTriggerBinding, ScopedLiveTriggerWatch,
};
use crate::trading::hedge_executor::{normalize_share_size, plan_fill_resolution, HedgeExecutor};
use crate::trading::order_manager::{build_tracked_order, TrackedOrder};
use crate::trading::{CancelOrderOutcome, TradingClient};

pub const LIVE_PROBE_ARM_ENV: &str = "SPREADEATER_HEDGE_LIVE_PROBE_ARM";
pub const LIVE_PROBE_ARM_TOKEN: &str = "I_UNDERSTAND_REAL_ORDERS";
const TRIGGER_VERIFICATION_RETRIES: usize = 6;
const TRIGGER_VERIFICATION_DELAY_MS: u64 = 250;
const CLEANUP_VERIFICATION_RETRIES: usize = 8;
const CLEANUP_VERIFICATION_DELAY_MS: u64 = 250;
const CLEANUP_STABILIZATION_RETRIES_AFTER_TRIGGER: usize = 12;
const CLEANUP_STABILIZATION_DELAY_MS: u64 = 500;

fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum DecimalJsonValue {
    String(String),
    Number(serde_json::Number),
}

fn deserialize_optional_json_decimal<'de, D>(deserializer: D) -> Result<Option<Decimal>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<DecimalJsonValue>::deserialize(deserializer)?;
    match value {
        Some(DecimalJsonValue::String(raw)) => Decimal::from_str(&raw)
            .map(Some)
            .map_err(serde::de::Error::custom),
        Some(DecimalJsonValue::Number(raw)) => Decimal::from_str(&raw.to_string())
            .map(Some)
            .map_err(serde::de::Error::custom),
        None => Ok(None),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HedgeLiveProbeScenario {
    pub name: String,
    pub description: String,
    pub market: LiveProbeMarket,
    pub trigger: LiveProbeTrigger,
    pub safety: LiveProbeSafety,
    pub expected: LiveProbeExpected,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveProbeMarket {
    pub condition_id: String,
    #[serde(default)]
    pub question: Option<String>,
    pub yes_token_id: String,
    pub no_token_id: String,
    pub tick_size: String,
    pub neg_risk: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveProbeTrigger {
    pub leg: QuoteLeg,
    #[serde(with = "rust_decimal::serde::str")]
    pub shares: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub max_trigger_limit_price: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveProbeSafety {
    #[serde(default = "default_true")]
    pub require_clean_market: bool,
    #[serde(with = "rust_decimal::serde::str")]
    pub max_planned_hedge_shares: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub max_planned_sellback_shares: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub max_planned_hedge_notional_usdc: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub max_post_sync_net_exposure: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub max_trigger_notional_usdc: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub max_cleanup_notional_usdc: Decimal,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CleanupStatus {
    Merged,
    Flattened,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExpectedCleanupStatus {
    Merged,
    Flattened,
    MergedOrFlattened,
}

impl ExpectedCleanupStatus {
    fn matches(self, actual: CleanupStatus) -> bool {
        match self {
            Self::Merged => actual == CleanupStatus::Merged,
            Self::Flattened => actual == CleanupStatus::Flattened,
            Self::MergedOrFlattened => true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveProbeExpected {
    pub success: bool,
    pub halted: bool,
    #[serde(default)]
    pub hedge_side: Option<Side>,
    #[serde(default)]
    pub critical_event_types: Vec<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_decimal",
        serialize_with = "serialize_optional_decimal"
    )]
    pub max_planned_hedge_shares: Option<Decimal>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_decimal",
        serialize_with = "serialize_optional_decimal"
    )]
    pub max_planned_sellback_shares: Option<Decimal>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_decimal",
        serialize_with = "serialize_optional_decimal"
    )]
    pub max_post_sync_net_exposure: Option<Decimal>,
    #[serde(default)]
    pub result_status: Option<String>,
    #[serde(default)]
    pub hedge_leg_status: Option<String>,
    #[serde(default)]
    pub sellback_leg_status: Option<String>,
    /// Deprecated: kept for fixture backward compatibility.
    #[serde(default)]
    pub cleanup_status: Option<ExpectedCleanupStatus>,
    /// Deprecated: kept for fixture backward compatibility.
    #[serde(default = "default_true")]
    pub clean_end_state: bool,
}

#[derive(Debug, Clone)]
pub struct LiveProbePlan {
    pub trigger_leg: QuoteLeg,
    pub trigger_token_id: String,
    pub trigger_shares: Decimal,
    pub trigger_snapshot_ask_price: Decimal,
    pub trigger_limit_price: Decimal,
    pub trigger_notional_usdc: Decimal,
    pub hedge_side: Side,
    pub planned_hedge_shares: Decimal,
    pub planned_sellback_shares: Decimal,
    pub planned_hedge_notional_usdc: Decimal,
    pub available_hedge_usdc_after_trigger: Decimal,
}

#[derive(Debug, Clone)]
pub struct LiveProbeEventSummary {
    pub event_type: String,
}

#[derive(Debug, Clone)]
pub struct TriggerAcquisitionSummary {
    pub attempted: bool,
    pub success: bool,
    pub order_id: Option<String>,
    pub requested_shares: Decimal,
    pub resolved_trade_shares: Decimal,
    pub limit_price: Decimal,
    pub snapshot_ask_price: Decimal,
    pub placement_status: Option<String>,
    pub trade_ids: Vec<String>,
    pub transaction_hashes: Vec<String>,
    pub placement_taking_shares: Option<Decimal>,
    pub lookup_status: Option<String>,
    pub lookup_matched_shares: Option<Decimal>,
    pub resolved_trade_id: Option<String>,
    pub ws_trade_observed: bool,
    pub ws_connected_observed: bool,
    pub matched_order_events: usize,
    pub verification_attempts: usize,
    pub failure_code: Option<String>,
    pub failure_reason: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CleanupSummary {
    pub attempted: bool,
    pub attempted_merge: bool,
    pub flatten_orders_placed: usize,
    pub status: Option<CleanupStatus>,
    pub success: bool,
    pub failure_code: Option<String>,
    pub failure_reason: Option<String>,
    pub cleanup_notional_usdc: Decimal,
    pub clean_end_state: bool,
    pub final_yes_size: Option<Decimal>,
    pub final_no_size: Option<Decimal>,
    pub final_direct_yes_size: Option<Decimal>,
    pub final_direct_no_size: Option<Decimal>,
    pub resting_order_count: usize,
}

#[derive(Debug)]
pub struct HedgeLiveProbeResult {
    pub scenario_name: String,
    pub passed: bool,
    pub meta_pass: bool,
    pub standard_pass: bool,
    pub actual_success: bool,
    pub expected_success: bool,
    pub halted: bool,
    pub meta_failures: Vec<String>,
    pub standard_mismatches: Vec<String>,
    pub observed: ObservedHedgeOutcome,
    pub critical_events: Vec<LiveProbeEventSummary>,
    pub preflight: LiveProbePlan,
    pub trigger: TriggerAcquisitionSummary,
    pub cleanup: CleanupSummary,
}

struct LiveProbePreflight {
    canonical_market: CanonicalMarket,
    yes_book: OrderBookSnapshot,
    no_book: OrderBookSnapshot,
    balance: Decimal,
    baseline_position: Position,
    baseline_target_open_orders: usize,
    exposure_tolerance: Decimal,
    trading_client: Arc<TradingClient>,
    book_rest: BookRestClient,
    probe_truth: Arc<ProbeTruthClient>,
    plan: LiveProbePlan,
}

struct TriggerAcquisitionOutcome {
    tracked_order: Option<TrackedOrder>,
    order_result: Option<OrderResult>,
    summary: TriggerAcquisitionSummary,
}

struct CleanupVerificationSnapshot {
    engine_position: Position,
    direct_position: Position,
    resting_order_count: usize,
}

struct TriggerDiagnostics {
    live_order: Option<crate::models::LiveOrder>,
    known_trade_ids: Vec<String>,
    direct_truth_acquired_shares: Decimal,
}

struct ProbeTruthClient {
    http: reqwest::Client,
    data_api_url: String,
    address: String,
}

#[derive(Debug, serde::Deserialize)]
struct ProbeRawPosition {
    #[serde(rename = "conditionId")]
    condition_id: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_json_decimal")]
    size: Option<Decimal>,
    #[serde(rename = "avgPrice")]
    #[serde(default, deserialize_with = "deserialize_optional_json_decimal")]
    avg_price: Option<Decimal>,
    outcome: Option<String>,
}

struct TriggerSummaryParts {
    success: bool,
    order_id: Option<String>,
    requested_shares: Decimal,
    resolved_trade_shares: Decimal,
    limit_price: Decimal,
    snapshot_ask_price: Decimal,
    placement_status: Option<String>,
    trade_ids: Vec<String>,
    transaction_hashes: Vec<String>,
    placement_taking_shares: Option<Decimal>,
    lookup_status: Option<String>,
    lookup_matched_shares: Option<Decimal>,
    resolved_trade_id: Option<String>,
    ws_trade_observed: bool,
    ws_connected_observed: bool,
    matched_order_events: usize,
    verification_attempts: usize,
    failure_code: Option<String>,
    failure_reason: Option<String>,
}

#[derive(Clone)]
#[doc(hidden)]
pub struct LiveProbeRuntimeOptions {
    ws_event_timeout_ms: u64,
    ws_prewarm_delay_ms: u64,
    ws_ambiguous_trigger_grace_ms: u64,
    cleanup_stabilization_retries_after_trigger: usize,
    cleanup_stabilization_delay_ms: u64,
    cleanup_trigger_recovery_timeout_ms: u64,
    cleanup_trigger_recovery_poll_ms: u64,
    merger: Arc<dyn ProbeMergeExecutor>,
}

impl Default for LiveProbeRuntimeOptions {
    fn default() -> Self {
        Self {
            ws_event_timeout_ms: 8_000,
            ws_prewarm_delay_ms: 750,
            ws_ambiguous_trigger_grace_ms: 10_000,
            cleanup_stabilization_retries_after_trigger:
                CLEANUP_STABILIZATION_RETRIES_AFTER_TRIGGER,
            cleanup_stabilization_delay_ms: CLEANUP_STABILIZATION_DELAY_MS,
            cleanup_trigger_recovery_timeout_ms: 10_000,
            cleanup_trigger_recovery_poll_ms: 500,
            merger: Arc::new(EngineProbeMergeExecutor),
        }
    }
}

impl LiveProbeRuntimeOptions {
    #[doc(hidden)]
    pub fn new_for_tests(merger: Arc<dyn ProbeMergeExecutor>) -> Self {
        Self {
            ws_event_timeout_ms: 1_000,
            ws_prewarm_delay_ms: 1,
            ws_ambiguous_trigger_grace_ms: 250,
            cleanup_stabilization_retries_after_trigger: 3,
            cleanup_stabilization_delay_ms: 1,
            // Keep the recovery window short in tests, but long enough to fit within
            // the fixed cleanup retry loop on slower CI schedulers.
            cleanup_trigger_recovery_timeout_ms: 20,
            cleanup_trigger_recovery_poll_ms: 5,
            merger,
        }
    }
}

impl ProbeTruthClient {
    fn new(data_api_url: String, address: String) -> Self {
        Self {
            http: reqwest::Client::new(),
            data_api_url,
            address,
        }
    }

    async fn fetch_position(&self, condition_id: &str) -> Result<Position> {
        let url = format!("{}/positions?user={}", self.data_api_url, self.address);
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .context("Failed to fetch probe truth positions")?;
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            bail!("Probe truth positions API returned error: {}", body);
        }

        let raw_positions: Vec<ProbeRawPosition> = resp
            .json()
            .await
            .context("Failed to parse probe truth positions response")?;
        let mut position = Position::new(condition_id.to_string());

        for raw in raw_positions {
            if raw.condition_id.as_deref() != Some(condition_id) {
                continue;
            }

            let size = raw.size.unwrap_or(Decimal::ZERO);
            let avg_price = raw.avg_price.unwrap_or(Decimal::ZERO);

            match raw.outcome.as_deref() {
                Some("Yes" | "YES") => {
                    position.yes_size = size;
                    position.avg_yes_price = avg_price;
                }
                Some("No" | "NO") => {
                    position.no_size = size;
                    position.avg_no_price = avg_price;
                }
                _ => {}
            }
        }

        Ok(position)
    }
}

#[async_trait]
#[doc(hidden)]
pub trait ProbeMergeExecutor: Send + Sync {
    async fn try_merge_pairs(
        &self,
        engine: &LiveEngine,
        condition_id: &str,
        pair_amount: Decimal,
    ) -> Result<Option<String>>;
}

struct EngineProbeMergeExecutor;

#[async_trait]
impl ProbeMergeExecutor for EngineProbeMergeExecutor {
    async fn try_merge_pairs(
        &self,
        engine: &LiveEngine,
        condition_id: &str,
        pair_amount: Decimal,
    ) -> Result<Option<String>> {
        engine.harness_merge_pairs(condition_id, pair_amount).await
    }
}

pub fn ensure_live_probe_armed() -> Result<()> {
    let value = std::env::var(LIVE_PROBE_ARM_ENV).unwrap_or_default();
    if value != LIVE_PROBE_ARM_TOKEN {
        bail!(
            "{LIVE_PROBE_ARM_ENV} must be set to {LIVE_PROBE_ARM_TOKEN} before hedge-live-probe can place real orders"
        );
    }
    Ok(())
}

pub async fn run_hedge_live_probe(
    scenario_path: &str,
    config: Config,
    credentials: ApiCredentials,
) -> Result<HedgeLiveProbeResult> {
    let scenario = load_scenario(scenario_path)?;
    run_hedge_live_probe_with_runtime(scenario, config, credentials).await
}

pub async fn run_hedge_live_probe_with_runtime(
    scenario: HedgeLiveProbeScenario,
    config: Config,
    credentials: ApiCredentials,
) -> Result<HedgeLiveProbeResult> {
    run_hedge_live_probe_with_options(
        scenario,
        config,
        credentials,
        LiveProbeRuntimeOptions::default(),
    )
    .await
}

#[doc(hidden)]
pub async fn run_hedge_live_probe_with_options(
    scenario: HedgeLiveProbeScenario,
    mut config: Config,
    credentials: ApiCredentials,
    options: LiveProbeRuntimeOptions,
) -> Result<HedgeLiveProbeResult> {
    validate_scenario(&scenario)?;

    let preflight = preflight_live_probe(&scenario, &config, &credentials).await?;

    let base_dir =
        std::env::temp_dir().join(format!("spreadeater-hedge-live-probe-{}", Uuid::new_v4()));
    let archive_dir = base_dir.join("archive");
    let error_dir = base_dir.join("errors");
    std::fs::create_dir_all(&archive_dir)?;
    std::fs::create_dir_all(&error_dir)?;
    config.persistence.archive_dir = archive_dir.to_string_lossy().into_owned();

    let event_collector = Arc::new(InMemoryEventCollector::default());
    let event_producer = Some(event_collector.clone() as Arc<dyn EventProducer>);
    let error_logger = Arc::new(ErrorLogger::new(&error_dir.to_string_lossy()));
    let engine = Arc::new(
        LiveEngine::new_for_harness(
            config,
            credentials,
            error_logger,
            event_producer,
            "hedge-live-probe",
        )
        .await?,
    );

    print_warning_banner(
        &scenario,
        &preflight.plan,
        engine.harness_ctf_merge_enabled(),
    );

    engine
        .harness_seed_market(
            preflight.canonical_market.clone(),
            true,
            true,
            preflight.yes_book.clone(),
            preflight.no_book.clone(),
        )
        .await;
    engine.harness_seed_balance(preflight.balance).await;
    engine.harness_sync_positions().await?;

    let user_rx = engine
        .harness_subscribe_user_stream()
        .await
        .ok_or_else(|| anyhow!("Failed to subscribe user stream for hedge-live-probe"))?;
    let trigger_watch = ScopedLiveTriggerWatch::new();
    let runtime_handle =
        Arc::clone(&engine).harness_spawn_scoped_live_runtime(user_rx, trigger_watch.clone());
    sleep(Duration::from_millis(options.ws_prewarm_delay_ms)).await;
    let placed_trigger =
        acquire_trigger_position(&scenario, &preflight, &engine, &trigger_watch).await?;
    let runtime_report = if placed_trigger.summary.order_id.is_some() {
        sleep(Duration::from_millis(options.ws_event_timeout_ms)).await;
        let interim_report = runtime_handle.snapshot().await;
        if !interim_report.matched_trigger_trade_ids.is_empty() {
            runtime_handle.stop().await?
        } else {
            if should_extend_trigger_ws_observation(&placed_trigger, &interim_report) {
                sleep(Duration::from_millis(options.ws_ambiguous_trigger_grace_ms)).await;
            }
            runtime_handle.stop().await?
        }
    } else {
        runtime_handle.stop().await?
    };

    let _ = engine.harness_refresh_balance().await;
    engine.harness_sync_positions().await?;

    let trigger =
        finalize_trigger_acquisition(&scenario, &preflight, placed_trigger, &runtime_report)
            .await?;

    let events = event_collector.events();
    let critical_events = collect_critical_events(&events);
    let observed = if trigger.summary.ws_trade_observed {
        build_observed_outcome(
            &events,
            engine.harness_risk_manager(),
            &scenario.market.condition_id,
            Vec::new(),
        )
        .await?
    } else {
        empty_observed_outcome()
    };

    let actual_success = observed.result_status.as_deref() == Some("success");

    let meta_failures = compare_meta_to_runtime(&trigger.summary, &runtime_report);
    let meta_pass = meta_failures.is_empty();

    let mut standard_mismatches =
        compare_expected_to_observed(&scenario.expected, &observed, actual_success);
    if !trigger.summary.ws_trade_observed {
        standard_mismatches.push(format!(
            "standard verdict unavailable: trigger did not reach the production user-stream trade path ({})",
            trigger
                .summary
                .failure_reason
                .clone()
                .unwrap_or_else(|| "unknown trigger failure".to_string())
        ));
    }
    standard_mismatches.extend(compare_expected_critical_event_types(
        &scenario.expected.critical_event_types,
        &critical_events,
    ));
    standard_mismatches.extend(compare_safety_to_observed(
        &scenario.safety,
        &preflight.plan,
        &observed,
    ));
    standard_mismatches.extend(compare_trigger_to_standard(&trigger.summary));
    let standard_pass = standard_mismatches.is_empty();

    let cleanup =
        run_probe_cleanup(&scenario, &preflight, &engine, &options, &trigger.summary).await?;

    Ok(HedgeLiveProbeResult {
        scenario_name: scenario.name,
        passed: meta_pass && standard_pass,
        meta_pass,
        standard_pass,
        actual_success,
        expected_success: scenario.expected.success,
        halted: observed.halted,
        meta_failures,
        standard_mismatches,
        observed,
        critical_events,
        preflight: preflight.plan,
        trigger: trigger.summary,
        cleanup,
    })
}

// section: scenario loading / validation / preflight helpers

pub fn load_scenario(path: &str) -> Result<HedgeLiveProbeScenario> {
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read live probe scenario file: {path}"))?;
    let scenario: HedgeLiveProbeScenario = serde_json::from_str(&contents)
        .with_context(|| format!("Failed to parse live probe scenario JSON from: {path}"))?;
    validate_scenario(&scenario)?;
    Ok(scenario)
}

fn validate_scenario(scenario: &HedgeLiveProbeScenario) -> Result<()> {
    validate_market(&scenario.market)?;
    if scenario.trigger.shares <= Decimal::ZERO {
        bail!("Scenario trigger.shares must be > 0");
    }
    if scenario.trigger.max_trigger_limit_price <= Decimal::ZERO {
        bail!("Scenario trigger.max_trigger_limit_price must be > 0");
    }
    if scenario.safety.max_planned_hedge_shares < Decimal::ZERO {
        bail!("Scenario safety.max_planned_hedge_shares must be >= 0");
    }
    if scenario.safety.max_planned_sellback_shares < Decimal::ZERO {
        bail!("Scenario safety.max_planned_sellback_shares must be >= 0");
    }
    if scenario.safety.max_planned_hedge_notional_usdc < Decimal::ZERO {
        bail!("Scenario safety.max_planned_hedge_notional_usdc must be >= 0");
    }
    if scenario.safety.max_post_sync_net_exposure < Decimal::ZERO {
        bail!("Scenario safety.max_post_sync_net_exposure must be >= 0");
    }
    if scenario.safety.max_trigger_notional_usdc < Decimal::ZERO {
        bail!("Scenario safety.max_trigger_notional_usdc must be >= 0");
    }
    if scenario.safety.max_cleanup_notional_usdc < Decimal::ZERO {
        bail!("Scenario safety.max_cleanup_notional_usdc must be >= 0");
    }
    Ok(())
}

fn validate_market(market: &LiveProbeMarket) -> Result<()> {
    validate_token_id_uint256_str(&market.yes_token_id).context(
        "Scenario market.yes_token_id must be a valid uint256 decimal string for order signing",
    )?;
    validate_token_id_uint256_str(&market.no_token_id).context(
        "Scenario market.no_token_id must be a valid uint256 decimal string for order signing",
    )?;
    Decimal::from_str(&market.tick_size)
        .with_context(|| format!("Scenario market.tick_size is invalid: {}", market.tick_size))?;
    Ok(())
}

async fn preflight_live_probe(
    scenario: &HedgeLiveProbeScenario,
    config: &Config,
    credentials: &ApiCredentials,
) -> Result<LiveProbePreflight> {
    let discovery = DiscoveryClient::new(config.discovery.clone());
    if let Some(live_market) = discovery
        .fetch_market_by_condition_id(&scenario.market.condition_id)
        .await?
    {
        ensure_live_market_matches_scenario(&scenario.market, &live_market)?;
    } else {
        eprintln!(
            "warning: target condition_id {} was not returned by sampling-markets; continuing with scenario metadata",
            scenario.market.condition_id
        );
    }

    let trading_client = build_trading_client(config, credentials)?;
    let book_rest = BookRestClient::new(config.discovery.clob_base_url.clone());
    let funder = credentials
        .funder
        .as_deref()
        .unwrap_or(&credentials.address)
        .to_string();
    let probe_truth = Arc::new(ProbeTruthClient::new(
        config.discovery.data_api_base_url.clone(),
        funder,
    ));

    let global_open_orders = trading_client.get_open_orders(None).await?;
    let target_open_orders = trading_client
        .get_open_orders(Some(&scenario.market.condition_id))
        .await?;
    let balance = trading_client.get_balance().await?;
    let baseline_position = probe_truth
        .fetch_position(&scenario.market.condition_id)
        .await?;
    let (yes_book, no_book) = book_rest
        .fetch_both_books(&scenario.market.yes_token_id, &scenario.market.no_token_id)
        .await?;

    let tolerance = hedge_exposure_tolerance(config);
    if scenario.safety.require_clean_market {
        if !target_open_orders.is_empty() {
            bail!(
                "live probe aborted: target market {} has {} existing open orders",
                scenario.market.condition_id,
                target_open_orders.len()
            );
        }
        if baseline_position.yes_size.abs() > tolerance
            || baseline_position.no_size.abs() > tolerance
        {
            bail!(
                "live probe aborted: target market {} has existing inventory (yes={}, no={})",
                scenario.market.condition_id,
                baseline_position.yes_size,
                baseline_position.no_size
            );
        }
    }

    let committed_exposure: Decimal = global_open_orders
        .iter()
        .filter(|order| order.side == Side::Buy)
        .map(|order| order.remaining_size())
        .sum();
    ensure_supported_trigger_creation_leg(scenario.trigger.leg)?;
    let tick = parse_tick_size(&scenario.market)?;
    let trigger_book = trigger_book(&scenario.trigger.leg, &yes_book, &no_book);
    let snapshot_ask_price = best_ask_price(trigger_book)
        .ok_or_else(|| anyhow!("live probe aborted: trigger book had no ask liquidity"))?;
    let trigger_limit_price = derive_marketable_limit_price(snapshot_ask_price, tick);
    if trigger_limit_price > scenario.trigger.max_trigger_limit_price {
        bail!(
            "live probe aborted: derived trigger_limit_price {} exceeded trigger.max_trigger_limit_price {}",
            trigger_limit_price,
            scenario.trigger.max_trigger_limit_price
        );
    }
    let trigger_notional_usdc = normalize_share_size(scenario.trigger.shares) * trigger_limit_price;
    if trigger_notional_usdc > scenario.safety.max_trigger_notional_usdc {
        bail!(
            "live probe aborted: trigger_notional_usdc {} exceeded safety.max_trigger_notional_usdc {}",
            trigger_notional_usdc,
            scenario.safety.max_trigger_notional_usdc
        );
    }

    let available_hedge_usdc_after_trigger =
        (balance - committed_exposure - trigger_notional_usdc).max(Decimal::ZERO);
    let plan = build_preflight_plan(
        scenario,
        snapshot_ask_price,
        &yes_book,
        &no_book,
        available_hedge_usdc_after_trigger,
        trigger_limit_price,
        trigger_notional_usdc,
    )?;
    enforce_preflight_safety(&scenario.safety, &plan)?;

    Ok(LiveProbePreflight {
        canonical_market: build_canonical_market(&scenario.market),
        yes_book,
        no_book,
        balance,
        baseline_position,
        baseline_target_open_orders: target_open_orders.len(),
        exposure_tolerance: tolerance,
        trading_client,
        book_rest,
        probe_truth,
        plan,
    })
}

fn build_trading_client(
    config: &Config,
    credentials: &ApiCredentials,
) -> Result<Arc<TradingClient>> {
    let signer = RequestSigner::new(credentials.clone());
    let funder = credentials
        .funder
        .as_deref()
        .unwrap_or(&credentials.address);
    Ok(Arc::new(TradingClient::new(
        config.discovery.clob_base_url.clone(),
        signer,
        credentials.private_key.as_deref(),
        funder,
        &credentials.api_key,
        false,
    )?))
}

fn build_preflight_plan(
    scenario: &HedgeLiveProbeScenario,
    trigger_fill_price: Decimal,
    yes_book: &OrderBookSnapshot,
    no_book: &OrderBookSnapshot,
    available_hedge_usdc_after_trigger: Decimal,
    trigger_limit_price: Decimal,
    trigger_notional_usdc: Decimal,
) -> Result<LiveProbePlan> {
    let trigger_leg = scenario.trigger.leg;
    let trigger_token_id = token_id_for_leg(trigger_leg, &scenario.market);
    let (_, hedge_side) = HedgeExecutor::compute_hedge_params(
        trigger_leg,
        &scenario.market.yes_token_id,
        &scenario.market.no_token_id,
    );

    let tick = parse_tick_size(&scenario.market)?;
    let (hedge_book, filled_book) = match trigger_leg {
        QuoteLeg::YesBid => (no_book, yes_book),
        QuoteLeg::NoBid => (yes_book, no_book),
        other => {
            bail!(
                "trigger.leg {:?} is not yet supported by hedge-live-probe trigger placement; the production hedge path itself remains unchanged once a trigger trade exists",
                other
            );
        }
    };
    let resolution = plan_fill_resolution(
        trigger_fill_price,
        &hedge_book.asks,
        &filled_book.bids,
        scenario.trigger.shares,
        available_hedge_usdc_after_trigger,
        tick,
    );

    Ok(LiveProbePlan {
        trigger_leg,
        trigger_token_id,
        trigger_shares: scenario.trigger.shares,
        trigger_snapshot_ask_price: trigger_fill_price,
        trigger_limit_price,
        trigger_notional_usdc,
        hedge_side,
        planned_hedge_shares: resolution.hedge_shares,
        planned_sellback_shares: resolution.sellback_shares,
        planned_hedge_notional_usdc: resolution.hedge_shares * resolution.hedge_limit_price,
        available_hedge_usdc_after_trigger,
    })
}

fn enforce_preflight_safety(safety: &LiveProbeSafety, plan: &LiveProbePlan) -> Result<()> {
    if plan.planned_hedge_shares > safety.max_planned_hedge_shares {
        bail!(
            "live probe aborted: planned_hedge_shares {} exceeded safety.max_planned_hedge_shares {}",
            plan.planned_hedge_shares,
            safety.max_planned_hedge_shares
        );
    }
    if plan.planned_sellback_shares > safety.max_planned_sellback_shares {
        bail!(
            "live probe aborted: planned_sellback_shares {} exceeded safety.max_planned_sellback_shares {}",
            plan.planned_sellback_shares,
            safety.max_planned_sellback_shares
        );
    }
    if plan.planned_hedge_notional_usdc > safety.max_planned_hedge_notional_usdc {
        bail!(
            "live probe aborted: planned_hedge_notional_usdc {} exceeded safety.max_planned_hedge_notional_usdc {}",
            plan.planned_hedge_notional_usdc,
            safety.max_planned_hedge_notional_usdc
        );
    }
    Ok(())
}

fn ensure_live_market_matches_scenario(
    scenario: &LiveProbeMarket,
    live_market: &Market,
) -> Result<()> {
    let live_yes = live_market
        .tokens
        .iter()
        .find(|token| token.outcome == Outcome::Yes)
        .map(|token| token.token_id.clone())
        .unwrap_or_default();
    let live_no = live_market
        .tokens
        .iter()
        .find(|token| token.outcome == Outcome::No)
        .map(|token| token.token_id.clone())
        .unwrap_or_default();

    let mut mismatches = Vec::new();
    if scenario.yes_token_id != live_yes {
        mismatches.push(format!(
            "yes_token_id mismatch: scenario={}, discovery={}",
            scenario.yes_token_id, live_yes
        ));
    }
    if scenario.no_token_id != live_no {
        mismatches.push(format!(
            "no_token_id mismatch: scenario={}, discovery={}",
            scenario.no_token_id, live_no
        ));
    }
    if scenario.tick_size != live_market.minimum_tick_size {
        mismatches.push(format!(
            "tick_size mismatch: scenario={}, discovery={}",
            scenario.tick_size, live_market.minimum_tick_size
        ));
    }
    if scenario.neg_risk != live_market.neg_risk {
        mismatches.push(format!(
            "neg_risk mismatch: scenario={}, discovery={}",
            scenario.neg_risk, live_market.neg_risk
        ));
    }

    if !mismatches.is_empty() {
        bail!(
            "live probe aborted: discovery metadata disagreed with scenario for {}: {}",
            scenario.condition_id,
            mismatches.join("; ")
        );
    }
    Ok(())
}

fn build_canonical_market(market: &LiveProbeMarket) -> CanonicalMarket {
    CanonicalMarket {
        condition_id: market.condition_id.clone(),
        market_slug: format!("{}-live-probe", market.condition_id),
        question: market
            .question
            .clone()
            .unwrap_or_else(|| format!("Live probe market {}", market.condition_id)),
        yes_token_id: market.yes_token_id.clone(),
        no_token_id: market.no_token_id.clone(),
        reward_config: RewardConfig {
            condition_id: market.condition_id.clone(),
            daily_reward_rates: Vec::new(),
            daily_reward_total: Decimal::ZERO,
            min_size: Decimal::ZERO,
            max_spread: Decimal::ZERO,
        },
        neg_risk: market.neg_risk,
        tick_size: market.tick_size.clone(),
        end_date: None,
        admitted_at: Utc::now(),
        status: MarketStatus::Admitted,
    }
}

fn parse_tick_size(market: &LiveProbeMarket) -> Result<Decimal> {
    Decimal::from_str(&market.tick_size)
        .with_context(|| format!("Invalid tick_size: {}", market.tick_size))
}

fn trigger_book<'a>(
    leg: &QuoteLeg,
    yes_book: &'a OrderBookSnapshot,
    no_book: &'a OrderBookSnapshot,
) -> &'a OrderBookSnapshot {
    match leg {
        QuoteLeg::YesBid => yes_book,
        QuoteLeg::NoBid => no_book,
        _ => unreachable!("validation restricts trigger legs"),
    }
}

fn ensure_supported_trigger_creation_leg(leg: QuoteLeg) -> Result<()> {
    if matches!(leg, QuoteLeg::YesBid | QuoteLeg::NoBid) {
        return Ok(());
    }
    bail!(
        "live probe trigger creation currently only supports synthetic BUY triggers for YesBid/NoBid; ask-leg probes require pre-owned inventory and are outside the accepted probe scaffolding"
    );
}

fn derive_marketable_limit_price(best_ask: Decimal, tick: Decimal) -> Decimal {
    (best_ask + tick).min(Decimal::new(99, 2))
}

fn best_ask_price(book: &OrderBookSnapshot) -> Option<Decimal> {
    book.asks.iter().map(|level| level.price).min()
}

fn best_bid_price(book: &OrderBookSnapshot) -> Option<Decimal> {
    book.bids.iter().map(|level| level.price).max()
}

// section: trigger acquisition helpers

async fn acquire_trigger_position(
    scenario: &HedgeLiveProbeScenario,
    preflight: &LiveProbePreflight,
    engine: &LiveEngine,
    trigger_watch: &ScopedLiveTriggerWatch,
) -> Result<TriggerAcquisitionOutcome> {
    if !matches!(scenario.trigger.leg, QuoteLeg::YesBid | QuoteLeg::NoBid) {
        bail!(
            "live probe trigger creation currently only supports synthetic BUY triggers for YesBid/NoBid; ask-leg probes require pre-owned inventory and are outside the accepted probe scaffolding"
        );
    }

    let requested_shares = normalize_share_size(scenario.trigger.shares);
    let request = OrderRequest {
        token_id: preflight.plan.trigger_token_id.clone(),
        price: preflight.plan.trigger_limit_price,
        size: requested_shares,
        amount_kind: OrderAmountKind::Shares,
        side: Side::Buy,
        order_type: OrderType::GTC,
        post_only: false,
        neg_risk: scenario.market.neg_risk,
        tick_size: scenario.market.tick_size.clone(),
    };

    let order_result = match preflight.trading_client.place_order(&request).await {
        Ok(result) => result,
        Err(err) => {
            return Ok(TriggerAcquisitionOutcome {
                tracked_order: None,
                order_result: None,
                summary: build_trigger_summary(TriggerSummaryParts {
                    success: false,
                    order_id: None,
                    requested_shares,
                    resolved_trade_shares: Decimal::ZERO,
                    limit_price: preflight.plan.trigger_limit_price,
                    snapshot_ask_price: preflight.plan.trigger_snapshot_ask_price,
                    placement_status: None,
                    trade_ids: Vec::new(),
                    transaction_hashes: Vec::new(),
                    placement_taking_shares: None,
                    lookup_status: None,
                    lookup_matched_shares: None,
                    resolved_trade_id: None,
                    ws_trade_observed: false,
                    ws_connected_observed: false,
                    matched_order_events: 0,
                    verification_attempts: 0,
                    failure_code: Some("trigger_order_placement_failed".to_string()),
                    failure_reason: Some(format!("trigger order placement failed: {}", err)),
                }),
            });
        }
    };

    if order_result.order_id.is_empty() {
        return Ok(TriggerAcquisitionOutcome {
            tracked_order: None,
            order_result: Some(order_result.clone()),
            summary: build_trigger_summary(TriggerSummaryParts {
                success: false,
                order_id: None,
                requested_shares,
                resolved_trade_shares: Decimal::ZERO,
                limit_price: preflight.plan.trigger_limit_price,
                snapshot_ask_price: preflight.plan.trigger_snapshot_ask_price,
                placement_status: Some(format!("{:?}", order_result.status)),
                trade_ids: order_result.trade_ids.clone(),
                transaction_hashes: order_result.transaction_hashes.clone(),
                placement_taking_shares: order_result.taking_amount,
                lookup_status: None,
                lookup_matched_shares: None,
                resolved_trade_id: None,
                ws_trade_observed: false,
                ws_connected_observed: false,
                matched_order_events: 0,
                verification_attempts: 0,
                failure_code: Some("trigger_order_missing_id".to_string()),
                failure_reason: Some("trigger placement returned no order_id".to_string()),
            }),
        });
    }

    let tracked = build_tracked_order(
        order_result.order_id.clone(),
        format!("hedge-live-probe-trigger-{}", Uuid::new_v4()),
        Utc::now(),
        scenario.market.condition_id.clone(),
        scenario.trigger.leg,
        token_id_for_leg(scenario.trigger.leg, &scenario.market),
        opposite_token_id_for_leg(scenario.trigger.leg, &scenario.market),
        Side::Buy,
        preflight.plan.trigger_limit_price,
        requested_shares,
        Decimal::ZERO,
        scenario.market.neg_risk,
        scenario.market.tick_size.clone(),
    );
    engine.harness_register_tracked_order(tracked.clone()).await;
    trigger_watch
        .bind(build_scoped_trigger_binding(
            scenario,
            &order_result.order_id,
        ))
        .await;

    Ok(TriggerAcquisitionOutcome {
        tracked_order: Some(tracked),
        order_result: Some(order_result.clone()),
        summary: build_trigger_summary(TriggerSummaryParts {
            success: false,
            order_id: Some(order_result.order_id.clone()),
            requested_shares,
            resolved_trade_shares: Decimal::ZERO,
            limit_price: preflight.plan.trigger_limit_price,
            snapshot_ask_price: preflight.plan.trigger_snapshot_ask_price,
            placement_status: Some(format!("{:?}", order_result.status)),
            trade_ids: order_result.trade_ids.clone(),
            transaction_hashes: order_result.transaction_hashes.clone(),
            placement_taking_shares: order_result.taking_amount,
            lookup_status: None,
            lookup_matched_shares: None,
            resolved_trade_id: None,
            ws_trade_observed: false,
            ws_connected_observed: false,
            matched_order_events: 0,
            verification_attempts: 0,
            failure_code: None,
            failure_reason: None,
        }),
    })
}

async fn finalize_trigger_acquisition(
    scenario: &HedgeLiveProbeScenario,
    preflight: &LiveProbePreflight,
    placed_trigger: TriggerAcquisitionOutcome,
    runtime_report: &crate::runtime::live_engine::ScopedLiveRunnerReport,
) -> Result<TriggerAcquisitionOutcome> {
    let Some(order_result) = placed_trigger.order_result.clone() else {
        return Ok(placed_trigger);
    };
    let requested_shares = placed_trigger.summary.requested_shares;
    let mut trade_ids = order_result.trade_ids.clone();
    merge_trade_ids(&mut trade_ids, &runtime_report.matched_trigger_trade_ids);
    let cancel_outcome =
        cancel_trigger_remainder(&preflight.trading_client, &order_result.order_id).await?;
    let runtime_lookup = if runtime_report.matched_trigger_trade_ids.is_empty()
        || runtime_report.matched_trigger_trade_shares != requested_shares
    {
        Some(collect_trigger_diagnostics(scenario, preflight, &order_result, 1).await?)
    } else {
        None
    };
    let lookup_status = runtime_lookup
        .as_ref()
        .and_then(|diagnostics| diagnostics.live_order.as_ref())
        .map(|order| format!("{:?}", order.status));
    let lookup_matched_shares = runtime_lookup
        .as_ref()
        .and_then(|diagnostics| diagnostics.live_order.as_ref())
        .map(|order| order.size_matched);
    if let Some(diagnostics) = &runtime_lookup {
        merge_trade_ids(&mut trade_ids, &diagnostics.known_trade_ids);
    }

    if !runtime_report.matched_trigger_trade_ids.is_empty() {
        if let Some((failure_code, failure_reason)) = trigger_size_mismatch_details(
            requested_shares,
            runtime_report.matched_trigger_trade_shares,
            cancel_outcome.as_deref(),
        ) {
            return Ok(TriggerAcquisitionOutcome {
                tracked_order: placed_trigger.tracked_order,
                order_result: placed_trigger.order_result,
                summary: build_trigger_summary(TriggerSummaryParts {
                    success: false,
                    order_id: Some(order_result.order_id.clone()),
                    requested_shares,
                    resolved_trade_shares: runtime_report.matched_trigger_trade_shares,
                    limit_price: placed_trigger.summary.limit_price,
                    snapshot_ask_price: placed_trigger.summary.snapshot_ask_price,
                    placement_status: Some(format!("{:?}", order_result.status)),
                    trade_ids,
                    transaction_hashes: order_result.transaction_hashes.clone(),
                    placement_taking_shares: order_result.taking_amount,
                    lookup_status,
                    lookup_matched_shares,
                    resolved_trade_id: runtime_report.matched_trigger_trade_ids.first().cloned(),
                    ws_trade_observed: true,
                    ws_connected_observed: runtime_report.connected_observed,
                    matched_order_events: runtime_report.matched_trigger_order_events,
                    verification_attempts: 1,
                    failure_code: Some(failure_code.to_string()),
                    failure_reason: Some(failure_reason),
                }),
            });
        }

        return Ok(TriggerAcquisitionOutcome {
            tracked_order: placed_trigger.tracked_order,
            order_result: placed_trigger.order_result,
            summary: build_trigger_summary(TriggerSummaryParts {
                success: true,
                order_id: Some(order_result.order_id.clone()),
                requested_shares,
                resolved_trade_shares: runtime_report.matched_trigger_trade_shares,
                limit_price: placed_trigger.summary.limit_price,
                snapshot_ask_price: placed_trigger.summary.snapshot_ask_price,
                placement_status: Some(format!("{:?}", order_result.status)),
                trade_ids,
                transaction_hashes: order_result.transaction_hashes.clone(),
                placement_taking_shares: order_result.taking_amount,
                lookup_status,
                lookup_matched_shares,
                resolved_trade_id: runtime_report.matched_trigger_trade_ids.first().cloned(),
                ws_trade_observed: true,
                ws_connected_observed: runtime_report.connected_observed,
                matched_order_events: runtime_report.matched_trigger_order_events,
                verification_attempts: 1,
                failure_code: None,
                failure_reason: None,
            }),
        });
    }

    let diagnostics = collect_trigger_diagnostics(
        scenario,
        preflight,
        &order_result,
        TRIGGER_VERIFICATION_RETRIES + 1,
    )
    .await?;
    let lookup_status = diagnostics
        .live_order
        .as_ref()
        .map(|order| format!("{:?}", order.status));
    let lookup_matched_shares = diagnostics
        .live_order
        .as_ref()
        .map(|order| order.size_matched);
    merge_trade_ids(&mut trade_ids, &diagnostics.known_trade_ids);
    let (failure_code, failure_reason) = trigger_failure_details(
        Some(format!("{:?}", order_result.status)),
        order_result.taking_amount,
        lookup_status.clone(),
        lookup_matched_shares,
        &trade_ids,
        diagnostics.direct_truth_acquired_shares,
        runtime_report.matched_trigger_order_events,
        cancel_outcome.as_deref(),
    );

    Ok(TriggerAcquisitionOutcome {
        tracked_order: placed_trigger.tracked_order,
        order_result: placed_trigger.order_result,
        summary: build_trigger_summary(TriggerSummaryParts {
            success: false,
            order_id: Some(order_result.order_id.clone()),
            requested_shares,
            resolved_trade_shares: runtime_report.matched_trigger_trade_shares,
            limit_price: placed_trigger.summary.limit_price,
            snapshot_ask_price: placed_trigger.summary.snapshot_ask_price,
            placement_status: Some(format!("{:?}", order_result.status)),
            trade_ids,
            transaction_hashes: order_result.transaction_hashes.clone(),
            placement_taking_shares: order_result.taking_amount,
            lookup_status,
            lookup_matched_shares,
            resolved_trade_id: None,
            ws_trade_observed: false,
            ws_connected_observed: runtime_report.connected_observed,
            matched_order_events: runtime_report.matched_trigger_order_events,
            verification_attempts: TRIGGER_VERIFICATION_RETRIES + 1,
            failure_code: Some(failure_code.to_string()),
            failure_reason: Some(failure_reason),
        }),
    })
}

fn build_scoped_trigger_binding(
    scenario: &HedgeLiveProbeScenario,
    trigger_order_id: &str,
) -> ScopedLiveTriggerBinding {
    ScopedLiveTriggerBinding {
        order_id: trigger_order_id.to_string(),
        condition_id: scenario.market.condition_id.clone(),
        asset_id: token_id_for_leg(scenario.trigger.leg, &scenario.market),
        side: Side::Buy,
        outcome: outcome_for_leg(scenario.trigger.leg).to_string(),
    }
}

async fn cancel_trigger_remainder(
    trading_client: &Arc<TradingClient>,
    order_id: &str,
) -> Result<Option<String>> {
    if order_id.is_empty() {
        return Ok(None);
    }

    let summary = match trading_client.cancel_order(order_id).await? {
        CancelOrderOutcome::Confirmed => "confirmed".to_string(),
        CancelOrderOutcome::Rejected(reason) => format!("rejected: {}", reason),
        CancelOrderOutcome::Unknown(reason) => format!("unknown: {}", reason),
    };
    Ok(Some(summary))
}

async fn collect_trigger_diagnostics(
    scenario: &HedgeLiveProbeScenario,
    preflight: &LiveProbePreflight,
    order_result: &OrderResult,
    retries: usize,
) -> Result<TriggerDiagnostics> {
    let mut known_trade_ids = order_result.trade_ids.clone();
    let mut live_order = None;

    for attempt in 0..retries {
        if !order_result.order_id.is_empty() {
            live_order = preflight
                .trading_client
                .get_order(&order_result.order_id)
                .await?;
            if let Some(order) = &live_order {
                merge_trade_ids(&mut known_trade_ids, &order.associated_trade_ids);
            }
        }

        if trigger_has_positive_execution_evidence(
            order_result.status,
            live_order.as_ref().map(|order| order.status),
            live_order.as_ref().map(|order| order.size_matched),
            &known_trade_ids,
            Decimal::ZERO,
        ) || attempt + 1 == retries
        {
            break;
        }

        sleep(Duration::from_millis(TRIGGER_VERIFICATION_DELAY_MS)).await;
    }

    let direct_position = preflight
        .probe_truth
        .fetch_position(&scenario.market.condition_id)
        .await?;
    Ok(TriggerDiagnostics {
        live_order,
        known_trade_ids,
        direct_truth_acquired_shares: probe_owned_shares(
            &preflight.baseline_position,
            &direct_position,
            scenario.trigger.leg,
        ),
    })
}

fn build_trigger_summary(parts: TriggerSummaryParts) -> TriggerAcquisitionSummary {
    TriggerAcquisitionSummary {
        attempted: true,
        success: parts.success,
        order_id: parts.order_id,
        requested_shares: parts.requested_shares,
        resolved_trade_shares: parts.resolved_trade_shares,
        limit_price: parts.limit_price,
        snapshot_ask_price: parts.snapshot_ask_price,
        placement_status: parts.placement_status,
        trade_ids: parts.trade_ids,
        transaction_hashes: parts.transaction_hashes,
        placement_taking_shares: parts.placement_taking_shares,
        lookup_status: parts.lookup_status,
        lookup_matched_shares: parts.lookup_matched_shares,
        resolved_trade_id: parts.resolved_trade_id,
        ws_trade_observed: parts.ws_trade_observed,
        ws_connected_observed: parts.ws_connected_observed,
        matched_order_events: parts.matched_order_events,
        verification_attempts: parts.verification_attempts,
        failure_code: parts.failure_code,
        failure_reason: parts.failure_reason,
    }
}

fn should_extend_trigger_ws_observation(
    placed_trigger: &TriggerAcquisitionOutcome,
    runtime_report: &crate::runtime::live_engine::ScopedLiveRunnerReport,
) -> bool {
    let Some(order_result) = placed_trigger.order_result.as_ref() else {
        return false;
    };
    if !runtime_report.matched_trigger_trade_ids.is_empty() {
        return false;
    }
    trigger_order_result_has_positive_execution_evidence(order_result)
        || runtime_report.matched_trigger_order_events > 0
}

fn trigger_order_result_has_positive_execution_evidence(order_result: &OrderResult) -> bool {
    order_result.status == OrderStatus::Matched
        || !order_result.trade_ids.is_empty()
        || order_result.taking_amount.unwrap_or(Decimal::ZERO) > Decimal::ZERO
}

fn trigger_failure_details(
    placement_status: Option<String>,
    placement_taking_shares: Option<Decimal>,
    lookup_status: Option<String>,
    lookup_matched_shares: Option<Decimal>,
    trade_ids: &[String],
    direct_acquired_shares: Decimal,
    matched_order_events: usize,
    cancel_outcome: Option<&str>,
) -> (&'static str, String) {
    let placement_status_text = placement_status.as_deref().unwrap_or("missing");
    let lookup_status_text = lookup_status.as_deref().unwrap_or("missing");
    let lookup_matched = lookup_matched_shares.unwrap_or(Decimal::ZERO);
    let placement_status = placement_status
        .as_deref()
        .and_then(parse_lookup_order_status)
        .unwrap_or(OrderStatus::Invalid);
    let lookup_status = lookup_status.as_deref().and_then(parse_lookup_order_status);

    if trigger_has_positive_execution_evidence(
        placement_status,
        lookup_status,
        Some(lookup_matched),
        trade_ids,
        placement_taking_shares.unwrap_or(Decimal::ZERO),
    ) || direct_acquired_shares > Decimal::ZERO
        || matched_order_events > 0
    {
        return (
            "trigger_ws_not_observed",
            format!(
                "trigger_ws_not_observed: placement/external evidence existed but no matching user-stream trade reached the production path (placement_status={}, lookup_status={}, lookup_matched={}, direct_truth_acquired={}, trade_ids={}, matched_order_events={}, placement_taking_shares={}, cancel_outcome={})",
                placement_status_text,
                lookup_status_text,
                lookup_matched,
                direct_acquired_shares,
                trade_ids.join(","),
                matched_order_events,
                placement_taking_shares.unwrap_or(Decimal::ZERO),
                cancel_outcome.unwrap_or("missing")
            ),
        );
    }

    (
        "trigger_no_fill",
        format!(
            "trigger_no_fill: trigger acquisition created no matching user-stream trade or external execution evidence (placement_status={}, lookup_status={}, lookup_matched={}, trade_ids={}, direct_truth_acquired={}, placement_taking_shares={}, cancel_outcome={})",
            placement_status_text,
            lookup_status_text,
            lookup_matched,
            trade_ids.join(","),
            direct_acquired_shares,
            placement_taking_shares.unwrap_or(Decimal::ZERO),
            cancel_outcome.unwrap_or("missing")
        ),
    )
}

fn trigger_size_mismatch_details(
    requested_shares: Decimal,
    resolved_trade_shares: Decimal,
    cancel_outcome: Option<&str>,
) -> Option<(&'static str, String)> {
    if resolved_trade_shares == requested_shares {
        return None;
    }

    let cancel_outcome = cancel_outcome.unwrap_or("missing");
    if resolved_trade_shares < requested_shares {
        return Some((
            "trigger_partial_fill",
            format!(
                "trigger_partial_fill: requested {} normalized shares but matching user-stream trades resolved only {} (cancel_outcome={})",
                requested_shares, resolved_trade_shares, cancel_outcome
            ),
        ));
    }

    Some((
        "trigger_overshoot",
        format!(
            "trigger_overshoot: requested {} normalized shares but matching user-stream trades resolved {} (cancel_outcome={})",
            requested_shares, resolved_trade_shares, cancel_outcome
        ),
    ))
}

fn trigger_has_positive_execution_evidence(
    placement_status: OrderStatus,
    lookup_status: Option<OrderStatus>,
    lookup_matched_shares: Option<Decimal>,
    trade_ids: &[String],
    resolved_trade_shares: Decimal,
) -> bool {
    resolved_trade_shares > Decimal::ZERO
        || !trade_ids.is_empty()
        || placement_status == OrderStatus::Matched
        || lookup_status == Some(OrderStatus::Matched)
        || lookup_matched_shares.unwrap_or(Decimal::ZERO) > Decimal::ZERO
}

fn parse_lookup_order_status(status: &str) -> Option<OrderStatus> {
    match status {
        "Live" => Some(OrderStatus::Live),
        "Matched" => Some(OrderStatus::Matched),
        "Delayed" => Some(OrderStatus::Delayed),
        "Cancelled" => Some(OrderStatus::Cancelled),
        "Invalid" => Some(OrderStatus::Invalid),
        _ => None,
    }
}

fn merge_trade_ids(target: &mut Vec<String>, extra: &[String]) {
    for trade_id in extra {
        if !trade_id.is_empty() && !target.iter().any(|existing| existing == trade_id) {
            target.push(trade_id.clone());
        }
    }
}

fn token_id_for_leg(leg: QuoteLeg, market: &LiveProbeMarket) -> String {
    match leg {
        QuoteLeg::YesBid | QuoteLeg::YesAsk => market.yes_token_id.clone(),
        QuoteLeg::NoBid | QuoteLeg::NoAsk => market.no_token_id.clone(),
    }
}

fn opposite_token_id_for_leg(leg: QuoteLeg, market: &LiveProbeMarket) -> String {
    match leg {
        QuoteLeg::YesBid | QuoteLeg::YesAsk => market.no_token_id.clone(),
        QuoteLeg::NoBid | QuoteLeg::NoAsk => market.yes_token_id.clone(),
    }
}

fn outcome_for_leg(leg: QuoteLeg) -> &'static str {
    match leg {
        QuoteLeg::YesBid | QuoteLeg::YesAsk => "YES",
        QuoteLeg::NoBid | QuoteLeg::NoAsk => "NO",
    }
}

fn probe_owned_shares(baseline: &Position, current: &Position, leg: QuoteLeg) -> Decimal {
    match leg {
        QuoteLeg::YesBid | QuoteLeg::YesAsk => probe_owned_yes(baseline, current),
        QuoteLeg::NoBid | QuoteLeg::NoAsk => probe_owned_no(baseline, current),
    }
}

fn probe_owned_yes(baseline: &Position, current: &Position) -> Decimal {
    normalize_share_size((current.yes_size - baseline.yes_size).max(Decimal::ZERO))
}

fn probe_owned_no(baseline: &Position, current: &Position) -> Decimal {
    normalize_share_size((current.no_size - baseline.no_size).max(Decimal::ZERO))
}

fn probe_owned_pairs(baseline: &Position, current: &Position) -> Decimal {
    probe_owned_yes(baseline, current)
        .min(probe_owned_no(baseline, current))
        .floor()
}

// section: cleanup / result helpers

async fn run_probe_cleanup(
    scenario: &HedgeLiveProbeScenario,
    preflight: &LiveProbePreflight,
    engine: &LiveEngine,
    options: &LiveProbeRuntimeOptions,
    trigger_summary: &TriggerAcquisitionSummary,
) -> Result<CleanupSummary> {
    let tolerance = preflight.exposure_tolerance;
    let ambiguous_trigger_truth = trigger_summary_has_unconfirmed_fill_evidence(trigger_summary);
    let mut attempted_merge = false;
    let mut flatten_orders_placed = 0usize;
    let mut cleanup_notional_usdc = Decimal::ZERO;
    let mut current_engine_position = Position::new(scenario.market.condition_id.clone());
    let mut current_direct_position = Position::new(scenario.market.condition_id.clone());
    let mut resting_order_count = preflight.baseline_target_open_orders;
    let mut merge_succeeded = false;
    let mut merge_attempted_once = false;
    let mut flatten_attempted_yes = false;
    let mut flatten_attempted_no = false;
    let mut trigger_recovery_flatten_attempted = false;
    let mut failure_code = None;
    let mut failure_reason = None;
    let trigger_recovery_deadline = ambiguous_trigger_truth.then(|| {
        tokio::time::Instant::now()
            + Duration::from_millis(options.cleanup_trigger_recovery_timeout_ms)
    });

    for attempt in 0..=CLEANUP_VERIFICATION_RETRIES {
        if attempt > 0 {
            let poll_delay_ms =
                if ambiguous_trigger_truth && flatten_orders_placed == 0 && !merge_succeeded {
                    options.cleanup_trigger_recovery_poll_ms
                } else {
                    CLEANUP_VERIFICATION_DELAY_MS
                };
            sleep(Duration::from_millis(poll_delay_ms)).await;
        }

        match sync_cleanup_snapshot(scenario, preflight, engine).await {
            Ok(snapshot) => {
                current_engine_position = snapshot.engine_position;
                current_direct_position = snapshot.direct_position;
                resting_order_count = snapshot.resting_order_count;
            }
            Err(err) => {
                failure_code = Some("cleanup_truth_unconfirmed".to_string());
                failure_reason = Some(format!(
                    "cleanup_truth_unconfirmed: failed to refresh engine/direct truth after {} verification attempts: {}",
                    attempt + 1,
                    err
                ));
                continue;
            }
        }

        let engine_clean = is_clean_against_baseline(
            &preflight.baseline_position,
            &current_engine_position,
            preflight.baseline_target_open_orders,
            resting_order_count,
            tolerance,
        );
        let direct_clean = is_clean_against_baseline(
            &preflight.baseline_position,
            &current_direct_position,
            preflight.baseline_target_open_orders,
            resting_order_count,
            tolerance,
        );
        let clean_end_state = engine_clean && direct_clean;
        let direct_trigger_delta = probe_owned_shares(
            &preflight.baseline_position,
            &current_direct_position,
            scenario.trigger.leg,
        );
        if clean_end_state {
            if ambiguous_trigger_truth && flatten_orders_placed == 0 && !merge_succeeded {
                let recovery_window_active = trigger_recovery_deadline
                    .map(|deadline| tokio::time::Instant::now() < deadline)
                    .unwrap_or(false);
                if recovery_window_active {
                    failure_code = Some("cleanup_truth_unconfirmed".to_string());
                    failure_reason = Some(
                        "cleanup_truth_unconfirmed: awaiting late trigger-side inventory before cleanup can verify the market is flat"
                            .to_string(),
                    );
                    continue;
                }
                if !trigger_recovery_flatten_attempted {
                    if let Some(recovery_shares) = best_known_trigger_fill_shares(
                        trigger_summary,
                        &preflight.baseline_position,
                        &current_direct_position,
                        scenario.trigger.leg,
                    ) {
                        let cleanup_step_notional = match estimate_trigger_side_cleanup_notional(
                            &preflight.book_rest,
                            &scenario.market,
                            scenario.trigger.leg,
                            recovery_shares,
                        )
                        .await
                        {
                            Ok(value) => value,
                            Err(err) => {
                                return Ok(cleanup_failure_summary(
                                    attempted_merge,
                                    flatten_orders_placed,
                                    &current_engine_position,
                                    &current_direct_position,
                                    cleanup_notional_usdc,
                                    "cleanup_truth_unconfirmed",
                                    format!(
                                        "cleanup_truth_unconfirmed: failed to estimate trigger recovery cleanup notional: {}",
                                        err
                                    ),
                                    resting_order_count,
                                ));
                            }
                        };
                        if cleanup_notional_usdc + cleanup_step_notional
                            > scenario.safety.max_cleanup_notional_usdc
                        {
                            return Ok(cleanup_failure_summary(
                                attempted_merge,
                                flatten_orders_placed,
                                &current_engine_position,
                                &current_direct_position,
                                cleanup_notional_usdc + cleanup_step_notional,
                                "cleanup_notional_exceeded",
                                format!(
                                    "cleanup_notional_usdc {} exceeded safety.max_cleanup_notional_usdc {} during trigger recovery cleanup",
                                    cleanup_notional_usdc + cleanup_step_notional,
                                    scenario.safety.max_cleanup_notional_usdc
                                ),
                                resting_order_count,
                            ));
                        }
                        if let Err(err) = place_flatten_order(
                            &preflight.trading_client,
                            &token_id_for_leg(scenario.trigger.leg, &scenario.market),
                            recovery_shares,
                            &scenario.market,
                        )
                        .await
                        {
                            return Ok(cleanup_failure_summary(
                                attempted_merge,
                                flatten_orders_placed,
                                &current_engine_position,
                                &current_direct_position,
                                cleanup_notional_usdc + cleanup_step_notional,
                                "cleanup_flatten_failed",
                                format!(
                                    "cleanup_flattened_after_trigger_only: trigger recovery flatten failed: {}",
                                    err
                                ),
                                resting_order_count,
                            ));
                        }
                        match scenario.trigger.leg {
                            QuoteLeg::YesBid | QuoteLeg::YesAsk => flatten_attempted_yes = true,
                            QuoteLeg::NoBid | QuoteLeg::NoAsk => flatten_attempted_no = true,
                        }
                        trigger_recovery_flatten_attempted = true;
                        flatten_orders_placed += 1;
                        cleanup_notional_usdc += cleanup_step_notional;
                        let _ = engine.harness_refresh_balance().await;
                        continue;
                    }
                }
                return Ok(cleanup_failure_summary(
                    attempted_merge,
                    flatten_orders_placed,
                    &current_engine_position,
                    &current_direct_position,
                    cleanup_notional_usdc,
                    "cleanup_truth_unconfirmed",
                    "cleanup_truth_unconfirmed: trigger fill evidence never became numeric enough for out-of-band trigger recovery cleanup"
                        .to_string(),
                    resting_order_count,
                ));
            }
            if trigger_summary.order_id.is_some() {
                return verify_cleanup_stability_after_candidate_clean(
                    scenario,
                    preflight,
                    engine,
                    options,
                    attempted_merge,
                    flatten_orders_placed,
                    merge_succeeded,
                    cleanup_notional_usdc,
                    current_engine_position,
                    current_direct_position,
                    resting_order_count,
                )
                .await;
            }
            return Ok(CleanupSummary {
                attempted: true,
                attempted_merge,
                flatten_orders_placed,
                status: cleanup_success_status(merge_succeeded, flatten_orders_placed),
                success: true,
                failure_code: None,
                failure_reason: None,
                cleanup_notional_usdc,
                clean_end_state: true,
                final_yes_size: Some(current_engine_position.yes_size),
                final_no_size: Some(current_engine_position.no_size),
                final_direct_yes_size: Some(current_direct_position.yes_size),
                final_direct_no_size: Some(current_direct_position.no_size),
                resting_order_count,
            });
        }

        failure_code = Some(if engine_clean != direct_clean {
            "cleanup_truth_disagreed".to_string()
        } else {
            "cleanup_residual_inventory".to_string()
        });
        failure_reason = Some(format!(
            "{}: engine yes/no=({},{}) direct yes/no=({},{}) resting_orders={} after {} verification attempts",
            failure_code.as_deref().unwrap_or("cleanup_truth_unconfirmed"),
            current_engine_position.yes_size,
            current_engine_position.no_size,
            current_direct_position.yes_size,
            current_direct_position.no_size,
            resting_order_count,
            attempt + 1
        ));

        if ambiguous_trigger_truth && flatten_orders_placed == 0 && !merge_succeeded {
            if direct_trigger_delta > tolerance {
                let cleanup_step_notional = match estimate_trigger_side_cleanup_notional(
                    &preflight.book_rest,
                    &scenario.market,
                    scenario.trigger.leg,
                    direct_trigger_delta,
                )
                .await
                {
                    Ok(value) => value,
                    Err(err) => {
                        return Ok(cleanup_failure_summary(
                            attempted_merge,
                            flatten_orders_placed,
                            &current_engine_position,
                            &current_direct_position,
                            cleanup_notional_usdc,
                            "cleanup_truth_unconfirmed",
                            format!(
                                "cleanup_truth_unconfirmed: failed to estimate cleanup notional for observed trigger-side residue: {}",
                                err
                            ),
                            resting_order_count,
                        ));
                    }
                };
                if cleanup_notional_usdc + cleanup_step_notional
                    > scenario.safety.max_cleanup_notional_usdc
                {
                    return Ok(cleanup_failure_summary(
                        attempted_merge,
                        flatten_orders_placed,
                        &current_engine_position,
                        &current_direct_position,
                        cleanup_notional_usdc + cleanup_step_notional,
                        "cleanup_notional_exceeded",
                        format!(
                            "cleanup_notional_usdc {} exceeded safety.max_cleanup_notional_usdc {} during trigger-side recovery cleanup",
                            cleanup_notional_usdc + cleanup_step_notional,
                            scenario.safety.max_cleanup_notional_usdc
                        ),
                        resting_order_count,
                    ));
                }
                if let Err(err) = place_flatten_order(
                    &preflight.trading_client,
                    &token_id_for_leg(scenario.trigger.leg, &scenario.market),
                    direct_trigger_delta,
                    &scenario.market,
                )
                .await
                {
                    return Ok(cleanup_failure_summary(
                        attempted_merge,
                        flatten_orders_placed,
                        &current_engine_position,
                        &current_direct_position,
                        cleanup_notional_usdc + cleanup_step_notional,
                        "cleanup_flatten_failed",
                        format!(
                            "cleanup_flattened_after_trigger_only: observed trigger-side cleanup flatten failed: {}",
                            err
                        ),
                        resting_order_count,
                    ));
                }
                match scenario.trigger.leg {
                    QuoteLeg::YesBid | QuoteLeg::YesAsk => flatten_attempted_yes = true,
                    QuoteLeg::NoBid | QuoteLeg::NoAsk => flatten_attempted_no = true,
                }
                trigger_recovery_flatten_attempted = true;
                flatten_orders_placed += 1;
                cleanup_notional_usdc += cleanup_step_notional;
                let _ = engine.harness_refresh_balance().await;
                continue;
            }

            let recovery_window_active = trigger_recovery_deadline
                .map(|deadline| tokio::time::Instant::now() < deadline)
                .unwrap_or(false);
            if recovery_window_active {
                continue;
            }
        }

        let direct_probe_pairs =
            probe_owned_pairs(&preflight.baseline_position, &current_direct_position);
        let direct_delta_yes =
            probe_owned_yes(&preflight.baseline_position, &current_direct_position);
        let direct_delta_no =
            probe_owned_no(&preflight.baseline_position, &current_direct_position);

        if direct_probe_pairs > Decimal::ZERO && !merge_attempted_once {
            attempted_merge = true;
            merge_attempted_once = true;
            match options
                .merger
                .try_merge_pairs(engine, &scenario.market.condition_id, direct_probe_pairs)
                .await
            {
                Ok(Some(_)) => {
                    merge_succeeded = true;
                    let _ = engine.harness_refresh_balance().await;
                    continue;
                }
                Ok(None) => {}
                Err(err) => {
                    tracing::warn!(
                        condition_id = %scenario.market.condition_id,
                        error = %err,
                        "Probe merge attempt failed — falling back to explicit flatten"
                    );
                    failure_reason = Some(format!("probe merge attempt failed: {}", err));
                }
            }
        }

        if merge_succeeded && direct_probe_pairs > Decimal::ZERO {
            continue;
        }

        if direct_clean && !engine_clean {
            continue;
        }

        let flatten_yes_now = direct_delta_yes > tolerance && !flatten_attempted_yes;
        let flatten_no_now = direct_delta_no > tolerance && !flatten_attempted_no;
        if !flatten_yes_now && !flatten_no_now {
            continue;
        }

        let cleanup_step_notional = match estimate_cleanup_notional(
            &preflight.book_rest,
            &scenario.market,
            if flatten_yes_now {
                direct_delta_yes
            } else {
                Decimal::ZERO
            },
            if flatten_no_now {
                direct_delta_no
            } else {
                Decimal::ZERO
            },
        )
        .await
        {
            Ok(value) => value,
            Err(err) => {
                return Ok(cleanup_failure_summary(
                    attempted_merge,
                    flatten_orders_placed,
                    &current_engine_position,
                    &current_direct_position,
                    cleanup_notional_usdc,
                    "cleanup_truth_unconfirmed",
                    format!(
                        "cleanup_truth_unconfirmed: failed to estimate cleanup notional: {}",
                        err
                    ),
                    resting_order_count,
                ));
            }
        };
        if cleanup_notional_usdc + cleanup_step_notional > scenario.safety.max_cleanup_notional_usdc
        {
            return Ok(cleanup_failure_summary(
                attempted_merge,
                flatten_orders_placed,
                &current_engine_position,
                &current_direct_position,
                cleanup_notional_usdc + cleanup_step_notional,
                "cleanup_notional_exceeded",
                format!(
                    "cleanup_notional_usdc {} exceeded safety.max_cleanup_notional_usdc {}",
                    cleanup_notional_usdc + cleanup_step_notional,
                    scenario.safety.max_cleanup_notional_usdc
                ),
                resting_order_count,
            ));
        }

        if flatten_yes_now {
            if let Err(err) = place_flatten_order(
                &preflight.trading_client,
                &scenario.market.yes_token_id,
                direct_delta_yes,
                &scenario.market,
            )
            .await
            {
                return Ok(cleanup_failure_summary(
                    attempted_merge,
                    flatten_orders_placed,
                    &current_engine_position,
                    &current_direct_position,
                    cleanup_notional_usdc + cleanup_step_notional,
                    "cleanup_flatten_failed",
                    format!(
                        "cleanup_flattened_after_trigger_only: cleanup YES flatten failed: {}",
                        err
                    ),
                    resting_order_count,
                ));
            }
            flatten_attempted_yes = true;
            flatten_orders_placed += 1;
        }
        if flatten_no_now {
            if let Err(err) = place_flatten_order(
                &preflight.trading_client,
                &scenario.market.no_token_id,
                direct_delta_no,
                &scenario.market,
            )
            .await
            {
                return Ok(cleanup_failure_summary(
                    attempted_merge,
                    flatten_orders_placed,
                    &current_engine_position,
                    &current_direct_position,
                    cleanup_notional_usdc + cleanup_step_notional,
                    "cleanup_flatten_failed",
                    format!(
                        "cleanup_flattened_after_trigger_only: cleanup NO flatten failed: {}",
                        err
                    ),
                    resting_order_count,
                ));
            }
            flatten_attempted_no = true;
            flatten_orders_placed += 1;
        }

        cleanup_notional_usdc += cleanup_step_notional;
        let _ = engine.harness_refresh_balance().await;
    }

    Ok(cleanup_failure_summary(
        attempted_merge,
        flatten_orders_placed,
        &current_engine_position,
        &current_direct_position,
        cleanup_notional_usdc,
        failure_code
            .as_deref()
            .unwrap_or("cleanup_truth_unconfirmed"),
        failure_reason.unwrap_or_else(|| {
            "cleanup_truth_unconfirmed: cleanup retries exhausted before engine and direct truth agreed on a clean end state".to_string()
        }),
        resting_order_count,
    ))
}

async fn verify_cleanup_stability_after_candidate_clean(
    scenario: &HedgeLiveProbeScenario,
    preflight: &LiveProbePreflight,
    engine: &LiveEngine,
    options: &LiveProbeRuntimeOptions,
    attempted_merge: bool,
    flatten_orders_placed: usize,
    merge_succeeded: bool,
    cleanup_notional_usdc: Decimal,
    initial_engine_position: Position,
    initial_direct_position: Position,
    initial_resting_order_count: usize,
) -> Result<CleanupSummary> {
    let tolerance = preflight.exposure_tolerance;
    let mut current_engine_position = initial_engine_position;
    let mut current_direct_position = initial_direct_position;
    let mut resting_order_count = initial_resting_order_count;

    for verification_pass in 0..options.cleanup_stabilization_retries_after_trigger {
        sleep(Duration::from_millis(
            options.cleanup_stabilization_delay_ms,
        ))
        .await;

        let snapshot = match sync_cleanup_snapshot(scenario, preflight, engine).await {
            Ok(snapshot) => snapshot,
            Err(err) => {
                return Ok(cleanup_failure_summary(
                    attempted_merge,
                    flatten_orders_placed,
                    &current_engine_position,
                    &current_direct_position,
                    cleanup_notional_usdc,
                    "cleanup_truth_unconfirmed",
                    format!(
                        "cleanup_truth_unconfirmed: stabilization refresh failed on verification pass {}: {}",
                        verification_pass + 1,
                        err
                    ),
                    resting_order_count,
                ));
            }
        };

        current_engine_position = snapshot.engine_position;
        current_direct_position = snapshot.direct_position;
        resting_order_count = snapshot.resting_order_count;

        let engine_clean = is_clean_against_baseline(
            &preflight.baseline_position,
            &current_engine_position,
            preflight.baseline_target_open_orders,
            resting_order_count,
            tolerance,
        );
        let direct_clean = is_clean_against_baseline(
            &preflight.baseline_position,
            &current_direct_position,
            preflight.baseline_target_open_orders,
            resting_order_count,
            tolerance,
        );

        if !(engine_clean && direct_clean) {
            let failure_code = if engine_clean != direct_clean {
                "cleanup_truth_disagreed"
            } else {
                "cleanup_residual_inventory"
            };
            return Ok(cleanup_failure_summary(
                attempted_merge,
                flatten_orders_placed,
                &current_engine_position,
                &current_direct_position,
                cleanup_notional_usdc,
                failure_code,
                format!(
                    "{}: cleanup became dirty again during post-clean stabilization on verification pass {} with engine yes/no=({},{}) direct yes/no=({},{}) resting_orders={}",
                    failure_code,
                    verification_pass + 1,
                    current_engine_position.yes_size,
                    current_engine_position.no_size,
                    current_direct_position.yes_size,
                    current_direct_position.no_size,
                    resting_order_count
                ),
                resting_order_count,
            ));
        }
    }

    Ok(CleanupSummary {
        attempted: true,
        attempted_merge,
        flatten_orders_placed,
        status: cleanup_success_status(merge_succeeded, flatten_orders_placed),
        success: true,
        failure_code: None,
        failure_reason: None,
        cleanup_notional_usdc,
        clean_end_state: true,
        final_yes_size: Some(current_engine_position.yes_size),
        final_no_size: Some(current_engine_position.no_size),
        final_direct_yes_size: Some(current_direct_position.yes_size),
        final_direct_no_size: Some(current_direct_position.no_size),
        resting_order_count,
    })
}

fn cleanup_failure_summary(
    attempted_merge: bool,
    flatten_orders_placed: usize,
    current_engine_position: &Position,
    current_direct_position: &Position,
    cleanup_notional_usdc: Decimal,
    failure_code: &str,
    failure_reason: String,
    resting_order_count: usize,
) -> CleanupSummary {
    CleanupSummary {
        attempted: true,
        attempted_merge,
        flatten_orders_placed,
        status: None,
        success: false,
        failure_code: Some(failure_code.to_string()),
        failure_reason: Some(failure_reason),
        cleanup_notional_usdc,
        clean_end_state: false,
        final_yes_size: Some(current_engine_position.yes_size),
        final_no_size: Some(current_engine_position.no_size),
        final_direct_yes_size: Some(current_direct_position.yes_size),
        final_direct_no_size: Some(current_direct_position.no_size),
        resting_order_count,
    }
}

fn cleanup_success_status(
    merge_succeeded: bool,
    flatten_orders_placed: usize,
) -> Option<CleanupStatus> {
    if flatten_orders_placed > 0 {
        Some(CleanupStatus::Flattened)
    } else if merge_succeeded {
        Some(CleanupStatus::Merged)
    } else {
        None
    }
}

fn best_known_trigger_fill_shares(
    trigger_summary: &TriggerAcquisitionSummary,
    baseline_position: &Position,
    current_direct_position: &Position,
    leg: QuoteLeg,
) -> Option<Decimal> {
    let direct_shares = probe_owned_shares(baseline_position, current_direct_position, leg);
    if direct_shares > Decimal::ZERO {
        return Some(direct_shares);
    }

    let lookup_matched = trigger_summary
        .lookup_matched_shares
        .unwrap_or(Decimal::ZERO);
    if lookup_matched > Decimal::ZERO {
        return Some(normalize_share_size(lookup_matched));
    }

    let taking_amount = trigger_summary
        .placement_taking_shares
        .unwrap_or(Decimal::ZERO);
    if taking_amount > Decimal::ZERO {
        return Some(normalize_share_size(taking_amount));
    }

    None
}

fn trigger_summary_has_unconfirmed_fill_evidence(
    trigger_summary: &TriggerAcquisitionSummary,
) -> bool {
    !trigger_summary.success
        && (trigger_summary.resolved_trade_shares > Decimal::ZERO
            || trigger_summary
                .lookup_matched_shares
                .unwrap_or(Decimal::ZERO)
                > Decimal::ZERO
            || !trigger_summary.trade_ids.is_empty()
            || trigger_summary
                .placement_taking_shares
                .unwrap_or(Decimal::ZERO)
                > Decimal::ZERO
            || trigger_summary.matched_order_events > 0
            || trigger_summary
                .placement_status
                .as_deref()
                .and_then(parse_lookup_order_status)
                .is_some_and(|status| status == OrderStatus::Matched))
}

async fn sync_cleanup_snapshot(
    scenario: &HedgeLiveProbeScenario,
    preflight: &LiveProbePreflight,
    engine: &LiveEngine,
) -> Result<CleanupVerificationSnapshot> {
    engine.harness_sync_positions().await?;
    let engine_position = engine
        .harness_get_position(&scenario.market.condition_id)
        .await
        .unwrap_or_else(|| Position::new(scenario.market.condition_id.clone()));
    let direct_position = preflight
        .probe_truth
        .fetch_position(&scenario.market.condition_id)
        .await?;
    let resting_order_count =
        count_target_open_orders(&preflight.trading_client, &scenario.market.condition_id).await?;
    Ok(CleanupVerificationSnapshot {
        engine_position,
        direct_position,
        resting_order_count,
    })
}

async fn estimate_cleanup_notional(
    book_rest: &BookRestClient,
    market: &LiveProbeMarket,
    delta_yes: Decimal,
    delta_no: Decimal,
) -> Result<Decimal> {
    let (yes_book, no_book) = book_rest
        .fetch_both_books(&market.yes_token_id, &market.no_token_id)
        .await?;
    let yes_bid = best_bid_price(&yes_book).unwrap_or(Decimal::ZERO);
    let no_bid = best_bid_price(&no_book).unwrap_or(Decimal::ZERO);
    Ok(delta_yes * yes_bid + delta_no * no_bid)
}

async fn estimate_trigger_side_cleanup_notional(
    book_rest: &BookRestClient,
    market: &LiveProbeMarket,
    leg: QuoteLeg,
    shares: Decimal,
) -> Result<Decimal> {
    match leg {
        QuoteLeg::YesBid | QuoteLeg::YesAsk => {
            estimate_cleanup_notional(book_rest, market, shares, Decimal::ZERO).await
        }
        QuoteLeg::NoBid | QuoteLeg::NoAsk => {
            estimate_cleanup_notional(book_rest, market, Decimal::ZERO, shares).await
        }
    }
}

async fn place_flatten_order(
    trading_client: &Arc<TradingClient>,
    token_id: &str,
    size: Decimal,
    market: &LiveProbeMarket,
) -> Result<()> {
    let request = OrderRequest {
        token_id: token_id.to_string(),
        price: Decimal::new(1, 2),
        size: normalize_share_size(size),
        amount_kind: OrderAmountKind::Shares,
        side: Side::Sell,
        order_type: OrderType::FOK,
        post_only: false,
        neg_risk: market.neg_risk,
        tick_size: market.tick_size.clone(),
    };
    trading_client.place_order(&request).await?;
    Ok(())
}

async fn count_target_open_orders(
    trading_client: &Arc<TradingClient>,
    condition_id: &str,
) -> Result<usize> {
    Ok(trading_client
        .get_open_orders(Some(condition_id))
        .await?
        .len())
}

fn is_clean_against_baseline(
    baseline: &Position,
    current: &Position,
    baseline_open_orders: usize,
    current_open_orders: usize,
    tolerance: Decimal,
) -> bool {
    let yes_delta = (current.yes_size - baseline.yes_size).abs();
    let no_delta = (current.no_size - baseline.no_size).abs();
    yes_delta <= tolerance && no_delta <= tolerance && current_open_orders <= baseline_open_orders
}

fn empty_observed_outcome() -> ObservedHedgeOutcome {
    ObservedHedgeOutcome {
        result_status: None,
        hedge_side: None,
        planned_hedge_shares: None,
        planned_sellback_shares: None,
        hedge_leg_status: None,
        sellback_leg_status: None,
        hedge_price: None,
        sellback_price: None,
        post_sync_yes_size: None,
        post_sync_no_size: None,
        post_sync_net_exposure: None,
        neutrality_residual_exposure: None,
        halted: false,
        request_log: Vec::new(),
    }
}

fn collect_critical_events(events: &[EventEnvelope]) -> Vec<LiveProbeEventSummary> {
    events
        .iter()
        .filter(|event| event.priority == Priority::Critical)
        .map(|event| LiveProbeEventSummary {
            event_type: event.event_type.to_string(),
        })
        .collect()
}

fn compare_expected_to_observed(
    expected: &LiveProbeExpected,
    observed: &ObservedHedgeOutcome,
    actual_success: bool,
) -> Vec<String> {
    let mut mismatches = Vec::new();

    if expected.success != actual_success {
        mismatches.push(format!(
            "success mismatch: expected {}, got {}",
            expected.success, actual_success
        ));
    }
    if expected.halted != observed.halted {
        mismatches.push(format!(
            "halted mismatch: expected {}, got {}",
            expected.halted, observed.halted
        ));
    }
    if expected.hedge_side != observed.hedge_side {
        mismatches.push(format!(
            "hedge_side mismatch: expected {:?}, got {:?}",
            expected.hedge_side, observed.hedge_side
        ));
    }
    compare_option_value(
        &mut mismatches,
        "result_status",
        &expected.result_status,
        &observed.result_status,
    );
    compare_option_value(
        &mut mismatches,
        "hedge_leg_status",
        &expected.hedge_leg_status,
        &observed.hedge_leg_status,
    );
    compare_option_value(
        &mut mismatches,
        "sellback_leg_status",
        &expected.sellback_leg_status,
        &observed.sellback_leg_status,
    );
    compare_max_decimal(
        &mut mismatches,
        "planned_hedge_shares",
        expected.max_planned_hedge_shares,
        observed.planned_hedge_shares,
        false,
    );
    compare_max_decimal(
        &mut mismatches,
        "planned_sellback_shares",
        expected.max_planned_sellback_shares,
        observed.planned_sellback_shares,
        true,
    );
    compare_max_decimal(
        &mut mismatches,
        "post_sync_net_exposure",
        expected.max_post_sync_net_exposure,
        observed.post_sync_net_exposure.map(|value| value.abs()),
        false,
    );

    mismatches
}

fn compare_expected_critical_event_types(
    expected: &[String],
    actual: &[LiveProbeEventSummary],
) -> Vec<String> {
    let mut mismatches = Vec::new();
    let mut cursor = 0usize;

    for wanted in expected {
        let mut matched = false;
        while let Some(candidate) = actual.get(cursor) {
            cursor += 1;
            if candidate.event_type == *wanted {
                matched = true;
                break;
            }
        }
        if !matched {
            mismatches.push(format!(
                "critical_event_types missing subsequence item: {}",
                wanted
            ));
            break;
        }
    }

    mismatches
}

fn compare_safety_to_observed(
    safety: &LiveProbeSafety,
    plan: &LiveProbePlan,
    observed: &ObservedHedgeOutcome,
) -> Vec<String> {
    let mut mismatches = Vec::new();
    if plan.planned_hedge_shares > safety.max_planned_hedge_shares {
        mismatches.push(format!(
            "safety violation: planned_hedge_shares {} exceeded safety.max_planned_hedge_shares {}",
            plan.planned_hedge_shares, safety.max_planned_hedge_shares
        ));
    }
    if plan.planned_sellback_shares > safety.max_planned_sellback_shares {
        mismatches.push(format!(
            "safety violation: planned_sellback_shares {} exceeded safety.max_planned_sellback_shares {}",
            plan.planned_sellback_shares, safety.max_planned_sellback_shares
        ));
    }
    if plan.planned_hedge_notional_usdc > safety.max_planned_hedge_notional_usdc {
        mismatches.push(format!(
            "safety violation: planned_hedge_notional_usdc {} exceeded safety.max_planned_hedge_notional_usdc {}",
            plan.planned_hedge_notional_usdc, safety.max_planned_hedge_notional_usdc
        ));
    }
    if let Some(post_sync_net_exposure) = observed.post_sync_net_exposure {
        if post_sync_net_exposure.abs() > safety.max_post_sync_net_exposure {
            mismatches.push(format!(
                "safety violation: post_sync_net_exposure {} exceeded safety.max_post_sync_net_exposure {}",
                post_sync_net_exposure.abs(),
                safety.max_post_sync_net_exposure
            ));
        }
    }
    mismatches
}

fn compare_meta_to_runtime(
    trigger: &TriggerAcquisitionSummary,
    runtime_report: &crate::runtime::live_engine::ScopedLiveRunnerReport,
) -> Vec<String> {
    let mut failures = Vec::new();
    if !runtime_report.used_normal_post_hedge_mode {
        failures.push(
            "runner_deviated_from_normal_post_hedge_mode: Layer 3 did not use the normal production post-hedge path"
                .to_string(),
        );
    }
    if trigger.failure_code.as_deref() == Some("trigger_ws_not_observed") {
        failures.push(format!(
            "trigger_ws_not_observed: no matching user-stream trigger trade reached the production path ({})",
            trigger
                .failure_reason
                .clone()
                .unwrap_or_else(|| "unknown trigger failure".to_string())
        ));
    }
    failures
}

fn compare_trigger_to_standard(trigger: &TriggerAcquisitionSummary) -> Vec<String> {
    let mut mismatches = Vec::new();
    match trigger.failure_code.as_deref() {
        Some("trigger_partial_fill") | Some("trigger_overshoot") => mismatches.push(format!(
            "{}: requested {} normalized trigger shares but observed {} on the real user-stream trigger path",
            trigger.failure_code.as_deref().unwrap_or("trigger_size_mismatch"),
            trigger.requested_shares,
            trigger.resolved_trade_shares
        )),
        _ => {}
    }
    mismatches
}

fn compare_option_value<T>(
    mismatches: &mut Vec<String>,
    field: &str,
    expected: &Option<T>,
    actual: &Option<T>,
) where
    T: PartialEq + std::fmt::Debug,
{
    if expected != actual {
        mismatches.push(format!(
            "{} mismatch: expected {:?}, got {:?}",
            field, expected, actual
        ));
    }
}

fn compare_max_decimal(
    mismatches: &mut Vec<String>,
    field: &str,
    expected_max: Option<Decimal>,
    actual: Option<Decimal>,
    treat_missing_as_zero: bool,
) {
    match (expected_max, actual) {
        (Some(max), Some(value)) if value > max => mismatches.push(format!(
            "{} exceeded max: expected <= {}, got {}",
            field, max, value
        )),
        (Some(max), None) if treat_missing_as_zero && Decimal::ZERO <= max => {}
        (Some(_), None) => mismatches.push(format!(
            "{} missing from observed outcome while max bound was provided",
            field
        )),
        _ => {}
    }
}

pub fn print_report(result: &HedgeLiveProbeResult) {
    println!("=== Hedge Live Probe: {} ===", result.scenario_name);
    println!(
        "Overall result: {}",
        if result.passed { "PASS" } else { "FAIL" }
    );
    println!(
        "Meta pass: {} | Standard pass: {} | Cleanup result: {}",
        result.meta_pass,
        result.standard_pass,
        if result.cleanup.success {
            "success"
        } else {
            "failure"
        }
    );
    println!(
        "Expected success: {}, Actual: {}",
        result.expected_success, result.actual_success
    );
    println!(
        "Preflight: trigger_leg={:?} trigger={} trigger_limit={} hedge={} sellback={} hedge_notional={}",
        result.preflight.trigger_leg,
        result.preflight.trigger_shares,
        result.preflight.trigger_limit_price,
        result.preflight.planned_hedge_shares,
        result.preflight.planned_sellback_shares,
        result.preflight.planned_hedge_notional_usdc
    );
    println!(
        "Trigger: success={} requested={} resolved_trade_shares={} order_id={:?} placement_status={:?} trade_ids={:?} lookup_status={:?} lookup_matched={:?} resolved_trade_id={:?} ws_trade_observed={} ws_connected_observed={} matched_order_events={} verification_attempts={} failure_code={:?}",
        result.trigger.success,
        result.trigger.requested_shares,
        result.trigger.resolved_trade_shares,
        result.trigger.order_id,
        result.trigger.placement_status,
        result.trigger.trade_ids,
        result.trigger.lookup_status,
        result.trigger.lookup_matched_shares,
        result.trigger.resolved_trade_id,
        result.trigger.ws_trade_observed,
        result.trigger.ws_connected_observed,
        result.trigger.matched_order_events,
        result.trigger.verification_attempts,
        result.trigger.failure_code
    );
    if let Some(failure_reason) = &result.trigger.failure_reason {
        println!("Trigger failure: {}", failure_reason);
    }
    println!(
        "Cleanup: attempted={} success={} status={:?} clean_end_state={} resting_orders={} engine_yes={:?} engine_no={:?} direct_yes={:?} direct_no={:?} failure_code={:?}",
        result.cleanup.attempted,
        result.cleanup.success,
        result.cleanup.status,
        result.cleanup.clean_end_state,
        result.cleanup.resting_order_count,
        result.cleanup.final_yes_size,
        result.cleanup.final_no_size,
        result.cleanup.final_direct_yes_size,
        result.cleanup.final_direct_no_size,
        result.cleanup.failure_code
    );
    if let Some(failure_reason) = &result.cleanup.failure_reason {
        println!("Cleanup failure: {}", failure_reason);
    }
    println!(
        "Observed: result_status={:?}, hedge_side={:?}, planned_hedge_shares={:?}, planned_sellback_shares={:?}, post_sync_net_exposure={:?}",
        result.observed.result_status,
        result.observed.hedge_side,
        result.observed.planned_hedge_shares,
        result.observed.planned_sellback_shares,
        result.observed.post_sync_net_exposure
    );
    if !result.meta_failures.is_empty() {
        println!("Meta failures:");
        for mismatch in &result.meta_failures {
            println!("- {}", mismatch);
        }
    }
    if !result.standard_mismatches.is_empty() {
        println!("Standard mismatches:");
        for mismatch in &result.standard_mismatches {
            println!("- {}", mismatch);
        }
    }
}

fn print_warning_banner(
    scenario: &HedgeLiveProbeScenario,
    plan: &LiveProbePlan,
    ctf_merge_enabled: bool,
) {
    println!("\n!!! LIVE PAIRED HEDGE PROBE WILL PLACE REAL ORDERS !!!");
    println!("market: {}", scenario.market.condition_id);
    println!(
        "question: {}",
        scenario.market.question.as_deref().unwrap_or("<unknown>")
    );
    println!(
        "trigger: leg={:?} shares={} best_ask={} limit={} notional={}",
        plan.trigger_leg,
        plan.trigger_shares,
        plan.trigger_snapshot_ask_price,
        plan.trigger_limit_price,
        plan.trigger_notional_usdc
    );
    println!(
        "hedge preflight: side={:?} hedge_shares={} sellback_shares={} hedge_notional={} available_hedge_usdc_after_trigger={}",
        plan.hedge_side,
        plan.planned_hedge_shares,
        plan.planned_sellback_shares,
        plan.planned_hedge_notional_usdc,
        plan.available_hedge_usdc_after_trigger
    );
    println!(
        "cleanup mode: out_of_band merge_or_flatten after verdict (ctf_merge_enabled={})\n",
        ctf_merge_enabled
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use serde_json::json;

    #[test]
    fn probe_raw_position_accepts_decimal_strings_and_numbers() {
        let from_string: ProbeRawPosition = serde_json::from_value(json!({
            "conditionId": "condition-1",
            "size": "5.25",
            "avgPrice": "0.68",
            "outcome": "YES"
        }))
        .expect("string decimals should parse");
        let from_number: ProbeRawPosition = serde_json::from_value(json!({
            "conditionId": "condition-1",
            "size": 5.25,
            "avgPrice": 0.68,
            "outcome": "YES"
        }))
        .expect("numeric decimals should parse");

        assert_eq!(from_string.size, Some(dec!(5.25)));
        assert_eq!(from_string.avg_price, Some(dec!(0.68)));
        assert_eq!(from_number.size, Some(dec!(5.25)));
        assert_eq!(from_number.avg_price, Some(dec!(0.68)));
    }

    #[test]
    fn probe_raw_position_rejects_invalid_decimal_values() {
        let parsed = serde_json::from_value::<ProbeRawPosition>(json!({
            "conditionId": "condition-1",
            "size": "not-a-decimal",
            "avgPrice": "0.68",
            "outcome": "YES"
        }));

        assert!(parsed.is_err());
    }
}
