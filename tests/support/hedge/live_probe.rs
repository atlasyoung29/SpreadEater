use super::*;

use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use spreadeater_core::payloads::{
    HedgeDecisionPayload, HedgeExitPathPayload, HedgeIntentPayload, HedgeResultPayload,
    OrderSubmittedPayload,
};
use spreadeater_core::{EventEnvelope, EventType, Priority};
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout, Duration, Instant};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use uuid::Uuid;

use crate::models::{LiveOrder, OrderAmountKind, OrderRequest, OrderStatus, OrderType, Outcome};
use crate::monitor::ErrorLogger;
use crate::trading::user_stream::{
    build_user_ws_auth_message, user_ws_text_is_pong, USER_WS_HEARTBEAT_TEXT,
};

pub(super) const LIVE_PROBE_ARM_ENV: &str = "SPREADEATER_HEDGE_LIVE_PROBE_ARM";
pub(super) const LIVE_PROBE_ARM_TOKEN: &str = "I_UNDERSTAND_REAL_ORDERS";
const LIVE_PROBE_SCENARIO_ENV: &str = "SPREADEATER_HEDGE_LIVE_PROBE_SCENARIO";
const LIVE_MERGE_PROBE_SCENARIO_ENV: &str = "SPREADEATER_MERGE_LIVE_PROBE_SCENARIO";
const LIVE_PROBE_USER_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/user";
const SELLBACK_CONFIRMATION_LOOKUP_ATTEMPTS: usize = 4;
const SELLBACK_CONFIRMATION_LOOKUP_RETRY_MS: u64 = 750;
const MERGE_PROBE_ORDER_FILL_WAIT_MS: u64 = 500;
const MERGE_PROBE_MIN_MARKETABLE_BUY_NOTIONAL_USDC: Decimal = Decimal::ONE;
const LIVE_PROBE_MIN_CLEANUP_WAIT_SECS: u64 = 3;

fn default_live_probe_timeout_secs() -> u64 {
    60
}

fn default_require_clean_market() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HedgeLiveProbeScenario {
    name: String,
    description: String,
    market: LiveProbeMarket,
    trigger: LiveProbeTrigger,
    safety: LiveProbeSafety,
    expected: LiveProbeExpected,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LiveProbeMarket {
    condition_id: String,
    #[serde(default)]
    question: Option<String>,
    yes_token_id: String,
    no_token_id: String,
    tick_size: String,
    neg_risk: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LiveProbeTrigger {
    leg: QuoteLeg,
    #[serde(with = "rust_decimal::serde::str")]
    shares: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    max_trigger_limit_price: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LiveProbeSafety {
    #[serde(default = "default_require_clean_market")]
    require_clean_market: bool,
    #[serde(with = "rust_decimal::serde::str")]
    max_planned_hedge_shares: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    max_planned_sellback_shares: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    max_planned_hedge_notional_usdc: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    max_post_sync_net_exposure: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    max_trigger_notional_usdc: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    max_cleanup_notional_usdc: Decimal,
    #[serde(default = "default_live_probe_timeout_secs")]
    timeout_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LiveProbeExpected {
    success: bool,
    halted: bool,
    #[serde(default)]
    hedge_side: Option<Side>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MergeLiveProbeScenario {
    name: String,
    description: String,
    market: LiveProbeMarket,
    acquisition: MergeLiveProbeAcquisition,
    safety: MergeLiveProbeSafety,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MergeLiveProbeAcquisition {
    #[serde(with = "rust_decimal::serde::str")]
    shares: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    yes_max_limit_price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    no_max_limit_price: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MergeLiveProbeSafety {
    #[serde(default = "default_require_clean_market")]
    require_clean_market: bool,
    #[serde(with = "rust_decimal::serde::str")]
    max_yes_notional_usdc: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    max_no_notional_usdc: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    max_cleanup_notional_usdc: Decimal,
    #[serde(default = "default_live_probe_timeout_secs")]
    timeout_secs: u64,
}

#[derive(Debug, Serialize)]
struct HedgeLiveProbeResult {
    scenario_name: String,
    meta_pass: bool,
    standard_pass: bool,
    cleanup_pass: bool,
    trigger_order_id: Option<String>,
    trigger_trade_id: Option<String>,
    decision_audit_status: DecisionAuditStatus,
    decision_audit_reason: String,
    flow_status: FlowStatus,
    production_decision_mode: Option<String>,
    production_decision_reason_code: Option<String>,
    production_exit_path_status: Option<String>,
    merge_status: Option<String>,
    merge_failure_reason: Option<String>,
    fallback_status: Option<String>,
    fallback_failure_reason: Option<String>,
    truth_reconciliation_status: Option<String>,
    truth_reconciliation_reason: Option<String>,
    production_sellback_confirmation_status: Option<String>,
    production_sellback_confirmation_reason: Option<String>,
    truth_reconciliation_warning_status: Option<String>,
    truth_reconciliation_warning_reason: Option<String>,
    post_decision_direct_yes_size: Option<Decimal>,
    post_decision_direct_no_size: Option<Decimal>,
    post_decision_direct_observed_for_secs: Option<u64>,
    cleanup_direct_yes_size: Option<Decimal>,
    cleanup_direct_no_size: Option<Decimal>,
    cleanup_direct_observed_for_secs: Option<u64>,
    production_hedge_cancel_status: Option<String>,
    production_hedge_cancel_reason: Option<String>,
    production_hedge_lookup_status: Option<String>,
    production_hedge_lookup_matched_shares: Option<Decimal>,
    production_hedge_lookup_error: Option<String>,
    production_hedge_trade_ids: Option<Vec<String>>,
    production_sellback_response_status: Option<String>,
    production_sellback_lookup_status: Option<String>,
    production_sellback_lookup_matched_shares: Option<Decimal>,
    production_sellback_lookup_error: Option<String>,
    production_sellback_trade_ids: Option<Vec<String>>,
    hedge_lookup_status: Option<String>,
    hedge_lookup_matched_shares: Option<Decimal>,
    planned_hedge_shares: Option<Decimal>,
    planned_sellback_shares: Option<Decimal>,
    hedge_leg_status: Option<String>,
    sellback_leg_status: Option<String>,
    hedge_verification_state: Option<String>,
    post_sync_net_exposure: Option<Decimal>,
    merge_observed: bool,
    fallback_asks_observed: bool,
    cleanup_status: String,
}

#[derive(Debug, Serialize)]
struct MergeLiveProbeResult {
    scenario_name: String,
    meta_pass: bool,
    standard_pass: bool,
    cleanup_pass: bool,
    ctf_merge_configured: bool,
    yes_order_id: Option<String>,
    no_order_id: Option<String>,
    yes_lookup_status: Option<String>,
    no_lookup_status: Option<String>,
    yes_matched_shares: Option<Decimal>,
    no_matched_shares: Option<Decimal>,
    yes_trade_ids: Option<Vec<String>>,
    no_trade_ids: Option<Vec<String>>,
    acquired_pair_shares: Option<Decimal>,
    engine_pair_shares_before_exit: Option<Decimal>,
    pair_exit_status: Option<String>,
    merge_tx_hash: Option<String>,
    merge_failure_reason: Option<String>,
    fallback_asks_attempted: Option<bool>,
    fallback_ask_count: Option<u64>,
    fallback_failure_reason: Option<String>,
    pre_exit_collateral_usdc: Option<Decimal>,
    post_exit_collateral_usdc: Option<Decimal>,
    collateral_delta_usdc: Option<Decimal>,
    post_exit_direct_yes_size: Option<Decimal>,
    post_exit_direct_no_size: Option<Decimal>,
    post_exit_direct_observed_for_secs: Option<u64>,
    failure_reason: Option<String>,
    cleanup_status: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum DecisionAuditStatus {
    Confirmed,
    Inconclusive,
    Failed,
    NotApplicable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum FlowStatus {
    SellbackCompleted,
    MergeCompleted,
    FallbackAsksPlaced,
    PairLeftIdle,
    DirectionalResidual,
    FlowInconclusive,
}

#[derive(Debug, Clone)]
struct DecisionAuditVerdict {
    status: DecisionAuditStatus,
    reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ObservedResolutionSplit {
    hedge_shares: Decimal,
    sellback_shares: Decimal,
}

#[derive(Debug, Clone)]
struct BookAuditSnapshot {
    yes_book: Option<OrderBookSnapshot>,
    no_book: Option<OrderBookSnapshot>,
    max_hedge_usdc: Decimal,
    note: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct DirectMarketPositionTruth {
    yes_size: Decimal,
    no_size: Decimal,
}

impl DirectMarketPositionTruth {
    fn is_flat(&self) -> bool {
        self.yes_size <= Decimal::ZERO && self.no_size <= Decimal::ZERO
    }

    fn matches(&self, other: &Self) -> bool {
        self.yes_size == other.yes_size && self.no_size == other.no_size
    }

    fn pair_amount(&self) -> Decimal {
        self.yes_size.min(self.no_size).max(Decimal::ZERO)
    }

    fn net_exposure_abs(&self) -> Decimal {
        (self.yes_size - self.no_size).abs()
    }
}

#[derive(Debug, Clone)]
struct CleanupTruthObservation {
    truth: DirectMarketPositionTruth,
    stable_baseline_confirmed: bool,
    observed_for: Duration,
}

#[derive(Debug, Clone)]
struct PairTruthObservation {
    truth: DirectMarketPositionTruth,
    observed_for: Duration,
}

#[derive(Debug, Clone, Default)]
struct MergeProbeLegAcquisition {
    order_id: Option<String>,
    lookup_status: Option<String>,
    matched_shares: Decimal,
    trade_ids: Vec<String>,
    failure_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DirectTruthSnapshot {
    stage: &'static str,
    truth: DirectMarketPositionTruth,
    observed_for: Duration,
}

#[derive(Debug, Clone)]
struct PostDecisionFlowObservation {
    truth: DirectMarketPositionTruth,
    merge_observed: bool,
    fallback_asks_observed: bool,
    observed_for: Duration,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ProductionHedgeVerificationEvidence {
    cancel_status: Option<String>,
    cancel_reason: Option<String>,
    lookup_status: Option<String>,
    lookup_matched_shares: Option<Decimal>,
    lookup_error: Option<String>,
    trade_ids: Option<Vec<String>>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct HedgeVerificationObservation {
    verification_state: Option<String>,
    production_cancel_status: Option<String>,
    production_cancel_reason: Option<String>,
    production_lookup_status: Option<String>,
    production_lookup_matched_shares: Option<Decimal>,
    production_lookup_error: Option<String>,
    production_trade_ids: Option<Vec<String>>,
    lookup_status: Option<String>,
    lookup_matched_shares: Option<Decimal>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ProductionSellbackConfirmationEvidence {
    response_status: Option<String>,
    lookup_status: Option<String>,
    lookup_matched_shares: Option<Decimal>,
    lookup_error: Option<String>,
    trade_ids: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SellbackConfirmationObservation {
    status: String,
    reason: String,
}

impl SellbackConfirmationObservation {
    fn confirmed(reason: impl Into<String>) -> Self {
        Self {
            status: "confirmed_before_cleanup".to_string(),
            reason: reason.into(),
        }
    }

    fn unconfirmed(reason: impl Into<String>) -> Self {
        Self {
            status: "unconfirmed_before_cleanup".to_string(),
            reason: reason.into(),
        }
    }

    fn not_applicable(reason: impl Into<String>) -> Self {
        Self {
            status: "not_applicable".to_string(),
            reason: reason.into(),
        }
    }

    fn is_confirmed_before_cleanup(&self) -> bool {
        self.status == "confirmed_before_cleanup"
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TruthReconciliationOutcome {
    status: String,
    reason: String,
    warning_status: Option<String>,
    warning_reason: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct DirectPositionEntry {
    #[serde(rename = "conditionId")]
    condition_id: Option<String>,
    outcome: Option<String>,
    #[serde(default, deserialize_with = "deserialize_direct_position_size")]
    size: Decimal,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum DirectPositionSize {
    String(String),
    Float(f64),
    Integer(i64),
}

#[derive(Debug, Serialize)]
struct UserStreamSmokeResult {
    scenario_name: String,
    connected_transport: bool,
    subscription_sent: bool,
    ack_received: bool,
    first_frame_type: Option<String>,
    first_frame_preview: Option<String>,
    elapsed_ms: u128,
    timeout_secs: u64,
}

#[tokio::test]
#[ignore = "manual connect-only smoke test; requires explicit scenario and real Polymarket credentials"]
async fn live_probe_user_stream_smoke_connects_without_orders() {
    let result = run_user_stream_smoke_from_env()
        .await
        .expect("user stream smoke test should execute with real credentials");
    println!(
        "{}",
        serde_json::to_string_pretty(&result).expect("result json should serialize")
    );
    assert!(
        result.connected_transport && result.ack_received,
        "user stream smoke failed: {:?}",
        result
    );
}

#[tokio::test]
#[ignore = "manual live-money probe; requires explicit arming and real Polymarket credentials"]
async fn live_probe_armed_runs_current_production_hedge_path() {
    let result = run_live_probe_from_env()
        .await
        .expect("live probe should execute when explicitly armed");
    println!(
        "{}",
        serde_json::to_string_pretty(&result).expect("result json should serialize")
    );
    assert!(
        result.meta_pass && result.standard_pass && result.cleanup_pass,
        "live probe failed: {:?}",
        result
    );
}

#[tokio::test]
#[ignore = "manual live-money merge probe; requires explicit arming and real Polymarket credentials"]
async fn merge_live_probe_armed_redeems_paired_inventory_via_ctf() {
    let result = run_merge_live_probe_from_env()
        .await
        .expect("merge live probe should execute when explicitly armed");
    println!(
        "{}",
        serde_json::to_string_pretty(&result).expect("result json should serialize")
    );
    assert!(
        result.meta_pass && result.standard_pass && result.cleanup_pass,
        "merge live probe failed: {:?}",
        result
    );
}

#[tokio::test]
async fn live_probe_event_fanout_preserves_repo_event_logs() {
    let (mut engine, event_dir) = build_test_engine("http://127.0.0.1:9", true).await;
    let file_producer = engine
        .event_producer
        .clone()
        .expect("observability-enabled test engine should create file producer");
    let event_collector = Arc::new(InMemoryEventCollector::default());
    let fanout: Arc<dyn spreadeater_core::EventProducer> =
        Arc::new(FanoutEventProducer::new(file_producer, event_collector.clone()));
    engine.event_producer = Some(fanout.clone());

    fanout
        .emit(EventEnvelope::new(
            EventType::DecisionEvaluated,
            Priority::Normal,
            engine.run_id.clone(),
            "fanout-condition".to_string(),
            engine.mode.clone(),
            serde_json::json!({ "source": "live-probe-fanout-test" }),
        ))
        .expect("fanout producer should accept event");

    let logged = wait_for_emitted_events(
        &event_dir,
        &engine.run_id,
        Duration::from_millis(500),
        |events| !events.is_empty(),
    )
    .await;
    assert_eq!(event_collector.events().len(), 1);
    assert_eq!(logged.len(), 1);
    assert_eq!(logged[0].payload["source"], "live-probe-fanout-test");
}

async fn run_user_stream_smoke_from_env() -> Result<UserStreamSmokeResult> {
    let scenario = load_live_probe_scenario_from_env()?;
    run_user_stream_smoke(scenario).await
}

async fn run_live_probe_from_env() -> Result<HedgeLiveProbeResult> {
    let arm_token =
        std::env::var(LIVE_PROBE_ARM_ENV).context("live probe arm token env var missing")?;
    if arm_token != LIVE_PROBE_ARM_TOKEN {
        bail!("live probe arm token mismatch");
    }
    let scenario = load_live_probe_scenario_from_env()?;

    run_live_probe(scenario).await
}

async fn run_merge_live_probe_from_env() -> Result<MergeLiveProbeResult> {
    let arm_token =
        std::env::var(LIVE_PROBE_ARM_ENV).context("merge live probe arm token env var missing")?;
    if arm_token != LIVE_PROBE_ARM_TOKEN {
        bail!("merge live probe arm token mismatch");
    }
    let scenario = load_merge_live_probe_scenario_from_env()?;

    run_merge_live_probe(scenario).await
}

fn load_live_probe_scenario_from_env() -> Result<HedgeLiveProbeScenario> {
    let scenario_path =
        std::env::var(LIVE_PROBE_SCENARIO_ENV).context("live probe scenario env var missing")?;
    let scenario: HedgeLiveProbeScenario = serde_json::from_str(
        &std::fs::read_to_string(&scenario_path)
            .with_context(|| format!("Failed to read live probe scenario {}", scenario_path))?,
    )
    .with_context(|| format!("Failed to parse live probe scenario {}", scenario_path))?;
    Ok(scenario)
}

fn load_merge_live_probe_scenario_from_env() -> Result<MergeLiveProbeScenario> {
    let scenario_path = std::env::var(LIVE_MERGE_PROBE_SCENARIO_ENV)
        .context("merge live probe scenario env var missing")?;
    let scenario: MergeLiveProbeScenario = serde_json::from_str(
        &std::fs::read_to_string(&scenario_path)
            .with_context(|| format!("Failed to read merge live probe scenario {}", scenario_path))?,
    )
    .with_context(|| format!("Failed to parse merge live probe scenario {}", scenario_path))?;
    validate_merge_live_probe_scenario(&scenario)?;
    Ok(scenario)
}

fn validate_merge_live_probe_scenario(scenario: &MergeLiveProbeScenario) -> Result<()> {
    if scenario.acquisition.shares <= Decimal::ZERO {
        bail!("merge live probe acquisition.shares must be > 0");
    }
    if normalize_share_size(scenario.acquisition.shares) < Decimal::from(5u32) {
        bail!("merge live probe acquisition.shares must be >= 5 shares");
    }
    if scenario.acquisition.yes_max_limit_price <= Decimal::ZERO {
        bail!("merge live probe acquisition.yes_max_limit_price must be > 0");
    }
    if scenario.acquisition.no_max_limit_price <= Decimal::ZERO {
        bail!("merge live probe acquisition.no_max_limit_price must be > 0");
    }
    if scenario.safety.max_yes_notional_usdc < Decimal::ZERO {
        bail!("merge live probe safety.max_yes_notional_usdc must be >= 0");
    }
    if scenario.safety.max_no_notional_usdc < Decimal::ZERO {
        bail!("merge live probe safety.max_no_notional_usdc must be >= 0");
    }
    if scenario.safety.max_cleanup_notional_usdc < Decimal::ZERO {
        bail!("merge live probe safety.max_cleanup_notional_usdc must be >= 0");
    }
    let yes_notional =
        normalize_share_size(scenario.acquisition.shares) * scenario.acquisition.yes_max_limit_price;
    if yes_notional < MERGE_PROBE_MIN_MARKETABLE_BUY_NOTIONAL_USDC {
        bail!(
            "merge live probe YES acquisition notional {} is below the venue minimum marketable BUY size of {} USDC",
            yes_notional,
            MERGE_PROBE_MIN_MARKETABLE_BUY_NOTIONAL_USDC
        );
    }
    if yes_notional > scenario.safety.max_yes_notional_usdc {
        bail!("merge live probe YES acquisition notional exceeds safety cap");
    }
    let no_notional =
        normalize_share_size(scenario.acquisition.shares) * scenario.acquisition.no_max_limit_price;
    if no_notional < MERGE_PROBE_MIN_MARKETABLE_BUY_NOTIONAL_USDC {
        bail!(
            "merge live probe NO acquisition notional {} is below the venue minimum marketable BUY size of {} USDC",
            no_notional,
            MERGE_PROBE_MIN_MARKETABLE_BUY_NOTIONAL_USDC
        );
    }
    if no_notional > scenario.safety.max_no_notional_usdc {
        bail!("merge live probe NO acquisition notional exceeds safety cap");
    }
    Ok(())
}

fn merge_trade_ids(target: &mut Vec<String>, extra: &[String]) {
    for trade_id in extra {
        if !target.iter().any(|existing| existing == trade_id) {
            target.push(trade_id.clone());
        }
    }
}

async fn run_user_stream_smoke(scenario: HedgeLiveProbeScenario) -> Result<UserStreamSmokeResult> {
    let credentials = ApiCredentials::from_env()?;
    let started = Instant::now();
    let (ws_stream, _) = connect_async(LIVE_PROBE_USER_WS_URL)
        .await
        .context("failed to connect transport to user websocket")?;
    let (mut write, mut read) = ws_stream.split();

    write
        .send(build_user_ws_auth_message(&credentials))
        .await
        .context("failed to send user websocket auth request")?;
    write
        .send(Message::Text(USER_WS_HEARTBEAT_TEXT.into()))
        .await
        .context("failed to send user websocket heartbeat")?;

    let (ack_received, first_frame_type, first_frame_preview) = match timeout(
        Duration::from_secs(scenario.safety.timeout_secs),
        read.next(),
    )
    .await
    {
        Ok(Some(Ok(Message::Text(text)))) => (
            true,
            Some(if user_ws_text_is_pong(&text) {
                "pong_text".to_string()
            } else {
                "text".to_string()
            }),
            Some(preview_ws_text(&text)),
        ),
        Ok(Some(Ok(Message::Binary(bytes)))) => (
            true,
            Some("binary".to_string()),
            Some(format!("{} binary bytes", bytes.len())),
        ),
        Ok(Some(Ok(Message::Ping(bytes)))) => (
            true,
            Some("ping".to_string()),
            Some(format!("{} ping bytes", bytes.len())),
        ),
        Ok(Some(Ok(Message::Pong(bytes)))) => (
            true,
            Some("pong".to_string()),
            Some(format!("{} pong bytes", bytes.len())),
        ),
        Ok(Some(Ok(Message::Close(frame)))) => (
            false,
            Some("close".to_string()),
            Some(format!("{:?}", frame)),
        ),
        Ok(Some(Ok(Message::Frame(_)))) => (true, Some("frame".to_string()), None),
        Ok(Some(Err(err))) => {
            return Err(err).context("user websocket returned an error frame before any ACK");
        }
        Ok(None) => bail!("user websocket stream ended before any ACK frame"),
        Err(_) => (false, None, None),
    };

    let _ = write.send(Message::Close(None)).await;

    Ok(UserStreamSmokeResult {
        scenario_name: scenario.name,
        connected_transport: true,
        subscription_sent: true,
        ack_received,
        first_frame_type,
        first_frame_preview,
        elapsed_ms: started.elapsed().as_millis(),
        timeout_secs: scenario.safety.timeout_secs,
    })
}

async fn run_live_probe(scenario: HedgeLiveProbeScenario) -> Result<HedgeLiveProbeResult> {
    let probe_started = Instant::now();
    let credentials = ApiCredentials::from_env()?;
    let direct_position_user = credentials
        .funder
        .clone()
        .unwrap_or_else(|| credentials.address.clone());
    let base_dir = std::env::temp_dir().join(format!("spreadeater-live-probe-{}", Uuid::new_v4()));
    let archive_dir = base_dir.join("archive");
    let error_dir = base_dir.join("errors");
    std::fs::create_dir_all(&archive_dir)?;
    std::fs::create_dir_all(&error_dir)?;

    let mut config = Config::default();
    config.persistence.archive_dir = archive_dir.to_string_lossy().into_owned();
    let data_api_base_url = config.discovery.data_api_base_url.clone();
    let error_logger = Arc::new(ErrorLogger::new(&error_dir.to_string_lossy()));
    let mut engine = LiveEngine::new(
        config,
        credentials.clone(),
        false,
        error_logger,
        "tests/support/hedge/live_probe.json".to_string(),
    )
    .await?;
    let event_collector = Arc::new(InMemoryEventCollector::default());
    let event_producer: Arc<dyn spreadeater_core::EventProducer> =
        match engine.event_producer.clone() {
            Some(primary) => Arc::new(FanoutEventProducer::new(primary, event_collector.clone())),
            None => event_collector.clone(),
        };
    engine.event_producer = Some(event_producer);

    let market = canonical_from_live_probe(&scenario.market);
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
    engine
        .subscribed_market_ids
        .write()
        .await
        .insert(market.condition_id.clone());

    let baseline_direct_truth = fetch_direct_market_position_truth(
        &data_api_base_url,
        &direct_position_user,
        &market.condition_id,
    )
    .await?;
    engine.position_manager.sync_positions().await?;
    let baseline = engine
        .position_manager
        .get_position(&market.condition_id)
        .await;
    if scenario.safety.require_clean_market {
        if !baseline_direct_truth.is_flat() {
            bail!(
                "live probe requires a clean direct baseline for {} on {}: yes_size={}, no_size={}",
                market.condition_id,
                direct_position_user,
                baseline_direct_truth.yes_size,
                baseline_direct_truth.no_size
            );
        }
        if let Some(position) = baseline.as_ref().filter(|position| {
            position.yes_size > Decimal::ZERO || position.no_size > Decimal::ZERO
        }) {
            bail!(
                "live probe requires a clean market baseline for {}: yes_size={}, no_size={}",
                market.condition_id,
                position.yes_size,
                position.no_size
            );
        }
    }

    let tick_size = tick_size_decimal(&scenario.market.tick_size);
    let pre_trigger_audit = capture_book_audit_snapshot(&engine, &scenario.market).await;
    let trigger_limit_price = derive_live_probe_marketable_limit_price(
        trigger_book_snapshot(&pre_trigger_audit, scenario.trigger.leg),
        tick_size,
        "trigger",
        scenario.trigger.max_trigger_limit_price,
    )?;
    let trigger_token_id = live_probe_token_id(&scenario.market, scenario.trigger.leg);
    let trigger_side = side_for_leg(scenario.trigger.leg);
    let trigger_notional = scenario.trigger.shares * trigger_limit_price;
    if trigger_notional > scenario.safety.max_trigger_notional_usdc {
        bail!("trigger notional exceeds live probe safety cap");
    }
    let user_stream = UserStream::new(credentials);
    let mut user_rx = user_stream
        .subscribe(vec![market.condition_id.clone()])
        .await?;
    // Polymarket can keep the user socket silent until actual account activity appears, so
    // the probe must not deadlock on an idle pre-order ACK before placing the trigger order.

    let trigger_request = OrderRequest {
        token_id: trigger_token_id.clone(),
        price: trigger_limit_price,
        size: scenario.trigger.shares,
        amount_kind: OrderAmountKind::Shares,
        side: trigger_side,
        order_type: OrderType::GTC,
        post_only: false,
        neg_risk: scenario.market.neg_risk,
        tick_size: scenario.market.tick_size.clone(),
    };
    let trigger_result = engine.trading_client.place_order(&trigger_request).await?;
    engine
        .order_manager
        .seed_live_order_for_test(TrackedOrder {
            order_id: trigger_result.order_id.clone(),
            trace_id: format!("live-probe-trigger-{}", Uuid::new_v4()),
            condition_id: market.condition_id.clone(),
            created_at: Utc::now(),
            leg: scenario.trigger.leg,
            token_id: trigger_token_id.clone(),
            opposite_token_id: opposite_live_probe_token_id(&scenario.market, scenario.trigger.leg),
            side: trigger_side,
            price: trigger_limit_price,
            size: scenario.trigger.shares,
            matched_size: Decimal::ZERO,
            neg_risk: scenario.market.neg_risk,
            tick_size: scenario.market.tick_size.clone(),
        })
        .await;

    let fill_handler = fill_handler_from_engine(&engine);
    let (fill_tx, mut fill_rx) = mpsc::unbounded_channel();
    let deadline = Instant::now() + Duration::from_secs(scenario.safety.timeout_secs);
    let mut observed_trade_id = None;
    let mut post_trade_audit: Option<BookAuditSnapshot> = None;

    while Instant::now() < deadline {
        match timeout(Duration::from_secs(1), user_rx.recv()).await {
            Ok(Some(UserEvent::Connected { .. })) => {
                let _ = engine.position_manager.sync_positions().await;
            }
            Ok(Some(UserEvent::RawActivity)) => {}
            Ok(Some(UserEvent::Trade(trade))) => {
                let matches_trigger = trade
                    .maker_order_id
                    .as_ref()
                    .is_some_and(|id| id == &trigger_result.order_id)
                    || trade
                        .taker_order_id
                        .as_ref()
                        .is_some_and(|id| id == &trigger_result.order_id);
                if !matches_trigger {
                    continue;
                }
                observed_trade_id = Some(trade.id.clone());
                post_trade_audit =
                    Some(capture_book_audit_snapshot(&engine, &scenario.market).await);
                if let Some(work) = engine.build_fill_work_item(trade).await {
                    fill_tx
                        .send(work)
                        .map_err(|_| anyhow!("fill handler channel unexpectedly closed"))?;
                    drain_fill_queue(&fill_handler, &mut fill_rx).await?;
                    break;
                }
            }
            Ok(Some(UserEvent::Order(order_event))) => {
                if order_event.event_type == OrderEventType::Cancellation {
                    engine.handle_external_cancellation(order_event).await;
                } else if order_event.event_type == OrderEventType::Update {
                    engine.handle_order_update(order_event).await;
                }
            }
            Ok(Some(UserEvent::Disconnected)) => {}
            Ok(None) => bail!("user stream closed before probe completed"),
            Err(_) => {
                engine.flush_pending_fill_fallbacks(&fill_tx).await?;
                drain_fill_queue(&fill_handler, &mut fill_rx).await?;
            }
        }
    }

    let events = event_collector.events();
    let hedge_decision =
        latest_payload::<HedgeDecisionPayload>(&events, EventType::HedgeDecisionEvaluated)?;
    let hedge_intent =
        latest_payload::<HedgeIntentPayload>(&events, EventType::HedgeIntentCreated)?;
    let hedge_result =
        latest_payload::<HedgeResultPayload>(&events, EventType::HedgeResultRecorded)?;
    let hedge_exit =
        latest_payload::<HedgeExitPathPayload>(&events, EventType::HedgeExitPathRecorded)?;
    let post_sync_net_exposure = hedge_result
        .as_ref()
        .and_then(|payload| payload.post_sync_net_exposure);
    let planned_hedge_shares = hedge_decision
        .as_ref()
        .map(|payload| payload.planned_hedge_shares)
        .or_else(|| {
            hedge_intent
                .as_ref()
                .and_then(|payload| payload.planned_hedge_shares)
        });
    let planned_sellback_shares = hedge_decision
        .as_ref()
        .map(|payload| payload.planned_sellback_shares)
        .or_else(|| {
            hedge_intent
                .as_ref()
                .and_then(|payload| payload.planned_sellback_shares)
        });
    let hedge_leg_status = hedge_result
        .as_ref()
        .and_then(|payload| payload.hedge_leg_status.clone());
    let sellback_leg_status = hedge_result
        .as_ref()
        .and_then(|payload| payload.sellback_leg_status.clone());
    let production_decision_mode = hedge_decision
        .as_ref()
        .map(|payload| payload.decision_mode.clone())
        .or_else(|| {
            hedge_intent.as_ref().map(|payload| {
                if payload.hedge_side == "SELL" {
                    "sell_side_direct".to_string()
                } else {
                    "buy_side_resolution".to_string()
                }
            })
        });
    let production_decision_reason_code = hedge_decision
        .as_ref()
        .map(|payload| payload.decision_reason_code.clone())
        .or_else(|| {
            hedge_intent.as_ref().and_then(|payload| {
                (payload.hedge_side == "SELL").then(|| "sell_side_direct".to_string())
            })
        });
    let production_exit_path_status = hedge_exit
        .as_ref()
        .map(|payload| payload.exit_path_status.clone());
    let meta_pass = observed_trade_id.is_some() && hedge_result.is_some();
    let decision_verdict = evaluate_decision_audit(
        &scenario,
        hedge_decision.as_ref(),
        hedge_intent.as_ref(),
        Some(&pre_trigger_audit),
        post_trade_audit.as_ref(),
    );
    let flow_observation = observe_post_decision_flow(
        &engine,
        &events,
        &data_api_base_url,
        &direct_position_user,
        &market.condition_id,
        initial_pair_amount(hedge_exit.as_ref(), hedge_result.as_ref()),
        remaining_probe_time(probe_started, scenario.safety.timeout_secs)
            .min(Duration::from_secs(8)),
    )
    .await?;
    let hedge_verification =
        observe_hedge_verification(&engine, &market.condition_id, hedge_result.as_ref()).await;
    let post_decision_snapshot = post_decision_direct_snapshot(&flow_observation);
    let sellback_confirmation = confirm_sellback_before_cleanup(
        observe_sellback_confirmation(
            &engine,
            &market.condition_id,
            hedge_result.as_ref(),
            planned_sellback_shares,
        )
        .await,
        hedge_result.as_ref(),
        hedge_exit.as_ref(),
        &post_decision_snapshot,
        scenario.safety.max_post_sync_net_exposure,
    );
    let flow_status = classify_flow_status(
        post_sync_net_exposure,
        scenario.safety.max_post_sync_net_exposure,
        planned_hedge_shares,
        planned_sellback_shares,
        hedge_exit.as_ref(),
        hedge_intent.as_ref(),
        &flow_observation,
        &sellback_confirmation,
    );
    let merge_status = derive_merge_status(hedge_exit.as_ref(), &flow_observation);
    let merge_failure_reason = hedge_exit
        .as_ref()
        .and_then(|payload| payload.merge_failure_reason.clone());
    let fallback_status = derive_fallback_status(hedge_exit.as_ref(), &flow_observation);
    let fallback_failure_reason = hedge_exit
        .as_ref()
        .and_then(|payload| payload.fallback_failure_reason.clone());
    let truth_reconciliation = reconcile_production_truth(
        hedge_result.as_ref(),
        hedge_exit.as_ref(),
        &flow_observation,
        scenario.safety.max_post_sync_net_exposure,
        &sellback_confirmation,
    );

    let _ = engine
        .kill_market(&market.condition_id, "manual_live_probe_cleanup")
        .await;
    let _ = engine.position_manager.sync_positions().await;
    let cleanup_truth = wait_for_direct_cleanup_truth(
        &data_api_base_url,
        &direct_position_user,
        &market.condition_id,
        &baseline_direct_truth,
        cleanup_probe_time(probe_started, scenario.safety.timeout_secs),
    )
    .await?;
    let cleanup_pass = cleanup_truth.stable_baseline_confirmed
        && cleanup_truth.truth.matches(&baseline_direct_truth);
    let cleanup_status = build_cleanup_status(
        cleanup_pass,
        &cleanup_truth,
        &baseline_direct_truth,
        &direct_position_user,
    );
    let cleanup_snapshot = cleanup_direct_snapshot(&cleanup_truth);
    let standard_pass = evaluate_standard_pass(
        meta_pass,
        decision_verdict.status,
        flow_status,
        post_sync_net_exposure,
        scenario.safety.max_post_sync_net_exposure,
        Some(truth_reconciliation.status.as_str()),
    );

    Ok(HedgeLiveProbeResult {
        scenario_name: scenario.name,
        meta_pass,
        standard_pass,
        cleanup_pass,
        trigger_order_id: Some(trigger_result.order_id),
        trigger_trade_id: observed_trade_id,
        decision_audit_status: decision_verdict.status,
        decision_audit_reason: decision_verdict.reason,
        flow_status,
        production_decision_mode,
        production_decision_reason_code,
        production_exit_path_status,
        merge_status,
        merge_failure_reason,
        fallback_status,
        fallback_failure_reason,
        truth_reconciliation_status: Some(truth_reconciliation.status),
        truth_reconciliation_reason: Some(truth_reconciliation.reason),
        production_sellback_confirmation_status: Some(sellback_confirmation.status),
        production_sellback_confirmation_reason: Some(sellback_confirmation.reason),
        truth_reconciliation_warning_status: truth_reconciliation.warning_status,
        truth_reconciliation_warning_reason: truth_reconciliation.warning_reason,
        post_decision_direct_yes_size: Some(post_decision_snapshot.truth.yes_size),
        post_decision_direct_no_size: Some(post_decision_snapshot.truth.no_size),
        post_decision_direct_observed_for_secs: Some(post_decision_snapshot.observed_for.as_secs()),
        cleanup_direct_yes_size: Some(cleanup_snapshot.truth.yes_size),
        cleanup_direct_no_size: Some(cleanup_snapshot.truth.no_size),
        cleanup_direct_observed_for_secs: Some(cleanup_snapshot.observed_for.as_secs()),
        production_hedge_cancel_status: hedge_verification.production_cancel_status,
        production_hedge_cancel_reason: hedge_verification.production_cancel_reason,
        production_hedge_lookup_status: hedge_verification.production_lookup_status,
        production_hedge_lookup_matched_shares: hedge_verification.production_lookup_matched_shares,
        production_hedge_lookup_error: hedge_verification.production_lookup_error,
        production_hedge_trade_ids: hedge_verification.production_trade_ids,
        production_sellback_response_status: hedge_result
            .as_ref()
            .and_then(|payload| payload.sellback_response_status.clone()),
        production_sellback_lookup_status: hedge_result
            .as_ref()
            .and_then(|payload| payload.sellback_lookup_status.clone()),
        production_sellback_lookup_matched_shares: hedge_result
            .as_ref()
            .and_then(|payload| payload.sellback_lookup_matched_shares),
        production_sellback_lookup_error: hedge_result
            .as_ref()
            .and_then(|payload| payload.sellback_lookup_error.clone()),
        production_sellback_trade_ids: hedge_result
            .as_ref()
            .and_then(|payload| payload.sellback_trade_ids.clone()),
        hedge_lookup_status: hedge_verification.lookup_status,
        hedge_lookup_matched_shares: hedge_verification.lookup_matched_shares,
        planned_hedge_shares,
        planned_sellback_shares,
        hedge_leg_status: hedge_leg_status.clone(),
        sellback_leg_status,
        hedge_verification_state: hedge_verification.verification_state,
        post_sync_net_exposure,
        merge_observed: flow_observation.merge_observed,
        fallback_asks_observed: flow_observation.fallback_asks_observed,
        cleanup_status,
    })
}

async fn run_merge_live_probe(scenario: MergeLiveProbeScenario) -> Result<MergeLiveProbeResult> {
    let probe_started = Instant::now();
    let credentials = ApiCredentials::from_env()?;
    let direct_position_user = credentials
        .funder
        .clone()
        .unwrap_or_else(|| credentials.address.clone());
    let base_dir =
        std::env::temp_dir().join(format!("spreadeater-merge-live-probe-{}", Uuid::new_v4()));
    let archive_dir = base_dir.join("archive");
    let error_dir = base_dir.join("errors");
    std::fs::create_dir_all(&archive_dir)?;
    std::fs::create_dir_all(&error_dir)?;

    let mut config = Config::default();
    config.persistence.archive_dir = archive_dir.to_string_lossy().into_owned();
    let data_api_base_url = config.discovery.data_api_base_url.clone();
    let error_logger = Arc::new(ErrorLogger::new(&error_dir.to_string_lossy()));
    let mut engine = LiveEngine::new(
        config,
        credentials.clone(),
        false,
        error_logger,
        "tests/support/hedge/live_probe.json".to_string(),
    )
    .await?;
    let event_collector = Arc::new(InMemoryEventCollector::default());
    let event_producer: Arc<dyn spreadeater_core::EventProducer> =
        match engine.event_producer.clone() {
            Some(primary) => Arc::new(FanoutEventProducer::new(primary, event_collector)),
            None => Arc::new(InMemoryEventCollector::default()),
        };
    engine.event_producer = Some(event_producer);

    let market = canonical_from_live_probe(&scenario.market);
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
    engine
        .subscribed_market_ids
        .write()
        .await
        .insert(market.condition_id.clone());

    let baseline_direct_truth = fetch_direct_market_position_truth(
        &data_api_base_url,
        &direct_position_user,
        &market.condition_id,
    )
    .await?;
    engine.position_manager.sync_positions().await?;
    let baseline = engine.position_manager.get_position(&market.condition_id).await;
    if scenario.safety.require_clean_market {
        if !baseline_direct_truth.is_flat() {
            bail!(
                "merge live probe requires a clean direct baseline for {} on {}: yes_size={}, no_size={}",
                market.condition_id,
                direct_position_user,
                baseline_direct_truth.yes_size,
                baseline_direct_truth.no_size
            );
        }
        if let Some(position) = baseline.as_ref().filter(|position| {
            position.yes_size > Decimal::ZERO || position.no_size > Decimal::ZERO
        }) {
            bail!(
                "merge live probe requires a clean market baseline for {}: yes_size={}, no_size={}",
                market.condition_id,
                position.yes_size,
                position.no_size
            );
        }
    }

    let pre_exit_audit = capture_book_audit_snapshot(&engine, &scenario.market).await;
    let tick_size = tick_size_decimal(&scenario.market.tick_size);
    let yes_limit_price = derive_live_probe_marketable_limit_price(
        pre_exit_audit.yes_book.as_ref(),
        tick_size,
        "merge YES acquisition",
        scenario.acquisition.yes_max_limit_price,
    )?;
    let no_limit_price = derive_live_probe_marketable_limit_price(
        pre_exit_audit.no_book.as_ref(),
        tick_size,
        "merge NO acquisition",
        scenario.acquisition.no_max_limit_price,
    )?;
    let yes_notional = normalize_share_size(scenario.acquisition.shares) * yes_limit_price;
    if yes_notional > scenario.safety.max_yes_notional_usdc {
        bail!("merge live probe YES acquisition notional exceeds safety cap");
    }
    let no_notional = normalize_share_size(scenario.acquisition.shares) * no_limit_price;
    if no_notional > scenario.safety.max_no_notional_usdc {
        bail!("merge live probe NO acquisition notional exceeds safety cap");
    }

    let ctf_merge_configured = engine.harness_ctf_merge_enabled();
    let mut failure_reason = None;
    let mut meta_pass = false;
    let mut standard_pass = false;
    let mut yes_acquisition = MergeProbeLegAcquisition::default();
    let mut no_acquisition = MergeProbeLegAcquisition::default();
    let mut acquired_pair_shares = None;
    let mut engine_pair_shares_before_exit = None;
    let mut pair_exit_status = None;
    let mut merge_tx_hash = None;
    let mut merge_failure_reason = None;
    let mut fallback_asks_attempted = None;
    let mut fallback_ask_count = None;
    let mut fallback_failure_reason = None;
    let mut pre_exit_collateral_usdc = None;
    let mut post_exit_collateral_usdc = None;
    let mut collateral_delta_usdc = None;
    let mut post_exit_direct_yes_size = None;
    let mut post_exit_direct_no_size = None;
    let mut post_exit_direct_observed_for_secs = None;

    let execution_result: Result<()> = async {
        if !ctf_merge_configured {
            bail!("ctf merger is not configured in the live engine");
        }
        engine.harness_ctf_merge_preflight().await?;

        yes_acquisition = acquire_merge_probe_leg(
            &engine,
            &scenario.market,
            QuoteLeg::YesBid,
            scenario.acquisition.shares,
            yes_limit_price,
        )
        .await?;
        no_acquisition = acquire_merge_probe_leg(
            &engine,
            &scenario.market,
            QuoteLeg::NoBid,
            scenario.acquisition.shares,
            no_limit_price,
        )
        .await?;

        let pair_truth = wait_for_direct_pair_truth(
            &data_api_base_url,
            &direct_position_user,
            &market.condition_id,
            scenario.acquisition.shares,
            remaining_probe_time(probe_started, scenario.safety.timeout_secs)
                .min(Duration::from_secs(20)),
        )
        .await?;
        acquired_pair_shares = Some(pair_truth.truth.pair_amount());

        engine.position_manager.sync_positions().await?;
        let engine_position = engine
            .position_manager
            .get_position(&market.condition_id)
            .await
            .unwrap_or_else(|| Position::new(market.condition_id.clone()));
        let engine_pairs = merge_eligible_pairs(&engine_position);
        engine_pair_shares_before_exit = Some(engine_pairs);
        if pair_truth.truth.pair_amount() < scenario.acquisition.shares
            || engine_pairs < scenario.acquisition.shares
        {
            bail!(
                "merge live probe failed to verify a complete pair before exit: direct_pairs={} engine_pairs={}",
                pair_truth.truth.pair_amount(),
                engine_pairs
            );
        }

        meta_pass = true;
        pre_exit_collateral_usdc = Some(engine.trading_client.get_balance().await?);
        let pair_exit = engine
            .harness_merge_pairs(&market.condition_id, scenario.acquisition.shares)
            .await?;
        pair_exit_status = Some(pair_exit.exit_path_status.clone());
        merge_tx_hash = pair_exit.merge_tx_hash.clone();
        merge_failure_reason = pair_exit.merge_failure_reason.clone();
        fallback_asks_attempted = Some(pair_exit.fallback_asks_attempted);
        fallback_ask_count = Some(pair_exit.fallback_ask_count);
        fallback_failure_reason = pair_exit.fallback_failure_reason.clone();
        post_exit_collateral_usdc = Some(engine.trading_client.get_balance().await?);
        collateral_delta_usdc =
            pre_exit_collateral_usdc.zip(post_exit_collateral_usdc).map(|(before, after)| {
                after - before
            });

        let post_exit_truth = wait_for_direct_truth_to_match_baseline(
            &data_api_base_url,
            &direct_position_user,
            &market.condition_id,
            &baseline_direct_truth,
            remaining_probe_time(probe_started, scenario.safety.timeout_secs)
                .min(Duration::from_secs(20)),
            "post-exit direct truth",
        )
        .await?;
        post_exit_direct_yes_size = Some(post_exit_truth.truth.yes_size);
        post_exit_direct_no_size = Some(post_exit_truth.truth.no_size);
        post_exit_direct_observed_for_secs = Some(post_exit_truth.observed_for.as_secs());

        standard_pass = pair_exit.exit_path_status == "merge_succeeded"
            && pair_exit.merge_tx_hash.is_some()
            && post_exit_truth.stable_baseline_confirmed
            && collateral_delta_usdc.is_some_and(|delta| delta > Decimal::ZERO);
        if !standard_pass {
            bail!(
                "merge live probe exit did not satisfy success criteria: exit_path_status={} merge_tx_hash_present={} post_exit_clean={} collateral_delta_usdc={}",
                pair_exit.exit_path_status,
                pair_exit.merge_tx_hash.is_some(),
                post_exit_truth.stable_baseline_confirmed,
                collateral_delta_usdc.unwrap_or(Decimal::ZERO)
            );
        }

        Ok(())
    }
    .await;

    if let Err(err) = execution_result {
        failure_reason = Some(format!("{err:#}"));
    }

    let _ = engine
        .kill_market(&market.condition_id, "manual_merge_live_probe_cleanup")
        .await;
    let _ = engine.position_manager.sync_positions().await;
    let cleanup_truth = wait_for_direct_cleanup_truth(
        &data_api_base_url,
        &direct_position_user,
        &market.condition_id,
        &baseline_direct_truth,
        cleanup_probe_time(probe_started, scenario.safety.timeout_secs),
    )
    .await?;
    let cleanup_pass = cleanup_truth.stable_baseline_confirmed
        && cleanup_truth.truth.matches(&baseline_direct_truth);
    let cleanup_status = build_cleanup_status(
        cleanup_pass,
        &cleanup_truth,
        &baseline_direct_truth,
        &direct_position_user,
    );

    Ok(MergeLiveProbeResult {
        scenario_name: scenario.name,
        meta_pass,
        standard_pass,
        cleanup_pass,
        ctf_merge_configured,
        yes_order_id: yes_acquisition.order_id,
        no_order_id: no_acquisition.order_id,
        yes_lookup_status: yes_acquisition.lookup_status,
        no_lookup_status: no_acquisition.lookup_status,
        yes_matched_shares: Some(yes_acquisition.matched_shares),
        no_matched_shares: Some(no_acquisition.matched_shares),
        yes_trade_ids: (!yes_acquisition.trade_ids.is_empty()).then_some(yes_acquisition.trade_ids),
        no_trade_ids: (!no_acquisition.trade_ids.is_empty()).then_some(no_acquisition.trade_ids),
        acquired_pair_shares,
        engine_pair_shares_before_exit,
        pair_exit_status,
        merge_tx_hash,
        merge_failure_reason,
        fallback_asks_attempted,
        fallback_ask_count,
        fallback_failure_reason,
        pre_exit_collateral_usdc,
        post_exit_collateral_usdc,
        collateral_delta_usdc,
        post_exit_direct_yes_size,
        post_exit_direct_no_size,
        post_exit_direct_observed_for_secs,
        failure_reason,
        cleanup_status,
    })
}

async fn acquire_merge_probe_leg(
    engine: &LiveEngine,
    market: &LiveProbeMarket,
    leg: QuoteLeg,
    shares: Decimal,
    limit_price: Decimal,
) -> Result<MergeProbeLegAcquisition> {
    let token_id = live_probe_token_id(market, leg);
    let request = OrderRequest {
        token_id,
        price: limit_price,
        size: shares,
        amount_kind: OrderAmountKind::Shares,
        side: side_for_leg(leg),
        order_type: OrderType::GTC,
        post_only: false,
        neg_risk: market.neg_risk,
        tick_size: market.tick_size.clone(),
    };
    let order_result = engine.trading_client.place_order(&request).await?;
    let mut trade_ids = order_result.trade_ids.clone();

    sleep(Duration::from_millis(MERGE_PROBE_ORDER_FILL_WAIT_MS)).await;
    if !order_result.order_id.is_empty() {
        let _ = engine
            .trading_client
            .cancel_order(&order_result.order_id)
            .await;
    }

    let mut outcome = MergeProbeLegAcquisition {
        order_id: (!order_result.order_id.is_empty()).then_some(order_result.order_id.clone()),
        matched_shares: Decimal::ZERO,
        ..Default::default()
    };
    if !order_result.order_id.is_empty() {
        match engine.trading_client.get_order(&order_result.order_id).await? {
            Some(order) => {
                outcome.lookup_status = Some(format!("{:?}", order.status));
                outcome.matched_shares = outcome.matched_shares.max(order.size_matched);
                if let Some(associated_trade_ids) = order.associated_trade_ids() {
                    merge_trade_ids(&mut trade_ids, &associated_trade_ids);
                }
            }
            None => {
                outcome.lookup_status = Some("missing".to_string());
                outcome.failure_reason = Some(format!(
                    "order lookup missing for {} acquisition order {}",
                    leg,
                    order_result.order_id
                ));
            }
        }
    }
    outcome.trade_ids = trade_ids;
    if outcome.matched_shares < shares {
        outcome.failure_reason.get_or_insert_with(|| {
            format!(
                "{} acquisition remained incomplete: matched_shares={} requested_shares={}",
                leg, outcome.matched_shares, shares
            )
        });
    }

    Ok(outcome)
}

async fn wait_for_direct_pair_truth(
    data_api_base_url: &str,
    user: &str,
    condition_id: &str,
    minimum_pairs: Decimal,
    timeout_window: Duration,
) -> Result<PairTruthObservation> {
    let started = Instant::now();
    let poll_interval = Duration::from_secs(1);
    let mut last_truth = fetch_direct_market_position_truth(data_api_base_url, user, condition_id)
        .await
        .context("failed to fetch direct pair truth")?;

    while started.elapsed() < timeout_window {
        if last_truth.pair_amount() >= minimum_pairs {
            return Ok(PairTruthObservation {
                truth: last_truth,
                observed_for: started.elapsed(),
            });
        }
        sleep(poll_interval).await;
        last_truth = fetch_direct_market_position_truth(data_api_base_url, user, condition_id)
            .await
            .context("failed to refresh direct pair truth")?;
    }

    Ok(PairTruthObservation {
        truth: last_truth,
        observed_for: started.elapsed(),
    })
}

async fn capture_book_audit_snapshot(
    engine: &LiveEngine,
    market: &LiveProbeMarket,
) -> BookAuditSnapshot {
    let max_hedge_usdc = engine.order_manager.available_hedge_resolution_usdc().await;
    match engine
        .book_rest
        .fetch_both_books(&market.yes_token_id, &market.no_token_id)
        .await
    {
        Ok((yes_book, no_book)) => BookAuditSnapshot {
            yes_book: Some(yes_book),
            no_book: Some(no_book),
            max_hedge_usdc,
            note: None,
        },
        Err(err) => BookAuditSnapshot {
            yes_book: None,
            no_book: None,
            max_hedge_usdc,
            note: Some(format!("book snapshot unavailable: {}", err)),
        },
    }
}

fn evaluate_decision_audit(
    scenario: &HedgeLiveProbeScenario,
    hedge_decision: Option<&HedgeDecisionPayload>,
    hedge_intent: Option<&HedgeIntentPayload>,
    pre_trigger_audit: Option<&BookAuditSnapshot>,
    post_trade_audit: Option<&BookAuditSnapshot>,
) -> DecisionAuditVerdict {
    let (
        fill_price,
        fill_size,
        observed,
        source_label,
        decision_mode,
        decision_reason_code,
        hedge_side,
    ) = if let Some(decision) = hedge_decision {
        (
            decision.fill_price,
            decision.fill_size,
            ObservedResolutionSplit {
                hedge_shares: decision.planned_hedge_shares,
                sellback_shares: decision.planned_sellback_shares,
            },
            "production decision event",
            Some(decision.decision_mode.as_str()),
            Some(decision.decision_reason_code.as_str()),
            decision.hedge_side.as_str(),
        )
    } else if let Some(intent) = hedge_intent {
        (
            intent.fill_price,
            intent.fill_size,
            ObservedResolutionSplit {
                hedge_shares: intent.planned_hedge_shares.unwrap_or(Decimal::ZERO),
                sellback_shares: intent.planned_sellback_shares.unwrap_or(Decimal::ZERO),
            },
            "hedge intent fallback",
            None,
            None,
            intent.hedge_side.as_str(),
        )
    } else {
        return DecisionAuditVerdict {
            status: DecisionAuditStatus::Inconclusive,
            reason: "hedge decision event and hedge intent payload missing".to_string(),
        };
    };
    if hedge_side != "BUY" || decision_mode == Some("sell_side_direct") {
        return DecisionAuditVerdict {
            status: DecisionAuditStatus::NotApplicable,
            reason: format!(
                "{} recorded sell-side direct resolution{}",
                source_label,
                decision_reason_code
                    .map(|code| format!(" reason_code={code}"))
                    .unwrap_or_default()
            ),
        };
    };
    let production_summary = format!(
        "{} recorded hedge={} sellback={}{}{}",
        source_label,
        observed.hedge_shares,
        observed.sellback_shares,
        decision_mode
            .map(|mode| format!(" decision_mode={mode}"))
            .unwrap_or_default(),
        decision_reason_code
            .map(|code| format!(" reason_code={code}"))
            .unwrap_or_default()
    );
    let pre = pre_trigger_audit.map(|snapshot| {
        compute_decision_audit_plan(
            snapshot,
            scenario.trigger.leg,
            &scenario.market,
            fill_price,
            fill_size,
        )
    });
    let post = post_trade_audit.map(|snapshot| {
        compute_decision_audit_plan(
            snapshot,
            scenario.trigger.leg,
            &scenario.market,
            fill_price,
            fill_size,
        )
    });

    match (pre, post) {
        (Some(Ok(pre_plan)), Some(Ok(post_plan))) => {
            let pre_matches = resolution_matches_observed(&pre_plan, observed);
            let post_matches = resolution_matches_observed(&post_plan, observed);
            if pre_matches && post_matches {
                return DecisionAuditVerdict {
                    status: DecisionAuditStatus::Confirmed,
                    reason: format!(
                        "pre-trigger and post-trade planner snapshots both matched {};",
                        production_summary
                    ),
                };
            }
            if pre_matches || post_matches {
                return DecisionAuditVerdict {
                    status: DecisionAuditStatus::Inconclusive,
                    reason: format!("only one planner snapshot matched {};", production_summary),
                };
            }
            if same_resolution_signature(&pre_plan, &post_plan) {
                return DecisionAuditVerdict {
                    status: DecisionAuditStatus::Failed,
                    reason: format!(
                        "both planner snapshots agreed on hedge={} sellback={}, but {}",
                        pre_plan.hedge_shares, pre_plan.sellback_shares, production_summary
                    ),
                };
            }
            DecisionAuditVerdict {
                status: DecisionAuditStatus::Inconclusive,
                reason: format!(
                    "planner snapshots disagreed (pre hedge={} sellback={}, post hedge={} sellback={}) and neither matched {}",
                    pre_plan.hedge_shares,
                    pre_plan.sellback_shares,
                    post_plan.hedge_shares,
                    post_plan.sellback_shares,
                    production_summary
                ),
            }
        }
        (Some(Err(pre_reason)), Some(Err(post_reason))) => DecisionAuditVerdict {
            status: DecisionAuditStatus::Inconclusive,
            reason: format!(
                "planner audit unavailable: {}; {}; {}",
                pre_reason, post_reason, production_summary
            ),
        },
        (Some(Err(reason)), _) | (_, Some(Err(reason))) => DecisionAuditVerdict {
            status: DecisionAuditStatus::Inconclusive,
            reason: format!("{reason}; {production_summary}"),
        },
        _ => DecisionAuditVerdict {
            status: DecisionAuditStatus::Inconclusive,
            reason: format!("planner audit snapshots missing; {production_summary}"),
        },
    }
}

fn compute_decision_audit_plan(
    snapshot: &BookAuditSnapshot,
    trigger_leg: QuoteLeg,
    market: &LiveProbeMarket,
    fill_price: Decimal,
    fill_size: Decimal,
) -> std::result::Result<HedgeResolution, String> {
    let Some(hedge_book) = hedge_book_for_leg(snapshot, trigger_leg) else {
        return Err(snapshot
            .note
            .clone()
            .unwrap_or_else(|| "missing hedge-side book snapshot".to_string()));
    };
    let Some(filled_book) = filled_book_for_leg(snapshot, trigger_leg) else {
        return Err(snapshot
            .note
            .clone()
            .unwrap_or_else(|| "missing filled-side book snapshot".to_string()));
    };

    Ok(plan_fill_resolution(
        fill_price,
        &hedge_book.asks,
        &filled_book.bids,
        fill_size,
        snapshot.max_hedge_usdc,
        tick_size_decimal(&market.tick_size),
    ))
}

fn hedge_book_for_leg<'a>(
    snapshot: &'a BookAuditSnapshot,
    trigger_leg: QuoteLeg,
) -> Option<&'a OrderBookSnapshot> {
    match trigger_leg {
        QuoteLeg::YesBid | QuoteLeg::YesAsk => snapshot.no_book.as_ref(),
        QuoteLeg::NoBid | QuoteLeg::NoAsk => snapshot.yes_book.as_ref(),
    }
}

fn filled_book_for_leg<'a>(
    snapshot: &'a BookAuditSnapshot,
    trigger_leg: QuoteLeg,
) -> Option<&'a OrderBookSnapshot> {
    match trigger_leg {
        QuoteLeg::YesBid | QuoteLeg::YesAsk => snapshot.yes_book.as_ref(),
        QuoteLeg::NoBid | QuoteLeg::NoAsk => snapshot.no_book.as_ref(),
    }
}

fn tick_size_decimal(tick_size: &str) -> Decimal {
    tick_size.parse().unwrap_or(dec!(0.01))
}

fn derive_live_probe_marketable_limit_price(
    book: Option<&OrderBookSnapshot>,
    tick_size: Decimal,
    label: &str,
    max_limit_price: Decimal,
) -> Result<Decimal> {
    let best_ask = book
        .and_then(|snapshot| snapshot.asks.iter().map(|level| level.price).min())
        .ok_or_else(|| anyhow!("live probe {label} book had no ask liquidity"))?;
    let limit_price = (best_ask + tick_size).min(dec!(0.99));
    if limit_price > max_limit_price {
        bail!(
            "live probe {} limit {} exceeded configured cap {}",
            label,
            limit_price,
            max_limit_price
        );
    }
    Ok(limit_price)
}

fn trigger_book_snapshot(
    snapshot: &BookAuditSnapshot,
    trigger_leg: QuoteLeg,
) -> Option<&OrderBookSnapshot> {
    match trigger_leg {
        QuoteLeg::YesBid | QuoteLeg::YesAsk => snapshot.yes_book.as_ref(),
        QuoteLeg::NoBid | QuoteLeg::NoAsk => snapshot.no_book.as_ref(),
    }
}

fn resolution_matches_observed(
    resolution: &HedgeResolution,
    observed: ObservedResolutionSplit,
) -> bool {
    resolution.hedge_shares == observed.hedge_shares
        && resolution.sellback_shares == observed.sellback_shares
}

fn same_resolution_signature(left: &HedgeResolution, right: &HedgeResolution) -> bool {
    left.hedge_shares == right.hedge_shares
        && left.sellback_shares == right.sellback_shares
        && left.unresolved_shares == right.unresolved_shares
}

fn initial_pair_amount(
    hedge_exit: Option<&HedgeExitPathPayload>,
    hedge_result: Option<&HedgeResultPayload>,
) -> Decimal {
    hedge_exit
        .map(|payload| payload.post_sync_complete_sets.max(Decimal::ZERO))
        .or_else(|| {
            hedge_result.map(|payload| {
                payload
                    .post_sync_yes_size
                    .unwrap_or(Decimal::ZERO)
                    .min(payload.post_sync_no_size.unwrap_or(Decimal::ZERO))
                    .max(Decimal::ZERO)
            })
        })
        .unwrap_or(Decimal::ZERO)
}

async fn observe_post_decision_flow(
    engine: &LiveEngine,
    events: &[EventEnvelope],
    data_api_base_url: &str,
    user: &str,
    condition_id: &str,
    initial_pair_amount: Decimal,
    observation_window: Duration,
) -> Result<PostDecisionFlowObservation> {
    let started = Instant::now();
    let inventory_ask_submitted = inventory_ask_submitted(events);
    let mut truth = fetch_direct_market_position_truth(data_api_base_url, user, condition_id)
        .await
        .context("failed to fetch post-decision direct truth")?;
    let mut merge_observed =
        initial_pair_amount > Decimal::ZERO && truth.pair_amount() < initial_pair_amount;
    let mut fallback_asks_observed =
        inventory_ask_submitted && inventory_asks_still_visible(engine, condition_id).await;

    if observation_window.is_zero() {
        return Ok(PostDecisionFlowObservation {
            truth,
            merge_observed,
            fallback_asks_observed,
            observed_for: Duration::ZERO,
        });
    }

    while started.elapsed() < observation_window && !merge_observed && !fallback_asks_observed {
        tokio::time::sleep(Duration::from_secs(1)).await;
        truth = fetch_direct_market_position_truth(data_api_base_url, user, condition_id)
            .await
            .context("failed to refresh post-decision direct truth")?;
        merge_observed =
            initial_pair_amount > Decimal::ZERO && truth.pair_amount() < initial_pair_amount;
        fallback_asks_observed =
            inventory_ask_submitted && inventory_asks_still_visible(engine, condition_id).await;
    }

    Ok(PostDecisionFlowObservation {
        truth,
        merge_observed,
        fallback_asks_observed,
        observed_for: started.elapsed(),
    })
}

fn inventory_ask_submitted(events: &[EventEnvelope]) -> bool {
    events.iter().any(|event| {
        if event.event_type != EventType::OrderSubmitted {
            return false;
        }
        serde_json::from_value::<OrderSubmittedPayload>(event.payload.clone())
            .ok()
            .is_some_and(|payload| {
                payload.origin.as_deref() == Some("inventory_ask")
                    || payload.role.as_deref() == Some("ask_inventory")
            })
    })
}

async fn inventory_asks_still_visible(engine: &LiveEngine, condition_id: &str) -> bool {
    if engine
        .order_manager
        .get_market_orders(condition_id)
        .await
        .iter()
        .any(|tracked| tracked.leg.is_ask())
    {
        return true;
    }

    match engine.trading_client.get_open_orders(None).await {
        Ok(open_orders) => open_orders
            .iter()
            .any(|order| order.condition_id == condition_id && order.side == Side::Sell),
        Err(_) => false,
    }
}

fn classify_flow_status(
    post_sync_net_exposure: Option<Decimal>,
    max_post_sync_net_exposure: Decimal,
    planned_hedge_shares: Option<Decimal>,
    planned_sellback_shares: Option<Decimal>,
    hedge_exit: Option<&HedgeExitPathPayload>,
    hedge_intent: Option<&HedgeIntentPayload>,
    observation: &PostDecisionFlowObservation,
    sellback_confirmation: &SellbackConfirmationObservation,
) -> FlowStatus {
    let post_sync_directional = post_sync_net_exposure
        .map(|exposure| exposure.abs() > max_post_sync_net_exposure)
        .unwrap_or(true);
    let direct_directional = observation.truth.net_exposure_abs() > max_post_sync_net_exposure;
    let confirmed_positions_lag = hedge_exit.is_some_and(|exit| {
        is_confirmed_sellback_positions_lag(
            exit,
            observation,
            max_post_sync_net_exposure,
            sellback_confirmation,
        )
    });
    if post_sync_directional || (direct_directional && !confirmed_positions_lag) {
        return FlowStatus::DirectionalResidual;
    }
    if let Some(exit) = hedge_exit {
        return match exit.exit_path_status.as_str() {
            "sellback_complete" | "no_exit_needed" => FlowStatus::SellbackCompleted,
            "merge_succeeded" => FlowStatus::MergeCompleted,
            "merge_attempted" => {
                if observation.merge_observed {
                    FlowStatus::MergeCompleted
                } else {
                    FlowStatus::FlowInconclusive
                }
            }
            "fallback_asks_placed" => FlowStatus::FallbackAsksPlaced,
            "merge_failed" | "fallback_asks_failed" | "pair_left_idle" => FlowStatus::PairLeftIdle,
            "directional_residual" => FlowStatus::DirectionalResidual,
            _ => FlowStatus::FlowInconclusive,
        };
    }

    let planned_hedge_shares = planned_hedge_shares.unwrap_or(Decimal::ZERO);
    let planned_sellback_shares = planned_sellback_shares.unwrap_or(Decimal::ZERO);

    if planned_hedge_shares <= Decimal::ZERO && planned_sellback_shares > Decimal::ZERO {
        return FlowStatus::SellbackCompleted;
    }
    if hedge_intent
        .map(|intent| intent.hedge_side.as_str() == "SELL")
        .unwrap_or(false)
    {
        return FlowStatus::SellbackCompleted;
    }
    if observation.merge_observed {
        return FlowStatus::MergeCompleted;
    }
    if observation.fallback_asks_observed {
        return FlowStatus::FallbackAsksPlaced;
    }
    if observation.truth.pair_amount() > Decimal::ZERO {
        return FlowStatus::PairLeftIdle;
    }
    FlowStatus::FlowInconclusive
}

fn is_confirmed_sellback_positions_lag(
    exit: &HedgeExitPathPayload,
    observation: &PostDecisionFlowObservation,
    max_post_sync_net_exposure: Decimal,
    sellback_confirmation: &SellbackConfirmationObservation,
) -> bool {
    sellback_confirmation.is_confirmed_before_cleanup()
        && exit.exit_path_status == "sellback_complete"
        && exit.post_sync_net_exposure.abs() <= max_post_sync_net_exposure
        && observation.truth.net_exposure_abs() > max_post_sync_net_exposure
        && observation.truth.pair_amount() <= Decimal::ZERO
}

fn derive_merge_status(
    hedge_exit: Option<&HedgeExitPathPayload>,
    observation: &PostDecisionFlowObservation,
) -> Option<String> {
    hedge_exit
        .map(|payload| {
            if payload.exit_path_status == "merge_succeeded" || payload.merge_tx_hash.is_some() {
                "succeeded".to_string()
            } else if payload.exit_path_status == "merge_failed"
                || payload.merge_failure_reason.is_some()
            {
                "failed".to_string()
            } else if payload.merge_attempted || payload.exit_path_status == "merge_attempted" {
                "attempted".to_string()
            } else if payload.merge_eligible_pairs <= Decimal::ZERO {
                "not_needed".to_string()
            } else if !payload.ctf_merge_configured {
                "not_configured".to_string()
            } else {
                "not_attempted".to_string()
            }
        })
        .or_else(|| {
            Some(if observation.merge_observed {
                "observed".to_string()
            } else {
                "not_observed".to_string()
            })
        })
}

fn derive_fallback_status(
    hedge_exit: Option<&HedgeExitPathPayload>,
    observation: &PostDecisionFlowObservation,
) -> Option<String> {
    hedge_exit
        .map(|payload| {
            if payload.exit_path_status == "fallback_asks_placed"
                || (payload.fallback_asks_attempted && payload.fallback_ask_count > 0)
            {
                "placed".to_string()
            } else if payload.exit_path_status == "fallback_asks_failed"
                || (payload.fallback_asks_attempted && payload.fallback_ask_count == 0)
            {
                "failed".to_string()
            } else if payload.merge_eligible_pairs <= Decimal::ZERO {
                "not_needed".to_string()
            } else if payload.fallback_asks_attempted {
                "attempted".to_string()
            } else {
                "skipped".to_string()
            }
        })
        .or_else(|| {
            Some(if observation.fallback_asks_observed {
                "observed".to_string()
            } else {
                "not_observed".to_string()
            })
        })
}

async fn observe_hedge_verification(
    engine: &LiveEngine,
    condition_id: &str,
    hedge_result: Option<&HedgeResultPayload>,
) -> HedgeVerificationObservation {
    let Some(result) = hedge_result else {
        return HedgeVerificationObservation::default();
    };
    let production = production_hedge_verification_evidence(result);
    let raw_status = result.hedge_leg_status.as_deref();
    if raw_status != Some("unverified") {
        return resolve_hedge_verification(
            raw_status,
            result.hedge_order_id.as_deref(),
            &production,
            None,
            None,
            false,
        );
    }

    let Some(order_id) = result.hedge_order_id.as_deref().filter(|id| !id.is_empty()) else {
        return resolve_hedge_verification(raw_status, None, &production, None, None, false);
    };

    let lookup_result = engine.trading_client.get_order(order_id).await;
    let lookup_error = lookup_result.is_err();
    let lookup_order = match lookup_result {
        Ok(order) => order,
        Err(_) => None,
    };
    let open_order = match engine
        .trading_client
        .get_open_orders(Some(condition_id))
        .await
    {
        Ok(orders) => orders.into_iter().find(|order| order.id == order_id),
        Err(_) => None,
    };

    resolve_hedge_verification(
        raw_status,
        Some(order_id),
        &production,
        lookup_order.as_ref(),
        open_order.as_ref(),
        lookup_error,
    )
}

async fn observe_sellback_confirmation(
    engine: &LiveEngine,
    condition_id: &str,
    hedge_result: Option<&HedgeResultPayload>,
    planned_sellback_shares: Option<Decimal>,
) -> SellbackConfirmationObservation {
    let Some(result) = hedge_result else {
        return SellbackConfirmationObservation::not_applicable(
            "hedge result missing before cleanup; no production sellback to confirm",
        );
    };
    if !sellback_confirmation_applicable(result) {
        return SellbackConfirmationObservation::not_applicable(
            "production did not attempt a sellback before cleanup",
        );
    }

    let production = production_sellback_confirmation_evidence(result);
    let Some(order_id) = result.sellback_order_id.as_deref().filter(|id| !id.is_empty()) else {
        return resolve_sellback_confirmation(
            None,
            &production,
            None,
            None,
            false,
            planned_sellback_shares,
        );
    };
    let (lookup_order, open_order, lookup_error) =
        lookup_sellback_confirmation_order(engine, condition_id, order_id).await;

    resolve_sellback_confirmation(
        Some(order_id),
        &production,
        lookup_order.as_ref(),
        open_order.as_ref(),
        lookup_error,
        planned_sellback_shares,
    )
}

fn confirm_sellback_before_cleanup(
    confirmation: SellbackConfirmationObservation,
    hedge_result: Option<&HedgeResultPayload>,
    hedge_exit: Option<&HedgeExitPathPayload>,
    post_decision_snapshot: &DirectTruthSnapshot,
    max_post_sync_net_exposure: Decimal,
) -> SellbackConfirmationObservation {
    if confirmation.is_confirmed_before_cleanup() || confirmation.status == "not_applicable" {
        return confirmation;
    }

    let Some(result) = hedge_result else {
        return confirmation;
    };
    let Some(exit) = hedge_exit else {
        return confirmation;
    };
    if !sellback_confirmation_applicable(result) {
        return confirmation;
    }

    let internal_flat = exit.post_sync_net_exposure.abs() <= max_post_sync_net_exposure
        && exit.post_sync_complete_sets <= Decimal::ZERO;
    if !internal_flat {
        return confirmation;
    }

    if exit.exit_path_status == "sellback_complete"
        && exit.post_sync_source == "execution_confirmed_sellback"
        && result.sellback_leg_status.as_deref() == Some("success")
    {
        return SellbackConfirmationObservation::confirmed(format!(
            "production emitted post_sync_source=execution_confirmed_sellback with sellback_complete before cleanup, so Layer 3 treated the production execution-confirmed sellback path as pre-cleanup confirmation{}",
            format_production_sellback_evidence(result),
        ));
    }

    let post_decision_flat = post_decision_snapshot.truth.net_exposure_abs()
        <= max_post_sync_net_exposure
        && post_decision_snapshot.truth.pair_amount() <= Decimal::ZERO;
    if !post_decision_flat || !matches!(exit.exit_path_status.as_str(), "sellback_complete" | "no_exit_needed")
    {
        return confirmation;
    }

    SellbackConfirmationObservation::confirmed(format!(
        "post_decision funded-wallet truth was already flat before cleanup ({}), so production resolved inventory before manual_live_probe_cleanup{}",
        format_direct_truth_snapshot(post_decision_snapshot),
        format_production_sellback_evidence(result),
    ))
}

async fn lookup_sellback_confirmation_order(
    engine: &LiveEngine,
    condition_id: &str,
    order_id: &str,
) -> (Option<LiveOrder>, Option<LiveOrder>, bool) {
    let mut lookup_error = false;
    let mut lookup_order = None;
    let mut open_order = None;

    for attempt in 0..SELLBACK_CONFIRMATION_LOOKUP_ATTEMPTS {
        let lookup_result = engine.trading_client.get_order(order_id).await;
        lookup_error = lookup_result.is_err();
        lookup_order = match lookup_result {
            Ok(order) => order,
            Err(_) => None,
        };
        open_order = match engine.trading_client.get_open_orders(Some(condition_id)).await {
            Ok(orders) => orders.into_iter().find(|order| order.id == order_id),
            Err(_) => None,
        };

        if lookup_order.is_some()
            || open_order.is_some()
            || attempt + 1 == SELLBACK_CONFIRMATION_LOOKUP_ATTEMPTS
        {
            break;
        }

        sleep(Duration::from_millis(SELLBACK_CONFIRMATION_LOOKUP_RETRY_MS)).await;
    }

    (lookup_order, open_order, lookup_error)
}

fn sellback_confirmation_applicable(result: &HedgeResultPayload) -> bool {
    result
        .sellback_leg_status
        .as_deref()
        .is_some_and(|status| status != "skipped")
        || result
            .sellback_order_id
            .as_deref()
            .is_some_and(|order_id| !order_id.is_empty())
        || result.sellback_response_status.is_some()
        || result.sellback_lookup_status.is_some()
        || result.sellback_lookup_matched_shares.is_some()
        || result
            .sellback_trade_ids
            .as_ref()
            .is_some_and(|trade_ids| !trade_ids.is_empty())
}

fn production_sellback_confirmation_evidence(
    result: &HedgeResultPayload,
) -> ProductionSellbackConfirmationEvidence {
    ProductionSellbackConfirmationEvidence {
        response_status: result.sellback_response_status.clone(),
        lookup_status: result.sellback_lookup_status.clone(),
        lookup_matched_shares: result.sellback_lookup_matched_shares,
        lookup_error: result.sellback_lookup_error.clone(),
        trade_ids: result
            .sellback_trade_ids
            .clone()
            .filter(|trade_ids| !trade_ids.is_empty()),
    }
}

fn resolve_sellback_confirmation(
    sellback_order_id: Option<&str>,
    production: &ProductionSellbackConfirmationEvidence,
    lookup_order: Option<&LiveOrder>,
    open_order: Option<&LiveOrder>,
    lookup_error: bool,
    requested_sellback_shares: Option<Decimal>,
) -> SellbackConfirmationObservation {
    let production_evidence = format_sellback_confirmation_production_evidence(production);
    let full_fill_confirmed = |matched: Decimal| {
        requested_sellback_shares
            .is_some_and(|requested| requested > Decimal::ZERO && matched >= requested)
    };
    if let Some(trade_ids) = production.trade_ids.as_ref().filter(|trade_ids| !trade_ids.is_empty())
    {
        return SellbackConfirmationObservation::confirmed(format!(
            "production sellback was confirmed before cleanup via trade_ids={}{}",
            trade_ids.join(","),
            production_evidence,
        ));
    }
    if production.lookup_status.as_deref() == Some("matched") {
        return SellbackConfirmationObservation::confirmed(format!(
            "production sellback was confirmed before cleanup via lookup_status=matched{}",
            production_evidence,
        ));
    }
    if production
        .lookup_matched_shares
        .is_some_and(full_fill_confirmed)
    {
        return SellbackConfirmationObservation::confirmed(format!(
            "production sellback was confirmed before cleanup via lookup_matched_shares={}{}",
            production.lookup_matched_shares.unwrap_or(Decimal::ZERO),
            production_evidence,
        ));
    }

    let Some(order_id) = sellback_order_id.filter(|order_id| !order_id.is_empty()) else {
        return SellbackConfirmationObservation::unconfirmed(format!(
            "production sellback remained unconfirmed before cleanup because response-only evidence was insufficient{}",
            production_evidence,
        ));
    };

    let observed_order = lookup_order.or(open_order);
    let observed_status = observed_order.map(|order| order_status_label(order.status));
    let observed_matched = observed_order.map(|order| order.size_matched);
    let observed_evidence = format!(
        " (order_id={order_id} observed_status={} observed_matched_shares={})",
        observed_status.unwrap_or(if lookup_error { "error" } else { "missing" }),
        observed_matched
            .map(|matched| matched.to_string())
            .unwrap_or_else(|| "unknown".to_string()),
    );

    if let Some(order) = observed_order {
        if order.status == OrderStatus::Matched || full_fill_confirmed(order.size_matched) {
            return SellbackConfirmationObservation::confirmed(format!(
                "production sellback was confirmed before cleanup via harness order lookup{}{}",
                observed_evidence, production_evidence,
            ));
        }
        if matches!(order.status, OrderStatus::Live | OrderStatus::Delayed) || open_order.is_some()
        {
            return SellbackConfirmationObservation::unconfirmed(format!(
                "production sellback remained unconfirmed before cleanup because the sellback order was still open{}{}",
                observed_evidence, production_evidence,
            ));
        }
        if matches!(order.status, OrderStatus::Cancelled | OrderStatus::Invalid)
            || order.size_matched <= Decimal::ZERO
        {
            return SellbackConfirmationObservation::unconfirmed(format!(
                "production sellback remained unconfirmed before cleanup because lookup showed no fill{}{}",
                observed_evidence, production_evidence,
            ));
        }
    }

    if lookup_error {
        return SellbackConfirmationObservation::unconfirmed(format!(
            "production sellback remained unconfirmed before cleanup because authenticated lookup failed{}{}",
            observed_evidence, production_evidence,
        ));
    }

    SellbackConfirmationObservation::unconfirmed(format!(
        "production sellback remained unconfirmed before cleanup because no confirming lookup evidence was found{}{}",
        observed_evidence, production_evidence,
    ))
}

fn format_sellback_confirmation_production_evidence(
    production: &ProductionSellbackConfirmationEvidence,
) -> String {
    let mut parts = Vec::new();
    if let Some(status) = production.response_status.as_deref() {
        parts.push(format!("response_status={status}"));
    }
    if let Some(status) = production.lookup_status.as_deref() {
        parts.push(format!("lookup_status={status}"));
    }
    if let Some(matched) = production.lookup_matched_shares {
        parts.push(format!("lookup_matched_shares={matched}"));
    }
    if let Some(error) = production.lookup_error.as_deref() {
        parts.push(format!("lookup_error={error}"));
    }

    if parts.is_empty() {
        String::new()
    } else {
        format!(" ({})", parts.join(" "))
    }
}

fn production_hedge_verification_evidence(
    result: &HedgeResultPayload,
) -> ProductionHedgeVerificationEvidence {
    ProductionHedgeVerificationEvidence {
        cancel_status: result.hedge_cancel_status.clone(),
        cancel_reason: result.hedge_cancel_reason.clone(),
        lookup_status: result.hedge_lookup_status.clone(),
        lookup_matched_shares: result.hedge_lookup_matched_shares,
        lookup_error: result.hedge_lookup_error.clone(),
        trade_ids: result
            .hedge_trade_ids
            .clone()
            .filter(|trade_ids| !trade_ids.is_empty()),
    }
}

fn resolve_hedge_verification(
    raw_status: Option<&str>,
    hedge_order_id: Option<&str>,
    production: &ProductionHedgeVerificationEvidence,
    lookup_order: Option<&LiveOrder>,
    open_order: Option<&LiveOrder>,
    lookup_error: bool,
) -> HedgeVerificationObservation {
    let mut observation = observation_from_production_evidence(production);

    match raw_status {
        Some("success") => {
            observation.verification_state = Some("verified_filled".to_string());
            return observation;
        }
        Some("failed") => {
            observation.verification_state = Some("verified_zero_fill".to_string());
            return observation;
        }
        Some("skipped") => {
            observation.verification_state = Some("skipped".to_string());
            return observation;
        }
        Some("unverified") => {}
        Some(_) => {
            observation.verification_state = Some("lookup_unavailable".to_string());
            return observation;
        }
        None => return observation,
    }

    let Some(_) = hedge_order_id.filter(|order_id| !order_id.is_empty()) else {
        observation.verification_state = Some("missing_order_id".to_string());
        return observation;
    };

    if let Some(production_state) = classify_production_hedge_verification(production) {
        observation.verification_state = Some(production_state.to_string());
        return observation;
    };

    let observed_order = lookup_order.or(open_order);
    observation.lookup_status = observed_order
        .map(|order| order_status_label(order.status).to_string())
        .or_else(|| lookup_error.then(|| "error".to_string()))
        .or_else(|| Some("missing".to_string()));
    observation.lookup_matched_shares = observed_order.map(|order| order.size_matched);

    if let Some(order) = observed_order {
        if order.size_matched > Decimal::ZERO || order.status == OrderStatus::Matched {
            observation.verification_state = Some("external_fill_confirmed".to_string());
            return observation;
        }
        if matches!(order.status, OrderStatus::Live | OrderStatus::Delayed) || open_order.is_some()
        {
            observation.verification_state = Some("resting_open".to_string());
            return observation;
        }
        if matches!(order.status, OrderStatus::Cancelled | OrderStatus::Invalid)
            || order.size_matched <= Decimal::ZERO
        {
            observation.verification_state = Some("external_zero_fill".to_string());
            return observation;
        }
    }

    observation.verification_state = Some("lookup_unavailable".to_string());
    observation
}

fn observation_from_production_evidence(
    production: &ProductionHedgeVerificationEvidence,
) -> HedgeVerificationObservation {
    HedgeVerificationObservation {
        production_cancel_status: production.cancel_status.clone(),
        production_cancel_reason: production.cancel_reason.clone(),
        production_lookup_status: production.lookup_status.clone(),
        production_lookup_matched_shares: production.lookup_matched_shares,
        production_lookup_error: production.lookup_error.clone(),
        production_trade_ids: production.trade_ids.clone(),
        ..Default::default()
    }
}

fn classify_production_hedge_verification(
    production: &ProductionHedgeVerificationEvidence,
) -> Option<&'static str> {
    if production_lookup_confirms_fill(production) {
        return Some("production_fill_confirmed");
    }

    match production.lookup_status.as_deref() {
        Some("live") => Some("resting_open"),
        Some("cancelled") | Some("invalid") => Some("production_zero_fill_confirmed"),
        Some("missing") => Some(match production.cancel_status.as_deref() {
            Some("confirmed") => "production_lookup_missing_after_cancel_confirmed",
            _ => "production_lookup_missing_after_cancel_unknown",
        }),
        Some("error") => Some("production_lookup_error"),
        _ => None,
    }
}

fn order_status_label(status: OrderStatus) -> &'static str {
    match status {
        OrderStatus::Live => "live",
        OrderStatus::Matched => "matched",
        OrderStatus::Delayed => "live",
        OrderStatus::Cancelled => "cancelled",
        OrderStatus::Invalid => "invalid",
    }
}

fn production_lookup_confirms_fill(production: &ProductionHedgeVerificationEvidence) -> bool {
    production.lookup_status.as_deref() == Some("matched")
        || production
            .lookup_matched_shares
            .is_some_and(|matched| matched > Decimal::ZERO)
        || production
            .trade_ids
            .as_ref()
            .is_some_and(|trade_ids| !trade_ids.is_empty())
}

fn reconcile_production_truth(
    hedge_result: Option<&HedgeResultPayload>,
    hedge_exit: Option<&HedgeExitPathPayload>,
    observation: &PostDecisionFlowObservation,
    max_post_sync_net_exposure: Decimal,
    sellback_confirmation: &SellbackConfirmationObservation,
) -> TruthReconciliationOutcome {
    let direct_snapshot = post_decision_direct_snapshot(observation);
    if let Some(reason) = hedge_result.and_then(production_sellback_failure_reason) {
        return TruthReconciliationOutcome {
            status: "failed".to_string(),
            reason,
            warning_status: None,
            warning_reason: None,
        };
    }
    let Some(exit) = hedge_exit else {
        if hedge_result.is_some_and(|payload| payload.result_status == "success") {
            return TruthReconciliationOutcome {
                status: "failed".to_string(),
                reason: "successful hedge result recorded without required hedge_exit_path_recorded; missing exit observability prevents reconciliation against direct funded-wallet truth".to_string(),
                warning_status: None,
                warning_reason: None,
            };
        }
        return TruthReconciliationOutcome {
            status: "event_missing".to_string(),
            reason:
                "hedge exit path event missing; using legacy flow inference against direct funded-wallet truth"
                    .to_string(),
            warning_status: None,
            warning_reason: None,
        };
    };

    let direct_pair_amount = direct_snapshot.truth.pair_amount();
    let direct_directional = direct_snapshot.truth.net_exposure_abs() > max_post_sync_net_exposure;
    let internal_directional = exit.post_sync_net_exposure.abs() > max_post_sync_net_exposure;

    if !internal_directional && direct_directional {
        if is_confirmed_sellback_positions_lag(
            exit,
            observation,
            max_post_sync_net_exposure,
            sellback_confirmation,
        ) {
            return TruthReconciliationOutcome {
                status: "confirmed".to_string(),
                reason: "production sellback was confirmed before cleanup; lagging post_decision funded-wallet truth was retained as a warning".to_string(),
                warning_status: Some("positions_lag_after_confirmed_execution".to_string()),
                warning_reason: Some(format!(
                    "production exit event recorded neutral post-sync inventory yes={} no={} net={}, production sellback was confirmed before cleanup ({}) but {} remained directional net={}; treating this as positions lag after confirmed execution",
                    exit.post_sync_yes_size,
                    exit.post_sync_no_size,
                    exit.post_sync_net_exposure,
                    sellback_confirmation.reason,
                    format_direct_truth_snapshot(&direct_snapshot),
                    direct_snapshot.truth.net_exposure_abs(),
                )),
            };
        }
        return TruthReconciliationOutcome {
            status: "failed".to_string(),
            reason: format!(
                "production exit event recorded neutral post-sync inventory yes={} no={} net={}, but {} remained directional net={}",
                exit.post_sync_yes_size,
                exit.post_sync_no_size,
                exit.post_sync_net_exposure,
                format_direct_truth_snapshot(&direct_snapshot),
                direct_snapshot.truth.net_exposure_abs(),
            ),
            warning_status: None,
            warning_reason: None,
        };
    }

    if exit.post_sync_complete_sets <= Decimal::ZERO && direct_pair_amount > Decimal::ZERO {
        return TruthReconciliationOutcome {
            status: "failed".to_string(),
            reason: format!(
                "production exit event recorded no complete sets after sync, but {} still shows paired inventory pairs={}",
                format_direct_truth_snapshot(&direct_snapshot),
                direct_pair_amount,
            ),
            warning_status: None,
            warning_reason: None,
        };
    }

    if matches!(
        exit.exit_path_status.as_str(),
        "sellback_complete" | "no_exit_needed"
    ) && direct_pair_amount > Decimal::ZERO
    {
        return TruthReconciliationOutcome {
            status: "failed".to_string(),
            reason: format!(
                "production exit event recorded {} with no paired inventory required, but {} still shows {} complete sets",
                exit.exit_path_status,
                format_direct_truth_snapshot(&direct_snapshot),
                direct_pair_amount,
            ),
            warning_status: None,
            warning_reason: None,
        };
    }

    if exit.exit_path_status == "merge_succeeded"
        && exit.post_sync_complete_sets > Decimal::ZERO
        && direct_pair_amount >= exit.post_sync_complete_sets
    {
        return TruthReconciliationOutcome {
            status: "failed".to_string(),
            reason: format!(
                "production exit event recorded merge_succeeded for {} complete sets, but {} still shows {} complete sets",
                exit.post_sync_complete_sets,
                format_direct_truth_snapshot(&direct_snapshot),
                direct_pair_amount,
            ),
            warning_status: None,
            warning_reason: None,
        };
    }

    TruthReconciliationOutcome {
        status: "confirmed".to_string(),
        reason: format!(
            "{} remained consistent with production exit event",
            format_direct_truth_snapshot(&direct_snapshot),
        ),
        warning_status: None,
        warning_reason: None,
    }
}

fn production_sellback_failure_reason(result: &HedgeResultPayload) -> Option<String> {
    let evidence = format_production_sellback_evidence(result);
    match result.sellback_leg_status.as_deref() {
        Some("unverified") => Some(format!(
            "production sellback verification remained unverified{}",
            evidence
        )),
        Some("failed") => Some(format!(
            "production sellback verification confirmed zero fill{}",
            evidence
        )),
        _ => None,
    }
}

fn format_production_sellback_evidence(result: &HedgeResultPayload) -> String {
    let mut parts = Vec::new();
    if let Some(status) = result.sellback_response_status.as_deref() {
        parts.push(format!("response_status={status}"));
    }
    if let Some(status) = result.sellback_lookup_status.as_deref() {
        parts.push(format!("lookup_status={status}"));
    }
    if let Some(matched) = result.sellback_lookup_matched_shares {
        parts.push(format!("lookup_matched_shares={matched}"));
    }
    if let Some(error) = result.sellback_lookup_error.as_deref() {
        parts.push(format!("lookup_error={error}"));
    }
    if let Some(trade_ids) = result.sellback_trade_ids.as_ref().filter(|trade_ids| !trade_ids.is_empty()) {
        parts.push(format!("trade_ids={}", trade_ids.join(",")));
    }

    if parts.is_empty() {
        String::new()
    } else {
        format!(" ({})", parts.join(" "))
    }
}

fn evaluate_standard_pass(
    meta_pass: bool,
    decision_status: DecisionAuditStatus,
    flow_status: FlowStatus,
    post_sync_net_exposure: Option<Decimal>,
    max_post_sync_net_exposure: Decimal,
    truth_reconciliation_status: Option<&str>,
) -> bool {
    if !meta_pass || decision_status == DecisionAuditStatus::Failed {
        return false;
    }
    if truth_reconciliation_status == Some("failed") {
        return false;
    }
    if post_sync_net_exposure
        .map(|exposure| exposure.abs() > max_post_sync_net_exposure)
        .unwrap_or(true)
    {
        return false;
    }

    matches!(
        flow_status,
        FlowStatus::SellbackCompleted | FlowStatus::MergeCompleted | FlowStatus::FallbackAsksPlaced
    )
}

fn preview_ws_text(text: &str) -> String {
    const PREVIEW_LIMIT: usize = 200;
    if text.len() <= PREVIEW_LIMIT {
        text.to_string()
    } else {
        format!("{}...", &text[..PREVIEW_LIMIT])
    }
}

fn remaining_probe_time(started_at: Instant, timeout_secs: u64) -> Duration {
    Duration::from_secs(timeout_secs).saturating_sub(started_at.elapsed())
}

fn cleanup_probe_time(started_at: Instant, timeout_secs: u64) -> Duration {
    remaining_probe_time(started_at, timeout_secs).max(Duration::from_secs(
        LIVE_PROBE_MIN_CLEANUP_WAIT_SECS,
    ))
}

async fn wait_for_direct_truth_to_match_baseline(
    data_api_base_url: &str,
    user: &str,
    condition_id: &str,
    baseline: &DirectMarketPositionTruth,
    timeout_window: Duration,
    context_label: &str,
) -> Result<CleanupTruthObservation> {
    let started = Instant::now();
    let poll_interval = Duration::from_secs(1);
    let required_stable_window = timeout_window.min(Duration::from_secs(8));
    let mut last_truth = fetch_direct_market_position_truth(data_api_base_url, user, condition_id)
        .await
        .with_context(|| format!("failed to fetch {}", context_label))?;
    if timeout_window.is_zero() {
        return Ok(CleanupTruthObservation {
            truth: last_truth,
            stable_baseline_confirmed: false,
            observed_for: Duration::ZERO,
        });
    }
    let mut stable_started = if last_truth.matches(baseline) {
        Some(Instant::now())
    } else {
        None
    };

    while started.elapsed() < timeout_window {
        if let Some(stable_start) = stable_started {
            if stable_start.elapsed() >= required_stable_window {
                return Ok(CleanupTruthObservation {
                    truth: last_truth,
                    stable_baseline_confirmed: true,
                    observed_for: started.elapsed(),
                });
            }
        }

        tokio::time::sleep(poll_interval).await;
        last_truth = fetch_direct_market_position_truth(data_api_base_url, user, condition_id)
            .await
            .with_context(|| format!("failed to refresh {}", context_label))?;
        if last_truth.matches(baseline) {
            stable_started.get_or_insert_with(Instant::now);
        } else {
            stable_started = None;
        }
    }

    if stable_started.is_some_and(|stable_start| stable_start.elapsed() >= required_stable_window)
        && last_truth.matches(baseline)
    {
        return Ok(CleanupTruthObservation {
            truth: last_truth,
            stable_baseline_confirmed: true,
            observed_for: started.elapsed(),
        });
    }

    Ok(CleanupTruthObservation {
        truth: last_truth,
        stable_baseline_confirmed: false,
        observed_for: started.elapsed(),
    })
}

async fn wait_for_direct_cleanup_truth(
    data_api_base_url: &str,
    user: &str,
    condition_id: &str,
    baseline: &DirectMarketPositionTruth,
    timeout_window: Duration,
) -> Result<CleanupTruthObservation> {
    wait_for_direct_truth_to_match_baseline(
        data_api_base_url,
        user,
        condition_id,
        baseline,
        timeout_window,
        "direct cleanup truth",
    )
    .await
}

fn build_cleanup_status(
    cleanup_pass: bool,
    cleanup_truth: &CleanupTruthObservation,
    baseline: &DirectMarketPositionTruth,
    user: &str,
) -> String {
    let snapshot = cleanup_direct_snapshot(cleanup_truth);
    if cleanup_pass {
        return format!(
            "clean {} user={}",
            format_direct_truth_snapshot(&snapshot),
            user
        );
    }
    if snapshot.truth.matches(baseline) {
        return format!(
            "cleanup_unconfirmed {} user={}",
            format_direct_truth_snapshot(&snapshot),
            user
        );
    }

    format!(
        "residual_inventory {} baseline_yes={} baseline_no={} user={}",
        format_direct_truth_snapshot(&snapshot),
        baseline.yes_size,
        baseline.no_size,
        user
    )
}

async fn fetch_direct_market_position_truth(
    data_api_base_url: &str,
    user: &str,
    condition_id: &str,
) -> Result<DirectMarketPositionTruth> {
    let client = Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .context("failed to build direct position truth client")?;
    let url = format!("{}/positions?user={}", data_api_base_url, user);
    let response = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("failed to fetch direct positions for {}", user))?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        bail!("direct positions API failed ({}): {}", status, body);
    }

    let entries: Vec<DirectPositionEntry> = response
        .json()
        .await
        .context("failed to parse direct positions response")?;
    Ok(summarize_direct_market_positions(entries, condition_id))
}

fn summarize_direct_market_positions(
    entries: Vec<DirectPositionEntry>,
    condition_id: &str,
) -> DirectMarketPositionTruth {
    let mut truth = DirectMarketPositionTruth::default();
    for entry in entries
        .into_iter()
        .filter(|entry| entry.condition_id.as_deref() == Some(condition_id))
    {
        match entry.outcome.as_deref() {
            Some("Yes" | "YES") => truth.yes_size += entry.size,
            Some("No" | "NO") => truth.no_size += entry.size,
            _ => {}
        }
    }
    truth
}

fn direct_truth_snapshot(
    stage: &'static str,
    truth: &DirectMarketPositionTruth,
    observed_for: Duration,
) -> DirectTruthSnapshot {
    DirectTruthSnapshot {
        stage,
        truth: truth.clone(),
        observed_for,
    }
}

fn post_decision_direct_snapshot(observation: &PostDecisionFlowObservation) -> DirectTruthSnapshot {
    direct_truth_snapshot(
        "post_decision",
        &observation.truth,
        observation.observed_for,
    )
}

fn cleanup_direct_snapshot(observation: &CleanupTruthObservation) -> DirectTruthSnapshot {
    direct_truth_snapshot("cleanup", &observation.truth, observation.observed_for)
}

fn format_direct_truth_snapshot(snapshot: &DirectTruthSnapshot) -> String {
    format!(
        "stage={} direct_yes={} direct_no={} observed_for={}s",
        snapshot.stage,
        snapshot.truth.yes_size,
        snapshot.truth.no_size,
        snapshot.observed_for.as_secs(),
    )
}

fn deserialize_direct_position_size<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<DirectPositionSize>::deserialize(deserializer)?;
    match value {
        Some(DirectPositionSize::String(raw)) => raw.parse().map_err(serde::de::Error::custom),
        Some(DirectPositionSize::Float(raw)) => raw
            .to_string()
            .parse::<Decimal>()
            .map_err(serde::de::Error::custom),
        Some(DirectPositionSize::Integer(raw)) => Ok(Decimal::from(raw)),
        None => Ok(Decimal::ZERO),
    }
}

#[test]
fn summarize_direct_market_positions_aggregates_yes_and_no() {
    let entries = vec![
        DirectPositionEntry {
            condition_id: Some("target".to_string()),
            outcome: Some("Yes".to_string()),
            size: dec!(5.007056),
        },
        DirectPositionEntry {
            condition_id: Some("target".to_string()),
            outcome: Some("NO".to_string()),
            size: dec!(5),
        },
        DirectPositionEntry {
            condition_id: Some("other".to_string()),
            outcome: Some("Yes".to_string()),
            size: dec!(99),
        },
    ];

    let truth = summarize_direct_market_positions(entries, "target");

    assert_eq!(truth.yes_size, dec!(5.007056));
    assert_eq!(truth.no_size, dec!(5));
}

#[test]
fn build_cleanup_status_uses_cleanup_stage_snapshot() {
    let cleanup_truth = CleanupTruthObservation {
        truth: DirectMarketPositionTruth {
            yes_size: Decimal::ZERO,
            no_size: dec!(5),
        },
        stable_baseline_confirmed: false,
        observed_for: Duration::from_secs(6),
    };

    let status = build_cleanup_status(
        false,
        &cleanup_truth,
        &DirectMarketPositionTruth::default(),
        "user-1",
    );

    assert!(status.contains("stage=cleanup"));
    assert!(status.contains("direct_yes=0"));
    assert!(status.contains("direct_no=5"));
    assert!(status.contains("observed_for=6s"));
}

#[test]
fn cleanup_probe_time_enforces_a_small_post_timeout_cleanup_window() {
    let started = Instant::now() - Duration::from_secs(60);

    assert_eq!(
        cleanup_probe_time(started, 60),
        Duration::from_secs(LIVE_PROBE_MIN_CLEANUP_WAIT_SECS)
    );
}

#[test]
fn derive_live_probe_marketable_limit_price_uses_lowest_ask_plus_tick() {
    let book = OrderBookSnapshot {
        token_id: "token".to_string(),
        exchange_ts: None,
        ingest_ts: Utc::now(),
        bids: vec![],
        asks: vec![
            crate::models::PriceLevel {
                price: dec!(0.99),
                size: dec!(10),
            },
            crate::models::PriceLevel {
                price: dec!(0.80),
                size: dec!(10),
            },
        ],
    };

    let price = derive_live_probe_marketable_limit_price(
        Some(&book),
        dec!(0.01),
        "trigger",
        dec!(0.90),
    )
    .expect("marketable limit should derive");

    assert_eq!(price, dec!(0.81));
}

#[test]
fn derive_live_probe_marketable_limit_price_rejects_when_cap_is_too_low() {
    let book = OrderBookSnapshot {
        token_id: "token".to_string(),
        exchange_ts: None,
        ingest_ts: Utc::now(),
        bids: vec![],
        asks: vec![crate::models::PriceLevel {
            price: dec!(0.80),
            size: dec!(10),
        }],
    };

    let err = derive_live_probe_marketable_limit_price(
        Some(&book),
        dec!(0.01),
        "trigger",
        dec!(0.79),
    )
    .expect_err("low cap should fail");

    assert!(err.to_string().contains("exceeded configured cap"));
}

#[test]
fn direct_market_position_truth_matches_requires_exact_baseline() {
    let baseline = DirectMarketPositionTruth {
        yes_size: Decimal::ZERO,
        no_size: Decimal::ZERO,
    };
    let residual = DirectMarketPositionTruth {
        yes_size: dec!(0.000001),
        no_size: Decimal::ZERO,
    };

    assert!(baseline.matches(&baseline));
    assert!(!residual.matches(&baseline));
}

#[test]
fn resolution_match_helper_detects_exact_split_match() {
    let resolution = HedgeResolution {
        hedge_shares: dec!(6),
        hedge_limit_price: dec!(0.27),
        sellback_shares: dec!(4),
        sellback_limit_price: dec!(0.73),
        unresolved_shares: Decimal::ZERO,
    };

    assert!(resolution_matches_observed(
        &resolution,
        ObservedResolutionSplit {
            hedge_shares: dec!(6),
            sellback_shares: dec!(4),
        }
    ));
    assert!(!resolution_matches_observed(
        &resolution,
        ObservedResolutionSplit {
            hedge_shares: dec!(10),
            sellback_shares: Decimal::ZERO,
        }
    ));
}

#[test]
fn evaluate_standard_pass_accepts_merge_completed_with_cleanup_left_separate() {
    assert!(evaluate_standard_pass(
        true,
        DecisionAuditStatus::Confirmed,
        FlowStatus::MergeCompleted,
        Some(Decimal::ZERO),
        dec!(0.5),
        Some("confirmed"),
    ));
}

#[test]
fn evaluate_standard_pass_accepts_fallback_asks_when_cleanup_can_fail() {
    assert!(evaluate_standard_pass(
        true,
        DecisionAuditStatus::Confirmed,
        FlowStatus::FallbackAsksPlaced,
        Some(Decimal::ZERO),
        dec!(0.5),
        Some("confirmed"),
    ));
}

#[test]
fn evaluate_standard_pass_rejects_idle_pairs() {
    assert!(!evaluate_standard_pass(
        true,
        DecisionAuditStatus::Confirmed,
        FlowStatus::PairLeftIdle,
        Some(Decimal::ZERO),
        dec!(0.5),
        Some("confirmed"),
    ));
}

#[test]
fn evaluate_standard_pass_keeps_cleanup_independent_from_strategy_success() {
    assert!(evaluate_standard_pass(
        true,
        DecisionAuditStatus::Inconclusive,
        FlowStatus::FallbackAsksPlaced,
        Some(Decimal::ZERO),
        dec!(0.5),
        Some("confirmed"),
    ));
}

#[test]
fn evaluate_standard_pass_rejects_clear_decision_contradiction() {
    assert!(!evaluate_standard_pass(
        true,
        DecisionAuditStatus::Failed,
        FlowStatus::MergeCompleted,
        Some(Decimal::ZERO),
        dec!(0.5),
        Some("confirmed"),
    ));
}

#[test]
fn evaluate_standard_pass_rejects_truth_reconciliation_failure() {
    assert!(!evaluate_standard_pass(
        true,
        DecisionAuditStatus::Confirmed,
        FlowStatus::FallbackAsksPlaced,
        Some(Decimal::ZERO),
        dec!(0.5),
        Some("failed"),
    ));
}

#[test]
fn classify_flow_status_marks_directional_residual_before_any_follow_through() {
    let observation = PostDecisionFlowObservation {
        truth: DirectMarketPositionTruth {
            yes_size: dec!(5),
            no_size: Decimal::ZERO,
        },
        merge_observed: false,
        fallback_asks_observed: false,
        observed_for: Duration::ZERO,
    };

    assert_eq!(
        classify_flow_status(
            Some(dec!(5)),
            dec!(0.5),
            Some(dec!(5)),
            Some(Decimal::ZERO),
            None,
            None,
            &observation,
            &SellbackConfirmationObservation::not_applicable("not applicable"),
        ),
        FlowStatus::DirectionalResidual
    );
}

#[test]
fn evaluate_decision_audit_marks_one_snapshot_match_as_inconclusive() {
    let scenario = sample_decision_audit_scenario();
    let intent = sample_decision_audit_intent(dec!(6), dec!(4));
    let verdict = evaluate_decision_audit(
        &scenario,
        None,
        Some(&intent),
        Some(&sample_book_audit_snapshot(vec![dec!(0.26), dec!(0.30)])),
        Some(&sample_book_audit_snapshot(vec![dec!(0.27), dec!(0.27)])),
    );

    assert_eq!(verdict.status, DecisionAuditStatus::Inconclusive);
}

#[test]
fn evaluate_decision_audit_fails_on_clear_planner_contradiction() {
    let scenario = sample_decision_audit_scenario();
    let intent = sample_decision_audit_intent(dec!(6), dec!(4));
    let verdict = evaluate_decision_audit(
        &scenario,
        None,
        Some(&intent),
        Some(&sample_book_audit_snapshot(vec![dec!(0.27), dec!(0.27)])),
        Some(&sample_book_audit_snapshot(vec![dec!(0.27), dec!(0.27)])),
    );

    assert_eq!(verdict.status, DecisionAuditStatus::Failed);
}

#[test]
fn evaluate_decision_audit_prefers_production_decision_event_reasoning() {
    let scenario = sample_decision_audit_scenario();
    let intent = sample_decision_audit_intent(dec!(6), dec!(4));
    let decision = sample_decision_payload(Decimal::ZERO, dec!(10), "budget_rerouted_to_sellback");
    let verdict = evaluate_decision_audit(
        &scenario,
        Some(&decision),
        Some(&intent),
        Some(&sample_book_audit_snapshot(vec![dec!(0.26), dec!(0.27)])),
        Some(&sample_book_audit_snapshot(vec![dec!(0.26), dec!(0.27)])),
    );

    assert_eq!(verdict.status, DecisionAuditStatus::Failed);
    assert!(verdict.reason.contains("production decision event"));
    assert!(verdict.reason.contains("budget_rerouted_to_sellback"));
}

#[test]
fn classify_flow_status_uses_exit_event_for_pair_left_idle() {
    let observation = PostDecisionFlowObservation {
        truth: DirectMarketPositionTruth {
            yes_size: dec!(2),
            no_size: dec!(2),
        },
        merge_observed: false,
        fallback_asks_observed: false,
        observed_for: Duration::ZERO,
    };

    assert_eq!(
        classify_flow_status(
            Some(Decimal::ZERO),
            dec!(0.5),
            Some(dec!(2)),
            Some(Decimal::ZERO),
            Some(&sample_exit_payload("pair_left_idle")),
            None,
            &observation,
            &SellbackConfirmationObservation::not_applicable("not applicable"),
        ),
        FlowStatus::PairLeftIdle
    );
}

#[test]
fn derive_merge_status_reports_failed_from_exit_event() {
    let observation = PostDecisionFlowObservation {
        truth: DirectMarketPositionTruth {
            yes_size: dec!(2),
            no_size: dec!(2),
        },
        merge_observed: false,
        fallback_asks_observed: false,
        observed_for: Duration::ZERO,
    };
    let mut exit = sample_exit_payload("fallback_asks_failed");
    exit.merge_attempted = true;
    exit.merge_failure_reason = Some("merge reverted".to_string());

    assert_eq!(
        derive_merge_status(Some(&exit), &observation).as_deref(),
        Some("failed")
    );
}

#[test]
fn resolve_hedge_verification_marks_external_fill_confirmed_from_matched_lookup() {
    let lookup_order = sample_live_order(OrderStatus::Matched, dec!(5));

    let observation = resolve_hedge_verification(
        Some("unverified"),
        Some("hedge-order"),
        &ProductionHedgeVerificationEvidence::default(),
        Some(&lookup_order),
        None,
        false,
    );

    assert_eq!(
        observation.verification_state.as_deref(),
        Some("external_fill_confirmed")
    );
    assert_eq!(observation.lookup_status.as_deref(), Some("matched"));
    assert_eq!(observation.lookup_matched_shares, Some(dec!(5)));
}

#[test]
fn resolve_hedge_verification_marks_resting_open_from_live_lookup() {
    let live_order = sample_live_order(OrderStatus::Live, Decimal::ZERO);

    let observation = resolve_hedge_verification(
        Some("unverified"),
        Some("hedge-order"),
        &ProductionHedgeVerificationEvidence::default(),
        Some(&live_order),
        None,
        false,
    );

    assert_eq!(
        observation.verification_state.as_deref(),
        Some("resting_open")
    );
    assert_eq!(observation.lookup_status.as_deref(), Some("live"));
    assert_eq!(observation.lookup_matched_shares, Some(Decimal::ZERO));
}

#[test]
fn resolve_hedge_verification_marks_external_zero_fill_from_invalid_lookup() {
    let invalid_order = sample_live_order(OrderStatus::Invalid, Decimal::ZERO);

    let observation = resolve_hedge_verification(
        Some("unverified"),
        Some("hedge-order"),
        &ProductionHedgeVerificationEvidence::default(),
        Some(&invalid_order),
        None,
        false,
    );

    assert_eq!(
        observation.verification_state.as_deref(),
        Some("external_zero_fill")
    );
    assert_eq!(observation.lookup_status.as_deref(), Some("invalid"));
    assert_eq!(observation.lookup_matched_shares, Some(Decimal::ZERO));
}

#[test]
fn resolve_hedge_verification_marks_missing_order_id_when_unverified_has_no_order() {
    let observation = resolve_hedge_verification(
        Some("unverified"),
        None,
        &ProductionHedgeVerificationEvidence::default(),
        None,
        None,
        false,
    );

    assert_eq!(
        observation.verification_state.as_deref(),
        Some("missing_order_id")
    );
    assert!(observation.lookup_status.is_none());
    assert!(observation.lookup_matched_shares.is_none());
}

#[test]
fn resolve_hedge_verification_marks_lookup_unavailable_on_missing_lookup() {
    let observation = resolve_hedge_verification(
        Some("unverified"),
        Some("hedge-order"),
        &ProductionHedgeVerificationEvidence::default(),
        None,
        None,
        true,
    );

    assert_eq!(
        observation.verification_state.as_deref(),
        Some("lookup_unavailable")
    );
    assert_eq!(observation.lookup_status.as_deref(), Some("error"));
    assert!(observation.lookup_matched_shares.is_none());
}

#[test]
fn resolve_hedge_verification_prefers_production_fill_confirmation() {
    let observation = resolve_hedge_verification(
        Some("unverified"),
        Some("hedge-order"),
        &ProductionHedgeVerificationEvidence {
            lookup_status: Some("matched".to_string()),
            lookup_matched_shares: Some(dec!(5)),
            trade_ids: Some(vec!["trade-1".to_string()]),
            ..Default::default()
        },
        None,
        None,
        false,
    );

    assert_eq!(
        observation.verification_state.as_deref(),
        Some("production_fill_confirmed")
    );
    assert_eq!(
        observation.production_lookup_status.as_deref(),
        Some("matched")
    );
    assert_eq!(
        observation.production_trade_ids.as_deref(),
        Some(&["trade-1".to_string()][..])
    );
}

#[test]
fn resolve_hedge_verification_marks_confirmed_cancel_missing_lookup() {
    let observation = resolve_hedge_verification(
        Some("unverified"),
        Some("hedge-order"),
        &ProductionHedgeVerificationEvidence {
            cancel_status: Some("confirmed".to_string()),
            lookup_status: Some("missing".to_string()),
            ..Default::default()
        },
        None,
        None,
        false,
    );

    assert_eq!(
        observation.verification_state.as_deref(),
        Some("production_lookup_missing_after_cancel_confirmed")
    );
}

#[test]
fn resolve_hedge_verification_marks_unknown_cancel_missing_lookup() {
    let observation = resolve_hedge_verification(
        Some("unverified"),
        Some("hedge-order"),
        &ProductionHedgeVerificationEvidence {
            cancel_status: Some("unknown".to_string()),
            lookup_status: Some("missing".to_string()),
            ..Default::default()
        },
        None,
        None,
        false,
    );

    assert_eq!(
        observation.verification_state.as_deref(),
        Some("production_lookup_missing_after_cancel_unknown")
    );
}

#[test]
fn resolve_hedge_verification_marks_production_lookup_error() {
    let observation = resolve_hedge_verification(
        Some("unverified"),
        Some("hedge-order"),
        &ProductionHedgeVerificationEvidence {
            cancel_status: Some("unknown".to_string()),
            lookup_status: Some("error".to_string()),
            lookup_error: Some("timeout".to_string()),
            ..Default::default()
        },
        None,
        None,
        false,
    );

    assert_eq!(
        observation.verification_state.as_deref(),
        Some("production_lookup_error")
    );
    assert_eq!(
        observation.production_lookup_error.as_deref(),
        Some("timeout")
    );
}

#[test]
fn resolve_sellback_confirmation_prefers_production_trade_ids() {
    let observation = resolve_sellback_confirmation(
        Some("sellback-order"),
        &ProductionSellbackConfirmationEvidence {
            response_status: Some("matched".to_string()),
            trade_ids: Some(vec!["trade-1".to_string()]),
            ..Default::default()
        },
        None,
        None,
        false,
        Some(dec!(5)),
    );

    assert_eq!(observation.status, "confirmed_before_cleanup");
    assert!(observation.reason.contains("trade_ids=trade-1"));
}

#[test]
fn resolve_sellback_confirmation_confirms_from_production_lookup() {
    let observation = resolve_sellback_confirmation(
        Some("sellback-order"),
        &ProductionSellbackConfirmationEvidence {
            response_status: Some("matched".to_string()),
            lookup_status: Some("matched".to_string()),
            ..Default::default()
        },
        None,
        None,
        false,
        Some(dec!(5)),
    );

    assert_eq!(observation.status, "confirmed_before_cleanup");
    assert!(observation.reason.contains("lookup_status=matched"));
}

#[test]
fn resolve_sellback_confirmation_confirms_from_harness_lookup() {
    let lookup_order = sample_live_order(OrderStatus::Matched, dec!(5));

    let observation = resolve_sellback_confirmation(
        Some("sellback-order"),
        &ProductionSellbackConfirmationEvidence {
            response_status: Some("matched".to_string()),
            ..Default::default()
        },
        Some(&lookup_order),
        None,
        false,
        Some(dec!(5)),
    );

    assert_eq!(observation.status, "confirmed_before_cleanup");
    assert!(observation.reason.contains("harness order lookup"));
}

#[test]
fn resolve_sellback_confirmation_marks_open_order_unconfirmed() {
    let open_order = sample_live_order(OrderStatus::Live, Decimal::ZERO);

    let observation = resolve_sellback_confirmation(
        Some("sellback-order"),
        &ProductionSellbackConfirmationEvidence {
            response_status: Some("matched".to_string()),
            ..Default::default()
        },
        None,
        Some(&open_order),
        false,
        Some(dec!(5)),
    );

    assert_eq!(observation.status, "unconfirmed_before_cleanup");
    assert!(observation.reason.contains("still open"));
}

#[test]
fn resolve_sellback_confirmation_rejects_response_only_evidence() {
    let observation = resolve_sellback_confirmation(
        None,
        &ProductionSellbackConfirmationEvidence {
            response_status: Some("matched".to_string()),
            ..Default::default()
        },
        None,
        None,
        false,
        Some(dec!(5)),
    );

    assert_eq!(observation.status, "unconfirmed_before_cleanup");
    assert!(observation.reason.contains("response-only evidence was insufficient"));
}

#[test]
fn resolve_sellback_confirmation_marks_lookup_error_unconfirmed() {
    let observation = resolve_sellback_confirmation(
        Some("sellback-order"),
        &ProductionSellbackConfirmationEvidence {
            response_status: Some("matched".to_string()),
            lookup_error: Some("timeout".to_string()),
            ..Default::default()
        },
        None,
        None,
        true,
        Some(dec!(5)),
    );

    assert_eq!(observation.status, "unconfirmed_before_cleanup");
    assert!(observation.reason.contains("authenticated lookup failed"));
}

#[test]
fn confirm_sellback_before_cleanup_upgrades_flat_post_decision_truth() {
    let mut result = sample_result_payload("success");
    result.sellback_leg_status = Some("success".to_string());
    result.sellback_response_status = Some("matched".to_string());
    let mut exit = sample_exit_payload("sellback_complete");
    exit.post_sync_complete_sets = Decimal::ZERO;
    let snapshot = direct_truth_snapshot(
        "post_decision",
        &DirectMarketPositionTruth::default(),
        Duration::from_secs(8),
    );

    let observation = confirm_sellback_before_cleanup(
        SellbackConfirmationObservation::unconfirmed("lookup missing"),
        Some(&result),
        Some(&exit),
        &snapshot,
        dec!(0.5),
    );

    assert_eq!(observation.status, "confirmed_before_cleanup");
    assert!(observation.reason.contains("already flat before cleanup"));
}

#[test]
fn confirm_sellback_before_cleanup_confirms_from_execution_source_for_sellback_complete() {
    let mut result = sample_result_payload("success");
    result.sellback_leg_status = Some("success".to_string());
    result.sellback_response_status = Some("matched".to_string());
    let mut exit =
        sample_exit_payload_with_source("sellback_complete", "execution_confirmed_sellback");
    exit.post_sync_complete_sets = Decimal::ZERO;
    let snapshot = direct_truth_snapshot(
        "post_decision",
        &DirectMarketPositionTruth {
            yes_size: dec!(5.007),
            no_size: Decimal::ZERO,
        },
        Duration::from_secs(8),
    );

    let observation = confirm_sellback_before_cleanup(
        SellbackConfirmationObservation::unconfirmed("lookup missing"),
        Some(&result),
        Some(&exit),
        &snapshot,
        dec!(0.5),
    );

    assert_eq!(observation.status, "confirmed_before_cleanup");
    assert!(observation.reason.contains("execution_confirmed_sellback"));
}

#[test]
fn classify_flow_status_treats_confirmed_sellback_positions_lag_as_completed() {
    let observation = PostDecisionFlowObservation {
        truth: DirectMarketPositionTruth {
            yes_size: dec!(5),
            no_size: Decimal::ZERO,
        },
        merge_observed: false,
        fallback_asks_observed: false,
        observed_for: Duration::ZERO,
    };
    let exit = sample_exit_payload("sellback_complete");

    assert_eq!(
        classify_flow_status(
            Some(Decimal::ZERO),
            dec!(0.5),
            Some(Decimal::ZERO),
            Some(dec!(5)),
            Some(&exit),
            None,
            &observation,
            &SellbackConfirmationObservation::confirmed(
                "production sellback was confirmed before cleanup via post_sync_source=execution_confirmed_sellback",
            ),
        ),
        FlowStatus::SellbackCompleted
    );
}

#[test]
fn classify_flow_status_keeps_unconfirmed_sellback_positions_lag_as_directional_residual() {
    let observation = PostDecisionFlowObservation {
        truth: DirectMarketPositionTruth {
            yes_size: dec!(5),
            no_size: Decimal::ZERO,
        },
        merge_observed: false,
        fallback_asks_observed: false,
        observed_for: Duration::ZERO,
    };
    let exit = sample_exit_payload("sellback_complete");

    assert_eq!(
        classify_flow_status(
            Some(Decimal::ZERO),
            dec!(0.5),
            Some(Decimal::ZERO),
            Some(dec!(5)),
            Some(&exit),
            None,
            &observation,
            &SellbackConfirmationObservation::unconfirmed("response-only evidence"),
        ),
        FlowStatus::DirectionalResidual
    );
}

#[test]
fn reconcile_production_truth_fails_on_internal_external_mismatch() {
    let observation = PostDecisionFlowObservation {
        truth: DirectMarketPositionTruth {
            yes_size: dec!(5),
            no_size: Decimal::ZERO,
        },
        merge_observed: false,
        fallback_asks_observed: false,
        observed_for: Duration::from_secs(3),
    };
    let exit = sample_exit_payload("sellback_complete");

    let result = sample_result_payload("success");
    let outcome = reconcile_production_truth(
        Some(&result),
        Some(&exit),
        &observation,
        dec!(0.5),
        &SellbackConfirmationObservation::unconfirmed("response-only evidence"),
    );

    assert_eq!(outcome.status, "failed");
    assert!(outcome.reason.contains("stage=post_decision"));
    assert!(outcome.warning_status.is_none());
}

#[test]
fn reconcile_production_truth_fails_when_successful_trace_is_missing_exit_event() {
    let observation = PostDecisionFlowObservation {
        truth: DirectMarketPositionTruth {
            yes_size: Decimal::ZERO,
            no_size: Decimal::ZERO,
        },
        merge_observed: false,
        fallback_asks_observed: false,
        observed_for: Duration::from_secs(2),
    };
    let result = sample_result_payload("success");

    let outcome = reconcile_production_truth(
        Some(&result),
        None,
        &observation,
        dec!(0.5),
        &SellbackConfirmationObservation::not_applicable("not applicable"),
    );

    assert_eq!(outcome.status, "failed");
    assert!(outcome.reason.contains("required hedge_exit_path_recorded"));
}

#[test]
fn reconcile_production_truth_keeps_event_missing_fallback_for_failed_trace() {
    let observation = PostDecisionFlowObservation {
        truth: DirectMarketPositionTruth {
            yes_size: Decimal::ZERO,
            no_size: Decimal::ZERO,
        },
        merge_observed: false,
        fallback_asks_observed: false,
        observed_for: Duration::from_secs(1),
    };
    let result = sample_result_payload("failed");

    let outcome = reconcile_production_truth(
        Some(&result),
        None,
        &observation,
        dec!(0.5),
        &SellbackConfirmationObservation::not_applicable("not applicable"),
    );

    assert_eq!(outcome.status, "event_missing");
    assert!(outcome.reason.contains("using legacy flow inference"));
}

#[test]
fn reconcile_production_truth_surfaces_unverified_sellback_before_missing_exit_event() {
    let observation = PostDecisionFlowObservation {
        truth: DirectMarketPositionTruth {
            yes_size: dec!(5),
            no_size: Decimal::ZERO,
        },
        merge_observed: false,
        fallback_asks_observed: false,
        observed_for: Duration::from_secs(1),
    };
    let mut result = sample_result_payload("failed");
    result.sellback_leg_status = Some("unverified".to_string());
    result.sellback_response_status = Some("live".to_string());
    result.sellback_lookup_status = Some("missing".to_string());

    let outcome = reconcile_production_truth(
        Some(&result),
        None,
        &observation,
        dec!(0.5),
        &SellbackConfirmationObservation::unconfirmed("response-only evidence"),
    );

    assert_eq!(outcome.status, "failed");
    assert!(outcome
        .reason
        .contains("sellback verification remained unverified"));
    assert!(outcome.reason.contains("response_status=live"));
    assert!(outcome.reason.contains("lookup_status=missing"));
}

#[test]
fn reconcile_production_truth_surfaces_verified_zero_fill_sellback() {
    let observation = PostDecisionFlowObservation {
        truth: DirectMarketPositionTruth {
            yes_size: dec!(5),
            no_size: Decimal::ZERO,
        },
        merge_observed: false,
        fallback_asks_observed: false,
        observed_for: Duration::from_secs(1),
    };
    let mut result = sample_result_payload("failed");
    result.sellback_leg_status = Some("failed".to_string());
    result.sellback_response_status = Some("delayed".to_string());
    result.sellback_lookup_status = Some("cancelled".to_string());
    result.sellback_lookup_matched_shares = Some(Decimal::ZERO);

    let outcome = reconcile_production_truth(
        Some(&result),
        None,
        &observation,
        dec!(0.5),
        &SellbackConfirmationObservation::unconfirmed("lookup showed zero fill"),
    );

    assert_eq!(outcome.status, "failed");
    assert!(outcome
        .reason
        .contains("sellback verification confirmed zero fill"));
    assert!(outcome.reason.contains("lookup_status=cancelled"));
}

#[test]
fn reconcile_production_truth_warns_on_confirmed_sellback_positions_lag() {
    let observation = PostDecisionFlowObservation {
        truth: DirectMarketPositionTruth {
            yes_size: dec!(5.007),
            no_size: Decimal::ZERO,
        },
        merge_observed: false,
        fallback_asks_observed: false,
        observed_for: Duration::from_secs(8),
    };
    let exit = sample_exit_payload("sellback_complete");
    let mut result = sample_result_payload("success");
    result.sellback_leg_status = Some("success".to_string());
    result.sellback_response_status = Some("matched".to_string());
    result.sellback_trade_ids = Some(vec!["trade-1".to_string()]);

    let outcome = reconcile_production_truth(
        Some(&result),
        Some(&exit),
        &observation,
        dec!(0.5),
        &SellbackConfirmationObservation::confirmed("trade_ids=trade-1"),
    );

    assert_eq!(outcome.status, "confirmed");
    assert_eq!(
        outcome.warning_status.as_deref(),
        Some("positions_lag_after_confirmed_execution")
    );
    assert!(outcome
        .warning_reason
        .as_deref()
        .is_some_and(|reason| reason.contains("positions lag after confirmed execution")));
}

#[test]
fn reconcile_production_truth_warns_on_execution_confirmed_sellback_positions_lag_when_independently_confirmed() {
    let observation = PostDecisionFlowObservation {
        truth: DirectMarketPositionTruth {
            yes_size: dec!(5.007),
            no_size: Decimal::ZERO,
        },
        merge_observed: false,
        fallback_asks_observed: false,
        observed_for: Duration::from_secs(8),
    };
    let exit = sample_exit_payload_with_source("sellback_complete", "execution_confirmed_sellback");
    let mut result = sample_result_payload("success");
    result.sellback_leg_status = Some("success".to_string());
    result.sellback_response_status = Some("matched".to_string());
    result.sellback_trade_ids = Some(vec!["trade-1".to_string()]);
    let sellback_confirmation = confirm_sellback_before_cleanup(
        SellbackConfirmationObservation::confirmed("lookup_status=matched"),
        Some(&result),
        Some(&exit),
        &direct_truth_snapshot("post_decision", &observation.truth, observation.observed_for),
        dec!(0.5),
    );

    let outcome = reconcile_production_truth(
        Some(&result),
        Some(&exit),
        &observation,
        dec!(0.5),
        &sellback_confirmation,
    );

    assert_eq!(sellback_confirmation.status, "confirmed_before_cleanup");
    assert_eq!(outcome.status, "confirmed");
    assert_eq!(
        outcome.warning_status.as_deref(),
        Some("positions_lag_after_confirmed_execution")
    );
}

#[test]
fn resolve_sellback_confirmation_rejects_partial_lookup_matched_shares() {
    let observation = resolve_sellback_confirmation(
        Some("sellback-order"),
        &ProductionSellbackConfirmationEvidence {
            response_status: Some("matched".to_string()),
            lookup_matched_shares: Some(dec!(1)),
            ..Default::default()
        },
        None,
        None,
        false,
        Some(dec!(5)),
    );

    assert_eq!(observation.status, "unconfirmed_before_cleanup");
}

#[test]
fn reconcile_production_truth_keeps_paired_inventory_as_hard_failure_after_confirmed_sellback() {
    let observation = PostDecisionFlowObservation {
        truth: DirectMarketPositionTruth {
            yes_size: dec!(5),
            no_size: dec!(1),
        },
        merge_observed: false,
        fallback_asks_observed: false,
        observed_for: Duration::from_secs(8),
    };
    let exit = sample_exit_payload("sellback_complete");
    let mut result = sample_result_payload("success");
    result.sellback_leg_status = Some("success".to_string());
    result.sellback_response_status = Some("matched".to_string());
    result.sellback_trade_ids = Some(vec!["trade-1".to_string()]);

    let outcome = reconcile_production_truth(
        Some(&result),
        Some(&exit),
        &observation,
        dec!(0.5),
        &SellbackConfirmationObservation::confirmed("trade_ids=trade-1"),
    );

    assert_eq!(outcome.status, "failed");
    assert!(outcome.warning_status.is_none());
}

#[test]
fn post_decision_and_cleanup_snapshots_remain_stage_labeled_when_truth_diverges() {
    let flow_observation = PostDecisionFlowObservation {
        truth: DirectMarketPositionTruth {
            yes_size: dec!(5.007056),
            no_size: Decimal::ZERO,
        },
        merge_observed: false,
        fallback_asks_observed: false,
        observed_for: Duration::from_secs(3),
    };
    let cleanup_truth = CleanupTruthObservation {
        truth: DirectMarketPositionTruth {
            yes_size: Decimal::ZERO,
            no_size: dec!(5),
        },
        stable_baseline_confirmed: false,
        observed_for: Duration::from_secs(7),
    };
    let result = sample_result_payload("success");
    let exit = sample_exit_payload("no_exit_needed");

    let outcome = reconcile_production_truth(
        Some(&result),
        Some(&exit),
        &flow_observation,
        dec!(0.5),
        &SellbackConfirmationObservation::not_applicable("not applicable"),
    );
    let cleanup_status = build_cleanup_status(
        false,
        &cleanup_truth,
        &DirectMarketPositionTruth::default(),
        "user-1",
    );

    assert!(outcome.reason.contains("stage=post_decision"));
    assert!(outcome.reason.contains("direct_yes=5.007056"));
    assert!(cleanup_status.contains("stage=cleanup"));
    assert!(cleanup_status.contains("direct_yes=0"));
    assert!(cleanup_status.contains("direct_no=5"));
}

#[test]
fn validate_merge_live_probe_scenario_rejects_zero_shares() {
    let mut scenario = sample_merge_live_probe_scenario();
    scenario.acquisition.shares = Decimal::ZERO;

    let err = validate_merge_live_probe_scenario(&scenario)
        .expect_err("zero-share merge probe scenario should fail validation");
    assert!(err.to_string().contains("acquisition.shares"));
}

#[test]
fn validate_merge_live_probe_scenario_accepts_positive_caps() {
    validate_merge_live_probe_scenario(&sample_merge_live_probe_scenario())
        .expect("sample merge probe scenario should be valid");
}

#[test]
fn validate_merge_live_probe_scenario_rejects_subminimum_share_size() {
    let mut scenario = sample_merge_live_probe_scenario();
    scenario.acquisition.shares = dec!(4);

    let err = validate_merge_live_probe_scenario(&scenario)
        .expect_err("subminimum share size should fail validation");
    assert!(err.to_string().contains(">= 5 shares"));
}

#[test]
fn validate_merge_live_probe_scenario_rejects_subminimum_market_buy_notional() {
    let mut scenario = sample_merge_live_probe_scenario();
    scenario.acquisition.shares = dec!(5);
    scenario.acquisition.no_max_limit_price = dec!(0.15);
    scenario.safety.max_no_notional_usdc = dec!(0.75);

    let err = validate_merge_live_probe_scenario(&scenario)
        .expect_err("subminimum marketable BUY notional should fail validation");
    assert!(err.to_string().contains("venue minimum marketable BUY size"));
}

fn sample_decision_audit_scenario() -> HedgeLiveProbeScenario {
    HedgeLiveProbeScenario {
        name: "decision-audit".to_string(),
        description: "decision-audit".to_string(),
        market: LiveProbeMarket {
            condition_id: "condition".to_string(),
            question: None,
            yes_token_id: "yes".to_string(),
            no_token_id: "no".to_string(),
            tick_size: "0.01".to_string(),
            neg_risk: false,
        },
        trigger: LiveProbeTrigger {
            leg: QuoteLeg::YesBid,
            shares: dec!(10),
            max_trigger_limit_price: dec!(0.74),
        },
        safety: LiveProbeSafety {
            require_clean_market: true,
            max_planned_hedge_shares: dec!(10),
            max_planned_sellback_shares: dec!(10),
            max_planned_hedge_notional_usdc: dec!(10),
            max_post_sync_net_exposure: dec!(0.5),
            max_trigger_notional_usdc: dec!(10),
            max_cleanup_notional_usdc: dec!(10),
            timeout_secs: 60,
        },
        expected: LiveProbeExpected {
            success: true,
            halted: false,
            hedge_side: Some(Side::Buy),
        },
    }
}

fn sample_merge_live_probe_scenario() -> MergeLiveProbeScenario {
    MergeLiveProbeScenario {
        name: "sample_merge_probe".to_string(),
        description: "Acquire a small YES/NO pair and merge it.".to_string(),
        market: LiveProbeMarket {
            condition_id: "condition".to_string(),
            question: Some("Sample market".to_string()),
            yes_token_id: "1001".to_string(),
            no_token_id: "1002".to_string(),
            tick_size: "0.01".to_string(),
            neg_risk: false,
        },
        acquisition: MergeLiveProbeAcquisition {
            shares: dec!(5),
            yes_max_limit_price: dec!(0.70),
            no_max_limit_price: dec!(0.35),
        },
        safety: MergeLiveProbeSafety {
            require_clean_market: true,
            max_yes_notional_usdc: dec!(3.50),
            max_no_notional_usdc: dec!(1.75),
            max_cleanup_notional_usdc: dec!(7.00),
            timeout_secs: 60,
        },
    }
}

fn sample_decision_audit_intent(
    planned_hedge_shares: Decimal,
    planned_sellback_shares: Decimal,
) -> HedgeIntentPayload {
    HedgeIntentPayload {
        trigger_order_id: "order".to_string(),
        trigger_leg: "YesBid".to_string(),
        fill_size: dec!(10),
        fill_price: dec!(0.74),
        hedge_token_id: "no".to_string(),
        hedge_side: "BUY".to_string(),
        planned_hedge_shares: Some(planned_hedge_shares),
        planned_hedge_price: Some(dec!(0.27)),
        planned_sellback_shares: Some(planned_sellback_shares),
        planned_sellback_price: Some(dec!(0.73)),
        planned_sellback_reference_bid: Some(dec!(0.73)),
        unresolved_shares: Some(Decimal::ZERO),
        pre_resolution_active_orders: Some(0),
        pre_resolution_pending_cancels: Some(0),
        cancel_wait_drained: Some(true),
        origin: Some("fill_handler".to_string()),
    }
}

fn sample_decision_payload(
    planned_hedge_shares: Decimal,
    planned_sellback_shares: Decimal,
    decision_reason_code: &str,
) -> HedgeDecisionPayload {
    HedgeDecisionPayload {
        trigger_leg: "YesBid".to_string(),
        hedge_side: "BUY".to_string(),
        fill_size: dec!(10),
        fill_price: dec!(0.74),
        decision_mode: "buy_side_resolution".to_string(),
        decision_reason_code: decision_reason_code.to_string(),
        available_hedge_budget_usd: dec!(1000),
        filled_best_bid_price: Some(dec!(0.73)),
        filled_best_bid_size: Some(dec!(20)),
        opposite_best_ask_price: Some(dec!(0.27)),
        opposite_best_ask_size: Some(dec!(20)),
        planned_hedge_shares,
        planned_hedge_price: dec!(0.27),
        planned_sellback_shares,
        planned_sellback_price: dec!(0.73),
        unresolved_shares: Decimal::ZERO,
    }
}

fn sample_exit_payload(exit_path_status: &str) -> HedgeExitPathPayload {
    sample_exit_payload_with_source(exit_path_status, "position_manager")
}

fn sample_exit_payload_with_source(
    exit_path_status: &str,
    post_sync_source: &str,
) -> HedgeExitPathPayload {
    HedgeExitPathPayload {
        post_sync_yes_size: dec!(2),
        post_sync_no_size: dec!(2),
        post_sync_net_exposure: Decimal::ZERO,
        post_sync_complete_sets: dec!(2),
        post_sync_source: post_sync_source.to_string(),
        exit_path_status: exit_path_status.to_string(),
        merge_eligible_pairs: dec!(2),
        ctf_merge_configured: true,
        merge_attempted: false,
        merge_tx_hash: None,
        merge_failure_reason: None,
        fallback_asks_attempted: false,
        fallback_ask_count: 0,
        fallback_failure_reason: None,
    }
}

fn sample_result_payload(result_status: &str) -> HedgeResultPayload {
    HedgeResultPayload {
        hedge_order_id: Some("hedge-order".to_string()),
        result_status: result_status.to_string(),
        hedge_price: Some(dec!(0.27)),
        hedge_leg_status: Some(
            match result_status {
                "success" => "success",
                "failed" => "failed",
                _ => "unverified",
            }
            .to_string(),
        ),
        hedge_cancel_status: None,
        hedge_cancel_reason: None,
        hedge_lookup_status: None,
        hedge_lookup_matched_shares: None,
        hedge_lookup_error: None,
        hedge_trade_ids: None,
        sellback_order_id: None,
        sellback_price: None,
        sellback_execution_limit_price: None,
        sellback_leg_status: Some("skipped".to_string()),
        sellback_response_status: None,
        sellback_lookup_status: None,
        sellback_lookup_matched_shares: None,
        sellback_lookup_error: None,
        sellback_trade_ids: None,
        post_sync_net_exposure: Some(Decimal::ZERO),
        post_sync_yes_size: Some(dec!(2)),
        post_sync_no_size: Some(dec!(2)),
        post_sync_source: Some("position_manager".to_string()),
        halt_signal_suppressed: false,
        failure_reason: None,
        latency_ms: 10,
        origin: Some("fill_handler".to_string()),
    }
}

fn sample_live_order(status: OrderStatus, matched_size: Decimal) -> LiveOrder {
    LiveOrder {
        id: "hedge-order".to_string(),
        condition_id: "condition".to_string(),
        asset_id: "asset".to_string(),
        side: Side::Buy,
        price: dec!(0.27),
        original_size: dec!(5),
        size_matched: matched_size,
        outcome: Outcome::No,
        order_type: OrderType::GTC,
        status,
        created_at: Utc::now(),
        associated_trade_ids: Vec::new(),
    }
}

fn sample_book_audit_snapshot(no_asks: Vec<Decimal>) -> BookAuditSnapshot {
    BookAuditSnapshot {
        yes_book: Some(OrderBookSnapshot {
            token_id: "yes".to_string(),
            exchange_ts: None,
            ingest_ts: Utc::now(),
            bids: vec![PriceLevel {
                price: dec!(0.73),
                size: dec!(20),
            }],
            asks: vec![PriceLevel {
                price: dec!(0.75),
                size: dec!(20),
            }],
        }),
        no_book: Some(OrderBookSnapshot {
            token_id: "no".to_string(),
            exchange_ts: None,
            ingest_ts: Utc::now(),
            bids: vec![PriceLevel {
                price: dec!(0.24),
                size: dec!(20),
            }],
            asks: no_asks
                .into_iter()
                .map(|price| PriceLevel {
                    price,
                    size: dec!(6),
                })
                .collect(),
        }),
        max_hedge_usdc: dec!(1000),
        note: None,
    }
}

fn canonical_from_live_probe(market: &LiveProbeMarket) -> CanonicalMarket {
    CanonicalMarket {
        condition_id: market.condition_id.clone(),
        market_slug: format!("{}-probe", market.condition_id),
        question: market
            .question
            .clone()
            .unwrap_or_else(|| format!("Probe {}", market.condition_id)),
        yes_token_id: market.yes_token_id.clone(),
        no_token_id: market.no_token_id.clone(),
        reward_config: crate::models::RewardConfig {
            condition_id: market.condition_id.clone(),
            daily_reward_rates: vec![Decimal::ZERO],
            daily_reward_total: Decimal::ZERO,
            min_size: Decimal::ONE,
            max_spread: dec!(0.10),
        },
        neg_risk: market.neg_risk,
        tick_size: market.tick_size.clone(),
        end_date: None,
        admitted_at: Utc::now(),
        status: crate::models::MarketStatus::Admitted,
    }
}

fn live_probe_token_id(market: &LiveProbeMarket, leg: QuoteLeg) -> String {
    match leg {
        QuoteLeg::YesBid | QuoteLeg::YesAsk => market.yes_token_id.clone(),
        QuoteLeg::NoBid | QuoteLeg::NoAsk => market.no_token_id.clone(),
    }
}

fn opposite_live_probe_token_id(market: &LiveProbeMarket, leg: QuoteLeg) -> String {
    match leg {
        QuoteLeg::YesBid | QuoteLeg::YesAsk => market.no_token_id.clone(),
        QuoteLeg::NoBid | QuoteLeg::NoAsk => market.yes_token_id.clone(),
    }
}
