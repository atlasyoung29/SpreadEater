//! Deterministic Layer 2 hedge replay harness.
//!
//! Replays raw user-stream and refresh/reconciliation steps through the real
//! `LiveEngine` attribution, fallback, exchange-sync, and reconciliation paths.

use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use chrono::Utc;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use spreadeater_core::{EventEnvelope, Priority};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::config::Config;
use crate::models::events::{OrderEvent, OrderEventType, TradeEvent, TradeStatus, UserEvent};
use crate::models::{Position, QuoteLeg, Side};
use crate::monitor::ErrorLogger;
use crate::runtime::hedge_harness_support::{
    build_canonical_market, build_observed_outcome, build_test_credentials,
    compare_expected_to_observed, deserialize_optional_decimal, opposite_token_id_for_leg,
    outcome_for_leg, scenario_book_to_snapshot, serialize_optional_decimal, side_for_leg,
    token_id_for_leg, validate_scenario_market, InMemoryEventCollector, MockExchangeServer,
    ObservedHedgeOutcome, ScenarioExchange, ScenarioExpected, ScenarioMarket, ScenarioPositionStep,
    ScenarioTrackedOrder, ScenarioTrade,
};
use crate::runtime::live_engine::{FillWorkItem, LiveEngine};
use crate::trading::order_manager::TrackedOrder;

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HedgeReplayScenario {
    pub name: String,
    pub description: String,
    pub market: ScenarioMarket,
    #[serde(default)]
    pub setup: ReplaySetup,
    pub sequence: Vec<ReplayStep>,
    pub exchange: ScenarioExchange,
    pub expected: ReplayExpectedOutcome,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReplaySetup {
    #[serde(default = "default_true")]
    pub managed_market: bool,
    #[serde(default = "default_true")]
    pub known_market: bool,
    #[serde(default)]
    pub tracked_orders: Vec<ScenarioTrackedOrder>,
    #[serde(default)]
    pub recently_cancelled_orders: Vec<ScenarioTrackedOrder>,
    #[serde(default)]
    pub positions: Vec<ScenarioPositionStep>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_decimal",
        serialize_with = "serialize_optional_decimal"
    )]
    pub cached_balance: Option<Decimal>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ReplayStep {
    UserConnected {
        #[serde(default)]
        reconnect: bool,
    },
    UserTrade {
        trade: ScenarioTrade,
    },
    UserOrderUpdate {
        order: ReplayOrderEvent,
    },
    UserOrderCancellation {
        order: ReplayOrderEvent,
    },
    RefreshQuotes,
    FlushPendingFillFallbacks,
    RecoverOrphanedPositions,
    ReconcileUnhedgedPositions,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayOrderEvent {
    pub order_id: String,
    pub side: Side,
    #[serde(with = "rust_decimal::serde::str")]
    pub price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub original_size: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub size_matched: Decimal,
    #[serde(default)]
    pub condition_id: Option<String>,
    #[serde(default)]
    pub asset_id: Option<String>,
    #[serde(default)]
    pub outcome: Option<String>,
    #[serde(default)]
    pub timestamp_unix: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayExpectedOutcome {
    #[serde(default)]
    pub critical_events: Vec<ExpectedCriticalEvent>,
    #[serde(flatten)]
    pub final_outcome: ScenarioExpected,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExpectedCriticalEvent {
    pub event_type: String,
    #[serde(default)]
    pub source_component: Option<String>,
    #[serde(default)]
    pub payload: Value,
}

#[derive(Debug, Clone)]
pub struct ReplayEventSummary {
    pub event_type: String,
    pub source_component: String,
    pub payload: Value,
}

#[derive(Debug)]
pub struct HedgeReplayResult {
    pub scenario_name: String,
    pub passed: bool,
    pub actual_success: bool,
    pub expected_success: bool,
    pub halted: bool,
    pub mismatches: Vec<String>,
    pub observed: ObservedHedgeOutcome,
    pub critical_events: Vec<ReplayEventSummary>,
}

pub struct HedgeReplayHarness {
    engine: LiveEngine,
    scenario: HedgeReplayScenario,
    event_collector: Arc<InMemoryEventCollector>,
    mock_server: MockExchangeServer,
    fill_tx: mpsc::UnboundedSender<FillWorkItem>,
    fill_rx: mpsc::UnboundedReceiver<FillWorkItem>,
}

impl HedgeReplayHarness {
    pub async fn from_scenario(scenario: HedgeReplayScenario) -> Result<Self> {
        validate_scenario(&scenario)?;

        let mock_server = MockExchangeServer::spawn(&scenario.market, &scenario.exchange).await?;
        let base_dir =
            std::env::temp_dir().join(format!("spreadeater-hedge-replay-{}", Uuid::new_v4()));
        let archive_dir = base_dir.join("archive");
        let error_dir = base_dir.join("errors");
        std::fs::create_dir_all(&archive_dir)?;
        std::fs::create_dir_all(&error_dir)?;

        let mut config = Config::default();
        config.discovery.clob_base_url = mock_server.base_url().to_string();
        config.discovery.data_api_base_url = mock_server.base_url().to_string();
        config.persistence.archive_dir = archive_dir.to_string_lossy().into_owned();

        let event_collector = Arc::new(InMemoryEventCollector::default());
        let event_producer =
            Some(event_collector.clone() as Arc<dyn spreadeater_core::EventProducer>);
        let error_logger = Arc::new(ErrorLogger::new(&error_dir.to_string_lossy()));
        let engine = LiveEngine::new_for_replay(
            config,
            build_test_credentials(),
            error_logger,
            event_producer,
            "hedge-replay",
        )
        .await?;

        let canonical = build_canonical_market(&scenario.market);
        engine
            .replay_seed_market(
                canonical,
                scenario.setup.managed_market,
                scenario.setup.known_market,
                scenario_book_to_snapshot(
                    &scenario.market.yes_token_id,
                    &scenario.exchange.books.yes,
                ),
                scenario_book_to_snapshot(
                    &scenario.market.no_token_id,
                    &scenario.exchange.books.no,
                ),
            )
            .await;

        let initial_balance = scenario
            .setup
            .cached_balance
            .or_else(|| scenario.exchange.balances.first().map(|step| step.amount))
            .unwrap_or(Decimal::ZERO);
        engine.replay_seed_balance(initial_balance).await;

        for position in &scenario.setup.positions {
            engine
                .replay_seed_position(position_from_step(&scenario.market.condition_id, position))
                .await;
        }
        for tracked in &scenario.setup.tracked_orders {
            engine
                .replay_seed_tracked_order(build_tracked_order(&scenario.market, tracked), false)
                .await;
        }
        for tracked in &scenario.setup.recently_cancelled_orders {
            engine
                .replay_seed_tracked_order(build_tracked_order(&scenario.market, tracked), true)
                .await;
        }

        let (fill_tx, fill_rx) = mpsc::unbounded_channel();

        Ok(Self {
            engine,
            scenario,
            event_collector,
            mock_server,
            fill_tx,
            fill_rx,
        })
    }

    pub async fn run(mut self) -> Result<HedgeReplayResult> {
        let scenario_name = self.scenario.name.clone();
        let expected = self.scenario.expected.clone();

        for step in self.scenario.sequence.clone() {
            self.execute_step(step).await?;
            self.engine
                .replay_drain_fill_queue(&mut self.fill_rx)
                .await?;
        }

        let events = self.event_collector.events();
        let observed = build_observed_outcome(
            &events,
            self.engine.replay_risk_manager(),
            &self.scenario.market.condition_id,
            self.mock_server.request_log().await,
        )
        .await?;
        let actual_success = observed.result_status.as_deref() == Some("success");
        let critical_events = collect_critical_events(&events);
        let mut mismatches =
            compare_expected_to_observed(&expected.final_outcome, &observed, actual_success);
        mismatches.extend(compare_expected_critical_events(
            &expected.critical_events,
            &critical_events,
        ));

        Ok(HedgeReplayResult {
            scenario_name,
            passed: mismatches.is_empty(),
            actual_success,
            expected_success: expected.final_outcome.success,
            halted: observed.halted,
            mismatches,
            observed,
            critical_events,
        })
    }

    async fn execute_step(&mut self, step: ReplayStep) -> Result<()> {
        match step {
            ReplayStep::UserConnected { reconnect } => {
                self.engine
                    .replay_dispatch_user_event(UserEvent::Connected { reconnect }, &self.fill_tx)
                    .await;
            }
            ReplayStep::UserTrade { trade } => {
                let fallback_leg = resolve_trade_fallback_leg(&self.engine, &trade).await;
                let event = build_trade_event(&self.scenario.market, &trade, fallback_leg)?;
                self.engine
                    .replay_dispatch_user_event(UserEvent::Trade(event), &self.fill_tx)
                    .await;
            }
            ReplayStep::UserOrderUpdate { order } => {
                let event = build_order_event(
                    &self.engine,
                    &self.scenario.market,
                    order,
                    OrderEventType::Update,
                )
                .await?;
                self.engine
                    .replay_dispatch_user_event(UserEvent::Order(event), &self.fill_tx)
                    .await;
            }
            ReplayStep::UserOrderCancellation { order } => {
                let event = build_order_event(
                    &self.engine,
                    &self.scenario.market,
                    order,
                    OrderEventType::Cancellation,
                )
                .await?;
                self.engine
                    .replay_dispatch_user_event(UserEvent::Order(event), &self.fill_tx)
                    .await;
            }
            ReplayStep::RefreshQuotes => {
                self.engine.replay_refresh_quotes(&self.fill_tx).await?;
            }
            ReplayStep::FlushPendingFillFallbacks => {
                self.engine
                    .replay_flush_pending_fill_fallbacks_due(&self.fill_tx)
                    .await?;
            }
            ReplayStep::RecoverOrphanedPositions => {
                self.engine.replay_recover_orphaned_positions().await;
            }
            ReplayStep::ReconcileUnhedgedPositions => {
                self.engine.replay_reconcile_unhedged_positions().await;
            }
        }

        Ok(())
    }
}

pub async fn run_hedge_replay(scenario_path: &str) -> Result<HedgeReplayResult> {
    let scenario = load_scenario(scenario_path)?;
    HedgeReplayHarness::from_scenario(scenario)
        .await?
        .run()
        .await
}

pub fn load_scenario(path: &str) -> Result<HedgeReplayScenario> {
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read replay scenario file: {path}"))?;
    let scenario: HedgeReplayScenario = serde_json::from_str(&contents)
        .with_context(|| format!("Failed to parse replay scenario JSON from: {path}"))?;
    validate_scenario(&scenario)?;
    Ok(scenario)
}

pub fn print_report(result: &HedgeReplayResult) {
    println!("=== Hedge Replay: {} ===", result.scenario_name);
    println!("Result: {}", if result.passed { "PASS" } else { "FAIL" });
    println!(
        "Expected success: {}, Actual: {}",
        result.expected_success, result.actual_success
    );
    println!("Critical events observed: {}", result.critical_events.len());
    if !result.mismatches.is_empty() {
        println!("Mismatches:");
        for mismatch in &result.mismatches {
            println!("- {}", mismatch);
        }
    }
}

fn validate_scenario(scenario: &HedgeReplayScenario) -> Result<()> {
    validate_scenario_market(&scenario.market)?;
    if scenario.sequence.is_empty() {
        bail!("Replay scenario sequence must not be empty");
    }
    Ok(())
}

fn build_tracked_order(
    market: &ScenarioMarket,
    tracked_order: &ScenarioTrackedOrder,
) -> TrackedOrder {
    let created_at = tracked_order
        .created_at_unix
        .and_then(|unix| chrono::DateTime::<Utc>::from_timestamp(unix, 0))
        .unwrap_or_else(Utc::now);
    TrackedOrder {
        order_id: tracked_order.order_id.clone(),
        trace_id: tracked_order
            .trace_id
            .clone()
            .unwrap_or_else(|| format!("hedge-replay-trace-{}", Uuid::new_v4())),
        condition_id: market.condition_id.clone(),
        created_at,
        leg: tracked_order.leg,
        token_id: token_id_for_leg(tracked_order.leg, market),
        opposite_token_id: opposite_token_id_for_leg(tracked_order.leg, market),
        side: side_for_leg(tracked_order.leg),
        price: tracked_order.price,
        size: tracked_order.size,
        matched_size: tracked_order.matched_size,
        neg_risk: false,
        tick_size: market.tick_size.clone(),
    }
}

fn position_from_step(condition_id: &str, step: &ScenarioPositionStep) -> Position {
    Position {
        condition_id: condition_id.to_string(),
        yes_size: step.yes_size,
        no_size: step.no_size,
        avg_yes_price: step.yes_avg_price,
        avg_no_price: step.no_avg_price,
    }
}

async fn resolve_trade_fallback_leg(
    engine: &LiveEngine,
    trade: &ScenarioTrade,
) -> Option<QuoteLeg> {
    for order_id in [&trade.maker_order_id, &trade.taker_order_id]
        .into_iter()
        .flatten()
    {
        if let Some(tracked) = engine.replay_get_tracked_order(order_id).await {
            return Some(tracked.leg);
        }
    }
    None
}

fn build_trade_event(
    market: &ScenarioMarket,
    trade: &ScenarioTrade,
    fallback_leg: Option<QuoteLeg>,
) -> Result<TradeEvent> {
    let leg = fallback_leg.or_else(|| match trade.outcome.as_deref() {
        Some("YES") if trade.side == Side::Buy => Some(QuoteLeg::YesBid),
        Some("YES") if trade.side == Side::Sell => Some(QuoteLeg::YesAsk),
        Some("NO") if trade.side == Side::Buy => Some(QuoteLeg::NoBid),
        Some("NO") if trade.side == Side::Sell => Some(QuoteLeg::NoAsk),
        _ => None,
    });
    let leg = leg.ok_or_else(|| {
        anyhow!(
            "Replay trade {} needs asset_id/outcome or a tracked maker/taker order",
            trade.trade_id
        )
    })?;
    let timestamp = trade
        .timestamp_unix
        .and_then(|unix| chrono::DateTime::<Utc>::from_timestamp(unix, 0))
        .unwrap_or_else(Utc::now);
    Ok(TradeEvent {
        id: trade.trade_id.clone(),
        condition_id: trade
            .condition_id
            .clone()
            .unwrap_or_else(|| market.condition_id.clone()),
        asset_id: trade
            .asset_id
            .clone()
            .unwrap_or_else(|| token_id_for_leg(leg, market)),
        side: trade.side,
        price: trade.price,
        size: trade.size,
        outcome: trade
            .outcome
            .clone()
            .unwrap_or_else(|| outcome_for_leg(leg).to_string()),
        status: TradeStatus::Matched,
        timestamp,
        maker_order_id: trade.maker_order_id.clone(),
        taker_order_id: trade.taker_order_id.clone(),
    })
}

async fn build_order_event(
    engine: &LiveEngine,
    market: &ScenarioMarket,
    order: ReplayOrderEvent,
    event_type: OrderEventType,
) -> Result<OrderEvent> {
    let tracked = engine.replay_get_tracked_order(&order.order_id).await;
    let leg = tracked.as_ref().map(|tracked| tracked.leg);
    let leg = leg.ok_or_else(|| {
        anyhow!(
            "Replay order event {} requires the order to be seeded",
            order.order_id
        )
    })?;
    let timestamp = order
        .timestamp_unix
        .and_then(|unix| chrono::DateTime::<Utc>::from_timestamp(unix, 0))
        .unwrap_or_else(Utc::now);
    Ok(OrderEvent {
        order_id: order.order_id,
        condition_id: order
            .condition_id
            .unwrap_or_else(|| market.condition_id.clone()),
        asset_id: order
            .asset_id
            .unwrap_or_else(|| token_id_for_leg(leg, market)),
        event_type,
        side: order.side,
        price: order.price,
        original_size: order.original_size,
        size_matched: order.size_matched,
        outcome: order
            .outcome
            .unwrap_or_else(|| outcome_for_leg(leg).to_string()),
        timestamp,
    })
}

fn collect_critical_events(events: &[EventEnvelope]) -> Vec<ReplayEventSummary> {
    events
        .iter()
        .filter(|event| event.priority == Priority::Critical)
        .map(|event| ReplayEventSummary {
            event_type: event.event_type.to_string(),
            source_component: event.source_component.clone(),
            payload: event.payload.clone(),
        })
        .collect()
}

fn compare_expected_critical_events(
    expected: &[ExpectedCriticalEvent],
    actual: &[ReplayEventSummary],
) -> Vec<String> {
    let mut mismatches = Vec::new();
    let mut cursor = 0usize;
    for wanted in expected {
        let mut matched = false;
        while let Some(candidate) = actual.get(cursor) {
            cursor += 1;
            if candidate.event_type != wanted.event_type {
                continue;
            }
            if wanted
                .source_component
                .as_ref()
                .is_some_and(|source| source != &candidate.source_component)
            {
                continue;
            }
            if !payload_contains_subset(&candidate.payload, &wanted.payload) {
                continue;
            }
            matched = true;
            break;
        }
        if !matched {
            mismatches.push(format!(
                "critical event subsequence mismatch: missing {} from {:?}",
                wanted.event_type, wanted.source_component
            ));
        }
    }
    mismatches
}

fn payload_contains_subset(actual: &Value, expected: &Value) -> bool {
    match (actual, expected) {
        (_, Value::Null) => true,
        (Value::Object(actual), Value::Object(expected)) => {
            expected.iter().all(|(key, expected_value)| {
                actual.get(key).is_some_and(|actual_value| {
                    payload_contains_subset(actual_value, expected_value)
                })
            })
        }
        _ => actual == expected,
    }
}
