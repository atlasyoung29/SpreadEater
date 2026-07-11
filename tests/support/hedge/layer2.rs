use super::*;

use std::sync::Arc;
use std::time::Instant;

use anyhow::{anyhow, bail, Context, Result};
use chrono::Utc;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use spreadeater_core::{EventEnvelope, Priority};
use std::str::FromStr;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::monitor::ErrorLogger;

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HedgeReplayScenario {
    name: String,
    description: String,
    market: ScenarioMarket,
    #[serde(default)]
    setup: ReplaySetup,
    sequence: Vec<ReplayStep>,
    exchange: ScenarioExchange,
    expected: ReplayExpectedOutcome,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct ReplaySetup {
    #[serde(default = "default_true")]
    managed_market: bool,
    #[serde(default = "default_true")]
    known_market: bool,
    #[serde(default)]
    tracked_orders: Vec<ScenarioTrackedOrder>,
    #[serde(default)]
    recently_cancelled_orders: Vec<ScenarioTrackedOrder>,
    #[serde(default)]
    positions: Vec<ScenarioPositionStep>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_decimal",
        serialize_with = "serialize_optional_decimal"
    )]
    cached_balance: Option<Decimal>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ReplayStep {
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
struct ReplayOrderEvent {
    order_id: String,
    side: Side,
    #[serde(with = "rust_decimal::serde::str")]
    price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    original_size: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    size_matched: Decimal,
    #[serde(default)]
    condition_id: Option<String>,
    #[serde(default)]
    asset_id: Option<String>,
    #[serde(default)]
    outcome: Option<String>,
    #[serde(default)]
    timestamp_unix: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReplayExpectedOutcome {
    #[serde(default)]
    critical_events: Vec<ExpectedCriticalEvent>,
    #[serde(flatten)]
    final_outcome: ScenarioExpected,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ExpectedCriticalEvent {
    event_type: String,
    #[serde(default)]
    source_component: Option<String>,
    #[serde(default)]
    payload: Value,
}

#[derive(Debug, Clone)]
struct ReplayEventSummary {
    event_type: String,
    source_component: String,
    payload: Value,
}

#[derive(Debug)]
struct HedgeReplayResult {
    scenario_name: String,
    passed: bool,
    actual_success: bool,
    expected_success: bool,
    halted: bool,
    mismatches: Vec<String>,
    observed: ObservedHedgeOutcome,
    critical_events: Vec<ReplayEventSummary>,
}

struct HedgeReplayHarness {
    engine: LiveEngine,
    fill_handler: FillHandler,
    scenario: HedgeReplayScenario,
    event_collector: Arc<InMemoryEventCollector>,
    mock_server: MockExchangeServer,
    fill_tx: mpsc::UnboundedSender<FillWorkItem>,
    fill_rx: mpsc::UnboundedReceiver<FillWorkItem>,
}

impl HedgeReplayHarness {
    async fn from_scenario(scenario: HedgeReplayScenario) -> Result<Self> {
        validate_scenario_market(&scenario.market)?;
        if scenario.sequence.is_empty() {
            bail!("Replay scenario sequence must not be empty");
        }

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

        let error_logger = Arc::new(ErrorLogger::new(&error_dir.to_string_lossy()));
        let mut engine = LiveEngine::new(
            config,
            build_test_credentials(),
            false,
            error_logger,
            "tests/support/hedge/layer2.json".to_string(),
        )
            .await
            .context("Failed to build replay engine")?;
        let event_collector = Arc::new(InMemoryEventCollector::default());
        let event_producer: Arc<dyn spreadeater_core::EventProducer> = event_collector.clone();
        engine.event_producer = Some(event_producer);

        let canonical = build_canonical_market(&scenario.market);
        engine
            .book_manager
            .insert_snapshot(scenario_book_to_snapshot(
                &scenario.market.yes_token_id,
                &scenario.exchange.books.yes,
            ))
            .await;
        engine
            .book_manager
            .insert_snapshot(scenario_book_to_snapshot(
                &scenario.market.no_token_id,
                &scenario.exchange.books.no,
            ))
            .await;

        if scenario.setup.managed_market {
            engine
                .managed_markets
                .write()
                .await
                .insert(canonical.condition_id.clone(), canonical.clone());
        }
        if scenario.setup.known_market {
            engine
                .known_markets
                .write()
                .await
                .insert(canonical.condition_id.clone(), canonical.clone());
        }

        let initial_balance = scenario
            .setup
            .cached_balance
            .or_else(|| scenario.exchange.balances.first().map(|step| step.amount))
            .unwrap_or(Decimal::ZERO);
        *engine.cached_balance.write().await = initial_balance;
        engine.risk_manager.update_balance(initial_balance).await;
        engine
            .order_manager
            .update_gross_balance(initial_balance)
            .await;

        for position in &scenario.setup.positions {
            engine
                .position_manager
                .update_position(position_from_step(&scenario.market.condition_id, position))
                .await;
        }

        seed_tracked_orders(&engine, &mock_server, &canonical, &scenario.setup).await?;

        let fill_handler = fill_handler_from_engine(&engine);
        let (fill_tx, fill_rx) = mpsc::unbounded_channel();

        Ok(Self {
            engine,
            fill_handler,
            scenario,
            event_collector,
            mock_server,
            fill_tx,
            fill_rx,
        })
    }

    async fn run(mut self) -> Result<HedgeReplayResult> {
        for step in self.scenario.sequence.clone() {
            self.execute_step(step).await?;
            drain_fill_queue(&self.fill_handler, &mut self.fill_rx).await?;
        }

        let events = self.event_collector.events();
        let observed = build_observed_outcome(
            &events,
            self.engine.risk_manager.clone(),
            &self.scenario.market.condition_id,
            self.mock_server.request_log().await,
        )
        .await?;
        let actual_success = observed.result_status.as_deref() == Some("success");
        let critical_events = collect_critical_events(&events);
        let mut mismatches = compare_expected_to_observed(
            &self.scenario.expected.final_outcome,
            &observed,
            actual_success,
        );
        mismatches.extend(compare_expected_critical_events(
            &self.scenario.expected.critical_events,
            &critical_events,
        ));

        Ok(HedgeReplayResult {
            scenario_name: self.scenario.name.clone(),
            passed: mismatches.is_empty(),
            actual_success,
            expected_success: self.scenario.expected.final_outcome.success,
            halted: observed.halted,
            mismatches,
            observed,
            critical_events,
        })
    }

    async fn execute_step(&mut self, step: ReplayStep) -> Result<()> {
        match step {
            ReplayStep::UserConnected { reconnect } => {
                self.dispatch_user_event(UserEvent::Connected { reconnect })
                    .await?;
            }
            ReplayStep::UserTrade { trade } => {
                let event = scenario_trade_to_trade_event(
                    &self.scenario.market,
                    &trade,
                    resolve_trade_fallback_leg(&self.engine, &trade).await,
                )?;
                self.dispatch_user_event(UserEvent::Trade(event)).await?;
            }
            ReplayStep::UserOrderUpdate { order } => {
                self.dispatch_user_event(UserEvent::Order(
                    build_order_event(
                        &self.engine,
                        &self.scenario.market,
                        order,
                        OrderEventType::Update,
                    )
                    .await?,
                ))
                .await?;
            }
            ReplayStep::UserOrderCancellation { order } => {
                self.dispatch_user_event(UserEvent::Order(
                    build_order_event(
                        &self.engine,
                        &self.scenario.market,
                        order,
                        OrderEventType::Cancellation,
                    )
                    .await?,
                ))
                .await?;
            }
            ReplayStep::RefreshQuotes => {
                self.engine
                    .detect_missed_fills_from_exchange(&self.fill_tx)
                    .await?;
            }
            ReplayStep::FlushPendingFillFallbacks => {
                let now = Instant::now();
                for pending in self
                    .engine
                    .pending_fill_fallbacks
                    .write()
                    .await
                    .values_mut()
                {
                    pending.queued_at = now - std::time::Duration::from_secs(3);
                }
                self.engine
                    .flush_pending_fill_fallbacks(&self.fill_tx)
                    .await?;
            }
            ReplayStep::RecoverOrphanedPositions => {
                self.engine.recover_orphaned_positions_on_refresh().await;
            }
            ReplayStep::ReconcileUnhedgedPositions => {
                let markets: Vec<CanonicalMarket> = self
                    .engine
                    .known_markets
                    .read()
                    .await
                    .values()
                    .cloned()
                    .collect();
                self.engine.reconcile_unhedged_positions(&markets).await;
            }
        }

        Ok(())
    }

    async fn dispatch_user_event(&self, event: UserEvent) -> Result<()> {
        match event {
            UserEvent::Connected { reconnect } => {
                self.engine
                    .emit_event(crate::monitor::emitters::build_user_stream_status_changed(
                        &self.engine.run_id,
                        &self.engine.mode,
                        if reconnect {
                            "reconnected"
                        } else {
                            "connected"
                        },
                        Some(self.engine.subscribed_market_ids.read().await.len() as u64),
                        Some("subscription acknowledged"),
                ));
                let _ = self.engine.position_manager.sync_positions().await;
            }
            UserEvent::RawActivity => {}
            UserEvent::Trade(trade) => {
                if let Some(work) = self.engine.build_fill_work_item(trade).await {
                    self.fill_tx
                        .send(work)
                        .map_err(|_| anyhow!("fill handler channel unexpectedly closed"))?;
                }
            }
            UserEvent::Order(order_event) => {
                if order_event.event_type == OrderEventType::Cancellation {
                    self.engine.handle_external_cancellation(order_event).await;
                } else if order_event.event_type == OrderEventType::Update {
                    self.engine.handle_order_update(order_event).await;
                }
            }
            UserEvent::Disconnected => {
                self.engine
                    .emit_event(crate::monitor::emitters::build_user_stream_status_changed(
                        &self.engine.run_id,
                        &self.engine.mode,
                        "disconnected",
                        Some(self.engine.subscribed_market_ids.read().await.len() as u64),
                        Some("auto-reconnect in progress"),
                    ));
            }
        }

        Ok(())
    }
}

async fn seed_tracked_orders(
    engine: &LiveEngine,
    mock_server: &MockExchangeServer,
    market: &CanonicalMarket,
    setup: &ReplaySetup,
) -> Result<()> {
    if setup.tracked_orders.is_empty() && setup.recently_cancelled_orders.is_empty() {
        return Ok(());
    }

    let mut orders = Vec::new();
    for tracked in setup
        .tracked_orders
        .iter()
        .chain(setup.recently_cancelled_orders.iter())
    {
        orders.push(ScenarioLiveOrder {
            id: tracked.order_id.clone(),
            leg: tracked.leg,
            price: tracked.price,
            original_size: tracked.size + tracked.matched_size,
            size_matched: tracked.matched_size,
            status: "live".to_string(),
            order_type: "GTC".to_string(),
            created_at_unix: tracked.created_at_unix,
            associated_trade_ids: Vec::new(),
        });
    }
    mock_server
        .prepend_market_open_orders(ScenarioOpenOrdersStep {
            orders,
            delay_ms: 0,
        })
        .await;
    engine
        .order_manager
        .sync_market_open_orders(
            &market.condition_id,
            market,
            MarketOrderSyncMode::ObserveOnly,
        )
        .await?;

    for tracked in &setup.recently_cancelled_orders {
        engine
            .order_manager
            .move_to_recently_cancelled(&tracked.order_id)
            .await;
    }

    Ok(())
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
        if let Some(tracked) = engine.order_manager.get_tracked_order(order_id).await {
            return Some(tracked.leg);
        }
    }
    None
}

async fn build_order_event(
    engine: &LiveEngine,
    market: &ScenarioMarket,
    order: ReplayOrderEvent,
    event_type: OrderEventType,
) -> Result<OrderEvent> {
    let tracked = engine
        .order_manager
        .get_tracked_order(&order.order_id)
        .await
        .ok_or_else(|| {
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
            .unwrap_or_else(|| token_id_for_leg(tracked.leg, market)),
        event_type,
        side: order.side,
        price: order.price,
        original_size: order.original_size,
        size_matched: order.size_matched,
        outcome: order
            .outcome
            .unwrap_or_else(|| outcome_for_leg(tracked.leg).to_string()),
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
        (Value::Object(actual_map), Value::Object(expected_map)) => {
            expected_map.iter().all(|(key, wanted)| {
                actual_map
                    .get(key)
                    .is_some_and(|value| payload_contains_subset(value, wanted))
            })
        }
        (Value::Array(actual_items), Value::Array(expected_items)) => {
            expected_items.len() <= actual_items.len()
                && expected_items
                    .iter()
                    .zip(actual_items.iter())
                    .all(|(wanted, value)| payload_contains_subset(value, wanted))
        }
        _ => actual == expected,
    }
}

async fn run_hedge_replay(name: &str) -> Result<HedgeReplayResult> {
    let scenario_path = fixture_path("hedge_replay_scenarios", name);
    let scenario: HedgeReplayScenario = serde_json::from_str(
        &std::fs::read_to_string(&scenario_path)
            .with_context(|| format!("Failed to read scenario {}", scenario_path.display()))?,
    )
    .with_context(|| format!("Failed to parse scenario {}", scenario_path.display()))?;

    HedgeReplayHarness::from_scenario(scenario)
        .await?
        .run()
        .await
}

fn assert_layer2_pass(result: &HedgeReplayResult) {
    assert!(
        result.passed,
        "layer2 scenario {} failed with mismatches {:?}; observed={:?}; critical_events={:?}",
        result.scenario_name, result.mismatches, result.observed, result.critical_events
    );
    assert_eq!(result.expected_success, result.actual_success);
    assert_eq!(result.halted, result.observed.halted);
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LoggedOrderRequest {
    token_id: String,
    side: String,
    order_type: String,
    price: Decimal,
    size: Decimal,
}

fn logged_order_requests(result: &HedgeReplayResult) -> Vec<LoggedOrderRequest> {
    result
        .observed
        .request_log
        .iter()
        .filter(|record| record.method == "POST" && record.path == "/order")
        .map(|record| parse_logged_order_request(record))
        .collect::<Result<Vec<_>>>()
        .expect("logged order requests should parse")
}

fn parse_logged_order_request(record: &MockRequestRecord) -> Result<LoggedOrderRequest> {
    let body = record
        .body
        .as_deref()
        .ok_or_else(|| anyhow!("logged order request body missing for {}", record.path))?;
    let payload: Value =
        serde_json::from_str(body).context("failed to parse logged order request body")?;
    let order = payload
        .get("order")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("logged order body missing order object"))?;

    Ok(LoggedOrderRequest {
        token_id: order
            .get("tokenId")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("logged order missing tokenId"))?
            .to_string(),
        side: order
            .get("side")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("logged order missing side"))?
            .to_string(),
        order_type: payload
            .get("orderType")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("logged order missing orderType"))?
            .to_string(),
        price: {
            let maker_amount = Decimal::from_str(
                order
                    .get("makerAmount")
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow!("logged order missing makerAmount"))?,
            )
            .context("failed to parse logged order makerAmount")?;
            let taker_amount = Decimal::from_str(
                order
                    .get("takerAmount")
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow!("logged order missing takerAmount"))?,
            )
            .context("failed to parse logged order takerAmount")?;
            let side = order
                .get("side")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("logged order missing side"))?;
            if maker_amount <= Decimal::ZERO || taker_amount <= Decimal::ZERO {
                bail!("logged order amount fields must be positive");
            }
            match side {
                "BUY" => maker_amount / taker_amount,
                "SELL" => taker_amount / maker_amount,
                other => bail!("unsupported logged order side {}", other),
            }
        },
        size: {
            let maker_amount = Decimal::from_str(
                order
                    .get("makerAmount")
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow!("logged order missing makerAmount"))?,
            )
            .context("failed to parse logged order makerAmount")?;
            let taker_amount = Decimal::from_str(
                order
                    .get("takerAmount")
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow!("logged order missing takerAmount"))?,
            )
            .context("failed to parse logged order takerAmount")?;
            let side = order
                .get("side")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("logged order missing side"))?;
            match side {
                "BUY" => taker_amount / Decimal::from(1_000_000u64),
                "SELL" => maker_amount / Decimal::from(1_000_000u64),
                other => bail!("unsupported logged order side {}", other),
            }
        },
    })
}

#[tokio::test]
async fn layer2_raw_trade_immediate_attribution_matches_fixture() {
    let result = run_hedge_replay("raw_trade_immediate_attribution.json")
        .await
        .expect("raw trade replay should run");

    assert_layer2_pass(&result);
}

#[tokio::test]
async fn layer2_raw_trade_sellback_cheaper_prefers_sellback() {
    let result = run_hedge_replay("raw_trade_sellback_cheaper.json")
        .await
        .expect("raw trade sellback replay should run");

    assert_layer2_pass(&result);
    assert_eq!(
        logged_order_requests(&result),
        vec![LoggedOrderRequest {
            token_id: "3401".to_string(),
            side: "SELL".to_string(),
            order_type: "FOK".to_string(),
            price: dec!(0.59),
            size: dec!(10),
        }]
    );
}

#[tokio::test]
async fn layer2_sellback_miss_recompute_switches_to_hedge() {
    let result = run_hedge_replay("raw_trade_sellback_miss_recompute_switches_to_hedge.json")
        .await
        .expect("sellback recompute success replay should run");

    assert_layer2_pass(&result);
    assert_eq!(
        logged_order_requests(&result),
        vec![
            LoggedOrderRequest {
                token_id: "3801".to_string(),
                side: "SELL".to_string(),
                order_type: "FOK".to_string(),
                price: dec!(0.73),
                size: dec!(10),
            },
            LoggedOrderRequest {
                token_id: "3802".to_string(),
                side: "BUY".to_string(),
                order_type: "GTC".to_string(),
                price: dec!(0.27),
                size: dec!(10),
            },
            LoggedOrderRequest {
                token_id: "3801".to_string(),
                side: "SELL".to_string(),
                order_type: "GTC".to_string(),
                price: dec!(0.76),
                size: dec!(10),
            },
            LoggedOrderRequest {
                token_id: "3802".to_string(),
                side: "SELL".to_string(),
                order_type: "GTC".to_string(),
                price: dec!(0.27),
                size: dec!(10),
            },
        ]
    );
    assert_eq!(result.observed.result_status.as_deref(), Some("success"));
    assert_eq!(result.observed.hedge_leg_status.as_deref(), Some("success"));
    assert_eq!(result.observed.sellback_leg_status.as_deref(), Some("skipped"));
}

#[tokio::test]
async fn layer2_order_update_fallback_respects_residual_exposure() {
    let result = run_hedge_replay("order_update_fallback_partial_accounted.json")
        .await
        .expect("fallback replay should run");

    assert_layer2_pass(&result);
    assert_eq!(result.observed.planned_hedge_shares, Some(dec!(6)));
}

#[tokio::test]
async fn layer2_order_update_fallback_split_executes_hedge_and_sellback() {
    let result = run_hedge_replay("order_update_fallback_split_resolution.json")
        .await
        .expect("split fallback replay should run");

    assert_layer2_pass(&result);
    assert_eq!(
        logged_order_requests(&result),
        vec![
            LoggedOrderRequest {
                token_id: "3502".to_string(),
                side: "BUY".to_string(),
                order_type: "GTC".to_string(),
                price: dec!(0.27),
                size: dec!(6),
            },
            LoggedOrderRequest {
                token_id: "3501".to_string(),
                side: "SELL".to_string(),
                order_type: "FOK".to_string(),
                price: dec!(0.73),
                size: dec!(4),
            },
            LoggedOrderRequest {
                token_id: "3501".to_string(),
                side: "SELL".to_string(),
                order_type: "GTC".to_string(),
                price: dec!(0.76),
                size: dec!(6),
            },
            LoggedOrderRequest {
                token_id: "3502".to_string(),
                side: "SELL".to_string(),
                order_type: "GTC".to_string(),
                price: dec!(0.27),
                size: dec!(6),
            },
        ]
    );
}

#[tokio::test]
async fn layer2_exchange_sync_missing_fill_uses_exchange_truth_path() {
    let result = run_hedge_replay("exchange_sync_missing_fill.json")
        .await
        .expect("exchange-sync replay should run");

    assert_layer2_pass(&result);
}

#[tokio::test]
async fn layer2_exchange_sync_missing_fill_sellback_cheaper_prefers_sellback() {
    let result = run_hedge_replay("exchange_sync_missing_fill_sellback_cheaper.json")
        .await
        .expect("exchange-sync sellback replay should run");

    assert_layer2_pass(&result);
    assert_eq!(
        logged_order_requests(&result),
        vec![LoggedOrderRequest {
            token_id: "3601".to_string(),
            side: "SELL".to_string(),
            order_type: "FOK".to_string(),
            price: dec!(0.59),
            size: dec!(10),
        }]
    );
}

#[tokio::test]
async fn layer2_sellback_miss_recompute_fails_closed_after_one_retry() {
    let result = run_hedge_replay("raw_trade_sellback_miss_recompute_fails_closed.json")
        .await
        .expect("sellback recompute fail-closed replay should run");

    assert_layer2_pass(&result);
    assert_eq!(
        logged_order_requests(&result),
        vec![
            LoggedOrderRequest {
                token_id: "3901".to_string(),
                side: "SELL".to_string(),
                order_type: "FOK".to_string(),
                price: dec!(0.73),
                size: dec!(10),
            },
            LoggedOrderRequest {
                token_id: "3901".to_string(),
                side: "SELL".to_string(),
                order_type: "FOK".to_string(),
                price: dec!(0.73),
                size: dec!(10),
            },
            LoggedOrderRequest {
                token_id: "3901".to_string(),
                side: "SELL".to_string(),
                order_type: "FOK".to_string(),
                price: dec!(0.01),
                size: dec!(10),
            },
        ]
    );
    assert_eq!(result.observed.result_status.as_deref(), Some("failed"));
    assert_eq!(result.observed.sellback_leg_status.as_deref(), Some("unverified"));
}

#[tokio::test]
async fn layer2_duplicate_trade_id_is_deduped_before_second_hedge() {
    let result = run_hedge_replay("duplicate_trade_id_deduped.json")
        .await
        .expect("duplicate trade replay should run");

    assert_layer2_pass(&result);
}

#[tokio::test]
async fn layer2_recently_cancelled_order_is_not_misattributed() {
    let result = run_hedge_replay("cancelled_order_not_misattributed.json")
        .await
        .expect("cancelled-order replay should run");

    assert_layer2_pass(&result);
    assert_eq!(result.observed.result_status, None);
}

#[tokio::test]
async fn layer2_orphan_recovery_routes_through_reconciliation_path() {
    let result = run_hedge_replay("reconciliation_orphan_recovery.json")
        .await
        .expect("orphan recovery replay should run");

    assert_layer2_pass(&result);
}

#[tokio::test]
async fn layer2_orphan_recovery_split_executes_hedge_and_sellback() {
    let result = run_hedge_replay("reconciliation_orphan_recovery_split_resolution.json")
        .await
        .expect("orphan split replay should run");

    assert_layer2_pass(&result);
    assert_eq!(
        logged_order_requests(&result),
        vec![
            LoggedOrderRequest {
                token_id: "3702".to_string(),
                side: "BUY".to_string(),
                order_type: "GTC".to_string(),
                price: dec!(0.27),
                size: dec!(6),
            },
            LoggedOrderRequest {
                token_id: "3701".to_string(),
                side: "SELL".to_string(),
                order_type: "FOK".to_string(),
                price: dec!(0.73),
                size: dec!(4),
            },
            LoggedOrderRequest {
                token_id: "3701".to_string(),
                side: "SELL".to_string(),
                order_type: "GTC".to_string(),
                price: dec!(0.76),
                size: dec!(6),
            },
            LoggedOrderRequest {
                token_id: "3702".to_string(),
                side: "SELL".to_string(),
                order_type: "GTC".to_string(),
                price: dec!(0.27),
                size: dec!(6),
            },
        ]
    );
}
