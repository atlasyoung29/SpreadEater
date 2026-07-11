use std::env;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use futures_util::StreamExt;
use reqwest::StatusCode;
use rust_decimal::Decimal;
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Executor, PgPool, Row};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_tungstenite::connect_async;
use url::Url;
use uuid::Uuid;

use spreadeater_core::payloads::{
    DecisionEventPayload, HedgeDecisionPayload, HedgeExitPathPayload, HedgeIntentPayload,
    HedgeResultPayload, MonitorDegradedPayload, NeutralityPayload, OrderCancelledPayload,
    OrderResizedPayload, OrderSubmittedPayload, QuoteLegSummary,
};
use spreadeater_core::{CancelReasonCode, EventEnvelope, EventType, Priority};
use spreadeater_monitor::api::build_app;
use spreadeater_monitor::dto::{
    BotErrorLogEntry, ConfigResponse, EventListItem, EventListResponse, LiveFrame,
    MarketDetailResponse, MarketSummary, OverviewResponse, PageResponse, TraceDetailResponse,
};
use spreadeater_monitor::ingestor::LogIngestor;
use spreadeater_monitor::logs::BotLogTailer;
use spreadeater_monitor::projector::PostgresProjector;
use spreadeater_monitor::store::{
    broadcast_event_updates, fetch_error_logs, fetch_inventory, ErrorLogFilter, InventoryFilter,
};

const DEFAULT_TEST_DATABASE_URL: &str =
    "postgres://postgres:postgres@127.0.0.1:54329/spreadeater_monitor";
const RUN_ID: &str = "run_fixture_monitor";
const CYCLE_ID: &str = "cycle_fixture_001";
const MODE: &str = "dry-run";
const CONDITION_ID: &str = "condition_fixture_01";
const MARKET_SLUG: &str = "fixture-monitor-market";
const QUESTION: &str = "Will the monitor integration suite stay deterministic?";
const TRACE_SUCCESS: &str = "trace_fixture_success";
const TRACE_CANCELLED: &str = "trace_fixture_cancelled";
const ORDER_OLD: &str = "order_fixture_old";
const ORDER_NEW: &str = "order_fixture_new";
const ORDER_CANCELLED: &str = "order_fixture_cancelled";
const HEDGE_ID: &str = "hedge_fixture_01";

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires local Postgres at SPREADEATER_MONITOR_TEST_DATABASE_URL or docker-compose.monitor.yml default"]
async fn migration_smoke_creates_projection_tables() -> Result<()> {
    let harness = TestHarness::new().await?;
    let required_tables = [
        "runs",
        "events_raw",
        "markets",
        "traces",
        "orders",
        "fills",
        "hedges",
        "neutrality_evaluations",
        "cancellations",
        "positions_latest",
        "ingestion_offsets",
        "bot_error_logs",
        "bot_log_offsets",
    ];

    for table in required_tables {
        let exists = sqlx::query_scalar::<_, Option<String>>("SELECT to_regclass($1)::text")
            .bind(format!("public.{table}"))
            .fetch_one(&harness.pool)
            .await?;
        assert_eq!(exists.as_deref(), Some(table), "missing table {table}");
    }

    harness.teardown().await
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires local Postgres at SPREADEATER_MONITOR_TEST_DATABASE_URL or docker-compose.monitor.yml default"]
async fn ingest_duplicate_replay_and_rebuild_stay_consistent() -> Result<()> {
    let harness = TestHarness::new().await?;
    let ingestor = harness.ingestor(None);

    ingestor.ingest_once().await?;
    let baseline = harness.snapshot().await?;
    assert_eq!(baseline.events_raw, 14);
    assert_eq!(baseline.cancellations, 2);
    assert_eq!(baseline.traces, 2);
    assert_eq!(baseline.observer_health, "degraded");
    assert_eq!(baseline.market_status.as_deref(), Some("approved"));
    assert_eq!(baseline.trace_status.as_deref(), Some("neutral"));

    harness
        .projector
        .store_offset(&harness.fixture.file_path, RUN_ID, 0)
        .await?;
    ingestor.ingest_once().await?;
    let duplicate = harness.snapshot().await?;
    assert_eq!(duplicate, baseline);

    let rebuild_stats = ingestor.rebuild().await?;
    assert_eq!(rebuild_stats.events_processed, 14);
    assert_eq!(rebuild_stats.files_processed, 1);
    assert_eq!(rebuild_stats.last_run_id.as_deref(), Some(RUN_ID));

    let rebuilt = harness.snapshot().await?;
    assert_eq!(rebuilt.events_raw, baseline.events_raw + 1);
    assert_eq!(rebuilt.projection_rebuilt_events, 1);
    assert_eq!(rebuilt.markets, baseline.markets);
    assert_eq!(rebuilt.traces, baseline.traces);
    assert_eq!(rebuilt.orders, baseline.orders);
    assert_eq!(rebuilt.fills, baseline.fills);
    assert_eq!(rebuilt.hedges, baseline.hedges);
    assert_eq!(rebuilt.cancellations, baseline.cancellations);
    assert_eq!(
        rebuilt.neutrality_evaluations,
        baseline.neutrality_evaluations
    );
    assert_eq!(rebuilt.positions_latest, baseline.positions_latest);
    assert_eq!(rebuilt.observer_health, baseline.observer_health);
    assert_eq!(rebuilt.market_status, baseline.market_status);
    assert_eq!(rebuilt.trace_status, baseline.trace_status);

    harness.teardown().await
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires local Postgres at SPREADEATER_MONITOR_TEST_DATABASE_URL or docker-compose.monitor.yml default"]
async fn api_endpoints_cover_success_and_error_cases() -> Result<()> {
    let harness = TestHarness::new().await?;
    let server = TestServer::start(harness.pool.clone()).await?;
    let client = reqwest::Client::new();

    let empty_overview = client.get(server.url("/api/v1/overview")).send().await?;
    assert_eq!(empty_overview.status(), StatusCode::SERVICE_UNAVAILABLE);

    let missing_market = client
        .get(server.url("/api/v1/markets/not-real"))
        .send()
        .await?;
    assert_eq!(missing_market.status(), StatusCode::NOT_FOUND);

    let missing_trace = client
        .get(server.url("/api/v1/traces/not-real"))
        .send()
        .await?;
    assert_eq!(missing_trace.status(), StatusCode::NOT_FOUND);

    let invalid_events = client
        .get(server.url("/api/v1/events?event_type=bogus"))
        .send()
        .await?;
    assert_eq!(invalid_events.status(), StatusCode::BAD_REQUEST);

    harness.ingestor(None).ingest_once().await?;

    let overview: OverviewResponse = get_json(&client, server.url("/api/v1/overview")).await?;
    assert_eq!(overview.run_id, RUN_ID);
    assert_eq!(overview.active_markets, 1);
    assert_eq!(overview.open_orders, 0);
    assert_eq!(overview.observer_health, "degraded");
    assert_eq!(overview.open_order_markets, 0);
    assert_eq!(overview.inventory_markets, 1);
    assert_eq!(overview.open_order_preview.len(), 0);
    assert_eq!(overview.inventory_preview.len(), 1);
    assert!(!overview.recent_history.is_empty());
    assert_eq!(overview.recent_alerts.len(), 3);
    assert!(overview.recent_alerts.iter().all(|item| {
        matches!(
            item.event_type.as_str(),
            "order_submitted" | "fill_detected"
        )
    }));
    assert!(overview
        .recent_alerts
        .iter()
        .any(|item| item.event_type == "fill_detected"));
    let replaced_order = overview
        .recent_history
        .iter()
        .find(|item| item.order_id.as_deref() == Some(ORDER_OLD))
        .context("missing replaced order in recent history")?;
    assert_eq!(replaced_order.order_state.as_deref(), Some("replaced"));
    assert_eq!(
        replaced_order.replacement_order_id.as_deref(),
        Some(ORDER_NEW)
    );
    let cancelled_order = overview
        .recent_history
        .iter()
        .find(|item| item.order_id.as_deref() == Some(ORDER_CANCELLED))
        .context("missing cancelled order in recent history")?;
    assert_eq!(cancelled_order.order_state.as_deref(), Some("cancelled"));
    assert_eq!(
        cancelled_order.order_cancel_reason.as_deref(),
        Some("RISK_HALT")
    );

    let open_orders: PageResponse<MarketSummary> =
        get_json(&client, server.url("/api/v1/open-orders?page_size=10")).await?;
    assert_eq!(open_orders.total, 0);

    let inventory: PageResponse<MarketSummary> =
        get_json(&client, server.url("/api/v1/inventory?page_size=10")).await?;
    assert_eq!(inventory.total, 1);
    assert_eq!(inventory.items.len(), 1);
    assert_eq!(inventory.items[0].condition_id, CONDITION_ID);

    let watchlist: PageResponse<MarketSummary> =
        get_json(&client, server.url("/api/v1/watchlist?page_size=10")).await?;
    assert_eq!(watchlist.total, 1);
    assert_eq!(watchlist.items[0].condition_id, CONDITION_ID);

    let history: PageResponse<EventListItem> =
        get_json(&client, server.url("/api/v1/history?page_size=10")).await?;
    assert!(history.total >= 10);
    assert_eq!(history.items.len(), 10);
    assert!(history
        .items
        .iter()
        .any(|item| item.event_type == "hedge_decision_evaluated"));
    assert!(history
        .items
        .iter()
        .any(|item| item.event_type == "hedge_exit_path_recorded"));

    let config: ConfigResponse = get_json(&client, server.url("/api/v1/config")).await?;
    assert!(config.path.ends_with("config.json"));
    assert!(config.value.get("mode").is_some());

    let market: MarketDetailResponse = get_json(
        &client,
        server.url(&format!(
            "/api/v1/markets/{CONDITION_ID}?include_timeline=true"
        )),
    )
    .await?;
    assert_eq!(market.condition_id, CONDITION_ID);
    assert_eq!(market.recent_traces.len(), 2);
    assert_eq!(market.recent_events.len(), 13);
    assert_eq!(market.decision_status.as_deref(), Some("approved"));
    assert!(market.is_neutral);
    assert!(market
        .recent_events
        .iter()
        .any(|item| item.event_type == "hedge_decision_evaluated"));
    assert!(market
        .recent_events
        .iter()
        .any(|item| item.event_type == "hedge_exit_path_recorded"));

    let trace: TraceDetailResponse = get_json(
        &client,
        server.url(&format!("/api/v1/traces/{TRACE_SUCCESS}")),
    )
    .await?;
    assert_eq!(trace.trace_id, TRACE_SUCCESS);
    assert_eq!(trace.status, "neutral");
    assert_eq!(trace.orders.len(), 2);
    let decision_payload: DecisionEventPayload =
        serde_json::from_value(trace.decision.context("missing decision snapshot")?.payload)?;
    assert_eq!(
        decision_payload.frontier_counterfactual_budget_usd,
        Some(decimal("75.00"))
    );
    assert_eq!(
        decision_payload
            .frontier_counterfactual_entrant_condition_id
            .as_deref(),
        Some(CONDITION_ID)
    );
    assert_eq!(
        decision_payload
            .frontier_counterfactual_loser_condition_id
            .as_deref(),
        Some("condition_fixture_loser")
    );
    assert_eq!(
        trace
            .orders
            .iter()
            .find(|order| order.order_id == ORDER_NEW)
            .and_then(|order| order.side.as_deref()),
        Some("BUY")
    );
    assert_eq!(trace.fills.len(), 1);
    assert_eq!(trace.hedges.len(), 1);
    assert_eq!(trace.timeline.len(), 9);
    assert!(trace
        .timeline
        .iter()
        .any(|item| item.event_type == "hedge_decision_evaluated"));
    assert!(trace
        .timeline
        .iter()
        .any(|item| item.event_type == "hedge_exit_path_recorded"));

    let first_page: EventListResponse = get_json(
        &client,
        server.url("/api/v1/events?condition_id=condition_fixture_01&limit=3"),
    )
    .await?;
    assert_eq!(first_page.items.len(), 3);
    assert!(first_page.next_cursor.is_some());

    let second_page: EventListResponse = get_json(
        &client,
        server.url(&format!(
            "/api/v1/events?condition_id={CONDITION_ID}&limit=3&before_id={}",
            first_page.next_cursor.unwrap()
        )),
    )
    .await?;
    assert_eq!(second_page.items.len(), 3);
    assert_ne!(first_page.items[0].id, second_page.items[0].id);

    let cancelled: EventListResponse = get_json(
        &client,
        server.url("/api/v1/events?trace_id=trace_fixture_cancelled&event_type=order_cancelled"),
    )
    .await?;
    assert_eq!(cancelled.items.len(), 1);
    assert_eq!(cancelled.items[0].reason_code.as_deref(), Some("RiskHalt"));

    server.shutdown().await;
    harness.teardown().await
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires local Postgres at SPREADEATER_MONITOR_TEST_DATABASE_URL or docker-compose.monitor.yml default"]
async fn websocket_receives_initial_overview_and_live_alert_updates() -> Result<()> {
    let harness = TestHarness::new().await?;
    let broadcaster = spreadeater_monitor::store::LiveBroadcaster::new(32);
    harness
        .ingestor(Some(broadcaster.clone()))
        .ingest_once()
        .await?;

    let server =
        TestServer::start_with_broadcaster(harness.pool.clone(), broadcaster.clone()).await?;
    let (mut socket, _) = connect_async(server.ws_url("/ws/live")).await?;

    let initial = next_live_frame(&mut socket).await?;
    assert_eq!(initial.channel, "overview");
    let initial_overview: OverviewResponse = serde_json::from_value(initial.payload)?;
    assert_eq!(initial_overview.run_id, RUN_ID);

    let event = build_monitor_degraded_event(
        "00000000-0000-0000-0000-000000000021",
        at_seconds(20),
        "websocket smoke signal",
    );
    let outcome = harness.projector.project_batch(&[event]).await?;
    for projected in &outcome.projected_events {
        broadcast_event_updates(&harness.pool, &broadcaster, projected).await?;
    }

    let mut saw_alert = false;
    for _ in 0..4 {
        let frame = next_live_frame(&mut socket).await?;
        if frame.channel == "alerts" {
            let item: EventListItem = serde_json::from_value(frame.payload)?;
            assert_eq!(item.event_type, "monitor_degraded");
            assert_eq!(item.payload["degraded_reason"], "websocket smoke signal");
            saw_alert = true;
            break;
        }
    }
    assert!(
        saw_alert,
        "expected alert frame after projecting monitor_degraded"
    );

    server.shutdown().await;
    harness.teardown().await
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires local Postgres at SPREADEATER_MONITOR_TEST_DATABASE_URL or docker-compose.monitor.yml default"]
async fn fill_updates_inventory_even_before_neutrality_event_arrives() -> Result<()> {
    let harness = TestHarness::new().await?;
    harness
        .projector
        .project_batch(&[
            build_event(
                "00000000-0000-0000-0000-000000000101",
                EventType::DecisionEvaluated,
                Priority::Normal,
                at_seconds(0),
                serde_json::to_value(DecisionEventPayload {
                    candidate_quotes: vec![QuoteLegSummary {
                        leg: "YES".to_string(),
                        price: decimal("0.42"),
                        size: decimal("2.0"),
                        status: "approved".to_string(),
                        reason: None,
                    }],
                    reasons: vec!["reward_viable".to_string()],
                    effective_quote_size: decimal("2.0"),
                    expected_reward_usd_day: Some(decimal("4.20")),
                    expected_hedge_cost_usd: Some(decimal("0.60")),
                    expected_edge_usd: Some(decimal("1.20")),
                    expected_edge_pct: Some(decimal("0.08")),
                    committed_capital_usd: Some(decimal("0.84")),
                    score_share: Some(decimal("0.15")),
                    max_hedgeable_size: Some(decimal("3.00")),
                    competition_multiplier_used: Some(decimal("1.20")),
                    api_balance_usd: Some(decimal("500.00")),
                    available_budget_usd: Some(decimal("499.16")),
                    rank_in_cycle: Some(1),
                    ranked_market_count: Some(4),
                    ranking_metric_name: Some("reward_per_share".to_string()),
                    ranking_metric_value: Some(decimal("2.10")),
                    frontier_eligible: None,
                    frontier_requires_reallocation: None,
                    frontier_replaces_condition_id: None,
                    frontier_replaced_by_condition_id: None,
                    frontier_counterfactual_budget_usd: None,
                    frontier_counterfactual_reclaimable_bid_capital_usd: None,
                    frontier_counterfactual_entrant_condition_id: None,
                    frontier_counterfactual_entrant_ranking_metric_name: None,
                    frontier_counterfactual_entrant_ranking_metric_value: None,
                    frontier_counterfactual_entrant_expected_reward_usd_day: None,
                    frontier_counterfactual_loser_condition_id: None,
                    frontier_counterfactual_loser_ranking_metric_name: None,
                    frontier_counterfactual_loser_ranking_metric_value: None,
                    frontier_counterfactual_loser_expected_reward_usd_day: None,
                    would_trade: true,
                })
                .unwrap(),
            ),
            build_event(
                "00000000-0000-0000-0000-000000000102",
                EventType::OrderSubmitted,
                Priority::High,
                at_seconds(1),
                serde_json::to_value(OrderSubmittedPayload {
                    leg: "YES".to_string(),
                    side: "BUY".to_string(),
                    price: decimal("0.42"),
                    size: decimal("2.0"),
                    matched_size: decimal("0"),
                    token_id: "token_yes_fixture".to_string(),
                    neg_risk: true,
                    origin: Some("new_quote".to_string()),
                    role: Some("bid_entry".to_string()),
                })
                .unwrap(),
            )
            .with_trace_id(TRACE_SUCCESS.to_string())
            .with_order_id(ORDER_NEW.to_string())
            .with_asset_id("token_yes_fixture".to_string()),
            build_event(
                "00000000-0000-0000-0000-000000000103",
                EventType::FillDetected,
                Priority::Critical,
                at_seconds(2),
                json!({
                    "trade_id":"fill_fixture_inventory",
                    "fill_price":"0.42",
                    "fill_size":"2.0",
                    "side":"BUY",
                    "outcome":"YES",
                    "fallback_match":false
                }),
            )
            .with_trace_id(TRACE_SUCCESS.to_string())
            .with_order_id(ORDER_NEW.to_string()),
        ])
        .await?;

    let inventory = fetch_inventory(&harness.pool, InventoryFilter::default()).await?;
    let market = inventory
        .items
        .iter()
        .find(|item| item.condition_id == CONDITION_ID)
        .context("missing market in inventory after fill-only projection")?;

    assert_eq!(market.yes_size, decimal("2.0"));
    assert_eq!(market.no_size, Decimal::ZERO);
    assert_eq!(market.net_exposure, decimal("2.0"));
    assert_eq!(market.complete_sets, Decimal::ZERO);
    assert!(!market.is_neutral);

    harness.teardown().await
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires local Postgres at SPREADEATER_MONITOR_TEST_DATABASE_URL or docker-compose.monitor.yml default"]
async fn bot_log_errors_are_ingested_persisted_and_broadcast() -> Result<()> {
    let harness = TestHarness::new().await?;
    let broadcaster = spreadeater_monitor::store::LiveBroadcaster::new(32);
    let server =
        TestServer::start_with_broadcaster(harness.pool.clone(), broadcaster.clone()).await?;

    let log_dir = harness.temp_dir.path().join("logs");
    tokio::fs::create_dir_all(&log_dir).await?;
    let log_path = log_dir.join("spreadeater-bot.log");
    tokio::fs::write(
        &log_path,
        concat!(
            "2026-03-11T16:32:00Z ERROR hedge verification failed\n",
            "2026-03-11T16:33:00Z INFO normal cycle line\n",
            "thread 'main' panicked at impossible state\n"
        ),
    )
    .await?;

    let (mut socket, _) = connect_async(server.ws_url("/ws/live")).await?;
    let tailer = BotLogTailer::new(harness.pool.clone(), log_path.clone(), Some(broadcaster));
    assert_eq!(tailer.ingest_once().await?, 2);

    let errors = fetch_error_logs(
        &harness.pool,
        ErrorLogFilter {
            page: 1,
            page_size: 10,
            ..Default::default()
        },
    )
    .await?;
    assert_eq!(errors.total, 2);
    assert!(errors
        .items
        .iter()
        .any(|item| item.level.as_deref() == Some("error")));
    assert!(errors.items.iter().any(|item| item.level.is_none()));

    let api_errors: PageResponse<BotErrorLogEntry> = get_json(
        &reqwest::Client::new(),
        server.url("/api/v1/errors?page_size=10"),
    )
    .await?;
    assert_eq!(api_errors.total, 2);

    let mut saw_error_frame = false;
    for _ in 0..4 {
        let frame = next_live_frame(&mut socket).await?;
        if frame.channel == "errors" {
            let item: BotErrorLogEntry = serde_json::from_value(frame.payload)?;
            assert!(
                item.message.contains("hedge verification failed")
                    || item.message.contains("panicked")
            );
            saw_error_frame = true;
            break;
        }
    }
    assert!(saw_error_frame, "expected websocket errors frame");

    server.shutdown().await;
    harness.teardown().await
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProjectionSnapshot {
    events_raw: i64,
    projection_rebuilt_events: i64,
    markets: i64,
    traces: i64,
    orders: i64,
    fills: i64,
    hedges: i64,
    neutrality_evaluations: i64,
    cancellations: i64,
    positions_latest: i64,
    observer_health: String,
    market_status: Option<String>,
    trace_status: Option<String>,
}

struct TestHarness {
    database: TestDatabase,
    pool: PgPool,
    projector: PostgresProjector,
    temp_dir: TempDir,
    fixture: FixtureRun,
}

impl TestHarness {
    async fn new() -> Result<Self> {
        let database = TestDatabase::create().await?;
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect(&database.database_url)
            .await
            .with_context(|| format!("connect test database {}", database.database_url))?;
        let projector = PostgresProjector::new(pool.clone());
        projector.migrate().await?;

        let temp_dir = tempfile::tempdir().context("create temp event log dir")?;
        let fixture = write_fixture_run(temp_dir.path()).await?;

        Ok(Self {
            database,
            pool,
            projector,
            temp_dir,
            fixture,
        })
    }

    fn ingestor(
        &self,
        broadcaster: Option<spreadeater_monitor::store::LiveBroadcaster>,
    ) -> LogIngestor {
        LogIngestor::new(
            self.projector.clone(),
            self.temp_dir.path().to_path_buf(),
            broadcaster,
        )
    }

    async fn snapshot(&self) -> Result<ProjectionSnapshot> {
        Ok(ProjectionSnapshot {
            events_raw: count_rows(&self.pool, "events_raw").await?,
            projection_rebuilt_events: sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM events_raw WHERE event_type = 'projection_rebuilt'",
            )
            .fetch_one(&self.pool)
            .await?,
            markets: count_rows(&self.pool, "markets").await?,
            traces: count_rows(&self.pool, "traces").await?,
            orders: count_rows(&self.pool, "orders").await?,
            fills: count_rows(&self.pool, "fills").await?,
            hedges: count_rows(&self.pool, "hedges").await?,
            neutrality_evaluations: count_rows(&self.pool, "neutrality_evaluations").await?,
            cancellations: count_rows(&self.pool, "cancellations").await?,
            positions_latest: count_rows(&self.pool, "positions_latest").await?,
            observer_health: sqlx::query_scalar::<_, String>(
                "SELECT observer_health FROM runs WHERE run_id = $1",
            )
            .bind(RUN_ID)
            .fetch_one(&self.pool)
            .await?,
            market_status: sqlx::query_scalar::<_, Option<String>>(
                "SELECT decision_status FROM markets WHERE run_id = $1 AND condition_id = $2",
            )
            .bind(RUN_ID)
            .bind(CONDITION_ID)
            .fetch_one(&self.pool)
            .await?,
            trace_status: sqlx::query_scalar::<_, Option<String>>(
                "SELECT status FROM traces WHERE trace_id = $1",
            )
            .bind(TRACE_SUCCESS)
            .fetch_one(&self.pool)
            .await?,
        })
    }

    async fn teardown(self) -> Result<()> {
        drop(self.projector);
        drop(self.pool);
        self.database.drop().await
    }
}

struct TestDatabase {
    admin_url: String,
    database_url: String,
    database_name: String,
}

impl TestDatabase {
    async fn create() -> Result<Self> {
        let base_url = env::var("SPREADEATER_MONITOR_TEST_DATABASE_URL")
            .unwrap_or_else(|_| DEFAULT_TEST_DATABASE_URL.to_string());
        let admin_url = with_database_name(&base_url, "postgres")?;
        let database_name = format!("spreadeater_monitor_test_{}", Uuid::new_v4().simple());
        let database_url = with_database_name(&base_url, &database_name)?;

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&admin_url)
            .await
            .with_context(|| {
                format!(
                    "connect admin Postgres at {admin_url}; start docker compose -f docker-compose.monitor.yml up -d"
                )
            })?;
        admin_pool
            .execute(format!("CREATE DATABASE {database_name}").as_str())
            .await
            .with_context(|| format!("create test database {database_name}"))?;
        drop(admin_pool);

        Ok(Self {
            admin_url,
            database_url,
            database_name,
        })
    }

    async fn drop(self) -> Result<()> {
        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&self.admin_url)
            .await?;
        admin_pool
            .execute(
                format!(
                    "DROP DATABASE IF EXISTS {} WITH (FORCE)",
                    self.database_name
                )
                .as_str(),
            )
            .await
            .with_context(|| format!("drop test database {}", self.database_name))?;
        Ok(())
    }
}

struct FixtureRun {
    file_path: String,
}

async fn write_fixture_run(root: &Path) -> Result<FixtureRun> {
    let run_dir = root.join(RUN_ID);
    tokio::fs::create_dir_all(&run_dir).await?;
    let path = run_dir.join("events.jsonl");
    let events = fixture_events();

    let mut body = String::new();
    for event in events {
        body.push_str(&serde_json::to_string(&event)?);
        body.push('\n');
    }
    tokio::fs::write(&path, body).await?;

    Ok(FixtureRun {
        file_path: path.to_string_lossy().to_string(),
    })
}

struct TestServer {
    base_url: String,
    ws_base_url: String,
    task: JoinHandle<()>,
}

impl TestServer {
    async fn start(pool: PgPool) -> Result<Self> {
        Self::start_with_broadcaster(pool, spreadeater_monitor::store::LiveBroadcaster::new(32))
            .await
    }

    async fn start_with_broadcaster(
        pool: PgPool,
        broadcaster: spreadeater_monitor::store::LiveBroadcaster,
    ) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let app = build_app(
            pool,
            broadcaster,
            PathBuf::from("does-not-exist"),
            repo_root().join("config.json"),
        );
        let task = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve monitor API in integration test");
        });

        Ok(Self {
            base_url: format!("http://{addr}"),
            ws_base_url: format!("ws://{addr}"),
            task,
        })
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    fn ws_url(&self, path: &str) -> String {
        format!("{}{}", self.ws_base_url, path)
    }

    async fn shutdown(self) {
        self.task.abort();
        let _ = self.task.await;
    }
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .map(Path::to_path_buf)
        .expect("workspace root")
}

fn fixture_events() -> Vec<EventEnvelope> {
    vec![
        build_event(
            "00000000-0000-0000-0000-000000000001",
            EventType::DecisionEvaluated,
            Priority::Normal,
            at_seconds(0),
            serde_json::to_value(DecisionEventPayload {
                candidate_quotes: vec![
                    QuoteLegSummary {
                        leg: "YES".to_string(),
                        price: decimal("0.42"),
                        size: decimal("3.0"),
                        status: "approved".to_string(),
                        reason: None,
                    },
                    QuoteLegSummary {
                        leg: "NO".to_string(),
                        price: decimal("0.57"),
                        size: decimal("3.0"),
                        status: "approved".to_string(),
                        reason: None,
                    },
                ],
                reasons: vec![
                    "reward_viable".to_string(),
                    "hedge_depth_available".to_string(),
                ],
                effective_quote_size: decimal("3.0"),
                expected_reward_usd_day: Some(decimal("12.50")),
                expected_hedge_cost_usd: Some(decimal("1.20")),
                expected_edge_usd: Some(decimal("3.40")),
                expected_edge_pct: Some(decimal("0.11")),
                committed_capital_usd: Some(decimal("6.00")),
                score_share: Some(decimal("0.20")),
                max_hedgeable_size: Some(decimal("5.00")),
                competition_multiplier_used: Some(decimal("1.20")),
                api_balance_usd: Some(decimal("2000.00")),
                available_budget_usd: Some(decimal("1994.00")),
                rank_in_cycle: Some(2),
                ranked_market_count: Some(6),
                ranking_metric_name: Some("reward_per_share".to_string()),
                ranking_metric_value: Some(decimal("4.166666666666666667")),
                frontier_eligible: Some(true),
                frontier_requires_reallocation: Some(true),
                frontier_replaces_condition_id: Some("condition_fixture_loser".to_string()),
                frontier_replaced_by_condition_id: None,
                frontier_counterfactual_budget_usd: Some(decimal("75.00")),
                frontier_counterfactual_reclaimable_bid_capital_usd: Some(decimal("25.00")),
                frontier_counterfactual_entrant_condition_id: Some(CONDITION_ID.to_string()),
                frontier_counterfactual_entrant_ranking_metric_name: Some(
                    "reward_per_share".to_string(),
                ),
                frontier_counterfactual_entrant_ranking_metric_value: Some(decimal(
                    "4.166666666666666667",
                )),
                frontier_counterfactual_entrant_expected_reward_usd_day: Some(decimal("12.50")),
                frontier_counterfactual_loser_condition_id: Some(
                    "condition_fixture_loser".to_string(),
                ),
                frontier_counterfactual_loser_ranking_metric_name: Some(
                    "reward_per_share".to_string(),
                ),
                frontier_counterfactual_loser_ranking_metric_value: Some(decimal("1.5000")),
                frontier_counterfactual_loser_expected_reward_usd_day: Some(decimal("4.50")),
                would_trade: true,
            })
            .unwrap(),
        ),
        build_event(
            "00000000-0000-0000-0000-000000000002",
            EventType::QuoteApproved,
            Priority::Normal,
            at_seconds(1),
            json!({"leg":"YES","reason":"best edge"}),
        )
        .with_trace_id(TRACE_SUCCESS.to_string()),
        build_event(
            "00000000-0000-0000-0000-000000000003",
            EventType::OrderSubmitted,
            Priority::High,
            at_seconds(2),
            serde_json::to_value(OrderSubmittedPayload {
                leg: "YES".to_string(),
                side: "BUY".to_string(),
                price: decimal("0.42"),
                size: decimal("3.0"),
                matched_size: decimal("0"),
                token_id: "token_yes_fixture".to_string(),
                neg_risk: true,
                origin: Some("new_quote".to_string()),
                role: Some("bid_entry".to_string()),
            })
            .unwrap(),
        )
        .with_trace_id(TRACE_SUCCESS.to_string())
        .with_order_id(ORDER_OLD.to_string())
        .with_asset_id("token_yes_fixture".to_string()),
        build_event(
            "00000000-0000-0000-0000-000000000004",
            EventType::OrderResized,
            Priority::High,
            at_seconds(3),
            serde_json::to_value(OrderResizedPayload {
                old_order_id: ORDER_OLD.to_string(),
                new_order_id: ORDER_NEW.to_string(),
                old_size: decimal("3.0"),
                new_size: decimal("2.0"),
                old_price: decimal("0.42"),
                new_price: decimal("0.41"),
                reason_code: CancelReasonCode::QuoteDrift,
                origin: Some("quote_refresh".to_string()),
                diagnostics: None,
            })
            .unwrap(),
        )
        .with_trace_id(TRACE_SUCCESS.to_string())
        .with_order_id(ORDER_NEW.to_string())
        .with_asset_id("token_yes_fixture".to_string()),
        build_event(
            "00000000-0000-0000-0000-000000000005",
            EventType::FillDetected,
            Priority::High,
            at_seconds(4),
            json!({
                "trade_id":"fill_fixture_01",
                "fill_price":"0.41",
                "fill_size":"2.0",
                "side":"BUY",
                "outcome":"YES",
                "fallback_match":false
            }),
        )
        .with_trace_id(TRACE_SUCCESS.to_string())
        .with_order_id(ORDER_NEW.to_string()),
        build_event(
            "00000000-0000-0000-0000-000000000006",
            EventType::HedgeDecisionEvaluated,
            Priority::High,
            at_seconds(5),
            serde_json::to_value(HedgeDecisionPayload {
                trigger_leg: "YES".to_string(),
                hedge_side: "BUY".to_string(),
                fill_size: decimal("2.0"),
                fill_price: decimal("0.41"),
                decision_mode: "buy_side_resolution".to_string(),
                decision_reason_code: "hedge_cheaper".to_string(),
                available_hedge_budget_usd: decimal("1994.00"),
                filled_best_bid_price: Some(decimal("0.41")),
                filled_best_bid_size: Some(decimal("2.0")),
                opposite_best_ask_price: Some(decimal("0.59")),
                opposite_best_ask_size: Some(decimal("5.0")),
                planned_hedge_shares: decimal("2.0"),
                planned_hedge_price: decimal("0.59"),
                planned_sellback_shares: decimal("0"),
                planned_sellback_price: decimal("0.59"),
                unresolved_shares: decimal("0"),
            })
            .unwrap(),
        )
        .with_trace_id(TRACE_SUCCESS.to_string())
        .with_hedge_id(HEDGE_ID.to_string()),
        build_event(
            "00000000-0000-0000-0000-000000000007",
            EventType::HedgeIntentCreated,
            Priority::High,
            at_seconds(6),
            serde_json::to_value(HedgeIntentPayload {
                trigger_order_id: ORDER_NEW.to_string(),
                trigger_leg: "YES".to_string(),
                fill_size: decimal("2.0"),
                fill_price: decimal("0.41"),
                hedge_token_id: "token_no_fixture".to_string(),
                hedge_side: "BUY".to_string(),
                planned_hedge_shares: Some(decimal("2.0")),
                planned_hedge_price: Some(decimal("0.59")),
                planned_sellback_shares: None,
                planned_sellback_price: None,
                planned_sellback_reference_bid: None,
                unresolved_shares: Some(decimal("0")),
                pre_resolution_active_orders: Some(1),
                pre_resolution_pending_cancels: Some(0),
                cancel_wait_drained: Some(true),
                origin: Some("fill_handler".to_string()),
            })
            .unwrap(),
        )
        .with_trace_id(TRACE_SUCCESS.to_string())
        .with_hedge_id(HEDGE_ID.to_string()),
        build_event(
            "00000000-0000-0000-0000-000000000008",
            EventType::HedgeResultRecorded,
            Priority::High,
            at_seconds(7),
            serde_json::to_value(HedgeResultPayload {
                hedge_order_id: Some("hedge_order_fixture".to_string()),
                result_status: "success".to_string(),
                hedge_price: Some(decimal("0.59")),
                hedge_leg_status: Some("success".to_string()),
                hedge_cancel_status: Some("confirmed".to_string()),
                hedge_cancel_reason: None,
                hedge_lookup_status: Some("matched".to_string()),
                hedge_lookup_matched_shares: Some(decimal("2.0")),
                hedge_lookup_error: None,
                hedge_trade_ids: Some(vec!["trade-1".to_string()]),
                sellback_order_id: None,
                sellback_price: None,
                sellback_execution_limit_price: None,
                sellback_leg_status: Some("skipped".to_string()),
                sellback_response_status: None,
                sellback_lookup_status: None,
                sellback_lookup_matched_shares: None,
                sellback_lookup_error: None,
                sellback_trade_ids: None,
                post_sync_net_exposure: Some(decimal("0")),
                post_sync_yes_size: Some(decimal("2.0")),
                post_sync_no_size: Some(decimal("2.0")),
                post_sync_source: Some("position_manager".to_string()),
                halt_signal_suppressed: false,
                failure_reason: None,
                latency_ms: 180,
                origin: Some("fill_handler".to_string()),
            })
            .unwrap(),
        )
        .with_trace_id(TRACE_SUCCESS.to_string())
        .with_hedge_id(HEDGE_ID.to_string()),
        build_event(
            "00000000-0000-0000-0000-000000000009",
            EventType::HedgeExitPathRecorded,
            Priority::High,
            at_seconds(8),
            serde_json::to_value(HedgeExitPathPayload {
                post_sync_yes_size: decimal("2.0"),
                post_sync_no_size: decimal("2.0"),
                post_sync_net_exposure: decimal("0"),
                post_sync_complete_sets: decimal("2.0"),
                post_sync_source: "position_manager".to_string(),
                exit_path_status: "merge_succeeded".to_string(),
                merge_eligible_pairs: decimal("2.0"),
                ctf_merge_configured: true,
                merge_attempted: true,
                merge_tx_hash: Some("0xfixturemerge".to_string()),
                merge_failure_reason: None,
                fallback_asks_attempted: false,
                fallback_ask_count: 0,
                fallback_failure_reason: None,
            })
            .unwrap(),
        )
        .with_trace_id(TRACE_SUCCESS.to_string())
        .with_hedge_id(HEDGE_ID.to_string()),
        build_event(
            "00000000-0000-0000-0000-000000000010",
            EventType::NeutralityEvaluated,
            Priority::High,
            at_seconds(9),
            serde_json::to_value(NeutralityPayload {
                pre_yes_size: decimal("2.0"),
                pre_no_size: decimal("0"),
                post_yes_size: decimal("2.0"),
                post_no_size: decimal("2.0"),
                residual_exposure: decimal("0"),
                complete_sets: decimal("2.0"),
                tolerance: decimal("0.01"),
                is_neutral: true,
            })
            .unwrap(),
        )
        .with_trace_id(TRACE_SUCCESS.to_string()),
        build_event(
            "00000000-0000-0000-0000-000000000011",
            EventType::QuoteApproved,
            Priority::Normal,
            at_seconds(10),
            json!({"leg":"NO","reason":"secondary trace"}),
        )
        .with_trace_id(TRACE_CANCELLED.to_string()),
        build_event(
            "00000000-0000-0000-0000-000000000012",
            EventType::OrderSubmitted,
            Priority::High,
            at_seconds(11),
            serde_json::to_value(OrderSubmittedPayload {
                leg: "NO".to_string(),
                side: "BUY".to_string(),
                price: decimal("0.55"),
                size: decimal("1.5"),
                matched_size: decimal("0"),
                token_id: "token_no_fixture".to_string(),
                neg_risk: true,
                origin: Some("exchange_sync".to_string()),
                role: Some("bid_entry".to_string()),
            })
            .unwrap(),
        )
        .with_trace_id(TRACE_CANCELLED.to_string())
        .with_order_id(ORDER_CANCELLED.to_string())
        .with_asset_id("token_no_fixture".to_string()),
        build_event(
            "00000000-0000-0000-0000-000000000013",
            EventType::OrderCancelled,
            Priority::High,
            at_seconds(12),
            serde_json::to_value(OrderCancelledPayload {
                reason_code: CancelReasonCode::RiskHalt,
                reason_text: CancelReasonCode::RiskHalt.description().to_string(),
                old_size: decimal("1.5"),
                capital_delta: Some(decimal("-0.825")),
                origin: Some("risk_halt".to_string()),
                diagnostics: None,
            })
            .unwrap(),
        )
        .with_trace_id(TRACE_CANCELLED.to_string())
        .with_order_id(ORDER_CANCELLED.to_string()),
        build_monitor_degraded_event(
            "00000000-0000-0000-0000-000000000014",
            at_seconds(13),
            "projector backlog exceeded threshold",
        ),
    ]
}

fn build_monitor_degraded_event(
    event_id: &str,
    occurred_at: DateTime<Utc>,
    reason: &str,
) -> EventEnvelope {
    let mut event = EventEnvelope::new(
        EventType::MonitorDegraded,
        Priority::Critical,
        RUN_ID.to_string(),
        "bot".to_string(),
        MODE.to_string(),
        serde_json::to_value(MonitorDegradedPayload {
            component: "projector".to_string(),
            degraded_reason: reason.to_string(),
            queue_depth: Some(9),
            index_lag_ms: Some(450),
        })
        .unwrap(),
    );
    event.event_id = Uuid::parse_str(event_id).unwrap();
    event.occurred_at = occurred_at;
    event.recorded_at = occurred_at + ChronoDuration::milliseconds(50);
    event
}

fn build_event(
    event_id: &str,
    event_type: EventType,
    priority: Priority,
    occurred_at: DateTime<Utc>,
    payload: Value,
) -> EventEnvelope {
    let mut event = EventEnvelope::new(
        event_type,
        priority,
        RUN_ID.to_string(),
        "bot".to_string(),
        MODE.to_string(),
        payload,
    )
    .with_cycle_id(CYCLE_ID.to_string())
    .with_condition_id(CONDITION_ID.to_string())
    .with_market_slug(MARKET_SLUG.to_string())
    .with_question(QUESTION.to_string());
    event.event_id = Uuid::parse_str(event_id).unwrap();
    event.occurred_at = occurred_at;
    event.recorded_at = occurred_at + ChronoDuration::milliseconds(50);
    event
}

async fn get_json<T: DeserializeOwned>(client: &reqwest::Client, url: String) -> Result<T> {
    let response = client.get(url).send().await?;
    let response = response.error_for_status()?;
    Ok(response.json::<T>().await?)
}

async fn next_live_frame(
    socket: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> Result<LiveFrame> {
    let message = timeout(Duration::from_secs(5), socket.next())
        .await
        .context("timeout waiting for websocket frame")?
        .context("websocket closed unexpectedly")??;
    let text = message.into_text()?;
    Ok(serde_json::from_str::<LiveFrame>(&text)?)
}

async fn count_rows(pool: &PgPool, table: &str) -> Result<i64> {
    let row = sqlx::query(&format!("SELECT COUNT(*) AS count FROM {table}"))
        .fetch_one(pool)
        .await?;
    Ok(row.try_get::<i64, _>("count")?)
}

fn with_database_name(database_url: &str, database_name: &str) -> Result<String> {
    let mut url =
        Url::parse(database_url).with_context(|| format!("parse database url: {database_url}"))?;
    url.set_path(&format!("/{database_name}"));
    Ok(url.to_string())
}

fn at_seconds(seconds: i64) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2026-03-08T14:00:00Z")
        .unwrap()
        .with_timezone(&Utc)
        + ChronoDuration::seconds(seconds)
}

fn decimal(raw: &str) -> Decimal {
    raw.parse().unwrap()
}
