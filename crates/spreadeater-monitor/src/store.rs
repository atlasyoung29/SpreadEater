use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::Value;
use sqlx::{FromRow, PgPool, Postgres, QueryBuilder, Row};
use std::env;
use std::path::Path;
use tokio::sync::broadcast;
use uuid::Uuid;

use spreadeater_core::EventEnvelope;

use crate::dto::{
    BotErrorLogEntry, ConfigResponse, DecisionSnapshot, EventListItem, EventListResponse,
    FillSnapshot, HedgeSnapshot, LiveFrame, MarketDetailResponse, MarketReference, MarketSummary,
    NeutralitySnapshot, OrderSnapshot, OverviewResponse, PageResponse, TraceDetailResponse,
};

#[derive(Debug, Clone)]
pub struct EventFilter {
    pub trace_id: Option<String>,
    pub condition_id: Option<String>,
    pub event_type: Option<String>,
    pub before_id: Option<i64>,
    pub limit: i64,
}

impl Default for EventFilter {
    fn default() -> Self {
        Self {
            trace_id: None,
            condition_id: None,
            event_type: None,
            before_id: None,
            limit: 200,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MarketPageFilter {
    pub q: Option<String>,
    pub halted: Option<bool>,
    pub page: i64,
    pub page_size: i64,
}

impl Default for MarketPageFilter {
    fn default() -> Self {
        Self {
            q: None,
            halted: None,
            page: 1,
            page_size: 100,
        }
    }
}

#[derive(Debug, Clone)]
pub struct OpenOrdersFilter {
    pub q: Option<String>,
    pub status: Option<String>,
    pub side: Option<String>,
    pub role: Option<String>,
    pub halted: Option<bool>,
    pub page: i64,
    pub page_size: i64,
}

impl Default for OpenOrdersFilter {
    fn default() -> Self {
        Self {
            q: None,
            status: None,
            side: None,
            role: None,
            halted: None,
            page: 1,
            page_size: 100,
        }
    }
}

#[derive(Debug, Clone)]
pub struct InventoryFilter {
    pub q: Option<String>,
    pub neutrality: Option<bool>,
    pub has_open_orders: Option<bool>,
    pub halted: Option<bool>,
    pub exposure_side: Option<String>,
    pub page: i64,
    pub page_size: i64,
}

impl Default for InventoryFilter {
    fn default() -> Self {
        Self {
            q: None,
            neutrality: None,
            has_open_orders: None,
            halted: None,
            exposure_side: None,
            page: 1,
            page_size: 100,
        }
    }
}

#[derive(Debug, Clone)]
pub struct HistoryFilter {
    pub q: Option<String>,
    pub category: Option<String>,
    pub event_type: Option<String>,
    pub priority: Option<String>,
    pub run_id: Option<String>,
    pub trace_id: Option<String>,
    pub condition_id: Option<String>,
    pub page: i64,
    pub page_size: i64,
}

impl Default for HistoryFilter {
    fn default() -> Self {
        Self {
            q: None,
            category: None,
            event_type: None,
            priority: None,
            run_id: None,
            trace_id: None,
            condition_id: None,
            page: 1,
            page_size: 100,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ErrorLogFilter {
    pub q: Option<String>,
    pub level: Option<String>,
    pub window_minutes: Option<i64>,
    pub page: i64,
    pub page_size: i64,
}

impl Default for ErrorLogFilter {
    fn default() -> Self {
        Self {
            q: None,
            level: None,
            window_minutes: None,
            page: 1,
            page_size: 100,
        }
    }
}

#[derive(Clone)]
pub struct LiveBroadcaster {
    tx: broadcast::Sender<LiveFrame>,
}

const EVENT_SELECT_SQL: &str = r#"
        SELECT
            er.id,
            er.event_id,
            er.event_type,
            er.priority,
            er.occurred_at,
            er.recorded_at,
            er.run_id,
            er.cycle_id,
            er.trace_id,
            er.source_component,
            er.mode,
            er.condition_id,
            COALESCE(er.market_slug, t.market_slug, m.market_slug) AS market_slug,
            COALESCE(er.question, t.question, m.question) AS question,
            er.order_id,
            o.state AS order_state,
            o.cancel_reason AS order_cancel_reason,
            o.replacement_order_id,
            o.size AS order_size,
            o.matched_size AS order_matched_size,
            er.asset_id,
            er.hedge_id,
            er.payload,
            er.payload ->> 'reason_code' AS reason_code
        FROM events_raw er
        LEFT JOIN traces t ON t.trace_id = er.trace_id
        LEFT JOIN markets m ON m.run_id = er.run_id AND m.condition_id = er.condition_id
        LEFT JOIN orders o ON o.order_id = er.order_id
    "#;

impl LiveBroadcaster {
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        Self { tx }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<LiveFrame> {
        self.tx.subscribe()
    }

    pub fn has_subscribers(&self) -> bool {
        self.tx.receiver_count() > 0
    }

    pub fn send<T: Serialize>(&self, channel: &str, payload: &T) -> Result<()> {
        let frame = LiveFrame {
            channel: channel.to_string(),
            payload: serde_json::to_value(payload).context("serialize WS payload")?,
        };
        let _ = self.tx.send(frame);
        Ok(())
    }
}

pub async fn broadcast_event_updates(
    pool: &PgPool,
    broadcaster: &LiveBroadcaster,
    event: &EventEnvelope,
) -> Result<()> {
    if !broadcaster.has_subscribers() {
        return Ok(());
    }

    if let Some(overview) = fetch_overview(pool).await? {
        broadcaster.send("overview", &overview)?;
    }

    if let Some(condition_id) = &event.condition_id {
        if let Some(market) = fetch_market_detail(pool, condition_id, false).await? {
            broadcaster.send("market", &market)?;
        }
    }

    if let Some(trace_id) = &event.trace_id {
        if let Some(trace) = fetch_trace_detail(pool, trace_id).await? {
            broadcaster.send("trace", &trace)?;
        }
    }

    if is_alert_event(&event.event_type.to_string(), &event.priority.to_string()) {
        if let Some(alert) = fetch_event_by_uuid(pool, event.event_id).await? {
            broadcaster.send("alerts", &alert)?;
        }
    }

    Ok(())
}

pub async fn fetch_overview(pool: &PgPool) -> Result<Option<OverviewResponse>> {
    let run = fetch_latest_run(pool).await?;

    let Some(run) = run else {
        return Ok(None);
    };

    let config = load_bot_config_summary();
    let markets = fetch_market_summaries_for_run(pool, &run.run_id).await?;

    let active_markets =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM markets WHERE run_id = $1")
            .bind(&run.run_id)
            .fetch_one(pool)
            .await?;

    let open_orders = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM orders WHERE run_id = $1 AND state IN ('open', 'partially_filled', 'submitted')",
    )
    .bind(&run.run_id)
    .fetch_one(pool)
    .await?;

    let committed_capital_usd = sqlx::query_scalar::<_, Decimal>(
        r#"
        SELECT COALESCE(
            SUM(
                CASE
                    WHEN side = 'BUY' AND state IN ('open', 'partially_filled', 'submitted')
                    THEN price * GREATEST(size - matched_size, 0)
                    ELSE 0
                END
            ),
            0
        )
        FROM orders
        WHERE run_id = $1
        "#,
    )
    .bind(&run.run_id)
    .fetch_one(pool)
    .await?;

    let unhedged_markets = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM markets WHERE run_id = $1 AND is_neutral = false AND open_order_notional_usd > 0",
    )
    .bind(&run.run_id)
    .fetch_one(pool)
    .await?;

    let mut open_order_preview: Vec<MarketSummary> = markets
        .iter()
        .filter(|market| market.open_order_count > 0)
        .cloned()
        .collect();
    open_order_preview.sort_by(|left, right| {
        right
            .open_order_notional_usd
            .cmp(&left.open_order_notional_usd)
            .then(right.open_order_count.cmp(&left.open_order_count))
            .then_with(|| right.last_event_at.cmp(&left.last_event_at))
    });
    let open_order_markets = i64::try_from(open_order_preview.len()).unwrap_or(i64::MAX);
    let decision_reward_sum = sum_decimal_options(
        open_order_preview
            .iter()
            .map(|market| market.expected_reward_usd_day),
    );
    let open_order_reward_usd_day = run.total_est_daily_usd.unwrap_or(decision_reward_sum);
    let open_order_notional_usd = sum_decimals(
        open_order_preview
            .iter()
            .map(|market| market.open_order_notional_usd),
    );
    open_order_preview.truncate(8);

    let mut inventory_preview: Vec<MarketSummary> = markets
        .iter()
        .filter(|market| market.yes_size != Decimal::ZERO || market.no_size != Decimal::ZERO)
        .cloned()
        .collect();
    inventory_preview.sort_by(|left, right| {
        right
            .net_exposure
            .abs()
            .cmp(&left.net_exposure.abs())
            .then_with(|| right.last_event_at.cmp(&left.last_event_at))
    });
    let inventory_markets = i64::try_from(inventory_preview.len()).unwrap_or(i64::MAX);
    inventory_preview.truncate(8);

    let recent_history = fetch_recent_history_preview(pool, &run.run_id, 12).await?;
    let recent_errors = fetch_error_logs(
        pool,
        ErrorLogFilter {
            page: 1,
            page_size: 10,
            ..Default::default()
        },
    )
    .await?
    .items;
    let recent_alerts = fetch_recent_alerts(pool, &run.run_id, 5).await?;

    Ok(Some(OverviewResponse {
        run_id: run.run_id,
        mode: run.mode,
        observer_health: run.observer_health,
        global_halt: run.global_halt,
        risk_reason: run.risk_reason,
        user_stream_status: run.user_stream_status,
        user_stream_detail: run.user_stream_detail,
        subscribed_markets: run.subscribed_markets,
        managed_markets: run.managed_markets,
        producer_lag_ms: run.producer_lag_ms,
        index_lag_ms: run.index_lag_ms,
        last_event_at: run.last_event_at,
        expected_cycle_interval_secs: config.discovery_poll_interval_secs,
        active_markets: run.managed_markets.unwrap_or(active_markets),
        open_orders,
        committed_capital_usd: run.total_committed_usd.unwrap_or(committed_capital_usd),
        order_committed_usd: run.order_committed_usd,
        position_committed_usd: run.position_committed_usd,
        total_committed_usd: run.total_committed_usd,
        api_balance_usd: run.api_balance_usd,
        available_budget_usd: run.available_budget_usd,
        competition_multiplier: run.competition_multiplier,
        max_total_exposure_usd: run.max_total_exposure_usd.or(config.max_total_exposure_usd),
        unhedged_markets,
        open_order_markets,
        inventory_markets,
        open_order_reward_usd_day,
        open_order_notional_usd,
        open_order_preview,
        inventory_preview,
        recent_history,
        recent_errors,
        recent_alerts,
    }))
}

pub async fn fetch_market_detail(
    pool: &PgPool,
    condition_id: &str,
    include_timeline: bool,
) -> Result<Option<MarketDetailResponse>> {
    let market = sqlx::query_as::<_, MarketDetailRow>(
        r#"
        SELECT
            run_id,
            condition_id,
            market_slug,
            question,
            decision_status,
            expected_reward_usd_day,
            expected_hedge_cost_usd,
            expected_edge_usd,
            expected_edge_pct,
            committed_capital_usd,
            effective_quote_size,
            score_share,
            max_hedgeable_size,
            decision_payload -> 'reasons' ->> 0 AS latest_reason,
            halted,
            halt_reason,
            COALESCE((
                SELECT COUNT(*)
                FROM orders
                WHERE run_id = markets.run_id
                  AND condition_id = markets.condition_id
                  AND state IN ('open', 'partially_filled', 'submitted')
            ), 0) AS open_order_count,
            COALESCE((
                SELECT SUM(GREATEST(COALESCE(size, 0) - matched_size, 0))
                FROM orders
                WHERE run_id = markets.run_id
                  AND condition_id = markets.condition_id
                  AND state IN ('open', 'partially_filled', 'submitted')
            ), 0) AS open_order_share_size,
            open_order_notional_usd,
            yes_size,
            no_size,
            net_exposure,
            complete_sets,
            is_neutral
        FROM markets
        WHERE condition_id = $1
        ORDER BY last_event_at DESC
        LIMIT 1
        "#,
    )
    .bind(condition_id)
    .fetch_optional(pool)
    .await?;

    let Some(market) = market else {
        return Ok(None);
    };

    let recent_traces = sqlx::query(
        r#"
        SELECT trace_id
        FROM (
            SELECT trace_id, MAX(id) AS last_seen
            FROM events_raw
            WHERE run_id = $1
              AND condition_id = $2
              AND trace_id IS NOT NULL
            GROUP BY trace_id
        ) traces
        ORDER BY last_seen DESC
        LIMIT 12
        "#,
    )
    .bind(&market.run_id)
    .bind(&market.condition_id)
    .fetch_all(pool)
    .await?
    .into_iter()
    .filter_map(|row| row.try_get::<Option<String>, _>("trace_id").ok().flatten())
    .collect();

    let recent_events = if include_timeline {
        fetch_events(
            pool,
            EventFilter {
                condition_id: Some(market.condition_id.clone()),
                limit: 50,
                ..Default::default()
            },
        )
        .await?
        .items
    } else {
        Vec::new()
    };

    Ok(Some(MarketDetailResponse {
        condition_id: market.condition_id,
        run_id: market.run_id,
        market_slug: market.market_slug,
        question: market.question,
        decision_status: market.decision_status,
        expected_edge_usd: market.expected_edge_usd,
        expected_edge_pct: market.expected_edge_pct,
        expected_reward_usd_day: market.expected_reward_usd_day,
        expected_hedge_cost_usd: market.expected_hedge_cost_usd,
        committed_capital_usd: market.committed_capital_usd,
        effective_quote_size: market.effective_quote_size,
        score_share: market.score_share,
        max_hedgeable_size: market.max_hedgeable_size,
        latest_reason: market.latest_reason,
        halted: market.halted,
        halt_reason: market.halt_reason,
        open_order_count: market.open_order_count,
        open_order_share_size: market.open_order_share_size,
        open_order_notional_usd: market.open_order_notional_usd,
        yes_size: market.yes_size,
        no_size: market.no_size,
        net_exposure: market.net_exposure,
        complete_sets: market.complete_sets,
        is_neutral: market.is_neutral,
        recent_traces,
        recent_events,
    }))
}

pub async fn fetch_trace_detail(
    pool: &PgPool,
    trace_id: &str,
) -> Result<Option<TraceDetailResponse>> {
    let trace = sqlx::query_as::<_, TraceRow>(
        r#"
        SELECT
            trace_id,
            run_id,
            condition_id,
            market_slug,
            question,
            status,
            decision_payload
        FROM traces
        WHERE trace_id = $1
        "#,
    )
    .bind(trace_id)
    .fetch_optional(pool)
    .await?;

    let Some(trace) = trace else {
        return Ok(None);
    };

    let orders = sqlx::query_as::<_, OrderSnapshot>(
        r#"
        SELECT
            order_id,
            trace_id,
            leg,
            side,
            price,
            size,
            matched_size,
            state,
            origin,
            role,
            cancel_reason,
            replacement_order_id,
            committed_capital_delta_usd,
            token_id,
            neg_risk,
            created_at,
            updated_at
        FROM orders
        WHERE trace_id = $1
        ORDER BY created_at ASC
        "#,
    )
    .bind(trace_id)
    .fetch_all(pool)
    .await?;

    let fills = sqlx::query_as::<_, FillSnapshot>(
        r#"
        SELECT
            fill_id,
            trace_id,
            order_id,
            price,
            size,
            side,
            outcome,
            match_source,
            fallback_match,
            occurred_at
        FROM fills
        WHERE trace_id = $1
        ORDER BY occurred_at ASC
        "#,
    )
    .bind(trace_id)
    .fetch_all(pool)
    .await?;

    let hedges = sqlx::query_as::<_, HedgeSnapshot>(
        r#"
        SELECT
            hedge_id,
            trace_id,
            trigger_order_id,
            trigger_leg,
            fill_size,
            fill_price,
            hedge_token_id,
            hedge_side,
            hedge_order_id,
            result_status,
            hedge_price,
            failure_reason,
            latency_ms,
            origin,
            created_at,
            updated_at
        FROM hedges
        WHERE trace_id = $1
        ORDER BY created_at ASC
        "#,
    )
    .bind(trace_id)
    .fetch_all(pool)
    .await?;

    let neutrality = sqlx::query_as::<_, NeutralitySnapshot>(
        r#"
        SELECT
            trace_id,
            pre_yes_size,
            pre_no_size,
            post_yes_size,
            post_no_size,
            residual_exposure,
            complete_sets,
            tolerance,
            is_neutral,
            occurred_at
        FROM neutrality_evaluations
        WHERE trace_id = $1
        ORDER BY occurred_at DESC
        LIMIT 1
        "#,
    )
    .bind(trace_id)
    .fetch_optional(pool)
    .await?;

    let timeline = fetch_events(
        pool,
        EventFilter {
            trace_id: Some(trace_id.to_string()),
            limit: 200,
            ..Default::default()
        },
    )
    .await?
    .items;

    Ok(Some(TraceDetailResponse {
        trace_id: trace.trace_id,
        run_id: trace.run_id,
        status: trace.status,
        market: MarketReference {
            condition_id: trace.condition_id,
            market_slug: trace.market_slug,
            question: trace.question,
        },
        decision: trace
            .decision_payload
            .as_ref()
            .map(decision_snapshot_from_payload),
        orders,
        fills,
        hedges,
        neutrality,
        timeline,
    }))
}

pub async fn fetch_open_orders(
    pool: &PgPool,
    filter: OpenOrdersFilter,
) -> Result<PageResponse<MarketSummary>> {
    let Some(run) = fetch_latest_run(pool).await? else {
        return Ok(empty_page(filter.page, filter.page_size));
    };

    let facets = fetch_open_order_facets(pool, &run.run_id).await?;
    let normalized_status = match filter.status.as_deref() {
        Some(value) => Some(normalize_open_order_status_filter(value)?),
        None => None,
    };
    let normalized_side = match filter.side.as_deref() {
        Some(value) => Some(normalize_open_order_side_filter(value)?),
        None => None,
    };
    let normalized_role = match filter.role.as_deref() {
        Some(value) => Some(normalize_open_order_role_filter(value)?),
        None => None,
    };
    let mut items: Vec<MarketSummary> = fetch_market_summaries_for_run(pool, &run.run_id)
        .await?
        .into_iter()
        .filter(|market| market.open_order_count > 0)
        .filter(|market| matches_market_search(market, filter.q.as_deref()))
        .filter(|market| matches_optional_bool(market.halted, filter.halted))
        .filter(|market| {
            matches_open_order_filter(
                market,
                &facets,
                normalized_status,
                normalized_side,
                normalized_role,
            )
        })
        .collect();

    items.sort_by(|left, right| {
        right
            .open_order_notional_usd
            .cmp(&left.open_order_notional_usd)
            .then(right.open_order_count.cmp(&left.open_order_count))
            .then_with(|| right.last_event_at.cmp(&left.last_event_at))
    });

    Ok(page_slice(items, filter.page, filter.page_size))
}

pub async fn fetch_inventory(
    pool: &PgPool,
    filter: InventoryFilter,
) -> Result<PageResponse<MarketSummary>> {
    let Some(run) = fetch_latest_run(pool).await? else {
        return Ok(empty_page(filter.page, filter.page_size));
    };

    let exposure_side = normalize_inventory_exposure_filter(filter.exposure_side.as_deref())?;
    let mut items: Vec<MarketSummary> = fetch_market_summaries_for_run(pool, &run.run_id)
        .await?
        .into_iter()
        .filter(|market| market.yes_size != Decimal::ZERO || market.no_size != Decimal::ZERO)
        .filter(|market| matches_market_search(market, filter.q.as_deref()))
        .filter(|market| matches_optional_bool(market.halted, filter.halted))
        .filter(|market| matches_optional_bool(market.is_neutral, filter.neutrality))
        .filter(|market| matches_optional_bool(market.open_order_count > 0, filter.has_open_orders))
        .filter(|market| matches_inventory_exposure_side(market, exposure_side.as_deref()))
        .collect();

    items.sort_by(|left, right| {
        right
            .net_exposure
            .abs()
            .cmp(&left.net_exposure.abs())
            .then_with(|| right.last_event_at.cmp(&left.last_event_at))
    });

    Ok(page_slice(items, filter.page, filter.page_size))
}

pub async fn fetch_watchlist(
    pool: &PgPool,
    filter: MarketPageFilter,
) -> Result<PageResponse<MarketSummary>> {
    let Some(run) = fetch_latest_run(pool).await? else {
        return Ok(empty_page(filter.page, filter.page_size));
    };

    let mut items: Vec<MarketSummary> = fetch_market_summaries_for_run(pool, &run.run_id)
        .await?
        .into_iter()
        .filter(|market| matches_market_search(market, filter.q.as_deref()))
        .filter(|market| matches_optional_bool(market.halted, filter.halted))
        .collect();

    items.sort_by(|left, right| {
        right
            .expected_reward_usd_day
            .unwrap_or(Decimal::ZERO)
            .cmp(&left.expected_reward_usd_day.unwrap_or(Decimal::ZERO))
            .then_with(|| right.last_event_at.cmp(&left.last_event_at))
    });

    Ok(page_slice(items, filter.page, filter.page_size))
}

pub async fn fetch_history(
    pool: &PgPool,
    filter: HistoryFilter,
) -> Result<PageResponse<EventListItem>> {
    let effective_run_id = match filter.run_id.clone() {
        Some(run_id) => Some(run_id),
        None => fetch_latest_run(pool).await?.map(|run| run.run_id),
    };
    let normalized_event_type = match filter.event_type.as_deref() {
        Some(value) => Some(
            normalize_event_type(value)
                .with_context(|| format!("invalid event_type filter: {value}"))?,
        ),
        None => None,
    };
    let normalized_priority = match filter.priority.as_deref() {
        Some(value) => Some(normalize_priority_filter(value)?),
        None => None,
    };
    let category_types = match filter.category.as_deref() {
        Some(value) => Some(normalize_history_category(value)?),
        None => None,
    };
    let (page, page_size, offset) = normalized_page(filter.page, filter.page_size);

    let mut count_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
        "SELECT COUNT(*) FROM events_raw er \
         LEFT JOIN traces t ON t.trace_id = er.trace_id \
         LEFT JOIN markets m ON m.run_id = er.run_id AND m.condition_id = er.condition_id \
         LEFT JOIN orders o ON o.order_id = er.order_id",
    );
    push_history_filters(
        &mut count_builder,
        effective_run_id.as_deref(),
        filter.trace_id.as_deref(),
        filter.condition_id.as_deref(),
        normalized_event_type.as_deref(),
        normalized_priority.as_deref(),
        category_types.as_deref(),
        filter.q.as_deref(),
    );
    let total = count_builder
        .build_query_scalar::<i64>()
        .fetch_one(pool)
        .await?;

    let mut select_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(EVENT_SELECT_SQL);
    push_history_filters(
        &mut select_builder,
        effective_run_id.as_deref(),
        filter.trace_id.as_deref(),
        filter.condition_id.as_deref(),
        normalized_event_type.as_deref(),
        normalized_priority.as_deref(),
        category_types.as_deref(),
        filter.q.as_deref(),
    );
    select_builder.push(" ORDER BY er.id DESC LIMIT ");
    select_builder.push_bind(page_size);
    select_builder.push(" OFFSET ");
    select_builder.push_bind(offset);

    let items = select_builder
        .build_query_as::<EventRow>()
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(Into::into)
        .collect();

    Ok(PageResponse {
        items,
        total,
        page,
        page_size,
    })
}

pub async fn fetch_error_logs(
    pool: &PgPool,
    filter: ErrorLogFilter,
) -> Result<PageResponse<BotErrorLogEntry>> {
    let normalized_level = match filter.level.as_deref() {
        Some(value) => Some(normalize_error_level_filter(value)?),
        None => None,
    };
    let (page, page_size, offset) = normalized_page(filter.page, filter.page_size);

    let mut count_builder: QueryBuilder<'_, Postgres> =
        QueryBuilder::new("SELECT COUNT(*) FROM bot_error_logs bel");
    push_error_filters(
        &mut count_builder,
        normalized_level.as_deref(),
        filter.window_minutes,
        filter.q.as_deref(),
    );
    let total = count_builder
        .build_query_scalar::<i64>()
        .fetch_one(pool)
        .await?;

    let mut select_builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
        "SELECT id, log_path, byte_offset, parsed_at, level, message, raw_line, created_at \
         FROM bot_error_logs bel",
    );
    push_error_filters(
        &mut select_builder,
        normalized_level.as_deref(),
        filter.window_minutes,
        filter.q.as_deref(),
    );
    select_builder.push(" ORDER BY COALESCE(parsed_at, created_at) DESC, id DESC LIMIT ");
    select_builder.push_bind(page_size);
    select_builder.push(" OFFSET ");
    select_builder.push_bind(offset);

    let items = select_builder
        .build_query_as::<BotErrorLogRow>()
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(Into::into)
        .collect();

    Ok(PageResponse {
        items,
        total,
        page,
        page_size,
    })
}

pub fn fetch_config_document(path: &Path) -> Result<ConfigResponse> {
    let canonical = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let metadata = std::fs::metadata(&canonical)
        .with_context(|| format!("read config metadata at {}", canonical.display()))?;
    let modified = metadata
        .modified()
        .context("read config modified timestamp")?;
    let contents = std::fs::read_to_string(&canonical)
        .with_context(|| format!("read config at {}", canonical.display()))?;
    let value = parse_jsonc_value(&contents)?;

    Ok(ConfigResponse {
        path: canonical.to_string_lossy().to_string(),
        last_modified_at: DateTime::<Utc>::from(modified),
        value,
    })
}

pub async fn fetch_events(pool: &PgPool, filter: EventFilter) -> Result<EventListResponse> {
    let normalized_event_type = match &filter.event_type {
        Some(event_type) => Some(
            normalize_event_type(event_type)
                .with_context(|| format!("invalid event_type filter: {event_type}"))?,
        ),
        None => None,
    };

    let limit = filter.limit.clamp(1, 500);
    let select = EVENT_SELECT_SQL;

    let rows = match (
        filter.trace_id.as_deref(),
        filter.condition_id.as_deref(),
        normalized_event_type.as_deref(),
        filter.before_id,
    ) {
        (Some(trace_id), Some(condition_id), Some(event_type), Some(before_id)) => {
            let sql = format!("{select} WHERE er.trace_id = $1 AND er.condition_id = $2 AND er.event_type = $3 AND er.id < $4 ORDER BY er.id DESC LIMIT $5");
            sqlx::query_as::<_, EventRow>(&sql)
                .bind(trace_id)
                .bind(condition_id)
                .bind(event_type)
                .bind(before_id)
                .bind(limit + 1)
                .fetch_all(pool)
                .await?
        }
        (Some(trace_id), Some(condition_id), Some(event_type), None) => {
            let sql = format!("{select} WHERE er.trace_id = $1 AND er.condition_id = $2 AND er.event_type = $3 ORDER BY er.id DESC LIMIT $4");
            sqlx::query_as::<_, EventRow>(&sql)
                .bind(trace_id)
                .bind(condition_id)
                .bind(event_type)
                .bind(limit + 1)
                .fetch_all(pool)
                .await?
        }
        (Some(trace_id), Some(condition_id), None, Some(before_id)) => {
            let sql = format!("{select} WHERE er.trace_id = $1 AND er.condition_id = $2 AND er.id < $3 ORDER BY er.id DESC LIMIT $4");
            sqlx::query_as::<_, EventRow>(&sql)
                .bind(trace_id)
                .bind(condition_id)
                .bind(before_id)
                .bind(limit + 1)
                .fetch_all(pool)
                .await?
        }
        (Some(trace_id), Some(condition_id), None, None) => {
            let sql = format!(
                "{select} WHERE er.trace_id = $1 AND er.condition_id = $2 ORDER BY er.id DESC LIMIT $3"
            );
            sqlx::query_as::<_, EventRow>(&sql)
                .bind(trace_id)
                .bind(condition_id)
                .bind(limit + 1)
                .fetch_all(pool)
                .await?
        }
        (Some(trace_id), None, Some(event_type), Some(before_id)) => {
            let sql = format!("{select} WHERE er.trace_id = $1 AND er.event_type = $2 AND er.id < $3 ORDER BY er.id DESC LIMIT $4");
            sqlx::query_as::<_, EventRow>(&sql)
                .bind(trace_id)
                .bind(event_type)
                .bind(before_id)
                .bind(limit + 1)
                .fetch_all(pool)
                .await?
        }
        (Some(trace_id), None, Some(event_type), None) => {
            let sql = format!(
                "{select} WHERE er.trace_id = $1 AND er.event_type = $2 ORDER BY er.id DESC LIMIT $3"
            );
            sqlx::query_as::<_, EventRow>(&sql)
                .bind(trace_id)
                .bind(event_type)
                .bind(limit + 1)
                .fetch_all(pool)
                .await?
        }
        (Some(trace_id), None, None, Some(before_id)) => {
            let sql = format!(
                "{select} WHERE er.trace_id = $1 AND er.id < $2 ORDER BY er.id DESC LIMIT $3"
            );
            sqlx::query_as::<_, EventRow>(&sql)
                .bind(trace_id)
                .bind(before_id)
                .bind(limit + 1)
                .fetch_all(pool)
                .await?
        }
        (Some(trace_id), None, None, None) => {
            let sql = format!("{select} WHERE er.trace_id = $1 ORDER BY er.id DESC LIMIT $2");
            sqlx::query_as::<_, EventRow>(&sql)
                .bind(trace_id)
                .bind(limit + 1)
                .fetch_all(pool)
                .await?
        }
        (None, Some(condition_id), Some(event_type), Some(before_id)) => {
            let sql = format!("{select} WHERE er.condition_id = $1 AND er.event_type = $2 AND er.id < $3 ORDER BY er.id DESC LIMIT $4");
            sqlx::query_as::<_, EventRow>(&sql)
                .bind(condition_id)
                .bind(event_type)
                .bind(before_id)
                .bind(limit + 1)
                .fetch_all(pool)
                .await?
        }
        (None, Some(condition_id), Some(event_type), None) => {
            let sql = format!(
                "{select} WHERE er.condition_id = $1 AND er.event_type = $2 ORDER BY er.id DESC LIMIT $3"
            );
            sqlx::query_as::<_, EventRow>(&sql)
                .bind(condition_id)
                .bind(event_type)
                .bind(limit + 1)
                .fetch_all(pool)
                .await?
        }
        (None, Some(condition_id), None, Some(before_id)) => {
            let sql = format!(
                "{select} WHERE er.condition_id = $1 AND er.id < $2 ORDER BY er.id DESC LIMIT $3"
            );
            sqlx::query_as::<_, EventRow>(&sql)
                .bind(condition_id)
                .bind(before_id)
                .bind(limit + 1)
                .fetch_all(pool)
                .await?
        }
        (None, Some(condition_id), None, None) => {
            let sql = format!("{select} WHERE er.condition_id = $1 ORDER BY er.id DESC LIMIT $2");
            sqlx::query_as::<_, EventRow>(&sql)
                .bind(condition_id)
                .bind(limit + 1)
                .fetch_all(pool)
                .await?
        }
        (None, None, Some(event_type), Some(before_id)) => {
            let sql = format!(
                "{select} WHERE er.event_type = $1 AND er.id < $2 ORDER BY er.id DESC LIMIT $3"
            );
            sqlx::query_as::<_, EventRow>(&sql)
                .bind(event_type)
                .bind(before_id)
                .bind(limit + 1)
                .fetch_all(pool)
                .await?
        }
        (None, None, Some(event_type), None) => {
            let sql = format!("{select} WHERE er.event_type = $1 ORDER BY er.id DESC LIMIT $2");
            sqlx::query_as::<_, EventRow>(&sql)
                .bind(event_type)
                .bind(limit + 1)
                .fetch_all(pool)
                .await?
        }
        (None, None, None, Some(before_id)) => {
            let sql = format!("{select} WHERE er.id < $1 ORDER BY er.id DESC LIMIT $2");
            sqlx::query_as::<_, EventRow>(&sql)
                .bind(before_id)
                .bind(limit + 1)
                .fetch_all(pool)
                .await?
        }
        (None, None, None, None) => {
            let sql = format!("{select} ORDER BY er.id DESC LIMIT $1");
            sqlx::query_as::<_, EventRow>(&sql)
                .bind(limit + 1)
                .fetch_all(pool)
                .await?
        }
    };

    let has_more = rows.len() as i64 > limit;
    let items: Vec<EventListItem> = rows
        .into_iter()
        .take(limit as usize)
        .map(Into::into)
        .collect();
    let next_cursor = if has_more {
        items.last().map(|item| item.id)
    } else {
        None
    };

    Ok(EventListResponse { items, next_cursor })
}

pub async fn fetch_event_by_uuid(pool: &PgPool, event_id: Uuid) -> Result<Option<EventListItem>> {
    let sql = format!("{EVENT_SELECT_SQL} WHERE er.event_id = $1");
    let row = sqlx::query_as::<_, EventRow>(&sql)
        .bind(event_id)
        .fetch_optional(pool)
        .await?;

    Ok(row.map(Into::into))
}

fn decision_snapshot_from_payload(payload: &Value) -> DecisionSnapshot {
    DecisionSnapshot {
        payload: payload.clone(),
        would_trade: payload.get("would_trade").and_then(Value::as_bool),
        reasons: payload
            .get("reasons")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(ToString::to_string)
            .collect(),
        expected_edge_usd: decimal_field(payload, "expected_edge_usd"),
        expected_edge_pct: decimal_field(payload, "expected_edge_pct"),
        expected_reward_usd_day: decimal_field(payload, "expected_reward_usd_day"),
        expected_hedge_cost_usd: decimal_field(payload, "expected_hedge_cost_usd"),
        committed_capital_usd: decimal_field(payload, "committed_capital_usd"),
        effective_quote_size: decimal_field(payload, "effective_quote_size"),
        score_share: decimal_field(payload, "score_share"),
        max_hedgeable_size: decimal_field(payload, "max_hedgeable_size"),
        competition_multiplier_used: decimal_field(payload, "competition_multiplier_used"),
        api_balance_usd: decimal_field(payload, "api_balance_usd"),
        available_budget_usd: decimal_field(payload, "available_budget_usd"),
        rank_in_cycle: integer_field(payload, "rank_in_cycle"),
        ranked_market_count: integer_field(payload, "ranked_market_count"),
        ranking_metric_name: payload
            .get("ranking_metric_name")
            .and_then(Value::as_str)
            .map(ToString::to_string),
        ranking_metric_value: decimal_field(payload, "ranking_metric_value"),
    }
}

fn decimal_field(payload: &Value, key: &str) -> Option<Decimal> {
    payload
        .get(key)
        .and_then(Value::as_str)
        .and_then(|raw| raw.parse::<Decimal>().ok())
}

fn integer_field(payload: &Value, key: &str) -> Option<u64> {
    payload
        .get(key)
        .and_then(|value| value.as_u64().or_else(|| value.as_str()?.parse().ok()))
}

fn is_alert_event(event_type: &str, priority: &str) -> bool {
    let normalized_type = event_type.to_ascii_lowercase();
    let normalized_priority = priority.to_ascii_lowercase();
    matches!(
        normalized_type.as_str(),
        "order_submitted" | "fill_detected"
    ) || matches!(normalized_priority.as_str(), "critical" | "high")
        && matches!(
            normalized_type.as_str(),
            "monitor_degraded" | "projection_rebuilt"
        )
}

async fn fetch_recent_alerts(
    pool: &PgPool,
    run_id: &str,
    limit: i64,
) -> Result<Vec<EventListItem>> {
    let sql = format!(
        "{EVENT_SELECT_SQL} WHERE er.run_id = $1 AND er.event_type IN ('order_submitted', 'fill_detected') ORDER BY er.id DESC LIMIT $2"
    );
    let rows = sqlx::query_as::<_, EventRow>(&sql)
        .bind(run_id)
        .bind(limit)
        .fetch_all(pool)
        .await?;

    Ok(rows.into_iter().map(Into::into).collect())
}

async fn fetch_latest_run(pool: &PgPool) -> Result<Option<RunRow>> {
    sqlx::query_as::<_, RunRow>(
        r#"
        SELECT
            run_id,
            mode,
            observer_health,
            global_halt,
            risk_reason,
            user_stream_status,
            user_stream_detail,
            subscribed_markets,
            managed_markets,
            order_committed_usd,
            position_committed_usd,
            total_committed_usd,
            max_total_exposure_usd,
            api_balance_usd,
            available_budget_usd,
            competition_multiplier,
            total_est_daily_usd,
            producer_lag_ms,
            index_lag_ms,
            last_event_at
        FROM runs
        ORDER BY last_event_at DESC
        LIMIT 1
        "#,
    )
    .fetch_optional(pool)
    .await
    .map_err(Into::into)
}

async fn fetch_market_summaries_for_run(pool: &PgPool, run_id: &str) -> Result<Vec<MarketSummary>> {
    let items = sqlx::query_as::<_, MarketSummaryRow>(
        r#"
        SELECT
            condition_id,
            market_slug,
            question,
            decision_status,
            expected_reward_usd_day,
            expected_edge_usd,
            expected_edge_pct,
            decision_payload -> 'reasons' ->> 0 AS latest_reason,
            halted,
            halt_reason,
            COALESCE((
                SELECT COUNT(*)
                FROM orders
                WHERE run_id = $1
                  AND condition_id = markets.condition_id
                  AND state IN ('open', 'partially_filled', 'submitted')
            ), 0) AS open_order_count,
            COALESCE((
                SELECT SUM(GREATEST(COALESCE(size, 0) - matched_size, 0))
                FROM orders
                WHERE run_id = $1
                  AND condition_id = markets.condition_id
                  AND state IN ('open', 'partially_filled', 'submitted')
            ), 0) AS open_order_share_size,
            open_order_notional_usd,
            yes_size,
            no_size,
            net_exposure,
            complete_sets,
            is_neutral,
            last_event_at
        FROM markets
        WHERE run_id = $1
        ORDER BY last_event_at DESC
        "#,
    )
    .bind(run_id)
    .fetch_all(pool)
    .await?;

    Ok(items.into_iter().map(Into::into).collect())
}

async fn fetch_open_order_facets(
    pool: &PgPool,
    run_id: &str,
) -> Result<std::collections::HashMap<String, OpenOrderFacetRow>> {
    let rows = sqlx::query_as::<_, OpenOrderFacetRow>(
        r#"
        SELECT
            condition_id,
            COUNT(*) FILTER (WHERE state = 'submitted') AS submitted_count,
            COUNT(*) FILTER (WHERE state = 'open') AS open_count,
            COUNT(*) FILTER (WHERE state = 'partially_filled') AS partial_count,
            BOOL_OR(side = 'BUY') AS has_buy_orders,
            BOOL_OR(side = 'SELL') AS has_sell_orders,
            BOOL_OR(role = 'bid_entry') AS has_bid_entry_orders,
            BOOL_OR(role = 'ask_inventory') AS has_ask_inventory_orders
        FROM orders
        WHERE run_id = $1
          AND state IN ('open', 'partially_filled', 'submitted')
        GROUP BY condition_id
        "#,
    )
    .bind(run_id)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| (row.condition_id.clone(), row))
        .collect())
}

async fn fetch_recent_history_preview(
    pool: &PgPool,
    run_id: &str,
    limit: i64,
) -> Result<Vec<EventListItem>> {
    let sql = format!(
        "{EVENT_SELECT_SQL} WHERE er.run_id = $1 \
         AND er.event_type IN (
             'order_submitted',
             'order_cancelled',
             'order_resized',
             'fill_detected',
             'hedge_intent_created',
             'hedge_decision_evaluated',
             'hedge_result_recorded',
             'hedge_exit_path_recorded',
             'decision_evaluated',
             'quote_rejected',
             'monitor_degraded',
             'risk_state_changed'
         ) \
         ORDER BY er.id DESC LIMIT $2"
    );
    let rows = sqlx::query_as::<_, EventRow>(&sql)
        .bind(run_id)
        .bind(limit)
        .fetch_all(pool)
        .await?;

    Ok(rows.into_iter().map(Into::into).collect())
}

fn push_history_filters(
    builder: &mut QueryBuilder<'_, Postgres>,
    run_id: Option<&str>,
    trace_id: Option<&str>,
    condition_id: Option<&str>,
    event_type: Option<&str>,
    priority: Option<&str>,
    category_types: Option<&[&'static str]>,
    q: Option<&str>,
) {
    builder.push(" WHERE 1=1");

    if let Some(run_id) = run_id {
        builder.push(" AND er.run_id = ");
        builder.push_bind(run_id.to_string());
    }
    if let Some(trace_id) = trace_id {
        builder.push(" AND er.trace_id = ");
        builder.push_bind(trace_id.to_string());
    }
    if let Some(condition_id) = condition_id {
        builder.push(" AND er.condition_id = ");
        builder.push_bind(condition_id.to_string());
    }
    if let Some(event_type) = event_type {
        builder.push(" AND er.event_type = ");
        builder.push_bind(event_type.to_string());
    }
    if let Some(priority) = priority {
        builder.push(" AND er.priority = ");
        builder.push_bind(priority.to_string());
    }
    if let Some(category_types) = category_types {
        builder.push(" AND er.event_type IN (");
        let mut separated = builder.separated(", ");
        for value in category_types {
            separated.push_bind(*value);
        }
        separated.push_unseparated(")");
    }
    if let Some(q) = q.filter(|value| !value.trim().is_empty()) {
        let pattern = format!("%{}%", q.trim());
        builder.push(" AND (");
        builder.push("COALESCE(er.question, t.question, m.question, '') ILIKE ");
        builder.push_bind(pattern.clone());
        builder.push(" OR COALESCE(er.market_slug, t.market_slug, m.market_slug, '') ILIKE ");
        builder.push_bind(pattern.clone());
        builder.push(" OR COALESCE(er.condition_id, '') ILIKE ");
        builder.push_bind(pattern.clone());
        builder.push(" OR COALESCE(er.order_id, '') ILIKE ");
        builder.push_bind(pattern.clone());
        builder.push(" OR COALESCE(er.trace_id, '') ILIKE ");
        builder.push_bind(pattern.clone());
        builder.push(" OR COALESCE(o.cancel_reason, er.payload ->> 'reason_code', '') ILIKE ");
        builder.push_bind(pattern.clone());
        builder.push(" OR CAST(er.payload AS TEXT) ILIKE ");
        builder.push_bind(pattern);
        builder.push(")");
    }
}

fn push_error_filters(
    builder: &mut QueryBuilder<'_, Postgres>,
    level: Option<&str>,
    window_minutes: Option<i64>,
    q: Option<&str>,
) {
    builder.push(" WHERE 1=1");

    if let Some(level) = level {
        builder.push(" AND COALESCE(level, 'unknown') = ");
        builder.push_bind(level.to_string());
    }
    if let Some(window_minutes) = window_minutes.filter(|value| *value > 0) {
        builder.push(" AND COALESCE(parsed_at, created_at) >= NOW() - (");
        builder.push_bind(window_minutes);
        builder.push(" * INTERVAL '1 minute')");
    }
    if let Some(q) = q.filter(|value| !value.trim().is_empty()) {
        let pattern = format!("%{}%", q.trim());
        builder.push(" AND (message ILIKE ");
        builder.push_bind(pattern.clone());
        builder.push(" OR raw_line ILIKE ");
        builder.push_bind(pattern);
        builder.push(")");
    }
}

fn matches_market_search(market: &MarketSummary, q: Option<&str>) -> bool {
    let Some(q) = q.filter(|value| !value.trim().is_empty()) else {
        return true;
    };
    let needle = q.trim().to_ascii_lowercase();
    market
        .question
        .as_deref()
        .unwrap_or_default()
        .to_ascii_lowercase()
        .contains(&needle)
        || market
            .market_slug
            .as_deref()
            .unwrap_or_default()
            .to_ascii_lowercase()
            .contains(&needle)
        || market.condition_id.to_ascii_lowercase().contains(&needle)
}

fn matches_optional_bool(actual: bool, expected: Option<bool>) -> bool {
    expected.map(|value| value == actual).unwrap_or(true)
}

fn matches_open_order_filter(
    market: &MarketSummary,
    facets: &std::collections::HashMap<String, OpenOrderFacetRow>,
    status: Option<&str>,
    side: Option<&str>,
    role: Option<&str>,
) -> bool {
    let Some(facet) = facets.get(&market.condition_id) else {
        return false;
    };

    if let Some(status) = status {
        let matches = match status {
            "submitted" => facet.submitted_count > 0,
            "open" => facet.open_count > 0,
            "partial" => facet.partial_count > 0,
            "active" => market.open_order_count > 0,
            _ => false,
        };
        if !matches {
            return false;
        }
    }

    if let Some(side) = side {
        let matches = match side {
            "buy" => facet.has_buy_orders,
            "sell" => facet.has_sell_orders,
            _ => false,
        };
        if !matches {
            return false;
        }
    }

    if let Some(role) = role {
        let matches = match role {
            "bid_entry" => facet.has_bid_entry_orders,
            "ask_inventory" => facet.has_ask_inventory_orders,
            _ => false,
        };
        if !matches {
            return false;
        }
    }

    true
}

fn matches_inventory_exposure_side(market: &MarketSummary, side: Option<&str>) -> bool {
    match side {
        None => true,
        Some("yes") => market.net_exposure > Decimal::ZERO,
        Some("no") => market.net_exposure < Decimal::ZERO,
        Some("flat") => market.net_exposure == Decimal::ZERO,
        Some(_) => false,
    }
}

fn normalize_open_order_status_filter(input: &str) -> Result<&'static str> {
    match input.trim().to_ascii_lowercase().as_str() {
        "submitted" => Ok("submitted"),
        "open" => Ok("open"),
        "partial" | "partially_filled" => Ok("partial"),
        "active" => Ok("active"),
        _ => anyhow::bail!("invalid status filter"),
    }
}

fn normalize_open_order_side_filter(input: &str) -> Result<&'static str> {
    match input.trim().to_ascii_lowercase().as_str() {
        "buy" => Ok("buy"),
        "sell" => Ok("sell"),
        _ => anyhow::bail!("invalid side filter"),
    }
}

fn normalize_open_order_role_filter(input: &str) -> Result<&'static str> {
    match input.trim().to_ascii_lowercase().as_str() {
        "bid_entry" | "bid" => Ok("bid_entry"),
        "ask_inventory" | "inventory_ask" | "ask" => Ok("ask_inventory"),
        _ => anyhow::bail!("invalid role filter"),
    }
}

fn normalize_inventory_exposure_filter(input: Option<&str>) -> Result<Option<String>> {
    match input.map(|value| value.trim().to_ascii_lowercase()) {
        None => Ok(None),
        Some(value) if matches!(value.as_str(), "yes" | "no" | "flat") => Ok(Some(value)),
        Some(_) => anyhow::bail!("invalid exposure_side filter"),
    }
}

fn normalize_priority_filter(input: &str) -> Result<String> {
    match input.trim().to_ascii_lowercase().as_str() {
        "critical" | "high" | "normal" | "debug" => Ok(input.trim().to_ascii_lowercase()),
        _ => anyhow::bail!("invalid priority filter"),
    }
}

fn normalize_history_category(input: &str) -> Result<Vec<&'static str>> {
    match input.trim().to_ascii_lowercase().as_str() {
        "orders" => Ok(vec!["order_submitted", "order_cancelled", "order_resized"]),
        "fills" => Ok(vec!["fill_detected"]),
        "hedges" => Ok(vec![
            "hedge_intent_created",
            "hedge_decision_evaluated",
            "hedge_result_recorded",
            "hedge_exit_path_recorded",
        ]),
        "decisions" => Ok(vec!["decision_evaluated", "quote_rejected"]),
        "alerts" => Ok(vec![
            "monitor_degraded",
            "risk_state_changed",
            "projection_rebuilt",
        ]),
        _ => anyhow::bail!("invalid category filter"),
    }
}

fn normalize_error_level_filter(input: &str) -> Result<String> {
    match input.trim().to_ascii_lowercase().as_str() {
        "error" | "critical" | "warn" | "unknown" => Ok(input.trim().to_ascii_lowercase()),
        _ => anyhow::bail!("invalid error level filter"),
    }
}

fn normalized_page(page: i64, page_size: i64) -> (i64, i64, i64) {
    let page = page.max(1);
    let page_size = page_size.clamp(1, 250);
    let offset = (page - 1) * page_size;
    (page, page_size, offset)
}

fn page_slice<T>(mut items: Vec<T>, page: i64, page_size: i64) -> PageResponse<T> {
    let (page, page_size, offset) = normalized_page(page, page_size);
    let total = i64::try_from(items.len()).unwrap_or(i64::MAX);
    let offset_usize = usize::try_from(offset).unwrap_or(usize::MAX);
    let page_size_usize = usize::try_from(page_size).unwrap_or(usize::MAX);
    let paged_items = if offset_usize >= items.len() {
        Vec::new()
    } else {
        let tail = items.split_off(offset_usize);
        tail.into_iter().take(page_size_usize).collect()
    };

    PageResponse {
        items: paged_items,
        total,
        page,
        page_size,
    }
}

fn empty_page<T>(page: i64, page_size: i64) -> PageResponse<T> {
    let (page, page_size, _) = normalized_page(page, page_size);
    PageResponse {
        items: Vec::new(),
        total: 0,
        page,
        page_size,
    }
}

fn sum_decimals(values: impl Iterator<Item = Decimal>) -> Decimal {
    values.fold(Decimal::ZERO, |accumulator, value| accumulator + value)
}

fn sum_decimal_options(values: impl Iterator<Item = Option<Decimal>>) -> Decimal {
    values
        .flatten()
        .fold(Decimal::ZERO, |accumulator, value| accumulator + value)
}

fn parse_jsonc_value(contents: &str) -> Result<Value> {
    let stripped: String = contents
        .lines()
        .map(strip_json_line_comment)
        .collect::<Vec<_>>()
        .join("\n");
    serde_json::from_str(&stripped).context("parse config json")
}

fn strip_json_line_comment(line: &str) -> String {
    let mut in_string = false;
    let mut escaped = false;
    let mut chars = line.char_indices().peekable();

    while let Some((index, ch)) = chars.next() {
        if escaped {
            escaped = false;
            continue;
        }
        match ch {
            '\\' if in_string => escaped = true,
            '"' => in_string = !in_string,
            '/' if !in_string => {
                if let Some((_, next)) = chars.peek() {
                    if *next == '/' {
                        return line[..index].trim_end().to_string();
                    }
                }
            }
            _ => {}
        }
    }

    line.to_string()
}

#[derive(Debug)]
struct BotConfigSummary {
    max_total_exposure_usd: Option<Decimal>,
    discovery_poll_interval_secs: i64,
}

impl Default for BotConfigSummary {
    fn default() -> Self {
        Self {
            max_total_exposure_usd: None,
            discovery_poll_interval_secs: 300,
        }
    }
}

fn load_bot_config_summary() -> BotConfigSummary {
    let path = env::var("SPREADEATER_BOT_CONFIG").unwrap_or_else(|_| "config.json".to_string());
    let contents = match std::fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(_) => return BotConfigSummary::default(),
    };
    let value: Value = match parse_jsonc_value(&contents) {
        Ok(value) => value,
        Err(_) => return BotConfigSummary::default(),
    };

    BotConfigSummary {
        max_total_exposure_usd: value
            .get("risk")
            .and_then(|risk| risk.get("max_total_exposure"))
            .and_then(parse_decimal_value),
        discovery_poll_interval_secs: value
            .get("discovery")
            .and_then(|discovery| discovery.get("poll_interval_secs"))
            .and_then(Value::as_i64)
            .filter(|seconds| *seconds > 0)
            .unwrap_or(300),
    }
}

fn parse_decimal_value(value: &Value) -> Option<Decimal> {
    match value {
        Value::String(text) => text.parse::<Decimal>().ok(),
        Value::Number(number) => number.to_string().parse::<Decimal>().ok(),
        _ => None,
    }
}

fn normalize_event_type(input: &str) -> Result<String> {
    let normalized = input.trim().to_ascii_lowercase();
    let value = match normalized.as_str() {
        "decisionevaluated" | "decision_evaluated" => "decision_evaluated",
        "quoteapproved" | "quote_approved" => "quote_approved",
        "quoterejected" | "quote_rejected" => "quote_rejected",
        "ordersubmitted" | "order_submitted" => "order_submitted",
        "orderresized" | "order_resized" => "order_resized",
        "ordercancelled" | "order_cancelled" => "order_cancelled",
        "filldetected" | "fill_detected" => "fill_detected",
        "hedgeintentcreated" | "hedge_intent_created" => "hedge_intent_created",
        "hedgedecisionevaluated" | "hedge_decision_evaluated" => "hedge_decision_evaluated",
        "hedgeresultrecorded" | "hedge_result_recorded" => "hedge_result_recorded",
        "hedgeexitpathrecorded" | "hedge_exit_path_recorded" => "hedge_exit_path_recorded",
        "neutralityevaluated" | "neutrality_evaluated" => "neutrality_evaluated",
        "monitordegraded" | "monitor_degraded" => "monitor_degraded",
        "riskstatechanged" | "risk_state_changed" => "risk_state_changed",
        "userstreamstatuschanged" | "user_stream_status_changed" => "user_stream_status_changed",
        "statussnapshot" | "status_snapshot" => "status_snapshot",
        "calibrationadjusted" | "calibration_adjusted" => "calibration_adjusted",
        "projectionrebuilt" | "projection_rebuilt" => "projection_rebuilt",
        _ => anyhow::bail!("unsupported event type"),
    };
    Ok(value.to_string())
}

#[derive(Debug, FromRow)]
struct RunRow {
    run_id: String,
    mode: String,
    observer_health: String,
    global_halt: bool,
    risk_reason: Option<String>,
    user_stream_status: Option<String>,
    user_stream_detail: Option<String>,
    subscribed_markets: Option<i64>,
    managed_markets: Option<i64>,
    order_committed_usd: Option<Decimal>,
    position_committed_usd: Option<Decimal>,
    total_committed_usd: Option<Decimal>,
    max_total_exposure_usd: Option<Decimal>,
    api_balance_usd: Option<Decimal>,
    available_budget_usd: Option<Decimal>,
    competition_multiplier: Option<Decimal>,
    total_est_daily_usd: Option<Decimal>,
    producer_lag_ms: i64,
    index_lag_ms: i64,
    last_event_at: DateTime<Utc>,
}

#[derive(Debug, FromRow)]
struct MarketSummaryRow {
    condition_id: String,
    market_slug: Option<String>,
    question: Option<String>,
    decision_status: Option<String>,
    expected_reward_usd_day: Option<Decimal>,
    expected_edge_usd: Option<Decimal>,
    expected_edge_pct: Option<Decimal>,
    latest_reason: Option<String>,
    halted: bool,
    halt_reason: Option<String>,
    open_order_count: i64,
    open_order_share_size: Decimal,
    open_order_notional_usd: Decimal,
    yes_size: Decimal,
    no_size: Decimal,
    net_exposure: Decimal,
    complete_sets: Decimal,
    is_neutral: bool,
    last_event_at: DateTime<Utc>,
}

impl From<MarketSummaryRow> for MarketSummary {
    fn from(value: MarketSummaryRow) -> Self {
        Self {
            condition_id: value.condition_id,
            market_slug: value.market_slug,
            question: value.question,
            decision_status: value.decision_status,
            expected_reward_usd_day: value.expected_reward_usd_day,
            expected_edge_usd: value.expected_edge_usd,
            expected_edge_pct: value.expected_edge_pct,
            latest_reason: value.latest_reason,
            halted: value.halted,
            halt_reason: value.halt_reason,
            open_order_count: value.open_order_count,
            open_order_share_size: value.open_order_share_size,
            open_order_notional_usd: value.open_order_notional_usd,
            yes_size: value.yes_size,
            no_size: value.no_size,
            net_exposure: value.net_exposure,
            complete_sets: value.complete_sets,
            is_neutral: value.is_neutral,
            last_event_at: value.last_event_at,
        }
    }
}

#[derive(Debug, FromRow)]
struct MarketDetailRow {
    run_id: String,
    condition_id: String,
    market_slug: Option<String>,
    question: Option<String>,
    decision_status: Option<String>,
    expected_reward_usd_day: Option<Decimal>,
    expected_hedge_cost_usd: Option<Decimal>,
    expected_edge_usd: Option<Decimal>,
    expected_edge_pct: Option<Decimal>,
    committed_capital_usd: Decimal,
    effective_quote_size: Option<Decimal>,
    score_share: Option<Decimal>,
    max_hedgeable_size: Option<Decimal>,
    latest_reason: Option<String>,
    halted: bool,
    halt_reason: Option<String>,
    open_order_count: i64,
    open_order_share_size: Decimal,
    open_order_notional_usd: Decimal,
    yes_size: Decimal,
    no_size: Decimal,
    net_exposure: Decimal,
    complete_sets: Decimal,
    is_neutral: bool,
}

#[derive(Debug, FromRow)]
struct TraceRow {
    trace_id: String,
    run_id: String,
    condition_id: Option<String>,
    market_slug: Option<String>,
    question: Option<String>,
    status: String,
    decision_payload: Option<Value>,
}

#[derive(Debug, FromRow)]
struct OpenOrderFacetRow {
    condition_id: String,
    submitted_count: i64,
    open_count: i64,
    partial_count: i64,
    has_buy_orders: bool,
    has_sell_orders: bool,
    has_bid_entry_orders: bool,
    has_ask_inventory_orders: bool,
}

#[derive(Debug, FromRow)]
struct EventRow {
    id: i64,
    event_id: Uuid,
    event_type: String,
    priority: String,
    occurred_at: DateTime<Utc>,
    recorded_at: DateTime<Utc>,
    run_id: String,
    cycle_id: Option<String>,
    trace_id: Option<String>,
    source_component: String,
    mode: String,
    condition_id: Option<String>,
    market_slug: Option<String>,
    question: Option<String>,
    order_id: Option<String>,
    order_state: Option<String>,
    order_cancel_reason: Option<String>,
    replacement_order_id: Option<String>,
    order_size: Option<Decimal>,
    order_matched_size: Option<Decimal>,
    asset_id: Option<String>,
    hedge_id: Option<String>,
    reason_code: Option<String>,
    payload: Value,
}

#[derive(Debug, FromRow)]
struct BotErrorLogRow {
    id: i64,
    log_path: String,
    byte_offset: i64,
    parsed_at: Option<DateTime<Utc>>,
    level: Option<String>,
    message: String,
    raw_line: String,
    created_at: DateTime<Utc>,
}

impl From<EventRow> for EventListItem {
    fn from(value: EventRow) -> Self {
        Self {
            id: value.id,
            event_id: value.event_id,
            event_type: value.event_type,
            priority: value.priority,
            occurred_at: value.occurred_at,
            recorded_at: value.recorded_at,
            run_id: value.run_id,
            cycle_id: value.cycle_id,
            trace_id: value.trace_id,
            source_component: value.source_component,
            mode: value.mode,
            condition_id: value.condition_id,
            market_slug: value.market_slug,
            question: value.question,
            order_id: value.order_id,
            order_state: value.order_state,
            order_cancel_reason: value.order_cancel_reason,
            replacement_order_id: value.replacement_order_id,
            order_size: value.order_size,
            order_matched_size: value.order_matched_size,
            asset_id: value.asset_id,
            hedge_id: value.hedge_id,
            reason_code: value.reason_code,
            payload: value.payload,
        }
    }
}

impl From<BotErrorLogRow> for BotErrorLogEntry {
    fn from(value: BotErrorLogRow) -> Self {
        Self {
            id: value.id,
            log_path: value.log_path,
            byte_offset: value.byte_offset,
            parsed_at: value.parsed_at,
            level: value.level,
            message: value.message,
            raw_line: value.raw_line,
            created_at: value.created_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{normalize_event_type, normalize_history_category};

    #[test]
    fn normalize_event_type_accepts_snake_and_camel_inputs() {
        assert_eq!(
            normalize_event_type("order_cancelled").unwrap(),
            "order_cancelled"
        );
        assert_eq!(
            normalize_event_type("OrderCancelled").unwrap(),
            "order_cancelled"
        );
        assert_eq!(
            normalize_event_type("HedgeDecisionEvaluated").unwrap(),
            "hedge_decision_evaluated"
        );
        assert_eq!(
            normalize_event_type("hedge_exit_path_recorded").unwrap(),
            "hedge_exit_path_recorded"
        );
    }

    #[test]
    fn normalize_event_type_rejects_unknown_values() {
        assert!(normalize_event_type("not_real").is_err());
    }

    #[test]
    fn normalize_history_category_includes_new_hedge_events() {
        assert_eq!(
            normalize_history_category("hedges").unwrap(),
            vec![
                "hedge_intent_created",
                "hedge_decision_evaluated",
                "hedge_result_recorded",
                "hedge_exit_path_recorded",
            ]
        );
    }
}
