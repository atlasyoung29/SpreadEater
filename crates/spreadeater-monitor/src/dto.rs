use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OverviewResponse {
    pub run_id: String,
    pub mode: String,
    pub observer_health: String,
    pub global_halt: bool,
    pub risk_reason: Option<String>,
    pub user_stream_status: Option<String>,
    pub user_stream_detail: Option<String>,
    pub subscribed_markets: Option<i64>,
    pub managed_markets: Option<i64>,
    pub producer_lag_ms: i64,
    pub index_lag_ms: i64,
    pub last_event_at: DateTime<Utc>,
    pub expected_cycle_interval_secs: i64,
    pub active_markets: i64,
    pub open_orders: i64,
    pub committed_capital_usd: Decimal,
    pub order_committed_usd: Option<Decimal>,
    pub position_committed_usd: Option<Decimal>,
    pub total_committed_usd: Option<Decimal>,
    pub api_balance_usd: Option<Decimal>,
    pub available_budget_usd: Option<Decimal>,
    pub competition_multiplier: Option<Decimal>,
    pub max_total_exposure_usd: Option<Decimal>,
    pub unhedged_markets: i64,
    pub open_order_markets: i64,
    pub inventory_markets: i64,
    pub open_order_reward_usd_day: Decimal,
    pub open_order_notional_usd: Decimal,
    pub open_order_preview: Vec<MarketSummary>,
    pub inventory_preview: Vec<MarketSummary>,
    pub recent_history: Vec<EventListItem>,
    pub recent_errors: Vec<BotErrorLogEntry>,
    pub recent_alerts: Vec<EventListItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketSummary {
    pub condition_id: String,
    pub market_slug: Option<String>,
    pub question: Option<String>,
    pub decision_status: Option<String>,
    pub expected_reward_usd_day: Option<Decimal>,
    pub expected_edge_usd: Option<Decimal>,
    pub expected_edge_pct: Option<Decimal>,
    pub latest_reason: Option<String>,
    pub halted: bool,
    pub halt_reason: Option<String>,
    pub open_order_count: i64,
    pub open_order_share_size: Decimal,
    pub open_order_notional_usd: Decimal,
    pub yes_size: Decimal,
    pub no_size: Decimal,
    pub net_exposure: Decimal,
    pub complete_sets: Decimal,
    pub is_neutral: bool,
    pub last_event_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketDetailResponse {
    pub condition_id: String,
    pub run_id: String,
    pub market_slug: Option<String>,
    pub question: Option<String>,
    pub decision_status: Option<String>,
    pub expected_edge_usd: Option<Decimal>,
    pub expected_edge_pct: Option<Decimal>,
    pub expected_reward_usd_day: Option<Decimal>,
    pub expected_hedge_cost_usd: Option<Decimal>,
    pub committed_capital_usd: Decimal,
    pub effective_quote_size: Option<Decimal>,
    pub score_share: Option<Decimal>,
    pub max_hedgeable_size: Option<Decimal>,
    pub latest_reason: Option<String>,
    pub halted: bool,
    pub halt_reason: Option<String>,
    pub open_order_count: i64,
    pub open_order_share_size: Decimal,
    pub open_order_notional_usd: Decimal,
    pub yes_size: Decimal,
    pub no_size: Decimal,
    pub net_exposure: Decimal,
    pub complete_sets: Decimal,
    pub is_neutral: bool,
    pub recent_traces: Vec<String>,
    pub recent_events: Vec<EventListItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketReference {
    pub condition_id: Option<String>,
    pub market_slug: Option<String>,
    pub question: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecisionSnapshot {
    pub payload: Value,
    pub would_trade: Option<bool>,
    pub reasons: Vec<String>,
    pub expected_edge_usd: Option<Decimal>,
    pub expected_edge_pct: Option<Decimal>,
    pub expected_reward_usd_day: Option<Decimal>,
    pub expected_hedge_cost_usd: Option<Decimal>,
    pub committed_capital_usd: Option<Decimal>,
    pub effective_quote_size: Option<Decimal>,
    pub score_share: Option<Decimal>,
    pub max_hedgeable_size: Option<Decimal>,
    pub competition_multiplier_used: Option<Decimal>,
    pub api_balance_usd: Option<Decimal>,
    pub available_budget_usd: Option<Decimal>,
    pub rank_in_cycle: Option<u64>,
    pub ranked_market_count: Option<u64>,
    pub ranking_metric_name: Option<String>,
    pub ranking_metric_value: Option<Decimal>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct OrderSnapshot {
    pub order_id: String,
    pub trace_id: Option<String>,
    pub leg: Option<String>,
    pub side: Option<String>,
    pub price: Option<Decimal>,
    pub size: Option<Decimal>,
    pub matched_size: Decimal,
    pub state: String,
    pub origin: Option<String>,
    pub role: Option<String>,
    pub cancel_reason: Option<String>,
    pub replacement_order_id: Option<String>,
    pub committed_capital_delta_usd: Decimal,
    pub token_id: Option<String>,
    pub neg_risk: Option<bool>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct FillSnapshot {
    pub fill_id: String,
    pub trace_id: Option<String>,
    pub order_id: Option<String>,
    pub price: Option<Decimal>,
    pub size: Option<Decimal>,
    pub side: Option<String>,
    pub outcome: Option<String>,
    pub match_source: Option<String>,
    pub fallback_match: bool,
    pub occurred_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct HedgeSnapshot {
    pub hedge_id: String,
    pub trace_id: Option<String>,
    pub trigger_order_id: Option<String>,
    pub trigger_leg: Option<String>,
    pub fill_size: Option<Decimal>,
    pub fill_price: Option<Decimal>,
    pub hedge_token_id: Option<String>,
    pub hedge_side: Option<String>,
    pub hedge_order_id: Option<String>,
    pub result_status: Option<String>,
    pub hedge_price: Option<Decimal>,
    pub failure_reason: Option<String>,
    pub latency_ms: Option<i64>,
    pub origin: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct NeutralitySnapshot {
    pub trace_id: String,
    pub pre_yes_size: Decimal,
    pub pre_no_size: Decimal,
    pub post_yes_size: Decimal,
    pub post_no_size: Decimal,
    pub residual_exposure: Decimal,
    pub complete_sets: Decimal,
    pub tolerance: Decimal,
    pub is_neutral: bool,
    pub occurred_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceDetailResponse {
    pub trace_id: String,
    pub run_id: String,
    pub status: String,
    pub market: MarketReference,
    pub decision: Option<DecisionSnapshot>,
    pub orders: Vec<OrderSnapshot>,
    pub fills: Vec<FillSnapshot>,
    pub hedges: Vec<HedgeSnapshot>,
    pub neutrality: Option<NeutralitySnapshot>,
    pub timeline: Vec<EventListItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventListItem {
    pub id: i64,
    pub event_id: Uuid,
    pub event_type: String,
    pub priority: String,
    pub occurred_at: DateTime<Utc>,
    pub recorded_at: DateTime<Utc>,
    pub run_id: String,
    pub cycle_id: Option<String>,
    pub trace_id: Option<String>,
    pub source_component: String,
    pub mode: String,
    pub condition_id: Option<String>,
    pub market_slug: Option<String>,
    pub question: Option<String>,
    pub order_id: Option<String>,
    pub order_state: Option<String>,
    pub order_cancel_reason: Option<String>,
    pub replacement_order_id: Option<String>,
    pub order_size: Option<Decimal>,
    pub order_matched_size: Option<Decimal>,
    pub asset_id: Option<String>,
    pub hedge_id: Option<String>,
    pub reason_code: Option<String>,
    pub payload: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventListResponse {
    pub items: Vec<EventListItem>,
    pub next_cursor: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageResponse<T> {
    pub items: Vec<T>,
    pub total: i64,
    pub page: i64,
    pub page_size: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BotErrorLogEntry {
    pub id: i64,
    pub log_path: String,
    pub byte_offset: i64,
    pub parsed_at: Option<DateTime<Utc>>,
    pub level: Option<String>,
    pub message: String,
    pub raw_line: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigResponse {
    pub path: String,
    pub last_modified_at: DateTime<Utc>,
    pub value: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveFrame {
    pub channel: String,
    pub payload: Value,
}
