export type DecimalString = string;

export interface EventListItem {
  id: number;
  event_id: string;
  event_type: string;
  priority: string;
  occurred_at: string;
  recorded_at: string;
  run_id: string;
  cycle_id: string | null;
  trace_id: string | null;
  source_component: string;
  mode: string;
  condition_id: string | null;
  market_slug: string | null;
  question: string | null;
  order_id: string | null;
  order_state: string | null;
  order_cancel_reason: string | null;
  replacement_order_id: string | null;
  order_size: DecimalString | null;
  order_matched_size: DecimalString | null;
  asset_id: string | null;
  hedge_id: string | null;
  reason_code: string | null;
  payload: Record<string, unknown>;
}

export interface MarketSummary {
  condition_id: string;
  market_slug: string | null;
  question: string | null;
  decision_status: string | null;
  expected_reward_usd_day: DecimalString | null;
  expected_edge_usd: DecimalString | null;
  expected_edge_pct: DecimalString | null;
  latest_reason: string | null;
  halted: boolean;
  halt_reason: string | null;
  open_order_count: number;
  open_order_share_size: DecimalString;
  open_order_notional_usd: DecimalString;
  yes_size: DecimalString;
  no_size: DecimalString;
  net_exposure: DecimalString;
  complete_sets: DecimalString;
  is_neutral: boolean;
  last_event_at: string;
}

export interface OverviewResponse {
  run_id: string;
  mode: string;
  observer_health: string;
  global_halt: boolean;
  risk_reason: string | null;
  user_stream_status: string | null;
  user_stream_detail: string | null;
  subscribed_markets: number | null;
  managed_markets: number | null;
  producer_lag_ms: number;
  index_lag_ms: number;
  last_event_at: string;
  expected_cycle_interval_secs: number;
  active_markets: number;
  open_orders: number;
  committed_capital_usd: DecimalString;
  order_committed_usd: DecimalString | null;
  position_committed_usd: DecimalString | null;
  total_committed_usd: DecimalString | null;
  api_balance_usd: DecimalString | null;
  available_budget_usd: DecimalString | null;
  competition_multiplier: DecimalString | null;
  max_total_exposure_usd: DecimalString | null;
  unhedged_markets: number;
  open_order_markets: number;
  inventory_markets: number;
  open_order_reward_usd_day: DecimalString;
  open_order_notional_usd: DecimalString;
  open_order_preview: MarketSummary[];
  inventory_preview: MarketSummary[];
  recent_history: EventListItem[];
  recent_errors: BotErrorLogEntry[];
  recent_alerts: EventListItem[];
}

export interface PageResponse<T> {
  items: T[];
  total: number;
  page: number;
  page_size: number;
}

export interface BotErrorLogEntry {
  id: number;
  log_path: string;
  byte_offset: number;
  parsed_at: string | null;
  level: string | null;
  message: string;
  raw_line: string;
  created_at: string;
}

export interface ConfigResponse {
  path: string;
  last_modified_at: string;
  value: Record<string, unknown>;
}

export interface MarketDetailResponse {
  condition_id: string;
  run_id: string;
  market_slug: string | null;
  question: string | null;
  decision_status: string | null;
  expected_edge_usd: DecimalString | null;
  expected_edge_pct: DecimalString | null;
  expected_reward_usd_day: DecimalString | null;
  expected_hedge_cost_usd: DecimalString | null;
  committed_capital_usd: DecimalString;
  effective_quote_size: DecimalString | null;
  score_share: DecimalString | null;
  max_hedgeable_size: DecimalString | null;
  latest_reason: string | null;
  halted: boolean;
  halt_reason: string | null;
  open_order_count: number;
  open_order_share_size: DecimalString;
  open_order_notional_usd: DecimalString;
  yes_size: DecimalString;
  no_size: DecimalString;
  net_exposure: DecimalString;
  complete_sets: DecimalString;
  is_neutral: boolean;
  recent_traces: string[];
  recent_events: EventListItem[];
}

export interface MarketReference {
  condition_id: string | null;
  market_slug: string | null;
  question: string | null;
}

export interface DecisionSnapshot {
  payload: Record<string, unknown>;
  would_trade: boolean | null;
  reasons: string[];
  expected_edge_usd: DecimalString | null;
  expected_edge_pct: DecimalString | null;
  expected_reward_usd_day: DecimalString | null;
  expected_hedge_cost_usd: DecimalString | null;
  committed_capital_usd: DecimalString | null;
  effective_quote_size: DecimalString | null;
  score_share: DecimalString | null;
  max_hedgeable_size: DecimalString | null;
  competition_multiplier_used: DecimalString | null;
  api_balance_usd: DecimalString | null;
  available_budget_usd: DecimalString | null;
  rank_in_cycle: number | null;
  ranked_market_count: number | null;
  ranking_metric_name: string | null;
  ranking_metric_value: DecimalString | null;
}

export interface OrderSnapshot {
  order_id: string;
  trace_id: string | null;
  leg: string | null;
  side: string | null;
  price: DecimalString | null;
  size: DecimalString | null;
  matched_size: DecimalString;
  state: string;
  origin: string | null;
  role: string | null;
  cancel_reason: string | null;
  replacement_order_id: string | null;
  committed_capital_delta_usd: DecimalString;
  token_id: string | null;
  neg_risk: boolean | null;
  created_at: string;
  updated_at: string;
}

export interface FillSnapshot {
  fill_id: string;
  trace_id: string | null;
  order_id: string | null;
  price: DecimalString | null;
  size: DecimalString | null;
  side: string | null;
  outcome: string | null;
  match_source: string | null;
  fallback_match: boolean;
  occurred_at: string;
}

export interface HedgeSnapshot {
  hedge_id: string;
  trace_id: string | null;
  trigger_order_id: string | null;
  trigger_leg: string | null;
  fill_size: DecimalString | null;
  fill_price: DecimalString | null;
  hedge_token_id: string | null;
  hedge_side: string | null;
  hedge_order_id: string | null;
  result_status: string | null;
  hedge_price: DecimalString | null;
  failure_reason: string | null;
  latency_ms: number | null;
  origin: string | null;
  created_at: string;
  updated_at: string;
}

export interface NeutralitySnapshot {
  trace_id: string;
  pre_yes_size: DecimalString;
  pre_no_size: DecimalString;
  post_yes_size: DecimalString;
  post_no_size: DecimalString;
  residual_exposure: DecimalString;
  complete_sets: DecimalString;
  tolerance: DecimalString;
  is_neutral: boolean;
  occurred_at: string;
}

export interface TraceDetailResponse {
  trace_id: string;
  run_id: string;
  status: string;
  market: MarketReference;
  decision: DecisionSnapshot | null;
  orders: OrderSnapshot[];
  fills: FillSnapshot[];
  hedges: HedgeSnapshot[];
  neutrality: NeutralitySnapshot | null;
  timeline: EventListItem[];
}

export interface LiveFrame<T = unknown> {
  channel: "overview" | "market" | "trace" | "alerts" | "errors" | string;
  payload: T;
}
