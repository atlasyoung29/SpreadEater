use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Payload for HedgeIntentCreated events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HedgeIntentPayload {
    pub trigger_order_id: String,
    pub trigger_leg: String,
    pub fill_size: Decimal,
    pub fill_price: Decimal,
    pub hedge_token_id: String,
    pub hedge_side: String,
    #[serde(default)]
    pub planned_hedge_shares: Option<Decimal>,
    #[serde(default)]
    pub planned_hedge_price: Option<Decimal>,
    #[serde(default)]
    pub planned_sellback_shares: Option<Decimal>,
    #[serde(default)]
    pub planned_sellback_price: Option<Decimal>,
    #[serde(default)]
    pub planned_sellback_reference_bid: Option<Decimal>,
    #[serde(default)]
    pub unresolved_shares: Option<Decimal>,
    #[serde(default)]
    pub pre_resolution_active_orders: Option<u64>,
    #[serde(default)]
    pub pre_resolution_pending_cancels: Option<u64>,
    #[serde(default)]
    pub cancel_wait_drained: Option<bool>,
    #[serde(default)]
    pub origin: Option<String>,
}

/// Payload for HedgeDecisionEvaluated events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HedgeDecisionPayload {
    pub trigger_leg: String,
    pub hedge_side: String,
    pub fill_size: Decimal,
    pub fill_price: Decimal,
    pub decision_mode: String,
    pub decision_reason_code: String,
    pub available_hedge_budget_usd: Decimal,
    #[serde(default)]
    pub filled_best_bid_price: Option<Decimal>,
    #[serde(default)]
    pub filled_best_bid_size: Option<Decimal>,
    #[serde(default)]
    pub opposite_best_ask_price: Option<Decimal>,
    #[serde(default)]
    pub opposite_best_ask_size: Option<Decimal>,
    pub planned_hedge_shares: Decimal,
    pub planned_hedge_price: Decimal,
    pub planned_sellback_shares: Decimal,
    pub planned_sellback_price: Decimal,
    pub unresolved_shares: Decimal,
}

/// Payload for HedgeResultRecorded events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HedgeResultPayload {
    pub hedge_order_id: Option<String>,
    pub result_status: String,
    pub hedge_price: Option<Decimal>,
    #[serde(default)]
    pub hedge_leg_status: Option<String>,
    #[serde(default)]
    pub hedge_cancel_status: Option<String>,
    #[serde(default)]
    pub hedge_cancel_reason: Option<String>,
    #[serde(default)]
    pub hedge_lookup_status: Option<String>,
    #[serde(default)]
    pub hedge_lookup_matched_shares: Option<Decimal>,
    #[serde(default)]
    pub hedge_lookup_error: Option<String>,
    #[serde(default)]
    pub hedge_trade_ids: Option<Vec<String>>,
    #[serde(default)]
    pub sellback_order_id: Option<String>,
    #[serde(default)]
    pub sellback_price: Option<Decimal>,
    #[serde(default)]
    pub sellback_execution_limit_price: Option<Decimal>,
    #[serde(default)]
    pub sellback_leg_status: Option<String>,
    #[serde(default)]
    pub sellback_response_status: Option<String>,
    #[serde(default)]
    pub sellback_lookup_status: Option<String>,
    #[serde(default)]
    pub sellback_lookup_matched_shares: Option<Decimal>,
    #[serde(default)]
    pub sellback_lookup_error: Option<String>,
    #[serde(default)]
    pub sellback_trade_ids: Option<Vec<String>>,
    #[serde(default)]
    pub post_sync_net_exposure: Option<Decimal>,
    #[serde(default)]
    pub post_sync_yes_size: Option<Decimal>,
    #[serde(default)]
    pub post_sync_no_size: Option<Decimal>,
    #[serde(default)]
    pub post_sync_source: Option<String>,
    #[serde(default)]
    pub halt_signal_suppressed: bool,
    pub failure_reason: Option<String>,
    pub latency_ms: u64,
    #[serde(default)]
    pub origin: Option<String>,
}

/// Payload for HedgeExitPathRecorded events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HedgeExitPathPayload {
    pub post_sync_yes_size: Decimal,
    pub post_sync_no_size: Decimal,
    pub post_sync_net_exposure: Decimal,
    pub post_sync_complete_sets: Decimal,
    pub post_sync_source: String,
    pub exit_path_status: String,
    pub merge_eligible_pairs: Decimal,
    pub ctf_merge_configured: bool,
    pub merge_attempted: bool,
    #[serde(default)]
    pub merge_tx_hash: Option<String>,
    #[serde(default)]
    pub merge_failure_reason: Option<String>,
    pub fallback_asks_attempted: bool,
    pub fallback_ask_count: u64,
    #[serde(default)]
    pub fallback_failure_reason: Option<String>,
}

/// Payload for NeutralityEvaluated events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NeutralityPayload {
    pub pre_yes_size: Decimal,
    pub pre_no_size: Decimal,
    pub post_yes_size: Decimal,
    pub post_no_size: Decimal,
    pub residual_exposure: Decimal,
    pub complete_sets: Decimal,
    pub tolerance: Decimal,
    pub is_neutral: bool,
}
