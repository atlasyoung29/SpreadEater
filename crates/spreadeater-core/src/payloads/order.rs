use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::reason_codes::CancelReasonCode;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuoteRefreshDiagnostics {
    pub would_trade: bool,
    pub reasons: Vec<String>,
    pub effective_quote_size: Decimal,
    pub available_budget_usd: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HedgeDepthDiagnostics {
    pub hedgeable_size: Decimal,
    pub min_order_size: Decimal,
    pub opposite_best_price: Decimal,
    pub opposite_best_size: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderEventDiagnostics {
    #[serde(default)]
    pub quote_refresh: Option<QuoteRefreshDiagnostics>,
    #[serde(default)]
    pub hedge_depth: Option<HedgeDepthDiagnostics>,
}

/// Payload for OrderSubmitted events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderSubmittedPayload {
    pub leg: String,
    pub side: String,
    pub price: Decimal,
    pub size: Decimal,
    #[serde(default)]
    pub matched_size: Decimal,
    pub token_id: String,
    pub neg_risk: bool,
    #[serde(default)]
    pub origin: Option<String>,
    #[serde(default)]
    pub role: Option<String>,
}

/// Payload for OrderCancelled events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderCancelledPayload {
    pub reason_code: CancelReasonCode,
    pub reason_text: String,
    pub old_size: Decimal,
    pub capital_delta: Option<Decimal>,
    #[serde(default)]
    pub origin: Option<String>,
    #[serde(default)]
    pub diagnostics: Option<OrderEventDiagnostics>,
}

/// Payload for OrderResized events (cancel-replace).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderResizedPayload {
    pub old_order_id: String,
    pub new_order_id: String,
    pub old_size: Decimal,
    pub new_size: Decimal,
    pub old_price: Decimal,
    pub new_price: Decimal,
    pub reason_code: CancelReasonCode,
    #[serde(default)]
    pub origin: Option<String>,
    #[serde(default)]
    pub diagnostics: Option<OrderEventDiagnostics>,
}

/// Payload for FillDetected events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FillDetectedPayload {
    pub trade_id: String,
    pub fill_price: Decimal,
    pub fill_size: Decimal,
    pub side: String,
    pub outcome: String,
    #[serde(default)]
    pub match_source: Option<String>,
    pub fallback_match: bool,
    #[serde(default)]
    pub anchored_order_id: Option<String>,
    #[serde(default)]
    pub deferred_to_reconciliation: bool,
}
