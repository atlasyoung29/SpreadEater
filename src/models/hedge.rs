use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::QuoteLeg;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HedgeabilityReport {
    pub condition_id: String,
    pub trigger_leg: QuoteLeg,
    pub candidate_size: Decimal,
    pub opposite_token_id: String,
    pub opposite_depth_available: Decimal,
    pub max_hedgeable_size: Decimal,
    pub weighted_avg_hedge_price: Decimal,
    pub estimated_hedge_cost: Decimal,
    pub slippage_bps: Decimal,
    pub is_approved: bool,
    pub rejection_reason: Option<String>,
}
