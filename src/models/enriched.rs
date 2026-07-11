use serde::{Deserialize, Serialize};

use super::{DecisionReport, OrderBookSnapshot, RewardConfig};

/// Decision report bundled with the raw book data that produced it,
/// enabling full replay with updated parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnrichedDecisionReport {
    pub report: DecisionReport,
    pub yes_book: OrderBookSnapshot,
    pub no_book: OrderBookSnapshot,
    pub reward_config: RewardConfig,
}
