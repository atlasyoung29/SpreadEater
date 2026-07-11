use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::{QuoteCandidate, QuoteStatus};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RewardViability {
    pub estimated_reward: Decimal,
    pub estimated_hedge_cost: Decimal,
    pub estimated_edge: Decimal,
    /// The approximate score share used for this estimate.
    #[serde(default)]
    pub score_share_approx: Decimal,
    /// Return per share committed (estimated_edge / shares_committed).
    /// Hedge-aware: each share costs $1 total (bid + hedge), so this equals return per dollar of true account capacity.
    #[serde(default, alias = "return_per_dollar")]
    pub return_per_share: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecisionReport {
    pub condition_id: String,
    pub market_slug: String,
    pub question: String,
    pub daily_reward_total: Decimal,
    pub score_proxy: Option<Decimal>,
    /// Max spread from RewardConfig, stored for replay.
    #[serde(default)]
    pub max_spread: Decimal,
    /// The effective quote size used (dynamic, may differ from default).
    #[serde(default)]
    pub effective_quote_size: Decimal,
    pub candidate_quotes: Vec<QuoteCandidate>,
    pub reward_viability: Option<RewardViability>,
    pub would_trade: bool,
    pub reasons: Vec<String>,
}

impl DecisionReport {
    pub fn approved_count(&self) -> usize {
        self.candidate_quotes
            .iter()
            .filter(|q| q.status == QuoteStatus::Approved)
            .count()
    }
}
