use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Summary of a single quote leg within a decision.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuoteLegSummary {
    pub leg: String,
    pub price: Decimal,
    pub size: Decimal,
    pub status: String,
    pub reason: Option<String>,
}

/// Payload for DecisionEvaluated events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecisionEventPayload {
    pub candidate_quotes: Vec<QuoteLegSummary>,
    pub reasons: Vec<String>,
    pub effective_quote_size: Decimal,
    pub expected_reward_usd_day: Option<Decimal>,
    pub expected_hedge_cost_usd: Option<Decimal>,
    pub expected_edge_usd: Option<Decimal>,
    pub expected_edge_pct: Option<Decimal>,
    pub committed_capital_usd: Option<Decimal>,
    pub score_share: Option<Decimal>,
    pub max_hedgeable_size: Option<Decimal>,
    pub competition_multiplier_used: Option<Decimal>,
    pub api_balance_usd: Option<Decimal>,
    pub available_budget_usd: Option<Decimal>,
    #[serde(default)]
    pub rank_in_cycle: Option<u64>,
    #[serde(default)]
    pub ranked_market_count: Option<u64>,
    #[serde(default)]
    pub ranking_metric_name: Option<String>,
    #[serde(default)]
    pub ranking_metric_value: Option<Decimal>,
    #[serde(default)]
    pub frontier_eligible: Option<bool>,
    #[serde(default)]
    pub frontier_requires_reallocation: Option<bool>,
    #[serde(default)]
    pub frontier_replaces_condition_id: Option<String>,
    #[serde(default)]
    pub frontier_replaced_by_condition_id: Option<String>,
    #[serde(default)]
    pub frontier_counterfactual_budget_usd: Option<Decimal>,
    #[serde(default)]
    pub frontier_counterfactual_reclaimable_bid_capital_usd: Option<Decimal>,
    #[serde(default)]
    pub frontier_counterfactual_entrant_condition_id: Option<String>,
    #[serde(default)]
    pub frontier_counterfactual_entrant_ranking_metric_name: Option<String>,
    #[serde(default)]
    pub frontier_counterfactual_entrant_ranking_metric_value: Option<Decimal>,
    #[serde(default)]
    pub frontier_counterfactual_entrant_expected_reward_usd_day: Option<Decimal>,
    #[serde(default)]
    pub frontier_counterfactual_loser_condition_id: Option<String>,
    #[serde(default)]
    pub frontier_counterfactual_loser_ranking_metric_name: Option<String>,
    #[serde(default)]
    pub frontier_counterfactual_loser_ranking_metric_value: Option<Decimal>,
    #[serde(default)]
    pub frontier_counterfactual_loser_expected_reward_usd_day: Option<Decimal>,
    pub would_trade: bool,
}
