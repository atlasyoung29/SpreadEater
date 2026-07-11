use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Market {
    pub condition_id: String,
    pub market_slug: String,
    pub question: String,
    pub active: bool,
    pub closed: bool,
    pub archived: bool,
    pub accepting_orders: bool,
    pub is_binary: bool,
    pub neg_risk: bool,
    pub minimum_tick_size: String,
    pub tokens: Vec<TokenInfo>,
    pub reward_config: Option<RewardConfig>,
    pub end_date_iso: Option<String>,
    pub discovered_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenInfo {
    pub token_id: String,
    pub outcome: Outcome,
    pub last_price: Option<Decimal>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Outcome {
    Yes,
    No,
}

impl std::fmt::Display for Outcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Outcome::Yes => write!(f, "YES"),
            Outcome::No => write!(f, "NO"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RewardConfig {
    pub condition_id: String,
    pub daily_reward_rates: Vec<Decimal>,
    pub daily_reward_total: Decimal,
    pub min_size: Decimal,
    pub max_spread: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CanonicalMarket {
    pub condition_id: String,
    pub market_slug: String,
    pub question: String,
    pub yes_token_id: String,
    pub no_token_id: String,
    pub reward_config: RewardConfig,
    pub neg_risk: bool,
    pub tick_size: String,
    pub end_date: Option<DateTime<Utc>>,
    pub admitted_at: DateTime<Utc>,
    pub status: MarketStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MarketStatus {
    Admitted,
    Quarantined,
    Rejected,
}

impl Market {
    pub fn yes_token(&self) -> Option<&TokenInfo> {
        self.tokens.iter().find(|t| t.outcome == Outcome::Yes)
    }

    pub fn no_token(&self) -> Option<&TokenInfo> {
        self.tokens.iter().find(|t| t.outcome == Outcome::No)
    }
}
