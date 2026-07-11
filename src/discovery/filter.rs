use rust_decimal::Decimal;
use tracing::{debug, info};

use crate::models::{CanonicalMarket, Market, MarketStatus};
use chrono::Utc;

#[derive(Debug)]
pub struct FilterResult {
    pub admitted: Vec<CanonicalMarket>,
    pub rejected: Vec<(Market, String)>,
}

pub fn filter_and_reconcile(markets: Vec<Market>, min_daily_reward: Decimal) -> FilterResult {
    let mut admitted = Vec::new();
    let mut rejected = Vec::new();

    for market in markets {
        match evaluate_market(&market, min_daily_reward) {
            Ok(canonical) => {
                info!(
                    condition_id = %canonical.condition_id,
                    slug = %canonical.market_slug,
                    reward = %canonical.reward_config.daily_reward_total,
                    "Market admitted"
                );
                admitted.push(canonical);
            }
            Err(reason) => {
                debug!(
                    condition_id = %market.condition_id,
                    reason = %reason,
                    "Market rejected"
                );
                rejected.push((market, reason));
            }
        }
    }

    info!(
        admitted = admitted.len(),
        rejected = rejected.len(),
        "Filter complete"
    );

    FilterResult { admitted, rejected }
}

/// Hours before market end_date to stop trading and exit positions.
const EXIT_BEFORE_HOURS: i64 = 24;

fn evaluate_market(market: &Market, min_daily_reward: Decimal) -> Result<CanonicalMarket, String> {
    // Must be active
    if !market.active {
        return Err("Market is not active".to_string());
    }

    // Must not be closed or archived
    if market.closed {
        return Err("Market is closed".to_string());
    }
    if market.archived {
        return Err("Market is archived".to_string());
    }

    // Must be accepting orders
    if !market.accepting_orders {
        return Err("Market is not accepting orders".to_string());
    }

    // Must be binary
    if !market.is_binary {
        return Err("Market is not binary".to_string());
    }

    // Must have reward config
    let reward_config = market
        .reward_config
        .as_ref()
        .ok_or_else(|| "No reward config".to_string())?;

    // Must meet reward threshold
    if reward_config.daily_reward_total < min_daily_reward {
        return Err(format!(
            "Daily reward total {} below threshold {}",
            reward_config.daily_reward_total, min_daily_reward
        ));
    }

    // Must have YES and NO tokens
    let yes_token = market
        .yes_token()
        .ok_or_else(|| "No YES token found".to_string())?;
    let no_token = market
        .no_token()
        .ok_or_else(|| "No NO token found".to_string())?;

    // Token IDs must not be empty
    if yes_token.token_id.is_empty() || no_token.token_id.is_empty() {
        return Err("Token ID is empty".to_string());
    }

    // Token IDs must be distinct
    if yes_token.token_id == no_token.token_id {
        return Err("YES and NO token IDs are identical".to_string());
    }

    // Parse end_date and reject markets expiring within EXIT_BEFORE_HOURS
    let end_date = market.end_date_iso.as_deref().and_then(|s| {
        chrono::DateTime::parse_from_rfc3339(s)
            .ok()
            .map(|dt| dt.with_timezone(&Utc))
    });

    if let Some(end) = end_date {
        let cutoff = Utc::now() + chrono::Duration::hours(EXIT_BEFORE_HOURS);
        if end <= cutoff {
            return Err(format!(
                "Market expires within {} hours (end_date: {})",
                EXIT_BEFORE_HOURS,
                end.format("%Y-%m-%d %H:%M UTC")
            ));
        }
    }

    Ok(CanonicalMarket {
        condition_id: market.condition_id.clone(),
        market_slug: market.market_slug.clone(),
        question: market.question.clone(),
        yes_token_id: yes_token.token_id.clone(),
        no_token_id: no_token.token_id.clone(),
        reward_config: reward_config.clone(),
        neg_risk: market.neg_risk,
        tick_size: market.minimum_tick_size.clone(),
        end_date,
        admitted_at: Utc::now(),
        status: MarketStatus::Admitted,
    })
}
