use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceLevel {
    pub price: Decimal,
    pub size: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBookSnapshot {
    pub token_id: String,
    pub exchange_ts: Option<DateTime<Utc>>,
    pub ingest_ts: DateTime<Utc>,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
}

impl OrderBookSnapshot {
    pub fn best_bid(&self) -> Option<&PriceLevel> {
        self.bids.first()
    }

    pub fn best_ask(&self) -> Option<&PriceLevel> {
        self.asks.first()
    }

    pub fn mid(&self) -> Option<Decimal> {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) => Some((bid.price + ask.price) / Decimal::from(2)),
            _ => None,
        }
    }

    pub fn spread(&self) -> Option<Decimal> {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) => Some(ask.price - bid.price),
            _ => None,
        }
    }

    pub fn is_stale(&self, max_age: chrono::Duration) -> bool {
        Utc::now() - self.ingest_ts > max_age
    }

    /// Walk depth on the ask side to compute cost of buying `target_size`.
    /// Returns (filled_size, weighted_avg_price, total_cost).
    pub fn walk_asks(&self, target_size: Decimal) -> DepthWalkResult {
        walk_levels(&self.asks, target_size)
    }

    /// Walk depth on the bid side to compute proceeds of selling `target_size`.
    /// Returns (filled_size, weighted_avg_price, total_cost).
    pub fn walk_bids(&self, target_size: Decimal) -> DepthWalkResult {
        walk_levels(&self.bids, target_size)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepthWalkResult {
    pub filled_size: Decimal,
    pub weighted_avg_price: Decimal,
    pub total_cost: Decimal,
    pub levels_consumed: usize,
    pub fully_filled: bool,
    pub worst_price: Option<Decimal>,
}

fn walk_levels(levels: &[PriceLevel], target_size: Decimal) -> DepthWalkResult {
    let mut remaining = target_size;
    let mut total_cost = Decimal::ZERO;
    let mut filled = Decimal::ZERO;
    let mut levels_consumed = 0;
    let mut worst_price = None;

    for level in levels {
        if remaining <= Decimal::ZERO {
            break;
        }
        let take = remaining.min(level.size);
        if take > Decimal::ZERO {
            worst_price = Some(level.price);
        }
        total_cost += take * level.price;
        filled += take;
        remaining -= take;
        levels_consumed += 1;
    }

    let weighted_avg_price = if filled > Decimal::ZERO {
        total_cost / filled
    } else {
        Decimal::ZERO
    };

    DepthWalkResult {
        filled_size: filled,
        weighted_avg_price,
        total_cost,
        levels_consumed,
        fully_filled: remaining <= Decimal::ZERO,
        worst_price,
    }
}
