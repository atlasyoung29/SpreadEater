use rust_decimal::Decimal;
use tracing::debug;

use crate::config::StrategyConfig;
use crate::models::{
    CanonicalMarket, OrderBookSnapshot, QuoteCandidate, QuoteLeg, QuoteSet, QuoteStatus,
};

/// Compute four candidate quote legs for a market given current YES and NO books.
pub fn compute_quote_set(
    market: &CanonicalMarket,
    yes_book: &OrderBookSnapshot,
    no_book: &OrderBookSnapshot,
    config: &StrategyConfig,
    shadow_mode: bool,
    size_override: Option<Decimal>,
) -> QuoteSet {
    let base_size = size_override.unwrap_or(config.default_quote_size);
    let max_spread = market.reward_config.max_spread;
    let effective_size = base_size.max(market.reward_config.min_size);
    let tick_size = market
        .tick_size
        .parse::<Decimal>()
        .unwrap_or(Decimal::new(1, 2)); // fallback 0.01

    let mut candidates = Vec::with_capacity(4);

    // YES BID: we bid to buy YES tokens
    candidates.push(compute_bid_leg(
        &market.condition_id,
        QuoteLeg::YesBid,
        yes_book,
        effective_size,
        max_spread,
        config.bid_depth_pct,
        tick_size,
        config.min_outcome_price,
    ));

    // YES ASK: we offer to sell YES tokens
    // In shadow mode this is SIMULATED_ONLY (requires owned inventory in live)
    candidates.push(compute_ask_leg(
        &market.condition_id,
        QuoteLeg::YesAsk,
        yes_book,
        effective_size,
        max_spread,
        tick_size,
        shadow_mode,
    ));

    // NO BID: we bid to buy NO tokens
    candidates.push(compute_bid_leg(
        &market.condition_id,
        QuoteLeg::NoBid,
        no_book,
        effective_size,
        max_spread,
        config.bid_depth_pct,
        tick_size,
        config.min_outcome_price,
    ));

    // NO ASK: we offer to sell NO tokens
    candidates.push(compute_ask_leg(
        &market.condition_id,
        QuoteLeg::NoAsk,
        no_book,
        effective_size,
        max_spread,
        tick_size,
        shadow_mode,
    ));

    debug!(
        condition_id = %market.condition_id,
        legs = candidates.len(),
        "Quote set computed"
    );

    QuoteSet {
        condition_id: market.condition_id.clone(),
        candidates,
    }
}

fn compute_bid_leg(
    condition_id: &str,
    leg: QuoteLeg,
    book: &OrderBookSnapshot,
    size: Decimal,
    max_spread: Decimal,
    bid_depth_pct: Decimal,
    tick_size: Decimal,
    min_outcome_price: Decimal,
) -> QuoteCandidate {
    let two = Decimal::from(2);

    let (price, status, reason) = match (book.best_bid(), book.best_ask()) {
        (Some(bid), Some(ask)) => {
            let mid = (bid.price + ask.price) / two;

            // Reject outcomes with mid-price below minimum
            if mid < min_outcome_price {
                return QuoteCandidate {
                    condition_id: condition_id.to_string(),
                    leg,
                    price: mid,
                    size,
                    status: QuoteStatus::Rejected,
                    reason: Some(format!(
                        "Mid ${:.2} below min_outcome_price ${:.2}",
                        mid, min_outcome_price
                    )),
                };
            }

            // Reward floor: ask - max_spread, clamped to at least 1 tick
            let floor = (ask.price - max_spread).max(tick_size);
            // Available range below mid that still earns rewards
            let range = mid - floor;
            let target = if range > Decimal::ZERO {
                mid - (bid_depth_pct * range)
            } else {
                // Floor is at or above mid — just use best_bid
                bid.price
            };
            let price = round_down_to_tick(target.max(floor), tick_size);

            if price > Decimal::ZERO {
                (price, QuoteStatus::Approved, None)
            } else {
                (
                    Decimal::ZERO,
                    QuoteStatus::Rejected,
                    Some("Computed bid price <= 0".to_string()),
                )
            }
        }
        (Some(bid), None) => {
            // No ask to compute mid — use best_bid as proxy
            if bid.price < min_outcome_price {
                return QuoteCandidate {
                    condition_id: condition_id.to_string(),
                    leg,
                    price: bid.price,
                    size,
                    status: QuoteStatus::Rejected,
                    reason: Some(format!(
                        "Best bid ${:.2} below min_outcome_price ${:.2}",
                        bid.price, min_outcome_price
                    )),
                };
            }
            (bid.price, QuoteStatus::Approved, None)
        }
        (None, Some(ask)) => {
            // Use ask as upper-bound proxy — if ask < threshold, mid is certainly below
            if ask.price < min_outcome_price {
                return QuoteCandidate {
                    condition_id: condition_id.to_string(),
                    leg,
                    price: ask.price,
                    size,
                    status: QuoteStatus::Rejected,
                    reason: Some(format!(
                        "Best ask ${:.2} below min_outcome_price ${:.2}",
                        ask.price, min_outcome_price
                    )),
                };
            }

            let floor = (ask.price - max_spread).max(tick_size);
            let range = ask.price - floor;
            let target = if range > Decimal::ZERO {
                ask.price - (bid_depth_pct * range)
            } else {
                floor
            };
            let price = round_down_to_tick(target.max(floor), tick_size);

            if price > Decimal::ZERO {
                (price, QuoteStatus::Approved, None)
            } else {
                (
                    Decimal::ZERO,
                    QuoteStatus::Rejected,
                    Some("Cannot compute valid bid price".to_string()),
                )
            }
        }
        (None, None) => (
            Decimal::ZERO,
            QuoteStatus::Rejected,
            Some("Empty book - no bids or asks".to_string()),
        ),
    };

    QuoteCandidate {
        condition_id: condition_id.to_string(),
        leg,
        price,
        size,
        status,
        reason,
    }
}

fn compute_ask_leg(
    condition_id: &str,
    leg: QuoteLeg,
    book: &OrderBookSnapshot,
    size: Decimal,
    max_spread: Decimal,
    tick_size: Decimal,
    shadow_mode: bool,
) -> QuoteCandidate {
    let (price, status, reason) = match (book.best_bid(), book.best_ask()) {
        (Some(bid), Some(ask)) => {
            let spread = ask.price - bid.price;
            let quote_price = if spread > max_spread {
                bid.price + max_spread
            } else {
                ask.price
            };

            if shadow_mode {
                (
                    quote_price,
                    QuoteStatus::SimulatedOnly,
                    Some("Shadow mode: requires owned inventory in live".to_string()),
                )
            } else {
                (quote_price, QuoteStatus::Approved, None)
            }
        }
        (Some(bid), None) => {
            let quote_price = bid.price + max_spread;
            let status = if shadow_mode {
                QuoteStatus::SimulatedOnly
            } else {
                QuoteStatus::Approved
            };
            (
                quote_price,
                status,
                shadow_mode.then(|| "Shadow mode: requires owned inventory in live".to_string()),
            )
        }
        (None, Some(ask)) => {
            if shadow_mode {
                (
                    ask.price,
                    QuoteStatus::SimulatedOnly,
                    Some("Shadow mode: requires owned inventory in live".to_string()),
                )
            } else {
                (ask.price, QuoteStatus::Approved, None)
            }
        }
        (None, None) => (
            Decimal::ZERO,
            QuoteStatus::Rejected,
            Some("Empty book - no bids or asks".to_string()),
        ),
    };

    let price = round_down_to_tick(price, tick_size);

    QuoteCandidate {
        condition_id: condition_id.to_string(),
        leg,
        price,
        size,
        status,
        reason,
    }
}

/// Round price down to the nearest tick size.
fn round_down_to_tick(price: Decimal, tick_size: Decimal) -> Decimal {
    (price / tick_size).floor() * tick_size
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use rust_decimal_macros::dec;

    fn test_book(bid: Decimal, ask: Decimal) -> OrderBookSnapshot {
        OrderBookSnapshot {
            token_id: "token".to_string(),
            exchange_ts: None,
            ingest_ts: Utc::now(),
            bids: vec![crate::models::PriceLevel {
                price: bid,
                size: dec!(100),
            }],
            asks: vec![crate::models::PriceLevel {
                price: ask,
                size: dec!(100),
            }],
        }
    }

    #[test]
    fn bid_leg_can_price_below_floor_when_mid_still_meets_min_outcome_price() {
        let book = test_book(dec!(0.18), dec!(0.22));

        let candidate = compute_bid_leg(
            "market",
            QuoteLeg::YesBid,
            &book,
            dec!(50),
            dec!(0.03),
            dec!(0.50),
            dec!(0.01),
            dec!(0.20),
        );

        assert_eq!(candidate.status, QuoteStatus::Approved);
        assert_eq!(candidate.price, dec!(0.19));
        assert!(candidate.reason.is_none());
    }
}
