use rust_decimal::Decimal;
use tracing::debug;

use crate::config::ScoreProxyConfig;
use crate::models::{OrderBookSnapshot, PriceLevel, QuoteLeg, QuoteSet, RewardConfig};

/// Result of the local score proxy estimation.
#[derive(Debug, Clone)]
pub struct ScoreProxyResult {
    /// Our estimated share of daily rewards, clamped to [min, max].
    pub estimated_share: Decimal,
    /// Our total raw score (sum of per-order scores).
    pub our_total_score: Decimal,
    /// Estimated total competitor score from visible book depth (approximate).
    pub competitor_total_approx: Decimal,
    /// Whether the market is in extreme territory (mid < 0.10 or > 0.90).
    pub is_extreme_market: bool,
}

/// Compute per-order score using Polymarket's documented formula:
/// S(v, s) = ((v - s) / v)^2 * size
///
/// Returns zero if spread >= max_spread or max_spread == 0.
pub fn per_order_score(max_spread: Decimal, spread_to_mid: Decimal, size: Decimal) -> Decimal {
    if max_spread <= Decimal::ZERO || spread_to_mid >= max_spread {
        return Decimal::ZERO;
    }
    let ratio = (max_spread - spread_to_mid) / max_spread;
    ratio * ratio * size
}

/// Sum scores for all visible price levels on one side of a book,
/// treating each level as a competitor order.
pub fn score_book_side(
    levels: &[PriceLevel],
    mid: Decimal,
    max_spread: Decimal,
    is_bid_side: bool,
) -> Decimal {
    levels
        .iter()
        .map(|level| {
            let spread = if is_bid_side {
                mid - level.price // bids are below mid
            } else {
                level.price - mid // asks are above mid
            };
            per_order_score(max_spread, spread.abs(), level.size)
        })
        .sum()
}

/// Compute the local score proxy for a market.
///
/// Uses the Polymarket scoring formula to estimate:
/// 1. Our score from proposed quotes
/// 2. Competitor scores from visible book depth
/// 3. Our approximate share = our_Q / (our_Q + competitor_Q)
///
/// All estimates are approximate — no aggregate market-maker score
/// endpoint exists in the documented API.
pub fn compute_score_proxy(
    quote_set: &QuoteSet,
    yes_book: &OrderBookSnapshot,
    no_book: &OrderBookSnapshot,
    reward_config: &RewardConfig,
    config: &ScoreProxyConfig,
) -> ScoreProxyResult {
    let max_spread = reward_config.max_spread;

    // Guard: if max_spread is zero or missing, can't compute scores
    if max_spread <= Decimal::ZERO {
        return fallback_result(config.min_score_share);
    }

    let yes_mid = yes_book.mid();
    let no_mid = no_book.mid();

    // Need at least one mid to compute anything useful
    if yes_mid.is_none() && no_mid.is_none() {
        return fallback_result(config.min_score_share);
    }

    // Determine if extreme market (mid < 0.10 or > 0.90)
    let ten_cents = Decimal::new(10, 2);
    let ninety_cents = Decimal::new(90, 2);
    let is_extreme = yes_mid
        .map(|m| m < ten_cents || m > ninety_cents)
        .unwrap_or(false);

    // --- Score our proposed quotes ---
    // Simple sum of per-order scores (matches status log estimation).
    let mut our_total = Decimal::ZERO;

    for candidate in &quote_set.candidates {
        if candidate.status != crate::models::QuoteStatus::Approved {
            continue;
        }

        let spread = match candidate.leg {
            QuoteLeg::YesBid => {
                let Some(m) = yes_mid else { continue };
                (m - candidate.price).abs()
            }
            QuoteLeg::YesAsk => {
                let Some(m) = yes_mid else { continue };
                (candidate.price - m).abs()
            }
            QuoteLeg::NoBid => {
                let Some(m) = no_mid else { continue };
                (m - candidate.price).abs()
            }
            QuoteLeg::NoAsk => {
                let Some(m) = no_mid else { continue };
                (candidate.price - m).abs()
            }
        };
        our_total += per_order_score(max_spread, spread, candidate.size);
    }

    // --- Score competitor orders from visible book depth ---
    // Simple sum of all visible depth (no two_sided_q_min — we can't attribute
    // orders to individual market makers, so aggregating via Q_min was wrong).
    let mut comp_total = Decimal::ZERO;

    if let Some(m) = yes_mid {
        comp_total += score_book_side(&yes_book.bids, m, max_spread, true);
        comp_total += score_book_side(&yes_book.asks, m, max_spread, false);
    }
    if let Some(m) = no_mid {
        comp_total += score_book_side(&no_book.bids, m, max_spread, true);
        comp_total += score_book_side(&no_book.asks, m, max_spread, false);
    }

    // Apply competition multiplier (inflates competitor estimate conservatively)
    let adjusted_comp = comp_total * config.competition_multiplier;

    // Compute share
    let total = our_total + adjusted_comp;
    let raw_share = if total > Decimal::ZERO {
        our_total / total
    } else {
        config.min_score_share
    };

    // Clamp to configured bounds
    let estimated_share = raw_share
        .max(config.min_score_share)
        .min(config.max_score_share);

    debug!(
        our_total = %our_total,
        comp_total = %comp_total,
        adjusted_comp = %adjusted_comp,
        raw_share = %raw_share,
        estimated_share = %estimated_share,
        is_extreme = is_extreme,
        "Score proxy computed (approx)"
    );

    ScoreProxyResult {
        estimated_share,
        our_total_score: our_total,
        competitor_total_approx: adjusted_comp,
        is_extreme_market: is_extreme,
    }
}

/// Compute the quote size needed to achieve the target score share.
///
/// Scores visible book depth as competition, then solves for the size
/// that would give us `target_score_share` of the rewards pool.
/// Clamps to [min_size, max_size] and respects Polymarket's min_size
/// for reward eligibility.
pub fn compute_dynamic_size(
    yes_book: &OrderBookSnapshot,
    no_book: &OrderBookSnapshot,
    reward_config: &RewardConfig,
    config: &ScoreProxyConfig,
    min_size: Decimal,
    max_size: Decimal,
) -> Decimal {
    // Fallback size: respect both min_size and max_size (budget) constraints.
    let fallback = min_size.min(max_size);

    let max_spread = reward_config.max_spread;
    if max_spread <= Decimal::ZERO {
        return fallback;
    }

    let yes_mid = yes_book.mid();
    let no_mid = no_book.mid();

    if yes_mid.is_none() && no_mid.is_none() {
        return fallback;
    }

    // Score competitor book depth (simple sum, matches compute_score_proxy)
    let mut comp_total = Decimal::ZERO;

    if let Some(m) = yes_mid {
        comp_total += score_book_side(&yes_book.bids, m, max_spread, true);
        comp_total += score_book_side(&yes_book.asks, m, max_spread, false);
    }
    if let Some(m) = no_mid {
        comp_total += score_book_side(&no_book.bids, m, max_spread, true);
        comp_total += score_book_side(&no_book.asks, m, max_spread, false);
    }

    let adjusted_comp = comp_total * config.competition_multiplier;

    if adjusted_comp <= Decimal::ZERO {
        return fallback;
    }

    let target = config.target_score_share;
    if target <= Decimal::ZERO || target >= Decimal::ONE {
        return fallback;
    }

    // Solve: target = our_Q / (our_Q + comp_Q)
    // => our_Q = target / (1 - target) * comp_Q
    let needed_q = (target / (Decimal::ONE - target)) * adjusted_comp;

    // Estimate score-per-unit from average bid-ask half-spread
    let mut spread_sum = Decimal::ZERO;
    let mut spread_count = 0u32;

    if let (Some(bid), Some(ask)) = (yes_book.best_bid(), yes_book.best_ask()) {
        let half_spread = (ask.price - bid.price) / Decimal::from(2);
        spread_sum += half_spread;
        spread_count += 1;
    }
    if let (Some(bid), Some(ask)) = (no_book.best_bid(), no_book.best_ask()) {
        let half_spread = (ask.price - bid.price) / Decimal::from(2);
        spread_sum += half_spread;
        spread_count += 1;
    }

    if spread_count == 0 {
        return fallback;
    }

    let avg_spread = spread_sum / Decimal::from(spread_count);
    let score_per_unit = per_order_score(max_spread, avg_spread, Decimal::ONE);

    if score_per_unit <= Decimal::ZERO {
        return fallback;
    }

    // Each side gets ~2 legs of size S, so per-side score ≈ 2 * S * score_per_unit
    // needed_q = 2 * S * score_per_unit => S = needed_q / (2 * score_per_unit)
    let raw_size = needed_q / (Decimal::from(2) * score_per_unit);

    let clamped = raw_size.max(min_size).min(max_size);

    debug!(
        comp_total = %comp_total,
        adjusted_comp = %adjusted_comp,
        needed_q = %needed_q,
        score_per_unit = %score_per_unit,
        raw_size = %raw_size,
        clamped = %clamped,
        "Dynamic size computed"
    );

    clamped.round_dp(0)
}

fn fallback_result(min_share: Decimal) -> ScoreProxyResult {
    ScoreProxyResult {
        estimated_share: min_share,
        our_total_score: Decimal::ZERO,
        competitor_total_approx: Decimal::ZERO,
        is_extreme_market: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use rust_decimal_macros::dec;

    use crate::models::{QuoteCandidate, QuoteStatus};

    fn sample_book(token_id: &str) -> OrderBookSnapshot {
        OrderBookSnapshot {
            token_id: token_id.to_string(),
            exchange_ts: None,
            ingest_ts: Utc::now(),
            bids: vec![PriceLevel {
                price: dec!(0.45),
                size: dec!(100),
            }],
            asks: vec![PriceLevel {
                price: dec!(0.55),
                size: dec!(100),
            }],
        }
    }

    fn sample_reward_config() -> RewardConfig {
        RewardConfig {
            condition_id: "market".to_string(),
            daily_reward_rates: vec![dec!(5)],
            daily_reward_total: dec!(10),
            min_size: dec!(20),
            max_spread: dec!(0.10),
        }
    }

    fn sample_proxy_config() -> ScoreProxyConfig {
        ScoreProxyConfig {
            competition_multiplier: dec!(1.5),
            max_score_share: dec!(0.25),
            min_score_share: dec!(0.0001),
            target_score_share: dec!(0.03),
            calibration_sample_size: 10,
        }
    }

    #[test]
    fn compute_score_proxy_ignores_non_approved_candidates() {
        let yes_book = sample_book("yes");
        let no_book = sample_book("no");
        let reward_config = sample_reward_config();
        let proxy_config = sample_proxy_config();

        let approved_only = QuoteSet {
            condition_id: "market".to_string(),
            candidates: vec![QuoteCandidate {
                condition_id: "market".to_string(),
                leg: QuoteLeg::YesBid,
                price: dec!(0.45),
                size: dec!(20),
                status: QuoteStatus::Approved,
                reason: None,
            }],
        };

        let mixed_statuses = QuoteSet {
            condition_id: "market".to_string(),
            candidates: vec![
                approved_only.candidates[0].clone(),
                QuoteCandidate {
                    condition_id: "market".to_string(),
                    leg: QuoteLeg::NoBid,
                    price: dec!(0.45),
                    size: dec!(200),
                    status: QuoteStatus::Rejected,
                    reason: Some("not placeable".to_string()),
                },
                QuoteCandidate {
                    condition_id: "market".to_string(),
                    leg: QuoteLeg::YesAsk,
                    price: dec!(0.55),
                    size: dec!(200),
                    status: QuoteStatus::Suppressed,
                    reason: Some("no inventory".to_string()),
                },
            ],
        };

        let approved_score = compute_score_proxy(
            &approved_only,
            &yes_book,
            &no_book,
            &reward_config,
            &proxy_config,
        );
        let mixed_score = compute_score_proxy(
            &mixed_statuses,
            &yes_book,
            &no_book,
            &reward_config,
            &proxy_config,
        );

        assert_eq!(mixed_score.our_total_score, approved_score.our_total_score);
        assert_eq!(mixed_score.estimated_share, approved_score.estimated_share);
    }
}
