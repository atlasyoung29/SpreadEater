use rust_decimal::Decimal;
use tracing::debug;

use super::score_proxy::ScoreProxyResult;
use crate::config::StrategyConfig;
use crate::models::{CanonicalMarket, HedgeabilityReport, QuoteSet, RewardViability};

/// Estimate reward viability for a market's quote set.
///
/// Uses the score proxy to estimate our share of daily rewards based on
/// the Polymarket scoring formula and visible book competition.
/// All estimates are approximate — no aggregate market-maker score
/// endpoint exists in the documented API.
///
/// Model:
/// - estimated_reward = daily_reward_total * score_proxy.estimated_share * discount_factor
/// - estimated_hedge_cost = sum of hedge costs across approved legs
/// - estimated_edge = estimated_reward - estimated_hedge_cost
/// - return_per_share = estimated_edge / shares_committed  (hedge-aware: $1/share)
fn committed_shares_for_quotes(quote_set: &QuoteSet, effective_quote_size: Decimal) -> Decimal {
    let bid_shares: Decimal = quote_set
        .candidates
        .iter()
        .filter(|c| c.status == crate::models::QuoteStatus::Approved && c.leg.is_bid())
        .map(|c| c.size)
        .sum();

    if bid_shares > Decimal::ZERO {
        bid_shares
    } else {
        effective_quote_size
    }
    .max(Decimal::ONE)
}

/// Ranking-only metric: discounted reward per hedge-aware share.
/// Unlike `return_per_share`, this intentionally excludes hedge economics
/// from the numerator so favorable pair pricing does not boost ordering.
pub fn compute_reward_per_share_ranking_metric(
    quote_set: &QuoteSet,
    estimated_reward: Decimal,
    effective_quote_size: Decimal,
) -> Decimal {
    estimated_reward / committed_shares_for_quotes(quote_set, effective_quote_size)
}

pub fn compute_viability(
    market: &CanonicalMarket,
    quote_set: &QuoteSet,
    hedge_reports: &[HedgeabilityReport],
    config: &StrategyConfig,
    score_proxy: &ScoreProxyResult,
    effective_quote_size: Decimal,
) -> (RewardViability, bool) {
    let daily_reward = market.reward_config.daily_reward_total;
    let score_share = score_proxy.estimated_share;
    let estimated_reward = daily_reward * score_share * config.reward_discount_factor;

    // Position cost for approved BID legs: entry_price + hedge_price - 1.00 per unit.
    // Positive = we pay more than $1.00 for the hedged pair (reward must cover it).
    // Negative = locked-in profit (pair costs less than $1.00 payout).
    // ASK fills sell existing inventory — no new position cost.
    let estimated_hedge_cost: Decimal = quote_set
        .candidates
        .iter()
        .zip(hedge_reports.iter())
        .filter(|(c, r)| {
            c.status == crate::models::QuoteStatus::Approved
                && r.is_approved
                && c.leg.hedge_uses_asks()
        })
        .map(|(c, r)| (c.price + r.weighted_avg_hedge_price - Decimal::ONE) * r.candidate_size)
        .sum();

    let estimated_edge = estimated_reward - estimated_hedge_cost;

    // Capital committed = sum of size for approved bid legs (hedge-aware: $1/share).
    // In binary markets, bid + hedge ≈ $1, so shares ≈ true account capacity consumed.
    // Falls back to effective_quote_size if no approved bids (e.g. ask-only).
    let capital = committed_shares_for_quotes(quote_set, effective_quote_size);
    let return_pct = estimated_edge / capital;
    let is_viable = return_pct >= config.min_return_pct;

    debug!(
        condition_id = %market.condition_id,
        estimated_edge = %estimated_edge,
        return_pct = %return_pct,
        viable = is_viable,
        "Viability computed"
    );

    let viability = RewardViability {
        estimated_reward,
        estimated_hedge_cost,
        estimated_edge,
        score_share_approx: score_share,
        return_per_share: return_pct,
    };

    (viability, is_viable)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ScoreProxyConfig;
    use crate::models::{MarketStatus, QuoteCandidate, QuoteLeg, QuoteStatus, RewardConfig};
    use chrono::Utc;
    use rust_decimal_macros::dec;

    fn test_market(daily_reward: Decimal) -> CanonicalMarket {
        CanonicalMarket {
            condition_id: "test-cid".to_string(),
            market_slug: "test-slug".to_string(),
            question: "test?".to_string(),
            yes_token_id: "yes-tok".to_string(),
            no_token_id: "no-tok".to_string(),
            reward_config: RewardConfig {
                condition_id: "test-cid".to_string(),
                daily_reward_rates: vec![daily_reward],
                daily_reward_total: daily_reward,
                min_size: dec!(5),
                max_spread: dec!(0.10),
            },
            neg_risk: false,
            tick_size: "0.01".to_string(),
            end_date: None,
            admitted_at: Utc::now(),
            status: MarketStatus::Admitted,
        }
    }

    fn test_strategy_config() -> StrategyConfig {
        StrategyConfig {
            max_hedge_cost_bps: dec!(80),
            max_slippage_bps: dec!(80),
            default_quote_size: dec!(5),
            min_edge_threshold: dec!(0.50),
            quote_drift_bps: dec!(30),
            bid_depth_pct: dec!(0.50),
            ask_depth_pct: dec!(0.20),
            quote_refresh_secs: 5,
            min_est_daily: dec!(0.25),
            min_return_pct: dec!(0.0025),
            min_outcome_price: dec!(0.20),
            reward_discount_factor: dec!(0.70),
            min_frontier_improvement: dec!(0.05),
            frontier_handoff_window_secs: 5,
            score_proxy: ScoreProxyConfig {
                competition_multiplier: dec!(1.5),
                max_score_share: dec!(0.25),
                min_score_share: dec!(0.0001),
                target_score_share: dec!(0.03),
                calibration_sample_size: 10,
            },
        }
    }

    fn test_score_proxy(share: Decimal) -> ScoreProxyResult {
        ScoreProxyResult {
            estimated_share: share,
            our_total_score: dec!(100),
            competitor_total_approx: dec!(900),
            is_extreme_market: false,
        }
    }

    fn bid_candidate(price: Decimal, size: Decimal) -> QuoteCandidate {
        QuoteCandidate {
            condition_id: "test-cid".to_string(),
            leg: QuoteLeg::YesBid,
            price,
            size,
            status: QuoteStatus::Approved,
            reason: None,
        }
    }

    fn hedge_report_for(candidate: &QuoteCandidate, hedge_price: Decimal) -> HedgeabilityReport {
        HedgeabilityReport {
            condition_id: candidate.condition_id.clone(),
            trigger_leg: candidate.leg,
            candidate_size: candidate.size,
            opposite_token_id: "no-tok".to_string(),
            opposite_depth_available: candidate.size,
            max_hedgeable_size: candidate.size,
            weighted_avg_hedge_price: hedge_price,
            estimated_hedge_cost: (candidate.price + hedge_price - Decimal::ONE) * candidate.size,
            slippage_bps: dec!(10),
            is_approved: true,
            rejection_reason: None,
        }
    }

    /// Test A: Same share size, different bid prices → same return_per_share.
    /// In the per-share model, the denominator is shares committed (not price × size),
    /// so two bids with the same size but different prices yield equal return_per_share
    /// when hedge costs net to the same amount.
    #[test]
    fn same_size_same_hedge_cost_yields_same_per_share_return() {
        let market = test_market(dec!(100)); // $100/day pool
        let config = test_strategy_config();
        let proxy = test_score_proxy(dec!(0.10)); // 10% share → $7 discounted

        // Market at $0.50 bid, hedge at $0.51 → hedge cost = (0.50+0.51-1)*50 = $0.50
        let cheap_bid = bid_candidate(dec!(0.50), dec!(50));
        let cheap_qs = QuoteSet {
            condition_id: "test-cid".to_string(),
            candidates: vec![cheap_bid.clone()],
        };
        let cheap_hr = vec![hedge_report_for(&cheap_bid, dec!(0.51))];
        let (cheap_v, _) =
            compute_viability(&market, &cheap_qs, &cheap_hr, &config, &proxy, dec!(50));

        // Market at $0.80 bid, hedge at $0.21 → hedge cost = (0.80+0.21-1)*50 = $0.50
        let expensive_bid = bid_candidate(dec!(0.80), dec!(50));
        let expensive_qs = QuoteSet {
            condition_id: "test-cid".to_string(),
            candidates: vec![expensive_bid.clone()],
        };
        let expensive_hr = vec![hedge_report_for(&expensive_bid, dec!(0.21))];
        let (expensive_v, _) = compute_viability(
            &market,
            &expensive_qs,
            &expensive_hr,
            &config,
            &proxy,
            dec!(50),
        );

        // Same size, same hedge cost → same per-share return
        assert_eq!(
            cheap_v.return_per_share, expensive_v.return_per_share,
            "same size + same hedge cost should yield equal per-share return"
        );
    }

    /// Test B: Different sizes yield different per-share returns, demonstrating ranking.
    /// Market A deploys more shares with same reward → lower return per share.
    #[test]
    fn larger_size_yields_lower_per_share_return() {
        let config = test_strategy_config();

        // Market A: $200/day pool, 5% share → $7 discounted. Size 200 shares.
        let market_a = test_market(dec!(200));
        let proxy_a = test_score_proxy(dec!(0.05));
        let bid_a = bid_candidate(dec!(0.50), dec!(200));
        let qs_a = QuoteSet {
            condition_id: "test-cid".to_string(),
            candidates: vec![bid_a.clone()],
        };
        let hr_a = vec![hedge_report_for(&bid_a, dec!(0.51))];
        let (v_a, _) = compute_viability(&market_a, &qs_a, &hr_a, &config, &proxy_a, dec!(200));

        // Market B: $100/day pool, 10% share → $7 discounted. Size 50 shares.
        let market_b = test_market(dec!(100));
        let proxy_b = test_score_proxy(dec!(0.10));
        let bid_b = bid_candidate(dec!(0.50), dec!(50));
        let qs_b = QuoteSet {
            condition_id: "test-cid".to_string(),
            candidates: vec![bid_b.clone()],
        };
        let hr_b = vec![hedge_report_for(&bid_b, dec!(0.51))];
        let (v_b, _) = compute_viability(&market_b, &qs_b, &hr_b, &config, &proxy_b, dec!(50));

        // Same absolute reward ($7), but B uses fewer shares → higher per-share return
        assert_eq!(
            v_a.estimated_reward, v_b.estimated_reward,
            "same absolute reward"
        );
        assert!(
            v_b.return_per_share > v_a.return_per_share,
            "Market B (50 shares) should rank higher than Market A (200 shares), got B={} A={}",
            v_b.return_per_share,
            v_a.return_per_share
        );
    }

    /// Test C: Verify estimated_reward includes the discount factor.
    #[test]
    fn estimated_reward_includes_discount_factor() {
        let market = test_market(dec!(100));
        let config = test_strategy_config(); // discount = 0.70
        let proxy = test_score_proxy(dec!(0.10)); // 10% share

        let bid = bid_candidate(dec!(0.50), dec!(50));
        let qs = QuoteSet {
            condition_id: "test-cid".to_string(),
            candidates: vec![bid.clone()],
        };
        let hr = vec![hedge_report_for(&bid, dec!(0.51))];
        let (v, _) = compute_viability(&market, &qs, &hr, &config, &proxy, dec!(50));

        // 100 * 0.10 * 0.70 = 7.00
        assert_eq!(
            v.estimated_reward,
            dec!(7.00),
            "reward should be daily * share * discount"
        );
    }

    #[test]
    fn reward_per_share_ranking_ignores_hedge_economics_when_reward_and_size_match() {
        let quote_set = QuoteSet {
            condition_id: "test-cid".to_string(),
            candidates: vec![bid_candidate(dec!(0.50), dec!(50))],
        };

        let favorable = compute_reward_per_share_ranking_metric(&quote_set, dec!(7.00), dec!(50));
        let unfavorable = compute_reward_per_share_ranking_metric(&quote_set, dec!(7.00), dec!(50));

        assert_eq!(favorable, unfavorable);
    }

    #[test]
    fn higher_reward_market_outranks_more_favorable_pair_when_size_matches() {
        let quote_set = QuoteSet {
            condition_id: "test-cid".to_string(),
            candidates: vec![bid_candidate(dec!(0.50), dec!(373))],
        };

        let gavin_like =
            compute_reward_per_share_ranking_metric(&quote_set, dec!(0.0166), dec!(373));
        let fed_like = compute_reward_per_share_ranking_metric(&quote_set, dec!(0.8432), dec!(373));

        assert!(fed_like > gavin_like);
    }
}
