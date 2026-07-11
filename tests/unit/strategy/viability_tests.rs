use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use spreadeater::models::*;
use spreadeater::strategy::*;

use super::super::helpers::*;

// ---------------------------------------------------------------------------
// Helpers local to this test module
// ---------------------------------------------------------------------------

fn make_score_proxy(share: Decimal) -> ScoreProxyResult {
    ScoreProxyResult {
        estimated_share: share,
        our_total_score: dec!(100),
        competitor_total_approx: dec!(900),
        is_extreme_market: false,
    }
}

fn approved_bid(condition_id: &str, price: f64, size: f64) -> QuoteCandidate {
    make_quote_candidate(
        condition_id,
        QuoteLeg::YesBid,
        price,
        size,
        QuoteStatus::Approved,
    )
}

fn hedge_report_approved(candidate: &QuoteCandidate, hedge_price: Decimal) -> HedgeabilityReport {
    HedgeabilityReport {
        condition_id: candidate.condition_id.clone(),
        trigger_leg: candidate.leg,
        candidate_size: candidate.size,
        opposite_token_id: format!("{}-no", candidate.condition_id),
        opposite_depth_available: candidate.size,
        max_hedgeable_size: candidate.size,
        weighted_avg_hedge_price: hedge_price,
        estimated_hedge_cost: (candidate.price + hedge_price - Decimal::ONE) * candidate.size,
        slippage_bps: dec!(10),
        is_approved: true,
        rejection_reason: None,
    }
}

// ---------------------------------------------------------------------------
// compute_viability
// ---------------------------------------------------------------------------

#[test]
fn viable_when_return_exceeds_threshold() {
    // daily_reward_total=10, share=0.10, discount=0.70 → reward = 10*0.10*0.70 = 0.70
    // bid=0.48, hedge=0.51 → hedge_cost = (0.48+0.51-1)*5 = -0.05 (negative = profit)
    // edge = 0.70 - (-0.05) = 0.75
    // return_per_share = 0.75 / 5 = 0.15, well above min_return_pct=0.0025
    let market = make_canonical_market("c1");
    let config = make_strategy_config();
    let proxy = make_score_proxy(dec!(0.10));

    let bid = approved_bid("c1", 0.48, 5.0);
    let quote_set = QuoteSet {
        condition_id: "c1".to_string(),
        candidates: vec![bid.clone()],
    };
    let reports = vec![hedge_report_approved(&bid, dec!(0.51))];

    let (viability, is_viable) =
        compute_viability(&market, &quote_set, &reports, &config, &proxy, dec!(5));

    assert!(is_viable, "should be viable when return exceeds threshold");
    assert!(
        viability.return_per_share >= config.min_return_pct,
        "return_per_share {} should be >= min_return_pct {}",
        viability.return_per_share,
        config.min_return_pct
    );
}

#[test]
fn not_viable_when_return_below_threshold() {
    // Use tiny share so reward is tiny → return below threshold.
    // daily=10, share=0.0001, discount=0.70 → reward = 10*0.0001*0.70 = 0.0007
    // bid=0.50, hedge=0.51 → hedge_cost = (0.50+0.51-1)*5 = 0.05
    // edge = 0.0007 - 0.05 = -0.0493 (negative)
    // return_per_share = -0.0493 / 5 = -0.00986 < 0.0025
    let market = make_canonical_market("c1");
    let config = make_strategy_config();
    let proxy = make_score_proxy(dec!(0.0001));

    let bid = approved_bid("c1", 0.50, 5.0);
    let quote_set = QuoteSet {
        condition_id: "c1".to_string(),
        candidates: vec![bid.clone()],
    };
    let reports = vec![hedge_report_approved(&bid, dec!(0.51))];

    let (_viability, is_viable) =
        compute_viability(&market, &quote_set, &reports, &config, &proxy, dec!(5));

    assert!(
        !is_viable,
        "should not be viable when return is below threshold"
    );
}

#[test]
fn zero_score_share_yields_zero_reward() {
    let market = make_canonical_market("c1");
    let config = make_strategy_config();
    let proxy = make_score_proxy(dec!(0));

    let bid = approved_bid("c1", 0.50, 5.0);
    let quote_set = QuoteSet {
        condition_id: "c1".to_string(),
        candidates: vec![bid.clone()],
    };
    let reports = vec![hedge_report_approved(&bid, dec!(0.51))];

    let (viability, _) = compute_viability(&market, &quote_set, &reports, &config, &proxy, dec!(5));

    assert_eq!(
        viability.estimated_reward,
        dec!(0),
        "zero share should yield zero reward"
    );
}

#[test]
fn negative_hedge_cost_increases_edge() {
    // hedge price < 1.0 - quote_price → negative cost → edge > reward alone
    // bid=0.45, hedge=0.50 → pair cost = 0.45+0.50-1 = -0.05 per unit
    // hedge_cost = -0.05 * 5 = -0.25
    // reward = 10 * 0.10 * 0.70 = 0.70
    // edge = 0.70 - (-0.25) = 0.95 > 0.70
    let market = make_canonical_market("c1");
    let config = make_strategy_config();
    let proxy = make_score_proxy(dec!(0.10));

    let bid = approved_bid("c1", 0.45, 5.0);
    let quote_set = QuoteSet {
        condition_id: "c1".to_string(),
        candidates: vec![bid.clone()],
    };
    let reports = vec![hedge_report_approved(&bid, dec!(0.50))];

    let (viability, _) = compute_viability(&market, &quote_set, &reports, &config, &proxy, dec!(5));

    assert!(
        viability.estimated_edge > viability.estimated_reward,
        "negative hedge cost should make edge ({}) > reward ({})",
        viability.estimated_edge,
        viability.estimated_reward
    );
    assert!(
        viability.estimated_hedge_cost < dec!(0),
        "hedge cost should be negative, got {}",
        viability.estimated_hedge_cost
    );
}

// ---------------------------------------------------------------------------
// compute_reward_per_share_ranking_metric
// ---------------------------------------------------------------------------

#[test]
fn ranking_metric_divides_reward_by_committed_shares() {
    // 2 approved bids of size 5 each → committed = 10, reward = 1 → metric = 1/10 = 0.1
    let bid1 = make_quote_candidate("c1", QuoteLeg::YesBid, 0.48, 5.0, QuoteStatus::Approved);
    let bid2 = make_quote_candidate("c1", QuoteLeg::NoBid, 0.48, 5.0, QuoteStatus::Approved);
    let quote_set = QuoteSet {
        condition_id: "c1".to_string(),
        candidates: vec![bid1, bid2],
    };

    let metric = compute_reward_per_share_ranking_metric(&quote_set, dec!(1), dec!(5));

    assert_eq!(metric, dec!(0.1), "reward 1 / committed 10 = 0.1");
}

#[test]
fn ranking_metric_falls_back_to_effective_size() {
    // No approved bids → falls back to effective_quote_size
    let ask = make_quote_candidate("c1", QuoteLeg::YesAsk, 0.52, 5.0, QuoteStatus::Approved);
    let quote_set = QuoteSet {
        condition_id: "c1".to_string(),
        candidates: vec![ask],
    };

    let metric = compute_reward_per_share_ranking_metric(&quote_set, dec!(1), dec!(10));

    // No approved bids, so committed_shares = max(effective_quote_size, 1) = 10
    assert_eq!(metric, dec!(0.1), "reward 1 / effective 10 = 0.1");
}
