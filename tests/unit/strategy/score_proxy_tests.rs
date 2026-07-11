use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use spreadeater::models::*;
use spreadeater::strategy::*;

use super::super::helpers::*;

// ---------------------------------------------------------------------------
// per_order_score
// ---------------------------------------------------------------------------

#[test]
fn per_order_score_zero_when_spread_exceeds_max() {
    // spread_to_mid=0.05, max_spread=0.04 → 0
    let score = per_order_score(dec!(0.04), dec!(0.05), dec!(10));
    assert_eq!(score, Decimal::ZERO);
}

#[test]
fn per_order_score_zero_when_max_spread_zero() {
    let score = per_order_score(dec!(0), dec!(0.02), dec!(10));
    assert_eq!(score, Decimal::ZERO);
}

#[test]
fn per_order_score_correct_calculation() {
    // max_spread=0.04, spread_to_mid=0.02, size=10
    // ((0.04-0.02)/0.04)^2 * 10 = (0.5)^2 * 10 = 0.25 * 10 = 2.5
    let score = per_order_score(dec!(0.04), dec!(0.02), dec!(10));
    assert_eq!(score, dec!(2.5));
}

#[test]
fn per_order_score_full_at_mid() {
    // spread_to_mid=0 → ((0.04)/0.04)^2 * size = 1 * size = size
    let score = per_order_score(dec!(0.04), dec!(0), dec!(10));
    assert_eq!(score, dec!(10));
}

// ---------------------------------------------------------------------------
// score_book_side
// ---------------------------------------------------------------------------

#[test]
fn score_book_side_bids() {
    // bids at [0.49, 0.48], mid=0.50, max_spread=0.04
    // bid 0.49: spread = 0.50 - 0.49 = 0.01, score = ((0.04-0.01)/0.04)^2 * size
    //   = (0.75)^2 * 10 = 0.5625 * 10 = 5.625
    // bid 0.48: spread = 0.50 - 0.48 = 0.02, score = ((0.04-0.02)/0.04)^2 * 10
    //   = (0.5)^2 * 10 = 0.25 * 10 = 2.5
    // total = 8.125
    let levels = vec![
        PriceLevel {
            price: dec!(0.49),
            size: dec!(10),
        },
        PriceLevel {
            price: dec!(0.48),
            size: dec!(10),
        },
    ];

    let total = score_book_side(&levels, dec!(0.50), dec!(0.04), true);

    assert_eq!(total, dec!(8.125));
}

#[test]
fn score_book_side_asks() {
    // asks at [0.51, 0.52], mid=0.50, max_spread=0.04
    // ask 0.51: spread = 0.51 - 0.50 = 0.01, score = ((0.04-0.01)/0.04)^2 * 10
    //   = (0.75)^2 * 10 = 5.625
    // ask 0.52: spread = 0.52 - 0.50 = 0.02, score = ((0.04-0.02)/0.04)^2 * 10
    //   = (0.5)^2 * 10 = 2.5
    // total = 8.125
    let levels = vec![
        PriceLevel {
            price: dec!(0.51),
            size: dec!(10),
        },
        PriceLevel {
            price: dec!(0.52),
            size: dec!(10),
        },
    ];

    let total = score_book_side(&levels, dec!(0.50), dec!(0.04), false);

    assert_eq!(total, dec!(8.125));
}

#[test]
fn score_book_side_empty() {
    let total = score_book_side(&[], dec!(0.50), dec!(0.04), true);
    assert_eq!(total, Decimal::ZERO);
}

// ---------------------------------------------------------------------------
// compute_score_proxy — share clamping
// ---------------------------------------------------------------------------

#[test]
fn score_share_clamped_min() {
    // Our score near zero, competitor huge → share clamped to min_score_share(0.0001)
    // Use books with heavy competitor depth but our quotes contribute nearly nothing.
    let config = make_strategy_config();
    let market = make_canonical_market("c1");

    // Large competitor depth: bids and asks with huge size close to mid
    let yes_book = make_orderbook_snapshot("c1-yes", vec![(0.49, 10000.0)], vec![(0.51, 10000.0)]);
    let no_book = make_orderbook_snapshot("c1-no", vec![(0.49, 10000.0)], vec![(0.51, 10000.0)]);

    // Our quote: tiny size far from mid (high spread, low score)
    let candidate = make_quote_candidate("c1", QuoteLeg::YesBid, 0.47, 1.0, QuoteStatus::Approved);
    let quote_set = QuoteSet {
        condition_id: "c1".to_string(),
        candidates: vec![candidate],
    };

    let result = compute_score_proxy(
        &quote_set,
        &yes_book,
        &no_book,
        &market.reward_config,
        &config.score_proxy,
    );

    assert_eq!(
        result.estimated_share,
        dec!(0.0001),
        "share should be clamped to min_score_share"
    );
}

#[test]
fn score_share_clamped_max() {
    // Our score huge, competitor zero → share clamped to max_score_share(0.25)
    // Use empty books so competitor score = 0, but we have a quote.
    // With empty books, mid() returns None, so compute_score_proxy falls back.
    // Instead: use books where competitor depth is zero but still has bid/ask for mid.
    // Actually: we need books that produce a mid but have zero competitor score.
    // Trick: competitor levels outside max_spread don't score.
    let config = make_strategy_config();
    let market = make_canonical_market("c1"); // max_spread = 0.04

    // Competitor at price far from mid (spread > max_spread → score = 0)
    let yes_book = make_orderbook_snapshot("c1-yes", vec![(0.40, 100.0)], vec![(0.60, 100.0)]);
    let no_book = make_orderbook_snapshot("c1-no", vec![(0.40, 100.0)], vec![(0.60, 100.0)]);
    // mid = (0.40+0.60)/2 = 0.50
    // competitor bid spread = 0.50 - 0.40 = 0.10 > max_spread 0.04 → score 0
    // competitor ask spread = 0.60 - 0.50 = 0.10 > max_spread 0.04 → score 0

    // Our quote: right at mid, max score
    let candidate =
        make_quote_candidate("c1", QuoteLeg::YesBid, 0.50, 100.0, QuoteStatus::Approved);
    let quote_set = QuoteSet {
        condition_id: "c1".to_string(),
        candidates: vec![candidate],
    };

    let result = compute_score_proxy(
        &quote_set,
        &yes_book,
        &no_book,
        &market.reward_config,
        &config.score_proxy,
    );

    assert_eq!(
        result.estimated_share,
        dec!(0.25),
        "share should be clamped to max_score_share"
    );
}
