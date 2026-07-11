use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use spreadeater::models::*;
use spreadeater::strategy::*;

use super::super::helpers::*;

// ---------------------------------------------------------------------------
// compute_quote_set
// ---------------------------------------------------------------------------

#[test]
fn generates_four_legs() {
    let market = make_canonical_market("c1");
    let config = make_strategy_config();
    let yes_book = make_orderbook_snapshot("c1-yes", vec![(0.48, 100.0)], vec![(0.52, 100.0)]);
    let no_book = make_orderbook_snapshot("c1-no", vec![(0.48, 100.0)], vec![(0.52, 100.0)]);

    let qs = compute_quote_set(&market, &yes_book, &no_book, &config, false, None);

    assert_eq!(qs.candidates.len(), 4, "should generate exactly 4 legs");
    assert!(qs.get_leg(QuoteLeg::YesBid).is_some());
    assert!(qs.get_leg(QuoteLeg::YesAsk).is_some());
    assert!(qs.get_leg(QuoteLeg::NoBid).is_some());
    assert!(qs.get_leg(QuoteLeg::NoAsk).is_some());
}

#[test]
fn min_outcome_price_suppresses_low_mid() {
    // min_outcome_price = 0.20 (from make_strategy_config)
    // Book with mid = (0.10 + 0.20) / 2 = 0.15 → below 0.20 → bid legs Rejected
    let market = make_canonical_market("c1");
    let config = make_strategy_config();
    let yes_book = make_orderbook_snapshot("c1-yes", vec![(0.10, 100.0)], vec![(0.20, 100.0)]);
    let no_book = make_orderbook_snapshot("c1-no", vec![(0.48, 100.0)], vec![(0.52, 100.0)]);

    let qs = compute_quote_set(&market, &yes_book, &no_book, &config, false, None);

    let yes_bid = qs.get_leg(QuoteLeg::YesBid).unwrap();
    assert_eq!(
        yes_bid.status,
        QuoteStatus::Rejected,
        "YesBid should be rejected when mid < min_outcome_price"
    );
    assert!(yes_bid
        .reason
        .as_ref()
        .unwrap()
        .contains("min_outcome_price"));
}

#[test]
fn bid_price_from_mid_and_depth() {
    // mid = (0.48 + 0.52) / 2 = 0.50
    // max_spread = 0.04 (from make_canonical_market reward_config)
    // floor = ask - max_spread = 0.52 - 0.04 = 0.48
    // range = mid - floor = 0.50 - 0.48 = 0.02
    // bid_depth_pct = 0.50 (from make_strategy_config)
    // target = mid - bid_depth_pct * range = 0.50 - 0.50 * 0.02 = 0.49
    // round_down_to_tick(0.49, 0.01) = 0.49
    let market = make_canonical_market("c1");
    let config = make_strategy_config();
    let yes_book = make_orderbook_snapshot("c1-yes", vec![(0.48, 100.0)], vec![(0.52, 100.0)]);
    let no_book = make_orderbook_snapshot("c1-no", vec![(0.48, 100.0)], vec![(0.52, 100.0)]);

    let qs = compute_quote_set(&market, &yes_book, &no_book, &config, false, None);

    let yes_bid = qs.get_leg(QuoteLeg::YesBid).unwrap();
    assert_eq!(yes_bid.status, QuoteStatus::Approved);
    assert_eq!(yes_bid.price, dec!(0.49), "bid price should be 0.49");
}

#[test]
fn shadow_mode_asks_simulated_only() {
    let market = make_canonical_market("c1");
    let config = make_strategy_config();
    let yes_book = make_orderbook_snapshot("c1-yes", vec![(0.48, 100.0)], vec![(0.52, 100.0)]);
    let no_book = make_orderbook_snapshot("c1-no", vec![(0.48, 100.0)], vec![(0.52, 100.0)]);

    let qs = compute_quote_set(&market, &yes_book, &no_book, &config, true, None);

    let yes_ask = qs.get_leg(QuoteLeg::YesAsk).unwrap();
    let no_ask = qs.get_leg(QuoteLeg::NoAsk).unwrap();
    assert_eq!(
        yes_ask.status,
        QuoteStatus::SimulatedOnly,
        "YesAsk should be SimulatedOnly in shadow mode"
    );
    assert_eq!(
        no_ask.status,
        QuoteStatus::SimulatedOnly,
        "NoAsk should be SimulatedOnly in shadow mode"
    );
}

#[test]
fn live_mode_asks_approved() {
    let market = make_canonical_market("c1");
    let config = make_strategy_config();
    let yes_book = make_orderbook_snapshot("c1-yes", vec![(0.48, 100.0)], vec![(0.52, 100.0)]);
    let no_book = make_orderbook_snapshot("c1-no", vec![(0.48, 100.0)], vec![(0.52, 100.0)]);

    let qs = compute_quote_set(&market, &yes_book, &no_book, &config, false, None);

    let yes_ask = qs.get_leg(QuoteLeg::YesAsk).unwrap();
    let no_ask = qs.get_leg(QuoteLeg::NoAsk).unwrap();
    assert_eq!(
        yes_ask.status,
        QuoteStatus::Approved,
        "YesAsk should be Approved in live mode"
    );
    assert_eq!(
        no_ask.status,
        QuoteStatus::Approved,
        "NoAsk should be Approved in live mode"
    );
}

#[test]
fn empty_book_rejects_all() {
    let market = make_canonical_market("c1");
    let config = make_strategy_config();
    let yes_book = make_orderbook_snapshot("c1-yes", vec![], vec![]);
    let no_book = make_orderbook_snapshot("c1-no", vec![], vec![]);

    let qs = compute_quote_set(&market, &yes_book, &no_book, &config, false, None);

    assert_eq!(qs.candidates.len(), 4);
    for candidate in &qs.candidates {
        assert_eq!(
            candidate.status,
            QuoteStatus::Rejected,
            "all legs should be rejected with empty books, but {:?} was {:?}",
            candidate.leg,
            candidate.status
        );
    }
}

#[test]
fn size_override_used() {
    let market = make_canonical_market("c1");
    let config = make_strategy_config();
    let yes_book = make_orderbook_snapshot("c1-yes", vec![(0.48, 100.0)], vec![(0.52, 100.0)]);
    let no_book = make_orderbook_snapshot("c1-no", vec![(0.48, 100.0)], vec![(0.52, 100.0)]);

    let qs = compute_quote_set(&market, &yes_book, &no_book, &config, false, Some(dec!(20)));

    for candidate in &qs.candidates {
        assert_eq!(
            candidate.size,
            dec!(20),
            "all legs should use size_override=20, got {} for {:?}",
            candidate.size,
            candidate.leg
        );
    }
}

#[test]
fn effective_size_uses_min_size() {
    // make_canonical_market has reward_config.min_size = 5.0
    // make_strategy_config has default_quote_size = 5
    // effective_size = max(base_size, reward_config.min_size) = max(5, 5) = 5
    // If we override with size=3, effective = max(3, 5) = 5
    let market = make_canonical_market("c1");
    let config = make_strategy_config();
    let yes_book = make_orderbook_snapshot("c1-yes", vec![(0.48, 100.0)], vec![(0.52, 100.0)]);
    let no_book = make_orderbook_snapshot("c1-no", vec![(0.48, 100.0)], vec![(0.52, 100.0)]);

    let qs = compute_quote_set(&market, &yes_book, &no_book, &config, false, Some(dec!(3)));

    // base_size = 3, min_size = 5, effective = max(3, 5) = 5
    for candidate in &qs.candidates {
        assert_eq!(
            candidate.size,
            dec!(5),
            "effective size should be min_size=5 when override is below it, got {} for {:?}",
            candidate.size,
            candidate.leg
        );
    }
}
