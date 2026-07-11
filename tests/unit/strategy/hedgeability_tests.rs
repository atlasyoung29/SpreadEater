use rust_decimal_macros::dec;

use spreadeater::models::*;
use spreadeater::strategy::*;

use super::super::helpers::*;

// ---------------------------------------------------------------------------
// compute_hedgeability
// ---------------------------------------------------------------------------

#[test]
fn yes_bid_hedges_via_no_asks() {
    // YesBid → hedge by walking NO asks
    let candidate = make_quote_candidate("c1", QuoteLeg::YesBid, 0.48, 5.0, QuoteStatus::Approved);
    let yes_book = make_orderbook_snapshot("c1-yes", vec![(0.50, 10.0)], vec![(0.52, 10.0)]);
    let no_book = make_orderbook_snapshot("c1-no", vec![(0.50, 10.0)], vec![(0.51, 10.0)]);
    let config = make_strategy_config();

    let report = compute_hedgeability(&candidate, &yes_book, &no_book, &config);

    assert!(
        report.is_approved,
        "should be approved when NO asks have depth"
    );
    assert_eq!(report.trigger_leg, QuoteLeg::YesBid);
    assert_eq!(report.opposite_token_id, "c1-no");
    assert!(report.rejection_reason.is_none());
}

#[test]
fn yes_ask_hedges_via_no_bids() {
    // YesAsk → hedge by walking NO bids
    let candidate = make_quote_candidate("c1", QuoteLeg::YesAsk, 0.52, 5.0, QuoteStatus::Approved);
    let yes_book = make_orderbook_snapshot("c1-yes", vec![(0.50, 10.0)], vec![(0.52, 10.0)]);
    let no_book = make_orderbook_snapshot("c1-no", vec![(0.50, 10.0)], vec![(0.52, 10.0)]);
    let config = make_strategy_config();

    let report = compute_hedgeability(&candidate, &yes_book, &no_book, &config);

    assert!(
        report.is_approved,
        "should be approved when NO bids have depth"
    );
    assert_eq!(report.trigger_leg, QuoteLeg::YesAsk);
    assert_eq!(report.opposite_token_id, "c1-no");
}

#[test]
fn no_bid_hedges_via_yes_asks() {
    // NoBid → hedge by walking YES asks
    let candidate = make_quote_candidate("c1", QuoteLeg::NoBid, 0.48, 5.0, QuoteStatus::Approved);
    let yes_book = make_orderbook_snapshot("c1-yes", vec![(0.50, 10.0)], vec![(0.51, 10.0)]);
    let no_book = make_orderbook_snapshot("c1-no", vec![(0.50, 10.0)], vec![(0.52, 10.0)]);
    let config = make_strategy_config();

    let report = compute_hedgeability(&candidate, &yes_book, &no_book, &config);

    assert!(
        report.is_approved,
        "should be approved when YES asks have depth"
    );
    assert_eq!(report.trigger_leg, QuoteLeg::NoBid);
    assert_eq!(report.opposite_token_id, "c1-yes");
}

#[test]
fn no_ask_hedges_via_yes_bids() {
    // NoAsk → hedge by walking YES bids
    let candidate = make_quote_candidate("c1", QuoteLeg::NoAsk, 0.52, 5.0, QuoteStatus::Approved);
    let yes_book = make_orderbook_snapshot("c1-yes", vec![(0.50, 10.0)], vec![(0.52, 10.0)]);
    let no_book = make_orderbook_snapshot("c1-no", vec![(0.50, 10.0)], vec![(0.52, 10.0)]);
    let config = make_strategy_config();

    let report = compute_hedgeability(&candidate, &yes_book, &no_book, &config);

    assert!(
        report.is_approved,
        "should be approved when YES bids have depth"
    );
    assert_eq!(report.trigger_leg, QuoteLeg::NoAsk);
    assert_eq!(report.opposite_token_id, "c1-yes");
}

#[test]
fn rejected_insufficient_depth() {
    // YesBid size=20, NO asks only have size=5 → not fully filled → rejected
    let candidate = make_quote_candidate("c1", QuoteLeg::YesBid, 0.48, 20.0, QuoteStatus::Approved);
    let yes_book = make_orderbook_snapshot("c1-yes", vec![(0.50, 10.0)], vec![(0.52, 10.0)]);
    let no_book = make_orderbook_snapshot("c1-no", vec![(0.50, 10.0)], vec![(0.51, 5.0)]);
    let config = make_strategy_config();

    let report = compute_hedgeability(&candidate, &yes_book, &no_book, &config);

    assert!(
        !report.is_approved,
        "should be rejected when depth is insufficient"
    );
    assert!(
        report
            .rejection_reason
            .as_ref()
            .unwrap()
            .contains("Insufficient"),
        "reason should mention insufficient depth, got: {:?}",
        report.rejection_reason
    );
    assert_eq!(report.max_hedgeable_size, dec!(5));
}

#[test]
fn rejected_slippage_exceeds_max() {
    // NO asks at multiple levels with high price spread to produce slippage > 80 bps
    // Best ask at 0.50, second level at 0.60. Need size=10 to walk both levels.
    // Walk: 5 @ 0.50 + 5 @ 0.60 = 7.50/10 = 0.55 avg. Slippage = (0.55-0.50)/0.50 * 10000 = 1000 bps > 80
    let candidate = make_quote_candidate("c1", QuoteLeg::YesBid, 0.48, 10.0, QuoteStatus::Approved);
    let yes_book = make_orderbook_snapshot("c1-yes", vec![(0.50, 10.0)], vec![(0.52, 10.0)]);
    let no_book =
        make_orderbook_snapshot("c1-no", vec![(0.50, 10.0)], vec![(0.50, 5.0), (0.60, 5.0)]);
    let config = make_strategy_config();

    let report = compute_hedgeability(&candidate, &yes_book, &no_book, &config);

    assert!(
        !report.is_approved,
        "should be rejected when slippage exceeds max"
    );
    assert!(
        report
            .rejection_reason
            .as_ref()
            .unwrap()
            .contains("Slippage"),
        "reason should mention slippage, got: {:?}",
        report.rejection_reason
    );
}

#[test]
fn approved_at_exact_slippage_boundary() {
    // max_slippage_bps = 80. We need slippage exactly at 80 bps.
    // Best ask at 1.0000. avg price must be 1.0080 for exactly 80 bps.
    // Walk: size=100 across two levels. We want weighted avg = best * (1 + 80/10000) = 1.0000 * 1.0080
    // Level1: 1.0000 size=A, Level2: P size=B, A+B=100, (A*1.0 + B*P)/100 = 1.008
    // Let A=80, B=20, P = (100*1.008 - 80)/20 = (100.8-80)/20 = 20.8/20 = 1.04
    let candidate =
        make_quote_candidate("c1", QuoteLeg::YesBid, 0.48, 100.0, QuoteStatus::Approved);
    let yes_book = make_orderbook_snapshot("c1-yes", vec![(0.50, 100.0)], vec![(0.52, 100.0)]);
    let no_book = make_orderbook_snapshot(
        "c1-no",
        vec![(0.50, 100.0)],
        vec![(1.0, 80.0), (1.04, 20.0)],
    );
    let config = make_strategy_config();

    let report = compute_hedgeability(&candidate, &yes_book, &no_book, &config);

    // slippage = (1.008 - 1.0)/1.0 * 10000 = 80 bps exactly, should be approved (<= check)
    assert_eq!(report.slippage_bps, dec!(80));
    assert!(
        report.is_approved,
        "should be approved at exact slippage boundary (<=)"
    );
}

#[test]
fn empty_opposite_book() {
    // NO asks are empty → rejected
    let candidate = make_quote_candidate("c1", QuoteLeg::YesBid, 0.48, 5.0, QuoteStatus::Approved);
    let yes_book = make_orderbook_snapshot("c1-yes", vec![(0.50, 10.0)], vec![(0.52, 10.0)]);
    let no_book = make_orderbook_snapshot("c1-no", vec![(0.50, 10.0)], vec![]);
    let config = make_strategy_config();

    let report = compute_hedgeability(&candidate, &yes_book, &no_book, &config);

    assert!(
        !report.is_approved,
        "should be rejected when opposite book is empty"
    );
    assert_eq!(report.max_hedgeable_size, dec!(0));
}

#[test]
fn thin_book_partial_fill() {
    // NO asks have 3 of 5 needed → partial fill → rejected
    let candidate = make_quote_candidate("c1", QuoteLeg::YesBid, 0.48, 5.0, QuoteStatus::Approved);
    let yes_book = make_orderbook_snapshot("c1-yes", vec![(0.50, 10.0)], vec![(0.52, 10.0)]);
    let no_book = make_orderbook_snapshot("c1-no", vec![(0.50, 10.0)], vec![(0.51, 3.0)]);
    let config = make_strategy_config();

    let report = compute_hedgeability(&candidate, &yes_book, &no_book, &config);

    assert!(
        !report.is_approved,
        "should be rejected with only partial fill"
    );
    assert_eq!(report.max_hedgeable_size, dec!(3));
    assert!(report
        .rejection_reason
        .as_ref()
        .unwrap()
        .contains("Insufficient"));
}

// ---------------------------------------------------------------------------
// apply_hedgeability_gate
// ---------------------------------------------------------------------------

#[test]
fn apply_gate_sets_rejected() {
    let mut candidate =
        make_quote_candidate("c1", QuoteLeg::YesBid, 0.48, 5.0, QuoteStatus::Approved);
    let report = HedgeabilityReport {
        condition_id: "c1".to_string(),
        trigger_leg: QuoteLeg::YesBid,
        candidate_size: dec!(5),
        opposite_token_id: "c1-no".to_string(),
        opposite_depth_available: dec!(2),
        max_hedgeable_size: dec!(2),
        weighted_avg_hedge_price: dec!(0.51),
        estimated_hedge_cost: dec!(0.02),
        slippage_bps: dec!(10),
        is_approved: false,
        rejection_reason: Some("Insufficient opposite depth: need 5, available 2".to_string()),
    };

    apply_hedgeability_gate(&mut candidate, &report);

    assert_eq!(candidate.status, QuoteStatus::Rejected);
    assert_eq!(
        candidate.reason.as_deref(),
        Some("Insufficient opposite depth: need 5, available 2")
    );
}

#[test]
fn apply_gate_preserves_already_rejected() {
    let mut candidate =
        make_quote_candidate("c1", QuoteLeg::YesBid, 0.48, 5.0, QuoteStatus::Rejected);
    candidate.reason = Some("original reason".to_string());

    let report = HedgeabilityReport {
        condition_id: "c1".to_string(),
        trigger_leg: QuoteLeg::YesBid,
        candidate_size: dec!(5),
        opposite_token_id: "c1-no".to_string(),
        opposite_depth_available: dec!(10),
        max_hedgeable_size: dec!(5),
        weighted_avg_hedge_price: dec!(0.51),
        estimated_hedge_cost: dec!(0.01),
        slippage_bps: dec!(5),
        is_approved: false,
        rejection_reason: Some("some other reason".to_string()),
    };

    apply_hedgeability_gate(&mut candidate, &report);

    assert_eq!(candidate.status, QuoteStatus::Rejected);
    assert_eq!(
        candidate.reason.as_deref(),
        Some("original reason"),
        "should preserve the original rejection reason"
    );
}
