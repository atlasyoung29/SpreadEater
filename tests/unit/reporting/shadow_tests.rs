use rust_decimal_macros::dec;
use spreadeater::models::{HedgeabilityReport, QuoteLeg, QuoteSet, QuoteStatus, RewardViability};
use spreadeater::reporting::shadow::build_decision_report;
use spreadeater::strategy::ScoreProxyResult;

use super::super::helpers::{make_canonical_market, make_quote_candidate};

fn make_score_proxy() -> ScoreProxyResult {
    ScoreProxyResult {
        estimated_share: dec!(0.05),
        our_total_score: dec!(100),
        competitor_total_approx: dec!(1900),
        is_extreme_market: false,
    }
}

fn make_hedge_report(condition_id: &str, leg: QuoteLeg, approved: bool) -> HedgeabilityReport {
    HedgeabilityReport {
        condition_id: condition_id.to_string(),
        trigger_leg: leg,
        candidate_size: dec!(5),
        opposite_token_id: "opposite-token".to_string(),
        opposite_depth_available: dec!(50),
        max_hedgeable_size: dec!(5),
        weighted_avg_hedge_price: dec!(0.50),
        estimated_hedge_cost: dec!(0.01),
        slippage_bps: dec!(10),
        is_approved: approved,
        rejection_reason: if approved {
            None
        } else {
            Some("insufficient depth".to_string())
        },
    }
}

#[test]
fn all_approved_would_trade() {
    let market = make_canonical_market("cond-1");
    let quote_set = QuoteSet {
        condition_id: "cond-1".to_string(),
        candidates: vec![
            make_quote_candidate("cond-1", QuoteLeg::YesBid, 0.48, 5.0, QuoteStatus::Approved),
            make_quote_candidate("cond-1", QuoteLeg::NoBid, 0.48, 5.0, QuoteStatus::Approved),
        ],
    };
    let hedge_reports = vec![
        make_hedge_report("cond-1", QuoteLeg::YesBid, true),
        make_hedge_report("cond-1", QuoteLeg::NoBid, true),
    ];
    let score = make_score_proxy();

    let report = build_decision_report(&market, &quote_set, &hedge_reports, None, true, &score);

    assert!(report.would_trade, "All approved + viable should trade");
}

#[test]
fn no_approved_would_not_trade() {
    let market = make_canonical_market("cond-2");
    let quote_set = QuoteSet {
        condition_id: "cond-2".to_string(),
        candidates: vec![
            make_quote_candidate("cond-2", QuoteLeg::YesBid, 0.48, 5.0, QuoteStatus::Rejected),
            make_quote_candidate("cond-2", QuoteLeg::NoBid, 0.48, 5.0, QuoteStatus::Rejected),
        ],
    };
    let hedge_reports = vec![
        make_hedge_report("cond-2", QuoteLeg::YesBid, false),
        make_hedge_report("cond-2", QuoteLeg::NoBid, false),
    ];
    let score = make_score_proxy();

    let report = build_decision_report(&market, &quote_set, &hedge_reports, None, true, &score);

    assert!(!report.would_trade, "No approved legs should not trade");
}

#[test]
fn collects_rejection_reasons() {
    let market = make_canonical_market("cond-3");
    let mut candidate =
        make_quote_candidate("cond-3", QuoteLeg::YesBid, 0.48, 5.0, QuoteStatus::Rejected);
    candidate.reason = Some("spread too wide".to_string());

    let quote_set = QuoteSet {
        condition_id: "cond-3".to_string(),
        candidates: vec![candidate],
    };
    let hedge_reports = vec![make_hedge_report("cond-3", QuoteLeg::YesBid, false)];
    let score = make_score_proxy();

    let report = build_decision_report(&market, &quote_set, &hedge_reports, None, true, &score);

    assert!(
        !report.reasons.is_empty(),
        "Rejected legs should populate reasons"
    );
    let joined = report.reasons.join(" ");
    assert!(
        joined.contains("spread too wide"),
        "Reasons should include quote rejection: {}",
        joined
    );
}

#[test]
fn not_viable_would_not_trade() {
    let market = make_canonical_market("cond-4");
    let quote_set = QuoteSet {
        condition_id: "cond-4".to_string(),
        candidates: vec![make_quote_candidate(
            "cond-4",
            QuoteLeg::YesBid,
            0.48,
            5.0,
            QuoteStatus::Approved,
        )],
    };
    let hedge_reports = vec![make_hedge_report("cond-4", QuoteLeg::YesBid, true)];
    let score = make_score_proxy();

    let report = build_decision_report(
        &market,
        &quote_set,
        &hedge_reports,
        None,
        false, // not viable
        &score,
    );

    assert!(
        !report.would_trade,
        "Non-viable market should not trade even with approved legs"
    );
}

#[test]
fn effective_quote_size_from_approved_bids() {
    let market = make_canonical_market("cond-5");
    let quote_set = QuoteSet {
        condition_id: "cond-5".to_string(),
        candidates: vec![
            make_quote_candidate("cond-5", QuoteLeg::YesBid, 0.48, 7.0, QuoteStatus::Approved),
            make_quote_candidate("cond-5", QuoteLeg::NoBid, 0.48, 5.0, QuoteStatus::Approved),
        ],
    };
    let hedge_reports = vec![
        make_hedge_report("cond-5", QuoteLeg::YesBid, true),
        make_hedge_report("cond-5", QuoteLeg::NoBid, true),
    ];
    let score = make_score_proxy();

    let report = build_decision_report(&market, &quote_set, &hedge_reports, None, true, &score);

    assert_eq!(
        report.effective_quote_size,
        dec!(7),
        "Effective quote size should be max of approved bid sizes"
    );
}

#[test]
fn condition_id_matches_market() {
    let market = make_canonical_market("cond-6");
    let quote_set = QuoteSet {
        condition_id: "cond-6".to_string(),
        candidates: vec![],
    };
    let score = make_score_proxy();

    let report = build_decision_report(&market, &quote_set, &[], None, false, &score);

    assert_eq!(
        report.condition_id, market.condition_id,
        "Report condition_id should match market"
    );
}
