use rust_decimal_macros::dec;
use spreadeater::models::*;

use super::super::helpers::*;

// ── EnrichedDecisionReport serde roundtrip ────────────────────────

#[test]
fn enriched_decision_report_serde_roundtrip() {
    let report = DecisionReport {
        condition_id: "cond-1".to_string(),
        market_slug: "test-market-cond-1".to_string(),
        question: "Will test pass?".to_string(),
        daily_reward_total: dec!(10),
        score_proxy: Some(dec!(0.05)),
        max_spread: dec!(0.04),
        effective_quote_size: dec!(5),
        candidate_quotes: vec![make_quote_candidate(
            "cond-1",
            QuoteLeg::YesBid,
            0.48,
            5.0,
            QuoteStatus::Approved,
        )],
        reward_viability: None,
        would_trade: true,
        reasons: vec!["edge above threshold".to_string()],
    };

    let enriched = EnrichedDecisionReport {
        report,
        yes_book: make_orderbook_snapshot("cond-1-yes", vec![(0.48, 10.0)], vec![(0.52, 10.0)]),
        no_book: make_orderbook_snapshot("cond-1-no", vec![(0.48, 10.0)], vec![(0.52, 10.0)]),
        reward_config: make_reward_config("cond-1", 10.0),
    };

    let json = serde_json::to_string(&enriched).expect("serialize EnrichedDecisionReport");
    let back: EnrichedDecisionReport =
        serde_json::from_str(&json).expect("deserialize EnrichedDecisionReport");
    assert_eq!(back.report.condition_id, "cond-1");
    assert_eq!(back.report.would_trade, true);
    assert_eq!(back.yes_book.token_id, "cond-1-yes");
    assert_eq!(back.no_book.token_id, "cond-1-no");
    assert_eq!(
        back.reward_config.daily_reward_total,
        enriched.reward_config.daily_reward_total
    );
}

// ── EnrichedDecisionReport preserves nested fields ────────────────

#[test]
fn enriched_report_preserves_nested_fields() {
    let report = DecisionReport {
        condition_id: "cond-42".to_string(),
        market_slug: "test-market-cond-42".to_string(),
        question: "Will it rain?".to_string(),
        daily_reward_total: dec!(8),
        score_proxy: None,
        max_spread: dec!(0.04),
        effective_quote_size: dec!(5),
        candidate_quotes: vec![],
        reward_viability: None,
        would_trade: false,
        reasons: vec![],
    };

    let enriched = EnrichedDecisionReport {
        report,
        yes_book: make_orderbook_snapshot("cond-42-yes", vec![(0.48, 10.0)], vec![(0.52, 10.0)]),
        no_book: make_orderbook_snapshot("cond-42-no", vec![(0.50, 20.0)], vec![(0.51, 15.0)]),
        reward_config: make_reward_config("cond-42", 8.0),
    };

    let json = serde_json::to_string(&enriched).expect("serialize");
    let back: EnrichedDecisionReport = serde_json::from_str(&json).expect("deserialize");

    // Verify the inner report.condition_id survives the roundtrip
    assert_eq!(back.report.condition_id, "cond-42");
    assert_eq!(back.report.question, "Will it rain?");
    assert_eq!(back.reward_config.condition_id, "cond-42");
}
