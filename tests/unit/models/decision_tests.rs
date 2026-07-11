use rust_decimal_macros::dec;
use spreadeater::models::*;

use super::super::helpers::*;

fn make_decision_report(candidates: Vec<QuoteCandidate>) -> DecisionReport {
    DecisionReport {
        condition_id: "cond-1".to_string(),
        market_slug: "test-market-cond-1".to_string(),
        question: "Will test pass?".to_string(),
        daily_reward_total: dec!(10),
        score_proxy: Some(dec!(0.05)),
        max_spread: dec!(0.04),
        effective_quote_size: dec!(5),
        candidate_quotes: candidates,
        reward_viability: None,
        would_trade: true,
        reasons: vec!["test reason".to_string()],
    }
}

// ── DecisionReport approved_count ─────────────────────────────────

#[test]
fn decision_report_approved_count_mixed() {
    let candidates = vec![
        make_quote_candidate("cond-1", QuoteLeg::YesBid, 0.48, 5.0, QuoteStatus::Approved),
        make_quote_candidate("cond-1", QuoteLeg::YesAsk, 0.52, 5.0, QuoteStatus::Approved),
        make_quote_candidate("cond-1", QuoteLeg::NoBid, 0.48, 5.0, QuoteStatus::Rejected),
        make_quote_candidate(
            "cond-1",
            QuoteLeg::NoAsk,
            0.52,
            5.0,
            QuoteStatus::Suppressed,
        ),
    ];
    let report = make_decision_report(candidates);
    assert_eq!(report.approved_count(), 2);
}

#[test]
fn decision_report_approved_count_none() {
    let candidates = vec![
        make_quote_candidate("cond-1", QuoteLeg::YesBid, 0.48, 5.0, QuoteStatus::Rejected),
        make_quote_candidate("cond-1", QuoteLeg::YesAsk, 0.52, 5.0, QuoteStatus::Rejected),
    ];
    let report = make_decision_report(candidates);
    assert_eq!(report.approved_count(), 0);
}

// ── DecisionReport serde ──────────────────────────────────────────

#[test]
fn decision_report_serde_roundtrip() {
    let candidates = vec![
        make_quote_candidate("cond-1", QuoteLeg::YesBid, 0.48, 5.0, QuoteStatus::Approved),
        make_quote_candidate("cond-1", QuoteLeg::YesAsk, 0.52, 5.0, QuoteStatus::Rejected),
    ];
    let report = make_decision_report(candidates);
    let json = serde_json::to_string(&report).expect("serialize DecisionReport");
    let back: DecisionReport = serde_json::from_str(&json).expect("deserialize DecisionReport");
    assert_eq!(back.condition_id, "cond-1");
    assert_eq!(back.daily_reward_total, dec!(10));
    assert_eq!(back.candidate_quotes.len(), 2);
    assert_eq!(back.approved_count(), 1);
    assert!(back.would_trade);
}

// ── RewardViability serde ─────────────────────────────────────────

#[test]
fn reward_viability_serde_roundtrip() {
    let rv = RewardViability {
        estimated_reward: dec!(5.0),
        estimated_hedge_cost: dec!(1.0),
        estimated_edge: dec!(4.0),
        score_share_approx: dec!(0.05),
        return_per_share: dec!(0.40),
    };
    let json = serde_json::to_string(&rv).expect("serialize RewardViability");
    let back: RewardViability = serde_json::from_str(&json).expect("deserialize RewardViability");
    assert_eq!(back.estimated_reward, rv.estimated_reward);
    assert_eq!(back.estimated_edge, rv.estimated_edge);
    assert_eq!(back.return_per_share, rv.return_per_share);
}

#[test]
fn reward_viability_alias_return_per_dollar() {
    let json = r#"{
        "estimated_reward": "1.0",
        "estimated_hedge_cost": "0.5",
        "estimated_edge": "0.5",
        "score_share_approx": "0.05",
        "return_per_dollar": "0.10"
    }"#;
    let rv: RewardViability = serde_json::from_str(json).unwrap();
    assert_eq!(rv.return_per_share, dec!(0.10));
}
