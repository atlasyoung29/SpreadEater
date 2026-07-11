use rust_decimal_macros::dec;
use spreadeater::models::*;

// ── HedgeabilityReport serde roundtrip ────────────────────────────

#[test]
fn hedgeability_report_serde_roundtrip() {
    let hr = HedgeabilityReport {
        condition_id: "cond-1".to_string(),
        trigger_leg: QuoteLeg::YesBid,
        candidate_size: dec!(5),
        opposite_token_id: "cond-1-no".to_string(),
        opposite_depth_available: dec!(20),
        max_hedgeable_size: dec!(5),
        weighted_avg_hedge_price: dec!(0.52),
        estimated_hedge_cost: dec!(0.10),
        slippage_bps: dec!(40),
        is_approved: true,
        rejection_reason: None,
    };
    let json = serde_json::to_string(&hr).expect("serialize HedgeabilityReport");
    let back: HedgeabilityReport =
        serde_json::from_str(&json).expect("deserialize HedgeabilityReport");
    assert_eq!(back.condition_id, "cond-1");
    assert_eq!(back.trigger_leg, QuoteLeg::YesBid);
    assert_eq!(back.candidate_size, dec!(5));
    assert_eq!(back.opposite_token_id, "cond-1-no");
    assert_eq!(back.opposite_depth_available, dec!(20));
    assert_eq!(back.max_hedgeable_size, dec!(5));
    assert_eq!(back.weighted_avg_hedge_price, dec!(0.52));
    assert_eq!(back.estimated_hedge_cost, dec!(0.10));
    assert_eq!(back.slippage_bps, dec!(40));
    assert!(back.is_approved);
    assert!(back.rejection_reason.is_none());
}

// ── HedgeabilityReport approved fields ────────────────────────────

#[test]
fn hedgeability_report_approved_fields() {
    let hr = HedgeabilityReport {
        condition_id: "cond-2".to_string(),
        trigger_leg: QuoteLeg::NoBid,
        candidate_size: dec!(10),
        opposite_token_id: "cond-2-yes".to_string(),
        opposite_depth_available: dec!(50),
        max_hedgeable_size: dec!(10),
        weighted_avg_hedge_price: dec!(0.48),
        estimated_hedge_cost: dec!(0.05),
        slippage_bps: dec!(20),
        is_approved: true,
        rejection_reason: None,
    };
    assert!(hr.is_approved);
    assert!(hr.rejection_reason.is_none());
}
