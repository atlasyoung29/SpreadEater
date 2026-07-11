use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use spreadeater::models::{PriceLevel, QuoteLeg, Side};
use spreadeater::trading::hedge_executor::{
    compute_hedge_resolution, normalize_share_size, plan_fill_resolution, HedgeExecutor,
    HedgeIntent, HedgeResolution, HedgeResult, HedgeVerificationState,
};

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

fn pl(price: Decimal, size: Decimal) -> PriceLevel {
    PriceLevel { price, size }
}

const TICK: Decimal = dec!(0.01);

// ---------------------------------------------------------------------------
// compute_hedge_params
// ---------------------------------------------------------------------------

#[test]
fn compute_hedge_params_yes_bid() {
    let (token, side) = HedgeExecutor::compute_hedge_params(QuoteLeg::YesBid, "yes-tok", "no-tok");
    assert_eq!(token, "no-tok");
    assert_eq!(side, Side::Buy);
}

#[test]
fn compute_hedge_params_yes_ask() {
    let (token, side) = HedgeExecutor::compute_hedge_params(QuoteLeg::YesAsk, "yes-tok", "no-tok");
    assert_eq!(token, "no-tok");
    assert_eq!(side, Side::Sell);
}

#[test]
fn compute_hedge_params_no_bid() {
    let (token, side) = HedgeExecutor::compute_hedge_params(QuoteLeg::NoBid, "yes-tok", "no-tok");
    assert_eq!(token, "yes-tok");
    assert_eq!(side, Side::Buy);
}

#[test]
fn compute_hedge_params_no_ask() {
    let (token, side) = HedgeExecutor::compute_hedge_params(QuoteLeg::NoAsk, "yes-tok", "no-tok");
    assert_eq!(token, "yes-tok");
    assert_eq!(side, Side::Sell);
}

// ---------------------------------------------------------------------------
// normalize_share_size
// ---------------------------------------------------------------------------

#[test]
fn normalize_share_size_whole_number() {
    assert_eq!(normalize_share_size(dec!(10)), dec!(10));
}

#[test]
fn normalize_share_size_truncates_fractional() {
    assert_eq!(normalize_share_size(dec!(10.7)), dec!(10.70));
}

#[test]
fn normalize_share_size_truncates_more_than_two_decimals() {
    assert_eq!(normalize_share_size(dec!(10.789)), dec!(10.78));
}

#[test]
fn normalize_share_size_zero() {
    assert_eq!(normalize_share_size(Decimal::ZERO), Decimal::ZERO);
}

#[test]
fn normalize_share_size_negative_truncates_toward_zero() {
    assert_eq!(normalize_share_size(dec!(-3.999)), dec!(-3.99));
}

#[test]
fn normalize_share_size_large_value() {
    assert_eq!(normalize_share_size(dec!(999999.999)), dec!(999999.99));
}

// ---------------------------------------------------------------------------
// HedgeIntent serde roundtrip
// ---------------------------------------------------------------------------

#[test]
fn hedge_intent_serde_roundtrip() {
    let intent = HedgeIntent {
        condition_id: "cond-1".to_string(),
        trigger_order_id: "order-42".to_string(),
        trigger_leg: QuoteLeg::YesBid,
        fill_size: dec!(100),
        fill_price: dec!(0.55),
        hedge_token_id: "tok-no".to_string(),
        hedge_side: Side::Buy,
        neg_risk: false,
        tick_size: "0.01".to_string(),
    };

    let json = serde_json::to_string(&intent).expect("serialize HedgeIntent");
    let deserialized: HedgeIntent = serde_json::from_str(&json).expect("deserialize HedgeIntent");

    assert_eq!(deserialized.condition_id, "cond-1");
    assert_eq!(deserialized.trigger_order_id, "order-42");
    assert_eq!(deserialized.trigger_leg, QuoteLeg::YesBid);
    assert_eq!(deserialized.fill_size, dec!(100));
    assert_eq!(deserialized.fill_price, dec!(0.55));
    assert_eq!(deserialized.hedge_token_id, "tok-no");
    assert_eq!(deserialized.hedge_side, Side::Buy);
    assert!(!deserialized.neg_risk);
    assert_eq!(deserialized.tick_size, "0.01");
}

// ---------------------------------------------------------------------------
// HedgeResult field access
// ---------------------------------------------------------------------------

#[test]
fn hedge_result_fields_accessible() {
    let result = HedgeResult {
        intent: HedgeIntent {
            condition_id: "c".to_string(),
            trigger_order_id: "o".to_string(),
            trigger_leg: QuoteLeg::NoBid,
            fill_size: dec!(50),
            fill_price: dec!(0.30),
            hedge_token_id: "t".to_string(),
            hedge_side: Side::Buy,
            neg_risk: true,
            tick_size: "0.01".to_string(),
        },
        success: true,
        order_result: None,
        hedge_price: Some(dec!(0.71)),
        failure_reason: None,
        verification_state: HedgeVerificationState::VerifiedFilled,
        verification_metadata: Default::default(),
    };

    assert!(result.success);
    assert_eq!(result.hedge_price, Some(dec!(0.71)));
    assert!(result.failure_reason.is_none());
    assert_eq!(
        result.verification_state,
        HedgeVerificationState::VerifiedFilled
    );
}

#[test]
fn hedge_result_serde_roundtrip() {
    let result = HedgeResult {
        intent: HedgeIntent {
            condition_id: "c".to_string(),
            trigger_order_id: "o".to_string(),
            trigger_leg: QuoteLeg::YesAsk,
            fill_size: dec!(25),
            fill_price: dec!(0.60),
            hedge_token_id: "t".to_string(),
            hedge_side: Side::Sell,
            neg_risk: false,
            tick_size: "0.01".to_string(),
        },
        success: false,
        order_result: None,
        hedge_price: None,
        failure_reason: Some("book too thin".to_string()),
        verification_state: HedgeVerificationState::VerifiedZeroFill,
        verification_metadata: Default::default(),
    };

    let json = serde_json::to_string(&result).expect("serialize");
    let back: HedgeResult = serde_json::from_str(&json).expect("deserialize");
    assert!(!back.success);
    assert_eq!(back.failure_reason.as_deref(), Some("book too thin"));
    assert_eq!(
        back.verification_state,
        HedgeVerificationState::VerifiedZeroFill
    );
}

// ---------------------------------------------------------------------------
// HedgeResolution field access
// ---------------------------------------------------------------------------

#[test]
fn hedge_resolution_fields() {
    let res = HedgeResolution {
        hedge_shares: dec!(200),
        hedge_limit_price: dec!(0.27),
        sellback_shares: dec!(100),
        sellback_limit_price: dec!(0.73),
        unresolved_shares: dec!(50),
    };

    assert_eq!(res.hedge_shares, dec!(200));
    assert_eq!(res.hedge_limit_price, dec!(0.27));
    assert_eq!(res.sellback_shares, dec!(100));
    assert_eq!(res.sellback_limit_price, dec!(0.73));
    assert_eq!(res.unresolved_shares, dec!(50));
}

#[test]
fn hedge_resolution_serde_roundtrip() {
    let res = HedgeResolution {
        hedge_shares: dec!(50),
        hedge_limit_price: dec!(0.28),
        sellback_shares: dec!(25),
        sellback_limit_price: dec!(0.72),
        unresolved_shares: Decimal::ZERO,
    };

    let json = serde_json::to_string(&res).expect("serialize");
    let back: HedgeResolution = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(back.hedge_shares, dec!(50));
    assert_eq!(back.sellback_limit_price, dec!(0.72));
}

// ---------------------------------------------------------------------------
// HedgeVerificationState variants
// ---------------------------------------------------------------------------

#[test]
fn hedge_verification_state_all_variants() {
    let states = [
        HedgeVerificationState::VerifiedFilled,
        HedgeVerificationState::VerifiedZeroFill,
        HedgeVerificationState::Unknown,
    ];
    // All three are distinct
    assert_ne!(states[0], states[1]);
    assert_ne!(states[1], states[2]);
    assert_ne!(states[0], states[2]);
}

#[test]
fn hedge_verification_state_serde_roundtrip() {
    for state in [
        HedgeVerificationState::VerifiedFilled,
        HedgeVerificationState::VerifiedZeroFill,
        HedgeVerificationState::Unknown,
    ] {
        let json = serde_json::to_string(&state).expect("serialize");
        let back: HedgeVerificationState = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back, state);
    }
}

// ---------------------------------------------------------------------------
// compute_hedge_resolution delegates to plan_fill_resolution
// ---------------------------------------------------------------------------

#[test]
fn compute_hedge_resolution_matches_unlimited_budget_plan() {
    let asks = vec![pl(dec!(0.26), dec!(400))];
    let bids = vec![pl(dec!(0.73), dec!(400))];

    let from_compute = compute_hedge_resolution(dec!(0.74), &asks, &bids, dec!(100), TICK);
    let from_plan = plan_fill_resolution(dec!(0.74), &asks, &bids, dec!(100), Decimal::MAX, TICK);

    assert_eq!(from_compute.hedge_shares, from_plan.hedge_shares);
    assert_eq!(from_compute.hedge_limit_price, from_plan.hedge_limit_price);
    assert_eq!(from_compute.sellback_shares, from_plan.sellback_shares);
    assert_eq!(
        from_compute.sellback_limit_price,
        from_plan.sellback_limit_price
    );
    assert_eq!(from_compute.unresolved_shares, from_plan.unresolved_shares);
}

// ---------------------------------------------------------------------------
// plan_fill_resolution: fractional total_size truncation
// ---------------------------------------------------------------------------

#[test]
fn plan_resolution_truncates_fractional_total_size() {
    let asks = vec![pl(dec!(0.26), dec!(500))];
    let bids = vec![pl(dec!(0.73), dec!(500))];
    // 10.99 → normalized to 10.99 (two decimal truncation)
    let res = plan_fill_resolution(dec!(0.74), &asks, &bids, dec!(10.999), Decimal::MAX, TICK);
    // Total resolved should equal normalized total_size
    let total = res.hedge_shares + res.sellback_shares + res.unresolved_shares;
    assert_eq!(total, dec!(10.99));
}

// ---------------------------------------------------------------------------
// plan_fill_resolution: negative budget treated as zero
// ---------------------------------------------------------------------------

#[test]
fn plan_resolution_negative_budget_treated_as_zero() {
    let asks = vec![pl(dec!(0.26), dec!(500))];
    let bids = vec![pl(dec!(0.73), dec!(500))];
    let res = plan_fill_resolution(dec!(0.74), &asks, &bids, dec!(100), dec!(-50), TICK);
    // Negative budget → 0 budget → all sellback
    assert_eq!(res.hedge_shares, Decimal::ZERO);
    assert_eq!(res.sellback_shares, dec!(100));
}

// ---------------------------------------------------------------------------
// plan_fill_resolution: single share
// ---------------------------------------------------------------------------

#[test]
fn plan_resolution_single_share() {
    let asks = vec![pl(dec!(0.26), dec!(10))];
    let bids = vec![pl(dec!(0.73), dec!(10))];
    let res = plan_fill_resolution(dec!(0.74), &asks, &bids, dec!(1), Decimal::MAX, TICK);
    assert_eq!(res.hedge_shares, dec!(1));
    assert_eq!(res.sellback_shares, Decimal::ZERO);
    assert_eq!(res.unresolved_shares, Decimal::ZERO);
}
