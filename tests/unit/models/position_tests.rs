use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use spreadeater::models::*;

use super::super::helpers::*;

// ---------------------------------------------------------------------------
// Position tests
// ---------------------------------------------------------------------------

#[test]
fn position_new_zeroed() {
    let p = Position::new("cond-1".to_string());
    assert_eq!(p.condition_id, "cond-1");
    assert_eq!(p.yes_size, Decimal::ZERO);
    assert_eq!(p.no_size, Decimal::ZERO);
    assert_eq!(p.avg_yes_price, Decimal::ZERO);
    assert_eq!(p.avg_no_price, Decimal::ZERO);
}

#[test]
fn net_exposure_positive_when_yes_greater() {
    let p = make_position("cond-1", 10.0, 5.0);
    assert_eq!(p.net_exposure(), dec!(5));
}

#[test]
fn net_exposure_negative_when_no_greater() {
    let p = make_position("cond-1", 3.0, 7.0);
    assert_eq!(p.net_exposure(), dec!(-4));
}

#[test]
fn net_exposure_zero_when_balanced() {
    let p = make_position("cond-1", 5.0, 5.0);
    assert_eq!(p.net_exposure(), Decimal::ZERO);
}

#[test]
fn complete_sets_returns_min() {
    let p = make_position("cond-1", 10.0, 7.0);
    assert_eq!(p.complete_sets(), dec!(7));
}

#[test]
fn complete_sets_zero_when_one_empty() {
    let p = make_position("cond-1", 10.0, 0.0);
    assert_eq!(p.complete_sets(), Decimal::ZERO);
}

#[test]
fn has_yes_inventory_true() {
    let p = make_position("cond-1", 10.0, 0.0);
    assert!(p.has_yes_inventory(dec!(5)));
}

#[test]
fn has_yes_inventory_false() {
    let p = make_position("cond-1", 3.0, 0.0);
    assert!(!p.has_yes_inventory(dec!(5)));
}

#[test]
fn has_no_inventory_true() {
    let p = make_position("cond-1", 0.0, 10.0);
    assert!(p.has_no_inventory(dec!(5)));
}

#[test]
fn has_no_inventory_false() {
    let p = make_position("cond-1", 0.0, 3.0);
    assert!(!p.has_no_inventory(dec!(5)));
}

#[test]
fn is_hedged_true_when_both_nonzero() {
    let p = make_position("cond-1", 5.0, 3.0);
    assert!(p.is_hedged());
}

#[test]
fn is_hedged_false_when_one_zero() {
    let p = make_position("cond-1", 5.0, 0.0);
    assert!(!p.is_hedged());
}

#[test]
fn sellable_yes_with_excess() {
    let p = make_position("cond-1", 10.0, 3.0);
    assert_eq!(p.sellable_yes(), dec!(7));
}

#[test]
fn sellable_no_with_excess() {
    let p = make_position("cond-1", 3.0, 10.0);
    assert_eq!(p.sellable_no(), dec!(7));
}

#[test]
fn position_serde_roundtrip() {
    let p = make_position("cond-1", 10.0, 5.0);
    let json = serde_json::to_string(&p).expect("serialize position");
    let p2: Position = serde_json::from_str(&json).expect("deserialize position");
    assert_eq!(p.condition_id, p2.condition_id);
    assert_eq!(p.yes_size, p2.yes_size);
    assert_eq!(p.no_size, p2.no_size);
    assert_eq!(p.avg_yes_price, p2.avg_yes_price);
    assert_eq!(p.avg_no_price, p2.avg_no_price);
}

// ---------------------------------------------------------------------------
// AccountState tests
// ---------------------------------------------------------------------------

#[test]
fn account_state_new_defaults() {
    let state = AccountState::new();
    assert_eq!(state.collateral_balance, Decimal::ZERO);
    assert_eq!(state.total_position_value, Decimal::ZERO);
    assert!(state.positions.is_empty());
}

#[test]
fn account_state_get_position_found() {
    let mut state = AccountState::new();
    state.positions.push(make_position("cond-1", 10.0, 5.0));
    let found = state.get_position("cond-1");
    assert!(found.is_some());
    assert_eq!(found.unwrap().condition_id, "cond-1");
}

#[test]
fn account_state_get_position_not_found() {
    let state = AccountState::new();
    assert!(state.get_position("nonexistent").is_none());
}
