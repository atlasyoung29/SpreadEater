use chrono::{Duration, Utc};
use rust_decimal_macros::dec;
use spreadeater::discovery::filter_and_reconcile;
use spreadeater::models::*;

use super::super::helpers::*;

// ---------------------------------------------------------------------------
// Valid market
// ---------------------------------------------------------------------------

#[test]
fn valid_market_admitted() {
    let result = filter_and_reconcile(vec![make_market("c1")], dec!(5));
    assert_eq!(result.admitted.len(), 1);
    assert_eq!(result.rejected.len(), 0);
    assert_eq!(result.admitted[0].condition_id, "c1");
}

// ---------------------------------------------------------------------------
// Single-flag rejections
// ---------------------------------------------------------------------------

#[test]
fn inactive_market_rejected() {
    let mut m = make_market("c1");
    m.active = false;
    let result = filter_and_reconcile(vec![m], dec!(5));
    assert_eq!(result.admitted.len(), 0);
    assert_eq!(result.rejected.len(), 1);
}

#[test]
fn closed_market_rejected() {
    let mut m = make_market("c1");
    m.closed = true;
    let result = filter_and_reconcile(vec![m], dec!(5));
    assert_eq!(result.admitted.len(), 0);
    assert_eq!(result.rejected.len(), 1);
}

#[test]
fn archived_market_rejected() {
    let mut m = make_market("c1");
    m.archived = true;
    let result = filter_and_reconcile(vec![m], dec!(5));
    assert_eq!(result.admitted.len(), 0);
    assert_eq!(result.rejected.len(), 1);
}

#[test]
fn not_accepting_orders_rejected() {
    let mut m = make_market("c1");
    m.accepting_orders = false;
    let result = filter_and_reconcile(vec![m], dec!(5));
    assert_eq!(result.admitted.len(), 0);
    assert_eq!(result.rejected.len(), 1);
}

#[test]
fn non_binary_rejected() {
    let mut m = make_market("c1");
    m.is_binary = false;
    let result = filter_and_reconcile(vec![m], dec!(5));
    assert_eq!(result.admitted.len(), 0);
    assert_eq!(result.rejected.len(), 1);
}

// ---------------------------------------------------------------------------
// Reward config rejections
// ---------------------------------------------------------------------------

#[test]
fn no_reward_config_rejected() {
    let mut m = make_market("c1");
    m.reward_config = None;
    let result = filter_and_reconcile(vec![m], dec!(5));
    assert_eq!(result.admitted.len(), 0);
    assert_eq!(result.rejected.len(), 1);
}

#[test]
fn reward_below_threshold_rejected() {
    let mut m = make_market("c1");
    m.reward_config = Some(make_reward_config("c1", 1.0));
    let result = filter_and_reconcile(vec![m], dec!(5));
    assert_eq!(result.admitted.len(), 0);
    assert_eq!(result.rejected.len(), 1);
}

// ---------------------------------------------------------------------------
// Token rejections
// ---------------------------------------------------------------------------

#[test]
fn missing_yes_token_rejected() {
    let mut m = make_market("c1");
    // Keep only the No token
    m.tokens.retain(|t| t.outcome == Outcome::No);
    let result = filter_and_reconcile(vec![m], dec!(5));
    assert_eq!(result.admitted.len(), 0);
    assert_eq!(result.rejected.len(), 1);
}

#[test]
fn missing_no_token_rejected() {
    let mut m = make_market("c1");
    // Keep only the Yes token
    m.tokens.retain(|t| t.outcome == Outcome::Yes);
    let result = filter_and_reconcile(vec![m], dec!(5));
    assert_eq!(result.admitted.len(), 0);
    assert_eq!(result.rejected.len(), 1);
}

#[test]
fn empty_token_id_rejected() {
    let mut m = make_market("c1");
    // Set the Yes token's ID to empty
    for token in &mut m.tokens {
        if token.outcome == Outcome::Yes {
            token.token_id = String::new();
        }
    }
    let result = filter_and_reconcile(vec![m], dec!(5));
    assert_eq!(result.admitted.len(), 0);
    assert_eq!(result.rejected.len(), 1);
}

#[test]
fn identical_token_ids_rejected() {
    let mut m = make_market("c1");
    // Set both token IDs to the same value
    for token in &mut m.tokens {
        token.token_id = "same-id".to_string();
    }
    let result = filter_and_reconcile(vec![m], dec!(5));
    assert_eq!(result.admitted.len(), 0);
    assert_eq!(result.rejected.len(), 1);
}

// ---------------------------------------------------------------------------
// End date expiry
// ---------------------------------------------------------------------------

#[test]
fn expiring_within_24h_rejected() {
    let mut m = make_market("c1");
    // 12 hours from now is within the 24-hour cutoff
    let expires_soon = (Utc::now() + Duration::hours(12)).to_rfc3339();
    m.end_date_iso = Some(expires_soon);
    let result = filter_and_reconcile(vec![m], dec!(5));
    assert_eq!(result.admitted.len(), 0);
    assert_eq!(result.rejected.len(), 1);
}

#[test]
fn no_end_date_admitted() {
    let mut m = make_market("c1");
    m.end_date_iso = None; // already the default, but explicit
    let result = filter_and_reconcile(vec![m], dec!(5));
    assert_eq!(result.admitted.len(), 1);
    assert_eq!(result.rejected.len(), 0);
}

// ---------------------------------------------------------------------------
// Multiple markets
// ---------------------------------------------------------------------------

#[test]
fn multiple_markets_mixed() {
    let valid = make_market("c1");

    let mut closed = make_market("c2");
    closed.closed = true;

    let mut no_reward = make_market("c3");
    no_reward.reward_config = None;

    let result = filter_and_reconcile(vec![valid, closed, no_reward], dec!(5));
    assert_eq!(result.admitted.len(), 1);
    assert_eq!(result.rejected.len(), 2);
    assert_eq!(result.admitted[0].condition_id, "c1");
}
