use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use spreadeater::models::*;

use super::super::helpers::*;

// ---------------------------------------------------------------------------
// best_bid / best_ask
// ---------------------------------------------------------------------------

#[test]
fn best_bid_returns_highest() {
    // bids are stored in order; best_bid() returns first element.
    // Build with highest first (sorted descending as expected by orderbook).
    let snap = make_orderbook_snapshot("tok", vec![(0.50, 10.0), (0.48, 5.0), (0.45, 3.0)], vec![]);
    let best = snap.best_bid().unwrap();
    assert_eq!(best.price, dec!(0.50));
}

#[test]
fn best_bid_none_when_empty() {
    let snap = make_orderbook_snapshot("tok", vec![], vec![(0.55, 10.0)]);
    assert!(snap.best_bid().is_none());
}

#[test]
fn best_ask_returns_lowest() {
    // asks stored ascending; best_ask() returns first element.
    let snap = make_orderbook_snapshot("tok", vec![], vec![(0.52, 5.0), (0.55, 10.0), (0.60, 3.0)]);
    let best = snap.best_ask().unwrap();
    assert_eq!(best.price, dec!(0.52));
}

#[test]
fn best_ask_none_when_empty() {
    let snap = make_orderbook_snapshot("tok", vec![(0.50, 10.0)], vec![]);
    assert!(snap.best_ask().is_none());
}

// ---------------------------------------------------------------------------
// mid / spread
// ---------------------------------------------------------------------------

#[test]
fn mid_correct() {
    let snap = make_orderbook_snapshot("tok", vec![(0.50, 10.0)], vec![(0.52, 10.0)]);
    let mid = snap.mid().unwrap();
    assert_eq!(mid, dec!(0.51));
}

#[test]
fn mid_none_when_one_side_empty() {
    let snap = make_orderbook_snapshot("tok", vec![(0.50, 10.0)], vec![]);
    assert!(snap.mid().is_none());
}

#[test]
fn spread_correct() {
    let snap = make_orderbook_snapshot("tok", vec![(0.50, 10.0)], vec![(0.52, 10.0)]);
    let spread = snap.spread().unwrap();
    assert_eq!(spread, dec!(0.02));
}

#[test]
fn spread_none_when_one_side_empty() {
    let snap = make_orderbook_snapshot("tok", vec![], vec![(0.55, 10.0)]);
    assert!(snap.spread().is_none());
}

// ---------------------------------------------------------------------------
// is_stale
// ---------------------------------------------------------------------------

#[test]
fn is_stale_true_when_old() {
    let mut snap = make_orderbook_snapshot("tok", vec![(0.50, 10.0)], vec![(0.55, 10.0)]);
    snap.ingest_ts = Utc::now() - chrono::Duration::seconds(200);
    assert!(snap.is_stale(chrono::Duration::seconds(120)));
}

#[test]
fn is_stale_false_when_fresh() {
    let snap = make_orderbook_snapshot("tok", vec![(0.50, 10.0)], vec![(0.55, 10.0)]);
    assert!(!snap.is_stale(chrono::Duration::seconds(120)));
}

// ---------------------------------------------------------------------------
// walk_asks
// ---------------------------------------------------------------------------

#[test]
fn walk_asks_single_level_full_fill() {
    let snap = make_orderbook_snapshot("tok", vec![], vec![(0.55, 10.0)]);
    let result = snap.walk_asks(dec!(5));
    assert_eq!(result.filled_size, dec!(5));
    assert!(result.fully_filled);
    assert_eq!(result.levels_consumed, 1);
    assert_eq!(result.worst_price, Some(dec!(0.55)));
}

#[test]
fn walk_asks_multi_level() {
    let snap = make_orderbook_snapshot("tok", vec![], vec![(0.52, 3.0), (0.55, 5.0)]);
    let result = snap.walk_asks(dec!(6));
    assert_eq!(result.filled_size, dec!(6));
    assert!(result.fully_filled);
    assert_eq!(result.levels_consumed, 2);
    assert_eq!(result.worst_price, Some(dec!(0.55)));
}

#[test]
fn walk_asks_partial_insufficient_depth() {
    let snap = make_orderbook_snapshot("tok", vec![], vec![(0.55, 3.0)]);
    let result = snap.walk_asks(dec!(10));
    assert_eq!(result.filled_size, dec!(3));
    assert!(!result.fully_filled);
}

#[test]
fn walk_asks_empty_book() {
    let snap = make_orderbook_snapshot("tok", vec![], vec![]);
    let result = snap.walk_asks(dec!(5));
    assert_eq!(result.filled_size, Decimal::ZERO);
    assert!(!result.fully_filled);
    assert_eq!(result.levels_consumed, 0);
}

// ---------------------------------------------------------------------------
// walk_bids
// ---------------------------------------------------------------------------

#[test]
fn walk_bids_single_level() {
    let snap = make_orderbook_snapshot("tok", vec![(0.50, 10.0)], vec![]);
    let result = snap.walk_bids(dec!(5));
    assert_eq!(result.filled_size, dec!(5));
    assert!(result.fully_filled);
    assert_eq!(result.levels_consumed, 1);
}

#[test]
fn walk_bids_partial() {
    let snap = make_orderbook_snapshot("tok", vec![(0.50, 3.0)], vec![]);
    let result = snap.walk_bids(dec!(10));
    assert_eq!(result.filled_size, dec!(3));
    assert!(!result.fully_filled);
}

#[test]
fn walk_bids_empty() {
    let snap = make_orderbook_snapshot("tok", vec![], vec![]);
    let result = snap.walk_bids(dec!(5));
    assert_eq!(result.filled_size, Decimal::ZERO);
    assert!(!result.fully_filled);
    assert_eq!(result.levels_consumed, 0);
}

#[test]
fn walk_zero_target_returns_zero() {
    let snap = make_orderbook_snapshot("tok", vec![(0.50, 10.0)], vec![(0.55, 10.0)]);
    let ask_result = snap.walk_asks(Decimal::ZERO);
    assert_eq!(ask_result.filled_size, Decimal::ZERO);
    let bid_result = snap.walk_bids(Decimal::ZERO);
    assert_eq!(bid_result.filled_size, Decimal::ZERO);
}
