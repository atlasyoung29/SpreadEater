use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use spreadeater::models::{QuoteLeg, Side};
use spreadeater::trading::client::CancelOrderOutcome;
use spreadeater::trading::order_manager::{
    DuplicateLiveBidLeg, MarketOrderSyncMode, MarketOrderSyncResult, MatchUpdate,
    OpenOrderSyncResult, TrackedOrder,
};

// ---------------------------------------------------------------------------
// TrackedOrder construction & field access
// ---------------------------------------------------------------------------

fn sample_tracked_order() -> TrackedOrder {
    TrackedOrder {
        order_id: "order-1".to_string(),
        trace_id: "trace-abc".to_string(),
        condition_id: "cond-1".to_string(),
        created_at: chrono::Utc::now(),
        leg: QuoteLeg::YesBid,
        token_id: "tok-yes".to_string(),
        opposite_token_id: "tok-no".to_string(),
        side: Side::Buy,
        price: dec!(0.55),
        size: dec!(100),
        matched_size: dec!(0),
        neg_risk: false,
        tick_size: "0.01".to_string(),
    }
}

#[test]
fn tracked_order_fields_accessible() {
    let t = sample_tracked_order();
    assert_eq!(t.order_id, "order-1");
    assert_eq!(t.condition_id, "cond-1");
    assert_eq!(t.leg, QuoteLeg::YesBid);
    assert_eq!(t.side, Side::Buy);
    assert_eq!(t.price, dec!(0.55));
    assert_eq!(t.size, dec!(100));
    assert_eq!(t.matched_size, dec!(0));
    assert!(!t.neg_risk);
    assert_eq!(t.tick_size, "0.01");
}

#[test]
fn tracked_order_clone_is_independent() {
    let t = sample_tracked_order();
    let mut t2 = t.clone();
    t2.matched_size = dec!(50);
    // Original unchanged
    assert_eq!(t.matched_size, dec!(0));
    assert_eq!(t2.matched_size, dec!(50));
}

// ---------------------------------------------------------------------------
// MatchUpdate
// ---------------------------------------------------------------------------

#[test]
fn match_update_newly_matched() {
    let before = sample_tracked_order();
    let mut after = before.clone();
    after.matched_size = dec!(8);

    let update = MatchUpdate {
        tracked_before: before,
        tracked_after: Some(after),
        newly_matched: dec!(8),
    };

    assert_eq!(update.newly_matched, dec!(8));
    assert_eq!(update.tracked_before.matched_size, dec!(0));
    assert!(update.tracked_after.is_some());
    assert_eq!(update.tracked_after.unwrap().matched_size, dec!(8));
}

#[test]
fn match_update_with_none_after() {
    let before = sample_tracked_order();
    let update = MatchUpdate {
        tracked_before: before,
        tracked_after: None,
        newly_matched: dec!(5),
    };
    assert!(update.tracked_after.is_none());
    assert_eq!(update.newly_matched, dec!(5));
}

// ---------------------------------------------------------------------------
// OpenOrderSyncResult
// ---------------------------------------------------------------------------

#[test]
fn open_order_sync_result_default_fields() {
    let result = OpenOrderSyncResult::default();
    assert_eq!(result.fetched, 0);
    assert_eq!(result.live, 0);
    assert_eq!(result.imported, 0);
    assert_eq!(result.already_tracked, 0);
    assert_eq!(result.updated, 0);
    assert!(result.duplicate_live_bid_legs.is_empty());
}

#[test]
fn open_order_sync_result_custom_fields() {
    let result = OpenOrderSyncResult {
        fetched: 10,
        live: 8,
        imported: 3,
        already_tracked: 5,
        updated: 2,
        duplicate_live_bid_legs: vec![DuplicateLiveBidLeg {
            condition_id: "cond-1".to_string(),
            leg: QuoteLeg::YesBid,
            order_ids: vec!["o1".to_string(), "o2".to_string()],
        }],
    };
    assert_eq!(result.fetched, 10);
    assert_eq!(result.duplicate_live_bid_legs.len(), 1);
    assert_eq!(result.duplicate_live_bid_legs[0].order_ids.len(), 2);
}

// ---------------------------------------------------------------------------
// MarketOrderSyncResult
// ---------------------------------------------------------------------------

#[test]
fn market_order_sync_result_default_fields() {
    let result = MarketOrderSyncResult::default();
    assert_eq!(result.fetched, 0);
    assert_eq!(result.live, 0);
    assert_eq!(result.imported, 0);
    assert_eq!(result.already_tracked, 0);
    assert_eq!(result.updated, 0);
    assert_eq!(result.pruned, 0);
    assert!(result.missing_order_ids.is_empty());
    assert!(result.duplicate_live_bid_legs.is_empty());
}

#[test]
fn market_order_sync_result_custom_fields() {
    let result = MarketOrderSyncResult {
        fetched: 20,
        live: 15,
        imported: 5,
        already_tracked: 10,
        updated: 3,
        pruned: 2,
        missing_order_ids: vec!["missing-1".to_string()],
        duplicate_live_bid_legs: vec![],
    };
    assert_eq!(result.pruned, 2);
    assert_eq!(result.missing_order_ids.len(), 1);
}

// ---------------------------------------------------------------------------
// DuplicateLiveBidLeg
// ---------------------------------------------------------------------------

#[test]
fn duplicate_live_bid_leg_fields() {
    let dup = DuplicateLiveBidLeg {
        condition_id: "cond-42".to_string(),
        leg: QuoteLeg::NoBid,
        order_ids: vec!["a".to_string(), "b".to_string(), "c".to_string()],
    };
    assert_eq!(dup.condition_id, "cond-42");
    assert_eq!(dup.leg, QuoteLeg::NoBid);
    assert_eq!(dup.order_ids.len(), 3);
}

#[test]
fn duplicate_live_bid_leg_equality() {
    let a = DuplicateLiveBidLeg {
        condition_id: "c1".to_string(),
        leg: QuoteLeg::YesBid,
        order_ids: vec!["o1".to_string()],
    };
    let b = a.clone();
    assert_eq!(a, b);
}

// ---------------------------------------------------------------------------
// MarketOrderSyncMode
// ---------------------------------------------------------------------------

#[test]
fn market_order_sync_mode_variants_are_distinct() {
    assert_ne!(
        MarketOrderSyncMode::ObserveOnly,
        MarketOrderSyncMode::Reconcile
    );
}

#[test]
fn market_order_sync_mode_equality() {
    assert_eq!(
        MarketOrderSyncMode::ObserveOnly,
        MarketOrderSyncMode::ObserveOnly
    );
    assert_eq!(
        MarketOrderSyncMode::Reconcile,
        MarketOrderSyncMode::Reconcile
    );
}

// ---------------------------------------------------------------------------
// CancelOrderOutcome (re-tested here for order_manager context)
// ---------------------------------------------------------------------------

#[test]
fn cancel_order_outcome_confirmed() {
    let outcome = CancelOrderOutcome::Confirmed;
    assert!(matches!(outcome, CancelOrderOutcome::Confirmed));
}

#[test]
fn cancel_order_outcome_rejected() {
    let outcome = CancelOrderOutcome::Rejected("not found".to_string());
    match outcome {
        CancelOrderOutcome::Rejected(reason) => assert_eq!(reason, "not found"),
        _ => panic!("Expected Rejected variant"),
    }
}

#[test]
fn cancel_order_outcome_unknown() {
    let outcome = CancelOrderOutcome::Unknown("timeout".to_string());
    match outcome {
        CancelOrderOutcome::Unknown(reason) => assert_eq!(reason, "timeout"),
        _ => panic!("Expected Unknown variant"),
    }
}
