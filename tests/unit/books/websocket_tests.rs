use spreadeater::books::{BookEvent, BookWsStats, BookWsStatsSnapshot};
use spreadeater::models::PriceLevel;

use super::super::helpers::*;

// ---------------------------------------------------------------------------
// BookEvent construction
// ---------------------------------------------------------------------------

#[test]
fn book_event_snapshot_construction() {
    let event = BookEvent::Snapshot {
        token_id: "snap-tok".to_string(),
        bids: vec![make_price_level(0.45, 100.0)],
        asks: vec![make_price_level(0.55, 200.0)],
    };

    match event {
        BookEvent::Snapshot {
            token_id,
            bids,
            asks,
        } => {
            assert_eq!(token_id, "snap-tok");
            assert_eq!(bids.len(), 1);
            assert_eq!(asks.len(), 1);
        }
        _ => panic!("expected Snapshot variant"),
    }
}

#[test]
fn book_event_delta_construction() {
    let event = BookEvent::Delta {
        token_id: "delta-tok".to_string(),
        bid_updates: vec![make_price_level(0.45, 100.0), make_price_level(0.44, 50.0)],
        ask_updates: vec![make_price_level(0.55, 0.0)],
    };

    match event {
        BookEvent::Delta {
            token_id,
            bid_updates,
            ask_updates,
        } => {
            assert_eq!(token_id, "delta-tok");
            assert_eq!(bid_updates.len(), 2);
            assert_eq!(ask_updates.len(), 1);
        }
        _ => panic!("expected Delta variant"),
    }
}

#[test]
fn book_event_disconnected_is_unit() {
    let event = BookEvent::Disconnected;
    assert!(matches!(event, BookEvent::Disconnected));
}

// ---------------------------------------------------------------------------
// BookWsStatsSnapshot defaults
// ---------------------------------------------------------------------------

#[test]
fn stats_default_is_zero() {
    let snap = BookWsStatsSnapshot::default();
    assert_eq!(snap.accepted_messages, 0);
    assert_eq!(snap.ignored_messages, 0);
    assert_eq!(snap.parse_errors, 0);
    assert_eq!(snap.snapshot_events, 0);
    assert_eq!(snap.delta_events, 0);
    assert!(snap.last_raw_message_at.is_none());
    assert!(snap.last_parsed_event_at.is_none());
    assert!(snap.last_parse_error_at.is_none());
}

#[test]
fn stats_snapshot_equality() {
    let a = BookWsStatsSnapshot::default();
    let b = BookWsStatsSnapshot::default();
    assert_eq!(a, b);
}
