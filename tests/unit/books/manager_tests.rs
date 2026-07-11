use chrono::{Duration, Utc};
use rust_decimal_macros::dec;
use spreadeater::books::{BookEvent, BookManager};
use spreadeater::models::*;

use super::super::helpers::*;

// ---------------------------------------------------------------------------
// new / count
// ---------------------------------------------------------------------------

#[tokio::test]
async fn new_is_empty() {
    let mgr = BookManager::new();
    assert_eq!(mgr.count().await, 0);
}

// ---------------------------------------------------------------------------
// insert_snapshot / get_book
// ---------------------------------------------------------------------------

#[tokio::test]
async fn insert_and_get() {
    let mgr = BookManager::new();
    let snap = make_orderbook_snapshot("tok-1", vec![(0.50, 10.0)], vec![(0.55, 5.0)]);
    mgr.insert_snapshot(snap).await;

    let book = mgr.get_book("tok-1").await;
    assert!(book.is_some());
    let book = book.unwrap();
    assert_eq!(book.token_id, "tok-1");
    assert_eq!(book.bids.len(), 1);
    assert_eq!(book.asks.len(), 1);
}

#[tokio::test]
async fn get_book_none_for_unknown() {
    let mgr = BookManager::new();
    assert!(mgr.get_book("unknown").await.is_none());
}

// ---------------------------------------------------------------------------
// get_pair
// ---------------------------------------------------------------------------

#[tokio::test]
async fn get_pair_returns_both() {
    let mgr = BookManager::new();
    let yes_snap = make_orderbook_snapshot("yes-tok", vec![(0.50, 10.0)], vec![(0.55, 5.0)]);
    let no_snap = make_orderbook_snapshot("no-tok", vec![(0.48, 8.0)], vec![(0.52, 3.0)]);
    mgr.insert_snapshot(yes_snap).await;
    mgr.insert_snapshot(no_snap).await;

    let pair = mgr.get_pair("yes-tok", "no-tok").await;
    assert!(pair.is_some());
    let (yes, no) = pair.unwrap();
    assert_eq!(yes.token_id, "yes-tok");
    assert_eq!(no.token_id, "no-tok");
}

#[tokio::test]
async fn get_pair_none_if_one_missing() {
    let mgr = BookManager::new();
    let yes_snap = make_orderbook_snapshot("yes-tok", vec![(0.50, 10.0)], vec![(0.55, 5.0)]);
    mgr.insert_snapshot(yes_snap).await;

    assert!(mgr.get_pair("yes-tok", "no-tok").await.is_none());
}

// ---------------------------------------------------------------------------
// apply_event — Snapshot
// ---------------------------------------------------------------------------

#[tokio::test]
async fn apply_snapshot_event() {
    let mgr = BookManager::new();
    mgr.apply_event(BookEvent::Snapshot {
        token_id: "tok-a".to_string(),
        bids: vec![make_price_level(0.50, 10.0), make_price_level(0.48, 5.0)],
        asks: vec![make_price_level(0.55, 8.0)],
    })
    .await;

    let book = mgr.get_book("tok-a").await.unwrap();
    assert_eq!(book.token_id, "tok-a");
    assert_eq!(book.bids.len(), 2);
    assert_eq!(book.asks.len(), 1);
    // Bids should be sorted descending (highest first)
    assert_eq!(book.bids[0].price, dec!(0.50));
    assert_eq!(book.bids[1].price, dec!(0.48));
}

// ---------------------------------------------------------------------------
// apply_event — Delta
// ---------------------------------------------------------------------------

#[tokio::test]
async fn apply_delta_updates_existing_level() {
    let mgr = BookManager::new();
    let snap = make_orderbook_snapshot("tok-d", vec![(0.50, 10.0)], vec![(0.55, 5.0)]);
    mgr.insert_snapshot(snap).await;

    mgr.apply_event(BookEvent::Delta {
        token_id: "tok-d".to_string(),
        bid_updates: vec![make_price_level(0.50, 15.0)],
        ask_updates: vec![],
    })
    .await;

    let book = mgr.get_book("tok-d").await.unwrap();
    assert_eq!(book.bids.len(), 1);
    assert_eq!(book.bids[0].size, dec!(15));
}

#[tokio::test]
async fn apply_delta_removes_zero_size_level() {
    let mgr = BookManager::new();
    let snap = make_orderbook_snapshot("tok-r", vec![(0.50, 10.0)], vec![(0.55, 5.0)]);
    mgr.insert_snapshot(snap).await;

    mgr.apply_event(BookEvent::Delta {
        token_id: "tok-r".to_string(),
        bid_updates: vec![make_price_level(0.50, 0.0)],
        ask_updates: vec![],
    })
    .await;

    let book = mgr.get_book("tok-r").await.unwrap();
    assert!(book.bids.is_empty(), "bid at 0.50 should have been removed");
}

#[tokio::test]
async fn apply_delta_adds_new_level() {
    let mgr = BookManager::new();
    let snap = make_orderbook_snapshot("tok-n", vec![(0.50, 10.0)], vec![(0.55, 5.0)]);
    mgr.insert_snapshot(snap).await;

    mgr.apply_event(BookEvent::Delta {
        token_id: "tok-n".to_string(),
        bid_updates: vec![make_price_level(0.48, 5.0)],
        ask_updates: vec![],
    })
    .await;

    let book = mgr.get_book("tok-n").await.unwrap();
    assert_eq!(book.bids.len(), 2);
    // Should be sorted: 0.50 then 0.48
    assert_eq!(book.bids[0].price, dec!(0.50));
    assert_eq!(book.bids[1].price, dec!(0.48));
}

#[tokio::test]
async fn apply_delta_ignores_unknown_token() {
    let mgr = BookManager::new();
    // No books inserted — delta for unknown token should not panic
    mgr.apply_event(BookEvent::Delta {
        token_id: "ghost".to_string(),
        bid_updates: vec![make_price_level(0.50, 10.0)],
        ask_updates: vec![],
    })
    .await;

    assert_eq!(mgr.count().await, 0);
}

#[tokio::test]
async fn apply_disconnected_no_panic() {
    let mgr = BookManager::new();
    mgr.apply_event(BookEvent::Disconnected).await;
    // Just verify no panic and count is still 0
    assert_eq!(mgr.count().await, 0);
}

// ---------------------------------------------------------------------------
// is_stale
// ---------------------------------------------------------------------------

#[tokio::test]
async fn is_stale_true_when_no_book() {
    let mgr = BookManager::new();
    assert!(mgr.is_stale("missing", Duration::seconds(60)).await);
}

#[tokio::test]
async fn is_stale_true_when_old() {
    let mgr = BookManager::new();
    let mut snap = make_orderbook_snapshot("tok-old", vec![(0.50, 10.0)], vec![(0.55, 5.0)]);
    // Set ingest_ts far in the past
    snap.ingest_ts = Utc::now() - Duration::seconds(120);
    mgr.insert_snapshot(snap).await;

    assert!(mgr.is_stale("tok-old", Duration::seconds(60)).await);
}

#[tokio::test]
async fn is_stale_false_when_fresh() {
    let mgr = BookManager::new();
    let snap = make_orderbook_snapshot("tok-fresh", vec![(0.50, 10.0)], vec![(0.55, 5.0)]);
    mgr.insert_snapshot(snap).await;

    assert!(!mgr.is_stale("tok-fresh", Duration::seconds(60)).await);
}

// ---------------------------------------------------------------------------
// count
// ---------------------------------------------------------------------------

#[tokio::test]
async fn count_tracks_insertions() {
    let mgr = BookManager::new();
    mgr.insert_snapshot(make_orderbook_snapshot("a", vec![(0.50, 1.0)], vec![]))
        .await;
    mgr.insert_snapshot(make_orderbook_snapshot("b", vec![(0.50, 1.0)], vec![]))
        .await;
    mgr.insert_snapshot(make_orderbook_snapshot("c", vec![(0.50, 1.0)], vec![]))
        .await;
    assert_eq!(mgr.count().await, 3);
}
