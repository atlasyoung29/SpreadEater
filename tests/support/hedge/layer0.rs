use super::*;

use anyhow::Result;
use rust_decimal::Decimal;
use tokio::sync::mpsc;
use tokio::time::Duration;

async fn seeded_engine_with_tracked_yes_bid() -> Result<(LiveEngine, TrackedOrder)> {
    let engine = test_engine().await;
    let market = test_market();
    engine.order_manager.update_gross_balance(dec!(1000)).await;

    let quote_set = QuoteSet {
        condition_id: market.condition_id.clone(),
        candidates: vec![QuoteCandidate {
            condition_id: market.condition_id.clone(),
            leg: QuoteLeg::YesBid,
            price: dec!(0.45),
            size: dec!(10),
            status: QuoteStatus::Approved,
            reason: None,
        }],
    };

    let tracked = engine
        .order_manager
        .place_quotes(&market, &quote_set, None, dec!(10), None, "test", None)
        .await?
        .into_iter()
        .next()
        .expect("tracked order should exist");

    Ok((engine, tracked))
}

#[tokio::test]
async fn layer0_build_fill_work_item_uses_residual_exposure_not_raw_fill_size() {
    let (engine, tracked) = seeded_engine_with_tracked_yes_bid()
        .await
        .expect("seeded engine should build");

    engine
        .position_manager
        .update_position(Position {
            condition_id: tracked.condition_id.clone(),
            yes_size: Decimal::ZERO,
            no_size: dec!(4),
            avg_yes_price: dec!(0.5),
            avg_no_price: dec!(0.26),
        })
        .await;

    let work = engine
        .build_fill_work_item(TradeEvent {
            id: "layer0-residual".to_string(),
            condition_id: tracked.condition_id.clone(),
            asset_id: tracked.token_id.clone(),
            side: Side::Buy,
            price: dec!(0.74),
            size: dec!(10),
            outcome: "YES".to_string(),
            status: TradeStatus::Matched,
            timestamp: Utc::now(),
            maker_order_id: Some(tracked.order_id.clone()),
            taker_order_id: None,
        })
        .await
        .expect("trade should attribute");

    assert_eq!(work.size_to_apply, dec!(10));
    assert_eq!(work.hedge_size, dec!(6));
}

#[tokio::test]
async fn layer0_build_fill_work_item_dedupes_duplicate_trade_ids() {
    let (engine, tracked) = seeded_engine_with_tracked_yes_bid()
        .await
        .expect("seeded engine should build");

    let trade = TradeEvent {
        id: "layer0-duplicate".to_string(),
        condition_id: tracked.condition_id.clone(),
        asset_id: tracked.token_id.clone(),
        side: Side::Buy,
        price: dec!(0.74),
        size: dec!(5),
        outcome: "YES".to_string(),
        status: TradeStatus::Matched,
        timestamp: Utc::now(),
        maker_order_id: Some(tracked.order_id.clone()),
        taker_order_id: None,
    };

    assert!(engine.build_fill_work_item(trade.clone()).await.is_some());
    assert!(engine.build_fill_work_item(trade).await.is_none());
}

#[tokio::test]
async fn layer0_flush_pending_fill_fallback_emits_order_update_fallback_work_item() {
    let (engine, tracked) = seeded_engine_with_tracked_yes_bid()
        .await
        .expect("seeded engine should build");

    engine
        .position_manager
        .update_position(Position {
            condition_id: tracked.condition_id.clone(),
            yes_size: Decimal::ZERO,
            no_size: dec!(4),
            avg_yes_price: dec!(0.5),
            avg_no_price: dec!(0.26),
        })
        .await;

    engine
        .handle_order_update(OrderEvent {
            order_id: tracked.order_id.clone(),
            condition_id: tracked.condition_id.clone(),
            asset_id: tracked.token_id.clone(),
            event_type: OrderEventType::Update,
            side: Side::Buy,
            price: dec!(0.74),
            original_size: dec!(10),
            size_matched: dec!(10),
            outcome: "YES".to_string(),
            timestamp: Utc::now(),
        })
        .await;

    let now = Instant::now();
    for pending in engine.pending_fill_fallbacks.write().await.values_mut() {
        pending.queued_at = now - std::time::Duration::from_secs(3);
    }

    let (fill_tx, mut fill_rx) = mpsc::unbounded_channel();
    engine
        .flush_pending_fill_fallbacks(&fill_tx)
        .await
        .expect("flush should succeed");

    let work = tokio::time::timeout(Duration::from_millis(100), fill_rx.recv())
        .await
        .expect("work item should arrive")
        .expect("fill work item should exist");

    assert_eq!(work.match_source, "order_update_fallback");
    assert!(work.fallback_match);
    assert_eq!(work.hedge_size, dec!(6));
}
