use chrono::Utc;
use rust_decimal_macros::dec;
use spreadeater::models::*;

// ── Helper ────────────────────────────────────────────────────────

fn make_trade_event() -> TradeEvent {
    TradeEvent {
        id: "trade-1".to_string(),
        condition_id: "cond-1".to_string(),
        asset_id: "asset-1".to_string(),
        side: Side::Buy,
        price: dec!(0.50),
        size: dec!(10),
        outcome: "Yes".to_string(),
        status: TradeStatus::Confirmed,
        timestamp: Utc::now(),
        maker_order_id: Some("order-1".to_string()),
        taker_order_id: None,
    }
}

// ── TradeStatus serde ─────────────────────────────────────────────

#[test]
fn trade_status_serde_roundtrip() {
    for status in [
        TradeStatus::Matched,
        TradeStatus::Mined,
        TradeStatus::Confirmed,
        TradeStatus::Retrying,
        TradeStatus::Failed,
    ] {
        let json = serde_json::to_string(&status).expect("serialize TradeStatus");
        let back: TradeStatus = serde_json::from_str(&json).expect("deserialize TradeStatus");
        assert_eq!(status, back);
    }
}

// ── OrderEventType serde ──────────────────────────────────────────

#[test]
fn order_event_type_serde_roundtrip() {
    for evt in [
        OrderEventType::Placement,
        OrderEventType::Update,
        OrderEventType::Cancellation,
    ] {
        let json = serde_json::to_string(&evt).expect("serialize OrderEventType");
        let back: OrderEventType = serde_json::from_str(&json).expect("deserialize OrderEventType");
        assert_eq!(evt, back);
    }
}

// ── TradeEvent serde ──────────────────────────────────────────────

#[test]
fn trade_event_serde_roundtrip() {
    let te = make_trade_event();
    let json = serde_json::to_string(&te).expect("serialize TradeEvent");
    let back: TradeEvent = serde_json::from_str(&json).expect("deserialize TradeEvent");
    assert_eq!(back.id, "trade-1");
    assert_eq!(back.condition_id, "cond-1");
    assert_eq!(back.status, TradeStatus::Confirmed);
    assert_eq!(back.side, Side::Buy);
    assert_eq!(back.price, dec!(0.50));
    assert_eq!(back.size, dec!(10));
}

// ── OrderEvent serde ──────────────────────────────────────────────

#[test]
fn order_event_serde_roundtrip() {
    let oe = OrderEvent {
        order_id: "order-1".to_string(),
        condition_id: "cond-1".to_string(),
        asset_id: "asset-1".to_string(),
        event_type: OrderEventType::Placement,
        side: Side::Buy,
        price: dec!(0.50),
        original_size: dec!(10),
        size_matched: dec!(3),
        outcome: "Yes".to_string(),
        timestamp: Utc::now(),
    };
    let json = serde_json::to_string(&oe).expect("serialize OrderEvent");
    let back: OrderEvent = serde_json::from_str(&json).expect("deserialize OrderEvent");
    assert_eq!(back.order_id, "order-1");
    assert_eq!(back.event_type, OrderEventType::Placement);
    assert_eq!(back.side, Side::Buy);
}

// ── UserEvent serde ───────────────────────────────────────────────

#[test]
fn user_event_connected_serde() {
    let ev = UserEvent::Connected { reconnect: false };
    let json = serde_json::to_string(&ev).expect("serialize UserEvent::Connected");
    let back: UserEvent = serde_json::from_str(&json).expect("deserialize UserEvent::Connected");
    match back {
        UserEvent::Connected { reconnect } => assert!(!reconnect),
        _ => panic!("expected Connected variant"),
    }
}

#[test]
fn user_event_trade_serde() {
    let te = make_trade_event();
    let ev = UserEvent::Trade(te);
    let json = serde_json::to_string(&ev).expect("serialize UserEvent::Trade");
    let back: UserEvent = serde_json::from_str(&json).expect("deserialize UserEvent::Trade");
    match back {
        UserEvent::Trade(t) => {
            assert_eq!(t.id, "trade-1");
            assert_eq!(t.status, TradeStatus::Confirmed);
        }
        _ => panic!("expected Trade variant"),
    }
}

#[test]
fn user_event_raw_activity_serde() {
    let ev = UserEvent::RawActivity;
    let json = serde_json::to_string(&ev).expect("serialize UserEvent::RawActivity");
    let back: UserEvent = serde_json::from_str(&json).expect("deserialize UserEvent::RawActivity");
    assert!(matches!(back, UserEvent::RawActivity));
}

#[test]
fn user_event_disconnected_serde() {
    let ev = UserEvent::Disconnected;
    let json = serde_json::to_string(&ev).expect("serialize UserEvent::Disconnected");
    let back: UserEvent = serde_json::from_str(&json).expect("deserialize UserEvent::Disconnected");
    assert!(matches!(back, UserEvent::Disconnected));
}
