use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use spreadeater::models::*;
use std::str::FromStr;

fn make_live_order(original_size: f64, size_matched: f64, status: OrderStatus) -> LiveOrder {
    LiveOrder {
        id: "order-1".to_string(),
        condition_id: "cond-1".to_string(),
        asset_id: "asset-1".to_string(),
        side: Side::Buy,
        price: dec!(0.50),
        original_size: Decimal::from_str(&format!("{}", original_size)).unwrap(),
        size_matched: Decimal::from_str(&format!("{}", size_matched)).unwrap(),
        outcome: Outcome::Yes,
        order_type: OrderType::GTC,
        status,
        created_at: Utc::now(),
        associated_trade_ids: Vec::new(),
    }
}

// ---------------------------------------------------------------------------
// OrderStatus Display
// ---------------------------------------------------------------------------

#[test]
fn order_status_display_live() {
    assert_eq!(format!("{}", OrderStatus::Live), "LIVE");
}

#[test]
fn order_status_display_matched() {
    assert_eq!(format!("{}", OrderStatus::Matched), "MATCHED");
}

#[test]
fn order_status_display_cancelled() {
    assert_eq!(format!("{}", OrderStatus::Cancelled), "CANCELLED");
}

#[test]
fn order_status_serde_roundtrip() {
    let variants = vec![
        OrderStatus::Live,
        OrderStatus::Matched,
        OrderStatus::Delayed,
        OrderStatus::Cancelled,
        OrderStatus::Invalid,
    ];
    for v in variants {
        let json = serde_json::to_string(&v).expect("serialize OrderStatus");
        let v2: OrderStatus = serde_json::from_str(&json).expect("deserialize OrderStatus");
        assert_eq!(v, v2);
    }
}

// ---------------------------------------------------------------------------
// Side Display and Serde
// ---------------------------------------------------------------------------

#[test]
fn side_display_buy() {
    assert_eq!(format!("{}", Side::Buy), "BUY");
}

#[test]
fn side_display_sell() {
    assert_eq!(format!("{}", Side::Sell), "SELL");
}

#[test]
fn side_serde_buy_uppercase() {
    let json = serde_json::to_string(&Side::Buy).expect("serialize Side::Buy");
    assert_eq!(json, "\"BUY\"");
}

#[test]
fn side_serde_sell_uppercase() {
    let json = serde_json::to_string(&Side::Sell).expect("serialize Side::Sell");
    assert_eq!(json, "\"SELL\"");
}

// ---------------------------------------------------------------------------
// OrderType Serde
// ---------------------------------------------------------------------------

#[test]
fn order_type_serde_roundtrip() {
    let variants = vec![
        OrderType::GTC,
        OrderType::GTD,
        OrderType::FOK,
        OrderType::FAK,
    ];
    for v in variants {
        let json = serde_json::to_string(&v).expect("serialize OrderType");
        let v2: OrderType = serde_json::from_str(&json).expect("deserialize OrderType");
        assert_eq!(v, v2);
    }
}

// ---------------------------------------------------------------------------
// LiveOrder methods
// ---------------------------------------------------------------------------

#[test]
fn live_order_remaining_size() {
    let order = make_live_order(10.0, 3.0, OrderStatus::Live);
    assert_eq!(order.remaining_size(), dec!(7));
}

#[test]
fn live_order_remaining_size_zero_matched() {
    let order = make_live_order(10.0, 0.0, OrderStatus::Live);
    assert_eq!(order.remaining_size(), dec!(10));
}

#[test]
fn live_order_is_fully_filled_true() {
    let order = make_live_order(10.0, 10.0, OrderStatus::Matched);
    assert!(order.is_fully_filled());
}

#[test]
fn live_order_is_fully_filled_false() {
    let order = make_live_order(10.0, 3.0, OrderStatus::Live);
    assert!(!order.is_fully_filled());
}

#[test]
fn live_order_is_active_live() {
    let order = make_live_order(10.0, 0.0, OrderStatus::Live);
    assert!(order.is_active());
}

#[test]
fn live_order_is_active_cancelled() {
    let order = make_live_order(10.0, 0.0, OrderStatus::Cancelled);
    assert!(!order.is_active());
}

#[test]
fn live_order_is_active_matched() {
    let order = make_live_order(10.0, 10.0, OrderStatus::Matched);
    assert!(!order.is_active());
}

// ---------------------------------------------------------------------------
// OrderResult Serde
// ---------------------------------------------------------------------------

#[test]
fn order_result_serde_roundtrip() {
    let result = OrderResult {
        order_id: "ord-123".to_string(),
        status: OrderStatus::Live,
        trade_ids: vec!["trade-1".to_string(), "trade-2".to_string()],
    };
    let json = serde_json::to_string(&result).expect("serialize OrderResult");
    let result2: OrderResult = serde_json::from_str(&json).expect("deserialize OrderResult");
    assert_eq!(result.order_id, result2.order_id);
    assert_eq!(result.status, result2.status);
    assert_eq!(result.trade_ids, result2.trade_ids);
}

// ---------------------------------------------------------------------------
// SignedOrder camelCase fields
// ---------------------------------------------------------------------------

#[test]
fn signed_order_camel_case_fields() {
    let signed = SignedOrder {
        salt: "1".to_string(),
        maker: "0xmaker".to_string(),
        signer: "0xsigner".to_string(),
        taker: "0xtaker".to_string(),
        token_id: "tok-1".to_string(),
        maker_amount: "1000".to_string(),
        taker_amount: "500".to_string(),
        expiration: "0".to_string(),
        nonce: "1".to_string(),
        fee_rate_bps: "0".to_string(),
        side: "BUY".to_string(),
        signature_type: 0,
        signature: "0xsig".to_string(),
    };
    let json = serde_json::to_string(&signed).expect("serialize SignedOrder");
    let value: serde_json::Value = serde_json::from_str(&json).unwrap();
    let obj = value.as_object().unwrap();
    // Verify camelCase keys are present
    assert!(obj.contains_key("tokenId"), "missing tokenId");
    assert!(obj.contains_key("makerAmount"), "missing makerAmount");
    assert!(obj.contains_key("takerAmount"), "missing takerAmount");
    assert!(obj.contains_key("feeRateBps"), "missing feeRateBps");
    assert!(obj.contains_key("signatureType"), "missing signatureType");
    // Verify snake_case keys are NOT present
    assert!(!obj.contains_key("token_id"), "unexpected token_id");
    assert!(!obj.contains_key("maker_amount"), "unexpected maker_amount");
    assert!(!obj.contains_key("taker_amount"), "unexpected taker_amount");
    assert!(!obj.contains_key("fee_rate_bps"), "unexpected fee_rate_bps");
    assert!(
        !obj.contains_key("signature_type"),
        "unexpected signature_type"
    );
}
