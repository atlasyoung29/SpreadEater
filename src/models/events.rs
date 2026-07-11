use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::order::Side;

/// Event from the user WebSocket channel when a trade occurs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeEvent {
    pub id: String,
    pub condition_id: String,
    pub asset_id: String,
    pub side: Side,
    pub price: Decimal,
    pub size: Decimal,
    pub outcome: String,
    pub status: TradeStatus,
    pub timestamp: DateTime<Utc>,
    pub maker_order_id: Option<String>,
    pub taker_order_id: Option<String>,
}

/// Trade lifecycle status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TradeStatus {
    Matched,
    Mined,
    Confirmed,
    Retrying,
    Failed,
}

/// Event from the user WebSocket channel for order state changes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderEvent {
    pub order_id: String,
    pub condition_id: String,
    pub asset_id: String,
    pub event_type: OrderEventType,
    pub side: Side,
    pub price: Decimal,
    pub original_size: Decimal,
    pub size_matched: Decimal,
    pub outcome: String,
    pub timestamp: DateTime<Utc>,
}

/// What happened to the order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderEventType {
    Placement,
    Update,
    Cancellation,
}

/// Combined user channel event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UserEvent {
    Connected { reconnect: bool },
    RawActivity,
    Trade(TradeEvent),
    Order(OrderEvent),
    Disconnected,
}
