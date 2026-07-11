use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::Outcome;

/// Live order status as reported by the CLOB API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderStatus {
    Live,
    Matched,
    Delayed,
    Cancelled,
    Invalid,
}

impl std::fmt::Display for OrderStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OrderStatus::Live => write!(f, "LIVE"),
            OrderStatus::Matched => write!(f, "MATCHED"),
            OrderStatus::Delayed => write!(f, "DELAYED"),
            OrderStatus::Cancelled => write!(f, "CANCELLED"),
            OrderStatus::Invalid => write!(f, "INVALID"),
        }
    }
}

/// Side of an order (from Polymarket's perspective).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Side {
    Buy,
    Sell,
}

impl std::fmt::Display for Side {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Side::Buy => write!(f, "BUY"),
            Side::Sell => write!(f, "SELL"),
        }
    }
}

/// Order type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderType {
    GTC,
    GTD,
    FOK,
    FAK,
}

/// What the `size` field represents for an order request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderAmountKind {
    Shares,
    UsdcNotional,
}

/// A live order tracked by the system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveOrder {
    pub id: String,
    pub condition_id: String,
    pub asset_id: String,
    pub side: Side,
    pub price: Decimal,
    pub original_size: Decimal,
    pub size_matched: Decimal,
    pub outcome: Outcome,
    pub order_type: OrderType,
    pub status: OrderStatus,
    pub created_at: DateTime<Utc>,
    #[serde(default)]
    pub associated_trade_ids: Vec<String>,
}

impl LiveOrder {
    pub fn remaining_size(&self) -> Decimal {
        self.original_size - self.size_matched
    }

    pub fn is_fully_filled(&self) -> bool {
        self.size_matched >= self.original_size
    }

    pub fn is_active(&self) -> bool {
        self.status == OrderStatus::Live
    }

    /// Returns any associated trade IDs recorded for this live order.
    pub fn associated_trade_ids(&self) -> Option<Vec<String>> {
        (!self.associated_trade_ids.is_empty()).then(|| self.associated_trade_ids.clone())
    }
}

/// Parameters for creating a new order.
#[derive(Debug, Clone, Serialize)]
pub struct OrderRequest {
    pub token_id: String,
    pub price: Decimal,
    pub size: Decimal,
    pub amount_kind: OrderAmountKind,
    pub side: Side,
    pub order_type: OrderType,
    /// If true, order is rejected if it would immediately match (maker-only).
    pub post_only: bool,
    /// Whether this market uses the Neg Risk CTF Exchange.
    pub neg_risk: bool,
    /// Tick size for the market (e.g. "0.01", "0.001").
    pub tick_size: String,
}

/// EIP-712 signed order for Polymarket CLOB API.
#[derive(Debug, Clone, Serialize)]
pub struct SignedOrder {
    pub salt: String,
    pub maker: String,
    pub signer: String,
    pub taker: String,
    #[serde(rename = "tokenId")]
    pub token_id: String,
    #[serde(rename = "makerAmount")]
    pub maker_amount: String,
    #[serde(rename = "takerAmount")]
    pub taker_amount: String,
    pub expiration: String,
    pub nonce: String,
    #[serde(rename = "feeRateBps")]
    pub fee_rate_bps: String,
    pub side: String,
    #[serde(rename = "signatureType")]
    pub signature_type: u8,
    pub signature: String,
}

/// Wrapper payload for POST /order.
#[derive(Debug, Clone, Serialize)]
pub struct SignedOrderPayload {
    pub order: SignedOrder,
    pub owner: String,
    #[serde(rename = "orderType")]
    pub order_type: String,
}

/// Result from order placement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderResult {
    pub order_id: String,
    pub status: OrderStatus,
    pub trade_ids: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use rust_decimal_macros::dec;

    fn sample_live_order(
        order_id: &str,
        original_size: Decimal,
        size_matched: Decimal,
    ) -> LiveOrder {
        LiveOrder {
            id: order_id.to_string(),
            condition_id: "cond-1".to_string(),
            asset_id: "asset-1".to_string(),
            side: Side::Buy,
            price: dec!(0.50),
            original_size,
            size_matched,
            outcome: Outcome::Yes,
            order_type: OrderType::GTC,
            status: OrderStatus::Matched,
            created_at: Utc::now(),
            associated_trade_ids: Vec::new(),
        }
    }

    #[test]
    fn live_order_is_fully_filled_requires_full_size() {
        let full = sample_live_order("order-full-1", dec!(10), dec!(10));
        let partial = sample_live_order("order-partial-1", dec!(10), dec!(3));

        assert!(full.is_fully_filled());
        assert!(!partial.is_fully_filled());
    }

    #[test]
    fn live_order_associated_trade_ids_round_trip_from_struct_field() {
        let mut order = sample_live_order("order-associated-trades-1", dec!(10), dec!(10));
        assert_eq!(order.associated_trade_ids(), None);
        order.associated_trade_ids = vec!["trade-1".to_string(), "trade-2".to_string()];

        assert_eq!(
            order.associated_trade_ids(),
            Some(vec!["trade-1".to_string(), "trade-2".to_string()])
        );
    }
}
