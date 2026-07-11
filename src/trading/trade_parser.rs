use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::str::FromStr;

use crate::models::events::{TradeEvent, TradeStatus};
use crate::models::Side;

pub(crate) struct TradeEventInput {
    pub(crate) id: String,
    pub(crate) condition_id: String,
    pub(crate) asset_id: String,
    pub(crate) side: String,
    pub(crate) price: String,
    pub(crate) size: String,
    pub(crate) outcome: String,
    pub(crate) status: Option<String>,
    pub(crate) timestamp: Option<String>,
    pub(crate) maker_order_id: Option<String>,
    pub(crate) taker_order_id: Option<String>,
}

pub(crate) fn build_trade_event(input: TradeEventInput) -> Option<TradeEvent> {
    Some(TradeEvent {
        id: input.id,
        condition_id: input.condition_id,
        asset_id: input.asset_id,
        side: parse_trade_side(&input.side)?,
        price: Decimal::from_str(&input.price).ok()?,
        size: Decimal::from_str(&input.size).ok()?,
        outcome: input.outcome,
        status: parse_trade_status(input.status.as_deref()),
        timestamp: parse_trade_timestamp(input.timestamp.as_deref()),
        maker_order_id: input.maker_order_id,
        taker_order_id: input.taker_order_id,
    })
}

pub(crate) fn parse_trade_side(value: &str) -> Option<Side> {
    if value.eq_ignore_ascii_case("BUY") {
        Some(Side::Buy)
    } else if value.eq_ignore_ascii_case("SELL") {
        Some(Side::Sell)
    } else {
        None
    }
}

pub(crate) fn parse_trade_status(value: Option<&str>) -> TradeStatus {
    match value {
        Some(status) if status.eq_ignore_ascii_case("MATCHED") => TradeStatus::Matched,
        Some(status) if status.eq_ignore_ascii_case("MINED") => TradeStatus::Mined,
        Some(status) if status.eq_ignore_ascii_case("CONFIRMED") => TradeStatus::Confirmed,
        Some(status) if status.eq_ignore_ascii_case("RETRYING") => TradeStatus::Retrying,
        Some(status) if status.eq_ignore_ascii_case("FAILED") => TradeStatus::Failed,
        _ => TradeStatus::Matched,
    }
}

pub(crate) fn parse_trade_timestamp(value: Option<&str>) -> DateTime<Utc> {
    value
        .and_then(|raw| DateTime::parse_from_rfc3339(raw).ok())
        .map(|parsed| parsed.with_timezone(&Utc))
        .unwrap_or_else(Utc::now)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn normalizes_rest_and_ws_style_trade_inputs_to_the_same_event_shape() {
        let rest = build_trade_event(TradeEventInput {
            id: "trade-1".to_string(),
            condition_id: "condition-1".to_string(),
            asset_id: "asset-1".to_string(),
            side: "BUY".to_string(),
            price: "0.42".to_string(),
            size: "7".to_string(),
            outcome: "YES".to_string(),
            status: Some("matched".to_string()),
            timestamp: Some("2026-03-29T00:00:00Z".to_string()),
            maker_order_id: Some("maker-1".to_string()),
            taker_order_id: Some("taker-1".to_string()),
        })
        .expect("rest trade should parse");
        let ws = build_trade_event(TradeEventInput {
            id: "trade-1".to_string(),
            condition_id: "condition-1".to_string(),
            asset_id: "asset-1".to_string(),
            side: "buy".to_string(),
            price: "0.42".to_string(),
            size: "7".to_string(),
            outcome: "YES".to_string(),
            status: Some("MATCHED".to_string()),
            timestamp: Some("2026-03-29T00:00:00Z".to_string()),
            maker_order_id: Some("maker-1".to_string()),
            taker_order_id: Some("taker-1".to_string()),
        })
        .expect("ws trade should parse");

        assert_eq!(rest.side, ws.side);
        assert_eq!(rest.status, ws.status);
        assert_eq!(rest.timestamp, ws.timestamp);
        assert_eq!(rest.price, dec!(0.42));
        assert_eq!(rest.size, dec!(7));
    }

    #[test]
    fn rejects_invalid_trade_side() {
        let event = build_trade_event(TradeEventInput {
            id: "trade-1".to_string(),
            condition_id: "condition-1".to_string(),
            asset_id: "asset-1".to_string(),
            side: "HOLD".to_string(),
            price: "0.42".to_string(),
            size: "7".to_string(),
            outcome: "YES".to_string(),
            status: None,
            timestamp: None,
            maker_order_id: None,
            taker_order_id: None,
        });

        assert!(event.is_none());
    }
}
