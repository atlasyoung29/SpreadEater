use anyhow::{Context, Result};
use chrono::DateTime;
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::str::FromStr;
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info, warn};

use crate::auth::ApiCredentials;
use crate::models::events::{OrderEvent, OrderEventType, TradeEvent, TradeStatus, UserEvent};
use crate::models::order::Side;

const USER_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/user";
pub(crate) const USER_WS_HEARTBEAT_TEXT: &str = "PING";
pub(crate) const USER_WS_HEARTBEAT_RESPONSE_TEXT: &str = "PONG";

/// Authenticated WebSocket client for user order/trade events.
pub struct UserStream {
    credentials: ApiCredentials,
}

// Raw WS message types
#[derive(Debug, Deserialize)]
struct RawUserMessage {
    #[serde(rename = "type")]
    msg_type: Option<String>,
    // Trade fields
    id: Option<String>,
    market: Option<String>,
    asset_id: Option<String>,
    side: Option<String>,
    price: Option<String>,
    size: Option<String>,
    outcome: Option<String>,
    status: Option<String>,
    timestamp: Option<String>,
    maker_order_id: Option<String>,
    taker_order_id: Option<String>,
    // Order fields
    order_id: Option<String>,
    original_size: Option<String>,
    size_matched: Option<String>,
    event_type: Option<String>,
}

impl UserStream {
    pub fn new(credentials: ApiCredentials) -> Self {
        Self { credentials }
    }

    /// Subscribe to user events for specific markets.
    /// Returns a receiver channel that emits UserEvents.
    pub async fn subscribe(
        &self,
        condition_ids: Vec<String>,
    ) -> Result<mpsc::UnboundedReceiver<UserEvent>> {
        let (tx, rx) = mpsc::unbounded_channel();
        let credentials = self.credentials.clone();

        tokio::spawn(async move {
            let mut backoff_secs = 1u64;
            let mut reconnect = false;
            loop {
                let started = std::time::Instant::now();
                match run_user_ws(
                    credentials.clone(),
                    condition_ids.clone(),
                    tx.clone(),
                    reconnect,
                )
                .await
                {
                    Ok(()) => {
                        info!("User WebSocket closed cleanly");
                        break;
                    }
                    Err(e) => {
                        error!(error = %e, backoff_secs, "User WebSocket error, reconnecting");
                        let _ = tx.send(UserEvent::Disconnected);
                        // Reset backoff if connection lasted >60s (was healthy)
                        if started.elapsed().as_secs() > 60 {
                            backoff_secs = 1;
                        }
                        tokio::time::sleep(tokio::time::Duration::from_secs(backoff_secs)).await;
                        backoff_secs = (backoff_secs * 2).min(30);
                        reconnect = true;
                    }
                }
            }
        });

        Ok(rx)
    }
}

pub(crate) fn build_user_ws_auth_message(credentials: &ApiCredentials) -> Message {
    Message::Text(
        serde_json::json!({
            "auth": {
                "apiKey": credentials.api_key,
                "secret": credentials.secret,
                "passphrase": credentials.passphrase,
            },
            "type": "user",
        })
        .to_string()
        .into(),
    )
}

pub(crate) fn user_ws_text_is_pong(text: &str) -> bool {
    text.eq_ignore_ascii_case(USER_WS_HEARTBEAT_RESPONSE_TEXT)
}

async fn run_user_ws(
    credentials: ApiCredentials,
    condition_ids: Vec<String>,
    tx: mpsc::UnboundedSender<UserEvent>,
    reconnect: bool,
) -> Result<()> {
    info!(url = USER_WS_URL, "Connecting to user WebSocket");

    let (ws_stream, _) = connect_async(USER_WS_URL)
        .await
        .context("Failed to connect to user WebSocket")?;

    let (mut write, mut read) = ws_stream.split();

    // The current Polymarket user-channel docs show an auth request followed by
    // heartbeat pings, with market updates sent separately only when needed.
    // We keep the user stream subscribed to all account events and filter
    // client-side in LiveEngine for reliability.
    info!(
        requested_markets = condition_ids.len(),
        "Authenticating user stream for all account events"
    );

    write
        .send(build_user_ws_auth_message(&credentials))
        .await
        .context("Failed to send user auth subscription")?;
    write
        .send(Message::Text(USER_WS_HEARTBEAT_TEXT.into()))
        .await
        .context("Failed to send initial user heartbeat")?;

    info!(
        markets = condition_ids.len(),
        "User WebSocket authenticated — waiting for first server frame or heartbeat response"
    );

    let mut ping_interval = tokio::time::interval(tokio::time::Duration::from_secs(5));
    ping_interval.tick().await; // consume the immediate first tick

    let mut last_message_at = std::time::Instant::now();
    let heartbeat_timeout = std::time::Duration::from_secs(90);
    let mut message_count: u64 = 0;
    let mut connected_sent = false;

    loop {
        tokio::select! {
            msg = read.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        last_message_at = std::time::Instant::now();

                        if !connected_sent {
                            connected_sent = true;
                            info!("User WebSocket confirmed live — signalling Connected");
                            let _ = tx.send(UserEvent::Connected { reconnect });
                        }

                        if user_ws_text_is_pong(&text) {
                            if tx.send(UserEvent::RawActivity).is_err() {
                                info!("UserEvent receiver dropped, exiting");
                                return Ok(());
                            }
                            continue;
                        }

                        message_count += 1;

                        // Log first 10 messages at INFO to verify WS is alive
                        if message_count <= 10 {
                            info!(
                                msg_num = message_count,
                                len = text.len(),
                                preview = %if text.len() > 200 { &text[..200] } else { &text },
                                "User WS message received"
                            );
                        } else {
                            debug!(len = text.len(), "User WS text message received");
                        }

                        if let Some(event) = parse_user_message(&text) {
                            if tx.send(event).is_err() {
                                info!("UserEvent receiver dropped, exiting");
                                return Ok(());
                            }
                        } else if tx.send(UserEvent::RawActivity).is_err() {
                            info!("UserEvent receiver dropped, exiting");
                            return Ok(());
                        }
                    }
                    Some(Ok(Message::Ping(data))) => {
                        last_message_at = std::time::Instant::now();
                        if !connected_sent {
                            connected_sent = true;
                            info!("User WebSocket confirmed live via ping — signalling Connected");
                            let _ = tx.send(UserEvent::Connected { reconnect });
                        }
                        let _ = write.send(Message::Pong(data)).await;
                        if tx.send(UserEvent::RawActivity).is_err() {
                            info!("UserEvent receiver dropped, exiting");
                            return Ok(());
                        }
                    }
                    Some(Ok(Message::Pong(_))) => {
                        last_message_at = std::time::Instant::now();
                        if !connected_sent {
                            connected_sent = true;
                            info!("User WebSocket confirmed live via heartbeat — signalling Connected");
                            let _ = tx.send(UserEvent::Connected { reconnect });
                        }
                        if tx.send(UserEvent::RawActivity).is_err() {
                            info!("UserEvent receiver dropped, exiting");
                            return Ok(());
                        }
                    }
                    Some(Ok(Message::Close(_))) => {
                        info!("User WebSocket closed by server");
                        let _ = tx.send(UserEvent::Disconnected);
                        anyhow::bail!("Server closed connection");
                    }
                    Some(Err(e)) => {
                        let _ = tx.send(UserEvent::Disconnected);
                        anyhow::bail!("WebSocket error: {}", e);
                    }
                    None => {
                        let _ = tx.send(UserEvent::Disconnected);
                        anyhow::bail!("WebSocket stream ended");
                    }
                    _ => {}
                }
            }
            _ = ping_interval.tick() => {
                // Heartbeat: if no message for 15s, consider WS dead
                if last_message_at.elapsed() > heartbeat_timeout {
                    error!(
                        silence_secs = last_message_at.elapsed().as_secs(),
                        "User WS heartbeat timeout — no messages for {}s, reconnecting",
                        heartbeat_timeout.as_secs()
                    );
                    let _ = tx.send(UserEvent::Disconnected);
                    anyhow::bail!("Heartbeat timeout — WS silent for {}s", heartbeat_timeout.as_secs());
                }

                if write.send(Message::Text(USER_WS_HEARTBEAT_TEXT.into())).await.is_err() {
                    let _ = tx.send(UserEvent::Disconnected);
                    anyhow::bail!("Failed to send ping");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    #[test]
    fn build_user_ws_auth_message_matches_documented_shape() {
        let credentials = ApiCredentials {
            api_key: "key".to_string(),
            secret: "secret".to_string(),
            passphrase: "pass".to_string(),
            address: "0xabc".to_string(),
            private_key: None,
            funder: None,
        };

        let Message::Text(message) = build_user_ws_auth_message(&credentials) else {
            panic!("auth message should be text");
        };
        let payload: Value =
            serde_json::from_str(&message).expect("auth message should be valid json");

        assert_eq!(payload["type"], "user");
        assert_eq!(payload["auth"]["apiKey"], "key");
        assert_eq!(payload["auth"]["secret"], "secret");
        assert_eq!(payload["auth"]["passphrase"], "pass");
        assert!(payload.get("markets").is_none());
        assert!(payload.get("operation").is_none());
        assert!(payload.get("initial_dump").is_none());
    }

    #[test]
    fn user_ws_text_is_pong_is_case_insensitive() {
        assert!(user_ws_text_is_pong("PONG"));
        assert!(user_ws_text_is_pong("pong"));
        assert!(!user_ws_text_is_pong("PING"));
    }
}

fn parse_user_message(text: &str) -> Option<UserEvent> {
    let raw: RawUserMessage = match serde_json::from_str(text) {
        Ok(r) => r,
        Err(e) => {
            warn!(error = %e, "Failed to parse user WS JSON");
            debug!(raw = %text, "Unparseable WS message body");
            return None;
        }
    };

    let msg_type = match raw.msg_type.as_deref() {
        Some(t) => t,
        None => {
            debug!(raw = %text, "User WS message has no type field");
            return None;
        }
    };

    match msg_type.to_ascii_uppercase().as_str() {
        "TRADE" => match parse_trade_event(&raw) {
            Some(event) => {
                info!(
                    trade_id = %event.id,
                    condition_id = %event.condition_id,
                    asset_id = %event.asset_id,
                    side = %event.side,
                    price = %event.price,
                    size = %event.size,
                    status = ?event.status,
                    maker_order_id = ?event.maker_order_id,
                    taker_order_id = ?event.taker_order_id,
                    ">>> WS TRADE EVENT received"
                );
                Some(UserEvent::Trade(event))
            }
            None => {
                warn!(
                    side = ?raw.side,
                    price = ?raw.price,
                    size = ?raw.size,
                    market = ?raw.market,
                    asset_id = ?raw.asset_id,
                    status = ?raw.status,
                    "Failed to parse trade event — missing or invalid fields"
                );
                None
            }
        },
        "ORDER" | "PLACEMENT" | "CANCELLATION" => match parse_order_event(&raw) {
            Some(event) => {
                info!(
                    order_id = %event.order_id,
                    condition_id = %event.condition_id,
                    event_type = ?event.event_type,
                    side = %event.side,
                    price = %event.price,
                    ">>> WS ORDER EVENT received"
                );
                Some(UserEvent::Order(event))
            }
            None => {
                warn!(
                    order_id = ?raw.order_id,
                    event_type = ?raw.event_type,
                    side = ?raw.side,
                    market = ?raw.market,
                    "Failed to parse order event — missing or invalid fields"
                );
                None
            }
        },
        _ => {
            warn!(msg_type = %msg_type, "Unknown user WS message type (could be subscription ACK/error)");
            None
        }
    }
}

fn parse_trade_event(raw: &RawUserMessage) -> Option<TradeEvent> {
    let side = match raw.side.as_deref()? {
        "BUY" => Side::Buy,
        "SELL" => Side::Sell,
        _ => return None,
    };

    let status = match raw.status.as_deref()? {
        "MATCHED" => TradeStatus::Matched,
        "MINED" => TradeStatus::Mined,
        "CONFIRMED" => TradeStatus::Confirmed,
        "RETRYING" => TradeStatus::Retrying,
        "FAILED" => TradeStatus::Failed,
        _ => TradeStatus::Matched,
    };

    let timestamp = raw
        .timestamp
        .as_deref()
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .unwrap_or_else(chrono::Utc::now);

    Some(TradeEvent {
        id: raw.id.clone().unwrap_or_default(),
        condition_id: raw.market.clone().unwrap_or_default(),
        asset_id: raw.asset_id.clone().unwrap_or_default(),
        side,
        price: Decimal::from_str(raw.price.as_deref()?).ok()?,
        size: Decimal::from_str(raw.size.as_deref()?).ok()?,
        outcome: raw.outcome.clone().unwrap_or_default(),
        status,
        timestamp,
        maker_order_id: raw.maker_order_id.clone(),
        taker_order_id: raw.taker_order_id.clone(),
    })
}

fn parse_order_event(raw: &RawUserMessage) -> Option<OrderEvent> {
    let side = match raw.side.as_deref()? {
        "BUY" => Side::Buy,
        "SELL" => Side::Sell,
        _ => return None,
    };

    let event_type_str = raw.event_type.as_deref().or(raw.msg_type.as_deref())?;
    let event_type = match event_type_str.to_ascii_uppercase().as_str() {
        "PLACEMENT" | "ORDER" => OrderEventType::Placement,
        "UPDATE" => OrderEventType::Update,
        "CANCELLATION" | "CANCEL" => OrderEventType::Cancellation,
        _ => {
            debug!(event_type = %event_type_str, "Unknown order event type, treating as placement");
            OrderEventType::Placement
        }
    };

    let timestamp = raw
        .timestamp
        .as_deref()
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .unwrap_or_else(chrono::Utc::now);

    Some(OrderEvent {
        order_id: raw.id.clone().or(raw.order_id.clone()).unwrap_or_default(),
        condition_id: raw.market.clone().unwrap_or_default(),
        asset_id: raw.asset_id.clone().unwrap_or_default(),
        event_type,
        side,
        price: Decimal::from_str(raw.price.as_deref()?).ok()?,
        original_size: Decimal::from_str(raw.original_size.as_deref().unwrap_or("0")).ok()?,
        size_matched: Decimal::from_str(raw.size_matched.as_deref().unwrap_or("0")).ok()?,
        outcome: raw.outcome.clone().unwrap_or_default(),
        timestamp,
    })
}
