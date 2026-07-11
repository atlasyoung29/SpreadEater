use anyhow::{Context, Result};
use chrono::{DateTime, TimeZone, Utc};
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::Duration;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info, warn};

use crate::models::PriceLevel;

#[derive(Debug, Clone)]
pub enum BookEvent {
    /// Full snapshot replacement for a token
    Snapshot {
        token_id: String,
        bids: Vec<PriceLevel>,
        asks: Vec<PriceLevel>,
    },
    /// Incremental update: price levels changed
    Delta {
        token_id: String,
        bid_updates: Vec<PriceLevel>,
        ask_updates: Vec<PriceLevel>,
    },
    /// Connection lost
    Disconnected,
}

// Raw WS message types from Polymarket
#[derive(Debug, Deserialize)]
struct WsMessage {
    event_type: Option<String>,
    asset_id: Option<String>,
    bids: Option<Vec<WsLevel>>,
    asks: Option<Vec<WsLevel>>,
    price_changes: Option<Vec<WsPriceChange>>,
}

#[derive(Debug, Deserialize)]
struct WsLevel {
    price: String,
    size: String,
}

#[derive(Debug, Deserialize)]
struct WsPriceChange {
    asset_id: String,
    price: String,
    size: String,
    side: String,
}

#[derive(Debug)]
enum ParsedBookWsMessage {
    Events(Vec<BookEvent>),
    Ignored { event_type: Option<String> },
    Malformed,
}

#[derive(Debug, Default)]
pub struct BookWsStats {
    accepted_messages_resettable: AtomicU64,
    ignored_messages_resettable: AtomicU64,
    parse_errors_resettable: AtomicU64,
    snapshot_events_resettable: AtomicU64,
    delta_events_resettable: AtomicU64,
    accepted_messages_total: AtomicU64,
    ignored_messages_total: AtomicU64,
    parse_errors_total: AtomicU64,
    snapshot_events_total: AtomicU64,
    delta_events_total: AtomicU64,
    last_raw_message_at_ms: AtomicI64,
    last_parsed_event_at_ms: AtomicI64,
    last_parse_error_at_ms: AtomicI64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BookWsStatsSnapshot {
    pub accepted_messages: u64,
    pub ignored_messages: u64,
    pub parse_errors: u64,
    pub snapshot_events: u64,
    pub delta_events: u64,
    pub last_raw_message_at: Option<DateTime<Utc>>,
    pub last_parsed_event_at: Option<DateTime<Utc>>,
    pub last_parse_error_at: Option<DateTime<Utc>>,
}

impl BookWsStats {
    pub fn record_raw_message(&self) {
        self.last_raw_message_at_ms
            .store(Utc::now().timestamp_millis(), Ordering::Relaxed);
    }

    pub fn record_accepted(&self, events: &[BookEvent]) {
        let mut snapshot_events = 0u64;
        let mut delta_events = 0u64;
        for event in events {
            match event {
                BookEvent::Snapshot { .. } => snapshot_events += 1,
                BookEvent::Delta { .. } => delta_events += 1,
                BookEvent::Disconnected => {}
            }
        }
        self.accepted_messages_resettable
            .fetch_add(1, Ordering::Relaxed);
        self.accepted_messages_total.fetch_add(1, Ordering::Relaxed);
        if snapshot_events > 0 {
            self.snapshot_events_resettable
                .fetch_add(snapshot_events, Ordering::Relaxed);
            self.snapshot_events_total
                .fetch_add(snapshot_events, Ordering::Relaxed);
        }
        if delta_events > 0 {
            self.delta_events_resettable
                .fetch_add(delta_events, Ordering::Relaxed);
            self.delta_events_total
                .fetch_add(delta_events, Ordering::Relaxed);
        }
        self.last_parsed_event_at_ms
            .store(Utc::now().timestamp_millis(), Ordering::Relaxed);
    }

    pub fn record_ignored(&self) {
        self.ignored_messages_resettable
            .fetch_add(1, Ordering::Relaxed);
        self.ignored_messages_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_parse_error(&self) {
        self.parse_errors_resettable.fetch_add(1, Ordering::Relaxed);
        self.parse_errors_total.fetch_add(1, Ordering::Relaxed);
        self.last_parse_error_at_ms
            .store(Utc::now().timestamp_millis(), Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> BookWsStatsSnapshot {
        BookWsStatsSnapshot {
            accepted_messages: self.accepted_messages_total.load(Ordering::Relaxed),
            ignored_messages: self.ignored_messages_total.load(Ordering::Relaxed),
            parse_errors: self.parse_errors_total.load(Ordering::Relaxed),
            snapshot_events: self.snapshot_events_total.load(Ordering::Relaxed),
            delta_events: self.delta_events_total.load(Ordering::Relaxed),
            last_raw_message_at: timestamp_from_millis(
                self.last_raw_message_at_ms.load(Ordering::Relaxed),
            ),
            last_parsed_event_at: timestamp_from_millis(
                self.last_parsed_event_at_ms.load(Ordering::Relaxed),
            ),
            last_parse_error_at: timestamp_from_millis(
                self.last_parse_error_at_ms.load(Ordering::Relaxed),
            ),
        }
    }

    pub fn snapshot_and_reset(&self) -> BookWsStatsSnapshot {
        BookWsStatsSnapshot {
            accepted_messages: self.accepted_messages_resettable.swap(0, Ordering::Relaxed),
            ignored_messages: self.ignored_messages_resettable.swap(0, Ordering::Relaxed),
            parse_errors: self.parse_errors_resettable.swap(0, Ordering::Relaxed),
            snapshot_events: self.snapshot_events_resettable.swap(0, Ordering::Relaxed),
            delta_events: self.delta_events_resettable.swap(0, Ordering::Relaxed),
            last_raw_message_at: timestamp_from_millis(
                self.last_raw_message_at_ms.load(Ordering::Relaxed),
            ),
            last_parsed_event_at: timestamp_from_millis(
                self.last_parsed_event_at_ms.load(Ordering::Relaxed),
            ),
            last_parse_error_at: timestamp_from_millis(
                self.last_parse_error_at_ms.load(Ordering::Relaxed),
            ),
        }
    }
}

fn timestamp_from_millis(ts_ms: i64) -> Option<DateTime<Utc>> {
    if ts_ms <= 0 {
        return None;
    }
    Utc.timestamp_millis_opt(ts_ms).single()
}

pub struct BookWebSocket {
    ws_url: String,
    stats: Arc<BookWsStats>,
}

impl BookWebSocket {
    pub fn new(ws_url: String, stats: Arc<BookWsStats>) -> Self {
        Self { ws_url, stats }
    }

    /// Subscribe to book updates for a set of token IDs.
    /// Returns a receiver channel that emits BookEvents.
    /// Automatically reconnects with exponential backoff on disconnection.
    pub async fn subscribe(
        &self,
        token_ids: Vec<String>,
    ) -> Result<mpsc::UnboundedReceiver<BookEvent>> {
        let (tx, rx) = mpsc::unbounded_channel();
        let ws_url = self.ws_url.clone();
        let stats = self.stats.clone();

        tokio::spawn(async move {
            let mut backoff_secs = 1u64;
            loop {
                let started = std::time::Instant::now();
                match run_ws_loop(ws_url.clone(), token_ids.clone(), tx.clone(), stats.clone())
                    .await
                {
                    Ok(()) => {
                        info!("Book WebSocket closed cleanly");
                        break; // Only on clean close (receiver dropped)
                    }
                    Err(e) => {
                        error!(error = %e, backoff_secs, "Book WebSocket error, reconnecting");
                        let _ = tx.send(BookEvent::Disconnected);
                        // Reset backoff if connection lasted >60s (was healthy)
                        if started.elapsed().as_secs() > 60 {
                            backoff_secs = 1;
                        }
                        tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                        backoff_secs = (backoff_secs * 2).min(30);
                    }
                }
            }
        });

        Ok(rx)
    }
}

async fn run_ws_loop(
    ws_url: String,
    token_ids: Vec<String>,
    tx: mpsc::UnboundedSender<BookEvent>,
    stats: Arc<BookWsStats>,
) -> Result<()> {
    info!(url = %ws_url, tokens = token_ids.len(), "Connecting to market WebSocket");

    let (ws_stream, _) = connect_async(&ws_url)
        .await
        .context("Failed to connect to WebSocket")?;

    let (mut write, mut read) = ws_stream.split();

    let sub_msg = build_market_subscribe_message(&token_ids);
    write
        .send(Message::Text(sub_msg.to_string()))
        .await
        .context("Failed to send subscribe message")?;
    debug!(tokens = token_ids.len(), "Subscribed to market channel");

    info!(
        tokens = token_ids.len(),
        "Book WebSocket subscriptions active"
    );

    let mut ping_interval = tokio::time::interval(Duration::from_secs(15));
    ping_interval.tick().await; // consume the immediate first tick

    loop {
        tokio::select! {
            msg = read.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        stats.record_raw_message();
                        match parse_ws_message(&text) {
                            ParsedBookWsMessage::Events(events) => {
                                stats.record_accepted(&events);
                                for event in events {
                                    if tx.send(event).is_err() {
                                        info!("BookEvent receiver dropped, exiting WS loop");
                                        return Ok(());
                                    }
                                }
                            }
                            ParsedBookWsMessage::Ignored { event_type } => {
                                stats.record_ignored();
                                debug!(
                                    event_type = event_type.as_deref().unwrap_or("missing"),
                                    "Ignoring unsupported market WS message"
                                );
                            }
                            ParsedBookWsMessage::Malformed => {
                                stats.record_parse_error();
                                debug!(
                                    preview = preview_message(&text),
                                    "Failed to parse market WS message"
                                );
                            }
                        }
                    }
                    Some(Ok(Message::Ping(data))) => {
                        let _ = write.send(Message::Pong(data)).await;
                    }
                    Some(Ok(Message::Close(_))) => {
                        warn!("Book WebSocket closed by server");
                        let _ = tx.send(BookEvent::Disconnected);
                        anyhow::bail!("Server closed connection");
                    }
                    Some(Err(e)) => {
                        let _ = tx.send(BookEvent::Disconnected);
                        anyhow::bail!("WebSocket error: {}", e);
                    }
                    None => {
                        let _ = tx.send(BookEvent::Disconnected);
                        anyhow::bail!("WebSocket stream ended");
                    }
                    _ => {}
                }
            }
            _ = ping_interval.tick() => {
                if write.send(Message::Text(r#"{"type":"ping"}"#.to_string())).await.is_err() {
                    let _ = tx.send(BookEvent::Disconnected);
                    anyhow::bail!("Failed to send ping");
                }
            }
        }
    }
}

fn build_market_subscribe_message(token_ids: &[String]) -> serde_json::Value {
    serde_json::json!({
        "type": "market",
        "assets_ids": token_ids,
    })
}

fn parse_ws_message(text: &str) -> ParsedBookWsMessage {
    let msg: WsMessage = match serde_json::from_str(text) {
        Ok(msg) => msg,
        Err(_) => return ParsedBookWsMessage::Malformed,
    };

    match msg.event_type.as_deref() {
        Some("book") | Some("snapshot") => {
            let Some(token_id) = msg.asset_id else {
                return ParsedBookWsMessage::Malformed;
            };
            ParsedBookWsMessage::Events(vec![BookEvent::Snapshot {
                token_id,
                bids: parse_ws_levels(msg.bids.unwrap_or_default()),
                asks: parse_ws_levels(msg.asks.unwrap_or_default()),
            }])
        }
        Some("price_change") | Some("delta") | Some("update") => {
            let Some(price_changes) = msg.price_changes else {
                return ParsedBookWsMessage::Malformed;
            };
            let mut grouped: HashMap<String, (Vec<PriceLevel>, Vec<PriceLevel>)> = HashMap::new();
            for change in price_changes {
                let Some(level) = parse_ws_level(&change.price, &change.size) else {
                    continue;
                };
                let entry = grouped
                    .entry(change.asset_id)
                    .or_insert_with(|| (Vec::new(), Vec::new()));
                match change.side.as_str() {
                    "BUY" => entry.0.push(level),
                    "SELL" => entry.1.push(level),
                    _ => continue,
                }
            }

            if grouped.is_empty() {
                return ParsedBookWsMessage::Malformed;
            }

            ParsedBookWsMessage::Events(
                grouped
                    .into_iter()
                    .map(|(token_id, (bid_updates, ask_updates))| BookEvent::Delta {
                        token_id,
                        bid_updates,
                        ask_updates,
                    })
                    .collect(),
            )
        }
        other => ParsedBookWsMessage::Ignored {
            event_type: other.map(str::to_string),
        },
    }
}

fn parse_ws_levels(levels: Vec<WsLevel>) -> Vec<PriceLevel> {
    levels
        .into_iter()
        .filter_map(|l| parse_ws_level(&l.price, &l.size))
        .collect()
}

fn parse_ws_level(price: &str, size: &str) -> Option<PriceLevel> {
    let price = Decimal::from_str(price).ok()?;
    let size = Decimal::from_str(size).ok()?;
    Some(PriceLevel { price, size })
}

fn preview_message(text: &str) -> String {
    const MAX_PREVIEW_CHARS: usize = 160;
    let mut preview: String = text.chars().take(MAX_PREVIEW_CHARS).collect();
    if text.chars().count() > MAX_PREVIEW_CHARS {
        preview.push_str("...");
    }
    preview
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn subscribe_payload_matches_current_market_channel_shape() {
        let token_ids = vec!["a".to_string(), "b".to_string()];
        let payload = build_market_subscribe_message(&token_ids);
        assert_eq!(
            payload,
            json!({
                "type": "market",
                "assets_ids": ["a", "b"],
            })
        );
    }

    #[test]
    fn book_event_uses_event_type_and_asset_id() {
        let text = json!({
            "event_type": "book",
            "asset_id": "token-1",
            "bids": [{"price": "0.45", "size": "100"}],
            "asks": [{"price": "0.55", "size": "200"}]
        })
        .to_string();

        let parsed = parse_ws_message(&text);
        let ParsedBookWsMessage::Events(events) = parsed else {
            panic!("expected snapshot event");
        };
        assert_eq!(events.len(), 1);
        match &events[0] {
            BookEvent::Snapshot {
                token_id,
                bids,
                asks,
            } => {
                assert_eq!(token_id, "token-1");
                assert_eq!(bids.len(), 1);
                assert_eq!(bids[0].price, Decimal::new(45, 2));
                assert_eq!(asks.len(), 1);
                assert_eq!(asks[0].size, Decimal::new(200, 0));
            }
            _ => panic!("expected snapshot"),
        }
    }

    #[test]
    fn price_change_groups_updates_by_asset_id_and_side() {
        let text = json!({
            "event_type": "price_change",
            "price_changes": [
                {"asset_id": "token-1", "price": "0.45", "size": "100", "side": "BUY"},
                {"asset_id": "token-1", "price": "0.55", "size": "200", "side": "SELL"},
                {"asset_id": "token-2", "price": "0.22", "size": "0", "side": "BUY"}
            ]
        })
        .to_string();

        let parsed = parse_ws_message(&text);
        let ParsedBookWsMessage::Events(events) = parsed else {
            panic!("expected delta events");
        };
        assert_eq!(events.len(), 2);

        let mut deltas_by_token = HashMap::new();
        for event in events {
            match event {
                BookEvent::Delta {
                    token_id,
                    bid_updates,
                    ask_updates,
                } => {
                    deltas_by_token.insert(token_id, (bid_updates, ask_updates));
                }
                _ => panic!("expected delta"),
            }
        }

        let token_1 = deltas_by_token.get("token-1").expect("token-1 delta");
        assert_eq!(token_1.0.len(), 1);
        assert_eq!(token_1.0[0].price, Decimal::new(45, 2));
        assert_eq!(token_1.1.len(), 1);
        assert_eq!(token_1.1[0].price, Decimal::new(55, 2));

        let token_2 = deltas_by_token.get("token-2").expect("token-2 delta");
        assert_eq!(token_2.0.len(), 1);
        assert_eq!(token_2.0[0].size, Decimal::ZERO);
        assert!(token_2.1.is_empty());
    }

    #[test]
    fn unsupported_event_types_are_ignored() {
        let text = json!({
            "event_type": "best_bid_ask",
            "asset_id": "token-1"
        })
        .to_string();

        let parsed = parse_ws_message(&text);
        assert!(matches!(
            parsed,
            ParsedBookWsMessage::Ignored {
                event_type: Some(ref event_type)
            } if event_type == "best_bid_ask"
        ));
    }

    #[test]
    fn malformed_json_is_classified_as_parse_error() {
        let parsed = parse_ws_message("{not valid json");
        assert!(matches!(parsed, ParsedBookWsMessage::Malformed));
    }

    #[test]
    fn stats_snapshot_resets_counters() {
        let stats = BookWsStats::default();
        stats.record_raw_message();
        stats.record_accepted(&[
            BookEvent::Snapshot {
                token_id: "a".to_string(),
                bids: vec![],
                asks: vec![],
            },
            BookEvent::Delta {
                token_id: "a".to_string(),
                bid_updates: vec![],
                ask_updates: vec![],
            },
        ]);
        stats.record_ignored();
        stats.record_parse_error();

        let snapshot = stats.snapshot_and_reset();
        assert_eq!(
            snapshot,
            BookWsStatsSnapshot {
                accepted_messages: 1,
                ignored_messages: 1,
                parse_errors: 1,
                snapshot_events: 1,
                delta_events: 1,
                last_raw_message_at: Some(snapshot.last_raw_message_at.unwrap()),
                last_parsed_event_at: Some(snapshot.last_parsed_event_at.unwrap()),
                last_parse_error_at: Some(snapshot.last_parse_error_at.unwrap()),
            }
        );
        assert!(snapshot.last_raw_message_at.is_some());
        assert!(snapshot.last_parsed_event_at.is_some());
        assert!(snapshot.last_parse_error_at.is_some());

        let after_reset = stats.snapshot_and_reset();
        assert_eq!(after_reset.accepted_messages, 0);
        assert_eq!(after_reset.ignored_messages, 0);
        assert_eq!(after_reset.parse_errors, 0);
        assert_eq!(after_reset.snapshot_events, 0);
        assert_eq!(after_reset.delta_events, 0);
    }

    #[test]
    fn stats_snapshot_preserves_lifetime_counters() {
        let stats = BookWsStats::default();
        stats.record_raw_message();
        stats.record_accepted(&[BookEvent::Snapshot {
            token_id: "a".to_string(),
            bids: vec![],
            asks: vec![],
        }]);
        stats.record_parse_error();

        let snapshot = stats.snapshot();

        assert_eq!(snapshot.accepted_messages, 1);
        assert_eq!(snapshot.snapshot_events, 1);
        assert_eq!(snapshot.parse_errors, 1);
        assert!(snapshot.last_raw_message_at.is_some());
        assert!(snapshot.last_parsed_event_at.is_some());
        assert!(snapshot.last_parse_error_at.is_some());
    }
}
