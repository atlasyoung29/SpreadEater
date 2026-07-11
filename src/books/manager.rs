use chrono::Utc;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, warn};

use super::BookEvent;
use crate::models::{OrderBookSnapshot, PriceLevel};

/// Thread-safe in-memory book store for all tracked tokens.
pub struct BookManager {
    books: Arc<RwLock<HashMap<String, OrderBookSnapshot>>>,
}

impl BookManager {
    pub fn new() -> Self {
        Self {
            books: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Insert or replace a full book snapshot.
    pub async fn insert_snapshot(&self, snapshot: OrderBookSnapshot) {
        let token_id = snapshot.token_id.clone();
        self.books.write().await.insert(token_id, snapshot);
    }

    /// Get a clone of the current book for a token.
    pub async fn get_book(&self, token_id: &str) -> Option<OrderBookSnapshot> {
        self.books.read().await.get(token_id).cloned()
    }

    /// Get books for both YES and NO tokens of a market.
    pub async fn get_pair(
        &self,
        yes_token_id: &str,
        no_token_id: &str,
    ) -> Option<(OrderBookSnapshot, OrderBookSnapshot)> {
        let books = self.books.read().await;
        let yes = books.get(yes_token_id)?.clone();
        let no = books.get(no_token_id)?.clone();
        Some((yes, no))
    }

    /// Apply a WebSocket book event.
    pub async fn apply_event(&self, event: BookEvent) {
        match event {
            BookEvent::Snapshot {
                token_id,
                mut bids,
                mut asks,
            } => {
                // Bids: highest first (best bid). Asks: lowest first (best ask).
                bids.sort_by(|a, b| b.price.cmp(&a.price));
                asks.sort_by(|a, b| a.price.cmp(&b.price));
                let snapshot = OrderBookSnapshot {
                    token_id: token_id.clone(),
                    exchange_ts: None,
                    ingest_ts: Utc::now(),
                    bids,
                    asks,
                };
                self.insert_snapshot(snapshot).await;
                debug!(token_id = %token_id, "Applied WS snapshot");
            }
            BookEvent::Delta {
                token_id,
                bid_updates,
                ask_updates,
            } => {
                let mut books = self.books.write().await;
                if let Some(book) = books.get_mut(&token_id) {
                    apply_delta(&mut book.bids, bid_updates, true);
                    apply_delta(&mut book.asks, ask_updates, false);
                    book.ingest_ts = Utc::now();
                    debug!(token_id = %token_id, "Applied WS delta");
                } else {
                    warn!(token_id = %token_id, "Delta for unknown token, ignoring");
                }
            }
            BookEvent::Disconnected => {
                warn!("WebSocket disconnected, books may be stale");
            }
        }
    }

    /// Check if a token's book is stale.
    pub async fn is_stale(&self, token_id: &str, max_age: chrono::Duration) -> bool {
        match self.books.read().await.get(token_id) {
            Some(book) => book.is_stale(max_age),
            None => true,
        }
    }

    /// Number of tracked books.
    pub async fn count(&self) -> usize {
        self.books.read().await.len()
    }
}

/// Apply delta updates to a sorted price level list.
/// Size of 0 means remove the level.
/// `is_bids`: true for bid side (sort descending), false for ask side (sort ascending).
fn apply_delta(levels: &mut Vec<PriceLevel>, updates: Vec<PriceLevel>, is_bids: bool) {
    for update in updates {
        if let Some(pos) = levels.iter().position(|l| l.price == update.price) {
            if update.size == Decimal::ZERO {
                levels.remove(pos);
            } else {
                levels[pos].size = update.size;
            }
        } else if update.size > Decimal::ZERO {
            levels.push(update);
        }
    }
    if is_bids {
        levels.sort_by(|a, b| b.price.cmp(&a.price)); // highest first
    } else {
        levels.sort_by(|a, b| a.price.cmp(&b.price)); // lowest first
    }
}
