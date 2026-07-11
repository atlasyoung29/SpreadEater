use anyhow::{Context, Result};
use chrono::Utc;
use rust_decimal::Decimal;
use serde::Deserialize;
use std::str::FromStr;
use tracing::{debug, info};

use crate::models::{OrderBookSnapshot, PriceLevel};

#[derive(Clone)]
pub struct BookRestClient {
    http: reqwest::Client,
    base_url: String,
}

#[derive(Debug, Deserialize)]
struct RawBookResponse {
    market: Option<String>,
    asset_id: Option<String>,
    timestamp: Option<String>,
    bids: Option<Vec<RawLevel>>,
    asks: Option<Vec<RawLevel>>,
}

#[derive(Debug, Deserialize)]
struct RawLevel {
    price: String,
    size: String,
}

impl BookRestClient {
    pub fn new(base_url: String) -> Self {
        Self {
            http: reqwest::Client::new(),
            base_url,
        }
    }

    pub async fn fetch_book(&self, token_id: &str) -> Result<OrderBookSnapshot> {
        let url = format!("{}/book?token_id={}", self.base_url, token_id);
        debug!(token_id = %token_id, "Fetching order book");

        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .context("Failed to fetch order book")?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Book API returned {}: {}", status, body);
        }

        let raw: RawBookResponse = resp.json().await.context("Failed to parse book response")?;

        let mut bids = parse_levels(raw.bids.unwrap_or_default());
        let mut asks = parse_levels(raw.asks.unwrap_or_default());

        // API returns bids low-to-high and asks high-to-low.
        // We need bids highest-first (best bid) and asks lowest-first (best ask).
        bids.sort_by(|a, b| b.price.cmp(&a.price));
        asks.sort_by(|a, b| a.price.cmp(&b.price));

        let snapshot = OrderBookSnapshot {
            token_id: token_id.to_string(),
            exchange_ts: None,
            ingest_ts: Utc::now(),
            bids,
            asks,
        };

        info!(
            token_id = %token_id,
            bid_levels = snapshot.bids.len(),
            ask_levels = snapshot.asks.len(),
            best_bid = ?snapshot.best_bid().map(|l| l.price),
            best_ask = ?snapshot.best_ask().map(|l| l.price),
            "Book bootstrapped"
        );

        Ok(snapshot)
    }

    pub async fn fetch_both_books(
        &self,
        yes_token_id: &str,
        no_token_id: &str,
    ) -> Result<(OrderBookSnapshot, OrderBookSnapshot)> {
        let (yes_book, no_book) =
            tokio::try_join!(self.fetch_book(yes_token_id), self.fetch_book(no_token_id),)?;
        Ok((yes_book, no_book))
    }
}

fn parse_levels(raw: Vec<RawLevel>) -> Vec<PriceLevel> {
    raw.into_iter()
        .filter_map(|l| {
            let price = Decimal::from_str(&l.price).ok()?;
            let size = Decimal::from_str(&l.size).ok()?;
            if size > Decimal::ZERO {
                Some(PriceLevel { price, size })
            } else {
                None
            }
        })
        .collect()
}
