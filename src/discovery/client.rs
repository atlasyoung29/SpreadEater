use anyhow::{Context, Result};
use chrono::Utc;
use rust_decimal::Decimal;
use serde::Deserialize;
use std::str::FromStr;
use tracing::{debug, info, warn};

use crate::config::DiscoveryConfig;
use crate::models::{Market, Outcome, RewardConfig, TokenInfo};

pub struct DiscoveryClient {
    http: reqwest::Client,
    config: DiscoveryConfig,
}

// Raw API response types for Polymarket sampling-markets endpoint
#[derive(Debug, Deserialize)]
struct SamplingMarketsResponse {
    data: Vec<SamplingMarket>,
}

#[derive(Debug, Deserialize)]
struct SamplingMarket {
    condition_id: String,
    market_slug: Option<String>,
    question: Option<String>,
    active: Option<bool>,
    closed: Option<bool>,
    archived: Option<bool>,
    accepting_orders: Option<bool>,
    neg_risk: Option<bool>,
    minimum_tick_size: Option<f64>,
    tokens: Option<Vec<SamplingToken>>,
    rewards: Option<SamplingRewards>,
    end_date_iso: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SamplingToken {
    token_id: String,
    outcome: String,
    price: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct SamplingRewards {
    rates: Option<Vec<SamplingRewardRate>>,
    min_size: Option<f64>,
    max_spread: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct SamplingRewardRate {
    rewards_daily_rate: Option<f64>,
}

impl DiscoveryClient {
    pub fn new(config: DiscoveryConfig) -> Self {
        Self {
            http: reqwest::Client::new(),
            config,
        }
    }

    /// Fetch a single market by condition_id from the sampling-markets API.
    /// Returns `None` if the market is not found (or not currently in the sampling set).
    pub async fn fetch_market_by_condition_id(&self, condition_id: &str) -> Result<Option<Market>> {
        let all = self.fetch_sampling_markets().await?;
        Ok(all.into_iter().find(|m| m.condition_id == condition_id))
    }

    pub async fn fetch_sampling_markets(&self) -> Result<Vec<Market>> {
        let url = format!("{}/sampling-markets", self.config.clob_base_url);
        info!(url = %url, "Fetching sampling markets");

        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .context("Failed to fetch sampling markets")?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Sampling markets API returned {}: {}", status, body);
        }

        let raw: SamplingMarketsResponse = resp
            .json()
            .await
            .context("Failed to parse sampling markets response")?;

        info!(count = raw.data.len(), "Received sampling markets");

        let markets: Vec<Market> = raw
            .data
            .into_iter()
            .filter_map(|m| self.convert_market(m))
            .collect();

        info!(converted = markets.len(), "Converted valid markets");
        Ok(markets)
    }

    fn convert_market(&self, raw: SamplingMarket) -> Option<Market> {
        let tokens_raw = raw.tokens.as_ref()?;

        // Must be binary (exactly 2 tokens)
        if tokens_raw.len() != 2 {
            debug!(
                condition_id = %raw.condition_id,
                token_count = tokens_raw.len(),
                "Skipping non-binary market"
            );
            return None;
        }

        let tokens: Vec<TokenInfo> = tokens_raw
            .iter()
            .filter_map(|t| {
                let outcome = match t.outcome.to_uppercase().as_str() {
                    "YES" => Outcome::Yes,
                    "NO" => Outcome::No,
                    _ => {
                        warn!(
                            condition_id = %raw.condition_id,
                            outcome = %t.outcome,
                            "Unknown outcome label"
                        );
                        return None;
                    }
                };
                Some(TokenInfo {
                    token_id: t.token_id.clone(),
                    outcome,
                    last_price: t.price.and_then(|p| Decimal::from_str(&p.to_string()).ok()),
                })
            })
            .collect();

        if tokens.len() != 2 {
            return None;
        }

        // Parse reward config
        let reward_config = raw.rewards.and_then(|r| {
            let rates: Vec<Decimal> = r
                .rates
                .unwrap_or_default()
                .iter()
                .filter_map(|rate| {
                    rate.rewards_daily_rate
                        .and_then(|v| Decimal::from_str(&v.to_string()).ok())
                })
                .collect();

            let total: Decimal = rates.iter().sum();
            if total <= Decimal::ZERO {
                return None;
            }

            Some(RewardConfig {
                condition_id: raw.condition_id.clone(),
                daily_reward_rates: rates,
                daily_reward_total: total,
                min_size: r
                    .min_size
                    .and_then(|v| Decimal::from_str(&v.to_string()).ok())
                    .unwrap_or(Decimal::ZERO),
                // API returns max_spread in cents; convert to 0-1 price units
                max_spread: r
                    .max_spread
                    .and_then(|v| Decimal::from_str(&v.to_string()).ok())
                    .map(|v| v / Decimal::from(100))
                    .unwrap_or(Decimal::ZERO),
            })
        });

        Some(Market {
            condition_id: raw.condition_id,
            market_slug: raw.market_slug.unwrap_or_default(),
            question: raw.question.unwrap_or_default(),
            active: raw.active.unwrap_or(false),
            closed: raw.closed.unwrap_or(false),
            archived: raw.archived.unwrap_or(false),
            accepting_orders: raw.accepting_orders.unwrap_or(false),
            is_binary: true,
            neg_risk: raw.neg_risk.unwrap_or(false),
            minimum_tick_size: format_tick_size(raw.minimum_tick_size.unwrap_or(0.01)),
            tokens,
            reward_config,
            end_date_iso: raw.end_date_iso,
            discovered_at: Utc::now(),
        })
    }
}

/// Format tick size float to clean string (e.g. 0.01 -> "0.01", 0.001 -> "0.001").
fn format_tick_size(v: f64) -> String {
    if v == 0.001 {
        "0.001".to_string()
    } else if v == 0.1 {
        "0.1".to_string()
    } else {
        "0.01".to_string()
    }
}
