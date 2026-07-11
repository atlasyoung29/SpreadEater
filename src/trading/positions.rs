use anyhow::{Context, Result};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

use crate::models::{AccountState, Position};

/// Manages position and balance state from the Polymarket data API.
pub struct PositionManager {
    http: reqwest::Client,
    data_api_url: String,
    address: String,
    state: Arc<RwLock<AccountState>>,
}

#[derive(Debug, Deserialize)]
struct RawPosition {
    #[serde(rename = "conditionId")]
    condition_id: Option<String>,
    size: Option<f64>,
    #[serde(rename = "avgPrice")]
    avg_price: Option<f64>,
    outcome: Option<String>,
}

impl PositionManager {
    pub fn new(data_api_url: String, address: String) -> Self {
        Self {
            http: reqwest::Client::new(),
            data_api_url,
            address,
            state: Arc::new(RwLock::new(AccountState::new())),
        }
    }

    /// Fetch current positions from the Polymarket data API and update local state.
    pub async fn sync_positions(&self) -> Result<()> {
        let url = format!("{}/positions?user={}", self.data_api_url, self.address);

        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .context("Failed to fetch positions")?;
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Positions API returned error: {}", body);
        }

        let raw_positions: Vec<RawPosition> = resp
            .json()
            .await
            .context("Failed to parse positions response")?;

        // Group by condition_id, merge YES/NO sides
        let mut positions_map: HashMap<String, Position> = HashMap::new();
        for rp in &raw_positions {
            let condition_id = match &rp.condition_id {
                Some(id) => id.clone(),
                None => continue,
            };

            let size = rp
                .size
                .map(|v| Decimal::from_str(&v.to_string()).unwrap_or(Decimal::ZERO))
                .unwrap_or(Decimal::ZERO);
            let avg_price = rp
                .avg_price
                .map(|v| Decimal::from_str(&v.to_string()).unwrap_or(Decimal::ZERO))
                .unwrap_or(Decimal::ZERO);

            let pos = positions_map
                .entry(condition_id.clone())
                .or_insert_with(|| Position::new(condition_id));

            match rp.outcome.as_deref() {
                Some("Yes" | "YES") => {
                    pos.yes_size = size;
                    pos.avg_yes_price = avg_price;
                }
                Some("No" | "NO") => {
                    pos.no_size = size;
                    pos.avg_no_price = avg_price;
                }
                _ => {}
            }
        }

        let positions: Vec<Position> = positions_map.into_values().collect();

        info!(
            count = positions.len(),
            raw_entries = raw_positions.len(),
            "Positions synced"
        );

        let mut state = self.state.write().await;
        state.positions = positions;

        Ok(())
    }

    /// Get a snapshot of account state.
    pub async fn get_state(&self) -> AccountState {
        self.state.read().await.clone()
    }

    /// Get position for a specific market.
    pub async fn get_position(&self, condition_id: &str) -> Option<Position> {
        self.state
            .read()
            .await
            .positions
            .iter()
            .find(|p| p.condition_id == condition_id)
            .cloned()
    }

    /// Update a single position (e.g., after a fill event).
    pub async fn update_position(&self, updated: Position) {
        let mut state = self.state.write().await;
        if let Some(pos) = state
            .positions
            .iter_mut()
            .find(|p| p.condition_id == updated.condition_id)
        {
            *pos = updated;
        } else {
            state.positions.push(updated);
        }
        debug!(condition_id = %state.positions.last().map(|p| p.condition_id.as_str()).unwrap_or("?"), "Position updated");
    }

    /// Total USDC locked in held positions (yes_size * avg_price + no_size * avg_price).
    /// This capital is committed and unavailable for new BUY orders.
    pub async fn total_position_cost(&self) -> Decimal {
        let state = self.state.read().await;
        let mut total = Decimal::ZERO;
        for pos in &state.positions {
            total += pos.yes_size * pos.avg_yes_price;
            total += pos.no_size * pos.avg_no_price;
        }
        total
    }

    /// Check if we have sufficient inventory for a sell order.
    pub async fn has_inventory(&self, condition_id: &str, outcome: &str, size: Decimal) -> bool {
        let state = self.state.read().await;
        match state
            .positions
            .iter()
            .find(|p| p.condition_id == condition_id)
        {
            Some(pos) => match outcome {
                "YES" => pos.has_yes_inventory(size),
                "NO" => pos.has_no_inventory(size),
                _ => false,
            },
            None => false,
        }
    }
}
