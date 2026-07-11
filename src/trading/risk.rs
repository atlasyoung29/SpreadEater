use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

use crate::config::RiskConfig;
use crate::models::Position;

#[derive(Debug, Clone)]
pub struct HaltMarketResult {
    pub newly_halted: bool,
    pub canonical_reason: String,
    pub suppressed_reason: Option<String>,
}

/// Per-market risk state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketRiskState {
    pub condition_id: String,
    pub halted: bool,
    pub halt_reason: Option<String>,
    pub unhedged_since: Option<chrono::DateTime<chrono::Utc>>,
    pub total_exposure: Decimal,
}

/// Risk manager enforcing hard limits and kill switches.
pub struct RiskManager {
    config: RiskConfig,
    markets: Arc<RwLock<HashMap<String, MarketRiskState>>>,
    global_halt: Arc<RwLock<bool>>,
    /// Cached USDC balance updated each cycle by LiveEngine.
    cached_balance: RwLock<Decimal>,
}

impl RiskManager {
    pub fn new(config: RiskConfig) -> Self {
        Self {
            config,
            markets: Arc::new(RwLock::new(HashMap::new())),
            global_halt: Arc::new(RwLock::new(false)),
            cached_balance: RwLock::new(Decimal::ZERO),
        }
    }

    /// Update the cached USDC balance (called by LiveEngine each cycle).
    pub async fn update_balance(&self, balance: Decimal) {
        *self.cached_balance.write().await = balance;
    }

    /// Check if a market is safe to trade.
    pub async fn is_market_tradable(&self, condition_id: &str) -> bool {
        if *self.global_halt.read().await {
            return false;
        }

        let markets = self.markets.read().await;
        match markets.get(condition_id) {
            Some(state) => !state.halted,
            None => true, // Unknown market is allowed until registered
        }
    }

    /// Register or update a market's risk state.
    pub async fn update_market_exposure(&self, condition_id: &str, position: &Position) {
        let exposure = position.net_exposure().abs();
        let tolerance = self.config.hedge_exposure_tolerance;
        let mut markets = self.markets.write().await;

        let state = markets
            .entry(condition_id.to_string())
            .or_insert_with(|| MarketRiskState {
                condition_id: condition_id.to_string(),
                halted: false,
                halt_reason: None,
                unhedged_since: None,
                total_exposure: Decimal::ZERO,
            });

        state.total_exposure = exposure;

        // Track unhedged exposure duration
        if exposure > tolerance && state.unhedged_since.is_none() {
            state.unhedged_since = Some(chrono::Utc::now());
        } else if exposure <= tolerance {
            state.unhedged_since = None;
        }
    }

    /// Check for unhedged exposure timeouts across all markets.
    pub async fn check_hedge_timeouts(&self) {
        let timeout = chrono::Duration::seconds(self.config.hedge_timeout_secs as i64);
        let now = chrono::Utc::now();
        let mut markets = self.markets.write().await;

        for (cid, state) in markets.iter_mut() {
            if let Some(since) = state.unhedged_since {
                if now - since > timeout && !state.halted {
                    state.halted = true;
                    state.halt_reason = Some(format!(
                        "Unhedged exposure for {}s exceeds timeout {}s",
                        (now - since).num_seconds(),
                        self.config.hedge_timeout_secs
                    ));
                    error!(
                        condition_id = %cid,
                        duration_secs = (now - since).num_seconds(),
                        "KILL SWITCH: Unhedged exposure timeout"
                    );
                }
            }
        }
    }

    /// Halt a specific market.
    pub async fn halt_market(&self, condition_id: &str, reason: &str) -> HaltMarketResult {
        let mut markets = self.markets.write().await;
        let state = markets
            .entry(condition_id.to_string())
            .or_insert_with(|| MarketRiskState {
                condition_id: condition_id.to_string(),
                halted: false,
                halt_reason: None,
                unhedged_since: None,
                total_exposure: Decimal::ZERO,
            });

        if state.halted {
            let canonical_reason = state
                .halt_reason
                .clone()
                .unwrap_or_else(|| reason.to_string());
            warn!(
                condition_id = %condition_id,
                attempted_reason = %reason,
                canonical_reason = %canonical_reason,
                "Suppressing duplicate market halt signal"
            );
            return HaltMarketResult {
                newly_halted: false,
                canonical_reason,
                suppressed_reason: Some(reason.to_string()),
            };
        }

        state.halted = true;
        state.halt_reason = Some(reason.to_string());

        warn!(
            condition_id = %condition_id,
            reason = %reason,
            "Market halted"
        );
        HaltMarketResult {
            newly_halted: true,
            canonical_reason: reason.to_string(),
            suppressed_reason: None,
        }
    }

    /// Resume a halted market.
    pub async fn resume_market(&self, condition_id: &str) {
        let mut markets = self.markets.write().await;
        if let Some(state) = markets.get_mut(condition_id) {
            state.halted = false;
            state.halt_reason = None;
            info!(condition_id = %condition_id, "Market resumed");
        }
    }

    /// Global halt — stops all trading.
    pub async fn global_halt(&self, reason: &str) {
        *self.global_halt.write().await = true;
        error!(reason = %reason, "GLOBAL HALT activated");
    }

    /// Check if the system is globally halted.
    pub async fn is_globally_halted(&self) -> bool {
        *self.global_halt.read().await
    }

    /// Get risk state for all markets.
    pub async fn get_all_states(&self) -> Vec<MarketRiskState> {
        self.markets.read().await.values().cloned().collect()
    }

    pub async fn get_market_state(&self, condition_id: &str) -> Option<MarketRiskState> {
        self.markets.read().await.get(condition_id).cloned()
    }

    /// Pre-trade check: validate that an order is within risk limits.
    /// `hedge_cost`: Some(price * size) for BUY hedges (USDC needed), None for SELL hedges.
    /// `is_hedge`: true when this order is an offsetting hedge (skip position cap since
    /// hedges *reduce* net exposure, not increase it).
    pub async fn pre_trade_check(
        &self,
        condition_id: &str,
        order_size: Decimal,
        hedge_cost: Option<Decimal>,
        is_hedge: bool,
        available_balance_override: Option<Decimal>,
    ) -> Result<(), String> {
        if *self.global_halt.read().await {
            return Err("Global halt is active".to_string());
        }

        let markets = self.markets.read().await;
        if let Some(state) = markets.get(condition_id) {
            if state.halted {
                return Err(format!(
                    "Market halted: {}",
                    state.halt_reason.as_deref().unwrap_or("unknown")
                ));
            }
        }

        // Balance check for BUY hedges (USDC outflow required)
        if let Some(cost) = hedge_cost {
            let balance = match available_balance_override {
                Some(balance) => balance,
                None => *self.cached_balance.read().await,
            };
            if balance < cost {
                return Err(format!(
                    "Insufficient balance for hedge: need {} USDC, have {}",
                    cost, balance
                ));
            }
        }

        Ok(())
    }
}
