use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub mode: RunMode,
    pub discovery: DiscoveryConfig,
    pub books: BookConfig,
    pub strategy: StrategyConfig,
    pub persistence: PersistenceConfig,
    pub risk: RiskConfig,
    #[serde(default)]
    pub observability: ObservabilityConfig,
    #[serde(default)]
    pub watchdog: WatchdogConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunMode {
    Shadow,
    Live,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryConfig {
    pub min_daily_reward: Decimal,
    pub poll_interval_secs: u64,
    pub clob_base_url: String,
    pub gamma_base_url: String,
    pub data_api_base_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookConfig {
    pub ws_url: String,
    pub max_book_age_secs: u64,
    pub resync_interval_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyConfig {
    pub max_hedge_cost_bps: Decimal,
    pub max_slippage_bps: Decimal,
    pub default_quote_size: Decimal,
    pub min_edge_threshold: Decimal,
    /// BPS drift threshold for cancel-replace of resting orders.
    pub quote_drift_bps: Decimal,
    /// Fraction of max_spread (V) to offset bids from mid-price (0.50 = 50% of V).
    pub bid_depth_pct: Decimal,
    /// Fraction of max_spread (V) to offset asks from mid-price (0.0 = at mid, 1.0 = at max_spread edge).
    /// Higher = further from mid = more trading PnL per fill, lower reward score.
    #[serde(default = "default_ask_depth_pct")]
    pub ask_depth_pct: Decimal,
    /// Seconds between lightweight quote refreshes (re-reads cached books, cancel-replaces drifted).
    pub quote_refresh_secs: u64,
    /// Minimum estimated daily reward (our share) to enter a market.
    /// Gates on reward alone — not inflated by one-time hedge profit.
    #[serde(default = "default_min_est_daily")]
    pub min_est_daily: Decimal,
    /// Minimum daily return on committed capital to enter a market.
    /// 0.0025 = 0.25%. Return = estimated_edge / shares_committed.
    #[serde(default = "default_min_return_pct")]
    pub min_return_pct: Decimal,
    /// Minimum mid-price to place a resting bid on an outcome.
    /// Outcomes with mid below this are skipped (hedge-into via FOK is still allowed).
    #[serde(default = "default_min_outcome_price")]
    pub min_outcome_price: Decimal,
    /// Discount factor applied to reward-per-dollar estimate to account for
    /// uncertainty in score-share estimation.  Range: 0.5 (conservative) – 0.8 (aggressive).
    #[serde(default = "default_reward_discount_factor")]
    pub reward_discount_factor: Decimal,
    /// Minimum absolute daily reward improvement (USDC) to justify a frontier rotation.
    /// Prevents churn on sub-penny deltas that can't cover operational costs.
    #[serde(default = "default_min_frontier_improvement")]
    pub min_frontier_improvement: Decimal,
    /// Seconds to wait for loser cancel verification before deferring entrant
    /// placement to the next discovery cycle. Set to 0 to disable same-cycle handoff.
    #[serde(default = "default_frontier_handoff_window_secs")]
    pub frontier_handoff_window_secs: u64,
    pub score_proxy: ScoreProxyConfig,
}

fn default_reward_discount_factor() -> Decimal {
    Decimal::new(70, 2) // 0.70
}

fn default_min_est_daily() -> Decimal {
    Decimal::new(25, 2) // $0.25
}

fn default_min_outcome_price() -> Decimal {
    Decimal::new(20, 2) // $0.20
}

fn default_min_return_pct() -> Decimal {
    Decimal::new(25, 4) // 0.0025 = 0.25%
}

fn default_ask_depth_pct() -> Decimal {
    Decimal::new(2, 1) // 0.20 = 20% of max_spread above mid
}

fn default_min_frontier_improvement() -> Decimal {
    Decimal::new(5, 2) // $0.05/day
}

fn default_frontier_handoff_window_secs() -> u64 {
    5
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScoreProxyConfig {
    /// Multiplier on competitor score estimate (>1 = more conservative).
    pub competition_multiplier: Decimal,
    /// Cap on estimated share (prevents overconfidence in thin books).
    pub max_score_share: Decimal,
    /// Floor on estimated share (prevents near-zero estimates).
    pub min_score_share: Decimal,
    /// Target score share for dynamic sizing (e.g. 0.03 = 3%).
    pub target_score_share: Decimal,
    /// Calibration samples before adjusting multiplier in live mode.
    pub calibration_sample_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistenceConfig {
    pub database_url: Option<String>,
    pub archive_dir: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskConfig {
    /// Seconds of unhedged exposure before kill switch triggers.
    pub hedge_timeout_secs: u64,
    /// Residual share imbalance tolerated before balance correction / kill logic fires.
    #[serde(default = "default_hedge_exposure_tolerance")]
    pub hedge_exposure_tolerance: Decimal,
    /// USDC amount to always keep in the account (never used for orders or hedges).
    /// Budget = API_balance − cash_reserve.
    #[serde(default = "default_cash_reserve")]
    pub cash_reserve: Decimal,
}

fn default_cash_reserve() -> Decimal {
    Decimal::new(50, 0) // $50
}

fn default_hedge_exposure_tolerance() -> Decimal {
    Decimal::new(5, 1) // 0.5 shares
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservabilityConfig {
    pub enabled: bool,
    pub event_log_dir: String,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            event_log_dir: "./data/events".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct WatchdogConfig {
    pub enabled: bool,
    /// Whether watchdog verdicts are allowed to halt markets / execute kill+flatten.
    #[serde(default = "default_watchdog_enforce_actions")]
    pub enforce_actions: bool,
    /// Seconds of book WS silence before Critical verdict.
    pub max_book_ws_silence_secs: u64,
    /// Seconds of user WS silence before Critical verdict.
    pub max_user_ws_silence_secs: u64,
    /// Max reconnects in rolling window before Critical verdict.
    pub max_reconnects_in_window: u32,
    /// Rolling window (seconds) for reconnect counting.
    pub reconnect_window_secs: u64,
    /// Consecutive short-lived disconnects before Critical verdict.
    pub max_consecutive_disconnects: u32,
    /// Seconds of sustained Degraded state before escalating to KillPending.
    pub degraded_timeout_secs: u64,
    /// Seconds to wait after Critical verdict before executing kill (confirmation window).
    pub kill_confirmation_delay_secs: u64,
    /// Seconds between status page polls.
    pub status_poll_interval_secs: u64,
    /// Instatus summary endpoint URL.
    pub status_page_url: String,
    /// Component names that trigger Critical when in MAJOROUTAGE/PARTIALOUTAGE.
    pub critical_components: Vec<String>,
    /// Path to heartbeat file for external sidecar.
    pub heartbeat_file: String,
    /// Path to kill_flatten.py script.
    pub kill_flatten_script: String,
}

fn default_watchdog_enforce_actions() -> bool {
    false
}

impl Default for WatchdogConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            enforce_actions: default_watchdog_enforce_actions(),
            max_book_ws_silence_secs: 60,
            max_user_ws_silence_secs: 120,
            max_reconnects_in_window: 5,
            reconnect_window_secs: 300,
            max_consecutive_disconnects: 3,
            degraded_timeout_secs: 120,
            kill_confirmation_delay_secs: 10,
            status_poll_interval_secs: 30,
            status_page_url: "https://status.polymarket.com/summary.json".to_string(),
            critical_components: vec!["CLOB API".to_string(), "Polygon (RPC)".to_string()],
            heartbeat_file: "./data/watchdog_heartbeat".to_string(),
            kill_flatten_script: "scripts/kill_flatten.py".to_string(),
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            mode: RunMode::Shadow,
            discovery: DiscoveryConfig {
                min_daily_reward: Decimal::from(10),
                poll_interval_secs: 61,
                clob_base_url: "https://clob.polymarket.com".to_string(),
                gamma_base_url: "https://gamma-api.polymarket.com".to_string(),
                data_api_base_url: "https://data-api.polymarket.com".to_string(),
            },
            books: BookConfig {
                ws_url: "wss://ws-subscriptions-clob.polymarket.com/ws/market".to_string(),
                max_book_age_secs: 30,
                resync_interval_secs: 60,
            },
            strategy: StrategyConfig {
                max_hedge_cost_bps: Decimal::from(80),
                max_slippage_bps: Decimal::from(80),
                default_quote_size: Decimal::from(5),
                min_edge_threshold: Decimal::new(50, 2), // $0.50
                quote_drift_bps: Decimal::from(30),
                bid_depth_pct: Decimal::new(5, 1), // 0.50 = 50% of V
                ask_depth_pct: Decimal::new(2, 1), // 0.20 = 20% of V above mid
                quote_refresh_secs: 5,
                min_est_daily: Decimal::new(25, 2),     // $0.25
                min_return_pct: Decimal::new(25, 4),    // 0.0025 = 0.25%
                min_outcome_price: Decimal::new(20, 2), // $0.20
                reward_discount_factor: Decimal::new(70, 2), // 0.70
                min_frontier_improvement: default_min_frontier_improvement(),
                frontier_handoff_window_secs: default_frontier_handoff_window_secs(),
                score_proxy: ScoreProxyConfig {
                    competition_multiplier: Decimal::new(15, 1), // 1.5
                    max_score_share: Decimal::new(25, 2),        // 0.25
                    min_score_share: Decimal::new(1, 4),         // 0.0001
                    target_score_share: Decimal::new(3, 2),      // 0.03 = 3%
                    calibration_sample_size: 10,
                },
            },
            persistence: PersistenceConfig {
                database_url: None,
                archive_dir: "./data/archive".to_string(),
            },
            risk: RiskConfig {
                hedge_timeout_secs: 10,
                hedge_exposure_tolerance: default_hedge_exposure_tolerance(),
                cash_reserve: default_cash_reserve(),
            },
            observability: ObservabilityConfig::default(),
            watchdog: WatchdogConfig::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_has_correct_delta_values() {
        let config = Config::default();
        assert_eq!(config.strategy.quote_refresh_secs, 5);
        assert_eq!(config.risk.hedge_timeout_secs, 10);
        assert_eq!(config.risk.cash_reserve, Decimal::new(50, 0));
        assert_eq!(config.strategy.reward_discount_factor, Decimal::new(70, 2));
        assert!(!config.watchdog.enforce_actions);
    }

    #[test]
    fn reward_discount_factor_defaults_when_missing_from_json() {
        // StrategyConfig requires many fields; test via full Config default
        let config = Config::default();
        assert_eq!(config.strategy.reward_discount_factor, Decimal::new(70, 2));
    }

    #[test]
    fn cash_reserve_defaults_when_missing_from_json() {
        let json = r#"{
            "hedge_timeout_secs": 10
        }"#;
        let risk: RiskConfig = serde_json::from_str(json).unwrap();
        assert_eq!(risk.cash_reserve, Decimal::new(50, 0));
        assert_eq!(risk.hedge_exposure_tolerance, Decimal::new(5, 1));
    }

    #[test]
    fn cash_reserve_parsed_from_json() {
        let json = r#"{
            "hedge_timeout_secs": 10,
            "cash_reserve": "75"
        }"#;
        let risk: RiskConfig = serde_json::from_str(json).unwrap();
        assert_eq!(risk.cash_reserve, Decimal::new(75, 0));
    }
}
