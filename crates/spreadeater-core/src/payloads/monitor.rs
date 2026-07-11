use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Payload for MonitorDegraded events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitorDegradedPayload {
    pub component: String,
    pub degraded_reason: String,
    pub queue_depth: Option<u64>,
    pub index_lag_ms: Option<u64>,
}

/// Payload for RiskStateChanged events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskStateChangedPayload {
    pub scope: String,
    pub status: String,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub total_exposure: Option<Decimal>,
    #[serde(default)]
    pub global_halt: Option<bool>,
}

/// Payload for UserStreamStatusChanged events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserStreamStatusChangedPayload {
    pub status: String,
    #[serde(default)]
    pub subscribed_markets: Option<u64>,
    #[serde(default)]
    pub detail: Option<String>,
}

/// Payload for StatusSnapshot events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusSnapshotPayload {
    pub managed_markets: u64,
    pub order_committed_usd: Decimal,
    pub position_committed_usd: Decimal,
    pub total_committed_usd: Decimal,
    pub api_balance_usd: Decimal,
    pub available_budget_usd: Decimal,
    pub competition_multiplier: Decimal,
    #[serde(default)]
    pub total_est_daily_usd: Option<Decimal>,
    #[serde(default)]
    pub book_ws_accepted_messages: Option<u64>,
    #[serde(default)]
    pub book_ws_ignored_messages: Option<u64>,
    #[serde(default)]
    pub book_ws_parse_errors: Option<u64>,
    #[serde(default)]
    pub book_ws_snapshot_events: Option<u64>,
    #[serde(default)]
    pub book_ws_delta_events: Option<u64>,
}

/// Payload for CalibrationAdjusted events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CalibrationAdjustedPayload {
    pub old_multiplier: Decimal,
    pub new_multiplier: Decimal,
    pub sample_count: u64,
    pub false_positives: u64,
    pub false_negatives: u64,
}

/// Payload for WatchdogVerdict events (emitted each assessment cycle).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatchdogVerdictPayload {
    pub ws_verdict: String,
    pub status_verdict: String,
    pub escalation_level: String,
    #[serde(default)]
    pub ws_reason: Option<String>,
    #[serde(default)]
    pub status_reason: Option<String>,
    pub book_ws_connected: bool,
    pub user_ws_connected: bool,
    pub enforcement_enabled: bool,
    pub kill_actions_suppressed: bool,
    #[serde(default)]
    pub last_raw_book_ws_message_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub last_parsed_book_event_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub last_book_parse_error_at: Option<DateTime<Utc>>,
    pub book_ws_accepted_messages: u64,
    pub book_ws_ignored_messages: u64,
    pub book_ws_parse_errors: u64,
    pub book_ws_snapshot_events: u64,
    pub book_ws_delta_events: u64,
}

/// Payload for WatchdogKillTriggered events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatchdogKillTriggeredPayload {
    pub reason: String,
    pub escalation_level: String,
    pub time_in_critical_secs: u64,
}
