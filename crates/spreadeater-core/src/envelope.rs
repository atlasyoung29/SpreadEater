use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Schema version for forward-compatible evolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaVersion {
    pub major: u16,
    pub minor: u16,
}

impl SchemaVersion {
    pub const V1_0: Self = Self { major: 1, minor: 0 };
    pub const V1_1: Self = Self { major: 1, minor: 1 };
    pub const V1_2: Self = Self { major: 1, minor: 2 };
    pub const V1_3: Self = Self { major: 1, minor: 3 };
    pub const V1_4: Self = Self { major: 1, minor: 4 };
    pub const V1_5: Self = Self { major: 1, minor: 5 };
}

impl std::fmt::Display for SchemaVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.major, self.minor)
    }
}

/// Domain event types emitted by the bot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EventType {
    #[serde(rename = "decision_evaluated", alias = "DecisionEvaluated")]
    DecisionEvaluated,
    #[serde(rename = "quote_approved", alias = "QuoteApproved")]
    QuoteApproved,
    #[serde(rename = "quote_rejected", alias = "QuoteRejected")]
    QuoteRejected,
    #[serde(rename = "order_submitted", alias = "OrderSubmitted")]
    OrderSubmitted,
    #[serde(rename = "order_resized", alias = "OrderResized")]
    OrderResized,
    #[serde(rename = "order_cancelled", alias = "OrderCancelled")]
    OrderCancelled,
    #[serde(rename = "fill_detected", alias = "FillDetected")]
    FillDetected,
    #[serde(rename = "hedge_intent_created", alias = "HedgeIntentCreated")]
    HedgeIntentCreated,
    #[serde(rename = "hedge_decision_evaluated", alias = "HedgeDecisionEvaluated")]
    HedgeDecisionEvaluated,
    #[serde(rename = "hedge_result_recorded", alias = "HedgeResultRecorded")]
    HedgeResultRecorded,
    #[serde(rename = "hedge_exit_path_recorded", alias = "HedgeExitPathRecorded")]
    HedgeExitPathRecorded,
    #[serde(rename = "neutrality_evaluated", alias = "NeutralityEvaluated")]
    NeutralityEvaluated,
    #[serde(rename = "monitor_degraded", alias = "MonitorDegraded")]
    MonitorDegraded,
    #[serde(rename = "risk_state_changed", alias = "RiskStateChanged")]
    RiskStateChanged,
    #[serde(
        rename = "user_stream_status_changed",
        alias = "UserStreamStatusChanged"
    )]
    UserStreamStatusChanged,
    #[serde(rename = "status_snapshot", alias = "StatusSnapshot")]
    StatusSnapshot,
    #[serde(rename = "calibration_adjusted", alias = "CalibrationAdjusted")]
    CalibrationAdjusted,
    #[serde(rename = "projection_rebuilt", alias = "ProjectionRebuilt")]
    ProjectionRebuilt,
    #[serde(rename = "watchdog_verdict", alias = "WatchdogVerdict")]
    WatchdogVerdict,
    #[serde(rename = "watchdog_kill_triggered", alias = "WatchdogKillTriggered")]
    WatchdogKillTriggered,
}

impl std::fmt::Display for EventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::DecisionEvaluated => "decision_evaluated",
            Self::QuoteApproved => "quote_approved",
            Self::QuoteRejected => "quote_rejected",
            Self::OrderSubmitted => "order_submitted",
            Self::OrderResized => "order_resized",
            Self::OrderCancelled => "order_cancelled",
            Self::FillDetected => "fill_detected",
            Self::HedgeIntentCreated => "hedge_intent_created",
            Self::HedgeDecisionEvaluated => "hedge_decision_evaluated",
            Self::HedgeResultRecorded => "hedge_result_recorded",
            Self::HedgeExitPathRecorded => "hedge_exit_path_recorded",
            Self::NeutralityEvaluated => "neutrality_evaluated",
            Self::MonitorDegraded => "monitor_degraded",
            Self::RiskStateChanged => "risk_state_changed",
            Self::UserStreamStatusChanged => "user_stream_status_changed",
            Self::StatusSnapshot => "status_snapshot",
            Self::CalibrationAdjusted => "calibration_adjusted",
            Self::ProjectionRebuilt => "projection_rebuilt",
            Self::WatchdogVerdict => "watchdog_verdict",
            Self::WatchdogKillTriggered => "watchdog_kill_triggered",
        };
        write!(f, "{}", s)
    }
}

/// Event priority for queue routing. Critical events get a dedicated channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Priority {
    Debug = 0,
    Normal = 1,
    High = 2,
    Critical = 3,
}

impl std::fmt::Display for Priority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Debug => write!(f, "DEBUG"),
            Self::Normal => write!(f, "NORMAL"),
            Self::High => write!(f, "HIGH"),
            Self::Critical => write!(f, "CRITICAL"),
        }
    }
}

/// Canonical event envelope — the single schema for all domain events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub event_id: Uuid,
    pub schema_version: SchemaVersion,
    pub event_type: EventType,
    pub priority: Priority,
    pub occurred_at: DateTime<Utc>,
    pub recorded_at: DateTime<Utc>,
    pub run_id: String,
    pub cycle_id: Option<String>,
    pub trace_id: Option<String>,
    pub source_component: String,
    pub mode: String,
    pub condition_id: Option<String>,
    pub market_slug: Option<String>,
    pub question: Option<String>,
    pub order_id: Option<String>,
    pub asset_id: Option<String>,
    pub hedge_id: Option<String>,
    pub payload: serde_json::Value,
}

impl EventEnvelope {
    /// Create a new envelope with current timestamps and the latest v1.x schema.
    pub fn new(
        event_type: EventType,
        priority: Priority,
        run_id: String,
        source_component: String,
        mode: String,
        payload: serde_json::Value,
    ) -> Self {
        let now = Utc::now();
        Self {
            event_id: Uuid::new_v4(),
            schema_version: SchemaVersion::V1_5,
            event_type,
            priority,
            occurred_at: now,
            recorded_at: now,
            run_id,
            cycle_id: None,
            trace_id: None,
            source_component,
            mode,
            condition_id: None,
            market_slug: None,
            question: None,
            order_id: None,
            asset_id: None,
            hedge_id: None,
            payload,
        }
    }

    pub fn with_cycle_id(mut self, cycle_id: String) -> Self {
        self.cycle_id = Some(cycle_id);
        self
    }

    pub fn with_trace_id(mut self, trace_id: String) -> Self {
        self.trace_id = Some(trace_id);
        self
    }

    pub fn with_condition_id(mut self, condition_id: String) -> Self {
        self.condition_id = Some(condition_id);
        self
    }

    pub fn with_market_slug(mut self, slug: String) -> Self {
        self.market_slug = Some(slug);
        self
    }

    pub fn with_question(mut self, question: String) -> Self {
        self.question = Some(question);
        self
    }

    pub fn with_order_id(mut self, order_id: String) -> Self {
        self.order_id = Some(order_id);
        self
    }

    pub fn with_asset_id(mut self, asset_id: String) -> Self {
        self.asset_id = Some(asset_id);
        self
    }

    pub fn with_hedge_id(mut self, hedge_id: String) -> Self {
        self.hedge_id = Some(hedge_id);
        self
    }
}
