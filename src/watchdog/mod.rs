pub mod health;
pub mod kill_trigger;
pub mod status_poller;

pub use health::{HealthVerdict, WsHealthTracker};
pub use kill_trigger::{KillAction, KillTrigger};
pub use status_poller::{StatusPoller, StatusVerdict};

use std::sync::Arc;
use std::time::Instant;

use spreadeater_core::EventProducer;
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, error, info, warn};

use crate::books::BookWsStats;
use crate::config::WatchdogConfig;
use crate::monitor::{emitters, ErrorLogger};
use crate::trading::RiskManager;

/// Escalation state for the watchdog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EscalationLevel {
    Normal,
    Warning,
    KillPending,
    Killed,
}

/// The main watchdog orchestrator.
///
/// Spawned as an independent tokio task by LiveEngine. Periodically:
/// 1. Reads WS health from the shared WsHealthTracker
/// 2. Reads status page verdicts from the StatusPoller channel
/// 3. Runs escalation state machine
/// 4. Writes heartbeat file for external sidecar
pub struct WatchdogManager {
    config: WatchdogConfig,
    health_tracker: Arc<Mutex<WsHealthTracker>>,
    risk_manager: Arc<RiskManager>,
    error_logger: Arc<ErrorLogger>,
    kill_action: Arc<dyn KillAction>,
    event_producer: Option<Arc<dyn EventProducer>>,
    run_id: String,
    mode: String,
    book_ws_stats: Arc<BookWsStats>,
}

impl WatchdogManager {
    pub fn new(
        config: WatchdogConfig,
        health_tracker: Arc<Mutex<WsHealthTracker>>,
        risk_manager: Arc<RiskManager>,
        error_logger: Arc<ErrorLogger>,
        event_producer: Option<Arc<dyn EventProducer>>,
        run_id: String,
        mode: String,
        book_ws_stats: Arc<BookWsStats>,
    ) -> Self {
        let kill_action = Arc::new(KillTrigger::new(
            config.clone(),
            Arc::clone(&risk_manager),
            Arc::clone(&error_logger),
        ));
        Self::new_with_kill_action(
            config,
            health_tracker,
            risk_manager,
            error_logger,
            kill_action,
            event_producer,
            run_id,
            mode,
            book_ws_stats,
        )
    }

    fn new_with_kill_action(
        config: WatchdogConfig,
        health_tracker: Arc<Mutex<WsHealthTracker>>,
        risk_manager: Arc<RiskManager>,
        error_logger: Arc<ErrorLogger>,
        kill_action: Arc<dyn KillAction>,
        event_producer: Option<Arc<dyn EventProducer>>,
        run_id: String,
        mode: String,
        book_ws_stats: Arc<BookWsStats>,
    ) -> Self {
        Self {
            config,
            health_tracker,
            risk_manager,
            error_logger,
            kill_action,
            event_producer,
            run_id,
            mode,
            book_ws_stats,
        }
    }

    /// Spawn the watchdog as an independent tokio task.
    ///
    /// This task runs the escalation loop and heartbeat writer concurrently.
    /// It does NOT depend on the LiveEngine select! loop ticking — it fires
    /// even if the main loop is blocked in a long run_cycle().
    pub fn spawn(self) {
        if !self.config.enabled {
            info!("Watchdog is disabled via config");
            return;
        }

        // Start the status page poller
        let status_poller = StatusPoller::new(self.config.clone());
        let status_rx = status_poller.spawn();

        tokio::spawn(async move {
            self.run(status_rx).await;
        });

        info!("Watchdog spawned");
    }

    async fn run(self, mut status_rx: mpsc::UnboundedReceiver<StatusVerdict>) {
        let mut check_interval = tokio::time::interval(std::time::Duration::from_secs(5));
        let mut heartbeat_interval = tokio::time::interval(std::time::Duration::from_secs(5));

        let mut escalation = EscalationLevel::Normal;
        let mut warning_started_at: Option<Instant> = None;
        let mut kill_pending_since: Option<Instant> = None;
        let mut last_status_verdict = StatusVerdict::Healthy;
        let mut kill_reason = String::new();

        loop {
            tokio::select! {
                _ = check_interval.tick() => {
                    // Feed raw WS activity into health tracker before assessing
                    let book_stats_snapshot = self.book_ws_stats.snapshot();

                    // Assess WS health
                    let (ws_verdict, book_ws_connected, user_ws_connected) = {
                        let mut tracker = self.health_tracker.lock().await;
                        tracker.update_book_ws_raw_activity(&book_stats_snapshot);
                        let verdict = tracker.assess(&self.config);
                        let (book_ws_connected, user_ws_connected) = tracker.connection_state();
                        (verdict, book_ws_connected, user_ws_connected)
                    };

                    // Drain latest status verdict (take the most recent)
                    while let Ok(v) = status_rx.try_recv() {
                        last_status_verdict = v;
                    }

                    // Determine the worst verdict
                    let (is_critical, is_degraded, reason) =
                        Self::combine_verdicts(&ws_verdict, &last_status_verdict);
                    let mut kill_actions_suppressed = false;

                    // State machine
                    match escalation {
                        EscalationLevel::Killed => {
                            kill_actions_suppressed = !self.config.enforce_actions;
                        }
                        _ if is_critical => {
                            if escalation != EscalationLevel::KillPending {
                                warn!(
                                    reason = %reason,
                                    "Watchdog: CRITICAL detected, entering KillPending"
                                );
                                self.error_logger.log_error(
                                    "HIGH",
                                    &format!("Watchdog critical: {}", reason),
                                    None,
                                );
                                escalation = EscalationLevel::KillPending;
                                kill_pending_since = Some(Instant::now());
                                kill_reason = reason.clone();
                                if self.config.enforce_actions {
                                    self.risk_manager
                                        .global_halt(&format!("watchdog critical: {}", reason))
                                        .await;
                                } else {
                                    kill_actions_suppressed = true;
                                    warn!(
                                        reason = %reason,
                                        "Watchdog observe-only: suppressing critical halt/kill actions"
                                    );
                                }
                            } else {
                                // Already KillPending — check confirmation delay
                                if let Some(since) = kill_pending_since {
                                    let elapsed = since.elapsed().as_secs();
                                    if elapsed >= self.config.kill_confirmation_delay_secs {
                                        if self.config.enforce_actions {
                                            error!(
                                                reason = %kill_reason,
                                                elapsed_secs = elapsed,
                                                "Watchdog: Kill confirmed after {}s delay",
                                                elapsed
                                            );
                                            self.emit_event(emitters::build_watchdog_kill_triggered(
                                                &self.run_id,
                                                &self.mode,
                                                &kill_reason,
                                                Self::escalation_name(EscalationLevel::KillPending),
                                                elapsed,
                                            ));
                                            if let Err(err) = self.kill_action.execute(&kill_reason).await {
                                                error!(
                                                    reason = %kill_reason,
                                                    error = %err,
                                                    "Watchdog kill action returned error"
                                                );
                                            }
                                            escalation = EscalationLevel::Killed;
                                        } else {
                                            kill_actions_suppressed = true;
                                            debug!(
                                                reason = %kill_reason,
                                                elapsed_secs = elapsed,
                                                "Watchdog observe-only: kill confirmation reached with actions suppressed"
                                            );
                                        }
                                    } else {
                                        kill_actions_suppressed = !self.config.enforce_actions;
                                        debug!(
                                            elapsed_secs = elapsed,
                                            confirmation_delay = self.config.kill_confirmation_delay_secs,
                                            "Watchdog: KillPending, waiting for confirmation"
                                        );
                                    }
                                }
                            }
                        }
                        _ if is_degraded => {
                            if escalation == EscalationLevel::Normal {
                                warn!(
                                    reason = %reason,
                                    "Watchdog: Degraded detected, entering Warning"
                                );
                                self.error_logger.log_error(
                                    "WARN",
                                    &format!("Watchdog degraded: {}", reason),
                                    None,
                                );
                                escalation = EscalationLevel::Warning;
                                warning_started_at = Some(Instant::now());
                            } else if escalation == EscalationLevel::Warning {
                                // Check if degraded has persisted too long
                                if let Some(since) = warning_started_at {
                                    let elapsed = since.elapsed().as_secs();
                                    if elapsed >= self.config.degraded_timeout_secs {
                                        warn!(
                                            reason = %reason,
                                            elapsed_secs = elapsed,
                                            "Watchdog: Degraded for {}s, escalating to KillPending",
                                            elapsed
                                        );
                                        escalation = EscalationLevel::KillPending;
                                        kill_pending_since = Some(Instant::now());
                                        kill_reason = format!(
                                            "Sustained degradation ({}s): {}",
                                            elapsed, reason
                                        );
                                        if self.config.enforce_actions {
                                            self.risk_manager
                                                .global_halt(&format!("watchdog degraded timeout: {}", reason))
                                                .await;
                                        } else {
                                            kill_actions_suppressed = true;
                                            warn!(
                                                reason = %reason,
                                                elapsed_secs = elapsed,
                                                "Watchdog observe-only: suppressing degraded-timeout halt/kill actions"
                                            );
                                        }
                                    }
                                }
                            } else if escalation == EscalationLevel::KillPending {
                                // De-escalate: system improved from Critical to Degraded
                                info!(
                                    reason = %reason,
                                    "Watchdog: Verdict improved to Degraded, de-escalating from KillPending to Warning"
                                );
                                escalation = EscalationLevel::Warning;
                                warning_started_at = Some(Instant::now());
                                kill_pending_since = None;
                                kill_reason.clear();
                            }
                        }
                        _ => {
                            // Healthy — reset escalation
                            if escalation != EscalationLevel::Normal {
                                info!(
                                    prev_level = ?escalation,
                                    "Watchdog: System recovered, resetting to Normal"
                                );
                                escalation = EscalationLevel::Normal;
                                warning_started_at = None;
                                kill_pending_since = None;
                                kill_reason.clear();
                            }
                        }
                    }

                    if !self.config.enforce_actions && escalation == EscalationLevel::KillPending {
                        kill_actions_suppressed = true;
                    }

                    self.emit_event(emitters::build_watchdog_verdict(
                        &self.run_id,
                        &self.mode,
                        Self::verdict_name(&ws_verdict),
                        Self::status_verdict_name(&last_status_verdict),
                        Self::escalation_name(escalation),
                        Self::verdict_reason(&ws_verdict),
                        Self::status_verdict_reason(&last_status_verdict),
                        book_ws_connected,
                        user_ws_connected,
                        self.config.enforce_actions,
                        kill_actions_suppressed,
                        self.book_ws_stats.snapshot(),
                    ));
                }

                _ = heartbeat_interval.tick() => {
                    self.write_heartbeat().await;
                }
            }
        }
    }

    /// Combine WS and status verdicts into a single assessment.
    fn combine_verdicts(ws: &HealthVerdict, status: &StatusVerdict) -> (bool, bool, String) {
        let ws_critical = matches!(ws, HealthVerdict::Critical { .. });
        let ws_degraded = matches!(ws, HealthVerdict::Degraded { .. });
        let status_critical = matches!(status, StatusVerdict::Critical { .. });
        let status_degraded = matches!(status, StatusVerdict::Degraded { .. });

        let is_critical = ws_critical || status_critical;
        let is_degraded = ws_degraded || status_degraded;

        let reason = match (ws, status) {
            (HealthVerdict::Critical { reason }, StatusVerdict::Critical { reason: sr }) => {
                format!("WS: {} | Status: {}", reason, sr)
            }
            (HealthVerdict::Critical { reason }, _) => format!("WS: {}", reason),
            (_, StatusVerdict::Critical { reason }) => format!("Status: {}", reason),
            (HealthVerdict::Degraded { reason }, StatusVerdict::Degraded { reason: sr }) => {
                format!("WS: {} | Status: {}", reason, sr)
            }
            (HealthVerdict::Degraded { reason }, _) => format!("WS: {}", reason),
            (_, StatusVerdict::Degraded { reason }) => format!("Status: {}", reason),
            _ => String::new(),
        };

        (is_critical, is_degraded, reason)
    }

    fn verdict_name(verdict: &HealthVerdict) -> &'static str {
        match verdict {
            HealthVerdict::Healthy => "healthy",
            HealthVerdict::Degraded { .. } => "degraded",
            HealthVerdict::Critical { .. } => "critical",
        }
    }

    fn verdict_reason(verdict: &HealthVerdict) -> Option<&str> {
        match verdict {
            HealthVerdict::Healthy => None,
            HealthVerdict::Degraded { reason } | HealthVerdict::Critical { reason } => {
                Some(reason.as_str())
            }
        }
    }

    fn status_verdict_name(verdict: &StatusVerdict) -> &'static str {
        match verdict {
            StatusVerdict::Healthy => "healthy",
            StatusVerdict::Degraded { .. } => "degraded",
            StatusVerdict::Critical { .. } => "critical",
        }
    }

    fn status_verdict_reason(verdict: &StatusVerdict) -> Option<&str> {
        match verdict {
            StatusVerdict::Healthy => None,
            StatusVerdict::Degraded { reason } | StatusVerdict::Critical { reason } => {
                Some(reason.as_str())
            }
        }
    }

    fn escalation_name(level: EscalationLevel) -> &'static str {
        match level {
            EscalationLevel::Normal => "normal",
            EscalationLevel::Warning => "warning",
            EscalationLevel::KillPending => "kill_pending",
            EscalationLevel::Killed => "killed",
        }
    }

    fn emit_event(&self, event: spreadeater_core::EventEnvelope) {
        let Some(producer) = &self.event_producer else {
            return;
        };

        match producer.emit(event) {
            Ok(true) => {}
            Ok(false) => warn!("Dropping watchdog event: queue is full"),
            Err(err) => warn!(error = %err, "Failed to enqueue watchdog event"),
        }
    }

    /// Write Unix timestamp to heartbeat file for the external sidecar.
    async fn write_heartbeat(&self) {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        if let Err(e) = tokio::fs::write(&self.config.heartbeat_file, ts.to_string()).await {
            warn!(error = %e, "Failed to write watchdog heartbeat file");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex as StdMutex;

    use rust_decimal_macros::dec;
    use spreadeater_core::payloads::WatchdogVerdictPayload;
    use spreadeater_core::{EventEnvelope, EventType, ProducerError, QueueDepthSnapshot};
    use uuid::Uuid;

    use crate::books::BookEvent;
    use crate::config::RiskConfig;

    #[derive(Default)]
    struct TestProducer {
        events: StdMutex<Vec<EventEnvelope>>,
    }

    impl TestProducer {
        fn events(&self) -> Vec<EventEnvelope> {
            self.events.lock().unwrap().clone()
        }
    }

    impl EventProducer for TestProducer {
        fn emit(&self, event: EventEnvelope) -> Result<bool, ProducerError> {
            self.events.lock().unwrap().push(event);
            Ok(true)
        }

        fn queue_depth(&self) -> QueueDepthSnapshot {
            QueueDepthSnapshot {
                critical: 0,
                normal: 0,
            }
        }

        fn is_degraded(&self) -> bool {
            false
        }
    }

    fn test_watchdog_config() -> WatchdogConfig {
        let heartbeat_dir =
            std::env::temp_dir().join(format!("spreadeater-watchdog-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&heartbeat_dir).unwrap();
        WatchdogConfig {
            enabled: true,
            enforce_actions: false,
            max_book_ws_silence_secs: 0,
            max_user_ws_silence_secs: 120,
            max_reconnects_in_window: 99,
            reconnect_window_secs: 300,
            max_consecutive_disconnects: 99,
            degraded_timeout_secs: 120,
            kill_confirmation_delay_secs: 10,
            status_poll_interval_secs: 300,
            status_page_url: "http://127.0.0.1:9".to_string(),
            critical_components: Vec::new(),
            heartbeat_file: heartbeat_dir
                .join("heartbeat")
                .to_string_lossy()
                .into_owned(),
            kill_flatten_script: "scripts/kill_flatten.py".to_string(),
        }
    }

    fn test_risk_manager() -> Arc<RiskManager> {
        Arc::new(RiskManager::new(RiskConfig {
            hedge_timeout_secs: 10,
            hedge_exposure_tolerance: dec!(0.5),
            cash_reserve: dec!(50),
        }))
    }

    struct TestKillAction {
        executed: AtomicBool,
    }

    impl TestKillAction {
        fn new() -> Self {
            Self {
                executed: AtomicBool::new(false),
            }
        }

        fn was_executed(&self) -> bool {
            self.executed.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl KillAction for TestKillAction {
        async fn execute(&self, _reason: &str) -> Result<()> {
            self.executed.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn observe_only_watchdog_emits_verdict_without_global_halt() {
        let config = test_watchdog_config();
        let mut tracker = WsHealthTracker::new();
        tracker.report_book_message();
        let health_tracker = Arc::new(Mutex::new(tracker));
        let risk_manager = test_risk_manager();
        let error_dir =
            std::env::temp_dir().join(format!("spreadeater-watchdog-errors-{}", Uuid::new_v4()));
        let error_logger = Arc::new(ErrorLogger::new(&error_dir.to_string_lossy().into_owned()));
        let producer = Arc::new(TestProducer::default());
        let book_ws_stats = Arc::new(BookWsStats::default());
        book_ws_stats.record_raw_message();
        book_ws_stats.record_accepted(&[BookEvent::Snapshot {
            token_id: "token".to_string(),
            bids: vec![],
            asks: vec![],
        }]);
        book_ws_stats.record_parse_error();

        let (_status_tx, status_rx) = mpsc::unbounded_channel();
        let manager = WatchdogManager::new(
            config,
            health_tracker,
            Arc::clone(&risk_manager),
            error_logger,
            Some(producer.clone() as Arc<dyn EventProducer>),
            "run-1".to_string(),
            "live".to_string(),
            book_ws_stats,
        );

        let handle = tokio::spawn(async move {
            manager.run(status_rx).await;
        });

        tokio::task::yield_now().await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        tokio::task::yield_now().await;

        assert!(!risk_manager.is_globally_halted().await);

        let verdict_event = producer
            .events()
            .into_iter()
            .find(|event| event.event_type == EventType::WatchdogVerdict)
            .expect("expected watchdog verdict event");
        let payload: WatchdogVerdictPayload =
            serde_json::from_value(verdict_event.payload).unwrap();
        assert!(!payload.enforcement_enabled);
        assert!(payload.kill_actions_suppressed);
        assert!(payload.last_raw_book_ws_message_at.is_some());
        assert!(payload.last_parsed_book_event_at.is_some());
        assert!(payload.last_book_parse_error_at.is_some());
        assert_eq!(payload.book_ws_accepted_messages, 1);
        assert_eq!(payload.book_ws_parse_errors, 1);
        assert!(!producer
            .events()
            .into_iter()
            .any(|event| event.event_type == EventType::WatchdogKillTriggered));

        handle.abort();
    }

    #[tokio::test]
    async fn enforcing_watchdog_emits_kill_triggered_after_confirmation() {
        let mut config = test_watchdog_config();
        config.enforce_actions = true;
        config.kill_confirmation_delay_secs = 0;

        let mut tracker = WsHealthTracker::new();
        tracker.report_book_message();
        let health_tracker = Arc::new(Mutex::new(tracker));
        let risk_manager = test_risk_manager();
        let error_dir =
            std::env::temp_dir().join(format!("spreadeater-watchdog-errors-{}", Uuid::new_v4()));
        let error_logger = Arc::new(ErrorLogger::new(&error_dir.to_string_lossy().into_owned()));
        let producer = Arc::new(TestProducer::default());
        let book_ws_stats = Arc::new(BookWsStats::default());
        let kill_action = Arc::new(TestKillAction::new());
        book_ws_stats.record_raw_message();
        book_ws_stats.record_accepted(&[BookEvent::Delta {
            token_id: "token".to_string(),
            bid_updates: vec![],
            ask_updates: vec![],
        }]);

        let (_status_tx, status_rx) = mpsc::unbounded_channel();
        let manager = WatchdogManager::new_with_kill_action(
            config,
            health_tracker,
            Arc::clone(&risk_manager),
            error_logger,
            kill_action.clone() as Arc<dyn KillAction>,
            Some(producer.clone() as Arc<dyn EventProducer>),
            "run-2".to_string(),
            "live".to_string(),
            book_ws_stats,
        );

        let handle = tokio::spawn(async move {
            manager.run(status_rx).await;
        });

        tokio::task::yield_now().await;
        tokio::time::sleep(std::time::Duration::from_millis(5_200)).await;
        tokio::task::yield_now().await;

        assert!(risk_manager.is_globally_halted().await);
        assert!(kill_action.was_executed());
        assert!(producer
            .events()
            .into_iter()
            .any(|event| event.event_type == EventType::WatchdogKillTriggered));

        handle.abort();
    }

    #[tokio::test]
    async fn enforcing_watchdog_recovers_from_kill_pending_before_re_escalating() {
        let mut config = test_watchdog_config();
        config.enforce_actions = true;
        config.max_book_ws_silence_secs = 120;
        config.max_user_ws_silence_secs = 120;
        config.kill_confirmation_delay_secs = 30;
        config.degraded_timeout_secs = 1;

        let mut tracker = WsHealthTracker::new();
        tracker.report_book_message();
        tracker.report_user_connected();
        let health_tracker = Arc::new(Mutex::new(tracker));
        let risk_manager = test_risk_manager();
        let error_dir =
            std::env::temp_dir().join(format!("spreadeater-watchdog-errors-{}", Uuid::new_v4()));
        let error_logger = Arc::new(ErrorLogger::new(&error_dir.to_string_lossy().into_owned()));
        let producer = Arc::new(TestProducer::default());
        let book_ws_stats = Arc::new(BookWsStats::default());
        let kill_action = Arc::new(TestKillAction::new());
        book_ws_stats.record_raw_message();
        book_ws_stats.record_accepted(&[BookEvent::Snapshot {
            token_id: "token".to_string(),
            bids: vec![],
            asks: vec![],
        }]);

        let (status_tx, status_rx) = mpsc::unbounded_channel();
        status_tx
            .send(StatusVerdict::Critical {
                reason: "status critical".to_string(),
            })
            .unwrap();

        let manager = WatchdogManager::new_with_kill_action(
            config,
            health_tracker,
            Arc::clone(&risk_manager),
            error_logger,
            kill_action.clone() as Arc<dyn KillAction>,
            Some(producer.clone() as Arc<dyn EventProducer>),
            "run-3".to_string(),
            "live".to_string(),
            book_ws_stats,
        );

        let handle = tokio::spawn(async move {
            manager.run(status_rx).await;
        });

        tokio::task::yield_now().await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        tokio::task::yield_now().await;

        status_tx
            .send(StatusVerdict::Degraded {
                reason: "status degraded".to_string(),
            })
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(10_300)).await;
        tokio::task::yield_now().await;

        assert!(risk_manager.is_globally_halted().await);
        assert!(!kill_action.was_executed());

        let events = producer.events();
        assert!(!events
            .iter()
            .any(|event| event.event_type == EventType::WatchdogKillTriggered));

        let verdicts: Vec<WatchdogVerdictPayload> = events
            .into_iter()
            .filter(|event| event.event_type == EventType::WatchdogVerdict)
            .map(|event| serde_json::from_value(event.payload).unwrap())
            .collect();

        assert!(verdicts.iter().any(|payload| {
            payload.status_verdict == "critical" && payload.escalation_level == "kill_pending"
        }));
        assert!(verdicts.iter().any(|payload| {
            payload.status_verdict == "degraded" && payload.escalation_level == "warning"
        }));
        assert_eq!(
            verdicts
                .last()
                .map(|payload| payload.escalation_level.as_str()),
            Some("kill_pending")
        );

        handle.abort();
    }
}
