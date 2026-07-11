use std::collections::VecDeque;
use std::time::Instant;

use chrono::Utc;

use crate::books::websocket::BookWsStatsSnapshot;
use crate::config::WatchdogConfig;

/// Verdict from WS health assessment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HealthVerdict {
    Healthy,
    Degraded { reason: String },
    Critical { reason: String },
}

/// Tracks WebSocket connection health for both book and user streams.
///
/// Fed by LiveEngine's select! loop via `report_*` methods.
/// Assessed periodically by WatchdogManager via `assess()`.
pub struct WsHealthTracker {
    // Book WS state
    book_ws_connected: bool,
    book_ws_last_message_at: Option<Instant>,
    book_ws_disconnect_times: VecDeque<Instant>,
    book_ws_consecutive_disconnects: u32,
    book_ws_last_connect_at: Option<Instant>,

    // User WS state
    user_ws_connected: bool,
    user_ws_last_message_at: Option<Instant>,
    user_ws_disconnect_times: VecDeque<Instant>,
    user_ws_consecutive_disconnects: u32,
    user_ws_last_connect_at: Option<Instant>,

    /// Minimum connection duration (seconds) to count as "stable" and reset consecutive counter.
    stable_connection_secs: u64,
}

impl WsHealthTracker {
    pub fn new() -> Self {
        Self {
            book_ws_connected: false,
            book_ws_last_message_at: None,
            book_ws_disconnect_times: VecDeque::new(),
            book_ws_consecutive_disconnects: 0,
            book_ws_last_connect_at: None,

            user_ws_connected: false,
            user_ws_last_message_at: None,
            user_ws_disconnect_times: VecDeque::new(),
            user_ws_consecutive_disconnects: 0,
            user_ws_last_connect_at: None,

            stable_connection_secs: 60,
        }
    }

    /// Called when a book WS message is received (Snapshot or Delta).
    pub fn report_book_message(&mut self) {
        let now = Instant::now();
        self.book_ws_last_message_at = Some(now);
        if !self.book_ws_connected {
            self.book_ws_connected = true;
            self.book_ws_last_connect_at = Some(now);
            // If connection lasted > stable_connection_secs, reset consecutive counter
            // (checked on disconnect, but also reset on fresh connect after stable)
        }
    }

    /// Called when book WS disconnects.
    pub fn report_book_disconnect(&mut self) {
        let now = Instant::now();
        self.book_ws_connected = false;
        self.book_ws_disconnect_times.push_back(now);

        // Check if the connection that just died was stable (>60s)
        let was_stable = self
            .book_ws_last_connect_at
            .map(|t| now.duration_since(t).as_secs() >= self.stable_connection_secs)
            .unwrap_or(false);

        if was_stable {
            self.book_ws_consecutive_disconnects = 1; // Reset — this was a fresh failure after stability
        } else {
            self.book_ws_consecutive_disconnects += 1;
        }
    }

    /// Called when a user WS event is received (Connected, Trade, Order).
    pub fn report_user_message(&mut self) {
        let now = Instant::now();
        self.user_ws_last_message_at = Some(now);
        if !self.user_ws_connected {
            self.user_ws_connected = true;
            self.user_ws_last_connect_at = Some(now);
        }
    }

    /// Called when a raw authenticated user WS frame proves the socket is alive
    /// without carrying a Trade/Order business event.
    pub fn report_user_raw_activity(&mut self) {
        self.report_user_message();
    }

    /// Called when user WS connects.
    pub fn report_user_connected(&mut self) {
        let now = Instant::now();
        self.user_ws_connected = true;
        self.user_ws_last_connect_at = Some(now);
        self.user_ws_last_message_at = Some(now);
    }

    /// Called when user WS disconnects.
    pub fn report_user_disconnect(&mut self) {
        let now = Instant::now();
        self.user_ws_connected = false;
        self.user_ws_disconnect_times.push_back(now);

        let was_stable = self
            .user_ws_last_connect_at
            .map(|t| now.duration_since(t).as_secs() >= self.stable_connection_secs)
            .unwrap_or(false);

        if was_stable {
            self.user_ws_consecutive_disconnects = 1;
        } else {
            self.user_ws_consecutive_disconnects += 1;
        }
    }

    /// Update the book WS silence timer from raw WS activity stats.
    ///
    /// The silence check in `assess()` only sees parsed Snapshot/Delta events
    /// via `report_book_message()`. This method feeds the raw-message timestamp
    /// from `BookWsStats` so that heartbeats, subscription acks, and other
    /// non-book messages also count as "the connection is alive".
    pub fn update_book_ws_raw_activity(&mut self, stats: &BookWsStatsSnapshot) {
        if let Some(last_raw) = stats.last_raw_message_at {
            let age = Utc::now() - last_raw;
            if age.num_seconds() >= 0 {
                let age_duration = std::time::Duration::from_secs(age.num_seconds() as u64);
                let approx_instant = Instant::now()
                    .checked_sub(age_duration)
                    .unwrap_or(Instant::now());
                match self.book_ws_last_message_at {
                    Some(existing) if existing >= approx_instant => {}
                    _ => self.book_ws_last_message_at = Some(approx_instant),
                }
            }
        }
    }

    pub fn connection_state(&self) -> (bool, bool) {
        (self.book_ws_connected, self.user_ws_connected)
    }

    /// Prune disconnect timestamps outside the rolling window.
    fn prune_window(&mut self, window_secs: u64) {
        let now = Instant::now();
        let cutoff = std::time::Duration::from_secs(window_secs);
        while self
            .book_ws_disconnect_times
            .front()
            .map(|t| now.duration_since(*t) > cutoff)
            .unwrap_or(false)
        {
            self.book_ws_disconnect_times.pop_front();
        }
        while self
            .user_ws_disconnect_times
            .front()
            .map(|t| now.duration_since(*t) > cutoff)
            .unwrap_or(false)
        {
            self.user_ws_disconnect_times.pop_front();
        }
    }

    /// Count reconnects in the rolling window for both WS streams.
    fn reconnects_in_window(&self) -> u32 {
        (self.book_ws_disconnect_times.len() + self.user_ws_disconnect_times.len()) as u32
    }

    /// Assess overall WS health against configured thresholds.
    pub fn assess(&mut self, config: &WatchdogConfig) -> HealthVerdict {
        self.prune_window(config.reconnect_window_secs);
        let now = Instant::now();

        // Critical: book WS silent too long
        if let Some(last) = self.book_ws_last_message_at {
            let silence_secs = now.duration_since(last).as_secs();
            if silence_secs >= config.max_book_ws_silence_secs {
                return HealthVerdict::Critical {
                    reason: format!(
                        "Book WS silent for {}s (threshold: {}s)",
                        silence_secs, config.max_book_ws_silence_secs
                    ),
                };
            }
        } else if self.book_ws_connected {
            // Connected but never received a message — not yet alarming, wait for data
        }

        // Critical: user WS silent too long
        if let Some(last) = self.user_ws_last_message_at {
            let silence_secs = now.duration_since(last).as_secs();
            if silence_secs >= config.max_user_ws_silence_secs {
                return HealthVerdict::Critical {
                    reason: format!(
                        "User WS silent for {}s (threshold: {}s)",
                        silence_secs, config.max_user_ws_silence_secs
                    ),
                };
            }
        }

        // Critical: too many reconnects in rolling window
        let total_reconnects = self.reconnects_in_window();
        if total_reconnects >= config.max_reconnects_in_window {
            return HealthVerdict::Critical {
                reason: format!(
                    "{} reconnects in {}s window (threshold: {})",
                    total_reconnects, config.reconnect_window_secs, config.max_reconnects_in_window
                ),
            };
        }

        // Critical: too many consecutive short-lived disconnects
        let max_consecutive = std::cmp::max(
            self.book_ws_consecutive_disconnects,
            self.user_ws_consecutive_disconnects,
        );
        if max_consecutive >= config.max_consecutive_disconnects {
            return HealthVerdict::Critical {
                reason: format!(
                    "{} consecutive short-lived disconnects (threshold: {})",
                    max_consecutive, config.max_consecutive_disconnects
                ),
            };
        }

        // Degraded: some reconnects but not yet critical
        if total_reconnects > 0 {
            return HealthVerdict::Degraded {
                reason: format!(
                    "{} reconnects in {}s window",
                    total_reconnects, config.reconnect_window_secs
                ),
            };
        }

        // Degraded: book WS not connected (but not timed out yet)
        if !self.book_ws_connected && self.book_ws_last_message_at.is_some() {
            return HealthVerdict::Degraded {
                reason: "Book WS disconnected, awaiting reconnect".to_string(),
            };
        }

        // Degraded: user WS not connected
        if !self.user_ws_connected && self.user_ws_last_message_at.is_some() {
            return HealthVerdict::Degraded {
                reason: "User WS disconnected, awaiting reconnect".to_string(),
            };
        }

        HealthVerdict::Healthy
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use std::thread;
    use std::time::Duration;

    fn test_config() -> WatchdogConfig {
        WatchdogConfig {
            max_book_ws_silence_secs: 2,
            max_user_ws_silence_secs: 3,
            max_reconnects_in_window: 3,
            reconnect_window_secs: 10,
            max_consecutive_disconnects: 2,
            ..WatchdogConfig::default()
        }
    }

    #[test]
    fn healthy_when_no_events() {
        let mut tracker = WsHealthTracker::new();
        assert_eq!(tracker.assess(&test_config()), HealthVerdict::Healthy);
    }

    #[test]
    fn healthy_after_normal_messages() {
        let mut tracker = WsHealthTracker::new();
        tracker.report_book_message();
        tracker.report_user_connected();
        assert_eq!(tracker.assess(&test_config()), HealthVerdict::Healthy);
    }

    #[test]
    fn degraded_on_single_disconnect() {
        let mut tracker = WsHealthTracker::new();
        tracker.report_book_message();
        tracker.report_book_disconnect();
        let verdict = tracker.assess(&test_config());
        assert!(matches!(verdict, HealthVerdict::Degraded { .. }));
    }

    #[test]
    fn critical_on_too_many_reconnects() {
        let mut tracker = WsHealthTracker::new();
        for _ in 0..3 {
            tracker.report_book_disconnect();
        }
        let verdict = tracker.assess(&test_config());
        assert!(matches!(verdict, HealthVerdict::Critical { .. }));
        if let HealthVerdict::Critical { reason } = verdict {
            assert!(reason.contains("reconnects"));
        }
    }

    #[test]
    fn critical_on_consecutive_short_disconnects() {
        let mut tracker = WsHealthTracker::new();
        // Two consecutive disconnects without stable connection
        tracker.report_book_message();
        tracker.report_book_disconnect();
        tracker.report_book_message();
        tracker.report_book_disconnect();
        let verdict = tracker.assess(&test_config());
        assert!(matches!(verdict, HealthVerdict::Critical { .. }));
    }

    #[test]
    fn critical_on_book_ws_silence() {
        let mut tracker = WsHealthTracker::new();
        let config = test_config();
        tracker.report_book_message();
        // Wait for silence threshold
        thread::sleep(Duration::from_secs(3));
        let verdict = tracker.assess(&config);
        assert!(matches!(verdict, HealthVerdict::Critical { .. }));
        if let HealthVerdict::Critical { reason } = verdict {
            assert!(reason.contains("Book WS silent"));
        }
    }

    #[test]
    fn degraded_on_user_disconnect() {
        let mut tracker = WsHealthTracker::new();
        tracker.report_user_connected();
        tracker.report_user_disconnect();
        let verdict = tracker.assess(&test_config());
        assert!(matches!(verdict, HealthVerdict::Degraded { .. }));
    }

    #[test]
    fn raw_activity_resets_silence_timer() {
        let mut tracker = WsHealthTracker::new();
        let config = test_config(); // max_book_ws_silence_secs = 2

        // Simulate: parsed book event happened 5 seconds ago (beyond threshold)
        tracker.report_book_message();
        thread::sleep(Duration::from_secs(3));

        // Without raw activity, this would be Critical
        assert!(matches!(
            tracker.assess(&config),
            HealthVerdict::Critical { .. }
        ));

        // Now feed raw activity showing WS received a message just now
        let stats = BookWsStatsSnapshot {
            last_raw_message_at: Some(Utc::now()),
            ..BookWsStatsSnapshot::default()
        };
        tracker.update_book_ws_raw_activity(&stats);

        // Should be Healthy now — raw message resets the timer
        assert_eq!(tracker.assess(&config), HealthVerdict::Healthy);
    }

    #[test]
    fn stale_raw_activity_does_not_prevent_critical() {
        let mut tracker = WsHealthTracker::new();
        let config = test_config(); // max_book_ws_silence_secs = 2

        tracker.report_book_message();
        thread::sleep(Duration::from_secs(3));

        // Feed raw activity from 5 seconds ago (also beyond threshold)
        let stats = BookWsStatsSnapshot {
            last_raw_message_at: Some(Utc::now() - chrono::Duration::seconds(5)),
            ..BookWsStatsSnapshot::default()
        };
        tracker.update_book_ws_raw_activity(&stats);

        // Should still be Critical — raw activity is too old
        assert!(matches!(
            tracker.assess(&config),
            HealthVerdict::Critical { .. }
        ));
    }
}
