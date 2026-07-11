use spreadeater::config::WatchdogConfig;
use spreadeater::watchdog::health::{HealthVerdict, WsHealthTracker};
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
fn healthy_when_no_events_reported() {
    let mut tracker = WsHealthTracker::new();
    let config = test_config();
    assert_eq!(tracker.assess(&config), HealthVerdict::Healthy);
}

#[test]
fn healthy_after_normal_book_and_user_messages() {
    let mut tracker = WsHealthTracker::new();
    let config = test_config();
    tracker.report_book_message();
    tracker.report_user_connected();
    tracker.report_user_message();
    assert_eq!(tracker.assess(&config), HealthVerdict::Healthy);
}

#[test]
fn degraded_on_single_book_disconnect() {
    let mut tracker = WsHealthTracker::new();
    let config = test_config();
    tracker.report_book_message();
    tracker.report_book_disconnect();
    let verdict = tracker.assess(&config);
    assert!(matches!(verdict, HealthVerdict::Degraded { .. }));
}

#[test]
fn degraded_on_single_user_disconnect() {
    let mut tracker = WsHealthTracker::new();
    let config = test_config();
    tracker.report_user_connected();
    tracker.report_user_disconnect();
    let verdict = tracker.assess(&config);
    assert!(matches!(verdict, HealthVerdict::Degraded { .. }));
}

#[test]
fn critical_when_reconnects_exceed_threshold() {
    let mut tracker = WsHealthTracker::new();
    let config = test_config();
    // 3 disconnects = threshold
    tracker.report_book_disconnect();
    tracker.report_book_disconnect();
    tracker.report_book_disconnect();
    let verdict = tracker.assess(&config);
    assert!(matches!(verdict, HealthVerdict::Critical { .. }));
    if let HealthVerdict::Critical { reason } = verdict {
        assert!(
            reason.contains("reconnects"),
            "Expected reconnect reason, got: {}",
            reason
        );
    }
}

#[test]
fn critical_on_consecutive_short_lived_disconnects() {
    let mut tracker = WsHealthTracker::new();
    let config = test_config();
    // Two consecutive disconnects (threshold is 2)
    tracker.report_book_message(); // connect
    tracker.report_book_disconnect(); // disconnect 1 (short-lived)
    tracker.report_book_message(); // reconnect
    tracker.report_book_disconnect(); // disconnect 2 (short-lived)
    let verdict = tracker.assess(&config);
    assert!(matches!(verdict, HealthVerdict::Critical { .. }));
}

#[test]
fn critical_on_book_ws_silence() {
    let mut tracker = WsHealthTracker::new();
    let config = test_config();
    tracker.report_book_message();
    // Wait past the 2-second silence threshold
    thread::sleep(Duration::from_secs(3));
    let verdict = tracker.assess(&config);
    assert!(matches!(verdict, HealthVerdict::Critical { .. }));
    if let HealthVerdict::Critical { reason } = verdict {
        assert!(
            reason.contains("Book WS silent"),
            "Expected silence reason, got: {}",
            reason
        );
    }
}

#[test]
fn critical_on_user_ws_silence() {
    let mut tracker = WsHealthTracker::new();
    let config = test_config();
    tracker.report_user_connected();
    // Wait past the 3-second silence threshold
    thread::sleep(Duration::from_secs(4));
    let verdict = tracker.assess(&config);
    assert!(matches!(verdict, HealthVerdict::Critical { .. }));
    if let HealthVerdict::Critical { reason } = verdict {
        assert!(
            reason.contains("User WS silent"),
            "Expected user silence reason, got: {}",
            reason
        );
    }
}

#[test]
fn raw_user_activity_resets_silence_timer() {
    let mut tracker = WsHealthTracker::new();
    let config = test_config();
    tracker.report_user_connected();
    thread::sleep(Duration::from_secs(4));

    assert!(matches!(
        tracker.assess(&config),
        HealthVerdict::Critical { .. }
    ));

    tracker.report_user_raw_activity();

    assert_eq!(tracker.assess(&config), HealthVerdict::Healthy);
}

#[test]
fn repeated_raw_user_activity_prevents_critical_silence() {
    let mut tracker = WsHealthTracker::new();
    let config = test_config();
    tracker.report_user_connected();

    for _ in 0..2 {
        thread::sleep(Duration::from_secs(2));
        tracker.report_user_raw_activity();
        assert_eq!(tracker.assess(&config), HealthVerdict::Healthy);
    }
}

#[test]
fn mixed_reconnects_across_both_streams_counted_together() {
    let mut tracker = WsHealthTracker::new();
    let config = test_config();
    // 2 book + 1 user = 3 total (threshold)
    tracker.report_book_disconnect();
    tracker.report_book_disconnect();
    tracker.report_user_disconnect();
    let verdict = tracker.assess(&config);
    assert!(matches!(verdict, HealthVerdict::Critical { .. }));
}

#[test]
fn reconnect_below_threshold_is_degraded_not_critical() {
    let mut tracker = WsHealthTracker::new();
    let config = test_config();
    // Keep reconnect count below the global threshold without tripping
    // the same-stream consecutive-disconnect threshold.
    tracker.report_book_disconnect();
    tracker.report_user_disconnect();
    let verdict = tracker.assess(&config);
    // Should be Degraded (reconnects > 0 but < threshold)
    assert!(matches!(verdict, HealthVerdict::Degraded { .. }));
}
