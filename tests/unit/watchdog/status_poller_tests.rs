// Note: StatusPoller evaluate() tests are inline in the module.
// This file tests additional edge cases and the verdict semantics.

use spreadeater::config::WatchdogConfig;

#[test]
fn watchdog_config_defaults_are_sensible() {
    let config = WatchdogConfig::default();
    assert!(config.enabled);
    assert_eq!(config.max_book_ws_silence_secs, 60);
    assert_eq!(config.max_user_ws_silence_secs, 120);
    assert_eq!(config.max_reconnects_in_window, 5);
    assert_eq!(config.reconnect_window_secs, 300);
    assert_eq!(config.max_consecutive_disconnects, 3);
    assert_eq!(config.degraded_timeout_secs, 120);
    assert_eq!(config.kill_confirmation_delay_secs, 10);
    assert_eq!(config.status_poll_interval_secs, 30);
    assert!(config.status_page_url.contains("polymarket.com"));
    assert!(config.critical_components.contains(&"CLOB API".to_string()));
    assert!(config
        .critical_components
        .contains(&"Polygon (RPC)".to_string()));
}

#[test]
fn watchdog_config_deserializes_with_defaults() {
    let json = r#"{}"#;
    let config: WatchdogConfig = serde_json::from_str(json).unwrap();
    assert!(config.enabled);
    assert_eq!(config.max_book_ws_silence_secs, 60);
}

#[test]
fn watchdog_config_custom_values_override_defaults() {
    let json = r#"{
        "enabled": false,
        "max_book_ws_silence_secs": 30,
        "kill_confirmation_delay_secs": 5
    }"#;
    let config: WatchdogConfig = serde_json::from_str(json).unwrap();
    assert!(!config.enabled);
    assert_eq!(config.max_book_ws_silence_secs, 30);
    assert_eq!(config.kill_confirmation_delay_secs, 5);
    // Other fields should keep defaults
    assert_eq!(config.max_user_ws_silence_secs, 120);
}
