use rust_decimal_macros::dec;
use spreadeater::config::*;

// ---------------------------------------------------------------------------
// 1. Config::default().mode == RunMode::Shadow
// ---------------------------------------------------------------------------
#[test]
fn config_default_mode_shadow() {
    let cfg = Config::default();
    assert_eq!(cfg.mode, RunMode::Shadow);
}

// ---------------------------------------------------------------------------
// 2. RunMode serde roundtrip (Shadow + Live)
// ---------------------------------------------------------------------------
#[test]
fn run_mode_serde_roundtrip() {
    for mode in [RunMode::Shadow, RunMode::Live] {
        let json = serde_json::to_string(&mode).unwrap();
        let back: RunMode = serde_json::from_str(&json).unwrap();
        assert_eq!(back, mode);
    }
}

// ---------------------------------------------------------------------------
// 3. RiskConfig defaults for optional fields
// ---------------------------------------------------------------------------
#[test]
fn risk_config_defaults() {
    let json = r#"{"hedge_timeout_secs": 10}"#;
    let rc: RiskConfig = serde_json::from_str(json).unwrap();
    assert_eq!(rc.hedge_exposure_tolerance, dec!(0.5));
    assert_eq!(rc.cash_reserve, dec!(50));
}

// ---------------------------------------------------------------------------
// 4. RiskConfig explicit values override defaults
// ---------------------------------------------------------------------------
#[test]
fn risk_config_explicit_values() {
    let json = r#"{
        "hedge_timeout_secs": 30,
        "hedge_exposure_tolerance": "1.5",
        "cash_reserve": "100"
    }"#;
    let rc: RiskConfig = serde_json::from_str(json).unwrap();
    assert_eq!(rc.hedge_timeout_secs, 30);
    assert_eq!(rc.hedge_exposure_tolerance, dec!(1.5));
    assert_eq!(rc.cash_reserve, dec!(100));
}

// ---------------------------------------------------------------------------
// 5. StrategyConfig serde defaults for optional fields
// ---------------------------------------------------------------------------
#[test]
fn strategy_config_defaults() {
    // Supply all required fields; let serde-default fields fall through.
    let json = r#"{
        "max_hedge_cost_bps": "80",
        "max_slippage_bps": "80",
        "default_quote_size": "5",
        "min_edge_threshold": "0.50",
        "quote_drift_bps": "30",
        "bid_depth_pct": "0.50",
        "quote_refresh_secs": 5,
        "score_proxy": {
            "competition_multiplier": "1.5",
            "max_score_share": "0.25",
            "min_score_share": "0.0001",
            "target_score_share": "0.03",
            "calibration_sample_size": 10
        }
    }"#;
    let sc: StrategyConfig = serde_json::from_str(json).unwrap();

    assert_eq!(sc.min_est_daily, dec!(0.25));
    assert_eq!(sc.min_return_pct, dec!(0.0025));
    assert_eq!(sc.min_outcome_price, dec!(0.20));
    assert_eq!(sc.ask_depth_pct, dec!(0.2));
    assert_eq!(sc.reward_discount_factor, dec!(0.70));
    assert_eq!(sc.min_frontier_improvement, dec!(0.05));
}

// ---------------------------------------------------------------------------
// 6. WatchdogConfig::default() values
// ---------------------------------------------------------------------------
#[test]
fn watchdog_config_default_values() {
    let wc = WatchdogConfig::default();
    assert!(wc.enabled);
    assert!(!wc.enforce_actions);
    assert_eq!(wc.max_book_ws_silence_secs, 60);
    assert_eq!(wc.max_user_ws_silence_secs, 120);
    assert_eq!(wc.max_reconnects_in_window, 5);
    assert_eq!(wc.reconnect_window_secs, 300);
    assert_eq!(wc.max_consecutive_disconnects, 3);
    assert_eq!(wc.degraded_timeout_secs, 120);
    assert_eq!(wc.kill_confirmation_delay_secs, 10);
    assert_eq!(wc.status_poll_interval_secs, 30);
    assert_eq!(
        wc.status_page_url,
        "https://status.polymarket.com/summary.json"
    );
    assert_eq!(wc.critical_components, vec!["CLOB API", "Polygon (RPC)"]);
    assert_eq!(wc.heartbeat_file, "./data/watchdog_heartbeat");
    assert_eq!(wc.kill_flatten_script, "scripts/kill_flatten.py");
}

// ---------------------------------------------------------------------------
// 7. ObservabilityConfig::default() values
// ---------------------------------------------------------------------------
#[test]
fn observability_config_default() {
    let oc = ObservabilityConfig::default();
    assert!(oc.enabled);
    assert_eq!(oc.event_log_dir, "./data/events");
}

// ---------------------------------------------------------------------------
// 8. Full Config JSON roundtrip
// ---------------------------------------------------------------------------
#[test]
fn full_config_json_roundtrip() {
    let original = Config::default();
    let json = serde_json::to_string_pretty(&original).unwrap();
    let restored: Config = serde_json::from_str(&json).unwrap();
    assert_eq!(restored.mode, original.mode);
    assert_eq!(
        restored.risk.hedge_timeout_secs,
        original.risk.hedge_timeout_secs
    );
    assert_eq!(
        restored.strategy.min_est_daily,
        original.strategy.min_est_daily
    );
    assert_eq!(restored.watchdog.enabled, original.watchdog.enabled);
    assert_eq!(
        restored.observability.enabled,
        original.observability.enabled
    );
}

// ---------------------------------------------------------------------------
// 9. Config deserializes when observability + watchdog keys are absent
// ---------------------------------------------------------------------------
#[test]
fn config_with_missing_optional_fields() {
    let original = Config::default();
    let mut value: serde_json::Value = serde_json::to_value(&original).unwrap();

    // Remove the two serde(default) fields
    let obj = value.as_object_mut().unwrap();
    obj.remove("observability");
    obj.remove("watchdog");

    let json = serde_json::to_string(&value).unwrap();
    let restored: Config = serde_json::from_str(&json).unwrap();

    // Should get the defaults back
    assert_eq!(restored.mode, RunMode::Shadow);
    assert!(restored.observability.enabled);
    assert_eq!(restored.observability.event_log_dir, "./data/events");
    assert!(restored.watchdog.enabled);
    assert!(!restored.watchdog.enforce_actions);
    assert_eq!(restored.watchdog.max_book_ws_silence_secs, 60);
}

// ---------------------------------------------------------------------------
// 10. ScoreProxyConfig serde roundtrip
// ---------------------------------------------------------------------------
#[test]
fn score_proxy_config_serde_roundtrip() {
    let original = ScoreProxyConfig {
        competition_multiplier: dec!(1.5),
        max_score_share: dec!(0.25),
        min_score_share: dec!(0.0001),
        target_score_share: dec!(0.03),
        calibration_sample_size: 10,
    };

    let json = serde_json::to_string(&original).unwrap();
    let restored: ScoreProxyConfig = serde_json::from_str(&json).unwrap();

    assert_eq!(restored.competition_multiplier, dec!(1.5));
    assert_eq!(restored.max_score_share, dec!(0.25));
    assert_eq!(restored.min_score_share, dec!(0.0001));
    assert_eq!(restored.target_score_share, dec!(0.03));
    assert_eq!(restored.calibration_sample_size, 10);
}
