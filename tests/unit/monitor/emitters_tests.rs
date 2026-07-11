use rust_decimal_macros::dec;
use spreadeater::monitor::emitters;
use spreadeater_core::EventType;

#[test]
fn risk_state_changed_has_event_type() {
    let envelope = emitters::build_risk_state_changed(
        "run-1",
        "shadow",
        Some("cond-1"),
        "halted",
        Some("exposure limit"),
        Some(dec!(100)),
        Some(true),
    );
    assert_eq!(envelope.event_type, EventType::RiskStateChanged);
}

#[test]
fn user_stream_status_has_event_type() {
    let envelope = emitters::build_user_stream_status_changed(
        "run-1",
        "shadow",
        "connected",
        Some(5),
        Some("all good"),
    );
    assert_eq!(envelope.event_type, EventType::UserStreamStatusChanged);
}

#[test]
fn status_snapshot_has_event_type() {
    let envelope = emitters::build_status_snapshot(
        "run-1",
        "shadow",
        3,              // managed_markets
        dec!(100),      // order_committed_usd
        dec!(50),       // position_committed_usd
        dec!(150),      // total_committed_usd
        dec!(1000),     // api_balance_usd
        dec!(850),      // available_budget_usd
        dec!(1.5),      // competition_multiplier
        Some(dec!(10)), // total_est_daily_usd
        None,           // book_ws_stats
    );
    assert_eq!(envelope.event_type, EventType::StatusSnapshot);
}

#[test]
fn monitor_degraded_has_event_type() {
    let envelope = emitters::build_monitor_degraded(
        "run-1",
        "shadow",
        "event_writer",
        "queue overflow",
        Some(1000),
    );
    assert_eq!(envelope.event_type, EventType::MonitorDegraded);
}
