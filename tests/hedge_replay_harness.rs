#![cfg(any())]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use rust_decimal_macros::dec;
use spreadeater::models::Side;
use spreadeater::runtime::hedge_replay::{load_scenario, run_hedge_replay};
use tempfile::{NamedTempFile, TempDir};

fn fixture_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join("hedge_replay_scenarios")
        .join(name)
}

fn fee_rate_paths(requests: &[spreadeater::runtime::hedge_test::MockRequestRecord]) -> Vec<String> {
    requests
        .iter()
        .filter(|entry| entry.method == "GET" && entry.path.starts_with("/fee-rate?token_id="))
        .map(|entry| entry.path.clone())
        .collect()
}

#[tokio::test]
async fn replay_scenario_parsing_supports_sequence_setup_and_expectations() {
    let scenario =
        load_scenario(&fixture_path("raw_trade_immediate_attribution.json").to_string_lossy())
            .expect("scenario should parse");

    assert_eq!(scenario.setup.tracked_orders.len(), 1);
    assert_eq!(scenario.sequence.len(), 2);
    assert_eq!(scenario.expected.final_outcome.hedge_side, Some(Side::Buy));
}

#[tokio::test]
async fn raw_trade_immediate_attribution_uses_real_fill_work_item_path() {
    let result =
        run_hedge_replay(&fixture_path("raw_trade_immediate_attribution.json").to_string_lossy())
            .await
            .expect("raw trade replay should run");

    assert!(result.passed);
    assert_eq!(result.observed.hedge_side, Some(Side::Buy));
    assert_eq!(result.observed.planned_hedge_shares, Some(dec!(10)));
    assert!(result
        .critical_events
        .iter()
        .any(|event| event.event_type == "fill_detected"
            && event.source_component == "fill_handler"
            && event.payload["match_source"] == serde_json::json!("maker_order_id")));
}

#[tokio::test]
async fn order_update_fallback_replays_residual_sizing_instead_of_raw_fill_size() {
    let result = run_hedge_replay(
        &fixture_path("order_update_fallback_partial_accounted.json").to_string_lossy(),
    )
    .await
    .expect("fallback replay should run");

    assert!(result.passed);
    assert_eq!(result.observed.planned_hedge_shares, Some(dec!(6)));
    assert_eq!(result.observed.post_sync_yes_size, Some(dec!(10)));
    assert_eq!(result.observed.post_sync_no_size, Some(dec!(10)));
}

#[tokio::test]
async fn exchange_sync_missing_fill_replays_exchange_order_sync_branch() {
    let result =
        run_hedge_replay(&fixture_path("exchange_sync_missing_fill.json").to_string_lossy())
            .await
            .expect("exchange sync replay should run");

    assert!(result.passed);
    assert!(result
        .critical_events
        .iter()
        .any(|event| event.event_type == "fill_detected"
            && event.payload["match_source"] == serde_json::json!("exchange_order_sync")));

    let cancel_requests = result
        .observed
        .request_log
        .iter()
        .filter(|entry| entry.method == "DELETE" && entry.path == "/order")
        .count();
    assert!(cancel_requests >= 1);
}

#[tokio::test]
async fn orphan_recovery_routes_exposure_into_reconciliation() {
    let result =
        run_hedge_replay(&fixture_path("reconciliation_orphan_recovery.json").to_string_lossy())
            .await
            .expect("orphan recovery replay should run");

    assert!(result.passed);
    assert!(result
        .critical_events
        .iter()
        .any(|event| event.event_type == "fill_detected"
            && event.source_component == "reconciliation_position_orphan"));
}

#[tokio::test]
async fn cancelled_order_regression_stays_deferred_instead_of_misattributed() {
    let result =
        run_hedge_replay(&fixture_path("cancelled_order_not_misattributed.json").to_string_lossy())
            .await
            .expect("cancelled-order replay should run");

    assert!(result.passed);
    assert!(!result.actual_success);
    assert_eq!(result.observed.result_status, None);
    assert!(result
        .critical_events
        .iter()
        .any(|event| event.event_type == "fill_detected"
            && event.payload["deferred_to_reconciliation"] == serde_json::json!(true)));
}

#[tokio::test]
async fn duplicate_trade_id_is_deduped_to_one_actionable_fill_path() {
    let result =
        run_hedge_replay(&fixture_path("duplicate_trade_id_deduped.json").to_string_lossy())
            .await
            .expect("duplicate trade replay should run");

    assert!(result.passed);
    let fill_detected = result
        .critical_events
        .iter()
        .filter(|event| event.event_type == "fill_detected")
        .count();
    assert_eq!(fill_detected, 1);

    let order_posts = result
        .observed
        .request_log
        .iter()
        .filter(|entry| entry.method == "POST" && entry.path == "/order")
        .count();
    assert_eq!(order_posts, 1);
}

#[tokio::test]
async fn per_token_fee_override_is_fetched_in_layer2_replay() {
    let mut scenario =
        load_scenario(&fixture_path("raw_trade_immediate_attribution.json").to_string_lossy())
            .unwrap();
    let expected_token = scenario.market.no_token_id.clone();
    scenario.exchange.default_fee_rate_bps = 3;
    scenario
        .exchange
        .fee_rate_bps
        .insert(expected_token.clone(), 17);

    let temp = NamedTempFile::new().expect("temp file");
    fs::write(
        temp.path(),
        serde_json::to_string_pretty(&scenario).expect("serialize scenario"),
    )
    .expect("write temp scenario");

    let result = run_hedge_replay(&temp.path().to_string_lossy())
        .await
        .expect("fee-bearing replay should execute");

    assert!(result.passed);
    let fee_paths = fee_rate_paths(&result.observed.request_log);
    assert!(
        fee_paths
            .iter()
            .any(|path| path.contains(&format!("token_id={expected_token}"))),
        "expected fee lookup for {}, got {:?}",
        expected_token,
        fee_paths
    );
}

#[test]
fn hedge_replay_cli_returns_zero_for_passing_fixture() {
    let fixture = fixture_path("raw_trade_immediate_attribution.json");
    let temp_dir = TempDir::new().expect("temp dir");
    let status = Command::new(env!("CARGO_BIN_EXE_spreadeater"))
        .args(["hedge-replay", "--scenario", &fixture.to_string_lossy()])
        .current_dir(temp_dir.path())
        .status()
        .expect("CLI should run");

    assert!(status.success());
}

#[test]
fn hedge_replay_cli_returns_non_zero_for_assertion_mismatch() {
    let mut scenario =
        load_scenario(&fixture_path("raw_trade_immediate_attribution.json").to_string_lossy())
            .unwrap();
    scenario.expected.final_outcome.hedge_side = Some(Side::Sell);

    let temp = NamedTempFile::new().expect("temp file");
    fs::write(
        temp.path(),
        serde_json::to_string_pretty(&scenario).expect("serialize scenario"),
    )
    .expect("write temp scenario");

    let temp_dir = TempDir::new().expect("temp dir");
    let status = Command::new(env!("CARGO_BIN_EXE_spreadeater"))
        .args(["hedge-replay", "--scenario", &temp.path().to_string_lossy()])
        .current_dir(temp_dir.path())
        .status()
        .expect("CLI should run");

    assert_eq!(status.code(), Some(1));
}
