#![cfg(any())]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use rust_decimal_macros::dec;
use spreadeater::models::Side;
use spreadeater::runtime::hedge_test::{load_scenario, run_hedge_test};
use tempfile::{NamedTempFile, TempDir};

fn fixture_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join("hedge_scenarios")
        .join(name)
}

fn assert_subsequence(actual: &[String], expected: &[&str]) {
    let mut index = 0usize;
    for entry in actual {
        if entry == expected[index] {
            index += 1;
            if index == expected.len() {
                return;
            }
        }
    }
    panic!("expected subsequence {:?} in {:?}", expected, actual);
}

fn fee_rate_paths(requests: &[spreadeater::runtime::hedge_test::MockRequestRecord]) -> Vec<String> {
    requests
        .iter()
        .filter(|entry| entry.method == "GET" && entry.path.starts_with("/fee-rate?token_id="))
        .map(|entry| entry.path.clone())
        .collect()
}

#[tokio::test]
async fn scenario_parsing_supports_post_attribution_work_item() {
    let scenario = load_scenario(&fixture_path("clean_full_buy_hedge.json").to_string_lossy())
        .expect("scenario should parse");

    assert_eq!(scenario.trigger.work_item.match_source, "maker_order_id");
    assert_eq!(scenario.trigger.work_item.size_to_apply, dec!(10));
    assert_eq!(scenario.trigger.work_item.hedge_size, dec!(10));
    assert_eq!(scenario.expected.hedge_side, Some(Side::Buy));
}

#[tokio::test]
async fn full_buy_hedge_records_real_buy_leg_instead_of_sellback_only() {
    let result = run_hedge_test(&fixture_path("clean_full_buy_hedge.json").to_string_lossy())
        .await
        .expect("clean full hedge should run");

    assert!(result.passed);
    assert_eq!(result.observed.hedge_side, Some(Side::Buy));
    assert_eq!(result.observed.planned_hedge_shares, Some(dec!(10)));
    assert_eq!(
        result.observed.sellback_leg_status.as_deref(),
        Some("skipped")
    );
    assert_eq!(result.observed.hedge_price, Some(dec!(0.27)));

    let post_orders = result
        .observed
        .request_log
        .iter()
        .filter(|entry| entry.method == "POST" && entry.path == "/order")
        .count();
    assert_eq!(post_orders, 1);
}

#[tokio::test]
async fn thin_book_split_sequences_buy_then_sellback() {
    let result = run_hedge_test(&fixture_path("thin_book_split.json").to_string_lossy())
        .await
        .expect("thin book split should run");

    assert!(result.passed);
    assert_eq!(result.observed.planned_hedge_shares, Some(dec!(6)));
    assert_eq!(result.observed.planned_sellback_shares, Some(dec!(4)));
    assert_eq!(
        result.observed.sellback_leg_status.as_deref(),
        Some("success")
    );

    let requests: Vec<String> = result
        .observed
        .request_log
        .iter()
        .map(|entry| format!("{} {}", entry.method, entry.path))
        .collect();
    assert_subsequence(&requests, &["POST /order", "DELETE /order", "POST /order"]);
}

#[tokio::test]
async fn delayed_truth_confirmation_uses_retry_sync_position_truth() {
    let result = run_hedge_test(&fixture_path("delayed_truth_confirmation.json").to_string_lossy())
        .await
        .expect("delayed truth scenario should run");

    assert!(result.passed);
    assert_eq!(
        result.observed.hedge_leg_status.as_deref(),
        Some("unverified")
    );
    assert_eq!(result.observed.post_sync_net_exposure, Some(dec!(0)));

    let position_syncs = result
        .observed
        .request_log
        .iter()
        .filter(|entry| entry.path.starts_with("/positions"))
        .count();
    assert!(
        position_syncs >= 2,
        "expected retry sync path, got {}",
        position_syncs
    );
}

#[tokio::test]
async fn non_zero_default_fee_rate_is_fetched_in_layer1_harness() {
    let mut scenario =
        load_scenario(&fixture_path("clean_full_buy_hedge.json").to_string_lossy()).unwrap();
    scenario.exchange.default_fee_rate_bps = 7;
    let expected_token = scenario.market.no_token_id.clone();

    let temp = NamedTempFile::new().expect("temp file");
    fs::write(
        temp.path(),
        serde_json::to_string_pretty(&scenario).expect("serialize scenario"),
    )
    .expect("write temp scenario");

    let result = run_hedge_test(&temp.path().to_string_lossy())
        .await
        .expect("fee-bearing scenario should execute");

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

#[tokio::test]
async fn resolution_failure_fixture_halts_without_submitting_orders() {
    let result =
        run_hedge_test(&fixture_path("resolution_failure_halts_market.json").to_string_lossy())
            .await
            .expect("failure fixture should run");

    assert!(result.passed);
    assert!(result.halted);
    assert_eq!(result.actual_success, false);
    assert_eq!(result.observed.result_status.as_deref(), Some("failed"));

    let cancel_requests = result
        .observed
        .request_log
        .iter()
        .filter(|entry| entry.method == "DELETE" && entry.path == "/order")
        .count();
    assert_eq!(cancel_requests, 0);
}

#[tokio::test]
async fn wrong_expected_hedge_side_and_size_fail_deterministically() {
    let mut scenario =
        load_scenario(&fixture_path("clean_full_buy_hedge.json").to_string_lossy()).unwrap();
    scenario.expected.hedge_side = Some(Side::Sell);
    scenario.expected.planned_hedge_shares = Some(dec!(1));

    let temp = NamedTempFile::new().expect("temp file");
    fs::write(
        temp.path(),
        serde_json::to_string_pretty(&scenario).expect("serialize scenario"),
    )
    .expect("write temp scenario");

    let result = run_hedge_test(&temp.path().to_string_lossy())
        .await
        .expect("mismatch scenario should still execute");

    assert!(!result.passed);
    assert!(result
        .mismatches
        .iter()
        .any(|mismatch| mismatch.contains("hedge_side")));
    assert!(result
        .mismatches
        .iter()
        .any(|mismatch| mismatch.contains("planned_hedge_shares")));
}

#[test]
fn hedge_test_cli_returns_zero_for_passing_fixture() {
    let fixture = fixture_path("clean_full_buy_hedge.json");
    let temp_dir = TempDir::new().expect("temp dir");
    let status = Command::new(env!("CARGO_BIN_EXE_spreadeater"))
        .args(["hedge-test", "--scenario", &fixture.to_string_lossy()])
        .current_dir(temp_dir.path())
        .status()
        .expect("CLI should run");

    assert!(status.success());
}

#[test]
fn hedge_test_cli_returns_non_zero_for_assertion_mismatch() {
    let mut scenario =
        load_scenario(&fixture_path("clean_full_buy_hedge.json").to_string_lossy()).unwrap();
    scenario.expected.hedge_side = Some(Side::Sell);

    let temp = NamedTempFile::new().expect("temp file");
    fs::write(
        temp.path(),
        serde_json::to_string_pretty(&scenario).expect("serialize scenario"),
    )
    .expect("write temp scenario");

    let temp_dir = TempDir::new().expect("temp dir");
    let status = Command::new(env!("CARGO_BIN_EXE_spreadeater"))
        .args(["hedge-test", "--scenario", &temp.path().to_string_lossy()])
        .current_dir(temp_dir.path())
        .status()
        .expect("CLI should run");

    assert_eq!(status.code(), Some(1));
}
