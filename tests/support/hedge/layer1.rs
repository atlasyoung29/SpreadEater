use super::*;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, Result};
use rust_decimal::Decimal;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::monitor::ErrorLogger;

#[derive(Debug)]
struct HedgeTestResult {
    scenario_name: String,
    passed: bool,
    actual_success: bool,
    expected_success: bool,
    halted: bool,
    mismatches: Vec<String>,
    observed: ObservedHedgeOutcome,
}

struct HedgeTestHarness {
    fill_handler: FillHandler,
    work_item: FillWorkItem,
    scenario: HedgeScenario,
    event_collector: Arc<InMemoryEventCollector>,
    mock_server: MockExchangeServer,
}

impl HedgeTestHarness {
    async fn from_scenario(scenario: HedgeScenario) -> Result<Self> {
        validate_scenario_market(&scenario.market)?;

        let mock_server = MockExchangeServer::spawn(&scenario.market, &scenario.exchange).await?;

        let mut config = Config::default();
        config.discovery.clob_base_url = mock_server.base_url().to_string();
        config.discovery.data_api_base_url = mock_server.base_url().to_string();

        let creds = build_test_credentials();
        let signer = RequestSigner::new(creds.clone());
        let trading_client = Arc::new(
            TradingClient::new(
                mock_server.base_url().to_string(),
                signer,
                Some(TEST_PRIVATE_KEY),
                TEST_ADDRESS,
                &creds.api_key,
                false,
            )
            .context("Failed to build TradingClient")?,
        );

        let book_manager = Arc::new(BookManager::new());
        book_manager
            .insert_snapshot(scenario_book_to_snapshot(
                &scenario.market.yes_token_id,
                &scenario.exchange.books.yes,
            ))
            .await;
        book_manager
            .insert_snapshot(scenario_book_to_snapshot(
                &scenario.market.no_token_id,
                &scenario.exchange.books.no,
            ))
            .await;

        let book_rest = BookRestClient::new(mock_server.base_url().to_string());
        let position_manager = Arc::new(PositionManager::new(
            mock_server.base_url().to_string(),
            TEST_ADDRESS.to_string(),
        ));
        let risk_manager = Arc::new(RiskManager::new(config.risk.clone()));

        let initial_balance = scenario
            .exchange
            .balances
            .first()
            .map(|step| step.amount)
            .unwrap_or(Decimal::ZERO);
        risk_manager.update_balance(initial_balance).await;

        let cached_balance = Arc::new(RwLock::new(initial_balance));
        let order_manager = OrderManager::new(
            Arc::clone(&trading_client),
            Arc::clone(&cached_balance),
            None,
            "hedge-test".to_string(),
            "hedge-test".to_string(),
            config.risk.cash_reserve,
        );
        order_manager.update_gross_balance(initial_balance).await;

        let hedge_executor =
            HedgeExecutor::new(Arc::clone(&trading_client), Arc::clone(&book_manager));

        let canonical = build_canonical_market(&scenario.market);
        let condition_id = canonical.condition_id.clone();
        let managed_markets = Arc::new(RwLock::new(HashMap::from([(
            condition_id.clone(),
            canonical.clone(),
        )])));
        let known_markets = Arc::new(RwLock::new(HashMap::from([(condition_id, canonical)])));

        let error_dir =
            std::env::temp_dir().join(format!("spreadeater-hedge-test-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&error_dir).context("Failed to create temp error dir")?;
        let error_logger = Arc::new(ErrorLogger::new(&error_dir.to_string_lossy()));

        let event_collector = Arc::new(InMemoryEventCollector::default());
        let event_producer: Arc<dyn spreadeater_core::EventProducer> = event_collector.clone();
        let ctf_merger = scenario.exchange.merge.as_ref().and_then(|behavior| {
            behavior.configured.then(|| {
                Arc::new(ScenarioPairMerger {
                    behavior: behavior.clone(),
                }) as Arc<dyn crate::trading::ctf_merge::PairMerger>
            })
        });

        let fill_handler = FillHandler {
            order_manager,
            hedge_executor,
            managed_markets,
            known_markets,
            risk_manager,
            position_manager,
            book_manager,
            book_rest,
            trading_client,
            config,
            event_producer: Some(event_producer),
            run_id: format!("hedge-test-{}", Uuid::new_v4()),
            mode: "hedge-test".to_string(),
            cached_balance,
            hedge_order_ids: Arc::new(RwLock::new(HashSet::new())),
            recon_baselines: Arc::new(RwLock::new(HashMap::new())),
            hedge_signals: Arc::new(RwLock::new(HashMap::new())),
            recent_resolution_trades: Arc::new(RwLock::new(Vec::new())),
            ctf_merger,
            hedge_locks: Arc::new(RwLock::new(HashMap::new())),
            error_logger,
        };

        Ok(Self {
            fill_handler,
            work_item: build_fill_work_item_from_scenario(&scenario),
            scenario,
            event_collector,
            mock_server,
        })
    }

    async fn run(self) -> Result<HedgeTestResult> {
        self.fill_handler
            .handle_fill(self.work_item)
            .await
            .with_context(|| {
                format!(
                    "Hedge harness runtime error in scenario {}",
                    self.scenario.name
                )
            })?;

        let events = self.event_collector.events();
        let observed = build_observed_outcome(
            &events,
            Arc::clone(&self.fill_handler.risk_manager),
            &self.scenario.market.condition_id,
            self.mock_server.request_log().await,
        )
        .await?;

        let actual_success = observed.result_status.as_deref() == Some("success");
        let mismatches =
            compare_expected_to_observed(&self.scenario.expected, &observed, actual_success);

        Ok(HedgeTestResult {
            scenario_name: self.scenario.name.clone(),
            passed: mismatches.is_empty(),
            actual_success,
            expected_success: self.scenario.expected.success,
            halted: observed.halted,
            mismatches,
            observed,
        })
    }
}

async fn run_hedge_test(name: &str) -> Result<HedgeTestResult> {
    let scenario_path = fixture_path("hedge_scenarios", name);
    let scenario: HedgeScenario = serde_json::from_str(
        &std::fs::read_to_string(&scenario_path)
            .with_context(|| format!("Failed to read scenario {}", scenario_path.display()))?,
    )
    .with_context(|| format!("Failed to parse scenario {}", scenario_path.display()))?;

    HedgeTestHarness::from_scenario(scenario).await?.run().await
}

fn assert_layer1_pass(result: &HedgeTestResult) {
    assert!(
        result.passed,
        "layer1 scenario {} failed with mismatches {:?}; observed={:?}",
        result.scenario_name, result.mismatches, result.observed
    );
    assert_eq!(result.expected_success, result.actual_success);
    assert_eq!(result.halted, result.observed.halted);
}

#[tokio::test]
async fn layer1_clean_full_buy_hedge_matches_expected_outcome() {
    let result = run_hedge_test("clean_full_buy_hedge.json")
        .await
        .expect("clean full hedge should run");

    assert_layer1_pass(&result);
    assert_eq!(result.observed.hedge_side, Some(Side::Buy));
    assert_eq!(result.observed.planned_hedge_shares, Some(dec!(10)));
    assert_eq!(
        result.observed.sellback_leg_status.as_deref(),
        Some("skipped")
    );
}

#[tokio::test]
async fn layer1_thin_book_split_runs_buy_then_sellback_resolution() {
    let result = run_hedge_test("thin_book_split.json")
        .await
        .expect("thin book split should run");

    assert_layer1_pass(&result);
    assert_eq!(result.observed.planned_hedge_shares, Some(dec!(6)));
    assert_eq!(result.observed.planned_sellback_shares, Some(dec!(4)));
    assert_eq!(
        result.observed.sellback_leg_status.as_deref(),
        Some("success")
    );
}

#[tokio::test]
async fn layer1_delayed_truth_confirmation_surfaces_unverified_but_flat_result() {
    let result = run_hedge_test("delayed_truth_confirmation.json")
        .await
        .expect("delayed truth should run");

    assert_layer1_pass(&result);
    assert_eq!(
        result.observed.hedge_leg_status.as_deref(),
        Some("unverified")
    );
    assert_eq!(result.observed.post_sync_net_exposure, Some(Decimal::ZERO));
}

#[tokio::test]
async fn layer1_resolution_failure_halts_market_when_no_resolution_path_exists() {
    let result = run_hedge_test("resolution_failure_halts_market.json")
        .await
        .expect("resolution failure should run");

    assert!(
        result.passed,
        "expected halt fixture to match; mismatches={:?}",
        result.mismatches
    );
    assert!(!result.actual_success);
    assert!(result.halted);
    assert_eq!(result.observed.result_status.as_deref(), Some("failed"));
}

#[tokio::test]
async fn layer1_merge_success_redeems_paired_inventory() {
    let result = run_hedge_test("merge_success_after_full_buy_hedge.json")
        .await
        .expect("merge success fixture should run");

    assert_layer1_pass(&result);
    assert_eq!(
        result.observed.exit_path_status.as_deref(),
        Some("merge_succeeded")
    );
    assert_eq!(result.observed.merge_attempted, Some(true));
    assert_eq!(
        result.observed.merge_tx_hash.as_deref(),
        Some("0xmerge-success")
    );
    assert_eq!(result.observed.merge_eligible_pairs, Some(dec!(10)));
    assert_eq!(result.observed.fallback_ask_count, Some(0));
}

#[tokio::test]
async fn layer1_merge_failure_places_fallback_asks_for_fully_paired_inventory() {
    let result = run_hedge_test("merge_failure_places_fallback_asks.json")
        .await
        .expect("merge failure fixture should run");

    assert_layer1_pass(&result);
    assert_eq!(
        result.observed.exit_path_status.as_deref(),
        Some("fallback_asks_placed")
    );
    assert_eq!(result.observed.merge_attempted, Some(true));
    assert_eq!(result.observed.fallback_ask_count, Some(2));
}

#[tokio::test]
async fn layer1_unconfigured_merger_places_fallback_asks_for_pairs() {
    let result = run_hedge_test("merge_unconfigured_places_fallback_asks.json")
        .await
        .expect("unconfigured merge fixture should run");

    assert_layer1_pass(&result);
    assert_eq!(
        result.observed.exit_path_status.as_deref(),
        Some("fallback_asks_placed")
    );
    assert_eq!(result.observed.ctf_merge_configured, Some(false));
    assert_eq!(result.observed.merge_attempted, Some(false));
    assert_eq!(result.observed.fallback_ask_count, Some(2));
}
