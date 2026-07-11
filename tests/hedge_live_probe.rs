#![cfg(any())]

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus};
use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use spreadeater::auth::ApiCredentials;
use spreadeater::config::{Config, RunMode};
use spreadeater::models::events::UserEvent;
use spreadeater::models::{OrderType, QuoteLeg, Side};
use spreadeater::runtime::hedge_live_probe::{
    load_scenario, run_hedge_live_probe_with_options, CleanupStatus, ExpectedCleanupStatus,
    HedgeLiveProbeScenario, LiveProbeExpected, LiveProbeMarket, LiveProbeRuntimeOptions,
    LiveProbeSafety, LiveProbeTrigger, ProbeMergeExecutor, LIVE_PROBE_ARM_ENV,
    LIVE_PROBE_ARM_TOKEN,
};
use spreadeater::runtime::hedge_test::{
    MockExchangeServer, ScenarioBalanceStep, ScenarioBook, ScenarioCancelActionResponse,
    ScenarioExchange, ScenarioExchangeAction, ScenarioExchangeBooks, ScenarioExchangeMutations,
    ScenarioLiveOrder, ScenarioMarket, ScenarioOpenOrdersStep, ScenarioOrderLookupScript,
    ScenarioOrderLookupStep, ScenarioPlacedOrderResponse, ScenarioPositionStep, ScenarioPriceLevel,
    ScenarioTradeLookupScript, ScenarioTradeLookupStep, ScenarioTradeRecord,
    ScenarioUserStreamMessage,
};
use spreadeater::trading::user_stream::UserStream;
use tempfile::{NamedTempFile, TempDir};

fn fixture_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join("hedge_live_probe_scenarios")
        .join(name)
}

fn test_credentials() -> ApiCredentials {
    // Canonical public Hardhat/Anvil test account. Never use this key with real funds.
    ApiCredentials {
        api_key: "hedge-live-probe-key".to_string(),
        secret: base64::engine::general_purpose::STANDARD.encode(b"test-secret-key!!"),
        passphrase: "hedge-live-probe-pass".to_string(),
        address: "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266".to_string(),
        private_key: Some(
            "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80".to_string(),
        ),
        funder: None,
    }
}

fn apply_test_credentials(command: &mut Command) {
    let creds = test_credentials();
    command
        .env("POLY_API_KEY", creds.api_key)
        .env("POLY_SECRET", creds.secret)
        .env("POLY_PASSPHRASE", creds.passphrase)
        .env("POLY_ADDRESS", creds.address)
        .env_remove("POLY_FUNDER");
    if let Some(private_key) = creds.private_key {
        command.env("POLY_PRIVATE_KEY", private_key);
    }
}

fn base_market() -> LiveProbeMarket {
    LiveProbeMarket {
        condition_id: "market-clean-full-buy".to_string(),
        question: Some("Will the paired hedge probe pass?".to_string()),
        yes_token_id: "1001".to_string(),
        no_token_id: "1002".to_string(),
        tick_size: "0.01".to_string(),
        neg_risk: false,
    }
}

fn base_scenario() -> HedgeLiveProbeScenario {
    HedgeLiveProbeScenario {
        name: "paired_yes_buy_probe".to_string(),
        description: "Acquire YES live, then hedge with NO and clean the market.".to_string(),
        market: base_market(),
        trigger: LiveProbeTrigger {
            leg: QuoteLeg::YesBid,
            shares: dec!(10),
            max_trigger_limit_price: dec!(0.76),
        },
        safety: LiveProbeSafety {
            require_clean_market: true,
            max_planned_hedge_shares: dec!(10),
            max_planned_sellback_shares: Decimal::ZERO,
            max_planned_hedge_notional_usdc: dec!(3),
            max_post_sync_net_exposure: dec!(0.5),
            max_trigger_notional_usdc: dec!(8),
            max_cleanup_notional_usdc: dec!(12),
        },
        expected: LiveProbeExpected {
            success: true,
            halted: false,
            hedge_side: Some(Side::Buy),
            critical_event_types: vec![
                "fill_detected".to_string(),
                "hedge_intent_created".to_string(),
                "hedge_result_recorded".to_string(),
            ],
            max_planned_hedge_shares: Some(dec!(10)),
            max_planned_sellback_shares: Some(Decimal::ZERO),
            max_post_sync_net_exposure: Some(dec!(0.5)),
            result_status: Some("success".to_string()),
            hedge_leg_status: Some("success".to_string()),
            sellback_leg_status: Some("skipped".to_string()),
            cleanup_status: Some(ExpectedCleanupStatus::MergedOrFlattened),
            clean_end_state: true,
        },
    }
}

fn mock_market(market: &LiveProbeMarket) -> ScenarioMarket {
    ScenarioMarket {
        condition_id: market.condition_id.clone(),
        question: market
            .question
            .clone()
            .unwrap_or_else(|| "Mock live probe market".to_string()),
        yes_token_id: market.yes_token_id.clone(),
        no_token_id: market.no_token_id.clone(),
        daily_reward_total: dec!(100),
        max_spread: dec!(0.10),
        tick_size: market.tick_size.clone(),
    }
}

fn position_step(
    yes: Decimal,
    no: Decimal,
    yes_avg: Decimal,
    no_avg: Decimal,
) -> ScenarioPositionStep {
    ScenarioPositionStep {
        yes_size: yes,
        no_size: no,
        yes_avg_price: yes_avg,
        no_avg_price: no_avg,
        delay_ms: 0,
    }
}

fn order_lookup(
    id: &str,
    leg: QuoteLeg,
    price: Decimal,
    size: Decimal,
    matched: Decimal,
    status: &str,
    associated_trade_ids: &[&str],
) -> ScenarioOrderLookupScript {
    ScenarioOrderLookupScript {
        order_id: id.to_string(),
        responses: vec![ScenarioOrderLookupStep {
            order: Some(ScenarioLiveOrder {
                id: id.to_string(),
                leg,
                price,
                original_size: size,
                size_matched: matched,
                status: status.to_string(),
                order_type: "GTC".to_string(),
                created_at_unix: None,
                associated_trade_ids: associated_trade_ids
                    .iter()
                    .map(|value| value.to_string())
                    .collect(),
            }),
            delay_ms: 0,
        }],
    }
}

fn trade_lookup(
    trade_id: &str,
    leg: QuoteLeg,
    price: Decimal,
    size: Decimal,
    taker_order_id: &str,
) -> ScenarioTradeLookupScript {
    ScenarioTradeLookupScript {
        trade_id: trade_id.to_string(),
        responses: vec![ScenarioTradeLookupStep {
            trade: Some(ScenarioTradeRecord {
                id: trade_id.to_string(),
                leg,
                side: Side::Buy,
                price,
                size,
                status: "MATCHED".to_string(),
                taker_order_id: taker_order_id.to_string(),
                maker_order_id: Some(format!("maker-{}", trade_id)),
                match_time_unix: None,
            }),
            delay_ms: 0,
        }],
    }
}

fn ws_trade_message(
    trade_id: &str,
    market: &ScenarioMarket,
    leg: QuoteLeg,
    size: Decimal,
    order_id: &str,
    delay_ms: u64,
) -> ScenarioUserStreamMessage {
    let (asset_id, outcome) = match leg {
        QuoteLeg::YesBid | QuoteLeg::YesAsk => (market.yes_token_id.clone(), "YES"),
        QuoteLeg::NoBid | QuoteLeg::NoAsk => (market.no_token_id.clone(), "NO"),
    };
    ScenarioUserStreamMessage {
        text: serde_json::json!({
            "type": "TRADE",
            "id": trade_id,
            "market": market.condition_id,
            "asset_id": asset_id,
            "side": "BUY",
            "price": "0.75",
            "size": size.to_string(),
            "outcome": outcome,
            "status": "MATCHED",
            "timestamp": "2026-03-29T00:00:00Z",
            "maker_order_id": format!("maker-{trade_id}"),
            "taker_order_id": order_id,
        })
        .to_string(),
        delay_ms,
    }
}

fn ws_order_update_message(
    order_id: &str,
    market: &ScenarioMarket,
    leg: QuoteLeg,
    size_matched: Decimal,
    delay_ms: u64,
) -> ScenarioUserStreamMessage {
    let (asset_id, outcome) = match leg {
        QuoteLeg::YesBid | QuoteLeg::YesAsk => (market.yes_token_id.clone(), "YES"),
        QuoteLeg::NoBid | QuoteLeg::NoAsk => (market.no_token_id.clone(), "NO"),
    };
    ScenarioUserStreamMessage {
        text: serde_json::json!({
            "type": "UPDATE",
            "id": order_id,
            "order_id": order_id,
            "market": market.condition_id,
            "asset_id": asset_id,
            "side": "BUY",
            "price": "0.75",
            "original_size": "10",
            "size_matched": size_matched.to_string(),
            "outcome": outcome,
            "timestamp": "2026-03-29T00:00:00Z",
            "event_type": "UPDATE",
        })
        .to_string(),
        delay_ms,
    }
}

fn place_action_with_scripts_and_ws(
    token_id: &str,
    side: Side,
    order_type: OrderType,
    order_id: &str,
    response_status: &str,
    trade_ids: &[&str],
    balances: Option<Vec<ScenarioBalanceStep>>,
    positions: Option<Vec<ScenarioPositionStep>>,
    lookup: Vec<ScenarioOrderLookupScript>,
    trade_lookup: Vec<ScenarioTradeLookupScript>,
    ws_messages: Vec<ScenarioUserStreamMessage>,
) -> ScenarioExchangeAction {
    ScenarioExchangeAction::Place {
        expected_token_id: Some(token_id.to_string()),
        expected_side: Some(side),
        expected_order_type: Some(order_type),
        response: ScenarioPlacedOrderResponse {
            order_id: order_id.to_string(),
            status: response_status.to_string(),
            trade_ids: trade_ids.iter().map(|value| value.to_string()).collect(),
            transaction_hashes: if response_status.eq_ignore_ascii_case("matched") {
                vec![format!("0x{}", order_id)]
            } else {
                Vec::new()
            },
            taking_amount: None,
            making_amount: None,
            delay_ms: 0,
        },
        mutations: ScenarioExchangeMutations {
            replace_balances: balances,
            replace_positions: positions,
            replace_global_open_orders: None,
            replace_market_open_orders: None,
            replace_order_lookup: lookup,
            replace_trade_lookup: trade_lookup,
            append_user_stream_messages: ws_messages,
        },
    }
}

fn place_action_with_scripts(
    token_id: &str,
    side: Side,
    order_type: OrderType,
    order_id: &str,
    response_status: &str,
    trade_ids: &[&str],
    balances: Option<Vec<ScenarioBalanceStep>>,
    positions: Option<Vec<ScenarioPositionStep>>,
    lookup: Vec<ScenarioOrderLookupScript>,
    trade_lookup: Vec<ScenarioTradeLookupScript>,
) -> ScenarioExchangeAction {
    place_action_with_scripts_and_ws(
        token_id,
        side,
        order_type,
        order_id,
        response_status,
        trade_ids,
        balances,
        positions,
        lookup,
        trade_lookup,
        Vec::new(),
    )
}

fn cancel_action(order_id: &str) -> ScenarioExchangeAction {
    ScenarioExchangeAction::Cancel {
        expected_order_id: order_id.to_string(),
        response: ScenarioCancelActionResponse::default(),
        mutations: ScenarioExchangeMutations::default(),
    }
}

fn set_place_taking_amount(action: &mut ScenarioExchangeAction, taking_amount: Decimal) {
    match action {
        ScenarioExchangeAction::Place { response, .. } => {
            response.taking_amount = Some(taking_amount.to_string());
        }
        other => panic!("expected place action, got {:?}", other),
    }
}

fn base_exchange() -> ScenarioExchange {
    ScenarioExchange {
        books: ScenarioExchangeBooks {
            yes: ScenarioBook {
                bids: vec![ScenarioPriceLevel(dec!(0.73), dec!(40))],
                asks: vec![ScenarioPriceLevel(dec!(0.75), dec!(40))],
            },
            no: ScenarioBook {
                bids: vec![ScenarioPriceLevel(dec!(0.24), dec!(40))],
                asks: vec![ScenarioPriceLevel(dec!(0.26), dec!(40))],
            },
        },
        default_fee_rate_bps: 0,
        fee_rate_bps: HashMap::new(),
        balances: vec![ScenarioBalanceStep {
            amount: dec!(100),
            delay_ms: 0,
        }],
        positions: vec![position_step(
            Decimal::ZERO,
            Decimal::ZERO,
            dec!(0.5),
            dec!(0.5),
        )],
        global_open_orders: vec![ScenarioOpenOrdersStep {
            orders: Vec::new(),
            delay_ms: 0,
        }],
        market_open_orders: vec![ScenarioOpenOrdersStep {
            orders: Vec::new(),
            delay_ms: 0,
        }],
        order_lookup: Vec::new(),
        trade_lookup: Vec::new(),
        user_stream_connect_ack_delay_ms: 0,
        actions: Vec::new(),
    }
}

fn success_exchange_flattened() -> ScenarioExchange {
    let mut exchange = base_exchange();
    let market = mock_market(&base_market());
    exchange.actions = vec![
        place_action_with_scripts_and_ws(
            "1001",
            Side::Buy,
            OrderType::GTC,
            "trigger-order",
            "matched",
            &["trigger-trade"],
            Some(vec![ScenarioBalanceStep {
                amount: dec!(92.40),
                delay_ms: 0,
            }]),
            Some(vec![position_step(
                dec!(10),
                Decimal::ZERO,
                dec!(0.75),
                dec!(0.5),
            )]),
            vec![order_lookup(
                "trigger-order",
                QuoteLeg::YesBid,
                dec!(0.76),
                dec!(10),
                dec!(10),
                "matched",
                &["trigger-trade"],
            )],
            vec![trade_lookup(
                "trigger-trade",
                QuoteLeg::YesBid,
                dec!(0.75),
                dec!(10),
                "trigger-order",
            )],
            vec![ws_trade_message(
                "trigger-trade",
                &market,
                QuoteLeg::YesBid,
                dec!(10),
                "trigger-order",
                0,
            )],
        ),
        place_action_with_scripts(
            "1002",
            Side::Buy,
            OrderType::GTC,
            "hedge-order",
            "live",
            &[],
            Some(vec![ScenarioBalanceStep {
                amount: dec!(89.80),
                delay_ms: 0,
            }]),
            Some(vec![position_step(
                dec!(10),
                dec!(10),
                dec!(0.75),
                dec!(0.26),
            )]),
            vec![order_lookup(
                "hedge-order",
                QuoteLeg::NoBid,
                dec!(0.27),
                dec!(10),
                dec!(10),
                "matched",
                &[],
            )],
            Vec::new(),
        ),
        cancel_action("hedge-order"),
        cancel_action("trigger-order"),
        place_action_with_scripts(
            "1001",
            Side::Sell,
            OrderType::FOK,
            "cleanup-yes",
            "matched",
            &[],
            Some(vec![ScenarioBalanceStep {
                amount: dec!(97.10),
                delay_ms: 0,
            }]),
            Some(vec![position_step(
                Decimal::ZERO,
                dec!(10),
                Decimal::ZERO,
                dec!(0.26),
            )]),
            Vec::new(),
            Vec::new(),
        ),
        place_action_with_scripts(
            "1002",
            Side::Sell,
            OrderType::FOK,
            "cleanup-no",
            "matched",
            &[],
            Some(vec![ScenarioBalanceStep {
                amount: dec!(99.50),
                delay_ms: 0,
            }]),
            Some(vec![position_step(
                Decimal::ZERO,
                Decimal::ZERO,
                Decimal::ZERO,
                Decimal::ZERO,
            )]),
            Vec::new(),
            Vec::new(),
        ),
        place_action_with_scripts(
            "1002",
            Side::Sell,
            OrderType::FOK,
            "cleanup-no",
            "matched",
            &[],
            Some(vec![ScenarioBalanceStep {
                amount: dec!(99.95),
                delay_ms: 0,
            }]),
            Some(vec![position_step(
                Decimal::ZERO,
                Decimal::ZERO,
                Decimal::ZERO,
                Decimal::ZERO,
            )]),
            Vec::new(),
            Vec::new(),
        ),
    ];
    exchange
}

fn config_for_mock(base_url: &str, temp_dir: &TempDir) -> Config {
    let mut config = Config::default();
    config.mode = RunMode::Live;
    config.discovery.clob_base_url = base_url.to_string();
    config.discovery.data_api_base_url = base_url.to_string();
    config.persistence.archive_dir = temp_dir
        .path()
        .join("archive")
        .to_string_lossy()
        .into_owned();
    config.observability.enabled = false;
    config
}

fn write_temp_json<T: serde::Serialize>(value: &T) -> NamedTempFile {
    let temp = NamedTempFile::new().expect("temp file");
    fs::write(
        temp.path(),
        serde_json::to_string_pretty(value).expect("serialize json"),
    )
    .expect("write temp file");
    temp
}

async fn run_command_status(mut command: Command) -> ExitStatus {
    tokio::task::spawn_blocking(move || command.status().expect("CLI should run"))
        .await
        .expect("spawn_blocking should complete")
}

struct NoopMerger;

#[async_trait]
impl ProbeMergeExecutor for NoopMerger {
    async fn try_merge_pairs(
        &self,
        _engine: &spreadeater::runtime::LiveEngine,
        _condition_id: &str,
        _pair_amount: Decimal,
    ) -> anyhow::Result<Option<String>> {
        Ok(None)
    }
}

struct SuccessfulTestMerger {
    server: Arc<MockExchangeServer>,
}

#[async_trait]
impl ProbeMergeExecutor for SuccessfulTestMerger {
    async fn try_merge_pairs(
        &self,
        _engine: &spreadeater::runtime::LiveEngine,
        _condition_id: &str,
        _pair_amount: Decimal,
    ) -> anyhow::Result<Option<String>> {
        self.server
            .replace_positions(vec![position_step(
                Decimal::ZERO,
                Decimal::ZERO,
                Decimal::ZERO,
                Decimal::ZERO,
            )])
            .await;
        Ok(Some("0xtest-merge".to_string()))
    }
}

async fn run_probe(
    scenario: HedgeLiveProbeScenario,
    server: Arc<MockExchangeServer>,
    temp_dir: &TempDir,
) -> anyhow::Result<spreadeater::runtime::hedge_live_probe::HedgeLiveProbeResult> {
    let mut config = config_for_mock(server.base_url(), temp_dir);
    config.discovery.user_ws_url = server.user_ws_url().to_string();
    run_hedge_live_probe_with_options(
        scenario,
        config,
        test_credentials(),
        LiveProbeRuntimeOptions::new_for_tests(Arc::new(NoopMerger)),
    )
    .await
}

fn fee_rate_paths(requests: &[spreadeater::runtime::hedge_test::MockRequestRecord]) -> Vec<String> {
    requests
        .iter()
        .filter(|entry| entry.method == "GET" && entry.path.starts_with("/fee-rate?token_id="))
        .map(|entry| entry.path.clone())
        .collect()
}

#[test]
fn scenario_parsing_supports_paired_probe_shape() {
    let scenario =
        load_scenario(&fixture_path("template_small_yes_buy_probe.json").to_string_lossy())
            .expect("scenario should parse");
    assert!(scenario.expected.clean_end_state);
    assert_eq!(scenario.trigger.leg, QuoteLeg::YesBid);
    assert_eq!(scenario.trigger.shares, dec!(1));
}

#[test]
fn scenario_shape_no_longer_rejects_non_bid_trigger_leg_at_parse_time() {
    let mut scenario = base_scenario();
    scenario.trigger.leg = QuoteLeg::YesAsk;
    let temp = write_temp_json(&scenario);
    let parsed = load_scenario(&temp.path().to_string_lossy()).expect("scenario should parse");
    assert_eq!(parsed.trigger.leg, QuoteLeg::YesAsk);
}

#[test]
fn hedge_live_probe_cli_requires_arm_env_before_loading_credentials() {
    let fixture = fixture_path("template_small_yes_buy_probe.json");
    let temp_dir = TempDir::new().expect("temp dir");
    let output = Command::new(env!("CARGO_BIN_EXE_spreadeater"))
        .args(["hedge-live-probe", "--scenario", &fixture.to_string_lossy()])
        .current_dir(temp_dir.path())
        .output()
        .expect("CLI should run");
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains(LIVE_PROBE_ARM_ENV));
}

#[tokio::test]
async fn user_stream_mock_ack_and_trade_parse_with_real_parser() {
    let live_market = base_market();
    let market = mock_market(&live_market);
    let server = Arc::new(
        MockExchangeServer::spawn(&market, &base_exchange())
            .await
            .unwrap(),
    );
    let mut rx = UserStream::new_with_url(test_credentials(), server.user_ws_url().to_string())
        .subscribe(Vec::new())
        .await
        .unwrap();

    loop {
        match rx.recv().await {
            Some(UserEvent::Connected { .. }) => break,
            Some(UserEvent::RawActivity) => continue,
            other => panic!("expected Connected, got {:?}", other),
        }
    }

    server
        .emit_user_stream_messages(vec![ws_trade_message(
            "ws-trigger-trade",
            &market,
            QuoteLeg::YesBid,
            dec!(10),
            "ws-trigger-order",
            0,
        )])
        .await;

    loop {
        match rx.recv().await {
            Some(UserEvent::RawActivity) => continue,
            Some(UserEvent::Trade(trade)) => {
                assert_eq!(trade.id, "ws-trigger-trade");
                assert_eq!(trade.condition_id, live_market.condition_id);
                assert_eq!(trade.taker_order_id.as_deref(), Some("ws-trigger-order"));
                break;
            }
            other => panic!("expected Trade, got {:?}", other),
        }
    }
}

#[tokio::test]
async fn clean_market_preflight_rejects_existing_open_orders() {
    let scenario = base_scenario();
    let market = mock_market(&scenario.market);
    let mut exchange = success_exchange_flattened();
    exchange.market_open_orders = vec![ScenarioOpenOrdersStep {
        orders: vec![ScenarioLiveOrder {
            id: "existing-open-order".to_string(),
            leg: QuoteLeg::YesBid,
            price: dec!(0.74),
            original_size: dec!(5),
            size_matched: Decimal::ZERO,
            status: "live".to_string(),
            order_type: "GTC".to_string(),
            created_at_unix: None,
            associated_trade_ids: Vec::new(),
        }],
        delay_ms: 0,
    }];
    let server = Arc::new(MockExchangeServer::spawn(&market, &exchange).await.unwrap());
    let temp_dir = TempDir::new().unwrap();
    let err = run_probe(scenario, server, &temp_dir).await.unwrap_err();
    assert!(err.to_string().contains("existing open orders"));
}

#[tokio::test]
async fn clean_market_preflight_rejects_directional_inventory() {
    let scenario = base_scenario();
    let market = mock_market(&scenario.market);
    let mut exchange = success_exchange_flattened();
    exchange.positions = vec![position_step(dec!(1), Decimal::ZERO, dec!(0.74), dec!(0.5))];
    let server = Arc::new(MockExchangeServer::spawn(&market, &exchange).await.unwrap());
    let temp_dir = TempDir::new().unwrap();
    let err = run_probe(scenario, server, &temp_dir).await.unwrap_err();
    assert!(err.to_string().contains("existing inventory"));
}

#[tokio::test]
async fn discovery_metadata_mismatch_aborts_probe() {
    let scenario = base_scenario();
    let mut mismatched_market = mock_market(&scenario.market);
    mismatched_market.yes_token_id = "2001".to_string();
    let server = Arc::new(
        MockExchangeServer::spawn(&mismatched_market, &success_exchange_flattened())
            .await
            .unwrap(),
    );
    let temp_dir = TempDir::new().unwrap();
    let err = run_probe(scenario, server, &temp_dir).await.unwrap_err();
    assert!(err.to_string().contains("metadata disagreed"));
}

#[tokio::test]
async fn trigger_price_cap_rejection_happens_before_order_placement() {
    let mut scenario = base_scenario();
    scenario.trigger.max_trigger_limit_price = dec!(0.75);
    let market = mock_market(&scenario.market);
    let server = Arc::new(
        MockExchangeServer::spawn(&market, &success_exchange_flattened())
            .await
            .unwrap(),
    );
    let temp_dir = TempDir::new().unwrap();
    let err = run_probe(scenario, server, &temp_dir).await.unwrap_err();
    assert!(err.to_string().contains("trigger_limit_price"));
}

#[tokio::test]
async fn successful_paired_probe_with_flatten_cleanup() {
    let scenario = base_scenario();
    let market = mock_market(&scenario.market);
    let server = Arc::new(
        MockExchangeServer::spawn(&market, &success_exchange_flattened())
            .await
            .unwrap(),
    );
    let temp_dir = TempDir::new().unwrap();
    let result = run_probe(scenario, server, &temp_dir).await.unwrap();
    assert!(
        result.meta_pass && result.standard_pass,
        "meta={:?} standard={:?}",
        result.meta_failures,
        result.standard_mismatches
    );
    assert_eq!(result.trigger.order_id.as_deref(), Some("trigger-order"));
    assert_eq!(result.trigger.resolved_trade_shares, dec!(10));
    assert_eq!(result.observed.post_sync_yes_size, Some(dec!(10)));
    assert_eq!(result.observed.post_sync_no_size, Some(dec!(10)));
    assert!(result.cleanup.success);
    assert_eq!(result.cleanup.status, Some(CleanupStatus::Flattened));
    assert!(result.cleanup.clean_end_state);
}

#[tokio::test]
async fn late_paired_inventory_after_candidate_clean_fails_cleanup() {
    let scenario = base_scenario();
    let market = mock_market(&scenario.market);
    let mut exchange = success_exchange_flattened();
    match exchange.actions.get_mut(5) {
        Some(ScenarioExchangeAction::Place { mutations, .. }) => {
            mutations.replace_positions = Some(vec![
                position_step(Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO),
                position_step(Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO),
                position_step(dec!(10), dec!(10), dec!(0.75), dec!(0.26)),
            ]);
        }
        other => panic!("expected cleanup place action, got {:?}", other),
    }
    let server = Arc::new(MockExchangeServer::spawn(&market, &exchange).await.unwrap());
    let temp_dir = TempDir::new().unwrap();
    let result = run_probe(scenario, server, &temp_dir).await.unwrap();
    assert!(result.meta_pass, "{:?}", result.meta_failures);
    assert!(result.standard_pass, "{:?}", result.standard_mismatches);
    assert!(!result.cleanup.success);
    assert!(!result.cleanup.clean_end_state);
    assert_eq!(
        result.cleanup.failure_code.as_deref(),
        Some("cleanup_residual_inventory")
    );
}

#[tokio::test]
async fn delayed_user_stream_trade_then_hedges() {
    let scenario = base_scenario();
    let market = mock_market(&scenario.market);
    let mut exchange = success_exchange_flattened();
    exchange.actions[0] = place_action_with_scripts_and_ws(
        "1001",
        Side::Buy,
        OrderType::GTC,
        "trigger-order",
        "matched",
        &["trigger-trade"],
        Some(vec![ScenarioBalanceStep {
            amount: dec!(92.40),
            delay_ms: 0,
        }]),
        Some(vec![position_step(
            dec!(10),
            Decimal::ZERO,
            dec!(0.75),
            dec!(0.5),
        )]),
        vec![order_lookup(
            "trigger-order",
            QuoteLeg::YesBid,
            dec!(0.76),
            dec!(10),
            dec!(10),
            "matched",
            &["trigger-trade"],
        )],
        Vec::new(),
        vec![ws_trade_message(
            "trigger-trade",
            &market,
            QuoteLeg::YesBid,
            dec!(10),
            "trigger-order",
            50,
        )],
    );
    let server = Arc::new(MockExchangeServer::spawn(&market, &exchange).await.unwrap());
    let temp_dir = TempDir::new().unwrap();
    let result = run_probe(scenario, server, &temp_dir).await.unwrap();
    assert!(
        result.meta_pass && result.standard_pass,
        "meta={:?} standard={:?}",
        result.meta_failures,
        result.standard_mismatches
    );
    assert!(result.trigger.success);
    assert_eq!(result.trigger.resolved_trade_shares, dec!(10));
    assert!(result.trigger.ws_trade_observed);
    assert_eq!(result.observed.result_status.as_deref(), Some("success"));
}

#[tokio::test]
async fn delayed_user_stream_trade_within_ambiguous_grace_window_meta_passes() {
    let scenario = base_scenario();
    let market = mock_market(&scenario.market);
    let mut exchange = success_exchange_flattened();
    exchange.actions[0] = place_action_with_scripts_and_ws(
        "1001",
        Side::Buy,
        OrderType::GTC,
        "trigger-order",
        "matched",
        &["trigger-trade"],
        Some(vec![ScenarioBalanceStep {
            amount: dec!(92.40),
            delay_ms: 0,
        }]),
        Some(vec![position_step(
            dec!(10),
            Decimal::ZERO,
            dec!(0.75),
            dec!(0.5),
        )]),
        vec![order_lookup(
            "trigger-order",
            QuoteLeg::YesBid,
            dec!(0.76),
            dec!(10),
            dec!(10),
            "matched",
            &["trigger-trade"],
        )],
        vec![trade_lookup(
            "trigger-trade",
            QuoteLeg::YesBid,
            dec!(0.75),
            dec!(10),
            "trigger-order",
        )],
        vec![ws_trade_message(
            "trigger-trade",
            &market,
            QuoteLeg::YesBid,
            dec!(10),
            "trigger-order",
            1_100,
        )],
    );
    let server = Arc::new(MockExchangeServer::spawn(&market, &exchange).await.unwrap());
    let temp_dir = TempDir::new().unwrap();
    let result = run_probe(scenario, server, &temp_dir).await.unwrap();
    assert!(result.meta_pass, "{:?}", result.meta_failures);
    assert!(result.standard_pass, "{:?}", result.standard_mismatches);
    assert!(result.trigger.ws_trade_observed);
    assert_eq!(result.trigger.resolved_trade_shares, dec!(10));
}

#[tokio::test]
async fn trigger_can_start_before_connected_ack_and_still_reach_production_path() {
    let scenario = base_scenario();
    let market = mock_market(&scenario.market);
    let mut exchange = success_exchange_flattened();
    exchange.user_stream_connect_ack_delay_ms = 500;
    let server = Arc::new(MockExchangeServer::spawn(&market, &exchange).await.unwrap());
    let temp_dir = TempDir::new().unwrap();
    let result = run_probe(scenario, server, &temp_dir).await.unwrap();
    assert!(result.meta_pass, "{:?}", result.meta_failures);
    assert!(result.standard_pass, "{:?}", result.standard_mismatches);
    assert!(result.trigger.ws_trade_observed);
}

#[tokio::test]
async fn duplicate_user_stream_trade_ids_are_deduped() {
    let scenario = base_scenario();
    let market = mock_market(&scenario.market);
    let mut exchange = success_exchange_flattened();
    exchange.actions[0] = place_action_with_scripts_and_ws(
        "1001",
        Side::Buy,
        OrderType::GTC,
        "trigger-order",
        "matched",
        &["trigger-trade"],
        Some(vec![ScenarioBalanceStep {
            amount: dec!(92.40),
            delay_ms: 0,
        }]),
        Some(vec![position_step(
            dec!(10),
            Decimal::ZERO,
            dec!(0.75),
            dec!(0.5),
        )]),
        vec![order_lookup(
            "trigger-order",
            QuoteLeg::YesBid,
            dec!(0.76),
            dec!(10),
            dec!(10),
            "matched",
            &["trigger-trade"],
        )],
        Vec::new(),
        vec![
            ws_trade_message(
                "trigger-trade",
                &market,
                QuoteLeg::YesBid,
                dec!(10),
                "trigger-order",
                0,
            ),
            ws_trade_message(
                "trigger-trade",
                &market,
                QuoteLeg::YesBid,
                dec!(10),
                "trigger-order",
                10,
            ),
        ],
    );
    let server = Arc::new(MockExchangeServer::spawn(&market, &exchange).await.unwrap());
    let temp_dir = TempDir::new().unwrap();
    let result = run_probe(scenario, server, &temp_dir).await.unwrap();
    assert!(
        result.meta_pass && result.standard_pass,
        "meta={:?} standard={:?}",
        result.meta_failures,
        result.standard_mismatches
    );
    let fill_detected_count = result
        .critical_events
        .iter()
        .filter(|event| event.event_type == "fill_detected")
        .count();
    assert_eq!(fill_detected_count, 1);
}

#[tokio::test]
async fn exact_trigger_fill_across_multiple_ws_trades_succeeds() {
    let scenario = base_scenario();
    let market = mock_market(&scenario.market);
    let mut exchange = base_exchange();
    exchange.actions = vec![
        place_action_with_scripts_and_ws(
            "1001",
            Side::Buy,
            OrderType::GTC,
            "trigger-order",
            "live",
            &["trigger-trade-a", "trigger-trade-b"],
            Some(vec![ScenarioBalanceStep {
                amount: dec!(92.40),
                delay_ms: 0,
            }]),
            Some(vec![position_step(
                dec!(10),
                Decimal::ZERO,
                dec!(0.75),
                dec!(0.5),
            )]),
            vec![order_lookup(
                "trigger-order",
                QuoteLeg::YesBid,
                dec!(0.76),
                dec!(10),
                dec!(10),
                "matched",
                &["trigger-trade-a", "trigger-trade-b"],
            )],
            vec![
                trade_lookup(
                    "trigger-trade-a",
                    QuoteLeg::YesBid,
                    dec!(0.75),
                    dec!(4),
                    "trigger-order",
                ),
                trade_lookup(
                    "trigger-trade-b",
                    QuoteLeg::YesBid,
                    dec!(0.75),
                    dec!(6),
                    "trigger-order",
                ),
            ],
            vec![
                ws_trade_message(
                    "trigger-trade-a",
                    &market,
                    QuoteLeg::YesBid,
                    dec!(4),
                    "trigger-order",
                    0,
                ),
                ws_trade_message(
                    "trigger-trade-b",
                    &market,
                    QuoteLeg::YesBid,
                    dec!(6),
                    "trigger-order",
                    25,
                ),
            ],
        ),
        place_action_with_scripts(
            "1002",
            Side::Buy,
            OrderType::GTC,
            "hedge-order-a",
            "live",
            &[],
            Some(vec![ScenarioBalanceStep {
                amount: dec!(91.36),
                delay_ms: 0,
            }]),
            Some(vec![position_step(
                dec!(10),
                dec!(4),
                dec!(0.75),
                dec!(0.26),
            )]),
            vec![order_lookup(
                "hedge-order-a",
                QuoteLeg::NoBid,
                dec!(0.27),
                dec!(4),
                dec!(4),
                "matched",
                &[],
            )],
            Vec::new(),
        ),
        cancel_action("hedge-order-a"),
        place_action_with_scripts(
            "1002",
            Side::Buy,
            OrderType::GTC,
            "hedge-order-b",
            "live",
            &[],
            Some(vec![ScenarioBalanceStep {
                amount: dec!(89.80),
                delay_ms: 0,
            }]),
            Some(vec![position_step(
                dec!(10),
                dec!(10),
                dec!(0.75),
                dec!(0.26),
            )]),
            vec![order_lookup(
                "hedge-order-b",
                QuoteLeg::NoBid,
                dec!(0.27),
                dec!(6),
                dec!(6),
                "matched",
                &[],
            )],
            Vec::new(),
        ),
        cancel_action("trigger-order"),
        cancel_action("trigger-order"),
        cancel_action("hedge-order-b"),
        place_action_with_scripts(
            "1001",
            Side::Sell,
            OrderType::FOK,
            "cleanup-yes",
            "matched",
            &[],
            Some(vec![ScenarioBalanceStep {
                amount: dec!(97.10),
                delay_ms: 0,
            }]),
            Some(vec![position_step(
                Decimal::ZERO,
                dec!(10),
                Decimal::ZERO,
                dec!(0.26),
            )]),
            Vec::new(),
            Vec::new(),
        ),
        place_action_with_scripts(
            "1002",
            Side::Sell,
            OrderType::FOK,
            "cleanup-no",
            "matched",
            &[],
            Some(vec![ScenarioBalanceStep {
                amount: dec!(99.95),
                delay_ms: 0,
            }]),
            Some(vec![position_step(
                Decimal::ZERO,
                Decimal::ZERO,
                Decimal::ZERO,
                Decimal::ZERO,
            )]),
            Vec::new(),
            Vec::new(),
        ),
    ];
    let server = Arc::new(MockExchangeServer::spawn(&market, &exchange).await.unwrap());
    let temp_dir = TempDir::new().unwrap();
    let result = run_probe(scenario, server, &temp_dir).await.unwrap();
    assert!(result.meta_pass, "{:?}", result.meta_failures);
    assert!(result.trigger.success);
    assert_eq!(result.trigger.resolved_trade_shares, dec!(10));
    assert!(result.trigger.ws_trade_observed);
}

#[tokio::test]
async fn non_zero_fee_paths_are_exercised_in_layer3_mock_probe() {
    let scenario = base_scenario();
    let market = mock_market(&scenario.market);
    let mut exchange = success_exchange_flattened();
    exchange.default_fee_rate_bps = 5;
    exchange
        .fee_rate_bps
        .insert(scenario.market.no_token_id.clone(), 11);
    let server = Arc::new(MockExchangeServer::spawn(&market, &exchange).await.unwrap());
    let temp_dir = TempDir::new().unwrap();
    let result = run_probe(scenario.clone(), Arc::clone(&server), &temp_dir)
        .await
        .unwrap();

    assert!(result.meta_pass, "{:?}", result.meta_failures);
    assert!(result.standard_pass, "{:?}", result.standard_mismatches);

    let fee_paths = fee_rate_paths(&server.request_log().await);
    assert!(
        fee_paths
            .iter()
            .any(|path| path.contains(&format!("token_id={}", scenario.market.yes_token_id))),
        "expected trigger token fee lookup, got {:?}",
        fee_paths
    );
    assert!(
        fee_paths
            .iter()
            .any(|path| path.contains(&format!("token_id={}", scenario.market.no_token_id))),
        "expected hedge token fee lookup, got {:?}",
        fee_paths
    );
}

#[tokio::test]
async fn trigger_no_fill_fails_without_hedging_or_cleanup_orders() {
    let scenario = base_scenario();
    let market = mock_market(&scenario.market);
    let mut exchange = base_exchange();
    exchange.actions = vec![
        place_action_with_scripts(
            "1001",
            Side::Buy,
            OrderType::GTC,
            "trigger-order",
            "invalid",
            &[],
            None,
            None,
            Vec::new(),
            Vec::new(),
        ),
        cancel_action("trigger-order"),
    ];
    let server = Arc::new(MockExchangeServer::spawn(&market, &exchange).await.unwrap());
    let temp_dir = TempDir::new().unwrap();
    let result = run_probe(scenario, server, &temp_dir).await.unwrap();
    spreadeater::runtime::hedge_live_probe::print_report(&result);
    assert!(!result.passed);
    assert!(result.meta_pass, "{:?}", result.meta_failures);
    assert!(!result.standard_pass);
    assert!(!result.trigger.success);
    assert_eq!(
        result.trigger.failure_code.as_deref(),
        Some("trigger_no_fill")
    );
    assert!(result.cleanup.clean_end_state);
    assert_eq!(result.cleanup.flatten_orders_placed, 0);
}

#[tokio::test]
async fn user_stream_order_event_without_trade_does_not_start_hedge() {
    let scenario = base_scenario();
    let market = mock_market(&scenario.market);
    let mut exchange = base_exchange();
    exchange.actions = vec![
        place_action_with_scripts_and_ws(
            "1001",
            Side::Buy,
            OrderType::GTC,
            "trigger-order",
            "invalid",
            &[],
            None,
            None,
            Vec::new(),
            Vec::new(),
            vec![ws_order_update_message(
                "trigger-order",
                &market,
                QuoteLeg::YesBid,
                Decimal::ZERO,
                0,
            )],
        ),
        cancel_action("trigger-order"),
    ];
    let server = Arc::new(MockExchangeServer::spawn(&market, &exchange).await.unwrap());
    let temp_dir = TempDir::new().unwrap();
    let result = run_probe(scenario, server, &temp_dir).await.unwrap();
    spreadeater::runtime::hedge_live_probe::print_report(&result);
    assert!(!result.passed);
    assert!(result.meta_pass, "{:?}", result.meta_failures);
    assert!(!result.standard_pass);
    assert_eq!(
        result.trigger.failure_code.as_deref(),
        Some("trigger_no_fill")
    );
    assert_eq!(result.cleanup.flatten_orders_placed, 0);
}

#[tokio::test]
async fn partial_trigger_trade_fails_and_flattens_back_to_clean() {
    let mut scenario = base_scenario();
    scenario.expected.max_planned_hedge_shares = Some(dec!(4));
    let market = mock_market(&scenario.market);
    let mut exchange = base_exchange();
    exchange.actions = vec![
        place_action_with_scripts_and_ws(
            "1001",
            Side::Buy,
            OrderType::GTC,
            "trigger-order",
            "matched",
            &["trigger-partial"],
            Some(vec![ScenarioBalanceStep {
                amount: dec!(96.20),
                delay_ms: 0,
            }]),
            Some(vec![position_step(
                dec!(5),
                Decimal::ZERO,
                dec!(0.75),
                dec!(0.5),
            )]),
            vec![order_lookup(
                "trigger-order",
                QuoteLeg::YesBid,
                dec!(0.76),
                dec!(10),
                dec!(5),
                "matched",
                &["trigger-partial"],
            )],
            vec![trade_lookup(
                "trigger-partial",
                QuoteLeg::YesBid,
                dec!(0.75),
                dec!(5),
                "trigger-order",
            )],
            vec![ws_trade_message(
                "trigger-partial",
                &market,
                QuoteLeg::YesBid,
                dec!(5),
                "trigger-order",
                0,
            )],
        ),
        place_action_with_scripts(
            "1002",
            Side::Buy,
            OrderType::GTC,
            "hedge-order",
            "live",
            &[],
            Some(vec![ScenarioBalanceStep {
                amount: dec!(94.85),
                delay_ms: 0,
            }]),
            Some(vec![position_step(
                dec!(5),
                dec!(5),
                dec!(0.75),
                dec!(0.26),
            )]),
            vec![order_lookup(
                "hedge-order",
                QuoteLeg::NoBid,
                dec!(0.27),
                dec!(5),
                dec!(5),
                "matched",
                &[],
            )],
            Vec::new(),
        ),
        cancel_action("hedge-order"),
        cancel_action("trigger-order"),
        place_action_with_scripts(
            "1001",
            Side::Sell,
            OrderType::FOK,
            "cleanup-yes",
            "matched",
            &[],
            Some(vec![ScenarioBalanceStep {
                amount: dec!(98.20),
                delay_ms: 0,
            }]),
            Some(vec![position_step(
                Decimal::ZERO,
                dec!(5),
                Decimal::ZERO,
                dec!(0.26),
            )]),
            Vec::new(),
            Vec::new(),
        ),
        place_action_with_scripts(
            "1002",
            Side::Sell,
            OrderType::FOK,
            "cleanup-no",
            "matched",
            &[],
            Some(vec![ScenarioBalanceStep {
                amount: dec!(99.85),
                delay_ms: 0,
            }]),
            Some(vec![position_step(
                Decimal::ZERO,
                Decimal::ZERO,
                Decimal::ZERO,
                Decimal::ZERO,
            )]),
            Vec::new(),
            Vec::new(),
        ),
    ];
    let server = Arc::new(MockExchangeServer::spawn(&market, &exchange).await.unwrap());
    let temp_dir = TempDir::new().unwrap();
    let result = run_probe(scenario, server, &temp_dir).await.unwrap();
    assert!(!result.passed);
    assert!(result.meta_pass, "{:?}", result.meta_failures);
    assert!(!result.standard_pass);
    assert!(!result.trigger.success);
    assert_eq!(
        result.trigger.failure_code.as_deref(),
        Some("trigger_partial_fill")
    );
    assert_eq!(result.trigger.resolved_trade_shares, dec!(5));
    assert_eq!(result.observed.result_status.as_deref(), Some("success"));
    assert!(
        result
            .standard_mismatches
            .iter()
            .any(|mismatch| mismatch.contains("trigger_partial_fill")),
        "{:?}",
        result.standard_mismatches
    );
    assert!(result.cleanup.attempted);
    assert!(result.cleanup.clean_end_state);
}

#[tokio::test]
async fn trigger_overshoot_fails_and_flattens_back_to_clean() {
    let scenario = base_scenario();
    let market = mock_market(&scenario.market);
    let mut exchange = base_exchange();
    exchange.actions = vec![
        place_action_with_scripts_and_ws(
            "1001",
            Side::Buy,
            OrderType::GTC,
            "trigger-order",
            "matched",
            &["trigger-overshoot"],
            Some(vec![ScenarioBalanceStep {
                amount: dec!(91.64),
                delay_ms: 0,
            }]),
            Some(vec![position_step(
                dec!(11),
                Decimal::ZERO,
                dec!(0.75),
                dec!(0.5),
            )]),
            vec![order_lookup(
                "trigger-order",
                QuoteLeg::YesBid,
                dec!(0.76),
                dec!(10),
                dec!(11),
                "matched",
                &["trigger-overshoot"],
            )],
            vec![trade_lookup(
                "trigger-overshoot",
                QuoteLeg::YesBid,
                dec!(0.75),
                dec!(11),
                "trigger-order",
            )],
            vec![ws_trade_message(
                "trigger-overshoot",
                &market,
                QuoteLeg::YesBid,
                dec!(11),
                "trigger-order",
                0,
            )],
        ),
        place_action_with_scripts(
            "1002",
            Side::Buy,
            OrderType::GTC,
            "hedge-order",
            "live",
            &[],
            Some(vec![ScenarioBalanceStep {
                amount: dec!(88.78),
                delay_ms: 0,
            }]),
            Some(vec![position_step(
                dec!(11),
                dec!(11),
                dec!(0.75),
                dec!(0.26),
            )]),
            vec![order_lookup(
                "hedge-order",
                QuoteLeg::NoBid,
                dec!(0.27),
                dec!(11),
                dec!(11),
                "matched",
                &[],
            )],
            Vec::new(),
        ),
        cancel_action("hedge-order"),
        cancel_action("trigger-order"),
        place_action_with_scripts(
            "1001",
            Side::Sell,
            OrderType::FOK,
            "cleanup-yes",
            "matched",
            &[],
            Some(vec![ScenarioBalanceStep {
                amount: dec!(96.31),
                delay_ms: 0,
            }]),
            Some(vec![position_step(
                Decimal::ZERO,
                dec!(11),
                Decimal::ZERO,
                dec!(0.26),
            )]),
            Vec::new(),
            Vec::new(),
        ),
        place_action_with_scripts(
            "1002",
            Side::Sell,
            OrderType::FOK,
            "cleanup-no",
            "matched",
            &[],
            Some(vec![ScenarioBalanceStep {
                amount: dec!(99.84),
                delay_ms: 0,
            }]),
            Some(vec![position_step(
                Decimal::ZERO,
                Decimal::ZERO,
                Decimal::ZERO,
                Decimal::ZERO,
            )]),
            Vec::new(),
            Vec::new(),
        ),
    ];
    let server = Arc::new(MockExchangeServer::spawn(&market, &exchange).await.unwrap());
    let temp_dir = TempDir::new().unwrap();
    let result = run_probe(scenario, server, &temp_dir).await.unwrap();
    assert!(!result.passed);
    assert!(result.meta_pass, "{:?}", result.meta_failures);
    assert!(!result.standard_pass);
    assert!(!result.trigger.success);
    assert_eq!(
        result.trigger.failure_code.as_deref(),
        Some("trigger_overshoot")
    );
    assert_eq!(result.trigger.resolved_trade_shares, dec!(11));
    assert_eq!(result.observed.result_status.as_deref(), Some("success"));
    assert!(
        result
            .standard_mismatches
            .iter()
            .any(|mismatch| mismatch.contains("trigger_overshoot")),
        "{:?}",
        result.standard_mismatches
    );
    assert!(result.cleanup.clean_end_state);
}

#[tokio::test]
async fn matched_trigger_without_trade_resolution_flattens_and_fails() {
    let scenario = base_scenario();
    let market = mock_market(&scenario.market);
    let mut exchange = base_exchange();
    exchange.actions = vec![
        place_action_with_scripts(
            "1001",
            Side::Buy,
            OrderType::GTC,
            "trigger-order",
            "matched",
            &["trigger-trade"],
            Some(vec![ScenarioBalanceStep {
                amount: dec!(92.40),
                delay_ms: 0,
            }]),
            Some(vec![position_step(
                dec!(10),
                Decimal::ZERO,
                dec!(0.75),
                dec!(0.5),
            )]),
            vec![order_lookup(
                "trigger-order",
                QuoteLeg::YesBid,
                dec!(0.76),
                dec!(10),
                dec!(10),
                "matched",
                &[],
            )],
            Vec::new(),
        ),
        cancel_action("trigger-order"),
        place_action_with_scripts(
            "1001",
            Side::Sell,
            OrderType::FOK,
            "cleanup-yes",
            "matched",
            &[],
            Some(vec![ScenarioBalanceStep {
                amount: dec!(99.85),
                delay_ms: 0,
            }]),
            Some(vec![position_step(
                Decimal::ZERO,
                Decimal::ZERO,
                Decimal::ZERO,
                Decimal::ZERO,
            )]),
            Vec::new(),
            Vec::new(),
        ),
    ];
    let server = Arc::new(MockExchangeServer::spawn(&market, &exchange).await.unwrap());
    let temp_dir = TempDir::new().unwrap();
    let result = run_probe(scenario, server, &temp_dir).await.unwrap();
    assert!(!result.passed);
    assert!(!result.meta_pass);
    assert!(!result.standard_pass);
    assert!(!result.trigger.success);
    assert_eq!(
        result.trigger.failure_code.as_deref(),
        Some("trigger_ws_not_observed")
    );
    assert_eq!(result.cleanup.status, Some(CleanupStatus::Flattened));
    assert!(result.cleanup.clean_end_state);
}

#[tokio::test]
async fn late_trigger_inventory_is_recovered_and_flattened_after_meta_fail() {
    let scenario = base_scenario();
    let market = mock_market(&scenario.market);
    let mut exchange = base_exchange();
    exchange.actions = vec![
        place_action_with_scripts(
            "1001",
            Side::Buy,
            OrderType::GTC,
            "trigger-order",
            "matched",
            &[],
            Some(vec![ScenarioBalanceStep {
                amount: dec!(92.40),
                delay_ms: 0,
            }]),
            Some(vec![
                position_step(Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO),
                position_step(Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO),
                position_step(dec!(10), Decimal::ZERO, dec!(0.75), Decimal::ZERO),
            ]),
            vec![order_lookup(
                "trigger-order",
                QuoteLeg::YesBid,
                dec!(0.76),
                dec!(10),
                dec!(10),
                "matched",
                &[],
            )],
            Vec::new(),
        ),
        cancel_action("trigger-order"),
        place_action_with_scripts(
            "1001",
            Side::Sell,
            OrderType::FOK,
            "cleanup-yes",
            "matched",
            &[],
            Some(vec![ScenarioBalanceStep {
                amount: dec!(99.85),
                delay_ms: 0,
            }]),
            Some(vec![position_step(
                Decimal::ZERO,
                Decimal::ZERO,
                Decimal::ZERO,
                Decimal::ZERO,
            )]),
            Vec::new(),
            Vec::new(),
        ),
    ];
    let server = Arc::new(MockExchangeServer::spawn(&market, &exchange).await.unwrap());
    let temp_dir = TempDir::new().unwrap();
    let result = run_probe(scenario, server, &temp_dir).await.unwrap();
    assert!(!result.meta_pass);
    assert!(!result.standard_pass);
    assert_eq!(
        result.trigger.failure_code.as_deref(),
        Some("trigger_ws_not_observed")
    );
    assert!(
        result.cleanup.success,
        "{:?}",
        result.cleanup.failure_reason
    );
    assert_eq!(result.cleanup.status, Some(CleanupStatus::Flattened));
    assert!(result.cleanup.clean_end_state);
}

#[tokio::test]
async fn numeric_order_evidence_without_visible_inventory_uses_provisional_trigger_recovery_sell() {
    let scenario = base_scenario();
    let market = mock_market(&scenario.market);
    let mut exchange = base_exchange();
    exchange.actions = vec![
        place_action_with_scripts(
            "1001",
            Side::Buy,
            OrderType::GTC,
            "trigger-order",
            "matched",
            &[],
            Some(vec![ScenarioBalanceStep {
                amount: dec!(92.40),
                delay_ms: 0,
            }]),
            Some(vec![
                position_step(Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO),
                position_step(Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO),
                position_step(Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO),
            ]),
            Vec::new(),
            Vec::new(),
        ),
        cancel_action("trigger-order"),
        place_action_with_scripts(
            "1001",
            Side::Sell,
            OrderType::FOK,
            "cleanup-yes",
            "matched",
            &[],
            Some(vec![ScenarioBalanceStep {
                amount: dec!(99.85),
                delay_ms: 0,
            }]),
            Some(vec![position_step(
                Decimal::ZERO,
                Decimal::ZERO,
                Decimal::ZERO,
                Decimal::ZERO,
            )]),
            Vec::new(),
            Vec::new(),
        ),
    ];
    set_place_taking_amount(&mut exchange.actions[0], dec!(10));
    let server = Arc::new(MockExchangeServer::spawn(&market, &exchange).await.unwrap());
    let temp_dir = TempDir::new().unwrap();
    let result = run_probe(scenario, server, &temp_dir).await.unwrap();
    assert!(!result.meta_pass);
    assert!(!result.standard_pass);
    assert_eq!(result.trigger.placement_taking_shares, Some(dec!(10)));
    assert!(
        result.cleanup.success,
        "{:?}",
        result.cleanup.failure_reason
    );
    assert_eq!(result.cleanup.status, Some(CleanupStatus::Flattened));
}

#[tokio::test]
async fn ambiguous_trigger_without_numeric_recovery_evidence_fails_cleanup_conservatively() {
    let scenario = base_scenario();
    let market = mock_market(&scenario.market);
    let mut exchange = base_exchange();
    exchange.actions = vec![
        place_action_with_scripts_and_ws(
            "1001",
            Side::Buy,
            OrderType::GTC,
            "trigger-order",
            "matched",
            &[],
            Some(vec![ScenarioBalanceStep {
                amount: dec!(92.40),
                delay_ms: 0,
            }]),
            Some(vec![
                position_step(Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO),
                position_step(Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO),
                position_step(Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO),
            ]),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        ),
        cancel_action("trigger-order"),
    ];
    let server = Arc::new(MockExchangeServer::spawn(&market, &exchange).await.unwrap());
    let temp_dir = TempDir::new().unwrap();
    let result = run_probe(scenario, server, &temp_dir).await.unwrap();
    assert!(!result.meta_pass);
    assert!(!result.standard_pass);
    assert_eq!(
        result.trigger.failure_code.as_deref(),
        Some("trigger_ws_not_observed")
    );
    assert!(!result.cleanup.success);
    assert_eq!(
        result.cleanup.failure_code.as_deref(),
        Some("cleanup_truth_unconfirmed")
    );
}

#[tokio::test]
async fn successful_paired_probe_with_merge_cleanup() {
    let mut scenario = base_scenario();
    scenario.expected.cleanup_status = Some(ExpectedCleanupStatus::Merged);
    let market = mock_market(&scenario.market);
    let mut exchange = success_exchange_flattened();
    exchange.actions.truncate(4);
    let server = Arc::new(MockExchangeServer::spawn(&market, &exchange).await.unwrap());
    let temp_dir = TempDir::new().unwrap();
    let mut config = config_for_mock(server.base_url(), &temp_dir);
    config.discovery.user_ws_url = server.user_ws_url().to_string();
    let result = run_hedge_live_probe_with_options(
        scenario,
        config,
        test_credentials(),
        LiveProbeRuntimeOptions::new_for_tests(Arc::new(SuccessfulTestMerger {
            server: Arc::clone(&server),
        })),
    )
    .await
    .unwrap();
    assert!(
        result.meta_pass && result.standard_pass,
        "meta={:?} standard={:?}",
        result.meta_failures,
        result.standard_mismatches
    );
    assert_eq!(result.cleanup.status, Some(CleanupStatus::Merged));
}

#[tokio::test]
async fn cleanup_failure_produces_failed_result() {
    let scenario = base_scenario();
    let market = mock_market(&scenario.market);
    let mut exchange = success_exchange_flattened();
    exchange.actions.truncate(4);
    let server = Arc::new(MockExchangeServer::spawn(&market, &exchange).await.unwrap());
    let temp_dir = TempDir::new().unwrap();
    let result = run_probe(scenario, server, &temp_dir).await.unwrap();
    assert!(result.meta_pass, "{:?}", result.meta_failures);
    assert!(result.standard_pass, "{:?}", result.standard_mismatches);
    assert!(result.passed);
    assert!(!result.cleanup.success);
}

#[tokio::test]
async fn bounded_expectation_mismatch_returns_failed_result() {
    let mut scenario = base_scenario();
    scenario.expected.max_planned_hedge_shares = Some(dec!(1));
    let market = mock_market(&scenario.market);
    let server = Arc::new(
        MockExchangeServer::spawn(&market, &success_exchange_flattened())
            .await
            .unwrap(),
    );
    let temp_dir = TempDir::new().unwrap();
    let result = run_probe(scenario, server, &temp_dir).await.unwrap();
    assert!(!result.passed);
    assert!(result
        .standard_mismatches
        .iter()
        .any(|mismatch| mismatch.contains("planned_hedge_shares")));
}

#[tokio::test]
async fn hedge_live_probe_cli_returns_non_zero_for_safety_failure() {
    let mut scenario = base_scenario();
    scenario.trigger.max_trigger_limit_price = dec!(0.75);
    let market = mock_market(&scenario.market);
    let server = Arc::new(
        MockExchangeServer::spawn(&market, &success_exchange_flattened())
            .await
            .unwrap(),
    );
    let temp_dir = TempDir::new().unwrap();
    let mut config = config_for_mock(server.base_url(), &temp_dir);
    config.discovery.user_ws_url = server.user_ws_url().to_string();
    let config_file = write_temp_json(&config);
    let scenario_file = write_temp_json(&scenario);
    let mut command = Command::new(env!("CARGO_BIN_EXE_spreadeater"));
    command
        .args([
            "--config",
            &config_file.path().to_string_lossy(),
            "hedge-live-probe",
            "--scenario",
            &scenario_file.path().to_string_lossy(),
        ])
        .current_dir(temp_dir.path())
        .env(LIVE_PROBE_ARM_ENV, LIVE_PROBE_ARM_TOKEN);
    apply_test_credentials(&mut command);
    assert_eq!(run_command_status(command).await.code(), Some(1));
}

#[tokio::test]
async fn hedge_live_probe_cli_returns_non_zero_for_cleanup_failure() {
    let scenario = base_scenario();
    let market = mock_market(&scenario.market);
    let mut exchange = success_exchange_flattened();
    exchange.actions.truncate(3);
    let server = Arc::new(MockExchangeServer::spawn(&market, &exchange).await.unwrap());
    let temp_dir = TempDir::new().unwrap();
    let mut config = config_for_mock(server.base_url(), &temp_dir);
    config.discovery.user_ws_url = server.user_ws_url().to_string();
    let config_file = write_temp_json(&config);
    let scenario_file = write_temp_json(&scenario);
    let mut command = Command::new(env!("CARGO_BIN_EXE_spreadeater"));
    command
        .args([
            "--config",
            &config_file.path().to_string_lossy(),
            "hedge-live-probe",
            "--scenario",
            &scenario_file.path().to_string_lossy(),
        ])
        .current_dir(temp_dir.path())
        .env(LIVE_PROBE_ARM_ENV, LIVE_PROBE_ARM_TOKEN);
    apply_test_credentials(&mut command);
    assert_eq!(run_command_status(command).await.code(), Some(1));
}
