use super::*;

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, bail, Context, Result};
use base64::Engine;
use chrono::Utc;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use spreadeater_core::payloads::{
    HedgeExitPathPayload, HedgeIntentPayload, HedgeResultPayload, NeutralityPayload,
};
use spreadeater_core::{
    EventEnvelope, EventProducer, EventType, ProducerError, QueueDepthSnapshot,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, Mutex as AsyncMutex};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};
use uuid::Uuid;

use crate::trading::ctf_merge::PairMerger;

// Canonical public Hardhat/Anvil test account. Never use this key with real funds.
pub(super) const TEST_PRIVATE_KEY: &str =
    "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
pub(super) const TEST_ADDRESS: &str = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266";

pub(super) fn fixture_path(dir: &str, name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join(dir)
        .join(name)
}

pub(super) fn serialize_optional_decimal<S>(
    value: &Option<Decimal>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match value {
        Some(d) => serializer.serialize_str(&d.to_string()),
        None => serializer.serialize_none(),
    }
}

pub(super) fn deserialize_optional_decimal<'de, D>(
    deserializer: D,
) -> Result<Option<Decimal>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let opt: Option<String> = Option::deserialize(deserializer)?;
    match opt {
        Some(s) => s
            .parse::<Decimal>()
            .map(Some)
            .map_err(serde::de::Error::custom),
        None => Ok(None),
    }
}

fn default_avg_price() -> Decimal {
    Decimal::new(5, 1)
}

fn default_fee_rate_bps() -> u64 {
    0
}

fn default_order_status() -> String {
    "live".to_string()
}

fn default_order_type_name() -> String {
    "GTC".to_string()
}

fn default_cancel_outcome() -> String {
    "confirmed".to_string()
}

fn default_place_status() -> String {
    "live".to_string()
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct HedgeScenario {
    pub name: String,
    pub description: String,
    pub market: ScenarioMarket,
    pub trigger: ScenarioTrigger,
    pub exchange: ScenarioExchange,
    pub expected: ScenarioExpected,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ScenarioMarket {
    pub condition_id: String,
    pub question: String,
    pub yes_token_id: String,
    pub no_token_id: String,
    #[serde(with = "rust_decimal::serde::str")]
    pub daily_reward_total: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub max_spread: Decimal,
    pub tick_size: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ScenarioTrigger {
    pub work_item: ScenarioWorkItem,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ScenarioWorkItem {
    pub tracked_order: ScenarioTrackedOrder,
    pub trade: ScenarioTrade,
    #[serde(default)]
    pub anchored_order_id: Option<String>,
    pub match_source: String,
    pub fallback_match: bool,
    #[serde(with = "rust_decimal::serde::str")]
    pub size_to_apply: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub hedge_size: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ScenarioTrackedOrder {
    pub order_id: String,
    #[serde(default)]
    pub trace_id: Option<String>,
    pub leg: QuoteLeg,
    #[serde(with = "rust_decimal::serde::str")]
    pub price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub size: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub matched_size: Decimal,
    #[serde(default)]
    pub created_at_unix: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ScenarioTrade {
    pub trade_id: String,
    pub side: Side,
    #[serde(with = "rust_decimal::serde::str")]
    pub price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub size: Decimal,
    #[serde(default)]
    pub maker_order_id: Option<String>,
    #[serde(default)]
    pub taker_order_id: Option<String>,
    #[serde(default)]
    pub condition_id: Option<String>,
    #[serde(default)]
    pub asset_id: Option<String>,
    #[serde(default)]
    pub outcome: Option<String>,
    #[serde(default)]
    pub timestamp_unix: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ScenarioExchange {
    pub books: ScenarioExchangeBooks,
    #[serde(default = "default_fee_rate_bps")]
    pub default_fee_rate_bps: u64,
    #[serde(default)]
    pub fee_rate_bps: HashMap<String, u64>,
    #[serde(default)]
    pub balances: Vec<ScenarioBalanceStep>,
    #[serde(default)]
    pub positions: Vec<ScenarioPositionStep>,
    #[serde(default)]
    pub global_open_orders: Vec<ScenarioOpenOrdersStep>,
    #[serde(default)]
    pub market_open_orders: Vec<ScenarioOpenOrdersStep>,
    #[serde(default)]
    pub order_lookup: Vec<ScenarioOrderLookupScript>,
    #[serde(default)]
    pub merge: Option<ScenarioMergeBehavior>,
    #[serde(default)]
    pub actions: Vec<ScenarioExchangeAction>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ScenarioMergeBehavior {
    #[serde(default = "default_true")]
    pub configured: bool,
    #[serde(default = "default_true")]
    pub succeed: bool,
    #[serde(default)]
    pub tx_hash: Option<String>,
    #[serde(default)]
    pub error: Option<String>,
}

impl Default for ScenarioMergeBehavior {
    fn default() -> Self {
        Self {
            configured: true,
            succeed: true,
            tx_hash: Some("0xmerge".to_string()),
            error: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ScenarioExchangeBooks {
    pub yes: ScenarioBook,
    pub no: ScenarioBook,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ScenarioBook {
    pub bids: Vec<ScenarioPriceLevel>,
    pub asks: Vec<ScenarioPriceLevel>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ScenarioPriceLevel(
    #[serde(with = "rust_decimal::serde::str")] pub Decimal,
    #[serde(with = "rust_decimal::serde::str")] pub Decimal,
);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ScenarioBalanceStep {
    #[serde(with = "rust_decimal::serde::str")]
    pub amount: Decimal,
    #[serde(default)]
    pub delay_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ScenarioPositionStep {
    #[serde(with = "rust_decimal::serde::str")]
    pub yes_size: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub no_size: Decimal,
    #[serde(default = "default_avg_price", with = "rust_decimal::serde::str")]
    pub yes_avg_price: Decimal,
    #[serde(default = "default_avg_price", with = "rust_decimal::serde::str")]
    pub no_avg_price: Decimal,
    #[serde(default)]
    pub delay_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ScenarioOpenOrdersStep {
    #[serde(default)]
    pub orders: Vec<ScenarioLiveOrder>,
    #[serde(default)]
    pub delay_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ScenarioOrderLookupScript {
    pub order_id: String,
    #[serde(default)]
    pub responses: Vec<ScenarioOrderLookupStep>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ScenarioOrderLookupStep {
    #[serde(default)]
    pub order: Option<ScenarioLiveOrder>,
    #[serde(default)]
    pub delay_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ScenarioLiveOrder {
    pub id: String,
    pub leg: QuoteLeg,
    #[serde(with = "rust_decimal::serde::str")]
    pub price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub original_size: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub size_matched: Decimal,
    #[serde(default = "default_order_status")]
    pub status: String,
    #[serde(default = "default_order_type_name")]
    pub order_type: String,
    #[serde(default)]
    pub created_at_unix: Option<i64>,
    #[serde(default)]
    pub associated_trade_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(super) enum ScenarioExchangeAction {
    Place {
        #[serde(default)]
        expected_token_id: Option<String>,
        #[serde(default)]
        expected_side: Option<Side>,
        #[serde(default)]
        expected_order_type: Option<OrderType>,
        response: ScenarioPlacedOrderResponse,
        #[serde(default)]
        mutations: ScenarioExchangeMutations,
    },
    Cancel {
        expected_order_id: String,
        #[serde(default)]
        response: ScenarioCancelActionResponse,
        #[serde(default)]
        mutations: ScenarioExchangeMutations,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ScenarioPlacedOrderResponse {
    pub order_id: String,
    #[serde(default = "default_place_status")]
    pub status: String,
    #[serde(default)]
    pub trade_ids: Vec<String>,
    #[serde(default)]
    pub transaction_hashes: Vec<String>,
    #[serde(default)]
    pub taking_amount: Option<String>,
    #[serde(default)]
    pub making_amount: Option<String>,
    #[serde(default)]
    pub delay_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ScenarioCancelActionResponse {
    #[serde(default = "default_cancel_outcome")]
    pub outcome: String,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub delay_ms: u64,
}

impl Default for ScenarioCancelActionResponse {
    fn default() -> Self {
        Self {
            outcome: default_cancel_outcome(),
            reason: None,
            delay_ms: 0,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(super) struct ScenarioExchangeMutations {
    #[serde(default)]
    pub replace_balances: Option<Vec<ScenarioBalanceStep>>,
    #[serde(default)]
    pub replace_positions: Option<Vec<ScenarioPositionStep>>,
    #[serde(default)]
    pub replace_global_open_orders: Option<Vec<ScenarioOpenOrdersStep>>,
    #[serde(default)]
    pub replace_market_open_orders: Option<Vec<ScenarioOpenOrdersStep>>,
    #[serde(default)]
    pub replace_order_lookup: Vec<ScenarioOrderLookupScript>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ScenarioExpected {
    pub success: bool,
    pub halted: bool,
    #[serde(default)]
    pub hedge_side: Option<Side>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_decimal",
        serialize_with = "serialize_optional_decimal"
    )]
    pub planned_hedge_shares: Option<Decimal>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_decimal",
        serialize_with = "serialize_optional_decimal"
    )]
    pub planned_sellback_shares: Option<Decimal>,
    #[serde(default)]
    pub result_status: Option<String>,
    #[serde(default)]
    pub hedge_leg_status: Option<String>,
    #[serde(default)]
    pub sellback_leg_status: Option<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_decimal",
        serialize_with = "serialize_optional_decimal"
    )]
    pub hedge_price: Option<Decimal>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_decimal",
        serialize_with = "serialize_optional_decimal"
    )]
    pub sellback_price: Option<Decimal>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_decimal",
        serialize_with = "serialize_optional_decimal"
    )]
    pub post_sync_yes_size: Option<Decimal>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_decimal",
        serialize_with = "serialize_optional_decimal"
    )]
    pub post_sync_no_size: Option<Decimal>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_decimal",
        serialize_with = "serialize_optional_decimal"
    )]
    pub post_sync_net_exposure: Option<Decimal>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_decimal",
        serialize_with = "serialize_optional_decimal"
    )]
    pub neutrality_residual_exposure: Option<Decimal>,
    #[serde(default)]
    pub exit_path_status: Option<String>,
    #[serde(default)]
    pub ctf_merge_configured: Option<bool>,
    #[serde(default)]
    pub merge_attempted: Option<bool>,
    #[serde(default)]
    pub merge_tx_hash: Option<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_decimal",
        serialize_with = "serialize_optional_decimal"
    )]
    pub merge_eligible_pairs: Option<Decimal>,
    #[serde(default)]
    pub fallback_ask_count: Option<u64>,
}

#[derive(Debug, Clone)]
pub(super) struct MockRequestRecord {
    pub method: String,
    pub path: String,
    pub body: Option<String>,
}

#[derive(Debug, Clone)]
pub(super) struct ObservedHedgeOutcome {
    pub result_status: Option<String>,
    pub hedge_side: Option<Side>,
    pub planned_hedge_shares: Option<Decimal>,
    pub planned_sellback_shares: Option<Decimal>,
    pub hedge_leg_status: Option<String>,
    pub sellback_leg_status: Option<String>,
    pub hedge_price: Option<Decimal>,
    pub sellback_price: Option<Decimal>,
    pub post_sync_yes_size: Option<Decimal>,
    pub post_sync_no_size: Option<Decimal>,
    pub post_sync_net_exposure: Option<Decimal>,
    pub neutrality_residual_exposure: Option<Decimal>,
    pub exit_path_status: Option<String>,
    pub ctf_merge_configured: Option<bool>,
    pub merge_attempted: Option<bool>,
    pub merge_tx_hash: Option<String>,
    pub merge_eligible_pairs: Option<Decimal>,
    pub fallback_ask_count: Option<u64>,
    pub halted: bool,
    pub request_log: Vec<MockRequestRecord>,
}

pub(super) struct ScenarioPairMerger {
    pub behavior: ScenarioMergeBehavior,
}

#[async_trait::async_trait]
impl PairMerger for ScenarioPairMerger {
    async fn preflight_check(&self) -> Result<()> {
        Ok(())
    }

    async fn merge_positions(
        &self,
        _condition_id: &str,
        _amount: u64,
        _neg_risk: bool,
    ) -> Result<String> {
        if self.behavior.succeed {
            Ok(self
                .behavior
                .tx_hash
                .clone()
                .unwrap_or_else(|| "0xmerge".to_string()))
        } else {
            bail!(
                "{}",
                self.behavior
                    .error
                    .clone()
                    .unwrap_or_else(|| "mock merge failed".to_string())
            );
        }
    }
}

#[derive(Default)]
pub(super) struct InMemoryEventCollector {
    events: Mutex<Vec<EventEnvelope>>,
}

impl InMemoryEventCollector {
    pub(super) fn events(&self) -> Vec<EventEnvelope> {
        self.events
            .lock()
            .expect("event collector poisoned")
            .clone()
    }
}

impl EventProducer for InMemoryEventCollector {
    fn emit(&self, event: EventEnvelope) -> std::result::Result<bool, ProducerError> {
        self.events
            .lock()
            .expect("event collector poisoned")
            .push(event);
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

pub(super) struct FanoutEventProducer {
    primary: Arc<dyn EventProducer>,
    secondary: Arc<dyn EventProducer>,
}

impl FanoutEventProducer {
    pub(super) fn new(
        primary: Arc<dyn EventProducer>,
        secondary: Arc<dyn EventProducer>,
    ) -> Self {
        Self { primary, secondary }
    }
}

impl EventProducer for FanoutEventProducer {
    fn emit(&self, event: EventEnvelope) -> std::result::Result<bool, ProducerError> {
        let primary = self.primary.emit(event.clone())?;
        let secondary = self.secondary.emit(event)?;
        Ok(primary && secondary)
    }

    fn queue_depth(&self) -> QueueDepthSnapshot {
        let primary = self.primary.queue_depth();
        let secondary = self.secondary.queue_depth();
        QueueDepthSnapshot {
            critical: primary.critical.max(secondary.critical),
            normal: primary.normal.max(secondary.normal),
        }
    }

    fn is_degraded(&self) -> bool {
        self.primary.is_degraded() || self.secondary.is_degraded()
    }
}

pub(super) struct MockExchangeServer {
    base_url: String,
    state: Arc<AsyncMutex<MockExchangeState>>,
    http_task: JoinHandle<()>,
}

impl MockExchangeServer {
    pub(super) async fn spawn(
        market: &ScenarioMarket,
        exchange: &ScenarioExchange,
    ) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .context("Failed to bind mock exchange server")?;
        let addr = listener
            .local_addr()
            .context("Failed to read mock server addr")?;
        let state = Arc::new(AsyncMutex::new(MockExchangeState::new(market, exchange)));
        let state_for_http = Arc::clone(&state);

        let http_task = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let state = Arc::clone(&state_for_http);
                tokio::spawn(async move {
                    let response = match read_http_request(&mut socket).await {
                        Ok(request) => handle_mock_request(state, request).await,
                        Err(err) => (
                            "400 Bad Request".to_string(),
                            json!({ "error": err.to_string() }).to_string(),
                        ),
                    };
                    let response = format!(
                        "HTTP/1.1 {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        response.0,
                        response.1.len(),
                        response.1,
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                    let _ = socket.shutdown().await;
                });
            }
        });

        Ok(Self {
            base_url: format!("http://{}", addr),
            state,
            http_task,
        })
    }

    pub(super) fn base_url(&self) -> &str {
        &self.base_url
    }

    pub(super) async fn request_log(&self) -> Vec<MockRequestRecord> {
        self.state.lock().await.request_log.clone()
    }

    pub(super) async fn prepend_market_open_orders(&self, step: ScenarioOpenOrdersStep) {
        self.state.lock().await.market_open_orders.push_front(step);
    }
}

impl Drop for MockExchangeServer {
    fn drop(&mut self) {
        self.http_task.abort();
    }
}

#[derive(Debug)]
struct MockHttpRequest {
    method: String,
    path: String,
    body: String,
}

#[derive(Debug)]
struct MockExchangeState {
    market: ScenarioMarket,
    books: ScenarioExchangeBooks,
    default_fee_rate_bps: u64,
    fee_rate_bps: HashMap<String, u64>,
    balances: VecDeque<ScenarioBalanceStep>,
    positions: VecDeque<ScenarioPositionStep>,
    global_open_orders: VecDeque<ScenarioOpenOrdersStep>,
    market_open_orders: VecDeque<ScenarioOpenOrdersStep>,
    order_lookup: HashMap<String, VecDeque<ScenarioOrderLookupStep>>,
    actions: VecDeque<ScenarioExchangeAction>,
    request_log: Vec<MockRequestRecord>,
}

impl MockExchangeState {
    fn new(market: &ScenarioMarket, exchange: &ScenarioExchange) -> Self {
        Self {
            market: market.clone(),
            books: exchange.books.clone(),
            default_fee_rate_bps: exchange.default_fee_rate_bps,
            fee_rate_bps: exchange.fee_rate_bps.clone(),
            balances: queue_or_default(
                exchange.balances.clone(),
                ScenarioBalanceStep {
                    amount: Decimal::ZERO,
                    delay_ms: 0,
                },
            ),
            positions: queue_or_default(
                exchange.positions.clone(),
                ScenarioPositionStep {
                    yes_size: Decimal::ZERO,
                    no_size: Decimal::ZERO,
                    yes_avg_price: default_avg_price(),
                    no_avg_price: default_avg_price(),
                    delay_ms: 0,
                },
            ),
            global_open_orders: queue_or_default(
                exchange.global_open_orders.clone(),
                ScenarioOpenOrdersStep {
                    orders: Vec::new(),
                    delay_ms: 0,
                },
            ),
            market_open_orders: queue_or_default(
                exchange.market_open_orders.clone(),
                ScenarioOpenOrdersStep {
                    orders: Vec::new(),
                    delay_ms: 0,
                },
            ),
            order_lookup: exchange
                .order_lookup
                .iter()
                .map(|script| {
                    (
                        script.order_id.clone(),
                        queue_or_default(
                            script.responses.clone(),
                            ScenarioOrderLookupStep {
                                order: None,
                                delay_ms: 0,
                            },
                        ),
                    )
                })
                .collect(),
            actions: VecDeque::from(exchange.actions.clone()),
            request_log: Vec::new(),
        }
    }

    fn apply_mutations(&mut self, mutations: &ScenarioExchangeMutations) {
        if let Some(steps) = &mutations.replace_balances {
            self.balances = queue_or_default(
                steps.clone(),
                ScenarioBalanceStep {
                    amount: Decimal::ZERO,
                    delay_ms: 0,
                },
            );
        }
        if let Some(steps) = &mutations.replace_positions {
            self.positions = queue_or_default(
                steps.clone(),
                ScenarioPositionStep {
                    yes_size: Decimal::ZERO,
                    no_size: Decimal::ZERO,
                    yes_avg_price: default_avg_price(),
                    no_avg_price: default_avg_price(),
                    delay_ms: 0,
                },
            );
        }
        if let Some(steps) = &mutations.replace_global_open_orders {
            self.global_open_orders = queue_or_default(
                steps.clone(),
                ScenarioOpenOrdersStep {
                    orders: Vec::new(),
                    delay_ms: 0,
                },
            );
        }
        if let Some(steps) = &mutations.replace_market_open_orders {
            self.market_open_orders = queue_or_default(
                steps.clone(),
                ScenarioOpenOrdersStep {
                    orders: Vec::new(),
                    delay_ms: 0,
                },
            );
        }
        for script in &mutations.replace_order_lookup {
            self.order_lookup.insert(
                script.order_id.clone(),
                queue_or_default(
                    script.responses.clone(),
                    ScenarioOrderLookupStep {
                        order: None,
                        delay_ms: 0,
                    },
                ),
            );
        }
    }
}

async fn read_http_request(socket: &mut tokio::net::TcpStream) -> Result<MockHttpRequest> {
    let mut raw = Vec::new();
    let mut header_end = None;
    let mut content_length = 0usize;
    let mut buf = [0u8; 2048];

    loop {
        let read = socket
            .read(&mut buf)
            .await
            .context("Failed to read mock request")?;
        if read == 0 {
            break;
        }
        raw.extend_from_slice(&buf[..read]);

        if header_end.is_none() {
            if let Some(pos) = raw.windows(4).position(|window| window == b"\r\n\r\n") {
                header_end = Some(pos + 4);
                let header_str = String::from_utf8_lossy(&raw[..pos + 4]);
                content_length = parse_content_length(&header_str);
            }
        }

        if let Some(end) = header_end {
            if raw.len() >= end + content_length {
                break;
            }
        }
    }

    let request = String::from_utf8(raw).context("Mock request was not valid UTF-8")?;
    let (head, body) = request
        .split_once("\r\n\r\n")
        .map(|(head, body)| (head, body.to_string()))
        .unwrap_or_else(|| (request.as_str(), String::new()));
    let line = head
        .lines()
        .next()
        .ok_or_else(|| anyhow!("Mock request missing request line"))?;
    let mut parts = line.split_whitespace();
    let method = parts
        .next()
        .ok_or_else(|| anyhow!("Mock request missing method"))?
        .to_string();
    let path = parts
        .next()
        .ok_or_else(|| anyhow!("Mock request missing path"))?
        .to_string();

    Ok(MockHttpRequest { method, path, body })
}

fn parse_content_length(headers: &str) -> usize {
    headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            if name.eq_ignore_ascii_case("content-length") {
                value.trim().parse::<usize>().ok()
            } else {
                None
            }
        })
        .unwrap_or(0)
}

async fn handle_mock_request(
    state: Arc<AsyncMutex<MockExchangeState>>,
    request: MockHttpRequest,
) -> (String, String) {
    let mut state = state.lock().await;
    state.request_log.push(MockRequestRecord {
        method: request.method.clone(),
        path: request.path.clone(),
        body: if request.body.is_empty() {
            None
        } else {
            Some(request.body.clone())
        },
    });

    let response = if request.method == "GET" && request.path.starts_with("/fee-rate") {
        let token_id = query_param(&request.path, "token_id").unwrap_or_default();
        let fee = state
            .fee_rate_bps
            .get(&token_id)
            .copied()
            .unwrap_or(state.default_fee_rate_bps);
        (
            "200 OK".to_string(),
            json!({ "base_fee": fee }).to_string(),
            0,
        )
    } else if request.method == "GET"
        && (request.path.starts_with("/balance-allowance")
            || request.path.starts_with("/neg-risk/balance-allowance"))
    {
        let step = next_scripted_step(
            &mut state.balances,
            ScenarioBalanceStep {
                amount: Decimal::ZERO,
                delay_ms: 0,
            },
        );
        (
            "200 OK".to_string(),
            balance_step_to_json(&step).to_string(),
            step.delay_ms,
        )
    } else if request.method == "GET" && request.path.starts_with("/positions") {
        let step = next_scripted_step(
            &mut state.positions,
            ScenarioPositionStep {
                yes_size: Decimal::ZERO,
                no_size: Decimal::ZERO,
                yes_avg_price: default_avg_price(),
                no_avg_price: default_avg_price(),
                delay_ms: 0,
            },
        );
        (
            "200 OK".to_string(),
            position_step_to_json(&state.market.condition_id, &step).to_string(),
            step.delay_ms,
        )
    } else if request.method == "GET" && request.path.starts_with("/data/orders") {
        let step = if query_param(&request.path, "market").is_some() {
            next_scripted_step(
                &mut state.market_open_orders,
                ScenarioOpenOrdersStep {
                    orders: Vec::new(),
                    delay_ms: 0,
                },
            )
        } else {
            next_scripted_step(
                &mut state.global_open_orders,
                ScenarioOpenOrdersStep {
                    orders: Vec::new(),
                    delay_ms: 0,
                },
            )
        };
        (
            "200 OK".to_string(),
            open_orders_step_to_json(&state.market, &step).to_string(),
            step.delay_ms,
        )
    } else if request.method == "GET" && request.path.starts_with("/data/order/") {
        let order_id = request.path.trim_start_matches("/data/order/").to_string();
        let Some(queue) = state.order_lookup.get_mut(&order_id) else {
            return (
                "404 Not Found".to_string(),
                json!({ "error": "missing_order_lookup" }).to_string(),
            );
        };
        let step = next_scripted_step(
            queue,
            ScenarioOrderLookupStep {
                order: None,
                delay_ms: 0,
            },
        );
        let status = if step.order.is_some() {
            "200 OK".to_string()
        } else {
            "404 Not Found".to_string()
        };
        let body = step
            .order
            .as_ref()
            .map(|order| live_order_to_json(&state.market, order))
            .unwrap_or_else(|| json!({ "error": "missing" }));
        (status, body.to_string(), step.delay_ms)
    } else if request.method == "GET" && request.path.starts_with("/book") {
        let token_id = query_param(&request.path, "token_id").unwrap_or_default();
        if token_id == state.market.yes_token_id {
            (
                "200 OK".to_string(),
                scenario_book_to_rest_json(&state.market.yes_token_id, &state.books.yes),
                0,
            )
        } else if token_id == state.market.no_token_id {
            (
                "200 OK".to_string(),
                scenario_book_to_rest_json(&state.market.no_token_id, &state.books.no),
                0,
            )
        } else {
            (
                "404 Not Found".to_string(),
                json!({ "error": "unknown_token" }).to_string(),
                0,
            )
        }
    } else if request.method == "POST" && request.path == "/order" {
        match consume_place_action(&mut state, &request.body) {
            Ok((status, body, delay_ms)) => (status, body, delay_ms),
            Err(err) => (
                "500 Internal Server Error".to_string(),
                json!({ "error": err.to_string() }).to_string(),
                0,
            ),
        }
    } else if request.method == "DELETE" && request.path == "/order" {
        match consume_cancel_action(&mut state, &request.body) {
            Ok((status, body, delay_ms)) => (status, body, delay_ms),
            Err(err) => (
                "500 Internal Server Error".to_string(),
                json!({ "error": err.to_string() }).to_string(),
                0,
            ),
        }
    } else {
        (
            "404 Not Found".to_string(),
            json!({ "error": "unhandled_route" }).to_string(),
            0,
        )
    };

    if response.2 > 0 {
        sleep(Duration::from_millis(response.2)).await;
    }

    (response.0, response.1)
}

fn consume_place_action(
    state: &mut MockExchangeState,
    body: &str,
) -> Result<(String, String, u64)> {
    let payload: Value = serde_json::from_str(body).context("Failed to parse order body")?;
    let actual_token_id = payload
        .get("order")
        .and_then(|order| order.get("tokenId"))
        .and_then(Value::as_str)
        .map(str::to_string);
    let actual_side = payload
        .get("order")
        .and_then(|order| order.get("side"))
        .and_then(Value::as_str)
        .and_then(parse_side);
    let actual_order_type = payload
        .get("orderType")
        .and_then(Value::as_str)
        .and_then(parse_order_type);

    let Some(action) = state.actions.pop_front() else {
        bail!("POST /order had no scripted action");
    };

    let ScenarioExchangeAction::Place {
        expected_token_id,
        expected_side,
        expected_order_type,
        response,
        mutations,
    } = action
    else {
        bail!("Expected next scripted action to be place");
    };

    if expected_token_id.as_ref() != actual_token_id.as_ref() {
        bail!(
            "Unexpected POST /order token_id: expected {:?}, got {:?}",
            expected_token_id,
            actual_token_id
        );
    }
    if expected_side != actual_side {
        bail!(
            "Unexpected POST /order side: expected {:?}, got {:?}",
            expected_side,
            actual_side
        );
    }
    if expected_order_type != actual_order_type {
        bail!(
            "Unexpected POST /order type: expected {:?}, got {:?}",
            expected_order_type,
            actual_order_type
        );
    }

    state.apply_mutations(&mutations);

    Ok((
        "200 OK".to_string(),
        json!({
            "orderID": response.order_id,
            "status": response.status,
            "tradeIDs": response.trade_ids,
            "transactionsHashes": response.transaction_hashes,
            "takingAmount": response.taking_amount,
            "makingAmount": response.making_amount,
        })
        .to_string(),
        response.delay_ms,
    ))
}

fn consume_cancel_action(
    state: &mut MockExchangeState,
    body: &str,
) -> Result<(String, String, u64)> {
    let payload: Value = serde_json::from_str(body).context("Failed to parse cancel body")?;
    let actual_order_id = payload
        .get("orderID")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("Cancel body missing orderID"))?
        .to_string();

    let Some(action) = state.actions.pop_front() else {
        bail!("DELETE /order had no scripted action");
    };

    let ScenarioExchangeAction::Cancel {
        expected_order_id,
        response,
        mutations,
    } = action
    else {
        bail!("Expected next scripted action to be cancel");
    };

    if expected_order_id != actual_order_id {
        bail!(
            "Unexpected DELETE /order order_id: expected {}, got {}",
            expected_order_id,
            actual_order_id
        );
    }

    state.apply_mutations(&mutations);

    let body = match response.outcome.as_str() {
        "confirmed" => json!({ "canceled": [actual_order_id] }),
        "rejected" => json!({
            "not_canceled": {
                actual_order_id: {
                    "reason": response.reason.unwrap_or_else(|| "cancel_rejected".to_string())
                }
            }
        }),
        "ambiguous" => json!({ "status": "accepted" }),
        other => bail!("Unsupported cancel outcome: {}", other),
    };

    Ok(("200 OK".to_string(), body.to_string(), response.delay_ms))
}

fn queue_or_default<T: Clone>(steps: Vec<T>, fallback: T) -> VecDeque<T> {
    if steps.is_empty() {
        VecDeque::from([fallback])
    } else {
        VecDeque::from(steps)
    }
}

fn next_scripted_step<T: Clone>(queue: &mut VecDeque<T>, fallback: T) -> T {
    match queue.len() {
        0 => fallback,
        1 => queue.front().cloned().unwrap_or(fallback),
        _ => queue.pop_front().unwrap_or(fallback),
    }
}

fn query_param(path: &str, key: &str) -> Option<String> {
    let query = path.split('?').nth(1)?;
    for pair in query.split('&') {
        let mut parts = pair.splitn(2, '=');
        if parts.next()? == key {
            return Some(parts.next().unwrap_or_default().to_string());
        }
    }
    None
}

pub(super) fn build_test_credentials() -> ApiCredentials {
    ApiCredentials {
        api_key: "hedge-test-key".to_string(),
        secret: base64::engine::general_purpose::STANDARD.encode(b"test-secret-key!!"),
        passphrase: "hedge-test-pass".to_string(),
        address: TEST_ADDRESS.to_string(),
        private_key: Some(TEST_PRIVATE_KEY.to_string()),
        funder: None,
    }
}

pub(super) fn validate_scenario_market(market: &ScenarioMarket) -> Result<()> {
    validate_token_id_uint256_str(&market.yes_token_id).context(
        "Scenario market.yes_token_id must be a valid uint256 decimal string for order signing",
    )?;
    validate_token_id_uint256_str(&market.no_token_id).context(
        "Scenario market.no_token_id must be a valid uint256 decimal string for order signing",
    )?;
    Ok(())
}

pub(super) fn build_canonical_market(market: &ScenarioMarket) -> CanonicalMarket {
    CanonicalMarket {
        condition_id: market.condition_id.clone(),
        market_slug: format!("{}-slug", market.condition_id),
        question: market.question.clone(),
        yes_token_id: market.yes_token_id.clone(),
        no_token_id: market.no_token_id.clone(),
        reward_config: crate::models::RewardConfig {
            condition_id: market.condition_id.clone(),
            daily_reward_rates: vec![market.daily_reward_total],
            daily_reward_total: market.daily_reward_total,
            min_size: Decimal::from(20),
            max_spread: market.max_spread,
        },
        neg_risk: false,
        tick_size: market.tick_size.clone(),
        end_date: None,
        admitted_at: Utc::now(),
        status: crate::models::MarketStatus::Admitted,
    }
}

pub(super) fn scenario_book_to_snapshot(token_id: &str, book: &ScenarioBook) -> OrderBookSnapshot {
    OrderBookSnapshot {
        token_id: token_id.to_string(),
        exchange_ts: None,
        ingest_ts: Utc::now(),
        bids: book
            .bids
            .iter()
            .map(|level| crate::models::PriceLevel {
                price: level.0,
                size: level.1,
            })
            .collect(),
        asks: book
            .asks
            .iter()
            .map(|level| crate::models::PriceLevel {
                price: level.0,
                size: level.1,
            })
            .collect(),
    }
}

fn scenario_book_to_rest_json(token_id: &str, book: &ScenarioBook) -> String {
    let bids: Vec<Value> = book
        .bids
        .iter()
        .map(|level| json!({ "price": level.0.to_string(), "size": level.1.to_string() }))
        .collect();
    let asks: Vec<Value> = book
        .asks
        .iter()
        .map(|level| json!({ "price": level.0.to_string(), "size": level.1.to_string() }))
        .collect();

    json!({
        "market": token_id,
        "asset_id": token_id,
        "bids": bids,
        "asks": asks,
        "timestamp": Utc::now().to_rfc3339(),
        "hash": "",
    })
    .to_string()
}

pub(super) fn build_fill_work_item_from_scenario(scenario: &HedgeScenario) -> FillWorkItem {
    let work_item = &scenario.trigger.work_item;
    let tracked_order = &work_item.tracked_order;
    let trade = &work_item.trade;

    let tracked = scenario_tracked_order_to_tracked(
        &scenario.market,
        tracked_order,
        Some(format!("hedge-test-trace-{}", Uuid::new_v4())),
    );
    let trade = scenario_trade_to_trade_event(&scenario.market, trade, Some(tracked.leg))
        .expect("scenario trade should build");

    FillWorkItem {
        tracked,
        trade,
        anchored_order_id: work_item.anchored_order_id.clone(),
        match_source: work_item.match_source.clone(),
        fallback_match: work_item.fallback_match,
        size_to_apply: work_item.size_to_apply,
        hedge_size: work_item.hedge_size,
    }
}

pub(super) fn scenario_tracked_order_to_tracked(
    market: &ScenarioMarket,
    tracked_order: &ScenarioTrackedOrder,
    default_trace_id: Option<String>,
) -> TrackedOrder {
    let created_at = tracked_order
        .created_at_unix
        .and_then(|unix| chrono::DateTime::<Utc>::from_timestamp(unix, 0))
        .unwrap_or_else(Utc::now);
    TrackedOrder {
        order_id: tracked_order.order_id.clone(),
        trace_id: tracked_order
            .trace_id
            .clone()
            .or(default_trace_id)
            .unwrap_or_else(|| format!("hedge-trace-{}", Uuid::new_v4())),
        condition_id: market.condition_id.clone(),
        created_at,
        leg: tracked_order.leg,
        token_id: token_id_for_leg(tracked_order.leg, market),
        opposite_token_id: opposite_token_id_for_leg(tracked_order.leg, market),
        side: side_for_leg(tracked_order.leg),
        price: tracked_order.price,
        size: tracked_order.size,
        matched_size: tracked_order.matched_size,
        neg_risk: false,
        tick_size: market.tick_size.clone(),
    }
}

pub(super) fn scenario_trade_to_trade_event(
    market: &ScenarioMarket,
    trade: &ScenarioTrade,
    fallback_leg: Option<QuoteLeg>,
) -> Result<TradeEvent> {
    let leg = fallback_leg.or_else(|| match trade.outcome.as_deref() {
        Some("YES") if trade.side == Side::Buy => Some(QuoteLeg::YesBid),
        Some("YES") if trade.side == Side::Sell => Some(QuoteLeg::YesAsk),
        Some("NO") if trade.side == Side::Buy => Some(QuoteLeg::NoBid),
        Some("NO") if trade.side == Side::Sell => Some(QuoteLeg::NoAsk),
        _ => None,
    });
    let leg = leg.ok_or_else(|| {
        anyhow!(
            "Scenario trade {} needs asset_id/outcome or a fallback tracked leg",
            trade.trade_id
        )
    })?;
    let trade_timestamp = trade
        .timestamp_unix
        .and_then(|unix| chrono::DateTime::<Utc>::from_timestamp(unix, 0))
        .unwrap_or_else(Utc::now);

    Ok(TradeEvent {
        id: trade.trade_id.clone(),
        condition_id: trade
            .condition_id
            .clone()
            .unwrap_or_else(|| market.condition_id.clone()),
        asset_id: trade
            .asset_id
            .clone()
            .unwrap_or_else(|| token_id_for_leg(leg, market)),
        side: trade.side,
        price: trade.price,
        size: trade.size,
        outcome: trade
            .outcome
            .clone()
            .unwrap_or_else(|| outcome_for_leg(leg).to_string()),
        status: TradeStatus::Matched,
        timestamp: trade_timestamp,
        maker_order_id: trade.maker_order_id.clone(),
        taker_order_id: trade.taker_order_id.clone(),
    })
}

pub(super) fn token_id_for_leg(leg: QuoteLeg, market: &ScenarioMarket) -> String {
    match leg {
        QuoteLeg::YesBid | QuoteLeg::YesAsk => market.yes_token_id.clone(),
        QuoteLeg::NoBid | QuoteLeg::NoAsk => market.no_token_id.clone(),
    }
}

pub(super) fn opposite_token_id_for_leg(leg: QuoteLeg, market: &ScenarioMarket) -> String {
    match leg {
        QuoteLeg::YesBid | QuoteLeg::YesAsk => market.no_token_id.clone(),
        QuoteLeg::NoBid | QuoteLeg::NoAsk => market.yes_token_id.clone(),
    }
}

fn balance_step_to_json(step: &ScenarioBalanceStep) -> Value {
    let atomic = step.amount * Decimal::from(1_000_000u64);
    json!({ "balance": atomic.to_string() })
}

fn position_step_to_json(condition_id: &str, step: &ScenarioPositionStep) -> Value {
    let mut entries = Vec::new();
    if step.yes_size > Decimal::ZERO {
        entries.push(json!({
            "conditionId": condition_id,
            "size": step.yes_size.to_string().parse::<f64>().unwrap_or(0.0),
            "avgPrice": step.yes_avg_price.to_string().parse::<f64>().unwrap_or(0.0),
            "outcome": "YES",
        }));
    }
    if step.no_size > Decimal::ZERO {
        entries.push(json!({
            "conditionId": condition_id,
            "size": step.no_size.to_string().parse::<f64>().unwrap_or(0.0),
            "avgPrice": step.no_avg_price.to_string().parse::<f64>().unwrap_or(0.0),
            "outcome": "NO",
        }));
    }
    json!(entries)
}

fn open_orders_step_to_json(market: &ScenarioMarket, step: &ScenarioOpenOrdersStep) -> Value {
    let orders: Vec<Value> = step
        .orders
        .iter()
        .map(|order| live_order_to_json(market, order))
        .collect();
    json!({
        "data": orders,
        "next_cursor": "LTE=",
    })
}

fn live_order_to_json(market: &ScenarioMarket, order: &ScenarioLiveOrder) -> Value {
    let (asset_id, outcome, side) = match order.leg {
        QuoteLeg::YesBid => (&market.yes_token_id, "YES", "BUY"),
        QuoteLeg::YesAsk => (&market.yes_token_id, "YES", "SELL"),
        QuoteLeg::NoBid => (&market.no_token_id, "NO", "BUY"),
        QuoteLeg::NoAsk => (&market.no_token_id, "NO", "SELL"),
    };
    json!({
        "id": order.id,
        "status": order.status,
        "market": market.condition_id,
        "asset_id": asset_id,
        "side": side,
        "price": order.price.to_string(),
        "original_size": order.original_size.to_string(),
        "size_matched": order.size_matched.to_string(),
        "outcome": outcome,
        "order_type": order.order_type,
        "created_at": order.created_at_unix.unwrap_or_else(|| Utc::now().timestamp()),
        "associate_trades": order.associated_trade_ids,
    })
}

fn parse_side(value: &str) -> Option<Side> {
    match value {
        "BUY" => Some(Side::Buy),
        "SELL" => Some(Side::Sell),
        _ => None,
    }
}

fn parse_order_type(value: &str) -> Option<OrderType> {
    match value {
        "GTC" => Some(OrderType::GTC),
        "GTD" => Some(OrderType::GTD),
        "FOK" => Some(OrderType::FOK),
        "FAK" => Some(OrderType::FAK),
        _ => None,
    }
}

pub(super) async fn build_observed_outcome(
    events: &[EventEnvelope],
    risk_manager: Arc<RiskManager>,
    condition_id: &str,
    request_log: Vec<MockRequestRecord>,
) -> Result<ObservedHedgeOutcome> {
    let intent = latest_payload::<HedgeIntentPayload>(events, EventType::HedgeIntentCreated)?;
    let hedge_result =
        latest_payload::<HedgeResultPayload>(events, EventType::HedgeResultRecorded)?;
    let hedge_exit =
        latest_payload::<HedgeExitPathPayload>(events, EventType::HedgeExitPathRecorded)?;
    let neutrality = latest_payload::<NeutralityPayload>(events, EventType::NeutralityEvaluated)?;

    let market_halted = risk_manager
        .get_market_state(condition_id)
        .await
        .map(|state| state.halted)
        .unwrap_or(false);
    let halted = market_halted || risk_manager.is_globally_halted().await;

    Ok(ObservedHedgeOutcome {
        result_status: hedge_result
            .as_ref()
            .map(|payload| payload.result_status.clone()),
        hedge_side: intent
            .as_ref()
            .and_then(|payload| parse_side(&payload.hedge_side)),
        planned_hedge_shares: intent
            .as_ref()
            .and_then(|payload| payload.planned_hedge_shares),
        planned_sellback_shares: intent
            .as_ref()
            .and_then(|payload| payload.planned_sellback_shares),
        hedge_leg_status: hedge_result
            .as_ref()
            .and_then(|payload| payload.hedge_leg_status.clone()),
        sellback_leg_status: hedge_result
            .as_ref()
            .and_then(|payload| payload.sellback_leg_status.clone()),
        hedge_price: hedge_result
            .as_ref()
            .and_then(|payload| payload.hedge_price),
        sellback_price: hedge_result
            .as_ref()
            .and_then(|payload| payload.sellback_price),
        post_sync_yes_size: hedge_result
            .as_ref()
            .and_then(|payload| payload.post_sync_yes_size),
        post_sync_no_size: hedge_result
            .as_ref()
            .and_then(|payload| payload.post_sync_no_size),
        post_sync_net_exposure: hedge_result
            .as_ref()
            .and_then(|payload| payload.post_sync_net_exposure),
        neutrality_residual_exposure: neutrality.as_ref().map(|payload| payload.residual_exposure),
        exit_path_status: hedge_exit
            .as_ref()
            .map(|payload| payload.exit_path_status.clone()),
        ctf_merge_configured: hedge_exit.as_ref().map(|payload| payload.ctf_merge_configured),
        merge_attempted: hedge_exit.as_ref().map(|payload| payload.merge_attempted),
        merge_tx_hash: hedge_exit
            .as_ref()
            .and_then(|payload| payload.merge_tx_hash.clone()),
        merge_eligible_pairs: hedge_exit
            .as_ref()
            .map(|payload| payload.merge_eligible_pairs),
        fallback_ask_count: hedge_exit.as_ref().map(|payload| payload.fallback_ask_count),
        halted,
        request_log,
    })
}

pub(super) fn latest_payload<T>(
    events: &[EventEnvelope],
    event_type: EventType,
) -> Result<Option<T>>
where
    T: for<'de> Deserialize<'de>,
{
    let Some(event) = events
        .iter()
        .rev()
        .find(|event| event.event_type == event_type)
    else {
        return Ok(None);
    };

    serde_json::from_value(event.payload.clone())
        .map(Some)
        .with_context(|| format!("Failed to deserialize payload for {}", event_type))
}

pub(super) fn compare_expected_to_observed(
    expected: &ScenarioExpected,
    observed: &ObservedHedgeOutcome,
    actual_success: bool,
) -> Vec<String> {
    let mut mismatches = Vec::new();

    if expected.success != actual_success {
        mismatches.push(format!(
            "success mismatch: expected {}, got {}",
            expected.success, actual_success
        ));
    }
    if expected.halted != observed.halted {
        mismatches.push(format!(
            "halted mismatch: expected {}, got {}",
            expected.halted, observed.halted
        ));
    }
    compare_option_value(
        &mut mismatches,
        "hedge_side",
        expected.hedge_side.map(|side| side.to_string()),
        observed.hedge_side.map(|side| side.to_string()),
    );
    compare_option_value(
        &mut mismatches,
        "planned_hedge_shares",
        expected.planned_hedge_shares,
        observed.planned_hedge_shares,
    );
    compare_option_value(
        &mut mismatches,
        "planned_sellback_shares",
        expected.planned_sellback_shares,
        observed.planned_sellback_shares,
    );
    compare_option_value(
        &mut mismatches,
        "result_status",
        expected.result_status.clone(),
        observed.result_status.clone(),
    );
    compare_option_value(
        &mut mismatches,
        "hedge_leg_status",
        expected.hedge_leg_status.clone(),
        observed.hedge_leg_status.clone(),
    );
    compare_option_value(
        &mut mismatches,
        "sellback_leg_status",
        expected.sellback_leg_status.clone(),
        observed.sellback_leg_status.clone(),
    );
    compare_option_value(
        &mut mismatches,
        "hedge_price",
        expected.hedge_price,
        observed.hedge_price,
    );
    compare_option_value(
        &mut mismatches,
        "sellback_price",
        expected.sellback_price,
        observed.sellback_price,
    );
    compare_option_value(
        &mut mismatches,
        "post_sync_yes_size",
        expected.post_sync_yes_size,
        observed.post_sync_yes_size,
    );
    compare_option_value(
        &mut mismatches,
        "post_sync_no_size",
        expected.post_sync_no_size,
        observed.post_sync_no_size,
    );
    compare_option_value(
        &mut mismatches,
        "post_sync_net_exposure",
        expected.post_sync_net_exposure,
        observed.post_sync_net_exposure,
    );
    compare_option_value(
        &mut mismatches,
        "neutrality_residual_exposure",
        expected.neutrality_residual_exposure,
        observed.neutrality_residual_exposure,
    );
    if expected.exit_path_status.is_some() {
        compare_option_value(
            &mut mismatches,
            "exit_path_status",
            expected.exit_path_status.clone(),
            observed.exit_path_status.clone(),
        );
    }
    if expected.ctf_merge_configured.is_some() {
        compare_option_value(
            &mut mismatches,
            "ctf_merge_configured",
            expected.ctf_merge_configured,
            observed.ctf_merge_configured,
        );
    }
    if expected.merge_attempted.is_some() {
        compare_option_value(
            &mut mismatches,
            "merge_attempted",
            expected.merge_attempted,
            observed.merge_attempted,
        );
    }
    if expected.merge_tx_hash.is_some() {
        compare_option_value(
            &mut mismatches,
            "merge_tx_hash",
            expected.merge_tx_hash.clone(),
            observed.merge_tx_hash.clone(),
        );
    }
    if expected.merge_eligible_pairs.is_some() {
        compare_option_value(
            &mut mismatches,
            "merge_eligible_pairs",
            expected.merge_eligible_pairs,
            observed.merge_eligible_pairs,
        );
    }
    if expected.fallback_ask_count.is_some() {
        compare_option_value(
            &mut mismatches,
            "fallback_ask_count",
            expected.fallback_ask_count,
            observed.fallback_ask_count,
        );
    }

    mismatches
}

fn compare_option_value<T>(
    mismatches: &mut Vec<String>,
    field: &str,
    expected: Option<T>,
    actual: Option<T>,
) where
    T: PartialEq + std::fmt::Debug,
{
    if expected != actual {
        mismatches.push(format!(
            "{} mismatch: expected {:?}, got {:?}",
            field, expected, actual
        ));
    }
}

pub(super) fn fill_handler_from_engine(engine: &LiveEngine) -> FillHandler {
    FillHandler {
        order_manager: engine.order_manager.clone(),
        hedge_executor: engine.hedge_executor.clone(),
        managed_markets: engine.managed_markets.clone(),
        known_markets: engine.known_markets.clone(),
        risk_manager: engine.risk_manager.clone(),
        position_manager: engine.position_manager.clone(),
        book_manager: engine.book_manager.clone(),
        book_rest: engine.book_rest.clone(),
        trading_client: engine.trading_client.clone(),
        config: engine.config.clone(),
        event_producer: engine.event_producer.clone(),
        run_id: engine.run_id.clone(),
        mode: engine.mode.clone(),
        cached_balance: engine.cached_balance.clone(),
        hedge_order_ids: engine.hedge_order_ids.clone(),
        recon_baselines: engine.recon_baselines.clone(),
        hedge_signals: engine.hedge_signals.clone(),
        recent_resolution_trades: engine.recent_resolution_trades.clone(),
        ctf_merger: engine.ctf_merger.clone(),
        hedge_locks: engine.hedge_locks.clone(),
        error_logger: engine.error_logger.clone(),
    }
}

pub(super) async fn drain_fill_queue(
    fill_handler: &FillHandler,
    fill_rx: &mut mpsc::UnboundedReceiver<FillWorkItem>,
) -> Result<()> {
    while let Ok(work) = fill_rx.try_recv() {
        fill_handler.handle_fill(work).await?;
    }
    Ok(())
}

fn validate_token_id_uint256_str(token_id: &str) -> Result<()> {
    let token_id = token_id.trim();
    if token_id.is_empty() {
        bail!("token_id must not be empty");
    }

    let mut result = [0u8; 32];
    for ch in token_id.chars() {
        let digit =
            ch.to_digit(10)
                .ok_or_else(|| anyhow!("Invalid digit in token_id: '{}'", ch))? as u16;

        let mut carry = digit;
        for byte in result.iter_mut().rev() {
            let value = (*byte as u16) * 10 + carry;
            *byte = (value & 0xFF) as u8;
            carry = value >> 8;
        }

        if carry > 0 {
            bail!("token_id overflows uint256");
        }
    }

    Ok(())
}
