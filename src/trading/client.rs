use anyhow::{anyhow, Context, Result};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::auth::{OrderSigner, RequestSigner};
use crate::models::{
    LiveOrder, OrderAmountKind, OrderRequest, OrderResult, OrderStatus, OrderType, Outcome, Side,
};

/// How long cached fee rates remain valid before re-fetching.
const FEE_CACHE_TTL_SECS: u64 = 300; // 5 minutes

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CancelOrderOutcome {
    Confirmed,
    Rejected(String),
    Unknown(String),
}

/// Authenticated trading client for the Polymarket CLOB API.
pub struct TradingClient {
    http: reqwest::Client,
    base_url: String,
    signer: RequestSigner,
    order_signer: Option<OrderSigner>,
    api_key: String,
    dry_run: bool,
    /// Cached fee rates per token_id → (rate_bps, fetched_at).
    /// Avoids a REST round-trip on every order placement (critical for hedge speed).
    fee_cache: Arc<RwLock<HashMap<String, (u64, Instant)>>>,
}

// Raw API response types
#[derive(Debug, Deserialize)]
struct RawOrderResponse {
    #[serde(rename = "orderID")]
    order_id: Option<String>,
    status: Option<String>,
    #[serde(rename = "transactionsHashes")]
    transaction_hashes: Option<Vec<String>>,
    #[serde(rename = "tradeIDs")]
    trade_ids: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct RawOrdersResponse {
    data: Option<Vec<RawOrder>>,
    next_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawOrder {
    id: Option<String>,
    status: Option<String>,
    market: Option<String>,
    asset_id: Option<String>,
    side: Option<String>,
    price: Option<String>,
    original_size: Option<String>,
    size_matched: Option<String>,
    outcome: Option<String>,
    order_type: Option<String>,
    created_at: Option<i64>,
    #[serde(rename = "associate_trades", alias = "associated_trade_ids", default)]
    associated_trade_ids: Option<Vec<String>>,
}

impl TradingClient {
    pub fn new(
        base_url: String,
        signer: RequestSigner,
        private_key: Option<&str>,
        proxy_address: &str,
        api_key: &str,
        dry_run: bool,
    ) -> Result<Self> {
        let order_signer = match private_key {
            Some(pk) => Some(OrderSigner::new(pk, proxy_address)?),
            None => None,
        };
        Ok(Self {
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(15))
                .build()
                .context("Failed to build HTTP client")?,
            base_url,
            signer,
            order_signer,
            api_key: api_key.to_string(),
            dry_run,
            fee_cache: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Place an order. In dry-run mode, logs the intent but doesn't submit.
    pub async fn place_order(&self, request: &OrderRequest) -> Result<OrderResult> {
        validate_order_request(request)?;

        if self.dry_run {
            info!(
                token_id = %request.token_id,
                side = %request.side,
                price = %request.price,
                size = %request.size,
                "[DRY RUN] Would place order"
            );
            return Ok(OrderResult {
                order_id: format!("dry-run-{}", uuid::Uuid::new_v4()),
                status: OrderStatus::Live,
                trade_ids: Vec::new(),
            });
        }

        let order_signer = self
            .order_signer
            .as_ref()
            .context("Cannot place real orders without POLY_PRIVATE_KEY")?;

        // Fetch the fee rate for this token (required for valid EIP-712 signature)
        let fee_rate_bps = self.get_fee_rate_bps(&request.token_id).await?;

        let signed_payload = order_signer.sign_order(
            &request.token_id,
            request.price,
            request.size,
            request.side,
            request.order_type,
            request.neg_risk,
            fee_rate_bps,
            &request.tick_size,
        )?;

        let mut body_value =
            serde_json::to_value(&signed_payload).context("Failed to serialize signed order")?;

        // owner = API key (not wallet address) per Polymarket CLOB protocol
        body_value["owner"] = serde_json::Value::String(self.api_key.clone());
        body_value["postOnly"] = serde_json::Value::Bool(request.post_only);

        // salt must be a JSON number (not string) per Polymarket API
        let salt_str = body_value["order"]["salt"]
            .as_str()
            .unwrap_or("0")
            .to_string();
        let body = serde_json::to_string(&body_value).context("Failed to serialize order body")?;
        let body = body.replace(
            &format!("\"salt\":\"{}\"", salt_str),
            &format!("\"salt\":{}", salt_str),
        );
        let path = "/order";

        let headers = self.signer.build_headers("POST", path, &body)?;
        let url = format!("{}{}", self.base_url, path);

        info!(url = %url, body = %body, "Posting signed order");

        let mut req = self.http.post(&url).body(body.clone());
        req = req.header("Content-Type", "application/json");
        for (key, value) in &headers {
            req = req.header(key, value);
        }

        let resp = req.send().await.context("Failed to send order")?;
        let status = resp.status();

        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Order placement failed ({}): {}", status, body);
        }

        let raw: RawOrderResponse = resp
            .json()
            .await
            .context("Failed to parse order response")?;

        let order_status = match raw.status.as_deref() {
            Some("live") => OrderStatus::Live,
            Some("matched") => OrderStatus::Matched,
            Some("delayed") => OrderStatus::Delayed,
            _ => OrderStatus::Invalid,
        };

        info!(
            order_id = ?raw.order_id,
            status = %order_status,
            "Order placed"
        );

        Ok(OrderResult {
            order_id: raw.order_id.unwrap_or_default(),
            status: order_status,
            trade_ids: raw.trade_ids.unwrap_or_default(),
        })
    }

    /// Cancel a specific order by ID.
    pub async fn cancel_order(&self, order_id: &str) -> Result<CancelOrderOutcome> {
        if self.dry_run {
            info!(order_id = %order_id, "[DRY RUN] Would cancel order");
            return Ok(CancelOrderOutcome::Confirmed);
        }

        let path = "/order";
        let body = serde_json::json!({"orderID": order_id}).to_string();
        let headers = self.signer.build_headers("DELETE", path, &body)?;
        let url = format!("{}{}", self.base_url, path);

        let mut req = self
            .http
            .delete(&url)
            .body(body)
            .header("Content-Type", "application/json");
        for (key, value) in &headers {
            req = req.header(key, value);
        }

        let resp = req.send().await.context("Failed to cancel order")?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Cancel failed ({}): {}", status, body);
        }

        let body = resp.text().await.unwrap_or_default();
        let parsed: Value = serde_json::from_str(&body).unwrap_or(Value::Null);

        if let Some(outcome) = parse_cancel_order_response(order_id, &parsed) {
            match &outcome {
                CancelOrderOutcome::Confirmed => {
                    info!(order_id = %order_id, "Order cancelled");
                }
                CancelOrderOutcome::Rejected(reason) => {
                    warn!(order_id = %order_id, reason = %reason, "Order cancel rejected");
                }
                CancelOrderOutcome::Unknown(reason) => {
                    warn!(order_id = %order_id, reason = %reason, "Order cancel unverified");
                }
            }
            return Ok(outcome);
        }

        let fallback = match self.get_order(order_id).await {
            Ok(order) => fallback_cancel_outcome_from_lookup(order_id, order.as_ref()),
            Err(err) => CancelOrderOutcome::Unknown(format!(
                "cancel response ambiguous and follow-up get_order failed: {}",
                err
            )),
        };

        match &fallback {
            CancelOrderOutcome::Confirmed => {
                info!(order_id = %order_id, "Order cancel verified by follow-up lookup");
            }
            CancelOrderOutcome::Rejected(reason) => {
                warn!(order_id = %order_id, reason = %reason, "Order cancel rejected");
            }
            CancelOrderOutcome::Unknown(reason) => {
                warn!(order_id = %order_id, reason = %reason, "Order cancel unverified");
            }
        }

        Ok(fallback)
    }

    /// Cancel all orders for a specific market and asset.
    pub async fn cancel_market_orders(&self, condition_id: &str, asset_id: &str) -> Result<()> {
        if self.dry_run {
            info!(
                condition_id = %condition_id,
                asset_id = %asset_id,
                "[DRY RUN] Would cancel all market orders"
            );
            return Ok(());
        }

        let path = "/cancel-market-orders";
        let body = serde_json::json!({
            "market": condition_id,
            "asset_id": asset_id,
        })
        .to_string();

        let headers = self.signer.build_headers("DELETE", path, &body)?;
        let url = format!("{}{}", self.base_url, path);

        let mut req = self
            .http
            .delete(&url)
            .body(body)
            .header("Content-Type", "application/json");
        for (key, value) in &headers {
            req = req.header(key, value);
        }

        let resp = req.send().await.context("Failed to cancel market orders")?;
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Cancel market orders failed: {}", body);
        }

        info!(condition_id = %condition_id, "Market orders cancelled");
        Ok(())
    }

    /// Fetch all open orders, optionally filtered by market.
    pub async fn get_open_orders(&self, condition_id: Option<&str>) -> Result<Vec<LiveOrder>> {
        let base_path = match condition_id {
            Some(cid) => format!("/data/orders?market={}", cid),
            None => "/data/orders".to_string(),
        };

        let mut all_orders = Vec::new();
        let mut cursor: Option<String> = None;

        loop {
            let path = match &cursor {
                Some(c) => {
                    if base_path.contains('?') {
                        format!("{}&next_cursor={}", base_path, c)
                    } else {
                        format!("{}?next_cursor={}", base_path, c)
                    }
                }
                None => base_path.clone(),
            };

            let headers = self.signer.build_headers("GET", &path, "")?;
            let url = format!("{}{}", self.base_url, path);

            let mut req = self.http.get(&url);
            for (key, value) in &headers {
                req = req.header(key, value);
            }

            let resp = req.send().await.context("Failed to fetch orders")?;
            let status = resp.status();
            if !status.is_success() {
                let body = resp.text().await.unwrap_or_default();
                anyhow::bail!("Get orders failed ({}): {}", status, body);
            }

            let raw: RawOrdersResponse = resp
                .json()
                .await
                .context("Failed to parse orders response")?;

            let page: Vec<LiveOrder> = raw
                .data
                .unwrap_or_default()
                .into_iter()
                .filter_map(|o| parse_raw_order(o))
                .collect();

            all_orders.extend(page);

            match raw.next_cursor {
                // "LTE=" is base64("-1") = Polymarket's end-of-data marker
                Some(c) if !c.is_empty() && c != "LTE=" => cursor = Some(c),
                _ => break,
            }
        }

        debug!(count = all_orders.len(), "Fetched open orders (all pages)");
        Ok(all_orders)
    }

    /// Check if a specific order is currently scoring for rewards.
    pub async fn check_order_scoring(&self, order_id: &str) -> Result<bool> {
        let path = format!("/orders/{}/scoring-status", order_id);
        let headers = self.signer.build_headers("GET", &path, "")?;
        let url = format!("{}{}", self.base_url, path);

        let mut req = self.http.get(&url);
        for (key, value) in &headers {
            req = req.header(key, value);
        }

        let resp = req.send().await.context("Failed to check scoring")?;
        if !resp.status().is_success() {
            let status = resp.status();
            warn!(order_id = %order_id, %status, "Scoring check failed");
            return Err(anyhow!("scoring check failed with status {}", status));
        }

        // The API returns scoring status; parse conservatively
        let body: serde_json::Value = resp.json().await.unwrap_or_default();
        let scoring = body
            .get("scoring")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        debug!(order_id = %order_id, scoring = scoring, "Order scoring status");
        Ok(scoring)
    }

    /// Fetch the fee rate (in basis points) for a token from the CLOB API.
    async fn get_fee_rate_bps(&self, token_id: &str) -> Result<u64> {
        // Check cache first — avoids a REST round-trip on the hedge hot path
        {
            let cache = self.fee_cache.read().await;
            if let Some((rate, cached_at)) = cache.get(token_id) {
                if cached_at.elapsed().as_secs() < FEE_CACHE_TTL_SECS {
                    return Ok(*rate);
                }
            }
        }

        let path = format!("/fee-rate?token_id={}", token_id);
        let headers = self.signer.build_headers("GET", &path, "")?;
        let url = format!("{}{}", self.base_url, path);

        let mut req = self.http.get(&url);
        for (key, value) in &headers {
            req = req.header(key, value);
        }

        let resp = req.send().await.context("Failed to fetch fee rate")?;
        if !resp.status().is_success() {
            warn!(token_id = %token_id, "Fee rate fetch failed, defaulting to 0");
            // Cache the default too — don't re-fetch on every order
            let mut cache = self.fee_cache.write().await;
            cache.insert(token_id.to_string(), (0, Instant::now()));
            return Ok(0);
        }

        let body: serde_json::Value = resp.json().await.unwrap_or_default();
        let fee = body
            .get("base_fee")
            .map(|v| match v {
                serde_json::Value::String(s) => s.parse::<u64>().unwrap_or(0),
                serde_json::Value::Number(n) => n.as_u64().unwrap_or(0),
                _ => 0,
            })
            .unwrap_or(0);

        debug!(token_id = %token_id, fee_rate_bps = fee, "Fetched fee rate");

        // Cache the result
        let mut cache = self.fee_cache.write().await;
        cache.insert(token_id.to_string(), (fee, Instant::now()));

        Ok(fee)
    }

    /// Fetch the user's USDC balance from the CLOB API.
    /// Returns total USDC (includes collateral locked in resting orders).
    pub async fn get_balance(&self) -> Result<Decimal> {
        if self.dry_run {
            return Ok(Decimal::ZERO);
        }

        let sign_path = "/balance-allowance";
        let headers = self.signer.build_headers("GET", sign_path, "")?;
        let url = format!(
            "{}{}?asset_type=COLLATERAL&signature_type=2",
            self.base_url, sign_path
        );

        let mut req = self.http.get(&url);
        for (key, value) in &headers {
            req = req.header(key, value);
        }

        let resp = req.send().await.context("Failed to fetch balance")?;
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Balance fetch failed: {}", body);
        }

        let body: serde_json::Value = resp
            .json()
            .await
            .context("Failed to parse balance response")?;

        // Parse balance from response — try common field names
        let raw_balance = body
            .get("balance")
            .or_else(|| body.get("available"))
            .and_then(|v| match v {
                serde_json::Value::String(s) => Decimal::from_str(s).ok(),
                serde_json::Value::Number(n) => Decimal::from_str(&n.to_string()).ok(),
                _ => None,
            })
            .unwrap_or(Decimal::ZERO);

        // API returns atomic USDC units (6 decimals). Convert to dollars.
        let balance = raw_balance / Decimal::from(1_000_000u64);

        info!(raw_atomic = %raw_balance, balance_usd = %balance, "Fetched USDC balance");
        Ok(balance)
    }

    /// Fetch a single order by ID. Returns None if the order is not found.
    pub async fn get_order(&self, order_id: &str) -> Result<Option<LiveOrder>> {
        if self.dry_run {
            return Ok(None);
        }

        let path = format!("/data/order/{}", order_id);
        let headers = self.signer.build_headers("GET", &path, "")?;
        let url = format!("{}{}", self.base_url, path);

        let mut req = self.http.get(&url);
        for (key, value) in &headers {
            req = req.header(key, value);
        }

        let resp = req.send().await.context("Failed to fetch order")?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            warn!(order_id = %order_id, status = %status, body = %body, "get_order failed");
            return Ok(None);
        }

        let body: Value = resp
            .json()
            .await
            .context("Failed to parse order response")?;

        parse_order_response(body)
    }

    pub fn is_dry_run(&self) -> bool {
        self.dry_run
    }
}

fn validate_order_request(request: &OrderRequest) -> Result<()> {
    let is_market_style = matches!(request.order_type, OrderType::FOK | OrderType::FAK);

    if request.amount_kind == OrderAmountKind::UsdcNotional {
        anyhow::bail!(
            "USDC-notional order requests are not supported here; use an explicit market-order implementation"
        );
    }

    if request.side == Side::Buy && is_market_style {
        anyhow::bail!(
            "BUY FOK/FAK orders must use explicit USDC-notional semantics; use an aggressive share-sized limit order instead"
        );
    }

    Ok(())
}

fn parse_order_response(body: Value) -> Result<Option<LiveOrder>> {
    if body.is_null() {
        return Ok(None);
    }

    let raw: RawOrder = serde_json::from_value(body).context("Failed to decode order payload")?;
    Ok(parse_raw_order(raw))
}

fn parse_cancel_order_response(order_id: &str, body: &Value) -> Option<CancelOrderOutcome> {
    if let Some(canceled) = body.get("canceled").and_then(|v| v.as_array()) {
        if canceled
            .iter()
            .filter_map(Value::as_str)
            .any(|id| id == order_id)
        {
            return Some(CancelOrderOutcome::Confirmed);
        }
    }

    if let Some(not_canceled) = body.get("not_canceled").and_then(|v| v.as_object()) {
        if let Some(reason) = not_canceled.get(order_id) {
            return Some(CancelOrderOutcome::Rejected(cancel_reason_from_value(
                reason,
            )));
        }
    }

    None
}

fn fallback_cancel_outcome_from_lookup(
    order_id: &str,
    order: Option<&LiveOrder>,
) -> CancelOrderOutcome {
    match order {
        Some(order) if order.status == OrderStatus::Live => CancelOrderOutcome::Unknown(format!(
            "cancel response ambiguous and order {} is still live",
            order_id
        )),
        Some(_order) => CancelOrderOutcome::Confirmed,
        None => CancelOrderOutcome::Unknown(format!(
            "cancel response ambiguous and order {} could not be verified",
            order_id
        )),
    }
}

fn cancel_reason_from_value(value: &Value) -> String {
    if let Some(reason) = value.as_str() {
        return reason.to_string();
    }

    if let Some(object) = value.as_object() {
        for key in ["reason", "message", "error", "errorMsg"] {
            if let Some(reason) = object.get(key).and_then(Value::as_str) {
                return reason.to_string();
            }
        }
    }

    value.to_string()
}

fn parse_raw_order(raw: RawOrder) -> Option<LiveOrder> {
    let RawOrder {
        id,
        status,
        market,
        asset_id,
        side,
        price,
        original_size,
        size_matched,
        outcome,
        order_type,
        created_at,
        associated_trade_ids,
    } = raw;

    let status = match status.as_deref()? {
        "ORDER_STATUS_LIVE" | "live" => OrderStatus::Live,
        "ORDER_STATUS_MATCHED" | "matched" => OrderStatus::Matched,
        "ORDER_STATUS_DELAYED" | "delayed" => OrderStatus::Delayed,
        "ORDER_STATUS_CANCELLED" | "cancelled" => OrderStatus::Cancelled,
        "ORDER_STATUS_INVALID" | "invalid" => OrderStatus::Invalid,
        _ => OrderStatus::Invalid,
    };

    let side = match side.as_deref()? {
        "BUY" => Side::Buy,
        "SELL" => Side::Sell,
        _ => return None,
    };

    let outcome = match outcome.as_deref()? {
        "YES" => Outcome::Yes,
        "NO" => Outcome::No,
        _ => return None,
    };

    let order_type = match order_type.as_deref() {
        Some("GTC") => OrderType::GTC,
        Some("GTD") => OrderType::GTD,
        Some("FOK") => OrderType::FOK,
        Some("FAK") => OrderType::FAK,
        _ => OrderType::GTC,
    };

    let id = id?;
    let associated_trade_ids = associated_trade_ids.unwrap_or_default();

    Some(LiveOrder {
        id,
        condition_id: market.unwrap_or_default(),
        asset_id: asset_id.unwrap_or_default(),
        side,
        price: Decimal::from_str(price.as_deref()?).ok()?,
        original_size: Decimal::from_str(original_size.as_deref().unwrap_or("0")).ok()?,
        size_matched: Decimal::from_str(size_matched.as_deref().unwrap_or("0")).ok()?,
        outcome,
        order_type,
        status,
        created_at: chrono::DateTime::from_timestamp(created_at.unwrap_or(0), 0)
            .unwrap_or_else(|| chrono::Utc::now()),
        associated_trade_ids,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use serde_json::json;

    fn sample_request(
        side: Side,
        order_type: OrderType,
        amount_kind: OrderAmountKind,
    ) -> OrderRequest {
        OrderRequest {
            token_id: "token".to_string(),
            price: dec!(0.39),
            size: dec!(176.95),
            amount_kind,
            side,
            order_type,
            post_only: false,
            neg_risk: false,
            tick_size: "0.01".to_string(),
        }
    }

    #[test]
    fn rejects_share_sized_buy_market_style_orders() {
        let request = sample_request(Side::Buy, OrderType::FOK, OrderAmountKind::Shares);
        let err = validate_order_request(&request).unwrap_err().to_string();
        assert!(err.contains("BUY FOK/FAK orders"));
    }

    #[test]
    fn allows_share_sized_buy_limit_orders() {
        let request = sample_request(Side::Buy, OrderType::GTC, OrderAmountKind::Shares);
        assert!(validate_order_request(&request).is_ok());
    }

    #[test]
    fn rejects_usdc_notional_requests_until_explicitly_implemented() {
        let request = sample_request(Side::Buy, OrderType::FOK, OrderAmountKind::UsdcNotional);
        let err = validate_order_request(&request).unwrap_err().to_string();
        assert!(err.contains("USDC-notional"));
    }

    #[test]
    fn parse_cancel_response_confirms_when_order_id_is_canceled() {
        let body = json!({
            "canceled": ["order-1"],
            "not_canceled": {}
        });

        assert_eq!(
            parse_cancel_order_response("order-1", &body),
            Some(CancelOrderOutcome::Confirmed)
        );
    }

    #[test]
    fn parse_cancel_response_rejects_when_order_id_is_not_canceled() {
        let body = json!({
            "canceled": [],
            "not_canceled": {
                "order-1": "order not found"
            }
        });

        assert_eq!(
            parse_cancel_order_response("order-1", &body),
            Some(CancelOrderOutcome::Rejected("order not found".to_string()))
        );
    }

    #[test]
    fn parse_raw_order_preserves_associate_trades() {
        let raw: RawOrder = serde_json::from_value(json!({
            "id": "order-associated-trades-1",
            "status": "matched",
            "market": "cond-1",
            "asset_id": "asset-1",
            "side": "SELL",
            "price": "0.51",
            "original_size": "10",
            "size_matched": "10",
            "outcome": "YES",
            "order_type": "FOK",
            "created_at": 1_700_000_000_i64,
            "associate_trades": ["trade-1", "trade-2"]
        }))
        .expect("deserialize raw order");

        let order = parse_raw_order(raw).expect("parse live order");
        assert_eq!(
            order.associated_trade_ids(),
            Some(vec!["trade-1".to_string(), "trade-2".to_string()])
        );
        assert!(order.is_fully_filled());
    }

    #[test]
    fn parse_raw_order_accepts_associated_trade_ids_alias() {
        let raw: RawOrder = serde_json::from_value(json!({
            "id": "order-associated-trades-2",
            "status": "live",
            "market": "cond-1",
            "asset_id": "asset-1",
            "side": "BUY",
            "price": "0.49",
            "original_size": "10",
            "size_matched": "3",
            "outcome": "NO",
            "order_type": "GTC",
            "created_at": 1_700_000_001_i64,
            "associated_trade_ids": ["trade-3"]
        }))
        .expect("deserialize raw order");

        let order = parse_raw_order(raw).expect("parse live order");
        assert_eq!(
            order.associated_trade_ids(),
            Some(vec!["trade-3".to_string()])
        );
        assert!(!order.is_fully_filled());
    }

    #[test]
    fn parse_raw_order_maps_delayed_status() {
        let raw: RawOrder = serde_json::from_value(json!({
            "id": "order-delayed-1",
            "status": "delayed",
            "market": "cond-1",
            "asset_id": "asset-1",
            "side": "SELL",
            "price": "0.51",
            "original_size": "10",
            "size_matched": "0",
            "outcome": "YES",
            "order_type": "FOK",
            "created_at": 1_700_000_002_i64
        }))
        .expect("deserialize raw order");

        let order = parse_raw_order(raw).expect("parse delayed order");
        assert_eq!(order.status, OrderStatus::Delayed);
    }

    #[test]
    fn parse_order_response_treats_null_as_missing_order() {
        let order = parse_order_response(Value::Null).expect("null response should parse");

        assert!(order.is_none());
    }

    #[test]
    fn parse_order_response_decodes_live_order_payload() {
        let order = parse_order_response(json!({
            "id": "order-response-1",
            "status": "matched",
            "market": "cond-1",
            "asset_id": "asset-1",
            "side": "BUY",
            "price": "0.49",
            "original_size": "5",
            "size_matched": "5",
            "outcome": "YES",
            "order_type": "GTC",
            "created_at": 1_700_000_003_i64
        }))
        .expect("order payload should parse")
        .expect("order payload should produce a live order");

        assert_eq!(order.id, "order-response-1");
        assert_eq!(order.status, OrderStatus::Matched);
        assert_eq!(order.size_matched, dec!(5));
    }

    #[test]
    fn fallback_cancel_lookup_marks_live_order_as_unknown() {
        let order = LiveOrder {
            id: "order-1".to_string(),
            condition_id: "market".to_string(),
            asset_id: "asset".to_string(),
            side: Side::Buy,
            price: dec!(0.45),
            original_size: dec!(20),
            size_matched: Decimal::ZERO,
            outcome: Outcome::Yes,
            order_type: OrderType::GTC,
            status: OrderStatus::Live,
            created_at: chrono::Utc::now(),
            associated_trade_ids: Vec::new(),
        };

        assert_eq!(
            fallback_cancel_outcome_from_lookup("order-1", Some(&order)),
            CancelOrderOutcome::Unknown(
                "cancel response ambiguous and order order-1 is still live".to_string()
            )
        );
    }

    #[test]
    fn fallback_cancel_lookup_marks_terminal_order_as_confirmed() {
        let order = LiveOrder {
            id: "order-1".to_string(),
            condition_id: "market".to_string(),
            asset_id: "asset".to_string(),
            side: Side::Buy,
            price: dec!(0.45),
            original_size: dec!(20),
            size_matched: dec!(20),
            outcome: Outcome::Yes,
            order_type: OrderType::GTC,
            status: OrderStatus::Matched,
            created_at: chrono::Utc::now(),
            associated_trade_ids: Vec::new(),
        };

        assert_eq!(
            fallback_cancel_outcome_from_lookup("order-1", Some(&order)),
            CancelOrderOutcome::Confirmed
        );
    }
}
