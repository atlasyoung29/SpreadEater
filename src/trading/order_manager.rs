use anyhow::Result;
use chrono::{DateTime, Utc};
use futures_util::future::join_all;
use rust_decimal::Decimal;
use spreadeater_core::payloads::OrderEventDiagnostics;
use spreadeater_core::{CancelReasonCode, EventEnvelope, EventProducer};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration as StdDuration, Instant};
use tokio::sync::RwLock;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::models::{
    CanonicalMarket, OrderAmountKind, OrderRequest, OrderStatus, OrderType, Outcome, Position,
    QuoteLeg, QuoteSet, QuoteStatus, Side,
};
use crate::monitor::emitters;
use crate::trading::client::CancelOrderOutcome;
use crate::trading::TradingClient;

const PENDING_CANCEL_RETRY_BACKOFF_SECS: u64 = 2;

fn instant_age_at_least(now: Instant, recorded_at: Instant, threshold: StdDuration) -> bool {
    now.checked_duration_since(recorded_at)
        .is_some_and(|age| age >= threshold)
}

fn prune_recently_cancelled_entries(
    recently_cancelled: &mut HashMap<String, (TrackedOrder, Instant)>,
    now: Instant,
    ttl: StdDuration,
) {
    recently_cancelled.retain(|_, (_, recorded_at)| !instant_age_at_least(now, *recorded_at, ttl));
}

/// A resting order placed and tracked by us.
#[derive(Debug, Clone)]
pub struct TrackedOrder {
    pub order_id: String,
    pub trace_id: String,
    pub condition_id: String,
    pub created_at: DateTime<Utc>,
    pub leg: QuoteLeg,
    pub token_id: String,
    /// The opposite outcome's token ID (needed for hedging when market metadata is unavailable).
    pub opposite_token_id: String,
    pub side: Side,
    pub price: Decimal,
    pub size: Decimal,
    pub matched_size: Decimal,
    pub neg_risk: bool,
    pub tick_size: String,
}

#[derive(Debug, Clone)]
pub struct MatchUpdate {
    pub tracked_before: TrackedOrder,
    pub tracked_after: Option<TrackedOrder>,
    pub newly_matched: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuplicateLiveBidLeg {
    pub condition_id: String,
    pub leg: QuoteLeg,
    pub order_ids: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OpenOrderSyncResult {
    pub fetched: usize,
    pub live: usize,
    pub imported: usize,
    pub already_tracked: usize,
    pub updated: usize,
    pub duplicate_live_bid_legs: Vec<DuplicateLiveBidLeg>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketOrderSyncMode {
    ObserveOnly,
    Reconcile,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MarketOrderSyncResult {
    pub fetched: usize,
    pub live: usize,
    pub imported: usize,
    pub already_tracked: usize,
    pub updated: usize,
    pub pruned: usize,
    pub missing_order_ids: Vec<String>,
    pub duplicate_live_bid_legs: Vec<DuplicateLiveBidLeg>,
}

#[derive(Debug, Clone)]
struct PendingCancelRetry {
    tracked: TrackedOrder,
    reason_code: CancelReasonCode,
    origin: &'static str,
    first_attempted_at: Instant,
    last_attempted_at: Instant,
    attempts: u32,
}

/// Manages our resting orders across all markets.
///
/// Clone is cheap — all mutable state lives behind Arc<RwLock<...>>.
/// Cloning shares the same underlying state (used by FillHandler task).
#[derive(Clone)]
pub struct OrderManager {
    trading_client: Arc<TradingClient>,
    /// condition_id -> (order_id -> TrackedOrder)
    orders: Arc<RwLock<HashMap<String, HashMap<String, TrackedOrder>>>>,
    /// Gross USDC balance (api_balance + committed_capital at last refresh).
    /// Used to correctly track budget mid-cycle as new orders consume USDC.
    gross_balance: Arc<RwLock<Decimal>>,
    /// (condition_id, leg) pairs that exist on the exchange as of last sync.
    /// Used to prevent duplicate placement when tracking is incomplete.
    exchange_legs: Arc<RwLock<HashSet<(String, QuoteLeg)>>>,
    /// Orders recently cancelled — kept briefly so in-flight fill events
    /// can still be matched (prevents cancel-replace race condition).
    recently_cancelled: Arc<RwLock<HashMap<String, (TrackedOrder, Instant)>>>,
    /// Orders with unverified or rejected cancel attempts.
    pending_cancel_retries: Arc<RwLock<HashMap<String, PendingCancelRetry>>>,
    event_producer: Option<Arc<dyn EventProducer>>,
    run_id: String,
    mode: String,
    /// USDC amount to always keep in the account (never used for orders).
    cash_reserve: Decimal,
}

impl OrderManager {
    pub fn new(
        trading_client: Arc<TradingClient>,
        gross_balance: Arc<RwLock<Decimal>>,
        event_producer: Option<Arc<dyn EventProducer>>,
        run_id: String,
        mode: String,
        cash_reserve: Decimal,
    ) -> Self {
        Self {
            trading_client,
            orders: Arc::new(RwLock::new(HashMap::new())),
            gross_balance,
            exchange_legs: Arc::new(RwLock::new(HashSet::new())),
            recently_cancelled: Arc::new(RwLock::new(HashMap::new())),
            pending_cancel_retries: Arc::new(RwLock::new(HashMap::new())),
            event_producer,
            run_id,
            mode,
            cash_reserve,
        }
    }

    async fn has_exchange_live_leg(&self, condition_id: &str, leg: QuoteLeg) -> bool {
        self.exchange_legs
            .read()
            .await
            .contains(&(condition_id.to_string(), leg))
    }

    async fn remember_exchange_leg(&self, condition_id: &str, leg: QuoteLeg) {
        self.exchange_legs
            .write()
            .await
            .insert((condition_id.to_string(), leg));
    }

    async fn extend_exchange_legs<I>(&self, legs: I)
    where
        I: IntoIterator<Item = (String, QuoteLeg)>,
    {
        self.exchange_legs.write().await.extend(legs);
    }

    async fn replace_exchange_legs_for_market<I>(&self, condition_id: &str, legs: I)
    where
        I: IntoIterator<Item = QuoteLeg>,
    {
        let mut exchange_legs = self.exchange_legs.write().await;
        exchange_legs.retain(|(cid, _)| cid != condition_id);
        for leg in legs {
            exchange_legs.insert((condition_id.to_string(), leg));
        }
    }

    async fn forget_exchange_leg_if_no_active_order(&self, condition_id: &str, leg: QuoteLeg) {
        let still_active = self
            .orders
            .read()
            .await
            .get(condition_id)
            .map(|market_orders| market_orders.values().any(|order| order.leg == leg))
            .unwrap_or(false);
        if still_active {
            return;
        }

        self.exchange_legs
            .write()
            .await
            .remove(&(condition_id.to_string(), leg));
    }

    async fn record_recently_cancelled(&self, tracked: TrackedOrder) {
        self.recently_cancelled
            .write()
            .await
            .insert(tracked.order_id.clone(), (tracked.clone(), Instant::now()));
        self.forget_exchange_leg_if_no_active_order(&tracked.condition_id, tracked.leg)
            .await;
    }

    /// Import existing open orders from the exchange into tracking.
    ///
    /// Called after discovery so the bot is aware of resting orders from prior sessions.
    /// Uses market metadata when available; falls back to defaults for unknown markets
    /// so that capital accounting is always correct.
    pub async fn sync_open_orders(
        &self,
        markets: &[CanonicalMarket],
    ) -> Result<OpenOrderSyncResult> {
        let fetched_orders = self.trading_client.get_open_orders(None).await?;
        let fetched_count = fetched_orders.len();
        let live_orders: Vec<crate::models::LiveOrder> = fetched_orders
            .into_iter()
            .filter(|o| o.status == OrderStatus::Live)
            .filter(|o| o.remaining_size() > Decimal::ZERO)
            .collect();
        let duplicate_live_bid_legs = detect_duplicate_live_bid_legs(&live_orders);

        info!(
            api_returned = fetched_count,
            live_count = live_orders.len(),
            "Open orders fetched from API"
        );

        let market_map: std::collections::HashMap<&str, &CanonicalMarket> = markets
            .iter()
            .map(|m| (m.condition_id.as_str(), m))
            .collect();

        let mut result = OpenOrderSyncResult {
            fetched: fetched_count,
            live: live_orders.len(),
            duplicate_live_bid_legs,
            ..Default::default()
        };
        let mut imported_orders: Vec<TrackedOrder> = Vec::new();
        let mut orders = self.orders.write().await;
        let mut live_legs = HashSet::new();

        for order in &live_orders {
            let remaining = order.remaining_size();
            let leg = quote_leg_from_live_order(order);
            live_legs.insert((order.condition_id.clone(), leg));

            let mut updated_existing = false;
            for market_orders in orders.values_mut() {
                if let Some(existing) = market_orders.get_mut(&order.id) {
                    let before_size = existing.size;
                    let before_matched = existing.matched_size;
                    if before_size != remaining || before_matched != order.size_matched {
                        existing.price = order.price;
                        existing.size = remaining;
                        existing.matched_size = order.size_matched;
                        result.updated += 1;
                        info!(
                            order_id = %order.id,
                            condition_id = %order.condition_id,
                            leg = %existing.leg,
                            remaining_before = %before_size,
                            remaining_after = %existing.size,
                            matched_before = %before_matched,
                            matched_after = %existing.matched_size,
                            "Updated tracked order from global exchange truth"
                        );
                    } else {
                        result.already_tracked += 1;
                    }
                    updated_existing = true;
                    break;
                }
            }
            if updated_existing {
                continue;
            }

            let (neg_risk, tick_size, opposite_token_id) =
                match market_map.get(order.condition_id.as_str()) {
                    Some(m) => {
                        let opposite = if order.asset_id == m.yes_token_id {
                            m.no_token_id.clone()
                        } else {
                            m.yes_token_id.clone()
                        };
                        (m.neg_risk, m.tick_size.clone(), opposite)
                    }
                    None => (false, "0.01".to_string(), String::new()),
                };

            let tracked = tracked_order_from_live_order(
                order,
                leg,
                opposite_token_id,
                neg_risk,
                tick_size,
                remaining,
            );

            info!(
                order_id = %order.id,
                condition_id = %order.condition_id,
                leg = %leg,
                price = %order.price,
                size = %remaining,
                known_market = market_map.contains_key(order.condition_id.as_str()),
                "Synced existing order"
            );

            orders
                .entry(order.condition_id.clone())
                .or_default()
                .insert(order.id.clone(), tracked.clone());

            imported_orders.push(tracked);
            result.imported += 1;
        }

        info!(
            fetched = result.fetched,
            live = result.live,
            imported = result.imported,
            already_tracked = result.already_tracked,
            updated = result.updated,
            duplicate_live_bid_legs = result.duplicate_live_bid_legs.len(),
            "Global open-order sync complete"
        );

        drop(orders);
        self.extend_exchange_legs(live_legs).await;

        for tracked in &imported_orders {
            self.emit_event(emitters::build_order_submitted(
                &self.run_id,
                &tracked.trace_id,
                &self.mode,
                tracked,
                Some("exchange_sync"),
                Some(order_role(tracked)),
            ));
        }

        Ok(result)
    }

    pub async fn sync_market_open_orders(
        &self,
        condition_id: &str,
        market_meta: &CanonicalMarket,
        mode: MarketOrderSyncMode,
    ) -> Result<MarketOrderSyncResult> {
        let fetched_orders = self
            .trading_client
            .get_open_orders(Some(condition_id))
            .await?;
        let fetched_count = fetched_orders.len();
        let live_orders: Vec<crate::models::LiveOrder> = fetched_orders
            .into_iter()
            .filter(|order| order.status == OrderStatus::Live)
            .filter(|order| order.remaining_size() > Decimal::ZERO)
            .collect();

        let mut result = MarketOrderSyncResult {
            fetched: fetched_count,
            live: live_orders.len(),
            duplicate_live_bid_legs: detect_duplicate_live_bid_legs(&live_orders),
            ..Default::default()
        };
        let mut imported_orders = Vec::new();
        let mut pruned_order_ids = Vec::new();
        let live_ids: HashSet<String> = live_orders.iter().map(|order| order.id.clone()).collect();
        let live_legs: HashSet<QuoteLeg> =
            live_orders.iter().map(quote_leg_from_live_order).collect();
        let mut orders = self.orders.write().await;

        {
            let market_orders = orders.entry(condition_id.to_string()).or_default();

            for live_order in &live_orders {
                let remaining = live_order.remaining_size();
                let leg = quote_leg_from_live_order(live_order);
                let opposite_token_id = opposite_token_id_for_order(live_order, market_meta);

                match market_orders.get_mut(&live_order.id) {
                    Some(existing) => {
                        let before_size = existing.size;
                        let before_matched = existing.matched_size;
                        if before_size != remaining || before_matched != live_order.size_matched {
                            existing.price = live_order.price;
                            existing.size = remaining;
                            existing.matched_size = live_order.size_matched;
                            result.updated += 1;
                            info!(
                                order_id = %live_order.id,
                                condition_id = %live_order.condition_id,
                                leg = %existing.leg,
                                remaining_before = %before_size,
                                remaining_after = %existing.size,
                                matched_before = %before_matched,
                                matched_after = %existing.matched_size,
                                "Updated tracked order from exchange truth"
                            );
                        } else {
                            result.already_tracked += 1;
                        }
                    }
                    None => {
                        let tracked = tracked_order_from_live_order(
                            live_order,
                            leg,
                            opposite_token_id,
                            market_meta.neg_risk,
                            market_meta.tick_size.clone(),
                            remaining,
                        );
                        market_orders.insert(live_order.id.clone(), tracked.clone());
                        imported_orders.push(tracked);
                        result.imported += 1;
                        info!(
                            order_id = %live_order.id,
                            condition_id = %live_order.condition_id,
                            leg = %leg,
                            size = %remaining,
                            matched = %live_order.size_matched,
                            "Imported still-live market order from exchange truth"
                        );
                    }
                }
            }

            let stale: Vec<String> = market_orders
                .keys()
                .filter(|order_id| !live_ids.contains(order_id.as_str()))
                .cloned()
                .collect();
            result.missing_order_ids = stale.clone();
            if mode == MarketOrderSyncMode::Reconcile {
                for order_id in stale {
                    if let Some(removed) = market_orders.remove(&order_id) {
                        result.pruned += 1;
                        pruned_order_ids.push(order_id.clone());
                        info!(
                            order_id = %order_id,
                            condition_id = %removed.condition_id,
                            leg = %removed.leg,
                            matched = %removed.matched_size,
                            remaining = %removed.size,
                            "Pruned market order missing from exchange truth"
                        );
                    }
                }
            }
        }

        orders.retain(|_, market_orders| !market_orders.is_empty());
        drop(orders);

        if !pruned_order_ids.is_empty() {
            let mut pending = self.pending_cancel_retries.write().await;
            for order_id in &pruned_order_ids {
                pending.remove(order_id);
            }
        }

        self.replace_exchange_legs_for_market(condition_id, live_legs)
            .await;

        for tracked in &imported_orders {
            self.emit_event(emitters::build_order_submitted(
                &self.run_id,
                &tracked.trace_id,
                &self.mode,
                tracked,
                Some("exchange_sync"),
                Some(order_role(tracked)),
            ));
        }

        info!(
            condition_id = %condition_id,
            fetched = result.fetched,
            live = result.live,
            imported = result.imported,
            already_tracked = result.already_tracked,
            updated = result.updated,
            pruned = result.pruned,
            missing = result.missing_order_ids.len(),
            duplicate_live_bid_legs = result.duplicate_live_bid_legs.len(),
            mode = ?mode,
            "Market open-order sync complete"
        );

        Ok(result)
    }

    /// Sum of (price * size) for all resting BUY orders = USDC locked up.
    pub async fn committed_capital(&self) -> Decimal {
        let orders = self.orders.read().await;
        let mut total = Decimal::ZERO;
        for market_orders in orders.values() {
            for order in market_orders.values() {
                if order.side == Side::Buy {
                    total += order.price * order.size;
                }
            }
        }
        total
    }

    /// Full notional exposure for resting BUY orders (bid + hedge reserve).
    pub async fn committed_exposure(&self) -> Decimal {
        let orders = self.orders.read().await;
        let mut total = Decimal::ZERO;
        for market_orders in orders.values() {
            for order in market_orders.values() {
                if order.side == Side::Buy {
                    total += order.size;
                }
            }
        }
        total
    }

    /// Update gross balance when a fresh API balance is fetched.
    ///
    /// The balance-allowance API returns total USDC (free + collateral locked
    /// in resting orders), so gross_balance = api_balance directly.
    pub async fn update_gross_balance(&self, api_balance: Decimal) {
        *self.gross_balance.write().await = api_balance;
    }

    /// How much USDC budget remains for new BUY orders.
    ///
    /// Formula: gross_balance - committed_exposure - cash_reserve
    /// where gross_balance = api_balance (total USDC from API, includes locked collateral)
    ///       committed_exposure = sum(size) for resting BUY orders
    ///       cash_reserve = USDC amount to always keep untouched
    ///
    /// In binary markets each buy share costs exactly $1 total:
    ///   order_cost (price) + hedge_cost (1 - price) = 1
    /// So committed_exposure (total shares) = total USDC commitment.
    pub async fn available_budget(&self) -> Decimal {
        let gross = *self.gross_balance.read().await;
        let total_commitment = self.committed_exposure().await;
        (gross - total_commitment - self.cash_reserve).max(Decimal::ZERO)
    }

    /// Free USDC available for emergency hedge resolution.
    ///
    /// Unlike `available_budget()`, this does not subtract the normal quoting reserve.
    pub async fn available_hedge_resolution_usdc(&self) -> Decimal {
        let gross = *self.gross_balance.read().await;
        let total_commitment = self.committed_exposure().await;
        (gross - total_commitment).max(Decimal::ZERO)
    }

    fn emit_event(&self, event: EventEnvelope) {
        let Some(producer) = &self.event_producer else {
            return;
        };

        match producer.emit(event) {
            Ok(true) => {}
            Ok(false) => warn!("Dropping order event: monitor queue is full"),
            Err(err) => warn!(error = %err, "Failed to enqueue order event"),
        }
    }

    async fn remove_active_order_from_tracking(&self, order_id: &str) -> Option<TrackedOrder> {
        let mut orders = self.orders.write().await;
        let mut removed = None;

        for market_orders in orders.values_mut() {
            if let Some(tracked) = market_orders.remove(order_id) {
                removed = Some(tracked);
                break;
            }
        }

        orders.retain(|_, market_orders| !market_orders.is_empty());
        removed
    }

    async fn record_pending_cancel(
        &self,
        tracked: &TrackedOrder,
        reason_code: CancelReasonCode,
        origin: &'static str,
    ) {
        let now = Instant::now();
        let mut pending = self.pending_cancel_retries.write().await;
        pending
            .entry(tracked.order_id.clone())
            .and_modify(|entry| {
                entry.tracked = tracked.clone();
                entry.reason_code = reason_code;
                entry.origin = origin;
                entry.last_attempted_at = now;
                entry.attempts += 1;
            })
            .or_insert_with(|| PendingCancelRetry {
                tracked: tracked.clone(),
                reason_code,
                origin,
                first_attempted_at: now,
                last_attempted_at: now,
                attempts: 1,
            });
    }

    async fn finalize_confirmed_cancel(
        &self,
        tracked: &TrackedOrder,
        reason_code: CancelReasonCode,
        origin: &'static str,
        diagnostics: Option<&OrderEventDiagnostics>,
    ) {
        self.pending_cancel_retries
            .write()
            .await
            .remove(&tracked.order_id);

        let cancelled = self
            .remove_active_order_from_tracking(&tracked.order_id)
            .await
            .unwrap_or_else(|| tracked.clone());

        self.record_recently_cancelled(cancelled.clone()).await;

        self.emit_event(emitters::build_order_cancelled(
            &self.run_id,
            &cancelled.trace_id,
            &self.mode,
            &cancelled,
            reason_code,
            Some(origin),
            diagnostics,
        ));
    }

    async fn apply_cancel_outcome(
        &self,
        tracked: &TrackedOrder,
        outcome: CancelOrderOutcome,
        reason_code: CancelReasonCode,
        origin: &'static str,
        diagnostics: Option<&OrderEventDiagnostics>,
    ) -> CancelOrderOutcome {
        match &outcome {
            CancelOrderOutcome::Confirmed => {
                self.finalize_confirmed_cancel(tracked, reason_code, origin, diagnostics)
                    .await;
            }
            CancelOrderOutcome::Rejected(reason) => {
                self.record_pending_cancel(tracked, reason_code, origin)
                    .await;
                warn!(
                    order_id = %tracked.order_id,
                    condition_id = %tracked.condition_id,
                    leg = %tracked.leg,
                    reason = %reason,
                    "Cancel rejected — keeping order tracked for retry"
                );
            }
            CancelOrderOutcome::Unknown(reason) => {
                self.record_pending_cancel(tracked, reason_code, origin)
                    .await;
                warn!(
                    order_id = %tracked.order_id,
                    condition_id = %tracked.condition_id,
                    leg = %tracked.leg,
                    reason = %reason,
                    "Cancel unverified — keeping order tracked for retry"
                );
            }
        }

        outcome
    }

    pub async fn cancel_tracked_order(
        &self,
        tracked: &TrackedOrder,
        reason_code: CancelReasonCode,
        origin: &'static str,
    ) -> CancelOrderOutcome {
        self.cancel_tracked_order_with_diagnostics(tracked, reason_code, origin, None)
            .await
    }

    pub async fn cancel_tracked_order_with_diagnostics(
        &self,
        tracked: &TrackedOrder,
        reason_code: CancelReasonCode,
        origin: &'static str,
        diagnostics: Option<&OrderEventDiagnostics>,
    ) -> CancelOrderOutcome {
        let outcome = match self.trading_client.cancel_order(&tracked.order_id).await {
            Ok(outcome) => outcome,
            Err(err) => CancelOrderOutcome::Unknown(format!("cancel request failed: {}", err)),
        };

        self.apply_cancel_outcome(tracked, outcome, reason_code, origin, diagnostics)
            .await
    }

    pub async fn retry_pending_cancels(&self) -> usize {
        let retry_backoff = StdDuration::from_secs(PENDING_CANCEL_RETRY_BACKOFF_SECS);
        let now = Instant::now();
        let pending: Vec<PendingCancelRetry> = self
            .pending_cancel_retries
            .read()
            .await
            .values()
            .filter(|entry| instant_age_at_least(now, entry.last_attempted_at, retry_backoff))
            .cloned()
            .collect();

        let mut confirmed = 0usize;
        for entry in pending {
            let outcome = match self
                .trading_client
                .cancel_order(&entry.tracked.order_id)
                .await
            {
                Ok(outcome) => outcome,
                Err(err) => CancelOrderOutcome::Unknown(format!("cancel retry failed: {}", err)),
            };

            if matches!(
                self.apply_cancel_outcome(
                    &entry.tracked,
                    outcome,
                    entry.reason_code,
                    entry.origin,
                    None,
                )
                .await,
                CancelOrderOutcome::Confirmed
            ) {
                confirmed += 1;
            }
        }

        confirmed
    }

    pub async fn market_order_state_counts(&self, condition_id: &str) -> (usize, usize) {
        let active = self
            .orders
            .read()
            .await
            .get(condition_id)
            .map(|orders| orders.len())
            .unwrap_or_default();
        let pending = self
            .pending_cancel_retries
            .read()
            .await
            .values()
            .filter(|entry| entry.tracked.condition_id == condition_id)
            .count();
        (active, pending)
    }

    pub async fn market_bid_order_state_counts(&self, condition_id: &str) -> (usize, usize) {
        let active = self
            .orders
            .read()
            .await
            .get(condition_id)
            .map(|orders| orders.values().filter(|order| order.leg.is_bid()).count())
            .unwrap_or_default();
        let pending = self
            .pending_cancel_retries
            .read()
            .await
            .values()
            .filter(|entry| {
                entry.tracked.condition_id == condition_id && entry.tracked.leg.is_bid()
            })
            .count();
        (active, pending)
    }

    pub async fn global_bid_order_state_counts_excluding(
        &self,
        excluded_condition_id: &str,
    ) -> (usize, usize) {
        let active = self
            .orders
            .read()
            .await
            .iter()
            .filter(|(condition_id, _)| condition_id.as_str() != excluded_condition_id)
            .map(|(_, orders)| orders.values().filter(|order| order.leg.is_bid()).count())
            .sum();
        let pending = self
            .pending_cancel_retries
            .read()
            .await
            .values()
            .filter(|entry| {
                entry.tracked.condition_id != excluded_condition_id && entry.tracked.leg.is_bid()
            })
            .count();
        (active, pending)
    }

    pub async fn has_orders_or_pending_cancels(&self, condition_id: &str) -> bool {
        let (active, pending) = self.market_order_state_counts(condition_id).await;
        active > 0 || pending > 0
    }

    pub async fn has_bid_orders_or_pending_cancels(&self, condition_id: &str) -> bool {
        let (active, pending) = self.market_bid_order_state_counts(condition_id).await;
        active > 0 || pending > 0
    }

    async fn place_candidate(
        &self,
        market: &CanonicalMarket,
        candidate: &crate::models::QuoteCandidate,
        position: Option<&Position>,
        min_order_size: Decimal,
        trace_id: Option<String>,
        created_at: Option<DateTime<Utc>>,
        origin: &'static str,
        role: Option<&'static str>,
    ) -> Result<Option<TrackedOrder>> {
        if candidate.status != QuoteStatus::Approved {
            return Ok(None);
        }

        {
            let orders = self.orders.read().await;
            let already_has_leg = orders
                .get(&market.condition_id)
                .map(|m| m.values().any(|o| o.leg == candidate.leg))
                .unwrap_or(false);
            if already_has_leg {
                return Ok(None);
            }
        }
        if self
            .has_exchange_live_leg(&market.condition_id, candidate.leg)
            .await
        {
            warn!(
                condition_id = %market.condition_id,
                leg = %candidate.leg,
                "Leg exists on exchange or is conservatively reserved — skipping placement"
            );
            return Ok(None);
        }

        let (token_id, side) = leg_to_order_params(candidate.leg, market);

        if side == Side::Sell {
            let sellable = sellable_inventory_for_ask(candidate.leg, position, origin);
            if sellable < candidate.size {
                info!(
                    condition_id = %market.condition_id,
                    leg = %candidate.leg,
                    sellable = %sellable,
                    requested = %candidate.size,
                    "Skipping ask: no excess inventory above hedge"
                );
                return Ok(None);
            }
        }

        let mut effective_size = candidate.size;
        if side == Side::Buy {
            let available_budget = self.available_budget().await;
            match cap_buy_size_to_budget(candidate.size, available_budget, min_order_size) {
                Some(capped_size) => {
                    if capped_size < candidate.size {
                        info!(
                            condition_id = %market.condition_id,
                            leg = %candidate.leg,
                            requested_exposure = %candidate.size,
                            available_budget = %available_budget,
                            capped_size = %capped_size,
                            "Capping bid size to hedge-aware full-exposure budget"
                        );
                        effective_size = capped_size;
                    }
                }
                None => {
                    info!(
                        condition_id = %market.condition_id,
                        leg = %candidate.leg,
                        requested_exposure = %candidate.size,
                        available_budget = %available_budget,
                        whole_share_budget = %whole_share_budget_limit(available_budget),
                        "Skipping bid: insufficient hedge-aware full-exposure budget"
                    );
                    return Ok(None);
                }
            }
        }

        let request = OrderRequest {
            token_id: token_id.clone(),
            price: candidate.price,
            size: effective_size,
            amount_kind: OrderAmountKind::Shares,
            side,
            order_type: OrderType::GTC,
            post_only: true,
            neg_risk: market.neg_risk,
            tick_size: market.tick_size.clone(),
        };

        match self.trading_client.place_order(&request).await {
            Ok(result) => {
                info!(
                    condition_id = %market.condition_id,
                    leg = %candidate.leg,
                    order_id = %result.order_id,
                    price = %candidate.price,
                    requested_size = %candidate.size,
                    placed_size = %effective_size,
                    "Passive order placed"
                );

                let opposite_token_id = match candidate.leg {
                    QuoteLeg::YesBid | QuoteLeg::YesAsk => market.no_token_id.clone(),
                    QuoteLeg::NoBid | QuoteLeg::NoAsk => market.yes_token_id.clone(),
                };
                let tracked = build_tracked_order(
                    result.order_id.clone(),
                    trace_id.unwrap_or_else(|| Uuid::new_v4().to_string()),
                    created_at.unwrap_or_else(Utc::now),
                    market.condition_id.clone(),
                    candidate.leg,
                    token_id,
                    opposite_token_id,
                    side,
                    candidate.price,
                    effective_size,
                    Decimal::ZERO,
                    market.neg_risk,
                    market.tick_size.clone(),
                );

                self.orders
                    .write()
                    .await
                    .entry(market.condition_id.clone())
                    .or_default()
                    .insert(result.order_id, tracked.clone());
                self.remember_exchange_leg(&tracked.condition_id, tracked.leg)
                    .await;

                self.emit_event(emitters::build_order_submitted(
                    &self.run_id,
                    &tracked.trace_id,
                    &self.mode,
                    &tracked,
                    Some(origin),
                    Some(role.unwrap_or(order_role(&tracked))),
                ));

                Ok(Some(tracked))
            }
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("crosses book") {
                    warn!(
                        condition_id = %market.condition_id,
                        leg = %candidate.leg,
                        "Post-only order crossed book — will retry next cycle"
                    );
                } else {
                    error!(
                        condition_id = %market.condition_id,
                        leg = %candidate.leg,
                        error = %e,
                        "Failed to place order"
                    );
                }
                Ok(None)
            }
        }
    }

    /// Place passive limit orders for approved quote legs.
    /// Bids are placed unconditionally; asks only if we have inventory.
    /// BUY orders are capped to available budget (api_balance - hedge_reserve).
    pub async fn place_quotes(
        &self,
        market: &CanonicalMarket,
        quote_set: &QuoteSet,
        position: Option<&Position>,
        min_order_size: Decimal,
        trace_ids: Option<&HashMap<QuoteLeg, String>>,
        origin: &'static str,
        role: Option<&'static str>,
    ) -> Result<Vec<TrackedOrder>> {
        let mut results = Vec::new();

        for candidate in &quote_set.candidates {
            let trace_id = trace_ids.and_then(|ids| ids.get(&candidate.leg)).cloned();
            if let Some(tracked) = self
                .place_candidate(
                    market,
                    candidate,
                    position,
                    min_order_size,
                    trace_id,
                    None,
                    origin,
                    role,
                )
                .await?
            {
                results.push(tracked);
            }
        }

        Ok(results)
    }

    /// Cancel-replace orders that have drifted beyond threshold.
    pub async fn cancel_replace_if_drifted(
        &self,
        market: &CanonicalMarket,
        new_quote_set: &QuoteSet,
        drift_threshold_bps: Decimal,
        position: Option<&Position>,
        min_order_size: Decimal,
        trace_ids: Option<&HashMap<QuoteLeg, String>>,
        origin: &'static str,
        role: Option<&'static str>,
    ) -> Result<()> {
        let existing = {
            let orders = self.orders.read().await;
            match orders.get(&market.condition_id) {
                Some(o) if !o.is_empty() => o.clone(),
                _ => return Ok(()),
            }
        };

        let mut to_cancel = Vec::new();
        let mut legs_to_place = Vec::new();

        for candidate in &new_quote_set.candidates {
            if candidate.status != QuoteStatus::Approved {
                continue;
            }

            let current = existing.values().find(|o| o.leg == candidate.leg);

            match current {
                Some(order) => {
                    let price_drift = if candidate.price > Decimal::ZERO {
                        ((order.price - candidate.price).abs() / candidate.price)
                            * Decimal::from(10000)
                    } else {
                        Decimal::ZERO
                    };

                    // Size drift: cancel-replace if existing size differs by >50%
                    let size_drift = if candidate.size > Decimal::ZERO {
                        ((order.size - candidate.size).abs() / candidate.size) * Decimal::from(100)
                    } else {
                        Decimal::ZERO
                    };
                    let size_threshold = Decimal::from(50); // 50%

                    if price_drift > drift_threshold_bps {
                        info!(
                            condition_id = %market.condition_id,
                            leg = %candidate.leg,
                            old_price = %order.price,
                            new_price = %candidate.price,
                            drift_bps = %price_drift,
                            "Quote price drifted, cancel-replacing"
                        );
                        to_cancel.push(order.clone());
                        legs_to_place.push(candidate.clone());
                    } else if size_drift > size_threshold {
                        info!(
                            condition_id = %market.condition_id,
                            leg = %candidate.leg,
                            old_size = %order.size,
                            new_size = %candidate.size,
                            size_drift_pct = %size_drift,
                            "Quote size drifted, cancel-replacing"
                        );
                        to_cancel.push(order.clone());
                        legs_to_place.push(candidate.clone());
                    }
                }
                None => {
                    // Check if this leg already exists on the exchange (from last sync).
                    // Prevents duplicates when tracking is incomplete.
                    let exchange = self.exchange_legs.read().await;
                    if exchange.contains(&(market.condition_id.clone(), candidate.leg)) {
                        warn!(
                            condition_id = %market.condition_id,
                            leg = %candidate.leg,
                            "Leg exists on exchange but not tracked — skipping placement"
                        );
                        continue;
                    }
                    legs_to_place.push(candidate.clone());
                }
            }
        }

        // Cancel drifted orders
        let mut cancel_failed = false;
        for tracked in &to_cancel {
            if !matches!(
                self.cancel_tracked_order(tracked, CancelReasonCode::QuoteDrift, origin)
                    .await,
                CancelOrderOutcome::Confirmed
            ) {
                cancel_failed = true;
            }
        }

        // Don't place replacements if any cancel failed — avoids duplicate orders
        if cancel_failed {
            return Ok(());
        }

        // Place replacements
        for candidate in &legs_to_place {
            let inherited_trace_id = existing
                .values()
                .find(|order| order.leg == candidate.leg)
                .map(|order| order.trace_id.clone())
                .or_else(|| trace_ids.and_then(|ids| ids.get(&candidate.leg)).cloned());

            if let Some(new_order) = self
                .place_candidate(
                    market,
                    candidate,
                    position,
                    min_order_size,
                    inherited_trace_id,
                    existing
                        .values()
                        .find(|order| order.leg == candidate.leg)
                        .map(|order| order.created_at),
                    origin,
                    role,
                )
                .await?
            {
                if let Some(old_order) = existing.values().find(|order| order.leg == candidate.leg)
                {
                    self.emit_event(emitters::build_order_resized(
                        &self.run_id,
                        &old_order.trace_id,
                        &self.mode,
                        old_order,
                        &new_order,
                        CancelReasonCode::QuoteDrift,
                        Some(origin),
                        None,
                    ));
                }
            }
        }

        Ok(())
    }

    /// Cancel all orders for a market.
    pub async fn cancel_all(
        &self,
        condition_id: &str,
        reason_code: CancelReasonCode,
        origin: &'static str,
    ) -> Result<()> {
        self.cancel_all_with_diagnostics(condition_id, reason_code, origin, None)
            .await
    }

    pub async fn cancel_all_with_diagnostics(
        &self,
        condition_id: &str,
        reason_code: CancelReasonCode,
        origin: &'static str,
        diagnostics: Option<&OrderEventDiagnostics>,
    ) -> Result<()> {
        let tracked = self.get_market_orders(condition_id).await;
        if tracked.is_empty() {
            return Ok(());
        }

        self.cancel_orders_snapshot(&tracked, reason_code, origin, diagnostics)
            .await;

        let (active, pending) = self.market_order_state_counts(condition_id).await;
        if active == 0 && pending == 0 {
            info!(
                condition_id = %condition_id,
                count = tracked.len(),
                "All market orders cancelled"
            );
        } else {
            warn!(
                condition_id = %condition_id,
                attempted = tracked.len(),
                active_remaining = active,
                pending_verification = pending,
                "Market cancel incomplete — orders remain tracked for retry"
            );
        }
        Ok(())
    }

    /// Cancel all resting bid orders outside the specified market.
    pub async fn cancel_other_bids_with_diagnostics(
        &self,
        excluded_condition_id: &str,
        reason_code: CancelReasonCode,
        origin: &'static str,
        diagnostics: Option<&OrderEventDiagnostics>,
    ) -> Result<()> {
        let bid_orders: Vec<TrackedOrder> = self
            .get_all_orders()
            .await
            .into_iter()
            .filter(|order| order.condition_id != excluded_condition_id && order.leg.is_bid())
            .collect();

        if bid_orders.is_empty() {
            return Ok(());
        }

        self.cancel_orders_snapshot(&bid_orders, reason_code, origin, diagnostics)
            .await;

        let (active, pending) = self
            .global_bid_order_state_counts_excluding(excluded_condition_id)
            .await;
        if active == 0 && pending == 0 {
            info!(
                excluded_condition_id = %excluded_condition_id,
                attempted = bid_orders.len(),
                "Global external bid cancellation pass drained cleanly"
            );
        } else {
            warn!(
                excluded_condition_id = %excluded_condition_id,
                attempted = bid_orders.len(),
                active_remaining = active,
                pending_verification = pending,
                "Global external bid cancellation pass incomplete"
            );
        }
        Ok(())
    }

    /// Look up a tracked order by order_id (checks active + recently-cancelled).
    pub async fn get_tracked_order(&self, order_id: &str) -> Option<TrackedOrder> {
        let orders = self.orders.read().await;
        for market_orders in orders.values() {
            if let Some(tracked) = market_orders.get(order_id) {
                return Some(tracked.clone());
            }
        }
        drop(orders);
        // Also check recently cancelled orders (grace period for in-flight fills)
        let cancelled = self.recently_cancelled.read().await;
        cancelled.get(order_id).map(|(t, _)| t.clone())
    }

    /// Remove an order from tracking (e.g. after fill or external cancel).
    pub async fn remove_order(&self, order_id: &str) -> Option<TrackedOrder> {
        if let Some(tracked) = self.remove_active_order_from_tracking(order_id).await {
            self.pending_cancel_retries.write().await.remove(order_id);
            self.forget_exchange_leg_if_no_active_order(&tracked.condition_id, tracked.leg)
                .await;
            return Some(tracked);
        }
        // Also remove from recently cancelled buffer
        self.pending_cancel_retries.write().await.remove(order_id);
        let removed = self
            .recently_cancelled
            .write()
            .await
            .remove(order_id)
            .map(|(tracked, _)| tracked);
        if let Some(tracked) = &removed {
            self.forget_exchange_leg_if_no_active_order(&tracked.condition_id, tracked.leg)
                .await;
        }
        removed
    }

    pub async fn apply_trade_fill(
        &self,
        order_id: &str,
        fill_size: Decimal,
    ) -> Option<MatchUpdate> {
        if fill_size <= Decimal::ZERO {
            return None;
        }

        let active_result = {
            let mut orders = self.orders.write().await;
            let mut result = None;
            let mut fully_matched = None;

            for market_orders in orders.values_mut() {
                if let Some(existing) = market_orders.get_mut(order_id) {
                    let before = existing.clone();
                    let newly_matched = fill_size.min(before.size.max(Decimal::ZERO));
                    if newly_matched <= Decimal::ZERO {
                        result = Some(MatchUpdate {
                            tracked_before: before.clone(),
                            tracked_after: Some(before),
                            newly_matched: Decimal::ZERO,
                        });
                        break;
                    }

                    let new_remaining = (before.size - newly_matched).max(Decimal::ZERO);
                    existing.size = new_remaining;
                    existing.matched_size = before.matched_size + newly_matched;
                    let after = existing.clone();

                    if new_remaining <= Decimal::ZERO {
                        let removed = market_orders.remove(order_id).unwrap_or(after.clone());
                        fully_matched = Some(removed);
                        result = Some(MatchUpdate {
                            tracked_before: before,
                            tracked_after: None,
                            newly_matched,
                        });
                    } else {
                        result = Some(MatchUpdate {
                            tracked_before: before,
                            tracked_after: Some(after),
                            newly_matched,
                        });
                    }
                    break;
                }
            }

            (result, fully_matched)
        };

        if let Some(removed) = active_result.1 {
            self.pending_cancel_retries.write().await.remove(order_id);
            self.record_recently_cancelled(removed).await;
        }
        if let Some(result) = active_result.0 {
            return Some(result);
        }

        let mut recent = self.recently_cancelled.write().await;
        if let Some((existing, ts)) = recent.get_mut(order_id) {
            let before = existing.clone();
            let newly_matched = fill_size.min(before.size.max(Decimal::ZERO));
            if newly_matched <= Decimal::ZERO {
                return Some(MatchUpdate {
                    tracked_before: before.clone(),
                    tracked_after: Some(before),
                    newly_matched: Decimal::ZERO,
                });
            }

            existing.size = (before.size - newly_matched).max(Decimal::ZERO);
            existing.matched_size = before.matched_size + newly_matched;
            *ts = Instant::now();
            let after = existing.clone();
            return Some(MatchUpdate {
                tracked_before: before,
                tracked_after: Some(after),
                newly_matched,
            });
        }

        None
    }

    pub async fn apply_order_update(
        &self,
        order_id: &str,
        cumulative_matched: Decimal,
    ) -> Option<MatchUpdate> {
        if cumulative_matched < Decimal::ZERO {
            return None;
        }

        let active_result = {
            let mut orders = self.orders.write().await;
            let mut result = None;
            let mut fully_matched = None;

            for market_orders in orders.values_mut() {
                if let Some(existing) = market_orders.get_mut(order_id) {
                    let before = existing.clone();
                    if cumulative_matched <= before.matched_size {
                        result = Some(MatchUpdate {
                            tracked_before: before.clone(),
                            tracked_after: Some(before),
                            newly_matched: Decimal::ZERO,
                        });
                        break;
                    }

                    let newly_matched = (cumulative_matched - before.matched_size)
                        .min(before.size.max(Decimal::ZERO));
                    let new_remaining = (before.size - newly_matched).max(Decimal::ZERO);
                    existing.size = new_remaining;
                    existing.matched_size = before.matched_size + newly_matched;
                    let after = existing.clone();

                    if new_remaining <= Decimal::ZERO {
                        let removed = market_orders.remove(order_id).unwrap_or(after.clone());
                        fully_matched = Some(removed);
                        result = Some(MatchUpdate {
                            tracked_before: before,
                            tracked_after: None,
                            newly_matched,
                        });
                    } else {
                        result = Some(MatchUpdate {
                            tracked_before: before,
                            tracked_after: Some(after),
                            newly_matched,
                        });
                    }
                    break;
                }
            }

            (result, fully_matched)
        };

        if let Some(removed) = active_result.1 {
            self.pending_cancel_retries.write().await.remove(order_id);
            self.record_recently_cancelled(removed).await;
        }
        if let Some(result) = active_result.0 {
            return Some(result);
        }

        let mut recent = self.recently_cancelled.write().await;
        if let Some((existing, ts)) = recent.get_mut(order_id) {
            let before = existing.clone();
            if cumulative_matched <= before.matched_size {
                return Some(MatchUpdate {
                    tracked_before: before.clone(),
                    tracked_after: Some(before),
                    newly_matched: Decimal::ZERO,
                });
            }

            let newly_matched =
                (cumulative_matched - before.matched_size).min(before.size.max(Decimal::ZERO));
            existing.size = (before.size - newly_matched).max(Decimal::ZERO);
            existing.matched_size = before.matched_size + newly_matched;
            *ts = Instant::now();
            let after = existing.clone();
            return Some(MatchUpdate {
                tracked_before: before,
                tracked_after: Some(after),
                newly_matched,
            });
        }

        None
    }

    /// Get recently cancelled orders for a market (for fill-matching fallback).
    pub async fn get_recently_cancelled_for_market(&self, condition_id: &str) -> Vec<TrackedOrder> {
        self.recently_cancelled
            .read()
            .await
            .values()
            .filter(|(t, _)| t.condition_id == condition_id)
            .map(|(t, _)| t.clone())
            .collect()
    }

    /// Move an order from active tracking to recently-cancelled buffer.
    ///
    /// Public so the WS cancellation handler can use grace-period matching
    /// instead of hard-deleting (prevents race with FillHandler task).
    pub async fn move_to_recently_cancelled(&self, order_id: &str) {
        let tracked = self.remove_active_order_from_tracking(order_id).await;
        if let Some(tracked) = tracked {
            self.pending_cancel_retries.write().await.remove(order_id);
            self.record_recently_cancelled(tracked).await;
        }
    }

    /// Remove entries older than 30 seconds from the recently-cancelled buffer.
    pub async fn cleanup_stale_cancels(&self) {
        let mut recently_cancelled = self.recently_cancelled.write().await;
        prune_recently_cancelled_entries(
            &mut recently_cancelled,
            Instant::now(),
            StdDuration::from_secs(30),
        );
    }

    /// Get all tracked order IDs across all markets.
    pub async fn get_tracked_order_ids(&self) -> Vec<String> {
        self.orders
            .read()
            .await
            .values()
            .flat_map(|m| m.keys().cloned())
            .collect()
    }

    pub async fn get_all_orders(&self) -> Vec<TrackedOrder> {
        self.orders
            .read()
            .await
            .values()
            .flat_map(|market_orders| market_orders.values().cloned())
            .collect()
    }

    /// Cancel only bid-side orders for a market (preserves asks for inventory rewards).
    pub async fn cancel_bids_only(
        &self,
        condition_id: &str,
        reason_code: CancelReasonCode,
        origin: &'static str,
    ) -> Result<()> {
        self.cancel_bids_only_with_diagnostics(condition_id, reason_code, origin, None)
            .await
    }

    pub async fn cancel_bids_only_with_diagnostics(
        &self,
        condition_id: &str,
        reason_code: CancelReasonCode,
        origin: &'static str,
        diagnostics: Option<&OrderEventDiagnostics>,
    ) -> Result<()> {
        let bid_orders: Vec<TrackedOrder> = self
            .get_market_orders(condition_id)
            .await
            .into_iter()
            .filter(|o| o.leg.is_bid())
            .collect();

        self.cancel_orders_snapshot(&bid_orders, reason_code, origin, diagnostics)
            .await;

        if !bid_orders.is_empty() {
            let (active, pending) = self.market_order_state_counts(condition_id).await;
            info!(
                condition_id = %condition_id,
                attempted = bid_orders.len(),
                active_remaining = active,
                pending_verification = pending,
                "Bid-order cancellation pass complete"
            );
        }
        Ok(())
    }

    /// Cancel only ask-side orders for a market (preserves bids).
    pub async fn cancel_asks_only(
        &self,
        condition_id: &str,
        reason_code: CancelReasonCode,
        origin: &'static str,
    ) -> Result<()> {
        self.cancel_asks_only_with_diagnostics(condition_id, reason_code, origin, None)
            .await
    }

    pub async fn cancel_asks_only_with_diagnostics(
        &self,
        condition_id: &str,
        reason_code: CancelReasonCode,
        origin: &'static str,
        diagnostics: Option<&OrderEventDiagnostics>,
    ) -> Result<()> {
        let ask_orders: Vec<TrackedOrder> = self
            .get_market_orders(condition_id)
            .await
            .into_iter()
            .filter(|o| o.leg.is_ask())
            .collect();

        self.cancel_orders_snapshot(&ask_orders, reason_code, origin, diagnostics)
            .await;

        if !ask_orders.is_empty() {
            let (active, pending) = self.market_order_state_counts(condition_id).await;
            info!(
                condition_id = %condition_id,
                attempted = ask_orders.len(),
                active_remaining = active,
                pending_verification = pending,
                "Ask-order cancellation pass complete"
            );
        }
        Ok(())
    }

    /// Cancel resting orders for a specific leg only (e.g. just YES_BID).
    /// Leaves other legs (including other bids) untouched.
    pub async fn cancel_leg(
        &self,
        condition_id: &str,
        leg: QuoteLeg,
        reason_code: CancelReasonCode,
        origin: &'static str,
    ) -> Result<()> {
        self.cancel_leg_with_diagnostics(condition_id, leg, reason_code, origin, None)
            .await
    }

    pub async fn cancel_leg_with_diagnostics(
        &self,
        condition_id: &str,
        leg: QuoteLeg,
        reason_code: CancelReasonCode,
        origin: &'static str,
        diagnostics: Option<&OrderEventDiagnostics>,
    ) -> Result<()> {
        let target_orders: Vec<TrackedOrder> = self
            .get_market_orders(condition_id)
            .await
            .into_iter()
            .filter(|o| o.leg == leg)
            .collect();

        self.cancel_orders_snapshot(&target_orders, reason_code, origin, diagnostics)
            .await;

        if !target_orders.is_empty() {
            let (active, pending) = self.market_order_state_counts(condition_id).await;
            info!(
                condition_id = %condition_id,
                leg = %leg,
                attempted = target_orders.len(),
                active_remaining = active,
                pending_verification = pending,
                "Leg-order cancellation pass complete"
            );
        }
        Ok(())
    }

    /// Cancel and repost an order at a new size.
    pub async fn resize_order(
        &self,
        order_id: &str,
        new_size: Decimal,
        reason_code: CancelReasonCode,
        origin: &'static str,
    ) -> Result<Option<TrackedOrder>> {
        self.resize_order_with_diagnostics(order_id, new_size, reason_code, origin, None)
            .await
    }

    pub async fn resize_order_with_diagnostics(
        &self,
        order_id: &str,
        new_size: Decimal,
        reason_code: CancelReasonCode,
        origin: &'static str,
        diagnostics: Option<&OrderEventDiagnostics>,
    ) -> Result<Option<TrackedOrder>> {
        let existing = self.get_tracked_order(order_id).await;
        let existing = match existing {
            Some(o) => o,
            None => return Ok(None),
        };

        if !matches!(
            self.cancel_tracked_order_with_diagnostics(&existing, reason_code, origin, diagnostics)
                .await,
            CancelOrderOutcome::Confirmed
        ) {
            warn!(
                order_id = %order_id,
                condition_id = %existing.condition_id,
                "Resize aborted: original order cancel was not verified"
            );
            return Ok(None);
        }

        // Place replacement at new size
        let request = OrderRequest {
            token_id: existing.token_id.clone(),
            price: existing.price,
            size: new_size,
            amount_kind: OrderAmountKind::Shares,
            side: existing.side,
            order_type: OrderType::GTC,
            post_only: true,
            neg_risk: existing.neg_risk,
            tick_size: existing.tick_size.clone(),
        };

        let result = self.trading_client.place_order(&request).await?;

        info!(
            condition_id = %existing.condition_id,
            old_order = %order_id,
            new_order = %result.order_id,
            old_size = %existing.size,
            new_size = %new_size,
            "Order resized"
        );

        let tracked = TrackedOrder {
            order_id: result.order_id.clone(),
            trace_id: existing.trace_id.clone(),
            condition_id: existing.condition_id.clone(),
            created_at: existing.created_at,
            leg: existing.leg,
            token_id: existing.token_id.clone(),
            opposite_token_id: existing.opposite_token_id.clone(),
            side: existing.side,
            price: existing.price,
            size: new_size,
            matched_size: Decimal::ZERO,
            neg_risk: existing.neg_risk,
            tick_size: existing.tick_size.clone(),
        };

        self.orders
            .write()
            .await
            .entry(existing.condition_id.clone())
            .or_default()
            .insert(result.order_id, tracked.clone());
        self.remember_exchange_leg(&tracked.condition_id, tracked.leg)
            .await;

        self.emit_event(emitters::build_order_submitted(
            &self.run_id,
            &tracked.trace_id,
            &self.mode,
            &tracked,
            Some(origin),
            Some(order_role(&tracked)),
        ));
        self.emit_event(emitters::build_order_resized(
            &self.run_id,
            &existing.trace_id,
            &self.mode,
            &existing,
            &tracked,
            reason_code,
            Some(origin),
            diagnostics,
        ));

        Ok(Some(tracked))
    }

    /// Get all tracked orders for a market.
    pub async fn get_market_orders(&self, condition_id: &str) -> Vec<TrackedOrder> {
        self.orders
            .read()
            .await
            .get(condition_id)
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default()
    }

    /// Get all tracked condition_ids.
    pub async fn get_tracked_condition_ids(&self) -> Vec<String> {
        self.orders.read().await.keys().cloned().collect()
    }

    pub async fn has_pending_cancel_retries(&self) -> bool {
        !self.pending_cancel_retries.read().await.is_empty()
    }

    async fn cancel_orders_snapshot(
        &self,
        tracked_orders: &[TrackedOrder],
        reason_code: CancelReasonCode,
        origin: &'static str,
        diagnostics: Option<&OrderEventDiagnostics>,
    ) {
        let cancel_futures = tracked_orders.iter().map(|tracked| {
            self.cancel_tracked_order_with_diagnostics(tracked, reason_code, origin, diagnostics)
        });
        let _ = join_all(cancel_futures).await;
    }

    #[cfg(test)]
    pub async fn seed_pending_cancel_for_test(
        &self,
        tracked: TrackedOrder,
        reason_code: CancelReasonCode,
        origin: &'static str,
    ) {
        let now = Instant::now();
        self.pending_cancel_retries.write().await.insert(
            tracked.order_id.clone(),
            PendingCancelRetry {
                tracked,
                reason_code,
                origin,
                first_attempted_at: now,
                last_attempted_at: now,
                attempts: 1,
            },
        );
    }

    #[cfg(test)]
    pub async fn seed_live_order_for_test(&self, tracked: TrackedOrder) {
        self.orders
            .write()
            .await
            .entry(tracked.condition_id.clone())
            .or_default()
            .insert(tracked.order_id.clone(), tracked.clone());
        self.remember_exchange_leg(&tracked.condition_id, tracked.leg)
            .await;
    }
}

/// Map a QuoteLeg to (token_id, side) for order placement.
fn leg_to_order_params(leg: QuoteLeg, market: &CanonicalMarket) -> (String, Side) {
    match leg {
        QuoteLeg::YesBid => (market.yes_token_id.clone(), Side::Buy),
        QuoteLeg::YesAsk => (market.yes_token_id.clone(), Side::Sell),
        QuoteLeg::NoBid => (market.no_token_id.clone(), Side::Buy),
        QuoteLeg::NoAsk => (market.no_token_id.clone(), Side::Sell),
    }
}

fn order_role(order: &TrackedOrder) -> &'static str {
    match order.leg {
        QuoteLeg::YesBid | QuoteLeg::NoBid => "bid_entry",
        QuoteLeg::YesAsk | QuoteLeg::NoAsk => "ask_inventory",
    }
}

fn quote_leg_from_live_order(order: &crate::models::LiveOrder) -> QuoteLeg {
    match (order.outcome, order.side) {
        (Outcome::Yes, Side::Buy) => QuoteLeg::YesBid,
        (Outcome::Yes, Side::Sell) => QuoteLeg::YesAsk,
        (Outcome::No, Side::Buy) => QuoteLeg::NoBid,
        (Outcome::No, Side::Sell) => QuoteLeg::NoAsk,
    }
}

fn detect_duplicate_live_bid_legs(
    live_orders: &[crate::models::LiveOrder],
) -> Vec<DuplicateLiveBidLeg> {
    let mut grouped = HashMap::<(String, QuoteLeg), Vec<String>>::new();
    for order in live_orders {
        let leg = quote_leg_from_live_order(order);
        if !leg.is_bid() || order.side != Side::Buy {
            continue;
        }
        grouped
            .entry((order.condition_id.clone(), leg))
            .or_default()
            .push(order.id.clone());
    }

    let mut duplicates = Vec::new();
    for ((condition_id, leg), mut order_ids) in grouped {
        if order_ids.len() <= 1 {
            continue;
        }
        order_ids.sort();
        duplicates.push(DuplicateLiveBidLeg {
            condition_id,
            leg,
            order_ids,
        });
    }
    duplicates.sort_by(|left, right| {
        left.condition_id
            .cmp(&right.condition_id)
            .then_with(|| left.leg.to_string().cmp(&right.leg.to_string()))
    });
    duplicates
}

fn opposite_token_id_for_order(
    order: &crate::models::LiveOrder,
    market_meta: &CanonicalMarket,
) -> String {
    if order.asset_id == market_meta.yes_token_id {
        market_meta.no_token_id.clone()
    } else {
        market_meta.yes_token_id.clone()
    }
}

fn build_tracked_order(
    order_id: String,
    trace_id: String,
    created_at: DateTime<Utc>,
    condition_id: String,
    leg: QuoteLeg,
    token_id: String,
    opposite_token_id: String,
    side: Side,
    price: Decimal,
    size: Decimal,
    matched_size: Decimal,
    neg_risk: bool,
    tick_size: String,
) -> TrackedOrder {
    TrackedOrder {
        order_id,
        trace_id,
        condition_id,
        created_at,
        leg,
        token_id,
        opposite_token_id,
        side,
        price,
        size,
        matched_size,
        neg_risk,
        tick_size,
    }
}

fn tracked_order_from_live_order(
    order: &crate::models::LiveOrder,
    leg: QuoteLeg,
    opposite_token_id: String,
    neg_risk: bool,
    tick_size: String,
    remaining_size: Decimal,
) -> TrackedOrder {
    build_tracked_order(
        order.id.clone(),
        Uuid::new_v4().to_string(),
        order.created_at,
        order.condition_id.clone(),
        leg,
        order.asset_id.clone(),
        opposite_token_id,
        order.side,
        order.price,
        remaining_size,
        order.size_matched,
        neg_risk,
        tick_size,
    )
}

pub(crate) fn cap_buy_size_to_budget(
    requested_size: Decimal,
    available_budget: Decimal,
    min_order_size: Decimal,
) -> Option<Decimal> {
    let whole_share_budget = whole_share_budget_limit(available_budget);
    let capped_size = requested_size.floor().min(whole_share_budget);

    if capped_size < min_order_size || capped_size <= Decimal::ZERO {
        None
    } else {
        Some(capped_size)
    }
}

pub(crate) fn whole_share_budget_limit(available_budget: Decimal) -> Decimal {
    available_budget.floor().max(Decimal::ZERO)
}

fn ask_origin_allows_full_inventory_exit(origin: &str) -> bool {
    origin == "inventory_exit_ask"
}

fn sellable_inventory_for_ask(leg: QuoteLeg, position: Option<&Position>, origin: &str) -> Decimal {
    match (leg, position, ask_origin_allows_full_inventory_exit(origin)) {
        (QuoteLeg::YesAsk, Some(pos), true) => pos.yes_size,
        (QuoteLeg::NoAsk, Some(pos), true) => pos.no_size,
        (QuoteLeg::YesAsk, Some(pos), false) => pos.sellable_yes(),
        (QuoteLeg::NoAsk, Some(pos), false) => pos.sellable_no(),
        _ => Decimal::ZERO,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration as ChronoDuration, TimeZone};
    use rust_decimal_macros::dec;
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    fn test_trading_client() -> Arc<TradingClient> {
        Arc::new(
            TradingClient::new(
                "https://example.com".to_string(),
                crate::auth::RequestSigner::new(crate::auth::ApiCredentials {
                    api_key: String::new(),
                    secret: String::new(),
                    passphrase: String::new(),
                    address: String::new(),
                    private_key: None,
                    funder: None,
                }),
                None,
                "",
                "",
                true,
            )
            .unwrap(),
        )
    }

    fn test_order_manager() -> OrderManager {
        OrderManager::new(
            test_trading_client(),
            Arc::new(RwLock::new(dec!(200))),
            None,
            "test".to_string(),
            "test".to_string(),
            dec!(0),
        )
    }

    fn live_trading_client(base_url: &str) -> Arc<TradingClient> {
        Arc::new(
            TradingClient::new(
                base_url.to_string(),
                crate::auth::RequestSigner::new(crate::auth::ApiCredentials {
                    api_key: String::new(),
                    secret: String::new(),
                    passphrase: String::new(),
                    address: "0x0".to_string(),
                    private_key: None,
                    funder: None,
                }),
                None,
                "",
                "",
                false,
            )
            .unwrap(),
        )
    }

    fn sample_market_meta() -> CanonicalMarket {
        CanonicalMarket {
            condition_id: "market".to_string(),
            market_slug: "market".to_string(),
            question: "Test market?".to_string(),
            yes_token_id: "yes-token".to_string(),
            no_token_id: "no-token".to_string(),
            reward_config: crate::models::RewardConfig {
                condition_id: "market".to_string(),
                daily_reward_rates: Vec::new(),
                daily_reward_total: Decimal::ZERO,
                min_size: dec!(1),
                max_spread: dec!(0.10),
            },
            neg_risk: false,
            tick_size: "0.01".to_string(),
            end_date: None,
            admitted_at: Utc::now(),
            status: crate::models::MarketStatus::Admitted,
        }
    }

    async fn spawn_orders_server(
        body: String,
    ) -> std::io::Result<(String, tokio::task::JoinHandle<()>)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;

        let task = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let body = body.clone();
                tokio::spawn(async move {
                    let mut buf = [0u8; 2048];
                    let Ok(read) = socket.read(&mut buf).await else {
                        return;
                    };
                    if read == 0 {
                        return;
                    }

                    let request = String::from_utf8_lossy(&buf[..read]);
                    let path = request
                        .lines()
                        .next()
                        .and_then(|line| line.split_whitespace().nth(1))
                        .unwrap_or_default();
                    let (status, response_body) = if path.starts_with("/data/orders") {
                        ("200 OK", body)
                    } else {
                        ("404 Not Found", "{\"error\":\"missing\"}".to_string())
                    };
                    let response = format!(
                        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response_body}",
                        response_body.len()
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                    let _ = socket.shutdown().await;
                });
            }
        });

        Ok((format!("http://{}", addr), task))
    }

    fn sample_tracked_order() -> TrackedOrder {
        TrackedOrder {
            order_id: "order-1".to_string(),
            trace_id: "trace-1".to_string(),
            condition_id: "market".to_string(),
            created_at: Utc::now(),
            leg: QuoteLeg::YesBid,
            token_id: "yes-token".to_string(),
            opposite_token_id: "no-token".to_string(),
            side: Side::Buy,
            price: dec!(0.45),
            size: dec!(20),
            matched_size: Decimal::ZERO,
            neg_risk: false,
            tick_size: "0.01".to_string(),
        }
    }

    fn sample_live_order(created_at: DateTime<Utc>) -> crate::models::LiveOrder {
        crate::models::LiveOrder {
            id: "live-order-1".to_string(),
            condition_id: "market".to_string(),
            asset_id: "yes-token".to_string(),
            side: Side::Buy,
            price: dec!(0.45),
            original_size: dec!(20),
            size_matched: dec!(5),
            outcome: crate::models::Outcome::Yes,
            order_type: OrderType::GTC,
            status: crate::models::OrderStatus::Live,
            created_at,
            associated_trade_ids: Vec::new(),
        }
    }

    #[test]
    fn cap_buy_size_to_budget_caps_to_whole_share_budget() {
        let capped = cap_buy_size_to_budget(dec!(200), dec!(100), dec!(5));
        assert_eq!(capped, Some(dec!(100)));
    }

    #[test]
    fn cap_buy_size_to_budget_floors_fractional_budget() {
        let capped = cap_buy_size_to_budget(dec!(200), dec!(184.7), dec!(5));
        assert_eq!(capped, Some(dec!(184)));
    }

    #[test]
    fn cap_buy_size_to_budget_keeps_exact_fit() {
        let capped = cap_buy_size_to_budget(dec!(180), dec!(180), dec!(5));
        assert_eq!(capped, Some(dec!(180)));
    }

    #[test]
    fn cap_buy_size_to_budget_respects_account_wide_remaining_exposure() {
        let capped = cap_buy_size_to_budget(dec!(283), dec!(180), dec!(5));
        assert_eq!(capped, Some(dec!(180)));
    }

    #[test]
    fn cap_buy_size_to_budget_skips_when_budget_below_min_size() {
        let capped = cap_buy_size_to_budget(dec!(200), dec!(4.9), dec!(5));
        assert_eq!(capped, None);
    }

    #[tokio::test]
    async fn available_budget_subtracts_cash_reserve() {
        let trading_client = test_trading_client();
        let balance = Arc::new(RwLock::new(dec!(200)));
        let om = OrderManager::new(
            trading_client,
            balance,
            None,
            "test".to_string(),
            "test".to_string(),
            dec!(50), // cash_reserve
        );

        // No orders → budget = 200 - 0 - 50 = 150
        let budget = om.available_budget().await;
        assert_eq!(budget, dec!(150));
    }

    #[tokio::test]
    async fn available_budget_floors_at_zero() {
        let trading_client = test_trading_client();
        let balance = Arc::new(RwLock::new(dec!(30)));
        let om = OrderManager::new(
            trading_client,
            balance,
            None,
            "test".to_string(),
            "test".to_string(),
            dec!(50), // reserve > balance
        );

        let budget = om.available_budget().await;
        assert_eq!(budget, Decimal::ZERO);
    }

    #[tokio::test]
    async fn available_hedge_resolution_usdc_ignores_cash_reserve() {
        let trading_client = test_trading_client();
        let balance = Arc::new(RwLock::new(dec!(200)));
        let om = OrderManager::new(
            trading_client,
            balance,
            None,
            "test".to_string(),
            "test".to_string(),
            dec!(50),
        );

        let tracked = sample_tracked_order();
        om.orders
            .write()
            .await
            .entry(tracked.condition_id.clone())
            .or_default()
            .insert(tracked.order_id.clone(), tracked);

        // Committed exposure tracks resting BUY share collateral, not quoted notional.
        let budget = om.available_hedge_resolution_usdc().await;
        assert_eq!(budget, dec!(180));
    }

    #[tokio::test]
    async fn sync_open_orders_partial_global_omission_keeps_tracked_bid_reserved() {
        let body = json!({
            "data": [json!({
                "id": "live-ask-1",
                "status": "live",
                "market": "other-market",
                "asset_id": "other-yes-token",
                "side": "SELL",
                "price": "0.55",
                "original_size": "10",
                "size_matched": "0",
                "outcome": "YES",
                "order_type": "GTC",
                "created_at": Utc::now().timestamp()
            })],
            "next_cursor": "LTE="
        })
        .to_string();
        let (base_url, server) = spawn_orders_server(body).await.unwrap();
        let om = OrderManager::new(
            live_trading_client(&base_url),
            Arc::new(RwLock::new(dec!(100))),
            None,
            "test".to_string(),
            "test".to_string(),
            dec!(0),
        );
        let tracked = sample_tracked_order();
        om.orders
            .write()
            .await
            .entry(tracked.condition_id.clone())
            .or_default()
            .insert(tracked.order_id.clone(), tracked.clone());

        assert_eq!(om.available_budget().await, dec!(80));

        let sync_result = om.sync_open_orders(&[sample_market_meta()]).await.unwrap();

        assert_eq!(sync_result.live, 1);
        assert_eq!(sync_result.imported, 1);
        assert_eq!(om.available_budget().await, dec!(80));
        assert!(om.get_tracked_order(&tracked.order_id).await.is_some());

        server.abort();
    }

    #[tokio::test]
    async fn sync_market_open_orders_observe_only_keeps_missing_bid_reserved() {
        let (base_url, server) = spawn_orders_server(
            json!({
                "data": [],
                "next_cursor": "LTE="
            })
            .to_string(),
        )
        .await
        .unwrap();
        let om = OrderManager::new(
            live_trading_client(&base_url),
            Arc::new(RwLock::new(dec!(100))),
            None,
            "test".to_string(),
            "test".to_string(),
            dec!(0),
        );
        let tracked = sample_tracked_order();
        om.orders
            .write()
            .await
            .entry(tracked.condition_id.clone())
            .or_default()
            .insert(tracked.order_id.clone(), tracked.clone());

        let sync_result = om
            .sync_market_open_orders(
                &tracked.condition_id,
                &sample_market_meta(),
                MarketOrderSyncMode::ObserveOnly,
            )
            .await
            .unwrap();

        assert_eq!(sync_result.live, 0);
        assert_eq!(sync_result.pruned, 0);
        assert_eq!(
            sync_result.missing_order_ids,
            vec![tracked.order_id.clone()]
        );
        assert_eq!(om.available_budget().await, dec!(80));
        assert!(om.get_tracked_order(&tracked.order_id).await.is_some());

        server.abort();
    }

    #[tokio::test]
    async fn place_quotes_records_exchange_leg_immediately() {
        let om = test_order_manager();
        let market = sample_market_meta();
        let quote_set = QuoteSet {
            condition_id: market.condition_id.clone(),
            candidates: vec![crate::models::QuoteCandidate {
                condition_id: market.condition_id.clone(),
                leg: QuoteLeg::YesBid,
                price: dec!(0.45),
                size: dec!(20),
                status: QuoteStatus::Approved,
                reason: None,
            }],
        };

        let placed = om
            .place_quotes(&market, &quote_set, None, dec!(20), None, "test", None)
            .await
            .unwrap();

        assert_eq!(placed.len(), 1);
        assert!(
            om.has_exchange_live_leg(&market.condition_id, QuoteLeg::YesBid)
                .await
        );
    }

    #[tokio::test]
    async fn place_quotes_skips_when_exchange_leg_is_reserved() {
        let om = test_order_manager();
        let market = sample_market_meta();
        om.exchange_legs
            .write()
            .await
            .insert((market.condition_id.clone(), QuoteLeg::YesBid));
        let quote_set = QuoteSet {
            condition_id: market.condition_id.clone(),
            candidates: vec![crate::models::QuoteCandidate {
                condition_id: market.condition_id.clone(),
                leg: QuoteLeg::YesBid,
                price: dec!(0.45),
                size: dec!(20),
                status: QuoteStatus::Approved,
                reason: None,
            }],
        };

        let placed = om
            .place_quotes(&market, &quote_set, None, dec!(20), None, "test", None)
            .await
            .unwrap();

        assert!(placed.is_empty());
        assert!(om.get_market_orders(&market.condition_id).await.is_empty());
    }

    #[tokio::test]
    async fn place_quotes_skips_regular_inventory_asks_for_fully_paired_inventory() {
        let om = test_order_manager();
        let market = sample_market_meta();
        let mut position = Position::new(market.condition_id.clone());
        position.yes_size = dec!(20);
        position.no_size = dec!(20);
        let quote_set = QuoteSet {
            condition_id: market.condition_id.clone(),
            candidates: vec![
                crate::models::QuoteCandidate {
                    condition_id: market.condition_id.clone(),
                    leg: QuoteLeg::YesAsk,
                    price: dec!(0.76),
                    size: dec!(20),
                    status: QuoteStatus::Approved,
                    reason: None,
                },
                crate::models::QuoteCandidate {
                    condition_id: market.condition_id.clone(),
                    leg: QuoteLeg::NoAsk,
                    price: dec!(0.27),
                    size: dec!(20),
                    status: QuoteStatus::Approved,
                    reason: None,
                },
            ],
        };

        let placed = om
            .place_quotes(
                &market,
                &quote_set,
                Some(&position),
                dec!(1),
                None,
                "inventory_ask",
                Some("ask_inventory"),
            )
            .await
            .unwrap();

        assert!(placed.is_empty());
        assert!(om.get_market_orders(&market.condition_id).await.is_empty());
    }

    #[tokio::test]
    async fn place_quotes_allows_full_inventory_exit_asks_for_fully_paired_inventory() {
        let om = test_order_manager();
        let market = sample_market_meta();
        let mut position = Position::new(market.condition_id.clone());
        position.yes_size = dec!(20);
        position.no_size = dec!(20);
        let quote_set = QuoteSet {
            condition_id: market.condition_id.clone(),
            candidates: vec![
                crate::models::QuoteCandidate {
                    condition_id: market.condition_id.clone(),
                    leg: QuoteLeg::YesAsk,
                    price: dec!(0.76),
                    size: dec!(20),
                    status: QuoteStatus::Approved,
                    reason: None,
                },
                crate::models::QuoteCandidate {
                    condition_id: market.condition_id.clone(),
                    leg: QuoteLeg::NoAsk,
                    price: dec!(0.27),
                    size: dec!(20),
                    status: QuoteStatus::Approved,
                    reason: None,
                },
            ],
        };

        let placed = om
            .place_quotes(
                &market,
                &quote_set,
                Some(&position),
                dec!(1),
                None,
                "inventory_exit_ask",
                Some("ask_inventory"),
            )
            .await
            .unwrap();

        assert_eq!(placed.len(), 2);
        assert_eq!(om.get_market_orders(&market.condition_id).await.len(), 2);
    }

    #[tokio::test]
    async fn unknown_cancel_keeps_order_tracked_and_pending() {
        let om = test_order_manager();
        let tracked = sample_tracked_order();
        om.orders
            .write()
            .await
            .entry(tracked.condition_id.clone())
            .or_default()
            .insert(tracked.order_id.clone(), tracked.clone());

        let outcome = om
            .apply_cancel_outcome(
                &tracked,
                CancelOrderOutcome::Unknown("lookup missing".to_string()),
                CancelReasonCode::RiskHalt,
                "test",
                None,
            )
            .await;

        assert!(matches!(outcome, CancelOrderOutcome::Unknown(_)));
        assert_eq!(om.get_market_orders(&tracked.condition_id).await.len(), 1);
        assert!(om
            .pending_cancel_retries
            .read()
            .await
            .contains_key(&tracked.order_id));
    }

    #[tokio::test]
    async fn confirmed_cancel_removes_tracking_and_pending_state() {
        let om = test_order_manager();
        let tracked = sample_tracked_order();
        om.orders
            .write()
            .await
            .entry(tracked.condition_id.clone())
            .or_default()
            .insert(tracked.order_id.clone(), tracked.clone());
        om.pending_cancel_retries.write().await.insert(
            tracked.order_id.clone(),
            PendingCancelRetry {
                tracked: tracked.clone(),
                reason_code: CancelReasonCode::RiskHalt,
                origin: "test",
                first_attempted_at: Instant::now(),
                last_attempted_at: Instant::now(),
                attempts: 1,
            },
        );

        let outcome = om
            .apply_cancel_outcome(
                &tracked,
                CancelOrderOutcome::Confirmed,
                CancelReasonCode::RiskHalt,
                "test",
                None,
            )
            .await;

        assert!(matches!(outcome, CancelOrderOutcome::Confirmed));
        assert!(om.get_market_orders(&tracked.condition_id).await.is_empty());
        assert!(!om
            .pending_cancel_retries
            .read()
            .await
            .contains_key(&tracked.order_id));
        assert!(om
            .recently_cancelled
            .read()
            .await
            .contains_key(&tracked.order_id));
    }

    #[tokio::test]
    async fn retry_pending_cancels_confirms_and_clears_dry_run_orders() {
        let om = test_order_manager();
        let tracked = sample_tracked_order();
        let now = Instant::now();
        om.orders
            .write()
            .await
            .entry(tracked.condition_id.clone())
            .or_default()
            .insert(tracked.order_id.clone(), tracked.clone());
        om.pending_cancel_retries.write().await.insert(
            tracked.order_id.clone(),
            PendingCancelRetry {
                tracked: tracked.clone(),
                reason_code: CancelReasonCode::RiskHalt,
                origin: "test",
                first_attempted_at: now,
                last_attempted_at: now,
                attempts: 1,
            },
        );
        tokio::time::sleep(StdDuration::from_secs(
            PENDING_CANCEL_RETRY_BACKOFF_SECS + 1,
        ))
        .await;

        let confirmed = om.retry_pending_cancels().await;

        assert_eq!(confirmed, 1);
        assert!(om.get_market_orders(&tracked.condition_id).await.is_empty());
        assert!(om.pending_cancel_retries.read().await.is_empty());
    }

    #[test]
    fn prune_recently_cancelled_entries_drops_old_entries_without_underflow() {
        let ttl = StdDuration::from_millis(1);
        let expired_at = Instant::now();
        let now = expired_at + ttl + StdDuration::from_millis(1);
        let tracked = sample_tracked_order();
        let fresh_tracked = TrackedOrder {
            order_id: "fresh-order".to_string(),
            ..tracked.clone()
        };
        let mut recently_cancelled = HashMap::from([
            (tracked.order_id.clone(), (tracked, expired_at)),
            (fresh_tracked.order_id.clone(), (fresh_tracked.clone(), now)),
        ]);

        prune_recently_cancelled_entries(&mut recently_cancelled, now, ttl);

        assert!(!recently_cancelled.contains_key("order-1"));
        assert!(recently_cancelled.contains_key(&fresh_tracked.order_id));
    }

    #[test]
    fn tracked_order_from_live_order_keeps_exchange_created_at() {
        let created_at = Utc
            .with_ymd_and_hms(2026, 3, 23, 12, 0, 0)
            .single()
            .unwrap();
        let tracked = tracked_order_from_live_order(
            &sample_live_order(created_at),
            QuoteLeg::YesBid,
            "no-token".to_string(),
            false,
            "0.01".to_string(),
            dec!(15),
        );

        assert_eq!(tracked.created_at, created_at);
        assert_eq!(tracked.size, dec!(15));
        assert_eq!(tracked.matched_size, dec!(5));
    }

    #[tokio::test]
    async fn resize_order_preserves_original_created_at() {
        let om = test_order_manager();
        let mut tracked = sample_tracked_order();
        tracked.created_at = Utc::now() - ChronoDuration::minutes(5);
        om.orders
            .write()
            .await
            .entry(tracked.condition_id.clone())
            .or_default()
            .insert(tracked.order_id.clone(), tracked.clone());

        let resized = om
            .resize_order(
                &tracked.order_id,
                dec!(10),
                CancelReasonCode::HedgeDepthPartialDownsize,
                "test_resize",
            )
            .await
            .unwrap()
            .expect("replacement order");

        assert_eq!(resized.created_at, tracked.created_at);
        assert_eq!(resized.size, dec!(10));
        assert!(
            om.has_exchange_live_leg(&tracked.condition_id, tracked.leg)
                .await
        );
    }

    #[tokio::test]
    async fn sync_market_open_orders_prunes_stale_bids_before_resolution_budget() {
        let body = json!({
            "data": [],
            "next_cursor": "LTE="
        })
        .to_string();
        let (base_url, server) = spawn_orders_server(body).await.unwrap();
        let om = OrderManager::new(
            live_trading_client(&base_url),
            Arc::new(RwLock::new(dec!(100))),
            None,
            "test".to_string(),
            "test".to_string(),
            dec!(0),
        );
        let tracked = sample_tracked_order();
        om.orders
            .write()
            .await
            .entry(tracked.condition_id.clone())
            .or_default()
            .insert(tracked.order_id.clone(), tracked.clone());
        om.pending_cancel_retries.write().await.insert(
            tracked.order_id.clone(),
            PendingCancelRetry {
                tracked: tracked.clone(),
                reason_code: CancelReasonCode::RiskHalt,
                origin: "test",
                first_attempted_at: Instant::now(),
                last_attempted_at: Instant::now(),
                attempts: 1,
            },
        );

        assert_eq!(om.available_hedge_resolution_usdc().await, dec!(80));

        let sync_result = om
            .sync_market_open_orders(
                &tracked.condition_id,
                &sample_market_meta(),
                MarketOrderSyncMode::Reconcile,
            )
            .await
            .unwrap();

        assert_eq!(sync_result.live, 0);
        assert_eq!(sync_result.pruned, 1);
        assert_eq!(om.available_hedge_resolution_usdc().await, dec!(100));
        assert!(om.pending_cancel_retries.read().await.is_empty());
        assert!(om.get_market_orders(&tracked.condition_id).await.is_empty());

        server.abort();
    }
}
