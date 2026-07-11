use chrono;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::books::BookManager;
use crate::models::{
    LiveOrder, OrderAmountKind, OrderRequest, OrderResult, OrderType, PriceLevel, QuoteLeg, Side,
};
use crate::trading::client::CancelOrderOutcome;
use crate::trading::TradingClient;

/// Intent to execute an offsetting hedge after a fill.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HedgeIntent {
    pub condition_id: String,
    pub trigger_order_id: String,
    pub trigger_leg: QuoteLeg,
    pub fill_size: Decimal,
    pub fill_price: Decimal,
    /// Token ID to trade for the hedge.
    pub hedge_token_id: String,
    /// Side of the hedge order.
    pub hedge_side: Side,
    /// Whether this market uses the Neg Risk CTF Exchange.
    pub neg_risk: bool,
    pub tick_size: String,
}

/// Result of a hedge execution attempt.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HedgeResult {
    pub intent: HedgeIntent,
    pub success: bool,
    pub order_result: Option<OrderResult>,
    pub hedge_price: Option<Decimal>,
    pub failure_reason: Option<String>,
    pub verification_state: HedgeVerificationState,
    #[serde(default)]
    pub verification_metadata: HedgeVerificationMetadata,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HedgeVerificationState {
    VerifiedFilled,
    VerifiedZeroFill,
    Unknown,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HedgeVerificationMetadata {
    #[serde(default)]
    pub cancel_status: Option<String>,
    #[serde(default)]
    pub cancel_reason: Option<String>,
    #[serde(default)]
    pub lookup_status: Option<String>,
    #[serde(default)]
    pub lookup_matched_shares: Option<Decimal>,
    #[serde(default)]
    pub lookup_error: Option<String>,
    #[serde(default)]
    pub trade_ids: Vec<String>,
}

impl HedgeVerificationMetadata {
    fn with_trade_ids(order_result: Option<&OrderResult>) -> Self {
        Self {
            trade_ids: order_result
                .map(|result| result.trade_ids.clone())
                .unwrap_or_default(),
            ..Default::default()
        }
    }
}

/// Executes offsetting hedges when fills occur on our resting orders.
#[derive(Clone)]
pub struct HedgeExecutor {
    trading_client: Arc<TradingClient>,
    book_manager: Arc<BookManager>,
}

impl HedgeExecutor {
    pub fn new(trading_client: Arc<TradingClient>, book_manager: Arc<BookManager>) -> Self {
        Self {
            trading_client,
            book_manager,
        }
    }

    /// Execute an offsetting hedge for a fill event.
    ///
    /// When `resolution` is provided, uses the book-aware limit price and size.
    /// When `None`, falls back to the legacy hardcoded $0.99 limit price (to be
    /// removed once all call sites supply a resolution).
    pub async fn execute_hedge(
        &self,
        intent: &HedgeIntent,
        resolution: Option<&HedgeResolution>,
    ) -> HedgeResult {
        let (hedge_size, buy_limit) = match resolution {
            Some(res) => (
                normalize_share_size(res.hedge_shares),
                res.hedge_limit_price,
            ),
            None => (
                normalize_share_size(intent.fill_size),
                buy_hedge_limit_price(),
            ),
        };

        if hedge_size <= Decimal::ZERO {
            return HedgeResult {
                intent: intent.clone(),
                success: false,
                order_result: None,
                hedge_price: None,
                failure_reason: Some("Hedge size rounded to zero".to_string()),
                verification_state: HedgeVerificationState::VerifiedZeroFill,
                verification_metadata: HedgeVerificationMetadata::default(),
            };
        }

        info!(
            condition_id = %intent.condition_id,
            trigger_leg = %intent.trigger_leg,
            fill_size = %hedge_size,
            hedge_token = %intent.hedge_token_id,
            hedge_side = %intent.hedge_side,
            limit_price = %buy_limit,
            book_aware = resolution.is_some(),
            "Executing hedge"
        );

        if let Some(book) = self.book_manager.get_book(&intent.hedge_token_id).await {
            if book.is_stale(chrono::Duration::seconds(30)) {
                warn!(
                    condition_id = %intent.condition_id,
                    "Hedge book is stale — attempting hedge anyway"
                );
            }
            let walk = match intent.hedge_side {
                Side::Buy => book.walk_asks(hedge_size),
                Side::Sell => book.walk_bids(hedge_size),
            };
            if !walk.fully_filled {
                warn!(
                    condition_id = %intent.condition_id,
                    needed = %hedge_size,
                    available = %walk.filled_size,
                    "Cached book shows insufficient depth — attempting hedge anyway"
                );
            }
        } else {
            warn!(
                condition_id = %intent.condition_id,
                "No cached book for hedge token — attempting hedge anyway"
            );
        }

        match intent.hedge_side {
            Side::Buy => {
                self.execute_buy_gtc_cancel(intent, hedge_size, buy_limit)
                    .await
            }
            Side::Sell => self.execute_fok_sell(intent, hedge_size).await,
        }
    }

    /// Determine the hedge parameters for a given fill leg and market.
    pub fn compute_hedge_params(
        leg: QuoteLeg,
        yes_token_id: &str,
        no_token_id: &str,
    ) -> (String, Side) {
        match leg {
            QuoteLeg::YesBid => (no_token_id.to_string(), Side::Buy),
            QuoteLeg::YesAsk => (no_token_id.to_string(), Side::Sell),
            QuoteLeg::NoBid => (yes_token_id.to_string(), Side::Buy),
            QuoteLeg::NoAsk => (yes_token_id.to_string(), Side::Sell),
        }
    }

    async fn execute_buy_gtc_cancel(
        &self,
        intent: &HedgeIntent,
        hedge_size: Decimal,
        hedge_price: Decimal,
    ) -> HedgeResult {
        let request = OrderRequest {
            token_id: intent.hedge_token_id.clone(),
            price: hedge_price,
            size: hedge_size,
            amount_kind: OrderAmountKind::Shares,
            side: Side::Buy,
            order_type: OrderType::GTC,
            post_only: false,
            neg_risk: intent.neg_risk,
            tick_size: intent.tick_size.clone(),
        };

        let result = match self.trading_client.place_order(&request).await {
            Ok(result) => result,
            Err(e) => {
                error!(
                    condition_id = %intent.condition_id,
                    error = %e,
                    "Aggressive hedge buy failed"
                );
                return HedgeResult {
                    intent: intent.clone(),
                    success: false,
                    order_result: None,
                    hedge_price: Some(hedge_price),
                    failure_reason: Some(format!("Order placement failed: {}", e)),
                    verification_state: HedgeVerificationState::VerifiedZeroFill,
                    verification_metadata: HedgeVerificationMetadata::default(),
                };
            }
        };

        info!(
            condition_id = %intent.condition_id,
            hedge_order_id = %result.order_id,
            hedge_price = %hedge_price,
            hedge_size = %hedge_size,
            "Aggressive share-sized hedge buy placed"
        );

        if result.order_id.is_empty() {
            let verification_metadata = HedgeVerificationMetadata::with_trade_ids(Some(&result));
            return HedgeResult {
                intent: intent.clone(),
                success: false,
                order_result: Some(result),
                hedge_price: Some(hedge_price),
                failure_reason: Some(
                    "Aggressive hedge buy returned no order_id for cancellation".to_string(),
                ),
                verification_state: HedgeVerificationState::VerifiedZeroFill,
                verification_metadata,
            };
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        let mut verification_metadata = HedgeVerificationMetadata::with_trade_ids(Some(&result));
        match self.trading_client.cancel_order(&result.order_id).await {
            Ok(CancelOrderOutcome::Confirmed) => {
                verification_metadata.cancel_status = Some("confirmed".to_string());
                info!(
                    condition_id = %intent.condition_id,
                    hedge_order_id = %result.order_id,
                    "Cancelled any unfilled aggressive hedge remainder"
                );
            }
            Ok(CancelOrderOutcome::Rejected(reason)) => {
                verification_metadata.cancel_status = Some("rejected".to_string());
                verification_metadata.cancel_reason = Some(reason.clone());
                warn!(
                    condition_id = %intent.condition_id,
                    hedge_order_id = %result.order_id,
                    reason = %reason,
                    "Aggressive hedge buy remainder cancel was not explicitly confirmed"
                );
            }
            Ok(CancelOrderOutcome::Unknown(reason)) => {
                verification_metadata.cancel_status = Some("unknown".to_string());
                verification_metadata.cancel_reason = Some(reason.clone());
                warn!(
                    condition_id = %intent.condition_id,
                    hedge_order_id = %result.order_id,
                    reason = %reason,
                    "Aggressive hedge buy remainder cancel was not explicitly confirmed"
                );
            }
            Err(e) => {
                verification_metadata.cancel_status = Some("unknown".to_string());
                verification_metadata.cancel_reason = Some(e.to_string());
                warn!(
                    condition_id = %intent.condition_id,
                    hedge_order_id = %result.order_id,
                    error = %e,
                    "Failed to cancel aggressive hedge buy remainder — order may have fully filled"
                );
            }
        }

        // Verify whether the order actually filled (fully or partially).
        // Without this check, a GTC that sat unfilled for 500ms then got
        // cancelled would be reported as success — masking a failed hedge.
        let verification_state = match self.trading_client.get_order(&result.order_id).await {
            Ok(Some(order)) => {
                verification_metadata.lookup_status = Some(hedge_lookup_status_label(order.status));
                verification_metadata.lookup_matched_shares = Some(order.size_matched);
                info!(
                    condition_id = %intent.condition_id,
                    hedge_order_id = %result.order_id,
                    size_matched = %order.size_matched,
                    original_size = %order.original_size,
                    status = ?order.status,
                    "GTC hedge fill verification"
                );
                classify_buy_hedge_verification(Some(&order))
            }
            Ok(None) => {
                verification_metadata.lookup_status = Some("missing".to_string());
                warn!(
                    condition_id = %intent.condition_id,
                    hedge_order_id = %result.order_id,
                    "Could not fetch hedge order for fill verification — marking hedge as unknown"
                );
                HedgeVerificationState::Unknown
            }
            Err(e) => {
                verification_metadata.lookup_status = Some("error".to_string());
                verification_metadata.lookup_error = Some(e.to_string());
                warn!(
                    condition_id = %intent.condition_id,
                    hedge_order_id = %result.order_id,
                    error = %e,
                    "Failed to verify hedge fill status — marking hedge as unknown"
                );
                HedgeVerificationState::Unknown
            }
        };

        match verification_state {
            HedgeVerificationState::VerifiedFilled => HedgeResult {
                intent: intent.clone(),
                success: true,
                order_result: Some(result),
                hedge_price: Some(hedge_price),
                failure_reason: None,
                verification_state,
                verification_metadata,
            },
            HedgeVerificationState::Unknown => HedgeResult {
                intent: intent.clone(),
                success: true,
                order_result: Some(result),
                hedge_price: Some(hedge_price),
                failure_reason: Some(
                    "Hedge fill could not be verified after cancel; awaiting position confirmation"
                        .to_string(),
                ),
                verification_state,
                verification_metadata,
            },
            HedgeVerificationState::VerifiedZeroFill => {
                error!(
                    condition_id = %intent.condition_id,
                    hedge_order_id = %result.order_id,
                    hedge_size = %hedge_size,
                    "GTC hedge got zero fills after 500ms — hedge FAILED"
                );
                HedgeResult {
                    intent: intent.clone(),
                    success: false,
                    order_result: Some(result),
                    hedge_price: Some(hedge_price),
                    failure_reason: Some("GTC hedge cancelled with zero fills".to_string()),
                    verification_state,
                    verification_metadata,
                }
            }
        }
    }

    async fn execute_fok_sell(&self, intent: &HedgeIntent, hedge_size: Decimal) -> HedgeResult {
        let hedge_price = sell_hedge_limit_price();
        let request = OrderRequest {
            token_id: intent.hedge_token_id.clone(),
            price: hedge_price,
            size: hedge_size,
            amount_kind: OrderAmountKind::Shares,
            side: Side::Sell,
            order_type: OrderType::FOK,
            post_only: false,
            neg_risk: intent.neg_risk,
            tick_size: intent.tick_size.clone(),
        };

        match self.trading_client.place_order(&request).await {
            Ok(result) => {
                let verification_metadata =
                    HedgeVerificationMetadata::with_trade_ids(Some(&result));
                info!(
                    condition_id = %intent.condition_id,
                    hedge_order_id = %result.order_id,
                    hedge_price = %hedge_price,
                    hedge_size = %hedge_size,
                    "Hedge sell placed"
                );
                HedgeResult {
                    intent: intent.clone(),
                    success: true,
                    order_result: Some(result),
                    hedge_price: Some(hedge_price),
                    failure_reason: None,
                    verification_state: HedgeVerificationState::VerifiedFilled,
                    verification_metadata,
                }
            }
            Err(e) => {
                error!(
                    condition_id = %intent.condition_id,
                    error = %e,
                    "Hedge sell failed"
                );
                HedgeResult {
                    intent: intent.clone(),
                    success: false,
                    order_result: None,
                    hedge_price: Some(hedge_price),
                    failure_reason: Some(format!("Order placement failed: {}", e)),
                    verification_state: HedgeVerificationState::VerifiedZeroFill,
                    verification_metadata: HedgeVerificationMetadata::default(),
                }
            }
        }
    }
}

/// Result of the book-aware cost-benefit analysis for resolving unhedged exposure.
///
/// Splits the total exposure into two buckets based on which is cheaper per share:
/// - Hedge: BUY opposite token (cost = fill_price + hedge_ask - 1.00)
/// - Sell back: SELL filled token (cost = fill_price - sellback_bid)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HedgeResolution {
    /// Shares to resolve by buying the opposite token.
    pub hedge_shares: Decimal,
    /// Limit price for the hedge BUY order (worst ask consumed + 1 tick buffer).
    pub hedge_limit_price: Decimal,
    /// Shares to resolve by selling back the filled token.
    pub sellback_shares: Decimal,
    /// Limit price for the sell-back FOK order (worst bid consumed).
    pub sellback_limit_price: Decimal,
    /// Shares that could not be resolved from either book under the available budget.
    pub unresolved_shares: Decimal,
}

/// Walk both books to determine the cheapest resolution per share of unhedged exposure.
///
/// For each share, compares:
/// - hedge cost   = fill_price + hedge_ask_price - 1.00
/// - sellback cost = fill_price - sellback_bid_price
///
/// Consumes from whichever side is cheaper. On ties, prefers sellback so the
/// bot reclaims capital immediately without depending on merge execution.
///
/// `max_hedge_usdc` limits how much actual USDC can be spent on the hedge BUY leg.
/// Shares that are cheap to hedge but exceed that budget are re-routed to sell-back
/// when filled-side bid depth exists, otherwise they remain unresolved.
///
/// `tick_size` is used to add a 1-tick buffer to the hedge limit price.
pub fn plan_fill_resolution(
    fill_price: Decimal,
    hedge_asks: &[PriceLevel],
    sellback_bids: &[PriceLevel],
    total_size: Decimal,
    max_hedge_usdc: Decimal,
    tick_size: Decimal,
) -> HedgeResolution {
    let mut remaining = normalize_share_size(total_size);
    let max_hedge_usdc = max_hedge_usdc.max(Decimal::ZERO);
    let mut hedge_shares = Decimal::ZERO;
    let mut sellback_shares = Decimal::ZERO;
    let mut worst_hedge_ask = Decimal::ZERO;
    let mut worst_sellback_bid = Decimal::ZERO;

    let mut h_ptr: usize = 0;
    let mut s_ptr: usize = 0;
    let mut h_remaining_at_level = if !hedge_asks.is_empty() {
        hedge_asks[0].size
    } else {
        Decimal::ZERO
    };
    let mut s_remaining_at_level = if !sellback_bids.is_empty() {
        sellback_bids[0].size
    } else {
        Decimal::ZERO
    };

    while remaining > Decimal::ZERO {
        let hedge_avail = if h_ptr < hedge_asks.len() {
            Some((hedge_asks[h_ptr].price, h_remaining_at_level))
        } else {
            None
        };
        let sell_avail = if s_ptr < sellback_bids.len() {
            Some((sellback_bids[s_ptr].price, s_remaining_at_level))
        } else {
            None
        };

        match (hedge_avail, sell_avail) {
            (None, None) => break,
            (Some((h_price, h_size)), None) => {
                let take = affordable_hedge_take(
                    remaining,
                    h_size,
                    hedge_shares,
                    max_hedge_usdc,
                    worst_hedge_ask.max(h_price),
                    tick_size,
                );
                if take <= Decimal::ZERO {
                    break;
                }
                hedge_shares += take;
                worst_hedge_ask = worst_hedge_ask.max(h_price);
                remaining -= take;
                h_remaining_at_level -= take;
                advance_level_if_empty(&mut h_ptr, &mut h_remaining_at_level, hedge_asks);
            }
            (None, Some((s_price, s_size))) => {
                let take = remaining.min(s_size);
                sellback_shares += take;
                worst_sellback_bid = s_price;
                remaining -= take;
                s_remaining_at_level -= take;
                advance_level_if_empty(&mut s_ptr, &mut s_remaining_at_level, sellback_bids);
            }
            (Some((h_price, h_size)), Some((s_price, s_size))) => {
                let hedge_cost = fill_price + h_price - Decimal::ONE;
                let sellback_cost = fill_price - s_price;

                if hedge_cost < sellback_cost {
                    // Hedge is strictly cheaper.
                    let take = affordable_hedge_take(
                        remaining,
                        h_size,
                        hedge_shares,
                        max_hedge_usdc,
                        worst_hedge_ask.max(h_price),
                        tick_size,
                    );
                    if take <= Decimal::ZERO {
                        let sell_take = remaining.min(s_size);
                        if sell_take <= Decimal::ZERO {
                            break;
                        }
                        sellback_shares += sell_take;
                        worst_sellback_bid = s_price;
                        remaining -= sell_take;
                        s_remaining_at_level -= sell_take;
                        advance_level_if_empty(
                            &mut s_ptr,
                            &mut s_remaining_at_level,
                            sellback_bids,
                        );
                        continue;
                    }
                    hedge_shares += take;
                    worst_hedge_ask = worst_hedge_ask.max(h_price);
                    remaining -= take;
                    h_remaining_at_level -= take;
                    advance_level_if_empty(&mut h_ptr, &mut h_remaining_at_level, hedge_asks);
                } else {
                    // Sell-back is cheaper
                    let take = remaining.min(s_size);
                    sellback_shares += take;
                    worst_sellback_bid = s_price;
                    remaining -= take;
                    s_remaining_at_level -= take;
                    advance_level_if_empty(&mut s_ptr, &mut s_remaining_at_level, sellback_bids);
                }
            }
        }
    }

    // Add 1-tick buffer to hedge limit price so we don't miss fills at the edge
    let hedge_limit_price = if hedge_shares > Decimal::ZERO {
        worst_hedge_ask + tick_size
    } else {
        Decimal::ZERO
    };

    let sellback_limit_price = if sellback_shares > Decimal::ZERO {
        worst_sellback_bid
    } else {
        Decimal::ZERO
    };

    HedgeResolution {
        hedge_shares: normalize_share_size(hedge_shares),
        hedge_limit_price,
        sellback_shares: normalize_share_size(sellback_shares),
        sellback_limit_price,
        unresolved_shares: normalize_share_size(remaining),
    }
}

/// Legacy convenience wrapper that plans with effectively unlimited hedge budget.
pub fn compute_hedge_resolution(
    fill_price: Decimal,
    hedge_asks: &[PriceLevel],
    sellback_bids: &[PriceLevel],
    total_size: Decimal,
    tick_size: Decimal,
) -> HedgeResolution {
    plan_fill_resolution(
        fill_price,
        hedge_asks,
        sellback_bids,
        total_size,
        Decimal::MAX,
        tick_size,
    )
}

fn affordable_hedge_take(
    remaining_size: Decimal,
    level_size: Decimal,
    current_hedge_shares: Decimal,
    max_hedge_usdc: Decimal,
    candidate_worst_hedge_ask: Decimal,
    tick_size: Decimal,
) -> Decimal {
    if max_hedge_usdc == Decimal::MAX {
        return remaining_size.min(level_size);
    }

    let candidate_limit_price = candidate_worst_hedge_ask + tick_size;
    if candidate_limit_price <= Decimal::ZERO || max_hedge_usdc <= Decimal::ZERO {
        return Decimal::ZERO;
    }

    let max_total_hedge_shares =
        normalize_share_size((max_hedge_usdc / candidate_limit_price).floor());
    let affordable_increment =
        normalize_share_size((max_total_hedge_shares - current_hedge_shares).max(Decimal::ZERO));

    remaining_size.min(level_size).min(affordable_increment)
}

fn advance_level_if_empty(
    ptr: &mut usize,
    remaining_at_level: &mut Decimal,
    levels: &[PriceLevel],
) {
    if *remaining_at_level <= Decimal::ZERO {
        *ptr += 1;
        if *ptr < levels.len() {
            *remaining_at_level = levels[*ptr].size;
        }
    }
}

/// Truncate a share size to 2 decimal places (toward zero).
pub fn normalize_share_size(size: Decimal) -> Decimal {
    let factor = Decimal::new(100, 0);
    if size >= Decimal::ZERO {
        (size * factor).floor() / factor
    } else {
        (size * factor).ceil() / factor
    }
}

fn buy_hedge_limit_price() -> Decimal {
    Decimal::new(99, 2)
}

fn sell_hedge_limit_price() -> Decimal {
    Decimal::new(1, 2)
}

fn classify_buy_hedge_verification(order: Option<&LiveOrder>) -> HedgeVerificationState {
    match order {
        Some(order) if order.size_matched > Decimal::ZERO => HedgeVerificationState::VerifiedFilled,
        Some(_) => HedgeVerificationState::VerifiedZeroFill,
        None => HedgeVerificationState::Unknown,
    }
}

fn hedge_lookup_status_label(status: crate::models::OrderStatus) -> String {
    match status {
        crate::models::OrderStatus::Matched => "matched",
        crate::models::OrderStatus::Live | crate::models::OrderStatus::Delayed => "live",
        crate::models::OrderStatus::Cancelled => "cancelled",
        crate::models::OrderStatus::Invalid => "invalid",
    }
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{OrderStatus, OrderType, Outcome};
    use rust_decimal_macros::dec;

    fn sample_live_order(size_matched: Decimal) -> LiveOrder {
        LiveOrder {
            id: "order".to_string(),
            condition_id: "market".to_string(),
            asset_id: "asset".to_string(),
            side: Side::Buy,
            price: dec!(0.99),
            original_size: dec!(10),
            size_matched,
            outcome: Outcome::Yes,
            order_type: OrderType::GTC,
            status: OrderStatus::Live,
            created_at: chrono::Utc::now(),
            associated_trade_ids: Vec::new(),
        }
    }

    #[test]
    fn classify_buy_hedge_verification_marks_positive_fill_as_verified() {
        assert_eq!(
            classify_buy_hedge_verification(Some(&sample_live_order(dec!(1.25)))),
            HedgeVerificationState::VerifiedFilled
        );
    }

    #[test]
    fn classify_buy_hedge_verification_marks_zero_fill_as_unfilled() {
        assert_eq!(
            classify_buy_hedge_verification(Some(&sample_live_order(Decimal::ZERO))),
            HedgeVerificationState::VerifiedZeroFill
        );
    }

    #[test]
    fn classify_buy_hedge_verification_marks_missing_lookup_as_unknown() {
        assert_eq!(
            classify_buy_hedge_verification(None),
            HedgeVerificationState::Unknown
        );
    }

    #[test]
    fn hedge_verification_metadata_captures_trade_ids() {
        let metadata = HedgeVerificationMetadata::with_trade_ids(Some(&OrderResult {
            order_id: "hedge-order".to_string(),
            status: OrderStatus::Matched,
            trade_ids: vec!["trade-1".to_string(), "trade-2".to_string()],
        }));

        assert_eq!(
            metadata.trade_ids,
            vec!["trade-1".to_string(), "trade-2".to_string()]
        );
    }

    #[test]
    fn hedge_lookup_status_label_maps_delayed_to_live() {
        assert_eq!(hedge_lookup_status_label(OrderStatus::Delayed), "live");
        assert_eq!(hedge_lookup_status_label(OrderStatus::Matched), "matched");
        assert_eq!(
            hedge_lookup_status_label(OrderStatus::Cancelled),
            "cancelled"
        );
    }

    #[test]
    fn normalize_share_size_truncates_toward_zero() {
        assert_eq!(normalize_share_size(dec!(176.959)), dec!(176.95));
        assert_eq!(normalize_share_size(dec!(-176.959)), dec!(-176.95));
    }

    // ── compute_hedge_resolution tests ────────────────────────────────

    fn pl(price: Decimal, size: Decimal) -> PriceLevel {
        PriceLevel { price, size }
    }

    const TICK: Decimal = dec!(0.01);

    /// Perfect hedge: fill_price + hedge_ask = $1.00 → all shares hedge, zero cost.
    #[test]
    fn resolution_perfect_hedge_all_shares_hedge() {
        let asks = vec![pl(dec!(0.26), dec!(400))];
        let bids = vec![pl(dec!(0.73), dec!(400))];
        let res = compute_hedge_resolution(dec!(0.74), &asks, &bids, dec!(373), TICK);
        assert_eq!(res.hedge_shares, dec!(373));
        assert_eq!(res.sellback_shares, Decimal::ZERO);
        assert_eq!(res.hedge_limit_price, dec!(0.27)); // 0.26 + 0.01 tick
    }

    /// Hedge cheaper than sell-back across all depth → all shares hedge.
    #[test]
    fn resolution_hedge_cheaper_all_shares_hedge() {
        // hedge cost = 0.74 + 0.25 - 1.00 = -0.01 (profit!)
        // sellback cost = 0.74 - 0.70 = 0.04
        let asks = vec![pl(dec!(0.25), dec!(500))];
        let bids = vec![pl(dec!(0.70), dec!(500))];
        let res = compute_hedge_resolution(dec!(0.74), &asks, &bids, dec!(100), TICK);
        assert_eq!(res.hedge_shares, dec!(100));
        assert_eq!(res.sellback_shares, Decimal::ZERO);
    }

    /// Sell-back cheaper → all shares sell back.
    #[test]
    fn resolution_sellback_cheaper_all_shares_sellback() {
        // hedge cost = 0.74 + 0.40 - 1.00 = 0.14
        // sellback cost = 0.74 - 0.73 = 0.01
        let asks = vec![pl(dec!(0.40), dec!(500))];
        let bids = vec![pl(dec!(0.73), dec!(500))];
        let res = compute_hedge_resolution(dec!(0.74), &asks, &bids, dec!(100), TICK);
        assert_eq!(res.hedge_shares, Decimal::ZERO);
        assert_eq!(res.sellback_shares, dec!(100));
        assert_eq!(res.sellback_limit_price, dec!(0.73));
    }

    /// Mixed: cheap hedge depth exhausted, then sell-back cheaper → split.
    #[test]
    fn resolution_mixed_split() {
        // Level 1: hedge @ 0.26 → cost = 0.00. sellback @ 0.73 → cost = 0.01. Hedge wins.
        // Level 2: hedge @ 0.30 → cost = 0.04. sellback @ 0.73 → cost = 0.01. Sellback wins.
        let asks = vec![pl(dec!(0.26), dec!(200)), pl(dec!(0.30), dec!(200))];
        let bids = vec![pl(dec!(0.73), dec!(500))];
        let res = compute_hedge_resolution(dec!(0.74), &asks, &bids, dec!(373), TICK);
        assert_eq!(res.hedge_shares, dec!(200));
        assert_eq!(res.sellback_shares, dec!(173));
        assert_eq!(res.hedge_limit_price, dec!(0.27)); // worst ask 0.26 + tick
        assert_eq!(res.sellback_limit_price, dec!(0.73));
    }

    /// Tie goes to sell-back so capital is reclaimed immediately.
    #[test]
    fn resolution_tie_prefers_sellback() {
        // hedge cost = 0.74 + 0.27 - 1.00 = 0.01
        // sellback cost = 0.74 - 0.73 = 0.01
        let asks = vec![pl(dec!(0.27), dec!(500))];
        let bids = vec![pl(dec!(0.73), dec!(500))];
        let res = compute_hedge_resolution(dec!(0.74), &asks, &bids, dec!(100), TICK);
        assert_eq!(res.hedge_shares, Decimal::ZERO);
        assert_eq!(res.sellback_shares, dec!(100));
    }

    /// Empty opposite book → all shares sell back.
    #[test]
    fn resolution_empty_hedge_book_all_sellback() {
        let asks: Vec<PriceLevel> = vec![];
        let bids = vec![pl(dec!(0.73), dec!(500))];
        let res = compute_hedge_resolution(dec!(0.74), &asks, &bids, dec!(100), TICK);
        assert_eq!(res.hedge_shares, Decimal::ZERO);
        assert_eq!(res.sellback_shares, dec!(100));
    }

    /// Empty sell-back book → all shares hedge.
    #[test]
    fn resolution_empty_sellback_book_all_hedge() {
        let asks = vec![pl(dec!(0.26), dec!(500))];
        let bids: Vec<PriceLevel> = vec![];
        let res = compute_hedge_resolution(dec!(0.74), &asks, &bids, dec!(100), TICK);
        assert_eq!(res.hedge_shares, dec!(100));
        assert_eq!(res.sellback_shares, Decimal::ZERO);
    }

    /// Both books empty → zero resolution (remaining escalates to kill_market).
    #[test]
    fn resolution_both_books_empty() {
        let asks: Vec<PriceLevel> = vec![];
        let bids: Vec<PriceLevel> = vec![];
        let res = compute_hedge_resolution(dec!(0.74), &asks, &bids, dec!(100), TICK);
        assert_eq!(res.hedge_shares, Decimal::ZERO);
        assert_eq!(res.sellback_shares, Decimal::ZERO);
    }

    /// Incident replay: 373 @ 0.74, thin book, split decision.
    #[test]
    fn resolution_incident_replay() {
        let asks = vec![
            pl(dec!(0.26), dec!(200)),
            pl(dec!(0.27), dec!(150)),
            pl(dec!(0.30), dec!(100)),
        ];
        let bids = vec![pl(dec!(0.73), dec!(300)), pl(dec!(0.72), dec!(200))];
        let res = compute_hedge_resolution(dec!(0.74), &asks, &bids, dec!(373), TICK);
        // 200 @ 0.26 (cost 0.00 vs 0.01) → hedge
        // Remaining 173 @ 0.27 / 0.73 (cost 0.01 vs 0.01) → sellback (tie)
        assert_eq!(res.hedge_shares, dec!(200));
        assert_eq!(res.sellback_shares, dec!(173));
        assert_eq!(res.hedge_limit_price, dec!(0.27)); // 0.26 + tick
        assert_eq!(res.sellback_limit_price, dec!(0.73));
    }

    // ── budget-aware planner tests ────────────────────────────────────

    /// Budget pressure reroutes hedge overflow into sell-back at live bid depth.
    #[test]
    fn resolution_budget_constrained_routes_excess_to_sellback() {
        let asks = vec![pl(dec!(0.26), dec!(400))];
        let bids = vec![pl(dec!(0.73), dec!(500))];

        let res = plan_fill_resolution(dec!(0.74), &asks, &bids, dec!(373), dec!(50), TICK);

        // Limit price is 0.27, so the affordable hedge size is floor(50 / 0.27) = 185.
        assert_eq!(res.hedge_shares, dec!(185));
        assert_eq!(res.hedge_limit_price, dec!(0.27));
        assert_eq!(res.sellback_shares, dec!(188));
        assert_eq!(res.sellback_limit_price, dec!(0.73));
        assert_eq!(res.unresolved_shares, Decimal::ZERO);
    }

    /// Budget is applied against the final hedge limit price, not raw ask prices.
    #[test]
    fn resolution_budget_constrained_across_multiple_hedge_levels() {
        let asks = vec![pl(dec!(0.26), dec!(200)), pl(dec!(0.27), dec!(200))];
        let bids = vec![pl(dec!(0.73), dec!(500))];

        let res = plan_fill_resolution(dec!(0.74), &asks, &bids, dec!(373), dec!(80), TICK);

        // Equality at the second hedge level now routes remaining shares to sell-back.
        assert_eq!(res.hedge_shares, dec!(200));
        assert_eq!(res.hedge_limit_price, dec!(0.27));
        assert_eq!(res.sellback_shares, dec!(173));
        assert_eq!(res.sellback_limit_price, dec!(0.73));
        assert_eq!(res.unresolved_shares, Decimal::ZERO);
    }

    /// Zero budget still produces a valid sell-back-only plan.
    #[test]
    fn resolution_sellback_only_is_valid_when_budget_is_zero() {
        let asks = vec![pl(dec!(0.26), dec!(500))];
        let bids = vec![pl(dec!(0.73), dec!(500))];

        let res = plan_fill_resolution(dec!(0.74), &asks, &bids, dec!(100), Decimal::ZERO, TICK);

        assert_eq!(res.hedge_shares, Decimal::ZERO);
        assert_eq!(res.hedge_limit_price, Decimal::ZERO);
        assert_eq!(res.sellback_shares, dec!(100));
        assert_eq!(res.sellback_limit_price, dec!(0.73));
        assert_eq!(res.unresolved_shares, Decimal::ZERO);
    }

    /// When neither budget nor sell-back depth can absorb the full fill, the residual stays explicit.
    #[test]
    fn resolution_budget_constrained_leaves_unresolved_without_sellback_depth() {
        let asks = vec![pl(dec!(0.26), dec!(500))];
        let bids: Vec<PriceLevel> = vec![];

        let res = plan_fill_resolution(dec!(0.74), &asks, &bids, dec!(373), dec!(50), TICK);

        assert_eq!(res.hedge_shares, dec!(185));
        assert_eq!(res.hedge_limit_price, dec!(0.27));
        assert_eq!(res.sellback_shares, Decimal::ZERO);
        assert_eq!(res.sellback_limit_price, Decimal::ZERO);
        assert_eq!(res.unresolved_shares, dec!(188));
    }

    #[test]
    fn sell_hedge_limit_price_remains_legacy_any_price_exit() {
        assert_eq!(sell_hedge_limit_price(), dec!(0.01));
    }
}
