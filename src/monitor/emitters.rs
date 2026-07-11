use rust_decimal::Decimal;
use serde::Serialize;
use spreadeater_core::payloads::{
    CalibrationAdjustedPayload, DecisionEventPayload, FillDetectedPayload, HedgeDecisionPayload,
    HedgeExitPathPayload, HedgeIntentPayload, HedgeResultPayload, MonitorDegradedPayload,
    NeutralityPayload, OrderCancelledPayload, OrderEventDiagnostics, OrderResizedPayload,
    OrderSubmittedPayload, QuoteLegSummary, RiskStateChangedPayload, StatusSnapshotPayload,
    UserStreamStatusChangedPayload, WatchdogKillTriggeredPayload, WatchdogVerdictPayload,
};
use spreadeater_core::{CancelReasonCode, EventEnvelope, EventType, Priority};

use crate::books::BookWsStatsSnapshot;
use crate::models::{CanonicalMarket, DecisionReport, Position, QuoteCandidate, Side, TradeEvent};
use crate::strategy::CalibrationAdjustment;
use crate::trading::hedge_executor::{HedgeIntent, HedgeResolution, HedgeResult};
use crate::trading::order_manager::TrackedOrder;

#[derive(Debug, Clone, Copy, Default)]
pub struct DecisionRankingContext<'a> {
    pub rank_in_cycle: Option<u64>,
    pub ranked_market_count: Option<u64>,
    pub ranking_metric_name: Option<&'a str>,
    pub ranking_metric_value: Option<Decimal>,
    pub frontier_eligible: Option<bool>,
    pub frontier_requires_reallocation: Option<bool>,
    pub frontier_replaces_condition_id: Option<&'a str>,
    pub frontier_replaced_by_condition_id: Option<&'a str>,
    pub frontier_counterfactual_budget_usd: Option<Decimal>,
    pub frontier_counterfactual_reclaimable_bid_capital_usd: Option<Decimal>,
    pub frontier_counterfactual_entrant_condition_id: Option<&'a str>,
    pub frontier_counterfactual_entrant_ranking_metric_name: Option<&'a str>,
    pub frontier_counterfactual_entrant_ranking_metric_value: Option<Decimal>,
    pub frontier_counterfactual_entrant_expected_reward_usd_day: Option<Decimal>,
    pub frontier_counterfactual_loser_condition_id: Option<&'a str>,
    pub frontier_counterfactual_loser_ranking_metric_name: Option<&'a str>,
    pub frontier_counterfactual_loser_ranking_metric_value: Option<Decimal>,
    pub frontier_counterfactual_loser_expected_reward_usd_day: Option<Decimal>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct HedgeIntentContext<'a> {
    pub resolution: Option<&'a HedgeResolution>,
    pub pre_resolution_active_orders: Option<u64>,
    pub pre_resolution_pending_cancels: Option<u64>,
    pub cancel_wait_drained: Option<bool>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct HedgeDecisionContext<'a> {
    pub resolution: Option<&'a HedgeResolution>,
    pub available_hedge_budget_usd: Decimal,
    pub filled_best_bid_price: Option<Decimal>,
    pub filled_best_bid_size: Option<Decimal>,
    pub opposite_best_ask_price: Option<Decimal>,
    pub opposite_best_ask_size: Option<Decimal>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct HedgeResultContext<'a> {
    pub hedge_result: Option<&'a HedgeResult>,
    pub aggregate_success: bool,
    pub aggregate_failure_reason: Option<&'a str>,
    pub sellback_order_id: Option<&'a str>,
    pub sellback_price: Option<Decimal>,
    pub sellback_execution_limit_price: Option<Decimal>,
    pub sellback_leg_status: Option<&'a str>,
    pub sellback_response_status: Option<&'a str>,
    pub sellback_lookup_status: Option<&'a str>,
    pub sellback_lookup_matched_shares: Option<Decimal>,
    pub sellback_lookup_error: Option<&'a str>,
    pub sellback_trade_ids: Option<&'a [String]>,
    pub post_sync_net_exposure: Option<Decimal>,
    pub post_sync_yes_size: Option<Decimal>,
    pub post_sync_no_size: Option<Decimal>,
    pub post_sync_source: Option<&'a str>,
    pub halt_signal_suppressed: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct HedgeExitPathContext<'a> {
    pub post_position: &'a Position,
    pub post_sync_source: &'a str,
    pub exit_path_status: &'a str,
    pub merge_eligible_pairs: Decimal,
    pub ctf_merge_configured: bool,
    pub merge_attempted: bool,
    pub merge_tx_hash: Option<&'a str>,
    pub merge_failure_reason: Option<&'a str>,
    pub fallback_asks_attempted: bool,
    pub fallback_ask_count: u64,
    pub fallback_failure_reason: Option<&'a str>,
}

pub fn build_decision_evaluated(
    run_id: &str,
    cycle_id: &str,
    mode: &str,
    source_component: &str,
    market: &CanonicalMarket,
    report: &DecisionReport,
    committed_capital_usd: Decimal,
    competition_multiplier_used: Option<Decimal>,
    api_balance_usd: Option<Decimal>,
    available_budget_usd: Option<Decimal>,
    ranking_context: DecisionRankingContext<'_>,
) -> EventEnvelope {
    let expected_reward = report.reward_viability.as_ref().map(|v| v.estimated_reward);
    let expected_hedge_cost = report
        .reward_viability
        .as_ref()
        .map(|v| v.estimated_hedge_cost);
    let expected_edge = report.reward_viability.as_ref().map(|v| v.estimated_edge);
    let expected_edge_pct = report.reward_viability.as_ref().and_then(|v| {
        if report.effective_quote_size > Decimal::ZERO {
            Some((v.estimated_edge / report.effective_quote_size) * Decimal::from(100))
        } else {
            None
        }
    });

    let payload = DecisionEventPayload {
        candidate_quotes: report
            .candidate_quotes
            .iter()
            .map(candidate_to_summary)
            .collect(),
        reasons: report.reasons.clone(),
        effective_quote_size: report.effective_quote_size,
        expected_reward_usd_day: expected_reward,
        expected_hedge_cost_usd: expected_hedge_cost,
        expected_edge_usd: expected_edge,
        expected_edge_pct,
        committed_capital_usd: Some(committed_capital_usd),
        score_share: report.score_proxy,
        max_hedgeable_size: report
            .candidate_quotes
            .iter()
            .map(|candidate| candidate.size)
            .max(),
        competition_multiplier_used,
        api_balance_usd,
        available_budget_usd,
        rank_in_cycle: ranking_context.rank_in_cycle,
        ranked_market_count: ranking_context.ranked_market_count,
        ranking_metric_name: ranking_context.ranking_metric_name.map(str::to_string),
        ranking_metric_value: ranking_context.ranking_metric_value,
        frontier_eligible: ranking_context.frontier_eligible,
        frontier_requires_reallocation: ranking_context.frontier_requires_reallocation,
        frontier_replaces_condition_id: ranking_context
            .frontier_replaces_condition_id
            .map(str::to_string),
        frontier_replaced_by_condition_id: ranking_context
            .frontier_replaced_by_condition_id
            .map(str::to_string),
        frontier_counterfactual_budget_usd: ranking_context.frontier_counterfactual_budget_usd,
        frontier_counterfactual_reclaimable_bid_capital_usd: ranking_context
            .frontier_counterfactual_reclaimable_bid_capital_usd,
        frontier_counterfactual_entrant_condition_id: ranking_context
            .frontier_counterfactual_entrant_condition_id
            .map(str::to_string),
        frontier_counterfactual_entrant_ranking_metric_name: ranking_context
            .frontier_counterfactual_entrant_ranking_metric_name
            .map(str::to_string),
        frontier_counterfactual_entrant_ranking_metric_value: ranking_context
            .frontier_counterfactual_entrant_ranking_metric_value,
        frontier_counterfactual_entrant_expected_reward_usd_day: ranking_context
            .frontier_counterfactual_entrant_expected_reward_usd_day,
        frontier_counterfactual_loser_condition_id: ranking_context
            .frontier_counterfactual_loser_condition_id
            .map(str::to_string),
        frontier_counterfactual_loser_ranking_metric_name: ranking_context
            .frontier_counterfactual_loser_ranking_metric_name
            .map(str::to_string),
        frontier_counterfactual_loser_ranking_metric_value: ranking_context
            .frontier_counterfactual_loser_ranking_metric_value,
        frontier_counterfactual_loser_expected_reward_usd_day: ranking_context
            .frontier_counterfactual_loser_expected_reward_usd_day,
        would_trade: report.would_trade,
    };

    EventEnvelope::new(
        EventType::DecisionEvaluated,
        Priority::Normal,
        run_id.to_string(),
        source_component.to_string(),
        mode.to_string(),
        to_payload_value(payload),
    )
    .with_cycle_id(cycle_id.to_string())
    .with_condition_id(market.condition_id.clone())
    .with_market_slug(market.market_slug.clone())
    .with_question(market.question.clone())
}

pub fn build_quote_approved(
    run_id: &str,
    cycle_id: &str,
    trace_id: &str,
    mode: &str,
    source_component: &str,
    market: &CanonicalMarket,
    candidate: &QuoteCandidate,
) -> EventEnvelope {
    build_quote_event(
        EventType::QuoteApproved,
        run_id,
        cycle_id,
        trace_id,
        mode,
        source_component,
        market,
        candidate,
    )
}

pub fn build_quote_rejected(
    run_id: &str,
    cycle_id: &str,
    trace_id: &str,
    mode: &str,
    source_component: &str,
    market: &CanonicalMarket,
    candidate: &QuoteCandidate,
) -> EventEnvelope {
    build_quote_event(
        EventType::QuoteRejected,
        run_id,
        cycle_id,
        trace_id,
        mode,
        source_component,
        market,
        candidate,
    )
}

pub fn build_order_submitted(
    run_id: &str,
    trace_id: &str,
    mode: &str,
    tracked_order: &TrackedOrder,
    origin: Option<&str>,
    role: Option<&str>,
) -> EventEnvelope {
    let payload = OrderSubmittedPayload {
        leg: tracked_order.leg.to_string(),
        side: tracked_order.side.to_string(),
        price: tracked_order.price,
        size: tracked_order.size,
        matched_size: tracked_order.matched_size,
        token_id: tracked_order.token_id.clone(),
        neg_risk: tracked_order.neg_risk,
        origin: origin.map(str::to_string),
        role: role.map(str::to_string),
    };

    EventEnvelope::new(
        EventType::OrderSubmitted,
        Priority::High,
        run_id.to_string(),
        "order_manager".to_string(),
        mode.to_string(),
        to_payload_value(payload),
    )
    .with_trace_id(trace_id.to_string())
    .with_condition_id(tracked_order.condition_id.clone())
    .with_order_id(tracked_order.order_id.clone())
    .with_asset_id(tracked_order.token_id.clone())
}

pub fn build_order_cancelled(
    run_id: &str,
    trace_id: &str,
    mode: &str,
    tracked_order: &TrackedOrder,
    reason_code: CancelReasonCode,
    origin: Option<&str>,
    diagnostics: Option<&OrderEventDiagnostics>,
) -> EventEnvelope {
    let capital_delta = if tracked_order.side == crate::models::Side::Buy {
        Some(tracked_order.price * tracked_order.size)
    } else {
        None
    };

    let payload = OrderCancelledPayload {
        reason_code,
        reason_text: reason_code.description().to_string(),
        old_size: tracked_order.size,
        capital_delta,
        origin: origin.map(str::to_string),
        diagnostics: diagnostics.cloned(),
    };

    EventEnvelope::new(
        EventType::OrderCancelled,
        Priority::High,
        run_id.to_string(),
        "order_manager".to_string(),
        mode.to_string(),
        to_payload_value(payload),
    )
    .with_trace_id(trace_id.to_string())
    .with_condition_id(tracked_order.condition_id.clone())
    .with_order_id(tracked_order.order_id.clone())
    .with_asset_id(tracked_order.token_id.clone())
}

pub fn build_order_resized(
    run_id: &str,
    trace_id: &str,
    mode: &str,
    old_order: &TrackedOrder,
    new_order: &TrackedOrder,
    reason_code: CancelReasonCode,
    origin: Option<&str>,
    diagnostics: Option<&OrderEventDiagnostics>,
) -> EventEnvelope {
    let payload = OrderResizedPayload {
        old_order_id: old_order.order_id.clone(),
        new_order_id: new_order.order_id.clone(),
        old_size: old_order.size,
        new_size: new_order.size,
        old_price: old_order.price,
        new_price: new_order.price,
        reason_code,
        origin: origin.map(str::to_string),
        diagnostics: diagnostics.cloned(),
    };

    EventEnvelope::new(
        EventType::OrderResized,
        Priority::High,
        run_id.to_string(),
        "order_manager".to_string(),
        mode.to_string(),
        to_payload_value(payload),
    )
    .with_trace_id(trace_id.to_string())
    .with_condition_id(new_order.condition_id.clone())
    .with_order_id(new_order.order_id.clone())
    .with_asset_id(new_order.token_id.clone())
}

pub fn build_fill_detected(
    run_id: &str,
    trace_id: &str,
    mode: &str,
    source_component: &str,
    trade_event: &TradeEvent,
    order_id: Option<&str>,
    match_source: Option<&str>,
    fallback_match: bool,
    anchored_order_id: Option<&str>,
    deferred_to_reconciliation: bool,
) -> EventEnvelope {
    let payload = FillDetectedPayload {
        trade_id: trade_event.id.clone(),
        fill_price: trade_event.price,
        fill_size: trade_event.size,
        side: trade_event.side.to_string(),
        outcome: trade_event.outcome.clone(),
        match_source: match_source.map(str::to_string),
        fallback_match,
        anchored_order_id: anchored_order_id.map(str::to_string),
        deferred_to_reconciliation,
    };

    let mut event = EventEnvelope::new(
        EventType::FillDetected,
        Priority::Critical,
        run_id.to_string(),
        source_component.to_string(),
        mode.to_string(),
        to_payload_value(payload),
    )
    .with_trace_id(trace_id.to_string())
    .with_condition_id(trade_event.condition_id.clone())
    .with_asset_id(trade_event.asset_id.clone());

    if let Some(order_id) = order_id {
        event = event.with_order_id(order_id.to_string());
    }

    event
}

pub fn build_hedge_intent(
    run_id: &str,
    trace_id: &str,
    hedge_id: &str,
    mode: &str,
    source_component: &str,
    intent: &HedgeIntent,
    context: HedgeIntentContext<'_>,
    origin: Option<&str>,
) -> EventEnvelope {
    let payload = HedgeIntentPayload {
        trigger_order_id: intent.trigger_order_id.clone(),
        trigger_leg: intent.trigger_leg.to_string(),
        fill_size: intent.fill_size,
        fill_price: intent.fill_price,
        hedge_token_id: intent.hedge_token_id.clone(),
        hedge_side: intent.hedge_side.to_string(),
        planned_hedge_shares: context
            .resolution
            .filter(|resolution| resolution.hedge_shares > Decimal::ZERO)
            .map(|resolution| resolution.hedge_shares),
        planned_hedge_price: context
            .resolution
            .filter(|resolution| resolution.hedge_shares > Decimal::ZERO)
            .map(|resolution| resolution.hedge_limit_price),
        planned_sellback_shares: context
            .resolution
            .filter(|resolution| resolution.sellback_shares > Decimal::ZERO)
            .map(|resolution| resolution.sellback_shares),
        planned_sellback_price: context
            .resolution
            .filter(|resolution| resolution.sellback_shares > Decimal::ZERO)
            .map(|resolution| resolution.sellback_limit_price),
        planned_sellback_reference_bid: context
            .resolution
            .filter(|resolution| resolution.sellback_shares > Decimal::ZERO)
            .map(|resolution| resolution.sellback_limit_price),
        unresolved_shares: context
            .resolution
            .map(|resolution| resolution.unresolved_shares),
        pre_resolution_active_orders: context.pre_resolution_active_orders,
        pre_resolution_pending_cancels: context.pre_resolution_pending_cancels,
        cancel_wait_drained: context.cancel_wait_drained,
        origin: origin.map(str::to_string),
    };

    EventEnvelope::new(
        EventType::HedgeIntentCreated,
        Priority::Critical,
        run_id.to_string(),
        source_component.to_string(),
        mode.to_string(),
        to_payload_value(payload),
    )
    .with_trace_id(trace_id.to_string())
    .with_condition_id(intent.condition_id.clone())
    .with_order_id(intent.trigger_order_id.clone())
    .with_asset_id(intent.hedge_token_id.clone())
    .with_hedge_id(hedge_id.to_string())
}

pub fn build_hedge_decision(
    run_id: &str,
    trace_id: &str,
    hedge_id: &str,
    mode: &str,
    source_component: &str,
    intent: &HedgeIntent,
    context: HedgeDecisionContext<'_>,
) -> EventEnvelope {
    let payload = HedgeDecisionPayload {
        trigger_leg: intent.trigger_leg.to_string(),
        hedge_side: intent.hedge_side.to_string(),
        fill_size: intent.fill_size,
        fill_price: intent.fill_price,
        decision_mode: hedge_decision_mode(intent.hedge_side).to_string(),
        decision_reason_code: hedge_decision_reason_code(intent, context).to_string(),
        available_hedge_budget_usd: context.available_hedge_budget_usd,
        filled_best_bid_price: context.filled_best_bid_price,
        filled_best_bid_size: context.filled_best_bid_size,
        opposite_best_ask_price: context.opposite_best_ask_price,
        opposite_best_ask_size: context.opposite_best_ask_size,
        planned_hedge_shares: planned_hedge_shares(intent, context.resolution),
        planned_hedge_price: planned_hedge_price(intent, context.resolution),
        planned_sellback_shares: context
            .resolution
            .map(|resolution| resolution.sellback_shares)
            .unwrap_or(Decimal::ZERO),
        planned_sellback_price: context
            .resolution
            .map(|resolution| resolution.sellback_limit_price)
            .unwrap_or(Decimal::ZERO),
        unresolved_shares: context
            .resolution
            .map(|resolution| resolution.unresolved_shares)
            .unwrap_or(Decimal::ZERO),
    };

    EventEnvelope::new(
        EventType::HedgeDecisionEvaluated,
        Priority::Critical,
        run_id.to_string(),
        source_component.to_string(),
        mode.to_string(),
        to_payload_value(payload),
    )
    .with_trace_id(trace_id.to_string())
    .with_condition_id(intent.condition_id.clone())
    .with_order_id(intent.trigger_order_id.clone())
    .with_asset_id(intent.hedge_token_id.clone())
    .with_hedge_id(hedge_id.to_string())
}

pub fn build_hedge_result(
    run_id: &str,
    trace_id: &str,
    hedge_id: &str,
    mode: &str,
    source_component: &str,
    intent: &HedgeIntent,
    context: HedgeResultContext<'_>,
    latency_ms: u64,
    origin: Option<&str>,
) -> EventEnvelope {
    let payload = HedgeResultPayload {
        hedge_order_id: context
            .hedge_result
            .and_then(|result| result.order_result.as_ref())
            .map(|order| order.order_id.clone()),
        result_status: if context.aggregate_success {
            "success".to_string()
        } else {
            "failed".to_string()
        },
        hedge_price: context.hedge_result.and_then(|result| result.hedge_price),
        hedge_leg_status: Some(hedge_leg_status(context.hedge_result).to_string()),
        hedge_cancel_status: context
            .hedge_result
            .and_then(|result| result.verification_metadata.cancel_status.clone()),
        hedge_cancel_reason: context
            .hedge_result
            .and_then(|result| result.verification_metadata.cancel_reason.clone()),
        hedge_lookup_status: context
            .hedge_result
            .and_then(|result| result.verification_metadata.lookup_status.clone()),
        hedge_lookup_matched_shares: context
            .hedge_result
            .and_then(|result| result.verification_metadata.lookup_matched_shares),
        hedge_lookup_error: context
            .hedge_result
            .and_then(|result| result.verification_metadata.lookup_error.clone()),
        hedge_trade_ids: context.hedge_result.and_then(|result| {
            (!result.verification_metadata.trade_ids.is_empty())
                .then(|| result.verification_metadata.trade_ids.clone())
        }),
        sellback_order_id: context.sellback_order_id.map(str::to_string),
        sellback_price: context.sellback_price,
        sellback_execution_limit_price: context.sellback_execution_limit_price,
        sellback_leg_status: context.sellback_leg_status.map(str::to_string),
        sellback_response_status: context.sellback_response_status.map(str::to_string),
        sellback_lookup_status: context.sellback_lookup_status.map(str::to_string),
        sellback_lookup_matched_shares: context.sellback_lookup_matched_shares,
        sellback_lookup_error: context.sellback_lookup_error.map(str::to_string),
        sellback_trade_ids: context
            .sellback_trade_ids
            .map(|trade_ids| trade_ids.to_vec()),
        post_sync_net_exposure: context.post_sync_net_exposure,
        post_sync_yes_size: context.post_sync_yes_size,
        post_sync_no_size: context.post_sync_no_size,
        post_sync_source: context.post_sync_source.map(str::to_string),
        halt_signal_suppressed: context.halt_signal_suppressed,
        failure_reason: context.aggregate_failure_reason.map(str::to_string),
        latency_ms,
        origin: origin.map(str::to_string),
    };

    EventEnvelope::new(
        EventType::HedgeResultRecorded,
        Priority::Critical,
        run_id.to_string(),
        source_component.to_string(),
        mode.to_string(),
        to_payload_value(payload),
    )
    .with_trace_id(trace_id.to_string())
    .with_condition_id(intent.condition_id.clone())
    .with_order_id(intent.trigger_order_id.clone())
    .with_asset_id(intent.hedge_token_id.clone())
    .with_hedge_id(hedge_id.to_string())
}

pub fn build_hedge_exit_path(
    run_id: &str,
    trace_id: &str,
    hedge_id: &str,
    mode: &str,
    source_component: &str,
    intent: &HedgeIntent,
    context: HedgeExitPathContext<'_>,
) -> EventEnvelope {
    let payload = HedgeExitPathPayload {
        post_sync_yes_size: context.post_position.yes_size,
        post_sync_no_size: context.post_position.no_size,
        post_sync_net_exposure: context.post_position.net_exposure().abs(),
        post_sync_complete_sets: context.post_position.complete_sets(),
        post_sync_source: context.post_sync_source.to_string(),
        exit_path_status: context.exit_path_status.to_string(),
        merge_eligible_pairs: context.merge_eligible_pairs,
        ctf_merge_configured: context.ctf_merge_configured,
        merge_attempted: context.merge_attempted,
        merge_tx_hash: context.merge_tx_hash.map(str::to_string),
        merge_failure_reason: context.merge_failure_reason.map(str::to_string),
        fallback_asks_attempted: context.fallback_asks_attempted,
        fallback_ask_count: context.fallback_ask_count,
        fallback_failure_reason: context.fallback_failure_reason.map(str::to_string),
    };

    EventEnvelope::new(
        EventType::HedgeExitPathRecorded,
        Priority::Critical,
        run_id.to_string(),
        source_component.to_string(),
        mode.to_string(),
        to_payload_value(payload),
    )
    .with_trace_id(trace_id.to_string())
    .with_condition_id(intent.condition_id.clone())
    .with_order_id(intent.trigger_order_id.clone())
    .with_asset_id(intent.hedge_token_id.clone())
    .with_hedge_id(hedge_id.to_string())
}

pub fn build_risk_state_changed(
    run_id: &str,
    mode: &str,
    condition_id: Option<&str>,
    status: &str,
    reason: Option<&str>,
    total_exposure: Option<Decimal>,
    global_halt: Option<bool>,
) -> EventEnvelope {
    let payload = RiskStateChangedPayload {
        scope: if condition_id.is_some() {
            "market".to_string()
        } else {
            "global".to_string()
        },
        status: status.to_string(),
        reason: reason.map(str::to_string),
        total_exposure,
        global_halt,
    };

    let mut event = EventEnvelope::new(
        EventType::RiskStateChanged,
        Priority::Critical,
        run_id.to_string(),
        "live_engine".to_string(),
        mode.to_string(),
        to_payload_value(payload),
    );

    if let Some(condition_id) = condition_id {
        event = event.with_condition_id(condition_id.to_string());
    }

    event
}

pub fn build_user_stream_status_changed(
    run_id: &str,
    mode: &str,
    status: &str,
    subscribed_markets: Option<u64>,
    detail: Option<&str>,
) -> EventEnvelope {
    let payload = UserStreamStatusChangedPayload {
        status: status.to_string(),
        subscribed_markets,
        detail: detail.map(str::to_string),
    };

    EventEnvelope::new(
        EventType::UserStreamStatusChanged,
        Priority::High,
        run_id.to_string(),
        "live_engine".to_string(),
        mode.to_string(),
        to_payload_value(payload),
    )
}

pub fn build_status_snapshot(
    run_id: &str,
    mode: &str,
    managed_markets: usize,
    order_committed_usd: Decimal,
    position_committed_usd: Decimal,
    total_committed_usd: Decimal,
    api_balance_usd: Decimal,
    available_budget_usd: Decimal,
    competition_multiplier: Decimal,
    total_est_daily_usd: Option<Decimal>,
    book_ws_stats: Option<BookWsStatsSnapshot>,
) -> EventEnvelope {
    let payload = StatusSnapshotPayload {
        managed_markets: managed_markets as u64,
        order_committed_usd,
        position_committed_usd,
        total_committed_usd,
        api_balance_usd,
        available_budget_usd,
        competition_multiplier,
        total_est_daily_usd,
        book_ws_accepted_messages: book_ws_stats.as_ref().map(|stats| stats.accepted_messages),
        book_ws_ignored_messages: book_ws_stats.as_ref().map(|stats| stats.ignored_messages),
        book_ws_parse_errors: book_ws_stats.as_ref().map(|stats| stats.parse_errors),
        book_ws_snapshot_events: book_ws_stats.as_ref().map(|stats| stats.snapshot_events),
        book_ws_delta_events: book_ws_stats.as_ref().map(|stats| stats.delta_events),
    };

    EventEnvelope::new(
        EventType::StatusSnapshot,
        Priority::Normal,
        run_id.to_string(),
        "live_engine".to_string(),
        mode.to_string(),
        to_payload_value(payload),
    )
}

pub fn build_calibration_adjusted(
    run_id: &str,
    mode: &str,
    adjustment: &CalibrationAdjustment,
) -> EventEnvelope {
    let payload = CalibrationAdjustedPayload {
        old_multiplier: adjustment.old_multiplier,
        new_multiplier: adjustment.new_multiplier,
        sample_count: adjustment.sample_count as u64,
        false_positives: adjustment.false_positives as u64,
        false_negatives: adjustment.false_negatives as u64,
    };

    EventEnvelope::new(
        EventType::CalibrationAdjusted,
        Priority::High,
        run_id.to_string(),
        "live_engine".to_string(),
        mode.to_string(),
        to_payload_value(payload),
    )
}

pub fn build_neutrality_evaluated(
    run_id: &str,
    trace_id: &str,
    mode: &str,
    market: &CanonicalMarket,
    pre_position: &Position,
    post_position: &Position,
    tolerance: Decimal,
) -> EventEnvelope {
    let residual_exposure = post_position.net_exposure().abs();
    let payload = NeutralityPayload {
        pre_yes_size: pre_position.yes_size,
        pre_no_size: pre_position.no_size,
        post_yes_size: post_position.yes_size,
        post_no_size: post_position.no_size,
        residual_exposure,
        complete_sets: post_position.complete_sets(),
        tolerance,
        is_neutral: residual_exposure <= tolerance,
    };

    EventEnvelope::new(
        EventType::NeutralityEvaluated,
        Priority::Critical,
        run_id.to_string(),
        "live_engine".to_string(),
        mode.to_string(),
        to_payload_value(payload),
    )
    .with_trace_id(trace_id.to_string())
    .with_condition_id(market.condition_id.clone())
    .with_market_slug(market.market_slug.clone())
    .with_question(market.question.clone())
}

pub fn build_monitor_degraded(
    run_id: &str,
    mode: &str,
    component: &str,
    reason: &str,
    queue_depth: Option<u64>,
) -> EventEnvelope {
    let payload = MonitorDegradedPayload {
        component: component.to_string(),
        degraded_reason: reason.to_string(),
        queue_depth,
        index_lag_ms: None,
    };

    EventEnvelope::new(
        EventType::MonitorDegraded,
        Priority::Critical,
        run_id.to_string(),
        "monitor".to_string(),
        mode.to_string(),
        to_payload_value(payload),
    )
}

fn build_quote_event(
    event_type: EventType,
    run_id: &str,
    cycle_id: &str,
    trace_id: &str,
    mode: &str,
    source_component: &str,
    market: &CanonicalMarket,
    candidate: &QuoteCandidate,
) -> EventEnvelope {
    EventEnvelope::new(
        event_type,
        Priority::Normal,
        run_id.to_string(),
        source_component.to_string(),
        mode.to_string(),
        to_payload_value(candidate_to_summary(candidate)),
    )
    .with_cycle_id(cycle_id.to_string())
    .with_trace_id(trace_id.to_string())
    .with_condition_id(market.condition_id.clone())
    .with_market_slug(market.market_slug.clone())
    .with_question(market.question.clone())
}

fn candidate_to_summary(candidate: &QuoteCandidate) -> QuoteLegSummary {
    QuoteLegSummary {
        leg: candidate.leg.to_string(),
        price: candidate.price,
        size: candidate.size,
        status: format!("{:?}", candidate.status),
        reason: candidate.reason.clone(),
    }
}

fn to_payload_value<T: Serialize>(payload: T) -> serde_json::Value {
    serde_json::to_value(payload).expect("payload serialization must succeed")
}

fn planned_hedge_shares(intent: &HedgeIntent, resolution: Option<&HedgeResolution>) -> Decimal {
    match (intent.hedge_side, resolution) {
        (Side::Buy, Some(resolution)) => resolution.hedge_shares,
        (Side::Buy, None) => intent.fill_size,
        (Side::Sell, _) => intent.fill_size,
    }
}

fn planned_hedge_price(intent: &HedgeIntent, resolution: Option<&HedgeResolution>) -> Decimal {
    match (intent.hedge_side, resolution) {
        (Side::Buy, Some(resolution)) => resolution.hedge_limit_price,
        (Side::Sell, _) => Decimal::new(1, 2),
        (Side::Buy, None) => Decimal::ZERO,
    }
}

fn hedge_decision_mode(hedge_side: Side) -> &'static str {
    match hedge_side {
        Side::Buy => "buy_side_resolution",
        Side::Sell => "sell_side_direct",
    }
}

fn hedge_decision_reason_code(
    intent: &HedgeIntent,
    context: HedgeDecisionContext<'_>,
) -> &'static str {
    if intent.hedge_side == Side::Sell {
        return "sell_side_direct";
    }

    let Some(resolution) = context.resolution else {
        return "no_resolution_available";
    };

    if resolution.hedge_shares <= Decimal::ZERO
        && resolution.sellback_shares <= Decimal::ZERO
        && resolution.unresolved_shares > Decimal::ZERO
    {
        return "no_resolution_available";
    }

    if resolution.sellback_shares > Decimal::ZERO
        && budget_rerouted_to_sellback(intent.fill_price, resolution, context)
    {
        return "budget_rerouted_to_sellback";
    }

    match compare_buy_side_resolution_costs(intent.fill_price, context) {
        Some(std::cmp::Ordering::Less) => "hedge_cheaper",
        Some(std::cmp::Ordering::Equal) => "tie_prefers_sellback",
        Some(std::cmp::Ordering::Greater) => "sellback_cheaper",
        None if resolution.hedge_shares > Decimal::ZERO => "hedge_cheaper",
        None if resolution.sellback_shares > Decimal::ZERO => "sellback_cheaper",
        None => "no_resolution_available",
    }
}

fn compare_buy_side_resolution_costs(
    fill_price: Decimal,
    context: HedgeDecisionContext<'_>,
) -> Option<std::cmp::Ordering> {
    let filled_best_bid_price = context.filled_best_bid_price?;
    let opposite_best_ask_price = context.opposite_best_ask_price?;
    let hedge_cost = fill_price + opposite_best_ask_price - Decimal::ONE;
    let sellback_cost = fill_price - filled_best_bid_price;
    Some(
        hedge_cost
            .partial_cmp(&sellback_cost)
            .unwrap_or(std::cmp::Ordering::Equal),
    )
}

fn budget_rerouted_to_sellback(
    fill_price: Decimal,
    resolution: &HedgeResolution,
    context: HedgeDecisionContext<'_>,
) -> bool {
    if resolution.sellback_shares <= Decimal::ZERO {
        return false;
    }

    let top_of_book_prefers_hedge = matches!(
        compare_buy_side_resolution_costs(fill_price, context),
        Some(std::cmp::Ordering::Less)
    );
    if !top_of_book_prefers_hedge {
        return false;
    }

    if context.available_hedge_budget_usd <= Decimal::ZERO {
        return true;
    }

    resolution.hedge_limit_price > Decimal::ZERO
        && resolution.hedge_shares * resolution.hedge_limit_price
            >= context.available_hedge_budget_usd
}

fn hedge_leg_status(result: Option<&HedgeResult>) -> &'static str {
    match result {
        Some(result)
            if result.verification_state
                == crate::trading::hedge_executor::HedgeVerificationState::Unknown =>
        {
            "unverified"
        }
        Some(result) if result.success => "success",
        Some(_) => "failed",
        None => "skipped",
    }
}

pub fn build_watchdog_verdict(
    run_id: &str,
    mode: &str,
    ws_verdict: &str,
    status_verdict: &str,
    escalation_level: &str,
    ws_reason: Option<&str>,
    status_reason: Option<&str>,
    book_ws_connected: bool,
    user_ws_connected: bool,
    enforcement_enabled: bool,
    kill_actions_suppressed: bool,
    book_ws_stats: BookWsStatsSnapshot,
) -> EventEnvelope {
    let payload = WatchdogVerdictPayload {
        ws_verdict: ws_verdict.to_string(),
        status_verdict: status_verdict.to_string(),
        escalation_level: escalation_level.to_string(),
        ws_reason: ws_reason.map(|s| s.to_string()),
        status_reason: status_reason.map(|s| s.to_string()),
        book_ws_connected,
        user_ws_connected,
        enforcement_enabled,
        kill_actions_suppressed,
        last_raw_book_ws_message_at: book_ws_stats.last_raw_message_at,
        last_parsed_book_event_at: book_ws_stats.last_parsed_event_at,
        last_book_parse_error_at: book_ws_stats.last_parse_error_at,
        book_ws_accepted_messages: book_ws_stats.accepted_messages,
        book_ws_ignored_messages: book_ws_stats.ignored_messages,
        book_ws_parse_errors: book_ws_stats.parse_errors,
        book_ws_snapshot_events: book_ws_stats.snapshot_events,
        book_ws_delta_events: book_ws_stats.delta_events,
    };
    EventEnvelope::new(
        EventType::WatchdogVerdict,
        Priority::Normal,
        run_id.to_string(),
        "watchdog".to_string(),
        mode.to_string(),
        serde_json::to_value(payload).unwrap_or_default(),
    )
}

pub fn build_watchdog_kill_triggered(
    run_id: &str,
    mode: &str,
    reason: &str,
    escalation_level: &str,
    time_in_critical_secs: u64,
) -> EventEnvelope {
    let payload = WatchdogKillTriggeredPayload {
        reason: reason.to_string(),
        escalation_level: escalation_level.to_string(),
        time_in_critical_secs,
    };
    EventEnvelope::new(
        EventType::WatchdogKillTriggered,
        Priority::Critical,
        run_id.to_string(),
        "watchdog".to_string(),
        mode.to_string(),
        serde_json::to_value(payload).unwrap_or_default(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{MarketStatus, QuoteLeg, QuoteStatus, RewardConfig, RewardViability, Side};
    use chrono::Utc;
    use rust_decimal_macros::dec;

    fn sample_market() -> CanonicalMarket {
        CanonicalMarket {
            condition_id: "cond-1".to_string(),
            market_slug: "market-slug".to_string(),
            question: "Will this compile?".to_string(),
            yes_token_id: "yes-token".to_string(),
            no_token_id: "no-token".to_string(),
            reward_config: RewardConfig {
                condition_id: "cond-1".to_string(),
                daily_reward_rates: vec![dec!(1)],
                daily_reward_total: dec!(25),
                min_size: dec!(5),
                max_spread: dec!(0.10),
            },
            neg_risk: true,
            tick_size: "0.01".to_string(),
            end_date: None,
            admitted_at: Utc::now(),
            status: MarketStatus::Admitted,
        }
    }

    fn sample_report() -> DecisionReport {
        DecisionReport {
            condition_id: "cond-1".to_string(),
            market_slug: "market-slug".to_string(),
            question: "Will this compile?".to_string(),
            daily_reward_total: dec!(25),
            score_proxy: Some(dec!(0.2)),
            max_spread: dec!(0.10),
            effective_quote_size: dec!(10),
            candidate_quotes: vec![
                QuoteCandidate {
                    condition_id: "cond-1".to_string(),
                    leg: QuoteLeg::YesBid,
                    price: dec!(0.45),
                    size: dec!(10),
                    status: QuoteStatus::Approved,
                    reason: None,
                },
                QuoteCandidate {
                    condition_id: "cond-1".to_string(),
                    leg: QuoteLeg::NoBid,
                    price: dec!(0.55),
                    size: dec!(10),
                    status: QuoteStatus::Rejected,
                    reason: Some("insufficient depth".to_string()),
                },
            ],
            reward_viability: Some(RewardViability {
                estimated_reward: dec!(2),
                estimated_hedge_cost: dec!(0.25),
                estimated_edge: dec!(1.75),
                score_share_approx: dec!(0.2),
                return_per_share: dec!(0.175),
            }),
            would_trade: true,
            reasons: vec!["reason-1".to_string()],
        }
    }

    fn sample_tracked_order(trace_id: &str) -> TrackedOrder {
        TrackedOrder {
            order_id: "order-1".to_string(),
            trace_id: trace_id.to_string(),
            condition_id: "cond-1".to_string(),
            created_at: Utc::now(),
            leg: QuoteLeg::YesBid,
            token_id: "yes-token".to_string(),
            opposite_token_id: "no-token".to_string(),
            side: Side::Buy,
            price: dec!(0.45),
            size: dec!(10),
            matched_size: dec!(0),
            neg_risk: true,
            tick_size: "0.01".to_string(),
        }
    }

    fn sample_hedge_intent(side: Side) -> HedgeIntent {
        HedgeIntent {
            condition_id: "cond-1".to_string(),
            trigger_order_id: "order-1".to_string(),
            trigger_leg: QuoteLeg::YesBid,
            fill_size: dec!(10),
            fill_price: dec!(0.74),
            hedge_token_id: "no-token".to_string(),
            hedge_side: side,
            neg_risk: true,
            tick_size: "0.01".to_string(),
        }
    }

    fn sample_resolution() -> HedgeResolution {
        HedgeResolution {
            hedge_shares: dec!(6),
            hedge_limit_price: dec!(0.27),
            sellback_shares: dec!(4),
            sellback_limit_price: dec!(0.73),
            unresolved_shares: Decimal::ZERO,
        }
    }

    fn sample_position(yes_size: Decimal, no_size: Decimal) -> Position {
        Position {
            condition_id: "cond-1".to_string(),
            yes_size,
            no_size,
            avg_yes_price: dec!(0.74),
            avg_no_price: dec!(0.26),
        }
    }

    #[test]
    fn decision_event_contains_expected_market_fields() {
        let market = sample_market();
        let report = sample_report();

        let event = build_decision_evaluated(
            "run-1",
            "cycle-1",
            "dry-run",
            "live_engine",
            &market,
            &report,
            dec!(50),
            None,
            None,
            None,
            DecisionRankingContext {
                rank_in_cycle: Some(1),
                ranked_market_count: Some(3),
                ranking_metric_name: Some("reward_per_share"),
                ranking_metric_value: Some(dec!(0.2)),
                ..Default::default()
            },
        );

        assert_eq!(event.event_type, EventType::DecisionEvaluated);
        assert_eq!(event.cycle_id.as_deref(), Some("cycle-1"));
        assert_eq!(event.condition_id.as_deref(), Some("cond-1"));
        assert_eq!(event.market_slug.as_deref(), Some("market-slug"));
        assert_eq!(event.question.as_deref(), Some("Will this compile?"));
        assert_eq!(event.payload["would_trade"], serde_json::json!(true));
        assert_eq!(event.payload["rank_in_cycle"], serde_json::json!(1));
        assert_eq!(
            event.payload["ranking_metric_name"],
            serde_json::json!("reward_per_share")
        );
        assert_eq!(
            event.payload["frontier_counterfactual_budget_usd"],
            serde_json::Value::Null
        );
    }

    #[test]
    fn decision_event_includes_frontier_counterfactual_fields_for_selected_pair() {
        let market = sample_market();
        let report = sample_report();

        let event = build_decision_evaluated(
            "run-1",
            "cycle-1",
            "dry-run",
            "live_engine",
            &market,
            &report,
            dec!(50),
            None,
            None,
            None,
            DecisionRankingContext {
                frontier_eligible: Some(true),
                frontier_requires_reallocation: Some(true),
                frontier_replaces_condition_id: Some("cond-loser"),
                frontier_counterfactual_budget_usd: Some(dec!(75)),
                frontier_counterfactual_reclaimable_bid_capital_usd: Some(dec!(25)),
                frontier_counterfactual_entrant_condition_id: Some("cond-1"),
                frontier_counterfactual_entrant_ranking_metric_name: Some("reward_per_share"),
                frontier_counterfactual_entrant_ranking_metric_value: Some(dec!(0.30)),
                frontier_counterfactual_entrant_expected_reward_usd_day: Some(dec!(9.50)),
                frontier_counterfactual_loser_condition_id: Some("cond-loser"),
                frontier_counterfactual_loser_ranking_metric_name: Some("reward_per_share"),
                frontier_counterfactual_loser_ranking_metric_value: Some(dec!(0.12)),
                frontier_counterfactual_loser_expected_reward_usd_day: Some(dec!(3.25)),
                ..Default::default()
            },
        );

        assert_eq!(
            event.payload["frontier_counterfactual_budget_usd"],
            serde_json::json!("75")
        );
        assert_eq!(
            event.payload["frontier_counterfactual_entrant_condition_id"],
            serde_json::json!("cond-1")
        );
        assert_eq!(
            event.payload["frontier_counterfactual_entrant_ranking_metric_value"],
            serde_json::json!("0.30")
        );
        assert_eq!(
            event.payload["frontier_counterfactual_loser_expected_reward_usd_day"],
            serde_json::json!("3.25")
        );
    }

    #[test]
    fn trace_id_propagates_across_quote_and_order_events() {
        let market = sample_market();
        let candidate = sample_report().candidate_quotes[0].clone();
        let trace_id = "trace-123";

        let quote_event = build_quote_approved(
            "run-1",
            "cycle-1",
            trace_id,
            "live",
            "live_engine",
            &market,
            &candidate,
        );
        let order_event = build_order_submitted(
            "run-1",
            trace_id,
            "live",
            &sample_tracked_order(trace_id),
            Some("new_quote"),
            Some("bid_entry"),
        );

        assert_eq!(quote_event.trace_id.as_deref(), Some(trace_id));
        assert_eq!(order_event.trace_id.as_deref(), Some(trace_id));
    }

    #[test]
    fn cancellation_event_uses_reason_code() {
        let event = build_order_cancelled(
            "run-1",
            "trace-1",
            "live",
            &sample_tracked_order("trace-1"),
            CancelReasonCode::FrontierRebalance,
            Some("frontier_rebalance"),
            None,
        );

        assert_eq!(event.event_type, EventType::OrderCancelled);
        assert_eq!(
            event.payload["reason_code"],
            serde_json::json!(CancelReasonCode::FrontierRebalance)
        );
    }

    #[test]
    fn cancellation_event_carries_optional_diagnostics() {
        let diagnostics = OrderEventDiagnostics {
            quote_refresh: Some(spreadeater_core::payloads::QuoteRefreshDiagnostics {
                would_trade: false,
                reasons: vec!["insufficient edge".to_string()],
                effective_quote_size: dec!(0),
                available_budget_usd: dec!(12.5),
            }),
            hedge_depth: None,
        };
        let event = build_order_cancelled(
            "run-1",
            "trace-1",
            "live",
            &sample_tracked_order("trace-1"),
            CancelReasonCode::MarketDeadmitted,
            Some("quote_refresh_non_viable"),
            Some(&diagnostics),
        );

        assert_eq!(
            event.payload["diagnostics"]["quote_refresh"]["would_trade"],
            serde_json::json!(false)
        );
        assert_eq!(
            event.payload["diagnostics"]["quote_refresh"]["available_budget_usd"],
            serde_json::json!("12.5")
        );
    }

    #[test]
    fn hedge_decision_event_records_buy_side_split_and_reason_code() {
        let intent = sample_hedge_intent(Side::Buy);
        let resolution = sample_resolution();

        let event = build_hedge_decision(
            "run-1",
            "trace-1",
            "hedge-1",
            "live",
            "fill_handler",
            &intent,
            HedgeDecisionContext {
                resolution: Some(&resolution),
                available_hedge_budget_usd: dec!(100),
                filled_best_bid_price: Some(dec!(0.73)),
                filled_best_bid_size: Some(dec!(10)),
                opposite_best_ask_price: Some(dec!(0.26)),
                opposite_best_ask_size: Some(dec!(10)),
            },
        );

        assert_eq!(event.event_type, EventType::HedgeDecisionEvaluated);
        assert_eq!(
            event.payload["decision_mode"],
            serde_json::json!("buy_side_resolution")
        );
        assert_eq!(
            event.payload["decision_reason_code"],
            serde_json::json!("hedge_cheaper")
        );
        assert_eq!(
            event.payload["planned_hedge_shares"],
            serde_json::json!("6")
        );
        assert_eq!(
            event.payload["planned_sellback_shares"],
            serde_json::json!("4")
        );
    }

    #[test]
    fn hedge_decision_event_records_sell_side_direct_path() {
        let intent = sample_hedge_intent(Side::Sell);

        let event = build_hedge_decision(
            "run-1",
            "trace-1",
            "hedge-1",
            "live",
            "fill_handler",
            &intent,
            HedgeDecisionContext {
                available_hedge_budget_usd: dec!(100),
                ..Default::default()
            },
        );

        assert_eq!(
            event.payload["decision_mode"],
            serde_json::json!("sell_side_direct")
        );
        assert_eq!(
            event.payload["decision_reason_code"],
            serde_json::json!("sell_side_direct")
        );
        assert_eq!(
            event.payload["planned_hedge_shares"],
            serde_json::json!("10")
        );
        assert_eq!(
            event.payload["planned_hedge_price"],
            serde_json::json!("0.01")
        );
    }

    #[test]
    fn hedge_decision_event_records_tie_prefers_sellback_reason_code() {
        let intent = sample_hedge_intent(Side::Buy);
        let resolution = HedgeResolution {
            hedge_shares: Decimal::ZERO,
            hedge_limit_price: Decimal::ZERO,
            sellback_shares: dec!(10),
            sellback_limit_price: dec!(0.73),
            unresolved_shares: Decimal::ZERO,
        };

        let event = build_hedge_decision(
            "run-1",
            "trace-1",
            "hedge-1",
            "live",
            "fill_handler",
            &intent,
            HedgeDecisionContext {
                resolution: Some(&resolution),
                available_hedge_budget_usd: dec!(100),
                filled_best_bid_price: Some(dec!(0.73)),
                filled_best_bid_size: Some(dec!(10)),
                opposite_best_ask_price: Some(dec!(0.27)),
                opposite_best_ask_size: Some(dec!(10)),
            },
        );

        assert_eq!(
            event.payload["decision_reason_code"],
            serde_json::json!("tie_prefers_sellback")
        );
        assert_eq!(
            event.payload["planned_sellback_shares"],
            serde_json::json!("10")
        );
    }

    #[test]
    fn hedge_result_event_carries_verification_metadata() {
        let intent = sample_hedge_intent(Side::Buy);
        let hedge_result = HedgeResult {
            intent: intent.clone(),
            success: true,
            order_result: Some(crate::models::OrderResult {
                order_id: "hedge-order".to_string(),
                status: crate::models::OrderStatus::Matched,
                trade_ids: vec!["trade-1".to_string()],
            }),
            hedge_price: Some(dec!(0.27)),
            failure_reason: None,
            verification_state: crate::trading::hedge_executor::HedgeVerificationState::Unknown,
            verification_metadata: crate::trading::hedge_executor::HedgeVerificationMetadata {
                cancel_status: Some("confirmed".to_string()),
                cancel_reason: None,
                lookup_status: Some("missing".to_string()),
                lookup_matched_shares: None,
                lookup_error: None,
                trade_ids: vec!["trade-1".to_string()],
            },
        };

        let event = build_hedge_result(
            "run-1",
            "trace-1",
            "hedge-1",
            "live",
            "fill_handler",
            &intent,
            HedgeResultContext {
                hedge_result: Some(&hedge_result),
                aggregate_success: true,
                aggregate_failure_reason: None,
                sellback_order_id: None,
                sellback_price: None,
                sellback_execution_limit_price: None,
                sellback_leg_status: Some("skipped"),
                sellback_response_status: None,
                sellback_lookup_status: None,
                sellback_lookup_matched_shares: None,
                sellback_lookup_error: None,
                sellback_trade_ids: None,
                post_sync_net_exposure: Some(Decimal::ZERO),
                post_sync_yes_size: Some(dec!(5)),
                post_sync_no_size: Some(dec!(5)),
                post_sync_source: Some("position_manager"),
                halt_signal_suppressed: false,
            },
            180,
            Some("fill_handler"),
        );

        assert_eq!(
            event.payload["hedge_cancel_status"],
            serde_json::json!("confirmed")
        );
        assert_eq!(
            event.payload["hedge_lookup_status"],
            serde_json::json!("missing")
        );
        assert_eq!(
            event.payload["hedge_trade_ids"],
            serde_json::json!(["trade-1"])
        );
        assert_eq!(
            event.payload["sellback_response_status"],
            serde_json::Value::Null
        );
    }

    #[test]
    fn hedge_exit_path_event_records_merge_success() {
        let intent = sample_hedge_intent(Side::Buy);
        let position = sample_position(dec!(10), dec!(10));

        let event = build_hedge_exit_path(
            "run-1",
            "trace-1",
            "hedge-1",
            "live",
            "fill_handler",
            &intent,
            HedgeExitPathContext {
                post_position: &position,
                post_sync_source: "retry_sync",
                exit_path_status: "merge_succeeded",
                merge_eligible_pairs: dec!(10),
                ctf_merge_configured: true,
                merge_attempted: true,
                merge_tx_hash: Some("0xmerge"),
                merge_failure_reason: None,
                fallback_asks_attempted: false,
                fallback_ask_count: 0,
                fallback_failure_reason: None,
            },
        );

        assert_eq!(event.event_type, EventType::HedgeExitPathRecorded);
        assert_eq!(
            event.payload["exit_path_status"],
            serde_json::json!("merge_succeeded")
        );
        assert_eq!(event.payload["merge_attempted"], serde_json::json!(true));
        assert_eq!(event.payload["merge_tx_hash"], serde_json::json!("0xmerge"));
    }

    #[test]
    fn hedge_exit_path_event_records_merge_failure() {
        let intent = sample_hedge_intent(Side::Buy);
        let position = sample_position(dec!(10), dec!(10));

        let event = build_hedge_exit_path(
            "run-1",
            "trace-1",
            "hedge-1",
            "live",
            "fill_handler",
            &intent,
            HedgeExitPathContext {
                post_position: &position,
                post_sync_source: "retry_sync",
                exit_path_status: "merge_failed",
                merge_eligible_pairs: dec!(10),
                ctf_merge_configured: true,
                merge_attempted: true,
                merge_tx_hash: None,
                merge_failure_reason: Some("reverted"),
                fallback_asks_attempted: false,
                fallback_ask_count: 0,
                fallback_failure_reason: None,
            },
        );

        assert_eq!(
            event.payload["merge_failure_reason"],
            serde_json::json!("reverted")
        );
    }

    #[test]
    fn hedge_exit_path_event_records_fallback_asks_placed() {
        let intent = sample_hedge_intent(Side::Buy);
        let position = sample_position(dec!(10), dec!(10));

        let event = build_hedge_exit_path(
            "run-1",
            "trace-1",
            "hedge-1",
            "live",
            "fill_handler",
            &intent,
            HedgeExitPathContext {
                post_position: &position,
                post_sync_source: "retry_sync",
                exit_path_status: "fallback_asks_placed",
                merge_eligible_pairs: dec!(10),
                ctf_merge_configured: false,
                merge_attempted: false,
                merge_tx_hash: None,
                merge_failure_reason: None,
                fallback_asks_attempted: true,
                fallback_ask_count: 2,
                fallback_failure_reason: None,
            },
        );

        assert_eq!(event.payload["fallback_ask_count"], serde_json::json!(2));
    }

    #[test]
    fn hedge_exit_path_event_records_fallback_asks_failed() {
        let intent = sample_hedge_intent(Side::Buy);
        let position = sample_position(dec!(10), dec!(10));

        let event = build_hedge_exit_path(
            "run-1",
            "trace-1",
            "hedge-1",
            "live",
            "fill_handler",
            &intent,
            HedgeExitPathContext {
                post_position: &position,
                post_sync_source: "retry_sync",
                exit_path_status: "fallback_asks_failed",
                merge_eligible_pairs: dec!(10),
                ctf_merge_configured: false,
                merge_attempted: false,
                merge_tx_hash: None,
                merge_failure_reason: None,
                fallback_asks_attempted: true,
                fallback_ask_count: 0,
                fallback_failure_reason: Some("book unavailable"),
            },
        );

        assert_eq!(
            event.payload["fallback_failure_reason"],
            serde_json::json!("book unavailable")
        );
    }

    #[test]
    fn hedge_exit_path_event_records_pair_left_idle() {
        let intent = sample_hedge_intent(Side::Buy);
        let position = sample_position(dec!(10), dec!(10));

        let event = build_hedge_exit_path(
            "run-1",
            "trace-1",
            "hedge-1",
            "live",
            "fill_handler",
            &intent,
            HedgeExitPathContext {
                post_position: &position,
                post_sync_source: "retry_sync",
                exit_path_status: "pair_left_idle",
                merge_eligible_pairs: dec!(10),
                ctf_merge_configured: false,
                merge_attempted: false,
                merge_tx_hash: None,
                merge_failure_reason: None,
                fallback_asks_attempted: false,
                fallback_ask_count: 0,
                fallback_failure_reason: None,
            },
        );

        assert_eq!(
            event.payload["exit_path_status"],
            serde_json::json!("pair_left_idle")
        );
    }

    #[test]
    fn hedge_exit_path_event_records_directional_residual() {
        let intent = sample_hedge_intent(Side::Buy);
        let position = sample_position(dec!(10), Decimal::ZERO);

        let event = build_hedge_exit_path(
            "run-1",
            "trace-1",
            "hedge-1",
            "live",
            "fill_handler",
            &intent,
            HedgeExitPathContext {
                post_position: &position,
                post_sync_source: "retry_sync",
                exit_path_status: "directional_residual",
                merge_eligible_pairs: Decimal::ZERO,
                ctf_merge_configured: false,
                merge_attempted: false,
                merge_tx_hash: None,
                merge_failure_reason: None,
                fallback_asks_attempted: false,
                fallback_ask_count: 0,
                fallback_failure_reason: None,
            },
        );

        assert_eq!(
            event.payload["post_sync_net_exposure"],
            serde_json::json!("10")
        );
    }

    #[test]
    fn watchdog_verdict_includes_observe_only_telemetry() {
        let now = chrono::Utc::now();
        let event = build_watchdog_verdict(
            "run-1",
            "live",
            "critical",
            "healthy",
            "kill_pending",
            Some("Book WS silent"),
            None,
            true,
            true,
            false,
            true,
            BookWsStatsSnapshot {
                accepted_messages: 10,
                ignored_messages: 3,
                parse_errors: 2,
                snapshot_events: 7,
                delta_events: 9,
                last_raw_message_at: Some(now),
                last_parsed_event_at: Some(now),
                last_parse_error_at: Some(now),
            },
        );

        assert_eq!(
            event.payload["enforcement_enabled"],
            serde_json::json!(false)
        );
        assert_eq!(
            event.payload["kill_actions_suppressed"],
            serde_json::json!(true)
        );
        assert_eq!(
            event.payload["book_ws_accepted_messages"],
            serde_json::json!(10)
        );
        assert!(event.payload["last_raw_book_ws_message_at"].is_string());
    }
}
