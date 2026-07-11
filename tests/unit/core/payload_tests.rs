use rust_decimal_macros::dec;
use spreadeater_core::payloads::{
    CalibrationAdjustedPayload, DecisionEventPayload, FillDetectedPayload, HedgeDecisionPayload,
    HedgeExitPathPayload, HedgeIntentPayload, HedgeResultPayload, MonitorDegradedPayload,
    NeutralityPayload, OrderCancelledPayload, OrderResizedPayload, OrderSubmittedPayload,
    QuoteLegSummary, RiskStateChangedPayload, StatusSnapshotPayload,
    UserStreamStatusChangedPayload,
};
use spreadeater_core::CancelReasonCode;

#[test]
fn decision_payload_serde_roundtrip() {
    let payload = DecisionEventPayload {
        candidate_quotes: vec![
            QuoteLegSummary {
                leg: "YesBid".to_string(),
                price: dec!(0.45),
                size: dec!(50),
                status: "Approved".to_string(),
                reason: None,
            },
            QuoteLegSummary {
                leg: "NoBid".to_string(),
                price: dec!(0.55),
                size: dec!(50),
                status: "Rejected".to_string(),
                reason: Some("insufficient depth".to_string()),
            },
        ],
        reasons: vec!["spread too wide".to_string()],
        effective_quote_size: dec!(50),
        expected_reward_usd_day: Some(dec!(1.25)),
        expected_hedge_cost_usd: Some(dec!(0.10)),
        expected_edge_usd: Some(dec!(1.15)),
        expected_edge_pct: Some(dec!(2.3)),
        committed_capital_usd: Some(dec!(25.0)),
        score_share: Some(dec!(0.05)),
        max_hedgeable_size: Some(dec!(100)),
        competition_multiplier_used: Some(dec!(1.25)),
        api_balance_usd: Some(dec!(250)),
        available_budget_usd: Some(dec!(225)),
        rank_in_cycle: Some(1),
        ranked_market_count: Some(10),
        ranking_metric_name: Some("reward_per_share".to_string()),
        ranking_metric_value: Some(dec!(0.025)),
        frontier_eligible: Some(true),
        frontier_requires_reallocation: Some(true),
        frontier_replaces_condition_id: Some("market-loser".to_string()),
        frontier_replaced_by_condition_id: None,
        frontier_counterfactual_budget_usd: Some(dec!(75.0)),
        frontier_counterfactual_reclaimable_bid_capital_usd: Some(dec!(25.0)),
        frontier_counterfactual_entrant_condition_id: Some("market-winner".to_string()),
        frontier_counterfactual_entrant_ranking_metric_name: Some("reward_per_share".to_string()),
        frontier_counterfactual_entrant_ranking_metric_value: Some(dec!(0.075)),
        frontier_counterfactual_entrant_expected_reward_usd_day: Some(dec!(3.75)),
        frontier_counterfactual_loser_condition_id: Some("market-loser".to_string()),
        frontier_counterfactual_loser_ranking_metric_name: Some("reward_per_share".to_string()),
        frontier_counterfactual_loser_ranking_metric_value: Some(dec!(0.025)),
        frontier_counterfactual_loser_expected_reward_usd_day: Some(dec!(1.25)),
        would_trade: true,
    };

    let json = serde_json::to_string(&payload).unwrap();
    let back: DecisionEventPayload = serde_json::from_str(&json).unwrap();
    assert_eq!(back.candidate_quotes.len(), 2);
    assert_eq!(back.effective_quote_size, dec!(50));
    assert!(back.would_trade);
    assert_eq!(back.expected_edge_usd, Some(dec!(1.15)));
    assert_eq!(back.rank_in_cycle, Some(1));
    assert_eq!(
        back.ranking_metric_name.as_deref(),
        Some("reward_per_share")
    );
    assert_eq!(back.frontier_eligible, Some(true));
    assert_eq!(
        back.frontier_replaces_condition_id.as_deref(),
        Some("market-loser")
    );
    assert_eq!(back.frontier_counterfactual_budget_usd, Some(dec!(75.0)));
    assert_eq!(
        back.frontier_counterfactual_entrant_condition_id.as_deref(),
        Some("market-winner")
    );
    assert_eq!(
        back.frontier_counterfactual_loser_ranking_metric_value,
        Some(dec!(0.025))
    );
}

#[test]
fn decision_payload_deserializes_without_frontier_counterfactual_fields() {
    let payload = serde_json::json!({
        "candidate_quotes": [],
        "reasons": [],
        "effective_quote_size": "0",
        "expected_reward_usd_day": null,
        "expected_hedge_cost_usd": null,
        "expected_edge_usd": null,
        "expected_edge_pct": null,
        "committed_capital_usd": null,
        "score_share": null,
        "max_hedgeable_size": null,
        "competition_multiplier_used": null,
        "api_balance_usd": null,
        "available_budget_usd": null,
        "rank_in_cycle": 1,
        "ranked_market_count": 2,
        "ranking_metric_name": "reward_per_share",
        "ranking_metric_value": "0.01",
        "frontier_eligible": true,
        "frontier_requires_reallocation": false,
        "frontier_replaces_condition_id": "market-a",
        "frontier_replaced_by_condition_id": null,
        "would_trade": false
    });

    let back: DecisionEventPayload = serde_json::from_value(payload).unwrap();
    assert_eq!(back.rank_in_cycle, Some(1));
    assert_eq!(back.frontier_counterfactual_budget_usd, None);
    assert_eq!(back.frontier_counterfactual_entrant_condition_id, None);
    assert_eq!(back.frontier_counterfactual_loser_condition_id, None);
}

#[test]
fn order_submitted_payload_serde_roundtrip() {
    let payload = OrderSubmittedPayload {
        leg: "YesBid".to_string(),
        side: "BUY".to_string(),
        price: dec!(0.45),
        size: dec!(50),
        matched_size: dec!(0),
        token_id: "token_abc".to_string(),
        neg_risk: true,
        origin: Some("new_quote".to_string()),
        role: Some("bid_entry".to_string()),
    };
    let json = serde_json::to_string(&payload).unwrap();
    let back: OrderSubmittedPayload = serde_json::from_str(&json).unwrap();
    assert_eq!(back.price, dec!(0.45));
    assert!(back.neg_risk);
}

#[test]
fn order_cancelled_payload_serde_roundtrip() {
    let payload = OrderCancelledPayload {
        reason_code: CancelReasonCode::QuoteDrift,
        reason_text: "Price drifted beyond threshold".to_string(),
        old_size: dec!(50),
        capital_delta: Some(dec!(22.5)),
        origin: Some("quote_refresh".to_string()),
        diagnostics: None,
    };
    let json = serde_json::to_string(&payload).unwrap();
    let back: OrderCancelledPayload = serde_json::from_str(&json).unwrap();
    assert_eq!(back.reason_code, CancelReasonCode::QuoteDrift);
    assert_eq!(back.old_size, dec!(50));
}

#[test]
fn order_resized_payload_serde_roundtrip() {
    let payload = OrderResizedPayload {
        old_order_id: "old_123".to_string(),
        new_order_id: "new_456".to_string(),
        old_size: dec!(50),
        new_size: dec!(30),
        old_price: dec!(0.45),
        new_price: dec!(0.46),
        reason_code: CancelReasonCode::HedgeDepthPartialDownsize,
        origin: Some("replacement".to_string()),
        diagnostics: None,
    };
    let json = serde_json::to_string(&payload).unwrap();
    let back: OrderResizedPayload = serde_json::from_str(&json).unwrap();
    assert_eq!(back.old_order_id, "old_123");
    assert_eq!(back.new_size, dec!(30));
}

#[test]
fn fill_detected_payload_serde_roundtrip() {
    let payload = FillDetectedPayload {
        trade_id: "trade_789".to_string(),
        fill_price: dec!(0.55),
        fill_size: dec!(10),
        side: "BUY".to_string(),
        outcome: "Yes".to_string(),
        match_source: Some("maker_order_id".to_string()),
        fallback_match: false,
        anchored_order_id: Some("order_abc".to_string()),
        deferred_to_reconciliation: false,
    };
    let json = serde_json::to_string(&payload).unwrap();
    let back: FillDetectedPayload = serde_json::from_str(&json).unwrap();
    assert_eq!(back.trade_id, "trade_789");
    assert!(!back.fallback_match);
    assert_eq!(back.anchored_order_id.as_deref(), Some("order_abc"));
}

#[test]
fn hedge_intent_payload_serde_roundtrip() {
    let payload = HedgeIntentPayload {
        trigger_order_id: "order_abc".to_string(),
        trigger_leg: "YesBid".to_string(),
        fill_size: dec!(10),
        fill_price: dec!(0.55),
        hedge_token_id: "token_def".to_string(),
        hedge_side: "BUY".to_string(),
        planned_hedge_shares: Some(dec!(8)),
        planned_hedge_price: Some(dec!(0.46)),
        planned_sellback_shares: Some(dec!(2)),
        planned_sellback_price: Some(dec!(0.54)),
        planned_sellback_reference_bid: Some(dec!(0.54)),
        unresolved_shares: Some(dec!(0)),
        pre_resolution_active_orders: Some(3),
        pre_resolution_pending_cancels: Some(1),
        cancel_wait_drained: Some(true),
        origin: Some("fill_handler".to_string()),
    };
    let json = serde_json::to_string(&payload).unwrap();
    let back: HedgeIntentPayload = serde_json::from_str(&json).unwrap();
    assert_eq!(back.trigger_order_id, "order_abc");
    assert_eq!(back.planned_sellback_shares, Some(dec!(2)));
    assert_eq!(back.planned_sellback_reference_bid, Some(dec!(0.54)));
}

#[test]
fn hedge_decision_payload_serde_roundtrip() {
    let payload = HedgeDecisionPayload {
        trigger_leg: "YesBid".to_string(),
        hedge_side: "BUY".to_string(),
        fill_size: dec!(10),
        fill_price: dec!(0.55),
        decision_mode: "buy_side_resolution".to_string(),
        decision_reason_code: "hedge_cheaper".to_string(),
        available_hedge_budget_usd: dec!(25),
        filled_best_bid_price: Some(dec!(0.54)),
        filled_best_bid_size: Some(dec!(12)),
        opposite_best_ask_price: Some(dec!(0.44)),
        opposite_best_ask_size: Some(dec!(10)),
        planned_hedge_shares: dec!(8),
        planned_hedge_price: dec!(0.45),
        planned_sellback_shares: dec!(2),
        planned_sellback_price: dec!(0.54),
        unresolved_shares: dec!(0),
    };
    let json = serde_json::to_string(&payload).unwrap();
    let back: HedgeDecisionPayload = serde_json::from_str(&json).unwrap();
    assert_eq!(back.decision_mode, "buy_side_resolution");
    assert_eq!(back.decision_reason_code, "hedge_cheaper");
    assert_eq!(back.planned_sellback_shares, dec!(2));
}

#[test]
fn hedge_result_payload_serde_roundtrip() {
    let payload = HedgeResultPayload {
        hedge_order_id: Some("hedge_order_1".to_string()),
        result_status: "filled".to_string(),
        hedge_price: Some(dec!(0.46)),
        hedge_leg_status: Some("success".to_string()),
        hedge_cancel_status: Some("confirmed".to_string()),
        hedge_cancel_reason: None,
        hedge_lookup_status: Some("matched".to_string()),
        hedge_lookup_matched_shares: Some(dec!(10)),
        hedge_lookup_error: None,
        hedge_trade_ids: Some(vec!["trade-1".to_string()]),
        sellback_order_id: Some("sellback_order_1".to_string()),
        sellback_price: Some(dec!(0.54)),
        sellback_execution_limit_price: Some(dec!(0.01)),
        sellback_leg_status: Some("success".to_string()),
        sellback_response_status: Some("matched".to_string()),
        sellback_lookup_status: None,
        sellback_lookup_matched_shares: None,
        sellback_lookup_error: None,
        sellback_trade_ids: Some(vec!["sellback-trade-1".to_string()]),
        post_sync_net_exposure: Some(dec!(0)),
        post_sync_yes_size: Some(dec!(10)),
        post_sync_no_size: Some(dec!(10)),
        post_sync_source: Some("retry_sync".to_string()),
        halt_signal_suppressed: true,
        failure_reason: None,
        latency_ms: 150,
        origin: Some("fill_handler".to_string()),
    };
    let json = serde_json::to_string(&payload).unwrap();
    let back: HedgeResultPayload = serde_json::from_str(&json).unwrap();
    assert_eq!(back.latency_ms, 150);
    assert!(back.failure_reason.is_none());
    assert_eq!(back.post_sync_net_exposure, Some(dec!(0)));
    assert_eq!(back.sellback_execution_limit_price, Some(dec!(0.01)));
    assert_eq!(back.sellback_response_status.as_deref(), Some("matched"));
    assert_eq!(
        back.sellback_trade_ids,
        Some(vec!["sellback-trade-1".to_string()])
    );
    assert_eq!(back.post_sync_source.as_deref(), Some("retry_sync"));
    assert!(back.halt_signal_suppressed);
    assert_eq!(back.hedge_cancel_status.as_deref(), Some("confirmed"));
    assert_eq!(back.hedge_lookup_status.as_deref(), Some("matched"));
    assert_eq!(back.hedge_trade_ids, Some(vec!["trade-1".to_string()]));
}

#[test]
fn hedge_exit_path_payload_serde_roundtrip() {
    let payload = HedgeExitPathPayload {
        post_sync_yes_size: dec!(10),
        post_sync_no_size: dec!(10),
        post_sync_net_exposure: dec!(0),
        post_sync_complete_sets: dec!(10),
        post_sync_source: "retry_sync".to_string(),
        exit_path_status: "fallback_asks_placed".to_string(),
        merge_eligible_pairs: dec!(10),
        ctf_merge_configured: true,
        merge_attempted: true,
        merge_tx_hash: Some("0xmerge".to_string()),
        merge_failure_reason: None,
        fallback_asks_attempted: true,
        fallback_ask_count: 2,
        fallback_failure_reason: None,
    };
    let json = serde_json::to_string(&payload).unwrap();
    let back: HedgeExitPathPayload = serde_json::from_str(&json).unwrap();
    assert_eq!(back.exit_path_status, "fallback_asks_placed");
    assert_eq!(back.fallback_ask_count, 2);
    assert_eq!(back.merge_tx_hash.as_deref(), Some("0xmerge"));
}

#[test]
fn neutrality_payload_serde_roundtrip() {
    let payload = NeutralityPayload {
        pre_yes_size: dec!(10),
        pre_no_size: dec!(0),
        post_yes_size: dec!(10),
        post_no_size: dec!(10),
        residual_exposure: dec!(0),
        complete_sets: dec!(10),
        tolerance: dec!(1),
        is_neutral: true,
    };
    let json = serde_json::to_string(&payload).unwrap();
    let back: NeutralityPayload = serde_json::from_str(&json).unwrap();
    assert!(back.is_neutral);
    assert_eq!(back.complete_sets, dec!(10));
}

#[test]
fn monitor_degraded_payload_serde_roundtrip() {
    let payload = MonitorDegradedPayload {
        component: "jsonl_writer".to_string(),
        degraded_reason: "disk full".to_string(),
        queue_depth: Some(500),
        index_lag_ms: Some(2000),
    };
    let json = serde_json::to_string(&payload).unwrap();
    let back: MonitorDegradedPayload = serde_json::from_str(&json).unwrap();
    assert_eq!(back.component, "jsonl_writer");
    assert_eq!(back.queue_depth, Some(500));
}

#[test]
fn payload_as_serde_json_value_roundtrip() {
    let payload = OrderSubmittedPayload {
        leg: "NoBid".to_string(),
        side: "SELL".to_string(),
        price: dec!(0.60),
        size: dec!(25),
        matched_size: dec!(5),
        token_id: "tok_1".to_string(),
        neg_risk: false,
        origin: Some("exchange_sync".to_string()),
        role: Some("ask_inventory".to_string()),
    };

    // Serialize to serde_json::Value (how it lives in EventEnvelope.payload)
    let value = serde_json::to_value(&payload).unwrap();
    assert!(value.is_object());

    // Deserialize back from Value
    let back: OrderSubmittedPayload = serde_json::from_value(value).unwrap();
    assert_eq!(back.leg, "NoBid");
    assert_eq!(back.size, dec!(25));
}

#[test]
fn risk_state_payload_serde_roundtrip() {
    let payload = RiskStateChangedPayload {
        scope: "market".to_string(),
        status: "halted".to_string(),
        reason: Some("hedge timeout".to_string()),
        total_exposure: Some(dec!(42)),
        global_halt: Some(false),
    };

    let json = serde_json::to_string(&payload).unwrap();
    let back: RiskStateChangedPayload = serde_json::from_str(&json).unwrap();
    assert_eq!(back.status, "halted");
    assert_eq!(back.total_exposure, Some(dec!(42)));
}

#[test]
fn user_stream_status_payload_serde_roundtrip() {
    let payload = UserStreamStatusChangedPayload {
        status: "connected".to_string(),
        subscribed_markets: Some(4),
        detail: Some("subscribed to managed markets".to_string()),
    };

    let json = serde_json::to_string(&payload).unwrap();
    let back: UserStreamStatusChangedPayload = serde_json::from_str(&json).unwrap();
    assert_eq!(back.subscribed_markets, Some(4));
}

#[test]
fn status_snapshot_payload_serde_roundtrip() {
    let payload = StatusSnapshotPayload {
        managed_markets: 7,
        order_committed_usd: dec!(125.50),
        position_committed_usd: dec!(30),
        total_committed_usd: dec!(155.50),
        api_balance_usd: dec!(220),
        available_budget_usd: dec!(344.50),
        competition_multiplier: dec!(1.2),
        total_est_daily_usd: Some(dec!(12.34)),
        book_ws_accepted_messages: Some(8),
        book_ws_ignored_messages: Some(1),
        book_ws_parse_errors: Some(0),
        book_ws_snapshot_events: Some(2),
        book_ws_delta_events: Some(6),
    };

    let json = serde_json::to_string(&payload).unwrap();
    let back: StatusSnapshotPayload = serde_json::from_str(&json).unwrap();
    assert_eq!(back.total_committed_usd, dec!(155.50));
    assert_eq!(back.competition_multiplier, dec!(1.2));
    assert_eq!(back.total_est_daily_usd, Some(dec!(12.34)));
    assert_eq!(back.book_ws_accepted_messages, Some(8));
    assert_eq!(back.book_ws_delta_events, Some(6));
}

#[test]
fn calibration_adjusted_payload_serde_roundtrip() {
    let payload = CalibrationAdjustedPayload {
        old_multiplier: dec!(1.0),
        new_multiplier: dec!(1.2),
        sample_count: 50,
        false_positives: 4,
        false_negatives: 1,
    };

    let json = serde_json::to_string(&payload).unwrap();
    let back: CalibrationAdjustedPayload = serde_json::from_str(&json).unwrap();
    assert_eq!(back.new_multiplier, dec!(1.2));
    assert_eq!(back.false_positives, 4);
}
