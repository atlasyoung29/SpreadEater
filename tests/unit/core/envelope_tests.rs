use spreadeater_core::{EventEnvelope, EventType, Priority, SchemaVersion};

#[test]
fn schema_version_display() {
    assert_eq!(SchemaVersion::V1_0.to_string(), "1.0");
    assert_eq!(SchemaVersion::V1_1.to_string(), "1.1");
    assert_eq!(SchemaVersion::V1_2.to_string(), "1.2");
    assert_eq!(SchemaVersion::V1_3.to_string(), "1.3");
    assert_eq!(SchemaVersion::V1_4.to_string(), "1.4");
    assert_eq!(SchemaVersion::V1_5.to_string(), "1.5");
}

#[test]
fn schema_version_serde_roundtrip() {
    for version in [
        SchemaVersion::V1_0,
        SchemaVersion::V1_1,
        SchemaVersion::V1_2,
        SchemaVersion::V1_3,
        SchemaVersion::V1_4,
        SchemaVersion::V1_5,
    ] {
        let json = serde_json::to_string(&version).unwrap();
        let back: SchemaVersion = serde_json::from_str(&json).unwrap();
        assert_eq!(version, back);
    }
}

#[test]
fn event_type_all_variants_display() {
    let cases = vec![
        (EventType::DecisionEvaluated, "decision_evaluated"),
        (EventType::QuoteApproved, "quote_approved"),
        (EventType::QuoteRejected, "quote_rejected"),
        (EventType::OrderSubmitted, "order_submitted"),
        (EventType::OrderResized, "order_resized"),
        (EventType::OrderCancelled, "order_cancelled"),
        (EventType::FillDetected, "fill_detected"),
        (EventType::HedgeIntentCreated, "hedge_intent_created"),
        (
            EventType::HedgeDecisionEvaluated,
            "hedge_decision_evaluated",
        ),
        (EventType::HedgeResultRecorded, "hedge_result_recorded"),
        (EventType::HedgeExitPathRecorded, "hedge_exit_path_recorded"),
        (EventType::NeutralityEvaluated, "neutrality_evaluated"),
        (EventType::MonitorDegraded, "monitor_degraded"),
        (EventType::ProjectionRebuilt, "projection_rebuilt"),
        (EventType::RiskStateChanged, "risk_state_changed"),
        (
            EventType::UserStreamStatusChanged,
            "user_stream_status_changed",
        ),
        (EventType::StatusSnapshot, "status_snapshot"),
        (EventType::CalibrationAdjusted, "calibration_adjusted"),
    ];
    for (variant, expected) in cases {
        assert_eq!(variant.to_string(), expected);
    }
}

#[test]
fn event_type_serde_roundtrip() {
    for variant in [
        EventType::DecisionEvaluated,
        EventType::QuoteApproved,
        EventType::QuoteRejected,
        EventType::OrderSubmitted,
        EventType::OrderResized,
        EventType::OrderCancelled,
        EventType::FillDetected,
        EventType::HedgeIntentCreated,
        EventType::HedgeDecisionEvaluated,
        EventType::HedgeResultRecorded,
        EventType::HedgeExitPathRecorded,
        EventType::NeutralityEvaluated,
        EventType::MonitorDegraded,
        EventType::ProjectionRebuilt,
        EventType::RiskStateChanged,
        EventType::UserStreamStatusChanged,
        EventType::StatusSnapshot,
        EventType::CalibrationAdjusted,
    ] {
        let json = serde_json::to_string(&variant).unwrap();
        let back: EventType = serde_json::from_str(&json).unwrap();
        assert_eq!(variant, back);
    }
}

#[test]
fn event_type_deserializes_legacy_pascal_case_aliases() {
    assert_eq!(
        serde_json::from_str::<EventType>("\"HedgeIntentCreated\"").unwrap(),
        EventType::HedgeIntentCreated
    );
    assert_eq!(
        serde_json::from_str::<EventType>("\"HedgeDecisionEvaluated\"").unwrap(),
        EventType::HedgeDecisionEvaluated
    );
    assert_eq!(
        serde_json::from_str::<EventType>("\"HedgeExitPathRecorded\"").unwrap(),
        EventType::HedgeExitPathRecorded
    );
}

#[test]
fn priority_ordering() {
    assert!(Priority::Critical > Priority::High);
    assert!(Priority::High > Priority::Normal);
    assert!(Priority::Normal > Priority::Debug);
}

#[test]
fn priority_serde_roundtrip() {
    for p in [
        Priority::Debug,
        Priority::Normal,
        Priority::High,
        Priority::Critical,
    ] {
        let json = serde_json::to_string(&p).unwrap();
        let back: Priority = serde_json::from_str(&json).unwrap();
        assert_eq!(p, back);
    }
}

#[test]
fn envelope_new_sets_defaults() {
    let env = EventEnvelope::new(
        EventType::DecisionEvaluated,
        Priority::Normal,
        "run_test".to_string(),
        "test_component".to_string(),
        "dry-run".to_string(),
        serde_json::json!({"key": "value"}),
    );

    assert_eq!(env.schema_version, SchemaVersion::V1_5);
    assert_eq!(env.event_type, EventType::DecisionEvaluated);
    assert_eq!(env.priority, Priority::Normal);
    assert_eq!(env.run_id, "run_test");
    assert_eq!(env.source_component, "test_component");
    assert_eq!(env.mode, "dry-run");
    assert!(env.cycle_id.is_none());
    assert!(env.trace_id.is_none());
    assert!(env.condition_id.is_none());
    assert!(env.market_slug.is_none());
    assert!(env.question.is_none());
    assert!(env.order_id.is_none());
    assert!(env.asset_id.is_none());
    assert!(env.hedge_id.is_none());
}

#[test]
fn envelope_builder_methods() {
    let env = EventEnvelope::new(
        EventType::OrderSubmitted,
        Priority::High,
        "run_1".to_string(),
        "order_manager".to_string(),
        "live".to_string(),
        serde_json::json!({}),
    )
    .with_cycle_id("cycle_42".to_string())
    .with_trace_id("trace_abc".to_string())
    .with_condition_id("cond_xyz".to_string())
    .with_market_slug("will-it-rain".to_string())
    .with_question("Will it rain?".to_string())
    .with_order_id("order_123".to_string())
    .with_asset_id("asset_456".to_string())
    .with_hedge_id("hedge_789".to_string());

    assert_eq!(env.cycle_id.as_deref(), Some("cycle_42"));
    assert_eq!(env.trace_id.as_deref(), Some("trace_abc"));
    assert_eq!(env.condition_id.as_deref(), Some("cond_xyz"));
    assert_eq!(env.market_slug.as_deref(), Some("will-it-rain"));
    assert_eq!(env.question.as_deref(), Some("Will it rain?"));
    assert_eq!(env.order_id.as_deref(), Some("order_123"));
    assert_eq!(env.asset_id.as_deref(), Some("asset_456"));
    assert_eq!(env.hedge_id.as_deref(), Some("hedge_789"));
}

#[test]
fn envelope_serde_roundtrip() {
    let env = EventEnvelope::new(
        EventType::FillDetected,
        Priority::Critical,
        "run_serde".to_string(),
        "live_engine".to_string(),
        "live".to_string(),
        serde_json::json!({"fill_price": "0.55", "fill_size": "10"}),
    )
    .with_trace_id("trace_serde".to_string())
    .with_condition_id("cond_serde".to_string())
    .with_order_id("order_serde".to_string());

    let json = serde_json::to_string(&env).unwrap();
    let back: EventEnvelope = serde_json::from_str(&json).unwrap();

    assert_eq!(back.event_id, env.event_id);
    assert_eq!(back.schema_version, env.schema_version);
    assert_eq!(back.event_type, env.event_type);
    assert_eq!(back.priority, env.priority);
    assert_eq!(back.occurred_at, env.occurred_at);
    assert_eq!(back.recorded_at, env.recorded_at);
    assert_eq!(back.run_id, env.run_id);
    assert_eq!(back.trace_id, env.trace_id);
    assert_eq!(back.condition_id, env.condition_id);
    assert_eq!(back.order_id, env.order_id);
    assert_eq!(back.payload, env.payload);
}

#[test]
fn envelope_jsonl_roundtrip() {
    let events: Vec<EventEnvelope> = (0..5)
        .map(|i| {
            EventEnvelope::new(
                EventType::DecisionEvaluated,
                Priority::Normal,
                "run_jsonl".to_string(),
                "test".to_string(),
                "dry-run".to_string(),
                serde_json::json!({"index": i}),
            )
        })
        .collect();

    // Serialize to JSONL (one JSON object per line)
    let jsonl: String = events
        .iter()
        .map(|e| serde_json::to_string(e).unwrap())
        .collect::<Vec<_>>()
        .join("\n");

    // Deserialize from JSONL
    let back: Vec<EventEnvelope> = jsonl
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();

    assert_eq!(back.len(), 5);
    for (orig, recovered) in events.iter().zip(back.iter()) {
        assert_eq!(orig.event_id, recovered.event_id);
        assert_eq!(orig.payload, recovered.payload);
    }
}
