use spreadeater::trading::client::CancelOrderOutcome;

// ---------------------------------------------------------------------------
// CancelOrderOutcome variants
// ---------------------------------------------------------------------------

#[test]
fn cancel_order_outcome_confirmed_is_unit_variant() {
    let outcome = CancelOrderOutcome::Confirmed;
    assert!(matches!(outcome, CancelOrderOutcome::Confirmed));
}

#[test]
fn cancel_order_outcome_rejected_carries_reason() {
    let reason = "order not found".to_string();
    let outcome = CancelOrderOutcome::Rejected(reason.clone());
    match outcome {
        CancelOrderOutcome::Rejected(r) => assert_eq!(r, "order not found"),
        _ => panic!("Expected Rejected"),
    }
}

#[test]
fn cancel_order_outcome_unknown_carries_reason() {
    let outcome = CancelOrderOutcome::Unknown("server timeout".to_string());
    match outcome {
        CancelOrderOutcome::Unknown(r) => assert_eq!(r, "server timeout"),
        _ => panic!("Expected Unknown"),
    }
}

#[test]
fn cancel_order_outcome_equality() {
    assert_eq!(CancelOrderOutcome::Confirmed, CancelOrderOutcome::Confirmed);
    assert_eq!(
        CancelOrderOutcome::Rejected("a".into()),
        CancelOrderOutcome::Rejected("a".into())
    );
    assert_ne!(
        CancelOrderOutcome::Rejected("a".into()),
        CancelOrderOutcome::Rejected("b".into())
    );
    assert_ne!(
        CancelOrderOutcome::Confirmed,
        CancelOrderOutcome::Unknown("x".into())
    );
}

#[test]
fn cancel_order_outcome_clone() {
    let original = CancelOrderOutcome::Rejected("reason".to_string());
    let cloned = original.clone();
    assert_eq!(original, cloned);
}

#[test]
fn cancel_order_outcome_debug_format() {
    let outcome = CancelOrderOutcome::Confirmed;
    let debug = format!("{:?}", outcome);
    assert!(debug.contains("Confirmed"));

    let outcome = CancelOrderOutcome::Rejected("bad request".into());
    let debug = format!("{:?}", outcome);
    assert!(debug.contains("Rejected"));
    assert!(debug.contains("bad request"));
}
