use spreadeater::models::events::UserEvent;

// ---------------------------------------------------------------------------
// UserEvent variant existence / pattern matching
// ---------------------------------------------------------------------------

#[test]
fn user_event_connected_variant() {
    let event = UserEvent::Connected { reconnect: false };
    match event {
        UserEvent::Connected { reconnect } => assert!(!reconnect),
        _ => panic!("Expected Connected"),
    }
}

#[test]
fn user_event_connected_reconnect_true() {
    let event = UserEvent::Connected { reconnect: true };
    match event {
        UserEvent::Connected { reconnect } => assert!(reconnect),
        _ => panic!("Expected Connected"),
    }
}

#[test]
fn user_event_raw_activity_variant() {
    let event = UserEvent::RawActivity;
    assert!(matches!(event, UserEvent::RawActivity));
}

#[test]
fn user_event_disconnected_variant() {
    let event = UserEvent::Disconnected;
    assert!(matches!(event, UserEvent::Disconnected));
}

#[test]
fn user_event_all_variants_matchable() {
    // Ensure the enum has exactly the variants we expect by matching exhaustively.
    // This test will fail to compile if a variant is added or removed.
    fn classify(event: &UserEvent) -> &'static str {
        match event {
            UserEvent::Connected { .. } => "connected",
            UserEvent::RawActivity => "raw-activity",
            UserEvent::Trade(_) => "trade",
            UserEvent::Order(_) => "order",
            UserEvent::Disconnected => "disconnected",
        }
    }

    assert_eq!(
        classify(&UserEvent::Connected { reconnect: false }),
        "connected"
    );
    assert_eq!(classify(&UserEvent::RawActivity), "raw-activity");
    assert_eq!(classify(&UserEvent::Disconnected), "disconnected");
}
