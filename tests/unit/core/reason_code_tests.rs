use spreadeater_core::CancelReasonCode;

#[test]
fn all_reason_codes_have_code_string() {
    let codes = vec![
        (CancelReasonCode::QuoteDrift, "QUOTE_DRIFT"),
        (
            CancelReasonCode::HedgeDepthBelowMinimum,
            "HEDGE_DEPTH_BELOW_MIN",
        ),
        (
            CancelReasonCode::HedgeDepthPartialDownsize,
            "HEDGE_DEPTH_PARTIAL_DOWNSIZE",
        ),
        (CancelReasonCode::MarketDeadmitted, "MARKET_DEADMITTED"),
        (CancelReasonCode::RiskHalt, "RISK_HALT"),
        (CancelReasonCode::ExternalCancel, "EXTERNAL_CANCEL"),
    ];
    for (variant, expected_code) in codes {
        assert_eq!(variant.code(), expected_code);
    }
}

#[test]
fn all_reason_codes_have_description() {
    for variant in [
        CancelReasonCode::QuoteDrift,
        CancelReasonCode::HedgeDepthBelowMinimum,
        CancelReasonCode::HedgeDepthPartialDownsize,
        CancelReasonCode::MarketDeadmitted,
        CancelReasonCode::RiskHalt,
        CancelReasonCode::ExternalCancel,
    ] {
        assert!(!variant.description().is_empty());
    }
}

#[test]
fn reason_code_display_matches_code() {
    for variant in [
        CancelReasonCode::QuoteDrift,
        CancelReasonCode::HedgeDepthBelowMinimum,
        CancelReasonCode::HedgeDepthPartialDownsize,
        CancelReasonCode::MarketDeadmitted,
        CancelReasonCode::RiskHalt,
        CancelReasonCode::ExternalCancel,
    ] {
        assert_eq!(variant.to_string(), variant.code());
    }
}

#[test]
fn reason_code_serde_roundtrip() {
    for variant in [
        CancelReasonCode::QuoteDrift,
        CancelReasonCode::HedgeDepthBelowMinimum,
        CancelReasonCode::HedgeDepthPartialDownsize,
        CancelReasonCode::MarketDeadmitted,
        CancelReasonCode::RiskHalt,
        CancelReasonCode::ExternalCancel,
    ] {
        let json = serde_json::to_string(&variant).unwrap();
        let back: CancelReasonCode = serde_json::from_str(&json).unwrap();
        assert_eq!(variant, back);
    }
}

#[test]
fn reason_code_equality() {
    assert_eq!(CancelReasonCode::QuoteDrift, CancelReasonCode::QuoteDrift);
    assert_ne!(CancelReasonCode::QuoteDrift, CancelReasonCode::RiskHalt);
}
