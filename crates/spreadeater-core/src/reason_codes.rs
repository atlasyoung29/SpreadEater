use serde::{Deserialize, Serialize};

/// Taxonomy of cancellation reasons for orders.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CancelReasonCode {
    QuoteDrift,
    HedgeDepthBelowMinimum,
    HedgeDepthPartialDownsize,
    OutcomePriceBelowMinimum,
    MarketDeadmitted,
    FrontierRebalance,
    RiskHalt,
    ExternalCancel,
}

impl CancelReasonCode {
    /// Short machine-readable code string.
    pub fn code(&self) -> &'static str {
        match self {
            Self::QuoteDrift => "QUOTE_DRIFT",
            Self::HedgeDepthBelowMinimum => "HEDGE_DEPTH_BELOW_MIN",
            Self::HedgeDepthPartialDownsize => "HEDGE_DEPTH_PARTIAL_DOWNSIZE",
            Self::OutcomePriceBelowMinimum => "OUTCOME_PRICE_BELOW_MIN",
            Self::MarketDeadmitted => "MARKET_DEADMITTED",
            Self::FrontierRebalance => "FRONTIER_REBALANCE",
            Self::RiskHalt => "RISK_HALT",
            Self::ExternalCancel => "EXTERNAL_CANCEL",
        }
    }

    /// Human-readable description.
    pub fn description(&self) -> &'static str {
        match self {
            Self::QuoteDrift => "Price drifted beyond quote_drift_bps threshold",
            Self::HedgeDepthBelowMinimum => "Opposite-side depth below min_size",
            Self::HedgeDepthPartialDownsize => "Depth below order size but above min_size",
            Self::OutcomePriceBelowMinimum => {
                "Outcome price fell below min_outcome_price threshold"
            }
            Self::MarketDeadmitted => "Market no longer meets admission criteria",
            Self::FrontierRebalance => {
                "Order cancelled to rotate capital into a better-ranked bid market"
            }
            Self::RiskHalt => "Risk manager triggered halt or kill switch",
            Self::ExternalCancel => "Order cancelled externally (UI, API)",
        }
    }
}

impl std::fmt::Display for CancelReasonCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.code())
    }
}
