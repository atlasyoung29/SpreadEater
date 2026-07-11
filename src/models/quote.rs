use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum QuoteLeg {
    YesBid,
    YesAsk,
    NoBid,
    NoAsk,
}

impl std::fmt::Display for QuoteLeg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QuoteLeg::YesBid => write!(f, "YES_BID"),
            QuoteLeg::YesAsk => write!(f, "YES_ASK"),
            QuoteLeg::NoBid => write!(f, "NO_BID"),
            QuoteLeg::NoAsk => write!(f, "NO_ASK"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum QuoteStatus {
    Approved,
    SimulatedOnly,
    Rejected,
    Suppressed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuoteCandidate {
    pub condition_id: String,
    pub leg: QuoteLeg,
    pub price: Decimal,
    pub size: Decimal,
    pub status: QuoteStatus,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuoteSet {
    pub condition_id: String,
    pub candidates: Vec<QuoteCandidate>,
}

impl QuoteSet {
    pub fn get_leg(&self, leg: QuoteLeg) -> Option<&QuoteCandidate> {
        self.candidates.iter().find(|c| c.leg == leg)
    }

    pub fn approved_legs(&self) -> Vec<&QuoteCandidate> {
        self.candidates
            .iter()
            .filter(|c| c.status == QuoteStatus::Approved)
            .collect()
    }
}

impl QuoteLeg {
    /// The opposite-outcome token side used for hedging this leg.
    /// If we get filled on YES_BID (we bought YES), we hedge by buying NO (NO asks).
    /// If we get filled on YES_ASK (we sold YES), we hedge by selling NO (NO bids).
    /// If we get filled on NO_BID (we bought NO), we hedge by buying YES (YES asks).
    /// If we get filled on NO_ASK (we sold NO), we hedge by selling YES (YES bids).
    pub fn hedge_uses_asks(&self) -> bool {
        matches!(self, QuoteLeg::YesBid | QuoteLeg::NoBid)
    }

    pub fn is_yes_side(&self) -> bool {
        matches!(self, QuoteLeg::YesBid | QuoteLeg::YesAsk)
    }

    pub fn is_bid(&self) -> bool {
        matches!(self, QuoteLeg::YesBid | QuoteLeg::NoBid)
    }

    pub fn is_ask(&self) -> bool {
        matches!(self, QuoteLeg::YesAsk | QuoteLeg::NoAsk)
    }
}
