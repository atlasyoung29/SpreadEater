use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Tracked position for a single market.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub condition_id: String,
    pub yes_size: Decimal,
    pub no_size: Decimal,
    pub avg_yes_price: Decimal,
    pub avg_no_price: Decimal,
}

impl Position {
    pub fn new(condition_id: String) -> Self {
        Self {
            condition_id,
            yes_size: Decimal::ZERO,
            no_size: Decimal::ZERO,
            avg_yes_price: Decimal::ZERO,
            avg_no_price: Decimal::ZERO,
        }
    }

    /// Net directional exposure: positive = long YES, negative = long NO.
    pub fn net_exposure(&self) -> Decimal {
        self.yes_size - self.no_size
    }

    /// Number of complete sets (min of YES and NO) that could be merged.
    pub fn complete_sets(&self) -> Decimal {
        self.yes_size.min(self.no_size)
    }

    /// True if we have YES tokens available to sell.
    pub fn has_yes_inventory(&self, size: Decimal) -> bool {
        self.yes_size >= size
    }

    /// True if we have NO tokens available to sell.
    pub fn has_no_inventory(&self, size: Decimal) -> bool {
        self.no_size >= size
    }

    /// True if the position is hedged (both YES and NO sides > 0).
    pub fn is_hedged(&self) -> bool {
        self.yes_size > Decimal::ZERO && self.no_size > Decimal::ZERO
    }

    /// YES shares available to sell (excess above hedge pair).
    /// If YES=200, NO=200 → 0 (fully hedged, nothing sellable)
    /// If YES=200, NO=150 → 50 (excess)
    pub fn sellable_yes(&self) -> Decimal {
        (self.yes_size - self.no_size).max(Decimal::ZERO)
    }

    /// NO shares available to sell (excess above hedge pair).
    pub fn sellable_no(&self) -> Decimal {
        (self.no_size - self.yes_size).max(Decimal::ZERO)
    }
}

/// Aggregate account state across all markets.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountState {
    pub collateral_balance: Decimal,
    pub positions: Vec<Position>,
    pub total_position_value: Decimal,
}

impl AccountState {
    pub fn new() -> Self {
        Self {
            collateral_balance: Decimal::ZERO,
            positions: Vec::new(),
            total_position_value: Decimal::ZERO,
        }
    }

    pub fn get_position(&self, condition_id: &str) -> Option<&Position> {
        self.positions
            .iter()
            .find(|p| p.condition_id == condition_id)
    }
}
