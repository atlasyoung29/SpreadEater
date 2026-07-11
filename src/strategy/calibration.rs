use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

/// Tracks per-order scoring feedback to calibrate the competition multiplier.
///
/// In live mode, we periodically check if our resting orders are scoring
/// via `GET /orders/{id}/scoring-status`. By comparing predicted vs actual,
/// we adjust the competition multiplier:
/// - Too many false positives (predicted scoring but not) → competition is
///   higher than estimated → increase multiplier
/// - False negatives (predicted not scoring but is) → we are too pessimistic
///   → decrease multiplier
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CalibrationTracker {
    samples: Vec<CalibrationSample>,
    current_multiplier: Decimal,
    min_multiplier: Decimal,
    max_multiplier: Decimal,
    sample_threshold: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CalibrationSample {
    pub condition_id: String,
    pub order_id: String,
    pub predicted_scoring: bool,
    pub actual_scoring: bool,
    pub predicted_share: Decimal,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CalibrationAdjustment {
    pub old_multiplier: Decimal,
    pub new_multiplier: Decimal,
    pub sample_count: usize,
    pub false_positives: usize,
    pub false_negatives: usize,
}

impl CalibrationTracker {
    pub fn new(initial_multiplier: Decimal, sample_threshold: usize) -> Self {
        Self {
            samples: Vec::new(),
            current_multiplier: initial_multiplier,
            min_multiplier: Decimal::new(5, 1), // 0.5
            max_multiplier: Decimal::new(5, 0), // 5.0
            sample_threshold,
        }
    }

    /// Record a calibration sample and auto-adjust if threshold is reached.
    pub fn record_sample(
        &mut self,
        condition_id: String,
        order_id: String,
        predicted_scoring: bool,
        actual_scoring: bool,
        predicted_share: Decimal,
    ) -> Option<CalibrationAdjustment> {
        self.samples.push(CalibrationSample {
            condition_id,
            order_id,
            predicted_scoring,
            actual_scoring,
            predicted_share,
            timestamp: Utc::now(),
        });

        debug!(
            samples = self.samples.len(),
            threshold = self.sample_threshold,
            "Calibration sample recorded"
        );

        if self.samples.len() >= self.sample_threshold {
            return self.adjust();
        }

        None
    }

    /// Current competition multiplier (may have been adjusted by calibration).
    pub fn current_multiplier(&self) -> Decimal {
        self.current_multiplier
    }

    /// Analyze samples and adjust the competition multiplier.
    fn adjust(&mut self) -> Option<CalibrationAdjustment> {
        let total = self.samples.len();
        if total == 0 {
            return None;
        }

        let predicted_scoring_count = self.samples.iter().filter(|s| s.predicted_scoring).count();

        let false_positives = self
            .samples
            .iter()
            .filter(|s| s.predicted_scoring && !s.actual_scoring)
            .count();

        let false_negatives = self
            .samples
            .iter()
            .filter(|s| !s.predicted_scoring && s.actual_scoring)
            .count();

        let old_multiplier = self.current_multiplier;

        if predicted_scoring_count > 0 {
            let fp_rate = Decimal::from(false_positives as u32)
                / Decimal::from(predicted_scoring_count as u32);

            // High false positive rate: we underestimate competition
            let thirty_pct = Decimal::new(30, 2);
            let ten_pct = Decimal::new(10, 2);

            if fp_rate > thirty_pct {
                // Increase multiplier by 20%
                self.current_multiplier *= Decimal::new(12, 1); // 1.2
            } else if fp_rate < ten_pct && false_negatives > 0 {
                // Decrease multiplier by 10%
                self.current_multiplier *= Decimal::new(9, 1); // 0.9
            }
        } else if false_negatives > 0 {
            // All predicted non-scoring but some actually are → too pessimistic
            self.current_multiplier *= Decimal::new(9, 1);
        }

        // Clamp
        self.current_multiplier = self
            .current_multiplier
            .max(self.min_multiplier)
            .min(self.max_multiplier);

        info!(
            old = %old_multiplier,
            new = %self.current_multiplier,
            false_positives = false_positives,
            false_negatives = false_negatives,
            total_samples = total,
            "Calibration adjustment"
        );

        let adjustment = CalibrationAdjustment {
            old_multiplier,
            new_multiplier: self.current_multiplier,
            sample_count: total,
            false_positives,
            false_negatives,
        };

        // Clear samples for next window
        self.samples.clear();
        Some(adjustment)
    }
}
