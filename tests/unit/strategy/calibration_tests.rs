use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use spreadeater::strategy::calibration::*;

// ---------------------------------------------------------------------------
// 1. Constructor returns initial multiplier
// ---------------------------------------------------------------------------
#[test]
fn new_returns_initial_multiplier() {
    let tracker = CalibrationTracker::new(dec!(1.5), 5);
    assert_eq!(tracker.current_multiplier(), dec!(1.5));
}

// ---------------------------------------------------------------------------
// 2. Below threshold, record_sample returns None
// ---------------------------------------------------------------------------
#[test]
fn record_sample_below_threshold_returns_none() {
    let mut tracker = CalibrationTracker::new(dec!(1.0), 5);
    for i in 0..3 {
        let result = tracker.record_sample(
            format!("cond-{}", i),
            format!("order-{}", i),
            true,
            true,
            dec!(0.05),
        );
        assert!(result.is_none());
    }
}

// ---------------------------------------------------------------------------
// 3. At threshold, record_sample returns Some
// ---------------------------------------------------------------------------
#[test]
fn record_sample_triggers_at_threshold() {
    let mut tracker = CalibrationTracker::new(dec!(1.0), 5);
    let mut last_result = None;
    for i in 0..5 {
        last_result = tracker.record_sample(
            format!("cond-{}", i),
            format!("order-{}", i),
            true,
            true,
            dec!(0.05),
        );
    }
    assert!(last_result.is_some());
}

// ---------------------------------------------------------------------------
// 4. High false positive rate increases multiplier by 20%
// ---------------------------------------------------------------------------
#[test]
fn high_false_positive_rate_increases_multiplier() {
    let mut tracker = CalibrationTracker::new(dec!(1.0), 5);

    // 4 false positives: predicted_scoring=true, actual_scoring=false
    for i in 0..4 {
        tracker.record_sample(
            format!("cond-{}", i),
            format!("order-{}", i),
            true,  // predicted scoring
            false, // actually not scoring
            dec!(0.05),
        );
    }
    // 1 true positive to reach threshold
    let adjustment = tracker.record_sample(
        "cond-4".to_string(),
        "order-4".to_string(),
        true,
        true,
        dec!(0.05),
    );

    let adj = adjustment.unwrap();
    // fp_rate = 4/5 = 0.80 > 0.30 → multiplier *= 1.2
    assert_eq!(adj.old_multiplier, dec!(1.0));
    assert_eq!(adj.new_multiplier, dec!(1.2));
    assert_eq!(tracker.current_multiplier(), dec!(1.2));
}

// ---------------------------------------------------------------------------
// 5. Low FP rate with false negatives decreases multiplier by 10%
// ---------------------------------------------------------------------------
#[test]
fn low_fp_with_false_negatives_decreases() {
    let mut tracker = CalibrationTracker::new(dec!(1.0), 5);

    // 4 false negatives: predicted=false, actual=true
    for i in 0..4 {
        tracker.record_sample(
            format!("cond-{}", i),
            format!("order-{}", i),
            false, // predicted not scoring
            true,  // actually scoring
            dec!(0.05),
        );
    }
    // 1 true positive (predicted_scoring=true, actual=true) → fp_rate = 0/1 = 0.0 < 0.10, fn=4>0
    let adjustment = tracker.record_sample(
        "cond-4".to_string(),
        "order-4".to_string(),
        true,
        true,
        dec!(0.05),
    );

    let adj = adjustment.unwrap();
    assert_eq!(adj.old_multiplier, dec!(1.0));
    assert_eq!(adj.new_multiplier, dec!(0.9));
    assert_eq!(tracker.current_multiplier(), dec!(0.9));
}

// ---------------------------------------------------------------------------
// 6. No predicted_scoring with false negatives decreases multiplier
// ---------------------------------------------------------------------------
#[test]
fn no_predicted_scoring_with_fn_decreases() {
    let mut tracker = CalibrationTracker::new(dec!(1.0), 5);

    // 3 false negatives: predicted=false, actual=true
    for i in 0..3 {
        tracker.record_sample(
            format!("cond-{}", i),
            format!("order-{}", i),
            false,
            true, // false negative
            dec!(0.05),
        );
    }
    // 1 true negative
    tracker.record_sample(
        "cond-3".to_string(),
        "order-3".to_string(),
        false,
        false,
        dec!(0.05),
    );
    // 5th sample triggers adjustment
    let adjustment = tracker.record_sample(
        "cond-4".to_string(),
        "order-4".to_string(),
        false,
        false,
        dec!(0.05),
    );

    let adj = adjustment.unwrap();
    // predicted_scoring_count = 0, false_negatives = 3 > 0 → multiply by 0.9
    assert_eq!(adj.old_multiplier, dec!(1.0));
    assert_eq!(adj.new_multiplier, dec!(0.9));
    assert_eq!(tracker.current_multiplier(), dec!(0.9));
}

// ---------------------------------------------------------------------------
// 7. Multiplier clamped at minimum (0.5)
// ---------------------------------------------------------------------------
#[test]
fn multiplier_clamped_min() {
    let mut tracker = CalibrationTracker::new(dec!(0.5), 5);

    // Trigger a decrease: all predicted=false, some actual=true
    for i in 0..4 {
        tracker.record_sample(
            format!("cond-{}", i),
            format!("order-{}", i),
            false,
            true,
            dec!(0.05),
        );
    }
    let adjustment = tracker.record_sample(
        "cond-4".to_string(),
        "order-4".to_string(),
        false,
        true,
        dec!(0.05),
    );

    let adj = adjustment.unwrap();
    // 0.5 * 0.9 = 0.45, clamped to 0.5
    assert_eq!(adj.new_multiplier, dec!(0.5));
    assert_eq!(tracker.current_multiplier(), dec!(0.5));
}

// ---------------------------------------------------------------------------
// 8. Multiplier clamped at maximum (5.0)
// ---------------------------------------------------------------------------
#[test]
fn multiplier_clamped_max() {
    let mut tracker = CalibrationTracker::new(dec!(4.5), 5);

    // Trigger an increase: high false positive rate
    for i in 0..4 {
        tracker.record_sample(
            format!("cond-{}", i),
            format!("order-{}", i),
            true,
            false, // false positive
            dec!(0.05),
        );
    }
    let adjustment = tracker.record_sample(
        "cond-4".to_string(),
        "order-4".to_string(),
        true,
        false,
        dec!(0.05),
    );

    let adj = adjustment.unwrap();
    // 4.5 * 1.2 = 5.4, clamped to 5.0
    assert_eq!(adj.new_multiplier, dec!(5.0));
    assert_eq!(tracker.current_multiplier(), dec!(5.0));
}

// ---------------------------------------------------------------------------
// 9. Samples cleared after adjustment — next call returns None
// ---------------------------------------------------------------------------
#[test]
fn samples_cleared_after_adjustment() {
    let mut tracker = CalibrationTracker::new(dec!(1.0), 5);

    // Fill to threshold
    for i in 0..5 {
        tracker.record_sample(
            format!("cond-{}", i),
            format!("order-{}", i),
            true,
            true,
            dec!(0.05),
        );
    }

    // After adjustment, samples are cleared. Next call should return None.
    let result = tracker.record_sample(
        "cond-new".to_string(),
        "order-new".to_string(),
        true,
        true,
        dec!(0.05),
    );
    assert!(result.is_none());
}
