use rust_decimal::Decimal;
use rust_decimal_macros::dec;

/// Compute reward per share: R = estimated_daily / shares_committed.
/// Returns ZERO when shares is zero.
fn reward_per_share(est_daily: Decimal, shares: Decimal) -> Decimal {
    if shares > Decimal::ZERO {
        est_daily / shares
    } else {
        Decimal::ZERO
    }
}

/// Apply uncertainty discount: R_effective = R * discount_factor.
fn reward_per_share_effective(r: Decimal, discount_factor: Decimal) -> Decimal {
    r * discount_factor
}

/// Weighted average reward per share across markets.
fn weighted_avg_reward_per_share(est_dailies: &[Decimal], shares: &[Decimal]) -> Decimal {
    let total_reward: Decimal = est_dailies.iter().copied().sum();
    let total_shares: Decimal = shares.iter().copied().sum();
    if total_shares > Decimal::ZERO {
        total_reward / total_shares
    } else {
        Decimal::ZERO
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[test]
fn reward_per_share_zero_shares() {
    let r = reward_per_share(dec!(5.00), Decimal::ZERO);
    assert_eq!(r, Decimal::ZERO);
}

#[test]
fn reward_per_share_single_market() {
    // $5 daily reward / 100 shares = $0.05 per share = 5¢/sh
    let r = reward_per_share(dec!(5.00), dec!(100));
    assert_eq!(r, dec!(0.05));
}

#[test]
fn reward_per_share_multiple_markets() {
    // $10 daily reward / 250 shares = $0.04 per share = 4¢/sh
    let r = reward_per_share(dec!(10.00), dec!(250));
    assert_eq!(r, dec!(0.04));
}

#[test]
fn reward_per_share_effective_default_discount() {
    let r = dec!(0.05);
    let r_eff = reward_per_share_effective(r, dec!(0.70));
    assert_eq!(r_eff, dec!(0.035));
}

#[test]
fn reward_per_share_effective_conservative_discount() {
    let r = dec!(0.05);
    let r_eff = reward_per_share_effective(r, dec!(0.50));
    assert_eq!(r_eff, dec!(0.025));
}

#[test]
fn weighted_avg_across_markets() {
    // Market A: $2/day, 50 shares; Market B: $8/day, 150 shares
    // Total: $10/200 = $0.05 per share
    let dailies = [dec!(2.00), dec!(8.00)];
    let shares = [dec!(50), dec!(150)];
    let avg = weighted_avg_reward_per_share(&dailies, &shares);
    assert_eq!(avg, dec!(0.05));
}

#[test]
fn weighted_avg_zero_shares() {
    let dailies = [dec!(5.00)];
    let shares = [Decimal::ZERO];
    let avg = weighted_avg_reward_per_share(&dailies, &shares);
    assert_eq!(avg, Decimal::ZERO);
}

#[test]
fn weighted_avg_empty() {
    let avg = weighted_avg_reward_per_share(&[], &[]);
    assert_eq!(avg, Decimal::ZERO);
}

// ---------------------------------------------------------------------------
// Decision rule tests
// ---------------------------------------------------------------------------

#[test]
fn decision_rule_reward_exceeds_hedge_cost() {
    let r_eff = dec!(0.05); // 5¢/sh reward
    let hedge_cost_per_share = dec!(0.03); // 3¢/sh hedge cost
    let edge = r_eff - hedge_cost_per_share;
    assert!(
        edge > Decimal::ZERO,
        "should be viable: reward > hedge cost"
    );
}

#[test]
fn decision_rule_reward_below_hedge_cost() {
    let r_eff = dec!(0.02); // 2¢/sh reward
    let hedge_cost_per_share = dec!(0.03); // 3¢/sh hedge cost
    let edge = r_eff - hedge_cost_per_share;
    assert!(
        edge < Decimal::ZERO,
        "should not be viable: reward < hedge cost"
    );
}

#[test]
fn decision_rule_with_min_return_threshold() {
    let r_eff = dec!(0.031); // 3.1¢/sh reward
    let hedge_cost_per_share = dec!(0.03); // 3.0¢/sh hedge cost
    let edge = r_eff - hedge_cost_per_share; // 0.1¢/sh
    let min_return_pct = dec!(0.0025); // 0.25% threshold
                                       // Edge is positive but below threshold
    assert!(edge > Decimal::ZERO, "edge is positive");
    assert!(edge < min_return_pct, "but below min return threshold");
}
