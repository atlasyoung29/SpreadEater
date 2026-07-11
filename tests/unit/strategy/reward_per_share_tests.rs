use rust_decimal::Decimal;
use rust_decimal_macros::dec;

/// Compute reward per share: R = estimated_daily / total_shares_deployed.
/// Returns ZERO when total_shares is zero.
fn reward_per_share(est_daily: Decimal, total_shares: Decimal) -> Decimal {
    if total_shares > Decimal::ZERO {
        est_daily / total_shares
    } else {
        Decimal::ZERO
    }
}

/// Apply uncertainty discount: R_effective = R * discount_factor.
fn reward_per_share_effective(r: Decimal, discount_factor: Decimal) -> Decimal {
    r * discount_factor
}

/// Weighted average reward per share across markets.
fn weighted_avg_reward_per_share(est_dailies: &[Decimal], sizes: &[Decimal]) -> Decimal {
    let total_reward: Decimal = est_dailies.iter().copied().sum();
    let total_size: Decimal = sizes.iter().copied().sum();
    if total_size > Decimal::ZERO {
        total_reward / total_size
    } else {
        Decimal::ZERO
    }
}

#[test]
fn reward_per_share_zero_orders() {
    let r = reward_per_share(dec!(5.00), Decimal::ZERO);
    assert_eq!(r, Decimal::ZERO);
}

#[test]
fn reward_per_share_single_order() {
    // $5 daily reward / 100 shares = $0.05 per share = 5¢
    let r = reward_per_share(dec!(5.00), dec!(100));
    assert_eq!(r, dec!(0.05));
}

#[test]
fn reward_per_share_multiple_orders() {
    // $10 daily reward / 250 total shares = $0.04 per share = 4¢
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
    let sizes = [dec!(50), dec!(150)];
    let avg = weighted_avg_reward_per_share(&dailies, &sizes);
    assert_eq!(avg, dec!(0.05));
}

#[test]
fn weighted_avg_zero_shares() {
    let dailies = [dec!(5.00)];
    let sizes = [Decimal::ZERO];
    let avg = weighted_avg_reward_per_share(&dailies, &sizes);
    assert_eq!(avg, Decimal::ZERO);
}

#[test]
fn weighted_avg_empty() {
    let avg = weighted_avg_reward_per_share(&[], &[]);
    assert_eq!(avg, Decimal::ZERO);
}
