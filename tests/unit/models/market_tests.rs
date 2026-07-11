use chrono::Utc;
use rust_decimal_macros::dec;
use spreadeater::models::*;

use super::super::helpers::*;

// ── Outcome Display ───────────────────────────────────────────────

#[test]
fn outcome_display_yes() {
    assert_eq!(Outcome::Yes.to_string(), "YES");
}

#[test]
fn outcome_display_no() {
    assert_eq!(Outcome::No.to_string(), "NO");
}

// ── Outcome serde ─────────────────────────────────────────────────

#[test]
fn outcome_serde_roundtrip() {
    for outcome in [Outcome::Yes, Outcome::No] {
        let json = serde_json::to_string(&outcome).unwrap();
        let back: Outcome = serde_json::from_str(&json).unwrap();
        assert_eq!(outcome, back);
    }
}

// ── MarketStatus serde ────────────────────────────────────────────

#[test]
fn market_status_serde_roundtrip() {
    for status in [
        MarketStatus::Admitted,
        MarketStatus::Quarantined,
        MarketStatus::Rejected,
    ] {
        let json = serde_json::to_string(&status).unwrap();
        let back: MarketStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(status, back);
    }
}

// ── Market token accessors ────────────────────────────────────────

#[test]
fn market_yes_token_found() {
    let market = make_market("c1");
    let yes = market.yes_token();
    assert!(yes.is_some());
    assert_eq!(yes.unwrap().token_id, "c1-yes");
}

#[test]
fn market_no_token_found() {
    let market = make_market("c1");
    let no = market.no_token();
    assert!(no.is_some());
    assert_eq!(no.unwrap().token_id, "c1-no");
}

#[test]
fn market_yes_token_not_found() {
    let market = Market {
        condition_id: "c1".to_string(),
        market_slug: "slug".to_string(),
        question: "Q?".to_string(),
        active: true,
        closed: false,
        archived: false,
        accepting_orders: true,
        is_binary: true,
        neg_risk: false,
        minimum_tick_size: "0.01".to_string(),
        tokens: vec![TokenInfo {
            token_id: "c1-no-only".to_string(),
            outcome: Outcome::No,
            last_price: Some(dec!(0.50)),
        }],
        reward_config: None,
        end_date_iso: None,
        discovered_at: Utc::now(),
    };
    assert!(market.yes_token().is_none());
}

// ── CanonicalMarket serde ─────────────────────────────────────────

#[test]
fn canonical_market_serde_roundtrip() {
    let cm = make_canonical_market("c1");
    let json = serde_json::to_string(&cm).unwrap();
    let back: CanonicalMarket = serde_json::from_str(&json).unwrap();
    assert_eq!(back.condition_id, "c1");
    assert_eq!(back.yes_token_id, "c1-yes");
    assert_eq!(back.no_token_id, "c1-no");
    assert_eq!(back.status, MarketStatus::Admitted);
}

// ── RewardConfig serde ────────────────────────────────────────────

#[test]
fn reward_config_serde_roundtrip() {
    let rc = make_reward_config("c1", 10.0);
    let json = serde_json::to_string(&rc).unwrap();
    let back: RewardConfig = serde_json::from_str(&json).unwrap();
    assert_eq!(back.condition_id, "c1");
    assert_eq!(back.daily_reward_total, rc.daily_reward_total);
    assert_eq!(back.min_size, dec!(5.0));
    assert_eq!(back.max_spread, dec!(0.04));
}
