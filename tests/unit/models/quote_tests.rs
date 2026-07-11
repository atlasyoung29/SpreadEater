use rust_decimal_macros::dec;
use spreadeater::models::*;

use super::super::helpers::make_quote_candidate;

// ── QuoteLeg Display ──────────────────────────────────────────────

#[test]
fn quote_leg_display_yes_bid() {
    assert_eq!(QuoteLeg::YesBid.to_string(), "YES_BID");
}

#[test]
fn quote_leg_display_no_ask() {
    assert_eq!(QuoteLeg::NoAsk.to_string(), "NO_ASK");
}

// ── QuoteLeg hedge_uses_asks ──────────────────────────────────────

#[test]
fn quote_leg_hedge_uses_asks_yes_bid() {
    // Buying YES → hedge by buying NO asks → true
    assert!(QuoteLeg::YesBid.hedge_uses_asks());
}

#[test]
fn quote_leg_hedge_uses_asks_yes_ask() {
    // Selling YES → hedge by selling NO to bids → false
    assert!(!QuoteLeg::YesAsk.hedge_uses_asks());
}

#[test]
fn quote_leg_hedge_uses_asks_no_bid() {
    assert!(QuoteLeg::NoBid.hedge_uses_asks());
}

#[test]
fn quote_leg_hedge_uses_asks_no_ask() {
    assert!(!QuoteLeg::NoAsk.hedge_uses_asks());
}

// ── QuoteLeg classification helpers ───────────────────────────────

#[test]
fn quote_leg_is_yes_side() {
    assert!(QuoteLeg::YesBid.is_yes_side());
    assert!(QuoteLeg::YesAsk.is_yes_side());
    assert!(!QuoteLeg::NoBid.is_yes_side());
    assert!(!QuoteLeg::NoAsk.is_yes_side());
}

#[test]
fn quote_leg_is_bid() {
    assert!(QuoteLeg::YesBid.is_bid());
    assert!(QuoteLeg::NoBid.is_bid());
    assert!(!QuoteLeg::YesAsk.is_bid());
    assert!(!QuoteLeg::NoAsk.is_bid());
}

#[test]
fn quote_leg_is_ask() {
    assert!(QuoteLeg::YesAsk.is_ask());
    assert!(QuoteLeg::NoAsk.is_ask());
    assert!(!QuoteLeg::YesBid.is_ask());
    assert!(!QuoteLeg::NoBid.is_ask());
}

// ── QuoteStatus serde ─────────────────────────────────────────────

#[test]
fn quote_status_serde_roundtrip() {
    for status in [
        QuoteStatus::Approved,
        QuoteStatus::SimulatedOnly,
        QuoteStatus::Rejected,
        QuoteStatus::Suppressed,
    ] {
        let json = serde_json::to_string(&status).unwrap();
        let back: QuoteStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(status, back);
    }
}

// ── QuoteSet ──────────────────────────────────────────────────────

#[test]
fn quote_set_get_leg_found() {
    let candidate = make_quote_candidate("c1", QuoteLeg::YesBid, 0.48, 10.0, QuoteStatus::Approved);
    let set = QuoteSet {
        condition_id: "c1".to_string(),
        candidates: vec![candidate],
    };
    let found = set.get_leg(QuoteLeg::YesBid);
    assert!(found.is_some());
    assert_eq!(found.unwrap().price, dec!(0.48));
}

#[test]
fn quote_set_get_leg_not_found() {
    let candidate = make_quote_candidate("c1", QuoteLeg::YesBid, 0.48, 10.0, QuoteStatus::Approved);
    let set = QuoteSet {
        condition_id: "c1".to_string(),
        candidates: vec![candidate],
    };
    assert!(set.get_leg(QuoteLeg::NoAsk).is_none());
}

#[test]
fn quote_set_approved_legs() {
    let candidates = vec![
        make_quote_candidate("c1", QuoteLeg::YesBid, 0.48, 10.0, QuoteStatus::Approved),
        make_quote_candidate("c1", QuoteLeg::YesAsk, 0.52, 10.0, QuoteStatus::Approved),
        make_quote_candidate("c1", QuoteLeg::NoBid, 0.48, 10.0, QuoteStatus::Rejected),
        make_quote_candidate("c1", QuoteLeg::NoAsk, 0.52, 10.0, QuoteStatus::Suppressed),
    ];
    let set = QuoteSet {
        condition_id: "c1".to_string(),
        candidates,
    };
    assert_eq!(set.approved_legs().len(), 2);
}
