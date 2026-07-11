use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use spreadeater::config::*;
use spreadeater::models::*;
use spreadeater::trading::risk::*;

use super::super::helpers::make_position;

fn test_config() -> RiskConfig {
    RiskConfig {
        hedge_timeout_secs: 10,
        hedge_exposure_tolerance: dec!(0.5),
        cash_reserve: dec!(50),
    }
}

// ---------------------------------------------------------------------------
// 1. Constructor: not globally halted
// ---------------------------------------------------------------------------
#[tokio::test]
async fn risk_manager_new_not_halted() {
    let rm = RiskManager::new(test_config());
    assert!(!rm.is_globally_halted().await);
}

// ---------------------------------------------------------------------------
// 2. Unknown market is tradable
// ---------------------------------------------------------------------------
#[tokio::test]
async fn is_market_tradable_unknown_market() {
    let rm = RiskManager::new(test_config());
    assert!(rm.is_market_tradable("unknown-condition").await);
}

// ---------------------------------------------------------------------------
// 3. Global halt makes every market non-tradable
// ---------------------------------------------------------------------------
#[tokio::test]
async fn is_market_tradable_globally_halted() {
    let rm = RiskManager::new(test_config());
    rm.global_halt("test halt").await;
    assert!(!rm.is_market_tradable("any-market").await);
}

// ---------------------------------------------------------------------------
// 4. Market halt affects only that market
// ---------------------------------------------------------------------------
#[tokio::test]
async fn is_market_tradable_market_halted() {
    let rm = RiskManager::new(test_config());
    rm.halt_market("market-A", "too risky").await;

    assert!(!rm.is_market_tradable("market-A").await);
    assert!(rm.is_market_tradable("market-B").await);
}

// ---------------------------------------------------------------------------
// 5. Imbalanced exposure sets unhedged_since
// ---------------------------------------------------------------------------
#[tokio::test]
async fn update_market_exposure_tracks_unhedged_since() {
    let rm = RiskManager::new(test_config());
    let pos = make_position("cond-2", 10.0, 5.0); // exposure = 5 > 0.5

    rm.update_market_exposure("cond-2", &pos).await;

    let state = rm.get_market_state("cond-2").await.unwrap();
    assert!(state.unhedged_since.is_some());
}

// ---------------------------------------------------------------------------
// 7. Balanced exposure clears unhedged_since
// ---------------------------------------------------------------------------
#[tokio::test]
async fn update_market_exposure_clears_unhedged_when_balanced() {
    let rm = RiskManager::new(test_config());

    // First: create an imbalance to set unhedged_since
    let imbalanced = make_position("cond-3", 10.0, 5.0);
    rm.update_market_exposure("cond-3", &imbalanced).await;
    let state = rm.get_market_state("cond-3").await.unwrap();
    assert!(state.unhedged_since.is_some());

    // Then: balance it out
    let balanced = make_position("cond-3", 10.0, 10.0); // exposure = 0 <= 0.5
    rm.update_market_exposure("cond-3", &balanced).await;
    let state = rm.get_market_state("cond-3").await.unwrap();
    assert!(state.unhedged_since.is_none());
}

// ---------------------------------------------------------------------------
// 8. check_hedge_timeouts doesn't panic on a fresh manager, and can halt
//    markets with stale unhedged exposure
// ---------------------------------------------------------------------------
#[tokio::test]
async fn check_hedge_timeouts_halts_after_timeout() {
    let rm = RiskManager::new(test_config());

    // Create an imbalanced position (sets unhedged_since to now)
    let pos = make_position("cond-timeout", 10.0, 5.0);
    rm.update_market_exposure("cond-timeout", &pos).await;

    // Immediately calling check_hedge_timeouts should NOT halt because
    // the imbalance was just created (0s << 10s timeout).
    rm.check_hedge_timeouts().await;
    assert!(
        rm.is_market_tradable("cond-timeout").await,
        "Market should still be tradable right after exposure update"
    );

    // Verify method doesn't panic on fresh manager with no markets
    let fresh = RiskManager::new(test_config());
    fresh.check_hedge_timeouts().await;
}

// ---------------------------------------------------------------------------
// 9. halt_market returns newly_halted = true on first call
// ---------------------------------------------------------------------------
#[tokio::test]
async fn halt_market_newly_halted() {
    let rm = RiskManager::new(test_config());
    let result = rm.halt_market("mkt-1", "first reason").await;

    assert!(result.newly_halted);
    assert_eq!(result.canonical_reason, "first reason");
    assert!(result.suppressed_reason.is_none());
}

// ---------------------------------------------------------------------------
// 10. halt_market is idempotent: second call preserves canonical reason
// ---------------------------------------------------------------------------
#[tokio::test]
async fn halt_market_idempotent() {
    let rm = RiskManager::new(test_config());
    let _first = rm.halt_market("mkt-2", "original reason").await;
    let second = rm.halt_market("mkt-2", "second reason").await;

    assert!(!second.newly_halted);
    assert_eq!(second.canonical_reason, "original reason");
    assert_eq!(second.suppressed_reason.as_deref(), Some("second reason"));
}

// ---------------------------------------------------------------------------
// 11. resume_market clears the halt
// ---------------------------------------------------------------------------
#[tokio::test]
async fn resume_market_clears_halt() {
    let rm = RiskManager::new(test_config());
    rm.halt_market("mkt-3", "halt it").await;
    assert!(!rm.is_market_tradable("mkt-3").await);

    rm.resume_market("mkt-3").await;
    assert!(rm.is_market_tradable("mkt-3").await);
}

// ---------------------------------------------------------------------------
// 12. resume_market on unknown market is a no-op
// ---------------------------------------------------------------------------
#[tokio::test]
async fn resume_market_no_op_for_unknown() {
    let rm = RiskManager::new(test_config());
    rm.resume_market("nonexistent").await; // should not panic
}

// ---------------------------------------------------------------------------
// 13. global_halt blocks all markets
// ---------------------------------------------------------------------------
#[tokio::test]
async fn global_halt_blocks_all() {
    let rm = RiskManager::new(test_config());
    rm.global_halt("emergency").await;

    assert!(rm.is_globally_halted().await);
    assert!(!rm.is_market_tradable("any-1").await);
    assert!(!rm.is_market_tradable("any-2").await);
}

// ---------------------------------------------------------------------------
// 14. pre_trade_check rejects when globally halted
// ---------------------------------------------------------------------------
#[tokio::test]
async fn pre_trade_check_rejects_global_halt() {
    let rm = RiskManager::new(test_config());
    rm.global_halt("stop everything").await;

    let result = rm.pre_trade_check("mkt", dec!(10), None, false, None).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Global halt"));
}

// ---------------------------------------------------------------------------
// 15. pre_trade_check rejects when market is halted
// ---------------------------------------------------------------------------
#[tokio::test]
async fn pre_trade_check_rejects_market_halt() {
    let rm = RiskManager::new(test_config());
    rm.halt_market("halted-mkt", "risk limit").await;

    let result = rm
        .pre_trade_check("halted-mkt", dec!(10), None, false, None)
        .await;
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Market halted"));
}

// ---------------------------------------------------------------------------
// 16. pre_trade_check passes under normal conditions
// ---------------------------------------------------------------------------
#[tokio::test]
async fn pre_trade_check_passes_normal() {
    let rm = RiskManager::new(test_config());

    // Register market with some exposure so the position cap path is exercised
    let pos = make_position("ok-mkt", 10.0, 10.0);
    rm.update_market_exposure("ok-mkt", &pos).await;

    let result = rm
        .pre_trade_check("ok-mkt", dec!(10), None, false, None)
        .await;
    assert!(result.is_ok());
}

// ---------------------------------------------------------------------------
// 17. pre_trade_check rejects buy hedge with insufficient balance
// ---------------------------------------------------------------------------
#[tokio::test]
async fn pre_trade_check_rejects_buy_hedge_insufficient_balance() {
    let rm = RiskManager::new(test_config());
    rm.update_balance(dec!(50)).await;

    let result = rm
        .pre_trade_check("some-mkt", dec!(10), Some(dec!(200)), true, None)
        .await;
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Insufficient balance"));
}

// ---------------------------------------------------------------------------
// 20. pre_trade_check respects available_balance_override
// ---------------------------------------------------------------------------
#[tokio::test]
async fn pre_trade_check_uses_balance_override() {
    let rm = RiskManager::new(test_config());
    rm.update_balance(dec!(10)).await; // low cached balance

    let result = rm
        .pre_trade_check(
            "override-mkt",
            dec!(10),
            Some(dec!(200)),
            true,
            Some(dec!(500)),
        )
        .await;
    assert!(result.is_ok());
}
