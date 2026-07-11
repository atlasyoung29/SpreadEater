use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use spreadeater::models::Position;
use spreadeater::trading::positions::PositionManager;

// ---------------------------------------------------------------------------
// PositionManager: construction and empty state
// ---------------------------------------------------------------------------

#[tokio::test]
async fn new_starts_empty() {
    let pm = PositionManager::new(
        "https://data-api.example.com".to_string(),
        "0xdeadbeef".to_string(),
    );
    let state = pm.get_state().await;
    assert!(state.positions.is_empty());
    assert_eq!(state.collateral_balance, Decimal::ZERO);
    assert_eq!(state.total_position_value, Decimal::ZERO);
}

#[tokio::test]
async fn get_position_none_initially() {
    let pm = PositionManager::new("http://localhost".to_string(), "0x1".to_string());
    let pos = pm.get_position("unknown-condition").await;
    assert!(pos.is_none());
}

// ---------------------------------------------------------------------------
// update_position / get_position
// ---------------------------------------------------------------------------

fn make_test_position(condition_id: &str, yes: Decimal, no: Decimal) -> Position {
    let mut p = Position::new(condition_id.to_string());
    p.yes_size = yes;
    p.no_size = no;
    p.avg_yes_price = dec!(0.55);
    p.avg_no_price = dec!(0.45);
    p
}

#[tokio::test]
async fn update_and_get_position() {
    let pm = PositionManager::new("http://localhost".to_string(), "0x1".to_string());
    let pos = make_test_position("cond-1", dec!(100), dec!(50));
    pm.update_position(pos).await;

    let retrieved = pm.get_position("cond-1").await;
    assert!(retrieved.is_some());
    let r = retrieved.unwrap();
    assert_eq!(r.condition_id, "cond-1");
    assert_eq!(r.yes_size, dec!(100));
    assert_eq!(r.no_size, dec!(50));
}

#[tokio::test]
async fn update_position_replaces_existing() {
    let pm = PositionManager::new("http://localhost".to_string(), "0x1".to_string());

    let pos1 = make_test_position("cond-1", dec!(100), dec!(50));
    pm.update_position(pos1).await;

    let pos2 = make_test_position("cond-1", dec!(200), dec!(150));
    pm.update_position(pos2).await;

    let retrieved = pm.get_position("cond-1").await.unwrap();
    assert_eq!(retrieved.yes_size, dec!(200));
    assert_eq!(retrieved.no_size, dec!(150));
}

#[tokio::test]
async fn update_multiple_conditions() {
    let pm = PositionManager::new("http://localhost".to_string(), "0x1".to_string());

    pm.update_position(make_test_position("c1", dec!(10), dec!(5)))
        .await;
    pm.update_position(make_test_position("c2", dec!(20), dec!(15)))
        .await;

    let state = pm.get_state().await;
    assert_eq!(state.positions.len(), 2);

    let c1 = pm.get_position("c1").await.unwrap();
    assert_eq!(c1.yes_size, dec!(10));

    let c2 = pm.get_position("c2").await.unwrap();
    assert_eq!(c2.yes_size, dec!(20));
}

// ---------------------------------------------------------------------------
// total_position_cost
// ---------------------------------------------------------------------------

#[tokio::test]
async fn total_position_cost_empty() {
    let pm = PositionManager::new("http://localhost".to_string(), "0x1".to_string());
    let cost = pm.total_position_cost().await;
    assert_eq!(cost, Decimal::ZERO);
}

#[tokio::test]
async fn total_position_cost_single_position() {
    let pm = PositionManager::new("http://localhost".to_string(), "0x1".to_string());
    // yes_size=100, avg_yes=0.55, no_size=50, avg_no=0.45
    // cost = 100*0.55 + 50*0.45 = 55 + 22.5 = 77.5
    pm.update_position(make_test_position("c1", dec!(100), dec!(50)))
        .await;
    let cost = pm.total_position_cost().await;
    assert_eq!(cost, dec!(77.5));
}

#[tokio::test]
async fn total_position_cost_multiple_positions() {
    let pm = PositionManager::new("http://localhost".to_string(), "0x1".to_string());
    pm.update_position(make_test_position("c1", dec!(100), dec!(50)))
        .await;
    pm.update_position(make_test_position("c2", dec!(200), dec!(100)))
        .await;
    // c1: 100*0.55 + 50*0.45 = 77.5
    // c2: 200*0.55 + 100*0.45 = 110 + 45 = 155
    // total = 232.5
    let cost = pm.total_position_cost().await;
    assert_eq!(cost, dec!(232.5));
}

// ---------------------------------------------------------------------------
// has_inventory
// ---------------------------------------------------------------------------

#[tokio::test]
async fn has_inventory_yes_sufficient() {
    let pm = PositionManager::new("http://localhost".to_string(), "0x1".to_string());
    pm.update_position(make_test_position("c1", dec!(100), dec!(50)))
        .await;
    assert!(pm.has_inventory("c1", "YES", dec!(50)).await);
    assert!(pm.has_inventory("c1", "YES", dec!(100)).await);
}

#[tokio::test]
async fn has_inventory_no_sufficient() {
    let pm = PositionManager::new("http://localhost".to_string(), "0x1".to_string());
    pm.update_position(make_test_position("c1", dec!(100), dec!(50)))
        .await;
    assert!(pm.has_inventory("c1", "NO", dec!(25)).await);
    assert!(pm.has_inventory("c1", "NO", dec!(50)).await);
}

#[tokio::test]
async fn has_inventory_insufficient() {
    let pm = PositionManager::new("http://localhost".to_string(), "0x1".to_string());
    pm.update_position(make_test_position("c1", dec!(100), dec!(50)))
        .await;
    assert!(!pm.has_inventory("c1", "YES", dec!(101)).await);
    assert!(!pm.has_inventory("c1", "NO", dec!(51)).await);
}

#[tokio::test]
async fn has_inventory_unknown_condition() {
    let pm = PositionManager::new("http://localhost".to_string(), "0x1".to_string());
    assert!(!pm.has_inventory("unknown", "YES", dec!(1)).await);
}

#[tokio::test]
async fn has_inventory_unknown_outcome() {
    let pm = PositionManager::new("http://localhost".to_string(), "0x1".to_string());
    pm.update_position(make_test_position("c1", dec!(100), dec!(50)))
        .await;
    // Invalid outcome string returns false
    assert!(!pm.has_inventory("c1", "MAYBE", dec!(1)).await);
}
