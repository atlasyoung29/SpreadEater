use rust_decimal_macros::dec;
use spreadeater::auth::OrderSigner;
use spreadeater::models::{OrderType, Side};

const TEST_PRIVATE_KEY: &str = "0000000000000000000000000000000000000000000000000000000000000001";
// Private key 1 derives to 0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf
const TEST_EOA_ADDRESS: &str = "0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf";

#[test]
fn new_with_valid_key() {
    let result = OrderSigner::new(TEST_PRIVATE_KEY, TEST_EOA_ADDRESS);
    assert!(
        result.is_ok(),
        "OrderSigner::new should succeed with valid hex key"
    );
}

#[test]
fn new_with_0x_prefix() {
    let prefixed = format!("0x{}", TEST_PRIVATE_KEY);
    let result = OrderSigner::new(&prefixed, TEST_EOA_ADDRESS);
    assert!(
        result.is_ok(),
        "OrderSigner::new should accept 0x-prefixed key"
    );
}

#[test]
fn eoa_address_is_hex() {
    let signer = OrderSigner::new(TEST_PRIVATE_KEY, TEST_EOA_ADDRESS).unwrap();
    let addr = signer.eoa_address();
    assert!(
        addr.starts_with("0x"),
        "EOA address should start with 0x, got: {}",
        addr
    );
    assert_eq!(
        addr.len(),
        42,
        "EOA address should be 42 chars (0x + 40 hex)"
    );
}

#[test]
fn sign_order_returns_valid_payload() {
    let signer = OrderSigner::new(TEST_PRIVATE_KEY, TEST_EOA_ADDRESS).unwrap();

    let payload = signer
        .sign_order(
            "1234",      // token_id
            dec!(0.50),  // price
            dec!(10.00), // size
            Side::Buy,
            OrderType::GTC,
            false,  // neg_risk
            0,      // fee_rate_bps
            "0.01", // tick_size
        )
        .unwrap();

    assert_eq!(payload.order_type, "GTC");
    assert_eq!(payload.order.side, "BUY");
    assert_eq!(payload.order.token_id, "1234");
    assert!(
        payload.order.signature.starts_with("0x"),
        "Signature should be hex-prefixed"
    );
    assert!(!payload.order.salt.is_empty(), "Salt should be non-empty");
    assert_eq!(payload.owner, TEST_EOA_ADDRESS);
}
