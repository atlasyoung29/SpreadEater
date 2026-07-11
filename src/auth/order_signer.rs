use anyhow::{Context, Result};
use k256::ecdsa::{signature::hazmat::PrehashSigner, SigningKey};
use rust_decimal::Decimal;
use sha3::{Digest, Keccak256};
use tracing::{debug, info};

use crate::models::{OrderType, Side, SignedOrder, SignedOrderPayload};

const CHAIN_ID: u64 = 137;
const USDC_DECIMALS: u64 = 1_000_000;

// Polymarket signature types
const SIG_TYPE_EOA: u8 = 0;
const SIG_TYPE_POLY_GNOSIS_SAFE: u8 = 2;

// Polymarket exchange contract addresses on Polygon
const CTF_EXCHANGE: &str = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E";
const NEG_RISK_CTF_EXCHANGE: &str = "0xC5d563A36AE78145C45a50134d48A1215220f80a";

/// Signs orders using EIP-712 typed data for Polymarket's CTF Exchange.
pub struct OrderSigner {
    signing_key: SigningKey,
    eoa_address: String,
    maker_address: String,
    sig_type: u8,
    /// Domain separator for regular CTF Exchange
    domain_separator: [u8; 32],
    /// Domain separator for Neg Risk CTF Exchange
    neg_risk_domain_separator: [u8; 32],
}

impl OrderSigner {
    pub fn new(private_key_hex: &str, poly_address: &str) -> Result<Self> {
        let hex_clean = private_key_hex
            .strip_prefix("0x")
            .unwrap_or(private_key_hex);
        let key_bytes = hex::decode(hex_clean).context("Invalid private key hex")?;
        let signing_key =
            SigningKey::from_bytes(key_bytes.as_slice().into()).context("Invalid private key")?;

        let eoa_address = derive_address(&signing_key);
        let domain_separator = compute_domain_separator("Polymarket CTF Exchange", CTF_EXCHANGE);
        let neg_risk_domain_separator =
            compute_domain_separator("Polymarket CTF Exchange", NEG_RISK_CTF_EXCHANGE);

        // Detect EOA vs proxy mode: if POLY_ADDRESS matches derived EOA, use EOA signing
        let (maker_address, sig_type) = if eoa_address.to_lowercase() == poly_address.to_lowercase()
        {
            info!(eoa = %eoa_address, "OrderSigner: EOA mode (signatureType=0)");
            (eoa_address.clone(), SIG_TYPE_EOA)
        } else {
            info!(eoa = %eoa_address, proxy = %poly_address, "OrderSigner: proxy mode (signatureType=2)");
            (poly_address.to_string(), SIG_TYPE_POLY_GNOSIS_SAFE)
        };

        Ok(Self {
            signing_key,
            eoa_address,
            maker_address,
            sig_type,
            domain_separator,
            neg_risk_domain_separator,
        })
    }

    /// Sign an order for the Polymarket CLOB API.
    pub fn sign_order(
        &self,
        token_id: &str,
        price: Decimal,
        size: Decimal,
        side: Side,
        order_type: OrderType,
        neg_risk: bool,
        fee_rate_bps: u64,
        tick_size: &str,
    ) -> Result<SignedOrderPayload> {
        // Salt must fit in JS Number.MAX_SAFE_INTEGER for server-side JSON parsing.
        // Python reference: generate_seed() = round(timestamp * random()) ≈ 1.7e9
        let salt: u64 = rand::random::<u64>() % 9_007_199_254_740_991;
        let side_u8: u8 = match side {
            Side::Buy => 0,
            Side::Sell => 1,
        };

        // Polymarket precision rules:
        //   size: up to 2 dp
        //   price: tick_size dp (e.g. "0.01" -> 2, "0.001" -> 3)
        //   notional amount: price_dp + size_dp (capped at 6 micro decimals)
        let tick_dp = tick_size_decimals(tick_size);
        let size_dp = 2;
        let amount_dp = (tick_dp + size_dp).min(6);

        let rounded_size = round_decimal_down(size, size_dp);
        let rounded_price = round_decimal_down(price, tick_dp);

        let size_units = decimal_to_units(rounded_size);
        let price_size_units = decimal_to_units(rounded_price * rounded_size);

        let (maker_amount, taker_amount) = match side {
            Side::Buy => (
                round_to_precision(price_size_units, amount_dp),
                round_to_precision(size_units, size_dp),
            ),
            Side::Sell => (
                round_to_precision(size_units, size_dp),
                round_to_precision(price_size_units, amount_dp),
            ),
        };

        // No expiration for GTC orders (Polymarket convention: 0 = never expires)
        let expiration: u64 = 0;

        let order_type_str = match order_type {
            OrderType::GTC => "GTC",
            OrderType::GTD => "GTD",
            OrderType::FOK => "FOK",
            OrderType::FAK => "FAK",
        };

        // Convert token_id decimal string to 32-byte big-endian
        let token_id_bytes =
            decimal_str_to_u256(token_id).context("Failed to parse token_id as uint256")?;

        // Build the struct hash
        let struct_hash = compute_order_struct_hash(
            salt as u128,
            &self.maker_address,
            &self.eoa_address,
            &token_id_bytes,
            maker_amount,
            taker_amount,
            expiration,
            0, // nonce
            fee_rate_bps,
            side_u8,
            self.sig_type,
        );

        // Use correct domain separator based on neg_risk
        let domain = if neg_risk {
            &self.neg_risk_domain_separator
        } else {
            &self.domain_separator
        };

        // EIP-712 digest
        let digest = eip712_digest(domain, &struct_hash);

        // ECDSA sign
        let signature = ecdsa_sign(&self.signing_key, &digest)?;

        let side_str = match side {
            Side::Buy => "BUY".to_string(),
            Side::Sell => "SELL".to_string(),
        };

        let signed_order = SignedOrder {
            salt: salt.to_string(),
            maker: self.maker_address.clone(),
            signer: self.eoa_address.clone(),
            taker: "0x0000000000000000000000000000000000000000".to_string(),
            token_id: token_id.to_string(),
            maker_amount: maker_amount.to_string(),
            taker_amount: taker_amount.to_string(),
            expiration: expiration.to_string(),
            nonce: "0".to_string(),
            fee_rate_bps: fee_rate_bps.to_string(),
            side: side_str,
            signature_type: self.sig_type,
            signature: format!("0x{}", hex::encode(&signature)),
        };

        debug!(
            token_id = %token_id,
            side = %side,
            price = %price,
            size = %size,
            maker_amount = %maker_amount,
            taker_amount = %taker_amount,
            neg_risk = %neg_risk,
            sig_type = %self.sig_type,
            "Order signed"
        );

        Ok(SignedOrderPayload {
            order: signed_order,
            owner: self.maker_address.clone(),
            order_type: order_type_str.to_string(),
        })
    }

    pub fn eoa_address(&self) -> &str {
        &self.eoa_address
    }
}

/// Convert a Decimal to USDC micro-units (multiply by 10^6, truncate to integer).
fn decimal_to_units(value: Decimal) -> u64 {
    let scaled = value * Decimal::from(USDC_DECIMALS);
    scaled.floor().to_string().parse::<u64>().unwrap_or(0)
}

/// Round a Decimal value down (truncate) to the given number of decimal places.
/// e.g. round_decimal_down(73.59769, 2) → 73.59
fn round_decimal_down(value: Decimal, dp: u32) -> Decimal {
    let factor = Decimal::from(10u64.pow(dp));
    (value * factor).floor() / factor
}

/// Round a micro-unit amount down to the allowed decimal precision.
/// `decimals` is the number of human-readable decimal places allowed.
/// e.g. decimals=2 means the amount must be a multiple of 10000 (10^(6-2)).
fn round_to_precision(amount: u64, decimals: u32) -> u64 {
    let divisor = 10u64.pow(6u32.saturating_sub(decimals));
    (amount / divisor) * divisor
}

/// Count the number of decimal places in a tick_size string.
/// "0.01" → 2, "0.001" → 3, "0.0001" → 4, "0.1" → 1
fn tick_size_decimals(tick_size: &str) -> u32 {
    match tick_size.find('.') {
        Some(dot) => (tick_size.len() - dot - 1) as u32,
        None => 0,
    }
}

fn keccak256(data: &[u8]) -> [u8; 32] {
    let mut hasher = Keccak256::new();
    hasher.update(data);
    hasher.finalize().into()
}

/// Derive the Ethereum address from a secp256k1 signing key (EIP-55 checksummed).
fn derive_address(key: &SigningKey) -> String {
    let verifying_key = key.verifying_key();
    let encoded = verifying_key.to_encoded_point(false);
    // Skip the 0x04 prefix byte, hash the 64-byte uncompressed public key
    let pubkey_bytes = &encoded.as_bytes()[1..];
    let hash = keccak256(pubkey_bytes);
    // Address is last 20 bytes
    let addr_hex = hex::encode(&hash[12..]);
    eip55_checksum(&addr_hex)
}

/// EIP-55 mixed-case checksum encoding for Ethereum addresses.
fn eip55_checksum(addr_hex_lower: &str) -> String {
    let hash = keccak256(addr_hex_lower.as_bytes());
    let hash_hex = hex::encode(hash);
    let mut result = String::with_capacity(42);
    result.push_str("0x");
    for (i, c) in addr_hex_lower.chars().enumerate() {
        if c.is_ascii_alphabetic() {
            // Each hex char of the hash corresponds to a nibble
            let nibble = u8::from_str_radix(&hash_hex[i..i + 1], 16).unwrap_or(0);
            if nibble >= 8 {
                result.push(c.to_ascii_uppercase());
            } else {
                result.push(c);
            }
        } else {
            result.push(c);
        }
    }
    result
}

/// Compute the EIP-712 domain separator for a Polymarket exchange contract.
/// Domain: { name, version: "1", chainId, verifyingContract }
fn compute_domain_separator(name: &str, verifying_contract: &str) -> [u8; 32] {
    compute_domain_separator_with_chain(name, verifying_contract, CHAIN_ID)
}

fn compute_domain_separator_with_chain(
    name: &str,
    verifying_contract: &str,
    chain_id: u64,
) -> [u8; 32] {
    let type_hash = keccak256(
        b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
    );
    let name_hash = keccak256(name.as_bytes());
    let version_hash = keccak256(b"1");

    let mut data = Vec::with_capacity(160);
    data.extend_from_slice(&type_hash);
    data.extend_from_slice(&name_hash);
    data.extend_from_slice(&version_hash);
    data.extend_from_slice(&u256_bytes(chain_id as u128));
    data.extend_from_slice(&address_bytes(verifying_contract));

    keccak256(&data)
}

/// Compute the EIP-712 struct hash for a Polymarket order.
fn compute_order_struct_hash(
    salt: u128,
    maker: &str,
    signer: &str,
    token_id_bytes: &[u8; 32],
    maker_amount: u64,
    taker_amount: u64,
    expiration: u64,
    nonce: u64,
    fee_rate_bps: u64,
    side: u8,
    signature_type: u8,
) -> [u8; 32] {
    let type_hash = keccak256(
        b"Order(uint256 salt,address maker,address signer,address taker,uint256 tokenId,uint256 makerAmount,uint256 takerAmount,uint256 expiration,uint256 nonce,uint256 feeRateBps,uint8 side,uint8 signatureType)"
    );

    let mut data = Vec::with_capacity(32 * 13);
    data.extend_from_slice(&type_hash);
    data.extend_from_slice(&u256_bytes(salt));
    data.extend_from_slice(&address_bytes(maker));
    data.extend_from_slice(&address_bytes(signer));
    data.extend_from_slice(&address_bytes("0x0000000000000000000000000000000000000000")); // taker
    data.extend_from_slice(token_id_bytes);
    data.extend_from_slice(&u256_bytes(maker_amount as u128));
    data.extend_from_slice(&u256_bytes(taker_amount as u128));
    data.extend_from_slice(&u256_bytes(expiration as u128));
    data.extend_from_slice(&u256_bytes(nonce as u128));
    data.extend_from_slice(&u256_bytes(fee_rate_bps as u128));
    data.extend_from_slice(&u256_bytes(side as u128));
    data.extend_from_slice(&u256_bytes(signature_type as u128));

    keccak256(&data)
}

/// EIP-712 digest: keccak256("\x19\x01" + domainSeparator + structHash)
fn eip712_digest(domain_separator: &[u8; 32], struct_hash: &[u8; 32]) -> [u8; 32] {
    let mut data = Vec::with_capacity(66);
    data.push(0x19);
    data.push(0x01);
    data.extend_from_slice(domain_separator);
    data.extend_from_slice(struct_hash);
    keccak256(&data)
}

/// ECDSA sign a 32-byte digest, returning 65-byte signature (r + s + v).
fn ecdsa_sign(key: &SigningKey, digest: &[u8; 32]) -> Result<Vec<u8>> {
    let (signature, recovery_id) = key
        .sign_prehash(digest)
        .map_err(|e| anyhow::anyhow!("ECDSA sign failed: {}", e))?;

    let r_bytes = signature.r().to_bytes();
    let s_bytes = signature.s().to_bytes();
    let v = recovery_id.to_byte() + 27;

    let mut sig = Vec::with_capacity(65);
    sig.extend_from_slice(&r_bytes);
    sig.extend_from_slice(&s_bytes);
    sig.push(v);

    Ok(sig)
}

/// Encode a u128 value as a big-endian 32-byte ABI word.
fn u256_bytes(value: u128) -> [u8; 32] {
    let mut buf = [0u8; 32];
    buf[16..].copy_from_slice(&value.to_be_bytes());
    buf
}

/// Encode an Ethereum address as a left-padded 32-byte ABI word.
fn address_bytes(addr: &str) -> [u8; 32] {
    let hex_clean = addr.strip_prefix("0x").unwrap_or(addr);
    let addr_bytes = hex::decode(hex_clean).unwrap_or_else(|_| vec![0u8; 20]);
    let mut buf = [0u8; 32];
    let len = addr_bytes.len().min(20);
    let start = 32 - len;
    buf[start..start + len].copy_from_slice(&addr_bytes[..len]);
    buf
}

/// Convert a decimal string (like a uint256 token ID) to 32-byte big-endian.
/// Handles arbitrarily large values up to 2^256 - 1.
fn decimal_str_to_u256(s: &str) -> Result<[u8; 32]> {
    let s = s.trim();
    if s.is_empty() || s == "0" {
        return Ok([0u8; 32]);
    }

    // Manual base-10 to base-256 conversion
    let mut result = [0u8; 32];
    for ch in s.chars() {
        let digit = ch
            .to_digit(10)
            .ok_or_else(|| anyhow::anyhow!("Invalid digit in token_id: '{}'", ch))?
            as u16;

        // Multiply result by 10 and add digit
        let mut carry = digit;
        for byte in result.iter_mut().rev() {
            let v = (*byte as u16) * 10 + carry;
            *byte = (v & 0xFF) as u8;
            carry = v >> 8;
        }
        if carry > 0 {
            anyhow::bail!("Token ID overflows uint256");
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_domain_separator_deterministic() {
        let ds1 = compute_domain_separator("Polymarket CTF Exchange", CTF_EXCHANGE);
        let ds2 = compute_domain_separator("Polymarket CTF Exchange", CTF_EXCHANGE);
        assert_eq!(ds1, ds2);
        assert_ne!(ds1, [0u8; 32]);

        // Neg risk domain should be different (same name, different contract address)
        let ds3 = compute_domain_separator("Polymarket CTF Exchange", NEG_RISK_CTF_EXCHANGE);
        assert_ne!(ds1, ds3);
    }

    #[test]
    fn test_derive_address() {
        // Well-known test: private key 1 -> known address
        let key_bytes =
            hex::decode("0000000000000000000000000000000000000000000000000000000000000001")
                .unwrap();
        let key = SigningKey::from_bytes(key_bytes.as_slice().into()).unwrap();
        let addr = derive_address(&key);
        assert!(addr.starts_with("0x"));
        assert_eq!(addr.len(), 42);
        // Private key 1 -> 0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf (EIP-55 checksummed)
        assert_eq!(addr, "0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf");
    }

    #[test]
    fn test_decimal_to_units() {
        assert_eq!(decimal_to_units(Decimal::new(1, 0)), 1_000_000); // 1.0 -> 1000000
        assert_eq!(decimal_to_units(Decimal::new(5, 1)), 500_000); // 0.5 -> 500000
        assert_eq!(decimal_to_units(Decimal::new(0, 0)), 0); // 0 -> 0
    }

    #[test]
    fn test_u256_bytes() {
        let bytes = u256_bytes(137);
        assert_eq!(bytes[31], 137);
        assert_eq!(bytes[30], 0);
    }

    #[test]
    fn test_address_bytes() {
        let bytes = address_bytes("0x0000000000000000000000000000000000000001");
        assert_eq!(bytes[31], 1);
        assert_eq!(bytes[11], 0); // padding
    }

    #[test]
    fn test_decimal_str_to_u256_small() {
        let result = decimal_str_to_u256("137").unwrap();
        assert_eq!(result[31], 137);
        assert_eq!(result[30], 0);
    }

    #[test]
    fn test_decimal_str_to_u256_large_token_id() {
        // Real Polymarket token ID that exceeds u128
        let token_id =
            "62174615336627888814453166657652087168672936561990669762061326057126859157348";
        let result = decimal_str_to_u256(token_id).unwrap();
        // Should not be all zeros (that would mean overflow/parse failure)
        assert_ne!(result, [0u8; 32]);
        // Verify round-trip: convert back to decimal string
        let mut val = vec![0u8; 0];
        let mut temp = result;
        loop {
            let mut remainder: u16 = 0;
            for byte in temp.iter_mut() {
                let v = (remainder << 8) | (*byte as u16);
                *byte = (v / 10) as u8;
                remainder = v % 10;
            }
            val.push(remainder as u8 + b'0');
            if temp.iter().all(|&b| b == 0) {
                break;
            }
        }
        val.reverse();
        let roundtrip = String::from_utf8(val).unwrap();
        assert_eq!(roundtrip, token_id);
    }

    #[test]
    fn test_sign_order_matches_reference() {
        // Test vector from 0xNathanW/clob-rs (uses alloy's built-in EIP-712)
        // Private key: Hardhat account #0
        let pk_hex = "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
        let key_bytes = hex::decode(pk_hex).unwrap();
        let signing_key = SigningKey::from_bytes(key_bytes.as_slice().into()).unwrap();
        let address = derive_address(&signing_key);
        assert_eq!(address, "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266");

        // Chain ID 80002 (Amoy testnet), non-neg-risk exchange
        let amoy_exchange = "0xdFE02Eb6733538f8Ea35D585af8DE5958AD99E40";
        let domain_sep =
            compute_domain_separator_with_chain("Polymarket CTF Exchange", amoy_exchange, 80002);

        let salt: u128 = 479249096354;
        let token_id_bytes = decimal_str_to_u256("1234").unwrap();

        let struct_hash = compute_order_struct_hash(
            salt,
            &address, // maker
            &address, // signer
            &token_id_bytes,
            100000000, // makerAmount
            50000000,  // takerAmount
            0,         // expiration
            0,         // nonce
            100,       // feeRateBps
            0,         // side (BUY)
            0,         // signatureType (EOA)
        );

        let digest = eip712_digest(&domain_sep, &struct_hash);
        let signature = ecdsa_sign(&signing_key, &digest).unwrap();
        let sig_hex = format!("0x{}", hex::encode(&signature));

        // Expected from alloy's sign_typed_data_sync (known-good reference)
        assert_eq!(
            sig_hex,
            "0x302cd9abd0b5fcaa202a344437ec0b6660da984e24ae9ad915a592a90facf5a51bb8a873cd8d270f070217fea1986531d5eec66f1162a81f66e026db653bf7ce1c"
        );
    }

    #[test]
    fn test_sign_order_preserves_fractional_share_precision_for_buy() {
        let signer = OrderSigner::new(
            "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
            "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
        )
        .unwrap();

        let payload = signer
            .sign_order(
                "1234",
                dec!(0.386),
                dec!(176.95),
                Side::Buy,
                OrderType::GTC,
                false,
                0,
                "0.001",
            )
            .unwrap();

        assert_eq!(payload.order.maker_amount, "68302700");
        assert_eq!(payload.order.taker_amount, "176950000");
    }

    #[test]
    fn test_sign_order_preserves_fractional_share_precision_for_sell() {
        let signer = OrderSigner::new(
            "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
            "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
        )
        .unwrap();

        let payload = signer
            .sign_order(
                "1234",
                dec!(0.329),
                dec!(176.95),
                Side::Sell,
                OrderType::FOK,
                false,
                0,
                "0.001",
            )
            .unwrap();

        assert_eq!(payload.order.maker_amount, "176950000");
        assert_eq!(payload.order.taker_amount, "58216550");
    }
}
