//! Gasless CTF mergePositions via Polymarket's SAFE relayer flow.
//!
//! Burns equal YES + NO token pairs and credits USDC to the Safe without
//! requiring wallet-funded Polygon gas.

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use k256::ecdsa::{signature::hazmat::PrehashSigner, SigningKey};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha3::{Digest, Keccak256};
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};
use tracing::{info, warn};

/// USDC.e (Bridged USDC) on Polygon
const USDC_ADDRESS: &str = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174";

/// Polymarket Conditional Tokens Framework contract on Polygon
const CTF_ADDRESS: &str = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045";

/// Polymarket negative-risk adapter contract on Polygon
const NEG_RISK_ADAPTER_ADDRESS: &str = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296";

const CHAIN_ID: u64 = 137; // Polygon
const DEFAULT_RELAYER_BASE_URL: &str = "https://relayer-v2.polymarket.com";
const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";
const RELAYER_MAX_POLLS: usize = 30;
const RELAYER_POLL_INTERVAL: Duration = Duration::from_secs(2);
const RELAYER_TIMEOUT_RETRY_ATTEMPTS: usize = 3;
const RELAYER_TIMEOUT_RETRY_DELAY: Duration = Duration::from_secs(1);
const SAFE_TX_TERMINAL_RETRY_ATTEMPTS: usize = 2;
const SAFE_TX_TERMINAL_RETRY_DELAY: Duration = Duration::from_secs(3);

#[async_trait]
pub trait PairMerger: Send + Sync {
    async fn preflight_check(&self) -> Result<()>;
    async fn merge_positions(
        &self,
        condition_id: &str,
        amount: u64,
        neg_risk: bool,
    ) -> Result<String>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MergeVenue {
    Standard,
    NegRisk,
}

impl MergeVenue {
    fn from_neg_risk(neg_risk: bool) -> Self {
        if neg_risk {
            Self::NegRisk
        } else {
            Self::Standard
        }
    }

    fn merge_target(self) -> &'static str {
        match self {
            Self::Standard => CTF_ADDRESS,
            Self::NegRisk => NEG_RISK_ADAPTER_ADDRESS,
        }
    }

    fn approval_operator(self) -> &'static str {
        self.merge_target()
    }

    fn label(self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::NegRisk => "neg_risk",
        }
    }
}

pub struct CtfMerger {
    signing_key: SigningKey,
    signer_address_hex: String,
    safe_address: [u8; 20],
    safe_address_hex: String,
    relayer_base_url: String,
    relayer_api_key: String,
    relayer_api_key_address: String,
    http_client: reqwest::Client,
    neg_risk_approval_checked: AtomicBool,
    neg_risk_approval_lock: Mutex<()>,
}

#[derive(Debug, Deserialize)]
struct RelayerApiKeyRecord {
    address: String,
}

#[derive(Debug, Deserialize)]
struct RelayerDeployedResponse {
    deployed: bool,
}

#[derive(Debug, Deserialize)]
struct RelayerNonceResponse {
    nonce: String,
}

#[derive(Debug, Clone, Deserialize)]
struct RelayerTransactionStatus {
    #[serde(rename = "transactionID")]
    transaction_id: String,
    #[serde(rename = "transactionHash", default)]
    transaction_hash: String,
    state: String,
}

#[derive(Debug, Serialize)]
struct RelayerSubmitRequest {
    from: String,
    to: String,
    #[serde(rename = "proxyWallet")]
    proxy_wallet: String,
    data: String,
    nonce: String,
    signature: String,
    #[serde(rename = "signatureParams")]
    signature_params: RelayerSignatureParams,
    #[serde(rename = "type")]
    tx_type: String,
}

#[derive(Debug)]
struct RelayerHttpError {
    message: String,
    retryable: bool,
}

impl RelayerHttpError {
    fn retryable(message: String) -> Self {
        Self {
            message,
            retryable: true,
        }
    }

    fn terminal(message: String) -> Self {
        Self {
            message,
            retryable: false,
        }
    }
}

impl std::fmt::Display for RelayerHttpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for RelayerHttpError {}

#[derive(Debug, Clone, Serialize)]
struct RelayerSignatureParams {
    #[serde(rename = "gasPrice")]
    gas_price: String,
    operation: String,
    #[serde(rename = "safeTxnGas")]
    safe_txn_gas: String,
    #[serde(rename = "baseGas")]
    base_gas: String,
    #[serde(rename = "gasToken")]
    gas_token: String,
    #[serde(rename = "refundReceiver")]
    refund_receiver: String,
}

impl Default for RelayerSignatureParams {
    fn default() -> Self {
        Self {
            gas_price: "0".to_string(),
            operation: "0".to_string(),
            safe_txn_gas: "0".to_string(),
            base_gas: "0".to_string(),
            gas_token: ZERO_ADDRESS.to_string(),
            refund_receiver: ZERO_ADDRESS.to_string(),
        }
    }
}

impl CtfMerger {
    pub fn new(
        private_key_hex: &str,
        signer_address_hex: &str,
        safe_address_hex: &str,
        relayer_api_key: &str,
        relayer_api_key_address: &str,
    ) -> Result<Self> {
        Self::new_with_relayer_url(
            private_key_hex,
            signer_address_hex,
            safe_address_hex,
            relayer_api_key,
            relayer_api_key_address,
            DEFAULT_RELAYER_BASE_URL,
        )
    }

    #[doc(hidden)]
    pub fn new_with_relayer_url(
        private_key_hex: &str,
        signer_address_hex: &str,
        safe_address_hex: &str,
        relayer_api_key: &str,
        relayer_api_key_address: &str,
        relayer_base_url: &str,
    ) -> Result<Self> {
        let hex_clean = private_key_hex
            .strip_prefix("0x")
            .unwrap_or(private_key_hex);
        let key_bytes = hex::decode(hex_clean).context("Invalid private key hex")?;
        let signing_key =
            SigningKey::from_bytes(key_bytes.as_slice().into()).context("Invalid private key")?;

        let signer_address_hex =
            normalize_address_hex(signer_address_hex, "Invalid signer address")?;
        let safe_address_hex = normalize_address_hex(safe_address_hex, "Invalid safe address")?;
        let relayer_api_key_address =
            normalize_address_hex(relayer_api_key_address, "Invalid relayer API key address")?;
        if relayer_api_key.trim().is_empty() {
            bail!("Relayer API key cannot be empty");
        }

        let derived_signer = derive_address(&signing_key);
        if !derived_signer.eq_ignore_ascii_case(&signer_address_hex) {
            bail!(
                "Provided signer address {} does not match POLY_PRIVATE_KEY-derived address {}",
                signer_address_hex,
                derived_signer
            );
        }
        if !relayer_api_key_address.eq_ignore_ascii_case(&signer_address_hex) {
            bail!(
                "RELAYER_API_KEY_ADDRESS {} must match the signer address {} for SAFE relayer merge",
                relayer_api_key_address,
                signer_address_hex
            );
        }

        let safe_address = parse_exact_address(&safe_address_hex, "Invalid safe address")?;
        let relayer_base_url = relayer_base_url.trim_end_matches('/').to_string();
        if relayer_base_url.is_empty() {
            bail!("Relayer base URL cannot be empty");
        }

        Ok(Self {
            signing_key,
            signer_address_hex,
            safe_address,
            safe_address_hex,
            relayer_base_url,
            relayer_api_key: relayer_api_key.to_string(),
            relayer_api_key_address,
            http_client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .context("Failed to build relayer HTTP client")?,
            neg_risk_approval_checked: AtomicBool::new(false),
            neg_risk_approval_lock: Mutex::new(()),
        })
    }

    /// Merge `amount` complete YES+NO sets into USDC via the Polymarket relayer.
    pub async fn merge_positions(
        &self,
        condition_id: &str,
        amount: u64,
        neg_risk: bool,
    ) -> Result<String> {
        if amount == 0 {
            bail!("Cannot merge 0 pairs");
        }
        let venue = MergeVenue::from_neg_risk(neg_risk);

        info!(
            condition_id = %condition_id,
            amount = amount,
            venue = venue.label(),
            "Merging YES+NO pairs for USDC via SAFE relayer"
        );

        self.ensure_approval(venue).await?;

        let merge_data = encode_merge_positions(condition_id, amount)?;
        self.submit_safe_transaction(venue.merge_target(), &merge_data, "merge_positions")
            .await
    }

    async fn validate_relayer_auth(&self) -> Result<()> {
        let api_keys: Vec<RelayerApiKeyRecord> = self
            .get_json_retrying("/relayer/api/keys", true, "relayer auth validation")
            .await?;
        if api_keys.is_empty() {
            bail!("relayer auth succeeded but returned no API keys");
        }
        if !api_keys.iter().any(|record| {
            record
                .address
                .eq_ignore_ascii_case(&self.relayer_api_key_address)
        }) {
            bail!(
                "relayer auth owner mismatch: expected {} to be present in authenticated key list",
                self.relayer_api_key_address
            );
        }
        Ok(())
    }

    async fn ensure_safe_deployed(&self) -> Result<()> {
        let path = format!("/deployed?address={}", self.safe_address_hex);
        let response: RelayerDeployedResponse = self
            .get_json_retrying(&path, false, "SAFE deployment check")
            .await?;
        if !response.deployed {
            bail!(
                "relayer reports SAFE wallet {} is not deployed",
                self.safe_address_hex
            );
        }
        Ok(())
    }

    async fn fetch_safe_nonce(&self) -> Result<u64> {
        let path = format!("/nonce?address={}&type=SAFE", self.signer_address_hex);
        let response: RelayerNonceResponse = self
            .get_json_retrying(&path, false, "SAFE nonce request")
            .await?;
        parse_u64_decimal(&response.nonce).with_context(|| {
            format!(
                "SAFE nonce response contained invalid nonce {}",
                response.nonce
            )
        })
    }

    async fn ensure_approval(&self, venue: MergeVenue) -> Result<()> {
        if venue == MergeVenue::Standard {
            // Standard merges intentionally skip on-chain approval initialization here.
            // The current production Safe already has the CTF operator approved; revisit
            // this assumption if we rotate to a new Safe or need runtime verification.
            return Ok(());
        }

        let approval_checked = &self.neg_risk_approval_checked;
        let approval_lock = &self.neg_risk_approval_lock;
        if approval_checked.load(Ordering::Relaxed) {
            return Ok(());
        }

        let _guard = approval_lock.lock().await;
        if approval_checked.load(Ordering::Relaxed) {
            return Ok(());
        }

        info!(
            venue = venue.label(),
            "Initializing CTF approval via SAFE relayer"
        );
        let approval_data = encode_set_approval_for_all(venue.approval_operator())?;
        let tx_hash = self
            .submit_safe_transaction(CTF_ADDRESS, &approval_data, "set_approval_for_all")
            .await?;
        info!(
            tx_hash = %tx_hash,
            venue = venue.label(),
            "CTF approval transaction confirmed"
        );
        approval_checked.store(true, Ordering::Relaxed);
        Ok(())
    }

    async fn submit_safe_transaction(
        &self,
        to_hex: &str,
        data: &[u8],
        metadata: &str,
    ) -> Result<String> {
        let to = parse_exact_address(to_hex, "Invalid target address")?;
        let normalized_target = normalize_address_hex(to_hex, "Invalid target address")?;
        let encoded_data = format!("0x{}", hex::encode(data));

        let mut attempt = 0usize;
        loop {
            let nonce = self.fetch_safe_nonce().await?;
            let safe_tx_hash = self.compute_safe_tx_hash_for(data, &to, nonce);
            let signature = format!("0x{}", hex::encode(self.sign_hash(&safe_tx_hash)?));

            let request = RelayerSubmitRequest {
                from: self.signer_address_hex.clone(),
                to: normalized_target.clone(),
                proxy_wallet: self.safe_address_hex.clone(),
                data: encoded_data.clone(),
                nonce: nonce.to_string(),
                signature,
                signature_params: RelayerSignatureParams::default(),
                tx_type: "SAFE".to_string(),
            };

            let outcome = async {
                let response: RelayerTransactionStatus = self
                    .post_json_retrying("/submit", &request, true, "relayer transaction submit")
                    .await?;
                info!(
                    transaction_id = %response.transaction_id,
                    state = %response.state,
                    metadata = metadata,
                    "SAFE relayer transaction submitted"
                );

                if relayer_state_is_failure(&response.state) {
                    bail!(
                        "relayer {} failed immediately: state={} tx_id={} tx_hash={}",
                        metadata,
                        response.state,
                        response.transaction_id,
                        display_hash(&response.transaction_hash)
                    );
                }
                if relayer_state_is_success(&response.state)
                    && !response.transaction_hash.is_empty()
                {
                    return Ok(response.transaction_hash);
                }

                let terminal = self
                    .wait_for_terminal_transaction(&response.transaction_id, metadata)
                    .await?;
                if terminal.transaction_hash.is_empty() {
                    bail!(
                        "relayer {} reached {} without an onchain transaction hash",
                        metadata,
                        terminal.state
                    );
                }
                Ok(terminal.transaction_hash)
            }
            .await;

            match outcome {
                Ok(tx_hash) => return Ok(tx_hash),
                Err(error)
                    if attempt < SAFE_TX_TERMINAL_RETRY_ATTEMPTS
                        && safe_transaction_error_is_retryable_terminal_failure(&error) =>
                {
                    attempt += 1;
                    warn!(
                        metadata = %metadata,
                        attempt,
                        max_attempts = SAFE_TX_TERMINAL_RETRY_ATTEMPTS + 1,
                        error = %error,
                        "SAFE transaction hit terminal STATE_FAILED; retrying once with a fresh nonce"
                    );
                    sleep(SAFE_TX_TERMINAL_RETRY_DELAY).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn wait_for_terminal_transaction(
        &self,
        transaction_id: &str,
        metadata: &str,
    ) -> Result<RelayerTransactionStatus> {
        let mut last_state = None;
        for _ in 0..RELAYER_MAX_POLLS {
            sleep(RELAYER_POLL_INTERVAL).await;
            let transaction = match self.get_transaction_attempt(transaction_id).await {
                Ok(transaction) => transaction,
                Err(error) if error.retryable => {
                    warn!(
                        tx_id = transaction_id,
                        metadata = %metadata,
                        error = %error,
                        "Relayer transaction lookup timed out; continuing within existing poll budget"
                    );
                    continue;
                }
                Err(error) => return Err(anyhow!(error)),
            };
            if relayer_state_is_success(&transaction.state) {
                return Ok(transaction);
            }
            if relayer_state_is_failure(&transaction.state) {
                bail!(
                    "relayer {} failed: state={} tx_id={} tx_hash={}",
                    metadata,
                    transaction.state,
                    transaction.transaction_id,
                    display_hash(&transaction.transaction_hash)
                );
            }
            last_state = Some(transaction.state);
        }

        bail!(
            "relayer {} did not reach a terminal state within {} seconds (last_state={} tx_id={})",
            metadata,
            RELAYER_MAX_POLLS as u64 * RELAYER_POLL_INTERVAL.as_secs(),
            last_state.unwrap_or_else(|| "STATE_UNKNOWN".to_string()),
            transaction_id
        );
    }

    async fn get_transaction_attempt(
        &self,
        transaction_id: &str,
    ) -> std::result::Result<RelayerTransactionStatus, RelayerHttpError> {
        let path = format!("/transaction?id={transaction_id}");
        let transactions: Vec<RelayerTransactionStatus> = self
            .send_json_attempt(
                self.build_request(reqwest::Method::GET, &path, true),
                "relayer transaction lookup",
            )
            .await?;
        transactions.into_iter().next().ok_or_else(|| {
            RelayerHttpError::terminal(format!(
                "relayer returned no transaction records for {transaction_id}"
            ))
        })
    }

    async fn get_json<T>(&self, path: &str, authenticated: bool, context: &str) -> Result<T>
    where
        T: DeserializeOwned,
    {
        let request = self.build_request(reqwest::Method::GET, path, authenticated);
        self.send_json(request, context).await
    }

    async fn get_json_retrying<T>(
        &self,
        path: &str,
        authenticated: bool,
        context: &str,
    ) -> Result<T>
    where
        T: DeserializeOwned,
    {
        let mut last_retryable_error = None;
        for attempt in 1..=RELAYER_TIMEOUT_RETRY_ATTEMPTS {
            let request = self.build_request(reqwest::Method::GET, path, authenticated);
            match self.send_json_attempt(request, context).await {
                Ok(value) => return Ok(value),
                Err(error) if error.retryable && attempt < RELAYER_TIMEOUT_RETRY_ATTEMPTS => {
                    warn!(
                        attempt,
                        max_attempts = RELAYER_TIMEOUT_RETRY_ATTEMPTS,
                        context = %context,
                        error = %error,
                        "Relayer timeout during GET; retrying"
                    );
                    last_retryable_error = Some(error);
                    sleep(RELAYER_TIMEOUT_RETRY_DELAY).await;
                }
                Err(error) => return Err(anyhow!(error)),
            }
        }

        Err(anyhow!(
            last_retryable_error.expect("retryable relayer GET error should exist")
        ))
    }

    async fn post_json<T, B>(
        &self,
        path: &str,
        body: &B,
        authenticated: bool,
        context: &str,
    ) -> Result<T>
    where
        T: DeserializeOwned,
        B: Serialize + ?Sized,
    {
        let request = self
            .build_request(reqwest::Method::POST, path, authenticated)
            .json(body);
        self.send_json(request, context).await
    }

    async fn post_json_retrying<T, B>(
        &self,
        path: &str,
        body: &B,
        authenticated: bool,
        context: &str,
    ) -> Result<T>
    where
        T: DeserializeOwned,
        B: Serialize + ?Sized,
    {
        let mut last_retryable_error = None;
        for attempt in 1..=RELAYER_TIMEOUT_RETRY_ATTEMPTS {
            let request = self
                .build_request(reqwest::Method::POST, path, authenticated)
                .json(body);
            match self.send_json_attempt(request, context).await {
                Ok(value) => return Ok(value),
                Err(error) if error.retryable && attempt < RELAYER_TIMEOUT_RETRY_ATTEMPTS => {
                    warn!(
                        attempt,
                        max_attempts = RELAYER_TIMEOUT_RETRY_ATTEMPTS,
                        context = %context,
                        error = %error,
                        "Relayer timeout during submit; retrying exact SAFE payload"
                    );
                    last_retryable_error = Some(error);
                    sleep(RELAYER_TIMEOUT_RETRY_DELAY).await;
                }
                Err(error) => return Err(anyhow!(error)),
            }
        }

        Err(anyhow!(
            last_retryable_error.expect("retryable relayer POST error should exist")
        ))
    }

    fn build_request(
        &self,
        method: reqwest::Method,
        path: &str,
        authenticated: bool,
    ) -> reqwest::RequestBuilder {
        let url = format!("{}{}", self.relayer_base_url, path);
        let builder = self.http_client.request(method, url);
        if authenticated {
            builder
                .header("RELAYER_API_KEY", &self.relayer_api_key)
                .header("RELAYER_API_KEY_ADDRESS", &self.relayer_api_key_address)
        } else {
            builder
        }
    }

    async fn send_json<T>(&self, request: reqwest::RequestBuilder, context: &str) -> Result<T>
    where
        T: DeserializeOwned,
    {
        self.send_json_attempt(request, context)
            .await
            .map_err(|error| anyhow!(error))
    }

    async fn send_json_attempt<T>(
        &self,
        request: reqwest::RequestBuilder,
        context: &str,
    ) -> std::result::Result<T, RelayerHttpError>
    where
        T: DeserializeOwned,
    {
        let body_text = self.send_text_attempt(request, context).await?;
        serde_json::from_str(&body_text).map_err(|_| {
            RelayerHttpError::terminal(format!(
                "{context} returned non-JSON body {}",
                truncate_response_body(&body_text)
            ))
        })
    }

    async fn send_text_attempt(
        &self,
        request: reqwest::RequestBuilder,
        context: &str,
    ) -> std::result::Result<String, RelayerHttpError> {
        let response = request.send().await.map_err(|error| {
            let message = format!("{context} request failed: {error}");
            if relayer_request_error_is_retryable(&error) {
                RelayerHttpError::retryable(message)
            } else {
                RelayerHttpError::terminal(message)
            }
        })?;
        let status = response.status();
        let body_text = response.text().await.map_err(|error| {
            let message = format!("{context} response body read failed: {error}");
            if relayer_request_error_is_retryable(&error) {
                RelayerHttpError::retryable(message)
            } else {
                RelayerHttpError::terminal(message)
            }
        })?;
        if !status.is_success() {
            let message = format!(
                "{context} returned HTTP {} with body {}",
                status,
                truncate_response_body(&body_text)
            );
            if relayer_http_status_is_retryable(status) {
                return Err(RelayerHttpError::retryable(message));
            }
            return Err(RelayerHttpError::terminal(message));
        }
        Ok(body_text)
    }

    /// Compute Safe tx hash for an arbitrary target.
    fn compute_safe_tx_hash_for(&self, data: &[u8], to: &[u8; 20], nonce: u64) -> [u8; 32] {
        let typehash = Keccak256::digest(
            b"SafeTx(address to,uint256 value,bytes data,uint8 operation,uint256 safeTxGas,uint256 baseGas,uint256 gasPrice,address gasToken,address refundReceiver,uint256 nonce)"
        );
        let data_hash = Keccak256::digest(data);

        let mut struct_data = Vec::with_capacity(352);
        struct_data.extend_from_slice(&typehash);
        struct_data.extend_from_slice(&left_pad_address(to));
        struct_data.extend_from_slice(&[0u8; 32]); // value = 0
        struct_data.extend_from_slice(&data_hash);
        struct_data.extend_from_slice(&[0u8; 32]); // operation = 0
        struct_data.extend_from_slice(&[0u8; 32]); // safeTxGas = 0
        struct_data.extend_from_slice(&[0u8; 32]); // baseGas = 0
        struct_data.extend_from_slice(&[0u8; 32]); // gasPrice = 0
        struct_data.extend_from_slice(&[0u8; 32]); // gasToken = 0
        struct_data.extend_from_slice(&[0u8; 32]); // refundReceiver = 0
        struct_data.extend_from_slice(&u256_bytes(nonce as u128));

        let struct_hash = Keccak256::digest(&struct_data);
        let domain_sep = self.compute_safe_domain_separator();

        let mut msg = Vec::with_capacity(66);
        msg.push(0x19);
        msg.push(0x01);
        msg.extend_from_slice(&domain_sep);
        msg.extend_from_slice(&struct_hash);

        Keccak256::digest(&msg).into()
    }

    fn compute_safe_domain_separator(&self) -> [u8; 32] {
        let typehash =
            Keccak256::digest(b"EIP712Domain(uint256 chainId,address verifyingContract)");

        let mut data = Vec::with_capacity(96);
        data.extend_from_slice(&typehash);
        data.extend_from_slice(&u256_bytes(CHAIN_ID as u128));
        data.extend_from_slice(&left_pad_address(&self.safe_address));

        Keccak256::digest(&data).into()
    }

    fn sign_hash(&self, hash: &[u8; 32]) -> Result<Vec<u8>> {
        // SAFE relayer expects an EIP-191 signed SafeTx digest with v=31/32.
        let mut msg = b"\x19Ethereum Signed Message:\n32".to_vec();
        msg.extend_from_slice(hash);
        let digest = Keccak256::digest(&msg);
        let mut digest_bytes = [0u8; 32];
        digest_bytes.copy_from_slice(&digest);
        let (sig, recid) = self
            .signing_key
            .sign_prehash(&digest_bytes)
            .context("Failed to sign Safe tx hash")?;

        let mut signature = Vec::with_capacity(65);
        signature.extend_from_slice(&sig.to_bytes());
        // v = 31/32 to signal eth_sign in Safe (v_raw + 31).
        signature.push(recid.to_byte() + 31);

        Ok(signature)
    }
}

#[async_trait]
impl PairMerger for CtfMerger {
    async fn preflight_check(&self) -> Result<()> {
        self.validate_relayer_auth()
            .await
            .context("relayer auth check failed")?;
        self.ensure_safe_deployed()
            .await
            .context("SAFE deployment check failed")?;
        self.fetch_safe_nonce()
            .await
            .context("SAFE nonce check failed")?;
        Ok(())
    }

    async fn merge_positions(
        &self,
        condition_id: &str,
        amount: u64,
        neg_risk: bool,
    ) -> Result<String> {
        CtfMerger::merge_positions(self, condition_id, amount, neg_risk).await
    }
}

fn relayer_state_is_success(state: &str) -> bool {
    matches!(state, "STATE_MINED" | "STATE_CONFIRMED")
}

fn relayer_state_is_failure(state: &str) -> bool {
    matches!(state, "STATE_INVALID" | "STATE_FAILED")
}

fn display_hash(hash: &str) -> &str {
    if hash.is_empty() {
        "<pending>"
    } else {
        hash
    }
}

fn truncate_response_body(body: &str) -> String {
    const MAX_LEN: usize = 200;
    let compact = body.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.len() <= MAX_LEN {
        return compact;
    }
    format!("{}...", &compact[..MAX_LEN])
}

fn relayer_request_error_is_retryable(error: &reqwest::Error) -> bool {
    error.is_timeout()
}

fn relayer_http_status_is_retryable(status: reqwest::StatusCode) -> bool {
    matches!(status.as_u16(), 408 | 429 | 500 | 502 | 503 | 504)
}

fn safe_transaction_error_is_retryable_terminal_failure(error: &anyhow::Error) -> bool {
    let message = error.to_string();
    message.contains("relayer ")
        && message.contains("STATE_FAILED")
        && (message.contains(" failed:") || message.contains(" failed immediately:"))
}

/// ABI-encode mergePositions(address,bytes32,bytes32,uint256[],uint256)
fn encode_merge_positions(condition_id: &str, amount: u64) -> Result<Vec<u8>> {
    let selector =
        &Keccak256::digest(b"mergePositions(address,bytes32,bytes32,uint256[],uint256)")[..4];

    let usdc_addr = parse_address(USDC_ADDRESS);
    let cond_id = parse_bytes32(condition_id)?;
    let amount_units = (amount as u128) * 1_000_000;

    let mut data = Vec::with_capacity(4 + 32 * 7);
    data.extend_from_slice(selector);
    data.extend_from_slice(&left_pad_address(&usdc_addr));
    data.extend_from_slice(&[0u8; 32]); // parentCollectionId = 0
    data.extend_from_slice(&cond_id);
    data.extend_from_slice(&u256_bytes(160)); // offset to partition array
    data.extend_from_slice(&u256_bytes(amount_units));
    data.extend_from_slice(&u256_bytes(2)); // partition length
    data.extend_from_slice(&u256_bytes(1));
    data.extend_from_slice(&u256_bytes(2));

    Ok(data)
}

fn encode_set_approval_for_all(operator_hex: &str) -> Result<Vec<u8>> {
    let operator = parse_exact_address(operator_hex, "Invalid approval operator address")?;
    let selector = &Keccak256::digest(b"setApprovalForAll(address,bool)")[..4];

    let mut data = Vec::with_capacity(4 + 64);
    data.extend_from_slice(selector);
    data.extend_from_slice(&left_pad_address(&operator));
    data.extend_from_slice(&u256_bytes(1));
    Ok(data)
}

fn parse_address(hex_addr: &str) -> [u8; 20] {
    let clean = hex_addr.strip_prefix("0x").unwrap_or(hex_addr);
    let bytes = hex::decode(clean).unwrap_or_default();
    let mut addr = [0u8; 20];
    let start = 20usize.saturating_sub(bytes.len());
    addr[start..].copy_from_slice(&bytes[..bytes.len().min(20)]);
    addr
}

fn parse_exact_address(hex_addr: &str, context: &str) -> Result<[u8; 20]> {
    let clean = hex_addr.strip_prefix("0x").unwrap_or(hex_addr);
    let bytes = hex::decode(clean).with_context(|| context.to_string())?;
    if bytes.len() != 20 {
        bail!("{context}: expected 20 bytes, got {}", bytes.len());
    }
    let mut addr = [0u8; 20];
    addr.copy_from_slice(&bytes);
    Ok(addr)
}

fn normalize_address_hex(hex_addr: &str, context: &str) -> Result<String> {
    let bytes = parse_exact_address(hex_addr, context)?;
    Ok(format!("0x{}", hex::encode(bytes)))
}

fn parse_bytes32(hex_str: &str) -> Result<[u8; 32]> {
    let clean = hex_str.strip_prefix("0x").unwrap_or(hex_str);
    let bytes = hex::decode(clean).context("Invalid hex for bytes32")?;
    if bytes.len() > 32 {
        bail!("bytes32 value exceeds 32 bytes");
    }
    let mut out = [0u8; 32];
    let start = 32usize.saturating_sub(bytes.len());
    out[start..].copy_from_slice(&bytes);
    Ok(out)
}

fn left_pad_address(addr: &[u8; 20]) -> [u8; 32] {
    let mut out = [0u8; 32];
    out[12..].copy_from_slice(addr);
    out
}

fn u256_bytes(val: u128) -> [u8; 32] {
    let mut out = [0u8; 32];
    out[16..].copy_from_slice(&val.to_be_bytes());
    out
}

fn parse_u64_decimal(raw: &str) -> Result<u64> {
    raw.parse::<u64>()
        .with_context(|| format!("Invalid decimal u64 value {raw}"))
}

fn derive_address(key: &SigningKey) -> String {
    let pubkey = key.verifying_key().to_encoded_point(false);
    let hash = Keccak256::digest(&pubkey.as_bytes()[1..]);
    format!("0x{}", hex::encode(&hash[12..]))
}
