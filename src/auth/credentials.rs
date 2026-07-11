use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tracing::info;

/// Polymarket CLOB API credentials (L2 auth).
/// These are derived from an L1 wallet signature and used for all trading operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiCredentials {
    pub api_key: String,
    pub secret: String,
    pub passphrase: String,
    pub address: String,
    /// Wallet private key (hex). Required for live order signing, optional for dry-run.
    #[serde(skip_serializing, default)]
    pub private_key: Option<String>,
    /// Funder address (Gnosis Safe / proxy wallet that holds funds).
    /// If different from `address` (EOA), orders use signatureType=2.
    #[serde(default)]
    pub funder: Option<String>,
}

impl ApiCredentials {
    /// Load credentials from environment variables.
    pub fn from_env() -> Result<Self> {
        let api_key = std::env::var("POLY_API_KEY").context("POLY_API_KEY not set")?;
        let secret = std::env::var("POLY_SECRET").context("POLY_SECRET not set")?;
        let passphrase = std::env::var("POLY_PASSPHRASE").context("POLY_PASSPHRASE not set")?;
        let address = std::env::var("POLY_ADDRESS").context("POLY_ADDRESS not set")?;

        let private_key = std::env::var("POLY_PRIVATE_KEY").ok();
        let funder = std::env::var("POLY_FUNDER").ok();

        info!(
            address = %address,
            funder = ?funder,
            has_private_key = private_key.is_some(),
            "API credentials loaded from environment"
        );

        Ok(Self {
            api_key,
            secret,
            passphrase,
            address,
            private_key,
            funder,
        })
    }

    /// Check if credentials look valid (non-empty).
    pub fn validate(&self) -> Result<()> {
        if self.api_key.is_empty() {
            anyhow::bail!("API key is empty");
        }
        if self.secret.is_empty() {
            anyhow::bail!("API secret is empty");
        }
        if self.passphrase.is_empty() {
            anyhow::bail!("Passphrase is empty");
        }
        if self.address.is_empty() || !self.address.starts_with("0x") {
            anyhow::bail!("Address is invalid: must start with 0x");
        }
        Ok(())
    }
}
