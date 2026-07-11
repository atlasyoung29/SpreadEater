use anyhow::Result;
use hmac::{Hmac, Mac};
use sha2::Sha256;

use super::ApiCredentials;

type HmacSha256 = Hmac<Sha256>;

/// Builds authenticated headers for Polymarket CLOB API requests (L2 auth).
///
/// All trading endpoints require these 5 headers:
/// - POLY_ADDRESS: Polygon signer address
/// - POLY_API_KEY: API key
/// - POLY_PASSPHRASE: Passphrase
/// - POLY_TIMESTAMP: Current UNIX timestamp
/// - POLY_SIGNATURE: HMAC-SHA256 signature of (timestamp + method + path + body)
pub struct RequestSigner {
    credentials: ApiCredentials,
}

impl RequestSigner {
    pub fn new(credentials: ApiCredentials) -> Self {
        Self { credentials }
    }

    /// Generate the HMAC-SHA256 signature for a request.
    ///
    /// Message format: "{timestamp}{method}{path}{body}"
    /// The secret is base64-decoded before use as HMAC key.
    pub fn sign(&self, timestamp: &str, method: &str, path: &str, body: &str) -> Result<String> {
        let secret_bytes = base64::Engine::decode(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD,
            self.credentials.secret.trim_end_matches('='),
        )?;

        let message = format!("{}{}{}{}", timestamp, method, path, body);

        let mut mac = HmacSha256::new_from_slice(&secret_bytes)
            .map_err(|e| anyhow::anyhow!("HMAC key error: {}", e))?;
        mac.update(message.as_bytes());

        let result = mac.finalize();
        let signature = base64::Engine::encode(
            &base64::engine::general_purpose::URL_SAFE,
            result.into_bytes(),
        );

        Ok(signature)
    }

    /// Build the full set of auth headers for a request.
    pub fn build_headers(
        &self,
        method: &str,
        path: &str,
        body: &str,
    ) -> Result<Vec<(String, String)>> {
        let timestamp = chrono::Utc::now().timestamp().to_string();
        let signature = self.sign(&timestamp, method, path, body)?;

        Ok(vec![
            ("POLY_ADDRESS".to_string(), self.credentials.address.clone()),
            ("POLY_API_KEY".to_string(), self.credentials.api_key.clone()),
            (
                "POLY_PASSPHRASE".to_string(),
                self.credentials.passphrase.clone(),
            ),
            ("POLY_TIMESTAMP".to_string(), timestamp.clone()),
            ("POLY_NONCE".to_string(), timestamp),
            ("POLY_SIGNATURE".to_string(), signature),
        ])
    }

    pub fn credentials(&self) -> &ApiCredentials {
        &self.credentials
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sign_deterministic() {
        let creds = ApiCredentials {
            api_key: "test-key".to_string(),
            // base64 of "test-secret-key!!"
            secret: base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                b"test-secret-key!!",
            ),
            passphrase: "test-pass".to_string(),
            address: "0x1234567890abcdef".to_string(),
            private_key: None,
            funder: None,
        };
        let signer = RequestSigner::new(creds);

        let sig1 = signer.sign("1000", "GET", "/orders", "").unwrap();
        let sig2 = signer.sign("1000", "GET", "/orders", "").unwrap();
        assert_eq!(sig1, sig2, "Same inputs must produce same signature");

        let sig3 = signer.sign("1001", "GET", "/orders", "").unwrap();
        assert_ne!(
            sig1, sig3,
            "Different timestamp must produce different signature"
        );
    }
}
