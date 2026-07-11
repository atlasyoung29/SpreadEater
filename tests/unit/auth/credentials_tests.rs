use spreadeater::auth::ApiCredentials;

fn make_credentials() -> ApiCredentials {
    ApiCredentials {
        api_key: "test-api-key".to_string(),
        secret: "dGVzdC1zZWNyZXQ=".to_string(), // base64 of "test-secret"
        passphrase: "test-passphrase".to_string(),
        address: "0x1234567890abcdef1234567890abcdef12345678".to_string(),
        private_key: Some(
            "0000000000000000000000000000000000000000000000000000000000000001".to_string(),
        ),
        funder: None,
    }
}

#[test]
fn validate_passes_with_valid_credentials() {
    let creds = make_credentials();
    assert!(creds.validate().is_ok());
}

#[test]
fn validate_fails_empty_api_key() {
    let mut creds = make_credentials();
    creds.api_key = String::new();
    assert!(creds.validate().is_err());
}

#[test]
fn validate_fails_empty_secret() {
    let mut creds = make_credentials();
    creds.secret = String::new();
    assert!(creds.validate().is_err());
}

#[test]
fn credentials_fields_accessible() {
    let creds = make_credentials();
    assert_eq!(creds.api_key, "test-api-key");
    assert_eq!(creds.secret, "dGVzdC1zZWNyZXQ=");
    assert_eq!(creds.passphrase, "test-passphrase");
    assert_eq!(creds.address, "0x1234567890abcdef1234567890abcdef12345678");
    assert_eq!(
        creds.private_key.as_deref(),
        Some("0000000000000000000000000000000000000000000000000000000000000001")
    );
    assert!(creds.funder.is_none());
}

#[test]
fn private_key_optional_none() {
    let mut creds = make_credentials();
    creds.private_key = None;
    assert!(creds.validate().is_ok());
}

#[test]
fn funder_optional_none() {
    let creds = make_credentials();
    assert!(creds.funder.is_none());
    assert!(creds.validate().is_ok());
}
