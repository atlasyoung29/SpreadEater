use spreadeater::auth::{ApiCredentials, RequestSigner};

fn make_signer() -> RequestSigner {
    let creds = ApiCredentials {
        api_key: "test-key".to_string(),
        // base64 of "test-secret-key!!" (standard encoding)
        secret: "dGVzdC1zZWNyZXQta2V5ISE=".to_string(),
        passphrase: "test-pass".to_string(),
        address: "0x1234567890abcdef1234567890abcdef12345678".to_string(),
        private_key: None,
        funder: None,
    };
    RequestSigner::new(creds)
}

#[test]
fn sign_produces_base64_string() {
    let signer = make_signer();
    let sig = signer.sign("1000", "GET", "/orders", "").unwrap();
    // Result should be non-empty and contain only base64-safe characters
    assert!(!sig.is_empty(), "Signature should be non-empty");
    // URL-safe base64 uses [A-Za-z0-9+/=_-]
    assert!(
        sig.chars().all(|c| c.is_ascii_alphanumeric()
            || c == '+'
            || c == '/'
            || c == '='
            || c == '-'
            || c == '_'),
        "Signature should be valid base64: {}",
        sig
    );
}

#[test]
fn sign_empty_body() {
    let signer = make_signer();
    let result = signer.sign("1000", "POST", "/order", "");
    assert!(result.is_ok(), "Signing with empty body should not panic");
}

#[test]
fn build_headers_contains_required_keys() {
    let signer = make_signer();
    let headers = signer.build_headers("GET", "/orders", "").unwrap();

    let keys: Vec<&str> = headers.iter().map(|(k, _)| k.as_str()).collect();

    assert!(
        keys.contains(&"POLY_ADDRESS"),
        "Missing POLY_ADDRESS header"
    );
    assert!(
        keys.contains(&"POLY_SIGNATURE"),
        "Missing POLY_SIGNATURE header"
    );
    assert!(
        keys.contains(&"POLY_TIMESTAMP"),
        "Missing POLY_TIMESTAMP header"
    );
    assert!(
        keys.contains(&"POLY_API_KEY"),
        "Missing POLY_API_KEY header"
    );
}

#[test]
fn sign_deterministic() {
    let signer = make_signer();
    let sig1 = signer
        .sign("12345", "POST", "/order", r#"{"size":5}"#)
        .unwrap();
    let sig2 = signer
        .sign("12345", "POST", "/order", r#"{"size":5}"#)
        .unwrap();
    assert_eq!(sig1, sig2, "Same inputs must produce same signature");
}
