use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use serde_json::{json, Value};
use sha3::{Digest, Keccak256};
use spreadeater::trading::ctf_merge::{CtfMerger, PairMerger};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

const TEST_PRIVATE_KEY: &str = "0000000000000000000000000000000000000000000000000000000000000001";
const TEST_SIGNER_ADDRESS: &str = "0x7e5f4552091a69125d5dfcb7b8c2659029395bdf";
const TEST_SAFE_ADDRESS: &str = "0x0000000000000000000000000000000000000001";
const TEST_RELAYER_API_KEY: &str = "test-relayer-key";
const CTF_ADDRESS_LOWER: &str = "0x4d97dcd97ec945f40cf65f87097ace5ea0476045";
const NEG_RISK_ADAPTER_LOWER: &str = "0xd91e80cf2e7be2e162c6513ced06f1dd0da35296";
const APPROVAL_SELECTOR: &str = "setApprovalForAll(address,bool)";
const MERGE_SELECTOR: &str = "mergePositions(address,bytes32,bytes32,uint256[],uint256)";

#[derive(Clone)]
struct MockRelayerState {
    valid_key: String,
    valid_address: String,
    deployed: bool,
    deployed_responses: Arc<Mutex<VecDeque<MockHttpResponse>>>,
    nonce_responses: Arc<Mutex<VecDeque<MockHttpResponse>>>,
    submit_responses: Arc<Mutex<VecDeque<MockHttpResponse>>>,
    transaction_responses: Arc<Mutex<HashMap<String, VecDeque<MockHttpResponse>>>>,
    submitted_requests: Arc<Mutex<Vec<Value>>>,
}

#[derive(Clone)]
struct MockHttpResponse {
    status: &'static str,
    body: String,
}

impl MockRelayerState {
    fn success() -> Self {
        Self {
            valid_key: TEST_RELAYER_API_KEY.to_string(),
            valid_address: TEST_SIGNER_ADDRESS.to_string(),
            deployed: true,
            deployed_responses: Arc::new(Mutex::new(VecDeque::new())),
            nonce_responses: Arc::new(Mutex::new(VecDeque::from([http_ok(json!({
                "nonce": "2"
            }))]))),
            submit_responses: Arc::new(Mutex::new(VecDeque::new())),
            transaction_responses: Arc::new(Mutex::new(HashMap::new())),
            submitted_requests: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

fn http_ok(body: Value) -> MockHttpResponse {
    MockHttpResponse {
        status: "200 OK",
        body: body.to_string(),
    }
}

fn http_error(status: &'static str, body: Value) -> MockHttpResponse {
    MockHttpResponse {
        status,
        body: body.to_string(),
    }
}

fn submit_response(transaction_id: &str, state: &str, tx_hash: &str) -> MockHttpResponse {
    http_ok(json!({
        "transactionID": transaction_id,
        "transactionHash": tx_hash,
        "state": state
    }))
}

fn transaction_response(transaction_id: &str, state: &str, tx_hash: &str) -> MockHttpResponse {
    http_ok(json!([
        {
            "transactionID": transaction_id,
            "transactionHash": tx_hash,
            "state": state
        }
    ]))
}

fn selector_hex(signature: &str) -> String {
    let hash = Keccak256::digest(signature.as_bytes());
    hex::encode(&hash[..4])
}

fn request_data(request: &Value) -> &str {
    request["data"].as_str().unwrap_or("")
}

fn approval_operator(data: &str) -> String {
    let clean = data.strip_prefix("0x").unwrap_or(data);
    let encoded_operator = &clean[8..72];
    format!("0x{}", &encoded_operator[24..64])
}

fn queued_response(
    queue: &mut VecDeque<MockHttpResponse>,
    fallback: impl FnOnce() -> MockHttpResponse,
) -> MockHttpResponse {
    match queue.len() {
        0 => fallback(),
        1 => queue.front().cloned().unwrap_or_else(fallback),
        _ => queue.pop_front().unwrap_or_else(fallback),
    }
}

async fn spawn_mock_relayer_server(
    state: MockRelayerState,
) -> std::io::Result<(String, tokio::task::JoinHandle<()>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    let task = tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                break;
            };
            let state = state.clone();
            tokio::spawn(async move {
                let Ok(request) = read_http_request(&mut socket).await else {
                    return;
                };
                let (method, path, headers, body) = parse_http_request(&request);
                let response =
                    route_mock_relayer_request(&state, &method, &path, &headers, &body).await;
                let wire = format!(
                    "HTTP/1.1 {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    response.status,
                    response.body.len(),
                    response.body
                );
                let _ = socket.write_all(wire.as_bytes()).await;
                let _ = socket.shutdown().await;
            });
        }
    });

    Ok((format!("http://{}", addr), task))
}

async fn read_http_request(socket: &mut tokio::net::TcpStream) -> std::io::Result<String> {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 2048];
    let mut headers_end = None;
    let mut content_len = 0usize;

    loop {
        let read = socket.read(&mut chunk).await?;
        if read == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..read]);
        if headers_end.is_none() {
            if let Some(pos) = find_bytes(&buffer, b"\r\n\r\n") {
                headers_end = Some(pos + 4);
                content_len = parse_content_length(&buffer[..pos + 4]);
                if buffer.len() >= pos + 4 + content_len {
                    break;
                }
            }
        } else if buffer.len() >= headers_end.unwrap_or_default() + content_len {
            break;
        }
    }

    Ok(String::from_utf8_lossy(&buffer).to_string())
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn parse_content_length(headers: &[u8]) -> usize {
    String::from_utf8_lossy(headers)
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            if name.eq_ignore_ascii_case("Content-Length") {
                return value.trim().parse::<usize>().ok();
            }
            None
        })
        .unwrap_or(0)
}

fn parse_http_request(request: &str) -> (String, String, HashMap<String, String>, String) {
    let (head, body) = request.split_once("\r\n\r\n").unwrap_or((request, ""));
    let mut lines = head.lines();
    let first_line = lines.next().unwrap_or_default();
    let mut parts = first_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let path = parts.next().unwrap_or_default().to_string();

    let headers = lines
        .filter_map(|line| {
            let (name, value) = line.split_once(':')?;
            Some((name.trim().to_ascii_lowercase(), value.trim().to_string()))
        })
        .collect::<HashMap<_, _>>();

    (method, path, headers, body.to_string())
}

async fn route_mock_relayer_request(
    state: &MockRelayerState,
    method: &str,
    path: &str,
    headers: &HashMap<String, String>,
    body: &str,
) -> MockHttpResponse {
    match (method, path) {
        ("GET", "/relayer/api/keys") => {
            if !auth_valid(state, headers) {
                return http_error(
                    "401 Unauthorized",
                    json!({ "error": "invalid authorization" }),
                );
            }
            http_ok(json!([{ "address": state.valid_address }]))
        }
        ("GET", _) if path.starts_with("/deployed?address=") => {
            let mut responses = state.deployed_responses.lock().await;
            queued_response(&mut responses, || {
                http_ok(json!({ "deployed": state.deployed }))
            })
        }
        ("GET", _) if path.starts_with("/nonce?") => {
            let mut responses = state.nonce_responses.lock().await;
            queued_response(&mut responses, || {
                http_ok(json!({
                    "nonce": "2"
                }))
            })
        }
        ("POST", "/submit") => {
            if !auth_valid(state, headers) {
                return http_error(
                    "401 Unauthorized",
                    json!({ "error": "invalid authorization" }),
                );
            }
            let parsed = serde_json::from_str::<Value>(body).unwrap_or_else(|_| json!({}));
            state.submitted_requests.lock().await.push(parsed);
            state
                .submit_responses
                .lock()
                .await
                .pop_front()
                .unwrap_or_else(|| {
                    http_error(
                        "500 Internal Server Error",
                        json!({ "error": "missing submit response" }),
                    )
                })
        }
        ("GET", _) if path.starts_with("/transaction?id=") => {
            if !auth_valid(state, headers) {
                return http_error(
                    "401 Unauthorized",
                    json!({ "error": "invalid authorization" }),
                );
            }
            let transaction_id = path.split('=').nth(1).unwrap_or_default();
            let mut responses = state.transaction_responses.lock().await;
            let Some(queue) = responses.get_mut(transaction_id) else {
                return http_error("404 Not Found", json!({ "error": "missing transaction" }));
            };
            match queue.len() {
                0 => http_error("404 Not Found", json!({ "error": "missing transaction" })),
                1 => queue.front().cloned().unwrap_or_else(|| {
                    http_error("404 Not Found", json!({ "error": "missing transaction" }))
                }),
                _ => queue.pop_front().unwrap_or_else(|| {
                    http_error("404 Not Found", json!({ "error": "missing transaction" }))
                }),
            }
        }
        _ => http_error("404 Not Found", json!({ "error": "missing route" })),
    }
}

fn auth_valid(state: &MockRelayerState, headers: &HashMap<String, String>) -> bool {
    headers.get("relayer_api_key") == Some(&state.valid_key)
        && headers.get("relayer_api_key_address") == Some(&state.valid_address)
}

fn test_merger(base_url: &str, api_key: &str) -> CtfMerger {
    CtfMerger::new_with_relayer_url(
        TEST_PRIVATE_KEY,
        TEST_SIGNER_ADDRESS,
        TEST_SAFE_ADDRESS,
        api_key,
        TEST_SIGNER_ADDRESS,
        base_url,
    )
    .expect("merger should initialize")
}

#[test]
fn new_without_rpc_dependency_initializes() {
    let merger = CtfMerger::new(
        TEST_PRIVATE_KEY,
        TEST_SIGNER_ADDRESS,
        TEST_SAFE_ADDRESS,
        TEST_RELAYER_API_KEY,
        TEST_SIGNER_ADDRESS,
    );
    assert!(
        merger.is_ok(),
        "Expected relayer-backed merger to initialize"
    );
}

#[tokio::test]
async fn preflight_succeeds_with_valid_auth_safe_and_nonce() {
    let state = MockRelayerState::success();
    let (base_url, server) = spawn_mock_relayer_server(state).await.unwrap();
    let merger = test_merger(&base_url, TEST_RELAYER_API_KEY);

    merger.preflight_check().await.unwrap();
    server.abort();
}

#[tokio::test]
async fn preflight_fails_when_auth_is_invalid() {
    let state = MockRelayerState::success();
    let (base_url, server) = spawn_mock_relayer_server(state).await.unwrap();
    let merger = test_merger(&base_url, "wrong-key");

    let error = merger.preflight_check().await.unwrap_err();
    assert!(error.to_string().contains("relayer auth check failed"));
    server.abort();
}

#[tokio::test]
async fn preflight_fails_when_safe_is_not_deployed() {
    let mut state = MockRelayerState::success();
    state.deployed = false;
    let (base_url, server) = spawn_mock_relayer_server(state).await.unwrap();
    let merger = test_merger(&base_url, TEST_RELAYER_API_KEY);

    let error = merger.preflight_check().await.unwrap_err();
    assert!(error.to_string().contains("SAFE deployment check failed"));
    server.abort();
}

#[tokio::test]
async fn preflight_fails_when_nonce_lookup_errors() {
    let state = MockRelayerState::success();
    *state.nonce_responses.lock().await = VecDeque::from([http_error(
        "500 Internal Server Error",
        json!({ "error": "nonce unavailable" }),
    )]);
    let (base_url, server) = spawn_mock_relayer_server(state).await.unwrap();
    let merger = test_merger(&base_url, TEST_RELAYER_API_KEY);

    let error = merger.preflight_check().await.unwrap_err();
    assert!(error.to_string().contains("SAFE nonce check failed"));
    server.abort();
}

#[tokio::test]
async fn preflight_retries_transient_deployment_failure_and_succeeds() {
    let state = MockRelayerState::success();
    *state.deployed_responses.lock().await = VecDeque::from([
        http_error("504 Gateway Timeout", json!({ "error": "timeout" })),
        http_ok(json!({ "deployed": true })),
    ]);
    let (base_url, server) = spawn_mock_relayer_server(state).await.unwrap();
    let merger = test_merger(&base_url, TEST_RELAYER_API_KEY);

    merger.preflight_check().await.unwrap();
    server.abort();
}

#[tokio::test]
async fn preflight_retries_transient_nonce_failure_and_succeeds() {
    let state = MockRelayerState::success();
    *state.nonce_responses.lock().await = VecDeque::from([
        http_error("504 Gateway Timeout", json!({ "error": "timeout" })),
        http_ok(json!({ "nonce": "2" })),
    ]);
    let (base_url, server) = spawn_mock_relayer_server(state).await.unwrap();
    let merger = test_merger(&base_url, TEST_RELAYER_API_KEY);

    merger.preflight_check().await.unwrap();
    server.abort();
}

#[tokio::test]
async fn preflight_retries_transient_nonce_internal_error_and_succeeds() {
    let state = MockRelayerState::success();
    *state.nonce_responses.lock().await = VecDeque::from([
        http_error(
            "500 Internal Server Error",
            json!({ "error": "internal server error" }),
        ),
        http_ok(json!({ "nonce": "2" })),
    ]);
    let (base_url, server) = spawn_mock_relayer_server(state).await.unwrap();
    let merger = test_merger(&base_url, TEST_RELAYER_API_KEY);

    merger.preflight_check().await.unwrap();
    server.abort();
}

#[tokio::test]
async fn standard_merge_submits_only_merge_and_returns_tx_hash() {
    let state = MockRelayerState::success();
    state
        .submit_responses
        .lock()
        .await
        .push_back(submit_response("merge-1", "STATE_NEW", ""));
    state.transaction_responses.lock().await.insert(
        "merge-1".to_string(),
        VecDeque::from([transaction_response(
            "merge-1",
            "STATE_CONFIRMED",
            "0xmerge",
        )]),
    );

    let submitted_requests = state.submitted_requests.clone();
    let (base_url, server) = spawn_mock_relayer_server(state).await.unwrap();
    let merger = test_merger(&base_url, TEST_RELAYER_API_KEY);

    let tx_hash = merger.merge_positions("0x01", 5, false).await.unwrap();
    assert_eq!(tx_hash, "0xmerge");

    let requests = submitted_requests.lock().await.clone();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0]["type"], "SAFE");
    assert_eq!(requests[0]["from"], TEST_SIGNER_ADDRESS);
    assert_eq!(requests[0]["proxyWallet"], TEST_SAFE_ADDRESS);
    assert_eq!(requests[0]["to"], CTF_ADDRESS_LOWER);
    let merge_selector = format!("0x{}", selector_hex(MERGE_SELECTOR));
    assert!(request_data(&requests[0]).starts_with(&merge_selector));
    assert_eq!(requests[0]["to"], CTF_ADDRESS_LOWER);
    server.abort();
}

#[tokio::test]
async fn standard_merge_never_submits_approval_transactions() {
    let state = MockRelayerState::success();
    state.submit_responses.lock().await.extend([
        submit_response("merge-1", "STATE_NEW", ""),
        submit_response("merge-2", "STATE_NEW", ""),
    ]);
    state.transaction_responses.lock().await.insert(
        "merge-1".to_string(),
        VecDeque::from([transaction_response(
            "merge-1",
            "STATE_CONFIRMED",
            "0xmerge1",
        )]),
    );
    state.transaction_responses.lock().await.insert(
        "merge-2".to_string(),
        VecDeque::from([transaction_response(
            "merge-2",
            "STATE_CONFIRMED",
            "0xmerge2",
        )]),
    );

    let submitted_requests = state.submitted_requests.clone();
    let (base_url, server) = spawn_mock_relayer_server(state).await.unwrap();
    let merger = test_merger(&base_url, TEST_RELAYER_API_KEY);

    merger.merge_positions("0x01", 1, false).await.unwrap();
    merger.merge_positions("0x02", 1, false).await.unwrap();

    let requests = submitted_requests.lock().await.clone();
    let approval_selector = format!("0x{}", selector_hex(APPROVAL_SELECTOR));
    let approval_calls = requests
        .iter()
        .filter(|request| request_data(request).starts_with(&approval_selector))
        .count();
    assert_eq!(requests.len(), 2);
    assert_eq!(approval_calls, 0);
    server.abort();
}

#[tokio::test]
async fn neg_risk_merge_routes_approval_and_merge_to_adapter_targets() {
    let state = MockRelayerState::success();
    state.submit_responses.lock().await.extend([
        submit_response("approval-1", "STATE_NEW", ""),
        submit_response("merge-1", "STATE_NEW", ""),
    ]);
    state.transaction_responses.lock().await.insert(
        "approval-1".to_string(),
        VecDeque::from([transaction_response(
            "approval-1",
            "STATE_CONFIRMED",
            "0xapproval",
        )]),
    );
    state.transaction_responses.lock().await.insert(
        "merge-1".to_string(),
        VecDeque::from([transaction_response(
            "merge-1",
            "STATE_CONFIRMED",
            "0xmerge",
        )]),
    );

    let submitted_requests = state.submitted_requests.clone();
    let (base_url, server) = spawn_mock_relayer_server(state).await.unwrap();
    let merger = test_merger(&base_url, TEST_RELAYER_API_KEY);

    let tx_hash = merger.merge_positions("0x01", 5, true).await.unwrap();
    assert_eq!(tx_hash, "0xmerge");

    let requests = submitted_requests.lock().await.clone();
    let approval_selector = format!("0x{}", selector_hex(APPROVAL_SELECTOR));
    let merge_selector = format!("0x{}", selector_hex(MERGE_SELECTOR));
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0]["to"], CTF_ADDRESS_LOWER);
    assert!(request_data(&requests[0]).starts_with(&approval_selector));
    assert_eq!(
        approval_operator(request_data(&requests[0])),
        NEG_RISK_ADAPTER_LOWER
    );
    assert_eq!(requests[1]["to"], NEG_RISK_ADAPTER_LOWER);
    assert!(request_data(&requests[1]).starts_with(&merge_selector));
    server.abort();
}

#[tokio::test]
async fn neg_risk_approval_is_independent_of_standard_merge_path() {
    let state = MockRelayerState::success();
    state.submit_responses.lock().await.extend([
        submit_response("merge-standard", "STATE_NEW", ""),
        submit_response("approval-neg-risk", "STATE_NEW", ""),
        submit_response("merge-neg-risk", "STATE_NEW", ""),
    ]);
    state.transaction_responses.lock().await.insert(
        "merge-standard".to_string(),
        VecDeque::from([transaction_response(
            "merge-standard",
            "STATE_CONFIRMED",
            "0xmerge-standard",
        )]),
    );
    state.transaction_responses.lock().await.insert(
        "approval-neg-risk".to_string(),
        VecDeque::from([transaction_response(
            "approval-neg-risk",
            "STATE_CONFIRMED",
            "0xapproval-neg-risk",
        )]),
    );
    state.transaction_responses.lock().await.insert(
        "merge-neg-risk".to_string(),
        VecDeque::from([transaction_response(
            "merge-neg-risk",
            "STATE_CONFIRMED",
            "0xmerge-neg-risk",
        )]),
    );

    let submitted_requests = state.submitted_requests.clone();
    let (base_url, server) = spawn_mock_relayer_server(state).await.unwrap();
    let merger = test_merger(&base_url, TEST_RELAYER_API_KEY);

    merger.merge_positions("0x01", 1, false).await.unwrap();
    merger.merge_positions("0x02", 1, true).await.unwrap();

    let requests = submitted_requests.lock().await.clone();
    let approval_selector = format!("0x{}", selector_hex(APPROVAL_SELECTOR));
    let approval_requests = requests
        .iter()
        .filter(|request| request_data(request).starts_with(&approval_selector))
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(requests.len(), 3);
    assert_eq!(approval_requests.len(), 1);
    assert_eq!(
        approval_operator(request_data(&approval_requests[0])),
        NEG_RISK_ADAPTER_LOWER
    );
    server.abort();
}

#[tokio::test]
async fn merge_retries_transient_transaction_lookup_failure_and_still_confirms() {
    let state = MockRelayerState::success();
    state
        .submit_responses
        .lock()
        .await
        .push_back(submit_response("merge-1", "STATE_NEW", ""));
    state.transaction_responses.lock().await.insert(
        "merge-1".to_string(),
        VecDeque::from([
            http_error("504 Gateway Timeout", json!({ "error": "timeout" })),
            transaction_response("merge-1", "STATE_CONFIRMED", "0xmerge"),
        ]),
    );

    let (base_url, server) = spawn_mock_relayer_server(state).await.unwrap();
    let merger = test_merger(&base_url, TEST_RELAYER_API_KEY);

    let tx_hash = merger.merge_positions("0x01", 5, false).await.unwrap();
    assert_eq!(tx_hash, "0xmerge");
    server.abort();
}

#[tokio::test]
async fn merge_retries_transient_submit_timeout_and_still_confirms() {
    let state = MockRelayerState::success();
    state.submit_responses.lock().await.extend([
        http_error("504 Gateway Timeout", json!({ "error": "timeout" })),
        submit_response("merge-1", "STATE_NEW", ""),
    ]);
    state.transaction_responses.lock().await.insert(
        "merge-1".to_string(),
        VecDeque::from([transaction_response(
            "merge-1",
            "STATE_CONFIRMED",
            "0xmerge",
        )]),
    );

    let (base_url, server) = spawn_mock_relayer_server(state).await.unwrap();
    let merger = test_merger(&base_url, TEST_RELAYER_API_KEY);

    let tx_hash = merger.merge_positions("0x01", 5, false).await.unwrap();
    assert_eq!(tx_hash, "0xmerge");
    server.abort();
}

#[tokio::test]
async fn merge_retries_terminal_state_failed_once_and_then_succeeds() {
    let state = MockRelayerState::success();
    state.submit_responses.lock().await.extend([
        submit_response("merge-1", "STATE_NEW", ""),
        submit_response("merge-2", "STATE_NEW", ""),
    ]);
    state.transaction_responses.lock().await.insert(
        "merge-1".to_string(),
        VecDeque::from([transaction_response("merge-1", "STATE_FAILED", "")]),
    );
    state.transaction_responses.lock().await.insert(
        "merge-2".to_string(),
        VecDeque::from([transaction_response(
            "merge-2",
            "STATE_CONFIRMED",
            "0xmerge",
        )]),
    );

    let submitted_requests = state.submitted_requests.clone();
    let (base_url, server) = spawn_mock_relayer_server(state).await.unwrap();
    let merger = test_merger(&base_url, TEST_RELAYER_API_KEY);

    let tx_hash = merger.merge_positions("0x01", 5, false).await.unwrap();
    assert_eq!(tx_hash, "0xmerge");
    assert_eq!(submitted_requests.lock().await.len(), 2);
    server.abort();
}

#[tokio::test]
async fn neg_risk_approval_retries_terminal_state_failed_once_and_then_succeeds() {
    let state = MockRelayerState::success();
    state.submit_responses.lock().await.extend([
        submit_response("approval-1", "STATE_NEW", ""),
        submit_response("approval-2", "STATE_NEW", ""),
        submit_response("merge-1", "STATE_NEW", ""),
    ]);
    state.transaction_responses.lock().await.insert(
        "approval-1".to_string(),
        VecDeque::from([transaction_response("approval-1", "STATE_FAILED", "")]),
    );
    state.transaction_responses.lock().await.insert(
        "approval-2".to_string(),
        VecDeque::from([transaction_response(
            "approval-2",
            "STATE_CONFIRMED",
            "0xapproval",
        )]),
    );
    state.transaction_responses.lock().await.insert(
        "merge-1".to_string(),
        VecDeque::from([transaction_response(
            "merge-1",
            "STATE_CONFIRMED",
            "0xmerge",
        )]),
    );

    let submitted_requests = state.submitted_requests.clone();
    let (base_url, server) = spawn_mock_relayer_server(state).await.unwrap();
    let merger = test_merger(&base_url, TEST_RELAYER_API_KEY);

    let tx_hash = merger.merge_positions("0x01", 5, true).await.unwrap();
    assert_eq!(tx_hash, "0xmerge");
    assert_eq!(submitted_requests.lock().await.len(), 3);
    server.abort();
}

#[tokio::test]
async fn merge_propagates_terminal_relayer_failure() {
    let state = MockRelayerState::success();
    state.submit_responses.lock().await.extend([
        submit_response("merge-1", "STATE_NEW", ""),
        submit_response("merge-2", "STATE_NEW", ""),
        submit_response("merge-3", "STATE_NEW", ""),
    ]);
    state.transaction_responses.lock().await.insert(
        "merge-1".to_string(),
        VecDeque::from([transaction_response("merge-1", "STATE_FAILED", "")]),
    );
    state.transaction_responses.lock().await.insert(
        "merge-2".to_string(),
        VecDeque::from([transaction_response("merge-2", "STATE_FAILED", "")]),
    );
    state.transaction_responses.lock().await.insert(
        "merge-3".to_string(),
        VecDeque::from([transaction_response("merge-3", "STATE_FAILED", "")]),
    );

    let (base_url, server) = spawn_mock_relayer_server(state).await.unwrap();
    let merger = test_merger(&base_url, TEST_RELAYER_API_KEY);

    let error = merger.merge_positions("0x01", 5, false).await.unwrap_err();
    assert!(error.to_string().contains("STATE_FAILED"));
    server.abort();
}

#[tokio::test]
async fn neg_risk_merge_propagates_terminal_relayer_failure() {
    let state = MockRelayerState::success();
    state.submit_responses.lock().await.extend([
        submit_response("approval-1", "STATE_NEW", ""),
        submit_response("merge-1", "STATE_NEW", ""),
        submit_response("merge-2", "STATE_NEW", ""),
        submit_response("merge-3", "STATE_NEW", ""),
    ]);
    state.transaction_responses.lock().await.insert(
        "approval-1".to_string(),
        VecDeque::from([transaction_response(
            "approval-1",
            "STATE_CONFIRMED",
            "0xapproval",
        )]),
    );
    state.transaction_responses.lock().await.insert(
        "merge-1".to_string(),
        VecDeque::from([transaction_response("merge-1", "STATE_FAILED", "")]),
    );
    state.transaction_responses.lock().await.insert(
        "merge-2".to_string(),
        VecDeque::from([transaction_response("merge-2", "STATE_FAILED", "")]),
    );
    state.transaction_responses.lock().await.insert(
        "merge-3".to_string(),
        VecDeque::from([transaction_response("merge-3", "STATE_FAILED", "")]),
    );

    let (base_url, server) = spawn_mock_relayer_server(state).await.unwrap();
    let merger = test_merger(&base_url, TEST_RELAYER_API_KEY);

    let error = merger.merge_positions("0x01", 5, true).await.unwrap_err();
    assert!(error.to_string().contains("STATE_FAILED"));
    server.abort();
}

#[tokio::test]
async fn merge_surfaces_immediate_submit_failure() {
    let state = MockRelayerState::success();
    state.submit_responses.lock().await.extend([
        submit_response("merge-1", "STATE_FAILED", ""),
        submit_response("merge-2", "STATE_FAILED", ""),
        submit_response("merge-3", "STATE_FAILED", ""),
    ]);

    let (base_url, server) = spawn_mock_relayer_server(state).await.unwrap();
    let merger = test_merger(&base_url, TEST_RELAYER_API_KEY);

    let error = merger.merge_positions("0x01", 5, false).await.unwrap_err();
    assert!(error.to_string().contains("failed immediately"));
    server.abort();
}
