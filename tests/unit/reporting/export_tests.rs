use rust_decimal_macros::dec;
use spreadeater::models::DecisionReport;
use spreadeater::reporting::export::export_session_csv;

fn make_decision_report(condition_id: &str) -> DecisionReport {
    DecisionReport {
        condition_id: condition_id.to_string(),
        market_slug: "test-market".to_string(),
        question: "Test?".to_string(),
        daily_reward_total: dec!(10),
        score_proxy: Some(dec!(0.05)),
        max_spread: dec!(0.04),
        effective_quote_size: dec!(5),
        candidate_quotes: vec![],
        reward_viability: None,
        would_trade: true,
        reasons: vec![],
    }
}

#[test]
fn csv_header_row() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("test.csv");

    export_session_csv(&[], &path).unwrap();

    let content = std::fs::read_to_string(&path).unwrap();
    let first_line = content.lines().next().unwrap();
    assert!(
        first_line.contains("Market Slug"),
        "Header should contain 'Market Slug'"
    );
    assert!(
        first_line.contains("Would Trade"),
        "Header should contain 'Would Trade'"
    );
    assert!(
        first_line.contains("Daily Reward"),
        "Header should contain 'Daily Reward'"
    );
}

#[test]
fn csv_with_reports() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("test.csv");

    let reports = vec![
        make_decision_report("cond-1"),
        make_decision_report("cond-2"),
    ];
    export_session_csv(&reports, &path).unwrap();

    let content = std::fs::read_to_string(&path).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    assert_eq!(
        lines.len(),
        3,
        "CSV should have 3 lines (1 header + 2 data rows)"
    );
}

#[test]
fn csv_empty_reports() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("test.csv");

    export_session_csv(&[], &path).unwrap();

    let content = std::fs::read_to_string(&path).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    assert_eq!(lines.len(), 1, "Empty reports should produce only header");
}
