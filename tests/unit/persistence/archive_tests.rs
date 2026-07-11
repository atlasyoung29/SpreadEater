use filetime::{set_file_mtime, FileTime};
use rust_decimal_macros::dec;
use serde_json::Value;
use spreadeater::models::DecisionReport;
use spreadeater::persistence::FileArchive;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

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

fn write_archive_file(path: &Path, contents: &str) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(path, contents).unwrap();
}

fn set_mtime_days_ago(path: &Path, days_ago: u64) {
    let timestamp = SystemTime::now()
        .checked_sub(Duration::from_secs(days_ago * 24 * 60 * 60))
        .unwrap();
    set_file_mtime(path, FileTime::from_system_time(timestamp)).unwrap();
}

fn decision_archive_files(root: &Path) -> Vec<PathBuf> {
    std::fs::read_dir(root.join("decisions"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect()
}

#[tokio::test]
async fn new_creates_directory() {
    let tmp = tempfile::tempdir().unwrap();
    let archive_dir = tmp.path().join("archive");
    let result = FileArchive::new(archive_dir.to_str().unwrap()).await;
    assert!(result.is_ok(), "FileArchive::new should create directory");
    assert!(
        archive_dir.exists(),
        "Archive directory should exist on disk"
    );
}

#[tokio::test]
async fn save_and_load_session_roundtrip() {
    let tmp = tempfile::tempdir().unwrap();
    let archive = FileArchive::new(tmp.path().to_str().unwrap())
        .await
        .unwrap();

    let reports = vec![
        make_decision_report("cond-1"),
        make_decision_report("cond-2"),
    ];

    let session_path = archive.save_session(&reports).await.unwrap();
    let loaded = FileArchive::load_session(&session_path).await.unwrap();

    assert_eq!(loaded.len(), 2);
    assert_eq!(loaded[0].condition_id, "cond-1");
    assert_eq!(loaded[1].condition_id, "cond-2");
    assert_eq!(loaded[0].would_trade, true);
    assert_eq!(loaded[1].daily_reward_total, dec!(10));
}

#[tokio::test]
async fn list_sessions_empty() {
    let tmp = tempfile::tempdir().unwrap();
    let archive = FileArchive::new(tmp.path().to_str().unwrap())
        .await
        .unwrap();

    let sessions = archive.list_sessions().await.unwrap();
    assert!(sessions.is_empty(), "New archive should have no sessions");
}

#[tokio::test]
async fn list_sessions_after_save() {
    let tmp = tempfile::tempdir().unwrap();
    let archive = FileArchive::new(tmp.path().to_str().unwrap())
        .await
        .unwrap();

    let reports = vec![make_decision_report("cond-a")];

    // Save two sessions (need slight delay to get different filenames based on timestamp)
    let _p1 = archive.save_session(&reports).await.unwrap();
    // Small sleep to ensure different timestamp in filename
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    let _p2 = archive.save_session(&reports).await.unwrap();

    let sessions = archive.list_sessions().await.unwrap();
    assert_eq!(sessions.len(), 2, "Should have 2 saved sessions");
}

#[tokio::test]
async fn save_decision_report_appends_jsonl_lines_to_daily_file() {
    let tmp = tempfile::tempdir().unwrap();
    let archive = FileArchive::new(tmp.path().to_str().unwrap())
        .await
        .unwrap();

    archive
        .save_decision_report(&make_decision_report("cond-x"))
        .await
        .unwrap();
    archive
        .save_decision_report(&make_decision_report("cond-y"))
        .await
        .unwrap();

    let entries = decision_archive_files(tmp.path());
    assert_eq!(entries.len(), 1, "Should have exactly one daily JSONL file");
    assert_eq!(
        entries[0].extension().and_then(|ext| ext.to_str()),
        Some("jsonl")
    );

    let contents = std::fs::read_to_string(&entries[0]).unwrap();
    let lines: Vec<_> = contents.lines().collect();
    assert_eq!(
        lines.len(),
        2,
        "Expected one JSONL line per decision report"
    );

    let first: Value = serde_json::from_str(lines[0]).unwrap();
    let second: Value = serde_json::from_str(lines[1]).unwrap();
    assert_eq!(first["condition_id"], "cond-x");
    assert_eq!(second["condition_id"], "cond-y");
}

#[tokio::test]
async fn new_prunes_old_legacy_decision_json_files() {
    let tmp = tempfile::tempdir().unwrap();
    let old_file = tmp.path().join("decisions").join("old-decision.json");
    write_archive_file(&old_file, "{}");
    set_mtime_days_ago(&old_file, 31);

    FileArchive::new(tmp.path().to_str().unwrap())
        .await
        .unwrap();

    assert!(
        !old_file.exists(),
        "Legacy decision JSON older than retention should be pruned"
    );
}

#[tokio::test]
async fn new_prunes_old_decision_jsonl_files() {
    let tmp = tempfile::tempdir().unwrap();
    let old_file = tmp.path().join("decisions").join("20240101.jsonl");
    write_archive_file(&old_file, "{\"condition_id\":\"old\"}\n");
    set_mtime_days_ago(&old_file, 31);

    FileArchive::new(tmp.path().to_str().unwrap())
        .await
        .unwrap();

    assert!(
        !old_file.exists(),
        "Decision JSONL older than retention should be pruned"
    );
}

#[tokio::test]
async fn new_preserves_recent_decision_files() {
    let tmp = tempfile::tempdir().unwrap();
    let recent_json = tmp.path().join("decisions").join("recent.json");
    let recent_jsonl = tmp.path().join("decisions").join("recent.jsonl");
    write_archive_file(&recent_json, "{}");
    write_archive_file(&recent_jsonl, "{\"condition_id\":\"recent\"}\n");
    set_mtime_days_ago(&recent_json, 5);
    set_mtime_days_ago(&recent_jsonl, 5);

    FileArchive::new(tmp.path().to_str().unwrap())
        .await
        .unwrap();

    assert!(recent_json.exists(), "Recent decision JSON should remain");
    assert!(recent_jsonl.exists(), "Recent decision JSONL should remain");
}

#[tokio::test]
async fn new_pruning_does_not_delete_session_files() {
    let tmp = tempfile::tempdir().unwrap();
    let old_session = tmp.path().join("sessions").join("session_20240101.json");
    let old_decision = tmp.path().join("decisions").join("old-decision.json");
    write_archive_file(&old_session, "[]");
    write_archive_file(&old_decision, "{}");
    set_mtime_days_ago(&old_session, 31);
    set_mtime_days_ago(&old_decision, 31);

    FileArchive::new(tmp.path().to_str().unwrap())
        .await
        .unwrap();

    assert!(old_session.exists(), "Session files should be retained");
    assert!(
        !old_decision.exists(),
        "Old decision files should still be pruned"
    );
}
