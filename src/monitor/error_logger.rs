use chrono::Utc;
use serde::Serialize;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Mutex;
use tracing::warn;

#[derive(Serialize)]
struct ErrorEntry<'a> {
    timestamp: String,
    level: &'a str,
    message: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    condition_id: Option<&'a str>,
}

/// Always-on JSONL error logger. Appends one JSON line per error to
/// `./data/error_log.jsonl`. Independent of the monitor/observability stack.
pub struct ErrorLogger {
    path: PathBuf,
    file: Mutex<std::fs::File>,
}

impl ErrorLogger {
    pub fn new(dir: &str) -> Self {
        let path = PathBuf::from(dir).join("error_log.jsonl");
        if let Some(parent) = path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .expect("Failed to open error log file");
        Self {
            path,
            file: Mutex::new(file),
        }
    }

    pub fn log_error(&self, level: &str, message: &str, condition_id: Option<&str>) {
        let entry = ErrorEntry {
            timestamp: Utc::now().to_rfc3339(),
            level,
            message,
            condition_id,
        };
        let Ok(json) = serde_json::to_string(&entry) else {
            return;
        };
        if let Ok(mut f) = self.file.lock() {
            if writeln!(f, "{}", json).is_err() {
                warn!(path = %self.path.display(), "Failed to write to error log");
            }
        }
    }
}
