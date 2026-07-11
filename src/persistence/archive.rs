use anyhow::{Context, Result};
use chrono::Utc;
use serde::Serialize;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::models::{DecisionReport, EnrichedDecisionReport};

const DECISION_RETENTION_DAYS: u64 = 7;
const SECONDS_PER_DAY: u64 = 24 * 60 * 60;

pub struct FileArchive {
    base_dir: PathBuf,
    decision_write_lock: Mutex<()>,
}

#[derive(Default)]
struct PruneStats {
    scanned_files: u64,
    deleted_files: u64,
    reclaimed_bytes: u64,
}

impl FileArchive {
    pub async fn new(base_dir: &str) -> Result<Self> {
        let path = PathBuf::from(base_dir);
        fs::create_dir_all(&path)
            .await
            .context("Failed to create archive directory")?;

        let archive = Self {
            base_dir: path.clone(),
            decision_write_lock: Mutex::new(()),
        };
        if let Err(error) = archive.prune_expired_decision_archives().await {
            warn!(
                error = %error,
                retention_days = DECISION_RETENTION_DAYS,
                "Decision archive retention prune failed"
            );
        }
        info!(path = %path.display(), "File archive initialized");
        Ok(archive)
    }

    /// Persist a decision report as a compact JSONL line in the current UTC day file.
    pub async fn save_decision_report(&self, report: &DecisionReport) -> Result<()> {
        let dir = self.base_dir.join("decisions");
        fs::create_dir_all(&dir).await?;

        let filename = format!("{}.jsonl", Utc::now().format("%Y%m%d"));
        let path = dir.join(filename);

        let json = serde_json::to_string(report).context("Failed to serialize decision report")?;
        let _guard = self.decision_write_lock.lock().await;
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .context("Failed to open decision archive file")?;
        file.write_all(json.as_bytes())
            .await
            .context("Failed to append decision report")?;
        file.write_all(b"\n")
            .await
            .context("Failed to append decision report newline")?;
        file.flush()
            .await
            .context("Failed to flush decision archive file")?;

        debug!(path = %path.display(), "Decision report appended");
        Ok(())
    }

    /// Persist a batch of decision reports as a single session file.
    pub async fn save_session(&self, reports: &[DecisionReport]) -> Result<PathBuf> {
        let dir = self.base_dir.join("sessions");
        fs::create_dir_all(&dir).await?;

        let filename = format!("session_{}.json", Utc::now().format("%Y%m%d_%H%M%S"));
        let path = dir.join(filename);

        let json = serde_json::to_string_pretty(reports).context("Failed to serialize session")?;
        fs::write(&path, &json)
            .await
            .context("Failed to write session file")?;

        info!(
            path = %path.display(),
            reports = reports.len(),
            "Session saved"
        );
        Ok(path)
    }

    /// Persist an enriched decision report (includes book snapshots for replay).
    pub async fn save_enriched_report(&self, enriched: &EnrichedDecisionReport) -> Result<()> {
        let dir = self.base_dir.join("enriched");
        fs::create_dir_all(&dir).await?;

        let filename = format!(
            "{}_{}.json",
            Utc::now().format("%Y%m%d_%H%M%S"),
            sanitize_filename(&enriched.report.condition_id)
        );
        let path = dir.join(filename);

        let json = serde_json::to_string_pretty(enriched)
            .context("Failed to serialize enriched report")?;
        fs::write(&path, json)
            .await
            .context("Failed to write enriched report")?;

        debug!(path = %path.display(), "Enriched report saved");
        Ok(())
    }

    /// Load a session file back into decision reports.
    pub async fn load_session(path: &std::path::Path) -> Result<Vec<DecisionReport>> {
        let data = fs::read_to_string(path)
            .await
            .context("Failed to read session file")?;
        let reports: Vec<DecisionReport> =
            serde_json::from_str(&data).context("Failed to parse session file")?;
        Ok(reports)
    }

    /// List all session files in the archive.
    pub async fn list_sessions(&self) -> Result<Vec<PathBuf>> {
        let dir = self.base_dir.join("sessions");
        if !dir.exists() {
            return Ok(Vec::new());
        }
        let mut entries = fs::read_dir(&dir).await?;
        let mut paths = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            let p = entry.path();
            if p.extension().map(|e| e == "json").unwrap_or(false) {
                paths.push(p);
            }
        }
        paths.sort();
        Ok(paths)
    }

    /// Save any serializable event with a category label.
    pub async fn save_event<T: Serialize>(&self, category: &str, event: &T) -> Result<()> {
        let dir = self.base_dir.join("events").join(category);
        fs::create_dir_all(&dir).await?;

        let filename = format!("{}.json", Utc::now().format("%Y%m%d_%H%M%S%.3f"));
        let path = dir.join(filename);

        let json = serde_json::to_string_pretty(event)?;
        fs::write(&path, json).await?;

        debug!(category = %category, path = %path.display(), "Event saved");
        Ok(())
    }

    async fn prune_expired_decision_archives(&self) -> Result<()> {
        let dir = self.base_dir.join("decisions");
        let mut stats = PruneStats::default();
        if !dir.exists() {
            info!(
                retention_days = DECISION_RETENTION_DAYS,
                scanned_files = stats.scanned_files,
                deleted_files = stats.deleted_files,
                reclaimed_bytes = stats.reclaimed_bytes,
                "Decision archive retention prune complete"
            );
            return Ok(());
        }

        let cutoff = SystemTime::now()
            .checked_sub(Duration::from_secs(
                DECISION_RETENTION_DAYS * SECONDS_PER_DAY,
            ))
            .unwrap_or(SystemTime::UNIX_EPOCH);

        let mut entries = fs::read_dir(&dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if !is_prunable_decision_archive(&path) {
                continue;
            }

            let metadata = entry.metadata().await?;
            if !metadata.is_file() {
                continue;
            }

            stats.scanned_files += 1;
            let modified = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);
            if modified >= cutoff {
                continue;
            }

            let bytes = metadata.len();
            fs::remove_file(&path).await?;
            stats.deleted_files += 1;
            stats.reclaimed_bytes += bytes;
        }

        info!(
            retention_days = DECISION_RETENTION_DAYS,
            scanned_files = stats.scanned_files,
            deleted_files = stats.deleted_files,
            reclaimed_bytes = stats.reclaimed_bytes,
            "Decision archive retention prune complete"
        );
        Ok(())
    }
}

fn sanitize_filename(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .take(64)
        .collect()
}

fn is_prunable_decision_archive(path: &std::path::Path) -> bool {
    matches!(
        path.extension().and_then(|ext| ext.to_str()),
        Some("json" | "jsonl")
    )
}
