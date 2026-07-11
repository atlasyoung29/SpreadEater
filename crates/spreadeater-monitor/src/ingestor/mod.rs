use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, Result};
use tokio::fs;
use tokio::io::{AsyncBufReadExt, AsyncSeekExt, BufReader};
use tokio::time::{sleep, Duration};

use spreadeater_core::EventEnvelope;

use crate::projector::PostgresProjector;
use crate::store::{broadcast_event_updates, LiveBroadcaster};

const BATCH_SIZE: usize = 100;
const SCAN_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Clone)]
pub struct RebuildStats {
    pub events_processed: usize,
    pub files_processed: usize,
    pub duration_ms: u128,
    pub last_run_id: Option<String>,
}

#[derive(Clone)]
pub struct LogIngestor {
    projector: PostgresProjector,
    event_log_dir: PathBuf,
    broadcaster: Option<LiveBroadcaster>,
}

impl LogIngestor {
    pub fn new(
        projector: PostgresProjector,
        event_log_dir: PathBuf,
        broadcaster: Option<LiveBroadcaster>,
    ) -> Self {
        Self {
            projector,
            event_log_dir,
            broadcaster,
        }
    }

    pub async fn run(&self) -> Result<()> {
        loop {
            if let Err(error) = self.ingest_once().await {
                tracing::error!(?error, "monitor ingest scan failed");
            }
            sleep(SCAN_INTERVAL).await;
        }
    }

    pub async fn rebuild(&self) -> Result<RebuildStats> {
        self.projector.reset_projections().await?;

        let started = Instant::now();
        let files = discover_log_files(&self.event_log_dir).await?;
        let mut files_processed = 0usize;
        let mut events_processed = 0usize;
        let mut last_run_id = None;
        let mut last_mode = None;

        for file in files {
            let run_id = file
                .parent()
                .and_then(Path::file_name)
                .and_then(|value| value.to_str())
                .context("rebuild log file missing run directory")?
                .to_string();

            let (projected, mode) = self.replay_file(&file, &run_id).await?;
            files_processed += 1;
            events_processed += projected;
            last_run_id = Some(run_id);
            if let Some(found_mode) = mode {
                last_mode = Some(found_mode);
            }
        }

        if let Some(run_id) = &last_run_id {
            self.projector
                .emit_projection_rebuilt(
                    run_id,
                    last_mode.as_deref().unwrap_or("live"),
                    events_processed,
                    started.elapsed().as_millis(),
                )
                .await?;
        }

        Ok(RebuildStats {
            events_processed,
            files_processed,
            duration_ms: started.elapsed().as_millis(),
            last_run_id,
        })
    }

    pub async fn ingest_once(&self) -> Result<()> {
        let files = discover_log_files(&self.event_log_dir).await?;
        for file in files {
            let run_id = file
                .parent()
                .and_then(Path::file_name)
                .and_then(|value| value.to_str())
                .context("log file missing run directory")?
                .to_string();
            let _ = self.ingest_file(&file, &run_id).await?;
        }
        Ok(())
    }

    async fn replay_file(&self, path: &Path, run_id: &str) -> Result<(usize, Option<String>)> {
        self.ingest_from_offset(path, run_id, 0).await
    }

    async fn ingest_file(&self, path: &Path, run_id: &str) -> Result<(usize, Option<String>)> {
        let offset = self
            .projector
            .offset_for_file(&path.to_string_lossy())
            .await?;
        self.ingest_from_offset(path, run_id, offset).await
    }

    async fn ingest_from_offset(
        &self,
        path: &Path,
        run_id: &str,
        offset: i64,
    ) -> Result<(usize, Option<String>)> {
        if fs::metadata(path).await.is_err() {
            return Ok((0, None));
        }

        let file = fs::File::open(path).await?;
        let mut reader = BufReader::new(file);
        reader.seek(std::io::SeekFrom::Start(offset as u64)).await?;

        let mut current_offset = offset;
        let mut line = String::new();
        let mut batch = Vec::with_capacity(BATCH_SIZE);
        let mut processed = 0usize;
        let mut last_mode = None;

        loop {
            line.clear();
            let bytes = reader.read_line(&mut line).await?;
            if bytes == 0 {
                break;
            }

            current_offset += bytes as i64;
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }

            let event: EventEnvelope = serde_json::from_str(trimmed)
                .with_context(|| format!("parse event in {}", path.display()))?;
            last_mode = Some(event.mode.clone());
            batch.push(event);

            if batch.len() >= BATCH_SIZE {
                processed += self
                    .flush_batch(path, run_id, current_offset, &mut batch)
                    .await?;
            }
        }

        if !batch.is_empty() {
            processed += self
                .flush_batch(path, run_id, current_offset, &mut batch)
                .await?;
        }

        if processed == 0 && current_offset != offset {
            self.projector
                .store_offset(&path.to_string_lossy(), run_id, current_offset)
                .await?;
        }

        Ok((processed, last_mode))
    }

    async fn flush_batch(
        &self,
        path: &Path,
        run_id: &str,
        offset: i64,
        batch: &mut Vec<EventEnvelope>,
    ) -> Result<usize> {
        let events = std::mem::take(batch);
        let outcome = self.projector.project_batch(&events).await?;
        self.projector
            .store_offset(&path.to_string_lossy(), run_id, offset)
            .await?;

        if let Some(broadcaster) = &self.broadcaster {
            for event in &outcome.projected_events {
                broadcast_event_updates(self.projector.pool(), broadcaster, event).await?;
            }
        }

        Ok(outcome.inserted)
    }
}

async fn discover_log_files(root: &Path) -> Result<Vec<PathBuf>> {
    if fs::metadata(root).await.is_err() {
        return Ok(Vec::new());
    }

    let mut runs = fs::read_dir(root).await?;
    let mut files = Vec::new();

    while let Some(entry) = runs.next_entry().await? {
        let file_type = entry.file_type().await?;
        if !file_type.is_dir() {
            continue;
        }

        let events_file = entry.path().join("events.jsonl");
        if fs::metadata(&events_file).await.is_ok() {
            files.push(events_file);
        }
    }

    files.sort();
    Ok(files)
}
