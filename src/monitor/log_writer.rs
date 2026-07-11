use async_trait::async_trait;
use spreadeater_core::{EventEnvelope, EventWriter, WriterError, WriterHealth};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::Mutex;

pub struct JsonlFileWriter {
    file: Mutex<BufWriter<File>>,
    health_ok: AtomicBool,
    path: PathBuf,
}

impl JsonlFileWriter {
    pub async fn new<P: AsRef<Path>>(
        event_log_dir: P,
        run_id: &str,
    ) -> Result<Self, std::io::Error> {
        let run_dir = event_log_dir.as_ref().join(run_id);
        fs::create_dir_all(&run_dir).await?;

        let path = run_dir.join("events.jsonl");
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await?;

        Ok(Self {
            file: Mutex::new(BufWriter::new(file)),
            health_ok: AtomicBool::new(true),
            path,
        })
    }

    fn set_health(&self, healthy: bool) {
        self.health_ok.store(healthy, Ordering::SeqCst);
    }

    #[cfg(test)]
    fn path(&self) -> &Path {
        &self.path
    }
}

#[async_trait]
impl EventWriter for JsonlFileWriter {
    async fn write_batch(&self, events: &[EventEnvelope]) -> Result<usize, WriterError> {
        let mut file = self.file.lock().await;

        for event in events {
            let line = serde_json::to_string(event)
                .map_err(|err| WriterError::Serialization(err.to_string()))?;
            if let Err(err) = file.write_all(line.as_bytes()).await {
                self.set_health(false);
                return Err(err.into());
            }
            if let Err(err) = file.write_all(b"\n").await {
                self.set_health(false);
                return Err(err.into());
            }
        }

        self.set_health(true);
        Ok(events.len())
    }

    async fn flush(&self) -> Result<(), WriterError> {
        let mut file = self.file.lock().await;
        file.flush().await.map_err(WriterError::from)?;
        file.get_mut()
            .sync_data()
            .await
            .map_err(WriterError::from)?;
        self.set_health(true);
        Ok(())
    }

    fn health(&self) -> WriterHealth {
        if self.health_ok.load(Ordering::SeqCst) {
            WriterHealth::Healthy
        } else {
            WriterHealth::Degraded
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use spreadeater_core::{EventEnvelope, EventType, Priority};
    use uuid::Uuid;

    fn temp_root() -> PathBuf {
        std::env::temp_dir().join(format!("spreadeater-jsonl-{}", Uuid::new_v4()))
    }

    fn sample_event(index: u64) -> EventEnvelope {
        EventEnvelope::new(
            EventType::DecisionEvaluated,
            Priority::Normal,
            "run_writer_test".to_string(),
            "test".to_string(),
            "dry-run".to_string(),
            serde_json::json!({ "index": index }),
        )
    }

    #[tokio::test]
    async fn creates_directory_and_appends_valid_jsonl() {
        let root = temp_root();
        let writer = JsonlFileWriter::new(&root, "run_123").await.unwrap();

        writer
            .write_batch(&[sample_event(1), sample_event(2)])
            .await
            .unwrap();
        writer.flush().await.unwrap();
        writer.write_batch(&[sample_event(3)]).await.unwrap();
        writer.flush().await.unwrap();

        let contents = fs::read_to_string(writer.path()).await.unwrap();
        let lines: Vec<_> = contents.lines().collect();
        assert_eq!(lines.len(), 3);

        let events: Vec<EventEnvelope> = lines
            .iter()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].run_id, "run_writer_test");
        assert_eq!(events[2].payload["index"], serde_json::json!(3));
    }
}
