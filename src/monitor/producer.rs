use spreadeater_core::{
    EventEnvelope, EventProducer, EventWriter, Priority, ProducerError, QueueDepthSnapshot,
    WriterHealth,
};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::{self, error::TrySendError, Receiver, Sender};
use tokio::task::JoinHandle;
use tokio::time::{self, Duration, MissedTickBehavior};

const DEFAULT_CRITICAL_CAPACITY: usize = 256;
const DEFAULT_NORMAL_CAPACITY: usize = 4096;
const DEFAULT_BATCH_SIZE: usize = 100;
const DEFAULT_FLUSH_INTERVAL_MS: u64 = 100;

pub struct BoundedEventQueue {
    critical_tx: Sender<EventEnvelope>,
    normal_tx: Sender<EventEnvelope>,
    critical_depth: Arc<AtomicUsize>,
    normal_depth: Arc<AtomicUsize>,
    degraded: Arc<AtomicBool>,
    flusher: Mutex<Option<JoinHandle<()>>>,
}

impl BoundedEventQueue {
    pub fn new(writer: Arc<dyn EventWriter>) -> Self {
        Self::with_settings(
            writer,
            DEFAULT_CRITICAL_CAPACITY,
            DEFAULT_NORMAL_CAPACITY,
            DEFAULT_BATCH_SIZE,
            Duration::from_millis(DEFAULT_FLUSH_INTERVAL_MS),
        )
    }

    fn with_settings(
        writer: Arc<dyn EventWriter>,
        critical_capacity: usize,
        normal_capacity: usize,
        batch_size: usize,
        flush_interval: Duration,
    ) -> Self {
        let (critical_tx, critical_rx) = mpsc::channel(critical_capacity);
        let (normal_tx, normal_rx) = mpsc::channel(normal_capacity);

        let critical_depth = Arc::new(AtomicUsize::new(0));
        let normal_depth = Arc::new(AtomicUsize::new(0));
        let degraded = Arc::new(AtomicBool::new(false));

        let flusher = tokio::spawn(run_flusher(
            writer,
            critical_rx,
            normal_rx,
            critical_depth.clone(),
            normal_depth.clone(),
            degraded.clone(),
            batch_size,
            flush_interval,
        ));

        Self {
            critical_tx,
            normal_tx,
            critical_depth,
            normal_depth,
            degraded,
            flusher: Mutex::new(Some(flusher)),
        }
    }
}

impl EventProducer for BoundedEventQueue {
    fn emit(&self, event: EventEnvelope) -> Result<bool, ProducerError> {
        let (tx, depth) = if event.priority == Priority::Critical {
            (&self.critical_tx, &self.critical_depth)
        } else {
            (&self.normal_tx, &self.normal_depth)
        };

        depth.fetch_add(1, Ordering::SeqCst);
        match tx.try_send(event) {
            Ok(()) => Ok(true),
            Err(TrySendError::Full(_)) => {
                depth.fetch_sub(1, Ordering::SeqCst);
                Ok(false)
            }
            Err(TrySendError::Closed(_)) => {
                depth.fetch_sub(1, Ordering::SeqCst);
                Err(ProducerError::Shutdown)
            }
        }
    }

    fn queue_depth(&self) -> QueueDepthSnapshot {
        QueueDepthSnapshot {
            critical: self.critical_depth.load(Ordering::SeqCst),
            normal: self.normal_depth.load(Ordering::SeqCst),
        }
    }

    fn is_degraded(&self) -> bool {
        self.degraded.load(Ordering::SeqCst)
    }
}

impl Drop for BoundedEventQueue {
    fn drop(&mut self) {
        if let Ok(mut flusher) = self.flusher.lock() {
            if let Some(handle) = flusher.take() {
                handle.abort();
            }
        }
    }
}

async fn run_flusher(
    writer: Arc<dyn EventWriter>,
    mut critical_rx: Receiver<EventEnvelope>,
    mut normal_rx: Receiver<EventEnvelope>,
    critical_depth: Arc<AtomicUsize>,
    normal_depth: Arc<AtomicUsize>,
    degraded: Arc<AtomicBool>,
    batch_size: usize,
    flush_interval: Duration,
) {
    let mut critical_pending = VecDeque::new();
    let mut normal_pending = VecDeque::new();
    let mut interval = time::interval(flush_interval);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let mut flush_requested = false;

    loop {
        tokio::select! {
            biased;
            Some(event) = critical_rx.recv() => {
                critical_depth.fetch_sub(1, Ordering::SeqCst);
                critical_pending.push_back(event);
                flush_requested = true;
            }
            Some(event) = normal_rx.recv() => {
                normal_depth.fetch_sub(1, Ordering::SeqCst);
                normal_pending.push_back(event);
            }
            _ = interval.tick() => {
                flush_requested = true;
            }
            else => {
                break;
            }
        }

        drain_channel(&mut critical_rx, &mut critical_pending, &critical_depth);
        drain_channel(&mut normal_rx, &mut normal_pending, &normal_depth);

        if flush_requested {
            flush_requested = false;
            flush_pending(
                writer.as_ref(),
                &mut critical_pending,
                &mut normal_pending,
                &degraded,
                batch_size,
            )
            .await;
        }
    }

    flush_pending(
        writer.as_ref(),
        &mut critical_pending,
        &mut normal_pending,
        &degraded,
        batch_size,
    )
    .await;
}

fn drain_channel(
    rx: &mut Receiver<EventEnvelope>,
    pending: &mut VecDeque<EventEnvelope>,
    depth: &AtomicUsize,
) {
    while let Ok(event) = rx.try_recv() {
        depth.fetch_sub(1, Ordering::SeqCst);
        pending.push_back(event);
    }
}

async fn flush_pending(
    writer: &dyn EventWriter,
    critical_pending: &mut VecDeque<EventEnvelope>,
    normal_pending: &mut VecDeque<EventEnvelope>,
    degraded: &AtomicBool,
    batch_size: usize,
) {
    while !critical_pending.is_empty() || !normal_pending.is_empty() {
        let mut batch = Vec::with_capacity(batch_size);

        while batch.len() < batch_size {
            if let Some(event) = critical_pending.pop_front() {
                batch.push(event);
            } else {
                break;
            }
        }

        while batch.len() < batch_size {
            if let Some(event) = normal_pending.pop_front() {
                batch.push(event);
            } else {
                break;
            }
        }

        if batch.is_empty() {
            return;
        }

        let healthy = writer.write_batch(&batch).await.is_ok()
            && writer.flush().await.is_ok()
            && writer.health() == WriterHealth::Healthy;
        degraded.store(!healthy, Ordering::SeqCst);

        if !healthy {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::monitor::JsonlFileWriter;
    use async_trait::async_trait;
    use spreadeater_core::{EventEnvelope, EventType, Priority, WriterError};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::fs;
    use tokio::sync::Mutex as AsyncMutex;
    use uuid::Uuid;

    struct RecordingWriter {
        events: AsyncMutex<Vec<EventEnvelope>>,
        flushes: AtomicUsize,
    }

    impl RecordingWriter {
        fn new() -> Self {
            Self {
                events: AsyncMutex::new(Vec::new()),
                flushes: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl EventWriter for RecordingWriter {
        async fn write_batch(&self, events: &[EventEnvelope]) -> Result<usize, WriterError> {
            self.events.lock().await.extend(events.iter().cloned());
            Ok(events.len())
        }

        async fn flush(&self) -> Result<(), WriterError> {
            self.flushes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn health(&self) -> WriterHealth {
            WriterHealth::Healthy
        }
    }

    fn sample_event(priority: Priority, index: usize) -> EventEnvelope {
        EventEnvelope::new(
            EventType::DecisionEvaluated,
            priority,
            "run_queue_test".to_string(),
            "test".to_string(),
            "dry-run".to_string(),
            serde_json::json!({ "index": index }),
        )
    }

    #[tokio::test]
    async fn drops_when_normal_queue_is_full() {
        let writer = Arc::new(RecordingWriter::new());
        let producer = BoundedEventQueue::with_settings(writer, 1, 1, 100, Duration::from_secs(60));

        assert!(producer.emit(sample_event(Priority::Normal, 1)).unwrap());
        assert!(!producer.emit(sample_event(Priority::Normal, 2)).unwrap());
        assert_eq!(producer.queue_depth().normal, 1);
    }

    #[tokio::test]
    async fn critical_events_use_separate_channel() {
        let writer = Arc::new(RecordingWriter::new());
        let producer = BoundedEventQueue::with_settings(writer, 1, 1, 100, Duration::from_secs(60));

        assert!(producer.emit(sample_event(Priority::Normal, 1)).unwrap());
        assert!(!producer.emit(sample_event(Priority::Normal, 2)).unwrap());
        assert!(producer.emit(sample_event(Priority::Critical, 3)).unwrap());

        let depth = producer.queue_depth();
        assert_eq!(depth.normal, 1);
        assert_eq!(depth.critical, 1);
    }

    #[tokio::test]
    async fn emit_is_non_blocking() {
        let writer = Arc::new(RecordingWriter::new());
        let producer = BoundedEventQueue::with_settings(writer, 8, 8, 100, Duration::from_secs(60));

        let started = std::time::Instant::now();
        let emitted = producer.emit(sample_event(Priority::Normal, 1)).unwrap();
        let elapsed = started.elapsed();

        assert!(emitted);
        assert!(
            elapsed < Duration::from_millis(5),
            "emit took {:?}",
            elapsed
        );
    }

    #[tokio::test]
    async fn emit_round_trips_to_jsonl_writer() {
        let root = std::env::temp_dir().join(format!("spreadeater-queue-{}", Uuid::new_v4()));
        let writer = Arc::new(JsonlFileWriter::new(&root, "run_roundtrip").await.unwrap());
        let producer =
            BoundedEventQueue::with_settings(writer, 4, 4, 100, Duration::from_millis(10));

        assert!(producer.emit(sample_event(Priority::Normal, 1)).unwrap());
        assert!(producer.emit(sample_event(Priority::Critical, 2)).unwrap());

        tokio::time::sleep(Duration::from_millis(100)).await;

        let contents = fs::read_to_string(root.join("run_roundtrip").join("events.jsonl"))
            .await
            .unwrap();
        let events: Vec<EventEnvelope> = contents
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();

        assert_eq!(events.len(), 2);
        assert!(events
            .iter()
            .any(|event| event.priority == Priority::Critical));
    }
}
