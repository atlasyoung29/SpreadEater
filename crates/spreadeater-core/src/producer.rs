use crate::envelope::EventEnvelope;

/// Snapshot of queue fill levels per priority.
#[derive(Debug, Clone)]
pub struct QueueDepthSnapshot {
    pub critical: usize,
    pub normal: usize,
}

/// Error producing an event.
#[derive(Debug, thiserror::Error)]
pub enum ProducerError {
    #[error("producer is shut down")]
    Shutdown,
    #[error("serialization failed: {0}")]
    Serialization(String),
}

/// Non-blocking event producer trait.
///
/// Implementations must never block or await in `emit()`.
/// Returns `Ok(true)` if enqueued, `Ok(false)` if dropped (queue full).
pub trait EventProducer: Send + Sync {
    fn emit(&self, event: EventEnvelope) -> Result<bool, ProducerError>;
    fn queue_depth(&self) -> QueueDepthSnapshot;
    fn is_degraded(&self) -> bool;
}
