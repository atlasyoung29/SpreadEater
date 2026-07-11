use async_trait::async_trait;

use crate::envelope::EventEnvelope;

/// Health status of an event writer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriterHealth {
    Healthy,
    Degraded,
}

/// Error writing events.
#[derive(Debug, thiserror::Error)]
pub enum WriterError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serialization(String),
}

/// Async event writer trait for durable persistence.
#[async_trait]
pub trait EventWriter: Send + Sync {
    /// Write a batch of events. Returns count of events written.
    async fn write_batch(&self, events: &[EventEnvelope]) -> Result<usize, WriterError>;

    /// Flush buffered data to durable storage.
    async fn flush(&self) -> Result<(), WriterError>;

    /// Current health status.
    fn health(&self) -> WriterHealth;
}
