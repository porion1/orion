use thiserror::Error;

#[derive(Debug, Error)]
pub enum OrionError {
    #[error("Node error: {0}")]
    NodeError(String),

    #[error("Network error: {0}")]
    NetworkError(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Persistence error: {0}")]
    PersistenceError(String),

    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Task error: {0}")]
    TaskError(String),

    #[error("Unknown error: {0}")]
    Unknown(String),
}