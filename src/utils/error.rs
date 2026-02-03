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
    ConfigurationError(String),

    #[error("Task error: {0}")]
    TaskError(String),

    #[error("Unknown error: {0}")]
    Unknown(String),
}

// Optional: Provide conversion helpers for common error types
impl OrionError {
    pub fn configuration<S: Into<String>>(msg: S) -> Self {
        OrionError::ConfigurationError(msg.into())
    }

    pub fn node<S: Into<String>>(msg: S) -> Self {
        OrionError::NodeError(msg.into())
    }

    pub fn network<S: Into<String>>(msg: S) -> Self {
        OrionError::NetworkError(msg.into())
    }

    pub fn task<S: Into<String>>(msg: S) -> Self {
        OrionError::TaskError(msg.into())
    }

    // Add these missing helper methods:
    pub fn serialization<S: Into<String>>(msg: S) -> Self {
        OrionError::SerializationError(msg.into())
    }

    pub fn persistence<S: Into<String>>(msg: S) -> Self {
        OrionError::PersistenceError(msg.into())
    }
}

// Optional: Implement From trait for common error conversions
impl From<serde_json::Error> for OrionError {
    fn from(err: serde_json::Error) -> Self {
        OrionError::SerializationError(err.to_string())
    }
}

impl From<std::io::Error> for OrionError {
    fn from(err: std::io::Error) -> Self {
        OrionError::PersistenceError(err.to_string())
    }
}

// If you add the config crate dependency, keep this:
impl From<config::ConfigError> for OrionError {
    fn from(err: config::ConfigError) -> Self {
        OrionError::ConfigurationError(err.to_string())
    }
}

// If you DON'T want to use the config crate, remove/comment out the above impl
// and instead do:
/*
// Remove or comment out this if config crate isn't needed:
// impl From<config::ConfigError> for OrionError {
//     fn from(err: config::ConfigError) -> Self {
//         OrionError::ConfigurationError(err.to_string())
//     }
// }
*/