pub mod base;
pub mod adaptive;
pub mod models;
pub mod feedback;

// Re-exports for easy access
pub use base::{Score, Scorer, StaticScorer, ScoringError};
pub use adaptive::AdaptiveScorer;
pub use models::{ResourceScorer, CompositeScorer};
pub use feedback::{FeedbackCollector, FeedbackMetrics};

// Configuration
pub mod config {
    pub use crate::engine::config::ScoringConfig;
}