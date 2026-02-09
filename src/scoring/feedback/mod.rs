pub mod collector;

use std::collections::HashMap;
pub use collector::FeedbackCollector;

#[derive(Debug, Clone)]
pub struct FeedbackMetrics {
    pub task_id: String,
    pub node_id: String,
    pub duration: std::time::Duration,
    pub success: bool,
    pub resource_utilization: HashMap<String, f64>, // Metric -> utilization percentage
    pub error_code: Option<String>,
    pub timestamp: std::time::Instant,
}