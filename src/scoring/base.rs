use std::collections::HashMap;
use uuid::Uuid;
use crate::engine::task::Task;
use crate::engine::node::Node;

#[derive(Debug, Clone)]
pub struct Score {
    pub value: f64,
    pub node_id: Uuid,
    pub breakdown: HashMap<String, f64>, // Metric -> value
    pub timestamp: std::time::Instant,
}

#[derive(Debug)]
pub enum ScoringError {
    InsufficientData,
    InvalidWeights,
    NodeUnavailable,
}

pub trait Scorer: Send + Sync {
    /// Score a single node for a given task
    fn score_node(&self, task: &Task, node: &Node) -> Result<Score, ScoringError>;

    /// Score all available nodes and return sorted results
    fn score_nodes(&self, task: &Task, nodes: &[Node]) -> Vec<Score> {
        let mut scores = Vec::new();
        for node in nodes {
            if let Ok(score) = self.score_node(task, node) {
                scores.push(score);
            }
        }
        scores.sort_by(|a, b| b.value.partial_cmp(&a.value).unwrap());
        scores
    }

    /// Update scorer with feedback (optional for static scorers)
    fn update(&mut self, _feedback: &crate::scoring::feedback::FeedbackMetrics) {
        // Default: no-op for static scorers
    }
}

// Debug implementation for dyn Scorer
impl std::fmt::Debug for dyn Scorer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Scorer")
    }
}

pub struct StaticScorer {
    weights: HashMap<String, f64>, // Metric name -> weight
}

impl StaticScorer {
    pub fn new(weights: HashMap<String, f64>) -> Self {
        Self { weights }
    }
}

impl Scorer for StaticScorer {
    fn score_node(&self, task: &Task, node: &Node) -> Result<Score, ScoringError> {
        // TODO: Implement actual scoring logic
        // This will integrate with existing scheduler logic
        Ok(Score {
            value: 0.0,
            node_id: node.id,
            breakdown: HashMap::new(),
            timestamp: std::time::Instant::now(),
        })
    }
}

// Also implement Debug for StaticScorer
impl std::fmt::Debug for StaticScorer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StaticScorer")
            .field("weights", &self.weights)
            .finish()
    }
}