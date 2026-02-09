use super::base::{Scorer, Score, ScoringError};
use super::feedback::FeedbackMetrics;
use crate::engine::task::Task;
use crate::engine::node::Node;
use std::collections::{HashMap, VecDeque};

pub struct AdaptiveScorer {
    base_weights: HashMap<String, f64>,
    current_weights: HashMap<String, f64>,
    learning_rate: f64,
    history: VecDeque<FeedbackMetrics>,
    max_history: usize,
}

impl AdaptiveScorer {
    pub fn new(
        initial_weights: HashMap<String, f64>,
        learning_rate: f64,
        max_history: usize,
    ) -> Self {
        Self {
            base_weights: initial_weights.clone(),
            current_weights: initial_weights,
            learning_rate,
            history: VecDeque::with_capacity(max_history),
            max_history,
        }
    }

    fn adapt_weights(&mut self, feedback: &FeedbackMetrics) {
        // Create new weights map
        let mut new_weights = std::collections::HashMap::new();

        // Calculate adjusted weights
        for (metric, current_weight) in &self.current_weights {
            if let Some(utilization) = feedback.resource_utilization.get(metric) {
                // Use static method to avoid borrowing self
                let adjustment = Self::calculate_adjustment(metric, *utilization, feedback);
                let new_weight = (*current_weight * (1.0 + self.learning_rate * adjustment))
                    .clamp(0.0, 1.0); // Use 0.0 and 1.0 as min/max since fields don't exist
                new_weights.insert(metric.clone(), new_weight);
            } else {
                // Keep original weight if no utilization data
                new_weights.insert(metric.clone(), *current_weight);
            }
        }

        // Replace old weights
        self.current_weights = new_weights;

        // Normalize weights
        self.normalize_weights();
    }

    // Make this a static method
    fn calculate_adjustment(metric: &str, utilization: f64, feedback: &FeedbackMetrics) -> f64 {
        // TODO: Implement actual adaptation logic
        // Simple example: adjust based on utilization
        if utilization > 0.8 {
            -0.1 // Reduce weight for high utilization
        } else if utilization < 0.2 {
            0.1  // Increase weight for low utilization
        } else {
            0.0  // No change for moderate utilization
        }
    }

    // Make this a static method
    fn calculate_adjustment_static(metric: &str, utilization: f64, feedback: &FeedbackMetrics) -> f64 {
        // TODO: Implement actual adaptation logic
        0.0
    }

    fn normalize_weights(&mut self) {
        let total: f64 = self.current_weights.values().sum();
        if total > 0.0 {
            for weight in self.current_weights.values_mut() {
                *weight /= total;
            }
        }
    }
}

impl Scorer for AdaptiveScorer {
    fn score_node(&self, task: &Task, node: &Node) -> Result<Score, ScoringError> {
        // Use current adaptive weights for scoring
        // TODO: Implement scoring with adaptive weights
        Ok(Score {
            value: 0.0,
            node_id: node.id.clone(),
            breakdown: HashMap::new(),
            timestamp: std::time::Instant::now(),
        })
    }

    fn update(&mut self, feedback: &FeedbackMetrics) {
        // Store feedback in history
        if self.history.len() >= self.max_history {
            self.history.pop_front();
        }
        self.history.push_back(feedback.clone());

        // Adapt weights based on new feedback
        self.adapt_weights(feedback);
    }
}