use super::super::base::{Scorer, Score, ScoringError};
use crate::engine::task::Task;
use std::collections::HashMap;
use crate::engine::node::Node;

pub struct CompositeScorer {
    scorers: Vec<Box<dyn Scorer>>,
    weights: Vec<f64>,
}

impl CompositeScorer {
    pub fn new(scorers: Vec<Box<dyn Scorer>>, weights: Vec<f64>) -> Self {
        assert_eq!(scorers.len(), weights.len(), "Scorers and weights must have same length");
        Self { scorers, weights }
    }
}

impl Scorer for CompositeScorer {
    fn score_node(&self, task: &Task, node: &Node) -> Result<Score, ScoringError> {
        let mut total_score = 0.0;
        let mut breakdown = HashMap::new();

        for (i, scorer) in self.scorers.iter().enumerate() {
            match scorer.score_node(task, node) {
                Ok(score) => {
                    total_score += score.value * self.weights[i];
                    breakdown.insert(format!("scorer_{}", i), score.value);
                }
                Err(e) => {
                    // Skip this scorer's contribution if it errors
                    breakdown.insert(format!("scorer_{}_error", i), 0.0);
                }
            }
        }

        Ok(Score {
            value: total_score,
            node_id: node.id.clone(),
            breakdown,
            timestamp: std::time::Instant::now(),
        })
    }
}