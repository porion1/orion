use super::FeedbackMetrics;
use std::collections::VecDeque;
use std::sync::{Arc, RwLock};

pub struct FeedbackCollector {
    feedback_history: Arc<RwLock<VecDeque<FeedbackMetrics>>>,
    max_history: usize,
}

impl FeedbackCollector {
    pub fn new(max_history: usize) -> Self {
        Self {
            feedback_history: Arc::new(RwLock::new(VecDeque::with_capacity(max_history))),
            max_history,
        }
    }

    pub fn record_feedback(&self, feedback: FeedbackMetrics) {
        let mut history = self.feedback_history.write().unwrap();
        if history.len() >= self.max_history {
            history.pop_front();
        }
        history.push_back(feedback);
    }

    pub fn get_recent_feedback(&self, count: usize) -> Vec<FeedbackMetrics> {
        let history = self.feedback_history.read().unwrap();
        history.iter().rev().take(count).cloned().collect()
    }

    pub fn get_feedback_for_node(&self, node_id: &str) -> Vec<FeedbackMetrics> {
        let history = self.feedback_history.read().unwrap();
        history
            .iter()
            .filter(|fb| fb.node_id == node_id)
            .cloned()
            .collect()
    }
}