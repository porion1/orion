use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct HealthScore {
    pub node_id: Uuid,
    pub score: f64, // 0.0 - 100.0
    pub components: HashMap<HealthComponent, f64>,
    pub last_calculated: SystemTime,
    pub trend: HealthTrend,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum HealthComponent {
    HeartbeatLatency,
    HeartbeatRegularity,
    ResourceUsage,
    TaskSuccessRate,
    NetworkLatency,
    Uptime,
}

#[derive(Debug, Clone, PartialEq)]
pub enum HealthTrend {
    Improving,
    Stable,
    Declining,
    Unknown,
}

#[derive(Debug)]
pub struct HealthScorer {
    registry: Arc<crate::node::registry::NodeRegistry>,
    scores: dashmap::DashMap<Uuid, HealthScore>,
    history: dashmap::DashMap<Uuid, Vec<(SystemTime, f64)>>,
    weights: HashMap<HealthComponent, f64>,
    max_history_size: usize,
}

impl HealthScorer {
    pub fn new(registry: Arc<crate::node::registry::NodeRegistry>) -> Self {
        let weights = HashMap::from([
            (HealthComponent::HeartbeatLatency, 0.2),
            (HealthComponent::HeartbeatRegularity, 0.15),
            (HealthComponent::ResourceUsage, 0.25),
            (HealthComponent::TaskSuccessRate, 0.25),
            (HealthComponent::NetworkLatency, 0.1),
            (HealthComponent::Uptime, 0.05),
        ]);

        Self {
            registry,
            scores: dashmap::DashMap::new(),
            history: dashmap::DashMap::new(),
            weights,
            max_history_size: 100,
        }
    }

    /// Calculate health score for a node
    pub fn calculate_score(&self, node_id: &Uuid) -> Result<HealthScore, crate::utils::error::OrionError> {
        let node_info = self.registry.get(node_id)
            .ok_or_else(|| crate::utils::error::OrionError::NodeError(format!("Node {} not found", node_id)))?;

        let mut components = HashMap::new();

        // Calculate heartbeat latency score
        let heartbeat_latency = self.calculate_heartbeat_latency(&node_info);
        components.insert(HealthComponent::HeartbeatLatency, heartbeat_latency);

        // Calculate heartbeat regularity score
        let heartbeat_regularity = self.calculate_heartbeat_regularity(&node_info);
        components.insert(HealthComponent::HeartbeatRegularity, heartbeat_regularity);

        // Calculate resource usage score
        let resource_usage = self.calculate_resource_usage(&node_info);
        components.insert(HealthComponent::ResourceUsage, resource_usage);

        // Calculate task success rate
        let task_success_rate = self.calculate_task_success_rate(&node_info);
        components.insert(HealthComponent::TaskSuccessRate, task_success_rate);

        // Calculate network latency
        let network_latency = self.calculate_network_latency(&node_info);
        components.insert(HealthComponent::NetworkLatency, network_latency);

        // Calculate uptime score
        let uptime = self.calculate_uptime(&node_info);
        components.insert(HealthComponent::Uptime, uptime);

        // Calculate weighted average
        let mut total_score = 0.0;
        let mut total_weight = 0.0;

        for (component, weight) in &self.weights {
            if let Some(score) = components.get(component) {
                total_score += score * weight;
                total_weight += weight;
            }
        }

        let final_score = if total_weight > 0.0 {
            (total_score / total_weight).clamp(0.0, 100.0)
        } else {
            0.0
        };

        // Determine trend
        let trend = self.calculate_trend(node_id, final_score);

        let score = HealthScore {
            node_id: *node_id,
            score: final_score,
            components,
            last_calculated: SystemTime::now(),
            trend,
        };

        // Store score
        self.scores.insert(*node_id, score.clone());

        // Update history
        self.update_history(node_id, final_score);

        Ok(score)
    }

    fn calculate_heartbeat_latency(&self, node_info: &crate::node::registry::NodeInfo) -> f64 {
        let now = SystemTime::now();
        if let Ok(duration) = now.duration_since(node_info.last_seen) {
            let latency_ms = duration.as_millis() as f64;
            // Score decreases as latency increases
            // Perfect score for < 1s, zero for > 30s
            (30_000.0 - latency_ms.min(30_000.0)) / 30_000.0 * 100.0
        } else {
            0.0
        }
    }

    fn calculate_heartbeat_regularity(&self, _node_info: &crate::node::registry::NodeInfo) -> f64 {
        // TODO: Implement based on heartbeat history
        // For now, return placeholder
        85.0
    }

    fn calculate_resource_usage(&self, _node_info: &crate::node::registry::NodeInfo) -> f64 {
        // TODO: Get actual resource usage from heartbeat
        // For now, return placeholder
        90.0
    }

    fn calculate_task_success_rate(&self, _node_info: &crate::node::registry::NodeInfo) -> f64 {
        // TODO: Get from task execution history
        // For now, return placeholder
        95.0
    }

    fn calculate_network_latency(&self, _node_info: &crate::node::registry::NodeInfo) -> f64 {
        // TODO: Measure actual network latency
        // For now, return placeholder
        80.0
    }

    fn calculate_uptime(&self, node_info: &crate::node::registry::NodeInfo) -> f64 {
        // Simple uptime calculation based on status
        match node_info.status {
            crate::node::registry::NodeStatus::Active => 100.0,
            crate::node::registry::NodeStatus::Unhealthy => 50.0,
            crate::node::registry::NodeStatus::Joining => 10.0,
            crate::node::registry::NodeStatus::Leaving => 20.0,
            crate::node::registry::NodeStatus::Dead => 0.0,
        }
    }

    fn calculate_trend(&self, node_id: &Uuid, current_score: f64) -> HealthTrend {
        if let Some(history) = self.history.get(node_id) {
            if history.len() >= 3 {
                let recent: Vec<_> = history.iter().rev().take(3).collect();
                let avg_recent: f64 = recent.iter().map(|(_, score)| *score).sum::<f64>() / 3.0;

                if current_score > avg_recent + 5.0 {
                    HealthTrend::Improving
                } else if current_score < avg_recent - 5.0 {
                    HealthTrend::Declining
                } else {
                    HealthTrend::Stable
                }
            } else {
                HealthTrend::Unknown
            }
        } else {
            HealthTrend::Unknown
        }
    }

    fn update_history(&self, node_id: &Uuid, score: f64) {
        let mut entry = self.history.entry(*node_id).or_insert_with(Vec::new);
        entry.push((SystemTime::now(), score));

        // Trim history if too large
        if entry.len() > self.max_history_size {
            entry.remove(0);
        }
    }

    pub fn get_score(&self, node_id: &Uuid) -> Option<HealthScore> {
        self.scores.get(node_id).map(|score| score.value().clone())
    }

    pub fn get_all_scores(&self) -> Vec<HealthScore> {
        self.scores.iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Get nodes sorted by health score (descending)
    pub fn get_healthy_nodes(&self, min_score: f64) -> Vec<Uuid> {
        let mut nodes: Vec<_> = self.scores.iter()
            .filter(|entry| entry.value().score >= min_score)
            .map(|entry| (entry.value().node_id, entry.value().score))
            .collect();

        nodes.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        nodes.into_iter().map(|(id, _)| id).collect()
    }

    /// Check if node is healthy enough for task assignment
    pub fn is_healthy_for_tasks(&self, node_id: &Uuid) -> bool {
        if let Some(score) = self.get_score(node_id) {
            score.score >= 70.0 && score.trend != HealthTrend::Declining
        } else {
            false
        }
    }
}