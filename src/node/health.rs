use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, Duration};
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
            .ok_or_else(|| crate::utils::error::OrionError::node(format!("Node {} not found", node_id)))?;

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

    /// Get score with fallback calculation
    pub fn get_or_calculate_score(&self, node_id: &Uuid) -> Option<HealthScore> {
        if let Some(score) = self.get_score(node_id) {
            // Check if score is stale (older than 30 seconds)
            if let Ok(age) = SystemTime::now().duration_since(score.last_calculated) {
                if age < Duration::from_secs(30) {
                    return Some(score);
                }
            }
        }

        // Calculate fresh score
        self.calculate_score(node_id).ok()
    }

    /// Get score with minimum freshness guarantee
    pub fn get_score_with_freshness(&self, node_id: &Uuid, max_age: Duration) -> Option<HealthScore> {
        if let Some(score) = self.get_score(node_id) {
            if let Ok(age) = SystemTime::now().duration_since(score.last_calculated) {
                if age <= max_age {
                    return Some(score);
                }
            }
        }

        // Score is stale or doesn't exist, calculate fresh one
        self.calculate_score(node_id).ok()
    }

    pub fn get_all_scores(&self) -> Vec<HealthScore> {
        self.scores.iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Get all scores with optional freshness filter
    pub fn get_all_fresh_scores(&self, max_age: Option<Duration>) -> Vec<HealthScore> {
        if let Some(max_age) = max_age {
            let now = SystemTime::now();
            self.scores.iter()
                .filter(|entry| {
                    if let Ok(age) = now.duration_since(entry.value().last_calculated) {
                        age <= max_age
                    } else {
                        false
                    }
                })
                .map(|entry| entry.value().clone())
                .collect()
        } else {
            self.get_all_scores()
        }
    }

    /// Get scores for multiple nodes at once
    pub fn get_scores_batch(&self, node_ids: &[Uuid]) -> HashMap<Uuid, Option<HealthScore>> {
        let mut result = HashMap::new();

        for node_id in node_ids {
            result.insert(*node_id, self.get_score(node_id));
        }

        result
    }

    /// Get scores for multiple nodes with freshness guarantee
    pub fn get_fresh_scores_batch(&self, node_ids: &[Uuid], max_age: Duration) -> HashMap<Uuid, Option<HealthScore>> {
        let mut result = HashMap::new();

        for node_id in node_ids {
            result.insert(*node_id, self.get_score_with_freshness(node_id, max_age));
        }

        result
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

    /// Get healthy nodes with additional filtering
    pub fn get_filtered_healthy_nodes(&self, min_score: f64, min_trend: Option<HealthTrend>, exclude_declining: bool) -> Vec<Uuid> {
        let mut nodes: Vec<_> = self.scores.iter()
            .filter(|entry| {
                let score = entry.value();

                // Check minimum score
                if score.score < min_score {
                    return false;
                }

                // Check trend filter
                if let Some(min_trend) = &min_trend {
                    match (&score.trend, min_trend) {
                        (HealthTrend::Improving, HealthTrend::Stable) => return true, // Improving is better than stable
                        (HealthTrend::Improving, HealthTrend::Improving) => return true,
                        (HealthTrend::Stable, HealthTrend::Stable) => return true,
                        (HealthTrend::Stable, HealthTrend::Improving) => return false, // Stable is not as good as improving
                        (HealthTrend::Declining, _) => return !exclude_declining, // Only include if not excluded
                        (HealthTrend::Unknown, _) => return false,
                        _ => return true,
                    }
                }

                // Check declining exclusion
                if exclude_declining && score.trend == HealthTrend::Declining {
                    return false;
                }

                true
            })
            .map(|entry| (entry.value().node_id, entry.value().score))
            .collect();

        nodes.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        nodes.into_iter().map(|(id, _)| id).collect()
    }

    /// Get nodes grouped by health tiers
    pub fn get_nodes_by_health_tier(&self) -> HashMap<String, Vec<Uuid>> {
        let mut tiers = HashMap::new();
        tiers.insert("critical".to_string(), Vec::new());  // 0-30
        tiers.insert("poor".to_string(), Vec::new());      // 31-60
        tiers.insert("good".to_string(), Vec::new());      // 61-80
        tiers.insert("excellent".to_string(), Vec::new()); // 81-100

        for entry in self.scores.iter() {
            let score = entry.value().score;
            let node_id = entry.value().node_id;

            let tier = if score <= 30.0 {
                "critical"
            } else if score <= 60.0 {
                "poor"
            } else if score <= 80.0 {
                "good"
            } else {
                "excellent"
            };

            if let Some(tier_nodes) = tiers.get_mut(tier) {
                tier_nodes.push(node_id);
            }
        }

        tiers
    }

    /// Check if node is healthy enough for task assignment
    pub fn is_healthy_for_tasks(&self, node_id: &Uuid) -> bool {
        if let Some(score) = self.get_score(node_id) {
            score.score >= 70.0 && score.trend != HealthTrend::Declining
        } else {
            false
        }
    }

    /// Check if node meets specific health requirements
    pub fn meets_health_requirements(&self, node_id: &Uuid, min_score: f64, max_score: Option<f64>, allowed_trends: &[HealthTrend]) -> bool {
        if let Some(score) = self.get_score(node_id) {
            // Check score range
            if score.score < min_score {
                return false;
            }

            if let Some(max_score) = max_score {
                if score.score > max_score {
                    return false;
                }
            }

            // Check trend
            if !allowed_trends.contains(&score.trend) && !allowed_trends.is_empty() {
                return false;
            }

            true
        } else {
            false
        }
    }

    /// Calculate cluster health statistics
    pub fn get_cluster_health_stats(&self) -> ClusterHealthStats {
        let scores = self.get_all_scores();
        let total_nodes = scores.len();

        let avg_score = if total_nodes > 0 {
            scores.iter().map(|s| s.score).sum::<f64>() / total_nodes as f64
        } else {
            0.0
        };

        let healthy_nodes = scores.iter().filter(|s| s.score >= 70.0).count();
        let critical_nodes = scores.iter().filter(|s| s.score <= 30.0).count();

        let mut improving_count = 0;
        let mut stable_count = 0;
        let mut declining_count = 0;
        let mut unknown_count = 0;

        for score in &scores {
            match score.trend {
                HealthTrend::Improving => improving_count += 1,
                HealthTrend::Stable => stable_count += 1,
                HealthTrend::Declining => declining_count += 1,
                HealthTrend::Unknown => unknown_count += 1,
            }
        }

        ClusterHealthStats {
            total_nodes,
            healthy_nodes,
            critical_nodes,
            average_score: avg_score,
            improving_nodes: improving_count,
            stable_nodes: stable_count,
            declining_nodes: declining_count,
            unknown_nodes: unknown_count,
            overall_status: if avg_score >= 70.0 && declining_count < (total_nodes / 3) {
                "healthy".to_string()
            } else if avg_score >= 50.0 {
                "degraded".to_string()
            } else {
                "unhealthy".to_string()
            },
        }
    }

    /// Force recalculation for all nodes
    pub fn recalculate_all_scores(&self) -> Result<Vec<HealthScore>, crate::utils::error::OrionError> {
        let nodes = self.registry.get_all_nodes();
        let mut results = Vec::new();

        for node in nodes {
            match self.calculate_score(&node.id) {
                Ok(score) => results.push(score),
                Err(e) => eprintln!("Failed to calculate score for node {}: {}", node.id, e),
            }
        }

        Ok(results)
    }

    /// Clean up old history entries
    pub fn cleanup_old_history(&self, max_age: Duration) {
        let now = SystemTime::now();

        for mut entry in self.history.iter_mut() {
            let history = entry.value_mut();
            history.retain(|(timestamp, _)| {
                now.duration_since(*timestamp).map_or(false, |age| age <= max_age)
            });
        }
    }

    /// Get health score history for a node
    pub fn get_score_history(&self, node_id: &Uuid, limit: Option<usize>) -> Vec<(SystemTime, f64)> {
        if let Some(history) = self.history.get(node_id) {
            let mut history = history.clone();
            history.sort_by(|a, b| b.0.cmp(&a.0)); // Sort by timestamp descending

            if let Some(limit) = limit {
                history.truncate(limit);
            }

            history
        } else {
            Vec::new()
        }
    }

    /// Get health component breakdown for a node
    pub fn get_component_breakdown(&self, node_id: &Uuid) -> Option<HashMap<HealthComponent, f64>> {
        self.get_score(node_id).map(|score| score.components)
    }

    /// Update health component weight
    pub fn update_component_weight(&mut self, component: HealthComponent, weight: f64) -> Result<(), crate::utils::error::OrionError> {
        if !(0.0..=1.0).contains(&weight) {
            return Err(crate::utils::error::OrionError::configuration(
                "Weight must be between 0.0 and 1.0".to_string()
            ));
        }

        self.weights.insert(component, weight);

        // Recalculate all scores to apply new weights
        let _ = self.recalculate_all_scores();

        Ok(())
    }

    /// Get current component weights
    pub fn get_component_weights(&self) -> &HashMap<HealthComponent, f64> {
        &self.weights
    }
}

/// Cluster health statistics
#[derive(Debug, Clone)]
pub struct ClusterHealthStats {
    pub total_nodes: usize,
    pub healthy_nodes: usize,
    pub critical_nodes: usize,
    pub average_score: f64,
    pub improving_nodes: usize,
    pub stable_nodes: usize,
    pub declining_nodes: usize,
    pub unknown_nodes: usize,
    pub overall_status: String,
}

/// Health scorer builder for custom configuration
pub struct HealthScorerBuilder {
    registry: Arc<crate::node::registry::NodeRegistry>,
    weights: Option<HashMap<HealthComponent, f64>>,
    max_history_size: Option<usize>,
}

impl HealthScorerBuilder {
    pub fn new(registry: Arc<crate::node::registry::NodeRegistry>) -> Self {
        Self {
            registry,
            weights: None,
            max_history_size: None,
        }
    }

    pub fn with_weights(mut self, weights: HashMap<HealthComponent, f64>) -> Self {
        self.weights = Some(weights);
        self
    }

    pub fn with_max_history_size(mut self, max_history_size: usize) -> Self {
        self.max_history_size = Some(max_history_size);
        self
    }

    pub fn build(self) -> HealthScorer {
        let weights = self.weights.unwrap_or_else(|| {
            HashMap::from([
                (HealthComponent::HeartbeatLatency, 0.2),
                (HealthComponent::HeartbeatRegularity, 0.15),
                (HealthComponent::ResourceUsage, 0.25),
                (HealthComponent::TaskSuccessRate, 0.25),
                (HealthComponent::NetworkLatency, 0.1),
                (HealthComponent::Uptime, 0.05),
            ])
        });

        HealthScorer {
            registry: self.registry,
            scores: dashmap::DashMap::new(),
            history: dashmap::DashMap::new(),
            weights,
            max_history_size: self.max_history_size.unwrap_or(100),
        }
    }
}