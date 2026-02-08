//! Routing strategy implementations

use serde::{Deserialize, Serialize};
use async_trait::async_trait;
use super::*;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Trait for all routing strategies
#[async_trait]
pub trait RoutingStrategy: Send + Sync + std::fmt::Debug {
    /// Select a node for task execution
    async fn select_node(
        &self,
        nodes: &[Node],
        task: &Task,
        context: RoutingContext,
    ) -> Result<String>;

    /// Get strategy name
    fn name(&self) -> &str;

    /// Validate if strategy can handle the task
    fn can_handle_task(&self, task: &Task, nodes: &[Node]) -> bool;
}

/// Round-robin routing strategy
#[derive(Debug, Clone)]
pub struct RoundRobinStrategy {
    current_index: Arc<AtomicUsize>,
    node_order: Arc<RwLock<VecDeque<String>>>,
}

impl RoundRobinStrategy {
    pub fn new() -> Self {
        Self {
            current_index: Arc::new(AtomicUsize::new(0)),
            node_order: Arc::new(RwLock::new(VecDeque::new())),
        }
    }

    async fn update_node_order(&self, nodes: &[Node]) {
        let mut order = self.node_order.write().await;
        *order = nodes.iter()
            .filter(|n| n.is_healthy)
            .map(|n| n.id.clone())
            .collect();
    }
}

#[async_trait]
impl RoutingStrategy for RoundRobinStrategy {
    async fn select_node(
        &self,
        nodes: &[Node],
        _task: &Task,
        _context: RoutingContext,
    ) -> Result<String> {
        if nodes.is_empty() {
            return Err(anyhow!("No available nodes"));
        }

        // Filter healthy nodes
        let healthy_nodes: Vec<&Node> = nodes.iter()
            .filter(|n| n.is_healthy)
            .collect();

        if healthy_nodes.is_empty() {
            return Err(anyhow!("No healthy nodes available"));
        }

        // Update node order if changed
        let current_order: Vec<String> = self.node_order.read().await.iter().cloned().collect();
        let current_ids: Vec<String> = healthy_nodes.iter().map(|n| n.id.clone()).collect();

        if current_order != current_ids {
            self.update_node_order(nodes).await;
        }

        // Get next node in round-robin order
        let order = self.node_order.read().await;
        if order.is_empty() {
            return Err(anyhow!("No nodes in rotation"));
        }

        let index = self.current_index.fetch_update(
            Ordering::SeqCst,
            Ordering::SeqCst,
            |idx| Some((idx + 1) % order.len())
        ).unwrap_or(0) % order.len();

        let node_id = order.get(index)
            .ok_or_else(|| anyhow!("Failed to get node from order"))?
            .clone();

        Ok(node_id)
    }

    fn name(&self) -> &str {
        "round_robin"
    }

    fn can_handle_task(&self, _task: &Task, nodes: &[Node]) -> bool {
        // Round-robin can handle any task as long as there are healthy nodes
        nodes.iter().any(|n| n.is_healthy)
    }
}

/// Least-loaded routing strategy
#[derive(Debug, Clone)]
pub struct LeastLoadedStrategy {
    config: RouterConfig,
}

impl LeastLoadedStrategy {
    pub fn new(config: RouterConfig) -> Self {
        Self {
            config,
        }
    }

    fn calculate_load_score(&self, metrics: &NodeMetrics) -> f64 {
        let weights = &self.config.least_loaded.weights;

        let cpu_score = metrics.cpu_usage as f64 * weights.cpu_weight;
        let memory_score = metrics.memory_usage as f64 * weights.memory_weight;
        let queue_score = (metrics.queue_length as f64 / metrics.capacity.max_queue_length as f64)
            * weights.queue_weight;
        let processing_score = (metrics.processing_tasks as f64 / metrics.capacity.max_concurrent_tasks as f64)
            * weights.processing_weight;

        cpu_score + memory_score + queue_score + processing_score
    }
}

#[async_trait]
impl RoutingStrategy for LeastLoadedStrategy {
    async fn select_node(
        &self,
        nodes: &[Node],
        task: &Task,
        _context: RoutingContext,
    ) -> Result<String> {
        if nodes.is_empty() {
            return Err(anyhow!("No available nodes"));
        }

        // Filter nodes that can handle this task type
        let capable_nodes: Vec<&Node> = nodes.iter()
            .filter(|n| n.is_healthy)
            .filter(|n| n.metrics.capacity.supported_task_types.contains(&task.task_type))
            .collect();

        if capable_nodes.is_empty() {
            return Err(anyhow!("No nodes capable of handling task type: {}", task.task_type));
        }

        // Find node with lowest load score
        let mut best_node = None;
        let mut best_score = f64::MAX;

        for node in capable_nodes {
            let score = self.calculate_load_score(&node.metrics);

            // Apply any node affinity from task
            let affinity_score = if let Some(preferred_node) = &task.routing_hints.preferred_node {
                if node.id == *preferred_node {
                    -10.0  // Strong preference for this node
                } else {
                    0.0
                }
            } else {
                0.0
            };

            let total_score = score + affinity_score;

            if total_score < best_score {
                best_score = total_score;
                best_node = Some(&node.id);
            }
        }

        best_node
            .map(|id| id.clone())
            .ok_or_else(|| anyhow!("Failed to select least loaded node"))
    }

    fn name(&self) -> &str {
        "least_loaded"
    }

    fn can_handle_task(&self, task: &Task, nodes: &[Node]) -> bool {
        nodes.iter()
            .filter(|n| n.is_healthy)
            .any(|n| n.metrics.capacity.supported_task_types.contains(&task.task_type))
    }
}

/// Latency-aware routing strategy
#[derive(Debug, Clone)]
pub struct LatencyAwareStrategy {
    config: RouterConfig,
    latency_tracker: Arc<RwLock<HashMap<String, f64>>>,
}

impl LatencyAwareStrategy {
    pub fn new(config: RouterConfig) -> Self {
        Self {
            config,
            latency_tracker: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn update_latencies(&self, nodes: &[Node]) {
        let mut tracker = self.latency_tracker.write().await;
        for node in nodes {
            if node.is_healthy {
                tracker.insert(node.id.clone(), node.metrics.avg_latency_ms);
            }
        }
    }

    fn calculate_adjusted_latency(
        &self,
        base_latency: f64,
        node_metrics: &NodeMetrics,
        task_priority: u8,
    ) -> f64 {
        let mut adjusted = base_latency;

        // Adjust for node load
        let load_factor = node_metrics.cpu_usage as f64 * 0.3
            + node_metrics.memory_usage as f64 * 0.2
            + (node_metrics.queue_length as f64 / node_metrics.capacity.max_queue_length as f64) * 0.5;

        adjusted *= 1.0 + load_factor;

        // Adjust for task priority (higher priority = lower effective latency)
        let priority_factor = match task_priority {
            0..=2 => 0.5,  // High priority
            3..=5 => 0.8,  // Medium priority
            _ => 1.0,      // Low priority
        };

        adjusted * priority_factor
    }
}

#[async_trait]
impl RoutingStrategy for LatencyAwareStrategy {
    async fn select_node(
        &self,
        nodes: &[Node],
        task: &Task,
        _context: RoutingContext,
    ) -> Result<String> {
        if nodes.is_empty() {
            return Err(anyhow!("No available nodes"));
        }

        // Update latency information
        self.update_latencies(nodes).await;

        // Filter capable nodes
        let capable_nodes: Vec<&Node> = nodes.iter()
            .filter(|n| n.is_healthy)
            .filter(|n| n.metrics.capacity.supported_task_types.contains(&task.task_type))
            .filter(|n| {
                // Check latency threshold
                n.metrics.avg_latency_ms <= self.config.latency_aware.max_latency_ms
            })
            .collect();

        if capable_nodes.is_empty() {
            // Fallback to any capable node
            let fallback_nodes: Vec<&Node> = nodes.iter()
                .filter(|n| n.is_healthy)
                .filter(|n| n.metrics.capacity.supported_task_types.contains(&task.task_type))
                .collect();

            if fallback_nodes.is_empty() {
                return Err(anyhow!("No nodes capable of handling task type: {}", task.task_type));
            }

            // Select best from fallback nodes
            let mut best_node = None;
            let mut best_latency = f64::MAX;

            for node in fallback_nodes {
                let adjusted_latency = self.calculate_adjusted_latency(
                    node.metrics.avg_latency_ms,
                    &node.metrics,
                    task.priority,
                );

                if adjusted_latency < best_latency {
                    best_latency = adjusted_latency;
                    best_node = Some(&node.id);
                }
            }

            return best_node
                .map(|id| id.clone())
                .ok_or_else(|| anyhow!("Failed to select fallback node"));
        }

        // Select node with best adjusted latency
        let mut best_node = None;
        let mut best_adjusted_latency = f64::MAX;

        for node in capable_nodes {
            let adjusted_latency = self.calculate_adjusted_latency(
                node.metrics.avg_latency_ms,
                &node.metrics,
                task.priority,
            );

            if adjusted_latency < best_adjusted_latency {
                best_adjusted_latency = adjusted_latency;
                best_node = Some(&node.id);
            }
        }

        best_node
            .map(|id| id.clone())
            .ok_or_else(|| anyhow!("Failed to select latency-aware node"))
    }

    fn name(&self) -> &str {
        "latency_aware"
    }

    fn can_handle_task(&self, task: &Task, nodes: &[Node]) -> bool {
        nodes.iter()
            .filter(|n| n.is_healthy)
            .any(|n| n.metrics.capacity.supported_task_types.contains(&task.task_type))
    }
}

/// Failover routing strategy
#[derive(Debug, Clone)]
pub struct FailoverStrategy {
    config: RouterConfig,
    failover_handler: Arc<FailoverHandler>,
    primary_nodes: Arc<RwLock<Vec<String>>>,
    backup_nodes: Arc<RwLock<Vec<String>>>,
}

impl FailoverStrategy {
    pub fn new(config: RouterConfig, failover_handler: Arc<FailoverHandler>) -> Self {
        Self {
            config: config.clone(),
            failover_handler,
            primary_nodes: Arc::new(RwLock::new(config.failover.primary_nodes.clone())),
            backup_nodes: Arc::new(RwLock::new(config.failover.backup_nodes.clone())),
        }
    }
}

#[async_trait]
impl RoutingStrategy for FailoverStrategy {
    async fn select_node(
        &self,
        nodes: &[Node],
        task: &Task,
        context: RoutingContext,
    ) -> Result<String> {
        if nodes.is_empty() {
            return Err(anyhow!("No available nodes"));
        }

        // Try primary nodes first
        let primary_nodes = self.primary_nodes.read().await;
        for node_id in primary_nodes.iter() {
            if let Some(node) = nodes.iter().find(|n| &n.id == node_id && n.is_healthy) {
                if node.metrics.capacity.supported_task_types.contains(&task.task_type) {
                    // Check if this node is in failover state
                    if !self.failover_handler.is_node_in_failover(node_id).await {
                        return Ok(node_id.clone());
                    }
                }
            }
        }

        // If no primary nodes available, try backup nodes
        let backup_nodes = self.backup_nodes.read().await;
        for node_id in backup_nodes.iter() {
            if let Some(node) = nodes.iter().find(|n| &n.id == node_id && n.is_healthy) {
                if node.metrics.capacity.supported_task_types.contains(&task.task_type) {
                    // Record failover
                    println!("🔄 Failover from primary to backup node: {}", node_id);
                    return Ok(node_id.clone());
                }
            }
        }

        // If no configured nodes available, fallback to any capable node
        let fallback_nodes: Vec<&Node> = nodes.iter()
            .filter(|n| n.is_healthy)
            .filter(|n| n.metrics.capacity.supported_task_types.contains(&task.task_type))
            .collect();

        if let Some(node) = fallback_nodes.first() {
            println!("🚨 Emergency failover to: {}", node.id);
            return Ok(node.id.clone());
        }

        Err(anyhow!("No available nodes for failover routing. Task type: {}", task.task_type))
    }

    fn name(&self) -> &str {
        "failover"
    }

    fn can_handle_task(&self, task: &Task, nodes: &[Node]) -> bool {
        nodes.iter()
            .filter(|n| n.is_healthy)
            .any(|n| n.metrics.capacity.supported_task_types.contains(&task.task_type))
    }
}

/// Hybrid strategy that combines multiple approaches
#[derive(Debug, Clone)]
pub struct HybridStrategy {
    config: RouterConfig,
    round_robin: RoundRobinStrategy,
    least_loaded: LeastLoadedStrategy,
    latency_aware: LatencyAwareStrategy,
}

impl HybridStrategy {
    pub fn new(config: RouterConfig) -> Self {
        Self {
            config: config.clone(),
            round_robin: RoundRobinStrategy::new(),
            least_loaded: LeastLoadedStrategy::new(config.clone()),
            latency_aware: LatencyAwareStrategy::new(config),
        }
    }

    async fn score_nodes(
        &self,
        nodes: &[Node],
        task: &Task,
        context: &RoutingContext,
    ) -> HashMap<String, f64> {
        let mut scores = HashMap::new();

        for weight in &self.config.hybrid.strategy_weights {
            let strategy_scores = match weight.strategy.as_str() {
                "round_robin" => self.round_robin_score(nodes).await,
                "least_loaded" => self.least_loaded_score(nodes, task).await,
                "latency_aware" => self.latency_aware_score(nodes, task, context).await,
                _ => continue,
            };

            for (node_id, score) in strategy_scores {
                *scores.entry(node_id).or_insert(0.0) += score * weight.weight;
            }
        }

        scores
    }

    async fn round_robin_score(&self, nodes: &[Node]) -> HashMap<String, f64> {
        // Round-robin gives equal scores to all healthy nodes
        nodes.iter()
            .filter(|n| n.is_healthy)
            .map(|n| (n.id.clone(), 1.0))
            .collect()
    }

    async fn least_loaded_score(&self, nodes: &[Node], task: &Task) -> HashMap<String, f64> {
        let mut scores = HashMap::new();

        for node in nodes.iter().filter(|n| n.is_healthy) {
            if node.metrics.capacity.supported_task_types.contains(&task.task_type) {
                let load = self.least_loaded.calculate_load_score(&node.metrics);
                // Lower load = higher score
                scores.insert(node.id.clone(), 1.0 / (load + 0.1));
            }
        }

        scores
    }

    async fn latency_aware_score(
        &self,
        nodes: &[Node],
        task: &Task,
        _context: &RoutingContext,
    ) -> HashMap<String, f64> {
        let mut scores = HashMap::new();

        for node in nodes.iter().filter(|n| n.is_healthy) {
            if node.metrics.capacity.supported_task_types.contains(&task.task_type) {
                let adjusted_latency = self.latency_aware.calculate_adjusted_latency(
                    node.metrics.avg_latency_ms,
                    &node.metrics,
                    task.priority,
                );

                // Lower latency = higher score
                scores.insert(node.id.clone(), 1.0 / (adjusted_latency + 1.0));
            }
        }

        scores
    }
}

#[async_trait]
impl RoutingStrategy for HybridStrategy {
    async fn select_node(
        &self,
        nodes: &[Node],
        task: &Task,
        context: RoutingContext,
    ) -> Result<String> {
        if nodes.is_empty() {
            return Err(anyhow!("No available nodes"));
        }

        // Calculate combined scores
        let scores = self.score_nodes(nodes, task, &context).await;

        if scores.is_empty() {
            return Err(anyhow!("No nodes capable of handling task type: {}", task.task_type));
        }

        // Select node with highest score
        scores.into_iter()
            .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(node_id, _)| node_id)
            .ok_or_else(|| anyhow!("Failed to select node with hybrid strategy"))
    }

    fn name(&self) -> &str {
        "hybrid"
    }

    fn can_handle_task(&self, task: &Task, nodes: &[Node]) -> bool {
        nodes.iter()
            .filter(|n| n.is_healthy)
            .any(|n| n.metrics.capacity.supported_task_types.contains(&task.task_type))
    }
}