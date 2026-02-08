//! Metrics collection and analysis for load balancing

use serde::{Deserialize, Serialize};
use super::*;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

// Use SerializableInstant instead of Instant
use crate::routing::SerializableInstant;

/// Collects and analyzes routing metrics
#[derive(Debug, Clone)]
pub struct MetricsCollector {
    total_selections: Arc<AtomicU64>,
    successful_selections: Arc<AtomicU64>,
    failed_selections: Arc<AtomicU64>,
    strategy_counts: Arc<RwLock<HashMap<String, AtomicU64>>>,
    node_selection_counts: Arc<RwLock<HashMap<String, AtomicU64>>>,
    latency_history: Arc<RwLock<VecDeque<(SerializableInstant, Duration)>>>,
    failover_events: Arc<RwLock<Vec<FailoverEvent>>>,
    node_metrics_history: Arc<RwLock<HashMap<String, VecDeque<NodeMetricsSnapshot>>>>,
    statistics: Arc<RwLock<RoutingStatistics>>,
    max_history_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NodeMetricsSnapshot {
    timestamp: SerializableInstant,
    metrics: NodeMetrics,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailoverEvent {
    pub timestamp: SerializableInstant,
    pub task_id: String,
    pub from_nodes: Vec<String>,
    pub to_node: String,
    pub reason: FailoverReason,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FailoverReason {
    PrimaryUnavailable,
    NodeUnhealthy,
    CapacityExceeded,
    LatencyThreshold,
    ManualOverride,
    Emergency,
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self {
            total_selections: Arc::new(AtomicU64::new(0)),
            successful_selections: Arc::new(AtomicU64::new(0)),
            failed_selections: Arc::new(AtomicU64::new(0)),
            strategy_counts: Arc::new(RwLock::new(HashMap::new())),
            node_selection_counts: Arc::new(RwLock::new(HashMap::new())),
            latency_history: Arc::new(RwLock::new(VecDeque::with_capacity(1000))),
            failover_events: Arc::new(RwLock::new(Vec::new())),
            node_metrics_history: Arc::new(RwLock::new(HashMap::new())),
            statistics: Arc::new(RwLock::new(RoutingStatistics {
                total_selections: 0,
                successful_selections: 0,
                failed_selections: 0,
                avg_selection_latency_ms: 0.0,
                strategy_distribution: HashMap::new(),
                node_selection_counts: HashMap::new(),
                failover_count: 0,
                timestamp: SerializableInstant::now(),
            })),
            max_history_size: 1000,
        }
    }

    /// Record a node selection event
    pub async fn record_selection(
        &self,
        strategy_name: &str,
        result: &Result<String, crate::routing::Error>,
        latency: Duration,
        available_nodes: usize,
    ) {
        self.total_selections.fetch_add(1, Ordering::SeqCst);

        match result {
            Ok(node_id) => {
                self.successful_selections.fetch_add(1, Ordering::SeqCst);

                // Update strategy count
                {
                    let mut strategies = self.strategy_counts.write().await;
                    let counter = strategies
                        .entry(strategy_name.to_string())
                        .or_insert_with(|| AtomicU64::new(0));
                    counter.fetch_add(1, Ordering::SeqCst);
                }

                // Update node selection count
                {
                    let mut node_counts = self.node_selection_counts.write().await;
                    let counter = node_counts
                        .entry(node_id.clone())
                        .or_insert_with(|| AtomicU64::new(0));
                    counter.fetch_add(1, Ordering::SeqCst);
                }
            }
            Err(_) => {
                self.failed_selections.fetch_add(1, Ordering::SeqCst);
            }
        }

        // Record latency
        {
            let mut history = self.latency_history.write().await;
            history.push_back((SerializableInstant::now(), latency));
            if history.len() > self.max_history_size {
                history.pop_front();
            }
        }

        // Update statistics
        self.update_statistics().await;
    }

    /// Record a strategy change
    pub async fn record_strategy_change(&self, new_strategy: &str) {
        // Use println instead of log for now
        println!("📊 Routing strategy changed to: {}", new_strategy);
    }

    /// Update node metrics history
    pub async fn update_node_metrics(&self, node_id: &str, metrics: NodeMetrics) {
        let mut history = self.node_metrics_history.write().await;
        let entry = history.entry(node_id.to_string())
            .or_insert_with(|| VecDeque::with_capacity(100));

        entry.push_back(NodeMetricsSnapshot {
            timestamp: SerializableInstant::now(),
            metrics,
        });

        if entry.len() > 100 {
            entry.pop_front();
        }
    }

    /// Record a failover event
    pub async fn record_failover(
        &self,
        task_id: &str,
        from_nodes: &[String],
        to_node: &str,
    ) {
        let mut events = self.failover_events.write().await;
        events.push(FailoverEvent {
            timestamp: SerializableInstant::now(),
            task_id: task_id.to_string(),
            from_nodes: from_nodes.to_vec(),
            to_node: to_node.to_string(),
            reason: FailoverReason::PrimaryUnavailable,
        });

        if events.len() > 1000 {
            events.remove(0);
        }
    }

    /// Record an emergency failover
    pub async fn record_emergency_failover(
        &self,
        task_id: &str,
        from_nodes: &[String],
        to_node: &str,
    ) {
        let mut events = self.failover_events.write().await;
        events.push(FailoverEvent {
            timestamp: SerializableInstant::now(),
            task_id: task_id.to_string(),
            from_nodes: from_nodes.to_vec(),
            to_node: to_node.to_string(),
            reason: FailoverReason::Emergency,
        });

        if events.len() > 1000 {
            events.remove(0);
        }

        println!("🚨 Emergency failover recorded for task: {}", task_id);
    }

    /// Update aggregated statistics
    async fn update_statistics(&self) {
        let total = self.total_selections.load(Ordering::SeqCst);
        let successful = self.successful_selections.load(Ordering::SeqCst);
        let failed = self.failed_selections.load(Ordering::SeqCst);

        // Calculate average latency
        let history = self.latency_history.read().await;
        let avg_latency = if !history.is_empty() {
            let sum: Duration = history.iter().map(|(_, d)| *d).sum();
            sum.as_secs_f64() * 1000.0 / history.len() as f64
        } else {
            0.0
        };

        // Get strategy distribution
        let strategy_distribution = {
            let strategies = self.strategy_counts.read().await;
            strategies.iter()
                .map(|(k, v)| (k.clone(), v.load(Ordering::SeqCst)))
                .collect()
        };

        // Get node selection counts
        let node_selection_counts = {
            let node_counts = self.node_selection_counts.read().await;
            node_counts.iter()
                .map(|(k, v)| (k.clone(), v.load(Ordering::SeqCst)))
                .collect()
        };

        // Get failover count
        let failover_count = {
            let events = self.failover_events.read().await;
            events.len() as u64
        };

        let mut stats = self.statistics.write().await;
        *stats = RoutingStatistics {
            total_selections: total,
            successful_selections: successful,
            failed_selections: failed,
            avg_selection_latency_ms: avg_latency,
            strategy_distribution,
            node_selection_counts,
            failover_count,
            timestamp: SerializableInstant::now(),
        };
    }

    /// Get current statistics
    pub async fn get_statistics(&self) -> RoutingStatistics {
        self.update_statistics().await;
        self.statistics.read().await.clone()
    }

    /// Get node metrics history
    pub async fn get_node_metrics_history(
        &self,
        node_id: &str,
        duration: Option<Duration>,
    ) -> Vec<NodeMetricsSnapshot> {
        let history = self.node_metrics_history.read().await;
        if let Some(snapshots) = history.get(node_id) {
            if let Some(duration) = duration {
                let cutoff = SerializableInstant::now();
                // Simplified: return all snapshots for now
                // In real implementation, compare timestamps
                snapshots.iter().cloned().collect()
            } else {
                snapshots.iter().cloned().collect()
            }
        } else {
            Vec::new()
        }
    }

    /// Get failover events within time window
    pub async fn get_failover_events(
        &self,
        duration: Duration,
    ) -> Vec<FailoverEvent> {
        let events = self.failover_events.read().await;
        // Simplified: return all events for now
        // In real implementation, filter by timestamp
        events.iter().cloned().collect()
    }

    /// Calculate load distribution efficiency
    pub async fn calculate_load_efficiency(&self) -> f64 {
        let stats = self.get_statistics().await;

        if stats.node_selection_counts.is_empty() {
            return 1.0;
        }

        let counts: Vec<u64> = stats.node_selection_counts.values().cloned().collect();
        let mean = counts.iter().sum::<u64>() as f64 / counts.len() as f64;

        if mean == 0.0 {
            return 1.0;
        }

        let variance = counts.iter()
            .map(|&x| (x as f64 - mean).powi(2))
            .sum::<f64>() / counts.len() as f64;

        let std_dev = variance.sqrt();

        // Efficiency is higher when standard deviation is lower (more balanced)
        1.0 / (1.0 + std_dev / mean)
    }

    /// Get recommendation for optimal strategy based on metrics
    pub async fn get_strategy_recommendation(&self) -> String {
        let stats = self.get_statistics().await;

        if stats.failed_selections > stats.total_selections / 10 {
            // High failure rate, try failover strategy
            return "failover".to_string();
        }

        let efficiency = self.calculate_load_efficiency().await;
        if efficiency < 0.7 {
            // Poor load distribution, use least loaded
            return "least_loaded".to_string();
        }

        // Check if latency is a concern
        if stats.avg_selection_latency_ms > 50.0 {
            return "latency_aware".to_string();
        }

        // Default to hybrid for balanced approach
        "hybrid".to_string()
    }
}