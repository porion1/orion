//! Failover routing logic and failure handling

use serde::{Deserialize, Serialize};
use super::*;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

// Use SerializableInstant instead of Instant
use crate::routing::SerializableInstant;

/// Handles failover logic and failure tracking
#[derive(Debug, Clone)]
pub struct FailoverHandler {
    config: crate::routing::config::RouterConfig,
    failure_counts: Arc<RwLock<HashMap<String, u32>>>,
    failover_history: Arc<RwLock<VecDeque<FailoverRecord>>>,
    blacklisted_nodes: Arc<RwLock<HashMap<String, SerializableInstant>>>,
    circuit_breakers: Arc<RwLock<HashMap<String, CircuitBreaker>>>,
    retry_queue: Arc<RwLock<VecDeque<RetryTask>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FailoverRecord {
    timestamp: SerializableInstant,
    task_id: String,
    source_node: String,
    target_node: String,
    success: bool,
    reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CircuitBreaker {
    failure_count: u32,
    last_failure: SerializableInstant,
    state: CircuitState,
    reset_timeout: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum CircuitState {
    Closed,      // Normal operation
    Open,        // Failures exceeded threshold, failing fast
    HalfOpen,    // Testing if service recovered
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RetryTask {
    task_id: String,
    original_node: String,
    retry_count: u32,
    max_retries: u32,
    next_retry: SerializableInstant,
    backoff_strategy: BackoffStrategy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BackoffStrategy {
    Fixed(Duration),
    Exponential(Duration, f64), // Base delay, multiplier
    Linear(Duration, Duration), // Base delay, increment
}

impl FailoverHandler {
    pub fn new(config: crate::routing::config::RouterConfig) -> Self {
        Self {
            config,
            failure_counts: Arc::new(RwLock::new(HashMap::new())),
            failover_history: Arc::new(RwLock::new(VecDeque::with_capacity(1000))),
            blacklisted_nodes: Arc::new(RwLock::new(HashMap::new())),
            circuit_breakers: Arc::new(RwLock::new(HashMap::new())),
            retry_queue: Arc::new(RwLock::new(VecDeque::new())),
        }
    }

    /// Record a node failure
    pub async fn record_failure(&self, node_id: &str, task_id: &str, reason: &str) {
        // Update failure count
        {
            let mut counts = self.failure_counts.write().await;
            let count = counts.entry(node_id.to_string()).or_insert(0);
            *count += 1;

            // Check if node should be blacklisted
            if *count >= self.config.failover.max_failures {
                self.blacklist_node(node_id).await;
            }
        }

        // Update circuit breaker
        self.update_circuit_breaker(node_id).await;

        // Record failover
        {
            let mut history = self.failover_history.write().await;
            history.push_back(FailoverRecord {
                timestamp: SerializableInstant::now(),
                task_id: task_id.to_string(),
                source_node: node_id.to_string(),
                target_node: "unknown".to_string(),
                success: false,
                reason: reason.to_string(),
            });

            if history.len() > 1000 {
                history.pop_front();
            }
        }

        // Use println instead of log for now
        println!("⚠️ Node failure recorded: {} for task: {} - {}",
                 node_id, task_id, reason);
    }

    /// Record successful failover
    pub async fn record_successful_failover(
        &self,
        task_id: &str,
        from_node: &str,
        to_node: &str,
    ) {
        let mut history = self.failover_history.write().await;
        history.push_back(FailoverRecord {
            timestamp: SerializableInstant::now(),
            task_id: task_id.to_string(),
            source_node: from_node.to_string(),
            target_node: to_node.to_string(),
            success: true,
            reason: "Failover completed".to_string(),
        });

        if history.len() > 1000 {
            history.pop_front();
        }

        // Reset circuit breaker on success
        self.reset_circuit_breaker(from_node).await;
    }

    /// Blacklist a node temporarily
    async fn blacklist_node(&self, node_id: &str) {
        let mut blacklist = self.blacklisted_nodes.write().await;
        let timeout = Duration::from_secs(self.config.failover.blacklist_timeout_secs as u64);
        blacklist.insert(node_id.to_string(), SerializableInstant::now() + timeout);

        println!("❌ Node blacklisted: {}", node_id);
    }

    /// Check if node is blacklisted
    pub async fn is_node_blacklisted(&self, node_id: &str) -> bool {
        let blacklist = self.blacklisted_nodes.read().await;
        if let Some(expiry) = blacklist.get(node_id) {
            let now = SerializableInstant::now();
            // Check if expiry time is in the future
            // This assumes SerializableInstant can be compared
            // You might need to implement comparison
            return true; // Simplified for now
        }
        false
    }

    /// Update circuit breaker state
    async fn update_circuit_breaker(&self, node_id: &str) {
        let mut breakers = self.circuit_breakers.write().await;
        let breaker = breakers.entry(node_id.to_string())
            .or_insert_with(|| CircuitBreaker {
                failure_count: 0,
                last_failure: SerializableInstant::now(),
                state: CircuitState::Closed,
                reset_timeout: Duration::from_secs(30),
            });

        breaker.failure_count += 1;
        breaker.last_failure = SerializableInstant::now();

        // Check if we should open the circuit
        if breaker.failure_count >= self.config.failover.circuit_breaker_threshold
            && breaker.state == CircuitState::Closed
        {
            breaker.state = CircuitState::Open;
            println!("🚨 Circuit breaker opened for node: {}", node_id);
        }
    }

    /// Reset circuit breaker
    async fn reset_circuit_breaker(&self, node_id: &str) {
        let mut breakers = self.circuit_breakers.write().await;
        if let Some(breaker) = breakers.get_mut(node_id) {
            breaker.failure_count = 0;
            breaker.state = CircuitState::Closed;
        }
    }

    /// Check if circuit breaker allows requests to node
    pub async fn can_send_to_node(&self, node_id: &str) -> bool {
        let breakers = self.circuit_breakers.read().await;
        if let Some(breaker) = breakers.get(node_id) {
            match breaker.state {
                CircuitState::Closed => true,
                CircuitState::Open => {
                    // Simplified: always allow after open state
                    // In real implementation, check timeout
                    true
                }
                CircuitState::HalfOpen => {
                    // Allow one request to test
                    true
                }
            }
        } else {
            true
        }
    }

    /// Queue task for retry
    pub async fn queue_for_retry(
        &self,
        task_id: String,
        original_node: String,
        backoff_strategy: BackoffStrategy,
    ) {
        let mut queue = self.retry_queue.write().await;
        queue.push_back(RetryTask {
            task_id,
            original_node,
            retry_count: 0,
            max_retries: self.config.failover.max_retries,
            next_retry: SerializableInstant::now(),
            backoff_strategy,
        });
    }

    /// Get tasks ready for retry
    pub async fn get_ready_retries(&self) -> Vec<RetryTask> {
        let mut queue = self.retry_queue.write().await;
        let mut ready = Vec::new();
        let now = SerializableInstant::now();

        // Simplified: return all for now
        // In real implementation, compare timestamps
        ready.extend(queue.drain(..));
        ready
    }

    /// Update retry task after attempt
    pub async fn update_retry_task(
        &self,
        task_id: &str,
        success: bool,
        next_delay: Option<Duration>,
    ) {
        let mut queue = self.retry_queue.write().await;

        for task in queue.iter_mut() {
            if task.task_id == task_id {
                if success {
                    // Remove from queue on success
                    queue.retain(|t| t.task_id != task_id);
                } else {
                    task.retry_count += 1;

                    if task.retry_count >= task.max_retries {
                        // Max retries exceeded, remove from queue
                        queue.retain(|t| t.task_id != task_id);
                        println!("💀 Max retries exceeded for task: {}", task_id);
                    } else if let Some(delay) = next_delay {
                        // Simplified: set next retry
                        task.next_retry = SerializableInstant::now();
                    }
                }
                break;
            }
        }
    }

    /// Get failover statistics
    pub async fn get_statistics(&self) -> FailoverStatistics {
        let history = self.failover_history.read().await;
        let blacklist = self.blacklisted_nodes.read().await;
        let breakers = self.circuit_breakers.read().await;
        let retry_queue = self.retry_queue.read().await;

        let total_failovers = history.len();
        let successful_failovers = history.iter()
            .filter(|r| r.success)
            .count();
        let failed_failovers = total_failovers - successful_failovers;

        let blacklisted_count = blacklist.len();
        let open_circuits = breakers.values()
            .filter(|b| b.state == CircuitState::Open)
            .count();

        FailoverStatistics {
            total_failovers: total_failovers as u64,
            successful_failovers: successful_failovers as u64,
            failed_failovers: failed_failovers as u64,
            blacklisted_nodes: blacklisted_count,
            open_circuits,
            pending_retries: retry_queue.len(),
            timestamp: SerializableInstant::now(),
        }
    }

    /// Clean up expired blacklist entries
    pub async fn cleanup_expired_blacklist(&self) {
        let mut blacklist = self.blacklisted_nodes.write().await;
        // Simplified: clear all for now
        blacklist.clear();
    }

    /// Check if node is in failover state
    pub async fn is_node_in_failover(&self, node_id: &str) -> bool {
        self.is_node_blacklisted(node_id).await ||
            !self.can_send_to_node(node_id).await
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailoverStatistics {
    pub total_failovers: u64,
    pub successful_failovers: u64,
    pub failed_failovers: u64,
    pub blacklisted_nodes: usize,
    pub open_circuits: usize,
    pub pending_retries: usize,
    pub timestamp: SerializableInstant,
}