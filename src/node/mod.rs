// CHANGE: All modules are now public
pub mod registry;
pub mod heartbeat;
pub mod health;
pub mod membership;
pub mod classification;

// Public exports
pub use registry::{NodeRegistry, NodeInfo, NodeStatus, NodeCapabilities};
pub use heartbeat::{HeartbeatListener, HeartbeatSender, HeartbeatMessage};
pub use health::{HealthScorer, HealthScore, HealthComponent, HealthTrend, ClusterHealthStats};
pub use membership::{MembershipManager, MembershipEvent, MembershipConfig, ClusterInfo};
pub use classification::{NodeClassification, Classifier, TaskAffinity, NetworkTopology};

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

// Node Manager main struct
#[derive(Debug)]
pub struct NodeManager {
    registry: Arc<NodeRegistry>,
    health_scorer: Arc<HealthScorer>,
    membership_manager: Arc<MembershipManager>,
    classifier: Arc<Classifier>,
    running: AtomicBool,
    // NEW: Distribution-related components
    distribution_metrics: Arc<RwLock<DistributionMetrics>>,
}

// NEW: Metrics for distribution decisions
#[derive(Debug, Clone)]
pub struct DistributionMetrics {
    pub task_assignments: u64,
    pub local_executions: u64,
    pub remote_executions: u64,
    pub failed_assignments: u64,
    pub average_assignment_latency_ms: f64,
    pub node_utilization: Vec<NodeUtilization>,
}

#[derive(Debug, Clone)]
pub struct NodeUtilization {
    pub node_id: uuid::Uuid,
    pub cpu_utilization: f64,
    pub memory_utilization: f64,
    pub task_count: u32,
    pub max_capacity: u32,
}

// Configuration for Node Manager (separate from ClusterConfig)
#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub node_id: Option<uuid::Uuid>,
    pub listen_address: std::net::SocketAddr,
    pub heartbeat_interval_ms: u64,
    pub persistence_path: Option<String>,
    pub cluster_peers: Vec<std::net::SocketAddr>,
    // NEW: Distribution-specific configuration
    pub enable_distribution: bool,
    pub distribution_metrics_interval_ms: u64,
    pub default_task_affinity: Option<String>,
}

impl NodeManager {
    pub fn new(config: NodeConfig) -> Result<Self, crate::utils::error::OrionError> {
        let registry = Arc::new(NodeRegistry::new(
            config.persistence_path.as_deref()
        )?);

        let health_scorer = Arc::new(HealthScorer::new(registry.clone()));

        let membership_manager = Arc::new(MembershipManager::new(
            registry.clone(),
            health_scorer.clone(),
            MembershipConfig::default(),
        ));

        let local_node_id = config.node_id.unwrap_or_else(uuid::Uuid::new_v4);
        let classifier = Arc::new(Classifier::new(local_node_id));

        // NEW: Initialize distribution metrics
        let distribution_metrics = Arc::new(RwLock::new(DistributionMetrics {
            task_assignments: 0,
            local_executions: 0,
            remote_executions: 0,
            failed_assignments: 0,
            average_assignment_latency_ms: 0.0,
            node_utilization: Vec::new(),
        }));

        Ok(Self {
            registry,
            health_scorer,
            membership_manager,
            classifier,
            running: AtomicBool::new(false),
            distribution_metrics,
        })
    }

    pub fn start(&self) -> Result<(), crate::utils::error::OrionError> {
        self.running.store(true, Ordering::SeqCst);

        // NEW: Start distribution metrics collection if enabled
        if self.is_distribution_enabled() {
            self.start_distribution_metrics_collection();
        }

        Ok(())
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    pub fn registry(&self) -> Arc<NodeRegistry> {
        self.registry.clone()
    }

    pub fn health_scorer(&self) -> Arc<HealthScorer> {
        self.health_scorer.clone()
    }

    pub fn membership_manager(&self) -> Arc<MembershipManager> {
        self.membership_manager.clone()
    }

    pub fn classifier(&self) -> Arc<Classifier> {
        self.classifier.clone()
    }

    // NEW: Distribution-related methods

    /// Check if distribution features are enabled
    pub fn is_distribution_enabled(&self) -> bool {
        // For now, check if we have cluster peers
        // In a real implementation, this would check the config
        !self.membership_manager.get_cluster_peers().is_empty()
    }

    /// Get distribution metrics
    pub async fn get_distribution_metrics(&self) -> DistributionMetrics {
        self.distribution_metrics.read().await.clone()
    }

    /// Update task assignment metrics
    pub async fn record_task_assignment(&self, is_local: bool, success: bool, latency_ms: f64) {
        let mut metrics = self.distribution_metrics.write().await;
        metrics.task_assignments += 1;

        if success {
            if is_local {
                metrics.local_executions += 1;
            } else {
                metrics.remote_executions += 1;
            }
        } else {
            metrics.failed_assignments += 1;
        }

        // Update average latency (moving average)
        let total_latency = metrics.average_assignment_latency_ms * (metrics.task_assignments - 1) as f64;
        metrics.average_assignment_latency_ms = (total_latency + latency_ms) / metrics.task_assignments as f64;
    }

    /// Update node utilization metrics
    pub async fn update_node_utilization(&self, utilization: Vec<NodeUtilization>) {
        let mut metrics = self.distribution_metrics.write().await;
        metrics.node_utilization = utilization;
    }

    /// Get nodes suitable for task distribution
    pub async fn get_distribution_candidates(&self, min_health_score: f64) -> Vec<NodeInfo> {
        let healthy_nodes = self.health_scorer.get_healthy_nodes(min_health_score);
        let mut candidates = Vec::new();

        for node_id in healthy_nodes {
            if let Some(node_info) = self.registry.get(&node_id) {
                // Additional filtering could be added here
                if node_info.status == NodeStatus::Active {
                    candidates.push(node_info);
                }
            }
        }

        candidates
    }

    /// Start background task for collecting distribution metrics
    fn start_distribution_metrics_collection(&self) {
        let manager_clone = Arc::new(self.clone());

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));

            loop {
                interval.tick().await;

                if !manager_clone.is_running() {
                    break;
                }

                // Collect and update node utilization
                if let Ok(utilization) = manager_clone.collect_node_utilization().await {
                    manager_clone.update_node_utilization(utilization).await;
                }
            }
        });
    }

    /// Collect node utilization data
    async fn collect_node_utilization(&self) -> Result<Vec<NodeUtilization>, crate::utils::error::OrionError> {
        let active_nodes = self.registry.get_active_nodes();
        let mut utilization = Vec::new();

        // FIX: Iterate over references instead of moving ownership
        for node in &active_nodes {
            // In a real implementation, this would collect actual utilization metrics
            // from heartbeat data or monitoring system
            let node_util = NodeUtilization {
                node_id: node.id,
                cpu_utilization: 0.5, // Placeholder
                memory_utilization: 0.3, // Placeholder
                task_count: 0, // Would come from task registry
                max_capacity: node.capabilities.max_concurrent_tasks,
            };

            utilization.push(node_util);
        }

        Ok(utilization)
    }

    /// NEW: Get overall cluster capacity
    pub async fn get_cluster_capacity(&self) -> ClusterCapacity {
        let active_nodes = self.registry.get_active_nodes();
        let mut total_cpu = 0;
        let mut total_memory = 0;
        let mut total_storage = 0;
        let mut total_task_capacity = 0;

        // FIX: Same issue here - iterate over references
        for node in &active_nodes {
            total_cpu += node.capabilities.cpu_cores;
            total_memory += node.capabilities.memory_mb;
            total_storage += node.capabilities.storage_mb;
            total_task_capacity += node.capabilities.max_concurrent_tasks;
        }

        ClusterCapacity {
            total_nodes: active_nodes.len(),
            total_cpu_cores: total_cpu,
            total_memory_mb: total_memory,
            total_storage_mb: total_storage,
            total_task_capacity,
        }
    }

    /// NEW: Check if cluster can handle a task with given requirements
    pub async fn can_handle_task(&self, cpu_cores: u32, memory_mb: u64, storage_mb: u64) -> bool {
        let candidates = self.get_distribution_candidates(70.0).await;

        candidates.iter().any(|node| {
            node.capabilities.cpu_cores >= cpu_cores &&
                node.capabilities.memory_mb >= memory_mb &&
                node.capabilities.storage_mb >= storage_mb
        })
    }

    /// NEW: Get recommended nodes for a task
    pub async fn get_recommended_nodes(&self,
                                       cpu_cores: u32,
                                       memory_mb: u64,
                                       storage_mb: u64,
                                       required_task_types: &[String],
                                       min_health_score: f64
    ) -> Vec<NodeInfo> {
        let candidates = self.get_distribution_candidates(min_health_score).await;

        candidates.into_iter()
            .filter(|node| {
                // Check basic resource requirements
                let caps = &node.capabilities;
                if caps.cpu_cores < cpu_cores ||
                    caps.memory_mb < memory_mb ||
                    caps.storage_mb < storage_mb {
                    return false;
                }

                // Check task type support if required
                if !required_task_types.is_empty() {
                    let has_required_type = required_task_types.iter().any(|req_type| {
                        caps.supported_task_types.contains(req_type)
                    });
                    if !has_required_type {
                        return false;
                    }
                }

                true
            })
            .collect()
    }

    /// NEW: Reset distribution metrics
    pub async fn reset_distribution_metrics(&self) {
        let mut metrics = self.distribution_metrics.write().await;
        *metrics = DistributionMetrics {
            task_assignments: 0,
            local_executions: 0,
            remote_executions: 0,
            failed_assignments: 0,
            average_assignment_latency_ms: 0.0,
            node_utilization: Vec::new(),
        };
    }

    /// NEW: Get distribution statistics
    pub async fn get_distribution_stats(&self) -> DistributionStats {
        let metrics = self.distribution_metrics.read().await;
        let capacity = self.get_cluster_capacity().await;
        let candidates = self.get_distribution_candidates(70.0).await.len();

        DistributionStats {
            total_task_assignments: metrics.task_assignments,
            local_execution_rate: if metrics.task_assignments > 0 {
                metrics.local_executions as f64 / metrics.task_assignments as f64 * 100.0
            } else {
                0.0
            },
            remote_execution_rate: if metrics.task_assignments > 0 {
                metrics.remote_executions as f64 / metrics.task_assignments as f64 * 100.0
            } else {
                0.0
            },
            success_rate: if metrics.task_assignments > 0 {
                (metrics.task_assignments - metrics.failed_assignments) as f64 /
                    metrics.task_assignments as f64 * 100.0
            } else {
                0.0
            },
            average_latency_ms: metrics.average_assignment_latency_ms,
            available_nodes: candidates,
            cluster_capacity: capacity,
        }
    }
}

// NEW: Cluster capacity information
#[derive(Debug, Clone)]
pub struct ClusterCapacity {
    pub total_nodes: usize,
    pub total_cpu_cores: u32,
    pub total_memory_mb: u64,
    pub total_storage_mb: u64,
    pub total_task_capacity: u32,
}

// NEW: Distribution statistics
#[derive(Debug, Clone)]
pub struct DistributionStats {
    pub total_task_assignments: u64,
    pub local_execution_rate: f64,
    pub remote_execution_rate: f64,
    pub success_rate: f64,
    pub average_latency_ms: f64,
    pub available_nodes: usize,
    pub cluster_capacity: ClusterCapacity,
}

impl Clone for NodeManager {
    fn clone(&self) -> Self {
        Self {
            registry: self.registry.clone(),
            health_scorer: self.health_scorer.clone(),
            membership_manager: self.membership_manager.clone(),
            classifier: self.classifier.clone(),
            running: AtomicBool::new(self.running.load(Ordering::SeqCst)),
            distribution_metrics: self.distribution_metrics.clone(),
        }
    }
}

// Add a conversion from ClusterConfig to NodeConfig
impl From<crate::engine::config::ClusterConfig> for NodeConfig {
    fn from(cluster_config: crate::engine::config::ClusterConfig) -> Self {
        Self {
            node_id: cluster_config.node_id,
            listen_address: cluster_config.node_listen_addr,
            heartbeat_interval_ms: cluster_config.heartbeat_interval_ms,
            persistence_path: cluster_config.node_persistence_path,
            cluster_peers: cluster_config.cluster_peers,
            // NEW: Default distribution settings
            enable_distribution: true,
            distribution_metrics_interval_ms: 30000,
            default_task_affinity: None,
        }
    }
}

// NEW: Default implementation for NodeConfig
impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            node_id: None,
            listen_address: "127.0.0.1:8080".parse().unwrap(),
            heartbeat_interval_ms: 5000,
            persistence_path: None,
            cluster_peers: Vec::new(),
            enable_distribution: false,
            distribution_metrics_interval_ms: 30000,
            default_task_affinity: None,
        }
    }
}