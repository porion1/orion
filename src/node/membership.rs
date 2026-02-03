use std::sync::Arc;
use std::time::{Duration, SystemTime};
use uuid::Uuid;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum MembershipEvent {
    NodeJoined(Uuid),
    NodeLeft(Uuid),
    NodeFailed(Uuid),
    NodeStatusChanged(Uuid, crate::node::registry::NodeStatus),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct JoinRequest {
    pub node_id: Uuid,
    pub address: std::net::SocketAddr,
    pub capabilities: crate::node::registry::NodeCapabilities,
    pub hostname: String,
    pub version: String,
    pub metadata: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct JoinResponse {
    pub success: bool,
    pub assigned_id: Option<Uuid>,
    pub cluster_info: ClusterInfo,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterInfo {
    pub leader_id: Option<Uuid>,
    pub member_count: usize,
    pub cluster_name: String,
    pub config_version: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LeaveRequest {
    pub node_id: Uuid,
    pub reason: LeaveReason,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum LeaveReason {
    GracefulShutdown,
    Maintenance,
    NetworkPartition,
    Failure,
    Manual,
}

#[derive(Debug, Clone)]
pub struct MembershipConfig {
    pub join_timeout: Duration,
    pub leave_timeout: Duration,
    pub failure_threshold: u32,
    pub min_healthy_nodes: usize,
    pub enable_auto_healing: bool,
}

impl Default for MembershipConfig {
    fn default() -> Self {
        Self {
            join_timeout: Duration::from_secs(30),
            leave_timeout: Duration::from_secs(10),
            failure_threshold: 3,
            min_healthy_nodes: 1,
            enable_auto_healing: true,
        }
    }
}

#[derive(Debug)]
pub struct MembershipManager {
    registry: Arc<crate::node::registry::NodeRegistry>,
    health_scorer: Arc<crate::node::health::HealthScorer>,
    event_sender: flume::Sender<MembershipEvent>,
    event_receiver: flume::Receiver<MembershipEvent>,
    config: MembershipConfig,
    local_node_id: Option<Uuid>,
}

impl MembershipManager {
    pub fn new(
        registry: Arc<crate::node::registry::NodeRegistry>,
        health_scorer: Arc<crate::node::health::HealthScorer>,
        config: MembershipConfig,
    ) -> Self {
        let (event_sender, event_receiver) = flume::unbounded();
        let local_node_id = registry.local_node().map(|node| node.id);

        Self {
            registry,
            health_scorer,
            event_sender,
            event_receiver,
            config,
            local_node_id,
        }
    }

    /// Handle node join request
    pub async fn handle_join(&self, request: JoinRequest) -> Result<JoinResponse, crate::utils::error::OrionError> {
        tracing::info!("Node join request from {} at {}", request.node_id, request.address);

        // Validate join request
        if !self.validate_join_request(&request) {
            return Ok(JoinResponse {
                success: false,
                assigned_id: None,
                cluster_info: self.get_cluster_info(),
                message: "Join request validation failed".to_string(),
            });
        }

        // Check if node already exists
        if self.registry.get(&request.node_id).is_some() {
            return Ok(JoinResponse {
                success: false,
                assigned_id: Some(request.node_id),
                cluster_info: self.get_cluster_info(),
                message: "Node already registered".to_string(),
            });
        }

        // Create node info
        let node_info = crate::node::registry::NodeInfo {
            id: request.node_id,
            address: request.address,
            hostname: request.hostname,
            capabilities: request.capabilities,
            status: crate::node::registry::NodeStatus::Joining,
            last_seen: SystemTime::now(),
            metadata: request.metadata,
            version: request.version,
        };

        // Register node
        self.registry.register(node_info.clone())?;

        // Update status to Active after successful registration
        self.registry.update_status(&request.node_id, crate::node::registry::NodeStatus::Active)?;

        // Send join event
        let _ = self.event_sender.send_async(MembershipEvent::NodeJoined(request.node_id)).await;

        // Calculate initial health score
        let _ = self.health_scorer.calculate_score(&request.node_id);

        tracing::info!("Node {} successfully joined the cluster", request.node_id);

        Ok(JoinResponse {
            success: true,
            assigned_id: Some(request.node_id),
            cluster_info: self.get_cluster_info(),
            message: "Welcome to the cluster".to_string(),
        })
    }

    /// Handle graceful node leave
    pub async fn handle_leave(&self, request: LeaveRequest) -> Result<(), crate::utils::error::OrionError> {
        tracing::info!("Node leave request from {}: {:?}", request.node_id, request.reason);

        // Update node status to Leaving
        self.registry.update_status(&request.node_id, crate::node::registry::NodeStatus::Leaving)?;

        // TODO: Redistribute tasks from leaving node
        // TODO: Wait for task completion

        // Remove node from registry
        self.registry.remove(&request.node_id)?;

        // Send leave event
        let _ = self.event_sender.send_async(MembershipEvent::NodeLeft(request.node_id)).await;

        tracing::info!("Node {} has left the cluster", request.node_id);

        Ok(())
    }

    /// Handle node failure detection
    pub async fn handle_node_failure(&self, node_id: &Uuid) -> Result<(), crate::utils::error::OrionError> {
        tracing::warn!("Node failure detected: {}", node_id);

        // Update node status to Dead
        self.registry.update_status(node_id, crate::node::registry::NodeStatus::Dead)?;

        // Send failure event
        let _ = self.event_sender.send_async(MembershipEvent::NodeFailed(*node_id)).await;

        // TODO: Handle task reassignment for failed node

        tracing::warn!("Node {} marked as dead", node_id);

        Ok(())
    }

    /// Monitor node health and handle failures
    pub async fn monitor_nodes(self: Arc<Self>) {
        tracing::info!("Starting node membership monitor");

        let mut failure_counts = std::collections::HashMap::new();

        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;

            let nodes = self.registry.get_active_nodes();

            for node in nodes {
                // Check if node is still healthy
                if let Some(health_score) = self.health_scorer.get_score(&node.id) {
                    if health_score.score < 30.0 {
                        // Node is unhealthy
                        let count = failure_counts.entry(node.id).or_insert(0);
                        *count += 1;

                        if *count >= self.config.failure_threshold {
                            // Node has failed
                            let _ = self.handle_node_failure(&node.id).await;
                            failure_counts.remove(&node.id);
                        }
                    } else {
                        // Node is healthy, reset failure count
                        failure_counts.remove(&node.id);
                    }
                }
            }

            // Clean up old failure counts
            failure_counts.retain(|node_id, _| {
                self.registry.get(node_id).is_some()
            });
        }
    }

    fn validate_join_request(&self, request: &JoinRequest) -> bool {
        // Check version compatibility
        if !self.is_version_compatible(&request.version) {
            tracing::warn!("Version mismatch for node {}: {}", request.node_id, request.version);
            return false;
        }

        // Check capabilities
        if !self.validate_capabilities(&request.capabilities) {
            tracing::warn!("Invalid capabilities for node {}", request.node_id);
            return false;
        }

        // Additional validation logic...
        true
    }

    fn is_version_compatible(&self, version: &str) -> bool {
        // Simple version compatibility check
        version.starts_with("0.1.")
    }

    fn validate_capabilities(&self, capabilities: &crate::node::registry::NodeCapabilities) -> bool {
        // Validate that node has reasonable capabilities
        capabilities.cpu_cores > 0 &&
            capabilities.memory_mb > 0 &&
            capabilities.max_concurrent_tasks > 0
    }

    pub fn get_cluster_info(&self) -> ClusterInfo {
        let nodes = self.registry.get_active_nodes();

        ClusterInfo {
            leader_id: None, // TODO: Implement leader election
            member_count: nodes.len(),
            cluster_name: "orion-cluster".to_string(),
            config_version: 1,
        }
    }

    pub fn get_event_receiver(&self) -> flume::Receiver<MembershipEvent> {
        self.event_receiver.clone()
    }

    /// Get cluster health status
    pub fn get_cluster_health(&self) -> ClusterHealth {
        let nodes = self.registry.get_active_nodes();
        let healthy_nodes = self.health_scorer.get_healthy_nodes(70.0);

        ClusterHealth {
            total_nodes: nodes.len(),
            healthy_nodes: healthy_nodes.len(),
            unhealthy_nodes: nodes.len() - healthy_nodes.len(),
            is_healthy: healthy_nodes.len() >= self.config.min_healthy_nodes,
        }
    }

    /// Get cluster peers (all nodes except the local node)
    pub fn get_cluster_peers(&self) -> Vec<Uuid> {
        let active_nodes = self.registry.get_active_nodes();

        active_nodes
            .iter()
            .filter_map(|node| {
                // Exclude the local node if we have a local node ID
                if let Some(local_id) = self.local_node_id {
                    if node.id == local_id {
                        return None;
                    }
                }
                Some(node.id)
            })
            .collect()
    }

    /// Get cluster peers with detailed information
    pub fn get_cluster_peers_detailed(&self) -> Vec<crate::node::registry::NodeInfo> {
        let active_nodes = self.registry.get_active_nodes();

        active_nodes
            .into_iter()
            .filter(|node| {
                // Exclude the local node if we have a local node ID
                if let Some(local_id) = self.local_node_id {
                    node.id != local_id
                } else {
                    true
                }
            })
            .collect()
    }

    /// Get healthy cluster peers (above minimum health threshold)
    pub fn get_healthy_cluster_peers(&self, min_health_score: f64) -> Vec<Uuid> {
        let healthy_nodes = self.health_scorer.get_healthy_nodes(min_health_score);

        healthy_nodes
            .into_iter()
            .filter(|node_id| {
                // Exclude the local node if we have a local node ID
                if let Some(local_id) = self.local_node_id {
                    *node_id != local_id
                } else {
                    true
                }
            })
            .collect()
    }

    /// Set the local node ID (should be called after node registration)
    pub fn set_local_node_id(&mut self, node_id: Uuid) {
        self.local_node_id = Some(node_id);
    }

    /// Get the local node ID
    pub fn get_local_node_id(&self) -> Option<Uuid> {
        self.local_node_id
    }

    /// Check if a specific node is a peer (not the local node)
    pub fn is_peer_node(&self, node_id: &Uuid) -> bool {
        match self.local_node_id {
            Some(local_id) => node_id != &local_id,
            None => true, // If we don't know our own ID, consider everyone a peer
        }
    }

    /// Get number of peer nodes in the cluster
    pub fn get_peer_count(&self) -> usize {
        let active_nodes = self.registry.get_active_nodes().len();

        // If we have a local node ID, subtract 1 (the local node)
        if self.local_node_id.is_some() && active_nodes > 0 {
            active_nodes - 1
        } else {
            active_nodes
        }
    }
}

#[derive(Debug)]
pub struct ClusterHealth {
    pub total_nodes: usize,
    pub healthy_nodes: usize,
    pub unhealthy_nodes: usize,
    pub is_healthy: bool,
}