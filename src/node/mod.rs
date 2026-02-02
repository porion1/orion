mod registry;
mod heartbeat;
mod health;
mod membership;
mod classification;

// Public exports
pub use registry::{NodeRegistry, NodeInfo, NodeStatus, NodeCapabilities};
pub use heartbeat::{HeartbeatListener, HeartbeatSender, HeartbeatMessage};
pub use health::{HealthScorer, HealthScore, HealthComponent, HealthTrend};
pub use membership::{MembershipManager, MembershipEvent, MembershipConfig, ClusterInfo};
pub use classification::{NodeClassification, Classifier, TaskAffinity, NetworkTopology};

use std::sync::atomic::{AtomicBool, Ordering};

// Node Manager main struct
#[derive(Debug)]
pub struct NodeManager {
    registry: std::sync::Arc<NodeRegistry>,
    health_scorer: std::sync::Arc<HealthScorer>,
    membership_manager: std::sync::Arc<MembershipManager>,
    classifier: std::sync::Arc<Classifier>,
    running: AtomicBool,
}

// Configuration for Node Manager (separate from ClusterConfig)
#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub node_id: Option<uuid::Uuid>,
    pub listen_address: std::net::SocketAddr,
    pub heartbeat_interval_ms: u64,
    pub persistence_path: Option<String>,
    pub cluster_peers: Vec<std::net::SocketAddr>,
}

impl NodeManager {
    pub fn new(config: NodeConfig) -> Result<Self, crate::utils::error::OrionError> {
        let registry = std::sync::Arc::new(NodeRegistry::new(
            config.persistence_path.as_deref()
        )?);

        let health_scorer = std::sync::Arc::new(HealthScorer::new(registry.clone()));

        let membership_manager = std::sync::Arc::new(MembershipManager::new(
            registry.clone(),
            health_scorer.clone(),
            MembershipConfig::default(),
        ));

        let local_node_id = config.node_id.unwrap_or_else(uuid::Uuid::new_v4);
        let classifier = std::sync::Arc::new(Classifier::new(local_node_id));

        Ok(Self {
            registry,
            health_scorer,
            membership_manager,
            classifier,
            running: AtomicBool::new(false),
        })
    }

    pub fn start(&self) -> Result<(), crate::utils::error::OrionError> {
        self.running.store(true, Ordering::SeqCst);
        Ok(())
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    pub fn registry(&self) -> std::sync::Arc<NodeRegistry> {
        self.registry.clone()
    }

    pub fn health_scorer(&self) -> std::sync::Arc<HealthScorer> {
        self.health_scorer.clone()
    }

    pub fn membership_manager(&self) -> std::sync::Arc<MembershipManager> {
        self.membership_manager.clone()
    }

    pub fn classifier(&self) -> std::sync::Arc<Classifier> {
        self.classifier.clone()
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
        }
    }
}