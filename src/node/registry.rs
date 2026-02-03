use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::net::SocketAddr;
use std::time::{Duration, SystemTime};
use uuid::Uuid;

/// Node information stored in the registry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: Uuid,
    pub address: SocketAddr,
    pub hostname: String,
    pub capabilities: NodeCapabilities,
    pub status: NodeStatus,
    pub last_seen: SystemTime,
    pub metadata: serde_json::Value,
    pub version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum NodeStatus {
    Joining,
    Active,
    Unhealthy,
    Leaving,
    Dead,
}

// Add Display implementation for NodeStatus
impl fmt::Display for NodeStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NodeStatus::Joining => write!(f, "Joining"),
            NodeStatus::Active => write!(f, "Active"),
            NodeStatus::Unhealthy => write!(f, "Unhealthy"),
            NodeStatus::Leaving => write!(f, "Leaving"),
            NodeStatus::Dead => write!(f, "Dead"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeCapabilities {
    pub cpu_cores: u32,
    pub memory_mb: u64,
    pub storage_mb: u64,
    pub max_concurrent_tasks: u32,
    pub supported_task_types: Vec<String>,
}

impl Default for NodeCapabilities {
    fn default() -> Self {
        Self {
            cpu_cores: 4,
            memory_mb: 8192,
            storage_mb: 102400,
            max_concurrent_tasks: 10,
            supported_task_types: vec!["default".to_string()],
        }
    }
}

/// Thread-safe node registry
#[derive(Debug)]
pub struct NodeRegistry {
    nodes: DashMap<Uuid, NodeInfo>,
    persistence: Option<sled::Db>,
    local_node_id: Uuid,
    // Cache for frequently accessed data
    active_nodes_cache: DashMap<Uuid, SystemTime>,
}

impl NodeRegistry {
    pub fn new(persistence_path: Option<&str>) -> Result<Self, crate::utils::error::OrionError> {
        let persistence = persistence_path.map(|path| {
            sled::open(path).map_err(|e| crate::utils::error::OrionError::PersistenceError(e.to_string()))
        }).transpose()?;

        Ok(Self {
            nodes: DashMap::new(),
            persistence,
            local_node_id: Uuid::new_v4(),
            // Initialize cache
            active_nodes_cache: DashMap::new(),
        })
    }

    /// Register a new node or update existing node
    pub fn register(&self, node_info: NodeInfo) -> Result<(), crate::utils::error::OrionError> {
        let id = node_info.id;

        // Store in memory
        self.nodes.insert(id, node_info.clone());

        // Update cache if node is active
        if node_info.status == NodeStatus::Active {
            self.active_nodes_cache.insert(id, SystemTime::now());
        } else {
            self.active_nodes_cache.remove(&id);
        }

        // Persist to disk if configured
        if let Some(db) = &self.persistence {
            let key = id.as_bytes().to_vec();
            let value = bincode::serialize(&node_info)
                .map_err(|e| crate::utils::error::OrionError::SerializationError(e.to_string()))?;
            db.insert(key, value)
                .map_err(|e| crate::utils::error::OrionError::PersistenceError(e.to_string()))?;
        }

        // Emit metrics
        metrics::gauge!("orion.nodes.registered", self.nodes.len() as f64);

        Ok(())
    }

    /// Get node by ID
    pub fn get(&self, node_id: &Uuid) -> Option<NodeInfo> {
        self.nodes.get(node_id).map(|entry| entry.value().clone())
    }

    /// Get all active nodes
    pub fn get_active_nodes(&self) -> Vec<NodeInfo> {
        self.active_nodes_cache.iter()
            .filter_map(|entry| {
                self.nodes.get(entry.key()).map(|node| node.value().clone())
            })
            .collect()
    }

    /// Get nodes by status
    pub fn get_nodes_by_status(&self, status: NodeStatus) -> Vec<NodeInfo> {
        self.nodes.iter()
            .filter(|entry| entry.value().status == status)
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Get nodes that support specific task types
    pub fn get_nodes_supporting_task_type(&self, task_type: &str) -> Vec<NodeInfo> {
        self.nodes.iter()
            .filter(|entry| {
                entry.value().capabilities.supported_task_types.contains(&task_type.to_string())
            })
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Get nodes with minimum capabilities
    pub fn get_nodes_with_capabilities(&self, min_cpu_cores: u32, min_memory_mb: u64, min_storage_mb: u64) -> Vec<NodeInfo> {
        self.nodes.iter()
            .filter(|entry| {
                let caps = &entry.value().capabilities;
                caps.cpu_cores >= min_cpu_cores &&
                    caps.memory_mb >= min_memory_mb &&
                    caps.storage_mb >= min_storage_mb
            })
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Remove node from registry
    pub fn remove(&self, node_id: &Uuid) -> Result<(), crate::utils::error::OrionError> {
        self.nodes.remove(node_id);
        self.active_nodes_cache.remove(node_id);

        if let Some(db) = &self.persistence {
            let key = node_id.as_bytes().to_vec();
            db.remove(key)
                .map_err(|e| crate::utils::error::OrionError::PersistenceError(e.to_string()))?;
        }

        Ok(())
    }

    /// Update node status
    pub fn update_status(&self, node_id: &Uuid, status: NodeStatus) -> Result<(), crate::utils::error::OrionError> {
        if let Some(mut entry) = self.nodes.get_mut(node_id) {
            entry.status = status.clone(); // Clone the status enum
            entry.last_seen = SystemTime::now();

            // Update cache
            if status == NodeStatus::Active {
                self.active_nodes_cache.insert(*node_id, SystemTime::now());
            } else {
                self.active_nodes_cache.remove(node_id);
            }

            // Update persistence if needed
            if let Some(db) = &self.persistence {
                let key = node_id.as_bytes().to_vec();
                let value = bincode::serialize(&*entry)
                    .map_err(|e| crate::utils::error::OrionError::SerializationError(e.to_string()))?;
                db.insert(key, value)
                    .map_err(|e| crate::utils::error::OrionError::PersistenceError(e.to_string()))?;
            }
        }

        Ok(())
    }

    /// Update node capabilities
    pub fn update_capabilities(&self, node_id: &Uuid, capabilities: NodeCapabilities) -> Result<(), crate::utils::error::OrionError> {
        if let Some(mut entry) = self.nodes.get_mut(node_id) {
            entry.capabilities = capabilities;
            entry.last_seen = SystemTime::now();

            if let Some(db) = &self.persistence {
                let key = node_id.as_bytes().to_vec();
                let value = bincode::serialize(&*entry)
                    .map_err(|e| crate::utils::error::OrionError::SerializationError(e.to_string()))?;
                db.insert(key, value)
                    .map_err(|e| crate::utils::error::OrionError::PersistenceError(e.to_string()))?;
            }
        }

        Ok(())
    }

    /// Get local node info
    pub fn local_node(&self) -> Option<NodeInfo> {
        self.get(&self.local_node_id)
    }

    /// Set local node info (used during node initialization)
    pub fn set_local_node(&self, node_info: NodeInfo) -> Result<(), crate::utils::error::OrionError> {
        // Store the local node ID
        // Note: In a real implementation, you might want to validate that this node_id matches
        // what was generated during NodeRegistry::new()

        // Register the node normally
        self.register(node_info)
    }

    /// Get the local node ID
    pub fn local_node_id(&self) -> Uuid {
        self.local_node_id
    }

    /// Clean up dead nodes older than threshold
    pub fn cleanup_dead_nodes(&self, threshold: Duration) -> Vec<Uuid> {
        let now = SystemTime::now();
        let mut to_remove = Vec::new();

        for entry in self.nodes.iter() {
            if entry.value().status == NodeStatus::Dead {
                if let Ok(elapsed) = now.duration_since(entry.value().last_seen) {
                    if elapsed > threshold {
                        to_remove.push(entry.value().id);
                    }
                }
            }
        }

        for id in &to_remove {
            let _ = self.remove(id);
        }

        to_remove
    }

    /// Get number of registered nodes
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Check if registry is empty
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Get all nodes
    pub fn get_all_nodes(&self) -> Vec<NodeInfo> {
        self.nodes.iter().map(|entry| entry.value().clone()).collect()
    }

    /// Check if node exists
    pub fn contains(&self, node_id: &Uuid) -> bool {
        self.nodes.contains_key(node_id)
    }

    /// Get node count by status
    pub fn count_by_status(&self, status: NodeStatus) -> usize {
        self.nodes.iter()
            .filter(|entry| entry.value().status == status)
            .count()
    }

    /// Get total available resources across all active nodes
    pub fn get_total_resources(&self) -> (u32, u64, u64) {
        let mut total_cpu = 0;
        let mut total_memory = 0;
        let mut total_storage = 0;

        for entry in self.active_nodes_cache.iter() {
            if let Some(node) = self.nodes.get(entry.key()) {
                total_cpu += node.capabilities.cpu_cores;
                total_memory += node.capabilities.memory_mb;
                total_storage += node.capabilities.storage_mb;
            }
        }

        (total_cpu, total_memory, total_storage)
    }

    /// Find nodes that can handle a task with given requirements
    pub fn find_nodes_for_task(&self, min_cpu_cores: u32, min_memory_mb: u64, min_storage_mb: u64, required_task_types: &[String]) -> Vec<NodeInfo> {
        self.get_active_nodes().into_iter()
            .filter(|node| {
                // Check basic resource requirements
                let caps = &node.capabilities;
                if caps.cpu_cores < min_cpu_cores ||
                    caps.memory_mb < min_memory_mb ||
                    caps.storage_mb < min_storage_mb {
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

    /// Refresh active nodes cache (called periodically)
    pub fn refresh_active_cache(&self) {
        let now = SystemTime::now();
        let mut to_remove = Vec::new();

        // Remove nodes from cache that are no longer active
        for entry in self.active_nodes_cache.iter() {
            if let Some(node) = self.nodes.get(entry.key()) {
                if node.status != NodeStatus::Active {
                    to_remove.push(*entry.key());
                }
            } else {
                to_remove.push(*entry.key());
            }
        }

        for node_id in to_remove {
            self.active_nodes_cache.remove(&node_id);
        }

        // Add newly active nodes to cache
        for entry in self.nodes.iter() {
            if entry.value().status == NodeStatus::Active &&
                !self.active_nodes_cache.contains_key(&entry.value().id) {
                self.active_nodes_cache.insert(entry.value().id, now);
            }
        }
    }

    /// Get node metadata field
    pub fn get_node_metadata(&self, node_id: &Uuid, key: &str) -> Option<String> {
        self.nodes.get(node_id)
            .and_then(|node| {
                node.metadata.get(key)
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
    }

    /// Set node metadata field
    pub fn set_node_metadata(&self, node_id: &Uuid, key: &str, value: &str) -> Result<(), crate::utils::error::OrionError> {
        if let Some(mut entry) = self.nodes.get_mut(node_id) {
            if let Some(obj) = entry.metadata.as_object_mut() {
                obj.insert(key.to_string(), serde_json::Value::String(value.to_string()));
            } else {
                let mut map = serde_json::Map::new();
                map.insert(key.to_string(), serde_json::Value::String(value.to_string()));
                entry.metadata = serde_json::Value::Object(map);
            }
            entry.last_seen = SystemTime::now();

            // Update persistence
            if let Some(db) = &self.persistence {
                let key_bytes = node_id.as_bytes().to_vec();
                let value_bytes = bincode::serialize(&*entry)
                    .map_err(|e| crate::utils::error::OrionError::SerializationError(e.to_string()))?;
                db.insert(key_bytes, value_bytes)
                    .map_err(|e| crate::utils::error::OrionError::PersistenceError(e.to_string()))?;
            }
        }

        Ok(())
    }

    /// Get nodes sorted by last seen time (most recent first)
    pub fn get_nodes_by_last_seen(&self) -> Vec<NodeInfo> {
        let mut nodes: Vec<NodeInfo> = self.get_all_nodes();
        nodes.sort_by(|a, b| b.last_seen.cmp(&a.last_seen));
        nodes
    }

    /// Get nodes with status older than a threshold
    pub fn get_stale_nodes(&self, status: NodeStatus, threshold: Duration) -> Vec<Uuid> {
        let now = SystemTime::now();
        let mut stale_nodes = Vec::new();

        for entry in self.nodes.iter() {
            if entry.value().status == status {
                if let Ok(elapsed) = now.duration_since(entry.value().last_seen) {
                    if elapsed > threshold {
                        stale_nodes.push(entry.value().id);
                    }
                }
            }
        }

        stale_nodes
    }

    /// Batch update node statuses
    pub fn batch_update_statuses(&self, updates: &[(Uuid, NodeStatus)]) -> Result<usize, crate::utils::error::OrionError> {
        let mut updated_count = 0;

        for (node_id, new_status) in updates {
            if self.contains(node_id) {
                self.update_status(node_id, new_status.clone())?;
                updated_count += 1;
            }
        }

        Ok(updated_count)
    }

    /// Get nodes with specific metadata
    pub fn get_nodes_with_metadata(&self, key: &str, value: &str) -> Vec<NodeInfo> {
        self.nodes.iter()
            .filter(|entry| {
                if let Some(metadata_value) = entry.value().metadata.get(key) {
                    metadata_value.as_str() == Some(value)
                } else {
                    false
                }
            })
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Export registry state for debugging/monitoring
    pub fn export_state(&self) -> RegistryState {
        let nodes = self.get_all_nodes();
        let active_nodes = self.get_active_nodes();

        RegistryState {
            total_nodes: nodes.len(),
            active_nodes: active_nodes.len(),
            nodes_by_status: [
                (NodeStatus::Joining, self.count_by_status(NodeStatus::Joining)),
                (NodeStatus::Active, self.count_by_status(NodeStatus::Active)),
                (NodeStatus::Unhealthy, self.count_by_status(NodeStatus::Unhealthy)),
                (NodeStatus::Leaving, self.count_by_status(NodeStatus::Leaving)),
                (NodeStatus::Dead, self.count_by_status(NodeStatus::Dead)),
            ].iter().cloned().collect(),
            total_resources: self.get_total_resources(),
        }
    }
}

/// Registry state for monitoring/debugging
#[derive(Debug, Clone, Serialize)]
pub struct RegistryState {
    pub total_nodes: usize,
    pub active_nodes: usize,
    pub nodes_by_status: Vec<(NodeStatus, usize)>,
    pub total_resources: (u32, u64, u64),
}