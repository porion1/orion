use dashmap::DashMap;
use serde::{Deserialize, Serialize};
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeCapabilities {
    pub cpu_cores: u32,
    pub memory_mb: u64,
    pub storage_mb: u64,
    pub max_concurrent_tasks: u32,
    pub supported_task_types: Vec<String>,
}

/// Thread-safe node registry
#[derive(Debug)]
pub struct NodeRegistry {
    nodes: DashMap<Uuid, NodeInfo>,
    persistence: Option<sled::Db>,
    local_node_id: Uuid,
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
        })
    }

    /// Register a new node or update existing node
    pub fn register(&self, node_info: NodeInfo) -> Result<(), crate::utils::error::OrionError> {
        let id = node_info.id;

        // Store in memory
        self.nodes.insert(id, node_info.clone());

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
        self.nodes.iter()
            .filter(|entry| entry.value().status == NodeStatus::Active)
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Remove node from registry
    pub fn remove(&self, node_id: &Uuid) -> Result<(), crate::utils::error::OrionError> {
        self.nodes.remove(node_id);

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
            entry.status = status;
            entry.last_seen = SystemTime::now();

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

    /// Get local node info
    pub fn local_node(&self) -> Option<NodeInfo> {
        self.get(&self.local_node_id)
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
}