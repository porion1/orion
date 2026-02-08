//! Load balancing and routing strategies for task distribution

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use std::ops::Add;

use tokio::sync::RwLock;
use serde::{Deserialize, Serialize, Serializer, Deserializer};
use serde::de::{self, Visitor};

// Use anyhow instead of your custom error type
pub use anyhow::{Error, Result, anyhow};

// Define Node struct for routing
#[derive(Debug, Clone)]
pub struct Node {
    pub id: String,
    pub is_healthy: bool,
    pub metrics: NodeMetrics,
}

// Define your own Task type for routing
#[derive(Debug, Clone)]
pub struct Task {
    pub id: String,
    pub name: String,
    pub task_type: String,
    pub priority: u8,
    pub payload: Option<serde_json::Value>,
    pub metadata: Option<HashMap<String, String>>,
    pub distribution: DistributionMetadata,
    pub routing_hints: RoutingHints,  // Added missing field
}

#[derive(Debug, Clone, Default)]
pub struct DistributionMetadata {
    pub tags: Vec<String>,
    pub force_local: bool,
    pub min_health_score: Option<f64>,
    pub preferred_node_id: Option<String>,
    pub remote_node_id: Option<String>,
    pub max_latency_ms: Option<u64>,
    pub resource_requirements: Option<ResourceRequirements>,
    pub affinity_rules: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct ResourceRequirements {
    pub min_cpu_cores: f64,
    pub min_memory_mb: f64,
    pub min_disk_mb: f64,
    pub needs_gpu: bool,
    pub needs_ssd: bool,
    pub estimated_duration_secs: f64,
    pub required_task_types: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct RoutingHints {
    pub preferred_node: Option<String>,
    pub preferred_region: Option<String>,
    pub required_capabilities: Vec<String>,
    pub deadline: Option<SystemTime>,
}

pub(crate) mod strategies;
mod metrics;
pub(crate) mod selector;
mod failover;
pub(crate) mod config;
mod adapter;

pub use adapter::TaskAdapter;
pub use adapter::*;
pub use strategies::*;
pub use metrics::*;
pub use selector::*;
pub use failover::*;
pub use config::*;

/// Wrapper for Instant to make it serializable
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SerializableInstant(Instant);

impl SerializableInstant {
    pub fn now() -> Self {
        Self(Instant::now())
    }

    pub fn duration_since(&self, other: Instant) -> Duration {
        self.0.duration_since(other)
    }
}

// Implement Add<Duration> for SerializableInstant
impl Add<Duration> for SerializableInstant {
    type Output = SerializableInstant;

    fn add(self, duration: Duration) -> Self::Output {
        SerializableInstant(self.0 + duration)
    }
}

// Fix: Use std::result::Result for Serialize/Deserialize traits
impl Serialize for SerializableInstant {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where S: Serializer {
        serializer.serialize_u64(self.0.elapsed().as_millis() as u64)
    }
}

impl<'de> Deserialize<'de> for SerializableInstant {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where D: Deserializer<'de> {
        struct InstantVisitor;
        impl<'de> Visitor<'de> for InstantVisitor {
            type Value = SerializableInstant;
            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a u64 representing milliseconds")
            }

            fn visit_u64<E>(self, value: u64) -> std::result::Result<Self::Value, E>
            where E: de::Error {
                Ok(SerializableInstant(Instant::now() - Duration::from_millis(value)))
            }
        }
        deserializer.deserialize_u64(InstantVisitor)
    }
}

/// Main router for load balancing decisions
#[derive(Debug, Clone)]
pub struct Router {
    pub strategies: Arc<RwLock<HashMap<String, Box<dyn RoutingStrategy + Send + Sync>>>>,
    pub active_strategy: Arc<RwLock<String>>,
    pub metrics_collector: Arc<MetricsCollector>,
    pub config: RouterConfig,
    pub failover_handler: Arc<FailoverHandler>,
    // REMOVED: pub selector: Arc<NodeSelector>,
}

impl Router {
    pub fn new(config: RouterConfig) -> Self {
        Self {
            strategies: Arc::new(RwLock::new(HashMap::new())),
            active_strategy: Arc::new(RwLock::new(config.default_strategy.clone())),
            metrics_collector: Arc::new(MetricsCollector::new()),
            config: config.clone(),
            failover_handler: Arc::new(FailoverHandler::new(config)),
            // REMOVED: selector: Arc::new(NodeSelector::new(config.clone())),
        }
    }
}

/// Context for routing decisions
#[derive(Debug, Clone)]
pub struct RoutingContext {
    pub task_id: String,
    pub task_priority: u8,
    pub task_type: String,
    pub timestamp: SerializableInstant,
    pub metadata: HashMap<String, String>,
}

/// Node load metrics for routing decisions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeMetrics {
    pub cpu_usage: f32,           // 0.0 to 1.0
    pub memory_usage: f32,        // 0.0 to 1.0
    pub queue_length: usize,      // Number of pending tasks
    pub processing_tasks: usize,  // Number of currently processing tasks
    pub avg_latency_ms: f64,      // Average task processing latency
    pub last_heartbeat: SerializableInstant,  // Last heartbeat timestamp
    pub is_healthy: bool,         // Health status
    pub capacity: NodeCapacity,   // Node capacity limits
    pub tags: HashMap<String, String>, // Node tags/attributes
}

impl Default for NodeMetrics {
    fn default() -> Self {
        Self {
            cpu_usage: 0.0,
            memory_usage: 0.0,
            queue_length: 0,
            processing_tasks: 0,
            avg_latency_ms: 0.0,
            last_heartbeat: SerializableInstant::now(),
            is_healthy: true,
            capacity: NodeCapacity::default(),
            tags: HashMap::new(),
        }
    }
}

/// Node capacity limits
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NodeCapacity {
    pub max_concurrent_tasks: usize,
    pub max_queue_length: usize,
    pub supported_task_types: Vec<String>,
    pub regions: Vec<String>,     // Geographical regions
}

/// Routing statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingStatistics {
    pub total_selections: u64,
    pub successful_selections: u64,
    pub failed_selections: u64,
    pub avg_selection_latency_ms: f64,
    pub strategy_distribution: HashMap<String, u64>,
    pub node_selection_counts: HashMap<String, u64>,
    pub failover_count: u64,
    pub timestamp: SerializableInstant,
}

// Helper implementations
impl RoutingContext {
    pub fn new(task_id: String, task_priority: u8, task_type: String) -> Self {
        Self {
            task_id,
            task_priority,
            task_type,
            timestamp: SerializableInstant::now(),
            metadata: HashMap::new(),
        }
    }
}

impl Default for RoutingContext {
    fn default() -> Self {
        Self {
            task_id: String::new(),
            task_priority: 5,
            task_type: String::new(),
            timestamp: SerializableInstant::now(),
            metadata: HashMap::new(),
        }
    }
}

// Add conversion from Engine Task to Routing Task
impl Task {
    pub fn from_engine_task(engine_task: &crate::engine::task::Task) -> Self {
        let distribution = DistributionMetadata {
            tags: vec![],
            force_local: false,
            min_health_score: None,
            preferred_node_id: None,
            remote_node_id: None,
            max_latency_ms: None,
            resource_requirements: None,
            affinity_rules: None,
        };

        Self {
            id: engine_task.id.to_string(),
            name: engine_task.name.clone(),
            // FIX: Use actual TaskType variants from your engine::task module
            // Check what variants your TaskType enum actually has
            task_type: match &engine_task.task_type {
                // These are common variants - adjust based on your actual enum
                crate::engine::task::TaskType::OneShot => "one_shot".to_string(),
                crate::engine::task::TaskType::Recurring { .. } => "recurring".to_string(),
                // If you have Dependent variant:
                // crate::engine::task::TaskType::Dependent { .. } => "dependent".to_string(),
                // If you have Cron variant:
                // crate::engine::task::TaskType::Cron { .. } => "cron".to_string(),
                // Fallback for any variant:
                _ => format!("{:?}", engine_task.task_type).to_lowercase(),
            },
            priority: 5, // Default priority
            payload: engine_task.payload.clone(),
            metadata: None,
            distribution,
            routing_hints: RoutingHints::default(),
        }
    }
}