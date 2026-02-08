// src/engine/distribution.rs
use crate::engine::task::{Task, TaskType, TaskStatus, DistributionMetadata};
use crate::node::{NodeManager, NodeInfo, NodeCapabilities, NodeClassification};
use crate::node::health::HealthScorer;
use std::sync::Arc;
use uuid::Uuid;
use serde::{Serialize, Deserialize};
use std::collections::{HashMap, HashSet};
use anyhow::{Result, anyhow};
use std::time::{SystemTime, Duration};
use tokio::sync::{RwLock, Mutex};
use async_trait::async_trait;
use std::net::SocketAddr;
use log::{info, warn, error, debug};

// ============================================
// ORION-ENG-045: Node-capacity-aware task assignment
// ============================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRequirements {
    pub min_cpu_cores: f32,
    pub min_memory_mb: u64,
    pub min_disk_mb: u64,
    pub needs_gpu: bool,
    pub estimated_duration_secs: f64,
    pub required_task_types: HashSet<String>,
}

impl TaskRequirements {
    pub fn from_task(task: &Task) -> Self {
        let mut requirements = Self {
            min_cpu_cores: 0.1,
            min_memory_mb: 50,
            min_disk_mb: 10,
            needs_gpu: false,
            estimated_duration_secs: 5.0,
            required_task_types: HashSet::new(),
        };

        // Extract from payload
        if let Some(payload) = &task.payload {
            if let serde_json::Value::Object(map) = payload {
                if let Some(cpu_val) = map.get("cpu_cores").and_then(|v| v.as_f64()) {
                    requirements.min_cpu_cores = cpu_val as f32;
                }

                if let Some(mem_val) = map.get("memory_mb").and_then(|v| v.as_u64()) {
                    requirements.min_memory_mb = mem_val;
                }

                if let Some(disk_val) = map.get("disk_mb").and_then(|v| v.as_u64()) {
                    requirements.min_disk_mb = disk_val;
                }

                if let Some(gpu_val) = map.get("needs_gpu").and_then(|v| v.as_bool()) {
                    requirements.needs_gpu = gpu_val;
                }

                if let Some(dur_val) = map.get("estimated_duration_secs").and_then(|v| v.as_f64()) {
                    requirements.estimated_duration_secs = dur_val;
                }

                // Extract task_type from payload
                if let Some(task_type_val) = map.get("task_type").and_then(|v| v.as_str()) {
                    requirements.required_task_types.insert(task_type_val.to_string());
                }

                // Extract from affinity rules in payload
                if let Some(affinity_val) = map.get("affinity").and_then(|v| v.as_str()) {
                    for part in affinity_val.split(',') {
                        let part = part.trim();
                        if let Some((key, value)) = part.split_once('=') {
                            if key.trim() == "class" {
                                requirements.required_task_types.insert(value.trim().to_string());
                            }
                        }
                    }
                }
            }
        }

        // Extract from metadata
        if let Some(metadata) = &task.metadata {
            // From task_type in metadata
            if let Some(task_type_val) = metadata.get("task_type") {
                requirements.required_task_types.insert(task_type_val.to_string());
            }

            // From affinity in metadata
            if let Some(affinity_val) = metadata.get("affinity") {
                for part in affinity_val.split(',') {
                    let part = part.trim();
                    if let Some((key, value)) = part.split_once('=') {
                        if key.trim() == "class" {
                            requirements.required_task_types.insert(value.trim().to_string());
                        }
                    }
                }
            }
        }

        requirements
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScoringWeights {
    pub cpu_weight: f64,
    pub memory_weight: f64,
    pub disk_weight: f64,
    pub gpu_weight: f64,
    pub load_weight: f64,
    pub health_weight: f64,
}

impl Default for ScoringWeights {
    fn default() -> Self {
        Self {
            cpu_weight: 0.25,
            memory_weight: 0.20,
            disk_weight: 0.15,
            gpu_weight: 0.10,
            load_weight: 0.15,
            health_weight: 0.15,
        }
    }
}

#[derive(Debug)]
pub struct CapacityMatcher {
    node_manager: Arc<NodeManager>,
    scoring_weights: ScoringWeights,
}

impl CapacityMatcher {
    pub fn new(node_manager: Arc<NodeManager>) -> Self {
        Self {
            node_manager,
            scoring_weights: ScoringWeights::default(),
        }
    }

    pub async fn find_suitable_nodes(
        &self,
        requirements: &TaskRequirements,
        min_health_score: f64
    ) -> Result<Vec<(Uuid, f64)>> {
        let registry = self.node_manager.registry();
        let health_scorer = self.node_manager.health_scorer();

        let healthy_nodes = health_scorer.get_healthy_nodes(min_health_score);
        let mut suitable_nodes = Vec::new();

        for node_id in healthy_nodes {
            if let Some(node_info) = registry.get(&node_id) {
                if self.can_node_satisfy_requirements(&node_info, requirements) {
                    let score = self.calculate_node_score(&node_info, requirements, &health_scorer).await;
                    suitable_nodes.push((node_id, score));
                }
            }
        }

        suitable_nodes.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        debug!("Found {} suitable nodes for task requirements", suitable_nodes.len());
        Ok(suitable_nodes)
    }

    fn can_node_satisfy_requirements(&self, node_info: &NodeInfo, requirements: &TaskRequirements) -> bool {
        let caps = &node_info.capabilities;

        // Check basic resource requirements
        if caps.cpu_cores < requirements.min_cpu_cores as u32 {
            debug!("Node {} insufficient CPU cores: {} < {}",
                node_info.id, caps.cpu_cores, requirements.min_cpu_cores);
            return false;
        }

        if caps.memory_mb < requirements.min_memory_mb {
            debug!("Node {} insufficient memory: {} < {}",
                node_info.id, caps.memory_mb, requirements.min_memory_mb);
            return false;
        }

        if caps.storage_mb < requirements.min_disk_mb {
            debug!("Node {} insufficient storage: {} < {}",
                node_info.id, caps.storage_mb, requirements.min_disk_mb);
            return false;
        }

        // Check GPU requirement
        if requirements.needs_gpu && !caps.supported_task_types.contains(&"gpu".to_string()) {
            debug!("Node {} lacks GPU support", node_info.id);
            return false;
        }

        // Check required task types
        if !requirements.required_task_types.is_empty() {
            let has_required_type = requirements.required_task_types.iter().any(|req_type| {
                caps.supported_task_types.contains(req_type)
            });
            if !has_required_type {
                debug!("Node {} lacks required task types: {:?}",
                    node_info.id, requirements.required_task_types);
                return false;
            }
        }

        true
    }

    async fn calculate_node_score(
        &self,
        node_info: &NodeInfo,
        requirements: &TaskRequirements,
        health_scorer: &HealthScorer
    ) -> f64 {
        let mut score = 0.0;
        let caps = &node_info.capabilities;

        if caps.cpu_cores > 0 {
            let cpu_ratio = requirements.min_cpu_cores as f64 / caps.cpu_cores as f64;
            let cpu_score = (1.0 - cpu_ratio).max(0.0);
            score += self.scoring_weights.cpu_weight * cpu_score;
        }

        if caps.memory_mb > 0 {
            let mem_ratio = requirements.min_memory_mb as f64 / caps.memory_mb as f64;
            let mem_score = (1.0 - mem_ratio).max(0.0);
            score += self.scoring_weights.memory_weight * mem_score;
        }

        if caps.storage_mb > 0 {
            let disk_ratio = requirements.min_disk_mb as f64 / caps.storage_mb as f64;
            let disk_score = (1.0 - disk_ratio).max(0.0);
            score += self.scoring_weights.disk_weight * disk_score;
        }

        if requirements.needs_gpu {
            if caps.supported_task_types.contains(&"gpu".to_string()) {
                score += self.scoring_weights.gpu_weight;
            }
        }

        let load_factor: f64 = if caps.max_concurrent_tasks > 0 {
            0.8 // Placeholder
        } else {
            1.0
        };
        score += self.scoring_weights.load_weight * load_factor.max(0.0);

        if let Some(health_score) = health_scorer.get_score(&node_info.id) {
            let health_factor = health_score.score / 100.0;
            score += self.scoring_weights.health_weight * health_factor;
        }

        score
    }

    pub fn update_scoring_weights(&mut self, weights: ScoringWeights) {
        self.scoring_weights = weights;
    }
}

// ============================================
// ORION-ENG-046: Local/remote execution decision logic
// ============================================

#[derive(Debug, Clone)]
pub enum AssignmentDecision {
    LocalExecution { node_id: Uuid },
    RemoteExecution { node_id: Uuid, estimated_latency: f64, cost: f64 },
    NoSuitableNode { reason: String },
}

#[derive(Debug, thiserror::Error)]
pub enum DistributionError {
    #[error("No suitable nodes available: {0}")]
    NoSuitableNodes(String),

    #[error("Node communication error: {0}")]
    CommunicationError(String),

    #[error("Task requirements invalid: {0}")]
    InvalidRequirements(String),

    #[error("Affinity rule parsing error: {0}")]
    AffinityRuleError(String),

    #[error("Remote task execution failed: {0}")]
    RemoteExecutionFailed(String),

    #[error("Task state tracking error: {0}")]
    StateTrackingError(String),

    #[error("Node not found: {0}")]
    NodeNotFound(Uuid),
}

#[derive(Debug)]
pub struct ExecutionDecisionLogic {
    node_manager: Arc<NodeManager>,
    capacity_matcher: CapacityMatcher,
    local_node_id: Uuid,
    decision_threshold: f64,
}

impl ExecutionDecisionLogic {
    pub fn new(node_manager: Arc<NodeManager>, local_node_id: Option<Uuid>) -> Self {
        let capacity_matcher = CapacityMatcher::new(node_manager.clone());

        // Use provided local node ID or try to find it
        let local_node_id = local_node_id.unwrap_or_else(|| {
            if let Some(local_node) = node_manager.registry().local_node() {
                info!("📌 Found local node: {}", local_node.id);
                local_node.id
            } else {
                // Fallback: get it from the first node if available
                let all_nodes = node_manager.registry().get_all_nodes();
                if !all_nodes.is_empty() {
                    warn!("⚠️ No local node found. Using first node: {}", all_nodes[0].id);
                    all_nodes[0].id
                } else {
                    error!("⚠️ No nodes found, using nil UUID");
                    Uuid::nil()
                }
            }
        });

        info!("Initialized ExecutionDecisionLogic with local node: {}", local_node_id);

        Self {
            node_manager,
            capacity_matcher,
            local_node_id,
            decision_threshold: 1.3,
        }
    }

    pub async fn decide_execution_location(
        &self,
        requirements: &TaskRequirements,
        suitable_nodes: &[(Uuid, f64)]
    ) -> Result<AssignmentDecision> {
        if suitable_nodes.is_empty() {
            warn!("No suitable nodes found for task requirements");
            return Ok(AssignmentDecision::NoSuitableNode {
                reason: "No nodes satisfy task requirements".to_string()
            });
        }

        debug!("Deciding execution location among {} suitable nodes", suitable_nodes.len());

        // Find local node score
        let local_score = suitable_nodes.iter()
            .find(|(id, _)| *id == self.local_node_id)
            .map(|(_, score)| *score);

        // Find best remote node
        let best_remote = suitable_nodes.iter()
            .filter(|(id, _)| *id != self.local_node_id)
            .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(id, score)| (*id, *score));

        match (local_score, best_remote) {
            (Some(local_score), Some((remote_id, remote_score))) => {
                debug!("Local score: {}, Remote score: {} (threshold: {})",
                    local_score, remote_score, self.decision_threshold);

                if remote_score > local_score * self.decision_threshold {
                    info!("Choosing remote node {} over local (score: {} > {} * {})",
                        remote_id, remote_score, local_score, self.decision_threshold);
                    self.calculate_remote_decision(remote_id, remote_score, requirements).await
                } else {
                    info!("Choosing local node {} (score: {} <= {} * {})",
                        self.local_node_id, remote_score, local_score, self.decision_threshold);
                    Ok(AssignmentDecision::LocalExecution {
                        node_id: self.local_node_id
                    })
                }
            }
            (Some(_), None) => {
                info!("No remote nodes available, using local node {}", self.local_node_id);
                Ok(AssignmentDecision::LocalExecution {
                    node_id: self.local_node_id
                })
            }
            (None, Some((remote_id, remote_score))) => {
                info!("Local node not suitable, using remote node {} (score: {})",
                    remote_id, remote_score);
                self.calculate_remote_decision(remote_id, remote_score, requirements).await
            }
            _ => {
                warn!("No suitable nodes found (internal error)");
                Ok(AssignmentDecision::NoSuitableNode {
                    reason: "Internal error in decision logic".to_string()
                })
            }
        }
    }

    async fn calculate_remote_decision(
        &self,
        remote_node_id: Uuid,
        remote_score: f64,
        requirements: &TaskRequirements
    ) -> Result<AssignmentDecision> {
        let registry = self.node_manager.registry();

        if let Some(node_info) = registry.get(&remote_node_id) {
            let estimated_latency = self.estimate_network_latency(&node_info).await;
            let cost = self.calculate_execution_cost(&node_info, requirements).await;

            info!("Remote execution to node {}: latency={}ms, cost={}, score={}",
                remote_node_id, estimated_latency, cost, remote_score);

            Ok(AssignmentDecision::RemoteExecution {
                node_id: remote_node_id,
                estimated_latency,
                cost,
            })
        } else {
            Err(anyhow!("Remote node {} not found", remote_node_id))
        }
    }

    async fn estimate_network_latency(&self, node_info: &NodeInfo) -> f64 {
        let classification = self.node_manager.classifier().classify(node_info);

        match classification {
            NodeClassification::Local => 0.0,
            NodeClassification::Remote => 10.0,
            NodeClassification::Edge => 50.0,
            NodeClassification::Cloud => 100.0,
            NodeClassification::Unknown => 500.0,
        }
    }

    async fn calculate_execution_cost(
        &self,
        node_info: &NodeInfo,
        requirements: &TaskRequirements
    ) -> f64 {
        let classification = self.node_manager.classifier().classify(node_info);

        let base_cost = match classification {
            NodeClassification::Local => 1.0,
            NodeClassification::Remote => 1.2,
            NodeClassification::Edge => 1.5,
            NodeClassification::Cloud => 2.0,
            NodeClassification::Unknown => 5.0,
        };

        let resource_cost = requirements.min_cpu_cores as f64 * 0.1 +
            requirements.min_memory_mb as f64 * 0.0001 +
            requirements.estimated_duration_secs * 0.01;

        base_cost + resource_cost
    }

    pub fn set_decision_threshold(&mut self, threshold: f64) {
        self.decision_threshold = threshold.max(1.0);
    }

    pub fn get_local_node_id(&self) -> Uuid {
        self.local_node_id
    }
}

// ============================================
// ORION-ENG-047: Task-to-node affinity/routing rules
// ============================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AffinityRule {
    NodeId(Uuid),
    NodeClass(String),
    Zone(String),
    Region(String),
    MaxLatency(u32),
    MinHealthScore(f64),
    AntiAffinity(Vec<Uuid>),
}

#[derive(Debug)]
pub struct AffinityRuleEngine {
    rules: Vec<AffinityRule>,
}

impl AffinityRuleEngine {
    pub fn new() -> Self {
        Self { rules: Vec::new() }
    }

    pub fn add_rule(&mut self, rule: AffinityRule) {
        self.rules.push(rule);
    }

    pub fn parse_rules_from_str(&mut self, rules_str: &str) -> Result<()> {
        // Strip outer quotes if the entire string is quoted
        let rules_str = rules_str.trim();
        let rules_str = if (rules_str.starts_with('"') && rules_str.ends_with('"')) ||
            (rules_str.starts_with('\'') && rules_str.ends_with('\'')) {
            &rules_str[1..rules_str.len()-1]
        } else {
            rules_str
        };

        for part in rules_str.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }

            if let Some((key, value)) = part.split_once('=') {
                let key = key.trim();
                let value = value.trim();

                // Strip quotes from both key and value
                let key = key.trim_matches('"').trim_matches('\'');
                let value = value.trim_matches('"').trim_matches('\'');

                match key {
                    "node_id" => {
                        if let Ok(uuid) = Uuid::parse_str(value) {
                            self.rules.push(AffinityRule::NodeId(uuid));
                        }
                    }
                    "class" => {
                        self.rules.push(AffinityRule::NodeClass(value.to_string()));
                    }
                    "zone" => {
                        self.rules.push(AffinityRule::Zone(value.to_string()));
                    }
                    "region" => {
                        self.rules.push(AffinityRule::Region(value.to_string()));
                    }
                    "max_latency" => {
                        if let Ok(latency) = value.parse() {
                            self.rules.push(AffinityRule::MaxLatency(latency));
                        }
                    }
                    "min_health" => {
                        if let Ok(score) = value.parse() {
                            self.rules.push(AffinityRule::MinHealthScore(score));
                        }
                    }
                    "anti_affinity" => {
                        let nodes: Vec<Uuid> = value.split(';')
                            .filter_map(|s| Uuid::parse_str(s.trim()).ok())
                            .collect();
                        if !nodes.is_empty() {
                            self.rules.push(AffinityRule::AntiAffinity(nodes));
                        }
                    }
                    _ => {
                        warn!("Unknown affinity rule key: {}", key);
                    }
                }
            }
        }
        Ok(())
    }

    pub fn apply_rules(&self, node_id: &Uuid, node_info: &NodeInfo, health_score: Option<f64>) -> bool {
        for rule in &self.rules {
            if !self.check_rule(rule, node_id, node_info, health_score) {
                debug!("Node {} failed rule: {:?}", node_id, rule);
                return false;
            }
        }
        true
    }

    fn check_rule(
        &self,
        rule: &AffinityRule,
        node_id: &Uuid,
        node_info: &NodeInfo,
        health_score: Option<f64>
    ) -> bool {
        match rule {
            AffinityRule::NodeId(target_id) => node_id == target_id,
            AffinityRule::NodeClass(class) => {
                node_info.capabilities.supported_task_types.contains(class)
            }
            AffinityRule::Zone(zone) => {
                if let Some(node_zone) = node_info.metadata.get("zone") {
                    node_zone.as_str() == Some(zone)
                } else {
                    false
                }
            }
            AffinityRule::Region(region) => {
                if let Some(node_region) = node_info.metadata.get("region") {
                    node_region.as_str() == Some(region)
                } else {
                    false
                }
            }
            AffinityRule::MaxLatency(max_ms) => {
                let classification = self.classify_node(node_info);
                let estimated_latency = match classification {
                    NodeClassification::Local => 0,
                    NodeClassification::Remote => 10,
                    NodeClassification::Edge => 50,
                    NodeClassification::Cloud => 100,
                    NodeClassification::Unknown => 500,
                };
                estimated_latency <= *max_ms
            }
            AffinityRule::MinHealthScore(min_score) => {
                health_score.map(|score| score >= *min_score).unwrap_or(false)
            }
            AffinityRule::AntiAffinity(avoid_nodes) => {
                !avoid_nodes.contains(node_id)
            }
        }
    }

    fn classify_node(&self, node_info: &NodeInfo) -> NodeClassification {
        // Simple classification based on hostname or metadata
        if node_info.hostname.contains("local") || node_info.hostname.contains("localhost") {
            NodeClassification::Local
        } else if node_info.hostname.contains("edge") {
            NodeClassification::Edge
        } else if node_info.hostname.contains("cloud") ||
            node_info.hostname.contains("aws") ||
            node_info.hostname.contains("gcp") ||
            node_info.hostname.contains("azure") {
            NodeClassification::Cloud
        } else if node_info.address.ip().is_loopback() {
            NodeClassification::Local
        } else {
            NodeClassification::Remote
        }
    }
}

// ============================================
// ORION-ENG-048: Distributed task state tracking
// ============================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RemoteTaskStatus {
    Pending,
    Running,
    Completed,
    Failed(String),
    Cancelled,
    Timeout,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteTaskState {
    pub task_id: Uuid,
    pub node_id: Uuid,
    pub status: RemoteTaskStatus,
    pub progress: f32,
    pub last_heartbeat: SystemTime,
    pub start_time: Option<SystemTime>,
    pub end_time: Option<SystemTime>,
    pub error_message: Option<String>,
    pub result_location: Option<String>,
    pub result_size: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskResult {
    pub task_id: Uuid,
    pub node_id: Uuid,
    pub result_data: Vec<u8>,
    pub result_metadata: HashMap<String, String>,
    pub execution_time: Duration,
    pub completed_at: SystemTime,
}

#[derive(Debug)]
pub struct DistributedTaskTracker {
    remote_tasks: Arc<RwLock<HashMap<Uuid, RemoteTaskState>>>,
    task_results: Arc<RwLock<HashMap<Uuid, TaskResult>>>,
    task_callbacks: Arc<Mutex<HashMap<Uuid, Vec<Callback>>>>,
}

#[derive(Clone)]
struct Callback(Arc<dyn Fn(&RemoteTaskState) + Send + Sync>);

impl std::fmt::Debug for Callback {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Callback")
    }
}

impl DistributedTaskTracker {
    pub fn new() -> Self {
        Self {
            remote_tasks: Arc::new(RwLock::new(HashMap::new())),
            task_results: Arc::new(RwLock::new(HashMap::new())),
            task_callbacks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn track_task(&self, task_id: Uuid, node_id: Uuid) {
        let state = RemoteTaskState {
            task_id,
            node_id,
            status: RemoteTaskStatus::Pending,
            progress: 0.0,
            last_heartbeat: SystemTime::now(),
            start_time: None,
            end_time: None,
            error_message: None,
            result_location: None,
            result_size: None,
        };

        self.remote_tasks.write().await.insert(task_id, state);
        info!("Tracking new task {} on node {}", task_id, node_id);
    }

    pub async fn update_task_status(
        &self,
        task_id: Uuid,
        status: RemoteTaskStatus,
        progress: f32,
        error_message: Option<String>,
    ) -> Result<()> {
        let mut tasks = self.remote_tasks.write().await;

        if let Some(state) = tasks.get_mut(&task_id) {
            state.status = status.clone();
            state.progress = progress;
            state.last_heartbeat = SystemTime::now();

            if let Some(err) = error_message {
                state.error_message = Some(err);
            }

            if matches!(status, RemoteTaskStatus::Running) && state.start_time.is_none() {
                state.start_time = Some(SystemTime::now());
            }

            if matches!(status, RemoteTaskStatus::Completed | RemoteTaskStatus::Failed(_) | RemoteTaskStatus::Cancelled) {
                state.end_time = Some(SystemTime::now());
            }

            self.notify_callbacks(task_id, state).await;
            info!("Task {} status updated: {:?} (progress: {})", task_id, state.status, progress);
            Ok(())
        } else {
            Err(anyhow!("Task {} not found in tracker", task_id))
        }
    }

    pub async fn store_task_result(&self, task_id: Uuid, result: TaskResult) {
        self.task_results.write().await.insert(task_id, result.clone());

        if let Some(state) = self.remote_tasks.write().await.get_mut(&task_id) {
            state.status = RemoteTaskStatus::Completed;
            state.progress = 1.0;
            state.end_time = Some(SystemTime::now());
            self.notify_callbacks(task_id, state).await;
        }

        info!("Stored result for task {} from node {}", task_id, result.node_id);
    }

    pub async fn get_task_state(&self, task_id: Uuid) -> Option<RemoteTaskState> {
        self.remote_tasks.read().await.get(&task_id).cloned()
    }

    pub async fn get_task_result(&self, task_id: Uuid) -> Option<TaskResult> {
        self.task_results.read().await.get(&task_id).cloned()
    }

    pub async fn register_callback<F>(&self, task_id: Uuid, callback: F)
    where
        F: Fn(&RemoteTaskState) + Send + Sync + 'static,
    {
        let mut callbacks = self.task_callbacks.lock().await;
        callbacks.entry(task_id).or_insert_with(Vec::new).push(Callback(Arc::new(callback)));
    }

    async fn notify_callbacks(&self, task_id: Uuid, state: &RemoteTaskState) {
        let callbacks = self.task_callbacks.lock().await;
        if let Some(callback_list) = callbacks.get(&task_id) {
            for callback in callback_list {
                (callback.0)(state);
            }
        }
    }

    pub async fn cleanup_old_tasks(&self, max_age: Duration) {
        let now = SystemTime::now();
        let mut tasks = self.remote_tasks.write().await;
        let mut results = self.task_results.write().await;

        let mut cleaned = 0;

        tasks.retain(|_id, state| {
            if let Some(end_time) = state.end_time {
                if let Ok(age) = now.duration_since(end_time) {
                    if age > max_age {
                        cleaned += 1;
                        return false;
                    }
                }
            }
            true
        });

        results.retain(|_, result| {
            if let Ok(age) = now.duration_since(result.completed_at) {
                if age > max_age {
                    cleaned += 1;
                    return false;
                }
            }
            true
        });

        if cleaned > 0 {
            info!("Cleaned up {} old task entries", cleaned);
        }
    }

    pub async fn get_all_remote_tasks(&self) -> Vec<RemoteTaskState> {
        self.remote_tasks.read().await.values().cloned().collect()
    }

    pub async fn get_running_remote_tasks(&self) -> Vec<RemoteTaskState> {
        self.remote_tasks.read().await
            .values()
            .filter(|state| matches!(state.status, RemoteTaskStatus::Running | RemoteTaskStatus::Pending))
            .cloned()
            .collect()
    }
}

// ============================================
// ORION-ENG-049: Cross-node communication protocol
// ============================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NodeMessage {
    TaskAssignment {
        message_id: Uuid,
        task: SerializedTask,
        requirements: TaskRequirements,
        affinity_rules: Option<String>,
    },
    TaskStatusUpdate {
        message_id: Uuid,
        task_id: Uuid,
        status: RemoteTaskStatus,
        progress: f32,
        error: Option<String>,
    },
    TaskResult {
        message_id: Uuid,
        task_id: Uuid,
        result_data: Vec<u8>,
        result_metadata: HashMap<String, String>,
        execution_time: Duration,
    },
    HealthCheckRequest {
        message_id: Uuid,
    },
    HealthCheckResponse {
        message_id: Uuid,
        node_id: Uuid,
        load: f32,
        available_resources: NodeCapabilities,
        health_score: f64,
    },
    Heartbeat {
        message_id: Uuid,
        node_id: Uuid,
        timestamp: u64,
    },
    NodeDiscovery {
        message_id: Uuid,
        node_info: NodeInfo,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializedTask {
    pub task_id: Uuid,
    pub name: String,
    pub payload: Option<Vec<u8>>,
    pub metadata: HashMap<String, String>,
}

impl SerializedTask {
    pub fn from_task(task: &Task) -> Self {
        let payload = task.payload.as_ref().map(|p| {
            serde_json::to_vec(p).unwrap_or_default()
        });

        Self {
            task_id: task.id,
            name: task.name.clone(),
            payload,
            metadata: task.metadata.clone().unwrap_or_default(),
        }
    }

    pub fn to_task(&self) -> Task {
        let payload = self.payload.as_ref().map(|p| {
            serde_json::from_slice(p).ok()
        }).flatten();

        Task {
            id: self.task_id,
            name: self.name.clone(),
            task_type: TaskType::OneShot,
            scheduled_at: SystemTime::now(),
            payload,
            status: TaskStatus::Pending,
            created_at: SystemTime::now(),
            updated_at: SystemTime::now(),
            retry_count: 0,
            max_retries: 3,
            metadata: Some(self.metadata.clone()),
            distribution: DistributionMetadata::default(),
        }
    }
}

#[async_trait]
pub trait NodeTransport: Send + Sync + std::fmt::Debug {
    async fn send_message(&self, node_id: Uuid, message: NodeMessage) -> Result<()>;
    async fn broadcast(&self, message: NodeMessage, filter: Option<NodeFilter>) -> Result<()>;
    async fn receive_messages(&self) -> Result<Vec<(Uuid, NodeMessage)>>;
    async fn connect_node(&self, node_info: &NodeInfo) -> Result<()>;
    async fn disconnect_node(&self, node_id: Uuid) -> Result<()>;
}

#[derive(Debug, Clone)]
pub struct NodeFilter {
    pub node_classes: Vec<String>,
    pub min_health_score: Option<f64>,
    pub zones: Vec<String>,
    pub regions: Vec<String>,
}

#[derive(Debug)]
pub struct SimpleTcpTransport {
    node_manager: Arc<NodeManager>,
    connections: Arc<RwLock<HashMap<Uuid, SocketAddr>>>,
}

impl SimpleTcpTransport {
    pub fn new(node_manager: Arc<NodeManager>) -> Self {
        Self {
            node_manager,
            connections: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn get_node_address(&self, node_id: Uuid) -> Option<SocketAddr> {
        let registry = self.node_manager.registry();
        registry.get(&node_id).map(|node| node.address)
    }
}

#[async_trait]
impl NodeTransport for SimpleTcpTransport {
    async fn send_message(&self, node_id: Uuid, message: NodeMessage) -> Result<()> {
        debug!("[TRANSPORT] Sending message to node {}: {:?}", node_id, message);
        Ok(())
    }

    async fn broadcast(&self, message: NodeMessage, filter: Option<NodeFilter>) -> Result<()> {
        let registry = self.node_manager.registry();
        let nodes = registry.get_all_nodes();

        for node in &nodes {
            if let Some(ref filter) = filter {
                if let Some(health_score) = self.node_manager.health_scorer().get_score(&node.id) {
                    if let Some(min_score) = filter.min_health_score {
                        if health_score.score < min_score {
                            continue;
                        }
                    }
                }

                if !filter.zones.is_empty() {
                    if let Some(zone) = node.metadata.get("zone") {
                        if !filter.zones.contains(&zone.to_string()) {
                            continue;
                        }
                    }
                }
            }

            let _ = self.send_message(node.id, message.clone()).await;
        }

        info!("[TRANSPORT] Broadcast message to {} nodes", nodes.len());
        Ok(())
    }

    async fn receive_messages(&self) -> Result<Vec<(Uuid, NodeMessage)>> {
        Ok(Vec::new())
    }

    async fn connect_node(&self, node_info: &NodeInfo) -> Result<()> {
        let mut connections = self.connections.write().await;
        let addr = node_info.address;
        connections.insert(node_info.id, addr);
        info!("[TRANSPORT] Connected to node {} at {}", node_info.id, addr);
        Ok(())
    }

    async fn disconnect_node(&self, node_id: Uuid) -> Result<()> {
        let mut connections = self.connections.write().await;
        connections.remove(&node_id);
        info!("[TRANSPORT] Disconnected from node {}", node_id);
        Ok(())
    }
}

// ============================================
// Main distributor that ties everything together
// ============================================

#[derive(Debug)]
pub struct NodeAwareDistributor {
    node_manager: Arc<NodeManager>,
    capacity_matcher: CapacityMatcher,
    decision_logic: ExecutionDecisionLogic,
    task_tracker: Arc<DistributedTaskTracker>,
    transport: Arc<dyn NodeTransport>,
}

impl NodeAwareDistributor {
    pub fn new(node_manager: Arc<NodeManager>, local_node_id: Option<Uuid>) -> Result<Self, DistributionError> {
        let capacity_matcher = CapacityMatcher::new(node_manager.clone());
        let decision_logic = ExecutionDecisionLogic::new(node_manager.clone(), local_node_id);
        let task_tracker = Arc::new(DistributedTaskTracker::new());
        let transport = Arc::new(SimpleTcpTransport::new(node_manager.clone()));

        Ok(Self {
            capacity_matcher,
            decision_logic,
            task_tracker,
            transport,
            node_manager,
        })
    }

    pub async fn assign_task(&self, task: &Task) -> Result<AssignmentDecision, DistributionError> {
        info!("Assigning task {}: {}", task.id, task.name);

        let requirements = TaskRequirements::from_task(task);
        let affinity_rules = self.parse_affinity_rules_from_task(task);
        let min_health_score = self.extract_min_health_score(task);

        debug!("Task requirements: {:?}", requirements);

        let suitable_nodes = self.capacity_matcher.find_suitable_nodes(&requirements, min_health_score)
            .await
            .map_err(|e| DistributionError::InvalidRequirements(e.to_string()))?;

        let filtered_nodes = self.apply_affinity_rules(&affinity_rules, &suitable_nodes).await?;

        if filtered_nodes.is_empty() {
            warn!("No nodes satisfy affinity rules for task {}", task.id);
            return Ok(AssignmentDecision::NoSuitableNode {
                reason: "No nodes satisfy affinity rules".to_string()
            });
        }

        debug!("Filtered nodes: {:?}", filtered_nodes);

        let decision = self.decision_logic.decide_execution_location(&requirements, &filtered_nodes)
            .await
            .map_err(|e| DistributionError::NoSuitableNodes(e.to_string()))?;

        match &decision {
            AssignmentDecision::LocalExecution { node_id } => {
                info!("Task {} assigned to local node {}", task.id, node_id);
            }
            AssignmentDecision::RemoteExecution { node_id, estimated_latency, cost } => {
                info!("Task {} assigned to remote node {} (latency: {}ms, cost: {})",
                    task.id, node_id, estimated_latency, cost);
            }
            AssignmentDecision::NoSuitableNode { reason } => {
                warn!("No suitable node found for task {}: {}", task.id, reason);
            }
        }

        Ok(decision)
    }

    pub async fn execute_task(&self, task: &Task) -> Result<Uuid, DistributionError> {
        let decision = self.assign_task(task).await?;

        match decision {
            AssignmentDecision::LocalExecution { node_id } => {
                info!("Executing task {} locally on node {}", task.id, node_id);
                Ok(task.id)
            }
            AssignmentDecision::RemoteExecution { node_id, .. } => {
                self.execute_remote_task(task, node_id).await
            }
            AssignmentDecision::NoSuitableNode { reason } => {
                Err(DistributionError::NoSuitableNodes(reason))
            }
        }
    }

    pub async fn execute_remote_task(&self, task: &Task, node_id: Uuid) -> Result<Uuid, DistributionError> {
        let serialized_task = SerializedTask::from_task(task);
        let requirements = TaskRequirements::from_task(task);
        let affinity_rules = self.parse_affinity_rules_from_task(task);

        let message = NodeMessage::TaskAssignment {
            message_id: Uuid::new_v4(),
            task: serialized_task,
            requirements,
            affinity_rules: affinity_rules.rules.iter()
                .map(|r| format!("{:?}", r))
                .collect::<Vec<_>>()
                .join(",")
                .into(),
        };

        self.task_tracker.track_task(task.id, node_id).await;

        self.transport.send_message(node_id, message)
            .await
            .map_err(|e| DistributionError::CommunicationError(e.to_string()))?;

        info!("[DISTRIBUTOR] Task {} assigned to remote node {}", task.id, node_id);
        Ok(task.id)
    }

    pub async fn handle_status_update(&self, node_id: Uuid, message: NodeMessage) -> Result<()> {
        if let NodeMessage::TaskStatusUpdate { task_id, status, progress, error, .. } = message {
            self.task_tracker.update_task_status(task_id, status, progress, error).await
                .map_err(|e| DistributionError::StateTrackingError(e.to_string()))?;

            info!("[DISTRIBUTOR] Status update for task {} from node {}: progress {}",
                task_id, node_id, progress);
        }

        Ok(())
    }

    pub async fn handle_task_result(&self, node_id: Uuid, message: NodeMessage) -> Result<()> {
        if let NodeMessage::TaskResult { task_id, result_data, result_metadata, execution_time, .. } = message {
            let result = TaskResult {
                task_id,
                node_id,
                result_data,
                result_metadata,
                execution_time,
                completed_at: SystemTime::now(),
            };

            self.task_tracker.store_task_result(task_id, result).await;
            info!("[DISTRIBUTOR] Received result for task {} from node {}", task_id, node_id);
        }

        Ok(())
    }

    fn parse_affinity_rules_from_task(&self, task: &Task) -> AffinityRuleEngine {
        let mut engine = AffinityRuleEngine::new();

        if let Some(metadata) = &task.metadata {
            if let Some(affinity_str) = metadata.get("affinity") {
                let _ = engine.parse_rules_from_str(affinity_str);
            }
        }

        // Also check payload for affinity rules
        if let Some(payload) = &task.payload {
            if let serde_json::Value::Object(map) = payload {
                if let Some(affinity_val) = map.get("affinity").and_then(|v| v.as_str()) {
                    let _ = engine.parse_rules_from_str(affinity_val);
                }
            }
        }

        engine
    }

    fn extract_min_health_score(&self, task: &Task) -> f64 {
        // Check metadata
        if let Some(metadata) = &task.metadata {
            if let Some(min_health_str) = metadata.get("min_health_score") {
                if let Ok(score) = min_health_str.parse::<f64>() {
                    return score.max(0.0).min(100.0);
                }
            }
        }

        // Check payload
        if let Some(payload) = &task.payload {
            if let serde_json::Value::Object(map) = payload {
                if let Some(min_health_val) = map.get("min_health_score").and_then(|v| v.as_f64()) {
                    return min_health_val.max(0.0).min(100.0);
                }
            }
        }

        50.0 // Lower default minimum health score for development
    }

    async fn apply_affinity_rules(
        &self,
        affinity_engine: &AffinityRuleEngine,
        suitable_nodes: &[(Uuid, f64)]
    ) -> Result<Vec<(Uuid, f64)>, DistributionError> {
        let registry = self.node_manager.registry();
        let health_scorer = self.node_manager.health_scorer();

        let mut filtered_nodes = Vec::new();

        for (node_id, score) in suitable_nodes {
            if let Some(node_info) = registry.get(node_id) {
                let health_score = health_scorer.get_score(node_id).map(|hs| hs.score);

                if affinity_engine.apply_rules(node_id, &node_info, health_score) {
                    filtered_nodes.push((*node_id, *score));
                }
            }
        }

        Ok(filtered_nodes)
    }

    pub fn get_distribution_stats(&self) -> DistributionStats {
        let registry = self.node_manager.registry();
        let nodes = registry.get_active_nodes();

        DistributionStats {
            total_nodes: nodes.len(),
            local_node_id: self.decision_logic.get_local_node_id(),
            decision_threshold: self.decision_logic.decision_threshold,
            scoring_weights: self.capacity_matcher.scoring_weights.clone(),
        }
    }

    pub fn update_scoring_weights(&mut self, weights: ScoringWeights) {
        self.capacity_matcher.update_scoring_weights(weights);
    }

    pub fn get_task_tracker(&self) -> Arc<DistributedTaskTracker> {
        self.task_tracker.clone()
    }

    pub fn get_transport(&self) -> Arc<dyn NodeTransport> {
        self.transport.clone()
    }

    pub async fn monitor_remote_tasks(&self) {
        let remote_tasks = self.task_tracker.get_running_remote_tasks().await;

        for task in remote_tasks {
            let now = SystemTime::now();
            if let Ok(duration) = now.duration_since(task.last_heartbeat) {
                if duration > Duration::from_secs(30) {
                    warn!("[MONITOR] Task {} on node {} appears stalled", task.task_id, task.node_id);
                }
            }
        }

        // Cleanup old tasks
        self.task_tracker.cleanup_old_tasks(Duration::from_secs(3600)).await;
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct DistributionStats {
    pub total_nodes: usize,
    pub local_node_id: Uuid,
    pub decision_threshold: f64,
    pub scoring_weights: ScoringWeights,
}

// Factory for creating distributed tasks
#[derive(Debug)]
pub struct DistributedTaskFactory;

impl DistributedTaskFactory {
    pub fn gpu_task(name: &str, cpu_cores: f32, memory_mb: u64) -> Task {
        let task = Task {
            id: Uuid::new_v4(),
            name: name.to_string(),
            task_type: TaskType::OneShot,
            scheduled_at: SystemTime::now(),
            payload: Some(serde_json::json!({
                "cpu_cores": cpu_cores,
                "memory_mb": memory_mb,
                "needs_gpu": true,
                "affinity": "class=gpu",  // FIXED: Use affinity instead of task_type
                "min_health_score": 50.0  // Lowered for development
            })),
            status: TaskStatus::Pending,
            created_at: SystemTime::now(),
            updated_at: SystemTime::now(),
            retry_count: 0,
            max_retries: 3,
            metadata: Some(HashMap::from([
                ("affinity".to_string(), "class=gpu".to_string()),  // FIXED
                ("min_health_score".to_string(), "50.0".to_string()),
            ])),
            distribution: DistributionMetadata::default(),
        };
        task
    }

    pub fn memory_intensive_task(name: &str, cpu_cores: f32, memory_mb: u64) -> Task {
        let mut task = Task {
            id: Uuid::new_v4(),
            name: name.to_string(),
            task_type: TaskType::OneShot,
            scheduled_at: SystemTime::now(),
            payload: Some(serde_json::json!({
                "cpu_cores": cpu_cores,
                "memory_mb": memory_mb,
                "needs_gpu": false,
                "affinity": "class=high-memory",
                "min_health_score": 50.0  // Lowered for development
            })),
            status: TaskStatus::Pending,
            created_at: SystemTime::now(),
            updated_at: SystemTime::now(),
            retry_count: 0,
            max_retries: 3,
            metadata: Some(HashMap::from([
                ("affinity".to_string(), "class=high-memory".to_string()),
                ("min_health_score".to_string(), "50.0".to_string()),
            ])),
            distribution: DistributionMetadata::default(),
        };
        task
    }

    pub fn low_latency_task(name: &str) -> Task {
        let mut task = Task {
            id: Uuid::new_v4(),
            name: name.to_string(),
            task_type: TaskType::OneShot,
            scheduled_at: SystemTime::now(),
            payload: Some(serde_json::json!({
                "force_local": true,
                "max_latency": 10,
                "min_health_score": 50.0  // Lowered for development
            })),
            status: TaskStatus::Pending,
            created_at: SystemTime::now(),
            updated_at: SystemTime::now(),
            retry_count: 0,
            max_retries: 3,
            metadata: Some(HashMap::from([
                ("force_local".to_string(), "true".to_string()),
                ("max_latency".to_string(), "10".to_string()),
                ("min_health_score".to_string(), "50.0".to_string()),
            ])),
            distribution: DistributionMetadata::default(),
        };
        task
    }

    pub fn cpu_intensive_task(name: &str, cpu_cores: f32, memory_mb: u64) -> Task {
        let mut task = Task {
            id: Uuid::new_v4(),
            name: name.to_string(),
            task_type: TaskType::OneShot,
            scheduled_at: SystemTime::now(),
            payload: Some(serde_json::json!({
                "cpu_cores": cpu_cores,
                "memory_mb": memory_mb,
                "needs_gpu": false,
                "affinity": "class=cpu-intensive",
                "min_health_score": 50.0
            })),
            status: TaskStatus::Pending,
            created_at: SystemTime::now(),
            updated_at: SystemTime::now(),
            retry_count: 0,
            max_retries: 3,
            metadata: Some(HashMap::from([
                ("affinity".to_string(), "class=cpu-intensive".to_string()),
                ("min_health_score".to_string(), "50.0".to_string()),
            ])),
            distribution: DistributionMetadata::default(),
        };
        task
    }
}