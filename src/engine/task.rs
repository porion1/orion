// src/engine/task.rs
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::time::{Duration, SystemTime};

use chrono::Utc;
use cron::Schedule;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use super::queue::{QueueTask, TaskPriority};
use super::distribution::{
    TaskRequirements, AffinityRuleEngine,
    RemoteTaskStatus, TaskResult, SerializedTask, NodeMessage
};

/// --------------------------------
/// Task Error
/// --------------------------------
#[derive(Debug, Error)]
pub enum TaskError {
    #[error("Invalid cron expression: {0}")]
    InvalidCron(String),
    #[error("Cron produced no future dates")]
    NoFutureDates,
    #[error("Invalid affinity rule: {0}")]
    InvalidAffinityRule(String),
    #[error("Invalid resource requirement: {0}")]
    InvalidResourceRequirement(String),
    #[error("Task serialization error: {0}")]
    SerializationError(String),
    #[error("Task deserialization error: {0}")]
    DeserializationError(String),
}

/// --------------------------------
/// Task Type
/// --------------------------------
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskType {
    OneShot,
    Recurring {
        cron_expr: String,
    },
}

/// --------------------------------
/// Task Status
/// --------------------------------
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskStatus {
    Pending,
    Running,
    Completed,
    Failed(String),
    Cancelled,
}

impl From<TaskStatus> for RemoteTaskStatus {
    fn from(status: TaskStatus) -> Self {
        match status {
            TaskStatus::Pending => RemoteTaskStatus::Pending,
            TaskStatus::Running => RemoteTaskStatus::Running,
            TaskStatus::Completed => RemoteTaskStatus::Completed,
            TaskStatus::Failed(err) => RemoteTaskStatus::Failed(err),
            TaskStatus::Cancelled => RemoteTaskStatus::Cancelled,
        }
    }
}

impl From<RemoteTaskStatus> for TaskStatus {
    fn from(status: RemoteTaskStatus) -> Self {
        match status {
            RemoteTaskStatus::Pending => TaskStatus::Pending,
            RemoteTaskStatus::Running => TaskStatus::Running,
            RemoteTaskStatus::Completed => TaskStatus::Completed,
            RemoteTaskStatus::Failed(err) => TaskStatus::Failed(err),
            RemoteTaskStatus::Cancelled => TaskStatus::Cancelled,
            RemoteTaskStatus::Timeout => TaskStatus::Failed("Timeout".to_string()),
        }
    }
}

/// --------------------------------
/// Distribution Metadata
/// --------------------------------
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DistributionMetadata {
    /// Affinity rules for node selection
    pub affinity_rules: Option<String>,

    /// Minimum node health score (0-100)
    pub min_health_score: Option<f64>,

    /// Force execution on local node
    pub force_local: bool,

    /// Preferred node ID
    pub preferred_node_id: Option<Uuid>,

    /// Avoid these node IDs
    pub avoid_node_ids: Vec<Uuid>,

    /// Maximum acceptable latency in milliseconds
    pub max_latency_ms: Option<u32>,

    /// Resource requirements
    pub resource_requirements: Option<ResourceRequirements>,

    /// Distribution tags (for grouping/routing)
    pub tags: Vec<String>,

    /// NEW: Remote execution tracking
    pub remote_node_id: Option<Uuid>,
    pub remote_execution_started: Option<SystemTime>,
    pub remote_execution_ended: Option<SystemTime>,
    pub remote_status_updates: Vec<RemoteStatusUpdate>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteStatusUpdate {
    pub timestamp: SystemTime,
    pub status: RemoteTaskStatus,
    pub progress: f32,
    pub message: Option<String>,
}

/// --------------------------------
/// Resource Requirements
/// --------------------------------
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceRequirements {
    pub min_cpu_cores: f32,
    pub min_memory_mb: u64,
    pub min_disk_mb: u64,
    pub needs_gpu: bool,
    pub needs_ssd: bool,
    pub estimated_duration_secs: f64,
    pub required_task_types: Vec<String>,
}

impl Default for ResourceRequirements {
    fn default() -> Self {
        Self {
            min_cpu_cores: 0.1,
            min_memory_mb: 50,
            min_disk_mb: 10,
            needs_gpu: false,
            needs_ssd: false,
            estimated_duration_secs: 5.0,
            required_task_types: vec![],
        }
    }
}

impl From<&ResourceRequirements> for TaskRequirements {
    fn from(req: &ResourceRequirements) -> Self {
        TaskRequirements {
            min_cpu_cores: req.min_cpu_cores,
            min_memory_mb: req.min_memory_mb,
            min_disk_mb: req.min_disk_mb,
            needs_gpu: req.needs_gpu,
            estimated_duration_secs: req.estimated_duration_secs,
            required_task_types: req.required_task_types.iter().cloned().collect(),
        }
    }
}

/// --------------------------------
/// TaskRequirements to serde_json::Value
/// --------------------------------
impl From<TaskRequirements> for serde_json::Value {
    fn from(req: TaskRequirements) -> Self {
        let mut json_map = serde_json::Map::new();

        json_map.insert("min_cpu_cores".to_string(), serde_json::Value::from(req.min_cpu_cores as f64));
        json_map.insert("min_memory_mb".to_string(), serde_json::Value::from(req.min_memory_mb));
        json_map.insert("min_disk_mb".to_string(), serde_json::Value::from(req.min_disk_mb));
        json_map.insert("needs_gpu".to_string(), serde_json::Value::from(req.needs_gpu));
        json_map.insert("estimated_duration_secs".to_string(), serde_json::Value::from(req.estimated_duration_secs));

        if !req.required_task_types.is_empty() {
            let types: Vec<_> = req.required_task_types.iter()
                .map(|t: &String| serde_json::Value::String(t.clone()))
                .collect();
            json_map.insert("required_task_types".to_string(), serde_json::Value::Array(types));
        }

        serde_json::Value::Object(json_map)
    }
}

/// --------------------------------
/// Task Model (Source of Truth)
/// --------------------------------
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub id: Uuid,
    pub name: String,
    pub task_type: TaskType,

    #[serde(with = "system_time_serde")]
    pub scheduled_at: SystemTime,

    pub payload: Option<serde_json::Value>,
    pub status: TaskStatus,

    #[serde(with = "system_time_serde")]
    pub created_at: SystemTime,

    #[serde(with = "system_time_serde")]
    pub updated_at: SystemTime,

    pub retry_count: u32,
    pub max_retries: u32,

    pub metadata: Option<HashMap<String, String>>,

    // NEW: Distribution metadata
    pub distribution: DistributionMetadata,
}

impl PartialEq for Task {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Task {}

/// --------------------------------
/// SystemTime <-> millis serde
/// --------------------------------
mod system_time_serde {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    pub fn serialize<S>(time: &SystemTime, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let millis = time
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        serializer.serialize_u64(millis)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<SystemTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let millis = u64::deserialize(deserializer)?;
        Ok(UNIX_EPOCH + Duration::from_millis(millis))
    }
}

/// --------------------------------
/// Constructors
/// --------------------------------
impl Task {
    pub fn new_one_shot(
        name: &str,
        delay: Duration,
        payload: Option<serde_json::Value>,
    ) -> Self {
        let now = SystemTime::now();

        Self {
            id: Uuid::new_v4(),
            name: name.to_string(),
            task_type: TaskType::OneShot,
            scheduled_at: now + delay,
            payload,
            status: TaskStatus::Pending,
            created_at: now,
            updated_at: now,
            retry_count: 0,
            max_retries: 3,
            metadata: None,
            distribution: DistributionMetadata::default(),
        }
    }

    pub fn new_recurring(
        name: &str,
        cron_expr: &str,
        payload: Option<serde_json::Value>,
    ) -> Result<Self, TaskError> {
        // Validate cron expression first
        let schedule = Schedule::from_str(cron_expr)
            .map_err(|e| TaskError::InvalidCron(e.to_string()))?;

        // Get next occurrence
        let next = schedule
            .upcoming(Utc)
            .next()
            .ok_or(TaskError::NoFutureDates)?;

        let now = SystemTime::now();
        let delay_secs = next
            .signed_duration_since(Utc::now())
            .num_seconds()
            .max(1) as u64;

        Ok(Self {
            id: Uuid::new_v4(),
            name: name.to_string(),
            task_type: TaskType::Recurring {
                cron_expr: cron_expr.to_string(),
            },
            scheduled_at: now + Duration::from_secs(delay_secs),
            payload,
            status: TaskStatus::Pending,
            created_at: now,
            updated_at: now,
            retry_count: 0,
            max_retries: 3,
            metadata: None,
            distribution: DistributionMetadata::default(),
        })
    }

    /// --------------------------------
    /// Distribution Builder Methods
    /// --------------------------------

    /// Add affinity rules to the task
    pub fn with_affinity_rules(mut self, rules: &str) -> Result<Self, TaskError> {
        // Validate the rule format
        if !rules.is_empty() {
            let mut engine = AffinityRuleEngine::new();
            engine.parse_rules_from_str(rules)
                .map_err(|e| TaskError::InvalidAffinityRule(e.to_string()))?;
        }

        self.distribution.affinity_rules = Some(rules.to_string());
        Ok(self)
    }

    /// Set resource requirements
    pub fn with_resource_requirements(mut self, requirements: ResourceRequirements) -> Self {
        self.distribution.resource_requirements = Some(requirements);
        self
    }

    /// Quick method for common resource requirements
    pub fn with_basic_resources(mut self, cpu_cores: f32, memory_mb: u64, needs_gpu: bool) -> Self {
        self.distribution.resource_requirements = Some(ResourceRequirements {
            min_cpu_cores: cpu_cores,
            min_memory_mb: memory_mb,
            min_disk_mb: 10, // default
            needs_gpu,
            needs_ssd: false,
            estimated_duration_secs: 5.0,
            required_task_types: vec![],
        });
        self
    }

    /// Force execution on local node
    pub fn force_local(mut self) -> Self {
        self.distribution.force_local = true;
        self
    }

    /// Set minimum health score for nodes
    pub fn with_min_health_score(mut self, score: f64) -> Self {
        self.distribution.min_health_score = Some(score.clamp(0.0, 100.0));
        self
    }

    /// Set preferred node ID
    pub fn prefer_node(mut self, node_id: Uuid) -> Self {
        self.distribution.preferred_node_id = Some(node_id);
        self
    }

    /// Add node IDs to avoid
    pub fn avoid_nodes(mut self, node_ids: Vec<Uuid>) -> Self {
        self.distribution.avoid_node_ids = node_ids;
        self
    }

    /// Set maximum acceptable latency
    pub fn with_max_latency(mut self, latency_ms: u32) -> Self {
        self.distribution.max_latency_ms = Some(latency_ms);
        self
    }

    /// Add distribution tags
    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.distribution.tags = tags;
        self
    }

    /// NEW: Mark task as assigned to remote node
    pub fn assign_to_remote_node(mut self, node_id: Uuid) -> Self {
        self.distribution.remote_node_id = Some(node_id);
        self.distribution.remote_execution_started = Some(SystemTime::now());
        self
    }

    /// NEW: Update remote task status
    pub fn update_remote_status(mut self, status: RemoteTaskStatus, progress: f32, message: Option<String>) -> Self {
        // Check the status before moving it
        let should_end_remote_execution = matches!(status, RemoteTaskStatus::Completed | RemoteTaskStatus::Failed(_) | RemoteTaskStatus::Cancelled);

        // Clone the status for the update record before consuming it
        let status_clone = status.clone();
        let update = RemoteStatusUpdate {
            timestamp: SystemTime::now(),
            status: status_clone,
            progress,
            message,
        };

        self.distribution.remote_status_updates.push(update);

        // Update local status based on remote status
        self.status = status.into();
        self.updated_at = SystemTime::now();

        // Use the flag instead of checking the moved status
        if should_end_remote_execution {
            self.distribution.remote_execution_ended = Some(SystemTime::now());
        }

        self
    }

    /// Get task requirements for distribution
    pub fn get_requirements(&self) -> TaskRequirements {
        if let Some(ref req) = self.distribution.resource_requirements {
            req.into()
        } else {
            // Fallback to extracting from payload
            let mut requirements = TaskRequirements {
                min_cpu_cores: 0.1,
                min_memory_mb: 50,
                min_disk_mb: 10,
                needs_gpu: false,
                estimated_duration_secs: 5.0,
                required_task_types: HashSet::new(),
            };

            // Extract from payload
            if let Some(payload) = &self.payload {
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

                    // Extract task_type
                    if let Some(task_type_val) = map.get("task_type").and_then(|v| v.as_str()) {
                        requirements.required_task_types.insert(task_type_val.to_string());
                    }

                    // Extract from affinity rules
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
            if let Some(metadata) = &self.metadata {
                if let Some(task_type_val) = metadata.get("task_type") {
                    requirements.required_task_types.insert(task_type_val.to_string());
                }

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

    /// Get distribution metadata as JSON
    pub fn get_distribution_metadata_json(&self) -> serde_json::Value {
        serde_json::to_value(&self.distribution).unwrap_or_default()
    }

    /// Check if task has distribution constraints
    pub fn has_distribution_constraints(&self) -> bool {
        self.distribution.affinity_rules.is_some() ||
            self.distribution.resource_requirements.is_some() ||
            self.distribution.force_local ||
            self.distribution.preferred_node_id.is_some() ||
            !self.distribution.avoid_node_ids.is_empty() ||
            self.distribution.max_latency_ms.is_some()
    }

    /// NEW: Check if task is assigned to remote node
    pub fn is_remote_task(&self) -> bool {
        self.distribution.remote_node_id.is_some()
    }

    /// NEW: Get the remote node ID if assigned
    pub fn get_remote_node_id(&self) -> Option<Uuid> {
        self.distribution.remote_node_id
    }

    /// NEW: Get remote execution duration
    pub fn get_remote_execution_duration(&self) -> Option<Duration> {
        if let (Some(start), Some(end)) = (self.distribution.remote_execution_started, self.distribution.remote_execution_ended) {
            end.duration_since(start).ok()
        } else {
            None
        }
    }

    /// --------------------------------
    /// Task Serialization for Distribution
    /// --------------------------------

    /// NEW: Serialize task for transmission to remote nodes
    pub fn serialize_for_distribution(&self) -> Result<SerializedTask, TaskError> {
        let payload_bytes = self.payload.as_ref().map(|p| {
            serde_json::to_vec(p).map_err(|e| TaskError::SerializationError(e.to_string()))
        }).transpose()?;

        let metadata = self.metadata.clone().unwrap_or_default();

        Ok(SerializedTask {
            task_id: self.id,
            name: self.name.clone(),
            payload: payload_bytes,
            metadata,
        })
    }

    /// NEW: Create a NodeMessage for task assignment
    pub fn create_assignment_message(&self, node_id: Uuid) -> Result<NodeMessage, TaskError> {
        let serialized_task = self.serialize_for_distribution()?;
        let requirements = self.get_requirements();
        let affinity_rules = self.distribution.affinity_rules.clone();

        Ok(NodeMessage::TaskAssignment {
            message_id: Uuid::new_v4(),
            task: serialized_task,
            requirements,
            affinity_rules,
        })
    }

    /// NEW: Create a NodeMessage for status update
    pub fn create_status_update_message(&self, progress: f32, error: Option<String>) -> NodeMessage {
        let remote_status: RemoteTaskStatus = self.status.clone().into();

        NodeMessage::TaskStatusUpdate {
            message_id: Uuid::new_v4(),
            task_id: self.id,
            status: remote_status,
            progress,
            error,
        }
    }

    /// NEW: Create a NodeMessage for task result
    pub fn create_result_message(&self, result_data: Vec<u8>, result_metadata: HashMap<String, String>, execution_time: Duration) -> NodeMessage {
        NodeMessage::TaskResult {
            message_id: Uuid::new_v4(),
            task_id: self.id,
            result_data,
            result_metadata,
            execution_time,
        }
    }

    /// NEW: Apply a TaskResult to this task
    pub fn apply_result(&mut self, result: &TaskResult) {
        self.status = TaskStatus::Completed;
        self.updated_at = SystemTime::now();

        // Store result in metadata if needed
        if self.metadata.is_none() {
            self.metadata = Some(HashMap::new());
        }

        if let Some(metadata) = &mut self.metadata {
            metadata.insert("result_node_id".to_string(), result.node_id.to_string());
            metadata.insert("execution_time_ms".to_string(), result.execution_time.as_millis().to_string());
            metadata.insert("completed_at".to_string(), result.completed_at.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis().to_string());

            // Store result metadata
            for (key, value) in &result.result_metadata {
                metadata.insert(format!("result_{}", key), value.clone());
            }
        }
    }

    /// --------------------------------
    /// Recurring scheduling (PURE)
    /// --------------------------------
    pub fn create_next_occurrence(&self) -> Option<Self> {
        let TaskType::Recurring { cron_expr } = &self.task_type else {
            return None;
        };

        let schedule = Schedule::from_str(cron_expr).ok()?;
        let next = schedule.upcoming(Utc).next()?;

        let now = SystemTime::now();
        let delay_secs = next
            .signed_duration_since(Utc::now())
            .num_seconds()
            .max(1) as u64;

        Some(Self {
            id: Uuid::new_v4(),
            name: self.name.clone(),
            task_type: self.task_type.clone(),
            scheduled_at: now + Duration::from_secs(delay_secs),
            payload: self.payload.clone(),
            status: TaskStatus::Pending,
            created_at: now,
            updated_at: now,
            retry_count: 0,
            max_retries: self.max_retries,
            metadata: self.metadata.clone(),
            distribution: self.distribution.clone(),
        })
    }

    pub fn update_status(&mut self, status: TaskStatus) {
        self.status = status;
        self.updated_at = SystemTime::now();
    }

    /// Helper to check if task can be retried
    pub fn can_retry(&self) -> bool {
        self.retry_count < self.max_retries
    }

    /// Helper to get delay until scheduled execution
    pub fn delay_until_scheduled(&self) -> Option<Duration> {
        self.scheduled_at
            .duration_since(SystemTime::now())
            .ok()
    }

    /// Helper to check if task is ready to execute
    pub fn is_ready(&self) -> bool {
        matches!(self.status, TaskStatus::Pending) &&
            self.scheduled_at <= SystemTime::now()
    }

    /// --------------------------------
    /// QueueTask → Task (scheduler fix)
    /// --------------------------------
    pub fn from_queue_task(qt: &QueueTask) -> Self {
        let now = SystemTime::now();

        // Extract distribution metadata from payload if present
        let distribution = if let Some(payload) = &qt.payload {
            Self::extract_distribution_from_payload(payload)
        } else {
            DistributionMetadata::default()
        };

        Self {
            id: qt.id,
            name: qt.name.clone(),
            task_type: qt.task_type.clone(),
            scheduled_at: qt.scheduled_at,
            payload: qt.payload.clone(),
            status: TaskStatus::Pending,
            created_at: now,
            updated_at: now,
            retry_count: qt.retry_count,
            max_retries: qt.max_retries,
            metadata: None,
            distribution,
        }
    }

    /// Extract distribution metadata from payload
    fn extract_distribution_from_payload(payload: &serde_json::Value) -> DistributionMetadata {
        let mut distribution = DistributionMetadata::default();

        if let serde_json::Value::Object(map) = payload {
            // Extract affinity rules
            if let Some(affinity) = map.get("affinity") {
                if let Some(rules) = affinity.as_str() {
                    distribution.affinity_rules = Some(rules.to_string());
                }
            }

            // Extract force_local
            if let Some(force_local) = map.get("force_local") {
                if let Some(force) = force_local.as_bool() {
                    distribution.force_local = force;
                }
            }

            // Extract min_health_score
            if let Some(min_health) = map.get("min_health_score") {
                if let Some(score) = min_health.as_f64() {
                    distribution.min_health_score = Some(score);
                }
            }

            // Extract preferred node ID
            if let Some(node_id_str) = map.get("preferred_node_id") {
                if let Some(node_id) = node_id_str.as_str() {
                    if let Ok(uuid) = Uuid::parse_str(node_id) {
                        distribution.preferred_node_id = Some(uuid);
                    }
                }
            }

            // Extract avoid node IDs
            if let Some(avoid_nodes) = map.get("avoid_node_ids") {
                if let Some(array) = avoid_nodes.as_array() {
                    for node_id_str in array {
                        if let Some(node_id) = node_id_str.as_str() {
                            if let Ok(uuid) = Uuid::parse_str(node_id) {
                                distribution.avoid_node_ids.push(uuid);
                            }
                        }
                    }
                }
            }

            // Extract max latency
            if let Some(max_latency) = map.get("max_latency_ms") {
                if let Some(latency) = max_latency.as_u64() {
                    distribution.max_latency_ms = Some(latency as u32);
                }
            }

            // Extract tags
            if let Some(tags) = map.get("distribution_tags") {
                if let Some(array) = tags.as_array() {
                    for tag in array {
                        if let Some(tag_str) = tag.as_str() {
                            distribution.tags.push(tag_str.to_string());
                        }
                    }
                }
            }

            // Extract resource requirements
            if let Some(cpu_cores) = map.get("cpu_cores").and_then(|v| v.as_f64()) {
                let mut requirements = ResourceRequirements::default();
                requirements.min_cpu_cores = cpu_cores as f32;

                if let Some(memory_mb) = map.get("memory_mb").and_then(|v| v.as_u64()) {
                    requirements.min_memory_mb = memory_mb;
                }

                if let Some(disk_mb) = map.get("min_disk_mb").and_then(|v| v.as_u64()) {
                    requirements.min_disk_mb = disk_mb;
                }

                if let Some(needs_gpu) = map.get("needs_gpu").and_then(|v| v.as_bool()) {
                    requirements.needs_gpu = needs_gpu;
                }

                if let Some(needs_ssd) = map.get("needs_ssd").and_then(|v| v.as_bool()) {
                    requirements.needs_ssd = needs_ssd;
                }

                if let Some(duration) = map.get("estimated_duration_secs").and_then(|v| v.as_f64()) {
                    requirements.estimated_duration_secs = duration;
                }

                if let Some(types) = map.get("required_task_types") {
                    if let Some(array) = types.as_array() {
                        for task_type in array {
                            if let Some(type_str) = task_type.as_str() {
                                requirements.required_task_types.push(type_str.to_string());
                            }
                        }
                    }
                }

                distribution.resource_requirements = Some(requirements);
            }
        }

        distribution
    }

    /// Convert to QueueTask with proper priority calculation
    pub fn to_queue_task(&self, priority: TaskPriority) -> QueueTask {
        // Merge distribution metadata into payload
        let payload = self.merge_distribution_into_payload();

        QueueTask {
            id: self.id,
            name: self.name.clone(),
            scheduled_at: self.scheduled_at,
            priority,
            payload,
            retry_count: self.retry_count,
            max_retries: self.max_retries,
            task_type: self.task_type.clone(),
        }
    }

    /// Merge distribution metadata into the payload
    fn merge_distribution_into_payload(&self) -> Option<serde_json::Value> {
        let mut payload = self.payload.clone().unwrap_or_else(|| serde_json::json!({}));

        if let serde_json::Value::Object(ref mut map) = payload {
            // Add affinity rules if present
            if let Some(ref affinity_rules) = self.distribution.affinity_rules {
                map.insert("affinity".to_string(), serde_json::Value::String(affinity_rules.clone()));
            }

            // Add force_local if true
            if self.distribution.force_local {
                map.insert("force_local".to_string(), serde_json::Value::Bool(true));
            }

            // Add min_health_score if present
            if let Some(min_health_score) = self.distribution.min_health_score {
                map.insert("min_health_score".to_string(), serde_json::Value::from(min_health_score));
            }

            // Add preferred node ID if present
            if let Some(preferred_node_id) = self.distribution.preferred_node_id {
                map.insert("preferred_node_id".to_string(), serde_json::Value::String(preferred_node_id.to_string()));
            }

            // Add avoid node IDs if present
            if !self.distribution.avoid_node_ids.is_empty() {
                let node_ids: Vec<_> = self.distribution.avoid_node_ids.iter()
                    .map(|id| serde_json::Value::String(id.to_string()))
                    .collect();
                map.insert("avoid_node_ids".to_string(), serde_json::Value::Array(node_ids));
            }

            // Add max latency if present
            if let Some(max_latency_ms) = self.distribution.max_latency_ms {
                map.insert("max_latency_ms".to_string(), serde_json::Value::from(max_latency_ms));
            }

            // Add tags if present
            if !self.distribution.tags.is_empty() {
                let tags: Vec<_> = self.distribution.tags.iter()
                    .map(|t| serde_json::Value::String(t.clone()))
                    .collect();
                map.insert("distribution_tags".to_string(), serde_json::Value::Array(tags));
            }

            // Add resource requirements if present
            if let Some(ref req) = self.distribution.resource_requirements {
                map.insert("cpu_cores".to_string(), serde_json::Value::from(req.min_cpu_cores));
                map.insert("memory_mb".to_string(), serde_json::Value::from(req.min_memory_mb));
                map.insert("min_disk_mb".to_string(), serde_json::Value::from(req.min_disk_mb));
                map.insert("needs_gpu".to_string(), serde_json::Value::from(req.needs_gpu));
                map.insert("needs_ssd".to_string(), serde_json::Value::from(req.needs_ssd));
                map.insert("estimated_duration_secs".to_string(), serde_json::Value::from(req.estimated_duration_secs));

                if !req.required_task_types.is_empty() {
                    let types: Vec<_> = req.required_task_types.iter()
                        .map(|t| serde_json::Value::String(t.clone()))
                        .collect();
                    map.insert("required_task_types".to_string(), serde_json::Value::Array(types));
                }
            }

            // Add remote execution info if present
            if let Some(remote_node_id) = self.distribution.remote_node_id {
                map.insert("remote_node_id".to_string(), serde_json::Value::String(remote_node_id.to_string()));
            }

            if let Some(started) = self.distribution.remote_execution_started {
                let started_ms = started.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis();
                map.insert("remote_execution_started_ms".to_string(), serde_json::Value::from(started_ms as u64));
            }
        }

        Some(payload)
    }

    /// Create a distribution summary string
    pub fn distribution_summary(&self) -> String {
        let mut parts = Vec::new();

        if self.distribution.force_local {
            parts.push("local-only".to_string());
        }

        if let Some(ref rules) = self.distribution.affinity_rules {
            parts.push(format!("affinity:{}", rules));
        }

        if let Some(ref req) = self.distribution.resource_requirements {
            parts.push(format!("cpu:{}c,mem:{}MB", req.min_cpu_cores, req.min_memory_mb));
            if req.needs_gpu {
                parts.push("gpu".to_string());
            }
            if req.needs_ssd {
                parts.push("ssd".to_string());
            }
        }

        if let Some(node_id) = self.distribution.remote_node_id {
            parts.push(format!("remote:{}", node_id));
        }

        if parts.is_empty() {
            "default".to_string()
        } else {
            parts.join(", ")
        }
    }
}

/// --------------------------------
/// Task → QueueTask (explicit)
/// --------------------------------
impl From<&Task> for QueueTask {
    fn from(task: &Task) -> Self {
        task.to_queue_task(TaskPriority::Medium)
    }
}

/// --------------------------------
/// From<Task> for QueueTask
/// --------------------------------
impl From<Task> for QueueTask {
    fn from(task: Task) -> Self {
        (&task).into()
    }
}

/// --------------------------------
/// Helper functions for creating distributed tasks
/// ----------------from_queue_task----------------

/// Create a distributed one-shot task
pub fn create_distributed_one_shot(
    name: &str,
    delay: Duration,
    payload: Option<serde_json::Value>,
    distribution_options: DistributionMetadata,
) -> Result<Task, TaskError> {
    let mut task = Task::new_one_shot(name, delay, payload);

    // Apply distribution options
    if let Some(requirements) = distribution_options.resource_requirements {
        task = task.with_resource_requirements(requirements);
    }

    if distribution_options.force_local {
        task = task.force_local();
    }

    if let Some(preferred_node_id) = distribution_options.preferred_node_id {
        task = task.prefer_node(preferred_node_id);
    }

    if !distribution_options.avoid_node_ids.is_empty() {
        task = task.avoid_nodes(distribution_options.avoid_node_ids);
    }

    if let Some(max_latency_ms) = distribution_options.max_latency_ms {
        task = task.with_max_latency(max_latency_ms);
    }

    if !distribution_options.tags.is_empty() {
        task = task.with_tags(distribution_options.tags);
    }

    // Handle the case where with_affinity_rules returns an error
    if let Some(affinity_rules) = distribution_options.affinity_rules {
        match task.with_affinity_rules(&affinity_rules) {
            Ok(t) => task = t,
            Err(e) => return Err(e),
        }
    }

    Ok(task)
}

/// Create a distributed recurring task
pub fn create_distributed_recurring(
    name: &str,
    cron_expr: &str,
    payload: Option<serde_json::Value>,
    distribution_options: DistributionMetadata,
) -> Result<Task, TaskError> {
    let mut task = Task::new_recurring(name, cron_expr, payload)?;

    // Apply distribution options
    if let Some(requirements) = distribution_options.resource_requirements {
        task = task.with_resource_requirements(requirements);
    }

    if distribution_options.force_local {
        task = task.force_local();
    }

    if let Some(preferred_node_id) = distribution_options.preferred_node_id {
        task = task.prefer_node(preferred_node_id);
    }

    if !distribution_options.avoid_node_ids.is_empty() {
        task = task.avoid_nodes(distribution_options.avoid_node_ids);
    }

    if let Some(max_latency_ms) = distribution_options.max_latency_ms {
        task = task.with_max_latency(max_latency_ms);
    }

    if !distribution_options.tags.is_empty() {
        task = task.with_tags(distribution_options.tags);
    }

    if let Some(affinity_rules) = distribution_options.affinity_rules {
        task = task.with_affinity_rules(&affinity_rules)?;
    }

    Ok(task)
}

/// Factory for common distributed task types
pub struct DistributedTaskFactory;

impl DistributedTaskFactory {
    /// Create a GPU-intensive task
    pub fn gpu_task(name: &str, cpu_cores: f32, memory_mb: u64) -> Result<Task, TaskError> {
        Ok(Task::new_one_shot(name, Duration::from_secs(0), None)
            .with_basic_resources(cpu_cores, memory_mb, true)
            .with_affinity_rules("class=gpu")?
            .with_min_health_score(80.0))
    }

    /// Create a high-memory task
    pub fn memory_intensive_task(name: &str, cpu_cores: f32, memory_mb: u64) -> Result<Task, TaskError> {
        Ok(Task::new_one_shot(name, Duration::from_secs(0), None)
            .with_basic_resources(cpu_cores, memory_mb, false)
            .with_affinity_rules("class=high-memory")?
            .with_min_health_score(70.0))
    }

    /// Create a low-latency task (forced local)
    pub fn low_latency_task(name: &str) -> Task {
        Task::new_one_shot(name, Duration::from_secs(0), None)
            .force_local()
            .with_max_latency(10)
            .with_min_health_score(90.0)
    }

    /// Create a task for specific zone
    pub fn zoned_task(name: &str, zone: &str) -> Result<Task, TaskError> {
        Task::new_one_shot(name, Duration::from_secs(0), None)
            .with_affinity_rules(&format!("zone={}", zone))
    }

    /// NEW: Create a task explicitly for remote execution
    pub fn remote_task(name: &str, target_node_id: Uuid, cpu_cores: f32, memory_mb: u64) -> Result<Task, TaskError> {
        Ok(Task::new_one_shot(name, Duration::from_secs(0), None)
            .with_basic_resources(cpu_cores, memory_mb, false)
            .prefer_node(target_node_id)
            .with_min_health_score(60.0))
    }

    /// NEW: Create a result-aggregation task (collects results from multiple nodes)
    pub fn result_aggregation_task(name: &str, source_task_ids: Vec<Uuid>) -> Task {
        let mut metadata = HashMap::new();
        metadata.insert("source_task_ids".to_string(),
                        source_task_ids.iter().map(|id| id.to_string()).collect::<Vec<_>>().join(","));
        metadata.insert("aggregation_type".to_string(), "reduce".to_string());

        Task::new_one_shot(name, Duration::from_secs(0), Some(serde_json::to_value(metadata).unwrap()))
            .force_local()  // Aggregation usually happens locally
            .with_min_health_score(90.0)
    }
}