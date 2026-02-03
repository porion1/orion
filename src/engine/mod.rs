pub mod config;
pub mod scheduler;
pub mod state;
pub mod task;
pub mod queue;
pub mod executor;

// NEW: Add distribution module
pub mod distribution;

pub use config::EngineConfig;
pub use scheduler::Scheduler;
pub use state::EngineState;
pub use task::Task;
pub use queue::{QueueTask, SharedTaskQueue, QueueConfig, TaskPriority};
pub use executor::TaskExecutor;
// NEW: Export distribution types
pub use distribution::{
    NodeAwareDistributor, AssignmentDecision, TaskRequirements,
    ScoringWeights, DistributionError, AffinityRuleEngine,
    DistributionStats, ResourceRequirements, DistributedTaskFactory,
    NodeMessage, RemoteTaskState, RemoteTaskStatus, TaskResult,
    SerializedTask, NodeTransport
};

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use uuid::Uuid;
use anyhow::Result;

/// Main Engine struct
#[derive(Debug)]
pub struct Engine {
    pub scheduler: Arc<Scheduler>,
    pub executor: Arc<TaskExecutor>,
    pub config: EngineConfig,
    pub state: Arc<tokio::sync::RwLock<EngineState>>,
    pub task_queue: Arc<SharedTaskQueue>,
    shutdown_tx: watch::Sender<bool>,

    // Node Manager (optional - only initialized if cluster is enabled)
    pub node_manager: Option<Arc<crate::node::NodeManager>>,

    // NEW: Node-Aware Distributor
    pub distributor: Option<Arc<NodeAwareDistributor>>,
}

impl Engine {
    pub fn new(config: EngineConfig) -> Self {
        let (shutdown_tx, _) = watch::channel(false);

        // Shared task queue
        let queue_config = QueueConfig::new(
            10_000,
            config.persistence_path.clone(),
        );
        let task_queue = Arc::new(SharedTaskQueue::new(queue_config));

        // Executor
        let executor = Arc::new(TaskExecutor::new(
            Arc::clone(&task_queue),
            config.max_concurrent_tasks,
            Duration::from_secs(5),
        ));

        // Scheduler
        let scheduler = Arc::new(Scheduler::new(
            Arc::clone(&task_queue),
            Arc::clone(&executor),
        ));

        // Initialize Node Manager and Distributor if cluster is enabled
        let (node_manager, distributor) = if config.is_cluster_enabled() {
            // Get the cluster config and convert it to NodeConfig
            if let Some(cluster_config) = &config.cluster_config {
                let node_config: crate::node::NodeConfig = cluster_config.clone().into();

                match crate::node::NodeManager::new(node_config) {
                    Ok(manager) => {
                        let manager_arc = Arc::new(manager);

                        // Create distributor
                        let distributor = match NodeAwareDistributor::new(manager_arc.clone()) {
                            Ok(dist) => {
                                println!("🌐 Node Manager and Distributor initialized");
                                Some(Arc::new(dist))
                            }
                            Err(e) => {
                                eprintln!("⚠️ Failed to initialize Distributor: {}", e);
                                None
                            }
                        };

                        (Some(manager_arc), distributor)
                    }
                    Err(e) => {
                        eprintln!("⚠️ Failed to initialize Node Manager: {}", e);
                        (None, None)
                    }
                }
            } else {
                (None, None)
            }
        } else {
            (None, None)
        };

        Self {
            scheduler,
            executor,
            config,
            state: Arc::new(tokio::sync::RwLock::new(EngineState::Init)),
            task_queue,
            shutdown_tx,
            node_manager,
            distributor,
        }
    }

    pub async fn start(self: Arc<Self>) -> Result<()> {
        self.set_state(EngineState::Running).await;
        println!("🚀 Engine starting...");

        // Start Node Manager and Distributor if enabled
        if let Some(node_manager) = &self.node_manager {
            if let Err(e) = node_manager.start() {
                eprintln!("⚠️ Failed to start Node Manager: {}", e);
            } else {
                println!("🌐 Node Manager started");
            }
        }

        // Load persisted tasks from storage
        match self.task_queue.load_persisted().await {
            Ok(_) => {
                let pending_count = self.task_queue.len().await;
                if pending_count > 0 {
                    println!("📂 Loaded {} persisted tasks from storage", pending_count);
                } else {
                    println!("📂 No persisted tasks found in storage");
                }
            }
            Err(e) => {
                eprintln!("⚠️ Failed to load persisted tasks: {}", e);
            }
        }

        // Start scheduler first
        let scheduler_clone = Arc::clone(&self.scheduler);
        let _scheduler_handle = tokio::spawn(async move {
            if let Err(e) = scheduler_clone.start().await {
                eprintln!("Scheduler error: {}", e);
            }
        });

        // Start executor loop
        let executor_clone = Arc::clone(&self.executor);
        let _executor_handle = tokio::spawn(async move {
            executor_clone.start().await;
        });

        // Start node monitor if cluster is enabled
        if let Some(node_manager) = &self.node_manager {
            let membership_manager = node_manager.membership_manager();
            let membership_manager_clone = Arc::clone(&membership_manager);

            let _monitor_handle = tokio::spawn(async move {
                membership_manager_clone.monitor_nodes().await;
            });

            println!("🌐 Node monitoring started");
        }

        // NEW: Start remote task monitoring if distribution is enabled
        if self.is_distribution_enabled() {
            let engine_clone = Arc::clone(&self);
            let _remote_monitor_handle = tokio::spawn(async move {
                engine_clone.monitor_remote_tasks().await;
            });
            println!("📡 Remote task monitoring started");

            // NEW: Start message receiver if distribution is enabled
            let engine_clone = Arc::clone(&self);
            let _message_receiver_handle = tokio::spawn(async move {
                engine_clone.receive_messages().await;
            });
            println!("📨 Message receiver started");
        }

        // Wait for shutdown
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let scheduler_shutdown = self.scheduler.shutdown_handle();

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("Shutdown signal received");
            }
            _ = shutdown_rx.changed() => {
                println!("Internal shutdown signal received");
            }
        }

        self.set_state(EngineState::Draining).await;

        // Stop Node Manager if enabled
        if let Some(node_manager) = &self.node_manager {
            node_manager.stop();
            println!("🌐 Node Manager stopped");
        }

        // Send shutdown signals
        let _ = scheduler_shutdown.send(true);
        let _ = self.shutdown_tx.send(true);

        // Clean up completed tasks before stopping
        self.cleanup_completed_tasks().await;

        tokio::time::sleep(Duration::from_secs(5)).await;
        self.set_state(EngineState::Stopped).await;

        println!("✅ Engine stopped");
        Ok(())
    }

    pub async fn set_state(&self, new_state: EngineState) {
        let current_state = *self.state.read().await;
        println!("State changed: {:?} → {:?}", current_state, new_state);
        *self.state.write().await = new_state;
    }

    pub async fn get_state(&self) -> EngineState {
        *self.state.read().await
    }

    /// Schedule a task with node-aware distribution
    pub async fn schedule_task(&self, task: QueueTask) -> Result<Uuid> {
        // Check if task should be distributed
        if let Some(distributor) = &self.distributor {
            // Check if task has distribution requirements
            if Self::should_distribute_task(&task) {
                return self.schedule_task_with_distribution(distributor, task).await;
            }
        }

        // Fall back to local-only scheduling
        self.schedule_task_locally(task).await
    }

    /// Determine if task should be distributed
    fn should_distribute_task(task: &QueueTask) -> bool {
        // Check for distribution flags in payload
        if let Some(payload) = &task.payload {
            if let serde_json::Value::Object(map) = payload {
                // Task has affinity rules
                if map.contains_key("affinity") {
                    return true;
                }
                // Task has resource requirements
                if map.contains_key("cpu_cores") || map.contains_key("memory_mb") || map.contains_key("needs_gpu") {
                    return true;
                }
                // Task explicitly requests distribution
                if map.get("distribute").and_then(|v| v.as_bool()).unwrap_or(false) {
                    return true;
                }
            }
        }
        false
    }

    /// Schedule a task using node-aware distribution
    async fn schedule_task_with_distribution(
        &self,
        distributor: &Arc<NodeAwareDistributor>,
        task: QueueTask
    ) -> Result<Uuid> {
        // Convert QueueTask to Task for distribution analysis
        let task_obj = Task::from_queue_task(&task);

        // Get distribution decision
        match distributor.assign_task(&task_obj).await {
            Ok(decision) => {
                match decision {
                    AssignmentDecision::LocalExecution { node_id } => {
                        println!("📌 Task '{}' assigned to local node: {}", task.name, node_id);
                        self.schedule_task_locally(task).await
                    }
                    AssignmentDecision::RemoteExecution { node_id, estimated_latency, cost } => {
                        println!("🌐 Task '{}' assigned to remote node: {} (latency: {:.2}ms, cost: {:.2})",
                                 task.name, node_id, estimated_latency, cost);

                        // FIXED: Actually execute the task remotely
                        match distributor.execute_remote_task(&task_obj, node_id).await {
                            Ok(task_id) => {
                                println!("✅ Remote task '{}' dispatched to node {}", task.name, node_id);

                                // Register callback to track remote task completion
                                let task_name = task.name.clone();
                                let _task_id_copy = task_obj.id;  // Prefix with underscore

                                distributor.get_task_tracker().register_callback(task_obj.id, move |state| {
                                    let task_name = task_name.clone();
                                    if let RemoteTaskStatus::Completed = state.status {
                                        println!("✅ Remote task '{}' completed on node {}", task_name, state.node_id);
                                        // Could add result processing here
                                    } else if let RemoteTaskStatus::Failed(ref error) = state.status {
                                        println!("❌ Remote task '{}' failed on node {}: {}", task_name, state.node_id, error);
                                        // Could add retry logic here
                                    }
                                }).await;

                                Ok(task_id)
                            }
                            Err(e) => {
                                println!("⚠️ Failed to execute task '{}' remotely: {}, falling back to local",
                                         task.name, e);
                                self.schedule_task_locally(task).await
                            }
                        }
                    }
                    AssignmentDecision::NoSuitableNode { reason } => {
                        println!("⚠️ No suitable node for task '{}': {}", task.name, reason);

                        // Try to schedule locally as fallback
                        println!("📌 Fallback: Scheduling '{}' locally", task.name);
                        self.schedule_task_locally(task).await
                    }
                }
            }
            Err(e) => {
                println!("⚠️ Distribution failed for task '{}': {}, falling back to local",
                         task.name, e);

                // Fall back to local execution
                self.schedule_task_locally(task).await
            }
        }
    }

    /// Schedule a task locally (original logic)
    async fn schedule_task_locally(&self, task: QueueTask) -> Result<Uuid> {
        self.task_queue.enqueue(task.clone()).await
            .map_err(|e| anyhow::anyhow!("Failed to enqueue task: {}", e))?;
        Ok(task.id)
    }

    /// Schedule a task with explicit distribution settings
    pub async fn schedule_task_with_options(
        &self,
        task: QueueTask,
        distribution_options: DistributionOptions
    ) -> Result<Uuid> {
        // Apply distribution options to task
        let task_with_options = apply_distribution_options_to_task(task, distribution_options);
        self.schedule_task(task_with_options).await
    }

    /// NEW: Execute a task on a specific remote node
    pub async fn execute_on_remote_node(&self, task: &Task, node_id: Uuid) -> Result<Uuid> {
        if let Some(distributor) = &self.distributor {
            distributor.execute_remote_task(task, node_id)
                .await
                .map_err(|e| anyhow::anyhow!("Remote execution failed: {}", e))
        } else {
            Err(anyhow::anyhow!("Distribution not enabled"))
        }
    }

    /// NEW: Get status of remote tasks
    pub async fn get_remote_task_status(&self, task_id: Uuid) -> Option<RemoteTaskState> {
        if let Some(distributor) = &self.distributor {
            distributor.get_task_tracker().get_task_state(task_id).await
        } else {
            None
        }
    }

    /// NEW: Get all remote tasks
    pub async fn get_all_remote_tasks(&self) -> Vec<RemoteTaskState> {
        if let Some(distributor) = &self.distributor {
            distributor.get_task_tracker().get_all_remote_tasks().await
        } else {
            Vec::new()
        }
    }

    /// NEW: Get remote task result
    pub async fn get_remote_task_result(&self, task_id: Uuid) -> Option<TaskResult> {
        if let Some(distributor) = &self.distributor {
            distributor.get_task_tracker().get_task_result(task_id).await
        } else {
            None
        }
    }

    pub async fn cancel_task(&self, id: Uuid) -> bool {
        // First check if it's a remote task
        if let Some(distributor) = &self.distributor {
            if let Some(state) = distributor.get_task_tracker().get_task_state(id).await {
                if matches!(state.status, RemoteTaskStatus::Pending | RemoteTaskStatus::Running) {
                    // Send cancellation message to remote node
                    let message = NodeMessage::TaskStatusUpdate {
                        message_id: Uuid::new_v4(),
                        task_id: id,
                        status: RemoteTaskStatus::Cancelled,
                        progress: 0.0,
                        error: Some("Cancelled by user".to_string()),
                    };

                    if distributor.get_transport().send_message(state.node_id, message).await.is_ok() {
                        println!("📨 Sent cancellation request for remote task {}", id);
                        return true;
                    }
                }
            }
        }

        // Fall back to local cancellation
        self.executor.cancel_task(id).await
    }

    pub async fn get_pending_tasks(&self) -> Vec<QueueTask> {
        self.task_queue.get_all_pending().await
    }

    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    pub async fn cleanup_completed_tasks(&self) {
        println!("🧹 Cleaning up completed tasks from persistence...");

        // Get all results from executor
        let completed_results = self.executor.get_all_results().await;
        let mut cleaned_count = 0;
        let mut error_count = 0;

        for result in completed_results {
            // Remove task from persistence DB
            match self.task_queue.remove_from_persistence(result.task_id).await {
                Ok(_) => {
                    cleaned_count += 1;
                }
                Err(e) => {
                    eprintln!("⚠️ Failed to remove task {} from persistence: {}",
                              result.task_id, e);
                    error_count += 1;
                }
            }
        }

        // NEW: Also cleanup old remote tasks
        if let Some(distributor) = &self.distributor {
            distributor.get_task_tracker().cleanup_old_tasks(Duration::from_secs(3600)).await;
        }

        println!("✅ Cleaned {} completed tasks from persistence ({} errors)",
                 cleaned_count, error_count);
    }

    pub async fn clear_persistence(&self) -> Result<()> {
        println!("🧹 Clearing all persistence data...");
        self.task_queue.clear_persistence().await
            .map_err(|e| anyhow::anyhow!("Failed to clear persistence: {}", e))?;
        println!("✅ All persistence data cleared");
        Ok(())
    }

    pub async fn get_completed_count(&self) -> u64 {
        self.executor.get_completed_count().await
    }

    pub async fn get_active_count(&self) -> usize {
        self.executor.get_active_count().await
    }

    pub async fn get_task_stats(&self) -> (usize, usize, u64) {
        let pending = self.task_queue.len().await;
        let active = self.get_active_count().await;
        let completed = self.get_completed_count().await;
        (pending, active, completed)
    }

    pub async fn get_task_count(&self) -> (usize, usize) {
        let pending = self.task_queue.len().await;
        let completed = self.executor.get_completed_count().await as usize;
        (pending, completed)
    }

    // NEW: Get cluster information
    pub async fn get_cluster_info(&self) -> Option<crate::node::ClusterInfo> {
        self.node_manager.as_ref().map(|nm| {
            nm.membership_manager().get_cluster_info()
        })
    }

    // NEW: Get node health information
    pub async fn get_node_health(&self) -> Option<Vec<crate::node::HealthScore>> {
        self.node_manager.as_ref().map(|nm| {
            nm.health_scorer().get_all_scores()
        })
    }

    // NEW: Get node registry information
    pub async fn get_nodes(&self) -> Option<Vec<crate::node::NodeInfo>> {
        self.node_manager.as_ref().map(|nm| {
            nm.registry().get_all_nodes()
        })
    }

    // NEW: Get distribution statistics
    pub async fn get_distribution_stats(&self) -> Option<DistributionStats> {
        self.distributor.as_ref().map(|distributor| {
            distributor.get_distribution_stats()
        })
    }

    // NEW: Check if distribution is enabled
    pub fn is_distribution_enabled(&self) -> bool {
        self.distributor.is_some()
    }

    // NEW: Update scoring weights for capacity matcher
    pub async fn update_scoring_weights(&self, weights: ScoringWeights) -> Result<()> {
        if let Some(distributor) = &self.distributor {
            println!("📊 Scoring weights would be updated to: {:?}", weights);
            println!("💡 Note: Requires redesign for mutable Arc access");
            Ok(())
        } else {
            Err(anyhow::anyhow!("Distribution not enabled"))
        }
    }

    // NEW: Get local node information
    pub async fn get_local_node_info(&self) -> Option<crate::node::NodeInfo> {
        self.node_manager.as_ref().and_then(|nm| {
            nm.registry().local_node()
        })
    }

    // NEW: Monitor remote tasks for timeouts and failures
    async fn monitor_remote_tasks(&self) {
        let mut interval = tokio::time::interval(Duration::from_secs(30));

        loop {
            interval.tick().await;

            if self.get_state().await != EngineState::Running {
                break;
            }

            if let Some(distributor) = &self.distributor {
                distributor.monitor_remote_tasks().await;
            }
        }
    }

    // NEW: Receive and process messages from other nodes
    async fn receive_messages(&self) {
        if let Some(distributor) = &self.distributor {
            let transport = distributor.get_transport();

            loop {
                if self.get_state().await != EngineState::Running {
                    break;
                }

                match transport.receive_messages().await {
                    Ok(messages) => {
                        for (node_id, message) in messages {
                            self.handle_node_message(node_id, message).await;
                        }
                    }
                    Err(e) => {
                        eprintln!("⚠️ Error receiving messages: {}", e);
                    }
                }

                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }

    // NEW: Handle incoming node messages
    async fn handle_node_message(&self, node_id: Uuid, message: NodeMessage) {
        if let Some(distributor) = &self.distributor {
            match message {
                NodeMessage::TaskStatusUpdate { .. } => {
                    let _ = distributor.handle_status_update(node_id, message).await;
                }
                NodeMessage::TaskResult { .. } => {
                    let _ = distributor.handle_task_result(node_id, message).await;
                }
                NodeMessage::HealthCheckRequest { message_id } => {
                    // Respond with health check
                    if let Some(node_manager) = &self.node_manager {
                        let health_score = node_manager.health_scorer().get_score(&node_id)
                            .map(|h| h.score)
                            .unwrap_or(0.0);

                        let response = NodeMessage::HealthCheckResponse {
                            message_id,
                            node_id: self.get_local_node_info().await.map(|n| n.id).unwrap_or_default(),
                            load: 0.5, // Simplified load calculation
                            available_resources: crate::node::NodeCapabilities::default(),
                            health_score,
                        };

                        let _ = distributor.get_transport().send_message(node_id, response).await;
                    }
                }
                _ => {
                    // Handle other message types
                    println!("📨 Received message from node {}: {:?}", node_id, message);
                }
            }
        }
    }

    // NEW: Broadcast a message to all nodes
    pub async fn broadcast_message(&self, message: NodeMessage) -> Result<()> {
        if let Some(distributor) = &self.distributor {
            distributor.get_transport().broadcast(message, None).await
                .map_err(|e| anyhow::anyhow!("Broadcast failed: {}", e))
        } else {
            Err(anyhow::anyhow!("Distribution not enabled"))
        }
    }

    // NEW: Send task to specific node
    pub async fn send_task_to_node(&self, task: &Task, node_id: Uuid) -> Result<Uuid> {
        self.execute_on_remote_node(task, node_id).await
    }

    // NEW: Get task tracker for monitoring
    pub async fn get_task_tracker(&self) -> Option<Arc<distribution::DistributedTaskTracker>> {
        self.distributor.as_ref().map(|d| d.get_task_tracker())
    }
}

/// Options for task distribution
#[derive(Debug, Clone)]
pub struct DistributionOptions {
    /// Affinity rule string (e.g., "zone=us-east-1,class=gpu")
    pub affinity_rule: Option<String>,

    /// Force local execution
    pub force_local: bool,

    /// Minimum node health score (0-100)
    pub min_health_score: Option<f64>,

    /// Resource requirements
    pub resource_requirements: Option<ResourceRequirements>,
}

impl Default for DistributionOptions {
    fn default() -> Self {
        Self {
            affinity_rule: None,
            force_local: false,
            min_health_score: Some(70.0),
            resource_requirements: None,
        }
    }
}

/// Apply distribution options to a QueueTask
fn apply_distribution_options_to_task(
    mut task: QueueTask,
    options: DistributionOptions
) -> QueueTask {
    // Start with existing payload or create new one
    let mut payload = task.payload.unwrap_or_else(|| serde_json::json!({}));

    if let serde_json::Value::Object(ref mut map) = payload {
        // Add affinity rules if present
        if let Some(affinity_rule) = options.affinity_rule {
            map.insert("affinity".to_string(), serde_json::Value::String(affinity_rule));
        }

        // Add force_local if true
        if options.force_local {
            map.insert("force_local".to_string(), serde_json::Value::Bool(true));
        }

        // Add min health score if specified
        if let Some(min_score) = options.min_health_score {
            map.insert("min_health_score".to_string(), serde_json::Value::from(min_score));
        }

        // Add resource requirements if specified
        if let Some(ref req) = options.resource_requirements {
            map.insert("cpu_cores".to_string(), serde_json::Value::from(req.min_cpu_cores));
            map.insert("memory_mb".to_string(), serde_json::Value::from(req.min_memory_mb));
            map.insert("needs_gpu".to_string(), serde_json::Value::from(req.needs_gpu));
            map.insert("estimated_duration_secs".to_string(),
                       serde_json::Value::from(req.estimated_duration_secs));

            if !req.required_task_types.is_empty() {
                let types: Vec<_> = req.required_task_types.iter()
                    .map(|t| serde_json::Value::String(t.clone()))
                    .collect();
                map.insert("required_task_types".to_string(), serde_json::Value::Array(types));
            }
        }
    }

    task.payload = Some(payload);
    task
}

/// Helper to create a QueueTask from a Task with distribution options
pub fn create_distributed_task(
    task: Task,
    priority: TaskPriority,
    distribution_options: Option<DistributionOptions>
) -> QueueTask {
    // First convert task to QueueTask
    let mut queue_task = QueueTask {
        id: task.id,
        name: task.name.clone(),
        scheduled_at: task.scheduled_at,
        priority,
        payload: task.payload.clone(),
        retry_count: task.retry_count,
        max_retries: task.max_retries,
        task_type: task.task_type.clone(),
    };

    // Apply distribution options if provided
    if let Some(options) = distribution_options {
        queue_task = apply_distribution_options_to_task(queue_task, options);
    }

    queue_task
}

/// Helper function for CLI distribution demo
pub async fn run_distribution_demo(engine: Arc<Engine>) -> Result<()> {
    if !engine.is_distribution_enabled() {
        println!("⚠️ Distribution is not enabled. Enable cluster mode in config.");
        return Ok(());
    }

    println!("🌐 Testing Node-Aware Distribution...");

    // Show current node status
    if let Some(nodes) = engine.get_nodes().await {
        println!("🖥️  Available Nodes ({}):", nodes.len());
        for node in &nodes {
            let status_emoji = match node.status {
                crate::node::NodeStatus::Active => "✅",
                crate::node::NodeStatus::Joining => "🔄",
                crate::node::NodeStatus::Unhealthy => "⚠️",
                crate::node::NodeStatus::Leaving => "👋",
                crate::node::NodeStatus::Dead => "💀",
            };
            println!("   {} Node {}: {} ({} CPU cores, {}MB RAM)",
                     status_emoji, node.id, node.hostname,
                     node.capabilities.cpu_cores, node.capabilities.memory_mb);
        }
    }

    // Task 1: GPU-intensive task
    println!("\n1️⃣ Scheduling GPU-intensive task...");
    let gpu_task = DistributedTaskFactory::gpu_task(
        "GPU Training Demo",
        4.0,
        8192,
    );
    let gpu_queue_task = create_distributed_task(
        gpu_task.clone(),
        TaskPriority::High,
        Some(DistributionOptions {
            affinity_rule: Some("class=gpu".to_string()),
            force_local: false,
            min_health_score: Some(80.0),
            resource_requirements: Some(ResourceRequirements {
                min_cpu_cores: 4.0,
                min_memory_mb: 8192,
                min_disk_mb: 1024,
                needs_gpu: true,
                needs_ssd: false,
                estimated_duration_secs: 30.0,
                required_task_types: vec!["gpu_training".to_string()],
            }),
        })
    );

    // Task 2: Memory-intensive task
    println!("2️⃣ Scheduling memory-intensive task...");
    let memory_task = DistributedTaskFactory::memory_intensive_task(
        "Memory Analytics Demo",
        2.0,
        16384,
    );
    let memory_queue_task = create_distributed_task(
        memory_task.clone(),
        TaskPriority::Medium,
        Some(DistributionOptions {
            affinity_rule: Some("class=high-memory".to_string()),
            force_local: false,
            min_health_score: Some(70.0),
            resource_requirements: Some(ResourceRequirements {
                min_cpu_cores: 2.0,
                min_memory_mb: 16384,
                min_disk_mb: 512,
                needs_gpu: false,
                needs_ssd: true,
                estimated_duration_secs: 10.0,
                required_task_types: vec!["analytics".to_string()],
            }),
        })
    );

    // Task 3: Low-latency task forced to local
    println!("3️⃣ Scheduling low-latency local task...");
    let low_latency_task = DistributedTaskFactory::low_latency_task(
        "Low-Latency Processing",
    );
    let latency_queue_task = create_distributed_task(
        low_latency_task.clone(),
        TaskPriority::High,
        Some(DistributionOptions {
            affinity_rule: None,
            force_local: true,
            min_health_score: Some(90.0),
            resource_requirements: Some(ResourceRequirements {
                min_cpu_cores: 1.0,
                min_memory_mb: 256,
                min_disk_mb: 10,
                needs_gpu: false,
                needs_ssd: false,
                estimated_duration_secs: 0.5,
                required_task_types: vec!["realtime".to_string()],
            }),
        })
    );

    // Schedule the tasks
    let tasks = vec![gpu_queue_task, memory_queue_task, latency_queue_task];

    for task in tasks {
        match engine.schedule_task(task.clone()).await {
            Ok(id) => {
                println!("✅ Distributed task scheduled: '{}' [{}]", task.name, id);
                // Show distribution summary
                let task_obj = Task::from_queue_task(&task);
                if task_obj.has_distribution_constraints() {
                    println!("   📋 Distribution: {}", task_obj.distribution_summary());
                }
            }
            Err(e) => println!("⚠️ Failed to schedule distributed task '{}': {}", task.name, e),
        }
    }

    // Show distribution stats
    if let Some(stats) = engine.get_distribution_stats().await {
        println!("\n📊 Distribution Stats:");
        println!("   - Total nodes: {}", stats.total_nodes);
        println!("   - Local node ID: {}", stats.local_node_id);
        println!("   - Decision threshold: {:.2}", stats.decision_threshold);
        println!("   - Scoring weights: CPU={:.2}, MEM={:.2}, DISK={:.2}, GPU={:.2}, LOAD={:.2}, HEALTH={:.2}",
                 stats.scoring_weights.cpu_weight,
                 stats.scoring_weights.memory_weight,
                 stats.scoring_weights.disk_weight,
                 stats.scoring_weights.gpu_weight,
                 stats.scoring_weights.load_weight,
                 stats.scoring_weights.health_weight);
    }

    // NEW: Show remote task monitoring info
    if let Some(tracker) = engine.get_task_tracker().await {
        let remote_tasks = tracker.get_all_remote_tasks().await;
        if !remote_tasks.is_empty() {
            println!("\n📡 Remote Tasks ({}):", remote_tasks.len());
            for task_state in remote_tasks {
                println!("   - Task {} on Node {}: {:?} ({:.0}%)",
                         task_state.task_id, task_state.node_id,
                         task_state.status, task_state.progress * 100.0);
            }
        }
    }

    println!("\n🎯 Distribution demo tasks scheduled.");
    println!("💡 Use 'stats' to see task execution and 'list' to see pending tasks.");
    println!("   Use 'nodes' to see node status and 'cluster' for cluster info.");
    println!("   Use 'remote-tasks' to see distributed task status.");

    Ok(())
}