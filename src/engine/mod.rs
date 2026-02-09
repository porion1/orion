pub mod config;
pub mod scheduler;
pub mod state;
pub mod task;
pub mod queue;
pub mod executor;
pub mod distribution;
pub mod node;

// Import routing module from crate root
pub use crate::routing;
use crate::engine::task::ResourceRequirements;
pub use config::EngineConfig;
pub use scheduler::Scheduler;
pub use state::EngineState;
pub use task::Task;
pub use queue::{QueueTask, SharedTaskQueue, QueueConfig, TaskPriority};
pub use executor::TaskExecutor;
pub use distribution::{
    NodeAwareDistributor, AssignmentDecision, TaskRequirements,
    ScoringWeights, DistributionError, AffinityRuleEngine,
    DistributionStats, DistributedTaskFactory,
    NodeMessage, RemoteTaskState, RemoteTaskStatus, TaskResult,
    SerializedTask, NodeTransport, DistributedTaskTracker
};

// Re-export routing types
pub use crate::routing::{
    Router, RouterConfig, RoutingStrategy, RoundRobinStrategy,
    LeastLoadedStrategy, LatencyAwareStrategy, FailoverStrategy,
    HybridStrategy, NodeMetrics, NodeCapacity, RoutingContext,
    TaskAdapter, TaskRouterIntegrator
};

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use uuid::Uuid;
use anyhow::Result;
use std::collections::HashMap;

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

    // Node-Aware Distributor
    pub distributor: Option<Arc<NodeAwareDistributor>>,

    // Router for load balancing
    pub router: Option<Arc<Router>>,
    pub task_router_integrator: Option<Arc<TaskRouterIntegrator>>,

    // Routing configuration
    pub routing_config: Option<RouterConfig>,
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

        // CREATE SCORER AND NODE REGISTRY FOR SCHEDULER
        use crate::scoring::StaticScorer;
        let scorer = Box::new(StaticScorer::new(HashMap::new()));

        // Create node registry (even if empty for now)
        let node_registry = Arc::new(
            crate::node::registry::NodeRegistry::new(None)
                .unwrap_or_else(|_| {
                    println!("⚠️ Failed to create node registry, using empty registry");
                    crate::node::registry::NodeRegistry::new(None).expect("Fallback node registry creation failed")
                })
        );

        // Scheduler with scoring capability
        let scheduler = Arc::new(Scheduler::new(
            scorer,
            Arc::clone(&task_queue),
            Arc::clone(&executor),
            node_registry,
        ));

        // Initialize components based on configuration
        let (node_manager, distributor, router, task_router_integrator, routing_config) =
            Self::initialize_components(&config);

        Self {
            scheduler,
            executor,
            config,
            state: Arc::new(tokio::sync::RwLock::new(EngineState::Init)),
            task_queue,
            shutdown_tx,
            node_manager,
            distributor,
            router,
            task_router_integrator,
            routing_config,
        }
    }

    /// Initialize all components based on configuration
    fn initialize_components(config: &EngineConfig) -> (
        Option<Arc<crate::node::NodeManager>>,
        Option<Arc<NodeAwareDistributor>>,
        Option<Arc<Router>>,
        Option<Arc<TaskRouterIntegrator>>,
        Option<RouterConfig>
    ) {
        let mut node_manager = None;
        let mut distributor = None;
        let mut router = None;
        let mut task_router_integrator = None;
        // FIXED: Use None for now since routing_config doesn't exist in EngineConfig
        let routing_config = None;

        // Initialize if cluster is enabled
        if config.is_cluster_enabled() {
            if let Some(cluster_config) = &config.cluster_config {
                let node_config: crate::node::NodeConfig = cluster_config.clone().into();

                // Initialize Node Manager
                match crate::node::NodeManager::new(node_config) {
                    Ok(manager) => {
                        let manager_arc = Arc::new(manager);
                        node_manager = Some(Arc::clone(&manager_arc));

                        // Initialize Distributor
                        match NodeAwareDistributor::new(Arc::clone(&manager_arc), None) {
                            Ok(dist) => {
                                println!("🌐 Node Manager and Distributor initialized");
                                distributor = Some(Arc::new(dist));
                            }
                            Err(e) => {
                                eprintln!("⚠️ Failed to initialize Distributor: {}", e);
                            }
                        }

                        // Initialize Router if routing config exists
                        if let Some(ref router_config) = routing_config {
                            match Self::initialize_router(router_config) {
                                Ok((r, i)) => {
                                    router = Some(r);
                                    task_router_integrator = Some(i);
                                    println!("📡 Router initialized with strategy: {}",
                                             router_config.default_strategy);
                                }
                                Err(e) => {
                                    eprintln!("⚠️ Failed to initialize Router: {}", e);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("⚠️ Failed to initialize Node Manager: {}", e);
                    }
                }
            }
        }

        (node_manager, distributor, router, task_router_integrator, routing_config)
    }

    /// Initialize router with strategies
    fn initialize_router(config: &RouterConfig) -> Result<(Arc<Router>, Arc<TaskRouterIntegrator>)> {
        let router = Arc::new(Router::new(config.clone()));
        let integrator = Arc::new(TaskRouterIntegrator::new());

        // Note: Strategies will be initialized when router starts
        // This is done asynchronously in start() method

        Ok((router, integrator))
    }

    pub async fn start(self: Arc<Self>) -> Result<()> {
        self.set_state(EngineState::Running).await;
        println!("🚀 Engine starting...");

        // Initialize routing strategies if router exists
        if let Some(router) = &self.router {
            if let Some(config) = &self.routing_config {
                self.initialize_routing_strategies(router, config).await;
            }
        }

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

        // Start remote task monitoring if distribution is enabled
        if self.is_distribution_enabled() {
            let engine_clone = Arc::clone(&self);
            let _remote_monitor_handle = tokio::spawn(async move {
                engine_clone.monitor_remote_tasks().await;
            });
            println!("📡 Remote task monitoring started");

            // Start message receiver if distribution is enabled
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

    /// Initialize routing strategies
    async fn initialize_routing_strategies(&self, router: &Arc<Router>, config: &RouterConfig) {
        let mut strategies = router.strategies.write().await;

        // Add Round Robin strategy
        strategies.insert(
            "round_robin".to_string(),
            Box::new(RoundRobinStrategy::new()),
        );

        // Add Least Loaded strategy
        strategies.insert(
            "least_loaded".to_string(),
            Box::new(LeastLoadedStrategy::new(config.clone())),
        );

        // Add Latency Aware strategy
        strategies.insert(
            "latency_aware".to_string(),
            Box::new(LatencyAwareStrategy::new(config.clone())),
        );

        // Add Failover strategy
        strategies.insert(
            "failover".to_string(),
            Box::new(FailoverStrategy::new(
                config.clone(),
                Arc::clone(&router.failover_handler),
            )),
        );

        // Add Hybrid strategy
        strategies.insert(
            "hybrid".to_string(),
            Box::new(HybridStrategy::new(config.clone())),
        );

        println!("🎯 Routing strategies initialized: round_robin, least_loaded, latency_aware, failover, hybrid");
    }

    pub async fn set_state(&self, new_state: EngineState) {
        let current_state = *self.state.read().await;
        println!("State changed: {:?} → {:?}", current_state, new_state);
        *self.state.write().await = new_state;
    }

    pub async fn get_state(&self) -> EngineState {
        *self.state.read().await
    }

    /// Schedule a task
    pub async fn schedule_task(&self, task: QueueTask) -> Result<Uuid> {
        // Check if task should be distributed
        if let Some(distributor) = &self.distributor {
            if Self::should_distribute_task(&task) {
                return self.schedule_with_intelligent_routing(distributor, task).await;
            }
        }

        // Fall back to local-only scheduling
        self.schedule_task_locally(task).await
    }

    /// Intelligent routing that tries multiple approaches
    async fn schedule_with_intelligent_routing(
        &self,
        distributor: &Arc<NodeAwareDistributor>,
        task: QueueTask
    ) -> Result<Uuid> {
        let task_obj = Task::from_queue_task(&task);

        // Try routing first if available
        if let Some((integrator, router)) = self.get_routing_components() {
            match self.route_task_with_router(integrator, router, &task_obj, &task).await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    println!("⚠️ Router failed: {}, falling back to distributor", e);
                    // Continue to distributor fallback
                }
            }
        }

        // Fall back to distributor logic
        self.schedule_with_distributor(distributor, task_obj, task).await
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

    /// Get routing components if available
    fn get_routing_components(&self) -> Option<(&Arc<TaskRouterIntegrator>, &Arc<Router>)> {
        if let (Some(integrator), Some(router)) = (&self.task_router_integrator, &self.router) {
            Some((integrator, router))
        } else {
            None
        }
    }

    /// Route task using router
    async fn route_task_with_router(
        &self,
        integrator: &Arc<TaskRouterIntegrator>,
        router: &Arc<Router>,
        task_obj: &Task,
        original_queue_task: &QueueTask,
    ) -> Result<Uuid> {
        println!("🚦 Using router for task '{}'", task_obj.name);

        // Get available nodes
        let available_nodes = match self.get_available_nodes().await {
            Some(nodes) if !nodes.is_empty() => nodes,
            _ => return Err(anyhow::anyhow!("No nodes available for routing")),
        };

        // Convert to routing format
        let routing_task = crate::routing::Task::from_engine_task(task_obj);
        let routing_nodes: Vec<_> = available_nodes.into_iter()
            .map(Self::convert_to_routing_node)
            .collect();

        // Use router to select node
        match integrator.route_orion_task(&routing_task, &routing_nodes, router).await {
            Ok(selected_node_id) => {
                println!("📍 Router selected node: {} for task: {}", selected_node_id, task_obj.name);

                // Check if it's local node
                if self.is_local_node(&selected_node_id).await {
                    println!("📌 Task '{}' assigned to local execution", task_obj.name);
                    return self.schedule_task_locally(original_queue_task.clone()).await;
                }

                // Execute remotely
                self.execute_on_remote_node_by_id(task_obj, &selected_node_id).await
            }
            Err(e) => Err(anyhow::anyhow!("Router failed: {}", e)),
        }
    }

    /// Get available nodes from node manager
    async fn get_available_nodes(&self) -> Option<Vec<crate::node::NodeInfo>> {
        self.node_manager
            .as_ref()
            .and_then(|nm| Some(nm.registry().get_all_nodes()))
    }

    /// Check if node is local
    async fn is_local_node(&self, node_id: &str) -> bool {
        self.get_local_node_info().await
            .map(|local_node| local_node.id.to_string() == node_id)
            .unwrap_or(false)
    }

    /// Execute task on remote node by node ID string
    async fn execute_on_remote_node_by_id(&self, task: &Task, node_id: &str) -> Result<Uuid> {
        match Uuid::parse_str(node_id) {
            Ok(node_uuid) => self.execute_on_remote_node(task, node_uuid).await,
            Err(_) => {
                println!("⚠️ Invalid node ID format: {}", node_id);
                Err(anyhow::anyhow!("Invalid node ID format"))
            }
        }
    }

    /// Convert NodeInfo to routing::Node
    fn convert_to_routing_node(node_info: crate::node::NodeInfo) -> crate::routing::Node {
        // Get node tags from capabilities
        let mut tags = HashMap::new();

        // Add capabilities as tags
        if node_info.capabilities.cpu_cores >= 4 {
            tags.insert("high_cpu".to_string(), "true".to_string());
        }
        if node_info.capabilities.memory_mb >= 8192 {
            tags.insert("high_memory".to_string(), "true".to_string());
        }

        // Add supported task types
        if !node_info.capabilities.supported_task_types.is_empty() {
            tags.insert(
                "supported_task_types".to_string(),
                node_info.capabilities.supported_task_types.join(",")
            );
        }

        // Create NodeMetrics
        let metrics = NodeMetrics {
            cpu_usage: 0.5, // TODO: Get actual CPU usage
            memory_usage: 0.5, // TODO: Get actual memory usage
            queue_length: 0,
            processing_tasks: 0,
            avg_latency_ms: 0.0,
            last_heartbeat: crate::routing::SerializableInstant::now(),
            is_healthy: node_info.status == crate::node::NodeStatus::Active,
            capacity: NodeCapacity {
                max_concurrent_tasks: node_info.capabilities.max_concurrent_tasks as usize,
                max_queue_length: 100,
                supported_task_types: node_info.capabilities.supported_task_types.clone(),
                regions: vec!["default".to_string()],
            },
            tags,
        };

        crate::routing::Node {
            id: node_info.id.to_string(),
            is_healthy: node_info.status == crate::node::NodeStatus::Active,
            metrics,
        }
    }

    /// Schedule using distributor (original logic)
    async fn schedule_with_distributor(
        &self,
        distributor: &Arc<NodeAwareDistributor>,
        task_obj: Task,
        queue_task: QueueTask
    ) -> Result<Uuid> {
        match distributor.assign_task(&task_obj).await {
            Ok(decision) => {
                match decision {
                    AssignmentDecision::LocalExecution { node_id } => {
                        println!("📌 Task '{}' assigned to local node: {}", queue_task.name, node_id);
                        self.schedule_task_locally(queue_task).await
                    }
                    AssignmentDecision::RemoteExecution { node_id, estimated_latency, cost } => {
                        println!("🌐 Task '{}' assigned to remote node: {} (latency: {:.2}ms, cost: {:.2})",
                                 queue_task.name, node_id, estimated_latency, cost);

                        match distributor.execute_remote_task(&task_obj, node_id).await {
                            Ok(task_id) => {
                                println!("✅ Remote task '{}' dispatched to node {}", queue_task.name, node_id);
                                self.register_remote_task_callback(&queue_task.name, task_obj.id, distributor).await;
                                Ok(task_id)
                            }
                            Err(e) => {
                                println!("⚠️ Failed to execute task '{}' remotely: {}, falling back to local",
                                         queue_task.name, e);
                                self.schedule_task_locally(queue_task).await
                            }
                        }
                    }
                    AssignmentDecision::NoSuitableNode { reason } => {
                        println!("⚠️ No suitable node for task '{}': {}", queue_task.name, reason);
                        println!("📌 Fallback: Scheduling '{}' locally", queue_task.name);
                        self.schedule_task_locally(queue_task).await
                    }
                }
            }
            Err(e) => {
                println!("⚠️ Distribution failed for task '{}': {}, falling back to local",
                         queue_task.name, e);
                self.schedule_task_locally(queue_task).await
            }
        }
    }

    /// Register callback for remote task completion
    async fn register_remote_task_callback(
        &self,
        task_name: &str,
        task_id: Uuid,
        distributor: &Arc<NodeAwareDistributor>
    ) {
        let task_name = task_name.to_string();
        distributor.get_task_tracker().register_callback(task_id, move |state| {
            let task_name = task_name.clone();
            match state.status {
                RemoteTaskStatus::Completed => {
                    println!("✅ Remote task '{}' completed on node {}", task_name, state.node_id);
                }
                RemoteTaskStatus::Failed(ref error) => {
                    println!("❌ Remote task '{}' failed on node {}: {}", task_name, state.node_id, error);
                }
                _ => {} // Ignore other statuses
            }
        }).await;
    }

    /// Schedule a task locally
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
        let task_with_options = apply_distribution_options_to_task(task, distribution_options);
        self.schedule_task(task_with_options).await
    }

    /// Execute a task on a specific remote node
    pub async fn execute_on_remote_node(&self, task: &Task, node_id: Uuid) -> Result<Uuid> {
        if let Some(distributor) = &self.distributor {
            distributor.execute_remote_task(task, node_id)
                .await
                .map_err(|e| anyhow::anyhow!("Remote execution failed: {}", e))
        } else {
            Err(anyhow::anyhow!("Distribution not enabled"))
        }
    }

    /// Get status of remote tasks - FIXED: Changed return type from RemoteTaskState to Option<RemoteTaskState>
    pub async fn get_remote_task_status(&self, task_id: Uuid) -> Option<RemoteTaskState> {
        if let Some(distributor) = &self.distributor {
            let task_tracker = distributor.get_task_tracker();
            task_tracker.get_task_state(task_id).await
        } else {
            None
        }
    }

    /// Get all remote tasks
    pub async fn get_all_remote_tasks(&self) -> Vec<RemoteTaskState> {
        if let Some(distributor) = &self.distributor {
            let task_tracker = distributor.get_task_tracker();
            task_tracker.get_all_remote_tasks().await
        } else {
            Vec::new()
        }
    }

    /// Get remote task result - FIXED: Changed return type from TaskResult to Option<TaskResult>
    pub async fn get_remote_task_result(&self, task_id: Uuid) -> Option<TaskResult> {
        if let Some(distributor) = &self.distributor {
            let task_tracker = distributor.get_task_tracker();
            task_tracker.get_task_result(task_id).await
        } else {
            None
        }
    }

    pub async fn cancel_task(&self, id: Uuid) -> bool {
        // First check if it's a remote task
        if let Some(distributor) = &self.distributor {
            let task_tracker = distributor.get_task_tracker();
            let state = task_tracker.get_task_state(id).await;
            if let Some(state) = state {
                if matches!(state.status, RemoteTaskStatus::Pending | RemoteTaskStatus::Running) {
                    let message = NodeMessage::TaskStatusUpdate {
                        message_id: Uuid::new_v4(),
                        task_id: id,
                        status: RemoteTaskStatus::Cancelled,
                        progress: 0.0,
                        error: Some("Cancelled by user".to_string()),
                    };

                    let transport = distributor.get_transport();
                    if transport.send_message(state.node_id, message).await.is_ok() {
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

        let completed_results = self.executor.get_all_results().await;
        let mut cleaned_count = 0;
        let mut error_count = 0;

        for result in completed_results {
            match self.task_queue.remove_from_persistence(result.task_id).await {
                Ok(_) => cleaned_count += 1,
                Err(e) => {
                    eprintln!("⚠️ Failed to remove task {} from persistence: {}", result.task_id, e);
                    error_count += 1;
                }
            }
        }

        // Cleanup old remote tasks
        if let Some(distributor) = &self.distributor {
            let task_tracker = distributor.get_task_tracker();
            task_tracker.cleanup_old_tasks(Duration::from_secs(3600)).await;
        }

        println!("✅ Cleaned {} completed tasks from persistence ({} errors)", cleaned_count, error_count);
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
        let completed = self.get_completed_count().await as usize;
        (pending, completed)
    }

    // Get cluster information
    pub async fn get_cluster_info(&self) -> Option<crate::node::ClusterInfo> {
        self.node_manager.as_ref().map(|nm| {
            nm.membership_manager().get_cluster_info()
        })
    }

    // Get node health information
    pub async fn get_node_health(&self) -> Option<Vec<crate::node::HealthScore>> {
        self.node_manager.as_ref().map(|nm| {
            nm.health_scorer().get_all_scores()
        })
    }

    // Get node registry information
    pub async fn get_nodes(&self) -> Option<Vec<crate::node::NodeInfo>> {
        self.node_manager.as_ref().map(|nm| {
            nm.registry().get_all_nodes()
        })
    }

    // Get distribution statistics
    pub async fn get_distribution_stats(&self) -> Option<DistributionStats> {
        self.distributor.as_ref().map(|distributor| {
            distributor.get_distribution_stats()
        })
    }

    /// Get routing statistics - FIXED: Removed async block inside Some()
    pub async fn get_routing_stats(&self) -> Option<crate::routing::RoutingStatistics> {
        if let Some(router) = &self.router {
            Some(router.metrics_collector.get_statistics().await)
        } else {
            None
        }
    }

    // Check if distribution is enabled
    pub fn is_distribution_enabled(&self) -> bool {
        self.distributor.is_some()
    }

    /// Check if routing is enabled
    pub fn is_routing_enabled(&self) -> bool {
        self.router.is_some()
    }

    /// Check if routing is ready to use
    pub async fn is_routing_ready(&self) -> bool {
        if !self.is_routing_enabled() {
            return false;
        }

        if let Some(nodes) = self.get_nodes().await {
            !nodes.is_empty()
        } else {
            false
        }
    }

    // Update scoring weights for capacity matcher
    pub async fn update_scoring_weights(&self, weights: ScoringWeights) -> Result<()> {
        if let Some(distributor) = &self.distributor {
            println!("📊 Scoring weights would be updated to: {:?}", weights);
            // Note: Actual implementation would update the distributor's weights
            Ok(())
        } else {
            Err(anyhow::anyhow!("Distribution not enabled"))
        }
    }

    // Get local node information
    pub async fn get_local_node_info(&self) -> Option<crate::node::NodeInfo> {
        self.node_manager.as_ref().and_then(|nm| {
            nm.registry().local_node()
        })
    }

    /// Validate routing configuration
    pub async fn validate_routing_config(&self) -> Result<()> {
        if let Some(config) = &self.routing_config {
            config.validate().map_err(|e| anyhow::anyhow!(e))?;
        }
        Ok(())
    }

    /// Switch routing strategy - FIXED: Removed async block inside Some()
    pub async fn switch_routing_strategy(&self, strategy_name: &str) -> Result<()> {
        if let Some(router) = &self.router {
            let strategies = router.strategies.read().await;
            if !strategies.contains_key(strategy_name) {
                return Err(anyhow::anyhow!("Unknown strategy: {}", strategy_name));
            }

            let mut active_strategy = router.active_strategy.write().await;
            *active_strategy = strategy_name.to_string();
            println!("🔄 Switched routing strategy to: {}", strategy_name);
            Ok(())
        } else {
            Err(anyhow::anyhow!("Routing not enabled"))
        }
    }

    /// Get current routing strategy - FIXED: Removed async block inside Some()
    pub async fn get_current_routing_strategy(&self) -> Option<String> {
        if let Some(router) = &self.router {
            Some(router.active_strategy.read().await.clone())
        } else {
            None
        }
    }

    /// Get available routing strategies - FIXED: Removed async block inside Some()
    pub async fn get_available_routing_strategies(&self) -> Vec<String> {
        if let Some(router) = &self.router {
            router.strategies.read().await.keys().cloned().collect()
        } else {
            Vec::new()
        }
    }

    // Monitor remote tasks for timeouts and failures
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

    // Receive and process messages from other nodes
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

    // Handle incoming node messages
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
                    if let Some(node_manager) = &self.node_manager {
                        let health_score = node_manager.health_scorer().get_score(&node_id)
                            .map(|h| h.score)
                            .unwrap_or(0.0);

                        let response = NodeMessage::HealthCheckResponse {
                            message_id,
                            node_id: self.get_local_node_info().await.map(|n| n.id).unwrap_or_default(),
                            load: 0.5,
                            available_resources: crate::node::NodeCapabilities::default(),
                            health_score,
                        };

                        let _ = distributor.get_transport().send_message(node_id, response).await;
                    }
                }
                _ => {
                    println!("📨 Received message from node {}: {:?}", node_id, message);
                }
            }
        }
    }

    // Broadcast a message to all nodes
    pub async fn broadcast_message(&self, message: NodeMessage) -> Result<()> {
        if let Some(distributor) = &self.distributor {
            distributor.get_transport().broadcast(message, None).await
                .map_err(|e| anyhow::anyhow!("Broadcast failed: {}", e))
        } else {
            Err(anyhow::anyhow!("Distribution not enabled"))
        }
    }

    // Send task to specific node
    pub async fn send_task_to_node(&self, task: &Task, node_id: Uuid) -> Result<Uuid> {
        self.execute_on_remote_node(task, node_id).await
    }

    // Get task tracker for monitoring
    pub async fn get_task_tracker(&self) -> Option<Arc<DistributedTaskTracker>> {
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

            // FIXED: Use `map` not `json_map`
            if !req.required_task_types.is_empty() {
                let types: Vec<_> = req.required_task_types.iter()
                    .map(|t: &String| serde_json::Value::String(t.clone()))
                    .collect();
                map.insert("required_task_types".to_string(), serde_json::Value::Array(types)); // Fixed: map.insert
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

    // Show routing information if enabled
    if engine.is_routing_enabled() {
        if let Some(strategy) = engine.get_current_routing_strategy().await {
            println!("🎯 Current routing strategy: {}", strategy);
        }
        if let Some(stats) = engine.get_routing_stats().await {
            println!("📊 Routing stats: {} successful, {} failed selections",
                     stats.successful_selections, stats.failed_selections);
        }
    }

    // Create and schedule demo tasks
    let tasks = vec![
        ("GPU-intensive task", crate::engine::task::DistributedTaskFactory::gpu_task("GPU Training Demo", 4.0, 8192)),
        ("Memory-intensive task", crate::engine::task::DistributedTaskFactory::memory_intensive_task("Memory Analytics Demo", 2.0, 16384)),
        ("Low-latency task", Ok(crate::engine::task::DistributedTaskFactory::low_latency_task("Low-Latency Processing"))),
    ];

    for (task_type, task) in tasks {
        println!("\n📝 Scheduling {}...", task_type);
        match task {
            Ok(task) => {
                let queue_task = create_distributed_task(
                    task.clone(),
                    TaskPriority::High,
                    None
                );

                match engine.schedule_task(queue_task.clone()).await {
                    Ok(id) => {
                        println!("✅ Task scheduled: '{}' [{}]", task.name, id);
                    }
                    Err(e) => println!("⚠️ Failed to schedule task '{}': {}", task.name, e),
                }
            }
            Err(e) => println!("⚠️ Failed to create task for {}: {}", task_type, e),
        }
    }

    // Show distribution stats
    if let Some(stats) = engine.get_distribution_stats().await {
        println!("\n📊 Distribution Stats:");
        println!("   - Total nodes: {}", stats.total_nodes);
        println!("   - Local node ID: {}", stats.local_node_id);
        println!("   - Decision threshold: {:.2}", stats.decision_threshold);
    }

    // Show remote task monitoring info
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

    println!("\n🎯 Distribution demo completed.");
    println!("💡 Use 'stats' to see task execution and 'list' to see pending tasks.");
    println!("   Use 'nodes' to see node status and 'cluster' for cluster info.");

    Ok(())
}