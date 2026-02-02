pub mod config;
pub mod scheduler;
pub mod state;
pub mod task;
pub mod queue;
pub mod executor;

pub use config::EngineConfig;
pub use scheduler::Scheduler;
pub use state::EngineState;
pub use task::Task;
pub use queue::{QueueTask, SharedTaskQueue, QueueConfig, TaskPriority};
pub use executor::TaskExecutor;

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{watch, RwLock};
use uuid::Uuid;

/// Main Engine struct
#[derive(Debug)]
pub struct Engine {
    pub scheduler: Arc<Scheduler>,
    pub executor: Arc<TaskExecutor>,
    pub config: EngineConfig,
    pub state: Arc<RwLock<EngineState>>,
    pub task_queue: Arc<SharedTaskQueue>,
    shutdown_tx: watch::Sender<bool>,

    // NEW: Node Manager (optional - only initialized if cluster is enabled)
    pub node_manager: Option<Arc<crate::node::NodeManager>>,
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

        // NEW: Initialize Node Manager only if cluster is enabled
        let node_manager = if config.is_cluster_enabled() {
            // Get the cluster config and convert it to NodeConfig using the From trait
            let cluster_config = config.cluster_config.as_ref().unwrap();
            let node_config: crate::node::NodeConfig = cluster_config.clone().into();

            match crate::node::NodeManager::new(node_config) {
                Ok(manager) => {
                    println!("🌐 Node Manager initialized");
                    Some(Arc::new(manager))
                }
                Err(e) => {
                    eprintln!("⚠️ Failed to initialize Node Manager: {}", e);
                    None
                }
            }
        } else {
            None
        };

        Self {
            scheduler,
            executor,
            config,
            state: Arc::new(RwLock::new(EngineState::Init)),
            task_queue,
            shutdown_tx,
            node_manager,
        }
    }

    pub async fn start(self: Arc<Self>) -> anyhow::Result<()> {
        self.set_state(EngineState::Running).await;
        println!("🚀 Engine starting...");

        // NEW: Start Node Manager if enabled
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
        tokio::spawn(async move {
            if let Err(e) = scheduler_clone.start().await {
                eprintln!("Scheduler error: {}", e);
            }
        });

        // Start executor loop
        let executor_clone = Arc::clone(&self.executor);
        tokio::spawn(async move {
            executor_clone.start().await;
        });

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

        // NEW: Stop Node Manager if enabled
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

    pub async fn schedule_task(&self, task: QueueTask) -> anyhow::Result<Uuid> {
        // NEW: Node-aware scheduling if Node Manager is enabled
        if let Some(node_manager) = &self.node_manager {
            // Check if we should distribute this task to other nodes
            if self.should_distribute_task(&task) {
                return self.distribute_task_to_node(task).await;
            }
        }

        // Original scheduling logic (unchanged)
        self.task_queue.enqueue(task.clone()).await
            .map_err(|e| anyhow::anyhow!("Failed to enqueue task: {}", e))?;
        Ok(task.id)
    }

    pub async fn cancel_task(&self, id: Uuid) -> bool {
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

        println!("✅ Cleaned {} completed tasks from persistence ({} errors)",
                 cleaned_count, error_count);
    }

    pub async fn clear_persistence(&self) -> anyhow::Result<()> {
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

    // NEW: Helper method to determine if task should be distributed
    fn should_distribute_task(&self, task: &QueueTask) -> bool {
        // Simple heuristic: distribute high priority or resource-intensive tasks
        task.priority == TaskPriority::High ||
            task.payload.as_ref().map_or(false, |p|
                p.as_object().map_or(false, |obj|
                    obj.contains_key("distribute") && obj["distribute"].as_bool().unwrap_or(false)
                )
            )
    }

    // NEW: Distribute task to best available node
    async fn distribute_task_to_node(&self, task: QueueTask) -> anyhow::Result<Uuid> {
        if let Some(_node_manager) = &self.node_manager {
            // Get healthy nodes
            let healthy_nodes = _node_manager.health_scorer().get_healthy_nodes(70.0);

            if healthy_nodes.is_empty() {
                // No healthy remote nodes, fall back to local
                return self.task_queue.enqueue(task.clone()).await
                    .map_err(|e| anyhow::anyhow!("Failed to enqueue task locally: {}", e))
                    .map(|_| task.id);
            }

            // TODO: Implement actual task distribution logic
            // For now, just log and schedule locally
            println!("🌐 Would distribute task {} to {} healthy node(s)", task.id, healthy_nodes.len());
        }

        // Fallback to local scheduling
        self.task_queue.enqueue(task.clone()).await
            .map_err(|e| anyhow::anyhow!("Failed to enqueue task: {}", e))?;
        Ok(task.id)
    }

    // NEW: Get cluster information if Node Manager is enabled
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
}