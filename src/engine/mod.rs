pub mod config;
pub mod node;
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

        Self {
            scheduler,
            executor,
            config,
            state: Arc::new(RwLock::new(EngineState::Init)),
            task_queue,
            shutdown_tx,
        }
    }

    pub async fn start(self: Arc<Self>) -> anyhow::Result<()> {
        self.set_state(EngineState::Running).await;
        println!("🚀 Engine starting...");

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

        // Send shutdown signals
        let _ = scheduler_shutdown.send(true);
        let _ = self.shutdown_tx.send(true);

        // NEW: Clean up completed tasks before stopping
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

    // NEW: Clean up completed tasks from persistence
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

    // NEW: Clear all persistence data (useful for testing/demo cleanup)
    pub async fn clear_persistence(&self) -> anyhow::Result<()> {
        println!("🧹 Clearing all persistence data...");
        self.task_queue.clear_persistence().await
            .map_err(|e| anyhow::anyhow!("Failed to clear persistence: {}", e))?;
        println!("✅ All persistence data cleared");
        Ok(())
    }

    // NEW: Get completed task count from executor
    pub async fn get_completed_count(&self) -> u64 {
        self.executor.get_completed_count().await
    }

    // NEW: Get active task count from executor
    pub async fn get_active_count(&self) -> usize {
        self.executor.get_active_count().await
    }

    // NEW: Get task statistics (pending, active, completed)
    pub async fn get_task_stats(&self) -> (usize, usize, u64) {
        let pending = self.task_queue.len().await;
        let active = self.get_active_count().await;
        let completed = self.get_completed_count().await;
        (pending, active, completed)
    }

    // NEW: Helper method to get task count (pending + completed) - updated
    pub async fn get_task_count(&self) -> (usize, usize) {
        let pending = self.task_queue.len().await;
        let completed = self.executor.get_completed_count().await as usize;
        (pending, completed)
    }
}