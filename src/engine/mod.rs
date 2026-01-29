pub mod config;
pub mod node;
pub mod scheduler;
pub mod state;
pub mod task;
pub mod queue;

// Re-export main types
pub use config::EngineConfig;
pub use scheduler::Scheduler;
pub use state::EngineState;
pub use task::Task;
pub use queue::{SharedTaskQueue, QueueConfig, QueueTask, TaskPriority};

use tokio::sync::watch;
use std::sync::Arc;
use uuid::Uuid;

/// Main Engine struct
#[derive(Debug)]
pub struct Engine {
    pub scheduler: Arc<Scheduler>,
    pub config: EngineConfig,
    pub state: EngineState,
    pub task_queue: Arc<SharedTaskQueue>,
    shutdown_tx: watch::Sender<bool>,
}

impl Engine {
    /// Initialize engine with config
    pub fn new(config: EngineConfig) -> Self {
        let (shutdown_tx, _) = watch::channel(false);

        // Start metrics server if enabled
        if config.should_enable_metrics() {
            match crate::metrics::start_metrics_server(config.metrics_port) {
                Ok(_) => println!("📊 Metrics server started on port {}", config.metrics_port),
                Err(e) => {
                    eprintln!("⚠️ Failed to start metrics server: {}", e);
                    eprintln!("⚠️ Metrics will not be available");
                }
            }
        }

        // Configure shared task queue
        let queue_config = QueueConfig {
            max_queue_size: 10_000,
            persistence_path: config.persistence_path.clone(),
        };
        let task_queue = Arc::new(SharedTaskQueue::new(queue_config));

        // Inject same queue into scheduler
        let scheduler = Scheduler::new(Arc::clone(&task_queue));

        Self {
            scheduler,
            config,
            task_queue,
            state: EngineState::Init,
            shutdown_tx,
        }
    }

    /// Update engine state with logging
    pub fn set_state(&mut self, new_state: EngineState) {
        println!("🔄 Engine state changed: {:?} → {:?}", self.state, new_state);
        self.state = new_state;
    }

    /// Start engine asynchronously
    pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.set_state(EngineState::Running);
        println!("🚀 Engine starting with config: {:?}", self.config);

        if self.config.should_enable_metrics() {
            println!(
                "📊 Metrics available at: http://localhost:{}/metrics",
                self.config.metrics_port
            );
        }

        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let scheduler_shutdown_tx = self.scheduler.get_shutdown_sender();

        // Wait for shutdown signal (Ctrl+C or internal)
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("Received shutdown signal, stopping Engine...");
            }
            _ = shutdown_rx.changed() => {
                println!("Received internal shutdown signal...");
            }
        }

        self.set_state(EngineState::Draining);

        // Notify scheduler to stop and persist tasks
        let _ = scheduler_shutdown_tx.send(true);
        let _ = self.shutdown_tx.send(true);

        // Wait briefly to allow pending tasks to complete
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        self.set_state(EngineState::Stopped);
        println!("✅ Engine stopped gracefully.");
        Ok(())
    }

    /// Schedule a new task into the shared queue
    pub async fn schedule_task(
        &self,
        task: QueueTask,
    ) -> Result<Uuid, Box<dyn std::error::Error + Send + Sync>> {
        self.task_queue.enqueue(task.clone()).await?;
        Ok(task.id)
    }

    /// Dequeue the next available task (priority + scheduled time)
    pub async fn dequeue_task(&self) -> Option<QueueTask> {
        self.task_queue.dequeue().await
    }

    /// Get all pending tasks in the queue
    pub async fn get_pending_tasks(&self) -> Vec<QueueTask> {
        self.task_queue.get_all_pending().await
    }

    /// Print current engine status for debugging
    pub async fn print_status(&self) {
        let pending = self.task_queue.len().await;
        println!("=== Orion Engine Status ===");
        println!("State: {:?}", self.state);
        println!("Config: {:?}", self.config);
        println!("Pending tasks: {}", pending);
        println!("===========================");
    }
}
