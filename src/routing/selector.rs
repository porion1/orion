use crate::engine::executor::TaskExecutor;
use crate::engine::queue::{QueueTask, SharedTaskQueue};
use crate::engine::task::{Task, TaskType};
use crate::node::registry::NodeRegistry;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::watch;
use tokio::time::interval;
use uuid::Uuid;

/// Scheduler drives task dispatching and lifecycle orchestration
#[derive(Debug)]
pub struct Scheduler {
    queue: Arc<SharedTaskQueue>,
    executor: Arc<TaskExecutor>,
    node_registry: Option<Arc<NodeRegistry>>,
    shutdown_tx: watch::Sender<bool>,
    shutdown_received: Arc<tokio::sync::RwLock<bool>>,
}

impl Scheduler {
    /// Create a new scheduler
    pub fn new(
        queue: Arc<SharedTaskQueue>,
        executor: Arc<TaskExecutor>,
        node_registry: Option<Arc<NodeRegistry>>,
    ) -> Self {
        let (shutdown_tx, _) = watch::channel(false);

        Self {
            queue,
            executor,
            node_registry,
            shutdown_tx,
            shutdown_received: Arc::new(tokio::sync::RwLock::new(false)),
        }
    }

    /// Start the scheduler main loop
    pub async fn start(self: Arc<Self>) -> anyhow::Result<()> {
        println!("🔄 Scheduler started");

        let mut ticker = interval(Duration::from_secs(1));
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if *self.shutdown_received.read().await {
                        println!("⏸️  Scheduler paused (shutdown received)");
                        continue;
                    }

                    self.process_ready_tasks().await;
                    self.update_metrics().await;
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        println!("🛑 Scheduler shutting down");
                        *self.shutdown_received.write().await = true;

                        if let Err(e) = self.queue.persist_all().await {
                            eprintln!("⚠️ Failed to persist queue state during shutdown: {}", e);
                        }
                        break;
                    }
                }
            }
        }

        println!("✅ Scheduler stopped");
        Ok(())
    }

    /// Dequeue and submit all available tasks
    async fn process_ready_tasks(&self) {
        while let Some(task) = self.queue.dequeue().await {
            self.dispatch(task).await;
        }
    }

    /// Dispatch task to executor
    async fn dispatch(&self, task: QueueTask) {
        // Wait until scheduled execution time
        if let Ok(delay) = task.scheduled_at.duration_since(SystemTime::now()) {
            if delay > Duration::ZERO {
                tokio::time::sleep(delay).await;
            }
        }

        let task_id = task.id;

        // Execute the task
        self.executor.execute_task(task.clone()).await;

        // Allow executor to publish result
        tokio::time::sleep(Duration::from_millis(100)).await;

        self.handle_task_result(task_id, &task).await;

        if !*self.shutdown_received.read().await {
            self.handle_recurring_tasks(&task).await;
        }
    }

    /// Handle task result and retry logic
    async fn handle_task_result(&self, task_id: Uuid, task: &QueueTask) {
        if let Some(result) = self.executor.get_result(task_id).await {
            if !result.success && task.retry_count < task.max_retries {
                if *self.shutdown_received.read().await {
                    println!("⏹️  Skipping retry for task {} (shutdown in progress)", task_id);
                    return;
                }

                let mut retry = task.clone();
                retry.retry_count += 1;
                retry.scheduled_at = SystemTime::now() + Duration::from_secs(5);

                match self.queue.enqueue(retry).await {
                    Ok(_) => {
                        println!("🔄 Task {} scheduled for retry", task_id);
                    }
                    Err(e) => {
                        eprintln!("⚠️ Failed to enqueue retry for task {}: {}", task_id, e);
                    }
                }
            }
        }
    }

    /// Handle recurring tasks
    async fn handle_recurring_tasks(&self, task: &QueueTask) {
        if let TaskType::Recurring { .. } = task.task_type {
            if *self.shutdown_received.read().await {
                println!("⏹️  Skipping recurring task reschedule (shutdown in progress)");
                return;
            }

            if let Some(next) = Task::from_queue_task(task).create_next_occurrence() {
                match self.queue.enqueue((&next).into()).await {
                    Ok(_) => {
                        println!("📅 Recurring task {} rescheduled", task.id);
                    }
                    Err(e) => {
                        eprintln!("⚠️ Failed to reschedule recurring task {}: {}", task.id, e);
                    }
                }
            }
        }
    }

    /// Emit scheduler metrics
    async fn update_metrics(&self) {
        let pending = self.queue.len().await;
        println!("📊 Pending tasks: {}", pending);

        if let Some(node_registry) = &self.node_registry {
            let node_count = node_registry.get_all_nodes().len();
            println!("🖥️  Available nodes: {}", node_count);
        }

        if *self.shutdown_received.read().await {
            println!("🛑 Scheduler shutdown in progress");
        }
    }

    /// Public API: schedule a task
    pub async fn schedule(&self, task: QueueTask) -> anyhow::Result<Uuid> {
        if *self.shutdown_received.read().await {
            return Err(anyhow::anyhow!("Scheduler is shutting down, cannot schedule new tasks"));
        }

        let id = task.id;
        self.queue.enqueue(task).await?;

        println!("✅ Task scheduled: {}", id);
        Ok(id)
    }

    /// Shutdown signal handle
    pub fn shutdown_handle(&self) -> watch::Sender<bool> {
        self.shutdown_tx.clone()
    }

    // Check if scheduler is shutting down
    pub async fn is_shutting_down(&self) -> bool {
        *self.shutdown_received.read().await
    }

    // Get shutdown state for monitoring
    pub async fn get_shutdown_state(&self) -> String {
        if *self.shutdown_received.read().await {
            "shutting_down".to_string()
        } else {
            "running".to_string()
        }
    }
}