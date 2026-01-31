use crate::engine::executor::TaskExecutor;
use crate::engine::queue::{QueueTask, SharedTaskQueue};
use crate::engine::task::{Task, TaskType};
use metrics::{counter, gauge};
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
    shutdown_tx: watch::Sender<bool>,
    shutdown_received: Arc<tokio::sync::RwLock<bool>>, // NEW: Track shutdown state
}

impl Scheduler {
    /// Create a new scheduler
    pub fn new(
        queue: Arc<SharedTaskQueue>,
        executor: Arc<TaskExecutor>,
    ) -> Self {
        let (shutdown_tx, _) = watch::channel(false);

        Self {
            queue,
            executor,
            shutdown_tx,
            shutdown_received: Arc::new(tokio::sync::RwLock::new(false)), // NEW: Initialize shutdown flag
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
                    // NEW: Check if shutdown received before processing tasks
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

                        // NEW: Set shutdown flag to stop recurring tasks
                        *self.shutdown_received.write().await = true;
                        println!("⏹️  Stopping new recurring task scheduling");

                        // FIXED: Handle the Result properly
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
        counter!("orion.scheduler.tasks_scheduled_total", 1);

        // Wait until scheduled execution time
        if let Ok(delay) = task.scheduled_at.duration_since(SystemTime::now()) {
            if delay > Duration::ZERO {
                tokio::time::sleep(delay).await;
            }
        }

        let task_id = task.id;
        let executor = Arc::clone(&self.executor);
        let queue = Arc::clone(&self.queue);

        // Execute the task
        executor.execute_task(task.clone()).await;

        // Check result after some delay to allow execution to complete
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Handle retry logic
        self.handle_task_result(task_id, &task, &executor, &queue).await;

        // Handle recurring tasks - NEW: Check shutdown flag
        if !*self.shutdown_received.read().await {
            self.handle_recurring_tasks(&task, &queue).await;
        } else {
            println!("⏹️  Skipping recurring task scheduling (shutdown in progress)");
        }
    }

    /// Handle task result and retry logic
    async fn handle_task_result(
        &self,
        task_id: Uuid,
        task: &QueueTask,
        executor: &Arc<TaskExecutor>,
        queue: &Arc<SharedTaskQueue>,
    ) {
        if let Some(result) = executor.get_result(task_id).await {
            if !result.success && task.retry_count < task.max_retries {
                // NEW: Check shutdown flag before scheduling retry
                if *self.shutdown_received.read().await {
                    println!("⏹️  Skipping retry for task {} (shutdown in progress)", task_id);
                    return;
                }

                let mut retry = task.clone();
                retry.retry_count += 1;
                retry.scheduled_at = SystemTime::now() + Duration::from_secs(5);

                match queue.enqueue(retry).await {
                    Ok(_) => {
                        counter!("orion.scheduler.tasks_retried_total", 1);
                    }
                    Err(e) => {
                        eprintln!("⚠️ Failed to enqueue retry for task {}: {}", task_id, e);
                        counter!("orion.scheduler.tasks_retry_failed_total", 1);
                    }
                }
            }
        }
    }

    /// Handle recurring tasks
    async fn handle_recurring_tasks(&self, task: &QueueTask, queue: &Arc<SharedTaskQueue>) {
        if let TaskType::Recurring { .. } = task.task_type {
            // NEW: Additional check for shutdown (belt and suspenders)
            if *self.shutdown_received.read().await {
                println!("⏹️  Skipping recurring task reschedule (shutdown in progress)");
                return;
            }

            if let Some(next) = Task::from_queue_task(task).create_next_occurrence() {
                match queue.enqueue((&next).into()).await {
                    Ok(_) => {
                        counter!("orion.scheduler.tasks_rescheduled_total", 1);
                    }
                    Err(e) => {
                        eprintln!("⚠️ Failed to reschedule recurring task {}: {}", task.id, e);
                        counter!("orion.scheduler.tasks_reschedule_failed_total", 1);
                    }
                }
            }
        }
    }

    /// Emit scheduler metrics
    async fn update_metrics(&self) {
        let pending = self.queue.len().await;
        gauge!("orion.scheduler.tasks_pending", pending as f64);

        // NEW: Also log shutdown state
        if *self.shutdown_received.read().await {
            gauge!("orion.scheduler.shutdown_state", 1.0);
        } else {
            gauge!("orion.scheduler.shutdown_state", 0.0);
        }
    }

    /// Public API: schedule a task
    pub async fn schedule(&self, task: QueueTask) -> anyhow::Result<Uuid> {
        // NEW: Check shutdown flag before scheduling new tasks
        if *self.shutdown_received.read().await {
            return Err(anyhow::anyhow!("Scheduler is shutting down, cannot schedule new tasks"));
        }

        let id = task.id;
        self.queue.enqueue(task).await?;
        Ok(id)
    }

    /// Shutdown signal handle
    pub fn shutdown_handle(&self) -> watch::Sender<bool> {
        self.shutdown_tx.clone()
    }

    // NEW: Check if scheduler is shutting down
    pub async fn is_shutting_down(&self) -> bool {
        *self.shutdown_received.read().await
    }

    // NEW: Get shutdown state for monitoring
    pub async fn get_shutdown_state(&self) -> String {
        if *self.shutdown_received.read().await {
            "shutting_down".to_string()
        } else {
            "running".to_string()
        }
    }
}