use super::task::{Task, TaskType, TaskStatus};
use crate::queue::{SharedTaskQueue, QueueTask};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::watch;
use metrics::{counter, gauge, histogram};
use uuid::Uuid;

/// Scheduler metrics tracking (can expand later)
#[derive(Debug, Clone)]
pub struct SchedulerMetrics {
    pub tasks_scheduled: u64,
    pub tasks_executed: u64,
    pub tasks_failed: u64,
    pub tasks_pending: usize,
    pub tasks_recurring: usize,
    pub avg_execution_time_ms: f64,
}

/// Main Scheduler
#[derive(Debug)]
pub struct Scheduler {
    queue: Arc<SharedTaskQueue>,
    shutdown_tx: watch::Sender<bool>,
}

impl Scheduler {
    /// Create a new Scheduler with an injected queue
    pub fn new(queue: Arc<SharedTaskQueue>) -> Arc<Self> {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let scheduler = Arc::new(Self {
            queue: Arc::clone(&queue),
            shutdown_tx: shutdown_tx.clone(),
        });

        // Spawn event loop
        let scheduler_clone = Arc::clone(&scheduler);
        tokio::spawn(async move {
            scheduler_clone.run_event_loop(shutdown_rx).await;
        });

        // Load persisted tasks in background
        let scheduler_load = Arc::clone(&scheduler);
        tokio::spawn(async move {
            scheduler_load.queue.load_persisted().await;
        });

        scheduler
    }

    /// Main async event loop
    async fn run_event_loop(self: Arc<Self>, mut shutdown_rx: watch::Receiver<bool>) {
        println!("🔄 Scheduler event loop started");

        // Initialize metrics
        counter!("orion.scheduler.tasks_scheduled_total", 0);
        counter!("orion.scheduler.tasks_executed_total", 0);
        counter!("orion.scheduler.tasks_failed_total", 0);
        counter!("orion.scheduler.tasks_cancelled_total", 0);
        gauge!("orion.scheduler.tasks_pending", 0.0);
        gauge!("orion.scheduler.tasks_recurring", 0.0);

        let mut interval = tokio::time::interval(Duration::from_secs(1));

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    // Dequeue and execute all ready tasks
                    while let Some(task) = self.queue.dequeue().await {
                        self.process_task(task).await;
                    }
                    self.update_metrics().await;
                }

                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        println!("🛑 Scheduler shutting down...");
                        self.queue.persist_all().await;
                        break;
                    }
                }
            }
        }
    }

    /// Process a single QueueTask
    async fn process_task(&self, task: QueueTask) {
        let task_id = task.id;
        let task_name = task.name.clone();
        let scheduled_at = task.scheduled_at;

        // Use payload directly (Option<Value>)
        let payload: Option<serde_json::Value> = task.payload.clone();

        let runtime_task = Task {
            id: task.id,
            name: task.name,
            task_type: TaskType::OneShot,
            status: TaskStatus::Pending,
            scheduled_at,
            payload,
            created_at: SystemTime::now(),
            updated_at: SystemTime::now(),
            retry_count: 0,
            max_retries: 3,
            metadata: None,
        };

        let mut task_for_spawn = runtime_task.clone();

        let now = SystemTime::now();
        let delay = if scheduled_at > now {
            scheduled_at.duration_since(now).unwrap_or(Duration::from_secs(0))
        } else {
            Duration::from_secs(0)
        };

        tokio::spawn(async move {
            if delay > Duration::from_secs(0) {
                tokio::time::sleep(delay).await;
            }

            task_for_spawn.update_status(TaskStatus::Running);
            println!("✅ Executing Task {}: {}", task_id, task_name);

            let start_time = SystemTime::now();

            // Simulate actual work
            tokio::time::sleep(Duration::from_millis(50)).await;

            task_for_spawn.update_status(TaskStatus::Completed);

            let exec_time = start_time.elapsed().map(|d| d.as_millis() as f64).unwrap_or(0.0);

            counter!("orion.scheduler.tasks_executed_total", 1);
            histogram!("orion.scheduler.task_execution_time_ms", exec_time);
        });

        counter!("orion.scheduler.tasks_scheduled_total", 1);
    }

    /// Schedule a task (enqueue it)
    pub async fn schedule(&self, task: QueueTask) -> Result<Uuid, String> {
        let task_id = task.id;
        self.queue.enqueue(task).await?;
        Ok(task_id)
    }

    /// Cancel a task by ID (placeholder, extend later)
    pub async fn cancel_task(&self, _task_id: Uuid) -> bool {
        println!("⚠️ Cancel task not implemented yet");
        false
    }

    /// Peek all pending tasks
    pub async fn get_pending_tasks(&self) -> Vec<QueueTask> {
        self.queue.get_all_pending().await
    }

    /// Update metrics
    async fn update_metrics(&self) {
        let pending = self.queue.len().await;
        gauge!("orion.scheduler.tasks_pending", pending as f64);
    }

    /// Expose shutdown sender
    pub fn get_shutdown_sender(&self) -> watch::Sender<bool> {
        self.shutdown_tx.clone()
    }
}
