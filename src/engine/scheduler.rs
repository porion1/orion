use super::task::{Task, TaskType, TaskStatus};
use crate::{SharedTaskQueue, QueueTask, TaskPriority}; // <- fixed import
use std::sync::Arc;
use std::time::{SystemTime, Duration};
use tokio::sync::watch;
use metrics::{counter, gauge, histogram};
use uuid::Uuid;

/// Scheduler struct: runs task event loop
#[derive(Debug)]
pub struct Scheduler {
    queue: Arc<SharedTaskQueue>,
    shutdown_tx: watch::Sender<bool>,
}

impl Scheduler {
    /// Create a new scheduler and start event loop
    pub fn new(queue: Arc<SharedTaskQueue>) -> Arc<Self> {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let scheduler = Arc::new(Self {
            queue,
            shutdown_tx: shutdown_tx.clone(),
        });

        let s = scheduler.clone();
        tokio::spawn(async move {
            s.run_event_loop(shutdown_rx).await;
        });

        scheduler
    }

    /// Main scheduler loop: dequeues ready tasks and executes them
    async fn run_event_loop(self: Arc<Self>, mut shutdown_rx: watch::Receiver<bool>) {
        println!("🔄 Scheduler event loop started");
        let mut tick = tokio::time::interval(Duration::from_secs(1));

        loop {
            tokio::select! {
                _ = tick.tick() => {
                    while let Some(task) = self.queue.dequeue().await {
                        self.execute(task).await;
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

    /// Execute a task asynchronously
    async fn execute(&self, qt: QueueTask) {
        let mut task = Task {
            id: qt.id,
            name: qt.name.clone(),
            task_type: qt.task_type.clone(),
            status: TaskStatus::Pending,
            scheduled_at: qt.scheduled_at,
            payload: qt.payload.clone(),
            created_at: SystemTime::now(),
            updated_at: SystemTime::now(),
            retry_count: qt.retry_count,
            max_retries: qt.max_retries,
            metadata: None,
        };

        let queue = self.queue.clone();

        tokio::spawn(async move {
            // Wait until scheduled time
            if let Ok(delay) = qt.scheduled_at.duration_since(SystemTime::now()) {
                if delay > Duration::ZERO {
                    tokio::time::sleep(delay).await;
                }
            }

            task.update_status(TaskStatus::Running);
            println!("✅ Executing Task {}: {}", task.id, task.name);

            let start = SystemTime::now();

            // Simulated task execution with timeout
            let exec_result = tokio::time::timeout(Duration::from_secs(10), async {
                tokio::time::sleep(Duration::from_millis(50)).await; // replace with actual work
            }).await;

            match exec_result {
                Ok(_) => task.update_status(TaskStatus::Completed),
                Err(_) => task.update_status(TaskStatus::Failed("Execution timed out".into())),
            }

            histogram!(
                "orion.scheduler.task_execution_time_ms",
                start.elapsed().unwrap().as_millis() as f64
            );
            counter!("orion.scheduler.tasks_executed_total", 1);

            // Retry failed task if allowed
            if matches!(task.status, TaskStatus::Failed(_)) {
                // Convert Task → QueueTask for retry
                let queue_task: QueueTask = (&task).into(); // use your existing impl elsewhere
                let _ = queue.retry_task(queue_task, Some(Duration::from_secs(5))).await;
                return;
            }

            // If recurring, schedule next occurrence
            if let TaskType::Recurring { .. } = task.task_type {
                if let Some(next) = task.create_next_occurrence() {
                    let _ = queue.enqueue((&next).into()).await;
                }
            }
        });

        counter!("orion.scheduler.tasks_scheduled_total", 1);
    }

    /// Update metrics for pending tasks
    async fn update_metrics(&self) {
        let pending = self.queue.len().await;
        gauge!("orion.scheduler.tasks_pending", pending as f64);
    }

    /// Public API to schedule a task
    pub async fn schedule(&self, task: QueueTask) -> Result<Uuid, String> {
        let id = task.id;
        self.queue.enqueue(task).await?;
        Ok(id)
    }

    /// Return shutdown sender
    pub fn get_shutdown_sender(&self) -> watch::Sender<bool> {
        self.shutdown_tx.clone()
    }
}
