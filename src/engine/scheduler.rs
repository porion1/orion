use tokio::sync::{mpsc, watch};
use super::task::{Task, TaskType};
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

#[derive(Debug)]
pub struct Scheduler {
    tx: mpsc::Sender<Task>,
    shutdown_tx: watch::Sender<bool>,
}

impl Scheduler {
    pub fn new() -> Arc<Self> {
        let (tx, mut rx) = mpsc::channel::<Task>(1000);
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

        let scheduler = Arc::new(Scheduler { tx, shutdown_tx });

        // Clone for async task
        let scheduler_clone = Arc::clone(&scheduler);
        tokio::spawn(async move {
            println!("🔄 Scheduler started");

            loop {
                tokio::select! {
                    Some(task) = rx.recv() => {
                        scheduler_clone.process_task(task).await;
                    }
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            println!("🛑 Scheduler shutting down...");
                            break;
                        }
                    }
                }
            }
        });

        scheduler
    }

    async fn process_task(&self, task: Task) {
        // Calculate delay until execution time
        let now = Instant::now();
        let delay = if task.scheduled_at > now {
            task.scheduled_at.duration_since(now)
        } else {
            Duration::from_secs(0)
        };

        // Clone the sender for potential rescheduling
        let tx_clone = self.tx.clone();
        let task_id = task.id;
        let task_name = task.name.clone();
        let is_recurring = matches!(task.task_type, TaskType::Recurring { .. });

        tokio::spawn(async move {
            // Wait until execution time
            if delay > Duration::from_secs(0) {
                tokio::time::sleep(delay).await;
            }

            println!("✅ Executing Task {}: {}", task_id, task_name);

            // If it's recurring, reschedule it for the next occurrence
            if is_recurring {
                // Create the next occurrence
                if let Some(next_task) = task.create_next_occurrence() {
                    let next_delay = if next_task.scheduled_at > Instant::now() {
                        next_task.scheduled_at.duration_since(Instant::now())
                    } else {
                        Duration::from_secs(1) // Minimum 1 second delay
                    };

                    println!("🔄 Task {} scheduled to run again in {:.1} seconds", task_id, next_delay.as_secs_f32());

                    // Reschedule the task
                    if let Err(e) = tx_clone.send(next_task).await {
                        eprintln!("❌ Failed to reschedule task {}: {}", task_id, e);
                    }
                } else {
                    eprintln!("⚠️ Could not create next occurrence for task {}", task_id);
                }
            }
        });
    }

    pub async fn schedule(&self, task: Task) -> Result<Uuid, Box<dyn std::error::Error + Send + Sync>> {
        let task_id = task.id;

        self.tx.send(task).await.map_err(|e| {
            Box::new(e) as Box<dyn std::error::Error + Send + Sync>
        })?;

        Ok(task_id)
    }

    pub fn get_shutdown_sender(&self) -> watch::Sender<bool> {
        self.shutdown_tx.clone()
    }
}