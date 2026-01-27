use tokio::sync::{mpsc, watch};
use super::task::{Task, TaskType, TaskStatus};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use uuid::Uuid;
use sled::{Db, Tree};
use serde_json;
use std::collections::HashMap;
use metrics::{counter, gauge, histogram};

#[derive(Debug, Clone)]
pub struct SchedulerMetrics {
    pub tasks_scheduled: u64,
    pub tasks_executed: u64,
    pub tasks_failed: u64,
    pub tasks_pending: usize,
    pub tasks_recurring: usize,
    pub avg_execution_time_ms: f64,
}

#[derive(Debug)]
pub struct Scheduler {
    tx: mpsc::Sender<Task>,
    db: Arc<Db>,
    tasks_tree: Tree,
    shutdown_tx: watch::Sender<bool>,
    pending_tasks: Arc<tokio::sync::RwLock<HashMap<Uuid, Task>>>,
}

impl Scheduler {
    pub fn new(persistence_path: Option<&str>) -> Arc<Self> {
        // Initialize sled database for persistence
        let db_path = persistence_path.unwrap_or("orion-scheduler");
        let db = sled::open(db_path).expect("Failed to open sled database");
        let tasks_tree = db.open_tree("tasks").expect("Failed to open tasks tree");

        let (tx, rx) = mpsc::channel::<Task>(1000);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let pending_tasks = Arc::new(tokio::sync::RwLock::new(HashMap::new()));

        let scheduler = Arc::new(Self {
            tx: tx.clone(),
            db: Arc::new(db),
            tasks_tree,
            shutdown_tx,
            pending_tasks: pending_tasks.clone(),
        });

        // Clone for async tasks
        let scheduler_clone = Arc::clone(&scheduler);

        // Start main scheduler loop
        tokio::spawn(async move {
            scheduler_clone.run_event_loop(rx, shutdown_rx).await;
        });

        // Load persisted tasks on startup
        let scheduler_for_load = Arc::clone(&scheduler);
        tokio::spawn(async move {
            scheduler_for_load.load_persisted_tasks().await;
        });

        scheduler
    }

    pub fn new_simple() -> Arc<Self> {
        Self::new(None)
    }

    async fn load_persisted_tasks(&self) {
        println!("📂 Loading persisted tasks...");
        let mut loaded_count = 0;

        for result in self.tasks_tree.iter() {
            match result {
                Ok((_key, value)) => {
                    if let Ok(task) = serde_json::from_slice::<Task>(&value) {
                        // Only load pending tasks
                        if let TaskStatus::Pending = task.status {
                            self.pending_tasks.write().await.insert(task.id, task.clone());

                            // Send to the scheduler for processing
                            if let Err(e) = self.tx.send(task).await {
                                eprintln!("❌ Failed to reload task: {}", e);
                            }

                            loaded_count += 1;
                        }
                    }
                }
                Err(e) => eprintln!("❌ Error loading persisted task: {}", e),
            }
        }

        println!("✅ Loaded {} persisted tasks", loaded_count);
    }

    async fn run_event_loop(
        self: Arc<Self>,
        mut rx: mpsc::Receiver<Task>,
        mut shutdown_rx: watch::Receiver<bool>,
    ) {
        println!("🔄 Scheduler event loop started");

        // Initialize metrics with zero values
        counter!("orion.scheduler.tasks_scheduled_total", 0);
        counter!("orion.scheduler.tasks_executed_total", 0);
        counter!("orion.scheduler.tasks_failed_total", 0);
        counter!("orion.scheduler.tasks_cancelled_total", 0);
        gauge!("orion.scheduler.tasks_pending", 0.0);
        gauge!("orion.scheduler.tasks_recurring", 0.0);

        let mut interval = tokio::time::interval(Duration::from_secs(1));

        loop {
            tokio::select! {
                Some(task) = rx.recv() => {
                    self.handle_new_task(task).await;
                }

                _ = interval.tick() => {
                    self.update_metrics().await;
                }

                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        println!("🛑 Scheduler shutting down...");
                        self.persist_state().await;
                        break;
                    }
                }
            }
        }
    }

    async fn handle_new_task(&self, task: Task) {
        let task_id = task.id;
        let task_name = task.name.clone();
        let is_recurring = matches!(task.task_type, TaskType::Recurring { .. });

        // Persist task to database
        self.persist_task(&task).await;

        // Store in pending tasks map
        self.pending_tasks.write().await.insert(task_id, task.clone());

        // Update metrics
        counter!("orion.scheduler.tasks_scheduled_total", 1);
        if is_recurring {
            gauge!("orion.scheduler.tasks_recurring", 1.0);
        }

        println!("📅 Scheduled task: {} (ID: {})", task_name, task_id);

        // Process the task
        self.process_task(task).await;
    }

    async fn persist_task(&self, task: &Task) {
        if let Ok(serialized) = serde_json::to_vec(task) {
            let _ = self.tasks_tree.insert(task.id.as_bytes(), serialized);
        }
    }

    async fn process_task(&self, task: Task) {
        let task_id = task.id;
        let task_name = task.name.clone();
        let is_recurring = matches!(task.task_type, TaskType::Recurring { .. });

        // Calculate delay until execution
        let delay = if task.scheduled_at > SystemTime::now() {
            task.scheduled_at.duration_since(SystemTime::now())
                .unwrap_or(Duration::from_secs(0))
        } else {
            Duration::from_secs(0)
        };

        // Clone what we need for the async block
        let tx_clone = self.tx.clone();
        let task_clone = task.clone();
        let pending_tasks_clone = Arc::clone(&self.pending_tasks);
        let tasks_tree_clone = self.tasks_tree.clone();

        tokio::spawn(async move {
            // Wait until execution time
            if delay > Duration::from_secs(0) {
                tokio::time::sleep(delay).await;
            }

            // Update task status to Running and persist
            {
                let mut task_to_update = task_clone.clone();
                task_to_update.update_status(TaskStatus::Running);
                if let Ok(serialized) = serde_json::to_vec(&task_to_update) {
                    let _ = tasks_tree_clone.insert(task_id.as_bytes(), serialized);
                }
            }

            let start_time = SystemTime::now();
            println!("✅ Executing Task {}: {}", task_id, task_name);

            // Simulate task execution
            tokio::time::sleep(Duration::from_millis(50)).await;

            let execution_time = start_time.elapsed()
                .map(|d| d.as_millis() as f64)
                .unwrap_or(0.0);

            // Update execution metrics
            counter!("orion.scheduler.tasks_executed_total", 1);
            histogram!("orion.scheduler.task_execution_time_ms", execution_time);

            // Remove from pending tasks
            pending_tasks_clone.write().await.remove(&task_id);

            // Update task status to Completed and persist
            {
                let mut completed_task = task_clone.clone();
                completed_task.update_status(TaskStatus::Completed);
                if let Ok(serialized) = serde_json::to_vec(&completed_task) {
                    let _ = tasks_tree_clone.insert(task_id.as_bytes(), serialized);
                }
            }

            // If it's recurring, reschedule it for the next occurrence
            if is_recurring {
                if let Some(next_task) = task_clone.create_next_occurrence() {
                    let next_delay = if next_task.scheduled_at > SystemTime::now() {
                        next_task.scheduled_at.duration_since(SystemTime::now())
                            .unwrap_or(Duration::from_secs(1))
                    } else {
                        Duration::from_secs(1)
                    };

                    println!("🔄 Task {} scheduled to run again in {:.1} seconds", task_id, next_delay.as_secs_f32());

                    if let Err(e) = tx_clone.send(next_task).await {
                        eprintln!("❌ Failed to reschedule task {}: {}", task_id, e);
                        counter!("orion.scheduler.tasks_failed_total", 1);
                    }
                }
            }
        });
    }

    async fn update_metrics(&self) {
        let pending_count = self.pending_tasks.read().await.len();
        gauge!("orion.scheduler.tasks_pending", pending_count as f64);
    }

    async fn persist_state(&self) {
        println!("💾 Persisting scheduler state...");

        // Flush database
        if let Err(e) = self.db.flush() {
            eprintln!("❌ Failed to flush database: {}", e);
        }

        println!("✅ Scheduler state persisted");
    }

    pub async fn schedule(&self, task: Task) -> Result<Uuid, Box<dyn std::error::Error + Send + Sync>> {
        let task_id = task.id;

        self.tx.send(task).await.map_err(|e| {
            Box::new(e) as Box<dyn std::error::Error + Send + Sync>
        })?;

        Ok(task_id)
    }

    pub async fn cancel_task(&self, task_id: Uuid) -> bool {
        let mut removed = false;

        // Remove from pending tasks
        if self.pending_tasks.write().await.remove(&task_id).is_some() {
            removed = true;
        }

        // Update task status to Cancelled in database
        if let Ok(Some(value)) = self.tasks_tree.get(task_id.as_bytes()) {
            if let Ok(mut task) = serde_json::from_slice::<Task>(&value) {
                task.update_status(TaskStatus::Cancelled);
                self.persist_task(&task).await;
                removed = true;
                counter!("orion.scheduler.tasks_cancelled_total", 1);
            }
        }

        if removed {
            self.update_metrics().await;
        }

        removed
    }

    pub async fn get_pending_tasks(&self) -> Vec<Task> {
        self.pending_tasks.read().await.values().cloned().collect()
    }

    pub fn get_shutdown_sender(&self) -> watch::Sender<bool> {
        self.shutdown_tx.clone()
    }
}