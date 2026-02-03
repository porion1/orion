use crate::engine::task::TaskType;
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sled::Db;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;
use uuid::Uuid;
use metrics::{counter, gauge};

/// --------------------------------
/// Task priority levels
/// --------------------------------
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum TaskPriority {
    High = 3,
    Medium = 2,
    Low = 1,
}

// Add FromStr implementation for TaskPriority
impl FromStr for TaskPriority {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "high" => Ok(TaskPriority::High),
            "medium" => Ok(TaskPriority::Medium),
            "low" => Ok(TaskPriority::Low),
            _ => Err(format!("Invalid priority: {}", s)),
        }
    }
}

/// --------------------------------
/// Task stored in the queue
/// --------------------------------
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueTask {
    pub id: Uuid,
    pub name: String,
    pub scheduled_at: SystemTime,
    pub priority: TaskPriority,
    pub payload: Option<Value>,
    pub retry_count: u32,
    pub max_retries: u32,
    pub task_type: TaskType,
}

/// --------------------------------
/// Queue configuration
/// --------------------------------
#[derive(Debug, Clone)]
pub struct QueueConfig {
    pub max_queue_size: usize,
    persistence_path: Option<String>,
}

impl QueueConfig {
    pub fn new(max_queue_size: usize, persistence_path: Option<String>) -> Self {
        Self {
            max_queue_size,
            persistence_path,
        }
    }

    pub fn persistence_path(&self) -> Option<&String> {
        self.persistence_path.as_ref()
    }
}

/// --------------------------------
/// Wrapper for BinaryHeap ordering
/// Earlier scheduled tasks come first
/// --------------------------------
#[derive(Debug, Clone)]
struct QueueTaskWrapper {
    task: QueueTask,
}

impl PartialEq for QueueTaskWrapper {
    fn eq(&self, other: &Self) -> bool {
        self.task.id == other.task.id
    }
}
impl Eq for QueueTaskWrapper {}

impl PartialOrd for QueueTaskWrapper {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for QueueTaskWrapper {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is max-heap, so reverse order
        other
            .task
            .scheduled_at
            .cmp(&self.task.scheduled_at)
            .then_with(|| self.task.id.cmp(&other.task.id))
    }
}

/// --------------------------------
/// Internal queue state
/// --------------------------------
#[derive(Debug)]
struct QueueInner {
    high: BinaryHeap<QueueTaskWrapper>,
    medium: BinaryHeap<QueueTaskWrapper>,
    low: BinaryHeap<QueueTaskWrapper>,
    pending_tasks: HashMap<Uuid, QueueTask>,
    task_ids_in_memory: HashSet<Uuid>, // NEW: Track IDs in memory for duplicate checking
}

/// --------------------------------
/// Shared task queue
/// --------------------------------
#[derive(Debug, Clone)]
pub struct SharedTaskQueue {
    inner: Arc<RwLock<QueueInner>>,
    config: QueueConfig,
    db: Option<Arc<Db>>,
}

impl SharedTaskQueue {
    /// Create a new queue
    pub fn new(config: QueueConfig) -> Self {
        let db = config.persistence_path().map(|path| {
            Arc::new(sled::open(path).expect("Failed to open queue DB"))
        });

        let inner = QueueInner {
            high: BinaryHeap::new(),
            medium: BinaryHeap::new(),
            low: BinaryHeap::new(),
            pending_tasks: HashMap::new(),
            task_ids_in_memory: HashSet::new(), // NEW: Initialize ID tracker
        };

        Self {
            inner: Arc::new(RwLock::new(inner)),
            config,
            db,
        }
    }

    /// Enqueue a new task
    pub async fn enqueue(&self, task: QueueTask) -> Result<()> {
        let mut inner = self.inner.write().await;

        if inner.pending_tasks.len() >= self.config.max_queue_size {
            counter!("orion.scheduler.tasks_rejected_total", 1);
            return Err(anyhow!("Queue is full (max: {})", self.config.max_queue_size));
        }

        // FIX: Check both pending_tasks AND task_ids_in_memory
        if inner.task_ids_in_memory.contains(&task.id) {
            return Err(anyhow!("Task already exists with ID: {}", task.id));
        }

        let wrapper = QueueTaskWrapper { task: task.clone() };
        match task.priority {
            TaskPriority::High => inner.high.push(wrapper),
            TaskPriority::Medium => inner.medium.push(wrapper),
            TaskPriority::Low => inner.low.push(wrapper),
        }

        // FIX: Insert into both tracking structures
        inner.pending_tasks.insert(task.id, task.clone());
        inner.task_ids_in_memory.insert(task.id);

        counter!("orion.scheduler.tasks_scheduled_total", 1);
        gauge!("orion.scheduler.tasks_pending", inner.pending_tasks.len() as f64);

        // Persist task
        if let Some(db) = &self.db {
            let serialized = serde_json::to_vec(&task)
                .map_err(|e| anyhow!("Failed to serialize task: {}", e))?;
            db.insert(task.id.as_bytes(), serialized)
                .map_err(|e| anyhow!("Failed to persist task to DB: {}", e))?;
        }

        Ok(())
    }

    /// Dequeue next ready task
    pub async fn dequeue(&self) -> Option<QueueTask> {
        let mut inner = self.inner.write().await;
        let now = SystemTime::now();

        fn pop_ready(heap: &mut BinaryHeap<QueueTaskWrapper>, now: SystemTime) -> Option<QueueTask> {
            while let Some(wrapper) = heap.peek() {
                if wrapper.task.scheduled_at <= now {
                    return heap.pop().map(|w| w.task);
                } else {
                    break;
                }
            }
            None
        }

        let task_opt = pop_ready(&mut inner.high, now)
            .or_else(|| pop_ready(&mut inner.medium, now))
            .or_else(|| pop_ready(&mut inner.low, now))?;

        // FIX: Remove from both tracking structures
        inner.pending_tasks.remove(&task_opt.id);
        inner.task_ids_in_memory.remove(&task_opt.id);

        gauge!("orion.scheduler.tasks_pending", inner.pending_tasks.len() as f64);

        if let Some(db) = &self.db {
            let _ = db.remove(task_opt.id.as_bytes());
        }

        Some(task_opt)
    }

    /// Retry task with optional delay
    pub async fn retry_task(&self, mut task: QueueTask, delay: Option<Duration>) -> Result<()> {
        if task.retry_count >= task.max_retries {
            counter!("orion.scheduler.tasks_failed_total", 1);
            return Err(anyhow!("Max retries reached (max: {})", task.max_retries));
        }

        task.retry_count += 1;

        if let Some(d) = delay {
            let queue_clone = self.clone();
            tokio::spawn(async move {
                tokio::time::sleep(d).await;
                if let Err(e) = queue_clone.enqueue(task).await {
                    eprintln!("⚠️ Failed to re-enqueue task: {}", e);
                }
            });
            return Ok(());
        }

        self.enqueue(task).await
    }

    /// Return all pending tasks
    pub async fn get_all_pending(&self) -> Vec<QueueTask> {
        let inner = self.inner.read().await;
        inner.pending_tasks.values().cloned().collect()
    }

    /// Return pending count
    pub async fn len(&self) -> usize {
        let inner = self.inner.read().await;
        inner.pending_tasks.len()
    }

    /// Check if queue is empty
    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }

    /// Check if a task ID exists in memory
    pub async fn contains_task(&self, task_id: &Uuid) -> bool {
        let inner = self.inner.read().await;
        inner.task_ids_in_memory.contains(task_id)
    }

    /// Persist all tasks
    pub async fn persist_all(&self) -> Result<()> {
        if let Some(db) = &self.db {
            let inner = self.inner.read().await;
            for task in inner.pending_tasks.values() {
                if let Ok(serialized) = serde_json::to_vec(task) {
                    db.insert(task.id.as_bytes(), serialized)
                        .map_err(|e| anyhow!("Failed to persist task {}: {}", task.id, e))?;
                }
            }
        }
        Ok(())
    }

    /// Load persisted tasks from DB
    pub async fn load_persisted(&self) -> Result<()> {
        if let Some(db) = &self.db {
            let mut loaded_count = 0;
            let mut skipped_count = 0;
            let mut error_count = 0;

            for result in db.iter() {
                match result {
                    Ok((_key, value)) => {
                        match serde_json::from_slice::<QueueTask>(&value) {
                            Ok(task) => {
                                // FIX: Check if task already exists in memory before enqueueing
                                if self.contains_task(&task.id).await {
                                    // Task already in memory, skip it
                                    skipped_count += 1;
                                    continue;
                                }

                                // Check queue size limit
                                if self.len().await >= self.config.max_queue_size {
                                    println!("⚠️ Queue full, stopping persistence load");
                                    break;
                                }

                                // Enqueue the task
                                match self.enqueue(task).await {
                                    Ok(_) => {
                                        loaded_count += 1;
                                    }
                                    Err(e) => {
                                        eprintln!("⚠️ Failed to load persisted task: {}", e);
                                        error_count += 1;
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("⚠️ Failed to deserialize persisted task: {}", e);
                                error_count += 1;
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("⚠️ Error reading from persistence DB: {}", e);
                        error_count += 1;
                    }
                }
            }

            println!("📂 Persistence load complete: {} loaded, {} skipped (duplicates), {} errors",
                     loaded_count, skipped_count, error_count);
        } else {
            println!("📂 No persistence configured, skipping load");
        }
        Ok(())
    }

    /// Remove task from persistence (for completed tasks)
    pub async fn remove_from_persistence(&self, task_id: Uuid) -> Result<()> {
        if let Some(db) = &self.db {
            db.remove(task_id.as_bytes())
                .map_err(|e| anyhow!("Failed to remove task from persistence: {}", e))?;
        }
        Ok(())
    }

    /// Clear all persisted tasks from DB
    pub async fn clear_persistence(&self) -> Result<()> {
        if let Some(db) = &self.db {
            db.clear()
                .map_err(|e| anyhow!("Failed to clear persistence DB: {}", e))?;
            println!("🧹 Cleared all persisted tasks");
        }
        Ok(())
    }
}