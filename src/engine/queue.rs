use crate::engine::task::TaskType;
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sled::Db;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
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
    pub persistence_path: Option<String>,
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
        let db = config.persistence_path.as_ref().map(|path| {
            Arc::new(sled::open(path).expect("Failed to open queue DB"))
        });

        let inner = QueueInner {
            high: BinaryHeap::new(),
            medium: BinaryHeap::new(),
            low: BinaryHeap::new(),
            pending_tasks: HashMap::new(),
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

        if inner.pending_tasks.contains_key(&task.id) {
            return Err(anyhow!("Task already exists with ID: {}", task.id));
        }

        let wrapper = QueueTaskWrapper { task: task.clone() };
        match task.priority {
            TaskPriority::High => inner.high.push(wrapper),
            TaskPriority::Medium => inner.medium.push(wrapper),
            TaskPriority::Low => inner.low.push(wrapper),
        }

        inner.pending_tasks.insert(task.id, task.clone());

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

        inner.pending_tasks.remove(&task_opt.id);
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
            for result in db.iter() {
                match result {
                    Ok((_key, value)) => {
                        if let Ok(task) = serde_json::from_slice::<QueueTask>(&value) {
                            if self.len().await < self.config.max_queue_size {
                                if let Err(e) = self.enqueue(task).await {
                                    eprintln!("⚠️ Failed to load persisted task: {}", e);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("⚠️ Error reading from persistence DB: {}", e);
                    }
                }
            }
        }
        Ok(())
    }
}