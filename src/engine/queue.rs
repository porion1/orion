use std::collections::{BinaryHeap, HashMap};
use std::cmp::Ordering;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};
use serde_json::Value;
use uuid::Uuid;
use std::time::{SystemTime, Duration};
use sled::Db;
use metrics::{counter, gauge};

use crate::task::TaskType;

/// Task priority levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum TaskPriority {
    High = 3,
    Medium = 2,
    Low = 1,
}

/// Task stored in the queue
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

/// Queue configuration
#[derive(Debug, Clone)]
pub struct QueueConfig {
    pub max_queue_size: usize,
    pub persistence_path: Option<String>,
}

/// Internal queue state
#[derive(Debug)]
struct QueueInner {
    high: BinaryHeap<QueueTaskWrapper>,
    medium: BinaryHeap<QueueTaskWrapper>,
    low: BinaryHeap<QueueTaskWrapper>,
    pending_tasks: HashMap<Uuid, QueueTask>,
}

/// Wrapper for BinaryHeap ordering (earlier tasks first)
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
        // earlier scheduled tasks first; break ties with UUID
        other
            .task
            .scheduled_at
            .cmp(&self.task.scheduled_at)
            .then_with(|| self.task.id.cmp(&other.task.id))
    }
}

/// Shared task queue
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
            Arc::new(sled::open(path).expect("Failed to open queue database"))
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
    pub async fn enqueue(&self, task: QueueTask) -> Result<(), String> {
        let mut inner = self.inner.write().await;

        if inner.pending_tasks.len() >= self.config.max_queue_size {
            return Err("Queue is full".into());
        }

        if inner.pending_tasks.contains_key(&task.id) {
            return Err("Task already exists".into());
        }

        let wrapper = QueueTaskWrapper { task: task.clone() };
        match task.priority {
            TaskPriority::High => inner.high.push(wrapper),
            TaskPriority::Medium => inner.medium.push(wrapper),
            TaskPriority::Low => inner.low.push(wrapper),
        }

        inner.pending_tasks.insert(task.id, task.clone());

        // Metrics
        counter!("orion.scheduler.tasks_scheduled_total", 1);
        gauge!("orion.scheduler.tasks_pending", inner.pending_tasks.len() as f64);

        // Persist task
        if let Some(db) = &self.db {
            let serialized = serde_json::to_vec(&task).map_err(|e| e.to_string())?;
            db.insert(task.id.as_bytes(), serialized)
                .map_err(|e| e.to_string())?;
        }

        Ok(())
    }

    /// Dequeue next **ready** task (priority first)
    pub async fn dequeue(&self) -> Option<QueueTask> {
        let mut inner = self.inner.write().await;
        let now = SystemTime::now();

        // Helper: pop first ready task from a heap
        fn pop_ready(heap: &mut BinaryHeap<QueueTaskWrapper>, now: SystemTime) -> Option<QueueTask> {
            while let Some(wrapper) = heap.peek() {
                if wrapper.task.scheduled_at <= now {
                    return heap.pop().map(|w| w.task);
                } else {
                    break; // not ready yet
                }
            }
            None
        }

        // Try high → medium → low
        let task_opt = pop_ready(&mut inner.high, now)
            .or_else(|| pop_ready(&mut inner.medium, now))
            .or_else(|| pop_ready(&mut inner.low, now))?;

        // Remove from pending
        inner.pending_tasks.remove(&task_opt.id);
        gauge!("orion.scheduler.tasks_pending", inner.pending_tasks.len() as f64);

        // Remove from DB
        if let Some(db) = &self.db {
            let _ = db.remove(task_opt.id.as_bytes());
        }

        Some(task_opt)
    }

    /// Retry a task with optional delay
    pub async fn retry_task(&self, mut task: QueueTask, delay: Option<Duration>) -> Result<(), String> {
        if task.retry_count >= task.max_retries {
            counter!("orion.scheduler.tasks_failed_total", 1);
            return Err("Max retries reached".into());
        }
        task.retry_count += 1;

        if let Some(d) = delay {
            let queue_clone = self.clone();
            tokio::spawn(async move {
                tokio::time::sleep(d).await;
                let _ = queue_clone.enqueue(task).await;
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

    /// Persist all tasks to DB
    pub async fn persist_all(&self) {
        if let Some(db) = &self.db {
            let inner = self.inner.read().await;
            for task in inner.pending_tasks.values() {
                if let Ok(serialized) = serde_json::to_vec(task) {
                    let _ = db.insert(task.id.as_bytes(), serialized);
                }
            }
        }
    }

    /// Load persisted tasks from DB
    pub async fn load_persisted(&self) {
        if let Some(db) = &self.db {
            for result in db.iter() {
                if let Ok((_key, value)) = result {
                    if let Ok(task) = serde_json::from_slice::<QueueTask>(&value) {
                        if self.len().await < self.config.max_queue_size {
                            let _ = self.enqueue(task).await;
                        }
                    }
                }
            }
        }
    }
}
