use std::collections::{BinaryHeap, HashMap};
use std::cmp::Ordering;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};
use serde_json::Value;
use uuid::Uuid;
use std::time::SystemTime;
use sled::Db;

/// Task priority levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum TaskPriority {
    High = 3,
    Medium = 2,
    Low = 1,
}

/// Task wrapper for queue operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueTask {
    pub id: Uuid,
    pub name: String,
    pub scheduled_at: SystemTime,
    pub priority: TaskPriority,
    pub payload: Option<Value>, // ✅ FIXED
}

/// Queue configuration
#[derive(Debug, Clone)]
pub struct QueueConfig {
    pub max_queue_size: usize,
    pub persistence_path: Option<String>,
}

/// Inner state of the queue
#[derive(Debug)]
struct QueueInner {
    high: BinaryHeap<QueueTaskWrapper>,
    medium: BinaryHeap<QueueTaskWrapper>,
    low: BinaryHeap<QueueTaskWrapper>,
    pending_tasks: HashMap<Uuid, QueueTask>,
}

/// Wrapper for ordering tasks in BinaryHeap
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
        // Primary: earlier scheduled time first
        // Secondary: stable tie-breaker by UUID
        other
            .task
            .scheduled_at
            .cmp(&self.task.scheduled_at)
            .then_with(|| self.task.id.cmp(&other.task.id))
    }
}

/// Shared task queue with async-safe access
#[derive(Debug, Clone)]
pub struct SharedTaskQueue {
    inner: Arc<RwLock<QueueInner>>,
    config: QueueConfig,
    db: Option<Arc<Db>>,
}

impl SharedTaskQueue {
    /// Create a new shared task queue
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

    /// Enqueue a task
    pub async fn enqueue(&self, task: QueueTask) -> Result<(), String> {
        let mut inner = self.inner.write().await;

        if inner.pending_tasks.len() >= self.config.max_queue_size {
            return Err("Queue is full".to_string());
        }

        if inner.pending_tasks.contains_key(&task.id) {
            return Err("Task already exists in queue".to_string());
        }

        let wrapper = QueueTaskWrapper { task: task.clone() };

        match task.priority {
            TaskPriority::High => inner.high.push(wrapper),
            TaskPriority::Medium => inner.medium.push(wrapper),
            TaskPriority::Low => inner.low.push(wrapper),
        }

        inner.pending_tasks.insert(task.id, task.clone());

        // Persist
        if let Some(db) = &self.db {
            let serialized = serde_json::to_vec(&task).map_err(|e| e.to_string())?;
            db.insert(task.id.as_bytes(), serialized)
                .map_err(|e| e.to_string())?;
        }

        Ok(())
    }

    /// Dequeue next task (priority first, scheduled_at next)
    pub async fn dequeue(&self) -> Option<QueueTask> {
        let mut inner = self.inner.write().await;

        let wrapper_opt = if let Some(w) = inner.high.pop() {
            Some(w)
        } else if let Some(w) = inner.medium.pop() {
            Some(w)
        } else if let Some(w) = inner.low.pop() {
            Some(w)
        } else {
            None
        };

        let task = wrapper_opt.map(|w| w.task)?;

        inner.pending_tasks.remove(&task.id);

        // Remove from persistence
        if let Some(db) = &self.db {
            let _ = db.remove(task.id.as_bytes());
        }

        Some(task)
    }

    /// Get all pending tasks (safe public access)
    pub async fn get_all_pending(&self) -> Vec<QueueTask> {
        let inner = self.inner.read().await;
        inner.pending_tasks.values().cloned().collect()
    }

    /// Return number of pending tasks
    pub async fn len(&self) -> usize {
        let inner = self.inner.read().await;
        inner.pending_tasks.len()
    }

    /// Persist all tasks
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

    /// Load persisted tasks
    pub async fn load_persisted(&self) {
        if let Some(db) = &self.db {
            for result in db.iter() {
                if let Ok((_key, value)) = result {
                    if let Ok(task) = serde_json::from_slice::<QueueTask>(&value) {
                        // Respect max queue size
                        if self.len().await < self.config.max_queue_size {
                            let _ = self.enqueue(task).await;
                        }
                    }
                }
            }
        }
    }
}
