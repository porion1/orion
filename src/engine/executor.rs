use crate::engine::queue::{QueueTask, SharedTaskQueue};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use uuid::Uuid;

/// Task execution result
#[derive(Debug, Clone)]
pub struct TaskResult {
    pub task_id: Uuid,
    pub success: bool,
    pub error: Option<String>,
    pub duration_ms: u128,
}

/// Async Task Executor
#[derive(Debug)]
pub struct TaskExecutor {
    queue: Arc<SharedTaskQueue>,
    semaphore: Arc<Semaphore>,
    timeout: Duration,

    active_tasks: Arc<Mutex<HashMap<Uuid, JoinHandle<()>>>>,
    cancelled_tasks: Arc<Mutex<HashSet<Uuid>>>,
    results: Arc<Mutex<HashMap<Uuid, TaskResult>>>,
}

impl TaskExecutor {
    pub fn new(queue: Arc<SharedTaskQueue>, max_concurrency: usize, timeout: Duration) -> Self {
        Self {
            queue,
            semaphore: Arc::new(Semaphore::new(max_concurrency)),
            timeout,
            active_tasks: Arc::new(Mutex::new(HashMap::new())),
            cancelled_tasks: Arc::new(Mutex::new(HashSet::new())),
            results: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Worker pool loop
    pub async fn start(self: Arc<Self>) {
        loop {
            let permit = match self.semaphore.clone().acquire_owned().await {
                Ok(p) => p,
                Err(_) => break,
            };

            let Some(task) = self.queue.dequeue().await else {
                drop(permit);
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            };

            let executor_clone = Arc::clone(&self);
            let task_clone = task.clone();
            let task_id = task.id;

            let handle: JoinHandle<()> = tokio::spawn(async move {
                let _permit = permit;
                executor_clone.execute_task(task_clone).await;
            });

            self.active_tasks.lock().await.insert(task_id, handle);
        }
    }

    /// Core execution logic
    pub async fn execute_task(&self, task: QueueTask) {
        let start = Instant::now();
        let task_id = task.id;

        if self.cancelled_tasks.lock().await.contains(&task_id) {
            return;
        }

        let result = timeout(self.timeout, async {
            // Replace with real task execution
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok::<(), String>(())
        })
            .await;

        let duration = start.elapsed().as_millis();

        match result {
            Ok(Ok(())) => {
                self.record_result(task_id, true, None, duration).await;
            }
            Ok(Err(err)) => {
                self.retry_or_fail(task.clone(), Some(err), duration).await;
            }
            Err(_) => {
                self.retry_or_fail(task.clone(), Some("Timeout".into()), duration).await;
            }
        }

        self.active_tasks.lock().await.remove(&task_id);
    }

    /// Retry or fail a task
    async fn retry_or_fail(&self, task: QueueTask, error: Option<String>, _duration: u128) {
        if task.retry_count < task.max_retries {
            let mut new_task = task.clone();
            new_task.retry_count += 1;
            new_task.scheduled_at = std::time::SystemTime::now() + Duration::from_secs(1);

            // Use a clone of the queue and handle errors properly
            let queue_clone = Arc::clone(&self.queue);

            // Don't use ? here - handle the error explicitly
            match queue_clone.enqueue(new_task).await {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("⚠️ Failed to re-enqueue task: {}", e);
                    // Record as failed if we can't even re-enqueue
                    self.record_result(task.id, false, Some(format!("Failed to re-enqueue: {}", e)), _duration).await;
                }
            }
        } else {
            self.record_result(task.id, false, error, _duration).await;
        }
    }

    /// Record task result
    async fn record_result(&self, id: Uuid, success: bool, error: Option<String>, duration_ms: u128) {
        let result = TaskResult {
            task_id: id,
            success,
            error,
            duration_ms,
        };
        self.results.lock().await.insert(id, result);
    }

    /// Cancel a task
    pub async fn cancel_task(&self, id: Uuid) -> bool {
        self.cancelled_tasks.lock().await.insert(id);
        if let Some(handle) = self.active_tasks.lock().await.remove(&id) {
            handle.abort();
            return true;
        }
        false
    }

    /// Get task result
    pub async fn get_result(&self, id: Uuid) -> Option<TaskResult> {
        self.results.lock().await.get(&id).cloned()
    }
}

impl Clone for TaskExecutor {
    fn clone(&self) -> Self {
        Self {
            queue: Arc::clone(&self.queue),
            semaphore: Arc::clone(&self.semaphore),
            timeout: self.timeout,
            active_tasks: Arc::clone(&self.active_tasks),
            cancelled_tasks: Arc::clone(&self.cancelled_tasks),
            results: Arc::clone(&self.results),
        }
    }
}