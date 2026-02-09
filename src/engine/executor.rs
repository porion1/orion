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
    pub retry_count: u32,
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
    completed_count: Arc<Mutex<u64>>,
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
            completed_count: Arc::new(Mutex::new(0)),
        }
    }

    /// NEW: Execute task on specific node
    pub async fn execute_task_on_node(&self, task: QueueTask, node_id: Uuid) {
        println!("🎯 Executing task {} on specific node {}", task.id, node_id);

        // TODO: Implement actual node-specific execution
        // For now, just call the generic execute_task
        // In the future, this should route the task to the specific node

        // You could add node_id to task metadata
        let mut task_with_node = task.clone();
        if let Some(payload) = &mut task_with_node.payload {
            // Check if payload is an Object (JSON object/map)
            if let serde_json::Value::Object(ref mut map) = payload {
                // It's already an object, insert into it
                map.insert("target_node".to_string(), serde_json::Value::String(node_id.to_string()));
            } else {
                // It's not an object (could be array, string, number, etc.)
                // Create a new object with the existing value and target_node
                let mut map = serde_json::Map::new();
                // Optionally keep the existing value under a different key
                map.insert("original_payload".to_string(), payload.clone());
                map.insert("target_node".to_string(), serde_json::Value::String(node_id.to_string()));
                *payload = serde_json::Value::Object(map);
            }
        } else {
            // No payload exists, create a new object
            let mut map = serde_json::Map::new();
            map.insert("target_node".to_string(), serde_json::Value::String(node_id.to_string()));
            task_with_node.payload = Some(serde_json::Value::Object(map));
        }

        self.execute_task(task_with_node).await;
    }

    /// Worker pool loop
    pub async fn start(self: Arc<Self>) {
        println!("👷 Executor started with {} max concurrent tasks", self.semaphore.available_permits());

        loop {
            let permit = match self.semaphore.clone().acquire_owned().await {
                Ok(p) => p,
                Err(_) => {
                    println!("🛑 Executor semaphore closed, shutting down");
                    break;
                },
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

        println!("✅ Executor stopped");
    }

    /// Core execution logic
    pub async fn execute_task(&self, task: QueueTask) {
        let start = Instant::now();
        let task_id = task.id;

        // Check cancellation BEFORE doing any work
        {
            let cancelled = self.cancelled_tasks.lock().await;
            if cancelled.contains(&task_id) {
                // Clean up if it was cancelled
                self.cancelled_tasks.lock().await.remove(&task_id);
                println!("⚠️ Task {} was cancelled before execution", task_id);
                self.record_result(task_id, false, Some("Cancelled before execution".into()), 0, task.retry_count).await;
                return;
            }
        }

        let result = timeout(self.timeout, async {
            // FIX: Check for demo tasks first (should always succeed)
            if let Some(payload) = &task.payload {
                // Check if this is a demo task that should always succeed
                if let Some(demo_task) = payload.get("demo_task") {
                    if demo_task.as_bool().unwrap_or(false) {
                        // Demo task - always succeed with short, consistent delay
                        let delay_ms = if let Some(should_succeed) = payload.get("should_succeed") {
                            if should_succeed.as_bool().unwrap_or(false) {
                                // Priority validation tasks - very quick, guaranteed success
                                20 + rand::random::<u64>() % 30
                            } else {
                                // Other demo tasks - slightly longer but still reliable
                                50 + rand::random::<u64>() % 50
                            }
                        } else {
                            // Default demo task delay
                            50 + rand::random::<u64>() % 50
                        };

                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                        return Ok::<(), String>(());
                    }
                }

                // FIX: Check for simulated failure for retry demo
                if let Some(should_fail) = payload.get("should_fail") {
                    if should_fail.as_bool().unwrap_or(false) {
                        // Simulate a failing task for retry demo
                        println!("🔄 Task {} intentionally failing for retry demo (attempt {}/{})",
                                 task_id, task.retry_count + 1, task.max_retries);
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        return Err::<(), String>("Simulated failure for retry demo".into());
                    }
                }
            }

            // Normal execution - simulate work with variable duration
            let work_duration = Duration::from_millis(50 + rand::random::<u64>() % 100);
            tokio::time::sleep(work_duration).await;

            // FIX: Reduced random failure rate for non-demo tasks (5% instead of 10%)
            // Also, skip random failures for tasks that have already failed and are retrying
            if task.retry_count == 0 && rand::random::<f32>() < 0.05 {
                return Err::<(), String>("Random execution failure".into());
            }

            Ok::<(), String>(())
        })
            .await;

        let duration = start.elapsed().as_millis();

        match result {
            Ok(Ok(())) => {
                // FIX: Less verbose logging for non-demo tasks
                if let Some(payload) = &task.payload {
                    if let Some(demo_task) = payload.get("demo_task") {
                        if demo_task.as_bool().unwrap_or(false) {
                            println!("✅ Task {} (demo) completed in {}ms (attempt {})",
                                     task_id, duration, task.retry_count + 1);
                        }
                    } else {
                        // Only log every 10th normal task to reduce noise
                        if rand::random::<u32>() % 10 == 0 {
                            println!("✅ Task {} completed in {}ms", task_id, duration);
                        }
                    }
                } else {
                    if rand::random::<u32>() % 10 == 0 {
                        println!("✅ Task {} completed in {}ms", task_id, duration);
                    }
                }
                self.record_result(task_id, true, None, duration, task.retry_count).await;
            }
            Ok(Err(err)) => {
                println!("❌ Task {} failed with error: {} (attempt {}/{})",
                         task_id, err, task.retry_count + 1, task.max_retries);
                self.retry_or_fail(task.clone(), Some(err), duration).await;
            }
            Err(_) => {
                println!("⏱️ Task {} timed out after {}ms (attempt {}/{})",
                         task_id, self.timeout.as_millis(), task.retry_count + 1, task.max_retries);
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

            // FIX: Better backoff strategy
            // For demo retry tasks, use fixed 1-second delays
            // For other tasks, use exponential backoff: 1s, 2s, 4s, 8s, etc.
            let backoff_secs = if let Some(payload) = &task.payload {
                if let Some(demo_retry) = payload.get("demo_retry") {
                    if demo_retry.as_bool().unwrap_or(false) {
                        // Demo retry tasks - fixed 1-second delay for predictability
                        1
                    } else {
                        // Normal tasks - exponential backoff
                        1 << (task.retry_count as u32).min(5) // Cap at 32 seconds
                    }
                } else {
                    // Normal tasks - exponential backoff
                    1 << (task.retry_count as u32).min(5)
                }
            } else {
                // Normal tasks - exponential backoff
                1 << (task.retry_count as u32).min(5)
            };

            let backoff = Duration::from_secs(backoff_secs);
            new_task.scheduled_at = std::time::SystemTime::now() + backoff;

            let queue_clone = Arc::clone(&self.queue);

            match queue_clone.enqueue(new_task).await {
                Ok(_) => {
                    // FIX: Less verbose logging for retries
                    if let Some(payload) = &task.payload {
                        if let Some(demo_task) = payload.get("demo_task") {
                            if demo_task.as_bool().unwrap_or(false) {
                                println!("🔄 Task {} scheduled for retry in {}s (attempt {}/{})",
                                         task.id, backoff.as_secs(), task.retry_count + 1, task.max_retries);
                            }
                        }
                    }
                }
                Err(e) => {
                    eprintln!("⚠️ Failed to re-enqueue task {}: {}", task.id, e);
                    self.record_result(task.id, false, Some(format!("Failed to re-enqueue: {}", e)), _duration, task.retry_count).await;
                }
            }
        } else {
            println!("💀 Task {} failed after {} retries", task.id, task.max_retries);
            self.record_result(task.id, false, error, _duration, task.retry_count).await;
        }
    }

    /// Record task result
    async fn record_result(&self, id: Uuid, success: bool, error: Option<String>, duration_ms: u128, retry_count: u32) {
        let result = TaskResult {
            task_id: id,
            success,
            error,
            duration_ms,
            retry_count,
        };
        self.results.lock().await.insert(id, result);

        // Increment completed counter
        let mut completed = self.completed_count.lock().await;
        *completed += 1;
    }

    /// Cancel a task
    pub async fn cancel_task(&self, id: Uuid) -> bool {
        // Mark as cancelled first
        self.cancelled_tasks.lock().await.insert(id);

        // Check if task is already executing
        if let Some(handle) = self.active_tasks.lock().await.get(&id) {
            handle.abort();
            println!("❌ Task {} cancelled while executing", id);
            return true;
        }

        // Task wasn't active yet, but is marked for cancellation
        println!("⚠️ Task {} marked for cancellation (not yet executing)", id);
        true
    }

    /// Get task result
    pub async fn get_result(&self, id: Uuid) -> Option<TaskResult> {
        self.results.lock().await.get(&id).cloned()
    }

    /// Get total completed task count
    pub async fn get_completed_count(&self) -> u64 {
        *self.completed_count.lock().await
    }

    /// Get active task count
    pub async fn get_active_count(&self) -> usize {
        self.active_tasks.lock().await.len()
    }

    /// Get all results
    pub async fn get_all_results(&self) -> Vec<TaskResult> {
        self.results.lock().await.values().cloned().collect()
    }

    // NEW: Helper method to check if a task is a demo task
    pub async fn is_demo_task(&self, id: Uuid) -> bool {
        // Note: This would require storing task payloads in results
        // For now, we can check by duration (demo tasks are usually quick)
        if let Some(result) = self.get_result(id).await {
            result.duration_ms < 200 && result.retry_count == 0
        } else {
            false
        }
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
            completed_count: Arc::clone(&self.completed_count),
        }
    }
}