use crate::engine::executor::TaskExecutor;
use crate::engine::queue::{QueueTask, SharedTaskQueue};
use crate::engine::task::TaskType;
use crate::scoring::Scorer;
use crate::node::registry::NodeRegistry;
use metrics::{counter, gauge};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::watch;
use tokio::time::interval;
use uuid::Uuid;

/// Scheduler drives task dispatching and lifecycle orchestration
pub struct Scheduler {
    scorer: Box<dyn Scorer>,
    queue: Arc<SharedTaskQueue>,
    executor: Arc<TaskExecutor>,
    node_registry: Arc<NodeRegistry>,
    shutdown_tx: watch::Sender<bool>,
    shutdown_received: Arc<tokio::sync::RwLock<bool>>,
}

impl Scheduler {
    /// Create a new scheduler with scoring capability
    pub fn new(
        scorer: Box<dyn Scorer>,
        queue: Arc<SharedTaskQueue>,
        executor: Arc<TaskExecutor>,
        node_registry: Arc<NodeRegistry>,
    ) -> Self {
        let (shutdown_tx, _) = watch::channel(false);

        Self {
            scorer,
            queue,
            executor,
            node_registry,
            shutdown_tx,
            shutdown_received: Arc::new(tokio::sync::RwLock::new(false)),
        }
    }

    /// Builder method for executor
    pub fn with_executor(mut self, executor: Arc<TaskExecutor>) -> Self {
        self.executor = executor;
        self
    }

    /// Builder method for scorer
    pub fn with_scorer(mut self, scorer: Box<dyn Scorer>) -> Self {
        self.scorer = scorer;
        self
    }

    /// Builder method for node registry
    pub fn with_node_registry(mut self, node_registry: Arc<NodeRegistry>) -> Self {
        self.node_registry = node_registry;
        self
    }

    /// Convenience constructor with defaults
    pub fn default_with_scorer(scorer: Box<dyn Scorer>) -> Self {
        let (shutdown_tx, _) = watch::channel(false);

        // Create queue with default config
        let queue_config = crate::engine::queue::QueueConfig::new(
            1000,  // max_queue_size
            None,  // persistence_path
        );
        let queue = Arc::new(SharedTaskQueue::new(queue_config));

        // Create executor with default parameters
        let max_concurrency = 10;
        let timeout = Duration::from_secs(30);
        let executor = Arc::new(TaskExecutor::new(
            Arc::clone(&queue),
            max_concurrency,
            timeout,
        ));

        // Create node registry
        let node_registry = Arc::new(
            NodeRegistry::new(None).expect("Failed to create node registry")
        );

        Self {
            scorer,
            queue,
            executor,
            node_registry,
            shutdown_tx,
            shutdown_received: Arc::new(tokio::sync::RwLock::new(false)),
        }
    }

    /// Start the scheduler main loop
    pub async fn start(self: Arc<Self>) -> anyhow::Result<()> {
        println!("🔄 Scheduler started");

        let mut ticker = interval(Duration::from_secs(1));
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if *self.shutdown_received.read().await {
                        continue;
                    }

                    self.process_ready_tasks().await;
                    self.update_metrics().await;
                }

                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        println!("🛑 Scheduler shutting down");
                        *self.shutdown_received.write().await = true;

                        if let Err(e) = self.queue.persist_all().await {
                            eprintln!("⚠️ Failed to persist queue state during shutdown: {}", e);
                        }

                        break;
                    }
                }
            }
        }

        println!("✅ Scheduler stopped");
        Ok(())
    }

    /// Drain queue and dispatch tasks
    async fn process_ready_tasks(&self) {
        while let Some(task) = self.queue.dequeue().await {
            self.dispatch(task).await;
        }
    }

    /// Dispatch a single task
    async fn dispatch(&self, task: QueueTask) {
        counter!("orion.scheduler.tasks_scheduled_total", 1);

        // Respect scheduled execution time
        if let Ok(delay) = task.scheduled_at.duration_since(SystemTime::now()) {
            if delay > Duration::ZERO {
                tokio::time::sleep(delay).await;
            }
        }

        let task_id = task.id;

        // Use scoring to select node
        if let Some(node_id) = self.select_node_for_task(&task).await {
            // Execute on selected node
            self.executor.execute_task_on_node(task.clone(), node_id).await;
        } else {
            // Fallback: let executor handle routing
            self.executor.execute_task(task.clone()).await;
        }

        // Allow executor to publish result
        tokio::time::sleep(Duration::from_millis(100)).await;

        self.handle_task_result(task_id, &task).await;

        if !*self.shutdown_received.read().await {
            self.handle_recurring_tasks(&task).await;
        }
    }

    /// Select node for task using scoring system
    async fn select_node_for_task(&self, task: &QueueTask) -> Option<Uuid> {
        // Get active nodes from registry
        let nodes = self.node_registry.get_active_nodes();

        if nodes.is_empty() {
            return None;
        }

        // Use the existing from_queue_task method
        use crate::engine::task::Task;
        let task_for_scoring = Task::from_queue_task(task);

        // Get nodes and convert them to Node type for scoring
        use crate::engine::node::Node;
        let scoring_nodes: Vec<Node> = nodes.into_iter()
            .map(|node_info| Node {
                id: node_info.id,
                address: node_info.address,
            })
            .collect();

        // Score nodes
        let scores = self.scorer.score_nodes(&task_for_scoring, &scoring_nodes);

        scores.first().map(|score| score.node_id)
    }

    /// Retry logic
    async fn handle_task_result(&self, task_id: Uuid, task: &QueueTask) {
        if let Some(result) = self.executor.get_result(task_id).await {
            if !result.success && task.retry_count < task.max_retries {
                if *self.shutdown_received.read().await {
                    return;
                }

                let mut retry = task.clone();
                retry.retry_count += 1;
                retry.scheduled_at = SystemTime::now() + Duration::from_secs(5);

                if self.queue.enqueue(retry).await.is_ok() {
                    counter!("orion.scheduler.tasks_retried_total", 1);
                } else {
                    counter!("orion.scheduler.tasks_retry_failed_total", 1);
                }
            }
        }
    }

    /// Recurring task handling
    async fn handle_recurring_tasks(&self, task: &QueueTask) {
        if let TaskType::Recurring { cron_expr } = &task.task_type {
            if *self.shutdown_received.read().await {
                return;
            }

            // Create next occurrence
            if let Some(next) = self.create_next_occurrence(task) {
                if self.queue.enqueue(next).await.is_ok() {
                    counter!("orion.scheduler.tasks_rescheduled_total", 1);
                } else {
                    counter!("orion.scheduler.tasks_reschedule_failed_total", 1);
                }
            }
        }
    }

    /// Helper to create next occurrence
    fn create_next_occurrence(&self, task: &QueueTask) -> Option<QueueTask> {
        match &task.task_type {
            TaskType::Recurring { cron_expr } => {
                // For now, just clone with 60 second interval
                // TODO: Parse cron expression and calculate next occurrence
                let mut next = task.clone();
                next.scheduled_at = SystemTime::now() + Duration::from_secs(60);
                Some(next)
            }
            _ => None,
        }
    }

    /// Emit scheduler metrics
    async fn update_metrics(&self) {
        let pending = self.queue.len().await;
        gauge!("orion.scheduler.tasks_pending", pending as f64);

        gauge!(
            "orion.scheduler.shutdown_state",
            if *self.shutdown_received.read().await { 1.0 } else { 0.0 }
        );
    }

    /// Public API
    pub async fn schedule(&self, task: QueueTask) -> anyhow::Result<Uuid> {
        if *self.shutdown_received.read().await {
            return Err(anyhow::anyhow!("Scheduler is shutting down"));
        }

        // Optionally pre-score and tag task with suggested node
        if let Some(node_id) = self.select_node_for_task(&task).await {
            // You could store the suggested node with the task
            // task.metadata.insert("suggested_node".to_string(), node_id.to_string());
        }

        let id = task.id;
        self.queue.enqueue(task).await?;
        Ok(id)
    }

    pub fn shutdown_handle(&self) -> watch::Sender<bool> {
        self.shutdown_tx.clone()
    }

    pub async fn is_shutting_down(&self) -> bool {
        *self.shutdown_received.read().await
    }

    pub async fn get_shutdown_state(&self) -> String {
        if *self.shutdown_received.read().await {
            "shutting_down".to_string()
        } else {
            "running".to_string()
        }
    }
}

// Manual Debug implementation to handle dyn Scorer
impl std::fmt::Debug for Scheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Scheduler")
            .field("queue", &self.queue)
            .field("executor", &self.executor)
            .field("node_registry", &self.node_registry)
            .finish_non_exhaustive()
    }
}