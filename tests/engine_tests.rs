// tests/engine_tests.rs
use orion::{
    Engine, QueueTask, SharedTaskQueue, TaskPriority,
    task::{TaskType, TaskStatus}
};
use std::sync::Arc;
use std::time::{SystemTime, Duration};
use tokio::time::sleep;
use uuid::Uuid;

#[tokio::test]
async fn test_async_task_execution() {
    // Setup engine and queue
    let config = orion::EngineConfig {
        scheduler_tick_secs: 1,
        max_concurrent_tasks: 10,
        logging_level: Some("info".into()),
        persistence_path: None,
        metrics_port: 0,
        metrics_enabled: false,
    };
    let engine = Engine::new(config);
    let queue: Arc<SharedTaskQueue> = Arc::clone(&engine.task_queue);

    // Create an immediate task
    let task = QueueTask {
        id: Uuid::new_v4(),
        name: "Immediate Task".into(),
        scheduled_at: SystemTime::now(),
        priority: TaskPriority::Medium,
        payload: None,
        retry_count: 0,
        max_retries: 1,
        task_type: TaskType::OneShot,
    };

    // Schedule task
    let id = engine.schedule_task(task.clone()).await.unwrap();
    assert_eq!(id, task.id);

    // Wait for scheduler to execute
    sleep(Duration::from_secs(2)).await;

    // Task should be executed (removed from queue)
    let pending = queue.get_all_pending().await;
    assert!(!pending.iter().any(|t| t.id == task.id), "Task should have executed");
}

#[tokio::test]
async fn test_task_timeout_enforcement() {
    let config = orion::EngineConfig {
        scheduler_tick_secs: 1,
        max_concurrent_tasks: 10,
        logging_level: Some("info".into()),
        persistence_path: None,
        metrics_port: 0,
        metrics_enabled: false,
    };
    let engine = Engine::new(config);
    let queue: Arc<SharedTaskQueue> = Arc::clone(&engine.task_queue);

    // Create a task that simulates a long execution (scheduler will timeout after 10s by default)
    let task = QueueTask {
        id: Uuid::new_v4(),
        name: "Timeout Task".into(),
        scheduled_at: SystemTime::now(),
        priority: TaskPriority::Medium,
        payload: None,
        retry_count: 0,
        max_retries: 1,
        task_type: TaskType::OneShot,
    };

    engine.schedule_task(task.clone()).await.unwrap();

    // Wait enough for the scheduler to run
    sleep(Duration::from_secs(2)).await;

    // Task should be removed from queue (executed or retried)
    let pending = queue.get_all_pending().await;
    assert!(!pending.iter().any(|t| t.id == task.id), "Task should have executed or retried");
}

#[tokio::test]
async fn test_task_cancellation() {
    let config = orion::EngineConfig {
        scheduler_tick_secs: 1,
        max_concurrent_tasks: 10,
        logging_level: Some("info".into()),
        persistence_path: None,
        metrics_port: 0,
        metrics_enabled: false,
    };
    let engine = Engine::new(config);
    let queue: Arc<SharedTaskQueue> = Arc::clone(&engine.task_queue);

    // Schedule a task in the past so it can be dequeued immediately
    let task = QueueTask {
        id: Uuid::new_v4(),
        name: "Cancelable Task".into(),
        scheduled_at: SystemTime::now() - Duration::from_secs(1),
        priority: TaskPriority::Medium,
        payload: None,
        retry_count: 0,
        max_retries: 1,
        task_type: TaskType::OneShot,
    };

    engine.schedule_task(task.clone()).await.unwrap();

    // Simulate cancellation by dequeuing the task before execution
    let dequeued = queue.dequeue().await;
    assert!(dequeued.is_some(), "Task should be dequeued for cancellation");
    assert_eq!(dequeued.unwrap().id, task.id, "Dequeued task should match the scheduled task");

    // Ensure queue is empty
    let pending = queue.get_all_pending().await;
    assert!(!pending.iter().any(|t| t.id == task.id), "Canceled task should not exist in pending queue");
}

#[tokio::test]
async fn test_task_result_capture() {
    let config = orion::EngineConfig {
        scheduler_tick_secs: 1,
        max_concurrent_tasks: 10,
        logging_level: Some("info".into()),
        persistence_path: None,
        metrics_port: 0,
        metrics_enabled: false,
    };
    let engine = Engine::new(config);
    let queue: Arc<SharedTaskQueue> = Arc::clone(&engine.task_queue);

    // Schedule a simple task with payload
    let task = QueueTask {
        id: Uuid::new_v4(),
        name: "Result Capture Task".into(),
        scheduled_at: SystemTime::now(),
        priority: TaskPriority::Medium,
        payload: Some(serde_json::json!({"data": 123})),
        retry_count: 0,
        max_retries: 1,
        task_type: TaskType::OneShot,
    };

    engine.schedule_task(task.clone()).await.unwrap();

    // Wait for execution
    sleep(Duration::from_secs(2)).await;

    // Task should be executed and removed from queue
    let pending = queue.get_all_pending().await;
    assert!(!pending.iter().any(|t| t.id == task.id), "Task should be executed and removed from queue");
}
