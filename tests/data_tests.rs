// tests/data_tests.rs
use orion::task::{Task, TaskType};
use orion::{SharedTaskQueue, QueueTask};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn test_queue_enqueue_dequeue_and_retry() {
    // Create an in-memory queue
    let queue_config = orion::queue::QueueConfig {
        max_queue_size: 10,
        persistence_path: None,
    };
    let queue = Arc::new(SharedTaskQueue::new(queue_config));

    // 1️⃣ One-shot task
    let task1 = Task::new_one_shot(
        "Task One",
        Duration::from_millis(10),
        Some(serde_json::json!({"action": "one"})),
    );
    let qt1: QueueTask = (&task1).into();
    queue.enqueue(qt1.clone()).await.unwrap();

    // 2️⃣ Another one-shot task
    let task2 = Task::new_one_shot(
        "Task Two",
        Duration::from_millis(20),
        Some(serde_json::json!({"action": "two"})),
    );
    let qt2: QueueTask = (&task2).into();
    queue.enqueue(qt2.clone()).await.unwrap();

    // Check queue length
    assert_eq!(queue.len().await, 2, "Queue should have 2 tasks");

    // 3️⃣ Dequeue first ready task
    sleep(Duration::from_millis(15)).await; // wait for task1 to be ready
    let dequeued = queue.dequeue().await.unwrap();
    assert_eq!(dequeued.name, "Task One");

    // Queue length should now be 1
    assert_eq!(queue.len().await, 1);

    // 4️⃣ Retry a task
    let mut retry_task = dequeued.clone();
    retry_task.max_retries = 2;
    retry_task.retry_count = 0;

    queue.retry_task(retry_task.clone(), Some(Duration::from_millis(10)))
        .await
        .unwrap();

    // Wait for retry to happen
    sleep(Duration::from_millis(20)).await;

    // There should now be 2 tasks in queue: task2 + retried task1
    let pending_tasks = queue.get_all_pending().await;
    assert_eq!(pending_tasks.len(), 2);
    assert!(pending_tasks.iter().any(|t| t.id == task2.id));
    assert!(pending_tasks.iter().any(|t| t.id == task1.id));

    // 5️⃣ Dequeue remaining tasks
    while let Some(_) = queue.dequeue().await {}

    assert_eq!(queue.len().await, 0, "Queue should be empty after dequeuing all tasks");
}

#[tokio::test]
async fn test_recurring_task_enqueue() {
    let queue_config = orion::queue::QueueConfig {
        max_queue_size: 10,
        persistence_path: None,
    };
    let queue = Arc::new(SharedTaskQueue::new(queue_config));

    // Recurring task
    let task = Task::new_recurring(
        "Recurring Test",
        "0/1 * * * * *", // every 1 second
        Some(serde_json::json!({"action": "recurring"})),
    )
        .unwrap();
    let qt: QueueTask = (&task).into();

    queue.enqueue(qt.clone()).await.unwrap();
    assert_eq!(queue.len().await, 1, "Queue should have 1 recurring task");

    // Simulate next occurrence
    if let Some(next_task) = task.create_next_occurrence() {
        let qt_next: QueueTask = (&next_task).into();
        queue.enqueue(qt_next).await.unwrap();
    }

    assert_eq!(queue.len().await, 2, "Queue should have 2 tasks after reschedule");
}
