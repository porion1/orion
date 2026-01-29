use orion::engine::{Engine, EngineConfig, Task, QueueTask, TaskPriority};
use tracing_subscriber;
use std::time::Duration;

#[tokio::main]
async fn main() {
    // Initialize logging/tracing
    tracing_subscriber::fmt::init();
    println!("🚀 Starting Orion with Enhanced Scheduler...");

    // Load config from YAML, fallback to defaults
    let config = EngineConfig::from_yaml("config.yaml").unwrap_or_else(|err| {
        eprintln!("⚠️ Failed to load config.yaml: {}. Using defaults.", err);
        EngineConfig::defaults()
    });
    println!("⚙️ Engine configuration: {:?}", config);

    // Initialize engine
    let mut engine = Engine::new(config);
    let scheduler = engine.scheduler.clone();

    // Helper: Task → QueueTask
    fn to_queue_task(task: &Task, priority: TaskPriority) -> QueueTask {
        QueueTask {
            id: task.id,
            name: task.name.clone(),
            scheduled_at: task.scheduled_at,
            priority,
            payload: task.payload.clone(),
            retry_count: task.retry_count,
            max_retries: task.max_retries,
            task_type: task.task_type.clone(),
        }
    }

    // 1️⃣ Immediate one-shot task
    let task_immediate = Task::new_one_shot(
        "Immediate Task",
        Duration::from_secs(0),
        Some(serde_json::json!({"action": "immediate"})),
    );
    scheduler.schedule(to_queue_task(&task_immediate, TaskPriority::Medium)).await.unwrap();
    println!("✅ Immediate task scheduled.");

    // 2️⃣ One-shot task (after 5 seconds)
    let task_delayed = Task::new_one_shot(
        "Process Data",
        Duration::from_secs(5),
        Some(serde_json::json!({"action": "process", "data": "sample"})),
    );
    scheduler.schedule(to_queue_task(&task_delayed, TaskPriority::Medium)).await.unwrap();
    println!("✅ One-shot task scheduled (5s delay).");

    // 3️⃣ Recurring task (every 10 seconds)
    if let Ok(task_recurring) = Task::new_recurring(
        "Health Check",
        "0/10 * * * * *", // every 10 seconds
        Some(serde_json::json!({"action": "health_check"})),
    ) {
        scheduler.schedule(to_queue_task(&task_recurring, TaskPriority::High)).await.unwrap();
        println!("✅ Recurring task scheduled (every 10s).");
    }

    // Start engine (blocks until shutdown)
    engine.start().await.unwrap();
    println!("✅ Orion has stopped gracefully.");
}
