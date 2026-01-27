use orion::engine::{Engine, EngineConfig, Task, QueueTask, TaskPriority};
use tracing_subscriber;
use std::process;
use std::time::Duration;

#[tokio::main]
async fn main() {
    // Initialize tracing/logging
    tracing_subscriber::fmt::init();
    println!("🚀 Starting Orion with Enhanced Scheduler...");

    // Load configuration from YAML or fallback to defaults
    let config = match EngineConfig::from_yaml("config.yaml") {
        Ok(cfg) => cfg,
        Err(err) => {
            eprintln!("⚠️ Failed to load config.yaml: {}. Using defaults.", err);
            EngineConfig::defaults()
        }
    };

    println!("⚙️ Engine configuration: {:?}", config);

    // Initialize the Engine
    let mut engine = Engine::new(config);

    // Helper to convert Task -> QueueTask
    fn to_queue_task(task: Task, priority: TaskPriority) -> QueueTask {
        QueueTask {
            id: task.id,
            name: task.name,
            scheduled_at: task.scheduled_at,
            priority,
            payload: task.payload.clone(),
        }
    }

    // Schedule example tasks
    let scheduler = engine.scheduler.clone();

    // 1️⃣ One-shot task (runs in 5 seconds)
    let task1 = Task::new_one_shot(
        "Process Data",
        Duration::from_secs(5),
        Some(serde_json::json!({"action": "process", "data": "sample"})),
    );
    let queue_task1 = to_queue_task(task1, TaskPriority::Medium);
    match scheduler.schedule(queue_task1).await {
        Ok(id) => println!("✅ One-shot task scheduled with ID: {}", id),
        Err(e) => eprintln!("❌ Failed to schedule task: {}", e),
    }

    // 2️⃣ Recurring task (every 10 seconds)
    match Task::new_recurring(
        "Health Check",
        "0/10 * * * * *", // every 10 seconds
        Some(serde_json::json!({"action": "health_check"})),
    ) {
        Ok(task2) => {
            let queue_task2 = to_queue_task(task2, TaskPriority::High);
            match scheduler.schedule(queue_task2).await {
                Ok(id) => println!("✅ Recurring task scheduled with ID: {}", id),
                Err(e) => eprintln!("❌ Failed to schedule recurring task: {}", e),
            }
        }
        Err(e) => eprintln!("❌ Invalid cron expression for recurring task: {}", e),
    }

    // 3️⃣ Immediate one-shot task
    let task3 = Task::new_one_shot(
        "Immediate Task",
        Duration::from_secs(0),
        Some(serde_json::json!({"action": "immediate"})),
    );
    let queue_task3 = to_queue_task(task3, TaskPriority::Medium);
    match scheduler.schedule(queue_task3).await {
        Ok(id) => println!("✅ Immediate task scheduled with ID: {}", id),
        Err(e) => eprintln!("❌ Failed to schedule immediate task: {}", e),
    }

    // Start the Engine
    if let Err(err) = engine.start().await {
        eprintln!("❌ Engine runtime error: {}", err);
        process::exit(1);
    }

    println!("✅ Orion has stopped gracefully.");
}
