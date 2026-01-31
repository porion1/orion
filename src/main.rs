use orion::engine::{Engine, EngineConfig, Task, QueueTask, TaskPriority};
use tracing_subscriber;
use std::time::Duration;
use uuid::Uuid;
use std::sync::Arc;
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();
    println!("🚀 Starting Orion Engine (Production Ready Demo)");

    // Load configuration
    let config = EngineConfig::from_yaml("config.yaml").unwrap_or_else(|err| {
        eprintln!("⚠️ Failed to load config.yaml: {}. Using defaults.", err);
        EngineConfig::default()
    });

    println!("⚙️ Engine configuration: {:?}", config);

    // Create engine
    let engine = Arc::new(Engine::new(config));

    // Helper: Task → QueueTask with priority
    async fn schedule_task(
        engine: Arc<Engine>,
        task: Task,
        priority: TaskPriority,
    ) -> Result<Uuid> {
        let qt = QueueTask {
            id: task.id,
            name: task.name.clone(),
            scheduled_at: task.scheduled_at,
            priority,
            payload: task.payload.clone(),
            retry_count: task.retry_count,
            max_retries: task.max_retries,
            task_type: task.task_type.clone(),
        };

        let id = engine.schedule_task(qt).await?;
        println!("✅ Task scheduled: '{}' [{}]", task.name, task.id);
        Ok(id)
    }

    println!("📋 Scheduling demo tasks...");

    // 1️⃣ Immediate task
    schedule_task(
        engine.clone(),
        Task::new_one_shot(
            "Immediate Task",
            Duration::from_secs(0),
            Some(serde_json::json!({"action": "immediate"})),
        ),
        TaskPriority::High,
    ).await?;

    // 2️⃣ Delayed task (to be cancelled)
    let delayed_id = schedule_task(
        engine.clone(),
        Task::new_one_shot(
            "Process Data",
            Duration::from_secs(5),
            Some(serde_json::json!({"action": "process"})),
        ),
        TaskPriority::Medium,
    ).await?;

    // 3️⃣ Recurring task
    match Task::new_recurring(
        "Health Check",
        "*/10 * * * * *",  // Every 10 seconds
        Some(serde_json::json!({"action": "health_check"})),
    ) {
        Ok(task) => {
            schedule_task(engine.clone(), task, TaskPriority::High).await?;
            println!("🔄 Recurring task scheduled (every 10 seconds)");
        }
        Err(e) => eprintln!("⚠️ Failed to create recurring task: {}", e),
    }

    // 4️⃣ Timeout demo
    let timeout_id = schedule_task(
        engine.clone(),
        Task::new_one_shot(
            "Timeout Task",
            Duration::from_secs(1),
            Some(serde_json::json!({"action": "timeout_demo"})),
        ),
        TaskPriority::High,
    ).await?;

    // Observe timeout result
    let engine_clone = engine.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(4)).await;
        if let Some(result) = engine_clone.executor.get_result(timeout_id).await {
            println!("⏱️ Timeout task result: {:?}", result);
        } else {
            println!("⏱️ Timeout task result not available yet");
        }
    });

    // 5️⃣ Cancel task demo - cancel before it executes (after 3 seconds, scheduled at 5)
    let engine_cancel = engine.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(3)).await;
        if engine_cancel.cancel_task(delayed_id).await {
            println!("❌ Delayed task cancelled (was scheduled for 5 seconds)");
        } else {
            println!("⚠️ Could not cancel delayed task - may have already executed");
        }
    });

    // 6️⃣ Retry demo - a task that will fail
    let retry_task = Task::new_one_shot(
        "Retry Demo Task",
        Duration::from_secs(2),
        Some(serde_json::json!({
            "action": "retry_demo",
            "should_fail": true
        })),
    );

    schedule_task(
        engine.clone(),
        retry_task,
        TaskPriority::High,
    ).await?;

    // 7️⃣ Monitor pending tasks
    let engine_monitor = engine.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        loop {
            interval.tick().await;
            let pending = engine_monitor.get_pending_tasks().await;
            println!("📊 Pending tasks: {}", pending.len());

            if pending.is_empty() {
                println!("📊 All tasks processed, shutting down in 10 seconds...");
                tokio::time::sleep(Duration::from_secs(10)).await;
                engine_monitor.shutdown();
                break;
            }
        }
    });

    println!("\n🎬 Starting engine (Ctrl+C to stop)...");
    // FIXED: Correct syntax for repeat
    println!("{}", "=".repeat(50));

    // Start the engine
    let engine_for_shutdown = engine.clone();
    let engine_handle = tokio::spawn(async move {
        if let Err(e) = engine_for_shutdown.start().await {
            eprintln!("Engine error: {}", e);
        }
    });

    // Wait for engine to finish or for Ctrl+C
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            println!("\n🛑 Ctrl+C received, shutting down...");
            engine.shutdown();
        }
        _ = engine_handle => {
            println!("🛑 Engine finished execution");
        }
    }

    // Wait for graceful shutdown
    tokio::time::sleep(Duration::from_secs(2)).await;

    println!("\n=== Final Statistics ===");
    let pending = engine.get_pending_tasks().await;
    println!("📋 Pending tasks: {}", pending.len());

    // Note: You might want to add a method to get completed task count
    println!("✅ Demo completed successfully");

    Ok(())
}