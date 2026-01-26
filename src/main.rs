use orion::engine::{Engine, EngineConfig, Task};
use tracing_subscriber;
use std::process;
use std::time::Duration;

#[tokio::main]
async fn main() {
    // Initialize tracing
    tracing_subscriber::fmt::init();
    println!("🚀 Starting Orion with Enhanced Scheduler...");

    // Load configuration from YAML or use defaults
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

    // Schedule some example tasks
    let engine_scheduler = engine.scheduler.clone();
    tokio::spawn(async move {
        // Give the scheduler a moment to start
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Schedule a one-shot task that runs after 5 seconds
        println!("📅 Scheduling one-shot task (runs in 5 seconds)...");
        let task1 = Task::new_one_shot(
            "Process Data",
            Duration::from_secs(5),
            Some(serde_json::json!({"action": "process", "data": "sample"})),
        );

        match engine_scheduler.schedule(task1).await {
            Ok(id) => println!("✅ One-shot task scheduled with ID: {}", id),
            Err(e) => eprintln!("❌ Failed to schedule task: {}", e),
        }

        // Schedule a recurring task that runs every 10 seconds
        // Using a simpler cron expression for testing
        println!("📅 Scheduling recurring task (every 10 seconds)...");
        match Task::new_recurring(
            "Health Check",
            "0/10 * * * * *",  // Every 10 seconds, starting at 0 seconds
            Some(serde_json::json!({"action": "health_check"})),
        ) {
            Ok(task2) => {
                match engine_scheduler.schedule(task2).await {
                    Ok(id) => println!("✅ Recurring task scheduled with ID: {}", id),
                    Err(e) => eprintln!("❌ Failed to schedule recurring task: {}", e),
                }
            }
            Err(e) => eprintln!("❌ Invalid cron expression: {}", e),
        }

        // Schedule another one-shot task that runs immediately
        println!("📅 Scheduling immediate one-shot task...");
        let task3 = Task::new_one_shot(
            "Immediate Task",
            Duration::from_secs(0),
            Some(serde_json::json!({"action": "immediate"})),
        );

        match engine_scheduler.schedule(task3).await {
            Ok(id) => println!("✅ Immediate task scheduled with ID: {}", id),
            Err(e) => eprintln!("❌ Failed to schedule immediate task: {}", e),
        }
    });

    // Start the Engine
    if let Err(err) = engine.start().await {
        eprintln!("❌ Engine runtime error: {}", err);
        process::exit(1);
    }

    println!("✅ Orion has stopped gracefully.");
}