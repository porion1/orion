use orion::engine::{Engine, EngineConfig, Task, QueueTask, TaskPriority};
use tracing_subscriber;
use std::time::Duration;
use uuid::Uuid;
use std::sync::Arc;
use anyhow::Result;
use clap::{Parser, Subcommand};
use tokio::sync::mpsc;
use std::io::{self, Write};

// Add CLI argument parsing with commands
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to configuration file
    #[arg(short, long, default_value = "config.yaml")]
    config: String,

    /// Command to execute
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Schedule a new task
    Schedule {
        /// Task name
        name: String,

        /// Delay in seconds
        #[arg(short, long, default_value = "0")]
        delay: u64,

        /// Task priority (high, medium, low)
        #[arg(short, long, default_value = "medium")]
        priority: String,

        /// Task payload as JSON
        #[arg(short, long)]
        payload: Option<String>,
    },

    /// List pending tasks
    List,

    /// Cancel a task
    Cancel {
        /// Task ID to cancel
        task_id: String,
    },

    /// Show engine statistics
    Stats,

    /// Run demo tasks
    Demo,
}

// Helper function to schedule tasks (moved outside main)
async fn schedule_task_helper(
    engine: Arc<Engine>,
    name: &str,
    delay_secs: u64,
    priority_str: &str,
    payload_json: Option<&str>,
) -> Result<Uuid> {
    let priority = match priority_str.to_lowercase().as_str() {
        "high" => TaskPriority::High,
        "medium" => TaskPriority::Medium,
        "low" => TaskPriority::Low,
        _ => TaskPriority::Medium,
    };

    let payload = payload_json.and_then(|json| {
        serde_json::from_str(json).ok()
    });

    let task = Task::new_one_shot(
        name,
        Duration::from_secs(delay_secs),
        payload,
    );

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
    println!("✅ Task scheduled: '{}' [{}]", name, id);
    Ok(id)
}

async fn process_command(cmd: &str, engine: Arc<Engine>) {
    let parts: Vec<&str> = cmd.split_whitespace().collect();
    if parts.is_empty() {
        return;
    }

    match parts[0].to_lowercase().as_str() {
        "help" => {
            println!("Available commands:");
            println!("  schedule <name> [--delay <secs>] [--priority <high|medium|low>] [--payload <json>]");
            println!("  list                        - List pending tasks");
            println!("  cancel <task_id>            - Cancel a task");
            println!("  stats                       - Show engine statistics");
            println!("  demo                        - Run demo tasks");
            println!("  quit                        - Shutdown the engine");
        }
        "schedule" => {
            if parts.len() < 2 {
                println!("Usage: schedule <name> [--delay <secs>] [--priority <high|medium|low>] [--payload <json>]");
                return;
            }

            let mut name = parts[1].to_string();
            let mut delay = 0;
            let mut priority = "medium".to_string();
            let mut payload: Option<String> = None;

            let mut i = 2;
            while i < parts.len() {
                match parts[i] {
                    "--delay" if i + 1 < parts.len() => {
                        delay = parts[i + 1].parse().unwrap_or(0);
                        i += 2;
                    }
                    "--priority" if i + 1 < parts.len() => {
                        priority = parts[i + 1].to_string();
                        i += 2;
                    }
                    "--payload" if i + 1 < parts.len() => {
                        payload = Some(parts[i + 1].to_string());
                        i += 2;
                    }
                    _ => {
                        // If no flag, treat as part of name
                        name.push(' ');
                        name.push_str(parts[i]);
                        i += 1;
                    }
                }
            }

            match schedule_task_helper(
                engine.clone(),
                &name,
                delay,
                &priority,
                payload.as_deref(),
            ).await {
                Ok(id) => println!("✅ Scheduled task with ID: {}", id),
                Err(e) => eprintln!("❌ Failed to schedule task: {}", e),
            }
        }
        "list" => {
            let tasks = engine.get_pending_tasks().await;
            println!("📋 Pending tasks ({}):", tasks.len());
            for task in tasks {
                println!("   - {} [{}] (Priority: {:?}, Scheduled: {:?})",
                         task.name, task.id, task.priority, task.scheduled_at);
            }
        }
        "cancel" => {
            if parts.len() < 2 {
                println!("Usage: cancel <task_id>");
                return;
            }

            match Uuid::parse_str(parts[1]) {
                Ok(id) => {
                    if engine.cancel_task(id).await {
                        println!("✅ Task {} cancelled", parts[1]);
                    } else {
                        println!("⚠️ Task {} not found or already completed", parts[1]);
                    }
                }
                Err(_) => {
                    eprintln!("❌ Invalid task ID: {}", parts[1]);
                }
            }
        }
        "stats" => {
            let pending = engine.get_pending_tasks().await.len();
            let completed = engine.executor.get_completed_count().await;
            let active = engine.executor.get_active_count().await;

            println!("📊 Engine Statistics:");
            println!("   Pending tasks: {}", pending);
            println!("   Active tasks: {}", active);
            println!("   Completed tasks: {}", completed);

            // Show node health if cluster enabled
            if let Some(node_health) = engine.get_node_health().await {
                println!("🌐 Node Health:");
                for health in node_health {
                    println!("   - Node {}: {:.1}/100.0 ({:?})",
                             health.node_id, health.score, health.trend);
                }
            }
        }
        "demo" => {
            println!("🎬 Running demo tasks...");
            if let Err(e) = run_demo_tasks(engine.clone()).await {
                eprintln!("❌ Demo failed: {}", e);
            }
        }
        _ => {
            println!("❌ Unknown command. Type 'help' for available commands.");
        }
    }
}

async fn run_demo_tasks(engine: Arc<Engine>) -> Result<()> {
    println!("📋 Scheduling demo tasks...");

    // Helper function
    async fn schedule_demo_task(
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

    // 1️⃣ Immediate task
    schedule_demo_task(
        engine.clone(),
        Task::new_one_shot(
            "Immediate Demo Task",
            Duration::from_secs(0),
            Some(serde_json::json!({
                "action": "demo",
                "timestamp": chrono::Utc::now().to_rfc3339(),
            })),
        ),
        TaskPriority::High,
    ).await?;

    // 2️⃣ Delayed task
    schedule_demo_task(
        engine.clone(),
        Task::new_one_shot(
            "Delayed Demo Task",
            Duration::from_secs(5),
            Some(serde_json::json!({
                "action": "delayed_demo",
                "message": "This task was scheduled 5 seconds in the future",
            })),
        ),
        TaskPriority::Medium,
    ).await?;

    // 3️⃣ Recurring task
    match Task::new_recurring(
        "Recurring Demo Task",
        "*/30 * * * * *",  // Every 30 seconds
        Some(serde_json::json!({
            "action": "recurring_demo",
            "iteration": 1,
        })),
    ) {
        Ok(task) => {
            let id = task.id;
            schedule_demo_task(engine.clone(), task, TaskPriority::Low).await?;
            println!("🔄 Recurring task scheduled (every 30 seconds) [{}]", id);
        }
        Err(e) => {
            eprintln!("⚠️ Failed to create recurring task: {}", e);
        }
    }

    println!("🎯 Demo tasks scheduled. They will execute in the background.");
    println!("💡 Use 'list' to see pending tasks or 'stats' to see execution status.");

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // Parse CLI arguments
    let args = Args::parse();
    let config_path = &args.config;

    // Initialize logging
    tracing_subscriber::fmt::init();
    println!("🚀 Starting Orion Engine");
    println!("📄 Loading configuration from: {}", config_path);

    // Load configuration
    let config = match EngineConfig::from_yaml(config_path) {
        Ok(config) => {
            println!("✅ Configuration loaded successfully");
            config
        }
        Err(err) => {
            eprintln!("⚠️ Failed to load config from '{}': {}", config_path, err);
            eprintln!("📝 Using default configuration");
            EngineConfig::default()
        }
    };

    println!("⚙️ Engine configuration: {:?}", config);

    // Check if cluster mode is enabled
    if config.is_cluster_enabled() {
        println!("🌐 CLUSTER MODE: ENABLED");
        if let Some(cluster_config) = &config.cluster_config {
            println!("   - Node listen address: {}", cluster_config.node_listen_addr);
            println!("   - Cluster peers: {:?}", cluster_config.cluster_peers);
            println!("   - Heartbeat interval: {}ms", cluster_config.heartbeat_interval_ms);
        }
    } else {
        println!("🌐 CLUSTER MODE: DISABLED");
    }

    // Create engine
    let engine = Arc::new(Engine::new(config));

    // If a command was provided, execute it and exit
    if let Some(command) = args.command {
        match command {
            Command::Schedule { name, delay, priority, payload } => {
                let _ = schedule_task_helper(
                    engine.clone(),
                    &name,
                    delay,
                    &priority,
                    payload.as_deref(),
                ).await?;
                return Ok(());
            }
            Command::List => {
                let tasks = engine.get_pending_tasks().await;
                println!("📋 Pending tasks ({}):", tasks.len());
                for task in tasks {
                    println!("   - {} [{}] (Priority: {:?}, Scheduled: {:?})",
                             task.name, task.id, task.priority, task.scheduled_at);
                }
                return Ok(());
            }
            Command::Cancel { task_id } => {
                match Uuid::parse_str(&task_id) {
                    Ok(id) => {
                        if engine.cancel_task(id).await {
                            println!("✅ Task {} cancelled", task_id);
                        } else {
                            println!("⚠️ Task {} not found or already completed", task_id);
                        }
                    }
                    Err(_) => {
                        eprintln!("❌ Invalid task ID: {}", task_id);
                    }
                }
                return Ok(());
            }
            Command::Stats => {
                let pending = engine.get_pending_tasks().await.len();
                let completed = engine.executor.get_completed_count().await;
                let active = engine.executor.get_active_count().await;

                println!("📊 Engine Statistics:");
                println!("   Pending tasks: {}", pending);
                println!("   Active tasks: {}", active);
                println!("   Completed tasks: {}", completed);

                // Show cluster info if enabled
                if let Some(cluster_info) = engine.get_cluster_info().await {
                    println!("🌐 Cluster Info:");
                    println!("   Member count: {}", cluster_info.member_count);
                    println!("   Cluster name: {}", cluster_info.cluster_name);
                }

                return Ok(());
            }
            Command::Demo => {
                println!("🎬 Running demo tasks...");
                run_demo_tasks(engine.clone()).await?;
                return Ok(());
            }
        }
    }

    // If no command, start the engine in interactive mode
    println!("\n🎯 Starting Orion Engine in interactive mode");
    println!("💡 Commands: schedule, list, cancel <id>, stats, demo, quit");
    println!("{}", "=".repeat(50));

    // Start the engine in background
    let engine_for_background = engine.clone();
    let _engine_handle = tokio::spawn(async move {
        if let Err(e) = engine_for_background.start().await {
            eprintln!("Engine error: {}", e);
        }
    });

    // Command channel for interactive input
    let (tx, mut rx) = mpsc::channel::<String>(100);

    // Spawn stdin reader
    let tx_clone = tx.clone();
    tokio::spawn(async move {
        let stdin = io::stdin();
        let mut buffer = String::new();

        loop {
            print!("orion> ");
            io::stdout().flush().unwrap();
            buffer.clear();

            if stdin.read_line(&mut buffer).is_ok() {
                let cmd = buffer.trim().to_string();
                if cmd.is_empty() {
                    continue;
                }

                if cmd == "quit" || cmd == "exit" {
                    let _ = tx_clone.send(cmd).await;
                    break;
                }

                let _ = tx_clone.send(cmd).await;
            }
        }
    });

    // Command processing loop
    println!("📝 Type 'help' for available commands");

    loop {
        tokio::select! {
            cmd = rx.recv() => {
                match cmd {
                    Some(cmd_str) => {
                        if cmd_str == "quit" || cmd_str == "exit" {
                            println!("🛑 Shutting down...");
                            engine.shutdown();
                            break;
                        }

                        process_command(&cmd_str, engine.clone()).await;
                    }
                    None => break,
                }
            }
            _ = tokio::signal::ctrl_c() => {
                println!("\n🛑 Ctrl+C received, shutting down...");
                engine.shutdown();
                break;
            }
        }
    }

    // Wait for engine to shutdown gracefully
    tokio::time::sleep(Duration::from_secs(3)).await;

    println!("✅ Orion Engine stopped");
    Ok(())
}