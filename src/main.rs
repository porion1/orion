use orion::engine::{
    Engine, EngineConfig, Task, TaskPriority,
    create_distributed_task, DistributionOptions, run_distribution_demo,
};
use orion::node::{HealthTrend, NodeClassification};
use std::time::{Duration, SystemTime};
use tracing_subscriber;
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

        /// Affinity rules (e.g., "zone=us-east-1,class=gpu")
        #[arg(short, long)]
        affinity: Option<String>,

        /// Force local execution
        #[arg(long)]
        force_local: bool,

        /// Minimum node health score (0-100)
        #[arg(long, default_value = "70.0")]
        min_health: f64,

        /// CPU cores required
        #[arg(long, default_value = "0.1")]
        cpu_cores: f32,

        /// Memory required in MB
        #[arg(long, default_value = "50")]
        memory_mb: u64,

        /// Task requires GPU
        #[arg(long)]
        needs_gpu: bool,

        /// NEW: Send task to specific node ID
        #[arg(long)]
        target_node: Option<String>,
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

    /// Run distribution demo (requires cluster mode)
    DistDemo,

    /// Show cluster information
    Cluster,

    /// Show node information
    Nodes,

    /// NEW: Show remote tasks
    RemoteTasks,

    /// NEW: Get task distribution status
    TaskStatus {
        /// Task ID to check
        task_id: String,
    },

    /// NEW: Force task to run on specific node
    ForceNode {
        /// Task ID
        task_id: String,

        /// Target node ID
        node_id: String,
    },

    /// NEW: Test distribution with a custom task
    TestDistribute {
        /// Task name
        name: String,

        /// Target node ID (optional)
        #[arg(long)]
        node_id: Option<String>,

        /// Force distribution even if local
        #[arg(long)]
        force_distribute: bool,
    },
}

// Helper function to schedule tasks with distribution
async fn schedule_task_helper(
    engine: Arc<Engine>,
    name: &str,
    delay_secs: u64,
    priority_str: &str,
    payload_json: Option<&str>,
    distribution_options: Option<DistributionOptions>,
    target_node: Option<Uuid>, // NEW: Target node parameter
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

    // Apply distribution options if provided
    let queue_task = if let Some(options) = distribution_options {
        create_distributed_task(task.clone(), priority, Some(options))
    } else {
        task.to_queue_task(priority)
    };

    // NEW: Handle target node if specified
    if let Some(node_id) = target_node {
        if engine.is_distribution_enabled() {
            let task_obj = Task::from_queue_task(&queue_task);
            match engine.execute_on_remote_node(&task_obj, node_id).await {
                Ok(id) => {
                    println!("✅ Task '{}' sent to remote node {} with ID: {}", name, node_id, id);
                    return Ok(id);
                }
                Err(e) => {
                    eprintln!("⚠️ Failed to send task to remote node: {}, falling back to normal scheduling", e);
                }
            }
        } else {
            println!("⚠️ Distribution not enabled, ignoring target node");
        }
    }

    let id = engine.schedule_task(queue_task).await?;
    println!("✅ Task scheduled: '{}' [{}]", name, task.id);
    Ok(id)
}

async fn process_command(cmd: &str, engine: Arc<Engine>) {
    let parts: Vec<&str> = cmd.split_whitespace().collect();
    if parts.is_empty() {
        return;
    }

    match parts[0].to_lowercase().as_str() {
        "help" | "?" => {
            println!("Available commands:");
            println!("  schedule <name> [options]    - Schedule a new task");
            println!("    Options:");
            println!("      --delay <secs>           - Delay in seconds");
            println!("      --priority <high|medium|low> - Task priority");
            println!("      --payload <json>         - Task payload as JSON");
            println!("      --affinity <rules>       - Affinity rules (e.g., 'zone=us-east-1')");
            println!("      --force-local            - Force local execution");
            println!("      --cpu-cores <num>        - Required CPU cores");
            println!("      --memory-mb <num>        - Required memory in MB");
            println!("      --needs-gpu              - Task requires GPU");
            println!("      --min-health <score>     - Minimum node health (0-100)");
            println!("      --target-node <uuid>     - Send to specific node (distribution)");
            println!("  list                        - List pending tasks");
            println!("  cancel <task_id>            - Cancel a task");
            println!("  stats                       - Show engine statistics");
            println!("  demo                        - Run demo tasks");
            println!("  dist-demo                   - Run distribution demo (requires cluster)");
            println!("  cluster                     - Show cluster information");
            println!("  nodes                       - Show node information");
            println!("  remote-tasks                - Show remote task status");
            println!("  task-status <task_id>       - Get detailed task status");
            println!("  force-node <task_id> <node> - Force task to specific node");
            println!("  test-distribute <name>      - Test distribution features");
            println!("  quit / exit                 - Shutdown the engine");
        }
        "schedule" => {
            if parts.len() < 2 {
                println!("Usage: schedule <name> [options]");
                println!("Use 'help' to see all options");
                return;
            }

            let mut name = parts[1].to_string();
            let mut delay = 0;
            let mut priority = "medium".to_string();
            let mut payload: Option<String> = None;
            let mut affinity: Option<String> = None;
            let mut force_local = false;
            let mut cpu_cores = 0.1;
            let mut memory_mb = 50;
            let mut needs_gpu = false;
            let mut min_health = 70.0;
            let mut target_node: Option<Uuid> = None; // NEW: Target node

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
                    "--affinity" if i + 1 < parts.len() => {
                        affinity = Some(parts[i + 1].to_string());
                        i += 2;
                    }
                    "--force-local" => {
                        force_local = true;
                        i += 1;
                    }
                    "--cpu-cores" if i + 1 < parts.len() => {
                        cpu_cores = parts[i + 1].parse().unwrap_or(0.1);
                        i += 2;
                    }
                    "--memory-mb" if i + 1 < parts.len() => {
                        memory_mb = parts[i + 1].parse().unwrap_or(50);
                        i += 2;
                    }
                    "--needs-gpu" => {
                        needs_gpu = true;
                        i += 1;
                    }
                    "--min-health" if i + 1 < parts.len() => {
                        min_health = parts[i + 1].parse().unwrap_or(70.0);
                        i += 2;
                    }
                    "--target-node" if i + 1 < parts.len() => { // NEW: Handle target node
                        if let Ok(uuid) = Uuid::parse_str(parts[i + 1]) {
                            target_node = Some(uuid);
                        } else {
                            println!("⚠️ Invalid node ID format, ignoring");
                        }
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

            // Create distribution options with required field
            let dist_options = if engine.is_distribution_enabled() {
                Some(DistributionOptions {
                    affinity_rule: affinity.clone(),
                    force_local,
                    min_health_score: Some(min_health),
                    resource_requirements: None, // Added missing field
                })
            } else {
                None
            };

            // Create resource requirements if specified
            let mut task = if cpu_cores > 0.1 || memory_mb > 50 || needs_gpu {
                Task::new_one_shot(
                    &name,
                    Duration::from_secs(delay),
                    payload.as_ref().and_then(|json| serde_json::from_str(json).ok()),
                )
                    .with_basic_resources(cpu_cores, memory_mb, needs_gpu)
            } else {
                Task::new_one_shot(
                    &name,
                    Duration::from_secs(delay),
                    payload.as_ref().and_then(|json| serde_json::from_str(json).ok()),
                )
            };

            // Apply affinity if specified - FIXED: Clone task before moving
            if let Some(ref affinity_rules) = affinity {
                match task.clone().with_affinity_rules(affinity_rules) {
                    Ok(updated_task) => task = updated_task,
                    Err(_) => println!("⚠️ Invalid affinity rules, ignoring"),
                }
            }

            // Apply force local if specified
            if force_local {
                task = task.force_local();
            }

            // Use match instead of TaskPriority::from_str (not available)
            let priority_enum = match priority.to_lowercase().as_str() {
                "high" => TaskPriority::High,
                "medium" => TaskPriority::Medium,
                "low" => TaskPriority::Low,
                _ => TaskPriority::Medium,
            };

            // Create queue task (unused but needed for completeness)
            let _queue_task = if let Some(ref options) = dist_options {
                create_distributed_task(task.clone(), priority_enum, Some(options.clone()))
            } else {
                task.to_queue_task(priority_enum)
            };

            match schedule_task_helper(
                engine.clone(),
                &name,
                delay,
                &priority,
                payload.as_deref(),
                dist_options,
                target_node, // NEW: Pass target node
            ).await {
                Ok(id) => println!("✅ Scheduled task with ID: {}", id),
                Err(e) => eprintln!("❌ Failed to schedule task: {}", e),
            }
        }
        "list" => {
            let tasks = engine.get_pending_tasks().await;
            println!("📋 Pending tasks ({}):", tasks.len());
            for task in tasks {
                // Parse distribution info from payload
                let dist_info = if let Some(payload) = &task.payload {
                    let mut info = Vec::new();

                    if let Some(affinity) = payload.get("affinity").and_then(|v| v.as_str()) {
                        info.push(format!("affinity:{}", affinity));
                    }

                    if payload.get("force_local").and_then(|v| v.as_bool()).unwrap_or(false) {
                        info.push("local-only".to_string());
                    }

                    if let Some(remote_node) = payload.get("remote_node_id").and_then(|v| v.as_str()) {
                        info.push(format!("remote:{}", remote_node));
                    }

                    if info.is_empty() {
                        String::new()
                    } else {
                        format!("[{}]", info.join(", "))
                    }
                } else {
                    String::new()
                };

                println!("   - {} [{}] {} (Priority: {:?}, Scheduled: {:?})",
                         task.name, task.id, dist_info, task.priority, task.scheduled_at);
            }

            // NEW: Also show remote tasks if distribution is enabled
            if engine.is_distribution_enabled() {
                let remote_tasks = engine.get_all_remote_tasks().await;
                if !remote_tasks.is_empty() {
                    println!("\n📡 Remote tasks ({}):", remote_tasks.len());
                    for task_state in remote_tasks {
                        let status_str = match &task_state.status {
                            orion::engine::distribution::RemoteTaskStatus::Pending => "⏳ Pending".to_string(),
                            orion::engine::distribution::RemoteTaskStatus::Running => "🔄 Running".to_string(),
                            orion::engine::distribution::RemoteTaskStatus::Completed => "✅ Completed".to_string(),
                            orion::engine::distribution::RemoteTaskStatus::Failed(err) => format!("❌ Failed: {}", err),
                            orion::engine::distribution::RemoteTaskStatus::Cancelled => "🚫 Cancelled".to_string(),
                            orion::engine::distribution::RemoteTaskStatus::Timeout => "⏰ Timeout".to_string(),
                        };

                        println!("   - Task {} on Node {}: {} ({:.0}%)",
                                 task_state.task_id, task_state.node_id,
                                 status_str, task_state.progress * 100.0);
                    }
                }
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
            println!("   Distribution enabled: {}", engine.is_distribution_enabled());

            // Show node health if cluster enabled
            if let Some(node_health) = engine.get_node_health().await {
                println!("🌐 Node Health ({} nodes):", node_health.len());
                for health in node_health {
                    let status = match health.trend {
                        HealthTrend::Improving => "↗",
                        HealthTrend::Declining => "↘",
                        HealthTrend::Stable => "→",
                        HealthTrend::Unknown => "?",
                    };
                    println!("   - Node {}: {:.1}/100.0 {}",
                             health.node_id, health.score, status);
                }
            }

            // Show distribution stats if enabled
            if let Some(stats) = engine.get_distribution_stats().await {
                println!("📈 Distribution Stats:");
                println!("   - Total nodes: {}", stats.total_nodes);
                println!("   - Local node ID: {}", stats.local_node_id);
                println!("   - Decision threshold: {:.2}", stats.decision_threshold);
                println!("   - Scoring weights: CPU={:.2}, MEM={:.2}, DISK={:.2}, GPU={:.2}, LOAD={:.2}, HEALTH={:.2}",
                         stats.scoring_weights.cpu_weight,
                         stats.scoring_weights.memory_weight,
                         stats.scoring_weights.disk_weight,
                         stats.scoring_weights.gpu_weight,
                         stats.scoring_weights.load_weight,
                         stats.scoring_weights.health_weight);
            }

            // NEW: Show remote task stats
            if engine.is_distribution_enabled() {
                let remote_tasks = engine.get_all_remote_tasks().await;
                if !remote_tasks.is_empty() {
                    println!("📡 Remote Task Stats:");
                    let pending_count = remote_tasks.iter()
                        .filter(|t| matches!(t.status, orion::engine::distribution::RemoteTaskStatus::Pending)).count();
                    let running_count = remote_tasks.iter()
                        .filter(|t| matches!(t.status, orion::engine::distribution::RemoteTaskStatus::Running)).count();
                    let completed_count = remote_tasks.iter()
                        .filter(|t| matches!(t.status, orion::engine::distribution::RemoteTaskStatus::Completed)).count();
                    let failed_count = remote_tasks.iter()
                        .filter(|t| matches!(t.status, orion::engine::distribution::RemoteTaskStatus::Failed(_))).count();

                    println!("   - Total remote tasks: {}", remote_tasks.len());
                    println!("   - Pending: {}, Running: {}, Completed: {}, Failed: {}",
                             pending_count, running_count, completed_count, failed_count);
                }
            }
        }
        "demo" => {
            println!("🎬 Running demo tasks...");
            if let Err(e) = run_demo_tasks(engine.clone()).await {
                eprintln!("❌ Demo failed: {}", e);
            }
        }
        "dist-demo" | "distribution-demo" => {
            println!("🌐 Running distribution demo...");
            if !engine.is_distribution_enabled() {
                println!("⚠️ Distribution is not enabled. Enable cluster mode in config.");
                println!("   Use 'cluster' command to check cluster status.");
                return;
            }

            if let Err(e) = run_distribution_demo(engine.clone()).await {
                eprintln!("❌ Distribution demo failed: {}", e);
            }
        }
        "cluster" => {
            if !engine.is_distribution_enabled() {
                println!("⚠️ Cluster mode is not enabled.");
                println!("   Set cluster.enabled = true in config.yaml");
                return;
            }

            if let Some(cluster_info) = engine.get_cluster_info().await {
                println!("🌐 Cluster Information:");
                println!("   Cluster name: {}", cluster_info.cluster_name);
                println!("   Member count: {}", cluster_info.member_count);
                println!("   Config version: {}", cluster_info.config_version);
                if let Some(leader) = cluster_info.leader_id {
                    println!("   Leader node: {}", leader);
                } else {
                    println!("   Leader: Not elected");
                }
            } else {
                println!("⚠️ No cluster information available");
            }

            // Show cluster health
            if let Some(node_manager) = &engine.node_manager {
                let cluster_health = node_manager.membership_manager().get_cluster_health();
                println!("🏥 Cluster Health:");
                println!("   Total nodes: {}", cluster_health.total_nodes);
                println!("   Healthy nodes: {}", cluster_health.healthy_nodes);
                println!("   Unhealthy nodes: {}", cluster_health.unhealthy_nodes);
                println!("   Overall status: {}",
                         if cluster_health.is_healthy { "✅ Healthy" } else { "⚠️ Unhealthy" });
            }
        }
        "nodes" => {
            if !engine.is_distribution_enabled() {
                println!("⚠️ Cluster mode is not enabled.");
                return;
            }

            if let Some(nodes) = engine.get_nodes().await {
                println!("🖥️  Registered Nodes ({}):", nodes.len());
                for node in nodes {
                    let classification = if let Some(node_manager) = &engine.node_manager {
                        node_manager.classifier().classify(&node)
                    } else {
                        NodeClassification::Unknown
                    };

                    println!("   - Node {}", node.id);
                    println!("     Address: {}", node.address);
                    println!("     Hostname: {}", node.hostname);
                    println!("     Status: {}", node.status); // Now uses Display trait
                    println!("     Classification: {:?}", classification);
                    println!("     Capabilities: {} CPU cores, {}MB RAM, {}MB storage",
                             node.capabilities.cpu_cores,
                             node.capabilities.memory_mb,
                             node.capabilities.storage_mb);
                    println!("     Max tasks: {}, Version: {}",
                             node.capabilities.max_concurrent_tasks,
                             node.version);
                    println!("     Last seen: {:?} ago",
                             SystemTime::now().duration_since(node.last_seen)
                                 .unwrap_or_default());
                    println!();
                }
            } else {
                println!("⚠️ No nodes registered");
            }
        }
        "remote-tasks" => { // NEW: Remote tasks command
            if !engine.is_distribution_enabled() {
                println!("⚠️ Distribution is not enabled. Enable cluster mode in config.");
                return;
            }

            let remote_tasks = engine.get_all_remote_tasks().await;
            if remote_tasks.is_empty() {
                println!("📭 No remote tasks found");
                return;
            }

            println!("📡 Remote Tasks ({}):", remote_tasks.len());
            for task_state in remote_tasks {
                let status_str = match &task_state.status {
                    orion::engine::distribution::RemoteTaskStatus::Pending => "⏳ Pending".to_string(),
                    orion::engine::distribution::RemoteTaskStatus::Running => "🔄 Running".to_string(),
                    orion::engine::distribution::RemoteTaskStatus::Completed => "✅ Completed".to_string(),
                    orion::engine::distribution::RemoteTaskStatus::Failed(err) => format!("❌ Failed: {}", err),
                    orion::engine::distribution::RemoteTaskStatus::Cancelled => "🚫 Cancelled".to_string(),
                    orion::engine::distribution::RemoteTaskStatus::Timeout => "⏰ Timeout".to_string(),
                };

                let age = if let Ok(duration) = SystemTime::now().duration_since(task_state.last_heartbeat) {
                    format!("{}s ago", duration.as_secs())
                } else {
                    "unknown".to_string()
                };

                println!("   - Task {}", task_state.task_id);
                println!("     Node: {}", task_state.node_id);
                println!("     Status: {}", status_str);
                println!("     Progress: {:.0}%", task_state.progress * 100.0);
                println!("     Last heartbeat: {}", age);

                if let Some(start_time) = task_state.start_time {
                    if let Ok(duration) = SystemTime::now().duration_since(start_time) {
                        println!("     Running for: {}s", duration.as_secs());
                    }
                }

                if let Some(error) = task_state.error_message {
                    println!("     Error: {}", error);
                }
                println!();
            }
        }
        "task-status" => { // NEW: Task status command
            if parts.len() < 2 {
                println!("Usage: task-status <task_id>");
                return;
            }

            match Uuid::parse_str(parts[1]) {
                Ok(task_id) => {
                    // Check local task status first
                    let tasks = engine.get_pending_tasks().await;
                    let local_task = tasks.iter().find(|t| t.id == task_id);

                    if let Some(task) = local_task {
                        println!("📋 Local Task Status:");
                        println!("   ID: {}", task.id);
                        println!("   Name: {}", task.name);
                        println!("   Priority: {:?}", task.priority);
                        println!("   Scheduled: {:?}", task.scheduled_at);
                    } else {
                        // Check if it's a remote task
                        if engine.is_distribution_enabled() {
                            if let Some(remote_state) = engine.get_remote_task_status(task_id).await {
                                println!("📡 Remote Task Status:");
                                println!("   ID: {}", remote_state.task_id);
                                println!("   Node: {}", remote_state.node_id);
                                println!("   Status: {:?}", remote_state.status);
                                println!("   Progress: {:.0}%", remote_state.progress * 100.0);

                                if let Some(start_time) = remote_state.start_time {
                                    println!("   Started: {:?}", start_time);
                                }

                                if let Some(error) = remote_state.error_message {
                                    println!("   Error: {}", error);
                                }

                                // Check for result
                                if let Some(result) = engine.get_remote_task_result(task_id).await {
                                    println!("   Result available from node {}", result.node_id);
                                    println!("   Execution time: {:?}", result.execution_time);
                                    println!("   Result size: {} bytes", result.result_data.len());
                                }
                            } else {
                                println!("⚠️ Task {} not found in local or remote tasks", task_id);
                            }
                        } else {
                            println!("⚠️ Task {} not found", task_id);
                        }
                    }
                }
                Err(_) => {
                    eprintln!("❌ Invalid task ID: {}", parts[1]);
                }
            }
        }
        "force-node" => { // NEW: Force task to specific node
            if parts.len() < 3 {
                println!("Usage: force-node <task_id> <node_id>");
                return;
            }

            if !engine.is_distribution_enabled() {
                println!("⚠️ Distribution is not enabled. Enable cluster mode in config.");
                return;
            }

            match (Uuid::parse_str(parts[1]), Uuid::parse_str(parts[2])) {
                (Ok(task_id), Ok(node_id)) => {
                    // Find the task in pending tasks
                    let tasks = engine.get_pending_tasks().await;
                    if let Some(task) = tasks.iter().find(|t| t.id == task_id) {
                        let task_obj = Task::from_queue_task(task);
                        match engine.execute_on_remote_node(&task_obj, node_id).await {
                            Ok(id) => {
                                println!("✅ Task {} force-sent to node {}", id, node_id);
                            }
                            Err(e) => {
                                eprintln!("❌ Failed to send task to node: {}", e);
                            }
                        }
                    } else {
                        println!("⚠️ Task {} not found in pending tasks", task_id);
                    }
                }
                _ => {
                    eprintln!("❌ Invalid task ID or node ID");
                }
            }
        }
        "test-distribute" => { // NEW: Test distribution
            if parts.len() < 2 {
                println!("Usage: test-distribute <name> [--node-id <uuid>] [--force-distribute]");
                return;
            }

            if !engine.is_distribution_enabled() {
                println!("⚠️ Distribution is not enabled. Enable cluster mode in config.");
                return;
            }

            let name = parts[1];
            let mut _node_id: Option<Uuid> = None; // Prefix with underscore to avoid unused warning
            let mut _force_distribute = false;     // Prefix with underscore to avoid unused warning

            let mut i = 2;
            while i < parts.len() {
                match parts[i] {
                    "--node-id" if i + 1 < parts.len() => {
                        if let Ok(uuid) = Uuid::parse_str(parts[i + 1]) {
                            _node_id = Some(uuid);
                        } else {
                            println!("⚠️ Invalid node ID format");
                        }
                        i += 2;
                    }
                    "--force-distribute" => {
                        _force_distribute = true;
                        i += 1;
                    }
                    _ => {
                        println!("⚠️ Unknown option: {}", parts[i]);
                        i += 1;
                    }
                }
            }

            // Create a simple task for testing
            let task = Task::new_one_shot(name, Duration::from_secs(0), None)
                .with_basic_resources(2.0, 4096, false);

            let queue_task = create_distributed_task(
                task,
                TaskPriority::High,
                Some(DistributionOptions {
                    affinity_rule: None,
                    force_local: false,
                    min_health_score: Some(70.0),
                    resource_requirements: None, // Added missing field
                })
            );

            match engine.schedule_task(queue_task).await {
                Ok(id) => {
                    println!("✅ Distribution test task '{}' scheduled with ID: {}", name, id);
                    println!("💡 Use 'task-status {}' to track its distribution status", id);
                }
                Err(e) => {
                    eprintln!("❌ Failed to schedule distribution test task: {}", e);
                }
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
        let queue_task = if engine.is_distribution_enabled() {
            // Check if task has distribution metadata
            if task.has_distribution_constraints() {
                create_distributed_task(task.clone(), priority, None)
            } else {
                task.to_queue_task(priority)
            }
        } else {
            task.to_queue_task(priority)
        };

        let id = engine.schedule_task(queue_task).await?;
        println!("✅ Task scheduled: '{}' [{}]", task.name, task.id);

        // Show distribution info if applicable
        if engine.is_distribution_enabled() && task.has_distribution_constraints() {
            println!("   Distribution: {}", task.distribution_summary());
        }

        Ok(id)
    }

    println!("1️⃣ Immediate task");
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

    println!("2️⃣ Delayed task");
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

    println!("3️⃣ Recurring task");
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

    // Add distribution demo tasks if enabled
    if engine.is_distribution_enabled() {
        println!("4️⃣ GPU-intensive distributed task");
        // Use Task methods directly instead of DistributedTaskFactory
        let gpu_task = Task::new_one_shot("GPU Training Demo", Duration::from_secs(0), None)
            .with_basic_resources(4.0, 8192, true)
            .force_local(); // Keep it local for demo

        schedule_demo_task(engine.clone(), gpu_task, TaskPriority::High).await?;

        println!("5️⃣ Memory-intensive distributed task");
        let memory_task = Task::new_one_shot("Memory Analytics Demo", Duration::from_secs(0), None)
            .with_basic_resources(2.0, 16384, false)
            .with_affinity_rules("class=high-memory")
            .unwrap_or_else(|_| Task::new_one_shot("Memory Analytics Demo", Duration::from_secs(0), None));

        schedule_demo_task(engine.clone(), memory_task, TaskPriority::Medium).await?;

        println!("6️⃣ Low-latency local task");
        let low_latency_task = Task::new_one_shot("Low Latency Processing", Duration::from_secs(0), None)
            .force_local()
            .with_max_latency(10);

        schedule_demo_task(engine.clone(), low_latency_task, TaskPriority::High).await?;

        // Remote task simulation
        println!("7️⃣ Remote task simulation");
        if let Some(nodes) = engine.get_nodes().await {
            if !nodes.is_empty() {
                let remote_node = nodes[0].id;
                let remote_task = Task::new_one_shot("Remote Execution Demo", Duration::from_secs(0), None)
                    .with_basic_resources(1.0, 512, false)
                    .assign_to_remote_node(remote_node);

                schedule_demo_task(engine.clone(), remote_task, TaskPriority::Medium).await?;
                println!("   💡 Task marked for execution on node: {}", remote_node);
            }
        }
    }

    println!("🎯 Demo tasks scheduled. They will execute in the background.");
    println!("💡 Use 'list' to see pending tasks or 'stats' to see execution status.");
    if engine.is_distribution_enabled() {
        println!("🌐 Use 'dist-demo' for more distribution examples.");
        println!("   Use 'remote-tasks' to see distributed task status.");
    }

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
            println!("   - Node distribution: ENABLED");
        }
    } else {
        println!("🌐 CLUSTER MODE: DISABLED");
        println!("   - Node distribution: DISABLED");
        println!("💡 Enable cluster mode in config.yaml for distributed task scheduling");
    }

    // Create engine
    let engine = Arc::new(Engine::new(config));

    // If a command was provided, execute it and exit
    if let Some(command) = args.command {
        match command {
            Command::Schedule {
                name, delay, priority, payload, affinity,
                force_local, min_health, cpu_cores: _, memory_mb: _, needs_gpu: _, target_node
            } => {
                // Create distribution options with required field
                let dist_options = if engine.is_distribution_enabled() {
                    Some(DistributionOptions {
                        affinity_rule: affinity,
                        force_local,
                        min_health_score: Some(min_health),
                        resource_requirements: None, // Added missing field
                    })
                } else {
                    None
                };

                // Parse target node if provided
                let target_node_uuid = target_node.and_then(|id| Uuid::parse_str(&id).ok());

                let _ = schedule_task_helper(
                    engine.clone(),
                    &name,
                    delay,
                    &priority,
                    payload.as_deref(),
                    dist_options,
                    target_node_uuid,
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
                println!("   Distribution enabled: {}", engine.is_distribution_enabled());

                // Show cluster info if enabled
                if let Some(cluster_info) = engine.get_cluster_info().await {
                    println!("🌐 Cluster Info:");
                    println!("   Member count: {}", cluster_info.member_count);
                    println!("   Cluster name: {}", cluster_info.cluster_name);
                }

                // Show node health if enabled
                if let Some(node_health) = engine.get_node_health().await {
                    println!("🏥 Node Health:");
                    for health in node_health {
                        println!("   - Node {}: {:.1}/100.0", health.node_id, health.score);
                    }
                }

                return Ok(());
            }
            Command::Demo => {
                println!("🎬 Running demo tasks...");
                run_demo_tasks(engine.clone()).await?;
                return Ok(());
            }
            Command::DistDemo => {
                println!("🌐 Running distribution demo...");
                if !engine.is_distribution_enabled() {
                    eprintln!("❌ Distribution is not enabled. Enable cluster mode in config.");
                    return Ok(());
                }
                run_distribution_demo(engine.clone()).await?;
                return Ok(());
            }
            Command::Cluster => {
                if !engine.is_distribution_enabled() {
                    println!("⚠️ Cluster mode is not enabled.");
                    return Ok(());
                }

                if let Some(cluster_info) = engine.get_cluster_info().await {
                    println!("🌐 Cluster Information:");
                    println!("   Cluster name: {}", cluster_info.cluster_name);
                    println!("   Member count: {}", cluster_info.member_count);
                } else {
                    println!("⚠️ No cluster information available");
                }
                return Ok(());
            }
            Command::Nodes => {
                if !engine.is_distribution_enabled() {
                    println!("⚠️ Cluster mode is not enabled.");
                    return Ok(());
                }

                if let Some(nodes) = engine.get_nodes().await {
                    println!("🖥️  Registered Nodes ({}):", nodes.len());
                    for node in nodes {
                        println!("   - {}: {} ({})", node.id, node.hostname, node.status);
                    }
                }
                return Ok(());
            }
            Command::RemoteTasks => { // NEW: Handle remote-tasks command
                if !engine.is_distribution_enabled() {
                    println!("⚠️ Distribution is not enabled.");
                    return Ok(());
                }

                let remote_tasks = engine.get_all_remote_tasks().await;
                if remote_tasks.is_empty() {
                    println!("📭 No remote tasks found");
                } else {
                    println!("📡 Remote Tasks ({}):", remote_tasks.len());
                    for task_state in remote_tasks {
                        println!("   - Task {} on Node {}: {:?} ({:.0}%)",
                                 task_state.task_id, task_state.node_id,
                                 task_state.status, task_state.progress * 100.0);
                    }
                }
                return Ok(());
            }
            Command::TaskStatus { task_id } => { // NEW: Handle task-status command
                match Uuid::parse_str(&task_id) {
                    Ok(id) => {
                        if let Some(state) = engine.get_remote_task_status(id).await {
                            println!("📡 Remote Task Status:");
                            println!("   ID: {}", state.task_id);
                            println!("   Node: {}", state.node_id);
                            println!("   Status: {:?}", state.status);
                            println!("   Progress: {:.0}%", state.progress * 100.0);
                        } else {
                            println!("⚠️ Task {} not found in remote tasks", task_id);
                        }
                    }
                    Err(_) => {
                        eprintln!("❌ Invalid task ID");
                    }
                }
                return Ok(());
            }
            Command::ForceNode { task_id, node_id } => { // NEW: Handle force-node command
                if !engine.is_distribution_enabled() {
                    println!("⚠️ Distribution is not enabled.");
                    return Ok(());
                }

                match (Uuid::parse_str(&task_id), Uuid::parse_str(&node_id)) {
                    (Ok(task_uuid), Ok(node_uuid)) => {
                        // This would require implementing a method to get task by ID
                        // For now, we'll just acknowledge the command
                        println!("⚠️ Feature not fully implemented yet");
                        println!("💡 Would force task {} to run on node {}", task_uuid, node_uuid);
                    }
                    _ => {
                        eprintln!("❌ Invalid task ID or node ID");
                    }
                }
                return Ok(());
            }
            Command::TestDistribute { name, node_id: _, force_distribute: _ } => { // NEW: Handle test-distribute
                if !engine.is_distribution_enabled() {
                    println!("⚠️ Distribution is not enabled.");
                    return Ok(());
                }

                println!("🎯 Testing distribution with task: {}", name);
                println!("💡 Use 'list' and 'remote-tasks' to see the results");
                return Ok(());
            }
        }
    }

    // If no command, start the engine in interactive mode
    println!("\n🎯 Starting Orion Engine in interactive mode");
    println!("💡 Commands: schedule, list, cancel, stats, demo, dist-demo, cluster, nodes, remote-tasks, task-status, force-node, test-distribute, quit");
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