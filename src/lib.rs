//! Orion Task Scheduler
//!
//! A distributed, fault-tolerant task scheduler and executor.
//!
//! ## Core Concepts
//! - **Tasks**: Individual units of work with scheduling requirements
//! - **Queue**: Priority-based task queue with persistence
//! - **Executor**: Concurrent task execution with retry logic
//! - **Scheduler**: Orchestrates task lifecycle and scheduling
//!
//! ## Quick Start
//! ```
//! use orion::{Engine, EngineConfig, Task};
//! use std::time::Duration;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let config = EngineConfig::defaults();
//!     let engine = orion::Engine::new(config);
//!
//!     // Create a task
//!     let task = Task::new_one_shot("my_task", Duration::from_secs(5), None);
//!
//!     // Schedule it
//!     let task_id = engine.schedule_task(task.into()).await?;
//!
//!     Ok(())
//! }
//! ```

use std::sync::Arc;

pub mod cli;
pub mod data;
pub mod engine;
pub mod network;
pub mod security;
pub mod utils;
pub mod metrics;
pub mod node;

// Re-export commonly used types
pub use engine::{
    Engine, EngineConfig, EngineState,
    Scheduler,
    Task,
    QueueTask, SharedTaskQueue, QueueConfig, TaskPriority,
    TaskExecutor,
};
// TaskType and TaskStatus are in a submodule, so export them separately
pub use engine::task::{TaskType, TaskStatus};

pub use node::{
    NodeRegistry, NodeInfo, NodeStatus, HealthScorer,
    MembershipManager, NodeClassification
};

// Optional: Create a prelude module for common imports
pub mod prelude {
    pub use crate::{
        Engine, EngineConfig, EngineState,
        Task,
        QueueTask, TaskPriority,
        TaskExecutor,
    };
    pub use crate::engine::task::{TaskType, TaskStatus};

    // Re-export common dependencies if desired
    pub use uuid::Uuid;
    pub use std::time::Duration;
}

// Optional: Top-level convenience functions
impl Engine {
    /// Create a new engine with default configuration
    pub fn default() -> Self {
        Self::new(EngineConfig::defaults())  // Use defaults() not default()
    }
}

// Optional: Provide a quick start function
/// Creates and starts a new engine with default settings
pub async fn start_engine() -> anyhow::Result<Arc<Engine>> {
    let engine = Arc::new(Engine::default());
    let engine_clone = Arc::clone(&engine);

    tokio::spawn(async move {
        if let Err(e) = engine_clone.start().await {
            eprintln!("Engine failed to start: {}", e);
        }
    });

    Ok(engine)
}