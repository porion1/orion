pub mod config;
pub mod node;
pub mod scheduler;
pub mod state;
pub mod task;

// Re-export main types
pub use config::EngineConfig;
pub use scheduler::Scheduler;
pub use state::EngineState;
pub use task::Task;

// Main Engine struct
use tokio::sync::watch;
use std::sync::Arc;

#[derive(Debug)]
pub struct Engine {
    pub scheduler: Arc<Scheduler>,
    pub config: EngineConfig,
    pub state: EngineState,
    shutdown_tx: watch::Sender<bool>,
}

impl Engine {
    pub fn new(config: EngineConfig) -> Self {
        let (shutdown_tx, _) = watch::channel(false);

        let scheduler = Scheduler::new();

        Self {
            scheduler,
            config,
            state: EngineState::Init,
            shutdown_tx,
        }
    }

    pub fn set_state(&mut self, new_state: EngineState) {
        println!("🔄 Engine state changed: {:?} → {:?}", self.state, new_state);
        self.state = new_state;
    }

    pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.set_state(EngineState::Running);
        println!("Engine starting with config: {:?}", self.config);

        // Get shutdown sender from scheduler
        let scheduler_shutdown_tx = self.scheduler.get_shutdown_sender();
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        // Wait for shutdown signal
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("Received shutdown signal, stopping Engine...");
            }
            _ = shutdown_rx.changed() => {
                println!("Received internal shutdown signal...");
            }
        }

        // Move to Draining state
        self.set_state(EngineState::Draining);

        // Notify scheduler to stop
        scheduler_shutdown_tx.send(true)?;
        self.shutdown_tx.send(true)?;

        // Give time for tasks to complete
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        // Move to Stopped state
        self.set_state(EngineState::Stopped);
        println!("Engine stopped gracefully.");

        Ok(())
    }

    pub async fn schedule_task(&self, task: Task) -> Result<uuid::Uuid, Box<dyn std::error::Error + Send + Sync>> {
        self.scheduler.schedule(task).await
    }
}