use serde::{Deserialize, Serialize};
use std::fs;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct EngineConfig {
    pub scheduler_tick_secs: u64,
    pub max_concurrent_tasks: usize,
    pub logging_level: Option<String>,
    pub persistence_path: Option<String>,
    pub metrics_port: u16,
    pub metrics_enabled: bool,
}

impl EngineConfig {
    pub fn from_yaml(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let contents = fs::read_to_string(path)?;
        let config: EngineConfig = serde_yaml::from_str(&contents)?;
        Ok(config)
    }

    pub fn from_toml(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let contents = fs::read_to_string(path)?;
        let config: EngineConfig = toml::from_str(&contents)?;
        Ok(config)
    }

    pub fn defaults() -> Self {
        Self {
            scheduler_tick_secs: 1,
            max_concurrent_tasks: 100,
            logging_level: Some("info".to_string()),
            persistence_path: Some("./orion-data".to_string()),
            metrics_port: 9000,
            metrics_enabled: true,
        }
    }

    pub fn get_persistence_path(&self) -> &str {
        self.persistence_path.as_deref().unwrap_or("./orion-data")
    }

    pub fn should_enable_metrics(&self) -> bool {
        self.metrics_enabled
    }
}