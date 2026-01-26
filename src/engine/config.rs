use serde::{Deserialize, Serialize};
use std::fs;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct EngineConfig {
    pub scheduler_tick_secs: u64,
    pub max_concurrent_tasks: usize,
    pub logging_level: Option<String>,
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
        }
    }
}