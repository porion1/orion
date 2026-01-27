use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct EngineConfig {
    pub scheduler_tick_secs: u64,
    pub max_concurrent_tasks: usize,
    pub logging_level: Option<String>,
    pub persistence_path: Option<String>,
    pub metrics_port: u16,     // u16 already guarantees 0..=65535
    pub metrics_enabled: bool,
}

impl EngineConfig {
    /// Load config from YAML file
    pub fn from_yaml(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let contents = fs::read_to_string(path)?;
        let config: EngineConfig = serde_yaml::from_str(&contents)?;
        config.validate()?;
        Ok(config)
    }

    /// Load config from TOML file
    pub fn from_toml(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let contents = fs::read_to_string(path)?;
        let config: EngineConfig = toml::from_str(&contents)?;
        config.validate()?;
        Ok(config)
    }

    /// Default configuration
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

    /// Return persistence path, ensuring it exists
    pub fn get_persistence_path(&self) -> String {
        let path = self
            .persistence_path
            .as_deref()
            .unwrap_or("./orion-data");

        if !Path::new(path).exists() {
            if let Err(e) = fs::create_dir_all(path) {
                eprintln!(
                    "⚠️ Failed to create persistence directory '{}': {}",
                    path, e
                );
            }
        }

        path.to_string()
    }

    /// Should metrics be enabled
    pub fn should_enable_metrics(&self) -> bool {
        self.metrics_enabled
    }

    /// Get normalized logging level (lowercase)
    pub fn get_logging_level(&self) -> String {
        self.logging_level
            .clone()
            .unwrap_or_else(|| "info".to_string())
            .to_lowercase()
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
        if self.scheduler_tick_secs == 0 {
            return Err("scheduler_tick_secs must be > 0".into());
        }

        if self.max_concurrent_tasks == 0 {
            return Err("max_concurrent_tasks must be > 0".into());
        }

        // u16 already ensures 0..=65535
        if self.metrics_port == 0 {
            return Err("metrics_port cannot be 0".into());
        }

        if let Some(level) = &self.logging_level {
            let level = level.to_lowercase();
            let allowed = ["trace", "debug", "info", "warn", "error"];
            if !allowed.contains(&level.as_str()) {
                return Err(format!(
                    "invalid logging_level '{}'. Must be one of: {:?}",
                    level, allowed
                )
                    .into());
            }
        }

        Ok(())
    }
}
