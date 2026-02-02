use serde::{Deserialize, Serialize};
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("Failed to read config file: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Failed to parse config: {0}")]
    ParseError(String),
    #[error("Invalid configuration: {0}")]
    ValidationError(String),
}

/// NEW: Node Manager configuration (optional)
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ClusterConfig {
    #[serde(default)]
    pub enabled: bool,

    #[serde(default = "default_node_listen_addr")]
    pub node_listen_addr: SocketAddr,

    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval_ms: u64,

    #[serde(default)]
    pub cluster_peers: Vec<SocketAddr>,

    #[serde(default = "default_node_persistence_path")]
    pub node_persistence_path: Option<String>,

    #[serde(default)]
    pub node_id: Option<Uuid>,

    #[serde(default = "default_min_health_score")]
    pub min_health_score: f64,

    #[serde(default = "default_auto_discovery")]
    pub auto_discovery: bool,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            node_listen_addr: default_node_listen_addr(),
            heartbeat_interval_ms: default_heartbeat_interval(),
            cluster_peers: Vec::new(),
            node_persistence_path: default_node_persistence_path(),
            node_id: None,
            min_health_score: default_min_health_score(),
            auto_discovery: default_auto_discovery(),
        }
    }
}

// Default value functions for serde
fn default_node_listen_addr() -> SocketAddr {
    "0.0.0.0:9090".parse().unwrap()
}

fn default_heartbeat_interval() -> u64 {
    5000
}

fn default_node_persistence_path() -> Option<String> {
    Some("./orion-nodes".to_string())
}

fn default_min_health_score() -> f64 {
    70.0
}

fn default_auto_discovery() -> bool {
    true
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct EngineConfig {
    pub scheduler_tick_secs: u64,
    pub max_concurrent_tasks: usize,
    pub logging_level: Option<String>,
    pub persistence_path: Option<String>,
    pub metrics_port: u16,
    pub metrics_enabled: bool,

    /// NEW: Optional cluster configuration
    /// If not provided, cluster mode is disabled
    #[serde(default)]
    pub cluster_config: Option<ClusterConfig>,
}

impl EngineConfig {
    /// Load config from YAML file
    pub fn from_yaml(path: &str) -> Result<Self, ConfigError> {
        let contents = fs::read_to_string(path)?;
        let config: EngineConfig = serde_yaml::from_str(&contents)
            .map_err(|e| ConfigError::ParseError(format!("YAML parsing error: {}", e)))?;
        config.validate()?;
        Ok(config)
    }

    /// Load config from TOML file
    pub fn from_toml(path: &str) -> Result<Self, ConfigError> {
        let contents = fs::read_to_string(path)?;
        let config: EngineConfig = toml::from_str(&contents)
            .map_err(|e| ConfigError::ParseError(format!("TOML parsing error: {}", e)))?;
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

            // Cluster mode disabled by default
            cluster_config: None,
        }
    }

    /// Check if cluster mode is enabled
    pub fn is_cluster_enabled(&self) -> bool {
        self.cluster_config.as_ref()
            .map(|c| c.enabled)
            .unwrap_or(false)
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

    /// Return node persistence path, ensuring it exists
    pub fn get_node_persistence_path(&self) -> Option<String> {
        self.cluster_config.as_ref()
            .and_then(|c| c.node_persistence_path.clone())
            .map(|path| {
                if !Path::new(&path).exists() {
                    if let Err(e) = fs::create_dir_all(&path) {
                        eprintln!(
                            "⚠️ Failed to create node persistence directory '{}': {}",
                            path, e
                        );
                    }
                }
                path
            })
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
    pub fn validate(&self) -> Result<(), ConfigError> {
        // Validate core engine config
        if self.scheduler_tick_secs == 0 {
            return Err(ConfigError::ValidationError(
                "scheduler_tick_secs must be > 0".into(),
            ));
        }

        if self.max_concurrent_tasks == 0 {
            return Err(ConfigError::ValidationError(
                "max_concurrent_tasks must be > 0".into(),
            ));
        }

        // u16 already ensures 0..=65535
        if self.metrics_port == 0 {
            return Err(ConfigError::ValidationError(
                "metrics_port cannot be 0".into(),
            ));
        }

        if let Some(level) = &self.logging_level {
            let level = level.to_lowercase();
            let allowed = ["trace", "debug", "info", "warn", "error"];
            if !allowed.contains(&level.as_str()) {
                return Err(ConfigError::ValidationError(format!(
                    "invalid logging_level '{}'. Must be one of: {:?}",
                    level, allowed
                )));
            }
        }

        // Validate cluster config if enabled
        if let Some(cluster_config) = &self.cluster_config {
            if cluster_config.enabled {
                if cluster_config.heartbeat_interval_ms < 100 {
                    return Err(ConfigError::ValidationError(
                        "heartbeat_interval_ms must be >= 100ms".into(),
                    ));
                }

                if cluster_config.min_health_score < 0.0 || cluster_config.min_health_score > 100.0 {
                    return Err(ConfigError::ValidationError(
                        "min_health_score must be between 0.0 and 100.0".into(),
                    ));
                }
            }
        }

        Ok(())
    }
}

// Add Default trait implementation for convenience
impl Default for EngineConfig {
    fn default() -> Self {
        Self::defaults()
    }
}