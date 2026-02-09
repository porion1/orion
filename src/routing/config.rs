//! Configuration for routing and load balancing

use serde::{Deserialize, Serialize};
//use std::time::Duration;

/// Router configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouterConfig {
    /// Default routing strategy
    pub default_strategy: String,

    /// Strategy-specific configurations
    pub round_robin: RoundRobinConfig,
    pub least_loaded: LeastLoadedConfig,
    pub latency_aware: LatencyAwareConfig,
    pub failover: FailoverConfig,
    pub hybrid: HybridConfig,

    /// Global routing settings
    pub enable_metrics: bool,
    pub metrics_collection_interval_secs: u64,
    pub node_selection_timeout_ms: u64,
    pub allow_strategy_override: bool,

    /// Performance tuning
    pub cache_ttl_secs: u64,
    pub cleanup_interval_secs: u64,
    pub max_concurrent_selections: usize,
}

/// Round-robin strategy configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoundRobinConfig {
    /// Whether to skip unhealthy nodes
    pub skip_unhealthy: bool,

    /// Whether to respect node capacity
    pub respect_capacity: bool,

    /// Node weights for weighted round-robin
    pub node_weights: Vec<NodeWeight>,
}

/// Least-loaded strategy configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeastLoadedConfig {
    /// Weight configuration for load calculation
    pub weights: LeastLoadedWeights,

    /// Minimum acceptable load score (0.0-1.0)
    pub min_load_threshold: f64,

    /// Maximum acceptable load score (0.0-1.0)
    pub max_load_threshold: f64,

    /// Whether to consider node affinity
    pub consider_affinity: bool,

    /// Load update interval in seconds
    pub load_update_interval_secs: u64,
}

/// Latency-aware strategy configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyAwareConfig {
    /// Maximum acceptable latency in milliseconds
    pub max_latency_ms: f64,

    /// Latency measurement window in seconds
    pub measurement_window_secs: u64,

    /// Whether to use weighted latency (considering node load)
    pub use_weighted_latency: bool,

    /// Fallback strategy when no nodes meet latency requirements
    pub fallback_strategy: String,

    /// Latency smoothing factor (0.0-1.0)
    pub smoothing_factor: f64,
}

/// Failover strategy configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailoverConfig {
    /// Primary nodes for failover strategy
    pub primary_nodes: Vec<String>,

    /// Backup nodes for failover strategy
    pub backup_nodes: Vec<String>,

    /// Maximum number of retries before giving up
    pub max_retries: u32,

    /// Retry backoff strategy
    pub retry_backoff_ms: u64,

    /// Maximum failures before blacklisting a node
    pub max_failures: u32,

    /// Blacklist timeout in seconds
    pub blacklist_timeout_secs: u32,

    /// Circuit breaker threshold
    pub circuit_breaker_threshold: u32,

    /// Whether to enable automatic failback
    pub enable_failback: bool,

    /// Failback check interval in seconds
    pub failback_check_interval_secs: u64,
}

/// Hybrid strategy configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HybridConfig {
    /// Strategy weights for hybrid approach
    pub strategy_weights: Vec<StrategyWeight>,

    /// Minimum confidence threshold (0.0-1.0)
    pub min_confidence: f64,

    /// Whether to dynamically adjust weights
    pub dynamic_weight_adjustment: bool,

    /// Weight adjustment interval in seconds
    pub weight_adjustment_interval_secs: u64,
}

/// Load calculation weights
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeastLoadedWeights {
    pub cpu_weight: f64,
    pub memory_weight: f64,
    pub queue_weight: f64,
    pub processing_weight: f64,
}

/// Node weight for weighted round-robin
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeWeight {
    pub node_id: String,
    pub weight: u32,
}

/// Strategy weight for hybrid approach
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyWeight {
    pub strategy: String,
    pub weight: f64,
}

impl Default for RouterConfig {
    fn default() -> Self {
        Self {
            default_strategy: "round_robin".to_string(),

            round_robin: RoundRobinConfig {
                skip_unhealthy: true,
                respect_capacity: true,
                node_weights: Vec::new(),
            },

            least_loaded: LeastLoadedConfig {
                weights: LeastLoadedWeights {
                    cpu_weight: 0.4,
                    memory_weight: 0.3,
                    queue_weight: 0.2,
                    processing_weight: 0.1,
                },
                min_load_threshold: 0.0,
                max_load_threshold: 0.9,
                consider_affinity: true,
                load_update_interval_secs: 5,
            },

            latency_aware: LatencyAwareConfig {
                max_latency_ms: 100.0,
                measurement_window_secs: 30,
                use_weighted_latency: true,
                fallback_strategy: "least_loaded".to_string(),
                smoothing_factor: 0.7,
            },

            failover: FailoverConfig {
                primary_nodes: Vec::new(),
                backup_nodes: Vec::new(),
                max_retries: 3,
                retry_backoff_ms: 1000,
                max_failures: 5,
                blacklist_timeout_secs: 300,
                circuit_breaker_threshold: 3,
                enable_failback: true,
                failback_check_interval_secs: 60,
            },

            hybrid: HybridConfig {
                strategy_weights: vec![
                    StrategyWeight {
                        strategy: "least_loaded".to_string(),
                        weight: 0.5,
                    },
                    StrategyWeight {
                        strategy: "latency_aware".to_string(),
                        weight: 0.3,
                    },
                    StrategyWeight {
                        strategy: "round_robin".to_string(),
                        weight: 0.2,
                    },
                ],
                min_confidence: 0.6,
                dynamic_weight_adjustment: true,
                weight_adjustment_interval_secs: 300,
            },

            enable_metrics: true,
            metrics_collection_interval_secs: 60,
            node_selection_timeout_ms: 5000,
            allow_strategy_override: true,

            cache_ttl_secs: 10,
            cleanup_interval_secs: 30,
            max_concurrent_selections: 100,
        }
    }
}

impl RouterConfig {
    /// Validate configuration
    pub fn validate(&self) -> Result<(), String> {
        // Validate strategy weights sum to ~1.0
        let total_weight: f64 = self.hybrid.strategy_weights.iter()
            .map(|w| w.weight)
            .sum();

        if (total_weight - 1.0).abs() > 0.01 {
            return Err("Hybrid strategy weights must sum to 1.0".to_string());
        }

        // Validate individual weights are positive
        for weight in &self.hybrid.strategy_weights {
            if weight.weight <= 0.0 {
                return Err(format!("Strategy weight for {} must be positive", weight.strategy));
            }
        }

        // Validate load calculation weights
        let load_weights = &self.least_loaded.weights;
        let load_total = load_weights.cpu_weight + load_weights.memory_weight +
            load_weights.queue_weight + load_weights.processing_weight;

        if (load_total - 1.0).abs() > 0.01 {
            return Err("Least loaded weights must sum to 1.0".to_string());
        }

        // Validate thresholds
        if self.least_loaded.min_load_threshold < 0.0 || self.least_loaded.min_load_threshold > 1.0 {
            return Err("Minimum load threshold must be between 0.0 and 1.0".to_string());
        }

        if self.least_loaded.max_load_threshold < 0.0 || self.least_loaded.max_load_threshold > 1.0 {
            return Err("Maximum load threshold must be between 0.0 and 1.0".to_string());
        }

        if self.least_loaded.min_load_threshold > self.least_loaded.max_load_threshold {
            return Err("Minimum load threshold cannot exceed maximum load threshold".to_string());
        }

        // Validate latency threshold
        if self.latency_aware.max_latency_ms <= 0.0 {
            return Err("Maximum latency must be positive".to_string());
        }

        // Validate smoothing factor
        if self.latency_aware.smoothing_factor < 0.0 || self.latency_aware.smoothing_factor > 1.0 {
            return Err("Smoothing factor must be between 0.0 and 1.0".to_string());
        }

        Ok(())
    }

    /// Get configuration for a specific strategy
    pub fn get_strategy_config(&self, strategy: &str) -> Option<StrategyConfig> {
        match strategy {
            "round_robin" => Some(StrategyConfig::RoundRobin(self.round_robin.clone())),
            "least_loaded" => Some(StrategyConfig::LeastLoaded(self.least_loaded.clone())),
            "latency_aware" => Some(StrategyConfig::LatencyAware(self.latency_aware.clone())),
            "failover" => Some(StrategyConfig::Failover(self.failover.clone())),
            "hybrid" => Some(StrategyConfig::Hybrid(self.hybrid.clone())),
            _ => None,
        }
    }
}

/// Enum representing different strategy configurations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StrategyConfig {
    RoundRobin(RoundRobinConfig),
    LeastLoaded(LeastLoadedConfig),
    LatencyAware(LatencyAwareConfig),
    Failover(FailoverConfig),
    Hybrid(HybridConfig),
}

/// Load balancer health check configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckConfig {
    pub enabled: bool,
    pub interval_secs: u64,
    pub timeout_secs: u64,
    pub success_threshold: u32,
    pub failure_threshold: u32,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_secs: 30,
            timeout_secs: 5,
            success_threshold: 2,
            failure_threshold: 3,
        }
    }
}