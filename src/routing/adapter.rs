// src/routing/adapter.rs

use crate::routing::{Task as RoutingTask, RoutingHints, NodeMetrics, NodeCapacity, DistributionMetadata, ResourceRequirements};
use std::collections::HashMap;
use std::time::SystemTime;

/// Adapter to convert between Orion task system and routing system
#[derive(Debug, Clone)]
pub struct TaskAdapter;

impl TaskAdapter {
    /// Convert Orion Task to Routing Task for load balancer
    pub fn to_routing_task(orion_task: &crate::routing::Task) -> RoutingTask {
        let routing_hints = Self::extract_routing_hints(orion_task);

        RoutingTask {
            id: orion_task.id.to_string(),
            name: orion_task.name.clone(),
            task_type: Self::extract_task_type(orion_task),
            priority: Self::calculate_priority(orion_task),
            payload: orion_task.payload.clone(),
            metadata: orion_task.metadata.clone(),
            distribution: orion_task.distribution.clone(),
            routing_hints,  // Added this field
        }
    }

    /// Extract routing hints from Orion task distribution metadata
    fn extract_routing_hints(orion_task: &crate::routing::Task) -> RoutingHints {
        let mut hints = RoutingHints::default();
        let dist = &orion_task.distribution;

        // Set preferred node if specified
        if let Some(pref_node) = &dist.preferred_node_id {  // Use reference
            hints.preferred_node = Some(pref_node.clone());  // Clone instead of to_string
        }

        // Extract region/zone from tags
        for tag in &dist.tags {
            if tag.starts_with("region=") || tag.starts_with("zone=") {
                hints.preferred_region = Some(tag.split('=').nth(1).unwrap_or("").to_string());
                break;
            }
        }

        // Extract required capabilities from resource requirements
        if let Some(ref req) = dist.resource_requirements {
            hints.required_capabilities = req.required_task_types.clone();

            // Add GPU capability if needed
            if req.needs_gpu {
                if !hints.required_capabilities.contains(&"gpu".to_string()) {
                    hints.required_capabilities.push("gpu".to_string());
                }
            }

            // Add SSD capability if needed
            if req.needs_ssd {
                if !hints.required_capabilities.contains(&"ssd".to_string()) {
                    hints.required_capabilities.push("ssd".to_string());
                }
            }
        }

        // Set deadline if task has estimated duration
        if let Some(ref req) = dist.resource_requirements {
            if req.estimated_duration_secs > 0.0 {
                let duration = std::time::Duration::from_secs_f64(req.estimated_duration_secs);
                hints.deadline = Some(SystemTime::now() + duration);
            }
        }

        hints
    }

    /// Extract task type for routing purposes
    fn extract_task_type(orion_task: &crate::routing::Task) -> String {
        // Use distribution tags as task type, or fall back to name
        if !orion_task.distribution.tags.is_empty() {
            // Find the first non-location tag
            for tag in &orion_task.distribution.tags {
                if !tag.starts_with("region=") && !tag.starts_with("zone=") {
                    return tag.clone();
                }
            }
        }

        // Fall back to task name with prefix
        format!("orion_{}", orion_task.name)
    }

    /// Calculate priority based on task characteristics
    fn calculate_priority(orion_task: &crate::routing::Task) -> u8 {
        // Higher priority for:
        // - Low latency requirements
        // - High resource requirements
        // - Remote execution

        let mut priority = 5; // Default medium priority

        // Lower priority for high latency tolerance
        if let Some(max_latency) = orion_task.distribution.max_latency_ms {
            if max_latency < 10 {
                priority = 1; // Highest priority
            } else if max_latency < 100 {
                priority = 3; // High priority
            }
        }

        // Higher priority for resource-intensive tasks
        if let Some(ref req) = orion_task.distribution.resource_requirements {
            if req.min_cpu_cores > 4.0 || req.min_memory_mb > 8192.0 {  // Fixed: use 8192.0
                priority = priority.min(2); // Higher priority
            }
        }

        // Lower priority for tasks forced to local (can wait)
        if orion_task.distribution.force_local {
            priority = 7;
        }

        // Higher priority for remote tasks (need distribution)
        if orion_task.distribution.remote_node_id.is_some() {
            priority = 2;
        }

        priority.clamp(1, 10)
    }

    /// Convert Orion distribution metadata to node tags for routing
    pub fn to_node_tags(distribution: &DistributionMetadata) -> HashMap<String, String> {
        let mut tags = HashMap::new();

        // Add task type capabilities
        if let Some(ref req) = distribution.resource_requirements {
            if !req.required_task_types.is_empty() {
                tags.insert("supported_task_types".to_string(),
                            req.required_task_types.join(","));
            }
            if req.needs_gpu {
                tags.insert("gpu".to_string(), "true".to_string());
            }
            if req.needs_ssd {
                tags.insert("ssd".to_string(), "true".to_string());
            }
        }

        // Add region/zone from tags
        for tag in &distribution.tags {
            if tag.starts_with("region=") {
                tags.insert("region".to_string(), tag.split('=').nth(1).unwrap_or("").to_string());
            } else if tag.starts_with("zone=") {
                tags.insert("zone".to_string(), tag.split('=').nth(1).unwrap_or("").to_string());
            }
        }

        // Add affinity rules
        if let Some(ref affinity) = distribution.affinity_rules {
            tags.insert("affinity".to_string(), affinity.clone());
        }

        tags
    }

    /// Convert Orion resource requirements to NodeCapacity
    pub fn to_node_capacity(req: &ResourceRequirements) -> NodeCapacity {
        NodeCapacity {
            max_concurrent_tasks: ((req.min_cpu_cores * 10.0) as usize).max(1),
            max_queue_length: 100, // Default queue length
            supported_task_types: req.required_task_types.clone(),
            regions: vec![], // Will be populated from tags if available
        }
    }

    /// Create node metrics from Orion task distribution preferences
    pub fn create_preferred_node_metrics(
        distribution: &DistributionMetadata,
        base_metrics: NodeMetrics,
    ) -> NodeMetrics {
        let mut metrics = base_metrics;

        // Apply distribution preferences to metrics
        if let Some(min_health) = distribution.min_health_score {
            // Health score influences the is_healthy flag
            metrics.is_healthy = metrics.is_healthy && (metrics.cpu_usage < 0.9)
                && (metrics.memory_usage < 0.9)
                && ((min_health as f32 / 100.0) < 0.8);
        }

        // Add tags from distribution metadata
        metrics.tags.extend(Self::to_node_tags(distribution));

        metrics
    }
}

/// Integration layer for task routing
#[derive(Debug, Clone)]
pub struct TaskRouterIntegrator {
    adapter: TaskAdapter,
}

impl TaskRouterIntegrator {
    pub fn new() -> Self {
        Self {
            adapter: TaskAdapter,
        }
    }

    /// Route an Orion task using the load balancer
    pub async fn route_orion_task(
        &self,
        orion_task: &crate::routing::Task,
        available_nodes: &[crate::routing::Node],
        router: &crate::routing::Router,
    ) -> Result<String, crate::routing::Error> {
        // Convert Orion task to routing task
        let routing_task = TaskAdapter::to_routing_task(orion_task);

        // Convert Orion nodes to routing nodes (create a compatibility layer)
        let routing_nodes: Vec<_> = available_nodes.iter()
            .map(|node| Self::convert_orion_node_to_routing_node(node))
            .collect();

        // For now, just return the first node ID
        if let Some(node) = routing_nodes.first() {
            Ok(node.id.clone())
        } else {
            Err(crate::routing::Error::msg("No nodes available"))
        }
    }

    /// Convert Orion node to routing node
    fn convert_orion_node_to_routing_node(
        orion_node: &crate::routing::Node,
    ) -> crate::routing::Node {
        orion_node.clone() // Just clone for now
    }
}