use std::net::{IpAddr, SocketAddr};
use uuid::Uuid;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum NodeClassification {
    Local,
    Remote,
    Edge,
    Cloud,
    Unknown,
}

impl NodeClassification {
    pub fn is_local(&self) -> bool {
        matches!(self, NodeClassification::Local)
    }

    pub fn is_remote(&self) -> bool {
        matches!(self, NodeClassification::Remote | NodeClassification::Edge | NodeClassification::Cloud)
    }

    pub fn latency_factor(&self) -> f64 {
        match self {
            NodeClassification::Local => 1.0,
            NodeClassification::Remote => 1.5,
            NodeClassification::Edge => 2.0,
            NodeClassification::Cloud => 3.0,
            NodeClassification::Unknown => 10.0,
        }
    }
}

#[derive(Debug)]
pub struct Classifier {
    local_node_id: Uuid,
    local_network_ranges: Vec<(IpAddr, u8)>, // CIDR ranges
    datacenter_zones: Vec<String>,
}

impl Classifier {
    pub fn new(local_node_id: Uuid) -> Self {
        Self {
            local_node_id,
            local_network_ranges: Self::detect_local_ranges(),
            datacenter_zones: Vec::new(),
        }
    }

    /// Classify a node based on its information
    pub fn classify(&self, node_info: &crate::node::registry::NodeInfo) -> NodeClassification {
        // Check if it's the local node
        if node_info.id == self.local_node_id {
            return NodeClassification::Local;
        }

        // Check network proximity
        let network_class = self.classify_by_network(&node_info.address);

        // Check datacenter/zone information if available
        let dc_class = self.classify_by_datacenter(node_info);

        // Combine classifications
        self.combine_classifications(network_class, dc_class)
    }

    fn classify_by_network(&self, addr: &SocketAddr) -> NodeClassification {
        let ip = addr.ip();

        // Check if it's localhost
        if ip.is_loopback() {
            return NodeClassification::Remote; // Loopback but not local node
        }

        // Check if it's in local network ranges
        for (network_ip, prefix) in &self.local_network_ranges {
            if Self::is_in_cidr(&ip, *network_ip, *prefix) {
                return NodeClassification::Remote;
            }
        }

        // Check if it's a private IP (likely in same datacenter)
        if Self::is_private_ip(&ip) {
            return NodeClassification::Remote;
        }

        // Check if it's a public IP (likely cloud/edge)
        if Self::is_public_ip(&ip) {
            // Could be edge or cloud based on additional heuristics
            return NodeClassification::Cloud;
        }

        NodeClassification::Unknown
    }

    fn classify_by_datacenter(&self, node_info: &crate::node::registry::NodeInfo) -> NodeClassification {
        // Extract datacenter/zone from metadata
        if let Some(dc) = node_info.metadata.get("datacenter") {
            if let Some(dc_str) = dc.as_str() {
                if self.datacenter_zones.contains(&dc_str.to_string()) {
                    return NodeClassification::Remote;
                } else {
                    return NodeClassification::Cloud;
                }
            }
        }

        // Extract from hostname pattern
        if node_info.hostname.contains("edge-") {
            return NodeClassification::Edge;
        }

        if node_info.hostname.contains("cloud-") ||
            node_info.hostname.ends_with(".aws") ||
            node_info.hostname.ends_with(".gcp") ||
            node_info.hostname.ends_with(".azure") {
            return NodeClassification::Cloud;
        }

        NodeClassification::Unknown
    }

    fn combine_classifications(
        &self,
        network_class: NodeClassification,
        dc_class: NodeClassification
    ) -> NodeClassification {
        // Prefer more specific classification
        match (network_class, dc_class) {
            (NodeClassification::Unknown, specific) => specific,
            (specific, NodeClassification::Unknown) => specific,
            (a, b) if a == b => a,
            // If there's a conflict, use network classification as primary
            (network, _) => network,
        }
    }

    /// Detect local network ranges
    fn detect_local_ranges() -> Vec<(IpAddr, u8)> {
        let mut ranges = Vec::new();

        // Common private IP ranges
        ranges.push((IpAddr::V4("10.0.0.0".parse().unwrap()), 8));
        ranges.push((IpAddr::V4("172.16.0.0".parse().unwrap()), 12));
        ranges.push((IpAddr::V4("192.168.0.0".parse().unwrap()), 16));

        // IPv6 private ranges
        ranges.push((IpAddr::V6("fc00::".parse().unwrap()), 7));
        ranges.push((IpAddr::V6("fe80::".parse().unwrap()), 10));

        ranges
    }

    fn is_in_cidr(ip: &IpAddr, network: IpAddr, prefix: u8) -> bool {
        match (ip, network) {
            (IpAddr::V4(ip), IpAddr::V4(network)) => {
                let mask = !((1 << (32 - prefix)) - 1);
                (u32::from(*ip) & mask) == (u32::from(network) & mask)
            }
            (IpAddr::V6(ip), IpAddr::V6(network)) => {
                let mask = !((1u128 << (128 - prefix)) - 1);
                let ip_u128 = u128::from(*ip);
                let network_u128 = u128::from(network);
                (ip_u128 & mask) == (network_u128 & mask)
            }
            _ => false,
        }
    }

    fn is_private_ip(ip: &IpAddr) -> bool {
        match ip {
            IpAddr::V4(ipv4) => {
                ipv4.is_private() ||
                    ipv4.is_loopback() ||
                    ipv4.is_link_local()
            }
            IpAddr::V6(ipv6) => {
                ipv6.is_loopback() ||
                    (u128::from(*ipv6) & 0xffff_ffff_ffff_ffff_0000_0000_0000_0000) == 0xfc00_0000_0000_0000_0000_0000_0000_0000 || // Unique local
                    (u128::from(*ipv6) & 0xffff_ffff_ffff_ffff_ffff_ffff_ffff_ffc0) == 0xfe80_0000_0000_0000_0000_0000_0000_0000    // Link-local
            }
        }
    }

    fn is_public_ip(ip: &IpAddr) -> bool {
        !Self::is_private_ip(ip)
    }

    /// Update datacenter zones (for multi-DC deployments)
    pub fn update_datacenter_zones(&mut self, zones: Vec<String>) {
        self.datacenter_zones = zones;
    }

    /// Get recommended task affinity based on node classification
    pub fn get_task_affinity(&self, classification: NodeClassification) -> TaskAffinity {
        match classification {
            NodeClassification::Local => TaskAffinity {
                preferred: true,
                weight: 1.0,
                max_latency_ms: 0,
                cost_factor: 1.0,
            },
            NodeClassification::Remote => TaskAffinity {
                preferred: true,
                weight: 0.8,
                max_latency_ms: 10,
                cost_factor: 1.2,
            },
            NodeClassification::Edge => TaskAffinity {
                preferred: false,
                weight: 0.6,
                max_latency_ms: 50,
                cost_factor: 1.5,
            },
            NodeClassification::Cloud => TaskAffinity {
                preferred: false,
                weight: 0.4,
                max_latency_ms: 100,
                cost_factor: 2.0,
            },
            NodeClassification::Unknown => TaskAffinity {
                preferred: false,
                weight: 0.1,
                max_latency_ms: 1000,
                cost_factor: 5.0,
            },
        }
    }
}

/// Task affinity recommendations based on node classification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskAffinity {
    pub preferred: bool,
    pub weight: f64,          // Scheduling weight (0.0-1.0)
    pub max_latency_ms: u32,  // Maximum acceptable latency
    pub cost_factor: f64,     // Relative cost factor
}

/// Network topology information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkTopology {
    pub nodes: Vec<TopologyNode>,
    pub edges: Vec<TopologyEdge>,
    pub latency_matrix: Vec<Vec<u32>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyNode {
    pub node_id: Uuid,
    pub classification: NodeClassification,
    pub zone: String,
    pub coordinates: (f64, f64), // For visualization
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyEdge {
    pub from: Uuid,
    pub to: Uuid,
    pub latency_ms: u32,
    pub bandwidth_mbps: u32,
    pub cost: f64,
}