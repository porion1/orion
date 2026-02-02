use std::net::{SocketAddr, UdpSocket};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub struct HeartbeatMessage {
    pub node_id: Uuid,
    pub timestamp: SystemTime,
    pub load_average: f32,
    pub memory_usage: f64,
    pub disk_usage: f64,
    pub active_tasks: u32,
}

pub struct HeartbeatListener {
    registry: Arc<crate::node::registry::NodeRegistry>,
    socket: UdpSocket,
    buffer: [u8; 1024],
    running: Arc<std::sync::atomic::AtomicBool>,
}

impl HeartbeatListener {
    pub fn new(
        registry: Arc<crate::node::registry::NodeRegistry>,
        listen_addr: SocketAddr,
    ) -> Result<Self, crate::utils::error::OrionError> {
        let socket = UdpSocket::bind(listen_addr)
            .map_err(|e| crate::utils::error::OrionError::NetworkError(e.to_string()))?;

        socket.set_nonblocking(true)
            .map_err(|e| crate::utils::error::OrionError::NetworkError(e.to_string()))?;

        Ok(Self {
            registry,
            socket,
            buffer: [0; 1024],
            running: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        })
    }

    // Fix: Don't call start() on Arc<Self>, call it on &Self
    pub fn start(&self) -> thread::JoinHandle<()> {
        // Clone what we need
        let registry = self.registry.clone();
        let socket = self.socket.try_clone().expect("Failed to clone socket");
        let mut buffer = self.buffer;
        let running = self.running.clone();

        thread::spawn(move || {
            tracing::info!("Heartbeat listener started");

            while running.load(std::sync::atomic::Ordering::Relaxed) {
                match socket.recv_from(&mut buffer) {
                    Ok((size, src_addr)) => {
                        if let Ok(msg) = bincode::deserialize::<HeartbeatMessage>(&buffer[..size]) {
                            // Handle heartbeat
                            Self::handle_heartbeat_internal(&registry, msg, src_addr);
                        }
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(100));
                    }
                    Err(e) => {
                        tracing::error!("Failed to receive heartbeat: {}", e);
                    }
                }
            }

            tracing::info!("Heartbeat listener stopped");
        })
    }

    fn handle_heartbeat_internal(
        registry: &Arc<crate::node::registry::NodeRegistry>,
        msg: HeartbeatMessage,
        src_addr: SocketAddr
    ) {
        // Update or create node info
        let node_info = match registry.get(&msg.node_id) {
            Some(info) => {
                let mut updated = info.clone();
                updated.last_seen = SystemTime::now();
                updated.status = crate::node::registry::NodeStatus::Active;
                updated
            }
            None => {
                crate::node::registry::NodeInfo {
                    id: msg.node_id,
                    address: src_addr,
                    hostname: "unknown".to_string(),
                    capabilities: crate::node::registry::NodeCapabilities {
                        cpu_cores: 0,
                        memory_mb: 0,
                        storage_mb: 0,
                        max_concurrent_tasks: 10,
                        supported_task_types: vec![],
                    },
                    status: crate::node::registry::NodeStatus::Joining,
                    last_seen: SystemTime::now(),
                    metadata: serde_json::json!({}),
                    version: "unknown".to_string(),
                }
            }
        };

        let _ = registry.register(node_info);
    }

    pub fn stop(&self) {
        self.running.store(false, std::sync::atomic::Ordering::Relaxed);
    }
}

/// Heartbeat sender for local node
pub struct HeartbeatSender {
    registry: Arc<crate::node::registry::NodeRegistry>,
    target_addrs: Vec<SocketAddr>,
    interval: Duration,
    running: Arc<std::sync::atomic::AtomicBool>,
}

impl HeartbeatSender {
    pub fn new(
        registry: Arc<crate::node::registry::NodeRegistry>,
        target_addrs: Vec<SocketAddr>,
        interval_ms: u64,
    ) -> Self {
        Self {
            registry,
            target_addrs,
            interval: Duration::from_millis(interval_ms),
            running: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        }
    }

    pub fn start(self: Arc<Self>) -> thread::JoinHandle<()> {
        let running = self.running.clone();

        thread::spawn(move || {
            tracing::info!("Heartbeat sender started");

            while running.load(std::sync::atomic::Ordering::Relaxed) {
                // Send heartbeat to all known nodes
                if let Some(local_node) = self.registry.local_node() {
                    let heartbeat = HeartbeatMessage {
                        node_id: local_node.id,
                        timestamp: SystemTime::now(),
                        load_average: 0.0,
                        memory_usage: 0.0,
                        disk_usage: 0.0,
                        active_tasks: 0,
                    };

                    let msg = bincode::serialize(&heartbeat).unwrap();

                    for addr in &self.target_addrs {
                        let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
                        let _ = socket.send_to(&msg, addr);
                    }
                }

                thread::sleep(self.interval);
            }

            tracing::info!("Heartbeat sender stopped");
        })
    }

    pub fn stop(&self) {
        self.running.store(false, std::sync::atomic::Ordering::Relaxed);
    }
}