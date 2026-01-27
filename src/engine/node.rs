use serde::{Serialize, Deserialize};
use uuid::Uuid;
use std::net::SocketAddr;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Node {
    pub id: Uuid,
    pub address: SocketAddr,
}

impl Node {
    /// Create a new node with a random UUID
    pub fn new(address: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let socket_addr: SocketAddr = address.parse()?;
        Ok(Self {
            id: Uuid::new_v4(),
            address: socket_addr,
        })
    }

    /// Create a node with a given UUID
    pub fn with_id(id: Uuid, address: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let socket_addr: SocketAddr = address.parse()?;
        Ok(Self { id, address: socket_addr })
    }
}
