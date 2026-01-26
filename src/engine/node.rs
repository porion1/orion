#[derive(Debug, Clone)]
pub struct Node {
    pub id: String,
    pub address: String,
}

impl Node {
    pub fn new(id: &str, address: &str) -> Self {
        Self {
            id: id.to_string(),
            address: address.to_string(),
        }
    }
}
