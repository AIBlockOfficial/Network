use serde::Deserialize;
use std::net::SocketAddr;

/// Configuration option for a compute node
#[derive(Debug, Clone, Deserialize)]
pub struct NodeSpec {
    pub address: SocketAddr,
}

/// Configuration option for a compute node
#[derive(Debug, Clone, Deserialize)]
pub struct ComputeNodeConfig {
    /// Index of the current node in compute_addrs
    pub compute_node_idx: usize,
    /// All compute nodes addresses: leader is first
    pub compute_nodes: Vec<NodeSpec>,
    /// All storage nodes addresses: only use first
    pub storage_nodes: Vec<NodeSpec>,
}

/// Configuration option for a storage node
#[derive(Debug, Clone, Deserialize)]
pub struct StorageNodeConfig {
    /// Index of the current node in compute_addrs
    pub storage_node_idx: usize,
    /// Use test database if 0
    pub use_live_db: usize,
    /// All compute nodes addresses: leader is first
    pub compute_nodes: Vec<NodeSpec>,
    /// All storage nodes addresses: only use first
    pub storage_nodes: Vec<NodeSpec>,
}
