//! This module provides a variety of utility functions to setup a test network,
//! to send a receive requests & responses, and generally to test the behavior and
//! correctness of the compute, miner, & storage modules.

use crate::compute::ComputeNode;
use crate::configurations::{
    ComputeNodeConfig, MinerNodeConfig, NodeSpec, StorageNodeConfig, UserNodeConfig,
};
use crate::miner::MinerNode;
use crate::storage::StorageNode;
use crate::user::UserNode;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::sync::Barrier;
use tokio::sync::Mutex;
use tracing::info;
use tracing::info_span;
use tracing_futures::Instrument;

pub type ArcMinerNode = Arc<Mutex<MinerNode>>;
pub type ArcComputeNode = Arc<Mutex<ComputeNode>>;
pub type ArcStorageNode = Arc<Mutex<StorageNode>>;
pub type ArcUserNode = Arc<Mutex<UserNode>>;

/// Represents a virtual configurable Zenotta network.
pub struct Network {
    miner_nodes: BTreeMap<String, ArcMinerNode>,
    compute_nodes: BTreeMap<String, ArcComputeNode>,
    storage_nodes: BTreeMap<String, ArcStorageNode>,
    user_nodes: BTreeMap<String, ArcUserNode>,
}

/// Represents a virtual network configuration.
/// Can be created using the builder or deserialized from JSON.
#[derive(Serialize, Deserialize)]
pub struct NetworkConfig {
    pub initial_port: u16,
    pub miner_nodes: Vec<String>,
    pub compute_nodes: Vec<String>,
    pub storage_nodes: Vec<String>,
    pub user_nodes: Vec<String>,
}

pub struct NetworkInstanceInfo {
    pub miner_nodes: Vec<NodeSpec>,
    pub compute_nodes: Vec<NodeSpec>,
    pub storage_nodes: Vec<NodeSpec>,
    pub user_nodes: Vec<NodeSpec>,
}

impl Network {
    pub async fn create_from_config(config: &NetworkConfig) -> Self {
        let info = Self::init_instance_info(config);
        let miner_nodes = Self::init_miners(&config, &info).await;
        let compute_nodes = Self::init_compute(&config, &info).await;
        let storage_nodes = Self::init_storage(&config, &info).await;
        let user_nodes = Self::init_users(&config, &info).await;

        Self {
            miner_nodes,
            compute_nodes,
            storage_nodes,
            user_nodes,
        }
    }

    pub async fn spawn_raft_loops(self) -> Self {
        let barrier = Arc::new(Barrier::new(self.compute_nodes.len()));
        for (name, c) in &self.compute_nodes {
            let c = c.lock().await;
            let barrier = barrier.clone();
            let name = name.clone();
            let address = c.address();
            let connect_all = c.connect_to_computes();
            let raft_loop = c.raft_loop();
            let peer_span = info_span!("compute_node", ?name, ?address);
            tokio::spawn(
                async move {
                    // Need to connect first so Raft messages can be sent.
                    info!("Start connect to peers");
                    let result = connect_all.await;
                    info!(?result, "Peer connect complete");
                    barrier.wait().await;
                    info!("All Peer connected: start raft");
                    raft_loop.await;
                    info!("raft complete");
                }
                .instrument(peer_span),
            );
        }

        self
    }

    fn init_instance_info(config: &NetworkConfig) -> NetworkInstanceInfo {
        let ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
        let next_port = config.initial_port;

        let (next_port, miner_nodes) = Self::node_specs(ip, next_port, config.miner_nodes.len());
        let (next_port, compute_nodes) =
            Self::node_specs(ip, next_port, config.compute_nodes.len());
        let (next_port, storage_nodes) =
            Self::node_specs(ip, next_port, config.storage_nodes.len());
        let (_next_port, user_nodes) = Self::node_specs(ip, next_port, config.user_nodes.len());

        NetworkInstanceInfo {
            miner_nodes,
            compute_nodes,
            storage_nodes,
            user_nodes,
        }
    }

    fn node_specs(ip: IpAddr, initial_port: u16, node_len: usize) -> (u16, Vec<NodeSpec>) {
        (
            initial_port + node_len as u16,
            (0..node_len)
                .map(|idx| NodeSpec {
                    address: SocketAddr::new(ip, initial_port + idx as u16),
                })
                .collect(),
        )
    }

    async fn init_miners(
        config: &NetworkConfig,
        info: &NetworkInstanceInfo,
    ) -> BTreeMap<String, ArcMinerNode> {
        let mut map = BTreeMap::new();

        for (idx, name) in config.miner_nodes.iter().enumerate() {
            let miner_config = MinerNodeConfig {
                miner_node_idx: idx,
                miner_compute_node_idx: 0,
                compute_nodes: info.compute_nodes.clone(),
                storage_nodes: info.storage_nodes.clone(),
                miner_nodes: info.miner_nodes.clone(),
                user_nodes: info.user_nodes.clone(),
            };
            map.insert(
                name.clone(),
                Arc::new(Mutex::new(MinerNode::new(miner_config).await.unwrap())),
            );
        }

        map
    }

    async fn init_storage(
        config: &NetworkConfig,
        info: &NetworkInstanceInfo,
    ) -> BTreeMap<String, ArcStorageNode> {
        let mut map = BTreeMap::new();

        for (idx, name) in config.storage_nodes.iter().enumerate() {
            let storage_config = StorageNodeConfig {
                storage_node_idx: idx,
                use_live_db: 0,
                compute_nodes: info.compute_nodes.clone(),
                storage_nodes: info.storage_nodes.clone(),
                user_nodes: info.user_nodes.clone(),
            };
            map.insert(
                name.clone(),
                Arc::new(Mutex::new(StorageNode::new(storage_config).await.unwrap())),
            );
        }

        map
    }

    async fn init_compute(
        config: &NetworkConfig,
        info: &NetworkInstanceInfo,
    ) -> BTreeMap<String, ArcComputeNode> {
        let mut map = BTreeMap::new();

        for (idx, name) in config.compute_nodes.iter().enumerate() {
            let compute_config = ComputeNodeConfig {
                compute_node_idx: idx,
                compute_nodes: info.compute_nodes.clone(),
                storage_nodes: info.storage_nodes.clone(),
                user_nodes: info.user_nodes.clone(),
            };
            map.insert(
                name.clone(),
                Arc::new(Mutex::new(ComputeNode::new(compute_config).await.unwrap())),
            );
        }

        map
    }

    async fn init_users(
        config: &NetworkConfig,
        info: &NetworkInstanceInfo,
    ) -> BTreeMap<String, ArcUserNode> {
        let mut map = BTreeMap::new();

        for (idx, name) in config.user_nodes.iter().enumerate() {
            let user_config = UserNodeConfig {
                user_node_idx: idx,
                user_compute_node_idx: 0,
                peer_user_node_idx: 0,
                compute_nodes: info.compute_nodes.clone(),
                storage_nodes: info.storage_nodes.clone(),
                miner_nodes: info.miner_nodes.clone(),
                user_nodes: info.user_nodes.clone(),
            };

            map.insert(
                name.clone(),
                Arc::new(Mutex::new(UserNode::new(user_config).await.unwrap())),
            );
        }

        map
    }

    pub fn miner(&mut self, name: &str) -> Option<&mut ArcMinerNode> {
        self.miner_nodes.get_mut(name)
    }

    pub fn miners_iter_mut(&mut self) -> impl Iterator<Item = &mut ArcMinerNode> {
        self.miner_nodes.values_mut()
    }

    pub fn compute(&mut self, name: &str) -> Option<&mut ArcComputeNode> {
        self.compute_nodes.get_mut(name)
    }

    pub fn storage(&mut self, name: &str) -> Option<&mut ArcStorageNode> {
        self.storage_nodes.get_mut(name)
    }

    pub fn user(&mut self, name: &str) -> Option<&mut ArcUserNode> {
        self.user_nodes.get_mut(name)
    }

    pub async fn get_address(&mut self, name: &str) -> Option<SocketAddr> {
        if let Some(miner) = self.miner_nodes.get(name) {
            return Some(miner.lock().await.address());
        }
        if let Some(compute) = self.compute_nodes.get(name) {
            return Some(compute.lock().await.address());
        }
        if let Some(storage) = self.storage_nodes.get(name) {
            return Some(storage.lock().await.address());
        }
        if let Some(user) = self.user_nodes.get(name) {
            return Some(user.lock().await.address());
        }
        None
    }
}
