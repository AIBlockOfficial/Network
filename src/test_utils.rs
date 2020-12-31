//! This module provides a variety of utility functions to setup a test network,
//! to send a receive requests & responses, and generally to test the behavior and
//! correctness of the compute, miner, & storage modules.

use crate::compute::ComputeNode;
use crate::configurations::{
    ComputeNodeConfig, DbMode, MinerNodeConfig, NodeSpec, StorageNodeConfig, UserNodeConfig,
};
use crate::miner::MinerNode;
use crate::storage::StorageNode;
use crate::user::UserNode;
use futures::future::join_all;
use naom::primitives::transaction::Transaction;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::error_span;
use tracing::info;
use tracing_futures::Instrument;

#[cfg(not(debug_assertions))] // Release
const TEST_DURATION_DIVIDER: usize = 10;

#[cfg(debug_assertions)] // Debug
const TEST_DURATION_DIVIDER: usize = 1;

pub type ArcMinerNode = Arc<Mutex<MinerNode>>;
pub type ArcComputeNode = Arc<Mutex<ComputeNode>>;
pub type ArcStorageNode = Arc<Mutex<StorageNode>>;
pub type ArcUserNode = Arc<Mutex<UserNode>>;

/// Represents a virtual configurable Zenotta network.
pub struct Network {
    pub config: NetworkConfig,
    miner_nodes: BTreeMap<String, ArcMinerNode>,
    compute_nodes: BTreeMap<String, ArcComputeNode>,
    storage_nodes: BTreeMap<String, ArcStorageNode>,
    user_nodes: BTreeMap<String, ArcUserNode>,
    raft_loop_handles: Vec<JoinHandle<()>>,
}

/// Represents a virtual network configuration.
/// Can be created using the builder or deserialized from JSON.
#[derive(Clone, Serialize, Deserialize)]
pub struct NetworkConfig {
    pub initial_port: u16,
    pub compute_raft: bool,
    pub storage_raft: bool,
    pub in_memory_db: bool,
    pub compute_seed_utxo: Vec<String>,
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
            config: config.clone(),
            miner_nodes,
            compute_nodes,
            storage_nodes,
            user_nodes,
            raft_loop_handles: Vec::new(),
        }
        .spawn_raft_loops()
        .await
    }

    async fn spawn_raft_loops(mut self) -> Self {
        if self.config.compute_raft {
            // Need to connect first so Raft messages can be sent.
            info!("Start connect to peers");
            for (name, node) in &self.compute_nodes {
                let node = node.lock().await;
                node.connect_to_raft_peers().await;
                info!(?name, "Peer connect complete");
            }
            info!("Peers connect complete");

            for (name, node) in &self.compute_nodes {
                let node = node.lock().await;
                let peer_span = error_span!("compute_node", ?name, addr = ?node.address());
                let raft_loop = node.raft_loop();
                self.raft_loop_handles.push(tokio::spawn(
                    async move {
                        info!("Start raft");
                        raft_loop.await;
                        info!("raft complete");
                    }
                    .instrument(peer_span),
                ));
            }
        }

        if self.config.storage_raft {
            // Need to connect first so Raft messages can be sent.
            info!("Start connect to peers");
            for (name, node) in &self.storage_nodes {
                let node = node.lock().await;
                node.connect_to_raft_peers().await;
                info!(?name, "Peer connect complete");
            }
            info!("Peers connect complete");

            for (name, node) in &self.storage_nodes {
                let node = node.lock().await;
                let peer_span = error_span!("storage_node", ?name, addr = ?node.address());
                let raft_loop = node.raft_loop();
                self.raft_loop_handles.push(tokio::spawn(
                    async move {
                        info!("Start raft");
                        raft_loop.await;
                        info!("raft complete");
                    }
                    .instrument(peer_span),
                ));
            }
        }

        self
    }

    pub async fn close_raft_loops_and_drop(self) {
        if self.config.compute_raft {
            info!("Close compute raft");
            for node in self.compute_nodes.values() {
                node.lock().await.close_raft_loop().await;
            }
        }

        if self.config.storage_raft {
            info!("Close storage raft");
            for node in self.storage_nodes.values() {
                node.lock().await.close_raft_loop().await;
            }
        }

        join_all(self.raft_loop_handles).await;
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
            let miner_db_mode = if config.in_memory_db {
                DbMode::InMemory
            } else {
                DbMode::Test(info.miner_nodes[idx].address.port() as usize)
            };
            let miner_config = MinerNodeConfig {
                miner_node_idx: idx,
                miner_db_mode,
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
            let storage_raft = if config.storage_raft { 1 } else { 0 };
            let storage_db_mode = if config.in_memory_db {
                DbMode::InMemory
            } else {
                DbMode::Test(info.storage_nodes[idx].address.port() as usize)
            };
            let storage_config = StorageNodeConfig {
                storage_node_idx: idx,
                storage_db_mode,
                compute_nodes: info.compute_nodes.clone(),
                storage_nodes: info.storage_nodes.clone(),
                user_nodes: info.user_nodes.clone(),
                storage_raft,
                storage_raft_tick_timeout: 200 / TEST_DURATION_DIVIDER,
                storage_block_timeout: 1000 / TEST_DURATION_DIVIDER,
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
            let compute_raft = if config.compute_raft { 1 } else { 0 };
            let compute_config = ComputeNodeConfig {
                compute_raft,
                compute_node_idx: idx,
                compute_nodes: info.compute_nodes.clone(),
                storage_nodes: info.storage_nodes.clone(),
                user_nodes: info.user_nodes.clone(),
                compute_raft_tick_timeout: 200 / TEST_DURATION_DIVIDER,
                compute_transaction_timeout: 100 / TEST_DURATION_DIVIDER,
                compute_seed_utxo: config.compute_seed_utxo.clone(),
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

    pub fn get_position(&mut self, name: &str) -> Option<usize> {
        let is_name = |n: &String| n.as_str() == name;
        if let Some(miner) = self.config.miner_nodes.iter().position(is_name) {
            return Some(miner);
        }
        if let Some(compute) = self.config.compute_nodes.iter().position(is_name) {
            return Some(compute);
        }
        if let Some(storage) = self.config.storage_nodes.iter().position(is_name) {
            return Some(storage);
        }
        if let Some(user) = self.config.user_nodes.iter().position(is_name) {
            return Some(user);
        }
        None
    }

    pub fn collect_initial_uxto_set(&self) -> BTreeMap<String, Transaction> {
        self.config
            .compute_seed_utxo
            .iter()
            .map(|h| (h.clone(), Transaction::new()))
            .collect()
    }
}
