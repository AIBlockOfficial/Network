//! This module provides a variety of utility functions to setup a test network,
//! to send a receive requests & responses, and generally to test the behavior and
//! correctness of the compute, miner, & storage modules.

use crate::compute::ComputeNode;
use crate::configurations::{
    ComputeNodeConfig, DbMode, MinerNodeConfig, NodeSpec, StorageNodeConfig, UserNodeConfig,
    UtxoSetSpec, WalletTxSpec,
};
use crate::miner::MinerNode;
use crate::storage::StorageNode;
use crate::user::UserNode;
use crate::utils::make_utxo_set_from_seed;
use futures::future::join_all;
use naom::primitives::transaction::Transaction;
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
#[derive(Clone)]
pub struct NetworkConfig {
    pub initial_port: u16,
    pub compute_raft: bool,
    pub storage_raft: bool,
    pub in_memory_db: bool,
    pub compute_partition_full_size: usize,
    pub compute_minimum_miner_pool_len: usize,
    pub compute_seed_utxo: UtxoSetSpec,
    pub user_wallet_seeds: Vec<Vec<WalletTxSpec>>,
    pub miner_nodes: Vec<String>,
    pub compute_nodes: Vec<String>,
    pub storage_nodes: Vec<String>,
    pub user_nodes: Vec<String>,
    pub compute_to_miner_mapping: BTreeMap<String, Vec<String>>,
}

pub struct NetworkInstanceInfo {
    pub miner_nodes: Vec<NodeSpec>,
    pub compute_nodes: Vec<NodeSpec>,
    pub storage_nodes: Vec<NodeSpec>,
    pub user_nodes: Vec<NodeSpec>,
}

impl Network {
    ///Creates a Network instance using a config object
    ///
    /// ###Arguments
    ///
    /// * `config` - Holds the values to instanciate a Network object
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

    ///Creates and tests rafts
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

    ///Completes and ends raft loops.
    pub async fn close_raft_loops_and_drop(mut self) {
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

        join_all(self.raft_loop_handles.drain(..)).await;
        self.stop_listening_and_drop().await;
    }

    ///Creates a NetworkInstanceInfo object with config object values.
    ///
    /// ### Arguments
    ///
    /// * `config` - &NetworkConfig object containing parameters for the NetworkInstanceInfo object creation.
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

    ///Returns a u16, of the initial_port and node length, and a Vec of node specs
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

    pub async fn stop_listening_and_drop(self) {
        info!("Stop listening user_nodes");
        for node in self.user_nodes.values() {
            join_all(node.lock().await.stop_listening_loop().await).await;
        }

        info!("Stop listening storage_nodes");
        for node in self.storage_nodes.values() {
            join_all(node.lock().await.stop_listening_loop().await).await;
        }

        info!("Stop listening compute_nodes");
        for node in self.compute_nodes.values() {
            join_all(node.lock().await.stop_listening_loop().await).await;
        }

        info!("Stop listening miner_nodes");
        for node in self.miner_nodes.values() {
            join_all(node.lock().await.stop_listening_loop().await).await;
        }
    }

    ///Clones storage nodes, compute nodes, miner nodes and user nodes. The miner nodes are initialised and a map is returned.
    ///
    /// ### Arguments
    ///
    /// * `config` - &NetworkConfig holding configuration Infomation.
    /// * `info` - &NetworkInstanceInfo holding nodes to be cloned.
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
            let info = format!("{} -> {}", name, info.miner_nodes[idx].address);
            map.insert(
                name.clone(),
                Arc::new(Mutex::new(MinerNode::new(miner_config).await.expect(&info))),
            );
        }

        map
    }

    ///Clones storage nodes, compute nodes, miner nodes and user nodes. The storage nodes are initialised and a map is returned.
    ///
    /// ### Arguments
    ///
    /// * `config` - &NetworkConfig holding configuration Infomation.
    /// * `info` - &NetworkInstanceInfo holding nodes to be cloned.
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
            let info = format!("{} -> {}", name, info.storage_nodes[idx].address);
            map.insert(
                name.clone(),
                Arc::new(Mutex::new(
                    StorageNode::new(storage_config).await.expect(&info),
                )),
            );
        }

        map
    }

    ///Clones storage nodes, compute nodes, miner nodes and user nodes. The compute nodes are initialised and a map is returned.
    ///
    /// ### Arguments
    ///
    /// * `config` - &NetworkConfig holding configuration Infomation.
    /// * `info` - &NetworkInstanceInfo holding nodes to be cloned.
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
                compute_partition_full_size: config.compute_partition_full_size,
                compute_minimum_miner_pool_len: config.compute_minimum_miner_pool_len,
                jurisdiction: "US".to_string(),
                sanction_list: Vec::new(),
            };
            let info = format!("{} -> {}", name, info.compute_nodes[idx].address);
            map.insert(
                name.clone(),
                Arc::new(Mutex::new(
                    ComputeNode::new(compute_config).await.expect(&info),
                )),
            );
        }

        map
    }
    ///Clones storage nodes, compute nodes, miner nodes and user nodes. The user nodes are initialised and a map is returned.
    ///
    /// ### Arguments
    ///
    /// * `config` - &NetworkConfig holding configuration Infomation.
    /// * `info` - &NetworkInstanceInfo holding nodes to be cloned.
    async fn init_users(
        config: &NetworkConfig,
        info: &NetworkInstanceInfo,
    ) -> BTreeMap<String, ArcUserNode> {
        let mut map = BTreeMap::new();

        for (idx, name) in config.user_nodes.iter().enumerate() {
            let user_db_mode = if config.in_memory_db {
                DbMode::InMemory
            } else {
                DbMode::Test(info.user_nodes[idx].address.port() as usize)
            };
            let user_config = UserNodeConfig {
                user_node_idx: idx,
                user_db_mode,
                user_compute_node_idx: 0,
                peer_user_node_idx: 0,
                compute_nodes: info.compute_nodes.clone(),
                storage_nodes: info.storage_nodes.clone(),
                miner_nodes: info.miner_nodes.clone(),
                user_nodes: info.user_nodes.clone(),
                api_port: 3000,
                user_wallet_seeds: config.user_wallet_seeds.clone(),
            };

            let info = format!("{} -> {}", name, info.user_nodes[idx].address);
            map.insert(
                name.clone(),
                Arc::new(Mutex::new(UserNode::new(user_config).await.expect(&info))),
            );
        }

        map
    }

    ///Returns a mutable reference to the miner node with the matching name
    ///
    /// ### Arguments
    ///
    /// * `name` - &str of the miner node's name to be found.
    pub fn miner(&mut self, name: &str) -> Option<&mut ArcMinerNode> {
        self.miner_nodes.get_mut(name)
    }

    ///Returns a mutable interator of the miner nodes.
    pub fn miners_iter_mut(&mut self) -> impl Iterator<Item = &mut ArcMinerNode> {
        self.miner_nodes.values_mut()
    }

    ///returns a mutable reference to the compute node with the matching name.
    ///
    /// ### Arguments
    ///
    /// * `name` - &str of the compute node's name to be found.
    pub fn compute(&mut self, name: &str) -> Option<&mut ArcComputeNode> {
        self.compute_nodes.get_mut(name)
    }

    ///returns a mutable reference to the storage node with the matching name.
    ///
    /// ### Arguments
    ///
    /// * `name` - &str of the storage node's name to be found.
    pub fn storage(&mut self, name: &str) -> Option<&mut ArcStorageNode> {
        self.storage_nodes.get_mut(name)
    }

    ///returns a mutable reference to the user node with the matching name.
    ///
    /// ### Arguments
    ///
    /// * `name` - &str of the user node's name to be found.
    pub fn user(&mut self, name: &str) -> Option<&mut ArcUserNode> {
        self.user_nodes.get_mut(name)
    }

    ///Searches all node types and returns an address to the node with the matching name.
    ///
    /// ### Arguments
    ///
    /// * `name` - &str of the name of the node found.
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

    ///Searches all node types and returns the position of the node with the matching name.
    ///
    /// ### Arguments
    ///
    /// * `name` - &str of the name of the node found.
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

    ///Returns a list of initial transactions
    pub fn collect_initial_uxto_txs(&self) -> BTreeMap<String, Transaction> {
        make_utxo_set_from_seed(&self.config.compute_seed_utxo)
    }
}
