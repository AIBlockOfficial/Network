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
use crate::utils::{
    loop_connnect_to_peers_async, loop_wait_connnect_to_peers_async, make_utxo_set_from_seed,
};
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
    instance_info: NetworkInstanceInfo,
    arc_nodes: BTreeMap<String, ArcNode>,
    raft_loop_handles: BTreeMap<String, JoinHandle<()>>,
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

pub struct NetworkNodeInfo {
    pub node_spec: NodeSpec,
    pub node_type: NodeType,
    pub db_mode: DbMode,
    pub index: usize,
}

pub struct NetworkInstanceInfo {
    pub node_infos: BTreeMap<String, NetworkNodeInfo>,
    pub miner_nodes: Vec<NodeSpec>,
    pub compute_nodes: Vec<NodeSpec>,
    pub storage_nodes: Vec<NodeSpec>,
    pub user_nodes: Vec<NodeSpec>,
}

#[derive(Clone)]
pub enum ArcNode {
    Miner(ArcMinerNode),
    Compute(ArcComputeNode),
    Storage(ArcStorageNode),
    User(ArcUserNode),
}

impl ArcNode {
    fn miner(&self) -> Option<&ArcMinerNode> {
        if let Self::Miner(s) = self {
            Some(s)
        } else {
            None
        }
    }

    fn compute(&self) -> Option<&ArcComputeNode> {
        if let Self::Compute(c) = self {
            Some(c)
        } else {
            None
        }
    }

    fn storage(&self) -> Option<&ArcStorageNode> {
        if let Self::Storage(s) = self {
            Some(s)
        } else {
            None
        }
    }

    fn user(&self) -> Option<&ArcUserNode> {
        if let Self::User(s) = self {
            Some(s)
        } else {
            None
        }
    }
}

#[derive(Copy, Clone)]
pub enum NodeType {
    Miner,
    Compute,
    Storage,
    User,
}

impl Network {
    ///Creates a Network instance using a config object
    ///
    /// ###Arguments
    ///
    /// * `config` - Holds the values to instanciate a Network object
    pub async fn create_from_config(config: &NetworkConfig) -> Self {
        let info = init_instance_info(config);

        let mut arc_nodes = BTreeMap::new();
        for name in info.node_infos.keys() {
            let arc_node = init_arc_node(name, &config, &info).await;
            arc_nodes.insert(name.to_owned(), arc_node);
        }

        let raft_loop_handles = Self::spawn_raft_loops(&arc_nodes).await;

        Self {
            config: config.clone(),
            instance_info: info,
            arc_nodes,
            raft_loop_handles,
        }
    }

    ///Creates and tests rafts
    async fn spawn_raft_loops(
        arc_nodes: &BTreeMap<String, ArcNode>,
    ) -> BTreeMap<String, JoinHandle<()>> {
        let mut raft_loop_handles = BTreeMap::new();
        let compute_nodes: BTreeMap<_, _> = arc_nodes
            .iter()
            .filter_map(|(n, v)| v.compute().map(|v| (n.clone(), v)))
            .collect();
        let storage_nodes: BTreeMap<_, _> = arc_nodes
            .iter()
            .filter_map(|(n, v)| v.storage().map(|v| (n.clone(), v)))
            .collect();

        // Need to connect first so Raft messages can be sent.
        info!("Start connect to peers");
        for (name, node) in &compute_nodes {
            let node = node.lock().await;
            let (node_conn, addrs, _) = node.connect_info_peers();
            loop_connnect_to_peers_async(node_conn, addrs, None).await;
            info!(?name, "Peer connect complete");
        }
        info!("Peers connect complete");
        for node in compute_nodes.values() {
            let node = node.lock().await;
            let (node_conn, _, expected_connected_addrs) = node.connect_info_peers();
            loop_wait_connnect_to_peers_async(node_conn, expected_connected_addrs).await;
        }
        info!("Peers connect complete: all connected");

        for (name, node) in &compute_nodes {
            let node = node.lock().await;
            let peer_span = error_span!("compute_node", ?name, addr = ?node.address());
            let raft_loop = node.raft_loop();
            raft_loop_handles.insert(
                name.clone(),
                tokio::spawn(
                    async move {
                        info!("Start raft");
                        raft_loop.await;
                        info!("raft complete");
                    }
                    .instrument(peer_span),
                ),
            );
        }

        // Need to connect first so Raft messages can be sent.
        info!("Start connect to peers");
        for (name, node) in &storage_nodes {
            let node = node.lock().await;
            let (node_conn, addrs, _) = node.connect_info_peers();
            loop_connnect_to_peers_async(node_conn, addrs, None).await;
            info!(?name, "Peer connect complete");
        }
        info!("Peers connect complete");
        for node in storage_nodes.values() {
            let node = node.lock().await;
            let (node_conn, _, expected_connected_addrs) = node.connect_info_peers();
            loop_wait_connnect_to_peers_async(node_conn, expected_connected_addrs).await;
        }
        info!("Peers connect complete: all connected");

        for (name, node) in &storage_nodes {
            let node = node.lock().await;
            let peer_span = error_span!("storage_node", ?name, addr = ?node.address());
            let raft_loop = node.raft_loop();
            raft_loop_handles.insert(
                name.clone(),
                tokio::spawn(
                    async move {
                        info!("Start raft");
                        raft_loop.await;
                        info!("raft complete");
                    }
                    .instrument(peer_span),
                ),
            );
        }

        raft_loop_handles
    }

    /// Completes and ends raft loops.
    pub async fn close_raft_loops_and_drop(mut self) {
        Self::close_raft_loops(&self.arc_nodes, &mut self.raft_loop_handles).await;
        Self::stop_listening(&self.arc_nodes).await;
    }

    /// Kill specified nodes.
    pub async fn close_raft_loops_and_drop_named(&mut self, names: &[&str]) {
        let mut arc_nodes = BTreeMap::new();
        let mut raft_loop_handles = BTreeMap::new();

        for name in names {
            if let Some((k, v)) = self.arc_nodes.remove_entry(*name) {
                arc_nodes.insert(k, v);
            }
            if let Some((k, v)) = self.raft_loop_handles.remove_entry(*name) {
                raft_loop_handles.insert(k, v);
            }
        }

        Self::close_raft_loops(&arc_nodes, &mut raft_loop_handles).await;
        Self::stop_listening(&arc_nodes).await;
    }

    async fn close_raft_loops(
        arc_nodes: &BTreeMap<String, ArcNode>,
        raft_loop_handles: &mut BTreeMap<String, JoinHandle<()>>,
    ) {
        let compute_nodes: BTreeMap<_, _> = arc_nodes
            .iter()
            .filter_map(|(n, v)| v.compute().map(|v| (n, v)))
            .collect();
        let storage_nodes: BTreeMap<_, _> = arc_nodes
            .iter()
            .filter_map(|(n, v)| v.storage().map(|v| (n, v)))
            .collect();

        info!("Close compute raft");
        for node in compute_nodes.values() {
            node.lock().await.close_raft_loop().await;
        }

        info!("Close storage raft");
        for node in storage_nodes.values() {
            node.lock().await.close_raft_loop().await;
        }

        join_all(
            std::mem::take(raft_loop_handles)
                .into_iter()
                .map(|(_, v)| v),
        )
        .await;
    }

    /// Stop node listening on port
    async fn stop_listening(arc_nodes: &BTreeMap<String, ArcNode>) {
        info!("Stop listening arc_nodes");
        for node in arc_nodes.values() {
            let handles = match node {
                ArcNode::Miner(n) => n.lock().await.stop_listening_loop().await,
                ArcNode::Compute(n) => n.lock().await.stop_listening_loop().await,
                ArcNode::Storage(n) => n.lock().await.stop_listening_loop().await,
                ArcNode::User(n) => n.lock().await.stop_listening_loop().await,
            };
            join_all(handles).await;
        }
    }

    ///Returns a mutable reference to the miner node with the matching name
    ///
    /// ### Arguments
    ///
    /// * `name` - &str of the miner node's name to be found.
    pub fn miner(&self, name: &str) -> Option<&ArcMinerNode> {
        self.arc_nodes.get(name).and_then(|v| v.miner())
    }

    ///returns a mutable reference to the compute node with the matching name.
    ///
    /// ### Arguments
    ///
    /// * `name` - &str of the compute node's name to be found.
    pub fn compute(&self, name: &str) -> Option<&ArcComputeNode> {
        self.arc_nodes.get(name).and_then(|v| v.compute())
    }

    ///returns a mutable reference to the storage node with the matching name.
    ///
    /// ### Arguments
    ///
    /// * `name` - &str of the storage node's name to be found.
    pub fn storage(&self, name: &str) -> Option<&ArcStorageNode> {
        self.arc_nodes.get(name).and_then(|v| v.storage())
    }

    ///returns a mutable reference to the user node with the matching name.
    ///
    /// ### Arguments
    ///
    /// * `name` - &str of the user node's name to be found.
    pub fn user(&self, name: &str) -> Option<&ArcUserNode> {
        self.arc_nodes.get(name).and_then(|v| v.user())
    }

    ///Searches all node types and returns an address to the node with the matching name.
    ///
    /// ### Arguments
    ///
    /// * `name` - &str of the name of the node found.
    pub async fn get_address(&self, name: &str) -> Option<SocketAddr> {
        if let Some(node) = self.arc_nodes.get(name) {
            Some(match node {
                ArcNode::Miner(v) => v.lock().await.address(),
                ArcNode::Compute(v) => v.lock().await.address(),
                ArcNode::Storage(v) => v.lock().await.address(),
                ArcNode::User(v) => v.lock().await.address(),
            })
        } else {
            None
        }
    }

    ///Searches all node types and returns the position of the node with the matching name.
    ///
    /// ### Arguments
    ///
    /// * `name` - &str of the name of the node found.
    pub fn get_position(&mut self, name: &str) -> Option<usize> {
        self.instance_info.node_infos.get(name).map(|i| i.index)
    }

    ///Returns a list of initial transactions
    pub fn collect_initial_uxto_txs(&self) -> BTreeMap<String, Transaction> {
        make_utxo_set_from_seed(&self.config.compute_seed_utxo)
    }
}

///Creates a NetworkInstanceInfo object with config object values.
///
/// ### Arguments
///
/// * `config` - &NetworkConfig object containing parameters for the NetworkInstanceInfo object creation.
fn init_instance_info(config: &NetworkConfig) -> NetworkInstanceInfo {
    let ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
    let next_port = config.initial_port;
    let miner_names = &config.miner_nodes;
    let compute_names = &config.compute_nodes;
    let storage_names = &config.storage_nodes;
    let user_names = &config.user_nodes;

    let (next_port, miner_nodes) = node_specs(ip, next_port, miner_names.len());
    let (next_port, compute_nodes) = node_specs(ip, next_port, compute_names.len());
    let (next_port, storage_nodes) = node_specs(ip, next_port, storage_names.len());
    let (_next_port, user_nodes) = node_specs(ip, next_port, user_names.len());

    let mem_db = config.in_memory_db;
    use NodeType::*;
    let node_infos = None
        .into_iter()
        .chain(node_infos(miner_names, &miner_nodes, Miner, mem_db))
        .chain(node_infos(compute_names, &compute_nodes, Compute, mem_db))
        .chain(node_infos(storage_names, &storage_nodes, Storage, mem_db))
        .chain(node_infos(user_names, &user_nodes, User, mem_db))
        .collect();

    NetworkInstanceInfo {
        node_infos,
        miner_nodes,
        compute_nodes,
        storage_nodes,
        user_nodes,
    }
}

///Return the infos necessary to initialize the node.
fn node_infos(
    node_names: &[String],
    node_specs: &[NodeSpec],
    node_type: NodeType,
    in_memory_db: bool,
) -> BTreeMap<String, NetworkNodeInfo> {
    node_names
        .iter()
        .zip(node_specs)
        .enumerate()
        .map(|(index, (name, node_spec))| {
            (
                name.clone(),
                NetworkNodeInfo {
                    node_spec: node_spec.clone(),
                    node_type,
                    db_mode: if in_memory_db {
                        DbMode::InMemory
                    } else {
                        DbMode::Test(node_spec.address.port() as usize)
                    },
                    index,
                },
            )
        })
        .collect()
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

///Initialize network node of given name based on network info.
///
/// ### Arguments
///
/// * `name`   - Name of the node to initialize.
/// * `config` - &NetworkConfig holding configuration Infomation.
/// * `info`   - &NetworkInstanceInfo holding nodes to be cloned.
async fn init_arc_node(name: &str, config: &NetworkConfig, info: &NetworkInstanceInfo) -> ArcNode {
    let node_info = &info.node_infos[name];
    match node_info.node_type {
        NodeType::Miner => ArcNode::Miner(init_miner(name, &info).await),
        NodeType::Compute => ArcNode::Compute(init_compute(name, &config, &info).await),
        NodeType::Storage => ArcNode::Storage(init_storage(name, &config, &info).await),
        NodeType::User => ArcNode::User(init_user(name, &config, &info).await),
    }
}

///Initialize Miner node of given name based on network info.
///
/// ### Arguments
///
/// * `name`   - Name of the node to initialize.
/// * `info`   - &NetworkInstanceInfo holding nodes to be cloned.
async fn init_miner(name: &str, info: &NetworkInstanceInfo) -> ArcMinerNode {
    let node_info = &info.node_infos[name];
    let miner_config = MinerNodeConfig {
        miner_node_idx: node_info.index,
        miner_db_mode: node_info.db_mode,
        miner_compute_node_idx: 0,
        compute_nodes: info.compute_nodes.clone(),
        storage_nodes: info.storage_nodes.clone(),
        miner_nodes: info.miner_nodes.clone(),
        user_nodes: info.user_nodes.clone(),
    };
    let info = format!("{} -> {}", name, node_info.node_spec.address);
    info!("New Miner {}", info);
    Arc::new(Mutex::new(MinerNode::new(miner_config).await.expect(&info)))
}

///Initialize Storage node of given name based on network info.
///
/// ### Arguments
///
/// * `name`   - Name of the node to initialize.
/// * `config` - &NetworkConfig holding configuration Infomation.
/// * `info`   - &NetworkInstanceInfo holding nodes to be cloned.
async fn init_storage(
    name: &str,
    config: &NetworkConfig,
    info: &NetworkInstanceInfo,
) -> ArcStorageNode {
    let node_info = &info.node_infos[name];
    let storage_raft = if config.storage_raft { 1 } else { 0 };

    let storage_config = StorageNodeConfig {
        storage_node_idx: node_info.index,
        storage_db_mode: node_info.db_mode,
        compute_nodes: info.compute_nodes.clone(),
        storage_nodes: info.storage_nodes.clone(),
        user_nodes: info.user_nodes.clone(),
        storage_raft,
        storage_raft_tick_timeout: 200 / TEST_DURATION_DIVIDER,
        storage_block_timeout: 1000 / TEST_DURATION_DIVIDER,
    };
    let info = format!("{} -> {}", name, node_info.node_spec.address);
    info!("New Storage {}", info);
    Arc::new(Mutex::new(
        StorageNode::new(storage_config).await.expect(&info),
    ))
}

///Initialize Compute node of given name based on network info.
///
/// ### Arguments
///
/// * `name`   - Name of the node to initialize.
/// * `config` - &NetworkConfig holding configuration Infomation.
/// * `info`   - &NetworkInstanceInfo holding nodes to be cloned.
async fn init_compute(
    name: &str,
    config: &NetworkConfig,
    info: &NetworkInstanceInfo,
) -> ArcComputeNode {
    let node_info = &info.node_infos[name];
    let compute_raft = if config.compute_raft { 1 } else { 0 };

    let compute_config = ComputeNodeConfig {
        compute_raft,
        compute_db_mode: node_info.db_mode,
        compute_node_idx: node_info.index,
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
    let info = format!("{} -> {}", name, node_info.node_spec.address);
    info!("New Compute {}", info);
    Arc::new(Mutex::new(
        ComputeNode::new(compute_config).await.expect(&info),
    ))
}

///Initialize User node of given name based on network info.
///
/// ### Arguments
///
/// * `name`   - Name of the node to initialize.
/// * `info`   - &NetworkInstanceInfo holding nodes to be cloned.
async fn init_user(name: &str, config: &NetworkConfig, info: &NetworkInstanceInfo) -> ArcUserNode {
    let node_info = &info.node_infos[name];
    let user_config = UserNodeConfig {
        user_node_idx: node_info.index,
        user_db_mode: node_info.db_mode,
        user_compute_node_idx: 0,
        peer_user_node_idx: 0,
        compute_nodes: info.compute_nodes.clone(),
        storage_nodes: info.storage_nodes.clone(),
        miner_nodes: info.miner_nodes.clone(),
        user_nodes: info.user_nodes.clone(),
        api_port: 3000,
        user_wallet_seeds: config.user_wallet_seeds.clone(),
    };

    let info = format!("{} -> {}", name, node_info.node_spec.address);
    info!("New User {}", info);
    Arc::new(Mutex::new(UserNode::new(user_config).await.expect(&info)))
}
