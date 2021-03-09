//! This module provides a variety of utility functions to setup a test network,
//! to send a receive requests & responses, and generally to test the behavior and
//! correctness of the compute, miner, & storage modules.

use crate::comms_handler::Node;
use crate::compute::ComputeNode;
use crate::configurations::{
    ComputeNodeConfig, DbMode, ExtraNodeParams, MinerNodeConfig, NodeSpec, StorageNodeConfig,
    UserNodeConfig, UtxoSetSpec, WalletTxSpec,
};
use crate::constants::{DB_PATH, DB_PATH_TEST, WALLET_PATH};
use crate::miner::MinerNode;
use crate::storage::StorageNode;
use crate::user::UserNode;
use crate::utils::{
    loop_connnect_to_peers_async, loop_wait_connnect_to_peers_async, make_utxo_set_from_seed,
};
use futures::future::join_all;
use naom::primitives::asset::TokenAmount;
use naom::primitives::transaction::Transaction;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::error_span;
use tracing::info;
use tracing_futures::Instrument;

pub type ArcMinerNode = Arc<Mutex<MinerNode>>;
pub type ArcComputeNode = Arc<Mutex<ComputeNode>>;
pub type ArcStorageNode = Arc<Mutex<StorageNode>>;
pub type ArcUserNode = Arc<Mutex<UserNode>>;

/// Represents a virtual configurable Zenotta network.
pub struct Network {
    config: NetworkConfig,
    /// The info needed to create network nodes
    instance_info: NetworkInstanceInfo,
    /// All networked nodes
    arc_nodes: BTreeMap<String, ArcNode>,
    /// Handles for raft loop tasks
    raft_loop_handles: BTreeMap<String, JoinHandle<()>>,
    /// Currently active miner nodes
    active_nodes: BTreeMap<NodeType, Vec<String>>,
    /// compute to miner mapping of only active nodes
    active_compute_to_miner_mapping: BTreeMap<String, Vec<String>>,
    /// Currently dead nodes
    dead_nodes: BTreeSet<String>,
    /// Extra params to use for node construction
    extra_params: BTreeMap<String, ExtraNodeParams>,
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
    pub nodes: BTreeMap<NodeType, Vec<String>>,
    pub compute_to_miner_mapping: BTreeMap<String, Vec<String>>,
    pub test_duration_divider: usize,
}

impl NetworkConfig {
    pub fn nodes_mut(&mut self, node_type: NodeType) -> &mut Vec<String> {
        self.nodes.get_mut(&node_type).unwrap()
    }
}

/// Node info to create node
pub struct NetworkNodeInfo {
    pub node_spec: NodeSpec,
    pub node_type: NodeType,
    pub db_mode: DbMode,
    pub index: usize,
}

/// Info needed to create all nodes in network
pub struct NetworkInstanceInfo {
    pub node_infos: BTreeMap<String, NetworkNodeInfo>,
    pub miner_nodes: Vec<NodeSpec>,
    pub compute_nodes: Vec<NodeSpec>,
    pub storage_nodes: Vec<NodeSpec>,
    pub user_nodes: Vec<NodeSpec>,
}

/// Nodes of any type
#[derive(Clone)]
pub enum ArcNode {
    Miner(ArcMinerNode),
    Compute(ArcComputeNode),
    Storage(ArcStorageNode),
    User(ArcUserNode),
}

/// Types of nodes to create
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
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
            let extra = Default::default();
            let arc_node = init_arc_node(name, &config, &info, extra).await;
            arc_nodes.insert(name.to_owned(), arc_node);
        }

        Self::connect_all_nodes(&arc_nodes, &BTreeSet::new()).await;
        let raft_loop_handles = Self::spawn_raft_loops(&arc_nodes).await;

        Self {
            config: config.clone(),
            active_nodes: config.nodes.clone(),
            active_compute_to_miner_mapping: config.compute_to_miner_mapping.clone(),
            instance_info: info,
            arc_nodes,
            raft_loop_handles,
            dead_nodes: BTreeSet::new(),
            extra_params: BTreeMap::new(),
        }
    }

    ///Disconnect all required nodes
    async fn disconnect_all_nodes(arc_nodes: &BTreeMap<String, ArcNode>) {
        // Need to connect first so Raft messages can be sent.
        info!("Start disconnect to peers");
        for node in arc_nodes.values() {
            let (mut node_conn, _, _) = connect_info_peers(node).await;
            join_all(node_conn.disconnect_all(None).await).await;
        }
    }

    ///Connect all required nodes
    async fn connect_all_nodes(arc_nodes: &BTreeMap<String, ArcNode>, dead: &BTreeSet<SocketAddr>) {
        // Need to connect first so Raft messages can be sent.
        info!("Start connect to peers");
        for (name, node) in arc_nodes {
            let (node_conn, mut addrs, _) = connect_info_peers(node).await;
            addrs.retain(|a| !dead.contains(a));

            loop_connnect_to_peers_async(node_conn, addrs, None).await;
            info!(?name, "Peer connect complete");
        }

        info!("Peers connect complete");

        for node in arc_nodes.values() {
            let (node_conn, _, mut expected_connected_addrs) = connect_info_peers(node).await;
            expected_connected_addrs.retain(|a| !dead.contains(a));

            loop_wait_connnect_to_peers_async(node_conn, expected_connected_addrs).await;
        }
        info!("Peers connect complete: all connected");
    }

    ///Creates and tests rafts
    async fn spawn_raft_loops(
        arc_nodes: &BTreeMap<String, ArcNode>,
    ) -> BTreeMap<String, JoinHandle<()>> {
        let mut raft_loop_handles = BTreeMap::new();

        for (name, node) in arc_nodes {
            if let Some((t, addr, raft_loop)) = raft_loop(node).await {
                let peer_span = error_span!("", ?t, ?name, ?addr);
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
        }

        raft_loop_handles
    }

    /// Kill all nodes.
    pub async fn close_raft_loops_and_drop(mut self) {
        Self::close_raft_loops(&self.arc_nodes, &mut self.raft_loop_handles).await;
        Self::stop_listening(&self.arc_nodes).await;
        Self::disconnect_all_nodes(&self.arc_nodes).await;
    }

    /// Re-connect specified nodes.
    pub async fn re_connect_nodes_named(&mut self, names: &[&str]) {
        // Re-spawn specified nodes
        for name in names {
            self.dead_nodes.remove(*name);
            if let Some(node) = self.arc_nodes.get(*name) {
                let (mut node_conn, _, _) = connect_info_peers(node).await;
                node_conn.set_pause_listening(false).await;
            }
        }
        self.update_active_nodes();

        // Re-establish all missing connections
        let dead_addr: BTreeSet<_> = self
            .dead_nodes
            .iter()
            .map(|n| self.instance_info.node_infos[n].node_spec.address)
            .collect();
        Self::connect_all_nodes(&self.arc_nodes, &dead_addr).await;
        self.update_active_nodes();
    }

    /// disconnect specified nodes.
    pub async fn disconnect_nodes_named(&mut self, names: &[&str]) {
        info!("Start disconnect to peers");
        for name in names {
            if let Some(node) = self.arc_nodes.get(*name) {
                let (mut node_conn, _, _) = connect_info_peers(node).await;
                node_conn.set_pause_listening(true).await;
                join_all(node_conn.disconnect_all(None).await).await;
            }
        }
        // Remove from active nodes
        self.dead_nodes.extend(names.iter().map(|v| v.to_string()));
        self.update_active_nodes();
    }

    /// Re-spawn specified nodes.
    pub async fn re_spawn_nodes_named(&mut self, names: &[&str]) {
        // Re-spawn specified nodes
        let mut arc_nodes = BTreeMap::new();
        for name in names {
            let extra = self.extra_params.remove(*name).unwrap_or_default();
            let arc_node = init_arc_node(name, &self.config, &self.instance_info, extra).await;
            arc_nodes.insert(name.to_string(), arc_node);
            self.dead_nodes.remove(*name);
        }
        self.arc_nodes.append(&mut arc_nodes.clone());
        self.update_active_nodes();

        // Re-establish all missing connections
        let dead_addr: BTreeSet<_> = self
            .dead_nodes
            .iter()
            .map(|n| self.instance_info.node_infos[n].node_spec.address)
            .collect();
        Self::connect_all_nodes(&self.arc_nodes, &dead_addr).await;

        // Spawn new nodes raft loops
        let mut raft_loop_handles = Self::spawn_raft_loops(&arc_nodes).await;
        self.raft_loop_handles.append(&mut raft_loop_handles);
    }

    /// Kill specified nodes.
    pub async fn close_raft_loops_and_drop_named(&mut self, names: &[&str]) {
        let mut arc_nodes = BTreeMap::new();
        let mut raft_loop_handles = BTreeMap::new();

        // Kill nodes
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
        Self::disconnect_all_nodes(&arc_nodes).await;

        // Store extra params for re-spawn
        for (name, node) in &arc_nodes {
            if let Some(extra) = take_closed_extra_params(node).await {
                self.extra_params.insert(name.clone(), extra);
            }
        }

        // Remove from active nodes
        self.dead_nodes.extend(names.iter().map(|v| v.to_string()));
        self.update_active_nodes();
    }

    /// Restore from config and Remove all dead nodes
    fn update_active_nodes(&mut self) {
        let mut active_nodes = self.config.nodes.clone();
        for nodes in active_nodes.values_mut() {
            nodes.retain(|n| !self.dead_nodes.contains(n));
        }

        let mut active_map: BTreeMap<_, _> = self
            .config
            .compute_to_miner_mapping
            .clone()
            .into_iter()
            .filter(|(c, _)| !self.dead_nodes.contains(c))
            .collect();

        for miners in active_map.values_mut() {
            miners.retain(|m| !self.dead_nodes.contains(m));
        }

        self.active_compute_to_miner_mapping = active_map;
        self.active_nodes = active_nodes;
    }

    /// Completes and ends raft loops.
    async fn close_raft_loops(
        arc_nodes: &BTreeMap<String, ArcNode>,
        raft_loop_handles: &mut BTreeMap<String, JoinHandle<()>>,
    ) {
        info!("Close raft");
        for node in arc_nodes.values() {
            close_raft_loop(node).await
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
            join_all(stop_listening(node).await).await;
        }
    }

    ///Returns a mutable reference to the miner node with the matching name
    ///
    /// ### Arguments
    ///
    /// * `name` - &str of the miner node's name to be found.
    pub fn miner(&self, name: &str) -> Option<&ArcMinerNode> {
        self.arc_nodes.get(name).and_then(|v| {
            if let ArcNode::Miner(v) = v {
                Some(v)
            } else {
                None
            }
        })
    }

    ///returns a mutable reference to the compute node with the matching name.
    ///
    /// ### Arguments
    ///
    /// * `name` - &str of the compute node's name to be found.
    pub fn compute(&self, name: &str) -> Option<&ArcComputeNode> {
        self.arc_nodes.get(name).and_then(|v| {
            if let ArcNode::Compute(v) = v {
                Some(v)
            } else {
                None
            }
        })
    }

    ///returns a mutable reference to the storage node with the matching name.
    ///
    /// ### Arguments
    ///
    /// * `name` - &str of the storage node's name to be found.
    pub fn storage(&self, name: &str) -> Option<&ArcStorageNode> {
        self.arc_nodes.get(name).and_then(|v| {
            if let ArcNode::Storage(v) = v {
                Some(v)
            } else {
                None
            }
        })
    }

    ///returns a mutable reference to the user node with the matching name.
    ///
    /// ### Arguments
    ///
    /// * `name` - &str of the user node's name to be found.
    pub fn user(&self, name: &str) -> Option<&ArcUserNode> {
        self.arc_nodes.get(name).and_then(|v| {
            if let ArcNode::User(v) = v {
                Some(v)
            } else {
                None
            }
        })
    }

    ///Searches all node types and returns an address to the node with the matching name.
    ///
    /// ### Arguments
    ///
    /// * `name` - &str of the name of the node found.
    pub async fn get_address(&self, name: &str) -> Option<SocketAddr> {
        if let Some(node) = self.arc_nodes.get(name) {
            Some(address(node).await)
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

    ///Returns active nodes of given type
    pub fn active_nodes(&self, node_type: NodeType) -> &[String] {
        &self.active_nodes[&node_type]
    }

    ///Returns all active nodes
    pub fn all_active_nodes(&self) -> &BTreeMap<NodeType, Vec<String>> {
        &self.active_nodes
    }

    ///Active Compute miner mapping
    pub fn active_compute_to_miner_mapping(&self) -> &BTreeMap<String, Vec<String>> {
        &self.active_compute_to_miner_mapping
    }

    ///Config that launched the network
    pub fn config(&self) -> &NetworkConfig {
        &self.config
    }

    ///Dead nodes not currently active
    pub fn dead_nodes(&self) -> &BTreeSet<String> {
        &self.dead_nodes
    }

    ///Mining Reward
    pub fn mining_reward(&self) -> TokenAmount {
        let c_len = self.config.nodes[&NodeType::Compute].len();
        TokenAmount(7510185241) / c_len as u64
    }
}

///Dispatch to address
async fn address(node: &ArcNode) -> SocketAddr {
    match node {
        ArcNode::Miner(v) => v.lock().await.address(),
        ArcNode::Compute(v) => v.lock().await.address(),
        ArcNode::Storage(v) => v.lock().await.address(),
        ArcNode::User(v) => v.lock().await.address(),
    }
}

///Dispatch to connect_info_peers
async fn connect_info_peers(node: &ArcNode) -> (Node, Vec<SocketAddr>, Vec<SocketAddr>) {
    match node {
        ArcNode::Miner(n) => n.lock().await.connect_info_peers(),
        ArcNode::Compute(n) => n.lock().await.connect_info_peers(),
        ArcNode::Storage(n) => n.lock().await.connect_info_peers(),
        ArcNode::User(n) => n.lock().await.connect_info_peers(),
    }
}

///Dispatch to stop_listening_loop
async fn stop_listening(node: &ArcNode) -> Vec<JoinHandle<()>> {
    match node {
        ArcNode::Miner(n) => n.lock().await.stop_listening_loop().await,
        ArcNode::Compute(n) => n.lock().await.stop_listening_loop().await,
        ArcNode::Storage(n) => n.lock().await.stop_listening_loop().await,
        ArcNode::User(n) => n.lock().await.stop_listening_loop().await,
    }
}

///Dispatch to close_raft_loop
async fn close_raft_loop(node: &ArcNode) {
    match node {
        ArcNode::Compute(n) => n.lock().await.close_raft_loop().await,
        ArcNode::Storage(n) => n.lock().await.close_raft_loop().await,
        ArcNode::Miner(_) | ArcNode::User(_) => (),
    }
}

///Dispatch to raft_loop, providing also the address and a tag.
async fn raft_loop(node: &ArcNode) -> Option<(String, SocketAddr, impl Future<Output = ()>)> {
    use futures::future::FutureExt;
    match node {
        ArcNode::Compute(n) => {
            let node = n.lock().await;
            Some((
                "compute_node".to_owned(),
                node.address(),
                node.raft_loop().left_future(),
            ))
        }
        ArcNode::Storage(n) => {
            let node = n.lock().await;
            Some((
                "storage_node".to_owned(),
                node.address(),
                node.raft_loop().right_future(),
            ))
        }
        ArcNode::Miner(_) | ArcNode::User(_) => None,
    }
}

///Dispatch to take_closed_extra_params
async fn take_closed_extra_params(node: &ArcNode) -> Option<ExtraNodeParams> {
    match node {
        ArcNode::Compute(n) => Some(n.lock().await.take_closed_extra_params().await),
        ArcNode::Storage(n) => Some(n.lock().await.take_closed_extra_params().await),
        ArcNode::Miner(_) | ArcNode::User(_) => None,
    }
}

///Creates a NetworkInstanceInfo object with config object values.
///
/// ### Arguments
///
/// * `config` - &NetworkConfig object containing parameters for the NetworkInstanceInfo object creation.
fn init_instance_info(config: &NetworkConfig) -> NetworkInstanceInfo {
    let ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
    let mut first_port = config.initial_port;

    let mut nodes = BTreeMap::new();
    for (node_type, names) in &config.nodes {
        let (next_port, specs) = node_specs(ip, first_port, names.len());
        nodes.insert(*node_type, (names, specs));
        first_port = next_port;
    }

    let mem_db = config.in_memory_db;
    let node_infos = nodes
        .iter()
        .flat_map(|(node_type, infos)| node_infos(*node_type, infos, mem_db))
        .collect();

    NetworkInstanceInfo {
        node_infos,
        miner_nodes: nodes.remove(&NodeType::Miner).unwrap().1,
        compute_nodes: nodes.remove(&NodeType::Compute).unwrap().1,
        storage_nodes: nodes.remove(&NodeType::Storage).unwrap().1,
        user_nodes: nodes.remove(&NodeType::User).unwrap().1,
    }
}

///Return the infos necessary to initialize the node.
fn node_infos(
    node_type: NodeType,
    (node_names, node_specs): &(&Vec<String>, Vec<NodeSpec>),
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
/// * `extra`  - additional parameter for construction
async fn init_arc_node(
    name: &str,
    config: &NetworkConfig,
    info: &NetworkInstanceInfo,
    extra: ExtraNodeParams,
) -> ArcNode {
    let node_info = &info.node_infos[name];
    match node_info.node_type {
        NodeType::Miner => ArcNode::Miner(init_miner(name, &config, &info).await),
        NodeType::Compute => ArcNode::Compute(init_compute(name, &config, &info, extra).await),
        NodeType::Storage => ArcNode::Storage(init_storage(name, &config, &info, extra).await),
        NodeType::User => ArcNode::User(init_user(name, &config, &info).await),
    }
}

///Initialize Miner node of given name based on network info.
///
/// ### Arguments
///
/// * `name`   - Name of the node to initialize.
/// * `config` - &NetworkConfig holding configuration Infomation.
/// * `info`   - &NetworkInstanceInfo holding nodes to be cloned.
async fn init_miner(
    name: &str,
    config: &NetworkConfig,
    info: &NetworkInstanceInfo,
) -> ArcMinerNode {
    // Handle the single miner multi compute node in test only
    let (miner_compute_node_idx, extra_connect_addr) = {
        let name = name.to_owned();
        let mut miner_compute_infos = config
            .compute_to_miner_mapping
            .iter()
            .filter(|(_, ms)| ms.contains(&name))
            .map(|(c, _)| &info.node_infos[c]);

        let miner_compute_node_idx = miner_compute_infos.next().unwrap().index;
        let extra_connect_addr = miner_compute_infos.map(|i| i.node_spec.address).collect();
        (miner_compute_node_idx, extra_connect_addr)
    };

    // Create node
    let node_info = &info.node_infos[name];
    let miner_config = MinerNodeConfig {
        miner_node_idx: node_info.index,
        miner_db_mode: node_info.db_mode,
        miner_compute_node_idx,
        compute_nodes: info.compute_nodes.clone(),
        storage_nodes: info.storage_nodes.clone(),
        miner_nodes: info.miner_nodes.clone(),
        user_nodes: info.user_nodes.clone(),
    };
    let info_str = format!("{} -> {}", name, node_info.node_spec.address);
    info!("New Miner {}", info_str);

    let mut miner = MinerNode::new(miner_config).await.expect(&info_str);
    miner.extra_connect_addr = extra_connect_addr;
    Arc::new(Mutex::new(miner))
}

///Initialize Storage node of given name based on network info.
///
/// ### Arguments
///
/// * `name`   - Name of the node to initialize.
/// * `config` - &NetworkConfig holding configuration Infomation.
/// * `info`   - &NetworkInstanceInfo holding nodes to be cloned.
/// * `extra`  - additional parameter for construction
async fn init_storage(
    name: &str,
    config: &NetworkConfig,
    info: &NetworkInstanceInfo,
    extra: ExtraNodeParams,
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
        storage_raft_tick_timeout: 200 / config.test_duration_divider,
        storage_block_timeout: 1000 / config.test_duration_divider,
    };
    let info = format!("{} -> {}", name, node_info.node_spec.address);
    info!("New Storage {}", info);
    Arc::new(Mutex::new(
        StorageNode::new(storage_config, extra).await.expect(&info),
    ))
}

///Initialize Compute node of given name based on network info.
///
/// ### Arguments
///
/// * `name`   - Name of the node to initialize.
/// * `config` - &NetworkConfig holding configuration Infomation.
/// * `info`   - &NetworkInstanceInfo holding nodes to be cloned.
/// * `extra`  - additional parameter for construction
async fn init_compute(
    name: &str,
    config: &NetworkConfig,
    info: &NetworkInstanceInfo,
    extra: ExtraNodeParams,
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
        compute_raft_tick_timeout: 200 / config.test_duration_divider,
        compute_transaction_timeout: 100 / config.test_duration_divider,
        compute_seed_utxo: config.compute_seed_utxo.clone(),
        compute_partition_full_size: config.compute_partition_full_size,
        compute_minimum_miner_pool_len: config.compute_minimum_miner_pool_len,
        jurisdiction: "US".to_string(),
        sanction_list: Vec::new(),
    };
    let info = format!("{} -> {}", name, node_info.node_spec.address);
    info!("New Compute {}", info);
    Arc::new(Mutex::new(
        ComputeNode::new(compute_config, extra).await.expect(&info),
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

/// Remove all db for the given config
pub fn remove_all_node_dbs(config: &NetworkConfig) {
    let info = init_instance_info(config);

    for (_name, node) in info.node_infos {
        let port = node.node_spec.address.port();
        use NodeType::*;
        let db_paths = match node.node_type {
            Miner | User => {
                let v = format!("{}/{}.{}", WALLET_PATH, DB_PATH_TEST, port);
                vec![v]
            }
            Compute => {
                let v1 = format!("{}/{}.compute.{}", DB_PATH, DB_PATH_TEST, port);
                let v2 = format!("{}/{}.compute_raft.{}", DB_PATH, DB_PATH_TEST, port);
                vec![v1, v2]
            }
            Storage => {
                let v1 = format!("{}/{}.storage.{}", DB_PATH, DB_PATH_TEST, port);
                let v2 = format!("{}/{}.storage_raft.{}", DB_PATH, DB_PATH_TEST, port);
                vec![v1, v2]
            }
        };
        for to_remove in db_paths {
            if let Err(e) = std::fs::remove_dir_all(to_remove.clone()) {
                info!("Not removed local db: {}, {:?}", to_remove, e);
            }
        }
    }
}
