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

/// Represents a virtual configurable Zenotta network.
pub struct Network {
    miner_nodes: BTreeMap<String, MinerNode>,
    compute_nodes: BTreeMap<String, ComputeNode>,
    storage_nodes: BTreeMap<String, StorageNode>,
    user_nodes: BTreeMap<String, UserNode>,
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
    ) -> BTreeMap<String, MinerNode> {
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
            map.insert(name.clone(), MinerNode::new(miner_config).await.unwrap());
        }

        map
    }

    async fn init_storage(
        config: &NetworkConfig,
        info: &NetworkInstanceInfo,
    ) -> BTreeMap<String, StorageNode> {
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
                StorageNode::new(storage_config).await.unwrap(),
            );
        }

        map
    }

    async fn init_compute(
        config: &NetworkConfig,
        info: &NetworkInstanceInfo,
    ) -> BTreeMap<String, ComputeNode> {
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
                ComputeNode::new(compute_config).await.unwrap(),
            );
        }

        map
    }

    async fn init_users(
        config: &NetworkConfig,
        info: &NetworkInstanceInfo,
    ) -> BTreeMap<String, UserNode> {
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

            map.insert(name.clone(), UserNode::new(user_config).await.unwrap());
        }

        map
    }

    pub fn miner(&mut self, name: &str) -> Option<&mut MinerNode> {
        self.miner_nodes.get_mut(name)
    }

    pub fn miners_iter_mut(&mut self) -> impl Iterator<Item = &mut MinerNode> {
        self.miner_nodes.values_mut()
    }

    pub fn compute(&mut self, name: &str) -> Option<&mut ComputeNode> {
        self.compute_nodes.get_mut(name)
    }

    pub fn storage(&mut self, name: &str) -> Option<&mut StorageNode> {
        self.storage_nodes.get_mut(name)
    }

    pub fn user(&mut self, name: &str) -> Option<&mut UserNode> {
        self.user_nodes.get_mut(name)
    }

    pub fn get_address(&mut self, name: &str) -> Option<SocketAddr> {
        if let Some(miner) = self.miner_nodes.get(name) {
            return Some(miner.address());
        }
        if let Some(compute) = self.compute_nodes.get(name) {
            return Some(compute.address());
        }
        if let Some(storage) = self.storage_nodes.get(name) {
            return Some(storage.address());
        }
        if let Some(user) = self.user_nodes.get(name) {
            return Some(user.address());
        }
        None
    }
}
