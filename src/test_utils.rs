//! This module provides a variety of utility functions to setup a test network,
//! to send a receive requests & responses, and generally to test the behavior and
//! correctness of the compute, miner, & storage modules.

use crate::compute::ComputeNode;
use crate::configurations::{ComputeNodeConfig, NodeSpec};
use crate::interfaces::{ComputeInterface, MinerInterface};
use crate::miner::MinerNode;
use crate::storage::StorageNode;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio::task::JoinHandle;
use tracing::{error, field, info_span, trace};
use tracing_futures::Instrument;

/// Represents a virtual configurable Zenotta network.
pub struct Network {
    miner_nodes: BTreeMap<String, MinerNode>,
    compute_nodes: BTreeMap<String, ComputeNode>,
    storage_nodes: BTreeMap<String, StorageNode>,
}

/// Represents a virtual network configuration.
/// Can be created using the builder or deserialized from JSON.
#[derive(Serialize, Deserialize)]
pub struct NetworkConfig {
    pub initial_port: u16,
    pub miner_nodes: Vec<String>,
    pub compute_nodes: Vec<String>,
    pub storage_nodes: Vec<String>,
}

pub struct NetworkInstanceInfo {
    pub miner_nodes: Vec<(String, SocketAddr)>,
    pub compute_nodes: Vec<(String, SocketAddr)>,
    pub storage_nodes: Vec<(String, SocketAddr)>,
}

impl Network {
    pub async fn create_from_config(config: &NetworkConfig) -> Self {
        let config = Self::init_instance_info(config);
        let miner_nodes = Self::init_miners(&config).await;
        let compute_nodes = Self::init_compute(&config).await;
        let storage_nodes = Self::init_storage(&config).await;
        Self {
            miner_nodes,
            compute_nodes,
            storage_nodes,
        }
    }

    fn init_instance_info(config: &NetworkConfig) -> NetworkInstanceInfo {
        let initial_miner_port = config.initial_port;
        let initial_compute_port = initial_miner_port + config.miner_nodes.len() as u16;
        let initial_storage_port = initial_compute_port + config.compute_nodes.len() as u16;
        let ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));

        NetworkInstanceInfo {
            miner_nodes: config
                .miner_nodes
                .iter()
                .enumerate()
                .map(|(idx, name)| {
                    (
                        name.clone(),
                        SocketAddr::new(ip, initial_miner_port + idx as u16),
                    )
                })
                .collect(),
            compute_nodes: config
                .compute_nodes
                .iter()
                .enumerate()
                .map(|(idx, name)| {
                    (
                        name.clone(),
                        SocketAddr::new(ip, initial_compute_port + idx as u16),
                    )
                })
                .collect(),
            storage_nodes: config
                .storage_nodes
                .iter()
                .enumerate()
                .map(|(idx, name)| {
                    (
                        name.clone(),
                        SocketAddr::new(ip, initial_storage_port + idx as u16),
                    )
                })
                .collect(),
        }
    }

    async fn init_miners(config: &NetworkInstanceInfo) -> BTreeMap<String, MinerNode> {
        let mut map = BTreeMap::new();

        for (name, addr) in &config.miner_nodes {
            map.insert(name.clone(), MinerNode::new(addr.clone()).await.unwrap());
        }

        map
    }

    async fn init_storage(config: &NetworkInstanceInfo) -> BTreeMap<String, StorageNode> {
        let mut map = BTreeMap::new();

        for (name, addr) in &config.storage_nodes {
            let net_test: usize = 0;
            map.insert(
                name.clone(),
                StorageNode::new(addr.clone(), net_test).await.unwrap(),
            );
        }

        map
    }

    async fn init_compute(config: &NetworkInstanceInfo) -> BTreeMap<String, ComputeNode> {
        let mut map = BTreeMap::new();

        let compute_config = ComputeNodeConfig {
            compute_node_idx: 0,
            compute_nodes: config
                .compute_nodes
                .iter()
                .map(|(name, addr)| NodeSpec {
                    address: addr.clone(),
                })
                .collect(),
            storage_nodes: config
                .storage_nodes
                .iter()
                .map(|(name, addr)| NodeSpec {
                    address: addr.clone(),
                })
                .collect(),
        };

        for (idx, (name, addr)) in config.compute_nodes.iter().enumerate() {
            let compute_config = ComputeNodeConfig {
                compute_node_idx: idx,
                ..compute_config.clone()
            };
            map.insert(
                name.clone(),
                ComputeNode::new(compute_config).await.unwrap(),
            );
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
        None
    }
}
