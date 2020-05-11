//! This module provides a variety of utility functions to setup a test network,
//! to send a receive requests & responses, and generally to test the behavior and
//! correctness of the compute, miner, & storage modules.

use crate::compute::ComputeNode;
use crate::interfaces::{ComputeInterface, MinerInterface};
use crate::miner::MinerNode;
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
}

/// Represents a virtual network configuration.
/// Can be created using the builder or deserialized from JSON.
#[derive(Serialize, Deserialize)]
pub struct NetworkConfig {
    pub miner_nodes: Vec<String>,
    pub compute_nodes: Vec<String>,
}

impl Network {
    pub async fn create_from_config(mut config: NetworkConfig) -> Self {
        let ip_addr = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
        let miner_nodes = Self::init_miners(ip_addr, &mut config).await;
        let compute_nodes = Self::init_compute(ip_addr, &mut config).await;
        Self {
            miner_nodes,
            compute_nodes,
        }
    }

    async fn init_miners(ip: IpAddr, config: &mut NetworkConfig) -> BTreeMap<String, MinerNode> {
        let mut port = 10000;
        let mut map = BTreeMap::new();

        for name in config.miner_nodes.drain(..) {
            map.insert(
                name,
                MinerNode::new(SocketAddr::new(ip, port)).await.unwrap(),
            );
            port += 1;
        }

        map
    }

    async fn init_compute(ip: IpAddr, config: &mut NetworkConfig) -> BTreeMap<String, ComputeNode> {
        let mut port = 20000;
        let mut map = BTreeMap::new();

        for name in config.compute_nodes.drain(..) {
            map.insert(
                name,
                ComputeNode::new(SocketAddr::new(ip, port)).await.unwrap(),
            );
            port += 1;
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

    pub fn get_address(&mut self, name: &str) -> Option<SocketAddr> {
        if let Some(miner) = self.miner_nodes.get(name) {
            return Some(miner.address());
        }
        if let Some(compute) = self.compute_nodes.get(name) {
            return Some(compute.address());
        }
        None
    }
}
