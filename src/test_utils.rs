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
    pub fn create_from_config(mut config: NetworkConfig) -> Self {
        let ip_addr = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
        let miner_nodes = Self::init_miners(ip_addr, &mut config);
        let compute_nodes = Self::init_compute(ip_addr, &mut config);
        Self {
            miner_nodes,
            compute_nodes,
        }
    }

    fn init_miners(ip: IpAddr, config: &mut NetworkConfig) -> BTreeMap<String, MinerNode> {
        let mut port = 10000;
        let mut map = BTreeMap::new();

        for name in config.miner_nodes.drain(..) {
            info_span!("miner", id = field::display(name.clone())).in_scope(|| {
                map.insert(name, MinerNode::new(SocketAddr::new(ip, port)));
            });
            port += 1;
        }

        map
    }

    fn init_compute(ip: IpAddr, config: &mut NetworkConfig) -> BTreeMap<String, ComputeNode> {
        let mut port = 20000;
        let mut map = BTreeMap::new();

        for name in config.compute_nodes.drain(..) {
            info_span!("compute", id = field::display(name.clone())).in_scope(|| {
                map.insert(name, ComputeNode::new(SocketAddr::new(ip, port)));
            });
            port += 1;
        }

        map
    }

    /// Starts all nodes.
    pub fn start(&mut self) {
        for (name, compute) in self.compute_nodes.iter_mut() {
            let mut cnode = compute.clone();

            info_span!("compute", id = field::display(name)).in_scope(|| {
                tokio::spawn(
                    async move {
                        let _ = cnode.start().await;
                    }
                    .in_current_span(),
                );
            });
        }
        for (name, mut miner) in self.miner_nodes.iter_mut() {
            info_span!("miner", id = field::display(name)).in_scope(|| {
                Self::start_miner(&mut miner);
            });
        }
        trace!("Started network");
    }

    fn start_miner(miner: &mut MinerNode) -> JoinHandle<()> {
        let mut miner = miner.clone();

        tokio::spawn(
            async move {
                match miner.start().await {
                    Ok(()) => (),
                    Err(error) => error!(error = field::display(error), "start"),
                }
            }
            .in_current_span(),
        )
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
