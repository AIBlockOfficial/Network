use crate::comms_handler::CommsError;
use crate::interfaces::{
    Asset, Contract, HandshakeRequest, NodeType, Response, UseInterface, UserRequest,
};
use crate::sha3::Digest;
use crate::Node;

use rand;
use sha3::Sha3_256;
use std::{fmt, net::SocketAddr, sync::Arc};
use tokio::{sync::RwLock, task};

/// Result wrapper for miner errors
pub type Result<T> = std::result::Result<T, UserError>;

#[derive(Debug)]
pub enum UserError {
    Network(CommsError),
    AsyncTask(task::JoinError),
}

impl fmt::Display for UserError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UserError::Network(err) => write!(f, "Network error: {}", err),
            UserError::AsyncTask(err) => write!(f, "Async task error: {}", err),
        }
    }
}

impl From<CommsError> for UserError {
    fn from(other: CommsError) -> Self {
        Self::Network(other)
    }
}

impl From<task::JoinError> for UserError {
    fn from(other: task::JoinError) -> Self {
        Self::AsyncTask(other)
    }
}

// An instance of a MinerNode
#[derive(Debug, Clone)]
pub struct UserNode {
    node: Node,
    assets: Vec<Asset>,
}

impl UserNode {
    /// Returns the miner node's public endpoint.
    pub fn address(&self) -> SocketAddr {
        self.node.address()
    }

    /// Start the compute node on the network.
    pub async fn start(&mut self) -> Result<()> {
        Ok(self.node.listen().await?)
    }

    /// Connect to a peer on the network.
    pub async fn connect_to(&mut self, peer: SocketAddr) -> Result<()> {
        self.node.connect_to(peer).await?;
        self.node
            .send(
                peer,
                HandshakeRequest {
                    node_type: NodeType::Miner,
                },
            )
            .await?;
        Ok(())
    }
}

impl UseInterface for UserNode {
    fn new(address: SocketAddr) -> UserNode {
        UserNode {
            node: Node::new(address, 2),
            assets: Vec::new(),
        }
    }

    fn check_contract<UserNode>(&self, contract: Contract, peers: Vec<UserNode>) -> Response {
        Response {
            success: false,
            reason: "Not implemented yet",
        }
    }

    fn receive_assets(&mut self, assets: Vec<Asset>) -> Response {
        self.assets = assets;

        Response {
            success: true,
            reason: "Successfully received assets",
        }
    }
}
