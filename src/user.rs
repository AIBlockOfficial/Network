use crate::comms_handler::{CommsError, Event, Node};
use crate::interfaces::{
    Asset, CommMessage::HandshakeRequest, Contract, NodeType, Response, UseInterface, UserRequest,
};
use naom::primitives::transaction::{OutPoint, Transaction, TxConstructor, TxIn, TxOut};
use naom::script::lang::Script;

use bincode::deserialize;
use bytes::Bytes;
use sodiumoxide::crypto::sign::ed25519::PublicKey;
use std::{error::Error, fmt, net::SocketAddr};
use tokio::{sync::RwLock, task};
use tracing::{debug, info, info_span, warn};

/// Result wrapper for miner errors
pub type Result<T> = std::result::Result<T, UserError>;

#[derive(Debug)]
pub enum UserError {
    Network(CommsError),
    AsyncTask(task::JoinError),
    Serialization(bincode::Error),
}

impl fmt::Display for UserError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UserError::Network(err) => write!(f, "Network error: {}", err),
            UserError::AsyncTask(err) => write!(f, "Async task error: {}", err),
            UserError::Serialization(err) => write!(f, "Serialization error: {}", err),
        }
    }
}

impl Error for UserError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Network(ref e) => Some(e),
            Self::Serialization(ref e) => Some(e),
            Self::AsyncTask(ref e) => Some(e),
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

impl From<bincode::Error> for UserError {
    fn from(other: bincode::Error) -> Self {
        Self::Serialization(other)
    }
}

/// A structure for an asset to send, along with its quantity
#[derive(Debug, Clone)]
pub struct AssetInTransit {
    pub asset: Asset,
    pub amount: u64,
}

/// An instance of a MinerNode
#[derive(Debug, Clone)]
pub struct UserNode {
    node: Node,
    assets: Vec<Asset>,
    network: usize,
}

impl UserNode {
    pub async fn new(address: SocketAddr, network: usize) -> Result<UserNode> {
        Ok(UserNode {
            node: Node::new(address, 2, NodeType::User).await?,
            assets: Vec::new(),
            network: network,
        })
    }

    /// Returns the miner node's public endpoint.
    pub fn address(&self) -> SocketAddr {
        self.node.address()
    }

    /// Connect to a peer on the network.
    pub async fn connect_to(&mut self, peer: SocketAddr) -> Result<()> {
        self.node.connect_to(peer).await?;
        self.node
            .send(
                peer,
                HandshakeRequest {
                    node_type: NodeType::Miner,
                    public_address: self.node.address(),
                },
            )
            .await?;
        Ok(())
    }

    /// Listens for new events from peers and handles them.
    /// The future returned from this function should be executed in the runtime. It will block execution.
    pub async fn handle_next_event(&mut self) -> Option<Result<Response>> {
        let event = self.node.next_event().await?;
        self.handle_event(event).await.into()
    }

    async fn handle_event(&mut self, event: Event) -> Result<Response> {
        match event {
            Event::NewFrame { peer, frame } => Ok(self.handle_new_frame(peer, frame).await?),
        }
    }

    /// Hanldes a new incoming message from a peer.
    async fn handle_new_frame(&mut self, peer: SocketAddr, frame: Bytes) -> Result<Response> {
        info_span!("peer", ?peer).in_scope(|| {
            let req = deserialize::<UserRequest>(&frame).map_err(|error| {
                warn!(?error, "frame-deserialize");
                error
            })?;

            info_span!("request", ?req).in_scope(|| {
                let response = self.handle_request(peer, req);
                debug!(?response, ?peer, "response");

                Ok(response)
            })
        })
    }

    /// Handles a compute request.
    fn handle_request(&mut self, peer: SocketAddr, req: UserRequest) -> Response {
        use UserRequest::*;
        match req {
            AdvertiseContract { contract, peers } => self.check_contract(contract, peers),
            SendAssets { assets } => self.receive_assets(assets),
        }
    }
}

impl UseInterface for UserNode {
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
