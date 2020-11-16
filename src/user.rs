use crate::comms_handler::{CommsError, Event, Node};
use crate::interfaces::{
    CommMessage::HandshakeRequest, ComputeRequest, Contract, NodeType, Response, UseInterface,
    UserRequest,
};
use naom::primitives::asset::Asset;
use naom::primitives::transaction::{OutPoint, Transaction, TxConstructor, TxIn, TxOut};
use naom::primitives::transaction_utils::{
    construct_payment_tx, construct_payment_tx_ins, construct_tx_hash,
};
use naom::script::lang::Script;
use crate::wallet::generate_payment_address;
use bincode::deserialize;
use bytes::Bytes;
use sodiumoxide::crypto::sign::ed25519::PublicKey;
use std::collections::BTreeMap;
use std::{error::Error, fmt, net::SocketAddr};
use tokio::task;
use tracing::{debug, info_span, warn};

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
    pub assets: Vec<Asset>,
    pub amount: u64,
    pub next_payment: Option<Transaction>,
}

impl UserNode {
    pub async fn new(comms_address: SocketAddr) -> Result<UserNode> {
        Ok(UserNode {
            node: Node::new(comms_address, 2, NodeType::User).await?,
            assets: Vec::new(),
            amount: 0,
            next_payment: None,
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
    fn handle_request(&mut self, _peer: SocketAddr, req: UserRequest) -> Response {
        use UserRequest::*;
        match req {
            AdvertiseContract { contract, peers } => self.check_contract(contract, peers),
            SendAddressRequest => self.receive_payment_address_request(),
            SendPaymentAddress { address } => self.make_payment_transaction(address),
        }
    }

    /// Sends the next internal payment transaction to be processed by the connected Compute
    /// node
    ///
    /// ### Arguments
    ///
    /// * `compute_peer`    - Compute peer to send the payment tx to
    pub async fn send_next_payment_to_compute(&mut self, compute_peer: SocketAddr) -> Result<()> {
        let _peer_span = info_span!("sending next payment transaction for processing");
        let mut tx_to_send: BTreeMap<String, Transaction> = BTreeMap::new();
        let hash = construct_tx_hash(&self.next_payment.as_ref().unwrap());

        tx_to_send.insert(hash, self.next_payment.clone().unwrap());
        self.next_payment = None;

        self.node
            .send(
                compute_peer,
                ComputeRequest::SendTransactions {
                    transactions: tx_to_send,
                },
            )
            .await?;

        Ok(())
    }

    /// Creates a new payment transaction and assigns it as an internal attribute
    ///
    /// ### Arguments
    ///
    /// * `address` - Address to assign the payment transaction to
    pub fn make_payment_transaction(&mut self, address: String) -> Response {
        // let payment_tx = construct_payment_tx(
        //     tx_ins,
        //     address,
        //     None,
        //     None,
        //     Asset::Token(self.amount),
        //     self.amount,
        // );
        // self.next_payment = Some(payment_tx);

        Response {
            success: true,
            reason: "Next payment transaction successfully constructed",
        }
    }

    /// Sends a request for a payment address
    ///
    /// ### Arguments
    ///
    /// * `peer`    - Socket address of peer to request from
    pub async fn send_address_request(&mut self, peer: SocketAddr) -> Result<()> {
        let _peer_span = info_span!("sending payment address request");

        self.node
            .send(peer, UserRequest::SendAddressRequest)
            .await?;

        Ok(())
    }

    /// Sends a payment address from a request
    ///
    /// ### Arguments
    ///
    /// * `peer`    - Socket address of peer to send the address to
    pub async fn send_address_to_peer(&mut self, peer: SocketAddr) -> Result<()> {
        let address = generate_payment_address(0).await;
        println!("Address to send: {:?}", address);

        self.node
            .send(peer, UserRequest::SendPaymentAddress { address: address })
            .await?;
        Ok(())
    }
}

impl UseInterface for UserNode {
    fn check_contract<UserNode>(&self, _contract: Contract, _peers: Vec<UserNode>) -> Response {
        Response {
            success: false,
            reason: "Not implemented yet",
        }
    }

    fn receive_payment_address_request(&self) -> Response {
        Response {
            success: true,
            reason: "New address ready to be sent",
        }
    }
}
