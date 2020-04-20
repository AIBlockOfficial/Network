use crate::comms_handler::{CommsError, Event};
use crate::interfaces::ProofOfWork;
use crate::interfaces::{Block, Contract, Response, StorageInterface, StorageRequest, Tx};
use crate::unicorn::UnicornShard;
use crate::Node;
use bincode::deserialize;
use bytes::Bytes;
use std::collections::HashMap;
use std::net::SocketAddr;
use tracing::{debug, info, info_span, warn};

/// Result wrapper for compute errors
pub type Result<T> = std::result::Result<T, StorageError>;

#[derive(Debug)]
pub enum StorageError {
    Network(CommsError),
    Serialization(bincode::Error),
}

impl From<CommsError> for StorageError {
    fn from(other: CommsError) -> Self {
        Self::Network(other)
    }
}

impl From<bincode::Error> for StorageError {
    fn from(other: bincode::Error) -> Self {
        Self::Serialization(other)
    }
}

#[derive(Debug, Clone)]
pub struct StorageNode {
    node: Node,
    whitelisted: HashMap<SocketAddr, bool>,
    pre_block: Block,
}

impl StorageNode {
    /// Start the compute node on the network.
    pub async fn start(&mut self) -> Result<()> {
        self.node.listen().await?;
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
            let req = deserialize::<StorageRequest>(&frame).map_err(|error| {
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
    fn handle_request(&mut self, peer: SocketAddr, req: StorageRequest) -> Response {
        use StorageRequest::*;
        match req {
            GetHistory {
                start_time,
                end_time,
            } => self.get_history(&start_time, &end_time),
            GetUnicornTable { n_last_items } => self.get_unicorn_table(n_last_items),
            SendPow { pow } => self.receive_pow(pow),
            SendPreBlock { pre_block } => self.receive_pre_block(pre_block),
            Store { incoming_contract } => self.receive_contracts(incoming_contract),
        }
    }
}

impl StorageInterface for StorageNode {
    fn new(address: SocketAddr) -> StorageNode {
        StorageNode {
            node: Node::new(address, 6),
            whitelisted: HashMap::new(),
            pre_block: Block,
        }
    }

    fn get_history(&self, start_time: &u64, end_time: &u64) -> Response {
        Response {
            success: false,
            reason: "Not implemented yet",
        }
    }

    fn whitelist(&mut self, address: SocketAddr) -> Response {
        self.whitelisted.insert(address, true);

        Response {
            success: true,
            reason: "Address added to whitelist",
        }
    }

    fn get_unicorn_table(&self, n_last_items: Option<u64>) -> Response {
        Response {
            success: false,
            reason: "Not implemented yet",
        }
    }

    fn receive_pow(&self, pow: ProofOfWork) -> Response {
        Response {
            success: false,
            reason: "Not implemented yet",
        }
    }

    fn receive_pre_block(&mut self, pre_block: Block) -> Response {
        self.pre_block = pre_block;

        Response {
            success: true,
            reason: "Pre-block received and added",
        }
    }

    fn receive_contracts(&self, contract: Contract) -> Response {
        Response {
            success: false,
            reason: "Not implemented yet",
        }
    }
}
