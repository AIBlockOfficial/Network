use crate::comms_handler::{CommsError, Event, Node};
use crate::constants::{DB_PATH, DB_PATH_LIVE, DB_PATH_TEST, PEER_LIMIT};
use crate::interfaces::{
    Contract, NodeType, ProofOfWork, Response, StorageInterface, StorageRequest,
};
use crate::sha3::Digest;

use bincode::{deserialize, serialize};
use bytes::Bytes;
use rocksdb::{Options, DB};
use sha3::Sha3_256;
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use tracing::{debug, info_span, warn};

use naom::primitives::{block::Block, transaction::Transaction};

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
    block: Block,
    net: usize,
}

impl StorageNode {
    pub async fn new(address: SocketAddr, net: usize) -> Result<StorageNode> {
        Ok(StorageNode {
            node: Node::new(address, PEER_LIMIT, NodeType::Storage).await?,
            whitelisted: HashMap::new(),
            block: Block::new(),
            net: net,
        })
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
            SendBlock { block, tx } => self.receive_block(peer, block, tx),
            Store { incoming_contract } => self.receive_contracts(incoming_contract),
        }
    }
}

impl StorageInterface for StorageNode {
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

    fn receive_block(
        &mut self,
        peer: SocketAddr,
        block: Block,
        tx: BTreeMap<String, Transaction>,
    ) -> Response {
        if let Some(_) = self.whitelisted.get(&peer) {
            self.block = block;

            // TODO: Makes the DB save process async

            // Save the block
            let hash_input = Bytes::from(serialize(&self.block).unwrap());
            let hash_digest = Sha3_256::digest(&hash_input);
            let hash_key = hex::encode(hash_digest);
            let save_path = match self.net {
                0 => format!("{}/{}", DB_PATH, DB_PATH_TEST),
                _ => format!("{}/{}", DB_PATH, DB_PATH_LIVE),
            };

            let db = DB::open_default(save_path.clone()).unwrap();
            db.put(hash_key, hash_input).unwrap();

            // Save each transaction
            for (tx_hash, tx_value) in &tx {
                let tx_input = Bytes::from(serialize(tx_value).unwrap());
                db.put(tx_hash, tx_input).unwrap();
            }

            let _ = DB::destroy(&Options::default(), save_path.clone());

            return Response {
                success: true,
                reason: "Block received and added",
            };
        }

        Response {
            success: false,
            reason: "Peer not whitelisted to edit the chain",
        }
    }

    fn receive_contracts(&self, contract: Contract) -> Response {
        Response {
            success: false,
            reason: "Not implemented yet",
        }
    }
}
