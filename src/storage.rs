use crate::comms_handler::{CommsError, Event, Node};
use crate::configurations::{DbMode, StorageNodeConfig};
use crate::constants::{DB_PATH, DB_PATH_LIVE, DB_PATH_TEST, PEER_LIMIT};
use crate::interfaces::{
    BlockStoredInfo, ComputeRequest, Contract, NodeType, ProofOfWork, Response, StorageInterface,
    StorageRequest,
};
use crate::storage_raft::{CompleteBlock, StorageRaft};
use crate::utils::{get_db_options, loop_connnect_to_peers_async};
use bincode::{deserialize, serialize};
use bytes::Bytes;
use naom::primitives::{block::Block, transaction::Transaction};
use rocksdb::DB;
use serde::Serialize;
use sha3::Digest;
use sha3::Sha3_256;
use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::net::SocketAddr;
use tracing::{error_span, info, trace, warn};
use tracing_futures::Instrument;

/// Result wrapper for compute errors
pub type Result<T> = std::result::Result<T, StorageError>;

#[derive(Debug)]
pub enum StorageError {
    ConfigError(&'static str),
    Network(CommsError),
    Serialization(bincode::Error),
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConfigError(err) => write!(f, "Config error: {}", err),
            Self::Network(err) => write!(f, "Network error: {}", err),
            Self::Serialization(err) => write!(f, "Serialization error: {}", err),
        }
    }
}

impl Error for StorageError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ConfigError(_) => None,
            Self::Network(ref e) => Some(e),
            Self::Serialization(ref e) => Some(e),
        }
    }
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

#[derive(Debug)]
pub struct StorageNode {
    node: Node,
    node_raft: StorageRaft,
    compute_addr: SocketAddr,
    whitelisted: HashMap<SocketAddr, bool>,
    db_mode: DbMode,
    last_block_stored: Option<(CompleteBlock, BlockStoredInfo)>,
}

impl StorageNode {
    pub async fn new(config: StorageNodeConfig) -> Result<StorageNode> {
        let addr = config
            .storage_nodes
            .get(config.storage_node_idx)
            .ok_or(StorageError::ConfigError("Invalid storage index"))?
            .address;
        let compute_addr = config
            .compute_nodes
            .get(config.storage_node_idx)
            .ok_or(StorageError::ConfigError("Invalid compute index"))?
            .address;

        Ok(StorageNode {
            node: Node::new(addr, PEER_LIMIT, NodeType::Storage).await?,
            node_raft: StorageRaft::new(&config),
            compute_addr,
            whitelisted: HashMap::new(),
            db_mode: config.storage_db_mode,
            last_block_stored: None,
        })
    }

    /// Returns the compute node's public endpoint.
    pub fn address(&self) -> SocketAddr {
        self.node.address()
    }

    pub fn inject_next_event(
        &self,
        from_peer_addr: SocketAddr,
        data: impl Serialize,
    ) -> Result<()> {
        Ok(self.node.inject_next_event(from_peer_addr, data)?)
    }

    /// Connect to a raft peer on the network.
    pub fn connect_to_raft_peers(&self) -> impl Future<Output = Result<()>> {
        loop_connnect_to_peers_async(
            self.node.clone(),
            self.node_raft.raft_peer_to_connect().cloned().collect(),
        )
    }

    /// Return the raft loop to spawn in it own task.
    pub fn raft_loop(&self) -> impl Future<Output = ()> {
        self.node_raft.raft_loop()
    }

    /// Signal to the raft loop to complete
    pub async fn close_raft_loop(&mut self) {
        self.node_raft.close_raft_loop().await
    }

    /// Listens for new events from peers and handles them.
    /// The future returned from this function should be executed in the runtime. It will block execution.
    pub async fn handle_next_event(&mut self) -> Option<Result<Response>> {
        loop {
            // Process pending submission.
            self.node_raft.propose_received_part_block().await;

            // State machines are not keept between iterations or calls.
            // All selection calls (between = and =>), need to be dropable
            // i.e they should only await a channel.
            tokio::select! {
                event = self.node.next_event() => {
                    trace!("handle_next_event evt {:?}", event);
                    if let res @ Some(_) = self.handle_event(event?).await.transpose() {
                        return res;
                    }
                }
                Some(commit_data) = self.node_raft.next_commit() => {
                    trace!("handle_next_event commit {:?}", commit_data);
                    if self.node_raft.received_commit(commit_data).await.is_some() {
                        let block = self.node_raft.generate_complete_block();
                        self.store_complete_block(block);

                        return Some(Ok(Response{
                            success: true,
                            reason: "Block complete stored",
                        }));
                    }
                }
                Some((addr, msg)) = self.node_raft.next_msg() => {
                    trace!("handle_next_event msg {:?}: {:?}", addr, msg);
                    match self.node.send(
                        addr,
                        StorageRequest::SendRaftCmd(msg)).await {
                            Err(e) => info!("Msg not sent to {}, from {}: {:?}", addr, self.address(), e),
                            Ok(()) => trace!("Msg sent to {}, from {}", addr, self.address()),
                        };

                }
                Some(_) = self.node_raft.timeout_propose_block() => {
                    trace!("handle_next_event timeout block");
                    self.node_raft.propose_block_at_timeout().await;
                }
            }
        }
    }

    async fn handle_event(&mut self, event: Event) -> Result<Option<Response>> {
        match event {
            Event::NewFrame { peer, frame } => {
                let peer_span = error_span!("peer", ?peer);
                self.handle_new_frame(peer, frame)
                    .instrument(peer_span)
                    .await
            }
        }
    }

    /// Hanldes a new incoming message from a peer.
    async fn handle_new_frame(
        &mut self,
        peer: SocketAddr,
        frame: Bytes,
    ) -> Result<Option<Response>> {
        let req = deserialize::<StorageRequest>(&frame).map_err(|error| {
            warn!(?error, "frame-deserialize");
            error
        })?;

        let req_span = error_span!("request", ?req);
        let response = self.handle_request(peer, req).instrument(req_span).await;
        trace!(?response, ?peer, "response");

        Ok(response)
    }

    /// Handles a compute request.
    async fn handle_request(&mut self, peer: SocketAddr, req: StorageRequest) -> Option<Response> {
        use StorageRequest::*;
        match req {
            GetHistory {
                start_time,
                end_time,
            } => Some(self.get_history(&start_time, &end_time)),
            GetUnicornTable { n_last_items } => Some(self.get_unicorn_table(n_last_items)),
            SendPow { pow } => Some(self.receive_pow(pow)),
            SendBlock { block, tx } => Some(self.receive_block(peer, block, tx)),
            Store { incoming_contract } => Some(self.receive_contracts(incoming_contract)),
            SendRaftCmd(msg) => {
                self.node_raft.received_message(msg).await;
                None
            }
        }
    }

    fn store_complete_block(&mut self, mut complete: CompleteBlock) {
        // TODO: Makes the DB save process async
        // TODO: only accept whitelisted blocks

        if complete.common.block.header.b_num == 0 {
            // No mining on first block
            complete.per_node.clear();
        }

        // Save the complete block
        trace!("Store complete block: {:?}", complete);
        let hash_input = Bytes::from(serialize(&complete).unwrap());
        let hash_digest = Sha3_256::digest(&hash_input);
        let hash_key = hex::encode(hash_digest);
        let save_path = match self.db_mode {
            DbMode::Live => format!("{}/{}", DB_PATH, DB_PATH_LIVE),
            DbMode::Test(idx) => format!("{}/{}.{}", DB_PATH, DB_PATH_TEST, idx),
            _ => panic!("unimplemented db mode"),
        };

        let opts = get_db_options();
        let db = DB::open(&opts, save_path.clone()).unwrap();
        db.put(hash_key.clone(), hash_input).unwrap();

        // Save each transaction
        for (tx_hash, tx_value) in &complete.common.block_txs {
            let tx_input = Bytes::from(serialize(tx_value).unwrap());
            db.put(tx_hash, tx_input).unwrap();
        }

        let stored_info = BlockStoredInfo {
            block_hash: hash_key,
            block_num: complete.common.block.header.b_num,
            mining_transactions: BTreeMap::new(),
        };

        self.last_block_stored = Some((complete, stored_info));

        let _ = DB::destroy(&opts, save_path);
    }

    pub fn get_last_block_stored(&self) -> &Option<(CompleteBlock, BlockStoredInfo)> {
        &self.last_block_stored
    }

    /// Sends the latest block to storage
    pub async fn send_stored_block(&mut self) -> Result<()> {
        // Only the first call will send to storage.
        let block = self.last_block_stored.as_ref().unwrap().1.clone();

        self.node
            .send(self.compute_addr, ComputeRequest::SendBlockStored(block))
            .await?;

        Ok(())
    }
}

impl StorageInterface for StorageNode {
    fn get_history(&self, _start_time: &u64, _end_time: &u64) -> Response {
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

    fn get_unicorn_table(&self, _n_last_items: Option<u64>) -> Response {
        Response {
            success: false,
            reason: "Not implemented yet",
        }
    }

    fn receive_pow(&self, _pow: ProofOfWork) -> Response {
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
        self.node_raft.append_to_our_blocks(peer, block, tx);

        Response {
            success: true,
            reason: "Block received to be added",
        }
    }

    fn receive_contracts(&self, _contract: Contract) -> Response {
        Response {
            success: false,
            reason: "Not implemented yet",
        }
    }
}
