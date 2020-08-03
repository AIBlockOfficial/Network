use crate::comms_handler::{CommsError, Event};
use crate::interfaces::ProofOfWork;
use crate::interfaces::{ComputeInterface, ComputeRequest, Contract, Response};
use crate::primitives::block::Block;
use crate::primitives::transaction::Transaction;
use crate::script::utils::tx_ins_are_valid;
use crate::unicorn::UnicornShard;
use crate::Node;

use bincode::deserialize;
use bytes::Bytes;
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use tracing::{debug, info, info_span, warn};

/// Result wrapper for compute errors
pub type Result<T> = std::result::Result<T, ComputeError>;

#[derive(Debug)]
pub enum ComputeError {
    Network(CommsError),
    Serialization(bincode::Error),
}

impl From<CommsError> for ComputeError {
    fn from(other: CommsError) -> Self {
        Self::Network(other)
    }
}

impl From<bincode::Error> for ComputeError {
    fn from(other: bincode::Error) -> Self {
        Self::Serialization(other)
    }
}

/// Limit for the number of peers a compute node may have
const PEER_LIMIT: usize = 6;

/// Limit for the number of PoWs a compute node may have for UnicornShard creation
const UNICORN_LIMIT: usize = 5;

/// Druid pool structure for checking and holding participants
#[derive(Debug, Clone)]
pub struct DruidPuddle {
    participants: usize,
    tx: Vec<Transaction>,
}

#[derive(Debug, Clone)]
pub struct ComputeNode {
    node: Node,
    pub tx_pool: Vec<Transaction>,
    pub druid_pool: BTreeMap<Vec<u8>, DruidPuddle>,
    pub current_block: Block,
    pub unicorn_list: HashMap<SocketAddr, UnicornShard>,
    pub unicorn_limit: usize,
}

impl ComputeNode {
    /// Returns the compute node's public endpoint.
    pub fn address(&self) -> SocketAddr {
        self.node.address()
    }

    /// Processes transaction for payment from user
    ///
    /// ### Arguments
    ///
    /// * `transaction` - Transaction to process
    pub fn process_p2pkh_tx(&mut self, transaction: Transaction) -> Response {
        if tx_ins_are_valid(transaction.clone().inputs) {
            self.current_block.transactions.push(transaction);

            return Response {
                success: true,
                reason: "Token payment made successfully",
            };
        }

        Response {
            success: false,
            reason: "You are not authorised to make this payment",
        }
    }

    /// Processes a dual double entry transaction
    ///
    /// ### Arguments
    ///
    /// * `transaction` - Transaction to process
    pub fn process_dde_tx(&mut self, transaction: Transaction) -> Response {
        if let Some(druid) = transaction.clone().druid {
            // If this transaction is meant to join others
            if self.druid_pool.contains_key(&druid) {
                self.process_tx_druid(druid, transaction);

                return Response {
                    success: true,
                    reason: "Transaction added to corresponding DRUID puddles",
                };

            // If we haven't seen this DRUID yet
            } else {
                let puddle = DruidPuddle {
                    participants: transaction.druid_participants.unwrap(),
                    tx: vec![transaction],
                };

                self.druid_pool.insert(druid, puddle);
                return Response {
                    success: true,
                    reason: "Transaction added to DRUID pool. Awaiting other parties",
                };
            }
        }

        Response {
            success: false,
            reason: "Dual double entry transaction doesn't contain a DRUID",
        }
    }

    /// Processes a dual double entry transaction's DRUID with the current pool
    ///
    /// ### Arguments
    ///
    /// * `transaction` - Transaction to process
    pub fn process_tx_druid(&mut self, druid: Vec<u8>, transaction: Transaction) {
        let mut current_puddle = self.druid_pool.get(&druid).unwrap().clone();
        current_puddle.tx.push(transaction);

        // Execute the tx if it's ready
        if current_puddle.tx.len() == current_puddle.participants {
            self.execute_dde_tx(current_puddle);
            let _removal = self.druid_pool.remove(&druid);
        }
    }

    /// Executes a waiting dual double entry transaction that is ready to execute
    ///
    /// ### Arguments
    ///
    /// * `puddle`  - DRUID puddle of transactions to execute
    pub fn execute_dde_tx(&mut self, puddle: DruidPuddle) {
        let mut txs_valid = true;

        for entry in &puddle.tx {
            if !tx_ins_are_valid(entry.inputs.clone()) {
                txs_valid = false;
                break;
            }
        }

        if txs_valid {
            self.current_block
                .transactions
                .append(&mut puddle.tx.clone());
            println!(
                "Transactions for dual double entry execution are valid. Adding to pending block"
            );
        } else {
            println!("Transactions for dual double entry execution are invalid");
        }
    }

    /// Processes the next batch of transactions from the floating tx pool
    /// to create the next block
    ///
    /// ### Arguments
    ///
    /// * `prev_hash`   - The hash of the previous block
    /// * `prev_time`   - The time of the previous block
    pub fn build_block(&mut self, prev_hash: Vec<u8>, prev_time: u32) {
        let mut next_block = Block::new();
        next_block.header.time = prev_time + 1;
        next_block.header.previous_hash = prev_hash;

        while !next_block.is_full() {
            if self.tx_pool.len() > 0 {
                let next_tx = self.tx_pool.remove(0);
                next_block.transactions.push(next_tx.clone());
            } else {
                println!("Tx pool is empty. Unable to build the next block");
                break;
            }
        }

        self.current_block = next_block;
    }

    /// Floods all peers with a PoW for UnicornShard creation
    /// TODO: Add in comms handling for sending and receiving requests
    ///
    /// ### Arguments
    ///
    /// * `address` - Address of the contributing node
    /// * `pow`     - PoW to flood
    pub fn flood_pow_to_peers(&self, _address: SocketAddr, _pow: &Vec<u8>) {
        println!("Flooding PoW to peers not implemented");
    }

    /// Floods all peers with a PoW commit for UnicornShard creation
    /// TODO: Add in comms handling for sending and receiving requests
    ///
    /// ### Arguments
    ///
    /// * `address` - Address of the contributing node
    /// * `pow`     - PoW to flood
    pub fn flood_commit_to_peers(&self, _address: SocketAddr, _commit: &ProofOfWork) {
        println!("Flooding commit to peers not implemented");
    }

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
            let req = deserialize::<ComputeRequest>(&frame).map_err(|error| {
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
    fn handle_request(&mut self, peer: SocketAddr, req: ComputeRequest) -> Response {
        use ComputeRequest::*;
        match req {
            SendPoW { pow } => self.receive_pow(peer, pow),
            SendTx { tx } => self.receive_transactions(tx),
        }
    }
}

impl ComputeInterface for ComputeNode {
    fn new(address: SocketAddr) -> ComputeNode {
        ComputeNode {
            node: Node::new(address, PEER_LIMIT),
            current_block: Block::new(),
            tx_pool: Vec::new(),
            unicorn_list: HashMap::new(),
            unicorn_limit: UNICORN_LIMIT,
            druid_pool: BTreeMap::new(),
        }
    }

    fn receive_commit(&mut self, address: SocketAddr, commit: ProofOfWork) -> Response {
        if let Some(entry) = self.unicorn_list.get_mut(&address) {
            if entry.is_valid(commit.clone()) {
                self.flood_commit_to_peers(address, &commit);
                return Response {
                    success: true,
                    reason: "Commit received successfully",
                };
            }

            return Response {
                success: false,
                reason: "Commit not valid. Rejecting...",
            };
        }

        Response {
            success: false,
            reason: "The node submitting a commit never submitted a promise",
        }
    }

    fn receive_pow(&mut self, address: SocketAddr, pow: Vec<u8>) -> Response {
        info!(?address, "Receieved PoW");

        if self.unicorn_list.len() < self.unicorn_limit {
            let mut unicorn_value = UnicornShard::new();
            unicorn_value.promise = pow.clone();

            self.unicorn_list.insert(address, unicorn_value);
            self.flood_pow_to_peers(address, &pow);

            return Response {
                success: true,
                reason: "Received PoW successfully",
            };
        }

        Response {
            success: false,
            reason: "UnicornShard limit reached. Unable to receive PoW",
        }
    }

    fn get_unicorn_table(&self) -> Vec<UnicornShard> {
        let mut unicorn_table = Vec::with_capacity(self.unicorn_list.len());

        for unicorn in self.unicorn_list.values() {
            unicorn_table.push(unicorn.clone());
        }

        unicorn_table
    }

    fn partition(&self, _uuids: Vec<&'static str>) -> Response {
        Response {
            success: false,
            reason: "Not implemented yet",
        }
    }

    fn get_service_levels(&self) -> Response {
        Response {
            success: false,
            reason: "Not implemented yet",
        }
    }

    fn receive_transactions(&mut self, transactions: Vec<Transaction>) -> Response {
        if self.tx_pool.len() + transactions.len() <= self.tx_pool.capacity() {
            self.tx_pool.append(&mut transactions.clone());

            return Response {
                success: true,
                reason: "Transactions added to current tx pool",
            };
        }

        Response {
            success: false,
            reason: "Transaction pool is full. Please push to next compute node",
        }
    }

    fn execute_contract(&self, _contract: Contract) -> Response {
        Response {
            success: false,
            reason: "Not implemented yet",
        }
    }

    fn get_next_block_reward(&self) -> f64 {
        0.0
    }
}
