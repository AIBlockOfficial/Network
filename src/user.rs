use crate::comms_handler::{CommsError, Event, Node};
use crate::configurations::UserNodeConfig;
use crate::constants::{ADDRESS_KEY, FUND_KEY, PEER_LIMIT, WALLET_PATH};
use crate::interfaces::{ComputeRequest, NodeType, Response, UseInterface, UserRequest};
use crate::utils::get_db_options;
use crate::wallet::{
    construct_address, generate_payment_address, save_address_to_wallet, save_payment_to_wallet,
    save_transactions_to_wallet, AddressStore, FundStore, TransactionStore,
};
use bincode::deserialize;
use bytes::Bytes;
use naom::primitives::asset::Asset;
use naom::primitives::transaction::{Transaction, TxConstructor, TxIn};
use naom::primitives::transaction_utils::{
    construct_payment_tx, construct_payment_tx_ins, construct_tx_hash,
};

use bincode::serialize;
use rocksdb::DB;
use sodiumoxide::crypto::sign;
use std::collections::BTreeMap;
use std::{error::Error, fmt, net::SocketAddr};
use tokio::task;
use tracing::{debug, info_span, warn};

/// Result wrapper for miner errors
pub type Result<T> = std::result::Result<T, UserError>;

#[derive(Debug)]
pub enum UserError {
    ConfigError(&'static str),
    Network(CommsError),
    AsyncTask(task::JoinError),
    Serialization(bincode::Error),
}

impl fmt::Display for UserError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConfigError(err) => write!(f, "Config error: {}", err),
            Self::Network(err) => write!(f, "Network error: {}", err),
            Self::AsyncTask(err) => write!(f, "Async task error: {}", err),
            Self::Serialization(err) => write!(f, "Serialization error: {}", err),
        }
    }
}

impl Error for UserError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ConfigError(_) => None,
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

#[derive(Debug, Clone)]
pub struct ReturnPayment {
    pub tx_in: TxIn,
    pub amount: u64,
    pub transaction: Transaction,
}

/// An instance of a MinerNode
#[derive(Debug)]
pub struct UserNode {
    node: Node,
    pub assets: Vec<Asset>,
    pub amount: u64,
    pub trading_peer: Option<SocketAddr>,
    pub next_payment: Option<Transaction>,
    pub return_payment: Option<ReturnPayment>,
}

impl UserNode {
    pub async fn new(config: UserNodeConfig) -> Result<UserNode> {
        let addr = config
            .user_nodes
            .get(config.user_node_idx)
            .ok_or(UserError::ConfigError("Invalid user index"))?
            .address;
        Ok(UserNode {
            node: Node::new(addr, PEER_LIMIT, NodeType::User).await?,
            assets: Vec::new(),
            amount: 0,
            trading_peer: None,
            next_payment: None,
            return_payment: None,
        })
    }

    /// Returns the miner node's public endpoint.
    pub fn address(&self) -> SocketAddr {
        self.node.address()
    }

    /// Connect to a peer on the network.
    pub async fn connect_to(&mut self, peer: SocketAddr) -> Result<()> {
        self.node.connect_to(peer).await?;
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
        println!("RECEIVED REQUEST: {:?}", req);

        match req {
            SendAddressRequest => self.receive_payment_address_request(peer),
            SendPaymentTransaction { transaction } => self.receive_payment_transaction(transaction),
            SendPaymentAddress { address } => self.make_payment_transactions(address),
        }
    }

    /// Sends the next internal payment transaction to be processed by the connected Compute
    /// node
    ///
    /// ### Arguments
    ///
    /// * `compute_peer`    - Compute peer to send the payment tx to
    /// * `payment_tx`      - Transaction to send
    pub async fn send_payment_to_compute(
        &mut self,
        compute_peer: SocketAddr,
        payment_tx: Transaction,
    ) -> Result<()> {
        let _peer_span = info_span!("sending payment transaction to compute node for processing");
        let mut tx_to_send: BTreeMap<String, Transaction> = BTreeMap::new();
        let hash = construct_tx_hash(&payment_tx);

        tx_to_send.insert(hash, payment_tx.clone());

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

    /// Receives a payment transaction to one of this user's addresses
    ///
    /// ### Arguments
    ///
    /// * `transaction` - Transaction to receive and save to wallet
    pub fn receive_payment_transaction(&mut self, transaction: Transaction) -> Response {
        let mut total_add = 0;
        let hash = construct_tx_hash(&transaction);

        for out in transaction.outputs {
            total_add += out.amount;
        }

        let _ = save_payment_to_wallet(hash, total_add);

        Response {
            success: true,
            reason: "Payment transaction received and saved successfully",
        }
    }

    /// Creates a new payment transaction and assigns it as an internal attribute
    ///
    /// ### Arguments
    ///
    /// * `address` - Address to assign the payment transaction to
    pub fn make_payment_transactions(&mut self, address: String) -> Response {
        let tx_ins = self.fetch_inputs_for_payment(self.amount);

        let payment_tx = construct_payment_tx(
            tx_ins,
            address,
            None,
            None,
            Asset::Token(self.amount),
            self.amount,
        );
        self.next_payment = Some(payment_tx);

        Response {
            success: true,
            reason: "Next payment transaction successfully constructed",
        }
    }

    /// Fetches valid TxIns based on the wallet's running total and available unspent
    /// transactions
    ///
    /// TODO: Replace errors here with Error enum types that the Result can return
    /// TODO: Possibly sort addresses found ascending, so that smaller amounts are consumed
    ///
    /// ### Arguments
    ///
    /// * `amount_required` - Amount needed
    pub fn fetch_inputs_for_payment(&mut self, amount_required: u64) -> Vec<TxIn> {
        let mut tx_ins = Vec::new();

        // Wallet DB handling
        let opts = get_db_options();
        let db = DB::open(&opts, WALLET_PATH).unwrap();
        let fund_store_state = match db.get(FUND_KEY) {
            Ok(Some(list)) => Some(deserialize(&list).unwrap()),
            Ok(None) => None,
            Err(e) => panic!("Failed to access the wallet database with error: {:?}", e),
        };

        if fund_store_state.is_none() {
            panic!("No funds available for payment!");
        }

        // At this point a valid fund store must exist
        let mut fund_store: FundStore = fund_store_state.unwrap();

        // Ensure we have enough funds to proceed with payment
        if fund_store.running_total < amount_required {
            panic!("Not enough funds available for payment!");
        }

        // Start fetching TxIns
        let mut amount_made = 0;
        let tx_hashes: Vec<_> = fund_store.transactions.keys().cloned().collect();

        // Start adding amounts to payment and updating FundStore
        for tx_hash in tx_hashes {
            let current_amount = fund_store.transactions.get(&tx_hash).unwrap();

            // If we've reached target
            if amount_made == amount_required {
                break;
            }
            // If we've overshot
            else if current_amount + amount_made > amount_required {
                let diff = amount_required - amount_made;

                fund_store.running_total -= current_amount;
                amount_made = amount_required;

                // Add a new return payment transaction
                let return_tx_in = self.construct_tx_in_from_prev_out(tx_hash.clone(), &db, false);
                self.return_payment = Some(ReturnPayment {
                    tx_in: return_tx_in,
                    amount: current_amount - diff,
                    transaction: Transaction::new(),
                });
            }
            // Else add to used stack
            else {
                amount_made += current_amount;
                fund_store.running_total -= current_amount;
            }

            // Add the new TxIn
            let tx_in = self.construct_tx_in_from_prev_out(tx_hash.clone(), &db, true);
            tx_ins.push(tx_in);

            fund_store.transactions.remove(&tx_hash);
        }

        // Save the updated fund store to disk
        db.put(FUND_KEY, Bytes::from(serialize(&fund_store).unwrap()))
            .unwrap();
        let _ = DB::destroy(&opts, WALLET_PATH);

        tx_ins
    }

    /// Constructs a return payment transaction for unspent tokens
    ///
    /// ### Arguments
    ///
    /// * `tx_hash`     - Hash of the output to create a return tx from
    /// * `return_amt`  - The amount to send to the return address
    pub async fn construct_return_payment_tx(
        &mut self,
        tx_in: TxIn,
        return_amt: u64,
    ) -> Result<()> {
        let tx_ins = vec![tx_in];
        let (pk, sk) = sign::gen_keypair();
        let address = construct_address(pk, 0);

        let key_store = AddressStore {
            public_key: pk,
            secret_key: sk,
        };
        let _ = save_address_to_wallet(address.clone(), key_store).await;

        let payment_tx = construct_payment_tx(
            tx_ins,
            address.clone(),
            None,
            None,
            Asset::Token(return_amt),
            return_amt,
        );

        let tx_store = TransactionStore { address, net: 0 };
        let mut tx_for_wallet = BTreeMap::new();
        tx_for_wallet.insert(construct_tx_hash(&payment_tx), tx_store);

        // Update saves to the wallet
        let _ = save_transactions_to_wallet(tx_for_wallet).await;
        let _ = save_payment_to_wallet(construct_tx_hash(&payment_tx), return_amt).await;

        // Completely reallocate the payment tx; required because an unwrap will just
        // consume self
        let mut current_r_payment = self.return_payment.clone().unwrap();
        current_r_payment.transaction = payment_tx;
        self.return_payment = Some(current_r_payment);

        Ok(())
    }

    /// Constructs a TxIn from a previous output
    ///
    /// ### Arguments
    ///
    /// * `tx_hash`     - Hash to the output to fetch
    /// * `output_vals` - Outpoint information required for TxIn
    /// * `db`          - Pointer to the wallet DB instance
    pub fn construct_tx_in_from_prev_out(
        &mut self,
        tx_hash: String,
        db: &DB,
        remove_from_wallet: bool,
    ) -> TxIn {
        let mut address_store: BTreeMap<String, AddressStore> = match db.get(ADDRESS_KEY) {
            Ok(Some(list)) => deserialize(&list).unwrap(),
            Ok(None) => panic!("No address store present in wallet"),
            Err(e) => panic!("Error accessing wallet: {:?}", e),
        };

        let tx_store: TransactionStore = match db.get(tx_hash.clone()) {
            Ok(Some(val)) => deserialize(&val).unwrap(),
            Ok(None) => panic!("Address for transaction not found in wallet"),
            Err(e) => panic!("Error accessing wallet: {:?}", e),
        };

        let needed_store: &AddressStore = address_store.get(&tx_store.address).unwrap();

        let pub_key = needed_store.public_key;
        let s_key = needed_store.secret_key.clone();
        let signature = sign::sign_detached(tx_hash.as_bytes(), &s_key);

        let tx_const = TxConstructor {
            t_hash: tx_hash.clone(),
            prev_n: 0,
            signatures: vec![signature],
            pub_keys: vec![pub_key],
        };

        if remove_from_wallet {
            // Update the values in the wallet
            db.delete(&tx_hash).unwrap();

            address_store.remove(&tx_store.address);
            db.put(ADDRESS_KEY, Bytes::from(serialize(&address_store).unwrap()))
                .unwrap();
        }

        let tx_ins = construct_payment_tx_ins(vec![tx_const]);

        tx_ins[0].clone()
    }

    /// Sends a payment transaction to the receiving party
    ///
    /// ### Arguments
    ///
    /// * `peer`        - Peer to send the transaction to
    /// * `transaction` - The transaction to be sent
    pub async fn send_payment_to_receiver(
        &mut self,
        peer: SocketAddr,
        transaction: Transaction,
    ) -> Result<()> {
        let _peer_span = info_span!("sending payment transaction to receiver");

        self.node
            .send(peer, UserRequest::SendPaymentTransaction { transaction })
            .await?;

        Ok(())
    }

    /// Sends a request for a payment address
    ///
    /// ### Arguments
    ///
    /// * `peer`    - Socket address of peer to request from
    pub async fn send_address_request(&mut self, peer: SocketAddr) -> Result<()> {
        let _peer_span = info_span!("sending payment address request");
        println!("Sending request for payment address to peer: {:?}", peer);

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
            .send(peer, UserRequest::SendPaymentAddress { address })
            .await?;
        Ok(())
    }
}

impl UseInterface for UserNode {
    fn receive_payment_address_request(&mut self, peer: SocketAddr) -> Response {
        self.trading_peer = Some(peer);

        Response {
            success: true,
            reason: "New address ready to be sent",
        }
    }
}
