use crate::comms_handler::{CommsError, Event, Node};
use crate::configurations::{ExtraNodeParams, UserNodeConfig, UserNodeSetup};
use crate::constants::PEER_LIMIT;
use crate::interfaces::{ComputeRequest, NodeType, Response, UseInterface, UserRequest};
use crate::transaction_gen::TransactionGen;
use crate::utils::{
    get_paiments_for_wallet, LocalEvent, LocalEventChannel, LocalEventSender, ResponseResult,
};
use crate::wallet::WalletDb;
use bincode::deserialize;
use bytes::Bytes;
use naom::primitives::asset::{Asset, TokenAmount};
use naom::primitives::block::Block;
use naom::primitives::transaction::{Transaction, TxIn, TxOut};
use naom::primitives::transaction_utils::{construct_payments_tx, construct_tx_hash};
use std::{error::Error, fmt, future::Future, net::SocketAddr};
use tokio::task;
use tracing::{debug, error, error_span, info, info_span, trace, warn};
use tracing_futures::Instrument;

/// Key for last pow coinbase produced
pub const TX_GENERATOR_KEY: &str = "TxGeneratorKey";

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
    pub amount: TokenAmount,
}

#[derive(Debug, Clone)]
pub struct ReturnPayment {
    pub tx_in: TxIn,
    pub amount: TokenAmount,
    pub transaction: Transaction,
}

/// An instance of a MinerNode
#[derive(Debug)]
pub struct UserNode {
    node: Node,
    wallet_db: WalletDb,
    local_events: LocalEventChannel,
    compute_addr: SocketAddr,
    api_addr: SocketAddr,
    assets: Vec<Asset>,
    trading_peer: Option<(SocketAddr, TokenAmount)>,
    next_payment: Option<(SocketAddr, Transaction)>,
    return_payment: Option<ReturnPayment>,
    last_block_notified: Block,
}

impl UserNode {
    ///Constructor to create a new UserNode
    ///
    /// ### Arguments
    ///
    /// * `config` - UserNodeConfig object containing UserNode parameters.
    /// * `extra`  - additional parameter for construction
    pub async fn new(config: UserNodeConfig, mut extra: ExtraNodeParams) -> Result<UserNode> {
        let addr = config
            .user_nodes
            .get(config.user_node_idx)
            .ok_or(UserError::ConfigError("Invalid user index"))?
            .address;
        let compute_addr = config
            .compute_nodes
            .get(config.user_compute_node_idx)
            .ok_or(UserError::ConfigError("Invalid compute index"))?
            .address;
        let api_addr = SocketAddr::new(addr.ip(), config.api_port);

        let node = Node::new(addr, PEER_LIMIT, NodeType::User).await?;
        let wallet_db = WalletDb::new(
            config.user_db_mode,
            extra.wallet_db.take(),
            config.passphrase,
        )
        .with_seed(config.user_node_idx, &config.user_wallet_seeds)
        .await;

        Ok(UserNode {
            node,
            wallet_db,
            local_events: Default::default(),
            compute_addr,
            api_addr,
            assets: Vec::new(),
            trading_peer: None,
            next_payment: None,
            return_payment: None,
            last_block_notified: Default::default(),
        })
    }

    /// Returns the node's public endpoint.
    pub fn address(&self) -> SocketAddr {
        self.node.address()
    }

    /// Returns the node's compute endpoint.
    pub fn compute_address(&self) -> SocketAddr {
        self.compute_addr
    }

    /// Connect to a peer on the network.
    ///
    /// ### Arguments
    ///
    /// * `peer` - Socket Address of the peer to connect to.
    pub async fn connect_to(&mut self, peer: SocketAddr) -> Result<()> {
        self.node.connect_to(peer).await?;
        Ok(())
    }

    /// Connect info for peers on the network.
    pub fn connect_info_peers(&self) -> (Node, Vec<SocketAddr>, Vec<SocketAddr>) {
        let compute = Some(self.compute_addr);
        let to_connect = compute.iter();
        let expect_connect = compute.iter();
        (
            self.node.clone(),
            to_connect.copied().collect(),
            expect_connect.copied().collect(),
        )
    }

    /// Info needed to run the API point.
    pub fn api_inputs(&self) -> (WalletDb, Node, SocketAddr) {
        (self.wallet_db.clone(), self.node.clone(), self.api_addr)
    }

    /// Extract persistent dbs
    pub async fn take_closed_extra_params(&mut self) -> ExtraNodeParams {
        ExtraNodeParams {
            wallet_db: Some(self.wallet_db.take_closed_persistent_store().await),
            ..Default::default()
        }
    }

    /// Listens for new events from peers and handles them, processing any errors.
    pub async fn handle_next_event_response(
        &mut self,
        setup: &UserNodeSetup,
        tx_generator: &mut TransactionGen,
        response: Result<Response>,
    ) -> ResponseResult {
        debug!("Response: {:?}", response);

        match response {
            Ok(Response {
                success: true,
                reason: "Shutdown",
            }) => {
                warn!("Shutdown now");
                return ResponseResult::Exit;
            }
            Ok(Response {
                success: true,
                reason: "New address ready to be sent",
            }) => {
                debug!("Sending new payment address");
                self.send_address_to_trading_peer().await.unwrap();
            }
            Ok(Response {
                success: true,
                reason: "Next payment transaction ready",
            }) => {
                self.send_next_payment_to_destinations(self.compute_address())
                    .await
                    .unwrap();
            }
            Ok(Response {
                success: true,
                reason: "Block mining notified",
            }) => {
                // Load any data after restart
                if !tx_generator.is_up_to_date_with_snapshot() {
                    if let Some(v) = self.wallet_db.get_db_value(TX_GENERATOR_KEY).await {
                        tx_generator.apply_snapshot_state(&v);
                    }
                }

                // Process committed transactions
                {
                    let txs = &self.last_block_notified.transactions;
                    info!("Block notified with txs: {}", txs.len());
                    tx_generator.commit_transactions(txs);
                }

                // Send next transactions
                let mut total_txs = 0;
                loop {
                    let txs_chunk_len = std::cmp::min(
                        setup.user_setup_tx_chunk_size.unwrap_or(usize::MAX),
                        setup.user_setup_tx_max_count.saturating_sub(total_txs),
                    );
                    let txs_chunk: Vec<_> = tx_generator
                        .make_all_transactions(setup.user_setup_tx_in_per_tx, txs_chunk_len)
                        .into_iter()
                        .map(|(_, tx)| tx)
                        .collect();

                    if txs_chunk.is_empty() {
                        break;
                    }

                    total_txs += txs_chunk.len();
                    debug!("New Generated txs:{:?}", txs_chunk);
                    if let Err(e) = self
                        .send_transactions_to_compute(self.compute_address(), txs_chunk)
                        .await
                    {
                        error!("Autogenerated tx not sent to {:?}", e);
                    }
                }
                if total_txs > 0 {
                    info!("New Generated txs sent: {}", total_txs);
                }

                self.wallet_db
                    .set_db_value(TX_GENERATOR_KEY, tx_generator.snapshot_state())
                    .await;
            }
            Ok(Response {
                success: true,
                reason,
            }) => {
                error!("UNHANDLED RESPONSE TYPE: {:?}", reason);
            }
            Ok(Response {
                success: false,
                reason,
            }) => {
                error!("WARNING: UNHANDLED RESPONSE TYPE FAILURE: {:?}", reason);
            }
            Err(error) => {
                panic!("ERROR HANDLING RESPONSE: {:?}", error);
            }
        };

        ResponseResult::Continue
    }

    /// Listens for new events from peers and handles them.
    /// The future returned from this function should be executed in the runtime. It will block execution.
    pub async fn handle_next_event<E: Future<Output = &'static str> + Unpin>(
        &mut self,
        exit: &mut E,
    ) -> Option<Result<Response>> {
        loop {
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
                Some(event) = self.local_events.rx.recv() => {
                    if let Some(res) = self.handle_local_event(event) {
                        return Some(Ok(res));
                    }
                }
                reason = &mut *exit => return Some(Ok(Response {
                    success: true,
                    reason,
                }))
            }
        }
    }

    /// Local event channel.
    pub fn local_event_tx(&self) -> &LocalEventSender {
        &self.local_events.tx
    }

    ///Handle a local event
    ///
    /// ### Arguments
    ///
    /// * `event` - Event to process.
    fn handle_local_event(&mut self, event: LocalEvent) -> Option<Response> {
        match event {
            LocalEvent::Exit(reason) => Some(Response {
                success: true,
                reason,
            }),
            LocalEvent::CoordinatedShutdown(_) => None,
            LocalEvent::Ignore => None,
        }
    }

    ///Passes a frame from an event to the handle_new_frame method
    ///
    /// ### Arguments
    ///
    /// * `event` - Event object holding the frame to be passed.
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
    ///
    /// ### Arguments
    ///
    /// * `peer` - Socket Address of the sending peer node.
    /// * `frame` - Byte object holding the frame being handled.
    async fn handle_new_frame(
        &mut self,
        peer: SocketAddr,
        frame: Bytes,
    ) -> Result<Option<Response>> {
        let req = deserialize::<UserRequest>(&frame).map_err(|error| {
            warn!(?error, "frame-deserialize");
            error
        })?;

        let req_span = error_span!("request", ?req);
        let response = self.handle_request(peer, req).instrument(req_span).await;
        trace!(?response, ?peer, "response");

        Ok(response)
    }

    /// Handles a compute request.
    ///
    /// ### Arguments
    ///
    /// * `peer` - SocketAddress holding the address of the peer sending the request.
    /// * `req` - UserRequest object containing the compute request.
    async fn handle_request(&mut self, peer: SocketAddr, req: UserRequest) -> Option<Response> {
        use UserRequest::*;
        trace!("handle_request: {:?}", req);

        match req {
            SendAddressRequest { amount } => {
                Some(self.receive_payment_address_request(peer, amount))
            }
            SendPaymentTransaction { transaction } => {
                Some(self.receive_payment_transaction(transaction).await)
            }
            SendPaymentAddress { address, amount } => {
                Some(self.make_payment_transactions(peer, address, amount).await)
            }
            BlockMining { block } => Some(self.notified_block_mining(peer, block)),
            Closing => self.receive_closing(peer),
        }
    }

    /// Handles the receipt of closing event
    ///
    /// ### Arguments
    ///
    /// * `peer`     - Sending peer's socket address
    fn receive_closing(&mut self, peer: SocketAddr) -> Option<Response> {
        if peer != self.compute_address() {
            return None;
        }

        Some(Response {
            success: true,
            reason: "Shutdown",
        })
    }

    /// Sends the next internal payment transaction to be processed by the connected Compute
    /// node
    ///
    /// ### Arguments
    ///
    /// * `compute_peer`    - Compute peer to send the payment tx to
    /// * `payment_tx`      - Transaction to send
    pub async fn send_next_payment_to_destinations(
        &mut self,
        compute_peer: SocketAddr,
    ) -> Result<()> {
        let (peer, tx) = self.next_payment.take().unwrap();

        self.send_transactions_to_compute(compute_peer, vec![tx.clone()])
            .await?;

        self.store_payment_transaction(tx.clone()).await;
        if peer != self.address() {
            self.send_payment_to_receiver(peer, tx).await?;
        }

        Ok(())
    }

    /// Sends the next internal payment transaction to be processed by the connected Compute
    /// node
    ///
    /// ### Arguments
    ///
    /// * `compute_peer` - Compute peer to send the payment tx to
    /// * `transactions` - Transactions to send
    pub async fn send_transactions_to_compute(
        &mut self,
        compute_peer: SocketAddr,
        transactions: Vec<Transaction>,
    ) -> Result<()> {
        let _peer_span = info_span!("sending transactions to compute node for processing");
        let transactions = transactions
            .into_iter()
            .map(construct_tx_hash_pair)
            .collect();

        self.node
            .send(
                compute_peer,
                ComputeRequest::SendTransactions { transactions },
            )
            .await?;

        Ok(())
    }

    /// Receives a payment transaction to one of this user's addresses
    ///
    /// ### Arguments
    ///
    /// * `transaction` - Transaction to receive and save to wallet
    pub async fn receive_payment_transaction(&mut self, transaction: Transaction) -> Response {
        self.store_payment_transaction(transaction).await;

        Response {
            success: true,
            reason: "Payment transaction received",
        }
    }

    /// Store payment transaction
    ///
    /// ### Arguments
    ///
    /// * `transaction` - Transaction to be received and saved to wallet
    pub async fn store_payment_transaction(&mut self, transaction: Transaction) {
        let hash = construct_tx_hash(&transaction);

        let payments = get_paiments_for_wallet(Some((&hash, &transaction)).into_iter());

        let our_payments = self
            .wallet_db
            .save_usable_payments_to_wallet(payments)
            .await
            .unwrap();
        debug!("store_payment_transactions: {:?}", our_payments);
    }

    /// Creates a new payment transaction and assigns it as an internal attribute
    /// Update wallet removing used transactions/addresses, adding return address
    ///
    /// ### Arguments
    ///
    /// * `peer` -  SocketAdress of the peer recieving the payment.
    /// * `address` - Address to assign the payment transaction to
    /// * `amount` - TokenAmount object of the price/amount payed
    pub async fn make_payment_transactions(
        &mut self,
        peer: SocketAddr,
        address: String,
        amount: TokenAmount,
    ) -> Response {
        let (tx_cons, total_amount, tx_used) =
            self.wallet_db.fetch_inputs_for_payment(amount).await;

        let mut tx_outs = vec![TxOut::new_amount(address, amount)];
        if total_amount > amount {
            let excess = total_amount - amount;
            let (excess_address, _) = self.wallet_db.generate_payment_address().await;
            tx_outs.push(TxOut::new_amount(excess_address, excess));
        }

        let tx_ins = self
            .wallet_db
            .consume_inputs_for_payment(tx_cons, tx_used)
            .await;
        let payment_tx = construct_payments_tx(tx_ins, tx_outs);
        self.next_payment = Some((peer, payment_tx));
        self.return_payment = None;

        Response {
            success: true,
            reason: "Next payment transaction ready",
        }
    }

    /// Sends a payment transaction to the receiving party
    ///
    /// ### Arguments
    ///
    /// * `peer`        - socket address of the peer to send the transaction to
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
    /// * `amount`    - Amount being payed
    pub async fn send_address_request(
        &mut self,
        peer: SocketAddr,
        amount: TokenAmount,
    ) -> Result<()> {
        let _peer_span = info_span!("sending payment address request");
        debug!("Sending request for payment address to peer: {:?}", peer);

        self.node
            .send(peer, UserRequest::SendAddressRequest { amount })
            .await?;

        Ok(())
    }

    /// Sends a block notification request to a Compute node
    ///
    /// ### Arguments
    ///
    /// * `compute`   - Socket address of recipient
    pub async fn send_block_notification_request(&mut self, compute: SocketAddr) -> Result<()> {
        let _peer_span = info_span!("sending block notification request");

        self.node
            .send(compute, ComputeRequest::SendUserBlockNotificationRequest)
            .await?;

        Ok(())
    }

    /// Sends a payment address from a request
    ///
    /// ### Arguments
    ///
    /// ///* `peer`    - Socket address of peer to send the address to
    pub async fn send_address_to_trading_peer(&mut self) -> Result<()> {
        let (peer, amount) = self.trading_peer.take().unwrap();
        let (address, _) = self.wallet_db.generate_payment_address().await;
        debug!("Address to send: {:?}", address);

        self.node
            .send(peer, UserRequest::SendPaymentAddress { address, amount })
            .await?;
        Ok(())
    }

    /// Received a mined block notification: allow to update pending transactions
    ///
    /// ### Arguments
    ///
    /// * `peer` -  SocketAdress of the peer notifying.
    /// * `block` - Block that is being mined and will be stored.
    pub fn notified_block_mining(&mut self, peer: SocketAddr, block: Block) -> Response {
        if peer == self.compute_addr {
            self.last_block_notified = block;

            Response {
                success: true,
                reason: "Block mining notified",
            }
        } else {
            Response {
                success: false,
                reason: "Invalid block mining notifier",
            }
        }
    }

    // Get the wallet db
    pub fn get_wallet_db(&self) -> &WalletDb {
        &self.wallet_db
    }

    // Get the last block notified to us
    pub fn get_last_block_notified(&self) -> &Block {
        &self.last_block_notified
    }
}

impl UseInterface for UserNode {
    fn receive_payment_address_request(
        &mut self,
        peer: SocketAddr,
        amount: TokenAmount,
    ) -> Response {
        self.trading_peer = Some((peer, amount));

        Response {
            success: true,
            reason: "New address ready to be sent",
        }
    }
}

/// Create the pair needed for transactions ordered containers
fn construct_tx_hash_pair(tx: Transaction) -> (String, Transaction) {
    let hash = construct_tx_hash(&tx);
    (hash, tx)
}
