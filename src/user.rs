use crate::comms_handler::{CommsError, Event, Node};
use crate::configurations::{ExtraNodeParams, UserAutoGenTxSetup, UserNodeConfig};
use crate::constants::PEER_LIMIT;
use crate::interfaces::{
    ComputeRequest, NodeType, Response, UserApiRequest, UserRequest, UtxoFetchType, UtxoSet,
};
use crate::transaction_gen::TransactionGen;
use crate::utils::{
    get_paiments_for_wallet, get_paiments_for_wallet_from_utxo, LocalEvent, LocalEventChannel,
    LocalEventSender, ResponseResult,
};
use crate::wallet::WalletDb;
use bincode::deserialize;
use bytes::Bytes;
use naom::primitives::asset::TokenAmount;
use naom::primitives::block::Block;
use naom::primitives::transaction::{OutPoint, Transaction, TxOut};
use naom::utils::transaction_utils::{construct_tx_core, construct_tx_hash};
use std::{collections::BTreeMap, error::Error, fmt, future::Future, net::SocketAddr};
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

/// Generates transactions
#[derive(Debug)]
pub struct AutoGenTx {
    tx_generator: TransactionGen,
    tx_chunk_size: Option<usize>,
    tx_in_per_tx: Option<usize>,
    tx_max_count: usize,
}

/// info for a pending paiment
#[derive(Debug)]
pub struct PendingPayment {
    amount: TokenAmount,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutoDonate {
    Disabled,
    Enabled(TokenAmount),
}

/// An instance of a UserNode
#[derive(Debug)]
pub struct UserNode {
    node: Node,
    wallet_db: WalletDb,
    local_events: LocalEventChannel,
    compute_addr: SocketAddr,
    api_addr: SocketAddr,
    trading_peer: Option<SocketAddr>,
    next_payment: Option<(Option<SocketAddr>, Transaction)>,
    last_block_notified: Block,
    test_auto_gen_tx: Option<AutoGenTx>,
    received_utxo_set: Option<UtxoSet>,
    pending_payments: (BTreeMap<SocketAddr, PendingPayment>, AutoDonate),
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
        let api_addr = SocketAddr::new(addr.ip(), config.user_api_port);

        let node = Node::new(addr, PEER_LIMIT, NodeType::User).await?;
        let wallet_db = WalletDb::new(
            config.user_db_mode,
            extra.wallet_db.take(),
            config.passphrase,
        )
        .with_seed(config.user_node_idx, &config.user_wallet_seeds)
        .await;

        let pending_payments = match config.user_auto_donate {
            0 => (Default::default(), AutoDonate::Disabled),
            amount => (Default::default(), AutoDonate::Enabled(TokenAmount(amount))),
        };

        let test_auto_gen_tx =
            make_transaction_gen(config.user_test_auto_gen_setup, config.user_node_idx);

        Ok(UserNode {
            node,
            wallet_db,
            local_events: Default::default(),
            compute_addr,
            api_addr,
            trading_peer: None,
            next_payment: None,
            last_block_notified: Default::default(),
            test_auto_gen_tx,
            received_utxo_set: None,
            pending_payments,
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

    /// Check if auto generator is active
    pub fn is_test_auto_gen_tx_active(&self) -> bool {
        self.test_auto_gen_tx.is_some()
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
        let wallet_db = self.wallet_db.take_closed_persistent_store().await;
        ExtraNodeParams {
            wallet_db: wallet_db.in_memory(),
            ..Default::default()
        }
    }

    /// Update the running total from a retrieved UTXO set/subset
    pub async fn update_running_total(&mut self) {
        let utxo_set = self.received_utxo_set.take();
        let payments: Vec<(OutPoint, TokenAmount, String)> =
            get_paiments_for_wallet_from_utxo(utxo_set.into_iter().flatten());

        self.wallet_db
            .save_usable_payments_to_wallet(payments)
            .await
            .unwrap();
    }

    /// Listens for new events from peers and handles them, processing any errors.
    pub async fn handle_next_event_response(
        &mut self,
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
                success: false,
                reason: "Ignore unexpected transaction",
            }) => {}
            Ok(Response {
                success: true,
                reason: "Block mining notified",
            }) => {
                self.process_mining_notified().await;
            }
            Ok(Response {
                success: true,
                reason: "Request UTXO set",
            }) => {}
            Ok(Response {
                success: true,
                reason: "Received UTXO set",
            }) => {
                self.update_running_total().await;
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

    /// Send initial requests:
    /// - block notification request (if active)
    pub async fn send_startup_requests(&mut self) -> Result<()> {
        if self.is_test_auto_gen_tx_active() {
            info!("Send startup requets: block notification");
            return self.send_block_notification_request().await;
        }
        Ok(())
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

    /// Handles a request.
    ///
    /// ### Arguments
    ///
    /// * `peer` - Peer sending the request.
    /// * `req`  - Request to execute
    async fn handle_request(&mut self, peer: SocketAddr, req: UserRequest) -> Option<Response> {
        use UserRequest::*;
        trace!("handle_request: {:?}", req);

        match req {
            UserApi(req) => self.handle_api_request(peer, req).await,
            SendUtxoSet { utxo_set } => Some(self.receive_utxo_set(utxo_set)),
            SendAddressRequest => Some(self.receive_payment_address_request(peer)),
            SendPaymentTransaction { transaction } => {
                Some(self.receive_payment_transaction(transaction).await)
            }
            SendPaymentAddress { address } => {
                self.process_pending_payment_transactions(peer, address)
                    .await
            }
            BlockMining { block } => Some(self.notified_block_mining(peer, block)),
            Closing => self.receive_closing(peer),
        }
    }

    /// Handles an API internal request.
    ///
    /// ### Arguments
    ///
    /// * `peer` - Peer sending the request.
    /// * `req`  - Request to execute
    async fn handle_api_request(
        &mut self,
        peer: SocketAddr,
        req: UserApiRequest,
    ) -> Option<Response> {
        use UserApiRequest::*;

        if peer != self.address() {
            // Do not process if not internal request
            return None;
        }

        match req {
            UpdateWalletFromUtxoSet { address_list } => {
                self.request_utxo_set_for_wallet_update(address_list).await
            }
            MakeIpPayment {
                payment_peer,
                amount,
            } => {
                self.request_payment_address_for_peer(payment_peer, amount)
                    .await
            }
            MakePayment { address, amount } => {
                Some(self.make_payment_transactions(None, address, amount).await)
            }
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

    pub async fn send_request_utxo_set(&mut self, address_list: UtxoFetchType) -> Result<()> {
        self.node
            .send(
                self.compute_address(),
                ComputeRequest::SendUtxoRequest { address_list },
            )
            .await?;
        Ok(())
    }

    pub fn get_received_utxo(&self) -> Option<UtxoSet> {
        self.received_utxo_set.clone()
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
        if let Some(peer) = peer {
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
        self.node
            .send(
                compute_peer,
                ComputeRequest::SendTransactions { transactions },
            )
            .await?;

        Ok(())
    }

    /// Request a UTXO set/subset from Compute for updating the running total
    ///
    /// ### Arguments
    ///
    /// * `address_list` - Address list of UTXO set/subset to retrieve
    pub async fn request_utxo_set_for_wallet_update(
        &mut self,
        address_list: UtxoFetchType,
    ) -> Option<Response> {
        self.send_request_utxo_set(address_list).await.ok()?;
        Some(Response {
            success: true,
            reason: "Request UTXO set",
        })
    }

    /// Request a UTXO set/subset from Compute for updating the running total
    ///
    /// ### Arguments
    ///
    /// * `address_list` - Address list of UTXO set/subset to retrieve
    pub async fn request_payment_address_for_peer(
        &mut self,
        payment_peer: SocketAddr,
        amount: TokenAmount,
    ) -> Option<Response> {
        self.send_address_request(payment_peer, amount).await.ok()?;
        Some(Response {
            success: true,
            reason: "Request Payment Address",
        })
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

    /// Process pending paiment transaction with received address
    ///
    /// ### Arguments
    ///
    /// * `peer`    - Peer recieving the payment.
    /// * `address` - Address to assign the payment transaction to
    pub async fn process_pending_payment_transactions(
        &mut self,
        peer: SocketAddr,
        address: String,
    ) -> Option<Response> {
        let amount = match (
            self.pending_payments.0.remove(&peer),
            self.pending_payments.1,
        ) {
            (Some(PendingPayment { amount }), _) => amount,
            (_, AutoDonate::Enabled(amount)) => amount,
            _ => {
                return Some(Response {
                    success: false,
                    reason: "Ignore unexpected transaction",
                })
            }
        };

        Some(
            self.make_payment_transactions(Some(peer), address, amount)
                .await,
        )
    }

    /// Process specified paiment, updating wallet and next_payment
    ///
    /// ### Arguments
    ///
    /// * `peer`    - Peer recieving the payment.
    /// * `address` - Address to assign the payment transaction to
    /// * `amount`  - Price/amount payed
    pub async fn make_payment_transactions(
        &mut self,
        peer: Option<SocketAddr>,
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
        let payment_tx = construct_tx_core(tx_ins, tx_outs);
        self.next_payment = Some((peer, payment_tx));

        Response {
            success: true,
            reason: "Next payment transaction ready",
        }
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
    /// * `amount`    - Amount being payed
    pub async fn send_address_request(
        &mut self,
        peer: SocketAddr,
        amount: TokenAmount,
    ) -> Result<()> {
        let _peer_span = info_span!("sending payment address request");
        debug!("Sending request for payment address to peer: {:?}", peer);

        self.pending_payments
            .0
            .insert(peer, PendingPayment { amount });

        self.node
            .send(peer, UserRequest::SendAddressRequest)
            .await?;

        Ok(())
    }

    /// Sends a block notification request to a Compute node
    pub async fn send_block_notification_request(&mut self) -> Result<()> {
        let _peer_span = info_span!("sending block notification request");

        self.node
            .send(
                self.compute_addr,
                ComputeRequest::SendUserBlockNotificationRequest,
            )
            .await?;

        Ok(())
    }

    /// Sends a payment address from a request
    pub async fn send_address_to_trading_peer(&mut self) -> Result<()> {
        let peer = self.trading_peer.take().unwrap();
        let (address, _) = self.wallet_db.generate_payment_address().await;
        debug!("Address to send: {:?}", address);

        self.node
            .send(peer, UserRequest::SendPaymentAddress { address })
            .await?;
        Ok(())
    }

    /// Sends a donation payment address
    ///
    /// ### Arguments
    ///
    ///* `peer`    - Peer to send the address to
    pub async fn send_donation_address_to_peer(&mut self, peer: SocketAddr) -> Result<()> {
        self.trading_peer = Some(peer);
        self.send_address_to_trading_peer().await
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

    /// Process a notification for block mining to auto generate next transactions
    pub async fn process_mining_notified(&mut self) {
        if self.test_auto_gen_tx.is_none() {
            return;
        }

        let auto_gen = self.test_auto_gen_tx.as_mut().unwrap();

        // Load any data after restart
        if !auto_gen.tx_generator.is_up_to_date_with_snapshot() {
            if let Some(v) = self.wallet_db.get_db_value(TX_GENERATOR_KEY).await {
                auto_gen.tx_generator.apply_snapshot_state(&v);
            }
        }

        // Process committed transactions
        {
            let txs = &self.last_block_notified.transactions;
            info!("Block notified with txs: {}", txs.len());
            auto_gen.tx_generator.commit_transactions(txs);
        }

        // Send next transactions
        let mut total_txs = 0;
        loop {
            let auto_gen = self.test_auto_gen_tx.as_mut().unwrap();
            let txs_chunk_len = std::cmp::min(
                auto_gen.tx_chunk_size.unwrap_or(usize::MAX),
                auto_gen.tx_max_count.saturating_sub(total_txs),
            );
            let txs_chunk: Vec<_> = auto_gen
                .tx_generator
                .make_all_transactions(auto_gen.tx_in_per_tx, txs_chunk_len)
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

        let auto_gen = self.test_auto_gen_tx.as_mut().unwrap();
        self.wallet_db
            .set_db_value(TX_GENERATOR_KEY, auto_gen.tx_generator.snapshot_state())
            .await;
    }

    /// Get pending autogenerated transactions
    pub fn pending_test_auto_gen_txs(&self) -> Option<&BTreeMap<String, Transaction>> {
        self.test_auto_gen_tx
            .as_ref()
            .map(|a| a.tx_generator.pending_txs())
    }

    /// Get the wallet db
    pub fn get_wallet_db(&self) -> &WalletDb {
        &self.wallet_db
    }

    /// Get the last block notified to us
    pub fn get_last_block_notified(&self) -> &Block {
        &self.last_block_notified
    }

    /// Receive the requested UTXO set/subset from Compute
    ///
    /// ### Arguments
    ///
    /// * `utxo_set` - The requested UTXO set
    fn receive_utxo_set(&mut self, utxo_set: Vec<u8>) -> Response {
        self.received_utxo_set = deserialize(&utxo_set).ok();
        Response {
            success: true,
            reason: "Received UTXO set",
        }
    }

    /// Receives a request for a new payment address to be produced
    ///
    /// ### Arguments
    ///
    /// * `peer`    - Peer who made the request
    /// * `amount`  - Amount the payment will be
    fn receive_payment_address_request(&mut self, peer: SocketAddr) -> Response {
        self.trading_peer = Some(peer);

        Response {
            success: true,
            reason: "New address ready to be sent",
        }
    }
}

fn make_transaction_gen(setup: UserAutoGenTxSetup, user_node_idx: usize) -> Option<AutoGenTx> {
    let initial_transactions = setup
        .user_initial_transactions
        .get(user_node_idx)
        .cloned()
        .unwrap_or_default();
    if !initial_transactions.is_empty() {
        Some(AutoGenTx {
            tx_generator: TransactionGen::new(initial_transactions),
            tx_chunk_size: setup.user_setup_tx_chunk_size,
            tx_in_per_tx: setup.user_setup_tx_in_per_tx,
            tx_max_count: setup.user_setup_tx_max_count,
        })
    } else {
        None
    }
}
