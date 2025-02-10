use crate::comms_handler::{CommsError, Event, Node, TcpTlsConfig};
use crate::configurations::{ExtraNodeParams, TlsPrivateInfo, UserAutoGenTxSetup, UserNodeConfig};
use crate::interfaces::{
    MempoolRequest, NodeType, PaymentResponse, RbPaymentData, RbPaymentRequestData,
    RbPaymentResponseData, Response, UserApi, UserApiRequest, UserRequest, UtxoFetchType, UtxoSet,
};
use crate::threaded_call::{ThreadedCallChannel, ThreadedCallSender};
use crate::transaction_gen::{PendingMap, TransactionGen};
use crate::transactor::Transactor;
use crate::utils::{
    create_socket_addr, generate_half_druid, get_payments_for_wallet_from_utxo, to_api_keys,
    to_route_pow_infos, try_send_to_ui, ApiKeys, LocalEvent, LocalEventChannel, LocalEventSender,
    ResponseResult, RoutesPoWInfo,
};
use crate::wallet::{AddressStore, WalletDb, WalletDbError};
use crate::Rs2JsMsg;
use async_trait::async_trait;
use bincode::deserialize;
use bytes::Bytes;
use serde::Serialize;
use std::collections::BTreeSet;
use tw_chain::primitives::asset::{Asset, TokenAmount};
use tw_chain::primitives::block::Block;
use tw_chain::primitives::druid::{DdeValues, DruidExpectation};
use tw_chain::primitives::transaction::{GenesisTxHashSpec, Transaction, TxIn, TxOut};
use tw_chain::utils::transaction_utils::{
    construct_item_create_tx, construct_rb_payments_send_tx, construct_rb_receive_payment_tx,
    construct_tx_core, construct_tx_hash, construct_tx_ins_address, update_input_signatures,
    ReceiverInfo,
};

use std::{collections::BTreeMap, error::Error, fmt, future::Future, net::SocketAddr};
use tokio::sync::mpsc;
use tokio::task;
use tracing::{debug, error, error_span, info, info_span, trace, warn};
use tracing_futures::Instrument;

/// Key for last pow coinbase produced
pub const TX_GENERATOR_KEY: &str = "TxGeneratorKey";

/// Result wrapper for user errors
pub type Result<T> = std::result::Result<T, UserError>;

#[derive(Debug)]
pub enum UserError {
    ConfigError(&'static str),
    Network(CommsError),
    AsyncTask(task::JoinError),
    Serialization(bincode::Error),
    WalletError(WalletDbError),
}

impl fmt::Display for UserError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConfigError(err) => write!(f, "Config error: {err}"),
            Self::Network(err) => write!(f, "Network error: {err}"),
            Self::AsyncTask(err) => write!(f, "Async task error: {err}"),
            Self::Serialization(err) => write!(f, "Serialization error: {err}"),
            Self::WalletError(err) => write!(f, "Wallet error: {err}"),
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
            Self::WalletError(ref e) => Some(e),
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

impl From<WalletDbError> for UserError {
    fn from(other: WalletDbError) -> Self {
        Self::WalletError(other)
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

/// info for a pending payment
#[derive(Debug)]
pub struct PendingPayment {
    amount: TokenAmount,
    locktime: Option<u64>,
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
    threaded_calls: ThreadedCallChannel<dyn UserApi>,
    ui_feedback_tx: Option<mpsc::Sender<Rs2JsMsg>>,
    mempool_addr: SocketAddr,
    api_info: (SocketAddr, Option<TlsPrivateInfo>, ApiKeys, RoutesPoWInfo),
    trading_peer: Option<SocketAddr>,
    next_payment: Option<(Option<SocketAddr>, Transaction)>,
    last_block_notified: Block,
    test_auto_gen_tx: Option<AutoGenTx>,
    received_utxo_set: Option<UtxoSet>,
    pending_payments: (BTreeMap<SocketAddr, PendingPayment>, AutoDonate),
    next_rb_payment_response: Option<(SocketAddr, Option<RbPaymentResponseData>)>,
    next_rb_payment_data: Option<RbPaymentData>,
    next_rb_payment: Option<(Option<SocketAddr>, Transaction)>,
}

impl UserNode {
    ///Constructor to create a new UserNode
    ///
    /// ### Arguments
    ///
    /// * `config` - UserNodeConfig object containing UserNode parameters.
    /// * `extra`  - additional parameter for construction
    pub async fn new(config: UserNodeConfig, mut extra: ExtraNodeParams) -> Result<UserNode> {
        let addr = config.user_address;
        let raw_mempool_addr = config
            .mempool_nodes
            .get(config.user_mempool_node_idx)
            .ok_or(UserError::ConfigError("Invalid mempool index"))?;
        let mempool_addr = create_socket_addr(&raw_mempool_addr.address)
            .await
            .map_err(|_| UserError::ConfigError("Invalid mempool address"))?;

        let tls_addr = create_socket_addr(&addr).await.unwrap();
        let tcp_tls_config = TcpTlsConfig::from_tls_spec(tls_addr, &config.tls_config)?;
        let api_addr = SocketAddr::new(tls_addr.ip(), config.user_api_port);
        let api_tls_info = config
            .user_api_use_tls
            .then(|| tcp_tls_config.clone_private_info());
        let api_keys = to_api_keys(config.api_keys.clone());
        let api_pow_info = to_route_pow_infos(config.routes_pow.clone());
        let disable_tcp_listener = extra.disable_tcp_listener;
        let node = Node::new(
            &tcp_tls_config,
            config.peer_limit,
            config.peer_limit,
            NodeType::User,
            disable_tcp_listener,
            false,
        )
        .await?;

        let wallet_db = match extra.shared_wallet_db {
            Some(shared_db) => shared_db,
            None => WalletDb::new(
                config.user_db_mode,
                extra.wallet_db.take(),
                config.passphrase,
                extra.custom_wallet_spec,
            )?,
        };
        let wallet_db = wallet_db.with_seed(config.user_wallet_seeds).await;

        let pending_payments = match config.user_auto_donate {
            0 => (Default::default(), AutoDonate::Disabled),
            amount => (Default::default(), AutoDonate::Enabled(TokenAmount(amount))),
        };

        let test_auto_gen_tx = make_transaction_gen(config.user_test_auto_gen_setup);

        Ok(UserNode {
            node,
            wallet_db,
            local_events: Default::default(),
            threaded_calls: Default::default(),
            ui_feedback_tx: Default::default(),
            mempool_addr,
            api_info: (api_addr, api_tls_info, api_keys, api_pow_info),
            trading_peer: None,
            next_payment: None,
            last_block_notified: Default::default(),
            test_auto_gen_tx,
            received_utxo_set: None,
            pending_payments,
            next_rb_payment_response: None,
            next_rb_payment_data: None,
            next_rb_payment: None,
        })
    }

    /// Injects a new event into user node
    pub fn inject_next_event(
        &self,
        from_peer_addr: SocketAddr,
        data: impl Serialize,
    ) -> Result<()> {
        Ok(self.node.inject_next_event(from_peer_addr, data)?)
    }

    /// Returns the user node's local endpoint.
    pub fn local_address(&self) -> SocketAddr {
        self.node.local_address()
    }

    /// Returns the user node's public endpoint.
    pub async fn public_address(&self) -> Option<SocketAddr> {
        self.node.public_address().await
    }

    /// Returns the node's mempool endpoint.
    pub fn mempool_address(&self) -> SocketAddr {
        self.mempool_addr
    }

    /// Returns whether the node is connected to its Mempool peer
    pub async fn is_disconnected(&self) -> bool {
        !self
            .node
            .unconnected_peers(&[self.mempool_address()])
            .await
            .is_empty()
    }

    /// Check if auto generator is active
    pub fn is_test_auto_gen_tx_active(&self) -> bool {
        self.test_auto_gen_tx.is_some()
    }

    /// Returns the last generated address.
    pub fn get_last_generated_address(&self) -> Option<String> {
        self.wallet_db.get_last_generated_address()
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
        let mempool = Some(self.mempool_addr);
        let to_connect = mempool.iter();
        let expect_connect = mempool.iter();
        (
            self.node.clone(),
            to_connect.copied().collect(),
            expect_connect.copied().collect(),
        )
    }

    /// Info needed to run the API point.
    pub fn api_inputs(
        &self,
    ) -> (
        WalletDb,
        Node,
        SocketAddr,
        Option<TlsPrivateInfo>,
        ApiKeys,
        RoutesPoWInfo,
    ) {
        let (api_addr, api_tls_info, api_keys, routes_pow_info) = self.api_info.clone();
        (
            self.wallet_db.clone(),
            self.node.clone(),
            api_addr,
            api_tls_info,
            api_keys,
            routes_pow_info,
        )
    }

    /// Extract persistent dbs
    pub async fn take_closed_extra_params(&mut self) -> ExtraNodeParams {
        let wallet_db = self.wallet_db.take_closed_persistent_store().await;
        ExtraNodeParams {
            wallet_db: wallet_db.in_memory(),
            ..Default::default()
        }
    }

    /// Backup persistent storage
    pub async fn backup_persistent_store(&mut self) {
        if let Err(e) = self.wallet_db.backup_persistent_store().await {
            let error = format!("Error backup up main db: {:?}", e);
            error!("{:?}", &error);
            try_send_to_ui(self.ui_feedback_tx.as_ref(), Rs2JsMsg::Error { error }).await;
        }
    }

    /// Listens for new events from peers and handles them, processing any errors.
    pub async fn handle_next_event_response(
        &mut self,
        response: Result<Response>,
    ) -> ResponseResult {
        debug!("Response: {:?}", response);

        if let Ok(resp) = &response {
            let ui_message = match resp.success {
                true => Rs2JsMsg::Info {
                    info: resp.reason.to_owned(),
                },
                false => Rs2JsMsg::Error {
                    error: resp.reason.to_owned(),
                },
            };
            try_send_to_ui(self.ui_feedback_tx.as_ref(), ui_message).await;
        }

        match response {
            Ok(Response {
                success: true,
                reason,
            }) if reason == "Sent startup requests on reconnection" => {
                debug!("Sent startup requests on reconnection")
            }
            Ok(Response {
                success: false,
                reason,
            }) if reason == "Failed to send startup requests on reconnection" => {
                error!("Failed to send startup requests on reconnection")
            }
            Ok(Response {
                success: true,
                reason,
            }) if reason == "Shutdown" => {
                warn!("Shutdown now");
                try_send_to_ui(self.ui_feedback_tx.as_ref(), Rs2JsMsg::Exit).await;
                return ResponseResult::Exit;
            }
            Ok(Response {
                success: true,
                reason,
            }) if reason == "Item asset create transaction ready" => {
                self.send_next_payment_to_destinations(self.mempool_address())
                    .await
                    .unwrap();
            }
            Ok(Response {
                success: true,
                reason,
            }) if reason == "Received item-based payment request" => {
                self.send_rb_payment_response().await.unwrap();
                self.send_next_rb_transaction_to_destinations(self.mempool_address())
                    .await
                    .unwrap();
            }
            Ok(Response {
                success: true,
                reason,
            }) if reason == "Received item-based payment response" => {
                self.send_next_rb_transaction_to_destinations(self.mempool_address())
                    .await
                    .unwrap();
            }
            Ok(Response {
                success: true,
                reason,
            }) if reason == "New address ready to be sent" => {
                debug!("Sending new payment address");
                self.send_address_to_trading_peer().await.unwrap();
            }
            Ok(Response {
                success: true,
                reason,
            }) if reason == "New address generated" => {
                debug!("New address generated");
            }
            Ok(Response {
                success: true,
                reason,
            }) if reason == "Addresses deleted" => {
                debug!("Addresses deleted");
            }
            Ok(Response {
                success: true,
                reason,
            }) if reason == "Next payment transaction ready" => {
                self.send_next_payment_to_destinations(self.mempool_address())
                    .await
                    .unwrap();
            }
            Ok(Response {
                success: true,
                reason,
            }) if reason == "Block mining notified" => {
                self.process_mining_notified().await;
            }
            Ok(Response {
                success: true,
                reason,
            }) if reason == "Received UTXO set" => {
                self.update_running_total().await;
            }
            Ok(Response {
                success: true,
                reason,
            }) => {
                trace!("Unknown response: {:?}", reason);
            }
            Ok(Response {
                success: false,
                reason,
            }) => {
                error!("WARNING: UNHANDLED RESPONSE TYPE FAILURE: {:?}", reason);
            }
            Err(error) => {
                error!("ERROR HANDLING RESPONSE: {:?}", error);
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
                    if let Some(res) = self.handle_local_event(event).await {
                        return Some(Ok(res));
                    }
                }
                Some(f) = self.threaded_calls.rx.recv() => {
                    f(self);
                }
                reason = &mut *exit => return Some(Ok(Response {
                    success: true,
                    reason: reason.to_string(),
                }))
            }
        }
    }

    /// Send initial requests:
    /// - block notification request (if active)
    pub async fn send_startup_requests(&mut self) -> Result<()> {
        if self.is_test_auto_gen_tx_active() {
            info!("Send startup requests: block notification");
            return self.send_block_notification_request().await;
        }
        Ok(())
    }

    /// Threaded call channel.
    pub fn threaded_call_tx(&self) -> &ThreadedCallSender<dyn UserApi> {
        &self.threaded_calls.tx
    }

    /// Local event channel.
    pub fn local_event_tx(&self) -> &LocalEventSender {
        &self.local_events.tx
    }

    /// UI feedback channel.
    pub fn ui_feedback_tx(&self) -> Option<mpsc::Sender<Rs2JsMsg>> {
        self.ui_feedback_tx.clone()
    }

    /// Set UI feedback channel.
    pub fn set_ui_feedback_tx(&mut self, tx: mpsc::Sender<Rs2JsMsg>) {
        self.ui_feedback_tx = Some(tx.clone());
        self.wallet_db.set_ui_feedback_tx(tx);
    }

    ///Handle a local event
    ///
    /// ### Arguments
    ///
    /// * `event` - Event to process.
    async fn handle_local_event(&mut self, event: LocalEvent) -> Option<Response> {
        match event {
            LocalEvent::Exit(reason) => Some(Response {
                success: true,
                reason: reason.to_string(),
            }),
            LocalEvent::ReconnectionComplete => {
                if let Err(err) = self.send_startup_requests().await {
                    error!("Failed to send startup requests on reconnect: {}", err);
                    return Some(Response {
                        success: false,
                        reason: "Failed to send startup requests on reconnection".to_string(),
                    });
                }
                Some(Response {
                    success: true,
                    reason: "Sent startup requests on reconnection".to_string(),
                })
            }
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
            SendUtxoSet { utxo_set, b_num } => {
                self.last_block_notified.header.b_num = b_num;

                return Some(self.receive_utxo_set(utxo_set));
            }
            SendAddressRequest => Some(self.receive_payment_address_request(peer)),
            SendPaymentTransaction { transaction } => {
                Some(self.receive_payment_transaction(transaction).await)
            }
            SendPaymentAddress { address } => {
                let resp = self.process_pending_payment_transactions(peer, address);

                return match resp {
                    Some(resp) => Some(Response {
                        success: resp.success,
                        reason: resp.reason,
                    }),
                    None => None,
                };
            }
            SendRbPaymentRequest {
                rb_payment_request_data,
            } => Some(
                self.receive_rb_payment_request(peer, rb_payment_request_data)
                    .await,
            ),
            SendRbPaymentResponse {
                rb_payment_response,
            } => Some(
                self.receive_rb_payment_response(peer, rb_payment_response)
                    .await,
            ),
            BlockMining { block } => Some(self.notified_block_mining(peer, block).await),
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

        if peer != self.local_address() {
            // Do not process if not internal request
            return None;
        }

        match req {
            UpdateWalletFromUtxoSet { address_list } => {
                info!("Update wallet from UTXO set");
                info!("Address list: {:?}", address_list);
                self.request_utxo_set_for_wallet_update(address_list).await
            }
            RequestDonation { paying_peer } => self.request_donation_from_peer(paying_peer).await,
            MakeIpPayment {
                payment_peer,
                amount,
                locktime,
            } => {
                self.request_payment_address_for_peer(payment_peer, amount, locktime)
                    .await
            }
            MakePayment {
                address,
                amount,
                locktime,
            } => {
                let resp = self.make_payment_transactions(None, address, amount, locktime);
                return Some(Response {
                    success: resp.success,
                    reason: resp.reason,
                });
            }
            SendCreateItemRequest {
                item_amount,
                genesis_hash_spec,
                metadata,
            } => Some(
                self.generate_item_asset_tx(item_amount, genesis_hash_spec, metadata)
                    .await,
            ),
            MakePaymentWithExcessAddress {
                address,
                amount,
                excess_address,
                locktime,
            } => {
                let resp = self.make_payment_transactions_provided_excess(
                    None,
                    address,
                    amount,
                    Some(excess_address),
                    locktime,
                );

                return Some(Response {
                    success: resp.success,
                    reason: resp.reason,
                });
            }
            GenerateNewAddress => Some(self.generate_new_address().await),
            GetConnectionStatus => Some(self.receive_connection_status_request().await),
            ConnectToMempool => Some(self.handle_connect_to_mempool().await),
            DisconnectFromMempool => Some(self.handle_disconnect_from_mempool().await),
            DeleteAddresses { addresses } => {
                Some(self.delete_addresses_from_wallet(peer, addresses).await)
            }
            MergeAddresses {
                addresses,
                excess_address,
            } => Some(
                self.make_merged_payment_transaction_from_input_addrs(
                    peer,
                    addresses,
                    excess_address,
                )
                .await,
            ),
            SendNextPayment => {
                match self
                    .send_next_payment_to_destinations(self.mempool_address())
                    .await
                {
                    Ok(_) => Some(Response {
                        success: true,
                        reason: "Next payment transaction sent".to_string(),
                    }),
                    Err(e) => Some(Response {
                        success: false,
                        reason: format!("Failed to send next payment transaction: {:?}", e),
                    }),
                }
            }
        }
    }

    /// Handle delete addresses from wallet
    pub async fn delete_addresses_from_wallet(
        &mut self,
        _peer: SocketAddr,
        addresses: BTreeSet<String>,
    ) -> Response {
        self.wallet_db
            .destroy_spent_transactions_and_keys(Some(addresses))
            .await;

        Response {
            success: true,
            reason: "Addresses deleted".to_string(),
        }
    }

    /// Handle connect to mempool node
    pub async fn handle_disconnect_from_mempool(&mut self) -> Response {
        let mempool_addr = self.mempool_address();
        let join_handles = self.node.disconnect_all(Some(&[mempool_addr])).await;
        if join_handles.is_empty() {
            return Response {
                success: false,
                reason: "Already disconnected from mempool".to_string(),
            };
        }
        for join_handle in join_handles {
            if let Err(err) = join_handle.await {
                error!("Failed to disconnect from mempool: {}", err);
                return Response {
                    success: false,
                    reason: "Failed to disconnect from mempool".to_string(),
                };
            }
        }
        try_send_to_ui(
            self.ui_feedback_tx.as_ref(),
            Rs2JsMsg::Value(serde_json::json!({
                "connected": false,
            })),
        )
        .await;
        Response {
            success: true,
            reason: "Disconnected from mempool".to_string(),
        }
    }

    /// Handle connect to mempool node
    pub async fn handle_connect_to_mempool(&mut self) -> Response {
        let mempool_addr = self.mempool_address();
        if let Err(e) = self.node.connect_to(mempool_addr).await {
            error!("Failed to connect to mempool: {e:?}");
            return Response {
                success: false,
                reason: "Failed to connect to mempool".to_string(),
            };
        }
        if let Err(e) = self.send_block_notification_request().await {
            error!("Failed to send startup requests to mempool: {e:?}");
            return Response {
                success: false,
                reason: "Failed to send startup requests on reconnection".to_string(),
            };
        }
        try_send_to_ui(
            self.ui_feedback_tx.as_ref(),
            Rs2JsMsg::Value(serde_json::json!({
                "connected": true,
            })),
        )
        .await;
        Response {
            success: true,
            reason: "Connected to mempool".to_string(),
        }
    }

    /// Handles the request to check connection status
    pub async fn receive_connection_status_request(&self) -> Response {
        if self.is_disconnected().await {
            try_send_to_ui(
                self.ui_feedback_tx.as_ref(),
                Rs2JsMsg::Value(serde_json::json!({
                    "connected":false,
                })),
            )
            .await;
            return Response {
                success: true,
                reason: "Node is disconnected".to_string(),
            };
        }
        try_send_to_ui(
            self.ui_feedback_tx.as_ref(),
            Rs2JsMsg::Value(serde_json::json!({
                "connected": true,
            })),
        )
        .await;
        Response {
            success: true,
            reason: "Node is connected".to_string(),
        }
    }

    /// Handles the item of closing event
    ///
    /// ### Arguments
    ///
    /// * `peer`     - Sending peer's socket address
    fn receive_closing(&mut self, peer: SocketAddr) -> Option<Response> {
        if peer != self.mempool_address() {
            return None;
        }

        Some(Response {
            success: true,
            reason: "Shutdown".to_string(),
        })
    }
    pub fn get_next_payment_transaction(&self) -> Option<(Option<SocketAddr>, Transaction)> {
        self.next_payment.clone()
    }

    pub fn get_received_utxo(&self) -> Option<UtxoSet> {
        self.received_utxo_set.clone()
    }

    /// Sends the next internal payment transaction to be processed by the connected Mempool
    /// node
    ///
    /// ### Arguments
    ///
    /// * `mempool_peer`    - Mempool peer to send the payment tx to
    /// * `payment_tx`      - Transaction to send
    pub async fn send_next_payment_to_destinations(
        &mut self,
        mempool_peer: SocketAddr,
    ) -> Result<()> {
        // TODO: having next_payment as part of the node is error-prone, it would be better to
        // simply pass the transaction in as an argument
        let (peer, tx) = self.next_payment.take().unwrap();

        debug!(
            ?tx,
            "Sending payment transaction to mempool node for processing"
        );
        debug!("Mempool peer: {:?}", mempool_peer);

        self.send_transactions_to_mempool(mempool_peer, vec![tx.clone()])
            .await?;

        debug!("Payment transaction sent to mempool node for processing");

        let b_num = self.last_block_notified.header.b_num;

        self.wallet_db
            .store_payment_transaction(tx.clone(), b_num)
            .await;

        if let Some(peer) = peer {
            self.send_payment_to_receiver(peer, tx).await?;
        }

        Ok(())
    }

    /// Sends the next internal item-based payment transaction to be processed by the connected Mempool
    /// node
    ///
    /// ### Arguments
    ///
    /// * `mempool_peer` - Mempool peer to send the payment tx to
    pub async fn send_next_rb_transaction_to_destinations(
        &mut self,
        mempool_peer: SocketAddr,
    ) -> Result<()> {
        let (peer, transaction) = self.next_rb_payment.take().unwrap();
        let b_num = self.last_block_notified.header.b_num;
        self.wallet_db
            .store_payment_transaction(transaction.clone(), b_num)
            .await;
        let _peer_span =
            info_span!("sending item-based transaction to mempool node for processing");
        let transactions = vec![transaction.clone()];
        self.node
            .send(
                mempool_peer,
                MempoolRequest::SendTransactions { transactions },
            )
            .await?;

        if let Some(peer) = peer {
            self.send_payment_to_receiver(peer, transaction).await?;
        }

        Ok(())
    }

    /// Request a UTXO set/subset from Mempool for updating the running total
    ///
    /// ### Arguments
    ///
    /// * `address_list` - Address list of UTXO set/subset to retrieve
    pub async fn request_utxo_set_for_wallet_update(
        &mut self,
        address_list: UtxoFetchType,
    ) -> Option<Response> {
        let mempool_addr = self.mempool_address();

        info!("Requesting UTXO set for wallet update");

        self.send_request_utxo_set(address_list, mempool_addr, NodeType::User)
            .await
            .ok()?;

        Some(Response {
            success: true,
            reason: "Request UTXO set".to_string(),
        })
    }

    /// Request a UTXO set/subset from Mempool for updating the running total
    ///
    /// ### Arguments
    ///
    /// * `payment_peer` - Peer to send request to
    pub async fn request_donation_from_peer(
        &mut self,
        payment_peer: SocketAddr,
    ) -> Option<Response> {
        self.connect_to(payment_peer).await.ok()?;
        self.send_donation_address_to_peer(payment_peer)
            .await
            .ok()?;
        Some(Response {
            success: true,
            reason: "Donation Requested".to_string(),
        })
    }

    /// Request a UTXO set/subset from Mempool for updating the running total
    ///
    /// ### Arguments
    ///
    /// * `payment_peer` - Peer to send request to
    /// * `amount`       - Amount to pay
    /// * `locktime` - Locktime for transaction
    pub async fn request_payment_address_for_peer(
        &mut self,
        payment_peer: SocketAddr,
        amount: TokenAmount,
        locktime: Option<u64>,
    ) -> Option<Response> {
        self.send_address_request(payment_peer, amount, locktime)
            .await
            .ok()?;
        Some(Response {
            success: true,
            reason: "Request Payment Address".to_string(),
        })
    }

    /// Receives a payment transaction to one of this user's addresses
    ///
    /// ### Arguments
    ///
    /// * `transaction` - Transaction to receive and save to wallet
    pub async fn receive_payment_transaction(&mut self, transaction: Transaction) -> Response {
        let b_num = self.last_block_notified.header.b_num;
        self.wallet_db
            .store_payment_transaction(transaction, b_num)
            .await;

        Response {
            success: true,
            reason: "Payment transaction received".to_string(),
        }
    }

    /// Process pending payment transaction with received address
    ///
    /// ### Arguments
    ///
    /// * `peer`    - Peer recieving the payment.
    /// * `address` - Address to assign the payment transaction to
    pub fn process_pending_payment_transactions(
        &mut self,
        peer: SocketAddr,
        address: String,
    ) -> Option<PaymentResponse> {
        let (amount, locktime) = match (
            self.pending_payments.0.remove(&peer),
            self.pending_payments.1,
        ) {
            (Some(PendingPayment { amount, locktime }), _) => (amount, locktime),
            (_, AutoDonate::Enabled(amount)) => (amount, None),
            _ => {
                return Some(PaymentResponse {
                    success: false,
                    reason: "Ignore unexpected transaction".to_string(),
                    tx_hash: "".to_string(),
                    tx: None,
                })
            }
        };

        Some(self.make_payment_transactions(Some(peer), address, amount, locktime))
    }

    /// Process specified payment with a provided excess address,
    /// updating wallet and next_payment
    ///
    /// ### Arguments
    ///
    /// * `peer`    - Peer recieving the payment.
    /// * `address` - Address to assign the payment transaction to
    /// * `amount`  - Price/amount paid
    /// * `excess_address` - Address to assign the excess to
    /// * `locktime` - Locktime for transaction
    pub fn make_payment_transactions_provided_excess(
        &mut self,
        peer: Option<SocketAddr>,
        address: String,
        amount: TokenAmount,
        excess_address: Option<String>,
        locktime: Option<u64>,
    ) -> PaymentResponse {
        let tx_out = TxOut::new_token_amount(address, amount, locktime);
        let asset_required = Asset::Token(amount);
        let (tx_ins, tx_outs) = if let Ok(value) = self
            .wallet_db
            .fetch_tx_ins_and_tx_outs_provided_excess(asset_required, vec![tx_out], excess_address)
        {
            value
        } else {
            return PaymentResponse {
                success: false,
                reason: "Insufficient funds for payment".to_string(),
                tx_hash: "".to_string(),
                tx: None,
            };
        };

        let key_material = self.wallet_db.get_key_material(&tx_ins);
        let final_tx_ins = update_input_signatures(&tx_ins, &tx_outs, &key_material);
        let payment_tx = construct_tx_core(final_tx_ins, tx_outs, None);
        let tx_hash = construct_tx_hash(&payment_tx);

        self.wallet_db.set_last_construct_tx(payment_tx.clone());
        self.next_payment = Some((peer, payment_tx.clone()));

        PaymentResponse {
            success: true,
            reason: "Payment transaction pending".to_string(),
            tx_hash,
            tx: Some(payment_tx),
        }
    }

    /// Process specified payment with a provided excess address,
    /// as well as provided input addresses; updating wallet and next_payment
    ///
    /// NOTE: This payment is meant to merge existing addresses in the wallet
    /// to a new address, and is not meant to be used for payments to peers
    ///
    /// ### Arguments
    ///
    /// * `input_addresses`  - Input addresses to use in this payment
    /// * `excess_address` - Address to assign the excess to
    pub async fn make_merged_payment_transaction_from_input_addrs(
        &mut self,
        _peer: SocketAddr,
        input_addresses: BTreeSet<String>,
        excess_address: Option<String>,
    ) -> Response {
        let (tx_ins, tx_outs) = if let Ok(value) = self
            .wallet_db
            .fetch_tx_ins_and_tx_outs_merge_input_addrs(input_addresses, excess_address)
            .await
        {
            value
        } else {
            return Response {
                success: false,
                reason: "Insufficient funds for payment".to_string(),
            };
        };
        let key_material = self.wallet_db.get_key_material(&tx_ins);
        let final_tx_ins = update_input_signatures(&tx_ins, &tx_outs, &key_material);
        let payment_tx = construct_tx_core(final_tx_ins, tx_outs, None);
        self.next_payment = Some((None, payment_tx));

        Response {
            success: true,
            reason: "Next payment transaction ready".to_string(),
        }
    }

    /// Process specified payment, updating wallet and next_payment
    ///
    /// ### Arguments
    ///
    /// * `peer`    - Peer recieving the payment.
    /// * `address` - Address to assign the payment transaction to
    /// * `amount`  - Price/amount paid
    /// * `locktime` - Locktime for transaction
    pub fn make_payment_transactions(
        &mut self,
        peer: Option<SocketAddr>,
        address: String,
        amount: TokenAmount,
        locktime: Option<u64>,
    ) -> PaymentResponse {
        self.make_payment_transactions_provided_excess(peer, address, amount, None, locktime)
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
    /// * `amount`    - Amount being paid
    /// * `locktime` - Locktime for transaction
    pub async fn send_address_request(
        &mut self,
        peer: SocketAddr,
        amount: TokenAmount,
        locktime: Option<u64>,
    ) -> Result<()> {
        let _peer_span = info_span!("sending payment address request");
        debug!("Sending request for payment address to peer: {:?}", peer);

        self.pending_payments
            .0
            .insert(peer, PendingPayment { amount, locktime });

        self.node
            .send(peer, UserRequest::SendAddressRequest)
            .await?;

        Ok(())
    }

    /// Sends a block notification request to a Mempool node
    pub async fn send_block_notification_request(&mut self) -> Result<()> {
        let _peer_span = info_span!("sending block notification request");

        self.node
            .send(
                self.mempool_addr,
                MempoolRequest::SendUserBlockNotificationRequest,
            )
            .await?;

        Ok(())
    }

    /// Sends a payment address from a request
    pub async fn send_address_to_trading_peer(&mut self) -> Result<()> {
        let peer = self.trading_peer.take().unwrap();
        let (address, _) = self.wallet_db.generate_payment_address();
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

    /// Filter locked coinbase
    ///
    ///
    /// ### Arguments
    ///
    /// * `b_num` - Block number to filter
    pub async fn filter_locked_coinbase(&mut self, b_num: u64) {
        self.wallet_db.filter_locked_coinbase(b_num).await;
    }

    /// Received a mined block notification: allow to update pending transactions
    ///
    /// ### Arguments
    ///
    /// * `peer` -  SocketAdress of the peer notifying.
    /// * `block` - Block that is being mined and will be stored.
    pub async fn notified_block_mining(&mut self, peer: SocketAddr, block: Block) -> Response {
        if peer == self.mempool_addr {
            self.wallet_db
                .filter_locked_coinbase(block.header.b_num)
                .await;
            self.last_block_notified = block;
            // Send the block to the UI for realtime feedback
            try_send_to_ui(
                self.ui_feedback_tx.as_ref(),
                Rs2JsMsg::Value(serde_json::json!( {
                    "last_block_notified": self.last_block_notified.clone(),
                })),
            )
            .await;
            Response {
                success: true,
                reason: "Block mining notified".to_string(),
            }
        } else {
            Response {
                success: false,
                reason: "Invalid block mining notifier".to_string(),
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
                .send_transactions_to_mempool(self.mempool_address(), txs_chunk)
                .await
            {
                let error = format!("Autogenerated tx not sent to {:?}", e);
                error!("{:?}", &error);
                try_send_to_ui(self.ui_feedback_tx.as_ref(), Rs2JsMsg::Error { error }).await;
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
    pub fn pending_test_auto_gen_txs(&self) -> Option<&PendingMap> {
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

    /// Generate a new payment address
    pub async fn generate_new_address(&mut self) -> Response {
        let _ = self.wallet_db.generate_payment_address();
        Response {
            success: true,
            reason: "New address generated".to_string(),
        }
    }

    #[cfg(test)]
    /// Generate a new payment address
    pub async fn generate_static_address_for_miner(&mut self) -> String {
        let (addr, _) = self.wallet_db.generate_payment_address();
        addr
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
            reason: "New address ready to be sent".to_string(),
        }
    }

    /// Sends a request for a new item-based payment
    ///
    /// ### Nomenclature
    ///
    /// The sender(Alice) and receiver(Bob) context stays consistent for
    /// `send_rb_payment_request`, `receive_rb_payment_request`,
    /// `send_rb_payment_response`, and `receive_rb_payment_response`
    ///
    /// ### Arguments
    ///
    /// * `peer`            - Peer who made the request
    /// * `send_asset`       - The asset to be sent
    pub async fn send_rb_payment_request(
        &mut self,
        peer: SocketAddr,
        sender_asset: Asset,
        genesis_hash: Option<String>, /* genesis_hash of Item asset to receive */
    ) -> Result<()> {
        let (sender_address, _) = self.wallet_db.generate_payment_address();
        let sender_half_druid = generate_half_druid();

        let (tx_ins, tx_outs) = self
            .wallet_db
            .fetch_tx_ins_and_tx_outs(sender_asset.clone(), Vec::new())
            .unwrap();

        let (rb_payment_data, rb_payment_request_data) = make_rb_payment_send_tx_and_request(
            sender_asset,
            (tx_ins, tx_outs),
            sender_half_druid,
            sender_address,
            genesis_hash,
        );

        self.next_rb_payment_data = Some(rb_payment_data);
        self.node
            .send(
                peer,
                UserRequest::SendRbPaymentRequest {
                    rb_payment_request_data,
                },
            )
            .await?;
        Ok(())
    }

    /// Sends a response to a new item-based payment request
    pub async fn send_rb_payment_response(&mut self) -> Result<()> {
        let (peer, rb_payment_response) = self.next_rb_payment_response.take().unwrap();
        self.node
            .send(
                peer,
                UserRequest::SendRbPaymentResponse {
                    rb_payment_response,
                },
            )
            .await?;
        Ok(())
    }

    /// Receives a request for a new item-based payment
    /// ### Nomenclature
    ///
    /// The sender(Alice) and receiver(Bob) context stays consistent for
    /// `send_rb_payment_request`, `receive_rb_payment_request`,
    /// `send_rb_payment_response`, and `receive_rb_payment_response`
    ///
    /// ### Arguments
    ///
    /// * `peer`                         - Peer who made the request
    /// * `rb_payment_request_data`      - Item-based payment request data struct
    async fn receive_rb_payment_request(
        &mut self,
        peer: SocketAddr,
        rb_payment_request_data: RbPaymentRequestData,
    ) -> Response {
        let receiver_half_druid = generate_half_druid();
        let (receiver_address, _) = self.wallet_db.generate_payment_address();
        let asset_required = Asset::item(
            1,
            rb_payment_request_data.sender_drs_tx_expectation.clone(),
            None,
        );
        let tx_ins_and_outs = self
            .wallet_db
            .fetch_tx_ins_and_tx_outs(asset_required, Vec::new());

        let (tx_ins, tx_outs) = if let Ok(value) = tx_ins_and_outs {
            value
        } else {
            return Response {
                success: false,
                reason: "Insufficient funds for payment".to_string(),
            };
        };

        let (rb_receive_tx, rb_payment_response) = make_rb_payment_item_tx_and_response(
            rb_payment_request_data,
            (tx_ins, tx_outs),
            receiver_half_druid,
            receiver_address,
        );

        self.next_rb_payment = Some((Some(peer), rb_receive_tx));
        self.next_rb_payment_response = Some((peer, Some(rb_payment_response)));

        Response {
            success: true,
            reason: "Received item-based payment request".to_string(),
        }
    }

    /// Receive a response for a new item-based payment
    ///
    /// ### Nomenclature
    ///
    /// The sender(Alice) and receiver(Bob) context stays consistent for
    /// `send_rb_payment_request`, `receive_rb_payment_request`,
    /// `send_rb_payment_response`, and `receive_rb_payment_response`
    ///
    /// ### Arguments
    ///
    /// * `peer`                    - Peer who sent the response
    /// * `rb_payment_response`     - (receiver address, half druid value, hash value of Vec<TxIn>)
    async fn receive_rb_payment_response(
        &mut self,
        peer: SocketAddr,
        rb_payment_response: Option<RbPaymentResponseData>,
    ) -> Response {
        let rb_payment_data = self.next_rb_payment_data.take().unwrap();
        //TODO: Handle `None` value upon item-based payment rejection
        if let Some(rb_payment_response) = rb_payment_response {
            let rb_send_tx = make_rb_payment_send_transaction(rb_payment_response, rb_payment_data);
            self.next_rb_payment = Some((Some(peer), rb_send_tx));
        }
        Response {
            success: true,
            reason: "Received item-based payment response".to_string(),
        }
    }

    /// Create new item-asset transaction to send to mempool for processing
    pub async fn generate_item_asset_tx(
        &mut self,
        item_amount: u64,
        genesis_hash_spec: GenesisTxHashSpec,
        metadata: Option<String>,
    ) -> Response {
        let AddressStore {
            public_key,
            secret_key,
            address_version: _,
        } = self.wallet_db.generate_payment_address().1;

        let block_num = self.last_block_notified.header.b_num;
        let item_asset_tx = construct_item_create_tx(
            block_num,
            public_key,
            &secret_key,
            item_amount,
            genesis_hash_spec,
            None,
            metadata,
        );

        self.next_payment = Some((None, item_asset_tx));

        Response {
            reason: "Item asset create transaction ready".to_string(),
            success: true,
        }
    }

    /// Get `Node` member
    pub fn get_node(&self) -> &Node {
        &self.node
    }

    /// Get `Node` member as mutable (useful to reach comms)
    pub fn get_node_mut(&mut self) -> &mut Node {
        &mut self.node
    }
}

impl UserApi for UserNode {
    fn make_payment(
        &mut self,
        address: String,
        amount: TokenAmount,
        locktime: Option<u64>,
    ) -> PaymentResponse {
        self.make_payment_transactions(None, address, amount, locktime)
    }
}

#[async_trait]
impl Transactor for UserNode {
    type Error = UserError;

    async fn send_transactions_to_mempool(
        &mut self,
        mempool_peer: SocketAddr,
        transactions: Vec<Transaction>,
    ) -> Result<()> {
        let _peer_span = info_span!("Sending transactions to mempool node for processing");
        self.node
            .send(
                mempool_peer,
                MempoolRequest::SendTransactions { transactions },
            )
            .await?;

        Ok(())
    }

    async fn send_request_utxo_set(
        &mut self,
        address_list: UtxoFetchType,
        mempool_addr: SocketAddr,
        requester_node_type: NodeType,
    ) -> Result<()> {
        let _peer_span = info_span!("Sending UXTO request to mempool node");
        self.node
            .send(
                mempool_addr,
                MempoolRequest::SendUtxoRequest {
                    address_list,
                    requester_node_type,
                },
            )
            .await?;

        Ok(())
    }

    fn receive_utxo_set(&mut self, utxo_set: UtxoSet) -> Response {
        self.received_utxo_set = Some(utxo_set);

        Response {
            success: true,
            reason: "Received UTXO set".to_string(),
        }
    }
    async fn update_running_total(&mut self) {
        let utxo_set = self.received_utxo_set.take();
        let payments = get_payments_for_wallet_from_utxo(utxo_set.into_iter().flatten());
        let known_addresses = self.wallet_db.get_known_addresses();
        let utxo_addresses = payments
            .iter()
            .map(|p| p.2.clone())
            .collect::<BTreeSet<_>>();

        let addr_diff: BTreeSet<_> = known_addresses.into_iter().collect();
        let new_addresses: Vec<String> = utxo_addresses.difference(&addr_diff).cloned().collect();
        let reset_db = new_addresses.is_empty();

        debug!("Reset DB: {}", reset_db);

        let b_num = self.last_block_notified.header.b_num;
        debug!("Current block number: {}", b_num);

        self.wallet_db
            .save_usable_payments_to_wallet(payments, b_num, reset_db)
            .await
            .unwrap();
    }
}

/// Make the send initial request for item based transaction
///
/// * `send_asset`        - The asset to be sent
/// * `(tx_ins, tx_outs)` - The send transaction infos
/// * `sender_half_druid` - The sender half druid part
/// * `sender_address`    - The sender address
pub fn make_rb_payment_send_tx_and_request(
    sender_asset: Asset,
    (tx_ins, tx_outs): (Vec<TxIn>, Vec<TxOut>),
    sender_half_druid: String,
    sender_address: String,
    sender_drs_tx_expectation: Option<String>,
) -> (RbPaymentData, RbPaymentRequestData) {
    let sender_from_addr = construct_tx_ins_address(&tx_ins);

    let rb_payment_request_data = RbPaymentRequestData {
        sender_address,
        sender_half_druid: sender_half_druid.clone(),
        sender_from_addr,
        sender_asset: sender_asset.clone(),
        sender_drs_tx_expectation,
    };

    let rb_payment_data = RbPaymentData {
        sender_asset,
        sender_half_druid,
        tx_ins,
        tx_outs,
    };

    (rb_payment_data, rb_payment_request_data)
}

/// Make The item transaction and response
///
/// * `rb_payment_request_data` - Item-based payment request data struct
/// * `(tx_ins, tx_outs)`       - The item transaction infos
/// * `receiver_half_druid`     - The receiver half druid part
/// * `receiver_address`        - The receiver address
pub fn make_rb_payment_item_tx_and_response(
    rb_payment_request_data: RbPaymentRequestData,
    (tx_ins, tx_outs): (Vec<TxIn>, Vec<TxOut>),
    receiver_half_druid: String,
    receiver_address: String,
) -> (Transaction, RbPaymentResponseData) {
    //TODO: Decide if the send/receive assets are acceptable
    let RbPaymentRequestData {
        sender_address,
        sender_half_druid,
        sender_from_addr,
        sender_asset,
        sender_drs_tx_expectation,
    } = rb_payment_request_data;

    let druid = sender_half_druid + &receiver_half_druid;
    let receiver_from_addr = construct_tx_ins_address(&tx_ins);

    // DruidExpectation for sender(Alice)
    let sender_druid_expectation = DruidExpectation {
        from: receiver_from_addr,
        to: sender_address.clone(),
        asset: Asset::item(1, sender_drs_tx_expectation.clone(), None),
    };

    // DruidExpectation for receiver(Bob)
    let receiver_druid_expectation = DruidExpectation {
        from: sender_from_addr,
        to: receiver_address.clone(),
        asset: sender_asset,
    };

    let dde_values = DdeValues {
        druid,
        participants: 2,
        expectations: vec![receiver_druid_expectation],
        genesis_hash: None,
    };

    let rb_receive_tx = construct_rb_receive_payment_tx(
        tx_ins,
        tx_outs,
        None,
        sender_address,
        0,
        dde_values,
        &BTreeMap::new(),
    );

    let rb_payment_response = RbPaymentResponseData {
        receiver_address,
        receiver_half_druid,
        sender_druid_expectation,
    };

    (rb_receive_tx, rb_payment_response)
}

/// Make The payment send transaction
///
/// * `rb_payment_response`- (receiver address, half druid value, hash value of Vec<TxIn>)
/// * `rb_payment_data`    - The data related to initial requrest for the payment
pub fn make_rb_payment_send_transaction(
    rb_payment_response: RbPaymentResponseData,
    rb_payment_data: RbPaymentData,
) -> Transaction {
    let RbPaymentData {
        sender_asset,
        sender_half_druid,
        tx_ins,
        tx_outs,
    } = rb_payment_data;

    let RbPaymentResponseData {
        receiver_address,
        receiver_half_druid,
        sender_druid_expectation,
    } = rb_payment_response;

    let druid = sender_half_druid + &receiver_half_druid;
    let druid_values = DdeValues {
        druid,
        participants: 2,
        expectations: vec![sender_druid_expectation],
        genesis_hash: None,
    };

    let receiver = ReceiverInfo {
        address: receiver_address,
        asset: sender_asset,
    };

    construct_rb_payments_send_tx(
        tx_ins,
        tx_outs,
        None,
        receiver,
        0,
        druid_values,
        &BTreeMap::new(),
    )
}

fn make_transaction_gen(setup: UserAutoGenTxSetup) -> Option<AutoGenTx> {
    if !setup.user_initial_transactions.is_empty() {
        Some(AutoGenTx {
            tx_generator: TransactionGen::new(setup.user_initial_transactions),
            tx_chunk_size: setup.user_setup_tx_chunk_size,
            tx_in_per_tx: setup.user_setup_tx_in_per_tx,
            tx_max_count: setup.user_setup_tx_max_count,
        })
    } else {
        None
    }
}
