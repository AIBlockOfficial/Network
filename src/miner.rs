use crate::comms_handler::Node;
use crate::comms_handler::{CommsError, Event, TcpTlsConfig};
use crate::configurations::{ExtraNodeParams, MinerNodeConfig, TlsPrivateInfo};
use crate::constants::INTERNAL_TX_LIMIT;
use crate::db_utils;
use crate::interfaces::{
    BlockchainItem, MempoolRequest, MineApiRequest, MineRequest, MinerInterface, NodeType, PowInfo,
    ProofOfWork, Response, Rs2JsMsg, StorageRequest, UtxoFetchType, UtxoSet,
};
use crate::threaded_call::{ThreadedCallChannel, ThreadedCallSender};
use crate::transactor::Transactor;
use crate::utils::{
    self, apply_mining_tx, construct_coinbase_tx, create_socket_addr, format_parition_pow_address,
    generate_pow_for_block, get_payments_for_wallet, get_payments_for_wallet_from_utxo,
    to_api_keys, to_route_pow_infos, try_send_to_ui, ApiKeys, DeserializedBlockchainItem,
    LocalEvent, LocalEventChannel, LocalEventSender, ResponseResult, RoutesPoWInfo,
    RunningTaskOrResult,
};
use crate::wallet::{LockedCoinbase, WalletDb, WalletDbError, DB_SPEC};
use async_trait::async_trait;
use bincode::{deserialize, serialize};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::{
    error::Error,
    fmt,
    future::Future,
    net::SocketAddr,
    net::{IpAddr, Ipv4Addr},
    str,
    time::SystemTime,
};
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::task;
use tracing::{debug, error, error_span, info, info_span, trace, warn};
use tracing_futures::Instrument;
use tw_chain::primitives::asset::{Asset, TokenAmount};
use tw_chain::primitives::block::{self, BlockHeader};
use tw_chain::primitives::transaction::Transaction;
use tw_chain::utils::transaction_utils::{
    construct_tx_core, construct_tx_hash, update_input_signatures,
};
use warp::filters::trace;

/// Key for last pow coinbase produced
pub const LAST_COINBASE_KEY: &str = "LastCoinbaseKey";

/// Key for last pow coinbase produced
pub const MINING_ADDRESS_KEY: &str = "MiningAddressKey";

/// Result wrapper for miner errors
pub type Result<T> = std::result::Result<T, MinerError>;

/// Wrapper for current block
///
/// TODO: Circumvent using a Mutex just for API purposes.
pub type CurrentBlockWithMutex = Arc<Mutex<Option<BlockPoWReceived>>>;

/// Block Pow task input/output
#[derive(Debug, Clone)]
pub struct BlockPoWInfo {
    peer: SocketAddr,
    start_time: SystemTime,
    header: BlockHeader,
    coinbase: Transaction,
}

/// Received block
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BlockPoWReceived {
    block: BlockHeader,
    reward: TokenAmount,
}

#[derive(Debug)]
pub enum MinerError {
    ConfigError(&'static str),
    Network(CommsError),
    Serialization(bincode::Error),
    AsyncTask(task::JoinError),
    WalletError(WalletDbError),
}

#[derive(Debug, PartialEq, Clone)]
pub enum AggregationStatus {
    Idle,
    UtxoUpdate(String),
}

impl Default for AggregationStatus {
    fn default() -> Self {
        Self::Idle
    }
}

impl fmt::Display for MinerError {
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

impl Error for MinerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ConfigError(_) => None,
            Self::Network(ref e) => Some(e),
            Self::AsyncTask(e) => Some(e),
            Self::Serialization(ref e) => Some(e),
            Self::WalletError(ref e) => Some(e),
        }
    }
}

impl From<bincode::Error> for MinerError {
    fn from(other: bincode::Error) -> Self {
        Self::Serialization(other)
    }
}

impl From<CommsError> for MinerError {
    fn from(other: CommsError) -> Self {
        Self::Network(other)
    }
}

impl From<task::JoinError> for MinerError {
    fn from(other: task::JoinError) -> Self {
        Self::AsyncTask(other)
    }
}

impl From<WalletDbError> for MinerError {
    fn from(other: WalletDbError) -> Self {
        Self::WalletError(other)
    }
}

/// An instance of a MinerNode
#[derive(Debug)]
pub struct MinerNode {
    node: Node,
    wallet_db: WalletDb,
    local_events: LocalEventChannel,
    threaded_calls: ThreadedCallChannel<MinerNode>,
    ui_feedback_tx: Option<mpsc::Sender<Rs2JsMsg>>,
    mempool_addr: SocketAddr,
    rand_num: Vec<u8>,
    pause_node: Arc<RwLock<bool>>,
    address_aggregation_limit: Option<usize>,
    current_block: CurrentBlockWithMutex,
    last_pow: Option<ProofOfWork>,
    current_coinbase: Option<(String, Transaction)>,
    current_payment_address: Option<String>,
    static_miner_address: Arc<RwLock<Option<String>>>,
    aggregation_status: AggregationStatus,
    wait_partition_task: bool,
    received_utxo_set: Option<UtxoSet>,
    mining_partition_task: RunningTaskOrResult<(ProofOfWork, PowInfo, SocketAddr)>,
    mining_block_task: RunningTaskOrResult<BlockPoWInfo>,
    mining_api_key: Option<String>,
    blockchain_item_received: Option<(String, BlockchainItem, SocketAddr)>,
    api_info: (SocketAddr, Option<TlsPrivateInfo>, ApiKeys, RoutesPoWInfo),
    activation_height_asert: u64,
}

impl MinerNode {
    /// Creates a new instance of Mining implementor
    ///
    /// ### Arguments
    ///
    /// * `config`   - MinerNodeConfig object that hold the miner_nodes and miner_db_mode
    /// * `extra`  - additional parameter for construction
    pub async fn new(config: MinerNodeConfig, mut extra: ExtraNodeParams) -> Result<MinerNode> {
        let addr = config.miner_address;
        let raw_mempool_addr = config
            .mempool_nodes
            .get(config.miner_mempool_node_idx)
            .ok_or(MinerError::ConfigError("Invalid mempool index"))?;
        let mempool_addr = create_socket_addr(&raw_mempool_addr.address)
            .await
            .map_err(|_| MinerError::ConfigError("Invalid mempool node address in config file"))?;

        // Restore old keys if backup is present
        if config.backup_restore.unwrap_or(false) {
            db_utils::restore_file_backup(config.miner_db_mode, &DB_SPEC, None).unwrap();
        }

        let wallet_db = WalletDb::new(
            config.miner_db_mode,
            extra.wallet_db.take(),
            config.passphrase,
            extra.custom_wallet_spec,
        )?;
        let disable_tcp_listener = extra.disable_tcp_listener;
        let tls_addr = create_socket_addr(&addr).await.unwrap();
        let tcp_tls_config = TcpTlsConfig::from_tls_spec(tls_addr, &config.tls_config)?;
        let api_addr = SocketAddr::new(tls_addr.ip(), config.miner_api_port);
        let api_tls_info = config
            .miner_api_use_tls
            .then(|| tcp_tls_config.clone_private_info());
        let api_keys = to_api_keys(config.api_keys.clone());
        let node = Node::new(
            &tcp_tls_config,
            config.peer_limit,
            config.peer_limit,
            NodeType::Miner,
            disable_tcp_listener,
            false,
        )
        .await?;
        let api_pow_info = to_route_pow_infos(config.routes_pow.clone());
        let static_miner_address = Arc::new(RwLock::new(config.static_miner_address.clone()));
        let mining_api_key = config.mining_api_key.clone();
        let address_aggregation_limit = config.address_aggregation_limit;

        let activation_height_asert = config
            .activation_height_asert
            .unwrap_or(crate::constants::ACTIVATION_HEIGHT_ASERT);

        MinerNode {
            node,
            local_events: Default::default(),
            threaded_calls: Default::default(),
            ui_feedback_tx: Default::default(),
            wallet_db,
            mempool_addr,
            rand_num: Default::default(),
            pause_node: Arc::new(RwLock::new(false)),
            current_block: Arc::new(Mutex::new(None)),
            last_pow: None,
            current_coinbase: None,
            current_payment_address: None,
            static_miner_address,
            aggregation_status: Default::default(),
            received_utxo_set: None,
            wait_partition_task: Default::default(),
            mining_partition_task: Default::default(),
            mining_block_task: Default::default(),
            blockchain_item_received: Default::default(),
            mining_api_key,
            api_info: (api_addr, api_tls_info, api_keys, api_pow_info),
            address_aggregation_limit,
            activation_height_asert,
        }
        .load_local_db()
        .await
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
        CurrentBlockWithMutex,
        RoutesPoWInfo,
    ) {
        let (api_addr, api_tls_info, api_keys, api_pow_info) = self.api_info.clone();
        (
            self.wallet_db.clone(),
            self.node.clone(),
            api_addr,
            api_tls_info,
            api_keys,
            self.current_block.clone(),
            api_pow_info,
        )
    }

    /// Get mining API key
    pub fn get_mining_api_key(&self) -> Option<String> {
        self.mining_api_key.clone()
    }

    /// Set mining API key
    pub fn set_mining_api_key(&mut self, api_key: Option<String>) {
        self.mining_api_key = api_key;
    }

    /// Only used during initialization
    pub async fn force_set_paused(&mut self, paused: bool) {
        *self.pause_node.write().await = paused;
    }

    /// Injects a new event into miner node
    pub fn inject_next_event(
        &self,
        from_peer_addr: SocketAddr,
        data: impl Serialize,
    ) -> Result<()> {
        Ok(self.node.inject_next_event(from_peer_addr, data)?)
    }

    /// Returns the node's local endpoint.
    pub fn local_address(&self) -> SocketAddr {
        self.node.local_address()
    }

    /// Returns the node's public endpoint.
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

    /// Connect to a peer on the network.
    ///
    /// ### Arguments
    ///
    /// * `peer`   - Address of the peer to connect to.
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

    /// Send initial requests:
    /// - partition request
    pub async fn send_startup_requests(&mut self) -> Result<()> {
        info!("Send startup requests: partition");
        self.send_partition_request().await
    }

    /// Local event channel.
    pub fn local_event_tx(&self) -> &LocalEventSender {
        &self.local_events.tx
    }

    /// Local event channel.
    pub fn local_event_tx_mut(&mut self) -> &mut LocalEventSender {
        &mut self.local_events.tx
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

    /// Set static mining address
    pub async fn set_static_miner_address(&mut self, address: Option<String>) {
        *self.static_miner_address.write().await = address;
    }

    /// Get static miner address
    pub async fn get_static_miner_address(&self) -> Option<String> {
        self.static_miner_address.read().await.clone()
    }

    /// Extract persistent dbs
    pub async fn take_closed_extra_params(&mut self) -> ExtraNodeParams {
        let wallet_db = self.wallet_db.take_closed_persistent_store().await;
        ExtraNodeParams {
            wallet_db: wallet_db.in_memory(),
            ..Default::default()
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
            }) if reason == "Blockchain item received" => {
                if let Some((key, item, peer)) = self.blockchain_item_received.as_ref() {
                    log_received_blockchain_item(key, item, peer);
                } else {
                    warn!("Failed to retrieve blockchain item");
                }
            }
            Ok(Response {
                success: true,
                reason,
            }) if reason == "Received random number successfully" => {
                info!("RANDOM NUMBER RECEIVED: {:?}", self.rand_num);
            }
            Ok(Response {
                success: true,
                reason,
            }) if reason == "Partition PoW complete" => {
                if self.process_found_partition_pow().await {
                    info!("Partition Pow found and sent");
                }
            }
            Ok(Response {
                success: true,
                reason,
            }) if reason == "Pre-block received successfully" => {
                info!("PRE-BLOCK RECEIVED");
            }
            Ok(Response {
                success: true,
                reason,
            }) if reason == "Block is valid" => {
                info!("MERKLE ROOT VALID");
            }
            Ok(Response {
                success: false,
                reason,
            }) if reason == "Block is not valid" => {
                info!("MERKLE ROOT INVALID");
            }
            Ok(Response {
                success: true,
                reason,
            }) if reason == "Block PoW complete" => {
                if self.process_found_block_pow().await {
                    info!("Block PoW found and sent");
                }
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
            }) if reason == "Initiate pause node" => {
                info!("Initiate pause node");
                if let Err(e) = self
                    .node
                    .send(self.mempool_address(), MempoolRequest::RequestRemoveMiner)
                    .await
                {
                    error!("Failed to send request to remove miner: {:?}", e);
                }
            }
            Ok(Response {
                success: true,
                reason,
            }) if reason == "Sent UTXO Request" => {
                debug!("Sent UTXO Request for wallet update")
            }
            Ok(Response {
                success: false,
                reason,
            }) if reason == "Miner not authorized" => return ResponseResult::Exit,
            Ok(Response {
                success: true,
                reason,
            }) => {
                debug!("Unknown response type: {:?}", reason);
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
                _ = self.mining_partition_task.wait() => {
                    self.wait_partition_task = false;
                    return Some(Ok(Response {
                        success: true,
                        reason: "Partition PoW complete".to_string(),
                    }));
                }
                _ = self.mining_block_task.wait(), if !self.wait_partition_task => {
                    return Some(Ok(Response {
                        success: true,
                        reason: "Block PoW complete".to_string(),
                    }));
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

    /// Handles an event using handle_new_frame
    ///
    /// ### Arguments
    ///
    /// * `event`   - Event object to be handled.
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
    /// * `peer`   - Socket address of the peer sending the message.
    /// * `frame`   - Bytes object holding the frame
    async fn handle_new_frame(
        &mut self,
        peer: SocketAddr,
        frame: Bytes,
    ) -> Result<Option<Response>> {
        let req = deserialize::<MineRequest>(&frame).map_err(|error| {
            warn!(?error, "frame-deserialize");
            error
        })?;

        let req_span = error_span!("request", ?req);
        let response = self.handle_request(peer, req).instrument(req_span).await;
        debug!(?response, ?peer, "response");

        Ok(response)
    }

    /// Handles a mempool request.
    ///
    /// ### Arguments
    ///
    /// * `peer`     - Sending peer's socket address
    /// * `req`   - MineRequest object that is the recieved request
    /// TODO: Find something to do with win_coinbase. Allows to know winner
    async fn handle_request(&mut self, peer: SocketAddr, req: MineRequest) -> Option<Response> {
        use MineRequest::*;
        trace!("handle_request: {:?}", req);

        match req {
            SendBlockchainItem { key, item } => Some(self.receive_blockchain_item(peer, key, item)),
            SendBlock {
                pow_info,
                rnum,
                win_coinbases,
                reward,
                block,
                b_num,
            } => {
                self.receive_pre_block_and_random(
                    peer,
                    pow_info,
                    rnum,
                    win_coinbases,
                    reward,
                    block,
                    b_num,
                )
                .await
            }
            SendTransactions {
                tx_merkle_verification,
            } => {
                self.receive_trans_verification(tx_merkle_verification)
                    .await
            }
            SendUtxoSet { utxo_set } => Some(self.receive_utxo_set(utxo_set)),
            Closing => self.receive_closing(peer),
            MinerRemovedAck => Some(self.handle_receive_miner_removed_ack(peer).await),
            MinerNotAuthorized => Some(self.handle_receive_miner_not_authorized(peer).await),
            MinerApi(api_request) => self.handle_miner_api(peer, api_request).await,
        }
    }

    /// Handle a miner API request
    ///
    /// ## Arguments
    /// `peer` - Socket address of the peer sending the message.
    /// `api_request` - The API request to handle
    pub async fn handle_miner_api(
        &mut self,
        peer: SocketAddr,
        api_request: MineApiRequest,
    ) -> Option<Response> {
        if self.local_address() != peer {
            return None; // Only Process internal requests
        }
        match api_request {
            MineApiRequest::GetConnectionStatus => {
                Some(self.receive_connection_status_request().await)
            }
            MineApiRequest::GetMiningStatus => Some(self.receive_get_mining_status_request().await),
            MineApiRequest::InitiatePauseMining => {
                Some(self.receive_pause_node_request(peer, true).await)
            }
            MineApiRequest::InitiateResumeMining => {
                Some(self.receive_pause_node_request(peer, false).await)
            }
            MineApiRequest::ConnectToMempool => Some(self.handle_connect_to_mempool().await),
            MineApiRequest::DisconnectFromMempool => {
                Some(self.handle_disconnect_from_mempool().await)
            }
            MineApiRequest::RequestUTXOSet(addrs) => self
                .send_request_utxo_set(addrs, self.mempool_address(), NodeType::Miner)
                .await
                .ok()
                .map(|_| Response {
                    success: true,
                    reason: "Sent UTXO Request".to_string(),
                }),
            MineApiRequest::SetStaticMinerAddress { address } => {
                Some(self.handle_set_static_miner_address(address).await)
            }
            MineApiRequest::GetStaticMinerAddress => self.handle_get_static_miner_address().await,
        }
    }

    pub async fn handle_set_static_miner_address(&mut self, address: Option<String>) -> Response {
        self.set_static_miner_address(address).await;

        Response {
            success: true,
            reason: "Static miner address set".to_string(),
        }
    }

    pub async fn handle_get_static_miner_address(&mut self) -> Option<Response> {
        let static_miner_address = self.get_static_miner_address().await;
        try_send_to_ui(
            self.ui_feedback_tx.as_ref(),
            Rs2JsMsg::Value(serde_json::json!({
                "static_miner_address": static_miner_address.clone(),
            })),
        )
        .await;
        None
    }

    /// Handle miner not being authorized
    pub async fn handle_receive_miner_not_authorized(&mut self, peer: SocketAddr) -> Response {
        if self.mempool_address() == peer {
            *self.pause_node.write().await = true;
            try_send_to_ui(
                self.ui_feedback_tx.as_ref(),
                Rs2JsMsg::Value(serde_json::json!({
                    "mining": false,
                })),
            )
            .await;
            try_send_to_ui(
                self.ui_feedback_tx.as_ref(),
                Rs2JsMsg::Error {
                    error: "Miner not authorized".to_string(),
                },
            )
            .await;
            Response {
                success: false,
                reason: "Miner not authorized".to_string(),
            }
        } else {
            Response {
                success: false,
                reason: "Received miner unauthorized notification from non-mempool peer"
                    .to_string(),
            }
        }
    }

    /// Handle acknowledgement of miner removed from mempool node
    pub async fn handle_receive_miner_removed_ack(&mut self, peer: SocketAddr) -> Response {
        if self.mempool_address() == peer {
            *self.pause_node.write().await = true;
            try_send_to_ui(
                self.ui_feedback_tx.as_ref(),
                Rs2JsMsg::Value(serde_json::json!({
                    "mining": false,
                })),
            )
            .await;
            Response {
                success: true,
                reason: "Node is paused".to_string(),
            }
        } else {
            Response {
                success: false,
                reason: "Received miner removed ack from non-mempool peer".to_string(),
            }
        }
    }

    /// Handle disconnect from mempool node
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
        try_send_to_ui(
            self.ui_feedback_tx.as_ref(),
            Rs2JsMsg::Value(serde_json::json!({
                "connected": true,
            })),
        )
        .await;
        // We do not send startup requests here,
        // because we don't necessarily want to start mining
        Response {
            success: true,
            reason: "Connected to mempool".to_string(),
        }
    }

    /// Handles a request to pause the node
    pub async fn receive_pause_node_request(&mut self, _peer: SocketAddr, pause: bool) -> Response {
        if self.is_disconnected().await {
            try_send_to_ui(
                self.ui_feedback_tx.as_ref(),
                Rs2JsMsg::Value(serde_json::json!({
                    "mining": false,
                })),
            )
            .await;

            return Response {
                success: false,
                reason: "Node is disconnected".to_string(),
            };
        }

        // Request came from the miner node itself,
        // which means we need to send a request to
        // the mempool node to have the miner removed
        if pause {
            // Pause mining
            Response {
                success: true,
                reason: "Initiate pause node".to_string(),
            }
        } else {
            // Resume mining
            if let Err(err) = self.send_startup_requests().await {
                error!("Failed to send startup requests: {}", err);
                try_send_to_ui(
                    self.ui_feedback_tx.as_ref(),
                    Rs2JsMsg::Error {
                        error: "Failed to send startup requests on reconnection".to_owned(),
                    },
                )
                .await;
                return Response {
                    success: false,
                    reason: "Failed to send startup requests on reconnection".to_string(),
                };
            }
            *self.pause_node.write().await = false;
            try_send_to_ui(
                self.ui_feedback_tx.as_ref(),
                Rs2JsMsg::Value(serde_json::json!({
                    "mining": true,
                })),
            )
            .await;
            Response {
                success: true,
                reason: "Node is resumed".to_string(),
            }
        }
    }

    /// Handles a request to get the mining status
    pub async fn receive_get_mining_status_request(&self) -> Response {
        if self.is_disconnected().await || *self.pause_node.read().await {
            try_send_to_ui(
                self.ui_feedback_tx.as_ref(),
                Rs2JsMsg::Value(serde_json::json!({
                    "mining": false,
                })),
            )
            .await;
            return Response {
                success: true,
                reason: "Node is not mining".to_string(),
            };
        }
        try_send_to_ui(
            self.ui_feedback_tx.as_ref(),
            Rs2JsMsg::Value(serde_json::json!({
                "mining": true,
            })),
        )
        .await;
        Response {
            success: true,
            reason: "Node is mining".to_string(),
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

    /// Return the blockchain item received
    pub async fn get_blockchain_item_received(
        &mut self,
    ) -> &Option<(String, BlockchainItem, SocketAddr)> {
        &self.blockchain_item_received
    }

    /// Threaded call channel.
    pub fn threaded_call_tx(&self) -> &ThreadedCallSender<MinerNode> {
        &self.threaded_calls.tx
    }

    /// Sends a request to retrieve a blockchain item from storage
    ///
    /// ### Arguments
    ///
    /// * `key`  - The blockchain item key.
    #[allow(unused)]
    pub async fn request_blockchain_item(
        &mut self,
        key: String,
        storage_node_addr: SocketAddr,
    ) -> Result<()> {
        self.blockchain_item_received = None;
        self.node
            .send(storage_node_addr, StorageRequest::GetBlockchainItem { key })
            .await?;
        Ok(())
    }

    /// Receives a new block to be mined
    ///
    /// ### Arguments
    ///
    /// * `peer`     - Sending peer's socket address
    /// * `pre_block` - New block to be mined
    /// * `reward`    - The block reward to be paid on successful PoW
    #[allow(clippy::too_many_arguments)]
    async fn receive_pre_block_and_random(
        &mut self,
        peer: SocketAddr,
        pow_info: PowInfo,
        rand_num: Vec<u8>,
        win_coinbases: Vec<String>,
        reward: TokenAmount,
        pre_block: Option<BlockHeader>,
        b_num: u64,
    ) -> Option<Response> {
        let process_rnd = self
            .receive_random_number(peer, pow_info, rand_num, win_coinbases, b_num)
            .await;
        let process_block = if let Some(pre_block) = pre_block {
            self.receive_pre_block(peer, pre_block, reward).await
        } else {
            false
        };

        self.wallet_db.filter_locked_coinbase(b_num).await;
        // TODO: should we check even if coinbase was not committed?
        self.check_for_threshold_and_send_aggregation_tx(b_num)
            .await;

        match (process_rnd, process_block) {
            (true, false) => Some(Response {
                success: true,
                reason: "Received random number successfully".to_string(),
            }),
            (_, true) => Some(Response {
                success: true,
                reason: "Pre-block received successfully".to_string(),
            }),
            (false, false) => None,
        }
    }

    /// Handles the item of the random number of partitioning
    ///
    /// ### Arguments
    ///
    /// * `peer`     - Sending peer's socket address
    /// * `rand_num` - random num to be recieved in Vec<u8>
    async fn receive_random_number(
        &mut self,
        peer: SocketAddr,
        pow_info: PowInfo,
        rand_num: Vec<u8>,
        win_coinbases: Vec<String>,
        b_num: u64,
    ) -> bool {
        if peer != self.mempool_address() {
            return false;
        }

        if self.rand_num == rand_num {
            self.process_found_partition_pow().await;
            return false;
        }

        // Commit our previous winnings if present
        if self.is_current_coinbase_found(&win_coinbases) {
            self.commit_found_coinbase(b_num).await;
        }

        self.start_generate_partition_pow(peer, pow_info, rand_num)
            .await;
        true
    }

    /// Receives a new block to be mined
    ///
    /// ### Arguments
    ///
    /// * `peer`     - Sending peer's socket address
    /// * `pre_block` - New block to be mined
    /// * `reward`    - The block reward to be paid on successful PoW
    async fn receive_pre_block(
        &mut self,
        peer: SocketAddr,
        pre_block: BlockHeader,
        reward: TokenAmount,
    ) -> bool {
        if peer != self.mempool_address() {
            return false;
        }

        let new_block = BlockPoWReceived {
            block: pre_block,
            reward,
        };

        let new_b_num = Some(new_block.block.b_num);
        let current_b_num = self
            .current_block
            .lock()
            .await
            .as_ref()
            .map(|c| c.block.b_num);
        if new_b_num <= current_b_num {
            if new_b_num == current_b_num {
                info!("Received block with same b_num as current block");
                info!("Current block: {:?}", self.current_block.lock().await);

                self.process_found_block_pow().await;
            }

            warn!("Received block with b_num less than current block");
            return false;
        }

        self.start_generate_pow_for_current_block(peer, new_block)
            .await;
        true
    }

    /// Verifies the block by checking the transactions using a merkle tree.
    ///
    /// ### Arguments
    ///
    /// * `transactions`     - Vec<String>. transactions used to build the merkle tree
    pub async fn receive_trans_verification(
        &self,
        tx_merkle_verification: Vec<String>,
    ) -> Option<Response> {
        let current_block_info = self.current_block.lock().await.clone().unwrap();
        let merkle_root = current_block_info.block.txs_merkle_root_and_hash.0.clone();
        let mut valid = true;

        if !merkle_root.is_empty() {
            let (mtree, _) = block::build_merkle_tree(&tx_merkle_verification)
                .await
                .unwrap();
            valid = hex::encode(mtree.root()) == merkle_root;
        }

        if valid {
            Some(Response {
                success: true,
                reason: "Block is valid".to_string(),
            })
        } else {
            Some(Response {
                success: false,
                reason: "Block is not valid".to_string(),
            })
        }
    }

    /// Util function to get a socket address for PID table checks
    fn get_comparison_addr(&self) -> SocketAddr {
        let comparison_port = self.local_address().port() + 1;
        let mut comparison_addr = self.local_address();

        comparison_addr.set_ip(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)));
        comparison_addr.set_port(comparison_port);

        comparison_addr
    }

    /// Process the found PoW sending it to the related peer and logging errors
    pub async fn process_found_block_pow(&mut self) -> bool {
        trace!("Current mining block task: {:?}", self.mining_block_task);

        let BlockPoWInfo {
            peer,
            start_time,
            header:
                BlockHeader {
                    b_num,
                    nonce_and_mining_tx_hash: (nonce, coinbase_hash),
                    ..
                },
            coinbase,
            ..
        } = match self.mining_block_task.completed_result() {
            Some(Ok(v)) => v.clone(),
            Some(Err(e)) => {
                let error = format!("process_found_block_pow PoW {:?}", e);
                error!("{:?}", &error);
                try_send_to_ui(self.ui_feedback_tx.as_ref(), Rs2JsMsg::Error { error }).await;
                return false;
            }
            None => {
                trace!("process_found_block_pow PoW Not ready yet");
                return false;
            }
        };

        if let Ok(elapsed) = start_time.elapsed() {
            debug!("Found block in {}ms", elapsed.as_millis());
        }

        let is_paused = *self.pause_node.read().await;

        if !is_paused {
            if let Err(e) = self.send_pow(peer, b_num, nonce, coinbase.clone()).await {
                let error = format!("process_found_block_pow PoW {:?}", e);
                error!("{:?}", &error);
                try_send_to_ui(self.ui_feedback_tx.as_ref(), Rs2JsMsg::Error { error }).await;
                return false;
            }

            self.current_coinbase = store_last_coinbase(
                &self.wallet_db,
                Some((coinbase_hash.clone(), coinbase.clone())),
            )
            .await;
        }

        true
    }

    /// Sends PoW to a mempool node.
    ///
    /// ### Arguments
    ///
    /// * `peer`   - Socket address of recipient
    /// * `nonce`   - sequence number of a block in Vec<u8>
    /// * `coinbase`   - Transaction object
    pub async fn send_pow(
        &mut self,
        peer: SocketAddr,
        block_num: u64,
        nonce: Vec<u8>,
        coinbase: Transaction,
    ) -> Result<()> {
        self.node
            .send(
                peer,
                MempoolRequest::SendPoW {
                    block_num,
                    nonce,
                    coinbase,
                },
            )
            .await?;
        Ok(())
    }

    /// Process the found Pow sending it to the related peer and logging errors
    pub async fn process_found_partition_pow(&mut self) -> bool {
        let (partition_entry, p_info, peer) = match self.mining_partition_task.completed_result() {
            Some(Ok((e, p_info, p))) => (e.clone(), *p_info, *p),
            Some(Err(e)) => {
                let error = format!("process_found_partition_pow PoW {:?}", e);
                error!("{:?}", &error);
                try_send_to_ui(self.ui_feedback_tx.as_ref(), Rs2JsMsg::Error { error }).await;
                return false;
            }
            None => {
                trace!("process_found_partition_pow PoW Not ready yet");
                return false;
            }
        };
        let is_paused = *self.pause_node.read().await;
        if !is_paused {
            if let Err(e) = self.send_partition_pow(peer, p_info, partition_entry).await {
                let error = format!("process_found_partition_pow PoW {:?}", e);
                error!("{:?}", &error);
                try_send_to_ui(self.ui_feedback_tx.as_ref(), Rs2JsMsg::Error { error }).await;
                return false;
            }
        }

        true
    }

    /// Sends the light partition PoW to a mempool node
    ///
    /// ### Arguments
    ///
    /// * `peer`   - Socket address of recipient/peer
    /// * `partition_entry`   - partition ProofOfWork being sent
    pub async fn send_partition_pow(
        &mut self,
        peer: SocketAddr,
        pow_info: PowInfo,
        partition_entry: ProofOfWork,
    ) -> Result<()> {
        self.node
            .send(
                peer,
                MempoolRequest::SendPartitionEntry {
                    pow_info,
                    partition_entry,
                },
            )
            .await?;
        Ok(())
    }

    /// Sends a request to partition to a Mempool node
    pub async fn send_partition_request(&mut self) -> Result<()> {
        let peer_span = error_span!("sending partition participation request");
        self.node
            .send(
                self.mempool_addr,
                MempoolRequest::SendPartitionRequest {
                    mining_api_key: self.mining_api_key.clone(),
                },
            )
            .instrument(peer_span)
            .await?;

        Ok(())
    }

    /// Check if block found include our wining mining tx
    fn is_current_coinbase_found(&self, win_coinbases: &[String]) -> bool {
        if let Some((tx_hash, _)) = &self.current_coinbase {
            win_coinbases.contains(tx_hash)
        } else {
            false
        }
    }

    /// Commit our winning mining tx to wallet
    async fn commit_found_coinbase(&mut self, b_num: u64) {
        trace!("Committing our latest winning");
        self.current_payment_address = Some(
            self.get_static_miner_address()
                .await
                .unwrap_or(generate_mining_address(&mut self.wallet_db).await),
        );
        let (hash, transaction) = std::mem::replace(
            &mut self.current_coinbase,
            store_last_coinbase(&self.wallet_db, None).await,
        )
        .unwrap();

        let payments = get_payments_for_wallet(Some((&hash, &transaction)).into_iter());

        let mut assets_won = Asset::Token(TokenAmount(0));

        for (_, asset, _, _) in &payments {
            assets_won.add_assign(asset);
        }

        debug!(
            "WON {:?} TOKENS FOR MINING ROUND {:?}",
            assets_won,
            b_num - 1
        );

        self.wallet_db
            .save_usable_payments_to_wallet(payments, b_num, false)
            .await
            .unwrap();

        // Backup wallet after committing the coinbase
        self.wallet_db.backup_persistent_store().await.unwrap();

        // Notify the end user that a winning PoW has been found
        try_send_to_ui(
            self.ui_feedback_tx.as_ref(),
            Rs2JsMsg::Success {
                success: "Found winning PoW".to_string(),
            },
        )
        .await;
    }

    /// Checks and aggregates all the winnings into a single address if the number of addresses stored
    /// breaches the set threshold `MAX_NO_OF_WINNINGS_HELD`
    async fn check_for_threshold_and_send_aggregation_tx(&mut self, b_num: u64) {
        let address_aggregation_limit = self.address_aggregation_limit.unwrap_or(INTERNAL_TX_LIMIT);

        match self.aggregation_status.clone() {
            AggregationStatus::Idle => {
                trace!(
                    "Checking if we are holding more than {address_aggregation_limit:?} addresses to trigger aggregation tx"
                );

                // All last known addresses
                let known_addresses = self.wallet_db.get_known_addresses();

                // Check if we have a reached the threshold of addresses stored
                if known_addresses.len() >= address_aggregation_limit {
                    trace!("Winnings aggregation triggered");

                    // Slice known addresses up to address_aggregation_limit
                    let addresses_to_aggregate = known_addresses
                        .iter()
                        .take(address_aggregation_limit)
                        .cloned()
                        .collect::<BTreeSet<_>>();

                    // Fetch the aggregating transaction inputs and outputs
                    let (tx_ins, tx_outs) = self
                        .wallet_db
                        .fetch_tx_ins_and_tx_outs_merge_input_addrs(addresses_to_aggregate, None)
                        .await
                        .unwrap();

                    // Sign the inputs
                    let key_material = self.wallet_db.get_key_material(&tx_ins);
                    let tx_ins = update_input_signatures(&tx_ins, &tx_outs, &key_material);

                    // Aggregation address is last generated address,
                    // which is generated by passing `None` as the `excess_address`
                    // to `fetch_tx_ins_and_tx_outs_merge_input_addrs`
                    let aggregating_addr = self.wallet_db.get_last_generated_address().unwrap(); // Should panic if `None`

                    trace!(
                        "Aggregating {:?} assets to {:?}",
                        tx_ins.len(),
                        aggregating_addr
                    );

                    // Construct aggregation transaction
                    let aggregating_tx = construct_tx_core(tx_ins, tx_outs, None);

                    trace!("Sending aggregation tx to mempool node");

                    // Send aggregating Transaction to mempool node
                    if let Err(e) = self
                        .send_transactions_to_mempool(
                            self.mempool_addr,
                            vec![aggregating_tx.clone()],
                        )
                        .await
                    {
                        let error = format!("Error sending aggregation tx to mempool nodes: {e:?}");
                        error!("{:?}", &e);
                        try_send_to_ui(self.ui_feedback_tx.as_ref(), Rs2JsMsg::Error { error })
                            .await;
                        // Return if sending to mempool has failed
                        return;
                    }

                    // After aggregation, our wallets will hold only 2 addresses: one for the holding all the winnings
                    // and the other for the excess amount(which will be `0` theoretically).

                    // TODO: Should we update the wallet DB here, or only once we've got confirmation
                    // from mempool node through received UTXO set?
                    self.wallet_db
                        .store_payment_transaction(aggregating_tx, b_num)
                        .await;

                    trace!("Pruning the wallet of old keys after aggregation");
                    self.wallet_db
                        .destroy_spent_transactions_and_keys(None)
                        .await;

                    self.aggregation_status = AggregationStatus::UtxoUpdate(aggregating_addr);
                }
            }
            AggregationStatus::UtxoUpdate(aggregation_addr) => {
                // Request for UTXO set to confirm that aggregation tx
                // has went through the previous time.
                let mempool_addr = self.mempool_address();

                if let Err(e) = self
                    .send_request_utxo_set(
                        UtxoFetchType::AnyOf(vec![aggregation_addr.clone()]),
                        mempool_addr,
                        NodeType::Miner,
                    )
                    .await
                {
                    let error = format!("Error sending UTXO request to mempool nodes: {e:?}");
                    error!("{:?}", &error);
                    try_send_to_ui(self.ui_feedback_tx.as_ref(), Rs2JsMsg::Error { error }).await;
                } else {
                    trace!("Sending UTXO request from Miner node to confirm our previous aggregation of winnings");
                }
            }
        }
    }

    /// Generates a valid PoW for a block specifically
    /// TODO: Update the numbers used for reward and block time
    /// * `peer`      - Peer to send PoW to
    /// * `new_block` - Block for PoW
    pub async fn start_generate_pow_for_current_block(
        &mut self,
        peer: SocketAddr,
        new_block: BlockPoWReceived,
    ) {
        let b_num = new_block.block.b_num;
        let current_payment_address = self.current_payment_address.clone().unwrap();

        let mining_tx = construct_coinbase_tx(b_num, new_block.reward, current_payment_address);
        let mining_tx_hash = construct_tx_hash(&mining_tx);

        self.mining_block_task = {
            let header = apply_mining_tx(new_block.block.clone(), Vec::new(), mining_tx_hash);
            let start_time = SystemTime::now();
            RunningTaskOrResult::Running(Self::generate_pow_for_block(BlockPoWInfo {
                peer,
                start_time,
                header,
                coinbase: mining_tx,
            }))
        };
        let mut current_block = self.current_block.lock().await;
        *current_block = Some(new_block);
    }

    /// Generates and returns the nonce of a block.active_raft
    /// Validates the POW using the block, hash and nonce
    ///
    /// ### Arguments
    ///
    /// * `info`      - Block Proof of work info
    fn generate_pow_for_block(mut info: BlockPoWInfo) -> task::JoinHandle<BlockPoWInfo> {
        task::spawn_blocking(move || {
            info.header.nonce_and_mining_tx_hash.0 = generate_pow_for_block(&info.header)
                .expect("error occurred while mining block")
                // TODO: We should make BlockPoWInfo actually indicate if no PoW could be found
                .expect("couldn't find a valid nonce");

            trace!(
                "Found possible POW for block: {:?}",
                info.header.nonce_and_mining_tx_hash.0
            );
            info
        })
    }

    /// Generates a valid Partition PoW
    ///
    /// ### Arguments
    ///
    /// * `peer`     - Peer to send PoW to
    /// * `rand_num` - random num for PoW
    pub async fn start_generate_partition_pow(
        &mut self,
        peer: SocketAddr,
        pow_info: PowInfo,
        rand_num: Vec<u8>,
    ) {
        // Here we need to generate a proof-of-work based on the public address
        // resolved by the mempool node.
        let address_proof = if let Some(public_addr) = self.public_address().await {
            format_parition_pow_address(public_addr)
        } else {
            let msg: &str = "No public address found for partition PoW";
            try_send_to_ui(
                self.ui_feedback_tx.as_ref(),
                Rs2JsMsg::Error {
                    error: msg.to_string(),
                },
            )
            .await;
            error!("{}", msg);
            return;
        };

        self.wait_partition_task = true;
        self.mining_partition_task = RunningTaskOrResult::Running(utils::generate_pow_for_address(
            peer,
            pow_info,
            address_proof,
            Some(rand_num.clone()),
        ));
        self.rand_num = rand_num;
    }

    /// Returns the last PoW.
    pub fn last_pow(&self) -> &Option<ProofOfWork> {
        &self.last_pow
    }

    /// Get the wallet db
    pub fn get_wallet_db(&self) -> &WalletDb {
        &self.wallet_db
    }

    /// Get locked coinbase
    pub async fn get_locked_coinbase(&self) -> LockedCoinbase {
        self.wallet_db.get_locked_coinbase().await
    }

    /// Load and apply the local database to our state
    async fn load_local_db(mut self) -> Result<Self> {
        if let Err(e) = self.wallet_db.load_locked_coinbase().await {
            error!("load_local_db: load_locked_coinbase {:?}", e);
            warn!("load_local_db: generating new locked coinbase from UTXO set");
            // Existing locked coinbase failed to deserialize, so we need to
            // generate a new one using a UTXO subset from the mempool node
            let all_known_addresses = self.wallet_db.get_known_addresses();
            let request = MempoolRequest::SendUtxoRequest {
                address_list: UtxoFetchType::AnyOf(all_known_addresses),
                requester_node_type: NodeType::Miner,
            };

            if let Err(e) = self.node.send(self.mempool_addr, request).await {
                error!("load_local_db: send UtxoRequest {:?}", e);
            }
        }
        self.current_coinbase = if let Some(cb) = load_last_coinbase(&self.wallet_db).await? {
            debug!("load_local_db: current_coinbase {:?}", cb);
            Some(cb)
        } else {
            None
        };

        self.current_payment_address = match self.get_static_miner_address().await {
            Some(static_address) => {
                warn!("using static miner address: {:?}", static_address);
                Some(static_address)
            }
            None => {
                if let Some(addr) = load_mining_address(&self.wallet_db).await? {
                    debug!("load_local_db: current_payment_address {:?}", addr);
                    Some(addr)
                } else {
                    Some(generate_mining_address(&mut self.wallet_db).await)
                }
            }
        };

        Ok(self)
    }

    /// Get `Node` member
    pub fn get_node(&self) -> &Node {
        &self.node
    }

    /// Returns `Some` if the miner has sent an aggregation tx and has not received the UTXO set yet.
    pub fn has_aggregation_tx_active(&self) -> Option<String> {
        match self.aggregation_status.clone() {
            AggregationStatus::UtxoUpdate(addr) => Some(addr),
            _ => None,
        }
    }
}

impl MinerInterface for MinerNode {
    fn receive_blockchain_item(
        &mut self,
        peer: SocketAddr,
        key: String,
        item: BlockchainItem,
    ) -> Response {
        self.blockchain_item_received =
            Some((key, item, peer)).filter(|(_, i, _)| !i.data.is_empty());
        Response {
            success: true,
            reason: "Blockchain item received".to_string(),
        }
    }
}

#[async_trait]
impl Transactor for MinerNode {
    type Error = MinerError;

    async fn send_transactions_to_mempool(
        &mut self,
        mempool_peer: SocketAddr,
        transactions: Vec<Transaction>,
    ) -> Result<()> {
        let _peer_span = info_span!("sending transactions to mempool node for processing");
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

        // Reset aggregation status.
        self.aggregation_status = AggregationStatus::Idle;

        Response {
            success: true,
            reason: "Received UTXO set".to_string(),
        }
    }

    async fn update_running_total(&mut self) {
        let utxo_set = self.received_utxo_set.take();
        let payments = get_payments_for_wallet_from_utxo(utxo_set.into_iter().flatten());
        let b_num = self
            .current_block
            .lock()
            .await
            .as_ref()
            .map(|c| c.block.b_num)
            .unwrap();
        self.wallet_db
            .save_usable_payments_to_wallet(payments, b_num, false)
            .await
            .unwrap();
    }
}

/// Load mining address from wallet
async fn load_mining_address(wallet_db: &WalletDb) -> Result<Option<String>> {
    Ok(wallet_db
        .get_db_value(MINING_ADDRESS_KEY)
        .await
        .map(|v| deserialize(&v))
        .transpose()?)
}

/// Generate mining address storing it in wallet
async fn generate_mining_address(wallet_db: &mut WalletDb) -> String {
    let addr: String = wallet_db.generate_payment_address().0;
    let ser_addr = serialize(&addr).unwrap();
    wallet_db.set_db_value(MINING_ADDRESS_KEY, ser_addr).await;
    addr
}

/// Load last coinbase from wallet
async fn load_last_coinbase(wallet_db: &WalletDb) -> Result<Option<(String, Transaction)>> {
    Ok(wallet_db
        .get_db_value(LAST_COINBASE_KEY)
        .await
        .map(|v| deserialize(&v))
        .transpose()?)
}

/// Store last coinbase in wallet
async fn store_last_coinbase(
    wallet_db: &WalletDb,
    coinbase: Option<(String, Transaction)>,
) -> Option<(String, Transaction)> {
    if let Some(cb) = &coinbase {
        let ser_cb = serialize(cb).unwrap();
        wallet_db.set_db_value(LAST_COINBASE_KEY, ser_cb).await;
    } else {
        wallet_db.delete_db_value(LAST_COINBASE_KEY).await;
    }
    coinbase
}

/// Log the received blockchain item
fn log_received_blockchain_item(_key: &str, item: &BlockchainItem, _peer: &SocketAddr) {
    use DeserializedBlockchainItem::*;
    match DeserializedBlockchainItem::from_item(item) {
        CurrentBlock(b, b_num, tx_len) => info!(
            "Successfully received blockchain item: b_num = {}({}), tx_len = {}, previous_hash = {:?}",
            b.block.header.b_num, b.block.header.b_num == b_num, tx_len, b.block.header.previous_hash
        ),
        CurrentTx(tx, b_num, tx_num) => info!(
            "Successfully received blockchain item: b_num = {}, tx_num = {}, tx_in={}, tx_out={}",
            b_num,
            tx_num,
            tx.inputs.len(),
            tx.outputs.len()
        ),
        VersionErr(v) => warn!("Unsupported blockchain item version {}", v),
        SerializationErr(e) => warn!("Failed to deserialize blockchain item {:?}", e),
    }
}
