use crate::comms_handler::{CommsError, Event, Node};
use crate::configurations::UserNodeConfig;
use crate::constants::PEER_LIMIT;
use crate::interfaces::{ComputeRequest, NodeType, Response, UseInterface, UserRequest};
use crate::wallet::WalletDb;
use bincode::deserialize;
use bytes::Bytes;
use naom::primitives::asset::{Asset, TokenAmount};
use naom::primitives::transaction::{Transaction, TxIn};
use naom::primitives::transaction_utils::{construct_payment_tx, construct_tx_hash};
use std::collections::BTreeMap;
use std::{error::Error, fmt, net::SocketAddr};
use tokio::task;
use tracing::{debug, error_span, info_span, warn};
use tracing_futures::Instrument;

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
    pub node: Node,
    pub assets: Vec<Asset>,
    pub trading_peer: Option<(SocketAddr, TokenAmount)>,
    pub next_payment: Option<Transaction>,
    pub return_payment: Option<ReturnPayment>,
    pub wallet_db: WalletDb,
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
            trading_peer: None,
            next_payment: None,
            return_payment: None,
            wallet_db: WalletDb::new(config.user_db_mode),
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
        let req = deserialize::<UserRequest>(&frame).map_err(|error| {
            warn!(?error, "frame-deserialize");
            error
        })?;

        let req_span = error_span!("request", ?req);
        let response = self.handle_request(peer, req).instrument(req_span).await;
        debug!(?response, ?peer, "response");

        Ok(response)
    }

    /// Handles a compute request.
    async fn handle_request(&mut self, peer: SocketAddr, req: UserRequest) -> Response {
        use UserRequest::*;
        println!("RECEIVED REQUEST: {:?}", req);

        match req {
            SendAddressRequest { amount } => self.receive_payment_address_request(peer, amount),
            SendPaymentTransaction { transaction } => {
                self.receive_payment_transaction(transaction).await
            }
            SendPaymentAddress { address, amount } => {
                self.make_payment_transactions(address, amount)
            }
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
    pub async fn receive_payment_transaction(&mut self, transaction: Transaction) -> Response {
        let hash = construct_tx_hash(&transaction);
        let total_to_save = transaction.outputs.iter().map(|out| out.amount).sum();

        self.wallet_db
            .save_payment_to_wallet(hash, total_to_save)
            .await
            .unwrap();

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
    pub fn make_payment_transactions(&mut self, address: String, amount: TokenAmount) -> Response {
        let tx_ins = self.fetch_inputs_for_payment(amount);

        let payment_tx =
            construct_payment_tx(tx_ins, address, None, None, Asset::Token(amount), amount);
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
    pub fn fetch_inputs_for_payment(&mut self, amount_required: TokenAmount) -> Vec<TxIn> {
        let mut tx_ins = Vec::new();

        // Wallet DB handling
        let mut fund_store = self
            .wallet_db
            .get_fund_store()
            .unwrap_or_else(WalletDb::default_fund_store);

        // Ensure we have enough funds to proceed with payment
        if fund_store.running_total.0 < amount_required.0 {
            panic!("Not enough funds available for payment!");
        }

        // Start fetching TxIns
        let mut amount_made = TokenAmount(0);
        let tx_hashes: Vec<_> = fund_store.transactions.keys().cloned().collect();

        // Start adding amounts to payment and updating FundStore
        for tx_hash in tx_hashes {
            let current_amount = *fund_store.transactions.get(&tx_hash).unwrap();

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
                let return_tx_in = self
                    .wallet_db
                    .construct_tx_in_from_prev_out(tx_hash.clone(), false);
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
            let tx_in = self
                .wallet_db
                .construct_tx_in_from_prev_out(tx_hash.clone(), true);
            tx_ins.push(tx_in);

            fund_store.transactions.remove(&tx_hash);
        }

        // Save the updated fund store to disk
        self.wallet_db.set_fund_store(fund_store);

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
        return_amt: TokenAmount,
    ) -> Result<()> {
        let tx_ins = vec![tx_in];
        let (address, _) = self.wallet_db.generate_payment_address().await;

        let payment_tx = construct_payment_tx(
            tx_ins,
            address.address.clone(),
            None,
            None,
            Asset::Token(return_amt),
            return_amt,
        );
        let payment_tx_hash = construct_tx_hash(&payment_tx);

        // Update saves to the wallet
        self.wallet_db
            .save_transaction_to_wallet(payment_tx_hash.clone(), address)
            .await
            .unwrap();
        self.wallet_db
            .save_payment_to_wallet(payment_tx_hash, return_amt)
            .await
            .unwrap();

        // Completely reallocate the payment tx; required because an unwrap will just
        // consume self
        let mut current_r_payment = self.return_payment.clone().unwrap();
        current_r_payment.transaction = payment_tx;
        self.return_payment = Some(current_r_payment);

        Ok(())
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
    pub async fn send_address_request(
        &mut self,
        peer: SocketAddr,
        amount: TokenAmount,
    ) -> Result<()> {
        let _peer_span = info_span!("sending payment address request");
        println!("Sending request for payment address to peer: {:?}", peer);

        self.node
            .send(peer, UserRequest::SendAddressRequest { amount })
            .await?;

        Ok(())
    }

    /// Sends a payment address from a request
    ///
    /// ### Arguments
    ///
    /// * `peer`    - Socket address of peer to send the address to
    pub async fn send_address_to_trading_peer(&mut self) -> Result<()> {
        let (peer, amount) = self.trading_peer.take().unwrap();
        let (address, _) = self.wallet_db.generate_payment_address().await;
        let address = address.address;
        println!("Address to send: {:?}", address);

        self.node
            .send(peer, UserRequest::SendPaymentAddress { address, amount })
            .await?;
        Ok(())
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
