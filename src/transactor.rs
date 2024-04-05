use crate::interfaces::{NodeType, UtxoFetchType, UtxoSet};
use crate::Response;
use async_trait::async_trait;
use std::net::SocketAddr;
use tw_chain::primitives::transaction::Transaction;

/// A common trait that can be implemented by nodes as necessary to
/// build transactions from their local wallets.
// TODO: Expand this trait by finding more common ground between UserNode and MinerNode
#[async_trait]
pub trait Transactor {
    type Error;

    /// Sends the next internal payment transaction to be processed by the connected Mempool
    /// node
    ///
    /// ### Arguments
    ///
    /// * `mempool_peer` - Mempool peer to send the payment tx to
    /// * `transactions` - Transactions to send
    async fn send_transactions_to_mempool(
        &mut self,
        mempool_peer: SocketAddr,
        transactions: Vec<Transaction>,
    ) -> Result<(), Self::Error>;

    /// Send a request to the mempool nodes to receive latest UTXO set
    ///
    /// ### Arguments
    /// * `address_list` - List of addresses for which UTXOs are requested
    async fn send_request_utxo_set(
        &mut self,
        address_list: UtxoFetchType,
        mempool_addr: SocketAddr,
        requester_node_type: NodeType,
    ) -> Result<(), Self::Error>;

    /// Receive the requested UTXO set/subset from Mempool
    ///
    /// ### Arguments
    ///
    /// * `utxo_set` - The requested UTXO set
    fn receive_utxo_set(&mut self, utxo_set: UtxoSet) -> Response;

    /// Updates the local running total with the latest received UTXO set
    async fn update_running_total(&mut self);
}
