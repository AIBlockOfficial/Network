use crate::primitives::transaction::OutPoint;

/// Determines whether a transaction has been spent before
///
/// ### Arguments
///
/// * `prev_out`    - OutPoint of previous transaction
pub fn tx_has_spent(prev_out: Option<OutPoint>) -> bool {
    if let Some(o) = prev_out {
        // TODO: Handle check for prev out
    }

    // If tx has no outpoint then this tx is a coinbase receipt
    // TODO: NB: NEED SOME CHECK ON NULL OUTPOINTS BEYOND ASSUMING COINBASE
    false
}
