use naom::primitives::asset::TokenAmount;
use naom::primitives::transaction::OutPoint;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A reference to fund stores, where `transactions` contains the hash
/// of the transaction and its holding amount
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct FundStore {
    running_total: TokenAmount,
    transactions: BTreeMap<OutPoint, TokenAmount>,
    spent_transactions: BTreeMap<OutPoint, TokenAmount>,
}

impl FundStore {
    pub fn running_total(&self) -> TokenAmount {
        self.running_total
    }

    pub fn transactions(&self) -> &BTreeMap<OutPoint, TokenAmount> {
        &self.transactions
    }

    pub fn into_transactions(self) -> BTreeMap<OutPoint, TokenAmount> {
        self.transactions
    }

    pub fn store_tx(&mut self, out_p: OutPoint, amount: TokenAmount) {
        if let Some(old_amount) = self.transactions.insert(out_p, amount) {
            if old_amount != amount {
                panic!("Try to insert existing transaction with different amount");
            }
        } else {
            self.running_total += amount;
        }
    }

    pub fn spend_tx(&mut self, out_p: &OutPoint) {
        if let Some((out_p_v, amount)) = self.transactions.remove_entry(out_p) {
            self.running_total -= amount;
            if self.spent_transactions.insert(out_p_v, amount).is_some() {
                panic!("Try to spend already spent transaction {:?}", out_p);
            }
        }
    }
}
