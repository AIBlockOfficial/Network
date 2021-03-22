use crate::configurations::WalletTxSpec;
use crate::utils::{create_valid_transaction_with_ins_outs, make_wallet_tx_info};
use bincode::{deserialize, serialize};
use naom::primitives::asset::TokenAmount;
use naom::primitives::transaction::{OutPoint, Transaction};
use naom::primitives::transaction_utils::{construct_address, get_tx_out_with_out_point};
use sodiumoxide::crypto::sign::ed25519::{PublicKey, SecretKey};
use std::collections::BTreeMap;

/// Key material to generate transactions
#[derive(Clone, Debug)]
pub struct TransactionsKeys {
    pub pk: PublicKey,
    pub sk: SecretKey,
    pub address: String,
}

/// Maintain a list of valid transactions to submit
#[derive(Clone, Debug)]
pub struct TransactionGen {
    addr_to_keys: BTreeMap<String, TransactionsKeys>,
    up_to_date_with_snapshot: bool,
    pending: BTreeMap<String, Transaction>,
    ready: BTreeMap<String, Vec<(OutPoint, TokenAmount)>>,
}

impl TransactionGen {
    /// New generator with seed infos
    pub fn new(tx_specs: Vec<WalletTxSpec>) -> Self {
        let infos: Vec<_> = tx_specs
            .iter()
            .map(|s| make_wallet_tx_info(s))
            .map(|(out_p, pk, sk, amount)| ((out_p, amount), (pk, sk, construct_address(pk))))
            .collect();

        let addr_to_keys = infos
            .iter()
            .map(|(_, (pk, sk, address))| (*pk, sk.clone(), address.clone()))
            .map(|(pk, sk, address)| (address.clone(), TransactionsKeys { sk, pk, address }))
            .collect();

        let mut ready: BTreeMap<_, Vec<_>> = Default::default();
        for (ready_val, (_, _, address)) in infos {
            ready.entry(address).or_default().push(ready_val);
        }

        Self {
            addr_to_keys,
            up_to_date_with_snapshot: false,
            pending: Default::default(),
            ready,
        }
    }

    pub fn is_up_to_date_with_snapshot(&self) -> bool {
        self.up_to_date_with_snapshot
    }

    pub fn snapshot_state(&mut self) -> Vec<u8> {
        self.up_to_date_with_snapshot = true;
        serialize(&(&self.pending, &self.ready)).unwrap()
    }

    pub fn apply_snapshot_state(&mut self, snapshot: &[u8]) {
        let (pending, ready) = deserialize(snapshot).unwrap();
        self.pending = pending;
        self.ready = ready;
        self.up_to_date_with_snapshot = true;
    }

    /// Commit transaction that are ready to be spent
    pub fn commit_transactions(&mut self, hashes: &[String]) {
        let txs: Vec<_> = hashes
            .iter()
            .filter_map(|c| self.pending.remove_entry(c))
            .collect();

        for (addr, read_txs) in &mut self.ready {
            read_txs.extend(
                get_tx_out_with_out_point(txs.iter().map(|(o, t)| (o, t)))
                    .filter(|(_, tx_out)| Some(addr) == tx_out.script_public_key.as_ref())
                    .map(|(out_p, tx_out)| (out_p, tx_out.amount)),
            );
        }
    }

    /// Make all transaction going from one address to the next
    pub fn make_all_transactions(
        &mut self,
        chunk_size: Option<usize>,
        total_tx: usize,
    ) -> Vec<(String, Transaction)> {
        let from_addr: Vec<_> = self.addr_to_keys.keys().cloned().collect();
        let to_addr = from_addr.iter().cycle().skip(1).cloned();
        let mut all_txs = Vec::new();

        for (to, from) in to_addr.zip(&from_addr) {
            let input_len = if all_txs.len() >= total_tx {
                break;
            } else {
                chunk_size.unwrap_or(total_tx - all_txs.len())
            };

            while let Some(tx) = self.make_transaction(&from, &to, input_len) {
                all_txs.push(tx);
                if all_txs.len() >= total_tx {
                    break;
                }
            }
        }
        all_txs
    }

    /// Make a single transaction
    pub fn make_transaction(
        &mut self,
        from: &str,
        to: &str,
        input_len: usize,
    ) -> Option<(String, Transaction)> {
        if let Some(inputs) = self.ready.get_mut(from) {
            let input_len = std::cmp::min(inputs.len(), input_len);
            let inputs: Vec<_> = if input_len == 0 {
                return None;
            } else {
                inputs.drain(..input_len).collect()
            };

            let ouputs = inputs.iter().map(|(_, amount)| amount.0).sum();
            let tx_ins: Vec<(i32, &str)> = inputs
                .iter()
                .map(|(out_p, _)| (out_p.n, out_p.t_hash.as_str()))
                .collect();
            let receiver_addr_hexs: Vec<_> = (0..ouputs).map(|_| to).collect();

            let keys = self.addr_to_keys.get(from).unwrap();
            let (hash, tx) = create_valid_transaction_with_ins_outs(
                &tx_ins,
                &receiver_addr_hexs,
                &keys.pk,
                &keys.sk,
                TokenAmount(1),
            );
            self.pending.insert(hash.clone(), tx.clone());

            Some((hash, tx))
        } else {
            None
        }
    }
}
