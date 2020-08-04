use crate::interfaces::Asset;
use crate::primitives::transaction::*;
use crate::script::lang::Script;
use crate::user::AssetInTransit;

/// Constructs a transaction to pay a receiver
///
/// ### Arguments
///
/// * `tx_ins`              - Address/es to pay from
/// * `receiver_address`    - Address to send to
/// * `amount`              - Number of tokens to send
pub fn create_payment_tx(tx_ins: Vec<TxIn>, receiver_address: Vec<u8>, amount: u64) -> Transaction {
    let mut tx = Transaction::new();
    let mut tx_out = TxOut::new();

    tx_out.value = Some(Asset::Token(amount));
    tx_out.amount = amount;
    tx_out.script_public_key = Some(receiver_address);

    tx.outputs = vec![tx_out];
    tx.inputs = tx_ins;
    tx.version = 0;

    tx
}

/// Constructs a set of TxIns for a payment
///
/// ### Arguments
///
/// * `tx_values`   - Series of values required for TxIn construction
pub fn create_payment_tx_ins(tx_values: Vec<TxConstructor>) -> Vec<TxIn> {
    let mut tx_ins = Vec::new();

    for entry in tx_values {
        let mut new_tx_in = TxIn::new();
        new_tx_in.script_signature = Script::pay2pkh(
            entry.prev_hash.clone(),
            entry.signatures[0],
            entry.pub_keys[0],
        );
        new_tx_in.previous_out = Some(OutPoint::new(entry.b_num, entry.prev_hash, entry.prev_n));

        tx_ins.push(new_tx_in);
    }

    tx_ins
}

/// Constructs a dual double entry tx
///
/// ### Arguments
///
/// * `tx_ins`              - Addresses to pay from
/// * `address`             - Address to send the asset to
/// * `send_asset`          - Asset to be sent as payment
/// * `receive_asset`       - Asset to receive
/// * `druid`               - DRUID value to match with the other party
/// * `druid_participants`  - Number of DRUID values to match with
pub fn create_dde_tx(
    tx_ins: Vec<TxIn>,
    address: Vec<u8>,
    send_asset: AssetInTransit,
    receive_asset: AssetInTransit,
    druid: Vec<u8>,
    druid_participants: usize,
) -> Transaction {
    let mut tx = Transaction::new();
    let mut tx_out = TxOut::new();

    tx_out.value = Some(send_asset.asset);
    tx_out.amount = send_asset.amount;
    tx_out.script_public_key = Some(address);

    tx.outputs = vec![tx_out];
    tx.inputs = tx_ins;
    tx.version = 0;
    tx.druid = Some(druid);
    tx.druid_participants = Some(druid_participants);
    tx.expect_value = Some(receive_asset.asset);
    tx.expect_value_amount = Some(receive_asset.amount);

    tx
}

/*---- TESTS ----*/

#[cfg(test)]
mod tests {
    use super::*;
    use sodiumoxide::crypto::sign;

    #[test]
    // Creates a valid payment transaction
    fn should_construct_a_valid_payment_tx() {
        let (_pk, sk) = sign::gen_keypair();
        let (pk, _sk) = sign::gen_keypair();
        let prev_hash = vec![0, 0, 0];
        let signature = sign::sign_detached(&prev_hash.clone(), &sk);

        let tx_const = TxConstructor {
            prev_hash: prev_hash,
            prev_n: 0,
            b_num: 0,
            signatures: vec![signature],
            pub_keys: vec![pk],
        };

        let tx_ins = create_payment_tx_ins(vec![tx_const]);
        let payment_tx = create_payment_tx(tx_ins, vec![0, 0, 0, 0], 4);

        assert_eq!(
            Asset::Token(4),
            payment_tx.outputs[0].clone().value.unwrap()
        );
        assert_eq!(
            payment_tx.outputs[0].clone().script_public_key,
            Some(vec![0, 0, 0, 0])
        );
    }

    #[test]
    // Creates a valid DDE transaction
    fn should_construct_a_valid_dde_tx() {
        let (_pk, sk) = sign::gen_keypair();
        let (pk, _sk) = sign::gen_keypair();
        let prev_hash = vec![0, 0, 0];
        let signature = sign::sign_detached(&prev_hash.clone(), &sk);

        let tx_const = TxConstructor {
            prev_hash: prev_hash,
            prev_n: 0,
            b_num: 0,
            signatures: vec![signature],
            pub_keys: vec![pk],
        };

        let tx_ins = create_payment_tx_ins(vec![tx_const]);

        // DDE params
        let druid = vec![1, 2, 3, 4, 5];
        let druid_participants = 2;

        let first_asset = Asset::Token(10);
        let first_amount = 10;
        let second_asset = Asset::Token(0);
        let second_amount = 0;

        let first_asset_t = AssetInTransit {
            asset: first_asset,
            amount: first_amount,
        };
        let second_asset_t = AssetInTransit {
            asset: second_asset,
            amount: second_amount,
        };

        // Actual DDE
        let dde = create_dde_tx(
            tx_ins,
            vec![0, 0, 0, 0],
            first_asset_t.clone(),
            second_asset_t,
            druid.clone(),
            druid_participants.clone(),
        );

        assert_eq!(dde.druid, Some(druid.clone()));
        assert_eq!(
            dde.outputs[0].clone().value,
            Some(first_asset_t.clone().asset)
        );
        assert_eq!(dde.outputs[0].clone().amount, first_asset_t.clone().amount);
        assert_eq!(dde.druid_participants, Some(druid_participants.clone()));
    }
}
