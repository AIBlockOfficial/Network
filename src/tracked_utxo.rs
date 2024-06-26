use crate::interfaces::{AddressesWithOutPoints, OutPointData, UtxoSet};
use crate::utils::get_pk_with_out_point_from_utxo_set_cloned;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Deref;
use tracing::debug;
use tw_chain::primitives::asset::AssetValues;
use tw_chain::primitives::transaction::{OutPoint, Transaction};
use tw_chain::utils::transaction_utils::{
    get_fees_with_out_point_cloned, get_tx_out_with_out_point_cloned,
};

#[derive(Default, Debug, Clone, Serialize)]
pub struct TrackedUtxoBalance {
    total: AssetValues,
    address_list: AddressesWithOutPoints,
}

impl TrackedUtxoBalance {
    pub fn get_asset_values(&self) -> &AssetValues {
        &self.total
    }
}
/// Invariant: `pk_cache` contains exactly all relevant mapping for `base`
#[derive(Default, Clone, Debug)]
pub struct TrackedUtxoSet {
    /// The `UtxoSet`
    base: UtxoSet,
    /// Cache mapping of Some `script_public_key` to `OutPoints` present in `base`.
    pk_cache: HashMap<String, BTreeSet<OutPoint>>,
}

impl TrackedUtxoSet {
    /// Get a clone of `pk_cache`
    ///
    /// ## NOTE
    ///
    /// Only used during testing
    #[cfg(test)]
    pub fn get_pk_cache(&self) -> HashMap<String, BTreeSet<OutPoint>> {
        self.pk_cache.clone()
    }

    /// Remove a `pk_cache` entry
    ///
    /// # Arguments
    ///
    /// * `entry` - `script_public_key` to remove
    ///
    /// ## NOTE
    ///
    /// Only used during testing
    #[cfg(test)]
    pub fn remove_pk_cache_entry(&mut self, entry: &str) {
        self.pk_cache.remove(entry);
    }

    /// Re-align `pk_cache` to `base`
    pub fn re_align(&mut self) {
        self.pk_cache = create_pk_cache_from_base(&self.base);
    }

    /// Get base `UtxoSet` length
    pub fn get_base_outpoint_count(&self) -> u64 {
        self.base.len() as u64
    }

    /// Get tracked `UtxoSet` set length
    pub fn get_tracked_outpoint_count(&self) -> u64 {
        self.pk_cache.values().map(|v| v.len() as u64).sum()
    }

    /// Create a new TrackedUtxoSet from `UtxoSet` base
    pub fn new(base: UtxoSet) -> Self {
        let pk_cache = create_pk_cache_from_base(&base);
        TrackedUtxoSet { base, pk_cache }
    }

    // Take ownership of self; return base `UtxoSet`
    pub fn into_utxoset(self) -> UtxoSet {
        self.base
    }

    /// Get all `OutPoints` for a `script_public_key`
    pub fn get_pk_cache_vec(&self, key: &str) -> Option<&BTreeSet<OutPoint>> {
        self.pk_cache.get(key)
    }

    /// Add base 'UtxoSet' and pk_cache entry concurrently
    pub fn extend_tracked_utxo_set(&mut self, block_tx: &BTreeMap<String, Transaction>) {
        self.base
            .extend(get_tx_out_with_out_point_cloned(block_tx.iter()));

        // Add fees to base
        self.base
            .extend(get_fees_with_out_point_cloned(block_tx.iter()));

        // Re-align `pk_cache` to `base`
        self.re_align();
    }

    /// Remove base 'UtxoSet' and pk_cache entry concurrently
    pub fn remove_tracked_utxo_entry<'a>(&mut self, key: &'a OutPoint) -> Option<&'a OutPoint> {
        self.base.remove(key)?.script_public_key.and_then(|spk| {
            let pk_cache_entry = self.pk_cache.get_mut(&spk)?;
            pk_cache_entry.retain(|op| op.t_hash != key.t_hash && op.n != key.n);
            if pk_cache_entry.is_empty() {
                self.pk_cache.remove(&spk);
            }
            Some(key)
        })
    }

    /// Calculates the balance of `OutPoint`s based on provided addresses
    pub fn get_balance_for_addresses(&self, addresses: &[String]) -> TrackedUtxoBalance {
        let mut address_list = AddressesWithOutPoints::new();
        let mut total = AssetValues::default();
        let mut known_op: BTreeSet<OutPoint> = Default::default();

        for address in addresses {
            if let Some(ops) = self.get_pk_cache_vec(address) {
                for op in ops {
                    debug!("OP: {:?}", op);
                    // Ignore `OutPoint` values already present
                    if known_op.get(op).is_some() {
                        continue;
                    }

                    known_op.insert(op.clone());
                    let t_out = self.base.get(op).unwrap();
                    let asset = t_out.value.clone().with_fixed_hash(op);
                    address_list
                        .entry(address.clone())
                        .or_default()
                        .push(OutPointData::new(op.clone(), asset.clone()));
                    total.update_add(&asset);
                }
            }
        }

        TrackedUtxoBalance {
            total,
            address_list,
        }
    }

    /// Get all `script_public_key` values from the current UTXO set
    pub fn get_all_addresses(&self) -> Vec<String> {
        self.base
            .iter()
            .filter_map(|(_, tx_out)| tx_out.script_public_key.clone())
            .collect::<Vec<String>>()
    }
}

/// Create `pk_cache` entries from base `UtxoSet`
pub fn create_pk_cache_from_base(base: &UtxoSet) -> HashMap<String, BTreeSet<OutPoint>> {
    let mut pk_cache: HashMap<String, BTreeSet<OutPoint>> = HashMap::new();
    extend_pk_cache_vec(
        &mut pk_cache,
        get_pk_with_out_point_from_utxo_set_cloned(base.iter()),
    );
    pk_cache
}

/// Extend `pk_cache` entries
pub fn extend_pk_cache_vec<'a>(
    pk_cache: &mut HashMap<String, BTreeSet<OutPoint>>,
    spk: impl Iterator<Item = (String, OutPoint)> + 'a,
) {
    spk.for_each(|(spk, op)| {
        pk_cache.entry(spk).or_default().insert(op);
    });
}

impl Deref for TrackedUtxoSet {
    type Target = UtxoSet;

    fn deref(&self) -> &UtxoSet {
        &self.base
    }
}

impl Serialize for TrackedUtxoSet {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        self.base.serialize(s)
    }
}

impl<'a> Deserialize<'a> for TrackedUtxoSet {
    fn deserialize<D: Deserializer<'a>>(deserializer: D) -> Result<Self, D::Error> {
        let base: UtxoSet = Deserialize::deserialize(deserializer)?;
        let pk_cache: HashMap<String, BTreeSet<OutPoint>> = create_pk_cache_from_base(&base);
        Ok(TrackedUtxoSet { base, pk_cache })
    }
}
