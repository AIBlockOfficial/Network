//! Implementation of a UNiCORN process, as per Lenstra and Wesolowski's "Random Zoo"
//! paper (https://eprint.iacr.org/2015/366.pdf). The eval-verify processes use modular
//! square root (sloth) with swapped neighbours, but this particular implementation may
//! need to be optimised in future given certain seed or modulus sizes.

use crate::constants::MR_PRIME_ITERS;
use bincode::serialize;
use rug::integer::IsPrime;
use rug::ops::DivRounding;
use rug::Integer;
use sha3::{Digest, Sha3_256};
use tracing::error;

#[derive(Default, Debug, Clone)]
pub struct Unicorn {
    pub modulus: Integer,
    pub iterations: u64,
    pub seed: Integer,
    pub witness: Integer,
    pub security_level: u32,
}

impl Unicorn {
    /// Sets the seed for the UNiCORN. Returns the commitment value `c`
    ///
    /// ### Arguments
    ///
    /// * `seed`    - Seed to set
    pub fn set_seed(&mut self, seed: Integer) -> String {
        let u = hex::encode(Sha3_256::digest(&serialize(&seed.to_u64()).unwrap()));
        let c = hex::encode(Sha3_256::digest(&u.as_bytes()));

        self.seed = seed;

        c
    }

    /// Evaluation of the Sloth VDF given internal params (swapped neighbours). Returns the
    /// raw witness value and hash `g`
    ///
    /// ### Arguments
    ///
    /// * `seed`        - Aggregated seed value
    /// * `iterations`  - Number of iterations to slow the function by
    pub fn eval(&mut self) -> Option<(Integer, String)> {
        if !self.is_valid_modulus() {
            error!("Modulus for UNiCORN eval invalid");
            return None;
        }

        let mut x = self.seed.clone().div_rem_floor(self.modulus.clone()).1;
        let exponent = (self.modulus.clone() + 1) / 4;

        for _ in 0..self.iterations {
            x ^= 1;

            while x >= self.modulus || x == 0 {
                x ^= 1;
            }

            x.pow_mod_mut(&exponent, &self.modulus).unwrap();
        }

        self.witness = x.clone();
        let g = hex::encode(Sha3_256::digest(&serialize(&x.to_u64()).unwrap()));

        Some((x, g))
    }

    /// Verifies a particular unicorn given a witness value
    ///
    /// ### Arguments
    ///
    /// * `seed`    - Seed to verify
    /// * `witness` - Witness value for trapdoor verification
    pub fn verify(&self, seed: Integer, witness: Integer) -> bool {
        let square: Integer = 2u64.into();
        let mut result = witness;

        for _ in 0..self.iterations {
            result.pow_mod_mut(&square, &self.modulus).unwrap();

            let inv_result = -result;
            result = inv_result.div_rem_floor(self.modulus.clone()).1;
            result ^= 1;

            while result >= self.modulus || result == 0 {
                result ^= 1;
            }
        }
        result == seed.div_rem_floor(self.modulus.clone()).1
    }

    /// Gets the calculated UNiCORN value, with an optional modulus division
    ///
    /// ### Arguments
    ///
    /// * `modulus` - Modulus to divide the UNiCORN by. Optional
    pub fn get_unicorn(&self, modulus: Option<Integer>) -> Integer {
        match modulus {
            Some(p) => self.witness.clone().div_floor(p),
            None => self.witness.clone(),
        }
    }

    /// Predicate for a valid modulus
    fn is_valid_modulus(&self) -> bool {
        self.modulus >= 2u64.pow(2 * self.security_level)
            && !matches!(self.modulus.is_probably_prime(MR_PRIME_ITERS), IsPrime::No)
    }
}
