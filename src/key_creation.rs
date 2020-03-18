use crate::sha3::Digest;
use hex::encode;
use rug::integer::Order;
use rug::ops::Pow;
use rug::Integer;
use sha3::Sha3_256;
use sodiumoxide::crypto::sign;
use sodiumoxide::crypto::sign::{PublicKey, SecretKey};

/// The data structure for partitioned key agreement
#[derive(Debug, Clone)]
pub struct KeyAgreement {
    g: u64,
    u_i: u32,
    x_i: u32,
    k_i: Vec<u8>,
    y_i: Vec<u8>,
    M_iI: Vec<u8>,
    M_iIU: Vec<u8>,
    sigma_iI: Vec<u8>,
    t_iL: Vec<u8>,
    t_iR: Vec<u8>,
    T_i: Vec<u8>,
    s_key: SecretKey,
    pub p_key: PublicKey,
    broadcast_1: Vec<u8>,
}

impl KeyAgreement {
    /// Generates a new KeyAgreement instance
    ///
    /// ### Arguments
    ///
    /// * `g` - Generator value
    pub fn new(g: u64, x_i: u32, u_i: u32) -> KeyAgreement {
        let (pk, sk) = sign::gen_keypair();

        KeyAgreement {
            g: g,
            x_i: x_i,
            u_i: u_i,
            s_key: sk,
            p_key: pk,
            k_i: Vec::new(),
            y_i: Vec::new(),
            M_iI: Vec::new(),
            M_iIU: Vec::new(),
            t_iL: Vec::new(),
            t_iR: Vec::new(),
            T_i: Vec::new(),
            sigma_iI: Vec::new(),
            broadcast_1: Vec::new(),
        }
    }

    /// Convenience method to run the first key agreement round of computation
    pub fn first_round(
        &mut self,
        address: &mut Vec<u8>,
        unicorn: &mut Vec<u8>,
        nonce: &mut Vec<u8>,
    ) {
        self.compute_k_i(address, unicorn, nonce);
        self.compute_y_i();
        self.compute_hashes();
    }

    /// Creates the K_i for the key creation protocol
    ///
    /// ### Arguments
    ///
    /// * `address` - Payment address
    /// * `unicorn` - Unicorn value
    /// * `nonce`   - Nonce value  
    fn compute_k_i(&mut self, address: &mut Vec<u8>, unicorn: &mut Vec<u8>, nonce: &mut Vec<u8>) {
        address.append(unicorn);
        address.append(nonce);

        self.k_i = address.to_vec();
    }

    /// Computes Y_i for the key creation protocol
    fn compute_y_i(&mut self) {
        self.y_i = self.g.pow(self.x_i).to_be_bytes().to_vec();
    }

    /// Computes M_iI, M_iIU, sigma_iI as the required hash values
    fn compute_hashes(&mut self) {
        let mut mi_handler = Sha3_256::digest(&self.k_i).to_vec();

        // Set M_i^I = H(k_i) || y_i
        mi_handler.append(&mut self.y_i.clone());
        self.M_iI = Sha3_256::digest(&mi_handler).to_vec();

        // Set M_i^U = M_i^I || U_i
        mi_handler.append(&mut self.u_i.to_be_bytes().to_vec());
        self.M_iIU = Sha3_256::digest(&mi_handler).to_vec();

        // Set sigma_i^I to the signed M_i^U
        self.sigma_iI = sign::sign(&self.M_iIU, &self.s_key);

        // Set broadcast_1 = M_i^I || sigma_i^I
        let mut M_iI_clone = self.M_iI.clone();
        M_iI_clone.append(&mut self.sigma_iI.clone());
        self.broadcast_1 = M_iI_clone;
    }

    /// Compute t_i^L = H(y_{i-1}^x_i) and t_i^R = H(y_{i+1}^x_i)
    ///
    /// ### Arguments
    ///
    /// * `peer_value_left`     - Y_i value from the peer to the "left"
    /// * `peer_value_right`    - Y_i value from the peer to the "right"
    fn compute_t_values(&mut self, peer_value_left: Vec<u8>, peer_value_right: Vec<u8>) {
        // LEFT SIDE
        let hexed_peer_left = encode(peer_value_left);
        let big_peer_left = Integer::from_str_radix(&hexed_peer_left, 16).unwrap();
        let val_l = big_peer_left.pow(self.x_i);

        // Complicated sorcery to get a big int into a byte form
        let l_as_digits = val_l.to_digits::<u8>(Order::MsfBe);

        self.t_iL = Sha3_256::digest(&l_as_digits).to_vec();

        // RIGHT SIDE
        let hexed_peer_right = encode(peer_value_right);
        let big_peer_right = Integer::from_str_radix(&hexed_peer_right, 16).unwrap();
        let val_r = big_peer_right.pow(self.x_i);
        let r_as_digits = val_r.to_digits::<u8>(Order::MsfBe);

        self.t_iR = Sha3_256::digest(&r_as_digits).to_vec();

        // Compute Ti
        self.T_i = self
            .t_iL
            .iter()
            .zip(self.t_iR.iter())
            .map(|(&x1, &x2)| x1 ^ x2)
            .collect();
    }

    /// Verifies a received value from a peer
    ///
    /// ### Arguments
    ///
    /// * `signed_data` - Signed data to verify
    /// * `pub_key`     - Public key for verification
    fn verify_data(&self, signed_data: Vec<u8>, pub_key: &PublicKey) -> bool {
        match sign::verify(&signed_data, pub_key) {
            Ok(_v) => true,
            Err(_e) => false,
        }
    }
}
