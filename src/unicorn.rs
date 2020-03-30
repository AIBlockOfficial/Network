use crate::interfaces::ProofOfWork;
use sha3::{Digest, Sha3_256};

/// A structure for the block header UnicornShard
#[derive(Debug, Clone)]
pub struct UnicornShard {
    pub promise: Vec<u8>,
    pub commit: ProofOfWork,
}

impl UnicornShard {
    /// Generate a new UnicornShard instance
    pub fn new() -> UnicornShard {
        let static_add: &'static str = "";
        let pow = ProofOfWork {
            address: static_add,
            nonce: Vec::new(),
        };

        UnicornShard {
            promise: Vec::new(),
            commit: pow,
        }
    }

    /// Checks for UnicornShard validity
    pub fn is_valid(&mut self, commit: ProofOfWork) -> bool {
        let mut commit_body = commit.address.as_bytes().to_vec();
        commit_body.append(&mut commit.nonce.clone());

        let result = Sha3_256::digest(&commit_body).to_vec();

        if result == self.promise {
            self.commit = commit;
            return true;
        }

        false
    }
}
