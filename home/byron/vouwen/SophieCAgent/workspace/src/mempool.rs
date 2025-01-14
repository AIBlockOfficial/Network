use crate::wallet::WalletDb;
use crate::config::Config;
use crate::context::Context;
use std::sync::Arc;

pub struct MempoolNode {
    wallet_db: WalletDb,
    // other fields
}

impl MempoolNode {
    pub fn new(config: Arc<Config>, context: Arc<Context>) -> Self {
        let wallet_db = WalletDb::new(config.wallet_db_options())
            .expect("Failed to create WalletDb");

        MempoolNode {
            wallet_db,
            // initialize other fields
        }
    }

    // other methods
}