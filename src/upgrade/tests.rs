use std::collections::BTreeMap;
use std::future::Future;
use std::time::Duration;
use tracing::info;

type ExtraNodeParamsFilterMap = BTreeMap<String, ExtraNodeParamsFilter>;

const TIMEOUT_TEST_WAIT_DURATION: Duration = Duration::from_millis(5000);
const KEEP_ALL_FILTER: ExtraNodeParamsFilter = ExtraNodeParamsFilter {
    db: true,
    raft_db: true,
    wallet_db: true,
};

#[derive(Clone, Copy)]
pub struct ExtraNodeParamsFilter {
    pub db: bool,
    pub raft_db: bool,
    pub wallet_db: bool,
}

fn test_step_start() {
    let _ = tracing_log_try_init();
    info!("Test Step start");
}

async fn test_step_complete(network: Network) {
    network.close_raft_loops_and_drop().await;
    info!("Test Step complete")
}

fn test_timeout() -> impl Future<Output = &'static str> + Unpin {
    Box::pin(async move {
        tokio::time::sleep(TIMEOUT_TEST_WAIT_DURATION).await;
        "Test timeout elapsed"
    })
}

async fn raft_node_handle_event(network: &mut Network, node: &str, reason_val: &str) {
    if let Some(n) = network.mempool(node) {
        let mut n = n.lock().await;
        match n.handle_next_event(&mut test_timeout()).await {
            Some(Ok(Response { success, reason })) if success && reason == reason_val => {}
            other => panic!("Unexpected result: {:?} (expected:{})", other, reason_val),
        }
    } else if let Some(n) = network.storage(node) {
        let mut n = n.lock().await;
        match n.handle_next_event(&mut test_timeout()).await {
            Some(Ok(Response { success, reason })) if success && reason == reason_val => {}
            other => panic!("Unexpected result: {:?} (expected:{})", other, reason_val),
        }
    }
}

async fn node_send_coordinated_shutdown(network: &mut Network, node: &str, at_block: u64) {
    use crate::utils::LocalEvent;
    let mut event_tx = network.get_local_event_tx(node).await.unwrap();
    let event = LocalEvent::CoordinatedShutdown(at_block);
    event_tx.send(event, "test shutdown").await.unwrap();
}

fn test_hash(t: BlockchainItem) -> (u32, BlockchainItemMeta, u64, u64) {
    use std::hash::{Hash, Hasher};
    let data_hash = {
        let mut s = std::collections::hash_map::DefaultHasher::new();
        t.data.hash(&mut s);
        s.finish()
    };
    let json_hash = {
        let mut s = std::collections::hash_map::DefaultHasher::new();
        t.data_json.hash(&mut s);
        s.finish()
    };

    (t.version, t.item_meta, data_hash, json_hash)
}

fn index_meta(v: &str) -> BlockchainItemMeta {
    let mut it = v.split('_');
    match (it.next(), it.next(), it.next()) {
        (Some("nIndexedBlockHashKey"), Some(block_num), None) => {
            let block_num = u64::from_str_radix(block_num, 16).unwrap();
            BlockchainItemMeta::Block {
                block_num,
                tx_len: STORAGE_DB_V0_6_0_BLOCK_LEN[block_num as usize],
            }
        }
        (Some("nIndexedTxHashKey"), Some(block_num), Some(tx_num)) => BlockchainItemMeta::Tx {
            block_num: u64::from_str_radix(block_num, 16).unwrap(),
            tx_num: u32::from_str_radix(tx_num, 16).unwrap(),
        },
        _ => panic!("index_meta not found {}", v),
    }
}