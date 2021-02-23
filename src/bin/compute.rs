//! App to run a compute node.

use clap::{App, Arg};
use system::configurations::ComputeNodeConfig;
use system::{
    get_sanction_addresses, loop_wait_connnect_to_peers_async, loops_re_connect_disconnect,
    SANC_LIST_PROD,
};
use system::{ComputeNode, Response};
use tracing::{debug, error, info, warn};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let matches = App::new("Zenotta Compute Node")
        .about("Runs a basic compute node.")
        .arg(
            Arg::with_name("config")
                .long("config")
                .short("c")
                .help("Run the compute node using the given config file.")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("initial_block_config")
                .long("initial_block_config")
                .help("Run the compute node using the given initial block config file.")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("index")
                .short("i")
                .long("index")
                .help("Run the specified compute node index from config file")
                .takes_value(true),
        )
        .get_matches();

    let mut config = {
        let mut settings = config::Config::default();
        let setting_file = matches
            .value_of("config")
            .unwrap_or("src/bin/node_settings.toml");
        let intial_block_setting_file = matches
            .value_of("initial_block_config")
            .unwrap_or("src/bin/initial_block.json");

        settings
            .set_default("sanction_list", Vec::<String>::new())
            .unwrap();
        settings.set_default("jurisdiction", "US").unwrap();
        settings.set_default("compute_node_idx", 0).unwrap();
        settings.set_default("compute_raft", 0).unwrap();
        settings
            .set_default("compute_raft_tick_timeout", 10)
            .unwrap();
        settings
            .set_default("compute_transaction_timeout", 100)
            .unwrap();

        settings
            .merge(config::File::with_name(setting_file))
            .unwrap();
        settings
            .merge(config::File::with_name(intial_block_setting_file))
            .unwrap();
        if let Some(index) = matches.value_of("index") {
            settings.set("compute_node_idx", index).unwrap();
            let mut db_mode = settings.get_table("compute_db_mode").unwrap();
            if let Some(test_idx) = db_mode.get_mut("Test") {
                *test_idx = config::Value::new(None, index);
                settings.set("compute_db_mode", db_mode).unwrap();
            }
        }

        let config: ComputeNodeConfig = settings.try_into().unwrap();
        config
    };
    println!("Start node with config {:?}", config);

    config.sanction_list = get_sanction_addresses(SANC_LIST_PROD.to_string(), &config.jurisdiction);
    let node = ComputeNode::new(config, Default::default()).await.unwrap();

    println!("Started node at {}", node.address());

    let (node_conn, addrs_to_connect, expected_connected_addrs) = node.connect_info_peers();

    // PERMANENT CONNEXION/DISCONNECTION HANDLING
    let ((conn_loop_handle, stop_re_connect_tx), (disconn_loop_handle, stop_disconnect_tx)) = {
        let (re_connect, disconnect_test) =
            loops_re_connect_disconnect(node_conn.clone(), addrs_to_connect);

        (
            (tokio::spawn(re_connect.0), re_connect.1),
            (tokio::spawn(disconnect_test.0), disconnect_test.1),
        )
    };

    // Need to connect first so Raft messages can be sent.
    loop_wait_connnect_to_peers_async(node_conn, expected_connected_addrs).await;
    println!("Raft and Storage connection complete");

    // RAFT HANDLING
    let raft_loop_handle = {
        let raft_loop = node.raft_loop();
        tokio::spawn(async move {
            println!("Peer connect complete, start Raft");
            raft_loop.await;
            println!("Raft complete");
        })
    };

    // REQUEST HANDLING
    let main_loop_handle = tokio::spawn({
        let mut node = node;

        async move {
            while let Some(response) = node.handle_next_event().await {
                debug!("Response: {:?}", response);

                match response {
                    Ok(Response {
                        success: true,
                        reason: "Received partition request successfully",
                    }) => {}
                    Ok(Response {
                        success: true,
                        reason: "Received first full partition request",
                    }) => {
                        node.propose_initial_uxto_set().await;
                    }
                    Ok(Response {
                        success: true,
                        reason: "Partition list is full",
                    }) => {
                        node.flood_list_to_partition().await.unwrap();
                        node.flood_block_to_partition().await.unwrap();
                        node.flood_block_to_users().await.unwrap();
                    }
                    Ok(Response {
                        success: true,
                        reason: "Received PoW successfully",
                    }) => {
                        info!("Send Block to storage");
                        debug!("CURRENT MINED BLOCK: {:?}", node.current_mined_block);
                        if let Err(e) = node.send_block_to_storage().await {
                            error!("Block not sent to storage {:?}", e);
                        }
                    }
                    Ok(Response {
                        success: true,
                        reason: "Transactions added to tx pool",
                    }) => {
                        debug!("Transactions received and processed successfully");
                    }
                    Ok(Response {
                        success: true,
                        reason: "First Block committed",
                    }) => {
                        debug!("First Block ready to mine: {:?}", node.get_mining_block());
                        node.flood_rand_num_to_requesters().await.unwrap();
                    }
                    Ok(Response {
                        success: true,
                        reason: "Block committed",
                    }) => {
                        debug!("Block ready to mine: {:?}", node.get_mining_block());
                        if let Err(e) = node.send_bf_notification().await {
                            error!("Could not send block found notification to winner {:?}", e);
                        }
                        node.flood_rand_num_to_requesters().await.unwrap();
                    }
                    Ok(Response {
                        success: true,
                        reason: "Transactions committed",
                    }) => {
                        debug!("Transactions ready to be used in next block");
                    }
                    Ok(Response {
                        success: true,
                        reason: "Received block stored",
                    }) => {
                        info!("Block info received from storage: ready to generate block");
                    }
                    Ok(Response {
                        success: true,
                        reason: "Snapshot applied",
                    }) => {
                        warn!("Snapshot applied");
                    }
                    Ok(Response {
                        success: true,
                        reason: "Received block notification",
                    }) => {}
                    Ok(Response {
                        success: true,
                        reason: "Partition PoW received successfully",
                    }) => {}
                    Ok(Response {
                        success: false,
                        reason: "Partition list is already full",
                    }) => {}
                    Ok(Response {
                        success: false,
                        reason: "No block to mine currently",
                    }) => {}
                    Ok(Response {
                        success: true,
                        reason,
                    }) => {
                        error!("UNHANDLED RESPONSE TYPE: {:?}", reason);
                    }
                    Ok(Response {
                        success: false,
                        reason,
                    }) => {
                        error!("WARNING: UNHANDLED RESPONSE TYPE FAILURE: {:?}", reason);
                    }
                    Err(error) => {
                        panic!("ERROR HANDLING RESPONSE: {:?}", error);
                    }
                }
            }
            node.close_raft_loop().await;
            stop_re_connect_tx.send(()).unwrap();
            stop_disconnect_tx.send(()).unwrap();
        }
    });

    let (main, raft, conn, disconn) = tokio::join!(
        main_loop_handle,
        raft_loop_handle,
        conn_loop_handle,
        disconn_loop_handle
    );
    main.unwrap();
    raft.unwrap();
    conn.unwrap();
    disconn.unwrap();
}
