//! App to run a mining node.

use clap::{App, Arg};
use std::time::SystemTime;
use system::configurations::MinerNodeConfig;
use system::MinerNode;
use system::{loop_wait_connnect_to_peers_async, loops_re_connect_disconnect};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let matches = App::new("Zenotta Mining Node")
        .about("Runs a basic miner node.")
        .arg(
            Arg::with_name("config")
                .long("config")
                .short("c")
                .help("Run the miner node using the given config file.")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("index")
                .short("i")
                .long("index")
                .help("Run the specified miner node index from config file")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("compute_index")
                .long("compute_index")
                .help("Endpoint index of a compute node that the miner should connect to")
                .takes_value(true),
        )
        .get_matches();

    let config = {
        let mut settings = config::Config::default();
        let setting_file = matches
            .value_of("config")
            .unwrap_or("src/bin/node_settings.toml");

        settings.set_default("miner_node_idx", 0).unwrap();
        settings.set_default("miner_compute_node_idx", 0).unwrap();
        settings
            .merge(config::File::with_name(setting_file))
            .unwrap();
        if let Some(index) = matches.value_of("index") {
            settings.set("miner_node_idx", index).unwrap();
            let mut db_mode = settings.get_table("miner_db_mode").unwrap();
            if let Some(test_idx) = db_mode.get_mut("Test") {
                *test_idx = config::Value::new(None, index);
                settings.set("miner_db_mode", db_mode).unwrap();
            }
        }
        if let Some(index) = matches.value_of("compute_index") {
            settings.set("miner_compute_node_idx", index).unwrap();
        }

        let config: MinerNodeConfig = settings.try_into().unwrap();
        config
    };
    println!("Start node with config {:?}", config);

    let mut node = MinerNode::new(config).await.unwrap();
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

    // Send any requests to the compute node here

    // Send partition request
    println!("MINER ADDRESS: {:?}", node.address());
    let _result = node
        .send_partition_request(node.compute_address())
        .await
        .unwrap();

    let now = SystemTime::now();
    let main_loop_handle = tokio::spawn({
        let mut node = node;

        async move {
            while let Some(response) = node.handle_next_event().await {
                node.handle_next_event_response(now, response).await;
            }
            stop_re_connect_tx.send(()).unwrap();
            stop_disconnect_tx.send(()).unwrap();
        }
    });

    let (result, conn, disconn) =
        tokio::join!(main_loop_handle, conn_loop_handle, disconn_loop_handle);
    result.unwrap();
    conn.unwrap();
    disconn.unwrap();
}
