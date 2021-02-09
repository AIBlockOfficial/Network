//! App to run a storage node.

use clap::{App, Arg};
use futures::future::join_all;
use std::time::Duration;
use system::configurations::StorageNodeConfig;
use system::{loop_connnect_to_peers_async, loop_wait_connnect_to_peers_async};
use system::{Response, StorageNode};
use tracing::error;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let matches = App::new("Zenotta Storage Node")
        .about("Runs a basic storage node.")
        .arg(
            Arg::with_name("config")
                .long("config")
                .short("c")
                .help("Run the storage node using the given config file.")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("index")
                .short("i")
                .long("index")
                .help("Run the specified storage node index from config file")
                .takes_value(true),
        )
        .get_matches();

    let config = {
        let mut settings = config::Config::default();
        let setting_file = matches
            .value_of("config")
            .unwrap_or("src/bin/node_settings.toml");

        settings.set_default("storage_node_idx", 0).unwrap();
        settings.set_default("storage_raft", 0).unwrap();
        settings
            .set_default("storage_raft_tick_timeout", 10)
            .unwrap();
        settings.set_default("storage_block_timeout", 1000).unwrap();
        settings
            .merge(config::File::with_name(setting_file))
            .unwrap();
        if let Some(index) = matches.value_of("index") {
            settings.set("storage_node_idx", index).unwrap();
            let mut db_mode = settings.get_table("storage_db_mode").unwrap();
            if let Some(test_idx) = db_mode.get_mut("Test") {
                *test_idx = config::Value::new(None, index);
                settings.set("storage_db_mode", db_mode).unwrap();
            }
        }

        let config: StorageNodeConfig = settings.try_into().unwrap();
        config
    };
    println!("Start node with config {:?}", config);
    let node = StorageNode::new(config).await.unwrap();

    println!("Started node at {}", node.address());

    let (node_conn, addrs_to_connect, expected_connected_addrs) = {
        let (node_conn, addrs_to_connect, all_addr_raft) = node.connect_to_raft_peers();
        let expected_connected_addrs = all_addr_raft;
        (node_conn, addrs_to_connect, expected_connected_addrs)
    };

    // PERMANENT CONNEXION HANDLING
    let (conn_loop_handle, stop_re_connect_tx) = {
        let (stop_re_connect_tx, stop_re_connect_rx) = tokio::sync::oneshot::channel::<()>();
        let node_conn = node_conn.clone();
        (
            tokio::spawn(async move {
                println!("Start connect to storage peers");
                loop_connnect_to_peers_async(node_conn, addrs_to_connect, Some(stop_re_connect_rx))
                    .await;
                println!("Reconnect complete");
            }),
            stop_re_connect_tx,
        )
    };

    // TEST DIS-CONNECTION HANDLING
    let (disconn_loop_handle, stop_disconnect_tx) = {
        let (stop_re_connect_tx, mut stop_re_connect_rx) = tokio::sync::oneshot::channel::<()>();
        let disconnect = format!("disconnect_{}", node.address().port());
        let mut node_conn = node_conn.clone();
        (
            tokio::spawn(async move {
                println!("Start mode input check");
                loop {
                    tokio::select! {
                        _ = tokio::time::delay_for(Duration::from_millis(500)) => {
                            if std::path::Path::new(&disconnect).exists() {
                                join_all(node_conn.disconnect_all().await).await;
                            }
                        },
                        _ = &mut stop_re_connect_rx => break,
                    };
                }
                println!("Complete mode input check");
            }),
            stop_re_connect_tx,
        )
    };

    // Need to connect first so Raft messages can be sent.
    loop_wait_connnect_to_peers_async(node_conn, expected_connected_addrs).await;

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
                println!("Response: {:?}", response);

                match response {
                    Ok(Response {
                        success: true,
                        reason: "Block received to be added",
                    }) => {}
                    Ok(Response {
                        success: true,
                        reason: "Block complete stored",
                    }) => {
                        println!("Block stored: Send to compute");
                        if let Err(e) = node.send_stored_block().await {
                            error!("Block stored not sent {:?}", e);
                        }
                    }
                    Ok(Response {
                        success: true,
                        reason: &_,
                    }) => {
                        println!("UNHANDLED RESPONSE TYPE: {:?}", response.unwrap().reason);
                    }
                    Ok(Response {
                        success: false,
                        reason: &_,
                    }) => {
                        println!("WARNING: UNHANDLED RESPONSE TYPE FAILURE");
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
