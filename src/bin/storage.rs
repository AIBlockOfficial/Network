//! App to run a storage node.

use clap::{App, Arg};
use std::net::SocketAddr;
use system::configurations::StorageNodeConfig;
use system::StorageNode;
use system::{
    loop_wait_connnect_to_peers_async, loops_re_connect_disconnect, routes, shutdown_connections,
    ResponseResult,
};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let matches = clap_app().get_matches();
    let config = configuration(load_settings(&matches));

    println!("Start node with config {:?}", config);
    let node = StorageNode::new(config, Default::default()).await.unwrap();

    println!("Started node at {}", node.address());

    let (node_conn, addrs_to_connect, expected_connected_addrs) = node.connect_info_peers();
    let api_addr = node.api_addr();

    let local_event_tx = node.local_event_tx().clone();

    // PERMANENT CONNEXION/DISCONNECTION HANDLING
    let ((conn_loop_handle, stop_re_connect_tx), (disconn_loop_handle, stop_disconnect_tx)) = {
        let (re_connect, disconnect_test) =
            loops_re_connect_disconnect(node_conn.clone(), addrs_to_connect, local_event_tx);

        (
            (tokio::spawn(re_connect.0), re_connect.1),
            (tokio::spawn(disconnect_test.0), disconnect_test.1),
        )
    };

    // Need to connect first so Raft messages can be sent.
    loop_wait_connnect_to_peers_async(node_conn.clone(), expected_connected_addrs).await;

    // RAFT HANDLING
    let raft_loop_handle = {
        let raft_loop = node.raft_loop();
        tokio::spawn(async move {
            println!("Peer connect complete, start Raft");
            raft_loop.await;
            println!("Raft complete");
        })
    };

    // Warp API
    let warp_handle = tokio::spawn({
        println!("Warp API starting at port {:?}", api_addr.port());
        println!();

        let db = node.db();
        let mut bind_address = "0.0.0.0:0".parse::<SocketAddr>().unwrap();
        bind_address.set_port(api_addr.port());

        async move {
            warp::serve(routes::block_info_by_nums(db))
                .run(bind_address)
                .await;
        }
    });

    // REQUEST HANDLING
    let main_loop_handle = tokio::spawn({
        let mut node = node;
        let mut node_conn = node_conn;

        async move {
            node.send_startup_requests().await.unwrap();

            let mut exit = std::future::pending();
            while let Some(response) = node.handle_next_event(&mut exit).await {
                if node.handle_next_event_response(response).await == ResponseResult::Exit {
                    break;
                }
            }
            stop_re_connect_tx.send(()).unwrap();
            stop_disconnect_tx.send(()).unwrap();

            node.close_raft_loop().await;
            shutdown_connections(&mut node_conn).await;
        }
    });

    let (main, warp_result, raft, conn, disconn) = tokio::join!(
        main_loop_handle,
        warp_handle,
        raft_loop_handle,
        conn_loop_handle,
        disconn_loop_handle
    );

    main.unwrap();
    warp_result.unwrap();
    raft.unwrap();
    conn.unwrap();
    disconn.unwrap();
}

fn clap_app<'a, 'b>() -> App<'a, 'b> {
    App::new("Zenotta Storage Node")
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
}

fn load_settings(matches: &clap::ArgMatches) -> config::Config {
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

    settings
}

fn configuration(settings: config::Config) -> StorageNodeConfig {
    settings.try_into().unwrap()
}

#[cfg(test)]
mod test {
    use super::*;
    use system::configurations::DbMode;

    #[test]
    fn validate_startup_no_args() {
        let args = vec!["bin_name"];
        let expected = DbMode::Test(0);

        validate_startup_common(args, expected);
    }

    #[test]
    fn validate_startup_raft_1() {
        let args = vec![
            "bin_name",
            "--config=src/bin/node_settings_local_raft_1.toml",
        ];
        let expected = DbMode::Test(0);

        validate_startup_common(args, expected);
    }

    #[test]
    fn validate_startup_raft_2_index_1() {
        let args = vec![
            "bin_name",
            "--config=src/bin/node_settings_local_raft_2.toml",
            "--index=1",
        ];
        let expected = DbMode::Test(1);

        validate_startup_common(args, expected);
    }

    #[test]
    fn validate_startup_raft_3() {
        let args = vec![
            "bin_name",
            "--config=src/bin/node_settings_local_raft_1.toml",
        ];
        let expected = DbMode::Test(0);

        validate_startup_common(args, expected);
    }

    fn validate_startup_common(args: Vec<&str>, expected: DbMode) {
        //
        // Act
        //
        let app = clap_app();
        let matches = app.get_matches_from_safe(args.into_iter()).unwrap();
        let settings = load_settings(&matches);
        let config = configuration(settings);

        //
        // Assert
        //
        assert_eq!(config.storage_db_mode, expected);
    }
}
