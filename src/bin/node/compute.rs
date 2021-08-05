//! App to run a compute node.

use clap::{App, Arg, ArgMatches};
use system::configurations::ComputeNodeConfig;
use system::ComputeNode;
use system::{
    get_sanction_addresses, loop_wait_connnect_to_peers_async, loops_re_connect_disconnect,
    shutdown_connections, ResponseResult, SANC_LIST_PROD,
};

pub async fn run_node(matches: &ArgMatches<'_>) {
    let mut config = configuration(load_settings(matches));

    println!("Start node with config {:?}", config);

    config.sanction_list = get_sanction_addresses(SANC_LIST_PROD.to_string(), &config.jurisdiction);
    let node = ComputeNode::new(config, Default::default()).await.unwrap();

    println!("Started node at {}", node.address());

    let (node_conn, addrs_to_connect, expected_connected_addrs) = node.connect_info_peers();
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

pub fn clap_app<'a, 'b>() -> App<'a, 'b> {
    App::new("compute")
        .about("Runs a basic compute node.")
        .arg(
            Arg::with_name("config")
                .long("config")
                .short("c")
                .help("Run the compute node using the given config file.")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("tls_config")
                .long("tls_config")
                .help("Use file to provide tls configuration options.")
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
        .arg(
            Arg::with_name("tls_private_key_override")
                .long("tls_private_key_override")
                .env("ZENOTA_TLS_PRIVATE_KEY")
                .help("Use PKCS8 private key as a string to use for this node TLS certificate.")
                .takes_value(true),
        )
}

fn load_settings(matches: &clap::ArgMatches) -> config::Config {
    let mut settings = config::Config::default();
    let setting_file = matches
        .value_of("config")
        .unwrap_or("src/bin/node_settings.toml");
    let intial_block_setting_file = matches
        .value_of("initial_block_config")
        .unwrap_or("src/bin/initial_block.json");
    let tls_setting_file = matches
        .value_of("tls_config")
        .unwrap_or("src/bin/tls_certificates.json");

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
    settings
        .merge(config::File::with_name(tls_setting_file))
        .unwrap();

    if let Some(index) = matches.value_of("index") {
        settings.set("compute_node_idx", index).unwrap();
        let mut db_mode = settings.get_table("compute_db_mode").unwrap();
        if let Some(test_idx) = db_mode.get_mut("Test") {
            *test_idx = config::Value::new(None, index);
            settings.set("compute_db_mode", db_mode).unwrap();
        }
    }

    if let Some(key) = matches.value_of("tls_private_key_override") {
        let mut tls_config = settings.get_table("tls_config").unwrap();
        tls_config.insert(
            "pem_pkcs8_private_key_override".to_owned(),
            config::Value::new(None, key),
        );
        settings.set("tls_config", tls_config).unwrap();
    }

    settings
}

fn configuration(settings: config::Config) -> ComputeNodeConfig {
    settings.try_into().unwrap()
}

#[cfg(test)]
mod test {
    use super::*;
    use system::configurations::DbMode;

    type Expected = (DbMode, Option<String>);

    #[test]
    fn validate_startup_no_args() {
        let args = vec!["bin_name"];
        let expected = (DbMode::Test(0), None);

        validate_startup_common(args, expected);
    }

    #[test]
    fn validate_startup_key_override() {
        // Use argument instead of std::env as env apply to all tests
        let args = vec!["bin_name", "--tls_private_key_override=42"];
        let expected = (DbMode::Test(0), Some("42".to_owned()));

        validate_startup_common(args, expected);
    }

    #[test]
    fn validate_startup_raft_1() {
        let args = vec![
            "bin_name",
            "--config=src/bin/node_settings_local_raft_1.toml",
        ];
        let expected = (DbMode::Test(0), None);

        validate_startup_common(args, expected);
    }

    #[test]
    fn validate_startup_raft_2_index_1() {
        let args = vec![
            "bin_name",
            "--config=src/bin/node_settings_local_raft_2.toml",
            "--index=1",
        ];
        let expected = (DbMode::Test(1), None);

        validate_startup_common(args, expected);
    }

    #[test]
    fn validate_startup_raft_3() {
        let args = vec![
            "bin_name",
            "--config=src/bin/node_settings_local_raft_1.toml",
        ];
        let expected = (DbMode::Test(0), None);

        validate_startup_common(args, expected);
    }

    fn validate_startup_common(args: Vec<&str>, expected: Expected) {
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
        let (expected_mode, expected_key) = expected;
        assert_eq!(config.compute_db_mode, expected_mode);
        assert_eq!(
            config.tls_config.pem_pkcs8_private_key_override,
            expected_key
        );
    }
}
