//! App to run a mining node.

use aiblock_network::configurations::{ExtraNodeParams, MinerNodeConfig, UserNodeConfig};
use aiblock_network::{
    loop_wait_connnect_to_peers_async, loops_re_connect_disconnect, routes, shutdown_connections,
    ResponseResult,
};
use aiblock_network::{MinerNode, UserNode};
use clap::{App, Arg, ArgMatches};
use config::{ConfigError, Value};
use std::collections::HashMap;
use std::net::SocketAddr;
use tracing::info;

pub async fn run_node(matches: &ArgMatches<'_>) {
    let (config, user_config) = configuration(load_settings(matches));
    info!("Start node with config {:?}", config);
    let node = MinerNode::new(config, Default::default()).await.unwrap();
    info!("Started node at {}", node.local_address());

    let miner_api_inputs = node.api_inputs();
    let shared_wallet_db = Some(node.get_wallet_db().clone());
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

    // Miner main loop
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

            shutdown_connections(&mut node_conn).await;
        }
    });

    match user_config {
        Some(config) => {
            let shared_members = ExtraNodeParams {
                shared_wallet_db,
                ..Default::default()
            };

            info!("Start user node with config {config:?}");
            let user_node = UserNode::new(config, shared_members).await.unwrap();
            let api_inputs = (user_node.api_inputs(), miner_api_inputs);
            info!("Started user node at {}", user_node.local_address());

            let (user_node_conn, user_addrs_to_connect, user_expected_connected_addrs) =
                user_node.connect_info_peers();
            let user_local_event_tx = user_node.local_event_tx().clone();
            let threaded_calls_tx = user_node.threaded_call_tx().clone();

            // PERMANENT CONNEXION/DISCONNECTION HANDLING
            let (
                (user_conn_loop_handle, user_stop_re_connect_tx),
                (user_disconn_loop_handle, user_stop_disconnect_tx),
            ) = {
                let (user_re_connect, user_disconnect_test) = loops_re_connect_disconnect(
                    user_node_conn.clone(),
                    user_addrs_to_connect,
                    user_local_event_tx,
                );