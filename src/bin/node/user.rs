//! App to run a user node.

use aiblock_network::configurations::UserNodeConfig;
use aiblock_network::interfaces::{UserApiRequest, UserRequest, UtxoFetchType};
use aiblock_network::{
    loop_wait_connnect_to_peers_async, loops_re_connect_disconnect, routes, shutdown_connections,
    ResponseResult, UserNode,
};
use clap::{App, Arg, ArgMatches};
use config::{ConfigError, Value};
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::time::{self, Duration};
use tracing::{info, trace, warn};
use warp::{path, Filter};

//================== BIN CONSTANTS ==================//

/// Interval between requested UTXO realignment, in seconds
const UTXO_REALIGN_INTERVAL: u64 = 120;

/// Default user API port
const DEFAULT_USER_API_PORT: i64 = 3000;

/// Default peer limit
const DEFAULT_PEER_LIMIT: i64 = 1000;

//===================================================//

pub async fn run_node(matches: &ArgMatches<'_>) {
    let config = configuration(load_settings(matches));

    info!("Starting node with config: {config:?}");
    info!("");

    let node = UserNode::new(config, Default::default()).await.unwrap();

    info!("Started node at {}", node.local_address());

    let (node_conn, addrs_to_connect, expected_connected_addrs) = node.connect_info_peers();
    let local_event_tx = node.local_event_tx().clone();
    let threaded_calls_tx = node.threaded_call_tx().clone();
    let api_inputs = node.api_inputs();
    let peer_node = node.get_node().clone();
    let wallet_db = node.get_wallet_db().clone();

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
            shutdown_connections(&mut node_conn).await;
        }
    });

    // Warp API
    let warp_handle = tokio::spawn({
        let (db, node, api_addr, api_tls, api_keys, api_pow_info) = api_inputs;
        let threaded_calls_tx = threaded_calls_tx.clone();

        info!("Warp API started on port {:?}", api_addr.port());
        info!("");

        let mut bind_address = "0.0.0.0:0".parse::<SocketAddr>().unwrap();
        bind_address.set_port(api_addr.port());

        async move {
            let serve = warp::serve(routes::user_node_routes(
                api_keys,
                api_pow_info,
                db,