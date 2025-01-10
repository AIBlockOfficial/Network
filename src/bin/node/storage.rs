//! App to run a storage node.

use aiblock_network::configurations::StorageNodeConfig;
use aiblock_network::StorageNode;
use aiblock_network::{
    loop_wait_connnect_to_peers_async, loops_re_connect_disconnect, routes, shutdown_connections,
    ResponseResult,
};
use clap::{App, Arg, ArgMatches};
use config::ConfigError;
use std::net::SocketAddr;
use tracing::info;

pub async fn run_node(matches: &ArgMatches<'_>) {
    let config = configuration(load_settings(matches));

    info!("Start node with config {config:?}");
    let node = StorageNode::new(config, Default::default()).await.unwrap();

    info!("Started node at {}", node.local_address());

    let (node_conn, addrs_to_connect, expected_connected_addrs) = node.connect_info_peers();
    let api_inputs = node.api_inputs();

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
            info!("Peer connect complete, start Raft");
            raft_loop.await;
            info!("Raft complete");
        })
    };

    // Warp API
    let warp_handle = tokio::spawn({
        let (db, api_addr, api_tls, api_keys, api_pow_info) = api_inputs;

        info!("Warp API started on port {:?}", api_addr.port());
        info!("");

        let mut bind_address = "0.0.0.0:0".parse::<SocketAddr>().unwrap();
        bind_address.set_port(api_addr.port());
        let node_conn_debug = node_conn.clone();

        async move {
            let serve = warp::serve(routes::storage_node_routes(
                api_keys,
                api_pow_info,
                db,
                node_conn_debug,
            )
            .or(warp::path("health_check").and_then(routes::health_check_handler)));
            if let Some(api_tls) = api_tls {
                serve
                    .tls()
                    .key(&api_tls.pem_pkcs8_private_keys)
                    .cert(&api_tls.pem_certs)
                    .run(bind_address)
                    .await;
            } else {
                serve.run(bind_address).await;
            }
        }
    });

    // REQUEST HANDLING
    let main_loop_handle = tokio::spawn({
        let mut node = node;
        let mut node_conn = node_conn.clone();

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

            node.close_