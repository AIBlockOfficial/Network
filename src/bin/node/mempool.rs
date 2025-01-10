//! App to run a mempool node.

use aiblock_network::configurations::MempoolNodeConfig;
use aiblock_network::MempoolNode;
use aiblock_network::{
    get_sanction_addresses, loop_wait_connnect_to_peers_async, loops_re_connect_disconnect, routes,
    shutdown_connections, ResponseResult, SANC_LIST_PROD,
};
use clap::{App, Arg, ArgMatches};
use config::ConfigError;
use std::net::SocketAddr;
use tracing::info;

pub async fn run_node(matches: &ArgMatches<'_>) {
    let mut config = configuration(load_settings(matches));

    info!("Start node with config {config:?}");

    config.sanction_list = get_sanction_addresses(SANC_LIST_PROD.to_string(), &config.jurisdiction);
    let node = MempoolNode::new(config, Default::default()).await.unwrap();
    let api_inputs = node.api_inputs();

    info!("API Inputs: {api_inputs:?}");
    info!("Started node at {}", node.local_address());

    let (node_conn, addrs_to_connect, expected_connected_addrs) = node.connect_info_peers();
    let local_event_tx = node.local_event_tx().clone();
    let threaded_calls_tx = node.threaded_call_tx().clone();

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
    info!("Raft and Storage connection complete");

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
        let (api_addr, api_tls, api_keys, routes_pow, peer) = api_inputs;
        let threaded_calls_tx = threaded_calls_tx;

        info!("Warp API started on port {:?}", api_addr.port());
        info!("");

        let mut bind_address = "0.0.0.0:0".parse::<SocketAddr>().unwrap();
        bind_address.set_port(api_addr.port());

        let serve = warp::serve(routes::mempool_node_routes(
            api_keys,
            routes_pow,
            threaded_calls_tx,
            peer,
        ).or(warp::path(".health").and_then(handlers::health_check_handler)))
        .run_handle()
        .bind_with_graceful_shutdown(async {
            let (addr, server) = bind_address.bind_server(()
                .boxed_local(),
                axum_handle
                    .clone(),
            );
            info!("Server started on http://{}", addr);
            server.await.unwrap();

        async move {
            if let Some(api_tls) = api_tls {
                serve
                    .tls()
                    .key(&api_tls.pem_pkcs8_private_keys)
                    .cert(&api_tls.pem_certs)
                    .run(bind_address)
                    .await;
            } else {