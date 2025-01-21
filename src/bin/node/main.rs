//! App to run an AIBlock node.

use clap::{App, ArgMatches};
use tracing::info;

mod mempool;
mod miner;
mod pre_launch;
mod storage;
mod user;

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    tracing_subscriber::fmt::init();
    let matches = clap_app().get_matches();
    launch_node_with_args(matches).await;
}

async fn launch_node_with_args(matches: ArgMatches<'_>) {
    if let Some(sub_command) = matches.subcommand_name() {
        let sub_matches = matches.subcommand_matches(sub_command).unwrap();
        match sub_command {
            "user" => user::run_node(sub_matches).await,
            "miner" => miner::run_node(sub_matches).await,
            "mempool" => mempool::run_node(sub_matches).await,
            "storage" => storage::run_node(sub_matches).await,
            "pre_launch" => pre_launch::run_node(sub_matches).await,
            invalid_type => panic!("Invalid node type: {:?}", invalid_type),
        }
    } else {
        info!("Node type needs to be specified.")
    }
}

fn clap_app<'a, 'b>() -> App<'a, 'b> {
    App::new("AIBlock Node")
        .about("Runs an AIBlock node.")
        .subcommand(user::clap_app())
        .subcommand(miner::clap_app())
        .subcommand(mempool::clap_app())
        .subcommand(storage::clap_app())
        .subcommand(pre_launch::clap_app())
}
