//! App to run a storage node.

use clap::{App, Arg};
use system::configurations::StorageNodeConfig;
use system::{Response, StorageNode};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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
    let node = StorageNode::new(config).await?;

    println!("Started node at {}", node.address());

    // RAFT HANDLING
    let raft_loop_handle = {
        let connect_all = node.connect_to_raft_peers();
        let raft_loop = node.raft_loop();
        tokio::spawn(async move {
            // Need to connect first so Raft messages can be sent.
            println!("Start connect to compute peers");
            let result = connect_all.await;
            println!("Peer connect complete, start Raft: {:?}", result);
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
                        node.send_stored_block().await.unwrap();
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
        }
    });

    let (main, raft) = tokio::join!(main_loop_handle, raft_loop_handle);
    main.unwrap();
    raft.unwrap();
    Ok(())
}
