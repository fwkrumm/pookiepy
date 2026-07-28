//! Main entry point for the gRPC server.
//!
//! This binary sets up and runs the `BaseServer` using Tonic.

use grpchook_server::server::core::BaseServer;
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Create the server instance
    let server = BaseServer::new(
        50051, // port
        "MyBaseServer", // name
        "127.0.0.1", // ip
        None, // config
        None, // global_exit_event
    );

    // Start the server
    println!("Starting server on 127.0.0.1:50051");
    server.serve_forever().await;

    Ok(())
}