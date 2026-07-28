use std::sync::Arc;
use tokio::signal;
use tonic::transport::Server;
use tracing_subscriber::{self, util::SubscriberInitExt};

use crate::core::Core;
use crate::grpc::server::GrpcServer;

mod core;
mod grpc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new("info"))
        .init();

    let core = Arc::new(Core::new("1.0".to_string(), 10));
    let server = GrpcServer::new(core.clone());

    let addr = "[::1]:50051".parse()?;
    let server_future = Server::builder()
        .add_service(message::stream_service_server::StreamServiceServer::new(server))
        .serve(addr);

    println!("Server listening on http://{}", addr);

    let shutdown = async {
        signal::ctrl_c().await.unwrap();
        println!("Shutting down...");
    };

    tokio::select! {
        _ = server_future => {},
        _ = shutdown => {},
    }

    Ok(())
}