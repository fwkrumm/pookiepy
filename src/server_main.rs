use std::net::SocketAddr;
use std::sync::Arc;
use tokio::signal;
use tonic::{transport::Server, Status};
use tracing_subscriber::{self, filter::LevelFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

// Import the core and gRPC modules.
mod core;
mod grpc;

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(LevelFilter::INFO)
        .init();

    let schema_version = "1.0.0".to_string();
    let max_workers = 100;

    let core = Arc::new(core::Core::new(schema_version, max_workers));
    
    let addr = SocketAddr::from(([127, 0, 0, 1], 50051));
    println!("Server listening on {}", addr);

    let server = Server::builder()
        .add_service(tonic_reflect::server::Reflect::new())
        // This is where the actual service would be added.
        .serve(addr);

    let graceful = server.with_graceful_shutdown(shutdown_signal()).await;

    if let Err(e) = graceful {
        eprintln!("Server error: {}", e);
    }

    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c().await.expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}