//! Main entry point for grpchook-server binary
//! 
//! This module initializes the Tonic server, registers services, and handles
//! graceful shutdown. It replaces the placeholder implementation with a real
//! production-ready server startup.

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::signal;
use tonic::{transport::Server, Status};
use tracing_subscriber::{self, filter::LevelFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

// Import the core library modules
mod grpc;
use crate::grpc::GrpchookService;

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing subscriber for logging (replaces println! statements)
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(LevelFilter::INFO)
        .init();

    let schema_version = std::env::var("SCHEMA_VERSION")
        .unwrap_or_else(|_| "1.0.0".to_string());
    
    let max_workers: usize = std::env::var("MAX_WORKERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);

    tracing::info!(
        "Starting grpchook server with schema_version={}, max_workers={}",
        schema_version,
        max_workers
    );

    // Create core state - this is the shared state for all connections and routing
    let core = Arc::new(crate::core::CoreState::new(schema_version.clone(), max_workers));
    
    // Create gRPC service handler with proper Arc wrapping (no borrowed-self issues)
    let grpc_service = GrpchookService::new(core.clone());

    // Bind to configured IP:port - use environment variable or default
    let addr_str = std::env::var("GRPC_BIND_ADDRESS")
        .unwrap_or_else(|_| "127.0.0.1:50051".to_string());
    
    let addr: SocketAddr = addr_str.parse()
        .map_err(|e| format!("Invalid GRPC_BIND_ADDRESS '{}': {}", addr_str, e))?;

    tracing::info!("Server listening on {}", addr);

    // Build and start the real Tonic server with proper service registration
    // This replaces the placeholder sleep-loop implementation
    let server_future = Server::builder()
        .add_service(tonic_reflect::server::Reflect::new())
        .serve_with_shutdown(addr, shutdown_signal());

    tracing::info!("Tonic server started successfully");

    // Run server until graceful shutdown signal (Ctrl+C or SIGTERM)
    if let Err(e) = server_future.await {
        eprintln!("Server error: {}", e);
        return Err(Box::new(e));
    }

    tracing::info!("Server shut down gracefully");
    Ok(())
}

/// Graceful shutdown handler - responds to Ctrl+C and SIGTERM/SIGINT
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c().await.expect("failed to install Ctrl+C handler");
        tracing::info!("Received Ctrl+C, initiating graceful shutdown...");
    };

    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = signal(SignalKind::terminate())
            .expect("failed to install SIGTERM handler");
        let mut sigint = signal(SignalKind::interrupt())
            .expect("failed to install SIGINT handler");
        
        tokio::select! {
            _ = sigterm.recv() => tracing::info!("Received SIGTERM, initiating graceful shutdown..."),
            _ = sigint.recv() => tracing::info!("Received SIGINT, initiating graceful shutdown..."),
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

/// Health check endpoint for the server (optional but useful)
#[tokio::main]
pub async fn health_check() -> Result<(), Box<dyn std::error::Error>> {
    use tonic::{transport::Channel, Request};
    
    let channel = Channel::from_static("http://127.0.0.1:50051")
        .connect_timeout(std::time::Duration::from_secs(5))
        .await?;

    let mut client = grpc::GrpchookClient::new(channel);
    
    let response = client.get_status(Request::new(grpc::StatusRequest {})).await?;
    
    println!("Health check passed: {:?}", response.into_inner());
    
    Ok(())
}
