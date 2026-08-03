use pookiepy_rust_server::server::{BaseServer, DefaultHooks, ServerConfig};

/// Run async Rust pookiepy-compatible server.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,hyper=warn,h2=warn".into()),
        )
        .with_target(false)
        .compact()
        .init();

    let bind_addr = std::env::var("pookiepy_RUST_ADDR").unwrap_or_else(|_| "[::]:50051".to_owned());
    let name = std::env::var("pookiepy_RUST_NAME").unwrap_or_else(|_| "rust-server".to_owned());

    let server = BaseServer::new(bind_addr, name, DefaultHooks, ServerConfig::default())?;

    let shutdown_server = server.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            shutdown_server.shutdown();
        }
    });

    server.serve_forever().await?;
    Ok(())
}
