//! Integration tests for grpchook-server
//! 
//! These tests verify the complete system behavior by testing through the gRPC interface.

use std::net::SocketAddr;
use tokio_stream::{StreamExt, Stream};
use tonic::{transport::Channel, Request, Response};
use grpchook_server_proto::*;

// Re-export types from proto compilation for integration tests
pub use grpchook_client_proto::*;

/// Test helper to create a gRPC client connection
async fn create_test_client() -> Result<grpchook_client_proto::GrpchookClient<Channel>, Box<dyn std::error::Error>> {
    let channel = Channel::from_static("http://127.0.0.1:50051")
        .connect_timeout(std::time::Duration::from_secs(5))
        .await?;

    Ok(grpchook_client_proto::GrpchookClient::new(channel))
}

#[tokio::test]
async fn test_schema_validation() {
    // This test requires a running server - skip if not available
    let client = match create_test_client().await {
        Ok(c) => c,
        Err(_) => return, // Server not running, skip test
    };

    // Test schema validation through gRPC metadata
    let mut request = Request::new(grpchook_server_proto::StatusRequest {});
    
    // Add correct schema version - should succeed
    request.metadata_mut().insert("x-schema-version", "1.0.0".parse().unwrap());
    
    match client.get_status(request).await {
        Ok(_) => println!("Schema validation test passed"),
        Err(e) => panic!("Expected success but got error: {}", e),
    }
}

#[tokio::test]
async fn test_connection_counting() {
    // This test requires a running server - skip if not available  
    let client = match create_test_client().await {
        Ok(c) => c,
        Err(_) => return, // Server not running, skip test
    };

    // Get initial status
    let response = client.get_status(Request::new(StatusRequest {})).await.unwrap();
    
    println!("Initial connected clients: {}", response.into_inner().connected_clients);
}

#[tokio::test]
async fn test_subscribe_and_broadcast() {
    // This test requires a running server - skip if not available
    let client = match create_test_client().await {
        Ok(c) => c,
        Err(_) => return, // Server not running, skip test
    };

    // Subscribe to message type
    let mut request = Request::new(SubscribeRequest {
        message_name: "test-event".to_string(),
        client_id: "".to_string(), // Let server assign one
    });
    
    request.metadata_mut().insert("x-schema-version", "1.0.0".parse().unwrap());

    match client.subscribe(request).await {
        Ok(response) => {
            let resp = response.into_inner();
            assert!(resp.success, "Subscribe should succeed: {}", resp.message);
            println!("Subscribe test passed: {}", resp.message);
        }
        Err(e) => panic!("Subscribe failed: {}", e),
    }
}

#[tokio::test]
async fn test_disconnect_cleanup() {
    // This test requires a running server - skip if not available
    let client = match create_test_client().await {
        Ok(c) => c,
        Err(_) => return, // Server not running, skip test
    };

    // Disconnect with empty client_id (uses current connection context)
    let mut request = Request::new(DisconnectRequest {
        client_id: "".to_string(),
    });
    
    request.metadata_mut().insert("x-schema-version", "1.0.0".parse().unwrap());

    match client.disconnect(request).await {
        Ok(response) => {
            let resp = response.into_inner();
            assert!(resp.success, "Disconnect should succeed: {}", resp.message);
            println!("Disconnect test passed: {}", resp.message);
        }
        Err(e) => panic!("Disconnect failed: {}", e),
    }
}
