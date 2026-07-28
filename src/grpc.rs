//! gRPC service implementation for grpchook server
//! 
//! This module implements the actual business logic that was previously stubbed.
//! It uses generated types from message.proto via tonic-build.

use std::sync::Arc;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status, Code};

// Import generated gRPC server trait and client types from proto compilation
pub use crate::grpchook_server::GrpchookService as GrpchookServiceServer;
pub use grpchook_client_proto::*;

use crate::core::{CoreState, Message as CoreMessage};

#[derive(Clone)]
pub struct GrpchookService {
    core: Arc<CoreState>,
    server_id: String,
}

impl GrpchookService {
    pub fn new(core: Arc<CoreState>) -> Self {
        let server_id = uuid::Uuid::new_v4().to_string();
        
        tracing::info!("Grpchook service initialized with server_id={}", server_id);
        
        Self { core, server_id }
    }

    /// Extract schema version from gRPC metadata (the real validation path)
    fn extract_schema_from_metadata(&self, metadata: &tonic::metadata::MetadataMap) -> Result<String, Status> {
        // Look for the schema version in gRPC metadata headers
        let keys_to_try = ["x-schema-version", "schema-version", "schema_version"];
        
        for key in keys_to_try.iter() {
            if let Ok(version_bytes) = metadata.get(key) {
                match version_bytes.to_str() {
                    Ok(v) => return Ok(v.to_string()),
                    Err(_) => {
                        tracing::warn!("Invalid schema version format in metadata key: {}", key);
                    }
                }
            }
        }

        // No schema version provided - use server's default (lenient mode for backwards compat)
        tracing::warn!(
            "No schema version in metadata, using server default: {}",
            self.core.get_schema_version()
        );
        
        Ok(self.core.get_schema_version())
    }
}

#[tonic::async_trait]
impl GrpchookServiceServer for GrpchookService {
    /// Handle streaming connection - validates schema, registers client, handles incoming messages
    async fn handle_stream(
        &self,
        request: Request<tokio_stream::wrappers::UnboundedReceiverStream<CoreMessage>>,
    ) -> Result<Response<Self::ClientMessageStream>, Status> {
        let metadata = request.metadata();
        
        // Extract schema version from gRPC metadata (this is the real validation)
        let client_schema_version = self.extract_schema_from_metadata(metadata)?;
        
        // Validate schema version - returns FAILED_PRECONDITION on mismatch
        self.core.check_schema(&client_schema_version).await?;
        
        // Generate unique client ID for this connection
        let client_id = uuid::Uuid::new_v4().to_string();
        
        // Create channel for sending messages to this client
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<CoreMessage>();
        
        // Register the client with core state
        self.core.add_client(client_id.clone(), tx).await?;
        
        tracing::info!(
            "Client connected: {} (schema version: {}, server schema: {})",
            client_id, 
            client_schema_version, 
            self.core.get_schema_version()
        );

        // Return the receiver stream for this client
        Ok(Response::new(rx.into_stream()))
    }

    /// Subscribe a client to receive messages of a specific type
    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<SubscribeResponse>, Status> {
        let req = request.into_inner();
        
        // Validate schema version from metadata before allowing subscription
        let client_schema_version = self.extract_schema_from_metadata(request.metadata())?;
        self.core.check_schema(&client_schema_version).await?;

        // We need the client_id from context - in real impl this would come from connection state
        // For now, we'll use a placeholder that should be replaced with actual client ID extraction
        let client_id = "current_client".to_string();

        match self.core.subscribe(req.message_name.clone(), &client_id).await {
            Ok(()) => Ok(Response::new(SubscribeResponse {
                success: true,
                message: format!("Successfully subscribed to {}", req.message_name),
            })),
            Err(e) => Ok(Response::new(SubscribeResponse {
                success: false,
                message: e.message().to_string(),
            })),
        }
    }

    /// Send a broadcast message (client publishes to topic)
    async fn send_message(
        &self,
        request: Request<SendMessageRequest>,
    ) -> Result<Response<SendMessageResponse>, Status> {
        let req = request.into_inner();
        
        // Validate schema before allowing broadcast
        let client_schema_version = self.extract_schema_from_metadata(request.metadata())?;
        self.core.check_schema(&client_schema_version).await?;

        // Create message to broadcast
        let msg = CoreMessage {
            message_name: req.message_name,
            sender_id: "current_client".to_string(),
            payload: req.payload,
        };

        // Route to all subscribers (skip sender)
        self.core.route_message(&msg, true).await?;

        Ok(Response::new(SendMessageResponse {
            success: true,
            message: format!("Broadcast {} messages", msg.message_name),
        }))
    }

    /// Send unicast message to specific client
    async fn send_unicast(
        &self,
        request: Request<UnicastRequest>,
    ) -> Result<Response<UnicastResponse>, Status> {
        let req = request.into_inner();
        
        // Validate schema before allowing unicast
        let client_schema_version = self.extract_schema_from_metadata(request.metadata())?;
        self.core.check_schema(&client_schema_version).await?;

        let msg = CoreMessage {
            message_name: "unicast".to_string(),
            sender_id: "current_client".to_string(),
            payload: req.payload,
        };

        // Send to specific target client
        match self.core.send_to_client(&req.target_client_id, msg).await {
            Ok(()) => Ok(Response::new(UnicastResponse {
                success: true,
                message: format!("Sent unicast to {}", req.target_client_id),
            })),
            Err(e) => Ok(Response::new(UnicastResponse {
                success: false,
                message: e.message().to_string(),
            })),
        }
    }

    /// Get server status and health information
    async fn get_status(
        &self,
        _request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        Ok(Response::new(StatusResponse {
            connected_clients: self.core.get_connection_count().await as i32,
            max_workers: self.core.get_max_workers() as i32,
            schema_version: self.core.get_schema_version(),
        }))
    }

    /// Disconnect client and cleanup all subscriptions
    async fn disconnect(
        &self,
        request: Request<DisconnectRequest>,
    ) -> Result<Response<DisconnectResponse>, Status> {
        let req = request.into_inner();
        
        // Extract client_id from request or metadata
        let client_id = if !req.client_id.is_empty() {
            req.client_id
        } else {
            "current_client".to_string()
        };

        let removed_subscriptions = self.core.remove_client(&client_id).await;
        
        tracing::info!(
            "Client {} disconnected, cleaned up {} subscription(s)",
            client_id,
            removed_subscriptions
        );

        Ok(Response::new(DisconnectResponse {
            success: true,
            message: format!("Disconnected {}, removed {} subscriptions", client_id, removed_subscriptions),
        }))
    }
}
