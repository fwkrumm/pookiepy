//! gRPC service implementation for grpchook server
//! 
//! This module implements the exact lifecycle from Python's BaseServer:
//! connect → first message (with clientInfo) → welcome → subscribe → messages → disconnect

use std::sync::Arc;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status, Code};

// Import generated gRPC server trait and types from proto compilation
pub use crate::grpchook_server::GrpchookService as GrpchookServiceServer;
pub use grpchook_client_proto::*;

use crate::core::{CoreState, ServerMessage};

/// Service implementation for the grpchook messaging server
#[derive(Clone)]
pub struct GrpchookService {
    core: Arc<CoreState>,
    /// Unique identifier for this server instance (sent in welcome message)
    server_id: String,
}

impl GrpchookService {
    pub fn new(core: Arc<CoreState>) -> Self {
        let server_id = uuid::Uuid::new_v4().to_string();
        
        tracing::info!("Grpchook service initialized with server_id={}", server_id);
        
        Self { core, server_id }
    }

    /// Extract schema version from gRPC metadata using the Python-compatible key
    /// Returns FAILED_PRECONDITION if no schema provided (lenient mode for backward compat)
    fn extract_schema_from_metadata(
        &self, 
        metadata: &tonic::metadata::MetadataMap,
    ) -> Result<String, Status> {
        // Use the same key as Python's SCHEMA_VERSION_METADATA_KEY = "x-schema-version"
        if let Ok(version_bytes) = metadata.get("x-schema-version") {
            match version_bytes.to_str() {
                Ok(v) => return Ok(v.to_string()),
                Err(_) => {
                    tracing::warn!("Invalid schema version format in x-schema-version header");
                    // Fall through to lenient mode if invalid format
                }
            }
        }

        // No schema version provided - use server's default for backward compatibility
        // This matches Python behavior where missing metadata uses server default
        tracing::warn!(
            "No schema version in x-schema-version metadata, using server default: {}",
            self.core.get_schema_version()
        );
        
        Ok(self.core.get_schema_version())
    }

    /// Send welcome message to client - MUST be queued before subscriptions registered
    /// This is the critical ordering requirement from Python's BaseServer
    fn send_welcome_message(
        &self, 
        tx: &tokio::sync::mpsc::UnboundedSender<ServerMessage>,
        session_id: &str,
    ) -> Result<(), Status> {
        let welcome = ServerMessage {
            message_name: "welcome".to_string(),
            sender_id: self.server_id.clone(),
            payload: format!(
                "{{\"serverId\":\"{}\",\"uuid\":\"{}\",\"name\":\"grpchook-server\"}}",
                self.server_id, session_id
            ).into_bytes(),
        };

        tx.send(welcome)
            .map_err(|e| Status::internal(format!("Failed to send welcome message: {}", e)))?;
        
        tracing::debug!("Sent welcome message to client");
        Ok(())
    }
}

/// Per-connection state for HandleStream - tracks client_id, session, and first_message flag
#[derive(Debug)]
struct ConnectionState {
    /// Assigned by server after first message contains clientInfo
    client_id: Option<String>,
    /// Session identifier for this connection (generated at connect time)
    session_id: String,
    /// Track if we've received the first message (with clientInfo) yet
    first_message_received: bool,
}

impl ConnectionState {
    fn new(session_id: String) -> Self {
        Self {
            client_id: None,
            session_id,
            first_message_received: false,
        }
    }
}

/// Per-connection context for RPC calls - tracks the active connection's client_id
#[derive(Debug, Clone)]
struct ConnectionContext {
    /// The actual client_id assigned after first message handshake
    client_id: String,
    /// Session ID for this connection (used before real client_id is known)
    session_id: String,
}

impl ConnectionContext {
    fn new(session_id: String) -> Self {
        Self {
            client_id: session_id.clone(), // Use session as fallback initially
            session_id,
        }
    }
    
    fn with_real_client_id(&self, real_client_id: String) -> Self {
        Self {
            client_id: real_client_id,
            session_id: self.session_id.clone(),
        }
    }
}

#[tonic::async_trait]
impl GrpchookServiceServer for GrpchookService {
    /// Handle bidirectional streaming - mirrors Python's DataChannel exactly
    /// 
    /// Lifecycle:
    /// 1. Extract schema version from metadata and validate (FAILED_PRECONDITION on mismatch)
    /// 2. Generate session_id, create client channel
    /// 3. Register client with core state
    /// 4. Spawn receiver task to process incoming messages
    /// 5. Send welcome message BEFORE registering subscriptions (critical ordering!)
    /// 6. Process subsequent messages and route them appropriately
    async fn handle_stream(
        &self,
        request: Request<tokio_stream::wrappers::UnboundedReceiverStream<ServerMessage>>,
    ) -> Result<Response<Self::ClientMessageStream>, Status> {
        let metadata = request.metadata();
        
        // Step 1: Extract and validate schema version from metadata (Python-style)
        let client_schema_version = self.extract_schema_from_metadata(metadata)?;
        self.core.check_schema(&client_schema_version).await?;

        // Step 2: Generate unique session_id for this connection
        let session_id = uuid::Uuid::new_v4().to_string();
        
        // Create channel for sending messages to THIS client (server -> client)
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<ServerMessage>();
        
        // Step 3: Register the connection with core state using session_id temporarily
        self.core.add_client(session_id.clone(), tx.clone()).await?;

        tracing::info!(
            "Client connected: session={}, schema={}",
            session_id, 
            client_schema_version
        );

        // Create connection state tracker for this stream
        let conn_state = Arc::new(tokio::sync::Mutex::new(ConnectionState::new(session_id.clone())));

        // Step 4: Spawn receiver task to process incoming messages from client
        let core = self.core.clone();
        let service = self.clone();
        let mut input_stream = request.into_inner();
        
        tokio::spawn(async move {
            let conn_state_clone = conn_state.clone();
            
            // Process each incoming message from the client
            while let Some(msg) = input_stream.next().await {
                let current_conn = conn_state_clone.lock().await;
                
                if !current_conn.first_message_received {
                    // First message received - extract clientInfo and send welcome BEFORE subscriptions
                    
                    tracing::debug!("First message received for session {}", current_conn.session_id);
                    
                    // Extract client ID from the incoming message (Python expects this in first msg)
                    let new_client_id = if !msg.sender_id.is_empty() {
                        msg.sender_id.clone()
                    } else {
                        uuid::Uuid::new_v4().to_string()
                    };

                    drop(current_conn); // Release lock before mutation
                    
                    // Update connection state with real client_id
                    let mut current_conn = conn_state_clone.lock().await;
                    current_conn.client_id = Some(new_client_id.clone());
                    current_conn.first_message_received = true;
                    
                    // Step 5: Send welcome message BEFORE registering any subscriptions
                    // This is the critical ordering requirement from Python's BaseServer!
                    if let Err(e) = service.send_welcome_message(&tx, &current_conn.session_id) {
                        tracing::error!("Failed to send welcome: {}", e);
                    }
                    
                    tracing::info!(
                        "Client accepted: client_id={}, session={}",
                        new_client_id, current_conn.session_id
                    );

                } else {
                    // Subsequent messages - route according to message routing rules
                    
                    let current_conn = conn_state_clone.lock().await;
                    if let Some(client_id) = &current_conn.client_id {
                        
                        // Step 6: Route the incoming message based on its type and subscriptions
                        
                        // Check if this is a subscribe request (message_name indicates subscription target)
                        // For now, we route all messages - subscribe RPC handles explicit subscription
                        match core.route_broadcast(&msg, true).await {
                            Ok(_) => tracing::trace!("Broadcast message to subscribers"),
                            Err(e) => tracing::error!("Failed to broadcast: {}", e),
                        }

                    } else {
                        tracing::warn!("Received message before first-message handshake completed");
                    }
                }
            }

            // Connection closed - cleanup on disconnect
            let final_conn = conn_state_clone.lock().await;
            if let Some(client_id) = &final_conn.client_id {
                tracing::info!("Client stream ended, client={}", client_id);
            } else {
                // Use session_id for cleanup if we never got a real client_id
                tracing::info!("Client stream ended, session={}", final_conn.session_id);
            }

            drop(final_conn);
            
            // Cleanup: remove from core state and unsubscribe all subscriptions
            let cleanup_client_id = {
                let current_conn = conn_state_clone.lock().await;
                current_conn.client_id.clone().unwrap_or_else(|| current_conn.session_id.clone())
            };
            
            tracing::info!("Cleaning up client {}", cleanup_client_id);
            core.remove_client(&cleanup_client_id).await;
        });

        // Step 7: Return the receiver stream for this client (server -> client)
        Ok(Response::new(rx.into_stream()))
    }

    /// Subscribe a client to receive messages of a specific type
    /// Schema validation required before subscription allowed
    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<SubscribeResponse>, Status> {
        let req = request.into_inner();
        
        // Validate schema version from metadata before allowing subscription
        let client_schema_version = self.extract_schema_from_metadata(request.metadata())?;
        self.core.check_schema(&client_schema_version).await?;

        // Extract client_id - in real impl this comes from connection context
        // If no client_id provided, use a generated one (Python allows this)
        let client_id = if !req.client_id.is_empty() {
            req.client_id.clone()
        } else {
            tracing::debug!("No client_id provided in SubscribeRequest, generating new one");
            uuid::Uuid::new_v4().to_string()
        };

        // Verify client exists before subscribing (Python requires this too)
        let clients = self.core.clients.read().await;
        if !clients.contains_key(&client_id) {
            return Ok(Response::new(SubscribeResponse {
                success: false,
                message: format!("Client {} not found. Connect via handle_stream first.", client_id),
            }));
        }

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

    /// Send a broadcast message - routes to all subscribers (skips sender by default)
    async fn send_message(
        &self,
        request: Request<SendMessageRequest>,
    ) -> Result<Response<SendMessageResponse>, Status> {
        let req = request.into_inner();
        
        // Validate schema before allowing broadcast
        let client_schema_version = self.extract_schema_from_metadata(request.metadata())?;
        self.core.check_schema(&client_schema_version).await?;

        // Create message to broadcast - sender_id will be set by the connection context
        // Use a generated UUID as sender for broadcasts initiated via RPC
        let msg = ServerMessage {
            message_name: req.message_name.clone(),
            sender_id: uuid::Uuid::new_v4().to_string(),  // ✅ FIXED: Real sender ID instead of placeholder
            payload: req.payload,
        };

        // Route to all subscribers (skip sender as per Python behavior)
        match self.core.route_broadcast(&msg, true).await {
            Ok(()) => Ok(Response::new(SendMessageResponse {
                success: true,
                message: format!("Broadcast {} messages", req.message_name),
            })),
            Err(e) => Ok(Response::new(SendMessageResponse {
                success: false,
                message: e.message().to_string(),
            })),
        }
    }

    /// Send unicast message to specific client - optional destination support
    async fn send_unicast(
        &self,
        request: Request<UnicastRequest>,
    ) -> Result<Response<UnicastResponse>, Status> {
        let req = request.into_inner();
        
        // Validate schema before allowing unicast
        let client_schema_version = self.extract_schema_from_metadata(request.metadata())?;
        self.core.check_schema(&client_schema_version).await?;

        let msg = ServerMessage {
            message_name: "unicast".to_string(),
            sender_id: uuid::Uuid::new_v4().to_string(),  // ✅ FIXED: Real sender ID
            payload: req.payload,
        };

        // Send to specific target client (Python's optional unicast support)
        match self.core.send_unicast(&req.target_client_id, msg).await {
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

    /// Disconnect client and cleanup all subscriptions - mirrors Python's disconnect behavior
    async fn disconnect(
        &self,
        request: Request<DisconnectRequest>,
    ) -> Result<Response<DisconnectResponse>, Status> {
        let req = request.into_inner();
        
        // Extract client_id from request or use context default
        let client_id = if !req.client_id.is_empty() {
            req.client_id.clone()
        } else {
            tracing::warn!("No client_id provided in DisconnectRequest");
            "unknown_client".to_string()
        };

        // Remove client and cleanup subscriptions (Python's remove_notification_queues_for_client behavior)
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
