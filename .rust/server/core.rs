//! Core server logic and data structures.
//!
//! This module defines the `BaseServer` struct, which encapsulates the server's behavior,
//! including connection handling, message routing, and hook execution.
//!
//! The `Peer` struct represents a connected client.

use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, error, info, warn};
use dashmap::DashMap;

use crate::server::grpc::{Message, MetaInformation, ClientProvides, ServerProvides, Payload, DataPoint};

/// Represents a connected client.
#[derive(Debug, Clone)]
pub struct Peer {
    pub peer: String,
    pub session_id: String,
    pub client_id: Option<String>,
    pub name: Option<String>,
}

impl std::fmt::Display for Peer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Peer(peer={}, clientId={:?}, name={:?}, sessionId={})",
            self.peer, self.client_id, self.name, self.session_id
        )
    }
}

/// Configuration for the server.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub max_queue_elements: usize,
    pub max_workers: Option<usize>,
    pub shutdown_poll_interval: f64,
    // Note: Compression and server_options are not directly modeled here as they are gRPC specifics.
    // They would be handled at the gRPC server level in the `grpc` module.
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            max_queue_elements: 0, // Unlimited
            max_workers: None,
            shutdown_poll_interval: 0.1,
        }
    }
}

/// Core server logic.
///
/// This struct encapsulates the server's state and behavior, including:
/// - Connection handling
/// - Message routing via `DataRegister`
/// - Hook execution for lifecycle events
pub struct BaseServer {
    /// The server's name.
    pub name: String,
    /// Unique identifier for this server instance.
    uid: String,
    /// IP address to bind to.
    ip: String,
    /// Port to bind to.
    port: u16,
    /// Configuration for the server.
    config: ServerConfig,
    /// Global exit event to signal shutdown.
    global_exit_event: Arc<Mutex<bool>>,
    /// Counter for connected clients (protected by a mutex).
    connected_clients: Arc<Mutex<usize>>,
    /// Data register for routing messages to clients.
    data_register: Arc<RwLock<DashMap<String, Vec<(String, crate::server::threading::NotificationQueue)>>>>,
}

impl BaseServer {
    /// Creates a new `BaseServer`.
    pub fn new(
        port: u16,
        name: &str,
        ip: &str,
        config: Option<ServerConfig>,
        global_exit_event: Option<Arc<Mutex<bool>>>,
    ) -> Self {
        let config = config.unwrap_or_default();
        let global_exit_event = global_exit_event.unwrap_or_else(|| Arc::new(Mutex::new(false)));
        let connected_clients = Arc::new(Mutex::new(0));
        let data_register = Arc::new(RwLock::new(DashMap::new()));

        Self {
            name: name.to_string(),
            uid: uuid::Uuid::new_v4().to_string(),
            ip: ip.to_string(),
            port,
            config,
            global_exit_event,
            connected_clients,
            data_register,
        }
    }

    /// Returns the server's unique identifier.
    pub fn uid(&self) -> &str {
        &self.uid
    }

    /// Handles a client connection.
    ///
    /// This method is called by the gRPC service when a new client connects.
    /// It manages the lifecycle of the connection, including:
    /// - Validating the client's schema version.
    /// - Handling the first message (client info).
    /// - Setting up the notification queue for the client.
    /// - Starting the background thread to process incoming messages.
    pub async fn handle_client_connection(
        &self,
        mut request_iterator: impl futures::stream::Stream<Item = Result<Message, tonic::Status>>,
        context: &tonic::Request<()>, // The context is used for metadata extraction in gRPC.
    ) -> Result<tonic::Response<impl futures::stream::Stream<Item = Result<Message, tonic::Status>>>, tonic::Status> {
        use tokio_stream::wrappers::ReceiverStream;
        use tokio::sync::mpsc;

        // Create a channel for sending messages to the client.
        let (tx, rx) = mpsc::channel::<Message>(self.config.max_queue_elements);

        // Start background task to process incoming requests from the client.
        let peer = Peer {
            peer: context.remote_addr().map(|addr| addr.to_string()).unwrap_or_default(),
            session_id: uuid::Uuid::new_v4().to_string(),
            client_id: None,
            name: None,
        };

        debug!("{}: connected to DataChannel. Checking permissions", peer);

        // Schema version check.
        // In a real implementation, extract metadata from `context` and compare against server's schema hash.
        // For now, we assume the schema is valid.
        let schema_valid = true; // Placeholder logic

        if !schema_valid {
            return Err(tonic::Status::failed_precondition(
                "Proto schema mismatch".to_string(),
            ));
        }

        debug!("{}: Schema check passed", peer);

        // Spawn background task to process incoming messages from client.
        let data_register = self.data_register.clone();
        let connected_clients = self.connected_clients.clone();
        let global_exit_event = self.global_exit_event.clone();
        let server_uid = self.uid.clone();
        let server_name = self.name.clone();

        // Increment the connected clients counter.
        {
            let mut clients = connected_clients.lock().await;
            *clients += 1;
            debug!("{}: connected. Connected clients: {}", peer, *clients);
        }

        tokio::spawn(async move {
            // Process messages from the client in a loop.
            let mut notification_queue = tx; // The sender side of the channel to send messages back to the client.
            let mut peer = peer; // Mutable reference to peer for updating client_id and name.

            loop {
                match request_iterator.next().await {
                    Some(Ok(request)) => {
                        debug!("{}: received message: {:?}", peer, request);

                        if peer.client_id.is_none() {
                            // First message: extract client info
                            debug!("{}: processing first message", peer);
                            // Extract client info from the request's metaInfo
                            let meta_info = match &request.meta_info {
                                Some(mi) => mi,
                                None => {
                                    error!("{}: First message missing metaInfo", peer);
                                    break;
                                }
                            };

                            let client_info = match &meta_info.client_info {
                                Some(ci) => ci,
                                None => {
                                    error!("{}: First message missing clientInfo", peer);
                                    break;
                                }
                            };

                            // Update peer with client info
                            peer.client_id = Some(client_info.uuid.clone());
                            peer.name = Some(client_info.name.clone());

                            // Call on_client_connect hook
                            let accepted = self.on_client_connect(&request, context);

                            if !accepted {
                                error!("{}: connection rejected by on_client_connect", peer);
                                // In a real implementation, we would send an error status.
                                break;
                            }

                            debug!("{}: client accepted", peer);

                            // Call on_client_accepted hook
                            self.on_client_accepted(&peer, &request);

                            // Send welcome message
                            let welcome_message = Message {
                                meta_info: Some(MetaInformation {
                                    message_id: uuid::Uuid::new_v4().to_string(),
                                    message_name: "welcome".to_string(),
                                    client_info: None,
                                    server_info: Some(ServerProvides {
                                        server_id: server_uid.clone(),
                                        uuid: peer.session_id.clone(),
                                        name: server_name.clone(),
                                    }),
                                }),
                                history: Vec::new(), // Placeholder
                                payload: None,       // Placeholder
                            };
                            if notification_queue.send(welcome_message).await.is_err() {
                                error!("{}: failed to send welcome message", peer);
                                break;
                            }

                            // Register requires in data register (race condition fix: send welcome first).
                            for require in &client_info.requires {
                                debug!("{}: registering require {}", peer, require);
                                // Add the notification queue to the data register for this 'require' message name.
                                // This enables routing of messages named `require` to this specific client.
                                if let Some(data_register) = data_register.clone().try_write() {
                                    data_register.entry(require.clone())
                                        .or_insert_with(Vec::new)
                                        .push((peer.client_id.clone().unwrap(), notification_queue.clone()));
                                } else {
                                    error!("{}: Failed to acquire write lock on data register", peer);
                                    break;
                                }
                            }

                            continue; // Continue to process subsequent messages
                        } else {
                            // Subsequent messages: route or process
                            debug!("{}: processing subsequent message", peer);

                            // Call on_receive hook
                            let should_route = self.on_receive(&peer, &request);

                            if should_route {
                                // Route the message to the data register
                                let message_name = match &request.meta_info {
                                    Some(mi) => mi.message_name.clone(),
                                    None => {
                                        error!("{}: Subsequent message missing messageName", peer);
                                        break;
                                    }
                                };
                                debug!("{}: routing message {}", peer, message_name);

                                // Route the message to all registered clients for this message name.
                                if let Some(data_register) = data_register.clone().try_read() {
                                    if let Some(notification_queues) = data_register.get(&message_name) {
                                        // Iterate through all queues registered for this message type.
                                        for (client_id, queue) in notification_queues.iter() {
                                            debug!("{}: routing message {} to client {}", peer, message_name, client_id);
                                            // Attempt to send the message. If the receiver is gone (e.g., client disconnected), we ignore the error.
                                            let _ = queue.send(request.clone()).await;
                                        }
                                    } else {
                                        debug!("{}: No clients registered for message type {}", peer, message_name);
                                    }
                                } else {
                                    error!("{}: Failed to acquire read lock on data register", peer);
                                }
                            } else {
                                debug!("{}: on_receive returned false, not routing", peer);
                            }
                        }
                    }
                    Some(Err(e)) => {
                        error!("{}: Error receiving message: {:?}", peer, e);
                        break;
                    }
                    None => {
                        debug!("{}: Client stream ended", peer);
                        break;
                    }
                }
            }

            // Cleanup on disconnect
            {
                let mut clients = connected_clients.lock().await;
                *clients -= 1;
                debug!("{}: disconnected. Connected clients: {}", peer, *clients);
            }

            // Remove notification queues for this client from the data register.
            if let Some(data_register) = data_register.clone().try_write() {
                for (message_name, queues) in data_register.iter_mut() {
                    // FIX: Properly identify and remove only the queue belonging to this specific peer.
                    queues.retain(|(client_id, _queue)| {
                        client_id != &peer.client_id.clone().unwrap_or_default()
                    });
                }
            } else {
                error!("{}: Failed to acquire write lock on data register during cleanup", peer);
            }
            debug!("{}: Cleaning up data register", peer);
        });

        // Return the stream of messages to be sent to the client.
        let response_stream = ReceiverStream::new(rx);
        Ok(tonic::Response::new(response_stream))
    }

    /// Shuts down the server gracefully.
    pub async fn shutdown(&self) {
        {
            let mut exit_event = self.global_exit_event.lock().await;
            if !*exit_event {
                *exit_event = true;
                info!("Setting global exit event for server shutdown");
            } else {
                debug!("Global exit event already set");
            }
        }
        // Call on_shutdown hook here if needed.
        info!("Server shutdown complete");
    }

    /// Starts the server and waits for termination.
    pub async fn serve_forever(&self) {
        info!(
            "Starting server {} (schema=TODO) on {}:{}. Max workers: {:?}",
            self.name, self.ip, self.port, self.config.max_workers
        );

        // In a real implementation, this would set up the gRPC server,
        // register the service, and start listening.
        // For now, we just simulate the loop.
        loop {
            tokio::select! {
                _ = tokio::time::sleep(tokio::time::Duration::from_secs_f64(self.config.shutdown_poll_interval)) => {
                    // Check for shutdown event
                    let exit_event = self.global_exit_event.lock().await;
                    if *exit_event {
                        break;
                    }
                }
            }
        }

        self.shutdown().await;
        info!("Server stopped");
    }

    // Placeholder for hooks. These would be implemented by subclasses or via a trait.
    pub fn on_init(&self) {
        debug!("BaseServer.on_init called");
    }

    pub fn on_shutdown(&self) {
        debug!("BaseServer.on_shutdown called");
    }

    pub fn on_client_connect(&self, _request: &Message, _context: &tonic::Request<()>) -> bool {
        debug!("BaseServer.on_client_connect called");
        true // Accept by default
    }

    pub fn on_receive(&self, _peer: &Peer, _request: &Message) -> bool {
        debug!("BaseServer.on_receive called");
        true // Route by default
    }

    pub fn on_client_disconnect(&self, _peer: &Peer) {
        debug!("BaseServer.on_client_disconnect called");
    }

    pub fn on_client_accepted(&self, _peer: &Peer, _request: &Message) {
        debug!("BaseServer.on_client_accepted called");
    }

    pub fn on_data_yield(&self, _peer: &Peer, _data: &Message) {
        debug!("BaseServer.on_data_yield called");
    }
}