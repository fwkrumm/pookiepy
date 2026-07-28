//! Core state and routing logic for grpchook-server
//! 
//! This module implements the message routing table semantics matching Python's DataRegister,
//! including fan-out, self-skip, optional unicast, and proper disconnect cleanup.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tonic::{Status, Code};

/// Client connection state - tracks the bidirectional stream for each connected client
#[derive(Debug, Clone)]
pub struct Client {
    pub id: String,
    /// Channel for sending messages TO this client (server -> client)
    pub tx: tokio::sync::mpsc::UnboundedSender<ServerMessage>,
}

/// Message type sent from server to client over the streaming connection
#[derive(Debug, Clone)]
pub struct ServerMessage {
    pub message_name: String,
    pub sender_id: String,
    pub payload: Vec<u8>,
}

/// Per-subscription state - maps message_name -> client_id -> notification queue
/// This matches Python's DataRegister._register structure
#[derive(Debug)]
pub struct SubscriptionState {
    /// Set of client IDs subscribed to this message name (for fan-out)
    subscribers: HashSet<String>,
    /// Channels for sending messages to each subscriber
    channels: HashMap<String, tokio::sync::mpsc::UnboundedSender<ServerMessage>>,
}

impl Default for SubscriptionState {
    fn default() -> Self {
        Self::new()
    }
}

impl SubscriptionState {
    pub fn new() -> Self {
        Self {
            subscribers: HashSet::new(),
            channels: HashMap::new(),
        }
    }
}

/// Routing table: message_name -> subscription state per client
#[derive(Debug)]
pub struct RoutingTable {
    /// Maps message name to subscription state
    subscriptions: HashMap<String, SubscriptionState>,
}

impl Default for RoutingTable {
    fn default() -> Self {
        Self::new()
    }
}

impl RoutingTable {
    pub fn new() -> Self {
        Self {
            subscriptions: HashMap::new(),
        }
    }

    /// Subscribe a client to receive messages of a given message name
    /// Returns the set of other subscribers (excluding sender) for fan-out validation
    pub fn subscribe(&mut self, message_name: String, client_id: String) -> HashSet<String> {
        let state = self.subscriptions.entry(message_name.clone()).or_insert_with(SubscriptionState::new);
        
        // Track other subscribers BEFORE adding this one (for fan-out logic matching Python)
        let mut others = state.subscribers.clone();
        others.remove(&client_id);
        
        state.subscribers.insert(client_id.clone());
        tracing::debug!("Subscribed client {} to message {}", client_id, message_name);
        
        others
    }

    /// Get other subscribers (excluding the given client) for fan-out broadcast
    pub fn get_other_subscribers(&self, message_name: &str, exclude_client: &str) -> Vec<String> {
        if let Some(state) = self.subscriptions.get(message_name) {
            state.subscribers.iter()
                .filter(|id| id != &exclude_client.to_string())
                .cloned()
                .collect()
        } else {
            vec![]
        }
    }

    /// Get all subscribers (including sender if present) for optional unicast validation
    pub fn get_all_subscribers(&self, message_name: &str) -> Vec<String> {
        if let Some(state) = self.subscriptions.get(message_name) {
            state.subscribers.iter().cloned().collect()
        } else {
            vec![]
        }
    }

    /// Unsubscribe all subscriptions for a specific client on disconnect
    pub fn unsubscribe_client(&mut self, client_id: &str) -> usize {
        let mut removed = 0;
        for (_msg_name, state) in self.subscriptions.iter_mut() {
            if state.subscribers.remove(client_id) {
                // Also remove the channel to prevent memory leaks
                state.channels.remove(client_id);
                removed += 1;
            }
        }
        tracing::debug!("Unsubscribed client {} from {} subscription(s)", client_id, removed);
        removed
    }

    /// Check if a message name has any subscribers
    pub fn has_subscribers(&self, message_name: &str) -> bool {
        self.subscriptions.get(message_name).map_or(false, |s| !s.subscribers.is_empty())
    }

    /// Register a channel for a subscriber (used during first-message handshake after subscription)
    pub fn register_channel(&mut self, message_name: &str, client_id: &str, tx: tokio::sync::mpsc::UnboundedSender<ServerMessage>) {
        if let Some(state) = self.subscriptions.get_mut(message_name) {
            state.channels.insert(client_id.to_string(), tx);
        }
    }

    /// Get total number of subscriptions across all messages
    pub fn subscription_count(&self) -> usize {
        self.subscriptions.values().map(|s| s.subscribers.len()).sum()
    }
}

/// Core server state - shared across all connections and RPC calls
#[derive(Debug)]
pub struct CoreState {
    /// Server schema version for validation against client metadata
    schema_version: String,
    /// Routing table: message_name -> subscription state per client
    routing_table: Arc<RwLock<RoutingTable>>,
    /// Connected clients: client_id -> Client (with channel for sending to them)
    clients: Arc<RwLock<HashMap<String, Client>>>,
    /// Maximum allowed connections/workers
    max_workers: usize,
    /// Current connection count (for enforcement)
    connection_count: Arc<Mutex<usize>>,
}

impl CoreState {
    pub fn new(schema_version: String, max_workers: usize) -> Self {
        Self {
            schema_version,
            routing_table: Arc::new(RwLock::new(RoutingTable::new())),
            clients: Arc::new(RwLock::new(HashMap::new())),
            max_workers,
            connection_count: Arc::new(Mutex::new(0)),
        }
    }

    /// Validate schema version from gRPC metadata
    /// Returns FAILED_PRECONDITION on mismatch - matches Python behavior exactly
    pub async fn check_schema(&self, client_schema_version: &str) -> Result<(), Status> {
        if client_schema_version != self.schema_version {
            return Err(Status::new(
                Code::FailedPrecondition,
                format!(
                    "Proto schema mismatch: server={}, client={}",
                    self.schema_version, client_schema_version
                ),
            ));
        }
        Ok(())
    }

    /// Get current schema version (for welcome message)
    pub fn get_schema_version(&self) -> String {
        self.schema_version.clone()
    }

    /// Add a new connected client to the server state
    /// Called after successful connection and schema validation
    pub async fn add_client(&self, client_id: String, tx: tokio::sync::mpsc::UnboundedSender<ServerMessage>) -> Result<(), Status> {
        let mut count = self.connection_count.lock().await;
        
        if *count >= self.max_workers {
            return Err(Status::new(
                Code::ResourceExhausted,
                format!("Max workers reached: {} / {}", *count, self.max_workers),
            ));
        }
        
        *count += 1;

        let mut clients = self.clients.write().await;
        clients.insert(client_id.clone(), Client { id: client_id.clone(), tx });
        
        tracing::info!("Client connected: {} (now {} total)", client_id, *count);
        Ok(())
    }

    /// Remove a disconnected client - cleanup subscriptions and decrement count
    pub async fn remove_client(&self, client_id: &str) -> usize {
        let mut removed_subscriptions = 0;
        
        // Decrement connection count
        let mut count = self.connection_count.lock().await;
        if *count > 0 {
            *count -= 1;
        }

        // Remove from clients map
        let mut clients = self.clients.write().await;
        if clients.remove(client_id).is_some() {
            tracing::info!("Removed client: {}", client_id);
        }

        // Clean up all subscriptions for this client (matching Python DataRegister behavior)
        let mut routing = self.routing_table.write().await;
        removed_subscriptions = routing.unsubscribe_client(client_id);
        
        tracing::debug!("Client {} disconnected, cleaned up {} subscription(s)", client_id, removed_subscriptions);

        removed_subscriptions
    }

    /// Subscribe a client to receive messages of a given message name
    pub async fn subscribe(&self, message_name: String, client_id: &str) -> Result<(), Status> {
        // Verify client exists before subscribing (Python requires this too)
        let clients = self.clients.read().await;
        if !clients.contains_key(client_id) {
            return Err(Status::new(
                Code::NotFound,
                format!("Client {} not found", client_id),
            ));
        }

        let mut routing = self.routing_table.write().await;
        tracing::debug!("Subscribing client {} to message {}", client_id, message_name);
        
        // Subscribe and get other subscribers for fan-out tracking
        let _others = routing.subscribe(message_name.clone(), client_id.to_string());
        
        Ok(())
    }

    /// Route a broadcast message to all subscribers (excluding sender)
    /// Implements Python's self-skip behavior exactly
    pub async fn route_broadcast(&self, msg: &ServerMessage, skip_sender: bool) -> Result<(), Status> {
        let mut routing = self.routing_table.read().await;
        
        // Get all other subscribers for this message name (excluding sender if requested)
        let recipients = if skip_sender {
            routing.get_other_subscribers(&msg.message_name, &msg.sender_id)
        } else {
            routing.get_all_subscribers(&msg.message_name)
        };

        drop(routing); // Release lock before sending to avoid deadlock

        if recipients.is_empty() {
            tracing::trace!("No subscribers for message {}", msg.message_name);
            return Ok(());
        }

        // Get clients map once and send to all recipients
        let clients = self.clients.read().await;
        
        for recipient_id in &recipients {
            if let Some(client) = clients.get(recipient_id) {
                match client.tx.send(ServerMessage {
                    message_name: msg.message_name.clone(),
                    sender_id: msg.sender_id.clone(),
                    payload: msg.payload.clone(),
                }) {
                    Ok(_) => {
                        tracing::trace!("Broadcast {} to {}", recipient_id, msg.message_name);
                    }
                    Err(e) => {
                        tracing::error!("Failed to broadcast to {}: {}", recipient_id, e);
                        // Client may have disconnected; cleanup will happen on next receive error
                    }
                }
            } else {
                tracing::warn!(
                    "Subscriber {} not found for message {}, skipping",
                    recipient_id, msg.message_name
                );
            }
        }

        Ok(())
    }

    /// Send a unicast message to a specific target client (optional destination)
    pub async fn send_unicast(&self, target_id: &str, msg: ServerMessage) -> Result<(), Status> {
        let clients = self.clients.read().await;
        
        if let Some(client) = clients.get(target_id) {
            match client.tx.send(msg) {
                Ok(_) => Ok(()),
                Err(e) => Err(Status::internal(format!(
                    "Failed to send unicast to {}: {}", target_id, e
                ))),
            }
        } else {
            Err(Status::new(
                Code::NotFound,
                format!("Target client {} not found", target_id),
            ))
        }
    }

    /// Get current connected client count for status reporting
    pub async fn get_connection_count(&self) -> usize {
        *self.connection_count.lock().await
    }

    /// Get max workers limit
    pub fn get_max_workers(&self) -> usize {
        self.max_workers
    }

    /// Verify client integrity (debug/health check - count should match actual clients)
    pub async fn verify_client_integrity(&self) -> bool {
        let count = self.connection_count.lock().await;
        let clients = self.clients.read().await;
        
        if *count != clients.len() {
            tracing::error!(
                "Client integrity check failed: tracked={}, actual={}",
                *count,
                clients.len(),
            );
            return false;
        }
        
        true
    }

    /// Get all active client IDs (for health checks and diagnostics)
    pub async fn get_all_client_ids(&self) -> Vec<String> {
        let clients = self.clients.read().await;
        clients.keys().cloned().collect()
    }

    /// Register a channel for a specific subscription (used during first-message handshake)
    pub async fn register_subscription_channel(
        &self,
        message_name: &str,
        client_id: &str,
        tx: tokio::sync::mpsc::UnboundedSender<ServerMessage>,
    ) {
        let mut routing = self.routing_table.write().await;
        routing.register_channel(message_name, client_id, tx);
    }

    /// Check if a message has any subscribers (for optional unicast validation)
    pub async fn has_subscribers(&self, message_name: &str) -> bool {
        let routing = self.routing_table.read().await;
        routing.has_subscribers(message_name)
    }
}
