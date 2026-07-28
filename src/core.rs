use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tonic::{Request, Response, Status, Code};

#[derive(Debug, Clone)]
pub struct Client {
    pub id: String,
    pub tx: tokio::sync::mpsc::UnboundedSender<Message>,
}

#[derive(Debug, Clone)]
pub struct Message {
    pub message_name: String,
    pub sender_id: String,
    pub payload: Vec<u8>,
}

/// Routing structure: messageName -> clientId -> queue (indices into clients)
/// This matches the Python pattern in data_register.py
#[derive(Debug, Clone)]
pub struct RoutingTable {
    /// Maps message name to set of client IDs subscribed to it
    subscriptions: HashMap<String, HashSet<String>>,
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
    pub fn subscribe(&mut self, message_name: String, client_id: String) {
        self.subscriptions
            .entry(message_name)
            .or_insert_with(HashSet::new)
            .insert(client_id);
    }

    /// Unsubscribe all subscriptions for a specific client
    pub fn unsubscribe_client(&mut self, client_id: &str) {
        for (_msg_name, client_ids) in self.subscriptions.iter_mut() {
            client_ids.remove(client_id);
        }
    }

    /// Get clients subscribed to a message (for broadcast)
    pub fn get_subscribers(&self, message_name: &str) -> Option<&HashSet<String>> {
        self.subscriptions.get(message_name)
    }

    /// Remove only this client's subscriptions without affecting others
    pub fn remove_client_subscriptions(&mut self, client_id: &str) -> usize {
        let mut removed = 0;
        for (_msg_name, client_ids) in self.subscriptions.iter_mut() {
            if client_ids.remove(client_id) {
                removed += 1;
            }
        }
        removed
    }

    /// Check if a message name exists in routing table
    pub fn has_subscriptions(&self, message_name: &str) -> bool {
        self.subscriptions.contains_key(message_name)
    }

    /// Get total number of subscriptions across all messages
    pub fn subscription_count(&self) -> usize {
        self.subscriptions.values().map(|s| s.len()).sum()
    }
}

#[derive(Debug)]
pub struct CoreState {
    /// Server schema version for validation
    schema_version: String,
    /// Routing table: messageName -> clientId -> queue
    routing_table: Arc<RwLock<RoutingTable>>,
    /// Connected clients: clientId -> Client
    clients: Arc<RwLock<HashMap<String, Client>>>,
    /// Maximum allowed workers/connections
    max_workers: usize,
    /// Current connected client count (for enforcement)
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
    pub async fn check_schema(&self, client_schema_version: &str) -> Result<(), Status> {
        if client_schema_version != self.schema_version {
            return Err(Status::new(
                Code::FailedPrecondition,
                format!(
                    "Schema version mismatch: expected {}, got {}",
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

    /// Add a new connected client (increment count, add to clients map)
    pub async fn add_client(&self, client_id: String, tx: tokio::sync::mpsc::UnboundedSender<Message>) -> Result<(), Status> {
        let mut count = self.connection_count.lock().await;
        
        if *count >= self.max_workers {
            return Err(Status::new(
                Code::ResourceExhausted,
                format!(
                    "Max workers reached: {} / {}",
                    *count, self.max_workers
                ),
            ));
        }
        
        *count += 1;

        let mut clients = self.clients.write().await;
        clients.insert(client_id.clone(), Client { id: client_id, tx });
        Ok(())
    }

    /// Remove a disconnected client (decrement count, cleanup subscriptions)
    pub async fn remove_client(&self, client_id: &str) -> usize {
        let mut removed_subscriptions = 0;
        
        // First, increment connection count decrement
        let mut count = self.connection_count.lock().await;
        if *count > 0 {
            *count -= 1;
        }

        // Remove from clients map
        let mut clients = self.clients.write().await;
        if clients.remove(client_id).is_some() {
            tracing::info!("Removed client: {}", client_id);
        }

        // Clean up only this client's subscriptions (NOT all subscribers)
        let mut routing = self.routing_table.write().await;
        removed_subscriptions = routing.remove_client_subscriptions(client_id);
        
        tracing::debug!(
            "Client {} disconnected, removed {} subscription(s)",
            client_id,
            removed_subscriptions
        );

        removed_subscriptions
    }

    /// Subscribe a client to receive messages of a given message name
    pub async fn subscribe(&self, message_name: String, client_id: &str) -> Result<(), Status> {
        // Verify client exists before subscribing
        let clients = self.clients.read().await;
        if !clients.contains_key(client_id) {
            return Err(Status::new(
                Code::NotFound,
                format!("Client {} not found", client_id),
            ));
        }

        let mut routing = self.routing_table.write().await;
        tracing::debug!(
            "Subscribing client {} to message name {}",
            client_id,
            message_name
        );
        
        routing.subscribe(message_name, client_id.to_string());
        Ok(())
    }

    /// Get all clients subscribed to a specific message (for broadcast)
    pub async fn get_subscribers(&self, message_name: &str) -> Vec<String> {
        let routing = self.routing_table.read().await;
        if let Some(client_ids) = routing.get_subscribers(message_name) {
            client_ids.iter().cloned().collect()
        } else {
            vec![]
        }
    }

    /// Route a message to all subscribers (excluding sender if specified)
    pub async fn route_message(&self, msg: &Message, skip_sender: bool) -> Result<(), Status> {
        let subscribers = self.get_subscribers(&msg.message_name).await;
        
        if subscribers.is_empty() {
            tracing::debug!(
                "No subscribers for message {}: {}",
                msg.message_name,
                msg.sender_id
            );
            return Ok(());
        }

        // Get all clients once to avoid multiple locks
        let clients = self.clients.read().await;
        
        for client_id in &subscribers {
            // Skip the sender of this message (broadcast behavior)
            if skip_sender && client_id == &msg.sender_id {
                tracing::debug!("Skipping sender {} from broadcast", msg.sender_id);
                continue;
            }

            if let Some(client) = clients.get(client_id) {
                match client.tx.send(msg.clone()) {
                    Ok(_) => {
                        tracing::trace!(
                            "Broadcast message to {}: {}",
                            client_id,
                            msg.message_name
                        );
                    }
                    Err(e) => {
                        tracing::error!("Failed to send message to {}: {}", client_id, e);
                        // Client may have disconnected; we'll handle cleanup on next receive error
                    }
                }
            } else {
                tracing::warn!(
                    "Subscriber {} not found in clients map for message {}",
                    client_id,
                    msg.message_name
                );
            }
        }

        Ok(())
    }

    /// Send a unicast message to a specific target client
    pub async fn send_to_client(&self, target_id: &str, msg: Message) -> Result<(), Status> {
        let clients = self.clients.read().await;
        
        if let Some(client) = clients.get(target_id) {
            match client.tx.send(msg) {
                Ok(_) => Ok(()),
                Err(e) => Err(Status::internal(format!(
                    "Failed to send message to {}: {}",
                    target_id, e
                ))),
            }
        } else {
            Err(Status::new(
                Code::NotFound,
                format!("Target client {} not found", target_id),
            ))
        }
    }

    /// Get current connected client count
    pub async fn get_connection_count(&self) -> usize {
        *self.connection_count.lock().await
    }

    /// Get max workers limit
    pub fn get_max_workers(&self) -> usize {
        self.max_workers
    }

    /// Verify all clients are properly accounted for (debug/health check)
    pub async fn verify_client_integrity(&self) -> bool {
        let count = self.connection_count.lock().await;
        let clients = self.clients.read().await;
        
        if *count != clients.len() {
            tracing::error!(
                "Client integrity check failed: count={} actual_clients={}",
                *count,
                clients.len()
            );
            return false;
        }
        
        true
    }

    /// Get all active client IDs (for health checks)
    pub async fn get_all_client_ids(&self) -> Vec<String> {
        let clients = self.clients.read().await;
        clients.keys().cloned().collect()
    }
}
