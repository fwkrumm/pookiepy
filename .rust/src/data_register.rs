use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{mpsc, RwLock};

use crate::pb::Message;

/// Sender abstraction that supports bounded and unbounded channels.
#[derive(Clone)]
pub enum NotificationTx {
    Bounded(mpsc::Sender<Message>),
    Unbounded(mpsc::UnboundedSender<Message>),
}

/// Receiver abstraction that supports bounded and unbounded channels.
pub enum NotificationRx {
    Bounded(mpsc::Receiver<Message>),
    Unbounded(mpsc::UnboundedReceiver<Message>),
}

impl NotificationTx {
    /// Send one message to client notification queue.
    pub async fn send(&self, data: Message) -> Result<(), ()> {
        match self {
            NotificationTx::Bounded(tx) => tx.send(data).await.map_err(|_| ()),
            NotificationTx::Unbounded(tx) => tx.send(data).map_err(|_| ()),
        }
    }
}

impl NotificationRx {
    /// Receive one message from client notification queue.
    pub async fn recv(&mut self) -> Option<Message> {
        match self {
            NotificationRx::Bounded(rx) => rx.recv().await,
            NotificationRx::Unbounded(rx) => rx.recv().await,
        }
    }
}

/// Build notification channel pair.
///
/// When max_queue_elements is 0, channel is unbounded.
pub fn notification_channel(max_queue_elements: usize) -> (NotificationTx, NotificationRx) {
    if max_queue_elements == 0 {
        let (tx, rx) = mpsc::unbounded_channel();
        return (NotificationTx::Unbounded(tx), NotificationRx::Unbounded(rx));
    }

    let (tx, rx) = mpsc::channel(max_queue_elements);
    (NotificationTx::Bounded(tx), NotificationRx::Bounded(rx))
}

/// Server-side routing table: message name to client queue map.
#[derive(Default)]
pub struct DataRegister {
    routes: RwLock<HashMap<String, HashMap<String, NotificationTx>>>,
}

impl DataRegister {
    /// Create empty routing table.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register notification queue for one message name.
    pub async fn add_notification_queue_for_message_name(
        &self,
        client_id: String,
        message_name: String,
        queue: NotificationTx,
    ) {
        let mut routes = self.routes.write().await;
        routes
            .entry(message_name)
            .or_default()
            .insert(client_id, queue);
    }

    /// Remove all queues for one client id.
    pub async fn remove_notification_queues_for_client(&self, client_id: &str) {
        let mut routes = self.routes.write().await;
        routes.retain(|_, clients| {
            clients.remove(client_id);
            !clients.is_empty()
        });
    }

    /// Fan-out data by message name.
    ///
    /// Sender client is skipped. When target_client_id is set, only that client receives data.
    pub async fn add_data_for_message_name(
        &self,
        sender_client_id: &str,
        message_name: &str,
        data: Message,
        target_client_id: Option<&str>,
    ) {
        let recipients: Vec<(String, NotificationTx)> = {
            let routes = self.routes.read().await;
            let Some(clients) = routes.get(message_name) else {
                return;
            };

            clients
                .iter()
                .filter(|(client_id, _)| {
                    if client_id.as_str() == sender_client_id {
                        return false;
                    }
                    if let Some(target) = target_client_id {
                        return client_id.as_str() == target;
                    }
                    true
                })
                .map(|(client_id, tx)| (client_id.clone(), tx.clone()))
                .collect()
        };

        let mut disconnected_clients = Vec::new();
        for (client_id, tx) in recipients {
            if tx.send(data.clone()).await.is_err() {
                disconnected_clients.push(client_id);
            }
        }

        if disconnected_clients.is_empty() {
            return;
        }

        let mut routes = self.routes.write().await;
        if let Some(clients) = routes.get_mut(message_name) {
            for client_id in disconnected_clients {
                clients.remove(&client_id);
            }
        }
    }
}

/// Shared router type.
pub type SharedDataRegister = Arc<DataRegister>;
