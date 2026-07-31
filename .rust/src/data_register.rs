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

#[cfg(test)]
mod tests {
    use tokio::time::{timeout, Duration};

    use super::{notification_channel, DataRegister};
    use crate::pb::{Message, MetaInformation};

    fn test_message(message_name: &str) -> Message {
        Message {
            meta_info: Some(MetaInformation {
                message_name: message_name.to_owned(),
                ..MetaInformation::default()
            }),
            ..Message::default()
        }
    }

    #[tokio::test]
    async fn unbounded_notification_channel_transfers_message() {
        let (tx, mut rx) = notification_channel(0);
        let message = test_message("alpha");

        tx.send(message.clone()).await.expect("send should succeed");

        let received = timeout(Duration::from_millis(50), rx.recv())
            .await
            .expect("receive should not time out")
            .expect("message should exist");

        assert_eq!(received.meta_info, message.meta_info);
    }

    #[tokio::test]
    async fn fanout_skips_sender_and_delivers_to_other_subscribers() {
        let register = DataRegister::new();
        let (sender_tx, mut sender_rx) = notification_channel(1);
        let (receiver_tx, mut receiver_rx) = notification_channel(1);

        register
            .add_notification_queue_for_message_name(
                "sender".to_owned(),
                "alpha".to_owned(),
                sender_tx,
            )
            .await;
        register
            .add_notification_queue_for_message_name(
                "receiver".to_owned(),
                "alpha".to_owned(),
                receiver_tx,
            )
            .await;

        register
            .add_data_for_message_name("sender", "alpha", test_message("alpha"), None)
            .await;

        let received = timeout(Duration::from_millis(50), receiver_rx.recv())
            .await
            .expect("receiver should not time out")
            .expect("receiver should get data");
        assert_eq!(
            received
                .meta_info
                .as_ref()
                .expect("meta info should exist")
                .message_name,
            "alpha"
        );

        let sender_result = timeout(Duration::from_millis(20), sender_rx.recv()).await;
        assert!(
            sender_result.is_err(),
            "sender should not receive its own data"
        );
    }

    #[tokio::test]
    async fn target_client_id_limits_delivery() {
        let register = DataRegister::new();
        let (first_tx, mut first_rx) = notification_channel(1);
        let (second_tx, mut second_rx) = notification_channel(1);

        register
            .add_notification_queue_for_message_name(
                "first".to_owned(),
                "alpha".to_owned(),
                first_tx,
            )
            .await;
        register
            .add_notification_queue_for_message_name(
                "second".to_owned(),
                "alpha".to_owned(),
                second_tx,
            )
            .await;

        register
            .add_data_for_message_name("sender", "alpha", test_message("alpha"), Some("second"))
            .await;

        let sender_result = timeout(Duration::from_millis(20), first_rx.recv()).await;
        assert!(
            sender_result.is_err(),
            "non-target client should not receive data"
        );

        let received = timeout(Duration::from_millis(50), second_rx.recv())
            .await
            .expect("target should not time out")
            .expect("target should get data");
        assert_eq!(
            received
                .meta_info
                .as_ref()
                .expect("meta info should exist")
                .message_name,
            "alpha"
        );
    }

    #[tokio::test]
    async fn removing_client_unregisters_all_routes() {
        let register = DataRegister::new();
        let (tx, mut rx) = notification_channel(1);

        register
            .add_notification_queue_for_message_name(
                "client-1".to_owned(),
                "alpha".to_owned(),
                tx.clone(),
            )
            .await;
        register
            .add_notification_queue_for_message_name("client-1".to_owned(), "beta".to_owned(), tx)
            .await;

        register
            .remove_notification_queues_for_client("client-1")
            .await;

        register
            .add_data_for_message_name("sender", "alpha", test_message("alpha"), None)
            .await;
        register
            .add_data_for_message_name("sender", "beta", test_message("beta"), None)
            .await;

        let receive_result = timeout(Duration::from_millis(20), rx.recv()).await;
        assert!(
            receive_result.is_err(),
            "removed client should not receive data on any route"
        );
    }
}
