use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tokio_stream::wrappers::ReceiverStream;
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

#[derive(Debug)]
pub struct Core {
    schema_version: String,
    data_register: Arc<RwLock<HashMap<String, Vec<String>>>>,
    clients: Arc<RwLock<HashMap<String, Client>>>,
    max_workers: usize,
    connection_count: Arc<Mutex<usize>>,
}

impl Core {
    pub fn new(schema_version: String, max_workers: usize) -> Self {
        Self {
            schema_version,
            data_register: Arc::new(RwLock::new(HashMap::new())),
            clients: Arc::new(RwLock::new(HashMap::new())),
            max_workers,
            connection_count: Arc::new(Mutex::new(0)),
        }
    }

    pub async fn check_schema(&self, client_schema_version: &str) -> Result<(), Status> {
        if client_schema_version != self.schema_version {
            return Err(Status::new(
                Code::FailedPrecondition,
                "Schema version mismatch".to_string(),
            ));
        }
        Ok(())
    }

    pub async fn add_client(&self, client_id: String, tx: tokio::sync::mpsc::UnboundedSender<Message>) -> Result<(), Status> {
        let mut count = self.connection_count.lock().await;
        if *count >= self.max_workers {
            return Err(Status::new(
                Code::PermissionDenied,
                "Max workers reached".to_string(),
            ));
        }
        *count += 1;

        let mut clients = self.clients.write().await;
        clients.insert(client_id.clone(), Client { id: client_id, tx });
        Ok(())
    }

    pub async fn remove_client(&self, client_id: &str) {
        let mut clients = self.clients.write().await;
        clients.remove(client_id);

        let mut register = self.data_register.write().await;
        for (_msg_name, client_ids) in register.iter_mut() {
            client.client_ids.retain(|id| id != client_id);
        }

        let mut count = self.connection_count.lock().await;
        *count -= 1;
    }

    pub async fn route_message(&self, msg: &Message, sender_id: Option<&str>) -> Result<(), Status> {
        let register = self.data_register.read().await;
        if let Some(client_ids) = register.get(&msg.message_name) {
            let clients = self.clients.read().await;
            for client_id in client_ids {
                if let Some(sender) = sender_id {
                    if client_id == sender {
                        continue;
                    }
                }
                if let Some(client) = clients.get(client_id) {
                    client.tx.send(msg.clone()).map_err(|e| Status::internal(e.to_string()))?;
                }
            }
        }
        Ok(())
    }

    pub async fn subscribe(&self, message_name: String, client_id: String) {
        let mut register = self.data_register.write().await;
        register.entry(message_name).or_insert_with(Vec::new).push(client_id);
    }
}