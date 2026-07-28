use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tonic::{Request, Response, Status, Code};
use tokio_stream::wrappers::ReceiverStream;

use crate::core::Core;
use crate::message::{Message, RegisterRequest, WelcomeRequest};

pub struct GrpcServer {
    core: Arc<Core>,
}

impl GrpcServer {
    pub fn new(core: Arc<Core>) -> Self {
        Self { core }
    }
}

#[tonic::async_trait]
impl message::stream_service_server::StreamService for GrpcServer {
    type StreamStream = ReceiverStream<Result<message::Message, Status>>;

    async fn welcome(
        &self,
        request: Request<WelcomeRequest>,
    ) -> Result<Response<Self::StreamStream>, Status> {
        let client_schema_version = request.metadata().get("schema-version").and_then(|v| v.to_str().ok()).unwrap_or("");
        self.core.check_schema(client_schema_version).await?;

        let client_id = request.get_ref().client_id.clone();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        self.core.add_client(client_id.clone(), tx).await?;

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn register(
        &self,
        request: Request<RegisterRequest>,
    ) -> Result<Response<()>, Status> {
        let req = request.get_ref();
        let client_id = &req.client_id;
        for message_name in &req.requires {
            self.core.subscribe(message_name.clone(), client_id.clone()).await;
        }
        Ok(Response::new(()))
    }

    async fn send_message(
        &self,
        request: Request<Message>,
    ) -> Result<Response<()>, Status> {
        let msg = request.get_ref();
        let sender_id = &msg.sender_id;
        self.core.route_message(msg, Some(sender_id)).await?;
        Ok(Response::new(()))
    }
}