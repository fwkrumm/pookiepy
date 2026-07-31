//! gRPC service implementation for grpchook server.

use std::pin::Pin;
use std::sync::Arc;

use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status};

use crate::core::{CoreState, ServerMessage};

pub mod proto {
    tonic::include_proto!("grpchook");
}

pub use proto::grpchook_service_client::GrpchookServiceClient as GrpchookClient;
pub use proto::grpchook_service_server::GrpchookServiceServer;
pub use proto::{
    DisconnectRequest, DisconnectResponse, Message, SendMessageRequest, SendMessageResponse,
    StatusRequest, StatusResponse, SubscribeRequest, SubscribeResponse, UnicastRequest,
    UnicastResponse,
};

#[derive(Clone)]
pub struct GrpchookService {
    core: Arc<CoreState>,
}

impl GrpchookService {
    pub fn new(core: Arc<CoreState>) -> Self {
        Self { core }
    }

    fn schema_from_metadata(&self, metadata: &tonic::metadata::MetadataMap) -> String {
        metadata
            .get("x-schema-version")
            .and_then(|v| v.to_str().ok())
            .map(|v| v.to_string())
            .unwrap_or_else(|| self.core.get_schema_version())
    }
}

#[tonic::async_trait]
impl proto::grpchook_service_server::GrpchookService for GrpchookService {
    type HandleStreamStream = Pin<Box<dyn Stream<Item = Result<Message, Status>> + Send + 'static>>;

    async fn handle_stream(
        &self,
        request: Request<tonic::Streaming<Message>>,
    ) -> Result<Response<Self::HandleStreamStream>, Status> {
        let schema = self.schema_from_metadata(request.metadata());
        self.core.check_schema(&schema).await?;

        let session_id = uuid::Uuid::new_v4().to_string();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<ServerMessage>();
        self.core.add_client(session_id.clone(), tx).await?;

        let core = self.core.clone();
        let mut inbound = request.into_inner();
        let cleanup_id = session_id.clone();

        tokio::spawn(async move {
            while let Some(item) = inbound.next().await {
                match item {
                    Ok(msg) => {
                        let routed = ServerMessage {
                            message_name: msg.message_name,
                            sender_id: if msg.sender_id.is_empty() {
                                cleanup_id.clone()
                            } else {
                                msg.sender_id
                            },
                            payload: msg.payload,
                        };
                        if let Err(e) = core.route_broadcast(&routed, true).await {
                            tracing::warn!("broadcast failed: {}", e);
                        }
                    }
                    Err(e) => {
                        tracing::warn!("stream recv error: {}", e);
                        break;
                    }
                }
            }
            core.remove_client(&cleanup_id).await;
        });

        let outbound = UnboundedReceiverStream::new(rx).map(|msg| {
            Ok(Message {
                message_name: msg.message_name,
                sender_id: msg.sender_id,
                payload: msg.payload,
            })
        });

        Ok(Response::new(Box::pin(outbound)))
    }

    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<SubscribeResponse>, Status> {
        let schema = self.schema_from_metadata(request.metadata());
        self.core.check_schema(&schema).await?;

        let req = request.into_inner();
        if req.client_id.is_empty() {
            return Ok(Response::new(SubscribeResponse {
                success: false,
                message: "client_id required".to_string(),
            }));
        }

        match self.core.subscribe(req.message_name.clone(), &req.client_id).await {
            Ok(()) => Ok(Response::new(SubscribeResponse {
                success: true,
                message: format!("Subscribed to {}", req.message_name),
            })),
            Err(e) => Ok(Response::new(SubscribeResponse {
                success: false,
                message: e.message().to_string(),
            })),
        }
    }

    async fn send_message(
        &self,
        request: Request<SendMessageRequest>,
    ) -> Result<Response<SendMessageResponse>, Status> {
        let schema = self.schema_from_metadata(request.metadata());
        self.core.check_schema(&schema).await?;

        let req = request.into_inner();
        let msg = ServerMessage {
            message_name: req.message_name.clone(),
            sender_id: uuid::Uuid::new_v4().to_string(),
            payload: req.payload,
        };

        match self.core.route_broadcast(&msg, true).await {
            Ok(()) => Ok(Response::new(SendMessageResponse {
                success: true,
                message: "ok".to_string(),
            })),
            Err(e) => Ok(Response::new(SendMessageResponse {
                success: false,
                message: e.message().to_string(),
            })),
        }
    }

    async fn send_unicast(
        &self,
        request: Request<UnicastRequest>,
    ) -> Result<Response<UnicastResponse>, Status> {
        let schema = self.schema_from_metadata(request.metadata());
        self.core.check_schema(&schema).await?;

        let req = request.into_inner();
        let msg = ServerMessage {
            message_name: "unicast".to_string(),
            sender_id: uuid::Uuid::new_v4().to_string(),
            payload: req.payload,
        };

        match self.core.send_unicast(&req.target_client_id, msg).await {
            Ok(()) => Ok(Response::new(UnicastResponse {
                success: true,
                message: "ok".to_string(),
            })),
            Err(e) => Ok(Response::new(UnicastResponse {
                success: false,
                message: e.message().to_string(),
            })),
        }
    }

    async fn get_status(
        &self,
        request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        let schema = self.schema_from_metadata(request.metadata());
        self.core.check_schema(&schema).await?;

        let connected_clients = self.core.get_connection_count().await as i32;
        let max_workers = self.core.get_max_workers() as i32;
        let schema_version = self.core.get_schema_version();

        Ok(Response::new(StatusResponse {
            connected_clients,
            max_workers,
            schema_version,
        }))
    }

    async fn disconnect(
        &self,
        request: Request<DisconnectRequest>,
    ) -> Result<Response<DisconnectResponse>, Status> {
        let schema = self.schema_from_metadata(request.metadata());
        self.core.check_schema(&schema).await?;

        let req = request.into_inner();
        if req.client_id.is_empty() {
            return Ok(Response::new(DisconnectResponse {
                success: false,
                message: "client_id required".to_string(),
            }));
        }

        self.core.remove_client(&req.client_id).await;
        Ok(Response::new(DisconnectResponse {
            success: true,
            message: "disconnected".to_string(),
        }))
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
