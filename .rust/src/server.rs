use std::net::{AddrParseError, SocketAddr};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime};

use async_trait::async_trait;
use thiserror::Error;
use tokio::sync::{mpsc, Notify};
use tokio_stream::wrappers::ReceiverStream;
use tonic::codec::CompressionEncoding;
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::data_register::{notification_channel, DataRegister, NotificationRx};
use crate::pb::stream_server::{Stream, StreamServer};
use crate::pb::{DataPoint, Message, MetaInformation, ServerProvides};
use crate::schema_version::{schema_version, SCHEMA_VERSION_METADATA_KEY};

static PERF_COUNTER_START: OnceLock<Instant> = OnceLock::new();

/// Runtime config for Rust BaseServer.
#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub max_queue_elements: usize,
    pub response_stream_buffer: usize,
    pub shutdown_poll_interval: Duration,
    pub strict_schema_version: bool,
    pub warning_client_threshold: Option<usize>,
    pub accept_gzip: bool,
    pub send_gzip: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            max_queue_elements: 0,
            response_stream_buffer: 64,
            shutdown_poll_interval: Duration::from_millis(100),
            strict_schema_version: false,
            warning_client_threshold: Some(32),
            accept_gzip: true,
            send_gzip: true,
        }
    }
}

/// Per-client connection information.
#[derive(Clone, Debug)]
pub struct Peer {
    pub peer: String,
    pub session_id: String,
    pub client_id: String,
    pub name: String,
}

impl Peer {
    fn new(peer: String) -> Self {
        Self {
            peer,
            session_id: Uuid::new_v4().to_string(),
            client_id: String::new(),
            name: String::new(),
        }
    }
}

/// Hook trait for application behavior.
#[async_trait]
pub trait ServerHooks: Send + Sync + 'static {
    /// Called when server starts.
    async fn on_init(&self) {}

    /// Called when server stops.
    async fn on_shutdown(&self) {}

    /// Called before first client message is accepted.
    async fn on_client_connect(
        &self,
        _request: &Message,
        _metadata: &MetadataMap,
        _peer: &Peer,
    ) -> bool {
        true
    }

    /// Called after client accepted and registered.
    async fn on_client_accepted(&self, _peer: &Peer, _request: &Message) {}

    /// Called after full stream disconnect cleanup.
    async fn on_client_disconnect(&self, _peer: &Peer) {}

    /// Called on every incoming message after connect.
    async fn on_receive(&self, _peer: &Peer, _request: &Message) -> bool {
        true
    }

    /// Called right before message is yielded to tonic stream.
    async fn on_data_yield(&self, _peer: &Peer, _data: &mut Message) {}
}

/// Default no-op hooks.
#[derive(Default)]
pub struct DefaultHooks;

#[async_trait]
impl ServerHooks for DefaultHooks {}

/// Build errors for BaseServer.
#[derive(Debug, Error)]
pub enum ServerBuildError {
    #[error("invalid listen address: {0}")]
    InvalidAddress(#[from] AddrParseError),
}

/// Runtime errors for serve_forever.
#[derive(Debug, Error)]
pub enum ServerRunError {
    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
}

/// Async Rust port of pookiepy BaseServer.
pub struct BaseServer<H: ServerHooks> {
    name: String,
    uid: String,
    addr: SocketAddr,
    config: ServerConfig,
    hooks: Arc<H>,
    data_register: Arc<DataRegister>,
    global_exit_event: Arc<AtomicBool>,
    global_exit_notify: Arc<Notify>,
    connected_clients: Arc<AtomicUsize>,
}

impl<H: ServerHooks> Clone for BaseServer<H> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            uid: self.uid.clone(),
            addr: self.addr,
            config: self.config.clone(),
            hooks: Arc::clone(&self.hooks),
            data_register: Arc::clone(&self.data_register),
            global_exit_event: Arc::clone(&self.global_exit_event),
            global_exit_notify: Arc::clone(&self.global_exit_notify),
            connected_clients: Arc::clone(&self.connected_clients),
        }
    }
}

impl<H: ServerHooks> BaseServer<H> {
    /// Create server with hook implementation and config.
    pub fn new(
        addr: impl AsRef<str>,
        name: impl Into<String>,
        hooks: H,
        config: ServerConfig,
    ) -> Result<Self, ServerBuildError> {
        let addr: SocketAddr = addr.as_ref().parse()?;
        Ok(Self {
            name: name.into(),
            uid: Uuid::new_v4().to_string(),
            addr,
            config,
            hooks: Arc::new(hooks),
            data_register: Arc::new(DataRegister::new()),
            global_exit_event: Arc::new(AtomicBool::new(false)),
            global_exit_notify: Arc::new(Notify::new()),
            connected_clients: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Trigger graceful shutdown.
    pub fn shutdown(&self) {
        let was_set = self.global_exit_event.swap(true, Ordering::SeqCst);
        if !was_set {
            self.global_exit_notify.notify_waiters();
        }
    }

    /// Read-only shutdown state.
    pub fn is_shutdown(&self) -> bool {
        self.global_exit_event.load(Ordering::SeqCst)
    }

    /// Start server and wait until shutdown event is set.
    pub async fn serve_forever(self) -> Result<(), ServerRunError> {
        self.hooks.on_init().await;

        info!(
            "server {} started at {} (schema={})",
            self.name,
            self.addr,
            schema_version()
        );

        let mut service = StreamServer::new(self.clone());
        if self.config.accept_gzip {
            service = service.accept_compressed(CompressionEncoding::Gzip);
        }
        if self.config.send_gzip {
            service = service.send_compressed(CompressionEncoding::Gzip);
        }

        let shutdown_owner = self.clone();
        let shutdown_poll_interval = self.config.shutdown_poll_interval;

        let serve_result = tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_shutdown(self.addr, async move {
                loop {
                    if shutdown_owner.is_shutdown() {
                        break;
                    }
                    tokio::time::sleep(shutdown_poll_interval).await;
                }
            })
            .await;

        self.hooks.on_shutdown().await;

        serve_result?;
        info!("server {} stopped", self.name);
        Ok(())
    }

    fn handle_schema(&self, metadata: &MetadataMap, peer: &Peer) -> Option<Status> {
        let client_schema_value = metadata.get(SCHEMA_VERSION_METADATA_KEY)?;

        let Ok(client_schema) = client_schema_value.to_str() else {
            return None;
        };

        let server_schema = schema_version();
        if client_schema == server_schema {
            return None;
        }

        if self.config.strict_schema_version {
            return Some(Status::failed_precondition(format!(
                "Proto schema mismatch: server={server_schema}, client={client_schema}"
            )));
        }

        warn!(
            "{}: schema mismatch tolerated strict_schema_version=false server={} client={}",
            peer.peer, server_schema, client_schema
        );
        None
    }

    fn spawn_outgoing_task(
        &self,
        mut notification_rx: NotificationRx,
        stream_tx: mpsc::Sender<Result<Message, Status>>,
        peer: Peer,
    ) {
        let hooks = Arc::clone(&self.hooks);
        let global_exit_event = Arc::clone(&self.global_exit_event);

        tokio::spawn(async move {
            while let Some(mut data) = notification_rx.recv().await {
                if global_exit_event.load(Ordering::SeqCst) {
                    break;
                }

                update_send_history(&mut data);
                hooks.on_data_yield(&peer, &mut data).await;

                if stream_tx.send(Ok(data)).await.is_err() {
                    break;
                }
            }
        });
    }

    fn spawn_incoming_task(&self, mut incoming: tonic::Streaming<Message>, peer: Peer) {
        let hooks = Arc::clone(&self.hooks);
        let data_register = Arc::clone(&self.data_register);
        let connected_clients = Arc::clone(&self.connected_clients);
        let global_exit_event = Arc::clone(&self.global_exit_event);

        tokio::spawn(async move {
            loop {
                if global_exit_event.load(Ordering::SeqCst) {
                    break;
                }

                let incoming_item = incoming.message().await;
                let Ok(maybe_request) = incoming_item else {
                    debug!("{}: stream read ended by status", peer.peer);
                    break;
                };

                let Some(mut request) = maybe_request else {
                    break;
                };

                append_receive_history(&mut request);

                if !hooks.on_receive(&peer, &request).await {
                    continue;
                }

                let message_name = request
                    .meta_info
                    .as_ref()
                    .map(|m| m.message_name.clone())
                    .unwrap_or_default();

                data_register
                    .add_data_for_message_name(&peer.client_id, &message_name, request, None)
                    .await;
            }

            data_register
                .remove_notification_queues_for_client(&peer.client_id)
                .await;
            hooks.on_client_disconnect(&peer).await;
            connected_clients.fetch_sub(1, Ordering::SeqCst);
            info!("{}: disconnected", peer.peer);
        });
    }
}

#[async_trait]
impl<H: ServerHooks> Stream for BaseServer<H> {
    type DataChannelStream =
        Pin<Box<dyn futures_core::Stream<Item = Result<Message, Status>> + Send + 'static>>;

    async fn data_channel(
        &self,
        request: Request<tonic::Streaming<Message>>,
    ) -> Result<Response<Self::DataChannelStream>, Status> {
        let connected = self.connected_clients.fetch_add(1, Ordering::SeqCst) + 1;
        if self
            .config
            .warning_client_threshold
            .is_some_and(|threshold| connected >= threshold)
        {
            warn!(
                "connected clients ({}) reached warning threshold ({})",
                connected,
                self.config.warning_client_threshold.unwrap_or_default()
            );
        }

        let mut peer = Peer::new(
            request
                .remote_addr()
                .map(|address| address.to_string())
                .unwrap_or_else(|| "unknown".to_owned()),
        );

        if let Some(status) = self.handle_schema(request.metadata(), &peer) {
            self.connected_clients.fetch_sub(1, Ordering::SeqCst);
            return Err(status);
        }

        let metadata = request.metadata().clone();
        let mut incoming = request.into_inner();

        let Some(mut first_message) = incoming.message().await? else {
            self.connected_clients.fetch_sub(1, Ordering::SeqCst);
            return Err(Status::invalid_argument(
                "connection message required as first stream message",
            ));
        };

        append_receive_history(&mut first_message);

        let Some(client_info) = first_message
            .meta_info
            .as_ref()
            .and_then(|meta| meta.client_info.as_ref())
        else {
            self.connected_clients.fetch_sub(1, Ordering::SeqCst);
            return Err(Status::invalid_argument(
                "first message must include metaInfo.clientInfo",
            ));
        };

        peer.client_id = client_info.uuid.clone();
        peer.name = client_info.name.clone();

        if !self
            .hooks
            .on_client_connect(&first_message, &metadata, &peer)
            .await
        {
            self.connected_clients.fetch_sub(1, Ordering::SeqCst);
            return Err(Status::permission_denied("connection rejected by server"));
        }

        self.hooks.on_client_accepted(&peer, &first_message).await;

        let (notification_tx, notification_rx) =
            notification_channel(self.config.max_queue_elements);

        let mut welcome_message = Message {
            meta_info: Some(MetaInformation {
                server_info: Some(ServerProvides {
                    server_id: self.uid.clone(),
                    uuid: peer.session_id.clone(),
                    name: self.name.clone(),
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        set_metadata(&mut welcome_message);

        if notification_tx.send(welcome_message).await.is_err() {
            self.connected_clients.fetch_sub(1, Ordering::SeqCst);
            return Err(Status::internal("failed to enqueue welcome message"));
        }

        for require in &client_info.requires {
            self.data_register
                .add_notification_queue_for_message_name(
                    peer.client_id.clone(),
                    require.clone(),
                    notification_tx.clone(),
                )
                .await;
        }

        info!(
            "{}: connected with requires={:?} provides={:?} session_id={}",
            peer.peer, client_info.requires, client_info.provides, peer.session_id
        );

        drop(notification_tx);

        let (stream_tx, stream_rx) = mpsc::channel(self.config.response_stream_buffer);
        self.spawn_outgoing_task(notification_rx, stream_tx, peer.clone());
        self.spawn_incoming_task(incoming, peer);

        let output_stream = ReceiverStream::new(stream_rx);
        Ok(Response::new(
            Box::pin(output_stream) as Self::DataChannelStream
        ))
    }
}

fn append_receive_history(data: &mut Message) {
    if data.history.is_empty() {
        return;
    }

    data.history.push(DataPoint {
        name: "server".to_owned(),
        receive_timestamp: Some(now_timestamp()),
        send_timestamp: None,
        perf_counter: now_perf_counter(),
    });
}

fn update_send_history(data: &mut Message) {
    if data.history.is_empty() {
        return;
    }

    if let Some(last) = data.history.last_mut() {
        last.perf_counter = now_perf_counter() - last.perf_counter;
        last.send_timestamp = Some(now_timestamp());
    }
}

fn set_metadata(data: &mut Message) {
    if data.meta_info.is_none() {
        data.meta_info = Some(MetaInformation::default());
    }

    if let Some(meta) = data.meta_info.as_mut() {
        meta.timestamp = Some(now_timestamp());
        if meta.message_id.is_empty() {
            meta.message_id = Uuid::new_v4().to_string();
        }
    }
}

fn now_timestamp() -> prost_types::Timestamp {
    prost_types::Timestamp::from(SystemTime::now())
}

fn now_perf_counter() -> f64 {
    PERF_COUNTER_START
        .get_or_init(Instant::now)
        .elapsed()
        .as_secs_f64()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tonic::Code;

    use super::{
        append_receive_history, now_timestamp, set_metadata, update_send_history, BaseServer,
        DefaultHooks, Message, MetaInformation, Peer, ServerConfig,
    };
    use crate::pb::DataPoint;
    use crate::schema_version::{schema_version, SCHEMA_VERSION_METADATA_KEY};

    fn test_server(strict_schema_version: bool) -> BaseServer<DefaultHooks> {
        BaseServer::new(
            "127.0.0.1:50051",
            "test-server",
            DefaultHooks,
            ServerConfig {
                strict_schema_version,
                shutdown_poll_interval: Duration::from_millis(1),
                ..ServerConfig::default()
            },
        )
        .expect("test server should build")
    }

    #[test]
    fn shutdown_sets_global_exit_state() {
        let server = test_server(false);
        assert!(!server.is_shutdown());

        server.shutdown();

        assert!(server.is_shutdown());
    }

    #[test]
    fn strict_schema_mismatch_is_rejected() {
        let server = test_server(true);
        let peer = Peer::new("127.0.0.1:12345".to_owned());
        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert(
            SCHEMA_VERSION_METADATA_KEY,
            "different-version"
                .parse()
                .expect("metadata value should parse"),
        );

        let err = server
            .handle_schema(&metadata, &peer)
            .expect("strict mismatch should reject");

        assert_eq!(err.code(), Code::FailedPrecondition);
        assert!(err.message().contains(schema_version()));
        assert!(err.message().contains("different-version"));
    }

    #[test]
    fn permissive_schema_mismatch_is_accepted() {
        let server = test_server(false);
        let peer = Peer::new("127.0.0.1:12345".to_owned());
        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert(
            SCHEMA_VERSION_METADATA_KEY,
            "different-version"
                .parse()
                .expect("metadata value should parse"),
        );

        let result = server.handle_schema(&metadata, &peer);

        assert!(result.is_none());
    }

    #[test]
    fn set_metadata_populates_timestamp_and_message_id() {
        let mut message = Message::default();

        set_metadata(&mut message);

        let meta = message.meta_info.expect("meta info should exist");
        assert!(meta.timestamp.is_some());
        assert!(!meta.message_id.is_empty());
    }

    #[test]
    fn append_receive_history_adds_server_datapoint() {
        let mut message = Message {
            meta_info: Some(MetaInformation {
                timestamp: Some(now_timestamp()),
                ..MetaInformation::default()
            }),
            history: vec![DataPoint {
                name: "client".to_owned(),
                receive_timestamp: Some(now_timestamp()),
                send_timestamp: None,
                perf_counter: 0.0,
            }],
            ..Message::default()
        };

        append_receive_history(&mut message);

        assert_eq!(message.history.len(), 2);
        let last = message.history.last().expect("history entry should exist");
        assert_eq!(last.name, "server");
        assert!(last.receive_timestamp.is_some());
        assert!(last.send_timestamp.is_none());
        assert!(last.perf_counter >= 0.0);
    }

    #[test]
    fn update_send_history_sets_send_timestamp_and_elapsed_time() {
        let mut message = Message {
            history: vec![DataPoint {
                name: "server".to_owned(),
                receive_timestamp: Some(now_timestamp()),
                send_timestamp: None,
                perf_counter: 0.0,
            }],
            ..Message::default()
        };

        update_send_history(&mut message);

        let last = message.history.last().expect("history entry should exist");
        assert!(last.send_timestamp.is_some());
        assert!(last.perf_counter >= 0.0);
    }
}
