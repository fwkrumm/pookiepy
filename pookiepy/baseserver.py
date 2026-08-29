
"""
gRPC server base class.

Provides ``BaseServer``, a ``StreamServicer`` subclass that handles all
transport-layer plumbing for bidirectional streaming.  Consumers subclass
``BaseServer`` and override the hook methods (``on_init``, ``on_shutdown``,
``on_client_connect``, ``on_receive``).
"""

import os
import queue
import threading
import time
import uuid
from concurrent import futures
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterator

import grpc
from google.protobuf.message import Message as PookieMessage


from pookiepy.custom_interface import ProtoInterface, _bundled_interface
from pookiepy.logger import get_logger
from pookiepy.data_register import DataRegister
from pookiepy.tools import set_metadata
from pookiepy.schema_version import SCHEMA_VERSION_METADATA_KEY, DEFAULT_SCHEMA_VERSION

@dataclass
class ServerConfig():
    """Central configuration for BaseServer. Pass an instance to BaseServer.__init__."""

    # max elements for per-client notification queue (0 = unlimited)
    max_queue_elements: int = 0
    # worker threads for the gRPC executor; None = Python default (min(32, cpu+4)).
    # Each connected client occupies one thread for the full connection lifetime.
    # Set to at least the expected number of concurrent clients.
    max_workers: int | None = None
    # queue size at which a warning is logged for a subscriber queue.
    # None disables warnings. This is also used for unbounded queues where queue.Full never fires.
    queue_warning_threshold: int | None = 100_000
    # interval in seconds for the serve_forever shutdown-detection watchdog
    shutdown_poll_interval: float = 0.1
    # application-managed schema version string expected from connecting clients
    # will be set to a default value if at runtime no custom interface is provided and
    # schema version is not explicitly set by the user
    schema_version: str = None
    # gRPC compression algorithm applied to server-sent messages.
    # Must be enabled on BOTH server and client to compress both directions.
    # If only the server sets this, only server->client messages are compressed;
    # client->server messages remain uncompressed (no error, just partial compression).
    # Example: grpc.Compression.Gzip
    compression: grpc.Compression = None
    server_options: list = field(default_factory=lambda: [
        ("grpc.keepalive_time_ms", 180000),  # 3 minutes
        ("grpc.keepalive_timeout_ms", 10000),  # 10 seconds
        ("grpc.keepalive_permit_without_calls", True),
        ("grpc.http2.max_ping_strikes", 0),
        # some possible options in case of buffer issues
        #("grpc.max_send_message_length", 50 * 1024 * 1024),  # 50MB
        #("grpc.max_receive_message_length", 50 * 1024 * 1024),  # 50MB
        #("grpc.http2.write_buffer_size", 64 * 1024 * 1024),  # 64MB
        #("grpc.http2.max_frame_size", 16384)  # Minimum allowed frame size
    ])

    @property
    def effective_max_workers(self) -> int:
        """Resolve the effective thread-pool size.

        Returns ``max_workers`` when set explicitly, otherwise mirrors
        ``ThreadPoolExecutor``'s own default: ``min(32, cpu_count + 4)``.
        """
        if self.max_workers is not None:
            return self.max_workers
        return min(32, (os.cpu_count() or 1) + 4)


@dataclass
class Peer:
    peer: str
    session_id: str
    client_id: str = "" # provided at first data exchange
    name: str = "" # provided at first data exchange

    def __repr__(self):
        return f"Peer(peer={self.peer}, clientId={self.client_id}, "\
               f"name={self.name}, sessionId={self.session_id})"

    def __str__(self):
        return self.__repr__()

class BaseServer:  # pylint: disable=too-many-instance-attributes
    """
    Base class for gRPC server implementations

    Client metadata is automatically extracted from incoming connections.
    Subclasses can override on_client_connect() to validate/handle client metadata.
    """

    def __init__(self,
                 port: int,
                 *,
                 name: str = "server",
                 ip: str = "[::]",
                 global_exit_event: threading.Event = None,
                 ssl_credentials: grpc.ServerCredentials = None,
                 config: ServerConfig = None,
                 proto_interface: ProtoInterface = None):

        self._name = name
        self.logger = get_logger(name=self._name)

        self.__ssl_credentials = ssl_credentials
        self.__config = config or ServerConfig()

        if proto_interface is None:
            self._proto_interface = _bundled_interface()
            if self.__config.schema_version is None:
                self.__config.schema_version = DEFAULT_SCHEMA_VERSION
        else:
            self._proto_interface = proto_interface

        if self.__config.schema_version is None:
            self.__config.schema_version = ""

        self._message_pb2 = self._proto_interface.message_pb2
        self._message_pb2_grpc = self._proto_interface.message_pb2_grpc

        # routes incoming messages to per-client notification queues
        self._data_register = DataRegister(
            self.logger,
            queue_warning_threshold=self.__config.queue_warning_threshold,
            message_type=self._message_pb2.PookieMessage,
        )
        self._global_exit_event = global_exit_event or threading.Event()  # exit event for shutdown

        self._port = port
        self._ip = ip
        self._uid = str(uuid.uuid4())

        self.__connected_clients = 0
        self.__connected_clients_lock = threading.Lock()

        self.on_init()

        self.logger.iinfo("initialized %s, using schema version %s",
                          self._name,
                          self.__config.schema_version)


    def __repr__(self):
        return f"BaseServer(name={self._name}, id={self._uid}, ip={self._ip}, port={self._port})"

    def __str__(self):
        return self.__repr__()

    @property
    def name(self) -> str:
        """Public name of this server."""
        return self._name

    def _handle_client_receive(  # pylint: disable=too-many-arguments,R0917
        self,
        request_iterator: Iterator[PookieMessage],
        context,
        peer: "Peer",
        notification_queue: queue.Queue,
        exit_event: threading.Event,
    ) -> None:
        """
        Process messages from a single connected client in a background thread.
        R0917 too many statements is ok here since this is a single
        logical flow with multiple early exits.

        Parameters
        ----------
        request_iterator : Iterator[google.protobuf.message.PookieMessage]
            Iterator over incoming messages from the client.
        context : _type_
            gRPC context for the current RPC.
        peer : Peer
            Information about the connected client.
        notification_queue : queue.Queue
            Queue for sending notifications to the client.
        exit_event : threading.Event
            Event to signal exit for the background thread.
        """
        try:
            for request in request_iterator:
                request: PookieMessage

                if request.history:
                    request.history.append(
                        self._message_pb2.DataPoint(
                            name="server",
                            receiveTimestamp=datetime.now(timezone.utc),
                            perfCounter=time.perf_counter(),
                        )
                    )

                self.logger.idebug("%s: received message: %s", peer, request.metaInfo)

                if not peer.client_id:
                    # first message

                    peer.client_id = request.metaInfo.clientInfo.uuid
                    peer.name = request.metaInfo.clientInfo.name
                    requires = request.metaInfo.clientInfo.requires
                    provides = request.metaInfo.clientInfo.provides

                    accepted = self.on_client_connect(request, context)

                    if not accepted:
                        self.logger.error("%s: connection rejected", peer)
                        # Set status metadata first so the client sees a proper gRPC status,
                        # then try abort() for explicit termination semantics.
                        # In this background-thread path grpcio may raise a bare Exception;
                        # swallow it because status/details are already set.
                        context.set_code(grpc.StatusCode.PERMISSION_DENIED)
                        context.set_details("connection rejected by server")
                        try:
                            context.abort(
                                grpc.StatusCode.PERMISSION_DENIED,
                                "connection rejected by server"
                            )
                        except Exception:  # pylint: disable=broad-exception-caught
                            pass
                        return

                    self.logger.iinfo(
                        "%s: connected with requires %s, provides %s, sessionId %s",
                        peer, requires, provides, peer.session_id
                    )

                    self.on_client_accepted(peer, request)

                    # Welcome message must be enqueued BEFORE registering requires in
                    # DataRegister.  Once a require is registered, other threads (e.g. a
                    # server-side broadcast loop) can immediately push data messages into
                    # notification_queue. If the welcome message were enqueued after
                    # registration there would be a race where a data message arrives first,
                    # causing _check_connection() on the client to consume the wrong message
                    # and subsequent get_data() calls to return the welcome (empty payload).
                    welcome_message = self._message_pb2.PookieMessage(
                        metaInfo=self._message_pb2.MetaInformation(
                            serverInfo=self._message_pb2.ServerProvides(
                                serverId=self._uid,
                                uuid=peer.session_id,
                                name=self._name,
                            )
                        )
                    )
                    set_metadata(welcome_message)
                    notification_queue.put(welcome_message)

                    for require in requires:
                        self._data_register.add_notification_queue_for_message_name(
                            peer.client_id,
                            require,
                            notification_queue,
                        )

                    continue

                self.logger.idebug("%s: received message from connected peer", peer)

                if not self.on_receive(peer, request):
                    self.logger.iinfo(
                        "%s: user defined on_receive returned False, "
                        "data will not be added to notification queue.",
                        peer
                    )
                    continue

                self._data_register.add_data_for_message_name(
                    peer.client_id,
                    request.metaInfo.messageName,
                    request,
                )
        except grpc.RpcError:
            # probably ok (disconnect)
            pass
        finally:
            exit_event.set()
            self.logger.idebug("%s: exit event set", peer)

    def DataChannel(  # pylint: disable=invalid-name
        self,
        request_iterator: Iterator[PookieMessage],
        context,
    ):
        """
        Handle bidirectional streaming. Client metadata is extracted first.
        """

        # get ip from context; client id received at first message receive
        # peer contains all important information peer context, client id, session id, etc
        connection_peer = Peer(peer=context.peer(), session_id=str(uuid.uuid4()))

        # queue for notifications to client
        notification_queue = queue.Queue(maxsize=self.__config.max_queue_elements)

        exit_event = threading.Event()

        with self.__connected_clients_lock:
            self.__connected_clients += 1
            current_count = self.__connected_clients

        if current_count >= self.__config.effective_max_workers:
            self.logger.warning(
                "Connected clients (%d) reached max_workers (%d). "
                "The next client will stall until a slot opens. "
                "Set ServerConfig.max_workers explicitly to handle more concurrent clients.",
                current_count, self.__config.effective_max_workers
            )

        self.logger.idebug(
            "%s: connected to DataChannel. Checking permissions",
            connection_peer,
        )

        try:
            # Verify application-managed schema compatibility before processing any messages.
            metadata = dict(context.invocation_metadata())
            client_schema = metadata.get(SCHEMA_VERSION_METADATA_KEY, "")
            server_schema = self.__config.schema_version

            if not server_schema and not client_schema:
                self.logger.warning("%s: cannot check schema because empty", connection_peer)
            elif client_schema != server_schema:
                self.logger.error(
                    "%s: schema mismatch - server=%s client=%s. Rejecting connection.",
                    connection_peer,
                    server_schema,
                    client_schema,
                )
                context.abort(
                    grpc.StatusCode.FAILED_PRECONDITION,
                    f"Proto schema mismatch: server={server_schema}, client={client_schema}"
                )
                return

            # Process messages
            t = threading.Thread(
                target=self._handle_client_receive,
                args=(request_iterator, context, connection_peer, notification_queue, exit_event),
                daemon=True,
            )
            t.start()
            try:
                while not (exit_event.is_set() or self._global_exit_event.is_set()):
                    self.logger.idebug("%s: main thread running", connection_peer)
                    try:
                        data =  notification_queue.get(timeout=1)  # wait for data to send to client
                        if data.history:
                            data.history[-1].perfCounter = (
                                time.perf_counter() - data.history[-1].perfCounter
                            )
                            data.history[-1].sendTimestamp = datetime.now(timezone.utc)

                        try:
                            self.on_data_yield(connection_peer, data)
                        except Exception as exc:  # pylint: disable=broad-exception-caught
                            self.logger.error(
                                "%s: error in on_data_yield hook: %s",
                                connection_peer,
                                exc,
                            )

                        yield data
                        self.logger.idebug("%s: sent notification", connection_peer)
                    except queue.Empty:
                        continue
            finally:
                self._data_register.remove_notification_queues_for_client(
                    connection_peer.client_id
                )
                self.on_client_disconnect(connection_peer)
                self.logger.iinfo("%s: disconnected", connection_peer)
        finally:
            with self.__connected_clients_lock:
                self.__connected_clients -= 1

    def shutdown(self):
        if not self._global_exit_event.is_set():
            self.logger.iinfo("setting global exit event for server shutdown")
            self._global_exit_event.set()
        else:
            self.logger.idebug("global exit event already set")
        self.on_shutdown()

    def serve_forever(self):
        """
        Start the server and wait for termination
        """
        executor = futures.ThreadPoolExecutor(
            max_workers=self.__config.effective_max_workers
        )
        self.logger.iinfo(
            "max_workers set to %d (effective). "
            "Each connected client occupies one thread for its full connection lifetime.",
            self.__config.effective_max_workers
        )
        if self.__config.max_queue_elements == 0:
            self.logger.iinfo(
                "Per-client notification queues are unbounded. queue.Full will never fire; "
                "set ServerConfig.max_queue_elements > 0 to enable backpressure."
            )
        server = grpc.server(
            executor,
            options=self.__config.server_options,
            compression=self.__config.compression,
        )
        self._message_pb2_grpc.add_StreamServicer_to_server(self, server)
        if self.__ssl_credentials is None:
            server.add_insecure_port(f"{self._ip}:{self._port}")
        else:
            self.logger.iinfo("Using SSL credentials for server")
            server.add_secure_port(f"{self._ip}:{self._port}", self.__ssl_credentials)
        server.start()
        self.logger.info(
            "server %s started (schema=%s)",
            self,
            self.__config.schema_version,
        )
        try:
            while not self._global_exit_event.is_set():
                self._global_exit_event.wait(timeout=self.__config.shutdown_poll_interval)
            # usually this was:
            # server.wait_for_termination()
        except KeyboardInterrupt:
            pass
        finally:
            self.logger.iinfo("shutting down server")
            self.shutdown()
            stop_event = server.stop(grace=None)
            if not stop_event.wait(timeout=10):
                self.logger.warning(
                    "gRPC server stop did not complete within 10s; continuing shutdown"
                )
            server = None
            # shutdown executor i.e. wait for all DataChannel threads to finish
            executor.shutdown(wait=True)
        self.logger.iinfo("server stopped")


#
# Hooks
#

    @property
    def global_exit_event(self) -> threading.Event:
        """The server's global exit event (read-only)."""
        return self._global_exit_event

    @property
    def config(self) -> "ServerConfig":
        """Server configuration (read-only)."""
        return self.__config

    def on_data_yield(self, peer: Peer, data: PookieMessage):
        """
        Hook called right before a message is yielded to a client stream.

        This is a best-effort notification point that occurs when the message is
        handed off to the gRPC stream iterator, not when the client has consumed it.
        """

    def on_init(self):
        """
        Called after server initialization. Override to perform additional setup.
        """

    def on_shutdown(self):
        """
        Called during server shutdown. Override to perform cleanup.
        """

    def on_receive(self,
                   peer: Peer,
                   request: PookieMessage,
                   ) -> bool:
        """
        Called when a message is received. Override to handle incoming messages.

        Parameters
        ----------
        peer : Peer
            The peer that sent the message
        request : google.protobuf.message.PookieMessage
            The message sent by the client

        Returns
        -------
        bool : True if the message should be added to the notification queue, False otherwise.
        """
        # pylint: disable=unused-argument
        return True

    def on_client_connect(self,
                          data: PookieMessage,
                          context: grpc.ServicerContext
                          ) -> bool:
        """
        Called when a client connects. Override to validate client metadata.

        For example one could require the clients to provide specific information within
        the payload. that may each user decide himself.

        Parameters
        ----------
        data : google.protobuf.message.PookieMessage
            The message sent by the client
        context : grpc.ServicerContext
            The RPC context (can be used to abort connection)

        Returns
        -------
        bool : True if connection is accepted, False to reject.
               If False, context.abort() should be called
        """
        # pylint: disable=unused-argument
        return True

    def on_client_disconnect(self, peer: Peer):
        """Called when a client stream is fully disconnected.

        Parameters
        ----------
        peer : Peer
            The disconnected peer.
        """
        # pylint: disable=unused-argument

    def on_client_accepted(self, peer: Peer, request: PookieMessage):
        """Called after a client has been accepted and registered.

        Parameters
        ----------
        peer : Peer
            The accepted peer.
        request : google.protobuf.message.PookieMessage
            The first connect message containing ``clientInfo``.
        """
        # pylint: disable=unused-argument
