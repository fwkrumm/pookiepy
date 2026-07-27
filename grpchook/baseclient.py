"""
gRPC client base class.

Provides ``BaseClient``, which handles channel management, send/receive
queuing, and connection lifecycle.  Consumers subclass ``BaseClient`` and
override the hook methods (``on_init``, ``on_shutdown``, ``on_receive``).

The synchronization of the receive thread and the main thread is
handled via a queue for received messages and a threading.Event (``run_event``) to signal shutdown.
"""
import queue
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Union

import grpc

from grpchook import message_pb2
from grpchook import message_pb2_grpc

from grpchook.logger import get_logger
from grpchook.exceptions import GrpcConnectionError, \
                              GrpcTimeoutError, \
                              GrpcResourceExhaustedError, \
                              GrpcValueError, \
                              ClientExit, \
                              GrpcEmpty

from grpchook.tools import set_metadata
from grpchook.schema_version import SCHEMA_VERSION, SCHEMA_VERSION_METADATA_KEY


class _StreamError:  # pylint: disable=too-few-public-methods
    """Sentinel placed into receive_queue when the receive thread encounters a fatal
    stream error.  This lets the main thread (``_check_connection``, ``get_data``)
    raise the correct exception immediately instead of waiting for a timeout.
    """

    def __init__(self, exc: Exception):
        self.exc = exc

# seconds to sleep after send_queue.join() in wait_done() to ensure all messages are processed
WAIT_DONE_ADDITIONAL_SLEEP_DEFAULT = 0.5

# timeout in seconds to wait for receive thread to join after signaling shutdown
TIMEOUT_WAIT_THREAD_JOIN = 5.0

@dataclass
class ClientConfig:
    """Central configuration for BaseClient. Pass an instance to BaseClient.__init__."""

    # max elements for receive queue (0 = unlimited)
    receive_queue_maxsize: int = 0
    # timeout in seconds to wait for server welcome message after connecting
    connection_check_timeout: float = 5.0
    # gRPC channel options
    ssl_credentials: grpc.ChannelCredentials = None
    # extra/extend (key, value) metadata tuples appended to every gRPC stream call;
    # the schema-version entry is always prepended automatically
    ext_metadata: list = field(default_factory=list)
    # gRPC compression algorithm applied to client-sent messages.
    # Must be enabled on BOTH server and client to compress both directions.
    # If only the client sets this, only client->server messages are compressed;
    # server->client messages remain uncompressed (no error, just partial compression).
    # Example: grpc.Compression.Gzip
    compression: grpc.Compression = None
    grpc_options: list = field(default_factory=lambda: [
        ("grpc.keepalive_time_ms", 180000),  # 3 minutes
        ("grpc.keepalive_timeout_ms", 10000),  # 10 seconds
        ("grpc.keepalive_without_calls", True),
    ])


class BaseClient:  # pylint: disable=too-many-instance-attributes
    """
    Base class for gRPC client implementations

    Client metadata is sent automatically on connection via gRPC metadata headers.
    Subclasses MUST implement get_client_metadata() to provide identification.
    """

    def __init__(self,
                 port: int,
                 *,
                 name: str = "client",
                 provides: list[str] = None,
                 requires: list[str] = None,
                 ip: str = "localhost",
                 config: ClientConfig = None):

        self.logger = get_logger(name=f"Client-{name}")

        self._config = config or ClientConfig()
        self.ip = ip

        # the following methods have to be overwritten by user in subclass
        self.provides = provides or []
        self.requires = requires or []
        self.port = port

        self.name = name
        self.uuid = ""  # set per-connection in _setup_connection()

        if not self.provides and not self.requires:
            self.logger.warning("Client provides and requires lists are both empty. "\
                                "That means the client will connect but neither receive not "\
                                "send anything. Set self.provides and self.requires in "\
                                "your subclass.")

        self.run_event = threading.Event()

        # will be set in _setup_connection()
        self.channel = None
        self.receive_thread = None
        self.stream = None
        self.server_session_id = ""

        self._setup_connection()

        # if connection fails exception will be raised before
        self.logger.info("Client %s connected (schema=%s)", self, SCHEMA_VERSION)


    def _setup_connection(self):
        """Create channel, stub, and queues, then connect. Safe to call on reconnect."""

        if self._config.ssl_credentials is None:
            self.channel = grpc.insecure_channel(f"{self.ip}:{self.port}",
                                                 options=self._config.grpc_options)
        else:
            self.logger.iinfo("Using SSL credentials for client")
            self.channel = grpc.secure_channel(f"{self.ip}:{self.port}",
                                               self._config.ssl_credentials,
                                               options=self._config.grpc_options)

        # stub and queues will be re-created on every connection attempt,
        # but this is necessary to ensure a clean state on reconnect.
        # The old ones will be garbage collected after disconnect and should
        # not cause any issues.
        self.stub = message_pb2_grpc.StreamStub(self.channel)
        self.send_queue = queue.Queue()
        self.receive_queue = queue.Queue(maxsize=self._config.receive_queue_maxsize)
        self.server_session_id = ""
        self.uuid = str(uuid.uuid4())  # new UUID per connection --- avoids DataRegister race
        self.run_event.set()  # set BEFORE receive thread so that the latter starts

        # start connection and receive thread
        self.run()

        # call on_init hook
        self.on_init()

    def run(self):
        """
        connect to server and start receiving thread; a message handshake with the server
        will be performed to assure connection
        """
        self._connect()
        self._start_receive_thread() # important to call that before _check_connection
        self._check_connection()

    def __repr__(self):
        return (
            f"Client(name={self.name} uuid={self.uuid}, "
            f"provides={self.provides}, requires={self.requires}, "
            f"ip={self.ip}, port={self.port}, "
            f"serverSessionId={self.server_session_id})"
        )

    def __str__(self):
        return self.__repr__()

    def _connect(self):
        """
        Connect to server and start send and receive threads
        """

        try:
            grpc.channel_ready_future(self.channel).result(timeout=2.0)
        except grpc.FutureTimeoutError:
            self.logger.error("Failed to connect to server at port %d: timeout", self.port)
            self.channel = None # ensure channel is set to None if connection failed since
                                # otherwise channel.close() blocks indefinitely. This might not be
                                # the optimal solution.
            raise GrpcConnectionError(
                f"Failed to connect to server at port {self.port}: timeout"
            ) from None


        self.stream = self.stub.DataChannel(
            self._request_generator(),
            metadata=[(SCHEMA_VERSION_METADATA_KEY, SCHEMA_VERSION)] + self._config.ext_metadata,
            compression=self._config.compression,
        )

        # send welcome message to server and exchange uuid, requires and provides lists
        self.logger.iinfo(
            "Connected to server at port %d sending connect message", self.port
        )
        first_message = message_pb2.Message(
            metaInfo=message_pb2.MetaInformation(
                clientInfo=message_pb2.ClientProvides(
                    uuid=self.uuid,
                    name=self.name,
                    requires=self.requires,
                    provides=self.provides
                )
            )
        )
        self.send_queue.put(first_message)

    def _check_connection(self):
        """Wait for the server welcome message and record the server session UUID."""

        self.logger.iinfo("waiting for server response")

        try:
            response = self.receive_queue.get(timeout=self._config.connection_check_timeout)
        except queue.Empty:
            self.logger.error(
                "Did not receive response from server within timeout after connecting"
            )
            self.disconnect()
            raise GrpcConnectionError("Did not receive response from server within"\
                " timeout after connecting") from None

        if isinstance(response, _StreamError):
            # Receive thread detected a fatal stream error (e.g. PERMISSION_DENIED).
            # Raise it here on the main thread with the correct type and message.
            # Here we log as info since the actual error is already logged in the receive thread,
            # and this just confirms that the connection failed.
            self.logger.iinfo("_StreamError received: %s; disconnecting.", response.exc)
            self.disconnect()
            raise response.exc

        if not response.metaInfo.HasField("serverInfo"):
            self.logger.error(
                "Expected welcome message (serverInfo) as first server response "
                "but got messageName=%r. This indicates a server-side ordering bug.",
                response.metaInfo.messageName,
            )
            self.disconnect()
            raise GrpcConnectionError(
                "Did not receive welcome message as first server response; "
                f"got messageName={response.metaInfo.messageName!r} instead"
            )

        self.server_session_id = response.metaInfo.serverInfo.uuid
        self.logger.iinfo(
            "Received response from server with uuid %s", self.server_session_id
        )

    def _start_receive_thread(self):
        """
        Start thread to receive messages from server
        """
        # use demon thread so that it does not block program exit if something goes wrong
        self.receive_thread = threading.Thread(target=self._receive_loop, daemon=True)
        self.receive_thread.start()

    def _request_generator(self):
        """
        generator for request iterator

        Yields
        ------
        message_pb2.Message
            message to send
        """
        while self.run_event.is_set():
            try:
                data: message_pb2.Message = self.send_queue.get(timeout=1)
                self.logger.idebug("Sending message to server: %s", data.metaInfo)
                if data.history:
                    # if there is a history extend it
                    data.history[-1].perfCounter = (
                        time.perf_counter() - data.history[-1].perfCounter
                    )
                    data.history[-1].sendTimestamp = datetime.now(timezone.utc)
                # if meta data have not been set, set them automatically
                set_metadata(data)

                if not data.metaInfo.timestamp:
                    raise GrpcValueError("Message timestamp is not set even after set_metadata()")

                # so far the only line where the message id is logged
                self.logger.idebug("Sending message with timestamp %s and messageId %s",
                                  data.metaInfo.timestamp,
                                  data.metaInfo.messageId)

                yield data

                # mark the message as done in the queue after it was sent to the server via yield
                self.send_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:  # pylint: disable=broad-exception-caught
                self.logger.error("Error in request generator: %s", e)
                break
        self.logger.idebug("Request generator terminated, clearing send queue")
        # in case of early disconnect, drain all unfinished_tasks so wait_done() never deadlocks
        while True:
            try:
                # make sure to mark all messages as done so that send_queue.join() in wait_done()
                # does not block indefinitely
                self.send_queue.task_done()
            except ValueError:
                break

    def disconnect(self):
        """
        Disconnect from server and stop send and receive threads

        This function is not thread safe i.e. do not call it from multiple
        threads or if you must (for whatever reason) ensure external
        synchronization.
        """

        self.logger.iinfo("Disconnecting client %s", self)

        if not self.run_event.is_set():
            # already disconnected or never connected, do nothing
            self.logger.iinfo("Client %s is already disconnected or never connected", self)
            return

        # clear event early so that receivec thread can finish gracefully.
        self.run_event.clear()

        if self.stream is not None:
            self.stream.cancel()  # cancel stream, unblocking the receive loop

        if self.channel is not None:
            self.channel.close()

        if self.receive_thread and self.receive_thread.is_alive():
            self.logger.iinfo("waiting for receive thread to finish")
            self.receive_thread.join(timeout=TIMEOUT_WAIT_THREAD_JOIN)

        self.on_shutdown()

        self.logger.iinfo("Client %s disconnected", self)

    def _receive_loop(self):
        """
        continuously receive messages from the server
        """
        self.logger.iinfo("Receive loop started")
        try:
            for response in self.stream:
                if response.history:
                    response.history.append(message_pb2.DataPoint(
                        name=self.name,
                        receiveTimestamp=datetime.now(timezone.utc),
                        perfCounter=time.perf_counter()
                    ))
                # NOTE do not log entire message since this might affect performance negatively.
                self.logger.idebug("received data from server: %s", response.metaInfo)
                self.receive_queue.put(response)
        except grpc.RpcError as err:
            if err.code() == grpc.StatusCode.CANCELLED:
                self.logger.iinfo("stream cancelled, likely due to client disconnect")
                return
            if err.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                self.logger.error(err.details())
                self.receive_queue.put(_StreamError(GrpcTimeoutError(err.details())))
                return
            if err.code() == grpc.StatusCode.RESOURCE_EXHAUSTED:
                self.logger.error("resource exhausted error: %s", err.details())
                self.receive_queue.put(
                    _StreamError(GrpcResourceExhaustedError(f"resource exhausted: {err.details()}"))
                )
                return
            # All other gRPC errors (e.g. PERMISSION_DENIED): route through the queue
            # so the main thread raises the correct exception immediately rather than
            # waiting for connection_check_timeout and getting a misleading message.
            self.logger.error(err.details())
            self.receive_queue.put(_StreamError(GrpcConnectionError(err.details())))
            return
        except Exception as e:  # pylint: disable=broad-exception-caught
            self.logger.error("Error in receive loop: %s", e)
            self.receive_queue.put(_StreamError(e))
            return
        finally:
            self.logger.iinfo("Receive loop terminated")

    def send_data(self, data: message_pb2.Message, add_history: bool = False):
        """
        put data to queue from where they will be sent to grpc server

        Parameters
        ----------
        data : message_pb2.Message
            The message to be sent to the gRPC server.

        Raises
        ------
        GrpcValueError
            If the data is not of type message_pb2.Message or if the message name
            is not in the provides list.
        GrpcValueError
            _description_
        """
        if not isinstance(data, message_pb2.Message):
            raise GrpcValueError(
                f"Data must be of type message_pb2.Message, but got {type(data)}"
            )

        if data.metaInfo.messageName not in self.provides:
            # actually we could work without this check and just let the user send whatever
            # they want, but this case we could not track "which data might be provided later on"
            raise GrpcValueError(
                f"Message name {data.metaInfo.messageName} not in provides list {self.provides}"
            )

        if add_history:
            if data.history:
                raise GrpcValueError("Data already has history, this will automatically extend "\
                                     "the history throughtout the data flow, so add_history "\
                                     "should not be used in this case.")
            data.history.append(message_pb2.DataPoint(
                name=self.name,
                receiveTimestamp=datetime.now(timezone.utc),
                perfCounter=time.perf_counter())
            )

        self.send_queue.put(data)
        self.logger.idebug("Enqueued data %s", data.metaInfo)

    def wait_done(self, additional_sleep: float = WAIT_DONE_ADDITIONAL_SLEEP_DEFAULT):
        """Block until all queued messages were handed to the gRPC stream.

        Important: this confirms only local hand-off to gRPC (``yield`` from the
        request generator), not that the server application has already processed
        the message. If callers disconnect immediately afterwards, the stream may
        still be cancelled while the message is in-flight.

        The optional ``additional_sleep`` adds a best-effort grace window after
        ``send_queue.join()`` to reduce this race, especially on loaded CI runners
        or remote links with higher latency/jitter.

        Parameters
        ----------
        additional_sleep : float, optional
            Extra seconds to sleep after queue drain. Default is
            ``WAIT_DONE_ADDITIONAL_SLEEP_DEFAULT`` (=0.5s), chosen as a
            conservative compromise for teardown/control messages (for example
            ``server-exit``). Set to 0 for max throughput-sensitive paths.
        """
        if not self.run_event.is_set():
            raise GrpcConnectionError("run event not set i.e. client is probably disconnected")

        if additional_sleep < 0:
            raise GrpcValueError("additional_sleep must be >= 0")

        self.send_queue.join()

        if additional_sleep > 0:
            # additional sleep to ensure that after the queue has joined the final message
            # has been sent to the server.
            time.sleep(additional_sleep)

    def __enter__(self):
        if not self.run_event.is_set():
            # if client is not connected (e.g. disconnected earlier), try to reconnect
            self._setup_connection()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def __del__(self):
        try:
            self.disconnect()
        except (OSError, AttributeError, RuntimeError):
            # sometimes on windows at __del__() the run_event is already garbage collected,
            # so we catch the OSError which is raised when trying to access it and just
            # ignore it since the client is already effectively disconnected at this point
            # also catch AttributeError and RuntimeError just to be safe
            pass

    def get_data(self, timeout: float = None) -> message_pb2.Message:
        """
        Get a response from the receive queue.

        Never blocks permanently --- polls in 1-second slices so that the run
        event is checked on every iteration and Ctrl+C (KeyboardInterrupt) is
        always handled promptly.

        Parameters
        ----------
        timeout : float, optional
            Total timeout in seconds. None means wait until a message arrives
            or the client disconnects. 0 means non-blocking (raises
            queue.Empty immediately if nothing is available).

        Returns
        -------
        message_pb2.Message
            The received message

        Raises
        ------
        queue.Empty
            If timeout is reached and no message is available, or if the
            client disconnects while waiting.
        ClientExit
            If the client is disconnected while waiting for a message.
        """
        slice_size = 1.0  # max seconds to block per iteration

        if timeout == 0:
            item = self.receive_queue.get_nowait()
            if isinstance(item, _StreamError):
                raise item.exc
            return item

        deadline = None if timeout is None else time.monotonic() + timeout

        while self.run_event.is_set():
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    # final throw after timeout is reached, to distinguish from queue.Empty
                    raise GrpcEmpty(f"Timeout ({timeout}s) reached while waiting for message")
                slice_timeout = min(slice_size, remaining)
            else:
                slice_timeout = slice_size

            try:
                item = self.receive_queue.get(timeout=slice_timeout)
                if isinstance(item, _StreamError):
                    raise item.exc
                return item
            except queue.Empty:
                continue  # check run_event / deadline on next iteration
            except KeyboardInterrupt:
                self.logger.warning("KeyboardInterrupt received")
                break

        # run_event was cleared before a message arrived
        raise ClientExit("Run event cleared")



    def spin(self, timeout: float = None) -> Union[bool, any]:
        """
        Process a single message from the receive queue.

        Parameters
        ----------
        timeout : float, optional
            Passed to get_data(). None = wait forever. 0 = non-blocking.

        Returns
        -------
        Union[bool, any]
            return value can be anything the user adds via on_receive hook
        """
        try:
            data = self.get_data(timeout=timeout)
            return self.on_receive(data)
        except ClientExit:
            self.logger.iinfo("ClientExit received, stopping spin")
            return False
        except GrpcEmpty:
            self.logger.iinfo("No message received within timeout, stopping spin")
            return False

    def spin_forever(self, timeout: float = None):
        """
        Continuously process messages from the receive queue until the client is disconnected.
        spin() has to return explicitly false to stop the loop.

        NOTE that if you use spin_forever() the data are not returned to the caller. In that
        case you should not return the actual data via on_receive.

        Parameters
        ----------
        timeout : float, optional
            Per-message timeout passed to spin(). None = wait forever per message.
        """
        while self.run_event.is_set():
            if self.spin(timeout=timeout) is False:
                break

#
# Hooks
#

    @property
    def config(self) -> "ClientConfig":
        """Client configuration (read-only)."""
        return self._config

    def on_init(self):
        """
        Hook method called after successful connection and initialization of the client.
        Override this in your subclass to implement custom behavior after the client is initialized.
        """

    def on_receive(self, data: message_pb2.Message) -> bool:
        """
        Hook method to handle received messages. Override this in your subclass to
        implement custom behavior.

        NOTE that if you return False, and only then, spin_forever()
            will stop processing further messages.

        Parameters
        ----------
        data : message_pb2.Message
            The message received from the server. This is passed directly from the receive loop, so
            it is not removed from the receive queue yet. If you want to remove it from the queue,
            you can call get_data() in this function, but be aware that this will block until a new
            message arrives if there are no more messages in the queue.

        Returns
        -------
        bool
            Whether the message was handled successfully.
        """
        self.logger.warning("Received data but on_receive() is not implemented. Data metaInfo: %s",
                            data.metaInfo)
        return True

    def on_shutdown(self):
        """
        Hook method called during client shutdown. Override this in your subclass to implement
        custom behavior during shutdown, e.g. to clean up resources or send a final
        message to the server.
        """
