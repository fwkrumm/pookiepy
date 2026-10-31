"""Client connection and lifecycle tests with transport creation mocked."""

import queue
import unittest
from unittest.mock import MagicMock, call, patch

import grpc

from pookiepy import message_pb2, message_pb2_grpc
from pookiepy.baseclient import BaseClient, ClientConfig, TIMEOUT_WAIT_THREAD_JOIN, _StreamError
from pookiepy.custom_interface import ProtoInterface
from pookiepy.exceptions import GrpcConnectionError
from pookiepy.schema_version import SCHEMA_VERSION_METADATA_KEY


class TestConnectionLifecycle(unittest.TestCase):
    """Verify transport setup, handshake validation, reconnect, and cleanup."""

    def setUp(self):
        self.channel_patch = patch("pookiepy.baseclient.grpc.insecure_channel")
        self.channel = self.channel_patch.start().return_value
        self.addCleanup(self.channel_patch.stop)
        with patch.object(BaseClient, "run"):
            self.client = BaseClient(port=50099, provides=["topic"], requires=["reply"])
        self.client.logger = MagicMock()
        self.addCleanup(self.client.disconnect)

    def test_run_orders_connection_thread_and_welcome_check(self):
        """Receiving starts before the main thread waits for the welcome."""
        hooks = MagicMock()
        with patch.object(self.client, "_connect", hooks.connect), patch.object(
            self.client, "_start_receive_thread", hooks.start
        ), patch.object(self.client, "_check_connection", hooks.check):
            self.client.run()
        self.assertEqual(hooks.mock_calls, [call.connect(), call.start(), call.check()])

    def test_secure_setup_preserves_options_and_resets_session(self):
        """TLS reconnect uses configured credentials and fresh queues and identity."""
        old_uuid = self.client.uuid
        old_send_queue = self.client.send_queue
        self.client.server_session_id = "old-session"
        credentials = MagicMock(spec=grpc.ChannelCredentials)
        self.client.config.ssl_credentials = credentials
        self.client.config.receive_queue_maxsize = 3
        self.client.disconnect()
        with patch("pookiepy.baseclient.grpc.secure_channel") as secure, patch.object(
            self.client, "run"
        ) as run:
            self.client._setup_connection()  # pylint: disable=protected-access
        secure.assert_called_once_with(
            "localhost:50099", credentials, options=self.client.config.grpc_options
        )
        run.assert_called_once_with()
        self.assertNotEqual(self.client.uuid, old_uuid)
        self.assertIsNot(self.client.send_queue, old_send_queue)
        self.assertEqual(self.client.receive_queue.maxsize, 3)
        self.assertEqual(self.client.server_session_id, "")
        self.assertTrue(self.client.run_event.is_set())

    def test_connect_queues_identification_and_passes_stream_settings(self):
        """Handshake advertises routing lists with configured metadata and compression."""
        self.client.config.schema_version = "test-v1"
        self.client.config.ext_metadata = [("x-app", "test")]
        self.client.config.compression = grpc.Compression.Gzip
        self.client.stub = MagicMock()
        with patch("pookiepy.baseclient.grpc.channel_ready_future") as ready:
            self.client._connect()  # pylint: disable=protected-access
        ready.assert_called_once_with(self.channel)
        ready.return_value.result.assert_called_once_with(timeout=2.0)
        self.assertIs(self.client.stream, self.client.stub.DataChannel.return_value)
        settings = self.client.stub.DataChannel.call_args.kwargs
        self.assertEqual(settings["metadata"], [
            (SCHEMA_VERSION_METADATA_KEY, "test-v1"), ("x-app", "test")
        ])
        self.assertEqual(settings["compression"], grpc.Compression.Gzip)
        identification = self.client.send_queue.get_nowait().metaInfo.clientInfo
        self.assertEqual(identification.uuid, self.client.uuid)
        self.assertEqual(identification.name, self.client.name)
        self.assertEqual(list(identification.provides), ["topic"])
        self.assertEqual(list(identification.requires), ["reply"])

    def test_connect_timeout_discards_channel_and_raises_connection_error(self):
        """Failed readiness avoids closing a channel known to hang during cleanup."""
        with patch("pookiepy.baseclient.grpc.channel_ready_future") as ready:
            ready.return_value.result.side_effect = grpc.FutureTimeoutError()
            with self.assertRaisesRegex(GrpcConnectionError, "timeout"):
                self.client._connect()  # pylint: disable=protected-access
        self.assertIsNone(self.client.channel)

    def test_welcome_records_session_uuid(self):
        """A valid server welcome sets the connection's session identity."""
        welcome = message_pb2.PookieMessage(
            metaInfo=message_pb2.MetaInformation(
                serverInfo=message_pb2.ServerProvides(uuid="session-1")
            )
        )
        self.client.receive_queue.put(welcome)
        self.client._check_connection()  # pylint: disable=protected-access
        self.assertEqual(self.client.server_session_id, "session-1")

    def test_invalid_welcome_disconnects_and_preserves_error(self):
        """Timeout, fatal stream error, and data-before-welcome all disconnect."""
        original = GrpcConnectionError("denied")
        scenarios = (
            (queue.Empty(), GrpcConnectionError, "timeout"),
            (_StreamError(original), GrpcConnectionError, "denied"),
            (self.client.generate_message("reply", None, b"data"),
             GrpcConnectionError, "welcome message"),
        )
        for response, error_type, text in scenarios:
            with self.subTest(response=response), patch.object(
                self.client, "receive_queue"
            ) as responses, patch.object(self.client, "disconnect") as disconnect:
                if isinstance(response, Exception):
                    responses.get.side_effect = response
                else:
                    responses.get.return_value = response
                with self.assertRaisesRegex(error_type, text) as raised:
                    self.client._check_connection()  # pylint: disable=protected-access
                if isinstance(response, _StreamError):
                    self.assertIs(raised.exception, original)
                responses.get.assert_called_once_with(
                    timeout=self.client.config.connection_check_timeout
                )
                disconnect.assert_called_once_with()

    def test_receive_thread_targets_loop_and_starts_as_daemon(self):
        """The receive worker is a started daemon bound to this client."""
        with patch("pookiepy.baseclient.threading.Thread") as thread:
            self.client._start_receive_thread()  # pylint: disable=protected-access
        thread.assert_called_once_with(target=self.client._receive_loop, daemon=True)  # pylint: disable=protected-access
        thread.return_value.start.assert_called_once_with()
        self.client.receive_thread = None

    def test_disconnect_joins_live_receiver_and_closes_transport(self):
        """Disconnect cancels the stream and joins a live worker with bounded wait."""
        self.client.stream = MagicMock()
        self.client.receive_thread = MagicMock()
        self.client.receive_thread.is_alive.return_value = True
        self.client.disconnect()
        self.client.stream.cancel.assert_called_once_with()
        self.channel.close.assert_called_once_with()
        self.client.receive_thread.join.assert_called_once_with(timeout=TIMEOUT_WAIT_THREAD_JOIN)
        self.assertFalse(self.client.run_event.is_set())

    def test_context_manager_reconnects_only_after_disconnect(self):
        """A reusable context reconnects once and never suppresses application errors."""
        with patch.object(
            self.client, "_setup_connection", wraps=self.client._setup_connection  # pylint: disable=protected-access
        ) as setup, patch.object(self.client, "run"):
            with self.client as connected:
                self.assertIs(connected, self.client)
                setup.assert_not_called()
            self.assertFalse(self.client.run_event.is_set())
            with self.assertRaisesRegex(ValueError, "failure"):
                with self.client as reconnected:
                    self.assertIs(reconnected, self.client)
                    setup.assert_called_once_with()
                    raise ValueError("failure")
            self.assertFalse(self.client.run_event.is_set())

    def test_destructor_tolerates_partially_destroyed_client(self):
        """Interpreter teardown errors do not escape the destructor."""
        for error_type in (OSError, AttributeError, RuntimeError):
            with self.subTest(error_type=error_type), patch.object(
                self.client, "disconnect", side_effect=error_type("teardown")
            ):
                self.client.__del__()  # pylint: disable=unnecessary-dunder-call

    def test_custom_interface_defaults_to_empty_schema(self):
        """An explicitly injected interface does not use the bundled schema version."""
        with patch.object(BaseClient, "run"):
            client = BaseClient(
                port=50099, provides=["topic"], config=ClientConfig(),
                proto_interface=ProtoInterface(message_pb2, message_pb2_grpc),
            )
        self.addCleanup(client.disconnect)
        self.assertEqual(client.config.schema_version, "")
        self.assertEqual(str(client), repr(client))
