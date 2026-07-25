"""
Unit tests for baseclasses/BaseClient.py

BaseClient.__init__ always calls self.run(), which opens a real gRPC channel.
To keep tests pure-unit (no network), run() is patched to a no-op.  All
gRPC-facing setup (channel, stub) that happens before run() is lazy and does
not cause I/O --- so only the patched run() call needs suppressing.

Covered:
- send_data: type guard, provides-list guard, happy-path enqueue.
- get_data: returns message from queue, timeout=0 on empty queue, ClientExit
  when run_event is cleared.
- disconnect: clears run_event, is idempotent, cancels the stream.
- on_receive / on_shutdown hooks: default behaviour and override.
- ClientConfig defaults.
"""
import queue
import threading
import unittest
from unittest.mock import MagicMock, patch

import grpc

from grpchook import message_pb2
from grpchook.baseclient import BaseClient, ClientConfig
from grpchook.exceptions import ClientExit, GrpcValueError


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _client(provides=None, requires=None) -> BaseClient:
    """Return a BaseClient with run() suppressed --- no network required."""
    with patch.object(BaseClient, "run", lambda self: None):
        c = BaseClient(
            name="test",
            port=50099,
            provides=provides if provides is not None else ["foo"],
            requires=requires if requires is not None else [],
        )
    # Replace the real lazy gRPC channel with a mock so close() is a no-op.
    c.channel = MagicMock()
    return c


# ---------------------------------------------------------------------------
# send_data
# ---------------------------------------------------------------------------

class TestSendData(unittest.TestCase):
    """Tests for BaseClient.send_data()."""

    def setUp(self):
        self.client = _client(provides=["foo"])

    def test_valid_message_is_enqueued(self):
        """A valid Message with a declared messageName is placed on the send queue."""
        msg = message_pb2.Message(
            metaInfo=message_pb2.MetaInformation(messageName="foo")
        )
        self.client.send_data(msg)
        self.assertEqual(self.client.send_queue.qsize(), 1)

    def test_wrong_type_raises_grpc_value_error(self):
        """Passing a non-Message raises GrpcValueError."""
        with self.assertRaises(GrpcValueError):
            self.client.send_data("not-a-message")

    def test_message_name_not_in_provides_raises(self):
        """A messageName absent from the provides list raises GrpcValueError."""
        msg = message_pb2.Message(
            metaInfo=message_pb2.MetaInformation(messageName="unknown")
        )
        with self.assertRaises(GrpcValueError):
            self.client.send_data(msg)

    def test_multiple_messages_all_enqueued(self):
        """Multiple consecutive send_data calls all land on the send queue."""
        for _ in range(5):
            msg = message_pb2.Message(
                metaInfo=message_pb2.MetaInformation(messageName="foo")
            )
            self.client.send_data(msg)
        self.assertEqual(self.client.send_queue.qsize(), 5)


# ---------------------------------------------------------------------------
# get_data
# ---------------------------------------------------------------------------

class TestGetData(unittest.TestCase):
    """Tests for BaseClient.get_data()."""

    def setUp(self):
        self.client = _client()

    def test_returns_message_when_available(self):
        """Returns the first Message waiting in the receive queue."""
        msg = message_pb2.Message(
            metaInfo=message_pb2.MetaInformation(messageName="foo")
        )
        self.client.receive_queue.put(msg)
        result = self.client.get_data(timeout=1.0)
        self.assertIs(result, msg)

    def test_timeout_zero_raises_empty_on_empty_queue(self):
        """timeout=0 on an empty queue raises queue.Empty immediately."""
        with self.assertRaises(queue.Empty):
            self.client.get_data(timeout=0)

    def test_timeout_zero_returns_message_when_available(self):
        """timeout=0 returns a waiting message without blocking."""
        msg = message_pb2.Message()
        self.client.receive_queue.put(msg)
        result = self.client.get_data(timeout=0)
        self.assertIs(result, msg)

    def test_raises_client_exit_when_run_event_cleared(self):
        """ClientExit is raised when run_event is cleared before a message arrives."""
        self.client.run_event.clear()
        with self.assertRaises(ClientExit):
            self.client.get_data()

    def test_raises_queue_empty_on_deadline_exceeded(self):
        """queue.Empty is raised when the timeout expires with no message available."""
        with self.assertRaises(queue.Empty):
            self.client.get_data(timeout=0.05)


# ---------------------------------------------------------------------------
# disconnect
# ---------------------------------------------------------------------------

class TestDisconnect(unittest.TestCase):
    """Tests for BaseClient.disconnect()."""

    def setUp(self):
        self.client = _client()

    def test_disconnect_clears_run_event(self):
        """disconnect() clears run_event, stopping the send and receive loops."""
        self.assertTrue(self.client.run_event.is_set())
        self.client.disconnect()
        self.assertFalse(self.client.run_event.is_set())

    def test_disconnect_idempotent(self):
        """Calling disconnect() twice does not raise."""
        self.client.disconnect()
        self.client.disconnect()  # must not raise

    def test_disconnect_cancels_stream(self):
        """disconnect() cancels the active gRPC stream."""
        mock_stream = MagicMock()
        self.client.stream = mock_stream
        self.client.disconnect()
        mock_stream.cancel.assert_called_once()

    def test_already_disconnected_stream_not_cancelled_again(self):
        """A second disconnect() does not cancel the stream again."""
        mock_stream = MagicMock()
        self.client.stream = mock_stream
        self.client.disconnect()
        call_count = mock_stream.cancel.call_count
        self.client.disconnect()  # second call --- run_event already cleared
        self.assertEqual(mock_stream.cancel.call_count, call_count)


# ---------------------------------------------------------------------------
# Hooks
# ---------------------------------------------------------------------------

class TestHooks(unittest.TestCase):
    """Tests for BaseClient hook methods (on_receive, on_shutdown)."""

    def test_on_receive_default_returns_true(self):
        """Default on_receive logs a warning and returns True."""
        client = _client()
        result = client.on_receive(message_pb2.Message())
        self.assertTrue(result)

    def test_on_receive_override_called_by_spin(self):
        """spin() calls the overridden on_receive with the dequeued Message."""
        received: list[message_pb2.Message] = []

        class _Client(BaseClient):
            def on_receive(self, data):
                received.append(data)
                return True

        with patch.object(BaseClient, "run", lambda self: None):
            client = _Client(name="hook-test", port=50099, provides=["foo"])
        client.channel = MagicMock()

        msg = message_pb2.Message(metaInfo=message_pb2.MetaInformation(messageName="foo"))
        client.receive_queue.put(msg)
        client.spin()

        self.assertEqual(len(received), 1)
        self.assertIs(received[0], msg)

    def test_on_shutdown_hook_called_on_disconnect(self):
        """on_shutdown is called when disconnect() is invoked."""
        shutdown_called = threading.Event()

        class _Client(BaseClient):
            def on_shutdown(self):
                shutdown_called.set()

        with patch.object(BaseClient, "run", lambda self: None):
            client = _Client(name="shutdown-test", port=50099, provides=["foo"])
        client.channel = MagicMock()

        client.disconnect()
        self.assertTrue(shutdown_called.is_set())


# ---------------------------------------------------------------------------
# ClientConfig
# ---------------------------------------------------------------------------

class TestClientConfig(unittest.TestCase):
    """Tests for ClientConfig defaults and custom values."""

    def test_default_receive_queue_maxsize_is_zero(self):
        """Default receive_queue_maxsize is 0 (unlimited queue)."""
        self.assertEqual(ClientConfig().receive_queue_maxsize, 0)

    def test_default_connection_check_timeout_positive(self):
        """Default connection_check_timeout is a positive number."""
        self.assertGreater(ClientConfig().connection_check_timeout, 0)

    def test_custom_config_applied(self):
        """A custom ClientConfig is stored and applied on the client instance."""
        cfg = ClientConfig(receive_queue_maxsize=10, connection_check_timeout=1.0)
        with patch.object(BaseClient, "run", lambda self: None):
            client = BaseClient(name="cfg-test", port=50099, config=cfg)
        self.assertEqual(client.config.receive_queue_maxsize, 10)
        self.assertEqual(client.config.connection_check_timeout, 1.0)

    def test_ext_metadata_default_is_empty(self):
        """Default ext_metadata is an empty list."""
        self.assertEqual(ClientConfig().ext_metadata, [])

    def test_ext_metadata_passed_to_config(self):
        """ext_metadata set on ClientConfig is accessible on the client."""
        cfg = ClientConfig(ext_metadata=[("x-token", "abc")])
        with patch.object(BaseClient, "run", lambda self: None):
            client = BaseClient(name="meta-test", port=50099, config=cfg)
        self.assertEqual(client.config.ext_metadata, [("x-token", "abc")])

    def test_compression_default_is_none(self):
        """ClientConfig.compression defaults to None (no compression)."""
        self.assertIsNone(ClientConfig().compression)

    def test_compression_gzip_accepted(self):
        """ClientConfig accepts grpc.Compression.Gzip."""
        cfg = ClientConfig(compression=grpc.Compression.Gzip)
        self.assertEqual(cfg.compression, grpc.Compression.Gzip)

    def test_compression_deflate_accepted(self):
        """ClientConfig accepts grpc.Compression.Deflate."""
        cfg = ClientConfig(compression=grpc.Compression.Deflate)
        self.assertEqual(cfg.compression, grpc.Compression.Deflate)

    def test_compression_passed_to_client(self):
        """BaseClient stores the config compression field for use in _connect."""
        cfg = ClientConfig(compression=grpc.Compression.Gzip)
        with patch.object(BaseClient, "run", lambda self: None):
            client = BaseClient(name="comp-test", port=50099, config=cfg)
        self.assertEqual(client.config.compression, grpc.Compression.Gzip)


# ---------------------------------------------------------------------------
# Identifier default
# ---------------------------------------------------------------------------

class TestClientNameDefault(unittest.TestCase):
    """Tests for the client name default value."""

    def test_explicit_name_is_stored(self):
        """An explicitly provided name is stored as-is."""
        with patch.object(BaseClient, "run", lambda self: None):
            c = BaseClient(port=50099, name="my-client", provides=["foo"])
        self.assertEqual(c.name, "my-client")

    def test_empty_name_is_stored_as_is(self):
        """An empty name string is stored as-is (no silent fallback)."""
        with patch.object(BaseClient, "run", lambda self: None):
            c = BaseClient(port=50099, name="", provides=["foo"])
        self.assertEqual(c.name, "")

    def test_omitted_name_defaults_to_client(self):
        """Omitting name entirely falls back to the default 'client'."""
        with patch.object(BaseClient, "run", lambda self: None):
            c = BaseClient(port=50099, provides=["foo"])
        self.assertEqual(c.name, "client")


if __name__ == "__main__":
    unittest.main()
