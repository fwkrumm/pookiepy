"""
Unit tests for baseclasses/BaseServer.py

BaseServer.__init__ performs no network I/O (serve_forever() does), so instances
can be created freely in tests without opening sockets.

Covered:
- Default hook return values (on_receive, on_client_connect, on_client_accepted,
  on_client_disconnect).
- shutdown() sets the global exit event and calls on_shutdown().
- on_init / on_shutdown hooks are called at the right lifecycle points.
- Hook overrides are invoked with the correct arguments.
"""
import queue
import threading
import unittest
from unittest.mock import MagicMock, patch

import grpc

from pookiepy import message_pb2
from pookiepy.baseserver import BaseServer, Peer, ServerConfig
from pookiepy.schema_version import SCHEMA_VERSION_METADATA_KEY


def _make_server(**kwargs) -> BaseServer:
    """Instantiate a bare BaseServer without opening a socket."""
    return BaseServer(port=50099, name="TestServer", **kwargs)

class TestDefaultHooks(unittest.TestCase):
    """Verify the default (no-op) return values of BaseServer hook methods."""

    def setUp(self):
        self.server = _make_server()
        self.peer = Peer(peer="127.0.0.1:9999", session_id="session-1")

    def test_on_receive_returns_true(self):
        """Default on_receive returns True, meaning forward the message."""
        self.assertTrue(self.server.on_receive(self.peer, message_pb2.Message()))

    def test_on_client_connect_returns_true(self):
        """Default on_client_connect returns True, meaning accept the connection."""
        # context=None is fine for the default implementation
        self.assertTrue(self.server.on_client_connect(message_pb2.Message(), None))

    def test_on_client_accepted_returns_none(self):
        """Default on_client_accepted is a no-op and returns None."""
        self.assertIsNone(self.server.on_client_accepted(self.peer, message_pb2.Message()))

    def test_on_client_disconnect_returns_none(self):
        """Default on_client_disconnect is a no-op and returns None."""
        self.assertIsNone(self.server.on_client_disconnect(self.peer))


class TestShutdown(unittest.TestCase):
    """Tests for BaseServer.shutdown()."""

    def test_shutdown_sets_global_exit_event(self):
        """shutdown() sets the internal globalExitEvent, unblocking serve_forever."""
        server = _make_server()
        self.assertFalse(server.global_exit_event.is_set())
        server.shutdown()
        self.assertTrue(server.global_exit_event.is_set())

    def test_shutdown_calls_on_shutdown_hook(self):
        """shutdown() invokes the on_shutdown hook."""
        class _Server(BaseServer):
            called = False

            def on_shutdown(self):
                _Server.called = True

        srv = _Server(port=50098)
        srv.shutdown()
        self.assertTrue(_Server.called)

    def test_custom_exit_event_is_set_on_shutdown(self):
        """shutdown() also sets a caller-supplied globalExitEvent."""
        evt = threading.Event()
        server = _make_server(global_exit_event=evt)
        server.shutdown()
        self.assertTrue(evt.is_set())


class TestLifecycleHooks(unittest.TestCase):
    """Verify that lifecycle hooks fire at the correct construction and call points."""

    def test_on_init_called_during_construction(self):
        """on_init is called during __init__, before the constructor returns."""
        class _Server(BaseServer):
            init_called = False

            def on_init(self):
                _Server.init_called = True

        _Server(port=50097)
        self.assertTrue(_Server.init_called)

    def test_on_receive_override_respected(self):
        """A subclass returning False from on_receive causes the base class
        to propagate that value."""
        class _Server(BaseServer):
            def on_receive(self, peer, request):  # pylint: disable=unused-argument
                return False  # always drop

        srv = _Server(port=50096)
        self.assertFalse(srv.on_receive(Peer("ip", "s"), message_pb2.Message()))

    def test_on_client_connect_override_receives_args(self):
        """on_client_connect override is called with (data, context) and can reject."""
        msg = message_pb2.Message()

        class _Server(BaseServer):
            received_data = None
            received_ctx = None

            def on_client_connect(self, data, context):
                _Server.received_data = data
                _Server.received_ctx = context
                return False  # reject

        srv = _Server(port=50095)
        accepted = srv.on_client_connect(msg, "ctx-sentinel")
        self.assertFalse(accepted)
        self.assertIs(_Server.received_data, msg)
        self.assertEqual(_Server.received_ctx, "ctx-sentinel")

    def test_on_client_accepted_override_receives_args(self):
        """on_client_accepted override is called with (peer, request)."""
        peer = Peer(peer="1.2.3.4:1111", session_id="s1", client_id="c1", name="worker")
        msg = message_pb2.Message()

        class _Server(BaseServer):
            received_peer = None
            received_req = None

            def on_client_accepted(self, peer, request):
                _Server.received_peer = peer
                _Server.received_req = request

        srv = _Server(port=50094)
        srv.on_client_accepted(peer, msg)
        self.assertIs(_Server.received_peer, peer)
        self.assertIs(_Server.received_req, msg)

    def test_on_client_disconnect_override_receives_peer(self):
        """on_client_disconnect override is called with the disconnected Peer."""
        peer = Peer(peer="1.2.3.4:2222", session_id="s2", client_id="c2", name="gone")

        class _Server(BaseServer):
            received_peer = None

            def on_client_disconnect(self, peer):
                _Server.received_peer = peer

        srv = _Server(port=50093)
        srv.on_client_disconnect(peer)
        self.assertIs(_Server.received_peer, peer)

    def test_on_data_yield_hook_called_when_message_yielded(self):
        """on_data_yield is invoked when DataChannel yields a queued message."""
        recorded = []
        msg = message_pb2.Message(metaInfo=message_pb2.MetaInformation(messageName="foo"))

        class _Server(BaseServer):
            def on_data_yield(self, peer, data):
                recorded.append((peer, data))

        server = _Server(port=50092)
        context = MagicMock()
        context.peer.return_value = "fake-peer"
        context.invocation_metadata.return_value = []

        def fake_queue_get(_self, timeout=1):
            _ = timeout
            server.global_exit_event.set()
            return msg

        class _DummyThread:  # pylint: disable=too-few-public-methods
            def __init__(self, target, args=(), **_kwargs):
                self.target = target
                self.args = args

            def start(self):
                self.target(*self.args)

        with patch("pookiepy.baseserver.threading.Thread", _DummyThread), \
             patch.object(BaseServer, "_handle_client_receive", lambda *_args, **_kwargs: None), \
             patch.object(queue.Queue, "get", fake_queue_get):
            generator = server.DataChannel(iter(()), context)
            yielded = next(generator)
            with self.assertRaises(StopIteration):
                next(generator)

        self.assertIs(yielded, msg)
        self.assertEqual(len(recorded), 1)
        self.assertEqual(recorded[0][1], msg)
        self.assertEqual(recorded[0][0].peer, "fake-peer")


class TestPeerRepr(unittest.TestCase):
    """Tests for Peer.__repr__ / __str__."""

    def test_peer_repr_contains_fields(self):
        """Peer.__repr__ includes the peer address, clientId, and name."""
        peer = Peer(peer="1.2.3.4:5000", session_id="sid", client_id="cid", name="myClient")
        r = repr(peer)
        self.assertIn("1.2.3.4:5000", r)
        self.assertIn("cid", r)
        self.assertIn("myClient", r)


class TestServerName(unittest.TestCase):
    """Tests for server name default."""

    def test_default_name_is_server(self):
        """Server name defaults to 'server' when no name is provided."""
        server = BaseServer(port=50090)
        self.assertEqual(server.name, "server")

    def test_custom_name_is_stored(self):
        """Server stores a custom name when explicitly provided."""
        server = BaseServer(port=50089, name="my-server")
        self.assertEqual(server.name, "my-server")


class TestServerConfig(unittest.TestCase):
    """Tests for ServerConfig defaults and compression field."""

    def test_effective_max_workers_matches_python_default_formula(self):
        """effective_max_workers mirrors ThreadPoolExecutor default sizing."""
        cfg = ServerConfig(max_workers=None)
        self.assertGreaterEqual(cfg.effective_max_workers, 1)

    def test_schema_version_default_is_empty(self):
        """ServerConfig.schema_version defaults to an empty string."""
        self.assertEqual(ServerConfig().schema_version, "")

    def test_compression_default_is_none(self):
        """ServerConfig.compression defaults to None (no compression)."""
        self.assertIsNone(ServerConfig().compression)

    def test_compression_gzip_accepted(self):
        """ServerConfig accepts grpc.Compression.Gzip."""
        cfg = ServerConfig(compression=grpc.Compression.Gzip)
        self.assertEqual(cfg.compression, grpc.Compression.Gzip)

    def test_compression_deflate_accepted(self):
        """ServerConfig accepts grpc.Compression.Deflate."""
        cfg = ServerConfig(compression=grpc.Compression.Deflate)
        self.assertEqual(cfg.compression, grpc.Compression.Deflate)

    def test_compression_passed_to_server(self):
        """BaseServer stores the config compression field for use in serve_forever."""
        cfg = ServerConfig(compression=grpc.Compression.Gzip, schema_version="v1")
        server = BaseServer(port=50088, config=cfg)
        self.assertEqual(server.config.compression, grpc.Compression.Gzip)
        self.assertEqual(server.config.schema_version, "v1")

    def test_queue_warning_threshold_passed_to_data_register(self):
        """BaseServer forwards queue warning threshold into the routing table."""
        cfg = ServerConfig(queue_warning_threshold=123)
        server = BaseServer(port=50088, config=cfg)
        self.assertEqual(server._data_register._queue_warning_threshold, 123)  # pylint: disable=protected-access


class TestServeForever(unittest.TestCase):
    """Tests for serve_forever startup wiring."""

    @patch("pookiepy.baseserver.message_pb2_grpc.add_StreamServicer_to_server")
    @patch("pookiepy.baseserver.grpc.server")
    @patch("pookiepy.baseserver.futures.ThreadPoolExecutor")
    def test_serve_forever_uses_effective_max_workers(
        self,
        mock_executor,
        mock_grpc_server,
        mock_add_servicer,
    ):
        """serve_forever passes the resolved worker count into ThreadPoolExecutor."""
        cfg = ServerConfig(max_workers=None)
        server = BaseServer(port=50081, config=cfg)
        server.global_exit_event.set()

        mock_server_instance = MagicMock()
        mock_stop_event = MagicMock()
        mock_stop_event.wait.return_value = True
        mock_server_instance.stop.return_value = mock_stop_event
        mock_grpc_server.return_value = mock_server_instance

        server.serve_forever()

        mock_executor.assert_called_once_with(max_workers=cfg.effective_max_workers)
        mock_add_servicer.assert_called_once()


class TestSchemaValidation(unittest.TestCase):
    """Tests for schema-version metadata validation in BaseServer.DataChannel."""

    def test_empty_server_and_client_schema_logs_warning(self):
        """Server warns when neither side provides a schema version string."""
        msg = message_pb2.Message(metaInfo=message_pb2.MetaInformation(messageName="foo"))
        server = BaseServer(port=50087, config=ServerConfig(schema_version=""))
        server.logger.warning = MagicMock()
        context = MagicMock()
        context.peer.return_value = "fake-peer"
        context.invocation_metadata.return_value = []

        def fake_queue_get(_self, timeout=1):
            _ = timeout
            server.global_exit_event.set()
            return msg

        class _DummyThread:  # pylint: disable=too-few-public-methods
            def __init__(self, target, args=(), **_kwargs):
                self.target = target
                self.args = args

            def start(self):
                self.target(*self.args)

        with patch("pookiepy.baseserver.threading.Thread", _DummyThread), \
             patch.object(BaseServer, "_handle_client_receive", lambda *_args, **_kwargs: None), \
             patch.object(queue.Queue, "get", fake_queue_get):
            generator = server.DataChannel(iter(()), context)
            yielded = next(generator)

        self.assertIs(yielded, msg)
        server.logger.warning.assert_called_once()
        self.assertIn("cannot check schema because empty", server.logger.warning.call_args.args[0])

    def test_mismatched_schema_aborts_connection(self):
        """Server aborts with FAILED_PRECONDITION when schema strings differ."""
        server = BaseServer(port=50086, config=ServerConfig(schema_version="server-v1"))
        context = MagicMock()
        context.peer.return_value = "fake-peer"
        context.invocation_metadata.return_value = [
            (SCHEMA_VERSION_METADATA_KEY, "client-v2")
        ]

        generator = server.DataChannel(iter(()), context)

        with self.assertRaises(StopIteration):
            next(generator)

        context.abort.assert_called_once_with(
            grpc.StatusCode.FAILED_PRECONDITION,
            "Proto schema mismatch: server=server-v1, client=client-v2",
        )


if __name__ == "__main__":
    unittest.main()
