"""Deterministic server streaming and transport lifecycle edge cases."""

import queue
import unittest
from unittest.mock import MagicMock, patch

import grpc

from pookiepy import message_pb2, message_pb2_grpc
from pookiepy.baseserver import BaseServer, Peer, ServerConfig
from pookiepy.custom_interface import ProtoInterface
from pookiepy.schema_version import DEFAULT_SCHEMA_VERSION, SCHEMA_VERSION_METADATA_KEY


class TestStreamLifecycle(unittest.TestCase):
    """Verify queued data and disconnect cleanup without launching worker threads."""

    def test_history_hook_failure_queue_retry_and_disconnect_cleanup(self):
        """Hook failures do not drop data; idle polls and generator close clean up."""
        server = BaseServer(port=50099, config=ServerConfig(max_workers=1))
        server.logger = MagicMock()
        server.on_data_yield = MagicMock(side_effect=ValueError("yield hook failed"))
        server.on_client_disconnect = MagicMock()
        data = server.generate_message("topic", None, b"payload")
        data.history.add(name="server", perfCounter=10.0)
        context = MagicMock()
        context.peer.return_value = "fake-peer"
        context.invocation_metadata.return_value = [
            (SCHEMA_VERSION_METADATA_KEY, DEFAULT_SCHEMA_VERSION)
        ]
        with patch("pookiepy.baseserver.threading.Thread") as worker, patch(
            "pookiepy.baseserver.queue.Queue"
        ) as notifications, patch.object(server, "_data_register") as register, patch(
            "pookiepy.baseserver.time.perf_counter", return_value=12.5
        ):
            notifications.return_value.get.side_effect = [queue.Empty, data]
            stream = server.DataChannel(iter(()), context)
            self.assertIs(next(stream), data)
            peer = server.on_data_yield.call_args.args[0]
            self.assertEqual(data.history[-1].perfCounter, 2.5)
            self.assertTrue(data.history[-1].HasField("sendTimestamp"))
            self.assertEqual(notifications.return_value.get.call_count, 2)
            server.logger.warning.assert_called_once()
            self.assertIn("reached max_workers", server.logger.warning.call_args.args[0])
            server.logger.error.assert_called_once()
            worker.return_value.start.assert_called_once()
            stream.close()
            register.remove_notification_queues_for_client.assert_called_once_with("")
            server.on_client_disconnect.assert_called_once_with(peer)
            self.assertEqual(server._BaseServer__connected_clients, 0)  # pylint: disable=protected-access

    def test_rejected_schema_releases_connection_capacity(self):
        """An aborting gRPC context leaves no connected client or worker behind."""
        server = BaseServer(port=50099)
        server.logger = MagicMock()
        context = MagicMock()
        context.invocation_metadata.return_value = []
        context.abort.side_effect = grpc.RpcError("schema mismatch")
        with patch("pookiepy.baseserver.threading.Thread") as worker:
            with self.assertRaises(grpc.RpcError):
                next(server.DataChannel(iter(()), context))
        worker.assert_not_called()
        self.assertEqual(server._BaseServer__connected_clients, 0)  # pylint: disable=protected-access


class TestTransportShutdown(unittest.TestCase):
    """Verify TLS setup, polling, interrupts, and resource release with mocked gRPC."""

    def test_startup_and_shutdown_hook_failures_release_resources(self):
        """Executor and transport cleanup run even when startup or user cleanup fails."""
        for failure in ("start", "hook", "bind"):
            with self.subTest(failure=failure):
                server = BaseServer(port=50099)
                server.logger = MagicMock()
                server.global_exit_event.set()
                with patch("pookiepy.baseserver.futures.ThreadPoolExecutor") as executor, patch(
                    "pookiepy.baseserver.grpc.server"
                ) as transport, patch.object(message_pb2_grpc, "add_StreamServicer_to_server"):
                    transport.return_value.add_insecure_port.return_value = (
                        0 if failure == "bind" else 50099
                    )
                    if failure == "start":
                        transport.return_value.start.side_effect = RuntimeError("start failed")
                    elif failure == "hook":
                        server.on_shutdown = MagicMock(side_effect=RuntimeError("hook failed"))
                    with self.assertRaises(RuntimeError):
                        server.serve_forever()
                    transport.return_value.stop.assert_called_once_with(grace=None)
                    executor.return_value.shutdown.assert_called_once_with(wait=True)

    def test_tls_interrupt_and_stop_timeout_release_resources(self):
        """Keyboard interruption stops TLS transport and joins its worker executor."""
        exit_event = MagicMock()
        exit_event.is_set.return_value = False
        exit_event.wait.side_effect = KeyboardInterrupt
        credentials = MagicMock(spec=grpc.ServerCredentials)
        config = ServerConfig(
            max_workers=3, max_queue_elements=2, shutdown_poll_interval=0.25,
            compression=grpc.Compression.Gzip,
        )
        server = BaseServer(
            port=50099, ip="localhost", config=config,
            ssl_credentials=credentials, global_exit_event=exit_event,
        )
        server.logger = MagicMock()
        server.on_shutdown = MagicMock()
        with patch("pookiepy.baseserver.futures.ThreadPoolExecutor") as executor, patch(
            "pookiepy.baseserver.grpc.server"
        ) as transport, patch.object(message_pb2_grpc, "add_StreamServicer_to_server"):
            transport.return_value.stop.return_value.wait.return_value = False
            server.serve_forever()

        executor.assert_called_once_with(max_workers=3)
        transport.assert_called_once_with(
            executor.return_value, options=config.server_options, compression=config.compression,
            maximum_concurrent_rpcs=3,
        )
        transport.return_value.add_secure_port.assert_called_once_with(
            "localhost:50099", credentials
        )
        transport.return_value.add_insecure_port.assert_not_called()
        transport.return_value.start.assert_called_once_with()
        exit_event.wait.assert_called_once_with(timeout=0.25)
        exit_event.set.assert_called_once_with()
        server.on_shutdown.assert_called_once_with()
        transport.return_value.stop.assert_called_once_with(grace=None)
        transport.return_value.stop.return_value.wait.assert_called_once_with(timeout=10)
        server.logger.warning.assert_called_once()
        executor.return_value.shutdown.assert_called_once_with(wait=True)

    def test_shutdown_with_already_set_event_still_calls_hook(self):
        """Repeated shutdown preserves the exit signal and invokes the lifecycle hook."""
        server = BaseServer(port=50099)
        server.logger = MagicMock()
        server.on_shutdown = MagicMock()
        server.global_exit_event.set()
        server.shutdown()
        server.shutdown()
        self.assertTrue(server.global_exit_event.is_set())
        self.assertEqual(server.on_shutdown.call_count, 2)
        self.assertEqual(server.logger.idebug.call_count, 2)


class TestServerConfigurationEdges(unittest.TestCase):
    """Cover explicit interface defaults and inspectable server identities."""

    def test_queue_bound_remains_opt_in(self):
        """Default buffering remains unbounded; finite queue limits are explicit."""
        self.assertEqual(ServerConfig().max_queue_elements, 0)
        self.assertEqual(ServerConfig(max_queue_elements=1024).max_queue_elements, 1024)

    def test_worker_default_uses_process_cpu_availability(self):
        """CPU affinity-aware counts control both executor size and admission capacity."""
        with patch("pookiepy.baseserver.os.process_cpu_count", return_value=2, create=True), patch(
            "pookiepy.baseserver.os.cpu_count", return_value=64
        ):
            self.assertEqual(ServerConfig().effective_max_workers, 6)
            self.assertEqual(ServerConfig(max_workers=10).effective_max_workers, 10)

    def test_injected_interface_without_schema_defaults_to_empty(self):
        """Injected interfaces do not inherit the bundled schema version."""
        interface = ProtoInterface(message_pb2, message_pb2_grpc)
        server = BaseServer(port=50099, proto_interface=interface)
        self.assertEqual(server.config.schema_version, "")

    def test_string_representations_match_repr(self):
        """Server and peer string forms expose the same identity as their repr."""
        server = BaseServer(port=50099)
        peer = Peer("fake-peer", "session-1", "client-1", "worker")
        self.assertEqual(str(server), repr(server))
        self.assertEqual(str(peer), repr(peer))
