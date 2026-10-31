"""Loopback regression coverage for admission control and stream shutdown."""

import queue
import threading
import unittest
from unittest.mock import MagicMock

import grpc

from pookiepy import message_pb2, message_pb2_grpc
from pookiepy.baseclient import BaseClient, ClientConfig
from pookiepy.baseserver import BaseServer, ServerConfig
from pookiepy.exceptions import GrpcResourceExhaustedError
from pookiepy.schema_version import DEFAULT_SCHEMA_VERSION, SCHEMA_VERSION_METADATA_KEY


class TestServerLifecycle(unittest.TestCase):
    """Use ephemeral ports and bounded waits without external services."""

    def test_overload_slot_reuse_and_shutdown(self):
        """Excess clients fail promptly; finished streams release workers and capacity."""
        # Keep setup and teardown together for this single transport lifecycle scenario.
        # pylint: disable=too-many-locals,too-many-statements,protected-access
        server = BaseServer(port=0, ip="127.0.0.1", config=ServerConfig(max_workers=1))
        server.logger = MagicMock()
        server.on_shutdown = MagicMock()
        started = threading.Event()
        port = []
        errors = []
        receive_workers = []
        original_receive = server._handle_client_receive

        def record_receive(*args):
            receive_workers.append(threading.current_thread())
            original_receive(*args)

        server._handle_client_receive = record_receive

        def record_start(_message, _server, bound_port, _schema):
            port.append(bound_port)
            started.set()

        server.logger.info.side_effect = record_start

        def serve():
            try:
                server.serve_forever()
            except Exception as exc:  # pylint: disable=broad-exception-caught
                errors.append(exc)
                started.set()

        worker = threading.Thread(target=serve, daemon=True)
        worker.start()
        channel = None
        streams = []
        request_queues = []
        try:
            self.assertTrue(started.wait(5), "server did not start")
            self.assertEqual(errors, [])
            channel = grpc.insecure_channel(f"127.0.0.1:{port[0]}")
            grpc.channel_ready_future(channel).result(timeout=5)
            stub = message_pb2_grpc.StreamStub(channel)

            def open_stream(client_id):
                requests = queue.Queue()
                request_queues.append(requests)

                def generate():
                    yield message_pb2.PookieMessage(
                        metaInfo=message_pb2.MetaInformation(
                            clientInfo=message_pb2.ClientProvides(
                                uuid=client_id, requires=["topic"]
                            )
                        )
                    )
                    while True:
                        request = requests.get()
                        if request is None:
                            return
                        yield request

                stream = stub.DataChannel(
                    generate(), timeout=10,
                    metadata=[(SCHEMA_VERSION_METADATA_KEY, DEFAULT_SCHEMA_VERSION)],
                )
                streams.append(stream)
                self.assertEqual(next(stream).metaInfo.serverInfo.name, server.name)
                return stream, requests

            first, requests = open_stream("first")
            with self.assertRaises(GrpcResourceExhaustedError):
                BaseClient(
                    port=port[0], ip="127.0.0.1", name="overloaded", requires=["topic"],
                    config=ClientConfig(connection_check_timeout=2),
                )

            requests.put_nowait(None)
            self.assertEqual(list(first), [])
            self.assertEqual(first.code(), grpc.StatusCode.OK)
            second, _ = open_stream("second")
            server.shutdown()
            try:
                list(second)
            except grpc.RpcError as exc:
                self.assertIn(exc.code(), (grpc.StatusCode.CANCELLED, grpc.StatusCode.UNAVAILABLE))
        finally:
            for requests in request_queues:
                requests.put_nowait(None)
            for stream in streams:
                stream.cancel()
            if channel is not None:
                channel.close()
            server.shutdown()
            worker.join(timeout=5)
            for receiver in receive_workers:
                receiver.join(timeout=2)

        self.assertFalse(worker.is_alive(), "server worker leaked")
        self.assertTrue(all(not receiver.is_alive() for receiver in receive_workers))
        self.assertEqual(errors, [])
        server.on_shutdown.assert_called_with()
        self.assertEqual(server._data_register.remove_notification_queues_for_client("second"), 0)
