"""Loopback regression coverage for admission control and stream shutdown."""

import queue
import threading

import grpc

from pookiepy import message_pb2, message_pb2_grpc
from pookiepy.baseclient import BaseClient, ClientConfig
from pookiepy.baseserver import BaseServer, ServerConfig
from pookiepy.exceptions import GrpcResourceExhaustedError
from pookiepy.schema_version import DEFAULT_SCHEMA_VERSION, SCHEMA_VERSION_METADATA_KEY
from tests.integration._interface import get_args


class LifecycleServer(BaseServer):
    """Record receive workers and shutdown hooks for lifecycle assertions."""

    def on_init(self):
        """Initialize lifecycle observations before serving clients."""
        self.receive_workers = []
        self.shutdown_calls = 0

    def _handle_client_receive(self, *args):
        self.receive_workers.append(threading.current_thread())
        super()._handle_client_receive(*args)

    def on_shutdown(self):
        """Record each shutdown hook invocation."""
        self.shutdown_calls += 1


def main():
    """Excess clients fail promptly; finished streams release workers and capacity."""
    # pylint: disable=too-many-locals,too-many-statements,protected-access
    args = get_args("Server lifecycle integration test")
    server = LifecycleServer(
        port=args.port, ip=args.ip, config=ServerConfig(max_workers=1)
    )
    errors = []

    def serve():
        try:
            server.serve_forever()
        except Exception as exc:  # pylint: disable=broad-exception-caught
            errors.append(exc)

    worker = threading.Thread(target=serve, daemon=True)
    worker.start()
    channel = None
    streams = []
    request_queues = []
    try:
        channel = grpc.insecure_channel(f"{args.ip}:{args.port}")
        grpc.channel_ready_future(channel).result(timeout=args.timeout)
        assert not errors, errors
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
            assert next(stream).metaInfo.serverInfo.name == server.name
            return stream, requests

        first, requests = open_stream("first")
        try:
            overloaded = BaseClient(
                port=args.port, ip=args.ip, name="overloaded", requires=["topic"],
                config=ClientConfig(connection_check_timeout=2),
            )
        except GrpcResourceExhaustedError:
            pass
        else:
            overloaded.disconnect()
            raise AssertionError("excess client was accepted")

        requests.put_nowait(None)
        assert list(first) == []
        assert first.code() == grpc.StatusCode.OK
        second, _ = open_stream("second")
        server.shutdown()
        try:
            list(second)
        except grpc.RpcError as exc:
            assert exc.code() in (grpc.StatusCode.CANCELLED, grpc.StatusCode.UNAVAILABLE)
    finally:
        for requests in request_queues:
            requests.put_nowait(None)
        for stream in streams:
            stream.cancel()
        if channel is not None:
            channel.close()
        server.shutdown()
        worker.join(timeout=5)
        for receiver in server.receive_workers:
            receiver.join(timeout=2)

    assert not worker.is_alive(), "server worker leaked"
    assert all(not receiver.is_alive() for receiver in server.receive_workers)
    assert not errors, errors
    assert server.shutdown_calls > 0, "shutdown hook was not called"
    assert server._data_register.remove_notification_queues_for_client("second") == 0
    print("Server lifecycle integration test passed.")


if __name__ == "__main__":
    main()