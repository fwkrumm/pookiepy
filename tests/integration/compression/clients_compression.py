"""Compression integration test -- clients.

Two clients exchange messages through a Gzip-compressed connection.
Both server and client enable grpc.Compression.Gzip so that both directions
are compressed. The test asserts that payload content survives compression
round-trips unchanged.

One-side-only note: if only one side sets compression, no exception is raised.
grpcio always has gzip/deflate decompression codecs available. Only the
sending side's outgoing messages are compressed; the other direction is plain.
"""
import grpc
from grpchook.baseclient import BaseClient, ClientConfig
from grpchook.tools import generate_message
from grpchook import message_pb2
from tests.integration._interface import get_args

TIMEOUT = 2  # seconds


class CompressionClient(BaseClient):
    """gRPC client with Gzip compression enabled on outgoing messages."""

    def __init__(self, name: str, port: int):
        cfg = ClientConfig(compression=grpc.Compression.Gzip)
        super().__init__(
            port,
            name=name,
            provides=["compressed_message", "server-exit"],
            requires=["compressed_message"],
            config=cfg,
        )
        self.logger.info("initialized CompressionClient with Gzip compression")

    def on_receive(self, data: message_pb2.Message):
        if self.name == "client1":
            assert data.payload.bytePayload == b"from-client2", (
                f"client1 expected b'from-client2', got {data.payload.bytePayload!r}"
            )
        elif self.name == "client2":
            assert data.payload.bytePayload == b"from-client1", (
                f"client2 expected b'from-client1', got {data.payload.bytePayload!r}"
            )


if __name__ == "__main__":
    args = get_args("Compression integration test -- clients")

    client1 = CompressionClient("client1", args.port)
    client2 = CompressionClient("client2", args.port)

    msg1 = generate_message(message_name="compressed_message", byte_payload=b"from-client1")
    msg2 = generate_message(message_name="compressed_message", byte_payload=b"from-client2")

    client1.send_data(msg1)
    client2.send_data(msg2)
    try:
        client1.spin(timeout=TIMEOUT)
        client2.spin(timeout=TIMEOUT)
    finally:
        client1.disconnect()
        client2.disconnect()

    # Second round via context manager to verify reconnect + compression still works
    with client1, client2:
        client1.send_data(generate_message(message_name="compressed_message",
                                           byte_payload=b"from-client1"))
        client2.send_data(generate_message(message_name="compressed_message",
                                           byte_payload=b"from-client2"))
        client1.spin(timeout=TIMEOUT)
        client2.spin(timeout=TIMEOUT)

        client1.send_data(generate_message(message_name="server-exit"))
        client1.wait_done()
