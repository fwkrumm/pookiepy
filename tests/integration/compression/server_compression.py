"""Compression integration test -- server.

Verifies that gRPC Gzip compression works end-to-end when enabled on both
the server (server->client direction) and the client (client->server direction).

Note: compression is per-direction. Enabling it only on the server compresses
server->client messages; client->server messages are still decompressed correctly
by the server regardless, because grpcio always registers gzip/deflate codecs.
No exception is raised when only one side enables compression.
"""
import grpc
from grpchook.baseserver import ServerConfig
from tests.integration._interface import get_args
from tests.integration._server_base import IntegrationServer


class CompressionServer(IntegrationServer):
    """Integration test server with Gzip compression enabled on outgoing messages."""

    def __init__(self, port: int):
        cfg = ServerConfig(compression=grpc.Compression.Gzip)
        super().__init__(port, config=cfg)
        self.logger.info("initialized CompressionServer with Gzip compression")


if __name__ == "__main__":
    args = get_args("Compression integration test -- server")
    s = CompressionServer(args.port)
    s.serve_forever()
