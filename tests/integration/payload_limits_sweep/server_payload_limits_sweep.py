"""Payload-limit sweep integration test -- server.

Server is started with configurable gRPC max send/receive message lengths.
It relays messages unchanged; benchmarking logic lives in the client script.
"""

import argparse

from grpchook.baseserver import ServerConfig
from tests.integration._server_base import IntegrationServer


def _build_server_options(
    max_send_bytes: int,
    max_receive_bytes: int,
) -> list[tuple[str, int | bool]]:
    """Return default server options extended with explicit message-size caps."""
    options = list(ServerConfig().server_options)
    options.extend([
        ("grpc.max_send_message_length", max_send_bytes),
        ("grpc.max_receive_message_length", max_receive_bytes),
    ])
    return options


class PayloadLimitServer(IntegrationServer):
    """Relay server configured with explicit gRPC message-size limits."""

    def __init__(self, port: int, max_send_bytes: int, max_receive_bytes: int):
        cfg = ServerConfig(
            server_options=_build_server_options(max_send_bytes, max_receive_bytes)
        )
        super().__init__(port, config=cfg)


def _parse_args() -> argparse.Namespace:
    """Parse CLI args for server port and message-size limits."""
    parser = argparse.ArgumentParser(description="Payload-limit sweep -- server")
    parser.add_argument("--port", type=int, required=True, help="Server port")
    parser.add_argument(
        "--max-send-message-length",
        type=int,
        required=True,
        help="grpc.max_send_message_length in bytes",
    )
    parser.add_argument(
        "--max-receive-message-length",
        type=int,
        required=True,
        help="grpc.max_receive_message_length in bytes",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    server = PayloadLimitServer(
        port=args.port,
        max_send_bytes=args.max_send_message_length,
        max_receive_bytes=args.max_receive_message_length,
    )
    server.serve_forever()
