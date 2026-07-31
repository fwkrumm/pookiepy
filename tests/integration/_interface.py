"""
Shared CLI argument parser for integration test scripts.

Usage
-----
    from tests.integration._interface import get_args

    args = get_args("My test description")
    client = MyClient(args.ip, args.port)
    data = client.get_data(timeout=args.timeout)
"""

import argparse

DEFAULT_PORT = 49999
DEFAULT_IP = "127.0.0.1"
DEFAULT_TIMEOUT = 5.0


def get_args(description: str = "gRPC integration test") -> argparse.Namespace:
    """Parse pookiepy CLI arguments for integration test scripts.

    Args:
        description: Help text shown in --help output.

    Returns:
        Parsed namespace with attributes: ip, port, timeout.
    """
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--ip",
        default=DEFAULT_IP,
        help=f"Server hostname or IP address (default: {DEFAULT_IP})",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_PORT,
        help=f"Server port number (default: {DEFAULT_PORT})",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT,
        help=f"Timeout in seconds for get_data calls (default: {DEFAULT_TIMEOUT})",
    )

    return parser.parse_args()
