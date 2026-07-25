"""
Server-Off Test --- Client connects, no freeze
=============================================

Demonstrates that when no server is reachable, ``BaseClient.__init__`` raises
``GrpcConnectionError`` promptly (2-second timeout) and the process can exit
cleanly --- no frozen threads, no hanging.

Run
---
    python tests/integration/server_off/clients_server_off.py
"""

import sys

from grpchook.baseclient import BaseClient
from grpchook.exceptions import GrpcConnectionError
from grpchook.logger import get_logger

# Use a dedicated port that the test runner will never start a server on.
# The point is that *nothing* is listening here.
PORT = 49001


class TestClient(BaseClient):
    """Minimal client used only to attempt a connection."""

    def __init__(self, port: int):
        super().__init__(port, name="server_off_client", provides=["test"])


if __name__ == "__main__":
    logger = get_logger("server_off")
    logger.info("server_off: attempting to connect to a port with no server...")

    try:
        client = TestClient(PORT)
        # If we reach here the connection succeeded --- that is a test failure
        client.disconnect()
        logger.error("ERROR: expected GrpcConnectionError but connection succeeded")
        sys.exit(1)
    except GrpcConnectionError as exc:
        logger.info("OK: GrpcConnectionError raised as expected: %s", exc)

    logger.info("server_off: no freeze detected, exiting cleanly")
    sys.exit(0)
