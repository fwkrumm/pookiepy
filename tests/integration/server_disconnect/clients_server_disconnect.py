"""
Server-Disconnect Test --- Client
================================

Demonstrates that when the server shuts down mid-stream the client detects it
within a bounded timeout and exits cleanly.  No threads are left hanging.

Assertions
----------
* ``spin()`` returns within ``MAX_WAIT`` seconds after server goes away.
* ``disconnect()`` completes without hanging.
* ``run_event`` is cleared after disconnect.
* ``receive_thread`` is no longer alive after disconnect.

Run
---
    python tests/integration/server_disconnect/clients_server_disconnect.py
"""

import sys
import time

from grpchook.baseclient import BaseClient
from grpchook.exceptions import GrpcEmpty, GrpcConnectionError, ClientExit
from tests.integration._interface import get_args

# Must be longer than SHUTDOWN_AFTER (1.0 s) on the server
SPIN_TIMEOUT = 3.0
# Hard upper bound --- if the whole flow takes longer we consider it a freeze
MAX_WAIT = 8.0


class WatchClient(BaseClient):
    """Simple client that waits for incoming messages."""

    def __init__(self, port: int):
        super().__init__(
            port,
            name="server_disconnect_client",
            provides=["ping"],
            requires=["pong"],
        )


if __name__ == "__main__":
    args = get_args("Server-disconnect test: client detects server shutdown, no freeze")

    client = WatchClient(args.port)

    t0 = time.monotonic()

    try:
        # spin() will raise GrpcEmpty once the timeout elapses with no data.
        # The server shuts down ~1 s after connect, so receive_loop terminates
        # shortly after that and no further messages will arrive.
        client.spin(timeout=SPIN_TIMEOUT)
    except GrpcEmpty:
        pass  # expected --- server went away before timeout elapsed or at timeout
    except (GrpcConnectionError, ClientExit, RuntimeError) as exc:
        # Any stream-level error is acceptable --- the server went away
        client.logger.info("spin() raised %s: %s", type(exc).__name__, exc)

    elapsed_spin = time.monotonic() - t0
    assert elapsed_spin < MAX_WAIT, (
        f"spin() took {elapsed_spin:.1f}s --- possible freeze (limit={MAX_WAIT}s)"
    )
    client.logger.info("OK: spin() returned in %.2fs", elapsed_spin)

    # disconnect() must complete promptly
    t1 = time.monotonic()
    client.disconnect()
    elapsed_disconnect = time.monotonic() - t1
    assert elapsed_disconnect < 5.0, (
        f"disconnect() took {elapsed_disconnect:.1f}s --- possible hang"
    )

    # post-disconnect invariants
    assert not client.run_event.is_set(), "run_event still set after disconnect"
    assert not client.receive_thread.is_alive(), "receive_thread still alive after disconnect"

    client.logger.info(
        "OK: disconnect() completed in %.2fs, no ghost threads detected",
        elapsed_disconnect,
    )
    sys.exit(0)
