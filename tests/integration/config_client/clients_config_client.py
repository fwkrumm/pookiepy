"""
Config-Client Test --- Clients
==============================

Demonstrates runtime control of the server from a dedicated config client.

Scenario
--------
1. ``WorkerClient`` connects and waits for data.
2. ``ConfigClient`` connects and sends ``{"action": "disconnect", "target": "worker"}``
   to the server.
3. Server calls ``context.cancel()`` on the worker's gRPC stream.
4. Worker's stream is terminated from the server side.
5. Worker's ``spin()`` returns without hanging (GrpcEmpty after timeout or
   stream ends).
6. ``ConfigClient`` then shuts the server down cleanly.

Assertions
----------
* After the disconnect command, the worker's ``spin()`` completes within
  ``MAX_WAIT`` seconds (no freeze).
* ``disconnect()`` on the worker completes promptly.

Run
---
    python tests/integration/config_client/clients_config_client.py
"""

import sys
import time

from grpchook import message_pb2
from grpchook.baseclient import BaseClient
from grpchook.exceptions import GrpcEmpty, GrpcConnectionError, ClientExit, GrpcTimeoutError
from grpchook.tools import generate_message
from tests.integration._interface import get_args

SPIN_TIMEOUT = 3.0   # worker blocks here waiting for data
MAX_WAIT = 6.0        # hard upper bound to detect a freeze
CONFIG_MESSAGE = "server_config"


class WorkerClient(BaseClient):
    """A client that just waits for messages."""

    def __init__(self, port: int):
        super().__init__(
            port,
            name="worker",
            provides=["worker_data"],
            requires=["worker_data"],
        )


class ConfigClient(BaseClient):
    """Sends runtime control commands to the server."""

    def __init__(self, port: int):
        super().__init__(
            port,
            name="config",
            provides=[CONFIG_MESSAGE, "server-exit"],
        )


if __name__ == "__main__":
    args = get_args("Config-client test: runtime disconnect of individual peers")

    worker = WorkerClient(args.port)
    config = ConfigClient(args.port)

    # give the server a moment to register the worker's context
    time.sleep(0.3)

    # send the disconnect command
    config.send_data(
        generate_message(
            CONFIG_MESSAGE,
            struct_payload={"action": "disconnect", "target": "worker"},
        )
    )
    config.wait_done()
    config.logger.info("INFO: disconnect command sent for 'worker'")

    # worker's spin() must complete promptly after the server cancels the stream
    t0 = time.monotonic()
    try:
        worker.spin(timeout=SPIN_TIMEOUT)
    except GrpcEmpty:
        pass  # expected --- no data arriving after disconnect
    except (GrpcConnectionError, ClientExit, GrpcTimeoutError, RuntimeError) as exc:
        config.logger.info("worker spin() raised %s: %s", type(exc).__name__, exc)

    elapsed = time.monotonic() - t0
    assert elapsed < MAX_WAIT, (
        f"worker.spin() took {elapsed:.1f}s --- possible freeze (limit={MAX_WAIT}s)"
    )
    config.logger.info("OK: worker.spin() completed in %.2fs after server-side disconnect", elapsed)

    # disconnect the worker client locally
    t1 = time.monotonic()
    worker.disconnect()
    elapsed_dc = time.monotonic() - t1
    assert elapsed_dc < 5.0, f"worker.disconnect() hung for {elapsed_dc:.1f}s"
    config.logger.info("OK: worker.disconnect() completed in %.2fs", elapsed_dc)

    # shut down server
    config.send_data(
        message_pb2.Message(
            metaInfo=message_pb2.MetaInformation(messageName="server-exit")
        )
    )
    config.wait_done()
    config.disconnect()

    config.logger.info("config_client: all assertions passed")
    sys.exit(0)
