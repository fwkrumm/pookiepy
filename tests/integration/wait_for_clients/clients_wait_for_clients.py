"""
Wait-for-Clients Test --- Clients
=================================

Demonstrates a coordinator that blocks (on the server side) until all required
worker providers are connected.

Scenario
--------
1. ``CoordinatorClient`` connects and sends ``"check_ready"`` immediately.
2. Server defers the reply until two worker providers are connected.
3. Two ``WorkerClient`` instances connect shortly after (in the main thread).
4. Server detects both workers → fires ready event → sends ``"ready_signal"``.
5. CoordinatorClient's background thread receives it and signals completion.
6. Main thread asserts the signal arrived within the timeout.

Assertions
----------
* ``ready_signal`` is received within ``READY_TIMEOUT`` seconds.
* The received message name is ``"ready_signal"``.

Run
---
    python tests/integration/wait_for_clients/clients_wait_for_clients.py
"""

import sys
import threading
import time

from grpchook import message_pb2
from grpchook.baseclient import BaseClient
from grpchook.exceptions import GrpcEmpty, GrpcConnectionError, ClientExit
from grpchook.tools import generate_message
from tests.integration._interface import get_args

READY_TIMEOUT = 5.0   # seconds to wait for the ready_signal
WORKER_DELAY = 0.5    # seconds before workers connect (server is already waiting)


class CoordinatorClient(BaseClient):
    """Sends check_ready and waits for the ready_signal."""

    def __init__(self, port: int):
        super().__init__(
            port,
            name="coordinator",
            provides=["check_ready", "server-exit"],
            requires=["ready_signal"],
        )


class WorkerClient(BaseClient):
    """A provider of worker_data --- its presence satisfies the server condition."""

    def __init__(self, name: str, port: int):
        super().__init__(
            port,
            name=name,
            provides=["worker_data"],
        )


if __name__ == "__main__":
    args = get_args(
        "Wait-for-clients test: server defers response until required providers connect"
    )

    coordinator = CoordinatorClient(args.port)

    # send check_ready and flush it to gRPC before connecting workers
    coordinator.send_data(generate_message("check_ready"))
    coordinator.wait_done()
    coordinator.logger.info("INFO: check_ready sent --- server is now waiting for workers")

    # track result from background receiver thread
    received_signal: list[message_pb2.Message] = []
    receive_error: list[Exception] = []

    def _receive():
        try:
            data = coordinator.get_data(timeout=READY_TIMEOUT)
            received_signal.append(data)
        except GrpcEmpty as exc:
            receive_error.append(exc)
        except (GrpcConnectionError, ClientExit, RuntimeError) as exc:
            receive_error.append(exc)

    receiver_thread = threading.Thread(target=_receive, daemon=True)
    receiver_thread.start()

    # slight delay so server has started waiting, then connect workers
    time.sleep(WORKER_DELAY)
    coordinator.logger.info("INFO: connecting worker1 and worker2")
    worker1 = WorkerClient("worker1", args.port)
    worker2 = WorkerClient("worker2", args.port)

    # wait for coordinator to get the ready_signal
    receiver_thread.join(timeout=READY_TIMEOUT + 1.0)

    assert not receive_error, (
        f"coordinator receiver raised: {receive_error[0]}"
    )
    assert received_signal, (
        f"coordinator did not receive ready_signal within {READY_TIMEOUT}s"
    )

    msg = received_signal[0]
    assert msg.metaInfo.messageName == "ready_signal", (
        f"expected 'ready_signal', got '{msg.metaInfo.messageName}'"
    )
    coordinator.logger.info("OK: coordinator received 'ready_signal' after workers connected")

    # graceful shutdown
    coordinator.send_data(
        message_pb2.Message(
            metaInfo=message_pb2.MetaInformation(messageName="server-exit")
        )
    )
    coordinator.wait_done()

    for client in (coordinator, worker1, worker2):
        client.disconnect()

    coordinator.logger.info("wait_for_clients: all assertions passed")
    sys.exit(0)
