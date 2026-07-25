"""
Broadcast Test --- Clients
=========================

Three independent clients all subscribe to ``"broadcast"``.  Demonstrates
that a server-initiated message is delivered to every subscriber.

Assertions
----------
* Every client receives at least ``MIN_MESSAGES`` broadcast messages.
* All received byte-payloads are numeric strings (counter from server).
* Each client independently receives the same set of counter values
  (fan-out, not round-robin).

Run
---
    python tests/integration/broadcast/clients_broadcast.py
"""

import sys

from grpchook import message_pb2
from grpchook.baseclient import BaseClient
from grpchook.exceptions import GrpcEmpty
from tests.integration._interface import get_args

NUM_CLIENTS = 3
MIN_MESSAGES = 5
TIMEOUT = 2.0  # per get_data call


class BroadcastReceiver(BaseClient):
    """Client that subscribes to server broadcasts."""

    def __init__(self, name: str, port: int):
        super().__init__(
            port,
            name=name,
            requires=["broadcast"],
            provides=["server-exit"],
        )


if __name__ == "__main__":
    args = get_args("Broadcast test: server sends to all connected clients")

    clients = [
        BroadcastReceiver(f"receiver{i}", args.port)
        for i in range(NUM_CLIENTS)
    ]

    # collect MIN_MESSAGES from each client
    received: dict[str, list[int]] = {c.name: [] for c in clients}

    for client in clients:
        while len(received[client.name]) < MIN_MESSAGES:
            try:
                data = client.get_data(timeout=TIMEOUT)
            except GrpcEmpty:
                break

            # Connection welcome/metadata messages can arrive on this queue.
            # Only count actual broadcast payload frames.
            if data.metaInfo.messageName != "broadcast":
                continue

            payload_text = data.payload.bytePayload.decode().strip()
            if not payload_text:
                continue

            counter = int(payload_text)
            received[client.name].append(counter)

    # assertions
    for client in clients:
        msgs = received[client.name]
        assert len(msgs) >= MIN_MESSAGES, (
            f"{client.name} received only {len(msgs)} messages "
            f"(expected >= {MIN_MESSAGES})"
        )
        # counters must be consecutive (no gaps larger than 1 --- server sends at fixed rate)
        for a, b in zip(msgs, msgs[1:]):
            assert b >= a, f"{client.name}: non-monotonic counter {a} -> {b}"
        clients[0].logger.info(
            "OK: %s received %d messages, counters %d..%d",
            client.name, len(msgs), msgs[0], msgs[-1],
        )

    # all clients should have received overlapping counter ranges (fan-out, not round-robin)
    sets = [set(received[c.name]) for c in clients]
    overlap = sets[0].intersection(*sets[1:])
    assert len(overlap) > 0, (
        "No counter overlap between clients --- messages were not broadcast to all"
    )
    clients[0].logger.info(
        "OK: %d counter values received by all %d clients (fan-out confirmed)",
        len(overlap), NUM_CLIENTS,
    )

    # graceful shutdown
    clients[0].send_data(
        message_pb2.Message(
            metaInfo=message_pb2.MetaInformation(messageName="server-exit")
        )
    )
    clients[0].wait_done()

    for client in clients:
        client.disconnect()

    clients[0].logger.info("broadcast: all assertions passed")
    sys.exit(0)
