"""High-Fire Test --- Clients
===========================
Stress-tests the gRPC framework by sending a high volume of messages as fast
as possible and asserting that all of them arrive at the receiving client.

Scenario
--------
1. ``StressSender`` sends ``HIGH_FIRE_COUNT`` ``"stress_data"`` messages,
   each carrying a ``PAYLOAD_SIZE``-byte payload.
2. ``StressReceiver`` receives and counts them using a background spin loop.
3. After all messages are confirmed sent (``wait_done()``), the test waits
   for the receiver count to reach ``HIGH_FIRE_COUNT``.
4. Throughput (messages/s and MB/s) is printed and all messages asserted.

Run
---
    python tests/integration/high_fire/clients_high_fire.py
"""
import threading
import time

from grpchook import message_pb2
from grpchook.baseclient import BaseClient
from grpchook.tools import generate_message
from tests.integration._interface import get_args

HIGH_FIRE_COUNT = 5_000
PAYLOAD_SIZE    = 4096    # bytes per message (4 KB)
STRESS_MESSAGE  = "stress_data"


class StressSender(BaseClient):
    """Sends ``HIGH_FIRE_COUNT`` stress messages as fast as possible."""

    def __init__(self, port: int):
        super().__init__(
            port,
            name="stress_sender",
            provides=[STRESS_MESSAGE, "server-exit"],
            requires=[],
        )


class StressReceiver(BaseClient):
    """Counts received stress messages; signals completion via an event."""

    def __init__(self, port: int):
        self.count = 0
        self.done = threading.Event()
        super().__init__(
            port,
            name="stress_receiver",
            provides=[],
            requires=[STRESS_MESSAGE],
        )

    def on_receive(self, data: message_pb2.Message) -> bool:
        """Increment counter and set the done event once all messages arrive.

        Returns True so spin_forever does not stop between messages.
        """
        self.count += 1
        if self.count >= HIGH_FIRE_COUNT:
            self.done.set()
        return True


if __name__ == "__main__":
    args = get_args("High-fire test: maximum-throughput stress test on localhost")

    receiver = StressReceiver(args.port)
    sender   = StressSender(args.port)

    # drain receiver's queue in a background thread
    spin_thread = threading.Thread(
        target=receiver.spin_forever,
        daemon=True,
    )
    spin_thread.start()

    payload = b"X" * PAYLOAD_SIZE

    t0 = time.perf_counter()
    for _ in range(HIGH_FIRE_COUNT):
        sender.send_data(generate_message(STRESS_MESSAGE, byte_payload=payload))

    # wait until all messages have been handed off to the gRPC stream
    sender.wait_done()
    t_sent = time.perf_counter()

    # wait up to 30 s for the receiver to collect every message
    got_all = receiver.done.wait(timeout=30.0)
    t_done = time.perf_counter()

    assert got_all, (
        f"Timeout: receiver only got {receiver.count}/{HIGH_FIRE_COUNT} messages"
    )

    elapsed     = t_done - t0
    msgs_per_s  = HIGH_FIRE_COUNT / elapsed
    mb_per_s    = (HIGH_FIRE_COUNT * PAYLOAD_SIZE) / (elapsed * 1024 * 1024)

    sender.logger.info(
        "OK: %d × %d-byte messages received --- %d msg/s  (%.2f MB/s)  "
        "send=%.3fs  total=%.3fs",
        HIGH_FIRE_COUNT, PAYLOAD_SIZE, msgs_per_s, mb_per_s,
        t_sent - t0, elapsed,
    )

    sender.send_data(generate_message("server-exit"))
    sender.wait_done()
    sender.disconnect()
    receiver.disconnect()
    spin_thread.join(timeout=5.0)
