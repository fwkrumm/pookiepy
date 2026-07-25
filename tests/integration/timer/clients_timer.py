"""Timer Test --- Clients
======================
Shows how ``grpchook/timer.py``'s ``timer()`` function drives a gRPC client.

Scenario
--------
1. ``ReceiverClient`` connects and subscribes to ``"timer_tick"`` messages.
2. ``TimerClient`` uses a background thread powered by the low-level ``timer()``
   function to fire ``N_TICKS`` periodic events at ``TICK_INTERVAL`` seconds each,
   sending one gRPC message per tick.
3. After all ticks are sent, it triggers a graceful server shutdown.
4. Assertions verify the receiver collected every tick.

Note
----
The low-level ``timer()`` function is used instead of ``timedevent`` to avoid
the psutil REALTIME priority escalation that requires admin rights on Windows.

Run
---
    python tests/integration/timer/clients_timer.py
"""
import threading

from grpchook import message_pb2
from grpchook.baseclient import BaseClient
from grpchook.timer import timer
from grpchook.tools import generate_message
from tests.integration._interface import get_args

N_TICKS = 5
TICK_INTERVAL = 0.3   # 5 × 0.3 s = 1.5 s total drive time
TICK_MESSAGE = "timer_tick"


class TimerClient(BaseClient):
    """Sends one ``TICK_MESSAGE`` per timer tick."""

    def __init__(self, port: int):
        super().__init__(
            port,
            name="timer_driver",
            provides=[TICK_MESSAGE, "server-exit"],
            requires=[],
        )


class ReceiverClient(BaseClient):
    """Counts received timer tick messages."""

    def __init__(self, port: int):
        self.count = 0
        super().__init__(
            port,
            name="tick_receiver",
            provides=[],
            requires=[TICK_MESSAGE],
        )

    def on_receive(self, data: message_pb2.Message) -> bool:
        """Increment counter; return True so spin_forever does not stop."""
        self.count += 1
        return True


if __name__ == "__main__":
    args = get_args("Timer test: timer-driven client sends tick messages to a subscriber")

    receiver = ReceiverClient(args.port)
    driver = TimerClient(args.port)

    tick_event = threading.Event()

    def _drive_ticks() -> None:
        """Fire N_TICKS gRPC messages, one per timer event."""
        timer_thread = threading.Thread(
            target=timer,
            args=(N_TICKS, TICK_INTERVAL, tick_event, False),  # compensation disabled
            daemon=True,
        )
        timer_thread.start()
        for tick_index in range(N_TICKS):
            fired = tick_event.wait(timeout=5.0)
            assert fired, f"Timer tick {tick_index} did not fire within 5 s"
            tick_event.clear()
            driver.send_data(
                generate_message(TICK_MESSAGE, byte_payload=str(tick_index).encode())
            )
        timer_thread.join(timeout=5.0)

    drive_thread = threading.Thread(target=_drive_ticks, daemon=True)
    drive_thread.start()

    # receive all ticks on the main thread --- spin(timeout=5) blocks until
    # one message arrives or 5 s elapses
    for i in range(N_TICKS):
        got = receiver.spin(timeout=5.0)
        assert got is not False, f"Receiver timed out waiting for tick {i}"

    drive_thread.join(timeout=10.0)
    driver.wait_done()

    assert receiver.count >= N_TICKS, (
        f"Receiver got {receiver.count} ticks, expected {N_TICKS}"
    )
    receiver.logger.info("OK: receiver collected all %d timer ticks", receiver.count)

    driver.send_data(generate_message("server-exit"))
    driver.wait_done()
    driver.disconnect()
    receiver.disconnect()
