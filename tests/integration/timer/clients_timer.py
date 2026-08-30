"""Timer Test --- Clients
======================
Shows how ``pookiepy/timer.py``'s ``TimedEvent`` drives gRPC clients.

Scenario
--------
1. Fifty ``ReceiverClient`` instances subscribe to ``"timer_tick"`` messages.
2. ``TimerClient`` sends 200 periodic events at 10 ms intervals.
3. Assertions verify every receiver collected every tick.
4. Each receiver reports its average observed tick interval.

Run
---
    python tests/integration/timer/clients_timer.py
"""
import threading
import time

from pookiepy import message_pb2
from pookiepy.baseclient import BaseClient
from pookiepy.timer import TimedEvent as timer
from pookiepy.tools import generate_message
from tests.integration._interface import get_args

N_TICKS = 200
N_RECEIVERS = 50
TICK_INTERVAL = 0.01  # 200 × 0.01 s = 2 s total drive time
RECEIVE_TIMEOUT = 15.0
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
    """Records received timer ticks and their observed interval."""

    def __init__(self, port: int, receiver_index: int):
        self.count = 0
        self.first_tick_at: float | None = None
        self.last_tick_at: float | None = None
        self.done = threading.Event()
        super().__init__(
            port,
            name=f"tick_receiver_{receiver_index:02d}",
            provides=[],
            requires=[TICK_MESSAGE],
        )

    def on_receive(self, data: message_pb2.PookieMessage) -> bool:
        """Record one tick and signal when all expected ticks arrived."""
        received_at = time.perf_counter()
        if self.first_tick_at is None:
            self.first_tick_at = received_at
        self.last_tick_at = received_at
        self.count += 1
        if self.count >= N_TICKS:
            self.done.set()
        return True

    @property
    def average_tick_length(self) -> float:
        """Return mean interval between first and last received ticks."""
        if self.count < 2 or self.first_tick_at is None or self.last_tick_at is None:
            raise RuntimeError(f"{self.name} needs at least two ticks for statistics")
        return (self.last_tick_at - self.first_tick_at) / (self.count - 1)


def _start_receivers(port: int) -> tuple[list[ReceiverClient], list[threading.Thread]]:
    """Connect receivers and start one queue-draining thread per receiver."""
    receivers = [ReceiverClient(port, index) for index in range(N_RECEIVERS)]
    threads = [
        threading.Thread(target=receiver.spin_forever, daemon=True)
        for receiver in receivers
    ]
    for thread in threads:
        thread.start()
    return receivers, threads


def _send_ticks(driver: TimerClient) -> None:
    """Send one message for every periodic timer event."""
    with timer(s=TICK_INTERVAL, n=N_TICKS, logger=driver.logger) as ticks:
        for tick_index in ticks:
            driver.send_data(
                generate_message(TICK_MESSAGE, byte_payload=str(tick_index).encode())
            )
    driver.wait_done()


def _wait_for_receivers(receivers: list[ReceiverClient]) -> None:
    """Wait for all receivers against one shared deadline."""
    deadline = time.monotonic() + RECEIVE_TIMEOUT
    incomplete = []
    for receiver in receivers:
        remaining = max(0.0, deadline - time.monotonic())
        if not receiver.done.wait(timeout=remaining):
            incomplete.append(f"{receiver.name}={receiver.count}/{N_TICKS}")
    assert not incomplete, "Receivers timed out: " + ", ".join(incomplete)


def _log_statistics(driver: TimerClient, receivers: list[ReceiverClient]) -> None:
    """Log average observed tick interval for every receiver."""
    for receiver in receivers:
        driver.logger.info(
            "%s: %d ticks, average tick %.6f s (%.3f ms)",
            receiver.name,
            receiver.count,
            receiver.average_tick_length,
            receiver.average_tick_length * 1_000,
        )


def _disconnect_all(
    driver: TimerClient,
    receivers: list[ReceiverClient],
    receiver_threads: list[threading.Thread],
) -> None:
    """Request server shutdown, then close every client and spin thread."""
    driver.send_data(generate_message("server-exit"))
    driver.wait_done()
    driver.disconnect()
    for receiver in receivers:
        receiver.disconnect()
    for thread in receiver_threads:
        thread.join(timeout=5.0)


def main() -> None:
    """Run timer broadcast integration scenario."""
    args = get_args("Timer test: 200 timed messages broadcast to 50 subscribers")

    receivers, spin_threads = _start_receivers(args.port)
    driver = TimerClient(args.port)
    try:
        _send_ticks(driver)
        _wait_for_receivers(receivers)
        _log_statistics(driver, receivers)
    finally:
        _disconnect_all(driver, receivers, spin_threads)


if __name__ == "__main__":
    main()
