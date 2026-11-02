"""Timer Test --- Clients
======================
Shows how ``pookiepy/timer.py``'s ``TimedEvent`` drives gRPC clients.

Scenario
--------
1. Fifty ``ReceiverClient`` instances subscribe to ``"timer_tick"`` messages.
2. ``TimerClient`` sends 200 periodic events at 10 ms intervals.
3. Assertions verify every receiver collected every sent tick.
4. Each receiver reports its average observed tick interval.

Run
---
    python tests/integration/timer/clients_timer.py
"""
import logging
import sys
import threading
import time

from pookiepy import message_pb2
from pookiepy.baseclient import BaseClient
from pookiepy.timer import TimedEvent as timer
from pookiepy.tools import generate_message
from tests.integration._interface import get_args

N_TICKS = 200
N_RECEIVERS = 50
# Short interval keeps example runtime near two seconds. Achievable precision
# depends on OS timer resolution, scheduler load, hardware, and CI environment.
TICK_INTERVAL = 0.01
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
        # Exclude synchronous per-message debug file writes from timing results.
        self.logger.setLevel(logging.INFO)


class ReceiverClient(BaseClient):
    """Records received timer ticks and their observed interval."""

    def __init__(self, port: int, receiver_index: int):
        self.count = 0
        self.first_tick_at: float | None = None
        self.last_tick_at: float | None = None
        self.received = threading.Condition()
        super().__init__(
            port,
            name=f"tick_receiver_{receiver_index:02d}",
            provides=[],
            requires=[TICK_MESSAGE],
        )
        # Exclude synchronous per-message debug file writes from timing results.
        self.logger.setLevel(logging.INFO)

    def on_receive(self, data: message_pb2.PookieMessage) -> bool:
        """Record one tick and wake receivers waiting for delivery."""
        received_at = time.perf_counter()
        with self.received:
            if self.first_tick_at is None:
                self.first_tick_at = received_at
            self.last_tick_at = received_at
            self.count += 1
            self.received.notify_all()
        return True

    def wait_for_ticks(self, expected: int, timeout: float) -> bool:
        with self.received:
            return self.received.wait_for(lambda: self.count >= expected, timeout=timeout)

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


def _send_ticks(driver: TimerClient) -> int:
    """Send one message for every periodic timer event."""
    # macOS reports inherited gRPC poller descriptors when multiprocessing
    # starts after gRPC threads. Its thread backend avoids child creation;
    # other platforms retain process-isolated timing.
    backend = "thread" if sys.platform == "darwin" else "process"
    driver.logger.info("Using %s timer backend on %s", backend, sys.platform)
    with timer(
        s=TICK_INTERVAL,
        n=N_TICKS,
        logger=driver.logger,
        backend=backend,
    ) as ticks:
        sent_count = 0
        for tick_index in ticks:
            driver.send_data(
                generate_message(TICK_MESSAGE, byte_payload=str(tick_index).encode())
            )
            sent_count += 1
    driver.wait_done()
    assert sent_count >= 2, f"Timer emitted only {sent_count} ticks"
    return sent_count


def _wait_for_receivers(receivers: list[ReceiverClient], expected: int) -> None:
    """Wait for all receivers against one shared deadline."""
    deadline = time.monotonic() + RECEIVE_TIMEOUT
    incomplete = []
    for receiver in receivers:
        remaining = max(0.0, deadline - time.monotonic())
        if not receiver.wait_for_ticks(expected, timeout=remaining):
            incomplete.append(f"{receiver.name}={receiver.count}/{expected}")
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
        sent_count = _send_ticks(driver)
        _wait_for_receivers(receivers, sent_count)
        _log_statistics(driver, receivers)
    finally:
        _disconnect_all(driver, receivers, spin_threads)


if __name__ == "__main__":
    main()
