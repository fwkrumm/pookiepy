"""Unit tests for pookiepy/timer.py public API."""
import inspect
import sys
import threading
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).parent.parent))  # pylint: disable=wrong-import-position

from pookiepy.timer import TimedEvent, timer
from tests.integration.timer.clients_timer import ReceiverClient, _send_ticks, _wait_for_receivers


class TestTimedEvent(unittest.TestCase):
    """Ensure TimedEvent class is directly constructible."""

    def test_timed_event_instance_creation(self):
        """TimedEvent constructor returns a TimedEvent context manager instance."""
        timer_ctx = TimedEvent(s=0.01, n=1, compensation=False)
        self.assertIsInstance(timer_ctx, TimedEvent)

    def test_thread_backend_emits_all_ticks(self):
        """Thread backend emits requested ticks without creating a process."""
        with TimedEvent(s=0.01, n=3, compensation=False, backend="thread") as timer_ctx:
            self.assertEqual(list(timer_ctx), [0, 1, 2])


class TestTimer(unittest.TestCase):
    """Ensure low-level timer keeps its established positional API."""

    def test_only_stop_event_is_keyword_only(self):
        """New shutdown event must not expand positional API."""
        parameters = inspect.signature(timer).parameters

        self.assertEqual(
            parameters["enable_compensation"].kind,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        )
        self.assertEqual(
            parameters["logger_level"].kind,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        )
        self.assertEqual(parameters["stop_event"].kind, inspect.Parameter.KEYWORD_ONLY)

    def test_existing_five_positional_arguments_remain_valid(self):
        """Established positional arguments still invoke timer unchanged."""
        tick_event = threading.Event()

        timer(0, 0.01, tick_event, False, None)


class TestTimerBroadcast(unittest.TestCase):
    """Verify timer overruns do not hide missed broadcasts."""

    def test_timer_overrun_waits_for_messages_actually_sent(self):
        driver = Mock()
        with patch("tests.integration.timer.clients_timer.timer") as timer_context:
            timer_context.return_value.__enter__.return_value = iter(range(199))
            sent_count = _send_ticks(driver)

        self.assertEqual(sent_count, 199)
        self.assertEqual(driver.send_data.call_count, 199)

        receiver = object.__new__(ReceiverClient)
        receiver.name = "tick_receiver_00"
        receiver.count = 0
        receiver.first_tick_at = None
        receiver.last_tick_at = None
        receiver.received = threading.Condition()
        for _ in range(sent_count - 1):
            receiver.on_receive(None)

        with patch("tests.integration.timer.clients_timer.RECEIVE_TIMEOUT", 0):
            with self.assertRaisesRegex(AssertionError, "tick_receiver_00=198/199"):
                _wait_for_receivers([receiver], sent_count)

        receiver.on_receive(None)
        _wait_for_receivers([receiver], sent_count)
        with patch("tests.integration.timer.clients_timer.RECEIVE_TIMEOUT", 0):
            with self.assertRaisesRegex(AssertionError, "tick_receiver_00=199/200"):
                _wait_for_receivers([receiver], sent_count + 1)


if __name__ == "__main__":
    unittest.main()
