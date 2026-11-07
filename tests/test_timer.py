"""Regression tests for timer-driven pookiepy broadcasts."""
import sys
import threading
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).parent.parent))  # pylint: disable=wrong-import-position

from tests.integration.timer.clients_timer import ReceiverClient, _send_ticks, _wait_for_receivers


class TestTimerBroadcast(unittest.TestCase):
    """Verify timer overruns do not hide missed broadcasts."""

    def test_timer_overrun_waits_for_messages_actually_sent(self):
        driver = Mock()
        with patch("tests.integration.timer.clients_timer.TimedEvent") as timer_context:
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
