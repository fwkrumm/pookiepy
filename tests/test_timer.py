"""Unit tests for pookiepy/timer.py public API."""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))  # pylint: disable=wrong-import-position

from pookiepy.timer import TimedEvent


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


if __name__ == "__main__":
    unittest.main()
