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


if __name__ == "__main__":
    unittest.main()
